//! P2: every checkpoint the engine wrote must be reproducible from the file.
//!
//! # Why this is evidence and `reconcile`-shaped checks are not
//!
//! The obvious check — ask `explain` for a number and compare it with
//! `reconcile`'s — would hold **by construction**, because both call
//! `journal::replay` over the same lines. M5.c already caught one identity of
//! exactly that shape: an assertion that could never fail, wearing the costume
//! of evidence.
//!
//! The independent pair is already in the journal and needs no new machinery. A
//! `Checkpoint` is what the **engine believed in memory** at that instant,
//! written down at the time by a process that is now gone. A fold of the fill
//! lines up to that instant is what **the file says**. They were produced by
//! different code from different inputs, and a fortnight writes roughly two
//! hundred of them — so this is two hundred independent agreements, not one.
//!
//! `reconcile` checks the *last* checkpoint. This checks all of them, which is
//! what turns "the totals came out right" into "the engine's belief was correct
//! at every point where it wrote one down".

use std::path::Path;

use quant_core::instrument::InstrumentRegistry;
use quant_engine::journal::{self, JournalEntry};

/// What a run of the check found.
#[derive(Debug, Default)]
pub struct Agreement {
    /// Checkpoints compared and matched.
    pub matched: u64,
    /// The first disagreement, with both values. Only the first: a divergence
    /// propagates, so the hundredth is a consequence rather than a finding.
    pub divergence: Option<String>,
}

impl Agreement {
    #[must_use]
    pub const fn agrees(&self) -> bool {
        self.divergence.is_none()
    }
}

/// Check every checkpoint in a journal against a replay of the entries it
/// describes.
///
/// # Errors
///
/// Propagates an unreadable journal.
pub fn check_all(journal_path: &Path) -> std::io::Result<Agreement> {
    let recovered = journal::read(journal_path)?;
    let mut out = Agreement::default();

    for (i, entry) in recovered.entries.iter().enumerate() {
        let JournalEntry::Checkpoint {
            at,
            cash,
            realized,
            fees,
            fills,
        } = entry
        else {
            continue;
        };

        // The prefix this checkpoint describes: everything up to and including
        // it. Not the whole file -- a checkpoint is a claim about a *prefix*,
        // and comparing it against a replay of entries written afterwards is
        // the false alarm M5.f found, which turned a healthy fortnight red
        // every six hours.
        let described = &recovered.entries[..=i];
        let replayed = journal::replay(described, &mut InstrumentRegistry::new());

        let mismatch = [
            ("cash", *cash, replayed.cash()),
            ("realized", *realized, replayed.realized()),
            ("fees", *fees, replayed.fees()),
        ]
        .into_iter()
        .find(|(_, claimed, actual)| claimed != actual);

        if *fills != replayed.fills() {
            out.divergence = Some(format!(
                "the checkpoint at {} claims {fills} fills; the lines before it replay to {}",
                at.to_rfc3339(),
                replayed.fills()
            ));
            return Ok(out);
        }
        if let Some((field, claimed, actual)) = mismatch {
            out.divergence = Some(format!(
                "the checkpoint at {} claims {field} {claimed}; the lines before it replay to {actual}",
                at.to_rfc3339()
            ));
            return Ok(out);
        }
        out.matched += 1;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::{check_all, Agreement};

    /// A journal with two fills and a checkpoint that describes them.
    fn sound_journal() -> String {
        [
            r#"{"type":"started","at":0,"cash":10000000000}"#,
            r#"{"type":"filled","at":1000,"client_order_id":1,"instrument":{"exchange":"binance","symbol":"BTCUSDT"},"side":"buy","px":10000000000,"qty":100000,"fee":1000,"is_maker":false}"#,
            r#"{"type":"filled","at":2000,"client_order_id":2,"instrument":{"exchange":"binance","symbol":"BTCUSDT"},"side":"sell","px":10100000000,"qty":100000,"fee":1010,"is_maker":false}"#,
        ]
        .join("\n")
    }

    /// Write `lines` to a temp file and check it.
    fn check(name: &str, lines: &str) -> Agreement {
        let thread = std::thread::current();
        let unique = thread.name().unwrap_or("unnamed").replace("::", "-");
        let path = std::env::temp_dir().join(format!("quant-explain-cp-{name}-{unique}.jsonl"));
        let mut file = std::fs::File::create(&path).expect("create");
        file.write_all(lines.as_bytes()).expect("write");
        file.write_all(b"\n").expect("newline");
        check_all(&path).expect("readable")
    }

    /// The checkpoint a sound journal's fills actually replay to.
    fn honest_checkpoint() -> String {
        // Buy 0.001 at 100 costs 100000 raw plus 1000 fee; sell at 101 returns
        // 101000 less 1010. Realized is the 1000 difference, fees are 2010.
        let cash = 10_000_000_000_i64 - 10_000_000 - 1_000 + 10_100_000 - 1_010;
        format!(
            r#"{{"type":"checkpoint","at":3000,"cash":{cash},"realized":100000,"fees":2010,"fills":2}}"#
        )
    }

    #[test]
    fn a_sound_journal_agrees_at_every_checkpoint() {
        let lines = format!("{}\n{}", sound_journal(), honest_checkpoint());
        let agreement = check("sound", &lines);
        assert!(agreement.agrees(), "{:?}", agreement.divergence);
        assert_eq!(agreement.matched, 1, "the checkpoint was actually compared");
    }

    #[test]
    fn a_checkpoint_claiming_the_wrong_cash_is_caught() {
        // Sabotage one: the engine's belief and the file disagree about money.
        let lines = format!(
            "{}\n{}",
            sound_journal(),
            r#"{"type":"checkpoint","at":3000,"cash":999,"realized":100000,"fees":2010,"fills":2}"#
        );
        let agreement = check("wrong-cash", &lines);
        let why = agreement.divergence.expect("must be caught");
        assert!(why.contains("cash"), "{why}");
    }

    #[test]
    fn a_fill_acted_on_but_never_written_down_is_caught() {
        // Sabotage two, and the one that matters most: the engine traded and the
        // line did not reach the disk, so a restart would resume from a position
        // we do not hold.
        let journal = sound_journal();
        let mut lines: Vec<&str> = journal.lines().collect();
        lines.remove(2);
        let owned = format!("{}\n{}", lines.join("\n"), honest_checkpoint());
        let agreement = check("lost-fill", &owned);
        let why = agreement.divergence.expect("must be caught");
        assert!(why.contains("fills"), "{why}");
    }

    #[test]
    fn a_journal_with_no_checkpoint_matches_nothing_and_says_so() {
        // Not agreement. "Nobody disagreed" and "two answers matched" are
        // different statements and only one of them is evidence -- the same
        // distinction `reconcile` draws with its own exit code.
        let agreement = check("no-checkpoint", &sound_journal());
        assert!(agreement.agrees(), "nothing disagreed");
        assert_eq!(
            agreement.matched, 0,
            "and nothing was checked, which the caller must not read as success"
        );
    }

    #[test]
    fn fills_after_a_checkpoint_are_not_a_disagreement() {
        // The false alarm M5.f found, pinned here too because this check has the
        // same shape and would fail the same way. A journal the engine is still
        // appending to always has fills after its last checkpoint.
        let lines = format!(
            "{}\n{}\n{}",
            sound_journal(),
            honest_checkpoint(),
            r#"{"type":"filled","at":4000,"client_order_id":3,"instrument":{"exchange":"binance","symbol":"BTCUSDT"},"side":"buy","px":10000000000,"qty":100000,"fee":1000,"is_maker":false}"#
        );
        let agreement = check("later-fills", &lines);
        assert!(agreement.agrees(), "{:?}", agreement.divergence);
        assert_eq!(agreement.matched, 1);
    }
}
