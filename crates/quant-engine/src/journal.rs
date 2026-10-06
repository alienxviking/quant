//! Our own record of what we did.
//!
//! A backtest can be re-run from raw. A two-week paper session cannot: the fills
//! it produced were decided against a live book that no longer exists in that
//! state, and the engine's position lives in memory. A restart on day nine with
//! no journal is a restart with a flat book and a forgotten position, which is
//! the shape of failure that loses real money at M8.
//!
//! # Why a file, and not the metadata tier
//!
//! `docs/data-contract.md` puts orders and fills in Postgres, and they will go
//! there. But M1.c2 made the metadata tier **best-effort and optional** — no
//! database, or one that will not answer, and the recorder carries on — and a
//! position that must survive a restart cannot depend on something optional.
//!
//! So the relationship is the one the raw tier already has with its index: this
//! file is the **source of truth** for our own actions, and Postgres is a query
//! surface over it. That is not a new rule, it is the existing rule applied to a
//! second kind of irreplaceable data.
//!
//! # Why JSON Lines
//!
//! Against the grain of this project, which framed the raw tier in binary for
//! good reasons. Those reasons do not apply here. The raw tier is tens of
//! millions of records a day and its density decides whether a week fits on a
//! laptop; a journal is a few hundred lines a week. What matters instead is that
//! it can be read at three in the morning by a person, and by `jq`, and appended
//! to safely after a crash — and a torn last line is trivially detectable and
//! discardable, whereas a torn binary frame needed a whole container format.
//!
//! # Why the instrument is `(exchange, symbol)`
//!
//! `InstrumentId` is a registry index and is **never persisted** — M0's rule,
//! and the same reason the Parquet tier has no instrument column. An id written
//! on Tuesday means something else on Wednesday if the registration order
//! changed. Identity is reattached on replay.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::RefusedBy;
use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, Fill, RejectReason};
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::instrument::{Exchange, InstrumentId, InstrumentRegistry};
use quant_core::time::Ts;

use crate::portfolio::Portfolio;

/// How an instrument is named on disk.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstrumentKey {
    pub exchange: Exchange,
    /// The venue's own symbol, verbatim.
    pub symbol: String,
}

/// One line of the journal.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JournalEntry {
    /// A session began, with this much cash.
    ///
    /// Written once per run. On a restart the *first* one in the file is the
    /// authority on starting capital, and later ones mark restarts — so the
    /// number a report quotes is the capital the whole session began with, not
    /// whatever it happened to have when it last came up.
    Started {
        at: Ts,
        cash: Notional,
        /// Which shape of this file the writer spoke.
        ///
        /// Added at M7.5, which widened this enum from five variants to a
        /// dozen. A pre-M7.5 reader meeting a decision entry treats the unknown
        /// tag as corruption, because `read` has no version to refuse at — the
        /// same problem M1.d1 had when the raw container went 1 → 2, without
        /// M1.d1's header to refuse in. This is here so the *next* change can
        /// say what is actually wrong rather than reporting a readable file as
        /// damaged.
        ///
        /// Defaulted on read so every journal written before it existed still
        /// parses, and reports as schema 1.
        #[serde(default = "schema_v1")]
        schema: u32,
    },
    /// Something traded.
    Filled {
        at: Ts,
        client_order_id: ClientOrderId,
        instrument: InstrumentKey,
        side: Side,
        px: Px,
        qty: Qty,
        fee: Notional,
        is_maker: bool,
    },
    /// What the *engine* believed at this moment.
    ///
    /// This is the entry that makes reconciliation a check rather than a
    /// tautology. Replaying the fills recomputes cash, realized and fees from
    /// the same lines, so `cash - starting == realized - fees` is true by
    /// construction and proves nothing. A checkpoint is a **second, independent**
    /// claim: the running engine states what it thinks it has, and the recompute
    /// has to arrive at the same numbers from the journal alone.
    ///
    /// A disagreement means a fill was acted on and not written down, or the
    /// accounting drifted — and neither would be visible from one number.
    Checkpoint {
        at: Ts,
        cash: Notional,
        realized: Notional,
        fees: Notional,
        fills: u64,
    },
    /// An order was minted and is about to reach the venue.
    ///
    /// Written **before** the wire, which is the fill rule generalised: an order
    /// at a venue we never recorded is an orphan position.
    ///
    /// `mark` is the mid the risk layer priced it against. It is the one input
    /// to a refusal that cannot be re-derived from raw, because it depends on
    /// the book *as the engine saw it* — which a replay reconstructs but a
    /// reader cannot attribute to a specific decision without this.
    Submitted {
        at: Ts,
        client_order_id: ClientOrderId,
        instrument: InstrumentKey,
        side: Side,
        qty: Qty,
        limit: Option<Px>,
        mark: Option<Px>,
    },
    /// An order was refused, and the venue never heard about it.
    Refused {
        at: Ts,
        client_order_id: ClientOrderId,
        instrument: InstrumentKey,
        side: Side,
        qty: Qty,
        reason: RejectReason,
        /// Which limit bound. `None` for a seam refusal, which is not a limit.
        bound: Option<crate::risk::Bound>,
        by: RefusedBy,
    },
    /// The venue acknowledged an order.
    Accepted {
        at: Ts,
        client_order_id: ClientOrderId,
    },
    /// The venue declined an order it had been told about.
    Rejected {
        at: Ts,
        client_order_id: ClientOrderId,
        reason: RejectReason,
    },
    /// A cancel was sent; whether it won the race is a separate entry.
    CancelRequested {
        at: Ts,
        client_order_id: ClientOrderId,
    },
    /// An order was withdrawn, with whatever never traded.
    Cancelled {
        at: Ts,
        client_order_id: ClientOrderId,
        remaining: Qty,
    },
    /// A fill arrived for an order this engine has no record of.
    ///
    /// Recorded rather than counted, because an orphan is the one fill that
    /// cannot be reconciled: the portfolio never saw it, so `Checkpoint` and the
    /// fold agree with each other while both disagree with the venue.
    Orphaned {
        at: Ts,
        client_order_id: ClientOrderId,
        px: Px,
        qty: Qty,
        fee: Notional,
    },
    /// The kill switch was thrown.
    ///
    /// Journalled because a switch held only in memory is re-armed by the
    /// supervisor that restarts the process — the machinery meant to keep the
    /// system running would silently resume trading minutes after a limit said
    /// stop. `RiskEngine::recover` reads this back.
    Tripped {
        at: Ts,
        cause: crate::risk::TripCause,
    },
    /// The market went dark, and for how long we could not see it.
    ///
    /// The answer to "why did it not trade between 03:00 and 04:00" is one of
    /// four things, and three of them are already recorded — a crossing that
    /// fired, a refusal, a suppression. This is the fourth, and without it a
    /// quiet hour and a blind hour are the same silence in the file.
    ///
    /// `last_good_ts` is the last instant we were confident the stream was
    /// intact, so `at - last_good_ts` is the width of the blindness. That is a
    /// **duration, not a message count**, which is a deliberate narrowing of
    /// what this slice was planned to record: a count would have to be carried
    /// on `MarketEvent::Gap`, and that type is persisted in the normalized tier,
    /// so a field added for a log line would change the Parquet schema of the
    /// whole `gaps/` dataset. The question the operator actually asks is *were
    /// we blind at 03:30*, and a duration answers it exactly.
    ///
    /// A `LocalOverflow` cause here is the engine's own account of records the
    /// tee dropped on its way in, which is the same fact the capture side
    /// counts in `TeeSink::secondary_dropped`. Neither is derived from the
    /// other, so a disagreement means one of them is wrong — and nothing would
    /// say so if only one existed.
    Blind {
        at: Ts,
        cause: quant_core::event::GapCause,
        last_good_ts: Ts,
    },
    /// The strategy's own account of something it decided.
    ///
    /// The column this milestone exists for, and the weakest one in the file.
    /// Everything else here is written by the engine about facts it witnessed —
    /// a fill it booked, an order it sent, a limit that bound. A note is the
    /// strategy talking about itself, and `detail` is opaque, so nothing can
    /// contradict it the way folding the fills contradicts a bad `Checkpoint`.
    /// A strategy that writes nothing leaves a file that looks complete.
    ///
    /// The narrowing, not a fix: a strategy may periodically emit a note whose
    /// `kind` is `claim`, holding its own running totals. The notes between two
    /// claims must fold to the difference between them, so a **dropped** line is
    /// detectable. Both sides of that fold are written by the same strategy, so
    /// it catches a lost note and not a wrong belief. Conceded in
    /// `docs/run-log.md` §6 rather than answered.
    ///
    /// Carries no `fee`, no `cash`, no `realized` and no `fills` — the three
    /// defences that keep a decision line from being folded as money by someone
    /// with the wrong filter. `Checkpoint` stays the only claim about money.
    Note {
        at: Ts,
        kind: String,
        detail: serde_json::Value,
    },
    /// A session ended cleanly.
    ///
    /// Its absence is how a crash is told from a clean stop — the same
    /// two-sided-completeness idea as the raw container's trailer.
    Stopped { at: Ts },
}

impl JournalEntry {
    #[must_use]
    pub const fn at(&self) -> Ts {
        match self {
            Self::Started { at, .. }
            | Self::Filled { at, .. }
            | Self::Checkpoint { at, .. }
            | Self::Tripped { at, .. }
            | Self::Submitted { at, .. }
            | Self::Refused { at, .. }
            | Self::Accepted { at, .. }
            | Self::Rejected { at, .. }
            | Self::CancelRequested { at, .. }
            | Self::Cancelled { at, .. }
            | Self::Orphaned { at, .. }
            | Self::Blind { at, .. }
            | Self::Note { at, .. }
            | Self::Stopped { at } => *at,
        }
    }
}

impl JournalEntry {
    /// A checkpoint stating what `portfolio` currently believes.
    ///
    /// Built from the portfolio rather than from fields a caller assembles, so
    /// the claim and the thing being claimed cannot drift apart.
    #[must_use]
    pub fn checkpoint(at: Ts, portfolio: &Portfolio) -> Self {
        Self::Checkpoint {
            at,
            cash: portfolio.cash(),
            realized: portfolio.realized(),
            fees: portfolio.fees(),
            fills: portfolio.fills(),
        }
    }
}

/// Append-only writer.
#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    out: BufWriter<File>,
    written: u64,
}

impl Journal {
    /// Open for appending, creating it if absent.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            path: path.to_owned(),
            out: BufWriter::new(file),
            written: 0,
        })
    }

    /// Append one entry, and make sure it is on the disk before returning.
    ///
    /// Flushed and `sync_data`d per entry. That is expensive per call and
    /// irrelevant in aggregate — a few hundred entries a week — and it buys the
    /// only property this file has to have: an entry that has been acknowledged
    /// to the strategy is an entry that survives losing power. Buffering fills
    /// to batch the syncs would trade that away for nothing.
    pub fn append(&mut self, entry: &JournalEntry) -> std::io::Result<()> {
        let line = serde_json::to_string(entry).map_err(std::io::Error::other)?;
        writeln!(self.out, "{line}")?;
        self.out.flush()?;
        self.out.get_ref().sync_data()?;
        self.written += 1;
        Ok(())
    }

    #[must_use]
    pub const fn written(&self) -> u64 {
        self.written
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// What reading a journal back found.
#[derive(Debug, Default, Clone)]
pub struct Recovered {
    pub entries: Vec<JournalEntry>,
    /// A final line that was not valid JSON.
    ///
    /// Reported rather than treated as an error: a process killed mid-`write`
    /// leaves a partial last line, and that is a *torn tail* — the same
    /// distinction `quant-storage` draws between an interrupted write and
    /// corruption. Only the last line may be torn; a bad line anywhere else is
    /// an error, because nothing writes into the middle of an append-only file.
    pub torn_tail: bool,
}

/// Read a journal back.
///
/// A missing file is an empty journal, not an error: the first run of a session
/// has not written one yet.
pub fn read(path: &Path) -> std::io::Result<Recovered> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Recovered::default()),
        Err(e) => return Err(e),
    };
    let mut out = Recovered::default();
    let lines: Vec<String> = BufReader::new(file).lines().collect::<Result<_, _>>()?;
    let last = lines.len().saturating_sub(1);
    for (i, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<JournalEntry>(line) {
            Ok(entry) => out.entries.push(entry),
            Err(_) if i == last => out.torn_tail = true,
            Err(e) => {
                return Err(std::io::Error::other(format!(
                    "journal line {} is not readable, and only the last line may be torn: {e}",
                    i + 1
                )))
            }
        }
    }
    Ok(out)
}

/// Rebuild a portfolio from a journal alone.
///
/// This is the **independent recompute** in `docs/engine-contract.md` §9: it
/// shares no state with the engine's running portfolio and reaches the same
/// numbers by replaying what was written down. Two tallies of one quantity, in
/// the same shape as the venue and the portfolio each totalling fees, and
/// `quant-verify` and `quant-normalize` each counting frames.
///
/// Instruments are re-registered from `(exchange, symbol)`, so the ids this
/// produces are this process's ids and not the ones the journal was written
/// under — which is precisely why the pair is what gets persisted.
#[must_use]
pub fn replay(entries: &[JournalEntry], registry: &mut InstrumentRegistry) -> Portfolio {
    let starting = entries
        .iter()
        .find_map(|e| match e {
            JournalEntry::Started { cash, .. } => Some(*cash),
            _ => None,
        })
        .unwrap_or(Notional::ZERO);
    let mut portfolio = Portfolio::new(starting);

    for entry in entries {
        // Exhaustive, with no `_` arm, and that is the point rather than style.
        // M7.5 widens this enum from five variants to a dozen, and the one
        // failure that must be impossible is a new variant that silently fails
        // to move money -- a decision record quietly treated as a position
        // record, or the reverse. With a catch-all the compiler says nothing and
        // the defect surfaces as a reconciliation mismatch weeks later; without
        // one, adding a variant does not build until somebody has decided what
        // it does to the portfolio. A compile error instead of a paragraph.
        // `Orphaned` is kept as its own arm though its body matches the
        // others'. The arms are the same *consequence* and different
        // *decisions*: every other variant does nothing to the portfolio
        // because it is not money, and an orphan does nothing because it is
        // money we could not attribute. Merging them would delete the only
        // place that distinction is written down, and the next person to touch
        // this would have no reason not to fold it.
        #[allow(clippy::match_same_arms)]
        match entry {
            JournalEntry::Filled {
                instrument,
                side,
                px,
                qty,
                fee,
                is_maker,
                ..
            } => {
                let id = resolve(registry, instrument);
                portfolio.apply_fill(
                    id,
                    *side,
                    &Fill {
                        px: *px,
                        qty: *qty,
                        fee: *fee,
                        is_maker: *is_maker,
                    },
                );
            }
            // `Started` is read above, where the *first* one wins: a restart
            // must not reset the capital the session began with.
            JournalEntry::Started { .. }
            // A claim about the fold, never an input to it -- folding a
            // checkpoint back in would make the check compare itself.
            | JournalEntry::Checkpoint { .. }
            // Controls, not money.
            | JournalEntry::Tripped { .. }
            // Blindness is the absence of input, not a movement of money.
            | JournalEntry::Blind { .. }
            // The strategy talking about itself. Deliberately carries nothing a
            // money fold keys on, so it cannot be mistaken for one.
            | JournalEntry::Note { .. }
            | JournalEntry::Stopped { .. }
            // Decisions. None of them moves the portfolio: a submission is an
            // intent, a refusal is an intent that stopped at the chokepoint,
            // and an acceptance or a cancellation changes what is outstanding
            // rather than what is held. Only `Filled` is money.
            | JournalEntry::Submitted { .. }
            | JournalEntry::Refused { .. }
            | JournalEntry::Accepted { .. }
            | JournalEntry::Rejected { .. }
            | JournalEntry::CancelRequested { .. }
            | JournalEntry::Cancelled { .. } => {}
            // Deliberately *not* folded, and this is the one worth arguing. An
            // orphan is a fill the engine could not attribute, so the live
            // portfolio never saw it either -- folding it here would make the
            // recompute disagree with a `Checkpoint` that is faithfully
            // reporting what the engine believed. The disagreement would then
            // read as an accounting bug rather than as what it is: a fill we
            // cannot account for. It is recorded so a human can see it, and
            // `runlog check` is where it becomes loud.
            JournalEntry::Orphaned { .. } => {}
        }
    }
    portfolio
}

/// Whether a recompute agrees with what the engine claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Agreement {
    /// No checkpoint in the journal, so there is nothing to check against.
    ///
    /// Reported rather than treated as agreement: "nobody disagreed" and "two
    /// independent answers matched" are very different statements, and only one
    /// of them is evidence.
    NothingToCheck,
    Agrees,
    Differs {
        field: &'static str,
        claimed: Notional,
        recomputed: Notional,
    },
    /// The fill counts differ, which localises the problem: a fill was acted on
    /// and never written down, or written twice.
    FillCountDiffers {
        claimed: u64,
        recomputed: u64,
    },
}

/// Compare the journal's last checkpoint against a recompute of the entries it
/// describes.
///
/// The check in `docs/engine-contract.md` §9. The two numbers are computed by
/// different code from different inputs -- one accumulated in memory as fills
/// arrived, one replayed from a file afterwards -- which is what makes their
/// agreement mean something.
///
/// # A checkpoint is a claim about a *prefix*
///
/// This used to take an already-recomputed portfolio, and every caller replayed
/// the whole journal to produce it. That is correct only for a journal nothing
/// is writing to any more. `ops/verify-loop.sh` runs `reconcile` *during* a run,
/// against a file the engine is still appending to, so the fills after the last
/// checkpoint are ordinarily there -- and comparing a claim made at fill 5
/// against a replay of 8 fills reported `DISAGREES on fills: the engine counted
/// 5, the journal holds 8` on a session that was perfectly healthy.
///
/// Observed, not theorised: a five-minute rehearsal went `OK`, `FAIL`, `FAIL` on
/// consecutive passes as the journal grew. Over a fortnight that is a red
/// verdict every six hours from the first checkpoint onward -- the failure M1's
/// verifier already taught this project once, where a check that cries wolf on
/// good data is worse than no check because it trains everyone to ignore the
/// exit code.
///
/// So the replay happens *here*, over the entries up to and including the last
/// checkpoint, rather than being the caller's job to get right. `the_last_checkpoint_is_the_one_that_counts`
/// already made this argument for earlier checkpoints -- "an earlier one
/// describes an earlier state and comparing against it would fail on every
/// healthy run" -- and it is the same argument, applied to the other end.
///
/// A fresh registry is used for the recompute because only the aggregate money
/// and fill count are compared, and `a_journal_replays_the_same_through_any_registry`
/// pins that those do not depend on id assignment.
#[must_use]
pub fn agrees(entries: &[JournalEntry]) -> Agreement {
    let last_checkpoint = entries
        .iter()
        .rposition(|e| matches!(e, JournalEntry::Checkpoint { .. }));
    let Some(end) = last_checkpoint else {
        return Agreement::NothingToCheck;
    };
    let described = &entries[..=end];
    let recomputed = &replay(described, &mut InstrumentRegistry::new());

    let Some(JournalEntry::Checkpoint {
        cash,
        realized,
        fees,
        fills,
        ..
    }) = entries.get(end)
    else {
        return Agreement::NothingToCheck;
    };

    if *fills != recomputed.fills() {
        return Agreement::FillCountDiffers {
            claimed: *fills,
            recomputed: recomputed.fills(),
        };
    }
    for (field, claimed, actual) in [
        ("cash", *cash, recomputed.cash()),
        ("realized", *realized, recomputed.realized()),
        ("fees", *fees, recomputed.fees()),
    ] {
        if claimed != actual {
            return Agreement::Differs {
                field,
                claimed,
                recomputed: actual,
            };
        }
    }
    Agreement::Agrees
}

/// The shape this build writes.
pub const SCHEMA: u32 = 2;

/// What a journal with no `schema` field was: everything before M7.5.
const fn schema_v1() -> u32 {
    1
}

/// The next client order id a resumed run should mint.
///
/// `max(id) + 1` over every entry that carries one, or 1 for a journal that has
/// none. Derived from the ids rather than stored beside them, so there is
/// nothing that can disagree with the record.
///
/// Returns 1 for an empty journal, which is what a fresh run uses anyway — so a
/// caller does not have to special-case the first session.
#[must_use]
pub fn next_order_id(entries: &[JournalEntry]) -> u64 {
    entries
        .iter()
        .filter_map(|e| match e {
            // Every entry that carries an id contributes, not only fills: a
            // refused order consumed an id without ever producing one, and a
            // resumed run that reused it would make the hole check report holes
            // where there are none.
            JournalEntry::Filled {
                client_order_id, ..
            }
            | JournalEntry::Submitted {
                client_order_id, ..
            }
            | JournalEntry::Refused {
                client_order_id, ..
            }
            | JournalEntry::Accepted {
                client_order_id, ..
            }
            | JournalEntry::Rejected {
                client_order_id, ..
            }
            | JournalEntry::CancelRequested {
                client_order_id, ..
            }
            | JournalEntry::Cancelled {
                client_order_id, ..
            }
            | JournalEntry::Orphaned {
                client_order_id, ..
            } => Some(client_order_id.0),
            JournalEntry::Started { .. }
            | JournalEntry::Checkpoint { .. }
            | JournalEntry::Tripped { .. }
            // Consumes no id: nothing was decided, we simply could not see.
            | JournalEntry::Blind { .. }
            // Consumes no id either: a note describes a decision, and the
            // decision that minted an id is recorded by the engine beside it.
            | JournalEntry::Note { .. }
            | JournalEntry::Stopped { .. } => None,
        })
        .max()
        .map_or(1, |highest| highest + 1)
}

/// Find or register an instrument by its persisted identity.
fn resolve(registry: &mut InstrumentRegistry, key: &InstrumentKey) -> InstrumentId {
    use quant_core::instrument::{InstrumentDef, InstrumentKind};
    registry.register(InstrumentDef {
        exchange: key.exchange,
        symbol: key.symbol.clone(),
        // A journal records what we traded, not the venue's filters. Registering
        // is idempotent on `(exchange, symbol)`, so an instrument already known
        // to this process keeps the definition it was registered with.
        base: String::new(),
        quote: String::new(),
        kind: InstrumentKind::Spot,
        tick_size: Px::MIN_TICK,
        lot_size: Qty::MIN_TICK,
        min_notional: Notional::ZERO,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("quant-journal-{name}.jsonl"));
            let _ = std::fs::remove_file(&path);
            Self { path }
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn key() -> InstrumentKey {
        InstrumentKey {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
        }
    }

    fn amount(text: &str) -> Notional {
        text.parse().expect("a valid amount")
    }

    fn filled(seq: u64, side: Side, px: &str, qty: &str, fee: &str) -> JournalEntry {
        JournalEntry::Filled {
            at: Ts::from_nanos(i64::try_from(seq).expect("small")),
            client_order_id: ClientOrderId(seq),
            instrument: key(),
            side,
            px: px.parse().expect("px"),
            qty: qty.parse().expect("qty"),
            fee: fee.parse().expect("fee"),
            is_maker: false,
        }
    }

    fn session() -> Vec<JournalEntry> {
        vec![
            JournalEntry::Started {
                at: Ts::from_nanos(0),
                cash: amount("100"),
                schema: SCHEMA,
            },
            filled(1, Side::Buy, "76650", "0.001", "0.0766"),
            filled(2, Side::Sell, "76700", "0.001", "0.0767"),
            JournalEntry::Stopped {
                at: Ts::from_nanos(3),
            },
        ]
    }

    fn write_all(path: &Path, entries: &[JournalEntry]) {
        let mut journal = Journal::open(path).expect("open");
        for entry in entries {
            journal.append(entry).expect("append");
        }
    }

    #[test]
    fn a_journal_round_trips() {
        let scratch = Scratch::new("round-trip");
        write_all(&scratch.path, &session());
        let recovered = read(&scratch.path).expect("read");
        assert_eq!(recovered.entries, session());
        assert!(!recovered.torn_tail);
    }

    #[test]
    fn a_missing_journal_is_empty_rather_than_an_error() {
        // The first run of a session has not written one yet.
        let scratch = Scratch::new("absent");
        let recovered = read(&scratch.path).expect("a missing file is fine");
        assert!(recovered.entries.is_empty(), "{:?}", recovered.entries);
    }

    #[test]
    fn appending_to_an_existing_journal_keeps_what_was_there() {
        // A restart appends; it does not start a new file. Truncating would lose
        // the position the restart exists to recover.
        let scratch = Scratch::new("append");
        write_all(&scratch.path, &session());
        write_all(
            &scratch.path,
            &[JournalEntry::Started {
                at: Ts::from_nanos(10),
                cash: amount("64"),
                schema: SCHEMA,
            }],
        );
        let recovered = read(&scratch.path).expect("read");
        assert_eq!(recovered.entries.len(), 5);
    }

    #[test]
    fn the_first_started_entry_is_the_authority_on_starting_capital() {
        // A restart writes its own Started with whatever it had. The number a
        // report quotes has to be what the *session* began with, or a run that
        // lost money would look like it started poorer.
        let mut restarted = session();
        restarted.push(JournalEntry::Started {
            at: Ts::from_nanos(10),
            cash: amount("64"),
            schema: SCHEMA,
        });
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&restarted, &mut registry);
        assert_eq!(portfolio.starting_cash(), amount("100"));
    }

    #[test]
    fn a_recomputed_portfolio_matches_one_built_by_applying_the_same_fills() {
        // The independent recompute. It shares no state with a running engine
        // and has to reach the same numbers -- two tallies of one quantity, the
        // same shape as the venue and the portfolio each totalling fees.
        let entries = session();
        let mut registry = InstrumentRegistry::new();
        let recomputed = replay(&entries, &mut registry);

        let mut direct = Portfolio::new(amount("100"));
        let id = resolve(&mut registry, &key());
        direct.apply_fill(
            id,
            Side::Buy,
            &Fill {
                px: "76650".parse().expect("px"),
                qty: "0.001".parse().expect("qty"),
                fee: amount("0.0766"),
                is_maker: false,
            },
        );
        direct.apply_fill(
            id,
            Side::Sell,
            &Fill {
                px: "76700".parse().expect("px"),
                qty: "0.001".parse().expect("qty"),
                fee: amount("0.0767"),
                is_maker: false,
            },
        );

        assert_eq!(recomputed.cash(), direct.cash());
        assert_eq!(recomputed.realized(), direct.realized());
        assert_eq!(recomputed.fees(), direct.fees());
        assert_eq!(recomputed.position(id), direct.position(id));
    }

    #[test]
    fn a_round_trip_through_the_journal_ends_flat_and_pays_its_fees() {
        // The arithmetic, checked end to end: bought and sold one unit 50 apart,
        // so 0.05 of gross gain against 0.1533 of fees.
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&session(), &mut registry);
        assert!(portfolio.position(resolve(&mut registry, &key())).is_flat());
        assert_eq!(portfolio.realized(), amount("0.05"));
        assert_eq!(portfolio.fees(), amount("0.1533"));
        assert_eq!(portfolio.cash(), amount("99.8967"));
    }

    #[test]
    fn a_torn_last_line_is_reported_rather_than_failing() {
        // A process killed mid-write leaves a partial last line. Same
        // distinction quant-storage draws between an interrupted write and
        // corruption -- and a paper run must come back up after a hard kill.
        let scratch = Scratch::new("torn");
        write_all(&scratch.path, &session());
        let mut bytes = std::fs::read(&scratch.path).expect("read");
        bytes.extend_from_slice(b"{\"type\":\"fil");
        std::fs::write(&scratch.path, &bytes).expect("write");

        let recovered = read(&scratch.path).expect("a torn tail is not an error");
        assert!(recovered.torn_tail);
        assert_eq!(
            recovered.entries,
            session(),
            "everything before it survives"
        );
    }

    #[test]
    fn a_bad_line_in_the_middle_is_an_error() {
        // Nothing writes into the middle of an append-only file, so this is
        // damage rather than an interrupted write -- and silently skipping it
        // would drop a fill and quietly change the position.
        let scratch = Scratch::new("corrupt-middle");
        write_all(&scratch.path, &session());
        let mut text = std::fs::read_to_string(&scratch.path).expect("read");
        text.push_str("not json\n");
        text.push_str("{\"type\":\"stopped\",\"at\":9}\n");
        std::fs::write(&scratch.path, text).expect("write");

        let err = read(&scratch.path).expect_err("damage must be loud");
        assert!(err.to_string().contains("only the last line may be torn"));
    }

    #[test]
    fn an_instrument_is_named_by_exchange_and_symbol_not_by_index() {
        // M0's rule. An id written on Tuesday means something else on Wednesday
        // if the registration order changed -- so the journal must survive being
        // replayed by a process that registered things in a different order.
        let entries = session();

        let mut first = InstrumentRegistry::new();
        let a = replay(&entries, &mut first);

        let mut second = InstrumentRegistry::new();
        // Register something else first, so the same symbol gets a different id.
        let _ = resolve(
            &mut second,
            &InstrumentKey {
                exchange: Exchange::Binance,
                symbol: "ETHUSDT".to_owned(),
            },
        );
        let b = replay(&entries, &mut second);

        assert_eq!(a.cash(), b.cash());
        assert_eq!(a.realized(), b.realized());
        let id_a = resolve(&mut first, &key());
        let id_b = resolve(&mut second, &key());
        assert_ne!(id_a, id_b, "the ids really do differ between processes");
        assert_eq!(a.position(id_a), b.position(id_b));
    }

    #[test]
    fn an_empty_journal_recomputes_to_nothing_rather_than_guessing() {
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&[], &mut registry);
        assert_eq!(portfolio.starting_cash(), Notional::ZERO);
        assert_eq!(portfolio.fills(), 0);
    }
    #[test]
    fn a_checkpoint_that_matches_the_recompute_agrees() {
        // Two independent computations of one quantity: the engine accumulated
        // its numbers in memory, this replayed them from the file.
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&entries, &mut registry);
        entries.push(JournalEntry::checkpoint(Ts::from_nanos(4), &portfolio));

        assert_eq!(agrees(&entries), Agreement::Agrees);
    }

    #[test]
    fn a_journal_with_no_checkpoint_reports_nothing_to_check() {
        // Deliberately not agreement. "Nobody disagreed" and "two answers
        // matched" are different statements and only one of them is evidence.
        assert_eq!(agrees(&session()), Agreement::NothingToCheck);
    }

    #[test]
    fn a_checkpoint_claiming_the_wrong_cash_disagrees() {
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&entries, &mut registry);
        entries.push(JournalEntry::Checkpoint {
            at: Ts::from_nanos(4),
            cash: amount("999"),
            realized: portfolio.realized(),
            fees: portfolio.fees(),
            fills: portfolio.fills(),
        });
        assert_eq!(
            agrees(&entries),
            Agreement::Differs {
                field: "cash",
                claimed: amount("999"),
                recomputed: portfolio.cash(),
            }
        );
    }

    #[test]
    fn a_fill_acted_on_but_never_written_down_is_caught() {
        // The failure this whole entry exists for: the engine traded, the line
        // did not reach the disk, and the position on restart would be wrong.
        // Checked before the money fields, because the fill count localises it.
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&entries, &mut registry);
        entries.push(JournalEntry::Checkpoint {
            at: Ts::from_nanos(4),
            cash: portfolio.cash(),
            realized: portfolio.realized(),
            fees: portfolio.fees(),
            fills: portfolio.fills() + 1,
        });
        assert_eq!(
            agrees(&entries),
            Agreement::FillCountDiffers {
                claimed: portfolio.fills() + 1,
                recomputed: portfolio.fills(),
            }
        );
    }

    #[test]
    fn fills_after_the_last_checkpoint_are_not_a_disagreement() {
        // The false alarm that would have turned a fortnight red every six
        // hours. `ops/verify-loop.sh` runs `reconcile` *during* a run, against a
        // journal the engine is still appending to, so fills after the last
        // checkpoint are the ordinary state of a healthy live file -- not
        // evidence that one was acted on and never written down.
        //
        // Seen for real before it was fixed: a five-minute rehearsal reported
        // OK, then "DISAGREES on fills: the engine counted 5, the journal holds
        // 8", then "counted 10, holds 11", on consecutive passes as the file
        // grew. The claim was right, the recompute was right, and the comparison
        // was between a claim about fill 5 and a replay of eight.
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let at_checkpoint = replay(&entries, &mut registry);
        entries.push(JournalEntry::checkpoint(Ts::from_nanos(4), &at_checkpoint));

        // The run carries on. These are good fills, correctly written down.
        let more = session();
        entries.extend(
            more.into_iter()
                .filter(|e| matches!(e, JournalEntry::Filled { .. })),
        );
        assert!(
            replay(&entries, &mut registry).fills() > at_checkpoint.fills(),
            "the journal really has grown past the checkpoint"
        );

        assert_eq!(agrees(&entries), Agreement::Agrees);
    }

    #[test]
    fn the_last_checkpoint_is_the_one_that_counts() {
        // A fortnight writes many. An earlier one describes an earlier state and
        // comparing against it would fail on every healthy run.
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let mid = replay(&entries, &mut registry);
        entries.push(JournalEntry::Checkpoint {
            at: Ts::from_nanos(4),
            cash: amount("1"),
            realized: amount("1"),
            fees: amount("1"),
            fills: 99,
        });
        entries.push(JournalEntry::checkpoint(Ts::from_nanos(5), &mid));
        assert_eq!(agrees(&entries), Agreement::Agrees);
    }

    #[test]
    fn a_checkpoint_is_built_from_the_portfolio_it_describes() {
        // Built from the portfolio rather than from fields a caller assembles,
        // so the claim and the thing claimed cannot drift apart.
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&session(), &mut registry);
        let JournalEntry::Checkpoint {
            cash,
            realized,
            fees,
            fills,
            ..
        } = JournalEntry::checkpoint(Ts::from_nanos(1), &portfolio)
        else {
            panic!("expected a checkpoint");
        };
        assert_eq!(cash, portfolio.cash());
        assert_eq!(realized, portfolio.realized());
        assert_eq!(fees, portfolio.fees());
        assert_eq!(fills, portfolio.fills());
    }
}

#[cfg(test)]
mod m75a_tests {
    use super::{next_order_id, read, replay, JournalEntry, SCHEMA};
    use quant_core::execution::ClientOrderId;
    use quant_core::instrument::InstrumentRegistry;
    use quant_core::time::Ts;
    use std::io::Write as _;

    /// A journal written before M7.5 existed, byte for byte.
    const PRE_M75: &str = concat!(
        r#"{"type":"started","at":0,"cash":10000000000}"#,
        "\n",
        r#"{"type":"filled","at":1000,"client_order_id":1,"instrument":{"exchange":"binance","symbol":"BTCUSDT"},"side":"buy","px":10000000000,"qty":100000,"fee":1000,"is_maker":false}"#,
        "\n",
    );

    fn temp(name: &str, body: &str) -> std::path::PathBuf {
        let thread = std::thread::current();
        let unique = thread.name().unwrap_or("unnamed").replace("::", "-");
        let path = std::env::temp_dir().join(format!("quant-m75a-{name}-{unique}.jsonl"));
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(body.as_bytes()).expect("write");
        path
    }

    #[test]
    fn a_journal_written_before_the_schema_field_still_reads() {
        // The compatibility the `serde(default)` buys, pinned rather than
        // assumed. The fortnight's two journals are this shape, and they are
        // the only live evidence M5 ever produced — a reader that could not
        // open them would make the milestone's result unverifiable.
        let recovered = read(&temp("pre-m75", PRE_M75)).expect("readable");
        assert_eq!(recovered.entries.len(), 2);
        let JournalEntry::Started { schema, cash, .. } = &recovered.entries[0] else {
            panic!("the first entry is a start")
        };
        assert_eq!(*schema, 1, "absent means the shape before M7.5");
        assert_eq!(cash.to_string(), "100");
    }

    #[test]
    fn the_schema_this_build_writes_is_not_the_one_it_defaults_to() {
        // Otherwise the field is decoration: every journal would report the
        // same number whether or not the writer knew about it, and the next
        // change would have nothing to refuse at.
        assert_ne!(SCHEMA, 1);
    }

    #[test]
    fn ids_continue_across_a_restart_rather_than_repeating() {
        // `next_id` resets to 1 with the process, and `resuming` replaces only
        // the portfolio. Without recovery a resumed run mints id 1 again and the
        // file holds two different orders claiming it — which would make M7.5's
        // hole check report holes that are not there, the "cries wolf on good
        // data" failure the verifier already taught this project once.
        let entries = vec![
            JournalEntry::Started {
                at: Ts::from_nanos(1),
                cash: "100".parse().expect("amount"),
                schema: SCHEMA,
            },
            filled(7),
            filled(9),
        ];
        assert_eq!(next_order_id(&entries), 10, "max + 1, not a count");
    }

    #[test]
    fn an_empty_journal_mints_from_one() {
        // So a first session needs no special case.
        assert_eq!(next_order_id(&[]), 1);
    }

    #[test]
    fn the_id_recovered_is_the_highest_not_the_last() {
        // Entries are appended in time order, but ids are only dense, not
        // sorted: a fill for an older order can land after a newer one once
        // latency is on. Taking the last would then hand out an id already used.
        let entries = vec![filled(9), filled(7)];
        assert_eq!(next_order_id(&entries), 10);
    }

    #[test]
    fn replaying_a_journal_ignores_everything_that_is_not_a_fill() {
        // The exhaustive match's behaviour, stated as a property rather than
        // left to the compiler: a checkpoint is a *claim about* the fold and
        // folding it back in would make the check compare itself.
        let mut registry = InstrumentRegistry::new();
        let fills_only = vec![filled(1)];
        let with_noise = vec![
            filled(1),
            JournalEntry::Checkpoint {
                at: Ts::from_nanos(5),
                cash: "999".parse().expect("amount"),
                realized: "999".parse().expect("amount"),
                fees: "999".parse().expect("amount"),
                fills: 99,
            },
            JournalEntry::Stopped {
                at: Ts::from_nanos(6),
            },
        ];
        let a = replay(&fills_only, &mut registry);
        let b = replay(&with_noise, &mut InstrumentRegistry::new());
        assert_eq!(a.cash(), b.cash());
        assert_eq!(a.fills(), b.fills());
    }

    fn filled(id: u64) -> JournalEntry {
        JournalEntry::Filled {
            at: Ts::from_nanos(i64::try_from(id).expect("small") * 1_000),
            client_order_id: ClientOrderId(id),
            instrument: super::InstrumentKey {
                exchange: quant_core::instrument::Exchange::Binance,
                symbol: "BTCUSDT".to_owned(),
            },
            side: quant_core::event::Side::Buy,
            px: "100".parse().expect("px"),
            qty: "0.001".parse().expect("qty"),
            fee: "0.0001".parse().expect("fee"),
            is_maker: false,
        }
    }
}
