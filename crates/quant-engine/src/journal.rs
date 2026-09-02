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

use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, Fill};
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
    Started { at: Ts, cash: Notional },
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
        if let JournalEntry::Filled {
            instrument,
            side,
            px,
            qty,
            fee,
            is_maker,
            ..
        } = entry
        {
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

/// Compare a recomputed portfolio against the journal's last checkpoint.
///
/// The check in `docs/engine-contract.md` §9. The two numbers are computed by
/// different code from different inputs -- one accumulated in memory as fills
/// arrived, one replayed from a file afterwards -- which is what makes their
/// agreement mean something.
#[must_use]
pub fn agrees(entries: &[JournalEntry], recomputed: &Portfolio) -> Agreement {
    let Some(JournalEntry::Checkpoint {
        cash,
        realized,
        fees,
        fills,
        ..
    }) = entries
        .iter()
        .rev()
        .find(|e| matches!(e, JournalEntry::Checkpoint { .. }))
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
        assert!(recovered.entries.is_empty());
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

        assert_eq!(agrees(&entries, &portfolio), Agreement::Agrees);
    }

    #[test]
    fn a_journal_with_no_checkpoint_reports_nothing_to_check() {
        // Deliberately not agreement. "Nobody disagreed" and "two answers
        // matched" are different statements and only one of them is evidence.
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&session(), &mut registry);
        assert_eq!(agrees(&session(), &portfolio), Agreement::NothingToCheck);
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
            agrees(&entries, &portfolio),
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
            agrees(&entries, &portfolio),
            Agreement::FillCountDiffers {
                claimed: portfolio.fills() + 1,
                recomputed: portfolio.fills(),
            }
        );
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
        assert_eq!(agrees(&entries, &mid), Agreement::Agrees);
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
