//! What was the system doing at a given instant.
//!
//! ```text
//! explain <DATA_ROOT> --at 2026-09-18T14:46:54Z --symbol BTCUSDT
//! ```
//!
//! # Why a reader, and not a recorder
//!
//! `docs/observability.md` carries the argument. In short: the market state at an
//! instant is a pure function of the raw capture, and our position is a pure
//! function of the journal, so neither needs to be written down a second time —
//! and a second copy that can disagree with the artifacts is the failure this
//! project refuses. What is genuinely lost at process exit (orders that never
//! filled, refusals, strategy state) cannot be recovered by *any* reader, and is
//! the next milestone's obligation rather than this one's.
//!
//! The defect being fixed is not missing data. It is that nothing here has a
//! notion of a **moment**: every existing tool is either present-tense
//! (`ops/status.sh`) or whole-run aggregate (`dump`, `normalize`, `verify`,
//! `reconcile`). This supplies the cursor.
//!
//! # This module writes nothing
//!
//! No index, no cache, no sidecar — see the crate manifest. Re-deriving from raw
//! costs about six seconds per symbol-day and does not grow with the run's
//! length, because the day partition bounds it.
//!
//! # `--at` is `local_recv_ts`
//!
//! Invariant 2: the engine orders and dispatches on `local_recv_ts` only.
//! Seeking on `exchange_ts` would answer a question the engine never asked, and
//! would quietly disagree with it about which events preceded a decision.

use std::path::{Path, PathBuf};

use quant_book::Book;
use quant_core::event::{GapCause, Level, MarketEvent};
use quant_core::instrument::{Exchange, InstrumentId};
use quant_core::time::Ts;
use quant_normalize::{ReplayItem, SessionReplay};
use quant_recorder::{catalog, SessionFiles};

pub mod checkpoints;
pub mod health;
pub mod ours;

#[cfg(test)]
mod tests;

pub use checkpoints::check_all;
pub use health::{health_at, HealthAt, NoHealth};
pub use ours::{ours_at, OurState};

/// The market as the recording says it was, at an instant.
#[derive(Debug)]
pub struct MarketAt {
    /// The instant asked for.
    pub at: Ts,
    /// The capture session the answer was read from.
    pub session_id: [u8; 16],
    /// Segments actually opened, for the provenance block.
    pub segments: Vec<PathBuf>,
    /// The last event at or before `at`, which is what "as of" means here.
    pub last_event_ts: Option<Ts>,
    /// `None` when the book could not be established — never a stale one.
    pub book: Option<BookAt>,
    /// The last trade at or before `at`.
    pub last_trade: Option<TradeAt>,
    /// The most recent gap at or before `at`, and the next one after.
    pub gap_before: Option<GapAt>,
    pub gap_after: Option<GapAt>,
    /// Why there is no book, when there is none. Always set when `book` is
    /// `None`, because a blank is indistinguishable from "nothing happened".
    pub no_book_reason: Option<String>,
    /// Events read to get here, for the report.
    pub events_read: u64,
}

/// The touch, and how deep the book was.
#[derive(Debug, Clone)]
pub struct BookAt {
    pub bid: Option<Level>,
    pub ask: Option<Level>,
    pub bid_levels: usize,
    pub ask_levels: usize,
}

impl BookAt {
    /// The mid, when both sides exist.
    ///
    /// Returned rather than stored, so it cannot drift from the touch it is
    /// derived from.
    #[must_use]
    pub fn mid(&self) -> Option<quant_core::fixed::Px> {
        let (bid, ask) = (self.bid.as_ref()?, self.ask.as_ref()?);
        Some(quant_core::fixed::Px::from_raw(
            (bid.px.raw() + ask.px.raw()) / 2,
        ))
    }

    /// The spread, when both sides exist.
    #[must_use]
    pub fn spread(&self) -> Option<quant_core::fixed::Px> {
        let (bid, ask) = (self.bid.as_ref()?, self.ask.as_ref()?);
        Some(quant_core::fixed::Px::from_raw(ask.px.raw() - bid.px.raw()))
    }
}

/// A trade, with how long before the instant it happened.
#[derive(Debug, Clone)]
pub struct TradeAt {
    pub at: Ts,
    pub px: quant_core::fixed::Px,
    pub qty: quant_core::fixed::Qty,
    pub aggressor: quant_core::event::Side,
}

/// A recorded blindness, and which side of the instant it falls.
#[derive(Debug, Clone)]
pub struct GapAt {
    pub at: Ts,
    pub cause: GapCause,
}

/// Why no answer could be given at all.
#[derive(Debug)]
pub enum ExplainError {
    /// Nothing under this root covers that symbol at that instant.
    NoCapture { symbol: String, at: Ts },
    /// The root holds no capture at all, or none this build can read.
    NoSessions { root: PathBuf },
}

impl core::fmt::Display for ExplainError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NoCapture { symbol, at } => write!(
                f,
                "no capture of {symbol} covers {} -- the run may not have been \
                 going, or its files are not under this root",
                at.to_rfc3339()
            ),
            Self::NoSessions { root } => {
                write!(f, "no capture sessions found under {}", root.display())
            }
        }
    }
}

impl std::error::Error for ExplainError {}

/// Reconstruct the market at `at`, from raw.
///
/// # How the day is chosen, and why the previous one is sometimes read too
///
/// The capture is partitioned by UTC day, so the instant's own day is the
/// obvious place to look and is usually enough. It is not always: a day's file
/// begins mid-stream, and the book stays unanchored until the next snapshot —
/// which M2.c measured at up to an hour, because the recorder's periodic anchor
/// is hourly. An instant in that window would get no book at all.
///
/// So the rule is self-correcting rather than fixed: read the instant's day; if
/// the book is still unanchored on arrival, read again including the previous
/// day, which guarantees an anchor within the hour. Doing it in that order means
/// the ordinary query pays for one day and only the boundary case pays for two.
///
/// Reading *always* from the previous day would be simpler and is the wrong
/// trade: it doubles the cost of every query to fix a minority of them.
///
/// # Errors
///
/// [`ExplainError`] when no session under `root` covers that symbol and instant.
pub fn market_at(
    root: &Path,
    exchange: Exchange,
    symbol: &str,
    instrument: InstrumentId,
    at: Ts,
) -> Result<MarketAt, ExplainError> {
    let found = catalog(root);
    if found.sessions.is_empty() {
        return Err(ExplainError::NoSessions {
            root: root.to_owned(),
        });
    }

    let day = at.utc_date();
    let sessions: Vec<&SessionFiles> = found
        .sessions
        .iter()
        .filter(|s| s.exchange == exchange && s.symbol == symbol)
        .collect();
    if sessions.is_empty() {
        return Err(ExplainError::NoCapture {
            symbol: symbol.to_owned(),
            at,
        });
    }

    // The session whose segments cover the instant's day. More than one can, if
    // a recorder restarted inside it (M2.e) -- each is tried in catalog order
    // and the first that yields events at or before `at` answers, because a
    // later session's segments begin after the earlier one ended.
    let mut best: Option<MarketAt> = None;
    for files in sessions {
        let covers = files.segments.iter().any(|s| s.target.date == day);
        if !covers {
            continue;
        }
        let answer = scan(files, instrument, at, false);
        // An unanchored book is the boundary case the previous day fixes.
        let answer = if answer.book.is_none() && answer.events_read > 0 {
            scan(files, instrument, at, true)
        } else {
            answer
        };
        if answer.events_read == 0 {
            continue;
        }
        let better = best
            .as_ref()
            .is_none_or(|b| answer.last_event_ts > b.last_event_ts);
        if better {
            best = Some(answer);
        }
    }

    best.ok_or_else(|| ExplainError::NoCapture {
        symbol: symbol.to_owned(),
        at,
    })
}

/// Replay one session up to `at`, optionally including the preceding day.
fn scan(files: &SessionFiles, instrument: InstrumentId, at: Ts, widen: bool) -> MarketAt {
    let day = at.utc_date();
    let previous = quant_core::time::UtcDate::from_days_since_epoch(day.days_since_epoch() - 1);

    // A subset of a session's segments, which is safe *because* `SessionReplay`
    // opens with no previous sequence: starting mid-session produces no spurious
    // hole, since the first frame it sees establishes the baseline rather than
    // being compared against one.
    let wanted: Vec<_> = files
        .segments
        .iter()
        .filter(|s| s.target.date == day || (widen && s.target.date == previous))
        .cloned()
        .collect();
    let subset = SessionFiles {
        exchange: files.exchange,
        symbol: files.symbol.clone(),
        session_id: files.session_id,
        segments: wanted.clone(),
    };

    let mut out = MarketAt {
        at,
        session_id: files.session_id,
        segments: wanted.iter().map(|s| s.path.clone()).collect(),
        last_event_ts: None,
        book: None,
        last_trade: None,
        gap_before: None,
        gap_after: None,
        no_book_reason: None,
        events_read: 0,
    };
    if wanted.is_empty() {
        out.no_book_reason = Some("no segment covers that day".to_owned());
        return out;
    }

    let mut book = Book::new();
    let mut unreadable: Option<String> = None;
    for item in SessionReplay::open(&subset, instrument) {
        match item {
            ReplayItem::Break(brk) => {
                // The archive itself is discontinuous. The book must be
                // invalidated for the same reason `replay_session` does it:
                // continuing would hand out a book built across data we could
                // not read.
                book.invalidate();
                unreadable = Some(format!("{:?}", brk.kind));
            }
            ReplayItem::Event(event) => {
                let ts = event.meta().local_recv_ts;
                if ts > at {
                    // One event past the instant, kept only to answer "when does
                    // the blindness end".
                    if let MarketEvent::Gap(gap) = &event {
                        if out.gap_after.is_none() {
                            out.gap_after = Some(GapAt {
                                at: ts,
                                cause: gap.cause,
                            });
                        }
                    }
                    if out.gap_after.is_some() {
                        break;
                    }
                    continue;
                }
                out.events_read += 1;
                out.last_event_ts = Some(ts);
                book.apply(&event);
                match &event {
                    MarketEvent::Trade(trade) => {
                        out.last_trade = Some(TradeAt {
                            at: ts,
                            px: trade.px,
                            qty: trade.qty,
                            aggressor: trade.aggressor,
                        });
                    }
                    MarketEvent::Gap(gap) => {
                        out.gap_before = Some(GapAt {
                            at: ts,
                            cause: gap.cause,
                        });
                    }
                    MarketEvent::BookDelta(_) | MarketEvent::BookSnapshot(_) => {}
                }
            }
        }
    }

    settle_book(&mut out, &book, unreadable.as_deref());
    out
}

/// Record the book, or record why there is none.
///
/// Extracted because it is the point of the whole type and deserves a name: a
/// book that is not established is reported as **absent with a reason**, never
/// as its last known state. M2.b clears an invalid book rather than flagging it,
/// "because a flag can be ignored" -- so there is no stale state here to print
/// even by accident, and this only has to avoid inventing one.
///
/// Every branch produces a reason. A blank is indistinguishable from "nothing
/// happened", and for a tool whose characteristic failure is confabulation, an
/// unexplained absence is the same mistake in a quieter voice.
fn settle_book(out: &mut MarketAt, book: &Book, unreadable: Option<&str>) {
    if book.is_live() {
        let (bid_levels, ask_levels) = book.depth();
        out.book = Some(BookAt {
            bid: book.best_bid(),
            ask: book.best_ask(),
            bid_levels,
            ask_levels,
        });
        return;
    }
    out.no_book_reason = Some(match (unreadable, &out.gap_before) {
        (Some(kind), _) => format!("the archive is discontinuous here ({kind})"),
        (None, Some(gap)) => format!(
            "blind since {} ({:?}), and no snapshot has re-anchored the book yet",
            gap.at.to_rfc3339(),
            gap.cause
        ),
        (None, None) if out.events_read == 0 => {
            "no events at or before this instant in that day's capture".to_owned()
        }
        (None, None) => "the book had not been anchored yet -- the first snapshot of this \
             stretch arrives later"
            .to_owned(),
    });
}
