//! What *we* were doing at an instant, from the journal.
//!
//! The counterpart to the market side. Where that reconstructs the venue from
//! raw, this reconstructs our own position and P&L from the journal — the only
//! durable record of what we actually did, and deliberately a file rather than
//! the metadata tier, because M1.c2 made Postgres best-effort and optional and a
//! position that must survive a restart cannot depend on something optional.
//!
//! # It calls the engine's own accounting, and does not reimplement it
//!
//! `journal::replay` folds the fill lines through the real `Portfolio`, so
//! average cost, realized P&L and fees are computed here by exactly the code
//! that computed them live. The alternative — a `jq` fold, or a second
//! accounting written here — is the thing this project has refused four times,
//! and it would be worse than usual: `Portfolio` does average-cost arithmetic in
//! fixed point, so a reimplementation would agree right up until the position
//! changed sign.

use std::path::{Path, PathBuf};

use quant_core::fixed::{Notional, Px};
use quant_core::instrument::{InstrumentId, InstrumentRegistry};
use quant_core::time::Ts;
use quant_engine::journal::{self, JournalEntry};
use quant_engine::Portfolio;

/// Our own state at an instant, as the journal tells it.
#[derive(Debug)]
pub struct OurState {
    /// The journal read.
    pub journal: PathBuf,
    /// Entries at or before the instant. The whole file is read, then truncated
    /// to the prefix that had happened — a journal is hundreds of lines a week,
    /// so seeking would buy nothing and cost the torn-tail tolerance `read`
    /// already has.
    pub entries_considered: usize,
    /// `None` when the journal does not exist or holds nothing yet.
    pub portfolio: Option<Portfolio>,
    /// The most recent fill at or before the instant, and the next one after.
    pub last_fill: Option<FillAt>,
    pub next_fill: Option<FillAt>,
    /// Set when the session had not started, or had already stopped.
    pub outside_session: Option<String>,
    /// A checkpoint at exactly this instant, if there is one — what the engine
    /// *believed* at the time, as opposed to what the file replays to. The pair
    /// is what makes the agreement check independent rather than vacuous.
    pub checkpoint_here: Option<Checkpoint>,
}

/// A fill, and which side of the instant it falls.
#[derive(Debug, Clone)]
pub struct FillAt {
    pub at: Ts,
    pub side: quant_core::event::Side,
    pub px: Px,
    pub qty: quant_core::fixed::Qty,
    pub fee: Notional,
    /// The journal's `client_order_id`.
    ///
    /// **A fill ordinal, not the engine's order id.** `paper.rs` writes
    /// `ClientOrderId(self.fills)` because `FillObserver::on_fill` is never
    /// handed the real one, and refused orders consume an id before the risk
    /// check — so the first refusal desynchronises it permanently. Carried and
    /// labelled rather than printed as an id: an unlabelled wrong number is M4's
    /// confidently-wrong-report failure in miniature.
    pub fill_ordinal: u64,
}

/// What the engine claimed to believe, written down at the time.
#[derive(Debug, Clone, Copy)]
pub struct Checkpoint {
    pub at: Ts,
    pub cash: Notional,
    pub realized: Notional,
    pub fees: Notional,
    pub fills: u64,
}

/// Reconstruct our state at `at` from a journal file.
///
/// # Errors
///
/// Propagates an unreadable journal. A *missing* one is not an error — a run
/// that never traded has none, and reporting that as a failure would make the
/// ordinary case look broken.
pub fn ours_at(
    journal_path: &Path,
    instrument: InstrumentId,
    registry: &mut InstrumentRegistry,
    at: Ts,
) -> std::io::Result<OurState> {
    let recovered = journal::read(journal_path)?;
    let mut out = OurState {
        journal: journal_path.to_owned(),
        entries_considered: 0,
        portfolio: None,
        last_fill: None,
        next_fill: None,
        outside_session: None,
        checkpoint_here: None,
    };
    if recovered.entries.is_empty() {
        out.outside_session = Some("the journal is empty or absent".to_owned());
        return Ok(out);
    }

    // `Started.at` is hard-coded to zero in `paper.rs`, so a journal cannot date
    // its own beginning -- see docs/observability.md. The first *fill* is the
    // earliest instant the journal can speak for, and anything before it is
    // reported as such rather than answered with a zero position that looks
    // observed.
    let first_event = recovered.entries.iter().find_map(entry_ts);
    if let Some(first) = first_event {
        if at < first {
            out.outside_session = Some(format!(
                "before this journal's first entry at {} -- nothing had happened yet",
                first.to_rfc3339()
            ));
        }
    }
    if let Some(JournalEntry::Stopped { at: stopped }) = recovered
        .entries
        .iter()
        .rev()
        .find(|e| matches!(e, JournalEntry::Stopped { .. }))
    {
        if at > *stopped {
            out.outside_session = Some(format!(
                "after the session stopped at {}",
                stopped.to_rfc3339()
            ));
        }
    }

    let prefix: Vec<JournalEntry> = recovered
        .entries
        .iter()
        .filter(|e| entry_ts(e).is_none_or(|ts| ts <= at))
        .cloned()
        .collect();
    out.entries_considered = prefix.len();
    out.portfolio = Some(journal::replay(&prefix, registry));

    for entry in &recovered.entries {
        match entry {
            JournalEntry::Filled {
                at: ts,
                side,
                px,
                qty,
                fee,
                client_order_id,
                ..
            } => {
                let fill = FillAt {
                    at: *ts,
                    side: *side,
                    px: *px,
                    qty: *qty,
                    fee: *fee,
                    fill_ordinal: client_order_id.0,
                };
                if *ts <= at {
                    out.last_fill = Some(fill);
                } else if out.next_fill.is_none() {
                    out.next_fill = Some(fill);
                }
            }
            JournalEntry::Checkpoint {
                at: ts,
                cash,
                realized,
                fees,
                fills,
            } if *ts == at => {
                out.checkpoint_here = Some(Checkpoint {
                    at: *ts,
                    cash: *cash,
                    realized: *realized,
                    fees: *fees,
                    fills: *fills,
                });
            }
            _ => {}
        }
    }

    let _ = instrument;
    Ok(out)
}

/// The instant an entry describes, where it has a meaningful one.
///
/// `Started` returns `None` rather than its stored zero: filtering on a hard-
/// coded epoch would drop the starting cash from every prefix and silently reset
/// the account to nothing — which is the shape of bug M5's first rehearsal hit
/// when `.resuming()` met an empty recompute.
const fn entry_ts(entry: &JournalEntry) -> Option<Ts> {
    match entry {
        JournalEntry::Started { .. } => None,
        JournalEntry::Filled { at, .. }
        | JournalEntry::Checkpoint { at, .. }
        | JournalEntry::Tripped { at, .. }
        | JournalEntry::Stopped { at } => Some(*at),
    }
}
