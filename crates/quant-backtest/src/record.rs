//! Writing a run log, shared by the two binaries that produce one.
//!
//! Lifted out of the `paper` binary when `backtest` needed to write one too. A
//! copy would have been shorter to type and would have had to agree forever with
//! the original about what an entry means — which is the pattern this project
//! has refused five times (no `PaperVenue`, no venue trait from one
//! implementation, no duplicated ops harness, no second fill model, no second
//! session-discovery routine).
//!
//! It matters more here than usual: `runlog diff` compares a live journal
//! against a replayed one, so the two writers *must* agree or the diff measures
//! the writers rather than the runs. One writer cannot disagree with itself.

use std::sync::{Arc, Mutex};

use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, Fill, OrderRequest, RejectReason};
use quant_core::fixed::{Px, Qty};
use quant_core::instrument::{Exchange, InstrumentId};
use quant_core::time::Ts;
use quant_engine::journal::{InstrumentKey, Journal, JournalEntry};
use quant_engine::risk::Bound;
use quant_engine::{RefusedBy, RunObserver};
use tracing::error;

/// How often the engine writes down what it believes.
///
/// Every checkpoint is what turns reconciliation from a tautology into a check,
/// so they are cheap and frequent rather than one at the end: a run that is
/// killed hard still leaves a recent claim for the recompute to be compared
/// against.
///
/// **Calibrated against the cadence that consumes it, not chosen for feel.**
/// `ops/verify-loop.sh` runs `reconcile` every six hours during a run, and a
/// checkpoint written less often than that means most passes re-check a claim
/// they already checked — or, before the first one, exit 2 with "nothing to
/// check". This was 25, and both 10-minute rehearsals produced 22 fills, so
/// neither wrote a single checkpoint before shutdown and the in-run
/// reconciliation reported `nothing to check yet` for the entire run. At the
/// fortnight's observed rate — M4's acceptance week was 418 fills over 7 days,
/// about 60 a day — 25 is roughly one checkpoint every ten hours, still coarser
/// than the thing reading it. Five is about two hours, so every reconcile pass
/// has something new.
///
/// The remaining hole is honest and unfixed: this triggers on *fills*, so a
/// strategy that trades less than five times between passes still leaves nothing
/// new to check, and a hard kill before the fifth fill loses the lot. A
/// time-based checkpoint would close it and needs a periodic hook the engine
/// does not have — `RunObserver` fires only on fills and `Engine::run` blocks —
/// which is a change to the seam and not a thing to slip in before a fortnight.
pub const CHECKPOINT_EVERY: u64 = 5;

/// Writes fills down before the strategy is told about them.
#[derive(Debug)]
pub struct JournalWriter {
    /// Shared with the shutdown path, which writes the final checkpoint and the
    /// `Stopped` mark. Two owners and a few hundred writes over a fortnight, so
    /// the lock is never contended -- and the alternative, prising the journal
    /// back out of the engine, would mean the engine knowing what a journal is.
    journal: Arc<Mutex<Journal>>,
    symbol: String,
    fills: u64,
    failures: u64,
}

impl JournalWriter {
    /// A writer for one symbol's journal.
    #[must_use]
    pub fn new(journal: Arc<Mutex<Journal>>, symbol: String, fills: u64) -> Self {
        Self {
            journal,
            symbol,
            fills,
            failures: 0,
        }
    }

    /// Write down what the engine currently believes.
    ///
    /// The entry that turns reconciliation from a tautology into a check: the
    /// recompute derives its numbers from the fill lines, so it can only be
    /// compared against a claim made independently of them.
    fn checkpoint(&mut self, at: Ts, portfolio: &quant_engine::Portfolio) {
        self.record(&JournalEntry::checkpoint(at, portfolio), "a checkpoint");
    }

    /// Append one entry, counting a failure rather than ending the run.
    ///
    /// One place rather than one per call site, so the never-fatal policy
    /// cannot be adopted unevenly as entry types multiply -- which is the shape
    /// of the M4 and M5.c near-misses, where a rule held everywhere it was
    /// written and not where a later edit forgot it.
    fn record(&mut self, entry: &JournalEntry, what: &str) {
        if let Err(e) = write(&self.journal, entry) {
            // Counted and logged, never fatal. A paper run that died because it
            // could not write one line would lose the live session it exists to
            // conduct; a run that carries on with a hole in its journal is
            // recoverable, and both `reconcile` and `runlog check` will say so
            // afterwards -- the checkpoint disagrees, or the id sequence has a
            // hole where the decision should be.
            self.failures += 1;
            error!(error = %e, what, "could not journal");
        }
    }

    fn key(&self) -> InstrumentKey {
        InstrumentKey {
            exchange: Exchange::Binance,
            symbol: self.symbol.clone(),
        }
    }
}

impl RunObserver for JournalWriter {
    fn on_tripped(&mut self, cause: quant_engine::risk::TripCause, at: Ts) {
        // At the instant, not at shutdown. A hard kill between the two used to
        // lose the trip outright, and the supervisor would re-arm a switch that
        // had fired -- the hole `CLAUDE.md` carried as *must be fixed before
        // M8*. Written through the same failure-tolerant path as every other
        // entry, because a journal write that killed the process on the way
        // down would be a worse outcome than a missing line.
        self.record(&JournalEntry::Tripped { at, cause }, "a kill switch trip");
    }

    fn on_note(&mut self, note: &quant_engine::Note, at: Ts) {
        self.record(
            &JournalEntry::Note {
                at,
                kind: note.kind.to_owned(),
                detail: note.detail.clone(),
            },
            "a strategy note",
        );
    }

    fn on_blind(&mut self, cause: quant_core::event::GapCause, last_good_ts: Ts, at: Ts) {
        self.record(
            &JournalEntry::Blind {
                at,
                cause,
                last_good_ts,
            },
            "a gap",
        );
    }

    fn on_fill(
        &mut self,
        client_order_id: ClientOrderId,
        _instrument: InstrumentId,
        side: Side,
        fill: &Fill,
        at: Ts,
        portfolio: &quant_engine::Portfolio,
    ) {
        self.fills += 1;
        self.record(
            &JournalEntry::Filled {
                at,
                client_order_id,
                instrument: self.key(),
                side,
                px: fill.px,
                qty: fill.qty,
                fee: fill.fee,
                is_maker: fill.is_maker,
            },
            "a fill",
        );
        // Frequent and cheap rather than one at the end: a run killed hard still
        // leaves a recent claim for the recompute to be compared against.
        if self.fills % CHECKPOINT_EVERY == 0 {
            self.checkpoint(at, portfolio);
        }
    }

    fn on_submitted(
        &mut self,
        client_order_id: ClientOrderId,
        request: &OrderRequest,
        mark: Option<Px>,
        at: Ts,
    ) {
        self.record(
            &JournalEntry::Submitted {
                at,
                client_order_id,
                instrument: self.key(),
                side: request.side,
                qty: request.qty,
                limit: request.limit(),
                mark,
            },
            "a submission",
        );
    }

    fn on_refused(
        &mut self,
        client_order_id: ClientOrderId,
        request: &OrderRequest,
        reason: RejectReason,
        bound: Option<Bound>,
        by: RefusedBy,
        at: Ts,
    ) {
        self.record(
            &JournalEntry::Refused {
                at,
                client_order_id,
                instrument: self.key(),
                side: request.side,
                qty: request.qty,
                reason,
                bound,
                by,
            },
            "a refusal",
        );
    }

    fn on_accepted(&mut self, client_order_id: ClientOrderId, at: Ts) {
        self.record(
            &JournalEntry::Accepted {
                at,
                client_order_id,
            },
            "an acceptance",
        );
    }

    fn on_rejected(&mut self, client_order_id: ClientOrderId, reason: RejectReason, at: Ts) {
        self.record(
            &JournalEntry::Rejected {
                at,
                client_order_id,
                reason,
            },
            "a rejection",
        );
    }

    fn on_cancel_requested(&mut self, client_order_id: ClientOrderId, at: Ts) {
        self.record(
            &JournalEntry::CancelRequested {
                at,
                client_order_id,
            },
            "a cancel request",
        );
    }

    fn on_cancelled(&mut self, client_order_id: ClientOrderId, remaining: Qty, at: Ts) {
        self.record(
            &JournalEntry::Cancelled {
                at,
                client_order_id,
                remaining,
            },
            "a cancellation",
        );
    }

    fn on_orphaned(&mut self, client_order_id: ClientOrderId, fill: &Fill, at: Ts) {
        // Loud as well as recorded: an unattributable fill means the venue and
        // our books disagree about what we own, and that is the one condition
        // where carrying on quietly is worse than the noise.
        error!(
            id = client_order_id.0,
            px = %fill.px, qty = %fill.qty,
            "a fill arrived for an order this engine has no record of"
        );
        self.record(
            &JournalEntry::Orphaned {
                at,
                client_order_id,
                px: fill.px,
                qty: fill.qty,
                fee: fill.fee,
            },
            "an orphaned fill",
        );
    }
}

/// Append one entry to the shared journal.
///
/// A poisoned lock is treated as a write failure rather than a panic: the
/// session is more valuable than the entry, and `reconcile` will notice the hole
/// afterwards because the checkpoint and the fill count will disagree.
pub fn write(journal: &Arc<Mutex<Journal>>, entry: &JournalEntry) -> std::io::Result<()> {
    journal
        .lock()
        .map_err(|_| std::io::Error::other("the journal lock is poisoned"))?
        .append(entry)
}
