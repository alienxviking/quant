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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use quant_core::execution::{
    ClientOrderId, Fill, OrderRequest, RejectReason, VenueOrderId, VenueTradeId,
};
use quant_core::fixed::{Px, Qty};
use quant_core::instrument::Exchange;
use quant_core::report::{
    AccountChange, DoubtChange, Ended, FillIds, ForeignOutcome, ForeignReport, Notice, OrderReport,
    RejectedWhy,
};
use quant_core::time::Ts;
use quant_engine::journal::{InstrumentKey, Journal, JournalEntry, StrayKind};
use quant_engine::risk::Bound;
use quant_engine::{Booked, RefusedBy, RunObserver, WriteAhead};
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
/// still does not have. `RunObserver` has fourteen methods since M8 slice (c),
/// and every one of them is driven by an *input* -- a market event, a venue's
/// report or notice, a strategy's decision -- so there is no tick an observer
/// can see. Since M8 `Engine::run` does wake on a
/// quiet market, but only to ask the venue, and a wake on which the venue has
/// nothing to say returns before any callback — so a quiet stretch still
/// produces no callback to checkpoint from. That is a change to the seam, not a
/// thing to slip in before a run.
pub const CHECKPOINT_EVERY: u64 = 5;

/// Where a journal's lines go: the file, always, outside a test.
///
/// A trait for two readers of the writer that are not a run. The account
/// replay check (`crate::account_replay`) writes what a replayed venue said
/// through this same writer into memory, so that the lines it compares with
/// the journal's are written by the code that wrote those, rather than by a
/// second mapping that must agree with it for ever. And a test of the strict
/// writer needs an append that fails when it says, which a file on a healthy
/// disk will not do.
pub trait Lines: Send {
    /// Append one entry, durably, or say why not.
    ///
    /// # Errors
    ///
    /// Whatever kept the entry from being held.
    fn append(&mut self, entry: &JournalEntry) -> std::io::Result<()>;
}

impl Lines for Journal {
    fn append(&mut self, entry: &JournalEntry) -> std::io::Result<()> {
        Journal::append(self, entry)
    }
}

/// In memory, for a reader that compares lines rather than keeping them.
impl Lines for Vec<JournalEntry> {
    fn append(&mut self, entry: &JournalEntry) -> std::io::Result<()> {
        self.push(entry.clone());
        Ok(())
    }
}

/// A journal shared between a writer and the path that writes around it.
pub type Shared = Arc<Mutex<dyn Lines>>;

/// Writes fills down before the strategy is told about them.
pub struct JournalWriter {
    /// Shared with the shutdown path, which writes the final checkpoint and the
    /// `Stopped` mark. Two owners and a few hundred writes over a fortnight, so
    /// the lock is never contended -- and the alternative, prising the journal
    /// back out of the engine, would mean the engine knowing what a journal is.
    journal: Shared,
    symbol: String,
    fills: u64,
    failures: u64,
    /// What a failed append means: see [`Policy`].
    policy: Policy,
    /// Set by a strict writer's first failure and never cleared. Shared, so
    /// that the shutdown path, which writes around this writer, stops where
    /// it stops ([`Latch`]).
    failed: Arc<AtomicBool>,
}

impl std::fmt::Debug for JournalWriter {
    /// By hand: the sink is a trait object, and what is worth printing is the
    /// policy and how it has fared.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalWriter")
            .field("symbol", &self.symbol)
            .field("fills", &self.fills)
            .field("failures", &self.failures)
            .field("policy", &self.policy)
            .field("failed", &self.failed.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

/// What a writer does once an append has failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Policy {
    /// Count it, log it, carry on writing, and let every order through.
    Lenient,
    /// Stop: write nothing more, and withhold every submission, from the
    /// first failure on.
    Strict,
}

/// A strict writer's stop, for the lines written around it.
///
/// The live binary writes `started` and the start `balances` before the
/// engine exists, and the closing checkpoint, `stopped` and the end
/// `balances` after it has gone, all through the shared journal rather than
/// through the writer. Each goes through this, so that none lands after a
/// line a failed append may have torn (`docs/run-log.md` §8, *The strict
/// writer*), and a failure among them stops the writer too.
#[derive(Debug, Clone, Default)]
pub struct Latch(Arc<AtomicBool>);

impl Latch {
    /// Whether an append has failed.
    #[must_use]
    pub fn is_set(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    /// Append `entry` unless an append has already failed. Returns whether
    /// it is on the disk; a failure sets the latch.
    ///
    /// # Errors
    ///
    /// The failure, which has set the latch.
    pub fn write<J: Lines + ?Sized>(
        &self,
        journal: &Arc<Mutex<J>>,
        entry: &JournalEntry,
    ) -> std::io::Result<bool> {
        if self.is_set() {
            return Ok(false);
        }
        write(journal, entry).inspect_err(|_| self.0.store(true, Ordering::SeqCst))?;
        Ok(true)
    }
}

impl JournalWriter {
    /// A lenient writer for one symbol's journal: a line it cannot write is
    /// counted and logged, and the run goes on. For paper and backtests.
    #[must_use]
    pub fn new(journal: Arc<Mutex<Journal>>, symbol: String, fills: u64) -> Self {
        Self::to(journal, symbol, fills)
    }

    /// [`Self::new`], writing into any [`Lines`]: what the account replay
    /// check writes a replayed venue's lines into, in memory.
    #[must_use]
    pub fn to(journal: Shared, symbol: String, fills: u64) -> Self {
        Self {
            journal,
            symbol,
            fills,
            failures: 0,
            policy: Policy::Lenient,
            failed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// A strict writer, for a live run: after its first failed append it
    /// writes nothing more, and answers every submission
    /// [`WriteAhead::Withhold`].
    ///
    /// Withheld, because at a live venue an order the journal does not hold
    /// is a position nobody knows about, and the engine then refuses it and
    /// every later one as `RefusedBy::Record`. Nothing more written, because a
    /// failed append can leave a torn line, and a line appended after one is
    /// in the middle of the file, where [`quant_engine::journal::read`]
    /// refuses it as damage: the journal would become unreadable to the
    /// restart that needs it most. Stopping keeps the torn line at the tail,
    /// which `read` tolerates. A latch rather than a retry, because what
    /// follows a torn line is not a question this process can answer by
    /// asking again.
    ///
    /// The shutdown path writes the closing checkpoint and `stopped` through
    /// the shared journal, outside this writer; a live binary must not write
    /// them after this writer has failed, for the same reason. It writes them
    /// through [`Self::latch`], which knows.
    #[must_use]
    pub fn strict(journal: Arc<Mutex<Journal>>, symbol: String, fills: u64) -> Self {
        Self::strict_to(journal, symbol, fills)
    }

    /// [`Self::strict`], writing into any [`Lines`]: the live binary's
    /// journal, which a test gives an append that fails when it says.
    #[must_use]
    pub fn strict_to(journal: Shared, symbol: String, fills: u64) -> Self {
        Self {
            policy: Policy::Strict,
            ..Self::to(journal, symbol, fills)
        }
    }

    /// This writer's latch, for the lines written around it.
    #[must_use]
    pub fn latch(&self) -> Latch {
        Latch(Arc::clone(&self.failed))
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
    /// written and not where a later edit forgot it. The strict policy lives
    /// here for the same reason.
    ///
    /// Returns whether the line is on the disk.
    fn record(&mut self, entry: &JournalEntry, what: &str) -> bool {
        if self.policy == Policy::Strict && self.failed.load(Ordering::SeqCst) {
            // Not written: after a failed append, a line written now could
            // land after a torn one. Loud, because the fact is lost.
            self.failures += 1;
            error!(
                what,
                "not journalled: this journal stopped at its first failure"
            );
            return false;
        }
        let Err(e) = write(&self.journal, entry) else {
            return true;
        };
        // Counted and logged, never fatal. A paper run that died because it
        // could not write one line would lose the live session it exists to
        // conduct; a run that carries on with a hole in its journal is
        // recoverable, and both `reconcile` and `runlog check` will say so
        // afterwards -- the checkpoint disagrees, or the id sequence has a
        // hole where the decision should be. A strict writer latches here
        // too, and writes nothing after this.
        self.failures += 1;
        error!(error = %e, what, "could not journal");
        if self.policy == Policy::Strict {
            self.failed.store(true, Ordering::SeqCst);
        }
        false
    }

    fn key(&self) -> InstrumentKey {
        InstrumentKey {
            exchange: Exchange::Binance,
            symbol: self.symbol.clone(),
        }
    }
}

/// The line a venue's notice is written as, and what to call it if the write
/// fails.
///
/// One exhaustive `match` with no `_` arm, and every notice destructured
/// without `..`: a fact a venue learns to report later does not build here
/// until it has a line, and a field added to one does not build until it has
/// been decided. The alternative is the defect this project keeps meeting, a
/// fact that reaches the record and is quietly not written down.
///
/// Each line is stamped `at`, which the engine passes as the notice's own
/// stamp, so the notice's `ts` is not read twice.
fn notice_line(notice: &Notice, at: Ts) -> (JournalEntry, &'static str) {
    match notice {
        Notice::Placed {
            client_order_id,
            venue_order_id,
            transact_ms,
            ts: _,
        } => (
            JournalEntry::Placed {
                at,
                client_order_id: *client_order_id,
                venue_order_id: venue_order_id.clone(),
                transact_ms: *transact_ms,
            },
            "a placement",
        ),
        Notice::Doubt {
            client_order_id,
            change,
            ts: _,
        } => (
            JournalEntry::Doubt {
                at,
                client_order_id: *client_order_id,
                change: change.clone(),
            },
            "a doubt",
        ),
        Notice::CancelRefused {
            client_order_id,
            why,
            ts: _,
        } => (
            JournalEntry::CancelRefused {
                at,
                client_order_id: *client_order_id,
                why: why.clone(),
            },
            "a refused cancel",
        ),
        Notice::Foreign(ForeignReport {
            client_id,
            provenance,
            venue_order_id,
            side,
            what,
            ts: _,
        }) => (
            JournalEntry::Foreign {
                at,
                client_id: client_id.clone(),
                provenance: provenance.clone(),
                venue_order_id: venue_order_id.clone(),
                side: *side,
                what: what.clone(),
            },
            "an order this run did not send",
        ),
        Notice::Account { change, ts: _ } => (
            JournalEntry::Account {
                at,
                change: change.clone(),
            },
            "the account stream",
        ),
        Notice::Anomaly {
            client_order_id,
            what,
            detail,
            ts: _,
        } => (
            JournalEntry::Anomaly {
                at,
                client_order_id: *client_order_id,
                what: *what,
                detail: detail.clone(),
            },
            "an anomaly",
        ),
    }
}

/// Say out loud, as well as write down, the notices a person must look at
/// while they happen, as an orphan is: a stranger's trade on this account, a
/// report that could not be taken at its word, an order whose outcome is
/// unknown, and the account stream lost.
///
/// Exhaustive like [`notice_line`], so a new notice is decided here too:
/// quiet is a choice, not a default.
fn say_if_loud(symbol: &str, notice: &Notice) {
    match notice {
        Notice::Doubt {
            client_order_id,
            change: DoubtChange::Raised { cause, said },
            ts: _,
        } => error!(
            symbol,
            id = client_order_id.0,
            ?cause,
            ?said,
            "an order's outcome is unknown; no order is sent until it is known"
        ),
        Notice::Foreign(ForeignReport {
            client_id,
            provenance,
            venue_order_id,
            side,
            what:
                ForeignOutcome::Traded {
                    trade_id,
                    px,
                    qty,
                    commission,
                    commission_asset,
                    remaining: _,
                },
            ts: _,
        }) => error!(
            symbol,
            client_id = client_id.as_str(),
            ?provenance,
            venue_order_id = venue_order_id.0,
            ?side,
            trade_id = trade_id.0,
            %px,
            %qty,
            %commission,
            commission_asset = commission_asset.as_str(),
            "an order this run did not send traded on this account"
        ),
        Notice::Account {
            change: AccountChange::Lost { cause },
            ts: _,
        } => error!(
            symbol,
            ?cause,
            "the account stream is lost; every working order is in doubt"
        ),
        Notice::Anomaly {
            client_order_id,
            what,
            detail,
            ts: _,
        } => error!(
            symbol,
            id = client_order_id.map(|id| id.0),
            ?what,
            detail = detail.as_str(),
            "a venue report could not be taken at its word"
        ),
        // Recorded quietly: an acknowledgement, a doubt resolved, a cancel
        // that did not take (the order's own report says how it ends), a
        // stranger's order accepted or ended without trading, and the stream
        // regained.
        Notice::Placed { .. }
        | Notice::Doubt {
            change: DoubtChange::Cleared { .. },
            ..
        }
        | Notice::CancelRefused { .. }
        | Notice::Foreign(ForeignReport {
            what: ForeignOutcome::Accepted | ForeignOutcome::Ended { .. },
            ..
        })
        | Notice::Account {
            change: AccountChange::Regained,
            ..
        } => {}
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

    fn on_fill(&mut self, booked: &Booked<'_>, portfolio: &quant_engine::Portfolio) {
        // Every field named, so one added to `Booked` has to be decided here.
        let Booked {
            client_order_id,
            // A registry index, never persisted: the line names the
            // instrument by `(exchange, symbol)`.
            instrument: _,
            side,
            fill,
            remaining,
            ids,
            at,
        } = *booked;
        let (venue_order_id, trade_id) = fill_ids(ids);
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
                fee_asset: fill.fee_asset,
                remaining: Some(remaining),
                venue_order_id,
                trade_id,
            },
            "a fill",
        );
        // Frequent and cheap rather than one at the end: a run killed hard still
        // leaves a recent claim for the recompute to be compared against.
        if self.fills % CHECKPOINT_EVERY == 0 {
            self.checkpoint(at, portfolio);
        }
    }

    /// A lenient writer answers [`WriteAhead::Proceed`] always, even when the
    /// line could not be written; a strict one answers
    /// [`WriteAhead::Withhold`] unless this line is on the disk.
    ///
    /// Lenient on purpose for paper and backtests, and the policy is
    /// `record`'s: a paper run that stopped trading because it could not
    /// write one line would lose the session it exists to conduct, while a
    /// run with a hole in its journal is recoverable and `reconcile` and
    /// `runlog check` say where the hole is. Paper and every backtest
    /// therefore send exactly what they sent before the engine asked. A live
    /// run needs the opposite answer, because there an order the journal does
    /// not hold is a position nobody knows about: [`Self::strict`].
    fn on_submitted(
        &mut self,
        client_order_id: ClientOrderId,
        request: &OrderRequest,
        mark: Option<Px>,
        at: Ts,
    ) -> WriteAhead {
        let held = self.record(
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
        match self.policy {
            Policy::Lenient => WriteAhead::Proceed,
            Policy::Strict if held => WriteAhead::Proceed,
            Policy::Strict => WriteAhead::Withhold,
        }
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

    fn on_accepted(
        &mut self,
        client_order_id: ClientOrderId,
        venue_order_id: Option<&VenueOrderId>,
        at: Ts,
    ) {
        self.record(
            &JournalEntry::Accepted {
                at,
                client_order_id,
                venue_order_id: venue_order_id.cloned(),
            },
            "an acceptance",
        );
    }

    fn on_rejected(
        &mut self,
        client_order_id: ClientOrderId,
        reason: RejectReason,
        why: Option<&RejectedWhy>,
        at: Ts,
    ) {
        let (cited, breach) = match why {
            // A simulator's rejection: nothing to cite.
            None => (None, None),
            Some(RejectedWhy::Declined { cited, breach }) => (Some(cited.clone()), *breach),
            // Not a rejection by the venue, which never had the request, so
            // not a `rejected` line: that keeps its meaning. Loud, because an
            // order the strategy decided on did not happen.
            Some(RejectedWhy::NeverLanded(landing)) => {
                error!(
                    symbol = %self.symbol,
                    id = client_order_id.0,
                    ?reason,
                    ?landing,
                    "an order never reached the venue"
                );
                self.record(
                    &JournalEntry::NeverLanded {
                        at,
                        client_order_id,
                        landing: landing.clone(),
                    },
                    "an order that never landed",
                );
                return;
            }
        };
        self.record(
            &JournalEntry::Rejected {
                at,
                client_order_id,
                reason,
                cited,
                breach,
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

    fn on_cancelled(
        &mut self,
        client_order_id: ClientOrderId,
        remaining: Qty,
        ended: Option<&Ended>,
        at: Ts,
    ) {
        let (venue_order_id, ended) = ended.map_or((None, None), |ended| {
            let Ended {
                venue_order_id,
                how,
            } = ended;
            (Some(venue_order_id.clone()), Some(*how))
        });
        self.record(
            &JournalEntry::Cancelled {
                at,
                client_order_id,
                remaining,
                venue_order_id,
                ended,
            },
            "a cancellation",
        );
    }

    fn on_orphaned(
        &mut self,
        client_order_id: ClientOrderId,
        fill: &Fill,
        ids: Option<&FillIds>,
        at: Ts,
    ) {
        let (venue_order_id, trade_id) = fill_ids(ids);
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
                fee_asset: fill.fee_asset,
                venue_order_id,
                trade_id,
            },
            "an orphaned fill",
        );
    }

    fn on_sighted(&mut self, at: Ts) {
        self.record(&JournalEntry::Sighted { at }, "the market's return");
    }

    fn on_notice(&mut self, notice: &Notice, at: Ts) {
        say_if_loud(&self.symbol, notice);
        let (entry, what) = notice_line(notice, at);
        self.record(&entry, what);
    }

    fn on_stray(&mut self, report: &OrderReport, at: Ts) {
        let (client_order_id, kind, venue_order_id) = match report {
            OrderReport::Accepted {
                client_order_id,
                venue_order_id,
                ts: _,
            } => (
                *client_order_id,
                StrayKind::Accepted,
                venue_order_id.clone(),
            ),
            // What the venue said about an order the engine is not working is
            // kept to which order and what kind of report: the detail would
            // describe an order nobody here placed or still holds.
            OrderReport::Rejected {
                client_order_id,
                reason: _,
                why: _,
                ts: _,
            } => (*client_order_id, StrayKind::Rejected, None),
            OrderReport::Cancelled {
                client_order_id,
                remaining: _,
                ended,
                ts: _,
            } => (
                *client_order_id,
                StrayKind::Cancelled,
                ended.as_ref().map(|ended| ended.venue_order_id.clone()),
            ),
            // The engine sends a fill it is not working to `on_orphaned`, so
            // this does not happen. Should it, a fill nobody can attribute is
            // an orphan whichever hook it came through, and is written as one
            // rather than dropped.
            OrderReport::Filled {
                client_order_id,
                fill,
                remaining: _,
                ids,
                ts: _,
            } => {
                self.on_orphaned(*client_order_id, fill, ids.as_ref(), at);
                return;
            }
        };
        self.record(
            &JournalEntry::Stray {
                at,
                client_order_id,
                kind,
                venue_order_id,
            },
            "a report about an order the engine is not working",
        );
    }
}

/// The venue's names for a fill, as the two optional fields a line holds.
///
/// Both or neither, because the venue names an order and its trade together;
/// destructured without `..`, so a third name added to [`FillIds`] has to be
/// given a field here before this builds.
fn fill_ids(ids: Option<&FillIds>) -> (Option<VenueOrderId>, Option<VenueTradeId>) {
    ids.map_or((None, None), |ids| {
        let FillIds {
            venue_order_id,
            trade_id,
        } = ids;
        (Some(venue_order_id.clone()), Some(trade_id.clone()))
    })
}

/// Append one entry to the shared journal.
///
/// A poisoned lock is treated as a write failure rather than a panic: the
/// session is more valuable than the entry, and `reconcile` will notice the hole
/// afterwards because the checkpoint and the fill count will disagree.
pub fn write<J: Lines + ?Sized>(
    journal: &Arc<Mutex<J>>,
    entry: &JournalEntry,
) -> std::io::Result<()> {
    journal
        .lock()
        .map_err(|_| std::io::Error::other("the journal lock is poisoned"))?
        .append(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::event::Side;
    use quant_core::execution::FeeAsset;
    use quant_core::fixed::Notional;
    use quant_core::instrument::FilterBreach;
    use quant_core::report::{
        AccountCause, AnomalyKind, CancelRefusal, Cited, DoubtCause, EndedBy, Landing, Provenance,
        VenueText,
    };
    use quant_engine::portfolio::Portfolio;

    /// A journal in a fresh temp file, unique to the calling test.
    fn journal(name: &str) -> (std::path::PathBuf, Arc<Mutex<Journal>>) {
        let dir = std::env::temp_dir().join(format!("quant-record-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("journal.jsonl");
        let journal = Journal::open(&path).expect("open journal");
        (path, Arc::new(Mutex::new(journal)))
    }

    fn base_fee_fill() -> Fill {
        Fill {
            px: "100".parse().expect("px"),
            qty: "1".parse().expect("qty"),
            fee: "0.001".parse().expect("fee in base units"),
            is_maker: false,
            fee_asset: FeeAsset::Base,
        }
    }

    #[test]
    fn a_fill_is_journalled_with_the_asset_its_fee_was_taken_in() {
        // The writer used to stamp every fill `FeeAsset::Quote`, whatever the
        // fill said. Harmless while every fill was simulated, because the
        // simulator then charged only in the quote; the first fill with a base
        // fee would have been written down as a quote fee, replayed as one, and
        // `reconcile` would then disagree with the engine's own checkpoint over
        // a value our writer invented.
        let (path, journal) = journal("filled");
        let mut writer = JournalWriter::new(journal, "BTCUSDT".to_owned(), 0);
        let portfolio = Portfolio::new(Notional::from_raw(1_000 * quant_core::SCALE));
        writer.on_fill(
            &Booked {
                client_order_id: ClientOrderId(1),
                instrument: quant_core::instrument::InstrumentId::from_index(0),
                side: Side::Buy,
                fill: &base_fee_fill(),
                remaining: Qty::ZERO,
                ids: None,
                at: Ts::from_nanos(1),
            },
            &portfolio,
        );
        let entries = quant_engine::journal::read(&path).expect("read").entries;
        let asset = entries.iter().find_map(|e| match e {
            JournalEntry::Filled { fee_asset, .. } => Some(*fee_asset),
            _ => None,
        });
        assert_eq!(asset, Some(FeeAsset::Base));
    }

    // Long because it spells out every line whole: a comparison of whole
    // entries is what shows a name written into the wrong field.
    #[allow(clippy::too_many_lines)]
    #[test]
    fn what_a_live_venue_says_beside_an_event_lands_on_the_event_s_line() {
        // J1, through the writer. The engine hands the record the venue's
        // names and words beside each event, and the line that records the
        // event is where they belong: one `filled` line is one `myTrades` row,
        // with no second line to pair it with. Every value distinct and
        // non-zero, so a name written into the wrong field shows.
        let (path, journal) = journal("detail");
        let mut writer = JournalWriter::new(journal, "BTCUSDT".to_owned(), 0);
        let portfolio = Portfolio::new(Notional::from_raw(1_000 * quant_core::SCALE));
        let venue = |id: &str| VenueOrderId(id.to_owned());
        let ids = FillIds {
            venue_order_id: venue("4293153"),
            trade_id: VenueTradeId("12345".to_owned()),
        };
        let fill = base_fee_fill();
        writer.on_accepted(ClientOrderId(1), Some(&venue("4293153")), Ts::from_nanos(1));
        writer.on_fill(
            &Booked {
                client_order_id: ClientOrderId(1),
                instrument: quant_core::instrument::InstrumentId::from_index(0),
                side: Side::Buy,
                fill: &fill,
                remaining: "0.25".parse().expect("qty"),
                ids: Some(&ids),
                at: Ts::from_nanos(2),
            },
            &portfolio,
        );
        let cited = Cited {
            http_status: Some(400),
            code: -1013,
            msg: VenueText::new("Filter failure: NOTIONAL"),
        };
        writer.on_rejected(
            ClientOrderId(2),
            RejectReason::Venue,
            Some(&RejectedWhy::Declined {
                cited: cited.clone(),
                breach: Some(FilterBreach::MinNotional),
            }),
            Ts::from_nanos(3),
        );
        writer.on_cancelled(
            ClientOrderId(3),
            "0.75".parse().expect("qty"),
            Some(&Ended {
                venue_order_id: venue("4293155"),
                how: EndedBy::SelfTradePrevention,
            }),
            Ts::from_nanos(4),
        );
        let orphan_ids = FillIds {
            venue_order_id: venue("4293156"),
            trade_id: VenueTradeId("12346".to_owned()),
        };
        writer.on_orphaned(
            ClientOrderId(9),
            &fill,
            Some(&orphan_ids),
            Ts::from_nanos(5),
        );

        let entries = quant_engine::journal::read(&path).expect("read").entries;
        let key = || InstrumentKey {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
        };
        assert_eq!(
            entries,
            vec![
                JournalEntry::Accepted {
                    at: Ts::from_nanos(1),
                    client_order_id: ClientOrderId(1),
                    venue_order_id: Some(venue("4293153")),
                },
                JournalEntry::Filled {
                    at: Ts::from_nanos(2),
                    client_order_id: ClientOrderId(1),
                    instrument: key(),
                    side: Side::Buy,
                    px: fill.px,
                    qty: fill.qty,
                    fee: fill.fee,
                    is_maker: fill.is_maker,
                    fee_asset: fill.fee_asset,
                    remaining: Some("0.25".parse().expect("qty")),
                    venue_order_id: Some(venue("4293153")),
                    trade_id: Some(VenueTradeId("12345".to_owned())),
                },
                JournalEntry::Rejected {
                    at: Ts::from_nanos(3),
                    client_order_id: ClientOrderId(2),
                    reason: RejectReason::Venue,
                    cited: Some(cited),
                    breach: Some(FilterBreach::MinNotional),
                },
                JournalEntry::Cancelled {
                    at: Ts::from_nanos(4),
                    client_order_id: ClientOrderId(3),
                    remaining: "0.75".parse().expect("qty"),
                    venue_order_id: Some(venue("4293155")),
                    ended: Some(EndedBy::SelfTradePrevention),
                },
                JournalEntry::Orphaned {
                    at: Ts::from_nanos(5),
                    client_order_id: ClientOrderId(9),
                    px: fill.px,
                    qty: fill.qty,
                    fee: fill.fee,
                    fee_asset: fill.fee_asset,
                    venue_order_id: Some(venue("4293156")),
                    trade_id: Some(VenueTradeId("12346".to_owned())),
                },
            ]
        );
    }

    /// What one hook call wrote, read back from a journal of its own.
    fn lines_from(name: &str, call: impl FnOnce(&mut JournalWriter)) -> Vec<JournalEntry> {
        let (path, journal) = journal(name);
        let mut writer = JournalWriter::new(journal, "BTCUSDT".to_owned(), 0);
        call(&mut writer);
        quant_engine::journal::read(&path).expect("read").entries
    }

    #[test]
    fn an_order_that_never_landed_is_not_written_as_a_rejection() {
        // J6. `rejected` means the venue had the request and declined it, and
        // keeps meaning that: an order that never reached the venue gets a
        // line of its own, with why, and no `rejected` line beside it.
        let landing = Landing::NotSent {
            why: VenueText::new("connection refused"),
        };
        let lines = lines_from("never-landed", |writer| {
            writer.on_rejected(
                ClientOrderId(4),
                RejectReason::Venue,
                Some(&RejectedWhy::NeverLanded(landing.clone())),
                Ts::from_nanos(9),
            );
        });
        assert_eq!(
            lines,
            vec![JournalEntry::NeverLanded {
                at: Ts::from_nanos(9),
                client_order_id: ClientOrderId(4),
                landing,
            }]
        );

        // And a rejection the venue cited is a `rejected` line, citation and
        // all.
        let cited = Cited {
            http_status: Some(400),
            code: -2010,
            msg: VenueText::new("Account has insufficient balance."),
        };
        let lines = lines_from("declined", |writer| {
            writer.on_rejected(
                ClientOrderId(5),
                RejectReason::InsufficientFunds,
                Some(&RejectedWhy::Declined {
                    cited: cited.clone(),
                    breach: None,
                }),
                Ts::from_nanos(10),
            );
        });
        assert_eq!(
            lines,
            vec![JournalEntry::Rejected {
                at: Ts::from_nanos(10),
                client_order_id: ClientOrderId(5),
                reason: RejectReason::InsufficientFunds,
                cited: Some(cited),
                breach: None,
            }]
        );
    }

    /// One notice of every kind, beside the one line each must become.
    // A table, one row per variant: long because the set is.
    #[allow(clippy::too_many_lines)]
    fn every_notice() -> Vec<(Notice, JournalEntry)> {
        let venue = |id: &str| VenueOrderId(id.to_owned());
        let cited = Cited {
            http_status: Some(400),
            code: -2011,
            msg: VenueText::new("Unknown order sent."),
        };
        let traded = ForeignOutcome::Traded {
            trade_id: VenueTradeId("77".to_owned()),
            px: "0.1026".parse().expect("px"),
            qty: "0.2".parse().expect("qty"),
            commission: "0.0002".parse().expect("commission"),
            commission_asset: VenueText::new("BNB"),
            remaining: "0.4".parse().expect("qty"),
        };
        let raised = DoubtChange::Raised {
            cause: DoubtCause::NoAnswer,
            said: None,
        };
        let lost = AccountChange::Lost {
            cause: AccountCause::SocketClosed,
        };
        vec![
            (
                Notice::Placed {
                    client_order_id: ClientOrderId(1),
                    venue_order_id: venue("4293153"),
                    transact_ms: 1_499_405_658_657,
                    ts: Ts::from_nanos(51),
                },
                JournalEntry::Placed {
                    at: Ts::from_nanos(51),
                    client_order_id: ClientOrderId(1),
                    venue_order_id: venue("4293153"),
                    transact_ms: 1_499_405_658_657,
                },
            ),
            (
                Notice::Doubt {
                    client_order_id: ClientOrderId(2),
                    change: raised.clone(),
                    ts: Ts::from_nanos(52),
                },
                JournalEntry::Doubt {
                    at: Ts::from_nanos(52),
                    client_order_id: ClientOrderId(2),
                    change: raised,
                },
            ),
            (
                Notice::CancelRefused {
                    client_order_id: ClientOrderId(3),
                    why: CancelRefusal::NotOpen(cited.clone()),
                    ts: Ts::from_nanos(53),
                },
                JournalEntry::CancelRefused {
                    at: Ts::from_nanos(53),
                    client_order_id: ClientOrderId(3),
                    why: CancelRefusal::NotOpen(cited),
                },
            ),
            (
                Notice::Foreign(ForeignReport {
                    client_id: VenueText::new("q-r0-7"),
                    provenance: Provenance::OtherRun(VenueText::new("r0")),
                    venue_order_id: venue("4293154"),
                    side: Side::Sell,
                    what: traded.clone(),
                    ts: Ts::from_nanos(54),
                }),
                JournalEntry::Foreign {
                    at: Ts::from_nanos(54),
                    client_id: VenueText::new("q-r0-7"),
                    provenance: Provenance::OtherRun(VenueText::new("r0")),
                    venue_order_id: venue("4293154"),
                    side: Side::Sell,
                    what: traded,
                },
            ),
            (
                Notice::Account {
                    change: lost.clone(),
                    ts: Ts::from_nanos(55),
                },
                JournalEntry::Account {
                    at: Ts::from_nanos(55),
                    change: lost,
                },
            ),
            (
                Notice::Anomaly {
                    client_order_id: Some(ClientOrderId(6)),
                    what: AnomalyKind::SecondEnd,
                    detail: VenueText::new("CANCELED after FILLED"),
                    ts: Ts::from_nanos(56),
                },
                JournalEntry::Anomaly {
                    at: Ts::from_nanos(56),
                    client_order_id: Some(ClientOrderId(6)),
                    what: AnomalyKind::SecondEnd,
                    detail: VenueText::new("CANCELED after FILLED"),
                },
            ),
        ]
    }

    #[test]
    fn every_notice_a_venue_gives_lands_as_one_line_of_its_own_type() {
        // J7. The only path these facts have to the record is this hook, and
        // the strategy hears none of them, so a notice the writer dropped
        // would be lost with no error anywhere and a journal that merely
        // looked like a quiet venue. Table-driven over every variant, each in
        // a journal of its own, so one that writes nothing, or writes two
        // lines, or the wrong one, shows by name.
        for (i, (notice, line)) in every_notice().into_iter().enumerate() {
            let at = notice.ts();
            let lines = lines_from(&format!("notice-{i}"), |writer| {
                writer.on_notice(&notice, at);
            });
            assert_eq!(lines, vec![line], "{notice:?}");
        }
    }

    #[test]
    fn a_report_about_an_order_the_engine_is_not_working_is_a_stray_line() {
        let venue = |id: &str| VenueOrderId(id.to_owned());
        let reports = [
            (
                OrderReport::Accepted {
                    client_order_id: ClientOrderId(97),
                    venue_order_id: Some(venue("4293157")),
                    ts: Ts::from_nanos(61),
                },
                StrayKind::Accepted,
                Some(venue("4293157")),
            ),
            (
                OrderReport::Rejected {
                    client_order_id: ClientOrderId(98),
                    reason: RejectReason::Venue,
                    why: None,
                    ts: Ts::from_nanos(62),
                },
                StrayKind::Rejected,
                None,
            ),
            (
                OrderReport::Cancelled {
                    client_order_id: ClientOrderId(99),
                    remaining: "0.3".parse().expect("qty"),
                    ended: Some(Ended {
                        venue_order_id: venue("4293158"),
                        how: EndedBy::Cancelled,
                    }),
                    ts: Ts::from_nanos(63),
                },
                StrayKind::Cancelled,
                Some(venue("4293158")),
            ),
        ];
        for (i, (report, kind, venue_order_id)) in reports.into_iter().enumerate() {
            let at = report.ts();
            let lines = lines_from(&format!("stray-{i}"), |writer| {
                writer.on_stray(&report, at);
            });
            assert_eq!(
                lines,
                vec![JournalEntry::Stray {
                    at,
                    client_order_id: report.client_order_id(),
                    kind,
                    venue_order_id,
                }],
                "{report:?}"
            );
        }
    }

    #[test]
    fn the_market_s_return_is_a_line() {
        // The other half of `blind`, and from schema 3 the only evidence a
        // reader takes that the market came back. A writer that inherited a
        // silent default would leave every outage open forever.
        let lines = lines_from("sighted", |writer| writer.on_sighted(Ts::from_nanos(71)));
        assert_eq!(
            lines,
            vec![JournalEntry::Sighted {
                at: Ts::from_nanos(71)
            }]
        );
    }

    fn order() -> OrderRequest {
        OrderRequest {
            instrument: quant_core::instrument::InstrumentId::from_index(0),
            side: Side::Buy,
            qty: "0.001".parse().expect("qty"),
            kind: quant_core::execution::OrderKind::Market,
            time_in_force: quant_core::execution::TimeInForce::Gtc,
        }
    }

    /// Poison the journal's lock, as a thread that panicked holding it does.
    fn poison(journal: &Arc<Mutex<Journal>>) {
        let held = Arc::clone(journal);
        let _ = std::thread::spawn(move || {
            let _guard = held.lock().expect("not yet poisoned");
            panic!("poisoning the journal lock on purpose");
        })
        .join();
        assert!(journal.is_poisoned(), "the fixture must poison the lock");
    }

    /// One writer driven through a failure and out the other side: a write
    /// that lands, one that cannot, and two after the lock is usable again.
    /// The answers to the three submissions, and what the file holds.
    fn through_a_failure(
        name: &str,
        make: fn(Arc<Mutex<Journal>>, String, u64) -> JournalWriter,
    ) -> (Vec<WriteAhead>, Vec<JournalEntry>) {
        let (path, journal) = journal(name);
        let mut writer = make(Arc::clone(&journal), "BTCUSDT".to_owned(), 0);
        let mut answers =
            vec![writer.on_submitted(ClientOrderId(1), &order(), None, Ts::from_nanos(1))];
        poison(&journal);
        answers.push(writer.on_submitted(ClientOrderId(2), &order(), None, Ts::from_nanos(2)));
        // The lock works again, so only the writer's own policy can stop a
        // later line now.
        journal.clear_poison();
        answers.push(writer.on_submitted(ClientOrderId(3), &order(), None, Ts::from_nanos(3)));
        writer.on_blind(
            quant_core::event::GapCause::Disconnect,
            Ts::from_nanos(3),
            Ts::from_nanos(4),
        );
        let entries = quant_engine::journal::read(&path).expect("read").entries;
        (answers, entries)
    }

    #[test]
    fn a_strict_writer_withholds_and_stops_at_its_first_failure_and_a_lenient_one_does_not() {
        // J8. A live run's record must hold every order before it is sent,
        // and an append after a torn line would leave that line in the middle
        // of the file, where `read` refuses the whole journal. So the strict
        // writer answers `Withhold` from its first failure on, and writes
        // nothing more even when it could.
        let (answers, entries) = through_a_failure("strict", JournalWriter::strict);
        assert_eq!(
            answers,
            vec![
                WriteAhead::Proceed,
                WriteAhead::Withhold,
                WriteAhead::Withhold
            ],
            "withheld at the failure, and still withheld after it"
        );
        assert_eq!(
            entries.len(),
            1,
            "only the line before the failure: {entries:?}"
        );
        assert!(matches!(
            entries[0],
            JournalEntry::Submitted {
                client_order_id: ClientOrderId(1),
                ..
            }
        ));

        // Paper and every backtest: the run is worth more than the line, and
        // a writer that can write again does.
        let (answers, entries) = through_a_failure("lenient", JournalWriter::new);
        assert_eq!(answers, vec![WriteAhead::Proceed; 3]);
        assert_eq!(
            entries.len(),
            3,
            "everything but the failed line: {entries:?}"
        );
    }

    #[test]
    fn an_orphaned_fill_records_its_fee_asset_too() {
        // An orphan is the one fill a person has to investigate by hand, against
        // the venue's own record. Its fee is half a fact without the asset.
        let (path, journal) = journal("orphaned");
        let mut writer = JournalWriter::new(journal, "BTCUSDT".to_owned(), 0);
        writer.on_orphaned(ClientOrderId(7), &base_fee_fill(), None, Ts::from_nanos(1));
        let entries = quant_engine::journal::read(&path).expect("read").entries;
        let asset = entries.iter().find_map(|e| match e {
            JournalEntry::Orphaned { fee_asset, .. } => Some(*fee_asset),
            _ => None,
        });
        assert_eq!(asset, Some(FeeAsset::Base));
    }
}
