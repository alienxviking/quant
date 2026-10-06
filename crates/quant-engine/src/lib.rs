//! The engine seam.
//!
//! One loop, wired to an [`EventSource`] on one side and an [`ExecutionVenue`]
//! on the other, with a [`Strategy`] in the middle and a [`RiskLayer`] on the
//! only path out. `docs/engine-contract.md` argues the shape; this implements
//! it.
//!
//! ```text
//!   EventSource ──► Engine ──► Strategy ──► RiskLayer ──► ExecutionVenue
//!                      │                                        │
//!                      └──────── ExecutionEvent ◄───────────────┘
//! ```
//!
//! Backtest is `HistoricalSource + SimulatedVenue`. Paper is
//! `LiveSource + SimulatedVenue` (there is no separate paper venue -- M5 found the
//! middle one need not exist). Live is `LiveSource + LiveVenue`. The strategy is
//! the same value in all three, and cannot ask which one it is in.
//!
//! # The order of operations inside one event, and why it is that order
//!
//! This is the part of the engine most able to lie, so it is written down and
//! pinned by a test rather than left to the reading order of a function body.
//! For each event at time `T`:
//!
//! 1. **The clock advances to `T`.** Nothing else moves it, so there is no time
//!    between events for a strategy to act in.
//! 2. **The book is updated.** A `Gap` clears it, which is what makes "no prices
//!    after a disconnect" structural rather than advisory.
//! 3. **The venue sees the event.** Orders resting from *before* `T` match
//!    against it. They were placed on earlier information, so trading against
//!    `T` is exactly what would have happened.
//! 4. **The strategy is told what happened to its orders**, from step 3.
//! 5. **The strategy sees the market event** and may submit.
//!
//! Step 5 comes after step 3, and that is the whole point. An order submitted on
//! seeing the trade at `T` is **not** eligible to match against that trade — it
//! becomes live for the next event. Reversing those two steps lets a strategy
//! react to a print and fill against the same print, which is trading on
//! information at the instant it is created. It is invisible in the output and
//! it inflates every result.
//!
//! # The engine clock is the event stream
//!
//! `now` is the `local_recv_ts` of the last event and nothing else — no system
//! clock anywhere on this path, in any of the three worlds. In a backtest that
//! makes the run deterministic and repeatable. Live it is still correct, because
//! the last event's receive stamp *is* roughly now, and a strategy that used a
//! finer notion of time would behave differently in the three worlds.
//!
//! That is invariant 4 arriving at its destination: components take a clock, and
//! here the clock is the data.

pub mod journal;
pub mod portfolio;
pub mod risk;
pub mod strategy;
pub mod venue;

use std::collections::HashMap;

use quant_book::Book;
use quant_core::event::{MarketEvent, Side};
use quant_core::execution::{ClientOrderId, ExecutionEvent, Fill, OrderRequest, RejectReason};
use quant_core::fixed::Notional;
use quant_core::fixed::{Px, Qty};
use quant_core::instrument::InstrumentId;
use quant_core::source::{EventSource, SourceError};
use quant_core::time::Ts;

pub use journal::{InstrumentKey, Journal, JournalEntry};
pub use portfolio::{Portfolio, Position};
pub use risk::{AllowAll, Bound, Limits, Refusal, RiskEngine, RiskLayer, TripCause};
pub use strategy::{Context, Note, Strategy};
pub use venue::ExecutionVenue;

/// What a run did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EngineStats {
    pub events: u64,
    pub gaps: u64,
    /// Orders that passed the risk layer and reached the venue.
    pub submitted: u64,
    /// Orders the risk layer or the seam refused. They never reached a venue.
    pub refused: u64,
    pub cancels: u64,
    pub execution_events: u64,
    pub fills: u64,
    /// First and last event times, for reporting the period actually covered.
    pub first_ts: Option<Ts>,
    pub last_ts: Option<Ts>,
}

/// The engine's mutable bookkeeping.
///
/// Grouped so a [`Context`] can borrow it as one thing. Four separate `&mut`
/// parameters is the same borrow with more places to get the order wrong, and
/// the compiler was starting to say so.
#[derive(Debug)]
pub(crate) struct Ledger {
    /// Next client order id. Starts at 1, so a zero id is always a bug rather
    /// than a legitimate first order.
    next_id: u64,
    /// Execution events raised during a strategy callback — a risk rejection, or
    /// a seam rejection — waiting to be delivered.
    ///
    /// Deferred rather than delivered inline because a strategy must not be
    /// re-entered while it is on the stack, and because even a rejection should
    /// arrive the way every other outcome does: as an event, afterwards.
    deferred: Vec<ExecutionEvent>,
    /// Which instrument and side each live order belongs to.
    ///
    /// A `Fill` names only the order, so this is what lets a fill be booked at
    /// all. The engine has to hold it in every world — a live venue's fill report
    /// names the order too — which is why the portfolio lives here and not in the
    /// simulator. Entries are removed on a terminal event, so this is bounded by
    /// the strategy's working orders rather than by the length of the run.
    orders: HashMap<ClientOrderId, (InstrumentId, Side)>,
    stats: EngineStats,
}

impl Default for Ledger {
    fn default() -> Self {
        Self {
            next_id: 1,
            deferred: Vec::new(),
            orders: HashMap::new(),
            stats: EngineStats::default(),
        }
    }
}

/// Told about every fill, so something outside the engine can write it down.
///
/// A callback rather than the engine owning a journal, for the reason M1.c2
/// gave when the recorder needed to reach Postgres: `quant-recorder` stayed
/// DB-free via a plain on-seal callback so the arrows only pointed down. Same
/// here — this crate knows no file format and no database, and a paper binary
/// wires the journal in.
///
/// The instrument and side come from the engine because a [`Fill`] does not
/// carry them: a fill belongs to an order, and the engine is what knows which.
///
/// `portfolio` is the state *after* the fill, so an observer that writes a
/// checkpoint records what the engine actually believes rather than a total it
/// accumulated itself. A third independent tally would not be a check against
/// the engine — it would be a check against itself.
pub trait RunObserver {
    /// An order was minted and is about to reach the venue.
    ///
    /// Called **before** `ExecutionVenue::submit`, which is the same rule fills
    /// already follow for the same reason: at M8 an order at a live venue that
    /// we never wrote down is an orphan position, and no amount of reading
    /// afterwards recovers it. The cost is one durable append inside the
    /// strategy's own call stack, which is charged identically in all three
    /// worlds because production charges it anyway.
    ///
    /// `mark` is the mid the risk layer priced this request against, or `None`
    /// when the book had no prices. The engine computes it and drops it today;
    /// it is the one input to a refusal decision that cannot be re-derived,
    /// because it depends on the book *as the engine saw it*.
    fn on_submitted(
        &mut self,
        _client_order_id: ClientOrderId,
        _request: &OrderRequest,
        _mark: Option<Px>,
        _at: Ts,
    ) {
    }

    /// An order was refused before the venue heard about it.
    ///
    /// `by` separates "we built a malformed order" from "we built a fine order
    /// the limits declined". Collapsing them would bury a bug of ours in a
    /// counter of events that are working as designed.
    ///
    /// `bound` names which limit stopped it, and is `None` for a seam refusal —
    /// the seam is not a limit, and a malformed order breached nothing. It
    /// deliberately does not reach the strategy: `RejectReason` stays two values
    /// wide, because a strategy that could tell "too large" from "the daily loss
    /// budget is spent" could trade around the limits.
    fn on_refused(
        &mut self,
        _client_order_id: ClientOrderId,
        _request: &OrderRequest,
        _reason: RejectReason,
        _bound: Option<crate::risk::Bound>,
        _by: RefusedBy,
        _at: Ts,
    ) {
    }

    /// The venue acknowledged an order.
    fn on_accepted(&mut self, _client_order_id: ClientOrderId, _at: Ts) {}

    /// The venue declined an order it had been told about.
    fn on_rejected(&mut self, _client_order_id: ClientOrderId, _reason: RejectReason, _at: Ts) {}

    /// A cancel was sent. Whether it won the race is a separate call.
    fn on_cancel_requested(&mut self, _client_order_id: ClientOrderId, _at: Ts) {}

    /// An order was withdrawn, with whatever never traded.
    fn on_cancelled(&mut self, _client_order_id: ClientOrderId, _remaining: Qty, _at: Ts) {}

    /// The kill switch was thrown, at the instant it was thrown.
    ///
    /// Written here rather than at shutdown, which is where M5 put it and is
    /// the hole `CLAUDE.md` has carried as *must be fixed before M8* ever
    /// since. Two things were wrong with the old placement and only one of
    /// them was ever written down: a hard kill between the trip and the
    /// shutdown lost the trip entirely, so the supervisor re-armed a switch
    /// that had fired — but even on a clean exit the entry was stamped with
    /// the *shutdown* time, so the record of the single most serious thing the
    /// risk layer can do named the wrong moment, by however long the session
    /// happened to continue.
    ///
    /// This is the write-ahead rule the journal already applies to fills and
    /// submissions, arriving at the third place that needs it.
    fn on_tripped(&mut self, _cause: crate::risk::TripCause, _at: Ts) {}

    /// We stopped being able to see the market.
    ///
    /// `last_good_ts` is the last instant the stream was known intact, so the
    /// pair bounds the blindness. The engine has always *counted* gaps in
    /// `stats.gaps`; this is the first time one is written down with a time
    /// attached, which is the difference between knowing there were 34 and
    /// knowing whether we were blind at 03:30.
    fn on_blind(&mut self, _cause: quant_core::event::GapCause, _last_good_ts: Ts, _at: Ts) {}

    /// The strategy wrote something down.
    ///
    /// `at` is the engine's clock, not the strategy's: the strategy accumulates
    /// notes during its own callbacks and the engine takes them at the end of
    /// the same event, so the two instants are identical by construction — and
    /// stamping here rather than there means a strategy cannot date its own
    /// entries, which is invariant 4 holding at one more seam.
    fn on_note(&mut self, _note: &crate::strategy::Note, _at: Ts) {}

    /// A fill arrived for an order this engine has no record of.
    ///
    /// The `else` the loop body does not have: `stats.fills` is incremented
    /// unconditionally and the booking sits inside an `if let` with no
    /// alternative, so a fill naming an unknown order is counted and then
    /// silently dropped — not booked into the portfolio, not journalled, and
    /// invisible in every output. Impossible with `SimulatedVenue`, which only
    /// fills what it was given; **not** impossible at M8, where a venue can
    /// report a fill for an order we lost track of across a restart. That is
    /// exactly the case where silence is most expensive.
    fn on_orphaned(&mut self, _client_order_id: ClientOrderId, _fill: &Fill, _at: Ts) {}

    /// Something traded, and it was ours.
    ///
    /// `client_order_id` is **the engine's own id**, not a count of fills. It
    /// was absent until M7.5, so the only writer invented
    /// `ClientOrderId(self.fills)` — a fill ordinal, correct exactly while
    /// nothing is ever refused, because `Ledger::mint` hands out ids densely and
    /// a refused order consumes one *before* the risk check.
    fn on_fill(
        &mut self,
        _client_order_id: ClientOrderId,
        _instrument: InstrumentId,
        _side: Side,
        _fill: &Fill,
        _at: Ts,
        _portfolio: &Portfolio,
    ) {
    }
}

/// Which layer refused an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusedBy {
    /// The seam: the request was not well-formed enough to send anywhere. Ours.
    Seam,
    /// The risk layer: a fine request the limits declined. Working as designed.
    Risk,
}

/// The loop.
pub struct Engine<S, V, R, K> {
    source: S,
    venue: V,
    risk: R,
    strategy: K,
    /// One book per instrument, indexed by [`InstrumentId::index`], which is a
    /// dense registry index. Grown on demand rather than pre-sized, so a run
    /// that touches one instrument pays for one book.
    books: Vec<Book>,
    now: Ts,
    scratch: Vec<ExecutionEvent>,
    /// Reused across events so the common case -- a strategy with nothing to
    /// say -- allocates nothing. Same reason `scratch` exists.
    notes: Vec<crate::strategy::Note>,
    ledger: Ledger,
    portfolio: Portfolio,
    /// Optional, because a backtest has nothing worth journalling: it can be
    /// re-run from raw, and a two-week paper session cannot.
    observer: Option<Box<dyn RunObserver + Send>>,
}

impl<S: core::fmt::Debug, V: core::fmt::Debug, R: core::fmt::Debug, K: core::fmt::Debug>
    core::fmt::Debug for Engine<S, V, R, K>
{
    /// Hand-written because the fill observer is a trait object that need not be
    /// `Debug` -- it is a callback into a file, and printing it would say
    /// nothing anyone wants.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Engine")
            .field("source", &self.source)
            .field("venue", &self.venue)
            .field("risk", &self.risk)
            .field("strategy", &self.strategy)
            .field("now", &self.now)
            .field("portfolio", &self.portfolio)
            .finish_non_exhaustive()
    }
}

impl<S, V, R, K> Engine<S, V, R, K>
where
    S: EventSource,
    V: ExecutionVenue,
    R: RiskLayer,
    K: Strategy,
{
    /// Wire one up with `starting_cash`.
    ///
    /// Capital is a constructor argument and not a default, because a run
    /// without it is meaningless and a default of zero would silently produce an
    /// equity curve of zero that looks like a flat strategy.
    pub fn new(source: S, venue: V, risk: R, strategy: K, starting_cash: Notional) -> Self {
        Self {
            source,
            venue,
            risk,
            strategy,
            books: Vec::new(),
            now: Ts::from_nanos(0),
            scratch: Vec::new(),
            notes: Vec::new(),
            ledger: Ledger::default(),
            portfolio: Portfolio::new(starting_cash),
            observer: None,
        }
    }

    /// Have every fill reported to `observer` as it is booked.
    ///
    /// Called *after* the portfolio applies the fill and *before* the strategy
    /// is told, so a journal entry exists before anything can act on the fill.
    /// The other order would allow a strategy to submit on a fill that a crash
    /// then erased from the record.
    #[must_use]
    pub fn observing_fills(mut self, observer: Box<dyn RunObserver + Send>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Start from a position already established, recovered from a journal.
    ///
    /// A restart on day nine of a two-week run has to come back up holding what
    /// it held, or the strategy's first act is to trade against a position it
    /// does not know it has.
    #[must_use]
    pub fn resuming(mut self, portfolio: Portfolio) -> Self {
        self.portfolio = portfolio;
        self
    }

    /// Continue minting client order ids from `next`, after a restart.
    ///
    /// `Ledger::mint` hands out ids densely from 1, and `next_id` resets to 1
    /// with the process. [`Self::resuming`] replaces only the portfolio, so a
    /// resumed run would mint id 1 again and the journal would hold two
    /// different orders claiming it.
    ///
    /// That is not cosmetic. M7.5's first acceptance criterion is that a **hole
    /// in the id sequence is a decision that was not written down** — a property
    /// that only works if the ids are dense and unique across the whole file.
    /// Duplicates would make the check report holes where there are none, which
    /// is the "cries wolf on good data" failure M1's verifier already taught
    /// this project once.
    ///
    /// Recovered as `max(id) + 1` over the journal rather than stored as a
    /// counter: the ids themselves are the record, and a separate counter would
    /// be a second thing that must agree with them forever.
    #[must_use]
    pub const fn minting_from(mut self, next: u64) -> Self {
        self.ledger.next_id = next;
        self
    }

    /// Run to the end of the source.
    ///
    /// A source error stops the run and is returned, rather than being treated
    /// as the end of the stream — see [`EventSource`] for why that distinction
    /// is the difference between a short backtest and a wrong one.
    pub fn run(&mut self) -> Result<EngineStats, SourceError> {
        while let Some(item) = self.source.next_event() {
            self.step(&item?);
        }
        Ok(self.ledger.stats)
    }

    /// Everything that happens because of one event, in the order of the module
    /// docs.
    fn step(&mut self, event: &MarketEvent) {
        // A macro and not a method, because a method would borrow the whole
        // engine and the strategy has to be called with the context in hand.
        // Expanding to explicit field borrows keeps them disjoint, which is the
        // borrow checker enforcing the thing the design already wanted: the
        // strategy and the things it may touch are separate parts of the engine.
        macro_rules! ctx {
            () => {
                Context::new(
                    &self.books,
                    self.now,
                    &mut self.venue,
                    &mut self.risk,
                    &mut self.ledger,
                    &self.portfolio,
                    // Disjoint from the five borrows above, so this compiles
                    // without restructuring the loop. `Context::new` is
                    // `pub(crate)`, so no external signature moves.
                    self.observer.as_deref_mut(),
                )
            };
        }

        // 1. The clock. Nothing else advances it.
        self.now = event.meta().local_recv_ts;
        self.ledger.stats.events += 1;
        self.ledger.stats.first_ts.get_or_insert(self.now);
        self.ledger.stats.last_ts = Some(self.now);
        if let MarketEvent::Gap(gap) = event {
            self.ledger.stats.gaps += 1;
            // Before the book is cleared, which is the next step. The order is
            // not load-bearing for correctness -- nothing reads the book here
            // -- but it keeps the file's story in the order it happened.
            if let Some(observer) = self.observer.as_deref_mut() {
                observer.on_blind(gap.cause, gap.last_good_ts, self.now);
            }
        }

        // 2. The book. A gap clears it, so there are no stale prices to read.
        let index = event.meta().instrument.index();
        if self.books.len() <= index {
            self.books.resize_with(index + 1, Book::new);
        }
        self.books[index].apply(event);

        // 3. The venue matches orders that were already resting.
        self.venue.observe(event, &self.books[index], self.now);
        self.scratch.clear();
        self.venue.poll(self.now, &mut self.scratch);

        // 4. Book what happened, then tell the strategy. Booking first means a
        //    strategy reading its own position during `on_execution` sees the
        //    fill it is being told about, rather than the state before it.
        for i in 0..self.scratch.len() {
            let execution = self.scratch[i].clone();
            self.ledger.stats.execution_events += 1;
            if let ExecutionEvent::Filled { fill, .. } = &execution {
                self.ledger.stats.fills += 1;
                if let Some(&(instrument, side)) =
                    self.ledger.orders.get(&execution.client_order_id())
                {
                    self.portfolio.apply_fill(instrument, side, fill);
                    // Written down *before* the strategy hears about it, so a
                    // crash cannot erase a fill the strategy has already acted
                    // on.
                    if let Some(observer) = self.observer.as_mut() {
                        observer.on_fill(
                            execution.client_order_id(),
                            instrument,
                            side,
                            fill,
                            execution.ts(),
                            &self.portfolio,
                        );
                    }
                    // Risk keeps its own tally, so it has to see fills too. It
                    // deliberately does not read the portfolio: a limit computed
                    // from the accounting can only be as correct as the
                    // accounting, and would fail in the same direction.
                    self.risk.on_fill(instrument, side, fill, execution.ts());
                } else if let Some(observer) = self.observer.as_mut() {
                    // The `else` this loop did not have. `stats.fills` is
                    // already incremented above, so a fill naming an order we
                    // have no record of was counted and then silently dropped --
                    // not booked, not journalled, invisible in every output.
                    // Impossible with `SimulatedVenue`, which only fills what it
                    // was given; not impossible at M8, where a venue can report
                    // a fill for an order we lost across a restart, which is
                    // exactly when silence costs most.
                    observer.on_orphaned(execution.client_order_id(), fill, execution.ts());
                }
            }
            // The rest of the lifecycle, reported where the engine already
            // dispatches it rather than anywhere new.
            if let Some(observer) = self.observer.as_mut() {
                match &execution {
                    ExecutionEvent::Accepted { ts, .. } => {
                        observer.on_accepted(execution.client_order_id(), *ts);
                    }
                    ExecutionEvent::Rejected { reason, ts, .. } => {
                        observer.on_rejected(execution.client_order_id(), *reason, *ts);
                    }
                    ExecutionEvent::Cancelled { remaining, ts, .. } => {
                        observer.on_cancelled(execution.client_order_id(), *remaining, *ts);
                    }
                    // Already reported above, with the portfolio it produced.
                    ExecutionEvent::Filled { .. } => {}
                }
            }
            if execution.is_terminal() {
                self.ledger.orders.remove(&execution.client_order_id());
            }
            let mut ctx = ctx!();
            self.strategy.on_execution(&execution, &mut ctx);
            // Rejections raised inside that callback, delivered before the next
            // one. Drained rather than iterated once, because a strategy may
            // submit from `on_execution` and be refused again.
            while let Some(refusal) = self.ledger.deferred.pop() {
                self.ledger.stats.execution_events += 1;
                let mut ctx = ctx!();
                self.strategy.on_execution(&refusal, &mut ctx);
            }
        }

        // 5. Only now does the strategy see the market event. An order it
        //    submits here is not eligible to trade against this event.
        let mut ctx = ctx!();
        self.strategy.on_market_event(event, &mut ctx);
        while let Some(refusal) = self.ledger.deferred.pop() {
            self.ledger.stats.execution_events += 1;
            let mut ctx = ctx!();
            self.strategy.on_execution(&refusal, &mut ctx);
        }

        // 6. Ask the strategy what it wrote down, once the event is fully dealt
        //    with. Last on purpose: a note taken between steps 4 and 5 would
        //    miss everything `on_market_event` decided, which is where a
        //    crossover strategy does all of its deciding. Taken even when the
        //    strategy is silent, because `take_notes` is how it says so.
        self.notes.clear();
        self.strategy.take_notes(&mut self.notes);
        if let Some(observer) = self.observer.as_deref_mut() {
            for note in &self.notes {
                observer.on_note(note, self.now);
            }
        }
    }

    /// The strategy, after the run — for reading whatever it recorded.
    pub const fn strategy(&self) -> &K {
        &self.strategy
    }

    /// The venue, after the run.
    pub const fn venue(&self) -> &V {
        &self.venue
    }

    #[must_use]
    pub const fn stats(&self) -> EngineStats {
        self.ledger.stats
    }

    /// Cash, holdings and P&L after the run.
    #[must_use]
    pub const fn portfolio(&self) -> &Portfolio {
        &self.portfolio
    }

    /// The risk layer, after the run — for reading whether it tripped.
    #[must_use]
    pub const fn risk(&self) -> &R {
        &self.risk
    }

    /// The reconstructed book for an instrument, after the run.
    #[must_use]
    pub fn book(&self, instrument: InstrumentId) -> Option<&Book> {
        self.books.get(instrument.index())
    }
}

impl Ledger {
    /// Refuse a request at the seam, before any venue sees it.
    ///
    /// The order still gets its id, because the caller was promised one and a
    /// refused order still has to be something the strategy can reconcile
    /// against.
    fn refuse(&mut self, client_order_id: ClientOrderId, reason: RejectReason, ts: Ts) {
        self.stats.refused += 1;
        self.deferred.push(ExecutionEvent::Rejected {
            client_order_id,
            reason,
            ts,
        });
    }

    /// Mint the next client order id.
    fn mint(&mut self) -> ClientOrderId {
        let id = ClientOrderId(self.next_id);
        self.next_id += 1;
        id
    }
}

/// Whether a request is well-formed enough to send anywhere.
pub(crate) fn seam_check(request: &OrderRequest) -> Option<RejectReason> {
    (!request.is_valid()).then_some(RejectReason::Malformed)
}

#[cfg(test)]
mod tests;
