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
//! `LiveSource + PaperVenue`. Live is `LiveSource + LiveVenue`. The strategy is
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

pub mod risk;
pub mod strategy;
pub mod venue;

use quant_book::Book;
use quant_core::event::MarketEvent;
use quant_core::execution::{ClientOrderId, ExecutionEvent, OrderRequest, RejectReason};
use quant_core::instrument::InstrumentId;
use quant_core::source::{EventSource, SourceError};
use quant_core::time::Ts;

pub use risk::{AllowAll, RiskLayer};
pub use strategy::{Context, Strategy};
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

/// The loop.
#[derive(Debug)]
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
    next_id: u64,
    /// Execution events produced during a strategy callback — a risk rejection,
    /// or a seam rejection — waiting to be delivered.
    ///
    /// Deferred rather than delivered inline because a strategy must not be
    /// re-entered while it is on the stack, and because even a rejection should
    /// arrive the way every other outcome does: as an event, afterwards.
    deferred: Vec<ExecutionEvent>,
    scratch: Vec<ExecutionEvent>,
    stats: EngineStats,
}

impl<S, V, R, K> Engine<S, V, R, K>
where
    S: EventSource,
    V: ExecutionVenue,
    R: RiskLayer,
    K: Strategy,
{
    /// Wire one up.
    pub fn new(source: S, venue: V, risk: R, strategy: K) -> Self {
        Self {
            source,
            venue,
            risk,
            strategy,
            books: Vec::new(),
            now: Ts::from_nanos(0),
            next_id: 1,
            deferred: Vec::new(),
            scratch: Vec::new(),
            stats: EngineStats::default(),
        }
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
        Ok(self.stats)
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
                    &mut self.next_id,
                    &mut self.deferred,
                    &mut self.stats,
                )
            };
        }

        // 1. The clock. Nothing else advances it.
        self.now = event.meta().local_recv_ts;
        self.stats.events += 1;
        self.stats.first_ts.get_or_insert(self.now);
        self.stats.last_ts = Some(self.now);
        if matches!(event, MarketEvent::Gap(_)) {
            self.stats.gaps += 1;
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
        self.venue.poll(&mut self.scratch);

        // 4. Tell the strategy what happened to its orders.
        for i in 0..self.scratch.len() {
            let execution = self.scratch[i].clone();
            self.stats.execution_events += 1;
            if matches!(execution, ExecutionEvent::Filled { .. }) {
                self.stats.fills += 1;
            }
            let mut ctx = ctx!();
            self.strategy.on_execution(&execution, &mut ctx);
            // Rejections raised inside that callback, delivered before the next
            // one. Drained rather than iterated once, because a strategy may
            // submit from `on_execution` and be refused again.
            while let Some(refusal) = self.deferred.pop() {
                self.stats.execution_events += 1;
                let mut ctx = ctx!();
                self.strategy.on_execution(&refusal, &mut ctx);
            }
        }

        // 5. Only now does the strategy see the market event. An order it
        //    submits here is not eligible to trade against this event.
        let mut ctx = ctx!();
        self.strategy.on_market_event(event, &mut ctx);
        while let Some(refusal) = self.deferred.pop() {
            self.stats.execution_events += 1;
            let mut ctx = ctx!();
            self.strategy.on_execution(&refusal, &mut ctx);
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
        self.stats
    }

    /// The reconstructed book for an instrument, after the run.
    #[must_use]
    pub fn book(&self, instrument: InstrumentId) -> Option<&Book> {
        self.books.get(instrument.index())
    }
}

/// Refuse a request at the seam, before any venue sees it.
///
/// Used for both risk rejections and malformed requests. Returns the id anyway,
/// because the caller was promised one and an order that was refused still has
/// to be something the strategy can reconcile against.
pub(crate) fn refuse(
    deferred: &mut Vec<ExecutionEvent>,
    stats: &mut EngineStats,
    client_order_id: ClientOrderId,
    reason: RejectReason,
    ts: Ts,
) {
    stats.refused += 1;
    deferred.push(ExecutionEvent::Rejected {
        client_order_id,
        reason,
        ts,
    });
}

/// Whether a request is well-formed enough to send anywhere.
pub(crate) fn seam_check(request: &OrderRequest) -> Option<RejectReason> {
    (!request.is_valid()).then_some(RejectReason::Malformed)
}

#[cfg(test)]
mod tests;
