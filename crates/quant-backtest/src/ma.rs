//! A moving-average crossover, kept as dumb as it can be.
//!
//! Long or flat, one instrument, fixed size, market orders. No stops, no sizing
//! rule, no filter. Every one of those would be a knob, and a knob is something
//! to blame a bad result on — the point of this strategy is that a bad result
//! has nowhere to hide but the harness.
//!
//! # Why it samples on a clock rather than on every event
//!
//! An average over the last N *events* is an average over a window whose length
//! in time depends on how busy the market was, which makes the indicator faster
//! in exactly the conditions where it should be slower. Sampling the mid once
//! per interval of **event time** gives a window that means the same thing all
//! week.
//!
//! The clock is `ctx.now()`, which is the last event's `local_recv_ts`. There is
//! no other clock available, which is the point.
//!
//! # Why it will not trade across a gap
//!
//! Not by remembering to check. A gap clears the book, so `best_bid`/`best_ask`
//! are `None`, so there is no mid to sample and no sample is taken. The
//! indicator simply does not advance while we are blind, and resumes when a
//! snapshot re-anchors the book. That is the invariant being structural rather
//! than advisory.

use quant_book::Book;
use quant_core::event::{MarketEvent, Side};
use quant_core::execution::{ClientOrderId, ExecutionEvent, OrderKind, OrderRequest, TimeInForce};
use quant_core::fixed::{Px, Qty};
use quant_core::instrument::InstrumentId;
use quant_core::time::Ts;
use quant_engine::{Context, Strategy};

/// How the crossover is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaConfig {
    pub instrument: InstrumentId,
    /// Samples in the fast average.
    pub fast: usize,
    /// Samples in the slow average. Must exceed `fast`, or there is no crossing.
    pub slow: usize,
    /// Event-time between samples.
    pub interval: i64,
    /// Size of each position, in base units.
    pub qty: Qty,
}

/// What the strategy did, for reporting.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MaStats {
    pub samples: u64,
    /// Times the fast average crossed the slow one, in either direction.
    pub crossings: u64,
    pub entries: u64,
    pub exits: u64,
    /// Signals ignored because an order was already working.
    ///
    /// Not a nuisance counter: a strategy that fires while it already has an
    /// order out is a strategy that would have doubled its position live, and
    /// this is where that shows up.
    pub suppressed: u64,
    /// Intervals with no mid to sample, because the book was dark.
    pub blind_intervals: u64,
}

/// Long when the fast average is above the slow one, flat otherwise.
#[derive(Debug)]
pub struct MaCrossover {
    config: MaConfig,
    /// Most recent samples, newest last. Bounded by `slow`.
    samples: std::collections::VecDeque<Px>,
    next_sample_at: Option<Ts>,
    /// Sign of (fast - slow) at the last sample, `None` until both are defined.
    last_side: Option<bool>,
    /// The order we are waiting on, if any.
    working: Option<ClientOrderId>,
    stats: MaStats,
}

impl MaCrossover {
    #[must_use]
    pub fn new(config: MaConfig) -> Self {
        assert!(
            config.slow > config.fast && config.fast > 0,
            "a crossover needs a fast window shorter than its slow one"
        );
        Self {
            config,
            samples: std::collections::VecDeque::with_capacity(config.slow),
            next_sample_at: None,
            last_side: None,
            working: None,
            stats: MaStats::default(),
        }
    }

    #[must_use]
    pub const fn stats(&self) -> MaStats {
        self.stats
    }

    /// Mid price, or `None` when either side of the book is missing.
    ///
    /// `None` covers both "we have not seen a snapshot yet" and "a gap cleared
    /// the book", which is right: in neither case do we know what anything is
    /// worth, and the two need no distinguishing here.
    fn mid(book: &Book) -> Option<Px> {
        let (bid, ask) = (book.best_bid()?, book.best_ask()?);
        // Averaging two `1e8`-scaled prices needs no widening: their sum is at
        // most twice a price, nowhere near `i64`.
        Some(Px::from_raw((bid.px.raw() + ask.px.raw()) / 2))
    }

    /// Simple average of the newest `n` samples, if there are that many.
    fn average(&self, n: usize) -> Option<Px> {
        if self.samples.len() < n {
            return None;
        }
        let sum: i128 = self
            .samples
            .iter()
            .rev()
            .take(n)
            .map(|px| i128::from(px.raw()))
            .sum();
        let n = i128::try_from(n).expect("a window fits in i128");
        Some(Px::from_raw(
            i64::try_from(sum / n).expect("an average of prices is a price"),
        ))
    }

    fn order(&self, side: Side) -> OrderRequest {
        OrderRequest {
            instrument: self.config.instrument,
            side,
            qty: self.config.qty,
            // Market orders, so the test is of the harness and not of a queueing
            // model the simulator does not have.
            kind: OrderKind::Market,
            time_in_force: TimeInForce::Gtc,
        }
    }
}

impl Strategy for MaCrossover {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>) {
        let instrument = self.config.instrument;
        if event.meta().instrument != instrument {
            return;
        }

        let now = ctx.now();
        let due = self.next_sample_at.unwrap_or(now);
        if now < due {
            return;
        }
        // Advance to the next boundary from `due`, not from `now`, so a quiet
        // patch does not shift the sampling grid for the rest of the run.
        self.next_sample_at = Some(Ts::from_nanos(due.as_nanos() + self.config.interval));

        let Some(mid) = ctx.book(instrument).and_then(Self::mid) else {
            // Blind: the book is empty because of a gap, or has not been
            // anchored yet. The indicator does not advance, which is how "do not
            // trade across a gap" holds without a rule about gaps.
            self.stats.blind_intervals += 1;
            return;
        };

        self.stats.samples += 1;
        if self.samples.len() == self.config.slow {
            self.samples.pop_front();
        }
        self.samples.push_back(mid);

        let (Some(fast), Some(slow)) = (
            self.average(self.config.fast),
            self.average(self.config.slow),
        ) else {
            return;
        };
        let above = fast > slow;
        let crossed = self.last_side.is_some_and(|was| was != above);
        self.last_side = Some(above);
        if !crossed {
            return;
        }
        self.stats.crossings += 1;

        // One order at a time. A strategy that fired again while an order was
        // working would double its position live; suppressing it here is the
        // cost of fire-and-forget submission, charged in every world.
        if self.working.is_some() {
            self.stats.suppressed += 1;
            return;
        }

        let position = ctx.position(instrument);
        if above && position.is_flat() {
            self.stats.entries += 1;
            self.working = Some(ctx.submit(self.order(Side::Buy)));
        } else if !above && !position.is_flat() {
            self.stats.exits += 1;
            self.working = Some(ctx.submit(self.order(Side::Sell)));
        }
    }

    fn on_execution(&mut self, event: &ExecutionEvent, _ctx: &mut Context<'_>) {
        if self.working == Some(event.client_order_id()) && event.is_terminal() {
            self.working = None;
        }
    }
}
