//! Recording an equity curve without the engine knowing about it.
//!
//! [`Recorded`] wraps any [`Strategy`] and samples equity on a timer, delegating
//! everything else to the strategy inside. A decorator rather than an engine
//! feature, for two reasons: the engine has no business knowing what a report
//! is, and this composes — a risk-limit observer or a trade log would be another
//! wrapper rather than another engine field.
//!
//! # Why a missing sample is recorded as missing
//!
//! When the book is dark, a held position has no mark, so there is no equity.
//! [`EquityPoint::equity`] is `None` there, and the curve keeps the point rather
//! than skipping it. Skipping would leave a gap that a plotting tool draws a
//! straight line across — inventing a smooth passage through exactly the period
//! we could not see. Keeping the hole makes it visible.

use quant_core::event::MarketEvent;
use quant_core::execution::ExecutionEvent;
use quant_core::fixed::{Notional, Px};
use quant_core::instrument::InstrumentId;
use quant_core::time::Ts;
use quant_engine::{Context, Strategy};

/// One sample of the account's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EquityPoint {
    pub ts: Ts,
    /// Cash plus the marked position, or `None` when there was no mark.
    pub equity: Option<Notional>,
    pub cash: Notional,
    /// The mid used to mark, for reproducing the number by hand.
    pub mark: Option<Px>,
}

/// A run's equity over time, plus the numbers derived from it.
#[derive(Debug, Default, Clone)]
pub struct EquityCurve {
    pub points: Vec<EquityPoint>,
}

impl EquityCurve {
    /// The last equity that could be computed.
    #[must_use]
    pub fn last(&self) -> Option<Notional> {
        self.points.iter().rev().find_map(|p| p.equity)
    }

    /// The first equity that could be computed.
    #[must_use]
    pub fn first(&self) -> Option<Notional> {
        self.points.iter().find_map(|p| p.equity)
    }

    /// Highest and lowest equity actually observed.
    #[must_use]
    pub fn range(&self) -> Option<(Notional, Notional)> {
        let mut lo: Option<Notional> = None;
        let mut hi: Option<Notional> = None;
        for equity in self.points.iter().filter_map(|p| p.equity) {
            lo = Some(lo.map_or(equity, |l: Notional| if equity < l { equity } else { l }));
            hi = Some(hi.map_or(equity, |h: Notional| if equity > h { equity } else { h }));
        }
        Some((lo?, hi?))
    }

    /// Largest peak-to-trough fall in equity.
    ///
    /// Computed over the points that *have* an equity, which means a drawdown
    /// spanning a blind period is measured from before it to after it. That is
    /// the honest reading: we do not know what happened in between, and
    /// pretending the peak was inside the hole would understate it.
    #[must_use]
    pub fn max_drawdown(&self) -> Notional {
        let mut peak: Option<Notional> = None;
        let mut worst = Notional::from_raw(0);
        for equity in self.points.iter().filter_map(|p| p.equity) {
            if peak.is_none_or(|p: Notional| equity > p) {
                peak = Some(equity);
            }
            if let Some(p) = peak {
                let fall = Notional::from_raw(p.raw() - equity.raw());
                if fall > worst {
                    worst = fall;
                }
            }
        }
        worst
    }

    /// Samples where the account could not be valued at all.
    #[must_use]
    pub fn blind_samples(&self) -> usize {
        self.points.iter().filter(|p| p.equity.is_none()).count()
    }
}

/// Wraps a strategy and records its equity on a timer.
#[derive(Debug)]
pub struct Recorded<S> {
    inner: S,
    instrument: InstrumentId,
    interval: i64,
    next_at: Option<Ts>,
    curve: EquityCurve,
}

impl<S: Strategy> Recorded<S> {
    /// Sample every `interval` nanoseconds of event time.
    #[must_use]
    pub fn new(inner: S, instrument: InstrumentId, interval: i64) -> Self {
        Self {
            inner,
            instrument,
            interval,
            next_at: None,
            curve: EquityCurve::default(),
        }
    }

    #[must_use]
    pub const fn curve(&self) -> &EquityCurve {
        &self.curve
    }

    #[must_use]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    fn sample(&mut self, ctx: &Context<'_>) {
        let mark = ctx.book(self.instrument).and_then(|book| {
            let (bid, ask) = (book.best_bid()?, book.best_ask()?);
            Some(Px::from_raw((bid.px.raw() + ask.px.raw()) / 2))
        });
        self.curve.points.push(EquityPoint {
            ts: ctx.now(),
            equity: ctx.equity(self.instrument, mark),
            cash: ctx.cash(),
            mark,
        });
    }
}

impl<S: Strategy> Strategy for Recorded<S> {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>) {
        // The strategy acts first, so a sample taken after it reflects the same
        // event the strategy saw. Sampling first would report the account as it
        // was one decision ago.
        self.inner.on_market_event(event, ctx);

        let now = ctx.now();
        let due = self.next_at.unwrap_or(now);
        if now >= due {
            self.next_at = Some(Ts::from_nanos(due.as_nanos() + self.interval));
            self.sample(ctx);
        }
    }

    fn on_execution(&mut self, event: &ExecutionEvent, ctx: &mut Context<'_>) {
        self.inner.on_execution(event, ctx);
    }
}
