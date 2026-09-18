//! Running a strategy over recorded data, and saying honestly what happened.
//!
//! Two things live here: a deliberately naive strategy, and an equity recorder
//! that wraps any strategy without the engine having to know about either.
//!
//! # Why the strategy is deliberately naive
//!
//! `docs/engine-contract.md` §7 makes "the equity curve is unimpressive" a
//! criterion rather than an observation. A moving-average crossover that looked
//! profitable on its first run would mean the harness was lying — dispatching on
//! the wrong timestamp, filling at prices the book never showed, or ignoring
//! costs — and every one of those is invisible in the output. A bad result from
//! a naive idea is evidence that the plumbing is honest. A good one is a bug
//! report.
//!
//! So the strategy is not trying to be good. It is trying to be *attributable*:
//! simple enough that any surprise in the result is the harness's fault.

pub mod equity;
pub mod ma;

pub use equity::{EquityCurve, EquityPoint, Recorded};
pub use ma::{MaConfig, MaCrossover};

/// The risk limits, as one report line.
///
/// Shared by `paper` and `backtest` because the two lines exist to be *diffed*
/// against each other -- M5's criterion compares a paper session with a backtest
/// over the same window, and the first thing to check on a divergence is that
/// both ran under the same limits. They were not comparable by eye: `paper`
/// printed the `Option<Notional>` with `{:?}`, giving
/// `order Some(Notional(200))`, while `backtest` printed `order 200`. Two
/// formattings of one fact is the shape this project keeps paying for, so there
/// is now one.
#[must_use]
pub fn limits_line(limits: quant_engine::Limits) -> String {
    if limits == quant_engine::Limits::default() {
        return "limits    none set -- nothing can be refused".to_owned();
    }
    let show = |limit: Option<quant_core::fixed::Notional>| {
        limit.map_or_else(|| "none".to_owned(), |value| value.to_string())
    };
    format!(
        "limits    order {}, position {}, daily loss {}, orders/day {}",
        show(limits.max_order_notional),
        show(limits.max_position_notional),
        show(limits.max_daily_loss),
        limits
            .max_orders_per_day
            .map_or_else(|| "none".to_owned(), |n| n.to_string()),
    )
}

#[cfg(test)]
mod tests;
