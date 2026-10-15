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
pub mod record;

pub use equity::{EquityCurve, EquityPoint, Recorded};
pub use ma::{MaConfig, MaConfigError, MaCrossover};

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

/// What the run still held at the end, as one report line.
///
/// Without it a run that ends holding something shows a gap between `cash`
/// and the last equity that nothing on the report accounts for. Fees taken in
/// the asset received make that every run rather than an occasional one: an
/// exit sells what it holds rounded down to the lot, so a few satoshi of dust
/// are left behind and ride the price to the end.
///
/// The mark is the last sample's mid, the one the curve valued the run at,
/// and it is said where there was none -- a holding with no price is not
/// worth zero, it is not worth anything we can name, and printing a value for
/// it would invent one. `last` is the last point of the curve, mark or not.
#[must_use]
pub fn position_line(position: quant_engine::Position, last: Option<&EquityPoint>) -> String {
    if position.is_flat() {
        return "position  flat at the end".to_owned();
    }
    let qty = position.qty;
    match last.and_then(|point| point.mark) {
        Some(mark) => match mark.notional(qty) {
            Some(value) => format!(
                "position  {qty} held at the end, worth {value} at the last sample's mid {mark}"
            ),
            None => format!(
                "position  {qty} held at the end, too large to value at the last sample's mid {mark}"
            ),
        },
        None => format!(
            "position  {qty} held at the end, and no mark at the last sample to value it at"
        ),
    }
}

#[cfg(test)]
mod tests;
