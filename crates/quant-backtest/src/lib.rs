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

#[cfg(test)]
mod tests;
