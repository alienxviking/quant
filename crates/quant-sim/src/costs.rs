//! What trading costs, beyond the price.
//!
//! M3's simulator was free: every fill cost exactly the prices in the book. M4
//! is the milestone that makes it expensive, and its criterion is not "the model
//! is accurate" — it cannot be, at our size, from recorded data — but that
//! **results degrade sensibly**. Two properties make that falsifiable, and both
//! are tested:
//!
//! 1. **Zero costs must be a no-op.** [`Costs::NONE`] has to reproduce the M3
//!    numbers to the last digit. If it does not, the cost machinery changed
//!    something it had no business touching.
//! 2. **Costs must be monotonic.** For fee rates `a < b`, the result under `b`
//!    is never better than under `a`. That is the property a sign error breaks,
//!    and a sign error on a fee is otherwise invisible — it just looks like a
//!    surprisingly good strategy.
//!
//! # What is a model here, and what is a knob
//!
//! **Fees are a model**: venues publish their schedules, so the number is looked
//! up rather than guessed.
//!
//! **Latency is a model**: it is one number, measurable against a venue, and
//! everything downstream of it follows. Notably it is where *slippage* comes
//! from — the book moves between deciding and arriving, and walking the book at
//! arrival time prices that automatically. There is no separate slippage model
//! because there does not need to be one.
//!
//! **[`Costs::adverse_per_fill`] is a knob**, and is labelled as one. It exists
//! for stress tests, defaults to zero, and is not calibrated against anything.
//! An uncalibrated number presented as a model is worse than no model, because
//! the output looks equally authoritative either way.
//!
//! **Queue position and market impact are neither.** They are not recoverable
//! from recorded data — our order was never in that book, and nobody reacted to
//! it — which is why M5 (paper trading) exists rather than a cleverer simulator.

use quant_core::fixed::{Notional, Px, Rate};

/// What the venue charges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeSchedule {
    /// Charged when our order was the resting side.
    ///
    /// May be negative: some venues pay for passive flow, and a schedule that
    /// could not express a rebate would misprice every passive strategy.
    pub maker: Rate,
    /// Charged when our order crossed the spread.
    pub taker: Rate,
}

impl FeeSchedule {
    /// No fees. M3's behaviour, and the baseline the no-op test compares to.
    pub const FREE: Self = Self {
        maker: Rate::ZERO,
        taker: Rate::ZERO,
    };

    /// Binance spot's standard tier: 0.1% both sides, ten basis points.
    ///
    /// The published number for an account with no volume history and no BNB
    /// discount, which is exactly what M8 will be. Using the discounted rate
    /// would be assuming a benefit we have not earned.
    #[must_use]
    pub fn binance_spot() -> Self {
        Self {
            maker: "0.001".parse().expect("a valid rate"),
            taker: "0.001".parse().expect("a valid rate"),
        }
    }

    /// Flat rate on both sides.
    #[must_use]
    pub const fn flat(rate: Rate) -> Self {
        Self {
            maker: rate,
            taker: rate,
        }
    }

    /// The fee on `gross`, which must be the absolute value traded.
    ///
    /// Positive is a cost. Charged in the **quote** currency, which is a
    /// simplification worth naming: a spot venue buying BTC with USDT takes its
    /// fee in BTC, so the position ends a fraction smaller rather than the cash
    /// a fraction lower. The difference is second-order at any size we will
    /// trade, and modelling it properly needs two balances — which is M5's
    /// problem, where a real statement can settle it.
    #[must_use]
    pub fn fee(&self, gross: Notional, is_maker: bool) -> Notional {
        let rate = if is_maker { self.maker } else { self.taker };
        gross
            .scaled_by(rate)
            .expect("a fee is smaller than the notional it is charged on")
    }
}

impl Default for FeeSchedule {
    fn default() -> Self {
        Self::FREE
    }
}

/// How long things take.
///
/// One number each way, in nanoseconds of event time. Not a distribution: a
/// distribution needs a random draw, and a random draw makes a backtest
/// irreproducible unless the seed becomes part of the result. A fixed
/// worst-plausible number is both honest and repeatable, and the engine
/// contract's determinism criterion depends on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Latency {
    /// From our decision to the venue having the order.
    pub outbound: i64,
    /// From the venue acting to us knowing about it.
    ///
    /// Separate from `outbound` because they are not symmetric in effect.
    /// Outbound latency changes *what price we get*; inbound changes *when we
    /// find out*, and therefore when the strategy's next decision is made on
    /// correct information. A strategy that reconciles its position will behave
    /// differently under the two.
    pub inbound: i64,
}

const NANOS_PER_MILLI: i64 = 1_000_000;

impl Latency {
    /// Instant. M3's behaviour.
    pub const NONE: Self = Self {
        outbound: 0,
        inbound: 0,
    };

    /// Symmetric latency in milliseconds.
    #[must_use]
    pub const fn millis(each_way: i64) -> Self {
        Self {
            outbound: each_way * NANOS_PER_MILLI,
            inbound: each_way * NANOS_PER_MILLI,
        }
    }

    /// A plausible retail round trip to Binance from a home connection.
    ///
    /// 50 ms each way. Not measured from anywhere in particular, which is why it
    /// is named for what it is rather than presented as a fact — but it is the
    /// right order of magnitude, and being wrong by a factor of two here matters
    /// far less than assuming zero.
    #[must_use]
    pub const fn retail() -> Self {
        Self::millis(50)
    }
}

/// Everything that makes a fill worse than the book suggested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Costs {
    pub fees: FeeSchedule,
    pub latency: Latency,
    /// A flat price concession per fill, always against us.
    ///
    /// **A stress knob, not a model.** Zero by default and calibrated against
    /// nothing. It exists to answer "how much worse would this have to get
    /// before the conclusion changes", which is a useful question, and *not* to
    /// be quoted as a slippage estimate. Real slippage in this simulator comes
    /// from `latency` moving the book before arrival, which at least has a
    /// number behind it.
    pub adverse_per_fill: Px,
}

impl Costs {
    /// Free and instant: exactly M3.
    pub const NONE: Self = Self {
        fees: FeeSchedule::FREE,
        latency: Latency::NONE,
        adverse_per_fill: Px::ZERO,
    };

    /// Fees and latency a retail account would actually face.
    #[must_use]
    pub fn retail() -> Self {
        Self {
            fees: FeeSchedule::binance_spot(),
            latency: Latency::retail(),
            adverse_per_fill: Px::ZERO,
        }
    }

    /// Whether this is the free, instant case.
    ///
    /// Used to say so in the output: a result produced with no costs has to
    /// announce that, per the engine contract.
    #[must_use]
    pub fn is_free(&self) -> bool {
        *self == Self::NONE
    }
}
