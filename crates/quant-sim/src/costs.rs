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
//!    surprisingly good strategy. It holds for fees taken in the quote, and
//!    only there. Taken in the asset received ([`FeeIn::Received`]) a fee also
//!    changes how much is held, so a strategy that sells whole lots can hold
//!    nearly a lot more after a higher fee, and what that holding does next can
//!    go either way, as a move during latency can.
//!
//!    And even in the quote it holds only where no daily loss limit can bind.
//!    Fees count against `max_daily_loss`, and a tripped switch stays tripped
//!    for the rest of the run, so a higher fee can trip it sooner and stop a
//!    losing strategy before the losses a cheaper run goes on to take. The
//!    property is of the cost model, not of a run under limits: M4's test
//!    wires a risk layer that permits everything, which is why it holds there.
//!
//! # What is a model here, and what is a knob
//!
//! **Fees are a model**: venues publish their schedules, so the number is looked
//! up rather than guessed. Which asset a fee is taken in is part of the same
//! model. By default every fee is taken in the quote, which is a choice of
//! asset and not a fee set to zero: see [`FeeIn`].
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

use quant_core::event::Side;
use quant_core::execution::FeeAsset;
use quant_core::fixed::{Notional, Px, Qty, Rate};

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

    /// The rate this side of the trade pays.
    const fn rate(&self, is_maker: bool) -> Rate {
        if is_maker {
            self.maker
        } else {
            self.taker
        }
    }

    /// The fee on `gross`, which must be the absolute value traded, in the
    /// quote.
    ///
    /// Positive is a cost. How every fill is charged under [`FeeIn::Quote`],
    /// and a sell under [`FeeIn::Received`]; a buy under `Received` pays
    /// [`Self::fee_on_quantity`] instead. [`Costs::charge`] decides which.
    ///
    /// Truncated toward zero, so a fee is never rounded up into money the venue
    /// did not charge.
    #[must_use]
    pub fn fee(&self, gross: Notional, is_maker: bool) -> Notional {
        gross
            .scaled_by(self.rate(is_maker))
            .expect("a fee is smaller than the notional it is charged on")
    }

    /// The fee on `qty`, which must be the absolute size traded, as a quantity
    /// of what was traded.
    ///
    /// What a spot venue takes from a buyer: the rate on the quantity bought,
    /// in the base -- 0.000001 BTC on a 0.001 BTC buy at ten basis points. How
    /// a buy is charged under [`FeeIn::Received`].
    ///
    /// Truncated toward zero, as [`Self::fee`] is and by the same arithmetic
    /// ([`Qty::scaled_by`]), so it is never rounded up into coins the venue did
    /// not take. Whether the venue rounds the same way is a question its own
    /// fill reports answer, fill by fill; this says what the model does.
    #[must_use]
    pub fn fee_on_quantity(&self, qty: Qty, is_maker: bool) -> Qty {
        qty.scaled_by(self.rate(is_maker))
            .expect("a fee is smaller than the quantity it is charged on")
    }
}

impl Default for FeeSchedule {
    fn default() -> Self {
        Self::FREE
    }
}

/// Which asset a fill's fee is taken in.
///
/// M4 charged every fee in the quote and named that a simplification. A spot
/// venue takes its fee out of what the fill *delivered*: a buyer of BTC with
/// USDT pays in BTC and holds a fraction less than it bought, and a seller pays
/// in USDT. At the fill the two charge the same value, but for rounding. After
/// it they part: a fee taken in the base is a quantity of the asset, so what it
/// costs moves with the price until the position is sold, and any dust it
/// leaves is worth whatever the price later says. Equity is not the same either
/// way: over M5's fortnight it ends at 31.70996473 with fees in the asset
/// received against 31.64743842 in the quote.
///
/// Where the fee lands also changes what is traded, not only what it is worth.
/// `MaCrossover` exits by selling what it holds rounded down to the lot, so
/// with fees taken in the base an exit can be a lot smaller than the entry, and
/// the dust it leaves decides the size of the next. Replaying M5's
/// fortnight that way, 42 of its 412 exits sold 0.00099 rather than 0.001. A
/// model charging the quote sells 0.001 every time, and gets each of those
/// wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FeeIn {
    /// Every fee in the quote. The default, because every published result in
    /// this repository was computed this way -- M4's table and M5's fortnight
    /// among them -- and a default that moved them would leave nothing to check
    /// a change to the money path against.
    #[default]
    Quote,
    /// The asset the fill delivered, as a spot venue charges: a buy's fee in the
    /// base, as a quantity ([`FeeSchedule::fee_on_quantity`]), and a sell's in
    /// the quote ([`FeeSchedule::fee`]). What the model replay for M8's criteria
    /// B and C is to run with, so that the exits it predicts are the size live
    /// sends.
    Received,
}

/// How a report names it, after "taken": fees "taken in the quote".
impl core::fmt::Display for FeeIn {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Quote => "in the quote",
            Self::Received => "in the asset received (a buy's in the base)",
        })
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
    /// Which asset each fill's fee is taken in: the quote unless a run asks
    /// otherwise. See [`FeeIn`].
    pub fee_in: FeeIn,
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
        fee_in: FeeIn::Quote,
        latency: Latency::NONE,
        adverse_per_fill: Px::ZERO,
    };

    /// Fees and latency a retail account would actually face.
    ///
    /// With the fees still taken in the quote, although a retail spot account
    /// pays a buy's fee in the base. M4's table and M5's money check were both
    /// computed with this, so the asset is chosen beside it ([`FeeIn`]) rather
    /// than folded into it, and those numbers keep reproducing.
    #[must_use]
    pub fn retail() -> Self {
        Self {
            fees: FeeSchedule::binance_spot(),
            fee_in: FeeIn::Quote,
            latency: Latency::retail(),
            adverse_per_fill: Px::ZERO,
        }
    }

    /// Whether this is the free, instant case: no fee, no latency, no
    /// concession.
    ///
    /// Which asset a fee would be taken in does not enter into it. A fee of
    /// zero is zero in either asset, so a free run is free whichever way
    /// `fee_in` points.
    #[must_use]
    pub fn is_free(&self) -> bool {
        self.fees == FeeSchedule::FREE
            && self.latency == Latency::NONE
            && self.adverse_per_fill == Px::ZERO
    }

    /// What one fill is charged, and in which asset.
    ///
    /// `qty` is the size filled and `px` the price it filled at, as the venue
    /// reports them, and `side` is the order's. Returns the [`Fill`]'s `fee`
    /// and `fee_asset`. Under [`FeeAsset::Base`] the `Notional` holds a
    /// quantity of the base, which is how a `Fill` carries one, and
    /// [`Fill::fee_in_quote`] is the one conversion back to money.
    ///
    /// # Panics
    ///
    /// If the value traded, or the fee on it, does not fit the fixed-point
    /// range, which needs inputs that were wrong before they got here.
    ///
    /// [`Fill`]: quant_core::execution::Fill
    /// [`Fill::fee_in_quote`]: quant_core::execution::Fill::fee_in_quote
    #[must_use]
    pub fn charge(&self, side: Side, px: Px, qty: Qty, is_maker: bool) -> (Notional, FeeAsset) {
        match (self.fee_in, side) {
            // A buy receives the base, so a spot venue takes its cut of that.
            (FeeIn::Received, Side::Buy) => {
                let fee = self.fees.fee_on_quantity(qty, is_maker);
                (Notional::from_raw(fee.raw()), FeeAsset::Base)
            }
            // A sell receives the quote; and under `Quote` everything is.
            (FeeIn::Received, Side::Sell) | (FeeIn::Quote, Side::Buy | Side::Sell) => {
                let gross = px
                    .notional(qty)
                    .expect("a notional beyond i64 means the inputs were wrong");
                (self.fees.fee(gross, is_maker), FeeAsset::Quote)
            }
        }
    }
}
