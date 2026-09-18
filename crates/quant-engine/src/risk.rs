//! The chokepoint every order passes through.
//!
//! Present since M3, empty until M6, because a chokepoint retrofitted into a
//! codebase that grew up without one means auditing every call site that learned
//! to go around it. `CLAUDE.md` puts it "between strategy and venue, not a module
//! the strategy politely calls", and the way that is enforced is structural: a
//! [`Strategy`](crate::Strategy) is handed a [`Context`](crate::Context), never a
//! venue, and `Context::submit` runs this on the way out. There is no other path.
//!
//! # Why risk keeps its own tally
//!
//! [`RiskEngine`] tracks position, notional traded and realized P&L itself,
//! rather than reading [`Portfolio`](crate::Portfolio). That is deliberate
//! duplication — the one kind this project accepts — and the reason is what a
//! risk layer is *for*.
//!
//! A limit computed from the portfolio can only be as correct as the portfolio.
//! If the accounting has a bug, the limits go wrong in the same direction and
//! stop protecting anything precisely when something is already wrong. An
//! independent tally means a portfolio bug cannot silently disable a limit, and
//! a disagreement between them is itself a signal.
//!
//! It is also the fourth time this pattern has paid: the venue and the portfolio
//! each total fees, `quant-verify` and `quant-normalize` each count frames, the
//! journal and the engine each claim a P&L. Each time the value is not the second
//! number but that a disagreement becomes visible.
//!
//! The tally is deliberately *simpler* than the portfolio's: signed position,
//! gross notional, and realized P&L on a first-in basis at the position's running
//! average. Not because average cost is wrong, but because a risk limit needs to
//! be obviously right rather than exactly right, and forty lines that can be read
//! in one sitting is worth more here than agreement to the satoshi.
//!
//! # Why a tripped kill switch has to survive a restart
//!
//! A kill switch that forgets is not a kill switch. The supervisor exists to
//! restart a dead process, so a switch held only in memory would be re-armed by
//! the very machinery meant to keep the system running — trading would resume
//! minutes after a limit said stop, with nobody having decided that.
//!
//! So tripping is journalled, and [`RiskEngine::recover`] reads it back. Clearing
//! it is a human action, by design: there is no automatic un-trip, because every
//! condition that trips it is one a person should look at.

use quant_core::event::Side;
use quant_core::execution::{Fill, OrderRequest, RejectReason};
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::instrument::InstrumentId;
use quant_core::time::{Ts, UtcDate};

/// Decides whether an order may leave.
pub trait RiskLayer {
    /// `None` to allow, `Some(reason)` to refuse.
    ///
    /// `mark` is the current mid for the request's instrument, or `None` when the
    /// book has no prices — after a gap, or before the first snapshot. A limit
    /// expressed in money **must** refuse in that case: you cannot size what you
    /// cannot price, and guessing would make the limit widest exactly when the
    /// market is least understood.
    ///
    /// Takes `&mut self` because a real limit is stateful — notional traded
    /// today, open position, a tripped switch — and a checker that could not
    /// remember could only enforce per-order limits, which are the least useful
    /// kind.
    fn check(&mut self, request: &OrderRequest, mark: Option<Px>, now: Ts) -> Option<RejectReason>;

    /// Told about every fill, so a stateful limit can see its own effect.
    ///
    /// Defaulted to nothing, so a layer with no state does not have to say so.
    fn on_fill(&mut self, instrument: InstrumentId, side: Side, fill: &Fill, at: Ts) {
        let _ = (instrument, side, fill, at);
    }
}

/// Allows everything.
///
/// M3's risk layer, kept for backtests where the question is the strategy rather
/// than the limits. Its job is to prove the path exists and that nothing can go
/// around it — not to have an opinion.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAll;

impl RiskLayer for AllowAll {
    fn check(
        &mut self,
        _request: &OrderRequest,
        _mark: Option<Px>,
        _now: Ts,
    ) -> Option<RejectReason> {
        None
    }
}

/// What a risk engine will not let past.
///
/// Every limit is optional, and `None` means unlimited. That is the honest
/// encoding — a sentinel like `Notional::MAX` would read as a limit and behave as
/// none — but it also means [`Limits::default()`] permits everything, so the
/// paper and live binaries construct limits explicitly rather than defaulting.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest single order, in quote currency.
    pub max_order_notional: Option<Notional>,
    /// Largest absolute position in one instrument, in quote currency at the
    /// current mark.
    ///
    /// In money and not in units, because a limit in units means something
    /// different at every price and would have to be re-tuned as the market
    /// moved. The mark is what makes it comparable across instruments.
    pub max_position_notional: Option<Notional>,
    /// Realized loss in one UTC day that stops trading for that day.
    ///
    /// A positive number describing a loss: `max_daily_loss: Some(10)` means
    /// stop after losing ten. Signed would invite `-10` meaning the same thing
    /// and `10` meaning "stop after making ten".
    pub max_daily_loss: Option<Notional>,
    /// Orders accepted in one UTC day.
    ///
    /// Not about money. It is the limit that catches a strategy stuck in a loop,
    /// which is the failure a fortnight of unattended running is most likely to
    /// produce and the one no money limit notices until it is expensive.
    pub max_orders_per_day: Option<u64>,
}

impl Limits {
    /// Nothing is allowed through.
    ///
    /// For a test that has to prove the chokepoint is a chokepoint, and for a
    /// deliberate halt.
    #[must_use]
    pub fn nothing() -> Self {
        Self {
            max_order_notional: Some(Notional::ZERO),
            ..Self::default()
        }
    }
}

/// Why the switch was thrown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TripCause {
    DailyLoss,
    OrderCount,
    /// A person, or an operator script.
    Manual,
}

impl core::fmt::Display for TripCause {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::DailyLoss => "the daily loss limit",
            Self::OrderCount => "the daily order limit",
            Self::Manual => "a manual halt",
        })
    }
}

/// What the risk engine has seen today.
///
/// Reset on a UTC day change, driven by the *event's* timestamp and never a
/// system clock — the same rule `quant-recorder::segment` follows, and for the
/// same reason: a day boundary that depended on our own scheduling would make the
/// limit a property of how busy we were.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Today {
    date: Option<UtcDate>,
    orders: u64,
    realized: Notional,
}

/// A holding, as risk counts it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Held {
    qty: Qty,
    avg_px: Px,
}

/// Enforces [`Limits`], with its own view of the world.
#[derive(Debug, Clone)]
pub struct RiskEngine {
    limits: Limits,
    today: Today,
    positions: Vec<Held>,
    tripped: Option<TripCause>,
    /// Every refusal, by reason, for reporting.
    refusals: u64,
}

impl RiskEngine {
    /// What this engine is actually enforcing.
    ///
    /// For a report to print the limits it *ran under* rather than the ones it
    /// parsed. M4 learned that distinction expensively: the backtest binary once
    /// announced ten basis points of fees while the venue had been handed a free
    /// one, because an edit missed a line `cargo fmt` had moved. A configured-but-
    /// unwired limit would fail the same way and look identical in the output, so
    /// the reader asks the thing that did the work.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }

    #[must_use]
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            today: Today::default(),
            positions: Vec::new(),
            tripped: None,
            refusals: 0,
        }
    }

    /// Start already tripped, because a previous run tripped and said so.
    ///
    /// The supervisor restarts a dead process, so a switch held only in memory
    /// would be re-armed by the machinery meant to keep things running.
    #[must_use]
    pub fn recover(limits: Limits, tripped: Option<TripCause>) -> Self {
        Self {
            tripped,
            ..Self::new(limits)
        }
    }

    #[must_use]
    pub const fn tripped(&self) -> Option<TripCause> {
        self.tripped
    }

    #[must_use]
    pub const fn refusals(&self) -> u64 {
        self.refusals
    }

    /// Realized P&L today, by risk's own reckoning.
    #[must_use]
    pub const fn realized_today(&self) -> Notional {
        self.today.realized
    }

    #[must_use]
    pub const fn orders_today(&self) -> u64 {
        self.today.orders
    }

    /// Throw the switch by hand.
    ///
    /// There is no matching automatic un-trip: every condition that trips it is
    /// one a person should look at, and a switch that resets itself is a switch
    /// that will reset itself at the worst moment.
    pub const fn trip(&mut self, cause: TripCause) {
        self.tripped = Some(cause);
    }

    /// Clear the switch. A human action.
    pub const fn reset(&mut self) {
        self.tripped = None;
    }

    /// Position in one instrument, in units.
    #[must_use]
    pub fn position(&self, instrument: InstrumentId) -> Qty {
        self.positions
            .get(instrument.index())
            .map_or(Qty::ZERO, |h| h.qty)
    }

    /// Roll the day if this event is on a later one.
    ///
    /// Never backwards, for the reason the recorder never rolls backwards: an
    /// NTP step must not reopen yesterday and hand a stopped strategy a fresh
    /// loss budget.
    fn roll(&mut self, now: Ts) {
        let date = now.utc_date();
        match self.today.date {
            Some(current) if current >= date => {}
            _ => {
                self.today = Today {
                    date: Some(date),
                    orders: 0,
                    realized: Notional::ZERO,
                };
                // Deliberately *not* clearing `tripped`. A daily loss limit
                // stops trading for the day; whether to resume tomorrow is a
                // decision, not a timeout.
            }
        }
    }

    /// Count a refusal and name it.
    ///
    /// Returns the reason rather than an `Option` so every call site reads
    /// `return Some(self.refuse(..))` -- explicit at the point of refusal, which
    /// is the one place in this file worth being unambiguous.
    fn refuse(&mut self, reason: RejectReason) -> RejectReason {
        self.refusals += 1;
        reason
    }
}

impl RiskLayer for RiskEngine {
    fn check(&mut self, request: &OrderRequest, mark: Option<Px>, now: Ts) -> Option<RejectReason> {
        self.roll(now);

        if self.tripped.is_some() {
            return Some(self.refuse(RejectReason::RiskLimit));
        }
        if self
            .limits
            .max_orders_per_day
            .is_some_and(|max| self.today.orders >= max)
        {
            // Trips rather than merely refusing: a strategy that has hit an
            // order-count limit is a strategy in a loop, and a loop does not
            // stop because one order was declined.
            self.trip(TripCause::OrderCount);
            return Some(self.refuse(RejectReason::RiskLimit));
        }
        if self
            .limits
            .max_daily_loss
            .is_some_and(|max| self.today.realized.raw() <= -max.raw())
        {
            self.trip(TripCause::DailyLoss);
            return Some(self.refuse(RejectReason::RiskLimit));
        }

        // Everything below is money, and money needs a price.
        let money_limited =
            self.limits.max_order_notional.is_some() || self.limits.max_position_notional.is_some();
        if money_limited {
            let Some(mark) = mark else {
                // You cannot size what you cannot price. Refusing here is what
                // makes the limit *tightest* when the market is least
                // understood, rather than widest.
                return Some(self.refuse(RejectReason::NoMarket));
            };

            if let Some(max) = self.limits.max_order_notional {
                let notional = mark.notional(request.qty).unwrap_or(Notional::MAX_VALUE);
                if notional > max {
                    return Some(self.refuse(RejectReason::RiskLimit));
                }
            }
            if let Some(max) = self.limits.max_position_notional {
                // The position this order would *create*, not the one we have.
                // Checking the current position would let every order through
                // right up to the one that mattered.
                let current = self.position(request.instrument).raw();
                let after = current + request.qty.raw() * request.side.sign();
                let exposure = mark
                    .notional(Qty::from_raw(after.abs()))
                    .unwrap_or(Notional::MAX_VALUE);
                if exposure > max {
                    return Some(self.refuse(RejectReason::RiskLimit));
                }
            }
        }

        self.today.orders += 1;
        None
    }

    fn on_fill(&mut self, instrument: InstrumentId, side: Side, fill: &Fill, at: Ts) {
        self.roll(at);
        let index = instrument.index();
        if self.positions.len() <= index {
            self.positions.resize(index + 1, Held::default());
        }
        let held = self.positions[index];
        let before = held.qty.raw();
        let delta = fill.qty.raw() * side.sign();
        let after = before + delta;

        // Realized on whatever this fill closed, at the running average. Simpler
        // than the portfolio's on purpose: a limit needs to be obviously right.
        let closing = if before == 0 || before.signum() == delta.signum() {
            0
        } else {
            delta.abs().min(before.abs())
        };
        if closing > 0 {
            let per_unit = Px::from_raw(fill.px.raw() - held.avg_px.raw());
            let moved = per_unit
                .notional(Qty::from_raw(closing))
                .map_or(0, quant_core::Notional::raw)
                * before.signum();
            self.today.realized = Notional::from_raw(self.today.realized.raw() + moved);
        }
        // Fees are a loss whichever way the trade went, so they count against
        // the daily budget. A limit that ignored them would be reached late by
        // exactly the amount a high-turnover strategy pays -- which M4 showed is
        // most of the damage.
        self.today.realized = Notional::from_raw(self.today.realized.raw() - fill.fee.raw());

        self.positions[index] = if after == 0 {
            Held::default()
        } else if closing > 0 && after.signum() != before.signum() {
            Held {
                qty: Qty::from_raw(after),
                avg_px: fill.px,
            }
        } else if closing > 0 {
            Held {
                qty: Qty::from_raw(after),
                avg_px: held.avg_px,
            }
        } else {
            let total = before.abs() + delta.abs();
            let weighted = i128::from(held.avg_px.raw()) * i128::from(before.abs())
                + i128::from(fill.px.raw()) * i128::from(delta.abs());
            Held {
                qty: Qty::from_raw(after),
                avg_px: Px::from_raw(
                    i64::try_from(weighted / i128::from(total))
                        .expect("an average of two prices lies between them"),
                ),
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::execution::{OrderKind, TimeInForce};
    use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};

    const DAY: i64 = 86_400 * 1_000_000_000;

    fn instrument() -> InstrumentId {
        InstrumentRegistry::new().register(InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.01".parse().expect("tick"),
            lot_size: "0.00001".parse().expect("lot"),
            min_notional: "5".parse().expect("notional"),
        })
    }

    fn amount(text: &str) -> Notional {
        text.parse().expect("a valid amount")
    }

    fn price(text: &str) -> Px {
        text.parse().expect("a valid price")
    }

    fn order(side: Side, qty: &str) -> OrderRequest {
        OrderRequest {
            instrument: instrument(),
            side,
            qty: qty.parse().expect("a valid quantity"),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::Gtc,
        }
    }

    fn fill(px: &str, qty: &str, fee: &str) -> Fill {
        Fill {
            px: price(px),
            qty: qty.parse().expect("qty"),
            fee: amount(fee),
            is_maker: false,
        }
    }

    /// Mid of 100, which makes notionals easy to read.
    const MARK: Option<Px> = Some(Px::from_raw(100 * quant_core::SCALE));
    const NOW: Ts = Ts::from_nanos(1_787_000_000_000_000_000);

    #[test]
    fn no_limits_allows_everything() {
        // Limits::default() permits all, which is why the binaries construct
        // limits explicitly rather than defaulting.
        let mut risk = RiskEngine::new(Limits::default());
        assert_eq!(risk.check(&order(Side::Buy, "1000000"), MARK, NOW), None);
        assert_eq!(risk.refusals(), 0);
    }

    #[test]
    fn an_order_above_the_notional_limit_is_refused() {
        let mut risk = RiskEngine::new(Limits {
            max_order_notional: Some(amount("500")),
            ..Limits::default()
        });
        // 4 units at a mark of 100 is 400: allowed.
        assert_eq!(risk.check(&order(Side::Buy, "4"), MARK, NOW), None);
        // 6 units is 600: refused.
        assert_eq!(
            risk.check(&order(Side::Buy, "6"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );
    }

    #[test]
    fn a_money_limit_refuses_when_there_is_no_price() {
        // You cannot size what you cannot price. This is what makes the limit
        // *tightest* when the market is least understood, rather than widest --
        // which is the failure mode of every "assume the last price" shortcut.
        let mut risk = RiskEngine::new(Limits {
            max_order_notional: Some(amount("500")),
            ..Limits::default()
        });
        assert_eq!(
            risk.check(&order(Side::Buy, "1"), None, NOW),
            Some(RejectReason::NoMarket)
        );
    }

    #[test]
    fn an_unpriced_order_is_allowed_when_no_money_limit_applies() {
        // Refusing here would make a gap stop a strategy that had asked for no
        // money limits at all -- a limit nobody set should not have an effect.
        let mut risk = RiskEngine::new(Limits {
            max_orders_per_day: Some(10),
            ..Limits::default()
        });
        assert_eq!(risk.check(&order(Side::Buy, "1"), None, NOW), None);
    }

    #[test]
    fn the_position_limit_looks_at_the_position_the_order_would_create() {
        // Checking the position we *have* would let every order through right up
        // to the one that mattered, which is a limit that never fires.
        let mut risk = RiskEngine::new(Limits {
            max_position_notional: Some(amount("500")),
            ..Limits::default()
        });
        assert_eq!(risk.check(&order(Side::Buy, "3"), MARK, NOW), None);
        risk.on_fill(instrument(), Side::Buy, &fill("100", "3", "0"), NOW);

        // Three more would be six units, 600 at the mark: refused before it
        // happens rather than after.
        assert_eq!(
            risk.check(&order(Side::Buy, "3"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );
        // But two more is five units, 500: exactly at the limit, allowed.
        assert_eq!(risk.check(&order(Side::Buy, "2"), MARK, NOW), None);
    }

    #[test]
    fn reducing_a_position_is_allowed_even_when_it_is_over_the_limit() {
        // The limit is on exposure, so the way *out* must never be blocked. A
        // risk layer that trapped a position it considered too large would be
        // the most dangerous thing in the system.
        let mut risk = RiskEngine::new(Limits {
            max_position_notional: Some(amount("500")),
            ..Limits::default()
        });
        risk.on_fill(instrument(), Side::Buy, &fill("100", "10", "0"), NOW);
        assert_eq!(risk.position(instrument()).to_string(), "10");
        assert_eq!(
            risk.check(&order(Side::Sell, "10"), MARK, NOW),
            None,
            "selling out of an oversized position must be allowed"
        );
    }

    #[test]
    fn a_short_counts_against_the_position_limit_too() {
        // Exposure is absolute. A limit that only looked at longs would permit
        // an unlimited short, which is the more dangerous direction.
        let mut risk = RiskEngine::new(Limits {
            max_position_notional: Some(amount("500")),
            ..Limits::default()
        });
        assert_eq!(
            risk.check(&order(Side::Sell, "6"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );
    }

    #[test]
    fn the_daily_loss_limit_trips_the_switch_rather_than_just_refusing() {
        // A strategy that has lost its budget is not fixed by declining one
        // order, so this latches. Nothing more goes out today.
        let mut risk = RiskEngine::new(Limits {
            max_daily_loss: Some(amount("10")),
            ..Limits::default()
        });
        risk.on_fill(instrument(), Side::Buy, &fill("100", "1", "0"), NOW);
        risk.on_fill(instrument(), Side::Sell, &fill("89", "1", "0"), NOW);
        assert_eq!(risk.realized_today(), amount("-11"));

        assert_eq!(
            risk.check(&order(Side::Buy, "1"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );
        assert_eq!(risk.tripped(), Some(TripCause::DailyLoss));
    }

    #[test]
    fn fees_count_against_the_daily_loss_budget() {
        // A limit that ignored fees would be reached late by exactly the amount
        // a high-turnover strategy pays -- which M4 showed is most of the damage.
        let mut risk = RiskEngine::new(Limits {
            max_daily_loss: Some(amount("1")),
            ..Limits::default()
        });
        // A round trip that breaks even on price and pays 1.2 in fees.
        risk.on_fill(instrument(), Side::Buy, &fill("100", "1", "0.6"), NOW);
        risk.on_fill(instrument(), Side::Sell, &fill("100", "1", "0.6"), NOW);
        assert_eq!(risk.realized_today(), amount("-1.2"));
        assert_eq!(
            risk.check(&order(Side::Buy, "1"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );
    }

    #[test]
    fn the_order_count_limit_trips_because_a_loop_does_not_stop_being_declined() {
        // The limit that catches a strategy stuck in a loop, which is the
        // failure an unattended fortnight is most likely to produce and the one
        // no money limit notices until it is expensive.
        let mut risk = RiskEngine::new(Limits {
            max_orders_per_day: Some(2),
            ..Limits::default()
        });
        assert_eq!(risk.check(&order(Side::Buy, "1"), MARK, NOW), None);
        assert_eq!(risk.check(&order(Side::Buy, "1"), MARK, NOW), None);
        assert_eq!(
            risk.check(&order(Side::Buy, "1"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );
        assert_eq!(risk.tripped(), Some(TripCause::OrderCount));
    }

    #[test]
    fn a_refused_order_does_not_consume_the_daily_count() {
        // Otherwise a strategy could exhaust its budget on orders that never
        // existed, and the count would measure our refusals rather than its
        // activity.
        let mut risk = RiskEngine::new(Limits {
            max_order_notional: Some(amount("500")),
            max_orders_per_day: Some(10),
            ..Limits::default()
        });
        for _ in 0..5 {
            assert_eq!(
                risk.check(&order(Side::Buy, "100"), MARK, NOW),
                Some(RejectReason::RiskLimit)
            );
        }
        assert_eq!(risk.orders_today(), 0);
    }

    #[test]
    fn a_new_day_resets_the_counters_but_not_the_switch() {
        // A daily loss limit stops trading for the day; whether to resume
        // tomorrow is a decision, not a timeout. A switch that cleared itself
        // overnight would resume trading with nobody having chosen to.
        let mut risk = RiskEngine::new(Limits {
            max_daily_loss: Some(amount("10")),
            max_orders_per_day: Some(5),
            ..Limits::default()
        });
        risk.on_fill(instrument(), Side::Buy, &fill("100", "1", "0"), NOW);
        risk.on_fill(instrument(), Side::Sell, &fill("80", "1", "0"), NOW);
        assert_eq!(
            risk.check(&order(Side::Buy, "1"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );

        let tomorrow = Ts::from_nanos(NOW.as_nanos() + DAY);
        assert_eq!(
            risk.check(&order(Side::Buy, "1"), MARK, tomorrow),
            Some(RejectReason::RiskLimit),
            "the switch is still thrown"
        );
        assert_eq!(risk.realized_today(), Notional::ZERO, "but the day rolled");
    }

    #[test]
    fn the_day_never_rolls_backwards() {
        // An NTP step must not reopen yesterday and hand a stopped strategy a
        // fresh loss budget. Same rule as quant-recorder::segment, same reason.
        let mut risk = RiskEngine::new(Limits {
            max_daily_loss: Some(amount("10")),
            ..Limits::default()
        });
        risk.on_fill(instrument(), Side::Buy, &fill("100", "1", "0"), NOW);
        risk.on_fill(instrument(), Side::Sell, &fill("95", "1", "0"), NOW);
        assert_eq!(risk.realized_today(), amount("-5"));

        let yesterday = Ts::from_nanos(NOW.as_nanos() - DAY);
        let _ = risk.check(&order(Side::Buy, "1"), MARK, yesterday);
        assert_eq!(
            risk.realized_today(),
            amount("-5"),
            "a clock that went backwards must not clear the day"
        );
    }

    #[test]
    fn a_tripped_switch_refuses_everything_including_the_way_out() {
        // Worth being explicit about, because it is a real trade-off. Once the
        // switch is thrown nothing goes out at all -- not even a flattening
        // order -- because a tripped switch means "a person should look at this"
        // and the alternative is a system that keeps trading after it decided
        // not to. Closing a position is then a manual act, which is the right
        // level of friction for something that has already gone wrong.
        let mut risk = RiskEngine::new(Limits::default());
        risk.on_fill(instrument(), Side::Buy, &fill("100", "5", "0"), NOW);
        risk.trip(TripCause::Manual);
        assert_eq!(
            risk.check(&order(Side::Sell, "5"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );
    }

    #[test]
    fn a_tripped_switch_can_only_be_cleared_by_hand() {
        let mut risk = RiskEngine::new(Limits::default());
        risk.trip(TripCause::DailyLoss);
        assert!(risk.tripped().is_some());
        risk.reset();
        assert_eq!(risk.tripped(), None);
        assert_eq!(risk.check(&order(Side::Buy, "1"), MARK, NOW), None);
    }

    #[test]
    fn a_recovered_engine_starts_already_tripped() {
        // A kill switch that forgets is not a kill switch. The supervisor exists
        // to restart a dead process, so a switch held only in memory would be
        // re-armed by the machinery meant to keep the system running.
        let mut risk = RiskEngine::recover(Limits::default(), Some(TripCause::DailyLoss));
        assert_eq!(
            risk.check(&order(Side::Buy, "1"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );
    }

    #[test]
    fn risks_own_tally_is_independent_of_the_portfolios() {
        // The reason for the duplication: a limit computed from the accounting
        // can only be as correct as the accounting. Here risk is fed fills and
        // never shown a Portfolio at all, and still knows its position.
        let mut risk = RiskEngine::new(Limits::default());
        risk.on_fill(instrument(), Side::Buy, &fill("100", "2", "0"), NOW);
        risk.on_fill(instrument(), Side::Buy, &fill("110", "2", "0"), NOW);
        assert_eq!(risk.position(instrument()).to_string(), "4");
        risk.on_fill(instrument(), Side::Sell, &fill("120", "4", "0"), NOW);
        assert_eq!(risk.position(instrument()), Qty::ZERO);
        // (120 - 105) * 4 = 60, at the running average of 105.
        assert_eq!(risk.realized_today(), amount("60"));
    }

    #[test]
    fn nothing_gets_through_limits_that_allow_nothing() {
        let mut risk = RiskEngine::new(Limits::nothing());
        assert_eq!(
            risk.check(&order(Side::Buy, "0.00000001"), MARK, NOW),
            Some(RejectReason::RiskLimit)
        );
    }
}
