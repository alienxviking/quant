//! What we own and what it cost.
//!
//! Maintained by the engine from execution events, in all three worlds. A
//! strategy could derive its own position from its own fills — it receives every
//! one — but then every strategy would derive it, and they would derive it
//! differently. Same argument as the book.
//!
//! # Average cost, and why realized P&L is separated from unrealized
//!
//! Closing part of a position realizes profit on the part closed, at the
//! position's average cost. The alternative, FIFO lot matching, gives different
//! numbers and is what most tax authorities want; average cost is what a trading
//! system wants, because it is path-independent and cannot be gamed by the order
//! in which lots are chosen.
//!
//! The separation matters more than the method. **Realized** P&L is money that
//! has moved and cannot change. **Unrealized** is an opinion about a price, and
//! it is an opinion that goes *away* when the book does — after a gap there is
//! no mark, so there is no unrealized number, and [`Portfolio::equity`] says so
//! by returning `None` rather than carrying a stale one forward.
//!
//! # Why fees are tracked separately from cash
//!
//! They are already inside cash — a fill's fee is paid at the fill. Tracking the
//! total as well is what makes "the strategy is profitable before costs and not
//! after" a thing you can *see*, which is exactly the sentence M4 exists to
//! produce.

use quant_core::event::Side;
use quant_core::execution::Fill;
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::instrument::InstrumentId;

/// A holding in one instrument.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    /// Signed: positive is long, negative is short.
    ///
    /// Signed here and unsigned on an order, which is not an inconsistency: an
    /// instruction has a direction field, and a holding *is* a direction.
    pub qty: Qty,
    /// Average price paid for what is currently held.
    ///
    /// Meaningless when `qty` is zero, and reset to zero there rather than left
    /// at its last value, so a stale cost basis cannot be read out of a flat
    /// position.
    pub avg_px: Px,
}

impl Position {
    #[must_use]
    pub const fn is_flat(&self) -> bool {
        self.qty.raw() == 0
    }
}

/// Cash, holdings, and what has been made or lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Portfolio {
    cash: Notional,
    starting_cash: Notional,
    positions: Vec<Position>,
    realized: Notional,
    fees: Notional,
    fills: u64,
}

impl Portfolio {
    /// Start with `cash` and nothing else.
    #[must_use]
    pub const fn new(cash: Notional) -> Self {
        Self {
            cash,
            starting_cash: cash,
            positions: Vec::new(),
            realized: Notional::from_raw(0),
            fees: Notional::from_raw(0),
            fills: 0,
        }
    }

    #[must_use]
    pub const fn cash(&self) -> Notional {
        self.cash
    }

    #[must_use]
    pub const fn starting_cash(&self) -> Notional {
        self.starting_cash
    }

    /// Money that has actually moved. Cannot change.
    #[must_use]
    pub const fn realized(&self) -> Notional {
        self.realized
    }

    /// Total fees paid, already deducted from `cash`.
    #[must_use]
    pub const fn fees(&self) -> Notional {
        self.fees
    }

    #[must_use]
    pub const fn fills(&self) -> u64 {
        self.fills
    }

    /// Instruments currently held.
    ///
    /// Exists so a report can say "flat" without iterating a registry it may not
    /// have. Counts non-flat holdings only, because a position that was closed
    /// leaves a zeroed slot behind and reporting that as a holding would be a
    /// lie about what we own.
    #[must_use]
    pub fn open_positions(&self) -> usize {
        self.positions.iter().filter(|p| !p.is_flat()).count()
    }

    /// Every non-flat holding, with the instrument it is in.
    #[must_use]
    pub fn held(&self) -> Vec<(InstrumentId, Position)> {
        self.positions
            .iter()
            .enumerate()
            .filter(|(_, p)| !p.is_flat())
            .map(|(i, p)| (InstrumentId::from_index(i), *p))
            .collect()
    }

    #[must_use]
    pub fn position(&self, instrument: InstrumentId) -> Position {
        self.positions
            .get(instrument.index())
            .copied()
            .unwrap_or_default()
    }

    /// Cash plus the marked value of one instrument's position.
    ///
    /// `None` when the position is not flat and `mark` is `None`: there is a
    /// holding and no price for it, so any number here would be invented. That
    /// is the case after a gap, and returning a stale equity through one is how
    /// an equity curve smooths over exactly the periods worth looking at.
    #[must_use]
    pub fn equity(&self, instrument: InstrumentId, mark: Option<Px>) -> Option<Notional> {
        let position = self.position(instrument);
        if position.is_flat() {
            return Some(self.cash);
        }
        let mark = mark?;
        Some(Notional::from_raw(
            self.cash.raw() + notional(mark, position.qty).raw(),
        ))
    }

    /// Apply a fill.
    ///
    /// The instrument and side come from the caller because a [`Fill`] does not
    /// carry them: a fill belongs to an *order*, and the engine is what knows
    /// which order. Putting them on the fill would duplicate the order's own
    /// fields into every execution report, where they could disagree with it.
    pub fn apply_fill(&mut self, instrument: InstrumentId, side: Side, fill: &Fill) {
        let index = instrument.index();
        if self.positions.len() <= index {
            self.positions.resize(index + 1, Position::default());
        }

        let signed = Qty::from_raw(fill.qty.raw() * side.sign());
        let gross = notional(fill.px, signed);
        // Buying spends cash, selling raises it; `gross` already carries the
        // sign, so this is one line for both directions rather than a branch
        // that can be written backwards.
        self.cash = Notional::from_raw(self.cash.raw() - gross.raw() - fill.fee.raw());
        self.fees = Notional::from_raw(self.fees.raw() + fill.fee.raw());
        self.fills += 1;

        let position = self.positions[index];
        let before = position.qty.raw();
        let delta = signed.raw();
        let after = before + delta;

        let closing = if before == 0 || before.signum() == delta.signum() {
            0
        } else {
            // Cannot close more than is held; the remainder opens the other way.
            delta.abs().min(before.abs())
        };
        if closing > 0 {
            // Realized on the part closed, at the position's average cost.
            // `before.signum()` makes a short's gain the mirror of a long's.
            let gain_per_unit = Px::from_raw(fill.px.raw() - position.avg_px.raw());
            let moved = notional(gain_per_unit, Qty::from_raw(closing)).raw() * before.signum();
            self.realized = Notional::from_raw(self.realized.raw() + moved);
        }

        self.positions[index] = if after == 0 {
            // Flat: the cost basis is meaningless, so it is cleared rather than
            // left to be read back later.
            Position::default()
        } else if closing > 0 && after.signum() != before.signum() {
            // Flipped through zero: the new position was opened at this fill.
            Position {
                qty: Qty::from_raw(after),
                avg_px: fill.px,
            }
        } else if closing > 0 {
            // Reduced: the average cost of what remains is unchanged.
            Position {
                qty: Qty::from_raw(after),
                avg_px: position.avg_px,
            }
        } else {
            // Added to: weighted average of old and new.
            Position {
                qty: Qty::from_raw(after),
                avg_px: Px::from_raw(weighted_average(
                    position.avg_px.raw(),
                    before.abs(),
                    fill.px.raw(),
                    delta.abs(),
                )),
            }
        };
    }
}

/// `px * qty` in quote currency. Sign follows `qty`.
///
/// `quant-core`'s, not a local reimplementation: this module and `quant-sim`
/// each had their own 128-bit version, which is two copies of a money
/// calculation that have to agree with each other forever. `Px::notional`
/// predates both of them.
fn notional(px: Px, qty: Qty) -> Notional {
    px.notional(qty)
        .expect("a notional beyond i64 means the inputs were wrong")
}

/// `(a * wa + b * wb) / (wa + wb)`, in 128-bit.
fn weighted_average(a: i64, wa: i64, b: i64, wb: i64) -> i64 {
    let total = i128::from(wa) + i128::from(wb);
    if total == 0 {
        return 0;
    }
    let sum = i128::from(a) * i128::from(wa) + i128::from(b) * i128::from(wb);
    i64::try_from(sum / total).expect("an average of two prices is between them")
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};

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

    fn cash(amount: &str) -> Notional {
        amount.parse().expect("a valid amount")
    }

    fn fill(px: &str, qty: &str, fee: &str) -> Fill {
        Fill {
            px: px.parse().expect("px"),
            qty: qty.parse().expect("qty"),
            fee: fee.parse().expect("fee"),
            is_maker: false,
        }
    }

    fn started() -> (Portfolio, InstrumentId) {
        (Portfolio::new(cash("1000")), instrument())
    }

    #[test]
    fn a_buy_spends_cash_and_a_sell_raises_it() {
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "2", "0"));
        assert_eq!(p.cash(), cash("800"));
        assert_eq!(p.position(i).qty.to_string(), "2");

        p.apply_fill(i, Side::Sell, &fill("100", "2", "0"));
        assert_eq!(p.cash(), cash("1000"));
        assert!(p.position(i).is_flat());
    }

    #[test]
    fn a_round_trip_at_the_same_price_with_no_fees_realizes_nothing() {
        // The property that catches a sign error anywhere in the accounting.
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("76650.12345678", "0.001", "0"));
        p.apply_fill(i, Side::Sell, &fill("76650.12345678", "0.001", "0"));
        assert_eq!(p.realized(), cash("0"));
        assert_eq!(p.cash(), cash("1000"));
        assert_eq!(p.equity(i, None), Some(cash("1000")));
    }

    #[test]
    fn a_profitable_round_trip_realizes_the_difference() {
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "2", "0"));
        p.apply_fill(i, Side::Sell, &fill("110", "2", "0"));
        assert_eq!(p.realized(), cash("20"), "10 a unit on two units");
        assert_eq!(p.cash(), cash("1020"));
    }

    #[test]
    fn a_losing_round_trip_realizes_a_negative() {
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "2", "0"));
        p.apply_fill(i, Side::Sell, &fill("90", "2", "0"));
        assert_eq!(p.realized(), cash("-20"));
        assert_eq!(p.cash(), cash("980"));
    }

    #[test]
    fn adding_to_a_position_averages_the_cost() {
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "1", "0"));
        p.apply_fill(i, Side::Buy, &fill("110", "1", "0"));
        assert_eq!(p.position(i).avg_px.to_string(), "105");
        assert_eq!(p.realized(), cash("0"), "adding realizes nothing");
    }

    #[test]
    fn reducing_a_position_leaves_the_average_cost_alone() {
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "1", "0"));
        p.apply_fill(i, Side::Buy, &fill("110", "1", "0"));
        p.apply_fill(i, Side::Sell, &fill("120", "1", "0"));
        assert_eq!(p.position(i).qty.to_string(), "1");
        assert_eq!(
            p.position(i).avg_px.to_string(),
            "105",
            "what remains cost what it cost"
        );
        assert_eq!(p.realized(), cash("15"), "120 - 105 on one unit");
    }

    #[test]
    fn a_flat_position_has_no_cost_basis_left_to_read() {
        // Cleared rather than left at its last value, so a stale basis cannot be
        // read out of a position that no longer exists.
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "1", "0"));
        p.apply_fill(i, Side::Sell, &fill("100", "1", "0"));
        assert_eq!(p.position(i), Position::default());
    }

    #[test]
    fn selling_through_zero_realizes_the_long_and_opens_the_short_at_the_fill() {
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "1", "0"));
        p.apply_fill(i, Side::Sell, &fill("110", "3", "0"));
        assert_eq!(p.realized(), cash("10"), "only the closed unit realizes");
        assert_eq!(p.position(i).qty.to_string(), "-2");
        assert_eq!(
            p.position(i).avg_px.to_string(),
            "110",
            "the short was opened at this fill, not at the old basis"
        );
    }

    #[test]
    fn a_short_makes_money_when_the_price_falls() {
        // The mirror of a long, which is the case a sign error gets backwards.
        let (mut p, i) = started();
        p.apply_fill(i, Side::Sell, &fill("100", "1", "0"));
        assert_eq!(p.cash(), cash("1100"));
        p.apply_fill(i, Side::Buy, &fill("90", "1", "0"));
        assert_eq!(p.realized(), cash("10"));
        assert_eq!(p.cash(), cash("1010"));
    }

    #[test]
    fn fees_come_out_of_cash_and_are_totalled_separately() {
        // Already inside cash; the total is what makes "profitable before costs
        // and not after" visible rather than inferred.
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "1", "0.5"));
        p.apply_fill(i, Side::Sell, &fill("100", "1", "0.5"));
        assert_eq!(p.realized(), cash("0"), "the trade itself broke even");
        assert_eq!(p.fees(), cash("1"));
        assert_eq!(p.cash(), cash("999"), "and the fees are why it lost");
    }

    #[test]
    fn a_maker_rebate_adds_to_cash() {
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "1", "-0.1"));
        assert_eq!(p.cash(), cash("900.1"));
        assert_eq!(p.fees(), cash("-0.1"));
    }

    #[test]
    fn equity_is_cash_plus_the_marked_position() {
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "2", "0"));
        assert_eq!(
            p.equity(i, Some("110".parse().expect("px"))),
            Some(cash("1020")),
            "800 cash plus 2 at 110"
        );
    }

    #[test]
    fn there_is_no_equity_for_a_position_with_no_mark() {
        // After a gap the book is cleared, so there is no price. Returning the
        // last equity would smooth over exactly the periods worth looking at.
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("100", "2", "0"));
        assert_eq!(p.equity(i, None), None);
    }

    #[test]
    fn a_flat_portfolio_has_equity_even_with_no_mark() {
        // Nothing to mark, so nothing is unknown.
        let (p, i) = started();
        assert_eq!(p.equity(i, None), Some(cash("1000")));
    }

    #[test]
    fn a_realistic_notional_does_not_overflow() {
        // 76,650 at 100,000 units, both scaled by 1e8, is about 1e26 before the
        // divide -- four orders of magnitude past i64.
        let (mut p, i) = started();
        p.apply_fill(i, Side::Buy, &fill("76650", "100000", "0"));
        assert_eq!(p.cash(), cash("-7664999000"));
        assert_eq!(p.position(i).avg_px.to_string(), "76650");
    }

    #[test]
    fn partial_fills_of_one_order_average_like_separate_buys() {
        // A venue may report several fills for one order, and the accounting
        // must not care which.
        let (mut whole, i) = started();
        whole.apply_fill(i, Side::Buy, &fill("101", "2", "0"));

        let mut split = Portfolio::new(cash("1000"));
        split.apply_fill(i, Side::Buy, &fill("100", "1", "0"));
        split.apply_fill(i, Side::Buy, &fill("102", "1", "0"));

        assert_eq!(whole.position(i), split.position(i));
        assert_eq!(whole.cash(), split.cash());
    }
}
