//! The simulated counterparty.
//!
//! Every backtest number this platform ever produces comes out of the
//! assumptions in this file, so they are stated rather than implied.
//!
//! # What this venue claims, and what it does not
//!
//! **A market order walks the book.** A buy consumes asks from the touch upward
//! until it is filled or the book runs out. This is the difference between a
//! simulator and a wish: filling an entire order at the touch price makes size
//! free, and a strategy tuned against that learns to trade sizes that do not
//! exist. At the capital this platform is aimed at, a single order will almost
//! never leave the first level — which is a fact about our size, not a licence
//! to skip the walk.
//!
//! **A resting limit order fills only when the market trades *through* it.** A
//! buy at `P` needs a print strictly below `P`. This is deliberately
//! conservative, and it is the opposite of the usual shortcut.
//!
//! The usual shortcut fills a resting buy at `P` as soon as the best ask touches
//! `P`. That silently assumes we were at the front of the queue at our own
//! price, which for a retail order arriving last is close to the least likely
//! outcome. Requiring the market to trade *past* our price means everything
//! resting at `P` was consumed first, so we would have been reached. It
//! understates fills; understating them is the safe direction, because a
//! backtest that misses trades is disappointing and a backtest that invents them
//! is dangerous.
//!
//! **Fees and latency come from [`Costs`]**, and default to nothing. `Costs::NONE`
//! reproduces M3 exactly, which is what makes the cost models checkable: if
//! adding a zero-valued model changes a number, the model touched something it
//! should not have. Queue position and market impact are still not modelled at
//! all, and cannot be from recorded data — [`SimStats::caveats`] says so in the
//! output rather than leaving it in a comment nobody reads.
//!
//! **An order is invisible to the market.** Our resting order does not appear in
//! the reconstructed book and nobody reacts to it. True enough at sizes that are
//! a rounding error against top-of-book depth, false at any size that matters,
//! and not fixable from recorded data at all — which is why M5 exists.
//!
//! # Why fills only happen in `observe`
//!
//! Never in `submit`. The engine calls `observe` before it hands the event to
//! the strategy and `submit` from inside a strategy callback, so filling on
//! submission would let an order trade against the very event that prompted it.
//! That is the lookahead the engine's step order exists to prevent, and this is
//! the other half of it: the engine can only guarantee the ordering if the venue
//! does not fill early.

pub mod costs;

use quant_book::Book;
use quant_core::event::{MarketEvent, Side};
use quant_core::execution::{
    ClientOrderId, ExecutionEvent, Fill, OrderRequest, RejectReason, TimeInForce,
};
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::time::Ts;
use quant_engine::ExecutionVenue;

pub use costs::{Costs, FeeSchedule, Latency};

/// An order the simulator is holding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Resting {
    id: ClientOrderId,
    request: OrderRequest,
    /// Size not yet filled.
    remaining: Qty,
    /// Whether the engine has been told this order exists yet.
    announced: bool,
}

/// What the simulation did, and what it did not model.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SimStats {
    pub submitted: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub cancelled: u64,
    pub fills: u64,
    /// Orders that consumed more than one price level.
    ///
    /// The number that says whether walking the book mattered. Zero over a whole
    /// run means every order was small against the touch — which is information,
    /// not a reason to stop walking.
    pub multi_level_fills: u64,
    /// Market orders that could not be filled in full because the book ran out.
    pub exhausted_book: u64,
    /// Orders refused because there was no book to price against — a gap, or
    /// before the first snapshot.
    pub no_market: u64,
    /// Total fees charged, as a positive cost.
    ///
    /// Tracked here as well as in the portfolio so the two can be compared: the
    /// venue says what it charged and the portfolio says what it paid, and a
    /// disagreement means one of them is wrong.
    pub fees_charged: Notional,
}

impl SimStats {
    /// What this run did **not** model, in words, for printing beside results.
    ///
    /// A method rather than a doc comment because the engine contract requires
    /// the absence of costs to be stated in the output. A number quoted without
    /// this list is a number quoted dishonestly.
    #[must_use]
    pub const fn caveats() -> &'static [&'static str] {
        &[
            "no fees: every fill is free, which flatters turnover most of all",
            "no latency: an order is at the venue by the next event",
            "no queue position: a resting order is filled only on a trade-through, \
             which understates passive fills rather than overstating them",
            "no market impact: our orders are invisible to everyone else",
        ]
    }
}

/// Fills orders against a reconstructed book.
#[derive(Debug, Default)]
pub struct SimulatedVenue {
    resting: Vec<Resting>,
    out: Vec<ExecutionEvent>,
    costs: Costs,
    stats: SimStats,
}

impl SimulatedVenue {
    /// Free and instant: M3's venue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// With costs.
    #[must_use]
    pub fn with_costs(costs: Costs) -> Self {
        Self {
            costs,
            ..Self::default()
        }
    }

    #[must_use]
    pub const fn costs(&self) -> Costs {
        self.costs
    }

    #[must_use]
    pub const fn stats(&self) -> SimStats {
        self.stats
    }

    /// Orders still working.
    #[must_use]
    pub fn open_orders(&self) -> usize {
        self.resting.len()
    }

    fn reject(&mut self, id: ClientOrderId, reason: RejectReason, now: Ts) {
        self.stats.rejected += 1;
        if matches!(reason, RejectReason::NoMarket) {
            self.stats.no_market += 1;
        }
        self.out.push(ExecutionEvent::Rejected {
            client_order_id: id,
            reason,
            ts: now,
        });
    }

    /// Consume levels from the touch outward, producing at most one fill.
    ///
    /// One fill and not one per level: a venue reports an execution, and a
    /// strategy that had to reassemble three prints into an average price would
    /// be doing arithmetic the venue already did. The price is the size-weighted
    /// average actually paid, which is the number a P&L must use.
    fn take(&mut self, order: &mut Resting, book: &Book, now: Ts) {
        let mut taken = Qty::from_raw(0);
        let mut cost = Notional::from_raw(0);
        let mut levels = 0_u32;

        for level in book.takeable(order.request.side) {
            if taken >= order.remaining {
                break;
            }
            // A limit order never pays worse than its limit.
            if let Some(limit) = order.request.limit() {
                let acceptable = match order.request.side {
                    Side::Buy => level.px <= limit,
                    Side::Sell => level.px >= limit,
                };
                if !acceptable {
                    break;
                }
            }
            let want = Qty::from_raw(order.remaining.raw() - taken.raw());
            let got = Qty::from_raw(want.raw().min(level.qty.raw()));
            if got.raw() == 0 {
                continue;
            }
            cost = Notional::from_raw(cost.raw() + notional(level.px, got).raw());
            taken = Qty::from_raw(taken.raw() + got.raw());
            levels += 1;
        }

        if taken.raw() == 0 {
            return;
        }
        if levels > 1 {
            self.stats.multi_level_fills += 1;
        }

        // A market order took liquidity; a resting order that filled on a
        // trade-through was the passive side.
        let is_maker = order.request.limit().is_some();
        // The stress concession, always against us: a buyer pays more, a seller
        // receives less. Zero unless somebody deliberately turned it on.
        let concession = self.costs.adverse_per_fill.raw() * order.request.side.sign();
        let px = Px::from_raw(average_price(cost, taken).raw() + concession);
        let gross = notional(px, taken);
        let fee = self.costs.fees.fee(gross, is_maker);
        self.stats.fees_charged = Notional::from_raw(self.stats.fees_charged.raw() + fee.raw());

        order.remaining = Qty::from_raw(order.remaining.raw() - taken.raw());
        self.stats.fills += 1;
        self.out.push(ExecutionEvent::Filled {
            client_order_id: order.id,
            fill: Fill {
                px,
                qty: taken,
                fee,
                is_maker,
            },
            remaining: order.remaining,
            ts: now,
        });
    }

    /// Whether a print at `px` went strictly past a resting order's limit.
    ///
    /// Strictly, per the module docs: a print *at* our price does not prove we
    /// were reached, only that someone at our price was.
    fn traded_through(order: &Resting, px: Px) -> bool {
        order
            .request
            .limit()
            .is_some_and(|limit| match order.request.side {
                Side::Buy => px < limit,
                Side::Sell => px > limit,
            })
    }
}

impl ExecutionVenue for SimulatedVenue {
    fn submit(&mut self, client_order_id: ClientOrderId, request: &OrderRequest, _now: Ts) {
        // Accepted, filled or rejected -- all of it waits for `observe`. See the
        // module docs: filling here would let the order trade against the event
        // that prompted it.
        self.stats.submitted += 1;
        self.resting.push(Resting {
            id: client_order_id,
            request: *request,
            remaining: request.qty,
            announced: false,
        });
    }

    fn cancel(&mut self, client_order_id: ClientOrderId, now: Ts) {
        // A cancel for an order that already filled finds nothing, and says
        // nothing. That is not an oversight: the cancel lost the race, and the
        // fill it lost to has already been reported.
        if let Some(i) = self.resting.iter().position(|o| o.id == client_order_id) {
            let order = self.resting.remove(i);
            self.stats.cancelled += 1;
            self.out.push(ExecutionEvent::Cancelled {
                client_order_id,
                remaining: order.remaining,
                ts: now,
            });
        }
    }

    fn observe(&mut self, event: &MarketEvent, book: &Book, now: Ts) {
        let printed = match event {
            MarketEvent::Trade(t) => Some(t.px),
            _ => None,
        };

        let mut orders = core::mem::take(&mut self.resting);
        for order in &mut orders {
            if !order.announced {
                order.announced = true;
                self.stats.accepted += 1;
                self.out.push(ExecutionEvent::Accepted {
                    client_order_id: order.id,
                    // A simulator has no venue-side identity to offer, and
                    // inventing one would make backtest output look like it came
                    // from an exchange.
                    venue_order_id: None,
                    ts: now,
                });
            }

            let is_market = order.request.limit().is_none();
            if is_market {
                if !book.is_live() {
                    // No prices to trade against: the expected outcome after a
                    // disconnect, and its own reject reason so it does not bury
                    // the unexpected ones.
                    let id = order.id;
                    self.reject(id, RejectReason::NoMarket, now);
                    order.remaining = Qty::from_raw(0);
                    continue;
                }
                self.take(order, book, now);
                if order.remaining.raw() > 0 {
                    // A market order is immediate by nature: whatever the book
                    // could not fill is gone, not resting at an unknown price.
                    self.stats.exhausted_book += 1;
                    self.stats.cancelled += 1;
                    self.out.push(ExecutionEvent::Cancelled {
                        client_order_id: order.id,
                        remaining: order.remaining,
                        ts: now,
                    });
                    order.remaining = Qty::from_raw(0);
                }
            } else if printed.is_some_and(|px| Self::traded_through(order, px)) && book.is_live() {
                self.take(order, book, now);
            } else if order.request.time_in_force == TimeInForce::Ioc {
                // Immediate-or-cancel: one look, then gone.
                self.stats.cancelled += 1;
                self.out.push(ExecutionEvent::Cancelled {
                    client_order_id: order.id,
                    remaining: order.remaining,
                    ts: now,
                });
                order.remaining = Qty::from_raw(0);
            }
        }
        orders.retain(|o| o.remaining.raw() > 0);
        // Anything submitted while we were matching -- there is nothing, but the
        // append is what keeps that true if that ever changes.
        orders.append(&mut self.resting);
        self.resting = orders;
    }

    fn poll(&mut self, out: &mut Vec<ExecutionEvent>) {
        out.append(&mut self.out);
    }
}

/// `px * qty`, in quote currency.
///
/// `quant-core`'s arithmetic, not our own: this module had its own 128-bit
/// `mul_div` and so did `quant-engine`, which is two copies of a money
/// calculation that must agree with each other forever. `Px::notional` predates
/// both.
fn notional(px: Px, qty: Qty) -> Notional {
    px.notional(qty)
        .expect("a notional beyond i64 means the inputs were wrong")
}

/// The size-weighted average price actually paid.
fn average_price(cost: Notional, qty: Qty) -> Px {
    cost.per_unit(qty)
        .expect("a fill with a non-zero size has a price")
}

#[cfg(test)]
mod tests;
