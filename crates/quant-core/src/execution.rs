//! Orders, and what comes back from having sent one.
//!
//! The counterpart to [`event`](crate::event): that module is what the market
//! tells us, this is what we tell a venue and what it tells us in return.
//! `docs/engine-contract.md` argues the shape; this is the shape.
//!
//! # Why an outcome is an event and not a return value
//!
//! [`OrderRequest`] goes out and nothing comes back but a [`ClientOrderId`].
//! Everything that then happens to the order — accepted, rejected, filled,
//! cancelled — arrives later as an [`ExecutionEvent`], into the same loop that
//! receives market data.
//!
//! The alternative, `submit(order) -> Result<Fill>`, cannot be implemented
//! honestly by a live venue: there is a network round trip in the middle, so it
//! would either block the engine or invent an answer. A *simulated* venue,
//! though, can return instantly — which would hand a strategy the outcome of its
//! own order at the moment of placing it, in backtest only. A strategy tuned on
//! that is tuned on a machine that does not exist.
//!
//! So the asynchrony is in the contract rather than in one implementation of it.
//! A strategy has to track its own outstanding orders, in all three worlds,
//! which is the cost production charges anyway.
//!
//! # Why there are two identifiers
//!
//! [`ClientOrderId`] is ours and exists **before** the request leaves.
//! [`VenueOrderId`] is the venue's and may never exist at all: a rejected
//! request was never an order, so there is nothing for the venue to name. If the
//! venue's identifier were the only one, a rejection could not be matched to the
//! submission that caused it and the strategy would hold an order it could never
//! resolve.

use crate::fixed::{Px, Qty};
use crate::instrument::InstrumentId;
use crate::time::Ts;

/// Our identifier for an order, minted before it is sent.
///
/// Dense from 1 and monotonic within a run, and persisted: written on every
/// journal line about the order, carried across a restart by the engine's
/// `minting_from`, and sent to a live venue as `q-<run>-<id>`, so that a report
/// can be told apart from one about an earlier run's order with the same
/// number. Not the thing to reconcile an exchange statement against — that is
/// [`VenueOrderId`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ClientOrderId(pub u64);

impl core::fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "c{}", self.0)
    }
}

/// The venue's identifier for an order.
///
/// A `String` rather than an integer because it is the venue's namespace, not
/// ours: Binance uses integers, others use UUIDs or opaque tokens, and
/// normalising them into a number we invented would destroy the one property
/// this field has — that pasting it into the venue's own interface finds the
/// order.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct VenueOrderId(pub String);

impl core::fmt::Display for VenueOrderId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The venue's identifier for one execution: Binance's `t` on a stream report,
/// and `id` on a `myTrades` row.
///
/// A type of its own rather than a second [`VenueOrderId`], because the order
/// id and the trade id are both integers on one `myTrades` row, and a swap must
/// not compile. A `String` for the reason `VenueOrderId` is one: it is the
/// venue's namespace, and pasting it into the venue's interface has to find the
/// trade.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct VenueTradeId(pub String);

impl core::fmt::Display for VenueTradeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

/// How an order is priced.
///
/// Deliberately two variants. Every venue has more, and every extra one is a
/// different set of edge cases in the simulator, the risk layer and the
/// reconciliation — so they arrive when a strategy needs them and can say what
/// the simulator should do with them, not before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OrderKind {
    /// Cross the spread now, at whatever the book offers.
    ///
    /// Carries no price, which is the honest encoding: a market order's fill
    /// price is an *outcome*, and a field for it here would be a prediction the
    /// caller is not entitled to make.
    Market,
    /// Rest at `limit` or better, never worse.
    Limit { limit: Px },
}

/// How long an order stays alive.
///
/// `Gtc` and `Ioc` only, for the reason [`OrderKind`] is short. Note the absence
/// of `Day`: it means different things on venues with different session
/// calendars, and this platform's first venue has none at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    /// Rest until filled or cancelled.
    #[default]
    Gtc,
    /// Take whatever is available immediately; cancel the remainder.
    Ioc,
}

/// An instruction to a venue.
///
/// Note what is **not** here: no timestamp, no venue, no account. The engine
/// stamps the time, the wiring determines the venue, and an account is a
/// property of the connection rather than of the instruction. A strategy that
/// could name any of them could tell which world it was running in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OrderRequest {
    pub instrument: InstrumentId,
    pub side: crate::event::Side,
    /// Size in base units, fixed-point. Always positive; direction is `side`.
    ///
    /// A signed quantity would make "sell -5" and "buy 5" both expressible and
    /// let a sign error become a position twice the size in the wrong
    /// direction.
    pub qty: Qty,
    pub kind: OrderKind,
    pub time_in_force: TimeInForce,
}

impl OrderRequest {
    /// The limit price, if this order has one.
    #[must_use]
    pub const fn limit(&self) -> Option<Px> {
        match self.kind {
            OrderKind::Market => None,
            OrderKind::Limit { limit } => Some(limit),
        }
    }

    /// Whether the request is self-consistent.
    ///
    /// Checked at the seam rather than trusted, because a zero or negative size
    /// is the kind of thing an arithmetic slip produces and a venue answers with
    /// an opaque error code hours later.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.qty.raw() > 0 && self.limit().is_none_or(|px| px.raw() > 0)
    }
}

/// Why a venue would not take an order.
///
/// A closed set of coarse categories rather than the venue's error text, for the
/// reason `ControlRecord`'s failure reasons are: this crosses into recorded
/// state, and unbounded remote strings do not belong there. The venue's own
/// message belongs in a log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    /// The request did not satisfy [`OrderRequest::is_valid`], or a venue
    /// filter — tick size, lot size, minimum notional.
    Malformed,
    /// Not enough balance or margin.
    InsufficientFunds,
    /// The risk layer refused it. Never reaches a venue at all.
    RiskLimit,
    /// The venue declined for a reason of its own, the request never reached
    /// it, or it was not ready to take an order. Never an order whose outcome
    /// is unknown: that is no event at all, because a rejection would make the
    /// engine forget an order that may yet fill.
    Venue,
    /// The book was invalid — a gap, or no prices yet — so there was nothing to
    /// price against. Its own variant because it is the *expected* rejection
    /// after a disconnect, and counting it as a venue error would bury the
    /// unexpected ones.
    NoMarket,
}

/// One execution against an order.
///
/// A partial fill is a fill: an order may produce several, and the sum of their
/// `qty` cannot exceed the request's.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Fill {
    pub px: Px,
    pub qty: Qty,
    /// What the venue charged. Positive is a cost, negative is a rebate — maker
    /// rebates are real and a type that could not express one would quietly
    /// misprice every passive strategy.
    ///
    /// Zero throughout M3, where fees are deliberately absent; M4 fills it in.
    pub fee: crate::fixed::Notional,
    /// Whether this side rested in the book and was traded against.
    ///
    /// Recorded rather than derived because it decides the fee tier on every
    /// venue that has one, and by M4 it must not be a guess.
    pub is_maker: bool,
    /// Which asset the fee was taken in.
    ///
    /// M4 charged every fee in the quote currency and named that a
    /// simplification: *"a spot venue takes its fee in the base, leaving the
    /// position a fraction smaller rather than the cash a fraction lower"*. M8
    /// is where that bill arrives, because the venue reports the asset and our
    /// record must agree with the venue's to the satoshi.
    ///
    /// Defaults to [`FeeAsset::Quote`] so every result computed before this
    /// field existed reproduces exactly — the `Costs::NONE` no-op property in a
    /// third place, and what makes a change to the money path checkable rather
    /// than hoped for.
    #[serde(default)]
    pub fee_asset: FeeAsset,
}

impl Fill {
    /// The fee as money, in the quote currency, whichever asset it was taken in.
    ///
    /// For [`FeeAsset::Base`] the `fee` field holds a *quantity of the base*,
    /// so it is valued at this fill's own price. Every total of fees, and every
    /// limit that counts them, must go through this rather than reading `fee`
    /// directly: adding a base quantity to a quote total understates it by a
    /// factor of the price, and nothing fails when you do it. One method here,
    /// so the portfolio and the risk layer cannot each convert it slightly
    /// differently.
    ///
    /// # Panics
    ///
    /// If the product overflows `Notional`, which needs a fee worth more than
    /// about 92 billion of the quote.
    #[must_use]
    pub fn fee_in_quote(&self) -> crate::fixed::Notional {
        match self.fee_asset {
            FeeAsset::Quote => self.fee,
            FeeAsset::Base => self
                .px
                .notional(Qty::from_raw(self.fee.raw()))
                .expect("a fee small enough to be taken is small enough to value"),
        }
    }

    /// The part of the fee taken out of the base asset, as a quantity.
    ///
    /// Zero for a quote fee. What a buyer receives is `qty` less this, and a
    /// seller parts with `qty` plus it.
    #[must_use]
    pub const fn fee_in_base(&self) -> Qty {
        match self.fee_asset {
            FeeAsset::Quote => Qty::from_raw(0),
            FeeAsset::Base => Qty::from_raw(self.fee.raw()),
        }
    }
}

/// Which side of the pair a fee was taken from.
///
/// Two variants and not a string: the set is closed by what a spot venue can do,
/// and an open type here would let a venue's asset code reach the portfolio,
/// where nothing could act on it anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeAsset {
    /// USDT for BTCUSDT. What a simulated fill charges unless the simulator is
    /// told to charge as a spot venue does, and M4's simplification.
    #[default]
    Quote,
    /// BTC for BTCUSDT. What Binance takes on a buy: the position arrives a
    /// fraction smaller rather than the cash a fraction lower.
    Base,
}

/// What happened to an order we sent, as a strategy is told it.
///
/// Every variant quotes the [`ClientOrderId`], because that is the only
/// identifier guaranteed to exist — see the module docs.
///
/// **Every field is one a simulator can produce exactly as a live venue does.**
/// A strategy is told this and nothing else, and a field only a real venue
/// could fill is a field by which a strategy could tell which venue it is
/// wired to, which is a bug. What only a real venue knows travels beside the
/// event, to the record alone, in a [`crate::report::OrderReport`]. The
/// acceptance carried one such field, the venue's order id, until M8 slice (c)
/// found it: the live translator filled it and the simulator left it empty.
/// It is gone, and a compile-time check below keeps anything like it out:
///
/// ```compile_fail
/// use quant_core::execution::{ClientOrderId, ExecutionEvent};
/// use quant_core::time::Ts;
/// let _ = ExecutionEvent::Accepted {
///     client_order_id: ClientOrderId(1),
///     venue_order_id: None,
///     ts: Ts::from_nanos(1),
/// };
/// ```
///
/// while an acceptance a simulator can make is what remains:
///
/// ```
/// use quant_core::execution::{ClientOrderId, ExecutionEvent};
/// use quant_core::time::Ts;
/// let _ = ExecutionEvent::Accepted {
///     client_order_id: ClientOrderId(1),
///     ts: Ts::from_nanos(1),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionEvent {
    /// The venue has the order and it is live.
    ///
    /// Not the venue's name for it: that is the record's, beside the event.
    Accepted {
        client_order_id: ClientOrderId,
        ts: Ts,
    },
    /// The order was refused and does not exist.
    Rejected {
        client_order_id: ClientOrderId,
        reason: RejectReason,
        ts: Ts,
    },
    /// Some or all of the order traded.
    Filled {
        client_order_id: ClientOrderId,
        fill: Fill,
        /// Size still working after this fill. Zero means the order is done.
        ///
        /// Carried rather than left for the recipient to subtract, because the
        /// venue is the authority on it and a recipient's own arithmetic can
        /// drift from the venue's over a long session — which is the number a
        /// reconciliation would then disagree about.
        remaining: Qty,
        ts: Ts,
    },
    /// The order is no longer working and will not fill again.
    Cancelled {
        client_order_id: ClientOrderId,
        /// Size that never traded.
        remaining: Qty,
        ts: Ts,
    },
}

impl ExecutionEvent {
    /// The order this concerns.
    #[must_use]
    pub const fn client_order_id(&self) -> ClientOrderId {
        match self {
            Self::Accepted {
                client_order_id, ..
            }
            | Self::Rejected {
                client_order_id, ..
            }
            | Self::Filled {
                client_order_id, ..
            }
            | Self::Cancelled {
                client_order_id, ..
            } => *client_order_id,
        }
    }

    /// When it was heard: from a simulator, the engine clock when the event was
    /// generated; from a live venue, when its bytes arrived. Never a time the
    /// venue wrote (invariant 2).
    #[must_use]
    pub const fn ts(&self) -> Ts {
        match self {
            Self::Accepted { ts, .. }
            | Self::Rejected { ts, .. }
            | Self::Filled { ts, .. }
            | Self::Cancelled { ts, .. } => *ts,
        }
    }

    /// Whether the order is finished after this event.
    ///
    /// The one place the lifecycle is stated, so a strategy tracking outstanding
    /// orders does not have to restate it — and cannot restate it differently
    /// from the simulator.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        match self {
            Self::Rejected { .. } | Self::Cancelled { .. } => true,
            Self::Filled { remaining, .. } => remaining.raw() == 0,
            Self::Accepted { .. } => false,
        }
    }
}

/// What a strategy may be told, closed by a compile error rather than a
/// paragraph.
///
/// A type is `Modelled` when a simulator produces its values exactly as a live
/// venue does, so a strategy reading one learns nothing about its wiring. The
/// trait is private to this module, so nothing elsewhere can declare a type
/// modelled; and [`strategy_may_see`] names every field of every
/// [`ExecutionEvent`] variant, with no `..`, and requires each to be
/// `Modelled`. A field added to an event therefore fails to compile until its
/// type is argued into this list, here, where the argument is visible.
///
/// That closes fields, and an enum a field carries has variants too. So the two
/// enums an event carries, [`RejectReason`] and [`FeeAsset`], are not declared
/// modelled on their own say-so: [`reject_reason_is_modelled`] and
/// [`fee_asset_is_modelled`] match every variant, with no `_` arm, and an arm
/// for a variant that carries a value passes every value to [`modelled`].
/// `RejectReason::VenueCode(i64)`, a venue's error code that a simulator has
/// none of, therefore fails to compile here rather than reaching a strategy.
///
/// There is deliberately no `impl<T: Modelled> Modelled for Option<T>`: an
/// optional field is how a venue's order id got in, absent in one world and
/// present in the other.
mod vocabulary {
    pub(super) trait Modelled {}
}

use vocabulary::Modelled;

impl Modelled for ClientOrderId {}
impl Modelled for Ts {}
impl Modelled for crate::fixed::Px {}
impl Modelled for Qty {}
impl Modelled for crate::fixed::Notional {}
impl Modelled for RejectReason {}
impl Modelled for FeeAsset {}
impl Modelled for bool {}

/// Requires `T` to be [`Modelled`], and does nothing else.
const fn modelled<T: Modelled>(_: &T) {}

/// Checked when the crate compiles, never called.
const _: fn(&ExecutionEvent) = strategy_may_see;
const _: fn(RejectReason) = reject_reason_is_modelled;
const _: fn(FeeAsset) = fee_asset_is_modelled;

/// Every variant, by name. None carries a value; one that did would bind it
/// here and pass it to [`modelled`].
const fn reject_reason_is_modelled(reason: RejectReason) {
    match reason {
        RejectReason::Malformed
        | RejectReason::InsufficientFunds
        | RejectReason::RiskLimit
        | RejectReason::Venue
        | RejectReason::NoMarket => {}
    }
}

/// Every variant, by name. None carries a value; one that did would bind it
/// here and pass it to [`modelled`].
const fn fee_asset_is_modelled(asset: FeeAsset) {
    match asset {
        FeeAsset::Quote | FeeAsset::Base => {}
    }
}

/// Every field of every variant, each required to be [`Modelled`].
fn strategy_may_see(event: &ExecutionEvent) {
    match event {
        ExecutionEvent::Accepted {
            client_order_id,
            ts,
        } => {
            modelled(client_order_id);
            modelled(ts);
        }
        ExecutionEvent::Rejected {
            client_order_id,
            reason,
            ts,
        } => {
            modelled(client_order_id);
            modelled(reason);
            modelled(ts);
        }
        ExecutionEvent::Filled {
            client_order_id,
            fill:
                Fill {
                    px,
                    qty,
                    fee,
                    is_maker,
                    fee_asset,
                },
            remaining,
            ts,
        } => {
            modelled(client_order_id);
            modelled(px);
            modelled(qty);
            modelled(fee);
            modelled(is_maker);
            modelled(fee_asset);
            modelled(remaining);
            modelled(ts);
        }
        ExecutionEvent::Cancelled {
            client_order_id,
            remaining,
            ts,
        } => {
            modelled(client_order_id);
            modelled(remaining);
            modelled(ts);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Side;
    use crate::fixed::Notional;

    fn instrument() -> InstrumentId {
        use crate::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
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

    fn request(qty: &str, kind: OrderKind) -> OrderRequest {
        OrderRequest {
            instrument: instrument(),
            side: Side::Buy,
            qty: qty.parse().expect("a valid quantity"),
            kind,
            time_in_force: TimeInForce::Gtc,
        }
    }

    #[test]
    fn a_market_order_carries_no_price() {
        // The honest encoding: a market order's fill price is an outcome, and a
        // field for it would be a prediction the caller cannot make.
        assert_eq!(request("1", OrderKind::Market).limit(), None);
    }

    #[test]
    fn a_non_positive_size_is_invalid() {
        // Direction is `side`; a negative size would make "sell -5" expressible
        // and turn a sign slip into a double-size position the wrong way.
        assert!(request("1", OrderKind::Market).is_valid());
        assert!(!request("0", OrderKind::Market).is_valid());
        assert!(!request("-1", OrderKind::Market).is_valid());
    }

    #[test]
    fn a_non_positive_limit_is_invalid() {
        let limit = OrderKind::Limit {
            limit: "0".parse().expect("px"),
        };
        assert!(!request("1", limit).is_valid());
    }

    #[test]
    fn the_lifecycle_is_stated_once() {
        // A strategy tracking outstanding orders must not have to restate this,
        // because then it could restate it differently from the simulator.
        let id = ClientOrderId(1);
        let ts = Ts::from_nanos(1);
        let partial = ExecutionEvent::Filled {
            client_order_id: id,
            fill: Fill {
                px: "100".parse().expect("px"),
                qty: "1".parse().expect("qty"),
                fee: Notional::from_raw(0),
                is_maker: false,
                fee_asset: FeeAsset::Quote,
            },
            remaining: "1".parse().expect("qty"),
            ts,
        };
        assert!(!partial.is_terminal(), "a partial fill leaves it working");

        let ExecutionEvent::Filled { fill, .. } = &partial else {
            unreachable!()
        };
        let complete = ExecutionEvent::Filled {
            client_order_id: id,
            fill: fill.clone(),
            remaining: "0".parse().expect("qty"),
            ts,
        };
        assert!(complete.is_terminal());

        assert!(ExecutionEvent::Cancelled {
            client_order_id: id,
            remaining: "1".parse().expect("qty"),
            ts,
        }
        .is_terminal());
        assert!(ExecutionEvent::Rejected {
            client_order_id: id,
            reason: RejectReason::RiskLimit,
            ts,
        }
        .is_terminal());
        assert!(!ExecutionEvent::Accepted {
            client_order_id: id,
            ts
        }
        .is_terminal());
    }

    #[test]
    fn every_execution_event_names_the_order_we_minted() {
        // The property the whole two-identifier scheme exists for: a rejection
        // can be correlated even though the venue never named the order.
        let id = ClientOrderId(7);
        let ts = Ts::from_nanos(1);
        let events = [
            ExecutionEvent::Accepted {
                client_order_id: id,
                ts,
            },
            ExecutionEvent::Rejected {
                client_order_id: id,
                reason: RejectReason::NoMarket,
                ts,
            },
            ExecutionEvent::Cancelled {
                client_order_id: id,
                remaining: "1".parse().expect("qty"),
                ts,
            },
        ];
        for event in events {
            assert_eq!(event.client_order_id(), id);
            assert_eq!(event.ts(), ts);
        }
    }

    #[test]
    fn a_fee_can_be_a_rebate() {
        // Maker rebates are real. A type that could not express one would
        // quietly misprice every passive strategy.
        let fill = Fill {
            px: "100".parse().expect("px"),
            qty: "1".parse().expect("qty"),
            fee: "-0.0001".parse().expect("a rebate"),
            is_maker: true,
            fee_asset: FeeAsset::Quote,
        };
        assert!(fill.fee.raw() < 0);
    }

    #[test]
    fn execution_events_round_trip_through_serde() {
        // They cross into recorded state at M7, so the wire form is pinned now
        // rather than discovered later.
        let event = ExecutionEvent::Filled {
            client_order_id: ClientOrderId(3),
            fill: Fill {
                px: "76650.12345678".parse().expect("px"),
                qty: "0.00040000".parse().expect("qty"),
                fee: "0.03".parse().expect("fee"),
                is_maker: false,
                fee_asset: FeeAsset::Quote,
            },
            remaining: "0".parse().expect("qty"),
            ts: Ts::from_nanos(1_787_315_652_514_000_001),
        };
        let json = serde_json::to_string(&event).expect("serialize");
        assert_eq!(
            serde_json::from_str::<ExecutionEvent>(&json).expect("deserialize"),
            event
        );
    }
}
