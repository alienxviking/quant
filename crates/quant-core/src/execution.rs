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
/// Opaque and monotonic within a run. Not persisted across runs, and not the
/// thing to reconcile an exchange statement against — that is
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
    /// The venue declined for a reason of its own, or was unreachable.
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
}

/// What happened to an order we sent.
///
/// Every variant quotes the [`ClientOrderId`], because that is the only
/// identifier guaranteed to exist — see the module docs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionEvent {
    /// The venue has the order and it is live.
    Accepted {
        client_order_id: ClientOrderId,
        /// Absent when the venue does not supply one, which is legal.
        venue_order_id: Option<VenueOrderId>,
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

    /// When it happened, on the engine clock.
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
            venue_order_id: None,
            ts,
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
                venue_order_id: None,
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
