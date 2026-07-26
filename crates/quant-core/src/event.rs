//! The market data event contract.
//!
//! This is the narrow waist of the whole platform. Recorders produce these,
//! the normalizer persists these, the backtester replays these, and every
//! strategy consumes only these. A venue adapter's entire job is to turn
//! that venue's idiosyncratic JSON into this vocabulary.
//!
//! Two rules keep it honest:
//!
//! 1. **Nothing venue-specific leaks in.** The moment `MarketEvent` grows a
//!    `binance_event_type` field, every strategy becomes coupled to Binance
//!    and the multi-venue goal is dead.
//! 2. **Nothing is derived here.** Mid price, imbalance, VWAP and so on are
//!    computed downstream from these primitives. If we store a derived value
//!    we have to keep it correct forever, and we lose the ability to change
//!    the definition without re-recording.
//!
//! Changing anything in this file is a data-contract change. See
//! `docs/data-contract.md` for the versioning rules.

use crate::fixed::{Px, Qty};
use crate::instrument::InstrumentId;
use crate::time::Ts;

/// Which side of the book, or which side initiated a trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    /// `+1` for buy, `-1` for sell. Convenient for turning a side and a size
    /// into a signed position delta without branching at the call site.
    #[must_use]
    pub const fn sign(self) -> i64 {
        match self {
            Self::Buy => 1,
            Self::Sell => -1,
        }
    }
}

/// One price level of a book update or snapshot.
///
/// A `qty` of zero in a *delta* means "remove this level" -- that is the
/// convention every major venue uses, and preserving it means the delta
/// stream stays a faithful record of what the venue sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Level {
    pub px: Px,
    pub qty: Qty,
}

impl Level {
    #[must_use]
    pub const fn new(px: Px, qty: Qty) -> Self {
        Self { px, qty }
    }

    #[must_use]
    pub const fn is_removal(self) -> bool {
        self.qty.is_zero()
    }
}

/// Timestamps and provenance carried by every event.
///
/// Factored into its own struct so that adding a field (say, a gateway hop
/// timestamp when we colocate) is one change rather than one per event type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct EventMeta {
    pub instrument: InstrumentId,
    /// The venue's timestamp, as reported. May be coarse or skewed.
    pub exchange_ts: Ts,
    /// When our process first saw the bytes. **The only timestamp a strategy
    /// may act on.** See the module docs in `time.rs`.
    pub local_recv_ts: Ts,
    /// Monotonically increasing per (recorder process, instrument).
    ///
    /// This is *ours*, not the venue's. It is what lets us prove after the
    /// fact that a replayed file is complete and in order, independently of
    /// whatever the venue's sequencing scheme happens to be.
    pub ingest_seq: u64,
}

impl EventMeta {
    /// Observed one-way latency from the venue, in nanoseconds. Signed:
    /// see [`Ts::delta_nanos`].
    #[must_use]
    pub const fn latency_nanos(&self) -> i64 {
        self.local_recv_ts.delta_nanos(self.exchange_ts)
    }
}

/// A public trade print.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Trade {
    pub meta: EventMeta,
    pub px: Px,
    /// Always positive; direction is in [`Trade::aggressor`].
    pub qty: Qty,
    /// Which side crossed the spread. This is the informative part of a
    /// trade print -- order-flow-imbalance strategies are built on it, and
    /// venues that omit it force us to infer it (badly).
    pub aggressor: Side,
    /// The venue's trade id, for dedup across reconnects.
    pub venue_trade_id: u64,
}

/// An incremental order book update.
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct BookDelta {
    pub meta: EventMeta,
    /// Venue update-id range this delta covers.
    ///
    /// Binance's depth stream is only usable if you check that each message's
    /// `first_update_id` continues from the previous message's
    /// `final_update_id`. A gap means the book is now wrong and must be
    /// resynchronized from a fresh snapshot. Storing both ids means we can
    /// verify that offline, on recorded data, instead of trusting that the
    /// recorder got it right at the time.
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// A full book snapshot: the state to apply deltas on top of.
///
/// Recorded periodically as well as on resync. Periodic snapshots are what
/// make a recorded day *seekable* -- without them, replaying an hour from
/// the middle of a file means applying every delta since midnight.
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct BookSnapshot {
    pub meta: EventMeta,
    /// The venue update id this snapshot is current as of.
    pub last_update_id: u64,
    /// Descending by price.
    pub bids: Vec<Level>,
    /// Ascending by price.
    pub asks: Vec<Level>,
}

/// Why a stream was interrupted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapCause {
    /// The transport dropped.
    Disconnect,
    /// Update ids were not contiguous: we missed messages.
    SequenceGap,
    /// We could not keep up and dropped messages ourselves. The most
    /// important one to record honestly -- it is a capacity problem, and
    /// hiding it means never learning we have one.
    LocalOverflow,
    /// The recorder process started or restarted.
    RecorderRestart,
}

/// An explicit marker that data is missing.
///
/// This is the event type most homegrown recorders lack, and its absence is
/// what makes their data quietly untrustworthy. Without it, a backtest
/// cannot distinguish "the market was silent for 40 seconds" from "we were
/// disconnected for 40 seconds" -- and a strategy will happily learn to
/// trade the gap. Recording the gap lets the backtester refuse to trade
/// across it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct Gap {
    pub meta: EventMeta,
    pub cause: GapCause,
    /// Last local time we are confident the stream was intact.
    pub last_good_ts: Ts,
}

/// Everything a strategy can observe about the market.
#[derive(Debug, Clone, PartialEq, Eq)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketEvent {
    Trade(Trade),
    BookDelta(BookDelta),
    BookSnapshot(BookSnapshot),
    Gap(Gap),
}

impl MarketEvent {
    #[must_use]
    pub fn meta(&self) -> &EventMeta {
        match self {
            Self::Trade(e) => &e.meta,
            Self::BookDelta(e) => &e.meta,
            Self::BookSnapshot(e) => &e.meta,
            Self::Gap(e) => &e.meta,
        }
    }

    #[must_use]
    pub fn instrument(&self) -> InstrumentId {
        self.meta().instrument
    }

    /// The timestamp the engine orders and dispatches events by. Always the
    /// local receive time -- never the exchange time. See `time.rs`.
    #[must_use]
    pub fn dispatch_ts(&self) -> Ts {
        self.meta().local_recv_ts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};

    fn meta(id: InstrumentId) -> EventMeta {
        EventMeta {
            instrument: id,
            exchange_ts: Ts::from_millis(1_699_999_999_000),
            local_recv_ts: Ts::from_millis(1_699_999_999_042),
            ingest_seq: 7,
        }
    }

    fn registry() -> (InstrumentRegistry, InstrumentId) {
        let mut reg = InstrumentRegistry::new();
        let id = reg.register(InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.01".parse().unwrap(),
            lot_size: "0.00001".parse().unwrap(),
            min_notional: "5".parse().unwrap(),
        });
        (reg, id)
    }

    #[test]
    fn events_dispatch_on_local_time_not_exchange_time() {
        let (_reg, id) = registry();
        let ev = MarketEvent::Trade(Trade {
            meta: meta(id),
            px: "68123.45".parse().unwrap(),
            qty: "0.01".parse().unwrap(),
            aggressor: Side::Buy,
            venue_trade_id: 1,
        });
        assert_eq!(ev.dispatch_ts(), Ts::from_millis(1_699_999_999_042));
        assert_ne!(ev.dispatch_ts(), ev.meta().exchange_ts);
        assert_eq!(ev.meta().latency_nanos(), 42_000_000);
    }

    #[test]
    fn zero_qty_level_means_removal() {
        let lvl = Level::new("68123.45".parse().unwrap(), Qty::ZERO);
        assert!(lvl.is_removal());
        assert!(!Level::new("68123.45".parse().unwrap(), "0.5".parse().unwrap()).is_removal());
    }

    #[test]
    fn event_json_is_self_describing_and_round_trips() {
        let (_reg, id) = registry();
        let ev = MarketEvent::Gap(Gap {
            meta: meta(id),
            cause: GapCause::SequenceGap,
            last_good_ts: Ts::from_millis(1_699_999_998_000),
        });
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"type\":\"gap\""));
        assert!(json.contains("\"cause\":\"sequence_gap\""));
        let back: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ev);
    }

    #[test]
    fn side_sign_turns_a_fill_into_a_position_delta() {
        assert_eq!(Side::Buy.sign(), 1);
        assert_eq!(Side::Sell.sign(), -1);
        assert_eq!(Side::Buy.opposite(), Side::Sell);
    }
}
