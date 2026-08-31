//! Binance payloads into [`MarketEvent`], in full.
//!
//! # This is the normalizer's half, and it is allowed to be wrong
//!
//! `sequence.rs` reads four fields and is used by the verifier to answer a
//! *completeness* question about a capture. This module reads everything and
//! answers a *correctness* question about the reconstruction -- and the two have
//! very different consequences when they are wrong.
//!
//! A mistake here costs a re-derive. The raw bytes are still on disk, still say
//! exactly what the venue said, and the normalized tier is disposable by
//! construction. That is the whole bargain `docs/data-contract.md` §1 strikes, and
//! it is why this parser can afford to be strict, opinionated, and rewritten later
//! without anybody losing data.
//!
//! # Why there are two parsers for one dialect
//!
//! Because they are asked different questions at very different rates. The
//! verifier walks seventy million frames and needs `U` and `u`; allocating a
//! `Vec<Level>` per delta to learn two integers would be absurd. This one runs
//! when the normalized tier is rebuilt, and needs every field.
//!
//! Two parsers for one venue is a drift risk, and it is bounded rather than
//! ignored: `both_parsers_agree_on_what_a_message_is` pins that they classify the
//! same bytes the same way. If they ever disagree about what a depth message is,
//! that test fails -- rather than a book quietly reconstructing itself from the
//! wrong set of messages.
//!
//! # Strictness
//!
//! Loud about anything it claims to understand, per invariant 5. A `depthUpdate`
//! missing `U`, or a price that will not parse to fixed-point, is an error and not
//! a skipped field: a malformed price means our model of the venue is wrong, and
//! rounding or defaulting it would put a silently incorrect number into a book that
//! a strategy later trades against.
//!
//! Unknown *event types* stay `Ok(None)`. A stream this build has not been taught
//! about is not a defect in the capture.

use core::fmt;

use quant_core::event::{BookDelta, BookSnapshot, EventMeta, Level, MarketEvent, Side, Trade};
use quant_core::fixed::ParseFixedError;
use quant_core::instrument::InstrumentId;
use quant_core::time::Ts;

use crate::sequence::{DEPTH_EVENT, TRADE_EVENT};

/// Why a payload could not be turned into an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Not the shape the combined-stream endpoint produces.
    NotAStreamMessage(String),
    /// Not the shape a depth snapshot response has.
    NotASnapshot(String),
    /// An event type we claim to understand is missing a field it must have.
    MissingField {
        event: &'static str,
        field: &'static str,
    },
    /// A decimal string the venue sent will not parse to fixed-point.
    ///
    /// Carries the offending text, because "a price somewhere was malformed" is
    /// not an actionable report and the file it came from has millions of frames.
    BadNumber {
        field: &'static str,
        value: String,
        source: ParseFixedError,
    },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAStreamMessage(e) => write!(f, "not a combined-stream message: {e}"),
            Self::NotASnapshot(e) => write!(f, "not a depth snapshot: {e}"),
            Self::MissingField { event, field } => {
                write!(f, "{event} payload has no `{field}` field")
            }
            Self::BadNumber {
                field,
                value,
                source,
            } => write!(
                f,
                "`{field}` is not a valid fixed-point value: {value:?} ({source})"
            ),
        }
    }
}

impl std::error::Error for ParseError {}

/// One `[price, quantity]` pair as the venue writes it.
///
/// Borrowed from the payload: these strings never contain escapes, and a
/// 5000-level snapshot would otherwise allocate twenty thousand `String`s only to
/// drop them a microsecond later.
type RawLevel<'a> = [&'a str; 2];

#[derive(serde::Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    data: Payload<'a>,
}

#[derive(serde::Deserialize)]
struct Payload<'a> {
    #[serde(rename = "e", borrow)]
    event: Option<&'a str>,
    /// Event time: when the venue *emitted* the message.
    #[serde(rename = "E")]
    event_time_millis: Option<i64>,
    #[serde(rename = "U")]
    first_update_id: Option<u64>,
    #[serde(rename = "u")]
    final_update_id: Option<u64>,
    #[serde(rename = "b", borrow, default)]
    bids: Vec<RawLevel<'a>>,
    #[serde(rename = "a", borrow, default)]
    asks: Vec<RawLevel<'a>>,
    #[serde(rename = "t")]
    trade_id: Option<u64>,
    #[serde(rename = "p", borrow)]
    price: Option<&'a str>,
    #[serde(rename = "q", borrow)]
    quantity: Option<&'a str>,
    /// "Is the buyer the market maker?" -- see [`aggressor`].
    #[serde(rename = "m")]
    buyer_is_maker: Option<bool>,
}

#[derive(serde::Deserialize)]
struct SnapshotBody<'a> {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(borrow)]
    bids: Vec<RawLevel<'a>>,
    #[serde(borrow)]
    asks: Vec<RawLevel<'a>>,
}

/// Turn one recorded venue stream payload into an event.
///
/// `Ok(None)` for a subscribed stream this build does not interpret.
///
/// The three values that cannot come from the payload are passed in, exactly as
/// `ControlRecord::into_market_event` does it: `instrument`, because an
/// `InstrumentId` is a registry index that must never be persisted, and
/// `local_recv_ts` and `ingest_seq`, because they are *ours* and live in the frame
/// header rather than in the venue's bytes.
pub fn parse_stream_message(
    payload: &[u8],
    instrument: InstrumentId,
    local_recv_ts: Ts,
    ingest_seq: u64,
) -> Result<Option<MarketEvent>, ParseError> {
    let data = serde_json::from_slice::<Envelope<'_>>(payload)
        .map_err(|e| ParseError::NotAStreamMessage(e.to_string()))?
        .data;

    // Rebound to the constant rather than kept as the borrowed slice: `ParseError`
    // holds `&'static str` so an error can outlive the payload it came from, and a
    // payload-borrowed name cannot.
    let event: &'static str = match data.event {
        Some(DEPTH_EVENT) => DEPTH_EVENT,
        Some(TRADE_EVENT) => TRADE_EVENT,
        _ => return Ok(None),
    };

    // `E`, not a trade's `T`, and the same field for both event types. `E` is when
    // the venue emitted the message, so `local_recv_ts - exchange_ts` measures
    // transport; `T` would fold Binance's own match-to-publish delay into what we
    // report as network latency. `T` stays in the raw tier for anyone who needs it.
    let meta = EventMeta {
        instrument,
        exchange_ts: Ts::from_millis(
            data.event_time_millis
                .ok_or(ParseError::MissingField { event, field: "E" })?,
        ),
        local_recv_ts,
        ingest_seq,
    };

    let required = |value: Option<u64>, field: &'static str| {
        value.ok_or(ParseError::MissingField { event, field })
    };

    if event == DEPTH_EVENT {
        return Ok(Some(MarketEvent::BookDelta(BookDelta {
            meta,
            first_update_id: required(data.first_update_id, "U")?,
            final_update_id: required(data.final_update_id, "u")?,
            bids: levels(&data.bids, "b")?,
            asks: levels(&data.asks, "a")?,
        })));
    }

    Ok(Some(MarketEvent::Trade(Trade {
        meta,
        px: number(
            data.price
                .ok_or(ParseError::MissingField { event, field: "p" })?,
            "p",
        )?,
        qty: number(
            data.quantity
                .ok_or(ParseError::MissingField { event, field: "q" })?,
            "q",
        )?,
        aggressor: aggressor(
            data.buyer_is_maker
                .ok_or(ParseError::MissingField { event, field: "m" })?,
        ),
        venue_trade_id: required(data.trade_id, "t")?,
    })))
}

/// Turn one recorded venue snapshot body into an event.
pub fn parse_snapshot(
    payload: &[u8],
    instrument: InstrumentId,
    local_recv_ts: Ts,
    ingest_seq: u64,
) -> Result<BookSnapshot, ParseError> {
    let body = serde_json::from_slice::<SnapshotBody<'_>>(payload)
        .map_err(|e| ParseError::NotASnapshot(e.to_string()))?;

    Ok(BookSnapshot {
        meta: EventMeta {
            instrument,
            // A REST snapshot carries no venue timestamp at all. Our receive time
            // is the only honest value here: inventing one, or borrowing the last
            // message's, would put a number in `exchange_ts` the venue never said.
            // `ControlRecord::Gap` makes the same choice for the same reason.
            exchange_ts: local_recv_ts,
            local_recv_ts,
            ingest_seq,
        },
        last_update_id: body.last_update_id,
        bids: levels(&body.bids, "bids")?,
        asks: levels(&body.asks, "asks")?,
    })
}

/// Which side crossed the spread.
///
/// Binance reports `m`, "is the buyer the market maker", which is the inverse of
/// what we want. A resting buyer means the *seller* lifted them, so `m: true` is a
/// sell-side aggressor. Getting this backwards silently inverts order-flow
/// imbalance -- a signal that would then appear predictive with the wrong sign,
/// which is worse than one that appears not to work at all.
const fn aggressor(buyer_is_maker: bool) -> Side {
    if buyer_is_maker {
        Side::Sell
    } else {
        Side::Buy
    }
}

fn levels(raw: &[RawLevel<'_>], field: &'static str) -> Result<Vec<Level>, ParseError> {
    raw.iter()
        .map(|[px, qty]| {
            Ok(Level {
                px: number(px, field)?,
                qty: number(qty, field)?,
            })
        })
        .collect()
}

/// Venue decimal string straight to fixed-point.
///
/// Never via `f64`, per invariant 1 -- and the workspace denies float arithmetic,
/// so that is enforced by the build rather than remembered.
fn number<T: core::str::FromStr<Err = ParseFixedError>>(
    text: &str,
    field: &'static str,
) -> Result<T, ParseError> {
    text.parse().map_err(|source| ParseError::BadNumber {
        field,
        value: text.to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};

    /// Verbatim from `data/acceptance`, BTCUSDT 2026-08-21, `ingest_seq` 2 and 4.
    /// Copied out of a real capture rather than written from the venue's
    /// documentation, because the thing worth testing against is the dialect
    /// actually being spoken.
    const DEPTH: &[u8] = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1787315652514,"s":"BTCUSDT","U":98853022585,"u":98853022588,"b":[["68985.00000000","0.00160000"],["61320.00000000","0.10999000"]],"a":[]}}"#;

    const TRADE: &[u8] = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1787315652708,"s":"BTCUSDT","t":6595931385,"p":"76650.00000000","q":"0.00040000","T":1787315652707,"m":true,"M":true}}"#;

    const SNAPSHOT: &[u8] = br#"{"lastUpdateId":98853022644,"bids":[["76650.00000000","0.00174000"],["76649.99000000","0.00049000"]],"asks":[["76651.00000000","0.01000000"]]}"#;

    fn instrument() -> (InstrumentRegistry, InstrumentId) {
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

    fn parse(payload: &[u8]) -> Option<MarketEvent> {
        let (_reg, id) = instrument();
        parse_stream_message(payload, id, Ts::from_millis(1_787_315_652_600), 42).unwrap()
    }

    #[test]
    fn a_real_depth_delta_becomes_a_book_delta() {
        let Some(MarketEvent::BookDelta(d)) = parse(DEPTH) else {
            panic!("expected a book delta");
        };
        assert_eq!(d.first_update_id, 98_853_022_585);
        assert_eq!(d.final_update_id, 98_853_022_588);
        assert_eq!(d.bids.len(), 2);
        assert!(d.asks.is_empty(), "an empty side is normal, not an error");

        // Exact fixed-point, straight from the decimal string.
        assert_eq!(d.bids[0].px, "68985.00000000".parse().unwrap());
        assert_eq!(d.bids[0].qty, "0.00160000".parse().unwrap());
        assert_eq!(d.bids[0].px.raw(), 6_898_500_000_000);

        // `E`, and dispatch is still on our own clock.
        assert_eq!(d.meta.exchange_ts, Ts::from_millis(1_787_315_652_514));
        assert_eq!(d.meta.local_recv_ts, Ts::from_millis(1_787_315_652_600));
        assert_eq!(d.meta.ingest_seq, 42);
    }

    #[test]
    fn a_zero_quantity_level_is_preserved_rather_than_dropped() {
        // Zero means "remove this level" and is the venue's own convention. A
        // parser that filtered them would silently turn a removal into a
        // no-op, leaving stale liquidity in the book forever.
        let removal = br#"{"stream":"s","data":{"e":"depthUpdate","E":1,"U":1,"u":2,
            "b":[["76614.48000000","0.00000000"]],"a":[]}}"#;
        let Some(MarketEvent::BookDelta(d)) = parse(removal) else {
            panic!("expected a book delta");
        };
        assert_eq!(d.bids.len(), 1);
        assert!(d.bids[0].is_removal());
    }

    #[test]
    fn a_real_trade_becomes_a_trade_with_the_right_aggressor() {
        let Some(MarketEvent::Trade(t)) = parse(TRADE) else {
            panic!("expected a trade");
        };
        assert_eq!(t.venue_trade_id, 6_595_931_385);
        assert_eq!(t.px, "76650.00000000".parse().unwrap());
        assert_eq!(t.qty, "0.00040000".parse().unwrap());
        // `m: true` -- the buyer was the maker, so the seller crossed the spread.
        assert_eq!(t.aggressor, Side::Sell);
        assert_eq!(t.meta.exchange_ts, Ts::from_millis(1_787_315_652_708));
    }

    #[test]
    fn the_aggressor_mapping_is_the_inverse_of_the_venues_flag() {
        // Pinned in both directions, because an inverted order-flow signal looks
        // predictive rather than broken.
        assert_eq!(aggressor(true), Side::Sell);
        assert_eq!(aggressor(false), Side::Buy);
    }

    #[test]
    fn a_real_snapshot_becomes_a_book_snapshot() {
        let (_reg, id) = instrument();
        let s = parse_snapshot(SNAPSHOT, id, Ts::from_millis(1_787_315_700_000), 317).unwrap();
        assert_eq!(s.last_update_id, 98_853_022_644);
        assert_eq!(s.bids.len(), 2);
        assert_eq!(s.asks.len(), 1);
        assert_eq!(s.bids[0].px, "76650.00000000".parse().unwrap());
        assert_eq!(s.asks[0].px, "76651.00000000".parse().unwrap());
        // No venue timestamp exists on a REST body; ours is the only honest one.
        assert_eq!(s.meta.exchange_ts, s.meta.local_recv_ts);
        assert_eq!(s.meta.ingest_seq, 317);
    }

    #[test]
    fn an_unknown_event_type_is_not_an_error() {
        let kline = br#"{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"k":{}}}"#;
        assert!(parse(kline).is_none());
    }

    #[test]
    fn a_missing_field_on_an_event_we_understand_is_loud() {
        // Invariant 5. Silently defaulting would put a wrong number in a book.
        let (_reg, id) = instrument();
        for (payload, field) in [
            (
                br#"{"stream":"s","data":{"e":"depthUpdate","E":1,"u":2}}"#.as_slice(),
                "U",
            ),
            (
                br#"{"stream":"s","data":{"e":"depthUpdate","U":1,"u":2}}"#.as_slice(),
                "E",
            ),
            (
                br#"{"stream":"s","data":{"e":"trade","E":1,"t":1,"q":"1","m":true}}"#.as_slice(),
                "p",
            ),
            (
                br#"{"stream":"s","data":{"e":"trade","E":1,"t":1,"p":"1","q":"1"}}"#.as_slice(),
                "m",
            ),
        ] {
            match parse_stream_message(payload, id, Ts::EPOCH, 1) {
                Err(ParseError::MissingField { field: got, .. }) => assert_eq!(got, field),
                other => panic!("expected a missing {field}, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_malformed_price_is_refused_rather_than_rounded() {
        let (_reg, id) = instrument();
        let bad = br#"{"stream":"s","data":{"e":"depthUpdate","E":1,"U":1,"u":2,
            "b":[["not-a-price","1.0"]],"a":[]}}"#;
        match parse_stream_message(bad, id, Ts::EPOCH, 1) {
            Err(ParseError::BadNumber { field, value, .. }) => {
                assert_eq!(field, "b");
                assert_eq!(value, "not-a-price");
            }
            other => panic!("expected a bad number, got {other:?}"),
        }
    }

    #[test]
    fn both_parsers_agree_on_what_a_message_is() {
        // The drift bound. Two parsers for one dialect is a real risk, and this is
        // what keeps it a bounded one: if `sequence` and `parse` ever disagree
        // about what a depth message is, the verifier's completeness check and the
        // normalizer's reconstruction would be reading different streams.
        use crate::sequence::{classify, StreamMessage};

        let (_reg, id) = instrument();
        for payload in [DEPTH, TRADE] {
            let cheap = classify(payload).unwrap();
            let full = parse_stream_message(payload, id, Ts::EPOCH, 1)
                .unwrap()
                .expect("both parsers should recognise this");
            match (cheap, full) {
                (
                    StreamMessage::Depth {
                        first_update_id,
                        final_update_id,
                    },
                    MarketEvent::BookDelta(d),
                ) => {
                    assert_eq!(first_update_id, d.first_update_id);
                    assert_eq!(final_update_id, d.final_update_id);
                }
                (StreamMessage::Trade { venue_trade_id }, MarketEvent::Trade(t)) => {
                    assert_eq!(venue_trade_id, t.venue_trade_id);
                }
                (c, f) => panic!("parsers disagree: {c:?} vs {f:?}"),
            }
        }
    }
}
