//! The venue's own sequence numbers, pulled out of a recorded payload.
//!
//! # Why the recorder's crate parses payloads at all
//!
//! It does not, on the capture path -- that is the whole bargain of the raw tier.
//! This module exists for the *offline* verifier, and it lives here because
//! "which fields carry this venue's sequence numbers, and what does contiguity
//! mean for them" is Binance knowledge, and Binance knowledge belongs in the
//! Binance crate. A verifier that knew about `U` and `u` would be a second place
//! the venue's dialect is encoded.
//!
//! # Why it is not the normalizer
//!
//! It reads four fields and ignores everything else. It does not touch prices or
//! quantities, so it needs no fixed-point conversion and cannot introduce the
//! rounding bugs that make a normalizer worth re-deriving. The full parse arrives
//! with M2; this is the narrow slice that answers "did we receive every message
//! the venue sent", which is a *completeness* question about the capture rather
//! than a *correctness* question about the book.
//!
//! # Strictness
//!
//! Strict about the fields it claims to understand, tolerant about the rest. A
//! `depthUpdate` without `U` and `u` is a hard error, because our model of the
//! venue would be wrong and the chain check would silently pass by skipping it. A
//! payload whose event type we do not recognize is [`StreamMessage::Other`],
//! because a stream we have not taught it about is not a defect in the capture.

use core::fmt;

/// Binance's event-type discriminator, in the `e` field of a stream payload.
const DEPTH_EVENT: &str = "depthUpdate";
const TRADE_EVENT: &str = "trade";

/// What a recorded stream payload turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMessage {
    /// An incremental depth update, carrying the venue update-id range it covers.
    ///
    /// The contiguity rule that makes the depth stream usable: this message's
    /// `first_update_id` must be the previous message's `final_update_id` plus
    /// one. A break means messages were missed and the book is wrong from there.
    Depth {
        first_update_id: u64,
        final_update_id: u64,
    },
    /// A trade print. The id is what lets a reconnect be de-duplicated.
    Trade { venue_trade_id: u64 },
    /// A subscribed stream this build does not interpret.
    ///
    /// Carries nothing deliberately: the verifier has no checks for it, and
    /// inventing fields nothing reads is how a narrow parser stops being narrow.
    Other,
}

/// Why a recorded payload could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceError {
    /// Not the shape the combined-stream endpoint produces.
    NotAStreamMessage(String),
    /// The event type is one we claim to understand, and a field it must have is
    /// missing. Loud on purpose: see the module docs.
    MissingField {
        event: &'static str,
        field: &'static str,
    },
    /// Not the shape a depth snapshot response has.
    NotASnapshot(String),
}

impl fmt::Display for SequenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAStreamMessage(e) => write!(f, "not a combined-stream message: {e}"),
            Self::MissingField { event, field } => {
                write!(f, "{event} payload has no `{field}` field")
            }
            Self::NotASnapshot(e) => write!(f, "not a depth snapshot: {e}"),
        }
    }
}

impl std::error::Error for SequenceError {}

/// The combined-stream wrapper.
///
/// `stream` is deliberately not read. Requiring `data` already proves the payload
/// came from the `/stream?streams=` endpoint, and the event type inside is a more
/// direct statement of the payload's *shape* than the stream name we happened to
/// subscribe to.
#[derive(serde::Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    data: Payload<'a>,
}

/// Only the sequencing fields. Everything else in the payload is ignored, which
/// is the point -- unknown fields are not an error, they are the normalizer's
/// business.
#[derive(serde::Deserialize)]
struct Payload<'a> {
    #[serde(rename = "e", borrow)]
    event: Option<&'a str>,
    #[serde(rename = "E")]
    event_time_millis: Option<i64>,
    #[serde(rename = "U")]
    first_update_id: Option<u64>,
    #[serde(rename = "u")]
    final_update_id: Option<u64>,
    #[serde(rename = "t")]
    trade_id: Option<u64>,
}

/// The venue's own timestamp on a stream message, in epoch milliseconds.
///
/// This is the `exchange_ts` half of the two-timestamp rule, and subtracting it
/// from `local_recv_ts` gives the observed venue latency that
/// `docs/data-contract.md` §7 asks the recorder to report percentiles for.
///
/// `None` rather than an error for anything unrecognized: latency is an
/// observation, and a message we cannot read the timestamp out of should cost us
/// a sample, never a recorded byte. That is the difference between this and
/// [`classify`], which is loud because a chain check that silently skips messages
/// would pass a capture with holes in it.
#[must_use]
pub fn event_time_millis(payload: &[u8]) -> Option<i64> {
    serde_json::from_slice::<Envelope<'_>>(payload)
        .ok()?
        .data
        .event_time_millis
}

/// Classify one recorded [`quant_storage::FrameKind::VenuePayload`].
pub fn classify(payload: &[u8]) -> Result<StreamMessage, SequenceError> {
    let envelope: Envelope<'_> = serde_json::from_slice(payload)
        .map_err(|e| SequenceError::NotAStreamMessage(e.to_string()))?;
    let data = envelope.data;

    match data.event {
        Some(DEPTH_EVENT) => Ok(StreamMessage::Depth {
            first_update_id: data.first_update_id.ok_or(SequenceError::MissingField {
                event: DEPTH_EVENT,
                field: "U",
            })?,
            final_update_id: data.final_update_id.ok_or(SequenceError::MissingField {
                event: DEPTH_EVENT,
                field: "u",
            })?,
        }),
        Some(TRADE_EVENT) => Ok(StreamMessage::Trade {
            venue_trade_id: data.trade_id.ok_or(SequenceError::MissingField {
                event: TRADE_EVENT,
                field: "t",
            })?,
        }),
        _ => Ok(StreamMessage::Other),
    }
}

#[derive(serde::Deserialize)]
struct Snapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
}

/// The update id a recorded [`quant_storage::FrameKind::VenueSnapshot`] is
/// current as of.
///
/// Reading it is also the only cheap proof that the frame holds a book rather
/// than an error document: the fetch checks the HTTP status, but "the venue
/// answered 200" and "these bytes are a book" are different claims, and this is
/// the one that matters a year later.
pub fn snapshot_last_update_id(payload: &[u8]) -> Result<u64, SequenceError> {
    serde_json::from_slice::<Snapshot>(payload)
        .map(|s| s.last_update_id)
        .map_err(|e| SequenceError::NotASnapshot(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trimmed to the fields under test plus a couple that must be ignored.
    const DEPTH: &[u8] = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate",
        "E":1769000000000,"s":"BTCUSDT","U":97993932468,"u":97993932471,
        "b":[["64151.67000000","0.31700000"]],"a":[]}}"#;

    const TRADE: &[u8] = br#"{"stream":"btcusdt@trade","data":{"e":"trade",
        "E":1769000000000,"s":"BTCUSDT","t":4321987,"p":"64151.67000000",
        "q":"0.00100000","T":1769000000000,"m":false,"M":true}}"#;

    #[test]
    fn a_depth_delta_yields_the_chain_it_covers() {
        assert_eq!(
            classify(DEPTH).unwrap(),
            StreamMessage::Depth {
                first_update_id: 97_993_932_468,
                final_update_id: 97_993_932_471,
            }
        );
    }

    #[test]
    fn a_trade_yields_its_venue_id() {
        assert_eq!(
            classify(TRADE).unwrap(),
            StreamMessage::Trade {
                venue_trade_id: 4_321_987
            }
        );
    }

    #[test]
    fn unknown_fields_are_ignored_rather_than_refused() {
        // The whole reason this parser is narrow: Binance adding a field must not
        // make a recorded file unverifiable.
        let with_novelty = br#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate",
            "U":1,"u":2,"somethingNew":{"nested":[1,2,3]}},"extraTopLevel":7}"#;
        assert_eq!(
            classify(with_novelty).unwrap(),
            StreamMessage::Depth {
                first_update_id: 1,
                final_update_id: 2
            }
        );
    }

    #[test]
    fn an_unknown_event_type_is_other_not_an_error() {
        // A stream we have not taught this about is not a defect in the capture.
        let other = br#"{"stream":"btcusdt@kline_1m","data":{"e":"kline","k":{}}}"#;
        assert_eq!(classify(other).unwrap(), StreamMessage::Other);

        let no_event = br#"{"stream":"x","data":{"whatever":1}}"#;
        assert_eq!(classify(no_event).unwrap(), StreamMessage::Other);
    }

    #[test]
    fn a_depth_delta_missing_its_ids_is_loud() {
        // Silently returning `Other` here would make the chain check pass by
        // skipping the very messages it exists to check.
        let no_u = br#"{"stream":"s","data":{"e":"depthUpdate","U":5}}"#;
        assert_eq!(
            classify(no_u),
            Err(SequenceError::MissingField {
                event: DEPTH_EVENT,
                field: "u"
            })
        );
        let no_first = br#"{"stream":"s","data":{"e":"depthUpdate","u":5}}"#;
        assert!(matches!(
            classify(no_first),
            Err(SequenceError::MissingField { field: "U", .. })
        ));
    }

    #[test]
    fn a_payload_without_the_stream_wrapper_is_refused() {
        // Every recorded stream frame comes from `/stream?streams=`, so a bare
        // payload means either a format change or the wrong bytes in the frame.
        assert!(matches!(
            classify(br#"{"e":"depthUpdate","U":1,"u":2}"#),
            Err(SequenceError::NotAStreamMessage(_))
        ));
        assert!(matches!(
            classify(b"not json at all"),
            Err(SequenceError::NotAStreamMessage(_))
        ));
    }

    #[test]
    fn the_venue_timestamp_comes_out_of_a_stream_message() {
        assert_eq!(event_time_millis(DEPTH), Some(1_769_000_000_000));
        assert_eq!(event_time_millis(TRADE), Some(1_769_000_000_000));
    }

    #[test]
    fn an_unreadable_payload_costs_a_latency_sample_and_nothing_else() {
        // Deliberately quiet, unlike `classify`. A metric we cannot compute is a
        // missing data point; a chain check that skips messages is a lie.
        assert_eq!(event_time_millis(b"not json"), None);
        assert_eq!(event_time_millis(br#"{"stream":"s","data":{}}"#), None);
    }

    #[test]
    fn a_snapshot_yields_the_update_id_it_is_current_as_of() {
        let body = br#"{"lastUpdateId":97993932468,"bids":[["64151.67","0.317"]],"asks":[]}"#;
        assert_eq!(snapshot_last_update_id(body).unwrap(), 97_993_932_468);
    }

    #[test]
    fn an_error_document_is_not_mistaken_for_a_snapshot() {
        // The case this check exists for: a venue answering 200 with a body that
        // is not a book. Recorded as a snapshot, it would corrupt a rebuild
        // silently a year from now.
        let rate_limited = br#"{"code":-1003,"msg":"Too much request weight used"}"#;
        assert!(matches!(
            snapshot_last_update_id(rate_limited),
            Err(SequenceError::NotASnapshot(_))
        ));
    }
}
