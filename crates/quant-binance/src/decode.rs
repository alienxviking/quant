//! Which parser recorded bytes belong to.
//!
//! A six-line mapping, extracted because it was about to exist twice: once in
//! `quant-normalize`'s replay, reading frames off disk, and once in
//! [`crate::live`], reading records off a channel. Two copies of "a
//! `VenueSnapshot` goes to `parse_snapshot`" would be a small duplication with a
//! large failure mode — a snapshot decoded as a delta corrupts a book silently,
//! which is the exact reason `FrameKind` was made explicit at M1.d1.
//!
//! It lives here rather than above because *this* is the venue-specific part.
//! The caller keeps the dispatch on `Exchange`, and control records are decoded
//! by `quant-storage`, which is venue-agnostic and already shared.
//!
//! Twice in this project I have reimplemented something `quant-core` already
//! had. This is the same lesson applied before the fact rather than after.

use quant_core::event::MarketEvent;
use quant_core::instrument::InstrumentId;
use quant_core::time::Ts;

use crate::parse::{parse_snapshot, parse_stream_message, ParseError};

/// Which kind of venue bytes these are.
///
/// An enum rather than a boolean because `decode(true, ..)` at a call site says
/// nothing, and this is a call site where being wrong is silent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueBytes {
    /// A message from the subscribed stream.
    Stream,
    /// A book snapshot the venue returned to a request of ours.
    Snapshot,
}

/// Turn recorded venue bytes into an event.
///
/// `Ok(None)` for a stream message of a type we do not model — a `kline`, say.
/// Not a failure: the parsers are strict about the fields they claim and
/// tolerant of everything else, so a new Binance message type cannot make a
/// recording unreadable.
pub fn decode(
    what: VenueBytes,
    payload: &[u8],
    instrument: InstrumentId,
    local_recv_ts: Ts,
    ingest_seq: u64,
) -> Result<Option<MarketEvent>, ParseError> {
    match what {
        VenueBytes::Stream => parse_stream_message(payload, instrument, local_recv_ts, ingest_seq),
        VenueBytes::Snapshot => parse_snapshot(payload, instrument, local_recv_ts, ingest_seq)
            .map(|snapshot| Some(MarketEvent::BookSnapshot(snapshot))),
    }
}
