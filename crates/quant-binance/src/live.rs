//! The live leg of the three worlds: a venue socket, as an [`EventSource`].
//!
//! Reads [`CaptureRecord`]s off the channel a [`TeeSink`](quant_recorder::TeeSink)
//! feeds and hands the engine [`MarketEvent`]s. The recorder does the connecting,
//! the stamping and the sequencing exactly as it does when nobody is trading;
//! this is only the last step, and deliberately so — a second WebSocket
//! subscription for the engine would stamp the same messages at different
//! instants and make `docs/engine-contract.md` §9's agreement criterion
//! unachievable.
//!
//! # Why blocking is right here
//!
//! [`EventSource::next_event`] blocks until a record arrives. That is not a
//! compromise: the engine clock *is* the event stream, in all three worlds, so a
//! quiet market means no time passes for the strategy. A live source that woke
//! the engine on a timer would give it a notion of time the backtest does not
//! have, and the same strategy would then behave differently in the two — which
//! is the one property the whole seam exists to protect.
//!
//! `None` means the channel closed: the recorder is gone. That is the end of the
//! stream and not an error, because a recorder that has stopped has already said
//! why in its own logs.
//!
//! # Why a dropped record becomes a gap
//!
//! The tee drops the engine's copy rather than the capture's when the engine
//! falls behind, so a busy minute can leave a hole in *this* consumer's view of
//! `ingest_seq` while the archive stays complete. The hole is the evidence —
//! exactly as it is for `quant-verify` reading files and `quant-normalize`
//! replaying them — and the response is the same: a `Gap`, which clears the book,
//! which stops the strategy trading until a snapshot re-anchors it.
//!
//! `GapCause::LocalOverflow` is the honest cause. It means *this consumer*
//! overflowed, not that the recorder did; the capture for the same instant will
//! show no gap at all, and that difference is real information rather than a
//! contradiction.
//!
//! # Why a parse failure is also a gap
//!
//! Invariant 5 says parse failures are loud, and they are: counted, and the
//! first is logged. But a two-week paper run must not die because one message
//! was malformed — and more importantly, a message we could not parse is a
//! message whose effect on the book we do not know. That is blindness, and
//! blindness has a representation already.

use std::sync::mpsc::Receiver;

use quant_core::event::{Gap, GapCause, MarketEvent};
use quant_core::instrument::InstrumentId;
use quant_core::source::{EventSource, SourceError};
use quant_core::time::Ts;
use quant_recorder::record::CaptureRecord;

use crate::decode::{decode, VenueBytes};

/// What the live source saw.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LiveStats {
    pub records: u64,
    pub events: u64,
    /// Records recognised as a message type we do not model.
    pub ignored: u64,
    /// Holes in this consumer's view of `ingest_seq`, each turned into a gap.
    pub holes: u64,
    /// Records lost to those holes.
    pub records_missed: u64,
    /// Payloads that would not parse. Each becomes a gap.
    pub parse_failures: u64,
    /// Gaps handed to the engine, from every cause including the recorder's own.
    pub gaps_emitted: u64,
}

/// A venue stream, as the engine sees it.
#[derive(Debug)]
pub struct LiveSource {
    records: Receiver<CaptureRecord>,
    instrument: InstrumentId,
    last_seq: Option<u64>,
    /// A single event can produce a synthesized gap *and* a market event, in
    /// that order, so there has to be somewhere to keep the second.
    pending: std::collections::VecDeque<MarketEvent>,
    stats: LiveStats,
    logged_parse_failure: bool,
}

impl LiveSource {
    /// Read from the channel a `TeeSink`'s secondary half feeds.
    #[must_use]
    pub fn new(records: Receiver<CaptureRecord>, instrument: InstrumentId) -> Self {
        Self {
            records,
            instrument,
            last_seq: None,
            pending: std::collections::VecDeque::new(),
            stats: LiveStats::default(),
            logged_parse_failure: false,
        }
    }

    #[must_use]
    pub const fn stats(&self) -> LiveStats {
        self.stats
    }

    /// Hand the engine a gap, so it clears its book.
    fn emit_gap(&mut self, cause: GapCause, at: Ts) {
        self.stats.gaps_emitted += 1;
        self.pending.push_back(MarketEvent::Gap(Gap {
            meta: quant_core::event::EventMeta {
                instrument: self.instrument,
                // A synthesized gap has no venue timestamp, so our own receive
                // time stands in -- the same choice `ControlRecord::Gap` makes.
                exchange_ts: at,
                local_recv_ts: at,
                // Deliberately *not* a fresh sequence number. This gap is our
                // account of a record we never saw, and giving it an id would
                // claim a position in a sequence the venue defines.
                ingest_seq: self.last_seq.unwrap_or(0),
            },
            cause,
            last_good_ts: at,
        }));
    }

    /// Check continuity, then decode.
    fn take(&mut self, record: &CaptureRecord) {
        self.stats.records += 1;
        let seq = record.ingest_seq();
        let at = record.local_recv_ts();

        if let Some(last) = self.last_seq {
            let expected = last.saturating_add(1);
            if seq != expected {
                self.stats.holes += 1;
                self.stats.records_missed += seq.saturating_sub(expected);
                self.emit_gap(GapCause::LocalOverflow, at);
            }
        }
        self.last_seq = Some(seq);

        let decoded = match record {
            CaptureRecord::Venue { payload, .. } => {
                decode(VenueBytes::Stream, payload, self.instrument, at, seq)
                    .map_err(|e| e.to_string())
            }
            CaptureRecord::Snapshot { payload, .. } => {
                decode(VenueBytes::Snapshot, payload, self.instrument, at, seq)
                    .map_err(|e| e.to_string())
            }
            // The recorder's own account of blindness, and venue-agnostic:
            // `quant-storage` owns the mapping and every reader shares it.
            CaptureRecord::Control { record, .. } => {
                Ok(record.into_market_event(self.instrument, at, seq))
            }
        };

        match decoded {
            Ok(Some(event)) => {
                self.stats.events += 1;
                if matches!(event, MarketEvent::Gap(_)) {
                    self.stats.gaps_emitted += 1;
                }
                self.pending.push_back(event);
            }
            Ok(None) => self.stats.ignored += 1,
            Err(e) => {
                self.stats.parse_failures += 1;
                if !self.logged_parse_failure {
                    self.logged_parse_failure = true;
                    tracing::error!(
                        ingest_seq = seq,
                        error = %e,
                        "a recorded payload would not parse; treating it as blindness. \
                         The bytes are in the capture -- re-derive to see them."
                    );
                }
                // We do not know what this message did to the book, so we are
                // blind. `SequenceGap` and not `LocalOverflow`: nothing was
                // dropped, we simply cannot read what arrived.
                self.emit_gap(GapCause::SequenceGap, at);
            }
        }
    }
}

impl EventSource for LiveSource {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Some(Ok(event));
            }
            // Blocking, on purpose: see the module docs.
            let record = self.records.recv().ok()?;
            self.take(&record);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
    use quant_storage::ControlRecord;

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

    const TRADE: &[u8] = br#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1787315652708,"s":"BTCUSDT","t":6595931385,"p":"76650.00000000","q":"0.00040000","T":1787315652707,"m":true,"M":true}}"#;
    const KLINE: &[u8] = br#"{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1,"k":{}}}"#;

    fn venue(seq: u64, payload: &[u8]) -> CaptureRecord {
        CaptureRecord::Venue {
            local_recv_ts: Ts::from_nanos(i64::try_from(seq).expect("small")),
            ingest_seq: seq,
            payload: payload.to_vec(),
        }
    }

    /// Feed a fixed script and drain everything the source produces.
    fn drain(records: Vec<CaptureRecord>) -> (Vec<MarketEvent>, LiveStats) {
        let (tx, rx) = std::sync::mpsc::sync_channel(64);
        for record in records {
            tx.try_send(record).expect("room");
        }
        drop(tx);
        let mut source = LiveSource::new(rx, instrument());
        let mut out = Vec::new();
        while let Some(item) = source.next_event() {
            out.push(item.expect("the script cannot fail"));
        }
        (out, source.stats())
    }

    #[test]
    fn a_closed_channel_is_the_end_of_the_stream_not_an_error() {
        // A recorder that has stopped has already said why in its own logs.
        let (events, stats) = drain(Vec::new());
        assert!(events.is_empty());
        assert_eq!(stats.records, 0);
    }

    #[test]
    fn stream_records_become_events() {
        let (events, stats) = drain(vec![venue(1, TRADE), venue(2, TRADE)]);
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| matches!(e, MarketEvent::Trade(_))));
        assert_eq!(stats.events, 2);
        assert_eq!(stats.holes, 0);
    }

    #[test]
    fn an_unmodelled_message_type_is_ignored_rather_than_failing() {
        // The parsers are strict about what they claim and tolerant of the rest,
        // so a new Binance message type cannot stop a paper run.
        let (events, stats) = drain(vec![venue(1, KLINE)]);
        assert!(events.is_empty());
        assert_eq!(stats.ignored, 1);
        assert_eq!(stats.parse_failures, 0);
    }

    #[test]
    fn a_hole_in_this_consumers_view_becomes_a_gap() {
        // The tee drops the engine's copy, not the capture's, so a hole here can
        // exist while the archive is complete. Same evidence as offline, same
        // remedy.
        let (events, stats) = drain(vec![venue(1, TRADE), venue(5, TRADE)]);
        assert_eq!(stats.holes, 1);
        assert_eq!(stats.records_missed, 3, "seq 2, 3 and 4");
        assert!(
            matches!(
                events.get(1),
                Some(MarketEvent::Gap(g)) if g.cause == GapCause::LocalOverflow
            ),
            "the gap comes before the event that revealed it: {events:?}"
        );
        assert!(matches!(events.get(2), Some(MarketEvent::Trade(_))));
    }

    #[test]
    fn the_first_record_of_a_run_is_not_a_hole() {
        // There is nothing before it to be discontinuous with, and a spurious
        // gap on startup would blind the strategy for its first snapshot.
        let (_events, stats) = drain(vec![venue(9_000, TRADE)]);
        assert_eq!(stats.holes, 0);
    }

    #[test]
    fn a_payload_that_will_not_parse_is_treated_as_blindness() {
        // We do not know what it did to the book, so we are blind -- and
        // blindness already has a representation. `SequenceGap`, not
        // `LocalOverflow`: nothing was dropped, we simply cannot read it.
        let (events, stats) = drain(vec![venue(1, br#"{"stream":"s","data":{"e":"trade","E":1,"t":1,"p":"not-a-price","q":"1","m":true}}"#)]);
        assert_eq!(stats.parse_failures, 1);
        assert!(matches!(
            events.first(),
            Some(MarketEvent::Gap(g)) if g.cause == GapCause::SequenceGap
        ));
    }

    #[test]
    fn the_recorders_own_gap_records_reach_the_engine() {
        // Invariant 3. A source that filtered them would make a disconnect
        // indistinguishable from a quiet market.
        let control = CaptureRecord::Control {
            local_recv_ts: Ts::from_nanos(2),
            ingest_seq: 2,
            record: ControlRecord::Gap {
                cause: GapCause::Disconnect,
                exchange_ts: Ts::from_nanos(2),
                last_good_ts: Ts::from_nanos(1),
            },
        };
        let (events, stats) = drain(vec![venue(1, TRADE), control]);
        assert!(matches!(
            events.get(1),
            Some(MarketEvent::Gap(g)) if g.cause == GapCause::Disconnect
        ));
        assert_eq!(stats.gaps_emitted, 1);
    }

    #[test]
    fn a_synthesized_gap_does_not_claim_a_sequence_number_of_its_own() {
        // It is our account of a record we never saw. Minting an id for it would
        // claim a position in a sequence the venue defines.
        let (events, _stats) = drain(vec![venue(1, TRADE), venue(4, TRADE)]);
        let MarketEvent::Gap(gap) = &events[1] else {
            panic!("expected a gap: {events:?}");
        };
        assert_eq!(gap.meta.ingest_seq, 1, "the last one we actually saw");
    }
}
