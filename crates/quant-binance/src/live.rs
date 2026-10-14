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
//! # Two ways to wait, and the engine uses the one that gives up
//!
//! [`EventSource::next_event`] blocks until a record arrives.
//! [`EventSource::next_event_timeout`] waits no longer than it is told and then
//! returns [`Wake::Idle`], and that is the one `Engine::run` calls.
//!
//! This module used to argue that blocking was right, and while the engine had
//! one input it was. M8 gives it a second. A live venue's reports arrive on
//! their own socket, on their own schedule, and an engine parked on this channel
//! while the market socket is stalled -- which Binance's own 24-hour stream
//! rotation makes routine -- cannot book a fill, cannot tell the strategy, and
//! cannot move the risk layer's tally. M8.a taught the engine to stop waiting,
//! but this source never let it: the trait's default calls `next_event`, so it
//! can never idle, and until this override the default is what the engine got.
//!
//! What the old argument protected still holds, because an idle wake is not a
//! timer the strategy can see. The engine clock *is* the input stream, so a
//! quiet market must not let time pass for the strategy, and `Wake::Idle`
//! carries no time at all. The engine uses it only to ask the venue whether it
//! has anything to say. If not, it goes back to waiting without calling the
//! strategy; if so, the clock moves to the report's own stamp when that is
//! later, and never to the wall clock. Against `SimulatedVenue` an idle wake
//! finds nothing, ever: the simulator makes reports only inside `observe`, the
//! engine polls it straight afterwards, and an idle wake polls again at a clock
//! nothing has moved since, so nothing new can be due. Paper therefore makes
//! the same strategy calls a backtest over the same data would.
//!
//! # The timeout bounds the wait, not the silence
//!
//! One deadline per call, and each `recv_timeout` gets only what is left of it.
//! Restarting the full timeout per record would bound how long the channel stays
//! *silent* rather than how long the call waits: a record that yields nothing --
//! a message type we do not model -- would buy the market another full wait, and
//! a steady trickle of them would keep the engine from ever looking at the
//! venue. That is M1's flush bug in a new place, the block that never sealed
//! because a steady trickle never went quiet.
//!
//! The deadline is the only use of the wall clock in this module, and it does
//! not breach invariant 4. The invariant exists so that a component can be
//! backtested; this one is live by definition, and the `recv_timeout` it bounds
//! measures wall time already. Nothing read from the clock reaches an event or
//! the engine's clock -- it only decides when to stop waiting.
//!
//! A record already queued is taken even once the deadline has passed, because
//! `recv_timeout` tries the channel before it looks at the time. So `Idle` means
//! the channel was found empty, never that a backlog went unread -- which
//! matters, because a wake the engine spends on the venue while market records
//! sit unread would act on the two inputs out of the order they arrived.
//!
//! `None` means the channel closed: the recorder is gone. That is the end of the
//! stream and not an error, because a recorder that has stopped has already said
//! why in its own logs. Records sent before it closed are delivered first.
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

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use quant_core::event::{Gap, GapCause, MarketEvent};
use quant_core::instrument::InstrumentId;
use quant_core::source::{EventSource, SourceError, Wake};
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
            // Blocking, on purpose: a caller that asks for the next event and
            // nothing else is asking to wait for it. The engine asks
            // `next_event_timeout` instead; see the module docs.
            let record = self.records.recv().ok()?;
            self.take(&record);
        }
    }

    fn next_event_timeout(&mut self, timeout: Duration) -> Option<Result<Wake, SourceError>> {
        // Taken once, so a record that yields nothing spends the wait rather
        // than restarting it. `None` only for a timeout too long to be an
        // instant, which `recv_timeout` itself treats as waiting for ever -- so
        // handing it the whole timeout again is the same answer.
        let deadline = Instant::now().checked_add(timeout);
        loop {
            // Whatever the last record produced comes out before anything is
            // waited for. A hole and the record that revealed it are a gap *and*
            // an event from one receive, and the event must not be left waiting
            // behind a record that may be minutes away.
            if let Some(event) = self.pending.pop_front() {
                return Some(Ok(Wake::Event(event)));
            }
            let left = deadline.map_or(timeout, |d| d.saturating_duration_since(Instant::now()));
            match self.records.recv_timeout(left) {
                Ok(record) => self.take(&record),
                Err(RecvTimeoutError::Timeout) => return Some(Ok(Wake::Idle)),
                Err(RecvTimeoutError::Disconnected) => return None,
            }
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
        assert!(events.is_empty(), "{events:?}");
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
        assert!(events.is_empty(), "{events:?}");
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

    /// Short, because every wait in these tests is meant to expire.
    const TICK: Duration = Duration::from_millis(50);

    /// Make one call on a thread of its own, so that a source which blocks
    /// fails the test instead of hanging the suite.
    fn bounded<T: Send + 'static>(
        mut source: LiveSource,
        call: impl FnOnce(&mut LiveSource) -> T + Send + 'static,
    ) -> (T, LiveSource) {
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let out = call(&mut source);
            // The receiver is gone only if the test has already failed.
            let _ = done.send((out, source));
        });
        finished
            .recv_timeout(Duration::from_secs(5))
            .expect("the call blocked instead of returning")
    }

    #[test]
    fn a_quiet_channel_idles_instead_of_blocking() {
        // What the override is for. An engine parked on a stalled market socket
        // cannot hear the venue, and the trait's default calls `next_event`,
        // which blocks -- so without the override the first call here never
        // returns, and `bounded` turns that into a failure.
        let (tx, rx) = std::sync::mpsc::sync_channel(8);
        let source = LiveSource::new(rx, instrument());

        let (woke, source) = bounded(source, |s| s.next_event_timeout(TICK));
        assert_eq!(
            woke,
            Some(Ok(Wake::Idle)),
            "nothing arrived, so nothing to say"
        );

        // An idle wake is not an ending: what arrives afterwards still comes
        // out. Closed before the call, so the record has to outlive its channel.
        tx.try_send(venue(1, TRADE)).expect("room");
        drop(tx);
        let (woke, source) = bounded(source, |s| s.next_event_timeout(TICK));
        assert!(
            matches!(&woke, Some(Ok(Wake::Event(MarketEvent::Trade(t)))) if t.meta.ingest_seq == 1),
            "the record sent after the idle wake: {woke:?}"
        );

        let (woke, _source) = bounded(source, |s| s.next_event_timeout(TICK));
        assert_eq!(
            woke, None,
            "a closed channel is the end, not a quiet market"
        );
    }

    #[test]
    fn everything_one_record_yields_comes_out_before_the_source_waits_again() {
        // A hole is found on the record after it, so that record yields a gap
        // *and* its own event. The event is owed before the source goes back to
        // the channel: waiting first would hold it behind a record that may be
        // minutes away, and on a quiet market report an idle wake while a trade
        // sat undelivered.
        let (tx, rx) = std::sync::mpsc::sync_channel(8);
        tx.try_send(venue(1, TRADE)).expect("room");
        tx.try_send(venue(5, TRADE)).expect("room");
        let mut source = LiveSource::new(rx, instrument());

        let woke: Vec<_> = (0..4).map(|_| source.next_event_timeout(TICK)).collect();
        assert!(
            matches!(&woke[0], Some(Ok(Wake::Event(MarketEvent::Trade(t)))) if t.meta.ingest_seq == 1),
            "{woke:?}"
        );
        assert!(
            matches!(
                &woke[1],
                Some(Ok(Wake::Event(MarketEvent::Gap(g)))) if g.cause == GapCause::LocalOverflow
            ),
            "the hole first: {woke:?}"
        );
        assert!(
            matches!(&woke[2], Some(Ok(Wake::Event(MarketEvent::Trade(t)))) if t.meta.ingest_seq == 5),
            "then the event that revealed it, without waiting for another record: {woke:?}"
        );
        assert_eq!(woke[3], Some(Ok(Wake::Idle)), "and only then nothing");
        // Held open until here, so running dry reads as quiet rather than closed.
        drop(tx);
    }

    #[test]
    fn a_record_that_yields_nothing_does_not_restart_the_wait() {
        // A message type we do not model is consumed and produces nothing. Were
        // each one to restart the timeout, a steady trickle of them would hold
        // the engine on the market socket for as long as the trickle lasted, and
        // the venue would go unheard -- M1's block that never sealed.
        let wait = Duration::from_millis(400);
        let (tx, rx) = std::sync::mpsc::sync_channel(64);
        let mut source = LiveSource::new(rx, instrument());
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let feeder = {
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                // Every 50 ms, well inside the wait, for three times as long.
                for seq in 1..=24 {
                    if stop.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                    tx.send(venue(seq, KLINE))
                        .expect("the source is still listening");
                    std::thread::sleep(Duration::from_millis(50));
                }
                // Handed back rather than dropped, so the channel stays open and
                // a wait that restarted ends late and idle rather than closed.
                tx
            })
        };

        let started = Instant::now();
        let woke = source.next_event_timeout(wait);
        let took = started.elapsed();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _tx = feeder.join().expect("the feeder cannot panic");

        assert_eq!(woke, Some(Ok(Wake::Idle)));
        assert!(
            source.stats().ignored >= 2,
            "the trickle has to arrive during the wait, or this proves nothing: {:?}",
            source.stats()
        );
        assert!(
            took < Duration::from_millis(1_000),
            "waited {took:?} on a timeout of {wait:?}"
        );
    }

    #[test]
    fn a_record_already_queued_is_read_even_with_no_time_left() {
        // `Idle` has to mean the channel was empty. An idle wake while market
        // records sat unread would send the engine to the venue ahead of market
        // events that reached us first -- the two inputs out of order.
        let (tx, rx) = std::sync::mpsc::sync_channel(8);
        tx.try_send(venue(1, TRADE)).expect("room");
        let mut source = LiveSource::new(rx, instrument());
        let woke = source.next_event_timeout(Duration::ZERO);
        assert!(
            matches!(&woke, Some(Ok(Wake::Event(MarketEvent::Trade(_))))),
            "a queued record, even with no time to wait: {woke:?}"
        );
        assert_eq!(
            source.next_event_timeout(Duration::ZERO),
            Some(Ok(Wake::Idle)),
            "and with nothing queued, no time means idle at once"
        );
        drop(tx);
    }
}
