//! Stamping, sequencing, and what to do when we cannot keep up.
//!
//! Everything in this module is a state machine over two inputs -- "a message
//! arrived" and "a gap happened" -- with no I/O, no async and no clock of its
//! own. That is what makes the overload behaviour, which is otherwise only
//! reachable by actually overloading a live recorder, an ordinary unit test.

use core::fmt;
use std::collections::VecDeque;
use std::sync::Arc;

use quant_core::event::GapCause;
use quant_core::time::{Clock, Ts};
use quant_storage::{ControlRecord, SnapshotFailure, SnapshotPurpose};

use crate::metrics::{Metrics, MetricsSample};
use crate::record::CaptureRecord;
use crate::sink::{RecordSink, SinkError};

/// How many control records may be held pending before we start counting losses.
///
/// Reaching this would require gap *events* to outpace the writer, which means
/// dozens of disconnects with no intervening drain. It is a backstop against
/// unbounded growth in a pathological case, not a limit anything normal
/// approaches -- consecutive overflow drops coalesce into one record, and the
/// events that generate the others are ones where inflow has stopped.
///
/// Snapshot-failure records share this budget. They are rarer still, and losing
/// one has the same shape of consequence as losing a gap record: the file stops
/// accounting for something it should account for.
pub const MAX_PENDING_GAPS: usize = 64;

/// The writer is gone, so nothing we record can reach disk.
///
/// Fatal by design. Carrying on would mean a read loop dutifully draining a
/// socket into nowhere, producing a capture that ends without explanation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterGone;

impl fmt::Display for WriterGone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("capture writer is gone; nothing can reach disk")
    }
}

impl std::error::Error for WriterGone {}

/// What happened to a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accepted {
    Enqueued,
    /// Dropped because the channel was full. A `LocalOverflow` gap is now
    /// pending, and this message's sequence number is a hole.
    Dropped,
}

/// What happened to a gap record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapOutcome {
    Recorded,
    /// Held because the channel was full; it will be retried by
    /// [`Ingress::pump`] and by the next [`Ingress::accept`]. No sequence number
    /// has been consumed for it yet.
    Deferred,
}

/// Ingress counters, read out of the shared [`Metrics`] at a moment in time.
///
/// A snapshot rather than the storage itself: the numbers live in atomics because
/// a reporter on another task has to read them, and a value type is what the
/// session-close row and the shutdown summary actually want.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngressStats {
    /// Stream messages that arrived from the venue.
    pub messages: u64,
    /// Venue stream bytes that arrived, before framing or compression.
    pub bytes: u64,
    pub enqueued: u64,
    /// Messages we dropped because we could not keep up. The number that
    /// matters; see the crate docs.
    pub dropped: u64,
    /// Gap records written. Counts gaps *only* -- a snapshot failure is not a gap
    /// and reporting them together would overstate how blind we were.
    pub gaps_recorded: u64,
    /// [`ControlRecord::SnapshotFailed`] records written.
    pub snapshot_failures: u64,
    /// Control records lost to [`MAX_PENDING_GAPS`]. Should always be zero; if it
    /// is not, the capture has something it cannot account for and we need to
    /// know. Named for the case that dominates it.
    pub gaps_abandoned: u64,
    /// Book snapshots that arrived, enqueued or not.
    ///
    /// Counted apart from `messages` because a snapshot is a megabyte arriving
    /// once an hour, and folding it into a stream throughput figure would make
    /// bytes/sec spike for reasons that have nothing to do with the market.
    pub snapshots: u64,
    pub snapshot_bytes: u64,
    /// Snapshots we could not enqueue. The caller's cue to fetch a fresh one --
    /// see [`Ingress::accept_snapshot`].
    pub snapshots_dropped: u64,
}

impl From<&MetricsSample> for IngressStats {
    fn from(sample: &MetricsSample) -> Self {
        Self {
            messages: sample.messages,
            bytes: sample.bytes,
            enqueued: sample.enqueued,
            dropped: sample.dropped,
            gaps_recorded: sample.gaps_recorded,
            snapshot_failures: sample.snapshot_failures,
            gaps_abandoned: sample.gaps_abandoned,
            snapshots: sample.snapshots,
            snapshot_bytes: sample.snapshot_bytes,
            snapshots_dropped: sample.snapshots_dropped,
        }
    }
}

/// A pending gap, holding the time we *noticed* rather than the time we manage
/// to write it.
///
/// Keeping the original timestamp matters: a gap flushed thirty seconds later
/// still describes an event that happened thirty seconds ago, and stamping it at
/// flush time would misattribute our own backlog to the venue.
#[derive(Debug, Clone, Copy)]
struct PendingControl {
    local_recv_ts: Ts,
    record: ControlRecord,
}

impl PendingControl {
    /// `None` for a control record that is not a gap, so the coalescing check
    /// below cannot mistake one for a gap of some unrelated cause.
    const fn cause(&self) -> Option<GapCause> {
        match self.record {
            ControlRecord::Gap { cause, .. } => Some(cause),
            ControlRecord::SnapshotFailed { .. } => None,
        }
    }
}

/// Stamps, sequences and enqueues everything the venue sends us.
///
/// One `Ingress` per (recorder process, instrument), matching the definition of
/// `ingest_seq` in `docs/data-contract.md` §3 and the one-file-per-symbol layout
/// in §5. Sequence numbers are therefore per instrument and strictly increasing
/// within a capture file, with no cross-instrument coordination to get wrong.
pub struct Ingress<S> {
    sink: S,
    clock: Arc<dyn Clock>,
    next_seq: u64,
    pending: VecDeque<PendingControl>,
    /// Last local time we are confident the stream was intact, i.e. the receive
    /// time of the last record we actually got into the channel.
    last_good_ts: Ts,
    /// Shared rather than owned, because something other than this task has to be
    /// able to read them while the connection loop runs. See [`Metrics`].
    metrics: Arc<Metrics>,
}

impl<S> fmt::Debug for Ingress<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ingress")
            .field("next_seq", &self.next_seq)
            .field("pending_gaps", &self.pending.len())
            .field("last_good_ts", &self.last_good_ts)
            // Read from the metrics directly: `stats()` needs `S: RecordSink`,
            // and a `Debug` impl that only works for some `S` is worse than one
            // that shows the same numbers.
            .field("stats", &IngressStats::from(&self.metrics.sample()))
            .finish_non_exhaustive()
    }
}

impl<S: RecordSink> Ingress<S> {
    /// Sequence numbers start at 1, so that 0 is never a valid `ingest_seq` and
    /// a zeroed or defaulted field cannot masquerade as the first message.
    #[must_use]
    pub fn new(sink: S, clock: Arc<dyn Clock>, metrics: Arc<Metrics>) -> Self {
        let now = clock.now();
        Self {
            sink,
            clock,
            next_seq: 1,
            pending: VecDeque::new(),
            // Before anything has been recorded, the last moment we can honestly
            // claim the stream was intact is the moment we started.
            last_good_ts: now,
            metrics,
        }
    }

    /// The shared counters, for a reporter or a shutdown summary.
    #[must_use]
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// The one clock. Exposed so a caller using [`Ingress::accept_at`] stamps
    /// from the same source rather than introducing a second one.
    #[must_use]
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// Take a message from the venue.
    ///
    /// `local_recv_ts` is stamped here, first thing, because this is the earliest
    /// point we control after the bytes leave the socket. Nothing downstream may
    /// recompute it.
    ///
    /// Never blocks. Returns [`Accepted::Dropped`] rather than waiting when the
    /// channel is full -- see the crate docs for why waiting would be worse than
    /// losing the message.
    pub fn accept(&mut self, payload: Vec<u8>) -> Result<Accepted, WriterGone> {
        let now = self.clock.now();
        self.accept_at(payload, now)
    }

    /// Take a message that the caller has already stamped.
    ///
    /// Exists for one reason, and it is the data contract's: §2 requires
    /// `local_recv_ts` to be stamped *"immediately on read from the socket,
    /// before parsing"*. A venue adapter that wants to measure venue latency has
    /// to parse the payload for the exchange timestamp, and doing that before
    /// handing the bytes over would put a JSON parse between the socket read and
    /// the stamp -- making the one timestamp everything dispatches on include our
    /// own parsing time.
    ///
    /// So the adapter reads the clock first, hands the stamp in here, and parses
    /// afterwards at its leisure. The clock is the same one, via
    /// [`Ingress::clock`], so there is still exactly one source of truth for when
    /// we saw the bytes.
    pub fn accept_at(
        &mut self,
        payload: Vec<u8>,
        local_recv_ts: Ts,
    ) -> Result<Accepted, WriterGone> {
        let now = local_recv_ts;
        self.metrics.record_message(payload.len() as u64);

        // Any pending gap goes out ahead of this message, so the record of the
        // hole always precedes the evidence that we recovered from it.
        self.pump()?;

        // The sequence number is consumed whether or not the message survives.
        // That is the entire mechanism by which a hole records how much was
        // lost, so it must not be conditional on success.
        let ingest_seq = self.next_seq;
        self.next_seq += 1;

        let record = CaptureRecord::Venue {
            local_recv_ts: now,
            ingest_seq,
            payload,
        };
        match self.sink.try_send(record) {
            Ok(()) => {
                self.last_good_ts = now;
                self.metrics.record_enqueued();
                Ok(Accepted::Enqueued)
            }
            Err(SinkError::Full(_)) => {
                self.metrics.record_dropped();
                self.queue_overflow_gap(now);
                Ok(Accepted::Dropped)
            }
            Err(SinkError::Disconnected) => Err(WriterGone),
        }
    }

    /// Take a book snapshot the caller fetched over REST.
    ///
    /// # Why a dropped snapshot does not consume a sequence number
    ///
    /// For a stream message the opposite is true, and deliberately so: burning
    /// the sequence number is the entire mechanism by which a hole records how
    /// much was lost. But a hole means *the venue sent us something and we lost
    /// it*, and the offline verifier reads it that way. A snapshot we fetched
    /// ourselves and failed to enqueue lost no venue stream data, so leaving a
    /// hole for it would manufacture evidence of a drop that did not happen --
    /// and there would be no gap record to explain it.
    ///
    /// # Why the caller should re-fetch rather than retry these bytes
    ///
    /// Gap records are held and retried, because a gap describes a moment in the
    /// past and its timestamp must not move. A snapshot is the opposite: its
    /// whole value is being *current*, so a fresh fetch is a strictly better
    /// record than the one we are holding, and holding a megabyte to retry it
    /// would consume memory precisely when we are already behind.
    /// [`Accepted::Dropped`] therefore means "fetch another one", and the caller
    /// counts attempts so it can eventually record
    /// [`SnapshotFailure::Overflow`].
    ///
    /// [`SnapshotFailure::Overflow`]: quant_storage::SnapshotFailure::Overflow
    pub fn accept_snapshot(&mut self, payload: Vec<u8>) -> Result<Accepted, WriterGone> {
        let now = self.clock.now();
        self.metrics.record_book_snapshot(payload.len() as u64);
        self.pump()?;

        let record = CaptureRecord::Snapshot {
            local_recv_ts: now,
            ingest_seq: self.next_seq,
            payload,
        };
        match self.sink.try_send(record) {
            Ok(()) => {
                self.next_seq += 1;
                self.last_good_ts = now;
                Ok(Accepted::Enqueued)
            }
            Err(SinkError::Full(_)) => {
                self.metrics.record_snapshot_dropped();
                Ok(Accepted::Dropped)
            }
            Err(SinkError::Disconnected) => Err(WriterGone),
        }
    }

    /// Record that a snapshot we wanted could not be obtained.
    ///
    /// Held and retried like a gap record, for the same reason: an absent
    /// snapshot frame is ambiguous, and the record that disambiguates it must not
    /// be the thing that gets dropped.
    pub fn record_snapshot_failure(
        &mut self,
        purpose: SnapshotPurpose,
        reason: SnapshotFailure,
        attempts: u32,
    ) -> Result<GapOutcome, WriterGone> {
        let now = self.clock.now();
        self.enqueue_control(
            now,
            ControlRecord::SnapshotFailed {
                purpose,
                reason,
                attempts,
            },
        );
        self.pump()?;
        Ok(if self.pending.is_empty() {
            GapOutcome::Recorded
        } else {
            GapOutcome::Deferred
        })
    }

    /// Record that data is missing for a reason the caller knows about:
    /// a disconnect, a recorder restart, a detected sequence break.
    ///
    /// [`GapCause::LocalOverflow`] is not passed in here -- ingress is the only
    /// thing that can know about it, and it raises its own.
    pub fn record_gap(&mut self, cause: GapCause) -> Result<GapOutcome, WriterGone> {
        debug_assert_ne!(
            cause,
            GapCause::LocalOverflow,
            "LocalOverflow is raised by ingress itself, not by its caller"
        );
        let now = self.clock.now();
        self.enqueue_gap(cause, now);
        self.pump()?;
        Ok(if self.pending.is_empty() {
            GapOutcome::Recorded
        } else {
            GapOutcome::Deferred
        })
    }

    /// Retry any gap records that a full channel forced us to hold.
    ///
    /// Worth calling wherever the read loop is already waiting -- reconnect
    /// backoff especially, since a disconnect both generates a gap record and
    /// guarantees the writer is about to drain.
    pub fn pump(&mut self) -> Result<(), WriterGone> {
        while let Some(pending) = self.pending.front().copied() {
            // The sequence number is taken only if the send succeeds. A gap we
            // could not write must not widen the hole it describes.
            let record = CaptureRecord::Control {
                local_recv_ts: pending.local_recv_ts,
                ingest_seq: self.next_seq,
                record: pending.record,
            };
            match self.sink.try_send(record) {
                Ok(()) => {
                    self.next_seq += 1;
                    self.last_good_ts = pending.local_recv_ts;
                    self.pending.pop_front();
                    match pending.record {
                        ControlRecord::Gap { cause, .. } => self.metrics.record_gap(cause),
                        ControlRecord::SnapshotFailed { .. } => {
                            self.metrics.record_snapshot_failure();
                        }
                    }
                }
                Err(SinkError::Full(_)) => return Ok(()),
                Err(SinkError::Disconnected) => return Err(WriterGone),
            }
        }
        Ok(())
    }

    /// Queue an overflow gap, coalescing consecutive drops into one record.
    ///
    /// Under sustained overload one record per lost message would be thousands
    /// of records contending for the very capacity we have just run out of. One
    /// record marks the episode and the width of the sequence hole supplies the
    /// count, so nothing is lost by collapsing them.
    fn queue_overflow_gap(&mut self, now: Ts) {
        let already_pending = self
            .pending
            .back()
            .is_some_and(|p| p.cause() == Some(GapCause::LocalOverflow));
        if already_pending {
            return;
        }
        self.enqueue_gap(GapCause::LocalOverflow, now);
    }

    fn enqueue_gap(&mut self, cause: GapCause, now: Ts) {
        self.enqueue_control(
            now,
            ControlRecord::Gap {
                cause,
                // A gap is ours, not the venue's; it never reported anything, so
                // our own observation time is the only honest value here.
                exchange_ts: now,
                last_good_ts: self.last_good_ts,
            },
        );
    }

    fn enqueue_control(&mut self, now: Ts, record: ControlRecord) {
        if self.pending.len() >= MAX_PENDING_GAPS {
            self.metrics.record_gap_abandoned();
            return;
        }
        self.pending.push_back(PendingControl {
            local_recv_ts: now,
            record,
        });
    }

    #[must_use]
    pub fn stats(&self) -> IngressStats {
        IngressStats::from(&self.metrics.sample())
    }

    /// Gap records currently held because the channel was full.
    #[must_use]
    pub fn pending_gaps(&self) -> usize {
        self.pending.len()
    }

    /// Next sequence number to be issued.
    #[must_use]
    pub fn next_ingest_seq(&self) -> u64 {
        self.next_seq
    }

    #[must_use]
    pub fn sink(&self) -> &S {
        &self.sink
    }

    pub fn sink_mut(&mut self) -> &mut S {
        &mut self.sink
    }
}

#[cfg(test)]
mod tests {
    use quant_core::time::ManualClock;
    use std::time::Duration;

    use super::*;
    use crate::sink::test_sink::TestSink;

    fn ingress(capacity: usize) -> (Ingress<TestSink>, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(Ts::from_millis(1_700_000_000_000)));
        let ing = Ingress::new(
            TestSink::with_capacity(capacity),
            clock.clone(),
            Metrics::new(capacity),
        );
        (ing, clock)
    }

    fn tick(clock: &ManualClock) {
        clock.advance(Duration::from_millis(1));
    }

    fn causes(records: &[CaptureRecord]) -> Vec<GapCause> {
        records
            .iter()
            .filter_map(|r| match r {
                CaptureRecord::Control {
                    record: ControlRecord::Gap { cause, .. },
                    ..
                } => Some(*cause),
                CaptureRecord::Control { .. }
                | CaptureRecord::Venue { .. }
                | CaptureRecord::Snapshot { .. } => None,
            })
            .collect()
    }

    #[test]
    fn stamps_receive_time_and_sequences_from_one() {
        let (mut ing, clock) = ingress(16);
        assert_eq!(ing.accept(b"a".to_vec()).unwrap(), Accepted::Enqueued);
        tick(&clock);
        assert_eq!(ing.accept(b"b".to_vec()).unwrap(), Accepted::Enqueued);

        let accepted = &ing.sink().accepted;
        assert_eq!(accepted[0].ingest_seq(), 1, "0 must never be a valid seq");
        assert_eq!(accepted[1].ingest_seq(), 2);
        assert_eq!(
            accepted[0].local_recv_ts(),
            Ts::from_millis(1_700_000_000_000)
        );
        assert_eq!(
            accepted[1].local_recv_ts(),
            Ts::from_millis(1_700_000_000_001),
            "each message is stamped when it arrived, not in a batch"
        );
        assert_eq!(ing.stats().enqueued, 2);
        assert_eq!(ing.stats().bytes, 2);
        assert_eq!(ing.stats().dropped, 0);
    }

    #[test]
    fn a_full_channel_drops_rather_than_waits() {
        let (mut ing, clock) = ingress(1);
        assert_eq!(ing.accept(b"kept".to_vec()).unwrap(), Accepted::Enqueued);
        tick(&clock);
        // No room. The read task must come straight back for the next message
        // rather than stalling the socket.
        assert_eq!(ing.accept(b"lost".to_vec()).unwrap(), Accepted::Dropped);
        assert_eq!(ing.stats().dropped, 1);
        assert_eq!(ing.pending_gaps(), 1, "the drop must be pending a record");
    }

    #[test]
    fn the_sequence_hole_equals_the_number_of_messages_dropped() {
        // The property the whole design rests on: no count is stored anywhere,
        // so the hole has to carry it exactly.
        let (mut ing, clock) = ingress(2);
        for _ in 0..2 {
            assert_eq!(ing.accept(b"kept".to_vec()).unwrap(), Accepted::Enqueued);
            tick(&clock);
        }

        let dropped_on_purpose = 40;
        for _ in 0..dropped_on_purpose {
            assert_eq!(ing.accept(b"lost".to_vec()).unwrap(), Accepted::Dropped);
            tick(&clock);
        }

        // The writer catches up.
        let before = ing.sink_mut().drain();
        assert_eq!(
            before
                .iter()
                .map(CaptureRecord::ingest_seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );

        assert_eq!(
            ing.accept(b"recovered".to_vec()).unwrap(),
            Accepted::Enqueued
        );
        let after = &ing.sink().accepted;

        // Reconstruct the stream as it will appear on disk.
        let seqs: Vec<u64> = before
            .iter()
            .chain(after.iter())
            .map(CaptureRecord::ingest_seq)
            .collect();
        assert_eq!(seqs, vec![1, 2, 43, 44]);

        // Seqs 3..=42 are missing: exactly the 40 messages we dropped.
        let hole = seqs[2] - seqs[1] - 1;
        assert_eq!(hole, dropped_on_purpose);
        assert_eq!(hole, ing.stats().dropped);
    }

    #[test]
    fn consecutive_drops_coalesce_into_one_gap_record() {
        let (mut ing, clock) = ingress(1);
        ing.accept(b"kept".to_vec()).unwrap();
        for _ in 0..100 {
            tick(&clock);
            ing.accept(b"lost".to_vec()).unwrap();
        }
        assert_eq!(ing.stats().dropped, 100);
        assert_eq!(
            ing.pending_gaps(),
            1,
            "100 lost messages must not queue 100 gap records"
        );

        ing.sink_mut().drain();
        ing.pump().unwrap();
        assert_eq!(causes(&ing.sink().accepted), vec![GapCause::LocalOverflow]);
        assert_eq!(ing.stats().gaps_recorded, 1);
    }

    #[test]
    fn the_overflow_gap_lands_before_the_message_that_proves_recovery() {
        let (mut ing, clock) = ingress(1);
        ing.accept(b"kept".to_vec()).unwrap();
        tick(&clock);
        ing.accept(b"lost".to_vec()).unwrap();
        tick(&clock);

        ing.sink_mut().drain();
        ing.sink_mut().capacity = 8;
        ing.accept(b"recovered".to_vec()).unwrap();

        let accepted = &ing.sink().accepted;
        assert!(
            accepted[0].is_control(),
            "a reader must meet the gap before it meets post-gap data"
        );
        assert!(!accepted[1].is_control());
        assert!(
            accepted[0].ingest_seq() < accepted[1].ingest_seq(),
            "sequence order must agree with stream order"
        );
    }

    #[test]
    fn a_deferred_gap_consumes_no_sequence_number() {
        // Otherwise a gap we failed to write would widen the very hole it exists
        // to explain, and the hole would overstate what was lost.
        let (mut ing, clock) = ingress(1);
        ing.accept(b"kept".to_vec()).unwrap();
        tick(&clock);
        ing.accept(b"lost".to_vec()).unwrap();

        let before = ing.next_ingest_seq();
        for _ in 0..5 {
            ing.pump().unwrap();
        }
        assert_eq!(ing.pending_gaps(), 1, "still no room");
        assert_eq!(
            ing.next_ingest_seq(),
            before,
            "failed gap writes must not burn sequence numbers"
        );
    }

    #[test]
    fn gap_records_survive_a_full_channel_and_are_retried() {
        let (mut ing, _clock) = ingress(1);
        ing.accept(b"kept".to_vec()).unwrap();
        // Channel full, so this cannot be written yet -- but it must not be lost.
        assert_eq!(
            ing.record_gap(GapCause::Disconnect).unwrap(),
            GapOutcome::Deferred
        );
        assert_eq!(ing.stats().gaps_recorded, 0);

        ing.sink_mut().drain();
        ing.pump().unwrap();
        assert_eq!(causes(&ing.sink().accepted), vec![GapCause::Disconnect]);
        assert_eq!(ing.pending_gaps(), 0);
    }

    #[test]
    fn gaps_are_recorded_in_the_order_they_happened() {
        let (mut ing, clock) = ingress(1);
        // Restart at startup, with room: recorded immediately.
        assert_eq!(
            ing.record_gap(GapCause::RecorderRestart).unwrap(),
            GapOutcome::Recorded
        );

        // Now the channel is full: a drop, then a disconnect, both deferred.
        tick(&clock);
        ing.accept(b"lost".to_vec()).unwrap();
        tick(&clock);
        ing.record_gap(GapCause::Disconnect).unwrap();
        assert_eq!(ing.pending_gaps(), 2);

        ing.sink_mut().drain();
        ing.sink_mut().capacity = 8;
        ing.pump().unwrap();
        assert_eq!(
            causes(&ing.sink().accepted),
            vec![GapCause::LocalOverflow, GapCause::Disconnect],
            "the overflow happened first and must be read first"
        );
    }

    #[test]
    fn a_gap_reports_the_last_moment_we_know_the_stream_was_intact() {
        let (mut ing, clock) = ingress(2);
        ing.accept(b"first".to_vec()).unwrap();
        tick(&clock);
        ing.accept(b"second".to_vec()).unwrap();
        let last_good = Ts::from_millis(1_700_000_000_001);

        tick(&clock);
        ing.accept(b"lost".to_vec()).unwrap();
        ing.sink_mut().drain();
        ing.pump().unwrap();

        match ing.sink().accepted[0] {
            CaptureRecord::Control {
                local_recv_ts,
                record:
                    ControlRecord::Gap {
                        cause,
                        last_good_ts,
                        exchange_ts,
                    },
                ..
            } => {
                assert_eq!(cause, GapCause::LocalOverflow);
                assert_eq!(
                    last_good_ts, last_good,
                    "must point at the last record that reached the channel"
                );
                // Stamped when we noticed the drop, not when we managed to write
                // the record: otherwise our own backlog would be attributed to
                // the venue.
                assert_eq!(local_recv_ts, Ts::from_millis(1_700_000_000_002));
                assert_eq!(exchange_ts, local_recv_ts);
            }
            ref other => panic!("expected an overflow gap, got {other:?}"),
        }
    }

    #[test]
    fn a_gap_before_anything_was_recorded_still_has_an_honest_last_good_ts() {
        let (mut ing, _clock) = ingress(8);
        ing.record_gap(GapCause::RecorderRestart).unwrap();
        match ing.sink().accepted[0] {
            CaptureRecord::Control {
                record: ControlRecord::Gap { last_good_ts, .. },
                ..
            } => assert_eq!(
                last_good_ts,
                Ts::from_millis(1_700_000_000_000),
                "with nothing recorded yet, the honest answer is when we started"
            ),
            ref other => panic!("expected a gap, got {other:?}"),
        }
    }

    #[test]
    fn a_snapshot_takes_its_place_in_the_sequence_between_deltas() {
        // The position is the point: it tells the book builder which deltas
        // precede the snapshot and are therefore stale. A snapshot delivered out
        // of band, or stamped with a sequence number from before the deltas it
        // followed, would make that undecidable.
        let (mut ing, clock) = ingress(16);
        ing.accept(b"delta 1".to_vec()).unwrap();
        tick(&clock);
        assert_eq!(
            ing.accept_snapshot(b"{\"lastUpdateId\":42}".to_vec())
                .unwrap(),
            Accepted::Enqueued
        );
        tick(&clock);
        ing.accept(b"delta 2".to_vec()).unwrap();

        let accepted = &ing.sink().accepted;
        assert_eq!(
            accepted
                .iter()
                .map(CaptureRecord::ingest_seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(matches!(accepted[1], CaptureRecord::Snapshot { .. }));
        assert_eq!(
            accepted[1].local_recv_ts(),
            Ts::from_millis(1_700_000_000_001),
            "stamped when the REST body arrived, not when the connection opened"
        );
        assert_eq!(ing.stats().snapshots, 1);
        assert_eq!(ing.stats().snapshot_bytes, 19);
        assert_eq!(
            ing.stats().messages,
            2,
            "a snapshot is not a stream message and must not inflate throughput"
        );
    }

    #[test]
    fn a_dropped_snapshot_leaves_no_hole_because_nothing_was_lost() {
        // A hole in ingest_seq means the venue sent us something and we lost it,
        // and that is how the verifier reads one. A snapshot we fetched ourselves
        // and could not enqueue lost no stream data, so burning a sequence number
        // would manufacture evidence of a drop -- and leave it unexplained, since
        // there is no gap record to pair it with.
        let (mut ing, clock) = ingress(1);
        ing.accept(b"delta".to_vec()).unwrap();
        tick(&clock);

        let before = ing.next_ingest_seq();
        assert_eq!(
            ing.accept_snapshot(vec![0; 1024]).unwrap(),
            Accepted::Dropped
        );
        assert_eq!(ing.next_ingest_seq(), before, "no sequence number consumed");
        assert_eq!(ing.stats().snapshots_dropped, 1);
        assert_eq!(
            ing.pending_gaps(),
            0,
            "and no gap record, because the stream is intact"
        );

        // The caller's remedy is a fresh fetch, which then sequences normally.
        ing.sink_mut().drain();
        ing.accept_snapshot(vec![1; 512]).unwrap();
        assert_eq!(ing.sink().accepted[0].ingest_seq(), before);
    }

    #[test]
    fn a_snapshot_we_could_not_get_is_recorded_rather_than_left_as_a_silence() {
        // Otherwise an absent snapshot frame cannot be told from a build that
        // never fetched one, and those mean opposite things.
        let (mut ing, _clock) = ingress(8);
        assert_eq!(
            ing.record_snapshot_failure(SnapshotPurpose::Resync, SnapshotFailure::Status, 3)
                .unwrap(),
            GapOutcome::Recorded
        );
        match ing.sink().accepted[0] {
            CaptureRecord::Control {
                record:
                    ControlRecord::SnapshotFailed {
                        purpose,
                        reason,
                        attempts,
                    },
                ..
            } => {
                assert_eq!(purpose, SnapshotPurpose::Resync);
                assert_eq!(reason, SnapshotFailure::Status);
                assert_eq!(attempts, 3);
            }
            ref other => panic!("expected a snapshot failure, got {other:?}"),
        }
        // Not a gap: no messages were lost, and treating it as one would make
        // "refuse to trade across a gap" reject data that is perfectly good.
        assert!(causes(&ing.sink().accepted).is_empty());
        assert_eq!(
            ing.stats().gaps_recorded,
            0,
            "a snapshot failure must not be counted as a gap: it would overstate \
             how blind we were, in the log line an operator reads first"
        );
        assert_eq!(ing.stats().snapshot_failures, 1);
    }

    #[test]
    fn a_snapshot_failure_is_held_and_retried_like_a_gap() {
        // Same reasoning as a gap record: the record that explains an absence must
        // not itself be the thing that goes missing.
        let (mut ing, _clock) = ingress(1);
        ing.accept(b"delta".to_vec()).unwrap();
        assert_eq!(
            ing.record_snapshot_failure(SnapshotPurpose::Periodic, SnapshotFailure::Timeout, 2)
                .unwrap(),
            GapOutcome::Deferred
        );

        ing.sink_mut().drain();
        ing.pump().unwrap();
        assert!(ing.sink().accepted[0].is_control());
        assert_eq!(ing.pending_gaps(), 0);
    }

    #[test]
    fn a_dead_writer_is_fatal_rather_than_a_silent_data_sink() {
        let (mut ing, _clock) = ingress(8);
        ing.accept(b"kept".to_vec()).unwrap();
        ing.sink_mut().disconnected = true;

        assert_eq!(ing.accept(b"nowhere".to_vec()), Err(WriterGone));
        assert_eq!(ing.accept_snapshot(b"nowhere".to_vec()), Err(WriterGone));
        assert_eq!(ing.record_gap(GapCause::Disconnect), Err(WriterGone));
        assert_eq!(
            ing.record_snapshot_failure(SnapshotPurpose::Resync, SnapshotFailure::Transport, 1),
            Err(WriterGone)
        );
        assert_eq!(ing.pump(), Err(WriterGone));
    }

    #[test]
    fn holding_gaps_is_bounded_and_losing_one_is_counted() {
        let (mut ing, clock) = ingress(0);
        for _ in 0..MAX_PENDING_GAPS {
            ing.record_gap(GapCause::Disconnect).unwrap();
            tick(&clock);
        }
        assert_eq!(ing.pending_gaps(), MAX_PENDING_GAPS);
        assert_eq!(ing.stats().gaps_abandoned, 0);

        ing.record_gap(GapCause::Disconnect).unwrap();
        assert_eq!(ing.pending_gaps(), MAX_PENDING_GAPS, "growth is bounded");
        assert_eq!(
            ing.stats().gaps_abandoned,
            1,
            "and a lost gap record is never silent"
        );
    }
}
