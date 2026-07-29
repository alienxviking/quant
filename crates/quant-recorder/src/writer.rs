//! The blocking consumer that turns records into a capture file.

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use quant_storage::StorageResult;

use crate::record::CaptureRecord;
use crate::segment::{CaptureSession, SegmentStore};

/// Maximum age of an unsealed block.
///
/// # What this bounds
///
/// Frames accumulate in memory until a block is sealed, and unsealed frames are
/// **our** memory, not the kernel's -- so a panic or a `SIGKILL` loses them,
/// unlike a sealed block, which survives process death in the page cache. This
/// interval is therefore the worst-case data loss when the process dies, and the
/// only reason it is not simply "as small as possible" is compression.
///
/// # Why it is an age and not an idle timeout
///
/// The obvious implementation -- seal after this much silence -- is wrong, and
/// wrong in a way that hides itself. A stream that trickles steadily never goes
/// silent, so it never triggers an idle flush, while at a few KB/s it also takes
/// a minute or more to reach the block-size threshold. The result is a recorder
/// that appears to be running, is receiving data, and has written nothing to
/// disk. Bounding the *age of the pending block* covers both the quiet stream and
/// the slow one.
///
/// # Why five seconds
///
/// It is a straight trade against compression ratio. zstd needs a window wide
/// enough to see redundancy between messages, so sealing early means smaller
/// blocks and a worse ratio: at a few KB/s, five seconds is tens of kilobytes,
/// which still compresses well on near-identical depth messages, where half a
/// second would be a few hundred bytes and barely compress at all.
///
/// The metric that says whether this is set wrong is
/// [`WriterOutcome::timed_flushes`] against `stats.blocks`: mostly timed flushes
/// means volume never reaches the threshold and the ratio is being left on the
/// table.
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// What the writer did, once the channel closed.
///
/// Per-file numbers are not here: one run can span several segments, so they live
/// on the [`SegmentReport`](crate::SegmentReport) the session produces for each.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriterOutcome {
    pub records: u64,
    /// Timed flushes performed, i.e. blocks sealed by the clock rather than by
    /// volume. A high ratio against `stats.blocks` means the flush interval is
    /// costing compression on a quiet stream.
    pub timed_flushes: u64,
}

/// Drain `rx` into `session` until the channel closes.
///
/// Blocking by design, and intended to own a thread. Framing and zstd are real
/// CPU work and file writes are real blocking I/O; running them here is what
/// keeps them off the task that has to keep draining the socket. See the crate
/// docs for why that separation is not optional.
///
/// Returns when every sender has been dropped, which is the recorder's shutdown
/// signal. The caller then calls [`CaptureSession::finish`] to seal the last
/// segment with its trailer, so a clean shutdown stays distinguishable from a kill
/// by inspection of the files alone.
pub fn run_writer<S: SegmentStore>(
    rx: &Receiver<CaptureRecord>,
    session: &mut CaptureSession<S>,
    flush_interval: Duration,
) -> StorageResult<WriterOutcome> {
    let mut records = 0_u64;
    let mut timed_flushes = 0_u64;
    // Deadline for the *pending block*, not for the next message. Waiting
    // `flush_interval` per `recv` would only ever fire on an idle stream; a
    // steady trickle would reset the wait on every message and never seal.
    //
    // `Instant`, deliberately, rather than the injected `Clock`. That trait
    // exists so recorded data is reproducible, and a flush deadline never
    // appears in the data -- it only decides when bytes reach the disk. Driving
    // it from an injected clock would also mean a `ManualClock` that nobody
    // advances never flushes at all.
    let mut deadline = Instant::now() + flush_interval;

    loop {
        let wait = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(wait) {
            Ok(record) => {
                // The session decides which segment this belongs in, from the
                // record's own timestamp -- see `segment.rs`.
                session.write(record)?;
                records += 1;
                // A single message can arrive right on the deadline; check rather
                // than assuming only a timeout can reach it.
                if Instant::now() >= deadline {
                    session.flush()?;
                    timed_flushes += 1;
                    deadline = Instant::now() + flush_interval;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                session.flush()?;
                timed_flushes += 1;
                deadline = Instant::now() + flush_interval;
                // The flush tick is also where a quiet instrument's finished day
                // gets closed. Without this, a healthy but silent symbol would
                // leave yesterday's file trailerless, which reads as a crash.
                session.roll_if_day_elapsed()?;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    Ok(WriterOutcome {
        records,
        timed_flushes,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use quant_core::event::GapCause;
    use quant_core::instrument::Exchange;
    use quant_core::time::{Clock, ManualClock, Ts};
    use quant_storage::{ControlRecord, FrameKind, RawReader, WriterOptions};

    use super::*;
    use crate::ingress::{Accepted, Ingress};
    use crate::segment::MemoryStore;
    use crate::sink::channel;

    /// 2026-07-29T23:59:59Z: one second before a UTC day boundary.
    const BEFORE_MIDNIGHT: i64 = 1_785_369_599;

    fn new_session(clock: Arc<dyn Clock>) -> CaptureSession<MemoryStore> {
        CaptureSession::new(
            MemoryStore::new(),
            Exchange::Binance,
            "BTCUSDT",
            [9; 16],
            clock,
            WriterOptions::default(),
        )
    }

    fn depth(n: u64) -> Vec<u8> {
        format!(
            r#"{{"e":"depthUpdate","E":{},"s":"BTCUSDT","U":{},"u":{},"b":[["68123.45","0.5"]],"a":[["68123.46","1.25"]]}}"#,
            1_700_000_000_000_u64 + n,
            n * 10,
            n * 10 + 9
        )
        .into_bytes()
    }

    /// The end-to-end property M1.b exists to establish: what ingress decided
    /// under overload is exactly what a reader finds on disk afterwards.
    #[test]
    fn overload_survives_the_round_trip_to_disk_as_a_hole_and_a_gap() {
        // Capacity 4 so the burst below genuinely cannot fit, deterministically.
        let (tx, rx) = channel(4);
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(Ts::from_millis(1_700_000_000_000)));
        let mut ing = Ingress::new(tx, Arc::clone(&clock));

        // Nobody is draining yet, so the channel fills and then overflows.
        let mut enqueued = 0_u64;
        let mut dropped = 0_u64;
        for i in 0..64 {
            match ing.accept(depth(i)).unwrap() {
                Accepted::Enqueued => enqueued += 1,
                Accepted::Dropped => dropped += 1,
            }
        }
        assert!(dropped > 0, "the burst was supposed to overflow");
        assert_eq!(enqueued + dropped, 64);

        // Now start the writer and let ingress flush its pending gap.
        let mut session = new_session(clock);
        let handle = thread::spawn(move || {
            let outcome = run_writer(&rx, &mut session, Duration::from_millis(50));
            (outcome, session)
        });

        // Retry the deferred overflow gap until it lands.
        while ing.pending_gaps() > 0 {
            ing.pump().unwrap();
        }
        let stats = ing.stats();
        drop(ing); // closes the channel, so the writer loop returns

        let (outcome, session) = handle.join().unwrap();
        let outcome = outcome.unwrap();
        let (reports, store) = session.finish().unwrap();
        assert_eq!(reports.len(), 1, "one day, one segment");
        let bytes = &store.segments[0].1;

        let mut reader = RawReader::open(bytes.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert_eq!(truncation, None);
        assert!(
            reader.is_finalized(),
            "a clean shutdown must leave a closed file"
        );

        // Exactly one gap record, and it says we were the problem.
        let gaps: Vec<_> = frames
            .iter()
            .filter(|f| f.kind == FrameKind::Control)
            .map(|f| f.control().unwrap().unwrap())
            .collect();
        assert_eq!(gaps.len(), 1);
        let ControlRecord::Gap { cause, .. } = gaps[0] else {
            panic!("expected a gap, got {:?}", gaps[0]);
        };
        assert_eq!(cause, GapCause::LocalOverflow);

        // And the hole in the sequence accounts for every dropped message,
        // with no count stored anywhere on disk.
        let seqs: Vec<u64> = frames.iter().map(|f| f.ingest_seq).collect();
        let missing: u64 = seqs.windows(2).map(|w| w[1] - w[0] - 1).sum();
        assert_eq!(
            missing, dropped,
            "the sequence hole must equal what ingress dropped"
        );
        assert_eq!(stats.dropped, dropped);
        assert_eq!(
            outcome.records,
            enqueued + 1,
            "everything enqueued, plus the one coalesced gap record"
        );
    }

    #[test]
    fn a_quiet_stream_still_gets_its_block_sealed_on_the_timer() {
        let (tx, rx) = channel(16);
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(Ts::from_millis(1_700_000_000_000)));
        let mut ing = Ingress::new(tx, Arc::clone(&clock));

        // Far below the 256 KiB block threshold: only a timed flush can seal it.
        let mut session = new_session(clock);
        let handle = thread::spawn(move || {
            let outcome = run_writer(&rx, &mut session, Duration::from_millis(20));
            (outcome, session)
        });

        ing.accept(depth(1)).unwrap();
        // Give the writer time to hit at least one timeout with data pending.
        thread::sleep(Duration::from_millis(150));
        drop(ing);

        let (outcome, session) = handle.join().unwrap();
        let outcome = outcome.unwrap();
        assert!(
            outcome.timed_flushes > 0,
            "a quiet stream must not hold frames in memory indefinitely"
        );
        assert_eq!(outcome.records, 1);

        let (_, store) = session.finish().unwrap();
        let mut reader = RawReader::open(store.segments[0].1.as_slice()).unwrap();
        assert_eq!(reader.read_all().unwrap().0.len(), 1);
    }

    /// Regression test for a flush policy that bounded idle time instead of
    /// block age.
    ///
    /// A stream that trickles steadily never goes idle, so an idle-based flush
    /// never fires; at a few KB/s it also takes a minute to reach the 256 KiB
    /// block threshold. The recorder then looks healthy, receives data, and writes
    /// nothing at all -- which is exactly what a live Binance feed did.
    #[test]
    fn a_steady_trickle_is_sealed_even_though_it_never_goes_idle() {
        let (tx, rx) = channel(64);
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(Ts::from_millis(1_700_000_000_000)));
        let mut ing = Ingress::new(tx, Arc::clone(&clock));

        // Payloads far too small to ever reach the block-size threshold, arriving
        // faster than the flush interval so the channel is never empty for long.
        let producer = thread::spawn(move || {
            for i in 0..60 {
                ing.accept(depth(i)).unwrap();
                thread::sleep(Duration::from_millis(5));
            }
            ing.stats()
        });

        let mut session = new_session(clock);
        let outcome = run_writer(&rx, &mut session, Duration::from_millis(25)).unwrap();
        let stats = producer.join().unwrap();
        let (reports, store) = session.finish().unwrap();

        assert_eq!(outcome.records, 60);
        assert_eq!(stats.dropped, 0);
        assert!(
            outcome.timed_flushes >= 2,
            "a trickling stream must still be sealed periodically, got {}",
            outcome.timed_flushes
        );
        assert!(
            reports[0].stats.blocks >= 2,
            "expected several sealed blocks, got {}",
            reports[0].stats.blocks
        );

        // And it is all readable, in order, with nothing lost.
        let mut reader = RawReader::open(store.segments[0].1.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert_eq!(frames.len(), 60);
        assert_eq!(truncation, None);
        assert!(reader.is_finalized());
    }

    #[test]
    fn dropping_the_sender_closes_the_file_cleanly() {
        let (tx, rx) = channel(16);
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(Ts::from_millis(1_700_000_000_000)));
        let mut ing = Ingress::new(tx, Arc::clone(&clock));
        for i in 0..10 {
            ing.accept(depth(i)).unwrap();
        }
        drop(ing);

        let mut session = new_session(clock);
        let outcome = run_writer(&rx, &mut session, Duration::from_millis(20)).unwrap();
        assert_eq!(outcome.records, 10);
        let (_, store) = session.finish().unwrap();

        let mut reader = RawReader::open(store.segments[0].1.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert_eq!(frames.len(), 10);
        assert_eq!(truncation, None);
        assert_eq!(reader.trailer().unwrap().frames, 10);
        assert_eq!(reader.trailer().unwrap().last_ingest_seq, Some(10));
    }

    /// The other half of the roll story: an instrument that goes quiet across
    /// midnight must still have yesterday's file sealed, or a healthy silent
    /// symbol is indistinguishable from a crashed one.
    #[test]
    fn the_flush_tick_closes_a_finished_day_with_no_traffic_at_all() {
        let (tx, rx) = channel(16);
        let clock = Arc::new(ManualClock::new(Ts::from_secs(BEFORE_MIDNIGHT)));
        let dyn_clock: Arc<dyn Clock> = Arc::clone(&clock) as Arc<dyn Clock>;
        let mut ing = Ingress::new(tx, Arc::clone(&dyn_clock));

        let mut session = new_session(dyn_clock);
        let handle = thread::spawn(move || {
            let outcome = run_writer(&rx, &mut session, Duration::from_millis(20));
            (outcome, session)
        });

        // One record just before midnight, then the stream falls silent.
        ing.accept(b"{\"e\":\"trade\"}".to_vec()).unwrap();
        thread::sleep(Duration::from_millis(60));
        // The day ends with nothing further arriving.
        clock.set(Ts::from_secs(BEFORE_MIDNIGHT + 5));
        thread::sleep(Duration::from_millis(120));
        drop(ing);

        let (outcome, session) = handle.join().unwrap();
        outcome.unwrap();
        assert_eq!(
            session.rolls(),
            1,
            "the finished day should have been closed by the flush tick"
        );
        // Only closed, not reopened: a day with no data leaves no empty file.
        assert!(session.open_target().is_none());

        let (reports, store) = session.finish().unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].target.date.to_string(), "2026-07-29");
        let mut reader = RawReader::open(store.segments[0].1.as_slice()).unwrap();
        let (frames, _) = reader.read_all().unwrap();
        assert_eq!(frames.len(), 1);
        assert!(
            reader.is_finalized(),
            "yesterday's file must not be left trailerless"
        );
    }
}
