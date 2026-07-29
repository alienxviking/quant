//! The blocking consumer that turns records into a capture file.

use std::io::Write;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use quant_storage::{RawWriter, StorageResult, WriterStats};

use crate::record::CaptureRecord;

/// How often a quiet instrument still gets its pending block sealed.
///
/// Without a timed flush, block sealing would be driven purely by volume, and a
/// thinly traded symbol could hold frames in memory for hours -- frames a crash
/// would take with it. Two seconds bounds that exposure at roughly two seconds
/// of data regardless of how slow the market is.
///
/// The cost of flushing early is a smaller block and therefore a worse
/// compression ratio, which is exactly the right thing to trade away: the
/// instruments this affects are the ones producing almost no data.
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(2);

/// What the writer did, once the channel closed.
#[derive(Debug)]
pub struct WriterOutcome<W> {
    /// The sink, returned so the caller can `sync_all` a file or inspect a
    /// buffer. Handed back rather than dropped because durability policy belongs
    /// to whoever opened it.
    pub sink: W,
    pub stats: WriterStats,
    pub records: u64,
    /// Timed flushes performed, i.e. blocks sealed by the clock rather than by
    /// volume. A high ratio against `stats.blocks` means the flush interval is
    /// costing compression on a quiet stream.
    pub timed_flushes: u64,
}

/// Drain `rx` into `writer` until the channel closes, then close the file.
///
/// Blocking by design, and intended to own a thread. Framing and zstd are real
/// CPU work and file writes are real blocking I/O; running them here is what
/// keeps them off the task that has to keep draining the socket. See the crate
/// docs for why that separation is not optional.
///
/// Returns when every sender has been dropped, which is the recorder's shutdown
/// signal: the file is sealed with a trailer, so a clean shutdown is
/// distinguishable from a kill by inspection of the file alone.
pub fn run_writer<W: Write>(
    rx: &Receiver<CaptureRecord>,
    mut writer: RawWriter<W>,
    flush_interval: Duration,
) -> StorageResult<WriterOutcome<W>> {
    let mut records = 0_u64;
    let mut timed_flushes = 0_u64;

    loop {
        match rx.recv_timeout(flush_interval) {
            Ok(record) => {
                write_one(&mut writer, record)?;
                records += 1;
            }
            Err(RecvTimeoutError::Timeout) => {
                // Nothing arrived for a whole interval. Seal what we have rather
                // than holding it indefinitely.
                writer.flush()?;
                timed_flushes += 1;
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    let (sink, stats) = writer.finish()?;
    Ok(WriterOutcome {
        sink,
        stats,
        records,
        timed_flushes,
    })
}

/// Forward one record, preserving the timestamp and sequence number ingress
/// assigned.
///
/// Nothing here consults a clock. The whole point of carrying `local_recv_ts`
/// through the channel is that the value on disk is when we *saw* the bytes, not
/// when we got round to writing them.
fn write_one<W: Write>(writer: &mut RawWriter<W>, record: CaptureRecord) -> StorageResult<()> {
    match record {
        CaptureRecord::Venue {
            local_recv_ts,
            ingest_seq,
            payload,
        } => writer.write_venue_payload(local_recv_ts, ingest_seq, &payload),
        CaptureRecord::Control {
            local_recv_ts,
            ingest_seq,
            record,
        } => writer.write_control(local_recv_ts, ingest_seq, &record),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use quant_core::event::GapCause;
    use quant_core::instrument::Exchange;
    use quant_core::time::{Clock, ManualClock, Ts};
    use quant_storage::{
        ControlRecord, FileHeader, FrameKind, RawReader, RawWriter, WriterOptions,
    };

    use super::*;
    use crate::ingress::{Accepted, Ingress};
    use crate::sink::channel;

    fn header() -> FileHeader {
        FileHeader::new(Exchange::Binance, "BTCUSDT", [9; 16])
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
        let mut ing = Ingress::new(tx, clock);

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
        let writer = RawWriter::create(Vec::new(), header(), WriterOptions::default()).unwrap();
        let handle = thread::spawn(move || run_writer(&rx, writer, Duration::from_millis(50)));

        // Retry the deferred overflow gap until it lands.
        while ing.pending_gaps() > 0 {
            ing.pump().unwrap();
        }
        let stats = ing.stats();
        drop(ing); // closes the channel, so the writer finishes and seals

        let outcome = handle.join().unwrap().unwrap();
        let bytes = outcome.sink;

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
        let ControlRecord::Gap { cause, .. } = gaps[0];
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
        let mut ing = Ingress::new(tx, clock);

        let writer = RawWriter::create(Vec::new(), header(), WriterOptions::default()).unwrap();
        // Far below the 256 KiB block threshold: only a timed flush can seal it.
        let handle = thread::spawn(move || run_writer(&rx, writer, Duration::from_millis(20)));

        ing.accept(depth(1)).unwrap();
        // Give the writer time to hit at least one timeout with data pending.
        thread::sleep(Duration::from_millis(150));
        drop(ing);

        let outcome = handle.join().unwrap().unwrap();
        assert!(
            outcome.timed_flushes > 0,
            "a quiet stream must not hold frames in memory indefinitely"
        );
        assert_eq!(outcome.records, 1);

        let mut reader = RawReader::open(outcome.sink.as_slice()).unwrap();
        assert_eq!(reader.read_all().unwrap().0.len(), 1);
    }

    #[test]
    fn dropping_the_sender_closes_the_file_cleanly() {
        let (tx, rx) = channel(16);
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(Ts::from_millis(1_700_000_000_000)));
        let mut ing = Ingress::new(tx, clock);
        for i in 0..10 {
            ing.accept(depth(i)).unwrap();
        }
        drop(ing);

        let writer = RawWriter::create(Vec::new(), header(), WriterOptions::default()).unwrap();
        let outcome = run_writer(&rx, writer, Duration::from_millis(20)).unwrap();
        assert_eq!(outcome.records, 10);

        let mut reader = RawReader::open(outcome.sink.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert_eq!(frames.len(), 10);
        assert_eq!(truncation, None);
        assert_eq!(reader.trailer().unwrap().frames, 10);
        assert_eq!(reader.trailer().unwrap().last_ingest_seq, Some(10));
    }
}
