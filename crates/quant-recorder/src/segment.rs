//! Capture files, and rolling to a new one at the UTC day boundary.
//!
//! # What decides when to roll
//!
//! The record's own `local_recv_ts`, not the writer's wall clock.
//!
//! That distinction matters because the writer runs behind the reader by design:
//! records sit in a channel, and the writer may be a second or more late under
//! load. Rolling on the writer's clock would file a message stamped 23:59:59.9
//! into the following day whenever the writer happened to cross midnight before
//! draining it -- so the partition a record lands in would depend on how busy our
//! disk was, and re-running the same capture could produce a different layout.
//!
//! Rolling on the record's timestamp makes the partition a property of the data.
//! It is also what makes this testable with a frozen clock.
//!
//! # The exception: an idle stream
//!
//! Timestamp-driven rolling alone leaves yesterday's file open until the next
//! message arrives, which on a quiet instrument could be hours. An unsealed file
//! means no trailer, and no trailer reads as "killed, or still running" -- so a
//! perfectly healthy quiet symbol would look like a crashed one.
//!
//! [`CaptureSession::roll_if_day_elapsed`] closes it on the writer's clock once
//! that day is genuinely over. Note that it only *closes*; the next segment is
//! opened lazily by the next record, so a day with no data produces no file at
//! all rather than an empty one.
//!
//! Two clocks appear in this module and that is deliberate: an injected
//! [`Clock`] for "which calendar day is it", which must be wall time and must be
//! fakeable, and [`std::time::Instant`] in the writer loop for "how long since I
//! sealed", which must be monotonic and must not lurch when NTP steps.
//!
//! # `ingest_seq` spans segments
//!
//! Sequence numbers belong to the *session*, per `docs/data-contract.md` §3, so
//! they continue across a roll rather than restarting at 1. A file therefore does
//! not begin at 1, and verifying a session means joining its files in order and
//! checking that each one's first sequence continues the previous one's last.
//! Restarting per file would have been tidier to look at and would have made a
//! hole spanning a midnight roll indistinguishable from a fresh start.

use core::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use quant_core::instrument::Exchange;
use quant_core::time::{Clock, Ts, UtcDate};
use quant_storage::{
    FileHeader, RawWriter, StorageError, StorageResult, WriterOptions, WriterStats,
};

use crate::layout::CaptureTarget;
use crate::record::CaptureRecord;

/// One day, for computing a segment's upper bound.
const ONE_DAY: Duration = Duration::from_secs(86_400);

/// What one sealed capture file ended up containing.
///
/// Produced on every roll and at shutdown. This is what M1.c reports into the
/// metadata database, and it is deliberately derived from the writer rather than
/// from re-reading the file: the numbers are what we *wrote*, so comparing them
/// against a later read of the file is an independent check rather than a
/// tautology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReport {
    pub target: CaptureTarget,
    pub stats: WriterStats,
    pub first_ingest_seq: Option<u64>,
    pub last_ingest_seq: Option<u64>,
}

/// Where capture segments are created and what happens when they are sealed.
///
/// A trait so that rolling behaviour can be tested without a filesystem -- see
/// [`MemoryStore`]. Rolling at a midnight boundary is otherwise a thing you can
/// only observe by waiting until midnight.
pub trait SegmentStore {
    type Sink: Write;

    /// Create the sink for a segment. Must fail rather than truncate if the
    /// target already exists.
    fn create(&mut self, target: &CaptureTarget) -> StorageResult<Self::Sink>;

    /// Called once a segment has been sealed with its trailer.
    fn sealed(&mut self, report: &SegmentReport, sink: Self::Sink) -> StorageResult<()>;
}

/// Segments as real files under a data root.
#[derive(Debug, Clone)]
pub struct FileStore {
    root: PathBuf,
}

impl FileStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl SegmentStore for FileStore {
    type Sink = File;

    fn create(&mut self, target: &CaptureTarget) -> StorageResult<File> {
        fs::create_dir_all(target.directory(&self.root))?;
        // create_new, never create: a session owns its file exclusively. Two
        // recorders sharing a path would interleave frames into nonsense, and
        // failing loudly at open is far better than discovering it in the data.
        Ok(fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target.file(&self.root))?)
    }

    fn sealed(&mut self, _report: &SegmentReport, sink: File) -> StorageResult<()> {
        // The one place an fsync is worth paying for: once, on a file we are
        // finished with. quant-storage never syncs per block because that would
        // cost throughput for a guarantee we do not need against process death.
        sink.sync_all()?;
        Ok(())
    }
}

/// Segments as in-memory buffers. A test double.
///
/// Public because rolling is behaviour that consumers of this crate will want to
/// assert on too, and because a store that can be inspected turns "did it roll at
/// midnight" into an ordinary test.
#[derive(Debug, Default)]
pub struct MemoryStore {
    pub segments: Vec<(CaptureTarget, Vec<u8>)>,
}

impl MemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Partition dates of the sealed segments, in the order they were sealed.
    #[must_use]
    pub fn dates(&self) -> Vec<UtcDate> {
        self.segments.iter().map(|(t, _)| t.date).collect()
    }
}

impl SegmentStore for MemoryStore {
    type Sink = Vec<u8>;

    fn create(&mut self, target: &CaptureTarget) -> StorageResult<Vec<u8>> {
        if self.segments.iter().any(|(t, _)| t == target) {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "segment already sealed",
            )));
        }
        Ok(Vec::new())
    }

    fn sealed(&mut self, report: &SegmentReport, sink: Vec<u8>) -> StorageResult<()> {
        self.segments.push((report.target.clone(), sink));
        Ok(())
    }
}

/// Wraps a store and notifies an observer as each segment is sealed.
///
/// This is the seam that keeps the metadata tier out of this crate. The recorder
/// knows only "call this when a segment is sealed"; that a `Postgres` row gets
/// written is entirely the caller's business, which is what makes a recorder with
/// no database configured a fully functional recorder.
///
/// The observer is called **after** the inner store has sealed the segment, so it
/// never reports a file as durable before the fsync that makes it so.
///
/// It must not block. Whatever it does happens on the writer thread, so a slow
/// observer stalls block sealing, backs up the capture channel and drops market
/// data -- a secondary concern damaging the primary one. Hand work to a channel.
pub struct ObservedStore<S, F> {
    inner: S,
    observe: F,
}

impl<S, F> ObservedStore<S, F> {
    pub fn new(inner: S, observe: F) -> Self {
        Self { inner, observe }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: fmt::Debug, F> fmt::Debug for ObservedStore<S, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedStore")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<S: SegmentStore, F: FnMut(&SegmentReport)> SegmentStore for ObservedStore<S, F> {
    type Sink = S::Sink;

    fn create(&mut self, target: &CaptureTarget) -> StorageResult<Self::Sink> {
        self.inner.create(target)
    }

    fn sealed(&mut self, report: &SegmentReport, sink: Self::Sink) -> StorageResult<()> {
        self.inner.sealed(report, sink)?;
        (self.observe)(report);
        Ok(())
    }
}

/// The segment currently being written.
struct Open<W: Write> {
    target: CaptureTarget,
    /// Cached day bounds, so the common path is one integer comparison rather
    /// than a calendar conversion per message.
    day_start: Ts,
    day_end: Ts,
    writer: RawWriter<W>,
    first_ingest_seq: Option<u64>,
}

/// One capture session: a sequence of day-partitioned segments for one instrument.
pub struct CaptureSession<S: SegmentStore> {
    store: S,
    exchange: Exchange,
    symbol: String,
    session_id: [u8; 16],
    opts: WriterOptions,
    clock: Arc<dyn Clock>,
    open: Option<Open<S::Sink>>,
    reports: Vec<SegmentReport>,
    rolls: u64,
    backdated: u64,
}

impl<S: SegmentStore> fmt::Debug for CaptureSession<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureSession")
            .field("exchange", &self.exchange)
            .field("symbol", &self.symbol)
            .field("open", &self.open.as_ref().map(|o| &o.target))
            .field("segments_sealed", &self.reports.len())
            .field("rolls", &self.rolls)
            .field("backdated", &self.backdated)
            .finish_non_exhaustive()
    }
}

impl<S: SegmentStore> CaptureSession<S> {
    #[must_use]
    pub fn new(
        store: S,
        exchange: Exchange,
        symbol: impl Into<String>,
        session_id: [u8; 16],
        clock: Arc<dyn Clock>,
        opts: WriterOptions,
    ) -> Self {
        Self {
            store,
            exchange,
            symbol: symbol.into(),
            session_id,
            opts,
            clock,
            open: None,
            reports: Vec::new(),
            rolls: 0,
            backdated: 0,
        }
    }

    /// Write a record, rolling to a new segment first if it belongs to a later day.
    pub fn write(&mut self, record: CaptureRecord) -> StorageResult<()> {
        let ts = record.local_recv_ts();

        match &self.open {
            None => self.open_segment(ts)?,
            Some(open) => {
                if ts >= open.day_end {
                    self.seal()?;
                    self.rolls += 1;
                    self.open_segment(ts)?;
                } else if ts < open.day_start {
                    // The wall clock went backwards across a midnight boundary --
                    // an NTP step, essentially. We do *not* roll backwards: the
                    // previous day's segment is already sealed, and reopening it
                    // would either fail on `create_new` or produce a second file
                    // for a finished day.
                    //
                    // So it goes in the open segment, slightly mis-partitioned,
                    // and is counted. The record's own `local_recv_ts` still tells
                    // the truth, so nothing is lost or altered -- only the
                    // directory it can be found in is off, and a non-zero count
                    // here is the signal to go and look at the host's clock.
                    self.backdated += 1;
                }
            }
        }

        let open = self.open.as_mut().expect("a segment is open");
        let seq = record.ingest_seq();
        open.first_ingest_seq.get_or_insert(seq);
        match record {
            CaptureRecord::Venue {
                local_recv_ts,
                ingest_seq,
                payload,
            } => open
                .writer
                .write_venue_payload(local_recv_ts, ingest_seq, &payload),
            CaptureRecord::Control {
                local_recv_ts,
                ingest_seq,
                record,
            } => open
                .writer
                .write_control(local_recv_ts, ingest_seq, &record),
        }
    }

    /// Seal a segment whose day is over, even with no traffic to trigger it.
    ///
    /// Returns whether anything was sealed. Only closes -- the next record opens
    /// the segment it belongs in, so a day with no data leaves no empty file.
    pub fn roll_if_day_elapsed(&mut self) -> StorageResult<bool> {
        let elapsed = self
            .open
            .as_ref()
            .is_some_and(|open| self.clock.now() >= open.day_end);
        if elapsed {
            self.seal()?;
            self.rolls += 1;
        }
        Ok(elapsed)
    }

    pub fn flush(&mut self) -> StorageResult<()> {
        if let Some(open) = self.open.as_mut() {
            open.writer.flush()?;
        }
        Ok(())
    }

    /// Seal the open segment and finish the session.
    ///
    /// Hands the store back along with the reports, mirroring
    /// [`RawWriter::finish`]: whoever supplied it may still need it, to inspect
    /// what was written or to release something the session should not know about.
    pub fn finish(mut self) -> StorageResult<(Vec<SegmentReport>, S)> {
        self.seal()?;
        Ok((self.reports, self.store))
    }

    fn open_segment(&mut self, ts: Ts) -> StorageResult<()> {
        let day_start = ts.start_of_utc_day();
        let target = CaptureTarget {
            exchange: self.exchange,
            symbol: self.symbol.clone(),
            date: ts.utc_date(),
            session_id: self.session_id,
            // Part resets per day, because the directory is per day. Size-based
            // rolling within a day would advance it.
            part: 0,
        };
        let sink = self.store.create(&target)?;
        let header = FileHeader::new(self.exchange, self.symbol.clone(), self.session_id);
        self.open = Some(Open {
            target,
            day_start,
            day_end: day_start + ONE_DAY,
            writer: RawWriter::create(sink, header, self.opts)?,
            first_ingest_seq: None,
        });
        Ok(())
    }

    fn seal(&mut self) -> StorageResult<()> {
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        let last_ingest_seq = open.writer.last_ingest_seq();
        let (sink, stats) = open.writer.finish()?;
        let report = SegmentReport {
            target: open.target,
            stats,
            first_ingest_seq: open.first_ingest_seq,
            last_ingest_seq,
        };
        self.store.sealed(&report, sink)?;
        self.reports.push(report);
        Ok(())
    }

    /// Segments sealed so far.
    #[must_use]
    pub fn reports(&self) -> &[SegmentReport] {
        &self.reports
    }

    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Day boundaries crossed.
    #[must_use]
    pub const fn rolls(&self) -> u64 {
        self.rolls
    }

    /// Records that arrived stamped before the open segment's day. Non-zero means
    /// the host clock stepped backwards; see [`CaptureSession::write`].
    #[must_use]
    pub const fn backdated_records(&self) -> u64 {
        self.backdated
    }

    /// Path-independent description of the segment currently being written.
    #[must_use]
    pub fn open_target(&self) -> Option<&CaptureTarget> {
        self.open.as_ref().map(|o| &o.target)
    }
}

#[cfg(test)]
mod tests {
    use quant_core::event::GapCause;
    use quant_core::time::ManualClock;
    use quant_storage::{ControlRecord, RawReader};

    use super::*;

    const SESSION: [u8; 16] = [3; 16];

    /// 2026-07-29T23:59:59Z, i.e. one second before a day boundary.
    const BEFORE_MIDNIGHT: i64 = 1_785_369_599;

    fn session(clock: Arc<ManualClock>) -> CaptureSession<MemoryStore> {
        CaptureSession::new(
            MemoryStore::new(),
            Exchange::Binance,
            "BTCUSDT",
            SESSION,
            clock,
            WriterOptions::default(),
        )
    }

    fn venue(ts: Ts, seq: u64) -> CaptureRecord {
        CaptureRecord::Venue {
            local_recv_ts: ts,
            ingest_seq: seq,
            payload: format!(r#"{{"e":"trade","seq":{seq}}}"#).into_bytes(),
        }
    }

    fn read(bytes: &[u8]) -> (Vec<u64>, bool) {
        let mut reader = RawReader::open(bytes).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert_eq!(truncation, None);
        (
            frames.iter().map(|f| f.ingest_seq).collect(),
            reader.is_finalized(),
        )
    }

    #[test]
    fn rolling_follows_the_record_timestamp_not_the_writers_clock() {
        // The clock is frozen well before midnight for the whole test, so any
        // roll that happens can only have come from the records themselves.
        let clock = Arc::new(ManualClock::new(Ts::from_secs(BEFORE_MIDNIGHT)));
        let mut s = session(clock);

        s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT), 1)).unwrap();
        s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT + 1), 2))
            .unwrap();
        s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT + 2), 3))
            .unwrap();

        assert_eq!(s.rolls(), 1);
        let (reports, _) = s.finish().unwrap();
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].target.date.to_string(), "2026-07-29");
        assert_eq!(reports[1].target.date.to_string(), "2026-07-30");
    }

    #[test]
    fn ingest_seq_belongs_to_the_session_not_the_segment() {
        let clock = Arc::new(ManualClock::new(Ts::from_secs(BEFORE_MIDNIGHT)));
        let mut s = session(clock);
        for (offset, seq) in [(0_i64, 1_u64), (0, 2), (1, 3), (2, 4)] {
            s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT + offset), seq))
                .unwrap();
        }
        let (segments, _) = s.finish().unwrap();
        assert_eq!(segments.len(), 2);

        // Sequence numbers run straight through the boundary rather than
        // restarting, so a hole spanning midnight stays visible as a hole.
        assert_eq!(segments[0].first_ingest_seq, Some(1));
        assert_eq!(segments[0].last_ingest_seq, Some(2));
        assert_eq!(segments[1].first_ingest_seq, Some(3));
        assert_eq!(segments[1].last_ingest_seq, Some(4));
        // The second segment continues the first with no discontinuity.
        assert_eq!(
            segments[1].first_ingest_seq.unwrap(),
            segments[0].last_ingest_seq.unwrap() + 1
        );
    }

    #[test]
    fn segments_read_back_complete_with_session_scoped_sequence() {
        let clock = Arc::new(ManualClock::new(Ts::from_secs(BEFORE_MIDNIGHT)));
        let mut store_owner = session(clock);
        for (off, seq) in [(0_i64, 1_u64), (0, 2), (1, 3), (2, 4)] {
            store_owner
                .write(venue(Ts::from_secs(BEFORE_MIDNIGHT + off), seq))
                .unwrap();
        }
        store_owner.seal().unwrap();

        let segments = &store_owner.store().segments;
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].0.date.to_string(), "2026-07-29");
        assert_eq!(segments[1].0.date.to_string(), "2026-07-30");

        let (seqs_a, finalized_a) = read(&segments[0].1);
        let (seqs_b, finalized_b) = read(&segments[1].1);
        assert_eq!(seqs_a, vec![1, 2]);
        assert_eq!(seqs_b, vec![3, 4]);
        assert!(finalized_a && finalized_b, "both segments must be sealed");
    }

    #[test]
    fn an_idle_stream_still_closes_yesterdays_segment() {
        // Otherwise a healthy but quiet symbol leaves a trailerless file, which
        // reads as "killed, or still running" -- indistinguishable from a crash.
        let clock = Arc::new(ManualClock::new(Ts::from_secs(BEFORE_MIDNIGHT)));
        let mut s = session(Arc::clone(&clock));
        s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT), 1)).unwrap();

        assert!(!s.roll_if_day_elapsed().unwrap(), "the day is not over yet");
        clock.set(Ts::from_secs(BEFORE_MIDNIGHT + 5));
        assert!(s.roll_if_day_elapsed().unwrap(), "the day is over");

        assert_eq!(s.reports().len(), 1);
        assert!(s.open_target().is_none());
        let (_, finalized) = read(&s.store().segments[0].1);
        assert!(finalized);
    }

    #[test]
    fn a_day_with_no_data_produces_no_file_at_all() {
        // roll_if_day_elapsed only closes; opening is the next record's job.
        let clock = Arc::new(ManualClock::new(Ts::from_secs(BEFORE_MIDNIGHT)));
        let mut s = session(Arc::clone(&clock));
        s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT), 1)).unwrap();

        // Two whole days pass with nothing arriving.
        clock.set(Ts::from_secs(BEFORE_MIDNIGHT + 2 * 86_400));
        assert!(s.roll_if_day_elapsed().unwrap());
        assert!(!s.roll_if_day_elapsed().unwrap(), "nothing left to close");

        let (reports, _) = s.finish().unwrap();
        assert_eq!(reports.len(), 1, "no empty segment for the skipped days");
        assert_eq!(reports[0].target.date.to_string(), "2026-07-29");
    }

    #[test]
    fn a_backdated_record_is_counted_rather_than_silently_repartitioned() {
        let clock = Arc::new(ManualClock::new(Ts::from_secs(BEFORE_MIDNIGHT)));
        let mut s = session(clock);
        // Cross into the new day, then have the host clock step backwards.
        s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT + 2), 1))
            .unwrap();
        s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT - 10), 2))
            .unwrap();

        assert_eq!(s.backdated_records(), 1);
        assert_eq!(s.rolls(), 0, "we never roll backwards");
        let (reports, _) = s.finish().unwrap();
        assert_eq!(reports.len(), 1);
        // Nothing was dropped: the record is present, with its true timestamp.
        assert_eq!(reports[0].stats.frames, 2);
    }

    #[test]
    fn control_records_roll_like_any_other() {
        let clock = Arc::new(ManualClock::new(Ts::from_secs(BEFORE_MIDNIGHT)));
        let mut s = session(clock);
        s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT), 1)).unwrap();
        s.write(CaptureRecord::Control {
            local_recv_ts: Ts::from_secs(BEFORE_MIDNIGHT + 2),
            ingest_seq: 2,
            record: ControlRecord::Gap {
                cause: GapCause::RecorderRestart,
                exchange_ts: Ts::from_secs(BEFORE_MIDNIGHT + 2),
                last_good_ts: Ts::from_secs(BEFORE_MIDNIGHT),
            },
        })
        .unwrap();
        assert_eq!(s.rolls(), 1);
        let (reports, _) = s.finish().unwrap();
        assert_eq!(reports[1].stats.frames, 1);
    }

    #[test]
    fn a_store_refuses_to_reuse_a_target() {
        // §5: one file per capture session. Two writers on one path would
        // interleave frames into a file whose sequence is nonsense.
        let clock = Arc::new(ManualClock::new(Ts::from_secs(BEFORE_MIDNIGHT)));
        let mut s = session(clock);
        s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT), 1)).unwrap();
        s.seal().unwrap();
        // Re-opening the same day must fail rather than truncate.
        assert!(s.write(venue(Ts::from_secs(BEFORE_MIDNIGHT), 2)).is_err());
    }
}
