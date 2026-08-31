//! The checks themselves, over one session's segments joined in order.
//!
//! # Why this is a streaming state machine
//!
//! A seven-day capture of a liquid symbol is hundreds of millions of frames and
//! tens of gigabytes. Reading a session into memory to check it is not an option,
//! so every check here is expressible as a fold over the frame stream carrying a
//! few words of state. That constraint is not a hardship -- it is what the
//! properties actually need, because all of them are statements about *adjacent*
//! records.
//!
//! # The four properties, and what explains a violation
//!
//! Each check is one-directional on purpose: it looks for a discontinuity and then
//! asks whether the file already explains it. Explanations are never inferred.
//!
//! | Property | Violated when | Explained by |
//! |---|---|---|
//! | `ingest_seq` contiguity | a number is skipped | the frame *immediately after* the hole is a gap record |
//! | depth update-id chain | `U != previous u + 1` | any gap record between the two deltas |
//! | book anchoring | a connection episode carries deltas and no snapshot | a snapshot anywhere in that episode, or a recorded `SnapshotFailed{Resync}` |
//! | file closure | no trailer | being the last segment, i.e. still open or killed |
//!
//! The first deserves its precision. A dropped message consumes its sequence
//! number at ingress and the gap record is written by the *next* successful
//! enqueue, so on disk the gap frame sits immediately after the hole it describes.
//! Checking merely that "a gap exists somewhere in the session" would pass a file
//! with one disconnect and a thousand unexplained holes.
//!
//! The third deserves an explanation, because the obvious rule is wrong and this
//! verifier shipped it once. "Every delta must be preceded by a snapshot" flags
//! every healthy capture, because the snapshot is fetched *concurrently* with the
//! drain and a handful of deltas legitimately arrive before it lands. That is not a
//! defect: Binance's book algorithm discards deltas whose `u` is at or below the
//! snapshot's `lastUpdateId` and bridges the one that straddles it, so a snapshot
//! anchors the deltas *around* it, not merely the ones after it.
//!
//! The unit is therefore the **episode**: one connection's worth of stream, bounded
//! by the gaps that begin and end it. An episode with deltas needs an anchor
//! somewhere inside it; an episode with no deltas needs nothing, which is the case
//! a live test turned up when a connection died before delivering anything.

use std::fs::File;
use std::io::BufReader;

use quant_core::event::GapCause;
use quant_core::instrument::Exchange;
use quant_storage::{ControlRecord, FrameKind, RawFrame, RawReader, SnapshotPurpose};

use crate::discover::{Segment, Session};
use crate::finding::{code, Report, Where};

/// Whether the book can be built for the episode in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Anchor {
    /// No snapshot yet. Not a defect on its own -- the episode may still get one.
    Missing,
    /// A snapshot was recorded.
    Present,
    /// No snapshot, but the recorder said so. Still no book, but honestly so --
    /// which is the difference between a known hole and a silent one.
    Failed,
}

/// One connection's worth of stream, bounded by the gaps that begin and end it.
///
/// The unit of the anchoring check. See the module docs for why it is not the
/// individual delta.
struct Episode {
    /// Where it began, so a finding points at something findable.
    start_seq: u64,
    deltas: u64,
    anchor: Anchor,
    /// Deltas seen before this episode's first anchor.
    ///
    /// Not lost, and an earlier version of this comment said they were. M2's book
    /// builder showed what actually happens to them: the snapshot arrives *later
    /// in the stream* than the deltas it supersedes, so a replay buffers them and,
    /// once anchored, discards the ones the snapshot accounts for and applies the
    /// rest. Both halves are needed -- dropping them leaves a hole at the start of
    /// every resync.
    ///
    /// Still worth counting: a number that climbs means snapshots are landing late,
    /// and the buffer that holds these is bounded.
    deltas_before_anchor: u64,
}

impl Episode {
    const fn new(start_seq: u64) -> Self {
        Self {
            start_seq,
            deltas: 0,
            anchor: Anchor::Missing,
            deltas_before_anchor: 0,
        }
    }
}

/// Everything carried across frames and across segment boundaries.
struct State {
    /// Session-scoped, which is exactly why segments must be joined.
    last_seq: Option<u64>,
    /// The venue's own chain position.
    last_final_update_id: Option<u64>,
    /// Whether a gap record has appeared since the previous depth delta, and so
    /// could explain a break in the chain.
    gap_since_delta: bool,
    episode: Episode,
    /// Set by a local overflow, cleared by the next anchor. See the note at its
    /// use: the recorder does not currently re-snapshot after dropping messages.
    overflow_unanchored: bool,
    /// Binance-specific checks only run for Binance captures.
    venue_understood: bool,
}

impl State {
    fn new(venue_understood: bool) -> Self {
        Self {
            last_seq: None,
            last_final_update_id: None,
            gap_since_delta: false,
            episode: Episode::new(0),
            overflow_unanchored: false,
            venue_understood,
        }
    }

    /// Whether nothing has been seen yet.
    ///
    /// Derived from `last_seq` rather than tracked separately: every frame sets
    /// it, so a second flag would be a second source of truth for one fact.
    const fn is_at_session_start(&self) -> bool {
        self.last_seq.is_none()
    }
}

/// What one segment turned out to hold.
///
/// Returned so `--reconcile` can compare it against the index without a second
/// pass: the frame count is the one number both the file's trailer and the
/// `capture_segments` row claim independently, which makes it the cross-check
/// worth having.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentOutcome {
    pub path: String,
    pub frames: u64,
    /// What the trailer said, if the file was closed.
    pub declared_frames: Option<u64>,
}

/// Verify one session end to end, accumulating into `report`.
pub fn verify(session: &Session, report: &mut Report) -> Vec<SegmentOutcome> {
    let venue_understood = session.exchange == Exchange::Binance;
    if !venue_understood {
        report.warn(
            code::VENUE_NOT_SUPPORTED,
            Where::session(session.session_id),
            format!(
                "{}: update-id chain and anchoring are not checked, because this \
                 build does not know how {} sequences its messages",
                session.exchange, session.exchange
            ),
        );
    }

    let mut state = State::new(venue_understood);
    report.totals.sessions += 1;

    let last_index = session.segments.len().saturating_sub(1);
    let mut outcomes = Vec::with_capacity(session.segments.len());
    for (index, segment) in session.segments.iter().enumerate() {
        outcomes.push(verify_segment(
            segment,
            session,
            index == last_index,
            &mut state,
            report,
        ));
    }

    if state.is_at_session_start() {
        // An empty session is not a defect -- a recorder started and stopped
        // without a message would produce one -- but it is worth saying, because
        // "verified 4 sessions, all clean" over four empty files is a lie of
        // omission.
        report.warn(
            code::NO_RESTART_MARKER,
            Where::session(session.session_id),
            "session contains no frames at all",
        );
    }

    // The last episode has no closing gap, so it is closed here.
    close_episode(&state.episode, session.session_id, report);

    outcomes.into_iter().flatten().collect()
}

fn verify_segment(
    segment: &Segment,
    session: &Session,
    is_last: bool,
    state: &mut State,
    report: &mut Report,
) -> Option<SegmentOutcome> {
    let at = || Where::file(segment.display()).in_session(session.session_id);

    let file = match File::open(&segment.path) {
        Ok(file) => file,
        Err(e) => {
            report.error(code::UNREADABLE_FILE, at(), e.to_string());
            return None;
        }
    };
    // Buffered: the reader issues one read per block header and one per block
    // body, and over a seven-day session that is a lot of small syscalls.
    let mut reader = match RawReader::open(BufReader::new(file)) {
        Ok(reader) => reader,
        Err(e) => {
            report.error(code::UNREADABLE_FILE, at(), e.to_string());
            return None;
        }
    };

    report.totals.segments += 1;
    let mut misfiled = 0_u64;
    let mut frames = 0_u64;

    while let Some(frame) = reader.next_frame() {
        let frame = match frame {
            Ok(frame) => frame,
            Err(e) => {
                // Not a torn tail: the block's checksum passed, so these are
                // provably our bytes and the writer and reader disagree.
                report.error(
                    code::CORRUPTION,
                    at().at_seq(state.last_seq.unwrap_or(0)),
                    format!("frame decode failed: {e}"),
                );
                break;
            }
        };
        report.totals.frames += 1;
        frames += 1;

        // A record whose own timestamp falls outside the partition it is filed
        // under. The recorder counts these as `backdated_records` and does it on
        // purpose -- rolling backwards would reopen a sealed day -- so this is a
        // signal about the host clock, not about the data.
        if frame.local_recv_ts.utc_date() != segment.target.date {
            misfiled += 1;
        }

        check_frame(&frame, segment, session, state, report);
    }

    if misfiled > 0 {
        report.warn(
            code::MISFILED_RECORDS,
            at(),
            format!(
                "{misfiled} records carry a timestamp outside date={}; \
                 the data is intact but the host clock stepped",
                segment.target.date
            ),
        );
    }

    check_closure(&reader, at(), is_last, report);

    Some(SegmentOutcome {
        path: segment.display(),
        frames,
        declared_frames: reader.trailer().map(|t| t.frames),
    })
}

/// Truncation and trailer: whether the file accounts for itself.
fn check_closure<R: std::io::Read>(
    reader: &RawReader<R>,
    at: Where,
    is_last: bool,
    report: &mut Report,
) {
    if let Some(truncation) = reader.truncation() {
        if truncation.reason.is_corruption() {
            report.error(
                code::CORRUPTION,
                at.clone(),
                format!("{truncation} -- the bytes on disk are not the bytes we wrote"),
            );
        } else if is_last {
            report.warn(
                code::TORN_TAIL,
                at.clone(),
                format!("{truncation} (expected for a file that was killed or is still open)"),
            );
        } else {
            report.error(
                code::TORN_TAIL,
                at.clone(),
                format!(
                    "{truncation} -- a torn tail before the last segment means this \
                     file was abandoned while the session carried on"
                ),
            );
        }
    }

    if reader.trailer().is_none() {
        if is_last {
            report.warn(
                code::NO_TRAILER,
                at,
                "writer never declared this file closed: killed, or still recording",
            );
        } else {
            report.error(
                code::NO_TRAILER,
                at,
                "an unsealed segment followed by another one: the writer moved on \
                 without closing this file",
            );
        }
    }
}

fn check_frame(
    frame: &RawFrame,
    segment: &Segment,
    session: &Session,
    state: &mut State,
    report: &mut Report,
) {
    let at = || {
        Where::file(segment.display())
            .in_session(session.session_id)
            .at_seq(frame.ingest_seq)
    };

    // Decoded before the sequence check, because whether this frame is a gap
    // record is exactly what decides if a hole immediately before it is explained.
    let control = if frame.kind == FrameKind::Control {
        match frame.control() {
            Ok(control) => control,
            Err(e) => {
                report.error(
                    code::PAYLOAD_UNREADABLE,
                    at(),
                    format!("control record will not decode: {e}"),
                );
                None
            }
        }
    } else {
        None
    };
    let is_gap = matches!(control, Some(ControlRecord::Gap { .. }));

    // Read before `check_sequence`, which is what sets `last_seq`.
    let is_first_frame = state.is_at_session_start();
    check_sequence(frame, is_gap, &at, state, report);

    let opens_with_restart = matches!(
        control,
        Some(ControlRecord::Gap {
            cause: GapCause::RecorderRestart,
            ..
        })
    );
    if is_first_frame && !opens_with_restart {
        // The recorder writes one first in every session precisely so that a
        // capture resuming after a crash cannot silently abut the previous one and
        // look like continuous coverage.
        report.warn(
            code::NO_RESTART_MARKER,
            at(),
            "session does not open with a RecorderRestart gap",
        );
    }

    match frame.kind {
        FrameKind::Control => {
            check_control(control, frame, session.session_id, &at, state, report);
        }
        FrameKind::VenueSnapshot => check_snapshot(frame, &at, state, report),
        FrameKind::VenuePayload => check_stream_payload(frame, &at, state, report),
    }
}

fn check_sequence(
    frame: &RawFrame,
    is_gap: bool,
    at: &impl Fn() -> Where,
    state: &mut State,
    report: &mut Report,
) {
    if let Some(previous) = state.last_seq {
        if frame.ingest_seq <= previous {
            // `RawReader` refuses this within one file, so reaching it means two
            // segments of one session overlap -- which no writer path can produce
            // and which would make the session's ordering meaningless.
            report.error(
                code::SEQUENCE_BACKWARDS,
                at(),
                format!("ingest_seq {} follows {previous}", frame.ingest_seq),
            );
        } else {
            let skipped = frame.ingest_seq - previous - 1;
            if skipped > 0 {
                report.totals.missing_messages += skipped;
                if !is_gap {
                    report.error(
                        code::UNEXPLAINED_HOLE,
                        at(),
                        format!(
                            "{skipped} messages missing between {previous} and {}, and the \
                             frame after the hole is a {:?} rather than a gap record",
                            frame.ingest_seq, frame.kind
                        ),
                    );
                }
            }
        }
    }
    state.last_seq = Some(frame.ingest_seq);
}

fn check_control(
    control: Option<ControlRecord>,
    frame: &RawFrame,
    session_id: [u8; 16],
    at: &impl Fn() -> Where,
    state: &mut State,
    report: &mut Report,
) {
    match control {
        Some(ControlRecord::Gap { cause, .. }) => {
            report.totals.gaps += 1;
            state.gap_since_delta = true;
            match cause {
                // Both end one connection's stream and begin another's: a reconnect
                // gets a fresh delta stream that needs its own anchor, and a restart
                // has never had one.
                GapCause::Disconnect | GapCause::RecorderRestart => {
                    close_episode(&state.episode, session_id, report);
                    state.episode = Episode::new(frame.ingest_seq);
                }
                GapCause::LocalOverflow => {
                    report.warn(
                        code::LOCAL_OVERFLOW,
                        at(),
                        "recorder dropped messages: the channel or the disk needs sizing",
                    );
                    state.overflow_unanchored = true;
                }
                GapCause::SequenceGap => {}
            }
        }
        Some(ControlRecord::SnapshotFailed {
            purpose,
            reason,
            attempts,
        }) => {
            report.warn(
                code::SNAPSHOT_FAILED,
                at(),
                format!("{purpose:?} snapshot unavailable ({reason:?}) after {attempts} attempts"),
            );
            if purpose == SnapshotPurpose::Resync && state.episode.anchor == Anchor::Missing {
                // Still no book, but the file says so. That is the whole reason this
                // record exists rather than a silence -- and it must not downgrade a
                // snapshot this episode already got.
                state.episode.anchor = Anchor::Failed;
            }
        }
        None => {}
    }
}

/// Decide whether a finished episode's deltas can ever be turned into a book.
fn close_episode(episode: &Episode, session_id: [u8; 16], report: &mut Report) {
    report.totals.deltas_before_anchor += episode.deltas_before_anchor;

    // No deltas, nothing to anchor. This is the case a live test turned up: a
    // connection that dies before delivering anything legitimately has no snapshot
    // and no failure record, and demanding one would fail good captures.
    if episode.deltas == 0 || episode.anchor != Anchor::Missing {
        return;
    }
    report.error(
        code::UNANCHORED_DELTAS,
        Where::session(session_id).at_seq(episode.start_seq),
        format!(
            "{} depth deltas in the connection episode starting at seq {}, with no \
             snapshot anywhere in it and no record of one being attempted: these \
             deltas can never be applied to a book",
            episode.deltas, episode.start_seq
        ),
    );
}

fn check_snapshot(
    frame: &RawFrame,
    at: &impl Fn() -> Where,
    state: &mut State,
    report: &mut Report,
) {
    report.totals.snapshots += 1;
    state.episode.anchor = Anchor::Present;
    state.overflow_unanchored = false;

    if !state.venue_understood {
        return;
    }
    // "The venue answered 200" and "these bytes are a book" are different claims,
    // and this is the one that matters a year from now.
    if let Err(e) = quant_binance::snapshot_last_update_id(&frame.payload) {
        report.error(
            code::SNAPSHOT_UNREADABLE,
            at(),
            format!("frame is recorded as a book snapshot but is not one: {e}"),
        );
    }
}

fn check_stream_payload(
    frame: &RawFrame,
    at: &impl Fn() -> Where,
    state: &mut State,
    report: &mut Report,
) {
    report.totals.stream_messages += 1;
    if !state.venue_understood {
        return;
    }

    let message = match quant_binance::classify(&frame.payload) {
        Ok(message) => message,
        Err(e) => {
            report.error(code::PAYLOAD_UNREADABLE, at(), e.to_string());
            return;
        }
    };

    match message {
        quant_binance::StreamMessage::Depth {
            first_update_id,
            final_update_id,
        } => {
            report.totals.depth_deltas += 1;
            check_chain(first_update_id, at, state, report);

            state.episode.deltas += 1;
            if state.episode.anchor == Anchor::Missing {
                // Counted, not reported. Whether it matters is only decidable when
                // the episode closes -- a snapshot arriving a moment from now
                // anchors these too.
                state.episode.deltas_before_anchor += 1;
            }
            check_overflow_resumption(at, state, report);

            state.last_final_update_id = Some(final_update_id);
            state.gap_since_delta = false;
        }
        quant_binance::StreamMessage::Trade { .. } => report.totals.trades += 1,
        quant_binance::StreamMessage::Other => {}
    }
}

fn check_chain(first_update_id: u64, at: &impl Fn() -> Where, state: &State, report: &mut Report) {
    let Some(previous) = state.last_final_update_id else {
        return;
    };
    if first_update_id == previous + 1 {
        return;
    }
    if state.gap_since_delta {
        // A recorded gap accounts for it. Not narrowed further on purpose: a
        // reconnect legitimately restarts the venue's numbering wherever it likes,
        // so there is no arithmetic relationship left to check across one.
        return;
    }
    report.error(
        code::CHAIN_BREAK,
        at(),
        format!(
            "depth update ids jump from {previous} to {first_update_id} with no \
             recorded gap: {} messages the venue sent are missing from this capture",
            first_update_id.saturating_sub(previous + 1)
        ),
    );
}

/// Deltas resuming after we dropped messages, with no fresh anchor.
///
/// A warning rather than an error, and the distinction is worth stating: dropping
/// messages genuinely does invalidate the book, but the capture is *honest* about
/// it -- there is a `LocalOverflow` gap and a hole in `ingest_seq` accounting for
/// exactly how much. What is missing is a fresh snapshot, and the recorder only
/// fetches one on reconnect, so the book stays unusable until the next hourly
/// anchor. That is a recorder limitation to weigh, not a defect in the file, and
/// making it an error would fail a capture that is telling the whole truth.
fn check_overflow_resumption(at: &impl Fn() -> Where, state: &mut State, report: &mut Report) {
    if !state.overflow_unanchored {
        return;
    }
    // Once per episode, not once per delta.
    state.overflow_unanchored = false;
    report.warn(
        code::UNANCHORED_AFTER_OVERFLOW,
        at(),
        "deltas resumed after a local overflow with no fresh snapshot: the book is \
         unusable until the next periodic anchor",
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::time::{Ts, UtcDate};
    use quant_recorder::CaptureTarget;
    use quant_storage::{ControlRecord, FileHeader, RawWriter, SnapshotFailure, WriterOptions};
    use std::io::Write as _;
    use std::path::PathBuf;

    const SESSION: [u8; 16] = [0x5e; 16];
    /// Exactly 2026-07-29T00:00:00Z, so records land in the partition they are
    /// filed under and the misfiled-records check stays quiet.
    const DAY_29: i64 = 1_785_283_200;
    const DAY_30: i64 = DAY_29 + 86_400;

    /// Builds a real capture file frame by frame.
    ///
    /// Deliberately writes through `RawWriter` rather than synthesizing bytes: the
    /// verifier's job is to read what the recorder actually produces, and a test
    /// fixture that bypasses the writer would stop testing that the moment the
    /// format changed.
    struct Capture {
        writer: RawWriter<Vec<u8>>,
        seq: u64,
        ts: i64,
    }

    impl Capture {
        fn new(day_start: i64) -> Self {
            Self {
                writer: RawWriter::create(
                    Vec::new(),
                    FileHeader::new(Exchange::Binance, "BTCUSDT", SESSION),
                    WriterOptions::default(),
                )
                .expect("create"),
                seq: 1,
                // Mid-day, so a few seconds either way stays inside the partition.
                ts: day_start + 3_600,
            }
        }

        /// Continue a session in a new segment: `ingest_seq` carries over, which is
        /// the property that makes cross-file checks possible at all.
        fn continuing(previous_seq: u64, day_start: i64) -> Self {
            let mut next = Self::new(day_start);
            next.seq = previous_seq;
            next
        }

        fn tick(&mut self) -> (Ts, u64) {
            let ts = Ts::from_millis(self.ts * 1_000 + i64::try_from(self.seq).unwrap());
            let seq = self.seq;
            self.seq += 1;
            (ts, seq)
        }

        fn gap(mut self, cause: GapCause) -> Self {
            let (ts, seq) = self.tick();
            self.writer
                .write_control(
                    ts,
                    seq,
                    &ControlRecord::Gap {
                        cause,
                        exchange_ts: ts,
                        last_good_ts: ts,
                    },
                )
                .unwrap();
            self
        }

        fn snapshot_failed(mut self, purpose: SnapshotPurpose) -> Self {
            let (ts, seq) = self.tick();
            self.writer
                .write_control(
                    ts,
                    seq,
                    &ControlRecord::SnapshotFailed {
                        purpose,
                        reason: SnapshotFailure::Status,
                        attempts: 4,
                    },
                )
                .unwrap();
            self
        }

        fn snapshot(mut self, last_update_id: u64) -> Self {
            let (ts, seq) = self.tick();
            let body = format!(r#"{{"lastUpdateId":{last_update_id},"bids":[],"asks":[]}}"#);
            self.writer
                .write_venue_snapshot(ts, seq, body.as_bytes())
                .unwrap();
            self
        }

        /// A snapshot frame holding something that is not a book.
        fn snapshot_of_an_error_document(mut self) -> Self {
            let (ts, seq) = self.tick();
            self.writer
                .write_venue_snapshot(ts, seq, br#"{"code":-1003,"msg":"banned"}"#)
                .unwrap();
            self
        }

        fn delta(mut self, first: u64, final_id: u64) -> Self {
            let (ts, seq) = self.tick();
            let body = format!(
                r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","U":{first},"u":{final_id}}}}}"#
            );
            self.writer
                .write_venue_payload(ts, seq, body.as_bytes())
                .unwrap();
            self
        }

        fn trade(mut self, id: u64) -> Self {
            let (ts, seq) = self.tick();
            let body = format!(r#"{{"stream":"btcusdt@trade","data":{{"e":"trade","t":{id}}}}}"#);
            self.writer
                .write_venue_payload(ts, seq, body.as_bytes())
                .unwrap();
            self
        }

        /// Burn `n` sequence numbers without writing them: exactly what ingress
        /// does when it drops messages.
        fn drop_messages(mut self, n: u64) -> Self {
            self.seq += n;
            self
        }

        fn seal(self) -> (Vec<u8>, u64) {
            let seq = self.seq;
            let (bytes, _) = self.writer.finish().expect("finish");
            (bytes, seq)
        }

        /// Stop without a trailer: what a `SIGKILL` leaves behind.
        ///
        /// Sealed and then cut back by exactly the trailer's length, which is the
        /// same bytes a writer that never reached `finish` would have left. Going
        /// through `finish` first keeps the fixture honest about block framing --
        /// the point is a missing trailer, not a torn block.
        fn abandon(self) -> (Vec<u8>, u64) {
            let (mut bytes, seq) = self.seal();
            bytes.truncate(bytes.len() - quant_storage::TRAILER_LEN);
            (bytes, seq)
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("quant-verify-session-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn target(day: u8, part: u32) -> CaptureTarget {
        CaptureTarget {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            date: UtcDate {
                year: 2026,
                month: 7,
                day,
            },
            session_id: SESSION,
            part,
        }
    }

    /// Materialize segments at their layout paths and verify the whole session.
    fn run(name: &str, segments: &[(CaptureTarget, Vec<u8>)]) -> Report {
        let root = temp_root(name);
        for (target, bytes) in segments {
            let path = target.file(&root);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::File::create(&path)
                .unwrap()
                .write_all(bytes)
                .unwrap();
        }

        let mut report = Report::default();
        let sessions = crate::discover::discover(&root, &mut report);
        assert_eq!(sessions.len(), 1, "fixture should be one session");
        verify(&sessions[0], &mut report);
        report
    }

    /// One segment, the common case.
    fn run_one(name: &str, bytes: Vec<u8>) -> Report {
        run(name, &[(target(29, 0), bytes)])
    }

    fn healthy() -> Capture {
        Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .delta(100, 101)
            .snapshot(101)
            .delta(102, 105)
            .trade(7)
            .delta(106, 106)
    }

    #[test]
    fn a_healthy_capture_is_clean() {
        let (bytes, _) = healthy().seal();
        let report = run_one("healthy", bytes);
        assert!(report.is_clean(), "{:#?}", report.findings());
        assert_eq!(report.findings().len(), 0, "{:#?}", report.findings());
        assert_eq!(report.totals.depth_deltas, 3);
        assert_eq!(report.totals.trades, 1);
        assert_eq!(report.totals.snapshots, 1);
    }

    #[test]
    fn deltas_before_the_snapshot_are_normal_and_not_a_defect() {
        // The bug this verifier shipped once. The snapshot is fetched concurrently
        // with the drain, so deltas legitimately precede it, and Binance's algorithm
        // discards the stale ones. Demanding an anchor *before* each delta flags
        // every healthy capture -- which is worse than having no verifier.
        let (bytes, _) = healthy().seal();
        let report = run_one("before-anchor", bytes);
        assert!(report.is_clean());
        assert_eq!(
            report.totals.deltas_before_anchor, 1,
            "counted, so late snapshots are visible, but not an error"
        );
    }

    #[test]
    fn a_hole_followed_by_a_gap_record_is_explained() {
        let (bytes, _) = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot(1)
            .delta(2, 3)
            .drop_messages(40)
            .gap(GapCause::LocalOverflow)
            .delta(50, 51)
            .seal();
        let report = run_one("hole-explained", bytes);
        assert_eq!(report.count(code::UNEXPLAINED_HOLE), 0);
        assert_eq!(report.totals.missing_messages, 40);
        assert_eq!(
            report.count(code::LOCAL_OVERFLOW),
            1,
            "still worth a warning: it is a capacity problem"
        );
        assert!(report.is_clean(), "{:#?}", report.findings());
    }

    #[test]
    fn a_hole_with_no_gap_record_after_it_is_an_error() {
        // The precise rule: the frame immediately after the hole must be the gap.
        // A capture with one disconnect somewhere and a thousand silent holes would
        // pass a laxer check.
        let (bytes, _) = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot(1)
            .delta(2, 3)
            .drop_messages(12)
            .delta(4, 5)
            .seal();
        let report = run_one("hole-silent", bytes);
        assert_eq!(report.count(code::UNEXPLAINED_HOLE), 1);
        assert_eq!(report.totals.missing_messages, 12);
        assert!(!report.is_clean());
    }

    #[test]
    fn a_break_in_the_venue_chain_with_no_gap_is_an_error() {
        // The check §7 asks for: each message's first_update_id must continue the
        // previous message's final_update_id.
        let (bytes, _) = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot(100)
            .delta(101, 105)
            .delta(200, 201)
            .seal();
        let report = run_one("chain-break", bytes);
        assert_eq!(report.count(code::CHAIN_BREAK), 1);
        assert!(!report.is_clean());
        // ingest_seq is intact: we received everything the socket gave us. The
        // venue's own numbering is what says messages went missing upstream.
        assert_eq!(report.count(code::UNEXPLAINED_HOLE), 0);
        assert_eq!(report.totals.missing_messages, 0);
    }

    #[test]
    fn a_chain_break_across_a_reconnect_is_explained() {
        // A new connection restarts the venue's numbering wherever it likes, so
        // there is no arithmetic left to check across a recorded disconnect.
        let (bytes, _) = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot(100)
            .delta(101, 105)
            .gap(GapCause::Disconnect)
            .snapshot(900)
            .delta(901, 902)
            .seal();
        let report = run_one("chain-reconnect", bytes);
        assert_eq!(report.count(code::CHAIN_BREAK), 0);
        assert!(report.is_clean(), "{:#?}", report.findings());
    }

    #[test]
    fn an_episode_with_deltas_and_no_snapshot_is_an_error() {
        let (bytes, _) = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot(100)
            .delta(101, 102)
            .gap(GapCause::Disconnect)
            // Reconnected, never anchored, and said nothing about it.
            .delta(900, 901)
            .delta(902, 903)
            .seal();
        let report = run_one("episode-unanchored", bytes);
        assert_eq!(report.count(code::UNANCHORED_DELTAS), 1);
        assert!(!report.is_clean());
        let detail = &report
            .findings()
            .iter()
            .find(|f| f.code == code::UNANCHORED_DELTAS)
            .expect("finding")
            .detail;
        assert!(detail.contains('2'), "should say how many deltas: {detail}");
    }

    #[test]
    fn a_recorded_snapshot_failure_explains_a_missing_anchor() {
        // The whole reason SnapshotFailed exists rather than a silence: the deltas
        // are still unusable, but the capture says so, and honest is checkable.
        let (bytes, _) = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot_failed(SnapshotPurpose::Resync)
            .delta(900, 901)
            .seal();
        let report = run_one("failure-explains", bytes);
        assert_eq!(report.count(code::UNANCHORED_DELTAS), 0);
        assert_eq!(report.count(code::SNAPSHOT_FAILED), 1);
        assert!(
            report.is_clean(),
            "a recorded failure is a warning, not an error"
        );
    }

    #[test]
    fn a_failed_periodic_snapshot_does_not_excuse_a_missing_resync() {
        // Only a resync anchors an episode. Treating a periodic failure as an
        // explanation would let a genuinely unanchored episode pass.
        let (bytes, _) = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot_failed(SnapshotPurpose::Periodic)
            .delta(900, 901)
            .seal();
        let report = run_one("periodic-not-excuse", bytes);
        assert_eq!(report.count(code::UNANCHORED_DELTAS), 1);
        assert!(!report.is_clean());
    }

    #[test]
    fn a_disconnect_with_no_deltas_after_it_needs_no_anchor() {
        // Found by a live test: a connection that dies before delivering anything
        // legitimately has neither a snapshot nor a failure record. The rule is
        // "a disconnect followed by deltas needs an anchor", not "every disconnect
        // needs one".
        let (bytes, _) = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot(100)
            .delta(101, 102)
            .gap(GapCause::Disconnect)
            .gap(GapCause::Disconnect)
            .seal();
        let report = run_one("empty-episode", bytes);
        assert_eq!(report.count(code::UNANCHORED_DELTAS), 0);
        assert!(report.is_clean(), "{:#?}", report.findings());
    }

    #[test]
    fn ingest_seq_is_checked_across_the_segment_boundary() {
        // The reason a session is the unit of verification. Verifying files
        // independently cannot see this hole at all, because each file is internally
        // contiguous.
        let first = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot(100)
            .delta(101, 102);
        let (first_bytes, next_seq) = first.seal();

        let (second_bytes, _) = Capture::continuing(next_seq, DAY_30)
            .drop_messages(9)
            .delta(103, 104)
            .seal();

        let report = run(
            "cross-segment",
            &[(target(29, 0), first_bytes), (target(30, 0), second_bytes)],
        );
        assert_eq!(report.totals.segments, 2);
        assert_eq!(
            report.count(code::UNEXPLAINED_HOLE),
            1,
            "a hole straddling midnight is still a hole"
        );
        assert_eq!(report.totals.missing_messages, 9);
    }

    #[test]
    fn the_venue_chain_is_also_followed_across_the_segment_boundary() {
        let first = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot(100)
            .delta(101, 102);
        let (first_bytes, next_seq) = first.seal();
        let (second_bytes, _) = Capture::continuing(next_seq, DAY_30).delta(500, 501).seal();

        let report = run(
            "cross-segment-chain",
            &[(target(29, 0), first_bytes), (target(30, 0), second_bytes)],
        );
        assert_eq!(
            report.count(code::CHAIN_BREAK),
            1,
            "the chain does not restart at a day boundary"
        );
    }

    #[test]
    fn an_unsealed_last_segment_warns_but_an_unsealed_earlier_one_is_an_error() {
        // No trailer on the final file is what a kill or an in-progress capture
        // looks like. No trailer on a file the writer then moved on from is not
        // something any code path produces.
        let (killed, _) = healthy().abandon();
        let report = run_one("killed", killed);
        assert_eq!(report.count(code::NO_TRAILER), 1);
        assert!(
            report.is_clean(),
            "a killed capture is readable up to its last complete frame"
        );

        let first = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot(100)
            .delta(101, 102);
        let (unsealed, next_seq) = first.abandon();
        let (sealed, _) = Capture::continuing(next_seq, DAY_30).delta(103, 104).seal();
        let report = run(
            "unsealed-middle",
            &[(target(29, 0), unsealed), (target(30, 0), sealed)],
        );
        assert_eq!(report.count(code::NO_TRAILER), 1);
        assert!(!report.is_clean(), "{:#?}", report.findings());
    }

    #[test]
    fn a_snapshot_frame_that_is_not_a_book_is_an_error() {
        // The failure this catches: a venue answering 200 with an error document.
        // Recorded as a snapshot it would corrupt a rebuild silently a year later.
        let (bytes, _) = Capture::new(DAY_29)
            .gap(GapCause::RecorderRestart)
            .snapshot_of_an_error_document()
            .delta(101, 102)
            .seal();
        let report = run_one("bad-snapshot", bytes);
        assert_eq!(report.count(code::SNAPSHOT_UNREADABLE), 1);
        assert!(!report.is_clean());
    }

    #[test]
    fn a_session_that_does_not_open_with_a_restart_marker_is_flagged() {
        // The marker exists so a capture resuming after a crash cannot silently
        // abut the previous one and look like continuous coverage.
        let (bytes, _) = Capture::new(DAY_29).snapshot(100).delta(101, 102).seal();
        let report = run_one("no-restart", bytes);
        assert_eq!(report.count(code::NO_RESTART_MARKER), 1);
        assert!(report.is_clean(), "odd, but not a reason to distrust it");
    }

    #[test]
    fn records_filed_under_the_wrong_day_are_reported_without_failing_the_run() {
        // The recorder does this on purpose when the host clock steps backwards:
        // rolling backwards would reopen a sealed day, so the record goes in the
        // open segment and is counted. Nothing is lost, but the clock wants looking
        // at.
        let (bytes, _) = Capture::new(DAY_30)
            .gap(GapCause::RecorderRestart)
            .snapshot(100)
            .delta(101, 102)
            .seal();
        // Filed under the 29th while its records are stamped on the 30th.
        let report = run("misfiled", &[(target(29, 0), bytes)]);
        assert_eq!(report.count(code::MISFILED_RECORDS), 1);
        assert!(report.is_clean());
    }
}
