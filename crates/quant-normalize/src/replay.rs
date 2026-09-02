//! One capture session, back out as an ordered stream of [`MarketEvent`]s.
//!
//! # Why an iterator of events, and not a program that checks a book
//!
//! The book is the consumer that happens to exist first. The Parquet writer at
//! M2.d and the engine's `HistoricalSource` at M3 want the identical stream, and
//! if this were written as "replay a session and validate the book", both would
//! have to take it apart to get at the events. So the replay's job stops at
//! producing events in order; what to do with them is somebody else's.
//!
//! # Why a segment boundary is not a stream boundary
//!
//! This is the whole point of M2.c. `ingest_seq` spans a *session*, and so does
//! the venue's update-id chain, so crossing from one UTC day's file into the next
//! must not reset anything: no re-anchoring, no re-buffering, no fresh book.
//! Replaying files one at a time is what made days 2 through 8 of the acceptance
//! capture drop tens of thousands of deltas each while waiting for an hourly
//! snapshot they did not need. A session is one stream that happens to be stored
//! in pieces.
//!
//! # Why the archive's own discontinuities are events too
//!
//! A segment that will not open, a file whose tail was lost, a hole in
//! `ingest_seq` — each means the stream we are handing downstream is not the
//! stream that was recorded. Skipping quietly to the next readable frame would
//! produce a book that *looks* continuous across data we never read, which is
//! precisely the failure `MarketEvent::Gap` exists to prevent at capture time.
//!
//! They are [`Break`]s rather than synthesized `Gap`s on purpose. `GapCause` is a
//! statement about what happened at the venue or in the recorder, and it is
//! persisted in the immutable tier; "I could not read this file just now" is a
//! statement about this replay, and inventing a fifth cause for it would put a
//! replay-time condition into a capture-time contract. Downstream they mean the
//! same thing — invalidate — and [`crate::replay_session`] is where that is
//! remembered, so no caller has to.
//!
//! # Why a hole always breaks, even where a gap record follows
//!
//! The recorder consumes a sequence number for every message it drops, so the
//! width of a hole *is* the count of what was lost, and it then writes a
//! `Gap{LocalOverflow}` record — which arrives *after* the hole it describes.
//! Waiting to see whether an explanation turns up would mean handing out events
//! across a known discontinuity in the hope of being forgiven. Breaking
//! immediately and letting the gap record arrive into an already-invalid book
//! costs nothing: invalidating twice is invalidating once.

use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use quant_core::event::MarketEvent;
use quant_core::instrument::{Exchange, InstrumentId};
use quant_core::time::UtcDate;
use quant_recorder::{CatalogEntry, SessionFiles};
use quant_storage::{FrameKind, RawFrame, RawReader, Truncation};

/// What the replay hands out, in order.
#[derive(Debug, Clone)]
pub enum ReplayItem {
    /// A market event, exactly as recorded.
    Event(MarketEvent),
    /// The archive is discontinuous here. See the module docs.
    Break(Break),
}

/// A discontinuity in the archive itself, as opposed to one the venue or the
/// recorder reported at the time.
#[derive(Debug, Clone)]
pub struct Break {
    pub kind: BreakKind,
    /// The segment being read when it happened.
    pub segment: PathBuf,
    /// Last `ingest_seq` successfully delivered before it, if any.
    pub after_seq: Option<u64>,
}

/// Why the replay could not carry on continuously.
#[derive(Debug, Clone)]
pub enum BreakKind {
    /// The file would not open, or a block would not decode.
    UnreadableSegment(String),
    /// The header disagrees with the session we are replaying. Refused rather
    /// than read: a file from another session has an unrelated `ingest_seq`
    /// space, and splicing it in would look like an enormous hole or, worse,
    /// like continuity.
    WrongSession(String),
    /// The file ended mid-write. Routine as the last segment of a killed
    /// session; a real loss anywhere else.
    Torn(Truncation),
    /// `ingest_seq` was not contiguous: messages were lost between these two.
    SequenceHole { expected: u64, found: u64 },
    /// A recorded payload did not parse. Loud by invariant 5 — a malformed
    /// price means our model of the venue is wrong.
    ParseFailure(String),
    /// No adapter in this build knows how to read this venue's bytes.
    UnsupportedVenue(Exchange),
}

impl core::fmt::Display for Break {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match &self.kind {
            BreakKind::UnreadableSegment(e) => write!(f, "unreadable segment: {e}"),
            BreakKind::WrongSession(e) => write!(f, "file belongs elsewhere: {e}"),
            BreakKind::Torn(t) => write!(f, "torn segment: {t}"),
            BreakKind::SequenceHole { expected, found } => write!(
                f,
                "sequence hole: expected ingest_seq {expected}, found {found} ({} lost)",
                found.saturating_sub(*expected)
            ),
            BreakKind::ParseFailure(e) => write!(f, "parse failure: {e}"),
            BreakKind::UnsupportedVenue(x) => write!(f, "no adapter for venue {x}"),
        }?;
        write!(f, " [{}]", self.segment.display())
    }
}

/// What the replay read, regardless of what anyone did with it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReplayStats {
    pub segments_read: u64,
    pub frames: u64,
    pub events: u64,
    pub trades: u64,
    pub deltas: u64,
    pub snapshots: u64,
    pub gaps: u64,
    /// Frames whose payload we recognise as a message type we do not model.
    /// Not a failure: `quant-binance`'s parsers are strict about the fields they
    /// claim and tolerant of everything else, so a new venue message type cannot
    /// make a recorded file unreadable.
    pub ignored: u64,
    pub breaks: u64,
}

/// An open segment.
#[derive(Debug)]
struct Open {
    path: PathBuf,
    reader: RawReader<BufReader<File>>,
}

/// One session's segments, joined, as a stream of [`ReplayItem`]s.
#[derive(Debug)]
pub struct SessionReplay {
    exchange: Exchange,
    symbol: String,
    session_id: [u8; 16],
    instrument: InstrumentId,
    remaining: std::vec::IntoIter<CatalogEntry>,
    current: Option<Open>,
    /// Items ready to hand out. A single frame can produce a break *and* an
    /// event, in that order, so there has to be somewhere to keep the second.
    pending: VecDeque<ReplayItem>,
    last_seq: Option<u64>,
    /// The date partition the most recently delivered event was read from.
    ///
    /// Exposed because the normalized tier files events under the day the *raw*
    /// tier filed them under, rather than re-deriving it from `local_recv_ts`.
    /// Those agree except where the recorder's "never roll backwards" rule
    /// applies — a record stamped before the open segment's day, after an NTP
    /// step, is written to the open segment on purpose. Re-deriving would file
    /// it elsewhere and the two tiers would stop lining up.
    segment_date: Option<UtcDate>,
    stats: ReplayStats,
    done: bool,
}

impl SessionReplay {
    /// Open a session for replay.
    ///
    /// `instrument` is supplied by the caller rather than derived here: a capture
    /// file records the venue's identity for the instrument, not its tick size or
    /// lot size, and M0's registry is where identity is reattached from
    /// `(exchange, symbol)`. Nothing in a replay needs the filters; M8 will.
    #[must_use]
    pub fn open(files: &SessionFiles, instrument: InstrumentId) -> Self {
        let mut replay = Self {
            exchange: files.exchange,
            symbol: files.symbol.clone(),
            session_id: files.session_id,
            instrument,
            remaining: files.segments.clone().into_iter(),
            current: None,
            pending: VecDeque::new(),
            last_seq: None,
            segment_date: None,
            stats: ReplayStats::default(),
            done: false,
        };
        if !supported(files.exchange) {
            // Reported, not skipped, and reported once: a capture from a venue
            // this build cannot read is a fact about the build, and staying
            // silent about it would let a partial normalization look complete.
            // Same stance `quant-verify` takes with an unknown venue.
            replay.pending.push_back(ReplayItem::Break(Break {
                kind: BreakKind::UnsupportedVenue(files.exchange),
                segment: files
                    .segments
                    .first()
                    .map_or_else(PathBuf::new, |s| s.path.clone()),
                after_seq: None,
            }));
            replay.stats.breaks += 1;
            replay.done = true;
        }
        replay
    }

    #[must_use]
    pub const fn stats(&self) -> ReplayStats {
        self.stats
    }

    #[must_use]
    pub const fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    /// Date partition of the segment the last delivered event came from.
    ///
    /// Valid immediately after [`Iterator::next`] returns a
    /// [`ReplayItem::Event`]; see the field's docs for why a caller wants it.
    #[must_use]
    pub const fn segment_date(&self) -> Option<UtcDate> {
        self.segment_date
    }

    /// Record a break and count it.
    fn brk(&mut self, kind: BreakKind, segment: PathBuf) {
        self.stats.breaks += 1;
        self.pending.push_back(ReplayItem::Break(Break {
            kind,
            segment,
            after_seq: self.last_seq,
        }));
    }

    /// Open the next segment. Returns false when there are none left.
    fn advance(&mut self) -> bool {
        let Some(entry) = self.remaining.next() else {
            return false;
        };
        let path = entry.path.clone();
        match File::open(&path).map_err(|e| e.to_string()).and_then(|f| {
            RawReader::open(BufReader::new(f)).map_err(|e| format!("cannot open: {e}"))
        }) {
            Ok(reader) => {
                let header = reader.header();
                if header.session_id != self.session_id
                    || header.exchange != self.exchange
                    || header.symbol != self.symbol
                {
                    self.brk(
                        BreakKind::WrongSession(format!(
                            "header names {} {} session {:02x?}",
                            header.exchange,
                            header.symbol,
                            &header.session_id[..4]
                        )),
                        path,
                    );
                } else {
                    self.stats.segments_read += 1;
                    self.segment_date = Some(entry.target.date);
                    self.current = Some(Open { path, reader });
                }
            }
            Err(e) => self.brk(BreakKind::UnreadableSegment(e), path),
        }
        true
    }

    /// Read one frame from the open segment and turn it into pending items.
    fn step(&mut self) {
        let (path, outcome) = {
            let open = self.current.as_mut().expect("caller checked");
            let outcome = match open.reader.next_frame() {
                None => Frame::End(open.reader.truncation()),
                Some(Err(e)) => Frame::Failed(e.to_string()),
                Some(Ok(frame)) => Frame::Got(frame),
            };
            (open.path.clone(), outcome)
        };

        match outcome {
            Frame::End(truncation) => {
                self.current = None;
                if let Some(t) = truncation {
                    self.brk(BreakKind::Torn(t), path);
                }
            }
            Frame::Failed(e) => {
                self.current = None;
                self.brk(BreakKind::UnreadableSegment(e), path);
            }
            Frame::Got(frame) => self.deliver(&frame, path),
        }
    }

    /// Sequence-check a frame, decode it, and queue what it produced.
    fn deliver(&mut self, frame: &RawFrame, path: PathBuf) {
        self.stats.frames += 1;
        if let Some(last) = self.last_seq {
            let expected = last.saturating_add(1);
            if frame.ingest_seq != expected {
                self.brk(
                    BreakKind::SequenceHole {
                        expected,
                        found: frame.ingest_seq,
                    },
                    path.clone(),
                );
            }
        }
        self.last_seq = Some(frame.ingest_seq);

        match decode(self.exchange, frame, self.instrument) {
            Ok(Some(event)) => {
                self.stats.events += 1;
                match event {
                    MarketEvent::Trade(_) => self.stats.trades += 1,
                    MarketEvent::BookDelta(_) => self.stats.deltas += 1,
                    MarketEvent::BookSnapshot(_) => self.stats.snapshots += 1,
                    MarketEvent::Gap(_) => self.stats.gaps += 1,
                }
                self.pending.push_back(ReplayItem::Event(event));
            }
            Ok(None) => self.stats.ignored += 1,
            Err(e) => self.brk(BreakKind::ParseFailure(e), path),
        }
    }
}

/// What one read of the open segment produced.
enum Frame {
    Got(RawFrame),
    End(Option<Truncation>),
    Failed(String),
}

impl Iterator for SessionReplay {
    type Item = ReplayItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Some(item);
            }
            if self.done {
                return None;
            }
            if self.current.is_none() {
                if !self.advance() {
                    self.done = true;
                }
                continue;
            }
            self.step();
        }
    }
}

/// Whether an adapter in this build can read this venue's bytes.
const fn supported(exchange: Exchange) -> bool {
    matches!(exchange, Exchange::Binance)
}

/// Venue dispatch: raw bytes to an event.
///
/// A `match` rather than a trait, deliberately, and for the same reason
/// `quant-verify` has no venue abstraction: a trait designed from one
/// implementation encodes one venue's assumptions and calls them universal.
/// A second adapter arrives, this match gains an arm, and the shape of the trait
/// that should exist becomes visible from two real examples instead of one.
fn decode(
    exchange: Exchange,
    frame: &RawFrame,
    instrument: InstrumentId,
) -> Result<Option<MarketEvent>, String> {
    match exchange {
        Exchange::Binance => decode_binance(frame, instrument),
        other => Err(format!("no adapter for venue {other}")),
    }
}

fn decode_binance(
    frame: &RawFrame,
    instrument: InstrumentId,
) -> Result<Option<MarketEvent>, String> {
    match frame.kind {
        FrameKind::VenuePayload => quant_binance::parse_stream_message(
            &frame.payload,
            instrument,
            frame.local_recv_ts,
            frame.ingest_seq,
        )
        .map_err(|e| e.to_string()),
        FrameKind::VenueSnapshot => quant_binance::parse_snapshot(
            &frame.payload,
            instrument,
            frame.local_recv_ts,
            frame.ingest_seq,
        )
        .map(|s| Some(MarketEvent::BookSnapshot(s)))
        .map_err(|e| e.to_string()),
        // Control frames carry the recorder's own account of what happened, and
        // a gap among them is what invalidates a book. A replay that filtered
        // them out would reconstruct straight through a period of blindness.
        FrameKind::Control => frame.control().map_err(|e| e.to_string()).map(|c| {
            c.and_then(|c| c.into_market_event(instrument, frame.local_recv_ts, frame.ingest_seq))
        }),
    }
}
