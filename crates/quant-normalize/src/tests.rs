//! Tests for the session join.
//!
//! These write real capture files through `quant-storage`'s writer, at real
//! layout paths, and read them back through the real catalog. Fixtures assembled
//! in memory would not exercise the thing this slice is about — that a segment
//! boundary is invisible to the reconstruction — because a boundary only exists
//! once the stream is in two files.

use std::path::{Path, PathBuf};

use quant_core::event::GapCause;
use quant_core::instrument::{
    Exchange, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
};
use quant_core::time::{Ts, UtcDate};
use quant_recorder::{catalog, CaptureTarget, SessionFiles};
use quant_storage::{ControlRecord, FileHeader, RawWriter, WriterOptions};

use crate::{replay_session, BreakKind, ReplayItem, SessionReplay};

const SESSION: [u8; 16] = [0x5e; 16];
const SYMBOL: &str = "BTCUSDT";

/// A capture tree under a temp directory, removed when the test ends.
struct Tree {
    root: PathBuf,
}

impl Tree {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!("quant-normalize-{name}"));
        let _ = std::fs::remove_dir_all(&root);
        Self { root }
    }

    fn target(day: u8) -> CaptureTarget {
        CaptureTarget {
            exchange: Exchange::Binance,
            symbol: SYMBOL.to_owned(),
            date: UtcDate {
                year: 2026,
                month: 8,
                day,
            },
            session_id: SESSION,
            part: 0,
        }
    }

    /// Write one segment. `frames` is `(ingest_seq, payload)` in order.
    fn segment(&self, day: u8, frames: &[Frame]) -> PathBuf {
        let path = Self::target(day).file(&self.root);
        std::fs::create_dir_all(path.parent().expect("a file has a parent")).expect("mkdir");
        let file = std::fs::File::create(&path).expect("create");
        let header = FileHeader::new(Exchange::Binance, SYMBOL, SESSION);
        let mut writer =
            RawWriter::create(file, header, WriterOptions::default()).expect("create writer");
        for frame in frames {
            let ts = Ts::from_millis(i64::try_from(frame.seq()).expect("small seq"));
            match frame {
                Frame::Stream { seq, payload } => writer.write_venue_payload(ts, *seq, payload),
                Frame::Snapshot { seq, payload } => writer.write_venue_snapshot(ts, *seq, payload),
                Frame::Gap { seq } => writer.write_control(
                    ts,
                    *seq,
                    &ControlRecord::Gap {
                        cause: GapCause::Disconnect,
                        exchange_ts: ts,
                        last_good_ts: ts,
                    },
                ),
            }
            .expect("write frame");
        }
        writer.finish().expect("finish");
        path
    }

    fn sessions(&self) -> Vec<SessionFiles> {
        catalog(&self.root).sessions
    }

    fn only_session(&self) -> SessionFiles {
        let mut sessions = self.sessions();
        assert_eq!(sessions.len(), 1, "expected exactly one session");
        sessions.remove(0)
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

enum Frame {
    Stream { seq: u64, payload: Vec<u8> },
    Snapshot { seq: u64, payload: Vec<u8> },
    Gap { seq: u64 },
}

impl Frame {
    const fn seq(&self) -> u64 {
        match self {
            Self::Stream { seq, .. } | Self::Snapshot { seq, .. } | Self::Gap { seq } => *seq,
        }
    }
}

/// A `depthUpdate` covering `[first, last]` that moves the best bid.
fn delta(seq: u64, first: u64, last: u64, bid: &str) -> Frame {
    Frame::Stream {
        seq,
        payload: format!(
            r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":{seq},"s":"BTCUSDT","U":{first},"u":{last},"b":[["{bid}","1.00000000"]],"a":[["200.00000000","1.00000000"]]}}}}"#
        )
        .into_bytes(),
    }
}

/// A REST snapshot anchored at `last_update_id`.
fn snapshot(seq: u64, last_update_id: u64) -> Frame {
    Frame::Snapshot {
        seq,
        payload: format!(
            r#"{{"lastUpdateId":{last_update_id},"bids":[["100.00000000","1.00000000"]],"asks":[["200.00000000","1.00000000"]]}}"#
        )
        .into_bytes(),
    }
}

fn instrument() -> InstrumentId {
    let mut registry = InstrumentRegistry::new();
    registry.register(InstrumentDef {
        exchange: Exchange::Binance,
        symbol: SYMBOL.to_owned(),
        base: "BTC".to_owned(),
        quote: "USDT".to_owned(),
        kind: InstrumentKind::Spot,
        tick_size: "0.01".parse().expect("tick"),
        lot_size: "0.00001".parse().expect("lot"),
        min_notional: "5".parse().expect("notional"),
    })
}

/// Day one anchors the book; day two continues the same chain.
fn two_day_session(tree: &Tree) {
    tree.segment(
        21,
        &[
            snapshot(1, 100),
            delta(2, 101, 101, "101.00000000"),
            delta(3, 102, 102, "102.00000000"),
        ],
    );
    tree.segment(
        22,
        &[
            delta(4, 103, 103, "103.00000000"),
            delta(5, 104, 104, "104.00000000"),
        ],
    );
}

#[test]
fn a_day_boundary_is_invisible_to_the_book() {
    // The criterion for this slice. Day two carries no snapshot of its own, so
    // every delta in it depends on the anchor established the day before.
    let tree = Tree::new("day-boundary");
    two_day_session(&tree);

    let summary = replay_session(&tree.only_session(), instrument());
    assert_eq!(summary.replay.segments_read, 2);
    assert_eq!(summary.replay.breaks, 0, "{:?}", summary.first_break);
    assert_eq!(summary.book.applied, 4, "every delta on both days applied");
    assert_eq!(summary.book.unanchored, 0, "nothing waited for an anchor");
    assert_eq!(summary.book.broken, 0);
    assert_eq!(summary.book.invalidations, 0);
    assert_eq!(summary.violations, 0, "{:?}", summary.first_violation);
    assert!(summary.is_clean());
}

#[test]
fn the_same_second_day_alone_reconstructs_nothing() {
    // The control for the test above, and the reason M2.c exists at all: read on
    // its own, day two has no anchor, so its deltas sit in the buffer waiting for
    // a snapshot that lives in yesterday's file. This is exactly the 7k-34k
    // dropped deltas seen on days 2 through 8 of the acceptance capture.
    let tree = Tree::new("second-day-alone");
    two_day_session(&tree);

    let mut session = tree.only_session();
    session.segments.remove(0);

    let summary = replay_session(&session, instrument());
    assert_eq!(summary.replay.segments_read, 1);
    assert_eq!(summary.book.applied, 0, "no anchor, so nothing can apply");
    assert_eq!(
        summary.checked_live, 0,
        "and never a book to read prices off"
    );
}

#[test]
fn a_sequence_hole_breaks_the_stream_even_with_no_gap_record() {
    // A hole means messages were lost -- the recorder consumes a sequence number
    // for every message it drops. Carrying on would hand downstream a book that
    // looks continuous across data we never had.
    let tree = Tree::new("hole");
    tree.segment(
        21,
        &[
            snapshot(1, 100),
            delta(2, 101, 101, "101.00000000"),
            // seq 3 is missing.
            delta(4, 102, 102, "102.00000000"),
        ],
    );

    let summary = replay_session(&tree.only_session(), instrument());
    assert_eq!(summary.replay.breaks, 1);
    assert!(
        summary
            .first_break
            .as_ref()
            .is_some_and(|b| b.contains("sequence hole")),
        "{:?}",
        summary.first_break
    );
    assert_eq!(summary.book.invalidations, 1, "the book was cleared");
    // And the delta after the hole did not apply to a book it does not belong to.
    assert_eq!(summary.book.applied, 1);
}

#[test]
fn a_recorded_gap_invalidates_without_being_a_break() {
    // The recorder's own account of blindness is an event, not an archive defect.
    // Both clear the book; only one of them says the file is wrong.
    let tree = Tree::new("recorded-gap");
    tree.segment(
        21,
        &[
            snapshot(1, 100),
            delta(2, 101, 101, "101.00000000"),
            Frame::Gap { seq: 3 },
            delta(4, 102, 102, "102.00000000"),
        ],
    );

    let summary = replay_session(&tree.only_session(), instrument());
    assert_eq!(summary.replay.breaks, 0, "the archive is intact");
    assert_eq!(summary.replay.gaps, 1);
    assert_eq!(summary.book.invalidations, 1);
}

#[test]
fn a_file_from_another_session_is_refused_not_read() {
    // Its ingest_seq space is unrelated, so splicing it in would read either as
    // an enormous hole or, worse, as continuity.
    let tree = Tree::new("wrong-session");
    two_day_session(&tree);

    let mut session = tree.only_session();
    // Point the second entry at a file whose header names a different session.
    let stranger = CaptureTarget {
        session_id: [0xaa; 16],
        ..Tree::target(23)
    };
    let path = stranger.file(&tree.root);
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    let file = std::fs::File::create(&path).expect("create");
    let writer = RawWriter::create(
        file,
        FileHeader::new(Exchange::Binance, SYMBOL, [0xaa; 16]),
        WriterOptions::default(),
    )
    .expect("writer");
    writer.finish().expect("finish");
    session.segments[1].path = path;

    let summary = replay_session(&session, instrument());
    assert_eq!(summary.replay.segments_read, 1, "only the file that fits");
    assert_eq!(summary.replay.breaks, 1);
    assert!(
        summary
            .first_break
            .as_ref()
            .is_some_and(|b| b.contains("belongs elsewhere")),
        "{:?}",
        summary.first_break
    );
}

#[test]
fn a_venue_with_no_adapter_is_reported_once_and_not_guessed_at() {
    // A capture this build cannot read is a fact about the build. Staying quiet
    // would let a partial normalization look complete.
    let files = SessionFiles {
        exchange: Exchange::Kraken,
        symbol: "XBTUSD".to_owned(),
        session_id: SESSION,
        segments: Vec::new(),
    };
    let items: Vec<ReplayItem> = SessionReplay::open(&files, instrument()).collect();
    assert_eq!(items.len(), 1);
    assert!(matches!(
        &items[0],
        ReplayItem::Break(b) if matches!(b.kind, BreakKind::UnsupportedVenue(Exchange::Kraken))
    ));
}

#[test]
fn a_torn_tail_is_a_break_and_the_frames_before_it_still_replay() {
    // The ordinary signature of a killed recorder. Everything written before the
    // tear is intact and must still be used; the tear itself must still be said
    // out loud, because what follows in the next segment does not continue it.
    let tree = Tree::new("torn");
    let path = tree.segment(
        21,
        &[
            snapshot(1, 100),
            delta(2, 101, 101, "101.00000000"),
            delta(3, 102, 102, "102.00000000"),
        ],
    );
    let bytes = std::fs::read(&path).expect("read");
    // Drop the trailer and part of the last block: a SIGKILL mid-write.
    std::fs::write(&path, &bytes[..bytes.len() - 40]).expect("truncate");

    let summary = replay_session(&tree.only_session(), instrument());
    assert_eq!(summary.replay.breaks, 1);
    assert!(
        summary
            .first_break
            .as_ref()
            .is_some_and(|b| { b.contains("torn segment") || b.contains("unreadable segment") }),
        "{:?}",
        summary.first_break
    );
}

#[test]
fn events_come_out_in_the_order_they_were_recorded() {
    // Across the join, and without the book in the way -- the ordering guarantee
    // belongs to the replay, and M2.d and M3 will depend on it without a book.
    let tree = Tree::new("order");
    two_day_session(&tree);

    let seqs: Vec<u64> = SessionReplay::open(&tree.only_session(), instrument())
        .filter_map(|item| match item {
            ReplayItem::Event(e) => Some(e.meta().ingest_seq),
            ReplayItem::Break(_) => None,
        })
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
}

/// Guard for the assumption every path test rests on.
#[test]
fn the_catalog_finds_both_days_as_one_session() {
    let tree = Tree::new("catalog");
    two_day_session(&tree);
    let session = tree.only_session();
    assert_eq!(session.segments.len(), 2);
    assert_eq!(session.segments[0].target.date.day, 21);
    assert_eq!(session.segments[1].target.date.day, 22);
    assert_eq!(session.symbol, SYMBOL);
    let _ = Path::new(".");
}
