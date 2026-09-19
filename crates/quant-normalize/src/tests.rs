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

use quant_core::event::MarketEvent;

use crate::{replay_session, BreakKind, ReplayItem, SessionReplay};

const SESSION: [u8; 16] = [0x5e; 16];
const SYMBOL: &str = "BTCUSDT";

/// A capture tree under a temp directory, removed when the test ends.
struct Tree {
    root: PathBuf,
}

impl Tree {
    fn new(name: &str) -> Self {
        // The test's own name is part of the path, so two fixtures that pick the
        // same label cannot collide. They did: a new test reused "re-derive" and
        // the two shared one directory, each wiping the other's tree on the way
        // in -- which failed about half the time and never alone, because the
        // harness runs tests in parallel. A fixture root that is unique by
        // construction is cheaper than remembering every name already taken.
        let thread = std::thread::current();
        let unique = thread.name().unwrap_or("unnamed").replace("::", "-");
        let root = std::env::temp_dir().join(format!("quant-normalize-{name}-{unique}"));
        let _ = std::fs::remove_dir_all(&root);
        Self { root }
    }

    fn target(day: u8) -> CaptureTarget {
        Self::target_for(SESSION, day)
    }

    fn target_for(session_id: [u8; 16], day: u8) -> CaptureTarget {
        CaptureTarget {
            exchange: Exchange::Binance,
            symbol: SYMBOL.to_owned(),
            date: UtcDate {
                year: 2026,
                month: 8,
                day,
            },
            session_id,
            part: 0,
        }
    }

    /// Write one segment. `frames` is `(ingest_seq, payload)` in order.
    ///
    /// Timestamps are milliseconds from the epoch and so deliberately disagree
    /// with the 2026 dates in the paths. Real captures never diverge like that,
    /// which is precisely why a fixture should: it means every partition test
    /// here proves the day comes from the segment rather than from the record's
    /// own clock. See `a_record_is_filed_where_the_raw_tier_filed_it`.
    fn segment(&self, day: u8, frames: &[Frame]) -> PathBuf {
        self.segment_for(SESSION, day, frames)
    }

    /// The same, for a session other than the default one — a recorder restart
    /// produces exactly this: a second session covering days the first also saw.
    fn segment_for(&self, session_id: [u8; 16], day: u8, frames: &[Frame]) -> PathBuf {
        let path = Self::target_for(session_id, day).file(&self.root);
        std::fs::create_dir_all(path.parent().expect("a file has a parent")).expect("mkdir");
        let file = std::fs::File::create(&path).expect("create");
        let header = FileHeader::new(Exchange::Binance, SYMBOL, session_id);
        let mut writer =
            RawWriter::create(file, header, WriterOptions::default()).expect("create writer");
        for frame in frames {
            if matches!(frame, Frame::Seal) {
                writer.flush().expect("seal the pending block");
                continue;
            }
            let ts = Ts::from_millis(i64::try_from(frame.seq()).expect("small seq"));
            match frame {
                Frame::Seal => unreachable!("handled above"),
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
    Stream {
        seq: u64,
        payload: Vec<u8>,
    },
    Snapshot {
        seq: u64,
        payload: Vec<u8>,
    },
    Gap {
        seq: u64,
    },
    /// Seal the pending block. Not a record — it exists so a test can put a
    /// block boundary where it wants one, which is what makes a torn *tail*
    /// distinguishable from a file destroyed entirely. A capture with one small
    /// block loses everything when its tail is cut; a real one seals a block
    /// every 256 KiB or every flush tick.
    Seal,
}

impl Frame {
    const fn seq(&self) -> u64 {
        match self {
            Self::Stream { seq, .. } | Self::Snapshot { seq, .. } | Self::Gap { seq } => *seq,
            Self::Seal => 0,
        }
    }
}

/// A `depthUpdate` covering `[first, last]` that moves the best bid.
/// The UTC date the fixtures file day `d` under.
fn day(d: u8) -> UtcDate {
    UtcDate {
        year: 2026,
        month: 8,
        day: d,
    }
}

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
            // Seal here, so the tear below takes the *second* block. Without a
            // boundary the file is one block and cutting its tail destroys all
            // of it, which is a different failure and not the one under test.
            Frame::Seal,
            delta(3, 102, 102, "102.00000000"),
        ],
    );
    let bytes = std::fs::read(&path).expect("read");
    // Drop the trailer and part of the last block: a SIGKILL mid-write.
    std::fs::write(&path, &bytes[..bytes.len() - 40]).expect("truncate");

    let summary = replay_session(&tree.only_session(), instrument());
    assert_eq!(summary.replay.breaks, 1);
    assert_eq!(
        summary.replay.events, 2,
        "everything before the tear is intact and must still be replayed"
    );
    assert_eq!(summary.book.applied, 1, "and applied to the book");
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

// --- The normalized tier, written from a real session ---

#[test]
fn a_session_is_written_into_one_partition_per_day() {
    use crate::{normalize_session, Dataset, TierTarget};

    let tree = Tree::new("write-partitions");
    two_day_session(&tree);
    let out = tree.root.join("out");

    let result = normalize_session(&tree.only_session(), instrument(), Some(&out));
    assert!(result.write_error.is_none(), "{:?}", result.write_error);
    let report = result.written.expect("a write report");

    // Two days in, two date partitions out -- and the deltas land in the day
    // they were received on, not all in the day the session opened.
    assert_eq!(report.days.len(), 2);
    assert_eq!(report.rows_in(Dataset::BookDeltas), 4);
    assert_eq!(report.rows_in(Dataset::BookSnapshots), 1);
    assert_eq!(report.rows_in(Dataset::Trades), 0);

    // Every day gets every dataset, empty ones included: an absent file and an
    // empty file are different claims.
    for day in &report.days {
        for dataset in Dataset::ALL {
            let target = TierTarget {
                exchange: Exchange::Binance,
                symbol: SYMBOL.to_owned(),
                date: day.date,
                dataset,
                part: 0,
            };
            assert!(
                target.file(&out).is_file(),
                "missing {}",
                target.file(&out).display()
            );
        }
    }
}

#[test]
fn what_was_written_reads_back_as_the_events_that_went_in() {
    // M2's last criterion in miniature: raw in, Parquet out, same events.
    use crate::tier::read_dataset;
    use crate::{normalize_session, Dataset, ReplayItem, SessionReplay, TierTarget};

    let tree = Tree::new("round-trip-session");
    two_day_session(&tree);
    let out = tree.root.join("out");
    let session = tree.only_session();

    let from_raw: Vec<MarketEvent> = SessionReplay::open(&session, instrument())
        .filter_map(|item| match item {
            ReplayItem::Event(e) => Some(e),
            ReplayItem::Break(_) => None,
        })
        .collect();

    let result = normalize_session(&session, instrument(), Some(&out));
    assert!(result.write_error.is_none());

    let mut from_parquet = Vec::new();
    for day in &result.written.expect("written").days {
        for dataset in Dataset::ALL {
            let target = TierTarget {
                exchange: Exchange::Binance,
                symbol: SYMBOL.to_owned(),
                date: day.date,
                dataset,
                part: 0,
            };
            from_parquet.extend(
                read_dataset(&target.file(&out), dataset, instrument()).expect("read back"),
            );
        }
    }
    // Datasets are separate files, so the stream is reassembled by ingest_seq --
    // which is exactly what makes it the ordering key.
    from_parquet.sort_by_key(|e| e.meta().ingest_seq);
    assert_eq!(from_parquet, from_raw);
}

#[test]
fn a_break_abandons_the_partition_rather_than_writing_across_it() {
    // Raw is the source of truth and this tier is disposable, so the answer to
    // an unreadable archive is to stop and re-derive -- never to bake a hole
    // into a file that will look continuous to everyone who reads it later.
    use crate::normalize_session;
    use crate::tier::TierTarget;

    let tree = Tree::new("break-abandons");
    tree.segment(
        21,
        &[
            snapshot(1, 100),
            delta(2, 101, 101, "101.00000000"),
            // seq 3 is missing: a hole with no gap record.
            delta(4, 102, 102, "102.00000000"),
        ],
    );
    let out = tree.root.join("out");

    let result = normalize_session(&tree.only_session(), instrument(), Some(&out));
    assert!(
        result
            .write_error
            .as_ref()
            .is_some_and(|e| e.contains("sequence hole")),
        "{:?}",
        result.write_error
    );
    let target = TierTarget {
        exchange: Exchange::Binance,
        symbol: SYMBOL.to_owned(),
        date: UtcDate {
            year: 2026,
            month: 8,
            day: 21,
        },
        dataset: crate::Dataset::BookDeltas,
        part: 0,
    };
    assert!(
        !target.file(&out).exists(),
        "an abandoned partition must leave nothing behind"
    );
    assert!(
        !target.file(&out).with_extension("parquet.tmp").exists(),
        "including its temporary file"
    );

    // The reconstruction is still reported: the two answer different questions.
    assert_eq!(result.summary.replay.breaks, 1);
}

#[test]
fn a_torn_tail_on_the_last_segment_still_publishes() {
    // The ordinary signature of a killed recorder. Everything before the tear is
    // intact and nothing follows it, so refusing to write would mean no session
    // that ended by being killed could ever be normalized.
    use crate::normalize_session;
    use crate::tier::TierTarget;

    let tree = Tree::new("torn-publishes");
    let path = tree.segment(
        21,
        &[
            snapshot(1, 100),
            delta(2, 101, 101, "101.00000000"),
            Frame::Seal,
            delta(3, 102, 102, "102.00000000"),
        ],
    );
    let bytes = std::fs::read(&path).expect("read");
    std::fs::write(&path, &bytes[..bytes.len() - 40]).expect("truncate");
    let out = tree.root.join("out");

    let result = normalize_session(&tree.only_session(), instrument(), Some(&out));
    assert!(result.write_error.is_none(), "{:?}", result.write_error);
    assert_eq!(
        result.summary.replay.breaks, 1,
        "the tear is still reported"
    );

    let target = TierTarget {
        exchange: Exchange::Binance,
        symbol: SYMBOL.to_owned(),
        date: UtcDate {
            year: 2026,
            month: 8,
            day: 21,
        },
        dataset: crate::Dataset::BookSnapshots,
        part: 0,
    };
    assert!(target.file(&out).is_file());
}

#[test]
fn re_deriving_replaces_a_partition_rather_than_appending_to_it() {
    // The tier is disposable, so running twice must leave what running once
    // does. A writer that appended would double every row on the second run.
    use crate::{normalize_session, Dataset};

    let tree = Tree::new("re-derive");
    two_day_session(&tree);
    let out = tree.root.join("out");
    let session = tree.only_session();

    let first = normalize_session(&session, instrument(), Some(&out))
        .written
        .expect("first");
    let second = normalize_session(&session, instrument(), Some(&out))
        .written
        .expect("second");
    assert_eq!(first.rows(), second.rows());
    assert_eq!(
        second.rows_in(Dataset::BookDeltas),
        4,
        "a re-derive replaces, it does not accumulate"
    );
}

#[test]
fn a_record_is_filed_where_the_raw_tier_filed_it() {
    // The rule this crate inherits rather than reimplements. quant-recorder rolls
    // on the record's timestamp but never backwards, so a record stamped before
    // the open segment's day -- after an NTP step -- is written to the open
    // segment on purpose. Re-deriving the day here would put it somewhere the raw
    // tier did not, and the two tiers would silently stop lining up.
    //
    // The fixture's timestamps are all 1970 while its segments are dated 2026, so
    // every record in it is that case in the extreme.
    use crate::{normalize_session, Dataset, TierTarget};

    let tree = Tree::new("filed-where-raw-filed-it");
    two_day_session(&tree);
    let out = tree.root.join("out");

    let result = normalize_session(&tree.only_session(), instrument(), Some(&out));
    let report = result.written.expect("written");

    let days: Vec<UtcDate> = report.days.iter().map(|d| d.date).collect();
    assert_eq!(
        days,
        vec![
            UtcDate {
                year: 2026,
                month: 8,
                day: 21
            },
            UtcDate {
                year: 2026,
                month: 8,
                day: 22
            },
        ],
        "the partitions follow the segments, not the records' own timestamps"
    );
    assert!(
        !TierTarget {
            exchange: Exchange::Binance,
            symbol: SYMBOL.to_owned(),
            date: UtcDate {
                year: 1970,
                month: 1,
                day: 1
            },
            dataset: Dataset::BookDeltas,
            part: 0,
        }
        .file(&out)
        .exists(),
        "and never re-derive a day from local_recv_ts"
    );
}

#[test]
fn two_sessions_on_one_day_are_merged_into_parts() {
    // The normalized layout has no session dimension, so a recorder restart --
    // which creates a new session -- can put two sessions on one symbol-day.
    // M2.d refused that, which was safe rather than lossy but left the day
    // unnormalizable; a fortnight that restarts once would have been unjudgeable.
    //
    // A restart is sequential, so the day is a concatenation and each session
    // gets its own part. Nothing inside a row changes.
    use crate::normalize_session;

    const RESTARTED: [u8; 16] = [0xaa; 16];

    let tree = Tree::new("two-sessions");
    two_day_session(&tree);
    // Later update ids than the first session's 100..102, which is what says
    // this session came after it.
    tree.segment_for(
        RESTARTED,
        21,
        &[snapshot(1, 200), delta(2, 201, 201, "201.00000000")],
    );
    let out = tree.root.join("out");

    let sessions = tree.sessions();
    assert_eq!(sessions.len(), 2, "the catalog sees both runs");
    let (first, second) = sessions
        .iter()
        .partition::<Vec<_>, _>(|s| s.session_id == SESSION);

    let a = normalize_session(first[0], instrument(), Some(&out));
    assert!(a.write_error.is_none(), "{:?}", a.write_error);
    let b = normalize_session(second[0], instrument(), Some(&out));
    assert!(b.write_error.is_none(), "{:?}", b.write_error);

    // The first session took part 0 of both its days; the second took part 1 of
    // the day they share.
    let written = b.written.as_ref().expect("written");
    let shared = written
        .days
        .iter()
        .find(|d| d.date == day(21))
        .expect("the shared day was written");
    assert_eq!(shared.part, 1, "the restart appended rather than replacing");
    assert!(
        a.written
            .as_ref()
            .expect("written")
            .days
            .iter()
            .all(|d| d.part == 0),
        "the first session is always part 0"
    );

    // And each session still reads back as itself, which is what `--check` does.
    for files in &sessions {
        let agreement = crate::check_session(files, instrument(), &out);
        assert!(agreement.agrees(), "{:?}", agreement.divergence);
    }
}

#[test]
fn a_re_derive_replaces_its_own_part_rather_than_appending() {
    // What keeps the tier disposable. Without this, every re-derive would add
    // another copy of the same session and the day would grow without bound.
    use crate::normalize_session;

    const RESTARTED: [u8; 16] = [0xaa; 16];

    let tree = Tree::new("re-derive-part");
    two_day_session(&tree);
    tree.segment_for(
        RESTARTED,
        21,
        &[snapshot(1, 200), delta(2, 201, 201, "201.00000000")],
    );
    let out = tree.root.join("out");
    let sessions = tree.sessions();
    let (first, second) = sessions
        .iter()
        .partition::<Vec<_>, _>(|s| s.session_id == SESSION);

    let _ = normalize_session(first[0], instrument(), Some(&out));
    let _ = normalize_session(second[0], instrument(), Some(&out));
    // Again, both, in the same order.
    let again_a = normalize_session(first[0], instrument(), Some(&out));
    let again_b = normalize_session(second[0], instrument(), Some(&out));
    assert!(again_a.write_error.is_none(), "{:?}", again_a.write_error);
    assert!(again_b.write_error.is_none(), "{:?}", again_b.write_error);

    let shared_dir = crate::tier::TierTarget {
        exchange: Exchange::Binance,
        symbol: SYMBOL.to_owned(),
        date: day(21),
        dataset: crate::tier::Dataset::BookDeltas,
        part: 0,
    }
    .directory(&out);
    let parts = std::fs::read_dir(&shared_dir)
        .expect("the shared day exists")
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with(".parquet"))
        })
        .count();
    assert_eq!(parts, 2, "still two parts, not four");
}

#[test]
fn concurrent_sessions_are_refused_rather_than_concatenated() {
    // The case a concatenation cannot represent. Two recorders subscribed at
    // once produce overlapping venue sequences, and there is no ordering of two
    // simultaneous recordings of the same messages that is the truth -- so this
    // refuses instead of picking one.
    use crate::normalize_session;

    const CONCURRENT: [u8; 16] = [0xbb; 16];

    let tree = Tree::new("concurrent");
    two_day_session(&tree);
    // Update ids inside the first session's 100..102, not after it.
    tree.segment_for(
        CONCURRENT,
        21,
        &[snapshot(1, 100), delta(2, 101, 101, "101.00000000")],
    );
    let out = tree.root.join("out");
    let sessions = tree.sessions();
    let (first, second) = sessions
        .iter()
        .partition::<Vec<_>, _>(|s| s.session_id == SESSION);

    let _ = normalize_session(first[0], instrument(), Some(&out));
    let result = normalize_session(second[0], instrument(), Some(&out));
    assert!(
        result
            .write_error
            .as_ref()
            .is_some_and(|e| e.contains("overlap")),
        "{:?}",
        result.write_error
    );
}

#[test]
fn a_shared_day_with_no_gaps_still_merges() {
    // The case that would have made the feature useless in practice. `gaps` is
    // ordinarily empty -- the whole acceptance week had 38 gap frames -- and an
    // empty dataset still gets a file, so a span keyed to each file's own rows
    // would leave that file unable to say where it belongs and refuse the
    // restart on a completely normal day. The span belongs to the part.
    use crate::normalize_session;

    const RESTARTED: [u8; 16] = [0xaa; 16];

    let tree = Tree::new("no-gaps");
    two_day_session(&tree);
    tree.segment_for(
        RESTARTED,
        21,
        &[snapshot(1, 200), delta(2, 201, 201, "201.00000000")],
    );
    let out = tree.root.join("out");
    let sessions = tree.sessions();
    let (first, second) = sessions
        .iter()
        .partition::<Vec<_>, _>(|s| s.session_id == SESSION);

    let a = normalize_session(first[0], instrument(), Some(&out));
    // Neither session produced a gap event.
    assert_eq!(
        a.written
            .as_ref()
            .expect("written")
            .days
            .iter()
            .map(|d| d.rows[crate::tier::Dataset::Gaps.index()])
            .sum::<u64>(),
        0,
        "the fixture has no gaps, which is the point"
    );
    let b = normalize_session(second[0], instrument(), Some(&out));
    assert!(b.write_error.is_none(), "{:?}", b.write_error);
}

#[test]
fn a_tmp_file_is_not_mistaken_for_a_published_part() {
    // A `.tmp` sibling exists beside published parts while a day is written and
    // survives a SIGKILL. Before parts the reader opened one exact path, so a
    // stray name was unreachable; enumerating a directory is what makes it
    // reachable, and counting one would put the next session at an index with a
    // hole under it.
    use crate::normalize_session;

    const RESTARTED: [u8; 16] = [0xaa; 16];

    let tree = Tree::new("tmp-stray");
    two_day_session(&tree);
    tree.segment_for(
        RESTARTED,
        21,
        &[snapshot(1, 200), delta(2, 201, 201, "201.00000000")],
    );
    let out = tree.root.join("out");
    let sessions = tree.sessions();
    let (first, second) = sessions
        .iter()
        .partition::<Vec<_>, _>(|s| s.session_id == SESSION);
    let _ = normalize_session(first[0], instrument(), Some(&out));

    for dataset in crate::tier::Dataset::ALL {
        let dir = crate::tier::TierTarget {
            exchange: Exchange::Binance,
            symbol: SYMBOL.to_owned(),
            date: day(21),
            dataset,
            part: 0,
        }
        .directory(&out);
        std::fs::write(dir.join("part-00001.parquet.tmp"), b"not a parquet file")
            .expect("leave a stray");
    }

    let b = normalize_session(second[0], instrument(), Some(&out));
    assert!(
        b.write_error.is_none(),
        "the stray must be ignored, not counted: {:?}",
        b.write_error
    );
    let written = b.written.as_ref().expect("written");
    let shared = written
        .days
        .iter()
        .find(|d| d.date == day(21))
        .expect("written");
    assert_eq!(shared.part, 1, "still part 1, not 2");
}

#[test]
fn a_half_deleted_day_is_refused_not_re_placed() {
    // Ownership used to be checked per dataset path, which quietly guarded this:
    // remove `trades/` and leave the rest, and a per-day enumeration that
    // trusted one dataset would hand the incoming session index 0 -- silently
    // overwriting the other three datasets of a session that is still there.
    use crate::normalize_session;

    const RESTARTED: [u8; 16] = [0xaa; 16];

    let tree = Tree::new("half-deleted");
    two_day_session(&tree);
    tree.segment_for(
        RESTARTED,
        21,
        &[snapshot(1, 200), delta(2, 201, 201, "201.00000000")],
    );
    let out = tree.root.join("out");
    let sessions = tree.sessions();
    let (first, second) = sessions
        .iter()
        .partition::<Vec<_>, _>(|s| s.session_id == SESSION);
    let _ = normalize_session(first[0], instrument(), Some(&out));

    let trades = crate::tier::TierTarget {
        exchange: Exchange::Binance,
        symbol: SYMBOL.to_owned(),
        date: day(21),
        dataset: crate::tier::Dataset::Trades,
        part: 0,
    }
    .directory(&out);
    std::fs::remove_dir_all(&trades).expect("remove one dataset");

    let result = normalize_session(second[0], instrument(), Some(&out));
    assert!(
        result
            .write_error
            .as_ref()
            .is_some_and(|e| e.contains("disagree about which parts exist")),
        "{:?}",
        result.write_error
    );
}

#[test]
fn a_written_partition_names_the_session_it_came_from() {
    // Provenance in the footer rather than a sidecar: a sidecar can be separated
    // from what it describes, and every Parquet reader can already see this.
    use crate::tier::Provenance;
    use crate::{normalize_session, Dataset, TierTarget};

    let tree = Tree::new("provenance");
    two_day_session(&tree);
    let out = tree.root.join("out");
    let session = tree.only_session();
    let _ = normalize_session(&session, instrument(), Some(&out));

    let target = TierTarget {
        exchange: Exchange::Binance,
        symbol: SYMBOL.to_owned(),
        date: UtcDate {
            year: 2026,
            month: 8,
            day: 21,
        },
        dataset: Dataset::BookDeltas,
        part: 0,
    };
    assert_eq!(
        Provenance::session_of(&target.file(&out)).expect("readable"),
        Some(session.session_id)
    );
}

#[test]
fn a_parquet_replay_agrees_with_the_raw_replay() {
    // M2's last criterion, in miniature. Event by event and not by summary:
    // two reconstructions can produce identical statistics from different
    // events, and a summary comparison would pass every one of those.
    use crate::{check_session, normalize_session};

    let tree = Tree::new("agreement");
    two_day_session(&tree);
    let out = tree.root.join("out");
    let session = tree.only_session();

    let written = normalize_session(&session, instrument(), Some(&out));
    assert!(written.write_error.is_none());

    let agreement = check_session(&session, instrument(), &out);
    assert!(agreement.agrees(), "{:?}", agreement.divergence);
    assert_eq!(agreement.matched, 5, "every recorded event, both days");
}

#[test]
fn a_missing_day_partition_is_a_disagreement_not_a_shorter_stream() {
    // The failure this check exists to catch: a tier that is *plausible* -- it
    // opens, it parses, its books reconstruct -- and is missing data. Comparing
    // lengths after the fact would notice; comparing event by event notices
    // where.
    use crate::{check_session, normalize_session, Dataset, TierTarget};

    let tree = Tree::new("missing-day");
    two_day_session(&tree);
    let out = tree.root.join("out");
    let session = tree.only_session();
    let _ = normalize_session(&session, instrument(), Some(&out));

    let target = TierTarget {
        exchange: Exchange::Binance,
        symbol: SYMBOL.to_owned(),
        date: UtcDate {
            year: 2026,
            month: 8,
            day: 22,
        },
        dataset: Dataset::BookDeltas,
        part: 0,
    };
    std::fs::remove_file(target.file(&out)).expect("remove a day's deltas");

    let agreement = check_session(&session, instrument(), &out);
    assert!(!agreement.agrees(), "a hole in the tier must not pass");
    assert_eq!(
        agreement.matched, 3,
        "and the count says how far it got before the hole"
    );
}

#[test]
fn a_tampered_value_is_caught_rather_than_averaged_away() {
    // A single wrong price in seventy million events is exactly what a summary
    // comparison would miss, so it is worth proving the check sees one.
    use crate::tier::{DatasetWriter, Provenance};
    use crate::{check_session, normalize_session, Dataset, TierTarget};

    let tree = Tree::new("tampered");
    two_day_session(&tree);
    let out = tree.root.join("out");
    let session = tree.only_session();
    let _ = normalize_session(&session, instrument(), Some(&out));

    // Rewrite one day's deltas with a price that was never recorded.
    let target = TierTarget {
        exchange: Exchange::Binance,
        symbol: SYMBOL.to_owned(),
        date: UtcDate {
            year: 2026,
            month: 8,
            day: 21,
        },
        dataset: Dataset::BookDeltas,
        part: 0,
    };
    let path = target.file(&out);
    let mut events =
        crate::tier::read_dataset(&path, Dataset::BookDeltas, instrument()).expect("read");
    if let MarketEvent::BookDelta(d) = &mut events[0] {
        d.bids[0].px = "999.00000000".parse().expect("px");
    }
    let provenance = Provenance {
        session_id: session.session_id,
        exchange: Exchange::Binance,
        symbol: SYMBOL.to_owned(),
        date: target.date,
    };
    let file = std::fs::File::create(&path).expect("create");
    let mut writer =
        DatasetWriter::new(file, Dataset::BookDeltas, Some(&provenance)).expect("writer");
    for event in &events {
        writer.push(event).expect("push");
    }
    writer.finish().expect("finish");

    let agreement = check_session(&session, instrument(), &out);
    assert!(!agreement.agrees(), "one altered price must not pass");
}

#[test]
fn a_restart_merges_whichever_order_the_sessions_are_normalized_in() {
    // The defect this test was written for, and it went red on the code that
    // shipped in M2.e. `two_sessions_on_one_day_are_merged_into_parts` always
    // normalizes the earlier session first -- so it pinned the lucky half of a
    // coin flip and never saw the other half.
    //
    // A part's index comes from arrival order, arrival order is `catalog` order,
    // and `catalog` sorts paths whose session component is a v4 UUID. Nothing
    // about that is chronological. The old check demanded that the arriving part
    // start after every published one, so the *earlier* session was refused
    // whenever it happened to be normalized second -- and `normalize` abandons
    // the writer on a refusal, taking the rest of that session's days with it.
    //
    // One restart in the fortnight would have hit this with probability one half.
    use crate::normalize_session;

    const RESTARTED: [u8; 16] = [0xaa; 16];

    let tree = Tree::new("restart-either-order");
    two_day_session(&tree);
    tree.segment_for(
        RESTARTED,
        21,
        &[snapshot(1, 200), delta(2, 201, 201, "201.00000000")],
    );
    let out = tree.root.join("out");

    let sessions = tree.sessions();
    let (first, second) = sessions
        .iter()
        .partition::<Vec<_>, _>(|s| s.session_id == SESSION);

    // Deliberately backwards: the session that recorded *later* is normalized
    // first, which is what a UUID sort does half the time.
    let b = normalize_session(second[0], instrument(), Some(&out));
    assert!(b.write_error.is_none(), "{:?}", b.write_error);
    let a = normalize_session(first[0], instrument(), Some(&out));
    assert!(
        a.write_error.is_none(),
        "the earlier session must not be refused for arriving second: {:?}",
        a.write_error
    );

    // The index records arrival, not chronology, and that is now allowed to be
    // true -- the later session holds part 0 here.
    let shared = a
        .written
        .as_ref()
        .expect("written")
        .days
        .iter()
        .find(|d| d.date == day(21))
        .expect("the shared day was written");
    assert_eq!(shared.part, 1, "arrival order still assigns the index");

    // And nothing about the day it does *not* share was lost to the refusal.
    assert!(
        a.written
            .as_ref()
            .expect("written")
            .days
            .iter()
            .any(|d| d.date == day(22)),
        "the rest of the abandoned session's days must still be written"
    );

    for files in &sessions {
        let agreement = crate::check_session(files, instrument(), &out);
        assert!(agreement.agrees(), "{:?}", agreement.divergence);
    }
}

#[test]
fn a_days_parts_are_read_in_venue_order_not_index_order() {
    // The other half of the same defect, and the more dangerous half: the old
    // reader walked parts by index. Had the write above been allowed, a backtest
    // over the shared day would have seen the afternoon before the morning with
    // nothing anywhere saying so -- a book reconstructed from events in the
    // wrong order, which is the failure this project has spent two milestones
    // making impossible.
    use crate::normalize_session;
    use crate::tier::TierReplay;

    const RESTARTED: [u8; 16] = [0xaa; 16];

    let tree = Tree::new("read-venue-order");
    two_day_session(&tree);
    tree.segment_for(
        RESTARTED,
        21,
        &[snapshot(1, 200), delta(2, 201, 201, "201.00000000")],
    );
    let out = tree.root.join("out");

    let sessions = tree.sessions();
    let (first, second) = sessions
        .iter()
        .partition::<Vec<_>, _>(|s| s.session_id == SESSION);
    // Backwards again, so the later stretch of the market is in part 0.
    let _ = normalize_session(second[0], instrument(), Some(&out));
    let _ = normalize_session(first[0], instrument(), Some(&out));

    let ids: Vec<u64> = TierReplay::open(
        &out,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        vec![day(21)],
    )
    .map(|e| e.expect("replay"))
    .filter_map(|e| match e {
        MarketEvent::BookDelta(d) => Some(d.final_update_id),
        MarketEvent::BookSnapshot(s) => Some(s.last_update_id),
        MarketEvent::Trade(_) | MarketEvent::Gap(_) => None,
    })
    .collect();

    assert_eq!(
        ids,
        vec![100, 101, 102, 200, 201],
        "the day must read back in the venue's order, not in part-index order"
    );
}
