//! Tests for the time cursor.
//!
//! Against synthetic captures whose shape is known exactly, because the
//! properties worth pinning are about *refusal*: that an instant before the data
//! says so, that a gap is reported as blindness rather than as a stale book, and
//! that a query is never answered with a number nobody observed. A query tool's
//! characteristic failure is confabulation, so most of these assert an absence
//! and a reason rather than a value.

use std::path::PathBuf;

use quant_core::event::GapCause;
use quant_core::instrument::{
    Exchange, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
};
use quant_core::time::{Ts, UtcDate};
use quant_storage::{ControlRecord, FileHeader, RawWriter, WriterOptions};

use crate::market_at;

const SYMBOL: &str = "BTCUSDT";
const SESSION: [u8; 16] = [7; 16];

fn instrument() -> InstrumentId {
    InstrumentRegistry::new().register(InstrumentDef {
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

fn date(day: u8) -> UtcDate {
    UtcDate {
        year: 2026,
        month: 8,
        day,
    }
}

/// `2026-08-<day>T00:00:<secs>Z`.
fn at(day: u8, secs: i64) -> Ts {
    Ts::from_nanos(date(day).days_since_epoch() * 86_400 * 1_000_000_000 + secs * 1_000_000_000)
}

/// What to write, in order. One frame per second from midnight.
enum Frame {
    Snapshot(u64),
    Delta(u64),
    Gap,
}

/// A capture root unique to the calling test.
///
/// Named by the test's own thread so two fixtures cannot share a directory and
/// wipe each other on entry -- the flake M2.e found, which failed half the time
/// and never in isolation.
struct Tree {
    root: PathBuf,
}

impl Tree {
    fn new(name: &str) -> Self {
        let thread = std::thread::current();
        let unique = thread.name().unwrap_or("unnamed").replace("::", "-");
        let root = std::env::temp_dir().join(format!("quant-explain-{name}-{unique}"));
        let _ = std::fs::remove_dir_all(&root);
        Self { root }
    }

    /// One segment for `day`, frames at one-second spacing from `from_secs`,
    /// numbered from `from_seq`.
    ///
    /// `ingest_seq` is **session-scoped**, not per-file: it spans the session's
    /// whole life so that a hole straddling midnight is still a hole. Restarting
    /// it per day -- which the first draft of this fixture did -- produces a
    /// genuine discontinuity that `SessionReplay` correctly refuses to read
    /// across, and the test failed for that reason rather than for the one it
    /// was written to check.
    fn segment(&self, day: u8, from_secs: i64, from_seq: u64, frames: &[Frame]) {
        let target = quant_recorder::CaptureTarget {
            exchange: Exchange::Binance,
            symbol: SYMBOL.to_owned(),
            date: date(day),
            session_id: SESSION,
            part: 0,
        };
        let path = target.file(&self.root);
        std::fs::create_dir_all(path.parent().expect("a file has a parent")).expect("mkdir");
        let file = std::fs::File::create(&path).expect("create");
        let header = FileHeader::new(Exchange::Binance, SYMBOL, SESSION);
        let mut writer =
            RawWriter::create(file, header, WriterOptions::default()).expect("create writer");

        for (i, frame) in frames.iter().enumerate() {
            let offset = i64::try_from(i).expect("small");
            let ts = at(day, from_secs + offset);
            let seq = from_seq + u64::try_from(i).expect("small");
            match frame {
                Frame::Snapshot(last_update_id) => writer.write_venue_snapshot(
                    ts,
                    seq,
                    format!(
                        r#"{{"lastUpdateId":{last_update_id},"bids":[["100.00000000","1.00000000"]],"asks":[["200.00000000","1.00000000"]]}}"#
                    )
                    .as_bytes(),
                ),
                Frame::Delta(id) => writer.write_venue_payload(
                    ts,
                    seq,
                    format!(
                        r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":{seq},"s":"BTCUSDT","U":{id},"u":{id},"b":[["100.00000000","2.00000000"]],"a":[["200.00000000","1.00000000"]]}}}}"#
                    )
                    .as_bytes(),
                ),
                Frame::Gap => writer.write_control(
                    ts,
                    seq,
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
    }
}

#[test]
fn an_instant_inside_the_capture_gets_a_book() {
    let tree = Tree::new("inside");
    tree.segment(
        21,
        100,
        1,
        &[
            Frame::Snapshot(100),
            Frame::Delta(101),
            Frame::Delta(102),
            Frame::Delta(103),
        ],
    );

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 102),
        0,
    )
    .expect("the instant is covered");

    let book = answer.book.expect("anchored and live");
    assert_eq!(book.bid.expect("a bid").px.to_string(), "100");
    assert_eq!(book.ask.expect("an ask").px.to_string(), "200");
    // Three events at or before 00:01:42 -- the snapshot and two deltas -- and
    // emphatically not the fourth, which is what "as of" has to mean.
    assert_eq!(answer.events_read, 3);
    assert_eq!(answer.last_event_ts, Some(at(21, 102)));
}

#[test]
fn an_instant_before_the_capture_refuses_rather_than_answering() {
    // The first refusal, and the shape of all of them: a query tool must say
    // "unknown" rather than produce a number nobody observed.
    let tree = Tree::new("before");
    tree.segment(21, 100, 1, &[Frame::Snapshot(100), Frame::Delta(101)]);

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 50),
        0,
    );
    assert!(
        answer.is_err(),
        "an instant before any event must not be answered"
    );
}

#[test]
fn a_gap_is_reported_as_blindness_and_never_as_a_stale_book() {
    // The failure this whole type exists to prevent. The book was live and
    // known; then the venue disconnected. Printing the last known touch would
    // look entirely plausible and would be a lie about what we could see.
    //
    // It is also M2.b's decision arriving where it was going: an invalid book is
    // *cleared, not flagged*, "because a flag can be ignored" -- so there is no
    // stale state here to print even by accident.
    let tree = Tree::new("gap");
    tree.segment(
        21,
        100,
        1,
        &[
            Frame::Snapshot(100),
            Frame::Delta(101),
            Frame::Gap,
            Frame::Delta(102),
        ],
    );

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 103),
        0,
    )
    .expect("the instant is covered");

    assert!(answer.book.is_none(), "no book across a gap");
    let reason = answer.no_book_reason.expect("and always a reason");
    assert!(reason.contains("blind since"), "{reason}");
    let gap = answer.gap_before.expect("the gap is reported");
    assert_eq!(gap.cause, GapCause::Disconnect);
    assert_eq!(gap.at, at(21, 102));
}

#[test]
fn a_gap_after_the_instant_says_when_the_blindness_began() {
    // The other direction, and the one an operator actually asks: things looked
    // fine at 03:14 -- when did they stop?
    let tree = Tree::new("gap-after");
    tree.segment(
        21,
        100,
        1,
        &[
            Frame::Snapshot(100),
            Frame::Delta(101),
            Frame::Gap,
            Frame::Delta(102),
        ],
    );

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 101),
        0,
    )
    .expect("covered");

    assert!(answer.book.is_some(), "still live at this instant");
    let next = answer.gap_after.expect("the coming gap is reported");
    assert_eq!(next.at, at(21, 102));
}

#[test]
fn an_unanchored_stretch_refuses_rather_than_inventing_a_book() {
    // A day's file begins mid-stream, so the book is unanchored until a snapshot
    // arrives -- up to an hour, because the recorder's periodic anchor is
    // hourly. An instant in that window has no book, and the reason must name
    // the missing anchor rather than leaving a blank.
    let tree = Tree::new("unanchored");
    tree.segment(21, 100, 1, &[Frame::Delta(101), Frame::Delta(102)]);

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 101),
        0,
    )
    .expect("covered -- there are events, just no anchor");

    assert!(answer.book.is_none());
    let reason = answer.no_book_reason.expect("a reason");
    assert!(reason.contains("anchor"), "{reason}");
}

#[test]
fn the_previous_day_is_read_when_the_instant_has_no_anchor_of_its_own() {
    // The self-correcting rule. Day 22 opens mid-stream with no snapshot of its
    // own; day 21 ends with one. Reading only day 22 would refuse; widening to
    // the previous day finds the anchor and the book is live.
    //
    // Verified to matter rather than assumed: `an_unanchored_stretch_refuses`
    // above is the same shape *without* a previous day, and it refuses.
    let tree = Tree::new("widen");
    tree.segment(21, 86_000, 1, &[Frame::Snapshot(100), Frame::Delta(101)]);
    // Sequence continues across midnight, as a real session's does.
    tree.segment(22, 100, 3, &[Frame::Delta(102), Frame::Delta(103)]);

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(22, 101),
        0,
    )
    .expect("covered");

    assert!(
        answer.book.is_some(),
        "the previous day's anchor should have been found: {:?}",
        answer.no_book_reason
    );
    assert_eq!(answer.segments.len(), 2, "both days were opened");
}

#[test]
fn an_empty_root_says_so_rather_than_answering_emptily() {
    let tree = Tree::new("empty");
    std::fs::create_dir_all(&tree.root).expect("mkdir");
    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 100),
        0,
    );
    assert!(answer.is_err(), "no capture is an error, not an empty book");
}

#[test]
fn a_symbol_that_was_never_captured_is_not_answered_from_another() {
    // Two symbols share a root in every real run. Answering about ETHUSDT with
    // BTCUSDT's book would be confabulation of the worst kind, because every
    // number in it would look right.
    let tree = Tree::new("other-symbol");
    tree.segment(21, 100, 1, &[Frame::Snapshot(100), Frame::Delta(101)]);

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        "ETHUSDT",
        instrument(),
        at(21, 101),
        0,
    );
    assert!(answer.is_err(), "a symbol with no capture must refuse");
}

#[test]
fn a_window_summarises_what_happened_across_the_span() {
    // Deltas at 100..103, so a 2-second window ending at 102 covers the events
    // at 101 and 102 and excludes the one at 100.
    let tree = Tree::new("window");
    tree.segment(
        21,
        100,
        1,
        &[
            Frame::Snapshot(100),
            Frame::Delta(101),
            Frame::Delta(102),
            Frame::Delta(103),
        ],
    );

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 102),
        2 * 1_000_000_000,
    )
    .expect("covered");

    // Half-open, `(at - span, at]`: the deltas at 101 and 102 are in, and the
    // snapshot exactly on the opening seam at 100 is out. Inclusive at both ends
    // would put a seam event into two adjacent windows, so stepping through a
    // run five minutes at a time would count it twice.
    let window = answer.window.expect("a window was asked for");
    assert_eq!(window.deltas, 2, "the deltas at 101 and 102");
    assert_eq!(window.snapshots, 0, "the snapshot is on the opening seam");
    assert_eq!(window.trades, 0);
}

#[test]
fn a_window_reports_the_gaps_inside_it() {
    // The question a span is usually asked in order to answer: were we blind at
    // any point in this stretch, not merely at its end?
    let tree = Tree::new("window-gap");
    tree.segment(
        21,
        100,
        1,
        &[
            Frame::Snapshot(100),
            Frame::Gap,
            Frame::Snapshot(200),
            Frame::Delta(201),
        ],
    );

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 103),
        10 * 1_000_000_000,
    )
    .expect("covered");

    // The book is live again -- the second snapshot re-anchored it -- so the
    // instant alone would say nothing was wrong. The window is what shows the
    // blindness in the middle.
    assert!(answer.book.is_some(), "re-anchored by the second snapshot");
    let window = answer.window.expect("a window");
    assert_eq!(window.gaps.len(), 1, "the gap inside the span is reported");
    assert_eq!(window.gaps[0].at, at(21, 101));
}

#[test]
fn no_window_asked_for_means_no_window_reported() {
    // The instant is the window of length zero and takes the same path, but it
    // must not *claim* a span it was not asked about.
    let tree = Tree::new("window-none");
    tree.segment(21, 100, 1, &[Frame::Snapshot(100), Frame::Delta(101)]);

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 101),
        0,
    )
    .expect("covered");
    assert!(answer.window.is_none());
}

#[test]
fn a_window_that_reaches_before_the_data_summarises_what_there_is() {
    // Not an error. Asking for the last hour of a run that started ten minutes
    // ago is an ordinary thing to do, and the honest answer is the ten minutes
    // -- the span is a filter on what was read, not a claim that the whole span
    // was covered.
    let tree = Tree::new("window-long");
    tree.segment(21, 100, 1, &[Frame::Snapshot(100), Frame::Delta(101)]);

    let answer = market_at(
        &tree.root,
        Exchange::Binance,
        SYMBOL,
        instrument(),
        at(21, 101),
        3_600 * 1_000_000_000,
    )
    .expect("covered");

    let window = answer.window.expect("a window");
    assert_eq!(window.deltas, 1);
    assert_eq!(window.snapshots, 1, "everything there is, and no more");
}
