//! Integration tests against a real Postgres.
//!
//! These need a database, so they read `QUANT_DATABASE_URL` and skip with a notice
//! when it is absent. Skipping rather than failing keeps `cargo test` usable
//! without Docker running; CI sets the variable, so they are guaranteed to run
//! somewhere. A test that silently vanishes everywhere would be worse than no test,
//! which is why the skip prints.
//!
//! Every test isolates itself with a fresh session UUID rather than truncating
//! tables, so they can run concurrently and against a database that already holds
//! real capture history.
//!
//! ```bash
//! docker compose up -d
//! QUANT_DATABASE_URL=postgres://quant:quant_local_dev@localhost:5432/quant \
//!   cargo test -p quant-meta
//! ```

use quant_core::instrument::Exchange;
use quant_core::time::{Ts, UtcDate};
use quant_meta::rows::{to_offset, SessionStatus};
use quant_meta::{
    connect, run_metadata, MetaEvent, SegmentRow, SessionClose, SessionRow,
    DEFAULT_METADATA_CAPACITY,
};
use quant_recorder::{CaptureTarget, SegmentReport};
use quant_storage::WriterStats;
use uuid::Uuid;

const URL_ENV: &str = "QUANT_DATABASE_URL";

/// Returns the URL, or `None` after printing why the test is being skipped.
fn database_url() -> Option<String> {
    let url = std::env::var(URL_ENV).ok();
    if url.is_none() {
        eprintln!("skipping: {URL_ENV} is not set (run `docker compose up -d` and set it)");
    }
    url
}

fn report(session_id: [u8; 16], day: u8, part: u32, frames: u64) -> SegmentReport {
    SegmentReport {
        target: CaptureTarget {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            date: UtcDate {
                year: 2026,
                month: 7,
                day,
            },
            session_id,
            part,
        },
        stats: WriterStats {
            frames,
            blocks: 7,
            frame_bytes: 1_222_498,
            file_bytes: 145_131,
        },
        first_ingest_seq: Some(1),
        last_ingest_seq: Some(frames),
    }
}

/// Connect and migrate. Migrations are idempotent, so every test may call this.
async fn open() -> Option<(quant_meta::Meta, [u8; 16])> {
    let url = database_url()?;
    let (mut meta, _driver) = connect(&url).await.expect("connect");
    let ran = meta.migrate().await.expect("migrate");
    // First run applies them, later runs apply none. Both are correct; what must
    // never happen is an error.
    assert!(ran <= quant_meta::MIGRATIONS.len());
    let session_id = *Uuid::new_v4().as_bytes();
    Some((meta, session_id))
}

#[tokio::test]
async fn migrations_are_idempotent() {
    let Some(url) = database_url() else { return };
    let (mut meta, _driver) = connect(&url).await.expect("connect");
    meta.migrate().await.expect("first migrate");
    let second = meta.migrate().await.expect("second migrate");
    assert_eq!(second, 0, "a second migrate must be a no-op, not an error");
}

#[tokio::test]
async fn a_session_and_its_segments_round_trip() {
    let Some((meta, session_id)) = open().await else {
        return;
    };
    let uuid = Uuid::from_bytes(session_id);

    let session = SessionRow::new(
        session_id,
        Exchange::Binance,
        "BTCUSDT",
        Ts::from_secs(1_785_283_200),
    )
    .unwrap();
    meta.open_session(&session).await.expect("open session");

    // A session that spans midnight produces two segments, and they must come back
    // in capture order.
    for (day, frames) in [(29_u8, 3350_u64), (30, 1200)] {
        let row = SegmentRow::from_report(
            &report(session_id, day, 0, frames),
            format!("data/raw/date=2026-07-{day:02}/part-00000.bin.zst"),
            Ts::from_secs(1_785_283_200),
        )
        .unwrap();
        meta.record_segment(&row).await.expect("record segment");
    }

    let segments = meta.segments_for(uuid).await.expect("query segments");
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].1, 3350, "ordered by capture date");
    assert_eq!(segments[1].1, 1200);
    assert!(segments[0].0.contains("2026-07-29"));

    // Still running until explicitly closed -- which is how a killed recorder is
    // found later.
    let running = meta.running_sessions().await.expect("running");
    assert!(
        running.iter().any(|(id, _, _)| *id == uuid),
        "an unclosed session must show as running"
    );

    meta.close_session(
        uuid,
        &SessionClose {
            ended_at: to_offset(Ts::from_secs(1_785_369_600)).unwrap(),
            status: SessionStatus::Closed,
            messages: 4550,
            venue_bytes: 1_600_000,
            dropped: 0,
            gaps_recorded: 1,
            gaps_abandoned: 0,
            backdated: 0,
            note: None,
        },
    )
    .await
    .expect("close session");

    let running = meta.running_sessions().await.expect("running");
    assert!(
        !running.iter().any(|(id, _, _)| *id == uuid),
        "a closed session must not still look running"
    );
}

#[tokio::test]
async fn recording_the_same_segment_twice_updates_rather_than_duplicating() {
    // The natural key is (session, date, part), so a retry after a transient
    // failure must be an upsert. A duplicate row would make the index disagree
    // with a directory that physically cannot hold two files for one target.
    let Some((meta, session_id)) = open().await else {
        return;
    };
    let uuid = Uuid::from_bytes(session_id);
    meta.open_session(
        &SessionRow::new(session_id, Exchange::Binance, "BTCUSDT", Ts::from_secs(0)).unwrap(),
    )
    .await
    .expect("open");

    let first = SegmentRow::from_report(
        &report(session_id, 29, 0, 100),
        "data/first.bin.zst",
        Ts::from_secs(0),
    )
    .unwrap();
    meta.record_segment(&first).await.expect("first");

    let corrected = SegmentRow::from_report(
        &report(session_id, 29, 0, 3350),
        "data/first.bin.zst",
        Ts::from_secs(0),
    )
    .unwrap();
    meta.record_segment(&corrected).await.expect("retry");

    let segments = meta.segments_for(uuid).await.expect("query");
    assert_eq!(segments.len(), 1, "the retry must not duplicate the row");
    assert_eq!(segments[0].1, 3350, "the retry must win");
}

#[tokio::test]
async fn a_segment_for_an_unknown_session_is_refused() {
    // The foreign key is what stops the index accumulating orphan segments that
    // reference a session nobody recorded.
    let Some((meta, _)) = open().await else {
        return;
    };
    let orphan = *Uuid::new_v4().as_bytes();
    let row = SegmentRow::from_report(
        &report(orphan, 29, 0, 10),
        "data/orphan.bin.zst",
        Ts::from_secs(0),
    )
    .unwrap();
    assert!(
        meta.record_segment(&row).await.is_err(),
        "a segment must not outlive its session row"
    );
}

#[tokio::test]
async fn the_task_survives_a_failing_write_and_reports_it() {
    // The core promise of the metadata tier: a bad write is counted and stepped
    // over, never propagated. Here the failure is a genuine constraint violation
    // (a segment with no session), which is exactly the shape a bug would take.
    let Some((meta, session_id)) = open().await else {
        return;
    };
    let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_METADATA_CAPACITY);
    let task = tokio::spawn(run_metadata(rx, meta));

    // Good event, then a doomed one, then another good one.
    tx.send(MetaEvent::SessionOpened(
        SessionRow::new(session_id, Exchange::Binance, "BTCUSDT", Ts::from_secs(0)).unwrap(),
    ))
    .await
    .unwrap();
    tx.send(MetaEvent::SegmentSealed(
        SegmentRow::from_report(
            &report(*Uuid::new_v4().as_bytes(), 29, 0, 1),
            "data/doomed.bin.zst",
            Ts::from_secs(0),
        )
        .unwrap(),
    ))
    .await
    .unwrap();
    tx.send(MetaEvent::SegmentSealed(
        SegmentRow::from_report(
            &report(session_id, 29, 0, 42),
            "data/good.bin.zst",
            Ts::from_secs(0),
        )
        .unwrap(),
    ))
    .await
    .unwrap();
    drop(tx);

    let stats = task.await.expect("task must not panic");
    assert_eq!(stats.failed, 1, "the doomed write should be counted");
    assert_eq!(
        stats.applied, 2,
        "a failure must not stop the events after it"
    );
}
