//! Running a capture: connect, stamp, sequence, write.
//!
//! This was the body of the `record` binary until M5.d. It moved here for one
//! reason: a paper run has to record raw **and** feed a trading engine from the
//! same ingress, and `docs/engine-contract.md` §9 requires those to be the same
//! records with the same stamps. A second recorder process would mean two
//! subscriptions and two sets of timestamps, and the agreement criterion could
//! never be checked.
//!
//! # How the second consumer attaches
//!
//! [`run`] takes a closure that wraps the capture channel's sender. A plain
//! recorder passes the sender through unchanged; a paper run wraps it in a
//! `TeeSink`. That is the whole of the difference, and it keeps one code path
//! rather than two -- the recorder that spent seven days unattended at M1 is
//! still running exactly the same sequence of steps.
//!
//! Everything else here is M1's, moved rather than rewritten: the optional
//! metadata tier, the writer thread, the host-clock check, the periodic metrics,
//! and the shutdown that seals a trailer so a clean stop is distinguishable from
//! a kill.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::{connection, ConnectionPolicy, SnapshotClient, StreamSpec, SPOT_REST, SPOT_WS};
use quant_core::event::GapCause;
use quant_core::instrument::Exchange;
use quant_core::time::{Clock, SystemClock, Ts};
use quant_meta::store::DATABASE_URL_ENV;
use quant_meta::{
    connect, run_metadata, MetaEvent, MetaStats, SegmentRow, SessionClose, SessionRow,
    SessionStatus, DEFAULT_METADATA_CAPACITY,
};
use quant_recorder::{
    channel, format_session_id, run_writer, CaptureSession, FileStore, Ingress, IngressStats,
    Metrics, MetricsReporter, ObservedStore, SegmentReport, WriterOutcome,
    DEFAULT_CHANNEL_CAPACITY, DEFAULT_FLUSH_INTERVAL,
};
use quant_storage::WriterOptions;
use tokio::sync::mpsc::Sender as MetaSender;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Ceiling on one snapshot request.
///
/// A 5000-level book is around a megabyte, so this has to allow for a real
/// transfer, not just a round trip. Generous rather than tight: an abandoned
/// snapshot costs a book anchor, while a slow one costs nothing at all -- the
/// socket keeps draining throughout, by construction.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(15);

/// How often the recorder says how it is doing.
///
/// Sixty seconds is a compromise between two failure modes over a seven-day run.
/// Shorter and a week of logs is mostly noise, so nobody reads them and the one
/// line that mattered goes unseen. Longer and a transient stall -- the exact thing
/// queue depth exists to catch -- can begin and end inside one interval and never
/// appear at all. The peak figures are cumulative precisely so that a spike
/// between two readings is still visible in the next one.
const METRICS_INTERVAL: Duration = Duration::from_secs(60);

/// How far the host clock may drift from the venue's before it is a problem.
///
/// Binance's own number: it rejects a signed request whose timestamp is more than
/// a second from server time. Borrowing the venue's threshold rather than
/// inventing one means that when M8 places a real order, a clock this recorder
/// already warned about is the same clock that would have had the order refused.
const MAX_CLOCK_OFFSET_MS: i64 = 1_000;

/// The metadata tier, if one is configured.
///
/// Optional by design: `QUANT_DATABASE_URL` unset, or a database that will not
/// answer, means the recorder runs without an index. See `quant-meta`'s crate docs
/// -- market data is irreplaceable and an index row is not, so the index is allowed
/// to be absent and the capture is not allowed to stop.
struct Metadata {
    tx: MetaSender<MetaEvent>,
    task: JoinHandle<MetaStats>,
    /// Drives the Postgres connection. Aborted at shutdown.
    driver: JoinHandle<()>,
}

async fn setup_metadata() -> Option<Metadata> {
    let url = std::env::var(DATABASE_URL_ENV).ok()?;

    let (mut meta, driver) = match connect(&url).await {
        Ok(pair) => pair,
        Err(e) => {
            warn!(error = %e, "no metadata tier: could not connect; recording anyway");
            return None;
        }
    };
    if let Err(e) = meta.migrate().await {
        warn!(error = %e, "no metadata tier: migration failed; recording anyway");
        driver.abort();
        return None;
    }

    let (tx, rx) = tokio::sync::mpsc::channel(DEFAULT_METADATA_CAPACITY);
    Some(Metadata {
        tx,
        task: tokio::spawn(run_metadata(rx, meta)),
        driver,
    })
}

/// A file store that reports each sealed segment to the metadata tier, if there is
/// one.
///
/// One code path whether or not a database is configured: the observer holds an
/// `Option` and does nothing when there is nothing to report. Branching on the
/// store type instead would mean two copies of the whole recording path, differing
/// only in a generic parameter.
fn build_store(
    root: PathBuf,
    tx: Option<MetaSender<MetaEvent>>,
    clock: Arc<dyn Clock>,
) -> ObservedStore<FileStore, impl FnMut(&SegmentReport)> {
    ObservedStore::new(FileStore::new(&root), move |report: &SegmentReport| {
        let Some(tx) = tx.as_ref() else {
            return;
        };
        let path = report.target.file(&root).to_string_lossy().into_owned();
        match SegmentRow::from_report(report, path, clock.now()) {
            // try_send, never send: this runs on the writer thread, and blocking
            // here would stall block sealing, back up the capture channel and drop
            // market data. Losing an index row is the cheaper failure.
            Ok(row) => {
                if tx.try_send(MetaEvent::SegmentSealed(row)).is_err() {
                    error!("metadata channel full or closed; segment not indexed");
                }
            }
            Err(e) => error!(error = %e, "could not build a segment row"),
        }
    })
}

/// Mark the session finished and drain the metadata task.
///
/// Called last, after every segment has been reported, so the row flips to `closed`
/// only once the index is actually complete.
async fn close_metadata(
    metadata: Metadata,
    session_id: [u8; 16],
    stats: IngressStats,
    backdated: u64,
    failure: Option<String>,
    ended_at: Ts,
) -> Result<(), Box<dyn std::error::Error>> {
    let close = SessionClose {
        ended_at: quant_meta::rows::to_offset(ended_at)?,
        // `failed` when the recorder died, so a session that fell over is
        // distinguishable from one that was asked to stop.
        status: if failure.is_some() {
            SessionStatus::Failed
        } else {
            SessionStatus::Closed
        },
        messages: saturating(stats.messages),
        venue_bytes: saturating(stats.bytes),
        dropped: saturating(stats.dropped),
        gaps_recorded: saturating(stats.gaps_recorded),
        gaps_abandoned: saturating(stats.gaps_abandoned),
        backdated: saturating(backdated),
        note: failure,
    };

    let id = uuid::Uuid::from_bytes(session_id);
    if metadata
        .tx
        .send(MetaEvent::SessionClosed { id, close })
        .await
        .is_err()
    {
        warn!("metadata task is gone; session left marked running");
    }
    // Dropping the last sender is what ends the metadata task.
    drop(metadata.tx);
    match metadata.task.await {
        Ok(stats) => info!(
            applied = stats.applied,
            failed = stats.failed,
            "metadata closed"
        ),
        Err(e) => error!(error = %e, "metadata task panicked"),
    }
    metadata.driver.abort();
    Ok(())
}

/// What to capture, and for how long.
#[derive(Debug, Clone)]
pub struct CaptureConfig {
    /// The venue's own symbol, verbatim.
    pub symbol: String,
    /// Data root; the capture lands under `<root>/raw/...`.
    pub root: PathBuf,
    /// Stop after this long. `None` runs until interrupted.
    pub limit: Option<Duration>,
}

impl CaptureConfig {
    #[must_use]
    pub fn new(symbol: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self {
            symbol: symbol.into(),
            root: root.into(),
            limit: None,
        }
    }

    #[must_use]
    pub const fn for_at_most(mut self, limit: Duration) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Record a symbol until interrupted, the limit expires, or something fatal.
///
/// `wrap` turns the capture channel's sender into the sink ingress will use. A
/// plain recorder passes it through (`|tx| tx`); a paper run returns a
/// `TeeSink` so the engine sees the identical records. Confining the difference
/// to one closure is what keeps the seven-day-proven path and the paper path the
/// same code.
///
/// # Errors
///
/// Only for conditions a recorder cannot continue through: no snapshot client,
/// the writer thread dying, or the connection loop giving up. Ordinary
/// disconnects are gaps, not errors.
pub async fn run<S, F>(config: CaptureConfig, wrap: F) -> Result<(), Box<dyn std::error::Error>>
where
    S: quant_recorder::sink::RecordSink,
    F: FnOnce(quant_recorder::CaptureSender) -> S,
{
    let CaptureConfig {
        symbol,
        root,
        limit,
    } = config;

    // The one and only clock. Everything that stamps a timestamp takes this, so
    // that the same code can be driven by a replayed event stream later.
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let started = clock.now();

    let session_id = *uuid::Uuid::new_v4().as_bytes();
    let metadata = setup_metadata().await;

    // Recorded before capture begins, so a killed recorder leaves a `running` row.
    // That row and the missing file trailer are two independent records of the same
    // fact, which is what lets them be cross-checked.
    if let Some(meta) = &metadata {
        match SessionRow::new(session_id, Exchange::Binance, symbol.clone(), started) {
            Ok(row) => {
                if meta.tx.send(MetaEvent::SessionOpened(row)).await.is_err() {
                    warn!("metadata task is gone; session will not be indexed");
                }
            }
            Err(e) => warn!(error = %e, "could not build the session row"),
        }
    }

    let store = build_store(
        root.clone(),
        metadata.as_ref().map(|m| m.tx.clone()),
        Arc::clone(&clock),
    );

    // The session owns file creation and day rolling. No file is opened here: the
    // first record decides which day it belongs to, so a recorder started at
    // 23:59:59.9 does not create a file it will never write to.
    let mut session = CaptureSession::new(
        store,
        Exchange::Binance,
        symbol.clone(),
        session_id,
        Arc::clone(&clock),
        WriterOptions::default(),
    );

    // One set of counters, shared by the connection task that fills them, the
    // writer thread that drains the queue, and the reporter that reads them.
    let metrics = Metrics::new(DEFAULT_CHANNEL_CAPACITY);
    let (tx, rx) = channel(DEFAULT_CHANNEL_CAPACITY, Arc::clone(&metrics));
    // A plain thread, not spawn_blocking: this runs for the life of the process,
    // and parking a tokio blocking-pool slot forever is not what that pool is for.
    //
    // The session moves in and comes back out, because sealing the final segment
    // has to happen after the loop returns and must be able to report an error.
    let writer_thread = thread::Builder::new()
        .name(format!("capture-writer-{symbol}"))
        .spawn(move || {
            let outcome = run_writer(&rx, &mut session, DEFAULT_FLUSH_INTERVAL);
            (outcome, session)
        })?;

    let mut ingress = Ingress::new(wrap(tx), Arc::clone(&clock), Arc::clone(&metrics));

    // A separate task, because the connection future borrows `ingress` for the
    // life of the run and nothing else can reach it. Aborted at shutdown; the
    // final numbers are logged below from the same counters.
    let reporter = tokio::spawn(report_periodically(Arc::clone(&metrics), symbol.clone()));

    // First record in every file. Without it, a capture that resumes after a crash
    // would silently abut the previous one and look like continuous coverage.
    ingress.record_gap(GapCause::RecorderRestart)?;

    let spec = StreamSpec::market_data(&symbol);
    let policy = ConnectionPolicy::default();
    // Built once, up front, and a failure here is fatal rather than warned about.
    // Unlike the metadata tier, this is not optional: without snapshots the depth
    // deltas are recorded faithfully and can never be turned into a book, so a
    // recorder that quietly ran without one would produce a week of data that
    // looks complete and is not usable.
    let snapshots = SnapshotClient::new(SPOT_REST, SNAPSHOT_TIMEOUT)?;
    check_host_clock(&snapshots, clock.as_ref()).await;
    // The session id, not a path: with day rolling a run can produce several
    // files, and the session is what identifies them all. Each is logged as it
    // is sealed.
    info!(
        %symbol,
        root = %root.display(),
        session = %format_session_id(&session_id),
        "recording; ctrl-c to stop"
    );

    let outcome = tokio::select! {
        result = connection::run(&spec, SPOT_WS, &mut ingress, &policy, Some(&snapshots)) => {
            // Only returns on a fatal condition -- normal disconnects are gaps.
            result.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
        }
        result = tokio::signal::ctrl_c() => {
            result?;
            info!("shutdown requested");
            Ok(())
        }
        () = until(limit) => {
            info!(?limit, "duration limit reached");
            Ok(())
        }
    };

    reporter.abort();
    let stats = ingress.stats();
    // Closes the channel, which is how the writer knows to seal the trailer.
    drop(ingress);

    let (written, session) = writer_thread
        .join()
        .map_err(|_| "capture writer thread panicked")?;
    let written = written?;

    let backdated = session.backdated_records();
    let rolls = session.rolls();
    // Seals the final segment with its trailer. This is the step that makes a
    // clean shutdown distinguishable from a kill.
    let (segments, store) = session.finish()?;

    report_capture(
        &segments,
        store.inner().root(),
        stats,
        written,
        rolls,
        backdated,
        clock.now().delta_nanos(started) / 1_000_000_000,
    );
    report_lifetime(&metrics);

    // The observer closure inside the store holds a clone of the metadata sender.
    // It has to be dropped before we await the metadata task below, or `recv`
    // never sees the channel close and the task never returns -- the recorder
    // would hang at shutdown having written all its data correctly.
    drop(store);

    if let Some(meta) = metadata {
        let failure = outcome.as_ref().err().map(ToString::to_string);
        close_metadata(meta, session_id, stats, backdated, failure, clock.now()).await?;
    }
    // Durability is FileStore's job: it fsyncs each segment as it is sealed, once
    // per file rather than per block.
    outcome
}

/// Compare the host clock against the venue's, once, before recording starts.
///
/// Not fatal, because a wrong clock does not make the capture wrong: every
/// `local_recv_ts` is shifted by the same amount, so ordering and dispatch -- the
/// only things the engine acts on -- are unaffected. What it does ruin is every
/// *latency* figure, and cross-venue comparison later.
///
/// So it is a loud warning and a recorded fact, not a refusal to start. Refusing
/// would mean a stopped recorder over a problem that costs a metric, and the data
/// is the thing that cannot be recreated.
async fn check_host_clock(snapshots: &SnapshotClient, clock: &dyn Clock) {
    let local_millis = clock.now().as_nanos() / 1_000_000;
    match snapshots.clock_offset_millis(local_millis).await {
        Ok(offset) if offset.abs() > MAX_CLOCK_OFFSET_MS => warn!(
            offset_ms = offset,
            limit_ms = MAX_CLOCK_OFFSET_MS,
            "host clock disagrees with the venue: every venue-latency figure from \
             this run is wrong by this much, and exchange_ts comparisons with it \
             are meaningless. Sync the host clock before a long capture."
        ),
        Ok(offset) => info!(offset_ms = offset, "host clock agrees with the venue"),
        // The snapshot fetch will report the same outage in a moment, and this
        // check is not worth failing a capture over.
        Err(e) => warn!(error = %e, "could not check the host clock against the venue"),
    }
}

/// Log a metrics line every [`METRICS_INTERVAL`] until aborted.
///
/// The point of a *periodic* line rather than a total at the end: over seven days,
/// a total cannot say when something changed, and the questions these metrics
/// exist to answer -- is the venue still talking to us, is the writer keeping up,
/// did latency degrade -- are all questions about a moment.
async fn report_periodically(metrics: Arc<Metrics>, symbol: String) {
    let mut reporter = MetricsReporter::new(metrics);
    let mut ticker = tokio::time::interval(METRICS_INTERVAL);
    // The first tick fires immediately and would report a zero-length interval.
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let report = reporter.report(METRICS_INTERVAL);
        let latency = report.latency;
        info!(
            symbol = %symbol,
            msgs_per_sec = report.messages_per_sec,
            bytes_per_sec = report.bytes_per_sec,
            queue = report.queue_depth,
            queue_peak = report.queue_high_water,
            queue_capacity = report.queue_capacity,
            dropped = report.dropped,
            latency_p50_ms = latency.p50_micros / 1_000,
            latency_p90_ms = latency.p90_micros / 1_000,
            latency_p99_ms = latency.p99_micros / 1_000,
            latency_max_ms = latency.max_micros / 1_000,
            latency_samples = latency.count,
            clock_skew = latency.clock_skew,
            gap_disconnect = report.gaps[GapCause::Disconnect.index()],
            gap_overflow = report.gaps[GapCause::LocalOverflow.index()],
            gap_sequence = report.gaps[GapCause::SequenceGap.index()],
            "metrics"
        );

        if latency.clock_skew > 0 {
            // §2: a venue timestamp ahead of ours means the host clock is wrong,
            // which makes every latency figure and every `local_recv_ts` suspect.
            error!(
                symbol = %symbol,
                samples = latency.clock_skew,
                "venue timestamps are ahead of ours: check the host clock"
            );
        }
    }
}

/// The whole-run numbers, once the capture has stopped.
fn report_lifetime(metrics: &Metrics) {
    let sample = metrics.sample();
    let latency = sample.lifetime_latency;
    info!(
        queue_peak = sample.queue_high_water,
        queue_capacity = sample.queue_capacity,
        latency_p50_ms = latency.p50_micros / 1_000,
        latency_p90_ms = latency.p90_micros / 1_000,
        latency_p99_ms = latency.p99_micros / 1_000,
        latency_max_ms = latency.max_micros / 1_000,
        latency_samples = latency.count,
        clock_skew = latency.clock_skew,
        "venue latency over the whole run"
    );
    for cause in GapCause::ALL {
        let count = sample.gaps_by_cause[cause.index()];
        if count > 0 {
            info!(cause = cause.name(), count, "gaps by cause");
        }
    }
}

/// Log what the capture produced, per segment and in total.
fn report_capture(
    segments: &[SegmentReport],
    root: &Path,
    stats: IngressStats,
    written: WriterOutcome,
    rolls: u64,
    backdated: u64,
    seconds: i64,
) {
    for segment in segments {
        info!(
            path = %segment.target.file(root).display(),
            date = %segment.target.date,
            frames = segment.stats.frames,
            blocks = segment.stats.blocks,
            file_bytes = segment.stats.file_bytes,
            venue_bytes = segment.stats.frame_bytes,
            ingest_seq = ?(segment.first_ingest_seq, segment.last_ingest_seq),
            "segment sealed"
        );
    }

    info!(
        messages = stats.messages,
        venue_bytes = stats.bytes,
        dropped = stats.dropped,
        gaps = stats.gaps_recorded,
        gaps_abandoned = stats.gaps_abandoned,
        snapshot_failures = stats.snapshot_failures,
        snapshots = stats.snapshots,
        snapshot_bytes = stats.snapshot_bytes,
        snapshots_dropped = stats.snapshots_dropped,
        segments = segments.len(),
        day_rolls = rolls,
        records = written.records,
        timed_flushes = written.timed_flushes,
        seconds,
        "capture closed"
    );

    if stats.dropped > 0 {
        // Not an error -- it is recorded honestly and the hole in ingest_seq proves
        // how much -- but it means the channel or the disk needs sizing, so it must
        // not be buried among the numbers above.
        error!(
            dropped = stats.dropped,
            "messages were dropped: recorder could not keep up"
        );
    }
    if backdated > 0 {
        // The host clock stepped backwards across a midnight boundary. Nothing was
        // lost, but some records are filed under the following day -- and a machine
        // whose clock jumps is a machine whose latency measurements are suspect.
        error!(
            backdated,
            "records arrived stamped before the open segment's day: check the host clock"
        );
    }
}

/// `u64` counter into the `bigint` the metadata schema uses.
fn saturating(value: u64) -> i64 {
    quant_meta::rows::saturating_i64(value)
}

/// Completes after `limit`, or never if there is no limit.
///
/// `pending()` rather than an `Option` branch in the `select!`, so the no-limit
/// case is a branch that simply never fires instead of a disabled arm.
async fn until(limit: Option<Duration>) {
    match limit {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending().await,
    }
}
