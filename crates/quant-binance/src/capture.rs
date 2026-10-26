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
//!
//! # Where, by which clock, and until when
//!
//! M8's live binary added three things to [`CaptureConfig`] and one way in,
//! each defaulted to what `record` and `paper` always did. The endpoints, so
//! that a run can capture Spot Testnet or a local stand-in
//! (`crate::fake_market`) rather than the live venue. The clock, so that one
//! clock stamps the market, the account stream and the request worker
//! (`docs/live-run.md` §5, *`LiveVenue`, decided before the code*, §4, and §6
//! duty 2). And [`run_until`], a stop that is not ctrl-c, which [`run`] now
//! calls with ctrl-c.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::account::{listen, AccountPolicy, Stopped};
use crate::credentials::Credentials;
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
    LastDropped, Metrics, MetricsLine, MetricsReporter, ObservedStore, SegmentReport,
    WriterOutcome, DEFAULT_CHANNEL_CAPACITY, DEFAULT_FLUSH_INTERVAL,
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

/// How far the host clock may be from the venue's before the recorder warns.
///
/// Borrowed from the venue rather than invented, but from one side of a rule
/// that has two. Binance refuses a signed request whose timestamp is a second or
/// more *ahead* of its own clock, and one *behind* it only once it is older than
/// `recvWindow` -- five seconds by default. So a second is the venue's whole
/// tolerance in one direction and a fifth of it in the other.
///
/// The same number as [`crate::trade::MAX_CLOCK_SKEW_MS`], and not the same
/// verdict, because it is not judging the same measurement. This one judges a
/// single uncorrected reading, `local_millis - serverTime` on a fresh client's
/// first request, which comes out short by the whole outbound leg and whatever
/// DNS, TCP and TLS added to it ([`SnapshotClient::clock_offset_millis`] says
/// why that is left alone). The trading gate judges the round-trip midpoint of
/// the tightest of several readings, as `venue-check` takes them
/// ([`crate::trade::ClockSample`]). So the two can disagree in either
/// direction. A host exactly on time behind a 1200 ms cold first leg reads
/// -1200 ms here and draws a warning, while the gate, reading it warm, finds it
/// on time and passes it; one 1200 ms ahead behind a 300 ms leg reads 900 ms
/// here, draws no warning, and is refused by the gate. A warning here says the
/// latency figures want a second look. Neither the warning nor its absence says
/// whether this host could trade.
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
    /// Where the market stream is subscribed: [`SPOT_WS`] unless a run says
    /// otherwise.
    pub ws_base: String,
    /// Where depth snapshots and the clock are asked for: [`SPOT_REST`]
    /// unless a run says otherwise.
    pub rest_base: String,
    /// The clock every record is stamped by. A live run hands the same one to
    /// its account stream and its request worker (`docs/live-run.md` §5,
    /// *`LiveVenue`, decided before the code*, §4): one clock stamps
    /// `local_recv_ts` everywhere. The wall clock unless a run says otherwise.
    pub clock: Arc<dyn Clock>,
}

impl CaptureConfig {
    /// The live venue's market stream and REST endpoints, stamped by the wall
    /// clock: what `record` and `paper` have always captured.
    #[must_use]
    pub fn new(symbol: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self {
            symbol: symbol.into(),
            root: root.into(),
            limit: None,
            ws_base: SPOT_WS.to_owned(),
            rest_base: SPOT_REST.to_owned(),
            clock: Arc::new(SystemClock),
        }
    }

    #[must_use]
    pub const fn for_at_most(mut self, limit: Duration) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Subscribe at `ws_base` and ask `rest_base` for snapshots instead: Spot
    /// Testnet, or a local stand-in for a rehearsal.
    #[must_use]
    pub fn at(mut self, ws_base: impl Into<String>, rest_base: impl Into<String>) -> Self {
        self.ws_base = ws_base.into();
        self.rest_base = rest_base.into();
        self
    }

    /// Stamp every record with `clock`, the one the rest of the run stamps
    /// with.
    #[must_use]
    pub fn stamped_by(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
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
    run_until(config, wrap, tokio::signal::ctrl_c()).await
}

/// [`run`], stopped by `stop` rather than by ctrl-c: what a run that has its
/// own way of being told to stop passes, a test among them.
///
/// `stop` completing with `Ok` is a requested shutdown, sealed as one; with
/// `Err`, the capture stops and the error is returned, as [`run`] returns a
/// failure to listen for ctrl-c.
///
/// # Errors
///
/// As [`run`].
pub async fn run_until<S, F, Stop>(
    config: CaptureConfig,
    wrap: F,
    stop: Stop,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: quant_recorder::sink::RecordSink,
    F: FnOnce(quant_recorder::CaptureSender) -> S,
    Stop: core::future::Future<Output = std::io::Result<()>>,
{
    let CaptureConfig {
        symbol,
        root,
        limit,
        ws_base,
        rest_base,
        clock,
    } = config;

    // The one and only clock. Everything that stamps a timestamp takes this, so
    // that the same code can be driven by a replayed event stream later.
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
    let snapshots = SnapshotClient::new(rest_base, SNAPSHOT_TIMEOUT)?;
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
        result = connection::run(&spec, &ws_base, &mut ingress, &policy, Some(&snapshots)) => {
            // Only returns on a fatal condition -- normal disconnects are gaps.
            result.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
        }
        result = stop => {
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

/// Where an account capture records, whom it listens to, and by which clock.
#[derive(Debug, Clone)]
pub struct AccountConfig {
    /// Data root; the capture lands under `<root>/account/...`, never beside
    /// the market's (`docs/live-run.md` §5).
    pub root: PathBuf,
    /// The WebSocket API: [`crate::account::SPOT_WS_API`], Spot Testnet's, or
    /// a local stand-in.
    pub url: String,
    pub policy: AccountPolicy,
    /// The host clock's measured offset from the venue's, positive when the
    /// host is ahead: the same number the order path takes off, which every
    /// logon is stamped with (`docs/live-run.md` §5, §6 duty 2).
    pub offset_ms: i64,
    /// The clock every record is stamped by: the run's one clock.
    pub clock: Arc<dyn Clock>,
}

/// How an account capture ended.
#[derive(Debug)]
pub struct AccountCaptured {
    /// Why the listener stopped by itself, or `None` when it was stopped.
    pub stopped: Option<Stopped>,
    pub stats: IngressStats,
    /// The segments sealed, each with its trailer.
    pub segments: Vec<SegmentReport>,
}

/// Why an account capture could not record or seal.
#[derive(Debug)]
pub enum AccountCaptureError {
    /// The writer thread was gone before the session's opening gap.
    WriterGone,
    /// The writer thread panicked.
    WriterPanicked,
    /// The writer, or sealing the last segment, failed.
    Storage(quant_storage::StorageError),
}

impl std::fmt::Display for AccountCaptureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WriterGone => f.write_str("the account capture's writer was gone at the start"),
            Self::WriterPanicked => f.write_str("the account capture's writer thread panicked"),
            Self::Storage(e) => write!(f, "the account capture could not write or seal: {e}"),
        }
    }
}

impl std::error::Error for AccountCaptureError {}

/// Record the account stream into its own data root until `stop` completes
/// or the listener stops for good, handing every record to `consumer` as
/// well: the run's `AccountFeed`, behind a [`TeeSink`](quant_recorder::TeeSink)
/// with the capture primary, as the market's engine copy is.
///
/// What a live run wires (`docs/live-run.md` §5, *`LiveVenue`, decided before
/// the code*, §6 duties 2 and 3): one clock, the measured offset, and the tee
/// marking its drops into `mark` so the feed tells a hole while the account
/// is quiet ([`AccountFeed::new`](crate::account::AccountFeed::new) requires
/// it). `mark` is optional only for a test that reads the consumer raw. The
/// session's opening `Gap{RecorderRestart}` is recorded here, as
/// [`run_until`] records the market's, and the capture is sealed with its
/// trailer however listening ended, so a clean stop never reads as a crash.
///
/// # Errors
///
/// [`AccountCaptureError`], for a capture that could not be written or
/// sealed. A listener that stopped by itself is not an error: it is
/// [`AccountCaptured::stopped`], and the gap it recorded is the loss.
pub async fn record_account<Stop>(
    config: AccountConfig,
    credentials: &Credentials,
    consumer: std::sync::mpsc::SyncSender<quant_recorder::CaptureRecord>,
    mark: Option<Arc<LastDropped>>,
    stop: Stop,
) -> Result<AccountCaptured, AccountCaptureError>
where
    Stop: core::future::Future<Output = ()>,
{
    let AccountConfig {
        root,
        url,
        policy,
        offset_ms,
        clock,
    } = config;
    let mut session = CaptureSession::account(
        FileStore::new(root),
        Exchange::Binance,
        Arc::clone(&clock),
        WriterOptions::default(),
    );
    let metrics = Metrics::new(DEFAULT_CHANNEL_CAPACITY);
    let (tx, rx) = channel(DEFAULT_CHANNEL_CAPACITY, Arc::clone(&metrics));
    let writer = thread::Builder::new()
        .name("capture-writer-account".to_owned())
        .spawn(move || {
            let outcome = run_writer(&rx, &mut session, DEFAULT_FLUSH_INTERVAL);
            (outcome, session)
        })
        .map_err(|e| AccountCaptureError::Storage(quant_storage::StorageError::Io(e)))?;
    let tee = quant_recorder::TeeSink::new(tx, consumer);
    let tee = match mark {
        Some(mark) => tee.marking_into(mark),
        None => tee,
    };
    let mut ingress = Ingress::new(tee, clock, metrics);
    // First record in every file, as for the market.
    let opened = ingress.record_gap(GapCause::RecorderRestart);

    let stopped = if opened.is_ok() {
        tokio::select! {
            stopped = listen(&url, credentials, &mut ingress, &policy, offset_ms) => Some(stopped),
            () = stop => None,
        }
    } else {
        None
    };
    let stats = ingress.stats();
    // Closes the channel, which is how the writer knows to seal the trailer.
    drop(ingress);
    let (outcome, session) = tokio::task::spawn_blocking(move || writer.join())
        .await
        .map_err(|_| AccountCaptureError::WriterPanicked)?
        .map_err(|_| AccountCaptureError::WriterPanicked)?;
    outcome.map_err(AccountCaptureError::Storage)?;
    let (segments, _store) = session.finish().map_err(AccountCaptureError::Storage)?;
    if opened.is_err() {
        return Err(AccountCaptureError::WriterGone);
    }
    if let Some(why) = &stopped {
        error!(%why, "the account listener stopped by itself; the account is not heard");
    }
    Ok(AccountCaptured {
        stopped,
        stats,
        segments,
    })
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
        // One serialized value rather than eighteen loose fields. `explain` reads
        // the same `MetricsLine` back, so the two sides share a *type* instead of
        // a list of names they must each keep in step -- which is the whole of
        // the debt M7 scheduled for repayment here.
        MetricsLine::from_report(&symbol, &report).emit();

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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use quant_core::time::{ManualClock, Ts};
    use quant_recorder::catalog;
    use quant_storage::{FrameKind, RawReader};

    use super::{record_account, run_until, AccountConfig, CaptureConfig};
    use crate::account::{AccountFeed, AccountRecord, Polled};
    use crate::fake_market::{self, FakeMarket};
    use crate::fake_venue::{self, FakeVenue};
    use quant_core::event::GapCause;
    use quant_recorder::{account_catalog, LastDropped};

    /// A data root of this test's own, named for this process and cleared, so
    /// that a root a reused process id left behind is never read as this one.
    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "quant-binance-capture-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    /// A REST base nothing answers: a port bound and let go.
    fn nowhere() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        format!("http://{}", listener.local_addr().expect("address"))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_capture_subscribes_where_it_is_told_stamps_by_the_clock_it_is_given_and_stops_when_asked(
    ) {
        // The three things a rehearsal needs of the capture that `record` and
        // `paper` never did: another endpoint, another clock, and a stop that
        // is not ctrl-c. Every record is stamped by the clock handed in, which
        // a live run shares with its account stream and its request worker.
        let root = scratch("endpoints");
        let market = FakeMarket::start().await;
        let at = Ts::from_nanos(1_789_000_000_000_000_000);
        let clock = Arc::new(ManualClock::new(at));
        let frames = [
            fake_market::trade("BTCUSDT", 1, "76650.12", "0.01", 1_000),
            fake_market::trade("BTCUSDT", 2, "76650.13", "0.02", 1_001),
        ];
        for frame in &frames {
            market.send(frame);
        }
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let config = CaptureConfig::new("BTCUSDT", &root)
            .at(market.url(), nowhere())
            .stamped_by(clock);
        // A second consumer, as paper's engine is, to see the records arrive:
        // the capture's own file is not readable until it is sealed.
        let (side_tx, side) = std::sync::mpsc::sync_channel(64);
        let capture = run_until(
            config,
            |tx| quant_recorder::TeeSink::new(tx, side_tx),
            async {
                let _ = stopped.await;
                Ok(())
            },
        );
        let drive = async {
            market
                .until("both frames sent", Duration::from_secs(20), |m| {
                    m.sent().len() == frames.len()
                })
                .await;
            // Sent is not recorded. Wait until ingress has handed both on, the
            // session's opening gap first: what ingress has taken, the writer
            // seals on the way out.
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            let mut handed = 0;
            while handed < 1 + frames.len() {
                assert!(std::time::Instant::now() < deadline, "not handed on");
                match side.try_recv() {
                    Ok(_) => handed += 1,
                    Err(_) => tokio::time::sleep(Duration::from_millis(2)).await,
                }
            }
            let _ = stop.send(());
        };
        let (outcome, ()) = tokio::time::timeout(
            Duration::from_secs(30),
            Box::pin(async { tokio::join!(capture, drive) }),
        )
        .await
        .expect("the capture stops when asked");
        outcome.expect("a clean stop");

        let cat = catalog(&root);
        assert_eq!(cat.sessions.len(), 1);
        let mut read = Vec::new();
        for entry in &cat.sessions[0].segments {
            let file = std::fs::File::open(&entry.path).expect("open");
            let mut reader = RawReader::open(std::io::BufReader::new(file)).expect("a capture");
            let (frames, truncation) = reader.read_all().expect("readable");
            assert_eq!(truncation, None);
            assert!(reader.is_finalized(), "sealed, as a clean stop seals");
            read.extend(frames);
        }
        let payloads: Vec<String> = read
            .iter()
            .filter(|f| f.kind == FrameKind::VenuePayload)
            .map(|f| String::from_utf8(f.payload.clone()).expect("text"))
            .collect();
        assert_eq!(payloads, frames, "the venue's bytes, verbatim and in order");
        assert!(
            read.iter().all(|f| f.local_recv_ts == at),
            "every record stamped by the clock handed in: {:?}",
            read.iter().map(|f| f.local_recv_ts).collect::<Vec<_>>()
        );
        assert_eq!(
            market.targets(),
            ["/stream?streams=btcusdt@trade/btcusdt@depth@100ms"],
            "subscribed at the base it was given, to the streams M1 records"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_account_capture_opens_with_its_gap_and_marks_what_its_consumer_missed() {
        // The two things a live run's account capture must wire that a
        // listener alone does not: the session's opening gap, and the tee
        // marking its drops so the feed tells a hole while the account is
        // quiet (docs/live-run.md section 5, section 6 duty 3). A consumer
        // with room for one record and never drained takes the opening gap
        // and misses the subscription's answer; the account then says
        // nothing, and the feed must still say it missed something.
        let root = scratch("account");
        let venue = FakeVenue::start(fake_venue::Script::default()).await;
        let (consumer, records) = std::sync::mpsc::sync_channel(1);
        let mark = Arc::new(LastDropped::default());
        let mut feed = AccountFeed::new(records, Arc::clone(&mark));
        let config = AccountConfig {
            root: root.clone(),
            url: venue.url().to_owned(),
            policy: fake_venue::quick_policy(),
            offset_ms: 0,
            clock: Arc::new(quant_core::time::SystemClock),
        };
        let credentials = venue.credentials();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let capture = record_account(config, &credentials, consumer, Some(mark), async {
            let _ = stopped.await;
        });
        let drive = async {
            venue
                .until("the subscription", Duration::from_secs(20), |log| {
                    log.asked("userDataStream.subscribe") >= 1
                })
                .await;
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            let mut read = Vec::new();
            while !read.iter().any(|r| {
                matches!(
                    r,
                    AccountRecord::Blind {
                        cause: GapCause::LocalOverflow,
                        ..
                    }
                )
            }) {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the feed never told the hole: {read:?}"
                );
                match feed.poll() {
                    Polled::Record(record) => read.push(record),
                    Polled::Empty | Polled::Closed => {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                }
            }
            let _ = stop.send(());
            read
        };
        let (captured, read) = tokio::time::timeout(
            Duration::from_secs(30),
            Box::pin(async { tokio::join!(capture, drive) }),
        )
        .await
        .expect("the capture stops when asked");
        let captured = captured.expect("recorded and sealed");
        assert_eq!(captured.stopped, None, "stopped, not refused");
        assert!(
            matches!(
                read.first(),
                Some(AccountRecord::Blind {
                    cause: GapCause::RecorderRestart,
                    ..
                })
            ),
            "the session opens with its gap: {read:?}"
        );
        let cat = account_catalog(&root);
        assert_eq!(cat.sessions.len(), 1, "under the account's own root");
        assert!(!captured.segments.is_empty(), "sealed");
        let _ = std::fs::remove_dir_all(&root);
    }
}
