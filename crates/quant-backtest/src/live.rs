//! A live run: paper's session with a live venue, behind gates that decide
//! before anything is captured whether it may trade at all.
//!
//! `docs/live-run.md` §4 item 8 is the brief and §5, *`LiveVenue`, decided
//! before the code*, is the design: §1's shutdown order, §6's duties, and §7's
//! owner decisions. This module is that binary's body, so that a rehearsal
//! can run it against local stand-ins and the binary is only its command line.
//!
//! # Gates, then the capture
//!
//! Every gate is asked before the capture starts (§6 duty 1), so that the
//! engine's market channel never fills unread behind a slow check: an engine
//! started late begins on a book that waits up to an hour for a snapshot. In
//! order, and the first that fails decides:
//!
//! 1. **The venue's checks**, `venue-check`'s three, giving a `Checked` with
//!    the clock offset they passed with.
//! 2. **The instrument from `exchangeInfo`**, every rule the model takes from
//!    the venue and none from a literal.
//! 3. **The run tag from the journal**: minted when the journal is empty, the
//!    first `started` line's otherwise, and a non-empty journal without one
//!    refused (§5, *The run tag lives on `Started`*).
//! 4. **No latch on the journal**: `journal::diverged` (§6 duty 8). A latch
//!    that forgot across a restart would trade again against a position nobody
//!    knows; it holds until a person starts a new journal under a new tag.
//! 5. **Nothing left working by the last session** (§6 duty 6): until slice
//!    (d) can look an order up, a session that did not stop cleanly, or that
//!    stopped with orders working, may have orders on the venue nobody is
//!    watching.
//! 6. **A size whose exit the venue will take**: a whole number of lots, and
//!    the exit after a buy's fee in the base, rounded down to the lot, still at
//!    or above the minimum notional at the best bid (§2).
//! 7. **The baseline**: the account's two balances, which the run journals
//!    before it may trade (§7, decision 1, *no baseline, no trading*), and
//!    the credentials the closing balances and the account listener will
//!    sign with, read now so that none is found missing after the journal
//!    says a session started.
//!
//! A gate that fails means the run records the market and does not trade
//! (§7, decision 2), saying which gate and why. No strategy runs, no account
//! stream is opened and nothing is written to the journal: a run that trades
//! nothing has nothing to journal, and a `started` line from it would leave a
//! session without a `stopped` line for gate 5 to refuse the next run over.
//!
//! # A run that trades
//!
//! The venue is wired and every credential it needs read before the first
//! line, so that a run which cannot be wired records only and leaves the
//! journal as it found it. Then it writes `started`, with the run tag, and
//! the start `balances`, through the strict writer's latch; a journal that
//! cannot hold them records only.
//! Then the account stream is captured under its own root and the market
//! under `raw/`, both stamped by one clock, the account's offset the
//! `Checked` one (§6 duty 2), the account's tee marking its drops (duty 3).
//! The engine is paper's ([`crate::session`]) with `LiveVenue` behind it, the
//! strict writer journalling (duty 4), and the risk tally recounted from the
//! journal.
//!
//! # Shutdown, in §1's order
//!
//! 1. The market ends, so `run` returns and the strategy has stopped.
//! 2. `Engine::cancel_working`: each resting order's cancel through the
//!    record (decision 3).
//! 3. and 4. `quant_live::drain`: settle until nothing is working, then stop
//!    sending and settle until every request is answered, each wait with a
//!    deadline of its own.
//! 5. The closing checkpoint, `stopped`, and the end `balances`, only while
//!    the strict writer has not failed: a line written after a torn one would
//!    sit in the middle of the file, where `read` refuses the whole journal.
//! 6. The engine dropped, the venue with it, so the request worker returns.
//! 7. The account listener stopped, which seals its capture.
//! 8. The runtime shut down last: it drove the worker through 3 and 4 and the
//!    listener until 7.

use std::path::PathBuf;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quant_binance::account::{AccountFeed, AccountPolicy};
use quant_binance::capture::{self, AccountCaptured, AccountConfig, CaptureConfig};
use quant_binance::credentials::{CredentialError, Credentials};
use quant_binance::exchange_info::{self, VenueInstrument};
use quant_binance::trade::{self, Balances, Checked, ClockSample, TradeClient};
use quant_binance::user_data::RunTag;
use quant_binance::{parse_snapshot, SnapshotClient};
use quant_core::execution::ClientOrderId;
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::instrument::{InstrumentDef, InstrumentId};
use quant_core::time::{Clock, Ts};
use quant_engine::journal::{self, Recovered};
use quant_engine::{EngineStats, JournalEntry, Limits};
use quant_live::{Deadlines, Drained, LiveStats, LiveVenue};
use quant_recorder::{LastDropped, DEFAULT_CHANNEL_CAPACITY};
use tracing::{error, info, warn};

use crate::record::{JournalWriter, Latch, Shared};
use crate::session::{self, MarketRun, Settings};

/// How long any one REST request before the run may take: the venue's own
/// ten seconds, as `venue-check` allows.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the runtime is given to finish what it drives once everything has
/// been told to stop.
const RUNTIME_GRACE: Duration = Duration::from_secs(5);

/// Which venue a run trades at. Every endpoint follows from it unless
/// overridden, so the three cannot point at two venues by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    /// Binance, with real money.
    Live,
    /// Spot Testnet, with a key pair of its own and no money at risk: where
    /// the owner decided every protocol path is rehearsed first (§5).
    Testnet,
}

impl Environment {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Testnet => "testnet",
        }
    }

    /// Its REST base, WebSocket API and market stream.
    #[must_use]
    pub fn endpoints(self) -> Endpoints {
        match self {
            Self::Live => Endpoints {
                rest: quant_binance::SPOT_REST.to_owned(),
                ws_api: quant_binance::account::SPOT_WS_API.to_owned(),
                market_ws: quant_binance::SPOT_WS.to_owned(),
            },
            Self::Testnet => Endpoints {
                rest: quant_binance::SPOT_TESTNET_REST.to_owned(),
                ws_api: quant_binance::account::SPOT_TESTNET_WS_API.to_owned(),
                market_ws: quant_binance::SPOT_TESTNET_WS.to_owned(),
            },
        }
    }
}

/// Where a run sends and listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    /// Signed requests, the clock, `exchangeInfo` and depth snapshots.
    pub rest: String,
    /// The account stream's WebSocket API.
    pub ws_api: String,
    /// The market stream.
    pub market_ws: String,
}

/// Everything a live run is told.
#[derive(Debug, Clone)]
pub struct LiveConfig {
    pub environment: Environment,
    /// The environment's endpoints, with any override applied.
    pub endpoints: Endpoints,
    /// Data root: the market under `raw/`, the account under `account/`.
    pub root: PathBuf,
    pub symbol: String,
    /// The strategy's starting capital, for a fresh journal.
    pub cash: Notional,
    pub settings: Settings,
    pub limits: Limits,
    /// How long to capture before stopping by itself; `None` until stopped.
    pub limit: Option<Duration>,
    pub account_policy: AccountPolicy,
    /// The shutdown's two waits.
    pub deadlines: Deadlines,
    /// The run's one clock: the market's, the account's and the worker's.
    pub clock: Arc<dyn Clock>,
}

/// The journal as a run finds it, and where it writes.
pub struct Journalled {
    pub recovered: Recovered,
    pub sink: Shared,
}

impl std::fmt::Debug for Journalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journalled")
            .field("entries", &self.recovered.entries.len())
            .finish_non_exhaustive()
    }
}

/// Where a run's credentials come from: the environment, in the binary. A
/// source rather than a value, because `Credentials` is not `Clone` and three
/// holders need one each (the order path, the closing baseline, the account
/// listener), and because key material is read where it is used and nowhere
/// else.
pub type Keys = dyn Fn() -> Result<Credentials, CredentialError> + Sync;

/// A gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    VenueChecks,
    ExchangeInfo,
    RunTag,
    Diverged,
    LastSessionWorking,
    Size,
    Baseline,
}

impl Gate {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::VenueChecks => "venue checks",
            Self::ExchangeInfo => "exchangeInfo",
            Self::RunTag => "run tag",
            Self::Diverged => "diverged",
            Self::LastSessionWorking => "last session",
            Self::Size => "size",
            Self::Baseline => "baseline",
        }
    }
}

/// A gate that failed, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub gate: Gate,
    pub why: String,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the {} gate refused: {}", self.gate.name(), self.why)
    }
}

/// What a run that traded ended with.
#[derive(Debug)]
pub struct Traded {
    pub run: RunTag,
    /// The orders `cancel_working` asked about at the stop.
    pub cancelled: Vec<ClientOrderId>,
    pub drained: Drained,
    pub stats: EngineStats,
    pub venue: LiveStats,
    /// Whether the strict writer had failed, so that no closing line was
    /// written.
    pub latched: bool,
    /// How the account capture ended.
    pub account: Result<AccountCaptured, String>,
}

/// Whether a run traded, or recorded only and why.
#[derive(Debug)]
pub enum Outcome {
    RecordOnly(Refusal),
    Traded(Box<Traded>),
}

/// How a run ended.
#[derive(Debug)]
pub struct LiveRun {
    pub outcome: Outcome,
    /// How the market capture ended.
    pub capture: Result<(), String>,
    /// Records the market tee could not hand the engine.
    pub tee_dropped: u64,
}

/// What the gates hand a run that may trade.
struct Passed {
    checked: Checked,
    client: TradeClient,
    closing: TradeClient,
    /// The account listener's, read with the others so that a source that
    /// stops answering is found before the journal is written.
    listener: Credentials,
    offset_ms: i64,
    instrument: VenueInstrument,
    run: RunTag,
    balances: Balances,
}

/// Run until `stop` completes or the configured limit passes: the gates, then
/// a run that trades or one that records only, then the shutdown in §1's
/// order. The run owns its runtime, so that the runtime is shut down last,
/// and polls `stop` once as soon as the runtime exists (`Primed`), so that a
/// stop which listens for a signal listens before anything is written.
///
/// # Errors
///
/// For what the run could not get past at all: a runtime it could not build,
/// a thread it could not spawn, an engine thread that panicked, or a source
/// that failed mid-run (after the shutdown has run). A gate that fails is not
/// an error; it is [`Outcome::RecordOnly`].
#[allow(
    clippy::too_many_lines,
    reason = "the run's steps in the design's order, read as one sequence"
)]
pub fn run<Stop>(
    config: &LiveConfig,
    keys: &Keys,
    journalled: Journalled,
    stop: Stop,
) -> Result<LiveRun, String>
where
    Stop: core::future::Future<Output = std::io::Result<()>>,
{
    let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    let stop = Primed::new(&runtime, stop);
    let market = market_config(config);
    let Journalled { recovered, sink } = journalled;
    let gated = runtime
        .block_on(gates(config, keys, &recovered.entries))
        .and_then(|passed| ready(config, passed, recovered));
    let (passed, prepared, strategy, feed_parts) = match gated {
        Ok(parts) => parts,
        Err(refusal) => return Ok(record_only(runtime, market, stop, refusal)),
    };

    let instrument = prepared.instrument;
    let Some(registered) = prepared.registry.get(instrument).cloned() else {
        return Err("the instrument resume registered is not in its registry".to_owned());
    };
    // The venue, wired before anything is written: one that cannot be is
    // the instrument's refusal, and records only with the journal as it was.
    // Nothing runs yet; a venue dropped unrun sends nothing.
    let FeedParts {
        consumer,
        mark,
        feed,
    } = feed_parts;
    let wired = quant_live::wire(
        instrument,
        &passed.instrument,
        passed.run.clone(),
        passed.checked,
        passed.client,
        feed,
        Arc::clone(&config.clock),
    );
    let (venue, worker) = match wired {
        Ok(wired) => wired,
        Err(e) => {
            return Ok(record_only(
                runtime,
                market,
                stop,
                Refusal {
                    gate: Gate::ExchangeInfo,
                    why: format!("the venue could not be wired: {e}"),
                },
            ))
        }
    };

    // The journal's first lines for this session, through the latch the
    // writer will share. A journal that cannot hold them is no baseline.
    let latch = Latch::default();
    let wrote = latch.write(
        &sink,
        &JournalEntry::Started {
            at: config.clock.now(),
            cash: prepared.resumed.starting_cash,
            schema: journal::SCHEMA,
            run: Some(passed.run.as_str().to_owned()),
        },
    );
    let balances = balances_line(config.clock.now(), &registered, passed.balances);
    let baseline = match (wrote, balances) {
        (Ok(true), Ok(line)) => latch.write(&sink, &line).map_err(|e| e.to_string()),
        // Not written: the latch was set, which a fresh one is not.
        (Ok(false), Ok(_)) => Ok(false),
        (Ok(_), Err(e)) => Err(e),
        (Err(e), _) => Err(e.to_string()),
    };
    if !matches!(baseline, Ok(true)) {
        let why = match baseline {
            Err(e) => format!("the journal could not hold the started and balances lines: {e}"),
            Ok(_) => "the journal stopped before the balances line".to_owned(),
        };
        return Ok(record_only(
            runtime,
            market,
            stop,
            Refusal {
                gate: Gate::Baseline,
                why,
            },
        ));
    }

    // The account stream, under its own root, stamped by the run's clock and
    // logged on with the measured offset, its tee marking what it drops.
    let credentials = passed.listener;
    let (account_stop, account_stopped) = tokio::sync::oneshot::channel::<()>();
    let account_config = AccountConfig {
        root: config.root.clone(),
        url: config.endpoints.ws_api.clone(),
        policy: config.account_policy,
        offset_ms: passed.offset_ms,
        clock: Arc::clone(&config.clock),
    };
    let account = runtime.spawn(async move {
        capture::record_account(account_config, &credentials, consumer, Some(mark), async {
            let _ = account_stopped.await;
        })
        .await
        .map_err(|e| e.to_string())
    });

    runtime.spawn(worker.run());

    let writer = JournalWriter::strict_to(
        Arc::clone(&sink),
        config.symbol.clone(),
        session::recovered_fills(&prepared.recovered.entries),
    );
    let latch = writer.latch();
    let (engine_tx, engine_rx) = session::market_channel();
    let engine = session::engine(engine_rx, venue, prepared, strategy, Box::new(writer));
    info!(run = passed.run.as_str(), "trading");
    println!("trading   under run tag {}", passed.run.as_str());

    let deadlines = config.deadlines;
    let ran = session::run_market(
        engine,
        engine_tx,
        market,
        &runtime,
        stop,
        "live",
        move |engine| {
            let cancelled = engine.cancel_working();
            let drained = quant_live::drain(engine, &deadlines);
            (cancelled, drained)
        },
    )
    .map_err(|e| e.to_string())?;
    let MarketRun {
        engine,
        outcome,
        finished: (cancelled, drained),
        capture,
        tee_dropped,
    } = ran;

    // 5. The closing lines, only while the strict writer has not failed.
    let latched = latch.is_set();
    if latched {
        error!(
            "the journal stopped at its first failure: no closing checkpoint, stopped or \
             balances line is written, so the next run's last-session gate refuses until a \
             person has looked"
        );
    } else {
        let (checkpoint, stopped) = session::closing(&engine);
        let closed = latch
            .write(&sink, &checkpoint)
            .and_then(|_| latch.write(&sink, &stopped));
        if let Err(e) = closed {
            error!(error = %e, "the closing lines could not be written");
        }
        let end = runtime
            .block_on(passed.closing.balances(
                &registered.base,
                &registered.quote,
                passed.offset_ms,
            ))
            .map_err(|e| e.to_string())
            .and_then(|end| balances_line(config.clock.now(), &registered, end));
        match end {
            Ok(line) => {
                if let Err(e) = latch.write(&sink, &line) {
                    error!(error = %e, "the end balances line could not be written");
                }
            }
            Err(e) => error!(error = %e, "the venue's end balances could not be journalled"),
        }
    }
    report(&engine, &drained, &cancelled, latched, tee_dropped);
    let stats = engine.stats();
    let venue_stats = engine.venue().stats();
    // 6. The engine, and the venue with it: its queue closes and the worker
    //    returns.
    drop(engine);
    // 7. The account listener, which seals its capture.
    let _ = account_stop.send(());
    let account = runtime
        .block_on(account)
        .map_err(|e| format!("the account capture task failed: {e}"))
        .and_then(|captured| captured);
    // 8. The runtime, last.
    runtime.shutdown_timeout(RUNTIME_GRACE);

    let run = LiveRun {
        outcome: Outcome::Traded(Box::new(Traded {
            run: passed.run,
            cancelled,
            drained,
            stats,
            venue: venue_stats,
            latched,
            account,
        })),
        capture: capture.map_err(|e| e.to_string()),
        tee_dropped,
    };
    outcome.map_err(|e| format!("the market source failed: {e}"))?;
    Ok(run)
}

/// The stop, polled once as the run begins, so that whatever it listens for
/// is listened for from the run's first instant rather than from the
/// capture's.
///
/// The binary's stop is `tokio::signal::ctrl_c()`, which installs its handler
/// on its first poll. Polled first by the capture, a Ctrl-C during the gates
/// or between the journal's first lines and the capture took the system's
/// default and killed the process: harmless during the gates, but after
/// `started` it left a session with no `stopped` line, and the next run
/// refused at the last-session gate. Primed, a Ctrl-C at any point after the
/// runtime exists is a stop: the gates finish, each bounded by its request
/// timeout, and the run stops at once, cleanly, with its closing lines.
enum Primed<F> {
    /// Not yet complete: polled on, wherever the run polls its stop.
    Waiting(std::pin::Pin<Box<F>>),
    /// Complete on its first poll; what it completed with, until taken.
    Done(Option<std::io::Result<()>>),
}

impl<F> Primed<F>
where
    F: core::future::Future<Output = std::io::Result<()>>,
{
    fn new(runtime: &tokio::runtime::Runtime, stop: F) -> Self {
        let mut stop = Box::pin(stop);
        let first = runtime.block_on(std::future::poll_fn(|cx| {
            std::task::Poll::Ready(stop.as_mut().poll(cx))
        }));
        match first {
            std::task::Poll::Ready(result) => Self::Done(Some(result)),
            std::task::Poll::Pending => Self::Waiting(stop),
        }
    }
}

impl<F> core::future::Future for Primed<F>
where
    F: core::future::Future<Output = std::io::Result<()>>,
{
    type Output = std::io::Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Waiting(stop) => stop.as_mut().poll(cx),
            Self::Done(result) => std::task::Poll::Ready(result.take().unwrap_or(Ok(()))),
        }
    }
}

/// The market capture a run makes, whether or not it trades.
fn market_config(config: &LiveConfig) -> CaptureConfig {
    let market = CaptureConfig::new(config.symbol.clone(), config.root.clone())
        .at(
            config.endpoints.market_ws.clone(),
            config.endpoints.rest.clone(),
        )
        .stamped_by(Arc::clone(&config.clock));
    match config.limit {
        Some(limit) => market.for_at_most(limit),
        None => market,
    }
}

/// Record the market, trade nothing, and say why.
fn record_only<Stop>(
    runtime: tokio::runtime::Runtime,
    market: CaptureConfig,
    stop: Stop,
    refusal: Refusal,
) -> LiveRun
where
    Stop: core::future::Future<Output = std::io::Result<()>>,
{
    error!(gate = refusal.gate.name(), why = %refusal.why, "recording only: a gate refused");
    println!("RECORD ONLY  {refusal}");
    println!(
        "             the market is recorded and nothing is traded: no strategy runs, no \
         account stream is opened, and the journal is not written"
    );
    let capture = runtime
        .block_on(capture::run_until(market, |tx| tx, stop))
        .map_err(|e| e.to_string());
    runtime.shutdown_timeout(RUNTIME_GRACE);
    LiveRun {
        outcome: Outcome::RecordOnly(refusal),
        capture,
        tee_dropped: 0,
    }
}

/// What the account feed is built from, made with the session's other pieces
/// once the gates pass. `run` wires the venue from it before anything is
/// written, so that a venue that cannot be wired is found before the journal
/// says a session started.
struct FeedParts {
    consumer: std::sync::mpsc::SyncSender<quant_recorder::CaptureRecord>,
    mark: Arc<LastDropped>,
    feed: AccountFeed,
}

/// The session's pieces, once the gates have passed: its journal recovered
/// onto the venue's definition and its strategy built. A strategy that
/// cannot be built is the size gate's refusal.
#[allow(
    clippy::type_complexity,
    reason = "one step's four products, named where they are taken apart"
)]
fn ready(
    config: &LiveConfig,
    passed: Passed,
    recovered: Recovered,
) -> Result<
    (
        Passed,
        session::Prepared,
        crate::Recorded<crate::MaCrossover>,
        FeedParts,
    ),
    Refusal,
> {
    let prepared = session::prepare(
        recovered,
        passed.instrument.definition.clone(),
        config.cash,
        config.limits,
    )
    .map_err(|e| Refusal {
        gate: Gate::ExchangeInfo,
        why: e.to_string(),
    })?;
    let strategy =
        session::strategy(config.settings, prepared.instrument, prepared.lot).map_err(|e| {
            Refusal {
                gate: Gate::Size,
                why: e.to_string(),
            }
        })?;
    let (consumer, records) = sync_channel(DEFAULT_CHANNEL_CAPACITY);
    let mark = Arc::new(LastDropped::default());
    let feed = AccountFeed::new(records, Arc::clone(&mark));
    Ok((
        passed,
        prepared,
        strategy,
        FeedParts {
            consumer,
            mark,
            feed,
        },
    ))
}

/// The gates, in order; the first that fails decides.
async fn gates(
    config: &LiveConfig,
    keys: &Keys,
    entries: &[JournalEntry],
) -> Result<Passed, Refusal> {
    let refuse = |gate: Gate| move |why: String| Refusal { gate, why };
    let symbol = config.symbol.as_str();
    let qty = config.settings.qty;

    // 1. The venue's checks, venue-check's three, on the clock the run reads.
    let public = SnapshotClient::new(config.endpoints.rest.clone(), REQUEST_TIMEOUT)
        .map_err(|e| e.to_string())
        .map_err(refuse(Gate::VenueChecks))?;
    let client = trade_client(config, keys).map_err(refuse(Gate::VenueChecks))?;
    let clock = Arc::clone(&config.clock);
    let (readings, failures) = trade::read_clock(&public, || clock.now().as_millis()).await;
    let best = ClockSample::best_of(&readings, trade::CLOCK_READINGS).map_err(|e| {
        let failed: Vec<String> = failures
            .iter()
            .map(|(n, e)| format!("reading {n} failed: {e}"))
            .collect();
        Refusal {
            gate: Gate::VenueChecks,
            why: format!("{e} {}", failed.join("; ")),
        }
    })?;
    let offset_ms = best.offset_ms();
    let probe = trade::probe(symbol, qty)
        .map_err(|e| e.to_string())
        .map_err(refuse(Gate::VenueChecks))?;
    let checked = client
        .checked(&probe, symbol, offset_ms)
        .await
        .map_err(|e| e.to_string())
        .map_err(refuse(Gate::VenueChecks))?;
    info!(offset_ms, "the venue's checks passed");

    // 2. The instrument, every rule the model takes, from the venue.
    let instrument = exchange_info::fetch(&public, symbol)
        .await
        .map_err(|e| e.to_string())
        .map_err(refuse(Gate::ExchangeInfo))?;
    for rule in &instrument.unmodelled {
        info!(%rule, "a venue rule the model does not have");
    }

    // 3 to 5, from the journal alone.
    let run = run_tag_gate(entries, || mint_run_tag(config.clock.now()))?;
    diverged_gate(entries)?;
    last_session_gate(entries)?;

    // 6. A size whose exit the venue will take, at the best bid now.
    let mark = best_bid(&public, symbol)
        .await
        .map_err(refuse(Gate::Size))?;
    size_gate(qty, &instrument.definition, mark)?;

    // 7. The baseline.
    let definition = &instrument.definition;
    let balances = client
        .balances(&definition.base, &definition.quote, offset_ms)
        .await
        .map_err(|e| e.to_string())
        .map_err(refuse(Gate::Baseline))?;
    let closing = trade_client(config, keys).map_err(refuse(Gate::Baseline))?;
    let listener = keys()
        .map_err(|e| e.to_string())
        .map_err(refuse(Gate::Baseline))?;
    Ok(Passed {
        checked,
        client,
        closing,
        listener,
        offset_ms,
        instrument,
        run,
        balances,
    })
}

fn trade_client(config: &LiveConfig, keys: &Keys) -> Result<TradeClient, String> {
    let credentials = keys().map_err(|e| e.to_string())?;
    TradeClient::new(config.endpoints.rest.clone(), REQUEST_TIMEOUT, credentials)
        .map_err(|e| e.to_string())
}

/// The run tag: the first `started` line's, a fresh one for an empty journal,
/// and a refusal for a journal with lines and no tag.
///
/// # Errors
///
/// The run-tag gate's [`Refusal`].
pub fn run_tag_gate(
    entries: &[JournalEntry],
    mint: impl FnOnce() -> RunTag,
) -> Result<RunTag, Refusal> {
    let refuse = |why: String| Refusal {
        gate: Gate::RunTag,
        why,
    };
    if entries.is_empty() {
        return Ok(mint());
    }
    match journal::run_tag(entries) {
        Some(tag) => RunTag::new(tag).map_err(|e| {
            refuse(format!(
                "the journal's run tag {tag:?} is not one this build can send: {e}"
            ))
        }),
        None => Err(refuse(
            "the journal has lines and no run tag: it is not a live run's, and orders sent \
             under a tag minted now could not be told from whatever it records. Start a new \
             journal"
                .to_owned(),
        )),
    }
}

/// A run tag minted from `now`: its milliseconds in base 36, eight or nine
/// letters and digits, different for every journal begun a millisecond apart
/// (§5: the tag must change whenever a journal is lost).
#[must_use]
pub fn mint_run_tag(now: Ts) -> RunTag {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut n = u64::try_from(now.as_millis()).unwrap_or(0);
    let mut text = Vec::new();
    loop {
        let digit = usize::try_from(n % 36).expect("below 36");
        text.push(DIGITS[digit]);
        n /= 36;
        if n == 0 {
            break;
        }
    }
    text.reverse();
    let text = String::from_utf8(text).expect("ascii");
    RunTag::new(&text).expect("a base-36 number of a millisecond count is a valid tag")
}

/// No latch on the journal.
///
/// # Errors
///
/// The diverged gate's [`Refusal`].
pub fn diverged_gate(entries: &[JournalEntry]) -> Result<(), Refusal> {
    if journal::diverged(entries) {
        return Err(Refusal {
            gate: Gate::Diverged,
            why: "the journal holds a line that latches Diverged (a contradiction, an orphaned \
                  fill, a stranger's trade on this symbol, or an id in use): the account may \
                  hold what this record does not know. A person clears it by starting a new \
                  journal under a new run tag"
                .to_owned(),
        });
    }
    Ok(())
}

/// Nothing left working by the last session: it wrote `stopped`, and its
/// `stopped` names no order.
///
/// A journal with lines and no `started` line is refused too, though the
/// run never asks this about one: the run-tag gate, asked first, has already
/// refused it. The branch keeps this gate right on its own.
///
/// # Errors
///
/// The last-session gate's [`Refusal`].
pub fn last_session_gate(entries: &[JournalEntry]) -> Result<(), Refusal> {
    let refuse = |why: String| Refusal {
        gate: Gate::LastSessionWorking,
        why,
    };
    let Some(start) = entries
        .iter()
        .rposition(|e| matches!(e, JournalEntry::Started { .. }))
    else {
        return if entries.is_empty() {
            Ok(())
        } else {
            Err(refuse(
                "the journal has lines and no started line, so how its last session ended \
                 cannot be told"
                    .to_owned(),
            ))
        };
    };
    let stopped = entries[start..].iter().rev().find_map(|e| match e {
        JournalEntry::Stopped { working, .. } => Some(working),
        _ => None,
    });
    match stopped {
        None => Err(refuse(
            "the last session wrote no stopped line: it did not stop cleanly, and an order it \
             sent may still be working on the venue with nobody watching. Until slice (d) can \
             look orders up, a person settles them"
                .to_owned(),
        )),
        Some(working) if !working.is_empty() => {
            let ids: Vec<String> = working.iter().map(|id| id.0.to_string()).collect();
            Err(refuse(format!(
                "the last session stopped with orders still working ({}), which may be on the \
                 venue with nobody watching. Until slice (d) can look orders up, a person \
                 settles them",
                ids.join(", ")
            )))
        }
        Some(_) => Ok(()),
    }
}

/// The best bid now, from a depth snapshot: the price a market exit sells at.
async fn best_bid(public: &SnapshotClient, symbol: &str) -> Result<Px, String> {
    let body = public.depth(symbol).await.map_err(|e| e.to_string())?;
    let snapshot = parse_snapshot(&body, InstrumentId::from_index(0), Ts::from_nanos(0), 0)
        .map_err(|e| e.to_string())?;
    snapshot
        .bids
        .iter()
        .map(|level| level.px)
        .max()
        .ok_or_else(|| "the depth snapshot has no bids to price an exit at".to_owned())
}

/// What the exit after a buy of `qty` could sell: `qty` less the venue's fee
/// on it in the base, at the standard taker rate, rounded down to the lot.
///
/// The rate is the standard tier's ten basis points, which the run's account
/// pays: `venue-check`'s third check refuses READY while the BNB discount is
/// on. The fee is truncated, as the simulator's is; a venue that rounds its
/// fee up takes at most one more raw unit, which a rounding down to the lot
/// has already taken whenever the fee is not a whole number of lots.
#[must_use]
pub fn floored_exit(qty: Qty, definition: &InstrumentDef) -> Qty {
    let fee = quant_sim::FeeSchedule::binance_spot().fee_on_quantity(qty, false);
    qty.checked_sub(fee)
        .unwrap_or(Qty::ZERO)
        .round_to_lot(definition.lot_size)
}

/// A size whose exit the venue will take: a positive whole number of lots,
/// whose [`floored_exit`] at `mark` is not under the minimum notional (§2).
///
/// # Errors
///
/// The size gate's [`Refusal`], with the numbers.
pub fn size_gate(qty: Qty, definition: &InstrumentDef, mark: Px) -> Result<(), Refusal> {
    let refuse = |why: String| Refusal {
        gate: Gate::Size,
        why,
    };
    if !qty.is_positive() || qty.round_to_lot(definition.lot_size) != qty {
        return Err(refuse(format!(
            "{qty} is not a positive whole number of the venue's {} lot",
            definition.lot_size
        )));
    }
    let exit = floored_exit(qty, definition);
    let value = mark
        .notional(exit)
        .ok_or_else(|| refuse(format!("{exit} at {mark} is beyond the fixed-point range")))?;
    if value < definition.min_notional {
        return Err(refuse(format!(
            "an entry of {qty} leaves an exit of {exit} after the fee in the base, rounded down \
             to the lot, worth {value} at the best bid {mark}: under the venue's minimum \
             notional of {}, so every exit would be refused and the strategy would stop \
             trading holding a position. The size must clear the minimum by more than a lot \
             and the move expected between an entry and its exit",
            definition.min_notional
        )));
    }
    Ok(())
}

/// A `balances` line for the registered instrument, through the constructor
/// the design names for it, which takes the asset names from the definition.
fn balances_line(
    at: Ts,
    instrument: &quant_core::instrument::Instrument,
    balances: Balances,
) -> Result<JournalEntry, String> {
    JournalEntry::balances(at, instrument, balances.base, balances.quote).map_err(|e| e.to_string())
}

/// What the run did, said once at the end.
fn report(
    engine: &session::Session<LiveVenue>,
    drained: &Drained,
    cancelled: &[ClientOrderId],
    latched: bool,
    tee_dropped: u64,
) {
    let stats = engine.stats();
    let portfolio = engine.portfolio();
    println!();
    println!(
        "events    {} reached the engine ({} gaps), {} execution events, {} notices",
        stats.events, stats.gaps, stats.execution_events, stats.notices
    );
    println!("tee       {tee_dropped} market records dropped on the way to the engine");
    println!(
        "orders    {} sent, {} refused, {} fills, {} cancelled at the stop",
        stats.submitted,
        stats.refused,
        stats.fills,
        cancelled.len()
    );
    println!(
        "cash      {} from {}",
        portfolio.cash(),
        portfolio.starting_cash()
    );
    println!("fees      {}", portfolio.fees());
    if drained.working.is_empty() {
        println!("stopped   nothing working");
    } else {
        let ids: Vec<String> = drained.working.iter().map(|id| id.0.to_string()).collect();
        warn!(working = ?ids, "orders still working at the stop");
        println!(
            "stopped   WITH ORDERS WORKING: {} -- the next run refuses to trade until they \
             are settled",
            ids.join(", ")
        );
    }
    if latched {
        println!("journal   STOPPED at its first failure: no closing lines were written");
    }
}

/// A clean journal's sink, for the binary: the file at `path`, and the
/// directory it goes in, made when there is none. The binary's default
/// journal is under `data/<env>`, which nothing has made before the first
/// run on a fresh host; without this, the documented `live --symbol BTCUSDT`
/// could not start there.
///
/// # Errors
///
/// The directory could not be made, or the file read or opened.
pub fn journal_at(path: &std::path::Path) -> std::io::Result<Journalled> {
    if let Some(dir) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    let recovered = journal::read(path)?;
    let file = quant_engine::Journal::open(path)?;
    Ok(Journalled {
        recovered,
        sink: Arc::new(Mutex::new(file)),
    })
}

#[cfg(test)]
mod tests;
