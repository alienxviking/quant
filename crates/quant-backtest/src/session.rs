//! A session that captures the market and trades on it from one ingress,
//! whatever the venue: paper's, extracted so that the live binary runs the
//! proven path rather than a copy of it.
//!
//! `paper` was the only binary that captured and traded at once, and M5's
//! fortnight is what proved that shape: one ingress, a `TeeSink` with the
//! capture primary, the engine on a thread of its own reading the tee's
//! secondary through a `LiveSource`, a journal written ahead of every fill,
//! and a restart that comes back holding what it held. The live binary needs
//! all of it with one thing changed, the venue (`docs/live-run.md` §4,
//! item 8). Copied, the two would be two sessions that must agree for ever,
//! the shape this project has refused since `PaperVenue`; so the session is
//! here, generic over the venue, and each binary is the wiring around it.
//!
//! What stays in each binary is what differs: the instrument's definition
//! (paper's literal, the venue's `exchangeInfo` for live), the run tag on
//! `started`, the journal writer's policy (lenient for paper, strict for
//! live), and what happens after the market ends. That last is
//! [`run_market`]'s `finish`, which runs on the engine's thread once `run`
//! returns: nothing for paper, the cancels and the drain for live.
//!
//! Everything here is paper's code moved, in paper's order. Paper's behaviour
//! does not change.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;

use quant_binance::capture::{self, CaptureConfig};
use quant_binance::LiveSource;
use quant_core::fixed::{Notional, Qty};
use quant_core::instrument::{InstrumentDef, InstrumentId, InstrumentRegistry};
use quant_core::source::SourceError;
use quant_engine::journal::{self, Agreement, DefinitionConflict, Recovered, Resumption};
use quant_engine::{
    Engine, EngineStats, ExecutionVenue, JournalEntry, Limits, RiskEngine, RunObserver,
};
use quant_recorder::CaptureRecord;

use crate::{MaConfig, MaConfigError, MaCrossover, Recorded};

/// How deep the engine's channel is. Generous on purpose: holding a few
/// thousand records is cheaper than blinding the strategy, and the capture is
/// never at risk either way -- the tee drops the engine's copy, never the
/// writer's.
pub const ENGINE_CHANNEL: usize = 8192;

/// The engine a session runs: the market from the tee, `V` to trade on, the
/// risk layer, and the strategy with its equity sampler.
pub type Session<V> = Engine<LiveSource, V, RiskEngine, Recorded<MaCrossover>>;

/// The strategy's settings, as each binary's command line gives them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    pub fast: usize,
    pub slow: usize,
    /// Event time between samples, in nanoseconds.
    pub interval: i64,
    /// Size of each entry, in base units.
    pub qty: Qty,
}

/// What a session takes from its journal before it writes anything.
#[derive(Debug)]
pub struct Prepared {
    pub recovered: Recovered,
    pub registry: InstrumentRegistry,
    /// The session's instrument, under the definition it trades.
    pub instrument: InstrumentId,
    /// That definition's lot, which the strategy sizes its exits in.
    pub lot: Qty,
    pub resumed: Resumption,
    /// The risk layer, its tally recounted from the journal.
    pub risk: RiskEngine,
}

/// Recover before anything else.
///
/// A restart on day nine has to come back up holding what it held, or the
/// strategy's first act is to trade against a position it does not know
/// about. `resume` registers the instrument's own definition before it
/// replays a line: this used to replay first, so after a restart with fills
/// the journal's placeholder rules won and the session traded under none. It
/// fails if the registry already held other rules for the symbol, which a
/// fresh registry cannot.
///
/// The risk layer's tally is recounted from the same journal on the registry
/// `resume` just made the instrument's own, so the holding lands under the id
/// the engine trades. Recovering the switch alone would check this session's
/// first orders as if from flat on a fresh day: an addition to a real
/// position against nothing, a real exit as the opposite position, and a loss
/// budget and order count already spent today as unspent.
///
/// # Errors
///
/// [`DefinitionConflict`], which a fresh registry cannot produce.
pub fn prepare(
    recovered: Recovered,
    definition: InstrumentDef,
    cash: Notional,
    limits: Limits,
) -> Result<Prepared, DefinitionConflict> {
    let mut registry = InstrumentRegistry::new();
    // The strategy sizes its exits in the venue's lots, so it is handed the
    // lot of the definition the session trades under, which `resume` makes
    // the registry's own (it refuses a registry that held other rules).
    let lot = definition.lot_size;
    let resumed = journal::resume(&recovered.entries, &mut registry, definition, cash)?;
    let instrument = resumed.instrument;
    let risk = journal::recover_risk(&recovered.entries, &mut registry, limits);
    Ok(Prepared {
        recovered,
        registry,
        instrument,
        lot,
        resumed,
        risk,
    })
}

/// The strategy, wrapped to sample equity.
///
/// Built before anything is written, so a size the venue would refuse stops
/// the session before the journal says one started.
///
/// # Errors
///
/// [`MaConfigError`], for settings that cannot be a crossover.
pub fn strategy(
    settings: Settings,
    instrument: InstrumentId,
    lot: Qty,
) -> Result<Recorded<MaCrossover>, MaConfigError> {
    Ok(Recorded::new(
        MaCrossover::new(MaConfig {
            instrument,
            fast: settings.fast,
            slow: settings.slow,
            interval: settings.interval,
            qty: settings.qty,
            lot,
        })?,
        instrument,
        settings.interval,
    ))
}

/// The engine's channel: the tee's secondary half, and what the engine reads.
#[must_use]
pub fn market_channel() -> (SyncSender<CaptureRecord>, Receiver<CaptureRecord>) {
    sync_channel(ENGINE_CHANNEL)
}

/// The engine, reading `records`, trading on `venue`, journalling through
/// `observer`, and resumed from `prepared` when the journal held something.
///
/// Resumed only when there is something to resume, which `resume` says with
/// `None` rather than an empty recompute. `resuming` replaces the portfolio
/// outright, so calling it with a fresh journal's empty recompute would
/// silently reset starting capital to zero -- which a rehearsal caught,
/// reporting "cash 0 from 0" on a session configured with a hundred.
///
/// And the id counter, which `resuming` does *not* carry: `Ledger::mint`
/// starts at 1 with the process, so a restart would hand out ids the file
/// already holds. `runlog check` reads density from 1 and would see no hole,
/// because there is none -- the ids are not missing, they are ambiguous, which
/// is worse. Built in M7.5.a as `minting_from` and called from nowhere until
/// the post-milestone audit found it: the fourth time this project has built
/// a thing and not wired it.
pub fn engine<V: ExecutionVenue>(
    records: Receiver<CaptureRecord>,
    venue: V,
    prepared: Prepared,
    strategy: Recorded<MaCrossover>,
    observer: Box<dyn RunObserver + Send>,
) -> Session<V> {
    let Prepared {
        instrument,
        resumed,
        risk,
        ..
    } = prepared;
    let mut engine = Engine::new(
        LiveSource::new(records, instrument),
        venue,
        risk,
        strategy,
        resumed.starting_cash,
    )
    .observing_fills(observer);
    if let Some(portfolio) = resumed.portfolio {
        engine = engine.resuming(portfolio);
        engine = engine.minting_from(resumed.next_order_id);
    }
    engine
}

/// What [`run_market`] ended with.
pub struct MarketRun<V, T> {
    pub engine: Session<V>,
    /// What `run` returned: a source error stops the run and is not an
    /// ending.
    pub outcome: Result<EngineStats, SourceError>,
    /// What `finish` returned.
    pub finished: T,
    /// How the capture ended.
    pub capture: Result<(), Box<dyn std::error::Error>>,
    /// Records the tee could not hand the engine.
    pub tee_dropped: u64,
}

impl<V, T> std::fmt::Debug for MarketRun<V, T> {
    /// By hand: the engine holds a journal writer behind a trait object, and
    /// what is worth printing is how things ended.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MarketRun")
            .field("outcome", &self.outcome)
            .field("capture", &self.capture)
            .field("tee_dropped", &self.tee_dropped)
            .finish_non_exhaustive()
    }
}

/// Capture `config`'s market and run `engine` on it until the capture ends,
/// then `finish` on the engine's thread.
///
/// The engine spends the whole run waiting on its channel. Since M8 the waits
/// are bounded, so that it can look at the venue while the market is quiet,
/// but each one still holds the thread it runs on -- so it gets a thread of
/// its own, the same reasoning that gives the capture writer one. It ends
/// when the capture drops the sender, which is how a clean shutdown reaches
/// it: the capture is finished, so its sender is gone, so the engine's
/// channel is closed, so the source returns `None` and `run` returns.
///
/// `finish` runs there, after `run`, before the engine comes back: where a
/// live run cancels what rests and drains its venue, still on the thread the
/// engine lives on.
///
/// # Errors
///
/// Only for a thread that could not be spawned or that panicked; how the
/// capture and the run ended is in [`MarketRun`].
pub fn run_market<V, T, F, Stop>(
    engine: Session<V>,
    engine_tx: SyncSender<CaptureRecord>,
    config: CaptureConfig,
    runtime: &tokio::runtime::Runtime,
    stop: Stop,
    name: &str,
    finish: F,
) -> Result<MarketRun<V, T>, Box<dyn std::error::Error>>
where
    V: ExecutionVenue + Send + 'static,
    T: Send + 'static,
    F: FnOnce(&mut Session<V>) -> T + Send + 'static,
    Stop: core::future::Future<Output = std::io::Result<()>>,
{
    let mut engine = engine;
    let engine_thread = std::thread::Builder::new()
        .name(format!("{name}-engine-{}", config.symbol))
        .spawn(move || {
            let outcome = engine.run();
            let finished = finish(&mut engine);
            (outcome, finished, engine)
        })?;

    // The tee is moved into the closure and gone, so the only way to learn
    // what it dropped is to hand it somewhere to count. Unreachable for the
    // whole of M5's fortnight, which is why that run can only *assume* the
    // engine saw every record.
    let tee_drops = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&tee_drops);
    let capture = runtime.block_on(capture::run_until(
        config,
        move |tx| quant_recorder::TeeSink::new(tx, engine_tx).counting_into(Arc::clone(&counter)),
        stop,
    ));
    let tee_dropped = tee_drops.load(Ordering::Relaxed);

    let (outcome, finished, engine) = engine_thread
        .join()
        .map_err(|_| format!("the {name} engine thread panicked"))?;
    Ok(MarketRun {
        engine,
        outcome,
        finished,
        capture,
        tee_dropped,
    })
}

/// The engine's closing lines: its final claim, and the mark that says this
/// was a clean stop, both at the last instant it covered.
///
/// Without the checkpoint there is nothing for the recompute to be compared
/// against, and `reconcile` would exit 2 on a perfectly good session. The
/// stop carries the orders still working, which against the simulator is any
/// whose reports were still in flight when the capture ended, and at a live
/// venue any the drain could not settle. The instant is the engine's last,
/// which runs to the last report it booked, so no line the session wrote is
/// stamped after these.
#[must_use]
pub fn closing<V: ExecutionVenue>(engine: &Session<V>) -> (JournalEntry, JournalEntry) {
    let at = engine
        .stats()
        .last_ts
        .unwrap_or(quant_core::time::Ts::from_nanos(0));
    (
        JournalEntry::checkpoint(at, engine.portfolio()),
        JournalEntry::Stopped {
            at,
            working: engine.working(),
        },
    )
}

/// Fills already on record, so the writer's checkpoint cadence carries on
/// across a restart rather than counting from zero again.
///
/// It once kept client order ids from colliding, when the journal's id was a
/// fill ordinal. The engine's own ids replaced that at M7.5.a; what is left is
/// `JournalWriter` checkpointing every `CHECKPOINT_EVERY` fills of the session.
#[must_use]
pub fn recovered_fills(entries: &[JournalEntry]) -> u64 {
    entries
        .iter()
        .filter(|e| matches!(e, JournalEntry::Filled { .. }))
        .count() as u64
}

/// What the session did, and whether its own record agrees.
///
/// # Errors
///
/// The journal could not be read back.
pub fn report<V>(
    engine: &Session<V>,
    journal_path: &Path,
    tee_dropped: u64,
) -> Result<(), Box<dyn std::error::Error>>
where
    V: ExecutionVenue,
{
    let portfolio = engine.portfolio();
    let stats = engine.stats();
    println!();
    println!(
        "events    {} reached the engine ({} gaps), {} execution events",
        stats.events, stats.gaps, stats.execution_events
    );
    // Two independent counts of one fact. The tee counts what it failed to hand
    // over; the engine counts the `ingest_seq` holes those drops left, and
    // journals each as a `blind` line. Neither is derived from the other, so a
    // disagreement means one of them is wrong -- and for the whole of M5 only
    // one of them could be read at all.
    if tee_dropped == 0 {
        println!("tee       0 records dropped on the way to the engine");
    } else {
        println!(
            "tee       {tee_dropped} records DROPPED on the way to the engine -- \
             the engine was blind for them, and journalled it"
        );
    }
    println!(
        "orders    {} sent, {} refused, {} fills",
        stats.submitted, stats.refused, stats.fills
    );
    println!(
        "cash      {} from {}",
        portfolio.cash(),
        portfolio.starting_cash()
    );
    println!("realized  {}", portfolio.realized());
    println!("fees      {}", portfolio.fees());
    println!("fills     {}", portfolio.fills());

    // The check, run in-process at the end as well as by `reconcile` afterwards.
    // Two independent computations of one quantity: the engine accumulated its
    // numbers in memory, this replays them from the file.
    let recovered = journal::read(journal_path)?;
    match journal::agrees(&recovered.entries) {
        Agreement::Agrees => println!("journal   AGREES with the engine"),
        // Not a disagreement, and not reported as one: nothing was checked. It
        // means no checkpoint reached the disk, which is itself worth knowing.
        Agreement::NothingToCheck => {
            println!("journal   NOTHING TO CHECK: no checkpoint reached the disk");
        }
        other => println!("journal   DISAGREES: {other:?} -- run `reconcile` for detail"),
    }
    Ok(())
}
