//! Paper trading: live prices, simulated fills, real bookkeeping.
//!
//! ```text
//! paper --symbol SYM [DATA_ROOT] [--cash N] [--qty N] [--hours N]
//!       [--journal PATH] [--free] [--max-position N] [--max-order N]
//!       [--max-daily-loss N] [--max-orders N]
//!
//! paper --symbol BTCUSDT data --hours 336     # the fortnight
//! paper --symbol BTCUSDT data --hours 1       # a rehearsal
//! ```
//!
//! # There is no `PaperVenue`, and that is the finding
//!
//! `CLAUDE.md`'s diagram *listed* `SimulatedVenue`, `PaperVenue` and `LiveVenue`
//! as three implementations, and this binary was expected to need the second. It
//! does not exist, and the diagram has since been corrected. A paper venue fills orders against a reconstructed book at
//! prices the book showed — which is exactly and entirely what [`SimulatedVenue`]
//! does. What separates a backtest from paper trading is the **source** and the
//! **durability**, not the matching.
//!
//! So this wires `LiveSource + SimulatedVenue`, and the three-venue row is really
//! two: simulated (M3) and live (M8). Writing a second fill model to satisfy a
//! diagram would have given the project two things that must agree forever and no
//! way to notice when they stopped.
//!
//! # The shape of the process
//!
//! ```text
//!   tokio runtime            OS thread
//!   ─────────────            ─────────
//!   capture::run  ──tee──►   LiveSource ──► Engine ──► SimulatedVenue
//!        │                                     │
//!        ▼                                     ▼
//!   raw capture                            journal
//! ```
//!
//! Two consumers of one ingress, per `docs/engine-contract.md` §9. The engine
//! runs on its own thread because [`LiveSource::next_event`] blocks — which is
//! correct, since the engine clock *is* the event stream and a quiet market means
//! no time passes for the strategy.
//!
//! # Why this refuses to run free
//!
//! A paper session exists to resemble live trading. Run with no fees it produces
//! a number that *looks* like paper trading and is not, and that number is
//! exactly the kind that gets quoted later without its caveats. So costs default
//! to `Costs::retail()` here — the opposite of `backtest`, where free is the
//! default because reproducing M3 exactly is what makes the cost models
//! checkable. `--free` exists for a rehearsal and says so, loudly, in the output.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quant_backtest::{MaConfig, MaCrossover, Recorded};
use quant_binance::capture::{self, CaptureConfig};
use quant_binance::LiveSource;
use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, Fill, OrderRequest, RejectReason};
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::instrument::{
    Exchange, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
};
use quant_core::time::Ts;
use quant_engine::journal::{self, Agreement};
use quant_engine::{
    Bound, Engine, InstrumentKey, Journal, JournalEntry, Limits, RefusedBy, RiskEngine,
    RunObserver, TripCause,
};
use quant_sim::{Costs, SimulatedVenue};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

/// How often the engine writes down what it believes.
///
/// Every checkpoint is what turns reconciliation from a tautology into a check,
/// so they are cheap and frequent rather than one at the end: a run that is
/// killed hard still leaves a recent claim for the recompute to be compared
/// against.
///
/// **Calibrated against the cadence that consumes it, not chosen for feel.**
/// `ops/verify-loop.sh` runs `reconcile` every six hours during a run, and a
/// checkpoint written less often than that means most passes re-check a claim
/// they already checked — or, before the first one, exit 2 with "nothing to
/// check". This was 25, and both 10-minute rehearsals produced 22 fills, so
/// neither wrote a single checkpoint before shutdown and the in-run
/// reconciliation reported `nothing to check yet` for the entire run. At the
/// fortnight's observed rate — M4's acceptance week was 418 fills over 7 days,
/// about 60 a day — 25 is roughly one checkpoint every ten hours, still coarser
/// than the thing reading it. Five is about two hours, so every reconcile pass
/// has something new.
///
/// The remaining hole is honest and unfixed: this triggers on *fills*, so a
/// strategy that trades less than five times between passes still leaves nothing
/// new to check, and a hard kill before the fifth fill loses the lot. A
/// time-based checkpoint would close it and needs a periodic hook the engine
/// does not have — `RunObserver` fires only on fills and `Engine::run` blocks —
/// which is a change to the seam and not a thing to slip in before a fortnight.
const CHECKPOINT_EVERY: u64 = 5;

/// Writes fills down before the strategy is told about them.
struct JournalWriter {
    /// Shared with the shutdown path, which writes the final checkpoint and the
    /// `Stopped` mark. Two owners and a few hundred writes over a fortnight, so
    /// the lock is never contended -- and the alternative, prising the journal
    /// back out of the engine, would mean the engine knowing what a journal is.
    journal: Arc<Mutex<Journal>>,
    symbol: String,
    fills: u64,
    failures: u64,
}

impl JournalWriter {
    /// Write down what the engine currently believes.
    ///
    /// The entry that turns reconciliation from a tautology into a check: the
    /// recompute derives its numbers from the fill lines, so it can only be
    /// compared against a claim made independently of them.
    fn checkpoint(&mut self, at: Ts, portfolio: &quant_engine::Portfolio) {
        self.record(&JournalEntry::checkpoint(at, portfolio), "a checkpoint");
    }

    /// Append one entry, counting a failure rather than ending the run.
    ///
    /// One place rather than one per call site, so the never-fatal policy
    /// cannot be adopted unevenly as entry types multiply -- which is the shape
    /// of the M4 and M5.c near-misses, where a rule held everywhere it was
    /// written and not where a later edit forgot it.
    fn record(&mut self, entry: &JournalEntry, what: &str) {
        if let Err(e) = write(&self.journal, entry) {
            // Counted and logged, never fatal. A paper run that died because it
            // could not write one line would lose the live session it exists to
            // conduct; a run that carries on with a hole in its journal is
            // recoverable, and both `reconcile` and `runlog check` will say so
            // afterwards -- the checkpoint disagrees, or the id sequence has a
            // hole where the decision should be.
            self.failures += 1;
            error!(error = %e, what, "could not journal");
        }
    }

    fn key(&self) -> InstrumentKey {
        InstrumentKey {
            exchange: Exchange::Binance,
            symbol: self.symbol.clone(),
        }
    }
}

impl RunObserver for JournalWriter {
    fn on_tripped(&mut self, cause: quant_engine::risk::TripCause, at: Ts) {
        // At the instant, not at shutdown. A hard kill between the two used to
        // lose the trip outright, and the supervisor would re-arm a switch that
        // had fired -- the hole `CLAUDE.md` carried as *must be fixed before
        // M8*. Written through the same failure-tolerant path as every other
        // entry, because a journal write that killed the process on the way
        // down would be a worse outcome than a missing line.
        self.record(&JournalEntry::Tripped { at, cause }, "a kill switch trip");
    }

    fn on_blind(&mut self, cause: quant_core::event::GapCause, last_good_ts: Ts, at: Ts) {
        self.record(
            &JournalEntry::Blind {
                at,
                cause,
                last_good_ts,
            },
            "a gap",
        );
    }

    fn on_fill(
        &mut self,
        client_order_id: ClientOrderId,
        _instrument: InstrumentId,
        side: Side,
        fill: &Fill,
        at: Ts,
        portfolio: &quant_engine::Portfolio,
    ) {
        self.fills += 1;
        self.record(
            &JournalEntry::Filled {
                at,
                client_order_id,
                instrument: self.key(),
                side,
                px: fill.px,
                qty: fill.qty,
                fee: fill.fee,
                is_maker: fill.is_maker,
            },
            "a fill",
        );
        // Frequent and cheap rather than one at the end: a run killed hard still
        // leaves a recent claim for the recompute to be compared against.
        if self.fills % CHECKPOINT_EVERY == 0 {
            self.checkpoint(at, portfolio);
        }
    }

    fn on_submitted(
        &mut self,
        client_order_id: ClientOrderId,
        request: &OrderRequest,
        mark: Option<Px>,
        at: Ts,
    ) {
        self.record(
            &JournalEntry::Submitted {
                at,
                client_order_id,
                instrument: self.key(),
                side: request.side,
                qty: request.qty,
                limit: request.limit(),
                mark,
            },
            "a submission",
        );
    }

    fn on_refused(
        &mut self,
        client_order_id: ClientOrderId,
        request: &OrderRequest,
        reason: RejectReason,
        bound: Option<Bound>,
        by: RefusedBy,
        at: Ts,
    ) {
        self.record(
            &JournalEntry::Refused {
                at,
                client_order_id,
                instrument: self.key(),
                side: request.side,
                qty: request.qty,
                reason,
                bound,
                by,
            },
            "a refusal",
        );
    }

    fn on_accepted(&mut self, client_order_id: ClientOrderId, at: Ts) {
        self.record(
            &JournalEntry::Accepted {
                at,
                client_order_id,
            },
            "an acceptance",
        );
    }

    fn on_rejected(&mut self, client_order_id: ClientOrderId, reason: RejectReason, at: Ts) {
        self.record(
            &JournalEntry::Rejected {
                at,
                client_order_id,
                reason,
            },
            "a rejection",
        );
    }

    fn on_cancel_requested(&mut self, client_order_id: ClientOrderId, at: Ts) {
        self.record(
            &JournalEntry::CancelRequested {
                at,
                client_order_id,
            },
            "a cancel request",
        );
    }

    fn on_cancelled(&mut self, client_order_id: ClientOrderId, remaining: Qty, at: Ts) {
        self.record(
            &JournalEntry::Cancelled {
                at,
                client_order_id,
                remaining,
            },
            "a cancellation",
        );
    }

    fn on_orphaned(&mut self, client_order_id: ClientOrderId, fill: &Fill, at: Ts) {
        // Loud as well as recorded: an unattributable fill means the venue and
        // our books disagree about what we own, and that is the one condition
        // where carrying on quietly is worse than the noise.
        error!(
            id = client_order_id.0,
            px = %fill.px, qty = %fill.qty,
            "a fill arrived for an order this engine has no record of"
        );
        self.record(
            &JournalEntry::Orphaned {
                at,
                client_order_id,
                px: fill.px,
                qty: fill.qty,
                fee: fill.fee,
            },
            "an orphaned fill",
        );
    }
}

/// Append one entry to the shared journal.
///
/// A poisoned lock is treated as a write failure rather than a panic: the
/// session is more valuable than the entry, and `reconcile` will notice the hole
/// afterwards because the checkpoint and the fill count will disagree.
fn write(journal: &Arc<Mutex<Journal>>, entry: &JournalEntry) -> std::io::Result<()> {
    journal
        .lock()
        .map_err(|_| std::io::Error::other("the journal lock is poisoned"))?
        .append(entry)
}

#[derive(Debug)]
struct Args {
    root: PathBuf,
    symbol: String,
    cash: Notional,
    qty: Qty,
    /// How long to run. Minutes rather than hours, so a rehearsal is expressible
    /// -- M1's lesson that a run harness which has never been run is not a
    /// harness applies here too, and an hour floor would leave the shutdown path
    /// unexercised until the fortnight itself.
    minutes: u64,
    journal: Option<PathBuf>,
    free: bool,
    limits: Limits,
    /// Indicator settings. Exposed so a rehearsal can actually produce fills:
    /// the fortnight's 10/30 on one-minute mids needs half an hour before it can
    /// cross, which would leave the journal path untested.
    fast: usize,
    slow: usize,
    interval_secs: i64,
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    quant_binance::install_crypto_provider();

    let args = match parse_args() {
        Ok(Some(args)) => args,
        Ok(None) => return ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };
    match run(&args) {
        Ok(code) => code,
        Err(e) => {
            error!(error = %e, "paper session stopped");
            ExitCode::FAILURE
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the wiring is the point of this binary"
)]
fn run(args: &Args) -> Result<ExitCode, Box<dyn std::error::Error>> {
    let costs = if args.free {
        Costs::NONE
    } else {
        Costs::retail()
    };
    let journal_path = args
        .journal
        .clone()
        .unwrap_or_else(|| args.root.join(format!("paper-{}.jsonl", args.symbol)));

    // Recover before anything else. A restart on day nine has to come back up
    // holding what it held, or the strategy's first act is to trade against a
    // position it does not know about.
    let recovered = journal::read(&journal_path)?;
    let mut registry = InstrumentRegistry::new();
    let resumed = journal::replay(&recovered.entries, &mut registry);
    let instrument = register(&mut registry, &args.symbol);
    let tripped = last_trip(&recovered.entries);

    let starting = if recovered.entries.is_empty() {
        args.cash
    } else {
        resumed.starting_cash()
    };

    print_banner(
        args,
        &journal_path,
        &recovered,
        &resumed,
        instrument,
        tripped,
    );

    let journal = Arc::new(Mutex::new(Journal::open(&journal_path)?));
    write(
        &journal,
        &JournalEntry::Started {
            // The wall clock, not a hard-coded zero. Both of the fortnight's
            // `started` lines read `"at":0` and were byte-identical, so a
            // journal that had recorded a restart could not date, or even
            // distinguish, its own two sessions. Operational only, in
            // `quant-meta`'s sense: nothing dispatches on it, and the engine's
            // own clock is still the event stream.
            at: quant_core::time::Clock::now(&quant_core::time::SystemClock),
            cash: starting,
            schema: quant_engine::journal::SCHEMA,
        },
    )?;

    // The engine's channel. Generous on purpose: holding a few thousand records
    // is cheaper than blinding the strategy, and the capture is never at risk
    // either way -- the tee drops the engine's copy, never the writer's.
    let (engine_tx, engine_rx) = sync_channel(8192);

    let strategy = Recorded::new(
        MaCrossover::new(MaConfig {
            instrument,
            fast: args.fast,
            slow: args.slow,
            interval: args.interval_secs * 1_000_000_000,
            qty: args.qty,
        }),
        instrument,
        args.interval_secs * 1_000_000_000,
    );
    let observer = Box::new(JournalWriter {
        journal: Arc::clone(&journal),
        symbol: args.symbol.clone(),
        fills: recovered_fills(&recovered.entries),
        failures: 0,
    });

    let mut engine = Engine::new(
        LiveSource::new(engine_rx, instrument),
        // See `backtest`: the venue enforces the venue's rules in every world,
        // so paper cannot fill what live would refuse.
        SimulatedVenue::with_costs(costs).enforcing(
            instrument,
            registry.get(instrument).expect("just registered").filters(),
        ),
        RiskEngine::recover(args.limits, tripped),
        strategy,
        starting,
    )
    .observing_fills(observer);
    // Only when there is something to resume. `resuming` replaces the portfolio
    // outright, so calling it with a fresh journal's empty recompute would
    // silently reset starting capital to zero -- which a rehearsal caught,
    // reporting "cash 0 from 0" on a session configured with a hundred.
    if !recovered.entries.is_empty() {
        engine = engine.resuming(resumed);
    }

    // The engine blocks on its channel, so it gets a thread of its own -- the
    // same reasoning that gives the capture writer one. It ends when the capture
    // drops the sender, which is how a clean shutdown reaches it.
    let engine_thread = std::thread::Builder::new()
        .name(format!("paper-engine-{}", args.symbol))
        .spawn(move || {
            let outcome = engine.run();
            (outcome, engine)
        })?;

    let config = CaptureConfig::new(args.symbol.clone(), args.root.clone())
        .for_at_most(Duration::from_secs(args.minutes * 60));
    let runtime = tokio::runtime::Runtime::new()?;
    // The tee is moved into the closure and gone, so the only way to learn what
    // it dropped is to hand it somewhere to count. Unreachable for the whole of
    // M5's fortnight, which is why that run can only *assume* the engine saw
    // every record.
    let tee_drops = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counter = std::sync::Arc::clone(&tee_drops);
    let capture = runtime.block_on(capture::run(config, move |tx| {
        quant_recorder::TeeSink::new(tx, engine_tx).counting_into(std::sync::Arc::clone(&counter))
    }));
    let tee_dropped = tee_drops.load(std::sync::atomic::Ordering::Relaxed);

    // The capture is finished, so its sender is gone, so the engine's channel is
    // closed, so `next_event` returns None and the thread ends on its own.
    let (outcome, engine) = engine_thread
        .join()
        .map_err(|_| "the paper engine thread panicked")?;
    outcome?;

    // The engine's final claim, and the mark that says this was a clean stop.
    // Without the checkpoint there is nothing for the recompute to be compared
    // against, and `reconcile` would exit 2 on a perfectly good session.
    let stopped_at = engine.stats().last_ts.unwrap_or(Ts::from_nanos(0));
    write(
        &journal,
        &JournalEntry::checkpoint(stopped_at, engine.portfolio()),
    )?;
    // The trip itself is journalled by `on_tripped` at the instant it happens,
    // so there is nothing to write here -- only something to say. Writing it
    // again would put a second `tripped` line in the file stamped with the
    // shutdown time, which is what this slice removed.
    if let Some(cause) = engine.risk().tripped() {
        warn!(%cause, "the kill switch is thrown; the next session will refuse orders");
    }
    write(&journal, &JournalEntry::Stopped { at: stopped_at })?;

    report(&engine, &journal_path, tee_dropped)?;
    capture.map(|()| ExitCode::SUCCESS)
}

/// Everything the run is about to do, before it does any of it.
#[allow(clippy::too_many_arguments, reason = "a banner names what it names")]
fn print_banner(
    args: &Args,
    journal_path: &std::path::Path,
    recovered: &journal::Recovered,
    resumed: &quant_engine::Portfolio,
    instrument: InstrumentId,
    tripped: Option<TripCause>,
) {
    println!("root      {}", args.root.display());
    println!("symbol    {} on {}", args.symbol, Exchange::Binance);
    println!("duration  {} minutes", args.minutes);
    println!(
        "strategy  MA crossover, fast {} slow {} on {}s mids, {} per position",
        args.fast, args.slow, args.interval_secs, args.qty
    );
    println!(
        "journal   {} ({} entries{})",
        journal_path.display(),
        recovered.entries.len(),
        if recovered.torn_tail {
            ", last line torn and discarded"
        } else {
            ""
        }
    );
    if recovered.entries.is_empty() {
        println!("resuming  no: a fresh session");
    } else {
        println!(
            "resuming  position {}, cash {}, {} fills already booked",
            resumed.position(instrument).qty,
            resumed.cash(),
            resumed.fills()
        );
    }
    if let Some(cause) = tripped {
        // Loud, because it means this process will refuse every order until a
        // person clears it. A run that silently did nothing for a fortnight is
        // the worst possible outcome here.
        println!("RISK      TRIPPED by {cause}. Nothing will be sent. Clear it deliberately.");
        warn!(%cause, "starting with the kill switch already thrown");
    }
    println!("{}", quant_backtest::limits_line(args.limits));
    if args.free {
        println!("costs     NONE -- a rehearsal, not a paper result. Do not quote it.");
        warn!("running with no fees or latency; this is not a paper trading result");
    } else {
        println!("costs     retail: Binance spot fees, 50ms each way");
    }
    println!();
}

/// What the session did, and whether its own record agrees.
fn report<V, R, K>(
    engine: &Engine<LiveSource, V, R, K>,
    journal_path: &std::path::Path,
    tee_dropped: u64,
) -> Result<(), Box<dyn std::error::Error>>
where
    V: quant_engine::ExecutionVenue,
    R: quant_engine::RiskLayer,
    K: quant_engine::Strategy,
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

/// The trip state a previous session left behind.
///
/// A kill switch that forgets is not a kill switch: the supervisor exists to
/// restart a dead process, so a switch held only in memory would be re-armed by
/// the machinery meant to keep things running.
fn last_trip(entries: &[JournalEntry]) -> Option<TripCause> {
    entries.iter().rev().find_map(|e| match e {
        JournalEntry::Tripped { cause, .. } => Some(*cause),
        // Deliberately *not* cleared by a later `Started`: a restart is exactly
        // the moment a forgotten switch would be re-armed.
        _ => None,
    })
}

/// Fills already on record, so a restart's client order ids do not collide.
fn recovered_fills(entries: &[JournalEntry]) -> u64 {
    entries
        .iter()
        .filter(|e| matches!(e, JournalEntry::Filled { .. }))
        .count() as u64
}

fn register(registry: &mut InstrumentRegistry, symbol: &str) -> InstrumentId {
    registry.register(InstrumentDef {
        exchange: Exchange::Binance,
        symbol: symbol.to_owned(),
        base: String::new(),
        quote: String::new(),
        kind: InstrumentKind::Spot,
        tick_size: "0.01".parse().expect("a valid literal"),
        lot_size: "0.00001".parse().expect("a valid literal"),
        min_notional: "5".parse().expect("a valid literal"),
    })
}

fn parse_args() -> Result<Option<Args>, String> {
    let mut args = Args {
        root: PathBuf::from("data"),
        symbol: String::new(),
        cash: "100".parse().expect("a valid amount"),
        qty: "0.001".parse().expect("a valid quantity"),
        minutes: 336 * 60,
        journal: None,
        free: false,
        // Sized for $100 of capital trading 0.001 BTC (~$76). Explicit rather
        // than defaulted to nothing, because Limits::default() permits
        // everything and a fortnight unattended is the wrong place for that.
        fast: 10,
        slow: 30,
        interval_secs: 60,
        limits: Limits {
            max_order_notional: Some("200".parse().expect("a valid amount")),
            max_position_notional: Some("200".parse().expect("a valid amount")),
            max_daily_loss: Some("20".parse().expect("a valid amount")),
            max_orders_per_day: Some(200),
        },
    };
    let mut remaining = std::env::args().skip(1);
    while let Some(arg) = remaining.next() {
        let mut value = || {
            remaining
                .next()
                .ok_or_else(|| format!("{arg} needs a value"))
        };
        match arg.as_str() {
            "-h" | "--help" => {
                println!(
                    "usage: paper --symbol SYM [DATA_ROOT] [--cash N] [--qty N] [--hours N]\n\
                     \x20            [--journal PATH] [--free]\n\
                     \x20            [--max-order N] [--max-position N] [--max-daily-loss N] [--max-orders N]"
                );
                return Ok(None);
            }
            "--symbol" => args.symbol = value()?,
            "--cash" => args.cash = value()?.parse().map_err(|e| format!("--cash: {e}"))?,
            "--qty" => args.qty = value()?.parse().map_err(|e| format!("--qty: {e}"))?,
            "--hours" => {
                let hours: u64 = value()?.parse().map_err(|e| format!("--hours: {e}"))?;
                args.minutes = hours * 60;
            }
            "--minutes" => {
                args.minutes = value()?.parse().map_err(|e| format!("--minutes: {e}"))?;
            }
            "--fast" => args.fast = value()?.parse().map_err(|e| format!("--fast: {e}"))?,
            "--slow" => args.slow = value()?.parse().map_err(|e| format!("--slow: {e}"))?,
            "--interval-secs" => {
                args.interval_secs = value()?
                    .parse()
                    .map_err(|e| format!("--interval-secs: {e}"))?;
            }
            "--journal" => args.journal = Some(PathBuf::from(value()?)),
            "--free" => args.free = true,
            "--max-order" => {
                args.limits.max_order_notional =
                    Some(value()?.parse().map_err(|e| format!("--max-order: {e}"))?);
            }
            "--max-position" => {
                args.limits.max_position_notional = Some(
                    value()?
                        .parse()
                        .map_err(|e| format!("--max-position: {e}"))?,
                );
            }
            "--max-daily-loss" => {
                args.limits.max_daily_loss = Some(
                    value()?
                        .parse()
                        .map_err(|e| format!("--max-daily-loss: {e}"))?,
                );
            }
            "--max-orders" => {
                args.limits.max_orders_per_day =
                    Some(value()?.parse().map_err(|e| format!("--max-orders: {e}"))?);
            }
            other if other.starts_with('-') => return Err(format!("unknown option: {other}")),
            other => args.root = PathBuf::from(other),
        }
    }
    if args.symbol.is_empty() {
        return Err("--symbol is required; a paper session trades one instrument".to_owned());
    }
    if args.minutes == 0 {
        return Err("the duration must be positive".to_owned());
    }
    if args.slow <= args.fast {
        return Err("--slow must exceed --fast, or the averages never cross".to_owned());
    }
    if args.interval_secs <= 0 {
        return Err("--interval-secs must be positive".to_owned());
    }
    info!(symbol = %args.symbol, "paper session configured");
    Ok(Some(args))
}
