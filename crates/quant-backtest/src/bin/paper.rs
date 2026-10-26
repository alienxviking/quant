//! Paper trading: live prices, simulated fills, real bookkeeping.
//!
//! ```text
//! paper --symbol SYM [DATA_ROOT] [--cash N] [--qty N] [--hours N]
//!       [--journal PATH] [--free] [--max-position N] [--max-order N]
//!       [--max-daily-loss N] [--max-orders N] [--market-ws URL --rest URL]
//!
//! paper --symbol BTCUSDT data --hours 336     # the fortnight
//! paper --symbol BTCUSDT data --hours 1       # a rehearsal
//! ```
//!
//! `--market-ws` and `--rest` point the capture at another market stream and
//! REST base, together: a local stand-in, for a rehearsal that must not reach
//! the venue. Without them it captures the live venue, as it always has.
//!
//! The session itself is `quant_backtest::session`, extracted from here so
//! that the live binary runs the same path with its own venue.
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
//! runs on its own thread because it spends the whole run waiting on
//! [`LiveSource`] -- in bounded waits, so that a live venue's reports are looked
//! for while the market is quiet, but for a thread's whole life all the same. A
//! quiet market still means no time passes for the strategy: an idle wake
//! carries no time, and against `SimulatedVenue` it never finds anything to
//! deliver. `quant_binance::live`'s module docs have the argument.
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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quant_backtest::record::{write, JournalWriter};
use quant_backtest::session::{self, Settings};
use quant_binance::capture::CaptureConfig;
use quant_core::fixed::{Notional, Qty};
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind};
use quant_engine::journal;
use quant_engine::{Journal, JournalEntry, Limits, RiskEngine};
use quant_sim::{Costs, SimulatedVenue};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

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
    /// The market stream and REST base to capture from, when not the live
    /// venue's: a local stand-in for a rehearsal.
    endpoints: Option<(String, String)>,
}

fn main() -> ExitCode {
    // JSON, so the metrics line is a value rather than prose. `explain` reads it
    // back through `MetricsLine`, and the two sharing a type is what M7.5.g
    // repaid -- the emitter and the reader previously agreed through a rendering
    // neither of them owned, with nothing to notice when it changed.
    tracing_subscriber::fmt()
        .json()
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

    // Recover before anything else: `session::prepare` says why, and what the
    // risk layer's tally is recounted from.
    let recovered = journal::read(&journal_path)?;
    let prepared = session::prepare(recovered, definition(&args.symbol), args.cash, args.limits)?;
    let instrument = prepared.instrument;
    let starting = prepared.resumed.starting_cash;

    // Built before anything is written, so a size the venue would refuse stops
    // the session here rather than after the journal says one started.
    let strategy = session::strategy(
        Settings {
            fast: args.fast,
            slow: args.slow,
            interval: args.interval_secs * 1_000_000_000,
            qty: args.qty,
        },
        instrument,
        prepared.lot,
    )?;

    print_banner(
        args,
        &journal_path,
        &prepared.recovered,
        &prepared.resumed,
        &prepared.risk,
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
            // No run tag: nothing here is sent to a venue, so there are no
            // client ids at the venue to tell apart.
            run: None,
        },
    )?;

    let (engine_tx, engine_rx) = session::market_channel();

    let observer = Box::new(JournalWriter::new(
        Arc::clone(&journal),
        args.symbol.clone(),
        session::recovered_fills(&prepared.recovered.entries),
    ));

    // See `backtest`: the venue enforces the venue's rules in every world, so
    // paper cannot fill what live would refuse.
    let venue = SimulatedVenue::with_costs(costs).enforcing(
        instrument,
        prepared
            .registry
            .get(instrument)
            .expect("just registered")
            .filters(),
    );
    let engine = session::engine(engine_rx, venue, prepared, strategy, observer);

    let mut config = CaptureConfig::new(args.symbol.clone(), args.root.clone())
        .for_at_most(Duration::from_secs(args.minutes * 60));
    if let Some((ws, rest)) = &args.endpoints {
        config = config.at(ws.clone(), rest.clone());
    }
    let runtime = tokio::runtime::Runtime::new()?;
    let ran = session::run_market(
        engine,
        engine_tx,
        config,
        &runtime,
        tokio::signal::ctrl_c(),
        "paper",
        |_| (),
    )?;
    let engine = ran.engine;
    ran.outcome?;

    // The engine's final claim, and the mark that says this was a clean stop
    // (`session::closing` says why each).
    let (checkpoint, stopped) = session::closing(&engine);
    write(&journal, &checkpoint)?;
    // The trip itself is journalled by `on_tripped` at the instant it happens,
    // so there is nothing to write here -- only something to say. Writing it
    // again would put a second `tripped` line in the file stamped with the
    // shutdown time, which is what this slice removed.
    if let Some(cause) = engine.risk().tripped() {
        warn!(%cause, "the kill switch is thrown; the next session will refuse orders");
    }
    write(&journal, &stopped)?;

    session::report(&engine, &journal_path, ran.tee_dropped)?;
    ran.capture.map(|()| ExitCode::SUCCESS)
}

/// Everything the run is about to do, before it does any of it.
fn print_banner(
    args: &Args,
    journal_path: &std::path::Path,
    recovered: &journal::Recovered,
    resumed: &journal::Resumption,
    risk: &RiskEngine,
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
    if !recovered.torn_before_restart.is_empty() {
        let lines: Vec<String> = recovered
            .torn_before_restart
            .iter()
            .map(ToString::to_string)
            .collect();
        println!(
            "          not valid JSON, the shape an interrupted write leaves, and written \
             past by a restart: {} {}",
            if lines.len() == 1 { "line" } else { "lines" },
            lines.join(", ")
        );
    }
    match &resumed.portfolio {
        None => println!("resuming  no: a fresh session"),
        Some(portfolio) => println!(
            "resuming  position {}, cash {}, {} fills already booked",
            portfolio.position(resumed.instrument).qty,
            portfolio.cash(),
            portfolio.fills()
        ),
    }
    if resumed.portfolio.is_some() {
        // What the limits will be judged against, said before the first
        // order: the holding and the day's count and loss the journal
        // recounts to, by risk's own reckoning rather than the portfolio's.
        // Named by the day they belong to, which is the journal's last and
        // may be yesterday: the first check today rolls them to zero, so
        // calling them "today" would overstate what is spent.
        let holding = risk.position(resumed.instrument);
        match risk.counted_day() {
            Some(day) => println!(
                "risk      holding {holding}, and on {day}, the last day the journal \
                 counted: {} orders, realized {}",
                risk.orders_today(),
                risk.realized_today()
            ),
            None => println!("risk      holding {holding}, and no order or fill counted yet"),
        }
    }
    if let Some(cause) = resumed.tripped {
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

/// The instrument this session trades, with the rules its venue enforces.
///
/// The same definition `backtest` registers, which is part of what lets the two
/// be compared. Handed to `journal::resume` rather than registered here because
/// the order is the point: registered after a replay of fills, it was silently
/// ignored. The asset names stay empty because nothing in a paper session reads
/// them. The user-data translator does, and refuses an instrument without both,
/// so a live session cannot take its definition from here.
fn definition(symbol: &str) -> InstrumentDef {
    InstrumentDef {
        exchange: Exchange::Binance,
        symbol: symbol.to_owned(),
        base: String::new(),
        quote: String::new(),
        kind: InstrumentKind::Spot,
        tick_size: "0.01".parse().expect("a valid literal"),
        lot_size: "0.00001".parse().expect("a valid literal"),
        min_notional: "5".parse().expect("a valid literal"),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one arm per flag, which reads better as one list than as several"
)]
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
        endpoints: None,
        limits: Limits {
            max_order_notional: Some("200".parse().expect("a valid amount")),
            max_position_notional: Some("200".parse().expect("a valid amount")),
            max_daily_loss: Some("20".parse().expect("a valid amount")),
            max_orders_per_day: Some(200),
        },
    };
    let (mut market_ws, mut rest) = (None, None);
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
                     \x20            [--max-order N] [--max-position N] [--max-daily-loss N] [--max-orders N]\n\
                     \x20            [--market-ws URL --rest URL]"
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
            "--market-ws" => market_ws = Some(value()?),
            "--rest" => rest = Some(value()?),
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
    // Both or neither: a market from one place and its snapshots from another
    // would anchor a book with a stranger's depth.
    args.endpoints = match (market_ws, rest) {
        (Some(ws), Some(rest)) => Some((ws, rest)),
        (None, None) => None,
        _ => return Err("--market-ws and --rest go together".to_owned()),
    };
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
