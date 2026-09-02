//! Run a strategy over the normalized tier and report what happened.
//!
//! ```text
//! backtest [DATA_ROOT] [--symbol SYM] [--cash N] [--fast N] [--slow N]
//!          [--interval-secs N] [--qty N] [--equity-csv PATH]
//! backtest data/acceptance --symbol BTCUSDT
//! ```
//!
//! Exit code 0 means the run completed. It says **nothing** about whether the
//! strategy made money, and there is deliberately no threshold that would let it
//! — `docs/engine-contract.md` §7 makes an unimpressive curve the criterion, so
//! a tool that failed on a bad result would fail on success.

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use quant_backtest::{EquityCurve, MaCrossover, Recorded};
use quant_core::fixed::{Notional, Qty};
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
use quant_engine::{AllowAll, Engine, EngineStats, Portfolio};
use quant_normalize::{discover_days, HistoricalSource};
use quant_sim::{SimStats, SimulatedVenue};

const NANOS_PER_SEC: i64 = 1_000_000_000;

/// Everything the run was configured with.
#[derive(Debug)]
struct Args {
    root: PathBuf,
    symbol: String,
    cash: Notional,
    fast: usize,
    slow: usize,
    interval_secs: i64,
    qty: Qty,
    equity_csv: Option<PathBuf>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            root: PathBuf::from("data"),
            symbol: "BTCUSDT".to_owned(),
            // Deliberately the real number from docs/overview.md's sizing
            // discussion, not a round 1e6: a backtest at a size we will never
            // trade is a backtest of a different strategy.
            cash: "100".parse().expect("a valid amount"),
            fast: 10,
            slow: 30,
            interval_secs: 60,
            // ~$76 of BTC at the prices in the acceptance capture, so one
            // position is most of the account.
            qty: "0.001".parse().expect("a valid quantity"),
            equity_csv: None,
        }
    }
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(Some(args)) => args,
        Ok(None) => return ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let mut registry = InstrumentRegistry::new();
    let instrument = registry.register(InstrumentDef {
        exchange: Exchange::Binance,
        symbol: args.symbol.clone(),
        base: String::new(),
        quote: String::new(),
        kind: InstrumentKind::Spot,
        tick_size: "0.01".parse().expect("tick"),
        lot_size: "0.00001".parse().expect("lot"),
        min_notional: "5".parse().expect("notional"),
    });

    let days = discover_days(&args.root, Exchange::Binance, &args.symbol);
    if days.is_empty() {
        eprintln!(
            "no normalized data for {} under {} -- run `normalize --write` first",
            args.symbol,
            args.root.display()
        );
        return ExitCode::FAILURE;
    }

    let source = HistoricalSource::new(
        &args.root,
        Exchange::Binance,
        &args.symbol,
        instrument,
        days.clone(),
    );
    let strategy = Recorded::new(
        MaCrossover::new(quant_backtest::ma::MaConfig {
            instrument,
            fast: args.fast,
            slow: args.slow,
            interval: args.interval_secs * NANOS_PER_SEC,
            qty: args.qty,
        }),
        instrument,
        args.interval_secs * NANOS_PER_SEC,
    );

    let mut engine = Engine::new(source, SimulatedVenue::new(), AllowAll, strategy, args.cash);
    let stats = match engine.run() {
        Ok(stats) => stats,
        Err(e) => {
            eprintln!("the source failed: {e}");
            return ExitCode::FAILURE;
        }
    };

    report(&args, &days, stats, &engine);

    if let Some(path) = &args.equity_csv {
        if let Err(e) = write_csv(path, engine.strategy().curve()) {
            eprintln!("could not write {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
        println!("equity    written to {}", path.display());
    }

    ExitCode::SUCCESS
}

type Wiring = Engine<HistoricalSource, SimulatedVenue, AllowAll, Recorded<MaCrossover>>;

fn report(args: &Args, days: &[quant_core::time::UtcDate], stats: EngineStats, engine: &Wiring) {
    let curve = engine.strategy().curve();
    let ma = engine.strategy().inner().stats();
    let portfolio = engine.portfolio();

    println!("root      {}", args.root.display());
    println!(
        "period    {} days, {} .. {}",
        days.len(),
        days.first().map_or_else(String::new, ToString::to_string),
        days.last().map_or_else(String::new, ToString::to_string),
    );
    println!(
        "strategy  MA crossover, fast {} slow {} on {}s mids, {} per position",
        args.fast, args.slow, args.interval_secs, args.qty
    );
    println!(
        "events    {} ({} gaps), {} execution events",
        stats.events, stats.gaps, stats.execution_events
    );
    println!(
        "signals   {} samples, {} crossings, {} blind intervals",
        ma.samples, ma.crossings, ma.blind_intervals
    );
    println!(
        "orders    {} submitted, {} refused, {} suppressed while one was working",
        stats.submitted, stats.refused, ma.suppressed
    );
    print_pnl(portfolio, curve, ma.entries, ma.exits);
    print_sim(engine.venue().stats());
    print_caveats();
}

fn print_pnl(portfolio: &Portfolio, curve: &EquityCurve, entries: u64, exits: u64) {
    println!(
        "fills     {} ({entries} entries signalled, {exits} exits signalled)",
        portfolio.fills()
    );
    println!(
        "cash      {} from {}",
        portfolio.cash(),
        portfolio.starting_cash()
    );
    println!("realized  {}", portfolio.realized());
    println!("fees      {} (M4 has not happened yet)", portfolio.fees());
    match (curve.first(), curve.last()) {
        (Some(first), Some(last)) => {
            let pnl = Notional::from_raw(last.raw() - first.raw());
            println!("equity    {first} -> {last}, change {pnl}");
        }
        _ => println!("equity    never valued: no mark at any sample"),
    }
    if let Some((lo, hi)) = curve.range() {
        println!("range     low {lo}, high {hi}");
    }
    println!("drawdown  {} at worst", curve.max_drawdown());
    println!(
        "samples   {} recorded, {} with no mark",
        curve.points.len(),
        curve.blind_samples()
    );
}

fn print_sim(sim: SimStats) {
    println!(
        "venue     {} fills, {} walked more than one level, {} exhausted the book, {} refused for no market",
        sim.fills, sim.multi_level_fills, sim.exhausted_book, sim.no_market
    );
}

/// What the numbers above do not include.
///
/// Printed every time, not behind a flag. The engine contract requires the
/// absence of costs to be in the output: a result quoted without this list is
/// quoted dishonestly, and the surest way for that to happen is for the list to
/// be somewhere the reader has to go and look.
fn print_caveats() {
    println!("caveats   this is M3; the following are NOT modelled:");
    for caveat in SimStats::caveats() {
        println!("            - {caveat}");
    }
}

fn write_csv(path: &PathBuf, curve: &EquityCurve) -> std::io::Result<()> {
    let mut out = std::io::BufWriter::new(std::fs::File::create(path)?);
    writeln!(out, "ts_nanos,equity,cash,mark")?;
    for point in &curve.points {
        // An empty field for a missing equity, not a zero and not a carried
        // value: the reader has to decide what to do about a hole, and a zero
        // would look like a wiped-out account.
        writeln!(
            out,
            "{},{},{},{}",
            point.ts.as_nanos(),
            point.equity.map_or_else(String::new, |e| e.to_string()),
            point.cash,
            point.mark.map_or_else(String::new, |m| m.to_string()),
        )?;
    }
    out.flush()
}

/// `Ok(None)` means `--help` was asked for and printed.
fn parse_args() -> Result<Option<Args>, String> {
    let mut args = Args::default();
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
                    "usage: backtest [DATA_ROOT] [--symbol SYM] [--cash N] [--fast N] \
                     [--slow N] [--interval-secs N] [--qty N] [--equity-csv PATH]"
                );
                return Ok(None);
            }
            "--symbol" => args.symbol = value()?,
            "--cash" => args.cash = value()?.parse().map_err(|e| format!("--cash: {e}"))?,
            "--qty" => args.qty = value()?.parse().map_err(|e| format!("--qty: {e}"))?,
            "--fast" => args.fast = value()?.parse().map_err(|e| format!("--fast: {e}"))?,
            "--slow" => args.slow = value()?.parse().map_err(|e| format!("--slow: {e}"))?,
            "--interval-secs" => {
                args.interval_secs = value()?
                    .parse()
                    .map_err(|e| format!("--interval-secs: {e}"))?;
            }
            "--equity-csv" => args.equity_csv = Some(PathBuf::from(value()?)),
            other if other.starts_with('-') => return Err(format!("unknown option: {other}")),
            other => args.root = PathBuf::from(other),
        }
    }
    if args.slow <= args.fast {
        return Err("--slow must exceed --fast, or nothing ever crosses".to_owned());
    }
    if args.interval_secs <= 0 {
        return Err("--interval-secs must be positive".to_owned());
    }
    Ok(Some(args))
}
