//! Run a strategy over the normalized tier and report what happened.
//!
//! ```text
//! backtest [DATA_ROOT] [--symbol SYM] [--cash N] [--fast N] [--slow N]
//!          [--interval-secs N] [--qty N] [--equity-csv PATH]
//!          [--realistic | --fee-rate R --latency-ms N --adverse P]
//!          [--max-order N] [--max-position N] [--max-daily-loss N] [--max-orders N]
//! backtest data/acceptance --symbol BTCUSDT              # free and instant
//! backtest data/acceptance --realistic                   # what it would cost
//! backtest data/acceptance --fee-rate 0.001              # fees only
//! ```
//!
//! Costs are **off by default**, so the default run is M3's. That is not
//! laziness: `Costs::NONE` reproducing the M3 numbers to the last digit is the
//! property that makes the cost models checkable at all, and it is easier to
//! trust when it is the thing that runs when you type nothing.
//!
//! **Limits are off by default too, for a second reason on top of that one.**
//! M5's criterion compares a paper session against a backtest over the same
//! window and demands they match exactly, so the two binaries have to be the same
//! system. Until now this one wired `AllowAll` while `paper` wired a real
//! `RiskEngine`: on a run where nothing bound, that difference was invisible, and
//! on the first run where something *did* bind it would have made the comparison
//! impossible rather than merely wrong — the backtest would send an order paper
//! had refused, and every number after it would differ for a reason that is not a
//! bug. So the layer is now always a `RiskEngine`, and `Limits::default()`
//! permits everything. The flags are spelled exactly as `paper`'s, so a paper
//! run's arguments can be replayed here verbatim.
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
use quant_core::fixed::{Px, Rate};
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
use quant_engine::{Engine, EngineStats, Limits, Portfolio, RiskEngine};
use quant_normalize::{discover_days, HistoricalSource};
use quant_sim::{Costs, FeeSchedule, Latency, SimStats, SimulatedVenue};

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
    costs: Costs,
    limits: Limits,
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
            // Free and instant unless asked otherwise. See the module docs.
            costs: Costs::NONE,
            // Permits everything, so a bare run is byte-identical to the one
            // before limits existed. `paper` defaults the other way and says why:
            // a fortnight unattended is the wrong place for no limits, whereas a
            // backtest whose limits changed under it would stop being a
            // measurement of the strategy.
            limits: Limits::default(),
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

    let mut engine = Engine::new(
        source,
        SimulatedVenue::with_costs(args.costs),
        RiskEngine::new(args.limits),
        strategy,
        args.cash,
    );
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

type Wiring = Engine<HistoricalSource, SimulatedVenue, RiskEngine, Recorded<MaCrossover>>;

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
    // Read back from the venue, not from the arguments. The first version of
    // this line printed what was parsed while the venue had been handed
    // `SimulatedVenue::new()` -- so the report announced ten basis points of fees
    // and the fills were free. A cost model that is configured but not wired
    // produces a confidently wrong number, which is the exact failure this
    // milestone exists to prevent. Asking the thing that did the work makes the
    // two impossible to disagree.
    let costs = engine.venue().costs();
    println!(
        "costs     fee maker {} taker {}, latency {}ms out / {}ms in, adverse {}",
        costs.fees.maker,
        costs.fees.taker,
        costs.latency.outbound / 1_000_000,
        costs.latency.inbound / 1_000_000,
        costs.adverse_per_fill,
    );
    // Same argument as the costs line above, applied to the other thing that can
    // be configured and not wired: ask the layer that did the refusing.
    print_limits(engine.risk().limits());
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
    print_caveats(costs);
}

/// What the risk layer was enforcing.
///
/// Printed on every run, including the default one where nothing is set. Silence
/// would be the wrong encoding: a reader comparing this against a paper session
/// needs to know whether the backtest had the same limits or none at all, and an
/// absent line reads as "not applicable" rather than "nothing was refusing".
fn print_limits(limits: Limits) {
    if limits == Limits::default() {
        println!("limits    none set -- nothing can be refused");
        return;
    }
    let show = |limit: Option<Notional>| {
        limit.map_or_else(|| "none".to_owned(), |value| value.to_string())
    };
    println!(
        "limits    order {}, position {}, daily loss {}, orders/day {}",
        show(limits.max_order_notional),
        show(limits.max_position_notional),
        show(limits.max_daily_loss),
        limits
            .max_orders_per_day
            .map_or_else(|| "none".to_owned(), |n| n.to_string()),
    );
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
    println!(
        "charged   {} in fees, {} reports still in flight when the data ran out",
        sim.fees_charged, sim.undelivered
    );
}

/// What the numbers above do not include.
///
/// Printed every time, not behind a flag. The engine contract requires the
/// absence of costs to be in the output: a result quoted without this list is
/// quoted dishonestly, and the surest way for that to happen is for the list to
/// be somewhere the reader has to go and look.
///
/// Driven by the actual [`Costs`] rather than being a fixed paragraph, so that
/// "not modelled" and "modelled as zero because you asked for zero" cannot be
/// confused -- and so the list gets shorter as M4's models get used, rather than
/// going stale.
fn print_caveats(costs: Costs) {
    println!("caveats   not modelled at all:");
    for caveat in SimStats::caveats() {
        println!("            - {caveat}");
    }
    let switched_off = SimStats::switched_off(costs);
    if !switched_off.is_empty() {
        println!("          switched off for this run:");
        for caveat in switched_off {
            println!("            - {caveat}");
        }
    }
    if costs.adverse_per_fill != Px::ZERO {
        println!(
            "          NOTE: --adverse is a stress knob, not a calibrated model. \
             Do not quote this as a slippage estimate."
        );
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
                     [--slow N] [--interval-secs N] [--qty N] [--equity-csv PATH] \
                     [--realistic] [--fee-rate R] [--latency-ms N] [--adverse P]\n\
                     \x20               [--max-order N] [--max-position N] \
                     [--max-daily-loss N] [--max-orders N]"
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
            // A preset rather than three flags, because the three belong
            // together: quoting a fee-only result as "realistic" would be
            // exactly the kind of half-costed number M4 exists to prevent.
            "--realistic" => args.costs = Costs::retail(),
            "--fee-rate" => {
                let rate: Rate = value()?.parse().map_err(|e| format!("--fee-rate: {e}"))?;
                args.costs.fees = FeeSchedule::flat(rate);
            }
            "--latency-ms" => {
                let ms: i64 = value()?.parse().map_err(|e| format!("--latency-ms: {e}"))?;
                args.costs.latency = Latency::millis(ms);
            }
            "--adverse" => {
                args.costs.adverse_per_fill =
                    value()?.parse().map_err(|e| format!("--adverse: {e}"))?;
            }
            // Spelled exactly as `paper`'s, deliberately. The whole point is that
            // a paper session's arguments can be replayed into a backtest, and a
            // flag that meant the same thing under a different name would be one
            // more thing to get right at the moment the comparison is being made.
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
    if args.slow <= args.fast {
        return Err("--slow must exceed --fast, or nothing ever crosses".to_owned());
    }
    if args.interval_secs <= 0 {
        return Err("--interval-secs must be positive".to_owned());
    }
    if args.costs.latency.outbound < 0 || args.costs.latency.inbound < 0 {
        return Err("latency cannot be negative; time does not work that way".to_owned());
    }
    if args.costs.adverse_per_fill.raw() < 0 {
        return Err("--adverse is a concession against us; it cannot be negative".to_owned());
    }
    Ok(Some(args))
}
