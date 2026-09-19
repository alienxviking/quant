//! What was the system doing at a given instant.
//!
//! ```text
//! explain [DATA_ROOT] --at <RFC3339> [--symbol SYM]
//! explain ~/paper --at 2026-09-18T14:46:54Z --symbol BTCUSDT
//! explain ~/paper --at "2026-09-18T20:16:54+05:30"      # your wall clock
//! ```
//!
//! Five blocks, every one of them named so that its absence is visible: when,
//! market, ours, health, provenance. A block that cannot be filled says why
//! rather than printing nothing — a blank is indistinguishable from "nothing
//! happened", and the characteristic failure of a tool like this is
//! confabulation.
//!
//! Exit 0 means the instant was answered, including "we were blind then, and
//! here is the gap that says so". Non-zero means no answer could be given at
//! all.
//!
//! ```text
//! explain --check-journal ~/paper/paper-BTCUSDT.jsonl
//! ```
//!
//! checks every checkpoint in a journal against a replay of the entries it
//! describes -- M7's P2. Exit 1 on a disagreement, 2 when there was nothing to
//! check, because "nobody disagreed" and "two answers matched" are different
//! statements and only one of them is evidence.

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

/// `println!`, but a closed pipe ends the program quietly instead of panicking.
///
/// This tool is meant to be piped into `head`, `grep` or `less`, and Rust
/// ignores `SIGPIPE` by default -- so the write returns `EPIPE` and the default
/// `println!` turns that into a panic with a backtrace. A debugging tool that
/// panics when you pipe it into `head` teaches you not to pipe it into `head`.
///
/// The usual fix is to restore the default signal handler, which needs `unsafe`
/// and the workspace forbids it. Writing through a locked handle and treating a
/// broken pipe as a normal end is the same outcome in safe code.
macro_rules! outln {
    ($($arg:tt)*) => {{
        // Deliberately ignored rather than propagated: once the reader has gone
        // there is nobody to tell, and the work already done is still correct.
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, $($arg)*);
    }};
}

use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
use quant_core::time::Ts;
use quant_explain::{market_at, ours_at, MarketAt, OurState};

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(Some(args)) => args,
        Ok(None) => return ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    if let Some(path) = &args.check_journal {
        return check_journal(path);
    }

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

    outln!("when      {}", args.at.to_rfc3339());
    if args.as_typed != args.at.to_rfc3339() {
        // Echoed in both forms, always. The offset is the first thing to get
        // wrong and the last thing anyone suspects: a query aimed five and a
        // half hours from where it was meant still returns a confident answer.
        outln!("          {} as given", args.as_typed);
    }

    let market = match market_at(
        &args.root,
        Exchange::Binance,
        &args.symbol,
        instrument,
        args.at,
        args.window_nanos,
    ) {
        Ok(market) => market,
        Err(e) => {
            outln!("market    {e}");
            return ExitCode::FAILURE;
        }
    };
    print_market(&market);

    let journal = args
        .journal
        .clone()
        .unwrap_or_else(|| args.root.join(format!("paper-{}.jsonl", args.symbol)));
    match ours_at(&journal, instrument, &mut registry, args.at) {
        Ok(ours) => print_ours(&ours, instrument, &market),
        Err(e) => outln!("ours      could not read {}: {e}", journal.display()),
    }

    print_window(&market);
    print_health(&args);
    print_provenance(&args, &market);
    ExitCode::SUCCESS
}

/// P2, as a command: every checkpoint against the entries it describes.
fn check_journal(path: &std::path::Path) -> ExitCode {
    match quant_explain::check_all(path) {
        Err(e) => {
            eprintln!("could not read {}: {e}", path.display());
            ExitCode::FAILURE
        }
        Ok(agreement) => {
            outln!("journal   {}", path.display());
            match agreement.divergence {
                Some(why) => {
                    outln!(
                        "checked   {} before the first disagreement",
                        agreement.matched
                    );
                    outln!();
                    outln!("verdict   DISAGREES: {why}");
                    ExitCode::FAILURE
                }
                None if agreement.matched == 0 => {
                    outln!();
                    outln!(
                        "verdict   NOTHING TO CHECK: this journal holds no checkpoint, so no \
                         claim was compared"
                    );
                    ExitCode::from(2)
                }
                None => {
                    outln!(
                        "checked   {} checkpoints against the entries each describes",
                        agreement.matched
                    );
                    outln!();
                    outln!(
                        "verdict   AGREES: what the engine believed matches what the file replays to"
                    );
                    ExitCode::SUCCESS
                }
            }
        }
    }
}

fn print_market(market: &MarketAt) {
    match (&market.book, &market.no_book_reason) {
        (Some(book), _) => {
            let touch = match (&book.bid, &book.ask) {
                (Some(bid), Some(ask)) => format!(
                    "bid {} x {}   ask {} x {}",
                    bid.px, bid.qty, ask.px, ask.qty
                ),
                (Some(bid), None) => format!("bid {} x {}   ask none", bid.px, bid.qty),
                (None, Some(ask)) => format!("bid none   ask {} x {}", ask.px, ask.qty),
                (None, None) => "the book is live but empty on both sides".to_owned(),
            };
            outln!("market    {touch}");
            if let (Some(mid), Some(spread)) = (book.mid(), book.spread()) {
                outln!("          mid {mid}, spread {spread}");
            }
            outln!(
                "          {} bid levels, {} ask levels",
                book.bid_levels,
                book.ask_levels
            );
        }
        // The refusal. Never a stale book, and never a blank.
        (None, Some(reason)) => outln!("market    no book: {reason}"),
        (None, None) => outln!("market    no book, and no reason recorded -- this is a bug"),
    }

    if let Some(trade) = &market.last_trade {
        outln!(
            "          last trade {} x {} ({:?}), {}",
            trade.px,
            trade.qty,
            trade.aggressor,
            ago(market.at, trade.at)
        );
    } else {
        outln!("          no trade at or before this instant");
    }

    if let Some(gap) = &market.gap_before {
        outln!(
            "blind     last gap {:?} at {} ({})",
            gap.cause,
            gap.at.to_rfc3339(),
            ago(market.at, gap.at)
        );
    } else {
        outln!("blind     no gap before this instant in this day's capture");
    }
    if let Some(gap) = &market.gap_after {
        outln!(
            "          next gap {:?} at {} ({} later)",
            gap.cause,
            gap.at.to_rfc3339(),
            duration(gap.at.as_nanos() - market.at.as_nanos())
        );
    }
}

fn print_ours(
    ours: &OurState,
    instrument: quant_core::instrument::InstrumentId,
    market: &MarketAt,
) {
    if let Some(reason) = &ours.outside_session {
        outln!("ours      {reason}");
    }
    let Some(portfolio) = &ours.portfolio else {
        return;
    };

    let position = portfolio.position(instrument);
    if position.is_flat() {
        outln!("ours      flat");
    } else {
        outln!("ours      {} @ average {}", position.qty, position.avg_px);
    }
    outln!(
        "          cash {}, realized {}, fees {}, {} fills",
        portfolio.cash(),
        portfolio.realized(),
        portfolio.fees(),
        portfolio.fills()
    );

    // Equity is `None` across a gap, and that is three earlier decisions
    // composing rather than a rule written here: a gap clears the book, a
    // cleared book has no mid, and `Portfolio::equity` refuses to value a
    // position it cannot mark.
    let mark = market.book.as_ref().and_then(quant_explain::BookAt::mid);
    if let Some(equity) = portfolio.equity(instrument, mark) {
        outln!("          equity {equity}");
    } else {
        outln!("          equity unknown -- a position is open and there is no mark");
    }

    if let Some(check) = &ours.checkpoint_here {
        outln!(
            "          the engine checkpointed here claiming cash {}, {} fills",
            check.cash,
            check.fills
        );
    }
    if let Some(fill) = &ours.last_fill {
        outln!(
            "          last fill {:?} {} x {} fee {}, {} (fill #{})",
            fill.side,
            fill.px,
            fill.qty,
            fill.fee,
            ago(market.at, fill.at),
            fill.fill_ordinal
        );
    } else {
        outln!("          no fill at or before this instant");
    }
    if let Some(fill) = &ours.next_fill {
        outln!(
            "          next fill {:?} {} x {}, {} later",
            fill.side,
            fill.px,
            fill.qty,
            duration(fill.at.as_nanos() - market.at.as_nanos())
        );
    }
}

/// The operational picture for the containing minute, with time bases.
///
/// The labels are the point. One line mixes instantaneous, lifetime and
/// 60-second-window figures with nothing marking which is which, and
/// `queue_peak=270` beside `queue=0` is how an operator concludes the wrong
/// thing at 3am.
/// What happened across the span, when one was asked for.
fn print_window(market: &MarketAt) {
    let Some(window) = &market.window else {
        return;
    };
    outln!(
        "window    the {} to {}: {} trades, {} deltas, {} snapshots",
        span(window.span_nanos),
        market.at.to_rfc3339(),
        window.trades,
        window.deltas,
        window.snapshots
    );
    if let (Some(low), Some(high)) = (window.low, window.high) {
        outln!("          traded {low} to {high}, {} volume", window.volume);
    } else {
        // A fact, not a zero: nothing traded is different from a price of zero.
        outln!("          nothing traded in this span");
    }
    if window.gaps.is_empty() {
        outln!("          no gaps -- we could see the whole span");
    } else {
        outln!("          {} gaps inside the span:", window.gaps.len());
        for gap in &window.gaps {
            outln!("            {:?} at {}", gap.cause, gap.at.to_rfc3339());
        }
    }
}

/// A nanosecond span in the units it was probably asked in.
fn span(nanos: i64) -> String {
    let secs = nanos / 1_000_000_000;
    if secs % 3_600 == 0 {
        format!("{}h", secs / 3_600)
    } else if secs % 60 == 0 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

fn print_health(args: &Args) {
    match quant_explain::health_at(&args.root, &args.symbol, args.at) {
        Err(why) => outln!("health    {why}"),
        Ok(health) => {
            outln!(
                "health    queue {} now, {} at its worst since start, of {} capacity",
                health.queue,
                health.queue_peak,
                health.queue_capacity
            );
            outln!(
                "          in the 60s to {}: {} msg/s, {} dropped, latency p50 {}ms p99 {}ms max {}ms",
                health.emitted_at.to_rfc3339(),
                health.msgs_per_sec,
                health.dropped,
                health.latency_p50_ms,
                health.latency_p99_ms,
                health.latency_max_ms
            );
            let gaps = health.gap_disconnect + health.gap_overflow + health.gap_sequence;
            if gaps > 0 {
                outln!(
                    "          and {} gaps that minute ({} disconnect, {} overflow, {} sequence)",
                    gaps,
                    health.gap_disconnect,
                    health.gap_overflow,
                    health.gap_sequence
                );
            }
            if health.clock_skew_ms.abs() > 1_000 {
                outln!(
                    "          clock skew {}ms -- past Binance's own tolerance for a signed request",
                    health.clock_skew_ms
                );
            }
        }
    }
}

fn print_provenance(args: &Args, market: &MarketAt) {
    outln!(
        "read      session {}",
        quant_recorder::format_session_id(&market.session_id)
    );
    for path in &market.segments {
        outln!("          {}", path.display());
    }
    outln!(
        "          {} events replayed to reach the instant",
        market.events_read
    );
    let _ = args;
}

/// How long before the instant something happened, in words.
fn ago(at: Ts, then: Ts) -> String {
    format!("{} before", duration(at.as_nanos() - then.as_nanos()))
}

/// A nanosecond span, rendered at whatever scale reads best.
fn duration(nanos: i64) -> String {
    let millis = nanos / 1_000_000;
    if millis.abs() < 1_000 {
        return format!("{millis}ms");
    }
    let secs = millis / 1_000;
    if secs.abs() < 60 {
        return format!("{}.{:03}s", secs, (millis % 1_000).abs());
    }
    if secs.abs() < 3_600 {
        return format!("{}m{:02}s", secs / 60, (secs % 60).abs());
    }
    format!("{}h{:02}m", secs / 3_600, ((secs % 3_600) / 60).abs())
}

struct Args {
    root: PathBuf,
    symbol: String,
    at: Ts,
    /// Exactly what was typed, so the report can echo it beside the UTC form.
    as_typed: String,
    journal: Option<PathBuf>,
    check_journal: Option<PathBuf>,
    /// Zero means "just the instant", which is the window of length zero and
    /// takes the same code path.
    window_nanos: i64,
}

fn parse_args() -> Result<Option<Args>, String> {
    let mut root = PathBuf::from("data");
    let mut symbol = "BTCUSDT".to_owned();
    let mut at = None;
    let mut as_typed = String::new();
    let mut journal = None;
    let mut check_journal = None;
    let mut window_nanos = 0_i64;

    let mut remaining = std::env::args().skip(1);
    while let Some(arg) = remaining.next() {
        let mut value = || {
            remaining
                .next()
                .ok_or_else(|| format!("{arg} needs a value"))
        };
        match arg.as_str() {
            "-h" | "--help" => {
                outln!(
                    "usage: explain [DATA_ROOT] --at <RFC3339> [--symbol SYM] [--journal PATH]\n\
                     \x20      [--window 5m]  summarise the span ending at --at\n\
                     \x20      explain --check-journal PATH\n\
                     \x20      --at needs a UTC offset: `...Z` or `...+05:30`"
                );
                return Ok(None);
            }
            "--at" => {
                let text = value()?;
                at = Some(Ts::parse_rfc3339(&text)?);
                as_typed = text;
            }
            "--symbol" => symbol = value()?,
            "--journal" => journal = Some(PathBuf::from(value()?)),
            "--check-journal" => check_journal = Some(PathBuf::from(value()?)),
            "--window" => window_nanos = parse_span(&value()?)?,
            other if other.starts_with('-') => return Err(format!("unknown option: {other}")),
            other => root = PathBuf::from(other),
        }
    }
    if let Some(path) = check_journal {
        return Ok(Some(Args {
            root,
            symbol,
            at: Ts::from_nanos(0),
            as_typed: String::new(),
            journal,
            check_journal: Some(path),
            window_nanos: 0,
        }));
    }
    let at = at.ok_or_else(|| {
        "--at is required; this tool answers about a moment and there is no \
         sensible default for which one"
            .to_owned()
    })?;
    Ok(Some(Args {
        root,
        symbol,
        at,
        as_typed,
        journal,
        check_journal: None,
        window_nanos,
    }))
}

/// A span like `5m`, `90s` or `2h`, in nanoseconds.
///
/// The same unit-suffixed shape `ops/verify-loop.sh` takes, for the same reason:
/// an unsuffixed number would need a documented default that every reader has to
/// remember. Here there is no sensible default at all -- a bare `5` could be
/// seconds or minutes and the difference is a factor of sixty -- so the suffix is
/// required rather than merely allowed.
fn parse_span(text: &str) -> Result<i64, String> {
    let (digits, scale) = match text.as_bytes().last() {
        Some(b's') => (&text[..text.len() - 1], 1_000_000_000_i64),
        Some(b'm') => (&text[..text.len() - 1], 60 * 1_000_000_000),
        Some(b'h') => (&text[..text.len() - 1], 3_600 * 1_000_000_000),
        _ => {
            return Err(format!(
                "--window {text:?} needs a unit: `30s`, `5m` or `2h`. A bare number \
                 would need a default nobody would remember, and seconds versus \
                 minutes is a factor of sixty"
            ))
        }
    };
    let magnitude: i64 = digits
        .parse()
        .map_err(|_| format!("--window {text:?} is not a number and a unit"))?;
    if magnitude <= 0 {
        return Err("--window must be positive; a span ends at --at and runs backwards".to_owned());
    }
    magnitude
        .checked_mul(scale)
        .ok_or_else(|| format!("--window {text:?} is longer than time"))
}
