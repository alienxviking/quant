//! Replay every capture session under a data root and report the reconstruction.
//!
//! ```text
//! normalize [DATA_ROOT] [--symbol SYM]
//! normalize data/acceptance
//! normalize data/acceptance --symbol BTCUSDT
//! ```
//!
//! Exit code 0 means every session reconstructed a book that held its invariants
//! at every tick, which is `docs/data-contract.md` §8's second criterion.
//!
//! # What this does *not* answer
//!
//! Whether the capture is complete. That is `quant-verify`'s question, and the
//! two are kept apart on purpose: the verifier asks whether every discontinuity
//! is explained by a record in the capture, and this asks what the market did
//! given whatever is there. A capture with an honest, recorded gap is *complete*
//! and still produces a book that goes dark for a while, and a tool that
//! conflated those would have to call one of them a failure.
//!
//! Two independent checks agreeing is worth more than one tool asserting both.

use std::path::PathBuf;
use std::process::ExitCode;

use quant_core::instrument::{InstrumentDef, InstrumentKind, InstrumentRegistry};
use quant_normalize::{replay_session, SessionSummary};
use quant_recorder::{catalog, format_session_id, SessionFiles};

fn main() -> ExitCode {
    let mut root = None;
    let mut symbol: Option<String> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("usage: normalize [DATA_ROOT] [--symbol SYM]");
                return ExitCode::SUCCESS;
            }
            "--symbol" => {
                let Some(s) = args.next() else {
                    eprintln!("--symbol needs a value");
                    return ExitCode::FAILURE;
                };
                symbol = Some(s);
            }
            other if other.starts_with('-') => {
                eprintln!("unknown option: {other}");
                return ExitCode::FAILURE;
            }
            other => root = Some(PathBuf::from(other)),
        }
    }
    let root = root.unwrap_or_else(|| PathBuf::from("data"));

    let found = catalog(&root);
    let sessions: Vec<SessionFiles> = found
        .sessions
        .into_iter()
        .filter(|s| symbol.as_ref().map_or(true, |want| &s.symbol == want))
        .collect();

    println!("root      {}", root.display());
    if !found.strays.is_empty() {
        // Named but not judged: `quant-verify` is what decides whether a stray
        // file under raw/ matters. Silence here would be worse, because a
        // capture file this tool did not recognise is a capture file it did not
        // replay.
        println!(
            "strays    {} file(s) under raw/ that are not capture files (run verify)",
            found.strays.len()
        );
    }

    let mut registry = InstrumentRegistry::new();
    let mut failures = 0;
    for files in &sessions {
        let instrument = registry.register(InstrumentDef {
            exchange: files.exchange,
            symbol: files.symbol.clone(),
            // A capture records the venue's identity for an instrument, not its
            // trading filters. Identity is all a replay needs; M8 is where the
            // filters matter, and they come from the venue then.
            base: String::new(),
            quote: String::new(),
            kind: InstrumentKind::Spot,
            tick_size: "0.00000001".parse().expect("a valid literal"),
            lot_size: "0.00000001".parse().expect("a valid literal"),
            min_notional: "0".parse().expect("a valid literal"),
        });
        let summary = replay_session(files, instrument);
        if !summary.is_clean() {
            failures += 1;
        }
        print_session(files, &summary);
    }

    println!();
    println!(
        "verdict   {}",
        if failures > 0 {
            "RECONSTRUCTION FAILED: at least one session did not hold its invariants"
        } else if sessions.is_empty() {
            "NOTHING REPLAYED: no capture sessions found under this root"
        } else {
            "every session reconstructed a book that held at every tick"
        }
    );

    if failures > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn print_session(files: &SessionFiles, s: &SessionSummary) {
    println!();
    println!(
        "session   {} {} {}",
        files.exchange,
        files.symbol,
        format_session_id(&files.session_id)
    );
    println!(
        "segments  {} of {} joined, {} .. {}",
        s.replay.segments_read,
        files.segments.len(),
        files.segments.first().map_or_else(String::new, day),
        files.segments.last().map_or_else(String::new, day),
    );
    println!(
        "frames    {} read, {} events ({} trades, {} deltas, {} snapshots, {} gaps, {} ignored)",
        s.replay.frames,
        s.replay.events,
        s.replay.trades,
        s.replay.deltas,
        s.replay.snapshots,
        s.replay.gaps,
        s.replay.ignored
    );
    println!(
        "applied   {} deltas, {} snapshots, {} buffered then replayed",
        s.book.applied, s.book.snapshots, s.book.buffered
    );
    println!(
        "discarded {} superseded by a snapshot, {} dropped for want of an anchor, {} stale snapshots",
        s.book.stale, s.book.unanchored, s.book.stale_snapshots
    );
    println!(
        "chain     {} breaks, {} invalidations, {} archive breaks",
        s.book.broken, s.book.invalidations, s.replay.breaks
    );
    if let Some(brk) = &s.first_break {
        println!("  first   {brk}");
    }
    println!("depth     {} levels at the deepest", s.book.max_depth);
    println!(
        "checked   {} ticks with a live book, {} with none, {} full audits",
        s.checked_live, s.checked_dark, s.audits
    );
    if s.violations == 0 {
        println!("holds     book invariants held at every tick");
    } else {
        println!(
            "holds     {} VIOLATIONS. first: {}",
            s.violations,
            s.first_violation.as_deref().unwrap_or("(none recorded)")
        );
    }
}

fn day(entry: &quant_recorder::CatalogEntry) -> String {
    entry.target.date.to_string()
}
