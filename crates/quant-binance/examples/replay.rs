//! Replay a capture file through a reconstructed book and check the invariants.
//!
//! This is the question M2 exists to answer, in its smallest useful form: does
//! applying the recorded messages, in order, produce a sane book at every tick?
//!
//! ```text
//! cargo run --release -p quant-binance --example replay -- <path to part-*.bin.zst>
//! ```
//!
//! One file, so it does not join a session's segments across midnight — a
//! `quant-normalize` crate does that properly at M2.c, along with the Parquet
//! output. What this proves is the pair that matters most and is easiest to get
//! wrong: the parser and the resync rule, against real venue bytes rather than
//! against fixtures somebody wrote from the documentation.
//!
//! # What "every tick" costs
//!
//! `Book::check` is the `O(1)` crossed-book test and runs on every event, which is
//! the criterion. `Book::audit` scans both sides and runs at each snapshot and at
//! the end — running it per tick would be tens of billions of comparisons for
//! properties that hold by construction.

use std::fs::File;
use std::io::BufReader;
use std::process::ExitCode;

use quant_book::{Book, Outcome};
use quant_core::event::MarketEvent;
use quant_core::instrument::{InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry};
use quant_storage::{FrameKind, RawFrame, RawReader};

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: replay <path to part-NNNNN.bin.zst>");
        return ExitCode::FAILURE;
    };
    match run(&path) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Everything that went wrong, or did not.
#[derive(Default)]
struct Report {
    events: u64,
    parse_failures: u64,
    /// Invariant violations, with where the first one happened.
    violations: u64,
    first_violation: Option<(u64, String)>,
    /// Ticks on which the book was live and uncrossed. The number the criterion
    /// is really about.
    checked_live: u64,
    /// Ticks on which there was no book to check, because a gap or a chain break
    /// had invalidated it and no snapshot had arrived yet.
    checked_dark: u64,
}

fn run(path: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let mut reader = RawReader::open(BufReader::new(File::open(path)?))?;
    let header = reader.header().clone();

    let mut registry = InstrumentRegistry::new();
    let instrument = registry.register(InstrumentDef {
        exchange: header.exchange,
        symbol: header.symbol.clone(),
        base: String::new(),
        quote: String::new(),
        kind: InstrumentKind::Spot,
        tick_size: "0.01".parse()?,
        lot_size: "0.00001".parse()?,
        min_notional: "5".parse()?,
    });

    let mut book = Book::new();
    let mut report = Report::default();
    let mut audits = 0_u64;

    while let Some(frame) = reader.next_frame() {
        let frame = frame?;
        let Some(event) = to_event(&frame, instrument, &mut report) else {
            continue;
        };
        report.events += 1;

        let was_snapshot = matches!(event, MarketEvent::BookSnapshot(_));
        let outcome = book.apply(&event);
        if let Outcome::Broken { expected, found } = outcome {
            note(
                &mut report,
                frame.ingest_seq,
                format!("chain break: expected U={expected}, found {found}"),
            );
        }

        // The criterion, on every single event.
        match book.check() {
            Ok(()) => {
                if book.is_live() {
                    report.checked_live += 1;
                } else {
                    report.checked_dark += 1;
                }
            }
            Err(v) => note(&mut report, frame.ingest_seq, v.to_string()),
        }

        // The O(n) invariants, where they are affordable.
        if was_snapshot {
            audits += 1;
            if let Err(v) = book.audit() {
                note(&mut report, frame.ingest_seq, format!("audit: {v}"));
            }
        }
    }

    audits += 1;
    if let Err(v) = book.audit() {
        note(&mut report, 0, format!("final audit: {v}"));
    }

    print_report(path, &header, &book, &report, audits);
    Ok(report.violations == 0 && report.parse_failures == 0)
}

/// What the replay saw. Split out of `run` because summarising and replaying are
/// two jobs, and one of them was pushing the other past the line limit.
fn print_report(
    path: &str,
    header: &quant_storage::FileHeader,
    book: &Book,
    report: &Report,
    audits: u64,
) {
    let stats = book.stats();
    println!("file      {path}");
    println!("header    {header}");
    println!("events    {} parsed into the book", report.events);
    println!(
        "applied   {} deltas, {} snapshots",
        stats.applied, stats.snapshots
    );
    println!(
        "buffered  {} deltas held for an anchor and then replayed",
        stats.buffered
    );
    println!(
        "discarded {} stale (superseded by a snapshot), {} dropped (buffer full), {} stale snapshots",
        stats.stale, stats.unanchored, stats.stale_snapshots
    );
    println!(
        "chain     {} breaks, {} invalidations",
        stats.broken, stats.invalidations
    );
    println!("depth     {} levels at the deepest", stats.max_depth);
    if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) {
        println!(
            "touch     {} x {}  /  {} x {}",
            bid.px, bid.qty, ask.px, ask.qty
        );
    }
    println!(
        "checked   {} ticks with a live book, {} with none",
        report.checked_live, report.checked_dark
    );
    println!("audits    {audits} full scans");
    if report.parse_failures > 0 {
        println!("parse     {} failures", report.parse_failures);
    }

    if report.violations == 0 && report.parse_failures == 0 {
        println!("verdict   book invariants held at every tick");
    } else {
        println!(
            "verdict   {} VIOLATIONS. first: {}",
            report.violations,
            report
                .first_violation
                .as_ref()
                .map_or("(none recorded)", |(_, m)| m.as_str())
        );
    }
}

fn note(report: &mut Report, seq: u64, message: String) {
    report.violations += 1;
    if report.first_violation.is_none() {
        // Only the first is kept in full. A systematic defect over four million
        // frames would otherwise print four million lines and bury itself, which
        // is the same reason `quant-verify` caps its findings.
        eprintln!("violation at ingest_seq {seq}: {message}");
        report.first_violation = Some((seq, message));
    }
}

fn to_event(
    frame: &RawFrame,
    instrument: InstrumentId,
    report: &mut Report,
) -> Option<MarketEvent> {
    match frame.kind {
        FrameKind::VenuePayload => match quant_binance::parse_stream_message(
            &frame.payload,
            instrument,
            frame.local_recv_ts,
            frame.ingest_seq,
        ) {
            Ok(event) => event,
            Err(e) => {
                if report.parse_failures == 0 {
                    eprintln!("parse failure at ingest_seq {}: {e}", frame.ingest_seq);
                }
                report.parse_failures += 1;
                None
            }
        },
        FrameKind::VenueSnapshot => match quant_binance::parse_snapshot(
            &frame.payload,
            instrument,
            frame.local_recv_ts,
            frame.ingest_seq,
        ) {
            Ok(s) => Some(MarketEvent::BookSnapshot(s)),
            Err(e) => {
                if report.parse_failures == 0 {
                    eprintln!(
                        "snapshot parse failure at ingest_seq {}: {e}",
                        frame.ingest_seq
                    );
                }
                report.parse_failures += 1;
                None
            }
        },
        // Gaps must reach the book: that is what invalidates it. A replay that
        // skipped control frames would reconstruct straight through a period of
        // blindness and produce a book that looks continuous and is not.
        FrameKind::Control => {
            frame.control().ok().flatten().and_then(|c| {
                c.into_market_event(instrument, frame.local_recv_ts, frame.ingest_seq)
            })
        }
    }
}
