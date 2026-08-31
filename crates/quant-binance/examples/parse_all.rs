//! Parse every frame in a capture file, and report what refused to parse.
//!
//! The parser's unit tests use a handful of payloads copied out of a real
//! capture. That proves it handles the shapes we thought to look at. This proves
//! it handles the ones we did not -- which over seventy million frames is the
//! more interesting question, and the only way to find out whether the venue's
//! dialect has corners we have not met.
//!
//! ```text
//! cargo run --release -p quant-binance --example parse_all -- <path to part-*.bin.zst>
//! ```

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::process::ExitCode;

use quant_core::event::MarketEvent;
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
use quant_storage::{FrameKind, RawReader};

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: parse_all <path to part-NNNNN.bin.zst>");
        return ExitCode::FAILURE;
    };
    match run(&path) {
        Ok(clean) => {
            if clean {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
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
    let _ = Exchange::Binance;

    let mut trades = 0_u64;
    let mut deltas = 0_u64;
    let mut snapshots = 0_u64;
    let mut ignored = 0_u64;
    let mut levels = 0_u64;
    // Grouped rather than listed: one systematic defect over millions of frames
    // should read as one line with a count, not as millions of lines.
    let mut failures: BTreeMap<String, (u64, u64)> = BTreeMap::new();

    while let Some(frame) = reader.next_frame() {
        let frame = frame?;
        let mut note = |e: String, seq: u64| {
            let entry = failures.entry(e).or_insert((0, seq));
            entry.0 += 1;
        };

        match frame.kind {
            FrameKind::VenuePayload => {
                match quant_binance::parse_stream_message(
                    &frame.payload,
                    instrument,
                    frame.local_recv_ts,
                    frame.ingest_seq,
                ) {
                    Ok(Some(MarketEvent::Trade(_))) => trades += 1,
                    Ok(Some(MarketEvent::BookDelta(d))) => {
                        deltas += 1;
                        levels += (d.bids.len() + d.asks.len()) as u64;
                    }
                    Ok(Some(_) | None) => ignored += 1,
                    Err(e) => note(e.to_string(), frame.ingest_seq),
                }
            }
            FrameKind::VenueSnapshot => {
                match quant_binance::parse_snapshot(
                    &frame.payload,
                    instrument,
                    frame.local_recv_ts,
                    frame.ingest_seq,
                ) {
                    Ok(s) => {
                        snapshots += 1;
                        levels += (s.bids.len() + s.asks.len()) as u64;
                    }
                    Err(e) => note(e.to_string(), frame.ingest_seq),
                }
            }
            FrameKind::Control => {}
        }
    }

    println!("file      {path}");
    println!("header    {header}");
    println!("parsed    {trades} trades, {deltas} deltas, {snapshots} snapshots");
    println!("levels    {levels} price levels through fixed-point");
    println!("ignored   {ignored} (event types this build does not interpret)");
    if failures.is_empty() {
        println!("failures  none");
    } else {
        println!("failures  {} distinct", failures.len());
        for (message, (count, first_seq)) in &failures {
            println!("  {count} x  (first at seq {first_seq})  {message}");
        }
    }
    Ok(failures.is_empty())
}
