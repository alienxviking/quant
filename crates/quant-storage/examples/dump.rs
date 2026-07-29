//! Summarise one raw capture file.
//!
//! ```text
//! cargo run -p quant-storage --example dump -- <path>
//! ```
//!
//! A debugging aid, deliberately not the M1.d verifier. That one walks a whole
//! directory tree, cross-checks venue update-id chains, and reconciles sessions
//! against the metadata database. This answers the much smaller question "what is
//! actually in this file", which is what you want at the moment a capture has
//! just been written and you do not yet trust anything.

use std::fs::File;
use std::io::BufReader;
use std::process::ExitCode;

use quant_storage::{ControlRecord, FrameKind, RawReader};

fn main() -> ExitCode {
    let Some(path) = std::env::args().nth(1) else {
        eprintln!("usage: dump <path to part-NNNNN.bin.zst>");
        return ExitCode::FAILURE;
    };

    match dump(&path) {
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

fn dump(path: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    // Buffered because the reader issues one read per block header and one per
    // block body; unbuffered that is two syscalls per 256 KiB, which is fine, but
    // buffering costs nothing and this is also the pattern the verifier will use.
    let mut reader = RawReader::open(BufReader::new(file))?;
    println!("file      {path}");
    println!("header    {}", reader.header());
    println!(
        "session   {}",
        quant_recorder_session_text(&reader.header().session_id)
    );

    let mut first_seq = None;
    let mut last_seq = None;
    let mut holes = 0_u64;
    let mut missing = 0_u64;
    let mut venue = 0_u64;
    let mut gaps: Vec<(u64, String)> = Vec::new();

    while let Some(frame) = reader.next_frame() {
        let frame = frame?;
        if let Some(previous) = last_seq {
            let skipped = frame.ingest_seq - previous - 1;
            if skipped > 0 {
                holes += 1;
                missing += skipped;
            }
        }
        first_seq.get_or_insert(frame.ingest_seq);
        last_seq = Some(frame.ingest_seq);

        match frame.kind {
            FrameKind::VenuePayload => venue += 1,
            FrameKind::Control => {
                if let Some(ControlRecord::Gap { cause, .. }) = frame.control()? {
                    gaps.push((frame.ingest_seq, format!("{cause:?}")));
                }
            }
        }
    }

    let stats = reader.stats();
    println!(
        "frames    {} ({venue} venue, {} control)",
        stats.frames,
        gaps.len()
    );
    println!("blocks    {}", stats.blocks);
    println!("bytes     {} consumed", stats.bytes_consumed);
    println!(
        "ingest    {}..={}",
        first_seq.unwrap_or(0),
        last_seq.unwrap_or(0)
    );
    println!("holes     {holes} ({missing} messages unaccounted by sequence)");

    for (seq, cause) in &gaps {
        println!("gap       seq {seq}: {cause}");
    }

    match reader.truncation() {
        None => println!("tail      intact"),
        Some(t) => println!(
            "tail      {t}{}",
            if t.reason.is_corruption() {
                "  <-- CORRUPTION, not a torn tail"
            } else {
                ""
            }
        ),
    }

    // The two-sided completeness check: every dropped message must be explained
    // by a recorded gap, and the writer must have declared the file closed.
    let explained = missing == 0 || !gaps.is_empty();
    match reader.trailer() {
        Some(t) => println!(
            "trailer   present: {} frames, {} blocks, last seq {:?}",
            t.frames, t.blocks, t.last_ingest_seq
        ),
        None => {
            println!("trailer   ABSENT: writer never closed this file (killed, or still running)");
        }
    }
    println!(
        "verdict   {}",
        if explained {
            "sequence holes are explained by recorded gaps"
        } else {
            "UNEXPLAINED HOLES: messages missing with no gap record"
        }
    );

    // `map_or`, not `is_none_or`: the latter is stable only from 1.82 and the
    // workspace declares an MSRV of 1.75.
    Ok(explained
        && reader
            .truncation()
            .map_or(true, |t| !t.reason.is_corruption()))
}

/// Canonical UUID text. Duplicated from `quant-recorder::layout` rather than
/// depending on it, because `quant-storage` sits *below* the recorder and an
/// example must not invert that.
fn quant_recorder_session_text(id: &[u8; 16]) -> String {
    use core::fmt::Write as _;
    let mut out = String::with_capacity(36);
    for (i, byte) in id.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
    }
    out
}
