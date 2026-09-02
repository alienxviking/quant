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
        eprintln!("usage: dump <path to part-NNNNN.bin.zst> [--sample N]");
        return ExitCode::FAILURE;
    };

    // How many payloads of each frame kind to print in full.
    //
    // "What do the bytes actually look like" is the question a normalizer gets
    // written against, and answering it from the venue's documentation rather
    // than from a recorded file is how a parser ends up correct about a dialect
    // nobody is speaking.
    let sample = std::env::args()
        .position(|a| a == "--sample")
        .and_then(|i| std::env::args().nth(i + 1))
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);

    match dump(&path, sample) {
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

fn dump(path: &str, sample: usize) -> Result<bool, Box<dyn std::error::Error>> {
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

    let scan = scan(&mut reader, sample)?;

    let stats = reader.stats();
    println!(
        "frames    {} ({} stream, {} snapshot, {} control)",
        stats.frames,
        scan.venue,
        scan.snapshots,
        scan.control.len()
    );
    println!("blocks    {}", stats.blocks);
    println!("bytes     {} consumed", stats.bytes_consumed);
    println!("snapshot  {} bytes of book anchors", scan.snapshot_bytes);
    println!(
        "ingest    {}..={}",
        scan.first_seq.unwrap_or(0),
        scan.last_seq.unwrap_or(0)
    );
    println!(
        "holes     {} ({} messages unaccounted by sequence)",
        scan.holes, scan.missing
    );

    for (seq, head) in &scan.snapshot_seqs {
        println!("anchor    seq {seq}: {head}...");
    }
    for (seq, what) in &scan.control {
        println!("control   seq {seq}: {what}");
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
    let explained = scan.missing == 0 || scan.gaps > 0;
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
            .is_none_or(|t| !t.reason.is_corruption()))
}

/// What one pass over the frames saw.
#[derive(Default)]
struct Scan {
    first_seq: Option<u64>,
    last_seq: Option<u64>,
    /// Discontinuities in `ingest_seq`, and how many messages they account for.
    holes: u64,
    missing: u64,
    venue: u64,
    snapshots: u64,
    snapshot_bytes: u64,
    /// Where the book anchors sit. Worth printing: a snapshot's position in the
    /// sequence is what tells the book builder which deltas precede it and are
    /// therefore stale, so "is it in a sensible place" is a real question.
    ///
    /// The prefix comes along because the other real question is whether the
    /// bytes are a book at all. A venue that answers a snapshot request with an
    /// error document returns HTTP 200 often enough that "the request succeeded"
    /// is not the same claim as "this is a book", and sixty characters settles it
    /// by eye.
    snapshot_seqs: Vec<(u64, String)>,
    /// Gap records only, counted separately: they are what *explains* a hole,
    /// which a snapshot-failure record does not.
    gaps: u64,
    control: Vec<(u64, String)>,
}

fn scan<R: std::io::Read>(
    reader: &mut RawReader<R>,
    sample: usize,
) -> Result<Scan, Box<dyn std::error::Error>> {
    let mut scan = Scan::default();
    let mut sampled = [0_usize; 3];

    while let Some(frame) = reader.next_frame() {
        let frame = frame?;
        if let Some(previous) = scan.last_seq {
            let skipped = frame.ingest_seq - previous - 1;
            if skipped > 0 {
                scan.holes += 1;
                scan.missing += skipped;
            }
        }
        scan.first_seq.get_or_insert(frame.ingest_seq);
        scan.last_seq = Some(frame.ingest_seq);

        // Printed before the counters so the first of each kind is the one shown.
        let slot = match frame.kind {
            FrameKind::VenuePayload => 0,
            FrameKind::VenueSnapshot => 1,
            FrameKind::Control => 2,
        };
        if sampled[slot] < sample {
            sampled[slot] += 1;
            let text = String::from_utf8_lossy(&frame.payload);
            println!(
                "sample    seq {} {:?} ({} bytes)
          {}",
                frame.ingest_seq,
                frame.kind,
                frame.payload.len(),
                // Snapshots run to hundreds of kilobytes; the shape is in the head.
                text.chars().take(400).collect::<String>()
            );
        }

        match frame.kind {
            FrameKind::VenuePayload => scan.venue += 1,
            FrameKind::VenueSnapshot => {
                scan.snapshots += 1;
                scan.snapshot_bytes += frame.payload.len() as u64;
                let head = String::from_utf8_lossy(&frame.payload)
                    .chars()
                    .take(60)
                    .collect();
                scan.snapshot_seqs.push((frame.ingest_seq, head));
            }
            FrameKind::Control => match frame.control()? {
                Some(ControlRecord::Gap { cause, .. }) => {
                    scan.gaps += 1;
                    scan.control
                        .push((frame.ingest_seq, format!("gap {cause:?}")));
                }
                Some(ControlRecord::SnapshotFailed {
                    purpose,
                    reason,
                    attempts,
                }) => scan.control.push((
                    frame.ingest_seq,
                    format!("snapshot failed: {purpose:?}/{reason:?} after {attempts} attempts"),
                )),
                None => {}
            },
        }
    }

    Ok(scan)
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
