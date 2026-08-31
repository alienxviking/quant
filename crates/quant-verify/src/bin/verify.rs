//! Verify a capture tree.
//!
//! ```text
//! verify [DATA_ROOT] [--reconcile]
//! verify data                  # every session under ./data
//! verify data --reconcile      # also cross-check the metadata index
//! ```
//!
//! Exit code 0 means every session is trustworthy for what it claims to cover;
//! non-zero means at least one error finding. That is what makes this runnable from
//! cron during the seven-day acceptance run, and from CI, rather than something a
//! person reads and forms an impression about.
//!
//! Warnings never fail the run. If they did, the first torn tail on a file still
//! being written would teach everyone to ignore the exit code, and then the errors
//! would be ignored too.

use std::path::PathBuf;
use std::process::ExitCode;

use quant_meta::store::DATABASE_URL_ENV;
use quant_verify::{discover, reconcile, verify, Report, Verified};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let mut root = None;
    let mut want_reconcile = false;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--reconcile" => want_reconcile = true,
            "-h" | "--help" => {
                println!("usage: verify [DATA_ROOT] [--reconcile]");
                return ExitCode::SUCCESS;
            }
            other if other.starts_with('-') => {
                eprintln!("unknown option: {other}");
                return ExitCode::FAILURE;
            }
            other => root = Some(PathBuf::from(other)),
        }
    }
    let root = root.unwrap_or_else(|| PathBuf::from("data"));

    let mut report = Report::default();
    let sessions = discover(&root, &mut report);

    let verified: Vec<Verified<'_>> = sessions
        .iter()
        .map(|session| Verified {
            session,
            segments: verify(session, &mut report),
        })
        .collect();

    if want_reconcile {
        let Ok(url) = std::env::var(DATABASE_URL_ENV) else {
            eprintln!("--reconcile needs {DATABASE_URL_ENV} to be set");
            return ExitCode::from(2);
        };
        if let Err(e) = reconcile(&url, &verified, &mut report).await {
            // A database we cannot reach is not evidence about the capture, so it
            // must not silently turn a clean run into a failed one. But it does
            // mean the check did not happen, and a check that quietly did not
            // happen is the worst outcome available here -- hence its own exit
            // code, distinct from both "clean" and "the capture is bad".
            eprintln!("--reconcile could not run: {e}");
            return ExitCode::from(2);
        }
    }

    print_report(&report, &root, want_reconcile);

    if report.is_clean() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn print_report(report: &Report, root: &std::path::Path, reconciled: bool) {
    let t = report.totals;
    println!("root      {}", root.display());
    println!("sessions  {} in {} segments", t.sessions, t.segments);
    println!(
        "frames    {} ({} stream, {} snapshot, {} gap)",
        t.frames, t.stream_messages, t.snapshots, t.gaps
    );
    println!(
        "streams   {} depth deltas, {} trades",
        t.depth_deltas, t.trades
    );
    println!(
        "anchoring {} deltas arrived before their episode's snapshot (superseded \
         by it, or replayed after it)",
        t.deltas_before_anchor
    );
    println!(
        "missing   {} messages accounted for by sequence holes",
        t.missing_messages
    );
    println!(
        "index     {}",
        if reconciled {
            "reconciled"
        } else {
            "not checked (pass --reconcile)"
        }
    );

    if report.findings().is_empty() {
        println!("findings  none");
    } else {
        println!(
            "findings  {} errors, {} warnings",
            report.errors(),
            report.warnings()
        );
        for finding in report.findings() {
            println!("  {finding}");
        }
        for (code, suppressed) in report.suppressed() {
            // Never dropped: this number is what says a defect is systematic
            // rather than a one-off, which is the most useful thing here.
            println!("  ... and {suppressed} more {code}");
        }
    }

    // Errors first: "nothing verified" is a guard against a *false clean bill of
    // health*, so it must not displace the reason a run actually failed. A capture
    // corrupt in its first block reads zero frames, and saying "no files found"
    // about it would send whoever is on call looking in the wrong place.
    println!(
        "verdict   {}",
        if !report.is_clean() {
            "UNEXPLAINED FINDINGS: this capture cannot be trusted as complete"
        } else if t.frames == 0 {
            "NOTHING VERIFIED: no capture frames found under this root"
        } else {
            "every discontinuity is explained by a record in the capture"
        }
    );
}
