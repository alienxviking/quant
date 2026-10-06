//! Reading a run log: is it complete, and do two of them agree?
//!
//! Two subcommands answering two questions that do not imply each other. A
//! complete record of a run that diverged is still complete; two files can agree
//! about everything they hold while both omit the same line. Keeping them apart
//! is the same rule `quant-verify` and `quant-normalize` keep.
//!
//! ```text
//! runlog check ~/paper/paper-BTCUSDT.jsonl
//! runlog diff  ~/paper/paper-BTCUSDT.jsonl replay-BTCUSDT.jsonl
//! ```
//!
//! # Exit codes, and why there are three
//!
//! `0` agreement, `1` a real disagreement, **`2` nothing was compared**. The
//! third exists because M5.c shipped a check that could not fail and looked
//! exactly like one that passed: "nobody disagreed" is not "two answers
//! matched". `reconcile` and `explain --check-journal` already draw that
//! distinction and this draws it twice — for an empty comparison, and for a
//! live side that restarted, which forfeits the exact match before any
//! comparison begins.

use std::path::PathBuf;
use std::process::ExitCode;

use quant_explain::runlog::{self, Divergence};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        usage();
        return ExitCode::FAILURE;
    };
    let rest: Vec<PathBuf> = args.map(PathBuf::from).collect();

    match (command.as_str(), rest.as_slice()) {
        ("check", [path]) => check(path),
        ("diff", [live, replay]) => diff(live, replay),
        ("-h" | "--help", _) => {
            usage();
            ExitCode::SUCCESS
        }
        _ => {
            usage();
            ExitCode::FAILURE
        }
    }
}

fn usage() {
    eprintln!(
        "usage: runlog check <JOURNAL>\n\
         \x20      runlog diff <LIVE> <REPLAY>\n\
         \n\
         exit 0 complete / agrees, 1 a real problem, 2 nothing was compared"
    );
}

fn check(path: &std::path::Path) -> ExitCode {
    let recovered = match quant_engine::journal::read(path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("could not read {}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };
    let report = runlog::check(&recovered.entries);
    println!("runlog    {}", path.display());
    println!(
        "entries   {} lines, {} decisions, {} strategy claims",
        report.entries, report.decisions, report.claims
    );

    if report.decisions == 0 && report.claims == 0 {
        println!();
        println!(
            "verdict   NOTHING TO CHECK: no decision and no claim, so nothing in this file \
             could have been found missing"
        );
        return ExitCode::from(2);
    }
    if report.is_complete() {
        println!();
        println!(
            "verdict   COMPLETE: every id minted from 1 to {} was written down, and every \
             claim folds",
            report.decisions
        );
        return ExitCode::SUCCESS;
    }
    println!();
    for finding in &report.findings {
        println!("missing   {finding}");
    }
    println!();
    println!(
        "verdict   INCOMPLETE: {} thing(s) this file implies happened are not in it",
        report.findings.len()
    );
    ExitCode::FAILURE
}

fn diff(live: &std::path::Path, replay: &std::path::Path) -> ExitCode {
    let (a, b) = match (
        quant_engine::journal::read(live),
        quant_engine::journal::read(replay),
    ) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) => {
            eprintln!("could not read {}: {e}", live.display());
            return ExitCode::FAILURE;
        }
        (_, Err(e)) => {
            eprintln!("could not read {}: {e}", replay.display());
            return ExitCode::FAILURE;
        }
    };
    println!("live      {}", live.display());
    println!("replay    {}", replay.display());
    println!();

    match runlog::diff(&a.entries, &b.entries) {
        Divergence::Agree { decisions } => {
            println!("verdict   AGREE: {decisions} decisions, in the same order, both sides");
            ExitCode::SUCCESS
        }
        Divergence::Differs {
            at_decision,
            live,
            replay,
        } => {
            println!("decision  #{at_decision} is the first to differ");
            println!("live      {live}");
            println!("replay    {replay}");
            println!();
            println!("verdict   DIVERGES");
            ExitCode::FAILURE
        }
        Divergence::LengthDiffers { live, replay } => {
            println!("live      {live} decisions");
            println!("replay    {replay} decisions");
            println!();
            println!("verdict   DIVERGES: one side decided more than the other");
            ExitCode::FAILURE
        }
        Divergence::NothingToCompare => {
            println!(
                "verdict   NOTHING TO COMPARE: neither file holds a decision, so agreement \
                 here would mean nothing"
            );
            ExitCode::from(2)
        }
        Divergence::LiveRestarted { started } => {
            println!(
                "verdict   CANNOT COMPARE: the live side holds {started} `started` entries, so \
                 it restarted"
            );
            println!(
                "          A restart forfeits the exact match. `Engine::resuming` replaces only"
            );
            println!(
                "          the portfolio, so the session came back with empty indicator windows"
            );
            println!(
                "          and a zeroed risk tally while the replay ran straight through. Compare"
            );
            println!("          the segments between restarts instead.");
            ExitCode::from(2)
        }
    }
}
