//! Replay a live run's recorded account stream against its journal.
//!
//! ```text
//! replay-account <JOURNAL> [DATA_ROOT] [--symbol SYM]
//! replay-account data/testnet/live-BTCUSDT.jsonl          # root: the journal's directory
//! ```
//!
//! Item 8's acceptance asks that replaying the recorded account frames through
//! the translator reproduce every venue-produced journal entry of a run.
//! `quant_backtest::account_replay` is how, and why each step is what it is:
//! the REST answers come back off their own journal lines, the frames off the
//! account capture under `DATA_ROOT/account`, a fresh merge machine hears both
//! in the order the live venue heard them, and what it says, written by the
//! journal's own writer, must equal the journal's lines of the kinds a venue
//! produces, every field and stamp included.
//!
//! # Exit codes, as `runlog`'s
//!
//! `0` the replay agrees, `1` it does not, or the capture could not be read,
//! and **`2` nothing was compared**: no journal at the path given, a journal
//! that is not a live run's, or one with no account record and no line a
//! venue produced. A check that
//! compared nothing must not look like one that agreed.

use std::path::PathBuf;
use std::process::ExitCode;

use quant_backtest::account_replay::{self, ReplayError, Verdict};

fn main() -> ExitCode {
    let mut paths = Vec::new();
    let mut symbol = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                usage();
                return ExitCode::SUCCESS;
            }
            "--symbol" => symbol = args.next(),
            other if other.starts_with('-') => {
                usage();
                return ExitCode::from(2);
            }
            other => paths.push(PathBuf::from(other)),
        }
    }
    let (journal, root) = match paths.as_slice() {
        [journal] => (
            journal.clone(),
            journal
                .parent()
                .map_or_else(|| PathBuf::from("."), std::path::Path::to_path_buf),
        ),
        [journal, root] => (journal.clone(), root.clone()),
        _ => {
            usage();
            return ExitCode::from(2);
        }
    };
    // `journal::read` takes a missing file for an empty journal, which is
    // right for a run about to begin one and wrong here: a mistyped path
    // would be told it "has no run tag", sending whoever reads it to the
    // journal's contents rather than its name.
    if matches!(journal.try_exists(), Ok(false)) {
        println!(
            "verdict   NOTHING TO COMPARE: there is no journal at {}",
            journal.display()
        );
        return ExitCode::from(2);
    }
    let recovered = match quant_engine::journal::read(&journal) {
        Ok(recovered) => recovered,
        Err(e) => {
            eprintln!("could not read {}: {e}", journal.display());
            return ExitCode::FAILURE;
        }
    };
    println!("replay    {}", journal.display());
    println!("account   {}", root.join("account").display());
    let replay = match account_replay::replay(&recovered.entries, &root, symbol.as_deref()) {
        Ok(replay) => replay,
        Err(ReplayError::NotLive(why)) => {
            println!();
            println!("verdict   NOTHING TO COMPARE: {why}");
            return ExitCode::from(2);
        }
        Err(e @ ReplayError::Unreadable(_)) => {
            println!();
            println!("verdict   DISAGREES: {e}");
            return ExitCode::FAILURE;
        }
    };
    report(&replay)
}

/// What the replay found, said, and its exit code.
fn report(replay: &account_replay::Replay) -> ExitCode {
    println!(
        "sessions  {} under the run tag, {} account capture(s), {} matched to one",
        replay.sessions, replay.captures, replay.matched
    );
    println!(
        "heard     {} account record(s) replayed; {} venue line(s) in the journal",
        replay.records, replay.compared
    );
    if replay.strays > 0 {
        println!(
            "strays    {} file(s) in the account tree that are not its captures",
            replay.strays
        );
    }
    for line in &replay.missing {
        println!(
            "missing   {}",
            serde_json::to_string(line).unwrap_or_default()
        );
    }
    for line in &replay.extra {
        println!(
            "extra     {}",
            serde_json::to_string(line).unwrap_or_default()
        );
    }
    println!();
    match replay.verdict() {
        Verdict::Agrees => {
            println!(
                "verdict   AGREES: the recorded account stream and the journal's REST lines \
                 reproduce every line the venue produced"
            );
            ExitCode::SUCCESS
        }
        Verdict::Disagrees => {
            println!(
                "verdict   DISAGREES: {} line(s) the capture does not support, {} the record \
                 did not take",
                replay.missing.len(),
                replay.extra.len()
            );
            ExitCode::FAILURE
        }
        Verdict::NothingToCompare(why) => {
            println!("verdict   NOTHING TO COMPARE: {why}");
            ExitCode::from(2)
        }
    }
}

fn usage() {
    eprintln!(
        "usage: replay-account <JOURNAL> [DATA_ROOT] [--symbol SYM]\n\
         \n\
         DATA_ROOT defaults to the journal's directory; the account capture is\n\
         under DATA_ROOT/account. --symbol, when the journal's orders do not name it.\n\
         \n\
         exit 0 agrees, 1 disagrees or the capture cannot be read, 2 nothing was compared"
    );
}
