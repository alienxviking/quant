//! Recompute a paper session's P&L from its journal alone.
//!
//! ```text
//! reconcile PATH_TO_JOURNAL
//! reconcile data/paper-BTCUSDT.jsonl
//! ```
//!
//! `docs/engine-contract.md` §9 requires P&L recomputed from the journal to
//! agree with the engine's running portfolio. This is the recompute half: it
//! shares no state with a running engine, reads only what was written down, and
//! reaches the numbers by replaying them.
//!
//! Two tallies of one quantity is a pattern this project keeps returning to —
//! the venue and the portfolio each totalling fees, `quant-verify` and
//! `quant-normalize` each counting frames, a file's trailer and its metadata row
//! each claiming a frame count. Each time the value is not the second number but
//! the fact that a disagreement becomes *visible*.
//!
//! Exit code 0 means the journal is readable and self-consistent. It says
//! nothing about whether the session made money.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use quant_core::instrument::InstrumentRegistry;
use quant_engine::journal::{self, Agreement, JournalEntry, Recovered};
use quant_engine::Portfolio;

fn main() -> ExitCode {
    let Some(path) = parse_args() else {
        return ExitCode::FAILURE;
    };

    let recovered = match journal::read(&path) {
        Ok(recovered) => recovered,
        Err(e) => {
            eprintln!("{}: {e}", path.display());
            return ExitCode::FAILURE;
        }
    };

    let mut registry = InstrumentRegistry::new();
    let portfolio = journal::replay(&recovered.entries, &mut registry);

    describe_journal(&path, &recovered);
    describe_portfolio(&portfolio, &registry, &recovered);
    verdict(&recovered)
}

/// `None` when the arguments were wrong or help was printed.
fn parse_args() -> Option<PathBuf> {
    let mut paths: Vec<PathBuf> = Vec::new();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("usage: reconcile PATH_TO_JOURNAL");
                return None;
            }
            other if other.starts_with('-') => {
                eprintln!("unknown option: {other}");
                return None;
            }
            other => paths.push(PathBuf::from(other)),
        }
    }
    if let [path] = paths.as_slice() {
        return Some(path.clone());
    }
    eprintln!("usage: reconcile PATH_TO_JOURNAL");
    None
}

fn describe_journal(path: &Path, recovered: &Recovered) {
    let count =
        |wanted: fn(&JournalEntry) -> bool| recovered.entries.iter().filter(|e| wanted(e)).count();
    let sessions = count(|e| matches!(e, JournalEntry::Started { .. }));
    let clean_stops = count(|e| matches!(e, JournalEntry::Stopped { .. }));
    let checkpoints = count(|e| matches!(e, JournalEntry::Checkpoint { .. }));

    println!("journal   {}", path.display());
    println!("entries   {}", recovered.entries.len());
    println!("sessions  {sessions} started, {clean_stops} stopped cleanly");
    // A start with no matching stop is a crash. Not an error -- the supervisor
    // restarting is the designed behaviour -- but it is the number that says how
    // eventful a fortnight was.
    println!(
        "restarts  {}{}",
        sessions.saturating_sub(1),
        if sessions > clean_stops {
            " (the last session is still running, or did not stop cleanly)"
        } else {
            ""
        }
    );
    println!("marks     {checkpoints} checkpoints");
    // Said as a shape rather than a cause. `read` tolerates a line that is not
    // valid JSON, which is what a hard kill mid-write leaves and also what a
    // power loss that left NUL bytes leaves, and the file cannot say which.
    if recovered.torn_tail {
        println!(
            "tail      the last line is not valid JSON and was discarded: the shape an \
             interrupted write leaves"
        );
    }
    // Each one is an entry an interrupted write lost, and the restart after it.
    // Said with the line numbers so a person can go and look at what is there,
    // which the file still holds.
    if !recovered.torn_before_restart.is_empty() {
        let lines: Vec<String> = recovered
            .torn_before_restart
            .iter()
            .map(ToString::to_string)
            .collect();
        println!(
            "torn      {} {}: not valid JSON, the shape an interrupted write leaves, then \
             a restart wrote past it",
            if lines.len() == 1 { "line" } else { "lines" },
            lines.join(", ")
        );
    }
}

fn describe_portfolio(portfolio: &Portfolio, registry: &InstrumentRegistry, recovered: &Recovered) {
    let journalled = recovered
        .entries
        .iter()
        .filter(|e| matches!(e, JournalEntry::Filled { .. }))
        .count();

    println!();
    println!("recomputed from the journal alone:");
    println!("  starting cash  {}", portfolio.starting_cash());
    println!("  cash           {}", portfolio.cash());
    println!("  realized       {}", portfolio.realized());
    println!("  fees           {}", portfolio.fees());
    println!(
        "  fills          {} ({journalled} journalled)",
        portfolio.fills()
    );
    for (id, position) in portfolio.held() {
        let symbol = registry
            .get(id)
            .map_or_else(|| id.to_string(), |i| i.symbol.clone());
        println!(
            "  position       {} {symbol} at an average of {}",
            position.qty, position.avg_px
        );
    }
    if portfolio.open_positions() == 0 {
        println!("  position       flat");
    }

    println!();
    // The identity below is *not* the check, and it is worth saying why. The
    // recompute derives cash, realized and fees from the same fill lines, so
    // `cash - starting == realized - fees` holds by construction and can never
    // fail. Printed as arithmetic a reader can follow, not as evidence.
    let moved =
        quant_core::Notional::from_raw(portfolio.cash().raw() - portfolio.starting_cash().raw());
    let accounted =
        quant_core::Notional::from_raw(portfolio.realized().raw() - portfolio.fees().raw());
    println!(
        "arithmetic  cash moved {moved} = realized {} less fees {} = {accounted}",
        portfolio.realized(),
        portfolio.fees()
    );
    if portfolio.open_positions() > 0 {
        println!("            (a position is open, so cash is also short what it cost)");
    }
}

/// The actual check: two independent computations of one quantity.
///
/// The engine accumulated its numbers in memory as fills arrived; this replayed
/// them from the file afterwards. Their agreement is evidence. The identity
/// printed above is not.
///
/// A line torn before a restart rides on the verdict line, whatever the
/// verdict, and changes no exit code. Agreement cannot see the entry it lost:
/// the restarted engine resumed from the same short file, so its checkpoints
/// and this fold are short by the same entry. The verdict line is what
/// `ops/verify-loop.sh` copies into its log and `ops/status.sh` shows, so
/// that is where the loss has to be said; the `torn` line above it is in a
/// report nobody opens at 3am.
fn verdict(recovered: &Recovered) -> ExitCode {
    println!();
    let (said, code) = match journal::agrees(&recovered.entries) {
        Agreement::Agrees => (
            "AGREES: the engine's checkpoint matches this recompute".to_owned(),
            ExitCode::SUCCESS,
        ),
        // Deliberately not success, and its own exit code, distinct from
        // both. "Nobody disagreed" and "two independent answers matched" are
        // different statements and only one of them is evidence.
        Agreement::NothingToCheck => (
            "NOTHING TO CHECK: no checkpoint in this journal, so the recompute has \
             nothing to be compared against"
                .to_owned(),
            ExitCode::from(2),
        ),
        Agreement::Differs {
            field,
            claimed,
            recomputed,
        } => (
            format!(
                "DISAGREES on {field}: the engine claimed {claimed}, the journal replays \
                 to {recomputed}"
            ),
            ExitCode::FAILURE,
        ),
        Agreement::FillCountDiffers {
            claimed,
            recomputed,
        } => (
            format!(
                "DISAGREES on fills: the engine counted {claimed}, the journal holds \
                 {recomputed}. A fill was acted on and not written down, or written twice."
            ),
            ExitCode::FAILURE,
        ),
    };
    match recovered.torn_note() {
        Some(torn) => println!("verdict   {}. But {torn}", said.trim_end_matches('.')),
        None => println!("verdict   {said}"),
    }
    code
}
