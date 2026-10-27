//! `replay-account` as a program: what it says about a path with no journal.
//! The replay itself is tested in `account_replay`'s own tests and end to end
//! in `quant-explain`'s `tests/live_run.rs`.

use std::process::Command;

#[test]
fn a_path_with_no_journal_compares_nothing_and_says_so() {
    // A directory of this process's own, cleared: a process id is reused
    // once its process has gone.
    let dir = std::env::temp_dir().join(format!(
        "quant-backtest-replay-account-cli-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    let missing = dir.join("live-BTCUSDT.jsonl");

    let out = Command::new(env!("CARGO_BIN_EXE_replay-account"))
        .arg(&missing)
        .output()
        .expect("runs");
    let said = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(2), "{said}");
    assert!(said.contains("there is no journal at"), "{said}");
    assert!(!said.contains("run tag"), "{said}");

    // And a journal that is there but empty is still read and judged, as
    // it was: nothing to compare, for the reason its contents give.
    std::fs::write(&missing, b"").expect("an empty journal");
    let out = Command::new(env!("CARGO_BIN_EXE_replay-account"))
        .arg(&missing)
        .output()
        .expect("runs");
    let said = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(2), "{said}");
    assert!(!said.contains("there is no journal at"), "{said}");
    assert!(said.contains("run tag"), "{said}");
    let _ = std::fs::remove_dir_all(&dir);
}
