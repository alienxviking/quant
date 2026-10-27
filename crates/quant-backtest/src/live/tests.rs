//! The gates that read the journal and the size, by themselves, and how the
//! journal is opened. The gates that ask the venue, and the run around them,
//! are rehearsed end to end against local stand-ins in `quant-explain`'s
//! `tests/live_run.rs`.

use quant_core::execution::ClientOrderId;
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind};
use quant_core::report::{AnomalyKind, VenueText};
use quant_core::time::Ts;
use quant_engine::JournalEntry;

use super::{
    diverged_gate, floored_exit, journal_at, last_session_gate, mint_run_tag, run_tag_gate,
    size_gate, Gate,
};

fn started(run: Option<&str>) -> JournalEntry {
    JournalEntry::Started {
        at: Ts::from_nanos(1),
        cash: "100".parse().expect("cash"),
        schema: quant_engine::journal::SCHEMA,
        run: run.map(str::to_owned),
    }
}

fn stopped(working: &[u64]) -> JournalEntry {
    JournalEntry::Stopped {
        at: Ts::from_nanos(9),
        working: working.iter().map(|&id| ClientOrderId(id)).collect(),
    }
}

fn btcusdt() -> InstrumentDef {
    InstrumentDef {
        exchange: Exchange::Binance,
        symbol: "BTCUSDT".to_owned(),
        base: "BTC".to_owned(),
        quote: "USDT".to_owned(),
        kind: InstrumentKind::Spot,
        tick_size: "0.01".parse().expect("tick"),
        lot_size: "0.00001".parse().expect("lot"),
        min_notional: "5".parse().expect("notional"),
    }
}

fn qty(text: &str) -> Qty {
    text.parse().expect("qty")
}

fn px(text: &str) -> Px {
    text.parse().expect("px")
}

#[test]
fn the_run_tag_is_minted_for_an_empty_journal_kept_from_a_tagged_one_and_refused_without() {
    let minted = mint_run_tag(Ts::from_millis(1_789_000_000_123));
    assert_eq!(
        run_tag_gate(&[], || minted.clone()).expect("minted"),
        minted
    );
    // The first `started` line's tag, whatever a later one says.
    let entries = [started(Some("abc1")), stopped(&[]), started(Some("zzz9"))];
    assert_eq!(
        run_tag_gate(&entries, || unreachable!("not minted"))
            .expect("kept")
            .as_str(),
        "abc1"
    );
    // A journal with lines and no tag: a paper journal, or one older than
    // the tag. Refused, never minted over.
    for entries in [vec![started(None), stopped(&[])], vec![stopped(&[])]] {
        let refused = run_tag_gate(&entries, || unreachable!("not minted")).expect_err("refused");
        assert_eq!(refused.gate, Gate::RunTag);
    }
}

#[test]
fn a_minted_tag_is_a_valid_tag_and_changes_with_the_millisecond() {
    let a = mint_run_tag(Ts::from_millis(1_789_000_000_000));
    let b = mint_run_tag(Ts::from_millis(1_789_000_000_001));
    assert_ne!(a, b);
    assert!(a.as_str().len() <= quant_binance::user_data::MAX_RUN_TAG_LEN);
    assert_eq!(mint_run_tag(Ts::from_millis(35)).as_str(), "z");
    assert_eq!(mint_run_tag(Ts::from_millis(36)).as_str(), "10");
}

#[test]
fn a_journal_holding_a_latching_line_refuses_and_one_without_does_not() {
    let clean = [started(Some("r1")), stopped(&[])];
    assert_eq!(diverged_gate(&clean), Ok(()));
    let latched = [
        started(Some("r1")),
        JournalEntry::Anomaly {
            at: Ts::from_nanos(5),
            client_order_id: Some(ClientOrderId(1)),
            what: AnomalyKind::Contradiction,
            detail: VenueText::new("two venue ids for one of ours"),
        },
        stopped(&[]),
    ];
    assert_eq!(
        diverged_gate(&latched).expect_err("refused").gate,
        Gate::Diverged
    );
}

#[test]
fn the_last_session_must_have_stopped_with_nothing_working() {
    assert_eq!(last_session_gate(&[]), Ok(()), "a fresh journal");
    assert_eq!(
        last_session_gate(&[started(Some("r1")), stopped(&[])]),
        Ok(())
    );
    // An earlier session's orders are that session's business only if the
    // last one stopped clean: the gate reads the last.
    assert_eq!(
        last_session_gate(&[
            started(Some("r1")),
            stopped(&[3]),
            started(Some("r1")),
            stopped(&[]),
        ]),
        Ok(())
    );
    for entries in [
        // Crashed: no stopped line.
        vec![started(Some("r1"))],
        vec![started(Some("r1")), stopped(&[]), started(Some("r1"))],
        // Stopped with an order working.
        vec![started(Some("r1")), stopped(&[4])],
        // No started line to read a session from.
        vec![stopped(&[])],
    ] {
        assert_eq!(
            last_session_gate(&entries).expect_err("refused").gate,
            Gate::LastSessionWorking,
            "{entries:?}"
        );
    }
    let said = last_session_gate(&[started(Some("r1")), stopped(&[4, 7])])
        .expect_err("refused")
        .why;
    assert!(said.contains("4, 7"), "{said}");
}

#[test]
fn the_exit_is_the_entry_less_its_fee_in_the_base_rounded_down_to_the_lot() {
    // Ten basis points of 0.001 is 0.000001: 0.000999, of which the venue
    // takes 99 lots.
    assert_eq!(floored_exit(qty("0.001"), &btcusdt()), qty("0.00099"));
    assert_eq!(floored_exit(qty("0.00007"), &btcusdt()), qty("0.00006"));
}

#[test]
fn a_size_whose_exit_falls_under_the_minimum_is_refused_and_one_at_it_is_not() {
    let def = btcusdt();
    // M5's size clears the minimum many times over.
    assert_eq!(size_gate(qty("0.001"), &def, px("76650.00")), Ok(()));
    // Both sides of the boundary, exactly: an entry of 0.00006 leaves an exit
    // of 0.00005, worth exactly the minimum at 100000.00, which passes, and
    // one tick under it at 99999.99, which does not.
    let at = |p: &str| size_gate(qty("0.00006"), &def, px(p));
    assert_eq!(floored_exit(qty("0.00006"), &def), qty("0.00005"));
    assert_eq!(
        px("100000.00").notional(qty("0.00005")),
        Some(def.min_notional)
    );
    assert_eq!(at("100000.00"), Ok(()));
    let refused = at("99999.99").expect_err("refused");
    assert_eq!(refused.gate, Gate::Size);
    assert!(refused.why.contains("0.00005"), "{}", refused.why);
    // The case the gate exists for: the entry alone clears the minimum, and
    // its exit does not.
    assert_eq!(
        size_gate(qty("0.00007"), &def, px("76650.00"))
            .expect_err("refused")
            .gate,
        Gate::Size
    );
    assert!(
        px("76651.00").notional(qty("0.00007")) >= Some(Notional::from_raw(5 * quant_core::SCALE)),
        "the entry alone would have passed a check of the entry alone"
    );
    // Not a whole number of lots.
    assert_eq!(
        size_gate(qty("0.000015"), &def, px("76650.00"))
            .expect_err("refused")
            .gate,
        Gate::Size
    );
    assert_eq!(
        size_gate(Qty::ZERO, &def, px("76650.00"))
            .expect_err("refused")
            .gate,
        Gate::Size
    );
}

#[test]
fn the_journal_s_directory_is_made_when_a_fresh_root_has_none() {
    // A directory of this process's own, cleared: a process id is reused
    // once its process has gone.
    let dir =
        std::env::temp_dir().join(format!("quant-backtest-journal-at-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.join("data").join("testnet").join("live-BTCUSDT.jsonl");
    assert!(!path.parent().expect("a parent").exists());

    let journalled = journal_at(&path).expect("opens on a fresh root");
    assert_eq!(journalled.recovered.entries, Vec::<JournalEntry>::new());
    journalled
        .sink
        .lock()
        .expect("sink")
        .append(&started(Some("r1")))
        .expect("appends");
    drop(journalled);
    // And a journal that is there is read, not replaced.
    let again = journal_at(&path).expect("opens again");
    assert_eq!(again.recovered.entries, vec![started(Some("r1"))]);
    drop(again);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_stop_file_stops_only_once_it_exists() {
    // Both sides of the boundary: still waiting after several looks while the
    // file is absent, and done within a bound once it appears.
    let dir = std::env::temp_dir().join(format!("quant-backtest-stop-file-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a directory");
    let path = dir.join("live.stop");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let mut stop = Box::pin(super::stop_file_appears(path.clone()));
        let absent = tokio::time::timeout(super::STOP_FILE_POLL * 4, stop.as_mut()).await;
        assert!(absent.is_err(), "it stopped with no stop file");
        std::fs::write(&path, b"stop").expect("the stop file");
        let present = tokio::time::timeout(std::time::Duration::from_secs(10), stop.as_mut()).await;
        assert!(matches!(present, Ok(Ok(()))), "the stop file was not seen");
    });
    let _ = std::fs::remove_dir_all(&dir);
}
