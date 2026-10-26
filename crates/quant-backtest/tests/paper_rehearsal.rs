//! Paper's session, rehearsed against local stand-ins, and held to M5's
//! criterion: paper P&L must match a backtest over the data captured during
//! the same window, exactly.
//!
//! The session moved out of the `paper` binary into `quant_backtest::session`
//! so that the live binary runs the same path. This is the check that moving
//! it changed nothing that matters: the extracted session captures a market
//! (`quant_binance::fake_market`, its snapshots from
//! `quant_live::fixtures::rest`), trades it on the simulator, and the capture
//! it wrote, normalized and backtested, reproduces its fills and cash to the
//! last unit. Nothing here reaches the venue.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quant_backtest::record::{write, JournalWriter};
use quant_backtest::session::{self, Settings};
use quant_binance::capture::CaptureConfig;
use quant_binance::fake_market::{self, FakeMarket};
use quant_core::fixed::Notional;
use quant_core::instrument::{Exchange, InstrumentRegistry};
use quant_core::time::{Clock, SystemClock, Ts};
use quant_engine::journal::{self, Agreement, Journal, JournalEntry};
use quant_engine::{Engine, Limits, RiskEngine};
use quant_live::fixtures::rest::{RestVenue, Script};
use quant_live::fixtures::{self};
use quant_normalize::{discover_days, normalize_session, HistoricalSource};
use quant_recorder::catalog;
use quant_sim::{Costs, SimulatedVenue};

/// How long any one wait may take before the test fails.
const WITHIN: Duration = Duration::from_secs(20);

/// The sampling interval, short enough for a rehearsal.
const INTERVAL: Duration = Duration::from_millis(25);

/// A data root of this test's own, named for this process and cleared: a
/// process id is reused once its process has gone, and a root left by one
/// that held it before must not be read as this one's.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("quant-backtest-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// Paper's own limits.
fn limits() -> Limits {
    Limits {
        max_order_notional: Some("200".parse().expect("amount")),
        max_position_notional: Some("200".parse().expect("amount")),
        max_daily_loss: Some("20".parse().expect("amount")),
        max_orders_per_day: Some(200),
    }
}

fn settings() -> Settings {
    Settings {
        fast: 1,
        slow: 2,
        interval: i64::try_from(INTERVAL.as_nanos()).expect("fits"),
        qty: "0.001".parse().expect("qty"),
    }
}

/// Wait until `holds`, or fail naming `what`.
fn until(what: &str, holds: impl Fn() -> bool) {
    let deadline = Instant::now() + WITHIN;
    while !holds() {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// A price path the crossover trades on: the touch moved a dollar at a time,
/// up twice and down twice, over and over, from the default snapshot's
/// 76650/76651 at update id 100. Each move is one depth update, sent once
/// the interval since the last has passed on the clock that stamps them, so
/// the crossover has a new mid to sample at each.
fn drive(market: &FakeMarket, moves: usize) {
    let (mut bid, mut update) = (76_650_i64, 100_u64);
    let mut last = SystemClock.now();
    for step in 0..moves {
        let next = if step % 4 < 2 { bid + 1 } else { bid - 1 };
        let interval = i64::try_from(INTERVAL.as_nanos()).expect("fits");
        until("the interval to pass", || {
            SystemClock.now().as_nanos() >= last.as_nanos() + interval
        });
        update += 1;
        let (old_bid, new_bid) = (format!("{bid}.00"), format!("{next}.00"));
        let (old_ask, new_ask) = (format!("{}.00", bid + 1), format!("{}.00", next + 1));
        market.send(&fake_market::depth(
            "BTCUSDT",
            update,
            update,
            &[(new_bid.as_str(), "1"), (old_bid.as_str(), "0")],
            &[(new_ask.as_str(), "1"), (old_ask.as_str(), "0")],
            SystemClock.now().as_millis(),
        ));
        last = SystemClock.now();
        bid = next;
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one rehearsal and the backtest it is held to, read top to bottom"
)]
fn paper_s_session_against_a_local_market_agrees_with_a_backtest_over_its_own_capture() {
    let root = scratch("paper-rehearsal");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");
    let market = Arc::new(runtime.block_on(FakeMarket::start()));
    let rest = RestVenue::start(Script::default());
    let cash: Notional = "100".parse().expect("cash");

    // Paper's session, as the binary wires it, with its costs off so that
    // every order fills on the next event and the path trades.
    let journal_path = root.join("paper-BTCUSDT.jsonl");
    let prepared = session::prepare(
        journal::read(&journal_path).expect("an empty journal reads"),
        fixtures::btcusdt(),
        cash,
        limits(),
    )
    .expect("a fresh registry");
    let instrument = prepared.instrument;
    let filters = prepared
        .registry
        .get(instrument)
        .expect("registered")
        .filters();
    let strategy = session::strategy(settings(), instrument, prepared.lot).expect("a crossover");
    let journal = Arc::new(Mutex::new(Journal::open(&journal_path).expect("opens")));
    write(
        &journal,
        &JournalEntry::Started {
            at: SystemClock.now(),
            cash,
            schema: journal::SCHEMA,
            run: None,
        },
    )
    .expect("started");
    let (engine_tx, engine_rx) = session::market_channel();
    let observer = Box::new(JournalWriter::new(
        Arc::clone(&journal),
        "BTCUSDT".to_owned(),
        0,
    ));
    let venue = SimulatedVenue::with_costs(Costs::NONE).enforcing(instrument, filters);
    let engine = session::engine(engine_rx, venue, prepared, strategy, observer);
    let config = CaptureConfig::new("BTCUSDT", &root).at(market.url(), rest.base());

    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let driver = {
        let market = Arc::clone(&market);
        std::thread::spawn(move || {
            until("the capture's connection", || market.connections() == 1);
            drive(&market, 24);
            until("every move sent", || market.sent().len() == 24);
            let _ = stop.send(());
        })
    };
    let ran = session::run_market(
        engine,
        engine_tx,
        config,
        &runtime,
        async {
            let _ = stopped.await;
            Ok(())
        },
        "paper-rehearsal",
        |_| (),
    )
    .expect("the session runs");
    driver.join().expect("the driver");
    ran.capture.expect("a clean capture");
    let paper = ran.engine;
    ran.outcome.expect("no source failure");
    let (checkpoint, stopped) = session::closing(&paper);
    write(&journal, &checkpoint).expect("checkpoint");
    write(&journal, &stopped).expect("stopped");

    let fills = paper.portfolio().fills();
    assert!(
        fills >= 2,
        "the path traded, or the comparison below proves nothing: {fills} fills"
    );

    // The capture it wrote, normalized, and backtested with the same
    // strategy, venue, limits and capital.
    let mut registry = InstrumentRegistry::new();
    let replayed = registry.register(fixtures::btcusdt());
    let sessions = catalog(&root).sessions;
    assert_eq!(sessions.len(), 1, "one capture session");
    let normalized = normalize_session(&sessions[0], replayed, Some(&root));
    assert!(
        normalized.write_error.is_none(),
        "{:?}",
        normalized.write_error
    );
    let days = discover_days(&root, Exchange::Binance, "BTCUSDT");
    let mut backtest = Engine::new(
        HistoricalSource::new(&root, Exchange::Binance, "BTCUSDT", replayed, days),
        SimulatedVenue::with_costs(Costs::NONE).enforcing(replayed, filters),
        RiskEngine::new(limits()),
        session::strategy(settings(), replayed, filters.lot_size).expect("a crossover"),
        cash,
    );
    backtest.run().expect("the backtest runs");

    let (p, b) = (paper.portfolio(), backtest.portfolio());
    assert_eq!(
        (p.fills(), p.cash(), p.realized(), p.fees()),
        (b.fills(), b.cash(), b.realized(), b.fees()),
        "paper and a backtest over its own capture agree exactly"
    );
    assert_eq!(
        paper.stats().last_ts.map(Ts::as_nanos),
        backtest.stats().last_ts.map(Ts::as_nanos),
        "over the same window"
    );
    assert_eq!(ran.tee_dropped, 0, "the engine saw every record");
    assert_eq!(
        journal::agrees(&journal::read(&journal_path).expect("reads").entries),
        Agreement::Agrees,
        "the journal agrees with the engine"
    );
    let _ = std::fs::remove_dir_all(&root);
}
