//! Item 8's binary, rehearsed: `quant_backtest::live::run`, the body of the
//! `live` binary, against local stand-ins for every endpoint it reaches
//! (`docs/live-run.md` §4 item 8, 8a). Nothing here reaches Binance.
//!
//! - the REST side is `quant_live::fixtures::rest`: the venue's clock, its
//!   three checks, `exchangeInfo`, a depth snapshot, the account's balances,
//!   and orders and cancels acknowledged, matching nothing;
//! - the account side is `quant_binance::fake_venue`, which checks each
//!   logon's signature and pushes what a test tells it;
//! - the market is `quant_binance::fake_market`, which sends what a test
//!   tells it.
//!
//! Here because this is the one crate that may link the binary's crate, the
//! verifier and `runlog::check` together.
//!
//! # How a run is driven
//!
//! The run goes on a thread of its own, as the binary's main thread, with a
//! runtime of its own. The test moves the touch a dollar at a time on the
//! fake market, one move per sampling interval, until the crossover sends an
//! order; answers it on the account stream as the venue would, a `NEW` and a
//! `TRADE`; and waits for the fill to be journalled before moving the market
//! again. Every wait is on an observable fact, and bounded.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quant_backtest::account_replay::{self, Verdict};
use quant_backtest::live::{self, Gate, Journalled, LiveConfig, LiveRun, Outcome};
use quant_backtest::record::Lines;
use quant_backtest::session::Settings;
use quant_binance::fake_market::{self, FakeMarket};
use quant_binance::fake_venue::{self, FakeVenue};
use quant_core::event::Side;
use quant_core::execution::ClientOrderId;
use quant_core::instrument::Exchange;
use quant_core::report::EndedBy;
use quant_core::time::{Clock, SystemClock, Ts};
use quant_engine::journal::{self, Agreement, InstrumentKey, Journal, JournalEntry, Recovered};
use quant_engine::Limits;
use quant_explain::runlog;
use quant_live::fixtures::rest::{venue_order_id, Reply, RestVenue, Script, Seen};
use quant_live::fixtures::{ended, new_order, trade_priced};
use quant_live::Deadlines;
use quant_recorder::{account_catalog, catalog};

/// How long any one wait may take before the test fails.
const WITHIN: Duration = Duration::from_secs(20);

/// The crossover's sampling interval, short enough for a rehearsal.
const INTERVAL: Duration = Duration::from_millis(25);

static RIGS: AtomicUsize = AtomicUsize::new(0);

/// A directory of this rig's own, named for this process and the rig, and
/// cleared: a process id is reused once its process has gone, and a journal
/// or a capture left by one that held it before must not be read as this
/// rig's.
fn scratch() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "quant-explain-live-run-{}-{}",
        std::process::id(),
        RIGS.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// Wait until `holds`, or fail naming `what`.
fn until(what: &str, holds: impl Fn() -> bool) {
    let deadline = Instant::now() + WITHIN;
    while !holds() {
        assert!(
            Instant::now() < deadline,
            "gave up waiting for {what} after {WITHIN:?}"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The local venue a run is pointed at, and where it records.
struct Rig {
    runtime: tokio::runtime::Runtime,
    account: Arc<FakeVenue>,
    market: Arc<FakeMarket>,
    rest: Arc<RestVenue>,
    dir: PathBuf,
    root: PathBuf,
    journal: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Rig {
    fn new(script: Script) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a runtime");
        let account = Arc::new(runtime.block_on(FakeVenue::start(fake_venue::Script::default())));
        let market = Arc::new(runtime.block_on(FakeMarket::start()));
        let rest = Arc::new(RestVenue::start(script));
        let dir = scratch();
        let root = dir.join("data");
        let journal = root.join("live-BTCUSDT.jsonl");
        std::fs::create_dir_all(&root).expect("data root");
        Self {
            runtime,
            account,
            market,
            rest,
            dir,
            root,
            journal,
        }
    }

    fn config(&self, qty: &str) -> LiveConfig {
        LiveConfig {
            environment: live::Environment::Testnet,
            endpoints: live::Endpoints {
                rest: self.rest.base().to_owned(),
                ws_api: self.account.url().to_owned(),
                market_ws: self.market.url().to_owned(),
            },
            root: self.root.clone(),
            symbol: "BTCUSDT".to_owned(),
            cash: "100".parse().expect("cash"),
            settings: Settings {
                fast: 1,
                slow: 2,
                interval: i64::try_from(INTERVAL.as_nanos()).expect("fits"),
                qty: qty.parse().expect("qty"),
            },
            limits: Limits {
                max_order_notional: Some("200".parse().expect("amount")),
                max_position_notional: Some("200".parse().expect("amount")),
                max_daily_loss: Some("20".parse().expect("amount")),
                max_orders_per_day: Some(200),
            },
            limit: None,
            account_policy: fake_venue::quick_policy(),
            deadlines: Deadlines {
                settle: Duration::from_secs(2),
                answers: Duration::from_secs(5),
                pause: Duration::from_millis(5),
            },
            clock: Arc::new(SystemClock),
        }
    }

    /// The journal file, as the binary opens it.
    fn journalled(&self) -> Journalled {
        live::journal_at(&self.journal).expect("the journal opens")
    }

    fn entries(&self) -> Vec<JournalEntry> {
        journal::read(&self.journal).expect("reads").entries
    }

    /// Write `entries` into the journal, as an earlier session left them.
    fn earlier(&self, entries: &[JournalEntry]) {
        let mut file = Journal::open(&self.journal).expect("opens");
        for entry in entries {
            file.append(entry).expect("appends");
        }
    }

    /// Start a run on its own thread, with the account's credentials, and
    /// stopped by the handle returned.
    fn start(&self, config: LiveConfig, journalled: Journalled) -> Running {
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let account = Arc::clone(&self.account);
        let thread = std::thread::spawn(move || {
            let keys = move || Ok(account.credentials());
            live::run(&config, &keys, journalled, async {
                let _ = stopped.await;
                Ok(())
            })
        });
        Running {
            thread: Some(thread),
            stop: Some(stop),
        }
    }

    /// Wait until the run has subscribed to the account and connected to the
    /// market: a run that passed its gates.
    fn trading(&self) {
        let account = Arc::clone(&self.account);
        self.runtime
            .block_on(account.until("the account subscription", WITHIN, |log| {
                log.asked("userDataStream.subscribe") >= 1
            }));
        until("the market connection", || self.market.connections() == 1);
    }

    /// Orders sent, cancels left out.
    fn orders(&self) -> Vec<Seen> {
        self.rest
            .order_requests()
            .into_iter()
            .filter(|seen| seen.method == "POST")
            .collect()
    }

    fn cancels(&self) -> Vec<Seen> {
        self.rest
            .order_requests()
            .into_iter()
            .filter(|seen| seen.method == "DELETE")
            .collect()
    }

    /// The venue's `NEW` for an order the run sent.
    fn accept(&self, order: &Seen) {
        let (client, n, qty) = named(order);
        self.account
            .event(&text(new_order(&client, venue_order_id(n), &qty)));
    }

    /// The venue's `NEW` and its `TRADE` in full, for an order the run sent:
    /// at `price`, the venue's `fee` in `asset`, as trade `trade_id`.
    fn fill(&self, order: &Seen, price: &str, (fee, asset): (&str, &str), trade_id: i64) {
        self.accept(order);
        let (client, n, qty) = named(order);
        let side = order.param("side").expect("a side").to_owned();
        self.account.event(&text(trade_priced(
            &client,
            venue_order_id(n),
            &side,
            &qty,
            (&qty, &qty, price),
            (fee, asset),
            trade_id,
        )));
    }

    /// Wait until the journal holds a line `holds` picks out.
    fn journalled_line(&self, what: &str, holds: impl Fn(&JournalEntry) -> bool) {
        until(what, || {
            journal::read(&self.journal).is_ok_and(|r| r.entries.iter().any(&holds))
        });
    }
}

/// An order request's client id, our id in it, and its quantity.
fn named(order: &Seen) -> (String, u64, String) {
    (
        order
            .param("newClientOrderId")
            .expect("a client id")
            .to_owned(),
        order.names().expect("ours"),
        order.param("quantity").expect("a quantity").to_owned(),
    )
}

fn text(frame: Vec<u8>) -> String {
    String::from_utf8(frame).expect("text")
}

/// A run in flight.
struct Running {
    thread: Option<std::thread::JoinHandle<Result<LiveRun, String>>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Running {
    /// Tell the run to stop, and wait for it, within a bound.
    fn stop(mut self) -> LiveRun {
        self.signal();
        self.join()
    }

    /// Tell the run to stop, without waiting.
    fn signal(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }

    fn join(mut self) -> LiveRun {
        let thread = self.thread.take().expect("joined once");
        let deadline = Instant::now() + WITHIN * 2;
        while !thread.is_finished() {
            assert!(Instant::now() < deadline, "the run did not stop");
            std::thread::sleep(Duration::from_millis(5));
        }
        thread
            .join()
            .expect("the run's thread")
            .expect("the run ends without an error")
    }
}

/// The touch, moved a dollar at a time from the default snapshot's
/// 76650/76651 at update id 100, one move per sampling interval on the clock
/// that stamps them.
///
/// To make the crossover act, a test moves the market one way until an order
/// appears ([`Self::cross`]), and keeps moving it that way while it notices:
/// a market that only rises or only falls crosses nothing, so the moves sent
/// before the test saw the order cannot cross back and send another. Moving
/// up twice and down twice ([`Self::step`]) crosses every other move, for a
/// test that wants many crossings and no fill.
struct Market {
    bid: i64,
    update: u64,
    step: usize,
    last: Ts,
}

impl Market {
    fn new() -> Self {
        Self {
            bid: 76_650,
            update: 100,
            step: 0,
            last: SystemClock.now(),
        }
    }

    fn step(&mut self, market: &FakeMarket) {
        let by = if self.step % 4 < 2 { 1 } else { -1 };
        self.step_by(market, by);
    }

    /// Move the touch `by` dollars.
    fn step_by(&mut self, market: &FakeMarket, by: i64) {
        let next = self.bid + by;
        let interval = i64::try_from(INTERVAL.as_nanos()).expect("fits");
        let last = self.last;
        until("the interval to pass", || {
            SystemClock.now().as_nanos() >= last.as_nanos() + interval
        });
        self.update += 1;
        let (old_bid, new_bid) = (format!("{}.00", self.bid), format!("{next}.00"));
        let (old_ask, new_ask) = (format!("{}.00", self.bid + 1), format!("{}.00", next + 1));
        market.send(&fake_market::depth(
            "BTCUSDT",
            self.update,
            self.update,
            &[(new_bid.as_str(), "1"), (old_bid.as_str(), "0")],
            &[(new_ask.as_str(), "1"), (old_ask.as_str(), "0")],
            SystemClock.now().as_millis(),
        ));
        self.last = SystemClock.now();
        self.bid = next;
        self.step += 1;
    }

    /// Move `by` until the run has sent `n` orders in all, or fail: a run of
    /// moves one way, and if no order came of it, a short turn the other way
    /// and back, so that the next turn is a crossing whatever the crossover
    /// had already seen. A turn crosses the wrong way only into a no-op: down
    /// while flat, up while long, or anything while an order is working.
    fn cross(&mut self, rig: &Rig, by: i64, n: usize) -> Seen {
        for _ in 0..6 {
            for (moves, way) in [(6, by), (3, -by)] {
                for _ in 0..moves {
                    if rig.orders().len() >= n {
                        return rig.orders()[n - 1].clone();
                    }
                    self.step_by(&rig.market, way);
                }
            }
        }
        until("the order", || rig.orders().len() >= n);
        rig.orders()[n - 1].clone()
    }

    /// Move `n` times.
    fn moves(&mut self, market: &FakeMarket, n: usize) {
        for _ in 0..n {
            self.step(market);
        }
    }
}

fn filled(id: u64) -> impl Fn(&JournalEntry) -> bool {
    move |e| matches!(e, JournalEntry::Filled { client_order_id, .. } if *client_order_id == ClientOrderId(id))
}

fn traded(run: LiveRun) -> Box<live::Traded> {
    match run.outcome {
        Outcome::Traded(traded) => traded,
        Outcome::RecordOnly(refusal) => panic!("recorded only: {refusal}"),
    }
}

/// Every session under `root`, market and account, verified clean.
fn verified_clean(root: &Path) -> usize {
    let mut report = quant_verify::Report::default();
    let sessions = quant_verify::discover(root, &mut report);
    for session in &sessions {
        quant_verify::verify(session, &mut report);
    }
    assert!(report.is_clean(), "{:#?}", report.findings());
    assert_eq!(report.findings(), &[] as &[_], "{:#?}", report.findings());
    sessions.len()
}

/// A run that passes its gates and trades one round trip: a buy, its fee
/// taken in the base, and an exit that sells what is held. Stopped once the
/// exit's fill is journalled.
fn round_trip(rig: &Rig) -> LiveRun {
    let running = rig.start(rig.config("0.001"), rig.journalled());
    rig.trading();
    let mut market = Market::new();

    let buy = market.cross(rig, 1, 1);
    assert_eq!(buy.param("side"), Some("BUY"));
    assert_eq!(buy.param("quantity"), Some("0.001"));
    // A spot venue takes a buy's fee in the asset bought.
    rig.fill(&buy, "76651.00", ("0.00000100", "BTC"), 1);
    rig.journalled_line("the buy's fill", filled(1));

    let sell = market.cross(rig, -1, 2);
    assert_eq!(sell.param("side"), Some("SELL"));
    assert_eq!(
        sell.param("quantity"),
        Some("0.00099"),
        "the exit sells what is held, rounded down to the lot"
    );
    rig.fill(&sell, "76649.00", ("0.07588251", "USDT"), 2);
    rig.journalled_line("the sell's fill", filled(2));
    running.stop()
}

#[test]
fn a_rehearsal_passes_its_gates_trades_one_round_trip_and_stops_with_nothing_resting() {
    let rig = Rig::new(Script::default());
    let run = round_trip(&rig);
    assert_eq!(run.capture, Ok(()));
    let traded = traded(run);
    assert_eq!(
        traded.cancelled,
        Vec::new(),
        "nothing rested, so nothing cancelled"
    );
    assert_eq!(traded.drained.working, Vec::new());
    assert!(traded.drained.settled && traded.drained.answered);
    assert!(!traded.latched);
    assert!(traded.account.is_ok(), "{:?}", traded.account);
    assert_eq!(traded.stats.fills, 2);
    assert_eq!(rig.orders().len(), 2);
    assert_eq!(rig.cancels().len(), 0);

    let entries = rig.entries();
    match entries.first() {
        Some(JournalEntry::Started { run, .. }) => {
            assert_eq!(run.as_deref(), Some(traded.run.as_str()));
        }
        other => panic!("the journal begins with started: {other:?}"),
    }
    assert!(
        matches!(entries.get(1), Some(JournalEntry::Balances { .. })),
        "and the baseline, before any order: {entries:#?}"
    );
    let balances = entries
        .iter()
        .filter(|e| matches!(e, JournalEntry::Balances { .. }))
        .count();
    assert_eq!(balances, 2, "at the start and at the stop");
    assert!(entries
        .iter()
        .any(|e| matches!(e, JournalEntry::Stopped { working, .. } if working.is_empty())));
    assert!(!entries
        .iter()
        .any(|e| matches!(e, JournalEntry::CancelRequested { .. })));

    // reconcile's check, runlog check, and the verifier over both roots.
    assert_eq!(journal::agrees(&entries), Agreement::Agrees, "reconcile");
    let checked = runlog::check_file(&journal::read(&rig.journal).expect("reads"));
    assert!(checked.is_complete(), "{:#?}", checked.findings);
    assert_eq!(checked.decisions, 2);
    assert_eq!(account_catalog(&rig.root).sessions.len(), 1);
    assert_eq!(catalog(&rig.root).sessions.len(), 1);
    assert_eq!(
        verified_clean(&rig.root),
        2,
        "the market's session and the account's"
    );

    // And the account replay check: the recorded account frames and the
    // journal's REST lines reproduce every line the venue produced.
    let replay = account_replay::replay(&entries, &rig.root, None).expect("a live journal");
    assert_eq!(replay.verdict(), Verdict::Agrees, "{replay:#?}");
    assert_eq!((replay.sessions, replay.matched), (1, 1));
    assert!(replay.records > 0 && replay.compared > 0, "{replay:#?}");
}

#[test]
fn the_account_replay_disagrees_with_a_journal_its_capture_does_not_support() {
    // Criterion A's sabotages, against the replay: a fill line deleted, a fee
    // moved by one raw unit, a stamp moved by a nanosecond, and the capture
    // taken away. Each must be a
    // disagreement, naming the line; and a journal that is not a live run's
    // is nothing to compare.
    let rig = Rig::new(Script::default());
    let _ = round_trip(&rig);
    let entries = rig.entries();
    let replayed = |entries: &[JournalEntry], root: &Path| {
        account_replay::replay(entries, root, None).expect("a live journal")
    };
    assert_eq!(replayed(&entries, &rig.root).verdict(), Verdict::Agrees);

    let fill = entries.iter().position(filled(1)).expect("the buy's fill");
    let mut deleted = entries.clone();
    let gone = deleted.remove(fill);
    let replay = replayed(&deleted, &rig.root);
    assert_eq!(replay.verdict(), Verdict::Disagrees);
    assert_eq!(
        replay.extra,
        [gone],
        "the replay made the line the journal lost"
    );

    let mut moved = entries.clone();
    if let JournalEntry::Filled { fee, .. } = &mut moved[fill] {
        *fee = quant_core::fixed::Notional::from_raw(fee.raw() + 1);
    }
    let replay = replayed(&moved, &rig.root);
    assert_eq!(replay.verdict(), Verdict::Disagrees);
    assert_eq!(replay.missing, [moved[fill].clone()]);
    assert_eq!(replay.extra, [entries[fill].clone()]);

    let mut late = entries.clone();
    if let JournalEntry::Filled { at, .. } = &mut late[fill] {
        *at = Ts::from_nanos(at.as_nanos() + 1);
    }
    let replay = replayed(&late, &rig.root);
    assert_eq!(
        replay.verdict(),
        Verdict::Disagrees,
        "a stamp a nanosecond out"
    );
    assert_eq!(replay.missing, [late[fill].clone()]);

    let elsewhere = rig.dir.join("no-capture");
    std::fs::create_dir_all(&elsewhere).expect("an empty root");
    let replay = replayed(&entries, &elsewhere);
    assert_eq!(replay.verdict(), Verdict::Disagrees);
    assert!(
        replay
            .missing
            .iter()
            .any(|e| matches!(e, JournalEntry::Filled { .. })),
        "what the stream said is missing without its capture: {replay:#?}"
    );

    let paper = [started(None), stopped()];
    assert!(matches!(
        account_replay::replay(&paper, &rig.root, Some("BTCUSDT")),
        Err(account_replay::ReplayError::NotLive(_))
    ));
}

/// A run whose gate `gate` refuses: it records the market, trades nothing,
/// opens no account stream and writes nothing to the journal, however the
/// market moves.
fn refused_at(gate: Gate, script: Script, earlier: &[JournalEntry], qty: &str) {
    let rig = Rig::new(script);
    rig.earlier(earlier);
    let before = rig.entries();
    let running = rig.start(rig.config(qty), rig.journalled());
    until("the market connection", || rig.market.connections() == 1);
    // Enough moves for a run that traded to have sent an order.
    Market::new().moves(&rig.market, 12);
    until("every move sent", || rig.market.sent().len() == 12);
    let run = running.stop();
    assert_eq!(run.capture, Ok(()));
    match run.outcome {
        Outcome::RecordOnly(refusal) => assert_eq!(refusal.gate, gate, "{refusal}"),
        Outcome::Traded(traded) => panic!("traded past the {} gate: {traded:?}", gate.name()),
    }
    assert_eq!(rig.orders().len(), 0, "no order sent");
    assert_eq!(rig.entries(), before, "nothing written to the journal");
    assert_eq!(
        account_catalog(&rig.root).sessions.len(),
        0,
        "no account stream opened"
    );
    assert_eq!(rig.account.log().connections, 0);
    assert_eq!(catalog(&rig.root).sessions.len(), 1, "the market recorded");
    assert_eq!(verified_clean(&rig.root), 1);
}

fn started(run: Option<&str>) -> JournalEntry {
    JournalEntry::Started {
        at: Ts::from_nanos(1),
        cash: "100".parse().expect("cash"),
        schema: journal::SCHEMA,
        run: run.map(str::to_owned),
    }
}

fn stopped() -> JournalEntry {
    JournalEntry::Stopped {
        at: Ts::from_nanos(9),
        working: Vec::new(),
    }
}

#[test]
fn a_venue_that_refuses_the_checks_is_recorded_and_not_traded() {
    let script = Script::default().once(
        "/api/v3/account",
        Reply::refuse(
            401,
            -2015,
            "Invalid API-key, IP, or permissions for action.",
        ),
    );
    refused_at(Gate::VenueChecks, script, &[], "0.001");
}

#[test]
fn rules_that_cannot_be_had_from_the_venue_are_recorded_and_not_traded() {
    let script = Script::default().once(
        "/api/v3/exchangeInfo",
        Reply::Http {
            status: 503,
            body: String::new(),
        },
    );
    refused_at(Gate::ExchangeInfo, script, &[], "0.001");
}

#[test]
fn a_journal_with_lines_and_no_run_tag_is_recorded_and_not_traded() {
    refused_at(
        Gate::RunTag,
        Script::default(),
        &[started(None), stopped()],
        "0.001",
    );
}

#[test]
fn a_journal_holding_a_latching_line_is_recorded_and_not_traded() {
    refused_at(
        Gate::Diverged,
        Script::default(),
        &[
            started(Some("r1")),
            JournalEntry::Anomaly {
                at: Ts::from_nanos(5),
                client_order_id: Some(ClientOrderId(1)),
                what: quant_core::report::AnomalyKind::Contradiction,
                detail: quant_core::report::VenueText::new("two venue ids for one of ours"),
            },
            stopped(),
        ],
        "0.001",
    );
}

#[test]
fn a_last_session_that_left_an_order_working_is_recorded_and_not_traded() {
    refused_at(
        Gate::LastSessionWorking,
        Script::default(),
        &[
            started(Some("r1")),
            JournalEntry::Submitted {
                at: Ts::from_nanos(3),
                client_order_id: ClientOrderId(1),
                instrument: InstrumentKey {
                    exchange: Exchange::Binance,
                    symbol: "BTCUSDT".to_owned(),
                },
                side: Side::Buy,
                qty: "0.001".parse().expect("qty"),
                limit: None,
                mark: None,
            },
            JournalEntry::Stopped {
                at: Ts::from_nanos(9),
                working: vec![ClientOrderId(1)],
            },
        ],
        "0.001",
    );
}

#[test]
fn a_size_whose_exit_falls_under_the_minimum_is_recorded_and_not_traded() {
    // 0.00007 at 76650 is worth 5.37, over the minimum of 5; the exit after
    // the fee in the base, 0.00006, is worth 4.60, under it.
    refused_at(Gate::Size, Script::default(), &[], "0.00007");
}

#[test]
fn no_baseline_is_recorded_and_not_traded() {
    // The checks read the account first and pass; the balances read it again
    // and find no BTC.
    let script = Script::default()
        .once("/api/v3/account", Reply::Ack)
        .once(
            "/api/v3/account",
            Reply::Http {
                status: 200,
                body: r#"{"canTrade":true,"balances":[{"asset":"USDT","free":"1000.00000000","locked":"0.00000000"}]}"#
                    .to_owned(),
            },
        );
    refused_at(Gate::Baseline, script, &[], "0.001");
}

#[test]
fn a_clean_stop_cancels_a_resting_order_and_settles_its_end() {
    let rig = Rig::new(Script::default());
    let running = rig.start(rig.config("0.001"), rig.journalled());
    rig.trading();
    let mut market = Market::new();
    let buy = market.cross(&rig, 1, 1);
    // Accepted and never traded: an order resting on the venue.
    rig.accept(&buy);
    rig.journalled_line("the acceptance", |e| {
        matches!(e, JournalEntry::Accepted { client_order_id, .. } if *client_order_id == ClientOrderId(1))
    });

    let mut running = running;
    running.signal();
    // The stop cancels it through the record, and the venue says it ended.
    let cancel = {
        until("the cancel", || !rig.cancels().is_empty());
        rig.cancels()[0].clone()
    };
    assert_eq!(cancel.names(), Some(1));
    let (client, n, qty) = named(&buy);
    rig.account.event(&text(ended(
        &client,
        venue_order_id(n),
        &qty,
        "0",
        EndedBy::Cancelled,
    )));
    let traded = traded(running.join());

    assert_eq!(traded.cancelled, vec![ClientOrderId(1)]);
    assert_eq!(traded.drained.working, Vec::new(), "{:?}", traded.drained);
    assert!(traded.drained.settled);
    let entries = rig.entries();
    let requested = entries
        .iter()
        .position(|e| matches!(e, JournalEntry::CancelRequested { client_order_id, .. } if *client_order_id == ClientOrderId(1)))
        .expect("the cancel written down");
    let cancelled = entries
        .iter()
        .position(|e| matches!(e, JournalEntry::Cancelled { client_order_id, .. } if *client_order_id == ClientOrderId(1)))
        .expect("and its end");
    assert!(requested < cancelled);
    assert!(entries
        .iter()
        .any(|e| matches!(e, JournalEntry::Stopped { working, .. } if working.is_empty())));
    assert_eq!(journal::agrees(&entries), Agreement::Agrees);
    let checked = runlog::check_file(&journal::read(&rig.journal).expect("reads"));
    assert!(checked.is_complete(), "{:#?}", checked.findings);
}

#[test]
fn a_restart_after_a_stop_that_left_an_order_working_records_only() {
    // The first run's order rests, and the venue never says it ended, so the
    // stop's cancel is asked and the drain runs out its deadline: `stopped`
    // names the order. The next run on the journal must not trade past it.
    let rig = Rig::new(Script::default());
    let running = rig.start(rig.config("0.001"), rig.journalled());
    rig.trading();
    let mut market = Market::new();
    let buy = market.cross(&rig, 1, 1);
    rig.accept(&buy);
    rig.journalled_line("the acceptance", |e| {
        matches!(e, JournalEntry::Accepted { .. })
    });
    let first = traded(running.stop());
    assert_eq!(first.drained.working, vec![ClientOrderId(1)]);
    assert!(rig.entries().iter().any(
        |e| matches!(e, JournalEntry::Stopped { working, .. } if working == &[ClientOrderId(1)])
    ));

    let before = rig.entries();
    let running = rig.start(rig.config("0.001"), rig.journalled());
    until("the market again", || rig.market.connections() == 2);
    let run = running.stop();
    match run.outcome {
        Outcome::RecordOnly(refusal) => assert_eq!(refusal.gate, Gate::LastSessionWorking),
        Outcome::Traded(traded) => panic!("traded past a working order: {traded:?}"),
    }
    assert_eq!(rig.entries(), before);
    assert_eq!(rig.orders().len(), 1);
}

#[test]
fn a_restart_onto_a_journal_with_a_stranger_s_trade_records_only() {
    // A trade on our symbol under an id that is not ours latches Diverged in
    // the first run, which stops clean with nothing working. The latch must
    // survive the restart: the journal holds the line that set it.
    let rig = Rig::new(Script::default());
    let running = rig.start(rig.config("0.001"), rig.journalled());
    rig.trading();
    rig.account.event(&text(trade_priced(
        "web_7a3f",
        555,
        "BUY",
        "0.01",
        ("0.01", "0.01", "76650.00"),
        ("0.00001000", "BTC"),
        77,
    )));
    rig.journalled_line("the stranger's trade", |e| {
        matches!(e, JournalEntry::Foreign { .. })
    });
    let first = traded(running.stop());
    assert_eq!(first.drained.working, Vec::new());
    assert!(journal::diverged(&rig.entries()));

    let before = rig.entries();
    let running = rig.start(rig.config("0.001"), rig.journalled());
    until("the market again", || rig.market.connections() == 2);
    let run = running.stop();
    match run.outcome {
        Outcome::RecordOnly(refusal) => assert_eq!(refusal.gate, Gate::Diverged),
        Outcome::Traded(traded) => panic!("traded past the latch: {traded:?}"),
    }
    assert_eq!(rig.entries(), before);
    assert_eq!(rig.orders().len(), 0);
}

/// A journal whose first `submitted` append fails, keeping every line it
/// held and saying when it failed.
struct FailsAtSubmitted {
    held: Arc<Mutex<Vec<JournalEntry>>>,
    failed: Arc<AtomicBool>,
    /// Appends asked of it after it failed.
    after: Arc<AtomicUsize>,
}

impl Lines for FailsAtSubmitted {
    fn append(&mut self, entry: &JournalEntry) -> std::io::Result<()> {
        if self.failed.load(Ordering::SeqCst) {
            self.after.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(entry, JournalEntry::Submitted { .. }) {
            self.failed.store(true, Ordering::SeqCst);
            return Err(std::io::Error::other("the disk is full"));
        }
        self.held.lock().expect("held").push(entry.clone());
        Ok(())
    }
}

#[test]
fn a_journal_that_fails_stops_every_order_and_writes_no_closing_line() {
    let rig = Rig::new(Script::default());
    let held = Arc::new(Mutex::new(Vec::new()));
    let failed = Arc::new(AtomicBool::new(false));
    let after = Arc::new(AtomicUsize::new(0));
    let sink = FailsAtSubmitted {
        held: Arc::clone(&held),
        failed: Arc::clone(&failed),
        after: Arc::clone(&after),
    };
    let journalled = Journalled {
        recovered: Recovered::default(),
        sink: Arc::new(Mutex::new(sink)),
    };
    let running = rig.start(rig.config("0.001"), journalled);
    rig.trading();
    let mut market = Market::new();
    // Move until the crossover submits and the journal fails on it, and then
    // on through more crossings, each of which the record must refuse.
    for _ in 0..80 {
        if failed.load(Ordering::SeqCst) {
            break;
        }
        market.step(&rig.market);
    }
    until("the failed append", || failed.load(Ordering::SeqCst));
    market.moves(&rig.market, 12);
    let traded = traded(running.stop());

    assert!(traded.latched, "the run knew its journal had stopped");
    assert_eq!(rig.orders().len(), 0, "no order reached the venue");
    assert!(traded.stats.refused >= 1, "{:?}", traded.stats);
    assert_eq!(
        after.load(Ordering::SeqCst),
        0,
        "nothing more asked of the journal after it failed"
    );
    let held = held.lock().expect("held").clone();
    assert!(
        matches!(held.first(), Some(JournalEntry::Started { .. })),
        "{held:#?}"
    );
    assert!(
        !held.iter().any(|e| matches!(
            e,
            JournalEntry::Checkpoint { .. } | JournalEntry::Stopped { .. }
        )),
        "no closing checkpoint or stopped line: {held:#?}"
    );
    assert_eq!(
        held.iter()
            .filter(|e| matches!(e, JournalEntry::Balances { .. }))
            .count(),
        1,
        "the start's baseline only: {held:#?}"
    );
}
