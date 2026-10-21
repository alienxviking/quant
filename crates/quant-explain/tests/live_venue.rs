//! Item 7's shell, end to end: `LiveVenue` behind the real `Engine` and the
//! real `JournalWriter`, its request worker sending to a scripted local REST
//! server (`quant_live::fixtures::rest`), and its account side the real
//! listener, tee and `AccountFeed` against `quant_binance::fake_venue`
//! (`docs/live-run.md` §5, *`LiveVenue`, decided before the code*, §5).
//!
//! Here because this is the one crate that may link the venue, the engine,
//! the writer and `runlog::check` together.
//!
//! # How a test is driven
//!
//! The engine runs on the test's thread, as on the engine's own thread in a
//! live run, and the worker and the listener on a runtime beside it. The
//! market source is a script of steps: idle wakes until a fact the test can
//! observe holds, an action (a frame pushed on the account stream, a held
//! answer released), or one market event, on which the scripted strategy acts.
//! Every wait is on an observable fact, and bounded. A step that waits for a
//! fact holds the wake it sees the fact on as an idle one, so that the
//! engine's poll on that wake drains whatever the fact put on a channel.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quant_backtest::record::{write, JournalWriter};
use quant_binance::fake_venue::{self, FakeVenue, Listening, Said};
use quant_binance::trade::TradeClient;
use quant_core::event::{BookSnapshot, EventMeta, Level, MarketEvent, Side, Trade};
use quant_core::execution::{ClientOrderId, ExecutionEvent};
use quant_core::fixed::Notional;
use quant_core::instrument::{InstrumentId, InstrumentRegistry};
use quant_core::report::{Cited, DoubtCause, DoubtChange};
use quant_core::source::{EventSource, SourceError, Wake};
use quant_core::time::{Clock, SystemClock, Ts};
use quant_engine::journal::{self, Journal, JournalEntry};
use quant_engine::{AllowAll, Context, Engine, RefusedBy, Strategy, Unready};
use quant_explain::runlog;
use quant_live::fixtures::rest::{venue_order_id, Reply, RestVenue, Script, Seen};
use quant_live::fixtures::{self, market_buy, ours};
use quant_live::{drain, wire, Deadlines, Drained, LiveVenue, WorkerStats};
use quant_sim::SimulatedVenue;

/// How long any one wait may take before the test fails.
const WITHIN: Duration = Duration::from_secs(20);

/// What the strategy was told, without stamps, shared with the source.
type Told = Arc<Mutex<Vec<String>>>;

/// One step of a source's script.
enum Step {
    /// Idle wakes until this holds; the wake it holds on is idle too.
    Until(&'static str, Box<dyn FnMut() -> bool>),
    /// Do this, on an idle wake.
    Do(Box<dyn FnMut()>),
    /// One market event.
    Market,
}

/// A source that plays a script of steps, then ends.
struct Steps {
    instrument: InstrumentId,
    script: VecDeque<Step>,
    /// When the step at the front began, for its deadline.
    since: Instant,
    seq: u64,
}

impl std::fmt::Debug for Steps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Steps")
            .field("left", &self.script.len())
            .finish_non_exhaustive()
    }
}

impl Steps {
    fn new(instrument: InstrumentId, steps: Vec<Step>) -> Self {
        Self {
            instrument,
            script: steps.into(),
            since: Instant::now(),
            seq: 0,
        }
    }

    fn market(&mut self) -> MarketEvent {
        self.seq += 1;
        let now = SystemClock.now();
        MarketEvent::Trade(Trade {
            meta: EventMeta {
                instrument: self.instrument,
                exchange_ts: now,
                local_recv_ts: now,
                ingest_seq: self.seq,
            },
            px: "76650.12".parse().expect("px"),
            qty: "0.01".parse().expect("qty"),
            aggressor: Side::Buy,
            venue_trade_id: self.seq,
        })
    }

    fn next(&mut self) -> Option<Wake> {
        let step = self.script.front_mut()?;
        let wake = match step {
            Step::Until(what, holds) => {
                if holds() {
                    self.script.pop_front();
                    self.since = Instant::now();
                } else {
                    assert!(
                        self.since.elapsed() < WITHIN,
                        "gave up waiting for {what} after {WITHIN:?}"
                    );
                    std::thread::sleep(Duration::from_millis(2));
                }
                Wake::Idle
            }
            Step::Do(act) => {
                act();
                self.script.pop_front();
                self.since = Instant::now();
                Wake::Idle
            }
            Step::Market => {
                self.script.pop_front();
                self.since = Instant::now();
                Wake::Event(self.market())
            }
        };
        Some(wake)
    }
}

impl EventSource for Steps {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        loop {
            match self.next()? {
                Wake::Event(event) => return Some(Ok(event)),
                Wake::Idle => {}
            }
        }
    }

    fn next_event_timeout(&mut self, _timeout: Duration) -> Option<Result<Wake, SourceError>> {
        self.next().map(Ok)
    }
}

/// What the strategy does on one market event.
#[derive(Debug, Clone, Copy)]
enum Act {
    Buy,
    Cancel(u64),
}

/// Acts on each market event as its script says, and writes down everything
/// it is told, without stamps.
#[derive(Debug)]
struct Scripted {
    instrument: InstrumentId,
    acts: VecDeque<Vec<Act>>,
    told: Told,
}

impl Strategy for Scripted {
    fn on_market_event(&mut self, _event: &MarketEvent, ctx: &mut Context<'_>) {
        for act in self.acts.pop_front().unwrap_or_default() {
            match act {
                Act::Buy => {
                    ctx.submit(market_buy(self.instrument));
                }
                Act::Cancel(id) => ctx.cancel(ClientOrderId(id)),
            }
        }
    }

    fn on_execution(&mut self, event: &ExecutionEvent, _ctx: &mut Context<'_>) {
        let line = match event {
            ExecutionEvent::Accepted {
                client_order_id,
                ts: _,
            } => format!("accepted {}", client_order_id.0),
            ExecutionEvent::Rejected {
                client_order_id,
                reason,
                ts: _,
            } => format!("rejected {} {reason:?}", client_order_id.0),
            ExecutionEvent::Filled {
                client_order_id,
                fill,
                remaining,
                ts: _,
            } => format!(
                "filled {} {} at {} left {}",
                client_order_id.0, fill.qty, fill.px, remaining
            ),
            ExecutionEvent::Cancelled {
                client_order_id,
                remaining,
                ts: _,
            } => format!("cancelled {} left {}", client_order_id.0, remaining),
        };
        self.told.lock().expect("told").push(line);
    }
}

static RIGS: AtomicUsize = AtomicUsize::new(0);

/// A directory of this rig's own, named for this process and the rig, and
/// empty: a process id is reused once its process has gone, and a journal
/// left by one that held it before must not be read as this rig's.
fn scratch() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "quant-explain-live-venue-{}-{}",
        std::process::id(),
        RIGS.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// How a rig's trade client and worker are set.
#[derive(Default)]
struct Setup {
    script: Script,
    /// The trade client's order timeout, when a test needs one to pass.
    order_timeout: Option<Duration>,
    /// Decision 6's timer, when a test needs it to fire.
    unheard_after: Option<Duration>,
}

/// Everything a live venue is wired from, its servers, and its journal.
struct Rig {
    runtime: Option<tokio::runtime::Runtime>,
    rest: Arc<RestVenue>,
    account: Arc<FakeVenue>,
    listening: Option<Listening>,
    venue: Option<LiveVenue>,
    worker: Arc<WorkerStats>,
    journal: Arc<Mutex<Journal>>,
    dir: PathBuf,
    path: PathBuf,
    instrument: InstrumentId,
    told: Told,
}

impl Rig {
    fn new(setup: Setup) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a runtime");
        let account = Arc::new(runtime.block_on(FakeVenue::start(fake_venue::Script::default())));
        let dir = scratch();
        let root = dir.join("account");
        let (listening, feed) = {
            let _inside = runtime.enter();
            Listening::feeding(
                &root,
                account.url(),
                account.credentials(),
                fake_venue::quick_policy(),
                0,
            )
        };
        let rest = Arc::new(RestVenue::start(setup.script));
        let client = TradeClient::new(rest.base(), Duration::from_secs(5), fixtures::credentials())
            .expect("a client");
        let client = match setup.order_timeout {
            Some(timeout) => client.with_order_timeout(timeout),
            None => client,
        };
        let checked = runtime
            .block_on(fixtures::checked(&client, 0))
            .expect("the local checks pass");
        let instrument = InstrumentRegistry::new().register(fixtures::btcusdt());
        let (venue, worker) = wire(
            instrument,
            &fixtures::venue_instrument(),
            fixtures::run(),
            checked,
            client,
            feed,
            Arc::new(SystemClock),
        )
        .expect("wired");
        let worker = match setup.unheard_after {
            Some(after) => worker.unheard_after(after),
            None => worker,
        };
        let stats = worker.stats();
        runtime.spawn(worker.run());

        let path = dir.join("journal.jsonl");
        let journal = Arc::new(Mutex::new(Journal::open(&path).expect("journal opens")));
        write(
            &journal,
            &JournalEntry::Started {
                at: SystemClock.now(),
                cash: "100".parse().expect("cash"),
                schema: journal::SCHEMA,
                run: Some(fixtures::RUN.to_owned()),
            },
        )
        .expect("started");
        Self {
            runtime: Some(runtime),
            rest,
            account,
            listening: Some(listening),
            venue: Some(venue),
            worker: stats,
            journal,
            dir,
            path,
            instrument,
            told: Arc::default(),
        }
    }

    /// Idle until the listener has read the subscription's answer, which is
    /// then on the feed's channel for the engine's next poll.
    fn heard(&self) -> Step {
        let account = Arc::clone(&self.account);
        Step::Until(
            "the account subscription",
            Box::new(move || {
                let log = account.log();
                log.sent
                    .iter()
                    .find(|sent| sent.said == Said::SubscribeAnswer)
                    .is_some_and(|sent| log.read_by_listener(&sent.text))
            }),
        )
    }

    /// Push `frames` on the account stream, and idle until the listener has
    /// read the last of them.
    fn push(&self, frames: Vec<Vec<u8>>) -> [Step; 2] {
        let account = Arc::clone(&self.account);
        let last = String::from_utf8(frames.last().cloned().unwrap_or_default()).expect("text");
        let reader = Arc::clone(&self.account);
        [
            Step::Do(Box::new(move || {
                for frame in &frames {
                    account.event(std::str::from_utf8(frame).expect("text"));
                }
            })),
            Step::Until(
                "the listener to read what was pushed",
                Box::new(move || reader.log().read_by_listener(&last)),
            ),
        ]
    }

    /// Idle until the REST server has read `n` orders and cancels.
    fn rest_saw(&self, n: usize) -> Step {
        let rest = Arc::clone(&self.rest);
        Step::Until(
            "the REST server's requests",
            Box::new(move || rest.order_requests().len() >= n),
        )
    }

    /// Idle until the worker has answered `n` requests.
    fn answered(&self, n: u64) -> Step {
        let worker = Arc::clone(&self.worker);
        Step::Until(
            "the worker's answers",
            Box::new(move || worker.answered() >= n),
        )
    }

    /// Idle until the strategy has been told `line`.
    fn told(&self, line: &'static str) -> Step {
        let told = Arc::clone(&self.told);
        Step::Until(
            "the strategy to be told",
            Box::new(move || told.lock().expect("told").iter().any(|l| l == line)),
        )
    }

    /// Run `steps` through an engine with this venue, the strategy acting as
    /// `acts` says, then drain within `deadlines` and write the closing
    /// checkpoint and `stopped` line.
    fn run(&mut self, steps: Vec<Step>, acts: Vec<Vec<Act>>, deadlines: &Deadlines) -> Ran {
        let venue = self.venue.take().expect("run once");
        let writer = JournalWriter::strict(Arc::clone(&self.journal), "BTCUSDT".to_owned(), 0);
        let strategy = Scripted {
            instrument: self.instrument,
            acts: acts.into(),
            told: Arc::clone(&self.told),
        };
        let cash: Notional = "100".parse().expect("cash");
        let mut engine = Engine::new(
            Steps::new(self.instrument, steps),
            venue,
            AllowAll,
            strategy,
            cash,
        )
        .observing_fills(Box::new(writer));
        engine.run().expect("the script ends cleanly");
        let drained = drain(&mut engine, deadlines);
        let at = SystemClock.now();
        write(
            &self.journal,
            &JournalEntry::checkpoint(at, engine.portfolio()),
        )
        .expect("checkpoint");
        write(
            &self.journal,
            &JournalEntry::Stopped {
                at,
                working: drained.working.clone(),
            },
        )
        .expect("stopped");
        let venue = engine.venue();
        let ran = Ran {
            drained,
            ready: venue.readiness(),
            stats: venue.stats(),
            told: self.told.lock().expect("told").clone(),
            entries: Vec::new(),
        };
        // Step 6: the engine goes, and the venue and the queue with it.
        drop(engine);
        Ran {
            entries: journal::read(&self.path).expect("journal reads").entries,
            ..ran
        }
    }

    /// Steps 7 and 8: stop the listener, which seals the account capture,
    /// and only then the runtime.
    fn finish(mut self) {
        if let (Some(runtime), Some(listening)) = (self.runtime.take(), self.listening.take()) {
            runtime.block_on(listening.stop(WITHIN));
            runtime.shutdown_background();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// What a run ended with.
struct Ran {
    drained: Drained,
    ready: Result<(), Unready>,
    stats: quant_live::LiveStats,
    told: Vec<String>,
    entries: Vec<JournalEntry>,
}

/// Deadlines short enough for a test, the first of which an order still
/// working runs out.
const QUICK: Deadlines = Deadlines {
    settle: Duration::from_millis(300),
    answers: Duration::from_millis(300),
    pause: Duration::from_millis(5),
};

/// The order requests, as `METHOD n`.
fn requests(seen: &[Seen]) -> Vec<String> {
    seen.iter()
        .map(|s| format!("{} {}", s.method, s.names().unwrap_or_default()))
        .collect()
}

/// The journal's lines, by kind, for the orders: `kind id`.
fn kinds(entries: &[JournalEntry]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|entry| match entry {
            JournalEntry::Submitted {
                client_order_id, ..
            } => Some(format!("submitted {}", client_order_id.0)),
            JournalEntry::Placed {
                client_order_id, ..
            } => Some(format!("placed {}", client_order_id.0)),
            JournalEntry::Accepted {
                client_order_id, ..
            } => Some(format!("accepted {}", client_order_id.0)),
            JournalEntry::Filled {
                client_order_id, ..
            } => Some(format!("filled {}", client_order_id.0)),
            JournalEntry::Rejected {
                client_order_id, ..
            } => Some(format!("rejected {}", client_order_id.0)),
            JournalEntry::Refused {
                client_order_id, ..
            } => Some(format!("refused {}", client_order_id.0)),
            JournalEntry::Doubt {
                client_order_id,
                change,
                ..
            } => Some(match change {
                DoubtChange::Raised { cause, .. } => {
                    format!("doubt {} raised {cause:?}", client_order_id.0)
                }
                DoubtChange::Cleared { .. } => format!("doubt {} cleared", client_order_id.0),
            }),
            JournalEntry::CancelRequested {
                client_order_id, ..
            } => Some(format!("cancel_requested {}", client_order_id.0)),
            JournalEntry::NeverLanded {
                client_order_id, ..
            } => Some(format!("never_landed {}", client_order_id.0)),
            JournalEntry::Cancelled {
                client_order_id, ..
            } => Some(format!("cancelled {}", client_order_id.0)),
            JournalEntry::CancelRefused {
                client_order_id, ..
            } => Some(format!("cancel_refused {}", client_order_id.0)),
            JournalEntry::Stopped { working, .. } => Some(format!(
                "stopped {:?}",
                working.iter().map(|id| id.0).collect::<Vec<_>>()
            )),
            JournalEntry::Started { .. }
            | JournalEntry::Checkpoint { .. }
            | JournalEntry::Orphaned { .. }
            | JournalEntry::Tripped { .. }
            | JournalEntry::Blind { .. }
            | JournalEntry::Note { .. }
            | JournalEntry::Foreign { .. }
            | JournalEntry::Account { .. }
            | JournalEntry::Anomaly { .. }
            | JournalEntry::Stray { .. }
            | JournalEntry::Sighted { .. }
            | JournalEntry::Balances { .. } => None,
        })
        .collect()
}

/// A market buy of 0.001 that the stream reports accepted and filled in one
/// trade, as the fixtures write the venue's frames.
fn filled(id: u64) -> Vec<Vec<u8>> {
    let order = venue_order_id(id);
    vec![
        fixtures::new_order(&ours(id), order, "0.001"),
        fixtures::trade(&ours(id), order, "0.001", "0.001", "0.001", 7),
    ]
}

#[test]
fn a_market_buy_is_placed_accepted_and_filled_and_the_journal_accounts_for_it() {
    // Submit; REST says `Placed`; the stream says NEW and then the trade.
    // The strategy is told the acceptance and the fill once each, the journal
    // holds the venue's ids on each line, `runlog check` finds nothing to
    // flag, and a closing checkpoint of the engine's portfolio agrees with a
    // fold of the fills, which is what `reconcile` checks.
    let mut rig = Rig::new(Setup::default());
    let [push, read] = rig.push(filled(1));
    let steps = vec![
        rig.heard(),
        Step::Market,
        rig.answered(1),
        push,
        read,
        rig.told("filled 1 0.001 at 76650.12 left 0"),
    ];
    let ran = rig.run(steps, vec![vec![Act::Buy]], &QUICK);
    let requests = requests(&rig.rest.order_requests());
    rig.finish();

    assert_eq!(requests, ["POST 1"]);
    assert_eq!(
        ran.told,
        ["accepted 1", "filled 1 0.001 at 76650.12 left 0"],
        "told each once"
    );
    assert_eq!(
        kinds(&ran.entries),
        [
            "submitted 1",
            "placed 1",
            "accepted 1",
            "filled 1",
            "stopped []"
        ]
    );
    let venue_id = quant_core::execution::VenueOrderId(venue_order_id(1).to_string());
    for entry in &ran.entries {
        if let JournalEntry::Placed { venue_order_id, .. } = entry {
            assert_eq!(venue_order_id, &venue_id);
        } else if let JournalEntry::Accepted { venue_order_id, .. } = entry {
            assert_eq!(venue_order_id.as_ref(), Some(&venue_id));
        } else if let JournalEntry::Filled {
            venue_order_id,
            trade_id,
            remaining,
            ..
        } = entry
        {
            assert_eq!(venue_order_id.as_ref(), Some(&venue_id));
            assert_eq!(trade_id.as_ref().map(|t| t.0.as_str()), Some("7"));
            assert_eq!(remaining.map(quant_core::fixed::Qty::is_zero), Some(true));
        }
    }
    let report = runlog::check(&ran.entries);
    assert!(report.is_complete(), "{:#?}", report.findings);
    assert_eq!(report.decisions, 1);
    assert_eq!(journal::agrees(&ran.entries), journal::Agreement::Agrees);
    assert_eq!(ran.ready, Err(Unready::QueueFull), "stopped sending");
    assert!(ran.drained.settled && ran.drained.answered);
}

#[test]
fn a_rest_refusal_reaches_the_strategy_as_a_rejection_alone_with_the_venues_code() {
    let mut rig = Rig::new(Setup {
        script: Script {
            orders: [Reply::refuse(
                400,
                -2010,
                "Account has insufficient balance for requested action.",
            )]
            .into(),
            ..Script::default()
        },
        ..Setup::default()
    });
    let told = Arc::clone(&rig.told);
    let steps = vec![
        rig.heard(),
        Step::Market,
        Step::Until(
            "the rejection",
            Box::new(move || !told.lock().expect("told").is_empty()),
        ),
    ];
    let ran = rig.run(steps, vec![vec![Act::Buy]], &QUICK);
    rig.finish();

    assert_eq!(ran.told.len(), 1, "{:?}", ran.told);
    assert!(ran.told[0].starts_with("rejected 1 "), "{:?}", ran.told);
    assert_eq!(
        kinds(&ran.entries),
        ["submitted 1", "rejected 1", "stopped []"]
    );
    let cited = ran.entries.iter().find_map(|entry| {
        if let JournalEntry::Rejected { cited, .. } = entry {
            cited.clone()
        } else {
            None
        }
    });
    assert_eq!(
        cited,
        Some(Cited {
            http_status: Some(400),
            code: -2010,
            msg: quant_core::report::VenueText::new(
                "Account has insufficient balance for requested action."
            ),
        })
    );
    assert!(runlog::check(&ran.entries).is_complete());
    assert_eq!(ran.drained.working, []);
}

#[test]
fn an_order_that_times_out_is_in_doubt_until_the_stream_clears_it() {
    // The venue takes the order and never answers; the client gives up, and
    // the order's outcome is unknown: in doubt, never rejected. The stream
    // then reports it, which clears the doubt and tells the strategy.
    let mut rig = Rig::new(Setup {
        script: Script {
            orders: [Reply::Silence].into(),
            ..Script::default()
        },
        order_timeout: Some(Duration::from_millis(300)),
        ..Setup::default()
    });
    let [push, read] = rig.push(filled(1));
    let steps = vec![
        rig.heard(),
        Step::Market,
        rig.answered(1),
        push,
        read,
        rig.told("filled 1 0.001 at 76650.12 left 0"),
    ];
    let ran = rig.run(steps, vec![vec![Act::Buy]], &QUICK);
    rig.finish();

    assert_eq!(
        ran.told,
        ["accepted 1", "filled 1 0.001 at 76650.12 left 0"]
    );
    assert_eq!(
        kinds(&ran.entries),
        [
            "submitted 1",
            "doubt 1 raised NoAnswer",
            "doubt 1 cleared",
            "accepted 1",
            "filled 1",
            "stopped []"
        ]
    );
    assert!(runlog::check(&ran.entries).is_complete());
    assert_eq!(journal::agrees(&ran.entries), journal::Agreement::Agrees);
}

#[test]
fn a_full_queue_answers_queue_full_and_a_held_cancel_goes_before_later_orders() {
    // Order 1 is held at the server. Four more fill the queue; a sixth is
    // refused `QueueFull` before anything is written but the refusal; a
    // cancel of order 1 then waits in the shell, and a seventh order is
    // refused while it does. Released, the cancel goes out ahead of the
    // eighth order, submitted after it.
    let mut rig = Rig::new(Setup {
        script: Script {
            orders: [Reply::Hold].into(),
            ..Script::default()
        },
        ..Setup::default()
    });
    let rest = Arc::clone(&rig.rest);
    let steps = vec![
        rig.heard(),
        Step::Market,
        rig.rest_saw(1),
        Step::Market,
        Step::Market,
        Step::Do(Box::new(move || rest.release())),
        rig.answered(6),
        Step::Market,
        rig.answered(7),
    ];
    let acts = vec![
        vec![Act::Buy],
        vec![
            Act::Buy,
            Act::Buy,
            Act::Buy,
            Act::Buy,
            Act::Buy,
            Act::Cancel(1),
        ],
        vec![Act::Buy],
        vec![Act::Buy],
    ];
    let ran = rig.run(steps, acts, &QUICK);
    let seen = rig.rest.order_requests();
    rig.finish();

    assert_eq!(
        requests(&seen),
        ["POST 1", "POST 2", "POST 3", "POST 4", "POST 5", "DELETE 1", "POST 8"]
    );
    assert_eq!(ran.stats.held, 1);
    let refused: Vec<(u64, RefusedBy)> = ran
        .entries
        .iter()
        .filter_map(|entry| {
            if let JournalEntry::Refused {
                client_order_id,
                by,
                ..
            } = entry
            {
                Some((client_order_id.0, *by))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        refused,
        [
            (6, RefusedBy::Venue(Unready::QueueFull)),
            (7, RefusedBy::Venue(Unready::QueueFull))
        ]
    );
    assert_eq!(
        ran.told,
        ["rejected 6 Venue", "rejected 7 Venue"],
        "refused, and told only that the venue would not take them"
    );
}

#[test]
fn an_order_placed_and_never_mentioned_goes_into_doubt_when_its_timer_fires() {
    // Decision 6: `Placed`, and then a healthy stream says nothing about the
    // order. Its timer fires and the order is in doubt for `fills_missed`.
    let mut rig = Rig::new(Setup {
        unheard_after: Some(Duration::from_millis(150)),
        ..Setup::default()
    });
    let worker = Arc::clone(&rig.worker);
    let steps = vec![
        rig.heard(),
        Step::Market,
        rig.answered(1),
        Step::Until("the timer", Box::new(move || worker.unheard() >= 1)),
    ];
    let ran = rig.run(steps, vec![vec![Act::Buy]], &QUICK);
    rig.finish();

    assert_eq!(
        kinds(&ran.entries),
        [
            "submitted 1",
            "placed 1",
            "doubt 1 raised FillsMissed",
            "stopped [1]"
        ]
    );
    assert_eq!(ran.stats.unheard, 1);
    assert!(ran.entries.iter().any(|entry| matches!(
        entry,
        JournalEntry::Doubt {
            change: DoubtChange::Raised {
                cause: DoubtCause::FillsMissed,
                ..
            },
            ..
        }
    )));
    assert_eq!(ran.ready, Err(Unready::InDoubt));
}

#[test]
fn an_order_the_stream_has_mentioned_is_not_put_in_doubt_by_its_timer() {
    // The other side: the stream reports the order before REST answers, so
    // by the time `Placed` starts the timer the venue is known to have it,
    // and the timer firing changes nothing, whenever it lands.
    let mut rig = Rig::new(Setup {
        script: Script {
            orders: [Reply::Hold].into(),
            ..Script::default()
        },
        unheard_after: Some(Duration::from_millis(150)),
        ..Setup::default()
    });
    let (rest, worker) = (Arc::clone(&rig.rest), Arc::clone(&rig.worker));
    let [push, read] = rig.push(vec![fixtures::new_order(
        &ours(1),
        venue_order_id(1),
        "0.001",
    )]);
    let steps = vec![
        rig.heard(),
        Step::Market,
        rig.rest_saw(1),
        push,
        read,
        rig.told("accepted 1"),
        Step::Do(Box::new(move || rest.release())),
        Step::Until("the timer", Box::new(move || worker.unheard() >= 1)),
    ];
    let ran = rig.run(steps, vec![vec![Act::Buy]], &QUICK);
    rig.finish();

    assert_eq!(ran.stats.unheard, 1, "the timer fired");
    assert_eq!(
        kinds(&ran.entries),
        ["submitted 1", "accepted 1", "placed 1", "stopped [1]"]
    );
    assert_eq!(
        ran.ready,
        Err(Unready::QueueFull),
        "stopped, and in no doubt"
    );
}

#[test]
fn a_shutdown_with_a_worker_that_never_answers_ends_at_its_deadline() {
    // The venue takes the order and never answers, and the client waits its
    // full twenty seconds: both waits end at their deadlines, and the order
    // is still working, written into `stopped`.
    let mut rig = Rig::new(Setup {
        script: Script {
            orders: [Reply::Silence].into(),
            ..Script::default()
        },
        ..Setup::default()
    });
    let steps = vec![rig.heard(), Step::Market, rig.rest_saw(1)];
    let started = Instant::now();
    let ran = rig.run(steps, vec![vec![Act::Buy]], &QUICK);
    let took = started.elapsed();
    rig.finish();

    assert!(took < Duration::from_secs(15), "{took:?}");
    assert!(!ran.drained.settled && !ran.drained.answered);
    assert_eq!(ran.drained.outstanding, 1);
    assert_eq!(ran.drained.working, [ClientOrderId(1)]);
    assert_eq!(kinds(&ran.entries), ["submitted 1", "stopped [1]"]);
}

#[test]
fn a_shutdown_after_the_runtime_has_gone_does_not_hang() {
    // The runtime goes before the drain: the worker and the listener with it.
    // Nothing answers and nothing can, and the drain still ends.
    let mut rig = Rig::new(Setup {
        script: Script {
            orders: [Reply::Silence].into(),
            ..Script::default()
        },
        ..Setup::default()
    });
    let runtime = Arc::new(Mutex::new(rig.runtime.take()));
    let gone = Arc::clone(&runtime);
    let steps = vec![
        rig.heard(),
        Step::Market,
        rig.rest_saw(1),
        Step::Do(Box::new(move || {
            // Waits for the runtime's threads to drop their tasks, so that
            // the worker and the listener are certainly gone after it.
            if let Some(runtime) = gone.lock().expect("runtime").take() {
                runtime.shutdown_timeout(WITHIN);
            }
        })),
    ];
    let started = Instant::now();
    let ran = rig.run(steps, vec![vec![Act::Buy]], &QUICK);
    assert!(started.elapsed() < Duration::from_secs(15));
    assert!(runtime.lock().expect("runtime").is_none(), "it went");

    assert!(ran.stats.worker_gone, "{:?}", ran.stats);
    assert!(!ran.drained.answered);
    assert_eq!(ran.drained.working, [ClientOrderId(1)]);
    assert_eq!(ran.ready, Err(Unready::StreamDown), "the listener went too");
}

/// One market buy, through `venue`, the strategy told what it was told.
fn told_through<V: quant_engine::ExecutionVenue>(
    venue: V,
    source: impl EventSource,
    instrument: InstrumentId,
) -> Vec<String> {
    let told = Told::default();
    let strategy = Scripted {
        instrument,
        acts: vec![vec![Act::Buy]].into(),
        told: Arc::clone(&told),
    };
    let mut engine = Engine::new(
        source,
        venue,
        AllowAll,
        strategy,
        "100".parse().expect("cash"),
    );
    engine.run().expect("ran");
    let told = told.lock().expect("told").clone();
    told
}

/// A book with an ask at the fixtures' trade price, then a print.
#[derive(Debug)]
struct Book(VecDeque<MarketEvent>);

impl EventSource for Book {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        self.0.pop_front().map(Ok)
    }
}

#[test]
fn a_strategy_cannot_tell_this_venue_from_the_simulator() {
    // engine-contract §11: one lifecycle, a market buy filled in one trade,
    // through the simulator and through the live venue, tells the strategy
    // the same events.
    let instrument = InstrumentRegistry::new().register(fixtures::btcusdt());
    let meta = |seq: u64| EventMeta {
        instrument,
        exchange_ts: Ts::from_nanos(i64::try_from(seq).expect("small") * 1_000),
        local_recv_ts: Ts::from_nanos(i64::try_from(seq).expect("small") * 1_000),
        ingest_seq: seq,
    };
    let level = |px: &str| Level {
        px: px.parse().expect("px"),
        qty: "1".parse().expect("qty"),
    };
    let book = Book(
        vec![
            MarketEvent::BookSnapshot(BookSnapshot {
                meta: meta(1),
                last_update_id: 1,
                bids: vec![level("76650.11")],
                asks: vec![level("76650.12")],
            }),
            MarketEvent::Trade(Trade {
                meta: meta(2),
                px: "76650.12".parse().expect("px"),
                qty: "0.01".parse().expect("qty"),
                aggressor: Side::Buy,
                venue_trade_id: 1,
            }),
        ]
        .into(),
    );
    let simulated = told_through(SimulatedVenue::new(), book, instrument);

    let mut rig = Rig::new(Setup::default());
    let [push, read] = rig.push(filled(1));
    let steps = vec![
        rig.heard(),
        Step::Market,
        rig.answered(1),
        push,
        read,
        rig.told("filled 1 0.001 at 76650.12 left 0"),
    ];
    let live = rig.run(steps, vec![vec![Act::Buy]], &QUICK).told;
    rig.finish();

    assert_eq!(
        simulated,
        ["accepted 1", "filled 1 0.001 at 76650.12 left 0"]
    );
    assert_eq!(live, simulated);
}
