//! Item 7's acceptance: the merge machine, wrapped as an `ExecutionVenue`,
//! driven into a real `Engine` with the real `JournalWriter`, through every
//! interleaving of the REST answers and the stream's records
//! (`docs/live-run.md` §5, *`LiveVenue`, decided before the code*, §5).
//!
//! Here because this is the one crate that may link the machine, the engine,
//! the writer and `runlog::check` together: it sits at the top of the graph and
//! nothing depends on it.
//!
//! # What a scenario is
//!
//! One order's truth: what REST answered, a cancel's answer if there is a
//! cancel, and the stream's records for it in socket order. The scenarios are
//! generated: a set of lifecycles, crossed with every `WhyUnknown` where the
//! design asks for each, and each of those crossed with a lost stream (a
//! `Blind` and the resubscribe's answer, or an overflow) placed before, among
//! and after the order's records, which is where a deferred cause meets a
//! stream report before its REST answer. Every interleaving of the REST
//! answers among the stream's records is then generated too, each source kept
//! in its own order, and each is stamped by position so the engine's clock
//! moves sanely.
//!
//! For every interleaving of one scenario: the strategy is told the same
//! events, the journal holds the same lines as a multiset (without `at`), the
//! portfolio, the working orders and `ready` end the same, `ready` ends
//! `Diverged` exactly when `journal::diverged` holds over the journal, and
//! `runlog::check` reports the same findings. No order has two terminals and
//! no trade id is booked twice. A contradiction is not a consistent truth, so
//! the contradictions are run through every interleaving too and held only to
//! the invariants and to diverging in every one.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use quant_backtest::record::{write, JournalWriter};
use quant_binance::orders::{
    CancelReply, FilterName, Placement, Refusal, RefusalCause, VenueError, WhyUnknown,
};
use quant_binance::user_data::Translator;
use quant_core::event::{EventMeta, GapCause, MarketEvent, Side, Trade};
use quant_core::execution::{
    ClientOrderId, ExecutionEvent, OrderKind, OrderRequest, TimeInForce, VenueOrderId,
};
use quant_core::fixed::Notional;
use quant_core::instrument::{InstrumentId, InstrumentRegistry};
use quant_core::report::{DoubtCause, EndedBy, VenueReport};
use quant_core::source::{EventSource, SourceError, Wake};
use quant_core::time::Ts;
use quant_engine::journal::{self, Journal, JournalEntry};
use quant_engine::{AllowAll, Context, Engine, ExecutionVenue, Portfolio, Strategy, Unready};
use quant_explain::runlog::{self, Finding};
use quant_live::fixtures::{self, ours};
use quant_live::{Input, Merge, Queue};

/// One thing heard, in a script.
#[derive(Debug, Clone)]
enum Item {
    /// A REST answer, from the request worker.
    Rest(Input),
    /// A recorded account frame.
    Frame(Vec<u8>),
    /// The account feed's `Blind`.
    Blind(GapCause),
}

/// The scripted account feed and request worker, and the machine behind
/// them: what the next pull request's shell will be, without the transport.
#[derive(Debug)]
struct Scripted {
    merge: Merge,
    instrument: InstrumentId,
    /// Stamped by position, one handed to the machine per poll.
    script: VecDeque<(Ts, Item)>,
    /// What the machine said between polls.
    pending: Vec<VenueReport>,
    /// `ready` after each item, for the tests that read it as it went.
    readiness: Vec<Result<(), Unready>>,
}

impl ExecutionVenue for Scripted {
    fn ready(&self, request: &OrderRequest) -> Result<(), Unready> {
        self.merge.ready(request.instrument, Queue::Room)
    }

    fn submit(&mut self, client_order_id: ClientOrderId, request: &OrderRequest, now: Ts) {
        self.merge.step(
            now,
            Input::Submitted {
                id: client_order_id,
                qty: request.qty,
            },
            &mut self.pending,
        );
    }

    /// The cancel's answer is in the script.
    fn cancel(&mut self, _client_order_id: ClientOrderId, _now: Ts) {}

    fn poll(&mut self, _now: Ts, out: &mut Vec<VenueReport>) {
        out.append(&mut self.pending);
        let Some((at, item)) = self.script.pop_front() else {
            return;
        };
        match item {
            Item::Rest(input) => self.merge.step(at, input, out),
            Item::Frame(payload) => self.merge.frame(at, &payload, out),
            Item::Blind(cause) => self.merge.step(at, Input::Blind(cause), out),
        }
        self.readiness
            .push(self.merge.ready(self.instrument, Queue::Room));
    }
}

/// One market event, then an idle wake per scripted item, then the end.
#[derive(Debug)]
struct Source {
    market: Option<MarketEvent>,
    idles: usize,
}

impl EventSource for Source {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        self.market.take().map(Ok)
    }

    fn next_event_timeout(
        &mut self,
        _timeout: core::time::Duration,
    ) -> Option<Result<Wake, SourceError>> {
        if let Some(event) = self.market.take() {
            return Some(Ok(Wake::Event(event)));
        }
        if self.idles == 0 {
            return None;
        }
        self.idles -= 1;
        Some(Ok(Wake::Idle))
    }
}

/// Submits its orders on the first market event, cancels the ones it was told
/// to, and writes down everything it is told, without stamps.
#[derive(Debug)]
struct Probe {
    orders: Vec<(OrderRequest, bool)>,
    told: Vec<String>,
}

impl Strategy for Probe {
    fn on_market_event(&mut self, _event: &MarketEvent, ctx: &mut Context<'_>) {
        for (request, cancel) in std::mem::take(&mut self.orders) {
            let id = ctx.submit(request);
            if cancel {
                ctx.cancel(id);
            }
        }
    }

    fn on_execution(&mut self, event: &ExecutionEvent, _ctx: &mut Context<'_>) {
        self.told.push(match event {
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
        });
    }
}

/// What one run ended with, everything compared across interleavings.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Outcome {
    told: Vec<String>,
    /// The journal's lines without `at`, sorted: a multiset.
    lines: Vec<String>,
    portfolio: Portfolio,
    working: Vec<ClientOrderId>,
    ready: Result<(), Unready>,
    /// `runlog::check`'s findings without their stamps, sorted.
    findings: Vec<String>,
}

/// What one run left, beside its outcome.
struct Ran {
    outcome: Outcome,
    entries: Vec<JournalEntry>,
    readiness: Vec<Result<(), Unready>>,
}

static RUNS: AtomicUsize = AtomicUsize::new(0);

/// A journal file of its own, under a directory named for this process.
fn journal_path() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("quant-explain-live-merge-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir.join(format!("{}.jsonl", RUNS.fetch_add(1, Ordering::Relaxed)))
}

fn instrument() -> InstrumentId {
    InstrumentRegistry::new().register(fixtures::btcusdt())
}

/// A market order, or a limit order resting below the market.
fn order(qty: &str, limit: bool) -> OrderRequest {
    OrderRequest {
        instrument: instrument(),
        side: Side::Buy,
        qty: qty.parse().expect("qty"),
        kind: if limit {
            OrderKind::Limit {
                limit: "70000".parse().expect("limit"),
            }
        } else {
            OrderKind::Market
        },
        time_in_force: TimeInForce::Gtc,
    }
}

/// Drive `script` through a real engine and a real journal.
fn run(orders: &[(OrderRequest, bool)], script: &[Item]) -> Ran {
    let instrument = instrument();
    let path = journal_path();
    let journal = Arc::new(Mutex::new(Journal::open(&path).expect("journal opens")));
    write(
        &journal,
        &JournalEntry::Started {
            at: Ts::from_nanos(0),
            cash: "100".parse().expect("cash"),
            schema: journal::SCHEMA,
            run: Some(fixtures::RUN.to_owned()),
        },
    )
    .expect("started");
    let writer = JournalWriter::new(Arc::clone(&journal), "BTCUSDT".to_owned(), 0);

    // The account is heard at the first wake, before the strategy submits;
    // then one item per wake, stamped by position.
    let mut stamped = VecDeque::new();
    stamped.push_back((Ts::from_nanos(1_000), Item::Frame(fixtures::subscribed())));
    for (n, item) in (2_i64..).zip(script.iter().cloned()) {
        stamped.push_back((Ts::from_nanos(n * 1_000), item));
    }
    let venue = Scripted {
        merge: Merge::new(
            instrument,
            Translator::new(fixtures::run(), &fixtures::btcusdt()).expect("assets named"),
        ),
        instrument,
        script: stamped,
        pending: Vec::new(),
        readiness: Vec::new(),
    };
    let market = MarketEvent::Trade(Trade {
        meta: EventMeta {
            instrument,
            exchange_ts: Ts::from_nanos(900),
            local_recv_ts: Ts::from_nanos(1_000),
            ingest_seq: 1,
        },
        px: "76650.12".parse().expect("px"),
        qty: "0.01".parse().expect("qty"),
        aggressor: Side::Buy,
        venue_trade_id: 1,
    });
    let source = Source {
        market: Some(market),
        idles: script.len() + 1,
    };
    let probe = Probe {
        orders: orders.to_vec(),
        told: Vec::new(),
    };
    let cash: Notional = "100".parse().expect("cash");
    let mut engine =
        Engine::new(source, venue, AllowAll, probe, cash).observing_fills(Box::new(writer));
    engine.run().expect("the script ends cleanly");
    // What the engine believes, for `journal::agrees` to set against a fold
    // of the lines: the check `reconcile` and `explain --check-journal` make.
    let end = i64::try_from(script.len()).expect("a short script") + 2;
    write(
        &journal,
        &JournalEntry::checkpoint(Ts::from_nanos(end * 1_000), engine.portfolio()),
    )
    .expect("checkpoint");

    let entries = journal::read(&path).expect("journal reads").entries;
    std::fs::remove_file(&path).expect("journal removed");
    let mut lines: Vec<String> = entries
        .iter()
        .map(|entry| {
            let mut value = serde_json::to_value(entry).expect("serializes");
            value.as_object_mut().expect("an object").remove("at");
            value.to_string()
        })
        .collect();
    lines.sort();
    let mut findings: Vec<String> = runlog::check(&entries)
        .findings
        .iter()
        .map(finding)
        .collect();
    findings.sort();
    let venue = engine.venue();
    Ran {
        outcome: Outcome {
            told: engine.strategy().told.clone(),
            lines,
            portfolio: engine.portfolio().clone(),
            working: engine.working(),
            ready: venue.merge.ready(instrument, Queue::Room),
            findings,
        },
        readiness: venue.readiness.clone(),
        entries,
    }
}

/// A finding, without its stamps.
fn finding(finding: &Finding) -> String {
    match finding {
        Finding::MissingDecision { client_order_id } => format!("missing {client_order_id}"),
        Finding::ClaimDoesNotFold { kind, .. } => format!("claim does not fold {kind}"),
        Finding::ClaimWentBackwards { kind, .. } => format!("claim went backwards {kind}"),
        Finding::DuplicateDecision {
            client_order_id, ..
        } => format!("duplicate {client_order_id}"),
        Finding::UnattributedFill {
            client_order_id,
            at: _,
        } => format!("orphan {client_order_id}"),
        Finding::Stray {
            client_order_id,
            kind,
            at: _,
        } => format!("stray {client_order_id} {kind:?}"),
        Finding::ForeignOrder {
            client_id, trades, ..
        } => format!("foreign {client_id} {trades}"),
        Finding::LeftInDoubt {
            client_order_id,
            since: _,
            cause,
        } => format!("in doubt {client_order_id} {cause:?}"),
        Finding::Anomaly {
            what,
            client_order_id,
            at: _,
        } => format!("anomaly {client_order_id:?} {what:?}"),
        Finding::TornBeforeRestart { line } => format!("torn {line}"),
        Finding::Unwatched {
            client_order_id,
            lost_at: _,
        } => format!("unwatched {client_order_id}"),
    }
}

/// The invariants every interleaving keeps, consistent or not.
fn invariants(name: &str, ran: &Ran) {
    let outcome = &ran.outcome;
    assert_eq!(
        journal::agrees(&ran.entries),
        journal::Agreement::Agrees,
        "{name}: the engine's checkpoint and a fold of the journal disagree"
    );
    assert_eq!(
        outcome.ready == Err(Unready::Diverged),
        journal::diverged(&ran.entries),
        "{name}: the machine's latch and the journal's disagree: {outcome:#?}"
    );
    // One terminal per order, to the strategy and in the record.
    let mut told: BTreeMap<String, usize> = BTreeMap::new();
    for event in &outcome.told {
        let mut words = event.split(' ');
        let kind = words.next().unwrap_or_default();
        let id = words.next().unwrap_or_default().to_owned();
        let terminal = match kind {
            "rejected" | "cancelled" => true,
            "filled" => event.ends_with("left 0"),
            _ => false,
        };
        if terminal {
            *told.entry(id).or_default() += 1;
        }
    }
    assert!(told.values().all(|&n| n == 1), "{name}: {:?}", outcome.told);
    let mut ends: BTreeMap<u64, usize> = BTreeMap::new();
    let mut trades = Vec::new();
    for entry in &ran.entries {
        if let JournalEntry::Rejected {
            client_order_id, ..
        }
        | JournalEntry::NeverLanded {
            client_order_id, ..
        }
        | JournalEntry::Cancelled {
            client_order_id, ..
        } = entry
        {
            *ends.entry(client_order_id.0).or_default() += 1;
        } else if let JournalEntry::Filled {
            client_order_id,
            remaining,
            trade_id,
            ..
        } = entry
        {
            if remaining.is_some_and(quant_core::fixed::Qty::is_zero) {
                *ends.entry(client_order_id.0).or_default() += 1;
            }
            trades.push(trade_id.clone());
        }
    }
    assert!(ends.values().all(|&n| n == 1), "{name}: {:?}", ran.entries);
    let booked = trades.len();
    trades.sort();
    trades.dedup();
    assert_eq!(trades.len(), booked, "{name}: a trade id booked twice");
}

/// What the check may say about a consistent truth, set against what the
/// machine says: an order in doubt exactly when `ready` says so, unless it
/// latched first, and a duplicate trade only where one was resent. The
/// equality across interleavings cannot see a finding every interleaving
/// shares.
fn findings_fit(name: &str, outcome: &Outcome) {
    let in_doubt = outcome.findings.iter().any(|f| f.starts_with("in doubt "));
    let duplicate = outcome
        .findings
        .iter()
        .any(|f| f.ends_with(" DuplicateTrade"));
    for finding in &outcome.findings {
        assert!(
            finding.starts_with("in doubt ") || finding.ends_with(" DuplicateTrade"),
            "{name}: {finding}"
        );
    }
    if outcome.ready == Ok(()) {
        assert!(!in_doubt, "{name}: {:?}", outcome.findings);
    } else if outcome.ready == Err(Unready::InDoubt) {
        assert!(in_doubt, "{name}: {:?}", outcome.findings);
    }
    assert_eq!(duplicate, name.contains("resent"), "{name}");
}

/// Every way to merge `rest` into `stream`, each kept in its own order.
fn interleavings(rest: &[Item], stream: &[Item]) -> Vec<Vec<Item>> {
    if rest.is_empty() {
        return vec![stream.to_vec()];
    }
    if stream.is_empty() {
        return vec![rest.to_vec()];
    }
    let mut out = Vec::new();
    for mut tail in interleavings(&rest[1..], stream) {
        tail.insert(0, rest[0].clone());
        out.push(tail);
    }
    for mut tail in interleavings(rest, &stream[1..]) {
        tail.insert(0, stream[0].clone());
        out.push(tail);
    }
    out
}

/// One order's truth.
#[derive(Clone)]
struct Scenario {
    name: String,
    order: OrderRequest,
    cancel: bool,
    rest: Vec<Input>,
    stream: Vec<Item>,
    /// How every interleaving must end: `ready`, and the fills booked. `None`
    /// for a contradiction, which only diverges.
    ends: Option<(Result<(), Unready>, u64)>,
}

const ID: u64 = 1;
const VENUE_ID: u64 = 100;

fn placed() -> Input {
    Input::Placement {
        id: ClientOrderId(ID),
        placement: Placement::Placed {
            order_id: VenueOrderId(VENUE_ID.to_string()),
            client_order_id: ours(ID),
            transact_time_ms: 1_499_405_658_657,
        },
    }
}

fn unknown(why: WhyUnknown) -> Input {
    Input::Placement {
        id: ClientOrderId(ID),
        placement: Placement::Unknown { why },
    }
}

fn refused() -> Input {
    Input::Placement {
        id: ClientOrderId(ID),
        placement: Placement::Refused(Refusal {
            http_status: 400,
            code: -1013,
            msg: "Filter failure: NOTIONAL".to_owned(),
            cause: RefusalCause::Filter(FilterName::Notional),
        }),
    }
}

fn not_sent() -> Input {
    Input::Placement {
        id: ClientOrderId(ID),
        placement: Placement::NotSent {
            why: "connection refused".to_owned(),
        },
    }
}

fn cancel_reply(reply: CancelReply) -> Input {
    Input::CancelReply {
        id: ClientOrderId(ID),
        reply,
    }
}

fn not_open() -> CancelReply {
    CancelReply::NotOpen {
        http_status: 400,
        code: -2011,
        msg: "Unknown order sent.".to_owned(),
    }
}

/// Every `WhyUnknown`, every field distinct.
fn every_why_unknown() -> Vec<WhyUnknown> {
    vec![
        WhyUnknown::ServerError {
            http_status: 503,
            said: Some(VenueError {
                code: -1001,
                msg: "Internal error".to_owned(),
            }),
        },
        WhyUnknown::Unsettled {
            http_status: 400,
            code: -1007,
            msg: "Timeout".to_owned(),
        },
        WhyUnknown::IdInUse {
            id: ours(ID),
            http_status: 400,
            code: -2010,
            msg: "Duplicate order sent.".to_owned(),
        },
        WhyUnknown::UnreadableBody {
            http_status: 200,
            detail: "EOF".to_owned(),
        },
        WhyUnknown::OtherOrder {
            sent: ours(ID),
            echoed: ours(ID + 1),
        },
        WhyUnknown::UnexpectedStatus { http_status: 409 },
        WhyUnknown::TimedOut,
        WhyUnknown::Interrupted("reset".to_owned()),
    ]
}

/// The stream's records for an order of `qty`.
struct Records(&'static str);

impl Records {
    fn new_order(&self) -> Item {
        Item::Frame(fixtures::new_order(&ours(ID), VENUE_ID, self.0))
    }

    fn trade(&self, last: &str, filled: &str, trade_id: i64) -> Item {
        Item::Frame(fixtures::trade(
            &ours(ID),
            VENUE_ID,
            self.0,
            last,
            filled,
            trade_id,
        ))
    }

    fn ended(&self, filled: &str, how: EndedBy) -> Item {
        Item::Frame(fixtures::ended(&ours(ID), VENUE_ID, self.0, filled, how))
    }
}

/// The scenarios the design lists, generated.
#[allow(clippy::too_many_lines)]
fn scenarios() -> Vec<Scenario> {
    let mut out = Vec::new();
    let mut push = |name: String,
                    order: OrderRequest,
                    cancel: bool,
                    rest: Vec<Input>,
                    stream: Vec<Item>,
                    ends: Option<(Result<(), Unready>, u64)>| {
        out.push(Scenario {
            name,
            order,
            cancel,
            rest,
            stream,
            ends,
        });
    };
    let one = Records("0.001");
    let three = Records("0.003");
    let two = Records("0.002");

    push(
        "a market buy filled in one trade".to_owned(),
        order("0.001", false),
        false,
        vec![placed()],
        vec![one.new_order(), one.trade("0.001", "0.001", 7)],
        Some((Ok(()), 1)),
    );
    push(
        "a market buy filled in three trades".to_owned(),
        order("0.003", false),
        false,
        vec![placed()],
        vec![
            three.new_order(),
            three.trade("0.001", "0.001", 7),
            three.trade("0.001", "0.002", 8),
            three.trade("0.001", "0.003", 9),
        ],
        Some((Ok(()), 3)),
    );
    push(
        "filled in part, then expired".to_owned(),
        order("0.003", false),
        false,
        vec![placed()],
        vec![
            three.new_order(),
            three.trade("0.001", "0.001", 7),
            three.ended("0.001", EndedBy::Expired),
        ],
        Some((Ok(()), 1)),
    );
    push(
        "a resting limit cancelled, the cancel acknowledged".to_owned(),
        order("0.002", true),
        true,
        vec![
            placed(),
            cancel_reply(CancelReply::Acknowledged {
                order_id: VenueOrderId(VENUE_ID.to_string()),
            }),
        ],
        vec![two.new_order(), two.ended("0", EndedBy::Cancelled)],
        Some((Ok(()), 0)),
    );
    push(
        "a resting limit cancelled, the cancel answered not open".to_owned(),
        order("0.002", true),
        true,
        vec![placed(), cancel_reply(not_open())],
        vec![two.new_order(), two.ended("0", EndedBy::Cancelled)],
        Some((Ok(()), 0)),
    );
    push(
        "a cancel that lost the race to a fill".to_owned(),
        order("0.001", true),
        true,
        vec![placed(), cancel_reply(not_open())],
        vec![one.new_order(), one.trade("0.001", "0.001", 7)],
        Some((Ok(()), 1)),
    );
    push(
        "the last trade resent with an execution id of its own".to_owned(),
        order("0.001", false),
        false,
        vec![placed()],
        vec![
            one.new_order(),
            one.trade("0.001", "0.001", 7),
            Item::Frame(fixtures::trade_with(
                &ours(ID),
                VENUE_ID,
                "0.001",
                "0.001",
                "0.001",
                7,
                5_555,
                "USDT",
            )),
        ],
        Some((Ok(()), 1)),
    );
    push(
        "refused, with the stream silent".to_owned(),
        order("0.001", false),
        false,
        vec![refused()],
        vec![],
        Some((Ok(()), 0)),
    );
    push(
        "never sent, with the stream silent".to_owned(),
        order("0.001", false),
        false,
        vec![not_sent()],
        vec![],
        Some((Ok(()), 0)),
    );
    for why in every_why_unknown() {
        let latched = if matches!(why, WhyUnknown::IdInUse { .. }) {
            Err(Unready::Diverged)
        } else {
            Ok(())
        };
        let stuck = latched.and(Err(Unready::InDoubt));
        push(
            format!("unknown ({why}), the stream reporting"),
            order("0.001", false),
            false,
            vec![unknown(why.clone())],
            vec![one.new_order(), one.trade("0.001", "0.001", 7)],
            Some((latched, 1)),
        );
        push(
            format!("unknown ({why}), the stream silent"),
            order("0.001", false),
            false,
            vec![unknown(why.clone())],
            vec![],
            Some((stuck, 0)),
        );
    }
    // A report held behind a hole, against each REST answer but a refusal:
    // the hole after the order's NEW, so the order is heard and then held,
    // and the hole swallowing the NEW, so it is mentioned and never heard.
    let answers = std::iter::once(("placed".to_owned(), placed(), Err(Unready::InDoubt))).chain(
        every_why_unknown().into_iter().map(|why| {
            let stuck = if matches!(why, WhyUnknown::IdInUse { .. }) {
                Err(Unready::Diverged)
            } else {
                Err(Unready::InDoubt)
            };
            (format!("unknown ({why})"), unknown(why), stuck)
        }),
    );
    for (answer, input, stuck) in answers {
        for (hole, records) in [
            (
                "after its NEW",
                vec![
                    three.new_order(),
                    three.trade("0.001", "0.002", 8),
                    three.trade("0.001", "0.003", 9),
                ],
            ),
            (
                "from its first report",
                vec![
                    three.trade("0.001", "0.002", 8),
                    three.trade("0.001", "0.003", 9),
                ],
            ),
        ] {
            push(
                format!("a report held behind a hole {hole}, against {answer}"),
                order("0.003", false),
                false,
                vec![input.clone()],
                records,
                Some((stuck, 0)),
            );
        }
    }

    // Contradictions: not consistent truths, held to the invariants alone.
    push(
        "contradiction: refused, and the stream reported a fill".to_owned(),
        order("0.001", false),
        false,
        vec![refused()],
        vec![one.new_order(), one.trade("0.001", "0.001", 7)],
        None,
    );
    push(
        "contradiction: never sent, and the stream reported it".to_owned(),
        order("0.001", false),
        false,
        vec![not_sent()],
        vec![one.new_order()],
        None,
    );
    push(
        "contradiction: placed under another venue id".to_owned(),
        order("0.001", false),
        false,
        vec![Input::Placement {
            id: ClientOrderId(ID),
            placement: Placement::Placed {
                order_id: VenueOrderId("999".to_owned()),
                client_order_id: ours(ID),
                transact_time_ms: 1,
            },
        }],
        vec![one.new_order(), one.trade("0.001", "0.001", 7)],
        None,
    );
    push(
        "contradiction: a second NEW".to_owned(),
        order("0.001", false),
        false,
        vec![placed()],
        vec![one.new_order(), one.new_order()],
        None,
    );
    out
}

/// Each scenario as the design asks for it: as it is, and with the stream
/// lost and regained at every position among its records, by a disconnect
/// and the resubscribe's answer and by an overflow of our own copy.
fn variants(scenario: &Scenario) -> Vec<(String, Vec<Item>)> {
    let mut out = vec![(scenario.name.clone(), scenario.stream.clone())];
    let losses: [(&str, Vec<Item>); 2] = [
        (
            "a disconnect",
            vec![
                Item::Blind(GapCause::Disconnect),
                Item::Frame(fixtures::subscribed()),
            ],
        ),
        ("an overflow", vec![Item::Blind(GapCause::LocalOverflow)]),
    ];
    for (loss, items) in &losses {
        for at in 0..=scenario.stream.len() {
            let mut stream = scenario.stream.clone();
            for (k, item) in items.iter().enumerate() {
                stream.insert(at + k, item.clone());
            }
            out.push((format!("{}, with {loss} at {at}", scenario.name), stream));
        }
    }
    out
}

#[test]
fn every_interleaving_of_rest_and_the_stream_tells_and_records_the_same() {
    let mut runs = 0;
    let mut variants_run = 0;
    let scenarios = scenarios();
    let truths = scenarios.len();
    for scenario in scenarios {
        let rest: Vec<Item> = scenario.rest.iter().cloned().map(Item::Rest).collect();
        let orders = [(scenario.order, scenario.cancel)];
        for (name, stream) in variants(&scenario) {
            variants_run += 1;
            let mut first: Option<(Vec<Item>, Outcome)> = None;
            for script in interleavings(&rest, &stream) {
                let ran = run(&orders, &script);
                runs += 1;
                invariants(&name, &ran);
                let Some((ready, fills)) = &scenario.ends else {
                    // A contradiction: it latches in every interleaving, and
                    // the check names it.
                    assert_eq!(
                        ran.outcome.ready,
                        Err(Unready::Diverged),
                        "{name}: a contradiction must latch in every interleaving: {script:#?}"
                    );
                    assert!(
                        ran.outcome
                            .findings
                            .iter()
                            .any(|f| f.ends_with("Contradiction")),
                        "{name}: {:?}",
                        ran.outcome.findings
                    );
                    continue;
                };
                assert_eq!(&ran.outcome.ready, ready, "{name}: {script:#?}");
                assert_eq!(ran.outcome.portfolio.fills(), *fills, "{name}");
                findings_fit(&name, &ran.outcome);
                if let Some((before, outcome)) = &first {
                    assert_eq!(
                        &ran.outcome, outcome,
                        "{name}: two interleavings disagree.\nfirst: {before:#?}\nthen: {script:#?}"
                    );
                } else {
                    first = Some((script, ran.outcome));
                }
            }
        }
    }
    println!("{truths} truths, {variants_run} scenario variants, {runs} interleavings");
    assert!(runs > 1_000, "{runs}");
}

#[test]
fn losing_the_stream_puts_every_working_order_in_doubt_until_its_next_report() {
    // Item 7's second acceptance, `account_blind`. Two working orders, the
    // stream lost: `account { lost }` and one `account_blind` raise each;
    // `ready` StreamDown, then InDoubt after the resubscribe, then nothing
    // once each order's next report clears it. Seen red: a venue that skips
    // the raise leaves the check reporting both orders `unwatched`.
    let first = Records("0.003");
    let second = |item: Vec<u8>| Item::Frame(item);
    let placed2 = Input::Placement {
        id: ClientOrderId(2),
        placement: Placement::Placed {
            order_id: VenueOrderId("101".to_owned()),
            client_order_id: ours(2),
            transact_time_ms: 1,
        },
    };
    let orders = [(order("0.003", true), false), (order("0.003", true), false)];
    let lifecycle = |cleared_both: bool| {
        let mut script = vec![
            Item::Rest(placed()),
            Item::Rest(placed2.clone()),
            first.new_order(),
            second(fixtures::new_order(&ours(2), 101, "0.003")),
            Item::Blind(GapCause::Disconnect),
            Item::Frame(fixtures::subscribed()),
            first.trade("0.001", "0.001", 7),
        ];
        if cleared_both {
            script.push(second(fixtures::trade(
                &ours(2),
                101,
                "0.003",
                "0.001",
                "0.001",
                17,
            )));
        }
        script
    };

    let ran = run(&orders, &lifecycle(true));
    invariants("account_blind", &ran);
    // First, so that a venue that skipped the raise fails here, naming both
    // orders `unwatched`.
    assert_eq!(ran.outcome.findings, Vec::<String>::new());
    let lines: Vec<&JournalEntry> = ran
        .entries
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                JournalEntry::Account { .. } | JournalEntry::Doubt { .. }
            )
        })
        .collect();
    let blind = |id: u64| {
        lines.iter().any(|entry| {
            matches!(
                entry,
                JournalEntry::Doubt {
                    client_order_id,
                    change: quant_core::report::DoubtChange::Raised {
                        cause: DoubtCause::AccountBlind,
                        ..
                    },
                    ..
                } if client_order_id.0 == id
            )
        })
    };
    assert!(blind(1) && blind(2), "{lines:#?}");
    assert_eq!(
        lines.len(),
        1 + 1 + 2 + 1 + 2,
        "the opening regained, lost, two raises, regained, two clearings: {lines:#?}"
    );
    // After each item: placed, placed, NEW, NEW, the loss, the resubscribe,
    // order 1's trade, order 2's trade.
    assert_eq!(
        ran.readiness[1..],
        [
            Ok(()),
            Ok(()),
            Ok(()),
            Ok(()),
            Err(Unready::StreamDown),
            Err(Unready::InDoubt),
            Err(Unready::InDoubt),
            Ok(()),
        ],
        "the first entry is the session's opening subscription"
    );

    // Order 2 never reports again: the check flags it in doubt.
    let ran = run(&orders, &lifecycle(false));
    assert_eq!(ran.outcome.ready, Err(Unready::InDoubt));
    assert_eq!(ran.outcome.findings, ["in doubt 2 AccountBlind"]);
}

/// `frame` with its event's `field` made unreadable.
fn spoiled(frame: &[u8], field: &str) -> Vec<u8> {
    let mut value: serde_json::Value = serde_json::from_slice(frame).expect("json");
    value["event"][field] = serde_json::json!("oops");
    serde_json::to_vec(&value).expect("json")
}

#[test]
// One list of cases, read top to bottom.
#[allow(clippy::too_many_lines)]
fn the_machine_latches_exactly_when_the_journal_says_it_diverged() {
    // `journal::diverged` reads what was written; the machine latches from
    // what it heard; neither is derived from the other. Over the latches the
    // permutation scenarios do not reach, and the near misses that must not
    // latch, each run through the engine and the writer.
    let one = Records("0.001");
    let cases: Vec<(&str, Vec<Item>, bool)> = vec![
        (
            "a stranger's trade on our symbol",
            vec![Item::Frame(fixtures::trade(
                "web_1", 900, "1", "1", "1", 77,
            ))],
            true,
        ),
        (
            "a stranger's acceptance",
            vec![Item::Frame(fixtures::new_order("web_1", 900, "1"))],
            false,
        ),
        (
            "another run's trade",
            vec![Item::Frame(fixtures::trade(
                "q-r0-1", 901, "1", "1", "1", 78,
            ))],
            true,
        ),
        (
            "a fill for an id of this run never sent",
            vec![Item::Frame(fixtures::trade(
                &ours(9),
                909,
                "1",
                "1",
                "1",
                79,
            ))],
            true,
        ),
        (
            "an acceptance for an id of this run never sent",
            vec![Item::Frame(fixtures::new_order(&ours(9), 909, "1"))],
            false,
        ),
        (
            "a fill after the order's end",
            vec![
                Item::Rest(placed()),
                one.new_order(),
                one.ended("0", EndedBy::Expired),
                one.trade("0.001", "0.001", 7),
            ],
            true,
        ),
        (
            "a second end",
            vec![
                Item::Rest(placed()),
                one.new_order(),
                one.ended("0", EndedBy::Expired),
                one.ended("0", EndedBy::Cancelled),
            ],
            false,
        ),
        (
            "a NEW after the end",
            vec![
                Item::Rest(placed()),
                one.new_order(),
                one.ended("0", EndedBy::Expired),
                one.new_order(),
            ],
            false,
        ),
        (
            "an unreadable report",
            vec![Item::Rest(placed()), Item::Frame(b"not json".to_vec())],
            false,
        ),
        (
            "a cancel acknowledged for an order REST refused",
            vec![
                Item::Rest(refused()),
                Item::Rest(cancel_reply(CancelReply::Acknowledged {
                    order_id: VenueOrderId(VENUE_ID.to_string()),
                })),
            ],
            true,
        ),
        (
            "a stranger's trade on our symbol that cannot be read",
            vec![Item::Frame(spoiled(
                &fixtures::trade("web_1", 900, "1", "1", "1", 77),
                "L",
            ))],
            true,
        ),
        (
            "a stranger's acceptance that cannot be read",
            vec![Item::Frame(spoiled(
                &fixtures::new_order("web_1", 900, "1"),
                "S",
            ))],
            false,
        ),
        (
            "a cancel acknowledged for another venue id",
            vec![
                Item::Rest(placed()),
                Item::Rest(cancel_reply(CancelReply::Acknowledged {
                    order_id: VenueOrderId("999".to_owned()),
                })),
            ],
            true,
        ),
    ];
    for (name, script, latches) in cases {
        let ran = run(&[(order("0.001", false), false)], &script);
        invariants(name, &ran);
        assert_eq!(
            ran.outcome.ready == Err(Unready::Diverged),
            latches,
            "{name}: {:#?}",
            ran.entries
        );
        assert_eq!(journal::diverged(&ran.entries), latches, "{name}");
    }
}
