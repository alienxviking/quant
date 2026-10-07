//! Tests for the seam.
//!
//! Four of `docs/engine-contract.md` §7's criteria are properties of this crate
//! rather than of a strategy, and each is pinned here: the same strategy value
//! runs against two wirings, nothing reaches a venue past a refusing risk layer,
//! prices go away across a gap, and an order cannot trade against the event that
//! prompted it.

use quant_book::Book;
use quant_core::event::{BookDelta, EventMeta, Gap, GapCause, Level, MarketEvent, Side, Trade};
use quant_core::execution::{
    ClientOrderId, ExecutionEvent, Fill, OrderKind, OrderRequest, RejectReason, TimeInForce,
};
use quant_core::instrument::{
    Exchange, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
};
use quant_core::source::{EventSource, SourceError};
use quant_core::time::Ts;
use quant_core::Notional;

use crate::{AllowAll, Context, Engine, ExecutionVenue, Refusal, RiskLayer, Strategy};

/// Starting capital for the wiring tests. Any positive number; these tests are
/// about the seam, and the accounting has its own.
const CASH: Notional = Notional::from_raw(100 * quant_core::SCALE);

fn instrument() -> InstrumentId {
    InstrumentRegistry::new().register(InstrumentDef {
        exchange: Exchange::Binance,
        symbol: "BTCUSDT".to_owned(),
        base: "BTC".to_owned(),
        quote: "USDT".to_owned(),
        kind: InstrumentKind::Spot,
        tick_size: "0.01".parse().expect("tick"),
        lot_size: "0.00001".parse().expect("lot"),
        min_notional: "5".parse().expect("notional"),
    })
}

fn meta(seq: u64) -> EventMeta {
    let n = i64::try_from(seq).expect("small");
    EventMeta {
        instrument: instrument(),
        exchange_ts: Ts::from_nanos(1_000_000_000 + n),
        local_recv_ts: Ts::from_nanos(2_000_000_000 + n),
        ingest_seq: seq,
    }
}

fn level(px: &str, qty: &str) -> Level {
    Level {
        px: px.parse().expect("px"),
        qty: qty.parse().expect("qty"),
    }
}

/// A book snapshot as a delta, anchored so the book goes live.
fn snapshot(seq: u64) -> MarketEvent {
    MarketEvent::BookSnapshot(quant_core::event::BookSnapshot {
        meta: meta(seq),
        last_update_id: 100,
        bids: vec![level("100.00", "5")],
        asks: vec![level("101.00", "5")],
    })
}

fn delta(seq: u64, first: u64, last: u64, bid: &str) -> MarketEvent {
    MarketEvent::BookDelta(BookDelta {
        meta: meta(seq),
        first_update_id: first,
        final_update_id: last,
        bids: vec![level(bid, "5")],
        asks: vec![level("101.00", "5")],
    })
}

fn trade(seq: u64, px: &str) -> MarketEvent {
    MarketEvent::Trade(Trade {
        meta: meta(seq),
        px: px.parse().expect("px"),
        qty: "1".parse().expect("qty"),
        aggressor: Side::Buy,
        venue_trade_id: seq,
    })
}

/// A trade at an instant the sequence number does not imply.
///
/// `meta` derives `local_recv_ts` from the sequence, which is right for every
/// ordinary fixture and makes it impossible to express the one thing M8.a has to
/// handle: an event that arrives after another and is stamped before it.
fn trade_at(seq: u64, px: &str, at: Ts) -> MarketEvent {
    let MarketEvent::Trade(mut t) = trade(seq, px) else {
        unreachable!("trade() builds a trade")
    };
    t.meta.local_recv_ts = at;
    MarketEvent::Trade(t)
}

fn gap(seq: u64) -> MarketEvent {
    MarketEvent::Gap(Gap {
        meta: meta(seq),
        cause: GapCause::Disconnect,
        last_good_ts: Ts::from_nanos(1),
    })
}

/// A source that hands out a fixed script.
#[derive(Debug)]
struct Scripted {
    events: std::vec::IntoIter<MarketEvent>,
    /// Fail after this many events, to prove an error is not an ending.
    fail_after: Option<usize>,
    served: usize,
}

impl Scripted {
    fn new(events: Vec<MarketEvent>) -> Self {
        Self {
            events: events.into_iter(),
            fail_after: None,
            served: 0,
        }
    }

    fn failing_after(events: Vec<MarketEvent>, n: usize) -> Self {
        Self {
            events: events.into_iter(),
            fail_after: Some(n),
            served: 0,
        }
    }
}

impl EventSource for Scripted {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        if self.fail_after == Some(self.served) {
            return Some(Err(SourceError("scripted failure".to_owned())));
        }
        self.served += 1;
        self.events.next().map(Ok)
    }
}

/// A venue that records what it was asked to do and fills everything at once.
#[derive(Debug, Default)]
struct RecordingVenue {
    received: Vec<(ClientOrderId, OrderRequest)>,
    cancelled: Vec<ClientOrderId>,
    /// Orders to report as filled on the next `observe`.
    pending: Vec<ClientOrderId>,
    out: Vec<ExecutionEvent>,
    /// Events this venue has seen, in order — for the ordering test.
    observed: Vec<u64>,
}

impl ExecutionVenue for RecordingVenue {
    fn submit(&mut self, client_order_id: ClientOrderId, request: &OrderRequest, _now: Ts) {
        self.received.push((client_order_id, *request));
        self.pending.push(client_order_id);
    }

    fn cancel(&mut self, client_order_id: ClientOrderId, _now: Ts) {
        self.cancelled.push(client_order_id);
    }

    fn observe(&mut self, event: &MarketEvent, _book: &Book, now: Ts) {
        self.observed.push(event.meta().ingest_seq);
        for id in self.pending.drain(..) {
            self.out.push(ExecutionEvent::Filled {
                client_order_id: id,
                fill: Fill {
                    px: "100.00".parse().expect("px"),
                    qty: "1".parse().expect("qty"),
                    fee: Notional::from_raw(0),
                    is_maker: false,
                    fee_asset: quant_core::execution::FeeAsset::Quote,
                },
                remaining: "0".parse().expect("qty"),
                ts: now,
            });
        }
    }

    fn poll(&mut self, _now: Ts, out: &mut Vec<ExecutionEvent>) {
        out.append(&mut self.out);
    }
}

fn buy() -> OrderRequest {
    OrderRequest {
        instrument: instrument(),
        side: Side::Buy,
        qty: "1".parse().expect("qty"),
        kind: OrderKind::Market,
        time_in_force: TimeInForce::Gtc,
    }
}

/// Submits once, on the first event it sees, and records everything.
#[derive(Debug, Default)]
struct Recorder {
    /// `(ingest_seq at submission)` — one entry per submitted order.
    submitted_at: Vec<u64>,
    executions: Vec<ExecutionEvent>,
    /// Best bid seen at each market event, `None` when the book had none.
    best_bids: Vec<Option<String>>,
    submit_every_event: bool,
    submitted: bool,
}

impl Strategy for Recorder {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>) {
        self.best_bids.push(
            ctx.book(event.meta().instrument)
                .and_then(Book::best_bid)
                .map(|l| l.px.to_string()),
        );
        if self.submit_every_event || !self.submitted {
            self.submitted = true;
            self.submitted_at.push(event.meta().ingest_seq);
            ctx.submit(buy());
        }
    }

    fn on_execution(&mut self, event: &ExecutionEvent, _ctx: &mut Context<'_>) {
        self.executions.push(event.clone());
    }
}

#[test]
fn an_order_cannot_trade_against_the_event_that_prompted_it() {
    // The anti-lookahead property the whole step order exists for. The strategy
    // submits on seeing event 2; the venue must not have matched it against
    // event 2, because the venue saw event 2 first.
    let events = vec![
        snapshot(1),
        delta(2, 101, 101, "100.50"),
        trade(3, "100.75"),
    ];
    let mut engine = Engine::new(
        Scripted::new(events),
        RecordingVenue::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    );
    engine.run().expect("no source failure");

    let strategy = engine.strategy();
    assert_eq!(
        strategy.submitted_at,
        vec![1],
        "submitted on the first event"
    );
    let ExecutionEvent::Filled { ts, .. } = &strategy.executions[0] else {
        panic!("expected a fill, got {:?}", strategy.executions);
    };
    // Submitted while handling ingest_seq 1, filled while handling 2.
    assert_eq!(
        *ts,
        meta(2).local_recv_ts,
        "the fill must belong to a later event than the submission"
    );
}

#[test]
fn the_venue_sees_an_event_before_the_strategy_does() {
    // The same property from the other side: step 3 precedes step 5.
    let events = vec![snapshot(1), trade(2, "100.75")];
    let mut engine = Engine::new(
        Scripted::new(events),
        RecordingVenue::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    );
    engine.run().expect("run");
    assert_eq!(
        engine.venue().observed,
        vec![1, 2],
        "every event reaches the venue, in order"
    );
}

#[test]
fn a_refusing_risk_layer_means_no_order_reaches_the_venue() {
    // The chokepoint criterion. Not "the venue rejects it" -- the venue never
    // hears about it, which is what makes risk a chokepoint rather than a
    // module the strategy politely calls.
    #[derive(Debug)]
    struct RefuseAll;
    impl RiskLayer for RefuseAll {
        fn check(
            &mut self,
            _r: &OrderRequest,
            _mark: Option<quant_core::Px>,
            _now: Ts,
        ) -> Option<Refusal> {
            Some(Refusal::unnamed(RejectReason::RiskLimit))
        }
    }

    let events = vec![snapshot(1), trade(2, "100.75"), trade(3, "100.80")];
    let mut engine = Engine::new(
        Scripted::new(events),
        RecordingVenue::default(),
        RefuseAll,
        Recorder {
            submit_every_event: true,
            ..Recorder::default()
        },
        CASH,
    );
    let stats = engine.run().expect("run");

    assert!(
        engine.venue().received.is_empty(),
        "the venue must never have been told"
    );
    assert_eq!(stats.submitted, 0);
    assert_eq!(stats.refused, 3);
    // And the strategy still learns, the same way it learns everything else.
    assert_eq!(engine.strategy().executions.len(), 3);
    assert!(engine.strategy().executions.iter().all(|e| matches!(
        e,
        ExecutionEvent::Rejected {
            reason: RejectReason::RiskLimit,
            ..
        }
    )));
}

#[test]
fn a_malformed_request_is_refused_at_the_seam() {
    // A zero size is the shape an arithmetic slip takes, and a venue answers it
    // with an opaque code much later.
    #[derive(Debug, Default)]
    struct SendsZero {
        executions: Vec<ExecutionEvent>,
    }
    impl Strategy for SendsZero {
        fn on_market_event(&mut self, _event: &MarketEvent, ctx: &mut Context<'_>) {
            ctx.submit(OrderRequest {
                qty: "0".parse().expect("qty"),
                ..buy()
            });
        }
        fn on_execution(&mut self, event: &ExecutionEvent, _ctx: &mut Context<'_>) {
            self.executions.push(event.clone());
        }
    }

    let mut engine = Engine::new(
        Scripted::new(vec![snapshot(1)]),
        RecordingVenue::default(),
        AllowAll,
        SendsZero::default(),
        CASH,
    );
    engine.run().expect("run");
    assert!(
        engine.venue().received.is_empty(),
        "{:?}",
        engine.venue().received
    );
    assert!(matches!(
        engine.strategy().executions[0],
        ExecutionEvent::Rejected {
            reason: RejectReason::Malformed,
            ..
        }
    ));
}

#[test]
fn prices_go_away_across_a_gap_rather_than_going_stale() {
    // Not a convention a strategy has to remember: an invalidated book is
    // cleared, so there is no stale price to read even by accident.
    let events = vec![
        snapshot(1),
        delta(2, 101, 101, "100.50"),
        gap(3),
        trade(4, "100.75"),
    ];
    let mut engine = Engine::new(
        Scripted::new(events),
        RecordingVenue::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    );
    engine.run().expect("run");

    let bids = &engine.strategy().best_bids;
    assert!(bids[1].is_some(), "a live book has a bid: {bids:?}");
    assert_eq!(bids[2], None, "the gap itself clears it");
    assert_eq!(bids[3], None, "and it stays cleared until a new anchor");
}

#[test]
fn the_same_strategy_value_runs_against_two_different_wirings() {
    // The property in section 1 of the engine contract. Not "two strategies
    // behave alike" -- the *same* type, with no knowledge of which venue it got.
    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct Counting {
        seen: Vec<u64>,
    }
    impl Strategy for Counting {
        fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>) {
            self.seen.push(event.meta().ingest_seq);
            ctx.submit(buy());
        }
    }

    /// A second venue with entirely different behaviour: accepts, never fills.
    #[derive(Debug, Default)]
    struct AcceptOnly {
        out: Vec<ExecutionEvent>,
    }
    impl ExecutionVenue for AcceptOnly {
        fn submit(&mut self, client_order_id: ClientOrderId, _r: &OrderRequest, now: Ts) {
            self.out.push(ExecutionEvent::Accepted {
                client_order_id,
                venue_order_id: None,
                ts: now,
            });
        }
        fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}
        fn poll(&mut self, _now: Ts, out: &mut Vec<ExecutionEvent>) {
            out.append(&mut self.out);
        }
    }

    let events = || vec![snapshot(1), trade(2, "100.75"), trade(3, "100.80")];

    let mut filling = Engine::new(
        Scripted::new(events()),
        RecordingVenue::default(),
        AllowAll,
        Counting::default(),
        CASH,
    );
    filling.run().expect("run");

    let mut accepting = Engine::new(
        Scripted::new(events()),
        AcceptOnly::default(),
        AllowAll,
        Counting::default(),
        CASH,
    );
    accepting.run().expect("run");

    assert_eq!(
        filling.strategy(),
        accepting.strategy(),
        "the strategy cannot tell which venue it was wired to"
    );
}

#[test]
fn a_source_error_stops_the_run_and_is_not_an_ending() {
    // The failure mode EventSource's signature exists to prevent: a backtest
    // that silently covers less data than it claims to.
    let events = vec![snapshot(1), trade(2, "100.75"), trade(3, "100.80")];
    let mut engine = Engine::new(
        Scripted::failing_after(events, 2),
        RecordingVenue::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    );
    let err = engine
        .run()
        .expect_err("a broken source is not a clean end");
    assert_eq!(err.to_string(), "scripted failure");
    assert_eq!(engine.stats().events, 2, "and it stopped where it broke");
}

#[test]
fn the_clock_is_the_event_stream_and_nothing_else() {
    // No wall-clock anywhere on this path, which is what makes a backtest
    // deterministic. Checked by running the identical wiring twice.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Clocks {
        seen: Vec<i64>,
    }
    impl Strategy for Clocks {
        fn on_market_event(&mut self, _event: &MarketEvent, ctx: &mut Context<'_>) {
            self.seen.push(ctx.now().as_nanos());
        }
    }

    let run = || {
        let mut engine = Engine::new(
            Scripted::new(vec![snapshot(1), trade(2, "100.75")]),
            RecordingVenue::default(),
            AllowAll,
            Clocks::default(),
            CASH,
        );
        engine.run().expect("run");
        engine.strategy().seen.clone()
    };
    assert_eq!(run(), run(), "two runs must be identical");
    assert_eq!(
        run(),
        vec![
            meta(1).local_recv_ts.as_nanos(),
            meta(2).local_recv_ts.as_nanos()
        ],
        "and the times are the events' own, not a wall clock's"
    );
}

#[test]
fn a_cancel_reports_nothing_and_reaches_the_venue() {
    // Fire-and-forget for a sharper reason than submission: a cancel can lose
    // the race with a fill, so a boolean return would be an invention.
    #[derive(Debug, Default)]
    struct Canceller;
    impl Strategy for Canceller {
        fn on_market_event(&mut self, _event: &MarketEvent, ctx: &mut Context<'_>) {
            let id = ctx.submit(buy());
            ctx.cancel(id);
        }
    }

    let mut engine = Engine::new(
        Scripted::new(vec![snapshot(1)]),
        RecordingVenue::default(),
        AllowAll,
        Canceller,
        CASH,
    );
    let stats = engine.run().expect("run");
    assert_eq!(stats.cancels, 1);
    assert_eq!(engine.venue().cancelled, vec![ClientOrderId(1)]);
}

#[test]
fn client_order_ids_are_unique_even_across_refusals() {
    // A refused order still gets an id, because the caller was promised one and
    // has to be able to reconcile the refusal against the submission.
    #[derive(Debug, Default)]
    struct Two {
        ids: Vec<ClientOrderId>,
    }
    impl Strategy for Two {
        fn on_market_event(&mut self, _event: &MarketEvent, ctx: &mut Context<'_>) {
            self.ids.push(ctx.submit(OrderRequest {
                qty: "0".parse().expect("qty"),
                ..buy()
            }));
            self.ids.push(ctx.submit(buy()));
        }
    }

    let mut engine = Engine::new(
        Scripted::new(vec![snapshot(1)]),
        RecordingVenue::default(),
        AllowAll,
        Two::default(),
        CASH,
    );
    engine.run().expect("run");
    let ids = &engine.strategy().ids;
    assert_eq!(ids, &[ClientOrderId(1), ClientOrderId(2)]);
}

/// Everything the engine told an observer, in order.
///
/// The first `RunObserver` in a test: slices (a) through (c) built the hooks and
/// pinned their *payloads* in `journal.rs`, which leaves the wiring — whether
/// the engine calls them at all, and when — resting on the paper binary. That is
/// the shape of defect M6 found in M5.c, where `FillObserver` was never invoked
/// and the durability the slice claimed did not exist.
#[derive(Debug, Default)]
struct Witness {
    tripped: Vec<(crate::risk::TripCause, Ts)>,
    blind: Vec<(GapCause, Ts, Ts)>,
    notes: Vec<(&'static str, Ts)>,
}

impl crate::RunObserver for std::sync::Arc<std::sync::Mutex<Witness>> {
    fn on_tripped(&mut self, cause: crate::risk::TripCause, at: Ts) {
        self.lock().expect("witness").tripped.push((cause, at));
    }

    fn on_blind(&mut self, cause: GapCause, last_good_ts: Ts, at: Ts) {
        self.lock()
            .expect("witness")
            .blind
            .push((cause, last_good_ts, at));
    }

    fn on_note(&mut self, note: &crate::strategy::Note, at: Ts) {
        self.lock().expect("witness").notes.push((note.kind, at));
    }
}

/// Submits on every event, which is what makes an order-count limit bind.
#[derive(Debug, Default)]
struct Greedy;

impl Strategy for Greedy {
    fn on_market_event(&mut self, _event: &MarketEvent, ctx: &mut Context<'_>) {
        ctx.submit(buy());
    }
}

fn witnessed_run(events: Vec<MarketEvent>, limits: crate::risk::Limits) -> Witness {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(events),
        RecordingVenue::default(),
        crate::risk::RiskEngine::new(limits),
        Greedy,
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    engine.run().expect("no source failure");
    let out = std::mem::take(&mut *seen.lock().expect("witness"));
    out
}

#[test]
fn a_trip_is_reported_at_the_instant_it_trips_not_at_shutdown() {
    // The defect this slice closes, and it is two defects wearing one coat. M5
    // wrote `Tripped` during shutdown, so a hard kill in between lost it
    // outright and the supervisor re-armed a switch that had fired; and even on
    // a clean exit the entry carried the *shutdown* time, so the record of the
    // most serious thing the risk layer can do named the wrong moment.
    //
    // Three events, a limit of one order a day: the second submission trips.
    let events = vec![trade(1, "100.00"), trade(2, "100.00"), trade(3, "100.00")];
    let seen = witnessed_run(
        events,
        crate::risk::Limits {
            max_orders_per_day: Some(1),
            ..crate::risk::Limits::default()
        },
    );
    assert_eq!(
        seen.tripped.len(),
        1,
        "exactly one trip: {:?}",
        seen.tripped
    );
    let (cause, at) = seen.tripped[0];
    assert_eq!(cause, crate::risk::TripCause::OrderCount);
    // The instant of the *second* event, which is where the limit bound --
    // not the third, and not whenever the run happened to end.
    assert_eq!(
        at,
        meta(2).local_recv_ts,
        "stamped at the breach, not at the end of the run"
    );
}

#[test]
fn a_switch_that_is_already_thrown_is_not_reported_again() {
    // Every order after the trip is refused *by* the trip, and a layer that
    // reported each one would write a `tripped` line per refused order -- a file
    // claiming the limit fired eleven times when it fired once.
    let events = vec![
        trade(1, "100.00"),
        trade(2, "100.00"),
        trade(3, "100.00"),
        trade(4, "100.00"),
        trade(5, "100.00"),
    ];
    let seen = witnessed_run(
        events,
        crate::risk::Limits {
            max_orders_per_day: Some(1),
            ..crate::risk::Limits::default()
        },
    );
    assert_eq!(
        seen.tripped.len(),
        1,
        "one trip however many orders it refuses: {:?}",
        seen.tripped
    );
}

#[test]
fn a_switch_recovered_from_a_previous_session_reports_nothing() {
    // It is already on record -- that is where it was recovered from. Reporting
    // it would add a `tripped` line at every restart, and `last_trip` reads the
    // most recent one, so the file would say the limit fired at the start of the
    // session that merely inherited it.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00"), trade(2, "100.00")]),
        RecordingVenue::default(),
        crate::risk::RiskEngine::recover(
            crate::risk::Limits::default(),
            Some(crate::risk::TripCause::DailyLoss),
        ),
        Greedy,
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    engine.run().expect("no source failure");
    assert_eq!(
        seen.lock().expect("witness").tripped,
        Vec::new(),
        "a recovered switch is not a new trip"
    );
}

#[test]
fn going_blind_is_reported_with_its_cause_and_how_long_it_lasted() {
    // "Why did it not trade between 03:00 and 04:00" has four answers and three
    // of them are already in the file. Without this one a quiet hour and a blind
    // hour are the same silence.
    let seen = witnessed_run(
        vec![trade(1, "100.00"), gap(2), trade(3, "100.00")],
        crate::risk::Limits::default(),
    );
    assert_eq!(seen.blind.len(), 1, "one gap, one line: {:?}", seen.blind);
    let (cause, last_good, at) = seen.blind[0];
    assert_eq!(cause, GapCause::Disconnect);
    assert_eq!(
        last_good,
        Ts::from_nanos(1),
        "the last instant we could see"
    );
    assert_eq!(at, meta(2).local_recv_ts, "and the instant we could not");
    assert!(at > last_good, "the pair has to bound a real interval");
}

#[test]
fn a_run_that_never_goes_blind_says_nothing_about_blindness() {
    // The other half, and the one that stops the entry becoming noise: an
    // absence of gaps must produce an absence of lines, or "were we blind" is
    // answered the same way on every run.
    let seen = witnessed_run(
        vec![trade(1, "100.00"), trade(2, "100.00")],
        crate::risk::Limits::default(),
    );
    assert_eq!(seen.blind, Vec::new());
    assert_eq!(seen.tripped, Vec::new(), "and no limit bound either");
}

/// Writes one note per event, naming the event it saw.
#[derive(Debug, Default)]
struct Diarist {
    pending: Vec<crate::strategy::Note>,
}

impl Strategy for Diarist {
    fn on_market_event(&mut self, event: &MarketEvent, _ctx: &mut Context<'_>) {
        self.pending.push(crate::strategy::Note::new(
            "saw",
            serde_json::json!({ "seq": event.meta().ingest_seq }),
        ));
    }

    fn take_notes(&mut self, out: &mut Vec<crate::strategy::Note>) {
        out.append(&mut self.pending);
    }
}

#[test]
fn a_note_is_taken_in_the_same_event_the_strategy_wrote_it() {
    // The pull happens last in the step, after `on_market_event`, which is
    // where a strategy does its deciding. Taken between steps 4 and 5 it would
    // collect each note one event late and every timestamp in the strategy's
    // column would be wrong by one event -- a file that disagrees with the
    // engine's own entries about when the same instant was.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00"), trade(2, "100.00")]),
        RecordingVenue::default(),
        AllowAll,
        Diarist::default(),
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    engine.run().expect("run");

    let notes = std::mem::take(&mut seen.lock().expect("witness").notes);
    assert_eq!(
        notes,
        vec![
            ("saw", meta(1).local_recv_ts),
            ("saw", meta(2).local_recv_ts)
        ],
        "each note carries the instant of the event that produced it"
    );
}

#[test]
fn a_strategy_that_keeps_no_diary_is_not_made_to_say_so() {
    // The defaulted method, and the no-op property: `Recorder` implements
    // nothing, and the run must produce no notes rather than empty ones.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00"), trade(2, "100.00")]),
        RecordingVenue::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    engine.run().expect("run");
    assert_eq!(seen.lock().expect("witness").notes, Vec::new());
}

#[test]
fn notes_cannot_reveal_which_venue_the_strategy_was_wired_to() {
    // The §7 criterion, re-run with a record attached. `take_notes` is a new
    // path out of a strategy, and the thing to prove is that it did not become
    // a path *in*: the same strategy value against a venue that fills
    // everything and one that fills nothing must write the same diary.
    //
    // It is not a restatement of the existing wiring test. That one compares
    // strategy state, which a note is derived from; this compares what actually
    // reached the observer, which is what ends up in the file.
    fn notes_against<V: ExecutionVenue>(venue: V) -> Vec<(&'static str, Ts)> {
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
        let mut engine = Engine::new(
            Scripted::new(vec![snapshot(1), trade(2, "100.75"), trade(3, "100.80")]),
            venue,
            AllowAll,
            Diarist::default(),
            CASH,
        )
        .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
        engine.run().expect("run");
        let out = std::mem::take(&mut seen.lock().expect("witness").notes);
        out
    }

    #[derive(Debug, Default)]
    struct FillsNothing;
    impl ExecutionVenue for FillsNothing {
        fn submit(&mut self, _id: ClientOrderId, _r: &OrderRequest, _now: Ts) {}
        fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}
        fn poll(&mut self, _now: Ts, _out: &mut Vec<ExecutionEvent>) {}
    }

    let filling = notes_against(RecordingVenue::default());
    // Non-empty first. Comparing two empty diaries is a test that passes
    // whatever the engine does -- it stayed green with the pull deleted
    // outright, which is M5.c's vacuous identity in a new costume.
    assert_eq!(
        filling.len(),
        3,
        "one note per event, or there is nothing being compared"
    );
    assert_eq!(
        filling,
        notes_against(FillsNothing),
        "the diary is the same whichever venue it was wired to"
    );
}

#[test]
fn an_engine_told_where_to_start_does_not_remint_ids_from_one() {
    // `minting_from` existed from M7.5.a and was called from nowhere until the
    // post-milestone audit. A unit test on `journal::next_order_id` would not
    // have caught that -- it tests the number, not that anyone uses it -- so
    // this tests the seam the binary actually goes through.
    //
    // What it guards: a restarted session whose ids began at 1 again would hand
    // out ids the journal already holds. `runlog check` reads density from 1 and
    // would report no hole, because there is none. The ids are not missing, they
    // are ambiguous, which nothing downstream can detect.
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00")]),
        RecordingVenue::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    )
    .minting_from(825);
    engine.run().expect("run");

    let first = engine
        .venue()
        .received
        .first()
        .expect("the strategy submits on the first event")
        .0;
    assert_eq!(
        first,
        ClientOrderId(825),
        "an engine resumed at 825 must not hand out 1"
    );
}

/// A source that goes quiet: it yields its events, then idles forever.
///
/// What a live source does when the market socket stalls. `Scripted` cannot
/// express it — it ends the stream — and the difference is the whole of M8.a:
/// an ended stream stops the engine, a quiet one leaves it running with nothing
/// to do but ask the venue.
#[derive(Debug)]
struct GoesQuiet {
    events: std::vec::IntoIter<MarketEvent>,
    idles: usize,
}

impl GoesQuiet {
    fn new(events: Vec<MarketEvent>, idles: usize) -> Self {
        Self {
            events: events.into_iter(),
            idles,
        }
    }
}

impl EventSource for GoesQuiet {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        self.events.next().map(Ok)
    }

    fn next_event_timeout(
        &mut self,
        _timeout: core::time::Duration,
    ) -> Option<Result<quant_core::source::Wake, SourceError>> {
        if let Some(event) = self.events.next() {
            return Some(Ok(quant_core::source::Wake::Event(event)));
        }
        if self.idles == 0 {
            return None;
        }
        self.idles -= 1;
        Some(Ok(quant_core::source::Wake::Idle))
    }
}

/// Idles once, then yields its events, then ends.
///
/// A live source before its first frame: the socket is up, nothing has arrived.
#[derive(Debug)]
struct IdlesFirst {
    events: std::vec::IntoIter<MarketEvent>,
    idled: bool,
}

impl IdlesFirst {
    fn new(events: Vec<MarketEvent>) -> Self {
        Self {
            events: events.into_iter(),
            idled: false,
        }
    }
}

impl EventSource for IdlesFirst {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        self.events.next().map(Ok)
    }

    fn next_event_timeout(
        &mut self,
        _timeout: core::time::Duration,
    ) -> Option<Result<quant_core::source::Wake, SourceError>> {
        if !self.idled {
            self.idled = true;
            return Some(Ok(quant_core::source::Wake::Idle));
        }
        self.events
            .next()
            .map(|e| Ok(quant_core::source::Wake::Event(e)))
    }
}

/// Reports a fill on a later `poll`, having been told nothing in between.
///
/// A live venue: the fill arrives from the user-data stream on the venue's
/// schedule, not in response to anything the engine did.
#[derive(Debug, Default)]
struct SpeaksLater {
    held: Option<ExecutionEvent>,
    polls: usize,
}

impl ExecutionVenue for SpeaksLater {
    fn submit(&mut self, client_order_id: ClientOrderId, _r: &OrderRequest, _now: Ts) {
        self.held = Some(ExecutionEvent::Filled {
            client_order_id,
            fill: Fill {
                px: "100.00".parse().expect("px"),
                qty: "1".parse().expect("qty"),
                fee: Notional::from_raw(0),
                is_maker: false,
                fee_asset: quant_core::execution::FeeAsset::Quote,
            },
            remaining: "0".parse().expect("qty"),
            // Stamped by the venue, *later* than any market event in the
            // fixture -- `meta` puts those near 2s. An earlier stamp is a
            // legitimate thing for a venue to send and the clock clamps it,
            // which is how the first draft of this fixture failed.
            ts: Ts::from_nanos(3_000_000_000),
        });
    }

    fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}

    fn poll(&mut self, _now: Ts, out: &mut Vec<ExecutionEvent>) {
        self.polls += 1;
        // Not on the poll that follows submission -- that one happens inside the
        // same market event, and a venue answering there is the simulated one.
        if self.polls > 1 {
            if let Some(event) = self.held.take() {
                out.push(event);
            }
        }
    }
}

#[test]
fn a_fill_arriving_while_the_market_is_silent_is_still_booked() {
    // The defect `docs/live-run.md` §1 found by reading, before any M8 code
    // existed. `venue.poll` had one call site, inside the handling of a market
    // event, and the source blocked forever -- so a fill arriving while the
    // market socket was stalled could not be booked, the strategy could not be
    // told, and the risk layer's daily tally could not move.
    //
    // Invisible in the other two worlds: `SimulatedVenue` answers only inside
    // `observe`, which the engine calls while handling an event it already has.
    let mut engine = Engine::new(
        GoesQuiet::new(vec![trade(1, "100.00")], 4),
        SpeaksLater::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    );
    let stats = engine.run().expect("no source failure");

    assert_eq!(stats.fills, 1, "the fill is booked without a market event");
    assert!(stats.idle_wakes > 0, "and it took an idle wake to get it");
    assert_eq!(
        engine.strategy().executions.len(),
        1,
        "the strategy is told, not merely the ledger"
    );
    assert_eq!(
        stats.last_ts,
        Some(Ts::from_nanos(3_000_000_000)),
        "booked at the instant the venue acted, not at the last market event"
    );
    assert_eq!(stats.clock_clamps, 0, "and nothing had to be clamped");
}

#[test]
fn an_idle_wake_before_the_first_event_does_not_date_the_run_from_nineteen_seventy() {
    // What the early return in `dispatch` actually guards, found by sabotage
    // rather than by design: removing it reddened nothing, because an empty
    // report list books nothing and tells nobody. The comment claimed it
    // prevented noise and it does not.
    //
    // What it does prevent is this. The engine's clock starts at zero, and a
    // live source idles before its first frame as a matter of course -- so
    // without the return, `first_ts.get_or_insert(self.now)` fires on an idle
    // wake and the run reports a period beginning at the epoch.
    let mut engine = Engine::new(
        IdlesFirst::new(vec![trade(1, "100.00")]),
        RecordingVenue::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    );
    let stats = engine.run().expect("no source failure");
    assert!(stats.idle_wakes >= 1, "the fixture has to idle first");
    assert_eq!(
        stats.first_ts,
        Some(meta(1).local_recv_ts),
        "the run began at its first event, not at the epoch"
    );
}

#[test]
fn the_clock_never_runs_backwards_and_says_when_it_was_asked_to() {
    // Two asynchronous inputs can deliver out of order. Everything downstream
    // dispatches on this clock, so a report stamped before an event that arrived
    // first must not drag it back -- a fill would appear to happen before the
    // order that caused it.
    let backwards = vec![trade(1, "100.00"), trade_at(2, "100.00", Ts::from_nanos(1))];

    let mut engine = Engine::new(
        Scripted::new(backwards),
        RecordingVenue::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    );
    let stats = engine.run().expect("run");
    assert_eq!(stats.clock_clamps, 1, "it was asked to go back, once");
    assert_eq!(
        stats.last_ts,
        Some(meta(1).local_recv_ts),
        "and did not: the clock held at the later instant"
    );
}

#[test]
fn a_source_that_never_idles_leaves_every_count_at_zero() {
    // The no-op property, and the reason `next_event_timeout` is defaulted.
    // `HistoricalSource` uses the default, which can never return `Idle`, so a
    // backtest's event sequence cannot depend on how fast the disk was. Same
    // shape as `Costs::NONE` reproducing M3 to the last digit.
    let mut engine = Engine::new(
        Scripted::new(vec![snapshot(1), trade(2, "100.75"), trade(3, "100.80")]),
        RecordingVenue::default(),
        AllowAll,
        Recorder::default(),
        CASH,
    );
    let stats = engine.run().expect("run");
    assert_eq!(stats.idle_wakes, 0, "a file has nothing to wait for");
    assert_eq!(stats.clock_clamps, 0, "and is monotone by construction");
}
