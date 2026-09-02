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

use crate::{AllowAll, Context, Engine, ExecutionVenue, RiskLayer, Strategy};

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
                },
                remaining: "0".parse().expect("qty"),
                ts: now,
            });
        }
    }

    fn poll(&mut self, out: &mut Vec<ExecutionEvent>) {
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
        fn check(&mut self, _r: &OrderRequest, _now: Ts) -> Option<RejectReason> {
            Some(RejectReason::RiskLimit)
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
    );
    engine.run().expect("run");
    assert!(engine.venue().received.is_empty());
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
        fn poll(&mut self, out: &mut Vec<ExecutionEvent>) {
            out.append(&mut self.out);
        }
    }

    let events = || vec![snapshot(1), trade(2, "100.75"), trade(3, "100.80")];

    let mut filling = Engine::new(
        Scripted::new(events()),
        RecordingVenue::default(),
        AllowAll,
        Counting::default(),
    );
    filling.run().expect("run");

    let mut accepting = Engine::new(
        Scripted::new(events()),
        AcceptOnly::default(),
        AllowAll,
        Counting::default(),
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
    );
    engine.run().expect("run");
    let ids = &engine.strategy().ids;
    assert_eq!(ids, &[ClientOrderId(1), ClientOrderId(2)]);
}
