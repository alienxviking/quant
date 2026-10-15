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
    VenueOrderId,
};
use quant_core::instrument::{
    Exchange, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
};
use quant_core::report::{Ended, FillIds, Notice, OrderReport, RejectedWhy, VenueReport};
use quant_core::source::{EventSource, SourceError};
use quant_core::time::Ts;
use quant_core::{Notional, Qty};

use crate::{AllowAll, Context, Engine, ExecutionVenue, Refusal, RiskLayer, Strategy, Unready};

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
    fn ready(&self, _request: &OrderRequest) -> Result<(), Unready> {
        Ok(())
    }

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

    fn poll(&mut self, _now: Ts, out: &mut Vec<VenueReport>) {
        out.extend(self.out.drain(..).map(VenueReport::from));
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
        fn ready(&self, _request: &OrderRequest) -> Result<(), Unready> {
            Ok(())
        }
        fn submit(&mut self, client_order_id: ClientOrderId, _r: &OrderRequest, now: Ts) {
            self.out.push(ExecutionEvent::Accepted {
                client_order_id,
                ts: now,
            });
        }
        fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}
        fn poll(&mut self, _now: Ts, out: &mut Vec<VenueReport>) {
            out.extend(self.out.drain(..).map(VenueReport::from));
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

/// One thing the engine told an observer, in the words a test compares.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Heard {
    Submitted(ClientOrderId, Ts),
    Refused(ClientOrderId, RejectReason, crate::RefusedBy, Ts),
    Accepted(ClientOrderId, Option<VenueOrderId>, Ts),
    Rejected(ClientOrderId, RejectReason, Option<RejectedWhy>, Ts),
    CancelRequested(ClientOrderId, Ts),
    Cancelled(ClientOrderId, Qty, Option<Ended>, Ts),
    Tripped(crate::risk::TripCause, Ts),
    Blind(GapCause, Ts, Ts),
    Note(&'static str, Ts),
    Orphaned(ClientOrderId, Option<FillIds>, Ts),
    /// The order, the size, the venue's names for it, what is left, and when.
    Filled(ClientOrderId, Qty, Option<FillIds>, Qty, Ts),
    Notice(Notice, Ts),
    Stray(OrderReport, Ts),
    /// Not a hook: a venue that writes to the same log says it was sent this.
    Sent(ClientOrderId),
    /// Not a hook: a strategy that writes to the same log was told this, with
    /// its clock.
    Told(ExecutionEvent, Ts),
    /// Not a hook: a strategy that writes to the same log saw this market
    /// event, with its clock.
    Saw(u64, Ts),
}

/// Everything the engine told an observer, in the order it was told.
///
/// The first `RunObserver` in a test: slices (a) through (c) built the hooks and
/// pinned their *payloads* in `journal.rs`, which leaves the wiring — whether
/// the engine calls them at all, and when — resting on the paper binary. That is
/// the shape of defect M6 found in M5.c, where `FillObserver` was never invoked
/// and the durability the slice claimed did not exist.
///
/// One log for every hook rather than a list per hook, because the order is
/// most of what there is to check: a fill written down after the strategy
/// acted on it is the defect the write-ahead rule exists to stop, and three
/// separate lists could not show it. It heard three hooks of the eleven there
/// were until the venue seam needed the rest, and the seam added `on_notice`
/// and `on_stray` to make thirteen.
#[derive(Debug, Default)]
struct Witness {
    heard: Vec<Heard>,
    /// Answer `Withhold` to this submission, counting from 1, and `Proceed` to
    /// every other: a record that failed once.
    withhold_at: Option<usize>,
    submissions: usize,
}

impl Witness {
    fn tripped(&self) -> Vec<(crate::risk::TripCause, Ts)> {
        self.heard
            .iter()
            .filter_map(|h| match h {
                Heard::Tripped(cause, at) => Some((*cause, *at)),
                _ => None,
            })
            .collect()
    }

    fn blind(&self) -> Vec<(GapCause, Ts, Ts)> {
        self.heard
            .iter()
            .filter_map(|h| match h {
                Heard::Blind(cause, last_good, at) => Some((*cause, *last_good, *at)),
                _ => None,
            })
            .collect()
    }

    fn notes(&self) -> Vec<(&'static str, Ts)> {
        self.heard
            .iter()
            .filter_map(|h| match h {
                Heard::Note(kind, at) => Some((*kind, *at)),
                _ => None,
            })
            .collect()
    }
}

impl crate::RunObserver for std::sync::Arc<std::sync::Mutex<Witness>> {
    fn on_submitted(
        &mut self,
        client_order_id: ClientOrderId,
        _request: &OrderRequest,
        _mark: Option<quant_core::Px>,
        at: Ts,
    ) -> crate::WriteAhead {
        let mut witness = self.lock().expect("witness");
        witness.heard.push(Heard::Submitted(client_order_id, at));
        witness.submissions += 1;
        if witness.withhold_at == Some(witness.submissions) {
            crate::WriteAhead::Withhold
        } else {
            crate::WriteAhead::Proceed
        }
    }

    fn on_refused(
        &mut self,
        client_order_id: ClientOrderId,
        _request: &OrderRequest,
        reason: RejectReason,
        _bound: Option<crate::risk::Bound>,
        by: crate::RefusedBy,
        at: Ts,
    ) {
        self.lock()
            .expect("witness")
            .heard
            .push(Heard::Refused(client_order_id, reason, by, at));
    }

    fn on_accepted(
        &mut self,
        client_order_id: ClientOrderId,
        venue_order_id: Option<&VenueOrderId>,
        at: Ts,
    ) {
        self.lock().expect("witness").heard.push(Heard::Accepted(
            client_order_id,
            venue_order_id.cloned(),
            at,
        ));
    }

    fn on_rejected(
        &mut self,
        client_order_id: ClientOrderId,
        reason: RejectReason,
        why: Option<&RejectedWhy>,
        at: Ts,
    ) {
        self.lock().expect("witness").heard.push(Heard::Rejected(
            client_order_id,
            reason,
            why.cloned(),
            at,
        ));
    }

    fn on_cancel_requested(&mut self, client_order_id: ClientOrderId, at: Ts) {
        self.lock()
            .expect("witness")
            .heard
            .push(Heard::CancelRequested(client_order_id, at));
    }

    fn on_cancelled(
        &mut self,
        client_order_id: ClientOrderId,
        remaining: Qty,
        ended: Option<&Ended>,
        at: Ts,
    ) {
        self.lock().expect("witness").heard.push(Heard::Cancelled(
            client_order_id,
            remaining,
            ended.cloned(),
            at,
        ));
    }

    fn on_tripped(&mut self, cause: crate::risk::TripCause, at: Ts) {
        self.lock()
            .expect("witness")
            .heard
            .push(Heard::Tripped(cause, at));
    }

    fn on_blind(&mut self, cause: GapCause, last_good_ts: Ts, at: Ts) {
        self.lock()
            .expect("witness")
            .heard
            .push(Heard::Blind(cause, last_good_ts, at));
    }

    fn on_note(&mut self, note: &crate::strategy::Note, at: Ts) {
        self.lock()
            .expect("witness")
            .heard
            .push(Heard::Note(note.kind, at));
    }

    fn on_orphaned(
        &mut self,
        client_order_id: ClientOrderId,
        _fill: &Fill,
        ids: Option<&FillIds>,
        at: Ts,
    ) {
        self.lock().expect("witness").heard.push(Heard::Orphaned(
            client_order_id,
            ids.cloned(),
            at,
        ));
    }

    fn on_fill(&mut self, booked: &crate::Booked<'_>, _portfolio: &crate::Portfolio) {
        self.lock().expect("witness").heard.push(Heard::Filled(
            booked.client_order_id,
            booked.fill.qty,
            booked.ids.cloned(),
            booked.remaining,
            booked.at,
        ));
    }

    fn on_notice(&mut self, notice: &Notice, at: Ts) {
        self.lock()
            .expect("witness")
            .heard
            .push(Heard::Notice(notice.clone(), at));
    }

    fn on_stray(&mut self, report: &OrderReport, at: Ts) {
        self.lock()
            .expect("witness")
            .heard
            .push(Heard::Stray(report.clone(), at));
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

/// A venue that says what it was scripted to, one batch per poll, whatever it
/// was sent.
///
/// For a test about what the *engine* does with what a venue says. A venue
/// that reacted to its orders would make the script depend on the behaviour
/// under test.
#[derive(Debug, Default)]
struct Says {
    batches: std::collections::VecDeque<Vec<VenueReport>>,
}

impl Says {
    /// Events with nothing beside them, which is what a simulator says.
    fn new(batches: Vec<Vec<ExecutionEvent>>) -> Self {
        Self::reporting(
            batches
                .into_iter()
                .map(|batch| batch.into_iter().map(VenueReport::from).collect())
                .collect(),
        )
    }

    /// Anything a venue can say, detail and notices included.
    fn reporting(batches: Vec<Vec<VenueReport>>) -> Self {
        Self {
            batches: batches.into(),
        }
    }
}

impl ExecutionVenue for Says {
    fn ready(&self, _request: &OrderRequest) -> Result<(), Unready> {
        Ok(())
    }

    fn submit(&mut self, _id: ClientOrderId, _r: &OrderRequest, _now: Ts) {}

    fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}

    fn poll(&mut self, _now: Ts, out: &mut Vec<VenueReport>) {
        out.extend(self.batches.pop_front().unwrap_or_default());
    }
}

/// A fill of `qty` at 100, free.
fn fill_of(qty: &str) -> Fill {
    Fill {
        px: "100.00".parse().expect("px"),
        qty: qty.parse().expect("qty"),
        fee: Notional::from_raw(0),
        is_maker: false,
        fee_asset: quant_core::execution::FeeAsset::Quote,
    }
}

/// Reaches every hook an observer has, over three events: on the first, an
/// order, a malformed one, a cancel of the first and a note; on the second,
/// two orders, the second of which a daily count of two refuses.
#[derive(Debug, Default)]
struct EveryHook {
    notes: Vec<crate::strategy::Note>,
}

impl Strategy for EveryHook {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>) {
        match event.meta().ingest_seq {
            1 => {
                let first = ctx.submit(buy());
                ctx.submit(OrderRequest {
                    qty: "0".parse().expect("qty"),
                    ..buy()
                });
                ctx.cancel(first);
                self.notes.push(crate::strategy::Note::new(
                    "decided",
                    serde_json::Value::Null,
                ));
            }
            2 => {
                ctx.submit(buy());
                ctx.submit(buy());
            }
            _ => {}
        }
    }

    fn take_notes(&mut self, out: &mut Vec<crate::strategy::Note>) {
        out.append(&mut self.notes);
    }
}

/// What the venue says to [`EveryHook`]: nothing on the first event; on the
/// second, order 1 accepted, part filled and cancelled; on the third, order 3
/// declined, a fill for order 99 and a cancellation of order 98, neither of
/// which this engine minted, and the account stream lost.
///
/// The stream loss is stamped with the third event, no later than the order
/// reports beside it, so it is heard on that wake without moving the clock
/// past them.
fn every_hook_venue() -> Says {
    let (t2, t3) = (meta(2).local_recv_ts, meta(3).local_recv_ts);
    let qty = |s: &str| -> Qty { s.parse().expect("qty") };
    let plain = |events: Vec<ExecutionEvent>| -> Vec<VenueReport> {
        events.into_iter().map(VenueReport::from).collect()
    };
    let mut third = plain(vec![
        ExecutionEvent::Rejected {
            client_order_id: ClientOrderId(3),
            reason: RejectReason::Venue,
            ts: t3,
        },
        ExecutionEvent::Filled {
            client_order_id: ClientOrderId(99),
            fill: fill_of("1"),
            remaining: qty("0"),
            ts: t3,
        },
        ExecutionEvent::Cancelled {
            client_order_id: ClientOrderId(98),
            remaining: qty("1"),
            ts: t3,
        },
    ]);
    third.push(VenueReport::Notice(stream_lost(t3)));
    Says::reporting(vec![
        vec![],
        plain(vec![
            ExecutionEvent::Accepted {
                client_order_id: ClientOrderId(1),
                ts: t2,
            },
            ExecutionEvent::Filled {
                client_order_id: ClientOrderId(1),
                fill: fill_of("0.4"),
                remaining: qty("0.6"),
                ts: t2,
            },
            ExecutionEvent::Cancelled {
                client_order_id: ClientOrderId(1),
                remaining: qty("0.6"),
                ts: t2,
            },
        ]),
        third,
    ])
}

#[test]
fn an_observer_hears_every_hook_in_the_order_the_engine_calls_them() {
    // The witness's own proof, and the order every later seam test reads it
    // in: one run that reaches all thirteen hooks, each once.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(vec![snapshot(1), trade(2, "100.75"), gap(3)]),
        every_hook_venue(),
        crate::risk::RiskEngine::new(crate::risk::Limits {
            max_orders_per_day: Some(2),
            ..crate::risk::Limits::default()
        }),
        EveryHook::default(),
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    engine.run().expect("no source failure");

    let (t1, t2, t3) = (
        meta(1).local_recv_ts,
        meta(2).local_recv_ts,
        meta(3).local_recv_ts,
    );
    let c = ClientOrderId;
    let qty = |s: &str| -> Qty { s.parse().expect("qty") };
    assert_eq!(
        seen.lock().expect("witness").heard,
        vec![
            Heard::Submitted(c(1), t1),
            Heard::Refused(c(2), RejectReason::Malformed, crate::RefusedBy::Seam, t1),
            Heard::CancelRequested(c(1), t1),
            Heard::Note("decided", t1),
            Heard::Accepted(c(1), None, t2),
            Heard::Filled(c(1), qty("0.4"), None, qty("0.6"), t2),
            Heard::Cancelled(c(1), qty("0.6"), None, t2),
            Heard::Submitted(c(3), t2),
            Heard::Tripped(crate::risk::TripCause::OrderCount, t2),
            Heard::Refused(c(4), RejectReason::RiskLimit, crate::RefusedBy::Risk, t2),
            Heard::Blind(GapCause::Disconnect, Ts::from_nanos(1), t3),
            Heard::Rejected(c(3), RejectReason::Venue, None, t3),
            Heard::Orphaned(c(99), None, t3),
            Heard::Stray(
                OrderReport::Cancelled {
                    client_order_id: c(98),
                    remaining: qty("1"),
                    ended: None,
                    ts: t3,
                },
                t3
            ),
            Heard::Notice(stream_lost(t3), t3),
        ],
        "every hook, each once, in the order the engine reached it"
    );
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
    let tripped = seen.tripped();
    assert_eq!(tripped.len(), 1, "exactly one trip: {tripped:?}");
    let (cause, at) = tripped[0];
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
    let tripped = seen.tripped();
    assert_eq!(
        tripped.len(),
        1,
        "one trip however many orders it refuses: {tripped:?}"
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
        seen.lock().expect("witness").tripped(),
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
    let blind = seen.blind();
    assert_eq!(blind.len(), 1, "one gap, one line: {blind:?}");
    let (cause, last_good, at) = blind[0];
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
    assert_eq!(seen.blind(), Vec::new());
    assert_eq!(seen.tripped(), Vec::new(), "and no limit bound either");
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

    let notes = seen.lock().expect("witness").notes();
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
    assert_eq!(seen.lock().expect("witness").notes(), Vec::new());
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
        let out = seen.lock().expect("witness").notes();
        out
    }

    #[derive(Debug, Default)]
    struct FillsNothing;
    impl ExecutionVenue for FillsNothing {
        fn ready(&self, _request: &OrderRequest) -> Result<(), Unready> {
            Ok(())
        }
        fn submit(&mut self, _id: ClientOrderId, _r: &OrderRequest, _now: Ts) {}
        fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}
        fn poll(&mut self, _now: Ts, _out: &mut Vec<VenueReport>) {}
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

/// A source that goes quiet: it yields its events, then idles a given number of
/// times before ending, because a test has to end.
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
/// schedule, not in response to anything the engine did, and carries a time of
/// the venue's choosing rather than one the engine gave it.
#[derive(Debug)]
struct SpeaksLater {
    held: Option<ExecutionEvent>,
    stamp: Ts,
}

impl SpeaksLater {
    fn stamping(stamp: Ts) -> Self {
        Self { held: None, stamp }
    }
}

impl Default for SpeaksLater {
    /// Later than any market event in the fixture -- `meta` puts those near 2s
    /// -- so the report moves the clock, and a test can see that it did.
    ///
    /// An earlier stamp would leave the clock where it was and count nothing,
    /// on either kind of wake; see
    /// `a_report_stamped_before_the_event_neither_moves_the_clock_nor_counts_a_clamp`
    /// and its idle-wake twin.
    fn default() -> Self {
        Self::stamping(Ts::from_nanos(3_000_000_000))
    }
}

impl ExecutionVenue for SpeaksLater {
    fn ready(&self, _request: &OrderRequest) -> Result<(), Unready> {
        Ok(())
    }

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
            ts: self.stamp,
        });
    }

    fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}

    fn poll(&mut self, _now: Ts, out: &mut Vec<VenueReport>) {
        // On whichever wake comes next, market or idle. The engine polls before
        // the strategy sees an event, so an order placed on one event cannot be
        // reported on that same wake -- nothing is held yet when it polls.
        out.extend(self.held.take().map(VenueReport::from));
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

/// Writes down what the clock read every time it was called, and submits once.
#[derive(Debug, Default)]
struct Clocked {
    submitted: bool,
    /// `(ingest_seq, ctx.now())` at each market event.
    at_events: Vec<(u64, Ts)>,
    /// `(the report's own time, ctx.now())` at each execution report.
    at_executions: Vec<(Ts, Ts)>,
}

impl Strategy for Clocked {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>) {
        self.at_events.push((event.meta().ingest_seq, ctx.now()));
        if !self.submitted {
            self.submitted = true;
            ctx.submit(buy());
        }
    }

    fn on_execution(&mut self, event: &ExecutionEvent, ctx: &mut Context<'_>) {
        self.at_executions.push((event.ts(), ctx.now()));
    }
}

/// Two trades and a venue that reports the order placed on the first one
/// during the second, stamped `report`. `Scripted` never idles, so the report
/// arrives on a market wake.
fn reported_on_the_next_event(report: Ts) -> Engine<Scripted, SpeaksLater, AllowAll, Clocked> {
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00"), trade(2, "100.00")]),
        SpeaksLater::stamping(report),
        AllowAll,
        Clocked::default(),
        CASH,
    );
    engine.run().expect("no source failure");
    engine
}

#[test]
fn a_report_received_after_the_event_moves_the_clock_before_the_strategy_reads_it() {
    // Live, the venue's reports come down another socket, so one can be
    // received after the market event in hand and polled on that same wake.
    // Only an idle wake used to take its time from a report. On a market wake
    // the fill was booked at its own later instant while the strategy, told of
    // it and then shown the event, read the earlier one -- and journalled what
    // it decided before the fill it was deciding on.
    let report = Ts::from_nanos(3_000_000_000);
    assert!(
        report > meta(2).local_recv_ts,
        "the fixture's report is the later"
    );
    let engine = reported_on_the_next_event(report);
    let stats = engine.stats();
    let strategy = engine.strategy();

    assert_eq!(stats.idle_wakes, 0, "polled on a market wake");
    assert_eq!(
        strategy.at_executions,
        vec![(report, report)],
        "told of the fill no earlier than the fill itself"
    );
    assert_eq!(
        strategy.at_events,
        vec![(1, meta(1).local_recv_ts), (2, report)],
        "and shown the event after it at the report's time, not the event's own"
    );
    assert_eq!(stats.clock_clamps, 0, "moving forward is not a clamp");
    assert_eq!(
        stats.last_ts,
        Some(report),
        "the period covered runs to the fill, or a closing checkpoint lands before it"
    );
}

#[test]
fn a_report_stamped_before_the_event_neither_moves_the_clock_nor_counts_a_clamp() {
    // The ordinary case, and every backtest with latency is made of it: a
    // simulated fill is stamped when it was made and delivered on a later
    // event. It asks nothing of the clock. Moving the clock to it would run the
    // clock backwards, and counting it as a clamp would make a stat that is
    // documented as always zero in a backtest non-zero in every one with
    // latency.
    let report = meta(1).local_recv_ts;
    let engine = reported_on_the_next_event(report);
    let stats = engine.stats();
    let strategy = engine.strategy();

    assert_eq!(
        strategy.at_executions,
        vec![(report, meta(2).local_recv_ts)],
        "delivered on the second event, and the clock stayed on it"
    );
    assert_eq!(strategy.at_events[1], (2, meta(2).local_recv_ts));
    assert_eq!(stats.clock_clamps, 0, "an earlier report is not a clamp");
    assert_eq!(stats.last_ts, Some(meta(2).local_recv_ts));
}

#[test]
fn a_report_stamped_before_the_clock_on_an_idle_wake_neither_moves_it_nor_counts_a_clamp() {
    // The test above, with the report delivered by an idle wake instead of a
    // market one. The idle path used to advance to the newest report
    // unconditionally, so the same report counted a clamp here and none there:
    // the count said which kind of wake happened to deliver it, and nothing
    // about the order the inputs arrived in.
    //
    // Arranged the shortest way: the report is stamped before the only event,
    // so nothing but an idle wake can deliver it. Live it takes a market event
    // acted on between the report's arrival and its delivery, or the host
    // clock stepping back; the engine cannot tell which, and neither is a
    // reason to move its clock or count anything.
    let report = Ts::from_nanos(1_000_000_000);
    assert!(
        report < meta(1).local_recv_ts,
        "the fixture's report is the earlier"
    );
    let mut engine = Engine::new(
        GoesQuiet::new(vec![trade(1, "100.00")], 2),
        SpeaksLater::stamping(report),
        AllowAll,
        Clocked::default(),
        CASH,
    );
    let stats = engine.run().expect("no source failure");

    assert_eq!(
        stats.fills, 1,
        "delivered, and only an idle wake could have"
    );
    assert_eq!(stats.idle_wakes, 2);
    assert_eq!(
        engine.strategy().at_executions,
        vec![(report, meta(1).local_recv_ts)],
        "told of it at the clock the event set, which did not move back"
    );
    assert_eq!(
        stats.clock_clamps, 0,
        "an earlier report is not a clamp on an idle wake either"
    );
    assert_eq!(stats.last_ts, Some(meta(1).local_recv_ts));
}

// --- M8.c: a venue can say it is not ready ---

/// A venue that is not ready for its first `refusals` questions, saying why
/// with `said`, and ready after that. Counts the questions and keeps what it was
/// sent, so a test can tell "refused" from "sent and declined".
#[derive(Debug)]
struct Readiness {
    said: Unready,
    refusals: std::cell::Cell<usize>,
    asked: std::cell::Cell<usize>,
    received: Vec<ClientOrderId>,
}

impl Readiness {
    /// Not ready for the first `refusals` questions.
    fn refusing(said: Unready, refusals: usize) -> Self {
        Self {
            said,
            refusals: std::cell::Cell::new(refusals),
            asked: std::cell::Cell::new(0),
            received: Vec::new(),
        }
    }

    /// Not ready, ever, as far as any test can ask.
    fn never(said: Unready) -> Self {
        Self::refusing(said, usize::MAX)
    }
}

impl ExecutionVenue for Readiness {
    fn ready(&self, _request: &OrderRequest) -> Result<(), Unready> {
        self.asked.set(self.asked.get() + 1);
        let left = self.refusals.get();
        if left == 0 {
            return Ok(());
        }
        self.refusals.set(left - 1);
        Err(self.said)
    }

    fn submit(&mut self, client_order_id: ClientOrderId, _r: &OrderRequest, _now: Ts) {
        self.received.push(client_order_id);
    }

    fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}

    fn poll(&mut self, _now: Ts, _out: &mut Vec<VenueReport>) {}
}

#[test]
fn an_order_the_venue_cannot_take_is_refused_and_never_written_down_as_sent() {
    // The defect this seam exists for, found by reading before a live venue was
    // written. By the time `ExecutionVenue::submit` runs, `on_submitted` has
    // journalled the order and the ledger holds it, so an adapter that refused
    // there could only answer with a `Rejected` -- which the journal records as
    // the venue declining an order it had been told about. For an order that
    // never left, that line is false. Asked first, the venue's "not now" is a
    // refusal, written as one, and nothing claims the order went anywhere.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00")]),
        Readiness::never(Unready::StreamDown),
        AllowAll,
        Recorder::default(),
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    let stats = engine.run().expect("no source failure");

    let t1 = meta(1).local_recv_ts;
    assert_eq!(
        seen.lock().expect("witness").heard,
        vec![Heard::Refused(
            ClientOrderId(1),
            RejectReason::Venue,
            crate::RefusedBy::Venue(Unready::StreamDown),
            t1
        )],
        "refused, saying why, and never written down as submitted"
    );
    assert!(
        engine.venue().received.is_empty(),
        "the venue was sent nothing"
    );
    assert_eq!(
        engine.strategy().executions,
        vec![ExecutionEvent::Rejected {
            client_order_id: ClientOrderId(1),
            reason: RejectReason::Venue,
            ts: t1,
        }],
        "the strategy hears a venue rejection, and not why"
    );
    assert_eq!((stats.refused, stats.submitted), (1, 0));
}

#[test]
fn an_order_the_venue_could_not_take_spends_none_of_the_days_count() {
    // Asked before the risk layer, so an order the venue turned away is not an
    // order the strategy got out. One a day, and the venue is not ready for
    // the first: the second is the day's first, and goes. Asked after, the
    // refused order would have spent the day's one, and the second would trip
    // the switch -- a venue that was briefly down would stop the strategy.
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00"), trade(2, "100.00")]),
        Readiness::refusing(Unready::QueueFull, 1),
        crate::risk::RiskEngine::new(crate::risk::Limits {
            max_orders_per_day: Some(1),
            ..crate::risk::Limits::default()
        }),
        Greedy,
        CASH,
    );
    engine.run().expect("no source failure");

    assert_eq!(
        engine.venue().received,
        vec![ClientOrderId(2)],
        "the order the venue could take was sent"
    );
    assert_eq!(
        engine.risk().orders_today(),
        1,
        "and it was the only one counted"
    );
    assert_eq!(engine.risk().tripped(), None, "so nothing tripped");
}

#[test]
fn a_malformed_order_is_the_seams_and_the_venue_is_not_asked() {
    // The seam first: a zero-size order is our bug whatever state the venue is
    // in, and asking a venue that is down about it would file our bug under the
    // venue's outage.
    #[derive(Debug, Default)]
    struct SendsZero;
    impl Strategy for SendsZero {
        fn on_market_event(&mut self, _event: &MarketEvent, ctx: &mut Context<'_>) {
            ctx.submit(OrderRequest {
                qty: "0".parse().expect("qty"),
                ..buy()
            });
        }
    }

    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00")]),
        Readiness::never(Unready::StreamDown),
        AllowAll,
        SendsZero,
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    engine.run().expect("no source failure");

    assert_eq!(
        seen.lock().expect("witness").heard,
        vec![Heard::Refused(
            ClientOrderId(1),
            RejectReason::Malformed,
            crate::RefusedBy::Seam,
            meta(1).local_recv_ts
        )]
    );
    assert_eq!(engine.venue().asked.get(), 0, "the venue was not asked");
}

// --- M8.c: a submission the record could not hold is never sent ---

#[test]
fn a_submission_the_record_could_not_hold_is_never_sent_and_nothing_after_it_is() {
    // Write-ahead was a call order until now: `on_submitted` ran before the
    // venue was told, and whether the line reached the disk changed nothing.
    // The record now answers, and an order it could not hold does not go --
    // nor does any order after it, because a record that failed once cannot
    // be trusted with the next line, and an order the record does not hold is
    // a position nobody knows about.
    //
    // A record that fails on the second of four submissions, against a venue
    // that is always ready.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness {
        withhold_at: Some(2),
        ..Witness::default()
    }));
    let mut engine = Engine::new(
        Scripted::new(vec![
            trade(1, "100.00"),
            trade(2, "100.00"),
            trade(3, "100.00"),
            trade(4, "100.00"),
        ]),
        Readiness::refusing(Unready::StreamDown, 0),
        crate::risk::RiskEngine::new(crate::risk::Limits::default()),
        Greedy,
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    let stats = engine.run().expect("no source failure");

    let t = |n| meta(n).local_recv_ts;
    let c = ClientOrderId;
    let by_record =
        |id, at| Heard::Refused(c(id), RejectReason::Venue, crate::RefusedBy::Record, at);
    assert_eq!(
        seen.lock().expect("witness").heard,
        vec![
            Heard::Submitted(c(1), t(1)),
            Heard::Submitted(c(2), t(2)),
            by_record(2, t(2)),
            by_record(3, t(3)),
            by_record(4, t(4)),
        ]
    );
    assert_eq!(
        engine.venue().received,
        vec![c(1)],
        "only the order the record holds was sent"
    );
    assert_eq!(
        engine.venue().asked.get(),
        2,
        "and once the record had failed, the venue was not even asked"
    );
    assert_eq!(
        engine.risk().orders_today(),
        2,
        "the withheld order had passed the risk layer; the two after it never reached it"
    );
    assert_eq!((stats.submitted, stats.refused), (1, 3));
}

// --- M8.c: a venue hands back reports, and the record hears what the strategy does not ---

/// What a strategy saw, in order: a market event or a report.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    Market(u64),
    Told(ExecutionEvent),
}

/// Drives one lifecycle and writes down everything it could read: each thing
/// it saw, with the clock, the position and the cash it read at that moment.
///
/// Order 1 on the first event; order 2 and a cancel of order 1 on the second;
/// order 3 and a cancel of it on the third.
#[derive(Debug, Default)]
struct Lifecycle {
    log: Vec<(Seen, Ts, Qty, Notional)>,
    notes_taken: usize,
}

impl Strategy for Lifecycle {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>) {
        let seq = event.meta().ingest_seq;
        self.log.push((
            Seen::Market(seq),
            ctx.now(),
            ctx.position(instrument()).qty,
            ctx.cash(),
        ));
        match seq {
            1 => {
                ctx.submit(buy());
            }
            2 => {
                ctx.submit(buy());
                ctx.cancel(ClientOrderId(1));
            }
            3 => {
                let third = ctx.submit(buy());
                ctx.cancel(third);
            }
            _ => {}
        }
    }

    fn on_execution(&mut self, event: &ExecutionEvent, ctx: &mut Context<'_>) {
        self.log.push((
            Seen::Told(event.clone()),
            ctx.now(),
            ctx.position(instrument()).qty,
            ctx.cash(),
        ));
    }

    fn take_notes(&mut self, _out: &mut Vec<crate::strategy::Note>) {
        self.notes_taken += 1;
    }
}

fn qty(s: &str) -> Qty {
    s.parse().expect("qty")
}

fn vid(n: u64) -> VenueOrderId {
    VenueOrderId(n.to_string())
}

fn fill_ids(order: u64, trade: u64) -> FillIds {
    FillIds {
        venue_order_id: vid(order),
        trade_id: quant_core::execution::VenueTradeId(trade.to_string()),
    }
}

fn cited(code: i64, msg: &str) -> quant_core::report::Cited {
    quant_core::report::Cited {
        http_status: Some(400),
        code,
        msg: quant_core::report::VenueText::new(msg),
    }
}

/// When the account stream is lost in [`talkative_lifecycle`]: after every
/// market event, on the idle wake that follows them.
const LOST_AT: Ts = Ts::from_nanos(2_500_000_000);

fn stream_lost(at: Ts) -> Notice {
    Notice::Account {
        change: quant_core::report::AccountChange::Lost {
            cause: quant_core::report::AccountCause::SocketClosed,
        },
        ts: at,
    }
}

/// The lifecycle [`Lifecycle`] drives, as a venue with nothing to say beside
/// an event would report it: order 1 accepted and filled in two parts, order 2
/// declined, order 3 accepted and cancelled. One batch per wake: four market
/// events, then one idle wake on which this venue has nothing to say.
fn plain_lifecycle() -> Says {
    let t = |n| meta(n).local_recv_ts;
    let c = ClientOrderId;
    Says::new(vec![
        vec![],
        vec![
            ExecutionEvent::Accepted {
                client_order_id: c(1),
                ts: t(2),
            },
            ExecutionEvent::Filled {
                client_order_id: c(1),
                fill: fill_of("0.4"),
                remaining: qty("0.6"),
                ts: t(2),
            },
        ],
        vec![
            ExecutionEvent::Filled {
                client_order_id: c(1),
                fill: fill_of("0.6"),
                remaining: qty("0"),
                ts: t(3),
            },
            ExecutionEvent::Rejected {
                client_order_id: c(2),
                reason: RejectReason::Venue,
                ts: t(3),
            },
        ],
        vec![
            ExecutionEvent::Accepted {
                client_order_id: c(3),
                ts: t(4),
            },
            ExecutionEvent::Cancelled {
                client_order_id: c(3),
                remaining: qty("1"),
                ts: t(4),
            },
        ],
        vec![],
    ])
}

/// The same lifecycle as a live venue would report it: every kind of detail
/// beside the events, notices among them stamped no later than their batch's
/// latest order report, and the account stream lost on the idle wake.
fn talkative_lifecycle() -> Says {
    let t = |n| meta(n).local_recv_ts;
    let c = ClientOrderId;
    let order = VenueReport::Order;
    let notice = VenueReport::Notice;
    Says::reporting(vec![
        vec![],
        vec![
            notice(Notice::Placed {
                client_order_id: c(1),
                venue_order_id: vid(8001),
                transact_ms: 1_789_742_815_743,
                ts: Ts::from_nanos(t(2).as_nanos() - 1),
            }),
            order(OrderReport::Accepted {
                client_order_id: c(1),
                venue_order_id: Some(vid(8001)),
                ts: t(2),
            }),
            order(OrderReport::Filled {
                client_order_id: c(1),
                fill: fill_of("0.4"),
                remaining: qty("0.6"),
                ids: Some(fill_ids(8001, 50001)),
                ts: t(2),
            }),
        ],
        vec![
            order(OrderReport::Filled {
                client_order_id: c(1),
                fill: fill_of("0.6"),
                remaining: qty("0"),
                ids: Some(fill_ids(8001, 50002)),
                ts: t(3),
            }),
            // The cancel of order 1 lost the race to its last fill.
            notice(Notice::CancelRefused {
                client_order_id: c(1),
                why: quant_core::report::CancelRefusal::NotOpen(cited(
                    -2011,
                    "Unknown order sent.",
                )),
                ts: t(3),
            }),
            order(OrderReport::Rejected {
                client_order_id: c(2),
                reason: RejectReason::Venue,
                why: Some(RejectedWhy::Declined {
                    cited: cited(-1013, "Filter failure: NOTIONAL"),
                    breach: Some(quant_core::instrument::FilterBreach::MinNotional),
                }),
                ts: t(3),
            }),
        ],
        vec![
            notice(Notice::Placed {
                client_order_id: c(3),
                venue_order_id: vid(8003),
                transact_ms: 1_789_742_815_744,
                ts: Ts::from_nanos(t(4).as_nanos() - 1),
            }),
            order(OrderReport::Accepted {
                client_order_id: c(3),
                venue_order_id: Some(vid(8003)),
                ts: t(4),
            }),
            order(OrderReport::Cancelled {
                client_order_id: c(3),
                remaining: qty("1"),
                ended: Some(Ended {
                    venue_order_id: vid(8003),
                    how: quant_core::report::EndedBy::Cancelled,
                }),
                ts: t(4),
            }),
        ],
        vec![notice(stream_lost(LOST_AT))],
    ])
}

fn lifecycle_against(venue: Says) -> (Lifecycle, Vec<Heard>, crate::EngineStats) {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        GoesQuiet::new(
            vec![
                trade(1, "100.00"),
                trade(2, "100.00"),
                trade(3, "100.00"),
                trade(4, "100.00"),
            ],
            1,
        ),
        venue,
        AllowAll,
        Lifecycle::default(),
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    let stats = engine.run().expect("no source failure");
    let strategy = std::mem::take(&mut engine.strategy);
    let heard = std::mem::take(&mut seen.lock().expect("witness").heard);
    (strategy, heard, stats)
}

/// Everything in `heard` that only a real venue could have said: the detail
/// beside each report, and every notice.
fn venue_detail(heard: &[Heard]) -> Vec<Heard> {
    heard
        .iter()
        .filter(|h| match h {
            Heard::Accepted(_, venue_order_id, _) => venue_order_id.is_some(),
            Heard::Rejected(_, _, why, _) => why.is_some(),
            Heard::Cancelled(_, _, ended, _) => ended.is_some(),
            Heard::Orphaned(_, ids, _) | Heard::Filled(_, _, ids, _, _) => ids.is_some(),
            Heard::Notice(..) => true,
            _ => false,
        })
        .cloned()
        .collect()
}

#[test]
fn a_strategy_sees_the_same_lifecycle_whichever_venue_reported_it() {
    // The seam's acceptance criterion. A live venue knows its own ids, the
    // trade ids, the codes it cited, that a cancel lost, that its stream
    // dropped, and the record needs every one of them -- while a strategy that
    // could read any of them could tell which venue it was wired to, which is a
    // bug. So the same lifecycle, reported by a venue with nothing beside its
    // events and by one that says everything a live venue says, must look
    // identical from inside the strategy: every event, the clock it read, its
    // position and cash at each moment, and how often it was asked for notes.
    let (plain, plain_heard, plain_stats) = lifecycle_against(plain_lifecycle());
    let (talkative, talkative_heard, talkative_stats) = lifecycle_against(talkative_lifecycle());

    // Non-vacuous first: four market events and six reports, the last fill
    // leaving the strategy holding the whole first order.
    assert_eq!(plain.log.len(), 10, "{:#?}", plain.log);
    assert_eq!(
        plain.log.last().map(|(_, _, held, _)| *held),
        Some(qty("1"))
    );
    assert_eq!(
        plain.notes_taken, 4,
        "once per market event, and not on the idle wake"
    );

    assert_eq!(talkative.log, plain.log, "what the strategy saw, and when");
    assert_eq!(
        talkative.notes_taken, plain.notes_taken,
        "and how often it was asked what it wrote down"
    );

    // And the record heard all of it from the one and none of it from the
    // other.
    let t = |n| meta(n).local_recv_ts;
    let c = ClientOrderId;
    let placed = |id, order: u64, ms, at| {
        Heard::Notice(
            Notice::Placed {
                client_order_id: c(id),
                venue_order_id: vid(order),
                transact_ms: ms,
                ts: at,
            },
            at,
        )
    };
    let early = |n| Ts::from_nanos(t(n).as_nanos() - 1);
    assert_eq!(venue_detail(&plain_heard), Vec::new());
    assert_eq!(
        venue_detail(&talkative_heard),
        vec![
            placed(1, 8001, 1_789_742_815_743, early(2)),
            Heard::Accepted(c(1), Some(vid(8001)), t(2)),
            Heard::Filled(
                c(1),
                qty("0.4"),
                Some(fill_ids(8001, 50001)),
                qty("0.6"),
                t(2)
            ),
            Heard::Filled(
                c(1),
                qty("0.6"),
                Some(fill_ids(8001, 50002)),
                qty("0"),
                t(3)
            ),
            Heard::Notice(
                Notice::CancelRefused {
                    client_order_id: c(1),
                    why: quant_core::report::CancelRefusal::NotOpen(cited(
                        -2011,
                        "Unknown order sent."
                    )),
                    ts: t(3),
                },
                t(3)
            ),
            Heard::Rejected(
                c(2),
                RejectReason::Venue,
                Some(RejectedWhy::Declined {
                    cited: cited(-1013, "Filter failure: NOTIONAL"),
                    breach: Some(quant_core::instrument::FilterBreach::MinNotional),
                }),
                t(3)
            ),
            placed(3, 8003, 1_789_742_815_744, early(4)),
            Heard::Accepted(c(3), Some(vid(8003)), t(4)),
            Heard::Cancelled(
                c(3),
                qty("1"),
                Some(Ended {
                    venue_order_id: vid(8003),
                    how: quant_core::report::EndedBy::Cancelled,
                }),
                t(4)
            ),
            Heard::Notice(stream_lost(LOST_AT), LOST_AT),
        ]
    );
    assert_eq!((plain_stats.notices, talkative_stats.notices), (0, 4));
    assert_eq!(
        talkative_stats.execution_events, plain_stats.execution_events,
        "a notice is not an execution event"
    );
}

/// Counts every call the engine makes into it.
#[derive(Debug, Default)]
struct Calls {
    markets: usize,
    executions: usize,
    notes: usize,
}

impl Strategy for Calls {
    fn on_market_event(&mut self, _event: &MarketEvent, _ctx: &mut Context<'_>) {
        self.markets += 1;
    }

    fn on_execution(&mut self, _event: &ExecutionEvent, _ctx: &mut Context<'_>) {
        self.executions += 1;
    }

    fn take_notes(&mut self, _out: &mut Vec<crate::strategy::Note>) {
        self.notes += 1;
    }
}

#[test]
fn a_notice_on_a_silent_market_is_recorded_when_heard_and_tells_the_strategy_nothing() {
    // The account stream drops while the market is quiet. Nothing happened to
    // any order, so the strategy has nothing to be told -- but the record must
    // hold the loss, dated when it was heard rather than at the last market
    // event, which during a silence could be minutes earlier. So a notice moves
    // the clock like any report, and a wake that brought only notices calls no
    // strategy method at all.
    let at = Ts::from_nanos(3_000_000_000);
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        GoesQuiet::new(vec![trade(1, "100.00")], 1),
        Says::reporting(vec![vec![], vec![VenueReport::Notice(stream_lost(at))]]),
        AllowAll,
        Calls::default(),
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    let stats = engine.run().expect("no source failure");

    assert_eq!(stats.idle_wakes, 1, "the notice came on an idle wake");
    assert_eq!(
        seen.lock().expect("witness").heard,
        vec![Heard::Notice(stream_lost(at), at)],
        "recorded, at the instant it was heard"
    );
    assert_eq!(stats.notices, 1);
    assert_eq!(
        stats.last_ts,
        Some(at),
        "and the period covered runs to it, so a closing line cannot land before it"
    );
    let calls = engine.strategy();
    assert_eq!(
        (calls.markets, calls.executions, calls.notes),
        (1, 0, 1),
        "the market wake called the strategy; the notice's wake did not"
    );
}

/// Writes what it sees and is told into the log the observer writes to,
/// submits on the first event, and writes a note when told of a fill.
#[derive(Debug)]
struct Tells {
    log: std::sync::Arc<std::sync::Mutex<Witness>>,
    pending: Vec<crate::strategy::Note>,
}

impl Strategy for Tells {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>) {
        let seq = event.meta().ingest_seq;
        self.log
            .lock()
            .expect("log")
            .heard
            .push(Heard::Saw(seq, ctx.now()));
        if seq == 1 {
            ctx.submit(buy());
        }
    }

    fn on_execution(&mut self, event: &ExecutionEvent, ctx: &mut Context<'_>) {
        self.log
            .lock()
            .expect("log")
            .heard
            .push(Heard::Told(event.clone(), ctx.now()));
        if matches!(event, ExecutionEvent::Filled { .. }) {
            self.pending.push(crate::strategy::Note::new(
                "filled",
                serde_json::Value::Null,
            ));
        }
    }

    fn take_notes(&mut self, out: &mut Vec<crate::strategy::Note>) {
        out.append(&mut self.pending);
    }
}

/// A scripted venue that writes into the same log when it is sent an order.
#[derive(Debug)]
struct Hears {
    log: std::sync::Arc<std::sync::Mutex<Witness>>,
    says: Says,
}

impl ExecutionVenue for Hears {
    fn ready(&self, _request: &OrderRequest) -> Result<(), Unready> {
        Ok(())
    }

    fn submit(&mut self, client_order_id: ClientOrderId, _r: &OrderRequest, _now: Ts) {
        self.log
            .lock()
            .expect("log")
            .heard
            .push(Heard::Sent(client_order_id));
    }

    fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}

    fn poll(&mut self, now: Ts, out: &mut Vec<VenueReport>) {
        self.says.poll(now, out);
    }
}

#[test]
fn the_record_hears_each_report_before_the_strategy_does() {
    // The write-ahead rule across every party, in one log: an order is written
    // down before the venue is sent it, a venue's fact is written down where it
    // arrived, and each report is written down -- with its detail -- before the
    // strategy is told the event, so a crash cannot erase something the
    // strategy has already acted on. The note the strategy wrote on hearing of
    // the fill comes last, at the end of the wake.
    let log = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let t2 = meta(2).local_recv_ts;
    let c1 = ClientOrderId(1);
    let placed = Notice::Placed {
        client_order_id: c1,
        venue_order_id: vid(8001),
        transact_ms: 1_789_742_815_743,
        ts: t2,
    };
    let says = Says::reporting(vec![
        vec![],
        vec![
            VenueReport::Notice(placed.clone()),
            VenueReport::Order(OrderReport::Accepted {
                client_order_id: c1,
                venue_order_id: Some(vid(8001)),
                ts: t2,
            }),
            VenueReport::Order(OrderReport::Filled {
                client_order_id: c1,
                fill: fill_of("1"),
                remaining: qty("0"),
                ids: Some(fill_ids(8001, 50001)),
                ts: t2,
            }),
        ],
    ]);
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00"), trade(2, "100.00")]),
        Hears {
            log: std::sync::Arc::clone(&log),
            says,
        },
        AllowAll,
        Tells {
            log: std::sync::Arc::clone(&log),
            pending: Vec::new(),
        },
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&log)));
    engine.run().expect("no source failure");

    let t1 = meta(1).local_recv_ts;
    assert_eq!(
        log.lock().expect("log").heard,
        vec![
            Heard::Saw(1, t1),
            Heard::Submitted(c1, t1),
            Heard::Sent(c1),
            Heard::Notice(placed, t2),
            Heard::Accepted(c1, Some(vid(8001)), t2),
            Heard::Told(
                ExecutionEvent::Accepted {
                    client_order_id: c1,
                    ts: t2,
                },
                t2
            ),
            Heard::Filled(c1, qty("1"), Some(fill_ids(8001, 50001)), qty("0"), t2),
            Heard::Told(
                ExecutionEvent::Filled {
                    client_order_id: c1,
                    fill: fill_of("1"),
                    remaining: qty("0"),
                    ts: t2,
                },
                t2
            ),
            Heard::Saw(2, t2),
            Heard::Note("filled", t2),
        ]
    );
}

// --- M8.c: only an order the engine is working reaches the strategy ---

#[test]
fn a_report_about_an_order_the_engine_is_not_working_is_written_down_and_kept_from_the_strategy() {
    // A live venue reports on the account, not on this process, and a report
    // can name an order this engine never minted or one whose end it has
    // already heard. Such a report used to reach the strategy: an orphaned
    // fill was told to `on_execution` though the portfolio never booked it, so
    // a strategy could react to a fill its own position did not contain. Now
    // it is written down -- a fill as an orphan, anything else as a stray --
    // and goes no further.
    let t2 = meta(2).local_recv_ts;
    let (c8, c9) = (ClientOrderId(8), ClientOrderId(9));
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00"), trade(2, "100.00")]),
        Says::new(vec![
            vec![],
            vec![
                ExecutionEvent::Accepted {
                    client_order_id: c9,
                    ts: t2,
                },
                ExecutionEvent::Cancelled {
                    client_order_id: c9,
                    remaining: qty("1"),
                    ts: t2,
                },
                ExecutionEvent::Filled {
                    client_order_id: c8,
                    fill: fill_of("1"),
                    remaining: qty("0"),
                    ts: t2,
                },
            ],
        ]),
        AllowAll,
        Recorder::default(),
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    let stats = engine.run().expect("no source failure");

    assert_eq!(
        engine.strategy().executions,
        Vec::new(),
        "the strategy was told of none of them"
    );
    assert_eq!(
        seen.lock().expect("witness").heard,
        vec![
            Heard::Submitted(ClientOrderId(1), meta(1).local_recv_ts),
            Heard::Stray(
                OrderReport::Accepted {
                    client_order_id: c9,
                    venue_order_id: None,
                    ts: t2,
                },
                t2
            ),
            Heard::Stray(
                OrderReport::Cancelled {
                    client_order_id: c9,
                    remaining: qty("1"),
                    ended: None,
                    ts: t2,
                },
                t2
            ),
            Heard::Orphaned(c8, None, t2),
        ],
        "each written down, as what it is"
    );
    assert_eq!(stats.withheld, 3);
    assert_eq!(
        (stats.execution_events, stats.fills),
        (3, 1),
        "counted as reports, and the fill as a fill: something did trade"
    );
    assert_eq!(engine.portfolio().fills(), 0, "but nothing was booked");
}

// --- M8.c: settle, which books what the venue still says and tells nobody ---

/// Fills the first order it is sent, on its `on_poll`th poll, stamped `at`.
///
/// A live venue at shutdown: the market source has ended, and a fill for an
/// order sent before it ended is still on its way.
#[derive(Debug)]
struct FillsLate {
    on_poll: usize,
    polls: usize,
    at: Ts,
    sent: Option<ClientOrderId>,
}

impl ExecutionVenue for FillsLate {
    fn ready(&self, _request: &OrderRequest) -> Result<(), Unready> {
        Ok(())
    }

    fn submit(&mut self, client_order_id: ClientOrderId, _r: &OrderRequest, _now: Ts) {
        self.sent.get_or_insert(client_order_id);
    }

    fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}

    fn poll(&mut self, _now: Ts, out: &mut Vec<VenueReport>) {
        self.polls += 1;
        if self.polls == self.on_poll {
            if let Some(client_order_id) = self.sent {
                out.push(VenueReport::from(ExecutionEvent::Filled {
                    client_order_id,
                    fill: fill_of("1"),
                    remaining: qty("0"),
                    ts: self.at,
                }));
            }
        }
    }
}

#[test]
fn settle_books_a_late_fill_and_tells_the_strategy_nothing() {
    // The source has ended with an order still working, and its fill arrives
    // two polls later. The fill is real money: it must be booked, journalled
    // and tallied before the closing checkpoint, or the record ends short of
    // the account. But the market the strategy trades on has ended, so the
    // strategy is told nothing -- a backtest never delivers a report still in
    // flight at the end of its data, and a strategy acting during the drain
    // would leave decisions in the journal that no replay has.
    let at = Ts::from_nanos(3_000_000_000);
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00")]),
        FillsLate {
            on_poll: 3,
            polls: 0,
            at,
            sent: None,
        },
        AllowAll,
        Recorder::default(),
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    let stats = engine.run().expect("no source failure");
    assert_eq!(stats.fills, 0, "nothing had arrived when the source ended");
    assert_eq!(engine.working(), vec![ClientOrderId(1)]);

    let settled: Vec<crate::Settled> = (0..3).map(|_| engine.settle()).collect();
    assert_eq!(
        settled,
        vec![
            crate::Settled {
                delivered: 0,
                working: vec![ClientOrderId(1)],
            },
            crate::Settled {
                delivered: 1,
                working: Vec::new(),
            },
            crate::Settled {
                delivered: 0,
                working: Vec::new(),
            },
        ]
    );
    let stats = engine.stats();
    assert_eq!(stats.fills, 1);
    assert_eq!(engine.portfolio().fills(), 1, "booked");
    let fills: Vec<Heard> = seen
        .lock()
        .expect("witness")
        .heard
        .iter()
        .filter(|h| matches!(h, Heard::Filled(..)))
        .cloned()
        .collect();
    assert_eq!(
        fills,
        vec![Heard::Filled(
            ClientOrderId(1),
            qty("1"),
            None,
            qty("0"),
            at
        )],
        "written down once"
    );
    assert_eq!(
        engine.strategy().executions,
        Vec::new(),
        "and the strategy was told nothing"
    );
    assert_eq!(
        stats.last_ts,
        Some(at),
        "the period covered runs to the fill, so the closing checkpoint cannot land before it"
    );
}

/// Raises doubt about the first order it is sent, on the poll after, and is
/// not ready for anything while that order's outcome is unknown.
#[derive(Debug, Default)]
struct Doubts {
    sent: Option<ClientOrderId>,
    raised: bool,
}

impl ExecutionVenue for Doubts {
    fn ready(&self, _request: &OrderRequest) -> Result<(), Unready> {
        if self.raised {
            Err(Unready::InDoubt)
        } else {
            Ok(())
        }
    }

    fn submit(&mut self, client_order_id: ClientOrderId, _r: &OrderRequest, _now: Ts) {
        self.sent.get_or_insert(client_order_id);
    }

    fn cancel(&mut self, _id: ClientOrderId, _now: Ts) {}

    fn poll(&mut self, now: Ts, out: &mut Vec<VenueReport>) {
        if let (Some(client_order_id), false) = (self.sent, self.raised) {
            self.raised = true;
            out.push(VenueReport::Notice(Notice::Doubt {
                client_order_id,
                change: quant_core::report::DoubtChange::Raised {
                    cause: quant_core::report::DoubtCause::NoAnswer,
                    said: None,
                },
                ts: now,
            }));
        }
    }
}

#[test]
fn an_order_in_doubt_is_still_working_when_the_run_ends() {
    // An unknown outcome is not an event: the order stays in the ledger, the
    // venue refuses new orders until it is settled, and at the end the drain
    // names it as still working rather than pretending it ended. Slice (d)'s
    // lookup is what will settle it; until then it is loud and stuck.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Witness::default()));
    let mut engine = Engine::new(
        Scripted::new(vec![trade(1, "100.00"), trade(2, "100.00")]),
        Doubts::default(),
        AllowAll,
        Greedy,
        CASH,
    )
    .observing_fills(Box::new(std::sync::Arc::clone(&seen)));
    engine.run().expect("no source failure");

    let t2 = meta(2).local_recv_ts;
    let heard = seen.lock().expect("witness").heard.clone();
    assert!(
        heard.contains(&Heard::Refused(
            ClientOrderId(2),
            RejectReason::Venue,
            crate::RefusedBy::Venue(Unready::InDoubt),
            t2
        )),
        "the second order was refused while the first was in doubt: {heard:?}"
    );
    assert_eq!(
        engine.settle().working,
        vec![ClientOrderId(1)],
        "and the first is still working when the run ends"
    );
}
