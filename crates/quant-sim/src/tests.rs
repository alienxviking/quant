//! Tests for the fill model.
//!
//! Each of these pins one of the assumptions in the crate docs, because those
//! assumptions are where every backtest number comes from. The ones worth the
//! most are the two that check the simulator is *pessimistic*: a resting order
//! is not filled by a print at its own price, and a market order pays for the
//! levels it consumes.

use quant_book::Book;
use quant_core::event::{BookSnapshot, EventMeta, Gap, GapCause, Level, MarketEvent, Side, Trade};
use quant_core::execution::{
    ClientOrderId, ExecutionEvent, OrderKind, OrderRequest, RejectReason, TimeInForce,
};
use quant_core::instrument::{
    Exchange, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
};
use quant_core::time::Ts;
use quant_engine::ExecutionVenue;

use crate::{SimStats, SimulatedVenue};

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

fn meta() -> EventMeta {
    EventMeta {
        instrument: instrument(),
        exchange_ts: Ts::from_nanos(1),
        local_recv_ts: Ts::from_nanos(2),
        ingest_seq: 1,
    }
}

fn level(px: &str, qty: &str) -> Level {
    Level::new(px.parse().expect("px"), qty.parse().expect("qty"))
}

const NOW: Ts = Ts::from_nanos(42);

/// A three-deep book on each side, so walking is observable.
fn book() -> Book {
    let mut book = Book::new();
    book.apply_snapshot(&BookSnapshot {
        meta: meta(),
        last_update_id: 10,
        bids: vec![level("99.0", "1"), level("98.0", "2"), level("97.0", "5")],
        asks: vec![
            level("101.0", "1"),
            level("102.0", "2"),
            level("103.0", "5"),
        ],
    });
    book
}

fn dark_book() -> Book {
    Book::new()
}

fn tick() -> MarketEvent {
    MarketEvent::BookSnapshot(BookSnapshot {
        meta: meta(),
        last_update_id: 10,
        bids: vec![level("99.0", "1")],
        asks: vec![level("101.0", "1")],
    })
}

fn print_at(px: &str) -> MarketEvent {
    MarketEvent::Trade(Trade {
        meta: meta(),
        px: px.parse().expect("px"),
        qty: "1".parse().expect("qty"),
        aggressor: Side::Buy,
        venue_trade_id: 1,
    })
}

fn order(side: Side, qty: &str, kind: OrderKind) -> OrderRequest {
    OrderRequest {
        instrument: instrument(),
        side,
        qty: qty.parse().expect("qty"),
        kind,
        time_in_force: TimeInForce::Gtc,
    }
}

fn limit(px: &str) -> OrderKind {
    OrderKind::Limit {
        limit: px.parse().expect("px"),
    }
}

/// Submit, then let one event go by, and collect what came out.
fn run(
    venue: &mut SimulatedVenue,
    request: OrderRequest,
    event: &MarketEvent,
    book: &Book,
) -> Vec<ExecutionEvent> {
    venue.submit(ClientOrderId(1), &request, NOW);
    venue.observe(event, book, NOW);
    let mut out = Vec::new();
    venue.poll(&mut out);
    out
}

fn fill_of(events: &[ExecutionEvent]) -> Option<(String, String)> {
    events.iter().find_map(|e| match e {
        ExecutionEvent::Filled { fill, .. } => Some((fill.px.to_string(), fill.qty.to_string())),
        _ => None,
    })
}

#[test]
fn nothing_happens_at_submission() {
    // The other half of the engine's step order. If the venue filled here, an
    // order could trade against the event that prompted it whatever the engine
    // did.
    let mut venue = SimulatedVenue::new();
    venue.submit(
        ClientOrderId(1),
        &order(Side::Buy, "1", OrderKind::Market),
        NOW,
    );
    let mut out = Vec::new();
    venue.poll(&mut out);
    assert!(out.is_empty(), "not even an acceptance: {out:?}");
    assert_eq!(venue.open_orders(), 1);
}

#[test]
fn a_small_market_order_pays_the_touch() {
    let mut venue = SimulatedVenue::new();
    let events = run(
        &mut venue,
        order(Side::Buy, "0.5", OrderKind::Market),
        &tick(),
        &book(),
    );
    assert_eq!(fill_of(&events), Some(("101".to_owned(), "0.5".to_owned())));
    assert_eq!(venue.stats().multi_level_fills, 0);
}

#[test]
fn a_market_order_walks_the_book_and_pays_the_average() {
    // The assumption that separates a simulator from a wish. Buying 2.5 against
    // asks of 1 @ 101, 2 @ 102 takes 1 at 101 and 1.5 at 102:
    //   (1 * 101 + 1.5 * 102) / 2.5 = 254 / 2.5 = 101.60
    let mut venue = SimulatedVenue::new();
    let events = run(
        &mut venue,
        order(Side::Buy, "2.5", OrderKind::Market),
        &tick(),
        &book(),
    );
    assert_eq!(
        fill_of(&events),
        Some(("101.6".to_owned(), "2.5".to_owned())),
        "size has to cost something, or a strategy learns to trade sizes that do not exist"
    );
    assert_eq!(venue.stats().multi_level_fills, 1);
}

#[test]
fn a_sell_walks_the_bids_not_the_asks() {
    // The inversion `Book::takeable` is named to prevent. Selling 1.5 against
    // bids of 1 @ 99, 2 @ 98 gets (1 * 99 + 0.5 * 98) / 1.5 = 148 / 1.5 = 98.666...
    let mut venue = SimulatedVenue::new();
    let events = run(
        &mut venue,
        order(Side::Sell, "1.5", OrderKind::Market),
        &tick(),
        &book(),
    );
    let (px, qty) = fill_of(&events).expect("a fill");
    assert_eq!(qty, "1.5");
    assert!(
        px.starts_with("98.66666"),
        "a seller takes bids and gets less than the touch: {px}"
    );
}

#[test]
fn a_market_order_the_book_cannot_fill_is_cancelled_not_left_resting() {
    // A market order is immediate by nature. Leaving the remainder to rest would
    // put it at a price nobody chose.
    let mut venue = SimulatedVenue::new();
    let events = run(
        &mut venue,
        order(Side::Buy, "100", OrderKind::Market),
        &tick(),
        &book(),
    );
    assert!(matches!(
        events.last(),
        Some(ExecutionEvent::Cancelled { .. })
    ));
    assert_eq!(venue.stats().exhausted_book, 1);
    assert_eq!(venue.open_orders(), 0);
    // It did fill what it could: 1 + 2 + 5 = 8 of the 100 asked for.
    let (_, qty) = fill_of(&events).expect("a partial fill");
    assert_eq!(qty, "8");
}

#[test]
fn a_resting_buy_is_not_filled_by_a_print_at_its_own_price() {
    // The conservative half of the model, and the one most simulators get
    // wrong. A print at our price proves somebody at our price was filled, not
    // that we were -- we arrived last.
    let mut venue = SimulatedVenue::new();
    let events = run(
        &mut venue,
        order(Side::Buy, "1", limit("100.0")),
        &print_at("100.0"),
        &book(),
    );
    assert!(fill_of(&events).is_none(), "{events:?}");
    assert_eq!(venue.open_orders(), 1, "still working");
}

#[test]
fn a_resting_buy_fills_when_the_market_trades_through_it() {
    // Everything at our price must have been consumed for the print to happen
    // below it, so we would have been reached.
    let mut venue = SimulatedVenue::new();
    let mut book = book();
    // Move the ask down so there is something to buy at or below our limit.
    book.apply_snapshot(&BookSnapshot {
        meta: meta(),
        last_update_id: 20,
        bids: vec![level("98.0", "1")],
        asks: vec![level("99.0", "1")],
    });
    let events = run(
        &mut venue,
        order(Side::Buy, "1", limit("100.0")),
        &print_at("99.0"),
        &book,
    );
    let (px, _) = fill_of(&events).expect("a fill: the market traded through");
    assert_eq!(px, "99", "and never worse than the limit");
}

#[test]
fn a_resting_order_never_pays_worse_than_its_limit() {
    // Walking the book must stop at the limit, or a limit order is a market
    // order with extra steps.
    let mut venue = SimulatedVenue::new();
    let mut book = Book::new();
    book.apply_snapshot(&BookSnapshot {
        meta: meta(),
        last_update_id: 10,
        bids: vec![level("98.0", "1")],
        // Only the first level is acceptable to a buy limited at 100.
        asks: vec![level("99.0", "1"), level("101.0", "5")],
    });
    let events = run(
        &mut venue,
        order(Side::Buy, "3", limit("100.0")),
        &print_at("99.0"),
        &book,
    );
    let (px, qty) = fill_of(&events).expect("a partial fill");
    assert_eq!(px, "99");
    assert_eq!(qty, "1", "it stopped at the limit, not at the size");
    assert_eq!(venue.open_orders(), 1, "the rest keeps working");
}

#[test]
fn a_market_order_with_no_book_is_refused_not_guessed() {
    // The expected outcome after a disconnect, with its own reason so it does
    // not bury the unexpected ones.
    let mut venue = SimulatedVenue::new();
    let gap = MarketEvent::Gap(Gap {
        meta: meta(),
        cause: GapCause::Disconnect,
        last_good_ts: Ts::from_nanos(1),
    });
    let events = run(
        &mut venue,
        order(Side::Buy, "1", OrderKind::Market),
        &gap,
        &dark_book(),
    );
    assert!(matches!(
        events
            .iter()
            .find(|e| matches!(e, ExecutionEvent::Rejected { .. })),
        Some(ExecutionEvent::Rejected {
            reason: RejectReason::NoMarket,
            ..
        })
    ));
    assert_eq!(venue.stats().no_market, 1);
    assert_eq!(venue.open_orders(), 0);
}

#[test]
fn an_ioc_limit_that_cannot_fill_immediately_is_cancelled() {
    let mut venue = SimulatedVenue::new();
    let request = OrderRequest {
        time_in_force: TimeInForce::Ioc,
        ..order(Side::Buy, "1", limit("90.0"))
    };
    let events = run(&mut venue, request, &tick(), &book());
    assert!(matches!(
        events.last(),
        Some(ExecutionEvent::Cancelled { .. })
    ));
    assert_eq!(venue.open_orders(), 0);
}

#[test]
fn a_cancel_after_a_fill_says_nothing_because_it_lost_the_race() {
    // Not an oversight. The cancel lost, and the fill it lost to was already
    // reported -- inventing a Cancelled here would tell a strategy its order was
    // withdrawn when it had actually traded.
    let mut venue = SimulatedVenue::new();
    venue.submit(
        ClientOrderId(1),
        &order(Side::Buy, "0.5", OrderKind::Market),
        NOW,
    );
    venue.observe(&tick(), &book(), NOW);
    let mut out = Vec::new();
    venue.poll(&mut out);
    assert!(fill_of(&out).is_some());

    venue.cancel(ClientOrderId(1), NOW);
    let mut after = Vec::new();
    venue.poll(&mut after);
    assert!(after.is_empty(), "{after:?}");
    assert_eq!(venue.stats().cancelled, 0);
}

#[test]
fn a_cancel_before_a_fill_returns_the_unfilled_size() {
    let mut venue = SimulatedVenue::new();
    venue.submit(ClientOrderId(1), &order(Side::Buy, "1", limit("90.0")), NOW);
    venue.cancel(ClientOrderId(1), NOW);
    let mut out = Vec::new();
    venue.poll(&mut out);
    assert!(matches!(
        out.first(),
        Some(ExecutionEvent::Cancelled { remaining, .. }) if remaining.to_string() == "1"
    ));
    assert_eq!(venue.open_orders(), 0);
}

#[test]
fn an_order_is_accepted_once_and_only_once() {
    let mut venue = SimulatedVenue::new();
    venue.submit(ClientOrderId(1), &order(Side::Buy, "1", limit("90.0")), NOW);
    for _ in 0..3 {
        venue.observe(&tick(), &book(), NOW);
    }
    let mut out = Vec::new();
    venue.poll(&mut out);
    let accepts = out
        .iter()
        .filter(|e| matches!(e, ExecutionEvent::Accepted { .. }))
        .count();
    assert_eq!(accepts, 1, "{out:?}");
}

#[test]
fn a_simulated_venue_offers_no_venue_order_id() {
    // Inventing one would make backtest output look like it came from an
    // exchange, and it is the field somebody reconciles a statement against.
    let mut venue = SimulatedVenue::new();
    let events = run(
        &mut venue,
        order(Side::Buy, "1", limit("90.0")),
        &tick(),
        &book(),
    );
    assert!(matches!(
        events.first(),
        Some(ExecutionEvent::Accepted {
            venue_order_id: None,
            ..
        })
    ));
}

#[test]
fn every_fill_is_free_and_the_caveats_say_so() {
    // The engine contract requires the absence of costs to be in the output, not
    // merely known. This test is what keeps the list honest if fees arrive.
    let mut venue = SimulatedVenue::new();
    let events = run(
        &mut venue,
        order(Side::Buy, "0.5", OrderKind::Market),
        &tick(),
        &book(),
    );
    let ExecutionEvent::Filled { fill, .. } = events
        .iter()
        .find(|e| matches!(e, ExecutionEvent::Filled { .. }))
        .expect("a fill")
    else {
        unreachable!()
    };
    assert_eq!(fill.fee.raw(), 0);
    assert!(SimStats::caveats().iter().any(|c| c.contains("no fees")));
}

#[test]
fn a_notional_larger_than_i64_would_hold_is_computed_in_128_bits() {
    // A price near 1e5 and a size near 1e5, both scaled by 1e8, multiply to
    // about 1e26 -- four orders of magnitude past i64. In i64 this wraps and
    // produces a plausible wrong number, which is the worst failure a money
    // calculation has.
    let mut venue = SimulatedVenue::new();
    let mut book = Book::new();
    book.apply_snapshot(&BookSnapshot {
        meta: meta(),
        last_update_id: 10,
        bids: vec![level("50000.0", "100000")],
        asks: vec![level("76650.0", "100000")],
    });
    let events = run(
        &mut venue,
        order(Side::Buy, "100000", OrderKind::Market),
        &tick(),
        &book,
    );
    assert_eq!(
        fill_of(&events),
        Some(("76650".to_owned(), "100000".to_owned()))
    );
}
