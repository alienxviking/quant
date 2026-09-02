//! Tests for the strategy and the equity recorder.
//!
//! The integration tests wire the *real* engine, the *real* simulated venue and
//! the real strategy against a scripted price path, because the properties worth
//! checking here — that a signal turns into a position, that a gap stops trading,
//! that a hole in the curve is visible — are properties of the whole wiring and
//! not of any one piece.

use quant_core::event::{BookSnapshot, EventMeta, Gap, GapCause, Level, MarketEvent};
use quant_core::fixed::{Notional, Px};
use quant_core::instrument::{
    Exchange, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
};
use quant_core::source::{EventSource, SourceError};
use quant_core::time::Ts;
use quant_engine::{AllowAll, Engine};
use quant_sim::SimulatedVenue;

use crate::equity::{EquityCurve, EquityPoint};
use crate::ma::MaConfig;
use crate::{MaCrossover, Recorded};

const SECOND: i64 = 1_000_000_000;
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

/// What each scripted step is.
enum Step {
    /// A book at this mid, one unit a side, a tick wide.
    Book(i64),
    /// A disconnect: the book is cleared.
    Gap,
}

/// One event per second, so the strategy's 1s sampling takes every one.
struct Script {
    events: std::vec::IntoIter<MarketEvent>,
}

impl Script {
    fn new(steps: &[Step]) -> Self {
        let mut events = Vec::new();
        for (i, step) in steps.iter().enumerate() {
            let seq = u64::try_from(i).expect("small") + 1;
            let n = i64::try_from(i).expect("small");
            let meta = EventMeta {
                instrument: instrument(),
                exchange_ts: Ts::from_nanos(n * SECOND),
                local_recv_ts: Ts::from_nanos(n * SECOND),
                ingest_seq: seq,
            };
            events.push(match step {
                Step::Book(mid) => MarketEvent::BookSnapshot(BookSnapshot {
                    meta,
                    // Rising ids so each snapshot re-anchors rather than being
                    // ignored as stale.
                    last_update_id: 100 + seq,
                    bids: vec![Level::new(
                        Px::from_raw((mid - 1) * quant_core::SCALE),
                        "1".parse().expect("qty"),
                    )],
                    asks: vec![Level::new(
                        Px::from_raw((mid + 1) * quant_core::SCALE),
                        "1".parse().expect("qty"),
                    )],
                }),
                Step::Gap => MarketEvent::Gap(Gap {
                    meta,
                    cause: GapCause::Disconnect,
                    last_good_ts: Ts::from_nanos(0),
                }),
            });
        }
        Self {
            events: events.into_iter(),
        }
    }
}

impl EventSource for Script {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        self.events.next().map(Ok)
    }
}

type Wired = Engine<Script, SimulatedVenue, AllowAll, Recorded<MaCrossover>>;

/// Run a price path through the whole wiring.
fn run(steps: &[Step]) -> Wired {
    let instrument = instrument();
    let strategy = Recorded::new(
        MaCrossover::new(MaConfig {
            instrument,
            fast: 2,
            slow: 3,
            interval: SECOND,
            qty: "0.001".parse().expect("qty"),
        }),
        instrument,
        SECOND,
    );
    let mut engine = Engine::new(
        Script::new(steps),
        SimulatedVenue::new(),
        AllowAll,
        strategy,
        CASH,
    );
    engine.run().expect("the script cannot fail");
    engine
}

/// Rise long enough to cross up, then fall long enough to cross back.
fn up_then_down() -> Vec<Step> {
    let mut steps = Vec::new();
    for mid in [100, 100, 100, 100, 101, 103, 106, 110] {
        steps.push(Step::Book(mid));
    }
    for mid in [108, 104, 99, 95, 90, 85] {
        steps.push(Step::Book(mid));
    }
    steps
}

#[test]
fn a_rising_path_opens_a_position_and_a_falling_one_closes_it() {
    let engine = run(&up_then_down());
    let ma = engine.strategy().inner().stats();
    assert!(ma.entries >= 1, "an up-cross must open something: {ma:?}");
    assert!(ma.exits >= 1, "and a down-cross must close it");
    assert!(
        engine.portfolio().position(instrument()).is_flat(),
        "the path ends below where it started, so it ends flat"
    );
    assert!(engine.portfolio().fills() >= 2);
}

#[test]
fn a_flat_path_never_trades() {
    // No crossing, no orders. Guards against a strategy that fires on the first
    // sample where both averages become defined.
    let steps: Vec<Step> = (0..20).map(|_| Step::Book(100)).collect();
    let engine = run(&steps);
    assert_eq!(engine.strategy().inner().stats().crossings, 0);
    assert_eq!(engine.stats().submitted, 0);
    assert_eq!(engine.portfolio().fills(), 0);
}

#[test]
fn the_indicator_does_not_advance_while_the_book_is_dark() {
    // Not by checking for gaps. A gap clears the book, so there is no mid to
    // sample -- the strategy has no rule about gaps at all, and that is the
    // point.
    let mut steps = vec![Step::Book(100), Step::Book(100), Step::Book(100)];
    steps.push(Step::Gap);
    for _ in 0..5 {
        // Trades would arrive here in a real capture; with the book cleared
        // there is nothing to price, so these are further gaps.
        steps.push(Step::Gap);
    }
    let engine = run(&steps);
    let ma = engine.strategy().inner().stats();
    assert_eq!(ma.samples, 3, "only the three live ticks were sampled");
    assert_eq!(ma.blind_intervals, 6, "and the rest were counted as blind");
    assert_eq!(engine.stats().submitted, 0, "nothing was traded blind");
}

#[test]
fn no_order_is_ever_refused_for_want_of_a_market() {
    // The end-to-end version of the property above: because signals cannot fire
    // while blind, the simulator's NoMarket rejection should never trigger. If
    // this ever fails, a signal escaped a gap.
    let mut steps = up_then_down();
    steps.insert(6, Step::Gap);
    steps.insert(7, Step::Gap);
    let engine = run(&steps);
    assert_eq!(engine.venue().stats().no_market, 0);
    assert_eq!(engine.stats().refused, 0);
}

#[test]
fn the_curve_has_a_point_for_every_sampling_interval() {
    let steps = up_then_down();
    let engine = run(&steps);
    assert_eq!(
        engine.strategy().curve().points.len(),
        steps.len(),
        "one event per second, sampling every second"
    );
}

#[test]
fn a_sample_with_no_mark_is_kept_as_a_hole_not_skipped() {
    // Skipping would leave a gap that a plotting tool draws a straight line
    // across, inventing a smooth passage through the period we could not see.
    let mut steps = up_then_down();
    // A gap while a position is open: cash is known, the holding is not.
    steps.insert(8, Step::Gap);
    let engine = run(&steps);
    let curve = engine.strategy().curve();
    assert_eq!(curve.points.len(), steps.len(), "the hole is still a point");
    assert!(
        curve.blind_samples() >= 1,
        "and it is marked as unvaluable: {:?}",
        curve.points.iter().map(|p| p.equity).collect::<Vec<_>>()
    );
}

#[test]
fn a_run_that_never_trades_holds_its_starting_equity() {
    let steps: Vec<Step> = (0..10).map(|_| Step::Book(100)).collect();
    let engine = run(&steps);
    let curve = engine.strategy().curve();
    assert_eq!(curve.first(), Some(CASH));
    assert_eq!(curve.last(), Some(CASH));
    assert_eq!(curve.max_drawdown(), Notional::from_raw(0));
}

#[test]
fn the_whole_wiring_is_deterministic() {
    // The engine contract's last criterion, at the level a user sees it.
    let a = run(&up_then_down());
    let b = run(&up_then_down());
    assert_eq!(a.portfolio(), b.portfolio());
    assert_eq!(a.strategy().curve().points, b.strategy().curve().points);
}

// --- The curve's own arithmetic, which is pure and worth testing directly ---

fn point(equity: Option<&str>) -> EquityPoint {
    EquityPoint {
        ts: Ts::from_nanos(0),
        equity: equity.map(|e| e.parse().expect("a valid amount")),
        cash: Notional::from_raw(0),
        mark: None,
    }
}

fn curve(equities: &[Option<&str>]) -> EquityCurve {
    EquityCurve {
        points: equities.iter().map(|e| point(*e)).collect(),
    }
}

#[test]
fn drawdown_is_the_worst_fall_from_a_peak() {
    let c = curve(&[
        Some("100"),
        Some("120"),
        Some("90"),
        Some("110"),
        Some("80"),
    ]);
    // Peak 120, trough 80 -> 40. Not 120 - 90 = 30, and not 110 - 80 = 30.
    assert_eq!(c.max_drawdown(), "40".parse().expect("amount"));
}

#[test]
fn a_curve_that_only_rises_has_no_drawdown() {
    let c = curve(&[Some("100"), Some("110"), Some("120")]);
    assert_eq!(c.max_drawdown(), Notional::from_raw(0));
}

#[test]
fn drawdown_spans_a_hole_rather_than_ignoring_it() {
    // The honest reading: we do not know what happened inside the blind period,
    // so the fall is measured from before it to after it. Pretending the peak
    // was inside the hole would understate it.
    let c = curve(&[Some("120"), None, None, Some("80")]);
    assert_eq!(c.max_drawdown(), "40".parse().expect("amount"));
    assert_eq!(c.blind_samples(), 2);
}

#[test]
fn first_and_last_skip_holes_at_the_ends() {
    let c = curve(&[None, Some("100"), Some("90"), None]);
    assert_eq!(c.first(), Some("100".parse().expect("amount")));
    assert_eq!(c.last(), Some("90".parse().expect("amount")));
}

#[test]
fn a_curve_with_no_valuation_at_all_reports_nothing_rather_than_zero() {
    let c = curve(&[None, None]);
    assert_eq!(c.first(), None);
    assert_eq!(c.last(), None);
    assert_eq!(c.range(), None);
}

// --- Costs (M4): the properties that make "degrades sensibly" falsifiable ---

/// The same wiring, with costs.
fn run_with(steps: &[Step], costs: quant_sim::Costs) -> Wired {
    let instrument = instrument();
    let strategy = Recorded::new(
        MaCrossover::new(MaConfig {
            instrument,
            fast: 2,
            slow: 3,
            interval: SECOND,
            qty: "0.001".parse().expect("qty"),
        }),
        instrument,
        SECOND,
    );
    let mut engine = Engine::new(
        Script::new(steps),
        SimulatedVenue::with_costs(costs),
        AllowAll,
        strategy,
        CASH,
    );
    engine.run().expect("the script cannot fail");
    engine
}

/// A path that trades several times, so fees have something to bite.
fn choppy() -> Vec<Step> {
    let mut steps = Vec::new();
    for _ in 0..4 {
        for mid in [100, 100, 100, 101, 103, 106, 110, 108, 104, 99, 95, 90] {
            steps.push(Step::Book(mid));
        }
    }
    steps
}

#[test]
fn zero_costs_reproduce_the_free_run_exactly() {
    // The no-op property, end to end. If a zero-valued model changes a number,
    // it touched something it had no business touching -- and every M4 result is
    // quoted against this baseline, so it has to be the same baseline.
    let free = run(&choppy());
    let zeroed = run_with(&choppy(), quant_sim::Costs::NONE);
    assert_eq!(free.portfolio(), zeroed.portfolio());
    assert_eq!(
        free.strategy().curve().points,
        zeroed.strategy().curve().points
    );
}

#[test]
fn a_higher_fee_rate_never_leaves_more_equity() {
    // Monotonicity: the property a sign error on a fee breaks, and a sign error
    // on a fee is otherwise invisible because it looks like a good strategy.
    let mut previous: Option<quant_core::Notional> = None;
    for rate in ["0", "0.0001", "0.00075", "0.001", "0.01"] {
        let engine = run_with(
            &choppy(),
            quant_sim::Costs {
                fees: quant_sim::FeeSchedule::flat(rate.parse().expect("rate")),
                ..quant_sim::Costs::NONE
            },
        );
        let equity = engine
            .strategy()
            .curve()
            .last()
            .expect("the path ends with a live book");
        if let Some(before) = previous {
            assert!(
                equity <= before,
                "fees at {rate} left {equity}, more than the cheaper run's {before}"
            );
        }
        previous = Some(equity);
    }
}

#[test]
fn fees_are_visible_separately_from_the_price_result() {
    // "Profitable before costs and not after" has to be readable off the
    // output, not inferred. Realized P&L is price only; fees are their own
    // total; cash carries both.
    let costed = run_with(
        &choppy(),
        quant_sim::Costs {
            fees: quant_sim::FeeSchedule::binance_spot(),
            ..quant_sim::Costs::NONE
        },
    );
    let free = run(&choppy());

    assert_eq!(
        costed.portfolio().realized(),
        free.portfolio().realized(),
        "fees must not be folded into the price result"
    );
    assert!(costed.portfolio().fees().raw() > 0);
    assert!(
        costed.portfolio().cash() < free.portfolio().cash(),
        "but they must come out of the money"
    );
}

#[test]
fn the_venue_and_the_portfolio_agree_on_what_was_charged() {
    // Two independent tallies of the same number: the venue says what it
    // charged and the portfolio says what it paid. A disagreement means one of
    // them is wrong, and neither would say so on its own.
    let engine = run_with(
        &choppy(),
        quant_sim::Costs {
            fees: quant_sim::FeeSchedule::binance_spot(),
            ..quant_sim::Costs::NONE
        },
    );
    assert_eq!(
        engine.venue().stats().fees_charged,
        engine.portfolio().fees()
    );
}

#[test]
fn latency_is_allowed_to_help_or_hurt_because_it_is_variance_not_a_cost() {
    // Deliberately *not* a monotonicity test, and the reason is worth keeping.
    // Latency does not subtract a fee; it moves the fill to a later book, and
    // over a horizon much longer than the latency the sign of that move is a
    // coin flip. On the acceptance week 50ms of latency slightly *improved* the
    // result, and a criterion saying it must not have would have been wrong.
    //
    // What must hold is that it changes something -- a latency model that
    // altered no fill would be a field rather than a model -- and that the run
    // stays well-defined.
    let instant = run_with(&choppy(), quant_sim::Costs::NONE);
    let slow = run_with(
        &choppy(),
        quant_sim::Costs {
            latency: quant_sim::Latency::millis(400),
            ..quant_sim::Costs::NONE
        },
    );
    assert!(
        slow.portfolio().fills() > 0,
        "orders still reach the venue eventually"
    );
    assert_ne!(
        instant.portfolio().realized(),
        slow.portfolio().realized(),
        "a latency model that changed no fill would be a field, not a model"
    );
}

#[test]
fn a_latency_longer_than_the_data_means_nothing_ever_arrives() {
    // The degenerate end, checked so it fails loudly rather than looking like a
    // strategy that chose not to trade.
    let engine = run_with(
        &choppy(),
        quant_sim::Costs {
            latency: quant_sim::Latency::millis(1_000_000),
            ..quant_sim::Costs::NONE
        },
    );
    assert_eq!(engine.portfolio().fills(), 0);
    assert!(
        engine.venue().stats().accepted == 0,
        "not even acknowledged"
    );
    assert!(engine.stats().submitted > 0, "but orders were sent");
}

// --- M6: limits provably veto a misbehaving strategy ---
//
// The criterion says "provably, under test", so the misbehaving strategies are
// part of the suite rather than something imagined. Each one below is a real
// failure a fortnight of unattended running could produce, wired to the real
// engine, the real simulator and the real risk layer.

use quant_core::execution::{OrderKind, OrderRequest, TimeInForce};
use quant_engine::{Limits, RiskEngine, TripCause};

/// Buys as much as it can, every single event. The size failure.
#[derive(Debug, Default)]
struct Greedy {
    instrument: Option<InstrumentId>,
    refused: u64,
    accepted: u64,
}

impl quant_engine::Strategy for Greedy {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut quant_engine::Context<'_>) {
        self.instrument = Some(event.meta().instrument);
        ctx.submit(OrderRequest {
            instrument: event.meta().instrument,
            side: quant_core::event::Side::Buy,
            // Absurd on purpose: 1000 units at a mid near 100 is 100,000.
            qty: "1000".parse().expect("qty"),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::Gtc,
        });
    }

    fn on_execution(
        &mut self,
        event: &quant_core::execution::ExecutionEvent,
        _ctx: &mut quant_engine::Context<'_>,
    ) {
        match event {
            quant_core::execution::ExecutionEvent::Rejected { .. } => self.refused += 1,
            quant_core::execution::ExecutionEvent::Filled { .. } => self.accepted += 1,
            _ => {}
        }
    }
}

/// Submits on every event forever. The loop failure.
#[derive(Debug, Default)]
struct Runaway {
    submitted: u64,
}

impl quant_engine::Strategy for Runaway {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut quant_engine::Context<'_>) {
        self.submitted += 1;
        ctx.submit(OrderRequest {
            instrument: event.meta().instrument,
            side: quant_core::event::Side::Buy,
            qty: "0.001".parse().expect("qty"),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::Gtc,
        });
    }
}

fn run_risked<S: quant_engine::Strategy>(
    steps: &[Step],
    strategy: S,
    limits: Limits,
) -> quant_engine::Engine<Script, SimulatedVenue, RiskEngine, S> {
    let mut engine = quant_engine::Engine::new(
        Script::new(steps),
        SimulatedVenue::new(),
        RiskEngine::new(limits),
        strategy,
        CASH,
    );
    engine.run().expect("the script cannot fail");
    engine
}

#[test]
fn an_oversized_order_never_reaches_the_venue() {
    // Not "the venue declines it" -- the venue is never told, which is what
    // makes risk a chokepoint rather than a module the strategy calls.
    let engine = run_risked(
        &choppy(),
        Greedy::default(),
        Limits {
            max_order_notional: Some("500".parse().expect("amount")),
            ..Limits::default()
        },
    );
    assert_eq!(
        engine.venue().stats().submitted,
        0,
        "the venue heard nothing"
    );
    assert_eq!(engine.portfolio().fills(), 0);
    assert!(engine.strategy().refused > 0, "and the strategy was told");
    assert_eq!(engine.strategy().accepted, 0);
    assert_eq!(engine.stats().submitted, 0);
    assert!(engine.stats().refused > 0);
}

#[test]
fn a_runaway_strategy_trips_the_switch_and_stops() {
    // A loop does not stop because one order was declined, so the order-count
    // limit latches. The number of orders that got out is bounded by the limit
    // and not by the length of the run, which is the property that matters for
    // an unattended fortnight.
    let steps = choppy();
    let engine = run_risked(
        &steps,
        Runaway::default(),
        Limits {
            max_orders_per_day: Some(3),
            ..Limits::default()
        },
    );
    assert!(
        engine.strategy().submitted >= steps.len() as u64,
        "the strategy kept trying"
    );
    assert_eq!(
        engine.stats().submitted,
        3,
        "exactly the limit got through, however long the run"
    );
    assert_eq!(engine.risk().tripped(), Some(TripCause::OrderCount));
}

#[test]
fn a_strategy_that_loses_its_budget_is_stopped_for_the_day() {
    // The crossover with a loss limit tight enough to bite. It has to stop
    // trading, and the switch has to stay thrown.
    let instrument = instrument();
    let strategy = Recorded::new(
        MaCrossover::new(MaConfig {
            instrument,
            fast: 2,
            slow: 3,
            interval: SECOND,
            qty: "0.001".parse().expect("qty"),
        }),
        instrument,
        SECOND,
    );
    let mut engine = quant_engine::Engine::new(
        Script::new(&choppy()),
        SimulatedVenue::with_costs(quant_sim::Costs {
            fees: quant_sim::FeeSchedule::flat("0.05".parse().expect("rate")),
            ..quant_sim::Costs::NONE
        }),
        RiskEngine::new(Limits {
            max_daily_loss: Some("0.005".parse().expect("amount")),
            ..Limits::default()
        }),
        strategy,
        CASH,
    );
    engine.run().expect("the script cannot fail");

    assert_eq!(
        engine.risk().tripped(),
        Some(TripCause::DailyLoss),
        "a 5% fee on every fill spends a half-cent budget quickly"
    );
    assert!(
        engine.stats().refused > 0,
        "and orders were refused afterwards"
    );
}

#[test]
fn a_refusal_reaches_the_strategy_as_an_execution_event() {
    // A strategy has to be able to tell that it was stopped. A refusal that
    // vanished would leave it believing it had an order working, and its next
    // decision would be made on a position it does not have.
    let engine = run_risked(
        &choppy(),
        Greedy::default(),
        Limits {
            max_order_notional: Some("1".parse().expect("amount")),
            ..Limits::default()
        },
    );
    assert!(engine.strategy().refused > 0);
}

#[test]
fn the_same_strategy_is_untouched_when_the_limits_permit_it() {
    // The other half of the criterion: limits that do not bind must not change
    // behaviour. A risk layer that quietly altered a permitted run would make
    // every backtest a different system from the one that trades.
    let permissive = run_risked(&choppy(), Runaway::default(), Limits::default());
    let mut unrisked = quant_engine::Engine::new(
        Script::new(&choppy()),
        SimulatedVenue::new(),
        AllowAll,
        Runaway::default(),
        CASH,
    );
    unrisked.run().expect("the script cannot fail");

    assert_eq!(permissive.portfolio().fills(), unrisked.portfolio().fills());
    assert_eq!(permissive.portfolio().cash(), unrisked.portfolio().cash());
    assert_eq!(permissive.stats().submitted, unrisked.stats().submitted);
}

#[test]
fn no_money_limit_can_be_enforced_across_a_gap_so_it_refuses() {
    // A gap clears the book, so there is no mark. A money limit has to refuse
    // rather than guess -- tightest when the market is least understood.
    let steps = vec![Step::Gap, Step::Gap, Step::Gap];
    let engine = run_risked(
        &steps,
        Greedy::default(),
        Limits {
            max_order_notional: Some("1000000".parse().expect("amount")),
            ..Limits::default()
        },
    );
    assert_eq!(engine.venue().stats().submitted, 0);
    assert!(engine.strategy().refused > 0, "refused for want of a price");
}
