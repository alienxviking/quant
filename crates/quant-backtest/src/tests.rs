//! Tests for the strategy and the equity recorder.
//!
//! The integration tests wire the *real* engine, the *real* simulated venue and
//! the real strategy against a scripted price path, because the properties worth
//! checking here — that a signal turns into a position, that a gap stops trading,
//! that a hole in the curve is visible — are properties of the whole wiring and
//! not of any one piece.

use quant_core::event::{BookSnapshot, EventMeta, Gap, GapCause, Level, MarketEvent};
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::instrument::{
    Exchange, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
};
use quant_core::source::{EventSource, SourceError, Wake};
use quant_core::time::Ts;
use quant_engine::{AllowAll, Engine};
use quant_sim::SimulatedVenue;

use crate::equity::{EquityCurve, EquityPoint};
use crate::ma::{MaConfig, MaConfigError};
use crate::{MaCrossover, Recorded};

const SECOND: i64 = 1_000_000_000;
const CASH: Notional = Notional::from_raw(100 * quant_core::SCALE);

fn definition() -> InstrumentDef {
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

fn instrument() -> InstrumentId {
    InstrumentRegistry::new().register(definition())
}

/// The lot the crossover sizes to, read off the definition the instrument was
/// registered from -- as the binaries do -- rather than typed in again.
fn lot() -> Qty {
    definition().lot_size
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
            lot: lot(),
        })
        .expect("a valid configuration"),
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
    wire(Script::new(steps), costs)
}

/// The same wiring around any source, run to its end.
fn wire<S: EventSource>(
    source: S,
    costs: quant_sim::Costs,
) -> Engine<S, SimulatedVenue, AllowAll, Recorded<MaCrossover>> {
    let instrument = instrument();
    let strategy = Recorded::new(
        MaCrossover::new(MaConfig {
            instrument,
            fast: 2,
            slow: 3,
            interval: SECOND,
            qty: "0.001".parse().expect("qty"),
            lot: lot(),
        })
        .expect("a valid configuration"),
        instrument,
        SECOND,
    );
    let mut engine = Engine::new(
        source,
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

/// The script, with the market going quiet before every event and for a while
/// after the last: a live socket, as `LiveSource` reports one since M8.
struct Quiet {
    script: Script,
    /// Flipped on every call, so idle wakes and events alternate, starting
    /// with an idle wake -- a socket that is up before its first frame.
    idle_next: bool,
    /// How many more times to find the script empty and idle rather than end.
    ///
    /// Not the number of idle wakes after the last event: the flip carries on
    /// regardless, so each of these is preceded by an idle wake of its own and
    /// one more comes before the end -- `2 * trailing + 1` in all.
    trailing: usize,
}

impl EventSource for Quiet {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        self.script.next_event()
    }

    fn next_event_timeout(
        &mut self,
        _timeout: std::time::Duration,
    ) -> Option<Result<Wake, SourceError>> {
        self.idle_next = !self.idle_next;
        if self.idle_next {
            return Some(Ok(Wake::Idle));
        }
        match self.script.next_event() {
            Some(event) => Some(event.map(Wake::Event)),
            None if self.trailing > 0 => {
                self.trailing -= 1;
                Some(Ok(Wake::Idle))
            }
            None => None,
        }
    }
}

#[test]
fn idle_wakes_change_nothing_against_a_simulated_venue() {
    // Paper is `LiveSource + SimulatedVenue`, and since M8 `LiveSource` idles
    // whenever the market goes quiet. That must not move a number. The
    // simulator makes its reports only inside `observe` and the engine polls it
    // straight afterwards, so an idle wake polls again at a clock nothing has
    // moved and finds nothing due -- not even a fill still in flight on inbound
    // latency, which a backtest leaves undelivered at the end. Were that to stop
    // holding, M5's criterion -- paper reproduces the backtest to the satoshi --
    // would be measuring idle wakes rather than live data.
    //
    // Reports travel back for three seconds -- three events. Short enough that
    // fills are booked along the way, long enough that the last order's reports
    // are still in flight when the script ends; the first two assertions say so
    // if a change to the path or the strategy ever makes either untrue.
    let costs = quant_sim::Costs {
        latency: quant_sim::Latency {
            outbound: 0,
            inbound: 3 * SECOND,
        },
        ..quant_sim::Costs::NONE
    };
    // Both runs start a second in. The script's first event sits at the epoch,
    // which is exactly where a run that dated itself from an idle wake would
    // begin -- so from there the two periods would agree even if that broke.
    let script = || {
        let mut script = Script::new(&choppy());
        script.events.next();
        script
    };
    let busy = wire(script(), costs);
    let quiet = wire(
        Quiet {
            script: script(),
            idle_next: false,
            trailing: 5,
        },
        costs,
    );

    assert!(
        busy.stats().fills > 0,
        "fills have to be booked along the way, or the idle wakes between \
         events test nothing"
    );
    assert!(
        busy.venue().stats().undelivered > 0,
        "a report has to be in flight at the end, or the quiet tail tests nothing"
    );
    assert!(
        quiet.stats().idle_wakes > 0,
        "and the quiet run has to idle"
    );
    assert_eq!(quiet.portfolio(), busy.portfolio());
    assert_eq!(
        quiet.strategy().curve().points,
        busy.strategy().curve().points
    );
    assert_eq!(quiet.venue().stats(), busy.venue().stats());
    assert_eq!(
        quant_engine::EngineStats {
            idle_wakes: 0,
            ..quiet.stats()
        },
        busy.stats(),
        "every count but the idle wakes themselves"
    );
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
            lot: lot(),
        })
        .expect("a valid configuration"),
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

// --- The backtest binary's risk wiring (M5's criterion) ---
//
// `backtest` used to wire `AllowAll` while `paper` wired a real `RiskEngine`.
// M5's criterion compares the two over the same window and demands they match
// exactly, so a difference in the layer every order passes through is a
// difference between the two systems being compared. These pin both halves of
// the swap: that it changes nothing when no limit is set, and that a limit set
// here binds the same way it does in paper.

/// The backtest binary's wiring exactly: the real crossover behind a `RiskEngine`.
fn run_crossover_risked(
    steps: &[Step],
    limits: Limits,
) -> quant_engine::Engine<Script, SimulatedVenue, RiskEngine, Recorded<MaCrossover>> {
    let instrument = instrument();
    let strategy = Recorded::new(
        MaCrossover::new(MaConfig {
            instrument,
            fast: 2,
            slow: 3,
            interval: SECOND,
            qty: "0.001".parse().expect("qty"),
            lot: lot(),
        })
        .expect("a valid configuration"),
        instrument,
        SECOND,
    );
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
fn unset_limits_reproduce_the_unrisked_backtest_exactly() {
    // The no-op property, and the reason the swap is safe to make at all. It is
    // the same shape as `zero_costs_reproduce_the_free_run_exactly`: the default
    // run has to be byte-identical to the one before the feature existed, or
    // every earlier result silently describes a different system.
    //
    // `the_same_strategy_is_untouched_when_the_limits_permit_it` proves this for
    // `Runaway`. This proves it for the strategy the binary actually ships, which
    // is the one whose numbers M5 compares.
    let risked = run_crossover_risked(&choppy(), Limits::default());
    let unrisked = run_with(&choppy(), quant_sim::Costs::NONE);

    assert_eq!(risked.portfolio().cash(), unrisked.portfolio().cash());
    assert_eq!(risked.portfolio().fills(), unrisked.portfolio().fills());
    assert_eq!(
        risked.portfolio().realized(),
        unrisked.portfolio().realized()
    );
    assert_eq!(risked.stats().submitted, unrisked.stats().submitted);
    assert_eq!(risked.stats().refused, 0);
    assert_eq!(
        risked.strategy().curve().last(),
        unrisked.strategy().curve().last()
    );
}

#[test]
fn a_binding_order_limit_stops_the_crossover_before_the_venue() {
    // And the other half: a limit set on a backtest has to actually bind, or the
    // flags would be decoration and a paper run's refusals could never be
    // reproduced here. Not "the venue declined it" -- the venue is never told.
    // 0.05, because `choppy()` runs between 90 and 110 and the crossover trades
    // 0.001 -- so an order is 0.09 to 0.11 of notional and this binds on every
    // one of them. The first draft of this test used 1, copied from the `Greedy`
    // test above where the order is 1000 units, and it did not bind at all: the
    // venue saw 14 submissions and the test failed. A limit's test value is a
    // fact about the price path it runs on, not a number to copy.
    let engine = run_crossover_risked(
        &choppy(),
        Limits {
            max_order_notional: Some("0.05".parse().expect("amount")),
            ..Limits::default()
        },
    );

    assert_eq!(engine.venue().stats().submitted, 0, "never reached a venue");
    assert_eq!(engine.stats().submitted, 0);
    assert!(engine.stats().refused > 0, "the limit has to bind");
    assert_eq!(engine.portfolio().fills(), 0);
    assert_eq!(engine.portfolio().cash(), CASH, "nothing was ever spent");

    // The control, and it is what makes the assertions above mean anything. With
    // the layer neutered to refuse everything, every assertion so far still holds
    // -- so on its own this test cannot tell "the limit bound" from "nothing gets
    // through at all". Verified by doing exactly that: it stayed green. A loosened
    // limit on the *same* path has to trade, which is the difference.
    let loose = run_crossover_risked(
        &choppy(),
        Limits {
            max_order_notional: Some("1".parse().expect("amount")),
            ..Limits::default()
        },
    );
    assert!(loose.stats().submitted > 0, "0.11 is under a limit of 1");
    assert_eq!(loose.stats().refused, 0);
    assert!(loose.portfolio().fills() > 0);
}

/// Rising from the very first sample, then falling.
///
/// The fixture that produces a crossing with nothing to do, and getting it right
/// took a failed attempt worth recording. `up_then_down` opens flat at 100, so
/// the first side the indicator takes is *below* and its first crossing is
/// upward — an entry. A no-op needs the opposite: the side established as
/// **above** at the first sample where both averages exist (which is never
/// itself a crossing, since there is no previous side to differ from), and then
/// a fall. That crossing is downward while flat, which is a real signal and
/// legitimately nothing to do.
///
/// That `up_then_down` cannot reach the arm is exactly why the arm was missing
/// for five milestones: every fixture in this file rose before it fell.
fn rise_then_fall() -> Vec<Step> {
    let mut steps = Vec::new();
    for mid in [100, 102, 105, 109, 114] {
        steps.push(Step::Book(mid));
    }
    for mid in [112, 107, 101, 95, 89] {
        steps.push(Step::Book(mid));
    }
    steps
}

#[test]
fn every_crossing_is_an_entry_an_exit_a_suppression_or_nothing_to_do() {
    // `crossings` was documented as the number of signals and silently was not
    // the number of *decisions*: the `if`/`else if` had no `else`, so a crossing
    // up while already long, or down while already flat, went nowhere and was
    // counted by nothing. M5's ETHUSDT leg signalled 828, submitted 822 and
    // suppressed 4 -- and the two that made up the difference had never been
    // seen by any counter in the system.
    //
    // This assertion is what the missing arm makes possible, and it is worth
    // more than the counter it increments.
    for steps in [up_then_down(), rise_then_fall()] {
        let engine = run(&steps);
        let ma = engine.strategy().inner().stats();
        assert!(ma.crossings > 0, "the fixture has to cross: {ma:?}");
        assert_eq!(
            ma.entries + ma.exits + ma.suppressed + ma.no_ops,
            ma.crossings,
            "four outcomes, and they are exhaustive: {ma:?}"
        );
    }
}

#[test]
fn a_crossing_with_nothing_to_do_is_recorded_as_such() {
    // Crossing down while flat is a real signal and a legitimate no-op. Before
    // this slice it produced no order, no suppression and no counter -- the same
    // silence in the file as an hour in which nothing happened at all.
    let engine = run(&rise_then_fall());
    let ma = engine.strategy().inner().stats();
    assert!(
        ma.no_ops >= 1,
        "a down-cross while flat has nothing to do, and says so: {ma:?}"
    );
}

#[test]
fn a_wrapped_strategy_still_gets_its_notes_out() {
    // `Recorded<S>` is a decorator, and a decorator that forgets to forward a
    // trait method inherits the default -- which for `take_notes` is silence.
    // Every note `MaCrossover` wrote would have been swallowed here, in both
    // binaries, with nothing raised anywhere and a journal that merely looked
    // like a quiet strategy.
    //
    // Found by writing this test rather than before it. The same shape as M6's
    // `FillObserver`, which was never called at all, and whose durability
    // therefore did not exist.
    #[derive(Debug, Default)]
    struct Talks;
    impl quant_engine::Strategy for Talks {
        fn on_market_event(&mut self, _e: &MarketEvent, _c: &mut quant_engine::Context<'_>) {}
        fn take_notes(&mut self, out: &mut Vec<quant_engine::Note>) {
            out.push(quant_engine::Note::new("hello", serde_json::Value::Null));
        }
    }

    let mut wrapped = Recorded::new(Talks, instrument(), SECOND);
    let mut notes = Vec::new();
    quant_engine::Strategy::take_notes(&mut wrapped, &mut notes);
    assert_eq!(
        notes.iter().map(|n| n.kind).collect::<Vec<_>>(),
        vec!["hello"],
        "the wrapper has to pass the diary through"
    );
}

// --- M8.c: the size, in whole lots ---

/// A configuration that differs from the suite's only in size and lot.
fn sized(qty: &str, lot: &str) -> MaConfig {
    MaConfig {
        instrument: instrument(),
        fast: 2,
        slow: 3,
        interval: SECOND,
        qty: qty.parse().expect("qty"),
        lot: lot.parse().expect("lot"),
    }
}

#[test]
fn a_size_that_is_not_whole_lots_is_refused_at_construction() {
    // An order the venue would reject anyway, so it is refused once, here --
    // not on every order, and not by a live venue on the first funded trade.
    let refused = |qty: &str, lot: &str| MaCrossover::new(sized(qty, lot)).err();

    // 1.23 lots of 0.00001.
    assert_eq!(
        refused("0.0000123", "0.00001"),
        Some(MaConfigError::Size {
            qty: "0.0000123".parse().expect("qty"),
            lot: "0.00001".parse().expect("lot"),
        })
    );
    // Zero and negative are whole numbers of lots, and not sizes.
    assert!(matches!(
        refused("0", "0.00001"),
        Some(MaConfigError::Size { .. })
    ));
    assert!(matches!(
        refused("-0.001", "0.00001"),
        Some(MaConfigError::Size { .. })
    ));
    // A lot of zero would make every size "whole", and is no lot at all.
    assert_eq!(refused("0.001", "0"), Some(MaConfigError::Lot(Qty::ZERO)));

    // The boundary from the other side: whole lots are fine, down to one.
    assert_eq!(refused("0.001", "0.00001"), None);
    assert_eq!(refused("0.00001", "0.00001"), None);
}

#[test]
fn windows_that_cannot_cross_are_refused_rather_than_panicking() {
    // This was an `assert!` until the size check made the constructor
    // fallible; one constructor with two ways of saying no would have left
    // every caller handling both.
    let windows = |fast: usize, slow: usize| {
        MaCrossover::new(MaConfig {
            fast,
            slow,
            ..sized("0.001", "0.00001")
        })
        .err()
    };
    assert_eq!(
        windows(3, 3),
        Some(MaConfigError::Windows { fast: 3, slow: 3 })
    );
    assert_eq!(
        windows(0, 3),
        Some(MaConfigError::Windows { fast: 0, slow: 3 })
    );
    assert_eq!(windows(2, 3), None);
}

// --- M8.c: an exit sells what a base-asset fee left behind ---

use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, ExecutionEvent, FeeAsset};
use quant_engine::ExecutionVenue;

/// The real simulator, with each buy's fee taken out of what was bought as a
/// fixed quantity of the base.
///
/// Written when `SimulatedVenue` charged every fee in the quote. A buy filling
/// in full then left exactly what was ordered, so an exit that sold `qty` could
/// not be told from one that sold what was held, and rewriting the fill was the
/// only way to take a buy's fee out of what was bought. The simulator can now
/// do that itself (`FeeIn::Received`), as a rate on what was bought. These
/// tests keep stating the fee as the quantity of the base they then count in,
/// which is how they land the holding on the boundaries they are about -- a
/// whole lot, a lot and change, dust under one. The matching, the prices and
/// the sells are still the simulator's. The rate itself, through the
/// simulator's own option, is
/// `a_crossover_under_fees_in_the_asset_received_exits_the_floored_position`.
#[derive(Debug)]
struct BaseFeeOnBuys {
    inner: SimulatedVenue,
    fee: Qty,
    buys: std::collections::HashSet<ClientOrderId>,
    /// Every request the venue was told about, in order.
    sent: Vec<OrderRequest>,
}

impl ExecutionVenue for BaseFeeOnBuys {
    fn submit(&mut self, client_order_id: ClientOrderId, request: &OrderRequest, now: Ts) {
        if request.side == Side::Buy {
            self.buys.insert(client_order_id);
        }
        self.sent.push(*request);
        self.inner.submit(client_order_id, request, now);
    }

    fn cancel(&mut self, client_order_id: ClientOrderId, now: Ts) {
        self.inner.cancel(client_order_id, now);
    }

    // Forwarded explicitly: `observe` has a default that does nothing, and a
    // wrapper that inherited it would hide the market from the simulator.
    fn observe(&mut self, event: &MarketEvent, book: &quant_book::Book, now: Ts) {
        self.inner.observe(event, book, now);
    }

    fn poll(&mut self, now: Ts, out: &mut Vec<ExecutionEvent>) {
        let from = out.len();
        self.inner.poll(now, out);
        for event in &mut out[from..] {
            if let ExecutionEvent::Filled {
                client_order_id,
                fill,
                ..
            } = event
            {
                if self.buys.contains(client_order_id) {
                    // With a `Base` asset, `fee` holds a quantity of the base.
                    fill.fee = Notional::from_raw(self.fee.raw());
                    fill.fee_asset = FeeAsset::Base;
                }
            }
        }
    }
}

fn qty(s: &str) -> Qty {
    s.parse().expect("a valid quantity")
}

/// The suite's crossover entering at `size`, against a venue that takes `fee`
/// of the base out of every buy fill.
///
/// The usual pair is 0.001 and 0.000001: ten basis points, Binance's taker
/// rate, so the suite's usual size pays what it would on the venue. A test
/// that needs the holding to land somewhere exact chooses its own.
fn run_base_fee(
    steps: &[Step],
    size: &str,
    fee: &str,
) -> quant_engine::Engine<Script, BaseFeeOnBuys, AllowAll, Recorded<MaCrossover>> {
    let instrument = instrument();
    let rules = definition();
    let strategy = Recorded::new(
        MaCrossover::new(MaConfig {
            instrument,
            fast: 2,
            slow: 3,
            interval: SECOND,
            qty: qty(size),
            lot: rules.lot_size,
        })
        .expect("a valid configuration"),
        instrument,
        SECOND,
    );
    let venue = BaseFeeOnBuys {
        // The venue enforces the lot, so an exit it would not take is refused
        // rather than filled. Not the minimum notional: the script trades near
        // 100, where 0.001 is worth a dime and the real $5 floor refuses all.
        inner: SimulatedVenue::new().enforcing(
            instrument,
            quant_core::instrument::Filters {
                tick_size: rules.tick_size,
                lot_size: rules.lot_size,
                min_notional: Notional::ZERO,
            },
        ),
        // Per buy fill, in BTC.
        fee: qty(fee),
        buys: std::collections::HashSet::new(),
        sent: Vec::new(),
    };
    let mut engine = Engine::new(Script::new(steps), venue, AllowAll, strategy, CASH);
    engine.run().expect("the script cannot fail");
    engine
}

#[test]
fn an_exit_after_a_base_asset_fee_sells_what_is_held_in_whole_lots() {
    // Buy 0.001 and pay 0.000001 of it in fee: 0.000999 is held, which is 99.9
    // lots. The exit sells the 99 whole ones -- not `qty`, which would sell
    // coins that were never bought, and never more than is held.
    let engine = run_base_fee(&up_then_down(), "0.001", "0.000001");
    let ma = engine.strategy().inner().stats();
    assert_eq!((ma.entries, ma.exits), (1, 1), "one round trip: {ma:?}");

    let sent = &engine.venue().sent;
    assert_eq!(sent.len(), 2, "an entry and an exit: {sent:?}");
    assert_eq!((sent[0].side, sent[0].qty), (Side::Buy, qty("0.001")));
    assert_eq!(
        (sent[1].side, sent[1].qty),
        (Side::Sell, qty("0.00099")),
        "the 0.000999 held, rounded down to the lot of 0.00001"
    );
    assert_eq!(
        engine.venue().inner.stats().filtered(),
        0,
        "and a venue enforcing the lot took both"
    );
    assert_eq!(engine.portfolio().fills(), 2);
    assert_eq!(
        engine.portfolio().position(instrument()).qty,
        qty("0.000009"),
        "what is left is under a lot and not below zero: nothing unheld was sold"
    );
}

#[test]
fn dust_below_one_lot_counts_as_flat_so_the_next_up_cross_enters_again() {
    // The exit above leaves 0.000009: real, and unsellable. Counted as a
    // holding, it would keep the strategy "long" for good -- every up-cross a
    // no-op, so it never trades again.
    let mut steps = up_then_down();
    // From the bottom at 85, rise far enough to cross back up.
    for mid in [88, 95, 104] {
        steps.push(Step::Book(mid));
    }
    let engine = run_base_fee(&steps, "0.001", "0.000001");
    let ma = engine.strategy().inner().stats();
    assert_eq!(
        (ma.entries, ma.exits, ma.no_ops),
        (2, 1, 0),
        "the second up-cross is an entry, not 'already long': {ma:?}"
    );

    let sent = &engine.venue().sent;
    assert_eq!(sent.len(), 3, "{sent:?}");
    assert_eq!(
        (sent[2].side, sent[2].qty),
        (Side::Buy, qty("0.001")),
        "a full entry, dust or no dust"
    );
    assert_eq!(
        engine.portfolio().position(instrument()).qty,
        qty("0.001008"),
        "the dust, plus the 0.000999 the second buy delivered"
    );
}

#[test]
fn one_lot_and_change_held_sells_the_lot_and_keeps_the_change() {
    // Just above the boundary. Enter at two lots and pay 0.000001 of it:
    // 0.000019 is held, one whole lot and nine tenths of another. The exit
    // sells the whole lot and keeps the change, which is dust. A flat
    // boundary moved up by a lot fails here, as it does at exactly one lot.
    let engine = run_base_fee(&up_then_down(), "0.00002", "0.000001");
    let ma = engine.strategy().inner().stats();
    assert_eq!((ma.entries, ma.exits), (1, 1), "{ma:?}");
    let sent = &engine.venue().sent;
    assert_eq!(
        (sent[1].side, sent[1].qty),
        (Side::Sell, qty("0.00001")),
        "exactly one lot of the 0.000019 held"
    );
    assert_eq!(
        engine.portfolio().position(instrument()).qty,
        qty("0.000009")
    );
}

#[test]
fn exactly_one_whole_lot_held_is_a_holding_and_is_sold() {
    // The flat boundary itself. Enter at two lots and pay a whole lot of it
    // in fee: 0.00001 is held, one lot and not a satoshi over. As far as the
    // lot rule goes that is a holding, so the down-cross sells all of it and
    // leaves nothing behind.
    //
    // The test that first had this name held 0.000019, over a lot, so a
    // boundary drawn on the unrounded position -- long only while it is
    // strictly more than a lot -- passed every test in the file.
    let engine = run_base_fee(&up_then_down(), "0.00002", "0.00001");
    let ma = engine.strategy().inner().stats();
    assert_eq!(
        (ma.entries, ma.exits, ma.no_ops),
        (1, 1, 0),
        "the down-cross is an exit, not 'already flat': {ma:?}"
    );

    let sent = &engine.venue().sent;
    assert_eq!(sent.len(), 2, "an entry and an exit: {sent:?}");
    assert_eq!((sent[0].side, sent[0].qty), (Side::Buy, qty("0.00002")));
    assert_eq!(
        (sent[1].side, sent[1].qty),
        (Side::Sell, qty("0.00001")),
        "the one lot held, all of it"
    );
    assert_eq!(
        engine.venue().inner.stats().filtered(),
        0,
        "a venue enforcing the lot took both"
    );
    assert!(
        engine.portfolio().position(instrument()).qty.is_zero(),
        "and nothing is left: a whole lot is not dust"
    );
}

// --- M8.c: the simulator's own fee in the asset received ---

/// The real simulator, remembering every request it was told about and
/// otherwise left alone.
#[derive(Debug)]
struct Told {
    inner: SimulatedVenue,
    sent: Vec<OrderRequest>,
}

impl ExecutionVenue for Told {
    fn submit(&mut self, client_order_id: ClientOrderId, request: &OrderRequest, now: Ts) {
        self.sent.push(*request);
        self.inner.submit(client_order_id, request, now);
    }

    fn cancel(&mut self, client_order_id: ClientOrderId, now: Ts) {
        self.inner.cancel(client_order_id, now);
    }

    // Forwarded explicitly, as `BaseFeeOnBuys` does and for its reason: the
    // default does nothing, and would hide the market from the simulator.
    fn observe(&mut self, event: &MarketEvent, book: &quant_book::Book, now: Ts) {
        self.inner.observe(event, book, now);
    }

    fn poll(&mut self, now: Ts, out: &mut Vec<ExecutionEvent>) {
        self.inner.poll(now, out);
    }
}

/// Binance spot's ten basis points, taken as Binance takes them: a buy's in
/// the base, a sell's in the quote.
fn spot_fees() -> quant_sim::Costs {
    quant_sim::Costs {
        fees: quant_sim::FeeSchedule::binance_spot(),
        fee_in: quant_sim::FeeIn::Received,
        ..quant_sim::Costs::NONE
    }
}

/// The suite's crossover at 0.001, against the simulator charging
/// [`spot_fees`] and enforcing the lot, run to the end of `steps`.
fn run_told(steps: &[Step]) -> Engine<Script, Told, AllowAll, Recorded<MaCrossover>> {
    let instrument = instrument();
    let rules = definition();
    let strategy = Recorded::new(
        MaCrossover::new(MaConfig {
            instrument,
            fast: 2,
            slow: 3,
            interval: SECOND,
            qty: qty("0.001"),
            lot: rules.lot_size,
        })
        .expect("a valid configuration"),
        instrument,
        SECOND,
    );
    let venue = Told {
        // Enforcing the lot, so an exit the venue would not take is refused
        // rather than filled. Not the minimum notional, for the reason
        // `run_base_fee` gives: the script trades near 100.
        inner: SimulatedVenue::with_costs(spot_fees()).enforcing(
            instrument,
            quant_core::instrument::Filters {
                tick_size: rules.tick_size,
                lot_size: rules.lot_size,
                min_notional: Notional::ZERO,
            },
        ),
        sent: Vec::new(),
    };
    let mut engine = Engine::new(Script::new(steps), venue, AllowAll, strategy, CASH);
    engine.run().expect("the script cannot fail");
    engine
}

#[test]
fn a_crossover_under_fees_in_the_asset_received_exits_the_floored_position() {
    // What slice (e)'s model replay needs, through the simulator's own option
    // rather than a wrapper rewriting its fills. Ten basis points of a 0.001
    // buy is 0.000001 of the base, so 0.000999 arrives. The down-cross sells
    // the 99 whole lots of it, 0.00099 -- what live sends, where a model
    // charging the quote would have sold 0.001 -- and leaves 0.000009 of dust,
    // which the next up-cross treats as flat and enters over.
    let mut steps = up_then_down();
    // From the bottom at 85, rise far enough to cross back up.
    for mid in [88, 95, 104] {
        steps.push(Step::Book(mid));
    }
    let instrument = instrument();
    let engine = run_told(&steps);

    let ma = engine.strategy().inner().stats();
    assert_eq!(
        (ma.entries, ma.exits, ma.no_ops),
        (2, 1, 0),
        "the second up-cross is an entry, not 'already long': {ma:?}"
    );
    let sent = &engine.venue().sent;
    assert_eq!(sent.len(), 3, "{sent:?}");
    assert_eq!((sent[0].side, sent[0].qty), (Side::Buy, qty("0.001")));
    assert_eq!(
        (sent[1].side, sent[1].qty),
        (Side::Sell, qty("0.00099")),
        "the 0.000999 the fee left, floored to the lot of 0.00001"
    );
    assert_eq!(
        (sent[2].side, sent[2].qty),
        (Side::Buy, qty("0.001")),
        "a full entry over the dust"
    );
    assert_eq!(
        engine.venue().inner.stats().filtered(),
        0,
        "and a venue enforcing the lot took every one"
    );
    assert_eq!(engine.portfolio().fills(), 3);
    assert_eq!(
        engine.portfolio().position(instrument).qty,
        qty("0.001008"),
        "the 0.000009 of dust, plus the 0.000999 the second buy delivered"
    );
}

#[test]
fn under_fees_in_the_asset_received_the_venue_and_the_portfolio_agree_in_quote_value() {
    // M4's two independent tallies, with a fee that is no longer money. A
    // buy's fee is a quantity of the base now, and the venue and the portfolio
    // each total it as the money it was worth at its own fill, through the one
    // conversion both use -- so they still count one quantity, and a total
    // that added the raw quantity would fall short by a factor of the price.
    let engine = run_with(&choppy(), spot_fees());
    let charged = engine.venue().stats().fees_charged;
    assert!(charged.is_positive(), "fees were charged: {charged}");
    assert_eq!(charged, engine.portfolio().fees());

    // And not vacuously. Were the buys charged in the quote, this would be
    // M4's comparison over again. Every exit sells whole lots of a holding a
    // base fee has shaved, so the run ends holding dust: real, under a lot.
    let held = engine.portfolio().position(instrument()).qty;
    assert!(
        held.is_positive() && held < lot(),
        "the dust of base-asset fees is left: {held}"
    );
}

#[test]
fn under_fees_in_the_asset_received_the_dust_an_exit_leaves_tops_up_the_next() {
    // Why a model charging the quote gets some exits wrong and not all of
    // them. The first exit sells 0.00099 of the 0.000999 a base fee left and
    // keeps 0.000009. The next buy delivers 0.000999 on top, 0.001008 in all,
    // so that exit sells a whole 0.001 and keeps 0.000008 -- and so on, each
    // trip spending 0.000001 of the dust, until none is left and an exit comes
    // up a lot short again. At this size and rate that is one exit in ten,
    // which is what replaying M5's fortnight showed: 42 of its 412.
    let engine = run_told(&choppy());
    let exits: Vec<Qty> = engine
        .venue()
        .sent
        .iter()
        .filter(|r| r.side == Side::Sell)
        .map(|r| r.qty)
        .collect();
    let whole = qty("0.001");
    assert_eq!(
        exits,
        [qty("0.00099"), whole, whole, whole, whole, whole, whole],
        "short once, then topped up by the dust each time"
    );
    assert_eq!(
        engine.portfolio().position(instrument()).qty,
        qty("0.000003"),
        "the 0.000009 the first exit left, less 0.000001 for each of six"
    );
    assert_eq!(engine.venue().inner.stats().filtered(), 0);
}

#[test]
fn under_fees_in_the_asset_received_a_higher_rate_can_leave_more_held_and_more_equity() {
    // Why `a_higher_fee_rate_never_leaves_more_equity` charges in the quote,
    // and has to. Taken in the base, a fee changes how much is held, and the
    // exit sells whole lots of that. One basis point of a 0.001 buy leaves
    // 0.0009999; the down-cross sells 0.00099 and 0.0000099 -- nearly a lot
    // -- is still held when the price doubles. With no fee the exit sold all
    // 0.001 and nothing rides the jump. So the costlier run ends richer: not a
    // sign error, but exposure, which can go either way as latency's can.
    let mut steps = up_then_down();
    steps.push(Step::Book(200));
    let ending = |rate: &str, fee_in| {
        let engine = run_with(
            &steps,
            quant_sim::Costs {
                fees: quant_sim::FeeSchedule::flat(rate.parse().expect("rate")),
                fee_in,
                ..quant_sim::Costs::NONE
            },
        );
        let held = engine.portfolio().position(instrument()).qty;
        let equity = engine
            .strategy()
            .curve()
            .last()
            .expect("the path ends with a live book");
        (held, equity)
    };
    let (quote, received) = (quant_sim::FeeIn::Quote, quant_sim::FeeIn::Received);

    // In the quote a fee is only a cost: the same fills, less money.
    let (held_free, free) = ending("0", quote);
    let (held_quote, costed) = ending("0.0001", quote);
    assert!(held_free.is_zero() && held_quote.is_zero(), "both sold all");
    assert!(costed < free, "{costed} under fees, {free} without");

    // In the asset received, the fee also left nearly a lot unsold.
    let (held, richer) = ending("0.0001", received);
    assert_eq!(held, qty("0.0000099"));
    assert!(
        richer > free,
        "{richer} under a fee in the base, {free} without: the unsold lot rode the jump"
    );
}

// --- The position the report ends on ---

fn held(qty: &str) -> quant_engine::Position {
    quant_engine::Position {
        qty: qty.parse().expect("qty"),
        avg_px: "100".parse().expect("px"),
    }
}

fn sampled_at(mark: Option<&str>) -> EquityPoint {
    EquityPoint {
        ts: Ts::from_nanos(0),
        equity: None,
        cash: CASH,
        mark: mark.map(|m| m.parse().expect("mark")),
    }
}

#[test]
fn the_report_says_what_was_held_at_the_end_and_at_what_mark() {
    // Flat is said, not left to be inferred from a missing line.
    assert_eq!(
        crate::position_line(
            quant_engine::Position::default(),
            Some(&sampled_at(Some("100")))
        ),
        "position  flat at the end"
    );
    // Held and priced: the quantity, what it was worth, and the mid it was
    // worth that at, so cash plus the value is the last equity by hand.
    assert_eq!(
        crate::position_line(held("0.000003"), Some(&sampled_at(Some("120000")))),
        "position  0.000003 held at the end, worth 0.36 at the last sample's mid 120000"
    );
    // Held and blind at the end: no value is invented for it.
    for last in [Some(sampled_at(None)), None] {
        let line = crate::position_line(held("0.000003"), last.as_ref());
        assert_eq!(
            line,
            "position  0.000003 held at the end, and no mark at the last sample to value it at"
        );
    }
    // A value past what a notional holds is said to be, not wrapped or panicked on.
    assert!(
        crate::position_line(held("10000000000"), Some(&sampled_at(Some("10000000000"))))
            .contains("too large to value")
    );
}

#[test]
fn a_run_with_fees_in_the_base_ends_on_dust_and_the_report_accounts_for_it() {
    // The case the line exists for. Under fees taken in the asset received an
    // exit sells what it holds rounded down to the lot, so a run that ends
    // "flat" in every sense the strategy knows still holds a few satoshi, and
    // cash alone is not the account.
    let engine = run_with(&choppy(), spot_fees());
    let position = engine.portfolio().position(instrument());
    assert!(!position.is_flat(), "the base fees leave dust behind");
    let curve = engine.strategy().curve();
    let last = curve.points.last().expect("a sampled run");
    let mark = last.mark.expect("the path ends with a live book");
    let value = mark.notional(position.qty).expect("a small value");
    assert_eq!(
        crate::position_line(position, Some(last)),
        format!(
            "position  {} held at the end, worth {value} at the last sample's mid {mark}",
            position.qty
        )
    );
    // And it is the whole of the gap between the cash and the last equity.
    assert_eq!(
        Notional::from_raw(engine.portfolio().cash().raw() + value.raw()),
        curve.last().expect("valued at the end"),
    );
}
