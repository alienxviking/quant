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
