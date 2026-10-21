//! The shell's own tests (`docs/live-run.md` §5, *`LiveVenue`, decided before
//! the code*, §5), against [`RestVenue`], a scripted local REST server, and a
//! `ManualClock` driving the worker. The account side is a channel the test
//! holds, read through the real `AccountFeed`. Every wait is on a fact the
//! test can observe, and bounded.

use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use quant_binance::account::AccountFeed;
use quant_binance::trade::TradeClient;
use quant_core::execution::{ClientOrderId, OrderRequest};
use quant_core::instrument::{InstrumentId, InstrumentRegistry};
use quant_core::report::{CancelRefusal, Landing, Notice, OrderReport, RejectedWhy, VenueReport};
use quant_core::time::{ManualClock, Ts};
use quant_engine::{ExecutionVenue, Unready};
use quant_recorder::CaptureRecord;

use crate::fixtures::rest::{Reply, RestVenue, Script, Seen};
use crate::fixtures::{self, market_buy};
use crate::shell::{wire, LiveVenue, ORDERS_BOUND};
use crate::worker::{Worker, WorkerStats};

/// The host clock, measured this far ahead of the venue's.
const OFFSET_MS: i64 = 700;

/// The engine's clock when the strategy submits.
const NOW: Ts = Ts::from_millis(1_700_000_000_000);

/// How long any one wait may take before the test fails.
const WITHIN: Duration = Duration::from_secs(10);

/// A wired venue, its server, its worker's clock and the account's channel.
struct Rig {
    runtime: tokio::runtime::Runtime,
    rest: RestVenue,
    venue: LiveVenue,
    worker: Option<Worker>,
    stats: Arc<WorkerStats>,
    clock: Arc<ManualClock>,
    _account: SyncSender<CaptureRecord>,
    instrument: InstrumentId,
}

impl Rig {
    /// A venue whose account is heard, wired to a server answering `script`.
    fn new(script: Script) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a runtime");
        let rest = RestVenue::start(script);
        let client = TradeClient::new(rest.base(), Duration::from_secs(5), fixtures::credentials())
            .expect("a client");
        let checked = runtime
            .block_on(fixtures::checked(&client, OFFSET_MS))
            .expect("the local checks pass");
        let (account, records) = sync_channel(64);
        account
            .send(CaptureRecord::Account {
                local_recv_ts: NOW,
                ingest_seq: 1,
                payload: fixtures::subscribed(),
            })
            .expect("open");
        let instrument = InstrumentRegistry::new().register(fixtures::btcusdt());
        let clock = Arc::new(ManualClock::new(NOW));
        let (mut venue, worker) = wire(
            instrument,
            &fixtures::venue_instrument(),
            fixtures::run(),
            checked,
            client,
            AccountFeed::new(records, Arc::default()),
            Arc::clone(&clock) as Arc<dyn quant_core::time::Clock>,
        )
        .expect("wired");
        let mut heard = Vec::new();
        venue.poll(NOW, &mut heard);
        assert_eq!(
            venue.readiness(),
            Ok(()),
            "the account is heard: {heard:#?}"
        );
        let stats = worker.stats();
        Self {
            runtime,
            rest,
            venue,
            worker: Some(worker),
            stats,
            clock,
            _account: account,
            instrument,
        }
    }

    /// Start the worker on the rig's runtime.
    fn spawn(&mut self) {
        let worker = self.worker.take().expect("spawned once");
        self.runtime.spawn(worker.run());
    }

    fn order(&self) -> OrderRequest {
        market_buy(self.instrument)
    }

    /// Submit order `id` as the engine does: asked first, then sent.
    fn submit(&mut self, id: u64) {
        let request = self.order();
        assert_eq!(self.venue.ready(&request), Ok(()), "order {id}");
        self.venue.submit(ClientOrderId(id), &request, NOW);
    }

    /// Poll until `done` holds of everything handed out so far.
    fn poll_until(
        &mut self,
        what: &str,
        done: impl Fn(&[VenueReport]) -> bool,
    ) -> Vec<VenueReport> {
        let deadline = Instant::now() + WITHIN;
        let mut out = Vec::new();
        loop {
            self.venue.poll(NOW, &mut out);
            if done(&out) {
                return out;
            }
            assert!(
                Instant::now() < deadline,
                "gave up waiting for {what}: {out:#?}"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// Wait until the worker has put `n` answers on the inbox.
    fn answered(&self, n: u64) {
        let deadline = Instant::now() + WITHIN;
        while self.stats.answered() < n {
            assert!(Instant::now() < deadline, "the worker never answered {n}");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

/// The order requests, as `METHOD n`.
fn requests(seen: &[Seen]) -> Vec<String> {
    seen.iter()
        .map(|s| format!("{} {}", s.method, s.names().unwrap_or_default()))
        .collect()
}

fn placed(out: &[VenueReport], id: u64) -> Option<Ts> {
    out.iter().find_map(|report| match report {
        VenueReport::Notice(Notice::Placed {
            client_order_id,
            ts,
            ..
        }) if client_order_id.0 == id => Some(*ts),
        VenueReport::Notice(_) | VenueReport::Order(_) => None,
    })
}

fn never_landed(out: &[VenueReport], id: u64) -> Option<String> {
    out.iter().find_map(|report| match report {
        VenueReport::Order(OrderReport::Rejected {
            client_order_id,
            why: Some(RejectedWhy::NeverLanded(Landing::NotSent { why })),
            ..
        }) if client_order_id.0 == id => Some(why.as_str().to_owned()),
        VenueReport::Order(_) | VenueReport::Notice(_) => None,
    })
}

fn cancel_not_sent(out: &[VenueReport], id: u64) -> Option<String> {
    out.iter().find_map(|report| match report {
        VenueReport::Notice(Notice::CancelRefused {
            client_order_id,
            why: CancelRefusal::NotSent { why },
            ..
        }) if client_order_id.0 == id => Some(why.as_str().to_owned()),
        VenueReport::Notice(_) | VenueReport::Order(_) => None,
    })
}

#[test]
fn the_venue_and_the_worker_can_cross_threads() {
    fn send<T: Send>(_: &T) {}
    let rig = Rig::new(Script::default());
    send(&rig.venue);
    let worker = rig.worker.expect("not spawned");
    send(&worker.run());
}

#[test]
fn an_order_is_stamped_with_the_engines_now_less_the_offset_and_its_answer_when_it_came() {
    // The stamp the venue checks against `recvWindow` is the engine's `now`
    // less the measured offset: positive when the host is ahead, so taken
    // off. `venue-check` once had this sign wrong. And the answer is stamped
    // by the worker's clock as it arrived, not by the engine's.
    let mut rig = Rig::new(Script::default());
    rig.spawn();
    let answered_at = Ts::from_millis(1_700_000_000_250);
    rig.clock.set(answered_at);
    rig.submit(1);
    let out = rig.poll_until("the order's answer", |out| placed(out, 1).is_some());
    let seen = rig.rest.order_requests();
    assert_eq!(requests(&seen), ["POST 1"]);
    let expected = (NOW.as_millis() - OFFSET_MS).to_string();
    assert_eq!(seen[0].param("timestamp"), Some(expected.as_str()));
    assert_eq!(placed(&out, 1), Some(answered_at));
    assert_eq!(rig.venue.outstanding(), 0);
}

#[test]
fn a_cancel_goes_after_the_answer_to_its_order_stamped_by_the_worker_at_sending() {
    // The order and its cancel are both queued before the worker starts,
    // and the order is held at the server. A worker that sent the next
    // request without waiting for the last one's answer would send the
    // cancel while the order waits; this one sends it after the order's
    // answer, stamped from its own clock as it goes rather than at the
    // engine's `now`.
    let mut rig = Rig::new(Script {
        orders: [Reply::Hold].into(),
        ..Script::default()
    });
    rig.submit(1);
    rig.venue.cancel(ClientOrderId(1), NOW);
    rig.spawn();
    let seen = rig.rest.until("a request", WITHIN, |seen| !seen.is_empty());
    assert_eq!(requests(&seen[..1]), ["POST 1"], "the order goes first");
    let sent_at = Ts::from_millis(1_700_000_009_000);
    rig.clock.set(sent_at);
    let released = Instant::now();
    rig.rest.release();
    let seen = rig.rest.until("the cancel", WITHIN, |seen| seen.len() == 2);
    assert_eq!(requests(&seen), ["POST 1", "DELETE 1"]);
    assert!(
        seen[1].at > released,
        "the cancel went while its order waited"
    );
    let expected = (sent_at.as_millis() - OFFSET_MS).to_string();
    assert_eq!(seen[1].param("timestamp"), Some(expected.as_str()));
}

#[test]
fn an_order_for_an_instrument_not_wired_is_refused_and_reaches_nothing() {
    // `NoFilters` from `ready`, and if `submit` is called regardless, the
    // order is `never_landed` and goes nowhere. The order after it shows the
    // worker had nothing ahead of it: the queue is first in first out.
    let mut rig = Rig::new(Script::default());
    rig.spawn();
    let mut registry = InstrumentRegistry::new();
    registry.register(fixtures::btcusdt());
    let mut other = fixtures::btcusdt();
    other.symbol = "ETHUSDT".to_owned();
    let elsewhere = OrderRequest {
        instrument: registry.register(other),
        ..rig.order()
    };
    assert_eq!(rig.venue.ready(&elsewhere), Err(Unready::NoFilters));
    rig.venue.submit(ClientOrderId(1), &elsewhere, NOW);
    rig.submit(2);
    let out = rig.poll_until("order 2's answer", |out| placed(out, 2).is_some());
    assert_eq!(requests(&rig.rest.order_requests()), ["POST 2"]);
    let why = never_landed(&out, 1).expect("order 1 never landed");
    assert!(why.contains("NoFilters"), "{why}");
}

#[test]
fn with_the_worker_stalled_the_queue_fills_and_a_held_cancel_goes_before_later_orders() {
    // One order held at the server, four queued behind it: the queue is
    // full, `submit` has returned every time, and `ready` says `QueueFull`.
    // A cancel then waits in the shell, and once room appears it goes out
    // ahead of any order submitted after it.
    let mut rig = Rig::new(Script {
        orders: [Reply::Hold].into(),
        ..Script::default()
    });
    rig.spawn();
    rig.submit(1);
    rig.rest.until("order 1", WITHIN, |seen| !seen.is_empty());
    for id in 2..=5 {
        rig.submit(id);
    }
    assert_eq!(ORDERS_BOUND, 4);
    assert_eq!(rig.venue.ready(&rig.order()), Err(Unready::QueueFull));
    rig.venue.cancel(ClientOrderId(1), NOW);
    assert_eq!(rig.venue.stats().held, 1, "held, not refused");
    rig.rest.release();
    rig.rest.until("order 2", WITHIN, |seen| seen.len() >= 2);
    // The worker has taken order 2, so the queue has room, and the cancel
    // is still held: nothing new may take the slot ahead of it.
    assert_eq!(rig.venue.ready(&rig.order()), Err(Unready::QueueFull));
    // The poll that finds the room sends the held cancel first, and only
    // then is the venue ready.
    let deadline = Instant::now() + WITHIN;
    let mut out = Vec::new();
    while rig.venue.ready(&rig.order()).is_err() {
        assert!(Instant::now() < deadline, "never ready again: {out:#?}");
        rig.venue.poll(NOW, &mut out);
        std::thread::sleep(Duration::from_millis(2));
    }
    rig.submit(6);
    let seen = rig.rest.until("order 6", WITHIN, |seen| seen.len() == 7);
    assert_eq!(
        requests(&seen),
        ["POST 1", "POST 2", "POST 3", "POST 4", "POST 5", "DELETE 1", "POST 6"]
    );
}

#[test]
fn after_stop_sending_nothing_more_leaves_and_every_request_is_answered_unsent() {
    // Order 1 is held at the server, in flight. Behind it, orders 2 and 3, a
    // cancel of 1 and order 4 fill the queue, and a cancel of 2 is held. The
    // venue stops sending, and the server is let answer order 1: everything
    // else comes back unsent, nothing else reaches it, and `outstanding`
    // reaches zero.
    let mut rig = Rig::new(Script {
        orders: [Reply::Hold].into(),
        ..Script::default()
    });
    rig.spawn();
    rig.submit(1);
    rig.rest.until("order 1", WITHIN, |seen| !seen.is_empty());
    rig.submit(2);
    rig.submit(3);
    rig.venue.cancel(ClientOrderId(1), NOW);
    rig.submit(4);
    rig.venue.cancel(ClientOrderId(2), NOW);
    assert_eq!(rig.venue.stats().held, 1);
    assert_eq!(rig.venue.outstanding(), 5);

    rig.venue.stop_sending();
    assert_eq!(rig.venue.ready(&rig.order()), Err(Unready::QueueFull));
    rig.rest.release();
    rig.answered(5);
    let out = rig.poll_until("every answer", |_| true);
    assert_eq!(rig.venue.outstanding(), 0, "{out:#?}");
    assert!(placed(&out, 1).is_some(), "order 1 was in flight: {out:#?}");
    for id in [2, 3, 4] {
        assert_eq!(never_landed(&out, id).as_deref(), Some("stopping"), "{id}");
    }
    for id in [1, 2] {
        assert_eq!(
            cancel_not_sent(&out, id).as_deref(),
            Some("stopping"),
            "{id}"
        );
    }
    assert_eq!(rig.stats.stopped(), 4);
    assert_eq!(requests(&rig.rest.order_requests()), ["POST 1"]);

    // And a cancel asked for after it goes nowhere either.
    rig.venue.cancel(ClientOrderId(1), NOW);
    let mut out = Vec::new();
    rig.venue.poll(NOW, &mut out);
    assert_eq!(cancel_not_sent(&out, 1).as_deref(), Some("stopping"));
}

#[test]
fn a_cancel_for_an_order_never_sent_or_already_held_goes_nowhere() {
    let mut rig = Rig::new(Script {
        orders: [Reply::Hold].into(),
        ..Script::default()
    });
    rig.spawn();
    rig.venue.cancel(ClientOrderId(9), NOW);
    let mut out = Vec::new();
    rig.venue.poll(NOW, &mut out);
    assert!(cancel_not_sent(&out, 9).is_some(), "{out:#?}");

    rig.submit(1);
    rig.rest.until("order 1", WITHIN, |seen| !seen.is_empty());
    for id in 2..=5 {
        rig.submit(id);
    }
    rig.venue.cancel(ClientOrderId(3), NOW);
    rig.venue.cancel(ClientOrderId(3), NOW);
    let mut out = Vec::new();
    rig.venue.poll(NOW, &mut out);
    assert_eq!(rig.venue.stats().held, 1, "one held per order");
    assert!(cancel_not_sent(&out, 3).is_some(), "{out:#?}");
    rig.rest.release();
}
