//! Steps 3 and 4 of a live run's shutdown: settle, stop sending, settle.
//!
//! `docs/live-run.md` §5, *`LiveVenue`, decided before the code*, §1, gives
//! the whole sequence, and [`crate::shell`]'s module docs repeat it for the
//! binary that runs it. These two steps are the waits, and each has a deadline
//! of its own, which is what keeps the sequence from hanging: nothing in it
//! waits for a channel to close.

use std::time::{Duration, Instant};

use quant_binance::trade::ORDER_TIMEOUT;
use quant_core::execution::ClientOrderId;
use quant_core::source::EventSource;
use quant_engine::{Engine, RiskLayer, Strategy, IDLE_TICK};
use tracing::error;

use crate::shell::LiveVenue;

/// How long each wait may take, and how long to pause when a wake brought
/// nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deadlines {
    /// Step 3: settling until nothing is working. Longer than
    /// `ORDER_TIMEOUT`, so that an order the worker is still waiting on can
    /// be answered.
    pub settle: Duration,
    /// Step 4: settling until every request handed to the worker is answered,
    /// once it sends nothing more. `ORDER_TIMEOUT` and a margin: the one
    /// request in flight may take that long, and everything queued behind it
    /// is answered at once.
    pub answers: Duration,
    /// The pause after a wake that delivered nothing.
    pub pause: Duration,
}

impl Default for Deadlines {
    fn default() -> Self {
        let margin = Duration::from_secs(5);
        Self {
            settle: ORDER_TIMEOUT + margin,
            answers: ORDER_TIMEOUT + margin,
            pause: IDLE_TICK,
        }
    }
}

/// What the drain ended with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drained {
    /// Orders the engine is still working: what `stopped.working` holds, and
    /// what item 8's duty 6 refuses to trade past.
    pub working: Vec<ClientOrderId>,
    /// Nothing was working before step 3's deadline.
    pub settled: bool,
    /// Every request handed to the worker was answered before step 4's
    /// deadline.
    pub answered: bool,
    /// Requests still unanswered when the drain ended.
    pub outstanding: usize,
}

/// Steps 3 and 4: `settle` until nothing is working or `deadlines.settle`
/// passes; then [`LiveVenue::stop_sending`], and `settle` until
/// [`LiveVenue::outstanding`] is zero or `deadlines.answers` passes.
///
/// The second wait ends at its deadline whether or not the worker runs: one
/// never spawned, or gone with its runtime, would hold `outstanding` above
/// zero for ever. Passing either deadline is logged as an error, and every
/// order still unanswered is in [`Drained::working`].
///
/// The deadlines are measured on the host's monotonic clock. This paces a
/// shutdown and decides nothing a replay must reproduce, so it is not the
/// injected clock: what it delivers is stamped by the venue as on any wake.
pub fn drain<S, R, K>(engine: &mut Engine<S, LiveVenue, R, K>, deadlines: &Deadlines) -> Drained
where
    S: EventSource,
    R: RiskLayer,
    K: Strategy,
{
    let started = Instant::now();
    let settled = loop {
        let settled = engine.settle();
        if settled.working.is_empty() {
            break true;
        }
        if started.elapsed() >= deadlines.settle {
            error!(
                working = settled.working.len(),
                "orders still working at the settle deadline"
            );
            break false;
        }
        if settled.delivered == 0 {
            std::thread::sleep(deadlines.pause);
        }
    };

    engine.venue().stop_sending();
    let stopped = Instant::now();
    let answered = loop {
        let settled = engine.settle();
        if engine.venue().outstanding() == 0 {
            break true;
        }
        if stopped.elapsed() >= deadlines.answers {
            error!(
                outstanding = engine.venue().outstanding(),
                "requests still unanswered at the shutdown deadline"
            );
            break false;
        }
        if settled.delivered == 0 {
            std::thread::sleep(deadlines.pause);
        }
    };

    Drained {
        working: engine.working(),
        settled,
        answered,
        outstanding: engine.venue().outstanding(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use quant_binance::account::AccountFeed;
    use quant_binance::trade::TradeClient;
    use quant_core::event::{EventMeta, MarketEvent, Side, Trade};
    use quant_core::execution::{ClientOrderId, ExecutionEvent};
    use quant_core::instrument::{InstrumentId, InstrumentRegistry};
    use quant_core::source::{EventSource, SourceError, Wake};
    use quant_core::time::{Clock, SystemClock};
    use quant_engine::{AllowAll, Context, Engine, Strategy};
    use quant_recorder::CaptureRecord;

    use super::{drain, Deadlines};
    use crate::fixtures::rest::{RestVenue, Script};
    use crate::fixtures::{self, market_buy};
    use crate::shell::wire;

    /// One market event, then the end.
    #[derive(Debug)]
    struct Once(Option<MarketEvent>);

    impl EventSource for Once {
        fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
            self.0.take().map(Ok)
        }

        fn next_event_timeout(&mut self, _: Duration) -> Option<Result<Wake, SourceError>> {
            self.0.take().map(|event| Ok(Wake::Event(event)))
        }
    }

    /// Buys once, on the first market event.
    #[derive(Debug)]
    struct BuyOnce(InstrumentId);

    impl Strategy for BuyOnce {
        fn on_market_event(&mut self, _: &MarketEvent, ctx: &mut Context<'_>) {
            ctx.submit(market_buy(self.0));
        }

        fn on_execution(&mut self, _: &ExecutionEvent, _: &mut Context<'_>) {}
    }

    /// An engine that has run, with one order sent to a venue whose account
    /// is heard and whose worker runs only if `spawn` says so.
    fn ran(
        spawn: bool,
    ) -> (
        tokio::runtime::Runtime,
        RestVenue,
        impl FnOnce() -> super::Drained,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a runtime");
        let rest = RestVenue::start(Script::default());
        let client = TradeClient::new(rest.base(), Duration::from_secs(5), fixtures::credentials())
            .expect("a client");
        let checked = runtime
            .block_on(fixtures::checked(&client, 0))
            .expect("the checks pass");
        let now = SystemClock.now();
        let (account, records) = std::sync::mpsc::sync_channel(8);
        account
            .send(CaptureRecord::Account {
                local_recv_ts: now,
                ingest_seq: 1,
                payload: fixtures::subscribed(),
            })
            .expect("open");
        let instrument = InstrumentRegistry::new().register(fixtures::btcusdt());
        let (venue, worker) = wire(
            instrument,
            &fixtures::venue_instrument(),
            fixtures::run(),
            checked,
            client,
            AccountFeed::new(records, Arc::default()),
            Arc::new(SystemClock),
        )
        .expect("wired");
        // Never spawned and never dropped: what a binary that forgot to spawn
        // it would hold. Dropped, its queue would close and the venue would
        // take no order at all.
        let unspawned = if spawn {
            runtime.spawn(worker.run());
            None
        } else {
            Some(worker)
        };
        let market = MarketEvent::Trade(Trade {
            meta: EventMeta {
                instrument,
                exchange_ts: now,
                local_recv_ts: now,
                ingest_seq: 1,
            },
            px: "76650.12".parse().expect("px"),
            qty: "0.01".parse().expect("qty"),
            aggressor: Side::Buy,
            venue_trade_id: 1,
        });
        let mut engine = Engine::new(
            Once(Some(market)),
            venue,
            AllowAll,
            BuyOnce(instrument),
            "100".parse().expect("cash"),
        );
        let finish = move || {
            engine.run().expect("ran");
            let drained = {
                let deadlines = Deadlines {
                    settle: Duration::from_millis(300),
                    answers: Duration::from_millis(300),
                    pause: Duration::from_millis(5),
                };
                drain(&mut engine, &deadlines)
            };
            drop((account, unspawned));
            drained
        };
        (runtime, rest, finish)
    }

    #[test]
    fn a_worker_never_spawned_holds_no_shutdown_past_its_deadlines() {
        // The order was handed to a worker that never ran, so nothing can
        // answer it: both waits end at their deadlines, and the order is
        // still working, for `stopped.working`.
        let (_runtime, rest, finish) = ran(false);
        let started = Instant::now();
        let drained = finish();
        assert!(started.elapsed() < Duration::from_secs(5), "it ended");
        assert!(!drained.settled && !drained.answered, "{drained:?}");
        assert_eq!(drained.outstanding, 1);
        assert_eq!(drained.working, [ClientOrderId(1)]);
        assert!(rest.order_requests().is_empty());
    }

    #[test]
    fn a_worker_that_answers_ends_the_second_wait_before_its_deadline() {
        // The other side of the boundary: placed and never heard from on the
        // stream, the order is still working at the first deadline, and the
        // answer has come, so the second wait ends on it.
        let (_runtime, rest, finish) = ran(true);
        let drained = finish();
        assert!(!drained.settled && drained.answered, "{drained:?}");
        assert_eq!(drained.outstanding, 0);
        assert_eq!(drained.working, [ClientOrderId(1)]);
        assert_eq!(rest.order_requests().len(), 1);
    }
}
