//! A local stand-in for Binance, for rehearsing the live harness end to end:
//! `ops/rehearse-live.ps1` runs under it (`docs/live-run.md` §7, *Running it*).
//!
//! ```text
//! cargo build --release -p quant-backtest --example fake-binance
//! target\release\examples\fake-binance.exe [--minutes N] -- COMMAND [ARGS...]
//! ```
//!
//! It starts three servers on `127.0.0.1`, each on a port the system
//! chooses, from the fixtures the tests already use: the REST side
//! (`quant_live::fixtures::rest`: the clock, `venue-check`'s three checks,
//! `exchangeInfo`, a depth snapshot, the balances, orders and cancels
//! acknowledged), the account side (`quant_binance::fake_venue`, which
//! checks each logon's Ed25519 signature) and the market stream
//! (`quant_binance::fake_market`). Then it runs `COMMAND` with five variables
//! in its environment and nothing else changed, waits for it, and exits with
//! its exit code:
//!
//! - `QUANT_FAKE_REST`, `QUANT_FAKE_WS_API` and `QUANT_FAKE_MARKET_WS`, the
//!   three servers' addresses, for the harness's `-Rest`, `-WsApi` and
//!   `-MarketWs`;
//! - `BINANCE_API_KEY` and `BINANCE_ED25519_PRIVATE_KEY`, the credentials the
//!   account side accepts. The key pair is generated in this process when it
//!   starts and is valid nowhere else. It goes into the child's environment
//!   and nowhere more: it is never printed and never written to a file, which
//!   is why this runs the command rather than printing what to export.
//!
//! # What it does that the fixtures do not
//!
//! The fixtures answer what a test tells them to; a harness rehearsal has no
//! test to tell them. So this drives them, and that is all it adds:
//!
//! - **The market moves.** Once the run has connected, the touch walks a
//!   dollar every [`STEP`], eight steps up and eight down, so a crossover on
//!   one-second mids crosses at every turn. The depth snapshot is kept at the
//!   book's current state, so a resync anchors wherever the walk has got to.
//! - **Every order fills.** Each order the REST side acknowledges is answered
//!   on the account stream as the venue would answer a market order: a `NEW`
//!   and a `TRADE` for the whole quantity, a buy at the ask with its fee in
//!   the base asset and a sell at the bid with its fee in the quote, ten
//!   basis points either way. So every order has filled by the time a cancel
//!   for it can arrive, and a cancel is refused as the venue refuses one for
//!   an order it no longer has open: `-2011`, *"Unknown order sent."*, with
//!   nothing on the account stream.
//!
//! It is still not an exchange: no book of its own beyond the touch it
//! walks, no partial fills, no refusals. It exists so that the harness, the
//! real binaries and their checks can be run end to end on a machine that
//! must not contact Binance, and it is an example, so no build that ships
//! contains it.

use std::process::{Command, ExitCode};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quant_binance::fake_market::{self, FakeMarket};
use quant_binance::fake_venue::{self, FakeVenue};
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::time::{Clock, SystemClock};
use quant_live::fixtures::rest::{venue_order_id, Reply, RestVenue, Script, Seen, DEPTH};
use quant_live::fixtures::{new_order, trade_priced};

/// How often the touch moves.
const STEP: Duration = Duration::from_millis(400);

/// How many steps the walk takes each way before it turns.
const LEG: u64 = 8;

/// The default snapshot's best bid, at update id 100 ([`DEPTH`]).
const FIRST_BID: i64 = 76_650;
const FIRST_UPDATE: u64 = 100;

/// How long the servers run when not told: longer than any rehearsal.
const DEFAULT_MINUTES: u64 = 30;

fn main() -> ExitCode {
    let mut minutes = DEFAULT_MINUTES;
    let mut args = std::env::args().skip(1);
    let mut command = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--minutes" => match args.next().and_then(|m| m.parse().ok()) {
                Some(m) if m > 0 => minutes = m,
                _ => return usage(),
            },
            "--" => {
                command.extend(args.by_ref());
                break;
            }
            _ => return usage(),
        }
    }
    let Some((program, rest)) = command.split_first() else {
        return usage();
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("a runtime");
    let account = Arc::new(runtime.block_on(FakeVenue::start(fake_venue::Script::default())));
    let market = Arc::new(runtime.block_on(FakeMarket::start()));
    // Every order fills as it lands, so a cancel always finds the order gone.
    let mut script = Script::default().always("/api/v3/depth", DEPTH);
    script.cancels = std::iter::repeat_with(|| Reply::refuse(400, -2011, "Unknown order sent."))
        .take(10_000)
        .collect();
    let rest_venue = Arc::new(RestVenue::start_for(
        script,
        Duration::from_secs(minutes * 60),
    ));
    println!("fake-binance  rest    {}", rest_venue.base());
    println!("fake-binance  ws-api  {}", account.url());
    println!("fake-binance  market  {}", market.url());
    println!("fake-binance  running: {}", command.join(" "));

    let bid = Arc::new(Mutex::new(FIRST_BID));
    let walk = runtime.spawn(walk(
        Arc::clone(&market),
        Arc::clone(&rest_venue),
        Arc::clone(&bid),
    ));
    let answers = runtime.spawn(answer(
        Arc::clone(&account),
        Arc::clone(&rest_venue),
        Arc::clone(&bid),
    ));

    let mut child = Command::new(program);
    child.args(rest);
    child.env("QUANT_FAKE_REST", rest_venue.base());
    child.env("QUANT_FAKE_WS_API", account.url());
    child.env("QUANT_FAKE_MARKET_WS", market.url());
    for (name, value) in account.child_environment() {
        child.env(name, value);
    }
    let status = child.status();
    walk.abort();
    answers.abort();
    let seen = rest_venue.order_requests();
    let orders = seen.iter().filter(|s| s.method == "POST").count();
    println!(
        "fake-binance  done: {orders} order(s) filled, {} cancel(s) refused, {} market connection(s)",
        seen.len() - orders,
        market.connections()
    );
    match status {
        Ok(status) => status
            .code()
            .and_then(|code| u8::try_from(code).ok())
            .map_or(ExitCode::FAILURE, ExitCode::from),
        Err(e) => {
            eprintln!("fake-binance  could not run {program}: {e}");
            ExitCode::FAILURE
        }
    }
}

fn usage() -> ExitCode {
    eprintln!("usage: fake-binance [--minutes N] -- COMMAND [ARGS...]");
    ExitCode::from(2)
}

/// A whole number of dollars, as the venue writes a price.
fn dollars(n: i64) -> String {
    format!("{n}.00")
}

/// Walk the touch once the run is connected, keeping the snapshot current.
async fn walk(market: Arc<FakeMarket>, rest: Arc<RestVenue>, bid: Arc<Mutex<i64>>) {
    while market.connections() == 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut update = FIRST_UPDATE;
    let mut step = 0_u64;
    loop {
        tokio::time::sleep(STEP).await;
        let by = if (step / LEG).is_multiple_of(2) {
            1
        } else {
            -1
        };
        let (old, new) = {
            let mut bid = bid
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let old = *bid;
            *bid += by;
            (old, *bid)
        };
        update += 1;
        let (old_bid, new_bid) = (dollars(old), dollars(new));
        let (old_ask, new_ask) = (dollars(old + 1), dollars(new + 1));
        market.send(&fake_market::depth(
            "BTCUSDT",
            update,
            update,
            &[(new_bid.as_str(), "1"), (old_bid.as_str(), "0")],
            &[(new_ask.as_str(), "1"), (old_ask.as_str(), "0")],
            SystemClock.now().as_millis(),
        ));
        let snapshot = fake_market::snapshot(
            update,
            &[(new_bid.as_str(), "1.00000000")],
            &[(new_ask.as_str(), "1.00000000")],
        );
        rest.script(|script| {
            script.bodies.insert("/api/v3/depth".to_owned(), snapshot);
        });
        step += 1;
    }
}

/// Answer every order and cancel the REST side has acknowledged, on the
/// account stream, as the venue would.
async fn answer(account: Arc<FakeVenue>, rest: Arc<RestVenue>, bid: Arc<Mutex<i64>>) {
    let mut answered = 0;
    let mut trade_id = 0_i64;
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let seen = rest.order_requests();
        for request in seen.iter().skip(answered) {
            let bid = *bid
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            trade_id += 1;
            for frame in reports(request, bid, trade_id) {
                account.event(&String::from_utf8(frame).expect("text"));
            }
        }
        answered = seen.len();
    }
}

/// The account stream's reports for one order: a market order filled whole
/// at the touch. A cancel has none: the REST side refused it.
fn reports(request: &Seen, bid: i64, trade_id: i64) -> Vec<Vec<u8>> {
    let Some(n) = request.names() else {
        return Vec::new();
    };
    let order = venue_order_id(n);
    if request.method != "POST" {
        return Vec::new();
    }
    let (Some(client), Some(side), Some(qty)) = (
        request.param("newClientOrderId"),
        request.param("side"),
        request.param("quantity"),
    ) else {
        return Vec::new();
    };
    let Ok(amount) = qty.parse::<Qty>() else {
        return Vec::new();
    };
    // Ten basis points: a buy's fee in the asset bought, a sell's in the
    // asset received, as a spot venue takes them.
    let (price, fee, asset) = if side == "BUY" {
        let price = dollars(bid + 1);
        (
            price,
            Qty::from_raw(amount.raw() / 1_000).to_string(),
            "BTC",
        )
    } else {
        let price = dollars(bid);
        let notional = price
            .parse::<Px>()
            .ok()
            .and_then(|px| px.notional(amount))
            .unwrap_or(Notional::ZERO);
        (
            price,
            Notional::from_raw(notional.raw() / 1_000).to_string(),
            "USDT",
        )
    };
    vec![
        new_order(client, order, qty),
        trade_priced(
            client,
            order,
            side,
            qty,
            (qty, qty, &price),
            (&fee, asset),
            trade_id,
        ),
    ]
}
