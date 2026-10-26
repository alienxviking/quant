//! The venue's account frames, written as the venue writes them, and the
//! pieces a live venue is wired from, for tests.
//!
//! On only with the `fixtures` feature, which this crate's tests and
//! `quant-explain`'s use. One set of builders for both, so that the machine's
//! own tests and the permutation test feed it the same bytes: two copies would
//! be two models of the venue's frames that must agree for ever.
//!
//! [`rest`] is the REST side's stand-in, and the wiring pieces below it
//! ([`venue_instrument`], [`credentials`], [`checked`]) are what `wire` takes.
//!
//! Each frame is Binance's documented `executionReport`
//! (`user-data-stream.md` at commit `0556f1a`, the translator's own fixture)
//! with the fields a test names changed, wrapped in the envelope the WebSocket
//! API sends it in. Constructed, not captured: nothing in this system has
//! placed a real order yet.

use std::fmt::Write as _;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use quant_binance::credentials::Credentials;
use quant_binance::exchange_info::VenueInstrument;
use quant_binance::orders::NewOrder;
use quant_binance::trade::{Checked, NotReady, TradeClient};
use quant_binance::user_data::RunTag;
use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, OrderKind, OrderRequest, TimeInForce};
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
use quant_core::report::EndedBy;
use ring::rand::{SecureRandom as _, SystemRandom};
use ring::signature::Ed25519KeyPair;
use serde_json::{json, Value};

pub mod rest;

/// The run tag every fixture's ids are sent under.
pub const RUN: &str = "r1";

/// The run tag, as a [`RunTag`].
///
/// # Panics
///
/// Never: [`RUN`] is a valid tag.
#[must_use]
pub fn run() -> RunTag {
    RunTag::new(RUN).expect("a valid tag")
}

/// The instrument every fixture trades, with both its assets named, as the
/// venue's `exchangeInfo` gives them.
///
/// # Panics
///
/// Never: the literals parse.
#[must_use]
pub fn btcusdt() -> InstrumentDef {
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

/// [`btcusdt`] as the venue's `exchangeInfo` would give it, with no rule it
/// does not model: what `wire` takes.
#[must_use]
pub fn venue_instrument() -> VenueInstrument {
    VenueInstrument {
        definition: btcusdt(),
        unmodelled: Vec::new(),
    }
}

/// Credentials made in this process for this test, and nowhere else: an
/// Ed25519 key pair generated here and a random API key. No key material is
/// read from anywhere or written anywhere, and none outlives the test. The
/// local REST server checks no signature; `quant-binance`'s own tests do.
///
/// # Panics
///
/// If `ring` cannot make a key pair.
#[must_use]
pub fn credentials() -> Credentials {
    let rng = SystemRandom::new();
    let document = Ed25519KeyPair::generate_pkcs8(&rng).expect("ring makes a key pair");
    let mut bytes = [0_u8; 16];
    rng.fill(&mut bytes).expect("random bytes");
    let key = bytes.iter().fold(String::new(), |mut text, b| {
        let _ = write!(text, "{b:02x}");
        text
    });
    Credentials::new(key, &STANDARD.encode(document.as_ref())).expect("a key ring made")
}

/// A market buy of 0.001 [`btcusdt`], the shape `MaCrossover` sends, for
/// `instrument`.
///
/// # Panics
///
/// Never: the literals parse.
#[must_use]
pub fn market_buy(instrument: quant_core::instrument::InstrumentId) -> OrderRequest {
    OrderRequest {
        instrument,
        side: Side::Buy,
        qty: "0.001".parse().expect("qty"),
        kind: OrderKind::Market,
        time_in_force: TimeInForce::Gtc,
    }
}

/// A `Checked` minted the only way there is, by running the checks against
/// `client`'s venue, with the host clock measured `offset_ms` ahead.
///
/// # Errors
///
/// The first check's refusal.
///
/// # Panics
///
/// Never: the probe encodes.
pub async fn checked(client: &TradeClient, offset_ms: i64) -> Result<Checked, NotReady> {
    let instrument = InstrumentRegistry::new().register(btcusdt());
    let probe = NewOrder::encode(
        &market_buy(instrument),
        ClientOrderId(0),
        &run(),
        &btcusdt(),
    )
    .expect("the probe encodes");
    client.checked(&probe, "BTCUSDT", offset_ms).await
}

/// The client id this run sends order `id` under.
#[must_use]
pub fn ours(id: u64) -> String {
    format!("q-{RUN}-{id}")
}

/// The documented report with `fields` replaced, in its envelope.
fn report(fields: &[(&str, Value)]) -> Vec<u8> {
    let mut event = json!({
        "e": "executionReport",
        "E": 1_499_405_658_658_i64,
        "s": "BTCUSDT",
        "c": "",
        "S": "BUY",
        "o": "MARKET",
        "f": "GTC",
        "q": "0",
        "p": "0.00000000",
        "P": "0.00000000",
        "F": "0.00000000",
        "g": -1,
        "C": "",
        "x": "NEW",
        "X": "NEW",
        "r": "NONE",
        "i": 0,
        "l": "0.00000000",
        "z": "0.00000000",
        "L": "0.00000000",
        "n": "0",
        "N": null,
        "T": 1_499_405_658_657_i64,
        "t": -1,
        "v": 3,
        "I": 8_641_984,
        "w": true,
        "m": false,
        "M": false,
        "O": 1_499_405_658_657_i64,
        "Z": "0.00000000",
        "Y": "0.00000000",
        "Q": "0.00000000",
        "W": 1_499_405_658_657_i64,
        "V": "NONE"
    });
    for (field, value) in fields {
        event[*field] = value.clone();
    }
    serde_json::to_vec(&json!({ "subscriptionId": 0, "event": event })).expect("serializable")
}

/// `NEW`: the venue has order `order`, which we sent as `client` for `qty`.
#[must_use]
pub fn new_order(client: &str, order: u64, qty: &str) -> Vec<u8> {
    report(&[("c", json!(client)), ("i", json!(order)), ("q", json!(qty))])
}

/// `TRADE`: `last` of the order traded, as trade `trade_id`, leaving `filled`
/// filled in all. Each report's execution id `I` is the trade id plus a
/// thousand, so that a reader of the wrong one shows.
#[must_use]
pub fn trade(
    client: &str,
    order: u64,
    qty: &str,
    last: &str,
    filled: &str,
    trade_id: i64,
) -> Vec<u8> {
    trade_with(
        client,
        order,
        qty,
        last,
        filled,
        trade_id,
        trade_id + 1000,
        "USDT",
    )
}

/// [`trade`], with its execution id and its fee asset named.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn trade_with(
    client: &str,
    order: u64,
    qty: &str,
    last: &str,
    filled: &str,
    trade_id: i64,
    exec_id: i64,
    fee_asset: &str,
) -> Vec<u8> {
    let done = qty
        .parse::<quant_core::fixed::Qty>()
        .ok()
        .zip(filled.parse::<quant_core::fixed::Qty>().ok())
        .is_some_and(|(qty, filled)| qty == filled);
    report(&[
        ("c", json!(client)),
        ("i", json!(order)),
        ("q", json!(qty)),
        ("x", json!("TRADE")),
        ("X", json!(if done { "FILLED" } else { "PARTIALLY_FILLED" })),
        ("l", json!(last)),
        ("z", json!(filled)),
        ("L", json!("76650.12")),
        ("n", json!("0.05")),
        ("N", json!(fee_asset)),
        ("t", json!(trade_id)),
        ("I", json!(exec_id)),
    ])
}

/// `TRADE` with every field a rehearsal sets: the order's `side` as the venue
/// spells it (`BUY`, `SELL`), `last` of it traded at `price` as trade
/// `trade_id`, `filled` in all, and the venue's `fee` in `fee_asset`. For a
/// rehearsal whose round trip has a buy's fee taken in the base, as a spot
/// venue takes it, and a sell's in the quote.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn trade_priced(
    client: &str,
    order: u64,
    side: &str,
    qty: &str,
    (last, filled, price): (&str, &str, &str),
    (fee, fee_asset): (&str, &str),
    trade_id: i64,
) -> Vec<u8> {
    let done = qty
        .parse::<quant_core::fixed::Qty>()
        .ok()
        .zip(filled.parse::<quant_core::fixed::Qty>().ok())
        .is_some_and(|(qty, filled)| qty == filled);
    report(&[
        ("c", json!(client)),
        ("i", json!(order)),
        ("S", json!(side)),
        ("q", json!(qty)),
        ("x", json!("TRADE")),
        ("X", json!(if done { "FILLED" } else { "PARTIALLY_FILLED" })),
        ("l", json!(last)),
        ("z", json!(filled)),
        ("L", json!(price)),
        ("n", json!(fee)),
        ("N", json!(fee_asset)),
        ("t", json!(trade_id)),
        ("I", json!(trade_id + 1000)),
    ])
}

/// The order ended, `how`, with `filled` filled. A cancel names the order in
/// `C` and the cancel request, whose id the venue generated, in `c`.
#[must_use]
pub fn ended(client: &str, order: u64, qty: &str, filled: &str, how: EndedBy) -> Vec<u8> {
    let (x, status) = match how {
        EndedBy::Cancelled => ("CANCELED", "CANCELED"),
        EndedBy::Expired => ("EXPIRED", "EXPIRED"),
        EndedBy::SelfTradePrevention => ("TRADE_PREVENTION", "EXPIRED_IN_MATCH"),
    };
    let (c, original) = match how {
        EndedBy::Cancelled => ("6gCrw2kRUAF9CvJDGP16IP", client),
        EndedBy::Expired | EndedBy::SelfTradePrevention => (client, ""),
    };
    report(&[
        ("c", json!(c)),
        ("C", json!(original)),
        ("i", json!(order)),
        ("q", json!(qty)),
        ("x", json!(x)),
        ("X", json!(status)),
        ("z", json!(filled)),
    ])
}

/// The subscription's answer, accepting (web-socket-api.md:8222-8230).
#[must_use]
pub fn subscribed() -> Vec<u8> {
    br#"{"id":"subscribe-2","status":200,"result":{"subscriptionId":0}}"#.to_vec()
}

/// The subscription's answer, refusing.
#[must_use]
pub fn subscribe_refused() -> Vec<u8> {
    br#"{"id":"subscribe-3","status":400,"error":{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}}"#.to_vec()
}

/// `serverShutdown` (web-socket-api.md:368-381).
#[must_use]
pub fn server_shutdown() -> Vec<u8> {
    br#"{"event":{"e":"serverShutdown","E":1770123456789}}"#.to_vec()
}

/// `eventStreamTerminated` (user-data-stream.md:325-341).
#[must_use]
pub fn stream_terminated() -> Vec<u8> {
    br#"{"subscriptionId":0,"event":{"e":"eventStreamTerminated","E":1728973001334}}"#.to_vec()
}

/// An `outboundAccountPosition`, an account event nothing here reads.
#[must_use]
pub fn balances() -> Vec<u8> {
    br#"{"subscriptionId":0,"event":{"e":"outboundAccountPosition","E":1564034571105,"u":1564034571073,"B":[{"a":"BTC","f":"10000.000000","l":"0.000000"}]}}"#.to_vec()
}
