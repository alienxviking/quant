//! Orders as Binance's REST API spells them.
//!
//! Pure logic, like [`crate::user_data`]: no network, no credentials, no clock.
//! [`NewOrder::encode`] turns the engine's [`OrderRequest`] into the parameters
//! of `POST /api/v3/order`, and [`CancelOrder::encode`] names an order for
//! `DELETE /api/v3/order`. Both stop short of `timestamp`, `recvWindow` and
//! `signature`, which the signer appends, and neither sends anything: that is
//! the transport's job, wired in a later pull request.
//!
//! Citations are `file:line` in `binance/binance-spot-api-docs` at commit
//! `0556f1a`. Nothing here has met the real venue yet. Every request below is
//! checked against the documentation's own examples, and the requests criterion
//! D sends on purpose are the first real test of them.
//!
//! # Every order asks for an acknowledgement and nothing more
//!
//! `docs/live-run.md` §5, *one authority per fact*: an order's acceptance and
//! every fill come from the user-data stream. `MARKET` and `LIMIT` orders
//! default to a `FULL` response (rest-api.md:2159), which carries the fills
//! (rest-api.md:2236-2292), so every order sends `newOrderRespType=ACK`, whose
//! response carries only the order's identity (faqs/spot_glossary.md:7-8). A
//! response that delivered fills would give each fill two authorities.
//!
//! Every order also sends `newClientOrderId`, from [`RunTag::client_order_id`],
//! so that the stream's reports about it can be recognised as this run's and
//! an order whose outcome is unknown can be looked up by the name it was sent
//! under. A lookup key and not an idempotency key: the venue refuses a repeated
//! id only while the first order is still open (rest-api.md:2153), which is why
//! a timed-out order is looked up and never resent (`docs/live-run.md` §5).
//!
//! # A market order carries no `timeInForce`
//!
//! The venue makes it mandatory for `LIMIT` and does not list it for `MARKET`
//! (rest-api.md:2168-2173), and its error catalogue has codes for a parameter
//! sent when not required (errors.md:101-104, 115-116). [`OrderRequest`]
//! carries a time in force on every order, and `MaCrossover` sets `Gtc` on the
//! market orders it sends (`crates/quant-backtest/src/ma.rs`), so the encoder
//! drops it rather than forwarding what the venue does not ask for. Nothing is
//! lost by dropping it: a market order already takes what it can and expires
//! the rest (enums.md:56), which is all `Ioc` would ask for, and the simulator
//! ends an unfilled market order the same way whatever its time in force.
//!
//! # Every value is percent-encoded, once, and the result is what is signed
//!
//! The venue signs *"the query string concatenated without separator to the
//! HTTP body. Any non-ASCII character must be percent-encoded before signing."*
//! (rest-api.md:256, and in the same words for an Ed25519 key,
//! rest-api.md:555). A symbol may be non-ASCII: the documentation's
//! own example is `１２３４５６` (rest-api.md:288), whose encoded payload it
//! gives in full (rest-api.md:323). So every value is percent-encoded here,
//! and the query string returned is both the one that is signed and the one
//! that is sent. The transport must pass it on verbatim: a URL builder that
//! encodes its query again would turn each `%` into `%25`, and the venue would
//! check the signature against bytes nobody signed.
//!
//! Every byte outside RFC 3986's unreserved set is encoded, not only the
//! non-ASCII ones. That set is exactly what the documentation's own encoding
//! script leaves alone (rest-api.md:522), and it means no value can contain a
//! bare `&` or `=` and so smuggle in a parameter of its own.
//!
//! # The venue's filters are the venue's to apply
//!
//! The encoder refuses only what it cannot write down: an order of no size, a
//! limit at no price, an instrument this endpoint does not trade. Tick, lot and
//! notional rules are not checked here, on purpose. Criterion D6 sends an order
//! below the minimum notional to see the venue refuse it, and criterion B pairs
//! every order's venue verdict with the model's; an encoder that refused
//! locally would make the first impossible, and leave the second comparing the
//! model with itself.

use core::fmt;

use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, OrderKind, OrderRequest, TimeInForce};
use quant_core::fixed::{Px, Qty};
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind};

use crate::user_data::RunTag;

/// Where both requests go: `POST` places an order (rest-api.md:2128-2132) and
/// `DELETE` cancels one (rest-api.md:2380-2384).
pub const ORDER_PATH: &str = "/api/v3/order";

/// A new order, written as the parameters of `POST /api/v3/order`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOrder {
    query: String,
    client_order_id: String,
}

impl NewOrder {
    /// Write `request`, which the engine knows as `id`, as an order for
    /// `instrument` under `run`'s ids.
    ///
    /// The caller pairs the request with its instrument: an [`InstrumentDef`]
    /// carries no id to check `request.instrument` against.
    ///
    /// # Errors
    ///
    /// [`EncodeError`] for an order that cannot be written down at all. Never
    /// for one the venue's filters would refuse: see the module docs.
    pub fn encode(
        request: &OrderRequest,
        id: ClientOrderId,
        run: &RunTag,
        instrument: &InstrumentDef,
    ) -> Result<Self, EncodeError> {
        spot_on_binance(instrument)?;
        if !request.qty.is_positive() {
            return Err(EncodeError::NonPositiveQuantity(request.qty));
        }
        if let Some(limit) = request.limit() {
            if !limit.is_positive() {
                return Err(EncodeError::NonPositivePrice(limit));
            }
        }
        let client_order_id = run.client_order_id(id);
        let mut query = Query::default();
        query.push("symbol", &instrument.symbol);
        query.push("side", side(request.side));
        // Exhaustive, with no `_` arm, so a new order kind or time in force is
        // a compile error here rather than an order sent as something else.
        match (request.kind, request.time_in_force) {
            (OrderKind::Market, TimeInForce::Gtc | TimeInForce::Ioc) => {
                query.push("type", "MARKET");
                query.push("quantity", &request.qty.to_string());
            }
            (OrderKind::Limit { limit }, time_in_force) => {
                query.push("type", "LIMIT");
                query.push(
                    "timeInForce",
                    match time_in_force {
                        TimeInForce::Gtc => "GTC",
                        TimeInForce::Ioc => "IOC",
                    },
                );
                query.push("quantity", &request.qty.to_string());
                query.push("price", &limit.to_string());
            }
        }
        query.push("newClientOrderId", &client_order_id);
        query.push("newOrderRespType", "ACK");
        Ok(Self {
            query: query.0,
            client_order_id,
        })
    }

    /// The parameters as a query string, every value percent-encoded: what is
    /// signed and what is sent. See the module docs.
    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    /// The `newClientOrderId` this order is sent under.
    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }
}

/// A request to cancel an order, written as the parameters of
/// `DELETE /api/v3/order`.
///
/// The order is named by `origClientOrderId`, our id for it, and not by the
/// venue's `orderId`. The venue needs one of the two (rest-api.md:2402), and
/// ours is the one that exists from the moment the order leaves, where the
/// venue's may still be on its way back when the strategy cancels. Sending
/// both would make the venue check one against the other (rest-api.md:2403),
/// which adds a way to fail and nothing we need.
///
/// No `newClientOrderId` of its own. The venue gives a cancelled order a new
/// client id either way: the parameter is *"New ID for the canceled order.
/// Automatically generated if not sent"* and it *"will replace `clientOrderId`
/// of the canceled order"* (web-socket-api.md:4142, 4181; the REST page says
/// only *"Used to uniquely identify this cancel"*, rest-api.md:2396). So leaving
/// it out does not keep the order's name. What it avoids is our choosing the
/// new one. An id from our own namespace would be a name the run tag counts as
/// one of ours, given to an order that already has one, and [`RunTag`] exists
/// so that no name of ours ever means two orders. The venue's generated id
/// carries no prefix of ours, which is why the translator reads a cancel's `C`
/// and not its `c`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOrder {
    query: String,
    client_order_id: String,
}

impl CancelOrder {
    /// A cancel of the order the engine knows as `id`, sent under `run`, on
    /// `instrument`.
    ///
    /// # Errors
    ///
    /// [`EncodeError::NotBinanceSpot`] for an instrument this endpoint does not
    /// trade.
    pub fn encode(
        id: ClientOrderId,
        run: &RunTag,
        instrument: &InstrumentDef,
    ) -> Result<Self, EncodeError> {
        spot_on_binance(instrument)?;
        let client_order_id = run.client_order_id(id);
        let mut query = Query::default();
        query.push("symbol", &instrument.symbol);
        query.push("origClientOrderId", &client_order_id);
        Ok(Self {
            query: query.0,
            client_order_id,
        })
    }

    /// The parameters as a query string, every value percent-encoded: what is
    /// signed and what is sent.
    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    /// The `origClientOrderId` the order is named by.
    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }
}

/// Why an order cannot be written down.
///
/// Deliberately not a venue rule. An order the venue's filters would refuse is
/// encoded faithfully, so that the venue says so: see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodeError {
    /// Zero or negative. Direction is the side, so there is no order of no
    /// size, and a negative one is a sign error on its way to the venue.
    NonPositiveQuantity(Qty),
    /// A limit order whose limit is zero or negative.
    NonPositivePrice(Px),
    /// An instrument this endpoint does not trade. `/api/v3/order` is Binance
    /// spot, and the venue is the endpoint: no parameter could say that an
    /// order is meant for another venue or another product.
    NotBinanceSpot {
        exchange: Exchange,
        kind: InstrumentKind,
    },
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonPositiveQuantity(qty) => {
                write!(
                    f,
                    "an order for {qty} is not an order: size must be positive"
                )
            }
            Self::NonPositivePrice(px) => {
                write!(f, "a limit of {px} is not a price: it must be positive")
            }
            Self::NotBinanceSpot { exchange, kind } => write!(
                f,
                "this encoder writes Binance spot orders, and the instrument is {kind:?} on \
                 {exchange}"
            ),
        }
    }
}

impl std::error::Error for EncodeError {}

/// Whether `instrument` is something `/api/v3/order` trades.
fn spot_on_binance(instrument: &InstrumentDef) -> Result<(), EncodeError> {
    match (instrument.exchange, instrument.kind) {
        (Exchange::Binance, InstrumentKind::Spot) => Ok(()),
        (exchange, kind) => Err(EncodeError::NotBinanceSpot { exchange, kind }),
    }
}

/// The venue's spelling of a side (enums.md:115-118).
const fn side(side: Side) -> &'static str {
    match side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    }
}

/// Parameters as `name=value` pairs joined by `&` (rest-api.md:299), each
/// value percent-encoded as it is added.
///
/// Names are not encoded. They are this module's own literals, and every one
/// of them is letters only.
#[derive(Default)]
struct Query(String);

impl Query {
    fn push(&mut self, name: &str, value: &str) {
        if !self.0.is_empty() {
            self.0.push('&');
        }
        self.0.push_str(name);
        self.0.push('=');
        percent_encode(value, &mut self.0);
    }
}

/// Append `value` to `out`, percent-encoded.
///
/// RFC 3986's unreserved characters pass unchanged, which are exactly the ones
/// the documentation's encoding script leaves alone (rest-api.md:522). Every
/// other byte of the value's UTF-8 becomes `%` and two uppercase hex digits,
/// which is how the documentation writes its encoded example (rest-api.md:323)
/// and the case RFC 3986 asks for.
fn percent_encode(value: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::instrument::{FilterBreach, Filters, InstrumentRegistry};

    fn px(s: &str) -> Px {
        s.parse().expect("price")
    }

    fn qty(s: &str) -> Qty {
        s.parse().expect("quantity")
    }

    fn run() -> RunTag {
        RunTag::new("r1").expect("a valid tag")
    }

    /// The venue's rules for BTCUSDT as this repository's fixtures write them.
    /// The encoder must not read them; the filter tests use them to prove
    /// their orders really do break one.
    fn btcusdt() -> InstrumentDef {
        InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: px("0.01"),
            lot_size: qty("0.00001"),
            min_notional: "5".parse().expect("notional"),
        }
    }

    fn filters(instrument: &InstrumentDef) -> Filters {
        Filters {
            tick_size: instrument.tick_size,
            lot_size: instrument.lot_size,
            min_notional: instrument.min_notional,
        }
    }

    fn order(
        side: Side,
        amount: &str,
        kind: OrderKind,
        time_in_force: TimeInForce,
    ) -> OrderRequest {
        OrderRequest {
            instrument: InstrumentRegistry::new().register(btcusdt()),
            side,
            qty: qty(amount),
            kind,
            time_in_force,
        }
    }

    fn market(side: Side, amount: &str, time_in_force: TimeInForce) -> OrderRequest {
        order(side, amount, OrderKind::Market, time_in_force)
    }

    fn limit(side: Side, amount: &str, price: &str, time_in_force: TimeInForce) -> OrderRequest {
        order(
            side,
            amount,
            OrderKind::Limit { limit: px(price) },
            time_in_force,
        )
    }

    fn encode(request: &OrderRequest) -> NewOrder {
        NewOrder::encode(request, ClientOrderId(7), &run(), &btcusdt()).expect("encodes")
    }

    /// The query as `(name, value)` pairs, in order and still encoded.
    fn pairs(query: &str) -> Vec<(&str, &str)> {
        query
            .split('&')
            .map(|pair| pair.split_once('=').expect("every pair is name=value"))
            .collect()
    }

    /// The one value of `name`, failing if it is sent twice.
    fn value<'q>(query: &'q str, name: &str) -> Option<&'q str> {
        let mut found = pairs(query).into_iter().filter(|(n, _)| *n == name);
        let first = found.next().map(|(_, v)| v);
        assert!(found.next().is_none(), "{name} sent twice in {query}");
        first
    }

    #[test]
    fn a_market_order_never_carries_a_time_in_force() {
        // `MaCrossover`'s order exactly (crates/quant-backtest/src/ma.rs):
        // market, with `Gtc`, which the venue does not ask a market order for.
        let crossover = encode(&market(Side::Buy, "0.001", TimeInForce::Gtc));
        assert_eq!(
            crossover.query(),
            "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=0.001\
             &newClientOrderId=q-r1-7&newOrderRespType=ACK"
        );
        // And with `Ioc`, which a market order needs no more than `Gtc`.
        for time_in_force in [TimeInForce::Gtc, TimeInForce::Ioc] {
            for side in [Side::Buy, Side::Sell] {
                let sent = encode(&market(side, "0.001", time_in_force));
                assert_eq!(value(sent.query(), "timeInForce"), None, "{sent:?}");
                assert_eq!(value(sent.query(), "type"), Some("MARKET"), "{sent:?}");
            }
        }
    }

    #[test]
    fn a_limit_order_carries_its_time_in_force_quantity_and_price() {
        assert_eq!(
            encode(&limit(Side::Sell, "0.25", "68123.45", TimeInForce::Gtc)).query(),
            "symbol=BTCUSDT&side=SELL&type=LIMIT&timeInForce=GTC&quantity=0.25\
             &price=68123.45&newClientOrderId=q-r1-7&newOrderRespType=ACK"
        );
        assert_eq!(
            value(
                encode(&limit(Side::Buy, "0.25", "68123.45", TimeInForce::Ioc)).query(),
                "timeInForce"
            ),
            Some("IOC")
        );
    }

    #[test]
    fn every_order_asks_for_an_ack_under_its_run_tagged_id() {
        // Every kind, time in force and side, and ids at both ends of the
        // range, so a branch that forgot either parameter has nowhere to hide.
        let tag = RunTag::new("M8run2").expect("a valid tag");
        let kinds = [
            OrderKind::Market,
            OrderKind::Limit {
                limit: px("68123.45"),
            },
        ];
        for kind in kinds {
            for time_in_force in [TimeInForce::Gtc, TimeInForce::Ioc] {
                for side in [Side::Buy, Side::Sell] {
                    for id in [0, 7, u64::MAX] {
                        let request = order(side, "0.001", kind, time_in_force);
                        let sent = NewOrder::encode(&request, ClientOrderId(id), &tag, &btcusdt())
                            .expect("encodes");
                        let expected = tag.client_order_id(ClientOrderId(id));
                        assert_eq!(value(sent.query(), "newOrderRespType"), Some("ACK"));
                        assert_eq!(
                            value(sent.query(), "newClientOrderId"),
                            Some(expected.as_str()),
                            "{request:?}"
                        );
                        assert_eq!(sent.client_order_id(), expected);
                    }
                }
            }
        }
    }

    #[test]
    fn a_non_ascii_symbol_is_percent_encoded_as_the_documentation_shows() {
        // The documentation's two example requests (rest-api.md:271-295), and
        // their signature payloads after percent-encoding, verbatim
        // (rest-api.md:311, 323). Ours sends the same six parameters in the
        // same order, then two the examples do not; the signer appends the
        // examples' last two.
        let examples = [
            (
                "LTCBTC",
                "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1\
                 &recvWindow=5000&timestamp=1499827319559",
            ),
            (
                "１２３４５６",
                "symbol=%EF%BC%91%EF%BC%92%EF%BC%93%EF%BC%94%EF%BC%95%EF%BC%96&side=BUY\
                 &type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000\
                 &timestamp=1499827319559",
            ),
        ];
        for (symbol, documented) in examples {
            let mut instrument = btcusdt();
            symbol.clone_into(&mut instrument.symbol);
            let sent = NewOrder::encode(
                &limit(Side::Buy, "1", "0.1", TimeInForce::Gtc),
                ClientOrderId(7),
                &run(),
                &instrument,
            )
            .expect("encodes");
            let ours: Vec<&str> = sent.query().split('&').collect();
            let theirs: Vec<&str> = documented.split('&').collect();
            assert_eq!(ours[..6], theirs[..6], "{symbol}");
            assert_eq!(
                ours[6..],
                ["newClientOrderId=q-r1-7", "newOrderRespType=ACK"],
                "{symbol}"
            );
        }
    }

    #[test]
    fn no_value_can_smuggle_in_a_parameter() {
        // Everything outside the unreserved set is encoded, so a value can
        // never contain the `&` and `=` that would make it two parameters.
        let mut instrument = btcusdt();
        "A&side=SELL b+c%/~é".clone_into(&mut instrument.symbol);
        let sent = CancelOrder::encode(ClientOrderId(7), &run(), &instrument).expect("encodes");
        assert_eq!(
            sent.query(),
            "symbol=A%26side%3DSELL%20b%2Bc%25%2F~%C3%A9&origClientOrderId=q-r1-7"
        );
        assert_eq!(pairs(sent.query()).len(), 2);
    }

    #[test]
    fn quantities_and_prices_go_out_exactly_as_fixed_point_writes_them() {
        // `Display` on the fixed-point types: no exponent, no padding, no
        // rounding, and it reads back as exactly the value it came from.
        for (size, written) in [
            ("0.001", "0.001"),
            ("0.00000001", "0.00000001"),
            ("1", "1"),
            ("1.50000000", "1.5"),
            ("100000", "100000"),
            ("92233720368.54775807", "92233720368.54775807"),
        ] {
            let sent = encode(&market(Side::Buy, size, TimeInForce::Gtc));
            assert_eq!(value(sent.query(), "quantity"), Some(written), "{size}");
            assert_eq!(written.parse::<Qty>(), Ok(qty(size)), "{size}");
        }
        for (price, written) in [
            ("68123.45", "68123.45"),
            ("0.1", "0.1"),
            ("0.00000001", "0.00000001"),
            ("12.10000000", "12.1"),
        ] {
            let sent = encode(&limit(Side::Buy, "1", price, TimeInForce::Gtc));
            assert_eq!(value(sent.query(), "price"), Some(written), "{price}");
            assert_eq!(written.parse::<Px>(), Ok(px(price)), "{price}");
        }
    }

    #[test]
    fn filter_breaches_are_encoded_for_the_venue_to_judge() {
        // Each order breaks a rule the instrument states, which the first
        // assertion proves, so that the encoder's silence about it is a fact
        // about the encoder rather than about the fixture.
        let instrument = btcusdt();
        let rules = filters(&instrument);
        let mark = px("68123.45");

        // Criterion D6's order: worth about 68 cents against a minimum of 5.
        let tiny = market(Side::Buy, "0.00001", TimeInForce::Gtc);
        assert_eq!(
            rules.rejects(mark, tiny.qty),
            Some(FilterBreach::MinNotional)
        );
        assert_eq!(value(encode(&tiny).query(), "quantity"), Some("0.00001"));

        // Not a whole number of lots.
        let ragged = market(Side::Sell, "0.0012345", TimeInForce::Gtc);
        assert_eq!(rules.rejects(mark, ragged.qty), Some(FilterBreach::LotSize));
        assert_eq!(
            value(encode(&ragged).query(), "quantity"),
            Some("0.0012345")
        );

        // A limit off the tick.
        let off_tick = limit(Side::Buy, "0.001", "68123.456", TimeInForce::Gtc);
        assert_eq!(
            rules.rejects(px("68123.456"), off_tick.qty),
            Some(FilterBreach::TickSize)
        );
        assert_eq!(value(encode(&off_tick).query(), "price"), Some("68123.456"));
    }

    #[test]
    fn only_an_order_that_cannot_be_written_down_is_refused() {
        let id = ClientOrderId(7);
        for size in ["0", "-0.001"] {
            for request in [
                market(Side::Buy, size, TimeInForce::Gtc),
                limit(Side::Buy, size, "68123.45", TimeInForce::Gtc),
            ] {
                assert_eq!(
                    NewOrder::encode(&request, id, &run(), &btcusdt()),
                    Err(EncodeError::NonPositiveQuantity(qty(size))),
                    "{request:?}"
                );
            }
        }
        for price in ["0", "-68123.45"] {
            let request = limit(Side::Buy, "0.001", price, TimeInForce::Ioc);
            assert_eq!(
                NewOrder::encode(&request, id, &run(), &btcusdt()),
                Err(EncodeError::NonPositivePrice(px(price))),
                "{request:?}"
            );
        }
        // And the line falls exactly where the engine's own seam check draws
        // it, so an order the engine lets through is never one this refuses.
        for size in ["-1", "0", "0.00000001", "1"] {
            for kind in [
                OrderKind::Market,
                OrderKind::Limit { limit: px("-1") },
                OrderKind::Limit { limit: px("0") },
                OrderKind::Limit {
                    limit: px("0.00000001"),
                },
            ] {
                let request = order(Side::Sell, size, kind, TimeInForce::Gtc);
                assert_eq!(
                    NewOrder::encode(&request, id, &run(), &btcusdt()).is_ok(),
                    request.is_valid(),
                    "{request:?}"
                );
            }
        }
    }

    #[test]
    fn an_instrument_this_endpoint_does_not_trade_is_refused() {
        // The same symbol on the futures venue is a different product, and
        // this endpoint would trade it as spot.
        let mut futures = btcusdt();
        futures.exchange = Exchange::BinanceUsdFutures;
        let mut perpetual = btcusdt();
        perpetual.kind = InstrumentKind::PerpetualSwap;
        for instrument in [futures, perpetual] {
            let refused = EncodeError::NotBinanceSpot {
                exchange: instrument.exchange,
                kind: instrument.kind,
            };
            let request = market(Side::Buy, "0.001", TimeInForce::Gtc);
            assert_eq!(
                NewOrder::encode(&request, ClientOrderId(7), &run(), &instrument).err(),
                Some(refused)
            );
            assert_eq!(
                CancelOrder::encode(ClientOrderId(7), &run(), &instrument).err(),
                Some(refused)
            );
        }
    }

    #[test]
    fn a_cancel_names_the_order_by_our_id_and_sends_no_id_of_its_own() {
        let cancel = CancelOrder::encode(ClientOrderId(7), &run(), &btcusdt()).expect("encodes");
        assert_eq!(cancel.query(), "symbol=BTCUSDT&origClientOrderId=q-r1-7");
        assert_eq!(value(cancel.query(), "newClientOrderId"), None);
        assert_eq!(cancel.client_order_id(), "q-r1-7");
    }
}
