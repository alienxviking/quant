//! Orders as Binance's REST API spells them, and what its answers mean.
//!
//! Pure logic, like [`crate::user_data`]: no network, no credentials, no clock.
//! [`NewOrder::encode`] turns the engine's [`OrderRequest`] into the parameters
//! of `POST /api/v3/order`, and [`CancelOrder::encode`] names an order for
//! `DELETE /api/v3/order`. Both stop short of `timestamp`, `recvWindow` and
//! `signature`, which the signer appends, and neither sends anything: that is
//! the transport's job, [`TradeClient::place_order`] and
//! [`TradeClient::cancel_order`]. What the transport saw comes back here as an
//! [`Attempt`], and [`NewOrder::classify`] and [`CancelOrder::classify`] say
//! what it means.
//!
//! [`TradeClient::place_order`]: crate::trade::TradeClient::place_order
//! [`TradeClient::cancel_order`]: crate::trade::TradeClient::cancel_order
//!
//! Citations are `file:line` in `binance/binance-spot-api-docs` at commit
//! `0556f1a`. Nothing here has met the real venue yet. The requests are checked
//! against the documentation's own examples, and the answers are the
//! documentation's examples or are built from its error catalogue, so the
//! orders criterion D sends on purpose, and the answers they get, are the
//! first real test of both. Those answers belong beside these fixtures once
//! they are recorded.
//!
//! # Every order asks for an acknowledgement and nothing more
//!
//! `docs/live-run.md` §5, *one authority per fact*: an order's acceptance and
//! every fill come from the user-data stream. `MARKET` and `LIMIT` orders
//! default to a `FULL` response (rest-api.md:2159), which carries the fills
//! (rest-api.md:2236-2292), so every order sends `newOrderRespType=ACK`, whose
//! response carries only the order's identity (`faqs/spot_glossary.md:7-8`). A
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
//!
//! # An unknown outcome is never a refusal
//!
//! The classification is lopsided on purpose. An order classified
//! [`Placement::Refused`] leaves the engine's ledger as one that never existed.
//! If the venue in fact placed it, its fill arrives as an orphan and never
//! reaches the portfolio, and an order sent in its place doubles the position.
//! An order classified [`Placement::Unknown`] stays working, and readiness
//! refuses every new order until the stream mentions it or a lookup settles it
//! (`docs/live-run.md` §5): loud and stuck, which is the safe failure. So a
//! refusal needs a code the documentation ties to a request that was not
//! carried out, and every ambiguity is unknown:
//!
//! - any 5xx, whatever code it carries: *"It is important to **NOT** treat
//!   this as a failure operation; the execution status is **UNKNOWN** and could
//!   have been a success"* (rest-api.md:116-118);
//! - `-1007` and `-1006`, which say the execution status is unknown
//!   (errors.md:27-31), and `-1000`, `-1001` and `-1008`, an unknown error, an
//!   internal one and an overload, which the documentation does not tie to a
//!   request left undone (errors.md:13-17, 33-34);
//! - *"Duplicate order sent."*, under `-2010` or `-1013` alike: the request
//!   was refused because an open order already holds the id this order was
//!   sent under (errors.md:375; rest-api.md:2153), and that order may be this
//!   one, sent before. Matched on the word rather than the exact text, because
//!   messages can vary (errors.md:3-4) and a near miss here would fall on the
//!   unsafe side;
//! - any other code outside the ones that are, among them a code the endpoint
//!   is not expected to return, and a 4xx whose body is not a Binance error at
//!   all;
//! - a 2xx whose body is not the acknowledgement asked for, or that
//!   acknowledges an order sent under another id: the venue may still have
//!   placed ours;
//! - a timeout, or a failure after the request may have left.
//!
//! **The code decides, not the status.** The documentation pairs a code with a
//! status only a handful of times, in one FAQ and in the WebSocket API's
//! examples, which share the REST API's statuses and codes
//! (CHANGELOG.md:2015): `400` with `-2010`, `-2043` and `-2022`
//! (web-socket-api.md:232-235, 3328-3332, 4847-4850), `401` with `-2015`
//! (1324-1327), `409` with `-2021` (4894-4897), `418` with `-1003` (551-554),
//! and `429` with `-1015` (5065-5068; `faqs/order_count_decrement.md:57`). It
//! gives no status for `-1007` or `-1006`. Of a status alone, it says a 4xx is
//! the sender's doing: *"HTTP `4XX` return codes are used for malformed
//! requests; the issue is on the sender's side"* (rest-api.md:111), and
//! *"`400` – your request failed, see `error` for the reason"*
//! (web-socket-api.md:314-315). That comes close to "a 4xx was not placed",
//! and the module still wants a documented code before it calls anything
//! refused. The same lists count among the 4xx a `409`, a request that
//! *"partially failed but also partially succeeded"* (web-socket-api.md:317;
//! rest-api.md:113). And a rule by status would call a `-1007` or `-1006`
//! under a `400` refused, though each says the execution status is unknown
//! (errors.md:27-31) and nothing ties either to a status. So a 4xx is a
//! refusal only when its code says so, and a 5xx never is.
//!
//! Only a request that never left is certainly not placed, and only the
//! transport can know that it never left: [`Attempt::NotConnected`].
//!
//! A refusal keeps the venue's code and message verbatim, for the journal:
//! criterion B compares the rule the venue cited with the rule the model would
//! cite, and the message is where the venue cites it. The engine hears a
//! coarser [`RejectReason`], through [`RefusalCause::reject_reason`].
//!
//! # A cancel's answer is never the order's end
//!
//! The stream is the only authority for how an order ended, as it is for a
//! fill, so a cancel the venue took is [`CancelReply::Acknowledged`] and no
//! more. `-2011` is read by its message. *"Unknown order sent."* is an order
//! that *"could not be found"* (errors.md:374) by an endpoint that cancels
//! *"an active order"* (rest-api.md:2384): no open order by that id, which is
//! how a cancel that lost the race to a fill comes back, and is
//! [`CancelReply::NotOpen`]. The same code carries other messages, about the
//! account, the symbol or the request, that leave the order where it was
//! (errors.md:367-413), and a cancel meets `-2011` or `-1013` when the symbol
//! is not trading (CHANGELOG.md:2703). So every other `-2011` is the cancel
//! refused, and not news about the order.

use core::fmt;

use quant_core::event::Side;
use quant_core::execution::{
    ClientOrderId, OrderKind, OrderRequest, RejectReason, TimeInForce, VenueOrderId,
};
use quant_core::fixed::{Px, Qty};
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind};

use crate::user_data::RunTag;

/// Where both requests go: `POST` places an order (rest-api.md:2128-2132) and
/// `DELETE` cancels one (rest-api.md:2380-2384).
pub const ORDER_PATH: &str = "/api/v3/order";

/// Where a new order goes to be checked and not placed: `POST` to it
/// *"Creates and validates a new order but does not send it into the matching
/// engine"* (rest-api.md:2321-2326). It takes every parameter
/// `POST /api/v3/order` takes (rest-api.md:2337), so a [`NewOrder`]'s query is
/// its query too. [`TradeClient::check_trade_permission`] sends one there, and
/// is the one thing that sends anything there.
///
/// [`TradeClient::check_trade_permission`]: crate::trade::TradeClient::check_trade_permission
pub const ORDER_TEST_PATH: &str = "/api/v3/order/test";

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

    /// What `attempt`, the transport's account of sending this order, means
    /// for the order. The module docs give the rule, and why it leans the way
    /// it does.
    #[must_use]
    pub fn classify(&self, attempt: Attempt<'_>) -> Placement {
        match heard(attempt) {
            Heard::Success { http_status, body } => self.acknowledged(http_status, body),
            Heard::Error { http_status, error } => match error.code {
                // An open order holds this order's id, and may be this order:
                // never a refusal, whichever code it comes under (module docs).
                -1013 | -2010 if names_a_duplicate(&error.msg) => Placement::Unknown {
                    why: WhyUnknown::IdInUse {
                        id: self.client_order_id.clone(),
                        http_status,
                        code: error.code,
                        msg: error.msg,
                    },
                },
                // The two codes whose message names the reason: the API
                // refusing the request before the matching engine
                // (errors.md:36-38), and the matching engine refusing it
                // (errors.md:367-368). The message, not the code, says which
                // reason, because -1013's messages are drawn from both of the
                // documentation's tables (errors.md:38).
                -1013 | -2010 => {
                    let cause = declined_because(&error.msg);
                    Placement::Refused(Refusal::new(http_status, error, cause))
                }
                code => match request_refusal(http_status, code, &NEW_ORDER_ISSUES) {
                    Some(cause) => Placement::Refused(Refusal::new(http_status, error, cause)),
                    None => Placement::Unknown {
                        why: WhyUnknown::unsettled(http_status, error),
                    },
                },
            },
            Heard::Unknown(why) => Placement::Unknown { why },
            Heard::NotSent(why) => Placement::NotSent { why },
        }
    }

    /// A 2xx: placed, if the body is the acknowledgement of this order.
    fn acknowledged(&self, http_status: u16, body: &[u8]) -> Placement {
        match serde_json::from_slice::<Ack>(body) {
            Err(e) => Placement::Unknown {
                why: WhyUnknown::UnreadableBody {
                    http_status,
                    detail: e.to_string(),
                },
            },
            Ok(ack) if ack.client_order_id != self.client_order_id => Placement::Unknown {
                why: WhyUnknown::OtherOrder {
                    sent: self.client_order_id.clone(),
                    echoed: ack.client_order_id,
                },
            },
            Ok(ack) => Placement::Placed {
                order_id: VenueOrderId(ack.order_id.to_string()),
                client_order_id: ack.client_order_id,
                transact_time_ms: ack.transact_time,
            },
        }
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
/// Automatically generated if not sent"*, and it *"will replace `clientOrderId`
/// of the canceled order"* (web-socket-api.md:4142, 4181; the REST page says
/// *"Used to uniquely identify this cancel. Automatically generated by
/// default."*, rest-api.md:2396). So leaving it out does not keep the order's
/// name. What it avoids is our choosing the new one. An id we chose in our
/// run-tagged namespace would give the cancelled order a second name of ours,
/// one a later order could be sent under again, since the venue keeps an id
/// *"unique ... among open orders"* only (rest-api.md:2153), and [`RunTag`]
/// exists so that no name of ours ever means two orders. An id
/// outside that namespace is what the venue's generated one already is, and
/// it is why the translator reads a cancel's `C` and not its `c`.
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

    /// What `attempt`, the transport's account of sending this cancel, means.
    /// See the module docs.
    #[must_use]
    pub fn classify(&self, attempt: Attempt<'_>) -> CancelReply {
        match heard(attempt) {
            Heard::Success { http_status, body } => self.acknowledged(http_status, body),
            Heard::Error { http_status, error } => match error.code {
                -2011 if error.msg == UNKNOWN_ORDER => CancelReply::NotOpen {
                    http_status,
                    code: error.code,
                    msg: error.msg,
                },
                // -2011's other messages (errors.md:367-413), and -1013, a
                // request *"rejected by the API"* (errors.md:36-37), which a
                // cancel meets when the symbol is not trading
                // (CHANGELOG.md:2703).
                -2011 | -1013 => {
                    CancelReply::Refused(Refusal::new(http_status, error, RefusalCause::Declined))
                }
                code => match request_refusal(http_status, code, &CANCEL_ISSUES) {
                    Some(cause) => CancelReply::Refused(Refusal::new(http_status, error, cause)),
                    None => CancelReply::Unknown {
                        why: WhyUnknown::unsettled(http_status, error),
                    },
                },
            },
            Heard::Unknown(why) => CancelReply::Unknown { why },
            Heard::NotSent(why) => CancelReply::NotSent { why },
        }
    }

    /// A 2xx: acknowledged, if the body answers for the order this cancel
    /// named.
    fn acknowledged(&self, http_status: u16, body: &[u8]) -> CancelReply {
        match serde_json::from_slice::<CancelAck>(body) {
            Err(e) => CancelReply::Unknown {
                why: WhyUnknown::UnreadableBody {
                    http_status,
                    detail: e.to_string(),
                },
            },
            Ok(ack) if ack.orig_client_order_id != self.client_order_id => CancelReply::Unknown {
                why: WhyUnknown::OtherOrder {
                    sent: self.client_order_id.clone(),
                    echoed: ack.orig_client_order_id,
                },
            },
            Ok(ack) => CancelReply::Acknowledged {
                order_id: VenueOrderId(ack.order_id.to_string()),
            },
        }
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

/// `name=value`, the value percent-encoded as every parameter here is, for a
/// signed request that is not an order: the `symbol` that
/// [`TradeClient::check_bnb_discount_off`] asks about. The same encoder as an
/// order's, so a symbol goes out spelled the same way in both.
///
/// [`TradeClient::check_bnb_discount_off`]: crate::trade::TradeClient::check_bnb_discount_off
pub(crate) fn param(name: &str, value: &str) -> String {
    let mut query = Query::default();
    query.push(name, value);
    query.0
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

/// What the transport saw when it sent a request.
///
/// Filled in by the caller, the only one that can know it, so that everything
/// from here on is pure. The text in [`Attempt::NotConnected`] and
/// [`Attempt::Failed`] is a description for the log, which the caller has
/// stripped of anything that must not be logged: an HTTP client's error can
/// quote the URL, and the URL carries the signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attempt<'a> {
    /// The venue answered: its HTTP status, and the whole body.
    Answered { status: u16, body: &'a [u8] },
    /// No connection was made, so nothing of the request left this machine: a
    /// name that would not resolve, a connection refused, a TLS handshake that
    /// failed.
    ///
    /// The one outcome that proves an order was not placed, and so the one the
    /// caller must be sure of. A request that may have been written to a
    /// connection, even one that then broke, is [`Attempt::Failed`], and a
    /// timeout once connected is [`Attempt::TimedOut`]. When unsure, not this.
    NotConnected(&'a str),
    /// No answer within the client's own timeout, after the request may have
    /// left. `docs/live-run.md` §5 puts that timeout above the venue's ten
    /// seconds (rest-api.md:102), so that the venue's `-1007` arrives first
    /// when it can: [`ORDER_TIMEOUT`] is the number.
    ///
    /// [`ORDER_TIMEOUT`]: crate::trade::ORDER_TIMEOUT
    TimedOut,
    /// Any other failure after the request may have left: a connection reset,
    /// a body that stopped arriving.
    Failed(&'a str),
}

/// What the venue's answer to a new order means for the order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    /// The venue has the order: its acknowledgement (rest-api.md:2203-2212),
    /// naming the id the order was sent under.
    ///
    /// Not an `Accepted`. Acceptance comes from the stream, the one authority
    /// for it (module docs). What this settles is that the order is not in
    /// doubt, and it carries the venue's id for it.
    Placed {
        /// The venue's `orderId`, written as [`crate::user_data`] writes the
        /// stream's, so that both name one order identically.
        order_id: VenueOrderId,
        /// The `clientOrderId` acknowledged, which is the one the order was
        /// sent under: an acknowledgement naming another is
        /// [`Placement::Unknown`].
        client_order_id: String,
        /// The venue's `transactTime`, in milliseconds since the epoch on the
        /// venue's clock (rest-api.md:97; `faqs/spot_glossary.md:420-421`). For
        /// the record only: the engine dispatches on when we heard a thing,
        /// never on when the venue says it happened (invariant 2).
        transact_time_ms: i64,
    },
    /// The venue refused it, with a code the documentation ties to a request
    /// that was not carried out.
    Refused(Refusal),
    /// Whether the order exists cannot be told from this. Never a rejection:
    /// see the module docs.
    Unknown { why: WhyUnknown },
    /// The request never left. Certainly not placed.
    NotSent { why: String },
}

impl Placement {
    /// The reason to give the engine, when this outcome is one it may hear as
    /// a rejection: a refusal, or a request that never left, which is the
    /// venue being *"unreachable"* in [`RejectReason::Venue`]'s own words.
    ///
    /// `None` for an order the venue placed, and above all for one whose fate
    /// is unknown, which must stay working. A rejection removes an order from
    /// the engine's ledger, and the order's fill would then arrive as an
    /// orphan.
    #[must_use]
    pub fn reject_reason(&self) -> Option<RejectReason> {
        match self {
            Self::Refused(refusal) => Some(refusal.cause.reject_reason()),
            Self::NotSent { .. } => Some(RejectReason::Venue),
            Self::Placed { .. } | Self::Unknown { .. } => None,
        }
    }
}

/// What the venue's answer to a cancel means.
///
/// Never that the order was cancelled: the stream reports how an order ended,
/// and is the only authority for it (module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelReply {
    /// The venue took the cancel, for the order the request named
    /// (rest-api.md:2408-2427). The stream will say how the order ended.
    Acknowledged { order_id: VenueOrderId },
    /// `-2011` with *"Unknown order sent."*: the venue has no open order by
    /// this id (errors.md:374; rest-api.md:2384). That covers one already
    /// filled, which is how a cancel that lost the race comes back (criterion
    /// D5), one already cancelled or expired, and one never placed, so this
    /// does not prove the order ever existed.
    NotOpen {
        http_status: u16,
        code: i64,
        msg: String,
    },
    /// The cancel was refused. The order, if there is one, is as it was.
    Refused(Refusal),
    /// Whether the cancel took effect cannot be told from this.
    Unknown { why: WhyUnknown },
    /// The request never left. The order, if there is one, is as it was.
    NotSent { why: String },
}

/// A request the venue refused, as it said so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub http_status: u16,
    /// The venue's error code, verbatim.
    pub code: i64,
    /// The venue's message, verbatim. Kept for the journal: criterion B
    /// compares the rule the venue cited with the one the model would cite,
    /// and the message is where the venue cites it.
    pub msg: String,
    pub cause: RefusalCause,
}

impl Refusal {
    fn new(http_status: u16, error: VenueError, cause: RefusalCause) -> Self {
        Self {
            http_status,
            code: error.code,
            msg: error.msg,
            cause,
        }
    }
}

/// Why the venue refused a request, as a closed set. The code and message
/// beside it in [`Refusal`] keep the detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalCause {
    /// *"Filter failure: NAME"* (errors.md:425-446), sent under `-1013`, a
    /// request that *"didn't reach the Matching Engine"* (errors.md:36-38).
    Filter(FilterName),
    /// *"Account has insufficient balance for requested action."*
    /// (errors.md:377): criterion D7's refusal, and one the simulator never
    /// makes.
    InsufficientBalance,
    /// Any other refusal, named only in its message: the matching engine's
    /// (`-2010`, *"This code is sent when an error has been returned by the
    /// matching engine"*, errors.md:367-368), or the API's before it
    /// (`-1013`).
    Declined,
    /// `-1021`, a timestamp outside `recvWindow` or too far ahead of the
    /// venue's clock (errors.md:53-55), which the venue checks before it
    /// forwards a request to the matching engine (rest-api.md:225-241).
    Timestamp,
    /// `-1022`, `-2014` or `-2015`: the signature, the key's format, or the
    /// key, address or permissions (errors.md:57-58, 338-342).
    Credentials,
    /// One of the `-11xx` *"Request issues"* (errors.md:71) that the request
    /// can meet, given the parameters it sends: a parameter missing, empty,
    /// malformed or not wanted.
    Malformed,
    /// `-1003` or `-1015` with a 429, a request rate limit broken, or a 418,
    /// an address banned for not backing off (rest-api.md:114-115;
    /// `faqs/order_count_decrement.md:57`).
    RateLimited,
}

impl RefusalCause {
    /// The reason the engine hears, coarser on purpose: [`RejectReason`] is a
    /// closed set of categories, and the detail stays in the [`Refusal`].
    ///
    /// A filter breach is [`RejectReason::Malformed`], whose own docs include
    /// *"a venue filter — tick size, lot size, minimum notional"*.
    #[must_use]
    pub const fn reject_reason(self) -> RejectReason {
        match self {
            Self::Filter(_) | Self::Malformed => RejectReason::Malformed,
            Self::InsufficientBalance => RejectReason::InsufficientFunds,
            Self::Declined | Self::Timestamp | Self::Credentials | Self::RateLimited => {
                RejectReason::Venue
            }
        }
    }
}

/// The filter a *"Filter failure: NAME"* names.
///
/// Every filter the venue documents (filters.md:5-28; errors.md:428-446 lists
/// the same names, all but `MAX_ASSET`), and [`FilterName::Unrecognised`] for
/// any other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterName {
    PriceFilter,
    PercentPrice,
    PercentPriceBySide,
    LotSize,
    MinNotional,
    Notional,
    IcebergParts,
    MarketLotSize,
    MaxNumOrders,
    MaxNumAlgoOrders,
    MaxNumIcebergOrders,
    MaxPosition,
    TrailingDelta,
    MaxNumOrderAmends,
    MaxNumOrderLists,
    ExchangeMaxNumOrders,
    ExchangeMaxNumAlgoOrders,
    ExchangeMaxNumIcebergOrders,
    ExchangeMaxNumOrderLists,
    MaxAsset,
    /// A name the documentation did not have at the commit this was written
    /// against. Not guessed at: the message beside it in [`Refusal`] carries
    /// the name, and a filter nobody here has heard of is a reason to read it.
    Unrecognised,
}

/// Each filter as the venue spells it, in the order filters.md lists them.
/// One table for both directions, so parsing and spelling cannot disagree.
const FILTER_NAMES: [(&str, FilterName); 20] = [
    ("PRICE_FILTER", FilterName::PriceFilter),
    ("PERCENT_PRICE", FilterName::PercentPrice),
    ("PERCENT_PRICE_BY_SIDE", FilterName::PercentPriceBySide),
    ("LOT_SIZE", FilterName::LotSize),
    ("MIN_NOTIONAL", FilterName::MinNotional),
    ("NOTIONAL", FilterName::Notional),
    ("ICEBERG_PARTS", FilterName::IcebergParts),
    ("MARKET_LOT_SIZE", FilterName::MarketLotSize),
    ("MAX_NUM_ORDERS", FilterName::MaxNumOrders),
    ("MAX_NUM_ALGO_ORDERS", FilterName::MaxNumAlgoOrders),
    ("MAX_NUM_ICEBERG_ORDERS", FilterName::MaxNumIcebergOrders),
    ("MAX_POSITION", FilterName::MaxPosition),
    ("TRAILING_DELTA", FilterName::TrailingDelta),
    ("MAX_NUM_ORDER_AMENDS", FilterName::MaxNumOrderAmends),
    ("MAX_NUM_ORDER_LISTS", FilterName::MaxNumOrderLists),
    ("EXCHANGE_MAX_NUM_ORDERS", FilterName::ExchangeMaxNumOrders),
    (
        "EXCHANGE_MAX_NUM_ALGO_ORDERS",
        FilterName::ExchangeMaxNumAlgoOrders,
    ),
    (
        "EXCHANGE_MAX_NUM_ICEBERG_ORDERS",
        FilterName::ExchangeMaxNumIcebergOrders,
    ),
    (
        "EXCHANGE_MAX_NUM_ORDER_LISTS",
        FilterName::ExchangeMaxNumOrderLists,
    ),
    ("MAX_ASSET", FilterName::MaxAsset),
];

impl FilterName {
    /// The filter `name` spells, exactly as the venue spells it.
    #[must_use]
    pub fn parse(name: &str) -> Self {
        FILTER_NAMES
            .iter()
            .find(|(spelling, _)| *spelling == name)
            .map_or(Self::Unrecognised, |&(_, filter)| filter)
    }

    /// The venue's spelling, or `None` for [`FilterName::Unrecognised`].
    #[must_use]
    pub fn as_str(self) -> Option<&'static str> {
        FILTER_NAMES
            .iter()
            .find(|(_, filter)| *filter == self)
            .map(|&(spelling, _)| spelling)
    }

    /// The filter a refusal's message names when it is a *"Filter failure:
    /// NAME"* (errors.md:425-446), and `None` for any other message. A name
    /// the documentation does not have is still a filter failure, of
    /// [`FilterName::Unrecognised`].
    ///
    /// The one reading of that message, for the order classifier and for
    /// `venue-check`'s probe alike.
    #[must_use]
    pub fn in_message(msg: &str) -> Option<Self> {
        msg.strip_prefix(FILTER_FAILURE).map(Self::parse)
    }
}

/// Why an answer settles nothing about a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WhyUnknown {
    /// A 5xx: *"the issue is on Binance's side"*, and *"the execution status
    /// is UNKNOWN and could have been a success"* (rest-api.md:116-118). The
    /// body's code and message, when it carried a Binance error, are kept, and
    /// decide nothing.
    ServerError {
        http_status: u16,
        said: Option<VenueError>,
    },
    /// A Binance error whose code does not say the request was refused: one
    /// that says the outcome is unknown, one that says nothing about it, or one
    /// this endpoint is not expected to send.
    Unsettled {
        http_status: u16,
        code: i64,
        msg: String,
    },
    /// *"Duplicate order sent."*, under `-2010` or `-1013`: the id is
    /// *"already in use"* (errors.md:375), which means an open order holds
    /// `id`, the id this order was sent under (rest-api.md:2153). The request
    /// was refused and the order may exist all the same, because the order
    /// holding the id may be this one, sent before: by an HTTP client that
    /// resends a request, say, or by a slip that submitted it twice. Or it is
    /// another order under the same id, which [`RunTag`] exists to prevent.
    /// Either way some order is open under a name of ours that should mean
    /// one order, and that needs a person.
    IdInUse {
        id: String,
        http_status: u16,
        code: i64,
        msg: String,
    },
    /// A body that is not the answer expected: a 2xx that is not the
    /// acknowledgement, or a 4xx that is not a Binance error.
    UnreadableBody { http_status: u16, detail: String },
    /// An answer naming the order `echoed`, when the request named `sent`: the
    /// order placed, or for a cancel the order cancelled. Whether anything
    /// was done to ours is unknown.
    OtherOrder { sent: String, echoed: String },
    /// A status outside 2xx, 4xx and 5xx, or a 409. The venue sends 409 when a
    /// request *"partially failed but also partially succeeded"*
    /// (web-socket-api.md:317), for a cancel-replace (rest-api.md:113), which
    /// this system never sends; what succeeded may have been what was asked.
    UnexpectedStatus { http_status: u16 },
    /// [`Attempt::TimedOut`].
    TimedOut,
    /// [`Attempt::Failed`], with the caller's description.
    Interrupted(String),
}

impl WhyUnknown {
    fn unsettled(http_status: u16, error: VenueError) -> Self {
        Self::Unsettled {
            http_status,
            code: error.code,
            msg: error.msg,
        }
    }
}

impl fmt::Display for WhyUnknown {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServerError { http_status, said } => {
                write!(
                    f,
                    "HTTP {http_status}, an error on the venue's side, after which whether the \
                     request was carried out is unknown"
                )?;
                match said {
                    Some(said) => write!(f, " (code {}: {})", said.code, said.msg),
                    None => Ok(()),
                }
            }
            Self::Unsettled {
                http_status,
                code,
                msg,
            } => write!(
                f,
                "HTTP {http_status} with code {code} ({msg}), which does not say the request \
                 was refused"
            ),
            Self::IdInUse {
                id,
                http_status,
                code,
                msg,
            } => write!(
                f,
                "HTTP {http_status} with code {code} ({msg}): an open order already holds \
                 {id}, the id this order was sent under, and it may be this order"
            ),
            Self::UnreadableBody {
                http_status,
                detail,
            } => write!(
                f,
                "HTTP {http_status} with a body that is not the answer expected: {detail}"
            ),
            Self::OtherOrder { sent, echoed } => write!(
                f,
                "the venue's answer names {echoed}, and this request was sent as {sent}"
            ),
            Self::UnexpectedStatus { http_status } => write!(
                f,
                "HTTP {http_status}, which this endpoint is not expected to answer with"
            ),
            Self::TimedOut => f.write_str("no answer before the client stopped waiting"),
            Self::Interrupted(why) => {
                write!(f, "the request failed after it may have left: {why}")
            }
        }
    }
}

/// A Binance error body, `{"code": -1121, "msg": "Invalid symbol."}`
/// (errors.md:3-10). Both fields are required; anything else in the body is
/// ignored.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct VenueError {
    pub code: i64,
    pub msg: String,
}

/// An [`Attempt`], read as far as every endpoint reads it alike.
enum Heard<'a> {
    /// A 2xx, whose body is the endpoint's to read.
    Success {
        http_status: u16,
        body: &'a [u8],
    },
    /// A 4xx carrying a Binance error, for the endpoint to read by its code.
    Error {
        http_status: u16,
        error: VenueError,
    },
    Unknown(WhyUnknown),
    NotSent(String),
}

fn heard(attempt: Attempt<'_>) -> Heard<'_> {
    let (http_status, body) = match attempt {
        Attempt::Answered { status, body } => (status, body),
        Attempt::NotConnected(why) => return Heard::NotSent(why.to_owned()),
        Attempt::TimedOut => return Heard::Unknown(WhyUnknown::TimedOut),
        Attempt::Failed(why) => return Heard::Unknown(WhyUnknown::Interrupted(why.to_owned())),
    };
    match http_status {
        200..=299 => Heard::Success { http_status, body },
        409 => Heard::Unknown(WhyUnknown::UnexpectedStatus { http_status }),
        400..=499 => match serde_json::from_slice::<VenueError>(body) {
            Ok(error) => Heard::Error { http_status, error },
            Err(e) => Heard::Unknown(WhyUnknown::UnreadableBody {
                http_status,
                detail: e.to_string(),
            }),
        },
        500..=599 => Heard::Unknown(WhyUnknown::ServerError {
            http_status,
            said: serde_json::from_slice(body).ok(),
        }),
        _ => Heard::Unknown(WhyUnknown::UnexpectedStatus { http_status }),
    }
}

/// The fields of an acknowledgement this module reads
/// (rest-api.md:2203-2212). The fuller responses begin with the same fields
/// (rest-api.md:2214-2292), so a venue that ignored `ACK` would still be read
/// correctly, and the fills such a response carries would still be left to the
/// stream.
#[derive(serde::Deserialize)]
struct Ack {
    #[serde(rename = "orderId")]
    order_id: u64,
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    #[serde(rename = "transactTime")]
    transact_time: i64,
}

/// The fields of a cancel's answer this module reads (rest-api.md:2408-2427).
/// `origClientOrderId` is the order cancelled, and `clientOrderId` beside it is
/// the cancel's own id (web-socket-api.md:4200-4203), which is not ours.
#[derive(serde::Deserialize)]
struct CancelAck {
    #[serde(rename = "orderId")]
    order_id: u64,
    #[serde(rename = "origClientOrderId")]
    orig_client_order_id: String,
}

/// A refusal that a signed request to this endpoint can meet, or `None` when
/// the code is not one.
///
/// Every code here but the request issues can meet either request, whatever
/// it asks for. Of the request issues, `own_issues` are the ones about
/// parameters only this request sends, [`NEW_ORDER_ISSUES`] or
/// [`CANCEL_ISSUES`], so that a code about a parameter it never sent is left
/// unknown.
fn request_refusal(http_status: u16, code: i64, own_issues: &[i64]) -> Option<RefusalCause> {
    match code {
        -1021 => Some(RefusalCause::Timestamp),
        -1022 | -2014 | -2015 => Some(RefusalCause::Credentials),
        // A rate limit, under either status that says one turned the request
        // away. Requests *"fail"* with a 429 when a request rate limit or the
        // unfilled order count is broken (web-socket-api.md:398;
        // rest-api.md:114, 153), and with a 418 from an address auto-banned
        // for carrying on after 429s (web-socket-api.md:523; rest-api.md:115).
        // The documentation shows -1003 with a 418 (web-socket-api.md:551-554)
        // and -1015 with a 429 (5065-5068; faqs/order_count_decrement.md:57),
        // and neither other pairing. -1015 under a 418 is refused for what the
        // 418 itself means, a ban the request ran into whatever it asked, and
        // -1003 under a 429 because -1003 is the request rate limit
        // (errors.md:22-24) that a 429 reports. Under any other status neither
        // code is the documented failure, so it settles nothing.
        -1003 | -1015 if matches!(http_status, 418 | 429) => Some(RefusalCause::RateLimited),
        code if REQUEST_ISSUES.contains(&code) || own_issues.contains(&code) => {
            Some(RefusalCause::Malformed)
        }
        _ => None,
    }
}

/// Whether a `-1013` or `-2010` message says an open order already holds the
/// id: *"Duplicate order sent."* (errors.md:375).
///
/// The one message not matched exactly. The documentation warns that messages
/// can vary (errors.md:3-4), and for every other message a near miss is still a
/// refusal, which is safe. For this one a near miss would be read as a refusal
/// of an order the venue holds, taking it out of the engine's books while it
/// fills. Under these two codes it is the only documented message that says
/// "duplicate" (errors.md:367-413), so the looser match claims nothing else.
fn names_a_duplicate(msg: &str) -> bool {
    msg.to_ascii_lowercase().contains("duplicate")
}

/// The cause a `-1013` or `-2010` message names, for every message but a
/// duplicate ([`names_a_duplicate`]), which is not a refusal of the order at
/// all and which [`NewOrder::classify`] reads before this.
///
/// Matched exactly as the documentation writes each message. One that differs
/// by a character is still a refusal, as [`RefusalCause::Declined`], with the
/// message kept; it just names no cause this module knows.
fn declined_because(msg: &str) -> RefusalCause {
    if let Some(filter) = FilterName::in_message(msg) {
        return RefusalCause::Filter(filter);
    }
    match msg {
        INSUFFICIENT_BALANCE => RefusalCause::InsufficientBalance,
        _ => RefusalCause::Declined,
    }
}

/// How every filter failure's message begins (errors.md:428-446).
const FILTER_FAILURE: &str = "Filter failure: ";

/// errors.md:377, and the message of the venue's own example of a failed
/// order (web-socket-api.md:232-235).
const INSUFFICIENT_BALANCE: &str = "Account has insufficient balance for requested action.";

/// errors.md:374.
const UNKNOWN_ORDER: &str = "Unknown order sent.";

/// The `-11xx` *"Request issues"* (errors.md:71) that either request to
/// `/api/v3/order` can meet, a new order or a cancel: the ones about any
/// parameter, or about a parameter both send.
///
/// A request issue about a parameter only one of them sends is in that one's
/// own list, [`NEW_ORDER_ISSUES`] or [`CANCEL_ISSUES`], and the other leaves it
/// unknown. The rest of the range belongs to other endpoints and protocols
/// (intervals, listen keys, order lists, SBE, FIX) or to parameters this
/// module never sends, and one of those here would mean its model of the venue
/// is wrong, so it is left unknown. So is `-1112`, *"No orders on book for
/// symbol."* (errors.md:112-113): whether this endpoint sends it is not
/// documented.
const REQUEST_ISSUES: [i64; 12] = [
    -1100, // ILLEGAL_CHARS, errors.md:72-74
    -1101, // TOO_MANY_PARAMETERS, errors.md:76-79
    -1102, // MANDATORY_PARAM_EMPTY_OR_MALFORMED, errors.md:81-87
    -1103, // UNKNOWN_PARAM, errors.md:89-91
    -1104, // UNREAD_PARAMETERS, errors.md:93-95
    -1105, // PARAM_EMPTY, errors.md:97-99
    -1106, // PARAM_NOT_REQUIRED, errors.md:101-104
    -1108, // PARAM_OVERFLOW, errors.md:106-107
    -1111, // BAD_PRECISION, errors.md:109-110
    -1121, // BAD_SYMBOL, errors.md:136-137
    -1128, // OPTIONAL_PARAMS_BAD_COMBO, errors.md:149-154
    -1130, // INVALID_PARAMETER, errors.md:156-158
];

/// The request issues about the parameters only a new order sends: its
/// `timeInForce`, `type`, `side` and `newClientOrderId`. A cancel sends none
/// of them, so on a cancel these are unknown.
const NEW_ORDER_ISSUES: [i64; 5] = [
    -1114, // TIF_NOT_REQUIRED, errors.md:115-116
    -1115, // INVALID_TIF, errors.md:118-119
    -1116, // INVALID_ORDER_TYPE, errors.md:121-122
    -1117, // INVALID_SIDE, errors.md:124-125
    -1118, // EMPTY_NEW_CL_ORD_ID, errors.md:127-128
];

/// The request issue about the parameter only a cancel sends, its
/// `origClientOrderId`. A new order does not send it, so on a new order this
/// is unknown.
const CANCEL_ISSUES: [i64; 1] = [
    -1119, // EMPTY_ORG_CL_ORD_ID, errors.md:130-131
];

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
        // market, with `Gtc`, which the venue does not ask a market order for
        // (rest-api.md:2168-2173).
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
        // this encoder writes spot orders.
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

    /// Our order 7: a market buy, the shape `MaCrossover` sends.
    fn ours() -> NewOrder {
        encode(&market(Side::Buy, "0.001", TimeInForce::Gtc))
    }

    fn answered(status: u16, body: &str) -> Attempt<'_> {
        Attempt::Answered {
            status,
            body: body.as_bytes(),
        }
    }

    /// A Binance error body (errors.md:3-10).
    fn venue_error(code: i64, msg: &str) -> String {
        serde_json::json!({ "code": code, "msg": msg }).to_string()
    }

    /// `document` with `field` replaced by `value`.
    fn with_field(document: &str, field: &str, value: &str) -> String {
        let mut json: serde_json::Value =
            serde_json::from_str(document).expect("the example is JSON");
        json[field] = serde_json::Value::from(value);
        json.to_string()
    }

    /// The documented acknowledgement (rest-api.md:2205-2211), its comment
    /// removed and nothing else changed. It acknowledges somebody else's
    /// order; `with_field` makes it ours.
    const DOCUMENTED_ACK: &str = r#"{
        "symbol": "BTCUSDT",
        "orderId": 28,
        "orderListId": -1,
        "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP",
        "transactTime": 1507725176595
    }"#;

    /// The documented `FULL` response (rest-api.md:2238-2292), its comment and
    /// all but its first fill removed.
    const DOCUMENTED_FULL: &str = r#"{
        "symbol": "BTCUSDT",
        "orderId": 28,
        "orderListId": -1,
        "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP",
        "transactTime": 1507725176595,
        "price": "0.00000000",
        "origQty": "10.00000000",
        "executedQty": "10.00000000",
        "origQuoteOrderQty": "0.000000",
        "cummulativeQuoteQty": "10.00000000",
        "status": "FILLED",
        "timeInForce": "GTC",
        "type": "MARKET",
        "side": "SELL",
        "workingTime": 1507725176595,
        "selfTradePreventionMode": "NONE",
        "fills": [
            {
                "price": "4000.00000000",
                "qty": "1.00000000",
                "commission": "4.00000000",
                "commissionAsset": "USDT",
                "tradeId": 56
            }
        ]
    }"#;

    /// Which way an answer to a venue error went: what the documentation
    /// decides, without the detail around it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Class {
        Refused(RefusalCause),
        Unknown,
        /// A cancel's only: the venue has no open order by that id.
        NotOpen,
    }

    fn placement_class(placement: &Placement) -> Class {
        match placement {
            Placement::Refused(refusal) => Class::Refused(refusal.cause),
            Placement::Unknown { .. } => Class::Unknown,
            other => panic!("a venue error cannot be {other:?}"),
        }
    }

    fn cancel_class(reply: &CancelReply) -> Class {
        match reply {
            CancelReply::Refused(refusal) => Class::Refused(refusal.cause),
            CancelReply::NotOpen { .. } => Class::NotOpen,
            CancelReply::Unknown { .. } => Class::Unknown,
            other => panic!("a venue error cannot be {other:?}"),
        }
    }

    /// A cancel of our order 7.
    fn our_cancel() -> CancelOrder {
        CancelOrder::encode(ClientOrderId(7), &run(), &btcusdt()).expect("encodes")
    }

    /// The documented answer to a cancel (rest-api.md:2410-2427), its comment
    /// removed and nothing else changed. It answers for somebody else's order,
    /// `myOrder1`; `with_field` makes it ours.
    const DOCUMENTED_CANCEL: &str = r#"{
        "symbol": "LTCBTC",
        "origClientOrderId": "myOrder1",
        "orderId": 4,
        "orderListId": -1,
        "clientOrderId": "cancelMyOrder1",
        "transactTime": 1684804350068,
        "price": "2.00000000",
        "origQty": "1.00000000",
        "executedQty": "0.00000000",
        "origQuoteOrderQty": "0.000000",
        "cummulativeQuoteQty": "0.00000000",
        "status": "CANCELED",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "selfTradePreventionMode": "NONE"
    }"#;

    #[test]
    fn the_documented_acknowledgement_of_our_order_is_placed() {
        let placed = Placement::Placed {
            order_id: VenueOrderId("28".to_owned()),
            client_order_id: "q-r1-7".to_owned(),
            transact_time_ms: 1_507_725_176_595,
        };
        let ack = with_field(DOCUMENTED_ACK, "clientOrderId", "q-r1-7");
        assert_eq!(ours().classify(answered(200, &ack)), placed);
        assert_eq!(placed.reject_reason(), None);
        // What a venue that ignored `ACK` would send: still placed, and its
        // fill still left to the stream.
        let full = with_field(DOCUMENTED_FULL, "clientOrderId", "q-r1-7");
        assert_eq!(ours().classify(answered(200, &full)), placed);
    }

    #[test]
    fn an_acknowledgement_of_another_order_settles_nothing() {
        // The documented example as it stands, naming an id the venue made up.
        assert_eq!(
            ours().classify(answered(200, DOCUMENTED_ACK)),
            Placement::Unknown {
                why: WhyUnknown::OtherOrder {
                    sent: "q-r1-7".to_owned(),
                    echoed: "6gCrw2kRUAF9CvJDGP16IP".to_owned(),
                }
            }
        );
    }

    #[test]
    fn the_venue_may_have_placed_it_so_these_are_never_refused() {
        let insufficient = venue_error(
            -2010,
            "Account has insufficient balance for requested action.",
        );
        let timeout = venue_error(
            -1007,
            "Timeout waiting for response from backend server. Send status unknown; execution \
             status unknown.",
        );
        let unexpected = venue_error(
            -1006,
            "An unexpected response was received from the message bus. Execution status unknown.",
        );
        let unknown = venue_error(
            -1000,
            "An unknown error occurred while processing the request.",
        );
        let no_order_id =
            r#"{"symbol":"BTCUSDT","clientOrderId":"q-r1-7","transactTime":1507725176595}"#;
        let cases = [
            ("a 500 with no body", answered(500, "")),
            ("a 500 carrying a code", answered(500, &unknown)),
            ("a 502 from something in between", answered(502, "<html>")),
            // A 5xx whose body reads exactly like a refusal is still a 5xx
            // (rest-api.md:116-118).
            ("a 503 carrying a refusal", answered(503, &insufficient)),
            ("a 504 carrying -1007", answered(504, &timeout)),
            // And -1007 and -1006 say unknown under whatever status they come
            // (errors.md:27-31).
            ("a 400 carrying -1007", answered(400, &timeout)),
            ("a 400 carrying -1006", answered(400, &unexpected)),
            // A success the order may stand behind, whatever the body is.
            ("a 200 with no body", answered(200, "")),
            (
                "a 200 from a captive portal",
                answered(200, "<html>Sign in"),
            ),
            ("a 200 without an order id", answered(200, no_order_id)),
            ("a 200 carrying a refusal", answered(200, &insufficient)),
            ("a timeout", Attempt::TimedOut),
            ("a reset after sending", Attempt::Failed("connection reset")),
        ];
        for (what, attempt) in cases {
            let placement = ours().classify(attempt);
            assert!(
                matches!(placement, Placement::Unknown { .. }),
                "{what}: {placement:?}"
            );
            assert_eq!(placement.reject_reason(), None, "{what}");
        }
    }

    #[test]
    fn an_id_an_open_order_holds_may_be_this_order_so_it_is_never_refused() {
        // Both codes refuse the request, and the message says why: an open
        // order already holds its id (errors.md:375; rest-api.md:2153). That
        // order may be this one, sent before, so this one stays in doubt
        // rather than leaving the books while the venue fills it.
        for code in [-2010, -1013] {
            let duplicate = venue_error(code, "Duplicate order sent.");
            let placement = ours().classify(answered(400, &duplicate));
            assert_eq!(
                placement,
                Placement::Unknown {
                    why: WhyUnknown::IdInUse {
                        id: "q-r1-7".to_owned(),
                        http_status: 400,
                        code,
                        msg: "Duplicate order sent.".to_owned(),
                    }
                },
                "{code}"
            );
            assert_eq!(placement.reject_reason(), None, "{code}");
        }
    }

    #[test]
    fn a_duplicate_reworded_is_still_a_duplicate() {
        // Messages can vary (errors.md:3-4). For every other message a near miss
        // is still a refusal, which is safe; for this one a near miss would take
        // an order the venue holds out of the engine's books.
        for msg in [
            "Duplicate order sent",
            "duplicate order sent.",
            "Duplicate order sent. (clOrdId already in use)",
        ] {
            for code in [-2010, -1013] {
                let placement = ours().classify(answered(400, &venue_error(code, msg)));
                assert!(
                    matches!(
                        placement,
                        Placement::Unknown {
                            why: WhyUnknown::IdInUse { .. }
                        }
                    ),
                    "{code} {msg:?}: {placement:?}"
                );
            }
        }
    }

    #[test]
    fn a_client_error_that_is_not_a_binance_error_settles_nothing() {
        // A firewall's page (rest-api.md:112), a bare rate-limit reply, and
        // JSON that is not the venue's error shape (errors.md:3-10): none of
        // them is the venue's API naming a rule.
        for (status, body) in [
            (400, ""),
            (403, "<html>Request blocked</html>"),
            (429, ""),
            (418, "banned"),
            (400, r#"{"msg":"a message and no code"}"#),
            (400, r#"{"code":"-2010","msg":"a code that is text"}"#),
            (400, r#"{"code":-2010}"#),
        ] {
            let placement = ours().classify(answered(status, body));
            assert!(
                matches!(
                    &placement,
                    Placement::Unknown {
                        why: WhyUnknown::UnreadableBody { http_status, .. }
                    } if *http_status == status
                ),
                "{status} {body}: {placement:?}"
            );
        }
    }

    #[test]
    fn a_status_the_endpoint_does_not_answer_with_settles_nothing() {
        // 409 is how the venue says a request partly succeeded
        // (web-socket-api.md:317; rest-api.md:113), so a body that reads like
        // a refusal does not make it one.
        let refusal = venue_error(
            -2010,
            "Account has insufficient balance for requested action.",
        );
        for status in [409, 100, 302, 600] {
            assert_eq!(
                ours().classify(answered(status, &refusal)),
                Placement::Unknown {
                    why: WhyUnknown::UnexpectedStatus {
                        http_status: status
                    }
                },
                "{status}"
            );
        }
    }

    /// A request issue, which every `-11xx` row of the code tables expects.
    const MALFORMED: Class = Class::Refused(RefusalCause::Malformed);

    /// A row of a code table: the code and message as the catalogue writes
    /// them, the status they come under, the class the documentation puts them
    /// in, and the lines that say so.
    type Row = (i64, &'static str, u16, Class, &'static str);

    /// Every code `POST /api/v3/order` can meet.
    ///
    /// The documentation does not say which codes each endpoint returns, so
    /// this table is the reading of its catalogue that the module rests on.
    /// Codes for other endpoints and protocols, and for parameters never sent,
    /// are left out of it, and are in
    /// `a_code_a_new_order_is_not_expected_to_meet_settles_nothing`.
    const PLACING: &[Row] = &[
        (
            -1000,
            "An unknown error occurred while processing the request.",
            400,
            Class::Unknown,
            "errors.md:13-14",
        ),
        (
            -1001,
            "Internal error; unable to process your request. Please try again.",
            400,
            Class::Unknown,
            "errors.md:16-17",
        ),
        (
            -1002,
            "You are not authorized to execute this request.",
            401,
            Class::Unknown,
            "errors.md:19-20, outside the codes tied to a request not carried out",
        ),
        (
            -1003,
            "Too many requests queued.",
            429,
            Class::Refused(RefusalCause::RateLimited),
            "errors.md:22-23; rest-api.md:114, 153",
        ),
        (
            -1003,
            "Way too much request weight used; IP banned until %s. Please use WebSocket \
                 Streams for live updates to avoid bans.",
            418,
            Class::Refused(RefusalCause::RateLimited),
            "errors.md:25; rest-api.md:115, 159; web-socket-api.md:551-554",
        ),
        (
            -1003,
            "Too much request weight used; current limit is %s request weight per %s. \
                 Please use WebSocket Streams for live updates to avoid polling the API.",
            400,
            Class::Unknown,
            "errors.md:24, under a status rest-api.md:114-115 does not give it",
        ),
        (
            -1006,
            "An unexpected response was received from the message bus. Execution status \
                 unknown.",
            400,
            Class::Unknown,
            "errors.md:27-28",
        ),
        (
            -1007,
            "Timeout waiting for response from backend server. Send status unknown; \
                 execution status unknown.",
            400,
            Class::Unknown,
            "errors.md:30-31; rest-api.md:102-104",
        ),
        (
            -1008,
            "Server is currently overloaded with other requests. Please try again in a few \
                 minutes.",
            400,
            Class::Unknown,
            "errors.md:33-34",
        ),
        (
            -1013,
            "Filter failure: NOTIONAL",
            400,
            Class::Refused(RefusalCause::Filter(FilterName::Notional)),
            "errors.md:36-38, 433",
        ),
        (
            -1013,
            "Filter failure: MIN_NOTIONAL",
            400,
            Class::Refused(RefusalCause::Filter(FilterName::MinNotional)),
            "errors.md:36-38, 432",
        ),
        (
            -1013,
            "Filter failure: LOT_SIZE",
            400,
            Class::Refused(RefusalCause::Filter(FilterName::LotSize)),
            "errors.md:36-38, 431",
        ),
        (
            -1013,
            "Market is closed.",
            400,
            Class::Refused(RefusalCause::Declined),
            "errors.md:36-38, 376",
        ),
        (
            -1013,
            "Account has insufficient balance for requested action.",
            400,
            Class::Refused(RefusalCause::InsufficientBalance),
            "errors.md:36-38, 377",
        ),
        (
            -1013,
            "Duplicate order sent.",
            400,
            Class::Unknown,
            "errors.md:36-38, 375; rest-api.md:2153, held by an order that may be this one",
        ),
        (
            -1014,
            "Unsupported order combination.",
            400,
            Class::Unknown,
            "errors.md:40-41, outside the codes tied to a request not carried out",
        ),
        (
            -1015,
            "Too many new orders; current limit is %s orders per %s.",
            429,
            Class::Refused(RefusalCause::RateLimited),
            "errors.md:43-45; faqs/order_count_decrement.md:57; web-socket-api.md:5065-5068",
        ),
        (
            -1016,
            "This service is no longer available.",
            400,
            Class::Unknown,
            "errors.md:47-48",
        ),
        (
            -1020,
            "This operation is not supported.",
            400,
            Class::Unknown,
            "errors.md:50-51",
        ),
        (
            -1021,
            "Timestamp for this request is outside of the recvWindow.",
            400,
            Class::Refused(RefusalCause::Timestamp),
            "errors.md:53-54; rest-api.md:225-241",
        ),
        (
            -1021,
            "Timestamp for this request was 1000ms ahead of the server's time.",
            400,
            Class::Refused(RefusalCause::Timestamp),
            "errors.md:55; rest-api.md:225-241",
        ),
        (
            -1022,
            "Signature for this request is not valid.",
            400,
            Class::Refused(RefusalCause::Credentials),
            "errors.md:57-58",
        ),
        (
            -1100,
            "Illegal characters found in parameter '%s'; legal range is '%s'.",
            400,
            MALFORMED,
            "errors.md:72-74",
        ),
        (
            -1101,
            "Duplicate values for a parameter detected.",
            400,
            MALFORMED,
            "errors.md:76-79",
        ),
        (
            -1102,
            "Mandatory parameter '%s' was not sent, was empty/null, or malformed.",
            400,
            MALFORMED,
            "errors.md:81-87",
        ),
        (
            -1103,
            "An unknown parameter was sent.",
            400,
            MALFORMED,
            "errors.md:89-91",
        ),
        (
            -1104,
            "Not all sent parameters were read; read '%s' parameter(s) but was sent '%s'.",
            400,
            MALFORMED,
            "errors.md:93-95",
        ),
        (
            -1105,
            "Parameter '%s' was empty.",
            400,
            MALFORMED,
            "errors.md:97-99",
        ),
        (
            -1106,
            "Parameter '%s' sent when not required.",
            400,
            MALFORMED,
            "errors.md:101-104",
        ),
        (
            -1108,
            "Parameter '%s' overflowed.",
            400,
            MALFORMED,
            "errors.md:106-107",
        ),
        (
            -1111,
            "Parameter '%s' has too much precision.",
            400,
            MALFORMED,
            "errors.md:109-110",
        ),
        (
            -1114,
            "TimeInForce parameter sent when not required.",
            400,
            MALFORMED,
            "errors.md:115-116",
        ),
        (
            -1115,
            "Invalid timeInForce.",
            400,
            MALFORMED,
            "errors.md:118-119",
        ),
        (
            -1116,
            "Invalid orderType.",
            400,
            MALFORMED,
            "errors.md:121-122",
        ),
        (-1117, "Invalid side.", 400, MALFORMED, "errors.md:124-125"),
        (
            -1118,
            "New client order ID was empty.",
            400,
            MALFORMED,
            "errors.md:127-128",
        ),
        (
            -1121,
            "Invalid symbol.",
            400,
            MALFORMED,
            "errors.md:136-137",
        ),
        (
            -1128,
            "Combination of optional parameters invalid.",
            400,
            MALFORMED,
            "errors.md:149-154",
        ),
        (
            -1130,
            "Data sent for parameter '%s' is not valid.",
            400,
            MALFORMED,
            "errors.md:156-158",
        ),
        (
            -2010,
            "Account has insufficient balance for requested action.",
            400,
            Class::Refused(RefusalCause::InsufficientBalance),
            "errors.md:329-330, 367-368, 377; web-socket-api.md:232-235",
        ),
        (
            -2010,
            "Duplicate order sent.",
            400,
            Class::Unknown,
            "errors.md:367-368, 375; rest-api.md:2153, held by an order that may be this one",
        ),
        (
            -2010,
            "Market orders are not supported for this symbol.",
            400,
            Class::Refused(RefusalCause::Declined),
            "errors.md:367-368, 378",
        ),
        (
            -2010,
            "Price * QTY is zero or less.",
            400,
            Class::Refused(RefusalCause::Declined),
            "errors.md:367-368, 385",
        ),
        (
            -2010,
            "This account may not place or cancel orders.",
            400,
            Class::Refused(RefusalCause::Declined),
            "errors.md:367-368, 388",
        ),
        (
            -2014,
            "API-key format invalid.",
            401,
            Class::Refused(RefusalCause::Credentials),
            "errors.md:338-339",
        ),
        (
            -2015,
            "Invalid API-key, IP, or permissions for action.",
            401,
            Class::Refused(RefusalCause::Credentials),
            "errors.md:341-342",
        ),
    ];

    #[test]
    fn every_code_a_new_order_can_meet_is_classified_as_documented() {
        for &(code, msg, status, class, documented) in PLACING {
            let placement = ours().classify(answered(status, &venue_error(code, msg)));
            assert_eq!(
                placement_class(&placement),
                class,
                "{code} {msg:?} under {status}: {documented}"
            );
            if let Placement::Refused(refusal) = placement {
                // Kept verbatim, for the journal and for criterion B.
                assert_eq!(
                    (refusal.http_status, refusal.code, refusal.msg.as_str()),
                    (status, code, msg)
                );
            }
        }
    }

    #[test]
    fn a_code_a_new_order_is_not_expected_to_meet_settles_nothing() {
        // Each of these means the venue did something this module's model of
        // it does not cover, so none of them may take an order off the books.
        for (code, msg) in [
            // A cancel's code (errors.md:332-333), and a query's (335-336).
            (-2011, "Unknown order sent."),
            (-2013, "Order does not exist."),
            // A listen key's (errors.md:142-143), and a parameter's this
            // module never sends (errors.md:171-172).
            (-1125, "This listenKey does not exist."),
            (
                -1145,
                "`cancelRestrictions` has to be either `ONLY_NEW` or `ONLY_PARTIALLY_FILLED`.",
            ),
            // The parameter only a cancel sends (errors.md:130-131).
            (-1119, "Original client order ID was empty."),
            // Not documented for this endpoint (errors.md:112-113).
            (-1112, "No orders on book for symbol."),
            // Named only in a heading (errors.md:367): the message is a
            // refusal's, and the code, which decides, is not one.
            (
                -1010,
                "Account has insufficient balance for requested action.",
            ),
            (0, ""),
            (-9999, "A code from after this was written."),
        ] {
            assert_eq!(
                ours().classify(answered(400, &venue_error(code, msg))),
                Placement::Unknown {
                    why: WhyUnknown::Unsettled {
                        http_status: 400,
                        code,
                        msg: msg.to_owned(),
                    }
                },
                "{code}"
            );
        }
    }

    #[test]
    fn filter_failures_name_the_filter_and_an_unknown_one_says_so() {
        // Every filter filters.md documents, spelled as it spells them, with
        // the line each is defined at. Written out here, not read from the
        // module's table, which would only be checking the table against
        // itself.
        let documented = [
            ("PRICE_FILTER", FilterName::PriceFilter, 37),
            ("PERCENT_PRICE", FilterName::PercentPrice, 60),
            ("PERCENT_PRICE_BY_SIDE", FilterName::PercentPriceBySide, 81),
            ("LOT_SIZE", FilterName::LotSize, 111),
            ("MIN_NOTIONAL", FilterName::MinNotional, 134),
            ("NOTIONAL", FilterName::Notional, 157),
            ("ICEBERG_PARTS", FilterName::IcebergParts, 183),
            ("MARKET_LOT_SIZE", FilterName::MarketLotSize, 194),
            ("MAX_NUM_ORDERS", FilterName::MaxNumOrders, 217),
            ("MAX_NUM_ALGO_ORDERS", FilterName::MaxNumAlgoOrders, 229),
            (
                "MAX_NUM_ICEBERG_ORDERS",
                FilterName::MaxNumIcebergOrders,
                241,
            ),
            ("MAX_POSITION", FilterName::MaxPosition, 253),
            ("TRAILING_DELTA", FilterName::TrailingDelta, 272),
            ("MAX_NUM_ORDER_AMENDS", FilterName::MaxNumOrderAmends, 301),
            ("MAX_NUM_ORDER_LISTS", FilterName::MaxNumOrderLists, 316),
            (
                "EXCHANGE_MAX_NUM_ORDERS",
                FilterName::ExchangeMaxNumOrders,
                331,
            ),
            (
                "EXCHANGE_MAX_NUM_ALGO_ORDERS",
                FilterName::ExchangeMaxNumAlgoOrders,
                343,
            ),
            (
                "EXCHANGE_MAX_NUM_ICEBERG_ORDERS",
                FilterName::ExchangeMaxNumIcebergOrders,
                355,
            ),
            (
                "EXCHANGE_MAX_NUM_ORDER_LISTS",
                FilterName::ExchangeMaxNumOrderLists,
                366,
            ),
            ("MAX_ASSET", FilterName::MaxAsset, 381),
        ];
        for (name, filter, line) in documented {
            assert_eq!(FilterName::parse(name), filter, "filters.md:{line}");
            assert_eq!(filter.as_str(), Some(name), "filters.md:{line}");
            let message = format!("Filter failure: {name}");
            let refused = ours().classify(answered(400, &venue_error(-1013, &message)));
            assert_eq!(
                placement_class(&refused),
                Class::Refused(RefusalCause::Filter(filter)),
                "filters.md:{line}"
            );
            assert_eq!(refused.reject_reason(), Some(RejectReason::Malformed));
        }
        // Twenty names for twenty filters: no two share one.
        for (i, (_, a, _)) in documented.iter().enumerate() {
            for (_, b, _) in &documented[i + 1..] {
                assert_ne!(a, b);
            }
        }

        // A name the documentation does not have, and near misses of one it
        // does, are none of them.
        for name in [
            "SOMETHING_NEW",
            "notional",
            "NOTIONAL ",
            " NOTIONAL",
            "MIN-NOTIONAL",
            "",
            "Filter failure: NOTIONAL",
        ] {
            assert_eq!(
                FilterName::parse(name),
                FilterName::Unrecognised,
                "{name:?}"
            );
        }
        assert_eq!(FilterName::Unrecognised.as_str(), None);
        // Still a refusal, because the code says so; only the filter is
        // unknown, and the message names it.
        let unheard_of = venue_error(-1013, "Filter failure: SOMETHING_NEW");
        assert_eq!(
            placement_class(&ours().classify(answered(400, &unheard_of))),
            Class::Refused(RefusalCause::Filter(FilterName::Unrecognised))
        );
    }

    #[test]
    fn only_a_filter_failure_names_a_filter() {
        // The prefix decides, exactly as errors.md:428-446 writes it; what
        // follows it is the filter, known or not.
        assert_eq!(
            FilterName::in_message("Filter failure: NOTIONAL"),
            Some(FilterName::Notional)
        );
        assert_eq!(
            FilterName::in_message("Filter failure: SOMETHING_NEW"),
            Some(FilterName::Unrecognised)
        );
        // Every other message under the same codes (errors.md:374-413) names
        // no filter, and nor does a near miss of the prefix.
        for msg in [
            "Market is closed.",
            "Account has insufficient balance for requested action.",
            "Duplicate order sent.",
            "This account may not place or cancel orders.",
            "filter failure: NOTIONAL",
            "Filter failure NOTIONAL",
            "",
        ] {
            assert_eq!(FilterName::in_message(msg), None, "{msg:?}");
        }
    }

    #[test]
    fn each_refusal_gives_the_engine_a_reason_and_an_unknown_outcome_gives_it_none() {
        for (cause, reason) in [
            (
                RefusalCause::Filter(FilterName::Notional),
                RejectReason::Malformed,
            ),
            (
                RefusalCause::Filter(FilterName::Unrecognised),
                RejectReason::Malformed,
            ),
            (
                RefusalCause::InsufficientBalance,
                RejectReason::InsufficientFunds,
            ),
            (RefusalCause::Declined, RejectReason::Venue),
            (RefusalCause::Timestamp, RejectReason::Venue),
            (RefusalCause::Credentials, RejectReason::Venue),
            (RefusalCause::Malformed, RejectReason::Malformed),
            (RefusalCause::RateLimited, RejectReason::Venue),
        ] {
            assert_eq!(cause.reject_reason(), reason, "{cause:?}");
            let refused = Placement::Refused(Refusal {
                http_status: 400,
                code: -2010,
                msg: String::new(),
                cause,
            });
            assert_eq!(refused.reject_reason(), Some(reason), "{cause:?}");
        }
        // A request that never left is the venue unreachable. An unknown
        // outcome, and a placed order, are not rejections at all.
        let not_sent = Placement::NotSent {
            why: "dns error".to_owned(),
        };
        assert_eq!(not_sent.reject_reason(), Some(RejectReason::Venue));
        for why in [
            WhyUnknown::TimedOut,
            WhyUnknown::ServerError {
                http_status: 503,
                said: None,
            },
            WhyUnknown::IdInUse {
                id: "q-r1-7".to_owned(),
                http_status: 400,
                code: -2010,
                msg: "Duplicate order sent.".to_owned(),
            },
        ] {
            assert_eq!(Placement::Unknown { why }.reject_reason(), None);
        }
    }

    #[test]
    fn only_a_request_that_never_left_is_certainly_not_sent() {
        let order = ours();
        assert_eq!(
            order.classify(Attempt::NotConnected("dns error: no such host")),
            Placement::NotSent {
                why: "dns error: no such host".to_owned()
            }
        );
        assert_eq!(
            order.classify(Attempt::TimedOut),
            Placement::Unknown {
                why: WhyUnknown::TimedOut
            }
        );
        assert_eq!(
            order.classify(Attempt::Failed("connection reset")),
            Placement::Unknown {
                why: WhyUnknown::Interrupted("connection reset".to_owned())
            }
        );
        // And a cancel the same way: only one that never left left the order
        // certainly as it was.
        let cancel = our_cancel();
        assert_eq!(
            cancel.classify(Attempt::NotConnected("connection refused")),
            CancelReply::NotSent {
                why: "connection refused".to_owned()
            }
        );
        assert_eq!(
            cancel.classify(Attempt::TimedOut),
            CancelReply::Unknown {
                why: WhyUnknown::TimedOut
            }
        );
        assert_eq!(
            cancel.classify(Attempt::Failed("connection reset")),
            CancelReply::Unknown {
                why: WhyUnknown::Interrupted("connection reset".to_owned())
            }
        );
    }

    /// Every code `DELETE /api/v3/order` can meet, on the same reading of the
    /// catalogue as `PLACING`: codes for other endpoints and protocols, and
    /// for parameters never sent, are left out, and are in
    /// `a_code_a_cancel_is_not_expected_to_meet_settles_nothing`.
    const CANCELLING: &[Row] = &[
        (
            -1000,
            "An unknown error occurred while processing the request.",
            400,
            Class::Unknown,
            "errors.md:13-14",
        ),
        (
            -1001,
            "Internal error; unable to process your request. Please try again.",
            400,
            Class::Unknown,
            "errors.md:16-17",
        ),
        (
            -1002,
            "You are not authorized to execute this request.",
            401,
            Class::Unknown,
            "errors.md:19-20, outside the codes tied to a request not carried out",
        ),
        (
            -1003,
            "Too many requests queued.",
            429,
            Class::Refused(RefusalCause::RateLimited),
            "errors.md:22-23; rest-api.md:114, 153",
        ),
        (
            -1003,
            "Way too much request weight used; IP banned until %s. Please use WebSocket Streams \
             for live updates to avoid bans.",
            418,
            Class::Refused(RefusalCause::RateLimited),
            "errors.md:25; rest-api.md:115, 159; web-socket-api.md:551-554",
        ),
        (
            -1006,
            "An unexpected response was received from the message bus. Execution status unknown.",
            400,
            Class::Unknown,
            "errors.md:27-28",
        ),
        (
            -1007,
            "Timeout waiting for response from backend server. Send status unknown; execution \
             status unknown.",
            400,
            Class::Unknown,
            "errors.md:30-31; rest-api.md:102-104",
        ),
        (
            -1008,
            "Server is currently overloaded with other requests. Please try again in a few \
             minutes.",
            400,
            Class::Unknown,
            "errors.md:33-34",
        ),
        (
            -1013,
            "Market is closed.",
            400,
            Class::Refused(RefusalCause::Declined),
            "errors.md:36-37, 376; CHANGELOG.md:2703",
        ),
        (
            -1016,
            "This service is no longer available.",
            400,
            Class::Unknown,
            "errors.md:47-48",
        ),
        (
            -1020,
            "This operation is not supported.",
            400,
            Class::Unknown,
            "errors.md:50-51",
        ),
        (
            -1021,
            "Timestamp for this request is outside of the recvWindow.",
            400,
            Class::Refused(RefusalCause::Timestamp),
            "errors.md:53-54; rest-api.md:225-241",
        ),
        (
            -1022,
            "Signature for this request is not valid.",
            400,
            Class::Refused(RefusalCause::Credentials),
            "errors.md:57-58",
        ),
        (
            -1100,
            "Illegal characters found in parameter '%s'; legal range is '%s'.",
            400,
            MALFORMED,
            "errors.md:72-74",
        ),
        (
            -1101,
            "Duplicate values for a parameter detected.",
            400,
            MALFORMED,
            "errors.md:76-79",
        ),
        (
            -1102,
            "Mandatory parameter '%s' was not sent, was empty/null, or malformed.",
            400,
            MALFORMED,
            "errors.md:81-87",
        ),
        (
            -1103,
            "An unknown parameter was sent.",
            400,
            MALFORMED,
            "errors.md:89-91",
        ),
        (
            -1104,
            "Not all sent parameters were read; read '%s' parameter(s) but was sent '%s'.",
            400,
            MALFORMED,
            "errors.md:93-95",
        ),
        (
            -1105,
            "Parameter '%s' was empty.",
            400,
            MALFORMED,
            "errors.md:97-99",
        ),
        (
            -1106,
            "Parameter '%s' sent when not required.",
            400,
            MALFORMED,
            "errors.md:101-104",
        ),
        (
            -1108,
            "Parameter '%s' overflowed.",
            400,
            MALFORMED,
            "errors.md:106-107",
        ),
        (
            -1111,
            "Parameter '%s' has too much precision.",
            400,
            MALFORMED,
            "errors.md:109-110",
        ),
        (
            -1119,
            "Original client order ID was empty.",
            400,
            MALFORMED,
            "errors.md:130-131",
        ),
        (
            -1121,
            "Invalid symbol.",
            400,
            MALFORMED,
            "errors.md:136-137",
        ),
        (
            -1128,
            "Combination of optional parameters invalid.",
            400,
            MALFORMED,
            "errors.md:149-154",
        ),
        (
            -1130,
            "Data sent for parameter '%s' is not valid.",
            400,
            MALFORMED,
            "errors.md:156-158",
        ),
        (
            -2011,
            "Unknown order sent.",
            400,
            Class::NotOpen,
            "errors.md:332-333, 374; rest-api.md:2384",
        ),
        (
            -2011,
            "Order was not canceled due to cancel restrictions.",
            400,
            Class::Refused(RefusalCause::Declined),
            "errors.md:400; rest-api.md:2445-2450",
        ),
        (
            -2011,
            "This account may not place or cancel orders.",
            400,
            Class::Refused(RefusalCause::Declined),
            "errors.md:367-368, 388",
        ),
        (
            -2011,
            "Market is closed.",
            400,
            Class::Refused(RefusalCause::Declined),
            "errors.md:376; CHANGELOG.md:2703",
        ),
        (
            -2011,
            "Cancel order is invalid. Check origClOrdId and orderId.",
            400,
            Class::Refused(RefusalCause::Declined),
            "errors.md:391",
        ),
        (
            -2014,
            "API-key format invalid.",
            401,
            Class::Refused(RefusalCause::Credentials),
            "errors.md:338-339",
        ),
        (
            -2015,
            "Invalid API-key, IP, or permissions for action.",
            401,
            Class::Refused(RefusalCause::Credentials),
            "errors.md:341-342",
        ),
    ];

    #[test]
    fn every_code_a_cancel_can_meet_is_classified_as_documented() {
        for &(code, msg, status, class, documented) in CANCELLING {
            let reply = our_cancel().classify(answered(status, &venue_error(code, msg)));
            assert_eq!(
                cancel_class(&reply),
                class,
                "{code} {msg:?} under {status}: {documented}"
            );
            // Kept verbatim, refusal or notice, for the journal.
            if let CancelReply::Refused(Refusal {
                http_status,
                code: said_code,
                msg: said,
                ..
            })
            | CancelReply::NotOpen {
                http_status,
                code: said_code,
                msg: said,
            } = reply
            {
                assert_eq!((http_status, said_code, said.as_str()), (status, code, msg));
            }
        }
    }

    #[test]
    fn a_code_a_cancel_is_not_expected_to_meet_settles_nothing() {
        for (code, msg) in [
            // A new order's code (errors.md:329-330, 377): the message is a
            // refusal's, and on a cancel the code is not one.
            (
                -2010,
                "Account has insufficient balance for requested action.",
            ),
            // A query's (errors.md:335-336), and an archive's (347-348).
            (-2013, "Order does not exist."),
            (
                -2026,
                "Order was canceled or expired with no executed qty over 90 days ago and has \
                 been archived.",
            ),
            // A parameter this module never sends (errors.md:171-172).
            (
                -1145,
                "`cancelRestrictions` has to be either `ONLY_NEW` or `ONLY_PARTIALLY_FILLED`.",
            ),
            // Parameters only a new order sends (errors.md:115-128). A cancel
            // may send a `newClientOrderId` of its own (rest-api.md:2396), and
            // this one does not.
            (-1114, "TimeInForce parameter sent when not required."),
            (-1115, "Invalid timeInForce."),
            (-1116, "Invalid orderType."),
            (-1117, "Invalid side."),
            (-1118, "New client order ID was empty."),
            (0, ""),
            (-9999, "A code from after this was written."),
        ] {
            assert_eq!(
                our_cancel().classify(answered(400, &venue_error(code, msg))),
                CancelReply::Unknown {
                    why: WhyUnknown::Unsettled {
                        http_status: 400,
                        code,
                        msg: msg.to_owned(),
                    }
                },
                "{code}"
            );
        }
    }

    #[test]
    fn a_cancel_the_venue_took_is_acknowledged_and_never_a_cancelled() {
        let ours = with_field(DOCUMENTED_CANCEL, "origClientOrderId", "q-r1-7");
        assert_eq!(
            our_cancel().classify(answered(200, &ours)),
            CancelReply::Acknowledged {
                order_id: VenueOrderId("4".to_owned())
            }
        );
        // As documented, it answers for another order.
        assert_eq!(
            our_cancel().classify(answered(200, DOCUMENTED_CANCEL)),
            CancelReply::Unknown {
                why: WhyUnknown::OtherOrder {
                    sent: "q-r1-7".to_owned(),
                    echoed: "myOrder1".to_owned(),
                }
            }
        );
        // `clientOrderId` there is the cancel's own id, not the order's
        // (web-socket-api.md:4200-4203), so finding ours in it says nothing
        // about which order was cancelled.
        let crossed = with_field(DOCUMENTED_CANCEL, "clientOrderId", "q-r1-7");
        assert!(matches!(
            our_cancel().classify(answered(200, &crossed)),
            CancelReply::Unknown {
                why: WhyUnknown::OtherOrder { .. }
            }
        ));
    }

    #[test]
    fn a_cancel_whose_effect_the_answer_does_not_settle_is_unknown() {
        let not_open = venue_error(-2011, "Unknown order sent.");
        let timeout = venue_error(
            -1007,
            "Timeout waiting for response from backend server. Send status unknown; execution \
             status unknown.",
        );
        let cases = [
            ("a 500 with no body", answered(500, "")),
            // A 5xx carrying the venue's notice is still a 5xx
            // (rest-api.md:116-118).
            ("a 503 carrying a notice", answered(503, &not_open)),
            ("a 409 carrying a notice", answered(409, &not_open)),
            ("a 400 carrying -1007", answered(400, &timeout)),
            ("a 200 with no body", answered(200, "")),
            (
                "a 200 from a captive portal",
                answered(200, "<html>Sign in"),
            ),
            (
                "a 403 from a firewall",
                answered(403, "<html>Request blocked"),
            ),
            ("a timeout", Attempt::TimedOut),
        ];
        for (what, attempt) in cases {
            let reply = our_cancel().classify(attempt);
            assert!(
                matches!(reply, CancelReply::Unknown { .. }),
                "{what}: {reply:?}"
            );
        }
    }

    #[test]
    fn a_reason_for_doubt_says_what_was_seen() {
        let said = WhyUnknown::ServerError {
            http_status: 503,
            said: Some(VenueError {
                code: -1008,
                msg: "Server is currently overloaded".to_owned(),
            }),
        }
        .to_string();
        assert!(said.starts_with("HTTP 503, "), "{said}");
        assert!(
            said.ends_with("(code -1008: Server is currently overloaded)"),
            "{said}"
        );
        let said = WhyUnknown::ServerError {
            http_status: 500,
            said: None,
        }
        .to_string();
        assert!(!said.contains("code"), "{said}");
        let said = WhyUnknown::Unsettled {
            http_status: 400,
            code: -1007,
            msg: "Timeout".to_owned(),
        }
        .to_string();
        assert!(said.contains("400 with code -1007 (Timeout)"), "{said}");
        let said = WhyUnknown::IdInUse {
            id: "q-r1-7".to_owned(),
            http_status: 400,
            code: -2010,
            msg: "Duplicate order sent.".to_owned(),
        }
        .to_string();
        assert!(
            said.contains("400 with code -2010 (Duplicate order sent.)"),
            "{said}"
        );
        assert!(
            said.contains("an open order already holds q-r1-7"),
            "{said}"
        );
    }
}
