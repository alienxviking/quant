//! The signed half of the venue's REST API.
//!
//! Separate from [`crate::rest::SnapshotClient`] on purpose, and the separation
//! is the design rather than tidiness: that client fetches depth and server time
//! for a recorder that places no orders, and giving it a private key would put
//! one in the address space of a seven-day capture that has no use for it. A type that
//! can sign is a type that can spend money, and it is held only by the thing
//! that trades.
//!
//! # Why `recv_window` is small and the clock check is hard
//!
//! Every signed request carries a millisecond `timestamp`, and Binance refuses
//! it if that is a second or more ahead of the venue's clock, or older than
//! `recvWindow` — five seconds by default — by the time it is processed. M1
//! already learned what a wrong clock does here: this machine was ~2 s ahead of
//! the venue and the latency metric read 2883 ms, which is why
//! `clock_offset_millis` exists and why the recorder warns above 1000 ms, the
//! venue's tolerance for a timestamp running ahead. That was a warning because
//! a wrong clock costs a capture nothing but a metric.
//!
//! For signed requests it is not a warning. A request stamped from a clock far
//! enough out is refused, with an error that says the timestamp was outside the
//! window and nothing about which machine was wrong — so this checks before it
//! trades rather than after. [`TradeClient::check_ready`] stamps its own request
//! with the measured offset taken off, which changes what the check guards:
//! [`MAX_CLOCK_SKEW_MS`] says how.
//!
//! `recv_window` is set to 5000 ms and not raised. A wider window is the venue
//! offering to accept staler requests; the thing it protects against is an order
//! sitting in a retry queue and arriving long after the decision that produced
//! it, which is exactly the failure a live engine must not have.
//!
//! # Sending an order
//!
//! [`TradeClient::place_order`] and [`TradeClient::cancel_order`] are the
//! transport [`crate::orders`] leaves to this module. They send what the
//! encoder wrote, once, and hand what came back to its classifier, which can be
//! no more right than the account of the attempt it is given. Four things
//! decide that, and each is enforced here and pinned by a test rather than left
//! to care:
//!
//! - **The query goes out exactly as it was encoded and signed**, with
//!   `timestamp`, `recvWindow` and `signature` after it and the body empty
//!   (`signed_request` says why).
//! - **No redirect is followed**, by any request this client makes
//!   ([`TradeClient::new`] says why). A 3xx reaches the classifier, which calls
//!   it unknown.
//! - **"Not sent" is claimed only when nothing left.** It is the one outcome
//!   that proves an order was not placed, so it is claimed only for an error
//!   `reqwest` reports as a failure to connect: a name that would not resolve,
//!   a connection refused, a TLS handshake that failed, all before a byte of
//!   the request was written. A timeout, and any other failure, a body that
//!   broke off after its status arrived included, may have followed a request
//!   the venue received, and leaves the outcome unknown.
//! - **The client waits longer than the venue does**, so that the venue's own
//!   answer to a slow matching engine arrives first: [`ORDER_TIMEOUT`].
//!
//! The timestamp is the caller's: when the strategy submitted the order, on the
//! venue's clock. The transport does not restamp it. `recvWindow` exists to
//! refuse an order that sat in a queue, and a stamp taken at sending would hide
//! the queue from it.

use std::time::Duration;

use crate::credentials::Credentials;
use crate::orders::{
    Attempt, CancelOrder, CancelReply, FilterName, NewOrder, Placement, ORDER_PATH, ORDER_TEST_PATH,
};
use crate::rest::SnapshotError;
use quant_core::fixed::{Notional, Qty};
use quant_core::report::Holding;

/// The `recvWindow` sent with every signed request, in milliseconds.
///
/// Binance's default, and only one side of the venue's rule. A request is
/// refused if its `timestamp` is older than this by the time the venue
/// processes it -- so time on the wire counts against it -- and, whatever this
/// is set to, if its `timestamp` is a second or more *ahead* of the venue's
/// clock. A stamp may run up to this far behind the venue, and less than a
/// second ahead of it.
///
/// It is not what the clock check compares against: that is
/// [`MAX_CLOCK_SKEW_MS`].
pub const RECV_WINDOW_MS: i64 = 5_000;

/// How long the venue's API waits for its matching engine before answering
/// `-1007`, *"Timeout waiting for response from backend server. Send status
/// unknown; execution status unknown."* (rest-api.md:102; errors.md:30-31).
pub const VENUE_TIMEOUT: Duration = Duration::from_secs(10);

/// How long [`TradeClient::place_order`] and [`TradeClient::cancel_order`]
/// wait for an answer, from the moment a request starts to connect.
///
/// Longer than the venue can take to answer, so that its `-1007` arrives
/// before this gives up whenever the venue sends one. The outcome is unknown
/// either way, but `-1007` says the request reached the venue, and a timeout of
/// our own says nothing. The time an answer can take comes in three parts:
///
/// - **Up to [`RECV_WINDOW_MS`] on the way there.** The venue starts on a
///   request only while it is within `recvWindow` of its `timestamp`
///   (rest-api.md:225-241), and the timestamp is when the strategy submitted,
///   before this starts sending. A request that took longer is refused on
///   arrival, which is quick.
/// - **Up to [`VENUE_TIMEOUT`] waiting on the matching engine**, after which the
///   venue answers `-1007`.
/// - **The answer's way back**, which has the five seconds that remain.
///
/// Twenty, then, against the floor of the venue's ten that `docs/live-run.md`
/// §5 set, and a check at compile time keeps it above the first two parts.
pub const ORDER_TIMEOUT: Duration = Duration::from_secs(20);

// The point of the number, as arithmetic. At or under the venue's own ten
// seconds, our timeout races the venue's -1007 and can throw away the one answer
// that says the venue saw the request; under the window as well, it can do that
// for a request the venue was still entitled to start on.
const _: () = assert!(
    ORDER_TIMEOUT.as_millis()
        > VENUE_TIMEOUT.as_millis()
            + Duration::from_millis(RECV_WINDOW_MS.unsigned_abs()).as_millis()
);

/// How far the host clock may be from the venue's before this refuses to trade.
///
/// The venue's rule is lopsided ([`RECV_WINDOW_MS`] has it). A second is its
/// *whole* tolerance for a timestamp running ahead of its clock, and a fifth of
/// the room it gives one running behind, so this is no margin kept inside the
/// venue's window on the side where that window is tight.
///
/// Nor does it bound how near rejection [`TradeClient::check_ready`]'s own
/// request is. That request is stamped not with the host's clock as it stands
/// but with `chrono_millis() - offset_ms`, the host's clock with the measured
/// offset taken off. Its timestamp is out from the venue's by the error in that
/// measurement, which is at most half the round trip of the reading it came
/// from, plus whatever the host clock has moved since -- and not by the skew.
/// A host at this limit is no nearer rejection on that request than one
/// exactly on time.
///
/// What it is, then, is a refusal to trade from a host whose clock is a second
/// out at all, placed where an *uncorrected* stamp from a host running ahead
/// would be at the venue's edge. Whether a corrected stamp should be gated on
/// the skew, and at what number, is not settled here.
pub const MAX_CLOCK_SKEW_MS: i64 = 1_000;

/// The fewest usable readings of the venue's clock a verdict on it may rest on.
///
/// Two, so that no verdict rests on one reading alone. The first request a
/// client makes opens its connection, so DNS, TCP and TLS all ride on its
/// outbound leg -- the asymmetry a midpoint cannot correct, and one
/// [`ClockSample::tightest`] can pass over only when there is another reading
/// to prefer. A reading whose round trip ran backwards is not usable and does
/// not count.
pub const MIN_CLOCK_READINGS: usize = 2;

/// One reading of the venue's clock against the host's.
///
/// All three are milliseconds since the Unix epoch. `sent_at` and `received_at`
/// are the host's wall clock, read immediately before the request leaves and
/// immediately after the answer arrives; `server_time` is the venue's
/// `serverTime` from `GET /api/v3/time`, read somewhere in between.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockSample {
    pub sent_at: i64,
    pub server_time: i64,
    pub received_at: i64,
}

impl ClockSample {
    /// How long the request took, by the host's clock.
    #[must_use]
    pub const fn round_trip_ms(&self) -> i64 {
        self.received_at - self.sent_at
    }

    /// `sent_at - server_time`: the offset read naively, short of the true one
    /// by the outbound leg. Worth printing beside the estimate, so the size of
    /// the correction is on the screen rather than taken on trust.
    #[must_use]
    pub const fn naive_offset_ms(&self) -> i64 {
        self.sent_at - self.server_time
    }

    /// This reading's [`midpoint_offset_ms`]. Positive means the host is ahead.
    #[must_use]
    pub const fn offset_ms(&self) -> i64 {
        midpoint_offset_ms(self.sent_at, self.server_time, self.received_at)
    }

    /// Of several readings, the one to believe: the smallest round trip.
    ///
    /// The standard NTP choice, for the reason [`midpoint_offset_ms`] gives:
    /// whatever the split between the legs, a reading is wrong by at most half
    /// its round trip, so the smallest round trip is the tightest bound on
    /// offer.
    ///
    /// A negative round trip is never chosen. It means the host clock stepped
    /// backwards while the request was in flight -- the very clock being
    /// measured -- and "smallest wins" alone would rank it above every honest
    /// reading. Of equal round trips, the earliest. `None` when no reading
    /// qualifies.
    #[must_use]
    pub fn tightest(readings: &[Self]) -> Option<Self> {
        readings
            .iter()
            .filter(|r| r.ran_forwards())
            .min_by_key(|r| r.round_trip_ms())
            .copied()
    }

    /// The reading to judge the host clock by, from those that came back of
    /// `attempts` tried: the [`tightest`](Self::tightest), provided at least
    /// [`MIN_CLOCK_READINGS`] of them ran forwards.
    ///
    /// # Errors
    ///
    /// [`NotReady::TooFewClockReadings`] when fewer did. A refusal to judge,
    /// not a verdict that the clock is wrong -- and not a reason to judge on
    /// whatever did come back, because one reading on its own may be the cold
    /// one.
    pub fn best_of(readings: &[Self], attempts: usize) -> Result<Self, NotReady> {
        let usable = readings.iter().filter(|r| r.ran_forwards()).count();
        match Self::tightest(readings) {
            Some(best) if usable >= MIN_CLOCK_READINGS => Ok(best),
            _ => Err(NotReady::TooFewClockReadings {
                attempts,
                came_back: readings.len(),
                usable,
            }),
        }
    }

    /// Whether the host clock ran forwards across the request, as it must have
    /// for the reading to be usable at all: see [`Self::tightest`]. One
    /// predicate for choosing a reading and for counting the usable ones, so
    /// the two cannot disagree about which readings those are.
    const fn ran_forwards(&self) -> bool {
        self.round_trip_ms() >= 0
    }
}

/// How far the host clock is from the venue's, in milliseconds, from one
/// reading: the host's clock at `sent_at` and `received_at` either side of the
/// request, and the venue's `server_time` in between.
///
/// **Positive means the host is ahead.** That is the sign
/// [`TradeClient::check_ready`] expects -- it subtracts the offset from the
/// host's clock to stamp a request in the venue's time -- and the magnitude is
/// what [`MAX_CLOCK_SKEW_MS`] bounds. `ops/preflight.sh` and `ops/fix-clock.sh`
/// compute the same midpoint with the opposite sign: positive there means the
/// host is behind.
///
/// # Why the midpoint
///
/// Say the host is `theta` ahead and each leg of the request takes `d`. The
/// venue reads its clock as the request arrives, `d` after `sent_at`, so the
/// naive `sent_at - server_time` comes out at `theta - d`: short by the outbound
/// leg, which makes a slow link look like a clock behind the venue. The round
/// trip is `2d`, and adding half of it back gives `theta` exactly -- the venue's
/// reading is taken to describe the host's instant halfway through the round
/// trip. *Subtracting* that half instead lands on `theta - 2d`: wrong by the
/// whole round trip, twice the error the correction exists to remove, and in
/// the same misleading direction. `venue-check` computed exactly that, and gated
/// trading on it, until it called this function instead.
///
/// What no arithmetic on one reading can remove is asymmetry. With an outbound
/// leg of `d1` and a return of `d2` the estimate is off by `(d2 - d1) / 2`, which
/// can never exceed half the round trip. So half the round trip bounds the
/// error, give or take a millisecond of rounding, and of several readings the
/// one with the smallest round trip is the one to believe: that is
/// [`ClockSample::tightest`].
#[must_use]
pub const fn midpoint_offset_ms(sent_at: i64, server_time: i64, received_at: i64) -> i64 {
    let reading = ClockSample {
        sent_at,
        server_time,
        received_at,
    };
    reading.naive_offset_ms() + reading.round_trip_ms() / 2
}

/// Why a live session must not start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotReady {
    /// The host clock is further from the venue's than [`MAX_CLOCK_SKEW_MS`]
    /// allows. Not a forecast that the venue would refuse a request stamped
    /// with the offset taken off: that constant says what the limit is.
    ClockSkew { offset_ms: i64, allowed_ms: i64 },
    /// Too few readings of the venue's clock came back usable to judge the
    /// host's by: no verdict either way. See [`MIN_CLOCK_READINGS`].
    TooFewClockReadings {
        attempts: usize,
        came_back: usize,
        usable: usize,
    },
    /// The venue refused the credentials: HTTP 401 whatever the body says, or
    /// one of the venue's codes -2014, *"API-key format invalid"*, -2015,
    /// *"Invalid API-key, IP, or permissions for action"*, or -1022,
    /// *"Signature for this request is not valid"* (errors.md:57-58,
    /// :338-342), under any status but 403, 418, 429 and 5xx. Those four
    /// decide first, whatever code comes with them, as [`Self::Firewall`],
    /// [`Self::RateLimited`] and [`Self::VenueFailure`]: each says the request
    /// was never judged on its credentials. A bad API key, a private key that
    /// is not the registered one's, or a key restricted to other IP addresses.
    BadCredentials {
        status: u16,
        venue: Option<VenueError>,
    },
    /// HTTP 403: the venue's web application firewall refused the request,
    /// which it says can be a rate limit or a security block
    /// (rest-api.md:112). Not a verdict on the key.
    Firewall { venue: Option<VenueError> },
    /// HTTP 429, a request rate limit broken, or 418, this IP banned for
    /// carrying on after 429s (rest-api.md:114-115, :158-160).
    ///
    /// `retry_after` is the `Retry-After` header as the venue sent it, which it
    /// documents as a number of seconds: to wait to avoid a ban after a 429, or
    /// until the ban is over after a 418 (rest-api.md:161).
    RateLimited {
        status: u16,
        retry_after: Option<String>,
        venue: Option<VenueError>,
    },
    /// HTTP 5xx: the venue's own failure (rest-api.md:116-118), not a verdict
    /// on anything that was sent.
    VenueFailure {
        status: u16,
        venue: Option<VenueError>,
    },
    /// Any other refusal of a signed request.
    Refused {
        status: u16,
        venue: Option<VenueError>,
    },
    /// The venue accepted the key and the signature, and the account document
    /// reports `canTrade` false (rest-api.md:4207).
    ///
    /// The venue's documentation shows the field without defining it, so this
    /// does not claim to know whose the `false` is. Taken at its word it is the
    /// account's, and an account whose trading ability is disabled is refused
    /// an order with *"This account may not place or cancel orders"*, for which
    /// the venue's remedy is its customer support (errors.md:388). A key's own
    /// permission to `TRADE` is a separate setting, in API Management
    /// (rest-api.md:194-196), which the `USER_DATA` request
    /// [`TradeClient::check_ready`] makes cannot test and
    /// [`TradeClient::check_trade_permission`] does. Either way this stops
    /// here rather than finding out from an order.
    CannotTrade,
    /// The venue answered 200, but not with the account document this reads.
    /// Which way it was not is said; what it held is not, because it lists the
    /// account's balances.
    UnreadableAccount(AccountFault),
    /// The venue answered 200 to the account request, and its balances do
    /// not state the two the run needs, each once and readable. From
    /// [`TradeClient::balances`], whose answer is criterion A's baseline: a
    /// run without one does not trade (`docs/live-run.md` §5, the owner's
    /// decision 1). Which asset and which way is said; no amount is.
    UnreadableBalances(BalanceFault),
    /// The venue refused this API key a `TRADE` request: HTTP 401 whatever
    /// the body says, or `-2015`, *"Invalid API-key, IP, or permissions for
    /// action"* (errors.md:341-342), under any status but those that decide
    /// first (see [`Self::BadCredentials`]). The key may not trade: trading is
    /// not enabled for it in API Management, where a key cannot `TRADE` until
    /// it is (rest-api.md:194-196), or its IP restriction leaves this address
    /// out. From [`TradeClient::check_trade_permission`].
    KeyCannotTrade {
        status: u16,
        venue: Option<VenueError>,
    },
    /// The venue judged the trade probe and refused it for a reason other
    /// than a filter: `-1013` or `-2010` with any message but a filter
    /// failure (errors.md:367-413). The venue named a reason it refuses an
    /// order, and an order of the run's may meet it too. From
    /// [`TradeClient::check_trade_permission`].
    ///
    /// That the venue judged the order at all suggests it accepted the
    /// signature and the key's permission to trade, and the reason says so,
    /// as a reading the Spot Testnet rehearsal must confirm.
    ProbeRefused { status: u16, venue: VenueError },
    /// The venue refused the trade probe on a filter: `-1013` or `-2010`
    /// with *"Filter failure: NAME"* (errors.md:425-446). The probe is the
    /// run's own order ([`TradeClient::check_trade_permission`]), so the
    /// run's orders would meet the same filter, and that holds for both
    /// kinds there are: the filters that judge an order's numbers, and those
    /// that judge the account, its open orders on the symbol or on the
    /// exchange and its position in the base asset (filters.md:217-219,
    /// :253-260, :331-333; errors.md:436-446). `filter` is the one the venue
    /// cited.
    ///
    /// Read for the key's permission as [`Self::ProbeRefused`] is.
    ProbeFiltered {
        status: u16,
        filter: FilterName,
        venue: VenueError,
    },
    /// The venue answered the trade probe with a 2xx, and not with the JSON
    /// object documented for it (rest-api.md:2349-2353).
    UnreadableOrderTest { status: u16 },
    /// The account pays its fees in BNB at a discount: its commission rates
    /// report `discount.enabledForAccount` true (rest-api.md:4841-4842). The
    /// user-data translator books a fee only in the pair's base or quote
    /// asset (`crate::user_data`), so on a pair without BNB it refuses a fill
    /// whose fee was taken in BNB rather than book it as either, and the run
    /// procedure turns the discount off. From
    /// [`TradeClient::check_bnb_discount_off`].
    BnbDiscountOn,
    /// The venue answered 200, but not with the commission rates this reads.
    /// Which way it was not is said; what it held is not, because it holds
    /// the account's rates.
    UnreadableCommission(CommissionFault),
    /// The venue answered a signed request with a success status, and the
    /// body was cut short: it broke off, or did not finish arriving within the
    /// client's timeout. The venue was reached, so this is not
    /// [`Self::Unreachable`], and what it said is not known, because nothing
    /// is read from part of an answer. `why` says which, without the URL.
    AnswerCutShort { status: u16, why: String },
    /// We could not reach the venue at all.
    Unreachable(String),
}

/// How a 200 answer to `GET /api/v3/account/commission` failed to be the
/// document [`TradeClient::check_bnb_discount_off`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommissionFault {
    /// The body is not JSON.
    NotJson,
    /// There is no `discount.enabledForAccount`.
    NoDiscountFlag,
    /// `discount.enabledForAccount` is neither `true` nor `false`.
    DiscountFlagNotBool,
}

impl core::fmt::Display for CommissionFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::NotJson => "the body is not JSON",
            Self::NoDiscountFlag => "the body has no discount.enabledForAccount field",
            Self::DiscountFlagNotBool => "discount.enabledForAccount is neither true nor false",
        })
    }
}

/// Where an account's commission rates for one symbol are read:
/// `GET /api/v3/account/commission`, `USER_DATA`, weight 20
/// (rest-api.md:4797-4813).
const COMMISSION_PATH: &str = "/api/v3/account/commission";

/// What a refusal of the trade probe's order suggests about the signature and
/// the key's permission to trade, and on what footing, said the same way
/// wherever one is reported. [`TradeClient::check_trade_permission`] gives the
/// documentation behind it.
///
/// A reading, and never a reason to be ready: only a probe the venue accepts
/// outright is.
const A_VERDICT_SUGGESTS_PERMISSION: &str = "That the venue judged the order at all suggests \
     it accepted the signature and this key's permission to trade, because it judges an order \
     only for a request whose signature and key it has accepted for trading, and refuses one \
     that fails either with a code of its own, -1022 or -2015. That is a reading of its \
     documentation, which does not state the order of its checks, and the Spot Testnet \
     rehearsal must confirm it";

/// The venue's own account of a refusal: its error document,
/// `{"code": -2015, "msg": "..."}` (errors.md:3-10).
///
/// Codes are universal and messages vary (errors.md:3-4), so the code is what
/// [`NotReady`] is sorted on, and both are printed as the venue sent them. The
/// documented messages name parameters, limits and at most a symbol
/// (errors.md:74-110, :284), never a key or a signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueError {
    pub code: i64,
    pub msg: String,
}

impl VenueError {
    /// The venue's error document, if that is what `body` is. Anything else —
    /// a firewall's HTML page, an empty body — is `None`, and none of it is
    /// kept.
    fn parse(body: &[u8]) -> Option<Self> {
        #[derive(serde::Deserialize)]
        struct Document {
            code: i64,
            msg: String,
        }
        serde_json::from_slice::<Document>(body).ok().map(|d| Self {
            code: d.code,
            msg: d.msg,
        })
    }
}

/// How a 200 answer to `GET /api/v3/account` failed to be the document
/// [`TradeClient::check_ready`] reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountFault {
    /// The body is not JSON.
    NotJson,
    /// There is no `canTrade` field.
    NoCanTrade,
    /// `canTrade` is neither `true` nor `false`.
    CanTradeNotBool,
}

impl core::fmt::Display for AccountFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::NotJson => "the body is not JSON",
            Self::NoCanTrade => "the body has no canTrade field",
            Self::CanTradeNotBool => "canTrade is neither true nor false",
        })
    }
}

/// How a 200 answer to `GET /api/v3/account` failed to state the balances
/// [`TradeClient::balances`] reads. Names an asset, never an amount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BalanceFault {
    /// The body is not JSON, or has no `balances` list of objects.
    NoBalances,
    /// The list does not name this asset.
    Missing(String),
    /// The list names this asset more than once, so which entry is the
    /// account's cannot be told.
    Repeated(String),
    /// This asset's `free` or `locked` is absent, or not a decimal string the
    /// fixed-point types take: a malformed amount is never rounded or taken
    /// for zero (invariant 5).
    Unreadable { asset: String, field: &'static str },
}

impl core::fmt::Display for BalanceFault {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NoBalances => f.write_str("the body has no balances list"),
            Self::Missing(asset) => write!(f, "the balances do not name {asset}"),
            Self::Repeated(asset) => write!(f, "the balances name {asset} more than once"),
            Self::Unreadable { asset, field } => {
                write!(f, "{asset}'s {field} is absent or not a decimal amount")
            }
        }
    }
}

/// What the account held of an instrument's two assets, as the venue said:
/// what a `balances` journal line is built from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Balances {
    pub base: Holding<Qty>,
    pub quote: Holding<Notional>,
}

/// `, code -2015: Invalid API-key, IP, or permissions for action.`, or nothing.
struct Said<'a>(Option<&'a VenueError>);

impl core::fmt::Display for Said<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.0 {
            Some(VenueError { code, msg }) => write!(f, ", code {code}: {msg}"),
            None => Ok(()),
        }
    }
}

/// [`NotReady::RateLimited`], said: a 418 is a ban and anything else a limit,
/// with the venue's `Retry-After` when it sent one.
fn fmt_rate_limited(
    f: &mut core::fmt::Formatter<'_>,
    status: u16,
    retry_after: Option<&str>,
    venue: Option<&VenueError>,
) -> core::fmt::Result {
    if status == 418 {
        write!(
            f,
            "this IP is banned for carrying on after rate-limit refusals (HTTP 418{}). The \
             venue's bans run from 2 minutes to 3 days, longer for repeat offenders",
            Said(venue)
        )?;
    } else {
        write!(
            f,
            "the venue is rate limiting this IP (HTTP {status}{}). Back off: carrying on after a \
             429 gets the IP banned",
            Said(venue)
        )?;
    }
    match retry_after {
        Some(wait) => write!(f, "; its Retry-After says {wait} (seconds)"),
        None => Ok(()),
    }
}

/// [`NotReady::ProbeFiltered`], said: the filter the venue cited, by its name
/// when this knows it, and why it stops the run as well as the probe.
fn fmt_probe_filtered(
    f: &mut core::fmt::Formatter<'_>,
    status: u16,
    filter: FilterName,
    venue: &VenueError,
) -> core::fmt::Result {
    match filter.as_str() {
        Some(name) => write!(f, "the venue refused the trade probe on its {name} filter")?,
        None => f.write_str(
            "the venue refused the trade probe on a filter this does not know, which its message \
             names",
        )?,
    }
    write!(
        f,
        " (HTTP {status}{}). The probe is the run's own order, so the run's own orders would meet \
         the same filter, whether it judges the order's numbers or the account's open orders or \
         position. {A_VERDICT_SUGGESTS_PERMISSION}",
        Said(Some(venue)),
    )
}

/// [`NotReady::TooFewClockReadings`], said: how many came back, how many of
/// those were unusable and why, and why one is not enough when one did come
/// back.
fn fmt_too_few_readings(
    f: &mut core::fmt::Formatter<'_>,
    attempts: usize,
    came_back: usize,
    usable: usize,
) -> core::fmt::Result {
    write!(
        f,
        "{came_back} of {attempts} readings of the venue's clock came back"
    )?;
    if usable < came_back {
        write!(
            f,
            ", {} with a round trip that ran backwards (the host clock stepped while it was being \
             read)",
            came_back.saturating_sub(usable)
        )?;
    }
    write!(
        f,
        ", and this will not judge the clock on fewer than {MIN_CLOCK_READINGS} usable ones"
    )?;
    if usable > 0 {
        // Why not judge on what there is: the question a reading that did
        // come back invites.
        f.write_str(
            ": a lone reading may be the cold one, its outbound leg carrying the connection's \
             DNS, TCP and TLS, which no midpoint corrects",
        )?;
    }
    Ok(())
}

/// [`NotReady::CannotTrade`], said. A constant, as [`BNB_DISCOUNT_ON`] is, to
/// keep `NotReady`'s `fmt` within clippy's length limit.
const CANNOT_TRADE: &str = "the venue accepted the key and the signature, and the account \
     reports canTrade false. The venue does not define the field. If this account's trading \
     ability is disabled, the venue's remedy is its customer support; this API key's own \
     permission to trade is a separate setting, in Binance's API Management";

/// [`NotReady::BnbDiscountOn`], said.
const BNB_DISCOUNT_ON: &str = "the account pays its fees in BNB at a discount \
     (discount.enabledForAccount is true). The live path's translator of the account stream books \
     a fee only in the pair's base or quote asset, so on a pair without BNB it refuses every fill \
     whose fee was taken in BNB, and the run procedure turns the discount off: switch paying fees \
     in BNB off for this account before trading";

impl core::fmt::Display for NotReady {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::ClockSkew {
                offset_ms,
                allowed_ms,
            } => write!(
                f,
                "the host clock is {offset_ms} ms from the venue's, past the {allowed_ms} ms this \
                 refuses to trade beyond (the venue itself refuses a timestamp a second or more \
                 ahead of its clock, or more than {RECV_WINDOW_MS} ms behind it). Sync the clock \
                 -- ops/fix-clock.sh with sudo on macOS, ops/fix-clock.ps1 as Administrator on \
                 Windows"
            ),
            Self::TooFewClockReadings {
                attempts,
                came_back,
                usable,
            } => fmt_too_few_readings(f, *attempts, *came_back, *usable),
            Self::BadCredentials { status, venue } => write!(
                f,
                "the venue refused the credentials (HTTP {status}{}). The API key, the signature \
                 or the key's IP restriction is wrong: {} must be the API key Binance issued for \
                 the registered public key, that key must be the public half of {} (venue-check \
                 prints it), a testnet pair is separate from a live one, and a key restricted by \
                 IP fails this way from a new address",
                Said(venue.as_ref()),
                Credentials::KEY_VAR,
                Credentials::PRIVATE_KEY_VAR,
            ),
            Self::Firewall { venue } => write!(
                f,
                "the venue's web application firewall refused the request (HTTP 403{}). That is \
                 not a verdict on the key: the venue says a firewall block can be a rate limit or \
                 a security rule",
                Said(venue.as_ref()),
            ),
            Self::RateLimited {
                status,
                retry_after,
                venue,
            } => fmt_rate_limited(f, *status, retry_after.as_deref(), venue.as_ref()),
            Self::VenueFailure { status, venue } => write!(
                f,
                "the venue failed on its own side (HTTP {status}{}). Not a verdict on the key or \
                 the signature: try again",
                Said(venue.as_ref()),
            ),
            Self::Refused { status, venue } => write!(
                f,
                "the venue refused a signed request (HTTP {status}{})",
                Said(venue.as_ref()),
            ),
            Self::CannotTrade => f.write_str(CANNOT_TRADE),
            Self::UnreadableAccount(fault) => write!(
                f,
                "the venue answered 200, but {fault}. Nothing of the body is printed: it lists \
                 the account's balances"
            ),
            Self::UnreadableBalances(fault) => write!(
                f,
                "the venue answered 200 to the account request, but {fault}, so there is no \
                 baseline to trade from. No amount is printed"
            ),
            Self::KeyCannotTrade { status, venue } => write!(
                f,
                "the venue refused this API key a TRADE request (HTTP {status}{}), so the key \
                 may not trade. Either trading is not enabled for it in Binance's API \
                 Management, where a key cannot TRADE until it is, or its IP restriction leaves \
                 this address out",
                Said(venue.as_ref()),
            ),
            Self::ProbeRefused { status, venue } => write!(
                f,
                "the venue refused the trade probe for a reason other than a filter (HTTP \
                 {status}{}), and an order of the run's may meet the same refusal. \
                 {A_VERDICT_SUGGESTS_PERMISSION}",
                Said(Some(venue)),
            ),
            Self::ProbeFiltered {
                status,
                filter,
                venue,
            } => fmt_probe_filtered(f, *status, *filter, venue),
            Self::UnreadableOrderTest { status } => write!(
                f,
                "the venue answered the trade probe with HTTP {status}, but not with the JSON \
                 object documented for it, so whether this key may trade is not shown"
            ),
            Self::BnbDiscountOn => f.write_str(BNB_DISCOUNT_ON),
            Self::UnreadableCommission(fault) => write!(
                f,
                "the venue answered 200 to the commission rates request, but {fault}. Nothing of \
                 the body is printed: it holds the account's commission rates"
            ),
            Self::AnswerCutShort { status, why } => write!(
                f,
                "the venue answered HTTP {status} and the body was cut short ({why}), so what it \
                 said is not known. The venue was reached: try again"
            ),
            Self::Unreachable(why) => write!(f, "could not reach the venue: {why}"),
        }
    }
}

impl std::error::Error for NotReady {}

/// The signed client.
///
/// `Debug` is derived and safe: [`Credentials`] redacts itself, which is the
/// property that makes holding one inside another struct unremarkable.
#[derive(Debug)]
pub struct TradeClient {
    http: reqwest::Client,
    base: String,
    credentials: Credentials,
    /// How long an order or a cancel waits: [`ORDER_TIMEOUT`], whatever the
    /// client was built with. Only this module's tests change it, to see a
    /// timeout without waiting twenty seconds for one.
    order_timeout: Duration,
}

impl TradeClient {
    /// # Errors
    ///
    /// [`SnapshotError::Transport`] if the HTTP client cannot be built.
    pub fn new(
        base: impl Into<String>,
        request_timeout: Duration,
        credentials: Credentials,
    ) -> Result<Self, SnapshotError> {
        // Before `build`, which reads the process-global rustls provider and
        // panics if none is set. See `crate::install_crypto_provider`.
        crate::install_crypto_provider();

        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .pool_idle_timeout(Duration::from_secs(300))
            // Never follow a redirect, for any request this client makes.
            // `reqwest` follows up to ten by default, and following a 307 or
            // 308 sends the same request again, to wherever the answer points:
            // for an order, a second order. Unfollowed, the 3xx is the answer,
            // and what reads it can only call it unknown or refused.
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("quant-live/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| SnapshotError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            base: base.into(),
            credentials,
            order_timeout: ORDER_TIMEOUT,
        })
    }

    /// This client, with orders and cancels waiting `timeout` instead of
    /// [`ORDER_TIMEOUT`]: for a test that needs a timeout to happen, and only
    /// there. Production has no way to change the number: outside this
    /// crate's tests it exists only behind `fake-venue`, which nothing that
    /// ships turns on, for `quant-live`'s tests through the real engine.
    #[cfg(any(test, feature = "fake-venue"))]
    #[must_use]
    pub fn with_order_timeout(mut self, timeout: Duration) -> Self {
        self.order_timeout = timeout;
        self
    }

    /// The query string for a signed request: the caller's parameters, plus the
    /// timestamp and window the venue requires, plus the signature.
    ///
    /// Public so a test can check the shape without a network, which is the only
    /// way to pin it: the venue's answer to a malformed signature is a 401 that
    /// says nothing about which part was wrong.
    #[must_use]
    pub fn signed_query(&self, params: &str, now_millis: i64) -> String {
        // Appended *before* signing, because they are part of what is signed.
        // Adding them afterwards produces a signature over a different string
        // than the one sent -- rejected by the venue, with no hint as to why.
        let with_time = if params.is_empty() {
            format!("timestamp={now_millis}&recvWindow={RECV_WINDOW_MS}")
        } else {
            format!("{params}&timestamp={now_millis}&recvWindow={RECV_WINDOW_MS}")
        };
        self.credentials.signed_query(&with_time)
    }

    /// Send a signed GET and return the body.
    ///
    /// # Errors
    ///
    /// [`SnapshotError`] for transport, status and body failures, in the public
    /// client's terms -- except that a failure's text leaves out the URL, which
    /// here carries the signature, and says what caused the failure instead.
    pub async fn signed_get(
        &self,
        path: &str,
        params: &str,
        now_millis: i64,
    ) -> Result<Vec<u8>, SnapshotError> {
        let reply = self
            .send_signed(reqwest::Method::GET, path, params, now_millis)
            .await
            .map_err(|failure| match failure {
                SignedFailure::Unanswered(e) | SignedFailure::CutShort { cause: e, .. } => e,
            })?;
        if !reply.status.is_success() {
            return Err(SnapshotError::Status(reply.status.as_u16()));
        }
        Ok(reply.body)
    }

    /// A signed request to `path`, stamped `stamped_at_ms`: the caller's
    /// parameters and the signer's, all in the URL's query, and no body.
    ///
    /// The one place a signed request is built, so that every one of them is
    /// built alike. The URL is formatted with the query exactly as it was
    /// signed. It is not built up with `reqwest`'s `.query()`, which encodes
    /// what it is given, and given a query already encoded would turn each `%`
    /// into `%25`. Nothing goes in the body: the venue signs the query string
    /// and the body concatenated (rest-api.md:555), and with the body empty
    /// those are one string, the one signed.
    fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &str,
        stamped_at_ms: i64,
    ) -> reqwest::RequestBuilder {
        let url = format!(
            "{}{path}?{}",
            self.base.trim_end_matches('/'),
            self.signed_query(params, stamped_at_ms)
        );
        self.http
            .request(method, url)
            // The API key identifies the account and travels in the header;
            // the private key never leaves `Credentials`.
            .header("X-MBX-APIKEY", self.credentials.key())
    }

    /// Send a signed request and return whatever the venue answered, refusals
    /// included, so that a refusal can be read for what it says.
    ///
    /// A success whose body did not arrive whole is [`SignedFailure::CutShort`],
    /// not a reply: nothing is read from part of an answer, and not
    /// [`SignedFailure::Unanswered`] either, because the venue did answer.
    async fn send_signed(
        &self,
        method: reqwest::Method,
        path: &str,
        params: &str,
        now_millis: i64,
    ) -> Result<SignedReply, SignedFailure> {
        let response = self
            .signed_request(method, path, params, now_millis)
            .send()
            .await
            .map_err(|e| SignedFailure::Unanswered(transport_error(e)))?;
        let status = response.status();
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body = match response.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            // A refusal is still a refusal when its explanation does not
            // arrive: say what the status says rather than that the venue
            // could not be reached.
            Err(_) if !status.is_success() => Vec::new(),
            Err(e) => {
                return Err(SignedFailure::CutShort {
                    status: status.as_u16(),
                    cause: if e.is_timeout() {
                        SnapshotError::Timeout
                    } else {
                        SnapshotError::Body(describe(e))
                    },
                })
            }
        };
        Ok(SignedReply {
            status,
            retry_after,
            body,
        })
    }

    /// Send `order` to `POST /api/v3/order`, stamped `stamped_at_ms`, and say
    /// what the answer means for it.
    ///
    /// `stamped_at_ms` is when the strategy submitted the order, in
    /// milliseconds on the venue's clock: the host's wall clock with the
    /// measured offset taken off, as [`Self::check_ready`] stamps its own
    /// request. It goes out as the order's `timestamp` unchanged, so that the
    /// venue refuses an order that waited too long to be sent (module docs).
    ///
    /// Sent once, whatever comes back, and never an error: every outcome is a
    /// [`Placement`], and only [`Placement::NotSent`] says the order certainly
    /// was not placed. A timed-out order is looked up, never sent again
    /// (`docs/live-run.md` §5).
    #[must_use = "an order is sent once, and this is all that says whether it was placed"]
    pub async fn place_order(&self, order: &NewOrder, stamped_at_ms: i64) -> Placement {
        let sent = self
            .send_order(reqwest::Method::POST, order.query(), stamped_at_ms)
            .await;
        order.classify(sent.attempt())
    }

    /// Send `cancel` to `DELETE /api/v3/order`, stamped `stamped_at_ms`, and
    /// say what the answer means. The sending is as [`Self::place_order`]'s;
    /// what an answer means is [`CancelOrder::classify`]'s.
    ///
    /// The stamp is not. `stamped_at_ms` is when the request is sent, on the
    /// venue's clock, and not when the strategy asked: wanting an order gone
    /// does not go stale, and a cancel stamped when it was asked for and then
    /// queued behind a request that timed out would be refused for its age,
    /// leaving the order resting (`docs/live-run.md` §5, *`LiveVenue`, decided
    /// before the code*, §1). `quant-live`'s request worker stamps it as it
    /// sends it.
    #[must_use = "a cancel is sent once, and this is all that says whether it took effect"]
    pub async fn cancel_order(&self, cancel: &CancelOrder, stamped_at_ms: i64) -> CancelReply {
        let sent = self
            .send_order(reqwest::Method::DELETE, cancel.query(), stamped_at_ms)
            .await;
        cancel.classify(sent.attempt())
    }

    /// Send a request to [`ORDER_PATH`] once, waiting no longer than
    /// [`ORDER_TIMEOUT`] for it, and say what the transport saw.
    async fn send_order(&self, method: reqwest::Method, params: &str, stamped_at_ms: i64) -> Sent {
        let response = match self
            .signed_request(method, ORDER_PATH, params, stamped_at_ms)
            .timeout(self.order_timeout)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => return Sent::unanswered(e),
        };
        let status = response.status().as_u16();
        match response.bytes().await {
            Ok(body) => Sent::Answered {
                status,
                body: body.to_vec(),
            },
            // The status arrived, so the request had certainly left this
            // machine, and an answer whose body broke off says nothing about
            // what became of it: never a placement, whatever the status.
            Err(e) if e.is_timeout() => Sent::TimedOut,
            Err(e) => Sent::Failed(describe(e)),
        }
    }

    /// Check that the venue accepts this API key and its signature, that our
    /// clock agrees with the venue's, and that the account reports `canTrade`.
    ///
    /// `GET /api/v3/account` is not the cheapest signed call: its weight is 20
    /// (rest-api.md:4180-4181), where `GET /api/v3/openOrders` for one symbol
    /// is 6 (rest-api.md:4293-4294). It is the probe because, as well as
    /// proving the API key, the private key, the signature construction and
    /// the clock in one round trip, its answer carries the account's
    /// `canTrade` (rest-api.md:4207). It places no order.
    ///
    /// It does **not** prove this key may trade. Its security type is
    /// `USER_DATA` (rest-api.md:4174), and an API key cannot `TRADE` until
    /// trading is enabled for it in API Management (rest-api.md:194-196), so a
    /// key without that permission passes. Nor does the venue define
    /// `canTrade`: its documentation shows the field and says nothing of it.
    /// Only a request whose security type is `TRADE` tests the key's
    /// permission, and [`Self::check_trade_permission`] makes one.
    ///
    /// Its body also lists the account's balances (rest-api.md:4215-4226).
    /// They are read for `canTrade` and nothing else, and nothing here ever
    /// prints or logs the body.
    ///
    /// # Errors
    ///
    /// [`NotReady`] with the reason, which is a thing to print and stop on. A
    /// refusal is sorted by what the operator should do about it, from its
    /// status and the venue's own code and message, which it carries; no
    /// reason carries the URL or the signature.
    pub async fn check_ready(&self, offset_ms: i64) -> Result<(), NotReady> {
        // `unsigned_abs`, not `abs`. `i64::MIN` has no positive counterpart in
        // an `i64`, so `abs` panics on it in a debug build and hands it back
        // unchanged in a release one -- negative, so never past the limit --
        // which would let the most extreme offset there is through the gate.
        if offset_ms.unsigned_abs() > MAX_CLOCK_SKEW_MS.unsigned_abs() {
            return Err(NotReady::ClockSkew {
                offset_ms,
                allowed_ms: MAX_CLOCK_SKEW_MS,
            });
        }
        // The venue's clock, not ours: a signed request is checked against the
        // venue's, and the offset above is what reconciles them.
        let now = chrono_millis() - offset_ms;
        let reply = self
            .send_signed(reqwest::Method::GET, "/api/v3/account", "", now)
            .await
            .map_err(NotReady::from)?;
        if !reply.status.is_success() {
            return Err(classify_refusal(
                reply.status.as_u16(),
                reply.retry_after,
                &reply.body,
            ));
        }
        account_can_trade(&reply.body)
    }

    /// Check that this API key may trade, and that the venue would take the
    /// run's order, by sending `probe` to `POST /api/v3/order/test`, which
    /// *"Creates and validates a new order but does not send it into the
    /// matching engine"* (rest-api.md:2321-2326).
    ///
    /// Its security type is `TRADE`, which an API key lacks until trading is
    /// enabled for it in API Management (rest-api.md:186-196), so it tests
    /// what [`Self::check_ready`]'s `USER_DATA` request cannot. It weighs 1
    /// (rest-api.md:2328-2333) and places nothing. It goes to
    /// [`ORDER_TEST_PATH`] and nowhere else: this takes no path, and the
    /// probe's parameters are percent-encoded, so nothing in them can move the
    /// request to `/api/v3/order`. Stamped as `check_ready` stamps, the host's
    /// clock less `offset_ms`, and sent once.
    ///
    /// `probe` is the run's own order, as `venue-check`'s is: its symbol,
    /// side, type and size, encoded as the run encodes them. The venue checks
    /// it against its rules and the account as they stand, so whatever it
    /// refuses the probe for, the run's orders would meet too.
    ///
    /// What the answer says:
    ///
    /// - **A 2xx with a JSON object**, `{}` as documented
    ///   (rest-api.md:2349-2353): the key may trade, and the venue found
    ///   nothing in the order to refuse. The only answer that is `Ok`.
    /// - **A 401, or `-2015`**: the key may not trade
    ///   ([`NotReady::KeyCannotTrade`]).
    /// - **A refusal of the order itself**, under `-1013`, the API refusing a
    ///   request before the matching engine, or `-2010`, an order refused
    ///   (errors.md:36-38, 329-330, 367-368): not ready, because the run's
    ///   orders would meet it. A filter failure is [`NotReady::ProbeFiltered`],
    ///   naming the filter, and any other reason [`NotReady::ProbeRefused`].
    ///   A filter is no more the probe's alone than any other reason: some
    ///   judge an order's numbers, which are the run's, and some the
    ///   account's open orders and position (filters.md:217-219, :253-260,
    ///   :331-333; errors.md:436-446).
    ///
    ///   Either refusal's reason also says that the venue judged the order,
    ///   which suggests it accepted the signature and the key's permission to
    ///   trade. The documentation makes a valid key and signature the
    ///   condition of a `TRADE` endpoint at all
    ///   (rest-api.md:186-189, 205-207), names validating the order as this
    ///   endpoint's work (rest-api.md:2326), and answers a key or signature
    ///   that fails with its own code, -2015 or -1022 (errors.md:57-58,
    ///   341-342), not with a verdict on the order. It does not spell out the
    ///   order of the venue's checks, so that is a reading, which the Spot
    ///   Testnet rehearsal must confirm, and the reason calls it one. Nothing
    ///   is ready on it.
    /// - **Anything else** as [`Self::check_ready`] reports a refusal: a
    ///   firewall, a rate limit or a ban, the venue's own failure, which decide
    ///   first whatever code comes with them; a bad signature or key format;
    ///   or a refusal the venue explains for itself.
    ///
    /// # Errors
    ///
    /// [`NotReady`], as above. None carries the URL or the signature; the
    /// venue's own code and message are kept.
    pub async fn check_trade_permission(
        &self,
        probe: &NewOrder,
        offset_ms: i64,
    ) -> Result<(), NotReady> {
        let now = chrono_millis().saturating_sub(offset_ms);
        let reply = self
            .send_signed(reqwest::Method::POST, ORDER_TEST_PATH, probe.query(), now)
            .await
            .map_err(NotReady::from)?;
        read_order_test(reply.status.as_u16(), reply.retry_after, &reply.body)
    }

    /// Check that this account does not pay its fees in BNB at a discount, by
    /// reading `discount.enabledForAccount` from its commission rates for
    /// `symbol`: `GET /api/v3/account/commission` (rest-api.md:4797-4846).
    ///
    /// The run procedure turns it off. A spot venue takes a fee in the asset
    /// received, which the engine books ([`quant_core::execution::FeeAsset`]);
    /// with the discount on, it can take BNB instead, and the user-data
    /// translator books a fee only in the pair's base or quote asset, so on a
    /// pair without BNB it refuses such a fill loudly rather than book it as
    /// either. This finds that out before an order does.
    ///
    /// `USER_DATA`, weight 20, and stamped as [`Self::check_ready`] stamps,
    /// the host's clock less `offset_ms`. Its body holds the account's
    /// commission rates (rest-api.md:4820-4847). It is read for the one field
    /// and nothing else, and nothing here prints or logs it.
    ///
    /// # Errors
    ///
    /// [`NotReady::BnbDiscountOn`] when the discount is on, and otherwise
    /// [`NotReady`] with the reason, a refusal sorted as `check_ready` sorts
    /// one. None carries any of the body, the URL or the signature.
    pub async fn check_bnb_discount_off(
        &self,
        symbol: &str,
        offset_ms: i64,
    ) -> Result<(), NotReady> {
        let now = chrono_millis().saturating_sub(offset_ms);
        let reply = self
            .send_signed(
                reqwest::Method::GET,
                COMMISSION_PATH,
                &crate::orders::param("symbol", symbol),
                now,
            )
            .await
            .map_err(NotReady::from)?;
        if !reply.status.is_success() {
            return Err(classify_refusal(
                reply.status.as_u16(),
                reply.retry_after,
                &reply.body,
            ));
        }
        bnb_discount_off(&reply.body)
    }

    /// What the account holds of `base` and `quote`, from the same signed
    /// `GET /api/v3/account` [`Self::check_ready`] makes (rest-api.md:4196-4229),
    /// stamped as it stamps, the host's clock less `offset_ms`.
    ///
    /// Criterion A's baseline, which the live binary journals at the start of
    /// a run and at its stop: a run that cannot take it does not trade (the
    /// owner's decision 1). It reads the two assets' `free` and `locked` and
    /// nothing else of the body, straight to fixed-point, and neither prints
    /// nor logs any of it. An asset the list does not name is an error rather
    /// than a zero: a baseline taken for nothing would make criterion A
    /// compare against a number the venue never said.
    ///
    /// # Errors
    ///
    /// [`NotReady::UnreadableBalances`] for a body that does not state both,
    /// and otherwise [`NotReady`] with the reason, a refusal sorted as
    /// `check_ready` sorts one.
    pub async fn balances(
        &self,
        base: &str,
        quote: &str,
        offset_ms: i64,
    ) -> Result<Balances, NotReady> {
        let now = chrono_millis().saturating_sub(offset_ms);
        let reply = self
            .send_signed(reqwest::Method::GET, "/api/v3/account", "", now)
            .await
            .map_err(NotReady::from)?;
        if !reply.status.is_success() {
            return Err(classify_refusal(
                reply.status.as_u16(),
                reply.retry_after,
                &reply.body,
            ));
        }
        read_balances(&reply.body, base, quote).map_err(NotReady::UnreadableBalances)
    }

    /// The three checks `venue-check` runs, in its order, and the token that
    /// says they passed: [`Self::check_ready`], [`Self::check_trade_permission`]
    /// with `probe`, and [`Self::check_bnb_discount_off`] for `symbol`, each
    /// stamped with `offset_ms` taken off the host's clock.
    ///
    /// The first refusal stops it, and nothing after that refusal is asked.
    /// The only way to make a [`Checked`], which is what a live venue is built
    /// from (`quant-live`'s `wire`), so that no venue that can trade exists
    /// before the host, the key and the account have been shown fit to.
    ///
    /// # Errors
    ///
    /// The first check's [`NotReady`], as that check reports it.
    pub async fn checked(
        &self,
        probe: &NewOrder,
        symbol: &str,
        offset_ms: i64,
    ) -> Result<Checked, NotReady> {
        self.check_ready(offset_ms).await?;
        self.check_trade_permission(probe, offset_ms).await?;
        self.check_bnb_discount_off(symbol, offset_ms).await?;
        Ok(Checked { offset_ms })
    }
}

/// How many readings of the venue's clock to try before believing one.
///
/// Five, as `ops/preflight.sh` takes. The first reading also opens the
/// connection, so DNS, TCP and TLS all land on its outbound leg -- exactly the
/// asymmetry a midpoint cannot correct -- and its round trip is the longer for
/// it, which is what lets [`ClockSample::tightest`] pass it over. The client
/// pools the connection, so the readings after it normally go out warm.
///
/// All five are tried whatever happens to any one of them, each bounded by the
/// client's timeout, and a verdict needs [`MIN_CLOCK_READINGS`] of them back.
/// Stopping at the first failure could leave the verdict resting on the one
/// reading that came back before it -- the cold one.
pub const CLOCK_READINGS: usize = 5;

// A run that could never collect enough readings could never be judged, and
// would say so only at run time, every time.
const _: () = assert!(MIN_CLOCK_READINGS <= CLOCK_READINGS);

/// [`CLOCK_READINGS`] readings of the venue's clock through `public`, the
/// host's taken from `host_millis` either side of each `GET /api/v3/time`,
/// and the attempts that failed, numbered from 1.
///
/// The venue's clock read the way NTP reads one: its reading taken to
/// describe the midpoint, and of several the one with the smallest round trip
/// believed ([`ClockSample::best_of`]). One reading routine for `venue-check`
/// and the live binary, so that the offset the check prints is the offset the
/// run is stamped with. `host_millis` is the run's own clock, as every other
/// read of time is.
pub async fn read_clock(
    public: &crate::rest::SnapshotClient,
    host_millis: impl Fn() -> i64,
) -> (Vec<ClockSample>, Vec<(usize, SnapshotError)>) {
    let mut readings = Vec::with_capacity(CLOCK_READINGS);
    let mut failures = Vec::new();
    for attempt in 1..=CLOCK_READINGS {
        let sent_at = host_millis();
        match public.server_time_millis().await {
            Ok(server_time) => readings.push(ClockSample {
                sent_at,
                server_time,
                received_at: host_millis(),
            }),
            // Noted and carried on from: one failed request says little about
            // the next, and the client's timeout bounds what waiting costs.
            Err(e) => failures.push((attempt, e)),
        }
    }
    (readings, failures)
}

/// The run tag the trade probe's id is written under, `q-venuecheck-0`. The
/// probe is never an order, but its id is sent all the same, and under this
/// tag no run of ours would ever have sent it.
pub const PROBE_RUN: &str = "venuecheck";

/// The order [`TradeClient::check_trade_permission`] asks the venue to
/// validate: a market buy of `qty` of `symbol`, the shape `MaCrossover`
/// sends, written by the encoder every order goes through, under
/// [`PROBE_RUN`].
///
/// `venue-check` sends it at M5's size and the live binary at the size the
/// run trades, so that the venue validates the order the run would send
/// rather than one it never will. The instrument is built from literals,
/// because the encoder reads only its symbol, exchange and kind and leaves
/// every venue rule to the venue (`crate::orders`): nothing of the rules
/// below reaches the request, and the asset names stay empty because nothing
/// reads them. What does reach it is the run's own order, which the venue
/// checks against its rules and the account as they stand, as it will check
/// the run's. Some of its filters judge the account rather than the order
/// (filters.md:217-219, :253-260, :331-333), so any refusal of the probe, a
/// filter failure included, is one the run's orders would meet.
///
/// # Errors
///
/// [`EncodeError`], for a symbol or size the encoder cannot write.
///
/// # Panics
///
/// Never: the literals parse and the tag is valid.
pub fn probe(symbol: &str, qty: Qty) -> Result<NewOrder, crate::orders::EncodeError> {
    use quant_core::event::Side;
    use quant_core::execution::{ClientOrderId, OrderKind, OrderRequest, TimeInForce};
    use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};

    let instrument = InstrumentDef {
        exchange: Exchange::Binance,
        symbol: symbol.to_owned(),
        base: String::new(),
        quote: String::new(),
        kind: InstrumentKind::Spot,
        tick_size: "0.01".parse().expect("a valid literal"),
        lot_size: "0.00001".parse().expect("a valid literal"),
        min_notional: "5".parse().expect("a valid literal"),
    };
    let request = OrderRequest {
        instrument: InstrumentRegistry::new().register(instrument.clone()),
        side: Side::Buy,
        qty,
        kind: OrderKind::Market,
        time_in_force: TimeInForce::Gtc,
    };
    let run = crate::user_data::RunTag::new(PROBE_RUN).expect("a valid run tag");
    NewOrder::encode(&request, ClientOrderId(0), &run, &instrument)
}

/// Proof that the checks `venue-check` runs passed on this host, and the
/// clock offset they passed with: what a live venue is built from
/// (`docs/live-run.md` §5, *`LiveVenue`, decided before the code*, §3).
///
/// Minted only by [`TradeClient::checked`]; it has no other constructor, so a
/// venue built from one was built after the checks and never instead of them.
/// It carries the offset because every order and every logon is stamped with
/// it: the offset the clock check passed is the offset the run uses, and not a
/// second reading that could disagree with it.
///
/// Not `Clone`: one check, one venue. Item 8 reads [`Checked::offset_ms`] for
/// the account listener before handing the token to `wire`.
#[derive(Debug, PartialEq, Eq)]
pub struct Checked {
    offset_ms: i64,
}

impl Checked {
    /// The host clock's offset from the venue's, in milliseconds, positive
    /// when the host is ahead: what every signed request's stamp has taken
    /// off it.
    #[must_use]
    pub const fn offset_ms(&self) -> i64 {
        self.offset_ms
    }
}

/// What the venue answered a signed request with, whatever the status.
///
/// No `Debug`, derived or otherwise: the body of a successful account request
/// lists balances, and a `{:?}` is how a body ends up in a log.
struct SignedReply {
    status: reqwest::StatusCode,
    retry_after: Option<String>,
    body: Vec<u8>,
}

/// Why a signed request left no reply to read.
///
/// Two cases, because the operator should hear them differently: a request
/// that got no answer may never have reached the venue, and one whose answer
/// was cut short certainly did.
enum SignedFailure {
    /// No status came back: [`transport_error`]'s account of it.
    Unanswered(SnapshotError),
    /// The venue answered with a success status, and the body broke off or
    /// timed out before it was whole. `cause` is [`SnapshotError::Timeout`]
    /// or [`SnapshotError::Body`], so that [`TradeClient::signed_get`] can
    /// report it in the public client's terms, as it always has.
    CutShort { status: u16, cause: SnapshotError },
}

impl From<SignedFailure> for NotReady {
    fn from(failure: SignedFailure) -> Self {
        match failure {
            SignedFailure::Unanswered(e) => Self::Unreachable(e.to_string()),
            SignedFailure::CutShort { status, cause } => Self::AnswerCutShort {
                status,
                why: cause.to_string(),
            },
        }
    }
}

/// What became of one order request, as the transport saw it: what an
/// [`Attempt`] is borrowed from.
///
/// The text in [`Sent::NotConnected`] and [`Sent::Failed`] is [`describe`]'s,
/// which leaves out the URL and with it the signature.
enum Sent {
    /// A status, and the whole body.
    Answered { status: u16, body: Vec<u8> },
    /// Not a byte of the request left this machine.
    NotConnected(String),
    /// No answer before [`ORDER_TIMEOUT`], once the request may have left.
    TimedOut,
    /// Any other failure, after the request may have left.
    Failed(String),
}

impl Sent {
    /// A request that got no answer, sorted by whether it can have left.
    ///
    /// "Not sent" only for `is_connect`. In `reqwest` 0.12 that holds when
    /// `hyper-util`'s client failed to make a connection, or to take one from
    /// its pool, before the request was handed to one: errors of kind
    /// `Connect` (`hyper-util` 0.1, `client/legacy/client.rs`), which are the
    /// connector's own failures, DNS, TCP and TLS, and the pool's. It is asked
    /// before `is_timeout` because an error can be both: a connection timing
    /// out inside the connector is a connect error, and never carried the
    /// request. `reqwest`'s own timeout is not a connect error even when it
    /// fires mid-connect, and is unknown like any other timeout. Everything
    /// else came after the request may have been written.
    fn unanswered(e: reqwest::Error) -> Self {
        if e.is_connect() {
            Self::NotConnected(describe(e))
        } else if e.is_timeout() {
            Self::TimedOut
        } else {
            Self::Failed(describe(e))
        }
    }

    /// What [`NewOrder::classify`] and [`CancelOrder::classify`] read.
    fn attempt(&self) -> Attempt<'_> {
        match self {
            Self::Answered { status, body } => Attempt::Answered {
                status: *status,
                body,
            },
            Self::NotConnected(why) => Attempt::NotConnected(why),
            Self::TimedOut => Attempt::TimedOut,
            Self::Failed(why) => Attempt::Failed(why),
        }
    }
}

/// Sort a refusal of a signed request by what it means for the operator.
///
/// The status decides first where it says the request was never judged on its
/// credentials: a firewall (403), a rate limit or ban (418, 429), the venue's
/// own failure (5xx). Otherwise a 401, or the venue's code for a bad key or a
/// bad signature under any status, is the credentials. What is left is a
/// refusal the venue explains for itself.
fn classify_refusal(status: u16, retry_after: Option<String>, body: &[u8]) -> NotReady {
    let venue = VenueError::parse(body);
    let credential_code = matches!(venue.as_ref().map(|v| v.code), Some(-2014 | -2015 | -1022));
    match status {
        403 => NotReady::Firewall { venue },
        418 | 429 => NotReady::RateLimited {
            status,
            retry_after,
            venue,
        },
        500..=599 => NotReady::VenueFailure { status, venue },
        401 => NotReady::BadCredentials { status, venue },
        _ if credential_code => NotReady::BadCredentials { status, venue },
        _ => NotReady::Refused { status, venue },
    }
}

/// Whether the account document reports `canTrade` true (rest-api.md:4207).
///
/// What the account reports, which the venue does not define, and not whether
/// this API key may trade: a `USER_DATA` request cannot test that, as
/// [`TradeClient::check_ready`] says, and [`TradeClient::check_trade_permission`]
/// does.
///
/// The body is read as JSON for that one field. No error carries any of it: it
/// lists the account's balances.
fn account_can_trade(body: &[u8]) -> Result<(), NotReady> {
    let document: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| NotReady::UnreadableAccount(AccountFault::NotJson))?;
    match document.get("canTrade") {
        Some(serde_json::Value::Bool(true)) => Ok(()),
        Some(serde_json::Value::Bool(false)) => Err(NotReady::CannotTrade),
        Some(_) => Err(NotReady::UnreadableAccount(AccountFault::CanTradeNotBool)),
        None => Err(NotReady::UnreadableAccount(AccountFault::NoCanTrade)),
    }
}

/// `base`'s and `quote`'s `free` and `locked`, read out of an account
/// document's `balances`, each straight to fixed-point.
fn read_balances(body: &[u8], base: &str, quote: &str) -> Result<Balances, BalanceFault> {
    fn amount<T: core::str::FromStr>(
        entry: &serde_json::Value,
        asset: &str,
        field: &'static str,
    ) -> Result<T, BalanceFault> {
        entry
            .get(field)
            .and_then(serde_json::Value::as_str)
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| BalanceFault::Unreadable {
                asset: asset.to_owned(),
                field,
            })
    }
    let document: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| BalanceFault::NoBalances)?;
    let listed = document
        .get("balances")
        .and_then(serde_json::Value::as_array)
        .ok_or(BalanceFault::NoBalances)?;
    let entry = |asset: &str| -> Result<&serde_json::Value, BalanceFault> {
        let mut named = listed
            .iter()
            .filter(|e| e.get("asset").and_then(serde_json::Value::as_str) == Some(asset));
        let first = named
            .next()
            .ok_or_else(|| BalanceFault::Missing(asset.to_owned()))?;
        if named.next().is_some() {
            return Err(BalanceFault::Repeated(asset.to_owned()));
        }
        Ok(first)
    };
    let (b, q) = (entry(base)?, entry(quote)?);
    Ok(Balances {
        base: Holding {
            free: amount(b, base, "free")?,
            locked: amount(b, base, "locked")?,
        },
        quote: Holding {
            free: amount(q, quote, "free")?,
            locked: amount(q, quote, "locked")?,
        },
    })
}

/// What an answer to `POST /api/v3/order/test` says, sorted as
/// [`TradeClient::check_trade_permission`] describes: `Ok` only for the
/// venue's acceptance of the probe outright.
fn read_order_test(status: u16, retry_after: Option<String>, body: &[u8]) -> Result<(), NotReady> {
    if (200..=299).contains(&status) {
        return match serde_json::from_slice::<serde_json::Value>(body) {
            Ok(serde_json::Value::Object(_)) => Ok(()),
            _ => Err(NotReady::UnreadableOrderTest { status }),
        };
    }
    match (status, VenueError::parse(body)) {
        // As for any signed request: each says the request was never judged,
        // whatever code comes with it.
        (403 | 418 | 429 | 500..=599, _) => Err(classify_refusal(status, retry_after, body)),
        (401, venue) => Err(NotReady::KeyCannotTrade { status, venue }),
        (_, Some(venue)) if venue.code == -2015 => Err(NotReady::KeyCannotTrade {
            status,
            venue: Some(venue),
        }),
        // A verdict on the order, which is the run's: never ready, a filter
        // failure included, since the run's orders would meet it too.
        (_, Some(venue)) if matches!(venue.code, -1013 | -2010) => {
            Err(match FilterName::in_message(&venue.msg) {
                Some(filter) => NotReady::ProbeFiltered {
                    status,
                    filter,
                    venue,
                },
                None => NotReady::ProbeRefused { status, venue },
            })
        }
        _ => Err(classify_refusal(status, retry_after, body)),
    }
}

/// Whether the commission rates report the BNB discount off for the account:
/// `discount.enabledForAccount` (rest-api.md:4841-4842).
///
/// `enabledForSymbol` beside it is not read. The documentation shows both
/// without defining either (rest-api.md:4841-4846). Read by their names, the
/// account's is the setting the run procedure turns off, and it is the
/// stricter of the two: on for the account and off for this symbol is still
/// refused.
///
/// The body is read as JSON for that one field. No error carries any of it: it
/// holds the account's commission rates.
fn bnb_discount_off(body: &[u8]) -> Result<(), NotReady> {
    let document: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| NotReady::UnreadableCommission(CommissionFault::NotJson))?;
    match document
        .get("discount")
        .and_then(|discount| discount.get("enabledForAccount"))
    {
        Some(serde_json::Value::Bool(false)) => Ok(()),
        Some(serde_json::Value::Bool(true)) => Err(NotReady::BnbDiscountOn),
        Some(_) => Err(NotReady::UnreadableCommission(
            CommissionFault::DiscountFlagNotBool,
        )),
        None => Err(NotReady::UnreadableCommission(
            CommissionFault::NoDiscountFlag,
        )),
    }
}

/// A request that got no answer, said without its URL.
fn transport_error(e: reqwest::Error) -> SnapshotError {
    if e.is_timeout() {
        SnapshotError::Timeout
    } else {
        SnapshotError::Transport(describe(e))
    }
}

/// `reqwest`'s error as text, without the URL and with what caused it.
///
/// `reqwest` writes the request's URL into its error's text, and this URL
/// carries the signature: not the private key, and not reversible to it, but
/// per-request authentication material with no business in a log that outlives
/// the request. So the URL is dropped (`without_url`) rather than trusted to the
/// redaction, which stays as a second line. What the URL leaves out is what
/// went wrong — DNS, a refused connection, TLS — which `reqwest` keeps in the
/// error's sources rather than its text, so those are said instead.
fn describe(e: reqwest::Error) -> String {
    let e = e.without_url();
    let mut text = e.to_string();
    let mut cause = std::error::Error::source(&e);
    while let Some(c) = cause {
        text.push_str(": ");
        text.push_str(&c.to_string());
        cause = c.source();
    }
    redact_signature(&text)
}

/// Wall-clock milliseconds.
///
/// Not a `Clock`, and this is the one place in the codebase where that is
/// right: invariant 4 exists so that anything on the *dispatch* path can be
/// backtested, and a signed request's timestamp is not on it. The venue is
/// checking our wall clock against its own; an injected clock would let a
/// backtest produce a signature, which is not a thing a backtest should be able
/// to do.
fn chrono_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// Strip every signature out of anything about to be logged.
///
/// The whole value goes, whatever its alphabet: everything after `signature=`
/// up to the next `&`, whitespace, quote or closing parenthesis, or the end of
/// the text. This used to strip hex digits only, which was the whole of an HMAC
/// signature. An Ed25519 signature is base64, and percent-encoded in a query
/// (rest-api.md:644-650), so a hex-only rule would stop at its first `+`, `%`
/// or letter past `f` and let the rest of it into the log.
///
/// The parenthesis is there because `reqwest` writes a URL inside one, and none
/// of the alphabets a signature comes in — hex, base64, percent-encoding — has
/// one to cut a value short.
fn redact_signature(text: &str) -> String {
    const MARK: &str = "signature=";
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(MARK) {
        let value = at + MARK.len();
        out.push_str(&rest[..value]);
        out.push_str("<redacted>");
        let tail = &rest[value..];
        let end = tail
            .find(|c: char| matches!(c, '&' | '"' | '\'' | ')') || c.is_whitespace())
            .unwrap_or(tail.len());
        rest = &tail[end..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::{
        account_can_trade, bnb_discount_off, classify_refusal, midpoint_offset_ms, read_balances,
        read_order_test, redact_signature, AccountFault, BalanceFault, ClockSample,
        CommissionFault, NotReady, TradeClient, VenueError, A_VERDICT_SUGGESTS_PERMISSION,
        MAX_CLOCK_SKEW_MS, RECV_WINDOW_MS,
    };
    use crate::credentials::fixture;
    use crate::orders::{
        CancelOrder, CancelReply, FilterName, NewOrder, Placement, WhyUnknown, ORDER_PATH,
        ORDER_TEST_PATH,
    };
    use crate::user_data::RunTag;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use quant_core::event::Side;
    use quant_core::execution::{
        ClientOrderId, OrderKind, OrderRequest, RejectReason, TimeInForce, VenueOrderId,
    };
    use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
    use ring::signature::{UnparsedPublicKey, ED25519};
    use std::time::Duration;

    fn client() -> TradeClient {
        client_at("https://example.invalid")
    }

    fn client_at(base: &str) -> TradeClient {
        TradeClient::new(base, Duration::from_secs(5), fixture::example()).expect("builds")
    }

    /// An HTTP/1.1 response with a body, closing the connection after it.
    fn http(status: &str, headers: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\n{headers}Content-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// How long [`serve_once`] waits for its request, and then for each read
    /// of it, before it fails the test.
    ///
    /// A request to this machine takes milliseconds, so this is generous; it
    /// is short so that a request which never comes fails the run instead of
    /// hanging it. Two ways that happens: `check_ready` erring before it
    /// sends, and `reqwest` sending it to a proxy named in `HTTP_PROXY` or
    /// `ALL_PROXY` instead of here.
    const SERVE_DEADLINE: Duration = Duration::from_secs(10);

    /// A one-shot HTTP server on this machine. It reads the first request,
    /// answers it with `response` — or, when that is empty, hangs up without a
    /// word — and hands back the request it was sent, through [`received`].
    ///
    /// The venue, as far as the client can tell, minus the network: what
    /// `check_ready` makes of a status, a header and a body is tested on the
    /// path it takes in production, with no credentials and nothing sent
    /// beyond this machine.
    ///
    /// Nothing in it waits without a bound. A blocking `accept` would wait
    /// forever for a request that is not coming, and the test's join on this
    /// thread with it, so the listener is polled until [`SERVE_DEADLINE`]
    /// instead, and each read of the request times out after as long. Past
    /// either, this thread panics and [`received`] fails the test.
    fn serve_once(response: String) -> (String, std::thread::JoinHandle<String>) {
        use std::io::{ErrorKind, Write as _};
        use std::time::Instant;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("a listener to poll");
        let base = format!("http://{}", listener.local_addr().expect("address"));
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + SERVE_DEADLINE;
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        assert!(
                            Instant::now() < deadline,
                            "no request reached the test server in {SERVE_DEADLINE:?}: the client \
                             failed before sending, or sent it to a proxy (HTTP_PROXY, ALL_PROXY)"
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(e) => panic!("accept: {e}"),
                }
            };
            let request = read_request(&mut stream);
            stream.write_all(response.as_bytes()).expect("write");
            request
        });
        (base, server)
    }

    /// Read one request's head off `stream`, through the blank line that
    /// ends it. Each read waits no longer than [`SERVE_DEADLINE`], and past
    /// that this panics.
    fn read_request(stream: &mut std::net::TcpStream) -> String {
        use std::io::Read as _;
        // A socket accepted from a non-blocking listener can be non-blocking
        // itself, as it is on Windows. Reads block again, each for no longer
        // than the deadline's length.
        stream.set_nonblocking(false).expect("a blocking stream");
        stream
            .set_read_timeout(Some(SERVE_DEADLINE))
            .expect("a read timeout");
        let mut request = Vec::new();
        let mut buf = [0; 4096];
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream
                .read(&mut buf)
                .unwrap_or_else(|e| panic!("the request did not arrive in time: {e}"));
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buf[..n]);
        }
        String::from_utf8(request).expect("an HTTP request head is text")
    }

    /// What [`TestVenue`] does with a request it has read.
    #[derive(Clone)]
    enum Reply {
        /// Write these bytes and close the connection: a whole answer, or one
        /// cut short.
        Bytes(String),
        /// The same, after waiting this long: a venue that takes its time.
        Late(Duration, String),
        /// Write nothing, and hold the connection open until the venue is
        /// stopped: a venue that took the request and never answered it.
        Silence,
        /// Write these bytes, and then hold the connection open until the
        /// venue is stopped: a venue that began an answer and never finished
        /// it.
        Stall(String),
    }

    /// A test venue on this machine that, unlike [`serve_once`], can be sent
    /// more than one request. Each request it reads gets the next of its
    /// replies, and the last again once they run out, and every request is
    /// kept, in order, for [`TestVenue::stop`] to hand back.
    ///
    /// It runs until it is stopped, not until a request comes, so a test can
    /// count every request the client made, a second one that should never
    /// have been sent included. A test stops it once the client has its
    /// answer: by then every request the client made has been read, because
    /// each is kept before it is answered. Polled, like [`serve_once`], and
    /// never running past [`SERVE_DEADLINE`].
    struct TestVenue {
        base: String,
        stopping: std::sync::Arc<std::sync::atomic::AtomicBool>,
        server: std::thread::JoinHandle<Vec<String>>,
    }

    impl TestVenue {
        fn start(replies: Vec<Reply>) -> Self {
            use std::io::{ErrorKind, Write as _};
            use std::sync::atomic::{AtomicBool, Ordering};
            use std::sync::Arc;
            use std::time::Instant;
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            listener.set_nonblocking(true).expect("a listener to poll");
            let base = format!("http://{}", listener.local_addr().expect("address"));
            let stopping = Arc::new(AtomicBool::new(false));
            let stop = Arc::clone(&stopping);
            let server = std::thread::spawn(move || {
                let deadline = Instant::now() + SERVE_DEADLINE;
                let mut requests = Vec::new();
                // Connections answered with silence, open until the venue stops.
                let mut held = Vec::new();
                while !stop.load(Ordering::SeqCst) && Instant::now() < deadline {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let reply = replies.get(requests.len()).or(replies.last()).cloned();
                            requests.push(read_request(&mut stream));
                            match reply {
                                Some(Reply::Bytes(bytes)) => {
                                    stream.write_all(bytes.as_bytes()).expect("write");
                                }
                                Some(Reply::Late(wait, bytes)) => {
                                    std::thread::sleep(wait);
                                    stream.write_all(bytes.as_bytes()).expect("write");
                                }
                                Some(Reply::Silence) => held.push(stream),
                                Some(Reply::Stall(bytes)) => {
                                    stream.write_all(bytes.as_bytes()).expect("write");
                                    held.push(stream);
                                }
                                // No replies at all: hang up without a word.
                                None => {}
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(e) => panic!("accept: {e}"),
                    }
                }
                drop(held);
                requests
            });
            Self {
                base,
                stopping,
                server,
            }
        }

        /// Stop, and hand back every request it read, in the order they came.
        fn stop(self) -> Vec<String> {
            self.stopping
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.server
                .join()
                .unwrap_or_else(|_| panic!("the test venue failed"))
        }
    }

    /// The request [`serve_once`] was sent. Fails the test when there was none
    /// by the deadline: the server thread's own panic, printed above this
    /// one, says which wait ran out.
    fn received(server: std::thread::JoinHandle<String>) -> String {
        server
            .join()
            .unwrap_or_else(|_| panic!("the test server did not get its request"))
    }

    /// The payload `request` signed, once the signature it carried has been
    /// checked the way the venue reads one off the wire.
    ///
    /// The request line's query, whatever the method, is split at
    /// `&signature=`. What comes before is the payload, exactly as sent
    /// (rest-api.md:555). What comes after is
    /// decoded once, as a query string's values are read, `+` to a space and
    /// `%XX` to its byte, and must verify over that payload under the example
    /// key's public half as OpenSSL derives it. A signature encoded twice --
    /// what `reqwest`'s `.query()` would make of one this crate had already
    /// encoded -- still holds a `%` after one decoding, and in one sent raw
    /// any `+` is read back as a space: neither is base64, here or at the
    /// venue.
    fn signed_on_the_wire(request: &str) -> &str {
        let line = request.lines().next().unwrap_or_default();
        let (_, query) = line
            .split_once(' ')
            .and_then(|(_method, rest)| rest.strip_suffix(" HTTP/1.1"))
            .and_then(|target| target.split_once('?'))
            .unwrap_or_else(|| panic!("not a request with a query: {line}"));
        let (payload, sent) = query
            .split_once("&signature=")
            .unwrap_or_else(|| panic!("no signature: {line}"));
        let decoded = fixture::percent_decode(&sent.replace('+', " "));
        let signature = STANDARD.decode(&decoded).unwrap_or_else(|e| {
            panic!("one decoding of the signature sent, {sent}, is not base64: {decoded}: {e}")
        });
        UnparsedPublicKey::new(&ED25519, fixture::EXAMPLE_PUBLIC_KEY)
            .verify(payload.as_bytes(), &signature)
            .unwrap_or_else(|_| panic!("the signature sent does not verify over {payload}"));
        payload
    }

    /// The account document from the venue's documentation
    /// (rest-api.md:4196-4229), cut down, with `canTrade` as given. Its
    /// balance is distinctive, so a reason that quoted the body would show it.
    fn account(can_trade: &str) -> String {
        format!(
            r#"{{"makerCommission":15,"takerCommission":15,"canTrade":{can_trade},"canWithdraw":true,"canDeposit":true,"accountType":"SPOT","balances":[{{"asset":"BTC","free":"{BALANCE}","locked":"0.00000000"}}],"permissions":["SPOT"],"uid":354937868}}"#
        )
    }

    const BALANCE: &str = "4723846.89208129";

    /// A reading of a host clock `theta` ms ahead of the venue's (negative:
    /// behind), over a request that takes `out` ms to arrive and `back` ms to
    /// return. The venue reads its clock as the request arrives, and its clock
    /// is the host's minus `theta`.
    fn reading(sent_at: i64, theta: i64, out: i64, back: i64) -> ClockSample {
        ClockSample {
            sent_at,
            server_time: sent_at + out - theta,
            received_at: sent_at + out + back,
        }
    }

    #[test]
    fn the_midpoint_recovers_the_offset_exactly_when_the_legs_are_symmetric() {
        // Subtracting half the round trip instead of adding it gives
        // `theta - 2d`, which fails every case with a leg at all -- and puts
        // several on the wrong side of the gate, in both directions. A host
        // exactly on time behind a 600 ms leg would read -1200 ms and be refused
        // for a clock that is fine; one 2500 ms ahead behind a 995 ms leg would
        // read 510 ms and be let through.
        for theta in [-2_500, -1_001, -450, -1, 0, 1, 450, 1_001, 2_500] {
            for d in [0, 1, 37, 250, 600, 995] {
                let r = reading(1_700_000_000_000, theta, d, d);
                assert_eq!(
                    midpoint_offset_ms(r.sent_at, r.server_time, r.received_at),
                    theta,
                    "a host {theta} ms ahead, {d} ms each way"
                );
                assert_eq!(r.offset_ms(), theta, "{r:?}");
                // What the correction is for: the naive reading is short by
                // exactly the outbound leg, which is half the round trip.
                assert_eq!(r.naive_offset_ms(), theta - d, "{r:?}");
                assert_eq!(r.round_trip_ms(), 2 * d, "{r:?}");
            }
        }
    }

    #[test]
    fn the_reading_with_the_smallest_round_trip_is_the_one_believed() {
        // One clock, 250 ms ahead, read six times over legs of different length
        // and symmetry, so each reading tells a different story. The first
        // carries a handshake on its outbound leg, as a cold first request does,
        // and is the furthest from the truth. The tightest is neither first nor
        // last, so a rule that took either -- or the largest round trip --
        // believes a different reading and fails.
        //
        // Nor is the tightest the extreme of anything but its round trip. Its
        // return leg is the longer of its two, and a decoy beats it on each key
        // a rule could mistake for the round trip: the largest midpoint, the
        // largest naive offset (which is the shortest outbound leg), and the
        // shortest return leg. Where the outbound leg is always the longer, all
        // of those rank the readings as the round trip does, and a fixture built
        // only of such readings cannot tell any of them from the right rule.
        let theta = 250;
        let readings = [
            reading(1_700_000_000_000, theta, 400, 100), // round trip 500, reads 100
            reading(1_700_000_001_000, theta, 150, 30),  // 180, reads 190
            reading(1_700_000_002_000, theta, 5, 70),    // 75, reads 282: shortest out
            reading(1_700_000_003_000, theta, 15, 25),   // 40, reads 255: the tightest
            reading(1_700_000_004_000, theta, 50, 8),    // 58, reads 229: shortest back
            reading(1_700_000_005_000, theta, 10, 110),  // 120, reads 300: largest midpoint
        ];
        let believed = ClockSample::tightest(&readings).expect("six to choose from");
        assert_eq!(believed, readings[3]);
        assert_eq!(believed.round_trip_ms(), 40);
        // Its legs are uneven, so the midpoint misses -- by no more than half
        // the round trip, which is the bound the choice is made for.
        assert_eq!(believed.offset_ms(), 255);
        assert!(
            (believed.offset_ms() - theta).abs() <= believed.round_trip_ms() / 2,
            "{believed:?}"
        );
    }

    #[test]
    fn a_reading_whose_round_trip_ran_backwards_is_never_believed() {
        // The host clock stepped back while the request was in flight. Smallest
        // wins on its own would rank this above every honest reading. Pinned at
        // the edge as well as far from it: a round trip one millisecond below
        // zero is as impossible as one a second below, and a rule that refused
        // only the second would pass a test that tried only the second.
        let honest = reading(1_700_000_000_000, 250, 30, 30);
        for stepped_back_by in [1, 1_000] {
            let stepped = ClockSample {
                received_at: honest.sent_at - stepped_back_by,
                ..honest
            };
            assert_eq!(stepped.round_trip_ms(), -stepped_back_by, "{stepped:?}");
            assert_eq!(
                ClockSample::tightest(&[stepped, honest]),
                Some(honest),
                "{stepped:?}"
            );
            assert_eq!(ClockSample::tightest(&[stepped]), None, "{stepped:?}");
        }
        assert_eq!(ClockSample::tightest(&[]), None);

        // And the boundary from the other side: a round trip inside one
        // millisecond is quick, not impossible, and is believed.
        let instant = reading(1_700_000_000_000, 250, 0, 0);
        assert_eq!(instant.round_trip_ms(), 0);
        assert_eq!(ClockSample::tightest(&[honest, instant]), Some(instant));
    }

    #[test]
    fn no_verdict_on_the_clock_rests_on_fewer_than_two_readings() {
        // However many were tried. A client's first request carries its
        // connection's setup on the outbound leg, so a reading that came back
        // alone may be the cold one -- and with nothing to prefer it over, it
        // would be believed. This one reads a host 250 ms ahead as 100.
        let cold = reading(1_700_000_000_000, 250, 400, 100);
        let warm = reading(1_700_000_001_000, 250, 20, 20);
        assert_eq!(cold.offset_ms(), 100);

        for came_back in [&[][..], &[cold][..]] {
            assert_eq!(
                ClockSample::best_of(came_back, 5),
                Err(NotReady::TooFewClockReadings {
                    attempts: 5,
                    came_back: came_back.len(),
                    usable: came_back.len(),
                }),
                "{came_back:?}"
            );
        }

        // A reading whose round trip ran backwards came back, and still does
        // not count: two came back, one is usable, and one is not enough.
        let stepped = ClockSample {
            received_at: warm.sent_at - 1,
            ..warm
        };
        assert_eq!(
            ClockSample::best_of(&[cold, stepped], 5),
            Err(NotReady::TooFewClockReadings {
                attempts: 5,
                came_back: 2,
                usable: 1,
            })
        );

        // Two that ran forwards are enough, and the one believed is the tighter.
        assert_eq!(ClockSample::best_of(&[cold, warm], 5), Ok(warm));
        assert_eq!(ClockSample::best_of(&[cold, stepped, warm], 5), Ok(warm));
    }

    #[test]
    fn a_refusal_to_judge_the_clock_says_how_many_readings_came_back() {
        // One back says why one is not enough; none back has nothing to explain.
        let said = NotReady::TooFewClockReadings {
            attempts: 5,
            came_back: 1,
            usable: 1,
        }
        .to_string();
        assert!(
            said.starts_with("1 of 5 readings of the venue's clock came back, and "),
            "{said}"
        );
        assert!(
            said.contains("a lone reading may be the cold one"),
            "{said}"
        );
        let said = NotReady::TooFewClockReadings {
            attempts: 5,
            came_back: 0,
            usable: 0,
        }
        .to_string();
        assert!(
            said.starts_with("0 of 5 readings of the venue's clock came back, and "),
            "{said}"
        );
        assert!(!said.contains("lone reading"), "{said}");

        // And when some came back unusable, how many, and why.
        let said = NotReady::TooFewClockReadings {
            attempts: 5,
            came_back: 3,
            usable: 1,
        }
        .to_string();
        assert!(
            said.starts_with(
                "3 of 5 readings of the venue's clock came back, 2 with a round trip that ran \
                 backwards"
            ),
            "{said}"
        );
    }

    #[test]
    fn the_timestamp_and_window_are_inside_the_signature_not_appended_after_it() {
        // The mistake that produces a valid-looking request the venue refuses
        // with an error naming neither cause: sign the caller's parameters, then
        // append `timestamp`. The signature then covers a different string than
        // the one sent.
        let q = client().signed_query("symbol=BTCUSDT", 1_700_000_000_000);
        let signature_at = q.find("&signature=").expect("signed");
        let signed_part = &q[..signature_at];
        assert!(signed_part.contains("timestamp=1700000000000"), "{q}");
        assert!(
            signed_part.contains(&format!("recvWindow={RECV_WINDOW_MS}")),
            "{q}"
        );
        // The signature must be over exactly what precedes it: checked by
        // verifying it there, under the example key's public half as OpenSSL
        // derives it, rather than by signing again with the code under test.
        let sent = &q[signature_at + "&signature=".len()..];
        let signature = STANDARD
            .decode(fixture::percent_decode(sent))
            .unwrap_or_else(|e| panic!("{sent}: {e}"));
        UnparsedPublicKey::new(&ED25519, fixture::EXAMPLE_PUBLIC_KEY)
            .verify(signed_part.as_bytes(), &signature)
            .expect("the signature covers exactly what precedes it");
    }

    #[test]
    fn no_parameters_still_produces_a_well_formed_query() {
        // `GET /api/v3/account` takes none, and a leading `&` is a malformed
        // query the venue rejects for a reason unrelated to credentials.
        let q = client().signed_query("", 1);
        assert!(q.starts_with("timestamp=1&"), "{q}");
        assert!(!q.contains("&&"), "{q}");
    }

    #[tokio::test]
    async fn a_skewed_clock_refuses_before_any_request_is_made() {
        // `example.invalid` does not resolve, so reaching the network at all
        // would surface as `Unreachable`. Getting `ClockSkew` proves the check
        // ran first -- which is the point: a host refused on its clock is
        // refused before anything is sent, with a message about the clock
        // rather than whatever the venue would have said.
        let why = client()
            .check_ready(MAX_CLOCK_SKEW_MS + 1)
            .await
            .expect_err("must refuse");
        assert!(
            matches!(why, NotReady::ClockSkew { .. }),
            "expected a clock refusal, got {why:?}"
        );
        // And the fix it names works on the machine reading it. `fix-clock.sh`
        // drives macOS's network time and does nothing for Windows, which has
        // its own script.
        let said = why.to_string();
        assert!(
            said.contains("ops/fix-clock.sh with sudo on macOS"),
            "{said}"
        );
        assert!(
            said.contains("ops/fix-clock.ps1 as Administrator on Windows"),
            "{said}"
        );
    }

    #[tokio::test]
    async fn an_offset_too_large_to_negate_is_refused_rather_than_wrapped() {
        // `i64::MIN.abs()` is not an `i64`: a debug build panics on it, and a
        // release build -- overflow checks off -- hands back `i64::MIN`, which
        // is negative and so never past the limit. The most extreme offset
        // there is would pass the gate. Every offset past the limit is clock
        // skew, on either side, out to both ends of the range.
        for offset_ms in [
            i64::MIN,
            i64::MIN + 1,
            -(MAX_CLOCK_SKEW_MS + 1),
            MAX_CLOCK_SKEW_MS + 1,
            i64::MAX,
        ] {
            let why = client()
                .check_ready(offset_ms)
                .await
                .expect_err("must refuse");
            assert_eq!(
                why,
                NotReady::ClockSkew {
                    offset_ms,
                    allowed_ms: MAX_CLOCK_SKEW_MS
                },
                "{offset_ms}"
            );
        }
    }

    #[test]
    fn a_signature_is_stripped_from_anything_that_might_be_logged() {
        // `reqwest`'s error Display carries the URL, and the URL carries the
        // signature. Not the private key and not reversible to it, but
        // per-request authentication material with no business outliving the
        // request.
        let noisy = "error sending request for url \
                     (https://api.binance.com/api/v3/account?timestamp=1&signature=deadbeef1234): \
                     connection closed";
        let clean = redact_signature(noisy);
        assert!(!clean.contains("deadbeef1234"), "{clean}");
        assert!(clean.contains("signature=<redacted>"), "{clean}");
        assert!(
            clean.contains("connection closed"),
            "the rest survives: {clean}"
        );
    }

    #[test]
    fn a_whole_base64_signature_is_stripped_not_only_a_hex_prefix() {
        // A real signed query: what this client sends for `GET /api/v3/account`
        // at this instant under Binance's published example key. Its signature
        // is percent-encoded base64 beginning with `x`, which is not hex, so a
        // hex-only rule redacts none of it.
        let query = client().signed_query("", 1_700_000_000_000);
        let signature = query
            .split_once("&signature=")
            .map(|(_, s)| s)
            .expect("signed");
        assert!(signature.starts_with("xmyZ"), "{signature}");
        let noisy = format!(
            "error sending request for url (https://api.binance.com/api/v3/account?{query})"
        );
        let clean = redact_signature(&noisy);
        for piece in fixture::eight_char_windows(signature) {
            assert!(!clean.contains(piece), "{piece} survived: {clean}");
        }
        assert_eq!(
            clean,
            "error sending request for url (https://api.binance.com/api/v3/account?\
             timestamp=1700000000000&recvWindow=5000&signature=<redacted>)"
        );
    }

    #[test]
    fn a_signature_runs_to_the_next_separator_and_every_one_is_stripped() {
        // Raw base64 as well as encoded, ended by each thing that can follow a
        // value in a log line, and more than one in a line.
        for (text, expected) in [
            ("a?signature=Ab+/9=&b=1", "a?signature=<redacted>&b=1"),
            ("signature=Ab+/9= next", "signature=<redacted> next"),
            ("signature=Ab+/9=\tnext", "signature=<redacted>\tnext"),
            ("\"signature=Ab+/9=\" x", "\"signature=<redacted>\" x"),
            ("'signature=Ab%2B9%3D' x", "'signature=<redacted>' x"),
            ("(signature=Ab+/9=) x", "(signature=<redacted>) x"),
            ("ends signature=Ab+/9=", "ends signature=<redacted>"),
            (
                "signature=AAAA&x=1, then signature=BBBB",
                "signature=<redacted>&x=1, then signature=<redacted>",
            ),
        ] {
            assert_eq!(redact_signature(text), expected, "{text}");
        }
    }

    #[test]
    fn text_with_no_signature_is_left_alone() {
        assert_eq!(redact_signature("dns error"), "dns error");
    }

    #[test]
    fn a_refusal_is_sorted_by_its_status_and_then_by_the_venues_code() {
        // The venue's error document as sent, and as read back.
        let doc = |code: i64, msg: &str| format!(r#"{{"code":{code},"msg":"{msg}"}}"#);
        let read = |code: i64, msg: &str| {
            Some(VenueError {
                code,
                msg: msg.to_owned(),
            })
        };
        let creds = |status, venue| NotReady::BadCredentials { status, venue };
        let limited = |status, retry_after: Option<&str>, venue| NotReady::RateLimited {
            status,
            retry_after: retry_after.map(str::to_owned),
            venue,
        };
        let failed = |status, venue| NotReady::VenueFailure { status, venue };
        let refused = |status, venue| NotReady::Refused { status, venue };
        // The venue's own messages for these codes, the -1003 one with its
        // placeholders filled in.
        let key = "Invalid API-key, IP, or permissions for action."; // -2015, errors.md:342
        let format = "API-key format invalid."; // -2014, errors.md:339
        let signature = "Signature for this request is not valid."; // -1022, errors.md:58
        let weight = "Too much request weight used; current limit is 6000 request weight per 1 \
                      MINUTE. Please use WebSocket Streams for live updates to avoid polling the \
                      API."; // -1003, errors.md:24
        let busy = "Server is currently overloaded with other requests. Please try again in a few \
                    minutes."; // -1008, errors.md:34
        let stale = "Timestamp for this request is outside of the recvWindow."; // -1021, errors.md:54
        let html = "<html><body>403 Forbidden</body></html>".to_owned();
        let none = String::new();

        let cases = [
            // The credentials: 401 whatever the body says, and the three codes
            // under any status that does not decide first (403, 418, 429 and
            // 5xx, below).
            (401, None, doc(-2015, key), creds(401, read(-2015, key))),
            (401, None, none.clone(), creds(401, None)),
            (
                400,
                None,
                doc(-2014, format),
                creds(400, read(-2014, format)),
            ),
            (400, None, doc(-2015, key), creds(400, read(-2015, key))),
            (
                400,
                None,
                doc(-1022, signature),
                creds(400, read(-1022, signature)),
            ),
            // The firewall, whose body is not the venue's error document.
            (403, None, html.clone(), NotReady::Firewall { venue: None }),
            // Rate limits and bans, with the header when there is one.
            (
                429,
                Some("37"),
                doc(-1003, weight),
                limited(429, Some("37"), read(-1003, weight)),
            ),
            (
                418,
                Some("120"),
                none.clone(),
                limited(418, Some("120"), None),
            ),
            (429, None, none.clone(), limited(429, None, None)),
            // The venue's own failures.
            (500, None, none.clone(), failed(500, None)),
            (503, None, doc(-1008, busy), failed(503, read(-1008, busy))),
            // The order of the rule rather than a thing seen: each status that
            // says the request was never judged on its credentials decides
            // before the code does, whichever of the three codes comes with it.
            (
                503,
                None,
                doc(-1022, signature),
                failed(503, read(-1022, signature)),
            ),
            (
                403,
                None,
                doc(-2015, key),
                NotReady::Firewall {
                    venue: read(-2015, key),
                },
            ),
            (
                429,
                None,
                doc(-2014, format),
                limited(429, None, read(-2014, format)),
            ),
            (
                418,
                None,
                doc(-1022, signature),
                limited(418, None, read(-1022, signature)),
            ),
            // Anything else, explained by the venue or not.
            (
                400,
                None,
                doc(-1021, stale),
                refused(400, read(-1021, stale)),
            ),
            (404, None, html, refused(404, None)),
        ];
        for (status, retry_after, body, expected) in cases {
            assert_eq!(
                classify_refusal(status, retry_after.map(str::to_owned), body.as_bytes()),
                expected,
                "{status} {body}"
            );
        }
    }

    #[test]
    fn each_kind_of_refusal_says_something_different_with_the_venues_own_words() {
        let venue = Some(VenueError {
            code: -9999,
            msg: "the venue's own words".to_owned(),
        });
        let said: Vec<String> = [
            NotReady::BadCredentials {
                status: 401,
                venue: venue.clone(),
            },
            NotReady::Firewall {
                venue: venue.clone(),
            },
            NotReady::RateLimited {
                status: 429,
                retry_after: Some("37".to_owned()),
                venue: venue.clone(),
            },
            NotReady::RateLimited {
                status: 418,
                retry_after: None,
                venue: venue.clone(),
            },
            NotReady::VenueFailure {
                status: 502,
                venue: venue.clone(),
            },
            NotReady::Refused {
                status: 400,
                venue: venue.clone(),
            },
            NotReady::KeyCannotTrade {
                status: 401,
                venue: venue.clone(),
            },
            NotReady::ProbeRefused {
                status: 400,
                venue: venue.clone().expect("a venue error"),
            },
            NotReady::ProbeFiltered {
                status: 400,
                filter: FilterName::Notional,
                venue: venue.clone().expect("a venue error"),
            },
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        for text in &said {
            assert!(text.contains("code -9999: the venue's own words"), "{text}");
        }
        // What comes before the status is what the operator acts on, and no
        // two kinds share it.
        let openings: std::collections::HashSet<&str> = said
            .iter()
            .map(|s| s.split(" (HTTP").next().unwrap_or(s))
            .collect();
        assert_eq!(openings.len(), said.len(), "{said:#?}");
        assert!(said[0].contains("BINANCE_API_KEY"), "{}", said[0]);
        assert!(said[0].contains("IP"), "{}", said[0]);
        assert!(said[1].contains("firewall"), "{}", said[1]);
        assert!(said[2].contains("Retry-After says 37"), "{}", said[2]);
        assert!(said[3].contains("banned"), "{}", said[3]);
        assert!(!said[3].contains("Retry-After"), "{}", said[3]);
        assert!(said[4].contains("own side"), "{}", said[4]);
        // Without the venue's document, nothing stands in for it.
        assert_eq!(
            NotReady::Refused {
                status: 400,
                venue: None
            }
            .to_string(),
            "the venue refused a signed request (HTTP 400)"
        );
    }

    #[test]
    fn can_trade_is_read_from_the_account_and_nothing_of_the_body_is_said() {
        assert_eq!(account_can_trade(account("true").as_bytes()), Ok(()));
        let refusals = [
            (account("false"), NotReady::CannotTrade),
            (
                account("\"true\""),
                NotReady::UnreadableAccount(AccountFault::CanTradeNotBool),
            ),
            (
                format!(r#"{{"balances":[{{"asset":"BTC","free":"{BALANCE}"}}]}}"#),
                NotReady::UnreadableAccount(AccountFault::NoCanTrade),
            ),
            (
                format!("<html>{BALANCE}</html>"),
                NotReady::UnreadableAccount(AccountFault::NotJson),
            ),
        ];
        for (body, expected) in refusals {
            let why = account_can_trade(body.as_bytes()).expect_err("not ready");
            assert_eq!(why, expected, "{body}");
            let said = why.to_string();
            assert!(!said.contains(BALANCE), "{said}");
            assert!(!format!("{why:?}").contains(BALANCE), "{why:?}");
        }
        // An account that reports it cannot trade has its own reason. The
        // venue does not define the field, so the reason claims nothing about
        // whose the `false` is, and names both remedies: the venue's support,
        // for an account whose trading is disabled, and API Management, for a
        // key's permission to trade.
        let said = NotReady::CannotTrade.to_string();
        for needle in [
            "the account reports canTrade false",
            "The venue does not define the field",
            "customer support",
            "API Management",
        ] {
            assert!(said.contains(needle), "{needle:?} missing from: {said}");
        }
    }

    #[tokio::test]
    async fn a_request_that_gets_no_answer_is_reported_without_its_url_or_signature() {
        // The server reads the request and hangs up without a word, which
        // `reqwest` reports with the URL in its text -- and this URL carries
        // the signature.
        let (base, server) = serve_once(String::new());
        let why = client_at(&base)
            .check_ready(0)
            .await
            .expect_err("no answer is not ready");
        let request = received(server);
        signed_on_the_wire(&request);
        let request_line = request.lines().next().unwrap_or_default();
        let signature = request_line
            .split_once("&signature=")
            .and_then(|(_, rest)| rest.split(' ').next())
            .expect("the request carried a signature");
        let NotReady::Unreachable(cause) = &why else {
            panic!("expected unreachable, got {why:?}")
        };
        for text in [cause.clone(), why.to_string()] {
            for absent in ["signature", "/api/v3/account", "http://", "timestamp="] {
                assert!(!text.contains(absent), "{absent:?} in {text}");
            }
            for piece in fixture::eight_char_windows(signature) {
                assert!(!text.contains(piece), "{piece} in {text}");
            }
        }
        // And it still says what went wrong, from the error's own sources
        // rather than from a URL.
        assert!(
            cause.starts_with("transport error: error sending request: "),
            "{cause}"
        );
    }

    /// One of the three checks venue-check makes, by number, against `client`.
    async fn check_number(client: &TradeClient, which: usize) -> Result<(), NotReady> {
        match which {
            0 => client.check_ready(0).await,
            1 => client.check_trade_permission(&order_on("BTCUSDT"), 0).await,
            _ => client.check_bnb_discount_off("BTCUSDT", 0).await,
        }
    }

    #[tokio::test]
    async fn a_success_whose_body_is_cut_short_says_the_venue_answered() {
        // Each check's venue answers 200 with a body that would pass it, and
        // the body is cut short: broken off under a Content-Length promising
        // more than arrives, or held open past the client's timeout after the
        // head. The venue was reached, so "could not reach the venue" would
        // send the operator after a network that delivered an answer, and
        // nothing is read from the part that arrived.
        let passing = [
            account("true"),
            "{}".to_owned(),
            commission("false", "false"),
        ];
        for (which, body) in passing.iter().enumerate() {
            let broken = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len() + 64
            );
            let stalled = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            for (reply, cause) in [
                (Reply::Bytes(broken), "could not read the response body: "),
                (Reply::Stall(stalled), "request timed out"),
            ] {
                let venue = TestVenue::start(vec![reply]);
                let client = TradeClient::new(
                    venue.base.clone(),
                    Duration::from_millis(500),
                    fixture::example(),
                )
                .expect("builds");
                let answer = check_number(&client, which).await;
                let requests = venue.stop();
                assert_eq!(requests.len(), 1, "check {which}: {requests:#?}");
                let Err(NotReady::AnswerCutShort { status: 200, why }) = &answer else {
                    panic!("check {which}: expected an answer cut short, got {answer:?}")
                };
                assert!(why.starts_with(cause), "check {which}: {why}");
                let said = answer.expect_err("not ready").to_string();
                assert!(
                    said.starts_with("the venue answered HTTP 200 and the body was cut short ("),
                    "check {which}: {said}"
                );
                assert!(said.contains("The venue was reached"), "{said}");
                assert!(!said.contains("could not reach"), "{said}");
                says_nothing_secret(&said, &requests[0]);
                assert!(!said.contains("/api/v3/account"), "{said}");
            }
        }
    }

    #[tokio::test]
    async fn a_rate_limit_is_read_from_its_status_its_retry_after_and_its_body() {
        let body = r#"{"code":-1003,"msg":"Too much request weight used; current limit is 6000 request weight per 1 MINUTE."}"#;
        let (base, server) = serve_once(http("429 Too Many Requests", "Retry-After: 37\r\n", body));
        let why = client_at(&base).check_ready(0).await.expect_err("refused");
        signed_on_the_wire(&received(server));
        assert_eq!(
            why,
            NotReady::RateLimited {
                status: 429,
                retry_after: Some("37".to_owned()),
                venue: Some(VenueError {
                    code: -1003,
                    msg:
                        "Too much request weight used; current limit is 6000 request weight per 1 \
                          MINUTE."
                            .to_owned(),
                }),
            }
        );
    }

    /// An account document holding `entries`, each `(asset, free, locked)`
    /// as written.
    fn holding(entries: &[(&str, &str, &str)]) -> String {
        let listed: Vec<String> = entries
            .iter()
            .map(|(asset, free, locked)| {
                format!(r#"{{"asset":"{asset}","free":"{free}","locked":"{locked}"}}"#)
            })
            .collect();
        format!(
            r#"{{"canTrade":true,"accountType":"SPOT","balances":[{}],"permissions":["SPOT"]}}"#,
            listed.join(",")
        )
    }

    #[test]
    fn balances_are_read_for_the_two_assets_straight_to_fixed_point() {
        // Distinct amounts in all four places, so a field read from the wrong
        // asset, or `free` for `locked`, shows.
        let body = holding(&[
            ("BNB", "9.00000000", "0.00000000"),
            ("BTC", "0.01234567", "0.00000011"),
            ("USDT", "1000.12345678", "2.50000000"),
        ]);
        let read = read_balances(body.as_bytes(), "BTC", "USDT").expect("both stated");
        assert_eq!(
            (
                read.base.free.to_string(),
                read.base.locked.to_string(),
                read.quote.free.to_string(),
                read.quote.locked.to_string(),
            ),
            (
                "0.01234567".to_owned(),
                "0.00000011".to_owned(),
                "1000.12345678".to_owned(),
                "2.5".to_owned(),
            )
        );
    }

    #[test]
    fn a_balance_not_stated_once_and_readably_is_no_baseline_and_says_no_amount() {
        let cases = [
            (
                holding(&[("USDT", BALANCE, "0")]),
                BalanceFault::Missing("BTC".to_owned()),
            ),
            (
                holding(&[("BTC", BALANCE, "0"), ("BTC", "1", "0"), ("USDT", "1", "0")]),
                BalanceFault::Repeated("BTC".to_owned()),
            ),
            (
                holding(&[("BTC", BALANCE, "0"), ("USDT", "1e3", "0")]),
                BalanceFault::Unreadable {
                    asset: "USDT".to_owned(),
                    field: "free",
                },
            ),
            (
                format!(
                    r#"{{"balances":[{{"asset":"BTC","free":"{BALANCE}"}},{{"asset":"USDT","free":"1","locked":"0"}}]}}"#
                ),
                BalanceFault::Unreadable {
                    asset: "BTC".to_owned(),
                    field: "locked",
                },
            ),
            (
                account("true").replace("balances", "holdings"),
                BalanceFault::NoBalances,
            ),
            (format!("<html>{BALANCE}</html>"), BalanceFault::NoBalances),
        ];
        for (body, expected) in cases {
            let fault = read_balances(body.as_bytes(), "BTC", "USDT").expect_err("no baseline");
            assert_eq!(fault, expected, "{body}");
            let said = NotReady::UnreadableBalances(fault).to_string();
            assert!(!said.contains(BALANCE), "{said}");
            assert!(said.contains("no baseline"), "{said}");
        }
    }

    #[tokio::test]
    async fn balances_are_asked_for_with_the_signed_account_request() {
        let body = holding(&[("BTC", "0.5", "0"), ("USDT", "1000", "0")]);
        let (base, server) = serve_once(http("200 OK", "", &body));
        let read = client_at(&base)
            .balances("BTC", "USDT", 0)
            .await
            .expect("both stated");
        let request = received(server);
        assert_eq!(read.base.free.to_string(), "0.5");
        assert!(
            request.starts_with("GET /api/v3/account?timestamp="),
            "{request}"
        );
        signed_on_the_wire(&request);
        // A refusal is sorted as the account probe's is.
        let (base, server) = serve_once(http("401 Unauthorized", "", ""));
        let why = client_at(&base)
            .balances("BTC", "USDT", 0)
            .await
            .expect_err("refused");
        received(server);
        assert!(
            matches!(why, NotReady::BadCredentials { status: 401, .. }),
            "{why:?}"
        );
    }

    #[tokio::test]
    async fn an_account_that_cannot_trade_is_not_ready_and_one_that_can_is_ready() {
        let (base, server) = serve_once(http("200 OK", "", &account("false")));
        let why = client_at(&base)
            .check_ready(0)
            .await
            .expect_err("cannot trade");
        let request = received(server);
        assert_eq!(why, NotReady::CannotTrade);
        assert!(!why.to_string().contains(BALANCE), "{why}");
        // The request it answered was the signed account probe, with its key,
        // and with a signature that verifies over the query it arrived with.
        assert!(
            request.starts_with("GET /api/v3/account?timestamp="),
            "{request}"
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("x-mbx-apikey: the-api-key\r\n"),
            "{request}"
        );
        let payload = signed_on_the_wire(&request);
        assert!(
            payload.starts_with("timestamp=") && payload.ends_with("&recvWindow=5000"),
            "{payload}"
        );

        let (base, server) = serve_once(http("200 OK", "", &account("true")));
        assert_eq!(client_at(&base).check_ready(0).await, Ok(()));
        signed_on_the_wire(&received(server));
    }

    /// A redirect to `location` on the same venue, of the kind that keeps the
    /// method: a client that follows it sends the same request again.
    fn redirect(location: &str) -> String {
        http(
            "307 Temporary Redirect",
            &format!("Location: {location}\r\n"),
            "",
        )
    }

    #[tokio::test]
    async fn a_redirect_is_the_answer_and_is_never_followed() {
        // Followed, a 307 sends the request again, wherever it points, and for
        // an order that is a second order. Every request this client makes is
        // sent once, so the redirect is what it answers with. Here the venue
        // would have said the account can trade, had the client asked again.
        let venue = TestVenue::start(vec![
            Reply::Bytes(redirect("/api/v3/account/elsewhere")),
            Reply::Bytes(http("200 OK", "", &account("true"))),
        ]);
        let answer = client_at(&venue.base).check_ready(0).await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "{requests:#?}");
        assert_eq!(
            answer,
            Err(NotReady::Refused {
                status: 307,
                venue: None
            })
        );
    }

    /// The documentation's own non-ASCII symbol (rest-api.md:582), whose
    /// encoded form is all `%`s (rest-api.md:618). An order on it is the one
    /// whose query a second encoding would change.
    const NON_ASCII: &str = "１２３４５６";

    /// When the strategy submitted, on the venue's clock: years before any
    /// run of this test, so a transport that stamped at sending could not
    /// match it by accident.
    const STAMP: i64 = 1_700_000_000_000;

    fn on(symbol: &str) -> InstrumentDef {
        InstrumentDef {
            exchange: Exchange::Binance,
            symbol: symbol.to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.01".parse().expect("price"),
            lot_size: "0.00001".parse().expect("quantity"),
            min_notional: "5".parse().expect("notional"),
        }
    }

    fn run() -> RunTag {
        RunTag::new("r1").expect("a valid tag")
    }

    /// Our order 7 on `symbol`: a market buy of 0.001, the shape `MaCrossover`
    /// sends.
    fn order_on(symbol: &str) -> NewOrder {
        let instrument = on(symbol);
        let request = OrderRequest {
            instrument: InstrumentRegistry::new().register(instrument.clone()),
            side: Side::Buy,
            qty: "0.001".parse().expect("quantity"),
            kind: OrderKind::Market,
            time_in_force: TimeInForce::Gtc,
        };
        NewOrder::encode(&request, ClientOrderId(7), &run(), &instrument).expect("encodes")
    }

    /// A cancel of our order 7 on `symbol`.
    fn cancel_on(symbol: &str) -> CancelOrder {
        CancelOrder::encode(ClientOrderId(7), &run(), &on(symbol)).expect("encodes")
    }

    /// The documented acknowledgement (rest-api.md:2205-2211), its comment
    /// removed and its `clientOrderId` made our order 7's.
    const OUR_ACK: &str = r#"{"symbol":"BTCUSDT","orderId":28,"orderListId":-1,"clientOrderId":"q-r1-7","transactTime":1507725176595}"#;

    /// The documented answer to a cancel (rest-api.md:2410-2427), cut down,
    /// its `origClientOrderId` made our order 7's.
    const OUR_CANCEL: &str = r#"{"symbol":"BTCUSDT","origClientOrderId":"q-r1-7","orderId":28,"orderListId":-1,"clientOrderId":"cancelMyOrder1","transactTime":1684804350068,"status":"CANCELED"}"#;

    /// What [`OUR_ACK`] means.
    fn placed() -> Placement {
        Placement::Placed {
            order_id: VenueOrderId("28".to_owned()),
            client_order_id: "q-r1-7".to_owned(),
            transact_time_ms: 1_507_725_176_595,
        }
    }

    /// Whether `request` carries nothing after its head: no body declared, and
    /// none sent. The venue signs the query and the body together
    /// (rest-api.md:555), so with no body the query is the whole of it.
    fn has_no_body(request: &str) -> bool {
        let (head, after) = request.split_once("\r\n\r\n").expect("a whole head");
        let head = head.to_ascii_lowercase();
        after.is_empty()
            && !head.contains("\r\ntransfer-encoding:")
            && !head.contains("\r\ncontent-type:")
            && head
                .lines()
                .filter(|line| line.starts_with("content-length:"))
                .all(|line| line == "content-length: 0")
    }

    /// A port on this machine that nothing listens on: bound, and let go.
    fn closed_port() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address");
        drop(listener);
        format!("http://{address}")
    }

    /// That `said` is a description fit for a log: it names neither the URL
    /// nor anything of the signature `request` carried.
    fn says_nothing_secret(said: &str, request: &str) {
        for absent in ["signature", "/api/v3/order", "http://", "timestamp="] {
            assert!(!said.contains(absent), "{absent:?} in {said}");
        }
        if let Some(signature) = request
            .lines()
            .next()
            .and_then(|line| line.split_once("&signature="))
            .and_then(|(_, rest)| rest.split(' ').next())
        {
            for piece in fixture::eight_char_windows(signature) {
                assert!(!said.contains(piece), "{piece} in {said}");
            }
        }
    }

    #[tokio::test]
    async fn an_order_and_a_cancel_go_out_exactly_as_encoded_and_signed() {
        // Each encoded query holds `%`s, which a second encoding would turn
        // into `%25`s, and the venue would check the signature against bytes
        // nobody signed. Each must arrive as encoded, followed by exactly the
        // caller's stamp and the window, with a signature over all of that,
        // and no body.
        let order = order_on(NON_ASCII);
        let cancel = cancel_on(NON_ASCII);
        for query in [order.query(), cancel.query()] {
            assert!(
                query.contains("%EF%BC%91"),
                "the fixture must exercise %: {query}"
            );
        }

        let venue = TestVenue::start(vec![Reply::Bytes(http("200 OK", "", OUR_ACK))]);
        let placement = client_at(&venue.base).place_order(&order, STAMP).await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "{requests:#?}");
        assert!(
            requests[0].starts_with("POST /api/v3/order?"),
            "{}",
            requests[0]
        );
        assert_eq!(
            signed_on_the_wire(&requests[0]),
            format!(
                "{}&timestamp={STAMP}&recvWindow={RECV_WINDOW_MS}",
                order.query()
            )
        );
        assert!(has_no_body(&requests[0]), "{}", requests[0]);
        assert_eq!(placement, placed());

        let venue = TestVenue::start(vec![Reply::Bytes(http("200 OK", "", OUR_CANCEL))]);
        let reply = client_at(&venue.base).cancel_order(&cancel, STAMP).await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "{requests:#?}");
        assert!(
            requests[0].starts_with("DELETE /api/v3/order?"),
            "{}",
            requests[0]
        );
        assert_eq!(
            signed_on_the_wire(&requests[0]),
            format!(
                "{}&timestamp={STAMP}&recvWindow={RECV_WINDOW_MS}",
                cancel.query()
            )
        );
        assert!(has_no_body(&requests[0]), "{}", requests[0]);
        assert_eq!(
            reply,
            CancelReply::Acknowledged {
                order_id: VenueOrderId("28".to_owned())
            }
        );
    }

    #[tokio::test]
    async fn a_redirected_order_is_unknown_and_never_sent_again() {
        // The venue redirects once, and would then acknowledge the order: a
        // client that followed would have placed it, or placed it twice. The
        // 307 is the answer instead, and settles nothing about either request.
        let order = order_on("BTCUSDT");
        let venue = TestVenue::start(vec![
            Reply::Bytes(redirect("/api/v3/order/elsewhere")),
            Reply::Bytes(http("200 OK", "", OUR_ACK)),
        ]);
        let placement = client_at(&venue.base).place_order(&order, STAMP).await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "{requests:#?}");
        assert_eq!(
            placement,
            Placement::Unknown {
                why: WhyUnknown::UnexpectedStatus { http_status: 307 }
            }
        );

        let cancel = cancel_on("BTCUSDT");
        let venue = TestVenue::start(vec![
            Reply::Bytes(redirect("/api/v3/order/elsewhere")),
            Reply::Bytes(http("200 OK", "", OUR_CANCEL)),
        ]);
        let reply = client_at(&venue.base).cancel_order(&cancel, STAMP).await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "{requests:#?}");
        assert_eq!(
            reply,
            CancelReply::Unknown {
                why: WhyUnknown::UnexpectedStatus { http_status: 307 }
            }
        );
    }

    #[tokio::test]
    async fn an_order_that_never_connected_was_not_sent() {
        // A connection refused: not a byte of the request left this machine,
        // so the order certainly was not placed, and the engine may hear it
        // as rejected. The one outcome that may say so.
        let base = closed_port();
        let client = client_at(&base);
        let placement = client.place_order(&order_on("BTCUSDT"), STAMP).await;
        let Placement::NotSent { why } = &placement else {
            panic!("expected not sent, got {placement:?}")
        };
        says_nothing_secret(why, "");
        assert_eq!(placement.reject_reason(), Some(RejectReason::Venue));

        let reply = client.cancel_order(&cancel_on("BTCUSDT"), STAMP).await;
        let CancelReply::NotSent { why } = &reply else {
            panic!("expected not sent, got {reply:?}")
        };
        says_nothing_secret(why, "");
    }

    #[tokio::test]
    async fn an_order_the_venue_took_and_never_answered_is_unknown() {
        // The venue read the request, so the order may exist: whatever else
        // this is, it is never "not sent". The client's own timeout ends the
        // wait, shortened here so the test does not take twenty seconds.
        let venue = TestVenue::start(vec![Reply::Silence]);
        let client = client_at(&venue.base).with_order_timeout(Duration::from_millis(300));
        let placement = client.place_order(&order_on("BTCUSDT"), STAMP).await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "the venue had the order: {requests:#?}");
        assert_eq!(
            placement,
            Placement::Unknown {
                why: WhyUnknown::TimedOut
            }
        );
        assert_eq!(placement.reject_reason(), None);
    }

    #[tokio::test]
    async fn an_acknowledgement_cut_short_is_never_a_placement() {
        // The venue's whole acknowledgement of our order, readable as it
        // stands, under a Content-Length that promises more than arrives
        // before the connection closes. A transport that classified what did
        // arrive would call the order placed, on an answer that broke off.
        let cut = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{OUR_ACK}",
            OUR_ACK.len() + 64
        );
        let venue = TestVenue::start(vec![Reply::Bytes(cut)]);
        let placement = client_at(&venue.base)
            .place_order(&order_on("BTCUSDT"), STAMP)
            .await;
        let requests = venue.stop();
        let Placement::Unknown {
            why: WhyUnknown::Interrupted(why),
        } = &placement
        else {
            panic!("expected an interrupted answer, got {placement:?}")
        };
        // It broke off in the body, after the status had arrived.
        assert!(why.contains("body"), "{why}");
        says_nothing_secret(why, &requests[0]);
    }

    #[tokio::test]
    async fn an_order_or_a_cancel_the_venue_read_and_hung_up_on_is_unknown_never_not_sent() {
        // The venue read the whole request and closed the connection without
        // a word. The request had left this machine, so the order may exist
        // and the cancel may have taken effect: the outcome is unknown, and
        // the one thing it must never be is "not sent", which the engine
        // would hear as a rejection. Nor is it sent again.
        let venue = TestVenue::start(Vec::new());
        let placement = client_at(&venue.base)
            .place_order(&order_on("BTCUSDT"), STAMP)
            .await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "the venue had the order: {requests:#?}");
        let Placement::Unknown {
            why: WhyUnknown::Interrupted(why),
        } = &placement
        else {
            panic!("expected an interrupted answer, got {placement:?}")
        };
        says_nothing_secret(why, &requests[0]);
        assert_eq!(placement.reject_reason(), None);

        let venue = TestVenue::start(Vec::new());
        let reply = client_at(&venue.base)
            .cancel_order(&cancel_on("BTCUSDT"), STAMP)
            .await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "the venue had the cancel: {requests:#?}");
        let CancelReply::Unknown {
            why: WhyUnknown::Interrupted(why),
        } = &reply
        else {
            panic!("expected an interrupted answer, got {reply:?}")
        };
        says_nothing_secret(why, &requests[0]);
    }

    #[tokio::test]
    async fn an_answer_whose_body_never_finishes_arriving_is_a_timeout_never_not_sent() {
        // The venue sent its status line and headers, promising a body, and
        // then held the connection open without sending it. The status says
        // the request reached the venue; the wait ends on the client's own
        // timeout, shortened here, and the outcome is unknown, never "not
        // sent".
        let head = format!(
            "HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: {}

",
            OUR_ACK.len()
        );
        let venue = TestVenue::start(vec![Reply::Stall(head.clone())]);
        let client = client_at(&venue.base).with_order_timeout(Duration::from_millis(300));
        let placement = client.place_order(&order_on("BTCUSDT"), STAMP).await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "the venue had the order: {requests:#?}");
        assert_eq!(
            placement,
            Placement::Unknown {
                why: WhyUnknown::TimedOut
            }
        );
        assert_eq!(placement.reject_reason(), None);

        let venue = TestVenue::start(vec![Reply::Stall(head)]);
        let client = client_at(&venue.base).with_order_timeout(Duration::from_millis(300));
        let reply = client.cancel_order(&cancel_on("BTCUSDT"), STAMP).await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "the venue had the cancel: {requests:#?}");
        assert_eq!(
            reply,
            CancelReply::Unknown {
                why: WhyUnknown::TimedOut
            }
        );
    }

    #[tokio::test]
    async fn an_order_waits_for_the_venue_longer_than_the_client_otherwise_would() {
        // The venue answers -1007 after its own wait (rest-api.md:102) --
        // here, after longer than this client waits for anything else. An
        // order waits for it all the same, so the venue's answer, which says
        // the request reached it, is what comes back rather than a timeout of
        // ours, which says nothing. Shortened, both numbers, to keep the test
        // quick: the order's wait is the longer, as ORDER_TIMEOUT's is.
        let timeout = r#"{"code":-1007,"msg":"Timeout waiting for response from backend server. Send status unknown; execution status unknown."}"#;
        let venue = TestVenue::start(vec![Reply::Late(
            Duration::from_millis(600),
            http("400 Bad Request", "", timeout),
        )]);
        let client = TradeClient::new(
            venue.base.clone(),
            Duration::from_millis(200),
            fixture::example(),
        )
        .expect("builds")
        .with_order_timeout(Duration::from_secs(5));
        let placement = client.place_order(&order_on("BTCUSDT"), STAMP).await;
        venue.stop();
        assert!(
            matches!(
                &placement,
                Placement::Unknown {
                    why: WhyUnknown::Unsettled { code: -1007, .. }
                }
            ),
            "{placement:?}"
        );
    }

    #[tokio::test]
    async fn the_trade_probe_goes_to_the_test_endpoint_and_nowhere_else() {
        // It must place nothing. The path is pinned as written out here
        // (rest-api.md:2323), not read back from the constant, and it is not
        // the path that places an order.
        assert_eq!(ORDER_TEST_PATH, "/api/v3/order/test");
        assert_ne!(ORDER_TEST_PATH, ORDER_PATH);
        // And on the wire, whatever the base's spelling and whatever the
        // probe's symbol holds: the method takes no path, and the symbol is
        // percent-encoded, so neither can move the probe to /api/v3/order.
        for (suffix, symbol) in [
            ("", "BTCUSDT"),
            ("/", "BTCUSDT"),
            ("//", "BTCUSDT"),
            ("", "/../../../api/v3/order?symbol=BTCUSDT#"),
        ] {
            let probe = order_on(symbol);
            let venue = TestVenue::start(vec![Reply::Bytes(http("200 OK", "", "{}"))]);
            let answer = client_at(&format!("{}{suffix}", venue.base))
                .check_trade_permission(&probe, 0)
                .await;
            let requests = venue.stop();
            assert_eq!(requests.len(), 1, "{requests:#?}");
            let line = requests[0].lines().next().unwrap_or_default();
            let path = line
                .strip_prefix("POST ")
                .and_then(|rest| rest.split_once('?'))
                .map(|(path, _)| path);
            assert_eq!(path, Some("/api/v3/order/test"), "{suffix:?} {symbol:?}");
            let payload = signed_on_the_wire(&requests[0]);
            assert!(
                payload.starts_with(&format!("{}&timestamp=", probe.query())),
                "{payload}"
            );
            assert_eq!(answer, Ok(()), "{symbol:?}");
        }
    }

    /// The venue's error document, as sent.
    fn error_doc(code: i64, msg: &str) -> String {
        format!(r#"{{"code":{code},"msg":"{msg}"}}"#)
    }

    /// The venue's error document, as read back.
    fn error_read(code: i64, msg: &str) -> VenueError {
        VenueError {
            code,
            msg: msg.to_owned(),
        }
    }

    /// -2015's message (errors.md:342).
    const KEY_REFUSED: &str = "Invalid API-key, IP, or permissions for action.";

    /// A filter failure's (errors.md:433).
    const NOTIONAL: &str = "Filter failure: NOTIONAL";

    #[test]
    fn the_trade_probe_reads_the_keys_permission_off_the_answer() {
        let (doc, read, key, notional) = (error_doc, error_read, KEY_REFUSED, NOTIONAL);
        // The venue's own messages for these codes.
        let lot = "Filter failure: LOT_SIZE"; // errors.md:431
        let closed = "Market is closed."; // errors.md:376
        let disabled = "This account may not place or cancel orders."; // errors.md:388
        let duplicate = "Duplicate order sent."; // errors.md:375

        let cannot = |status, venue| Err(NotReady::KeyCannotTrade { status, venue });
        let filtered = |filter, code, msg| {
            Err(NotReady::ProbeFiltered {
                status: 400,
                filter,
                venue: read(code, msg),
            })
        };
        let probe_refused = |code, msg| {
            Err(NotReady::ProbeRefused {
                status: 400,
                venue: read(code, msg),
            })
        };
        let unreadable = Err(NotReady::UnreadableOrderTest { status: 200 });
        let cases: [(u16, String, Result<(), NotReady>); 11] = [
            // Validated, and nothing to refuse (rest-api.md:2349-2353): the
            // one answer that is ready.
            (200, "{}".to_owned(), Ok(())),
            // A 2xx that is not the venue's answer shows nothing either way.
            (200, "<html>Sign in".to_owned(), unreadable.clone()),
            (200, "[]".to_owned(), unreadable),
            // The key may not trade: 401 whatever the body says, and -2015
            // under any status that does not decide first.
            (401, doc(-2015, key), cannot(401, Some(read(-2015, key)))),
            (401, String::new(), cannot(401, None)),
            (400, doc(-2015, key), cannot(400, Some(read(-2015, key)))),
            // A filter failure, under either code: not ready, and the filter
            // the venue cited is kept.
            (
                400,
                doc(-1013, notional),
                filtered(FilterName::Notional, -1013, notional),
            ),
            (
                400,
                doc(-2010, lot),
                filtered(FilterName::LotSize, -2010, lot),
            ),
            // A verdict for any other reason: not ready either, since the run
            // may meet the same refusal.
            (400, doc(-1013, closed), probe_refused(-1013, closed)),
            (400, doc(-2010, disabled), probe_refused(-2010, disabled)),
            (400, doc(-1013, duplicate), probe_refused(-1013, duplicate)),
        ];
        for (status, body, expected) in cases {
            assert_eq!(
                read_order_test(status, None, body.as_bytes()),
                expected,
                "{status} {body}"
            );
        }
    }

    #[test]
    fn a_trade_probe_answer_that_settles_nothing_is_reported_as_check_ready_reports_it() {
        let (doc, read, key, notional) = (error_doc, error_read, KEY_REFUSED, NOTIONAL);
        let signature = "Signature for this request is not valid."; // -1022, errors.md:58
        let stale = "Timestamp for this request is outside of the recvWindow."; // -1021, errors.md:54
        let missing = "Mandatory parameter 'quantity' was not sent, was empty/null, or malformed."; // -1102, errors.md:83
        let cases: [(u16, String, NotReady); 6] = [
            // Never judged at all, whatever code comes with it: a -2015 or a
            // filter failure here says nothing about the key.
            (
                403,
                doc(-2015, key),
                NotReady::Firewall {
                    venue: Some(read(-2015, key)),
                },
            ),
            (
                429,
                doc(-1013, notional),
                NotReady::RateLimited {
                    status: 429,
                    retry_after: None,
                    venue: Some(read(-1013, notional)),
                },
            ),
            (
                503,
                String::new(),
                NotReady::VenueFailure {
                    status: 503,
                    venue: None,
                },
            ),
            // The rest, as check_ready reports a refusal: a bad signature,
            // a stale stamp, and a request the venue could not read, which
            // here would mean the encoder's order is not one it takes.
            (
                400,
                doc(-1022, signature),
                NotReady::BadCredentials {
                    status: 400,
                    venue: Some(read(-1022, signature)),
                },
            ),
            (
                400,
                doc(-1021, stale),
                NotReady::Refused {
                    status: 400,
                    venue: Some(read(-1021, stale)),
                },
            ),
            (
                400,
                doc(-1102, missing),
                NotReady::Refused {
                    status: 400,
                    venue: Some(read(-1102, missing)),
                },
            ),
        ];
        for (status, body, expected) in cases {
            assert_eq!(
                read_order_test(status, None, body.as_bytes()),
                Err(expected),
                "{status} {body}"
            );
        }
    }

    #[tokio::test]
    async fn a_refused_trade_probe_is_read_with_its_status_header_and_body() {
        // Through the client, not only the reader: the 401 with -2015 the venue
        // pairs (web-socket-api.md:1324-1327), and a rate limit's Retry-After.
        let key = r#"{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}"#;
        let venue = TestVenue::start(vec![Reply::Bytes(http("401 Unauthorized", "", key))]);
        let answer = client_at(&venue.base)
            .check_trade_permission(&order_on("BTCUSDT"), 0)
            .await;
        venue.stop();
        assert_eq!(
            answer,
            Err(NotReady::KeyCannotTrade {
                status: 401,
                venue: Some(VenueError {
                    code: -2015,
                    msg: "Invalid API-key, IP, or permissions for action.".to_owned(),
                }),
            })
        );

        let venue = TestVenue::start(vec![Reply::Bytes(http(
            "429 Too Many Requests",
            "Retry-After: 37\r\n",
            "",
        ))]);
        let answer = client_at(&venue.base)
            .check_trade_permission(&order_on("BTCUSDT"), 0)
            .await;
        venue.stop();
        assert_eq!(
            answer,
            Err(NotReady::RateLimited {
                status: 429,
                retry_after: Some("37".to_owned()),
                venue: None,
            })
        );
    }

    #[test]
    fn what_the_trade_probe_says_names_the_remedy_and_calls_a_refusal_a_reading_of_permission() {
        let said = NotReady::KeyCannotTrade {
            status: 401,
            venue: Some(VenueError {
                code: -2015,
                msg: "Invalid API-key, IP, or permissions for action.".to_owned(),
            }),
        }
        .to_string();
        for needle in [
            "refused this API key a TRADE request",
            "(HTTP 401, code -2015: Invalid API-key, IP, or permissions for action.)",
            "API Management",
            "IP restriction",
        ] {
            assert!(said.contains(needle), "{needle:?} missing from: {said}");
        }
        let said = NotReady::ProbeRefused {
            status: 400,
            venue: VenueError {
                code: -1013,
                msg: "Market is closed.".to_owned(),
            },
        }
        .to_string();
        for needle in [
            "(HTTP 400, code -1013: Market is closed.)",
            "an order of the run's may meet the same refusal",
            // What the refusal suggests about the key, and that it is a
            // reading of the documentation, not a proof: nothing in it states
            // the order of the venue's checks.
            "suggests it accepted the signature and this key's permission to trade",
            "does not state the order of its checks",
            "the Spot Testnet rehearsal must confirm it",
            A_VERDICT_SUGGESTS_PERMISSION,
        ] {
            assert!(said.contains(needle), "{needle:?} missing from: {said}");
        }
        assert!(!said.contains("prove"), "{said}");
    }

    #[tokio::test]
    async fn a_filter_failure_on_the_trade_probe_is_not_ready_and_names_the_filter() {
        // The probe is the run's own order, and nothing else of the caller's
        // reaches the request, so a filter that refuses it refuses the run's
        // orders too: one that judges the order's numbers, NOTIONAL
        // (errors.md:433), and one that judges the account, MAX_NUM_ORDERS,
        // too many orders open on the symbol (errors.md:437;
        // filters.md:217-219), which no change to the probe would pass.
        // Through the client, as venue-check meets it.
        for (name, filter) in [
            ("NOTIONAL", FilterName::Notional),
            ("MAX_NUM_ORDERS", FilterName::MaxNumOrders),
        ] {
            let msg = format!("Filter failure: {name}");
            let venue = TestVenue::start(vec![Reply::Bytes(http(
                "400 Bad Request",
                "",
                &error_doc(-1013, &msg),
            ))]);
            let answer = client_at(&venue.base)
                .check_trade_permission(&order_on("BTCUSDT"), 0)
                .await;
            assert_eq!(venue.stop().len(), 1, "{name}");
            let Err(why) = answer else {
                panic!("a failure of {name} was taken for ready")
            };
            assert_eq!(
                why,
                NotReady::ProbeFiltered {
                    status: 400,
                    filter,
                    venue: error_read(-1013, &msg),
                },
                "{name}"
            );
            // The reason names the filter the venue cited, and why it stops
            // the run and not only the probe.
            let said = why.to_string();
            for needle in [
                format!("the venue refused the trade probe on its {name} filter (HTTP 400, "),
                format!("code -1013: Filter failure: {name})"),
                "The probe is the run's own order, so the run's own orders would meet the same \
                 filter"
                    .to_owned(),
                "the Spot Testnet rehearsal must confirm it".to_owned(),
            ] {
                assert!(said.contains(&needle), "{needle:?} missing from: {said}");
            }
        }

        // A filter this does not know is a filter failure all the same, and
        // its reason does not pretend to a name: the venue's message has it.
        let msg = "Filter failure: SOMETHING_NEW";
        let why =
            read_order_test(400, None, error_doc(-1013, msg).as_bytes()).expect_err("not ready");
        assert_eq!(
            why,
            NotReady::ProbeFiltered {
                status: 400,
                filter: FilterName::Unrecognised,
                venue: error_read(-1013, msg),
            }
        );
        let said = why.to_string();
        assert!(
            said.starts_with(
                "the venue refused the trade probe on a filter this does not know, which its \
                 message names (HTTP 400, code -1013: Filter failure: SOMETHING_NEW)"
            ),
            "{said}"
        );
    }

    /// A rate in the commission fixture distinctive enough that a reason which
    /// quoted the body would show it.
    const RATE: &str = "0.00071234";

    /// The documented commission rates (rest-api.md:4820-4847), their
    /// comments removed, one rate made [`RATE`], and the two discount flags as
    /// given.
    fn commission(for_account: &str, for_symbol: &str) -> String {
        format!(
            r#"{{"symbol":"BTCUSDT","standardCommission":{{"maker":"{RATE}","taker":"0.00000020","buyer":"0.00000030","seller":"0.00000040"}},"specialCommission":{{"maker":"0.01000000","taker":"0.02000000","buyer":"0.03000000","seller":"0.04000000"}},"taxCommission":{{"maker":"0.00000112","taker":"0.00000114","buyer":"0.00000118","seller":"0.00000116"}},"discount":{{"enabledForAccount":{for_account},"enabledForSymbol":{for_symbol},"discountAsset":"BNB","discount":"0.75000000"}}}}"#
        )
    }

    #[test]
    fn the_bnb_discount_is_read_off_the_account_flag_and_nothing_of_the_body_is_said() {
        // The account's flag decides, and in every case the symbol's says the
        // opposite, so a reader of the wrong one is caught either way.
        assert_eq!(
            bnb_discount_off(commission("false", "true").as_bytes()),
            Ok(())
        );
        let refusals = [
            (commission("true", "false"), NotReady::BnbDiscountOn),
            (
                commission("\"false\"", "false"),
                NotReady::UnreadableCommission(CommissionFault::DiscountFlagNotBool),
            ),
            (
                format!(r#"{{"symbol":"BTCUSDT","standardCommission":{{"maker":"{RATE}"}}}}"#),
                NotReady::UnreadableCommission(CommissionFault::NoDiscountFlag),
            ),
            (
                format!(r#"{{"discount":{{"enabledForSymbol":false,"discount":"{RATE}"}}}}"#),
                NotReady::UnreadableCommission(CommissionFault::NoDiscountFlag),
            ),
            (
                format!("<html>{RATE}</html>"),
                NotReady::UnreadableCommission(CommissionFault::NotJson),
            ),
        ];
        for (body, expected) in refusals {
            let why = bnb_discount_off(body.as_bytes()).expect_err("not ready");
            assert_eq!(why, expected, "{body}");
            assert!(!why.to_string().contains(RATE), "{why}");
            assert!(!format!("{why:?}").contains(RATE), "{why:?}");
        }
        // The reason names the setting, and why the run needs it off.
        let said = NotReady::BnbDiscountOn.to_string();
        for needle in [
            "pays its fees in BNB",
            "discount.enabledForAccount is true",
            // What the translator does, and so on which pairs it bites: a fee
            // in BNB on a BNB pair is in one of the pair's assets.
            "books a fee only in the pair's base or quote asset",
            "on a pair without BNB it refuses every fill whose fee was taken in BNB",
            "the run procedure turns the discount off",
            "switch paying fees in BNB off",
        ] {
            assert!(said.contains(needle), "{needle:?} missing from: {said}");
        }
        // Not "in neither asset of the pair", which is false on BNBUSDT.
        assert!(!said.contains("neither asset"), "{said}");
    }

    #[tokio::test]
    async fn the_commission_rates_are_asked_for_the_symbol_checked() {
        // The symbol goes out encoded as an order's would be, and the
        // documentation's own non-ASCII symbol is the one that shows a second
        // encoding, or none: either way the signature stops verifying.
        let venue = TestVenue::start(vec![Reply::Bytes(http(
            "200 OK",
            "",
            &commission("false", "false"),
        ))]);
        let answer = client_at(&venue.base)
            .check_bnb_discount_off(NON_ASCII, 0)
            .await;
        let requests = venue.stop();
        assert_eq!(requests.len(), 1, "{requests:#?}");
        assert!(
            requests[0].starts_with(
                "GET /api/v3/account/commission?symbol=%EF%BC%91%EF%BC%92%EF%BC%93%EF%BC%94%EF%BC%95\
                 %EF%BC%96&timestamp="
            ),
            "{}",
            requests[0]
        );
        signed_on_the_wire(&requests[0]);
        assert_eq!(answer, Ok(()));

        // A refusal is sorted as check_ready sorts one. On a USER_DATA
        // request -2015 is the key, its address or its permission to read,
        // which is the credentials, not the key's permission to trade.
        let key = r#"{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}"#;
        let venue = TestVenue::start(vec![Reply::Bytes(http("401 Unauthorized", "", key))]);
        let answer = client_at(&venue.base)
            .check_bnb_discount_off("BTCUSDT", 0)
            .await;
        venue.stop();
        assert_eq!(
            answer,
            Err(NotReady::BadCredentials {
                status: 401,
                venue: Some(VenueError {
                    code: -2015,
                    msg: "Invalid API-key, IP, or permissions for action.".to_owned(),
                }),
            })
        );
    }

    #[tokio::test]
    async fn a_checked_token_is_minted_only_once_all_three_checks_pass() {
        // The three answers that pass, in venue-check's order: the account
        // probe, the trade probe, the commission rates.
        let pass = || {
            vec![
                http("200 OK", "", &account("true")),
                http("200 OK", "", "{}"),
                http("200 OK", "", &commission("false", "false")),
            ]
        };
        let venue = TestVenue::start(pass().into_iter().map(Reply::Bytes).collect());
        let checked = client_at(&venue.base)
            .checked(&order_on("BTCUSDT"), "BTCUSDT", 412)
            .await
            .expect("every check passed");
        let requests = venue.stop();
        assert_eq!(
            checked.offset_ms(),
            412,
            "the offset the checks passed with"
        );
        let paths: Vec<&str> = requests
            .iter()
            .map(|r| r.split(['?', ' ']).nth(1).unwrap_or_default())
            .collect();
        assert_eq!(
            paths,
            [
                "/api/v3/account",
                ORDER_TEST_PATH,
                "/api/v3/account/commission"
            ]
        );

        // Each check refusing in turn: no token, and nothing after it asked.
        let refusals = [
            http("200 OK", "", &account("false")),
            http(
                "400 Bad Request",
                "",
                r#"{"code":-2010,"msg":"Account has insufficient balance for requested action."}"#,
            ),
            http("200 OK", "", &commission("true", "true")),
        ];
        for (n, refusal) in refusals.into_iter().enumerate() {
            let mut replies = pass();
            replies[n] = refusal;
            let venue = TestVenue::start(replies.into_iter().map(Reply::Bytes).collect());
            let answer = client_at(&venue.base)
                .checked(&order_on("BTCUSDT"), "BTCUSDT", 0)
                .await;
            let requests = venue.stop();
            assert!(answer.is_err(), "check {n} refused, and still {answer:?}");
            assert_eq!(requests.len(), n + 1, "asked past check {n}: {requests:#?}");
        }
    }
}
