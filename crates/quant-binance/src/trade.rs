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

use std::time::Duration;

use crate::credentials::Credentials;
use crate::rest::SnapshotError;

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
    /// The venue refused the credentials: HTTP 401, or under any status one of
    /// -2014, *"API-key format invalid"*, -2015, *"Invalid API-key, IP, or
    /// permissions for action"*, or -1022, *"Signature for this request is not
    /// valid"* (errors.md:57-58, :338-342). A bad API key, a private key that
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
    /// The venue accepted the key and the signature, and says this account
    /// cannot trade: `canTrade` is false (rest-api.md:4207).
    ///
    /// The venue's documentation shows the field without defining it. What it
    /// does say is that an API key cannot trade until trading is enabled for it
    /// (rest-api.md:194-196). Whichever of the key or the account the `false`
    /// is about, an order would be refused, and this refuses first.
    CannotTrade,
    /// The venue answered 200, but not with the account document this reads.
    /// Which way it was not is said; what it held is not, because it lists the
    /// account's balances.
    UnreadableAccount(AccountFault),
    /// We could not reach the venue at all.
    Unreachable(String),
}

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
            } => {
                write!(
                    f,
                    "{came_back} of {attempts} readings of the venue's clock came back"
                )?;
                if usable < came_back {
                    write!(
                        f,
                        ", {} with a round trip that ran backwards (the host clock stepped while \
                         it was being read)",
                        came_back.saturating_sub(*usable)
                    )?;
                }
                write!(
                    f,
                    ", and this will not judge the clock on fewer than {MIN_CLOCK_READINGS} usable \
                     ones"
                )?;
                if *usable > 0 {
                    // Why not judge on what there is: the question a reading
                    // that did come back invites.
                    f.write_str(
                        ": a lone reading may be the cold one, its outbound leg carrying the \
                         connection's DNS, TCP and TLS, which no midpoint corrects",
                    )?;
                }
                Ok(())
            }
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
            Self::CannotTrade => f.write_str(
                "the venue accepted the key and the signature, and says this account cannot \
                 trade (canTrade is false). An API key cannot trade until trading is enabled for \
                 it in Binance's API Management",
            ),
            Self::UnreadableAccount(fault) => write!(
                f,
                "the venue answered 200, but {fault}. Nothing of the body is printed: it lists \
                 the account's balances"
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
            .user_agent(concat!("quant-live/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| SnapshotError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            base: base.into(),
            credentials,
        })
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
        let reply = self.send_signed_get(path, params, now_millis).await?;
        if !reply.status.is_success() {
            return Err(SnapshotError::Status(reply.status.as_u16()));
        }
        Ok(reply.body)
    }

    /// Send a signed GET and return whatever the venue answered, refusals
    /// included, so that a refusal can be read for what it says.
    async fn send_signed_get(
        &self,
        path: &str,
        params: &str,
        now_millis: i64,
    ) -> Result<SignedReply, SnapshotError> {
        let url = format!(
            "{}{path}?{}",
            self.base.trim_end_matches('/'),
            self.signed_query(params, now_millis)
        );
        let response = self
            .http
            .get(url)
            // The API key identifies the account and travels in the header;
            // the private key never leaves `Credentials`.
            .header("X-MBX-APIKEY", self.credentials.key())
            .send()
            .await
            .map_err(transport_error)?;
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
            Err(e) if e.is_timeout() => return Err(SnapshotError::Timeout),
            Err(e) => return Err(SnapshotError::Body(describe(e))),
        };
        Ok(SignedReply {
            status,
            retry_after,
            body,
        })
    }

    /// Ask the venue whether this account can trade, and whether our clock
    /// agrees with its own.
    ///
    /// `GET /api/v3/account` is not the cheapest signed call: its weight is 20
    /// (rest-api.md:4180-4181), where `GET /api/v3/openOrders` for one symbol
    /// is 6 (rest-api.md:4293-4294). It is the probe because its answer says
    /// whether this key may trade at all — `canTrade` (rest-api.md:4207) — as
    /// well as proving the API key, the private key, the signature
    /// construction and the clock in one round trip. It places no order.
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
            .send_signed_get("/api/v3/account", "", now)
            .await
            .map_err(|e| NotReady::Unreachable(e.to_string()))?;
        if !reply.status.is_success() {
            return Err(classify_refusal(
                reply.status.as_u16(),
                reply.retry_after,
                &reply.body,
            ));
        }
        account_can_trade(&reply.body)
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

/// Whether the account document says this key may trade (`canTrade`,
/// rest-api.md:4207).
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
        account_can_trade, classify_refusal, midpoint_offset_ms, redact_signature, AccountFault,
        ClockSample, NotReady, TradeClient, VenueError, MAX_CLOCK_SKEW_MS, RECV_WINDOW_MS,
    };
    use crate::credentials::fixture;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
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

    /// A one-shot HTTP server on this machine. It reads the first request,
    /// answers it with `response` — or, when that is empty, hangs up without a
    /// word — and hands back the request it was sent.
    ///
    /// The venue, as far as the client can tell, minus the network: what
    /// `check_ready` makes of a status, a header and a body is tested on the
    /// path it takes in production, with no credentials and nothing sent
    /// beyond this machine.
    fn serve_once(response: String) -> (String, std::thread::JoinHandle<String>) {
        use std::io::{Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("address"));
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut request = Vec::new();
            let mut buf = [0; 4096];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).expect("read");
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
            }
            stream.write_all(response.as_bytes()).expect("write");
            String::from_utf8(request).expect("an HTTP request head is text")
        });
        (base, server)
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
        // The venue's own messages for these codes (errors.md).
        let key = "Invalid API-key, IP, or permissions for action.";
        let format = "API-key format invalid.";
        let signature = "Signature for this request is not valid.";
        let weight = "Too much request weight used; current limit is 6000 request weight per 1 \
                      MINUTE. Please use WebSocket Streams for live updates to avoid polling the \
                      API.";
        let busy = "Server is currently overloaded with other requests. Please try again in a few \
                    minutes.";
        let stale = "Timestamp for this request is outside of the recvWindow.";
        let html = "<html><body>403 Forbidden</body></html>".to_owned();
        let none = String::new();

        let cases = [
            // The credentials: 401 whatever the body says, and the three codes
            // whatever the status.
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
            // The order of the rule rather than a thing seen: a status that
            // says the request was never judged on its credentials decides
            // before the code does.
            (
                503,
                None,
                doc(-1022, signature),
                failed(503, read(-1022, signature)),
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
        // A key without trading permission has its own reason, which says
        // what to do about it.
        let said = NotReady::CannotTrade.to_string();
        assert!(said.contains("canTrade is false"), "{said}");
        assert!(said.contains("API Management"), "{said}");
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
        let request = server.join().expect("the server ran");
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

    #[tokio::test]
    async fn a_rate_limit_is_read_from_its_status_its_retry_after_and_its_body() {
        let body = r#"{"code":-1003,"msg":"Too much request weight used; current limit is 6000 request weight per 1 MINUTE."}"#;
        let (base, server) = serve_once(http("429 Too Many Requests", "Retry-After: 37\r\n", body));
        let why = client_at(&base).check_ready(0).await.expect_err("refused");
        server.join().expect("the server ran");
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

    #[tokio::test]
    async fn an_account_that_cannot_trade_is_not_ready_and_one_that_can_is_ready() {
        let (base, server) = serve_once(http("200 OK", "", &account("false")));
        let why = client_at(&base)
            .check_ready(0)
            .await
            .expect_err("cannot trade");
        let request = server.join().expect("the server ran");
        assert_eq!(why, NotReady::CannotTrade);
        assert!(!why.to_string().contains(BALANCE), "{why}");
        // The request it answered was the signed account probe, with its key.
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

        let (base, server) = serve_once(http("200 OK", "", &account("true")));
        assert_eq!(client_at(&base).check_ready(0).await, Ok(()));
        server.join().expect("the server ran");
    }
}
