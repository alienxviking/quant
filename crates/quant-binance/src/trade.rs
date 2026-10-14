//! The signed half of the venue's REST API.
//!
//! Separate from [`crate::rest::SnapshotClient`] on purpose, and the separation
//! is the design rather than tidiness: that client fetches depth and server time
//! for a recorder that places no orders, and giving it a secret would put one in
//! the address space of a seven-day capture that has no use for it. A type that
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
            .filter(|r| r.round_trip_ms() >= 0)
            .min_by_key(|r| r.round_trip_ms())
            .copied()
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
    /// The venue would not answer a signed request. Almost always a bad key, a
    /// bad secret, or a key without trading permission.
    Rejected { status: u16 },
    /// We could not reach the venue at all.
    Unreachable(String),
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
            Self::Rejected { status } => write!(
                f,
                "the venue refused a signed request with HTTP {status}. The key, the secret, or \
                 the key's trading permission is wrong -- note that a key restricted by IP fails \
                 this way from a new address"
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
    /// [`SnapshotError`] for transport, status and body failures, exactly as the
    /// public client reports them.
    pub async fn signed_get(
        &self,
        path: &str,
        params: &str,
        now_millis: i64,
    ) -> Result<Vec<u8>, SnapshotError> {
        let url = format!(
            "{}{path}?{}",
            self.base.trim_end_matches('/'),
            self.signed_query(params, now_millis)
        );
        let response = self
            .http
            .get(url)
            // The key identifies the account and travels in the header; the
            // secret never leaves `Credentials`.
            .header("X-MBX-APIKEY", self.credentials.key())
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    SnapshotError::Timeout
                } else {
                    // `reqwest`'s error Display includes the URL, and the URL
                    // carries the signature. A signature is not the secret and
                    // cannot be reversed to it, but it is per-request
                    // authentication material and has no business in a log that
                    // survives the request.
                    SnapshotError::Transport(redact_signature(&e.to_string()))
                }
            })?;
        let status = response.status();
        if !status.is_success() {
            return Err(SnapshotError::Status(status.as_u16()));
        }
        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| SnapshotError::Body(redact_signature(&e.to_string())))
    }

    /// Ask the venue whether this account can trade, and whether our clock
    /// agrees with its own.
    ///
    /// `GET /api/v3/account` is the cheapest signed call there is, which makes
    /// it the right probe: it proves the key, the secret, the signature
    /// construction and the clock in one round trip, and it places no order.
    ///
    /// # Errors
    ///
    /// [`NotReady`] with the reason, which is a thing to print and stop on.
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
        match self.signed_get("/api/v3/account", "", now).await {
            Ok(_) => Ok(()),
            Err(SnapshotError::Status(status)) => Err(NotReady::Rejected { status }),
            Err(other) => Err(NotReady::Unreachable(other.to_string())),
        }
    }
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

/// Strip a signature out of anything about to be logged.
fn redact_signature(text: &str) -> String {
    let Some(at) = text.find("signature=") else {
        return text.to_owned();
    };
    let head = &text[..at + "signature=".len()];
    let tail = &text[at + "signature=".len()..];
    let end = tail
        .find(|c: char| !c.is_ascii_hexdigit())
        .unwrap_or(tail.len());
    format!("{head}<redacted>{}", &tail[end..])
}

#[cfg(test)]
mod tests {
    use super::{
        midpoint_offset_ms, redact_signature, ClockSample, NotReady, TradeClient,
        MAX_CLOCK_SKEW_MS, RECV_WINDOW_MS,
    };
    use crate::credentials::Credentials;
    use std::time::Duration;

    fn client() -> TradeClient {
        TradeClient::new(
            "https://example.invalid",
            Duration::from_secs(5),
            Credentials::new("the-key".to_owned(), "the-secret"),
        )
        .expect("builds")
    }

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
    fn the_timestamp_and_window_are_inside_the_signature_not_appended_after_it() {
        // The mistake that produces a valid-looking request the venue refuses
        // with an error naming neither cause: sign the caller's parameters, then
        // append `timestamp`. The signature then covers a different string than
        // the one sent.
        let c = client();
        let q = c.signed_query("symbol=BTCUSDT", 1_700_000_000_000);
        let signature_at = q.find("&signature=").expect("signed");
        let signed_part = &q[..signature_at];
        assert!(signed_part.contains("timestamp=1700000000000"), "{q}");
        assert!(
            signed_part.contains(&format!("recvWindow={RECV_WINDOW_MS}")),
            "{q}"
        );
        assert_eq!(
            c.signed_query("symbol=BTCUSDT", 1_700_000_000_000),
            format!(
                "{signed_part}&signature={}",
                Credentials::new("the-key".to_owned(), "the-secret").sign(signed_part)
            ),
            "the signature must be over exactly what precedes it"
        );
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
        // signature. Not the secret and not reversible to it, but per-request
        // authentication material with no business outliving the request.
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
    fn text_with_no_signature_is_left_alone() {
        assert_eq!(redact_signature("dns error"), "dns error");
    }
}
