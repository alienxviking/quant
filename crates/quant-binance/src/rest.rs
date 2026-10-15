//! The one thing the WebSocket cannot give us: a book to apply deltas to.
//!
//! # Why the recorder touches REST at all
//!
//! Binance's depth stream is *incremental*. On its own it is unusable: a delta
//! says "level 68123.45 now holds 0.31", which means nothing without a book to
//! apply it to. The book comes from `GET /api/v3/depth`, and that endpoint serves
//! only the book's **current** state.
//!
//! That asymmetry is the whole reason this module exists at capture time rather
//! than in the normalizer. Everything else the recorder writes can be
//! reinterpreted later from bytes already on disk -- that is the bargain of the
//! raw tier. A snapshot not taken at the moment of a reconnect is a snapshot that
//! can never be taken, and every delta after that reconnect stays unanchored
//! forever. Capture it or lose it.
//!
//! # What this module deliberately does not do
//!
//! It does not parse the response, discard stale deltas, or check that the
//! update-id chain joins. Those are steps in Binance's *local order book*
//! algorithm, they are pure functions of bytes we are about to write down, and
//! doing them here would mean deciding at 3am -- irreversibly, with no way to fix
//! a mistake afterwards -- something a normalizer can decide at leisure and redo
//! when we find a bug. The book belongs to M2.

use core::fmt;
use core::time::Duration;

use quant_storage::SnapshotFailure;

/// Binance spot REST endpoint.
pub const SPOT_REST: &str = "https://api.binance.com";

/// Binance Spot Testnet's REST endpoint: for `venue-check --testnet`, and for
/// the rehearsal `docs/live-run.md` §5 runs there, with a key pair of its own,
/// before any real order.
///
/// The venue's documentation names SPOT Testnet at this address
/// (CHANGELOG.md:548, :633) and gives its WebSocket API base on a sibling host,
/// `wss://ws-api.testnet.binance.vision/ws-api/v3` (web-socket-api.md:104). It
/// does not spell out a REST base for it; the paths are taken to be the live
/// ones, `/api/v3/...`, beneath this host. A wrong guess cannot pass for a right
/// one: `venue-check` reads the venue's clock first, and a path that is not
/// there leaves it no readings to judge by.
pub const SPOT_TESTNET_REST: &str = "https://testnet.binance.vision";

/// Levels per side to request.
///
/// The maximum the venue offers. Same reasoning as `depth@100ms` over
/// `depth@1000ms`: a shallower snapshot is detail the venue would have given us
/// for free and that cannot be recovered from capture later, whereas the cost is
/// about a megabyte and 250 of a 6000-per-minute request-weight budget. A book
/// truncated at 100 levels also silently breaks any delta that touches a deeper
/// level, which is the kind of error that shows up as a slowly diverging book
/// rather than as a failure.
pub const DEPTH_LIMIT: u16 = 5000;

/// Why a snapshot request did not produce bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    /// The request did not finish inside the configured timeout.
    Timeout,
    /// DNS, TCP, or TLS failure.
    Transport(String),
    /// The venue answered with a non-success status. Almost always 429 (rate
    /// limited) or 418 (banned for ignoring 429).
    Status(u16),
    /// We could not read the body we were promised.
    Body(String),
}

impl SnapshotError {
    /// The coarse category that goes on disk.
    ///
    /// The detailed text stays in the log. See
    /// [`quant_storage::SnapshotFailure`] for why the immutable tier gets a
    /// closed set of categories rather than a remote server's error strings.
    #[must_use]
    pub const fn reason(&self) -> SnapshotFailure {
        match self {
            Self::Timeout => SnapshotFailure::Timeout,
            Self::Status(_) => SnapshotFailure::Status,
            // A truncated body lands here too: the venue said yes and the network
            // lost the answer, which is a transport problem and not a refusal.
            Self::Transport(_) | Self::Body(_) => SnapshotFailure::Transport,
        }
    }
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("request timed out"),
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::Status(code) => write!(f, "venue answered {code}"),
            Self::Body(e) => write!(f, "could not read the response body: {e}"),
        }
    }
}

impl std::error::Error for SnapshotError {}

/// Fetches order-book snapshots over REST.
///
/// Holds one [`reqwest::Client`], which owns the connection pool. Reused across
/// fetches so that an hourly snapshot on a warm connection costs one round trip
/// rather than a fresh TLS handshake.
#[derive(Debug, Clone)]
pub struct SnapshotClient {
    http: reqwest::Client,
    base: String,
}

impl SnapshotClient {
    pub fn new(base: impl Into<String>, request_timeout: Duration) -> Result<Self, SnapshotError> {
        // Not the caller's problem, and it must happen before `build`: reqwest
        // reads the process-global rustls provider there and *panics* if none is
        // set. See `crate::install_crypto_provider`.
        crate::install_crypto_provider();

        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            // Idle sockets are cheap and a fresh handshake per hourly snapshot is
            // not, but an idle pool entry that the venue has silently dropped
            // costs a retry. A few minutes keeps the warm case warm without
            // holding sockets across a quiet night.
            .pool_idle_timeout(Duration::from_secs(300))
            .user_agent(concat!("quant-recorder/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| SnapshotError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            base: base.into(),
        })
    }

    /// The URL a depth snapshot comes from. Public so a test can check it without
    /// a network.
    #[must_use]
    pub fn depth_url(&self, symbol: &str) -> String {
        format!(
            "{}/api/v3/depth?symbol={}&limit={}",
            self.base.trim_end_matches('/'),
            symbol,
            DEPTH_LIMIT
        )
    }

    /// How far our clock is from the venue's, in milliseconds. Positive means we
    /// are ahead.
    ///
    /// # Why this is worth a request at startup
    ///
    /// Venue latency is `local_recv_ts - exchange_ts`, so a host clock that is two
    /// seconds fast reports two seconds of latency on a link that is actually
    /// fine. That is not a hypothetical: it is what the first run of the latency
    /// metric on this project's own development machine showed, and without this
    /// check the only way to tell it from real network trouble is to go and
    /// measure the clock by hand.
    ///
    /// It costs one request at startup and turns an ambiguous number into a stated
    /// fact. The outbound leg is included in the measurement -- `local_millis` is
    /// read before the request leaves, and on a client's first request that leg
    /// carries the connection setup too -- which biases the answer towards a host
    /// *behind* the venue: by a few milliseconds close to it, by hundreds over a
    /// slow link. On a good link that is small against the threshold that
    /// matters, and correcting for it would mean pretending to a precision this
    /// does not have, so it is deliberately left uncorrected. The offset that
    /// decides whether a host may *trade* is a different measurement: see
    /// [`crate::trade::midpoint_offset_ms`].
    pub async fn clock_offset_millis(&self, local_millis: i64) -> Result<i64, SnapshotError> {
        Ok(local_millis - self.server_time_millis().await?)
    }

    /// The venue's clock: `serverTime` from `GET /api/v3/time`, in milliseconds
    /// since the Unix epoch.
    ///
    /// The bare reading, with nothing of ours subtracted from it, for a caller
    /// that reads its own clock either side of the request and corrects for the
    /// round trip -- see [`crate::trade::ClockSample`].
    pub async fn server_time_millis(&self) -> Result<i64, SnapshotError> {
        #[derive(serde::Deserialize)]
        struct ServerTime {
            #[serde(rename = "serverTime")]
            server_time: i64,
        }

        let url = format!("{}/api/v3/time", self.base.trim_end_matches('/'));
        let response = self.http.get(url).send().await.map_err(|e| {
            if e.is_timeout() {
                SnapshotError::Timeout
            } else {
                SnapshotError::Transport(e.to_string())
            }
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(SnapshotError::Status(status.as_u16()));
        }
        let body = response
            .bytes()
            .await
            .map_err(|e| SnapshotError::Body(e.to_string()))?;
        let parsed: ServerTime =
            serde_json::from_slice(&body).map_err(|e| SnapshotError::Body(e.to_string()))?;
        Ok(parsed.server_time)
    }

    /// Fetch one depth snapshot, returning the response body verbatim.
    ///
    /// The bytes are not looked at. They go to disk as
    /// [`quant_storage::FrameKind::VenueSnapshot`] exactly as received, so that a
    /// mistake in our understanding of the payload is a normalizer bug we can fix
    /// rather than a capture we have to discard.
    pub async fn depth(&self, symbol: &str) -> Result<Vec<u8>, SnapshotError> {
        let response = self
            .http
            .get(self.depth_url(symbol))
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    SnapshotError::Timeout
                } else {
                    SnapshotError::Transport(e.to_string())
                }
            })?;

        // Status before body: a 429 body is an error document, and writing it into
        // the capture as though it were a book is exactly the sort of silent
        // corruption the typed frame kind exists to prevent.
        let status = response.status();
        if !status.is_success() {
            return Err(SnapshotError::Status(status.as_u16()));
        }

        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| match () {
                () if e.is_timeout() => SnapshotError::Timeout,
                () => SnapshotError::Body(e.to_string()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> SnapshotClient {
        SnapshotClient::new(SPOT_REST, Duration::from_secs(10)).unwrap()
    }

    #[test]
    fn the_depth_url_is_exactly_what_binance_expects() {
        assert_eq!(
            client().depth_url("BTCUSDT"),
            "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=5000"
        );
    }

    #[test]
    fn the_depth_limit_is_the_deepest_the_venue_offers() {
        // Silently dropping to a shallower book would break every delta touching
        // a deeper level, and it would show up as a book that slowly diverges
        // rather than as an error.
        assert_eq!(DEPTH_LIMIT, 5000);
    }

    #[test]
    fn a_trailing_slash_on_the_base_does_not_produce_a_double_slash() {
        let with = SnapshotClient::new("https://example.test/", Duration::from_secs(1)).unwrap();
        let without = SnapshotClient::new("https://example.test", Duration::from_secs(1)).unwrap();
        assert_eq!(with.depth_url("ETHUSDT"), without.depth_url("ETHUSDT"));
    }

    #[test]
    fn failures_map_onto_the_categories_that_reach_the_disk() {
        // The mapping is what a future reader of a capture file sees, so it is
        // pinned rather than left to whatever the match arms happen to say.
        assert_eq!(SnapshotError::Timeout.reason(), SnapshotFailure::Timeout);
        assert_eq!(
            SnapshotError::Transport("reset".to_owned()).reason(),
            SnapshotFailure::Transport
        );
        assert_eq!(SnapshotError::Status(429).reason(), SnapshotFailure::Status);
        // A truncated body is a transport problem, not a venue refusal: the venue
        // said yes and the network lost the answer.
        assert_eq!(
            SnapshotError::Body("eof".to_owned()).reason(),
            SnapshotFailure::Transport
        );
    }

    #[test]
    fn a_rate_limited_response_is_distinguishable_in_the_log() {
        assert!(SnapshotError::Status(429).to_string().contains("429"));
        assert_ne!(
            SnapshotError::Status(429).to_string(),
            SnapshotError::Timeout.to_string()
        );
    }
}
