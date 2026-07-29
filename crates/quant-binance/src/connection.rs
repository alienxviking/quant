//! The read loop: connect, drain, reconnect, forever.

use core::fmt;
use core::future::Future;
use core::pin::Pin;
use core::time::Duration;
use std::time::Instant;

use futures_util::{SinkExt as _, StreamExt as _};
use quant_core::event::GapCause;
use quant_recorder::{Accepted, Backoff, BackoffPolicy, Ingress, RecordSink, WriterGone};
use quant_storage::{SnapshotFailure, SnapshotPurpose};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{debug, info, warn};

use crate::rest::{SnapshotClient, SnapshotError};
use crate::stream::StreamSpec;

/// How the recorder behaves around a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionPolicy {
    pub backoff: BackoffPolicy,
    /// Treat the connection as dead if nothing at all arrives within this.
    pub idle_timeout: Duration,
    /// Emit a progress line every this many messages.
    pub log_every: u64,
    /// How often to take a book snapshot while connected, on top of the one
    /// taken at every (re)connect.
    pub snapshot_interval: Duration,
    /// How many times to try for one snapshot before recording that we could not
    /// get it.
    pub snapshot_attempts: u32,
    /// Wait between those attempts.
    pub snapshot_retry_delay: Duration,
}

impl Default for ConnectionPolicy {
    /// Default backoff, a 120 s idle timeout, progress every 50k messages.
    ///
    /// # Why an idle timeout at all
    ///
    /// The failure that ruins a 7-day run is not a socket that closes -- that we
    /// notice immediately -- it is a socket that stays open and stops delivering.
    /// A half-open TCP connection after a network path change looks perfectly
    /// healthy from our side, and without a timeout the recorder sits there
    /// contentedly reading nothing until someone looks at the data days later.
    ///
    /// # Why 120 seconds
    ///
    /// It has to exceed the venue's own keepalive interval with margin, because
    /// Binance's periodic ping is the only traffic guaranteed to arrive on a
    /// genuinely quiet instrument -- a depth diff stream sends nothing when the
    /// book does not move. Too tight and we manufacture disconnects, each one a
    /// real hole punched in otherwise good data; too loose and a true stall goes
    /// unnoticed for longer.
    ///
    /// 120 s is deliberately on the generous side of that trade, because a
    /// spurious reconnect corrupts the sample whereas a slow-detected stall only
    /// delays noticing. For a liquid symbol on 100 ms depth this never fires:
    /// silence for even a few seconds is already anomalous.
    ///
    /// # Why snapshots repeat on a timer
    ///
    /// A resync snapshot at every connect is what makes the book buildable at
    /// all. Repeating hourly buys two more things.
    ///
    /// It bounds the work to reach an arbitrary point: without periodic
    /// snapshots, replaying 16:00 on a day whose session began at midnight means
    /// applying sixteen hours of deltas first. And it bounds the *damage* of a
    /// single lost delta -- one hole otherwise invalidates the book for the rest
    /// of the session, permanently, whereas with an hourly anchor the book
    /// re-synchronizes at the next one. That is the same reason compression is
    /// per block rather than per file: keep damage local.
    ///
    /// An hour is cheap. A 5000-level snapshot is about a megabyte against
    /// several hundred megabytes a day of deltas for a liquid symbol, and it
    /// compresses well.
    ///
    /// # Why four attempts, two seconds apart
    ///
    /// The realistic failure is a 429: a momentary rate limit that clears in
    /// seconds. Four attempts over about six seconds rides that out. What we must
    /// not do is hammer an endpoint that has just asked us to stop, because the
    /// venue's escalation from 429 to a timed IP ban would take the *WebSocket*
    /// down with it -- turning a missing snapshot into a real hole in the deltas.
    fn default() -> Self {
        Self {
            backoff: BackoffPolicy::default(),
            idle_timeout: Duration::from_secs(120),
            log_every: 50_000,
            snapshot_interval: Duration::from_secs(3600),
            snapshot_attempts: 4,
            snapshot_retry_delay: Duration::from_secs(2),
        }
    }
}

/// Why a connection stopped delivering.
///
/// Every variant is recorded as the same `Gap{Disconnect}` -- the data contract
/// cares that we were blind, not about the mechanism -- but they are kept
/// distinct for the log, because "server closed" every 24 hours is Binance's
/// documented behaviour while "idle timeout" every 12 minutes is a bug.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndReason {
    /// The venue closed the stream. Expected: Binance drops connections after
    /// 24 hours regardless of health.
    ServerClosed,
    /// Nothing arrived within the idle timeout.
    IdleTimeout,
    /// The transport itself failed.
    Transport(String),
    /// We could not answer a keepalive, so the venue is about to hang up anyway.
    KeepaliveFailed(String),
}

impl fmt::Display for EndReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServerClosed => f.write_str("server closed the stream"),
            Self::IdleTimeout => f.write_str("idle timeout: connection open but silent"),
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::KeepaliveFailed(e) => write!(f, "could not send pong: {e}"),
        }
    }
}

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// A snapshot request in flight.
///
/// Boxed and held as a plain future rather than run as a spawned task, so that
/// dropping the drain loop cancels the request. A task would outlive the
/// connection it belongs to and could deliver a snapshot into the *next* one,
/// where its position in the sequence would be a lie.
type SnapshotFetch<'a> = Pin<Box<dyn Future<Output = Result<Vec<u8>, SnapshotError>> + Send + 'a>>;

/// Record `spec` until the writer goes away or the caller cancels.
///
/// Returns only on a fatal condition. A normal disconnect is not fatal: it is
/// recorded as a gap and retried, which is the entire point of an unattended
/// recorder. Cancellation is the caller's job -- drop this future (see the
/// `record` binary, which races it against ctrl-c) and the borrowed `ingress`
/// becomes droppable, which closes the channel and makes the writer seal the
/// file with a trailer.
///
/// `snapshots` is optional so that a run against a fake WebSocket has no reason
/// to reach a real REST endpoint. In production it is always present: without it
/// the depth deltas are recorded but can never be turned into a book.
pub async fn run<S: RecordSink>(
    spec: &StreamSpec,
    base_url: &str,
    ingress: &mut Ingress<S>,
    policy: &ConnectionPolicy,
    snapshots: Option<&SnapshotClient>,
) -> Result<(), WriterGone> {
    let url = spec.url(base_url);
    let mut backoff = Backoff::for_key(policy.backoff, &spec.symbol);
    info!(symbol = %spec.symbol, %url, "recorder starting");

    loop {
        match connect_async(url.as_str()).await {
            Ok((socket, response)) => {
                info!(
                    symbol = %spec.symbol,
                    status = response.status().as_u16(),
                    attempt = backoff.attempt(),
                    "connected"
                );
                // Reset only after a *successful* connect, so a symbol that
                // reconnects once an hour never inherits a 30 s wait from an
                // unrelated outage last week.
                backoff.reset();

                let reason = drain(socket, ingress, policy, &spec.symbol, snapshots).await?;
                warn!(symbol = %spec.symbol, %reason, "connection ended");

                // One gap per blind episode, recorded here rather than per failed
                // retry below: repeated connect failures are all the same hole.
                ingress.record_gap(GapCause::Disconnect)?;
            }
            Err(e) => {
                warn!(
                    symbol = %spec.symbol,
                    attempt = backoff.attempt(),
                    error = %e,
                    "connect failed"
                );
            }
        }

        let delay = backoff.next_delay();
        // We are about to wait anyway, and a disconnect means the writer is
        // draining -- so this is the natural place for a deferred gap record to
        // finally land.
        ingress.pump()?;
        debug!(symbol = %spec.symbol, ?delay, "backing off");
        sleep(delay).await;
    }
}

/// What the drain loop woke up for.
enum Event {
    /// The socket produced something, or did not, within the idle timeout.
    Socket(Result<Option<Result<Message, tungstenite::Error>>, tokio::time::error::Elapsed>),
    /// A snapshot request finished, one way or the other.
    Fetched(Result<Vec<u8>, SnapshotError>),
}

/// Pull messages until the connection stops being useful, taking book snapshots
/// alongside.
///
/// # Why the snapshot is concurrent rather than awaited first
///
/// The order is forced from both sides. It must be fetched *after* the
/// subscription is live, or the venue's book advances between the snapshot and
/// our first delta and nothing can bridge the difference. And it must not be
/// awaited *before* the first `socket.next()`, because that would stop draining
/// the socket for a full round trip -- reintroducing exactly the coupling between
/// our latency and the venue's patience that the split between this task and the
/// writer thread exists to remove.
///
/// Running it concurrently also gets the useful property for free: the snapshot's
/// `ingest_seq` lands *between* the deltas it arrived between, so the file itself
/// records which deltas precede the anchor and are therefore stale. That is the
/// input to Binance's book algorithm, and the alternative -- a snapshot on a side
/// channel with no position in the sequence -- would leave it undecidable.
async fn drain<S: RecordSink>(
    mut socket: Socket,
    ingress: &mut Ingress<S>,
    policy: &ConnectionPolicy,
    symbol: &str,
    snapshots: Option<&SnapshotClient>,
) -> Result<EndReason, WriterGone> {
    let mut since_log = 0_u64;

    // Every connection starts by wanting a resync snapshot. `None` means nothing
    // is wanted right now; `fetch` holds the attempt in flight.
    let mut want = snapshots.and(Some(SnapshotPurpose::Resync));
    let mut attempts = 0_u32;
    let mut fetch: Option<SnapshotFetch<'_>> = None;
    let mut next_periodic = Instant::now() + policy.snapshot_interval;

    loop {
        // A periodic snapshot is only noticed when the loop turns, which on a
        // silent symbol is once per idle timeout. Being up to two minutes late for
        // a recovery point is not worth a dedicated timer in the hot path.
        if want.is_none() && snapshots.is_some() && Instant::now() >= next_periodic {
            want = Some(SnapshotPurpose::Periodic);
            attempts = 0;
        }

        if let (Some(purpose), None, Some(client)) = (want, fetch.as_ref(), snapshots) {
            let delay = if attempts == 0 {
                Duration::ZERO
            } else {
                policy.snapshot_retry_delay
            };
            attempts += 1;
            debug!(symbol, ?purpose, attempts, "requesting a book snapshot");
            fetch = Some(Box::pin(fetch_snapshot(client, symbol, delay)));
        }

        let event = match fetch.as_mut() {
            // Biased with the socket first: draining it is the job, and a
            // snapshot arriving a few microseconds later costs nothing.
            Some(pending) => tokio::select! {
                biased;
                next = timeout(policy.idle_timeout, socket.next()) => Event::Socket(next),
                got = pending => Event::Fetched(got),
            },
            None => Event::Socket(timeout(policy.idle_timeout, socket.next()).await),
        };

        let next = match event {
            Event::Fetched(result) => {
                fetch = None;
                let purpose = want.unwrap_or(SnapshotPurpose::Periodic);
                match snapshot_outcome(result, ingress, symbol, purpose)? {
                    Ok(()) => {
                        want = None;
                        attempts = 0;
                        next_periodic = Instant::now() + policy.snapshot_interval;
                    }
                    Err(reason) if attempts >= policy.snapshot_attempts => {
                        warn!(
                            symbol,
                            ?purpose,
                            attempts,
                            ?reason,
                            "giving up on a book snapshot; recording that we could not get it"
                        );
                        ingress.record_snapshot_failure(purpose, reason, attempts)?;
                        want = None;
                        // Deliberately not re-armed immediately: the next periodic
                        // deadline applies, so a venue that is refusing us is left
                        // alone for an hour rather than asked again at once.
                        next_periodic = Instant::now() + policy.snapshot_interval;
                    }
                    // Attempts remain; the top of the loop re-arms with a delay.
                    Err(_) => {}
                }
                continue;
            }
            Event::Socket(Err(_elapsed)) => return Ok(EndReason::IdleTimeout),
            Event::Socket(Ok(None)) => return Ok(EndReason::ServerClosed),
            Event::Socket(Ok(Some(Err(e)))) => return Ok(EndReason::Transport(e.to_string())),
            Event::Socket(Ok(Some(Ok(message)))) => message,
        };

        match next {
            // `into_data` rather than matching the payload type, so a tungstenite
            // release that changes Text's representation does not change this.
            message @ (Message::Text(_) | Message::Binary(_)) => {
                ingress.accept(message.into_data().to_vec())?;
                since_log += 1;
                if since_log >= policy.log_every {
                    since_log = 0;
                    let s = ingress.stats();
                    info!(
                        messages = s.messages,
                        bytes = s.bytes,
                        dropped = s.dropped,
                        gaps = s.gaps_recorded,
                        "progress"
                    );
                }
            }
            // Answered explicitly. tungstenite queues a pong of its own, and a
            // duplicate is harmless per RFC 6455 -- whereas relying on library
            // internals for the one thing that keeps a 24-hour connection alive
            // is the kind of assumption that fails at 3am and looks like a venue
            // problem.
            Message::Ping(payload) => {
                if let Err(e) = socket.send(Message::Pong(payload)).await {
                    return Ok(EndReason::KeepaliveFailed(e.to_string()));
                }
            }
            Message::Close(_) => return Ok(EndReason::ServerClosed),
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

/// One snapshot attempt, after an optional delay.
///
/// The delay lives inside the future rather than as a `sleep` in the loop so that
/// waiting for a retry does not stop the socket being drained. A rate limit we are
/// backing off from must not cost us market data.
async fn fetch_snapshot(
    client: &SnapshotClient,
    symbol: &str,
    delay: Duration,
) -> Result<Vec<u8>, SnapshotError> {
    if !delay.is_zero() {
        sleep(delay).await;
    }
    client.depth(symbol).await
}

/// Route a finished snapshot attempt into the capture, or report why not.
///
/// `Ok(Ok(()))` means it is on its way to disk. `Ok(Err(reason))` means this
/// attempt failed and the caller may try again. The outer `Result` is the only
/// genuinely fatal case, a writer that is gone.
fn snapshot_outcome<S: RecordSink>(
    result: Result<Vec<u8>, SnapshotError>,
    ingress: &mut Ingress<S>,
    symbol: &str,
    purpose: SnapshotPurpose,
) -> Result<Result<(), SnapshotFailure>, WriterGone> {
    let bytes = match result {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(symbol, ?purpose, error = %e, "book snapshot request failed");
            return Ok(Err(e.reason()));
        }
    };

    let len = bytes.len();
    match ingress.accept_snapshot(bytes)? {
        Accepted::Enqueued => {
            info!(symbol, ?purpose, bytes = len, "book snapshot captured");
            Ok(Ok(()))
        }
        // We have the bytes and cannot enqueue them. Throwing them away and
        // asking again is better than holding a megabyte: a fresher snapshot is a
        // better record, and we are already short of capacity.
        Accepted::Dropped => {
            warn!(
                symbol,
                ?purpose,
                bytes = len,
                "capture channel full; discarding this snapshot and refetching"
            );
            Ok(Err(SnapshotFailure::Overflow))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_idle_timeout_leaves_room_for_a_venue_keepalive() {
        // If this ever drops below the venue's ping interval we would manufacture
        // disconnects on quiet instruments, each one a real hole in good data.
        let policy = ConnectionPolicy::default();
        assert!(
            policy.idle_timeout >= Duration::from_secs(60),
            "too tight to survive a venue keepalive interval"
        );
        assert!(
            policy.idle_timeout <= Duration::from_secs(300),
            "too loose to notice a half-open socket promptly"
        );
    }

    #[test]
    fn end_reasons_stay_distinguishable_in_the_log() {
        // All four become the same Gap{Disconnect} on disk, but a daily
        // ServerClosed is Binance behaving as documented while a recurring
        // IdleTimeout is our bug, and the log has to tell them apart.
        let reasons = [
            EndReason::ServerClosed,
            EndReason::IdleTimeout,
            EndReason::Transport("reset".to_owned()),
            EndReason::KeepaliveFailed("broken pipe".to_owned()),
        ];
        let rendered: Vec<String> = reasons.iter().map(ToString::to_string).collect();
        for (i, a) in rendered.iter().enumerate() {
            assert!(!a.is_empty());
            for b in rendered.iter().skip(i + 1) {
                assert_ne!(a, b);
            }
        }
    }
}
