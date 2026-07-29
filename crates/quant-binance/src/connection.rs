//! The read loop: connect, drain, reconnect, forever.

use core::fmt;
use core::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use quant_core::event::GapCause;
use quant_recorder::{Backoff, BackoffPolicy, Ingress, RecordSink, WriterGone};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{debug, info, warn};

use crate::stream::StreamSpec;

/// How the recorder behaves around a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionPolicy {
    pub backoff: BackoffPolicy,
    /// Treat the connection as dead if nothing at all arrives within this.
    pub idle_timeout: Duration,
    /// Emit a progress line every this many messages.
    pub log_every: u64,
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
    fn default() -> Self {
        Self {
            backoff: BackoffPolicy::default(),
            idle_timeout: Duration::from_secs(120),
            log_every: 50_000,
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

/// Record `spec` until the writer goes away or the caller cancels.
///
/// Returns only on a fatal condition. A normal disconnect is not fatal: it is
/// recorded as a gap and retried, which is the entire point of an unattended
/// recorder. Cancellation is the caller's job -- drop this future (see the
/// `record` binary, which races it against ctrl-c) and the borrowed `ingress`
/// becomes droppable, which closes the channel and makes the writer seal the
/// file with a trailer.
pub async fn run<S: RecordSink>(
    spec: &StreamSpec,
    base_url: &str,
    ingress: &mut Ingress<S>,
    policy: &ConnectionPolicy,
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

                let reason = drain(socket, ingress, policy).await?;
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

/// Pull messages until the connection stops being useful.
async fn drain<S: RecordSink>(
    mut socket: Socket,
    ingress: &mut Ingress<S>,
    policy: &ConnectionPolicy,
) -> Result<EndReason, WriterGone> {
    let mut since_log = 0_u64;

    loop {
        let next = match timeout(policy.idle_timeout, socket.next()).await {
            Err(_elapsed) => return Ok(EndReason::IdleTimeout),
            Ok(None) => return Ok(EndReason::ServerClosed),
            Ok(Some(Err(e))) => return Ok(EndReason::Transport(e.to_string())),
            Ok(Some(Ok(message))) => message,
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
