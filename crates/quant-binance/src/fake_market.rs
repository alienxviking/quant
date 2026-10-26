//! A stand-in for the market stream, on a local port: what a rehearsal of a
//! paper or live run captures and trades on, so that no rehearsal reaches
//! Binance.
//!
//! Only for tests, behind the `fake-venue` feature with [`crate::fake_venue`],
//! for the same reason: a server that accepts any client is the last thing a
//! production binary should be able to start by accident.
//!
//! It is not a market. It sends what the test hands it, in the order handed,
//! on whichever connection is open, and nothing else: no book of its own, no
//! trades of its own, no answer to anything. A fake that made up its own
//! prices would be a second model of the venue, to agree with the recorded
//! market for ever (`docs/live-run.md` §5, *`LiveVenue`, decided before the
//! code*, §5). Every frame is the combined-stream envelope the venue sends
//! (`stream.rs`), built by [`trade`] and [`depth`] from the fields the parser
//! reads, so what a rehearsal records goes through the same parser as a
//! capture of the real venue.
//!
//! A frame handed over before a connection is open waits for one, and one
//! handed over while a connection is open goes on it; a test that needs a
//! frame to arrive after the book is anchored waits for that itself.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

/// What the server does next on its open connection.
#[derive(Debug)]
enum Command {
    Send(String),
    Drop,
}

#[derive(Debug, Default)]
struct Shared {
    /// Frames handed over while no connection was open, in order.
    waiting: VecDeque<String>,
    current: Option<mpsc::UnboundedSender<Command>>,
    connections: usize,
    /// Frames written to a socket, in order.
    sent: Vec<String>,
    /// The request target each connection asked for, in order.
    targets: Vec<String>,
    tasks: Vec<JoinHandle<()>>,
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The server. Stops when dropped.
pub struct FakeMarket {
    url: String,
    shared: Arc<Mutex<Shared>>,
    accept: JoinHandle<()>,
}

impl fmt::Debug for FakeMarket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeMarket")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl Drop for FakeMarket {
    fn drop(&mut self) {
        self.accept.abort();
        for task in &lock(&self.shared).tasks {
            task.abort();
        }
    }
}

impl FakeMarket {
    /// Listen on `127.0.0.1`, on a port the OS chooses.
    ///
    /// # Panics
    ///
    /// If it cannot bind.
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("address"));
        let shared = Arc::new(Mutex::new(Shared::default()));
        let accept = tokio::spawn(accept_loop(listener, Arc::clone(&shared)));
        Self {
            url,
            shared,
            accept,
        }
    }

    /// The base a capture subscribes at, as [`crate::SPOT_WS`] is: the
    /// capture appends the combined stream's path.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Send `frame` on the open connection, or on the next one if none is.
    pub fn send(&self, frame: &str) {
        let mut shared = lock(&self.shared);
        if let Some(current) = shared.current.as_ref() {
            if current.send(Command::Send(frame.to_owned())).is_ok() {
                return;
            }
        }
        shared.waiting.push_back(frame.to_owned());
    }

    /// Close the open connection from this side.
    pub fn drop_connection(&self) {
        if let Some(current) = lock(&self.shared).current.as_ref() {
            let _ = current.send(Command::Drop);
        }
    }

    /// How many connections have been opened.
    #[must_use]
    pub fn connections(&self) -> usize {
        lock(&self.shared).connections
    }

    /// Frames written to a socket so far, in order.
    #[must_use]
    pub fn sent(&self) -> Vec<String> {
        lock(&self.shared).sent.clone()
    }

    /// The request target each connection asked for, in order.
    #[must_use]
    pub fn targets(&self) -> Vec<String> {
        lock(&self.shared).targets.clone()
    }

    /// Wait until `ready` holds, or fail the test naming `what`.
    ///
    /// # Panics
    ///
    /// When `within` passes first.
    pub async fn until(&self, what: &str, within: Duration, ready: impl Fn(&Self) -> bool) {
        let deadline = tokio::time::Instant::now() + within;
        while !ready(self) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "gave up waiting for {what} after {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

async fn accept_loop(listener: TcpListener, shared: Arc<Mutex<Shared>>) {
    while let Ok((tcp, _)) = listener.accept().await {
        let task = tokio::spawn(serve(tcp, Arc::clone(&shared)));
        lock(&shared).tasks.push(task);
    }
}

async fn serve(tcp: tokio::net::TcpStream, shared: Arc<Mutex<Shared>>) {
    let target = Arc::new(Mutex::new(String::new()));
    let seen = Arc::clone(&target);
    #[allow(
        clippy::result_large_err,
        reason = "the handshake callback's signature is tungstenite's"
    )]
    let callback = move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                         response| {
        *seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = request.uri().to_string();
        Ok(response)
    };
    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(tcp, callback).await else {
        return;
    };
    let (tx, mut rx) = mpsc::unbounded_channel();
    {
        let mut shared = lock(&shared);
        shared.connections += 1;
        let asked = target
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        shared.targets.push(asked);
        // What waited for a connection goes first, in order, ahead of
        // anything sent from here on.
        while let Some(frame) = shared.waiting.pop_front() {
            let _ = tx.send(Command::Send(frame));
        }
        shared.current = Some(tx);
    }
    loop {
        tokio::select! {
            frame = ws.next() => match frame {
                Some(Ok(Message::Ping(payload))) => {
                    if ws.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            command = rx.recv() => match command {
                Some(Command::Send(text)) => {
                    if ws.send(Message::text(text.clone())).await.is_err() {
                        // Not sent: it waits for the next connection.
                        lock(&shared).waiting.push_front(text);
                        break;
                    }
                    lock(&shared).sent.push(text);
                }
                Some(Command::Drop) | None => {
                    let _ = ws.close(None).await;
                    break;
                }
            },
        }
    }
    let mut shared = lock(&shared);
    shared.current = None;
    // Whatever this connection was handed and never sent waits for the next.
    while let Ok(Command::Send(text)) = rx.try_recv() {
        shared.waiting.push_back(text);
    }
}

/// A trade print on `symbol`'s `@trade` stream, as the venue sends it:
/// trade `id`, `qty` at `px`, emitted at `millis`, the buyer the maker or not.
#[must_use]
pub fn trade(symbol: &str, id: u64, px: &str, qty: &str, millis: i64) -> String {
    json!({
        "stream": format!("{}@trade", symbol.to_lowercase()),
        "data": {
            "e": "trade",
            "E": millis,
            "s": symbol,
            "t": id,
            "p": px,
            "q": qty,
            "T": millis,
            "m": false,
            "M": true,
        }
    })
    .to_string()
}

/// A depth update on `symbol`'s `@depth@100ms` stream, as the venue sends it:
/// update ids `first..=last`, with `bids` and `asks` as `(price, quantity)`,
/// a quantity of zero removing the level.
#[must_use]
pub fn depth(
    symbol: &str,
    first: u64,
    last: u64,
    bids: &[(&str, &str)],
    asks: &[(&str, &str)],
    millis: i64,
) -> String {
    json!({
        "stream": format!("{}@depth@100ms", symbol.to_lowercase()),
        "data": {
            "e": "depthUpdate",
            "E": millis,
            "s": symbol,
            "U": first,
            "u": last,
            "b": bids,
            "a": asks,
        }
    })
    .to_string()
}

/// A depth snapshot's body, as `GET /api/v3/depth` answers: what a book is
/// anchored by, with `bids` and `asks` as `(price, quantity)`.
#[must_use]
pub fn snapshot(last_update_id: u64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> String {
    json!({ "lastUpdateId": last_update_id, "bids": bids, "asks": asks }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{parse_snapshot, parse_stream_message};
    use quant_core::event::MarketEvent;
    use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind, InstrumentRegistry};
    use quant_core::time::Ts;

    fn instrument() -> quant_core::instrument::InstrumentId {
        InstrumentRegistry::new().register(InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.01".parse().expect("tick"),
            lot_size: "0.00001".parse().expect("lot"),
            min_notional: "5".parse().expect("notional"),
        })
    }

    #[test]
    fn its_frames_read_as_the_venue_s_through_the_real_parser() {
        // Built from the fields the parser reads, and read back by it: a
        // frame the parser called something else would make a rehearsal
        // record a market that trades nothing.
        let at = Ts::from_nanos(5);
        let id = instrument();
        let Ok(Some(MarketEvent::Trade(t))) = parse_stream_message(
            trade("BTCUSDT", 7, "76650.12", "0.01", 1_000).as_bytes(),
            id,
            at,
            1,
        ) else {
            panic!("a trade");
        };
        assert_eq!(
            (t.venue_trade_id, t.px.to_string(), t.qty.to_string()),
            (7, "76650.12".to_owned(), "0.01".to_owned())
        );
        let Ok(Some(MarketEvent::BookDelta(d))) = parse_stream_message(
            depth(
                "BTCUSDT",
                101,
                102,
                &[("76650.00", "1")],
                &[("76651.00", "0")],
                1_000,
            )
            .as_bytes(),
            id,
            at,
            2,
        ) else {
            panic!("a depth update");
        };
        assert_eq!((d.first_update_id, d.final_update_id), (101, 102));
        assert_eq!((d.bids.len(), d.asks.len()), (1, 1));
        let s = parse_snapshot(
            snapshot(100, &[("76650.00", "1")], &[("76651.00", "1")]).as_bytes(),
            id,
            at,
            3,
        )
        .expect("a snapshot");
        assert_eq!(s.last_update_id, 100);
    }

    #[tokio::test]
    async fn a_frame_handed_over_before_a_connection_waits_for_one_and_keeps_its_order() {
        use futures_util::StreamExt as _;
        let market = FakeMarket::start().await;
        market.send("one");
        let (mut socket, _) = tokio_tungstenite::connect_async(format!(
            "{}/stream?streams=btcusdt@trade",
            market.url()
        ))
        .await
        .expect("connects");
        market
            .until("the connection", Duration::from_secs(10), |m| {
                m.connections() == 1
            })
            .await;
        market.send("two");
        let mut read = Vec::new();
        while read.len() < 2 {
            let frame = tokio::time::timeout(Duration::from_secs(10), socket.next())
                .await
                .expect("a frame in time")
                .expect("open")
                .expect("a frame");
            if let Message::Text(text) = frame {
                read.push(text.to_string());
            }
        }
        assert_eq!(read, ["one", "two"]);
        assert_eq!(market.sent(), ["one", "two"]);
        assert_eq!(market.targets(), ["/stream?streams=btcusdt@trade"]);
    }
}
