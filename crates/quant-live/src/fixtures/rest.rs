//! A stand-in for the REST API's order endpoints, on a local port, for the
//! live venue's tests.
//!
//! Each test says what the venue answers: an order or a cancel takes the next
//! [`Reply`] from its queue, and an empty queue acknowledges, as the venue's
//! documented `ACK` (rest-api.md:2205-2211) and cancel answer
//! (rest-api.md:2410-2427) do, naming the order the request named. The venue's
//! order id for our order `n` is [`venue_order_id`]`(n)`, so a test can write
//! the stream's reports for it. The three checks `venue-check` runs always
//! pass, so a test can mint a `Checked` against it.
//!
//! The public endpoints a run asks before it trades answer too, for item 8's
//! rehearsals: the venue's clock (this host's), `BTCUSDT`'s rules
//! ([`EXCHANGE_INFO`]), a depth snapshot ([`DEPTH`]) and the account's
//! balances ([`ACCOUNT`]). [`Script::once`] answers the next request to a path
//! otherwise, which is how a rehearsal makes one gate fail, and
//! [`Script::always`] changes a path's standing answer.
//!
//! It is not an exchange (`docs/live-run.md` §5, *`LiveVenue`, decided before
//! the code*, §5): it matches nothing and reports nothing on the stream. A fake
//! that did would be a second model of the venue, to agree with the simulator
//! and with Binance for ever.
//!
//! Every request it reads is kept, with when it arrived, and nothing in it
//! waits without a bound: the listener is polled, each read times out, a held
//! connection is released or dropped when the server stops, and the server
//! stops by itself after [`LIFETIME`].

use std::collections::VecDeque;
use std::io::{ErrorKind, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long a server runs if nobody stops it.
pub const LIFETIME: Duration = Duration::from_secs(120);

/// How long each read of a request may take.
const READ_DEADLINE: Duration = Duration::from_secs(10);

/// The venue's order id for our order `n`.
#[must_use]
pub const fn venue_order_id(n: u64) -> u64 {
    1_000 + n
}

/// How to answer one order or cancel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    /// The documented acknowledgement, naming the request's order.
    Ack,
    /// This status and body.
    Http { status: u16, body: String },
    /// Read the request and say nothing, the connection held open until the
    /// server stops: a venue that took the request and never answered.
    Silence,
    /// Read the request and wait until [`RestVenue::release`], then
    /// acknowledge: a venue that is slow, for as long as a test needs.
    Hold,
}

impl Reply {
    /// A refusal in the venue's shape.
    #[must_use]
    pub fn refuse(status: u16, code: i64, msg: &str) -> Self {
        Self::Http {
            status,
            body: format!(r#"{{"code":{code},"msg":"{msg}"}}"#),
        }
    }
}

/// What the server answers, adjustable while it runs.
#[derive(Debug, Clone, Default)]
pub struct Script {
    pub orders: VecDeque<Reply>,
    pub cancels: VecDeque<Reply>,
    /// One-off answers for any other path, each taken by the next request to
    /// that path; an empty queue gives the path's standing answer.
    pub paths: std::collections::BTreeMap<String, VecDeque<Reply>>,
    /// Standing answers, a 200 with this body, for a path whose default
    /// below will not do: a depth snapshot at another price, say.
    pub bodies: std::collections::BTreeMap<String, String>,
}

impl Script {
    /// The next request to `path` answered `reply`, once.
    #[must_use]
    pub fn once(mut self, path: &str, reply: Reply) -> Self {
        self.paths
            .entry(path.to_owned())
            .or_default()
            .push_back(reply);
        self
    }

    /// Every request to `path` answered 200 with `body`.
    #[must_use]
    pub fn always(mut self, path: &str, body: &str) -> Self {
        self.bodies.insert(path.to_owned(), body.to_owned());
        self
    }
}

/// What the account holds by default, which a live run's baseline reads:
/// distinct amounts, so one read from the wrong place shows.
pub const ACCOUNT: &str = r#"{"canTrade":true,"accountType":"SPOT","balances":[{"asset":"BNB","free":"0.00000000","locked":"0.00000000"},{"asset":"BTC","free":"0.50000000","locked":"0.00000000"},{"asset":"USDT","free":"1000.00000000","locked":"0.00000000"}],"permissions":["SPOT"]}"#;

/// `BTCUSDT`'s rules by default, as `GET /api/v3/exchangeInfo` states them,
/// with the filters the definition reads: [`super::btcusdt`]'s, so that a
/// run wired from this answer trades the fixtures' instrument.
pub const EXCHANGE_INFO: &str = r#"{"timezone":"UTC","serverTime":1789000000000,"rateLimits":[],"exchangeFilters":[],"symbols":[{"symbol":"BTCUSDT","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","filters":[{"filterType":"PRICE_FILTER","minPrice":"0.01000000","maxPrice":"1000000.00000000","tickSize":"0.01000000"},{"filterType":"LOT_SIZE","minQty":"0.00001000","maxQty":"9000.00000000","stepSize":"0.00001000"},{"filterType":"NOTIONAL","minNotional":"5.00000000","applyMinToMarket":true,"maxNotional":"9000000.00000000","applyMaxToMarket":false,"avgPriceMins":5}]}]}"#;

/// The book by default, as `GET /api/v3/depth` answers: one level a side
/// about the fixtures' price, at update id 100.
pub const DEPTH: &str =
    r#"{"lastUpdateId":100,"bids":[["76650.00","1.00000000"]],"asks":[["76651.00","1.00000000"]]}"#;

/// A request the server read.
#[derive(Debug, Clone)]
pub struct Seen {
    pub method: String,
    pub path: String,
    /// The query, as it arrived.
    pub query: String,
    pub at: Instant,
}

impl Seen {
    /// The value of parameter `name`, as it arrived.
    #[must_use]
    pub fn param(&self, name: &str) -> Option<&str> {
        self.query.split('&').find_map(|pair| {
            pair.split_once('=')
                .and_then(|(key, value)| (key == name).then_some(value))
        })
    }

    /// Whether this is an order or a cancel, rather than a check.
    #[must_use]
    pub fn is_order_request(&self) -> bool {
        self.path == quant_binance::orders::ORDER_PATH
    }

    /// Our order id this order or cancel names, read off the client order id
    /// it was sent under.
    #[must_use]
    pub fn names(&self) -> Option<u64> {
        self.param("newClientOrderId")
            .or_else(|| self.param("origClientOrderId"))
            .and_then(|id| id.rsplit('-').next())
            .and_then(|n| n.parse().ok())
    }
}

#[derive(Debug, Default)]
struct Shared {
    script: Script,
    seen: Vec<Seen>,
}

/// The server. Stops when dropped.
#[derive(Debug)]
pub struct RestVenue {
    base: String,
    shared: Arc<Mutex<Shared>>,
    stop: Arc<AtomicBool>,
    released: Arc<AtomicUsize>,
    server: Option<JoinHandle<()>>,
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl RestVenue {
    /// Listen on `127.0.0.1`, on a port the OS chooses.
    ///
    /// # Panics
    ///
    /// If it cannot bind.
    #[must_use]
    pub fn start(script: Script) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("a listener to poll");
        let base = format!("http://{}", listener.local_addr().expect("address"));
        let shared = Arc::new(Mutex::new(Shared {
            script,
            seen: Vec::new(),
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicUsize::new(0));
        let server = {
            let (shared, stop, released) = (
                Arc::clone(&shared),
                Arc::clone(&stop),
                Arc::clone(&released),
            );
            std::thread::spawn(move || accept(&listener, &shared, &stop, &released))
        };
        Self {
            base,
            shared,
            stop,
            released,
            server: Some(server),
        }
    }

    /// Where a `TradeClient` sends its requests.
    #[must_use]
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Change the script.
    pub fn script(&self, change: impl FnOnce(&mut Script)) {
        change(&mut lock(&self.shared).script);
    }

    /// Let the next held request be answered.
    pub fn release(&self) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }

    /// Every request read so far, in the order they arrived.
    #[must_use]
    pub fn seen(&self) -> Vec<Seen> {
        lock(&self.shared).seen.clone()
    }

    /// Orders and cancels read so far, checks left out.
    #[must_use]
    pub fn order_requests(&self) -> Vec<Seen> {
        self.seen()
            .into_iter()
            .filter(Seen::is_order_request)
            .collect()
    }

    /// Wait until `ready` holds of the orders and cancels read, or fail the
    /// test naming `what`.
    ///
    /// # Panics
    ///
    /// When `within` passes first.
    pub fn until(
        &self,
        what: &str,
        within: Duration,
        ready: impl Fn(&[Seen]) -> bool,
    ) -> Vec<Seen> {
        let deadline = Instant::now() + within;
        loop {
            let seen = self.order_requests();
            if ready(&seen) {
                return seen;
            }
            assert!(
                Instant::now() < deadline,
                "gave up waiting for {what} after {within:?}: {seen:#?}"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

impl Drop for RestVenue {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

/// Accept connections until stopped, each answered on a thread of its own so
/// that a held one holds nothing else up.
fn accept(
    listener: &TcpListener,
    shared: &Arc<Mutex<Shared>>,
    stop: &Arc<AtomicBool>,
    released: &Arc<AtomicUsize>,
) {
    let deadline = Instant::now() + LIFETIME;
    let mut connections = Vec::new();
    let mut holds = 0_usize;
    while !stop.load(Ordering::SeqCst) && Instant::now() < deadline {
        match listener.accept() {
            Ok((stream, _)) => {
                let Some(seen) = read_request(&stream) else {
                    continue;
                };
                let reply = {
                    let mut shared = lock(shared);
                    let path = seen.path.clone();
                    let standing = shared.script.bodies.get(&path).cloned();
                    let queue = match (path.as_str(), seen.method.as_str()) {
                        (quant_binance::orders::ORDER_PATH, "POST") => {
                            Some(&mut shared.script.orders)
                        }
                        (quant_binance::orders::ORDER_PATH, _) => Some(&mut shared.script.cancels),
                        (other, _) => shared.script.paths.get_mut(other),
                    };
                    let reply = queue
                        .and_then(VecDeque::pop_front)
                        .or_else(|| standing.map(|body| Reply::Http { status: 200, body }))
                        .unwrap_or(Reply::Ack);
                    shared.seen.push(seen.clone());
                    reply
                };
                let hold = if reply == Reply::Hold {
                    holds += 1;
                    Some(holds)
                } else {
                    None
                };
                let (stop, released) = (Arc::clone(stop), Arc::clone(released));
                connections.push(std::thread::spawn(move || {
                    answer(stream, &seen, &reply, hold, &stop, &released);
                }));
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(_) => break,
        }
    }
    stop.store(true, Ordering::SeqCst);
    for connection in connections {
        let _ = connection.join();
    }
}

/// Read one request's head, through the blank line that ends it.
fn read_request(mut stream: &TcpStream) -> Option<Seen> {
    // A socket accepted from a non-blocking listener can be non-blocking
    // itself, as it is on Windows.
    stream.set_nonblocking(false).ok()?;
    stream.set_read_timeout(Some(READ_DEADLINE)).ok()?;
    let mut head = Vec::new();
    let mut buf = [0; 4096];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf).ok()?;
        if n == 0 {
            return None;
        }
        head.extend_from_slice(&buf[..n]);
    }
    let at = Instant::now();
    let head = String::from_utf8(head).ok()?;
    let line = head.lines().next()?;
    let mut words = line.split(' ');
    let method = words.next()?.to_owned();
    let target = words.next()?;
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    Some(Seen {
        method,
        path: path.to_owned(),
        query: query.to_owned(),
        at,
    })
}

/// Answer one request as `reply` says.
fn answer(
    mut stream: TcpStream,
    seen: &Seen,
    reply: &Reply,
    hold: Option<usize>,
    stop: &AtomicBool,
    released: &AtomicUsize,
) {
    let (status, body) = match reply {
        Reply::Http { status, body } => (*status, body.clone()),
        Reply::Ack => (200, ack(seen)),
        Reply::Hold => {
            let Some(n) = hold else { return };
            while released.load(Ordering::SeqCst) < n {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            (200, ack(seen))
        }
        Reply::Silence => {
            while !stop.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(2));
            }
            return;
        }
    };
    let response = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        if status == 200 { "OK" } else { "Answered" },
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

/// What the venue answers a request it took: the order acknowledged, the
/// cancel acknowledged, or a check passed.
fn ack(seen: &Seen) -> String {
    let symbol = seen.param("symbol").unwrap_or("BTCUSDT");
    let order_id = seen.names().map_or(0, venue_order_id);
    match (seen.path.as_str(), seen.method.as_str()) {
        (quant_binance::orders::ORDER_PATH, "POST") => format!(
            r#"{{"symbol":"{symbol}","orderId":{order_id},"orderListId":-1,"clientOrderId":"{}","transactTime":1507725176595}}"#,
            seen.param("newClientOrderId").unwrap_or_default()
        ),
        (quant_binance::orders::ORDER_PATH, _) => format!(
            r#"{{"symbol":"{symbol}","origClientOrderId":"{}","orderId":{order_id},"orderListId":-1,"clientOrderId":"cancelMyOrder1","transactTime":1684804350068,"status":"CANCELED"}}"#,
            seen.param("origClientOrderId").unwrap_or_default()
        ),
        ("/api/v3/account", _) => ACCOUNT.to_owned(),
        ("/api/v3/exchangeInfo", _) => EXCHANGE_INFO.to_owned(),
        ("/api/v3/depth", _) => DEPTH.to_owned(),
        // The venue's clock is this host's: a rehearsal's clock check reads
        // the host on time.
        ("/api/v3/time", _) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis());
            format!(r#"{{"serverTime":{now}}}"#)
        }
        ("/api/v3/account/commission", _) => {
            r#"{"symbol":"BTCUSDT","discount":{"enabledForAccount":false,"enabledForSymbol":false,"discountAsset":"BNB","discount":"0.75000000"}}"#.to_owned()
        }
        // `/api/v3/order/test`, and anything else: the documented `{}`.
        _ => "{}".to_owned(),
    }
}
