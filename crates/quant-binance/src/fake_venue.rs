//! A stand-in for Binance's WebSocket API, on a local port: the account side,
//! enough of it for [`crate::account`]'s listener to be tested end to end.
//!
//! Only for tests. It is compiled into this crate's tests and, behind the
//! `fake-venue` feature, into quant-verify's, which checks that what the
//! listener records verifies clean; nothing that ships turns the feature on.
//! It is a server that accepts any client, which is the last thing a
//! production binary should be able to start by accident.
//!
//! # What it imitates, and how closely
//!
//! The shapes are the documentation's (`web-socket-api.md` at commit
//! `0556f1a`): `session.logon` is answered with the documented result, which
//! echoes the API key (:3411-3425), `session.status` likewise (:3451-3466),
//! and `userDataStream.subscribe` with a `subscriptionId` (:8222-8230); a
//! failure is `{"id", "status", "error": {"code", "msg"}}` (:228-235); and
//! `serverShutdown` is the documented event (:374-381). It pings when told to,
//! as the venue does every 20 seconds (:111).
//!
//! It checks what a test would otherwise have to take on trust. A logon's
//! signature is verified against the public half of a key pair generated in
//! this process, over a payload it builds itself rather than with the
//! listener's own builder, so a signer that is wrong in a self-consistent way
//! is refused here as the venue would refuse it (`-1022`). And the logon's
//! timestamp must be within the default five-second `recvWindow` of this
//! side's clock, offset when a test asks, so a stale or skewed one is refused
//! with the venue's `-1021`.
//!
//! What it does not imitate: rate limits, SBE, the 24-hour limit on a
//! connection, and every method the listener does not send.
//!
//! # What it writes down
//!
//! Every frame it sent, tagged with what it was ([`Said`]), and every request
//! it was sent ([`Asked`]). The tag is this side's knowledge, not the
//! listener's: the fake knows it is answering a logon because it read the
//! method. So [`Log::recordable`], what the capture ought to hold, is derived
//! from the server's side of the conversation, independently of
//! [`crate::account::classify`] and [`crate::account::keep`].
//!
//! # Where the key comes from
//!
//! It is generated here, in process, for each fake venue, and the API key is
//! random text. No key material is read from anywhere or written anywhere,
//! and none outlives the test.

use std::collections::VecDeque;
use std::fmt::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use futures_util::{SinkExt as _, StreamExt as _};
use quant_core::event::GapCause;
use quant_core::instrument::Exchange;
use quant_core::time::{Clock, SystemClock};
use quant_recorder::{
    account_catalog, channel, run_writer, BackoffPolicy, CaptureRecord, CaptureSession, FileStore,
    Ingress, Metrics, TeeSink, DEFAULT_CHANNEL_CAPACITY, DEFAULT_FLUSH_INTERVAL,
};
use quant_storage::{RawFrame, RawReader, WriterOptions};
use ring::rand::{SecureRandom as _, SystemRandom};
use ring::signature::{Ed25519KeyPair, KeyPair as _, UnparsedPublicKey, ED25519};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use crate::account::{listen, AccountPolicy, Stopped};
use crate::credentials::Credentials;

/// The venue's default `recvWindow`, which a logon's timestamp is judged by.
const RECV_WINDOW_MS: i64 = 5_000;

/// How to answer one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Accept,
    Refuse { status: u16, code: i64, msg: String },
}

impl Reply {
    /// A refusal with the venue's shape.
    #[must_use]
    pub fn refuse(status: u16, code: i64, msg: &str) -> Self {
        Self::Refuse {
            status,
            code,
            msg: msg.to_owned(),
        }
    }
}

/// How to answer one `session.status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusReply {
    /// `userDataStream: false`, whatever the connection holds.
    Inactive,
    /// `userDataStream: null`.
    Unavailable,
    /// An answer with no `userDataStream` at all.
    Missing,
}

/// What the fake does, adjustable while it runs.
///
/// Each queue is consumed one entry per request of its kind, across
/// connections; an empty queue means the ordinary answer: accept, or for
/// `session.status` the truth about this connection's subscription.
#[derive(Debug, Clone, Default)]
pub struct Script {
    pub logon: VecDeque<Reply>,
    pub subscribe: VecDeque<Reply>,
    pub status: VecDeque<StatusReply>,
    /// Ping every this often, on every connection made after it is set.
    pub ping_every: Option<Duration>,
    /// How far this side's clock runs ahead of the listener's, for judging a
    /// logon's timestamp.
    pub clock_ahead_ms: i64,
}

/// What a frame the fake sent was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Said {
    LogonAnswer,
    SubscribeAnswer,
    StatusAnswer,
    /// An answer to a method the listener should never send.
    OtherAnswer,
    /// An event pushed: an account event, or `serverShutdown`.
    Event,
    /// A frame nobody asked for that is not an event, like the revocation
    /// notice.
    Unsolicited,
}

impl Said {
    /// Whether the capture should hold a frame of this kind. The answers that
    /// echo the key are the two it must not.
    #[must_use]
    pub const fn recordable(self) -> bool {
        !matches!(self, Self::LogonAnswer | Self::StatusAnswer)
    }
}

/// A frame the fake sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sent {
    /// Which connection, counting from 1.
    pub connection: usize,
    pub said: Said,
    pub text: String,
}

/// A request the fake was sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asked {
    pub connection: usize,
    pub method: String,
    /// The whole request, as it arrived.
    pub text: String,
    /// A logon's signature and timestamp.
    pub signature: Option<String>,
    pub timestamp: Option<i64>,
}

/// Everything the fake has seen and done.
#[derive(Debug, Clone, Default)]
pub struct Log {
    /// Connections accepted.
    pub connections: usize,
    pub sent: Vec<Sent>,
    pub asked: Vec<Asked>,
    /// Pongs received, on any connection.
    pub pongs: usize,
    /// Subscriptions accepted.
    pub subscribed: usize,
}

impl Log {
    /// How many times `method` was asked for.
    #[must_use]
    pub fn asked(&self, method: &str) -> usize {
        self.asked.iter().filter(|a| a.method == method).count()
    }

    /// What the capture should hold, in the order it was sent.
    #[must_use]
    pub fn recordable(&self) -> Vec<&[u8]> {
        self.sent
            .iter()
            .filter(|s| s.said.recordable())
            .map(|s| s.text.as_bytes())
            .collect()
    }

    /// Every signature the listener sent.
    #[must_use]
    pub fn signatures(&self) -> Vec<&str> {
        self.asked
            .iter()
            .filter_map(|a| a.signature.as_deref())
            .collect()
    }
}

enum Command {
    Send(String, Said),
    /// Close the connection from this side.
    Drop,
    /// Stop answering requests; keep pinging if pinging.
    StopAnswering,
    /// Stop answering and stop pinging.
    GoDark,
}

#[derive(Default)]
struct Shared {
    script: Script,
    log: Log,
    current: Option<mpsc::UnboundedSender<Command>>,
    tasks: Vec<JoinHandle<()>>,
}

/// A fake WebSocket API on `127.0.0.1`, port chosen by the OS.
pub struct FakeVenue {
    url: String,
    shared: Arc<Mutex<Shared>>,
    accept: JoinHandle<()>,
    api_key: String,
    /// The private key's PKCS#8 document, as base64: the text
    /// `BINANCE_ED25519_PRIVATE_KEY` would hold. Never printed.
    private_key: String,
}

impl fmt::Debug for FakeVenue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeVenue")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl Drop for FakeVenue {
    fn drop(&mut self) {
        self.accept.abort();
        for task in &lock(&self.shared).tasks {
            task.abort();
        }
    }
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    // A test that panicked while holding the lock has already failed; the
    // log is still worth reading for the report.
    shared
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl FakeVenue {
    /// Start listening, with a key pair of its own.
    pub async fn start(script: Script) -> Self {
        let rng = SystemRandom::new();
        let document = Ed25519KeyPair::generate_pkcs8(&rng).expect("ring makes a key pair");
        let pair = Ed25519KeyPair::from_pkcs8(document.as_ref()).expect("and reads it back");
        let public = pair.public_key().as_ref().to_vec();
        let mut bytes = [0_u8; 32];
        rng.fill(&mut bytes).expect("random bytes");
        let api_key = bytes.iter().fold(String::new(), |mut text, b| {
            let _ = write!(text, "{b:02x}");
            text
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let url = format!("ws://{}", listener.local_addr().expect("address"));
        let shared = Arc::new(Mutex::new(Shared {
            script,
            ..Shared::default()
        }));
        let accept = tokio::spawn(accept_loop(
            listener,
            Arc::clone(&shared),
            public,
            api_key.clone(),
        ));
        Self {
            url,
            shared,
            accept,
            api_key,
            private_key: STANDARD.encode(document.as_ref()),
        }
    }

    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The API key this venue knows.
    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Credentials this venue accepts.
    #[must_use]
    pub fn credentials(&self) -> Credentials {
        Credentials::new(self.api_key.clone(), &self.private_key).expect("a key ring made")
    }

    /// Credentials with this venue's API key and a private key it has never
    /// seen: what a wrong key file would give.
    #[must_use]
    pub fn credentials_with_another_key(&self) -> Credentials {
        let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("a key pair");
        Credentials::new(self.api_key.clone(), &STANDARD.encode(document.as_ref()))
            .expect("a key ring made")
    }

    /// Change the script.
    pub fn script(&self, change: impl FnOnce(&mut Script)) {
        change(&mut lock(&self.shared).script);
    }

    /// What has happened so far.
    #[must_use]
    pub fn log(&self) -> Log {
        lock(&self.shared).log.clone()
    }

    fn command(&self, command: Command) {
        let shared = lock(&self.shared);
        let current = shared.current.as_ref().expect("a connection to command");
        // A connection that has just closed takes the command with it, which
        // is what a real one would do.
        let _ = current.send(command);
    }

    /// Push an account event, verbatim, on the latest connection.
    pub fn event(&self, text: &str) {
        self.command(Command::Send(text.to_owned(), Said::Event));
    }

    /// Push a frame that is not an event and answers nothing.
    pub fn unsolicited(&self, text: &str) {
        self.command(Command::Send(text.to_owned(), Said::Unsolicited));
    }

    /// Send `serverShutdown` and keep the connection open, as the venue does
    /// until it actually goes.
    pub fn server_shutdown(&self) {
        let text = json!({ "event": { "e": "serverShutdown", "E": now_millis() } }).to_string();
        self.command(Command::Send(text, Said::Event));
    }

    /// Close the latest connection from this side.
    pub fn drop_connection(&self) {
        self.command(Command::Drop);
    }

    /// Stop answering on the latest connection, still pinging if pinging.
    pub fn stop_answering(&self) {
        self.command(Command::StopAnswering);
    }

    /// Stop answering and pinging on the latest connection, with the socket
    /// still open.
    pub fn go_dark(&self) {
        self.command(Command::GoDark);
    }

    /// Wait until `ready` holds of the log, or fail the test naming `what`.
    pub async fn until(&self, what: &str, within: Duration, ready: impl Fn(&Log) -> bool) -> Log {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let log = self.log();
            if ready(&log) {
                return log;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "gave up waiting for {what} after {within:?}: {log:#?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

fn now_millis() -> i64 {
    SystemClock.now().as_millis()
}

async fn accept_loop(
    listener: TcpListener,
    shared: Arc<Mutex<Shared>>,
    public: Vec<u8>,
    api_key: String,
) {
    while let Ok((tcp, _)) = listener.accept().await {
        let task = tokio::spawn(serve(
            tcp,
            Arc::clone(&shared),
            public.clone(),
            api_key.clone(),
        ));
        lock(&shared).tasks.push(task);
    }
}

/// One connection's state, as the venue would hold it.
struct Session {
    number: usize,
    logged_on: bool,
    subscribed: bool,
    voice: Voice,
}

/// How much of itself a connection still says.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Voice {
    /// Answers requests, and pings if pinging.
    Speaking,
    /// Pings if pinging, answers nothing.
    PingsOnly,
    /// Neither.
    Dark,
}

async fn serve(
    tcp: tokio::net::TcpStream,
    shared: Arc<Mutex<Shared>>,
    public: Vec<u8>,
    api_key: String,
) {
    let Ok(mut ws) = accept_async(tcp).await else {
        return;
    };
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (number, ping_every) = {
        let mut shared = lock(&shared);
        shared.log.connections += 1;
        shared.current = Some(tx);
        (shared.log.connections, shared.script.ping_every)
    };
    let mut session = Session {
        number,
        logged_on: false,
        subscribed: false,
        voice: Voice::Speaking,
    };
    let mut pings = ping_every.map(tokio::time::interval);

    loop {
        let tick = async {
            match pings.as_mut() {
                Some(interval) => {
                    interval.tick().await;
                }
                None => std::future::pending::<()>().await,
            }
        };
        let outgoing = tokio::select! {
            frame = ws.next() => match frame {
                Some(Ok(Message::Text(text))) => {
                    let answer = answer(text.as_str(), &mut session, &shared, &public, &api_key);
                    if session.voice == Voice::Speaking { answer } else { None }
                }
                Some(Ok(Message::Pong(_))) => {
                    lock(&shared).log.pongs += 1;
                    None
                }
                Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                Some(Ok(_)) => None,
            },
            command = rx.recv() => match command {
                Some(Command::Send(text, said)) => Some((text, said)),
                Some(Command::Drop) | None => return,
                Some(Command::StopAnswering) => {
                    session.voice = Voice::PingsOnly;
                    None
                }
                Some(Command::GoDark) => {
                    session.voice = Voice::Dark;
                    None
                }
            },
            () = tick, if session.voice != Voice::Dark => {
                if ws.send(Message::Ping(b"fake-venue".to_vec().into())).await.is_err() {
                    return;
                }
                None
            }
        };
        if let Some((text, said)) = outgoing {
            lock(&shared).log.sent.push(Sent {
                connection: session.number,
                said,
                text: text.clone(),
            });
            if ws.send(Message::text(text)).await.is_err() {
                return;
            }
        }
    }
}

/// Read one request, write it down, and say what to answer.
fn answer(
    text: &str,
    session: &mut Session,
    shared: &Mutex<Shared>,
    public: &[u8],
    api_key: &str,
) -> Option<(String, Said)> {
    let request: Value = serde_json::from_str(text).ok()?;
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let params = request.get("params").cloned().unwrap_or(Value::Null);
    let mut shared = lock(shared);
    shared.log.asked.push(Asked {
        connection: session.number,
        method: method.clone(),
        text: text.to_owned(),
        signature: params
            .get("signature")
            .and_then(Value::as_str)
            .map(str::to_owned),
        timestamp: params.get("timestamp").and_then(Value::as_i64),
    });
    let reply = Replying {
        id: request.get("id").cloned().unwrap_or(Value::Null),
        now: now_millis() + shared.script.clock_ahead_ms,
        api_key,
    };
    let shared = &mut *shared;
    Some(match method.as_str() {
        "session.logon" => (
            reply.logon(&params, public, session, &mut shared.script),
            Said::LogonAnswer,
        ),
        "userDataStream.subscribe" => (
            reply.subscribe(session, &mut shared.script, &mut shared.log),
            Said::SubscribeAnswer,
        ),
        "session.status" => (
            reply.status(session, &mut shared.script),
            Said::StatusAnswer,
        ),
        _ => (
            reply.refuse(400, -1020, "This operation is not supported."),
            Said::OtherAnswer,
        ),
    })
}

/// What every answer to one request shares.
struct Replying<'a> {
    id: Value,
    /// This side's clock, in milliseconds, with the script's offset.
    now: i64,
    api_key: &'a str,
}

impl Replying<'_> {
    fn refuse(&self, status: u16, code: i64, msg: &str) -> String {
        json!({ "id": self.id, "status": status, "error": { "code": code, "msg": msg } })
            .to_string()
    }

    fn reply(&self, reply: Reply, result: &Value) -> String {
        match reply {
            Reply::Accept => json!({ "id": self.id, "status": 200, "result": result }).to_string(),
            Reply::Refuse { status, code, msg } => self.refuse(status, code, &msg),
        }
    }

    fn logon(
        &self,
        params: &Value,
        public: &[u8],
        session: &mut Session,
        script: &mut Script,
    ) -> String {
        if !signed_by(params, public, self.api_key) {
            return self.refuse(400, -1022, "Signature for this request is not valid.");
        }
        let stamp = params
            .get("timestamp")
            .and_then(Value::as_i64)
            .unwrap_or_default();
        if stamp >= self.now + 1_000 || self.now - stamp > RECV_WINDOW_MS {
            return self.refuse(
                400,
                -1021,
                "Timestamp for this request is outside of the recvWindow.",
            );
        }
        let scripted = script.logon.pop_front().unwrap_or(Reply::Accept);
        session.logged_on = scripted == Reply::Accept;
        let result = json!({
            "apiKey": self.api_key,
            "authorizedSince": stamp,
            "connectedSince": stamp,
            "returnRateLimits": false,
            "serverTime": self.now,
            "userDataStream": session.subscribed,
        });
        self.reply(scripted, &result)
    }

    fn subscribe(&self, session: &mut Session, script: &mut Script, log: &mut Log) -> String {
        if !session.logged_on {
            return self.refuse(
                401,
                -1002,
                "You are not authorized to execute this request.",
            );
        }
        let scripted = script.subscribe.pop_front().unwrap_or(Reply::Accept);
        if scripted == Reply::Accept {
            session.subscribed = true;
            log.subscribed += 1;
        }
        self.reply(scripted, &json!({ "subscriptionId": 0 }))
    }

    fn status(&self, session: &Session, script: &mut Script) -> String {
        let flag = match script.status.pop_front() {
            None => json!(session.subscribed),
            Some(StatusReply::Inactive) => json!(false),
            Some(StatusReply::Unavailable) => Value::Null,
            Some(StatusReply::Missing) => {
                return self.reply(Reply::Accept, &json!({ "apiKey": self.api_key }));
            }
        };
        let result = json!({
            "apiKey": if session.logged_on { json!(self.api_key) } else { Value::Null },
            "authorizedSince": self.now,
            "connectedSince": self.now,
            "returnRateLimits": false,
            "serverTime": self.now,
            "userDataStream": flag,
        });
        self.reply(Reply::Accept, &result)
    }
}

/// Whether a logon is signed by this venue's key, over the payload the venue
/// documents: every param but `signature`, sorted, `name=value` joined by `&`
/// (web-socket-api.md:761, :1115-1153). Built here from the request, not by
/// the listener's own builder.
fn signed_by(params: &Value, public: &[u8], api_key: &str) -> bool {
    let Some(object) = params.as_object() else {
        return false;
    };
    if object.get("apiKey").and_then(Value::as_str) != Some(api_key) {
        return false;
    }
    let Some(signature) = object
        .get("signature")
        .and_then(Value::as_str)
        .and_then(|s| STANDARD.decode(s).ok())
    else {
        return false;
    };
    let mut names: Vec<&String> = object.keys().filter(|k| *k != "signature").collect();
    names.sort();
    let payload = names
        .iter()
        .map(|name| {
            let value = &object[name.as_str()];
            let value = value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned);
            format!("{name}={value}")
        })
        .collect::<Vec<_>>()
        .join("&");
    UnparsedPublicKey::new(&ED25519, public)
        .verify(payload.as_bytes(), &signature)
        .is_ok()
}

/// A policy at test speed: millisecond backoff, a probe every 40 ms that
/// must be answered within a second, and two seconds for any other answer.
///
/// The second is margin for a loaded test machine, not a model of anything:
/// a local answer comes back in milliseconds, and a deadline a busy CI host
/// can miss would record gaps that are artefacts of the test.
#[must_use]
pub fn quick_policy() -> AccountPolicy {
    AccountPolicy {
        backoff: BackoffPolicy {
            initial: Duration::from_millis(10),
            max: Duration::from_millis(40),
            multiplier: 2,
        },
        probe_interval: Duration::from_millis(40),
        probe_timeout: Duration::from_secs(1),
        answer_timeout: Duration::from_secs(2),
    }
}

/// A listener recording into an account tree, as a live run would: a
/// capture session of the account's own, an `Ingress` behind a
/// [`TeeSink`] with the capture primary, and a consumer on the secondary.
#[derive(Debug)]
pub struct Listening {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Heard>,
    consumer: Receiver<CaptureRecord>,
}

/// How a [`Listening`] ended.
#[derive(Debug)]
pub struct Heard {
    /// Why the listener stopped by itself, or `None` when it was stopped.
    pub stopped: Option<Stopped>,
    /// Everything the consumer was handed, in order.
    pub consumed: Vec<CaptureRecord>,
}

impl Listening {
    /// Start listening to `url`, recording under `root`'s account tree.
    #[must_use]
    pub fn start(root: &Path, url: &str, credentials: Credentials, policy: AccountPolicy) -> Self {
        let (stop, stopped) = oneshot::channel();
        let (consumer_tx, consumer) = sync_channel(DEFAULT_CHANNEL_CAPACITY);
        let task = tokio::spawn(capture(
            root.to_path_buf(),
            url.to_owned(),
            credentials,
            policy,
            stopped,
            consumer_tx,
        ));
        Self {
            stop: Some(stop),
            task,
            consumer,
        }
    }

    /// Stop listening, seal the capture, and say what was heard.
    pub async fn stop(mut self, within: Duration) -> Heard {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.finish(within).await
    }

    /// Wait for the listener to stop by itself, which it does only for good.
    pub async fn stopped_by_itself(self, within: Duration) -> Heard {
        let heard = self.finish(within).await;
        assert!(
            heard.stopped.is_some(),
            "the listener was expected to stop by itself"
        );
        heard
    }

    async fn finish(self, within: Duration) -> Heard {
        let mut heard = tokio::time::timeout(within, self.task)
            .await
            .unwrap_or_else(|_| panic!("the listener did not finish within {within:?}"))
            .expect("the listening task");
        heard.consumed = self.consumer.try_iter().collect();
        heard
    }
}

async fn capture(
    root: PathBuf,
    url: String,
    credentials: Credentials,
    policy: AccountPolicy,
    stop: oneshot::Receiver<()>,
    consumer: SyncSender<CaptureRecord>,
) -> Heard {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let mut session = CaptureSession::account(
        FileStore::new(root),
        Exchange::Binance,
        Arc::clone(&clock),
        WriterOptions::default(),
    );
    let metrics = Metrics::new(DEFAULT_CHANNEL_CAPACITY);
    let (tx, rx) = channel(DEFAULT_CHANNEL_CAPACITY, Arc::clone(&metrics));
    let writer = std::thread::spawn(move || {
        let outcome = run_writer(&rx, &mut session, DEFAULT_FLUSH_INTERVAL);
        (outcome, session)
    });
    let mut ingress = Ingress::new(TeeSink::new(tx, consumer), clock, metrics);
    ingress
        .record_gap(GapCause::RecorderRestart)
        .expect("the writer is running");

    let stopped = tokio::select! {
        stopped = listen(&url, &credentials, &mut ingress, &policy) => Some(stopped),
        _ = stop => None,
    };
    drop(ingress);
    let (outcome, session) = tokio::task::spawn_blocking(move || writer.join())
        .await
        .expect("joining the writer")
        .expect("the writer thread");
    outcome.expect("the writer");
    session.finish().expect("sealing the account capture");
    Heard {
        stopped,
        consumed: Vec::new(),
    }
}

/// Every frame of the account tree under `root`, in order, read back with
/// `quant-storage`'s reader. Asserts there is one session and that every file
/// is sealed, intact, and holds nothing else.
#[must_use]
pub fn account_frames(root: &Path) -> Vec<RawFrame> {
    let catalog = account_catalog(root);
    assert!(catalog.strays.is_empty(), "strays: {:?}", catalog.strays);
    assert!(
        catalog.unreadable.is_empty(),
        "unreadable: {:?}",
        catalog.unreadable
    );
    assert_eq!(catalog.sessions.len(), 1, "one account session");
    let mut frames = Vec::new();
    for entry in &catalog.sessions[0].segments {
        let file = std::fs::File::open(&entry.path).expect("open a capture file");
        let mut reader = RawReader::open(std::io::BufReader::new(file)).expect("a capture file");
        let (read, truncation) = reader.read_all().expect("readable");
        assert_eq!(truncation, None, "{}", entry.path.display());
        assert!(reader.is_finalized(), "sealed: {}", entry.path.display());
        frames.extend(read);
    }
    frames
}

/// Every byte of every file in the account tree under `root`, as stored.
#[must_use]
pub fn account_file_bytes(root: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    for session in &account_catalog(root).sessions {
        for entry in &session.segments {
            bytes.extend(std::fs::read(&entry.path).expect("read a capture file"));
        }
    }
    bytes
}
