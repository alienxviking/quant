//! The account stream: what the venue says about our own orders, as it arrives.
//!
//! Citations are to Binance's documentation at commit `0556f1a`, the same
//! reading `docs/live-run.md` §5 was settled on: `web-socket-api.md`,
//! `user-data-stream.md`, `errors.md` and `CHANGELOG.md`.
//!
//! # What is on this socket
//!
//! The account stream is not a stream of its own. Listen keys are gone
//! (CHANGELOG.md:405-418), so it is a subscription made on a WebSocket API
//! connection, and that connection carries three kinds of frame: the venue's
//! answers to our requests (web-socket-api.md:173-326), the account's events
//! in a `{"subscriptionId": .., "event": {..}}` envelope
//! (web-socket-api.md:328-366), and connection events such as `serverShutdown`
//! (web-socket-api.md:368-381), which have the envelope without the id.
//!
//! [`classify`] sorts one frame into those three, and it is a pure function of
//! the bytes on purpose. The listener uses it to decide what to record and
//! what to do next; whatever reads the recording later, `LiveVenue` or a
//! replay of the file, needs the same answer from the same bytes, and two
//! classifiers that had to agree forever would sooner or later not. M5 drew the
//! same line for market data in [`crate::decode`](mod@crate::decode).
//!
//! # Why our request ids name the request
//!
//! *"Request `id` is truly arbitrary ... The server does not interpret `id` in
//! any way, simply echoing it back in the response"* (web-socket-api.md:162-163).
//! So each id is a word for the method and a counter, `subscribe-3`, and an
//! answer says which request it answers by itself. A listener that remembered
//! the ids it sent would know the same thing, but only while it was running;
//! a recorded answer read back next month would not. The subscription's answer
//! is the record that the account was being heard from then on, so it has to
//! be recognisable from the file alone.
//!
//! A word, not the method's own name. The live API is remembered refusing an
//! id outside `^[a-zA-Z0-9-_]{1,36}$` with `-1100`, which the pinned pages do
//! not say (they call ids arbitrary, and the only such pattern in them is the
//! FIX API's, fix-api.md:394); `session.logon` has a `.` in it. Whether the
//! memory is right is for the testnet rehearsal to settle. An id inside the
//! pattern costs nothing either way, and the longest, `subscribe-` and a
//! `u64`, is 30 characters.
//!
//! # What is recorded, and what never is
//!
//! The raw tier is kept forever, and a credential written into it cannot be
//! taken back. So two things never reach it.
//!
//! - **Anything we send.** Outbound frames never pass through ingress at all;
//!   `session.logon` carries the API key and its signature
//!   (web-socket-api.md:3378-3389).
//! - **The answers to `session.logon` and `session.status`.** Both echo the API
//!   key in their `result` (web-socket-api.md:3411-3425 and :3451-3466). In SBE
//!   the field is `loggedOnApiKey` (CHANGELOG.md:718), which is why the rule is
//!   on the request answered rather than on a field name: a renamed field cannot
//!   let one through. These answers are our session's bookkeeping, not facts
//!   about the account, so nothing is lost.
//!
//! Everything else the venue sends is recorded verbatim: the account's events,
//! `serverShutdown`, `eventStreamTerminated`, the subscription's answer whether
//! it accepts or refuses, an error the venue sends unasked (the revocation
//! notice, web-socket-api.md:1317-1330, has `"id": null`), and frames this
//! module does not recognise, since our model of the venue may be what is wrong.
//!
//! Then one byte-level guard, under the rule rather than instead of it: a frame
//! whose bytes contain the API key is withheld whatever [`classify`] made of
//! it. No documented frame but the two answers above carries the key, so the
//! guard should never fire, and if it does it is logged as the anomaly it is.
//! It is the one check here that does not depend on our reading of the
//! venue's JSON being right.
//!
//! # One connection, start to finish
//!
//! [`listen`] connects, sends `session.logon` signed with the Ed25519 key and a
//! timestamp read from the venue's clock at that moment, waits for the answer,
//! sends `userDataStream.subscribe`, waits for that answer, and then drains.
//! *"A single connection to the API is only valid for 24 hours"*
//! (web-socket-api.md:105), a subscription belongs to the connection it was
//! made on, and nothing is replayed after a reconnect (`docs/live-run.md` §5),
//! so every new connection does all three again, with a fresh timestamp: a
//! logon is signed once and used once. The backoff between connections is
//! [`ConnectionPolicy`]'s, seeded by [`ACCOUNT_STREAM`] so that a venue
//! restart does not make this socket retry in step with the market's.
//!
//! The venue's clock is the one every order is stamped on
//! ([`crate::trade`]): the injected clock with the offset measured at startup
//! taken off. The injected clock itself stays the host's, because it also
//! stamps `local_recv_ts` (invariant 2), so the correction is made on the
//! logon's stamp alone. Stamped with the host's clock as it stands, a logon
//! would be held to a tighter rule than the orders it exists to hear about: a
//! host 900 ms ahead passes the order path's gate
//! ([`crate::trade::MAX_CLOCK_SKEW_MS`]) and its orders land on time, while its
//! logons sit 100 ms inside the venue's one second (web-socket-api.md:678-681).
//! A few hundred ms of drift later, the 24-hour reconnect is refused `-1021`,
//! which does not clear by itself, and the account goes unheard while orders
//! are still accepted.
//!
//! The backoff resets on a **subscription**, not on a connection. The market
//! socket is useful the moment it connects; this one is useful only once the
//! venue has said yes twice, and a venue that takes the TCP connection and
//! then refuses the logon, again and again, must be retried on a curve, not
//! at the floor of one.
//!
//! A connection ends on any of the things the venue documents as ending a
//! subscription: the socket closing (the 24-hour limit among them), a
//! `serverShutdown` event (web-socket-api.md:106, :368-381), an
//! `eventStreamTerminated` event (user-data-stream.md:325-341), and
//! `session.status` saying no subscription is active. `serverShutdown` asks for
//! a new connection *"as soon as possible"*, and gets one: this side leaves at
//! once, without waiting for the venue to hang up. Opening the new connection
//! before closing the old would avoid the gap, at the price of both
//! subscriptions delivering the same events for a moment; that is a
//! deduplication `LiveVenue` would own, and is not attempted here.
//!
//! # Liveness: what the venue answers, not what the account does
//!
//! The market socket gives up after 120 s without a frame
//! ([`ConnectionPolicy::idle_timeout`]). Applied to *events*, that rule would
//! be wrong here in the worst way: an account that trades nothing sends
//! nothing for hours, so it would reconnect a healthy quiet account every two
//! minutes and record a gap each time, a blindness that never happened, on the
//! one stream where a gap puts every working order in doubt.
//!
//! The market rule never was about events: it counts every frame, pings
//! included, and the venue pings every 20 seconds (web-socket-api.md:111). So
//! pings would keep a quiet account's socket open. But a ping proves the
//! socket, not the subscription (`docs/live-run.md` §5): a connection that
//! still pings and has lost its subscription, or whose API has stopped
//! answering, looks healthy from the ping alone. What the venue documents for
//! the subscription is `session.status`, whose `userDataStream` field says
//! whether one is active (web-socket-api.md:8192-8195, :8213).
//!
//! So liveness is a question, asked every [`AccountPolicy::probe_interval`]
//! once subscribed: `session.status`, which must be answered within
//! [`AccountPolicy::probe_timeout`] and answered `true`. An unanswered probe
//! ends the connection, as a silent socket ends the market's, and `false` ends
//! it as a lost subscription. Neither depends on the account doing anything,
//! and each costs weight 2 (web-socket-api.md:3442-3443) against a limit of
//! 6000 a minute in the venue's own example (web-socket-api.md:416). Pings are
//! still answered at once with their own payload (web-socket-api.md:111-114),
//! because a connection that does not answer them is cut within a minute; they
//! are just not taken as evidence of anything.
//!
//! The logon and the subscription are held to [`AccountPolicy::answer_timeout`]
//! the same way, and the connect itself too, so no step of a connection can
//! wait without a bound. A request's deadline starts before it is written,
//! not after, so a send that stalls on a socket the venue has stopped reading
//! is the same unanswered request; a pong is held to the answer timeout too.
//!
//! # Recording, in the order M1.e requires
//!
//! Each text or binary frame is stamped from the ingress clock first, then
//! classified, then either recorded through [`Ingress::accept_account_at`] or
//! withheld, and only then acted on. Stamp before parse is the data contract's
//! §2; record before act means that whatever this listener decides about a
//! frame, the frame it decided on is already in the capture. Recording is
//! also how a frame reaches its consumer (below), so a consumer can never be
//! handed a frame the capture does not have.
//!
//! A frame the capture had no room for leaves a hole and a `LocalOverflow` gap
//! record that ingress holds until there is room, and sends ahead of the next
//! frame. On a quiet account there may be no next frame for hours, so each
//! probe also sends what is held: the hole is in the capture, and in front of
//! its consumer, within a probe interval of the writer catching up.
//!
//! A connection that ended after its subscription was accepted records one
//! `Gap{Disconnect}`, whatever ended it, a stop included. One that never got
//! that far records nothing more: the stream was already dark, since the last
//! gap or since the session's opening `RecorderRestart`, and failed retries are
//! all the same hole, as they are for the market. The gap's `last_good_ts` is
//! the last frame recorded, which for a quiet account can be long before the
//! last probe that was answered; the gap claims less sight than we had, which
//! is the direction to be wrong in.
//!
//! # Stopping, and what does not stop
//!
//! [`listen`] returns only when listening has stopped for good, and says why
//! ([`Stopped`]): the writer is gone, the venue refused something in a way
//! that will not clear by itself ([`is_transient`]), or the venue's answer says
//! account subscriptions do not exist there or is not one we can read. A
//! refusal is stopped on whichever request it answered, the subscription's
//! included, and a refusal nobody asked for, like the revocation notice, stops
//! it too. So does an HTTP status that refuses the WebSocket upgrade itself, by
//! the same rule: a 418 is a ban wherever it turns up, and the pinned pages do
//! not say a banned address is refused only after the upgrade. Everything else,
//! a connect refused, a reset, a timeout, a 5xx or a busy venue, is a
//! connection that ended: logged, backed off, and retried.
//!
//! # Who reads the records
//!
//! Whatever sink the [`Ingress`] was built with. For a live run that is a
//! [`TeeSink`](quant_recorder::TeeSink) with the capture channel as its
//! primary, the market path's shape: the capture is the artifact that cannot
//! be recreated, and a consumer that falls behind loses its copy, never the
//! file's. The consumer reads the tee's secondary channel through an
//! [`AccountFeed`], which hands it [`AccountRecord`]s: each frame with the
//! bytes, stamp and sequence number the file has for it, and a
//! [`AccountRecord::Blind`] for each recorded gap and for each hole in
//! `ingest_seq` the tee left when the consumer fell behind, whether or not a
//! record has come since to reveal it (below). That is the
//! interface `LiveVenue` (`docs/live-run.md` §4 item 7) is built against. It
//! runs [`classify`] on a frame's bytes to tell an event from the
//! subscription's answer, and hands events to the translator in
//! [`crate::user_data`]; a recorded subscription answer is the record that the
//! account is heard from again, and `Blind` is the record that it was not,
//! which for a live venue means every working order is in doubt.
//!
//! The feed is the market's [`LiveSource`](crate::live::LiveSource) in this
//! stream's terms, and the hole is the point of it. A dropped market record
//! costs a book that the next snapshot rebuilds; a dropped account record can
//! be a fill, and nothing will send it again. So the consumer must never be
//! handed a frame after a hole without first being told the hole is there.
//!
//! Nor left waiting for that frame to learn of the hole. The market feed finds
//! a hole when the record after it arrives, which on the market is soon; an
//! account that trades nothing sends nothing for hours, and the probes'
//! answers are never recorded, so a fill the consumer missed could stay unseen
//! that long. So the tee also marks the last record it did not hand over
//! ([`LastDropped`](quant_recorder::LastDropped)), and the feed, finding its
//! channel empty, reads the mark and reports the hole at once.

use core::fmt;
use core::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use quant_core::event::GapCause;
use quant_core::time::Ts;
use quant_recorder::{Backoff, BackoffPolicy, Ingress, RecordSink, ACCOUNT_STREAM};
use serde_json::{json, Value};
use tokio::time::{sleep, sleep_until, timeout, timeout_at, Instant};
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{error, info, warn};

use crate::connection::ConnectionPolicy;
use crate::credentials::Credentials;
use crate::trade::VENUE_TIMEOUT;

/// The requests the account listener sends.
///
/// Three, and only three. Orders go over REST (`docs/live-run.md` §5, *one
/// authority per fact*), so this connection never carries one, and a fourth
/// method here would be a reason to re-read that decision first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Request {
    /// `session.logon`: authenticate the connection with the Ed25519 key
    /// (web-socket-api.md:3375-3426).
    Logon,
    /// `userDataStream.subscribe`: start the account's events on this
    /// connection (web-socket-api.md:8199-8231).
    Subscribe,
    /// `session.status`: is the connection authenticated, and does it still
    /// hold a subscription (web-socket-api.md:3430-3464, :8192-8195).
    Status,
}

impl Request {
    /// Every request, for anything that has to say something about each.
    pub const ALL: [Self; 3] = [Self::Logon, Self::Subscribe, Self::Status];

    /// The method name, as the venue spells it.
    #[must_use]
    pub const fn method(self) -> &'static str {
        match self {
            Self::Logon => "session.logon",
            Self::Subscribe => "userDataStream.subscribe",
            Self::Status => "session.status",
        }
    }

    /// The word for this request in its ids: one per method, letters only.
    #[must_use]
    pub const fn token(self) -> &'static str {
        match self {
            Self::Logon => "logon",
            Self::Subscribe => "subscribe",
            Self::Status => "status",
        }
    }

    /// The id for the `n`th request this listener sends: the request's
    /// [`token`](Self::token), a `-`, and the counter. No token has a `-` in
    /// it, so the last one splits the two unambiguously.
    #[must_use]
    pub fn id(self, n: u64) -> String {
        format!("{}-{n}", self.token())
    }

    /// Which request an id names, if it is one of ours.
    #[must_use]
    pub fn of_id(id: &str) -> Option<Self> {
        let (token, n) = id.rsplit_once('-')?;
        if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Self::ALL.into_iter().find(|r| r.token() == token)
    }

    /// Whether the venue's answer to this request may be recorded.
    ///
    /// The subscription's answer is the account's record that it was being
    /// heard from; the other two answers echo the API key.
    #[must_use]
    pub const fn answer_is_recorded(self) -> bool {
        match self {
            Self::Subscribe => true,
            Self::Logon | Self::Status => false,
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.method())
    }
}

/// One frame from the account's socket, as far as this module reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountFrame {
    /// Something the venue pushed: an account event, or a connection event
    /// like `serverShutdown`. `kind` is its `e`.
    Event { kind: String },
    /// The venue's answer to a request.
    Answer(Answer),
    /// Neither shape. Recorded all the same, and the reason is kept for the log.
    Unrecognised(String),
}

/// The venue's answer to a request (web-socket-api.md:173-326).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// The request it answers, when its id is one of ours. `None` for an id
    /// that is `null`, a number, or a string we did not mint.
    pub request: Option<Request>,
    /// The id as the venue echoed it, when it is a string.
    pub id: Option<String>,
    /// HTTP-style status: 200 for success (web-socket-api.md:309-324).
    pub status: u16,
    /// The `error` object, on a failure.
    pub error: Option<VenueError>,
    /// `result.userDataStream`, which `session.logon` and `session.status`
    /// carry (web-socket-api.md:8192-8195).
    pub stream: StreamFlag,
}

impl Answer {
    /// Whether the request succeeded.
    #[must_use]
    pub const fn ok(&self) -> bool {
        self.status == 200
    }
}

/// An `error` object's code and message (errors.md:3-10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueError {
    pub code: Option<i64>,
    pub msg: String,
}

/// What an answer says about the connection's subscriptions.
///
/// The documented values are three (web-socket-api.md:8192-8195), and the
/// field's absence is a fourth state, kept apart rather than folded into one
/// of them: an answer to `session.status` without it means our model of the
/// venue is wrong, which is not the same thing as a subscription that ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamFlag {
    /// The answer has no `result.userDataStream` at all.
    Absent,
    /// `null`: *"User Data Stream subscriptions are **not available** on this
    /// WebSocket API."*
    Unavailable,
    /// `true`: at least one subscription is active on this connection.
    Active,
    /// `false`: none is.
    Inactive,
}

/// Sort one frame from the account's socket.
///
/// Reads four things and nothing else: whether there is an `event` and its
/// `e`, and otherwise an answer's `id`, `status`, `error` and
/// `result.userDataStream`. Anything that is not JSON, or is JSON of neither
/// shape, is [`AccountFrame::Unrecognised`]; that is a note for the log, not a
/// reason to drop the bytes.
#[must_use]
pub fn classify(payload: &[u8]) -> AccountFrame {
    let value: Value = match serde_json::from_slice(payload) {
        Ok(value) => value,
        Err(e) => return AccountFrame::Unrecognised(format!("not JSON: {e}")),
    };
    if let Some(event) = value.get("event") {
        return match event.get("e").and_then(Value::as_str) {
            Some(kind) => AccountFrame::Event {
                kind: kind.to_owned(),
            },
            None => AccountFrame::Unrecognised("an event with no type `e`".to_owned()),
        };
    }
    let Some(status) = value.get("status").and_then(Value::as_u64) else {
        return AccountFrame::Unrecognised("neither an event nor an answer".to_owned());
    };
    let Ok(status) = u16::try_from(status) else {
        return AccountFrame::Unrecognised(format!("an answer with status {status}"));
    };
    let id = value.get("id").and_then(Value::as_str).map(str::to_owned);
    let error = value.get("error").map(|error| VenueError {
        code: error.get("code").and_then(Value::as_i64),
        msg: error
            .get("msg")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    });
    let stream = match value.get("result").and_then(|r| r.get("userDataStream")) {
        Some(Value::Null) => StreamFlag::Unavailable,
        Some(Value::Bool(true)) => StreamFlag::Active,
        Some(Value::Bool(false)) => StreamFlag::Inactive,
        // Missing, or not a boolean or null: either way not a value the
        // venue documents.
        None | Some(_) => StreamFlag::Absent,
    };
    AccountFrame::Answer(Answer {
        request: id.as_deref().and_then(Request::of_id),
        id,
        status,
        error,
        stream,
    })
}

/// Whether a frame goes to the raw tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keep {
    Record,
    Withhold(Withheld),
}

/// Why a frame was kept out of the raw tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Withheld {
    /// The answer to a request whose answer echoes the API key.
    SessionAnswer(Request),
    /// Its bytes contain the API key, whatever it is. Should never happen.
    EchoesKey,
}

/// The recording rule, in the order the module docs give it: the request
/// answered first, then the bytes themselves.
///
/// `api_key` is the key this connection logged on with. An empty one matches
/// nothing rather than everything, though [`crate::credentials::Credentials`]
/// refuses an empty key before it gets here.
#[must_use]
pub fn keep(frame: &AccountFrame, payload: &[u8], api_key: &str) -> Keep {
    if let AccountFrame::Answer(Answer {
        request: Some(request),
        ..
    }) = frame
    {
        if !request.answer_is_recorded() {
            return Keep::Withhold(Withheld::SessionAnswer(*request));
        }
    }
    if contains(payload, api_key.as_bytes()) {
        return Keep::Withhold(Withheld::EchoesKey);
    }
    Keep::Record
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
}

/// Whether a refusal may clear up by itself, so that a new connection is
/// worth trying.
///
/// The venue's own split is the starting point: *"`4XX` status codes indicate
/// invalid requests; the issue is on your side"* and *"`5XX` status codes
/// indicate internal errors; the issue is on Binance's side"*
/// (web-socket-api.md:314-324). A request of ours that was wrong is wrong the
/// next time too: a bad key (-2014), a revoked key or a missing permission
/// (-2015), a bad signature (-1022), and a timestamp outside `recvWindow`
/// (-1021), which a fresh timestamp from the same clock does not fix
/// (errors.md:53-58, :338-342). Retrying those forever would turn a stopped
/// listener, which someone notices, into a busy one, which nobody does.
///
/// Transient, then: any 5xx; a 429, which asks us to slow down and is answered
/// by the backoff (web-socket-api.md:519-522); and the codes that describe the
/// venue's state rather than the request's: -1000 unknown, -1001 disconnected
/// (*"Please try again"*), -1003 too many requests, -1006 and -1007 (no
/// answer from the matching engine), -1008 server busy, -1034 too many
/// connections, and -2042, which ends a connection's subscriptions until a new
/// one is made (errors.md:13-66, :359-360; web-socket-api.md:8190-8191).
///
/// Not -1016, though its name, `SERVICE_SHUTTING_DOWN`, sounds like a restart.
/// Its words are *"This service is no longer available."* (errors.md:44-45),
/// which describes a method that has gone, not a server that is busy; a
/// restart the venue announces is `serverShutdown`, and a busy one answers
/// 5xx, -1001 or -1008. Methods do go: listen keys did (CHANGELOG.md:405-418).
/// Retried, a retired method would be asked again every 30 s forever.
///
/// Except a 418. It is an IP ban (web-socket-api.md:318, :522-523), and a ban
/// is extended by exactly what a retry loop does, so it stops the listener
/// whatever code comes with it, -1003 included.
#[must_use]
pub fn is_transient(status: u16, code: Option<i64>) -> bool {
    const TRANSIENT_CODES: [i64; 8] = [-1000, -1001, -1003, -1006, -1007, -1008, -1034, -2042];
    if status == 418 {
        return false;
    }
    if (500..=599).contains(&status) || status == 429 {
        return true;
    }
    code.is_some_and(|code| TRANSIENT_CODES.contains(&code))
}

/// Binance's WebSocket API, where the account stream is subscribed
/// (web-socket-api.md:102).
pub const SPOT_WS_API: &str = "wss://ws-api.binance.com:443/ws-api/v3";

/// Spot Testnet's WebSocket API (web-socket-api.md:104), for the rehearsal
/// `docs/live-run.md` §5 runs before any real order, with a key pair of its
/// own.
pub const SPOT_TESTNET_WS_API: &str = "wss://ws-api.testnet.binance.vision/ws-api/v3";

/// How the account listener behaves around a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountPolicy {
    /// Between connections. The market socket's curve, by default.
    pub backoff: BackoffPolicy,
    /// How long after one `session.status` is answered the next is asked.
    pub probe_interval: Duration,
    /// How long a `session.status` may go unanswered before the connection is
    /// taken for dead.
    pub probe_timeout: Duration,
    /// How long the connect, the logon and the subscription may each take.
    pub answer_timeout: Duration,
}

impl Default for AccountPolicy {
    /// The market's backoff; a probe every 30 s, and ten seconds for any answer.
    ///
    /// # Why ten seconds
    ///
    /// It is the venue's own: its API gives a request ten seconds before it
    /// answers `-1007` (web-socket-api.md:124, [`VENUE_TIMEOUT`]). `session.status`
    /// and `session.logon` are served from memory (web-socket-api.md:3408-3409,
    /// :3448-3449), so an answer that has not come in the time the venue allows
    /// its slowest path is not coming.
    ///
    /// # Why every 30 s
    ///
    /// The probe's interval plus its timeout is how long a dead connection can
    /// go unnoticed: forty seconds here, against the market socket's 120. That
    /// is tighter than the market's on purpose. A blind market costs a book that
    /// a snapshot re-anchors; a blind account stream is a fill nobody hears
    /// about, and every second of it is a second the run must refuse to trade
    /// through. Faster than this buys little: at weight 2 a probe every 30 s is
    /// four in a minute's thousands, and the gap a reconnect records costs more
    /// than a missed probe would.
    fn default() -> Self {
        Self {
            backoff: ConnectionPolicy::default().backoff,
            probe_interval: Duration::from_secs(30),
            probe_timeout: VENUE_TIMEOUT,
            answer_timeout: VENUE_TIMEOUT,
        }
    }
}

/// A request the venue refused, as the venue put it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// The request refused, or `None` for a refusal nobody asked for, like the
    /// revocation notice (web-socket-api.md:1317-1330).
    pub request: Option<Request>,
    pub status: u16,
    pub code: Option<i64>,
    /// The venue's message, with the API key cut out of it should it ever
    /// appear there.
    pub msg: String,
}

impl Refusal {
    fn from_answer(answer: &Answer, request: Option<Request>, api_key: &str) -> Self {
        let (code, msg) = answer
            .error
            .as_ref()
            .map_or((None, String::new()), |e| (e.code, e.msg.clone()));
        let msg = if api_key.is_empty() {
            msg
        } else {
            msg.replace(api_key, "<api key>")
        };
        Self {
            request,
            status: answer.status,
            code,
            msg,
        }
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.request {
            Some(request) => write!(f, "{request} refused")?,
            None => f.write_str("the venue refused the session unasked")?,
        }
        write!(f, ": status {}", self.status)?;
        if let Some(code) = self.code {
            write!(f, ", code {code}")?;
        }
        if !self.msg.is_empty() {
            write!(f, ", {:?}", self.msg)?;
        }
        // The venue's words name its window and not our clock, and the clock
        // is what has to be fixed. The order path reads -1021 the same way
        // (`orders.rs`, `RefusalCause::Timestamp`).
        if self.code == Some(-1021) {
            f.write_str(
                "; the host clock, less the offset measured at startup, is outside the \
                 venue's recvWindow: measure it again with venue-check",
            )?;
        }
        Ok(())
    }
}

/// Why the account listener stopped for good.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stopped {
    /// The capture writer is gone, so nothing heard could be recorded.
    WriterGone,
    /// A refusal that will not clear by itself ([`is_transient`]).
    Refused(Refusal),
    /// The venue answered the WebSocket upgrade with this HTTP status, and it
    /// is not one that clears by itself ([`is_transient`]).
    UpgradeRefused(u16),
    /// `session.status` answered `userDataStream: null`: *"User Data Stream
    /// subscriptions are **not available** on this WebSocket API"*
    /// (web-socket-api.md:8193).
    Unavailable,
    /// An answer this module cannot read the way it must, which means our
    /// model of the venue is wrong (invariant 5).
    NotUnderstood(String),
}

impl fmt::Display for Stopped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriterGone => f.write_str("the capture writer is gone"),
            Self::Refused(refusal) => write!(f, "{refusal}"),
            Self::UpgradeRefused(status) => {
                write!(f, "the WebSocket upgrade was refused: HTTP {status}")
            }
            Self::Unavailable => {
                f.write_str("account subscriptions are not available on this WebSocket API")
            }
            Self::NotUnderstood(what) => write!(f, "an answer not understood: {what}"),
        }
    }
}

impl std::error::Error for Stopped {}

/// Why one connection ended, when it is worth making another.
///
/// All of them are the same `Gap{Disconnect}` on disk once the subscription had
/// been accepted; they are kept apart for the log, because a `ServerClosed`
/// once a day is the venue's documented 24-hour limit and an `Unanswered`
/// every few minutes is something wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ended {
    /// The socket closed.
    ServerClosed,
    /// The transport failed.
    Transport(String),
    /// A pong could not be sent, so the venue is about to hang up.
    KeepaliveFailed(String),
    /// `serverShutdown`: the venue asked for a new connection.
    ServerShutdown,
    /// `eventStreamTerminated`: the subscription ended.
    StreamTerminated,
    /// `session.status` answered that no subscription is active.
    SubscriptionLost,
    /// A request went unanswered past its deadline.
    Unanswered(Request),
    /// A refusal that may clear by itself.
    Refused(Refusal),
}

impl fmt::Display for Ended {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServerClosed => f.write_str("the venue closed the connection"),
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::KeepaliveFailed(e) => write!(f, "could not send a pong: {e}"),
            Self::ServerShutdown => {
                f.write_str("serverShutdown: the venue asked for a new connection")
            }
            Self::StreamTerminated => f.write_str("eventStreamTerminated: the subscription ended"),
            Self::SubscriptionLost => f.write_str("session.status says no subscription is active"),
            Self::Unanswered(request) => write!(f, "{request} went unanswered"),
            Self::Refused(refusal) => write!(f, "{refusal}, which may clear by itself"),
        }
    }
}

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Listen to the account until listening has stopped for good, recording
/// every frame the venue sends that may be recorded.
///
/// Returns only then, and says why. Everything short of that is a connection
/// that ended, which is logged, recorded as a gap when the subscription had
/// been accepted, and followed by another after the backoff. Cancellation is
/// the caller's, as for [`crate::connection::run`]: drop this future and the
/// borrowed `ingress` is free to be dropped, which closes the channel and
/// seals the capture.
///
/// The caller records the session's opening `Gap{RecorderRestart}`, as
/// `capture::run` does for the market, before the first call.
///
/// `offset_ms` is the host clock's offset from the venue's, positive when the
/// host is ahead: the same measured number the order path takes off its own
/// stamps ([`crate::trade::TradeClient::check_ready`]). Every logon is stamped
/// with the ingress clock less this, and nothing else is: `local_recv_ts` stays
/// the host's.
pub async fn listen<S: RecordSink>(
    url: &str,
    credentials: &Credentials,
    ingress: &mut Ingress<S>,
    policy: &AccountPolicy,
    offset_ms: i64,
) -> Stopped {
    // Not the caller's problem, as for `SnapshotClient::new` and
    // `TradeClient`: a `wss://` handshake reads the process-global rustls
    // provider. Today only `ring` is in the build, so rustls would pick it by
    // itself; a dependency that adds `aws-lc-rs` would make that first
    // handshake panic instead. See `crate::install_crypto_provider`.
    crate::install_crypto_provider();
    let mut backoff = Backoff::for_key(policy.backoff, ACCOUNT_STREAM);
    let mut requests = 0_u64;
    info!(%url, "account listener starting");

    loop {
        match timeout(policy.answer_timeout, connect_async(url)).await {
            Ok(Ok((socket, _response))) => {
                let mut heard = false;
                let outcome = converse(
                    socket,
                    credentials,
                    ingress,
                    policy,
                    offset_ms,
                    &mut requests,
                    &mut heard,
                )
                .await;
                if heard {
                    // The venue said yes to the logon and the subscription, so
                    // the next failure starts short again.
                    backoff.reset();
                    if ingress.record_gap(GapCause::Disconnect).is_err() {
                        return Stopped::WriterGone;
                    }
                }
                match outcome {
                    Ok(ended) => warn!(%ended, heard, "account connection ended"),
                    Err(stopped) => {
                        error!(%stopped, "account listener stopped; the account is not being heard");
                        return stopped;
                    }
                }
            }
            // A status refusing the upgrade is held to the rule an answer is:
            // a ban, a forbidden address or a wrong path will not clear by
            // itself, and asking again every 30 s is what extends a ban. The
            // body is not logged, for the reason no payload is.
            Ok(Err(tungstenite::Error::Http(response)))
                if !is_transient(response.status().as_u16(), None) =>
            {
                let stopped = Stopped::UpgradeRefused(response.status().as_u16());
                error!(%stopped, "account listener stopped; the account is not being heard");
                return stopped;
            }
            Ok(Err(e)) => warn!(attempt = backoff.attempt(), error = %e, "account connect failed"),
            Err(_elapsed) => warn!(attempt = backoff.attempt(), "account connect timed out"),
        }

        let delay = backoff.next_delay();
        // About to wait anyway, and a disconnect means the writer is draining:
        // where a deferred gap record lands, as on the market socket.
        if ingress.pump().is_err() {
            return Stopped::WriterGone;
        }
        sleep(delay).await;
    }
}

/// A request sent and not yet answered.
#[derive(Debug)]
struct Waiting {
    request: Request,
    id: String,
    deadline: Instant,
}

/// What one frame means for the conversation.
enum Step {
    Continue,
    /// The logon was accepted: subscribe.
    Subscribe,
    /// The subscription was accepted.
    Heard,
    /// A probe was answered `true`.
    Probed,
    End(Ended),
    Stop(Stopped),
}

/// One connection, from the logon to whatever ends it.
///
/// `heard` is set the moment the subscription is accepted, and left set
/// however the connection ends, so the caller knows to record the gap.
async fn converse<S: RecordSink>(
    mut socket: Socket,
    credentials: &Credentials,
    ingress: &mut Ingress<S>,
    policy: &AccountPolicy,
    offset_ms: i64,
    requests: &mut u64,
    heard: &mut bool,
) -> Result<Ended, Stopped> {
    let logon = {
        // A fresh timestamp for every logon, read from the one clock and put
        // on the venue's as the order path puts its own: a logon is signed
        // once and sent once.
        let venue_millis = ingress.clock().now().as_millis().saturating_sub(offset_ms);
        Outbound::logon(credentials, venue_millis)
    };
    let mut waiting = match send(&mut socket, logon, requests, policy.answer_timeout).await {
        Ok(waiting) => Some(waiting),
        Err(ended) => return Ok(ended),
    };
    let mut next_probe: Option<Instant> = None;

    loop {
        let wake = waiting.as_ref().map(|w| w.deadline).or(next_probe);
        let next = match wake {
            Some(at) => tokio::select! {
                biased;
                next = socket.next() => Some(next),
                () = sleep_until(at) => None,
            },
            None => Some(socket.next().await),
        };

        let Some(next) = next else {
            // A deadline passed: an answer that did not come, or a probe due.
            if let Some(unanswered) = waiting.take() {
                return Ok(Ended::Unanswered(unanswered.request));
            }
            next_probe = None;
            // A gap record held because the capture was full goes out ahead
            // of the next frame; on a quiet account there may be no next
            // frame for hours, and until it goes out the consumer cannot know
            // of the hole it explains. The probe is this loop's own tick.
            ingress.pump().map_err(|_| Stopped::WriterGone)?;
            match send(
                &mut socket,
                Outbound::Status,
                requests,
                policy.probe_timeout,
            )
            .await
            {
                Ok(probe) => waiting = Some(probe),
                Err(ended) => return Ok(ended),
            }
            continue;
        };

        let message = match next {
            None => return Ok(Ended::ServerClosed),
            Some(Err(e)) => return Ok(Ended::Transport(e.to_string())),
            Some(Ok(message)) => message,
        };
        match message {
            message @ (Message::Text(_) | Message::Binary(_)) => {
                // Stamp, classify, record, and only then act.
                let local_recv_ts = ingress.clock().now();
                let payload = message.into_data().to_vec();
                let frame = classify(&payload);
                record(&frame, payload, local_recv_ts, credentials.key(), ingress)?;
                match step(&frame, &mut waiting, credentials.key()) {
                    Step::Continue => {}
                    Step::Subscribe => {
                        match send(
                            &mut socket,
                            Outbound::Subscribe,
                            requests,
                            policy.answer_timeout,
                        )
                        .await
                        {
                            Ok(subscribing) => waiting = Some(subscribing),
                            Err(ended) => return Ok(ended),
                        }
                    }
                    Step::Heard => {
                        *heard = true;
                        info!("account subscribed; the account is being heard");
                        next_probe = Some(Instant::now() + policy.probe_interval);
                    }
                    Step::Probed => next_probe = Some(Instant::now() + policy.probe_interval),
                    Step::End(ended) => return Ok(ended),
                    Step::Stop(stopped) => return Err(stopped),
                }
            }
            // Answered at once with its own payload (web-socket-api.md:113),
            // explicitly, for the reason `connection.rs` gives: the one thing
            // that keeps a connection open is not left to library internals.
            Message::Ping(payload) => {
                match timeout(policy.answer_timeout, socket.send(Message::Pong(payload))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => return Ok(Ended::KeepaliveFailed(e.to_string())),
                    Err(_elapsed) => {
                        return Ok(Ended::KeepaliveFailed(format!(
                            "not written within {:?}",
                            policy.answer_timeout
                        )))
                    }
                }
            }
            Message::Close(_) => return Ok(Ended::ServerClosed),
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

/// Put a frame in the capture, or keep it out, by [`keep`]'s rule.
fn record<S: RecordSink>(
    frame: &AccountFrame,
    payload: Vec<u8>,
    local_recv_ts: Ts,
    api_key: &str,
    ingress: &mut Ingress<S>,
) -> Result<(), Stopped> {
    match keep(frame, &payload, api_key) {
        Keep::Record => {
            ingress
                .accept_account_at(payload, local_recv_ts)
                .map_err(|_| Stopped::WriterGone)?;
        }
        Keep::Withhold(Withheld::SessionAnswer(_)) => {}
        Keep::Withhold(Withheld::EchoesKey) => error!(
            bytes = payload.len(),
            "an account frame holding the API key was kept out of the capture"
        ),
    }
    Ok(())
}

/// What a classified frame means, given what we are waiting for.
fn step(frame: &AccountFrame, waiting: &mut Option<Waiting>, api_key: &str) -> Step {
    let answer = match frame {
        AccountFrame::Event { kind } => {
            return match kind.as_str() {
                "serverShutdown" => Step::End(Ended::ServerShutdown),
                "eventStreamTerminated" => Step::End(Ended::StreamTerminated),
                _ => Step::Continue,
            }
        }
        AccountFrame::Unrecognised(why) => {
            warn!(why, "an account frame not understood; recorded as it came");
            return Step::Continue;
        }
        AccountFrame::Answer(answer) => answer,
    };

    let awaited = waiting
        .as_ref()
        .is_some_and(|w| answer.id.as_deref() == Some(w.id.as_str()));
    let request = if awaited {
        waiting.take().map(|w| w.request)
    } else {
        // Nothing we are waiting for: the revocation notice, with its `null`
        // id, or an answer to nothing we sent. A failure stops or ends the
        // connection by the same rule as any other; a success is noise.
        if answer.ok() {
            warn!(id = ?answer.id, "an answer to no request in flight");
            return Step::Continue;
        }
        answer.request
    };
    if !answer.ok() {
        let refusal = Refusal::from_answer(answer, request, api_key);
        return if is_transient(refusal.status, refusal.code) {
            Step::End(Ended::Refused(refusal))
        } else {
            Step::Stop(Stopped::Refused(refusal))
        };
    }
    match request {
        Some(Request::Logon) => Step::Subscribe,
        Some(Request::Subscribe) => Step::Heard,
        Some(Request::Status) => match answer.stream {
            StreamFlag::Active => Step::Probed,
            StreamFlag::Inactive => Step::End(Ended::SubscriptionLost),
            StreamFlag::Unavailable => Step::Stop(Stopped::Unavailable),
            StreamFlag::Absent => Step::Stop(Stopped::NotUnderstood(
                "session.status answered without result.userDataStream".to_owned(),
            )),
        },
        None => Step::Continue,
    }
}

/// A request, before it has an id.
enum Outbound {
    /// The params, already signed.
    Logon {
        params: Value,
    },
    Subscribe,
    Status,
}

impl Outbound {
    /// `session.logon`'s params, signed (web-socket-api.md:3399-3406).
    ///
    /// `recvWindow` is left to the venue's default, the same five seconds
    /// every REST request sends ([`crate::trade::RECV_WINDOW_MS`]).
    ///
    /// The signature goes as base64 (web-socket-api.md:1161-1164), whatever
    /// the method's own example shows: its `signature` (:3383) is 64 hex
    /// characters, which is an HMAC-SHA256's 32 bytes and not an Ed25519
    /// signature's 64, on a method that takes Ed25519 keys only. The same
    /// kind of example slice (c) found on the REST page, and read the same
    /// way: as an example of the request's shape, not of its signature.
    ///
    /// `timestamp` is on the venue's clock, in milliseconds.
    fn logon(credentials: &Credentials, timestamp: i64) -> Self {
        let text = timestamp.to_string();
        let signature = credentials
            .sign_ws_params(&[("apiKey", credentials.key()), ("timestamp", &text)])
            .expect("apiKey and timestamp are two different names");
        Self::Logon {
            params: json!({
                "apiKey": credentials.key(),
                "signature": signature,
                "timestamp": timestamp,
            }),
        }
    }

    const fn request(&self) -> Request {
        match self {
            Self::Logon { .. } => Request::Logon,
            Self::Subscribe => Request::Subscribe,
            Self::Status => Request::Status,
        }
    }
}

/// Send a request, minting its id, and say when its answer is due: `within`
/// of starting to write it, so a write that stalls counts against the same
/// deadline as an answer that does not come.
async fn send(
    socket: &mut Socket,
    outbound: Outbound,
    requests: &mut u64,
    within: Duration,
) -> Result<Waiting, Ended> {
    *requests += 1;
    let request = outbound.request();
    let id = request.id(*requests);
    let text = match outbound {
        Outbound::Logon { params } => {
            json!({ "id": id, "method": request.method(), "params": params })
        }
        Outbound::Subscribe | Outbound::Status => json!({ "id": id, "method": request.method() }),
    }
    .to_string();
    let deadline = Instant::now() + within;
    match timeout_at(deadline, socket.send(Message::text(text))).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(Ended::Transport(e.to_string())),
        Err(_elapsed) => return Err(Ended::Unanswered(request)),
    }
    Ok(Waiting {
        request,
        id,
        deadline,
    })
}

/// One record of the account stream, as its consumer needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountRecord {
    /// A frame the venue sent, exactly as the capture holds it.
    Frame {
        local_recv_ts: Ts,
        ingest_seq: u64,
        payload: Vec<u8>,
    },
    /// The account was not being heard: a gap the capture recorded, or, as
    /// [`GapCause::LocalOverflow`], records this consumer was not handed.
    Blind { local_recv_ts: Ts, cause: GapCause },
}

/// What [`AccountFeed::poll`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Polled {
    Record(AccountRecord),
    /// Nothing waiting now.
    Empty,
    /// The listener is gone and everything it sent has been handed over.
    Closed,
}

/// What the feed has seen.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FeedStats {
    /// Frames handed over.
    pub frames: u64,
    /// Gaps the capture recorded, handed over as [`AccountRecord::Blind`].
    pub gaps: u64,
    /// Holes in this consumer's `ingest_seq`, each handed over as a `Blind`.
    pub holes: u64,
    /// Records lost to those holes.
    ///
    /// Two counts make it up, and neither alone does. The tee counts what it
    /// could not hand this consumer
    /// ([`TeeSink::secondary_dropped`](quant_recorder::TeeSink::secondary_dropped)),
    /// and ingress counts what the capture itself had no room for
    /// ([`IngressStats::dropped`](quant_recorder::IngressStats::dropped)): a
    /// record the capture refused reaches neither side, so the tee never sees
    /// it, and the hole is the consumer's all the same. Once the feed has
    /// caught up, this is their sum. A check built on the tee's count alone
    /// would fire exactly when the capture is overloaded.
    pub missed: u64,
    /// Records that are not the account stream's, which a correctly wired
    /// feed never sees. Dropped, counted, and the first is logged.
    pub foreign: u64,
}

/// The account stream's records, read off the channel a
/// [`TeeSink`](quant_recorder::TeeSink)'s secondary half feeds.
///
/// Never blocks: a live venue polls it from the engine's loop, which must not
/// wait on a socket.
#[derive(Debug)]
pub struct AccountFeed {
    records: std::sync::mpsc::Receiver<quant_recorder::CaptureRecord>,
    /// The tee's mark of the last record it did not hand over.
    mark: std::sync::Arc<quant_recorder::LastDropped>,
    last_seq: Option<u64>,
    /// The record that came after a hole, held while the hole is reported.
    pending: Option<AccountRecord>,
    stats: FeedStats,
}

impl AccountFeed {
    /// Read `records`, the tee's secondary channel, with `mark`, the handle the
    /// same tee marks its drops in
    /// ([`TeeSink::marking_into`](quant_recorder::TeeSink::marking_into)).
    ///
    /// Both are required, so a feed cannot be wired that waits for a record to
    /// reveal a hole: a mark nobody sets reports nothing, which is the feed of
    /// a tee that never drops.
    #[must_use]
    pub fn new(
        records: std::sync::mpsc::Receiver<quant_recorder::CaptureRecord>,
        mark: std::sync::Arc<quant_recorder::LastDropped>,
    ) -> Self {
        Self {
            records,
            mark,
            last_seq: None,
            pending: None,
            stats: FeedStats::default(),
        }
    }

    /// The next record, if one is waiting.
    ///
    /// A hole comes out first, as a `Blind` stamped with the receive time of
    /// the record that revealed it, and that record next. The first record is
    /// expected to be the session's first, `ingest_seq` 1; a feed that starts
    /// later has missed the start and says so.
    ///
    /// A hole no record has revealed yet comes out when the channel is empty
    /// (or closed) and the tee's mark is past the last record handed over: a
    /// `Blind` stamped with the receive time of the record the mark names,
    /// before `Empty` or `Closed`. The hole is then the feed's to account for,
    /// so the record that later follows it brings no second `Blind`.
    pub fn poll(&mut self) -> Polled {
        use quant_recorder::CaptureRecord;
        use std::sync::mpsc::TryRecvError;

        if let Some(record) = self.pending.take() {
            return Polled::Record(record);
        }
        loop {
            // The mark is read before the channel. Every record before the one
            // the mark names was handed over or dropped before the mark was
            // set, so if the channel is then empty, every record up to the
            // mark that this feed has not had is lost to it, not on its way.
            let mark = self.mark.get();
            let record = match self.records.try_recv() {
                Ok(record) => record,
                Err(TryRecvError::Empty) => {
                    return self.unrevealed(mark).map_or(Polled::Empty, Polled::Record);
                }
                Err(TryRecvError::Disconnected) => {
                    return self.unrevealed(mark).map_or(Polled::Closed, Polled::Record);
                }
            };
            let ingest_seq = record.ingest_seq();
            let local_recv_ts = record.local_recv_ts();
            let expected = self.last_seq.map_or(1, |last| last.saturating_add(1));
            self.last_seq = Some(ingest_seq);

            let this = match record {
                CaptureRecord::Account { payload, .. } => {
                    self.stats.frames += 1;
                    Some(AccountRecord::Frame {
                        local_recv_ts,
                        ingest_seq,
                        payload,
                    })
                }
                CaptureRecord::Control {
                    record: quant_storage::ControlRecord::Gap { cause, .. },
                    ..
                } => {
                    self.stats.gaps += 1;
                    Some(AccountRecord::Blind {
                        local_recv_ts,
                        cause,
                    })
                }
                CaptureRecord::Control { .. }
                | CaptureRecord::Venue { .. }
                | CaptureRecord::Snapshot { .. } => {
                    if self.stats.foreign == 0 {
                        error!(
                            ingest_seq,
                            "a record that is not the account stream's on the account feed"
                        );
                    }
                    self.stats.foreign += 1;
                    None
                }
            };

            if ingest_seq > expected {
                self.stats.holes += 1;
                self.stats.missed += ingest_seq - expected;
                self.pending = this;
                return Polled::Record(AccountRecord::Blind {
                    local_recv_ts,
                    cause: GapCause::LocalOverflow,
                });
            }
            if let Some(record) = this {
                return Polled::Record(record);
            }
        }
    }

    /// The hole the tee's mark shows past the last record handed over, if
    /// there is one, taken as revealed: counted, and `last_seq` moved to it.
    fn unrevealed(&mut self, mark: Option<(u64, Ts)>) -> Option<AccountRecord> {
        let (dropped, local_recv_ts) = mark?;
        let last = self.last_seq.unwrap_or(0);
        if dropped <= last {
            return None;
        }
        self.stats.holes += 1;
        self.stats.missed += dropped - last;
        self.last_seq = Some(dropped);
        Some(AccountRecord::Blind {
            local_recv_ts,
            cause: GapCause::LocalOverflow,
        })
    }

    #[must_use]
    pub const fn stats(&self) -> FeedStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A";

    /// The venue's documented answer to `session.logon` (web-socket-api.md:3411-3425),
    /// with one of our ids.
    const LOGON_ANSWER: &str = r#"{"id":"logon-1","status":200,"result":{"apiKey":"vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A","authorizedSince":1649729878532,"connectedSince":1649729873021,"returnRateLimits":false,"serverTime":1649729878630,"userDataStream":false}}"#;

    /// `session.status`'s (web-socket-api.md:3451-3466).
    const STATUS_ANSWER: &str = r#"{"id":"status-7","status":200,"result":{"apiKey":"vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A","authorizedSince":1649729878532,"connectedSince":1649729873021,"returnRateLimits":false,"serverTime":1649730611671,"userDataStream":true}}"#;

    /// `userDataStream.subscribe`'s (web-socket-api.md:8222-8230).
    const SUBSCRIBE_ANSWER: &str =
        r#"{"id":"subscribe-2","status":200,"result":{"subscriptionId":0}}"#;

    /// The revocation notice (web-socket-api.md:1317-1330).
    const REVOKED: &str = r#"{"id":null,"status":401,"error":{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}}"#;

    /// `serverShutdown` (web-socket-api.md:374-381).
    const SHUTDOWN: &str = r#"{"event":{"e":"serverShutdown","E":1770123456789}}"#;

    /// `eventStreamTerminated` (user-data-stream.md:334-341).
    const TERMINATED: &str =
        r#"{"subscriptionId":0,"event":{"e":"eventStreamTerminated","E":1728973001334}}"#;

    #[test]
    fn an_id_names_its_request_and_only_ours_do() {
        for request in Request::ALL {
            for n in [0, 1, 42, u64::MAX] {
                assert_eq!(Request::of_id(&request.id(n)), Some(request));
            }
        }
        for stranger in [
            "",
            "logon",
            "logon-",
            "logon-1x",
            "logon-+1",
            "order-1",
            "c174a2b1-3f51-4580-b200-8528bd237cb7",
            "Logon-1",
            // The method's own name is not the token.
            "session.logon-1",
        ] {
            assert_eq!(Request::of_id(stranger), None, "{stranger:?}");
        }
    }

    #[test]
    fn every_id_is_inside_the_pattern_the_venue_is_remembered_to_enforce() {
        // `^[a-zA-Z0-9-_]{1,36}$`, at both ends of the counter.
        for request in Request::ALL {
            for n in [0, 1, u64::MAX] {
                let id = request.id(n);
                assert!((1..=36).contains(&id.len()), "{id}");
                assert!(
                    id.bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                    "{id}"
                );
            }
        }
    }

    #[test]
    fn every_documented_frame_is_classified_as_its_kind() {
        let answer = |text: &str| match classify(text.as_bytes()) {
            AccountFrame::Answer(answer) => answer,
            other => panic!("not an answer: {other:?}"),
        };

        let logon = answer(LOGON_ANSWER);
        assert_eq!(logon.request, Some(Request::Logon));
        assert!(logon.ok());
        assert_eq!(logon.stream, StreamFlag::Inactive);

        let status = answer(STATUS_ANSWER);
        assert_eq!(status.request, Some(Request::Status));
        assert_eq!(status.stream, StreamFlag::Active);

        let subscribed = answer(SUBSCRIBE_ANSWER);
        assert_eq!(subscribed.request, Some(Request::Subscribe));
        assert_eq!(subscribed.stream, StreamFlag::Absent);

        let revoked = answer(REVOKED);
        assert_eq!(revoked.request, None);
        assert_eq!(revoked.id, None);
        assert_eq!(revoked.status, 401);
        assert_eq!(
            revoked.error,
            Some(VenueError {
                code: Some(-2015),
                msg: "Invalid API-key, IP, or permissions for action.".to_owned()
            })
        );

        for (text, kind) in [
            (SHUTDOWN, "serverShutdown"),
            (TERMINATED, "eventStreamTerminated"),
        ] {
            assert_eq!(
                classify(text.as_bytes()),
                AccountFrame::Event {
                    kind: kind.to_owned()
                }
            );
        }
    }

    #[test]
    fn the_subscription_flag_keeps_all_four_states_apart() {
        let flag = |result: &str| match classify(
            format!(r#"{{"id":"status-1","status":200,"result":{result}}}"#).as_bytes(),
        ) {
            AccountFrame::Answer(answer) => answer.stream,
            other => panic!("not an answer: {other:?}"),
        };
        assert_eq!(flag(r#"{"userDataStream":true}"#), StreamFlag::Active);
        assert_eq!(flag(r#"{"userDataStream":false}"#), StreamFlag::Inactive);
        assert_eq!(flag(r#"{"userDataStream":null}"#), StreamFlag::Unavailable);
        assert_eq!(flag("{}"), StreamFlag::Absent);
        assert_eq!(flag(r#"{"userDataStream":"yes"}"#), StreamFlag::Absent);
    }

    #[test]
    fn what_is_not_an_event_or_an_answer_is_unrecognised_not_lost() {
        for text in [
            "",
            "not json",
            "[]",
            "{}",
            r#"{"event":{}}"#,
            r#"{"status":-1}"#,
        ] {
            assert!(
                matches!(classify(text.as_bytes()), AccountFrame::Unrecognised(_)),
                "{text:?}"
            );
            // And it is still recorded: our model of the venue may be the
            // thing that is wrong.
            assert_eq!(
                keep(&classify(text.as_bytes()), text.as_bytes(), KEY),
                Keep::Record
            );
        }
    }

    #[test]
    fn the_answers_that_echo_the_key_are_withheld_and_nothing_else_is() {
        let rule = |text: &str| keep(&classify(text.as_bytes()), text.as_bytes(), KEY);
        assert_eq!(
            rule(LOGON_ANSWER),
            Keep::Withhold(Withheld::SessionAnswer(Request::Logon))
        );
        assert_eq!(
            rule(STATUS_ANSWER),
            Keep::Withhold(Withheld::SessionAnswer(Request::Status))
        );
        for recorded in [SUBSCRIBE_ANSWER, REVOKED, SHUTDOWN, TERMINATED] {
            assert_eq!(rule(recorded), Keep::Record, "{recorded}");
        }
        // A refused logon or status is withheld too, key or no key: the rule
        // is on the request answered, not on what the answer happens to hold.
        let refused = r#"{"id":"logon-3","status":400,"error":{"code":-1022,"msg":"Signature for this request is not valid."}}"#;
        assert_eq!(
            rule(refused),
            Keep::Withhold(Withheld::SessionAnswer(Request::Logon))
        );
        let refused = r#"{"id":"status-3","status":503,"error":{"code":-1001,"msg":"x"}}"#;
        assert_eq!(
            rule(refused),
            Keep::Withhold(Withheld::SessionAnswer(Request::Status))
        );
    }

    #[test]
    fn any_frame_holding_the_key_is_withheld_whatever_it_claims_to_be() {
        // The guard under the rule. Each of these would be recorded on its
        // shape alone; each holds the key, so none is.
        for text in [
            format!(r#"{{"subscriptionId":0,"event":{{"e":"executionReport","k":"{KEY}"}}}}"#),
            format!(r#"{{"id":null,"status":401,"error":{{"code":-2015,"msg":"{KEY}"}}}}"#),
            format!(r#"{{"id":"subscribe-9","status":200,"result":{{"apiKey":"{KEY}"}}}}"#),
            format!("not json at all {KEY}"),
        ] {
            assert_eq!(
                keep(&classify(text.as_bytes()), text.as_bytes(), KEY),
                Keep::Withhold(Withheld::EchoesKey),
                "{text}"
            );
        }
        // An empty key matches nothing, rather than every frame.
        assert_eq!(
            keep(&classify(SHUTDOWN.as_bytes()), SHUTDOWN.as_bytes(), ""),
            Keep::Record
        );
    }

    #[test]
    fn refusals_that_will_not_fix_themselves_are_told_from_ones_that_may() {
        // Permanent: our request, or our key, or our clock.
        for (status, code) in [
            (400, Some(-1021)),
            (400, Some(-1022)),
            (401, Some(-2014)),
            (401, Some(-2015)),
            (401, Some(-1002)),
            (400, Some(-1102)),
            (400, Some(-2035)),
            (400, Some(-2036)),
            // The service is gone, not busy.
            (400, Some(-1016)),
            (403, None),
            (401, None),
            (400, None),
            // A ban is not waited out by retrying; retrying is what extends it.
            (418, Some(-1003)),
            (418, None),
        ] {
            assert!(!is_transient(status, code), "{status} {code:?}");
        }
        // Transient: the venue's state, not our request's.
        for (status, code) in [
            (500, None),
            (502, Some(-1000)),
            (503, Some(-1008)),
            (599, None),
            (429, Some(-1003)),
            (429, None),
            (400, Some(-1000)),
            (400, Some(-1001)),
            (400, Some(-1006)),
            (400, Some(-1007)),
            (400, Some(-1034)),
            (400, Some(-2042)),
        ] {
            assert!(is_transient(status, code), "{status} {code:?}");
        }
    }

    #[test]
    fn a_timestamp_refusal_names_the_clock() {
        // The venue's message names its window; what has to be fixed is our
        // clock, and the operator reading the stop should be told so.
        let refusal = |code| Refusal {
            request: Some(Request::Logon),
            status: 400,
            code: Some(code),
            msg: "Timestamp for this request is outside of the recvWindow.".to_owned(),
        };
        let said = refusal(-1021).to_string();
        assert!(said.contains("host clock"), "{said}");
        assert!(said.contains("venue-check"), "{said}");
        assert!(!said.contains("  "), "one line, one space: {said}");
        // And only for that code.
        let said = refusal(-1022).to_string();
        assert!(!said.contains("host clock"), "{said}");
    }

    /// The consumer's side of the tee.
    mod feed {
        use std::sync::mpsc::sync_channel;
        use std::sync::Arc;

        use quant_core::event::GapCause;
        use quant_core::time::{ManualClock, Ts};
        use quant_recorder::{channel, CaptureRecord, Ingress, LastDropped, Metrics, TeeSink};
        use quant_storage::{ControlRecord, SnapshotFailure, SnapshotPurpose};

        use super::super::{AccountFeed, AccountRecord, FeedStats, Polled};

        fn frame(seq: u64) -> CaptureRecord {
            CaptureRecord::Account {
                local_recv_ts: Ts::from_millis(1_000 + i64::try_from(seq).expect("small")),
                ingest_seq: seq,
                payload: format!("frame {seq}").into_bytes(),
            }
        }

        fn gap(seq: u64, cause: GapCause) -> CaptureRecord {
            CaptureRecord::Control {
                local_recv_ts: Ts::from_millis(2_000),
                ingest_seq: seq,
                record: ControlRecord::Gap {
                    cause,
                    exchange_ts: Ts::from_millis(2_000),
                    last_good_ts: Ts::from_millis(1_500),
                },
            }
        }

        fn as_frame(seq: u64) -> Polled {
            Polled::Record(AccountRecord::Frame {
                local_recv_ts: Ts::from_millis(1_000 + i64::try_from(seq).expect("small")),
                ingest_seq: seq,
                payload: format!("frame {seq}").into_bytes(),
            })
        }

        fn blind(at: Ts, cause: GapCause) -> Polled {
            Polled::Record(AccountRecord::Blind {
                local_recv_ts: at,
                cause,
            })
        }

        #[test]
        fn frames_and_gaps_come_out_in_order_as_recorded() {
            let (tx, rx) = sync_channel(16);
            let mut feed = AccountFeed::new(rx, Arc::default());
            assert_eq!(feed.poll(), Polled::Empty, "nothing yet is not the end");
            for record in [
                gap(1, GapCause::RecorderRestart),
                frame(2),
                frame(3),
                gap(4, GapCause::Disconnect),
                frame(5),
            ] {
                tx.send(record).expect("open");
            }
            drop(tx);
            assert_eq!(
                feed.poll(),
                blind(Ts::from_millis(2_000), GapCause::RecorderRestart)
            );
            assert_eq!(feed.poll(), as_frame(2));
            assert_eq!(feed.poll(), as_frame(3));
            assert_eq!(
                feed.poll(),
                blind(Ts::from_millis(2_000), GapCause::Disconnect)
            );
            assert_eq!(feed.poll(), as_frame(5));
            assert_eq!(feed.poll(), Polled::Closed, "drained, then closed");
            assert_eq!(
                feed.stats(),
                FeedStats {
                    frames: 3,
                    gaps: 2,
                    ..FeedStats::default()
                }
            );
        }

        #[test]
        fn a_hole_is_blindness_before_the_frame_that_reveals_it() {
            // A missed record on this stream can be a fill. The frame after
            // the hole must not reach the consumer before the hole does.
            let (tx, rx) = sync_channel(16);
            let mut feed = AccountFeed::new(rx, Arc::default());
            for record in [frame(1), frame(2), frame(5), frame(6)] {
                tx.send(record).expect("open");
            }
            assert_eq!(feed.poll(), as_frame(1));
            assert_eq!(feed.poll(), as_frame(2));
            assert_eq!(
                feed.poll(),
                blind(Ts::from_millis(1_005), GapCause::LocalOverflow)
            );
            assert_eq!(feed.poll(), as_frame(5));
            assert_eq!(feed.poll(), as_frame(6));
            assert_eq!(feed.poll(), Polled::Empty);
            let stats = feed.stats();
            assert_eq!((stats.holes, stats.missed, stats.frames), (1, 2, 4));
        }

        #[test]
        fn a_feed_that_missed_the_start_says_so() {
            let (tx, rx) = sync_channel(4);
            let mut feed = AccountFeed::new(rx, Arc::default());
            tx.send(frame(3)).expect("open");
            assert_eq!(
                feed.poll(),
                blind(Ts::from_millis(1_003), GapCause::LocalOverflow)
            );
            assert_eq!(feed.poll(), as_frame(3));
            assert_eq!(feed.stats().missed, 2);
        }

        #[test]
        fn what_is_not_the_account_stream_is_never_handed_on() {
            let (tx, rx) = sync_channel(8);
            let mut feed = AccountFeed::new(rx, Arc::default());
            for record in [
                frame(1),
                CaptureRecord::Venue {
                    local_recv_ts: Ts::from_millis(1),
                    ingest_seq: 2,
                    payload: b"{\"e\":\"trade\"}".to_vec(),
                },
                CaptureRecord::Control {
                    local_recv_ts: Ts::from_millis(1),
                    ingest_seq: 3,
                    record: ControlRecord::SnapshotFailed {
                        purpose: SnapshotPurpose::Resync,
                        reason: SnapshotFailure::Timeout,
                        attempts: 1,
                    },
                },
                frame(4),
            ] {
                tx.send(record).expect("open");
            }
            assert_eq!(feed.poll(), as_frame(1));
            assert_eq!(feed.poll(), as_frame(4), "no hole: the numbers were taken");
            assert_eq!(feed.poll(), Polled::Empty);
            assert_eq!(feed.stats().foreign, 2);
        }

        /// The live wiring's ingress: the capture primary, the consumer second.
        type Teed = Ingress<
            TeeSink<quant_recorder::CaptureSender, std::sync::mpsc::SyncSender<CaptureRecord>>,
        >;

        /// An account ingress behind a tee whose consumer side holds one
        /// record, with the tee's count and mark both kept: the live wiring.
        fn wired() -> (
            Teed,
            quant_recorder::CaptureReceiver,
            AccountFeed,
            Arc<std::sync::atomic::AtomicU64>,
        ) {
            let clock = Arc::new(ManualClock::new(Ts::from_millis(1_000)));
            let metrics = Metrics::new(16);
            let (capture, capture_rx) = channel(16, Arc::clone(&metrics));
            let (consumer, consumer_rx) = sync_channel(1);
            let drops = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let mark = Arc::new(LastDropped::default());
            let tee = TeeSink::new(capture, consumer)
                .counting_into(Arc::clone(&drops))
                .marking_into(Arc::clone(&mark));
            let ingress = Ingress::new(tee, clock, metrics);
            (
                ingress,
                capture_rx,
                AccountFeed::new(consumer_rx, mark),
                drops,
            )
        }

        fn accept<S: quant_recorder::RecordSink>(ingress: &mut Ingress<S>, n: i64) {
            ingress
                .accept_account_at(
                    format!("frame {n}").into_bytes(),
                    Ts::from_millis(1_000 + n),
                )
                .expect("the writer is there");
        }

        fn dropped(drops: &std::sync::atomic::AtomicU64) -> u64 {
            drops.load(std::sync::atomic::Ordering::Relaxed)
        }

        #[test]
        fn what_the_tee_drops_while_the_account_is_quiet_is_reported_at_once() {
            // The second and third are the consumer's loss and not the
            // capture's, and then the account goes quiet: nothing comes after
            // them to reveal the hole, which on this stream can be hours.
            let (mut ingress, capture_rx, mut feed, drops) = wired();
            for n in 1..=3 {
                accept(&mut ingress, n);
            }
            assert_eq!(feed.poll(), as_frame(1));
            assert_eq!(
                feed.poll(),
                blind(Ts::from_millis(1_003), GapCause::LocalOverflow),
                "told now, stamped by the last record lost"
            );
            assert_eq!(feed.poll(), Polled::Empty);
            assert_eq!(feed.stats().missed, dropped(&drops), "the counts agree");
            assert_eq!(feed.stats().holes, 1);
            // The record that follows the hole is not a second hole.
            accept(&mut ingress, 4);
            assert_eq!(feed.poll(), as_frame(4));
            assert_eq!(feed.poll(), Polled::Empty);
            assert_eq!((feed.stats().holes, feed.stats().missed), (1, 2));
            // And the capture has all four.
            drop(ingress);
            let mut captured = 0;
            while capture_rx
                .recv_timeout(std::time::Duration::from_millis(10))
                .is_ok()
            {
                captured += 1;
            }
            assert_eq!(captured, 4);
        }

        #[test]
        fn a_hole_a_record_reveals_first_is_told_once() {
            // The other order: the record after the hole arrives before the
            // feed finds its channel empty.
            let (mut ingress, _capture_rx, mut feed, drops) = wired();
            accept(&mut ingress, 1);
            accept(&mut ingress, 2);
            assert_eq!(feed.poll(), as_frame(1));
            accept(&mut ingress, 3);
            assert_eq!(
                feed.poll(),
                blind(Ts::from_millis(1_003), GapCause::LocalOverflow)
            );
            assert_eq!(feed.poll(), as_frame(3));
            assert_eq!(feed.poll(), Polled::Empty, "and not again from the mark");
            assert_eq!(feed.stats().missed, dropped(&drops));
            assert_eq!(feed.stats().holes, 1);
        }

        #[test]
        fn what_the_capture_drops_is_missed_too_and_the_tee_never_counts_it() {
            // The capture holds two and nothing drains it, so the third is
            // refused by the capture itself: it reaches neither side of the
            // tee. The consumer has room for everything.
            let clock = Arc::new(ManualClock::new(Ts::from_millis(1_000)));
            let metrics = Metrics::new(2);
            let (capture, capture_rx) = channel(2, Arc::clone(&metrics));
            let (consumer, consumer_rx) = sync_channel(16);
            let drops = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let mark = Arc::new(LastDropped::default());
            let tee = TeeSink::new(capture, consumer)
                .counting_into(Arc::clone(&drops))
                .marking_into(Arc::clone(&mark));
            let mut ingress = Ingress::new(tee, clock, metrics);
            let mut feed = AccountFeed::new(consumer_rx, mark);
            for n in 1..=3 {
                accept(&mut ingress, n);
            }
            // The writer catches up; the held gap record goes out ahead of
            // the next frame, taking the next number.
            while capture_rx
                .recv_timeout(std::time::Duration::from_millis(10))
                .is_ok()
            {}
            accept(&mut ingress, 5);
            assert_eq!(feed.poll(), as_frame(1));
            assert_eq!(feed.poll(), as_frame(2));
            let Polled::Record(AccountRecord::Blind {
                cause: GapCause::LocalOverflow,
                ..
            }) = feed.poll()
            else {
                panic!("the hole first");
            };
            let Polled::Record(AccountRecord::Blind {
                cause: GapCause::LocalOverflow,
                ..
            }) = feed.poll()
            else {
                panic!("then the gap ingress recorded for it");
            };
            assert!(matches!(
                feed.poll(),
                Polled::Record(AccountRecord::Frame { ingest_seq: 5, .. })
            ));
            assert_eq!(feed.poll(), Polled::Empty);

            let ingress_dropped = ingress.stats().dropped;
            assert_eq!((dropped(&drops), ingress_dropped), (0, 1));
            assert_eq!(
                feed.stats().missed,
                dropped(&drops) + ingress_dropped,
                "the tee's drops and the capture's together"
            );
        }

        #[test]
        fn a_hole_at_the_end_is_told_before_the_feed_closes() {
            let (mut ingress, _capture_rx, mut feed, _drops) = wired();
            accept(&mut ingress, 1);
            accept(&mut ingress, 2);
            drop(ingress);
            assert_eq!(feed.poll(), as_frame(1));
            assert_eq!(
                feed.poll(),
                blind(Ts::from_millis(1_002), GapCause::LocalOverflow)
            );
            assert_eq!(feed.poll(), Polled::Closed);
        }
    }

    /// The listener end to end, against [`crate::fake_venue`] on a local port.
    ///
    /// Real time at test scale ([`quick_policy`]): every wait is bounded, so
    /// a wedged listener fails a test rather than hanging the suite.
    ///
    /// [`quick_policy`]: crate::fake_venue::quick_policy
    mod listening {
        use std::path::PathBuf;
        use std::time::Duration;

        use quant_core::time::Ts;
        use quant_recorder::CaptureRecord;
        use quant_storage::{ControlRecord, FrameKind, RawFrame};

        use quant_recorder::BackoffPolicy;

        use super::super::{classify, AccountFrame, AccountPolicy, Refusal, Request, Stopped};
        use crate::fake_venue::{
            account_file_bytes, account_frames, quick_policy, FakeVenue, Listening, Reply, Said,
            Script, StatusReply,
        };

        /// Long enough for anything that should happen; a test that waits this
        /// long has failed.
        const WAIT: Duration = Duration::from_secs(10);

        const EXECUTION: &str = r#"{"subscriptionId":0,"event":{"e":"executionReport","E":1499405658658,"s":"BTCUSDT","c":"q-run-1","x":"NEW"}}"#;
        const POSITION: &str = r#"{"subscriptionId":0,"event":{"e":"outboundAccountPosition","E":1564034571105,"u":1564034571073,"B":[{"a":"ETH","f":"10000.000000","l":"0.000000"}]}}"#;
        const BALANCE: &str = r#"{"subscriptionId":0,"event":{"e":"balanceUpdate","E":1573200697110,"a":"BTC","d":"100.00000000","T":1573200697068}}"#;
        const TERMINATED: &str =
            r#"{"subscriptionId":0,"event":{"e":"eventStreamTerminated","E":1728973001334}}"#;
        const REVOKED: &str = r#"{"id":null,"status":401,"error":{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}}"#;

        /// A data root of this test's own, removed when it ends.
        struct Root(PathBuf);

        impl Root {
            fn new(name: &str) -> Self {
                let path = std::env::temp_dir()
                    .join(format!("quant-account-{}-{name}", std::process::id()));
                let _ = std::fs::remove_dir_all(&path);
                Self(path)
            }
        }

        impl Drop for Root {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        fn start(root: &Root, venue: &FakeVenue) -> Listening {
            Listening::start(&root.0, venue.url(), venue.credentials(), quick_policy())
        }

        /// One word per frame, for asserting the shape of a capture.
        fn shape(frames: &[RawFrame]) -> Vec<String> {
            frames
                .iter()
                .map(|frame| match frame.kind {
                    FrameKind::Control => match frame.control().expect("a control record") {
                        Some(ControlRecord::Gap { cause, .. }) => format!("gap {cause:?}"),
                        other => format!("control {other:?}"),
                    },
                    FrameKind::AccountPayload => match classify(&frame.payload) {
                        AccountFrame::Event { kind } => kind,
                        AccountFrame::Answer(answer) => answer
                            .request
                            .map_or_else(|| "unasked".to_owned(), |r| r.method().to_owned()),
                        AccountFrame::Unrecognised(_) => "unrecognised".to_owned(),
                    },
                    other => panic!("an account tree holding {other:?}"),
                })
                .collect()
        }

        fn shape_of(words: &[&str]) -> Vec<String> {
            words.iter().map(|w| (*w).to_owned()).collect()
        }

        fn payloads(frames: &[RawFrame]) -> Vec<&[u8]> {
            frames
                .iter()
                .filter(|f| f.kind == FrameKind::AccountPayload)
                .map(|f| f.payload.as_slice())
                .collect()
        }

        fn assert_contiguous(frames: &[RawFrame]) {
            let seqs: Vec<u64> = frames.iter().map(|f| f.ingest_seq).collect();
            let expected: Vec<u64> = (1..=seqs.len() as u64).collect();
            assert_eq!(seqs, expected, "ingest_seq contiguous from 1");
        }

        fn contains(haystack: &[u8], needle: &[u8]) -> bool {
            !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
        }

        /// What the consumer was handed, in the reader's terms.
        fn consumed(records: &[CaptureRecord]) -> Vec<(u64, Ts, Vec<u8>)> {
            records
                .iter()
                .map(|record| match record {
                    CaptureRecord::Account {
                        local_recv_ts,
                        ingest_seq,
                        payload,
                    } => (*ingest_seq, *local_recv_ts, payload.clone()),
                    CaptureRecord::Control {
                        local_recv_ts,
                        ingest_seq,
                        record,
                    } => (
                        *ingest_seq,
                        *local_recv_ts,
                        record.to_json().expect("serializes"),
                    ),
                    other => panic!("not an account record: {other:?}"),
                })
                .collect()
        }

        fn on_disk(frames: &[RawFrame]) -> Vec<(u64, Ts, Vec<u8>)> {
            frames
                .iter()
                .map(|f| (f.ingest_seq, f.local_recv_ts, f.payload.clone()))
                .collect()
        }

        async fn read(venue: &FakeVenue, text: &str) {
            venue
                .until("the listener to read a frame", WAIT, |log| {
                    log.read_by_listener(text)
                })
                .await;
        }

        async fn subscriptions(venue: &FakeVenue, n: usize) {
            venue
                .until("a subscription", WAIT, |log| log.subscribed == n)
                .await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn the_account_is_recorded_byte_for_byte_and_the_key_never_is() {
            let root = Root::new("recorded");
            let venue = FakeVenue::start(Script {
                ping_every: Some(Duration::from_millis(20)),
                ..Script::default()
            })
            .await;
            let listening = start(&root, &venue);
            subscriptions(&venue, 1).await;
            for event in [EXECUTION, POSITION, BALANCE] {
                venue.event(event);
            }
            read(&venue, BALANCE).await;
            venue
                .until("pongs and probes", WAIT, |log| {
                    log.pongs >= 2 && log.asked("session.status") >= 3
                })
                .await;
            let heard = listening.stop(WAIT).await;
            assert_eq!(heard.stopped, None);

            let log = venue.log();
            let frames = account_frames(&root.0);
            // Byte for byte and in order: the subscription's answer and the
            // three events, from the venue's side of the conversation.
            assert_eq!(payloads(&frames), log.recordable());
            assert_eq!(
                shape(&frames),
                shape_of(&[
                    "gap RecorderRestart",
                    "userDataStream.subscribe",
                    "executionReport",
                    "outboundAccountPosition",
                    "balanceUpdate",
                ])
            );
            assert_contiguous(&frames);

            // The answers that echo the key were sent, so their absence is the
            // rule at work and not an accident of the script.
            let key = venue.api_key();
            for said in [Said::LogonAnswer, Said::StatusAnswer] {
                assert!(
                    log.sent
                        .iter()
                        .any(|s| s.said == said && s.text.contains(key)),
                    "{said:?} sent, holding the key"
                );
            }
            assert!(log.asked("session.status") >= 3, "probes were answered");
            // No key, no signature, and no answer to a logon or a status: in
            // what the frames hold, and in the files as stored.
            let decoded: Vec<u8> = frames.iter().flat_map(|f| f.payload.clone()).collect();
            let stored = account_file_bytes(&root.0);
            let signatures = log.signatures();
            assert_eq!(signatures.len(), 1);
            for haystack in [&decoded, &stored] {
                assert!(!contains(haystack, key.as_bytes()), "the API key");
                for signature in &signatures {
                    assert!(!contains(haystack, signature.as_bytes()), "a signature");
                }
                // An answer to either is known by its id, which opens with
                // the request's token.
                for request in [Request::Logon, Request::Status] {
                    let id = format!("\"{}-", request.token());
                    assert!(!contains(haystack, id.as_bytes()), "{request}");
                }
            }
            // And the consumer was handed exactly what the file holds.
            assert_eq!(consumed(&heard.consumed), on_disk(&frames));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_dropped_connection_is_a_gap_a_reconnect_and_a_second_logon_and_subscribe() {
            let root = Root::new("dropped");
            let venue = FakeVenue::start(Script::default()).await;
            let listening = start(&root, &venue);
            subscriptions(&venue, 1).await;
            venue.event(EXECUTION);
            read(&venue, EXECUTION).await;
            venue.drop_connection();
            subscriptions(&venue, 2).await;
            venue.event(BALANCE);
            read(&venue, BALANCE).await;
            let heard = listening.stop(WAIT).await;
            assert_eq!(heard.stopped, None);

            let log = venue.log();
            let frames = account_frames(&root.0);
            assert_eq!(
                shape(&frames),
                shape_of(&[
                    "gap RecorderRestart",
                    "userDataStream.subscribe",
                    "executionReport",
                    "gap Disconnect",
                    "userDataStream.subscribe",
                    "balanceUpdate",
                ])
            );
            assert_eq!(payloads(&frames), log.recordable());
            assert_contiguous(&frames);
            assert_eq!(log.connections, 2);
            assert_eq!(log.asked("session.logon"), 2);
            assert_eq!(log.asked("userDataStream.subscribe"), 2);
            // Each logon signed afresh: a later timestamp and its own signature.
            let logons: Vec<_> = log
                .asked
                .iter()
                .filter(|a| a.method == "session.logon")
                .collect();
            assert!(logons[1].timestamp > logons[0].timestamp, "{logons:#?}");
            assert_ne!(logons[0].signature, logons[1].signature);
            assert_eq!(consumed(&heard.consumed), on_disk(&frames));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn server_shutdown_is_recorded_and_answered_with_a_new_connection() {
            let root = Root::new("shutdown");
            let venue = FakeVenue::start(Script::default()).await;
            let listening = start(&root, &venue);
            subscriptions(&venue, 1).await;
            // The venue sends it and keeps the connection open; leaving is
            // the listener's job.
            venue.server_shutdown();
            subscriptions(&venue, 2).await;
            venue.event(EXECUTION);
            read(&venue, EXECUTION).await;
            let heard = listening.stop(WAIT).await;
            assert_eq!(heard.stopped, None);

            let log = venue.log();
            let frames = account_frames(&root.0);
            assert_eq!(
                shape(&frames),
                shape_of(&[
                    "gap RecorderRestart",
                    "userDataStream.subscribe",
                    "serverShutdown",
                    "gap Disconnect",
                    "userDataStream.subscribe",
                    "executionReport",
                ])
            );
            assert_eq!(payloads(&frames), log.recordable());
            assert_contiguous(&frames);
            assert_eq!(log.connections, 2);
            assert_eq!(log.asked("session.logon"), 2);
            assert_eq!(log.asked("userDataStream.subscribe"), 2);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_gap_held_for_a_full_capture_goes_out_while_the_account_is_quiet() {
            use std::sync::Arc;

            use quant_core::time::{Clock, SystemClock};
            use quant_recorder::{channel, Ingress, Metrics, TeeSink};
            use quant_storage::ControlRecord;

            use super::super::listen;

            // A capture of two that nothing drains yet, and a consumer with
            // room for everything.
            let venue = FakeVenue::start(Script::default()).await;
            let metrics = Metrics::new(2);
            let (capture, capture_rx) = channel(2, Arc::clone(&metrics));
            let (consumer, consumer_rx) = std::sync::mpsc::sync_channel(16);
            let clock: Arc<dyn Clock> = Arc::new(SystemClock);
            let mut ingress = Ingress::new(TeeSink::new(capture, consumer), clock, metrics);
            let (url, credentials) = (venue.url().to_owned(), venue.credentials());
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let task = tokio::spawn(async move {
                let policy = quick_policy();
                tokio::select! {
                    stopped = listen(&url, &credentials, &mut ingress, &policy, 0) => Some(stopped),
                    _ = stopped => None,
                }
            });

            // The subscription's answer and the first event fill the
            // capture, so it refuses the second.
            subscriptions(&venue, 1).await;
            venue.event(EXECUTION);
            venue.event(BALANCE);
            read(&venue, BALANCE).await;
            // Then the writer catches up, and the account says nothing more.
            while capture_rx.recv_timeout(Duration::from_millis(10)).is_ok() {}

            let deadline = tokio::time::Instant::now() + WAIT;
            let mut handed = Vec::new();
            loop {
                handed.extend(consumer_rx.try_iter());
                let told = handed.iter().any(|record| {
                    matches!(
                        record,
                        CaptureRecord::Control {
                            record: ControlRecord::Gap {
                                cause: quant_core::event::GapCause::LocalOverflow,
                                ..
                            },
                            ..
                        }
                    )
                });
                if told {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the held gap never went out: {handed:?}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let _ = stop.send(());
            let ended = tokio::time::timeout(WAIT, task)
                .await
                .expect("the listener stops")
                .expect("the listening task");
            assert_eq!(ended, None);
            // The answer, the event the capture took, and the gap for the one
            // it refused, which took the next number after the hole.
            let seqs: Vec<u64> = handed.iter().map(CaptureRecord::ingest_seq).collect();
            assert_eq!(seqs, [1, 2, 4]);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_quiet_account_is_not_a_dead_one() {
            // No events at all, and no pings either: the venue answering its
            // probes is the only sign of life, and it is enough. Five seconds
            // is five times the probe's deadline and over a hundred probe
            // intervals, and in real time a quiet account is quiet for hours.
            // A rule that waited for the account to say something, scaled to
            // the same deadline, would have reconnected four times over a
            // stream that never went dark, and recorded a gap each time.
            let root = Root::new("quiet");
            let venue = FakeVenue::start(Script::default()).await;
            let listening = start(&root, &venue);
            subscriptions(&venue, 1).await;
            let quiet = Duration::from_secs(5);
            assert!(quiet >= quick_policy().probe_timeout * 5);
            tokio::time::sleep(quiet).await;
            let heard = listening.stop(WAIT).await;
            assert_eq!(heard.stopped, None);

            let log = venue.log();
            assert_eq!(log.connections, 1, "no reconnect");
            assert_eq!(log.asked("session.logon"), 1);
            assert_eq!(log.asked("userDataStream.subscribe"), 1);
            assert!(
                log.asked("session.status") >= 20,
                "probed throughout: {}",
                log.asked("session.status")
            );
            assert!(log.sent.iter().all(|s| s.said != Said::Event));
            assert_eq!(
                shape(&account_frames(&root.0)),
                shape_of(&["gap RecorderRestart", "userDataStream.subscribe"]),
                "no gap"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_venue_that_stops_answering_is_found_even_while_it_still_pings() {
            let root = Root::new("dead");
            let venue = FakeVenue::start(Script {
                ping_every: Some(Duration::from_millis(20)),
                ..Script::default()
            })
            .await;
            let listening = start(&root, &venue);
            subscriptions(&venue, 1).await;
            // Still pinging, answering nothing: a socket that is alive and an
            // API that is not.
            venue.stop_answering();
            let pongs = venue.log().pongs;
            subscriptions(&venue, 2).await;
            assert!(
                venue.log().pongs > pongs,
                "the dead connection kept pinging, and was answered"
            );
            // And one that says nothing at all.
            venue.go_dark();
            subscriptions(&venue, 3).await;
            let heard = listening.stop(WAIT).await;
            assert_eq!(heard.stopped, None);

            let log = venue.log();
            let frames = account_frames(&root.0);
            assert_eq!(
                shape(&frames),
                shape_of(&[
                    "gap RecorderRestart",
                    "userDataStream.subscribe",
                    "gap Disconnect",
                    "userDataStream.subscribe",
                    "gap Disconnect",
                    "userDataStream.subscribe",
                ])
            );
            assert_contiguous(&frames);
            assert_eq!(log.connections, 3);
            assert_eq!(log.asked("session.logon"), 3);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_subscription_the_venue_says_is_gone_is_made_again() {
            let root = Root::new("lost");
            let venue = FakeVenue::start(Script::default()).await;
            let listening = start(&root, &venue);
            subscriptions(&venue, 1).await;
            venue.script(|s| s.status.push_back(StatusReply::Inactive));
            subscriptions(&venue, 2).await;
            let heard = listening.stop(WAIT).await;
            assert_eq!(heard.stopped, None);
            assert_eq!(
                shape(&account_frames(&root.0)),
                shape_of(&[
                    "gap RecorderRestart",
                    "userDataStream.subscribe",
                    "gap Disconnect",
                    "userDataStream.subscribe",
                ])
            );
            assert_eq!(venue.log().connections, 2);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_terminated_event_stream_is_recorded_and_made_again() {
            let root = Root::new("terminated");
            let venue = FakeVenue::start(Script::default()).await;
            let listening = start(&root, &venue);
            subscriptions(&venue, 1).await;
            venue.event(TERMINATED);
            subscriptions(&venue, 2).await;
            let heard = listening.stop(WAIT).await;
            assert_eq!(heard.stopped, None);
            let frames = account_frames(&root.0);
            assert_eq!(
                shape(&frames),
                shape_of(&[
                    "gap RecorderRestart",
                    "userDataStream.subscribe",
                    "eventStreamTerminated",
                    "gap Disconnect",
                    "userDataStream.subscribe",
                ])
            );
            assert_eq!(payloads(&frames), venue.log().recordable());
        }

        /// A logon refused for a reason that will not fix itself.
        struct Permanent {
            name: &'static str,
            script: Script,
            another_key: bool,
            status: u16,
            code: i64,
        }

        fn refused_logon(reply: Reply) -> Script {
            Script {
                logon: [reply].into(),
                ..Script::default()
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_logon_that_will_never_be_accepted_stops_the_listener_loudly() {
            // A bad key, a ban, a clock outside recvWindow in either
            // direction, and a signature by the wrong key: each is refused on
            // the first logon, and the listener stops there rather than
            // asking again forever.
            let cases = [
                Permanent {
                    name: "revoked",
                    script: refused_logon(Reply::refuse(
                        401,
                        -2015,
                        "Invalid API-key, IP, or permissions for action.",
                    )),
                    another_key: false,
                    status: 401,
                    code: -2015,
                },
                Permanent {
                    name: "banned",
                    script: refused_logon(Reply::refuse(
                        418,
                        -1003,
                        "Way too much request weight used",
                    )),
                    another_key: false,
                    status: 418,
                    code: -1003,
                },
                Permanent {
                    name: "stale",
                    script: Script {
                        clock_ahead_ms: 10_000,
                        ..Script::default()
                    },
                    another_key: false,
                    status: 400,
                    code: -1021,
                },
                Permanent {
                    name: "early",
                    script: Script {
                        clock_ahead_ms: -2_000,
                        ..Script::default()
                    },
                    another_key: false,
                    status: 400,
                    code: -1021,
                },
                Permanent {
                    name: "wrong-key",
                    script: Script::default(),
                    another_key: true,
                    status: 400,
                    code: -1022,
                },
            ];
            for case in cases {
                let root = Root::new(&format!("refused-{}", case.name));
                let venue = FakeVenue::start(case.script).await;
                let credentials = if case.another_key {
                    venue.credentials_with_another_key()
                } else {
                    venue.credentials()
                };
                let listening = Listening::start(&root.0, venue.url(), credentials, quick_policy());
                let heard = listening.stopped_by_itself(WAIT).await;
                let Some(Stopped::Refused(Refusal {
                    request,
                    status,
                    code,
                    ..
                })) = heard.stopped
                else {
                    panic!("{}: {:?}", case.name, heard.stopped);
                };
                assert_eq!(
                    (request, status, code),
                    (Some(Request::Logon), case.status, Some(case.code)),
                    "{}",
                    case.name
                );
                assert_eq!(venue.log().connections, 1, "{}: not retried", case.name);
                // The refusal answered a logon, so it is not recorded either.
                assert_eq!(
                    shape(&account_frames(&root.0)),
                    shape_of(&["gap RecorderRestart"]),
                    "{}",
                    case.name
                );
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_logon_is_stamped_on_the_venue_clock_the_orders_are() {
            // A host 1.5 s ahead of the venue, whose offset was measured at
            // 900 ms, inside the order path's gate, and has drifted 600 ms
            // since. An order stamped with the offset taken off lands 600 ms
            // ahead, inside the venue's one second; so must the logon. The
            // same host with no offset taken off is refused -1021, which is
            // what the listener did before the correction, and which stops
            // it for good.
            let measured = 900;
            assert!(measured <= crate::trade::MAX_CLOCK_SKEW_MS);
            for (name, offset_ms, accepted) in [("corrected", measured, true), ("raw", 0, false)] {
                let root = Root::new(&format!("offset-{name}"));
                let venue = FakeVenue::start(Script {
                    clock_ahead_ms: -1_500,
                    ..Script::default()
                })
                .await;
                let listening = Listening::start_with_offset(
                    &root.0,
                    venue.url(),
                    venue.credentials(),
                    quick_policy(),
                    offset_ms,
                );
                if accepted {
                    subscriptions(&venue, 1).await;
                    let heard = listening.stop(WAIT).await;
                    assert_eq!(heard.stopped, None, "{name}");
                } else {
                    let heard = listening.stopped_by_itself(WAIT).await;
                    let Some(Stopped::Refused(refusal)) = heard.stopped else {
                        panic!("{name}: {:?}", heard.stopped);
                    };
                    assert_eq!(refusal.code, Some(-1021), "{name}");
                    assert_eq!(venue.log().subscribed, 0, "{name}");
                }
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_refusal_that_may_clear_is_retried_on_the_backoff_as_one_hole() {
            let root = Root::new("transient");
            let venue = FakeVenue::start(Script::default()).await;
            let listening = start(&root, &venue);
            subscriptions(&venue, 1).await;
            venue.script(|s| {
                s.logon
                    .push_back(Reply::refuse(503, -1008, "Server is currently overloaded"));
                s.logon
                    .push_back(Reply::refuse(429, -1003, "Too much request weight used"));
            });
            venue.drop_connection();
            subscriptions(&venue, 2).await;
            let heard = listening.stop(WAIT).await;
            assert_eq!(heard.stopped, None);

            let log = venue.log();
            assert_eq!(log.connections, 4, "two refused, then accepted");
            assert_eq!(log.asked("session.logon"), 4);
            // Blind from the drop until the second subscription, and that is
            // one hole however many attempts it took.
            assert_eq!(
                shape(&account_frames(&root.0)),
                shape_of(&[
                    "gap RecorderRestart",
                    "userDataStream.subscribe",
                    "gap Disconnect",
                    "userDataStream.subscribe",
                ])
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn the_backoff_resets_on_a_subscription_not_on_a_connection() {
            // A venue that takes the TCP connection and refuses the logon,
            // again and again, must be retried on a curve: each connection
            // costs weight (web-socket-api.md:516) out of 300 a five minutes
            // (:389). But a connection the venue subscribed is a success, and
            // the next failure after it starts short again.
            //
            // The curve is wide enough to tell apart on a loaded machine:
            // with equal jitter, attempt 0 waits 100-200 ms, attempt 1
            // 400-800 ms, attempt 2 1.6-3.2 s.
            let policy = AccountPolicy {
                backoff: BackoffPolicy {
                    initial: Duration::from_millis(200),
                    max: Duration::from_millis(3_200),
                    multiplier: 4,
                },
                ..quick_policy()
            };
            let busy = || Reply::refuse(503, -1008, "Server is currently overloaded");
            let root = Root::new("backoff");
            let venue = FakeVenue::start(Script {
                logon: [busy(), busy()].into(),
                ..Script::default()
            })
            .await;
            let listening = Listening::start(&root.0, venue.url(), venue.credentials(), policy);
            subscriptions(&venue, 1).await;
            venue.script(|s| s.logon.extend([busy(), busy()]));
            venue.drop_connection();
            subscriptions(&venue, 2).await;
            let heard = listening.stop(WAIT).await;
            assert_eq!(heard.stopped, None);

            let log = venue.log();
            assert_eq!(
                log.connections, 6,
                "refused twice, heard, refused twice, heard"
            );
            let waits = log.waits();
            let ms = |d: Duration| d.as_millis();
            // Refused twice before ever being heard: the curve climbs.
            assert!(ms(waits[1]) >= 300, "the second wait grew: {waits:?}");
            // Heard, then dropped: the next failure starts short again.
            assert!(ms(waits[2]) < 1_000, "reset by the subscription: {waits:?}");
            // And climbs again, though every refused attempt connected.
            assert!(ms(waits[3]) >= 300, "not reset by a connection: {waits:?}");
            assert!(
                ms(waits[4]) >= 1_200,
                "not reset by a connection: {waits:?}"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_connection_that_fails_is_retried_rather_than_fatal() {
            // A port that takes the TCP connection and drops it at once, so
            // every attempt fails before the WebSocket handshake ends: the
            // reset a venue restart produces. Counted on this side, so the
            // retries are seen rather than inferred. (A port with nothing on
            // it would do as well, except that on Windows a refused connect
            // takes about two seconds to come back, which a test would have
            // to wait out on every attempt.)
            let root = Root::new("reset");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let url = format!("ws://{}", listener.local_addr().expect("address"));
            let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counted = std::sync::Arc::clone(&attempts);
            let resetting = tokio::spawn(async move {
                while let Ok((tcp, _)) = listener.accept().await {
                    counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    drop(tcp);
                }
            });
            let venue = FakeVenue::start(Script::default()).await;
            let listening = Listening::start(&root.0, &url, venue.credentials(), quick_policy());
            let deadline = tokio::time::Instant::now() + WAIT;
            while attempts.load(std::sync::atomic::Ordering::Relaxed) < 3 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "gave up waiting for three attempts"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let heard = listening.stop(WAIT).await;
            resetting.abort();
            assert_eq!(heard.stopped, None, "still trying");
            // Never heard, so nothing more than the session's opening gap.
            assert_eq!(
                shape(&account_frames(&root.0)),
                shape_of(&["gap RecorderRestart"])
            );
        }

        /// A port that answers every WebSocket upgrade with `status`, and
        /// counts the attempts.
        async fn refusing_upgrades(
            status: &'static str,
        ) -> (
            String,
            std::sync::Arc<std::sync::atomic::AtomicUsize>,
            tokio::task::JoinHandle<()>,
        ) {
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let url = format!("ws://{}", listener.local_addr().expect("address"));
            let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counted = std::sync::Arc::clone(&attempts);
            let refusing = tokio::spawn(async move {
                while let Ok((mut tcp, _)) = listener.accept().await {
                    counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Read the request through its blank line, then refuse it.
                    let mut request = Vec::new();
                    let mut buf = [0_u8; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match tcp.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }
                    let answer = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    let _ = tcp.write_all(answer.as_bytes()).await;
                    let _ = tcp.shutdown().await;
                }
            });
            (url, attempts, refusing)
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn an_upgrade_refused_for_good_stops_the_listener_and_one_that_may_clear_does_not() {
            // A ban, or a forbidden address, answered at the upgrade rather
            // than inside the connection: the same rule as an answer's.
            for (status, code) in [("418 I'm a teapot", 418), ("403 Forbidden", 403)] {
                let root = Root::new(&format!("upgrade-{code}"));
                let (url, attempts, refusing) = refusing_upgrades(status).await;
                let venue = FakeVenue::start(Script::default()).await;
                let listening =
                    Listening::start(&root.0, &url, venue.credentials(), quick_policy());
                let heard = listening.stopped_by_itself(WAIT).await;
                refusing.abort();
                assert_eq!(
                    heard.stopped,
                    Some(Stopped::UpgradeRefused(code)),
                    "{status}"
                );
                assert_eq!(
                    attempts.load(std::sync::atomic::Ordering::Relaxed),
                    1,
                    "{status}: not retried"
                );
            }
            // A busy venue at the upgrade is retried, as a busy answer is.
            let root = Root::new("upgrade-503");
            let (url, attempts, refusing) = refusing_upgrades("503 Service Unavailable").await;
            let venue = FakeVenue::start(Script::default()).await;
            let listening = Listening::start(&root.0, &url, venue.credentials(), quick_policy());
            let deadline = tokio::time::Instant::now() + WAIT;
            while attempts.load(std::sync::atomic::Ordering::Relaxed) < 3 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "gave up waiting for three attempts"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let heard = listening.stop(WAIT).await;
            refusing.abort();
            assert_eq!(heard.stopped, None, "still trying");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_refused_subscription_is_recorded_and_stops_the_listener() {
            let root = Root::new("unsubscribed");
            let venue = FakeVenue::start(Script {
                subscribe: [Reply::refuse(
                    400,
                    -2035,
                    "User Data Stream subscription already active.",
                )]
                .into(),
                ..Script::default()
            })
            .await;
            let listening = start(&root, &venue);
            let heard = listening.stopped_by_itself(WAIT).await;
            let Some(Stopped::Refused(refusal)) = heard.stopped else {
                panic!("{:?}", heard.stopped);
            };
            assert_eq!(refusal.request, Some(Request::Subscribe));
            assert_eq!(refusal.code, Some(-2035));
            assert_eq!(venue.log().connections, 1);
            // The subscription's answer is recorded whatever it said.
            let frames = account_frames(&root.0);
            assert_eq!(
                shape(&frames),
                shape_of(&["gap RecorderRestart", "userDataStream.subscribe"])
            );
            assert_eq!(payloads(&frames), venue.log().recordable());
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_key_revoked_mid_session_is_recorded_and_stops_the_listener_blind() {
            let root = Root::new("revoked");
            let venue = FakeVenue::start(Script::default()).await;
            let listening = start(&root, &venue);
            subscriptions(&venue, 1).await;
            venue.unsolicited(REVOKED);
            let heard = listening.stopped_by_itself(WAIT).await;
            let Some(Stopped::Refused(refusal)) = heard.stopped else {
                panic!("{:?}", heard.stopped);
            };
            assert_eq!((refusal.request, refusal.code), (None, Some(-2015)));
            // Recorded, and followed by the gap that says the account is no
            // longer being heard.
            assert_eq!(
                shape(&account_frames(&root.0)),
                shape_of(&[
                    "gap RecorderRestart",
                    "userDataStream.subscribe",
                    "unasked",
                    "gap Disconnect",
                ])
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_status_without_subscriptions_or_without_the_flag_stops_the_listener() {
            for (name, reply, expected) in [
                (
                    "unavailable",
                    StatusReply::Unavailable,
                    Stopped::Unavailable,
                ),
                (
                    "missing",
                    StatusReply::Missing,
                    Stopped::NotUnderstood(
                        "session.status answered without result.userDataStream".to_owned(),
                    ),
                ),
            ] {
                let root = Root::new(&format!("status-{name}"));
                let venue = FakeVenue::start(Script {
                    status: [reply].into(),
                    ..Script::default()
                })
                .await;
                let listening = start(&root, &venue);
                let heard = listening.stopped_by_itself(WAIT).await;
                assert_eq!(heard.stopped, Some(expected), "{name}");
                assert_eq!(venue.log().connections, 1, "{name}");
                assert_eq!(
                    shape(&account_frames(&root.0)),
                    shape_of(&[
                        "gap RecorderRestart",
                        "userDataStream.subscribe",
                        "gap Disconnect",
                    ]),
                    "{name}"
                );
            }
        }
    }
}
