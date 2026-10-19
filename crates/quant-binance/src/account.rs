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
//! same line for market data in [`crate::decode`].
//!
//! # Why our request ids name the request
//!
//! *"Request `id` is truly arbitrary ... The server does not interpret `id` in
//! any way, simply echoing it back in the response"* (web-socket-api.md:162-163).
//! So each id is the method and a counter, `userDataStream.subscribe-3`, and an
//! answer says which request it answers by itself. A listener that remembered
//! the ids it sent would know the same thing, but only while it was running;
//! a recorded answer read back next month would not. The subscription's answer
//! is the record that the account was being heard from then on, so it has to
//! be recognisable from the file alone.
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

use core::fmt;

use serde_json::Value;

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

    /// The id for the `n`th request this listener sends: the method, a `-`,
    /// and the counter. No method name has a `-` in it, so the last one splits
    /// the two unambiguously.
    #[must_use]
    pub fn id(self, n: u64) -> String {
        format!("{}-{n}", self.method())
    }

    /// Which request an id names, if it is one of ours.
    #[must_use]
    pub fn of_id(id: &str) -> Option<Self> {
        let (method, n) = id.rsplit_once('-')?;
        if n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Self::ALL.into_iter().find(|r| r.method() == method)
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
/// answer from the matching engine), -1008 server busy, -1016 shutting down,
/// -1034 too many connections, and -2042, which ends a connection's
/// subscriptions until a new one is made (errors.md:13-66, :359-360;
/// web-socket-api.md:8190-8191).
///
/// Except a 418. It is an IP ban (web-socket-api.md:318, :522-523), and a ban
/// is extended by exactly what a retry loop does, so it stops the listener
/// whatever code comes with it, -1003 included.
#[must_use]
pub fn is_transient(status: u16, code: Option<i64>) -> bool {
    const TRANSIENT_CODES: [i64; 9] = [
        -1000, -1001, -1003, -1006, -1007, -1008, -1016, -1034, -2042,
    ];
    if status == 418 {
        return false;
    }
    if (500..=599).contains(&status) || status == 429 {
        return true;
    }
    code.is_some_and(|code| TRANSIENT_CODES.contains(&code))
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A";

    /// The venue's documented answer to `session.logon` (web-socket-api.md:3411-3425),
    /// with one of our ids.
    const LOGON_ANSWER: &str = r#"{"id":"session.logon-1","status":200,"result":{"apiKey":"vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A","authorizedSince":1649729878532,"connectedSince":1649729873021,"returnRateLimits":false,"serverTime":1649729878630,"userDataStream":false}}"#;

    /// `session.status`'s (web-socket-api.md:3451-3466).
    const STATUS_ANSWER: &str = r#"{"id":"session.status-7","status":200,"result":{"apiKey":"vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A","authorizedSince":1649729878532,"connectedSince":1649729873021,"returnRateLimits":false,"serverTime":1649730611671,"userDataStream":true}}"#;

    /// `userDataStream.subscribe`'s (web-socket-api.md:8222-8230).
    const SUBSCRIBE_ANSWER: &str =
        r#"{"id":"userDataStream.subscribe-2","status":200,"result":{"subscriptionId":0}}"#;

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
            "session.logon",
            "session.logon-",
            "session.logon-1x",
            "session.logon-+1",
            "order.place-1",
            "c174a2b1-3f51-4580-b200-8528bd237cb7",
            "Session.logon-1",
        ] {
            assert_eq!(Request::of_id(stranger), None, "{stranger:?}");
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
            format!(r#"{{"id":"session.status-1","status":200,"result":{result}}}"#).as_bytes(),
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
        let refused = r#"{"id":"session.logon-3","status":400,"error":{"code":-1022,"msg":"Signature for this request is not valid."}}"#;
        assert_eq!(
            rule(refused),
            Keep::Withhold(Withheld::SessionAnswer(Request::Logon))
        );
        let refused = r#"{"id":"session.status-3","status":503,"error":{"code":-1001,"msg":"x"}}"#;
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
            format!(
                r#"{{"id":"userDataStream.subscribe-9","status":200,"result":{{"apiKey":"{KEY}"}}}}"#
            ),
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
            (400, Some(-1016)),
            (400, Some(-1034)),
            (400, Some(-2042)),
        ] {
            assert!(is_transient(status, code), "{status} {code:?}");
        }
    }
}
