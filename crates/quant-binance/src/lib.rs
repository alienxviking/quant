//! The Binance venue adapter.
//!
//! # What this crate is allowed to know
//!
//! Only how Binance talks. Stream names, the combined-stream URL shape, the
//! keepalive convention, how long a connection lives before the venue closes it.
//! Everything about what happens to the bytes afterwards -- timestamping,
//! sequencing, what to do under overload, framing, compression -- lives in
//! `quant-recorder` and `quant-storage`, because none of it is Binance-specific
//! and a second venue must inherit it rather than reimplement it.
//!
//! The test of that boundary: adding Coinbase should mean writing a new
//! [`stream::StreamSpec`] equivalent and a new [`connection::run`], and touching
//! nothing else.
//!
//! # One connection per symbol
//!
//! Binance can multiplex many symbols onto one socket, and we deliberately do not
//! do that yet, for two reasons.
//!
//! First, routing a multiplexed stream means parsing every payload on the hot
//! path just to discover which symbol it belongs to -- so the read task, whose
//! entire job is to not be the bottleneck, would be running a JSON parse per
//! message. Per-symbol connections make routing free: the socket *is* the route.
//!
//! Second, it isolates failure. One symbol's reconnect blinds only that symbol,
//! and `ingest_seq` stays naturally per-instrument with no cross-symbol
//! coordination to get wrong -- which is exactly the definition the data contract
//! gives it.
//!
//! The cost is one socket per symbol, which is irrelevant at the handful of
//! instruments M1 records and becomes a real constraint somewhere north of fifty,
//! where connection rate limits start to bite. That is the point to revisit it,
//! and the change is contained entirely within this crate.
//!
//! # Where async begins
//!
//! Here, and only here. This side of the seam is pure waiting on a network, which
//! is what async is genuinely good for. The other side -- framing, zstd, file
//! writes -- is blocking CPU and I/O and stays on a thread. See `quant-recorder`'s
//! crate docs for why that split is not optional.
//!
//! # Why a recorder makes REST calls
//!
//! It should not have to, and for trades it does not. But Binance's depth stream
//! is incremental, and an incremental stream is meaningless without a book to
//! apply it to -- which only `GET /api/v3/depth` provides, and only for *now*.
//!
//! That is the one piece of capture that is irreversible. Every other
//! interpretation of the venue can be redone from bytes already on disk; a
//! snapshot not taken at a reconnect can never be taken, and the deltas that
//! follow stay unanchored forever. So [`rest`] exists, and it stops there: it does
//! not parse the body, discard stale deltas, or verify that the update-id chain
//! joins. Those are steps in the *book* algorithm, they are pure functions of what
//! we are writing down, and they belong to the normalizer at M2 where a mistake
//! can be fixed by re-deriving rather than by re-recording a week.

pub mod connection;
pub mod parse;
pub mod rest;
pub mod sequence;
pub mod stream;

/// Install the TLS backend, if the process has not already chosen one.
///
/// rustls 0.23 refuses to guess between `ring` and `aws-lc-rs` and instead reads a
/// process-global default. Nothing sets it for us, and the two consumers fail
/// differently and both badly: `tokio-tungstenite` panics at the first handshake,
/// which is to say in production during a reconnect, looking like a venue outage;
/// `reqwest` panics inside `Client::build`, so merely constructing a
/// [`SnapshotClient`] in a unit test brings the test down.
///
/// Hence one function, called from both the binary's startup and
/// [`SnapshotClient::new`]. Calling it from a constructor is a global side effect
/// and worth a second thought, but the alternative is a library whose constructor
/// panics unless its caller knew to install a cryptography backend first. It is
/// idempotent and never overrides a choice already made: `install_default`
/// returns an error when a provider is present, and that error is the success
/// case here.
///
/// `ring` rather than `aws-lc-rs` because aws-lc-rs wants cmake and nasm on
/// Windows, and this has to build on a stock developer machine.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub use connection::{ConnectionPolicy, EndReason};
pub use parse::{parse_snapshot, parse_stream_message, ParseError};
pub use rest::{SnapshotClient, SnapshotError, DEPTH_LIMIT, SPOT_REST};
pub use sequence::{classify, snapshot_last_update_id, SequenceError, StreamMessage};
pub use stream::{StreamKind, StreamSpec, SPOT_WS};
