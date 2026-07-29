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

pub mod connection;
pub mod stream;

pub use connection::{ConnectionPolicy, EndReason};
pub use stream::{StreamKind, StreamSpec, SPOT_WS};
