//! Where market events come from.
//!
//! One trait, three implementations that live elsewhere: a historical source
//! reading the normalized tier as fast as it can, a replay source pacing raw
//! capture against the wall clock, and a live source on a venue socket.
//!
//! It lives in `quant-core` rather than in the engine so that the crates which
//! *provide* events — the normalizer, a venue adapter — do not have to depend on
//! the engine to be usable by it. The arrows keep pointing one way.
//!
//! # Why an error is not the end of the stream
//!
//! [`EventSource::next_event`] returns `Option<Result<..>>` and not `Option<..>`.
//! The simpler signature has one failure mode and it is the worst one available:
//! a source that hits an unreadable file returns `None`, the engine sees a clean
//! end of stream, and the backtest silently covers less data than it claims to.
//! Every number downstream is then computed over a period nobody chose.
//!
//! So "I am finished" and "I broke" are different answers, and the engine has to
//! handle them differently.

use crate::event::MarketEvent;

/// A source could not produce the next event.
///
/// A string rather than an enum because the engine's only sensible response is
/// to stop and say why: it cannot retry a corrupt Parquet page or reconnect a
/// socket on the source's behalf. The source logs the detail it has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceError(pub String);

impl core::fmt::Display for SourceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SourceError {}

/// A stream of market events in `local_recv_ts` order.
///
/// # What an implementation promises
///
/// - Events come out in non-decreasing `local_recv_ts` order. The engine
///   dispatches on that and only that (invariant 2), so a source that reordered
///   would introduce lookahead the engine cannot detect.
/// - `Gap` events are delivered, not filtered. They are what tells a strategy it
///   was blind (invariant 3), and a source that dropped them would make a
///   disconnect indistinguishable from a quiet market.
/// - Once `None` is returned, the stream is over.
pub trait EventSource {
    /// The next event, `None` at the end, `Some(Err(..))` if the source failed.
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>>;
}
