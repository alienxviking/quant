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

    /// The next event, or [`Wake::Idle`] if `timeout` passes first.
    ///
    /// # Why the engine needs a second way to be woken
    ///
    /// Until M8 the engine had exactly one input. Market data arrived, and the
    /// venue's answers were a pure function of it — `SimulatedVenue` produces an
    /// outcome only inside `observe`, which the engine calls while handling an
    /// event it already has. So blocking here forever was not merely acceptable,
    /// it was free.
    ///
    /// A live venue speaks on its own schedule. A fill can arrive while the
    /// market socket is stalled, and Binance closes a stream every 24 hours by
    /// design against a 120-second idle timeout, so that window opens many times
    /// in a run. An engine parked in `next_event` cannot book that fill, cannot
    /// tell the strategy, and cannot move the risk layer's daily tally — which
    /// is the state in which not knowing is most expensive.
    ///
    /// # Why it is defaulted, and what the default guarantees
    ///
    /// The default ignores `timeout` and calls [`Self::next_event`], so it can
    /// **never** return `Idle`. That is not a stub: for a source reading a file
    /// there is genuinely nothing else to wait for, and a historical replay that
    /// could idle would make a backtest's event sequence depend on how fast the
    /// disk was. `HistoricalSource` therefore stays bit-identical through this
    /// change, which is the no-op property `Costs::NONE` established as the way
    /// to make a seam change checkable.
    ///
    /// Only a source with a second thing to wait for overrides it.
    fn next_event_timeout(
        &mut self,
        timeout: core::time::Duration,
    ) -> Option<Result<Wake, SourceError>> {
        let _ = timeout;
        self.next_event().map(|r| r.map(Wake::Event))
    }
}

/// What woke the engine.
///
/// Three outcomes rather than two, and the third is the point: *the stream
/// ended*, *here is an event*, and *nothing arrived, go and look elsewhere*. An
/// implementation that collapsed `Idle` into `None` would tell the engine the
/// run was over every time the market went quiet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Wake {
    /// A market event, exactly as `next_event` would have returned it.
    Event(MarketEvent),
    /// The timeout passed with nothing to deliver. Not an end, not an error.
    Idle,
}
