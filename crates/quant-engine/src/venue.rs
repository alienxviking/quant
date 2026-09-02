//! Where orders go.
//!
//! Three implementations eventually: simulated (M3), paper (M5) and live (M8).
//! The trait is what makes them interchangeable, and its shape is the contract's
//! central claim — **submission returns nothing**.
//!
//! # Why `submit` has no return value
//!
//! `submit(order) -> Result<Fill>` cannot be implemented honestly by a live
//! venue: there is a network round trip in the middle. A simulated venue *can*
//! answer instantly, which is precisely the problem — a strategy written against
//! that signature would receive, in backtest only, the outcome of its own order
//! at the moment of placing it. Tuned on that, it is tuned on a machine that
//! does not exist.
//!
//! So outcomes come back through [`ExecutionVenue::poll`], into the same loop as
//! market data, and the asynchrony is in the contract rather than in one
//! implementation of it.

use quant_book::Book;
use quant_core::event::MarketEvent;
use quant_core::execution::{ClientOrderId, ExecutionEvent, OrderRequest};
use quant_core::time::Ts;

/// Somewhere an order can be sent.
pub trait ExecutionVenue {
    /// Send an order. Returns nothing; see the module docs.
    ///
    /// The engine has already minted `client_order_id` and already checked the
    /// request against the risk layer, so an implementation's job is to send it
    /// and report back — not to second-guess the seam.
    fn submit(&mut self, client_order_id: ClientOrderId, request: &OrderRequest, now: Ts);

    /// Ask for an order to be cancelled.
    ///
    /// Also fire-and-forget, and for a sharper reason than submission: a cancel
    /// can *lose the race* with a fill. A synchronous `cancel() -> bool` would
    /// have to invent an answer to "did it work", and every strategy would
    /// believe it.
    fn cancel(&mut self, client_order_id: ClientOrderId, now: Ts);

    /// Show the venue what the market just did.
    ///
    /// A simulated venue matches resting orders against this. A live or paper
    /// venue ignores it — the real market already knows — which is why the
    /// default implementation does nothing and only the simulator overrides it.
    ///
    /// Called *before* the strategy sees the event, so an order resting from
    /// earlier can trade against it and an order placed on seeing it cannot.
    /// That ordering is the engine's, not the venue's; see `lib.rs`.
    fn observe(&mut self, event: &MarketEvent, book: &Book, now: Ts) {
        let _ = (event, book, now);
    }

    /// Take everything the venue has told us **by** `now`.
    ///
    /// `now` is a parameter rather than something the venue remembers from the
    /// last [`Self::observe`], because a report that is not yet due has to stay
    /// undelivered and a venue inferring the time from call order would be one
    /// refactor away from being wrong. A simulator with inbound latency holds
    /// events until their delivery time; a live venue ignores it and hands over
    /// whatever arrived.
    ///
    /// Appends rather than returning, so a busy venue does not allocate a vector
    /// per event on the hot path.
    fn poll(&mut self, now: Ts, out: &mut Vec<ExecutionEvent>);
}
