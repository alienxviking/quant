//! Where orders go.
//!
//! Two implementations: simulated (M3), which paper trading uses as well -- M5
//! found that a paper venue would be the simulator again -- and live (M8, not
//! built yet). The trait is what makes them interchangeable, and its shape is
//! the contract's central claim — **submission returns nothing**.
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
use quant_core::execution::{ClientOrderId, OrderRequest};
use quant_core::report::VenueReport;
use quant_core::time::Ts;

/// Why a venue will not take an order now.
///
/// The strategy hears only `RejectReason::Venue`. Which of these it was is for
/// the record and for a person reading it, on the argument that keeps a risk
/// refusal's `Bound` from the strategy: a strategy that could tell "the stream
/// is down" from "an order is in doubt" could tell which venue it was wired to.
///
/// Not `NotReady`, because `quant_binance::trade::NotReady` already means
/// something else: a verdict on the host, before a run starts, that it must not
/// start. This is a verdict on one order, at one instant, from a run under way.
///
/// **Closed now, before anything produces most of it,** because it is
/// serialized inside `refused` journal lines, as `{"venue":"stream_down"}`. A
/// variant added after a journal holds one is a change to the journal's schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unready {
    /// `venue-check` has not passed: the clock is more than 1000 ms from the
    /// venue's, or the key or its permission to trade was refused.
    Unchecked,
    /// No venue-sourced filters for this instrument. Refusing to submit, and
    /// recording regardless, is `docs/live-run.md` §5's rule.
    NoFilters,
    /// A non-empty journal without a run tag, so ids this run mints could
    /// repeat ids the venue has already seen.
    Untagged,
    /// No balance snapshot was taken at the start, so criterion A could not be
    /// judged.
    NoBaseline,
    /// The account stream is not subscribed, so nothing that happens to an
    /// order could be heard.
    StreamDown,
    /// Some order's outcome is unknown. Loud and stuck until the stream
    /// mentions that order or a lookup settles it (`docs/live-run.md` §5).
    InDoubt,
    /// The outbound queue is full. The answer cannot go stale before the send:
    /// the engine thread is the queue's only producer, so room can only grow.
    QueueFull,
    /// The account and our record no longer describe the same thing: a fill
    /// nobody can attribute to an order, or a stranger's trade on our symbol.
    ///
    /// A latch, not a passing condition. `quant-live`'s merge machine sets it
    /// on the first such report, on every contradiction and on `IdInUse`, and
    /// never clears it in-process: carrying on would trade against a position
    /// nobody knows. Clearing it is a person's act -- reconciling the account
    /// by hand, then starting a new journal under a new run tag. A restart
    /// reads it back with `journal::diverged`, from the lines that latched it.
    Diverged,
}

/// Somewhere an order can be sent.
pub trait ExecutionVenue {
    /// Whether this venue can take `request` now.
    ///
    /// Asked by `Context::submit` after the seam check and before the risk
    /// layer. After the seam, because a malformed request is ours to refuse
    /// whatever the venue's state. Before risk, so an order the venue could
    /// not take spends none of the day's order count and can trip nothing. And
    /// before anything is written down as submitted, which is the reason this
    /// exists: by the time [`Self::submit`] runs the order is journalled and in
    /// the engine's ledger, and the only way back would be a `Rejected`, which
    /// the journal records as the venue declining an order it had been told
    /// about. That is false for an order that never left.
    ///
    /// **Required, not defaulted.** A defaulted method is what a forwarding
    /// wrapper inherits without a word, which is how M7.5.e's `take_notes`
    /// went missing in `Recorded`; here the inherited answer would be "ready"
    /// around a venue that is not, and orders would go out blind. Every venue,
    /// and every wrapper around one, says what it does here, and the compiler
    /// checks that it does.
    ///
    /// `&self`, because asking must change nothing. The request is passed
    /// because some answers are per instrument, such as filters.
    ///
    /// # Errors
    ///
    /// [`Unready`], saying why not. The order is refused, not queued.
    fn ready(&self, request: &OrderRequest) -> Result<(), Unready>;

    /// Send an order. Returns nothing; see the module docs.
    ///
    /// The engine has already minted `client_order_id`, found this venue
    /// [ready](Self::ready) for it, and checked it against the risk layer, so an
    /// implementation's job is to send it and report back — not to second-guess
    /// the seam.
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
    /// A simulated venue matches resting orders against this, in a backtest and
    /// in paper alike. A live venue ignores it — the real market already knows
    /// — which is why the default implementation does nothing and only the
    /// simulator overrides it.
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
    /// Each report is either something that happened to an order, with what
    /// only a real venue knows typed beside it, or a notice: a fact the record
    /// must hold that is not an event at all. The engine tells the strategy an
    /// order report's event and nothing beside it, and tells it nothing of a
    /// notice. What an implementation owes the engine:
    ///
    /// - **Append, and never block.** Appends rather than returning, so a busy
    ///   venue does not allocate a vector per event on the hot path; never
    ///   blocks, because the engine's only other input is waiting behind it.
    /// - **Arrival order, stamped by the clock that stamps `local_recv_ts`.**
    ///   Every report moves the engine clock when it is later, notices
    ///   included, so a stamp from any other clock would date what the engine
    ///   does next by that clock.
    /// - **At most one terminal report per order, and none before its last
    ///   fill.** The engine forgets an order on its terminal report, and a fill
    ///   after that could not be booked. One report may follow it: a venue
    ///   that reports a fill after an order's end hands that fill over all the
    ///   same, beside an anomaly, and the engine, no longer working the order,
    ///   writes it down as `orphaned` and books it nowhere. A contract written
    ///   for a venue that is right must not make the record drop money when
    ///   the venue is wrong.
    /// - **[`VenueReport::Order`] only for this run's orders.** Anything about an
    ///   order this run did not send is a notice.
    /// - **Never drop a report silently.** One that cannot be read becomes an
    ///   anomaly notice, and the order it might concern is in doubt, because
    ///   what it said may have been a fill.
    fn poll(&mut self, now: Ts, out: &mut Vec<VenueReport>);
}
