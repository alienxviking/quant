//! What a strategy is, and everything it is allowed to touch.
//!
//! # The seam, stated as a type
//!
//! A [`Strategy`] receives events and a [`Context`]. The context is the entire
//! surface: a read-only book, the engine clock, and a way to submit and cancel.
//! It holds **no venue**, **no event source** and **no wall clock**, which is how
//! `docs/engine-contract.md`'s central property is enforced rather than
//! promised — there is no accessor that could tell a strategy which of the three
//! worlds it is running in, so the same value runs in all of them.
//!
//! The absence of a venue reference is also what makes the risk layer a
//! chokepoint: [`Context::submit`] is the only way out, and it runs the check.

use quant_book::Book;
use quant_core::event::MarketEvent;
use quant_core::execution::{ClientOrderId, ExecutionEvent, OrderRequest};
use quant_core::instrument::InstrumentId;
use quant_core::time::Ts;

use crate::risk::RiskLayer;
use crate::venue::ExecutionVenue;
use crate::{refuse, seam_check, EngineStats};

/// Everything a strategy can reach.
///
/// Deliberately not `Clone` and not storable: it borrows the engine for the
/// duration of one callback. A strategy that could keep it could act between
/// events, which is time the data does not justify.
pub struct Context<'a> {
    books: &'a [Book],
    now: Ts,
    venue: &'a mut dyn ExecutionVenue,
    risk: &'a mut dyn RiskLayer,
    next_id: &'a mut u64,
    deferred: &'a mut Vec<ExecutionEvent>,
    stats: &'a mut EngineStats,
}

impl core::fmt::Debug for Context<'_> {
    /// Hand-written because the venue and risk layer are trait objects that need
    /// not be `Debug` — and because printing them would be printing the two
    /// things a strategy is not allowed to see.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Context")
            .field("now", &self.now)
            .field("books", &self.books.len())
            .finish_non_exhaustive()
    }
}

impl<'a> Context<'a> {
    pub(crate) fn new(
        books: &'a [Book],
        now: Ts,
        venue: &'a mut dyn ExecutionVenue,
        risk: &'a mut dyn RiskLayer,
        next_id: &'a mut u64,
        deferred: &'a mut Vec<ExecutionEvent>,
        stats: &'a mut EngineStats,
    ) -> Self {
        Self {
            books,
            now,
            venue,
            risk,
            next_id,
            deferred,
            stats,
        }
    }

    /// The engine clock: the `local_recv_ts` of the event being handled.
    ///
    /// The only notion of time available. See the engine's module docs for why
    /// there is no other.
    #[must_use]
    pub const fn now(&self) -> Ts {
        self.now
    }

    /// The reconstructed book for an instrument, if one has been built.
    ///
    /// After a gap this is *empty* rather than stale — an invalidated book is
    /// cleared, not flagged — so `book(i).and_then(Book::best_bid)` returns
    /// `None` and "do not trade across a gap" holds without the strategy having
    /// to remember it.
    #[must_use]
    pub fn book(&self, instrument: InstrumentId) -> Option<&Book> {
        self.books.get(instrument.index())
    }

    /// Send an order.
    ///
    /// Returns the id immediately and nothing else. What happens to the order
    /// arrives later at [`Strategy::on_execution`] — including a refusal, which
    /// is delivered the same way every other outcome is rather than returned
    /// here. A caller that got refusals synchronously and fills asynchronously
    /// would have two code paths for one question.
    pub fn submit(&mut self, request: OrderRequest) -> ClientOrderId {
        let client_order_id = ClientOrderId(*self.next_id);
        *self.next_id += 1;

        if let Some(reason) = seam_check(&request) {
            refuse(self.deferred, self.stats, client_order_id, reason, self.now);
            return client_order_id;
        }
        if let Some(reason) = self.risk.check(&request, self.now) {
            // Refused here, so the venue never hears about it at all. That is
            // the chokepoint being a chokepoint.
            refuse(self.deferred, self.stats, client_order_id, reason, self.now);
            return client_order_id;
        }

        self.stats.submitted += 1;
        self.venue.submit(client_order_id, &request, self.now);
        client_order_id
    }

    /// Ask for an order to be cancelled.
    ///
    /// May lose the race with a fill, which is why it reports nothing. Whether
    /// it worked arrives as a `Cancelled` — or does not, because the order
    /// filled first.
    pub fn cancel(&mut self, client_order_id: ClientOrderId) {
        self.stats.cancels += 1;
        self.venue.cancel(client_order_id, self.now);
    }
}

/// The thing being tested.
///
/// Both methods take `&mut self`, so a strategy holds its own state — its
/// outstanding orders, its indicators, its position. That is the cost of
/// fire-and-forget submission, and it is charged identically in all three
/// worlds.
pub trait Strategy {
    /// The market did something.
    ///
    /// Called *after* the venue has matched resting orders against this event,
    /// so an order submitted here cannot trade against it.
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>);

    /// Something happened to one of our orders.
    ///
    /// Defaulted to nothing, because a strategy that only ever sends market
    /// orders and never reconciles is a legitimate first thing to write — and
    /// making it implement an empty method would teach nobody anything.
    fn on_execution(&mut self, event: &ExecutionEvent, ctx: &mut Context<'_>) {
        let _ = (event, ctx);
    }
}
