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
use quant_core::fixed::{Notional, Px};
use quant_core::instrument::InstrumentId;
use quant_core::time::Ts;

use crate::portfolio::{Portfolio, Position};
use crate::risk::RiskLayer;
use crate::venue::ExecutionVenue;
use crate::{seam_check, Ledger};

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
    ledger: &'a mut Ledger,
    portfolio: &'a Portfolio,
    /// Where decisions are written down, if anything is listening.
    ///
    /// Reached through `Context` because submissions and refusals happen here
    /// and nowhere else. This is the one thing in `Context` that touches the
    /// outside world, which is a real widening of the seam -- so it is
    /// deliberately not something a *strategy* can reach: the field is private,
    /// there is no accessor, and `Context`'s public surface is unchanged. A
    /// strategy still cannot tell which pair it is wired to.
    observer: Option<&'a mut (dyn crate::RunObserver + Send + 'static)>,
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
        ledger: &'a mut Ledger,
        portfolio: &'a Portfolio,
        observer: Option<&'a mut (dyn crate::RunObserver + Send + 'static)>,
    ) -> Self {
        Self {
            books,
            now,
            venue,
            risk,
            ledger,
            portfolio,
            observer,
        }
    }

    /// What we hold in an instrument, and what it cost.
    ///
    /// Maintained by the engine rather than by the strategy, for the reason the
    /// book is: every strategy would otherwise derive it, and they would derive
    /// it differently.
    #[must_use]
    pub fn position(&self, instrument: InstrumentId) -> Position {
        self.portfolio.position(instrument)
    }

    /// Uncommitted cash.
    #[must_use]
    pub const fn cash(&self) -> Notional {
        self.portfolio.cash()
    }

    /// Cash plus the position marked at `mark`.
    ///
    /// `None` when there is a position and no price to mark it at — after a gap,
    /// there is no honest number, and a stale one would smooth over exactly the
    /// periods worth looking at.
    #[must_use]
    pub fn equity(&self, instrument: InstrumentId, mark: Option<Px>) -> Option<Notional> {
        self.portfolio.equity(instrument, mark)
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
        let client_order_id = self.ledger.mint();

        if let Some(reason) = seam_check(&request) {
            if let Some(observer) = self.observer.as_mut() {
                // No `bound`: the seam is not a limit. A malformed order did not
                // breach anything, it was never a well-formed order at all.
                observer.on_refused(
                    client_order_id,
                    &request,
                    reason,
                    None,
                    crate::RefusedBy::Seam,
                    self.now,
                );
            }
            self.ledger.refuse(client_order_id, reason, self.now);
            return client_order_id;
        }
        // The mark the risk layer prices against: this instrument's mid, or
        // `None` when the book has no prices. A money limit must refuse rather
        // than guess -- see `RiskLayer::check`.
        let mark = self.book(request.instrument).and_then(|book| {
            let (bid, ask) = (book.best_bid()?, book.best_ask()?);
            Some(Px::from_raw((bid.px.raw() + ask.px.raw()) / 2))
        });
        if let Some(refusal) = self.risk.check(&request, mark, self.now) {
            // Refused here, so the venue never hears about it at all. That is
            // the chokepoint being a chokepoint.
            if let Some(observer) = self.observer.as_mut() {
                observer.on_refused(
                    client_order_id,
                    &request,
                    refusal.reason,
                    refusal.bound,
                    crate::RefusedBy::Risk,
                    self.now,
                );
            }
            // Only `reason` reaches the strategy. Which limit bound is a fact
            // for the record and for a person reading it, not something a
            // strategy may condition on -- it could otherwise trade around the
            // limits, which is the opposite of a chokepoint.
            self.ledger
                .refuse(client_order_id, refusal.reason, self.now);
            return client_order_id;
        }

        // Written down *before* the venue is told, which is the fill rule
        // generalised: an order at a venue that we never recorded is an orphan
        // position, and nothing read afterwards recovers it.
        if let Some(observer) = self.observer.as_mut() {
            observer.on_submitted(client_order_id, &request, mark, self.now);
        }
        self.ledger.stats.submitted += 1;
        // Remembered so the fill can be booked: a fill names only the order.
        self.ledger
            .orders
            .insert(client_order_id, (request.instrument, request.side));
        self.venue.submit(client_order_id, &request, self.now);
        client_order_id
    }

    /// Ask for an order to be cancelled.
    ///
    /// May lose the race with a fill, which is why it reports nothing. Whether
    /// it worked arrives as a `Cancelled` — or does not, because the order
    /// filled first.
    pub fn cancel(&mut self, client_order_id: ClientOrderId) {
        if let Some(observer) = self.observer.as_mut() {
            observer.on_cancel_requested(client_order_id, self.now);
        }
        self.ledger.stats.cancels += 1;
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
