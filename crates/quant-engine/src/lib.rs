//! The engine seam.
//!
//! One loop, wired to an [`EventSource`] on one side and an [`ExecutionVenue`]
//! on the other, with a [`Strategy`] in the middle and a [`RiskLayer`] on the
//! only path out. `docs/engine-contract.md` argues the shape; this implements
//! it.
//!
//! ```text
//!   EventSource ──► Engine ──► Strategy ──► RiskLayer ──► ExecutionVenue
//!                      │                                        │
//!                      └────────── VenueReport ◄────────────────┘
//! ```
//!
//! What comes back is a [`VenueReport`]: an order report, whose event is all the
//! strategy is told while the record hears the venue's detail beside it, or a
//! notice, which only the record hears. `docs/engine-contract.md` §11.
//!
//! Backtest is `HistoricalSource + SimulatedVenue`. Paper is
//! `LiveSource + SimulatedVenue` (there is no separate paper venue -- M5 found the
//! middle one need not exist). Live is `LiveSource + LiveVenue`. The strategy is
//! the same value in all three, and cannot ask which one it is in.
//!
//! # The order of operations inside one event, and why it is that order
//!
//! This is the part of the engine most able to lie, so it is written down and
//! pinned by a test rather than left to the reading order of a function body.
//! For each event at time `T`:
//!
//! 1. **The clock advances to `T`**, or holds if an input has already carried
//!    it past (counted: [`EngineStats::clock_clamps`]). Only an input moves it
//!    -- this step, or a venue report or notice in step 4 -- and only forward,
//!    so there is no time between inputs for a strategy to act in.
//! 2. **The book is updated.** A `Gap` clears it, which is what makes "no prices
//!    after a disconnect" structural rather than advisory.
//! 3. **The venue sees the event.** Orders resting from *before* `T` match
//!    against it. They were placed on earlier information, so trading against
//!    `T` is exactly what would have happened.
//! 4. **The record hears what the venue said, and then the strategy is told
//!    what happened to its orders**: the reports from step 3 and, live,
//!    whatever the venue's own stream has delivered since the last wake. Each
//!    order report is booked and written down, with the venue's detail, before
//!    the strategy is told its event; a notice is written down and the
//!    strategy is told nothing; a report about an order the engine is not
//!    working is written down and withheld. Anything stamped later than the
//!    clock moves the clock up to it first, so the strategy is never told of
//!    something dated after the time it reads. In a backtest that never
//!    happens: a simulated report is stamped by this clock, so it is never later
//!    than it.
//! 5. **The strategy sees the market event** and may submit.
//! 6. **The strategy is asked what it wrote down**, if this wake called it.
//!
//! A wake on which the market said nothing -- the source idled -- skips steps 1,
//! 2, 3 and 5, and ends before calling anything if the venue said nothing
//! either. If the venue said only notices, the record hears them and no
//! strategy method is called at all. [`Engine::settle`] is such a wake on
//! which the strategy is never called, whatever the venue says.
//!
//! Step 5 comes after step 3, and that is the whole point. An order submitted on
//! seeing the trade at `T` is **not** eligible to match against that trade — it
//! becomes live for the next event. Reversing those two steps lets a strategy
//! react to a print and fill against the same print, which is trading on
//! information at the instant it is created. It is invisible in the output and
//! it inflates every result.
//!
//! # The engine clock is the input stream
//!
//! `now` is the newest stamp the engine has acted on: the `local_recv_ts` of the
//! market event in hand, or a venue report's or notice's stamp when that is
//! later. It only moves forward, and there is no system clock anywhere on this
//! path, in any of the three worlds. A simulated venue stamps its reports with
//! this clock, so in a backtest and in paper `now` is exactly the market event
//! stream, which makes a run deterministic and repeatable. Live, a venue report
//! or notice carries the time its bytes arrived, as a market event does, so
//! `now` is still roughly now, and a strategy that used a finer notion of time
//! would behave differently in the three worlds.
//!
//! That is invariant 4 arriving at its destination: components take a clock, and
//! here the clock is the data.

pub mod journal;
pub mod portfolio;
pub mod risk;
pub mod strategy;
pub mod venue;

use std::collections::HashMap;

use quant_book::Book;
use quant_core::event::{MarketEvent, Side};
use quant_core::execution::{
    ClientOrderId, ExecutionEvent, Fill, OrderRequest, RejectReason, VenueOrderId,
};
use quant_core::fixed::Notional;
use quant_core::fixed::{Px, Qty};
use quant_core::instrument::InstrumentId;
use quant_core::report::{Ended, FillIds, Notice, OrderReport, RejectedWhy, VenueReport};
use quant_core::source::{EventSource, SourceError, Wake};
use quant_core::time::Ts;

pub use journal::{InstrumentKey, Journal, JournalEntry};
pub use portfolio::{Portfolio, Position};
pub use risk::{AllowAll, Bound, Limits, Refusal, RiskEngine, RiskLayer, TripCause};
pub use strategy::{Context, Note, Strategy};
pub use venue::{ExecutionVenue, Unready};

/// What a run did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EngineStats {
    pub events: u64,
    pub gaps: u64,
    /// Orders that passed the risk layer and reached the venue.
    pub submitted: u64,
    /// Orders refused before they reached a venue: by the seam, by the record,
    /// by the venue's readiness, or by the risk layer. One count, because each
    /// is an order the strategy asked for and the venue never received;
    /// `RefusedBy` on each refusal says which.
    pub refused: u64,
    pub cancels: u64,
    /// Order reports the engine handled, plus refusals delivered to the
    /// strategy. Notices are not among them; see [`Self::notices`].
    pub execution_events: u64,
    pub fills: u64,
    /// Notices handled: facts a venue reported that are not events, which went
    /// to the record and never to the strategy.
    pub notices: u64,
    /// Order reports about an order the engine is not working -- one it never
    /// minted, or one it has already seen end -- written down and kept from
    /// the strategy. Strays and orphaned fills together; always zero against
    /// the simulator, which reports only on orders it was given and nothing
    /// after an order's end.
    pub withheld: u64,
    /// The clock's first and last readings, for reporting the period actually
    /// covered.
    ///
    /// Market event times in a backtest and in paper. Live, a venue report
    /// stamped later than the market can set either end, and `last_ts` has to
    /// follow it: the binaries stamp the closing checkpoint and `Stopped` from
    /// it, which would otherwise land before a fill already journalled.
    pub first_ts: Option<Ts>,
    pub last_ts: Option<Ts>,
    /// Market events stamped before the clock, which the engine refused to
    /// move it back for.
    ///
    /// Two asynchronous inputs can be acted on out of the order they arrived.
    /// A venue report stamped later than the market event in hand carries the
    /// clock forward, and a market event received before that report, still
    /// waiting in the source's queue, then arrives stamped in the past. The
    /// clock must not run back, because everything downstream dispatches on
    /// it. Counted rather than silently clamped: `quant-recorder::segment`'s
    /// `backdated_records` arriving at the engine for the same reason, since a
    /// clamp is evidence about the transport and one nobody counts is one
    /// nobody notices rising.
    ///
    /// **Only a market event counts.** A venue report moves the clock when it
    /// is stamped later and otherwise leaves it alone, on either kind of wake:
    /// one stamped at or before the clock asks nothing of it. That is every
    /// fill a simulator delivers late, and live, any report received before a
    /// market event the engine has already acted on. Counting those would make
    /// this number depend on whether a market wake or an idle one happened to
    /// deliver the report, rather than on the order the inputs arrived in.
    ///
    /// **Always zero in a backtest.** `HistoricalSource` is monotone, and a
    /// simulated report is never stamped after the clock, so nothing carries
    /// the clock past the next market event; a non-zero value from a replay
    /// means a source broke its own contract.
    pub clock_clamps: u64,
    /// Times the engine woke on nothing and went to look for venue reports.
    pub idle_wakes: u64,
}

/// The engine's mutable bookkeeping.
///
/// Grouped so a [`Context`] can borrow it as one thing. Four separate `&mut`
/// parameters is the same borrow with more places to get the order wrong, and
/// the compiler was starting to say so.
#[derive(Debug)]
pub(crate) struct Ledger {
    /// Next client order id. Starts at 1, so a zero id is always a bug rather
    /// than a legitimate first order.
    next_id: u64,
    /// Execution events raised during a strategy callback — a refusal by the
    /// seam, the record, the venue's readiness or the risk layer — waiting to be
    /// delivered.
    ///
    /// Deferred rather than delivered inline because a strategy must not be
    /// re-entered while it is on the stack, and because even a rejection should
    /// arrive the way every other outcome does: as an event, afterwards.
    deferred: Vec<ExecutionEvent>,
    /// Which instrument and side each live order belongs to.
    ///
    /// A `Fill` names only the order, so this is what lets a fill be booked at
    /// all. The engine has to hold it in every world — a live venue's fill report
    /// names the order too — which is why the portfolio lives here and not in the
    /// simulator. Entries are removed on a terminal event, so this is bounded by
    /// the strategy's working orders rather than by the length of the run.
    orders: HashMap<ClientOrderId, (InstrumentId, Side)>,
    /// Set the first time the record answers [`WriteAhead::Withhold`], and
    /// never cleared in-process.
    ///
    /// From then on every order is refused as [`RefusedBy::Record`] before the
    /// venue is even asked. A record that has failed to hold one line cannot be
    /// trusted to hold the next, and an order sent that the record does not
    /// hold is an orphan position at a live venue. A latch rather than a retry,
    /// because what follows a torn line is not a question this process can
    /// answer by asking again.
    unrecorded: bool,
    stats: EngineStats,
}

impl Default for Ledger {
    fn default() -> Self {
        Self {
            next_id: 1,
            deferred: Vec::new(),
            orders: HashMap::new(),
            unrecorded: false,
            stats: EngineStats::default(),
        }
    }
}

/// Told about every fill, so something outside the engine can write it down.
///
/// A callback rather than the engine owning a journal, for the reason M1.c2
/// gave when the recorder needed to reach Postgres: `quant-recorder` stayed
/// DB-free via a plain on-seal callback so the arrows only pointed down. Same
/// here — this crate knows no file format and no database, and a paper binary
/// wires the journal in.
///
/// The instrument and side come from the engine because a [`Fill`] does not
/// carry them: a fill belongs to an order, and the engine is what knows which.
///
/// `portfolio` is the state *after* the fill, so an observer that writes a
/// checkpoint records what the engine actually believes rather than a total it
/// accumulated itself. A third independent tally would not be a check against
/// the engine — it would be a check against itself.
pub trait RunObserver {
    /// An order was minted and is about to reach the venue — if the record says
    /// it may.
    ///
    /// Called **before** `ExecutionVenue::submit`, which is the same rule fills
    /// already follow for the same reason: at M8 an order at a live venue that
    /// we never wrote down is an orphan position, and no amount of reading
    /// afterwards recovers it. The cost is one durable append inside the
    /// strategy's own call stack, which is charged identically in all three
    /// worlds because production charges it anyway.
    ///
    /// The answer is what makes the rule enforced rather than hoped for. On
    /// [`WriteAhead::Withhold`] the engine does not send this order, refuses it
    /// as [`RefusedBy::Record`], and refuses every later one the same way for
    /// the rest of the process. This order has already passed the risk layer
    /// and spent one of the day's count, which can happen once per process.
    /// Defaulted to `Proceed`, so an observer that writes nothing durable need
    /// not say so.
    ///
    /// `mark` is the mid the risk layer priced this request against, or `None`
    /// when the book had no prices. The engine computes it and drops it today;
    /// it is the one input to a refusal decision that cannot be re-derived,
    /// because it depends on the book *as the engine saw it*.
    fn on_submitted(
        &mut self,
        _client_order_id: ClientOrderId,
        _request: &OrderRequest,
        _mark: Option<Px>,
        _at: Ts,
    ) -> WriteAhead {
        WriteAhead::Proceed
    }

    /// An order was refused before the venue heard about it.
    ///
    /// `by` separates "we built a malformed order" from "we could not write it
    /// down" from "the venue could not take an order now" from "we built a fine
    /// order the limits declined". Collapsing them would bury a bug of ours, a
    /// failing disk or a venue that is down in a counter of events that are
    /// working as designed.
    ///
    /// `bound` names which limit stopped it, and is `None` for every refusal
    /// but the risk layer's — the seam, the record and the venue's readiness
    /// are not limits, and an order they turn away breached nothing. It
    /// deliberately does not reach the strategy: `RejectReason` stays two values
    /// wide, because a strategy that could tell "too large" from "the daily loss
    /// budget is spent" could trade around the limits.
    fn on_refused(
        &mut self,
        _client_order_id: ClientOrderId,
        _request: &OrderRequest,
        _reason: RejectReason,
        _bound: Option<crate::risk::Bound>,
        _by: RefusedBy,
        _at: Ts,
    ) {
    }

    /// The venue acknowledged an order.
    ///
    /// `venue_order_id` is the venue's name for it, when the venue gave one: a
    /// fact for the record, which the strategy is never told.
    fn on_accepted(
        &mut self,
        _client_order_id: ClientOrderId,
        _venue_order_id: Option<&VenueOrderId>,
        _at: Ts,
    ) {
    }

    /// The venue declined an order it had been told about, or the request
    /// never reached it.
    ///
    /// `why` is what the venue cited, or why the request never landed, when the
    /// venue knows: the strategy hears `reason` and nothing more.
    fn on_rejected(
        &mut self,
        _client_order_id: ClientOrderId,
        _reason: RejectReason,
        _why: Option<&RejectedWhy>,
        _at: Ts,
    ) {
    }

    /// A cancel was sent. Whether it won the race is a separate call.
    fn on_cancel_requested(&mut self, _client_order_id: ClientOrderId, _at: Ts) {}

    /// An order was withdrawn, with whatever never traded.
    ///
    /// `ended` names the order in the venue's words and says how the venue
    /// ended it, when it says: cancelled, expired or stopped by self-trade
    /// prevention are one outcome to the strategy and three to a person
    /// reconciling against the venue.
    fn on_cancelled(
        &mut self,
        _client_order_id: ClientOrderId,
        _remaining: Qty,
        _ended: Option<&Ended>,
        _at: Ts,
    ) {
    }

    /// The kill switch was thrown, at the instant it was thrown.
    ///
    /// Written here rather than at shutdown, which is where M5 put it and is
    /// the hole `CLAUDE.md` has carried as *must be fixed before M8* ever
    /// since. Two things were wrong with the old placement and only one of
    /// them was ever written down: a hard kill between the trip and the
    /// shutdown lost the trip entirely, so the supervisor re-armed a switch
    /// that had fired — but even on a clean exit the entry was stamped with
    /// the *shutdown* time, so the record of the single most serious thing the
    /// risk layer can do named the wrong moment, by however long the session
    /// happened to continue.
    ///
    /// This is the write-ahead rule the journal already applies to fills and
    /// submissions, arriving at the third place that needs it.
    fn on_tripped(&mut self, _cause: crate::risk::TripCause, _at: Ts) {}

    /// We stopped being able to see the market.
    ///
    /// `last_good_ts` is the last instant the stream was known intact, so the
    /// pair bounds the blindness. The engine has always *counted* gaps in
    /// `stats.gaps`; this is the first time one is written down with a time
    /// attached, which is the difference between knowing there were 34 and
    /// knowing whether we were blind at 03:30.
    fn on_blind(&mut self, _cause: quant_core::event::GapCause, _last_good_ts: Ts, _at: Ts) {}

    /// The strategy wrote something down.
    ///
    /// `at` is the engine's clock, not the strategy's: the strategy accumulates
    /// notes during its own callbacks and the engine takes them at the end of
    /// the same event, so the two instants are identical by construction — and
    /// stamping here rather than there means a strategy cannot date its own
    /// entries, which is invariant 4 holding at one more seam.
    fn on_note(&mut self, _note: &crate::strategy::Note, _at: Ts) {}

    /// A fill arrived for an order this engine is not working: one it never
    /// minted, or one whose end it has already heard.
    ///
    /// Counted in `stats.fills`, because something did trade, and in
    /// `stats.withheld`; not booked into the portfolio, because the engine does
    /// not know whose it was; and not told to the strategy, which would
    /// otherwise act on a fill its own position does not contain. This hook is
    /// its only record. Impossible with `SimulatedVenue`, which only fills what
    /// it was given; **not** impossible at M8, where a venue can report a fill
    /// for an order we lost track of across a restart. That is exactly the
    /// case where silence is most expensive.
    ///
    /// `ids` are the venue's names for the order and the trade, when it gave
    /// them: for a fill nobody can attribute, the only handle a person has on
    /// the venue's own record of it.
    fn on_orphaned(
        &mut self,
        _client_order_id: ClientOrderId,
        _fill: &Fill,
        _ids: Option<&FillIds>,
        _at: Ts,
    ) {
    }

    /// Something traded, and it was ours.
    ///
    /// `client_order_id` is **the engine's own id**, not a count of fills. It
    /// was absent until M7.5, so the only writer invented
    /// `ClientOrderId(self.fills)` — a fill ordinal, correct exactly while
    /// nothing is ever refused, because `Ledger::mint` hands out ids densely and
    /// a refused order consumes one *before* the risk check.
    ///
    /// One [`Booked`] rather than its fields one by one, because they are seven
    /// and an eighth input to one method is where clippy draws the line.
    fn on_fill(&mut self, _booked: &Booked<'_>, _portfolio: &Portfolio) {}

    /// A report about an order the engine is not working: one it never minted,
    /// or one whose end it has already heard. Not a fill, which goes to
    /// [`Self::on_orphaned`].
    ///
    /// Written down and kept from the strategy. A strategy told of an order it
    /// never placed would act on it, and one told that an order it has already
    /// seen end was accepted or cancelled again would believe it. **Required**,
    /// for the reason [`Self::on_notice`] is: a default would make these
    /// silent.
    fn on_stray(&mut self, report: &OrderReport, at: Ts);

    /// The venue reported a fact that is not an event: an order's REST
    /// acknowledgement, an outcome that became unknown or known again, a
    /// cancel it did not carry out, a report about an order this run did not
    /// send, the account stream lost or regained, a report it could not read.
    ///
    /// The only path these facts have, which is why it is **required**. A
    /// defaulted hook here is the built-and-not-wired defect this project keeps
    /// meeting (M4's cost flags, M6's `FillObserver`, M7.5.e's `take_notes` in
    /// `Recorded`): an observer that did not write it would
    /// swallow every one of them, with no error anywhere and a record that
    /// merely looked like a quiet venue. The strategy is told none of them.
    ///
    /// `at` is when the notice was heard, which is also where it put the
    /// engine clock if it was the latest thing on its wake.
    fn on_notice(&mut self, notice: &Notice, at: Ts);
}

/// A fill as the engine booked it: the order, what the engine knows about it,
/// the fill, and the venue's names for it.
#[derive(Debug, Clone, Copy)]
pub struct Booked<'a> {
    /// **The engine's own id**, not a count of fills.
    pub client_order_id: ClientOrderId,
    /// From the engine's ledger: a fill names only its order.
    pub instrument: InstrumentId,
    pub side: Side,
    pub fill: &'a Fill,
    /// What of the order is still working after this fill.
    pub remaining: Qty,
    /// The venue's names for the order and the trade, when it gave them. For
    /// the record only; the strategy is never told.
    pub ids: Option<&'a FillIds>,
    pub at: Ts,
}

/// Which layer refused an order, listed in the order `Context::submit` asks
/// them.
///
/// Serialized inside `refused` journal lines, as `"seam"`, `"record"`,
/// `{"venue":"stream_down"}` and `"risk"`. A variant that carries a value
/// serializes as an object of one key, so every line written before it existed
/// still reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusedBy {
    /// The seam: the request was not well-formed enough to send anywhere. Ours.
    Seam,
    /// The record could not hold a submission, this one or an earlier one.
    /// Once an observer has answered [`WriteAhead::Withhold`], nothing more is
    /// sent for the rest of the process. Ours: the journal could not be
    /// written, for whatever reason the observer met.
    Record,
    /// The venue said it could not take an order now, and why. Neither our
    /// mistake nor a limit: a stream that is down, an order in doubt, a check
    /// not passed. The strategy hears only `RejectReason::Venue`.
    Venue(Unready),
    /// The risk layer: a fine request the limits declined. Working as designed.
    Risk,
}

/// The record's answer to a submission it has just been told about.
///
/// A decision, not a durability claim. `Proceed` says "send it", and a lenient
/// observer answers it even when its write failed, because it has decided that
/// a run with a hole in its record beats no run — `quant-backtest`'s
/// `JournalWriter` does exactly that, for paper. `Withhold` says the order must
/// not reach the venue, because the record could not hold it.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteAhead {
    /// Send it.
    Proceed,
    /// Do not send it, nor anything after it in this process.
    Withhold,
}

/// How long the engine waits on the market before going to look at the venue.
///
/// Reached only when a source overrides `next_event_timeout`; a historical
/// replay never idles, so no backtest number moves. 250 ms is chosen against
/// what it costs on each side: a fill learned a quarter-second late is nothing
/// to a strategy sampling on a 60-second grid, and four empty polls a second is
/// nothing against a market peak of 694 messages a second.
///
/// Public because the live binary paces its [`Engine::settle`] loop on it, so
/// that a drain at shutdown asks the venue no more often than a quiet market
/// does.
pub const IDLE_TICK: core::time::Duration = core::time::Duration::from_millis(250);

/// What one [`Engine::settle`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settled {
    /// Reports the venue handed over on this call, notices included. Zero
    /// means it had nothing to say yet, which is the binary's cue to wait.
    pub delivered: usize,
    /// Orders the engine is still working after it, lowest id first. Empty
    /// means there is nothing left to wait for.
    pub working: Vec<ClientOrderId>,
}

/// Who hears what a wake brings.
///
/// Private: the only wake that is not the strategy's is [`Engine::settle`]'s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Audience {
    /// The ordinary wake: the record hears everything, and the strategy is
    /// told what happened to its orders and shown the market.
    Strategy,
    /// After the run: the record hears everything, and the strategy nothing.
    RecordOnly,
}

/// The loop.
pub struct Engine<S, V, R, K> {
    source: S,
    venue: V,
    risk: R,
    strategy: K,
    /// One book per instrument, indexed by [`InstrumentId::index`], which is a
    /// dense registry index. Grown on demand rather than pre-sized, so a run
    /// that touches one instrument pays for one book.
    books: Vec<Book>,
    now: Ts,
    /// What the venue said on this wake, reused so a wake allocates nothing.
    reports: Vec<VenueReport>,
    /// Reused across events so the common case -- a strategy with nothing to
    /// say -- allocates nothing. Same reason `reports` exists.
    notes: Vec<crate::strategy::Note>,
    ledger: Ledger,
    portfolio: Portfolio,
    /// Optional, because a backtest need not journal: it can be re-run from raw, and a two-week paper session cannot.
    observer: Option<Box<dyn RunObserver + Send>>,
}

impl<S: core::fmt::Debug, V: core::fmt::Debug, R: core::fmt::Debug, K: core::fmt::Debug>
    core::fmt::Debug for Engine<S, V, R, K>
{
    /// Hand-written because the fill observer is a trait object that need not be
    /// `Debug` -- it is a callback into a file, and printing it would say
    /// nothing anyone wants.
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Engine")
            .field("source", &self.source)
            .field("venue", &self.venue)
            .field("risk", &self.risk)
            .field("strategy", &self.strategy)
            .field("now", &self.now)
            .field("portfolio", &self.portfolio)
            .finish_non_exhaustive()
    }
}

impl<S, V, R, K> Engine<S, V, R, K>
where
    S: EventSource,
    V: ExecutionVenue,
    R: RiskLayer,
    K: Strategy,
{
    /// Wire one up with `starting_cash`.
    ///
    /// Capital is a constructor argument and not a default, because a run
    /// without it is meaningless and a default of zero would silently produce an
    /// equity curve of zero that looks like a flat strategy.
    pub fn new(source: S, venue: V, risk: R, strategy: K, starting_cash: Notional) -> Self {
        Self {
            source,
            venue,
            risk,
            strategy,
            books: Vec::new(),
            now: Ts::from_nanos(0),
            reports: Vec::new(),
            notes: Vec::new(),
            ledger: Ledger::default(),
            portfolio: Portfolio::new(starting_cash),
            observer: None,
        }
    }

    /// Have every fill reported to `observer` as it is booked.
    ///
    /// Called *after* the portfolio applies the fill and *before* the strategy
    /// is told, so a journal entry exists before anything can act on the fill.
    /// The other order would allow a strategy to submit on a fill that a crash
    /// then erased from the record.
    #[must_use]
    pub fn observing_fills(mut self, observer: Box<dyn RunObserver + Send>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Start from a position already established, recovered from a journal.
    ///
    /// A restart on day nine of a two-week run has to come back up holding what
    /// it held, or the strategy's first act is to trade against a position it
    /// does not know it has.
    #[must_use]
    pub fn resuming(mut self, portfolio: Portfolio) -> Self {
        self.portfolio = portfolio;
        self
    }

    /// Continue minting client order ids from `next`, after a restart.
    ///
    /// `Ledger::mint` hands out ids densely from 1, and `next_id` resets to 1
    /// with the process. [`Self::resuming`] replaces only the portfolio, so a
    /// resumed run would mint id 1 again and the journal would hold two
    /// different orders claiming it.
    ///
    /// That is not cosmetic. M7.5's first acceptance criterion is that a **hole
    /// in the id sequence is a decision that was not written down** — a property
    /// that only works if the ids are dense and unique across the whole file.
    /// Duplicates would make the check report holes where there are none, which
    /// is the "cries wolf on good data" failure M1's verifier already taught
    /// this project once.
    ///
    /// Recovered as `max(id) + 1` over the journal rather than stored as a
    /// counter: the ids themselves are the record, and a separate counter would
    /// be a second thing that must agree with them forever.
    #[must_use]
    pub const fn minting_from(mut self, next: u64) -> Self {
        self.ledger.next_id = next;
        self
    }

    /// Run to the end of the source.
    ///
    /// Wakes on whichever input speaks: a market event, or -- after a quarter
    /// of a second with nothing from the market, for a source that can say so
    /// -- whatever the venue has to report. A source that never idles, which is
    /// every historical one, makes this the plain event loop it always was.
    ///
    /// A source error stops the run and is returned, rather than being treated
    /// as the end of the stream — see [`EventSource`] for why that distinction
    /// is the difference between a short backtest and a wrong one.
    pub fn run(&mut self) -> Result<EngineStats, SourceError> {
        while let Some(item) = self.source.next_event_timeout(IDLE_TICK) {
            match item? {
                Wake::Event(event) => self.dispatch(Some(&event), Audience::Strategy),
                // Nothing from the market -- but the venue may still have
                // spoken, and before M8.a nothing could ask it. `poll` was
                // reachable only from inside a market event's handling, so a
                // fill arriving while the market socket was stalled sat
                // undelivered until data resumed.
                Wake::Idle => {
                    self.ledger.stats.idle_wakes = self.ledger.stats.idle_wakes.saturating_add(1);
                    self.dispatch(None, Audience::Strategy);
                }
            }
        }
        Ok(self.ledger.stats)
    }

    /// After [`Self::run`]: one wake on which only the venue speaks. Whatever
    /// it hands over is booked, written down and tallied by the risk layer,
    /// exactly as on any wake, and **no strategy method is called**.
    ///
    /// A live run's last orders can still be working when the market source
    /// ends, and their fills are real money that the record must hold before
    /// the closing checkpoint is written. But the strategy is not told: the
    /// market it trades on has ended, a backtest never delivers a report still
    /// in flight at the end of its data, and a strategy that could act during
    /// the drain would leave decisions in the journal that no replay has.
    ///
    /// One wake per call, with no clock read and no waiting, so a test drives
    /// it deterministically. The live binary loops on it with a deadline of
    /// its own -- longer than the order client's timeout, which is above the
    /// venue's ten seconds -- stopping when nothing is working, and pausing
    /// [`IDLE_TICK`] when a call delivered nothing. Neither backtest nor paper
    /// calls it; against the simulator it would poll at a clock nothing has
    /// moved, and a report still in flight would stay undelivered, as M4
    /// decided it should.
    pub fn settle(&mut self) -> Settled {
        self.dispatch(None, Audience::RecordOnly);
        Settled {
            delivered: self.reports.len(),
            working: self.working(),
        }
    }

    /// Orders the engine is still working, lowest id first: sent, and not yet
    /// heard to have ended.
    #[must_use]
    pub fn working(&self) -> Vec<ClientOrderId> {
        let mut working: Vec<ClientOrderId> = self.ledger.orders.keys().copied().collect();
        working.sort_unstable();
        working
    }

    /// Move the clock forward, never back.
    ///
    /// Until M8 the engine had one input and this was an assignment. With two,
    /// it can act on them out of the order they arrived -- see
    /// [`EngineStats::clock_clamps`] for how -- and every dispatch decision
    /// downstream reads this clock, so letting it run backwards would date what
    /// the engine does next before what it has already done and written down.
    ///
    /// Clamp and count, which is the answer `quant-recorder::segment` gave when
    /// an NTP step put a record before the open segment's day. Only a market
    /// event's stamp can be clamped here: step 1 passes every one, and
    /// `dispatch` passes a venue report's only when it is later than the
    /// clock, on either kind of wake -- so a report never counts a clamp.
    fn advance_to(&mut self, ts: Ts) {
        if ts < self.now {
            self.ledger.stats.clock_clamps = self.ledger.stats.clock_clamps.saturating_add(1);
            return;
        }
        self.now = ts;
    }

    /// Steps 1 to 3: the clock, the book, and the venue seeing the event.
    ///
    /// Market-only by construction, which is why it is a separate function: a
    /// wake from the venue has no event to take a timestamp from, no book to
    /// update and nothing to show the venue that it did not just say itself.
    fn observe_market(&mut self, event: &MarketEvent) {
        // 1. The clock: the event's own stamp, or no move at all if the clock
        //    is already past it.
        self.advance_to(event.meta().local_recv_ts);
        self.ledger.stats.events += 1;
        self.ledger.stats.first_ts.get_or_insert(self.now);
        self.ledger.stats.last_ts = Some(self.now);
        if let MarketEvent::Gap(gap) = event {
            self.ledger.stats.gaps += 1;
            // Before the book is cleared, which is the next step. The order is
            // not load-bearing for correctness -- nothing reads the book here --
            // but it keeps the file's story in the order it happened.
            if let Some(observer) = self.observer.as_deref_mut() {
                observer.on_blind(gap.cause, gap.last_good_ts, self.now);
            }
        }

        // 2. The book. A gap clears it, so there are no stale prices to read.
        let index = event.meta().instrument.index();
        if self.books.len() <= index {
            self.books.resize_with(index + 1, Book::new);
        }
        self.books[index].apply(event);

        // 3. The venue matches orders that were already resting.
        self.venue.observe(event, &self.books[index], self.now);
    }

    /// Everything that happens because of one wake, in the order of the module
    /// docs.
    ///
    /// `None` means the market did not speak: the source idled, and the venue may
    /// have. Steps 1, 2, 3 and 5 are then skipped — there is no event to take a
    /// time from, no book to update, nothing to show the venue, and no market
    /// event to tell the strategy about. Calling `on_market_event` there would
    /// hand a strategy an event that did not happen. If the venue has nothing
    /// either, the wake ends before anything is called at all.
    ///
    /// One function rather than two, because the step order is the most
    /// load-bearing thing in this engine and splitting it would put half of it
    /// somewhere a reader has to go and find.
    ///
    /// `audience` says whether the strategy is part of this wake. Under
    /// [`Audience::RecordOnly`] everything is booked and written down exactly
    /// as it would be otherwise, and no strategy method is called.
    fn dispatch(&mut self, event: Option<&MarketEvent>, audience: Audience) {
        macro_rules! ctx {
            () => {
                Context::new(
                    &self.books,
                    self.now,
                    &mut self.venue,
                    &mut self.risk,
                    &mut self.ledger,
                    &self.portfolio,
                    // Disjoint from the five borrows above, so this compiles
                    // without restructuring the loop. `Context::new` is
                    // `pub(crate)`, so no external signature moves.
                    self.observer.as_deref_mut(),
                )
            };
        }

        if let Some(event) = event {
            self.observe_market(event);
        }

        // 4. What the venue has to report: the fills step 3 produced and, live,
        //    whatever its own stream delivered since the last wake.
        //
        //    The newest stamp among them all, notices included. A notice is a
        //    venue input the engine acts on -- it is journalled at its stamp, and
        //    live it changes whether the venue is ready -- so it moves the clock
        //    the way any report does, and the record stays in time order without
        //    holding anything back.
        self.reports.clear();
        self.venue.poll(self.now, &mut self.reports);
        let latest = self.reports.iter().map(VenueReport::ts).max();
        if event.is_some() {
            // The event set the clock in step 1, but a live venue's reports come
            // down another socket, and one can have been received after the
            // event in hand. Left at the event's instant, the clock would read
            // earlier than a fill the strategy is about to be told of: the fill
            // is journalled at its own instant, and whatever the strategy then
            // decides -- submissions, notes -- would be journalled before it. So
            // the clock catches up to the newest report first, before anything
            // below reads it.
            //
            // Only when the report is later. One stamped at or before the clock
            // is the ordinary case -- every fill a simulator delivers, stamped
            // when it was made and delivered at or after that -- and asks
            // nothing of the clock, so it must not count a clamp. That is what
            // keeps every backtest number and stat where it was.
            if let Some(latest) = latest.filter(|&ts| ts > self.now) {
                self.advance_to(latest);
                // The period covered runs to the report. The binaries stamp the
                // closing checkpoint from this, which would otherwise land
                // before the fill.
                self.ledger.stats.last_ts = Some(self.now);
            }
        } else {
            let Some(latest) = latest else {
                // Woke on nothing and the venue had nothing to say, so nothing
                // below this line has anything to act on.
                //
                // It is not only an optimisation, which is what the first
                // version of this comment claimed and a sabotage disproved: the
                // engine's clock starts at zero, so an idle wake arriving before
                // the first market event would set `first_ts` from it and the
                // run would report a period beginning in 1970. A live source
                // idles before its first frame as a matter of course.
                return;
            };
            // The clock comes from the reports, because there is no event to
            // take it from. Taking the latest rather than the first is what lets
            // a fill be booked at the instant the venue acted.
            //
            // Only when it is later, exactly as on a market wake. A report
            // stamped at or before the clock -- received before a market event
            // the engine has already acted on -- asks nothing of it, and must
            // not count a clamp here either: whether a report counted one would
            // otherwise depend on which kind of wake happened to deliver it.
            if latest > self.now {
                self.advance_to(latest);
            }
            self.ledger.stats.first_ts.get_or_insert(self.now);
            self.ledger.stats.last_ts = Some(self.now);
        }

        //    Then book what happened, and tell the strategy. Booking first means
        //    a strategy reading its own position during `on_execution` sees the
        //    fill it is being told about, rather than the state before it.
        //
        //    A notice goes to the record alone. An order report's detail goes to
        //    the record beside it, and the strategy is told the report's event,
        //    which is where venue detail stops.
        let mut told = false;
        for i in 0..self.reports.len() {
            let report = match &self.reports[i] {
                VenueReport::Notice(notice) => {
                    self.ledger.stats.notices += 1;
                    if let Some(observer) = self.observer.as_deref_mut() {
                        observer.on_notice(notice, notice.ts());
                    }
                    continue;
                }
                VenueReport::Order(report) => report.clone(),
            };
            self.ledger.stats.execution_events += 1;
            // Only an order the engine is working reaches the strategy. A
            // report about any other is written down and goes no further:
            // nothing is booked, because the engine does not know what the
            // order was, and the strategy is not told, because it would be
            // told of an order it never placed, or of one it has already seen
            // end.
            let Some(&(instrument, side)) = self.ledger.orders.get(&report.client_order_id())
            else {
                self.withhold(&report);
                continue;
            };
            self.book_report(&report, instrument, side);
            let execution = report.event();
            if execution.is_terminal() {
                self.ledger.orders.remove(&execution.client_order_id());
            }
            if audience == Audience::RecordOnly {
                continue;
            }
            let mut ctx = ctx!();
            self.strategy.on_execution(&execution, &mut ctx);
            told = true;
            // Rejections raised inside that callback, delivered before the next
            // one. Drained rather than iterated once, because a strategy may
            // submit from `on_execution` and be refused again.
            while let Some(refusal) = self.ledger.deferred.pop() {
                self.ledger.stats.execution_events += 1;
                let mut ctx = ctx!();
                self.strategy.on_execution(&refusal, &mut ctx);
            }
        }

        // 5. Only now does the strategy see the market event. An order it
        //    submits here is not eligible to trade against this event. Skipped
        //    entirely when the venue woke us: there is no market event, and
        //    inventing one would be the lookahead this ordering exists to stop.
        if let Some(event) = event {
            let mut ctx = ctx!();
            self.strategy.on_market_event(event, &mut ctx);
            told = true;
            while let Some(refusal) = self.ledger.deferred.pop() {
                self.ledger.stats.execution_events += 1;
                let mut ctx = ctx!();
                self.strategy.on_execution(&refusal, &mut ctx);
            }
        }

        // 6. Ask the strategy what it wrote down, once the event is fully dealt
        //    with. Last on purpose: a note taken between steps 4 and 5 would
        //    miss everything `on_market_event` decided, which is where a
        //    crossover strategy does all of its deciding. Taken even when the
        //    strategy is silent, because `take_notes` is how it says so.
        //
        //    Only on a wake that called the strategy. A wake that brought
        //    nothing but notices called no strategy method, and must not call
        //    this one either: a strategy that counted its `take_notes` calls
        //    could otherwise count the wakes on which the venue said something
        //    it was not told, and tell a talkative venue from a quiet one.
        if !told {
            return;
        }
        self.notes.clear();
        self.strategy.take_notes(&mut self.notes);
        if let Some(observer) = self.observer.as_deref_mut() {
            for note in &self.notes {
                observer.on_note(note, self.now);
            }
        }
    }

    /// Book one report about an order the engine is working, and tell the
    /// record, before the strategy hears of it.
    ///
    /// A fill is booked into the portfolio, written down and shown to the risk
    /// layer, in that order; the rest of the lifecycle is written down. The
    /// venue's detail goes to the record here and nowhere else.
    fn book_report(&mut self, report: &OrderReport, instrument: InstrumentId, side: Side) {
        let client_order_id = report.client_order_id();
        match report {
            OrderReport::Filled {
                client_order_id: _,
                fill,
                remaining,
                ids,
                ts,
            } => {
                self.ledger.stats.fills += 1;
                self.portfolio.apply_fill(instrument, side, fill);
                // Written down *before* the strategy hears about it, so a crash
                // cannot erase a fill the strategy has already acted on.
                if let Some(observer) = self.observer.as_mut() {
                    observer.on_fill(
                        &Booked {
                            client_order_id,
                            instrument,
                            side,
                            fill,
                            remaining: *remaining,
                            ids: ids.as_ref(),
                            at: *ts,
                        },
                        &self.portfolio,
                    );
                }
                // Risk keeps its own tally, so it has to see fills too. It
                // deliberately does not read the portfolio: a limit computed
                // from the accounting can only be as correct as the accounting,
                // and would fail in the same direction.
                self.risk.on_fill(instrument, side, fill, *ts);
            }
            OrderReport::Accepted {
                client_order_id: _,
                venue_order_id,
                ts,
            } => {
                if let Some(observer) = self.observer.as_mut() {
                    observer.on_accepted(client_order_id, venue_order_id.as_ref(), *ts);
                }
            }
            OrderReport::Rejected {
                client_order_id: _,
                reason,
                why,
                ts,
            } => {
                if let Some(observer) = self.observer.as_mut() {
                    observer.on_rejected(client_order_id, *reason, why.as_ref(), *ts);
                }
            }
            OrderReport::Cancelled {
                client_order_id: _,
                remaining,
                ended,
                ts,
            } => {
                if let Some(observer) = self.observer.as_mut() {
                    observer.on_cancelled(client_order_id, *remaining, ended.as_ref(), *ts);
                }
            }
        }
    }

    /// Write down a report about an order the engine is not working, and
    /// nothing else.
    ///
    /// A fill is an orphan: counted as a fill, because something did trade,
    /// and not booked, because the engine does not know whose it was.
    /// Impossible with `SimulatedVenue`, which only reports on what it was
    /// given and says nothing after an order's end; not impossible at M8, where
    /// a venue can report on an order lost across a restart, which is exactly
    /// when silence costs most. Anything else is a stray.
    fn withhold(&mut self, report: &OrderReport) {
        self.ledger.stats.withheld += 1;
        if let OrderReport::Filled { .. } = report {
            self.ledger.stats.fills += 1;
        }
        let Some(observer) = self.observer.as_mut() else {
            return;
        };
        match report {
            OrderReport::Filled {
                client_order_id,
                fill,
                remaining: _,
                ids,
                ts,
            } => observer.on_orphaned(*client_order_id, fill, ids.as_ref(), *ts),
            OrderReport::Accepted { ts, .. }
            | OrderReport::Rejected { ts, .. }
            | OrderReport::Cancelled { ts, .. } => observer.on_stray(report, *ts),
        }
    }

    /// The strategy, after the run — for reading whatever it recorded.
    pub const fn strategy(&self) -> &K {
        &self.strategy
    }

    /// The venue, after the run.
    pub const fn venue(&self) -> &V {
        &self.venue
    }

    #[must_use]
    pub const fn stats(&self) -> EngineStats {
        self.ledger.stats
    }

    /// Cash, holdings and P&L after the run.
    #[must_use]
    pub const fn portfolio(&self) -> &Portfolio {
        &self.portfolio
    }

    /// The risk layer, after the run — for reading whether it tripped.
    #[must_use]
    pub const fn risk(&self) -> &R {
        &self.risk
    }

    /// The reconstructed book for an instrument, after the run.
    #[must_use]
    pub fn book(&self, instrument: InstrumentId) -> Option<&Book> {
        self.books.get(instrument.index())
    }
}

impl Ledger {
    /// Refuse a request at the seam, before any venue sees it.
    ///
    /// The order still gets its id, because the caller was promised one and a
    /// refused order still has to be something the strategy can reconcile
    /// against.
    fn refuse(&mut self, client_order_id: ClientOrderId, reason: RejectReason, ts: Ts) {
        self.stats.refused += 1;
        self.deferred.push(ExecutionEvent::Rejected {
            client_order_id,
            reason,
            ts,
        });
    }

    /// Mint the next client order id.
    fn mint(&mut self) -> ClientOrderId {
        let id = ClientOrderId(self.next_id);
        self.next_id += 1;
        id
    }
}

/// Whether a request is well-formed enough to send anywhere.
pub(crate) fn seam_check(request: &OrderRequest) -> Option<RejectReason> {
    (!request.is_valid()).then_some(RejectReason::Malformed)
}

#[cfg(test)]
mod tests;
