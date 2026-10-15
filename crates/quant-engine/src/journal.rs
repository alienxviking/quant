//! Our own record of what we did.
//!
//! A backtest can be re-run from raw. A two-week paper session cannot: the fills
//! it produced were decided against a live book that no longer exists in that
//! state, and the engine's position lives in memory. A restart on day nine with
//! no journal is a restart with a flat book and a forgotten position, which is
//! the shape of failure that loses real money at M8.
//!
//! # Why a file, and not the metadata tier
//!
//! `docs/data-contract.md` puts orders and fills in Postgres, and they will go
//! there. But M1.c2 made the metadata tier **best-effort and optional** — no
//! database, or one that will not answer, and the recorder carries on — and a
//! position that must survive a restart cannot depend on something optional.
//!
//! So the relationship is the one the raw tier already has with its index: this
//! file is the **source of truth** for our own actions, and Postgres is a query
//! surface over it. That is not a new rule, it is the existing rule applied to a
//! second kind of irreplaceable data.
//!
//! # Why JSON Lines
//!
//! Against the grain of this project, which framed the raw tier in binary for
//! good reasons. Those reasons do not apply here. The raw tier is tens of
//! millions of records a day and its density decides whether a week fits on a
//! laptop; a journal is a few hundred lines a week. What matters instead is that
//! it can be read at three in the morning by a person, and by `jq`, and appended
//! to safely after a crash — and a torn line is trivially detectable and
//! discardable, whereas a torn binary frame needed a whole container format.
//!
//! # A torn line, and the restart after it
//!
//! A write that is cut short leaves a last line that is not valid JSON. A
//! process killed mid-`append` leaves the start of an entry; a power loss can
//! leave a run of NUL bytes instead, where the file's new length reached the
//! disk before its data did. The supervisor then restarts the process, and the
//! restart appends — so a torn line does not stay the last line for long. Two
//! rules keep that safe, and both keep every byte and the file append-only:
//!
//! - **Opening terminates the fragment.** [`Journal::open`] writes a newline
//!   first when the file does not end in one, so the next entry starts on its
//!   own line instead of being glued onto the fragment. Glued, the two became
//!   one unreadable line in the middle of the file, which `read` refused as
//!   damage — so a journal that had survived a hard kill was made unreadable
//!   by the restart that followed it: to `reconcile`, to `explain`, and to the
//!   next restart, which is when it is needed most.
//! - **Reading tolerates exactly that shape and counts it.** A line that is not
//!   valid JSON may be the last line, or it may be followed by a `Started`
//!   entry, which is the first thing every session writes. Either is reported
//!   in [`Recovered`] as the shape an interrupted write leaves; which
//!   interruption left it, the file cannot say. A bad line followed by anything
//!   else is damage and an error, because nothing else writes into the middle
//!   of an append-only file.
//!
//! "Not valid JSON" is the whole of that tolerance, and `read` asks
//! `serde_json` rather than guessing: JSON cut short is its `Eof`, and bytes
//! that are not JSON at all — NULs, or a fragment with NULs after it — are its
//! `Syntax`. A line that is valid JSON and still not an entry is its `Data`: an
//! entry type a newer build wrote, or a field this build requires that the
//! writer left out. No interrupted write produces that, so it is an error
//! wherever it is, the last line and the line before a `Started` included.
//! Tolerated at the end, it would be discarded under a torn line's name, and
//! the restart after it would leave it in the middle of the file for every
//! later reader to refuse.
//!
//! # Why the instrument is `(exchange, symbol)`
//!
//! `InstrumentId` is a registry index and is **never persisted** — M0's rule,
//! and the same reason the Parquet tier has no instrument column. An id written
//! on Tuesday means something else on Wednesday if the registration order
//! changed. Identity is reattached on replay.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde_json::error::Category;

use crate::RefusedBy;
use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, Fill, RejectReason, VenueOrderId, VenueTradeId};
use quant_core::fixed::{Notional, Px, Qty};
use quant_core::instrument::{
    Exchange, FilterBreach, Instrument, InstrumentDef, InstrumentId, InstrumentRegistry,
};
use quant_core::report::{
    AccountChange, AnomalyKind, CancelRefusal, Cited, DoubtChange, EndedBy, ForeignOutcome,
    Holding, Landing, Provenance, VenueText,
};
use quant_core::time::Ts;

use crate::portfolio::Portfolio;

/// How an instrument is named on disk.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstrumentKey {
    pub exchange: Exchange,
    /// The venue's own symbol, verbatim.
    pub symbol: String,
}

/// One line of the journal.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JournalEntry {
    /// A session began, with this much cash.
    ///
    /// Written once per run. On a restart the *first* one in the file is the
    /// authority on starting capital, and later ones mark restarts — so the
    /// number a report quotes is the capital the whole session began with, not
    /// whatever it happened to have when it last came up.
    ///
    /// And written *first*, before anything else a run appends. [`read`] relies
    /// on that to tell a line a crash tore and a restart wrote past from damage
    /// in the middle of the file.
    Started {
        at: Ts,
        cash: Notional,
        /// Which shape of this file the writer spoke.
        ///
        /// Added at M7.5, which widened this enum from five variants to a
        /// dozen. A pre-M7.5 reader meeting a decision entry treats the unknown
        /// tag as corruption, because `read` has no version to refuse at — the
        /// same problem M1.d1 had when the raw container went 1 → 2, without
        /// M1.d1's header to refuse in. This is here so the *next* change can
        /// say what is actually wrong rather than reporting a readable file as
        /// damaged.
        ///
        /// Defaulted on read so every journal written before it existed still
        /// parses, and reports as schema 1.
        ///
        /// Schema 3 is the first change it was there for, and [`read`] now
        /// refuses a session begun in a schema newer than [`SCHEMA`], naming
        /// both numbers.
        #[serde(default = "schema_v1")]
        schema: u32,
        /// The run tag a live session sends its orders under, as
        /// `q-<run>-<id>`.
        ///
        /// What tells this run's orders from an earlier run's at the venue,
        /// where the client order ids of every run share one namespace: an
        /// earlier run's order 5 and this run's order 5 differ only in the
        /// tag. Like the capital, the **first** `started` line's tag is the
        /// session's ([`run_tag`]); a restart carries the same one on.
        ///
        /// `None` for a backtest and for paper, which send nothing to a venue
        /// and so have nothing to tell apart. Absent from every journal
        /// written before schema 3, and written only when present, so a run
        /// without one writes the line it always wrote.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run: Option<String>,
    },
    /// Something traded.
    Filled {
        at: Ts,
        client_order_id: ClientOrderId,
        instrument: InstrumentKey,
        side: Side,
        px: Px,
        qty: Qty,
        fee: Notional,
        is_maker: bool,
        /// Which asset the venue took its fee in. Absent from journals written
        /// before M8.c, where it reads back as `Quote` — M4's simplification,
        /// which is what those runs actually did.
        #[serde(default)]
        fee_asset: quant_core::execution::FeeAsset,
        /// What of the order was still working after this fill.
        ///
        /// Schema 3. A venue reports it on every trade, and a restart that
        /// finds an order with something left knows the order may still be
        /// working at the venue, which the fill alone cannot say.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remaining: Option<Qty>,
        /// The venue's id for the order. With `trade_id`, the key one of the
        /// venue's `myTrades` rows is found by: one line here is one row
        /// there, which is what criterion A compares. Schema 3, and only ever
        /// written by a venue that names its orders, which a simulator does
        /// not.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        venue_order_id: Option<VenueOrderId>,
        /// The venue's id for the trade. Schema 3, as `venue_order_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trade_id: Option<VenueTradeId>,
    },
    /// What the *engine* believed at this moment.
    ///
    /// This is the entry that makes reconciliation a check rather than a
    /// tautology. Replaying the fills recomputes cash, realized and fees from
    /// the same lines, so `cash - starting == realized - fees` is true by
    /// construction and proves nothing. A checkpoint is a **second, independent**
    /// claim: the running engine states what it thinks it has, and the recompute
    /// has to arrive at the same numbers from the journal alone.
    ///
    /// A disagreement means a fill was acted on and not written down, or the
    /// accounting drifted — and neither would be visible from one number.
    Checkpoint {
        at: Ts,
        cash: Notional,
        realized: Notional,
        fees: Notional,
        fills: u64,
    },
    /// An order was minted and is about to reach the venue.
    ///
    /// Written **before** the wire, which is the fill rule generalised: an order
    /// at a venue we never recorded is an orphan position.
    ///
    /// `mark` is the mid the risk layer priced it against. It is the one input
    /// to a refusal that cannot be re-derived from raw, because it depends on
    /// the book *as the engine saw it* — which a replay reconstructs but a
    /// reader cannot attribute to a specific decision without this.
    Submitted {
        at: Ts,
        client_order_id: ClientOrderId,
        instrument: InstrumentKey,
        side: Side,
        qty: Qty,
        limit: Option<Px>,
        mark: Option<Px>,
    },
    /// An order was refused, and the venue never heard about it.
    Refused {
        at: Ts,
        client_order_id: ClientOrderId,
        instrument: InstrumentKey,
        side: Side,
        qty: Qty,
        reason: RejectReason,
        /// Which limit bound. `None` for a seam refusal, which is not a limit.
        bound: Option<crate::risk::Bound>,
        by: RefusedBy,
    },
    /// The venue acknowledged an order.
    Accepted {
        at: Ts,
        client_order_id: ClientOrderId,
        /// The venue's id for the order, when it named one. Schema 3; a
        /// simulator names none, and its lines read as they always did.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        venue_order_id: Option<VenueOrderId>,
    },
    /// The venue declined an order it had been told about.
    ///
    /// Keeps that meaning in schema 3: the venue had the request. One that
    /// never reached it is not a rejection by the venue.
    Rejected {
        at: Ts,
        client_order_id: ClientOrderId,
        reason: RejectReason,
        /// What the venue said, as it said it: the only durable copy of a
        /// REST answer, because REST bodies are not in raw. Schema 3.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cited: Option<Cited>,
        /// The filter rule the venue named, when it is one we model, for
        /// criterion B to compare with the rule `Filters::rejects` names.
        /// Schema 3.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        breach: Option<FilterBreach>,
    },
    /// A cancel was sent; whether it won the race is a separate entry.
    CancelRequested {
        at: Ts,
        client_order_id: ClientOrderId,
    },
    /// An order was withdrawn, with whatever never traded.
    Cancelled {
        at: Ts,
        client_order_id: ClientOrderId,
        remaining: Qty,
        /// The venue's id for the order, when it named one. Schema 3.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        venue_order_id: Option<VenueOrderId>,
        /// How the venue says it ended: cancelled, expired, or stopped by
        /// self-trade prevention. One outcome to the strategy and three to a
        /// person reconciling against the venue. Schema 3.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ended: Option<EndedBy>,
    },
    /// A fill arrived for an order this engine has no record of.
    ///
    /// Recorded rather than counted, because an orphan is the one fill that
    /// cannot be reconciled: the portfolio never saw it, so `Checkpoint` and the
    /// fold agree with each other while both disagree with the venue.
    Orphaned {
        at: Ts,
        client_order_id: ClientOrderId,
        px: Px,
        qty: Qty,
        fee: Notional,
        /// Which asset `fee` is in. An orphan is investigated by hand against
        /// the venue's record, and a fee without its asset is half a fact.
        /// Absent from journals written before M8.c, where it reads back as
        /// `Quote`, which is what every fill before then was.
        #[serde(default)]
        fee_asset: quant_core::execution::FeeAsset,
        /// The venue's ids for the order and the trade, when it gave them:
        /// for a fill nobody can attribute, the only handle a person has on
        /// the venue's own record of it. Schema 3.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        venue_order_id: Option<VenueOrderId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trade_id: Option<VenueTradeId>,
    },
    /// The kill switch was thrown.
    ///
    /// Journalled because a switch held only in memory is re-armed by the
    /// supervisor that restarts the process — the machinery meant to keep the
    /// system running would silently resume trading minutes after a limit said
    /// stop. `RiskEngine::recover` reads this back.
    Tripped {
        at: Ts,
        cause: crate::risk::TripCause,
    },
    /// The market went dark, and for how long we could not see it.
    ///
    /// The answer to "why did it not trade between 03:00 and 04:00" is one of
    /// four things, and three of them are already recorded — a crossing that
    /// fired, a refusal, a suppression. This is the fourth, and without it a
    /// quiet hour and a blind hour are the same silence in the file.
    ///
    /// `last_good_ts` is the last instant we were confident the stream was
    /// intact, so `at - last_good_ts` is the width of the blindness. That is a
    /// **duration, not a message count**, which is a deliberate narrowing of
    /// what this slice was planned to record: a count would have to be carried
    /// on `MarketEvent::Gap`, and that type is persisted in the normalized tier,
    /// so a field added for a log line would change the Parquet schema of the
    /// whole `gaps/` dataset. The question the operator actually asks is *were
    /// we blind at 03:30*, and a duration answers it exactly.
    ///
    /// A `LocalOverflow` cause is the engine's own account of records the tee
    /// dropped on its way in — but **one line per hole, not per record**, which
    /// the first version of this comment got wrong. `TeeSink` counts each record
    /// it could not hand over; `LiveSource` compares `ingest_seq` against the
    /// expected next and emits one gap for the whole discontinuity however wide
    /// it is. The two are equal only when every hole is one record across.
    ///
    /// The pair that *should* agree is `TeeSink::secondary_dropped` against
    /// `LiveStats::records_missed`, which is the width the live source
    /// accumulates separately. Neither is derived from the other, so a
    /// disagreement means one of them is wrong. Counting `blind` lines against
    /// the tee would instead report a disagreement on every healthy run that
    /// ever dropped two records at once.
    Blind {
        at: Ts,
        cause: quant_core::event::GapCause,
        last_good_ts: Ts,
    },
    /// The strategy's own account of something it decided.
    ///
    /// The column this milestone exists for, and the weakest one in the file.
    /// Everything else here is written by the engine about facts it witnessed —
    /// a fill it booked, an order it sent, a limit that bound. A note is the
    /// strategy talking about itself, and `detail` is opaque, so nothing can
    /// contradict it the way folding the fills contradicts a bad `Checkpoint`.
    /// A strategy that writes nothing leaves a file that looks complete.
    ///
    /// The narrowing, not a fix: a strategy may periodically emit a note whose
    /// `kind` is `claim`, holding its own running totals. The notes between two
    /// claims must fold to the difference between them, so a **dropped** line is
    /// detectable. Both sides of that fold are written by the same strategy, so
    /// it catches a lost note and not a wrong belief. Conceded in
    /// `docs/run-log.md` §6 rather than answered.
    ///
    /// Carries no `fee`, no `cash`, no `realized` and no `fills` — the three
    /// defences that keep a decision line from being folded as money by someone
    /// with the wrong filter. `Checkpoint` stays the only claim about money.
    Note {
        at: Ts,
        kind: String,
        detail: serde_json::Value,
    },
    /// A session ended cleanly.
    ///
    /// Its absence is how a crash is told from a clean stop — the same
    /// two-sided-completeness idea as the raw container's trailer.
    Stopped {
        at: Ts,
        /// The orders the engine was still working when it stopped, lowest id
        /// first: the engine's claim, which a fold of the lifecycle lines can
        /// be checked against. Stopping is not flattening, and a restart that
        /// does not know an order may still be live at the venue would trade
        /// as though it were not.
        ///
        /// Schema 3, and written only when not empty. Against the simulator it
        /// is empty unless reports were still in flight when the data ended,
        /// which inbound latency makes ordinary: M4 decided those are counted,
        /// not delivered, so the engine still holds the order.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        working: Vec<ClientOrderId>,
    },
    /// The venue's REST acknowledgement of an order. Schema 3.
    ///
    /// Not an acceptance: the account stream is the one authority for that.
    /// But the venue's id for the order is what still finds it after a cancel
    /// has replaced its client id (criterion E), and this may be the only
    /// line that holds it for an order whose stream report never came.
    Placed {
        at: Ts,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        /// The venue's `transactTime`, in milliseconds on the venue's clock.
        /// For the record only: the engine dispatches on when we heard.
        transact_ms: i64,
    },
    /// A submission that never became an order the venue had. Schema 3.
    ///
    /// Not `rejected`, which keeps its meaning: the venue had the request and
    /// declined it. This one never reached it, or a lookup afterwards found no
    /// such order. Certain either way, so the order is over.
    NeverLanded {
        at: Ts,
        client_order_id: ClientOrderId,
        landing: Landing,
    },
    /// An order's outcome stopped being known, or became known again.
    /// Schema 3.
    ///
    /// Raised and cleared explicitly, so "left in doubt" is a fold of this
    /// file rather than an inference from what is missing from it.
    Doubt {
        at: Ts,
        client_order_id: ClientOrderId,
        change: DoubtChange,
    },
    /// A cancel the venue did not carry out, or whose fate is unknown. The
    /// order, if there is one, is as it was. Schema 3.
    CancelRefused {
        at: Ts,
        client_order_id: ClientOrderId,
        why: CancelRefusal,
    },
    /// A report about an order this run did not send. Schema 3.
    ///
    /// The client id is kept as the venue sent it, as text, and never made a
    /// `ClientOrderId`: an earlier run's order 5 must not be read as this
    /// run's order 5. A trade is kept whole, commission asset verbatim,
    /// because it moved the account's balances outside this run's record and
    /// criterion A has to account for it.
    Foreign {
        at: Ts,
        client_id: VenueText,
        provenance: Provenance,
        venue_order_id: VenueOrderId,
        side: Side,
        what: ForeignOutcome,
    },
    /// The account stream was lost or regained. Schema 3.
    Account { at: Ts, change: AccountChange },
    /// A report that could not be read, or contradicts what came before it,
    /// recorded with its text rather than dropped. Schema 3.
    Anomaly {
        at: Ts,
        /// The order it concerns, when that much could be read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_order_id: Option<ClientOrderId>,
        what: AnomalyKind,
        detail: VenueText,
    },
    /// A report about an order the engine was not working, other than a fill
    /// (which is `orphaned`): an id it never minted, or an order whose end it
    /// had already heard. Kept from the strategy and written here. Schema 3.
    Stray {
        at: Ts,
        client_order_id: ClientOrderId,
        kind: StrayKind,
        /// The venue's id for the order, when the report named one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        venue_order_id: Option<VenueOrderId>,
    },
    /// The market came into view: a book went from meaning nothing to
    /// meaning something, at the first snapshot of a run or the one that
    /// anchors it again after a gap. Schema 3.
    ///
    /// The other half of `blind`, written rather than inferred. Before schema
    /// 3 a reader took any later line the engine wrote as proof the market was
    /// back, which holds only while a venue cannot speak without the market:
    /// a live venue reports on wakes a dark market produces.
    Sighted { at: Ts },
    /// What the venue said the account held of the instrument's two assets.
    /// Schema 3.
    ///
    /// Criterion A's baseline: our equity and the venue's two balances,
    /// marked at one price, must agree, and the balances at the start of a
    /// run can never be taken after the fact. Written by the live binary at
    /// start and at stop, through [`JournalEntry::balances`].
    ///
    /// The venue's claim, never folded: a recompute that read it would make
    /// criterion A compare the venue with itself. `at` is the wall clock when
    /// the venue was asked, as `started` is, because the market has not
    /// begun, or has ended, when it is taken.
    Balances {
        at: Ts,
        base_asset: String,
        base_free: Qty,
        base_locked: Qty,
        quote_asset: String,
        quote_free: Notional,
        quote_locked: Notional,
    },
}

/// An instrument whose definition names no assets, so its balances could not
/// be said to be of anything.
///
/// What a placeholder registered by [`replay`] looks like, and what the
/// user-data translator refuses for the same reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnnamedAssets {
    pub exchange: Exchange,
    pub symbol: String,
}

impl std::fmt::Display for UnnamedAssets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} is registered without its base and quote asset names, so a balance \
             could not say which asset it is of: register the venue's definition first",
            self.exchange, self.symbol
        )
    }
}

impl std::error::Error for UnnamedAssets {}

/// What a `stray` line reported: every kind of order report but a fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrayKind {
    Accepted,
    Rejected,
    Cancelled,
}

impl JournalEntry {
    #[must_use]
    pub const fn at(&self) -> Ts {
        match self {
            Self::Started { at, .. }
            | Self::Filled { at, .. }
            | Self::Checkpoint { at, .. }
            | Self::Tripped { at, .. }
            | Self::Submitted { at, .. }
            | Self::Refused { at, .. }
            | Self::Accepted { at, .. }
            | Self::Rejected { at, .. }
            | Self::CancelRequested { at, .. }
            | Self::Cancelled { at, .. }
            | Self::Orphaned { at, .. }
            | Self::Blind { at, .. }
            | Self::Note { at, .. }
            | Self::Stopped { at, .. }
            | Self::Placed { at, .. }
            | Self::NeverLanded { at, .. }
            | Self::Doubt { at, .. }
            | Self::CancelRefused { at, .. }
            | Self::Foreign { at, .. }
            | Self::Account { at, .. }
            | Self::Anomaly { at, .. }
            | Self::Stray { at, .. }
            | Self::Sighted { at }
            | Self::Balances { at, .. } => *at,
        }
    }
}

impl JournalEntry {
    /// A `balances` line: what the venue said the account held of
    /// `instrument`'s two assets, at `at` on the wall clock.
    ///
    /// What the live binary calls at start and at stop. The asset names come
    /// from the instrument's definition rather than from the caller, so the
    /// line cannot name one asset and hold another's amount, and an amount of
    /// the base is a quantity while one of the quote is money: two types, so
    /// the two cannot be passed the wrong way round.
    ///
    /// # Errors
    ///
    /// [`UnnamedAssets`] when the definition has no asset names, as a
    /// placeholder registered by [`replay`] has none.
    pub fn balances(
        at: Ts,
        instrument: &Instrument,
        base: Holding<Qty>,
        quote: Holding<Notional>,
    ) -> Result<Self, UnnamedAssets> {
        if instrument.base.is_empty() || instrument.quote.is_empty() {
            return Err(UnnamedAssets {
                exchange: instrument.exchange,
                symbol: instrument.symbol.clone(),
            });
        }
        let Holding {
            free: base_free,
            locked: base_locked,
        } = base;
        let Holding {
            free: quote_free,
            locked: quote_locked,
        } = quote;
        Ok(Self::Balances {
            at,
            base_asset: instrument.base.clone(),
            base_free,
            base_locked,
            quote_asset: instrument.quote.clone(),
            quote_free,
            quote_locked,
        })
    }

    /// A checkpoint stating what `portfolio` currently believes.
    ///
    /// Built from the portfolio rather than from fields a caller assembles, so
    /// the claim and the thing being claimed cannot drift apart.
    #[must_use]
    pub fn checkpoint(at: Ts, portfolio: &Portfolio) -> Self {
        Self::Checkpoint {
            at,
            cash: portfolio.cash(),
            realized: portfolio.realized(),
            fees: portfolio.fees(),
            fills: portfolio.fills(),
        }
    }
}

/// Append-only writer.
#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    out: BufWriter<File>,
    written: u64,
}

impl Journal {
    /// Open for appending, creating it if absent.
    ///
    /// A file that does not end in a newline gets one before anything else is
    /// written. That is the mark of a write that was cut short, and writing
    /// straight after the fragment would glue the next entry onto it: one
    /// unreadable line in the middle of the file, where `read` cannot tell an
    /// interrupted write from damage. Terminated, the fragment keeps every one
    /// of its bytes, the file stays append-only, and the next entry — a
    /// restart's `Started` — begins on its own line, which is the shape `read`
    /// accepts and counts.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        if ends_mid_line(&mut file)? {
            // Not synced here: the first `append` syncs the file, and a crash
            // before it leaves a torn tail either way, which `read` tolerates
            // and the next `open` terminates again.
            file.write_all(b"\n")?;
        }
        Ok(Self {
            path: path.to_owned(),
            out: BufWriter::new(file),
            written: 0,
        })
    }

    /// Append one entry, and make sure it is on the disk before returning.
    ///
    /// Flushed and `sync_data`d per entry. That is expensive per call and
    /// irrelevant in aggregate — a few hundred entries a week — and it buys the
    /// only property this file has to have: an entry that has been acknowledged
    /// to the strategy is an entry that survives losing power. Buffering fills
    /// to batch the syncs would trade that away for nothing.
    pub fn append(&mut self, entry: &JournalEntry) -> std::io::Result<()> {
        let line = serde_json::to_string(entry).map_err(std::io::Error::other)?;
        writeln!(self.out, "{line}")?;
        self.out.flush()?;
        self.out.get_ref().sync_data()?;
        self.written += 1;
        Ok(())
    }

    #[must_use]
    pub const fn written(&self) -> u64 {
        self.written
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Whether a file has bytes after its last newline: a line was being written
/// when the writer stopped.
fn ends_mid_line(file: &mut File) -> std::io::Result<bool> {
    if file.metadata()?.len() == 0 {
        return Ok(false);
    }
    file.seek(SeekFrom::End(-1))?;
    let mut last = [0_u8; 1];
    file.read_exact(&mut last)?;
    Ok(last[0] != b'\n')
}

/// What reading a journal back found.
#[derive(Debug, Default, Clone)]
pub struct Recovered {
    pub entries: Vec<JournalEntry>,
    /// A final line that was not valid JSON: cut short, or not JSON at all.
    ///
    /// Reported rather than treated as an error, because that is the shape an
    /// interrupted write leaves — a process killed mid-`append` leaves the start
    /// of an entry, and a power loss can leave NUL bytes — and that is a *torn
    /// tail*: the same distinction `quant-storage` draws between an interrupted
    /// write and corruption. A final line that is valid JSON and not an entry
    /// is not torn, and `read` refuses it.
    pub torn_tail: bool,
    /// Lines that were not valid JSON and that a restart then wrote past,
    /// numbered from 1.
    ///
    /// A torn tail stops being the tail as soon as the supervisor restarts the
    /// process: [`Journal::open`] terminates the fragment and the restart
    /// appends its `Started` on the next line. So a line that is not valid JSON
    /// can sit in the middle of the file in exactly one honest way, immediately
    /// before a `Started`, and these are those lines. Listed rather than counted
    /// silently, so a person can find each one; the entry that was being
    /// written when the write was cut short is lost, and this is where it was.
    ///
    /// A bad line followed by anything else is damage and `read` refuses it,
    /// because nothing else writes into the middle of an append-only file. So
    /// is a line that is valid JSON and not an entry, wherever it is.
    pub torn_before_restart: Vec<usize>,
}

/// Read a journal back.
///
/// A missing file is an empty journal, not an error: the first run of a session
/// has not written one yet.
///
/// A line that is not valid JSON — cut short, or not JSON at all — is tolerated
/// in two places only, the two an interrupted write can leave one in: as the
/// last line, and immediately before a `Started` entry. Anywhere else it is an
/// error. A line that *is* valid JSON and still not an entry is an error
/// wherever it is, those two places included, because no interrupted write
/// produces one. See the module docs.
pub fn read(path: &Path) -> std::io::Result<Recovered> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Recovered::default()),
        Err(e) => return Err(e),
    };
    let mut out = Recovered::default();
    // Bytes, not `String`s. A write cut through a multi-byte character leaves a
    // line that is not UTF-8, and reading text would refuse the whole file over
    // it before the rule below could judge that one line. `from_slice` reports
    // invalid UTF-8 as a syntax error, which is what such a line is. A lossy
    // decode would be worse: a whole line with one damaged byte would parse,
    // with the damage quietly replaced.
    let lines: Vec<Vec<u8>> = BufReader::new(file)
        .split(b'\n')
        .collect::<Result<_, _>>()?;
    // Numbered from 1, as an editor numbers them. Blank lines are dropped first
    // because they were never entries, so "the line after a torn one" is the
    // next line that says anything.
    let mut parsed = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| !line.iter().all(u8::is_ascii_whitespace))
        .map(|(i, line)| (i + 1, serde_json::from_slice::<JournalEntry>(line)))
        .peekable();
    while let Some((number, line)) = parsed.next() {
        let e = match line {
            // A session a newer build began. Refused at its `started` line,
            // which is the first thing that session wrote, so the error names
            // what is actually wrong -- a file this build is too old for --
            // rather than the first entry type it has never heard of, which
            // would read as damage. Refused even where every line happens to
            // parse: a newer schema may have changed what an old field means,
            // and a reader that guessed would fold money it does not
            // understand.
            Ok(JournalEntry::Started { schema, .. }) if schema > SCHEMA => {
                return Err(std::io::Error::other(format!(
                    "journal line {number} begins a session written in journal schema {schema}, \
                     and this build reads schema {SCHEMA} and older: read it with a newer build"
                )))
            }
            Ok(entry) => {
                out.entries.push(entry);
                continue;
            }
            Err(e) => e,
        };
        // Which kind of failure, before where it is. Exhaustive, so every
        // category is a decision here rather than a default.
        match e.classify() {
            // The two shapes an interrupted write leaves: JSON cut short, and
            // bytes that are not JSON at all -- a run of NULs where the file's
            // length reached the disk before its data, alone or after a
            // fragment. Whether one is tolerated depends on where it is.
            Category::Eof | Category::Syntax => {}
            // Valid JSON that is not an entry: a type a newer build wrote, or a
            // field this build requires that the writer left out. No
            // interrupted write produces that, so it is not torn wherever it
            // is, and calling it torn would discard a whole entry under a false
            // name. `Io` cannot come out of `from_str`, and is refused with it
            // rather than given a tolerance nothing has argued for.
            Category::Data | Category::Io => {
                return Err(std::io::Error::other(format!(
                    "journal line {number} is not an entry this build can read, and not the \
                     shape an interrupted write leaves either, so it is not torn -- an entry \
                     from a newer build, or damage: {e}"
                )))
            }
        }
        match parsed.peek() {
            // The write the crash interrupted, not yet written past.
            None => out.torn_tail = true,
            // The same, and the restart that followed it.
            Some((_, Ok(JournalEntry::Started { .. }))) => out.torn_before_restart.push(number),
            Some(_) => {
                return Err(std::io::Error::other(format!(
                    "journal line {number} is not readable, and only the last line may be torn, \
                     or a line a restart's `started` entry follows: {e}"
                )))
            }
        }
    }
    Ok(out)
}

/// Rebuild a portfolio from a journal alone.
///
/// This is the **independent recompute** in `docs/engine-contract.md` §9: it
/// shares no state with the engine's running portfolio and reaches the same
/// numbers by replaying what was written down. Two tallies of one quantity, in
/// the same shape as the venue and the portfolio each totalling fees, and
/// `quant-verify` and `quant-normalize` each counting frames.
///
/// Instruments are re-registered from `(exchange, symbol)`, so the ids this
/// produces are this process's ids and not the ones the journal was written
/// under — which is precisely why the pair is what gets persisted.
///
/// An instrument `registry` does not already hold is registered with
/// placeholder rules, because a journal records what we traded and not the
/// venue's filters. A caller that will trade the instrument afterwards must
/// register its real definition *before* replaying — which is what [`resume`]
/// is for.
#[must_use]
pub fn replay(entries: &[JournalEntry], registry: &mut InstrumentRegistry) -> Portfolio {
    let starting = entries
        .iter()
        .find_map(|e| match e {
            JournalEntry::Started { cash, .. } => Some(*cash),
            _ => None,
        })
        .unwrap_or(Notional::ZERO);
    let mut portfolio = Portfolio::new(starting);

    for entry in entries {
        // Exhaustive, with no `_` arm, and that is the point rather than style.
        // M7.5 widens this enum from five variants to a dozen, and the one
        // failure that must be impossible is a new variant that silently fails
        // to move money -- a decision record quietly treated as a position
        // record, or the reverse. With a catch-all the compiler says nothing and
        // the defect surfaces as a reconciliation mismatch weeks later; without
        // one, adding a variant does not build until somebody has decided what
        // it does to the portfolio. A compile error instead of a paragraph.
        // `Orphaned` is kept as its own arm though its body matches the
        // others'. The arms are the same *consequence* and different
        // *decisions*: every other variant does nothing to the portfolio
        // because it is not money, and an orphan does nothing because it is
        // money we could not attribute. Merging them would delete the only
        // place that distinction is written down, and the next person to touch
        // this would have no reason not to fold it.
        #[allow(clippy::match_same_arms)]
        match entry {
            // Every field named, and no `..` -- the exhaustive match above,
            // applied one level down. `fee_asset` is what a `..` cost: the
            // writer has recorded it since PR #50, this arm elided it and
            // folded every fee as quote, and nothing complained, because a
            // pattern with `..` compiles whatever the entry grows. Named, the
            // next field is a compile error here until somebody has decided
            // whether it moves money.
            JournalEntry::Filled {
                // When it happened and which order it was are not money.
                at: _,
                client_order_id: _,
                instrument,
                side,
                px,
                qty,
                fee,
                is_maker,
                fee_asset,
                // What was left, and the venue's names for the order and the
                // trade: facts about the order, not money.
                remaining: _,
                venue_order_id: _,
                trade_id: _,
            } => {
                let id = resolve(registry, instrument);
                portfolio.apply_fill(
                    id,
                    *side,
                    &Fill {
                        px: *px,
                        qty: *qty,
                        fee: *fee,
                        is_maker: *is_maker,
                        fee_asset: *fee_asset,
                    },
                );
            }
            // `Started` is read above, where the *first* one wins: a restart
            // must not reset the capital the session began with.
            JournalEntry::Started { .. }
            // A claim about the fold, never an input to it -- folding a
            // checkpoint back in would make the check compare itself.
            | JournalEntry::Checkpoint { .. }
            // Controls, not money.
            | JournalEntry::Tripped { .. }
            // Blindness is the absence of input, not a movement of money.
            | JournalEntry::Blind { .. }
            // The strategy talking about itself. Deliberately carries nothing a
            // money fold keys on, so it cannot be mistaken for one.
            | JournalEntry::Note { .. }
            | JournalEntry::Stopped { .. }
            // Decisions. None of them moves the portfolio: a submission is an
            // intent, a refusal is an intent that stopped at the chokepoint,
            // and an acceptance or a cancellation changes what is outstanding
            // rather than what is held. Only `Filled` is money.
            | JournalEntry::Submitted { .. }
            | JournalEntry::Refused { .. }
            | JournalEntry::Accepted { .. }
            | JournalEntry::Rejected { .. }
            | JournalEntry::CancelRequested { .. }
            | JournalEntry::Cancelled { .. }
            // What a live venue says on its own, schema 3. Conditions and
            // acknowledgements, none of them money: an order placed, in doubt,
            // a cancel refused, the stream lost, a report unreadable, a report
            // about an order not being worked. Whatever money moved because of
            // any of them arrives as a `filled` line of its own.
            | JournalEntry::Placed { .. }
            | JournalEntry::Doubt { .. }
            | JournalEntry::CancelRefused { .. }
            | JournalEntry::Account { .. }
            | JournalEntry::Anomaly { .. }
            | JournalEntry::Stray { .. }
            // Nothing traded: the venue never had the order.
            | JournalEntry::NeverLanded { .. }
            // Seeing the market again is the end of an absence of input, as
            // `blind` is the start of one.
            | JournalEntry::Sighted { .. } => {}
            // Money, and not this run's: the `Orphaned` argument below, for an
            // order that is not even ours to lose track of. Folding a
            // stranger's trade would make the recompute disagree with a
            // checkpoint that faithfully reports what this engine holds, and
            // the disagreement would read as an accounting bug rather than as
            // what it is. Criterion A accounts for it against the venue's
            // balances, which is the comparison it belongs in.
            JournalEntry::Foreign { .. } => {}
            // The venue's claim about the account, which criterion A compares
            // with this fold. Folding it in would make that comparison one of
            // the venue with itself.
            JournalEntry::Balances { .. } => {}
            // Deliberately *not* folded, and this is the one worth arguing. An
            // orphan is a fill the engine could not attribute, so the live
            // portfolio never saw it either -- folding it here would make the
            // recompute disagree with a `Checkpoint` that is faithfully
            // reporting what the engine believed. The disagreement would then
            // read as an accounting bug rather than as what it is: a fill we
            // cannot account for. It is recorded so a human can see it.
            //
            // Nothing that reads the journal back says so yet. The one thing
            // that says it out loud is the error `JournalWriter` logs as it
            // writes the line. `reconcile` agrees, because neither the
            // checkpoint nor this fold counts it, and `runlog check` never
            // reads an `Orphaned` line, so an orphan alone never changes its
            // verdict. An orphaned line must be loud in `runlog check`, and
            // making it so is owed to the readers' pull request,
            // `m8c-journal-readers` (`docs/live-run.md` §4), with the `stray`
            // and other-run `foreign` lines that need the same.
            JournalEntry::Orphaned { .. } => {}
        }
    }
    portfolio
}

/// Whether a recompute agrees with what the engine claimed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Agreement {
    /// No checkpoint in the journal, so there is nothing to check against.
    ///
    /// Reported rather than treated as agreement: "nobody disagreed" and "two
    /// independent answers matched" are very different statements, and only one
    /// of them is evidence.
    NothingToCheck,
    Agrees,
    Differs {
        field: &'static str,
        claimed: Notional,
        recomputed: Notional,
    },
    /// The fill counts differ, which localises the problem: a fill was acted on
    /// and never written down, or written twice.
    FillCountDiffers {
        claimed: u64,
        recomputed: u64,
    },
}

/// Compare the journal's last checkpoint against a recompute of the entries it
/// describes.
///
/// The check in `docs/engine-contract.md` §9. The two numbers are computed by
/// different code from different inputs -- one accumulated in memory as fills
/// arrived, one replayed from a file afterwards -- which is what makes their
/// agreement mean something.
///
/// # A checkpoint is a claim about a *prefix*
///
/// This used to take an already-recomputed portfolio, and every caller replayed
/// the whole journal to produce it. That is correct only for a journal nothing
/// is writing to any more. `ops/verify-loop.sh` runs `reconcile` *during* a run,
/// against a file the engine is still appending to, so the fills after the last
/// checkpoint are ordinarily there -- and comparing a claim made at fill 5
/// against a replay of 8 fills reported `DISAGREES on fills: the engine counted
/// 5, the journal holds 8` on a session that was perfectly healthy.
///
/// Observed, not theorised: a five-minute rehearsal went `OK`, `FAIL`, `FAIL` on
/// consecutive passes as the journal grew. Over a fortnight that is a red
/// verdict every six hours from the first checkpoint onward -- the failure M1's
/// verifier already taught this project once, where a check that cries wolf on
/// good data is worse than no check because it trains everyone to ignore the
/// exit code.
///
/// So the replay happens *here*, over the entries up to and including the last
/// checkpoint, rather than being the caller's job to get right. `the_last_checkpoint_is_the_one_that_counts`
/// already made this argument for earlier checkpoints -- "an earlier one
/// describes an earlier state and comparing against it would fail on every
/// healthy run" -- and it is the same argument, applied to the other end.
///
/// A fresh registry is used for the recompute because only the aggregate money
/// and fill count are compared, and `an_instrument_is_named_by_exchange_and_symbol_not_by_index`
/// pins that those do not depend on id assignment.
#[must_use]
pub fn agrees(entries: &[JournalEntry]) -> Agreement {
    let last_checkpoint = entries
        .iter()
        .rposition(|e| matches!(e, JournalEntry::Checkpoint { .. }));
    let Some(end) = last_checkpoint else {
        return Agreement::NothingToCheck;
    };
    let described = &entries[..=end];
    let recomputed = &replay(described, &mut InstrumentRegistry::new());

    let Some(JournalEntry::Checkpoint {
        cash,
        realized,
        fees,
        fills,
        ..
    }) = entries.get(end)
    else {
        return Agreement::NothingToCheck;
    };

    if *fills != recomputed.fills() {
        return Agreement::FillCountDiffers {
            claimed: *fills,
            recomputed: recomputed.fills(),
        };
    }
    for (field, claimed, actual) in [
        ("cash", *cash, recomputed.cash()),
        ("realized", *realized, recomputed.realized()),
        ("fees", *fees, recomputed.fees()),
    ] {
        if claimed != actual {
            return Agreement::Differs {
                field,
                claimed,
                recomputed: actual,
            };
        }
    }
    Agreement::Agrees
}

/// The shape this build writes.
///
/// - **1**, everything before M7.5: no `schema` field at all.
/// - **2**, M7.5: decisions, refusals, the lifecycle, blindness and notes.
/// - **3**, M8 slice (c): what a live venue says beside an event and on its
///   own, the run tag, the orders still working at a stop, the market's
///   return after blindness, and the account's balances.
///
/// [`read`] refuses a session begun in a schema above this one, naming both.
pub const SCHEMA: u32 = 3;

/// The run tag of the session a journal records: the first `started` line's,
/// or `None` when that line has none, or the journal has no `started` line.
///
/// The first and not the latest, as with the capital in [`replay`]: a restart
/// continues a session rather than beginning one, and a restart that wrote a
/// different tag would otherwise rename every order the session already sent.
/// A later line that disagrees is not reconciled here; it is evidence, and the
/// first line is the authority.
#[must_use]
pub fn run_tag(entries: &[JournalEntry]) -> Option<&str> {
    entries.iter().find_map(|e| {
        if let JournalEntry::Started { run, .. } = e {
            Some(run.as_deref())
        } else {
            None
        }
    })?
}

/// What a journal with no `schema` field was: everything before M7.5.
const fn schema_v1() -> u32 {
    1
}

/// The next client order id a resumed run should mint.
///
/// `max(id) + 1` over the entries that record a mint or the outcome of one
/// for an order the engine was working, or 1 for a journal that has none.
/// An id that could have been named without being minted is left out. Derived from the ids rather than stored beside them, so there is
/// nothing that can disagree with the record.
///
/// Returns 1 for an empty journal, which is what a fresh run uses anyway — so a
/// caller does not have to special-case the first session.
#[must_use]
pub fn next_order_id(entries: &[JournalEntry]) -> u64 {
    entries
        .iter()
        .filter_map(|e| match e {
            // Not only fills: a refused order consumed an id without ever
            // producing one, and a resumed run that reused it would make the
            // hole check report holes where there are none. Fills count
            // because before M7.5 they were the only lines with an id.
            JournalEntry::Filled {
                client_order_id, ..
            }
            | JournalEntry::Submitted {
                client_order_id, ..
            }
            | JournalEntry::Refused {
                client_order_id, ..
            }
            | JournalEntry::Accepted {
                client_order_id, ..
            }
            | JournalEntry::Rejected {
                client_order_id, ..
            }
            | JournalEntry::Cancelled {
                client_order_id, ..
            } => Some(client_order_id.0),
            // Before schema 3 these two fed the counter, and neither should
            // have: each can carry an id no `submitted` or `refused` line
            // holds, and the next session would then mint past it and leave
            // the runlog hole check a hole it could not explain.
            // `Context::cancel` writes whatever id the strategy passes, minted
            // or not. And an orphan is the fill twin of a `stray`, the engine
            // withholding a report on an order it is not working, which the
            // reason below already keeps off the counter.
            JournalEntry::CancelRequested { .. }
            | JournalEntry::Orphaned { .. }
            | JournalEntry::Started { .. }
            | JournalEntry::Checkpoint { .. }
            | JournalEntry::Tripped { .. }
            // Consumes no id: nothing was decided, we simply could not see.
            | JournalEntry::Blind { .. }
            // Consumes no id either: a note describes a decision, and the
            // decision that minted an id is recorded by the engine beside it.
            | JournalEntry::Note { .. }
            | JournalEntry::Stopped { .. }
            // What a live venue says, schema 3. Every id this engine minted is
            // already on a `submitted` or `refused` line, so these add nothing
            // to the counter -- and an id the venue named must never move it.
            // `Context::cancel` accepts any id, so a refused cancel can name
            // one this engine never minted; a stray's id is the venue's word
            // about an order the engine is not working; and a foreign line's
            // id is text precisely so it cannot become a `ClientOrderId`.
            | JournalEntry::Placed { .. }
            | JournalEntry::NeverLanded { .. }
            | JournalEntry::Doubt { .. }
            | JournalEntry::CancelRefused { .. }
            | JournalEntry::Foreign { .. }
            | JournalEntry::Account { .. }
            | JournalEntry::Anomaly { .. }
            | JournalEntry::Stray { .. }
            // Consumes no id: the market, not a decision.
            | JournalEntry::Sighted { .. }
            // Nor the account, which is the venue's claim.
            | JournalEntry::Balances { .. } => None,
        })
        .max()
        .map_or(1, |highest| highest + 1)
}

/// Everything a restarted session takes from its journal.
///
/// One value, so that a session is resumed or not as a whole. The capital, the
/// position, the kill switch and the id counter each have their own way of
/// going wrong across a restart, and each of those ways has been found in this
/// codebase at least once.
#[derive(Debug)]
pub struct Resumption {
    /// The session's instrument, under the definition the caller gave.
    ///
    /// Checked rather than assumed: [`resume`] fails before it hands back an
    /// id the registry holds under any other definition.
    pub instrument: InstrumentId,
    /// The capital the session began with, as [`resume`] decides it.
    pub starting_cash: Notional,
    /// What the journal replays to, or `None` when it holds nothing to resume.
    ///
    /// `None` rather than an empty recompute, because `Engine::resuming`
    /// replaces the portfolio outright and an empty recompute has no capital: a
    /// rehearsal once reported "cash 0 from 0" on a session configured with a
    /// hundred. A value that is not there cannot be passed by mistake.
    pub portfolio: Option<Portfolio>,
    /// The kill switch a previous session threw, which a restart must not
    /// re-arm.
    pub tripped: Option<crate::risk::TripCause>,
    /// The next client order id to mint, for `Engine::minting_from`.
    pub next_order_id: u64,
}

/// Resume a session from its journal, onto its instrument's real definition.
///
/// **It registers first and replays second, and the order is the point.**
/// Registration is idempotent on `(exchange, symbol)`, and [`replay`] registers
/// every instrument it meets with placeholder rules -- the smallest tick and
/// lot, no minimum notional, no asset names. Replay first and the placeholders
/// are what the registry keeps; the real definition registered afterwards is
/// silently ignored. `paper` did exactly that, so a session restarted after any
/// fill ran under no venue rules at all, and its simulated venue would have
/// filled the sub-minimum orders M4.c made it refuse. A live session resumed
/// that way would lose its asset names as well, and the user-data translator
/// refuses an instrument without them.
///
/// **And it checks what registering kept**, because ordering can only beat the
/// placeholders `resume` itself would cause. The same idempotence means a
/// registry that already held the instrument before `resume` was called hands
/// back whatever it held — a placeholder from an earlier replay, or a
/// definition from somewhere else — and the one given is dropped just as
/// silently. Nothing but a comparison can see that, so `resume` compares every
/// field and fails, naming each one that differs, rather than return an id
/// that names rules the caller never gave.
///
/// It lives here rather than in `paper` because a live session must resume the
/// same way, and beside [`replay`] and `resolve` because the placeholders it
/// has to beat are registered here: the rule and the hazard it orders against
/// are read together or not at all.
///
/// `cash` is what a fresh session starts with. Once the journal holds anything
/// it is ignored, because the first `Started` entry is the authority on the
/// capital a session began with and a restart must not reset it. A journal
/// that holds entries and no `Started` at all, which nothing in this workspace
/// writes, resumes from zero, as `paper` always did.
///
/// # Errors
///
/// [`DefinitionConflict`] when `registry` already held the instrument under a
/// different definition. Nothing is replayed, and the registry is left as it
/// was.
pub fn resume(
    entries: &[JournalEntry],
    registry: &mut InstrumentRegistry,
    instrument: InstrumentDef,
    cash: Notional,
) -> Result<Resumption, DefinitionConflict> {
    // Kept, because registering consumes the definition and what the registry
    // made of it is the thing to check.
    let given = instrument.clone();
    // Before anything in the journal can register a placeholder for it.
    let instrument = registry.register(instrument);
    let fields = conflicts(registry.expect(instrument), &given);
    if !fields.is_empty() {
        return Err(DefinitionConflict {
            exchange: given.exchange,
            symbol: given.symbol,
            fields,
        });
    }
    let portfolio = (!entries.is_empty()).then(|| replay(entries, registry));
    Ok(Resumption {
        instrument,
        starting_cash: portfolio.as_ref().map_or(cash, Portfolio::starting_cash),
        portfolio,
        tripped: last_trip(entries),
        next_order_id: next_order_id(entries),
    })
}

/// The registry already held a session's instrument under a different
/// definition from the one [`resume`] was given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionConflict {
    pub exchange: Exchange,
    pub symbol: String,
    /// Every field that differs, in [`InstrumentDef`]'s order. Never empty.
    pub fields: Vec<FieldConflict>,
}

/// One field two definitions of an instrument disagree about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldConflict {
    /// The field's name in [`InstrumentDef`].
    pub field: &'static str,
    /// What the registry held, as a person would read it.
    pub registered: String,
    /// What the caller gave.
    pub given: String,
}

impl std::fmt::Display for DefinitionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} is already registered under a different definition from the one \
             this session was given, so resuming would trade under rules nobody gave it:",
            self.exchange, self.symbol
        )?;
        for (i, conflict) in self.fields.iter().enumerate() {
            write!(
                f,
                "{}{} is {} where {} was given",
                if i == 0 { " " } else { "; " },
                conflict.field,
                conflict.registered,
                conflict.given
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for DefinitionConflict {}

/// Every field on which what the registry holds differs from what was given.
fn conflicts(registered: &Instrument, given: &InstrumentDef) -> Vec<FieldConflict> {
    // Every field named, and no `..` -- the rule `replay`'s `Filled` arm learned
    // from `fee_asset`. A field added to `InstrumentDef` does not build here
    // until somebody has decided whether disagreeing about it matters.
    let InstrumentDef {
        exchange,
        symbol,
        base,
        quote,
        kind,
        tick_size,
        lot_size,
        min_notional,
    } = given;
    let mut out = Vec::new();
    // One field, compared by value and rendered only when the two differ. The
    // name is the field's own identifier, so the two cannot drift apart.
    macro_rules! compare {
        ($field:ident, $show:expr) => {
            if registered.$field != *$field {
                out.push(FieldConflict {
                    field: stringify!($field),
                    registered: $show(&registered.$field),
                    given: $show($field),
                });
            }
        };
    }
    // Quoted, so an empty asset name reads as `""` rather than as nothing; and
    // a kind by its variant's name.
    let quoted = |text: &String| format!("{text:?}");
    let named = |kind| format!("{kind:?}");
    // The key registration is idempotent on, so these two cannot differ today.
    // Compared anyway: the claim is about every field, and a registry keyed
    // some other way tomorrow should not make it quietly narrower.
    compare!(exchange, ToString::to_string);
    compare!(symbol, quoted);
    compare!(base, quoted);
    compare!(quote, quoted);
    compare!(kind, named);
    compare!(tick_size, ToString::to_string);
    compare!(lot_size, ToString::to_string);
    compare!(min_notional, ToString::to_string);
    out
}

/// The trip state a previous session left behind.
///
/// A kill switch that forgets is not a kill switch: the supervisor exists to
/// restart a dead process, so a switch held only in memory would be re-armed by
/// the machinery meant to keep things running.
fn last_trip(entries: &[JournalEntry]) -> Option<crate::risk::TripCause> {
    entries.iter().rev().find_map(|e| match e {
        JournalEntry::Tripped { cause, .. } => Some(*cause),
        // Deliberately *not* cleared by a later `Started`: a restart is exactly
        // the moment a forgotten switch would be re-armed.
        _ => None,
    })
}

/// Find or register an instrument by its persisted identity.
fn resolve(registry: &mut InstrumentRegistry, key: &InstrumentKey) -> InstrumentId {
    use quant_core::instrument::InstrumentKind;
    registry.register(InstrumentDef {
        exchange: key.exchange,
        symbol: key.symbol.clone(),
        // A journal records what we traded, not the venue's filters. Registering
        // is idempotent on `(exchange, symbol)`, so an instrument already known
        // to this process keeps the definition it was registered with -- which
        // is why `resume` registers the real definition before it replays, and
        // why these placeholders win if anything does it the other way round.
        base: String::new(),
        quote: String::new(),
        kind: InstrumentKind::Spot,
        tick_size: Px::MIN_TICK,
        lot_size: Qty::MIN_TICK,
        min_notional: Notional::ZERO,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("quant-journal-{name}-{}.jsonl", std::process::id()));
            let _ = std::fs::remove_file(&path);
            Self { path }
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn key() -> InstrumentKey {
        InstrumentKey {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
        }
    }

    fn amount(text: &str) -> Notional {
        text.parse().expect("a valid amount")
    }

    fn filled(seq: u64, side: Side, px: &str, qty: &str, fee: &str) -> JournalEntry {
        JournalEntry::Filled {
            at: Ts::from_nanos(i64::try_from(seq).expect("small")),
            client_order_id: ClientOrderId(seq),
            instrument: key(),
            side,
            px: px.parse().expect("px"),
            qty: qty.parse().expect("qty"),
            fee: fee.parse().expect("fee"),
            is_maker: false,
            fee_asset: quant_core::execution::FeeAsset::Quote,
            remaining: None,
            venue_order_id: None,
            trade_id: None,
        }
    }

    fn session() -> Vec<JournalEntry> {
        vec![
            JournalEntry::Started {
                at: Ts::from_nanos(0),
                cash: amount("100"),
                schema: SCHEMA,
                run: None,
            },
            filled(1, Side::Buy, "76650", "0.001", "0.0766"),
            filled(2, Side::Sell, "76700", "0.001", "0.0767"),
            JournalEntry::Stopped {
                at: Ts::from_nanos(3),
                working: Vec::new(),
            },
        ]
    }

    fn write_all(path: &Path, entries: &[JournalEntry]) {
        let mut journal = Journal::open(path).expect("open");
        for entry in entries {
            journal.append(entry).expect("append");
        }
    }

    #[test]
    fn a_journal_round_trips() {
        let scratch = Scratch::new("round-trip");
        write_all(&scratch.path, &session());
        let recovered = read(&scratch.path).expect("read");
        assert_eq!(recovered.entries, session());
        assert!(!recovered.torn_tail);
    }

    #[test]
    fn a_missing_journal_is_empty_rather_than_an_error() {
        // The first run of a session has not written one yet.
        let scratch = Scratch::new("absent");
        let recovered = read(&scratch.path).expect("a missing file is fine");
        assert!(recovered.entries.is_empty(), "{:?}", recovered.entries);
    }

    #[test]
    fn appending_to_an_existing_journal_keeps_what_was_there() {
        // A restart appends; it does not start a new file. Truncating would lose
        // the position the restart exists to recover.
        let scratch = Scratch::new("append");
        write_all(&scratch.path, &session());
        write_all(
            &scratch.path,
            &[JournalEntry::Started {
                at: Ts::from_nanos(10),
                cash: amount("64"),
                schema: SCHEMA,
                run: None,
            }],
        );
        let recovered = read(&scratch.path).expect("read");
        assert_eq!(recovered.entries.len(), 5);
    }

    #[test]
    fn the_first_started_entry_is_the_authority_on_starting_capital() {
        // A restart writes its own Started with whatever it had. The number a
        // report quotes has to be what the *session* began with, or a run that
        // lost money would look like it started poorer.
        let mut restarted = session();
        restarted.push(JournalEntry::Started {
            at: Ts::from_nanos(10),
            cash: amount("64"),
            schema: SCHEMA,
            run: None,
        });
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&restarted, &mut registry);
        assert_eq!(portfolio.starting_cash(), amount("100"));
    }

    #[test]
    fn a_recomputed_portfolio_matches_one_built_by_applying_the_same_fills() {
        // The independent recompute. It shares no state with a running engine
        // and has to reach the same numbers -- two tallies of one quantity, the
        // same shape as the venue and the portfolio each totalling fees.
        let entries = session();
        let mut registry = InstrumentRegistry::new();
        let recomputed = replay(&entries, &mut registry);

        let mut direct = Portfolio::new(amount("100"));
        let id = resolve(&mut registry, &key());
        direct.apply_fill(
            id,
            Side::Buy,
            &Fill {
                px: "76650".parse().expect("px"),
                qty: "0.001".parse().expect("qty"),
                fee: amount("0.0766"),
                is_maker: false,
                fee_asset: quant_core::execution::FeeAsset::Quote,
            },
        );
        direct.apply_fill(
            id,
            Side::Sell,
            &Fill {
                px: "76700".parse().expect("px"),
                qty: "0.001".parse().expect("qty"),
                fee: amount("0.0767"),
                is_maker: false,
                fee_asset: quant_core::execution::FeeAsset::Quote,
            },
        );

        assert_eq!(recomputed.cash(), direct.cash());
        assert_eq!(recomputed.realized(), direct.realized());
        assert_eq!(recomputed.fees(), direct.fees());
        assert_eq!(recomputed.position(id), direct.position(id));
    }

    #[test]
    fn a_fee_taken_in_the_base_replays_as_the_engine_booked_it() {
        // Binance takes a buyer's fee in what was bought, so the position
        // arrives a fraction smaller and the cash does not pay the fee at all.
        // `JournalWriter` has written the asset down since PR #50; replay threw
        // it away and folded every fee as quote, which left the recompute's
        // cash short by the fee and its position long by it -- and `agrees`
        // then reported the engine's own, correct checkpoint as a disagreement.
        // A restart resumes from this fold, so the strategy would have come
        // back up holding a position it did not have.
        let buy = Fill {
            px: "76650".parse().expect("px"),
            qty: "0.001".parse().expect("qty"),
            // Ten basis points of what was bought, in BTC.
            fee: "0.000001".parse().expect("a fee in base units"),
            is_maker: false,
            fee_asset: quant_core::execution::FeeAsset::Base,
        };
        let mut registry = InstrumentRegistry::new();
        let id = resolve(&mut registry, &key());
        let mut live = Portfolio::new(amount("100"));
        live.apply_fill(id, Side::Buy, &buy);

        let entries = vec![
            JournalEntry::Started {
                at: Ts::from_nanos(0),
                cash: amount("100"),
                schema: SCHEMA,
                run: None,
            },
            JournalEntry::Filled {
                at: Ts::from_nanos(1),
                client_order_id: ClientOrderId(1),
                instrument: key(),
                side: Side::Buy,
                px: buy.px,
                qty: buy.qty,
                fee: buy.fee,
                is_maker: buy.is_maker,
                fee_asset: buy.fee_asset,
                remaining: None,
                venue_order_id: None,
                trade_id: None,
            },
            // What the engine writes down after booking that fill.
            JournalEntry::checkpoint(Ts::from_nanos(2), &live),
        ];

        let replayed = replay(&entries, &mut registry);
        assert_eq!(replayed.cash(), live.cash(), "the fee is not paid in cash");
        assert_eq!(
            replayed.position(id),
            live.position(id),
            "it comes out of the position instead"
        );
        assert_eq!(replayed.fees(), live.fees(), "and still counts as money");
        assert_eq!(replayed, live, "exactly the portfolio the engine held");
        assert_eq!(agrees(&entries), Agreement::Agrees);
    }

    #[test]
    fn a_round_trip_through_the_journal_ends_flat_and_pays_its_fees() {
        // The arithmetic, checked end to end: bought and sold one unit 50 apart,
        // so 0.05 of gross gain against 0.1533 of fees.
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&session(), &mut registry);
        assert!(portfolio.position(resolve(&mut registry, &key())).is_flat());
        assert_eq!(portfolio.realized(), amount("0.05"));
        assert_eq!(portfolio.fees(), amount("0.1533"));
        assert_eq!(portfolio.cash(), amount("99.8967"));
    }

    #[test]
    fn a_torn_last_line_is_reported_rather_than_failing() {
        // A process killed mid-write leaves a partial last line. Same
        // distinction quant-storage draws between an interrupted write and
        // corruption -- and a paper run must come back up after a hard kill.
        let scratch = Scratch::new("torn");
        write_all(&scratch.path, &session());
        let mut bytes = std::fs::read(&scratch.path).expect("read");
        bytes.extend_from_slice(b"{\"type\":\"fil");
        std::fs::write(&scratch.path, &bytes).expect("write");

        let recovered = read(&scratch.path).expect("a torn tail is not an error");
        assert!(recovered.torn_tail);
        assert_eq!(
            recovered.entries,
            session(),
            "everything before it survives"
        );
    }

    #[test]
    fn a_restart_after_a_torn_write_keeps_every_line_and_counts_the_torn_one() {
        // A hard kill mid-write leaves a partial last line, and the supervisor
        // restarts the process. The restart used to append straight after the
        // fragment, gluing its own `started` onto it: a bad line in the middle,
        // which `read` refuses as damage. From then on the journal was
        // unreadable -- to `reconcile`, and to the next restart, which is when
        // it is needed most.
        let scratch = Scratch::new("torn-then-restarted");
        write_all(&scratch.path, &session());
        let mut bytes = std::fs::read(&scratch.path).expect("read");
        bytes.extend_from_slice(b"{\"type\":\"fil");
        std::fs::write(&scratch.path, &bytes).expect("write");

        // What a restart does: open for append, write its `Started`, carry on.
        let restarted = JournalEntry::Started {
            at: Ts::from_nanos(10),
            cash: amount("100"),
            schema: SCHEMA,
            run: None,
        };
        let after = filled(11, Side::Buy, "76800", "0.001", "0.0768");
        write_all(&scratch.path, &[restarted.clone(), after.clone()]);

        let recovered = read(&scratch.path).expect("a torn line a restart followed is not damage");
        let mut expected = session();
        expected.push(restarted);
        expected.push(after);
        assert_eq!(
            recovered.entries, expected,
            "every whole entry survives, both starts included"
        );
        assert_eq!(
            recovered.torn_before_restart,
            vec![5],
            "the torn line is reported where it is, not skipped in silence"
        );
        assert!(!recovered.torn_tail, "the tail itself is whole");

        // Every byte kept: the fragment is still there, terminated rather than
        // overwritten or removed.
        let text = std::fs::read_to_string(&scratch.path).expect("read");
        assert!(text.contains("{\"type\":\"fil\n"), "{text}");
    }

    #[test]
    fn a_write_cut_through_a_multibyte_character_is_torn_like_any_other() {
        // A note's detail is free text, so an entry can hold a character UTF-8
        // spells in more than one byte, and a crash can cut the write between
        // them. The line is then not UTF-8 at all. Read as text, that refused
        // the whole journal before the torn-line rule could judge the line.
        let scratch = Scratch::new("torn-mid-character");
        write_all(&scratch.path, &session());
        let note = JournalEntry::Note {
            at: Ts::from_nanos(9),
            kind: "claim".to_owned(),
            detail: serde_json::json!({ "why": "prix moyen à 76 800" }),
        };
        let mut line = serde_json::to_vec(&note).expect("serialize");
        // `à` is 0xC3 0xA0; keep the first byte and lose the second.
        let cut = line
            .iter()
            .position(|&b| b == 0xC3)
            .expect("a two-byte character")
            + 1;
        line.truncate(cut);
        let mut bytes = std::fs::read(&scratch.path).expect("read");
        bytes.extend_from_slice(&line);
        std::fs::write(&scratch.path, &bytes).expect("write");
        assert!(
            std::str::from_utf8(&bytes).is_err(),
            "the fixture must not be valid UTF-8, or it tests nothing new"
        );

        let recovered = read(&scratch.path).expect("a write cut mid-character is a torn tail");
        assert!(recovered.torn_tail);
        assert_eq!(recovered.entries, session());

        // And once a restart has written past it.
        let restarted = JournalEntry::Started {
            at: Ts::from_nanos(10),
            cash: amount("100"),
            schema: SCHEMA,
            run: None,
        };
        write_all(&scratch.path, std::slice::from_ref(&restarted));
        let recovered = read(&scratch.path).expect("a restart after it is not damage");
        assert_eq!(recovered.torn_before_restart, vec![5]);
        assert!(!recovered.torn_tail);
        let mut expected = session();
        expected.push(restarted);
        assert_eq!(recovered.entries, expected);
    }

    #[test]
    fn opening_a_journal_that_ends_cleanly_adds_nothing_to_it() {
        // The newline `open` writes is for a torn fragment and nothing else.
        // Written every time, each restart would leave a blank line behind:
        // harmless to `read`, and noise in the one file a person reads by hand.
        let whole = Scratch::new("reopened-whole");
        write_all(&whole.path, &session());
        let before = std::fs::read(&whole.path).expect("read");
        drop(Journal::open(&whole.path).expect("open"));
        assert_eq!(std::fs::read(&whole.path).expect("read"), before);

        // Nor does a journal that does not exist yet start with one.
        let fresh = Scratch::new("reopened-fresh");
        drop(Journal::open(&fresh.path).expect("open"));
        assert_eq!(std::fs::read(&fresh.path).expect("read"), Vec::<u8>::new());
    }

    #[test]
    fn a_bad_line_in_the_middle_is_an_error() {
        // A torn line ends up in the middle only when a restart wrote past it,
        // and a restart always begins with `started`. Anything else in the
        // middle of an append-only file is damage rather than an interrupted
        // write -- and silently skipping it would drop a fill and quietly
        // change the position.
        let scratch = Scratch::new("corrupt-middle");
        write_all(&scratch.path, &session());
        let mut text = std::fs::read_to_string(&scratch.path).expect("read");
        text.push_str("not json\n");
        text.push_str("{\"type\":\"stopped\",\"at\":9}\n");
        std::fs::write(&scratch.path, text).expect("write");

        let err = read(&scratch.path).expect_err("damage must be loud");
        assert!(err.to_string().contains("only the last line may be torn"));
    }

    /// The restart that follows a torn write: what `paper` does on coming
    /// back up, which is to open the journal and write its `Started`.
    fn restart(path: &Path) -> JournalEntry {
        let started = JournalEntry::Started {
            at: Ts::from_nanos(10),
            cash: amount("100"),
            schema: SCHEMA,
            run: None,
        };
        write_all(path, std::slice::from_ref(&started));
        started
    }

    /// A whole line a newer build might write: valid JSON, and an entry type
    /// this build has never heard of.
    const FROM_A_NEWER_BUILD: &str = "{\"type\":\"venue_trade\",\"at\":5}\n";

    #[test]
    fn a_run_of_nul_bytes_reads_as_an_interrupted_write_before_and_after_a_restart() {
        // The other shape a lost write can read back as. A power loss can put
        // the file's new length on the disk before its data, and the bytes that
        // never arrived read back as NULs -- not JSON at all, where a hard
        // kill leaves JSON cut short. Both are the shape an interrupted write
        // leaves, and both must survive the restart that follows.
        let scratch = Scratch::new("nul-run");
        write_all(&scratch.path, &session());
        let mut bytes = std::fs::read(&scratch.path).expect("read");
        bytes.extend_from_slice(&[0_u8; 64]);
        std::fs::write(&scratch.path, &bytes).expect("write");

        let recovered = read(&scratch.path).expect("a run of NULs at the end is a torn tail");
        assert!(recovered.torn_tail);
        assert_eq!(
            recovered.entries,
            session(),
            "everything before it survives"
        );

        let started = restart(&scratch.path);
        let recovered = read(&scratch.path).expect("and a restart written past it is not damage");
        let mut expected = session();
        expected.push(started);
        assert_eq!(recovered.entries, expected);
        assert_eq!(
            recovered.torn_before_restart,
            vec![5],
            "listed where it is, like a line cut short"
        );
        assert!(!recovered.torn_tail);
    }

    #[test]
    fn a_whole_line_this_build_cannot_read_before_a_restart_is_an_error_naming_it() {
        // Valid JSON that fails only on what it says -- here an entry type a
        // newer build wrote. No interrupted write produces that: cut short is
        // not valid JSON, and neither are NULs. Tolerated because a `Started`
        // happened to follow it, it was reported as a torn write and the entry
        // was discarded under that name.
        let scratch = Scratch::new("unknown-type-then-restarted");
        write_all(&scratch.path, &session());
        let mut text = std::fs::read_to_string(&scratch.path).expect("read");
        text.push_str(FROM_A_NEWER_BUILD);
        std::fs::write(&scratch.path, text).expect("write");
        restart(&scratch.path);

        let err = read(&scratch.path).expect_err("an entry this build cannot read is not torn");
        let message = err.to_string();
        assert!(message.contains("journal line 5 "), "{message}");
        assert!(message.contains("venue_trade"), "{message}");
    }

    #[test]
    fn a_whole_line_this_build_cannot_read_at_the_end_is_an_error_not_a_torn_tail() {
        // The same line as the last one. Discarded here as a torn tail, it would
        // be written past by the restart that read it -- leaving the line the
        // test above refuses in the middle of the file, and a journal that
        // every reader after the restart refuses: `reconcile`, `explain`, the
        // session's own closing check, and the next restart.
        let scratch = Scratch::new("unknown-type-at-the-end");
        write_all(&scratch.path, &session());
        let mut text = std::fs::read_to_string(&scratch.path).expect("read");
        text.push_str(FROM_A_NEWER_BUILD);
        std::fs::write(&scratch.path, text).expect("write");

        let err = read(&scratch.path).expect_err("a whole line is not a torn tail");
        let message = err.to_string();
        assert!(message.contains("journal line 5 "), "{message}");
        assert!(message.contains("venue_trade"), "{message}");
    }

    #[test]
    fn an_instrument_is_named_by_exchange_and_symbol_not_by_index() {
        // M0's rule. An id written on Tuesday means something else on Wednesday
        // if the registration order changed -- so the journal must survive being
        // replayed by a process that registered things in a different order.
        let entries = session();

        let mut first = InstrumentRegistry::new();
        let a = replay(&entries, &mut first);

        let mut second = InstrumentRegistry::new();
        // Register something else first, so the same symbol gets a different id.
        let _ = resolve(
            &mut second,
            &InstrumentKey {
                exchange: Exchange::Binance,
                symbol: "ETHUSDT".to_owned(),
            },
        );
        let b = replay(&entries, &mut second);

        assert_eq!(a.cash(), b.cash());
        assert_eq!(a.realized(), b.realized());
        let id_a = resolve(&mut first, &key());
        let id_b = resolve(&mut second, &key());
        assert_ne!(id_a, id_b, "the ids really do differ between processes");
        assert_eq!(a.position(id_a), b.position(id_b));
    }

    /// BTCUSDT as the venue defines it -- which is what a journal cannot say.
    fn btcusdt() -> InstrumentDef {
        InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: quant_core::instrument::InstrumentKind::Spot,
            tick_size: "0.01".parse().expect("tick"),
            lot_size: "0.00001".parse().expect("lot"),
            min_notional: "5".parse().expect("notional"),
        }
    }

    #[test]
    fn resuming_a_journal_that_holds_fills_keeps_the_real_instrument_definition() {
        // Registration is idempotent on (exchange, symbol), and replay registers
        // a placeholder for every instrument it meets. `paper` replayed first
        // and registered second, so after a restart with fills the placeholder
        // won -- the smallest tick and lot, no minimum notional, no asset names
        // -- and the real definition was dropped without a word.
        let real = btcusdt();
        let mut registry = InstrumentRegistry::new();
        let resumed = resume(&session(), &mut registry, real.clone(), amount("100"))
            .expect("a fresh registry holds nothing to conflict with");
        assert!(
            resumed.portfolio.is_some(),
            "the journal holds fills, so this is a resume"
        );

        let kept = registry.expect(resumed.instrument);
        assert_eq!(kept.tick_size, real.tick_size);
        assert_eq!(kept.lot_size, real.lot_size);
        assert_eq!(kept.min_notional, real.min_notional);
        assert_eq!(kept.base, real.base);
        assert_eq!(kept.quote, real.quote);
    }

    #[test]
    fn a_fresh_journal_resumes_nothing_and_starts_with_the_configured_cash() {
        // An empty journal replays to a portfolio with no capital, and
        // `Engine::resuming` replaces the portfolio outright -- the rehearsal
        // that reported "cash 0 from 0" on a session configured with a hundred.
        let mut registry = InstrumentRegistry::new();
        let resumed = resume(&[], &mut registry, btcusdt(), amount("100")).expect("resumes");
        assert!(resumed.portfolio.is_none(), "nothing to resume");
        assert_eq!(resumed.starting_cash, amount("100"));
        assert_eq!(resumed.next_order_id, 1);
        assert_eq!(resumed.tripped, None);
    }

    #[test]
    fn a_restart_resumes_from_the_capital_the_session_began_with() {
        // The first `Started` wins: not the restart's own, and not whatever this
        // process happened to be configured with. A run that lost money must
        // not look like it began poorer.
        let mut restarted = session();
        restarted.push(JournalEntry::Started {
            at: Ts::from_nanos(10),
            cash: amount("64"),
            schema: SCHEMA,
            run: None,
        });
        let mut registry = InstrumentRegistry::new();
        let resumed = resume(&restarted, &mut registry, btcusdt(), amount("50")).expect("resumes");
        assert_eq!(resumed.starting_cash, amount("100"));
        assert_eq!(
            resumed.next_order_id, 3,
            "ids carry on from the journal's rather than starting again at 1"
        );
    }

    #[test]
    fn a_kill_switch_thrown_before_a_restart_is_still_thrown_after_it() {
        // The supervisor restarts a dead process, so a switch held only in
        // memory would be re-armed by the machinery meant to keep things
        // running. The restart's own `Started` must not clear it.
        let mut entries = session();
        entries.push(JournalEntry::Tripped {
            at: Ts::from_nanos(4),
            cause: crate::risk::TripCause::DailyLoss,
        });
        entries.push(JournalEntry::Started {
            at: Ts::from_nanos(5),
            cash: amount("99"),
            schema: SCHEMA,
            run: None,
        });
        let mut registry = InstrumentRegistry::new();
        let resumed = resume(&entries, &mut registry, btcusdt(), amount("100")).expect("resumes");
        assert_eq!(resumed.tripped, Some(crate::risk::TripCause::DailyLoss));
    }

    #[test]
    fn resuming_onto_a_placeholder_already_registered_fails_naming_each_difference() {
        // Registration is idempotent on (exchange, symbol), so a registry that
        // already holds the symbol hands back what it holds and drops the
        // definition given -- and `replay` registers a placeholder for every
        // instrument it meets. Resuming onto one would hand back an id under
        // the smallest tick and lot and no minimum notional while
        // `Resumption::instrument` claimed the caller's definition.
        let mut registry = InstrumentRegistry::new();
        let placeholder = resolve(&mut registry, &key());

        let conflict = resume(&session(), &mut registry, btcusdt(), amount("100"))
            .expect_err("a placeholder is not the definition the caller gave");

        assert_eq!(conflict.exchange, Exchange::Binance);
        assert_eq!(conflict.symbol, "BTCUSDT");
        let fields: Vec<&str> = conflict.fields.iter().map(|c| c.field).collect();
        assert_eq!(
            fields,
            ["base", "quote", "tick_size", "lot_size", "min_notional"],
            "every rule the placeholder lacks, and nothing the two share"
        );
        let text = conflict.to_string();
        assert!(text.contains("binance BTCUSDT"), "{text}");
        assert!(
            text.contains(r#"base is "" where "BTC" was given"#),
            "{text}"
        );
        assert!(
            text.contains("min_notional is 0 where 5 was given"),
            "{text}"
        );
        assert_eq!(
            registry.expect(placeholder).min_notional,
            Notional::ZERO,
            "the registry is left as it was"
        );
    }

    #[test]
    fn resuming_onto_the_same_definition_already_registered_is_not_a_conflict() {
        // The other side of that boundary. A caller that registered the real
        // definition before resuming -- a live session that read the venue's
        // rules first -- is resuming onto exactly what it gave.
        let mut registry = InstrumentRegistry::new();
        let registered = registry.register(btcusdt());
        let resumed = resume(&session(), &mut registry, btcusdt(), amount("100"))
            .expect("the same definition twice is not a conflict");
        assert_eq!(resumed.instrument, registered);
        assert!(resumed.portfolio.is_some(), "and the journal still replays");
    }

    #[test]
    fn an_empty_journal_recomputes_to_nothing_rather_than_guessing() {
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&[], &mut registry);
        assert_eq!(portfolio.starting_cash(), Notional::ZERO);
        assert_eq!(portfolio.fills(), 0);
    }
    #[test]
    fn a_checkpoint_that_matches_the_recompute_agrees() {
        // Two independent computations of one quantity: the engine accumulated
        // its numbers in memory, this replayed them from the file.
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&entries, &mut registry);
        entries.push(JournalEntry::checkpoint(Ts::from_nanos(4), &portfolio));

        assert_eq!(agrees(&entries), Agreement::Agrees);
    }

    #[test]
    fn a_journal_with_no_checkpoint_reports_nothing_to_check() {
        // Deliberately not agreement. "Nobody disagreed" and "two answers
        // matched" are different statements and only one of them is evidence.
        assert_eq!(agrees(&session()), Agreement::NothingToCheck);
    }

    #[test]
    fn a_checkpoint_claiming_the_wrong_cash_disagrees() {
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&entries, &mut registry);
        entries.push(JournalEntry::Checkpoint {
            at: Ts::from_nanos(4),
            cash: amount("999"),
            realized: portfolio.realized(),
            fees: portfolio.fees(),
            fills: portfolio.fills(),
        });
        assert_eq!(
            agrees(&entries),
            Agreement::Differs {
                field: "cash",
                claimed: amount("999"),
                recomputed: portfolio.cash(),
            }
        );
    }

    #[test]
    fn a_fill_acted_on_but_never_written_down_is_caught() {
        // The failure this whole entry exists for: the engine traded, the line
        // did not reach the disk, and the position on restart would be wrong.
        // Checked before the money fields, because the fill count localises it.
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&entries, &mut registry);
        entries.push(JournalEntry::Checkpoint {
            at: Ts::from_nanos(4),
            cash: portfolio.cash(),
            realized: portfolio.realized(),
            fees: portfolio.fees(),
            fills: portfolio.fills() + 1,
        });
        assert_eq!(
            agrees(&entries),
            Agreement::FillCountDiffers {
                claimed: portfolio.fills() + 1,
                recomputed: portfolio.fills(),
            }
        );
    }

    #[test]
    fn fills_after_the_last_checkpoint_are_not_a_disagreement() {
        // The false alarm that would have turned a fortnight red every six
        // hours. `ops/verify-loop.sh` runs `reconcile` *during* a run, against a
        // journal the engine is still appending to, so fills after the last
        // checkpoint are the ordinary state of a healthy live file -- not
        // evidence that one was acted on and never written down.
        //
        // Seen for real before it was fixed: a five-minute rehearsal reported
        // OK, then "DISAGREES on fills: the engine counted 5, the journal holds
        // 8", then "counted 10, holds 11", on consecutive passes as the file
        // grew. The claim was right, the recompute was right, and the comparison
        // was between a claim about fill 5 and a replay of eight.
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let at_checkpoint = replay(&entries, &mut registry);
        entries.push(JournalEntry::checkpoint(Ts::from_nanos(4), &at_checkpoint));

        // The run carries on. These are good fills, correctly written down.
        let more = session();
        entries.extend(
            more.into_iter()
                .filter(|e| matches!(e, JournalEntry::Filled { .. })),
        );
        assert!(
            replay(&entries, &mut registry).fills() > at_checkpoint.fills(),
            "the journal really has grown past the checkpoint"
        );

        assert_eq!(agrees(&entries), Agreement::Agrees);
    }

    #[test]
    fn the_last_checkpoint_is_the_one_that_counts() {
        // A fortnight writes many. An earlier one describes an earlier state and
        // comparing against it would fail on every healthy run.
        let mut entries = session();
        let mut registry = InstrumentRegistry::new();
        let mid = replay(&entries, &mut registry);
        entries.push(JournalEntry::Checkpoint {
            at: Ts::from_nanos(4),
            cash: amount("1"),
            realized: amount("1"),
            fees: amount("1"),
            fills: 99,
        });
        entries.push(JournalEntry::checkpoint(Ts::from_nanos(5), &mid));
        assert_eq!(agrees(&entries), Agreement::Agrees);
    }

    #[test]
    fn a_checkpoint_is_built_from_the_portfolio_it_describes() {
        // Built from the portfolio rather than from fields a caller assembles,
        // so the claim and the thing claimed cannot drift apart.
        let mut registry = InstrumentRegistry::new();
        let portfolio = replay(&session(), &mut registry);
        let JournalEntry::Checkpoint {
            cash,
            realized,
            fees,
            fills,
            ..
        } = JournalEntry::checkpoint(Ts::from_nanos(1), &portfolio)
        else {
            panic!("expected a checkpoint");
        };
        assert_eq!(cash, portfolio.cash());
        assert_eq!(realized, portfolio.realized());
        assert_eq!(fees, portfolio.fees());
        assert_eq!(fills, portfolio.fills());
    }
}

#[cfg(test)]
mod m75a_tests {
    use super::{next_order_id, read, replay, JournalEntry, SCHEMA};
    use quant_core::execution::ClientOrderId;
    use quant_core::instrument::InstrumentRegistry;
    use quant_core::time::Ts;
    use std::io::Write as _;

    /// A journal written before M7.5 existed, byte for byte.
    const PRE_M75: &str = concat!(
        r#"{"type":"started","at":0,"cash":10000000000}"#,
        "\n",
        r#"{"type":"filled","at":1000,"client_order_id":1,"instrument":{"exchange":"binance","symbol":"BTCUSDT"},"side":"buy","px":10000000000,"qty":100000,"fee":1000,"is_maker":false}"#,
        "\n",
    );

    fn temp(name: &str, body: &str) -> std::path::PathBuf {
        let thread = std::thread::current();
        let unique = thread.name().unwrap_or("unnamed").replace("::", "-");
        let path = std::env::temp_dir().join(format!(
            "quant-m75a-{name}-{unique}-{}.jsonl",
            std::process::id()
        ));
        let mut f = std::fs::File::create(&path).expect("create");
        f.write_all(body.as_bytes()).expect("write");
        path
    }

    #[test]
    fn a_journal_written_before_the_schema_field_still_reads() {
        // The compatibility the `serde(default)` buys, pinned rather than
        // assumed. The fortnight's two journals are this shape, and they are
        // the only live evidence M5 ever produced — a reader that could not
        // open them would make the milestone's result unverifiable.
        let recovered = read(&temp("pre-m75", PRE_M75)).expect("readable");
        assert_eq!(recovered.entries.len(), 2);
        let JournalEntry::Started { schema, cash, .. } = &recovered.entries[0] else {
            panic!("the first entry is a start")
        };
        assert_eq!(*schema, 1, "absent means the shape before M7.5");
        assert_eq!(cash.to_string(), "100");
    }

    /// The fortnight's shape: schema 1, money and marks only.
    const M5_SHAPED: &str = concat!(
        r#"{"type":"started","at":0,"cash":10000000000}"#,
        "\n",
        r#"{"type":"filled","at":1789744915849764000,"client_order_id":1,"instrument":{"exchange":"binance","symbol":"BTCUSDT"},"side":"buy","px":8085625000000,"qty":100000,"fee":8085625,"is_maker":false}"#,
        "\n",
        r#"{"type":"checkpoint","at":1789744915849764000,"cash":9991906290,"realized":0,"fees":8085625,"fills":1}"#,
        "\n",
        r#"{"type":"stopped","at":1789744915849764000}"#,
        "\n",
    );

    /// A session of schema `n`, which writes nothing a schema-2 build could
    /// not read: the refusal must come from the number, not from a line.
    fn begun_in(n: u32) -> String {
        format!(
            "{{\"type\":\"started\",\"at\":5,\"cash\":10000000000,\"schema\":{n}}}\n\
             {{\"type\":\"stopped\",\"at\":6}}\n"
        )
    }

    #[test]
    fn a_session_begun_in_a_newer_schema_is_refused_naming_both_numbers() {
        // What the field was added for at M7.5. Without the refusal, a schema-4
        // file whose lines all happen to parse would be read as though this
        // build understood them, and one whose lines do not would be reported
        // as damage at the first unfamiliar entry, which says the wrong thing.
        let err = read(&temp("schema-4", &begun_in(SCHEMA + 1)))
            .expect_err("a newer schema is refused, not guessed at");
        let message = err.to_string();
        assert!(message.contains("journal line 1 "), "{message}");
        assert!(
            message.contains(&format!("schema {}", SCHEMA + 1)),
            "names the file's schema: {message}"
        );
        assert!(
            message.contains(&format!("schema {SCHEMA} and older")),
            "and this build's: {message}"
        );

        // A restart under a newer build, after sessions this build wrote, is
        // refused at that restart's line.
        let mixed = format!("{}{}", begun_in(SCHEMA), begun_in(SCHEMA + 1));
        let err = read(&temp("schema-mixed", &mixed)).expect_err("still refused");
        assert!(err.to_string().contains("journal line 3 "), "{err}");
    }

    #[test]
    fn every_schema_up_to_this_one_still_reads() {
        // The other side of the boundary. The fortnight's journals are schema
        // 1, and M7.5's are schema 2, and both are evidence that must stay
        // readable for as long as the result they support is quoted.
        let pre = read(&temp("still-pre-m75", PRE_M75)).expect("schema 1, pre-M7.5");
        assert_eq!(pre.entries.len(), 2);
        let m5 = read(&temp("still-m5", M5_SHAPED)).expect("schema 1, M5's shape");
        assert_eq!(m5.entries.len(), 4);
        for n in 2..=SCHEMA {
            let file = read(&temp(&format!("still-{n}"), &begun_in(n)))
                .unwrap_or_else(|e| panic!("schema {n} reads: {e}"));
            assert!(
                matches!(file.entries[0], JournalEntry::Started { schema, .. } if schema == n),
                "{:?}",
                file.entries
            );
        }
    }

    #[test]
    fn the_run_tag_is_the_first_session_s_and_an_empty_journal_has_none() {
        // J9. The tag names this run's orders at the venue, where every run's
        // client ids share one namespace, so a restart must not rename them:
        // the first `started` is the authority, as it is for the capital.
        let started = |at: i64, run: Option<&str>| JournalEntry::Started {
            at: Ts::from_nanos(at),
            cash: "100".parse().expect("amount"),
            schema: SCHEMA,
            run: run.map(str::to_owned),
        };
        let entries = vec![
            started(1, Some("r7")),
            filled(1),
            started(5, Some("r8")),
            filled(2),
        ];
        assert_eq!(
            super::run_tag(&entries),
            Some("r7"),
            "the first, not the last"
        );
        assert_eq!(super::run_tag(&[]), None, "an empty journal has no tag");
        assert_eq!(
            super::run_tag(&[filled(1)]),
            None,
            "nor does one with no `started` line"
        );
        // The first line is the authority even when it has no tag: a later
        // one does not supply what the session never had.
        assert_eq!(
            super::run_tag(&[started(1, None), started(5, Some("r8"))]),
            None
        );
    }

    #[test]
    fn a_run_tag_is_written_only_when_there_is_one() {
        // Old files read, and a run without a tag writes the line it always
        // wrote: the tag is an addition, not a change to the line.
        let untagged = JournalEntry::Started {
            at: Ts::from_nanos(5),
            cash: "100".parse().expect("amount"),
            schema: SCHEMA,
            run: None,
        };
        assert_eq!(
            serde_json::to_string(&untagged).expect("serialize"),
            format!(r#"{{"type":"started","at":5,"cash":10000000000,"schema":{SCHEMA}}}"#)
        );
        let tagged = JournalEntry::Started {
            at: Ts::from_nanos(5),
            cash: "100".parse().expect("amount"),
            schema: SCHEMA,
            run: Some("r7".to_owned()),
        };
        let line = serde_json::to_string(&tagged).expect("serialize");
        assert_eq!(
            line,
            format!(
                r#"{{"type":"started","at":5,"cash":10000000000,"schema":{SCHEMA},"run":"r7"}}"#
            )
        );
        assert_eq!(
            serde_json::from_str::<JournalEntry>(&line).expect("reads back"),
            tagged
        );
    }

    #[test]
    fn the_schema_this_build_writes_is_not_the_one_it_defaults_to() {
        // Otherwise the field is decoration: every journal would report the
        // same number whether or not the writer knew about it, and the next
        // change would have nothing to refuse at.
        assert_ne!(SCHEMA, 1);
    }

    #[test]
    fn ids_continue_across_a_restart_rather_than_repeating() {
        // `next_id` resets to 1 with the process, and `resuming` replaces only
        // the portfolio. Without recovery a resumed run mints id 1 again and the
        // file holds two different orders claiming it — which would make M7.5's
        // hole check report holes that are not there, the "cries wolf on good
        // data" failure the verifier already taught this project once.
        let entries = vec![
            JournalEntry::Started {
                at: Ts::from_nanos(1),
                cash: "100".parse().expect("amount"),
                schema: SCHEMA,
                run: None,
            },
            filled(7),
            filled(9),
        ];
        assert_eq!(next_order_id(&entries), 10, "max + 1, not a count");
    }

    #[test]
    fn an_empty_journal_mints_from_one() {
        // So a first session needs no special case.
        assert_eq!(next_order_id(&[]), 1);
    }

    #[test]
    fn the_id_recovered_is_the_highest_not_the_last() {
        // Entries are appended in time order, but ids are only dense, not
        // sorted: a fill for an older order can land after a newer one once
        // latency is on. Taking the last would then hand out an id already used.
        let entries = vec![filled(9), filled(7)];
        assert_eq!(next_order_id(&entries), 10);
    }

    #[test]
    fn replaying_a_journal_ignores_everything_that_is_not_a_fill() {
        // The exhaustive match's behaviour, stated as a property rather than
        // left to the compiler: a checkpoint is a *claim about* the fold and
        // folding it back in would make the check compare itself.
        let mut registry = InstrumentRegistry::new();
        let fills_only = vec![filled(1)];
        let with_noise = vec![
            filled(1),
            JournalEntry::Checkpoint {
                at: Ts::from_nanos(5),
                cash: "999".parse().expect("amount"),
                realized: "999".parse().expect("amount"),
                fees: "999".parse().expect("amount"),
                fills: 99,
            },
            JournalEntry::Stopped {
                at: Ts::from_nanos(6),
                working: Vec::new(),
            },
        ];
        let a = replay(&fills_only, &mut registry);
        let b = replay(&with_noise, &mut InstrumentRegistry::new());
        assert_eq!(a.cash(), b.cash());
        assert_eq!(a.fills(), b.fills());
    }

    fn filled(id: u64) -> JournalEntry {
        JournalEntry::Filled {
            at: Ts::from_nanos(i64::try_from(id).expect("small") * 1_000),
            client_order_id: ClientOrderId(id),
            instrument: super::InstrumentKey {
                exchange: quant_core::instrument::Exchange::Binance,
                symbol: "BTCUSDT".to_owned(),
            },
            side: quant_core::event::Side::Buy,
            px: "100".parse().expect("px"),
            qty: "0.001".parse().expect("qty"),
            fee: "0.0001".parse().expect("fee"),
            is_maker: false,
            fee_asset: quant_core::execution::FeeAsset::Quote,
            remaining: None,
            venue_order_id: None,
            trade_id: None,
        }
    }
}

#[cfg(test)]
mod schema3_tests {
    //! What schema 3 added, pinned as bytes: every new field written with
    //! distinct, non-zero values (J1), and every line that carries none of
    //! them written exactly as schema 2 wrote it (J2).

    use super::{InstrumentKey, JournalEntry};
    use crate::risk::Bound;
    use crate::RefusedBy;
    use quant_core::event::Side;
    use quant_core::execution::{
        ClientOrderId, FeeAsset, RejectReason, VenueOrderId, VenueTradeId,
    };
    use quant_core::instrument::{Exchange, FilterBreach};
    use quant_core::report::{Cited, EndedBy, VenueText};
    use quant_core::time::Ts;

    fn key() -> InstrumentKey {
        InstrumentKey {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
        }
    }

    const KEY: &str = r#"{"exchange":"binance","symbol":"BTCUSDT"}"#;

    /// `entry` writes exactly `json`, and `json` reads back as `entry`.
    fn golden(entry: &JournalEntry, json: &str) {
        assert_eq!(serde_json::to_string(entry).expect("serialize"), json);
        assert_eq!(
            &serde_json::from_str::<JournalEntry>(json).expect("deserialize"),
            entry,
            "{json}"
        );
    }

    fn venue_order(id: &str) -> VenueOrderId {
        VenueOrderId(id.to_owned())
    }

    #[test]
    fn a_line_with_nothing_from_a_live_venue_is_the_line_schema_2_wrote() {
        // J2. Every backtest and paper session writes these, and `runlog diff`
        // compares a new replay with an old live journal: a line that changed
        // shape for a field it does not carry would make the two files differ
        // for a reason that has nothing to do with either run.
        golden(
            &JournalEntry::Accepted {
                at: Ts::from_nanos(7),
                client_order_id: ClientOrderId(3),
                venue_order_id: None,
            },
            r#"{"type":"accepted","at":7,"client_order_id":3}"#,
        );
        golden(
            &JournalEntry::Rejected {
                at: Ts::from_nanos(8),
                client_order_id: ClientOrderId(3),
                reason: RejectReason::InsufficientFunds,
                cited: None,
                breach: None,
            },
            r#"{"type":"rejected","at":8,"client_order_id":3,"reason":"insufficient_funds"}"#,
        );
        golden(
            &JournalEntry::Cancelled {
                at: Ts::from_nanos(9),
                client_order_id: ClientOrderId(3),
                remaining: "0.05".parse().expect("qty"),
                venue_order_id: None,
                ended: None,
            },
            r#"{"type":"cancelled","at":9,"client_order_id":3,"remaining":5000000}"#,
        );
        golden(
            &JournalEntry::Refused {
                at: Ts::from_nanos(10),
                client_order_id: ClientOrderId(4),
                instrument: key(),
                side: Side::Buy,
                qty: "0.002".parse().expect("qty"),
                reason: RejectReason::Malformed,
                bound: None,
                by: RefusedBy::Seam,
            },
            &format!(
                r#"{{"type":"refused","at":10,"client_order_id":4,"instrument":{KEY},"side":"buy","qty":200000,"reason":"malformed","bound":null,"by":"seam"}}"#
            ),
        );
        golden(
            &JournalEntry::Refused {
                at: Ts::from_nanos(11),
                client_order_id: ClientOrderId(5),
                instrument: key(),
                side: Side::Sell,
                qty: "0.001".parse().expect("qty"),
                reason: RejectReason::RiskLimit,
                bound: Some(Bound::OrderNotional),
                by: RefusedBy::Risk,
            },
            &format!(
                r#"{{"type":"refused","at":11,"client_order_id":5,"instrument":{KEY},"side":"sell","qty":100000,"reason":"risk_limit","bound":"order_notional","by":"risk"}}"#
            ),
        );
        golden(
            &JournalEntry::Stopped {
                at: Ts::from_nanos(12),
                working: Vec::new(),
            },
            r#"{"type":"stopped","at":12}"#,
        );
        // A fill without the venue's names, as every fill a schema-2 build
        // wrote reads back. (A schema-3 writer always adds `remaining`.)
        golden(
            &JournalEntry::Filled {
                at: Ts::from_nanos(13),
                client_order_id: ClientOrderId(6),
                instrument: key(),
                side: Side::Buy,
                px: "76650".parse().expect("px"),
                qty: "0.001".parse().expect("qty"),
                fee: "0.0766".parse().expect("fee"),
                is_maker: false,
                fee_asset: FeeAsset::Quote,
                remaining: None,
                venue_order_id: None,
                trade_id: None,
            },
            &format!(
                r#"{{"type":"filled","at":13,"client_order_id":6,"instrument":{KEY},"side":"buy","px":7665000000000,"qty":100000,"fee":7660000,"is_maker":false,"fee_asset":"quote"}}"#
            ),
        );
    }

    #[test]
    fn the_fields_schema_3_added_to_existing_lines_have_a_fixed_wire_form() {
        // J1, for the fields on lines that already existed. Every value is
        // distinct and none is zero, so a field dropped, swapped or defaulted
        // on the way through shows here.
        golden(
            &JournalEntry::Filled {
                at: Ts::from_nanos(21),
                client_order_id: ClientOrderId(7),
                instrument: key(),
                side: Side::Sell,
                px: "76651".parse().expect("px"),
                qty: "0.0004".parse().expect("qty"),
                fee: "0.0003".parse().expect("fee"),
                is_maker: true,
                fee_asset: FeeAsset::Base,
                remaining: Some("0.0006".parse().expect("qty")),
                venue_order_id: Some(venue_order("4293153")),
                trade_id: Some(VenueTradeId("12345".to_owned())),
            },
            &format!(
                r#"{{"type":"filled","at":21,"client_order_id":7,"instrument":{KEY},"side":"sell","px":7665100000000,"qty":40000,"fee":30000,"is_maker":true,"fee_asset":"base","remaining":60000,"venue_order_id":"4293153","trade_id":"12345"}}"#
            ),
        );
        golden(
            &JournalEntry::Accepted {
                at: Ts::from_nanos(22),
                client_order_id: ClientOrderId(8),
                venue_order_id: Some(venue_order("4293154")),
            },
            r#"{"type":"accepted","at":22,"client_order_id":8,"venue_order_id":"4293154"}"#,
        );
        golden(
            &JournalEntry::Rejected {
                at: Ts::from_nanos(23),
                client_order_id: ClientOrderId(9),
                reason: RejectReason::Venue,
                cited: Some(Cited {
                    http_status: Some(400),
                    code: -1013,
                    msg: VenueText::new("Filter failure: NOTIONAL"),
                }),
                breach: Some(FilterBreach::MinNotional),
            },
            r#"{"type":"rejected","at":23,"client_order_id":9,"reason":"venue","cited":{"http_status":400,"code":-1013,"msg":"Filter failure: NOTIONAL"},"breach":"min_notional"}"#,
        );
        golden(
            &JournalEntry::Cancelled {
                at: Ts::from_nanos(24),
                client_order_id: ClientOrderId(10),
                remaining: "0.0007".parse().expect("qty"),
                venue_order_id: Some(venue_order("4293155")),
                ended: Some(EndedBy::Expired),
            },
            r#"{"type":"cancelled","at":24,"client_order_id":10,"remaining":70000,"venue_order_id":"4293155","ended":"expired"}"#,
        );
        golden(
            &JournalEntry::Orphaned {
                at: Ts::from_nanos(25),
                client_order_id: ClientOrderId(11),
                px: "76652".parse().expect("px"),
                qty: "0.0002".parse().expect("qty"),
                fee: "0.0001".parse().expect("fee"),
                fee_asset: FeeAsset::Base,
                venue_order_id: Some(venue_order("4293156")),
                trade_id: Some(VenueTradeId("12346".to_owned())),
            },
            r#"{"type":"orphaned","at":25,"client_order_id":11,"px":7665200000000,"qty":20000,"fee":10000,"fee_asset":"base","venue_order_id":"4293156","trade_id":"12346"}"#,
        );
        golden(
            &JournalEntry::Stopped {
                at: Ts::from_nanos(26),
                working: vec![ClientOrderId(12), ClientOrderId(14)],
            },
            r#"{"type":"stopped","at":26,"working":[12,14]}"#,
        );
    }
}

#[cfg(test)]
mod venue_entry_tests {
    //! The entries schema 3 added for what a live venue says on its own, and
    //! what each site that has no `_` arm decided about them.

    use super::{agrees, next_order_id, replay, Agreement, InstrumentKey, JournalEntry, StrayKind};
    use quant_core::event::Side;
    use quant_core::execution::{ClientOrderId, FeeAsset, VenueOrderId, VenueTradeId};
    use quant_core::instrument::{Exchange, InstrumentRegistry};
    use quant_core::report::{
        AccountCause, AccountChange, AnomalyKind, CancelRefusal, Cited, Clearance, DoubtCause,
        DoubtChange, ForeignOutcome, Holding, Landing, Provenance, VenueText,
    };
    use quant_core::time::Ts;

    fn golden(entry: &JournalEntry, json: &str) {
        assert_eq!(serde_json::to_string(entry).expect("serialize"), json);
        assert_eq!(
            &serde_json::from_str::<JournalEntry>(json).expect("deserialize"),
            entry,
            "{json}"
        );
    }

    fn venue_order(id: &str) -> VenueOrderId {
        VenueOrderId(id.to_owned())
    }

    fn cited() -> Cited {
        Cited {
            http_status: Some(400),
            code: -2011,
            msg: VenueText::new("Unknown order sent."),
        }
    }

    const CITED: &str = r#"{"http_status":400,"code":-2011,"msg":"Unknown order sent."}"#;

    /// One line of every entry schema 3 added as a line of its own (the
    /// venue's, the market's return and the account's balances), every value
    /// distinct and none zero, with the JSON each must be.
    // A table, one row per variant: long because the set is.
    #[allow(clippy::too_many_lines)]
    fn every_venue_entry() -> Vec<(JournalEntry, String)> {
        vec![
            (
                JournalEntry::Placed {
                    at: Ts::from_nanos(31),
                    client_order_id: ClientOrderId(1),
                    venue_order_id: venue_order("4293153"),
                    transact_ms: 1_499_405_658_657,
                },
                r#"{"type":"placed","at":31,"client_order_id":1,"venue_order_id":"4293153","transact_ms":1499405658657}"#.to_owned(),
            ),
            (
                JournalEntry::NeverLanded {
                    at: Ts::from_nanos(32),
                    client_order_id: ClientOrderId(2),
                    landing: Landing::NotSent {
                        why: VenueText::new("connection refused"),
                    },
                },
                r#"{"type":"never_landed","at":32,"client_order_id":2,"landing":{"not_sent":{"why":"connection refused"}}}"#.to_owned(),
            ),
            (
                JournalEntry::Doubt {
                    at: Ts::from_nanos(33),
                    client_order_id: ClientOrderId(3),
                    change: DoubtChange::Raised {
                        cause: DoubtCause::ServerError,
                        said: Some(cited()),
                    },
                },
                format!(
                    r#"{{"type":"doubt","at":33,"client_order_id":3,"change":{{"raised":{{"cause":"server_error","said":{CITED}}}}}}}"#
                ),
            ),
            (
                JournalEntry::Doubt {
                    at: Ts::from_nanos(34),
                    client_order_id: ClientOrderId(3),
                    change: DoubtChange::Cleared {
                        how: Clearance::Reported,
                    },
                },
                r#"{"type":"doubt","at":34,"client_order_id":3,"change":{"cleared":{"how":"reported"}}}"#.to_owned(),
            ),
            (
                JournalEntry::CancelRefused {
                    at: Ts::from_nanos(35),
                    client_order_id: ClientOrderId(42),
                    why: CancelRefusal::NotOpen(cited()),
                },
                format!(
                    r#"{{"type":"cancel_refused","at":35,"client_order_id":42,"why":{{"not_open":{CITED}}}}}"#
                ),
            ),
            (
                JournalEntry::Foreign {
                    at: Ts::from_nanos(36),
                    client_id: VenueText::new("q-r0-7"),
                    provenance: Provenance::OtherRun(VenueText::new("r0")),
                    venue_order_id: venue_order("4293154"),
                    side: Side::Sell,
                    what: ForeignOutcome::Traded {
                        trade_id: VenueTradeId("77".to_owned()),
                        px: "0.1026".parse().expect("px"),
                        qty: "0.2".parse().expect("qty"),
                        commission: "0.0002".parse().expect("commission"),
                        commission_asset: VenueText::new("BNB"),
                        remaining: "0.4".parse().expect("qty"),
                    },
                },
                r#"{"type":"foreign","at":36,"client_id":"q-r0-7","provenance":{"other_run":"r0"},"venue_order_id":"4293154","side":"sell","what":{"traded":{"trade_id":"77","px":10260000,"qty":20000000,"commission":20000,"commission_asset":"BNB","remaining":40000000}}}"#.to_owned(),
            ),
            (
                JournalEntry::Account {
                    at: Ts::from_nanos(37),
                    change: AccountChange::Lost {
                        cause: AccountCause::IdleTimeout,
                    },
                },
                r#"{"type":"account","at":37,"change":{"lost":{"cause":"idle_timeout"}}}"#.to_owned(),
            ),
            (
                JournalEntry::Anomaly {
                    at: Ts::from_nanos(38),
                    client_order_id: Some(ClientOrderId(5)),
                    what: AnomalyKind::DuplicateTrade,
                    detail: VenueText::new("trade 77 again"),
                },
                r#"{"type":"anomaly","at":38,"client_order_id":5,"what":"duplicate_trade","detail":"trade 77 again"}"#.to_owned(),
            ),
            (
                JournalEntry::Anomaly {
                    at: Ts::from_nanos(39),
                    client_order_id: None,
                    what: AnomalyKind::Unreadable,
                    detail: VenueText::new("not json"),
                },
                r#"{"type":"anomaly","at":39,"what":"unreadable","detail":"not json"}"#.to_owned(),
            ),
            (
                JournalEntry::Stray {
                    at: Ts::from_nanos(40),
                    client_order_id: ClientOrderId(99),
                    kind: StrayKind::Cancelled,
                    venue_order_id: Some(venue_order("4293155")),
                },
                r#"{"type":"stray","at":40,"client_order_id":99,"kind":"cancelled","venue_order_id":"4293155"}"#.to_owned(),
            ),
            (
                JournalEntry::Stray {
                    at: Ts::from_nanos(41),
                    client_order_id: ClientOrderId(98),
                    kind: StrayKind::Rejected,
                    venue_order_id: None,
                },
                r#"{"type":"stray","at":41,"client_order_id":98,"kind":"rejected"}"#.to_owned(),
            ),
            (
                JournalEntry::Sighted {
                    at: Ts::from_nanos(43),
                },
                r#"{"type":"sighted","at":43}"#.to_owned(),
            ),
            (
                JournalEntry::Balances {
                    at: Ts::from_nanos(44),
                    base_asset: "BTC".to_owned(),
                    base_free: "0.05".parse().expect("qty"),
                    base_locked: "0.001".parse().expect("qty"),
                    quote_asset: "USDT".to_owned(),
                    quote_free: "92.5".parse().expect("notional"),
                    quote_locked: "7.25".parse().expect("notional"),
                },
                r#"{"type":"balances","at":44,"base_asset":"BTC","base_free":5000000,"base_locked":100000,"quote_asset":"USDT","quote_free":9250000000,"quote_locked":725000000}"#.to_owned(),
            ),
        ]
    }

    #[test]
    fn a_balances_line_names_its_assets_from_the_instrument() {
        // The helper the live binary calls at start and at stop. The names
        // come from the definition, so a line cannot name one asset and hold
        // another's amount; and every amount distinct, so a free swapped with
        // a locked shows.
        let mut registry = InstrumentRegistry::new();
        let btcusdt = registry.register(quant_core::instrument::InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: quant_core::instrument::InstrumentKind::Spot,
            tick_size: "0.01".parse().expect("tick"),
            lot_size: "0.00001".parse().expect("lot"),
            min_notional: "5".parse().expect("notional"),
        });
        let line = JournalEntry::balances(
            Ts::from_nanos(44),
            registry.expect(btcusdt),
            Holding {
                free: "0.05".parse().expect("qty"),
                locked: "0.001".parse().expect("qty"),
            },
            Holding {
                free: "92.5".parse().expect("notional"),
                locked: "7.25".parse().expect("notional"),
            },
        )
        .expect("a definition with both names");
        let (expected, _) = every_venue_entry()
            .into_iter()
            .find(|(entry, _)| matches!(entry, JournalEntry::Balances { .. }))
            .expect("the table holds a balances line");
        assert_eq!(line, expected);

        // A placeholder, as `replay` registers, names no assets: refused
        // rather than written as a balance of nothing.
        let mut placeholders = InstrumentRegistry::new();
        let placeholder = super::resolve(&mut placeholders, &key());
        let refused = JournalEntry::balances(
            Ts::from_nanos(45),
            placeholders.expect(placeholder),
            Holding {
                free: "1".parse().expect("qty"),
                locked: "2".parse().expect("qty"),
            },
            Holding {
                free: "3".parse().expect("notional"),
                locked: "4".parse().expect("notional"),
            },
        )
        .expect_err("no asset names, no balances");
        assert_eq!(refused.symbol, "BTCUSDT");
        assert!(refused.to_string().contains("without its base and quote"));
    }

    #[test]
    fn every_entry_a_live_venue_adds_has_a_fixed_wire_form() {
        // J1, for the new lines. Pinned before a live venue writes any, so
        // the first journal a live run produces is in a format that cannot
        // have drifted by accident.
        for (entry, json) in every_venue_entry() {
            golden(&entry, &json);
            assert_eq!(entry.at(), {
                let value: serde_json::Value = serde_json::from_str(&json).expect("json");
                Ts::from_nanos(value["at"].as_i64().expect("at"))
            });
        }
        golden(
            &JournalEntry::Stray {
                at: Ts::from_nanos(42),
                client_order_id: ClientOrderId(97),
                kind: StrayKind::Accepted,
                venue_order_id: Some(venue_order("4293157")),
            },
            r#"{"type":"stray","at":42,"client_order_id":97,"kind":"accepted","venue_order_id":"4293157"}"#,
        );
    }

    fn submitted(id: u64) -> JournalEntry {
        JournalEntry::Submitted {
            at: Ts::from_nanos(i64::try_from(id).expect("small")),
            client_order_id: ClientOrderId(id),
            instrument: key(),
            side: Side::Buy,
            qty: "0.001".parse().expect("qty"),
            limit: None,
            mark: None,
        }
    }

    fn key() -> InstrumentKey {
        InstrumentKey {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
        }
    }

    #[test]
    fn no_id_a_venue_named_moves_the_counter() {
        // J4. Every id this engine minted is on a `submitted` or `refused`
        // line already, so the new lines add nothing; and an id the venue
        // named must never move it. A stray's id is the venue's word about an
        // order the engine is not working, `Context::cancel` takes any id so a
        // refused cancel can name one never minted, and a foreign client id
        // that happens to look like ours is text on purpose.
        let mut entries: Vec<JournalEntry> = (1..=5).map(submitted).collect();
        entries.extend(every_venue_entry().into_iter().map(|(entry, _)| entry));
        assert_eq!(next_order_id(&entries), 6);

        // And each line on its own, with nothing minted beside it. Behind
        // five submissions only an id above 5 could show, and most of the
        // table's ids are 5 or below, so the assertion above sees two of the
        // eight lines that carry one. Alone, any id a line contributed would
        // lift the answer off 1.
        for (entry, json) in every_venue_entry() {
            assert_eq!(
                next_order_id(std::slice::from_ref(&entry)),
                1,
                "{json} moved the counter"
            );
        }
    }

    #[test]
    fn an_id_named_but_never_minted_does_not_move_the_counter() {
        // The two lines from before schema 3 that can carry such an id.
        // `Context::cancel` journals whatever id the strategy passes, and an
        // orphan is a fill for an order the engine is not working: the fill
        // twin of a stray. Either feeding the counter would make the next
        // session mint 100 here, and runlog's hole check would then report
        // ids 3 to 99 as decisions nobody wrote down.
        let mut entries: Vec<JournalEntry> = (1..=2).map(submitted).collect();
        entries.push(JournalEntry::CancelRequested {
            at: Ts::from_nanos(3),
            client_order_id: ClientOrderId(99),
        });
        assert_eq!(next_order_id(&entries), 3, "a cancel of an id never minted");

        let mut entries: Vec<JournalEntry> = (1..=2).map(submitted).collect();
        entries.push(JournalEntry::Orphaned {
            at: Ts::from_nanos(3),
            client_order_id: ClientOrderId(99),
            px: "76652".parse().expect("px"),
            qty: "0.001".parse().expect("qty"),
            fee: "0.0766".parse().expect("fee"),
            fee_asset: FeeAsset::Quote,
            venue_order_id: None,
            trade_id: None,
        });
        assert_eq!(next_order_id(&entries), 3, "a fill for an order not ours");
    }

    fn filled(id: u64, side: Side, px: &str) -> JournalEntry {
        JournalEntry::Filled {
            at: Ts::from_nanos(i64::try_from(id).expect("small") * 100),
            client_order_id: ClientOrderId(id),
            instrument: key(),
            side,
            px: px.parse().expect("px"),
            qty: "0.001".parse().expect("qty"),
            fee: "0.0766".parse().expect("fee"),
            is_maker: false,
            fee_asset: FeeAsset::Quote,
            remaining: Some("0".parse().expect("qty")),
            venue_order_id: Some(venue_order(&format!("90{id}"))),
            trade_id: Some(VenueTradeId(format!("80{id}"))),
        }
    }

    #[test]
    fn what_a_venue_says_on_its_own_moves_no_money() {
        // J5. A stranger's trade is money that is not this run's, the
        // `orphaned` argument for an order that is not even ours; the rest
        // are conditions and acknowledgements. Folding any of them would make
        // the recompute disagree with a checkpoint that faithfully reports
        // what this engine holds.
        let start = JournalEntry::Started {
            at: Ts::from_nanos(0),
            cash: "100".parse().expect("cash"),
            schema: super::SCHEMA,
            run: Some("r1".to_owned()),
        };
        let fills = vec![
            filled(1, Side::Buy, "76650"),
            filled(2, Side::Sell, "76700"),
            filled(3, Side::Buy, "76600"),
        ];
        let mut plain = vec![start.clone()];
        plain.extend(fills.iter().cloned());

        let mut noisy = vec![start];
        let mut venue = every_venue_entry().into_iter().map(|(entry, _)| entry);
        for fill in &fills {
            noisy.extend(venue.by_ref().take(4));
            noisy.push(fill.clone());
        }
        noisy.extend(venue);

        let expected = replay(&plain, &mut InstrumentRegistry::new());
        let got = replay(&noisy, &mut InstrumentRegistry::new());
        assert_eq!(got, expected);

        noisy.push(JournalEntry::checkpoint(Ts::from_nanos(1_000), &expected));
        assert_eq!(agrees(&noisy), Agreement::Agrees);
    }
}
