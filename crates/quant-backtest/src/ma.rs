//! A moving-average crossover, kept as dumb as it can be.
//!
//! Long or flat, one instrument, fixed-size entries, market orders. No stops,
//! no sizing rule, no filter. Every one of those would be a knob, and a knob is
//! something to blame a bad result on — the point of this strategy is that a
//! bad result has nowhere to hide but the harness.
//!
//! # Why it samples on a clock rather than on every event
//!
//! An average over the last N *events* is an average over a window whose length
//! in time depends on how busy the market was, which makes the indicator faster
//! in exactly the conditions where it should be slower. Sampling the mid once
//! per interval of **event time** gives a window that means the same thing all
//! week.
//!
//! The clock is `ctx.now()`, the engine clock, which only runs forward. With a
//! simulated venue only market events move it, so it is the last event's
//! `local_recv_ts`. With a live venue a report can move it too -- one stamped
//! later than the last market event carries the clock past it -- so in general
//! it is the engine's clock and not any one event's stamp. There is no other
//! clock available, which is the point.
//!
//! # Why it will not trade across a gap
//!
//! Not by remembering to check. A gap clears the book, so `best_bid`/`best_ask`
//! are `None`, so there is no mid to sample and no sample is taken. The
//! indicator simply does not advance while we are blind, and resumes when a
//! snapshot re-anchors the book. That is the invariant being structural rather
//! than advisory.
//!
//! # Why the exit sells what is held, and not `qty`
//!
//! A spot venue takes a buyer's fee in the asset bought: buy 0.001 BTC on
//! Binance and the position is 0.000999. An exit that sold `qty` would sell
//! coins the strategy never bought -- refused by the venue, or, with spare BTC
//! in the account, filled out of coins that were never the strategy's. And the
//! position would never be exactly zero again, so the strategy would never
//! count itself flat, and every later down-cross would try to sell `qty` once
//! more.
//!
//! So the exit sells the position rounded **down** to the instrument's lot,
//! 0.00099 here: the largest size the lot rule allows that is not more than
//! is held. What remains is less than one lot, which no venue will buy, so a
//! position smaller than one lot counts as flat -- for the entry decision as
//! well as the exit, or the next up-cross would call that dust "already long"
//! and never enter again.
//!
//! Where fees are charged in the quote asset, as in every backtest that does
//! not ask for them in the base and all of M5, this changes nothing for a run
//! whose entries fill in full and whose position was built at the same `qty`.
//! The position is then exactly zero or exactly `qty`, and `qty` is a whole
//! number of lots (a size that is not is refused at construction), so
//! rounding it down leaves it alone and less than one lot means zero. That is
//! what lets the fortnight's result still reproduce to the satoshi.
//!
//! Two kinds of run differ even with the fee in the quote: one whose entry
//! the book could fill only in part -- the simulator fills what is there and
//! cancels the rest -- and one resumed holding a position built at another
//! `--qty`. Either leaves something other than `qty` held. The exit used to
//! sell `qty` regardless, too much or too little; it now sells what is held.
//! A different answer from before, and the better one.
//!
//! The lot arrives as configuration rather than through `Context`, which is
//! the entire surface a strategy has and carries market data, not the venue's
//! rules. Each binary sets it from the instrument definition it registered.
//!
//! # Why `qty` needs more than the minimum notional
//!
//! Whole lots satisfy the lot rule, and a venue has others. Where the fee is
//! taken in the base, the exit is smaller than the entry by that fee rounded
//! up to a whole lot -- one lot wherever the fee is no bigger than a lot, as
//! at 0.001 BTC: 0.00099 out against 0.001 in. And an exit follows a
//! down-cross, so it is usually sold lower than the entry was bought. A `qty`
//! that clears the venue's `min_notional` by less than those two together
//! leaves an exit under it, which the venue refuses.
//!
//! The strategy is then stuck. The lot or more still held is a holding, not
//! dust, so every up-cross is "already long" and enters nothing, and every
//! down-cross sends the same exit to be refused again, until one comes at a
//! price high enough to lift it over the minimum. With the fee in the quote,
//! the fall alone can do the same.
//!
//! So `qty` has to clear `min_notional` by more than one lot plus the move
//! expected between an entry and its exit. Nothing here checks that: the
//! check needs a price, and the strategy has none at construction. It belongs
//! to the binary that trades live, which can read one before it sends
//! anything.

use quant_book::Book;
use quant_core::event::{MarketEvent, Side};
use quant_core::execution::{ClientOrderId, ExecutionEvent, OrderKind, OrderRequest, TimeInForce};
use quant_core::fixed::{Px, Qty};
use quant_core::instrument::InstrumentId;
use quant_core::time::Ts;
use quant_engine::{Context, Note, Strategy};

/// How the crossover is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaConfig {
    pub instrument: InstrumentId,
    /// Samples in the fast average.
    pub fast: usize,
    /// Samples in the slow average. Must exceed `fast`, or there is no crossing.
    pub slow: usize,
    /// Event-time between samples.
    pub interval: i64,
    /// Size of each entry, in base units. A whole number of `lot`s, or the
    /// venue would refuse every order sized from it.
    ///
    /// Not the size of each exit: a fee taken in the base asset leaves less
    /// than this held, and the exit sells what is held. See the module docs.
    ///
    /// Size it to clear the venue's minimum notional by more than one lot
    /// plus the move expected between an entry and its exit. The exit can be
    /// a lot smaller than this, and it is sold after a down-cross. One that
    /// falls under the minimum is refused, and the lot or more still held
    /// keeps every up-cross "already long", so the strategy stops trading
    /// until the price climbs far enough to lift the exit over it. Not checked
    /// here, because there is no price at construction.
    pub qty: Qty,
    /// The instrument's lot size: every order quantity must be a whole number
    /// of these. An exit sells the position rounded down to it, and a
    /// position smaller than one lot counts as flat.
    ///
    /// Configuration rather than something read through `Context`, which is
    /// the entire surface a strategy has and holds market data, not the
    /// venue's rules. Whoever registered the instrument sets this from the
    /// definition it registered, rather than typing the venue's rule in a
    /// second time.
    pub lot: Qty,
}

/// Why a configuration cannot be a crossover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaConfigError {
    /// `fast` is zero or not shorter than `slow`, so the averages never cross.
    Windows { fast: usize, slow: usize },
    /// The lot is zero or negative, so no size is a whole number of them.
    Lot(Qty),
    /// `qty` is not a positive whole number of lots: every order sized from it
    /// is one the venue would refuse anyway.
    Size { qty: Qty, lot: Qty },
}

impl std::fmt::Display for MaConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Windows { fast, slow } => write!(
                f,
                "a fast window of {fast} against a slow one of {slow} never crosses; \
                 fast must be at least 1 and shorter than slow"
            ),
            Self::Lot(lot) => write!(f, "a lot of {lot} is no lot at all; it must be positive"),
            Self::Size { qty, lot } => write!(
                f,
                "a size of {qty} is not a positive whole number of {lot} lots, \
                 so the venue would refuse every order sized from it"
            ),
        }
    }
}

impl std::error::Error for MaConfigError {}

/// What the strategy did, for reporting.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MaStats {
    pub samples: u64,
    /// Times the fast average crossed the slow one, in either direction.
    pub crossings: u64,
    pub entries: u64,
    pub exits: u64,
    /// Signals ignored because an order was already working.
    ///
    /// Not a nuisance counter: a strategy that fires while it already has an
    /// order out is a strategy that would have doubled its position live, and
    /// this is where that shows up.
    pub suppressed: u64,
    /// Intervals with no mid to sample, because the book was dark.
    pub blind_intervals: u64,
    /// Crossings that were legitimately nothing to do: crossed up while already
    /// long, or down while already flat.
    ///
    /// The `else` this strategy never had. `crossings` was documented as the
    /// number of signals and silently was not the number of decisions --
    /// `entries + exits + suppressed` fell short of it by however many of these
    /// occurred, and nothing counted the difference. M5's ETHUSDT leg: 828
    /// crossings, 822 submitted, 4 suppressed, and **two** that went nowhere and
    /// were recorded by nothing at all.
    ///
    /// Now counted, which makes `crossings` an identity rather than an
    /// approximation -- see
    /// `every_crossing_is_an_entry_an_exit_a_suppression_or_nothing_to_do`.
    pub no_ops: u64,
}

/// Long when the fast average is above the slow one, flat otherwise.
#[derive(Debug)]
pub struct MaCrossover {
    config: MaConfig,
    /// Most recent samples, newest last. Bounded by `slow`.
    samples: std::collections::VecDeque<Px>,
    next_sample_at: Option<Ts>,
    /// Sign of (fast - slow) at the last sample, `None` until both are defined.
    last_side: Option<bool>,
    /// The order we are waiting on, if any.
    working: Option<ClientOrderId>,
    stats: MaStats,
    /// Written here, taken by the engine at the end of the event. The strategy
    /// never hands these anywhere itself -- see `Strategy::take_notes`.
    notes: Vec<Note>,
    /// The averages behind the most recent decision, so a note can say *why*
    /// rather than only *what*.
    last_pair: Option<(Px, Px)>,
    /// Next instant a `claim` is due, on an hourly grid of event time.
    next_claim_at: Option<Ts>,
    /// Notes written so far, by kind — the running total a `claim` states.
    tally: std::collections::BTreeMap<&'static str, u64>,
}

/// How often the strategy states its own running totals.
///
/// Hourly: 336 lines over a fortnight against ~3,500 of everything else, which
/// is cheap enough not to think about, and fine enough that a dropped note is
/// localised to an hour rather than to a run.
const CLAIM_INTERVAL_NANOS: i64 = 3_600 * 1_000_000_000;

impl MaCrossover {
    /// A crossover for `config`, or the reason there cannot be one.
    ///
    /// The size is refused here, once, rather than by the venue on every
    /// order: a quantity that is not a whole number of lots is an order the
    /// venue rejects anyway, and the first live trade is the expensive place
    /// to find that out. The windows are refused here too, where they used to
    /// be an `assert!` -- one constructor with two ways of saying no would make
    /// every caller handle both.
    ///
    /// # Errors
    ///
    /// [`MaConfigError`] when the windows cannot cross, the lot is not
    /// positive, or `qty` is not a positive whole number of lots.
    pub fn new(config: MaConfig) -> Result<Self, MaConfigError> {
        if config.fast == 0 || config.slow <= config.fast {
            return Err(MaConfigError::Windows {
                fast: config.fast,
                slow: config.slow,
            });
        }
        if !config.lot.is_positive() {
            return Err(MaConfigError::Lot(config.lot));
        }
        if !config.qty.is_positive() || config.qty.round_to_lot(config.lot) != config.qty {
            return Err(MaConfigError::Size {
                qty: config.qty,
                lot: config.lot,
            });
        }
        Ok(Self {
            config,
            samples: std::collections::VecDeque::with_capacity(config.slow),
            next_sample_at: None,
            last_side: None,
            working: None,
            stats: MaStats::default(),
            notes: Vec::new(),
            last_pair: None,
            next_claim_at: None,
            tally: std::collections::BTreeMap::new(),
        })
    }

    /// Write something down, for the engine to collect.
    ///
    /// Money goes in as its decimal string, never as a raw integer.
    ///
    /// Not because `serde_json` would lose it — it stores integers as `u64`/`i64`
    /// and reaches for `f64` only when a token has a fraction or exponent, so a
    /// scaled price would in fact round-trip exactly. The first version of this
    /// comment claimed otherwise and was wrong.
    ///
    /// The real reason is invariant 1's other half: a bare integer makes every
    /// reader responsible for knowing the `1e8` convention out of band, and the
    /// first one that reads it as a double loses precision silently. That is the
    /// same argument that put `DECIMAL(18,8)` rather than `INT64` in the Parquet
    /// tier at M2.d — the scale travels with the value.
    fn note(&mut self, kind: &'static str, why: &'static str) {
        let (fast, slow) = match self.last_pair {
            Some((f, s)) => (Some(f.to_string()), Some(s.to_string())),
            None => (None, None),
        };
        // Counted here rather than at each call site, so a new note kind cannot
        // be added without entering the tally a claim is checked against.
        *self.tally.entry(kind).or_insert(0) += 1;
        self.notes.push(Note::new(
            kind,
            serde_json::json!({ "why": why, "fast": fast, "slow": slow }),
        ));
    }

    /// State the running totals, so a dropped note between two claims shows up
    /// as a fold that does not reconcile.
    ///
    /// `tally` is keyed **by note kind**, which is what makes the check generic:
    /// `runlog check` folds the notes between two claims by their own `kind`
    /// field and compares against the difference between the two tallies,
    /// knowing nothing about crossovers, averages or positions. A claim stating
    /// `entries` and `no_ops` would have forced the checker to learn this
    /// strategy's vocabulary, and the next strategy's after it.
    ///
    /// The rest is for a person reading the file. `samples` and `crossings` have
    /// no per-note record and so cannot be folded — stated anyway, because the
    /// claim is also the only place the indicator's own progress is written
    /// down.
    fn claim(&mut self) {
        let s = self.stats;
        self.notes.push(Note::new(
            "claim",
            serde_json::json!({
                "tally": self.tally,
                "samples": s.samples,
                "crossings": s.crossings,
            }),
        ));
    }

    #[must_use]
    pub const fn stats(&self) -> MaStats {
        self.stats
    }

    /// Mid price, or `None` when either side of the book is missing.
    ///
    /// `None` covers both "we have not seen a snapshot yet" and "a gap cleared
    /// the book", which is right: in neither case do we know what anything is
    /// worth, and the two need no distinguishing here.
    fn mid(book: &Book) -> Option<Px> {
        let (bid, ask) = (book.best_bid()?, book.best_ask()?);
        // Averaging two `1e8`-scaled prices needs no widening: their sum is at
        // most twice a price, nowhere near `i64`.
        Some(Px::from_raw((bid.px.raw() + ask.px.raw()) / 2))
    }

    /// Simple average of the newest `n` samples, if there are that many.
    fn average(&self, n: usize) -> Option<Px> {
        if self.samples.len() < n {
            return None;
        }
        let sum: i128 = self
            .samples
            .iter()
            .rev()
            .take(n)
            .map(|px| i128::from(px.raw()))
            .sum();
        let n = i128::try_from(n).expect("a window fits in i128");
        Some(Px::from_raw(
            i64::try_from(sum / n).expect("an average of prices is a price"),
        ))
    }

    fn order(&self, side: Side, qty: Qty) -> OrderRequest {
        OrderRequest {
            instrument: self.config.instrument,
            side,
            qty,
            // Market orders, so the test is of the harness and not of a queueing
            // model the simulator does not have.
            kind: OrderKind::Market,
            time_in_force: TimeInForce::Gtc,
        }
    }
}

impl Strategy for MaCrossover {
    fn on_market_event(&mut self, event: &MarketEvent, ctx: &mut Context<'_>) {
        let instrument = self.config.instrument;
        if event.meta().instrument != instrument {
            return;
        }

        let now = ctx.now();
        // Before the sampling gate, so a claim is still made across a quiet
        // stretch -- an hour with no samples is exactly an hour whose silence
        // someone will want accounted for.
        let claim_due = self.next_claim_at.unwrap_or(now);
        if now >= claim_due {
            self.next_claim_at = Some(Ts::from_nanos(claim_due.as_nanos() + CLAIM_INTERVAL_NANOS));
            self.claim();
        }

        let due = self.next_sample_at.unwrap_or(now);
        if now < due {
            return;
        }
        // Advance to the next boundary from `due`, not from `now`, so a quiet
        // patch does not shift the sampling grid for the rest of the run.
        self.next_sample_at = Some(Ts::from_nanos(due.as_nanos() + self.config.interval));

        let Some(mid) = ctx.book(instrument).and_then(Self::mid) else {
            // Blind: the book is empty because of a gap, or has not been
            // anchored yet. The indicator does not advance, which is how "do not
            // trade across a gap" holds without a rule about gaps.
            self.stats.blind_intervals += 1;
            self.note("no_mid", "the book had no prices to sample");
            return;
        };

        self.stats.samples += 1;
        if self.samples.len() == self.config.slow {
            self.samples.pop_front();
        }
        self.samples.push_back(mid);

        let (Some(fast), Some(slow)) = (
            self.average(self.config.fast),
            self.average(self.config.slow),
        ) else {
            return;
        };
        self.last_pair = Some((fast, slow));
        let above = fast > slow;
        let crossed = self.last_side.is_some_and(|was| was != above);
        self.last_side = Some(above);
        if !crossed {
            return;
        }
        self.stats.crossings += 1;

        // One order at a time. A strategy that fired again while an order was
        // working would double its position live; suppressing it here is the
        // cost of fire-and-forget submission, charged in every world.
        if self.working.is_some() {
            self.stats.suppressed += 1;
            self.note("suppressed", "an order was already working");
            return;
        }

        // What an exit could sell: the position in whole lots, rounded down so
        // it is never more than is held. Less than one lot is dust no venue
        // will take, so it counts as flat -- for the entry as well as the exit,
        // or dust left by a base-asset fee would keep the strategy "long"
        // forever. See the module docs.
        let sellable = ctx.position(instrument).qty.round_to_lot(self.config.lot);
        let long = sellable.is_positive();
        if above && !long {
            self.stats.entries += 1;
            self.working = Some(ctx.submit(self.order(Side::Buy, self.config.qty)));
            self.note("entry", "crossed up while flat");
        } else if !above && long {
            self.stats.exits += 1;
            self.working = Some(ctx.submit(self.order(Side::Sell, sellable)));
            self.note("exit", "crossed down while long");
        } else {
            // The arm that did not exist. Both cases are legitimate -- the
            // signal agrees with the position we already hold -- but "nothing
            // happened because there was nothing to do" and "nothing happened
            // because something is broken" are the same silence in a file, and
            // the operator at 3am is asking which.
            self.stats.no_ops += 1;
            self.note(
                "no_op",
                if above {
                    "crossed up while already long"
                } else {
                    "crossed down while already flat"
                },
            );
        }
    }

    fn on_execution(&mut self, event: &ExecutionEvent, _ctx: &mut Context<'_>) {
        if self.working == Some(event.client_order_id()) && event.is_terminal() {
            self.working = None;
        }
    }

    fn take_notes(&mut self, out: &mut Vec<Note>) {
        out.append(&mut self.notes);
    }
}
