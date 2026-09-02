//! Order book reconstruction, and the invariants that say it worked.
//!
//! # What this is for
//!
//! M1 proved the capture is *complete* — every message the venue sent is on disk,
//! and every discontinuity is explained by a record inside the file. That is a
//! different and weaker claim than the one this module exists to test:
//!
//! > **Does applying those messages, in order, produce a sane book?**
//!
//! `docs/data-contract.md` §7 is deliberate about keeping the two apart. A capture
//! can be complete and still reconstruct into nonsense if this code is wrong, and a
//! book can look perfectly sane while built from a capture with a silent hole in it.
//! Completeness was M1's criterion; correctness of reconstruction is M2's.
//!
//! # The three steps M1 deliberately deferred
//!
//! `quant-binance`'s `rest` module explains why the recorder captures a snapshot
//! and does nothing else with it. The rest of Binance's local-order-book algorithm
//! lands here, where a mistake costs a re-derive rather than a week of re-recording:
//!
//! 1. **Buffer the deltas.** Genuinely necessary, and it was tempting to think
//!    otherwise. The capture file holds them in order, but the snapshot arrives
//!    *later in the stream* than the deltas it supersedes: the recorder fetches it
//!    concurrently with the drain, so a few hundred milliseconds of messages land
//!    while the REST request is in flight. A replay that applied the snapshot and
//!    then carried on from the next frame would skip every delta between the
//!    venue's `lastUpdateId` and the snapshot's arrival, and the chain check would
//!    then refuse everything after it. So the book buffers while it has no anchor
//!    and replays the buffer once it gets one. See [`Outcome::Buffered`].
//! 2. **Discard the stale ones.** A delta whose whole range is at or below the
//!    snapshot's `lastUpdateId` is entirely superseded by it. See [`Outcome::Stale`].
//! 3. **Check the chain joins.** The first delta after a snapshot may *straddle*
//!    it; every one after that must continue exactly. See [`Book::apply_delta`].
//!
//! # Why the resync rule is venue-agnostic
//!
//! Because [`BookDelta`] carries a *range* — `first_update_id..=final_update_id` —
//! rather than Binance's field names. Binance's documented rule, "the first event
//! to apply must satisfy `U <= lastUpdateId + 1 <= u`, and each one after it must
//! satisfy `U == previous u + 1`", is a statement about that range. A venue with a
//! single monotonic sequence number sets both ends equal and the same rule holds
//! unchanged.
//!
//! That is a generalisation from one implementation, which this project is normally
//! sceptical of. It earns its place because the abstraction already existed: M0 put
//! the range in the event contract, before any of this was written.
//!
//! # An invalid book is empty, not flagged
//!
//! When the chain breaks or a [`Gap`] arrives, the book is **cleared**, not marked
//! unusable. A flag can be ignored; an empty book cannot be misread as prices. The
//! same reasoning that puts the risk layer between the strategy and the venue
//! rather than beside it.
//!
//! # A truncated snapshot is an accepted limitation
//!
//! The recorder asks for 5000 levels a side, which is the deepest Binance offers.
//! A book deeper than that is invisible to us beyond the window, so a delta
//! touching a level outside it inserts what looks like a new deep level, and a
//! removal of a level we never had is a no-op. Neither affects the touch, which is
//! what anything trading actually reads. Worth knowing rather than worth fixing:
//! the venue cannot tell us more.

use std::collections::{BTreeMap, VecDeque};

use quant_core::event::{BookDelta, BookSnapshot, Level, MarketEvent, Side};
use quant_core::fixed::{Px, Qty};

/// How many deltas may wait for a snapshot.
///
/// The window needing cover is one REST round trip: the recorder fetches its
/// snapshot concurrently with draining the socket, so only the messages arriving
/// during that request need holding. At `depth@100ms` that is a handful, and the
/// largest gap between a reconnect and its anchor in the acceptance capture is
/// well under a hundred deltas. A thousand is generous headroom, at roughly half
/// a megabyte.
///
/// A cap rather than an unbounded queue, because a snapshot may never arrive at
/// all -- `SnapshotFailed` is a recorded outcome, and an unbounded buffer would
/// turn that into a slow memory leak instead of a bounded loss of reconstruction.
/// The **oldest** go when it fills: the snapshot's `lastUpdateId` will be near the
/// recent end, so the old ones are the likeliest to have been stale anyway.
pub const MAX_PENDING_DELTAS: usize = 1024;

/// Where the book stands relative to the venue's own numbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Anchor {
    /// No snapshot has been applied, or the book was invalidated. Deltas cannot be
    /// used: there is nothing to apply them to.
    #[default]
    Unanchored,
    /// A snapshot is in place and no delta has been applied since.
    ///
    /// Distinct from [`Self::Streaming`] because the *first* delta after a snapshot
    /// is allowed to straddle it — the venue's numbering does not pause while a
    /// REST request is in flight, so the delta that was in progress when the
    /// snapshot was taken legitimately spans it.
    Anchored { last_update_id: u64 },
    /// Deltas are flowing. The next must continue exactly from `last_update_id`.
    Streaming { last_update_id: u64 },
}

/// What happened to a delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Applied. The book moved forward.
    Applied,
    /// Entirely superseded by the snapshot in place: its whole range is at or
    /// below the snapshot's `lastUpdateId`.
    ///
    /// **Expected, not a defect.** The recorder fetches its snapshot concurrently
    /// with draining the socket, so a handful of deltas always arrive before it —
    /// and this is the step that discards them. The acceptance capture has 569 of
    /// these across seven days, which `quant-verify` counts as
    /// `deltas_before_anchor`.
    Stale,
    /// Held until a snapshot arrives to apply it to.
    ///
    /// Not a discard. These are the deltas that straddle a snapshot which has not
    /// reached us yet, and dropping them would leave a hole at the start of every
    /// single resync. [`MAX_PENDING_DELTAS`] bounds the wait.
    Buffered,
    /// No anchor, and no room left to keep waiting for one, so the delta was
    /// dropped. Counted, because it is the difference between "recorded" and
    /// "reconstructible".
    Unanchored,
    /// The chain did not join. The book has been invalidated.
    ///
    /// Should never happen on a capture the verifier passed: it reports any chain
    /// break not explained by a recorded gap, and a gap invalidates the book here
    /// before a delta is ever reached. So this firing during replay means either
    /// this code or the verifier is wrong — two independently written checks
    /// disagreeing, which is worth more than either alone.
    Broken { expected: u64, found: u64 },
}

/// An invariant that does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Violation {
    /// The best bid is at or above the best ask. On a real venue this is
    /// impossible; in a reconstruction it means we applied something wrongly.
    Crossed { bid: Px, ask: Px },
    /// A level with no quantity was retained. Zero means *remove* — keeping one
    /// would leave phantom liquidity that a strategy could try to trade against.
    ZeroQuantity { px: Px },
    /// A price or quantity at or below zero.
    NonPositive { px: Px, qty: Qty },
}

impl core::fmt::Display for Violation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Crossed { bid, ask } => {
                write!(f, "crossed book: best bid {bid} >= best ask {ask}")
            }
            Self::ZeroQuantity { px } => write!(f, "zero-quantity level retained at {px}"),
            Self::NonPositive { px, qty } => write!(f, "non-positive level: {px} x {qty}"),
        }
    }
}

/// Counters over the life of a reconstruction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BookStats {
    pub snapshots: u64,
    /// Snapshots ignored because the book was already further ahead.
    pub stale_snapshots: u64,
    pub applied: u64,
    pub stale: u64,
    /// Deltas held to wait for an anchor, and then replayed against it.
    pub buffered: u64,
    /// Deltas dropped because the buffer filled before an anchor arrived.
    pub unanchored: u64,
    pub broken: u64,
    /// Times the book was cleared by a gap or a chain break.
    pub invalidations: u64,
    /// Deepest either side reached, as a sanity check against the 5000-level
    /// window the recorder asks for.
    pub max_depth: usize,
}

/// A reconstructed limit order book.
///
/// `BTreeMap` per side rather than a sorted `Vec`: insert and remove are
/// `O(log n)`, the best price is a single `first`/`last` lookup, and sortedness is
/// structural rather than something to maintain. At the observed ~33 levels per
/// delta over 11.6M deltas that is a few seconds for a week of data — the ordering
/// being correct by construction is worth more than the cache locality a `Vec`
/// would buy.
#[derive(Debug, Clone, Default)]
pub struct Book {
    /// Ascending by price, so the best bid is the **last** entry.
    bids: BTreeMap<Px, Qty>,
    /// Ascending by price, so the best ask is the **first** entry.
    asks: BTreeMap<Px, Qty>,
    anchor: Anchor,
    /// Deltas waiting for a snapshot to apply them to. Empty whenever the book is
    /// live, because a live book applies them on arrival.
    pending: VecDeque<BookDelta>,
    stats: BookStats,
}

impl Book {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one event, whatever it is.
    ///
    /// A single entry point so a replay driver does not have to know which event
    /// types move a book — and so that adding one later cannot be forgotten at a
    /// call site.
    pub fn apply(&mut self, event: &MarketEvent) -> Outcome {
        match event {
            MarketEvent::BookSnapshot(s) => self.apply_snapshot(s),
            MarketEvent::BookDelta(d) => self.apply_delta(d),
            // A trade does not move a diff-stream book: the deltas that accompany
            // it already carry the resulting quantities. Whether a trade price is
            // consistent with the touch is a fill-model question, and belongs to
            // M4 with the rest of the execution modelling.
            MarketEvent::Trade(_) => Outcome::Applied,
            // We were blind. Whatever the book said, it is now wrong, and it stays
            // wrong until a snapshot re-anchors it. This is what makes "refuse to
            // trade across a gap" enforceable rather than advisory.
            MarketEvent::Gap(_) => {
                self.invalidate();
                Outcome::Unanchored
            }
        }
    }

    /// Reset the book to a snapshot, then replay anything that was waiting.
    ///
    /// A snapshot the book has already moved past is **ignored**. That case is
    /// real: the recorder takes an hourly anchor whether or not the book needs
    /// one, and by the time an hourly snapshot is written the live stream is
    /// already further ahead than the `lastUpdateId` the venue served. Applying it
    /// would throw away newer information and move the book backwards.
    ///
    /// Which is also why periodic snapshots are not wasted: their value is for the
    /// case where the book *has* been invalidated, and re-anchoring is exactly
    /// what they then do.
    pub fn apply_snapshot(&mut self, snapshot: &BookSnapshot) -> Outcome {
        if let Some(current) = self.last_update_id() {
            if snapshot.last_update_id <= current {
                self.stats.stale_snapshots += 1;
                return Outcome::Stale;
            }
        }

        self.bids.clear();
        self.asks.clear();
        // Zero-quantity entries are skipped rather than stored. A snapshot should
        // not contain any, but storing one would break the invariant that every
        // level in the book is real liquidity.
        for level in &snapshot.bids {
            if !level.qty.is_zero() {
                self.bids.insert(level.px, level.qty);
            }
        }
        for level in &snapshot.asks {
            if !level.qty.is_zero() {
                self.asks.insert(level.px, level.qty);
            }
        }
        self.anchor = Anchor::Anchored {
            last_update_id: snapshot.last_update_id,
        };
        self.stats.snapshots += 1;
        self.replay_pending();
        self.note_depth();
        Outcome::Applied
    }

    /// Feed the waiting deltas through the freshly anchored book, in arrival order.
    ///
    /// The step that makes a resync work at all. Stops at the first break rather
    /// than pressing on: everything after a hole is past the point where the chain
    /// can be trusted, and a later snapshot is the thing that recovers it.
    fn replay_pending(&mut self) {
        let pending = core::mem::take(&mut self.pending);
        for delta in pending {
            self.stats.buffered += 1;
            if matches!(self.apply_anchored(&delta), Outcome::Broken { .. }) {
                break;
            }
        }
    }

    /// Apply one incremental update, if the chain allows it.
    ///
    /// The whole of Binance's resync rule, expressed over the range every
    /// [`BookDelta`] carries:
    ///
    /// | State | Condition | Result |
    /// |---|---|---|
    /// | no snapshot | — | [`Outcome::Unanchored`] |
    /// | any | `final_update_id <= last` | [`Outcome::Stale`], discarded |
    /// | just snapshotted | `first_update_id <= last + 1` | applied; may straddle |
    /// | streaming | `first_update_id == last + 1` | applied |
    /// | otherwise | — | [`Outcome::Broken`], book cleared |
    pub fn apply_delta(&mut self, delta: &BookDelta) -> Outcome {
        if !self.is_live() {
            // Held, not dropped. The snapshot that will supersede some of these
            // has not arrived yet, and the ones it does not supersede are the
            // only way the chain joins on the far side of it.
            if self.pending.len() >= MAX_PENDING_DELTAS {
                self.pending.pop_front();
                self.stats.unanchored += 1;
            }
            self.pending.push_back(delta.clone());
            return Outcome::Buffered;
        }
        self.apply_anchored(delta)
    }

    /// The rule itself, for a book that already has an anchor.
    fn apply_anchored(&mut self, delta: &BookDelta) -> Outcome {
        let (last, exact) = match self.anchor {
            // Unreachable: every caller checks `is_live` first. Counted rather
            // than panicking, because a book is not worth a crash.
            Anchor::Unanchored => {
                self.stats.unanchored += 1;
                return Outcome::Unanchored;
            }
            Anchor::Anchored { last_update_id } => (last_update_id, false),
            Anchor::Streaming { last_update_id } => (last_update_id, true),
        };

        // Checked before the continuity rule, and it must be: a delta the snapshot
        // already accounts for is not a break in the chain, it is one of the
        // deltas the snapshot was fetched to supersede.
        if delta.final_update_id <= last {
            self.stats.stale += 1;
            return Outcome::Stale;
        }

        let joins = if exact {
            delta.first_update_id == last + 1
        } else {
            // The venue's numbering does not pause while our REST request is in
            // flight, so the delta in progress when the snapshot was taken
            // legitimately spans it. `U <= L+1 <= u`; the right half is the
            // staleness check just above.
            delta.first_update_id <= last + 1
        };
        if !joins {
            self.stats.broken += 1;
            self.invalidate();
            return Outcome::Broken {
                expected: last + 1,
                found: delta.first_update_id,
            };
        }

        Self::apply_levels(&mut self.bids, &delta.bids);
        Self::apply_levels(&mut self.asks, &delta.asks);
        self.anchor = Anchor::Streaming {
            last_update_id: delta.final_update_id,
        };
        self.stats.applied += 1;
        self.note_depth();
        Outcome::Applied
    }

    /// Zero quantity means *remove*; anything else is the absolute new quantity.
    ///
    /// The venue's convention, and preserving it exactly is why the parser keeps
    /// zero-quantity levels instead of filtering them: a dropped removal leaves
    /// liquidity in the book that no longer exists, which is the kind of error a
    /// strategy would trade into rather than trip over.
    fn apply_levels(side: &mut BTreeMap<Px, Qty>, levels: &[Level]) {
        for level in levels {
            if level.qty.is_zero() {
                side.remove(&level.px);
            } else {
                side.insert(level.px, level.qty);
            }
        }
    }

    /// Clear the book and require a fresh snapshot before any delta is applied.
    pub fn invalidate(&mut self) {
        if self.anchor != Anchor::Unanchored {
            self.stats.invalidations += 1;
        }
        self.bids.clear();
        self.asks.clear();
        self.anchor = Anchor::Unanchored;
        // The buffer is cleared too. Deltas held from before a gap are on the far
        // side of a hole from anything that comes after it, so replaying them
        // against the next snapshot would splice two disjoint stretches of the
        // stream together and call the result a book.
        self.pending.clear();
    }

    /// Whether the book currently means anything.
    #[must_use]
    pub const fn is_live(&self) -> bool {
        !matches!(self.anchor, Anchor::Unanchored)
    }

    /// The venue update id this book is current as of.
    #[must_use]
    pub const fn last_update_id(&self) -> Option<u64> {
        match self.anchor {
            Anchor::Unanchored => None,
            Anchor::Anchored { last_update_id } | Anchor::Streaming { last_update_id } => {
                Some(last_update_id)
            }
        }
    }

    #[must_use]
    pub fn best_bid(&self) -> Option<Level> {
        self.bids
            .iter()
            .next_back()
            .map(|(px, qty)| Level::new(*px, *qty))
    }

    #[must_use]
    pub fn best_ask(&self) -> Option<Level> {
        self.asks
            .iter()
            .next()
            .map(|(px, qty)| Level::new(*px, *qty))
    }

    #[must_use]
    pub fn depth(&self) -> (usize, usize) {
        (self.bids.len(), self.asks.len())
    }

    /// Bids from the best downward.
    ///
    /// "From the best" in both directions, rather than exposing the maps' own
    /// ascending order, because every consumer that walks a book walks *away
    /// from the touch* — a simulator filling an order, a strategy measuring
    /// depth. Handing out ascending bids would make each of them remember to
    /// reverse, and the one that forgets fills at the worst price in the book
    /// and calls it a touch.
    #[must_use]
    pub fn bids(&self) -> impl DoubleEndedIterator<Item = Level> + '_ {
        self.bids
            .iter()
            .rev()
            .map(|(px, qty)| Level::new(*px, *qty))
    }

    /// Asks from the best upward.
    #[must_use]
    pub fn asks(&self) -> impl DoubleEndedIterator<Item = Level> + '_ {
        self.asks.iter().map(|(px, qty)| Level::new(*px, *qty))
    }

    /// Levels on one side, from the best outward.
    ///
    /// `Side::Buy` gives the **asks**: the side a buyer consumes. Named for what
    /// the caller is doing rather than for which half of the book it is, because
    /// "a buy order eats the ask side" is exactly the inversion that gets written
    /// backwards at a call site.
    #[must_use]
    pub fn takeable(&self, side: Side) -> Box<dyn Iterator<Item = Level> + '_> {
        match side {
            Side::Buy => Box::new(self.asks()),
            Side::Sell => Box::new(self.bids()),
        }
    }

    #[must_use]
    pub const fn stats(&self) -> BookStats {
        self.stats
    }

    /// The invariant cheap enough to check on every tick.
    ///
    /// `O(1)` — two map lookups. The crossed-book check is the one that actually
    /// falsifies a reconstruction: if bids and asks overlap, something was applied
    /// wrongly, and no amount of structural care elsewhere makes that acceptable.
    ///
    /// The other invariants are structural — [`Book::apply_levels`] cannot insert a
    /// zero — and are confirmed by [`Book::audit`] rather than re-derived 70 million
    /// times.
    pub fn check(&self) -> Result<(), Violation> {
        if let (Some(bid), Some(ask)) = (self.best_bid(), self.best_ask()) {
            if bid.px >= ask.px {
                return Err(Violation::Crossed {
                    bid: bid.px,
                    ask: ask.px,
                });
            }
        }
        Ok(())
    }

    /// Every invariant, including the `O(n)` ones.
    ///
    /// Separate from [`Book::check`] because scanning both sides costs thousands of
    /// comparisons and running it per tick would be tens of billions over a week
    /// of data — for properties that hold by construction. Worth running where it
    /// is cheap: at each snapshot, and at the end of a replay.
    pub fn audit(&self) -> Result<(), Violation> {
        self.check()?;
        for (px, qty) in self.bids.iter().chain(self.asks.iter()) {
            if qty.is_zero() {
                return Err(Violation::ZeroQuantity { px: *px });
            }
            if !px.is_positive() || !qty.is_positive() {
                return Err(Violation::NonPositive { px: *px, qty: *qty });
            }
        }
        Ok(())
    }

    fn note_depth(&mut self) {
        self.stats.max_depth = self
            .stats
            .max_depth
            .max(self.bids.len().max(self.asks.len()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::event::{EventMeta, Gap, GapCause, Side, Trade};
    use quant_core::instrument::{
        Exchange, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
    };
    use quant_core::time::Ts;

    fn instrument() -> (InstrumentRegistry, InstrumentId) {
        let mut reg = InstrumentRegistry::new();
        let id = reg.register(InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.01".parse().unwrap(),
            lot_size: "0.00001".parse().unwrap(),
            min_notional: "5".parse().unwrap(),
        });
        (reg, id)
    }

    fn meta() -> EventMeta {
        let (_reg, id) = instrument();
        EventMeta {
            instrument: id,
            exchange_ts: Ts::from_millis(1_787_315_652_514),
            local_recv_ts: Ts::from_millis(1_787_315_652_600),
            ingest_seq: 1,
        }
    }

    fn level(px: &str, qty: &str) -> Level {
        Level::new(px.parse().unwrap(), qty.parse().unwrap())
    }

    /// A snapshot with a two-level bid side and a two-level ask side.
    fn snapshot(last_update_id: u64) -> BookSnapshot {
        BookSnapshot {
            meta: meta(),
            last_update_id,
            bids: vec![level("100.0", "1.0"), level("99.0", "2.0")],
            asks: vec![level("101.0", "1.5"), level("102.0", "2.5")],
        }
    }

    fn delta(first: u64, final_id: u64, bids: Vec<Level>, asks: Vec<Level>) -> BookDelta {
        BookDelta {
            meta: meta(),
            first_update_id: first,
            final_update_id: final_id,
            bids,
            asks,
        }
    }

    #[test]
    fn a_snapshot_gives_a_live_book_with_an_uncrossed_touch() {
        let mut book = Book::new();
        assert!(!book.is_live(), "a fresh book has no prices to give");
        assert_eq!(book.best_bid(), None);

        book.apply_snapshot(&snapshot(100));
        assert!(book.is_live());
        assert_eq!(book.last_update_id(), Some(100));
        assert_eq!(book.best_bid(), Some(level("100.0", "1.0")));
        assert_eq!(book.best_ask(), Some(level("101.0", "1.5")));
        assert_eq!(book.depth(), (2, 2));
        book.audit().expect("a snapshot must audit clean");
    }

    #[test]
    fn deltas_that_the_snapshot_already_accounts_for_are_discarded() {
        // Step 2 of the algorithm M1 deferred, and the reason the acceptance
        // capture has 569 deltas before its anchors. The recorder fetches the
        // snapshot concurrently with the drain, so these always exist.
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));

        // Entirely at or below lastUpdateId: superseded.
        assert_eq!(
            book.apply_delta(&delta(90, 95, vec![level("100.0", "9.9")], vec![])),
            Outcome::Stale
        );
        assert_eq!(
            book.apply_delta(&delta(99, 100, vec![level("100.0", "9.9")], vec![])),
            Outcome::Stale
        );
        assert_eq!(
            book.best_bid(),
            Some(level("100.0", "1.0")),
            "a stale delta must not touch the book"
        );
        assert_eq!(book.stats().stale, 2);
        assert_eq!(book.stats().applied, 0);
    }

    #[test]
    fn the_first_delta_after_a_snapshot_may_straddle_it() {
        // The subtle half of the rule: `U <= lastUpdateId + 1 <= u`. The venue's
        // numbering does not pause while our REST request is in flight, so the
        // delta in progress when the snapshot was taken spans it -- and refusing it
        // would leave a one-message hole at every single resync.
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));

        // U=98 is below lastUpdateId, u=105 is above: it straddles.
        assert_eq!(
            book.apply_delta(&delta(98, 105, vec![level("100.5", "3.0")], vec![])),
            Outcome::Applied
        );
        assert_eq!(book.best_bid(), Some(level("100.5", "3.0")));
        assert_eq!(book.last_update_id(), Some(105));
        book.audit().unwrap();
    }

    #[test]
    fn after_the_first_delta_the_chain_must_continue_exactly() {
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));
        assert_eq!(
            book.apply_delta(&delta(101, 105, vec![], vec![])),
            Outcome::Applied
        );

        // 106 is the only acceptable next `U`.
        assert_eq!(
            book.apply_delta(&delta(106, 110, vec![], vec![])),
            Outcome::Applied
        );
        assert_eq!(book.last_update_id(), Some(110));

        // A jump is a break, and the straddle allowance is gone.
        assert_eq!(
            book.apply_delta(&delta(112, 120, vec![], vec![])),
            Outcome::Broken {
                expected: 111,
                found: 112
            }
        );
        assert!(
            !book.is_live(),
            "a broken chain must leave no readable prices"
        );
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.stats().broken, 1);
        assert_eq!(book.stats().invalidations, 1);
    }

    #[test]
    fn a_zero_quantity_removes_the_level_and_never_lingers() {
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));
        assert_eq!(book.depth(), (2, 2));

        // Remove the top bid; 99 becomes the touch.
        book.apply_delta(&delta(101, 102, vec![level("100.0", "0.0")], vec![]));
        assert_eq!(book.depth(), (1, 2));
        assert_eq!(book.best_bid(), Some(level("99.0", "2.0")));
        book.audit().expect("no zero level may be retained");

        // Removing a level we do not have is a no-op, not an error: with a
        // truncated snapshot window this happens legitimately.
        book.apply_delta(&delta(103, 104, vec![level("1.0", "0.0")], vec![]));
        assert_eq!(book.depth(), (1, 2));
    }

    #[test]
    fn a_quantity_update_replaces_rather_than_accumulates() {
        // The venue sends the absolute new quantity at a price, not a change to it.
        // Adding instead of replacing would inflate depth without bound.
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));
        book.apply_delta(&delta(101, 102, vec![level("100.0", "5.0")], vec![]));
        assert_eq!(book.best_bid(), Some(level("100.0", "5.0")));
        assert_eq!(book.depth(), (2, 2), "an update is not an insert");
    }

    #[test]
    fn deltas_before_any_snapshot_are_held_not_dropped() {
        // The correction that a real replay forced. Dropping these leaves a hole at
        // the start of every resync: the snapshot arrives *later in the stream*
        // than the deltas it supersedes, so the ones it does not supersede are the
        // only way the chain joins on its far side.
        let mut book = Book::new();
        assert_eq!(
            book.apply_delta(&delta(1, 2, vec![level("100.0", "1.0")], vec![])),
            Outcome::Buffered
        );
        assert!(!book.is_live(), "held is not applied");
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.stats().applied, 0);
        assert_eq!(book.stats().unanchored, 0, "nothing was dropped");
    }

    #[test]
    fn the_buffer_is_bounded_and_keeps_the_most_recent() {
        // A snapshot may never arrive -- SnapshotFailed is a recorded outcome -- so
        // an unbounded buffer would be a slow leak. The oldest go, because the
        // snapshot's lastUpdateId lands near the recent end.
        let mut book = Book::new();
        for i in 0..(MAX_PENDING_DELTAS as u64 + 50) {
            let first = i * 10 + 1;
            book.apply_delta(&delta(first, first + 9, vec![], vec![]));
        }
        assert_eq!(
            book.stats().unanchored,
            50,
            "the overflow is counted, not silent"
        );

        // The kept window ends at the newest delta, so a snapshot taken now still
        // joins.
        let newest_final = (MAX_PENDING_DELTAS as u64 + 49) * 10 + 10;
        let mut snap = snapshot(newest_final - 5);
        snap.last_update_id = newest_final - 5;
        assert_eq!(book.apply_snapshot(&snap), Outcome::Applied);
        assert!(book.is_live());
    }

    #[test]
    fn a_snapshot_supersedes_the_deltas_that_arrived_before_it() {
        // The real arrival order, and the bug a replay of the acceptance capture
        // exposed: the recorder fetches its snapshot concurrently with the drain,
        // so deltas land while the REST request is in flight and the snapshot frame
        // is written after them.
        let mut book = Book::new();
        assert_eq!(
            book.apply_delta(&delta(90, 95, vec![level("50.0", "1.0")], vec![])),
            Outcome::Buffered
        );
        assert_eq!(
            book.apply_delta(&delta(96, 100, vec![level("51.0", "1.0")], vec![])),
            Outcome::Buffered
        );
        assert_eq!(
            book.apply_delta(&delta(101, 105, vec![level("100.5", "3.0")], vec![])),
            Outcome::Buffered
        );

        // lastUpdateId = 100: the first two are superseded, the third is not.
        assert_eq!(book.apply_snapshot(&snapshot(100)), Outcome::Applied);
        assert_eq!(book.stats().stale, 2, "the two the snapshot accounts for");
        assert_eq!(book.stats().applied, 1, "and the one it does not");
        assert_eq!(book.last_update_id(), Some(105));
        assert_eq!(
            book.best_bid(),
            Some(level("100.5", "3.0")),
            "the replayed delta must be in the book"
        );
        assert_eq!(book.best_ask(), Some(level("101.0", "1.5")));

        // And the stream continues from there without a break.
        assert_eq!(
            book.apply_delta(&delta(106, 110, vec![], vec![])),
            Outcome::Applied
        );
        book.audit().unwrap();
    }

    #[test]
    fn a_snapshot_the_book_has_already_passed_is_ignored() {
        // The hourly anchor case. The recorder takes one whether the book needs it
        // or not, and by the time it is written the live stream is further ahead
        // than the lastUpdateId the venue served. Applying it would move the book
        // backwards and discard newer information.
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));
        book.apply_delta(&delta(101, 500, vec![level("100.5", "7.0")], vec![]));
        assert_eq!(book.last_update_id(), Some(500));

        assert_eq!(book.apply_snapshot(&snapshot(300)), Outcome::Stale);
        assert_eq!(
            book.last_update_id(),
            Some(500),
            "the book did not move back"
        );
        assert_eq!(book.best_bid(), Some(level("100.5", "7.0")));
        assert_eq!(book.stats().stale_snapshots, 1);

        // A snapshot that is genuinely ahead does re-anchor.
        assert_eq!(book.apply_snapshot(&snapshot(900)), Outcome::Applied);
        assert_eq!(book.last_update_id(), Some(900));
    }

    #[test]
    fn a_gap_clears_the_book_until_a_snapshot_re_anchors_it() {
        // The property that makes "refuse to trade across a gap" enforceable.
        // Clearing rather than flagging is deliberate: a flag can be ignored, an
        // empty book cannot be misread as prices.
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));
        book.apply_delta(&delta(101, 105, vec![], vec![]));
        assert!(book.best_bid().is_some());

        let gap = MarketEvent::Gap(Gap {
            meta: meta(),
            cause: GapCause::Disconnect,
            last_good_ts: Ts::from_millis(1_787_315_652_000),
        });
        assert_eq!(book.apply(&gap), Outcome::Unanchored);
        assert!(!book.is_live());
        assert_eq!(book.best_bid(), None, "no prices may survive blindness");

        // Deltas after the gap are held for the next snapshot, not applied.
        assert_eq!(
            book.apply_delta(&delta(106, 110, vec![], vec![])),
            Outcome::Buffered
        );
        assert!(!book.is_live(), "held deltas do not make a book readable");

        // And that snapshot re-anchors, replaying what was held.
        assert_eq!(book.apply_snapshot(&snapshot(105)), Outcome::Applied);
        assert!(book.is_live());
        assert_eq!(
            book.last_update_id(),
            Some(110),
            "the held delta was replayed"
        );
    }

    #[test]
    fn a_trade_does_not_move_the_book() {
        // In a diff-stream model the accompanying deltas already carry the
        // resulting quantities. Applying a trade as well would double-count it.
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));
        let before = book.depth();
        let trade = MarketEvent::Trade(Trade {
            meta: meta(),
            px: "101.0".parse().unwrap(),
            qty: "0.5".parse().unwrap(),
            aggressor: Side::Buy,
            venue_trade_id: 1,
        });
        assert_eq!(book.apply(&trade), Outcome::Applied);
        assert_eq!(book.depth(), before);
        assert_eq!(book.best_ask(), Some(level("101.0", "1.5")));
    }

    #[test]
    fn a_crossed_book_is_caught() {
        // The invariant the milestone criterion actually turns on. Constructed
        // deliberately, because a correct reconstruction cannot produce one.
        let mut book = Book::new();
        book.apply_snapshot(&BookSnapshot {
            meta: meta(),
            last_update_id: 100,
            bids: vec![level("102.0", "1.0")],
            asks: vec![level("101.0", "1.0")],
        });
        match book.check() {
            Err(Violation::Crossed { bid, ask }) => {
                assert_eq!(bid, "102.0".parse().unwrap());
                assert_eq!(ask, "101.0".parse().unwrap());
            }
            other => panic!("expected a crossed book, got {other:?}"),
        }
    }

    #[test]
    fn an_empty_side_is_not_a_violation() {
        // A one-sided book is unusual but legal, and it is what a truncated
        // window plus heavy removals can transiently produce. Only an *overlap*
        // is a violation.
        let mut book = Book::new();
        book.apply_snapshot(&BookSnapshot {
            meta: meta(),
            last_update_id: 100,
            bids: vec![level("100.0", "1.0")],
            asks: vec![],
        });
        book.check().expect("one side empty is fine");
        book.audit().expect("and it audits clean");
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn a_duplicate_delta_is_stale_rather_than_a_break() {
        // Replaying the same message twice must not invalidate a good book.
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));
        let d = delta(101, 105, vec![level("100.5", "3.0")], vec![]);
        assert_eq!(book.apply_delta(&d), Outcome::Applied);
        assert_eq!(book.apply_delta(&d), Outcome::Stale);
        assert!(book.is_live(), "a repeat must not break the chain");
        assert_eq!(book.best_bid(), Some(level("100.5", "3.0")));
    }

    #[test]
    fn the_touch_tracks_the_deepest_side_seen() {
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(100));
        book.apply_delta(&delta(
            101,
            102,
            vec![level("98.0", "1.0"), level("97.0", "1.0")],
            vec![],
        ));
        assert_eq!(book.depth(), (4, 2));
        assert_eq!(book.stats().max_depth, 4);
    }
    #[test]
    fn both_sides_iterate_from_the_touch_outward() {
        // The property the accessors exist for. Handing out the maps' own
        // ascending order would make every caller remember to reverse one side,
        // and the one that forgets fills at the worst price in the book and
        // calls it a touch.
        let mut book = Book::new();
        book.apply_snapshot(&BookSnapshot {
            meta: meta(),
            last_update_id: 10,
            bids: vec![level("100.0", "1"), level("99.0", "2"), level("98.0", "3")],
            asks: vec![
                level("101.0", "1"),
                level("102.0", "2"),
                level("103.0", "3"),
            ],
        });

        let bids: Vec<Px> = book.bids().map(|l| l.px).collect();
        let asks: Vec<Px> = book.asks().map(|l| l.px).collect();
        assert_eq!(bids[0], book.best_bid().expect("bid").px);
        assert_eq!(asks[0], book.best_ask().expect("ask").px);
        assert!(bids[0] > bids[2], "bids descend from the best");
        assert!(asks[0] < asks[2], "asks ascend from the best");
    }

    #[test]
    fn a_buyer_takes_the_ask_side() {
        // Named for what the caller is doing, because "a buy eats the asks" is
        // exactly the inversion that gets written backwards at a call site.
        let mut book = Book::new();
        book.apply_snapshot(&snapshot(10));
        assert_eq!(
            book.takeable(Side::Buy).next().expect("a level").px,
            book.best_ask().expect("ask").px
        );
        assert_eq!(
            book.takeable(Side::Sell).next().expect("a level").px,
            book.best_bid().expect("bid").px
        );
    }
}
