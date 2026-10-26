//! The shell: the merge machine behind `ExecutionVenue`, with the transport
//! on the other side of two channels.
//!
//! `docs/live-run.md` §5, *`LiveVenue`, decided before the code*, §1, is the
//! design, and this module is its engine-thread half:
//!
//! ```text
//!   engine thread (sync)                        tokio runtime (the binary's)
//!   Engine ─submit/cancel─► LiveVenue ─orders (4)─► request worker ─REST─► venue
//!          ◄─────poll────── │  ▲                          │
//!                           │  └──────inbox (8)◄──────────┘ answers, stamped
//!                           └◄─ AccountFeed ◄─ tee ◄─ account listener ◄─ venue
//! ```
//!
//! [`LiveVenue`] owns no runtime, spawns nothing and awaits nothing. Every
//! call returns at once: `submit` and `cancel` encode, tell the machine,
//! `try_send` and return, and `poll` drains what is waiting without blocking.
//! The engine thread is the market's only reader, and blocked for one 20 s
//! timeout it would book nothing, tally nothing, and lose market records once
//! the tee's channel filled.
//!
//! # Every holder of a sender
//!
//! | channel | senders | receiver | closed by |
//! |---|---|---|---|
//! | `orders` | this venue, alone | the worker | dropping the engine, which owns this venue |
//! | `inbox` | the worker | this venue | nothing waits for it to close; a worker whose send fails stops |
//! | account feed | the listener's `TeeSink` | this venue's `AccountFeed` | dropping the listener's future, last |
//! | stop | an `Arc<AtomicBool>`, set here, read by the worker | | not a channel |
//!
//! M1.c2's recorder hung at shutdown because a forgotten clone kept a channel
//! open, so the table is the whole list. Nothing here clones `orders`, and
//! nothing in this process waits for any of them to close.
//!
//! # Shutdown, in the binary's order
//!
//! Item 8's binary does this, in this order, and [`crate::drain`] is steps 3
//! and 4:
//!
//! 1. `run()` returns when the market source ends.
//! 2. `Engine::cancel_working()`, item 8's seam addition (§7, decision 3).
//! 3. `settle()` until nothing is working or a deadline longer than
//!    `ORDER_TIMEOUT` passes, pausing `IDLE_TICK` when nothing came.
//! 4. [`LiveVenue::stop_sending`], then `settle()` until
//!    [`LiveVenue::outstanding`] is zero or a second deadline passes. A worker
//!    never spawned holds it above zero for ever, which is why the wait has a
//!    deadline of its own. Every order still unanswered is then still working
//!    in the engine, and goes into `stopped.working`.
//! 5. The closing checkpoint, `stopped { working }`, `balances`, only while
//!    the strict writer has not failed: the binary writes them through its
//!    `Latch`, so none lands after a line a failed append may have torn.
//! 6. Drop the engine: this venue goes with it, its `Drop` sets the stop flag,
//!    `orders` closes, and the worker returns, its `inbox` sender with it.
//! 7. Stop the account listener, which seals the account capture.
//! 8. Only then shut the runtime down. It drives the worker through steps 3
//!    and 4 and the listener until step 7: dropped earlier, the worker never
//!    answers and the capture is left trailerless, which reads as a crash.
//!
//! # A worker gone
//!
//! A worker whose task is dropped while the venue lives, by a runtime shut
//! down too early or by a panic in a debug build, can never answer what it
//! owes, and an order it had sent may be on the venue. The shell sees it go
//! when the worker's end of `orders` closes, takes every answer it left on
//! the inbox, logs the loss once as an error, and answers each request still
//! owed itself: `Unknown { Interrupted }`, an order in doubt and a cancel of
//! unknown effect. It cannot tell a request the worker took from one still
//! queued when it went, so none is called certain. That writes the doubt the
//! journal owes, without which an order the account then went dark on reads
//! as `unwatched`, and brings `outstanding` to zero, so that step 4 does not
//! wait out its deadline for answers nobody can send.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use quant_binance::account::{AccountFeed, AccountRecord, Polled};
use quant_binance::exchange_info::VenueInstrument;
use quant_binance::orders::{CancelOrder, EncodeError, NewOrder};
use quant_binance::orders::{CancelReply, Placement, WhyUnknown};
use quant_binance::trade::{Checked, TradeClient};
use quant_binance::user_data::{MissingAsset, RunTag, Translator};
use quant_core::execution::{ClientOrderId, OrderRequest};
use quant_core::instrument::{InstrumentDef, InstrumentId};
use quant_core::report::VenueReport;
use quant_core::time::{Clock, Ts};
use quant_engine::{ExecutionVenue, Unready};
use tokio::sync::mpsc::{self, error::TryRecvError, error::TrySendError};
use tracing::error;

use crate::merge::{Input, Merge, MergeStats, Queue, Unsent};
use crate::worker::{Answer, Request, Worker, WorkerStats, STOPPING, UNHEARD_AFTER};

/// How many requests may wait for the worker: four (§1). Depth buys nothing.
/// An order queued behind a request that times out leaves more than 20 s
/// after the stamp it carries, past the venue's 5 s `recvWindow`, and is
/// refused on arrival; unbounded would hide a stalled worker behind a growing
/// queue, every order in it journalled `submitted`.
pub const ORDERS_BOUND: usize = 4;

/// How many answers may wait for the engine's next poll: eight (§1). The
/// worker awaits room rather than drop one, so the bound costs a delay and
/// never a loss.
pub const INBOX_BOUND: usize = 8;

/// Why a venue could not be wired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    /// The definition does not name both its assets, so the translator could
    /// book no fee.
    MissingAsset(MissingAsset),
    /// The instrument is not one `/api/v3/order` trades, so no order or cancel
    /// for it could be written down.
    NotBinanceSpot(EncodeError),
}

impl core::fmt::Display for WireError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::MissingAsset(missing) => write!(f, "{missing}"),
            Self::NotBinanceSpot(why) => write!(f, "{why}"),
        }
    }
}

impl std::error::Error for WireError {}

/// Build a live venue and the request worker behind it: what the binary calls.
///
/// `instrument` is the id the engine knows the wired instrument by, beside
/// the definition the venue gave for it (§3, `NoFilters`): an `InstrumentDef`
/// carries no id to check a request against, and the encoder writes `symbol`
/// from whatever definition it is handed. `run` is the journal's run tag,
/// which every client order id is minted under. `checked` is the proof that
/// `venue-check`'s checks passed, and carries the offset every order and
/// cancel is stamped with. `client` is the signed REST client the worker
/// alone holds, `feed` the account stream as the listener's tee hands it on,
/// and `clock` the one clock that stamps `local_recv_ts` everywhere (§4),
/// which stamps every answer.
///
/// The venue lives on the engine's thread and owns no runtime. The worker is
/// a future for the binary to spawn on its own, once, before the engine runs:
/// `runtime.spawn(worker.run())`.
///
/// # Errors
///
/// [`WireError`], for an instrument no order could be sent for.
// `checked` by value on purpose, though only its offset is read: the token is
// spent on the venue it builds, one check for one venue.
#[allow(clippy::needless_pass_by_value)]
pub fn wire(
    instrument: InstrumentId,
    venue_instrument: &VenueInstrument,
    run: RunTag,
    checked: Checked,
    client: TradeClient,
    feed: AccountFeed,
    clock: Arc<dyn Clock>,
) -> Result<(LiveVenue, Worker), WireError> {
    let definition = venue_instrument.definition.clone();
    let translator = Translator::new(run.clone(), &definition).map_err(WireError::MissingAsset)?;
    // The encoder's own check, made once here, so that a venue that could
    // send nothing is never built.
    CancelOrder::encode(ClientOrderId(0), &run, &definition).map_err(WireError::NotBinanceSpot)?;
    let (orders, orders_rx) = mpsc::channel(ORDERS_BOUND);
    let (inbox_tx, inbox) = mpsc::channel(INBOX_BOUND);
    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(WorkerStats::default());
    let offset_ms = checked.offset_ms();
    let venue = LiveVenue {
        merge: Merge::new(instrument, translator),
        instrument,
        definition,
        run,
        offset_ms,
        orders,
        inbox,
        feed,
        feed_closed: false,
        held: VecDeque::new(),
        stop: Arc::clone(&stop),
        owed: VecDeque::new(),
        latest: None,
        pending: Vec::new(),
        stats: LiveStats::default(),
        worker: Arc::clone(&stats),
        answers: VecDeque::new(),
        records: VecDeque::new(),
    };
    let worker = Worker {
        client,
        clock,
        offset_ms,
        orders: orders_rx,
        inbox: inbox_tx,
        stop,
        stats,
        unheard_after: UNHEARD_AFTER,
    };
    Ok((venue, worker))
}

/// What the venue has done, counted on the engine's thread.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LiveStats {
    /// Requests handed to the worker: orders and cancels.
    pub queued: u64,
    /// Cancels that found the queue full and waited in the shell.
    pub held: u64,
    /// Orders and cancels the shell answered itself, never queued.
    pub unsent: u64,
    /// Answers to requests taken off the inbox.
    pub answers: u64,
    /// Decision 6's `Unheard` inputs taken off the inbox.
    pub unheard: u64,
    /// Account frames and `Blind`s taken off the feed.
    pub frames: u64,
    pub blinds: u64,
    /// Whether the worker went while this venue lived: its task returned or
    /// was dropped.
    pub worker_gone: bool,
    /// Requests a gone worker never answered, answered unknown by the shell.
    pub abandoned: u64,
}

/// Which answer a request handed to the worker is owed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owed {
    /// A placement, for an order.
    Order,
    /// A cancel's reply.
    Cancel,
}

impl Owed {
    const fn of(request: &Request) -> Self {
        match request {
            Request::Order { .. } => Self::Order,
            Request::Cancel { .. } => Self::Cancel,
        }
    }
}

/// The live venue. See the module docs.
#[derive(Debug)]
pub struct LiveVenue {
    merge: Merge,
    instrument: InstrumentId,
    definition: InstrumentDef,
    run: RunTag,
    /// The `Checked` offset, which every order's stamp has taken off it.
    offset_ms: i64,
    orders: mpsc::Sender<Request>,
    inbox: mpsc::Receiver<Answer>,
    feed: AccountFeed,
    feed_closed: bool,
    /// Cancels that found `orders` full, first in first out, at most one per
    /// order. Only ever `Request::Cancel`.
    held: VecDeque<Request>,
    stop: Arc<AtomicBool>,
    /// Requests handed to the worker and not yet answered, in the order they
    /// were handed over, which is the order the worker answers them in.
    owed: VecDeque<(ClientOrderId, Owed)>,
    /// The latest stamp anything was stepped with: the earliest an input
    /// with no stamp of its own can have been heard.
    latest: Option<Ts>,
    /// What `submit` and `cancel` made the machine say, for the next poll.
    pending: Vec<VenueReport>,
    stats: LiveStats,
    worker: Arc<WorkerStats>,
    /// What one poll drained, reused so a poll allocates nothing.
    answers: VecDeque<Answer>,
    records: VecDeque<AccountRecord>,
}

impl Drop for LiveVenue {
    /// Set the stop flag before `orders` closes, so that a careless drop
    /// cannot release the queue: a tokio receiver still yields what is
    /// buffered after its senders are gone, and an order sent then would be
    /// on the venue with no journal holding it.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

impl LiveVenue {
    /// Send nothing more: every request the worker reads after this is
    /// answered `NotSent { "stopping" }`, every held cancel and every later
    /// cancel `cancel_refused { not_sent }`, and the queue takes nothing, so
    /// `ready` answers `QueueFull` (§1, shutdown step 4). A request the worker
    /// took just before this call may go out just after it; its answer counts
    /// against [`Self::outstanding`] like any other.
    ///
    /// `&self`, because the binary reaches the venue only through
    /// `Engine::venue`.
    pub fn stop_sending(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Requests handed to the worker and not yet answered. It reaches zero
    /// only while the worker runs or once the shell has seen it gone, so a
    /// wait on it needs a deadline: a worker never spawned holds it up for
    /// ever.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.owed.len()
    }

    /// The stop flag, for a test that must act once the venue stops sending.
    #[cfg(test)]
    pub(crate) fn stop_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    /// Whether [`Self::stop_sending`] has been called.
    #[must_use]
    pub fn stopping(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Whether this venue could take an order for its wired instrument now:
    /// what `ready` answers for one, for a log line or a test.
    ///
    /// # Errors
    ///
    /// [`Unready`], saying why not.
    pub fn readiness(&self) -> Result<(), Unready> {
        self.merge.ready(self.instrument, self.queue())
    }

    /// Whether the latch is set.
    #[must_use]
    pub const fn diverged(&self) -> bool {
        self.merge.diverged()
    }

    #[must_use]
    pub const fn stats(&self) -> LiveStats {
        self.stats
    }

    /// What the machine counted and did not journal.
    #[must_use]
    pub const fn merge_stats(&self) -> MergeStats {
        self.merge.stats()
    }

    /// The worker's counts, which keep counting after the engine takes this
    /// venue.
    #[must_use]
    pub fn worker_stats(&self) -> Arc<WorkerStats> {
        Arc::clone(&self.worker)
    }

    /// The queue's state, as the machine's readiness needs it: no room, a
    /// cancel held ahead of anything new, the worker gone, or the venue
    /// stopping are all `Full`, since a closed queue takes nothing either.
    fn queue(&self) -> Queue {
        if self.stopping()
            || !self.held.is_empty()
            || self.orders.is_closed()
            || self.orders.capacity() == 0
        {
            Queue::Full
        } else {
            Queue::Room
        }
    }

    fn step(&mut self, at: Ts, input: Input, out: &mut Vec<VenueReport>) {
        self.heard_at(at);
        self.merge.step(at, input, out);
    }

    fn heard_at(&mut self, at: Ts) {
        self.latest = Some(self.latest.map_or(at, |latest| latest.max(at)));
    }

    /// The stamp for an input with none of its own: the engine's `now`, or
    /// the latest stamp stepped if that is later. The engine's clock does not
    /// move on an idle wake, and records drained in this poll can be stamped
    /// after it, so `now` alone could date a loss before what was heard ahead
    /// of it.
    fn unstamped(&self, now: Ts) -> Ts {
        self.latest.map_or(now, |latest| latest.max(now))
    }

    /// An order the shell did not send.
    fn unsent_order(&mut self, id: ClientOrderId, why: String, now: Ts) {
        self.stats.unsent += 1;
        let mut out = std::mem::take(&mut self.pending);
        self.step(now, Input::Unsent(Unsent::Order { id, why }), &mut out);
        self.pending = out;
    }

    /// A cancel the shell did not send.
    fn unsent_cancel(
        &mut self,
        id: ClientOrderId,
        why: String,
        at: Ts,
        out: &mut Vec<VenueReport>,
    ) {
        self.stats.unsent += 1;
        self.step(at, Input::Unsent(Unsent::Cancel { id, why }), out);
    }

    /// Hand `request` to the worker, counting it outstanding; or hand it
    /// back, saying why not.
    fn enqueue(&mut self, request: Request) -> Result<(), TrySendError<Request>> {
        let owed = (request.id(), Owed::of(&request));
        self.orders.try_send(request)?;
        self.owed.push_back(owed);
        self.stats.queued += 1;
        Ok(())
    }

    /// Send held cancels while there is room, first in first out: the start
    /// of every poll, before anything else that wake can send.
    fn release_held(&mut self, now: Ts, out: &mut Vec<VenueReport>) {
        while let Some(cancel) = self.held.pop_front() {
            if self.stopping() {
                self.unsent_cancel(cancel.id(), STOPPING.to_owned(), now, out);
                continue;
            }
            match self.enqueue(cancel) {
                Ok(()) => {}
                Err(TrySendError::Full(cancel)) => {
                    self.held.push_front(cancel);
                    break;
                }
                Err(TrySendError::Closed(cancel)) => {
                    self.unsent_cancel(cancel.id(), WORKER_GONE.to_owned(), now, out);
                }
            }
        }
    }

    /// One answer off the inbox, into the machine.
    fn answer(&mut self, answer: Answer, out: &mut Vec<VenueReport>) {
        match answer {
            Answer::Placement { id, placement, at } => {
                self.answered(id, Owed::Order);
                self.step(at, Input::Placement { id, placement }, out);
            }
            Answer::CancelReply { id, reply, at } => {
                self.answered(id, Owed::Cancel);
                self.step(at, Input::CancelReply { id, reply }, out);
            }
            Answer::Unheard { id, at } => {
                self.stats.unheard += 1;
                self.step(at, Input::Unheard { id }, out);
            }
        }
    }

    fn answered(&mut self, id: ClientOrderId, owed: Owed) {
        self.stats.answers += 1;
        if let Some(at) = self.owed.iter().position(|&o| o == (id, owed)) {
            self.owed.remove(at);
        }
    }

    /// The worker is gone: answer everything it still owed, as unknown (the
    /// module docs, *A worker gone*).
    fn abandon_owed(&mut self, at: Ts, out: &mut Vec<VenueReport>) {
        while let Some((id, owed)) = self.owed.pop_front() {
            self.stats.abandoned += 1;
            let why = WhyUnknown::Interrupted(WORKER_GONE.to_owned());
            let input = match owed {
                Owed::Order => Input::Placement {
                    id,
                    placement: Placement::Unknown { why },
                },
                Owed::Cancel => Input::CancelReply {
                    id,
                    reply: CancelReply::Unknown { why },
                },
            };
            self.step(at, input, out);
        }
    }

    /// One record off the account feed, into the machine.
    fn record(&mut self, record: AccountRecord, out: &mut Vec<VenueReport>) {
        match record {
            AccountRecord::Frame {
                local_recv_ts,
                ingest_seq: _,
                payload,
            } => {
                self.stats.frames += 1;
                self.heard_at(local_recv_ts);
                self.merge.frame(local_recv_ts, &payload, out);
            }
            AccountRecord::Blind {
                local_recv_ts,
                cause,
            } => {
                self.stats.blinds += 1;
                self.step(local_recv_ts, Input::Blind(cause), out);
            }
        }
    }
}

/// Why a request went nowhere: the worker's end of the queue is gone.
const WORKER_GONE: &str = "the request worker is gone";

/// When a record on the account feed was received.
const fn received(record: &AccountRecord) -> Ts {
    match record {
        AccountRecord::Frame { local_recv_ts, .. } | AccountRecord::Blind { local_recv_ts, .. } => {
            *local_recv_ts
        }
    }
}

impl ExecutionVenue for LiveVenue {
    /// The machine's answer, given the queue's state (§3).
    fn ready(&self, request: &OrderRequest) -> Result<(), Unready> {
        self.merge.ready(request.instrument, self.queue())
    }

    /// Encode, tell the machine, `try_send`, return.
    ///
    /// The order's stamp is the engine's `now`, the stamp of the input the
    /// strategy acted on, less the `Checked` offset: no clock is read here, and
    /// the venue refuses an order that waited too long. An order that reached
    /// here unready, which only a `ready` that lied could let through, or that
    /// will not encode, is `never_landed { not_sent }` and is sent nowhere.
    fn submit(&mut self, client_order_id: ClientOrderId, request: &OrderRequest, now: Ts) {
        let id = client_order_id;
        if let Err(why) = self.ready(request) {
            self.unsent_order(
                id,
                format!("submitted while the venue was not ready: {why:?}"),
                now,
            );
            return;
        }
        let order = match NewOrder::encode(request, id, &self.run, &self.definition) {
            Ok(order) => order,
            Err(why) => {
                self.unsent_order(id, why.to_string(), now);
                return;
            }
        };
        let mut out = std::mem::take(&mut self.pending);
        self.step(
            now,
            Input::Submitted {
                id,
                qty: request.qty,
            },
            &mut out,
        );
        self.pending = out;
        let stamped_at_ms = now.as_millis().saturating_sub(self.offset_ms);
        match self.enqueue(Request::Order {
            id,
            order,
            stamped_at_ms,
        }) {
            Ok(()) => {}
            // `ready` said there was room, and nothing can take it between:
            // the engine thread is the queue's only sender.
            Err(TrySendError::Full(_)) => {
                self.unsent_order(id, "the request queue was full".to_owned(), now);
            }
            Err(TrySendError::Closed(_)) => self.unsent_order(id, WORKER_GONE.to_owned(), now),
        }
    }

    /// Encode, `try_send`, return; or hold it when the queue is full.
    ///
    /// A cancel that finds the queue full waits here and goes out at the
    /// start of the next poll that finds room, before anything else that wake
    /// can send (§1, *Held cancels*): refused instead, it would be a notice
    /// the strategy is never told, and the resting order would stay on the
    /// venue, free to fill hours later.
    fn cancel(&mut self, client_order_id: ClientOrderId, now: Ts) {
        let id = client_order_id;
        let mut out = std::mem::take(&mut self.pending);
        let why = if self.stopping() {
            Some(STOPPING.to_owned())
        } else if !self.merge.working(id) {
            Some("no order of this process is working under that id".to_owned())
        } else if self.held.iter().any(|held| held.id() == id) {
            Some("a cancel for this order is already held".to_owned())
        } else {
            None
        };
        if let Some(why) = why {
            self.unsent_cancel(id, why, now, &mut out);
            self.pending = out;
            return;
        }
        match CancelOrder::encode(id, &self.run, &self.definition) {
            Err(why) => self.unsent_cancel(id, why.to_string(), now, &mut out),
            // Behind a held cancel, so as never to overtake it.
            Ok(cancel) if !self.held.is_empty() => {
                self.stats.held += 1;
                self.held.push_back(Request::Cancel { id, cancel });
            }
            Ok(cancel) => match self.enqueue(Request::Cancel { id, cancel }) {
                Ok(()) => {}
                Err(TrySendError::Full(cancel)) => {
                    self.stats.held += 1;
                    self.held.push_back(cancel);
                }
                Err(TrySendError::Closed(_)) => {
                    self.unsent_cancel(id, WORKER_GONE.to_owned(), now, &mut out);
                }
            },
        }
        self.pending = out;
    }

    // `observe` is the trait's default, which does nothing: the real market
    // already knows what the market did, and a live venue matches nothing.

    /// What the shell said since the last poll; held cancels sent while there
    /// is room; then everything waiting on the inbox and the account feed,
    /// drained without blocking and merged by stamp as a two-way merge, each
    /// source kept in its own order (§1, `poll`; §4); then, if the worker is
    /// gone, what it owed, and if the feed has ended, its end.
    ///
    /// A sort would reorder one source's own records whenever the host clock
    /// steps back, and order within a source is evidence. Across the two, both
    /// stamped by one clock, ordering by stamp is arrival order unless that
    /// clock stepped, and the permutation test shows the order across them
    /// changes nothing but the order of lines.
    fn poll(&mut self, now: Ts, out: &mut Vec<VenueReport>) {
        out.append(&mut self.pending);
        self.release_held(now, out);

        // Asked before the inbox is drained: once the worker's end of
        // `orders` is closed the worker is gone, and every answer it sent is
        // already on the inbox. Not only the inbox's own close, which another
        // sender on it, such as item 8's rechecks, would hold off.
        let mut gone = self.orders.is_closed();
        loop {
            match self.inbox.try_recv() {
                Ok(answer) => self.answers.push_back(answer),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    gone = true;
                    break;
                }
            }
        }
        let mut closed = false;
        loop {
            match self.feed.poll() {
                Polled::Record(record) => self.records.push_back(record),
                Polled::Empty => break,
                Polled::Closed => {
                    closed = !self.feed_closed;
                    self.feed_closed = true;
                    break;
                }
            }
        }

        loop {
            let answer_first = match (self.answers.front(), self.records.front()) {
                (Some(answer), Some(record)) => answer.at() <= received(record),
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            if answer_first {
                if let Some(answer) = self.answers.pop_front() {
                    self.answer(answer, out);
                }
            } else if let Some(record) = self.records.pop_front() {
                self.record(record, out);
            }
        }

        if gone && !self.stats.worker_gone {
            self.stats.worker_gone = true;
            error!(
                owed = self.owed.len(),
                "the request worker is gone; what it owed is answered unknown"
            );
        }
        if self.stats.worker_gone {
            let at = self.unstamped(now);
            self.abandon_owed(at, out);
        }

        // The feed's end carries no stamp of its own, and every record before
        // it has been handed over.
        if closed {
            let at = self.unstamped(now);
            self.step(at, Input::Closed, out);
        }
    }
}

#[cfg(test)]
mod tests;
