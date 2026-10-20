//! The merge machine: what was heard, in, and what the engine is told, out.
//!
//! The rules are `docs/live-run.md` §5, *`LiveVenue`, decided before the code*,
//! §2 and §3, and this module is their implementation. What follows is how
//! they map onto the code; the arguments for them are in the document.
//!
//! # Inputs and outputs
//!
//! [`Merge::step`] takes one [`Input`], stamped with when it was heard, and
//! appends what it means to a `Vec<VenueReport>`: order reports, whose event
//! the strategy is told, and notices, which only the record hears. Every
//! output carries the stamp of the input that caused it, and the machine has
//! no clock to give it another (§4). [`Merge::ready`] is the readiness answer,
//! and reads nothing but the machine and the queue's state, which the shell
//! passes in.
//!
//! A recorded account frame is routed by [`Merge::route`] before anything
//! reaches the translator: `account::classify` first, so that an answer or a
//! frame nobody can read never comes back from the translator as an
//! unattributed report putting every working order in doubt for nothing.
//!
//! # The state table, as code
//!
//! Each order is an [`Order`]: what REST said ([`Rest`]), the venue's id for
//! it, whether the stream has **mentioned** it and whether it has been
//! **heard**, what is booked and which trade ids, whether and by whom it
//! ended, its open and deferred doubt causes, and any reports held behind a
//! hole. The phases are not stored; they are read off those fields
//! ([`Order::phase`]), so they cannot disagree with them.
//!
//! - REST answers are [`Merge::placement`], one method, which reads the phase
//!   for the four rows that differ by it (`Placed`, `Refused` and `NotSent`,
//!   `Unknown`, and `IdInUse` in any phase).
//! - Stream reports about our orders are [`Merge::ours`], which judges each
//!   one by the design's *Precedence*, steps 1 to 7 in order, and only then
//!   releases or holds it. The rows "released `NEW`", "released trade or end"
//!   and "a report held" are its last two steps.
//! - The doubt rules are four small functions on [`Order`]: `raise`,
//!   `defer_or_raise`, `raise_deferred` and `clear_all`. Every place a doubt
//!   moves goes through one of them, so "each cause raised once while open"
//!   and "one `cleared` line per cause" are true of every row at once.
//! - The account's transitions are [`Merge::lose`], [`Merge::regain`] and the
//!   arms of [`Merge::step`] for [`Input::Blind`] and [`Input::Closed`] and of
//!   [`Merge::heard`] for the subscription's answer and an event.
//!
//! Every `match` over an input, a report, a phase or the account names every
//! variant, so a new one does not build until it has a row.

use std::collections::{BTreeMap, BTreeSet};

use quant_binance::account::{self, AccountFrame, Request};
use quant_binance::orders::{CancelReply, Placement};
use quant_binance::user_data::{Concerns, Inbound, Translator, Unreadable};
use quant_core::event::GapCause;
use quant_core::execution::{ClientOrderId, VenueOrderId, VenueTradeId};
use quant_core::fixed::Qty;
use quant_core::instrument::InstrumentId;
use quant_core::report::{
    AccountCause, AccountChange, AnomalyKind, CancelRefusal, Clearance, DoubtCause, DoubtChange,
    FillIds, ForeignOutcome, Notice, OrderReport, VenueReport, VenueText,
};
use quant_core::time::Ts;
use quant_engine::Unready;

/// One thing the machine hears, without its stamp, which [`Merge::step`] takes
/// beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// The shell's `submit`: an order is about to be sent, for `qty`.
    ///
    /// The request's side is not here, though the design listed it: no rule
    /// reads it. Its quantity is, because a stream report's `z`, the quantity
    /// filled so far, is the request's quantity less the report's `remaining`.
    Submitted { id: ClientOrderId, qty: Qty },
    /// The request worker: what the venue's REST answer to placing an order
    /// means.
    Placement {
        id: ClientOrderId,
        placement: Placement,
    },
    /// The request worker: what the venue's answer to a cancel means.
    CancelReply {
        id: ClientOrderId,
        reply: CancelReply,
    },
    /// Something the shell did not send.
    Unsent(Unsent),
    /// A recorded account frame, routed by [`Merge::route`].
    Heard(Heard),
    /// The account feed's `Blind`: the stream was not heard, for this cause.
    Blind(GapCause),
    /// The account feed is closed: the listener stopped for good.
    Closed,
    /// A clock or key recheck, which item 8 may run during a run.
    Check(Check),
    /// The owner's decision 6 (`docs/live-run.md` §5, §7): the shell's timer,
    /// `ORDER_TIMEOUT` after an order's `Placed`. An order the stream has
    /// still not mentioned goes into doubt for `fills_missed`, cleared by the
    /// stream like any doubt.
    Unheard { id: ClientOrderId },
}

/// What the shell did not send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unsent {
    /// An order that would not encode, or reached `submit` when it should not
    /// have: certain never to have reached the venue, so a `never_landed`.
    ///
    /// With or without an [`Input::Submitted`] before it, since the shell may
    /// refuse an order before it says one is about to go; a `Submitted` after
    /// it is a second submission under one id. After a `Submitted` it is the
    /// request's one answer, as REST's would be: a second answer, from either,
    /// is a contradiction.
    Order { id: ClientOrderId, why: String },
    /// A cancel for an order ended or never sent, a second cancel for an order
    /// whose cancel is held, or one after the shell stopped sending.
    Cancel { id: ClientOrderId, why: String },
}

/// What a recorded account frame says, once routed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Heard {
    /// An event the translator read: a report about one of our orders,
    /// someone else's, another symbol's, the stream's termination, or an
    /// event it does not act on.
    Report(Inbound),
    /// An event the translator could not read, and whose order it was about,
    /// as far as it read.
    Unreadable {
        concerns: Concerns,
        detail: VenueText,
    },
    /// A frame `classify` could not read at all: not JSON, or an event with
    /// no type. It could have been a fill (invariant 5), and it names no
    /// order.
    Unrecognised(VenueText),
    /// The subscription's answer, accepting: the account is heard from here.
    Subscribed,
    /// The venue's `serverShutdown`.
    ServerShutdown,
    /// Any other answer: a refused subscription, or one to no request we
    /// sent. Counted. The listener ends or stops the connection on a failure,
    /// and the gap it records next is the loss.
    Answer,
}

/// The outcome of a recheck.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Check {
    Passed,
    Failed,
}

/// Whether the shell's outbound queue can take an order now, which the
/// machine cannot know and [`Merge::ready`] needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Queue {
    Room,
    /// No room, a cancel is held, or the queue is closed.
    Full,
}

/// What the machine counted and did not journal.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MergeStats {
    /// Account events it does not act on: other symbols' reports, balance
    /// updates, a stranger's cancel-replace.
    pub counted: u64,
    /// Answers other than an accepting subscription.
    pub answers: u64,
}

/// What REST said about an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rest {
    Pending,
    Placed,
    Unknown,
    Refused,
    NotSent,
}

/// Which authority ended an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ender {
    Rest,
    Stream,
}

/// The design's phases, read off an [`Order`]'s fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Not heard, not ended.
    Sent,
    /// Heard, not ended.
    Live,
    Ended(Ender),
}

/// The account stream, as far as the machine has heard it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Account {
    /// Not heard. `overflowed` when our copy lost records while down, which
    /// may have swallowed the subscription's answer, so that the next account
    /// event of any kind is evidence it is heard.
    Down {
        overflowed: bool,
    },
    Heard,
    /// The listener stopped for good: down for the rest of the process.
    Closed,
}

/// One order this machine sent, and everything heard about it.
#[derive(Debug, Clone)]
struct Order {
    qty: Qty,
    rest: Rest,
    /// The venue's id, from whichever input names it first.
    venue_id: Option<VenueOrderId>,
    /// Some stream report about it has arrived, held or released: the venue
    /// has the order.
    mentioned: bool,
    /// Some stream report about it has been released: the stream's account of
    /// it is continuous from its start.
    heard: bool,
    /// What its released trades add up to.
    booked: Qty,
    /// Trade ids already accounted for: booked, or handed over as an orphan.
    trades: BTreeSet<VenueTradeId>,
    ended: Option<Ender>,
    /// Doubt causes raised and not cleared, in the order they were raised.
    open: Vec<DoubtCause>,
    /// Doubt causes waiting for REST or the stream to say whether the order
    /// exists.
    deferred: Vec<DoubtCause>,
    /// Reports held behind a hole in the cumulative quantity.
    held: Vec<OrderReport>,
}

/// The merge machine. See the module docs.
#[derive(Debug)]
pub struct Merge {
    /// The one instrument this venue holds venue-sourced filters for.
    instrument: InstrumentId,
    translator: Translator,
    /// Every order sent, ended ones included: an ended order's ids and end
    /// are what a late report is judged against. Ordered by id, so that
    /// "every working order" is put in doubt in one order in every run.
    orders: BTreeMap<ClientOrderId, Order>,
    account: Account,
    /// The latch: set by every contradiction, every orphaned fill, a foreign
    /// trade on our symbol, read or not, and `IdInUse`, and never cleared in
    /// the process.
    diverged: bool,
    /// False after a failing recheck, true after a passing one.
    checked: bool,
    stats: MergeStats,
}

impl Merge {
    /// A machine for `instrument`, whose reports `translator` reads.
    ///
    /// Down until the account is heard. Checked, because the shell that owns
    /// it is built only from a passing `venue-check` (the next pull request's
    /// `wire`, taking a `Checked`); a recheck that fails during the run
    /// is what makes it unchecked.
    #[must_use]
    pub const fn new(instrument: InstrumentId, translator: Translator) -> Self {
        Self {
            instrument,
            translator,
            orders: BTreeMap::new(),
            account: Account::Down { overflowed: false },
            diverged: false,
            checked: true,
            stats: MergeStats {
                counted: 0,
                answers: 0,
            },
        }
    }

    /// Whether this venue can take an order for `instrument` now, given the
    /// shell's `queue`.
    ///
    /// When more than one reason holds, the refusal names one, in this order:
    /// what this venue can never send, then what needs a person, then what
    /// needs a check, then what clears by itself (§3). A `refused` line then
    /// points its reader at the cause that will last longest.
    ///
    /// # Errors
    ///
    /// [`Unready`], saying why not.
    pub fn ready(&self, instrument: InstrumentId, queue: Queue) -> Result<(), Unready> {
        if instrument != self.instrument {
            return Err(Unready::NoFilters);
        }
        if self.diverged {
            return Err(Unready::Diverged);
        }
        if !self.checked {
            return Err(Unready::Unchecked);
        }
        match self.account {
            Account::Heard => {}
            Account::Down { .. } | Account::Closed => return Err(Unready::StreamDown),
        }
        if self.orders.values().any(Order::in_doubt) {
            return Err(Unready::InDoubt);
        }
        match queue {
            Queue::Room => Ok(()),
            Queue::Full => Err(Unready::QueueFull),
        }
    }

    /// Whether the latch is set.
    #[must_use]
    pub const fn diverged(&self) -> bool {
        self.diverged
    }

    #[must_use]
    pub const fn stats(&self) -> MergeStats {
        self.stats
    }

    /// What a recorded account frame says, received at `at`.
    ///
    /// `classify` decides before the translator is asked anything: an answer
    /// and an unrecognised frame never reach it, and `serverShutdown`, a
    /// connection event rather than an account's, is read here.
    #[must_use]
    pub fn route(&self, payload: &[u8], at: Ts) -> Heard {
        match account::classify(payload) {
            AccountFrame::Event { kind } if kind == "serverShutdown" => Heard::ServerShutdown,
            AccountFrame::Event { kind: _ } => match self.translator.read(payload, at) {
                Ok(inbound) => Heard::Report(inbound),
                Err(Unreadable { concerns, error }) => Heard::Unreadable {
                    concerns,
                    detail: VenueText::new(&error.to_string()),
                },
            },
            AccountFrame::Answer(answer) => {
                if answer.request == Some(Request::Subscribe) && answer.ok() {
                    Heard::Subscribed
                } else {
                    Heard::Answer
                }
            }
            AccountFrame::Unrecognised(why) => Heard::Unrecognised(VenueText::new(&why)),
        }
    }

    /// [`Self::route`] a frame and [`Self::step`] what it says.
    pub fn frame(&mut self, at: Ts, payload: &[u8], out: &mut Vec<VenueReport>) {
        let heard = self.route(payload, at);
        self.step(at, Input::Heard(heard), out);
    }

    /// Hear `input` at `at`, and append what the engine is told.
    pub fn step(&mut self, at: Ts, input: Input, out: &mut Vec<VenueReport>) {
        match input {
            Input::Submitted { id, qty } => self.submitted(at, id, qty, out),
            Input::Placement { id, placement } => self.placement(at, id, &placement, out),
            Input::CancelReply { id, reply } => self.cancel_reply(at, id, &reply, out),
            Input::Unsent(Unsent::Order { id, why }) => {
                // The shell may refuse an order before it says one is about
                // to go, so this needs no `Submitted`. After one, it is the
                // request's one answer, through the same guard as REST's.
                self.orders
                    .entry(id)
                    .or_insert_with(|| Order::new(Qty::ZERO));
                self.placement(at, id, &Placement::NotSent { why }, out);
            }
            Input::Unsent(Unsent::Cancel { id, why }) => out.push(notice(Notice::CancelRefused {
                client_order_id: id,
                why: CancelRefusal::NotSent {
                    why: VenueText::new(&why),
                },
                ts: at,
            })),
            Input::Heard(heard) => self.heard(at, heard, out),
            Input::Blind(cause) => match (self.account, cause) {
                // A hole the tee left in our copy: in the ordinary case the
                // subscription never ended, only our copy of it.
                (Account::Heard, GapCause::LocalOverflow) => {
                    self.lose(at, AccountCause::LocalOverflow, out);
                    self.regain(at, out);
                }
                // The connection ended, for a reason the record does not
                // carry. A capture records `Disconnect` whatever ended it;
                // the other two never arrive mid-session, and if one did it
                // would mean the same.
                (
                    Account::Heard,
                    GapCause::Disconnect | GapCause::SequenceGap | GapCause::RecorderRestart,
                ) => self.lose(at, AccountCause::SocketClosed, out),
                (Account::Down { .. }, GapCause::LocalOverflow) => {
                    self.account = Account::Down { overflowed: true };
                }
                // A new connection begins with its own answer, so an overflow
                // before the connection ended no longer stands in for one.
                (
                    Account::Down { .. },
                    GapCause::Disconnect | GapCause::SequenceGap | GapCause::RecorderRestart,
                ) => self.account = Account::Down { overflowed: false },
                (
                    Account::Closed,
                    GapCause::LocalOverflow
                    | GapCause::Disconnect
                    | GapCause::SequenceGap
                    | GapCause::RecorderRestart,
                ) => {}
            },
            Input::Closed => {
                match self.account {
                    // The listener records a `Disconnect` before it stops, so
                    // this is the one place a loss reaches the machine without
                    // its gap. It is a loss all the same.
                    Account::Heard => self.lose(at, AccountCause::SocketClosed, out),
                    Account::Down { .. } | Account::Closed => {}
                }
                self.account = Account::Closed;
            }
            Input::Check(check) => {
                self.checked = match check {
                    Check::Passed => true,
                    Check::Failed => false,
                };
            }
            Input::Unheard { id } => {
                if let Some(order) = self.orders.get_mut(&id) {
                    if order.ended.is_none() && !order.mentioned {
                        order.defer_or_raise(id, DoubtCause::FillsMissed, at, out);
                    }
                }
            }
        }
    }

    fn submitted(&mut self, at: Ts, id: ClientOrderId, qty: Qty, out: &mut Vec<VenueReport>) {
        if self.orders.contains_key(&id) {
            self.contradiction(
                Some(id),
                "a second submission under an id already sent",
                at,
                out,
            );
            return;
        }
        self.orders.insert(id, Order::new(qty));
    }

    /// The REST answer to placing an order: the design's rows for `Placed`,
    /// `Refused`, `NotSent` and `Unknown`, in every phase.
    fn placement(
        &mut self,
        at: Ts,
        id: ClientOrderId,
        placement: &Placement,
        out: &mut Vec<VenueReport>,
    ) {
        let Some(order) = self.orders.get(&id) else {
            self.contradiction(
                Some(id),
                "a REST answer about an order this process never sent",
                at,
                out,
            );
            return;
        };
        // A request is answered once, by REST or by the shell's `Unsent`: a
        // second answer is our own plumbing telling two stories about one
        // request.
        if order.rest != Rest::Pending {
            self.contradiction(Some(id), "a second answer to one order's request", at, out);
            return;
        }
        match placement {
            Placement::Placed {
                order_id,
                client_order_id: _,
                transact_time_ms,
            } => {
                if let Some(named) = self.other_venue_id(id, order_id) {
                    self.contradiction(Some(id), &named, at, out);
                    return;
                }
                let Some(order) = self.orders.get_mut(&id) else {
                    return;
                };
                order.rest = Rest::Placed;
                order.venue_id.get_or_insert_with(|| order_id.clone());
                out.push(notice(Notice::Placed {
                    client_order_id: id,
                    venue_order_id: order_id.clone(),
                    transact_ms: *transact_time_ms,
                    ts: at,
                }));
                // The order may have been live in the dark.
                if order.ended.is_none() {
                    order.raise_deferred(id, at, out);
                }
            }
            Placement::Refused(_) => {
                self.refused_by_rest(at, id, Rest::Refused, placement, out);
            }
            Placement::NotSent { .. } => {
                self.refused_by_rest(at, id, Rest::NotSent, placement, out);
            }
            Placement::Unknown { why } => {
                let cause = why.cause();
                let said = why.said();
                // Some order is open under a name of ours that should mean
                // one order, in every phase.
                if cause == DoubtCause::IdInUse {
                    self.diverged = true;
                }
                let Some(order) = self.orders.get_mut(&id) else {
                    return;
                };
                order.rest = Rest::Unknown;
                match order.phase() {
                    // The stream has said the order exists and what happened
                    // to it: raised and cleared at once, which is what makes
                    // "REST answered first" and "the stream answered first"
                    // write the same lines.
                    Phase::Live | Phase::Ended(Ender::Stream) => {
                        if !order.open.contains(&cause) {
                            out.push(doubt(id, raised(cause, said), at));
                            out.push(doubt(id, cleared(), at));
                        }
                    }
                    // Mentioned or not, nothing past a hole is known: raised,
                    // and open, beside a held report's own `fills_missed`.
                    Phase::Sent => {
                        order.raise(id, cause, said, at, out);
                        order.raise_deferred(id, at, out);
                    }
                    // REST answers once, and this answer is the first.
                    Phase::Ended(Ender::Rest) => {}
                }
            }
        }
    }

    /// A refusal by REST, or a request never sent: certain, so terminal,
    /// unless the stream has already mentioned the order, in which case the
    /// classifier was wrong about the venue and the order stays working.
    ///
    /// What the rejection says comes from `orders.rs`'s own mapping,
    /// [`Placement::reject_reason`] and [`Placement::rejected_why`], so that
    /// the venue's words and the filter it named are written down by one
    /// piece of code wherever a rejection is made.
    fn refused_by_rest(
        &mut self,
        at: Ts,
        id: ClientOrderId,
        rest: Rest,
        placement: &Placement,
        out: &mut Vec<VenueReport>,
    ) {
        // Both are `Some` for exactly the two answers that come here, a
        // refusal and a request never sent; anything else is our plumbing.
        let (Some(reason), Some(why)) = (placement.reject_reason(), placement.rejected_why())
        else {
            self.contradiction(
                Some(id),
                "an answer that is no rejection, handled as one",
                at,
                out,
            );
            return;
        };
        let Some(order) = self.orders.get_mut(&id) else {
            self.contradiction(
                Some(id),
                "a REST answer about an order this process never sent",
                at,
                out,
            );
            return;
        };
        order.rest = rest;
        if order.mentioned {
            self.contradiction(
                Some(id),
                "REST says the order does not exist, and the stream has reported on it",
                at,
                out,
            );
            return;
        }
        // An order that never existed missed nothing.
        order.deferred.clear();
        order.ended = Some(Ender::Rest);
        out.push(VenueReport::Order(OrderReport::Rejected {
            client_order_id: id,
            reason,
            why: Some(why),
            ts: at,
        }));
    }

    /// The answer to a cancel: never the order's end, which is the stream's.
    fn cancel_reply(
        &mut self,
        at: Ts,
        id: ClientOrderId,
        reply: &CancelReply,
        out: &mut Vec<VenueReport>,
    ) {
        let Some(why) = reply.refusal() else {
            // Acknowledged: nothing, unless it names another order (rule 6).
            if let CancelReply::Acknowledged { order_id } = reply {
                if let Some(named) = self.other_venue_id(id, order_id) {
                    self.contradiction(Some(id), &named, at, out);
                } else if let Some(order) = self.orders.get_mut(&id) {
                    order.venue_id.get_or_insert_with(|| order_id.clone());
                }
            }
            return;
        };
        // An unknown cancel puts nothing in doubt: the stream decides whether
        // the order ended, and is still listening.
        out.push(notice(Notice::CancelRefused {
            client_order_id: id,
            why,
            ts: at,
        }));
    }

    /// A routed account frame.
    fn heard(&mut self, at: Ts, heard: Heard, out: &mut Vec<VenueReport>) {
        // An account event of any kind, after an overflow while down, is the
        // sign the account is heard: the venue sends them only on an active
        // subscription, and the hole may have swallowed its answer.
        let event = match &heard {
            Heard::Report(inbound) => match inbound {
                Inbound::Ours(_)
                | Inbound::Foreign(_)
                | Inbound::OtherSymbol(_)
                | Inbound::Ignored(_) => true,
                Inbound::StreamTerminated => false,
            },
            Heard::Unreadable { .. } => true,
            Heard::Unrecognised(_) | Heard::Subscribed | Heard::ServerShutdown | Heard::Answer => {
                false
            }
        };
        if event && self.account == (Account::Down { overflowed: true }) {
            self.regain(at, out);
        }
        match heard {
            Heard::Report(Inbound::Ours(report)) => self.ours(at, report, out),
            Heard::Report(Inbound::Foreign(report)) => {
                // A foreign trade on our symbol, whoever made it: the account
                // and our record no longer describe the same thing.
                match report.what {
                    ForeignOutcome::Traded { .. } => self.diverged = true,
                    ForeignOutcome::Accepted | ForeignOutcome::Ended { .. } => {}
                }
                out.push(notice(Notice::Foreign(report)));
            }
            Heard::Report(Inbound::OtherSymbol(_) | Inbound::Ignored(_)) => {
                self.stats.counted += 1;
            }
            Heard::Report(Inbound::StreamTerminated) => {
                if self.account == Account::Heard {
                    self.lose(at, AccountCause::StreamTerminated, out);
                }
            }
            Heard::Unreadable { concerns, detail } => self.unreadable(at, concerns, detail, out),
            Heard::Unrecognised(detail) => {
                self.unreadable(at, Concerns::Unknown, detail, out);
            }
            Heard::Subscribed => match self.account {
                Account::Down { .. } => self.regain(at, out),
                // An accepting answer comes only on a new connection, so an
                // overflow's hole hid a disconnect, and orders sent since may
                // have been reported in the dark.
                Account::Heard => {
                    self.lose(at, AccountCause::LocalOverflow, out);
                    self.regain(at, out);
                }
                // The feed is closed; nothing comes after.
                Account::Closed => {}
            },
            Heard::ServerShutdown => {
                if self.account == Account::Heard {
                    self.lose(at, AccountCause::ServerShutdown, out);
                }
            }
            Heard::Answer => self.stats.answers += 1,
        }
    }

    /// A report that could not be read: an anomaly always, `fills_missed` on
    /// whichever working orders it could have been about, and the latch for a
    /// stranger's trade.
    fn unreadable(
        &mut self,
        at: Ts,
        concerns: Concerns,
        detail: VenueText,
        out: &mut Vec<VenueReport>,
    ) {
        let (client_order_id, what) = match concerns {
            Concerns::Ours(id) => (Some(id), AnomalyKind::Unreadable),
            Concerns::Foreign | Concerns::Unknown => (None, AnomalyKind::Unreadable),
            Concerns::ForeignTrade => (None, AnomalyKind::UnreadableForeignTrade),
        };
        out.push(notice(Notice::Anomaly {
            client_order_id,
            what,
            detail,
            ts: at,
        }));
        match concerns {
            Concerns::Ours(id) => {
                let Some(order) = self.orders.get_mut(&id) else {
                    // An id of this run this process never sent: nothing of
                    // ours to doubt.
                    return;
                };
                match order.phase() {
                    // The stream reported on the order, so the venue has it.
                    // What it said may have been a fill.
                    Phase::Sent | Phase::Live => {
                        order.mentioned = true;
                        order.raise_deferred(id, at, out);
                        order.raise(id, DoubtCause::FillsMissed, None, at, out);
                    }
                    // REST said there is no such order.
                    Phase::Ended(Ender::Rest) => self.contradiction(
                        Some(id),
                        "the stream reported on an order REST says does not exist",
                        at,
                        out,
                    ),
                    // Nothing is working to doubt; the anomaly is the record.
                    Phase::Ended(Ender::Stream) => {}
                }
            }
            // Read far enough to know it was not ours.
            Concerns::Foreign => {}
            // Not ours, and it moved the balances: the latch, as for a
            // stranger's trade that could be read.
            Concerns::ForeignTrade => self.diverged = true,
            // It could have been any of ours.
            Concerns::Unknown => self.doubt_every_working_order(DoubtCause::FillsMissed, at, out),
        }
    }

    /// A stream report about one of this run's orders, judged by the design's
    /// *Precedence*: the first step that applies decides it alone.
    // One function for one ordered list, so the order is read top to bottom.
    #[allow(clippy::too_many_lines)]
    fn ours(&mut self, at: Ts, report: OrderReport, out: &mut Vec<VenueReport>) {
        let id = report.client_order_id();
        if !self.orders.contains_key(&id) {
            // An id of this run this process never sent, which only an
            // earlier process of the same run can leave. The engine is not
            // working it either, and withholds it as a stray or an orphan.
            match &report {
                OrderReport::Filled { .. } => self.diverged = true,
                OrderReport::Accepted { .. }
                | OrderReport::Rejected { .. }
                | OrderReport::Cancelled { .. } => {}
            }
            out.push(VenueReport::Order(report));
            return;
        }
        // What the report says, as the rules read it. A rejection never
        // comes from the stream (rule 1): the translator refuses the stream's
        // `REJECTED`, so one here is our model of the venue being wrong.
        let (venue_id, kind) = match &report {
            OrderReport::Accepted { venue_order_id, .. } => (venue_order_id.clone(), Kind::New),
            OrderReport::Filled {
                fill,
                remaining,
                ids,
                ..
            } => (
                ids.as_ref().map(|ids| ids.venue_order_id.clone()),
                Kind::Trade {
                    qty: fill.qty,
                    remaining: *remaining,
                    trade_id: ids
                        .as_ref()
                        .map(|FillIds { trade_id, .. }| trade_id.clone()),
                },
            ),
            OrderReport::Cancelled {
                remaining, ended, ..
            } => (
                ended.as_ref().map(|ended| ended.venue_order_id.clone()),
                Kind::End {
                    remaining: *remaining,
                },
            ),
            OrderReport::Rejected { .. } => {
                self.contradiction(
                    Some(id),
                    "the stream reported a rejection, which comes from REST alone",
                    at,
                    out,
                );
                return;
            }
        };

        // 1. Another venue id for the order: two orders answering to one name
        //    of ours. Changes nothing else.
        if let Some(named) = venue_id
            .as_ref()
            .and_then(|venue_id| self.other_venue_id(id, venue_id))
        {
            self.contradiction(Some(id), &named, at, out);
            return;
        }
        let Some(order) = self.orders.get_mut(&id) else {
            return;
        };
        if let Some(venue_id) = venue_id {
            order.venue_id.get_or_insert(venue_id);
        }

        // 2. A trade id already accounted for books nothing, in every phase:
        //    a resent copy of an order's last trade is a duplicate, not a fill
        //    after its end.
        if let Kind::Trade {
            trade_id: Some(trade_id),
            ..
        } = &kind
        {
            if order.trades.contains(trade_id) {
                out.push(anomaly(
                    Some(id),
                    AnomalyKind::DuplicateTrade,
                    &format!("trade {} is already accounted for", trade_id.0),
                    at,
                ));
                return;
            }
        }

        // 3. The order has ended.
        match order.phase() {
            Phase::Ended(Ender::Rest) => {
                // The classifier was wrong about the venue: an order exists
                // that our record says does not. A trade among them is an
                // orphan too, since the engine has forgotten the order.
                if let Kind::Trade {
                    trade_id: Some(trade_id),
                    ..
                } = &kind
                {
                    order.trades.insert(trade_id.clone());
                }
                let traded = matches!(kind, Kind::Trade { .. });
                self.contradiction(
                    Some(id),
                    "the stream reported on an order REST says does not exist",
                    at,
                    out,
                );
                if traded {
                    out.push(VenueReport::Order(report));
                }
                return;
            }
            Phase::Ended(Ender::Stream) => {
                match &kind {
                    Kind::Trade { trade_id, .. } => {
                        if let Some(trade_id) = trade_id {
                            order.trades.insert(trade_id.clone());
                        }
                        out.push(anomaly(
                            Some(id),
                            AnomalyKind::AfterEnd,
                            "a trade after the order's end",
                            at,
                        ));
                        // Handed over, so the engine writes it down as an
                        // orphan rather than the record dropping money when
                        // the venue is wrong. Every orphaned fill latches.
                        out.push(VenueReport::Order(report));
                        self.diverged = true;
                    }
                    Kind::End { .. } => out.push(anomaly(
                        Some(id),
                        AnomalyKind::SecondEnd,
                        "a second end for the order",
                        at,
                    )),
                    Kind::New => out.push(anomaly(
                        Some(id),
                        AnomalyKind::AfterEnd,
                        "a NEW after the order's end",
                        at,
                    )),
                }
                return;
            }
            Phase::Sent | Phase::Live => {}
        }

        // 4. A NEW about an order already heard, whatever its `z`.
        if kind == Kind::New && order.heard {
            self.contradiction(Some(id), "a second NEW for an order already heard", at, out);
            return;
        }

        // 5 and 6. The cumulative quantity: `z` is the request's quantity
        // less `remaining`, and a NEW always carries zero.
        let (z, expected) = match &kind {
            Kind::New => (Qty::ZERO, order.booked),
            Kind::Trade { qty, remaining, .. } => (order.qty - *remaining, order.booked + *qty),
            Kind::End { remaining } => (order.qty - *remaining, order.booked),
        };
        if z < expected {
            let detail = format!(
                "the stream says {z} has filled, and {} is already booked",
                order.booked
            );
            self.contradiction(Some(id), &detail, at, out);
            return;
        }
        if z > expected || !order.held.is_empty() {
            // Fills we never heard: nothing past the hole is applied, and
            // what came after it is kept.
            order.mentioned = true;
            order.raise_deferred(id, at, out);
            order.raise(id, DoubtCause::FillsMissed, None, at, out);
            order.held.push(report);
            return;
        }

        // 7. Released.
        order.mentioned = true;
        let first = !order.heard;
        order.heard = true;
        if first {
            order.raise_deferred(id, at, out);
        }
        order.clear_all(id, at, out);
        // An acceptance comes first: a trade or an end proves the venue had
        // the order, and every lifecycle told begins with an acceptance.
        if first && kind != Kind::New {
            out.push(VenueReport::Order(OrderReport::Accepted {
                client_order_id: id,
                venue_order_id: order.venue_id.clone(),
                ts: at,
            }));
        }
        match kind {
            Kind::New => {}
            Kind::Trade {
                qty,
                remaining,
                trade_id,
            } => {
                order.booked += qty;
                if let Some(trade_id) = trade_id {
                    order.trades.insert(trade_id);
                }
                if remaining.is_zero() {
                    order.ended = Some(Ender::Stream);
                }
            }
            Kind::End { .. } => order.ended = Some(Ender::Stream),
        }
        out.push(VenueReport::Order(report));
    }

    /// The account is heard from here.
    fn regain(&mut self, at: Ts, out: &mut Vec<VenueReport>) {
        self.account = Account::Heard;
        out.push(notice(Notice::Account {
            change: AccountChange::Regained,
            ts: at,
        }));
    }

    /// The account is lost, for `cause`: every working order is in doubt for
    /// what happened to it in the dark.
    fn lose(&mut self, at: Ts, cause: AccountCause, out: &mut Vec<VenueReport>) {
        self.account = Account::Down { overflowed: false };
        out.push(notice(Notice::Account {
            change: AccountChange::Lost { cause },
            ts: at,
        }));
        self.doubt_every_working_order(DoubtCause::AccountBlind, at, out);
    }

    /// `cause` on every working order, deferred where REST has not answered
    /// and the stream has never mentioned it.
    fn doubt_every_working_order(&mut self, cause: DoubtCause, at: Ts, out: &mut Vec<VenueReport>) {
        for (&id, order) in &mut self.orders {
            if order.ended.is_none() {
                order.defer_or_raise(id, cause, at, out);
            }
        }
    }

    /// Why `venue_id` is not the order's, when the order already has another.
    fn other_venue_id(&self, id: ClientOrderId, venue_id: &VenueOrderId) -> Option<String> {
        let known = self.orders.get(&id)?.venue_id.as_ref()?;
        (known != venue_id).then(|| {
            format!(
                "venue order {} named for an order the venue already called {}",
                venue_id.0, known.0
            )
        })
    }

    /// The venue said two things about one order of ours: an anomaly, and
    /// the latch. Every contradiction latches, so that the kind on a line says
    /// whether the process stopped trading.
    fn contradiction(
        &mut self,
        id: Option<ClientOrderId>,
        detail: &str,
        at: Ts,
        out: &mut Vec<VenueReport>,
    ) {
        self.diverged = true;
        out.push(anomaly(id, AnomalyKind::Contradiction, detail, at));
    }
}

/// What a stream report says, as the rules read it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    New,
    Trade {
        qty: Qty,
        remaining: Qty,
        trade_id: Option<VenueTradeId>,
    },
    End {
        remaining: Qty,
    },
}

impl Order {
    /// An order of `qty` that nothing has been heard about.
    const fn new(qty: Qty) -> Self {
        Self {
            qty,
            rest: Rest::Pending,
            venue_id: None,
            mentioned: false,
            heard: false,
            booked: Qty::ZERO,
            trades: BTreeSet::new(),
            ended: None,
            open: Vec::new(),
            deferred: Vec::new(),
            held: Vec::new(),
        }
    }

    const fn phase(&self) -> Phase {
        match (self.ended, self.heard) {
            (Some(ender), _) => Phase::Ended(ender),
            (None, true) => Phase::Live,
            (None, false) => Phase::Sent,
        }
    }

    /// What readiness refuses on.
    fn in_doubt(&self) -> bool {
        !self.open.is_empty() || !self.deferred.is_empty() || !self.held.is_empty()
    }

    /// Raise `cause`, once while it is open.
    fn raise(
        &mut self,
        id: ClientOrderId,
        cause: DoubtCause,
        said: Option<quant_core::report::Cited>,
        at: Ts,
        out: &mut Vec<VenueReport>,
    ) {
        if !self.open.contains(&cause) {
            self.open.push(cause);
            out.push(doubt(id, raised(cause, said), at));
        }
    }

    /// Raise `cause`, or defer it while REST has not answered and the stream
    /// has never mentioned the order: whether the order exists is not known
    /// yet, and an order that never existed missed nothing.
    fn defer_or_raise(
        &mut self,
        id: ClientOrderId,
        cause: DoubtCause,
        at: Ts,
        out: &mut Vec<VenueReport>,
    ) {
        if self.rest == Rest::Pending && !self.mentioned {
            if !self.deferred.contains(&cause) {
                self.deferred.push(cause);
            }
        } else {
            self.raise(id, cause, None, at, out);
        }
    }

    /// The order exists, or may have: every deferred cause is raised.
    fn raise_deferred(&mut self, id: ClientOrderId, at: Ts, out: &mut Vec<VenueReport>) {
        for cause in std::mem::take(&mut self.deferred) {
            self.raise(id, cause, None, at, out);
        }
    }

    /// The stream has accounted for the order: one `cleared` line per open
    /// cause, so that every raised doubt has its clearing in whatever order
    /// they came.
    fn clear_all(&mut self, id: ClientOrderId, at: Ts, out: &mut Vec<VenueReport>) {
        for _ in std::mem::take(&mut self.open) {
            out.push(doubt(id, cleared(), at));
        }
    }
}

const fn notice(notice: Notice) -> VenueReport {
    VenueReport::Notice(notice)
}

const fn raised(cause: DoubtCause, said: Option<quant_core::report::Cited>) -> DoubtChange {
    DoubtChange::Raised { cause, said }
}

const fn cleared() -> DoubtChange {
    DoubtChange::Cleared {
        how: Clearance::Reported,
    }
}

const fn doubt(id: ClientOrderId, change: DoubtChange, at: Ts) -> VenueReport {
    notice(Notice::Doubt {
        client_order_id: id,
        change,
        ts: at,
    })
}

fn anomaly(id: Option<ClientOrderId>, what: AnomalyKind, detail: &str, at: Ts) -> VenueReport {
    notice(Notice::Anomaly {
        client_order_id: id,
        what,
        detail: VenueText::new(detail),
        ts: at,
    })
}

#[cfg(test)]
mod tests;
