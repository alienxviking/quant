//! What a venue says: an event with what only a real venue knows typed beside
//! it, or a fact that is not an event at all.
//!
//! # Why the detail is not on the event
//!
//! An [`ExecutionEvent`] is what a strategy is told, so every field on it is a
//! field a strategy can read, and a strategy that can tell which venue it is
//! wired to is a bug. Live, a venue knows things a simulator never will: its own
//! id for an order, the id of each trade, the code it cited when it declined,
//! that its account stream dropped. The record needs every one of them,
//! because criterion A compares our journal with the venue's `myTrades` row for
//! row, and the strategy must see none of them.
//!
//! So the detail travels *beside* the event, in an [`OrderReport`] whose
//! [`OrderReport::event`] is the only part a strategy is told. That projection
//! is where venue detail stops. A fact with no event at all, such as a lost
//! stream or an order whose outcome is unknown, is a [`Notice`], which only the
//! record hears.
//!
//! Typed per variant rather than as one struct of optional fields, so a trade id
//! on an acceptance cannot be written down, and ids that belong together arrive
//! together.
//!
//! # Why it lives here
//!
//! The translator in `quant-binance` builds these, and `quant-binance` does not
//! depend on `quant-engine`.
//!
//! # Serialized, though nothing here is written yet
//!
//! Every type the journal will hold derives serde, with its wire form pinned by
//! the tests below, so that the journal entries carrying them can be written
//! against a format that is already fixed. [`Notice`], [`OrderReport`] and
//! [`VenueReport`] do not: each of their variants maps onto a journal entry of
//! its own, not onto one line holding a whole report.

use crate::event::Side;
use crate::execution::{
    ClientOrderId, ExecutionEvent, Fill, RejectReason, VenueOrderId, VenueTradeId,
};
use crate::fixed::{Notional, Px, Qty};
use crate::instrument::FilterBreach;
use crate::time::Ts;

/// One thing a venue said, in the order it arrived.
///
/// Every stamp is when we heard it, never a time the venue wrote (invariant 2):
/// local receipt time live, and the engine clock at generation in the
/// simulator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueReport {
    /// Something happened to an order.
    Order(OrderReport),
    /// A fact the record must hold and the strategy must never see.
    Notice(Notice),
}

impl VenueReport {
    /// When we heard it.
    #[must_use]
    pub const fn ts(&self) -> Ts {
        match self {
            Self::Order(report) => report.ts(),
            Self::Notice(notice) => notice.ts(),
        }
    }
}

impl From<ExecutionEvent> for VenueReport {
    /// An event with nothing beside it, which is everything a simulator says.
    fn from(event: ExecutionEvent) -> Self {
        Self::Order(OrderReport::plain(event))
    }
}

/// An event, with what only a real venue knows typed beside it.
///
/// The strategy is told [`Self::event`] and nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderReport {
    /// The venue has the order and it is live.
    Accepted {
        client_order_id: ClientOrderId,
        /// The venue's id for the order, when it gave one.
        venue_order_id: Option<VenueOrderId>,
        ts: Ts,
    },
    /// The order was refused and does not exist.
    Rejected {
        client_order_id: ClientOrderId,
        reason: RejectReason,
        /// What the venue said, or why the request never reached it.
        why: Option<RejectedWhy>,
        ts: Ts,
    },
    /// Some or all of the order traded: one venue trade.
    Filled {
        client_order_id: ClientOrderId,
        fill: Fill,
        remaining: Qty,
        /// Which order and which trade, in the venue's own names.
        ids: Option<FillIds>,
        ts: Ts,
    },
    /// The order is no longer working and will not fill again.
    Cancelled {
        client_order_id: ClientOrderId,
        remaining: Qty,
        /// Which order, and how the venue says it ended.
        ended: Option<Ended>,
        ts: Ts,
    },
}

impl OrderReport {
    /// The report of `event` with nothing beside it.
    #[must_use]
    pub fn plain(event: ExecutionEvent) -> Self {
        match event {
            ExecutionEvent::Accepted {
                client_order_id,
                ts,
            } => Self::Accepted {
                client_order_id,
                venue_order_id: None,
                ts,
            },
            ExecutionEvent::Rejected {
                client_order_id,
                reason,
                ts,
            } => Self::Rejected {
                client_order_id,
                reason,
                why: None,
                ts,
            },
            ExecutionEvent::Filled {
                client_order_id,
                fill,
                remaining,
                ts,
            } => Self::Filled {
                client_order_id,
                fill,
                remaining,
                ids: None,
                ts,
            },
            ExecutionEvent::Cancelled {
                client_order_id,
                remaining,
                ts,
            } => Self::Cancelled {
                client_order_id,
                remaining,
                ended: None,
                ts,
            },
        }
    }

    /// What the strategy is told: the event, and none of the detail.
    ///
    /// Every field is named rather than skipped with `..`, so a field added to
    /// a report has to be decided here, one way or the other, before the crate
    /// builds.
    #[must_use]
    pub fn event(&self) -> ExecutionEvent {
        match self {
            Self::Accepted {
                client_order_id,
                venue_order_id: _,
                ts,
            } => ExecutionEvent::Accepted {
                client_order_id: *client_order_id,
                ts: *ts,
            },
            Self::Rejected {
                client_order_id,
                reason,
                why: _,
                ts,
            } => ExecutionEvent::Rejected {
                client_order_id: *client_order_id,
                reason: *reason,
                ts: *ts,
            },
            Self::Filled {
                client_order_id,
                fill,
                remaining,
                ids: _,
                ts,
            } => ExecutionEvent::Filled {
                client_order_id: *client_order_id,
                fill: fill.clone(),
                remaining: *remaining,
                ts: *ts,
            },
            Self::Cancelled {
                client_order_id,
                remaining,
                ended: _,
                ts,
            } => ExecutionEvent::Cancelled {
                client_order_id: *client_order_id,
                remaining: *remaining,
                ts: *ts,
            },
        }
    }

    /// The order this concerns.
    #[must_use]
    pub const fn client_order_id(&self) -> ClientOrderId {
        match self {
            Self::Accepted {
                client_order_id, ..
            }
            | Self::Rejected {
                client_order_id, ..
            }
            | Self::Filled {
                client_order_id, ..
            }
            | Self::Cancelled {
                client_order_id, ..
            } => *client_order_id,
        }
    }

    /// When we heard it.
    #[must_use]
    pub const fn ts(&self) -> Ts {
        match self {
            Self::Accepted { ts, .. }
            | Self::Rejected { ts, .. }
            | Self::Filled { ts, .. }
            | Self::Cancelled { ts, .. } => *ts,
        }
    }

    /// Whether nothing is beside the event, which is what every simulated
    /// report should be.
    #[must_use]
    pub const fn is_plain(&self) -> bool {
        match self {
            Self::Accepted {
                client_order_id: _,
                venue_order_id,
                ts: _,
            } => venue_order_id.is_none(),
            Self::Rejected {
                client_order_id: _,
                reason: _,
                why,
                ts: _,
            } => why.is_none(),
            Self::Filled {
                client_order_id: _,
                fill: _,
                remaining: _,
                ids,
                ts: _,
            } => ids.is_none(),
            Self::Cancelled {
                client_order_id: _,
                remaining: _,
                ended,
                ts: _,
            } => ended.is_none(),
        }
    }
}

/// The venue's names for one fill: one `myTrades` row's order and trade.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FillIds {
    pub venue_order_id: VenueOrderId,
    pub trade_id: VenueTradeId,
}

/// How the venue says an order ended without filling in full.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Ended {
    pub venue_order_id: VenueOrderId,
    pub how: EndedBy,
}

/// Binance's `CANCELED`, `EXPIRED` and `TRADE_PREVENTION`.
///
/// One outcome to the engine, which needs only that the order is over and what
/// never traded. Three to a person reconciling against the venue, who needs to
/// know which.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndedBy {
    /// Withdrawn on request.
    Cancelled,
    /// Ended by the venue: how a market order ends when the book runs out
    /// before it does.
    Expired,
    /// Ended by self-trade prevention, against an order of the same account.
    SelfTradePrevention,
}

/// Why an order was rejected, beyond the [`RejectReason`] the strategy hears.
///
/// A simulator's rejection carries none: it has nothing to cite, and a request
/// to it always lands.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectedWhy {
    /// The venue had the request and refused it, citing this. `breach` is the
    /// filter rule it names, when it names one we model: criterion B compares
    /// it with the rule `Filters::rejects` names.
    Declined {
        cited: Cited,
        breach: Option<FilterBreach>,
    },
    /// The request never became an order the venue had.
    NeverLanded(Landing),
}

/// What the venue said, as it said it.
///
/// The only durable copy there will be of a REST answer: the account stream is
/// recorded in raw, and REST response bodies are not.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Cited {
    /// `None` when the answer did not come over HTTP.
    pub http_status: Option<u16>,
    /// The venue's error code, verbatim.
    pub code: i64,
    pub msg: VenueText,
}

/// Why a request never became an order the venue had.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Landing {
    /// The request never left. Certain, so it is terminal.
    NotSent { why: VenueText },
    /// A lookup after the fact found no such order, with what the venue said
    /// when it did not find it. Defined now so the record's set is complete;
    /// the lookup is slice (d)'s.
    NotFound { said: Option<Cited> },
}

/// Text from the venue, cut to [`VenueText::MAX`] bytes on a character
/// boundary.
///
/// Remote text bound for a file kept for good: a garbled body must not be able
/// to bloat the journal, and a cut through a character would leave a line that
/// is not valid UTF-8.
///
/// Reading one back goes through the same cut. A derived `Deserialize` would
/// build one of any length from whatever a file held, so the bound would hold
/// only for text this process made.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(transparent)]
pub struct VenueText(String);

impl<'de> serde::Deserialize<'de> for VenueText {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Ok(Self::new(&text))
    }
}

impl VenueText {
    /// The most bytes kept.
    pub const MAX: usize = 256;

    /// `text`, or as much of it as fits in [`Self::MAX`] bytes without cutting
    /// a character.
    #[must_use]
    pub fn new(text: &str) -> Self {
        let mut end = text.len().min(Self::MAX);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        Self(text[..end].to_owned())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A fact the record must hold and the strategy must never see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Notice {
    /// The venue's REST acknowledgement of an order (`Placement::Placed`).
    ///
    /// Not an acceptance: the account stream is the one authority for that
    /// (`docs/live-run.md` §5, *one authority per fact*). But its order id is
    /// the one that still finds the order after a cancel has replaced the
    /// order's client id (`docs/live-run.md`, criterion E).
    Placed {
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
        /// The venue's `transactTime`, in milliseconds on the venue's clock.
        /// For the record only: the engine dispatches on when we heard.
        transact_ms: i64,
        ts: Ts,
    },
    /// Whether an order exists, or what happened to it, stopped being known,
    /// or is known again.
    ///
    /// An unknown outcome is never an event. A `Rejected` would remove the
    /// order from the engine's ledger, and the real fill a second later would
    /// arrive as an orphan.
    Doubt {
        client_order_id: ClientOrderId,
        change: DoubtChange,
        ts: Ts,
    },
    /// A cancel the venue did not carry out, or whose fate is unknown. The
    /// order, if there is one, is as it was, and the stream says how it ends.
    CancelRefused {
        client_order_id: ClientOrderId,
        why: CancelRefusal,
        ts: Ts,
    },
    /// A report about an order this run did not send.
    Foreign(ForeignReport),
    /// The account stream was lost or regained.
    Account { change: AccountChange, ts: Ts },
    /// A report that could not be read, or contradicts what came before it.
    /// Recorded with its text rather than dropped.
    Anomaly {
        client_order_id: Option<ClientOrderId>,
        what: AnomalyKind,
        detail: VenueText,
        ts: Ts,
    },
}

impl Notice {
    /// When we heard it.
    #[must_use]
    pub const fn ts(&self) -> Ts {
        match self {
            Self::Placed { ts, .. }
            | Self::Doubt { ts, .. }
            | Self::CancelRefused { ts, .. }
            | Self::Account { ts, .. }
            | Self::Anomaly { ts, .. } => *ts,
            Self::Foreign(report) => report.ts,
        }
    }
}

/// An order's outcome became unknown, or known again.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoubtChange {
    /// Unknown from here, for this reason, with what the venue said if it said
    /// anything.
    Raised {
        cause: DoubtCause,
        said: Option<Cited>,
    },
    /// Known again, and how.
    Cleared { how: Clearance },
}

/// Why an order's outcome is unknown.
///
/// The first eight mirror `quant_binance::orders::WhyUnknown`, one for one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoubtCause {
    /// A 5xx: the venue says the outcome is unknown and could have been a
    /// success.
    ServerError,
    /// A venue error whose code does not say the request was refused.
    Unsettled,
    /// An open order already holds the id this one was sent under, and it may
    /// be this one.
    IdInUse,
    /// An answer that is not the answer expected.
    Unreadable,
    /// An answer naming another order than the one the request named.
    OtherOrder,
    /// An HTTP status the venue does not document for the request.
    UnexpectedStatus,
    /// No answer before the client's own timeout.
    NoAnswer,
    /// The request failed after it may already have left.
    Interrupted,
    /// The account stream was lost while the order was working, so whatever
    /// happened to it in between was not heard.
    AccountBlind,
    /// A report about the order could not be read, so a fill may have gone
    /// unbooked.
    FillsMissed,
    /// The order was working when an earlier process stopped. Slice (d)'s.
    Restart,
}

/// How an order's outcome came to be known again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Clearance {
    /// The stream reported on it, and what it reported accounts for every fill.
    Reported,
    /// Looked up at the venue. Slice (d)'s.
    LookedUp,
}

/// Why a cancel did not take effect, mirroring the venue client's answer to
/// one (`quant_binance::orders::CancelReply`), less its acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelRefusal {
    /// The venue has no open order by that id: already filled, which is how a
    /// cancel that lost the race comes back, or already over, or never placed.
    NotOpen(Cited),
    /// The venue refused the cancel.
    Refused(Cited),
    /// Whether the cancel took effect is unknown.
    Unknown {
        cause: DoubtCause,
        said: Option<Cited>,
    },
    /// The request never left.
    NotSent { why: VenueText },
}

/// A report about an order this run did not send.
///
/// Not serialized as a whole: the journal maps it onto an entry of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignReport {
    /// The client id exactly as the venue sent it. Text on purpose: making it a
    /// `ClientOrderId` would let an earlier run's order 5 be booked as this
    /// run's order 5.
    pub client_id: VenueText,
    pub provenance: Provenance,
    pub venue_order_id: VenueOrderId,
    pub side: Side,
    pub what: ForeignOutcome,
    pub ts: Ts,
}

/// Whose an order this run did not send is.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Ours, from the run with this tag.
    OtherRun(VenueText),
    /// Begins with our prefix, and is nothing we would have sent.
    Unparseable,
    /// Not placed by this system at all.
    NotOurs,
}

/// What happened to an order this run did not send.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignOutcome {
    Accepted,
    /// It traded, which moved the account's balances outside our record.
    ///
    /// `commission_asset` is kept verbatim rather than mapped onto a
    /// `FeeAsset`, so a stranger who pays in BNB is recorded rather than
    /// refused.
    Traded {
        trade_id: VenueTradeId,
        px: Px,
        qty: Qty,
        commission: Notional,
        commission_asset: VenueText,
        remaining: Qty,
    },
    Ended {
        remaining: Qty,
        how: EndedBy,
    },
}

/// The account stream's state changed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountChange {
    /// No longer subscribed, for this reason. Every working order is in doubt
    /// until the stream is back and has accounted for it.
    Lost { cause: AccountCause },
    /// Subscribed again.
    Regained,
}

/// Why the account stream was lost. The venue does not announce most of these;
/// they show up as transport (`docs/live-run.md` §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountCause {
    SocketClosed,
    /// The socket stayed open and stopped delivering.
    IdleTimeout,
    /// The venue's `serverShutdown`.
    ServerShutdown,
    /// The venue's `eventStreamTerminated`.
    StreamTerminated,
    /// A resubscribe the venue refused or never answered.
    SubscribeFailed,
    /// Our own channel overflowed and reports were dropped.
    LocalOverflow,
    /// We unsubscribed.
    Unsubscribed,
}

/// What was wrong with a report that could not be taken at its word.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyKind {
    /// It could not be read at all.
    Unreadable,
    /// A trade id already booked.
    DuplicateTrade,
    /// A report about an order that had already ended.
    AfterEnd,
    /// A second ending for one order.
    SecondEnd,
    /// It contradicts what was reported before it.
    Contradiction,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::FeeAsset;

    fn qty(s: &str) -> Qty {
        s.parse().expect("qty")
    }

    fn px(s: &str) -> Px {
        s.parse().expect("px")
    }

    /// One event of each kind, every field distinct from every other and none
    /// of them zero, so a projection that dropped, swapped or zeroed a field
    /// would show.
    fn events() -> [ExecutionEvent; 4] {
        [
            ExecutionEvent::Accepted {
                client_order_id: ClientOrderId(11),
                ts: Ts::from_nanos(101),
            },
            ExecutionEvent::Rejected {
                client_order_id: ClientOrderId(12),
                reason: RejectReason::InsufficientFunds,
                ts: Ts::from_nanos(102),
            },
            ExecutionEvent::Filled {
                client_order_id: ClientOrderId(13),
                fill: Fill {
                    px: px("76650.12345678"),
                    qty: qty("0.0004"),
                    fee: "0.03".parse().expect("fee"),
                    is_maker: true,
                    fee_asset: FeeAsset::Base,
                },
                remaining: qty("0.0006"),
                ts: Ts::from_nanos(103),
            },
            ExecutionEvent::Cancelled {
                client_order_id: ClientOrderId(14),
                remaining: qty("0.0007"),
                ts: Ts::from_nanos(104),
            },
        ]
    }

    #[test]
    fn a_plain_report_tells_the_strategy_exactly_the_event_it_was_made_from() {
        // The simulator's whole relationship with this module: every event it
        // makes becomes a report through `plain`, and the strategy is told the
        // report's `event()`. If that round trip moved a field, every backtest
        // would move with it.
        for event in events() {
            let report = OrderReport::plain(event.clone());
            assert_eq!(report.event(), event);
            assert_eq!(report.client_order_id(), event.client_order_id());
            assert_eq!(report.ts(), event.ts());
            assert_eq!(VenueReport::from(event.clone()).ts(), event.ts());
            assert_eq!(
                VenueReport::from(event.clone()),
                VenueReport::Order(OrderReport::plain(event))
            );
        }
    }

    #[test]
    fn nothing_beside_an_event_is_plain_and_anything_beside_it_is_not() {
        for event in events() {
            assert!(OrderReport::plain(event).is_plain());
        }

        let ids = FillIds {
            venue_order_id: VenueOrderId("4293153".to_owned()),
            trade_id: VenueTradeId("12345".to_owned()),
        };
        let detailed = [
            OrderReport::Accepted {
                client_order_id: ClientOrderId(11),
                venue_order_id: Some(VenueOrderId("4293153".to_owned())),
                ts: Ts::from_nanos(101),
            },
            OrderReport::Rejected {
                client_order_id: ClientOrderId(12),
                reason: RejectReason::Venue,
                why: Some(RejectedWhy::NeverLanded(Landing::NotSent {
                    why: VenueText::new("connection refused"),
                })),
                ts: Ts::from_nanos(102),
            },
            OrderReport::Filled {
                client_order_id: ClientOrderId(13),
                fill: Fill {
                    px: px("100"),
                    qty: qty("1"),
                    fee: Notional::ZERO,
                    is_maker: false,
                    fee_asset: FeeAsset::Quote,
                },
                remaining: qty("0"),
                ids: Some(ids),
                ts: Ts::from_nanos(103),
            },
            OrderReport::Cancelled {
                client_order_id: ClientOrderId(14),
                remaining: qty("1"),
                ended: Some(Ended {
                    venue_order_id: VenueOrderId("4293153".to_owned()),
                    how: EndedBy::Expired,
                }),
                ts: Ts::from_nanos(104),
            },
        ];
        for report in detailed {
            assert!(!report.is_plain(), "{report:?}");
        }
    }

    #[test]
    fn every_notice_is_stamped_when_it_was_heard() {
        let cited = Cited {
            http_status: Some(400),
            code: -2011,
            msg: VenueText::new("Unknown order sent."),
        };
        let notices = [
            Notice::Placed {
                client_order_id: ClientOrderId(1),
                venue_order_id: VenueOrderId("4293153".to_owned()),
                transact_ms: 1_499_405_658_657,
                ts: Ts::from_nanos(201),
            },
            Notice::Doubt {
                client_order_id: ClientOrderId(2),
                change: DoubtChange::Raised {
                    cause: DoubtCause::NoAnswer,
                    said: None,
                },
                ts: Ts::from_nanos(202),
            },
            Notice::CancelRefused {
                client_order_id: ClientOrderId(3),
                why: CancelRefusal::NotOpen(cited),
                ts: Ts::from_nanos(203),
            },
            Notice::Foreign(ForeignReport {
                client_id: VenueText::new("q-r0-7"),
                provenance: Provenance::OtherRun(VenueText::new("r0")),
                venue_order_id: VenueOrderId("4293154".to_owned()),
                side: Side::Sell,
                what: ForeignOutcome::Accepted,
                ts: Ts::from_nanos(204),
            }),
            Notice::Account {
                change: AccountChange::Regained,
                ts: Ts::from_nanos(205),
            },
            Notice::Anomaly {
                client_order_id: None,
                what: AnomalyKind::Unreadable,
                detail: VenueText::new("not json"),
                ts: Ts::from_nanos(206),
            },
        ];
        for (n, notice) in (201..).zip(notices) {
            assert_eq!(notice.ts(), Ts::from_nanos(n), "{notice:?}");
            assert_eq!(
                VenueReport::Notice(notice).ts(),
                Ts::from_nanos(n),
                "and as a report"
            );
        }
    }

    #[test]
    fn venue_text_is_cut_to_its_limit_without_cutting_a_character() {
        assert_eq!(
            VenueText::new("Unknown order sent.").as_str(),
            "Unknown order sent."
        );
        let long = "x".repeat(VenueText::MAX + 44);
        assert_eq!(VenueText::new(&long).as_str().len(), VenueText::MAX);
        // A two-byte character straddling the limit is dropped whole.
        let straddling = format!("{}é", "x".repeat(VenueText::MAX - 1));
        let cut = VenueText::new(&straddling);
        assert_eq!(cut.as_str(), "x".repeat(VenueText::MAX - 1));
        // One that ends exactly on it is kept.
        let fitting = format!("{}é", "x".repeat(VenueText::MAX - 2));
        assert_eq!(VenueText::new(&fitting).as_str(), fitting);
    }

    #[test]
    fn venue_text_read_back_is_bounded_as_when_it_was_made() {
        // The bound is a property of the type, not of one constructor: a
        // journal line edited by hand, or written by a build without the cut,
        // must not read back as a `VenueText` no constructor could make.
        let read = |text: &str| -> VenueText {
            serde_json::from_value(serde_json::Value::String(text.to_owned()))
                .expect("any string reads")
        };
        let long = "x".repeat(VenueText::MAX + 44);
        assert_eq!(read(&long), VenueText::new(&long));
        assert_eq!(read(&long).as_str().len(), VenueText::MAX);
        // Cut on a character boundary, as `new` cuts.
        let straddling = format!("{}é", "x".repeat(VenueText::MAX - 1));
        assert_eq!(read(&straddling).as_str(), "x".repeat(VenueText::MAX - 1));
        // And text within the bound reads back as it was.
        assert_eq!(read("Unknown order sent.").as_str(), "Unknown order sent.");
    }

    /// `value` writes exactly `json`, and `json` reads back as `value`.
    fn golden<T>(value: &T, json: &str)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + core::fmt::Debug,
    {
        assert_eq!(serde_json::to_string(value).expect("serialize"), json);
        assert_eq!(
            &serde_json::from_str::<T>(json).expect("deserialize"),
            value,
            "{json}"
        );
    }

    fn cited() -> Cited {
        Cited {
            http_status: Some(400),
            code: -1013,
            msg: VenueText::new("Filter failure: NOTIONAL"),
        }
    }

    const CITED: &str = r#"{"http_status":400,"code":-1013,"msg":"Filter failure: NOTIONAL"}"#;

    #[test]
    fn the_ids_and_endings_have_a_fixed_wire_form() {
        // Pinned before the journal holds any of it, so the entries that will
        // are written against a format that cannot drift by accident.
        golden(
            &FillIds {
                venue_order_id: VenueOrderId("4293153".to_owned()),
                trade_id: VenueTradeId("12345".to_owned()),
            },
            r#"{"venue_order_id":"4293153","trade_id":"12345"}"#,
        );
        golden(
            &Ended {
                venue_order_id: VenueOrderId("4293153".to_owned()),
                how: EndedBy::SelfTradePrevention,
            },
            r#"{"venue_order_id":"4293153","how":"self_trade_prevention"}"#,
        );
        golden(&EndedBy::Cancelled, r#""cancelled""#);
        golden(&EndedBy::Expired, r#""expired""#);
        golden(&VenueText::new("q-r0-7"), r#""q-r0-7""#);
    }

    #[test]
    fn why_an_order_was_rejected_has_a_fixed_wire_form() {
        golden(&cited(), CITED);
        golden(
            &Cited {
                http_status: None,
                code: -1021,
                msg: VenueText::new("Timestamp for this request is outside of the recvWindow."),
            },
            r#"{"http_status":null,"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}"#,
        );
        golden(
            &RejectedWhy::Declined {
                cited: cited(),
                breach: Some(FilterBreach::MinNotional),
            },
            &format!(r#"{{"declined":{{"cited":{CITED},"breach":"min_notional"}}}}"#),
        );
        golden(
            &RejectedWhy::NeverLanded(Landing::NotSent {
                why: VenueText::new("connection refused"),
            }),
            r#"{"never_landed":{"not_sent":{"why":"connection refused"}}}"#,
        );
        golden(
            &Landing::NotFound {
                said: Some(cited()),
            },
            &format!(r#"{{"not_found":{{"said":{CITED}}}}}"#),
        );
    }

    #[test]
    fn doubt_and_refused_cancels_have_a_fixed_wire_form() {
        golden(
            &DoubtChange::Raised {
                cause: DoubtCause::ServerError,
                said: Some(cited()),
            },
            &format!(r#"{{"raised":{{"cause":"server_error","said":{CITED}}}}}"#),
        );
        golden(
            &DoubtChange::Cleared {
                how: Clearance::LookedUp,
            },
            r#"{"cleared":{"how":"looked_up"}}"#,
        );
        for (cause, name) in [
            (DoubtCause::ServerError, "server_error"),
            (DoubtCause::Unsettled, "unsettled"),
            (DoubtCause::IdInUse, "id_in_use"),
            (DoubtCause::Unreadable, "unreadable"),
            (DoubtCause::OtherOrder, "other_order"),
            (DoubtCause::UnexpectedStatus, "unexpected_status"),
            (DoubtCause::NoAnswer, "no_answer"),
            (DoubtCause::Interrupted, "interrupted"),
            (DoubtCause::AccountBlind, "account_blind"),
            (DoubtCause::FillsMissed, "fills_missed"),
            (DoubtCause::Restart, "restart"),
        ] {
            golden(&cause, &format!(r#""{name}""#));
        }
        golden(&Clearance::Reported, r#""reported""#);
        golden(
            &CancelRefusal::NotOpen(cited()),
            &format!(r#"{{"not_open":{CITED}}}"#),
        );
        golden(
            &CancelRefusal::Refused(cited()),
            &format!(r#"{{"refused":{CITED}}}"#),
        );
        golden(
            &CancelRefusal::Unknown {
                cause: DoubtCause::Interrupted,
                said: None,
            },
            r#"{"unknown":{"cause":"interrupted","said":null}}"#,
        );
        golden(
            &CancelRefusal::NotSent {
                why: VenueText::new("queue closed"),
            },
            r#"{"not_sent":{"why":"queue closed"}}"#,
        );
    }

    #[test]
    fn foreign_orders_and_the_account_have_a_fixed_wire_form() {
        golden(
            &Provenance::OtherRun(VenueText::new("r0")),
            r#"{"other_run":"r0"}"#,
        );
        golden(&Provenance::Unparseable, r#""unparseable""#);
        golden(&Provenance::NotOurs, r#""not_ours""#);
        golden(&ForeignOutcome::Accepted, r#""accepted""#);
        // Money as fixed-point raw units, as every other money field is written.
        golden(
            &ForeignOutcome::Traded {
                trade_id: VenueTradeId("77".to_owned()),
                px: px("0.1026"),
                qty: qty("0.2"),
                commission: "0.0002".parse().expect("commission"),
                commission_asset: VenueText::new("BNB"),
                remaining: qty("0.4"),
            },
            r#"{"traded":{"trade_id":"77","px":10260000,"qty":20000000,"commission":20000,"commission_asset":"BNB","remaining":40000000}}"#,
        );
        golden(
            &ForeignOutcome::Ended {
                remaining: qty("0.75"),
                how: EndedBy::Cancelled,
            },
            r#"{"ended":{"remaining":75000000,"how":"cancelled"}}"#,
        );
        golden(
            &AccountChange::Lost {
                cause: AccountCause::ServerShutdown,
            },
            r#"{"lost":{"cause":"server_shutdown"}}"#,
        );
        golden(&AccountChange::Regained, r#""regained""#);
        for (cause, name) in [
            (AccountCause::SocketClosed, "socket_closed"),
            (AccountCause::IdleTimeout, "idle_timeout"),
            (AccountCause::ServerShutdown, "server_shutdown"),
            (AccountCause::StreamTerminated, "stream_terminated"),
            (AccountCause::SubscribeFailed, "subscribe_failed"),
            (AccountCause::LocalOverflow, "local_overflow"),
            (AccountCause::Unsubscribed, "unsubscribed"),
        ] {
            golden(&cause, &format!(r#""{name}""#));
        }
        for (kind, name) in [
            (AnomalyKind::Unreadable, "unreadable"),
            (AnomalyKind::DuplicateTrade, "duplicate_trade"),
            (AnomalyKind::AfterEnd, "after_end"),
            (AnomalyKind::SecondEnd, "second_end"),
            (AnomalyKind::Contradiction, "contradiction"),
        ] {
            golden(&kind, &format!(r#""{name}""#));
        }
    }
}
