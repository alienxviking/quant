//! The user-data stream, translated: what Binance says happened to an order,
//! as the engine's [`OrderReport`] -- the event a strategy is told, with the
//! venue's names for the order and the trade beside it, for the record alone.
//!
//! Pure logic. No network, no credentials, no clock: the transport stamps a
//! message's receive time before it is parsed (invariant 2) and hands both here,
//! so everything below can be tested against bytes, which is the only way to
//! test it before a funded run.
//!
//! # Written against the documentation, not a capture
//!
//! `parse.rs` was written against payloads copied out of a week of recorded
//! market data. There is no equivalent here: nothing in this system has placed a
//! real order yet, so no execution report has ever been recorded. The fixtures
//! are the example in Binance's own documentation (`binance/binance-spot-api-docs`,
//! `user-data-stream.md` at commit `0556f1a`) and variations of it changed one
//! field at a time. That is weaker footing than `parse.rs` had, and it is named
//! rather than hidden: M8's criterion D provokes every lifecycle state on
//! purpose, the reports it records belong beside these fixtures, and any
//! disagreement between the two is this module's model of the venue being wrong.
//!
//! # Whose order is it
//!
//! The stream reports on the **account**, not on this process. It carries orders
//! placed by hand on the website, a sibling process's orders on another symbol,
//! and orders an earlier run of ours left behind. So a report is classified
//! before anything else is read from it:
//!
//! - **Another symbol** is set aside first. On a shared account that includes a
//!   sibling process's orders, which carry our prefix and may well pay their fee
//!   in an asset this translator has never heard of. Reading further would turn
//!   someone else's ordinary report into our error.
//! - **Not ours**: this symbol, and an id without our prefix.
//! - **Ours**: our prefix and this run's tag. Whether the engine still knows the
//!   order is the engine's question; it already journals a fill for an id it
//!   does not know as `Orphaned`.
//! - **Orphan**: our prefix and anything else, meaning another run's tag or an id
//!   we would never have sent. It cannot become a [`ClientOrderId`], because an
//!   earlier run's order 5 and this run's order 5 are different orders, and
//!   booking one as the other is the worst outcome available here.
//!
//! The line between an orphan and "not ours" is drawn lopsided on purpose. An id
//! with our prefix that does not parse is an orphan, never "not ours": an orphan
//! is put in front of a person, while "not ours" is set aside. Wrong in the first direction
//! costs an investigation. Wrong in the second leaves a live order of ours that
//! nothing is watching.
//!
//! # A cancel names the order in `C`, not `c`
//!
//! The documentation for `C`: *"Original client order ID; This is the ID of the
//! order being canceled"*. On a `CANCELED` report `c` is the id of the cancel
//! request, which the venue generates when we do not supply one. Reading `c`
//! there would file every one of our own cancels as somebody else's, and the
//! order would stay open in the engine's books forever.
//!
//! # One authority per fact
//!
//! `docs/live-run.md` §5: acceptances and fills come from this stream, and
//! rejections come from the REST response, because a rejected request was never
//! an order for the stream to report on. The stream's own `REJECTED` execution
//! type is documented for cancel-replace, which we never send, and `REPLACED`
//! for amending an order, which we never do. A report of either about one of our
//! orders means our model of the venue is wrong, so both are refused rather than
//! mapped onto something nearby.
//!
//! # Strict where the mapping depends on it
//!
//! Loud about every field it reads (invariant 5) and silent about the rest, so a
//! field Binance adds later cannot make a report unreadable. Some facts are
//! stated twice in one report, and where the translation depends on them they
//! are checked against each other: the execution type `x` against the order
//! status `X`, and a `FILLED` status against the quantity still working.
//!
//! One consequence is written down so nobody has to rediscover it. A `TRADE`
//! whose status is anything but `PARTIALLY_FILLED` or `FILLED` is refused. If the
//! venue turns out to end a partly filled market order inside its last trade
//! report, rather than in an `EXPIRED` report after it, this is the place where
//! one report has to become two events.

use core::fmt;

use quant_core::event::Side;
use quant_core::execution::{ClientOrderId, FeeAsset, Fill, VenueOrderId, VenueTradeId};
use quant_core::fixed::{Notional, ParseFixedError, Px, Qty};
use quant_core::instrument::InstrumentDef;
use quant_core::report::{Ended, EndedBy, FillIds, OrderReport};
use quant_core::time::Ts;

/// What every client order id this system sends begins with.
pub const CLIENT_ID_PREFIX: &str = "q-";

/// The longest client order id the venue accepts.
///
/// The documentation gives the rule only as a template (`-1100`, *"legal range
/// is '%s'"*), so 36 comes from the venue's error text as it is widely reported,
/// not from a page this code can cite. The first order criterion D places
/// confirms it. Our ids use letters, digits and `-`, the alphabet of the venue's
/// own broker ids (`x-…`), so the character rule is not in doubt.
pub const MAX_CLIENT_ID_LEN: usize = 36;

/// The longest a [`RunTag`] may be: the longest id we can then produce, with
/// the largest `u64`, is `q-` + 12 + `-` + 20 digits = 35.
pub const MAX_RUN_TAG_LEN: usize = 12;

/// The part of a client order id that names the run which sent it, and the
/// scheme those ids follow: `q-<run>-<id>`.
///
/// **Stable for the life of one journal, and changed when a journal is lost.**
/// Stable, because a supervisor restart resumes the same run: `paper` carries
/// the id counter across it with `Engine::minting_from`, and an order placed
/// before the restart must still read as ours after it. Changed, because a lost
/// journal restarts the counter at 1, and the ids it mints would repeat ones the
/// venue has already seen.
///
/// Repeating an id is worse than it looks. The venue's documentation for
/// `newClientOrderId` says *"A unique id among open orders"*, and *"Orders with
/// the same `newClientOrderID` can be accepted only when the previous one is
/// filled"*. So a repeat of a filled order's id is **accepted**, silently: a
/// second order under the first one's name, after which a lookup by that name
/// is ambiguous. The venue enforces uniqueness only among orders still open;
/// keeping our ids unique for good is this type's job.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunTag(String);

impl RunTag {
    /// A run tag: one to [`MAX_RUN_TAG_LEN`] ASCII letters and digits. No `-`,
    /// because `-` is what the tag is read back out of an id by.
    ///
    /// # Errors
    ///
    /// [`BadRunTag`] for anything else.
    pub fn new(tag: &str) -> Result<Self, BadRunTag> {
        if tag.is_empty() {
            return Err(BadRunTag::Empty);
        }
        if let Some(c) = tag.chars().find(|c| !c.is_ascii_alphanumeric()) {
            return Err(BadRunTag::BadChar(c));
        }
        if tag.len() > MAX_RUN_TAG_LEN {
            return Err(BadRunTag::TooLong { len: tag.len() });
        }
        Ok(Self(tag.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The `newClientOrderId` to send for `id`.
    #[must_use]
    pub fn client_order_id(&self, id: ClientOrderId) -> String {
        format!("{CLIENT_ID_PREFIX}{}-{}", self.0, id.0)
    }

    /// Whose order `client_id` names.
    ///
    /// Recognises exactly what [`Self::client_order_id`] produces and nothing
    /// that merely resembles it: `q-<run>-007` is not what we would send for
    /// order 7, so it is not taken to mean order 7.
    #[must_use]
    pub fn whose(&self, client_id: &str) -> Whose {
        let Some(rest) = client_id.strip_prefix(CLIENT_ID_PREFIX) else {
            return Whose::NotOurs;
        };
        let parsed = rest
            .split_once('-')
            .and_then(|(run, digits)| Some((run, canonical_u64(digits)?)));
        match parsed {
            Some((run, id)) if run == self.0 => Whose::Ours(ClientOrderId(id)),
            Some((run, _)) if Self::new(run).is_ok() => {
                Whose::Orphan(OrphanReason::OtherRun(run.to_owned()))
            }
            _ => Whose::Orphan(OrphanReason::Unparseable),
        }
    }
}

impl fmt::Display for RunTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a string cannot be a [`RunTag`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BadRunTag {
    Empty,
    BadChar(char),
    TooLong { len: usize },
}

impl fmt::Display for BadRunTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("a run tag cannot be empty"),
            Self::BadChar(c) => write!(f, "a run tag is letters and digits only, not {c:?}"),
            Self::TooLong { len } => write!(
                f,
                "a run tag is at most {MAX_RUN_TAG_LEN} characters, and this one is {len}"
            ),
        }
    }
}

impl std::error::Error for BadRunTag {}

/// Whose order a client order id names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Whose {
    /// This run's.
    Ours(ClientOrderId),
    /// Carries our prefix, but this run did not send it.
    Orphan(OrphanReason),
    /// No prefix of ours: placed by something other than this system.
    NotOurs,
}

/// Why an id with our prefix is not one of this run's orders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanReason {
    /// A well-formed id from the run named here.
    OtherRun(String),
    /// Begins with our prefix, and is nothing we would have sent.
    Unparseable,
}

/// What a report says happened to an order, before it is known whose it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The venue has the order.
    Accepted,
    /// Some or all of it traded, in the venue's trade `trade_id`.
    Filled {
        fill: Fill,
        remaining: Qty,
        trade_id: VenueTradeId,
    },
    /// It will not trade again: cancelled, expired, or expired by self-trade
    /// prevention. One outcome to the engine, which needs to know only that the
    /// order is over and how much of it never traded; `how` is which of the
    /// three, for a person reconciling against the venue.
    Ended { remaining: Qty, how: EndedBy },
}

/// A report about an order with our prefix that this run did not send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanReport {
    /// The id exactly as the venue sent it. Text on purpose: making it a
    /// [`ClientOrderId`] is the collision this type exists to prevent.
    pub client_id: String,
    pub why: OrphanReason,
    pub venue_order_id: VenueOrderId,
    pub side: Side,
    pub outcome: Outcome,
}

/// What one message on the user-data stream means to this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Inbound {
    /// About one of this run's orders: the event, with the venue's order id
    /// and, for a fill, its trade id beside it.
    Ours(OrderReport),
    /// About an order with our prefix that this run did not send. Needs a
    /// person.
    Orphan(OrphanReport),
    /// About this symbol, but an order this system did not place. A `TRADE`
    /// here means the account's balances moved under the engine's feet, so
    /// reconciliation will disagree; the caller should say so when it happens
    /// rather than leave it to be found.
    NotOurs {
        client_id: String,
        exec_type: String,
    },
    /// About another symbol, named here.
    OtherSymbol(String),
    /// The subscription ended. Until it is re-established we cannot hear what
    /// happens to our own orders, which is blindness on the execution side.
    StreamTerminated,
    /// An event this translator does not act on, such as a balance update.
    /// Named, so a caller can count what it is skipping.
    Ignored(String),
}

/// Why a message could not be translated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportError {
    /// Not the `{"subscriptionId": .., "event": {..}}` shape the stream sends.
    NotAnEvent(String),
    /// An `executionReport` with a field of the wrong type.
    Malformed(String),
    /// An `executionReport` without a field the translation needs.
    MissingField(&'static str),
    /// A decimal string the venue sent will not parse to fixed-point.
    BadNumber {
        field: &'static str,
        value: String,
        source: ParseFixedError,
    },
    /// A value outside the documented set. Our model of the venue is wrong.
    Unknown { field: &'static str, value: String },
    /// A documented report that this system never causes, about one of its
    /// orders.
    Unsupported(&'static str),
    /// A commission taken in an asset that is neither side of the pair, which
    /// is what paying fees in BNB does. The run procedure switches that off;
    /// this is what notices if it was not.
    UnknownFeeAsset { asset: String, amount: String },
    /// A commission with no asset named.
    FeeWithoutAsset(String),
    /// Two fields that state one fact disagree.
    Inconsistent(String),
}

impl fmt::Display for ReportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAnEvent(e) => write!(f, "not a user-data event: {e}"),
            Self::Malformed(e) => write!(f, "executionReport has a field of the wrong type: {e}"),
            Self::MissingField(field) => write!(f, "executionReport has no usable `{field}` field"),
            Self::BadNumber {
                field,
                value,
                source,
            } => write!(
                f,
                "`{field}` is not a valid fixed-point value: {value:?} ({source})"
            ),
            Self::Unknown { field, value } => {
                write!(f, "`{field}` has an undocumented value {value:?}")
            }
            Self::Unsupported(what) => write!(f, "{what}, and this system never causes one"),
            Self::UnknownFeeAsset { asset, amount } => write!(
                f,
                "commission of {amount} taken in {asset}, which is neither side of the pair; \
                 is paying fees in BNB switched on?"
            ),
            Self::FeeWithoutAsset(amount) => {
                write!(f, "commission of {amount} with no asset named")
            }
            Self::Inconsistent(what) => write!(f, "report contradicts itself: {what}"),
        }
    }
}

impl std::error::Error for ReportError {}

/// Why a [`Translator`] cannot be built for an instrument: it names no base or
/// no quote asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissingAsset(pub &'static str);

impl fmt::Display for MissingAsset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the instrument names no {} asset, so no commission could be recognised",
            self.0
        )
    }
}

impl std::error::Error for MissingAsset {}

/// Translates one symbol's user-data reports for one run.
#[derive(Debug, Clone)]
pub struct Translator {
    run: RunTag,
    symbol: String,
    base: String,
    quote: String,
}

impl Translator {
    /// A translator for `instrument`'s reports, under `run`.
    ///
    /// # Errors
    ///
    /// [`MissingAsset`] when the instrument does not name its base or quote
    /// asset. Commissions are recognised by asset code, so without both every
    /// fill would be refused, and better once here than on the first trade of a
    /// funded run. Not hypothetical: `paper` and `backtest` both register their
    /// instruments with empty asset names, because until now nothing read them.
    pub fn new(run: RunTag, instrument: &InstrumentDef) -> Result<Self, MissingAsset> {
        if instrument.base.is_empty() {
            return Err(MissingAsset("base"));
        }
        if instrument.quote.is_empty() {
            return Err(MissingAsset("quote"));
        }
        Ok(Self {
            run,
            symbol: instrument.symbol.clone(),
            base: instrument.base.clone(),
            quote: instrument.quote.clone(),
        })
    }

    /// Translate one message, received at `recv`.
    ///
    /// Every event is stamped `recv`, never a time the venue wrote: the engine
    /// dispatches on when we learned a thing, not on when the venue says it
    /// happened (invariant 2). The venue's own times stay in the bytes.
    ///
    /// # Errors
    ///
    /// [`ReportError`] for a message this module does not understand. Reports
    /// about other symbols and other people's orders are not errors; they are
    /// classified and returned.
    pub fn translate(&self, message: &[u8], recv: Ts) -> Result<Inbound, ReportError> {
        let mut envelope: serde_json::Value =
            serde_json::from_slice(message).map_err(|e| ReportError::NotAnEvent(e.to_string()))?;
        let event = envelope
            .get_mut("event")
            .map(serde_json::Value::take)
            .ok_or_else(|| ReportError::NotAnEvent("no `event` field".to_owned()))?;
        match event.get("e").and_then(serde_json::Value::as_str) {
            Some("executionReport") => {}
            Some("eventStreamTerminated") => return Ok(Inbound::StreamTerminated),
            Some(other) => return Ok(Inbound::Ignored(other.to_owned())),
            None => {
                return Err(ReportError::NotAnEvent(
                    "an event with no type `e`".to_owned(),
                ))
            }
        }
        // Typed only once the type is known. Other events reuse these letters
        // for other things (`listStatus` has its own `c`, `C`, `l` and `L`),
        // and an event we were going to ignore must not be able to fail as a
        // malformed report.
        let report: RawReport =
            serde_json::from_value(event).map_err(|e| ReportError::Malformed(e.to_string()))?;
        self.execution(&report, recv)
    }

    fn execution(&self, r: &RawReport, recv: Ts) -> Result<Inbound, ReportError> {
        let symbol = r.symbol.as_deref().ok_or(ReportError::MissingField("s"))?;
        if symbol != self.symbol {
            return Ok(Inbound::OtherSymbol(symbol.to_owned()));
        }
        let exec_type = r
            .exec_type
            .as_deref()
            .ok_or(ReportError::MissingField("x"))?;
        let client_id = if exec_type == "CANCELED" {
            r.original_client_id
                .as_deref()
                .filter(|id| !id.is_empty())
                .ok_or(ReportError::MissingField("C"))?
        } else {
            r.client_id
                .as_deref()
                .ok_or(ReportError::MissingField("c"))?
        };
        match self.run.whose(client_id) {
            Whose::NotOurs => Ok(Inbound::NotOurs {
                client_id: client_id.to_owned(),
                exec_type: exec_type.to_owned(),
            }),
            Whose::Ours(client_order_id) => {
                let (venue_order_id, outcome) = self.detail(r, exec_type)?;
                Ok(Inbound::Ours(match outcome {
                    Outcome::Accepted => OrderReport::Accepted {
                        client_order_id,
                        venue_order_id: Some(venue_order_id),
                        ts: recv,
                    },
                    Outcome::Filled {
                        fill,
                        remaining,
                        trade_id,
                    } => OrderReport::Filled {
                        client_order_id,
                        fill,
                        remaining,
                        ids: Some(FillIds {
                            venue_order_id,
                            trade_id,
                        }),
                        ts: recv,
                    },
                    Outcome::Ended { remaining, how } => OrderReport::Cancelled {
                        client_order_id,
                        remaining,
                        ended: Some(Ended {
                            venue_order_id,
                            how,
                        }),
                        ts: recv,
                    },
                }))
            }
            Whose::Orphan(why) => {
                let (venue_order_id, outcome) = self.detail(r, exec_type)?;
                Ok(Inbound::Orphan(OrphanReport {
                    client_id: client_id.to_owned(),
                    why,
                    venue_order_id,
                    side: side(r)?,
                    outcome,
                }))
            }
        }
    }

    /// What happened to an order of ours, this run's or another's.
    fn detail(
        &self,
        r: &RawReport,
        exec_type: &str,
    ) -> Result<(VenueOrderId, Outcome), ReportError> {
        let venue_order_id = VenueOrderId(
            r.order_id
                .ok_or(ReportError::MissingField("i"))?
                .to_string(),
        );
        let status = r.status.as_deref().ok_or(ReportError::MissingField("X"))?;
        let ordered: Qty = fixed(r.quantity.as_deref(), "q")?;
        let filled: Qty = fixed(r.cumulative.as_deref(), "z")?;
        if filled.is_negative() || filled > ordered {
            return Err(ReportError::Inconsistent(format!(
                "{filled} filled of an order for {ordered}"
            )));
        }
        let remaining = ordered - filled;
        let status_is = |expected: &str| {
            if status == expected {
                Ok(())
            } else {
                Err(ReportError::Inconsistent(format!(
                    "execution type {exec_type} with order status {status}"
                )))
            }
        };
        let outcome = match exec_type {
            "NEW" => {
                status_is("NEW")?;
                Outcome::Accepted
            }
            "TRADE" => self.trade(r, status, filled, remaining)?,
            "CANCELED" => {
                status_is("CANCELED")?;
                Outcome::Ended {
                    remaining,
                    how: EndedBy::Cancelled,
                }
            }
            // How a market order ends when the book runs out before it does.
            "EXPIRED" => {
                status_is("EXPIRED")?;
                Outcome::Ended {
                    remaining,
                    how: EndedBy::Expired,
                }
            }
            "TRADE_PREVENTION" => {
                status_is("EXPIRED_IN_MATCH")?;
                Outcome::Ended {
                    remaining,
                    how: EndedBy::SelfTradePrevention,
                }
            }
            "REJECTED" => {
                return Err(ReportError::Unsupported(
                    "a `REJECTED` report, which the stream sends for cancel-replace",
                ))
            }
            "REPLACED" => {
                return Err(ReportError::Unsupported(
                    "a `REPLACED` report, which the stream sends for an amended order",
                ))
            }
            other => {
                return Err(ReportError::Unknown {
                    field: "x",
                    value: other.to_owned(),
                })
            }
        };
        Ok((venue_order_id, outcome))
    }

    /// A `TRADE` report as a fill: the last execution, `l` at `L`, which is
    /// neither the order's price `p` nor the running total `z`.
    fn trade(
        &self,
        r: &RawReport,
        status: &str,
        filled: Qty,
        remaining: Qty,
    ) -> Result<Outcome, ReportError> {
        let qty: Qty = fixed(r.last_qty.as_deref(), "l")?;
        let px: Px = fixed(r.last_px.as_deref(), "L")?;
        if !qty.is_positive() || !px.is_positive() {
            return Err(ReportError::Inconsistent(format!(
                "a trade of {qty} at {px}"
            )));
        }
        if qty > filled {
            return Err(ReportError::Inconsistent(format!(
                "a trade of {qty} when {filled} has filled in total"
            )));
        }
        // The status is the venue's word on whether the order is done, and the
        // engine reads that from `remaining`, so the two must agree.
        let done = match status {
            "FILLED" => true,
            "PARTIALLY_FILLED" => false,
            other => {
                return Err(ReportError::Inconsistent(format!(
                    "execution type TRADE with order status {other}"
                )))
            }
        };
        if done != remaining.is_zero() {
            return Err(ReportError::Inconsistent(format!(
                "order status {status} with {remaining} still to fill"
            )));
        }
        let (fee, fee_asset) = self.commission(r)?;
        let is_maker = r.is_maker.ok_or(ReportError::MissingField("m"))?;
        // `t`, the trade, and not `I`, the execution: `I` numbers every report
        // about the order, and `myTrades` names a row by `t`. The documentation
        // gives `-1` for a report that is not a trade, so on a trade a missing
        // or negative `t` is a report that cannot be matched to the venue's
        // own record of the fill, and criterion A could not be judged on it.
        let trade_id = match r.trade_id {
            Some(t) if t >= 0 => VenueTradeId(t.to_string()),
            _ => return Err(ReportError::MissingField("t")),
        };
        Ok(Outcome::Filled {
            fill: Fill {
                px,
                qty,
                fee,
                is_maker,
                fee_asset,
            },
            remaining,
            trade_id,
        })
    }

    /// The commission on a trade, and which side of the pair paid it.
    ///
    /// When it is the base asset, the amount is a quantity of the base, which
    /// is what [`Fill::fee`] means when its [`FeeAsset`] is `Base`.
    fn commission(&self, r: &RawReport) -> Result<(Notional, FeeAsset), ReportError> {
        let text = r
            .commission
            .as_deref()
            .ok_or(ReportError::MissingField("n"))?;
        let amount: Notional = fixed(Some(text), "n")?;
        if amount.is_negative() {
            // A sign error on a fee looks like a good strategy rather than a
            // bug (M4), so a rebate is refused until something models one.
            return Err(ReportError::Unknown {
                field: "n",
                value: text.to_owned(),
            });
        }
        if amount.is_zero() {
            // Nothing was taken, so there is no asset to be right or wrong
            // about. `N` is documented as `null` when there is no commission.
            return Ok((amount, FeeAsset::Quote));
        }
        match r.commission_asset.as_deref() {
            None | Some("") => Err(ReportError::FeeWithoutAsset(text.to_owned())),
            Some(asset) if asset == self.base => Ok((amount, FeeAsset::Base)),
            Some(asset) if asset == self.quote => Ok((amount, FeeAsset::Quote)),
            Some(asset) => Err(ReportError::UnknownFeeAsset {
                asset: asset.to_owned(),
                amount: text.to_owned(),
            }),
        }
    }
}

/// The side of an order, read only for orphans: for this run's orders the
/// engine already knows it.
fn side(r: &RawReport) -> Result<Side, ReportError> {
    match r.side.as_deref().ok_or(ReportError::MissingField("S"))? {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        other => Err(ReportError::Unknown {
            field: "S",
            value: other.to_owned(),
        }),
    }
}

/// `digits` as a `u64`, only if it is exactly how we would have written one:
/// ASCII digits, no sign, and no leading zero except `0` itself. `str::parse`
/// alone would accept `+7` and `007`.
fn canonical_u64(digits: &str) -> Option<u64> {
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if digits.len() > 1 && digits.starts_with('0') {
        return None;
    }
    digits.parse().ok()
}

/// A required fixed-point field.
fn fixed<T>(value: Option<&str>, field: &'static str) -> Result<T, ReportError>
where
    T: core::str::FromStr<Err = ParseFixedError>,
{
    let text = value.ok_or(ReportError::MissingField(field))?;
    text.parse().map_err(|source| ReportError::BadNumber {
        field,
        value: text.to_owned(),
        source,
    })
}

/// The fields of an `executionReport` this module reads, by the venue's
/// one-letter names. Everything else in the payload is ignored.
///
/// Owned strings, not borrowed ones as in `parse.rs`: JSON may escape any
/// character, and the documentation warns that these events can carry non-ASCII
/// names, so a borrowed field would fail a whole report over an escape.
#[derive(serde::Deserialize)]
struct RawReport {
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "c")]
    client_id: Option<String>,
    #[serde(rename = "C")]
    original_client_id: Option<String>,
    #[serde(rename = "S")]
    side: Option<String>,
    #[serde(rename = "x")]
    exec_type: Option<String>,
    #[serde(rename = "X")]
    status: Option<String>,
    #[serde(rename = "i")]
    order_id: Option<u64>,
    #[serde(rename = "q")]
    quantity: Option<String>,
    #[serde(rename = "z")]
    cumulative: Option<String>,
    #[serde(rename = "l")]
    last_qty: Option<String>,
    #[serde(rename = "L")]
    last_px: Option<String>,
    #[serde(rename = "n")]
    commission: Option<String>,
    #[serde(rename = "N")]
    commission_asset: Option<String>,
    #[serde(rename = "m")]
    is_maker: Option<bool>,
    #[serde(rename = "t")]
    trade_id: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::instrument::{Exchange, InstrumentKind};
    use serde_json::{json, Value};

    /// Binance's documented `executionReport` (`user-data-stream.md` at commit
    /// `0556f1a`), with its JavaScript comments removed and nothing else
    /// changed. Every other report here is this one with named fields
    /// replaced, so each test's claim is visible as the fields it changes.
    const DOCUMENTED: &str = r#"{
        "subscriptionId": 0,
        "event": {
            "e": "executionReport",
            "E": 1499405658658,
            "s": "ETHBTC",
            "c": "mUvoqJxFIILMdfAW5iGSOW",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "1.00000000",
            "p": "0.10264410",
            "P": "0.00000000",
            "F": "0.00000000",
            "g": -1,
            "C": "",
            "x": "NEW",
            "X": "NEW",
            "r": "NONE",
            "i": 4293153,
            "l": "0.00000000",
            "z": "0.00000000",
            "L": "0.00000000",
            "n": "0",
            "N": null,
            "T": 1499405658657,
            "t": -1,
            "v": 3,
            "I": 8641984,
            "w": true,
            "m": false,
            "M": false,
            "O": 1499405658657,
            "Z": "0.00000000",
            "Y": "0.00000000",
            "Q": "0.00000000",
            "W": 1499405658657,
            "V": "NONE"
        }
    }"#;

    /// When every message in these tests was received: 2026, where the
    /// fixture's `E` and `T` are 2017, so a translation that used either would
    /// show.
    const RECV: Ts = Ts::from_nanos(1_787_315_652_514_000_001);

    fn px(s: &str) -> Px {
        s.parse().expect("price")
    }

    fn qty(s: &str) -> Qty {
        s.parse().expect("quantity")
    }

    fn notional(s: &str) -> Notional {
        s.parse().expect("notional")
    }

    fn run() -> RunTag {
        RunTag::new("r1").expect("a valid tag")
    }

    /// The documented example's own pair, so the example needs no edit to be
    /// about our symbol.
    fn ethbtc() -> InstrumentDef {
        InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "ETHBTC".to_owned(),
            base: "ETH".to_owned(),
            quote: "BTC".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.00001".parse().expect("tick"),
            lot_size: "0.0001".parse().expect("lot"),
            min_notional: "0.0001".parse().expect("notional"),
        }
    }

    fn translator() -> Translator {
        Translator::new(run(), &ethbtc()).expect("both assets named")
    }

    /// The documented example with `fields` replaced, later entries winning.
    /// Constructed, not captured: see the module docs.
    fn report(fields: &[(&str, Value)]) -> Vec<u8> {
        let mut message: Value = serde_json::from_str(DOCUMENTED).expect("the example is JSON");
        for (field, value) in fields {
            message["event"][*field] = value.clone();
        }
        serde_json::to_vec(&message).expect("serializable")
    }

    fn translate(fields: &[(&str, Value)]) -> Result<Inbound, ReportError> {
        translator().translate(&report(fields), RECV)
    }

    fn with(
        mut fields: Vec<(&'static str, Value)>,
        changes: &[(&'static str, Value)],
    ) -> Vec<(&'static str, Value)> {
        fields.extend_from_slice(changes);
        fields
    }

    /// Our order 7 for 1 ETH at a limit of 0.10264410, taking 0.2 at 0.1026
    /// after an earlier 0.4. Every quantity and price differs from every other,
    /// and `m` is the non-default `true`, so reading the wrong field shows.
    fn partial_fill() -> Vec<(&'static str, Value)> {
        vec![
            ("c", json!("q-r1-7")),
            ("x", json!("TRADE")),
            ("X", json!("PARTIALLY_FILLED")),
            ("l", json!("0.20000000")),
            ("z", json!("0.60000000")),
            ("L", json!("0.10260000")),
            ("n", json!("0.00020000")),
            ("N", json!("ETH")),
            ("t", json!(12345)),
            ("m", json!(true)),
        ]
    }

    fn the_partial_fill() -> Fill {
        Fill {
            px: px("0.1026"),
            qty: qty("0.2"),
            fee: notional("0.0002"),
            is_maker: true,
            fee_asset: FeeAsset::Base,
        }
    }

    fn filled(inbound: Result<Inbound, ReportError>) -> (Fill, Qty) {
        match inbound {
            Ok(Inbound::Ours(OrderReport::Filled {
                fill, remaining, ..
            })) => (fill, remaining),
            other => panic!("expected a fill of ours, got {other:?}"),
        }
    }

    /// The documented example's order id.
    fn documented_order() -> VenueOrderId {
        VenueOrderId("4293153".to_owned())
    }

    /// What the venue calls the fill in [`partial_fill`].
    fn the_partial_fills_ids() -> FillIds {
        FillIds {
            venue_order_id: documented_order(),
            trade_id: VenueTradeId("12345".to_owned()),
        }
    }

    #[test]
    fn the_documented_example_is_somebody_elses_order() {
        assert_eq!(
            translator().translate(DOCUMENTED.as_bytes(), RECV),
            Ok(Inbound::NotOurs {
                client_id: "mUvoqJxFIILMdfAW5iGSOW".to_owned(),
                exec_type: "NEW".to_owned(),
            })
        );
    }

    #[test]
    fn our_new_order_is_accepted_with_the_venues_id_for_it() {
        assert_eq!(
            translate(&[("c", json!("q-r1-7"))]),
            Ok(Inbound::Ours(OrderReport::Accepted {
                client_order_id: ClientOrderId(7),
                venue_order_id: Some(documented_order()),
                ts: RECV,
            }))
        );
    }

    #[test]
    fn a_trade_is_the_last_execution_not_the_order_price_or_the_running_total() {
        assert_eq!(
            translate(&partial_fill()),
            Ok(Inbound::Ours(OrderReport::Filled {
                client_order_id: ClientOrderId(7),
                fill: the_partial_fill(),
                remaining: qty("0.4"),
                ids: Some(the_partial_fills_ids()),
                ts: RECV,
            }))
        );
    }

    #[test]
    fn a_trade_is_named_by_its_trade_id_not_by_its_execution_id() {
        // `t` and `I` are both integers on the same report, and only `t` finds
        // the row in `myTrades` that criterion A compares this fill with. The
        // fixture's are distinct (12345 and the documented 8641984), so reading
        // the wrong one shows.
        let (ids, _) = fill_ids(translate(&partial_fill()));
        assert_eq!(ids, the_partial_fills_ids());
        let (ids, _) = fill_ids(translate(&with(partial_fill(), &[("t", json!(0))])));
        assert_eq!(
            ids.trade_id,
            VenueTradeId("0".to_owned()),
            "zero is a trade id"
        );
    }

    #[test]
    fn a_trade_without_a_trade_id_is_refused_rather_than_defaulted() {
        // A fill that cannot be matched to the venue's own record of it would
        // leave criterion A unjudgeable on exactly the row that matters.
        let mut absent = partial_fill();
        absent.retain(|(field, _)| *field != "t");
        absent.push(("t", Value::Null));
        for fields in [absent, with(partial_fill(), &[("t", json!(-1))])] {
            assert_eq!(
                translate(&fields),
                Err(ReportError::MissingField("t")),
                "{fields:?}"
            );
        }
    }

    fn fill_ids(inbound: Result<Inbound, ReportError>) -> (FillIds, Qty) {
        match inbound {
            Ok(Inbound::Ours(OrderReport::Filled {
                ids: Some(ids),
                remaining,
                ..
            })) => (ids, remaining),
            other => panic!("expected a fill of ours with its ids, got {other:?}"),
        }
    }

    #[test]
    fn the_last_trade_of_an_order_leaves_nothing_remaining() {
        let fields = with(
            partial_fill(),
            &[("X", json!("FILLED")), ("z", json!("1.00000000"))],
        );
        let (_, remaining) = filled(translate(&fields));
        assert!(remaining.is_zero());
    }

    #[test]
    fn a_cancel_is_matched_on_the_order_in_capital_c_not_the_request_in_c() {
        let cancel = [
            ("x", json!("CANCELED")),
            ("X", json!("CANCELED")),
            // The cancel request's own id, generated by the venue.
            ("c", json!("6gCrw2kRUAF9CvJDGP16IP")),
            ("C", json!("q-r1-7")),
            ("z", json!("0.25000000")),
        ];
        assert_eq!(
            translate(&cancel),
            Ok(Inbound::Ours(OrderReport::Cancelled {
                client_order_id: ClientOrderId(7),
                remaining: qty("0.75"),
                ended: Some(Ended {
                    venue_order_id: documented_order(),
                    how: EndedBy::Cancelled,
                }),
                ts: RECV,
            }))
        );
    }

    #[test]
    fn a_cancel_that_does_not_name_its_order_is_refused_not_read_from_c() {
        // `c` is ours here, so falling back to it would quietly "work".
        let cancel = [
            ("x", json!("CANCELED")),
            ("X", json!("CANCELED")),
            ("c", json!("q-r1-7")),
            ("C", json!("")),
        ];
        assert_eq!(translate(&cancel), Err(ReportError::MissingField("C")));
    }

    #[test]
    fn expiry_and_self_trade_prevention_end_the_order_with_what_never_traded() {
        for (exec_type, status, how) in [
            ("EXPIRED", "EXPIRED", EndedBy::Expired),
            (
                "TRADE_PREVENTION",
                "EXPIRED_IN_MATCH",
                EndedBy::SelfTradePrevention,
            ),
        ] {
            let fields = [
                ("c", json!("q-r1-7")),
                ("x", json!(exec_type)),
                ("X", json!(status)),
                ("z", json!("0.25000000")),
            ];
            assert_eq!(
                translate(&fields),
                Ok(Inbound::Ours(OrderReport::Cancelled {
                    client_order_id: ClientOrderId(7),
                    remaining: qty("0.75"),
                    ended: Some(Ended {
                        venue_order_id: documented_order(),
                        how,
                    }),
                    ts: RECV,
                })),
                "{exec_type}"
            );
        }
    }

    #[test]
    fn every_event_is_stamped_when_we_received_it_not_when_the_venue_says() {
        let cancel = vec![
            ("x", json!("CANCELED")),
            ("X", json!("CANCELED")),
            ("C", json!("q-r1-7")),
        ];
        for fields in [vec![("c", json!("q-r1-7"))], partial_fill(), cancel] {
            match translate(&fields) {
                Ok(Inbound::Ours(report)) => assert_eq!(report.ts(), RECV, "{report:?}"),
                other => panic!("expected an event of ours, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_fee_is_booked_in_whichever_side_of_the_pair_the_venue_took_it_from() {
        for (asset, expected) in [("ETH", FeeAsset::Base), ("BTC", FeeAsset::Quote)] {
            let (fill, _) = filled(translate(&with(partial_fill(), &[("N", json!(asset))])));
            assert_eq!(fill.fee_asset, expected, "{asset}");
            assert_eq!(fill.fee, notional("0.0002"), "{asset}");
        }
    }

    #[test]
    fn a_fee_in_neither_asset_is_refused_rather_than_guessed() {
        // Paying fees in BNB does this. Booking it as either side would put a
        // wrong number into cash or into the position.
        assert_eq!(
            translate(&with(partial_fill(), &[("N", json!("BNB"))])),
            Err(ReportError::UnknownFeeAsset {
                asset: "BNB".to_owned(),
                amount: "0.00020000".to_owned(),
            })
        );
    }

    #[test]
    fn a_zero_fee_needs_no_asset_and_a_real_one_does() {
        let free = with(partial_fill(), &[("n", json!("0")), ("N", Value::Null)]);
        let (fill, _) = filled(translate(&free));
        assert!(fill.fee.is_zero());

        let unnamed = with(partial_fill(), &[("N", Value::Null)]);
        assert_eq!(
            translate(&unnamed),
            Err(ReportError::FeeWithoutAsset("0.00020000".to_owned()))
        );
    }

    #[test]
    fn a_negative_fee_is_refused() {
        assert_eq!(
            translate(&with(partial_fill(), &[("n", json!("-0.00020000"))])),
            Err(ReportError::Unknown {
                field: "n",
                value: "-0.00020000".to_owned(),
            })
        );
    }

    #[test]
    fn another_symbol_is_set_aside_before_anything_else_is_read() {
        // A sibling process on the same account: another run of ours, trading
        // BNBBTC, paying its fee in BNB. Read any further, its id makes it an
        // orphan and its fee makes that orphan an error.
        let sibling = with(
            partial_fill(),
            &[
                ("s", json!("BNBBTC")),
                ("c", json!("q-r2-7")),
                ("N", json!("BNB")),
            ],
        );
        assert_eq!(
            translate(&sibling),
            Ok(Inbound::OtherSymbol("BNBBTC".to_owned()))
        );
    }

    #[test]
    fn somebody_elses_trade_on_our_symbol_says_that_it_traded() {
        // Their fee asset is their business, so it is not read.
        let theirs = with(
            partial_fill(),
            &[("c", json!("web_4a1b2c3d")), ("N", json!("BNB"))],
        );
        assert_eq!(
            translate(&theirs),
            Ok(Inbound::NotOurs {
                client_id: "web_4a1b2c3d".to_owned(),
                exec_type: "TRADE".to_owned(),
            })
        );
    }

    #[test]
    fn our_prefix_from_another_run_is_an_orphan_carrying_what_a_person_needs() {
        let fields = with(
            partial_fill(),
            &[("c", json!("q-r0-7")), ("S", json!("SELL"))],
        );
        assert_eq!(
            translate(&fields),
            Ok(Inbound::Orphan(OrphanReport {
                client_id: "q-r0-7".to_owned(),
                why: OrphanReason::OtherRun("r0".to_owned()),
                venue_order_id: VenueOrderId("4293153".to_owned()),
                side: Side::Sell,
                outcome: Outcome::Filled {
                    fill: the_partial_fill(),
                    remaining: qty("0.4"),
                    trade_id: VenueTradeId("12345".to_owned()),
                },
            }))
        );
    }

    #[test]
    fn an_id_with_our_prefix_that_we_would_never_send_is_an_orphan_not_somebody_elses() {
        let run = run();
        for id in [
            "q-r1-007",
            "q-r1-+7",
            "q-r1-",
            "q-r1",
            "q-r1-7-8",
            "q-r1-18446744073709551616",
            "q-",
            "q--7",
            "q-r_1-7",
        ] {
            assert_eq!(
                run.whose(id),
                Whose::Orphan(OrphanReason::Unparseable),
                "{id}"
            );
        }
        for id in ["Q-r1-7", "r1-7", "x-r1-7", "mUvoqJxFIILMdfAW5iGSOW", ""] {
            assert_eq!(run.whose(id), Whose::NotOurs, "{id}");
        }
    }

    #[test]
    fn every_id_we_send_reads_back_as_ours_and_fits_the_venue() {
        let longest = RunTag::new("abcdefghijkl").expect("twelve characters");
        for tag in [run(), longest.clone()] {
            for id in [0, 1, 7, 10, u64::MAX] {
                let sent = tag.client_order_id(ClientOrderId(id));
                assert_eq!(tag.whose(&sent), Whose::Ours(ClientOrderId(id)), "{sent}");
                assert!(
                    sent.len() <= MAX_CLIENT_ID_LEN,
                    "{sent} is {} long",
                    sent.len()
                );
            }
        }
        // A tag that merely begins with ours is another run.
        assert_eq!(
            run().whose("q-r10-7"),
            Whose::Orphan(OrphanReason::OtherRun("r10".to_owned()))
        );
        assert_eq!(
            run().whose(&longest.client_order_id(ClientOrderId(7))),
            Whose::Orphan(OrphanReason::OtherRun("abcdefghijkl".to_owned()))
        );
    }

    #[test]
    fn a_run_tag_is_one_to_twelve_letters_and_digits() {
        assert!(RunTag::new("abcdefghijkl").is_ok());
        assert!(RunTag::new("M8run2").is_ok());
        assert_eq!(RunTag::new(""), Err(BadRunTag::Empty));
        assert_eq!(
            RunTag::new("abcdefghijklm"),
            Err(BadRunTag::TooLong { len: 13 })
        );
        assert_eq!(RunTag::new("a-b"), Err(BadRunTag::BadChar('-')));
        assert_eq!(RunTag::new("a_b"), Err(BadRunTag::BadChar('_')));
        assert_eq!(RunTag::new("éééééééé"), Err(BadRunTag::BadChar('é')));
    }

    #[test]
    fn a_trade_whose_status_contradicts_its_quantities_is_refused() {
        for (status, so_far) in [
            ("FILLED", "0.6"),
            ("PARTIALLY_FILLED", "1"),
            ("CANCELED", "0.6"),
            ("NEW", "0.6"),
        ] {
            let fields = with(
                partial_fill(),
                &[("X", json!(status)), ("z", json!(so_far))],
            );
            assert!(
                matches!(translate(&fields), Err(ReportError::Inconsistent(_))),
                "{status} with {so_far} filled"
            );
        }
    }

    #[test]
    fn an_ending_whose_status_says_otherwise_is_refused() {
        for (exec_type, status) in [
            ("NEW", "PARTIALLY_FILLED"),
            ("CANCELED", "NEW"),
            ("EXPIRED", "CANCELED"),
            ("TRADE_PREVENTION", "EXPIRED"),
        ] {
            let fields = [
                ("c", json!("q-r1-7")),
                ("C", json!("q-r1-7")),
                ("x", json!(exec_type)),
                ("X", json!(status)),
            ];
            assert!(
                matches!(translate(&fields), Err(ReportError::Inconsistent(_))),
                "{exec_type} with {status}"
            );
        }
    }

    #[test]
    fn quantities_that_cannot_be_true_are_refused() {
        // More filled than ordered, a trade larger than everything filled, a
        // trade of nothing, a trade at no price.
        for change in [
            ("z", json!("1.5")),
            ("l", json!("0.7")),
            ("l", json!("0")),
            ("L", json!("0")),
        ] {
            let fields = with(partial_fill(), std::slice::from_ref(&change));
            assert!(
                matches!(translate(&fields), Err(ReportError::Inconsistent(_))),
                "{change:?}"
            );
        }
    }

    #[test]
    fn reports_we_never_cause_are_refused_rather_than_mapped_onto_something_nearby() {
        for exec_type in ["REJECTED", "REPLACED"] {
            let fields = [
                ("c", json!("q-r1-7")),
                ("x", json!(exec_type)),
                ("X", json!("REJECTED")),
            ];
            assert!(
                matches!(translate(&fields), Err(ReportError::Unsupported(_))),
                "{exec_type}"
            );
        }
        assert_eq!(
            translate(&[("c", json!("q-r1-7")), ("x", json!("AMENDED"))]),
            Err(ReportError::Unknown {
                field: "x",
                value: "AMENDED".to_owned(),
            })
        );
        assert_eq!(
            translate(&[("c", json!("q-r0-7")), ("S", json!("HOLD"))]),
            Err(ReportError::Unknown {
                field: "S",
                value: "HOLD".to_owned(),
            })
        );
    }

    #[test]
    fn a_number_that_will_not_parse_is_loud_and_names_its_field() {
        let fields = with(partial_fill(), &[("L", json!("0.1026.1"))]);
        assert!(matches!(
            translate(&fields),
            Err(ReportError::BadNumber { field: "L", .. })
        ));
        // A ninth decimal place is precision fixed-point would lose, not round.
        let fields = with(partial_fill(), &[("n", json!("0.000000001"))]);
        assert!(matches!(
            translate(&fields),
            Err(ReportError::BadNumber { field: "n", .. })
        ));
    }

    #[test]
    fn an_escaped_id_reads_as_what_it_spells() {
        // `7` is "7". A borrowed `&str` field would fail the whole report.
        let message = String::from_utf8(report(&[("c", json!("q-r1-7"))]))
            .expect("utf-8")
            .replace("q-r1-7", "q-r1-\\u0037");
        assert_eq!(
            translator().translate(message.as_bytes(), RECV),
            Ok(Inbound::Ours(OrderReport::Accepted {
                client_order_id: ClientOrderId(7),
                venue_order_id: Some(documented_order()),
                ts: RECV,
            }))
        );
    }

    #[test]
    fn other_events_are_named_and_termination_is_its_own_outcome() {
        let t = translator();
        let balance = br#"{"subscriptionId":0,"event":{"e":"outboundAccountPosition","E":1564034571105,"u":1564034571073,"B":[{"a":"ETH","f":"10000.000000","l":"0.000000"}]}}"#;
        assert_eq!(
            t.translate(balance, RECV),
            Ok(Inbound::Ignored("outboundAccountPosition".to_owned()))
        );
        // Reuses `c`, `C`, `l` and `L` for other things; must not be read as a
        // report. The documented example, on our symbol.
        let list = br#"{"subscriptionId":0,"event":{"e":"listStatus","E":1564035303637,"s":"ETHBTC","g":2,"c":"OCO","l":"EXEC_STARTED","L":"EXECUTING","r":"NONE","C":"F4QN4G8DlFATFlIUQ0cjdD","T":1564035303625,"O":[{"s":"ETHBTC","i":17,"c":"AJYsMjErWJesZvqlJCTUgL"},{"s":"ETHBTC","i":18,"c":"bfYPSQdLoqAJeNrOr9adzq"}]}}"#;
        assert_eq!(
            t.translate(list, RECV),
            Ok(Inbound::Ignored("listStatus".to_owned()))
        );
        let ended =
            br#"{"subscriptionId":0,"event":{"e":"eventStreamTerminated","E":1728973001334}}"#;
        assert_eq!(t.translate(ended, RECV), Ok(Inbound::StreamTerminated));
        // Constructed, not documented: an event type from the future whose
        // letters mean something else. It is skipped, not malformed.
        let future = br#"{"subscriptionId":0,"event":{"e":"somethingNew","E":1,"i":"x","m":"y"}}"#;
        assert_eq!(
            t.translate(future, RECV),
            Ok(Inbound::Ignored("somethingNew".to_owned()))
        );
    }

    #[test]
    fn what_is_not_an_event_is_refused() {
        let t = translator();
        let messages: [&[u8]; 4] = [
            // The listen-key stream's shape: a bare event, no envelope.
            br#"{"e":"executionReport","s":"ETHBTC"}"#,
            // A response to a request, which belongs to whoever sent it.
            br#"{"id":"1","status":200,"result":{}}"#,
            br#"{"subscriptionId":0,"event":{"E":1}}"#,
            b"not json",
        ];
        for message in messages {
            assert!(
                matches!(t.translate(message, RECV), Err(ReportError::NotAnEvent(_))),
                "{}",
                String::from_utf8_lossy(message)
            );
        }
        // A report, but with a field of the wrong type.
        assert!(matches!(
            translate(&[("i", json!("4293153"))]),
            Err(ReportError::Malformed(_))
        ));
    }

    #[test]
    fn a_translator_refuses_an_instrument_that_does_not_name_its_assets() {
        // How `paper` and `backtest` register theirs today.
        let mut no_base = ethbtc();
        no_base.base = String::new();
        assert_eq!(
            Translator::new(run(), &no_base).err(),
            Some(MissingAsset("base"))
        );

        let mut no_quote = ethbtc();
        no_quote.quote = String::new();
        assert_eq!(
            Translator::new(run(), &no_quote).err(),
            Some(MissingAsset("quote"))
        );
    }
}
