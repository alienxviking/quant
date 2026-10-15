//! One symbol's trading rules, read from the venue into an [`InstrumentDef`].
//!
//! Pure parsing plus one fetch: [`parse`] reads an `exchangeInfo` answer, and
//! [`fetch`] asks the venue for one through the public client, which needs no
//! credentials.
//!
//! # Why the rules come from the venue
//!
//! Every binary so far registers its instrument from literals with the asset
//! names left empty: `paper`, `backtest` and `explain` with tick `0.01`, lot
//! `0.00001` and minimum notional `5`. Criterion B of `docs/live-run.md` §3
//! expects a live run's first failure to be one of those literals disagreeing
//! with the venue, and the user-data translator refuses an instrument without
//! both asset names ([`crate::user_data::Translator::new`]). So a live run asks
//! the venue, and this is what reads the answer.
//!
//! # A failed fetch stops trading, never recording
//!
//! `docs/live-run.md` §5: refuse to submit without venue-sourced filters, and
//! record regardless, because one process both captures and trades and a
//! capture must not stop over a filter table. So nothing here panics and
//! nothing falls back to a default definition. Every failure is a
//! [`FetchError`] the caller can act on, and a default would be the worst
//! answer available: a plausible set of rules the venue never stated.
//!
//! # Every venue rule lands in exactly one place
//!
//! Citations are to Binance's documentation, `binance/binance-spot-api-docs`
//! at commit `0556f1a`.
//!
//! The platform's [`Filters`] hold three rules, and the venue publishes many
//! more (`filters.md`). Its answer is split so that no rule is invented and none
//! is lost:
//!
//! | definition | taken from | `filters.md` |
//! |---|---|---|
//! | `tick_size` | `PRICE_FILTER.tickSize` | lines 37-58 |
//! | `lot_size` | `LOT_SIZE.stepSize` | lines 111-132 |
//! | `min_notional` | the larger `minNotional` of `NOTIONAL` and `MIN_NOTIONAL` | lines 134-181 |
//!
//! Everything else is an [`UnmodelledRule`]: every field of every filter the
//! definition did not take, under the venue's own name for the filter, with its
//! value exactly as the venue wrote it. The name matters: a rejection under a
//! rule reads `Filter failure: MARKET_LOT_SIZE` (`errors.md` lines 425-446), so
//! the report is what lets criterion B say *which* rule the model lacked rather
//! than only that it lacked one. The exchange-wide filters (`filters.md` lines
//! 330-377) are reported too: none of them fits a per-instrument model. So are
//! the answer's rate limits, which are not filters at all (below).
//!
//! # An absent or disabled rule is zero
//!
//! `InstrumentDef` has no "absent", and the readers that apply its rules need
//! none: zero in any of the three already means no rule to
//! [`Filters::accepts`] and [`Filters::rejects`], [`Instrument::round_price`]
//! and [`Qty::round_to_lot`] (see [`Filters`]).
//!
//! Not to every reader. `MaCrossover` refuses a zero lot, because it sizes an
//! exit in whole lots and a zero lot makes every size whole
//! (`quant_backtest::MaConfigError::Lot`). So a symbol the venue lists without
//! `LOT_SIZE` cannot be traded by it. The definition here is still exact; the
//! strategy refuses it when it is built, at startup, loudly and before any
//! order is sent.
//!
//! The venue's own cases land on zero without translation:
//!
//! - **An absent filter.** *"All filters are optional"* (`rest-api.md` lines
//!   858-859), so a symbol without `PRICE_FILTER` has no tick rule, and the
//!   definition says zero. Never a placeholder: a tick the venue did not publish
//!   is an invented rule, the one thing this module must not produce.
//! - **A `tickSize` of zero** disables the tick rule (`filters.md` lines 42 and
//!   44): zero, the same thing. `minPrice` and `maxPrice` disable the same way
//!   (lines 40-41), and are reported as the venue wrote them.
//! - **A `minNotional` of zero**: `price * quantity >= minNotional` (lines 145
//!   and 169) then holds for every order, so zero is exact.
//! - **A `LOT_SIZE.stepSize` of zero is refused.** The documentation gives
//!   `LOT_SIZE` no disabled value (lines 111-122) and `quantity % 0` has no
//!   meaning, so what the venue would do cannot be read off the page. Refusing
//!   costs a run its trading until somebody looks; guessing would cost criterion
//!   B the question it exists to answer.
//!
//! # `MARKET_LOT_SIZE` is never the lot
//!
//! `MARKET_LOT_SIZE` is the quantity rule for `MARKET` orders (`filters.md`
//! lines 194-205); `LOT_SIZE` is the symbol's (lines 111-122), and the only one a
//! limit order meets. The model has one lot for every kind of order, so it takes
//! `LOT_SIZE`'s, always, and reports `MARKET_LOT_SIZE` whole -- even when
//! `LOT_SIZE` is absent, which leaves the lot at zero, and the symbol one
//! `MaCrossover` refuses to trade (above), rather than borrowing the market
//! order's step.
//!
//! That matters more than the symmetry suggests. `MaCrossover` sends market
//! orders only, and a market order meets both rules. `LOT_SIZE`'s are the
//! symbol's, with no exception by order type (`filters.md` lines 112 and
//! 118-122); a quote-quantity `MARKET` order *"will not break `LOT_SIZE` filter
//! rules"* (`CHANGELOG.md` line 2513); and one is refused when the book holds
//! less than `LOT_SIZE`'s minimum (`errors.md` line 403). So where a symbol
//! lists both, every order `MaCrossover` sends meets two quantity rules; the
//! model checks one of them, and only its step. A refusal under the other reads
//! `Filter failure: MARKET_LOT_SIZE` (`errors.md` line 435), and this report is
//! what lets criterion B name it.
//!
//! # Two notional filters
//!
//! `MIN_NOTIONAL` is the older; `NOTIONAL` was added later (`CHANGELOG.md` line
//! 2249). Both bound a limit order at its own price (`filters.md` lines 137,
//! 144-145 and 167-169), so where a symbol has both, the floor a limit order must
//! clear is the larger, and that is the one the definition takes. The other is
//! reported whole, its `minNotional` included. On a tie, `NOTIONAL`'s is taken.
//!
//! Where both part from the model is the market order. The model holds every
//! order to its floor at the price the order would pay. The venue applies a
//! filter's floor to a `MARKET` order only when `applyToMarket` or
//! `applyMinToMarket` says so (lines 138 and 160), and then at a reference price
//! or an average over `avgPriceMins` minutes (lines 139-142 and 162-165), not at
//! the touch. Those fields are reported.
//!
//! # Rate limits, `ORDERS` among them
//!
//! Each entry of the answer's `rateLimits` is reported whole and exchange-wide,
//! under the name `rateLimits`, with its own `rateLimitType` among its fields.
//! They are not all budgets on requests. `ORDERS` (`enums.md` lines 146-155) is
//! the unfilled order rate limit, and this answer is where the venue states it
//! (`faqs/order_count_decrement.md` lines 16-18): every order placed counts
//! against it (line 37), and an order past it is refused with HTTP 429 and
//! `-1015`, *"Too many new orders"* (line 57; `errors.md` lines 43-45). That is
//! a rule about orders the model does not have, and criterion B names *"a rate
//! limit we do not model"* as one way it fails (`docs/live-run.md` §3).
//!
//! No entry is read, only reported, so a limiter the venue adds tomorrow
//! arrives in the report rather than being skipped as unknown.
//!
//! # Strict about what it reads, verbatim about the rest
//!
//! The fields the definition takes are decimal strings, and they go straight to
//! fixed-point (invariant 1). Absent, not a string, malformed, too precise or
//! below zero is an error naming the field (invariant 5). A bare JSON number is
//! refused rather than read, since reading one means going through a float.
//!
//! Everything else stays the venue's own text, never interpreted. So a field
//! Binance adds to a filter tomorrow is reported rather than ignored, and a field
//! nothing here uses cannot make an answer unreadable.
//!
//! # What it does not read
//!
//! - **The symbol's other fields** -- `orderTypes`, the `…Allowed` flags,
//!   `permissionSets`, the precisions (`rest-api.md` lines 827-864). They say
//!   which *kinds* of request the venue takes rather than which values, and the
//!   venue refuses the wrong kind loudly and by name: *"Market is closed."*,
//!   *"Market orders are not supported for this symbol."*, *"This symbol is not
//!   permitted for this account."* (`errors.md` lines 376, 378 and 398).
//! - **`MAX_ASSET`**, which only the signed `GET /api/v3/myFilters` shows
//!   (`rest-api.md` line 4910), so it cannot appear in this answer.
//! - **Execution rules**, from their own endpoint (`GET /api/v3/executionRules`,
//!   `rest-api.md` lines 878-925).
//!
//! # Not written down here
//!
//! Nothing is persisted. Criterion B's replay must judge the model against the
//! rules the live run actually used, which a fetch at judging time need not
//! return; how a run records them is the live journal's decision
//! (`docs/live-run.md` §4).
//!
//! [`Filters`]: quant_core::instrument::Filters
//! [`Filters::accepts`]: quant_core::instrument::Filters::accepts
//! [`Filters::rejects`]: quant_core::instrument::Filters::rejects
//! [`Instrument::round_price`]: quant_core::instrument::Instrument::round_price

use core::fmt;
use std::collections::BTreeMap;

use quant_core::fixed::{Notional, ParseFixedError, Px, Qty};
use quant_core::instrument::{Exchange, InstrumentDef, InstrumentKind};
use serde_json::value::RawValue;

use crate::rest::{SnapshotClient, SnapshotError};

#[cfg(test)]
mod tests;

/// The status a symbol must have to be traded (`enums.md` lines 7-11).
pub const TRADING: &str = "TRADING";

const PRICE_FILTER: &str = "PRICE_FILTER";
const LOT_SIZE: &str = "LOT_SIZE";
const MIN_NOTIONAL: &str = "MIN_NOTIONAL";
const NOTIONAL: &str = "NOTIONAL";

/// The name every rate limit is reported under: the answer's own name for the
/// list, since a rate limit is not a filter and has no `filterType`.
const RATE_LIMITS: &str = "rateLimits";

/// One symbol's rules as the venue states them: the part the platform models,
/// and every part it does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueInstrument {
    /// The instrument, with every rule in it taken from the venue.
    pub definition: InstrumentDef,
    /// Every venue rule the definition does not carry: the symbol's filters,
    /// then the exchange's, then the answer's rate limits, each in the order
    /// the venue listed them.
    pub unmodelled: Vec<UnmodelledRule>,
}

/// A venue rule the platform's
/// [`Filters`](quant_core::instrument::Filters) cannot express, as the venue
/// stated it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnmodelledRule {
    /// Whether the venue stated it for the symbol or for the exchange as a
    /// whole.
    pub scope: RuleScope,
    /// The venue's name for the rule. For a filter, its `filterType`:
    /// `MARKET_LOT_SIZE`, `NOTIONAL`. An order refused under one is refused as
    /// `Filter failure: <this>` (`errors.md` lines 425-446). A rate limit is
    /// not a filter, and goes by the name of the list the venue states it in,
    /// `rateLimits`, with its own `rateLimitType` among its fields.
    pub name: String,
    /// Every field the definition did not take, sorted by the venue's name for
    /// it, each value as the venue wrote it: a string's contents, or the JSON
    /// text of anything else.
    pub fields: Vec<(String, String)>,
}

impl fmt::Display for UnmodelledRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)?;
        if self.scope == RuleScope::Exchange {
            f.write_str(" (exchange-wide)")?;
        }
        for (key, value) in &self.fields {
            write!(f, " {key}={value}")?;
        }
        Ok(())
    }
}

/// Where the venue listed a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleScope {
    /// The symbol's own `filters`.
    Symbol,
    /// The answer's own lists rather than the symbol's: `exchangeFilters`,
    /// which bind the account across every symbol (`filters.md` lines
    /// 330-377), and `rateLimits`, which bind every request from one IP or,
    /// for `ORDERS`, every order from one account (`rest-api.md` lines 152, 162
    /// and 169).
    Exchange,
}

/// Why an `exchangeInfo` answer does not give a symbol the platform can trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExchangeInfoError {
    /// Not JSON, or not an object whose `symbols` -- and whose
    /// `exchangeFilters` and `rateLimits`, where present -- are lists of
    /// objects.
    NotExchangeInfo(String),
    /// The answer does not list the symbol asked for.
    SymbolAbsent(String),
    /// The answer lists the symbol more than once, so which entry states its
    /// rules cannot be told.
    SymbolRepeated(String),
    /// The symbol is listed, with a status other than [`TRADING`].
    NotTrading { symbol: String, status: String },
    /// A field the definition needs is absent, `null` or empty.
    MissingField(&'static str),
    /// A field the definition needs is not the shape the documentation gives
    /// it: a bare number where a decimal string belongs, a list that is not a
    /// list of objects.
    Malformed { field: &'static str, value: String },
    /// A decimal string that will not parse to fixed-point.
    BadNumber {
        field: &'static str,
        value: String,
        source: ParseFixedError,
    },
    /// A step or a floor below zero, which no rule of the documented kind has.
    Negative { field: &'static str, value: String },
    /// A `LOT_SIZE.stepSize` of zero, which the documentation gives no meaning.
    ZeroLotStep,
    /// A filter the definition reads, listed more than once, so which one the
    /// venue enforces cannot be told.
    RepeatedFilter(&'static str),
}

impl fmt::Display for ExchangeInfoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotExchangeInfo(e) => write!(f, "not an exchangeInfo answer: {e}"),
            Self::SymbolAbsent(symbol) => write!(f, "the answer does not list {symbol}"),
            Self::SymbolRepeated(symbol) => write!(
                f,
                "the answer lists {symbol} more than once, and which entry states its rules \
                 cannot be told"
            ),
            Self::NotTrading { symbol, status } => {
                write!(f, "{symbol} has status {status}, not {TRADING}")
            }
            Self::MissingField(field) => {
                write!(
                    f,
                    "the answer has no usable `{field}` (absent, null or empty)"
                )
            }
            Self::Malformed { field, value } => write!(
                f,
                "`{field}` is not the shape the documentation gives it: {value}"
            ),
            Self::BadNumber {
                field,
                value,
                source,
            } => write!(
                f,
                "`{field}` is not a valid fixed-point value: {value:?} ({source})"
            ),
            Self::Negative { field, value } => write!(
                f,
                "`{field}` is {value}, and no step or floor is below zero"
            ),
            Self::ZeroLotStep => f.write_str(
                "`LOT_SIZE.stepSize` is zero, which the documentation gives no meaning; \
                 refused rather than guessed to disable the lot rule",
            ),
            Self::RepeatedFilter(filter) => write!(
                f,
                "the symbol lists `{filter}` more than once, and which one the venue enforces \
                 cannot be told"
            ),
        }
    }
}

impl std::error::Error for ExchangeInfoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BadNumber { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Why a symbol's rules could not be had from the venue.
///
/// A value to act on -- refuse to submit, keep recording -- and never a default
/// definition (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// No answer to read: a timeout, the network, or a refusal by HTTP status.
    /// A symbol the venue does not list arrives here, as a status: the venue
    /// refuses the request (`rest-api.md` line 794; `-1121`, `errors.md` lines
    /// 136-137) and this client keeps only the status.
    Request(SnapshotError),
    /// The venue answered, and the answer does not give a symbol the platform
    /// can trade.
    Answer(ExchangeInfoError),
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(e) => write!(f, "could not fetch the venue's rules: {e}"),
            Self::Answer(e) => write!(f, "could not use the venue's rules: {e}"),
        }
    }
}

impl std::error::Error for FetchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Request(e) => Some(e),
            Self::Answer(e) => Some(e),
        }
    }
}

/// Ask the venue for `symbol`'s rules, and read them.
///
/// One request and no retry. Whether to try again is the caller's decision,
/// because only the caller knows whether it is starting up or has been
/// recording for a week.
///
/// # Errors
///
/// [`FetchError`], for the request or for what came back.
pub async fn fetch(client: &SnapshotClient, symbol: &str) -> Result<VenueInstrument, FetchError> {
    let body = client
        .exchange_info(symbol)
        .await
        .map_err(FetchError::Request)?;
    parse(symbol, &body).map_err(FetchError::Answer)
}

/// Read `symbol`'s rules out of an `exchangeInfo` answer.
///
/// `symbol` must be spelt as the venue spells it: entries are matched exactly.
///
/// # Errors
///
/// [`ExchangeInfoError`] when the answer does not list `symbol` exactly once,
/// lists it as anything but [`TRADING`], or states a rule the definition reads
/// in a way it cannot take (see the module docs).
pub fn parse(symbol: &str, body: &[u8]) -> Result<VenueInstrument, ExchangeInfoError> {
    let answer: Answer = serde_json::from_slice(body)
        .map_err(|e| ExchangeInfoError::NotExchangeInfo(e.to_string()))?;
    let entry = listed(symbol, &answer.symbols)?;

    // Status first: a symbol that is not trading is the fact a reader needs,
    // whatever else is wrong with its entry.
    let status = text(entry, "status")?;
    if status != TRADING {
        return Err(ExchangeInfoError::NotTrading {
            symbol: symbol.to_owned(),
            status,
        });
    }
    let base = text(entry, "baseAsset")?;
    let quote = text(entry, "quoteAsset")?;

    let raw_filters = field(entry, "filters").ok_or(ExchangeInfoError::MissingField("filters"))?;
    let filters: Vec<Object> =
        serde_json::from_str(raw_filters.get()).map_err(|_| ExchangeInfoError::Malformed {
            field: "filters",
            value: raw_filters.get().to_owned(),
        })?;
    let filters = named(&filters, "filters[].filterType")?;
    let exchange_filters = named(
        answer.exchange_filters.as_deref().unwrap_or_default(),
        "exchangeFilters[].filterType",
    )?;

    let rules = Rules::read(&filters)?;

    let mut unmodelled = Vec::new();
    for (filter_type, filter) in &filters {
        let taken = rules.taken_from(filter_type);
        let fields = untaken(filter, taken);
        // A filter is left out only when the definition took all of it. One
        // the definition takes nothing from is a rule in its own right even
        // with no fields: its presence is what the venue enforces.
        if taken.is_none() || !fields.is_empty() {
            unmodelled.push(UnmodelledRule {
                scope: RuleScope::Symbol,
                name: filter_type.clone(),
                fields,
            });
        }
    }
    for (filter_type, filter) in &exchange_filters {
        unmodelled.push(UnmodelledRule {
            scope: RuleScope::Exchange,
            name: filter_type.clone(),
            fields: untaken(filter, None),
        });
    }
    // Every rate limit, whole: none is read, and `ORDERS` is a rule about
    // orders the model does not have (see the module docs).
    for limit in answer.rate_limits.as_deref().unwrap_or_default() {
        unmodelled.push(UnmodelledRule {
            scope: RuleScope::Exchange,
            name: RATE_LIMITS.to_owned(),
            fields: as_written(limit, |_| true),
        });
    }

    Ok(VenueInstrument {
        definition: InstrumentDef {
            exchange: Exchange::Binance,
            symbol: symbol.to_owned(),
            base,
            quote,
            kind: InstrumentKind::Spot,
            // An absent filter is no rule, which is zero (see the module docs).
            tick_size: rules.tick.unwrap_or(Px::ZERO),
            lot_size: rules.lot.unwrap_or(Qty::ZERO),
            min_notional: rules.floor().unwrap_or(Notional::ZERO),
        },
        unmodelled,
    })
}

/// A JSON object with every value kept as the venue's own text.
type Object = BTreeMap<String, Box<RawValue>>;

/// The parts of the answer read as a whole. Everything else in it -- the
/// server time, `sors` -- is not looked at.
#[derive(serde::Deserialize)]
struct Answer {
    symbols: Vec<Object>,
    /// Absent or `null` is no exchange-wide filters, which are optional too.
    #[serde(rename = "exchangeFilters", default)]
    exchange_filters: Option<Vec<Object>>,
    /// Absent or `null` is no rate limits: *"All limits are optional"*
    /// (`rest-api.md` line 819).
    #[serde(rename = "rateLimits", default)]
    rate_limits: Option<Vec<Object>>,
}

/// The three rules the definition takes, as read from the symbol's filters.
struct Rules {
    tick: Option<Px>,
    lot: Option<Qty>,
    /// `MIN_NOTIONAL.minNotional`.
    older_floor: Option<Notional>,
    /// `NOTIONAL.minNotional`.
    newer_floor: Option<Notional>,
}

impl Rules {
    fn read(filters: &[(String, &Object)]) -> Result<Self, ExchangeInfoError> {
        let mut rules = Self {
            tick: None,
            lot: None,
            older_floor: None,
            newer_floor: None,
        };
        for (filter_type, filter) in filters {
            match filter_type.as_str() {
                PRICE_FILTER => {
                    first(rules.tick.is_some(), PRICE_FILTER)?;
                    rules.tick = Some(decimal(filter, "tickSize", "PRICE_FILTER.tickSize")?);
                }
                LOT_SIZE => {
                    first(rules.lot.is_some(), LOT_SIZE)?;
                    let step: Qty = decimal(filter, "stepSize", "LOT_SIZE.stepSize")?;
                    if step.is_zero() {
                        return Err(ExchangeInfoError::ZeroLotStep);
                    }
                    rules.lot = Some(step);
                }
                MIN_NOTIONAL => {
                    first(rules.older_floor.is_some(), MIN_NOTIONAL)?;
                    rules.older_floor =
                        Some(decimal(filter, "minNotional", "MIN_NOTIONAL.minNotional")?);
                }
                NOTIONAL => {
                    first(rules.newer_floor.is_some(), NOTIONAL)?;
                    rules.newer_floor =
                        Some(decimal(filter, "minNotional", "NOTIONAL.minNotional")?);
                }
                _ => {}
            }
        }
        Ok(rules)
    }

    /// Which filter's `minNotional` the definition takes: the larger, since a
    /// limit order must clear both, and `NOTIONAL`'s on a tie.
    fn floor_from(&self) -> Option<&'static str> {
        match (self.older_floor, self.newer_floor) {
            (None, None) => None,
            (Some(_), None) => Some(MIN_NOTIONAL),
            (None, Some(_)) => Some(NOTIONAL),
            (Some(older), Some(newer)) => Some(if older > newer {
                MIN_NOTIONAL
            } else {
                NOTIONAL
            }),
        }
    }

    fn floor(&self) -> Option<Notional> {
        match self.floor_from() {
            Some(MIN_NOTIONAL) => self.older_floor,
            Some(_) => self.newer_floor,
            None => None,
        }
    }

    /// The field the definition took from a filter of this type, if any.
    fn taken_from(&self, filter_type: &str) -> Option<&'static str> {
        match filter_type {
            PRICE_FILTER => Some("tickSize"),
            LOT_SIZE => Some("stepSize"),
            MIN_NOTIONAL | NOTIONAL if self.floor_from() == Some(filter_type) => {
                Some("minNotional")
            }
            _ => None,
        }
    }
}

/// Refuse a second filter of a type the definition reads.
fn first(seen: bool, filter_type: &'static str) -> Result<(), ExchangeInfoError> {
    if seen {
        return Err(ExchangeInfoError::RepeatedFilter(filter_type));
    }
    Ok(())
}

/// The one entry in `symbols` that is `symbol`.
fn listed<'a>(symbol: &str, symbols: &'a [Object]) -> Result<&'a Object, ExchangeInfoError> {
    let mut ours = symbols
        .iter()
        .filter(|entry| field(entry, "symbol").and_then(string).as_deref() == Some(symbol));
    let entry = ours
        .next()
        .ok_or_else(|| ExchangeInfoError::SymbolAbsent(symbol.to_owned()))?;
    if ours.next().is_some() {
        return Err(ExchangeInfoError::SymbolRepeated(symbol.to_owned()));
    }
    Ok(entry)
}

/// Each filter with its `filterType`, which every filter must have: a rule
/// with no name can be neither matched against a rejection nor told apart from
/// one the definition should have read.
fn named<'a>(
    filters: &'a [Object],
    name_field: &'static str,
) -> Result<Vec<(String, &'a Object)>, ExchangeInfoError> {
    filters
        .iter()
        .map(|filter| Ok((text_as(filter, "filterType", name_field)?, filter)))
        .collect()
}

/// A field, unless it is absent or `null`, which this module treats alike.
fn field<'a>(object: &'a Object, key: &str) -> Option<&'a RawValue> {
    object
        .get(key)
        .map(Box::as_ref)
        .filter(|raw| raw.get() != "null")
}

/// What a JSON string says, or `None` when the value is not a string.
fn string(raw: &RawValue) -> Option<String> {
    serde_json::from_str(raw.get()).ok()
}

/// A required, non-empty string field of the symbol's entry.
fn text(entry: &Object, key: &'static str) -> Result<String, ExchangeInfoError> {
    text_as(entry, key, key)
}

/// A required, non-empty string field, reported under `name` when it is not.
fn text_as(object: &Object, key: &str, name: &'static str) -> Result<String, ExchangeInfoError> {
    let raw = field(object, key).ok_or(ExchangeInfoError::MissingField(name))?;
    let value = string(raw).ok_or_else(|| ExchangeInfoError::Malformed {
        field: name,
        value: raw.get().to_owned(),
    })?;
    if value.is_empty() {
        return Err(ExchangeInfoError::MissingField(name));
    }
    Ok(value)
}

/// A rule the definition takes: a decimal string, straight to fixed-point.
fn decimal<T: FixedRule>(
    filter: &Object,
    key: &str,
    name: &'static str,
) -> Result<T, ExchangeInfoError> {
    let raw = field(filter, key).ok_or(ExchangeInfoError::MissingField(name))?;
    // Never a bare number: reading one means going through a float.
    let written = string(raw).ok_or_else(|| ExchangeInfoError::Malformed {
        field: name,
        value: raw.get().to_owned(),
    })?;
    let value: T = written
        .parse()
        .map_err(|source| ExchangeInfoError::BadNumber {
            field: name,
            value: written.clone(),
            source,
        })?;
    if value.below_zero() {
        return Err(ExchangeInfoError::Negative {
            field: name,
            value: written,
        });
    }
    Ok(value)
}

/// Every field of a filter but its name and the one the definition took, as
/// the venue wrote each.
fn untaken(filter: &Object, taken: Option<&str>) -> Vec<(String, String)> {
    as_written(filter, |key| key != "filterType" && Some(key) != taken)
}

/// The fields of `object` that `keep` admits, sorted by the venue's name for
/// each, every value as the venue wrote it.
fn as_written(object: &Object, keep: impl Fn(&str) -> bool) -> Vec<(String, String)> {
    object
        .iter()
        .filter(|(key, _)| keep(key))
        .map(|(key, raw)| (key.clone(), venue_text(raw)))
        .collect()
}

/// A value as the venue wrote it: a string's contents, or the JSON text of
/// anything else.
fn venue_text(raw: &RawValue) -> String {
    string(raw).unwrap_or_else(|| raw.get().to_owned())
}

/// The fixed-point types a rule is read into.
trait FixedRule: core::str::FromStr<Err = ParseFixedError> {
    fn below_zero(&self) -> bool;
}

impl FixedRule for Px {
    fn below_zero(&self) -> bool {
        self.is_negative()
    }
}

impl FixedRule for Qty {
    fn below_zero(&self) -> bool {
        self.is_negative()
    }
}

impl FixedRule for Notional {
    fn below_zero(&self) -> bool {
        self.is_negative()
    }
}
