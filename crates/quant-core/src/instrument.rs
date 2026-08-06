//! Venues and tradable instruments.
//!
//! # Why an `InstrumentId` and not a `String`
//!
//! Market events arrive tens of thousands per second and get compared,
//! bucketed and routed on every one. Carrying `"BTCUSDT"` around means a
//! heap allocation per event and a string compare per lookup, on the hot
//! path, forever. Instead symbols are interned once at startup into a small
//! `Copy` integer handle; the string lives in one place, in the registry.
//!
//! # Why the exchange filters live here
//!
//! `tick_size`, `lot_size` and `min_notional` are venue *rules*, not
//! strategy preferences. If they live in the strategy, every strategy has to
//! remember them and one of them eventually will not. Keeping them on the
//! instrument lets the execution layer reject or round an order centrally,
//! before it reaches the wire -- and lets the backtester enforce the exact
//! same constraints, so a backtest cannot fill an order the venue would
//! have rejected. Backtest realism is mostly a matter of modelling the
//! things that say "no".

use core::fmt;
use std::collections::HashMap;

use crate::fixed::{Notional, Px, Qty};

/// A trading venue.
///
/// An enum rather than a string because the set is small, closed, and
/// exhaustively matching on it is how we will catch "you added Kraken but
/// forgot to implement its fee schedule" at compile time.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    Binance,
    BinanceUsdFutures,
    Coinbase,
    Kraken,
    /// Not a real venue: the simulated counterparty used by the backtester
    /// and paper trader. Having it in the same enum means simulated fills
    /// flow through exactly the same code paths as real ones.
    Simulated,
}

impl Exchange {
    /// Stable numeric code for persisting a venue.
    ///
    /// These numbers are stamped into every recorded raw file header, so they
    /// are **permanent**: a code may never be reused for a different venue and
    /// never renumbered. The enum's declaration order is therefore free to
    /// change, and the variants can be reordered alphabetically later without
    /// making last year's capture unreadable.
    ///
    /// The match is exhaustive on purpose. Adding a venue is then a compile
    /// error here until someone assigns it a number, rather than a runtime
    /// surprise the first time we try to read the file back.
    #[must_use]
    pub const fn wire_code(self) -> u16 {
        match self {
            Self::Binance => 1,
            Self::BinanceUsdFutures => 2,
            Self::Coinbase => 3,
            Self::Kraken => 4,
            // Deliberately far from the real venues: a simulated file showing
            // up in a directory of production capture should look obviously
            // different, not adjacent to Kraken.
            Self::Simulated => 1_000,
        }
    }

    /// Inverse of [`Exchange::wire_code`].
    ///
    /// Returns `None` for an unknown code rather than a default. Reading a
    /// file recorded by a newer build that knows a venue we do not is a
    /// stop-and-look moment: we cannot know what its fee model, tick rules or
    /// payload dialect are, so guessing is worse than refusing.
    #[must_use]
    pub const fn from_wire_code(code: u16) -> Option<Self> {
        match code {
            1 => Some(Self::Binance),
            2 => Some(Self::BinanceUsdFutures),
            3 => Some(Self::Coinbase),
            4 => Some(Self::Kraken),
            1_000 => Some(Self::Simulated),
            _ => None,
        }
    }

    /// The venue's name as it appears in a partition directory and a database
    /// column.
    ///
    /// A published interface, like [`Exchange::wire_code`] but readable: it is
    /// `exchange=binance` on disk and `binance` in Postgres, and changing one of
    /// these strings orphans every partition recorded under the old spelling.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::BinanceUsdFutures => "binance_usdm",
            Self::Coinbase => "coinbase",
            Self::Kraken => "kraken",
            Self::Simulated => "simulated",
        }
    }

    /// Inverse of [`Exchange::name`], for reading a partition path back.
    ///
    /// `None` rather than a default, for the same reason as
    /// [`Exchange::from_wire_code`]: a directory naming a venue this build does
    /// not know is a stop-and-look moment, not something to guess at.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        [
            Self::Binance,
            Self::BinanceUsdFutures,
            Self::Coinbase,
            Self::Kraken,
            Self::Simulated,
        ]
        .into_iter()
        .find(|venue| venue.name() == name)
    }
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What kind of contract this is.
///
/// Present from day one even though we start with spot, because the
/// distinction changes P&L accounting (funding payments, margin, expiry) and
/// retrofitting it into a position model later is invasive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Spot,
    PerpetualSwap,
    Future,
}

/// A `Copy` handle to an instrument. Cheap to pass, compare and store.
///
/// Only meaningful relative to the [`InstrumentRegistry`] that issued it.
/// Ids are assigned in registration order and are stable for the life of a
/// process, but they are **not** stable across runs -- never persist one.
/// Persist the (exchange, symbol) pair instead.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct InstrumentId(u32);

impl InstrumentId {
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

impl fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// Static definition of something we can trade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instrument {
    pub id: InstrumentId,
    pub exchange: Exchange,
    /// The venue's own symbol, verbatim (`BTCUSDT`, `BTC-USD`, `XBT/USD`).
    /// Not normalized: we send this back to the venue, so it must match.
    pub symbol: String,
    pub base: String,
    pub quote: String,
    pub kind: InstrumentKind,
    /// Minimum price increment. Orders must be a multiple of this.
    pub tick_size: Px,
    /// Minimum quantity increment.
    pub lot_size: Qty,
    /// Venue-enforced floor on order value.
    pub min_notional: Notional,
}

impl Instrument {
    /// Round a price toward zero to a valid tick.
    ///
    /// Toward zero rather than to-nearest so the result is never *more*
    /// aggressive than intended -- rounding a bid up could cross the spread.
    /// Callers that want the other direction should say so explicitly.
    #[must_use]
    pub fn round_price(&self, px: Px) -> Px {
        if self.tick_size.is_zero() {
            return px;
        }
        Px::from_raw(px.raw() - px.raw() % self.tick_size.raw())
    }

    /// Round a quantity toward zero to a valid lot.
    #[must_use]
    pub fn round_qty(&self, qty: Qty) -> Qty {
        if self.lot_size.is_zero() {
            return qty;
        }
        Qty::from_raw(qty.raw() - qty.raw() % self.lot_size.raw())
    }

    /// Whether the venue would accept this order's size and price.
    ///
    /// Used by the risk layer live *and* by the simulated venue in
    /// backtests, so a strategy cannot profit in simulation from orders that
    /// would have been rejected in production.
    #[must_use]
    pub fn is_valid_order(&self, px: Px, qty: Qty) -> bool {
        let abs_qty = qty.abs();
        if abs_qty.is_zero() || !px.is_positive() {
            return false;
        }
        if self.round_price(px) != px || self.round_qty(abs_qty) != abs_qty {
            return false;
        }
        match px.notional(abs_qty) {
            Some(n) => n >= self.min_notional,
            None => false,
        }
    }
}

/// Interns instruments and hands out [`InstrumentId`]s.
///
/// Built once at startup from the venue's exchange-info endpoint (or from a
/// fixture, in tests and backtests) and then treated as immutable. A
/// registry that can change mid-run is a registry where an id can mean two
/// different things at two different times.
#[derive(Debug, Default)]
pub struct InstrumentRegistry {
    instruments: Vec<Instrument>,
    by_key: HashMap<(Exchange, String), InstrumentId>,
}

impl InstrumentRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an instrument, or return the existing id if the
    /// (exchange, symbol) pair is already known.
    ///
    /// Takes the definition without an id and assigns one, so callers cannot
    /// invent ids.
    pub fn register(&mut self, def: InstrumentDef) -> InstrumentId {
        let key = (def.exchange, def.symbol.clone());
        if let Some(&existing) = self.by_key.get(&key) {
            return existing;
        }
        let id = InstrumentId(u32::try_from(self.instruments.len()).expect("too many instruments"));
        self.instruments.push(Instrument {
            id,
            exchange: def.exchange,
            symbol: def.symbol,
            base: def.base,
            quote: def.quote,
            kind: def.kind,
            tick_size: def.tick_size,
            lot_size: def.lot_size,
            min_notional: def.min_notional,
        });
        self.by_key.insert(key, id);
        id
    }

    #[must_use]
    pub fn get(&self, id: InstrumentId) -> Option<&Instrument> {
        self.instruments.get(id.index())
    }

    /// Panicking lookup, for ids this registry issued.
    ///
    /// Safe by construction on the hot path: ids only come from `register`,
    /// and the registry never shrinks.
    #[must_use]
    pub fn expect(&self, id: InstrumentId) -> &Instrument {
        self.get(id).expect("instrument id not in this registry")
    }

    #[must_use]
    pub fn lookup(&self, exchange: Exchange, symbol: &str) -> Option<InstrumentId> {
        // Allocates on the miss path only when the key is absent; acceptable
        // because lookups by string happen at subscription time, not per event.
        self.by_key.get(&(exchange, symbol.to_owned())).copied()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.instruments.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Instrument> {
        self.instruments.iter()
    }
}

/// An instrument definition awaiting registration (i.e. without an id).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentDef {
    pub exchange: Exchange,
    pub symbol: String,
    pub base: String,
    pub quote: String,
    pub kind: InstrumentKind,
    pub tick_size: Px,
    pub lot_size: Qty,
    pub min_notional: Notional,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn btcusdt() -> InstrumentDef {
        InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.01".parse().unwrap(),
            lot_size: "0.00001".parse().unwrap(),
            min_notional: "5".parse().unwrap(),
        }
    }

    #[test]
    fn wire_codes_round_trip_and_are_pinned() {
        for ex in [
            Exchange::Binance,
            Exchange::BinanceUsdFutures,
            Exchange::Coinbase,
            Exchange::Kraken,
            Exchange::Simulated,
        ] {
            assert_eq!(Exchange::from_wire_code(ex.wire_code()), Some(ex));
        }

        // Pinned literals. These values are in recorded files; if this test
        // starts failing because someone reordered the enum, the fix is to
        // restore the numbers, not to update the test.
        assert_eq!(Exchange::Binance.wire_code(), 1);
        assert_eq!(Exchange::BinanceUsdFutures.wire_code(), 2);
        assert_eq!(Exchange::Coinbase.wire_code(), 3);
        assert_eq!(Exchange::Kraken.wire_code(), 4);
        assert_eq!(Exchange::Simulated.wire_code(), 1_000);

        // An unknown venue is refused, not defaulted.
        assert_eq!(Exchange::from_wire_code(0), None);
        assert_eq!(Exchange::from_wire_code(5), None);
        assert_eq!(Exchange::from_wire_code(u16::MAX), None);
    }

    #[test]
    fn venue_names_round_trip_and_match_what_is_on_disk() {
        for ex in [
            Exchange::Binance,
            Exchange::BinanceUsdFutures,
            Exchange::Coinbase,
            Exchange::Kraken,
            Exchange::Simulated,
        ] {
            assert_eq!(Exchange::from_name(ex.name()), Some(ex));
            // Display and `name` must not drift: the directory is written from
            // one and read back with the other.
            assert_eq!(ex.to_string(), ex.name());
        }

        // Pinned, because these strings are directory names in recorded data and
        // values in the metadata database.
        assert_eq!(Exchange::Binance.name(), "binance");
        assert_eq!(Exchange::BinanceUsdFutures.name(), "binance_usdm");

        assert_eq!(Exchange::from_name("BINANCE"), None, "case is significant");
        assert_eq!(Exchange::from_name("bitmex"), None);
        assert_eq!(Exchange::from_name(""), None);
    }

    #[test]
    fn registration_is_idempotent() {
        let mut reg = InstrumentRegistry::new();
        let a = reg.register(btcusdt());
        let b = reg.register(btcusdt());
        assert_eq!(a, b);
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.lookup(Exchange::Binance, "BTCUSDT"), Some(a));
        assert_eq!(reg.lookup(Exchange::Coinbase, "BTCUSDT"), None);
    }

    #[test]
    fn rounds_toward_zero_so_we_never_get_more_aggressive() {
        let mut reg = InstrumentRegistry::new();
        let id = reg.register(btcusdt());
        let inst = reg.expect(id);

        assert_eq!(
            inst.round_price("68123.459".parse().unwrap()).to_string(),
            "68123.45"
        );
        assert_eq!(
            inst.round_qty("0.000019".parse().unwrap()).to_string(),
            "0.00001"
        );
        // Negative (short) sizes round toward zero too -- i.e. smaller size.
        assert_eq!(
            inst.round_qty("-0.000019".parse().unwrap()).to_string(),
            "-0.00001"
        );
    }

    #[test]
    fn rejects_orders_the_venue_would_reject() {
        let mut reg = InstrumentRegistry::new();
        let id = reg.register(btcusdt());
        let inst = reg.expect(id);

        let px: Px = "68123.45".parse().unwrap();
        assert!(inst.is_valid_order(px, "0.001".parse().unwrap()));

        // Below min_notional of 5 USDT.
        assert!(!inst.is_valid_order(px, "0.00001".parse().unwrap()));
        // Not a whole number of ticks.
        assert!(!inst.is_valid_order("68123.456".parse().unwrap(), "0.001".parse().unwrap()));
        // Not a whole number of lots.
        assert!(!inst.is_valid_order(px, "0.0000123".parse().unwrap()));
        // Zero size, and non-positive price.
        assert!(!inst.is_valid_order(px, Qty::ZERO));
        assert!(!inst.is_valid_order(Px::ZERO, "0.001".parse().unwrap()));

        // Shorts are validated on absolute size.
        assert!(inst.is_valid_order(px, "-0.001".parse().unwrap()));
    }
}
