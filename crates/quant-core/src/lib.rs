//! Core vocabulary for the quant platform.
//!
//! Everything else -- venue adapters, the recorder, the backtester, the
//! execution engine, the risk layer -- depends on this crate and on nothing
//! venue-specific. If a type in here mentions Binance, the design has gone
//! wrong.
//!
//! Three invariants are enforced here rather than by convention, because
//! convention does not survive a codebase past a few thousand lines:
//!
//! 1. **Money is integral.** [`fixed`] gives exact fixed-point prices,
//!    quantities and notionals. The workspace denies `clippy::float_arithmetic`
//!    so a stray `f64` fails the build, not a reconciliation two months later.
//! 2. **Time is explicit and doubled.** [`time`] carries both the venue's
//!    timestamp and our receive timestamp, and only the latter is ever
//!    dispatched on. This is what keeps lookahead bias out of backtests.
//! 3. **Gaps are data.** [`event::Gap`] makes "we were not watching" a
//!    first-class event, so a backtest can decline to trade through a hole
//!    instead of pretending the market stood still.
//!
//! ## Where this sits
//!
//! ```text
//!               ┌── HistoricalSource (Parquet replay)
//!  EventSource ─┼── ReplaySource     (raw capture, wall-clock paced)
//!               └── LiveSource       (venue WebSocket)
//!                        │
//!                        │  MarketEvent  ← defined here
//!                        ▼
//!                     Engine ─────────►  Strategy
//!                        │
//!                        │  OrderRequest ← will be defined here (M3)
//!                        ▼
//!                    RiskLayer  (mandatory chokepoint, M6)
//!                        │
//!                        ▼
//!                 ┌── SimulatedVenue
//!  ExecutionVenue ┼── PaperVenue
//!                 └── LiveVenue
//! ```
//!
//! A strategy must not be able to tell which sources it is wired to. That
//! property is the reason a backtest is worth anything.

pub mod event;
pub mod fixed;
pub mod instrument;
pub mod time;

pub use event::{
    BookDelta, BookSnapshot, EventMeta, Gap, GapCause, Level, MarketEvent, Side, Trade,
};
pub use fixed::{Notional, ParseFixedError, Px, Qty, SCALE, SCALE_DECIMALS};
pub use instrument::{
    Exchange, Instrument, InstrumentDef, InstrumentId, InstrumentKind, InstrumentRegistry,
};
pub use time::{Clock, ManualClock, SystemClock, Ts};

/// Version of the on-disk / on-wire event contract.
///
/// Stamped into every recorded file header. When this changes, old data does
/// not become unreadable -- the normalizer keeps the ability to read every
/// version it has ever written. Recorded market data is irreplaceable; a
/// migration that cannot read last year's capture is a data loss event.
pub const EVENT_SCHEMA_VERSION: u16 = 1;
