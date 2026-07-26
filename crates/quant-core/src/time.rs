//! Timestamps and clocks.
//!
//! # The two-timestamp rule
//!
//! Every market event in this system carries **two** timestamps:
//!
//! - `exchange_ts`: what the venue said. Useful for measuring venue-side
//!   behaviour and for cross-venue analysis. Comes from a clock we do not
//!   control, may be coarse (Binance is millisecond-resolution), and may
//!   even go backwards across a venue failover.
//! - `local_recv_ts`: when *our process* first saw the bytes. This is the
//!   only timestamp a strategy is allowed to make decisions on.
//!
//! Why this matters more than almost anything else in the codebase: if a
//! backtest replays events ordered by `exchange_ts` and lets the strategy
//! act at `exchange_ts`, the strategy is trading on information it could not
//! possibly have had yet -- the network hop, the venue's own batching delay
//! and our parse time are all erased. That is lookahead bias, and it makes
//! bad strategies look excellent. It is the single most common reason a
//! backtest that shows a Sharpe of 3 loses money live.
//!
//! The difference `local_recv_ts - exchange_ts` is also a free, continuously
//! recorded measure of our latency to the venue. It is one of the first
//! things worth graphing in the dashboard.
//!
//! # Resolution and representation
//!
//! `i64` nanoseconds since the Unix epoch, UTC, always. Range is ±292 years
//! around 1970, which is fine. Nanoseconds because microseconds are already
//! too coarse for latency measurement on a fast path, and because widening
//! a persisted timestamp later is a painful migration.
//!
//! We deliberately do not use a calendar type here. Time zones, leap
//! seconds and human-readable formatting are presentation concerns; the
//! engine only ever needs a monotone integer it can compare and subtract.

use core::fmt;
use core::ops::{Add, Sub};
use core::time::Duration;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A point in time: nanoseconds since the Unix epoch, UTC.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct Ts(i64);

impl Ts {
    pub const EPOCH: Self = Self(0);
    pub const MIN: Self = Self(i64::MIN);
    pub const MAX: Self = Self(i64::MAX);

    #[must_use]
    pub const fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn from_micros(micros: i64) -> Self {
        Self(micros.saturating_mul(1_000))
    }

    /// Most REST and WebSocket venues quote milliseconds; this is the usual
    /// entry point when parsing an exchange payload.
    #[must_use]
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis.saturating_mul(1_000_000))
    }

    #[must_use]
    pub const fn from_secs(secs: i64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    #[must_use]
    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0 / 1_000_000
    }

    /// Signed difference, in nanoseconds. Negative results are meaningful:
    /// a venue clock ahead of ours produces a negative
    /// `local_recv_ts - exchange_ts`, and that skew is worth alerting on
    /// rather than clamping to zero.
    #[must_use]
    pub const fn delta_nanos(self, earlier: Self) -> i64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl fmt::Debug for Ts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Epoch-nanos with a seconds hint, so log lines stay greppable but a
        // human can still tell roughly when something happened.
        write!(f, "Ts({} /* ~{}s */)", self.0, self.0 / 1_000_000_000)
    }
}

impl fmt::Display for Ts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Add<Duration> for Ts {
    type Output = Self;
    fn add(self, rhs: Duration) -> Self {
        let nanos = i64::try_from(rhs.as_nanos()).expect("duration exceeds Ts range");
        Self(self.0.saturating_add(nanos))
    }
}

impl Sub for Ts {
    /// Nanoseconds, signed. See [`Ts::delta_nanos`] for why not `Duration`.
    type Output = i64;
    fn sub(self, rhs: Self) -> i64 {
        self.delta_nanos(rhs)
    }
}

/// Source of "now".
///
/// Everything that needs the current time takes a `&dyn Clock` rather than
/// calling [`SystemTime::now`] directly. That is what makes the backtester
/// and the live engine the same code: in a backtest the clock is driven by
/// the event stream, so timeouts, rate limiters and time-based strategy
/// logic all behave identically to live -- just faster than real time.
///
/// A component that reaches for the wall clock directly is a component that
/// cannot be backtested. Treat it as a bug.
pub trait Clock: Send + Sync + fmt::Debug {
    fn now(&self) -> Ts;
}

/// Wall-clock time, for live and paper trading.
///
/// Note this is `SystemTime`, not a monotonic clock: it can jump if NTP
/// steps the machine. That is the right trade-off for `local_recv_ts`,
/// because the value has to be comparable across process restarts and
/// across machines. We stamp it exactly once, at ingress, and never
/// recompute it -- so an NTP step can perturb one measurement but cannot
/// corrupt ordering within the recorder's own sequence numbers.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Ts {
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before the Unix epoch");
        Ts::from_nanos(i64::try_from(d.as_nanos()).expect("system clock beyond year 2262"))
    }
}

/// A clock driven explicitly, for backtests and tests.
///
/// The backtest engine advances this to each event's `local_recv_ts` before
/// dispatching that event, so strategy code sees exactly the time it would
/// have seen live.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicI64);

impl ManualClock {
    #[must_use]
    pub fn new(start: Ts) -> Self {
        Self(AtomicI64::new(start.as_nanos()))
    }

    /// Jump the clock to `t`.
    ///
    /// Debug-asserts monotonicity: a backtest that moves time backwards is
    /// replaying out-of-order events, which silently invalidates results.
    /// Better to fail the test run than to produce a plausible-looking
    /// equity curve from a broken replay.
    pub fn set(&self, t: Ts) {
        let prev = self.0.swap(t.as_nanos(), Ordering::Relaxed);
        debug_assert!(
            t.as_nanos() >= prev,
            "clock moved backwards: {prev} -> {}",
            t.as_nanos()
        );
    }

    pub fn advance(&self, d: Duration) {
        let nanos = i64::try_from(d.as_nanos()).expect("duration exceeds Ts range");
        self.0.fetch_add(nanos, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Ts {
        Ts::from_nanos(self.0.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn millis_conversion_matches_binance_payloads() {
        // Binance sends `"E": 1699999999999`.
        let t = Ts::from_millis(1_699_999_999_999);
        assert_eq!(t.as_nanos(), 1_699_999_999_999_000_000);
        assert_eq!(t.as_millis(), 1_699_999_999_999);
    }

    #[test]
    fn latency_delta_is_signed() {
        let exchange = Ts::from_millis(1_000);
        let local = Ts::from_millis(1_042);
        assert_eq!(local.delta_nanos(exchange), 42_000_000);
        // Venue clock ahead of ours: we want to see the negative value, not
        // a clamped zero, because clock skew is a real operational signal.
        assert_eq!(exchange.delta_nanos(local), -42_000_000);
    }

    #[test]
    fn manual_clock_drives_time_deterministically() {
        let clock = ManualClock::new(Ts::from_secs(1_000));
        assert_eq!(clock.now(), Ts::from_secs(1_000));
        clock.advance(Duration::from_millis(250));
        assert_eq!(clock.now(), Ts::from_nanos(1_000_250_000_000));
        clock.set(Ts::from_secs(2_000));
        assert_eq!(clock.now(), Ts::from_secs(2_000));
    }

    #[test]
    fn system_clock_is_plausible() {
        // Sanity check only: after 2020, before 2100.
        let now = SystemClock.now();
        assert!(now > Ts::from_secs(1_577_836_800));
        assert!(now < Ts::from_secs(4_102_444_800));
    }
}
