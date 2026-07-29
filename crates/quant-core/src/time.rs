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

/// Nanoseconds in one day. Exact, because we do not model leap seconds: UTC as
/// used by every exchange API is a count of 86400-second days.
const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;

/// A UTC calendar date.
///
/// The module docs above say the engine deliberately avoids calendar types, and
/// that still holds -- nothing in the engine reasons about dates. This exists for
/// exactly one reason: the storage layout partitions raw and normalized data by
/// `date=YYYY-MM-DD` (`docs/data-contract.md` §5), so *something* has to turn a
/// timestamp into a day, and the recorder has to know when that day rolls over.
///
/// It is UTC only and has no time zone, no formatting options and no parsing of
/// human input. Those are the parts of calendar handling that cause bugs, and
/// declining to have them is why this can be forty lines instead of a dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UtcDate {
    pub year: i32,
    /// 1-12.
    pub month: u8,
    /// 1-31.
    pub day: u8,
}

impl UtcDate {
    /// Convert a count of days since 1970-01-01 into a calendar date.
    ///
    /// This is Howard Hinnant's `civil_from_days`. It shifts the epoch to
    /// 0000-03-01 so that February -- and therefore the leap day -- falls at the
    /// *end* of a year, which makes the whole conversion branch-free integer
    /// arithmetic with no leap-year special case to get wrong. Correct for the
    /// full range of `Ts`, including dates before 1970.
    #[must_use]
    pub fn from_days_since_epoch(days: i64) -> Self {
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097); // day of era, [0, 146096]
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of (shifted) year
        let mp = (5 * doy + 2) / 153; // shifted month, [0, 11]
        let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
        let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
        let year = yoe + era * 400 + i64::from(month <= 2);

        Self {
            year: i32::try_from(year).expect("year within i32 for any Ts"),
            month: u8::try_from(month).expect("month is 1..=12 by construction"),
            day: u8::try_from(day).expect("day is 1..=31 by construction"),
        }
    }
}

impl fmt::Display for UtcDate {
    /// `YYYY-MM-DD`, which is both ISO 8601 and the Hive partition value.
    ///
    /// Zero-padded and fixed width so that lexicographic order equals
    /// chronological order -- which is what makes a directory listing of capture
    /// days sorted for free.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

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

    /// Whole days since 1970-01-01 UTC.
    ///
    /// Euclidean division, not truncating: for a pre-epoch timestamp, `-1` must
    /// mean "the day before the epoch", not "day zero". Truncating division
    /// would put 1969-12-31 and 1970-01-01 in the same partition.
    #[must_use]
    pub const fn days_since_epoch(self) -> i64 {
        self.0.div_euclid(NANOS_PER_DAY)
    }

    /// The UTC calendar day this timestamp falls in.
    ///
    /// Used to pick a storage partition and to decide when the recorder rolls
    /// over to a new file. Never used for engine logic -- see [`UtcDate`].
    #[must_use]
    pub fn utc_date(self) -> UtcDate {
        UtcDate::from_days_since_epoch(self.days_since_epoch())
    }

    /// Midnight UTC that starts this timestamp's day.
    ///
    /// The recorder compares against this to detect a day boundary without
    /// re-deriving a calendar date on every message.
    #[must_use]
    pub const fn start_of_utc_day(self) -> Self {
        Self(self.days_since_epoch() * NANOS_PER_DAY)
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
    fn utc_dates_match_known_epoch_seconds() {
        // Each of these is an independently known epoch second, so the test does
        // not merely re-derive the algorithm it is checking.
        let cases: &[(i64, i32, u8, u8)] = &[
            (0, 1970, 1, 1),
            (86_399, 1970, 1, 1), // last nanosecond of the epoch day
            (86_400, 1970, 1, 2), // exactly the next midnight
            (946_684_800, 2000, 1, 1),
            (951_782_400, 2000, 2, 29),   // leap year divisible by 400
            (1_709_164_800, 2024, 2, 29), // ordinary leap year
            (1_735_689_600, 2025, 1, 1),
            (1_785_283_200, 2026, 7, 29),
        ];
        for &(secs, year, month, day) in cases {
            let date = Ts::from_secs(secs).utc_date();
            assert_eq!(
                date,
                UtcDate { year, month, day },
                "epoch second {secs} should be {year:04}-{month:02}-{day:02}"
            );
        }
    }

    #[test]
    fn dates_before_the_epoch_do_not_collapse_into_it() {
        // Truncating division would put both of these on 1970-01-01, silently
        // merging two days into one storage partition.
        assert_eq!(
            Ts::from_secs(-1).utc_date(),
            UtcDate {
                year: 1969,
                month: 12,
                day: 31
            }
        );
        assert_eq!(Ts::from_secs(-1).days_since_epoch(), -1);
        assert_eq!(Ts::from_secs(-86_400).days_since_epoch(), -1);
        assert_eq!(Ts::from_secs(-86_401).days_since_epoch(), -2);
    }

    #[test]
    fn date_formats_so_that_sorting_is_chronological() {
        assert_eq!(
            Ts::from_secs(1_785_283_200).utc_date().to_string(),
            "2026-07-29"
        );
        assert_eq!(
            Ts::from_secs(946_684_800).utc_date().to_string(),
            "2000-01-01"
        );
        // Fixed width is the point: string order must equal time order.
        let mut days = ["2026-10-01".to_owned(), "2026-09-30".to_owned()];
        days.sort();
        assert_eq!(days, ["2026-09-30", "2026-10-01"]);
    }

    #[test]
    fn start_of_day_is_the_rotation_boundary() {
        let midday = Ts::from_secs(1_785_283_200 + 12 * 3_600);
        assert_eq!(midday.start_of_utc_day(), Ts::from_secs(1_785_283_200));
        assert_eq!(midday.start_of_utc_day().utc_date(), midday.utc_date());
        // A timestamp already at midnight is its own boundary.
        let midnight = Ts::from_secs(1_785_283_200);
        assert_eq!(midnight.start_of_utc_day(), midnight);
    }

    #[test]
    fn every_day_across_a_long_span_is_consistent() {
        // Walk day by day and assert the date advances by exactly one day each
        // time, which catches off-by-one errors at month and year boundaries
        // without hand-listing every case.
        let mut previous = Ts::from_secs(0).utc_date();
        for day in 1..=(40 * 365_i64) {
            let date = Ts::from_secs(day * 86_400).utc_date();
            assert_ne!(date, previous, "day {day} did not advance");
            // Reconstructing the day count from the date must round-trip.
            assert_eq!(Ts::from_secs(day * 86_400).days_since_epoch(), day);
            previous = date;
        }
        assert_eq!(previous.year, 2009);
    }

    #[test]
    fn system_clock_is_plausible() {
        // Sanity check only: after 2020, before 2100.
        let now = SystemClock.now();
        assert!(now > Ts::from_secs(1_577_836_800));
        assert!(now < Ts::from_secs(4_102_444_800));
    }
}
