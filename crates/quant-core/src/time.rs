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

    /// The inverse: days since 1970-01-01 for a calendar date.
    ///
    /// Howard Hinnant's `days_from_civil`, the counterpart to the above and
    /// exact over the same range. It existed nowhere until M7 needed to *parse*
    /// a time rather than only render one -- and the absence is the interesting
    /// part, because three binaries were about to grow their own.
    #[must_use]
    pub fn days_since_epoch(&self) -> i64 {
        let y = i64::from(self.year) - i64::from(self.month <= 2);
        let era = y.div_euclid(400);
        let yoe = y - era * 400; // [0, 399]
        let m = i64::from(self.month);
        let d = i64::from(self.day);
        let mp = if m > 2 { m - 3 } else { m + 9 }; // shifted month, [0, 11]
        let doy = (153 * mp + 2) / 5 + d - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146_097 + doe - 719_468
    }

    /// Is this a date the calendar actually has?
    ///
    /// Checked rather than assumed, because a parser must reject `2026-02-30`
    /// loudly instead of silently rolling it into March. Invariant 5 applied to
    /// time: a malformed input means our model of the world is wrong.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        if self.month < 1 || self.month > 12 || self.day < 1 {
            return false;
        }
        let leap = (self.year % 4 == 0 && self.year % 100 != 0) || self.year % 400 == 0;
        let last = match self.month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if leap => 29,
            2 => 28,
            _ => return false,
        };
        self.day <= last
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

/// Split an RFC 3339 string into its datetime part and its offset in nanoseconds.
///
/// `None` when there is no offset at all, which the caller reports rather than
/// guessing at -- see [`Ts::parse_rfc3339`].
fn split_offset(text: &str) -> Option<(&str, i64)> {
    if let Some(rest) = text.strip_suffix(['Z', 'z']) {
        return Some((rest, 0));
    }
    // Scan from the end so the date's own hyphens cannot be mistaken for the
    // sign of an offset.
    let sign_at = text.rfind(['+', '-'])?;
    let (datetime, offset) = text.split_at(sign_at);
    let negative = offset.starts_with('-');
    let (hours, minutes) = offset[1..].split_once(':')?;
    let hours: i64 = hours.parse().ok()?;
    let minutes: i64 = minutes.parse().ok()?;
    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return None;
    }
    let magnitude = (hours * 3_600 + minutes * 60) * 1_000_000_000;
    Some((datetime, if negative { -magnitude } else { magnitude }))
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

    /// Render as RFC 3339 with nanosecond precision, always UTC.
    ///
    /// `2026-09-18T14:46:54.123456789Z`. Always `Z` and always nine fractional
    /// digits: a fixed-width rendering sorts lexicographically in
    /// chronological order, which is the same property `UtcDate`'s `Display`
    /// exists for, and a reader never has to wonder whether a shorter string
    /// meant less precision or less time.
    #[must_use]
    pub fn to_rfc3339(self) -> String {
        let date = self.utc_date();
        // `rem_euclid` rather than subtracting `start_of_utc_day`, which
        // multiplies days back up and overflows within a day of `i64::MIN`. A
        // remainder cannot: it is always in `[0, NANOS_PER_DAY)`, and euclidean
        // so a pre-epoch instant counts forward from its own midnight rather
        // than backward from the next one.
        let into_day = self.0.rem_euclid(NANOS_PER_DAY);
        let secs = into_day / 1_000_000_000;
        let nanos = into_day % 1_000_000_000;
        format!(
            "{date}T{:02}:{:02}:{:02}.{nanos:09}Z",
            secs / 3_600,
            (secs / 60) % 60,
            secs % 60,
        )
    }

    /// Parse an RFC 3339 timestamp into a `Ts`.
    ///
    /// Accepts `2026-09-18T14:46:54Z`, an optional fractional second of one to
    /// nine digits, and either `Z` or an explicit `+HH:MM` / `-HH:MM` offset.
    ///
    /// # An offset is required, deliberately
    ///
    /// A bare `2026-09-18T14:46:54` is rejected rather than assumed to be UTC.
    /// The operator asking "what was it doing at 03:14" is reading a wall clock
    /// in their own zone while every artifact in this project is stamped UTC,
    /// and silently choosing one of the two is how a query lands hours from
    /// where it was aimed -- while still returning a confident, wrong answer.
    /// Invariant 5: a parse failure means our model of the input is wrong, so it
    /// is loud.
    ///
    /// Offsets only, never named zones: `Asia/Kolkata` needs a tz database,
    /// which is a dependency this workspace does not have and which would make
    /// the answer depend on the host's data files. Stated as a limitation rather
    /// than discovered.
    ///
    /// # Errors
    ///
    /// Returns the reason the string is not a timestamp, for showing to whoever
    /// typed it.
    pub fn parse_rfc3339(text: &str) -> Result<Self, String> {
        let bad = |why: &str| Err(format!("{text:?} is not an RFC 3339 timestamp: {why}"));

        let Some((datetime, offset_nanos)) = split_offset(text) else {
            return bad(
                "it needs a UTC offset -- `Z`, or `+05:30`. A bare local time would be \
                 guessed at, and a query aimed hours from where it was meant still \
                 returns a confident answer",
            );
        };
        let Some((date_part, time_part)) = datetime.split_once(['T', 't', ' ']) else {
            return bad("expected `<date>T<time>`");
        };

        let date_fields: Vec<&str> = date_part.split('-').collect();
        let [year, month, day] = date_fields.as_slice() else {
            return bad("expected a date as `YYYY-MM-DD`");
        };
        let (Ok(year), Ok(month), Ok(day)) =
            (year.parse::<i32>(), month.parse::<u8>(), day.parse::<u8>())
        else {
            return bad("the date is not three numbers");
        };
        let date = UtcDate { year, month, day };
        if !date.is_valid() {
            return bad("no such date in the calendar");
        }

        let (clock, fraction) = match time_part.split_once('.') {
            Some((clock, fraction)) => (clock, Some(fraction)),
            None => (time_part, None),
        };
        let clock_fields: Vec<&str> = clock.split(':').collect();
        let [hour, minute, second] = clock_fields.as_slice() else {
            return bad("expected a time as `HH:MM:SS`");
        };
        let (Ok(hour), Ok(minute), Ok(second)) = (
            hour.parse::<i64>(),
            minute.parse::<i64>(),
            second.parse::<i64>(),
        ) else {
            return bad("the time is not three numbers");
        };
        // 60 is a leap second, which UTC has and Unix time does not represent.
        // Rejected rather than clamped: pretending it is :59 would place an
        // event a second from where the input asked for.
        if !(0..24).contains(&hour) || !(0..60).contains(&minute) || !(0..60).contains(&second) {
            return bad("the time is out of range");
        }

        let nanos_of_second = match fraction {
            None => 0,
            Some(digits) => {
                if digits.is_empty()
                    || digits.len() > 9
                    || !digits.bytes().all(|b| b.is_ascii_digit())
                {
                    return bad("the fractional second must be one to nine digits");
                }
                let scale = 10_i64.pow(9 - u32::try_from(digits.len()).expect("at most 9"));
                digits.parse::<i64>().expect("digits only") * scale
            }
        };

        // In `i128`, and range-checked only at the end. Midnight of a day near
        // the bottom of `Ts`'s range is itself below `i64::MIN` even when the
        // instant asked for is comfortably inside it, so checking each
        // intermediate would reject representable timestamps.
        let nanos = i128::from(date.days_since_epoch()) * i128::from(NANOS_PER_DAY)
            + i128::from((hour * 3_600 + minute * 60 + second) * 1_000_000_000)
            + i128::from(nanos_of_second)
            - i128::from(offset_nanos);
        match i64::try_from(nanos) {
            Ok(nanos) => Ok(Self(nanos)),
            Err(_) => bad("outside the range of a nanosecond timestamp"),
        }
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

#[cfg(test)]
mod rfc3339_tests {
    use super::{Ts, UtcDate};

    #[test]
    fn a_timestamp_round_trips_through_its_rendering() {
        // The property that matters: render then parse is the identity. Spot
        // values are a weak test of a calendar, so this walks a range that
        // crosses leap days, century boundaries and the epoch itself.
        for day in [
            -40_000, // 1860, well before the epoch
            -1,      // 1969-12-31, the case euclidean division exists for
            0,       // 1970-01-01
            11_016,  // 2000-02-29, a leap year that is also a century
            20_514,  // 2026-03-01, the day after a non-leap February
            20_714,  // inside the fortnight
            60_000,  // 2134
        ] {
            for into_day in [0_i64, 1, 86_399_999_999_999, 43_200_000_000_000] {
                let ts = Ts::from_nanos(day * 86_400 * 1_000_000_000 + into_day);
                let text = ts.to_rfc3339();
                assert_eq!(
                    Ts::parse_rfc3339(&text),
                    Ok(ts),
                    "{text} did not round-trip"
                );
            }
        }
    }

    #[test]
    fn the_representable_range_is_about_three_centuries_and_is_stated() {
        // `Ts` is i64 nanoseconds since the epoch, so it spans roughly
        // 1677-09-21 to 2262-04-11 -- not the whole calendar `UtcDate` can
        // describe. Found by a round-trip test that reached for year 1 and
        // overflowed its own arithmetic, which is the right way to find it.
        // Pinned here so the limit is a documented property rather than a
        // surprise in whatever first needs a date outside it.
        let far_past = Ts::from_nanos(i64::MIN + 1);
        let far_future = Ts::from_nanos(i64::MAX);
        assert_eq!(far_past.utc_date().year, 1677);
        assert_eq!(far_future.utc_date().year, 2262);

        // Note what this test found on the way: `start_of_utc_day` multiplies
        // whole days back up to nanoseconds and so overflows within one day of
        // `i64::MIN`. Unreachable in practice -- it exists for the recorder's
        // day rolling, which runs on the system clock -- but it is why
        // `to_rfc3339` takes a remainder instead of calling it. Left as it is
        // rather than changed on a path the running capture depends on, and
        // written down here instead of forgotten.

        // And both ends still round-trip, which is what makes the range a range
        // rather than a region where the rendering quietly stops working.
        for ts in [far_past, far_future] {
            let text = ts.to_rfc3339();
            assert_eq!(Ts::parse_rfc3339(&text), Ok(ts), "{text}");
        }
    }

    #[test]
    fn the_calendar_inverse_is_an_inverse() {
        // `from_days_since_epoch` had no counterpart until now, so the pair is
        // pinned against each other over four centuries rather than at a few
        // points -- 146,097 days is one full Gregorian era, after which the
        // pattern repeats.
        for days in (-80_000..80_000).step_by(7) {
            let date = UtcDate::from_days_since_epoch(days);
            assert_eq!(date.days_since_epoch(), days, "{date} at {days}");
            assert!(date.is_valid(), "{date} was produced but is not valid");
        }
    }

    #[test]
    fn an_offset_is_applied_in_the_right_direction() {
        // The sign is the thing most likely to be wrong and least likely to be
        // noticed: an hour out still looks like a plausible answer. 05:30 ahead
        // of UTC means the UTC instant is *earlier*.
        let utc = Ts::parse_rfc3339("2026-09-18T09:16:54Z").expect("valid");
        let ist = Ts::parse_rfc3339("2026-09-18T14:46:54+05:30").expect("valid");
        assert_eq!(ist, utc, "+05:30 must subtract, not add");

        let behind = Ts::parse_rfc3339("2026-09-18T04:16:54-05:00").expect("valid");
        assert_eq!(behind, utc, "-05:00 must add");
    }

    #[test]
    fn a_time_with_no_offset_is_refused_rather_than_assumed() {
        // The operator reads a wall clock in their own zone; every artifact here
        // is UTC. Guessing either way returns a confident answer hours from
        // where the question was aimed.
        let e = Ts::parse_rfc3339("2026-09-18T14:46:54").expect_err("must refuse");
        assert!(e.contains("UTC offset"), "{e}");
    }

    #[test]
    fn a_date_the_calendar_does_not_have_is_refused() {
        for bad in [
            "2026-02-30T00:00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-00-10T00:00:00Z",
            "2026-09-31T00:00:00Z",
        ] {
            assert!(Ts::parse_rfc3339(bad).is_err(), "{bad} was accepted");
        }
        // And the one that is real: 2024 was a leap year.
        assert!(Ts::parse_rfc3339("2024-02-29T00:00:00Z").is_ok());
        assert!(Ts::parse_rfc3339("2026-02-29T00:00:00Z").is_err());
    }

    #[test]
    fn a_leap_second_is_refused_rather_than_clamped() {
        // UTC has :60; Unix time cannot represent it. Clamping to :59 would
        // place the query a second from where it was asked for, silently.
        let e = Ts::parse_rfc3339("2026-06-30T23:59:60Z").expect_err("must refuse");
        assert!(e.contains("out of range"), "{e}");
    }

    #[test]
    fn fractional_seconds_scale_by_their_digit_count() {
        // `.5` is half a second, not five nanoseconds -- the mistake a naive
        // parse of the digits makes.
        let base = Ts::parse_rfc3339("2026-09-18T00:00:00Z").expect("valid");
        for (text, expect) in [
            (".5", 500_000_000),
            (".05", 50_000_000),
            (".123456789", 123_456_789),
            (".000000001", 1),
        ] {
            let ts = Ts::parse_rfc3339(&format!("2026-09-18T00:00:00{text}Z")).expect("valid");
            assert_eq!(ts.as_nanos() - base.as_nanos(), expect, "{text}");
        }
        assert!(Ts::parse_rfc3339("2026-09-18T00:00:00.Z").is_err());
        assert!(Ts::parse_rfc3339("2026-09-18T00:00:00.1234567890Z").is_err());
    }

    #[test]
    fn the_rendering_is_fixed_width_so_text_order_is_time_order() {
        // The same property `UtcDate`'s Display exists for, extended to an
        // instant: a sorted listing of these is chronological.
        let mut rendered: Vec<String> = [
            Ts::from_nanos(1),
            Ts::from_nanos(0),
            Ts::from_nanos(86_400_000_000_000),
            Ts::from_nanos(-1),
        ]
        .iter()
        .map(|t| t.to_rfc3339())
        .collect();
        let mut by_time = rendered.clone();
        rendered.sort();
        by_time.sort_by_key(|text| Ts::parse_rfc3339(text).expect("valid"));
        assert_eq!(rendered, by_time);
        assert!(rendered.iter().all(|r| r.len() == rendered[0].len()));
    }
}
