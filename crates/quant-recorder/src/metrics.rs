//! What the recorder can say about itself while it is running.
//!
//! # Why this is a milestone criterion and not polish
//!
//! `docs/data-contract.md` §7 puts it plainly: *"if we cannot see queue depth we
//! cannot tell the difference between a quiet market and a stalled consumer."*
//! Both look identical from outside -- no output, no errors, a process sitting
//! there. One is a Sunday morning and the other is losing data. Over a seven-day
//! unattended run that distinction is the difference between a capture and a
//! rumour.
//!
//! The other four are the same kind of question. Messages and bytes per second say
//! whether the venue is still talking to us. Venue latency percentiles say whether
//! the path between us and it has degraded, which is invisible in a capture that
//! is otherwise complete. Gap count by cause says *why* we were blind, and a daily
//! `Disconnect` is Binance behaving as documented while a daily `LocalOverflow` is
//! a machine that needs replacing.
//!
//! # Why counters are atomic
//!
//! Not for correctness under contention -- [`Ingress`](crate::Ingress) is the only
//! writer of most of these. It is because they have to be *read* by something
//! else: a reporter that runs while the connection loop is borrowed forever by its
//! own future. A relaxed increment costs about twenty cycles, which at the tens of
//! thousands of messages a second this is built for is unmeasurable against the
//! zstd compression happening on the other side of the channel.
//!
//! Queue depth genuinely is cross-thread: the producer increments it on the async
//! side and the writer thread decrements it. That one has to be shared.
//!
//! # Why there is no exporter here
//!
//! [`Metrics::sample`] gives a consistent-enough snapshot and [`MetricsReporter`]
//! turns two samples into rates. What happens to that -- a log line, Prometheus,
//! OpenTelemetry -- is a separate decision, and it belongs to M7 where the rest of
//! the observability story lives. Wiring an exporter now would fix that choice
//! before there is anything to observe with it.
//!
//! Nothing in this module is async or spawns anything, which is what keeps
//! `quant-recorder` free of a runtime; see the crate docs. The caller decides when
//! to sample.

use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use core::time::Duration;
use std::sync::Arc;

use quant_core::event::GapCause;

/// Sub-buckets per power of two in the latency histogram.
///
/// Sets the accuracy/size trade directly: 16 sub-buckets bounds the relative
/// error of any reported value at about 6%, which is far finer than the question
/// being asked ("did p99 go from 40ms to 300ms?"), and costs 8 KB per histogram.
const SUB_BITS: u32 = 4;
const SUB: u64 = 1 << SUB_BITS;

/// Enough for values up to `u64::MAX` microseconds, which is longer than the
/// universe has existed. Sized once so the array is a fixed cost.
const BUCKETS: usize = 1024;

/// Latency percentiles over some window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LatencySummary {
    pub count: u64,
    pub p50_micros: u64,
    pub p90_micros: u64,
    pub p99_micros: u64,
    pub max_micros: u64,
    /// Samples where the venue's timestamp was *ahead* of ours.
    ///
    /// Counted apart rather than clamped to zero, because §2 of the data contract
    /// is explicit that a negative latency means clock skew and *"is an alert, not
    /// something to clamp"*. Folding these into the histogram would turn a broken
    /// clock into an implausibly good latency figure.
    pub clock_skew: u64,
}

/// A log-bucketed histogram of latencies, in microseconds.
///
/// # Why not retain samples, and why not a dependency
///
/// Exact percentiles need every sample, which over seven days is hundreds of
/// millions of values -- so a histogram is not an approximation we settle for, it
/// is the only shape that fits. `hdrhistogram` would do this well; it is
/// hand-rolled because the layout is forty lines, the error bound is pinned by a
/// test, and a metric that will eventually drive an alert is worth understanding
/// exactly rather than trusting.
///
/// Buckets are `value >> k` for a k that grows with the magnitude, so resolution
/// is fine where latencies actually live (single-digit milliseconds) and coarse
/// where nobody cares (minutes).
#[derive(Debug)]
struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    count: AtomicU64,
    max: AtomicU64,
    clock_skew: AtomicU64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: core::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            max: AtomicU64::new(0),
            clock_skew: AtomicU64::new(0),
        }
    }

    fn record(&self, micros: u64) {
        self.buckets[bucket_of(micros)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.max.fetch_max(micros, Ordering::Relaxed);
    }

    fn record_skew(&self) {
        self.clock_skew.fetch_add(1, Ordering::Relaxed);
    }

    /// Summarize, optionally zeroing as it goes.
    ///
    /// Not atomic as a whole: a sample recorded partway through the walk may land
    /// in a bucket already read. That is accepted, and it is why this is a metric
    /// rather than a ledger -- the alternative is a lock on the path that records
    /// every message, to make a number that is looked at once a minute exact to
    /// one sample.
    fn summarize(&self, reset: bool) -> LatencySummary {
        let mut counts = [0_u64; BUCKETS];
        let mut total = 0_u64;
        for (i, bucket) in self.buckets.iter().enumerate() {
            counts[i] = if reset {
                bucket.swap(0, Ordering::Relaxed)
            } else {
                bucket.load(Ordering::Relaxed)
            };
            total += counts[i];
        }

        let take = |n: &AtomicU64| {
            if reset {
                n.swap(0, Ordering::Relaxed)
            } else {
                n.load(Ordering::Relaxed)
            }
        };
        let max_micros = take(&self.max);
        let clock_skew = take(&self.clock_skew);
        let _ = take(&self.count);

        LatencySummary {
            count: total,
            p50_micros: percentile(&counts, total, 50),
            p90_micros: percentile(&counts, total, 90),
            p99_micros: percentile(&counts, total, 99),
            max_micros,
            clock_skew,
        }
    }
}

/// Which bucket a value falls in.
///
/// Values below [`SUB`] get their own bucket, so small latencies are exact;
/// above that, each power of two is split into [`SUB`] even slices.
fn bucket_of(value: u64) -> usize {
    let index = if value < SUB {
        value
    } else {
        let exponent = u64::from(value.ilog2());
        let shift = exponent - u64::from(SUB_BITS);
        let mantissa = (value >> shift) & (SUB - 1);
        (exponent - u64::from(SUB_BITS) + 1) * SUB + mantissa
    };
    // In range for every u64: the largest exponent is 63, giving
    // (63 - 4 + 1) * 16 + 15 = 975, comfortably inside BUCKETS.
    usize::try_from(index).expect("bucket index is at most 975")
}

/// The largest value a bucket can hold.
///
/// The *upper* bound rather than a midpoint, so a reported percentile never
/// understates the latency actually observed. For a number that will drive an
/// alert, erring towards "worse than reality" is the right direction.
fn bucket_upper_bound(index: usize) -> u64 {
    let index = index as u64;
    if index < SUB {
        return index;
    }
    let octave = index / SUB;
    let mantissa = index % SUB;
    let shift = octave - 1;
    ((SUB + mantissa) << shift) + (1 << shift) - 1
}

fn percentile(counts: &[u64; BUCKETS], total: u64, pct: u64) -> u64 {
    if total == 0 {
        return 0;
    }
    // Ceiling division, so p99 of a single sample is that sample rather than zero.
    let target = (total * pct).div_ceil(100);
    let mut cumulative = 0_u64;
    for (i, count) in counts.iter().enumerate() {
        cumulative += count;
        if cumulative >= target {
            return bucket_upper_bound(i);
        }
    }
    bucket_upper_bound(BUCKETS - 1)
}

/// Everything the recorder counts about itself.
///
/// Shared behind an [`Arc`]: the connection task increments most of it, the writer
/// thread decrements queue depth, and a reporter reads all of it.
#[derive(Debug)]
pub struct Metrics {
    messages: AtomicU64,
    bytes: AtomicU64,
    enqueued: AtomicU64,
    dropped: AtomicU64,
    gaps_recorded: AtomicU64,
    gaps_abandoned: AtomicU64,
    gaps_by_cause: [AtomicU64; GapCause::ALL.len()],
    snapshots: AtomicU64,
    snapshot_bytes: AtomicU64,
    snapshots_dropped: AtomicU64,
    snapshot_failures: AtomicU64,
    queue_depth: AtomicUsize,
    queue_high_water: AtomicUsize,
    queue_capacity: usize,
    /// Reset every time it is reported, so each line describes its own interval.
    /// A degradation that starts on day five is invisible in a lifetime figure.
    window: Histogram,
    /// Never reset: the number the acceptance run is judged on.
    lifetime: Histogram,
}

impl Metrics {
    #[must_use]
    pub fn new(queue_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            messages: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            gaps_recorded: AtomicU64::new(0),
            gaps_abandoned: AtomicU64::new(0),
            gaps_by_cause: core::array::from_fn(|_| AtomicU64::new(0)),
            snapshots: AtomicU64::new(0),
            snapshot_bytes: AtomicU64::new(0),
            snapshots_dropped: AtomicU64::new(0),
            snapshot_failures: AtomicU64::new(0),
            queue_depth: AtomicUsize::new(0),
            queue_high_water: AtomicUsize::new(0),
            queue_capacity,
            window: Histogram::new(),
            lifetime: Histogram::new(),
        })
    }

    pub fn record_message(&self, bytes: u64) {
        self.messages.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_enqueued(&self) {
        self.enqueued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_gap(&self, cause: GapCause) {
        self.gaps_recorded.fetch_add(1, Ordering::Relaxed);
        self.gaps_by_cause[cause.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_gap_abandoned(&self) {
        self.gaps_abandoned.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_book_snapshot(&self, bytes: u64) {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        self.snapshot_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_snapshot_dropped(&self) {
        self.snapshots_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_snapshot_failure(&self) {
        self.snapshot_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Observe one venue latency, as `local_recv_ts - exchange_ts` in nanoseconds.
    ///
    /// Signed, because the data contract says so and because a negative value is
    /// real information about the host clock rather than a number to floor at zero.
    pub fn record_latency(&self, nanos: i64) {
        if nanos < 0 {
            self.window.record_skew();
            self.lifetime.record_skew();
            return;
        }
        // Microseconds: nanosecond buckets would waste resolution on a quantity
        // the venue only reports to the millisecond anyway.
        let micros = nanos.unsigned_abs() / 1_000;
        self.window.record(micros);
        self.lifetime.record(micros);
    }

    /// A record is about to enter the channel.
    ///
    /// **Called before the send, not after.** The moment a record is handed to the
    /// channel the consumer can already be taking it out, so counting it
    /// afterwards lets the decrement beat the increment -- and an unsigned counter
    /// that goes below zero wraps to `usize::MAX`. See
    /// [`Metrics::queue_push_failed`] for the other half.
    pub fn queue_pushed(&self) {
        let depth = self
            .queue_depth
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        self.queue_high_water.fetch_max(depth, Ordering::Relaxed);
    }

    /// The send that [`Metrics::queue_pushed`] anticipated did not happen.
    pub fn queue_push_failed(&self) {
        self.decrement_depth();
    }

    /// A record left the channel.
    pub fn queue_popped(&self) {
        self.decrement_depth();
    }

    /// Saturating, deliberately.
    ///
    /// Ordering makes an underflow impossible today, but a metric must never be
    /// able to take the recorder down: if this pairing is ever got wrong again, the
    /// cost should be a wrong number in a log line, not a panicking capture.
    fn decrement_depth(&self) {
        let _ = self
            .queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                Some(depth.saturating_sub(1))
            });
    }

    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// Read every counter, leaving the window histogram in place.
    #[must_use]
    pub fn sample(&self) -> MetricsSample {
        self.read(false)
    }

    /// Read every counter and reset the window histogram, so the next reading
    /// describes only the interval that follows.
    #[must_use]
    pub fn sample_and_reset_window(&self) -> MetricsSample {
        self.read(true)
    }

    fn read(&self, reset_window: bool) -> MetricsSample {
        let load = |n: &AtomicU64| n.load(Ordering::Relaxed);
        MetricsSample {
            messages: load(&self.messages),
            bytes: load(&self.bytes),
            enqueued: load(&self.enqueued),
            dropped: load(&self.dropped),
            gaps_recorded: load(&self.gaps_recorded),
            gaps_abandoned: load(&self.gaps_abandoned),
            gaps_by_cause: core::array::from_fn(|i| load(&self.gaps_by_cause[i])),
            snapshots: load(&self.snapshots),
            snapshot_bytes: load(&self.snapshot_bytes),
            snapshots_dropped: load(&self.snapshots_dropped),
            snapshot_failures: load(&self.snapshot_failures),
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            queue_high_water: self.queue_high_water.load(Ordering::Relaxed),
            queue_capacity: self.queue_capacity,
            window_latency: self.window.summarize(reset_window),
            lifetime_latency: self.lifetime.summarize(false),
        }
    }
}

/// A reading of every counter at one moment.
///
/// Plain values, so it can be compared against an earlier reading to produce
/// rates. Not a consistent cross-counter snapshot -- see [`Histogram::summarize`]
/// -- and it does not need to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSample {
    pub messages: u64,
    pub bytes: u64,
    pub enqueued: u64,
    pub dropped: u64,
    pub gaps_recorded: u64,
    pub gaps_abandoned: u64,
    pub gaps_by_cause: [u64; GapCause::ALL.len()],
    pub snapshots: u64,
    pub snapshot_bytes: u64,
    pub snapshots_dropped: u64,
    pub snapshot_failures: u64,
    pub queue_depth: usize,
    pub queue_high_water: usize,
    pub queue_capacity: usize,
    pub window_latency: LatencySummary,
    pub lifetime_latency: LatencySummary,
}

impl MetricsSample {
    /// How full the channel is, in percent.
    ///
    /// The number §7 actually asks for. A steady zero means the writer keeps up; a
    /// number that climbs means it does not, and a high-water mark near capacity
    /// means we came close to dropping without doing so.
    #[must_use]
    pub const fn queue_percent(&self) -> usize {
        if self.queue_capacity == 0 {
            return 0;
        }
        self.queue_depth * 100 / self.queue_capacity
    }
}

/// One interval's worth of rates and levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsReport {
    pub elapsed: Duration,
    pub messages_per_sec: u64,
    pub bytes_per_sec: u64,
    /// Messages dropped *in this interval*. The one number that should always be
    /// zero, which is why it is reported as a delta rather than a total: a total
    /// that stopped growing looks the same as one that never grew.
    pub dropped: u64,
    pub gaps: [u64; GapCause::ALL.len()],
    pub queue_depth: usize,
    pub queue_high_water: usize,
    pub queue_capacity: usize,
    pub latency: LatencySummary,
    pub totals: MetricsSample,
}

/// Turns successive samples into rates.
///
/// Deliberately not a task, a thread or a timer. Rate computation is the only
/// logic here, and keeping it as a plain function of two samples makes it
/// testable without waiting for wall-clock seconds to pass -- while leaving the
/// *when* to the caller, which is the only part that needs a runtime.
#[derive(Debug)]
pub struct MetricsReporter {
    previous: MetricsSample,
    metrics: Arc<Metrics>,
}

impl MetricsReporter {
    #[must_use]
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            previous: metrics.sample(),
            metrics,
        }
    }

    /// Take a reading covering `elapsed` since the previous one.
    ///
    /// `elapsed` is passed in rather than measured here so that a test can state
    /// it exactly. In the recorder it comes from the interval timer that decided
    /// to call this.
    pub fn report(&mut self, elapsed: Duration) -> MetricsReport {
        let now = self.metrics.sample_and_reset_window();
        let previous = core::mem::replace(&mut self.previous, now);

        let millis = u64::try_from(elapsed.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        // Integer throughout: `clippy::float_arithmetic` is denied workspace-wide,
        // and a rate is a ratio of two counts anyway.
        let per_sec = |delta: u64| delta.saturating_mul(1_000) / millis;

        MetricsReport {
            elapsed,
            messages_per_sec: per_sec(now.messages.saturating_sub(previous.messages)),
            bytes_per_sec: per_sec(now.bytes.saturating_sub(previous.bytes)),
            dropped: now.dropped.saturating_sub(previous.dropped),
            gaps: core::array::from_fn(|i| {
                now.gaps_by_cause[i].saturating_sub(previous.gaps_by_cause[i])
            }),
            queue_depth: now.queue_depth,
            queue_high_water: now.queue_high_water,
            queue_capacity: now.queue_capacity,
            latency: now.window_latency,
            totals: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_value_always_falls_inside_the_bucket_that_claims_it() {
        // The property every percentile depends on: the bound reported for a
        // bucket must be at least every value that lands there, or a percentile
        // could understate a latency.
        for value in
            (0..2_000_u64).chain([10_000, 99_999, 1_000_000, 60_000_000, u64::from(u32::MAX)])
        {
            let bucket = bucket_of(value);
            assert!(bucket < BUCKETS, "value {value} escaped the array");
            assert!(
                bucket_upper_bound(bucket) >= value,
                "value {value} in bucket {bucket} bounded at {}",
                bucket_upper_bound(bucket)
            );
        }
    }

    #[test]
    fn the_reported_error_bound_is_what_the_docs_claim() {
        // 16 sub-buckets per octave should keep any reported value within about 7%
        // of the truth. If this fails, the module docs are lying about accuracy.
        for value in (16..100_000_u64).step_by(37) {
            let bound = bucket_upper_bound(bucket_of(value));
            let error_percent = (bound - value) * 100 / value;
            assert!(
                error_percent <= 7,
                "value {value} reported as {bound}: {error_percent}% error"
            );
        }
    }

    #[test]
    fn small_latencies_are_exact() {
        // Sub-millisecond values are where a colocated future would live, and
        // rounding them into a shared bucket would make the metric useless there.
        for value in 0..SUB {
            assert_eq!(bucket_upper_bound(bucket_of(value)), value);
        }
    }

    #[test]
    fn percentiles_come_out_where_they_should() {
        let h = Histogram::new();
        // 100 samples: 1..=100 milliseconds.
        for ms in 1..=100_u64 {
            h.record(ms * 1_000);
        }
        let summary = h.summarize(false);
        assert_eq!(summary.count, 100);
        assert_eq!(summary.max_micros, 100_000);

        // Within the bucket error bound of 50ms, 90ms and 99ms.
        let close = |got: u64, want: u64| {
            let diff = got.abs_diff(want);
            assert!(diff * 100 / want <= 7, "got {got}, wanted about {want}");
        };
        close(summary.p50_micros, 50_000);
        close(summary.p90_micros, 90_000);
        close(summary.p99_micros, 99_000);
    }

    #[test]
    fn a_single_sample_is_its_own_p99() {
        // Ceiling division rather than floor: otherwise the p99 of one observation
        // is zero, which reads as "no latency" instead of "one sample".
        let h = Histogram::new();
        h.record(42_000);
        let summary = h.summarize(false);
        assert_eq!(summary.count, 1);
        assert!(summary.p99_micros >= 42_000);
    }

    #[test]
    fn an_empty_histogram_reports_zero_rather_than_panicking() {
        let summary = Histogram::new().summarize(true);
        assert_eq!(summary, LatencySummary::default());
    }

    #[test]
    fn a_clock_running_backwards_is_counted_not_clamped() {
        // Data contract §2: a negative latency means clock skew and is an alert.
        // Folding it into the histogram would report an impossibly good latency.
        let metrics = Metrics::new(16);
        metrics.record_latency(5_000_000);
        metrics.record_latency(-2_000_000);
        let sample = metrics.sample();
        assert_eq!(sample.lifetime_latency.count, 1, "only the real sample");
        assert_eq!(sample.lifetime_latency.clock_skew, 1);
        assert_eq!(sample.lifetime_latency.max_micros, 5_000);
    }

    #[test]
    fn the_window_resets_but_the_lifetime_does_not() {
        // Each reported line must describe its own interval -- a degradation
        // starting on day five is invisible in a seven-day average -- while the
        // acceptance run still needs one number for the whole capture.
        let metrics = Metrics::new(16);
        for _ in 0..10 {
            metrics.record_latency(1_000_000);
        }
        let first = metrics.sample_and_reset_window();
        assert_eq!(first.window_latency.count, 10);
        assert_eq!(first.lifetime_latency.count, 10);

        metrics.record_latency(2_000_000);
        let second = metrics.sample_and_reset_window();
        assert_eq!(second.window_latency.count, 1, "window covers the interval");
        assert_eq!(second.lifetime_latency.count, 11, "lifetime covers the run");
    }

    #[test]
    fn rates_are_deltas_over_elapsed_time() {
        let metrics = Metrics::new(4096);
        let mut reporter = MetricsReporter::new(Arc::clone(&metrics));

        for _ in 0..600 {
            metrics.record_message(100);
        }
        let report = reporter.report(Duration::from_secs(60));
        assert_eq!(report.messages_per_sec, 10);
        assert_eq!(report.bytes_per_sec, 1_000);

        // A second interval reports only what happened in it, not the total.
        for _ in 0..120 {
            metrics.record_message(50);
        }
        let report = reporter.report(Duration::from_secs(60));
        assert_eq!(report.messages_per_sec, 2);
        assert_eq!(report.bytes_per_sec, 100);
        assert_eq!(report.totals.messages, 720, "totals still accumulate");
    }

    #[test]
    fn a_zero_length_interval_does_not_divide_by_zero() {
        let metrics = Metrics::new(16);
        let mut reporter = MetricsReporter::new(Arc::clone(&metrics));
        metrics.record_message(1);
        let report = reporter.report(Duration::ZERO);
        assert_eq!(report.messages_per_sec, 1_000, "treated as one millisecond");
    }

    #[test]
    fn queue_depth_tracks_both_ends_and_remembers_its_worst() {
        // The metric §7 singles out: without it a stalled consumer and a quiet
        // market are indistinguishable.
        let metrics = Metrics::new(100);
        for _ in 0..30 {
            metrics.queue_pushed();
        }
        for _ in 0..25 {
            metrics.queue_popped();
        }
        let sample = metrics.sample();
        assert_eq!(sample.queue_depth, 5);
        assert_eq!(sample.queue_percent(), 5);
        assert_eq!(
            sample.queue_high_water, 30,
            "the peak is what says how close we came to dropping"
        );
    }

    #[test]
    fn a_pop_that_beats_its_push_cannot_wrap_the_counter() {
        // The bug this pins, found by the multi-threaded writer test: counting a
        // record *after* handing it to the channel lets the consumer decrement
        // first, and an unsigned depth that goes below zero becomes usize::MAX --
        // which then panicked on the next increment and took the recorder with it.
        //
        // The ordering fix makes it impossible; this makes the arithmetic harmless
        // if the ordering is ever got wrong again, because a metric must not be
        // able to kill a capture.
        let metrics = Metrics::new(16);
        for _ in 0..5 {
            metrics.queue_popped();
        }
        assert_eq!(metrics.queue_depth(), 0, "never negative, never wrapped");

        metrics.queue_pushed();
        assert_eq!(metrics.queue_depth(), 1);
        assert_eq!(
            metrics.sample().queue_high_water,
            1,
            "and the peak is not usize::MAX either"
        );
    }

    #[test]
    fn a_send_that_fails_does_not_leave_a_phantom_in_the_queue() {
        let metrics = Metrics::new(16);
        metrics.queue_pushed();
        metrics.queue_push_failed();
        assert_eq!(metrics.queue_depth(), 0);
    }

    #[test]
    fn gaps_are_counted_per_cause() {
        // A daily Disconnect is Binance behaving as documented; a daily
        // LocalOverflow is a machine that needs replacing. One total cannot say
        // which happened.
        let metrics = Metrics::new(16);
        metrics.record_gap(GapCause::Disconnect);
        metrics.record_gap(GapCause::Disconnect);
        metrics.record_gap(GapCause::LocalOverflow);
        let sample = metrics.sample();
        assert_eq!(sample.gaps_recorded, 3);
        assert_eq!(sample.gaps_by_cause[GapCause::Disconnect.index()], 2);
        assert_eq!(sample.gaps_by_cause[GapCause::LocalOverflow.index()], 1);
        assert_eq!(sample.gaps_by_cause[GapCause::SequenceGap.index()], 0);
    }
}
