//! Reconnect backoff with decorrelating jitter.
//!
//! Venue-agnostic: every venue disconnects, and every venue rate-limits
//! reconnection attempts, so this belongs next to the rest of the recorder
//! plumbing rather than in an adapter.

use core::fmt;
use core::time::Duration;

/// Shape of the retry curve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: u32,
}

impl Default for BackoffPolicy {
    /// 250 ms doubling to a 30 s ceiling.
    ///
    /// **250 ms initial** because the overwhelming majority of disconnects are
    /// transient -- a venue rolling a gateway, a momentary network blip -- and
    /// reconnecting fast means the recorded gap is milliseconds rather than
    /// seconds. Starting at 5 s would turn every routine blip into a visible
    /// hole in the data.
    ///
    /// **30 s ceiling** because a real outage lasts minutes, and retrying every
    /// 250 ms for ten minutes is 2400 pointless connections -- a reliable way to
    /// get an IP ban, which converts a venue problem into our problem. Capped at
    /// 30 s we make at most two attempts a minute per symbol and still resume
    /// within half a minute of the venue returning.
    ///
    /// It is deliberately not higher: every second of backoff is a second of
    /// recorded gap, so the ceiling trades data loss against politeness and
    /// 30 s is about where those balance.
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(250),
            max: Duration::from_secs(30),
            multiplier: 2,
        }
    }
}

/// A tiny xorshift generator, used only to decorrelate retry timing.
///
/// Deliberately not the `rand` crate. This needs no cryptographic quality and no
/// uniformity guarantees -- it needs to stop N connections retrying in lockstep.
/// Being a seeded, reproducible sequence is a positive feature here: the jitter
/// bounds are then testable exactly, rather than statistically.
#[derive(Debug, Clone, Copy)]
struct XorShift64(u64);

impl XorShift64 {
    const fn new(seed: u64) -> Self {
        // A zero state is a fixed point for xorshift, so it must never be used.
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    const fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Exponential backoff with equal jitter.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    policy: BackoffPolicy,
    attempt: u32,
    rng: XorShift64,
}

impl Backoff {
    /// Seed the jitter from a stable key, normally the symbol.
    ///
    /// Per-key seeding is the entire point. When a venue restarts, every
    /// connection drops in the same instant; with a shared or absent seed they
    /// would all wake up together, hammer the venue in unison, and very likely
    /// fail together again -- a self-inflicted thundering herd that also makes
    /// the recorded gaps identical across symbols, hiding which connection was
    /// actually unhealthy.
    #[must_use]
    pub fn for_key(policy: BackoffPolicy, key: &str) -> Self {
        Self {
            policy,
            attempt: 0,
            rng: XorShift64::new(fnv1a(key.as_bytes())),
        }
    }

    /// Delay before the next attempt, and advance the curve.
    ///
    /// Uses *equal* jitter -- half the nominal delay plus a random amount up to
    /// the other half -- rather than full jitter over `[0, base]`. Full jitter
    /// decorrelates slightly better but can keep drawing near-zero delays, which
    /// defeats the point of backing off at all during a sustained outage. Equal
    /// jitter keeps a guaranteed floor while still spreading the herd.
    ///
    /// All integer arithmetic. The workspace denies floating point for money, and
    /// the same discipline pays off here: no rounding drift, and the bounds in
    /// the tests are exact.
    pub fn next_delay(&mut self) -> Duration {
        let base = self.base_millis();
        self.attempt = self.attempt.saturating_add(1);

        let half = base / 2;
        let jitter = if half == 0 {
            0
        } else {
            self.rng.next_u64() % (half + 1)
        };
        Duration::from_millis(half + jitter)
    }

    /// Nominal (un-jittered) delay for the current attempt.
    fn base_millis(&self) -> u64 {
        let max = millis(self.policy.max);
        let mut base = millis(self.policy.initial);
        if base > max {
            return max;
        }
        // Iterated saturating multiply rather than `pow`, so a large attempt
        // count clamps instead of overflowing.
        let mut n = 0;
        while n < self.attempt {
            base = base.saturating_mul(u64::from(self.policy.multiplier));
            if base >= max {
                return max;
            }
            n += 1;
        }
        base
    }

    /// Call after a successful connection, so the next failure starts short again.
    ///
    /// Without this, a connection that drops once an hour would eventually be
    /// waiting the full ceiling before reconnecting, even though every one of its
    /// disconnects was transient.
    pub const fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Consecutive failures so far.
    #[must_use]
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }
}

impl fmt::Display for Backoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "attempt {} (base {} ms)",
            self.attempt,
            self.base_millis()
        )
    }
}

fn millis(d: Duration) -> u64 {
    // `as_millis` is u128; anything past u64 milliseconds is 584 million years,
    // so the clamp is unreachable -- but it is a clamp rather than a cast, because
    // a silent `as` truncation is the pattern this codebase avoids everywhere else.
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// FNV-1a, for turning a symbol into a seed. Not a checksum and not persisted.
const fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        i += 1;
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> BackoffPolicy {
        BackoffPolicy {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(3_200),
            multiplier: 2,
        }
    }

    #[test]
    fn the_curve_doubles_and_then_holds_at_the_ceiling() {
        let mut b = Backoff::for_key(policy(), "BTCUSDT");
        // Nominal bases: 100, 200, 400, 800, 1600, 3200, 3200, 3200...
        let expected_bases = [100_u64, 200, 400, 800, 1_600, 3_200, 3_200, 3_200];
        for (attempt, base) in expected_bases.iter().enumerate() {
            assert_eq!(b.attempt(), u32::try_from(attempt).unwrap());
            let delay = b.next_delay();
            let ms = u64::try_from(delay.as_millis()).unwrap();
            assert!(
                ms >= base / 2 && ms <= *base,
                "attempt {attempt}: {ms} ms outside [{}, {base}]",
                base / 2
            );
        }
    }

    #[test]
    fn a_huge_attempt_count_clamps_rather_than_overflowing() {
        let mut b = Backoff::for_key(policy(), "BTCUSDT");
        for _ in 0..200 {
            let _ = b.next_delay();
        }
        let ms = u64::try_from(b.next_delay().as_millis()).unwrap();
        assert!((1_600..=3_200).contains(&ms), "{ms} ms escaped the ceiling");
    }

    #[test]
    fn success_resets_the_curve() {
        // Otherwise a connection that drops hourly would end up waiting the full
        // ceiling for a disconnect that was always transient.
        let mut b = Backoff::for_key(policy(), "BTCUSDT");
        for _ in 0..6 {
            let _ = b.next_delay();
        }
        assert_eq!(b.attempt(), 6);
        b.reset();
        assert_eq!(b.attempt(), 0);
        let ms = u64::try_from(b.next_delay().as_millis()).unwrap();
        assert!((50..=100).contains(&ms), "{ms} ms is not back to the start");
    }

    #[test]
    fn jitter_never_leaves_the_delay_at_zero() {
        // Full jitter over [0, base] could keep drawing near-zero delays and
        // defeat the backoff entirely; equal jitter guarantees a floor.
        let mut b = Backoff::for_key(policy(), "BTCUSDT");
        for _ in 0..500 {
            assert!(b.next_delay() >= Duration::from_millis(50));
        }
    }

    #[test]
    fn different_symbols_do_not_retry_in_lockstep() {
        // The property that stops a venue restart turning into a self-inflicted
        // thundering herd.
        let keys = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"];
        let sequences: Vec<Vec<u128>> = keys
            .iter()
            .map(|k| {
                let mut b = Backoff::for_key(policy(), k);
                (0..6).map(|_| b.next_delay().as_millis()).collect()
            })
            .collect();

        for (i, a) in sequences.iter().enumerate() {
            for b in sequences.iter().skip(i + 1) {
                assert_ne!(a, b, "two symbols produced identical retry timing");
            }
        }
    }

    #[test]
    fn the_same_symbol_is_reproducible() {
        // Seeded rather than randomly sourced, so a retry storm can be replayed.
        let run = || {
            let mut b = Backoff::for_key(policy(), "BTCUSDT");
            (0..8).map(|_| b.next_delay()).collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn the_default_policy_is_the_one_documented() {
        let d = BackoffPolicy::default();
        assert_eq!(d.initial, Duration::from_millis(250));
        assert_eq!(d.max, Duration::from_secs(30));
        assert_eq!(d.multiplier, 2);

        // Reaching the ceiling from 250 ms by doubling takes 7 attempts, i.e.
        // under a minute of cumulative delay. Slower than that would mean a
        // transient blip costing a long gap.
        let mut b = Backoff::for_key(d, "BTCUSDT");
        let total: Duration = (0..7).map(|_| b.next_delay()).sum();
        assert!(
            total < Duration::from_secs(60),
            "{total:?} to reach the cap"
        );
    }
}
