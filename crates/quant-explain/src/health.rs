//! How the process itself was doing, for the minute containing an instant.
//!
//! # This parses a format we do not own, and that is named debt
//!
//! The recorder emits its metrics through `tracing`'s default `fmt` layer, so
//! the only durable record of queue depth, drop counts and venue latency is a
//! *prose* log line. Nothing else has them: they are properties of one process
//! at one instant and appear nowhere in the capture, so unlike everything else
//! `explain` reports, they cannot be re-derived. If the log is deleted they are
//! gone.
//!
//! An emitter and a parser that must agree forever, coupled through a format
//! neither owns, with no test that they agree, is the pattern this project
//! refuses. It is accepted here for one reason: the alternative — switching the
//! emitter to `.json()` — would change the running binary, and the fortnight is
//! pinned. **The replacement is scheduled rather than hoped for:** switch the
//! emitter and delete this module in the same commit as the run log.
//!
//! Until then the parser is strict. An unrecognised line is reported as
//! unparseable rather than skipped, so the day the format changes is the day
//! this says so — instead of quietly reporting a minute as having no metrics.
//!
//! # The time bases, which are most of this module's value
//!
//! One line mixes three, unmarked:
//!
//! - `queue` is **instantaneous** — the depth at the moment the line was written.
//! - `queue_peak` is **lifetime** — the high-water mark since the process began,
//!   so a value frozen for six hours still reads like a live number.
//! - everything else — drops, gaps, and every latency percentile — is a **delta
//!   over the last 60 seconds**, reset each time the line is emitted.
//!
//! Reading `queue_peak=270` beside `queue=0` without knowing which is which is
//! how an operator concludes the wrong thing at 3am. So every figure is printed
//! with its base. That labelling costs nothing and is the part of this slice
//! worth keeping even after the emitter is fixed.

use std::path::{Path, PathBuf};

use quant_core::time::Ts;

/// The metrics line covering an instant, with each figure's time base.
#[derive(Debug, Clone)]
pub struct HealthAt {
    /// The log file the line came from.
    pub log: PathBuf,
    /// When the line was emitted — the end of the window it describes.
    pub emitted_at: Ts,
    /// Instantaneous: queue depth as the line was written.
    pub queue: u64,
    /// Lifetime: the high-water mark since the process started.
    pub queue_peak: u64,
    pub queue_capacity: u64,
    /// Window deltas over the preceding 60 seconds.
    pub dropped: u64,
    pub msgs_per_sec: u64,
    pub latency_p50_ms: i64,
    pub latency_p99_ms: i64,
    pub latency_max_ms: i64,
    pub clock_skew_ms: i64,
    pub gap_disconnect: u64,
    pub gap_overflow: u64,
    pub gap_sequence: u64,
}

/// Why no health figures could be given.
#[derive(Debug)]
pub enum NoHealth {
    /// No log file for that symbol under this root.
    NoLog,
    /// Logs exist but none covers the instant.
    NotCovered,
    /// A line was found and could not be read. **Loud on purpose:** the emitter
    /// may have changed, and silently reporting "no metrics" would hide that
    /// until someone noticed the block had been empty for a month.
    Unparseable { line: String, why: String },
}

impl core::fmt::Display for NoHealth {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NoLog => write!(f, "no process log for this symbol under this root"),
            Self::NotCovered => write!(
                f,
                "no metrics line covers this instant -- the process may not have been \
                 running, or the line for that minute was lost"
            ),
            Self::Unparseable { line, why } => write!(
                f,
                "a metrics line could not be read ({why}). The emitter's format may have \
                 changed, in which case this reader needs updating: {line}"
            ),
        }
    }
}

/// The metrics line covering `at`, from the process logs under `root`.
///
/// The *containing* minute: the line emitted at or after the instant, since a
/// line describes the window that ends when it is written. Taking the line
/// before would report the minute the instant is not in.
///
/// # Errors
///
/// [`NoHealth`] when no line covers the instant, or one does and cannot be read.
pub fn health_at(root: &Path, symbol: &str, at: Ts) -> Result<HealthAt, NoHealth> {
    let logs = logs_for(root, symbol);
    if logs.is_empty() {
        return Err(NoHealth::NoLog);
    }

    let mut best: Option<HealthAt> = None;
    let mut failure: Option<NoHealth> = None;
    for log in logs {
        let Ok(text) = std::fs::read_to_string(&log) else {
            continue;
        };
        for line in text.lines() {
            if !line.contains("metrics symbol=") {
                continue;
            }
            match parse_line(line, &log) {
                Err(why) => {
                    failure.get_or_insert(NoHealth::Unparseable {
                        line: line.to_owned(),
                        why,
                    });
                }
                Ok(health) => {
                    // The first line at or after the instant is the one whose
                    // window contains it.
                    if health.emitted_at >= at {
                        let better = best
                            .as_ref()
                            .is_none_or(|b| health.emitted_at < b.emitted_at);
                        if better {
                            best = Some(health);
                        }
                        break;
                    }
                }
            }
        }
    }

    match (best, failure) {
        (Some(health), _) => Ok(health),
        (None, Some(why)) => Err(why),
        (None, None) => Err(NoHealth::NotCovered),
    }
}

/// Every attempt's log for one symbol, in name order.
///
/// `supervise.sh` writes `SYMBOL-YYYYMMDD-HHMMSS.log`, a new file per restart,
/// with the attempt's UTC start in the name — so lexical order is chronological,
/// the same property the raw tier's zero-padded parts rely on.
fn logs_for(root: &Path, symbol: &str) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(root.join("logs")) else {
        return Vec::new();
    };
    // Matched on the whole file name rather than via `Path::extension`, which
    // clippy rightly flags as case-sensitive: these names are ours, written by
    // `supervise.sh` in exactly this shape, so recognising that shape exactly is
    // the same discipline `TierTarget::parse` follows -- the inverse of the
    // writer, not a guess at what a log file might be called.
    let prefix = format!("{symbol}-");
    let mut found: Vec<PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                n.strip_prefix(&prefix)
                    .and_then(|rest| rest.strip_suffix(".log"))
                    .is_some()
            })
        })
        .collect();
    found.sort();
    found
}

/// Read one metrics line.
///
/// Strict about the fields it claims to understand and silent about any it does
/// not, which is the same stance `quant-binance::sequence` takes with venue
/// payloads: a new field added upstream must not make an old line unreadable,
/// but a *missing* one must be loud, because it means the format moved.
fn parse_line(line: &str, log: &Path) -> Result<HealthAt, String> {
    let (stamp, rest) = line
        .split_once("  ")
        .ok_or_else(|| "no timestamp".to_owned())?;
    let emitted_at = Ts::parse_rfc3339(stamp.trim())
        .map_err(|e| format!("the leading timestamp did not parse: {e}"))?;

    let field = |name: &str| -> Result<&str, String> {
        rest.split_whitespace()
            .find_map(|pair| pair.strip_prefix(&format!("{name}=")))
            .ok_or_else(|| format!("no {name} field"))
    };
    let number = |name: &str| -> Result<i64, String> {
        field(name)?
            .parse()
            .map_err(|_| format!("{name} is not a number"))
    };
    let count = |name: &str| -> Result<u64, String> {
        field(name)?
            .parse()
            .map_err(|_| format!("{name} is not a count"))
    };

    Ok(HealthAt {
        log: log.to_owned(),
        emitted_at,
        queue: count("queue")?,
        queue_peak: count("queue_peak")?,
        queue_capacity: count("queue_capacity")?,
        dropped: count("dropped")?,
        msgs_per_sec: count("msgs_per_sec")?,
        latency_p50_ms: number("latency_p50_ms")?,
        latency_p99_ms: number("latency_p99_ms")?,
        latency_max_ms: number("latency_max_ms")?,
        clock_skew_ms: number("clock_skew")?,
        gap_disconnect: count("gap_disconnect")?,
        gap_overflow: count("gap_overflow")?,
        gap_sequence: count("gap_sequence")?,
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_line, NoHealth};
    use std::path::Path;

    /// A line copied verbatim out of the live fortnight's log, which is the only
    /// fixture worth having here: the point of this module is to read what the
    /// recorder actually writes, not what its format is supposed to be.
    const REAL: &str = "2026-09-19T13:54:52.334285Z  INFO quant_binance::capture: metrics symbol=BTCUSDT msgs_per_sec=42 bytes_per_sec=14067 queue=0 queue_peak=270 queue_capacity=4096 dropped=0 latency_p50_ms=63 latency_p90_ms=81 latency_p99_ms=110 latency_max_ms=153 latency_samples=2524 clock_skew=0 gap_disconnect=0 gap_overflow=0 gap_sequence=0";

    #[test]
    fn a_real_line_from_the_running_fortnight_parses() {
        let health = parse_line(REAL, Path::new("x.log")).expect("the live format must parse");
        assert_eq!(health.queue, 0);
        assert_eq!(health.queue_peak, 270);
        assert_eq!(health.dropped, 0);
        assert_eq!(health.latency_p50_ms, 63);
        assert_eq!(health.msgs_per_sec, 42);
        assert_eq!(
            health.emitted_at.to_rfc3339(),
            "2026-09-19T13:54:52.334285000Z"
        );
    }

    #[test]
    fn an_unknown_extra_field_does_not_make_a_line_unreadable() {
        // Tolerant of what it does not claim to understand: a field added
        // upstream must not make every existing log unreadable.
        let line = format!("{REAL} something_new=1");
        assert!(parse_line(&line, Path::new("x.log")).is_ok());
    }

    #[test]
    fn a_missing_field_is_loud_rather_than_defaulted() {
        // And strict about what it does. A zero where a number is missing would
        // be indistinguishable from a healthy queue, which is the exact figure
        // an operator checks first.
        let line = REAL.replace(" dropped=0", "");
        let why = parse_line(&line, Path::new("x.log")).expect_err("must refuse");
        assert!(why.contains("dropped"), "{why}");
    }

    #[test]
    fn an_unparseable_line_names_the_line_and_the_reason() {
        // Because the likeliest cause is that the emitter changed, and the
        // person reading this needs to know that rather than believing the
        // minute had no metrics.
        let why = NoHealth::Unparseable {
            line: "metrics symbol=BTCUSDT queue=nonsense".to_owned(),
            why: "queue is not a count".to_owned(),
        }
        .to_string();
        assert!(why.contains("emitter"), "{why}");
        assert!(why.contains("nonsense"), "{why}");
    }
}
