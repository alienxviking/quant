//! How the process itself was doing, for the minute containing an instant.
//!
//! # The debt M7 scheduled, and what actually repaid it
//!
//! The recorder's metrics are the one thing `explain` reports that cannot be
//! re-derived. Queue depth, drop counts and venue latency are properties of one
//! process at one instant; they appear nowhere in the capture, so if the log is
//! deleted they are gone. They therefore have to be read back out of a log.
//!
//! Until M7.5.g that meant **parsing prose**: matching the literal
//! `"metrics symbol="` and pulling `key=value` pairs out of whatever `tracing`'s
//! `fmt` layer happened to render. An emitter and a parser that must agree
//! forever, coupled through a format neither owns, with no test that they agree,
//! is the pattern this project refuses. M7 accepted it anyway and wrote down the
//! repayment date, because the only fix changed the running binary and the
//! fortnight was pinned.
//!
//! **The repayment is not "parse JSON instead of prose".** That moves the
//! coupling without removing it — two lists of field names that must still match,
//! in two crates, with nothing to notice when they stop. The fix is that there is
//! now **one type**: [`quant_recorder::MetricsLine`] is serialized by the emitter
//! and deserialized here, so adding a field changes one struct and both sides at
//! once. What remains of the format is `tracing`'s JSON envelope — a `timestamp`
//! and a `fields` object — and that is pinned by a test which runs the real
//! emitter through a real subscriber and reads its bytes back through this
//! reader. That test is the thing M7 said was owed and could not yet be written.
//!
//! # Three kinds of absence, which is the other half
//!
//! A query tool's characteristic failure is confabulation, and the most
//! plausible-looking confabulation here is blaming the run for a stale reader.
//! So "no log at all", "lines exist but none this reader recognises", and
//! "metrics lines exist but none covers the instant" stay three separate
//! answers. The middle one is a statement about *this parser*, and says so.
//!
//! # The time bases
//!
//! Each figure's base is on [`quant_recorder::MetricsLine`] where the figure is,
//! rather than restated here. One line mixes instantaneous, lifetime and window
//! figures, and printing them unmarked is how an operator concludes the wrong
//! thing at 3am.

use std::path::{Path, PathBuf};

use quant_core::time::Ts;
use quant_recorder::MetricsLine;

/// The metrics line covering an instant.
#[derive(Debug, Clone)]
pub struct HealthAt {
    /// The log file the line came from.
    pub log: PathBuf,
    /// When the line was emitted — the end of the window it describes.
    pub emitted_at: Ts,
    /// Exactly what the recorder wrote, as the type it wrote.
    pub line: MetricsLine,
}

/// Why there is nothing to report.
#[derive(Debug)]
pub enum NoHealth {
    /// No log file for that symbol under this root.
    NoLog,
    /// Logs exist and hold metrics lines, but none covers the instant.
    NotCovered,
    /// A log was read, held lines, and **not one of them looked like a metrics
    /// line**.
    ///
    /// Distinct from [`Self::NotCovered`] on purpose. "No line covers this
    /// instant" says something about the *run* — the process was down, or that
    /// minute was lost. "No line is recognisable" says something about *this
    /// reader*, and reporting it as the former would blame a healthy run for a
    /// parser that went stale.
    FormatUnrecognised { log: PathBuf, lines: usize },
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
            Self::FormatUnrecognised { log, lines } => write!(
                f,
                "read {lines} lines of {} and none is a metrics line. The emitter's format \
                 has changed and this reader has not been updated -- this is not a statement \
                 about the run",
                log.display()
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
    // Counted so an absence can say which kind of absence it is.
    let mut lines_read = 0_usize;
    let mut candidates = 0_usize;
    let mut last_read: Option<PathBuf> = None;
    for log in logs {
        let Ok(text) = std::fs::read_to_string(&log) else {
            continue;
        };
        last_read = Some(log.clone());
        for line in text.lines() {
            lines_read += 1;
            // Recognition is structural now: a JSON object whose `message`
            // field is the one the emitter writes. The old test was
            // `line.contains("metrics symbol=")`, and that literal was itself
            // part of a format this reader did not own -- change the emitter and
            // every line stopped matching at once.
            let Some(envelope) = metrics_envelope(line) else {
                continue;
            };
            candidates += 1;
            match parse_line(&envelope, line, &log) {
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
        // Lines were read and not one of them was even a candidate: the sentinel
        // no longer matches, which is a fact about this parser and not about the
        // run. Saying `NotCovered` here would blame a healthy process.
        (None, None) if candidates == 0 && lines_read > 0 => Err(NoHealth::FormatUnrecognised {
            log: last_read.unwrap_or_default(),
            lines: lines_read,
        }),
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
/// A metrics event, or `None` if this line is not one.
///
/// The whole of the format this reader still has to know: `tracing`'s JSON
/// envelope is an object with a `timestamp` and a `fields` map, and the emitter
/// puts the message in `fields.message`. Everything past that point is
/// [`MetricsLine`]'s own business.
fn metrics_envelope(line: &str) -> Option<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    let message = value.get("fields")?.get("message")?.as_str()?;
    (message == MetricsLine::MESSAGE).then_some(value)
}

/// Read one metrics event into the type the emitter wrote.
fn parse_line(envelope: &serde_json::Value, line: &str, log: &Path) -> Result<HealthAt, String> {
    let stamp = envelope
        .get("timestamp")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "no timestamp".to_owned())?;
    let emitted_at = Ts::parse_rfc3339(stamp)
        .map_err(|e| format!("the envelope timestamp did not parse: {e}"))?;

    // Carried as a string of its own JSON rather than as loose fields, so the
    // log renders identically under `fmt` and under `json` and neither a person
    // grepping it nor this reader has to know which layer produced it.
    let payload = envelope
        .get("fields")
        .and_then(|f| f.get(MetricsLine::FIELD))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("no `{}` field on a metrics event", MetricsLine::FIELD))?;
    let parsed: MetricsLine = serde_json::from_str(payload)
        .map_err(|e| format!("the metrics payload did not parse: {e}"))?;

    let _ = line;
    Ok(HealthAt {
        log: log.to_owned(),
        emitted_at,
        line: parsed,
    })
}

#[cfg(test)]
mod tests {
    use super::{health_at, metrics_envelope, parse_line, NoHealth};
    use quant_recorder::MetricsLine;
    use std::path::Path;

    /// Every field distinct and non-zero, which is load-bearing.
    ///
    /// A fixture of zeros cannot tell a field that was written from one that was
    /// dropped: `#[serde(skip)]` on a `u64` writes nothing and reads back `0`, so
    /// a round-trip of zeros succeeds either way. Found by sabotage — marking
    /// `dropped` as skipped reddened nothing until these values became distinct.
    fn line() -> MetricsLine {
        MetricsLine {
            symbol: "BTCUSDT".to_owned(),
            msgs_per_sec: 42,
            bytes_per_sec: 14_067,
            queue_depth: 7,
            queue_high_water: 270,
            queue_capacity: 4_096,
            dropped: 3,
            latency_p50_micros: 63_000,
            latency_p90_micros: 81_000,
            latency_p99_micros: 110_000,
            latency_max_micros: 153_000,
            latency_samples: 2_524,
            clock_skew_samples: 11,
            gaps: [1, 2, 4, 8],
        }
    }

    /// An envelope in the shape `tracing`'s JSON layer writes.
    fn event(line: &MetricsLine, stamp: &str) -> String {
        format!(
            r#"{{"timestamp":"{stamp}","level":"INFO","fields":{{"message":"metrics","metrics":{}}},"target":"quant_binance::capture"}}"#,
            serde_json::to_string(&serde_json::to_string(line).expect("inner")).expect("outer")
        )
    }

    #[test]
    fn a_line_round_trips_through_the_type_the_emitter_wrote() {
        let raw = event(&line(), "2026-09-19T13:54:52.334285Z");
        let envelope = metrics_envelope(&raw).expect("a metrics event");
        let health = parse_line(&envelope, &raw, Path::new("x.log")).expect("parses");
        assert_eq!(
            health.line,
            line(),
            "the whole value, not a field at a time"
        );
        assert_eq!(
            health.emitted_at.to_rfc3339(),
            "2026-09-19T13:54:52.334285000Z"
        );
    }

    #[test]
    fn a_field_added_upstream_does_not_make_a_log_unreadable() {
        // Tolerant of what it does not claim to understand, which is the same
        // stance `quant-binance::sequence` takes: a new field must not make an
        // existing log unreadable.
        let raw = event(&line(), "2026-09-19T13:54:52.334285Z")
            .replace(r#""level":"INFO""#, r#""level":"INFO","span":{"name":"x"}"#);
        let envelope = metrics_envelope(&raw).expect("still a metrics event");
        assert!(parse_line(&envelope, &raw, Path::new("x.log")).is_ok());
    }

    #[test]
    fn a_payload_that_will_not_parse_is_loud_rather_than_defaulted() {
        // Strict about what it does claim. A zero where a number is missing is
        // indistinguishable from a healthy queue, which is the first figure an
        // operator reads.
        let truncated = serde_json::to_string(r#"{"symbol":"BTCUSDT"}"#).expect("inner");
        let raw = format!(
            r#"{{"timestamp":"2026-09-19T13:54:52.334285Z","fields":{{"message":"metrics","metrics":{truncated}}}}}"#
        );
        let envelope = metrics_envelope(&raw).expect("a metrics event");
        let why = parse_line(&envelope, &raw, Path::new("x.log")).expect_err("must refuse");
        assert!(why.contains("did not parse"), "{why}");
    }

    #[test]
    fn clock_skew_reads_as_a_count_of_messages_not_as_milliseconds() {
        // The field is a counter: `quant-recorder::metrics` increments it once
        // per message whose venue timestamp is ahead of ours, and records no
        // duration anywhere. The old reader called it `clock_skew_ms` and warned
        // above 1000 "ms", so a thousand ordinary samples produced a clock alarm
        // in units nothing had measured. The name now carries the unit.
        let mut l = line();
        l.clock_skew_samples = 1_500;
        let raw = event(&l, "2026-09-19T13:54:52.334285Z");
        let envelope = metrics_envelope(&raw).expect("a metrics event");
        let health = parse_line(&envelope, &raw, Path::new("x.log")).expect("parses");
        assert_eq!(health.line.clock_skew_samples, 1_500, "a count of messages");
        assert!(
            health.line.clock_skew_samples < health.line.latency_samples,
            "1500 of 2524 samples is the honest reading; 1500ms is not a reading at all"
        );
    }

    #[test]
    fn a_log_this_reader_no_longer_understands_says_so_rather_than_reporting_silence() {
        // Three kinds of absence, and this is the one that blames the reader
        // instead of the run. "No line covers this instant" says the process was
        // down; "no line is recognisable" says this parser went stale. Reporting
        // the second as the first sends whoever is on call to the wrong place.
        let root = std::env::temp_dir().join("quant-explain-format-drift");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("logs")).expect("mkdir");
        // Real JSON events that are not metrics events.
        let other =
            r#"{"timestamp":"2026-09-19T13:54:52.334285Z","fields":{"message":"connected"}}"#;
        std::fs::write(
            root.join("logs").join("BTCUSDT-20260919-000000.log"),
            format!("{other}\n{other}\n{other}\n"),
        )
        .expect("write");

        let at = quant_core::time::Ts::parse_rfc3339("2026-09-19T13:54:52Z").expect("ts");
        match health_at(&root, "BTCUSDT", at) {
            Err(NoHealth::FormatUnrecognised { lines, .. }) => assert_eq!(lines, 3),
            other => panic!("expected FormatUnrecognised, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&root);
    }
    /// Captures whatever a subscriber writes, so a test can read it back.
    #[derive(Clone, Default)]
    struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("captured").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_real_emitter_writes_what_this_reader_reads() {
        // **The test M7 recorded as owed and could not write.** Everything above
        // feeds this reader an envelope built by hand, which proves only that it
        // can read a fixture someone wrote to match it. The thing worth pinning
        // is that the recorder's actual emitter, through an actual JSON
        // subscriber, produces bytes this actual reader understands.
        //
        // It could not be written before for two reasons and only one of them
        // was the freeze. The other was structural: the emitter sat in
        // `quant-binance`, nothing may depend on `quant-explain`, and
        // `quant-explain` may not depend on `quant-binance`. Moving the emitter
        // onto `MetricsLine` -- which both sides already depend on -- is what
        // made the pairing expressible.
        let sink = Captured::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer({
                let sink = sink.clone();
                move || sink.clone()
            })
            .finish();

        let expected = line();
        tracing::subscriber::with_default(subscriber, || expected.emit());

        let bytes = sink.0.lock().expect("captured").clone();
        let text = String::from_utf8(bytes).expect("utf-8");
        let raw = text.lines().next().expect("the emitter wrote a line");

        let envelope =
            metrics_envelope(raw).expect("the reader must recognise what the emitter writes");
        let health =
            parse_line(&envelope, raw, Path::new("x.log")).expect("and must be able to read it");
        assert_eq!(
            health.line, expected,
            "every field, through a real subscriber and back"
        );
    }
}
