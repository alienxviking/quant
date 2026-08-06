//! What the verifier found, and how much of it to say.
//!
//! # Why findings are capped
//!
//! A seven-day capture holds hundreds of millions of frames. One systematic
//! defect -- a chain check that is subtly wrong, a recorder that lost its anchor
//! on Tuesday -- would otherwise emit a finding per frame, and a report with four
//! million lines in it is a report nobody reads. Worse, it buries the *other*
//! findings, which is the failure mode where a verifier makes things worse than
//! having none.
//!
//! So each `(code, session)` pair reports at most [`MAX_PER_CODE`] examples and
//! then counts the rest. The count is never dropped: "and 3,201,884 more" is the
//! number that tells you it is systematic rather than a one-off, which is the most
//! important thing the report can say.

use core::fmt;
use std::collections::HashMap;

use quant_recorder::format_session_id;

/// Examples reported per `(code, session)` before switching to counting.
pub const MAX_PER_CODE: usize = 5;

/// How much a finding matters.
///
/// Only two levels, deliberately. A third ("info") becomes the level everything
/// drifts into, and the question the verifier answers is binary: may this capture
/// be trusted or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    /// The capture cannot be trusted for what it claims to cover. Fails the run.
    Error,
    /// True, worth knowing, and not a reason to distrust the data. A torn tail on
    /// a file still being written is the archetype.
    Warning,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Error => "ERROR",
            Self::Warning => "warn ",
        })
    }
}

/// Stable identifiers for what can go wrong.
///
/// Strings rather than an enum so a finding can be grepped and counted from a log
/// without this crate's vocabulary, and stable so a cron job can alert on one
/// specific code without matching prose that might get reworded.
pub mod code {
    /// `ingest_seq` skipped forward with no gap record to account for it.
    pub const UNEXPLAINED_HOLE: &str = "unexplained-hole";
    /// `ingest_seq` went backwards or repeated across a segment boundary.
    pub const SEQUENCE_BACKWARDS: &str = "sequence-backwards";
    /// The venue's depth update-id chain broke with no gap to explain it.
    pub const CHAIN_BREAK: &str = "chain-break";
    /// Depth deltas with no snapshot and no recorded reason for its absence.
    pub const UNANCHORED_DELTAS: &str = "unanchored-deltas";
    /// Deltas resumed after a local overflow with no fresh anchor. See the note
    /// in the session checks: currently a limitation of the recorder, not a bug
    /// in the capture.
    pub const UNANCHORED_AFTER_OVERFLOW: &str = "unanchored-after-overflow";
    /// A recorded payload could not be parsed at all.
    pub const PAYLOAD_UNREADABLE: &str = "payload-unreadable";
    /// A frame recorded as a snapshot does not contain a book.
    pub const SNAPSHOT_UNREADABLE: &str = "snapshot-unreadable";
    /// Bytes on disk are not the bytes we wrote.
    pub const CORRUPTION: &str = "corruption";
    /// A file was cut short.
    pub const TORN_TAIL: &str = "torn-tail";
    /// The writer never declared the file closed.
    pub const NO_TRAILER: &str = "no-trailer";
    /// A file's own frame or block counts disagree with what was decoded.
    pub const TRAILER_DISAGREES: &str = "trailer-disagrees";
    /// The file header and the path it sits under do not describe the same thing.
    pub const PATH_HEADER_MISMATCH: &str = "path-header-mismatch";
    /// Something under `raw/` is not a capture file at the expected depth.
    pub const STRAY_FILE: &str = "stray-file";
    /// A capture file could not be opened or read.
    pub const UNREADABLE_FILE: &str = "unreadable-file";
    /// The session does not open with a `RecorderRestart` gap.
    pub const NO_RESTART_MARKER: &str = "no-restart-marker";
    /// Records are filed under a partition their timestamp does not belong to.
    pub const MISFILED_RECORDS: &str = "misfiled-records";
    /// The recorder recorded that it could not obtain a snapshot.
    pub const SNAPSHOT_FAILED: &str = "snapshot-failed";
    /// The recorder recorded that it dropped messages.
    pub const LOCAL_OVERFLOW: &str = "local-overflow";
    /// Checks that need venue knowledge were skipped.
    pub const VENUE_NOT_SUPPORTED: &str = "venue-not-supported";
    /// The metadata index and the files disagree. Only from `--reconcile`.
    pub const INDEX_DISAGREES: &str = "index-disagrees";
    /// A session on disk has no rows in the index. Only from `--reconcile`.
    pub const NOT_INDEXED: &str = "not-indexed";
}

/// Where a finding is, precisely enough to go and look.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Where {
    pub session: Option<[u8; 16]>,
    /// Path as given, so it can be pasted into `dump`.
    pub file: Option<String>,
    pub ingest_seq: Option<u64>,
}

impl Where {
    #[must_use]
    pub const fn nowhere() -> Self {
        Self {
            session: None,
            file: None,
            ingest_seq: None,
        }
    }

    #[must_use]
    pub const fn session(id: [u8; 16]) -> Self {
        Self {
            session: Some(id),
            file: None,
            ingest_seq: None,
        }
    }

    #[must_use]
    pub fn file(path: impl Into<String>) -> Self {
        Self {
            session: None,
            file: Some(path.into()),
            ingest_seq: None,
        }
    }

    #[must_use]
    pub fn in_session(mut self, id: [u8; 16]) -> Self {
        self.session = Some(id);
        self
    }

    #[must_use]
    pub fn at_seq(mut self, seq: u64) -> Self {
        self.ingest_seq = Some(seq);
        self
    }
}

impl fmt::Display for Where {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut wrote = false;
        if let Some(id) = self.session {
            // Short prefix: the full UUID is in the file path when there is one,
            // and eight hex digits are plenty to tell sessions apart by eye.
            write!(f, "session {}", &format_session_id(&id)[..8])?;
            wrote = true;
        }
        if let Some(seq) = self.ingest_seq {
            write!(f, "{}seq {seq}", if wrote { " " } else { "" })?;
            wrote = true;
        }
        if let Some(path) = &self.file {
            write!(f, "{}{path}", if wrote { " " } else { "" })?;
        }
        Ok(())
    }
}

/// One thing that is wrong, or worth saying.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub severity: Severity,
    pub code: &'static str,
    pub at: Where,
    pub detail: String,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {:26} {}", self.severity, self.code, self.detail)?;
        let at = self.at.to_string();
        if at.is_empty() {
            Ok(())
        } else {
            write!(f, "  [{at}]")
        }
    }
}

/// Counts of what a run looked at, so a clean report still says something.
///
/// A verifier that prints nothing on success is indistinguishable from one that
/// silently found no files to check, and "zero errors" over zero frames is the
/// most dangerous clean bill of health there is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Totals {
    pub sessions: u64,
    pub segments: u64,
    pub frames: u64,
    pub stream_messages: u64,
    pub depth_deltas: u64,
    pub trades: u64,
    pub snapshots: u64,
    pub gaps: u64,
    /// Messages accounted for by holes in `ingest_seq`.
    pub missing_messages: u64,
    /// Deltas that arrived before their episode's first snapshot.
    ///
    /// Not a defect -- the snapshot is fetched concurrently with the drain, so a
    /// few always precede it, and Binance's algorithm discards them. Reported
    /// because it is the difference between "recorded" and "reconstructible", and
    /// a number that starts climbing means snapshots are arriving late.
    pub deltas_before_anchor: u64,
}

/// Occurrences per `(code, session)`, whether reported or capped, with the worst
/// severity seen so totals can be taken without re-deriving it.
type Seen = HashMap<(&'static str, Option<[u8; 16]>), (Severity, usize)>;

/// Everything a run found.
#[derive(Debug, Clone, Default)]
pub struct Report {
    findings: Vec<Finding>,
    seen: Seen,
    pub totals: Totals,
}

impl Report {
    /// Record a finding, or count it if this code has already said enough.
    pub fn push(&mut self, severity: Severity, code: &'static str, at: Where, detail: String) {
        let key = (code, at.session);
        let entry = self.seen.entry(key).or_insert((severity, 0));
        // Worst severity wins, so a code that is usually a warning cannot mask an
        // occurrence that was an error.
        entry.0 = entry.0.min(severity);
        entry.1 += 1;
        if entry.1 <= MAX_PER_CODE {
            self.findings.push(Finding {
                severity,
                code,
                at,
                detail,
            });
        }
    }

    pub fn error(&mut self, code: &'static str, at: Where, detail: impl Into<String>) {
        self.push(Severity::Error, code, at, detail.into());
    }

    pub fn warn(&mut self, code: &'static str, at: Where, detail: impl Into<String>) {
        self.push(Severity::Warning, code, at, detail.into());
    }

    #[must_use]
    pub fn findings(&self) -> &[Finding] {
        &self.findings
    }

    /// How many findings of each code were suppressed by the cap.
    #[must_use]
    pub fn suppressed(&self) -> Vec<(&'static str, usize)> {
        let mut per_code: HashMap<&'static str, usize> = HashMap::new();
        for ((code, _), (_, count)) in &self.seen {
            let over = count.saturating_sub(MAX_PER_CODE);
            if over > 0 {
                *per_code.entry(code).or_insert(0) += over;
            }
        }
        let mut out: Vec<_> = per_code.into_iter().collect();
        out.sort_unstable();
        out
    }

    /// Total occurrences of a code, cap or no cap.
    #[must_use]
    pub fn count(&self, code: &str) -> usize {
        self.seen
            .iter()
            .filter(|((c, _), _)| *c == code)
            .map(|(_, (_, n))| *n)
            .sum()
    }

    /// Total occurrences at [`Severity::Error`], including capped ones.
    #[must_use]
    pub fn errors(&self) -> usize {
        self.total_at(Severity::Error)
    }

    /// Total occurrences at [`Severity::Warning`], including capped ones.
    #[must_use]
    pub fn warnings(&self) -> usize {
        self.total_at(Severity::Warning)
    }

    fn total_at(&self, severity: Severity) -> usize {
        self.seen
            .values()
            .filter(|(s, _)| *s == severity)
            .map(|(_, n)| *n)
            .sum()
    }

    /// Whether the capture may be trusted.
    ///
    /// Warnings do not fail a run. If they did, the first torn tail on a file
    /// still being written would train everyone to pass `--ignore-warnings`, and
    /// then the errors would be ignored too.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.errors() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION_A: [u8; 16] = [0xaa; 16];
    const SESSION_B: [u8; 16] = [0xbb; 16];

    #[test]
    fn a_systematic_defect_is_capped_but_never_silently_dropped() {
        let mut report = Report::default();
        for seq in 0..1_000_u64 {
            report.error(
                code::CHAIN_BREAK,
                Where::session(SESSION_A).at_seq(seq),
                "broke",
            );
        }
        assert_eq!(report.findings().len(), MAX_PER_CODE);
        assert_eq!(
            report.count(code::CHAIN_BREAK),
            1_000,
            "the count is the thing that says it is systematic"
        );
        assert_eq!(
            report.suppressed(),
            vec![(code::CHAIN_BREAK, 1_000 - MAX_PER_CODE)]
        );
        assert!(!report.is_clean());
    }

    #[test]
    fn the_cap_is_per_session_so_one_bad_session_cannot_hide_another() {
        let mut report = Report::default();
        for _ in 0..100 {
            report.error(code::CHAIN_BREAK, Where::session(SESSION_A), "a");
        }
        report.error(code::CHAIN_BREAK, Where::session(SESSION_B), "b");
        assert_eq!(report.findings().len(), MAX_PER_CODE + 1);
        assert!(
            report.findings().iter().any(|f| f.detail == "b"),
            "the second session's single finding must survive the first's flood"
        );
    }

    #[test]
    fn warnings_do_not_fail_a_run() {
        // Otherwise the first torn tail on a file still being written teaches
        // everyone to ignore the tool.
        let mut report = Report::default();
        report.warn(code::NO_TRAILER, Where::file("part-00000.bin.zst"), "open");
        assert!(report.is_clean());
        assert_eq!(report.findings().len(), 1);
    }

    #[test]
    fn a_finding_says_where_to_look() {
        let mut report = Report::default();
        report.error(
            code::UNEXPLAINED_HOLE,
            Where::file("data/raw/x/part-00000.bin.zst")
                .in_session(SESSION_A)
                .at_seq(4_318_221),
            "12 messages missing",
        );
        let text = report.findings()[0].to_string();
        assert!(text.contains("unexplained-hole"), "{text}");
        assert!(text.contains("seq 4318221"), "{text}");
        assert!(text.contains("part-00000.bin.zst"), "{text}");
        assert!(text.contains("aaaaaaaa"), "session prefix missing: {text}");
    }
}
