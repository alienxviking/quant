//! Finding capture files, and grouping them back into the sessions that wrote
//! them.
//!
//! # Why a session, not a file
//!
//! `ingest_seq` spans a capture *session*, not a file -- `quant-recorder`'s
//! segment module is explicit about it, and it is the right choice, because a hole
//! that straddles midnight is still a hole. The consequence lands here: verifying
//! files one at a time cannot see that hole at all, so the unit of verification
//! has to be the session and its segments have to be joined in the order they were
//! written.
//!
//! # Why the order comes from the path
//!
//! Sorting a session's file paths as text puts them in the order they were
//! written. That is not luck: the layout uses ISO dates and zero-padded part
//! numbers precisely so that lexical order is chronological order, and
//! `quant-recorder`'s layout tests pin it. The alternative -- opening every file to
//! read its first sequence number and sorting on that -- is more work for a weaker
//! guarantee, since it would happily accept a set of files with no consistent
//! naming at all.
//!
//! # Why both the path and the header are read
//!
//! The header is authoritative about what a file *is*; the path says where it
//! *sits*. They are written from the same values, so a disagreement means somebody
//! moved or renamed files by hand -- and a capture filed under the wrong instrument
//! is worse than a missing one, because every tool downstream reads the directory.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use quant_core::instrument::Exchange;
use quant_recorder::CaptureTarget;
use quant_storage::{FileHeader, RawReader};

use crate::finding::{code, Report, Where};

/// One capture file, with what it says about itself.
#[derive(Debug, Clone)]
pub struct Segment {
    pub path: PathBuf,
    /// Where it sits, parsed back from the path.
    pub target: CaptureTarget,
    /// What it says it is.
    pub header: FileHeader,
}

impl Segment {
    /// Path as text, for reports and for pasting into `dump`.
    #[must_use]
    pub fn display(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

/// One capture session's segments, in the order they were written.
#[derive(Debug, Clone)]
pub struct Session {
    pub exchange: Exchange,
    pub symbol: String,
    pub session_id: [u8; 16],
    pub segments: Vec<Segment>,
}

/// Walk `root` and group every capture file into its session.
///
/// Problems with individual files are reported into `report` rather than aborting:
/// one unreadable file must not stop the other six days from being checked.
pub fn discover(root: &Path, report: &mut Report) -> Vec<Session> {
    let raw = root.join("raw");
    let mut paths = Vec::new();
    collect(&raw, &mut paths, report);
    // Text order is write order; see the module docs.
    paths.sort();

    let mut sessions: Vec<Session> = Vec::new();
    for path in paths {
        let Some(segment) = identify(&path, report) else {
            continue;
        };
        // Grouped by what the *header* says, because that is the authoritative
        // identity; the path has already been cross-checked against it.
        match sessions.iter_mut().find(|s| {
            s.session_id == segment.header.session_id
                && s.exchange == segment.header.exchange
                && s.symbol == segment.header.symbol
        }) {
            Some(existing) => existing.segments.push(segment),
            None => sessions.push(Session {
                exchange: segment.header.exchange,
                symbol: segment.header.symbol.clone(),
                session_id: segment.header.session_id,
                segments: vec![segment],
            }),
        }
    }
    sessions
}

/// Read a file's header and reconcile it against where the file sits.
fn identify(path: &Path, report: &mut Report) -> Option<Segment> {
    let Some(target) = CaptureTarget::parse(path) else {
        report.warn(
            code::STRAY_FILE,
            Where::file(path.to_string_lossy().into_owned()),
            "not a capture file at the expected partition depth",
        );
        return None;
    };

    let header = match File::open(path).map_err(|e| e.to_string()).and_then(|f| {
        // The header is 68 bytes, but `open` also validates magic and checksum,
        // which is what makes "you pointed me at a Parquet file" a clear error
        // rather than a confusing one.
        RawReader::open(BufReader::new(f))
            .map(|r| r.header().clone())
            .map_err(|e| e.to_string())
    }) {
        Ok(header) => header,
        Err(e) => {
            report.error(
                code::UNREADABLE_FILE,
                Where::file(path.to_string_lossy().into_owned()),
                e,
            );
            return None;
        }
    };

    let mut mismatches = Vec::new();
    if header.exchange != target.exchange {
        mismatches.push(format!(
            "header says exchange {} but the path says {}",
            header.exchange, target.exchange
        ));
    }
    if header.symbol != target.symbol {
        mismatches.push(format!(
            "header says symbol {} but the path says {}",
            header.symbol, target.symbol
        ));
    }
    if header.session_id != target.session_id {
        mismatches.push("header and path name different sessions".to_owned());
    }
    if !mismatches.is_empty() {
        report.error(
            code::PATH_HEADER_MISMATCH,
            Where::file(path.to_string_lossy().into_owned()).in_session(header.session_id),
            mismatches.join("; "),
        );
        return None;
    }

    Some(Segment {
        path: path.to_owned(),
        target,
        header,
    })
}

/// Recursive walk, collecting regular files.
///
/// Deliberately not filtered by name here: a `.parquet` or a stray `notes.txt`
/// under `raw/` is reported by [`identify`] rather than skipped, because something
/// unexpected in the immutable tier is worth a line of output either way.
fn collect(dir: &Path, out: &mut Vec<PathBuf>, report: &mut Report) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            report.error(
                code::UNREADABLE_FILE,
                Where::file(dir.to_string_lossy().into_owned()),
                format!("cannot list directory: {e}"),
            );
            return;
        }
    };

    for entry in entries {
        match entry {
            Ok(entry) => {
                let path = entry.path();
                if path.is_dir() {
                    collect(&path, out, report);
                } else {
                    out.push(path);
                }
            }
            Err(e) => report.error(
                code::UNREADABLE_FILE,
                Where::file(dir.to_string_lossy().into_owned()),
                format!("cannot read directory entry: {e}"),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_core::time::UtcDate;
    use quant_storage::{RawWriter, WriterOptions};
    use std::io::Write as _;

    fn target(session: [u8; 16], day: u8, part: u32) -> CaptureTarget {
        CaptureTarget {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            date: UtcDate {
                year: 2026,
                month: 7,
                day,
            },
            session_id: session,
            part,
        }
    }

    /// Write a minimal but real capture file at `target`'s path under `root`.
    fn write_capture(root: &Path, target: &CaptureTarget, header_session: [u8; 16]) {
        let path = target.file(root);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let header = FileHeader::new(target.exchange, &target.symbol, header_session);
        let writer =
            RawWriter::create(Vec::new(), header, WriterOptions::default()).expect("create");
        let (bytes, _) = writer.finish().expect("finish");
        File::create(&path).unwrap().write_all(&bytes).unwrap();
    }

    fn temp_root(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("quant-verify-discover-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn a_session_spanning_midnight_is_one_session_in_write_order() {
        // The property the whole crate rests on: two files, two date partitions,
        // one ingest_seq space -- so they must come back joined and in order.
        let root = temp_root("midnight");
        let session = [7_u8; 16];
        // Created in the wrong order on purpose, and including a second part on
        // one day: `part` resets per day today, but size-based rolling within a day
        // will advance it, and lexical order must already handle that.
        for (day, part) in [(30_u8, 0_u32), (29, 1), (29, 0)] {
            write_capture(&root, &target(session, day, part), session);
        }

        let mut report = Report::default();
        let sessions = discover(&root, &mut report);
        assert_eq!(sessions.len(), 1, "one session, not three");
        let order: Vec<String> = sessions[0]
            .segments
            .iter()
            .map(|s| format!("{}#{}", s.target.date, s.target.part))
            .collect();
        assert_eq!(
            order,
            vec!["2026-07-29#0", "2026-07-29#1", "2026-07-30#0"],
            "segments must come back in write order regardless of directory order"
        );
        assert!(report.is_clean(), "{:?}", report.findings());
    }

    #[test]
    fn different_sessions_stay_separate() {
        let root = temp_root("two-sessions");
        write_capture(&root, &target([1; 16], 29, 0), [1; 16]);
        write_capture(&root, &target([2; 16], 29, 0), [2; 16]);

        let mut report = Report::default();
        let sessions = discover(&root, &mut report);
        assert_eq!(sessions.len(), 2);
        assert!(report.is_clean());
    }

    #[test]
    fn a_file_whose_header_disagrees_with_its_path_is_refused_not_verified() {
        // A capture filed under the wrong instrument is worse than a missing one:
        // every tool downstream reads the directory, so it would be silently
        // attributed to whatever the path says.
        let root = temp_root("mismatch");
        write_capture(&root, &target([3; 16], 29, 0), [99; 16]);

        let mut report = Report::default();
        let sessions = discover(&root, &mut report);
        assert!(sessions.is_empty());
        assert_eq!(report.count(code::PATH_HEADER_MISMATCH), 1);
        assert!(!report.is_clean());
    }

    #[test]
    fn a_stray_file_is_mentioned_rather_than_skipped_in_silence() {
        let root = temp_root("stray");
        let session = [4_u8; 16];
        write_capture(&root, &target(session, 29, 0), session);
        let stray = target(session, 29, 0).directory(&root).join("notes.txt");
        File::create(stray)
            .unwrap()
            .write_all(b"hand-edited")
            .unwrap();

        let mut report = Report::default();
        let sessions = discover(&root, &mut report);
        assert_eq!(sessions.len(), 1, "the real capture is still found");
        assert_eq!(report.count(code::STRAY_FILE), 1);
        assert!(
            report.is_clean(),
            "a stray file is odd, not a reason to distrust the capture"
        );
    }

    #[test]
    fn an_absent_data_root_is_not_an_error_it_is_just_empty() {
        // `verify` running before the first capture should say "nothing here", not
        // fail; a cron job that alerts on an empty directory alerts on day one.
        let mut report = Report::default();
        let sessions = discover(&temp_root("missing"), &mut report);
        assert!(sessions.is_empty());
        assert!(report.is_clean());
        assert!(report.findings().is_empty());
    }
}
