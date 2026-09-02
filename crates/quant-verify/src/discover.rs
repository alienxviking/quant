//! Turning a capture tree into sessions to verify.
//!
//! # Where the grouping lives
//!
//! Finding the files, grouping them into sessions and ordering the segments is
//! `quant_recorder::catalog`, not this module — it is a pure function of the
//! layout that wrote them, and the normalizer needs the identical answer. If the
//! two ever disagreed, the normalizer would replay a different stream than the
//! one this verifier passed clean, and nothing would say so. That module's docs
//! carry the argument in full.
//!
//! What stays here is everything the layout crate has no business deciding:
//! reading each file's header, cross-checking it against the path, and turning
//! the leftovers into findings.
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
use quant_recorder::{CaptureTarget, CatalogEntry};
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
    let catalog = quant_recorder::catalog(root);

    for path in &catalog.strays {
        report.warn(
            code::STRAY_FILE,
            Where::file(path.to_string_lossy().into_owned()),
            "not a capture file at the expected partition depth",
        );
    }
    for (path, error) in &catalog.unreadable {
        report.error(
            code::UNREADABLE_FILE,
            Where::file(path.to_string_lossy().into_owned()),
            format!("cannot list directory: {error}"),
        );
    }

    catalog
        .sessions
        .into_iter()
        .filter_map(|files| {
            let segments: Vec<Segment> = files
                .segments
                .into_iter()
                .filter_map(|entry| identify(entry, report))
                .collect();
            // A session every one of whose files was rejected is not a session we
            // can say anything about; the rejections are already findings.
            if segments.is_empty() {
                return None;
            }
            Some(Session {
                exchange: files.exchange,
                symbol: files.symbol,
                session_id: files.session_id,
                segments,
            })
        })
        .collect()
}

/// Read a file's header and reconcile it against where the file sits.
fn identify(entry: CatalogEntry, report: &mut Report) -> Option<Segment> {
    let CatalogEntry { path, target } = entry;

    let header = match File::open(&path).map_err(|e| e.to_string()).and_then(|f| {
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
        path,
        target,
        header,
    })
}
