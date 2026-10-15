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
//! # Both trees
//!
//! A data root holds market data under `raw/` and, from M8, the account stream
//! under `account/`. Both are walked, each by its own catalog, so pointing the
//! verifier at a run root checks the whole of what the run recorded -- and a
//! stray in either tree is reported, including the other stream's files moved
//! into it by hand.
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
use quant_recorder::{CaptureStream, CaptureTarget, CatalogEntry, ACCOUNT_STREAM};
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
    /// One instrument's market data, or the account stream. Decides which
    /// checks the session gets: see [`crate::session`].
    pub stream: CaptureStream,
    pub session_id: [u8; 16],
    pub segments: Vec<Segment>,
}

/// Walk `root`'s market and account trees and group every capture file into its
/// session: market sessions first, then the account stream's.
///
/// Problems with individual files are reported into `report` rather than aborting:
/// one unreadable file must not stop the other six days from being checked.
pub fn discover(root: &Path, report: &mut Report) -> Vec<Session> {
    let market = quant_recorder::catalog(root);
    let account = quant_recorder::account_catalog(root);

    for path in market.strays.iter().chain(&account.strays) {
        report.warn(
            code::STRAY_FILE,
            Where::file(path.to_string_lossy().into_owned()),
            "not a capture file of this tree at the expected partition depth",
        );
    }
    for (path, error) in market.unreadable.iter().chain(&account.unreadable) {
        report.error(
            code::UNREADABLE_FILE,
            Where::file(path.to_string_lossy().into_owned()),
            format!("cannot list directory: {error}"),
        );
    }

    let market = market.sessions.into_iter().map(|files| {
        (
            files.exchange,
            CaptureStream::market(files.symbol),
            files.session_id,
            files.segments,
        )
    });
    let account = account.sessions.into_iter().map(|files| {
        (
            files.exchange,
            CaptureStream::Account,
            files.session_id,
            files.segments,
        )
    });
    market
        .chain(account)
        .filter_map(|(exchange, stream, session_id, entries)| {
            let segments: Vec<Segment> = entries
                .into_iter()
                .filter_map(|entry| identify(entry, report))
                .collect();
            // A session every one of whose files was rejected is not a session we
            // can say anything about; the rejections are already findings.
            if segments.is_empty() {
                return None;
            }
            Some(Session {
                exchange,
                stream,
                session_id,
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
    // What the header's symbol field must say is the layout's to decide, not
    // this module's: the venue's symbol for market data, the stream's own name
    // for the account stream, which has no symbol.
    if header.symbol != target.stream.header_symbol() {
        mismatches.push(match &target.stream {
            CaptureStream::Market { symbol } => format!(
                "header says symbol {} but the path says {symbol}",
                header.symbol
            ),
            CaptureStream::Account => format!(
                "header says symbol {} but the path says the account stream, whose \
                 files say {ACCOUNT_STREAM}",
                header.symbol
            ),
        });
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
