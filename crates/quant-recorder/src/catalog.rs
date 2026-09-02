//! Reading the capture tree back: which files exist, and which session wrote
//! each one.
//!
//! [`layout`](crate::layout) turns an identity into a path. This module is the
//! other direction over a whole tree — walk `root/raw`, parse every path back
//! through [`CaptureTarget::parse`], and group the results into the sessions that
//! produced them, with each session's segments in the order they were written.
//!
//! # Why this lives here and not in the tool that wants it
//!
//! The verifier needed it first, and for a while it was the verifier's own code.
//! But the normalizer needs exactly the same answer, and "exactly the same" is
//! load-bearing rather than merely convenient: if the two disagreed about which
//! files form a session, or about what order they go in, the normalizer would
//! replay a *different stream* than the one the verifier passed clean — and the
//! clean bill of health would be about something we never processed. That
//! divergence is silent. It is the same argument that put
//! [`CaptureTarget::parse`] beside [`CaptureTarget::file`] rather than wherever
//! it happened to be needed.
//!
//! So the grouping rule has one definition, and it sits with the layout it is a
//! function of.
//!
//! # Why it reports no findings
//!
//! This crate has no idea what a finding is, and should not. It returns what it
//! found, what did not fit the layout, and what it could not read; the caller
//! decides what each of those means. They genuinely differ: a stray file under
//! `raw/` is a warning to the verifier, and an unreadable segment is a reason for
//! the normalizer to invalidate its book rather than silently stitch across data
//! it never saw. A shared walker that decided for them would have to be wrong for
//! one of them.
//!
//! # Why the order comes from the path
//!
//! Sorting a session's paths as text puts them in write order. That is not luck:
//! the layout uses ISO dates and zero-padded part numbers precisely so lexical
//! order is chronological order, and `layout`'s tests pin it. Opening every file
//! to read its first sequence number instead would be more work for a weaker
//! guarantee — it would happily accept a set of files with no consistent naming
//! at all, which is the situation this is meant to detect.

use std::io;
use std::path::{Path, PathBuf};

use quant_core::instrument::Exchange;

use crate::layout::CaptureTarget;

/// One capture file, and where it sits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub path: PathBuf,
    pub target: CaptureTarget,
}

/// One capture session's files, in the order they were written.
///
/// Identity is `(exchange, symbol, session_id)` and not the session id alone.
/// Two symbols recorded in the same run are separate sessions with separate
/// `ingest_seq` spaces, and the recorder gives them separate ids today — but
/// grouping on the full triple means this stays correct if that ever changes,
/// and a book built from two symbols' deltas interleaved would be nonsense that
/// no downstream check could recognise as such.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionFiles {
    pub exchange: Exchange,
    /// The venue's own symbol, verbatim.
    pub symbol: String,
    pub session_id: [u8; 16],
    /// At least one, in write order.
    pub segments: Vec<CatalogEntry>,
}

/// Everything under a data root, sorted into what fits the layout and what does
/// not.
#[derive(Debug, Default)]
pub struct Catalog {
    /// Sessions, in the order their first segment was encountered.
    pub sessions: Vec<SessionFiles>,
    /// Files under `raw/` that are not capture files at the expected depth.
    ///
    /// Not filtered out silently: something unexpected in the immutable tier is
    /// worth a line of output whatever it turns out to be. The macOS
    /// `AppleDouble` stubs that arrived with the acceptance capture were found
    /// exactly this way.
    pub strays: Vec<PathBuf>,
    /// Directories that could not be listed.
    ///
    /// Kept as errors rather than folded into `strays`, because "this is not a
    /// capture file" and "I could not look" are different claims, and only one of
    /// them means the catalog is incomplete.
    pub unreadable: Vec<(PathBuf, io::Error)>,
}

/// Walk `root/raw` and group every capture file into its session.
///
/// A missing `raw/` directory is not an error — an empty root is a legitimate
/// state, and the caller has better words for it than this function does
/// (the verifier's "NOTHING VERIFIED" verdict, for one).
#[must_use]
pub fn catalog(root: &Path) -> Catalog {
    let mut out = Catalog::default();
    let mut paths = Vec::new();
    collect(&root.join("raw"), &mut paths, &mut out.unreadable);
    // Text order is write order; see the module docs.
    paths.sort();

    for path in paths {
        let Some(target) = CaptureTarget::parse(&path) else {
            out.strays.push(path);
            continue;
        };
        let entry = CatalogEntry { path, target };
        match out.sessions.iter_mut().find(|s| {
            s.session_id == entry.target.session_id
                && s.exchange == entry.target.exchange
                && s.symbol == entry.target.symbol
        }) {
            Some(existing) => existing.segments.push(entry),
            None => out.sessions.push(SessionFiles {
                exchange: entry.target.exchange,
                symbol: entry.target.symbol.clone(),
                session_id: entry.target.session_id,
                segments: vec![entry],
            }),
        }
    }
    out
}

/// Recursive walk, collecting regular files.
///
/// Deliberately unfiltered by name: deciding here that a `.parquet` or a
/// `notes.txt` is uninteresting would hide it from the caller that wanted to hear
/// about it.
fn collect(dir: &Path, out: &mut Vec<PathBuf>, unreadable: &mut Vec<(PathBuf, io::Error)>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return,
        Err(e) => {
            unreadable.push((dir.to_owned(), e));
            return;
        }
    };

    for entry in entries {
        match entry {
            Ok(entry) => {
                let path = entry.path();
                if path.is_dir() {
                    collect(&path, out, unreadable);
                } else {
                    out.push(path);
                }
            }
            Err(e) => unreadable.push((dir.to_owned(), e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::CaptureTarget;
    use quant_core::time::UtcDate;

    /// A tree of empty files at real capture paths. Contents are irrelevant here
    /// — this module reads paths, and the header check that would need real
    /// bytes belongs to the caller.
    struct Tree {
        root: PathBuf,
    }

    impl Tree {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!("quant-catalog-{name}"));
            let _ = std::fs::remove_dir_all(&root);
            Self { root }
        }

        fn touch(path: &Path) {
            std::fs::create_dir_all(path.parent().expect("a file has a parent"))
                .expect("create dirs");
            std::fs::write(path, b"").expect("write file");
        }

        fn segment(&self, symbol: &str, session: u8, day: u8, part: u32) {
            let target = CaptureTarget {
                exchange: Exchange::Binance,
                symbol: symbol.to_owned(),
                date: UtcDate {
                    year: 2026,
                    month: 8,
                    day,
                },
                session_id: [session; 16],
                part,
            };
            Self::touch(&target.file(&self.root));
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn a_sessions_segments_come_back_in_write_order() {
        let tree = Tree::new("write-order");
        // Written deliberately out of order, and across a month boundary within
        // the day numbers, to prove the sort is doing the work rather than the
        // filesystem's enumeration order happening to agree.
        tree.segment("BTCUSDT", 1, 23, 0);
        tree.segment("BTCUSDT", 1, 21, 0);
        tree.segment("BTCUSDT", 1, 22, 0);

        let catalog = catalog(&tree.root);
        assert_eq!(catalog.sessions.len(), 1);
        let days: Vec<u8> = catalog.sessions[0]
            .segments
            .iter()
            .map(|s| s.target.date.day)
            .collect();
        assert_eq!(days, vec![21, 22, 23]);
    }

    #[test]
    fn parts_within_a_day_sort_numerically_because_they_are_padded() {
        let tree = Tree::new("parts");
        for part in [10, 2, 1, 0] {
            tree.segment("BTCUSDT", 1, 21, part);
        }
        let catalog = catalog(&tree.root);
        let parts: Vec<u32> = catalog.sessions[0]
            .segments
            .iter()
            .map(|s| s.target.part)
            .collect();
        assert_eq!(parts, vec![0, 1, 2, 10]);
    }

    #[test]
    fn two_symbols_in_one_run_are_two_sessions() {
        // Even given the same session id, which is the case this guards: their
        // `ingest_seq` spaces are independent, so joining them would interleave
        // two instruments' deltas into one unusable stream.
        let tree = Tree::new("two-symbols");
        tree.segment("BTCUSDT", 7, 21, 0);
        tree.segment("ETHUSDT", 7, 21, 0);

        let catalog = catalog(&tree.root);
        assert_eq!(catalog.sessions.len(), 2);
        let mut symbols: Vec<&str> = catalog.sessions.iter().map(|s| s.symbol.as_str()).collect();
        symbols.sort_unstable();
        assert_eq!(symbols, vec!["BTCUSDT", "ETHUSDT"]);
        assert!(catalog.sessions.iter().all(|s| s.segments.len() == 1));
    }

    #[test]
    fn two_runs_of_one_symbol_are_two_sessions() {
        let tree = Tree::new("two-runs");
        tree.segment("BTCUSDT", 1, 21, 0);
        tree.segment("BTCUSDT", 2, 21, 0);

        let catalog = catalog(&tree.root);
        assert_eq!(catalog.sessions.len(), 2);
    }

    #[test]
    fn anything_that_is_not_a_capture_file_is_reported_not_skipped() {
        let tree = Tree::new("strays");
        tree.segment("BTCUSDT", 1, 21, 0);
        // The shape that actually turned up in the acceptance capture.
        Tree::touch(
            &tree
                .root
                .join("raw")
                .join("exchange=binance")
                .join("symbol=BTCUSDT")
                .join("date=2026-08-21")
                .join("session=01010101-0101-0101-0101-010101010101")
                .join("._part-00000.bin.zst"),
        );

        let catalog = catalog(&tree.root);
        assert_eq!(catalog.sessions.len(), 1);
        assert_eq!(catalog.sessions[0].segments.len(), 1);
        assert_eq!(catalog.strays.len(), 1);
    }

    #[test]
    fn an_empty_root_is_not_an_error() {
        let tree = Tree::new("empty");
        std::fs::create_dir_all(&tree.root).expect("create root");
        let catalog = catalog(&tree.root);
        assert!(catalog.sessions.is_empty());
        assert!(catalog.strays.is_empty());
        assert!(catalog.unreadable.is_empty());
    }
}
