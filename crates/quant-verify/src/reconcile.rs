//! Cross-checking the metadata index against the files it claims to describe.
//!
//! # Why this is a flag and not the default
//!
//! The data contract makes raw the source of truth and Postgres merely an index.
//! `FileTrailer`'s own documentation commits to the consequence: *"if validating a
//! capture required the database, that ordering would be inverted and a raw file
//! would stop being self-describing."* So every check that decides whether a
//! capture may be trusted runs with no database at all, and this module only ever
//! adds findings about the **index**.
//!
//! # What it is actually worth
//!
//! The frame count is claimed independently by two things that never spoke to each
//! other: the file's own trailer, written by the writer thread at seal time, and
//! the `capture_segments` row, written by a separate task over a bounded channel.
//! Agreement between them is real evidence. It is also the check that would have
//! caught the metadata tier silently dropping segment reports under load, which is
//! precisely the failure its `try_send` design makes possible on purpose.
//!
//! # What it deliberately does not do
//!
//! It does not walk the index looking for rows whose files are missing. That
//! direction needs a query this crate would have to add, and on any machine that
//! has run the `quant-meta` integration tests it would report every test fixture as
//! a missing capture -- a tool that is noisy on a developer box is a tool that gets
//! ignored on a production one. Sessions on disk are the population that matters
//! here; auditing the index for orphans is a separate job with a separate audience.

use quant_meta::connect;
use quant_meta::store::MetaError;
use uuid::Uuid;

use crate::discover::Session;
use crate::finding::{code, Report, Where};
use crate::session::SegmentOutcome;

/// One session's verified segments, paired with the session they came from.
#[derive(Debug)]
pub struct Verified<'a> {
    pub session: &'a Session,
    pub segments: Vec<SegmentOutcome>,
}

/// Compare what is on disk against what the index says about it.
pub async fn reconcile(
    url: &str,
    verified: &[Verified<'_>],
    report: &mut Report,
) -> Result<(), MetaError> {
    let (meta, driver) = connect(url).await?;
    let result = compare(&meta, verified, report).await;
    // The driver task owns the socket; nothing else will stop it.
    driver.abort();
    result
}

async fn compare(
    meta: &quant_meta::Meta,
    verified: &[Verified<'_>],
    report: &mut Report,
) -> Result<(), MetaError> {
    for entry in verified {
        let id = Uuid::from_bytes(entry.session.session_id);
        let at = Where::session(entry.session.session_id);
        let rows = meta.segments_for(id).await?;

        if rows.is_empty() {
            report.warn(
                code::NOT_INDEXED,
                at,
                format!(
                    "{} segments on disk and no rows in capture_segments: the recorder \
                     ran without a database, or the metadata task lost them",
                    entry.segments.len()
                ),
            );
            continue;
        }

        if rows.len() != entry.segments.len() {
            report.error(
                code::INDEX_DISAGREES,
                at.clone(),
                format!(
                    "index has {} segments, disk has {}",
                    rows.len(),
                    entry.segments.len()
                ),
            );
        }

        // Matched on path rather than on position, so a missing row in the middle
        // is reported as a missing row rather than shifting every comparison after
        // it and reporting all of them.
        for outcome in &entry.segments {
            let Some((_, indexed_frames, _)) = rows.iter().find(|(path, _, _)| {
                // The recorder writes the path it used, which may differ from the
                // one the verifier was pointed at (a different mount, a copy), so
                // the file name plus its partition is the honest comparison.
                same_file(path, &outcome.path)
            }) else {
                report.error(
                    code::INDEX_DISAGREES,
                    at.clone(),
                    format!("no index row for {}", outcome.path),
                );
                continue;
            };

            let indexed = u64::try_from(*indexed_frames).unwrap_or(0);
            if indexed != outcome.frames {
                report.error(
                    code::INDEX_DISAGREES,
                    at.clone(),
                    format!(
                        "index says {indexed} frames for {} but {} were read",
                        outcome.path, outcome.frames
                    ),
                );
            }
        }
    }
    Ok(())
}

/// Whether two recorded paths name the same capture file.
///
/// Compares the last three components -- `date=…/session=…/part-N` -- because that
/// triple is a file's identity in the §5 layout, and the data root above it is an
/// operational detail that legitimately differs between the machine that recorded
/// and the machine that verifies.
fn same_file(a: &str, b: &str) -> bool {
    tail(a) == tail(b)
}

fn tail(path: &str) -> Vec<&str> {
    let mut parts: Vec<&str> = path
        .split(['/', '\\'])
        .filter(|p| !p.is_empty())
        .rev()
        .take(3)
        .collect();
    parts.reverse();
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_is_identified_by_its_partition_not_its_data_root() {
        // The recorder may have written under /var/lib/quant and the verifier may
        // be pointed at a copy under ./data. Same capture.
        let recorded = "/var/lib/quant/raw/exchange=binance/symbol=BTCUSDT/\
                        date=2026-07-29/session=abc/part-00000.bin.zst";
        let local = r"data\raw\exchange=binance\symbol=BTCUSDT\date=2026-07-29\session=abc\part-00000.bin.zst";
        assert!(same_file(recorded, local));
    }

    #[test]
    fn different_parts_and_days_are_different_files() {
        let base =
            "d/raw/exchange=binance/symbol=BTCUSDT/date=2026-07-29/session=abc/part-00000.bin.zst";
        let next_part =
            "d/raw/exchange=binance/symbol=BTCUSDT/date=2026-07-29/session=abc/part-00001.bin.zst";
        let next_day =
            "d/raw/exchange=binance/symbol=BTCUSDT/date=2026-07-30/session=abc/part-00000.bin.zst";
        assert!(!same_file(base, next_part));
        assert!(!same_file(base, next_day));
    }
}
