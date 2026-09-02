//! The normalized tier: raw venue bytes back out as ordered [`MarketEvent`]s.
//!
//! Two things live here, and the split matters.
//!
//! [`SessionReplay`] joins a capture session's segments into one stream of
//! events. That is the reusable artifact — the Parquet writer at M2.d and the
//! engine's historical and replay sources at M3 all want exactly this and nothing
//! more.
//!
//! [`replay_session`] is the first consumer: it drives a [`Book`] with that
//! stream and checks the invariants at every tick, which is M2's acceptance
//! criterion. It is a consumer and not the point.
//!
//! # Why joining the segments is a milestone of its own
//!
//! `ingest_seq` and the venue's update-id chain both span a *session*, not a
//! file, so a UTC day boundary is a filing decision and nothing else. Replaying
//! one file at a time starts each day unanchored, which cost days 2 through 8 of
//! the acceptance capture between 7,000 and 34,000 dropped deltas apiece while
//! each waited for an hourly snapshot it should never have needed. Day 1 dropped
//! none, because its reconnect snapshot arrives 300 ms in. That difference is
//! the artifact this crate exists to remove.
//!
//! # Why the invalidation rule lives here
//!
//! A [`Break`] — an unreadable segment, a torn file, a hole in `ingest_seq` —
//! means the events after it do not continue the ones before it. The book must be
//! cleared, exactly as a recorded `Gap` clears it, and that obligation is
//! discharged in [`replay_session`] rather than documented for callers. M2.b
//! already established why: the buffering rule was put inside `Book` because a
//! caller who has to remember will forget, and this is the same rule wearing a
//! different hat.

pub mod replay;
pub mod tier;

use std::path::Path;

use quant_book::{Book, BookStats, Outcome};
use quant_core::event::MarketEvent;
use quant_core::instrument::InstrumentId;
use quant_recorder::SessionFiles;

pub use replay::{Break, BreakKind, ReplayItem, ReplayStats, SessionReplay};
pub use tier::{Dataset, DatasetWriter, PartitionWriter, TierError, TierTarget, WriteReport};

/// What a replayed session did, and whether it can be trusted.
#[derive(Debug, Clone, Default)]
pub struct SessionSummary {
    pub replay: ReplayStats,
    pub book: BookStats,
    /// Ticks on which the book was live and passed [`Book::check`]. The number
    /// the acceptance criterion is actually about.
    pub checked_live: u64,
    /// Ticks on which there was nothing to check, because a gap or a break had
    /// invalidated the book and no snapshot had re-anchored it yet.
    pub checked_dark: u64,
    /// Full `O(n)` scans run — at every snapshot, and once at the end.
    pub audits: u64,
    pub violations: u64,
    /// Kept in full only for the first. A systematic defect over seventy million
    /// frames would otherwise print seventy million lines and bury itself, which
    /// is the same reason `quant-verify` caps its findings.
    pub first_violation: Option<String>,
    pub first_break: Option<String>,
}

impl SessionSummary {
    /// Whether the reconstruction held.
    ///
    /// Breaks are deliberately **not** disqualifying on their own. A torn last
    /// segment is the ordinary signature of a killed recorder, and the book
    /// handled it correctly by clearing; what would be a failure is the book
    /// carrying on across it, which is what `violations` counts.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.violations == 0 && self.book.broken == 0
    }
}

/// A session read once: reconstructed, and optionally written out.
#[derive(Debug)]
pub struct Normalized {
    pub summary: SessionSummary,
    /// What reached disk, when an output root was given.
    pub written: Option<WriteReport>,
    /// Why the write stopped early, if it did. The summary still describes
    /// everything that was read — the two answer different questions.
    pub write_error: Option<String>,
}

/// Replay a whole session through a book, checking the invariants at every tick.
///
/// `check()` is the `O(1)` crossed-book test and runs on every event, because
/// "every tick" is the criterion. `audit()` scans both sides and runs at each
/// snapshot and at the end — per tick it would be tens of billions of comparisons
/// for properties that hold by construction.
#[must_use]
pub fn replay_session(files: &SessionFiles, instrument: InstrumentId) -> SessionSummary {
    normalize_session(files, instrument, None).summary
}

/// Replay a session, and write the normalized tier while doing it.
///
/// One pass over the raw bytes for both jobs. Reading three gigabytes twice to
/// answer two questions about the same events would be the obvious alternative
/// and the wrong one — and worse, it would mean two loops applying events to a
/// book, which is exactly the drift this crate was built to remove.
///
/// # When the write stops
///
/// A [`Break`] means the events after it do not continue the ones before it. The
/// book handles that by clearing, but a *file* cannot: a partition written
/// across a discontinuity looks continuous to everyone who reads it afterwards,
/// and nothing in Parquet can say otherwise. Since raw is the source of truth
/// and this tier is disposable, the right answer is to stop, fix the read, and
/// re-derive — not to bake the hole in.
///
/// The exception is a torn tail on the **last** segment, which is the ordinary
/// signature of a killed recorder: everything before it is intact and nothing
/// follows it. That is the same distinction `quant-verify` draws when it calls a
/// missing trailer a warning on the final segment and an error anywhere else.
#[must_use]
pub fn normalize_session(
    files: &SessionFiles,
    instrument: InstrumentId,
    out: Option<&Path>,
) -> Normalized {
    let mut book = Book::new();
    let mut summary = SessionSummary::default();
    let mut writer = out.map(|root| PartitionWriter::new(root, files.exchange, &files.symbol));
    let mut write_error = None;
    let mut written = None;
    // `while let` rather than `for`, because the iterator owns the counters this
    // has to read once the stream is exhausted, and collecting instead would hold
    // seventy million events in memory to learn nine numbers.
    let mut replay = SessionReplay::open(files, instrument);

    // `while let` rather than `for … in replay.by_ref()`, because the partition a
    // record belongs to is a property of the *segment* it was read from, which
    // only the replay knows — so the loop body has to be able to ask it.
    while let Some(item) = replay.next() {
        let event = match item {
            ReplayItem::Break(brk) => {
                // The rule this function exists to remember. See the crate docs.
                if summary.first_break.is_none() {
                    summary.first_break = Some(brk.to_string());
                }
                book.invalidate();
                if !tolerable(&brk, files) {
                    if let Some(w) = writer.take() {
                        write_error = Some(brk.to_string());
                        written = Some(w.abandon());
                    }
                }
                continue;
            }
            ReplayItem::Event(event) => event,
        };

        if let Some(w) = writer.as_mut() {
            if let Err(e) = w.push(&event, replay.segment_date()) {
                write_error = Some(e.to_string());
                if let Some(w) = writer.take() {
                    written = Some(w.abandon());
                }
            }
        }

        let is_snapshot = matches!(event, MarketEvent::BookSnapshot(_));
        if let Outcome::Broken { expected, found } = book.apply(&event) {
            note(
                &mut summary,
                format!("chain break: expected U={expected}, found {found}"),
            );
        }

        match book.check() {
            Ok(()) => {
                if book.is_live() {
                    summary.checked_live += 1;
                } else {
                    summary.checked_dark += 1;
                }
            }
            Err(v) => note(&mut summary, v.to_string()),
        }

        if is_snapshot {
            summary.audits += 1;
            if let Err(v) = book.audit() {
                note(&mut summary, format!("audit: {v}"));
            }
        }
    }

    summary.audits += 1;
    if let Err(v) = book.audit() {
        note(&mut summary, format!("final audit: {v}"));
    }

    summary.book = book.stats();
    summary.replay = replay.stats();

    if let Some(w) = writer {
        match w.finish() {
            Ok(report) => written = Some(report),
            Err(e) => write_error = Some(e.to_string()),
        }
    }

    Normalized {
        summary,
        written,
        write_error,
    }
}

/// Whether a break can be written across.
///
/// Only one can: a torn tail on the final segment. See [`normalize_session`].
fn tolerable(brk: &Break, files: &SessionFiles) -> bool {
    matches!(brk.kind, BreakKind::Torn(_))
        && files
            .segments
            .last()
            .is_some_and(|last| last.path == brk.segment)
}

fn note(summary: &mut SessionSummary, message: String) {
    summary.violations += 1;
    if summary.first_violation.is_none() {
        summary.first_violation = Some(message);
    }
}

#[cfg(test)]
mod tests;
