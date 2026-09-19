//! Writing a session's events into date partitions.
//!
//! # Which day a record is filed under
//!
//! **The day the raw tier filed it under** — the caller passes it in, read off
//! the segment the record came out of. Not re-derived from `local_recv_ts`.
//!
//! The two agree almost always, because `quant-recorder::segment` rolls on the
//! record's timestamp. They differ in exactly the case that rule carves out: a
//! record stamped *before* the open segment's day, after an NTP step, is written
//! to the open segment on purpose and counted as `backdated_records` rather than
//! reopening a sealed day. Re-deriving the day here would file that record
//! somewhere the raw tier did not, and the two tiers would silently stop lining
//! up — which would take with it the only cheap cross-check between them, that a
//! date partition holds the same events on both sides.
//!
//! So the rule is not reimplemented. It is inherited.
//!
//! # Why every day gets all four files, even empty ones
//!
//! An absent file and an empty file are different claims. "There were no gaps
//! that day" is information; "no gaps file exists" could equally mean the
//! normalizer crashed before writing it.
//!
//! # Why a partition is published by rename
//!
//! A Parquet file is only readable once its footer is written, but a *truncated*
//! one at the final path would still be found by every tool that globs the
//! directory. So each dataset is written to a `.tmp` sibling and renamed into
//! place after its footer lands. A reader therefore never sees a partial file at
//! a real path — it sees the old one or the new one.

use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use quant_core::event::MarketEvent;
use quant_core::instrument::Exchange;
use quant_core::time::UtcDate;

use super::provenance::{self, Provenance};
use super::schema::Dataset;
use super::write::{dataset_of, DatasetWriter};
use super::{published_parts, TierError, TierTarget};

/// Rows written, per day and in total.
#[derive(Debug, Default, Clone)]
pub struct WriteReport {
    /// One entry per day, in the order the days were written.
    pub days: Vec<DayReport>,
}

impl WriteReport {
    /// Total rows across every day and dataset.
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.days.iter().map(|d| d.rows.iter().sum::<u64>()).sum()
    }

    /// Total rows for one dataset across every day.
    #[must_use]
    pub fn rows_in(&self, dataset: Dataset) -> u64 {
        self.days.iter().map(|d| d.rows[dataset.index()]).sum()
    }
}

/// What one date partition received.
#[derive(Debug, Clone)]
pub struct DayReport {
    pub date: UtcDate,
    /// Which part of the day this session contributed.
    ///
    /// Non-zero means the day is shared: another session already published a
    /// slice of it. Reported rather than merely recorded, because a reader that
    /// opens only `part-00000` would under-read such a day and say nothing.
    pub part: u32,
    /// Rows per dataset, indexed by [`Dataset::index`].
    pub rows: [u64; Dataset::ALL.len()],
}

/// Writes one session's events into `root/normalized/…`, rolling by day.
#[derive(Debug)]
pub struct PartitionWriter {
    root: PathBuf,
    exchange: Exchange,
    symbol: String,
    /// The capture session these events came from, stamped into every file and
    /// checked against whatever is already there.
    session_id: [u8; 16],
    open: Option<OpenDay>,
    report: WriteReport,
}

/// The four files of the day currently being written.
#[derive(Debug)]
struct OpenDay {
    date: UtcDate,
    /// Which part of the day this session is contributing.
    part: u32,
    writers: Vec<DatasetWriter<BufWriter<File>>>,
    /// Where each writer is writing, and where it will be renamed to.
    paths: Vec<(PathBuf, PathBuf)>,
    /// The venue's update-id span over this part's book events, which is what
    /// orders it against the parts already published. `None` until a book event
    /// is seen, and a part that never sees one cannot be ordered — see
    /// [`TierError::PartOrderUnknowable`].
    book_seq: Option<(u64, u64)>,
}

impl PartitionWriter {
    /// Prepare to write a session's events under `root`.
    #[must_use]
    pub fn new(root: &Path, exchange: Exchange, symbol: &str, session_id: [u8; 16]) -> Self {
        Self {
            root: root.to_owned(),
            exchange,
            symbol: symbol.to_owned(),
            session_id,
            open: None,
            report: WriteReport::default(),
        }
    }

    /// File one event under the date partition the raw tier used for it.
    ///
    /// `date` is `None` only if the replay produced an event without having a
    /// segment open, which cannot happen; the record's own day is used as a
    /// last resort rather than panicking, because dropping a real event would be
    /// the worse failure.
    pub fn push(&mut self, event: &MarketEvent, date: Option<UtcDate>) -> Result<(), TierError> {
        let date = date.unwrap_or_else(|| event.meta().local_recv_ts.utc_date());
        if self.open.as_ref().is_none_or(|d| d.date != date) {
            self.roll(date)?;
        }
        let open = self.open.as_mut().expect("just opened");
        if let Some(seq) = book_seq_of(event) {
            open.book_seq = Some(match open.book_seq {
                None => (seq, seq),
                Some((first, last)) => (first.min(seq), last.max(seq)),
            });
        }
        open.writers[dataset_of(event).index()].push(event)
    }

    /// Close the open day, if any, and start `date`.
    fn roll(&mut self, date: UtcDate) -> Result<(), TierError> {
        self.close_open()?;

        let part = self.place(date)?;
        let mut writers = Vec::with_capacity(Dataset::ALL.len());
        let mut paths = Vec::with_capacity(Dataset::ALL.len());
        for dataset in Dataset::ALL {
            let target = TierTarget {
                exchange: self.exchange,
                symbol: self.symbol.clone(),
                date,
                dataset,
                part,
            };
            let final_path = target.file(&self.root);
            std::fs::create_dir_all(target.directory(&self.root))?;
            let temp_path = final_path.with_extension("parquet.tmp");
            let file = BufWriter::new(File::create(&temp_path)?);
            let provenance = Provenance {
                session_id: self.session_id,
                exchange: self.exchange,
                symbol: self.symbol.clone(),
                date,
            };
            writers.push(DatasetWriter::new(file, dataset, Some(&provenance))?);
            paths.push((temp_path, final_path));
        }
        self.open = Some(OpenDay {
            date,
            part,
            writers,
            paths,
            book_seq: None,
        });
        Ok(())
    }

    /// Decide which part of `date` this session is, and refuse rather than guess.
    ///
    /// Three questions, in order, because each makes the next one answerable.
    ///
    /// **Do the four datasets agree on what is published?** Ownership used to be
    /// checked per dataset path, which quietly guarded a case that is easy to
    /// miss: a half-finished cleanup that removes `trades/` but leaves `gaps/`.
    /// Enumerating one dataset and trusting the rest would let a new session take
    /// an index another session still holds in the three nobody looked at.
    ///
    /// **Are the indices contiguous from zero?** A hole means a part was removed,
    /// or that a rename died between unlinking the old file and putting the new
    /// one there — a window this very function's `close_open` has. Deriving the
    /// next index by *counting* would then hand back an index that is already
    /// occupied and overwrite it. Max-plus-one plus a contiguity requirement
    /// cannot.
    ///
    /// **Is one of them ours?** Re-deriving a session must replace its own part
    /// in place, or every re-derive would append another copy. That is what keeps
    /// the tier disposable.
    fn place(&self, date: UtcDate) -> Result<u32, TierError> {
        let mut per_dataset = Vec::with_capacity(Dataset::ALL.len());
        for dataset in Dataset::ALL {
            let target = TierTarget {
                exchange: self.exchange,
                symbol: self.symbol.clone(),
                date,
                dataset,
                part: 0,
            };
            per_dataset.push(published_parts(&target.directory(&self.root)));
        }

        let indices = per_dataset[0].clone();
        for (dataset, found) in Dataset::ALL.iter().zip(&per_dataset) {
            if *found != indices {
                return Err(TierError::DayPartsInconsistent {
                    date: date.to_string(),
                    detail: format!(
                        "{} holds {found:?} but {} holds {indices:?}",
                        dataset.dir(),
                        Dataset::ALL[0].dir()
                    ),
                });
            }
        }

        let n = u32::try_from(indices.len()).unwrap_or(u32::MAX);
        if indices.iter().copied().ne(0..n) {
            return Err(TierError::DayPartsNotContiguous {
                date: date.to_string(),
                found: format!("{indices:?}"),
            });
        }

        for &index in &indices {
            let target = TierTarget {
                exchange: self.exchange,
                symbol: self.symbol.clone(),
                date,
                dataset: Dataset::ALL[0],
                part: index,
            };
            let path = target.file(&self.root);
            match Provenance::session_of(&path)? {
                Some(id) if id == self.session_id => return Ok(index),
                Some(_) => {}
                None => {
                    return Err(TierError::PartOriginUnknown {
                        path: path.display().to_string(),
                    })
                }
            }
        }
        Ok(n)
    }

    /// Refuse to publish a part that overlaps one already there.
    ///
    /// Only reached when this session is appending — a first part has nothing to
    /// be disjoint from, which is why the single-session case never runs this at
    /// all.
    ///
    /// **This used to require that the incoming part start after every published
    /// part ended, and that was wrong.** A part index is assigned by `place` in
    /// arrival order, arrival order is the order `catalog` yields sessions, and
    /// `catalog` sorts by paths whose session component is a **v4 UUID**. So for
    /// two perfectly sequential recorders — one restart, the ordinary case this
    /// whole slice exists to serve — which one landed at part 0 was a coin flip,
    /// and the earlier session lost that flip half the time. Losing it refused
    /// the write, and `normalize` abandons the writer on a refusal, so the rest
    /// of that session's days went unwritten too. The check was rejecting the
    /// case it was built for.
    ///
    /// What is actually checkable here is **disjointness**, and that is all this
    /// asks. Sessions on one symbol-day hold non-overlapping stretches of the
    /// venue's sequence because a restart is sequential: `supervise.sh` reads the
    /// dead recorder's exit code before starting the next. Overlap means they ran
    /// at the same time, which is the one case with no true ordering and is what
    /// the error now says. *Which* stretch came first is read off the spans at
    /// read time, where it is a fact about the market rather than about the order
    /// we happened to derive things in.
    ///
    /// The comparison is on the venue's update ids and deliberately not on
    /// `local_recv_ts`; [`provenance`] carries the argument.
    fn check_disjoint(
        &self,
        date: UtcDate,
        part: u32,
        span: Option<(u64, u64)>,
    ) -> Result<(), TierError> {
        if part == 0 {
            return Ok(());
        }
        let Some((first, last)) = span else {
            return Err(TierError::PartOrderUnknowable {
                date: date.to_string(),
                detail: "the incoming part saw no book event, so it has no venue sequence"
                    .to_owned(),
            });
        };
        for index in 0..part {
            let target = TierTarget {
                exchange: self.exchange,
                symbol: self.symbol.clone(),
                date,
                dataset: Dataset::ALL[0],
                part: index,
            };
            let path = target.file(&self.root);
            let Some(published) = Provenance::book_span_of(&path)? else {
                return Err(TierError::PartOrderUnknowable {
                    date: date.to_string(),
                    detail: format!(
                        "{} records no venue sequence, so it predates part ordering",
                        path.display()
                    ),
                });
            };
            // Disjoint in either direction is fine; only an intersection is a
            // defect. Equality at a boundary counts as overlap: a shared update
            // id means both sessions saw the same message, which is the very
            // thing that cannot happen sequentially.
            if first <= published.1 && published.0 <= last {
                return Err(TierError::PartsConcurrent {
                    date: date.to_string(),
                    published,
                    incoming: (first, last),
                });
            }
        }
        Ok(())
    }

    fn close_open(&mut self) -> Result<(), TierError> {
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        // Before anything is published. A refusal here leaves four `.tmp` files
        // and no change to what a reader can see, which is the same failure shape
        // as abandoning a day -- and it is why the check is here rather than at
        // `roll`, where only the first row of the part is known and its span is
        // not.
        self.check_disjoint(open.date, open.part, open.book_seq)?;

        let mut rows = [0_u64; Dataset::ALL.len()];
        for (i, (mut writer, (temp, final_path))) in
            open.writers.into_iter().zip(open.paths).enumerate()
        {
            // Facts about the rows, so they are appended now rather than handed
            // to the constructor. Written into every dataset of the part, the
            // empty ones included: the span belongs to the *part*, and a day with
            // no gaps at all is ordinary -- keying the span to each file's own
            // rows would leave that day's `gaps` file unable to say where it
            // belongs, and refuse the next session on a perfectly normal day.
            writer.append_key_value(provenance::PART_KEY, open.part.to_string());
            if let Some((first, last)) = open.book_seq {
                writer.append_key_value(provenance::BOOK_SEQ_FIRST_KEY, first.to_string());
                writer.append_key_value(provenance::BOOK_SEQ_LAST_KEY, last.to_string());
            }
            // `finish` writes the footer; only after that is the file a Parquet
            // file at all, which is why the rename comes second -- and why the
            // row count comes from `finish` rather than from before it.
            let (sink, written) = writer.finish()?;
            rows[i] = written;
            sink.into_inner().map_err(|e| {
                TierError::Io(std::io::Error::other(format!(
                    "flushing {}: {e}",
                    temp.display()
                )))
            })?;
            // Windows will not rename onto an existing file, so a re-derive has
            // to unlink first. The window where neither path exists is why this
            // tier is the disposable one.
            if final_path.exists() {
                std::fs::remove_file(&final_path)?;
            }
            std::fs::rename(&temp, &final_path)?;
        }
        self.report.days.push(DayReport {
            date: open.date,
            part: open.part,
            rows,
        });
        Ok(())
    }

    /// Close the last day and return what was written.
    pub fn finish(mut self) -> Result<WriteReport, TierError> {
        self.close_open()?;
        Ok(self.report)
    }

    /// Give up, leaving no partial partition behind.
    ///
    /// Used when the replay hits a discontinuity it will not write across. The
    /// already-published days stay — they are complete and correct — but the
    /// day in progress is removed rather than left as a short file that looks
    /// like a quiet day.
    #[must_use]
    pub fn abandon(mut self) -> WriteReport {
        if let Some(open) = self.open.take() {
            for (temp, _) in open.paths {
                let _ = std::fs::remove_file(temp);
            }
        }
        self.report
    }
}

impl Dataset {
    /// Dense index, for arrays of per-dataset counters.
    ///
    /// Not persisted anywhere, so it is free to change; adding a variant makes
    /// this fail to compile until it is given a slot.
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::Trades => 0,
            Self::BookDeltas => 1,
            Self::BookSnapshots => 2,
            Self::Gaps => 3,
        }
    }
}

/// The venue's own update id for an event, where it has one.
///
/// Deltas and snapshots only. A trade id is a different namespace, and a span
/// that mixed the two would compare numbers that do not mean the same thing.
/// `BookDelta` carries the range in the M0 event contract, so reading it here
/// adds no venue knowledge to this crate -- the same argument that let
/// `quant-book` be venue-agnostic.
const fn book_seq_of(event: &MarketEvent) -> Option<u64> {
    match event {
        MarketEvent::BookDelta(d) => Some(d.final_update_id),
        MarketEvent::BookSnapshot(s) => Some(s.last_update_id),
        MarketEvent::Trade(_) | MarketEvent::Gap(_) => None,
    }
}
