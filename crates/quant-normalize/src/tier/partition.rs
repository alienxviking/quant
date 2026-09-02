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

use super::schema::Dataset;
use super::write::{dataset_of, DatasetWriter};
use super::{TierError, TierTarget};

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
    /// Rows per dataset, indexed by [`Dataset::index`].
    pub rows: [u64; Dataset::ALL.len()],
}

/// Writes one session's events into `root/normalized/…`, rolling by day.
#[derive(Debug)]
pub struct PartitionWriter {
    root: PathBuf,
    exchange: Exchange,
    symbol: String,
    open: Option<OpenDay>,
    report: WriteReport,
}

/// The four files of the day currently being written.
#[derive(Debug)]
struct OpenDay {
    date: UtcDate,
    writers: Vec<DatasetWriter<BufWriter<File>>>,
    /// Where each writer is writing, and where it will be renamed to.
    paths: Vec<(PathBuf, PathBuf)>,
}

impl PartitionWriter {
    /// Prepare to write a session's events under `root`.
    #[must_use]
    pub fn new(root: &Path, exchange: Exchange, symbol: &str) -> Self {
        Self {
            root: root.to_owned(),
            exchange,
            symbol: symbol.to_owned(),
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
        open.writers[dataset_of(event).index()].push(event)
    }

    /// Close the open day, if any, and start `date`.
    fn roll(&mut self, date: UtcDate) -> Result<(), TierError> {
        self.close_open()?;

        let mut writers = Vec::with_capacity(Dataset::ALL.len());
        let mut paths = Vec::with_capacity(Dataset::ALL.len());
        for dataset in Dataset::ALL {
            let target = TierTarget {
                exchange: self.exchange,
                symbol: self.symbol.clone(),
                date,
                dataset,
            };
            let final_path = target.file(&self.root);
            std::fs::create_dir_all(target.directory(&self.root))?;
            let temp_path = final_path.with_extension("parquet.tmp");
            let file = BufWriter::new(File::create(&temp_path)?);
            writers.push(DatasetWriter::new(file, dataset)?);
            paths.push((temp_path, final_path));
        }
        self.open = Some(OpenDay {
            date,
            writers,
            paths,
        });
        Ok(())
    }

    fn close_open(&mut self) -> Result<(), TierError> {
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        let mut rows = [0_u64; Dataset::ALL.len()];
        for (i, (writer, (temp, final_path))) in
            open.writers.into_iter().zip(open.paths).enumerate()
        {
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
