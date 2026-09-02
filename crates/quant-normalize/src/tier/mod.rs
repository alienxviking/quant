//! The normalized tier on disk: Parquet, partitioned `exchange / symbol / date`.
//!
//! Per `docs/data-contract.md` §5 and the storage-tier table, this is the
//! **disposable** tier — everything here is a pure function of the raw capture
//! and can be deleted and rebuilt. That is a claim the tier has to earn: M2's
//! last criterion is that a replay *from Parquet* agrees with a replay *from
//! raw*, which is only checkable because [`read`] exists alongside [`write`].
//!
//! The layout is `exchange=…/symbol=…/date=…/<dataset>/part-00000.parquet`, with
//! one file per [`Dataset`] — `trades`, `book_deltas`, `book_snapshots`, `gaps`.
//! Hive-style `key=value` directories because every tool that will ever read this
//! (`DuckDB`, Polars, pandas, Spark, `ClickHouse`) discovers them by convention,
//! which is the same reason `quant-recorder::layout` uses them for the raw tier.
//!
//! The two interesting encoding decisions — money as `DECIMAL(18,8)` and the
//! instrument being absent — are argued in [`schema`].

pub mod partition;
pub mod provenance;
pub mod read;
pub mod schema;
pub mod write;

use std::path::{Path, PathBuf};

use quant_core::instrument::Exchange;
use quant_core::time::UtcDate;

pub use partition::{DayReport, PartitionWriter, WriteReport};
pub use provenance::Provenance;
pub use read::read_dataset;
pub use schema::{Dataset, MAX_MONEY_RAW, MONEY_PRECISION, MONEY_SCALE};
pub use write::{dataset_of, DatasetWriter, BATCH_ROWS};

/// Where one dataset file goes.
///
/// The inverse lives beside it in [`TierTarget::parse`], for the reason
/// `CaptureTarget` gives: a path parser written wherever it happens to be needed
/// is a second, unversioned copy of the partition scheme.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierTarget {
    pub exchange: Exchange,
    /// The venue's own symbol, verbatim — the same string the raw tier files
    /// under, so the two tiers line up directory for directory.
    pub symbol: String,
    pub date: UtcDate,
    pub dataset: Dataset,
}

impl TierTarget {
    /// Directory holding this dataset's parts.
    #[must_use]
    pub fn directory(&self, root: &Path) -> PathBuf {
        root.join("normalized")
            .join(format!("exchange={}", self.exchange))
            .join(format!("symbol={}", self.symbol))
            .join(format!("date={}", self.date))
            .join(self.dataset.dir())
    }

    /// Full path of the dataset file.
    #[must_use]
    pub fn file(&self, root: &Path) -> PathBuf {
        self.directory(root).join("part-00000.parquet")
    }

    /// Read a path back into the identity that produced it.
    #[must_use]
    pub fn parse(path: &Path) -> Option<Self> {
        let mut parts = path.components().rev().map(|c| c.as_os_str().to_str());
        let file = parts.next()??;
        let dataset = parts.next()??;
        let date = parts.next()??;
        let symbol = parts.next()??;
        let exchange = parts.next()??;

        if !file.ends_with(".parquet") {
            return None;
        }
        Some(Self {
            exchange: Exchange::from_name(exchange.strip_prefix("exchange=")?)?,
            symbol: symbol.strip_prefix("symbol=")?.to_owned(),
            date: quant_recorder::parse_utc_date(date.strip_prefix("date=")?)?,
            dataset: Dataset::from_dir(dataset)?,
        })
    }
}

/// Anything that can go wrong writing or reading the normalized tier.
#[derive(Debug)]
pub enum TierError {
    Io(std::io::Error),
    /// Arrow or Parquet refused what we handed it. Kept as one variant because
    /// every case is a bug in this module rather than something a caller can act
    /// on differently.
    Format(String),
    /// An event was pushed into the writer for a different dataset.
    WrongDataset {
        expected: &'static str,
        found: &'static str,
    },
    /// A price or size beyond what `DECIMAL(18,8)` can hold. Loud, per
    /// invariant 5 — never rounded, never truncated.
    MoneyOutOfRange(i64),
    /// A batch held more than `i32::MAX` book levels. Unreachable at
    /// [`BATCH_ROWS`], and checked rather than assumed.
    BatchTooLarge,
    /// A column was not the type the schema declares. Means the file was written
    /// by something else, or by an older version of this module.
    UnexpectedColumn {
        dataset: &'static str,
        column: &'static str,
    },
    /// A `Utf8` label this build does not know.
    UnknownLabel {
        column: &'static str,
        value: String,
    },
    /// The partition already holds a file written by a different capture
    /// session. See [`provenance`] — overwriting it would lose a day of data
    /// silently, and merging the two is not yet implemented.
    PartitionOwnedByAnother {
        path: String,
        existing: String,
        writing: String,
    },
}

impl core::fmt::Display for TierError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Format(e) => write!(f, "parquet: {e}"),
            Self::WrongDataset { expected, found } => {
                write!(f, "a {found} event was pushed into the {expected} writer")
            }
            Self::MoneyOutOfRange(raw) => write!(
                f,
                "fixed-point value {raw} exceeds DECIMAL({MONEY_PRECISION},{MONEY_SCALE})"
            ),
            Self::BatchTooLarge => write!(f, "more book levels in one batch than i32 can index"),
            Self::UnexpectedColumn { dataset, column } => {
                write!(f, "{dataset}.{column} is not the type the schema declares")
            }
            Self::UnknownLabel { column, value } => {
                write!(f, "unknown {column} label {value:?}")
            }
            Self::PartitionOwnedByAnother {
                path,
                existing,
                writing,
            } => write!(
                f,
                "{path} was written by session {existing}, not {writing};                  merging two sessions into one day partition is not implemented"
            ),
        }
    }
}

impl std::error::Error for TierError {}

impl From<std::io::Error> for TierError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<parquet::errors::ParquetError> for TierError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Format(e.to_string())
    }
}

impl From<arrow_schema::ArrowError> for TierError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::Format(e.to_string())
    }
}

#[cfg(test)]
mod tests;
