//! The normalized tier on disk: Parquet, partitioned `exchange / symbol / date`.
//!
//! Per `docs/data-contract.md` §5 and the storage-tier table, this is the
//! **disposable** tier — everything here is a pure function of the raw capture
//! and can be deleted and rebuilt. That is a claim the tier has to earn: M2's
//! last criterion is that a replay *from Parquet* agrees with a replay *from
//! raw*, which is only checkable because [`read`] exists alongside [`write`].
//!
//! The layout is `exchange=…/symbol=…/date=…/<dataset>/part-NNNNN.parquet`, with
//! one file per [`Dataset`] — `trades`, `book_deltas`, `book_snapshots`, `gaps`.
//! Hive-style `key=value` directories because every tool that will ever read this
//! (`DuckDB`, Polars, pandas, Spark, `ClickHouse`) discovers them by convention,
//! which is the same reason `quant-recorder::layout` uses them for the raw tier.
//!
//! # Why a day has *parts*
//!
//! There is no session in that path, and there should not be — the tier is about
//! what the market did, and which of our capture runs saw it is a capture-side
//! concern. But a recorder restart makes a new session, and two sessions can
//! cover one symbol-day. M2.d refused that case rather than losing it, and left
//! the merge as work owing.
//!
//! A restart is **sequential** — `ops/supervise.sh` runs the recorder in the
//! foreground of its restart loop and reads its exit code before respawning, so
//! the dead process is dead before the next one starts. A merged day is therefore
//! a **concatenation**, not an interleave, and a file boundary is the exact and
//! free encoding of a concatenation: one part per contributing session, in
//! capture order. Nothing inside a row changes, which is forced rather than
//! chosen — `normalize --check` compares whole `MarketEvent` values with `==`,
//! and `EventMeta` includes `ingest_seq`, so renumbering rows would fail at the
//! first event with no tolerance available.
//!
//! The ordering key is `(part, ingest_seq)`, lexicographic. Within a part
//! `ingest_seq` is unique and strictly increasing; across parts the index is
//! distinct by construction *and* is enforced to be capture order, so a reader
//! that concatenates parts in index order gets the stream a single uninterrupted
//! consumer would have seen.
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
pub use read::{discover_days, read_dataset, DatasetStream, HistoricalSource, TierReplay};
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
    /// Which contributing session's slice of the day this is, from zero.
    ///
    /// Not a session id: the index says *order*, which is the only thing a
    /// reader needs, and the id is in the footer for anyone who wants identity.
    pub part: u32,
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

    /// Full path of this part's file.
    ///
    /// Zero-padded so lexical order is numeric order, which is what lets a
    /// reader sort names as text and get capture order. The raw tier pins the
    /// same property for the same reason.
    #[must_use]
    pub fn file(&self, root: &Path) -> PathBuf {
        self.directory(root).join(part_file_name(self.part))
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

        Some(Self {
            exchange: Exchange::from_name(exchange.strip_prefix("exchange=")?)?,
            symbol: symbol.strip_prefix("symbol=")?.to_owned(),
            date: quant_recorder::parse_utc_date(date.strip_prefix("date=")?)?,
            dataset: Dataset::from_dir(dataset)?,
            part: part_index(file)?,
        })
    }
}

/// The name [`TierTarget::file`] gives part `n`.
#[must_use]
pub fn part_file_name(part: u32) -> String {
    format!("part-{part:05}.parquet")
}

/// Read a part index back out of a file name, or `None` if this is not one of
/// ours.
///
/// Deliberately the exact inverse of [`part_file_name`] and not a pattern that
/// merely looks close: it must reject `part-00000.parquet.tmp`, which is a real
/// file that exists beside published parts while a partition is being written
/// and outlives a `SIGKILL`. Before parts existed the reader opened one exact
/// path and the question never arose; enumerating a directory is what makes a
/// stray name reachable, so the enumeration is the thing that has to be strict.
/// That is not a heuristic — recognising the names we ourselves emit is the same
/// discipline `TierTarget::parse` already follows.
#[must_use]
pub fn part_index(file_name: &str) -> Option<u32> {
    let digits = file_name.strip_prefix("part-")?.strip_suffix(".parquet")?;
    if digits.len() != 5 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
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
    /// The four dataset directories disagree about which parts this day holds.
    ///
    /// Before parts, ownership was checked per dataset path, and that was
    /// load-bearing rather than incidental: a half-finished cleanup that removed
    /// `trades/` but left `gaps/` would otherwise let a new session take an index
    /// another session still occupies in the datasets nobody looked at.
    DayPartsInconsistent {
        date: String,
        detail: String,
    },
    /// This day's parts are not numbered contiguously from zero.
    ///
    /// A hole means a published part was removed — or that a rename died between
    /// unlinking the old file and putting the new one in place. Either way the
    /// next index cannot be derived by counting, and guessing would overwrite a
    /// session that is still there. "Cannot tell" is not permission.
    DayPartsNotContiguous {
        date: String,
        found: String,
    },
    /// A published part whose capture session cannot be read.
    PartOriginUnknown {
        path: String,
    },
    /// A part's position in the day cannot be established, so it is not written.
    ///
    /// Ordering parts needs the venue's own update-id span at both ends. A part
    /// written before spans were recorded does not have one. The tier is
    /// disposable, so the remedy is cheap and is named in the message: delete
    /// the day and re-derive every session that covers it.
    PartOrderUnknowable {
        date: String,
        detail: String,
    },
    /// A day was asked for and holds no parts.
    ///
    /// An error rather than an empty stream: before parts, a missing day failed
    /// at `File::open`, and turning that into a shorter stream would let a check
    /// that compares two replays agree about data neither of them read.
    NoPartsForDay {
        date: String,
    },
    /// Two parts of one day cover overlapping venue update ids.
    ///
    /// A restart is sequential — `supervise.sh` reads the dead recorder's exit
    /// code before starting the next — so two sessions on one symbol-day hold
    /// disjoint stretches of the venue's sequence. Overlap means they were
    /// *concurrent*: two recorders on one symbol at one time, and there is no
    /// ordering of two simultaneous recordings of the same messages that is the
    /// truth. So this refuses rather than picking one.
    ///
    /// It deliberately says nothing about which part arrived first. Parts are
    /// published in the order sessions happen to be normalized, which is catalog
    /// order, which is a v4 UUID sort — arrival order carries no information
    /// about the market and is not evidence of anything. Order comes from the
    /// spans at read time; this check only establishes that an order exists.
    PartsConcurrent {
        date: String,
        published: (u64, u64),
        incoming: (u64, u64),
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
            Self::DayPartsInconsistent { date, detail } => write!(
                f,
                "the dataset directories for {date} disagree about which parts exist: {detail}"
            ),
            Self::DayPartsNotContiguous { date, found } => write!(
                f,
                "{date} holds parts {found}, which is not contiguous from 0 -- a part was removed, so the next index cannot be derived"
            ),
            Self::PartOriginUnknown { path } => {
                write!(f, "cannot establish which session wrote {path}")
            }
            Self::PartOrderUnknowable { date, detail } => write!(
                f,
                "cannot order the parts of {date}: {detail}; delete this day and re-derive every session covering it"
            ),
            Self::NoPartsForDay { date } => {
                write!(f, "{date} holds no parts under the normalized tier")
            }
            Self::PartsConcurrent {
                date,
                published,
                incoming,
            } => write!(
                f,
                "on {date} a published part covers venue update ids {}..={} and the incoming part covers {}..={}; these overlap, so the two sessions recorded the same messages at the same time and cannot be concatenated",
                published.0, published.1, incoming.0, incoming.1
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

/// The published part indices in one dataset directory, ascending.
///
/// Only names [`part_file_name`] could have produced. A directory listing is
/// what makes a stray name reachable at all -- `part-00000.parquet.tmp` is a
/// real file that sits beside published parts while a day is being written, and
/// survives a `SIGKILL` -- so the enumeration is where strictness belongs. A
/// missing directory is an empty list rather than an error: the day simply has
/// no parts yet.
fn published_parts(directory: &Path) -> Vec<u32> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    let mut found: Vec<u32> = entries
        .flatten()
        .filter_map(|e| part_index(e.file_name().to_str()?))
        .collect();
    found.sort_unstable();
    found
}
