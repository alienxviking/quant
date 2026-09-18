//! Reading the normalized tier back into events.
//!
//! The mirror of [`write`](super::write), and the half that makes M2's last
//! criterion checkable: a replay from Parquet has to agree with a replay from
//! raw, which is only a statement if the events come back out.
//!
//! # Why the instrument is a parameter
//!
//! For the same reason it is not a column. `InstrumentId` is a registry index
//! and is never persisted, so identity is reattached on read from the partition
//! path — the caller resolves `(exchange, symbol)` through its own registry and
//! passes the id in, exactly as `SessionReplay` does for the raw tier.

use std::collections::VecDeque;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal64Type, TimestampNanosecondType, UInt64Type};
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use quant_core::event::{
    BookDelta, BookSnapshot, EventMeta, Gap, GapCause, Level, MarketEvent, Side, Trade,
};
use quant_core::instrument::{Exchange, InstrumentId};
use quant_core::source::{EventSource, SourceError};
use quant_core::time::{Ts, UtcDate};
use quant_core::{Px, Qty};

use super::schema::Dataset;
use super::{published_parts, TierError, TierTarget};

/// Read one dataset file back into events, in the order they were written.
pub fn read_dataset(
    path: &Path,
    dataset: Dataset,
    instrument: InstrumentId,
) -> Result<Vec<MarketEvent>, TierError> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?;
    let mut events = Vec::new();
    for batch in reader {
        decode_batch(&batch?, dataset, instrument, &mut events)?;
    }
    Ok(events)
}

fn decode_batch(
    batch: &RecordBatch,
    dataset: Dataset,
    instrument: InstrumentId,
    out: &mut Vec<MarketEvent>,
) -> Result<(), TierError> {
    let ingest_seq = u64s(batch, dataset, "ingest_seq")?;
    let local_recv = timestamps(batch, dataset, "local_recv_ts")?;
    let exchange = timestamps(batch, dataset, "exchange_ts")?;

    for row in 0..batch.num_rows() {
        let meta = EventMeta {
            instrument,
            exchange_ts: Ts::from_nanos(exchange.value(row)),
            local_recv_ts: Ts::from_nanos(local_recv.value(row)),
            ingest_seq: ingest_seq.value(row),
        };
        out.push(match dataset {
            Dataset::Trades => MarketEvent::Trade(Trade {
                meta,
                px: Px::from_raw(money(batch, dataset, "px")?.value(row)),
                qty: Qty::from_raw(money(batch, dataset, "qty")?.value(row)),
                aggressor: side(strings(batch, dataset, "aggressor")?.value(row))?,
                venue_trade_id: u64s(batch, dataset, "venue_trade_id")?.value(row),
            }),
            Dataset::BookDeltas => MarketEvent::BookDelta(BookDelta {
                meta,
                first_update_id: u64s(batch, dataset, "first_update_id")?.value(row),
                final_update_id: u64s(batch, dataset, "final_update_id")?.value(row),
                bids: level_list(batch, dataset, "bids", row)?,
                asks: level_list(batch, dataset, "asks", row)?,
            }),
            Dataset::BookSnapshots => MarketEvent::BookSnapshot(BookSnapshot {
                meta,
                last_update_id: u64s(batch, dataset, "last_update_id")?.value(row),
                bids: level_list(batch, dataset, "bids", row)?,
                asks: level_list(batch, dataset, "asks", row)?,
            }),
            Dataset::Gaps => MarketEvent::Gap(Gap {
                meta,
                cause: gap_cause(strings(batch, dataset, "cause")?.value(row))?,
                last_good_ts: Ts::from_nanos(
                    timestamps(batch, dataset, "last_good_ts")?.value(row),
                ),
            }),
        });
    }
    Ok(())
}

/// One row's levels.
fn level_list(
    batch: &RecordBatch,
    dataset: Dataset,
    name: &'static str,
    row: usize,
) -> Result<Vec<Level>, TierError> {
    let list =
        column(batch, dataset, name)?
            .as_list_opt::<i32>()
            .ok_or(TierError::UnexpectedColumn {
                dataset: dataset.dir(),
                column: name,
            })?;
    let entry = list.value(row);
    let entry: &StructArray = entry.as_struct_opt().ok_or(TierError::UnexpectedColumn {
        dataset: dataset.dir(),
        column: name,
    })?;
    let px =
        entry
            .column(0)
            .as_primitive_opt::<Decimal64Type>()
            .ok_or(TierError::UnexpectedColumn {
                dataset: dataset.dir(),
                column: name,
            })?;
    let qty =
        entry
            .column(1)
            .as_primitive_opt::<Decimal64Type>()
            .ok_or(TierError::UnexpectedColumn {
                dataset: dataset.dir(),
                column: name,
            })?;
    Ok((0..entry.len())
        .map(|i| Level {
            px: Px::from_raw(px.value(i)),
            qty: Qty::from_raw(qty.value(i)),
        })
        .collect())
}

/// Inverse of [`super::write::side_name`], exhaustive in both directions.
fn side(name: &str) -> Result<Side, TierError> {
    [Side::Buy, Side::Sell]
        .into_iter()
        .find(|s| super::write::side_name(*s) == name)
        .ok_or_else(|| TierError::UnknownLabel {
            column: "aggressor",
            value: name.to_owned(),
        })
}

/// Inverse of `GapCause::name`, driven by `GapCause::ALL` so a new variant is
/// readable the moment it is writable.
fn gap_cause(name: &str) -> Result<GapCause, TierError> {
    GapCause::ALL
        .into_iter()
        .find(|c| c.name() == name)
        .ok_or_else(|| TierError::UnknownLabel {
            column: "cause",
            value: name.to_owned(),
        })
}

/// Look a column up by name.
///
/// By name and never by position. The first version of this module indexed
/// columns, got `gaps` off by one, and produced a "cause is not the type the
/// schema declares" error pointing at a perfectly good file. Positional access
/// is a second, silent copy of the field order that only the reader knows about;
/// a name that is missing fails where the mistake is.
fn column<'a>(
    batch: &'a RecordBatch,
    dataset: Dataset,
    name: &'static str,
) -> Result<&'a ArrayRef, TierError> {
    batch
        .column_by_name(name)
        .ok_or(TierError::UnexpectedColumn {
            dataset: dataset.dir(),
            column: name,
        })
}

macro_rules! typed_column {
    ($fn_name:ident, $ret:ty, $cast:ident $(::<$ty:ty>)?) => {
        fn $fn_name<'a>(
            batch: &'a RecordBatch,
            dataset: Dataset,
            name: &'static str,
        ) -> Result<&'a $ret, TierError> {
            column(batch, dataset, name)?
                .$cast $(::<$ty>)? ()
                .ok_or(TierError::UnexpectedColumn {
                    dataset: dataset.dir(),
                    column: name,
                })
        }
    };
}

typed_column!(
    u64s,
    arrow_array::UInt64Array,
    as_primitive_opt::<UInt64Type>
);
typed_column!(
    money,
    arrow_array::Decimal64Array,
    as_primitive_opt::<Decimal64Type>
);
typed_column!(
    timestamps,
    arrow_array::TimestampNanosecondArray,
    as_primitive_opt::<TimestampNanosecondType>
);
typed_column!(strings, arrow_array::StringArray, as_string_opt::<i32>);

// --- Streaming a whole symbol back out of the tier ---

/// One dataset file, decoded lazily into events.
///
/// A batch at a time rather than a file at a time: a day of book deltas is
/// several hundred megabytes once every level is a `Vec<Level>`, and the point
/// of this tier is to be replayable on an ordinary machine.
#[derive(Debug)]
pub struct DatasetStream {
    dataset: Dataset,
    instrument: InstrumentId,
    reader: ParquetRecordBatchReader,
    buffer: VecDeque<MarketEvent>,
}

impl DatasetStream {
    /// Open one dataset file. A missing file yields nothing, which is how a day
    /// that predates a dataset reads.
    pub fn open(
        path: &Path,
        dataset: Dataset,
        instrument: InstrumentId,
    ) -> Result<Self, TierError> {
        Ok(Self {
            dataset,
            instrument,
            reader: ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?,
            buffer: VecDeque::new(),
        })
    }
}

impl Iterator for DatasetStream {
    type Item = Result<MarketEvent, TierError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(event) = self.buffer.pop_front() {
                return Some(Ok(event));
            }
            let batch = match self.reader.next()? {
                Ok(batch) => batch,
                Err(e) => return Some(Err(e.into())),
            };
            let mut events = Vec::with_capacity(batch.num_rows());
            if let Err(e) = decode_batch(&batch, self.dataset, self.instrument, &mut events) {
                return Some(Err(e));
            }
            self.buffer.extend(events);
        }
    }
}

/// A symbol's normalized tier, back as one ordered event stream.
///
/// This is the shape M3's `HistoricalSource` needs, which is why it exists as a
/// lazy iterator rather than as a comparison routine: the check that a Parquet
/// replay agrees with a raw replay is its first consumer, not its purpose.
///
/// # How the order is recovered
///
/// The four datasets are four files, so a part's stream is a four-way merge on
/// `ingest_seq` — which is exactly what makes `ingest_seq` the ordering key
/// rather than an incidental column. Within a part, `ingest_seq` is one
/// session's and strictly increasing, so the merge reproduces the recorded order
/// exactly.
///
/// A day can hold more than one part when a recorder restarted inside it. Parts
/// are **concatenated in index order and never merged with each other**: the
/// writer establishes that index order is capture order, and `ingest_seq`
/// restarts at 1 for a new session, so merging across a part boundary on it
/// would interleave the second session's opening events into the middle of the
/// first. The full ordering key is `(day, part, ingest_seq)`, and only the last
/// of those is compared inside the four-way merge.
///
/// A day that resolves to no parts at all is an error, not an empty stream. The
/// reader used to open one exact path, so a missing day failed at `File::open`;
/// listing a directory would quietly turn that into a shorter stream, and a
/// check that compares two streams would then agree about data neither of them
/// read.
#[derive(Debug)]
pub struct TierReplay {
    root: PathBuf,
    exchange: Exchange,
    symbol: String,
    instrument: InstrumentId,
    days: std::vec::IntoIter<UtcDate>,
    /// When set, read only the parts this session wrote.
    only_session: Option<[u8; 16]>,
    /// The day whose parts are being walked.
    open_date: Option<UtcDate>,
    /// Parts still to read for the day now open, ascending.
    parts: std::vec::IntoIter<u32>,
    /// One head per dataset for the open part; `None` once that dataset is spent.
    heads: Vec<Option<MarketEvent>>,
    streams: Vec<DatasetStream>,
    failed: bool,
}

impl TierReplay {
    /// Replay `days` of one symbol, in the order given.
    #[must_use]
    pub fn open(
        root: &Path,
        exchange: Exchange,
        symbol: &str,
        instrument: InstrumentId,
        days: Vec<UtcDate>,
    ) -> Self {
        Self {
            root: root.to_owned(),
            exchange,
            symbol: symbol.to_owned(),
            instrument,
            days: days.into_iter(),
            only_session: None,
            open_date: None,
            parts: Vec::new().into_iter(),
            heads: Vec::new(),
            streams: Vec::new(),
            failed: false,
        }
    }

    /// Replay only the parts one capture session contributed.
    ///
    /// `normalize --check` compares a *session's* raw replay against the tier, so
    /// on a day two sessions share it must read back only its own slice —
    /// otherwise the other session's first event arrives where this session's
    /// next one should be, and a healthy merge reads as a divergence. Selecting
    /// by the footer's session id rather than by remembering which index was
    /// written keeps the two sides independent, which is the whole point of the
    /// check.
    #[must_use]
    pub fn open_session(
        root: &Path,
        exchange: Exchange,
        symbol: &str,
        instrument: InstrumentId,
        days: Vec<UtcDate>,
        session_id: [u8; 16],
    ) -> Self {
        let mut replay = Self::open(root, exchange, symbol, instrument, days);
        replay.only_session = Some(session_id);
        replay
    }

    /// Open the next part's four files. `false` when there is nothing left.
    ///
    /// Walks parts within the open day first, then moves to the next day, so the
    /// stream is day order outside and part order inside.
    fn advance(&mut self) -> Result<bool, TierError> {
        loop {
            if let Some(part) = self.parts.next() {
                self.open_part(self.open_date.expect("a day is open"), part)?;
                return Ok(true);
            }
            let Some(date) = self.days.next() else {
                return Ok(false);
            };
            let parts = published_parts(
                &TierTarget {
                    exchange: self.exchange,
                    symbol: self.symbol.clone(),
                    date,
                    dataset: Dataset::ALL[0],
                    part: 0,
                }
                .directory(&self.root),
            );
            let parts = match self.only_session {
                None => parts,
                Some(id) => parts
                    .into_iter()
                    .filter(|&part| {
                        let target = TierTarget {
                            exchange: self.exchange,
                            symbol: self.symbol.clone(),
                            date,
                            dataset: Dataset::ALL[0],
                            part,
                        };
                        super::Provenance::session_of(&target.file(&self.root))
                            .ok()
                            .flatten()
                            == Some(id)
                    })
                    .collect(),
            };
            if parts.is_empty() {
                // Loud, because the alternative is a stream that is quietly
                // shorter than the days it was asked for. `open_part` would fail
                // on the missing file anyway; this says which day and why.
                return Err(TierError::NoPartsForDay {
                    date: date.to_string(),
                });
            }
            self.open_date = Some(date);
            self.parts = parts.into_iter();
        }
    }

    /// Open one part's four dataset files and prime a head from each.
    fn open_part(&mut self, date: UtcDate, part: u32) -> Result<(), TierError> {
        self.streams.clear();
        self.heads.clear();
        for dataset in Dataset::ALL {
            let target = TierTarget {
                exchange: self.exchange,
                symbol: self.symbol.clone(),
                date,
                dataset,
                part,
            };
            let mut stream =
                DatasetStream::open(&target.file(&self.root), dataset, self.instrument)?;
            self.heads.push(stream.next().transpose()?);
            self.streams.push(stream);
        }
        Ok(())
    }

    /// Take the lowest-sequence head across the open day's datasets.
    fn take_lowest(&mut self) -> Result<Option<MarketEvent>, TierError> {
        let lowest = self
            .heads
            .iter()
            .enumerate()
            .filter_map(|(i, head)| head.as_ref().map(|e| (i, e.meta().ingest_seq)))
            .min_by_key(|(_, seq)| *seq)
            .map(|(i, _)| i);
        let Some(i) = lowest else { return Ok(None) };
        let event = self.heads[i].take();
        self.heads[i] = self.streams[i].next().transpose()?;
        Ok(event)
    }
}

impl Iterator for TierReplay {
    type Item = Result<MarketEvent, TierError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            match self.take_lowest() {
                Ok(Some(event)) => return Some(Ok(event)),
                Ok(None) => match self.advance() {
                    // The day is spent; the loop opens the next one.
                    Ok(true) => (),
                    Ok(false) => return None,
                    Err(e) => {
                        self.failed = true;
                        return Some(Err(e));
                    }
                },
                Err(e) => {
                    self.failed = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

/// The normalized tier as an [`EventSource`], for the engine.
///
/// The "as fast as possible" leg of the three worlds: no pacing, no sockets,
/// just the recorded events in recorded order. Pacing against the wall clock is
/// a different source (`ReplaySource`, when M5 needs it) precisely so that a
/// backtest cannot accidentally acquire timing behaviour it will not have.
#[derive(Debug)]
pub struct HistoricalSource {
    inner: TierReplay,
}

impl HistoricalSource {
    /// Replay `days` of one symbol out of `root`.
    #[must_use]
    pub fn new(
        root: &Path,
        exchange: Exchange,
        symbol: &str,
        instrument: InstrumentId,
        days: Vec<UtcDate>,
    ) -> Self {
        Self {
            inner: TierReplay::open(root, exchange, symbol, instrument, days),
        }
    }
}

impl EventSource for HistoricalSource {
    fn next_event(&mut self) -> Option<Result<MarketEvent, SourceError>> {
        self.inner
            .next()
            .map(|item| item.map_err(|e| SourceError(e.to_string())))
    }
}

/// Every date partition present for one symbol, in calendar order.
///
/// Reads the directory rather than taking a range, so a run covers what is
/// actually there. Asking for a range and silently getting less is how a
/// backtest ends up reporting a period it did not test.
#[must_use]
pub fn discover_days(root: &Path, exchange: Exchange, symbol: &str) -> Vec<UtcDate> {
    let dir = root
        .join("normalized")
        .join(format!("exchange={exchange}"))
        .join(format!("symbol={symbol}"));
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut days: Vec<UtcDate> = entries
        .filter_map(|e| {
            let name = e.ok()?.file_name();
            let name = name.to_str()?;
            quant_recorder::parse_utc_date(name.strip_prefix("date=")?)
        })
        .collect();
    days.sort_by_key(|d| (d.year, d.month, d.day));
    days
}
