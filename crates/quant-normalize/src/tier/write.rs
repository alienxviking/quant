//! Writing events into the normalized tier.
//!
//! One [`DatasetWriter`] per file. It accumulates plain `Vec`s and turns them
//! into an Arrow `RecordBatch` every [`BATCH_ROWS`] events, rather than using
//! Arrow's builders: the nested `LIST<STRUCT<decimal, decimal>>` a book side
//! needs would otherwise go through `StructBuilder`, which finishes its children
//! without the precision and scale the schema declares and then fails to match
//! it. Building the arrays directly is a few more lines and says exactly what
//! ends up on disk.

use std::io::Write;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Decimal64Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use quant_core::event::{Level, MarketEvent, Side};

use super::schema::{level_fields, levels, Dataset, MAX_MONEY_RAW, MONEY_PRECISION, MONEY_SCALE};
use super::TierError;

/// Events per `RecordBatch`.
///
/// Not the row group size — that is Parquet's own setting, left at its default.
/// This is only how much is held in memory before being handed to the writer,
/// and 64k events of book deltas is a few tens of MB at the observed ~33 levels
/// each. Small enough that a day of trades never materialises at once, large
/// enough that per-batch overhead disappears.
pub const BATCH_ROWS: usize = 64 * 1024;

/// Writes one dataset's events to one Parquet file.
#[derive(Debug)]
pub struct DatasetWriter<W: Write + Send> {
    dataset: Dataset,
    writer: ArrowWriter<W>,
    columns: Columns,
    rows_written: u64,
}

impl<W: Write + Send> DatasetWriter<W> {
    /// Open a writer over `sink`.
    ///
    /// zstd, matching the raw tier — one compressor to understand, and the
    /// project already links it. Level 3 for the same reason `WriterOptions`
    /// chose it: the curve past there costs an order of magnitude of CPU for
    /// low double-digit percentages.
    pub fn new(sink: W, dataset: Dataset) -> Result<Self, TierError> {
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .build();
        Ok(Self {
            dataset,
            writer: ArrowWriter::try_new(sink, dataset.schema(), Some(props))?,
            columns: Columns::new(dataset),
            rows_written: 0,
        })
    }

    /// Append an event.
    ///
    /// An event of the wrong kind is an error and not a silent skip: a writer
    /// that quietly dropped what it was not expecting would produce a tier
    /// missing rows with nothing to say so.
    pub fn push(&mut self, event: &MarketEvent) -> Result<(), TierError> {
        self.columns.push(event)?;
        if self.columns.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    /// Events handed to the Parquet writer so far.
    ///
    /// Excludes whatever is still buffered, which is why [`Self::finish`]
    /// returns the total rather than leaving a caller to read this afterwards —
    /// the first version did exactly that and reported zero for every file
    /// smaller than one batch. Same shape as `RawWriter::finish`, for the same
    /// reason: a count is only trustworthy once the thing counting has closed.
    #[must_use]
    pub const fn rows(&self) -> u64 {
        self.rows_written
    }

    fn flush(&mut self) -> Result<(), TierError> {
        if self.columns.len() == 0 {
            return Ok(());
        }
        let rows = self.columns.len();
        let batch = self.columns.take_batch(self.dataset)?;
        self.writer.write(&batch)?;
        self.rows_written += rows as u64;
        Ok(())
    }

    /// Flush, close the file, and hand back the sink with the final row count.
    pub fn finish(mut self) -> Result<(W, u64), TierError> {
        self.flush()?;
        let rows = self.rows_written;
        Ok((self.writer.into_inner()?, rows))
    }
}

/// The columns of whichever dataset is being written.
#[derive(Debug)]
enum Columns {
    Trades {
        meta: MetaColumns,
        px: Vec<i64>,
        qty: Vec<i64>,
        aggressor: Vec<&'static str>,
        venue_trade_id: Vec<u64>,
    },
    BookDeltas {
        meta: MetaColumns,
        first_update_id: Vec<u64>,
        final_update_id: Vec<u64>,
        bids: LevelColumns,
        asks: LevelColumns,
    },
    BookSnapshots {
        meta: MetaColumns,
        last_update_id: Vec<u64>,
        bids: LevelColumns,
        asks: LevelColumns,
    },
    Gaps {
        meta: MetaColumns,
        cause: Vec<&'static str>,
        last_good_ts: Vec<i64>,
    },
}

impl Columns {
    fn new(dataset: Dataset) -> Self {
        match dataset {
            Dataset::Trades => Self::Trades {
                meta: MetaColumns::default(),
                px: Vec::new(),
                qty: Vec::new(),
                aggressor: Vec::new(),
                venue_trade_id: Vec::new(),
            },
            Dataset::BookDeltas => Self::BookDeltas {
                meta: MetaColumns::default(),
                first_update_id: Vec::new(),
                final_update_id: Vec::new(),
                bids: LevelColumns::default(),
                asks: LevelColumns::default(),
            },
            Dataset::BookSnapshots => Self::BookSnapshots {
                meta: MetaColumns::default(),
                last_update_id: Vec::new(),
                bids: LevelColumns::default(),
                asks: LevelColumns::default(),
            },
            Dataset::Gaps => Self::Gaps {
                meta: MetaColumns::default(),
                cause: Vec::new(),
                last_good_ts: Vec::new(),
            },
        }
    }

    const fn meta(&self) -> &MetaColumns {
        match self {
            Self::Trades { meta, .. }
            | Self::BookDeltas { meta, .. }
            | Self::BookSnapshots { meta, .. }
            | Self::Gaps { meta, .. } => meta,
        }
    }

    fn len(&self) -> usize {
        self.meta().ingest_seq.len()
    }

    fn push(&mut self, event: &MarketEvent) -> Result<(), TierError> {
        match (self, event) {
            (
                Self::Trades {
                    meta,
                    px,
                    qty,
                    aggressor,
                    venue_trade_id,
                },
                MarketEvent::Trade(t),
            ) => {
                meta.push(&t.meta);
                px.push(money(t.px.raw())?);
                qty.push(money(t.qty.raw())?);
                aggressor.push(side_name(t.aggressor));
                venue_trade_id.push(t.venue_trade_id);
            }
            (
                Self::BookDeltas {
                    meta,
                    first_update_id,
                    final_update_id,
                    bids,
                    asks,
                },
                MarketEvent::BookDelta(d),
            ) => {
                meta.push(&d.meta);
                first_update_id.push(d.first_update_id);
                final_update_id.push(d.final_update_id);
                bids.push(&d.bids)?;
                asks.push(&d.asks)?;
            }
            (
                Self::BookSnapshots {
                    meta,
                    last_update_id,
                    bids,
                    asks,
                },
                MarketEvent::BookSnapshot(s),
            ) => {
                meta.push(&s.meta);
                last_update_id.push(s.last_update_id);
                bids.push(&s.bids)?;
                asks.push(&s.asks)?;
            }
            (
                Self::Gaps {
                    meta,
                    cause,
                    last_good_ts,
                },
                MarketEvent::Gap(g),
            ) => {
                meta.push(&g.meta);
                cause.push(g.cause.name());
                last_good_ts.push(g.last_good_ts.as_nanos());
            }
            (columns, event) => {
                return Err(TierError::WrongDataset {
                    expected: columns.dataset_name(),
                    found: event_name(event),
                })
            }
        }
        Ok(())
    }

    const fn dataset_name(&self) -> &'static str {
        match self {
            Self::Trades { .. } => Dataset::Trades.dir(),
            Self::BookDeltas { .. } => Dataset::BookDeltas.dir(),
            Self::BookSnapshots { .. } => Dataset::BookSnapshots.dir(),
            Self::Gaps { .. } => Dataset::Gaps.dir(),
        }
    }

    /// Turn what has accumulated into a batch, leaving the columns empty.
    fn take_batch(&mut self, dataset: Dataset) -> Result<RecordBatch, TierError> {
        let mut arrays: Vec<ArrayRef> = Vec::new();
        match self {
            Self::Trades {
                meta,
                px,
                qty,
                aggressor,
                venue_trade_id,
            } => {
                meta.take_into(&mut arrays);
                arrays.push(decimal(std::mem::take(px))?);
                arrays.push(decimal(std::mem::take(qty))?);
                arrays.push(Arc::new(StringArray::from(std::mem::take(aggressor))));
                arrays.push(Arc::new(UInt64Array::from(std::mem::take(venue_trade_id))));
            }
            Self::BookDeltas {
                meta,
                first_update_id,
                final_update_id,
                bids,
                asks,
            } => {
                meta.take_into(&mut arrays);
                arrays.push(Arc::new(UInt64Array::from(std::mem::take(first_update_id))));
                arrays.push(Arc::new(UInt64Array::from(std::mem::take(final_update_id))));
                arrays.push(bids.take()?);
                arrays.push(asks.take()?);
            }
            Self::BookSnapshots {
                meta,
                last_update_id,
                bids,
                asks,
            } => {
                meta.take_into(&mut arrays);
                arrays.push(Arc::new(UInt64Array::from(std::mem::take(last_update_id))));
                arrays.push(bids.take()?);
                arrays.push(asks.take()?);
            }
            Self::Gaps {
                meta,
                cause,
                last_good_ts,
            } => {
                meta.take_into(&mut arrays);
                arrays.push(Arc::new(StringArray::from(std::mem::take(cause))));
                arrays.push(timestamps(std::mem::take(last_good_ts)));
            }
        }
        Ok(RecordBatch::try_new(dataset.schema(), arrays)?)
    }
}

/// The three columns every event carries.
#[derive(Debug, Default)]
struct MetaColumns {
    ingest_seq: Vec<u64>,
    local_recv_ts: Vec<i64>,
    exchange_ts: Vec<i64>,
}

impl MetaColumns {
    fn push(&mut self, meta: &quant_core::event::EventMeta) {
        // `meta.instrument` is deliberately dropped; see the schema module.
        self.ingest_seq.push(meta.ingest_seq);
        self.local_recv_ts.push(meta.local_recv_ts.as_nanos());
        self.exchange_ts.push(meta.exchange_ts.as_nanos());
    }

    fn take_into(&mut self, arrays: &mut Vec<ArrayRef>) {
        arrays.push(Arc::new(UInt64Array::from(std::mem::take(
            &mut self.ingest_seq,
        ))));
        arrays.push(timestamps(std::mem::take(&mut self.local_recv_ts)));
        arrays.push(timestamps(std::mem::take(&mut self.exchange_ts)));
    }
}

/// One side of a book update, flattened with offsets — the layout Arrow's
/// `ListArray` wants anyway.
#[derive(Debug, Default)]
struct LevelColumns {
    px: Vec<i64>,
    qty: Vec<i64>,
    /// `offsets[i]..offsets[i + 1]` is row `i`'s levels. Always starts at 0, so
    /// it holds one more entry than there are rows.
    offsets: Vec<i32>,
}

impl LevelColumns {
    fn push(&mut self, levels: &[Level]) -> Result<(), TierError> {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        for level in levels {
            self.px.push(money(level.px.raw())?);
            self.qty.push(money(level.qty.raw())?);
        }
        let end = i32::try_from(self.px.len()).map_err(|_| TierError::BatchTooLarge)?;
        self.offsets.push(end);
        Ok(())
    }

    fn take(&mut self) -> Result<ArrayRef, TierError> {
        let offsets = std::mem::take(&mut self.offsets);
        let offsets = if offsets.is_empty() { vec![0] } else { offsets };
        let values = StructArray::new(
            level_fields(),
            vec![
                decimal(std::mem::take(&mut self.px))?,
                decimal(std::mem::take(&mut self.qty))?,
            ],
            None,
        );
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let arrow_schema::DataType::List(field) = levels() else {
            unreachable!("levels() is a List by construction")
        };
        Ok(Arc::new(ListArray::new(
            field,
            offsets,
            Arc::new(values),
            None,
        )))
    }
}

/// Check a raw fixed-point value against the declared decimal precision.
///
/// Invariant 5's shape: a value we cannot represent is loud, never rounded and
/// never truncated. See `MAX_MONEY_RAW` for why the bound is unreachable in
/// practice and enforced anyway.
fn money(raw: i64) -> Result<i64, TierError> {
    if raw.unsigned_abs() > MAX_MONEY_RAW.unsigned_abs() {
        return Err(TierError::MoneyOutOfRange(raw));
    }
    Ok(raw)
}

fn decimal(values: Vec<i64>) -> Result<ArrayRef, TierError> {
    Ok(Arc::new(
        Decimal64Array::from(values).with_precision_and_scale(MONEY_PRECISION, MONEY_SCALE)?,
    ))
}

fn timestamps(values: Vec<i64>) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC"))
}

/// The venue-neutral spelling of a side.
///
/// An exhaustive match rather than a lookup, so adding a variant to `Side` fails
/// to compile here instead of writing a file nothing can read back.
pub(super) const fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

const fn event_name(event: &MarketEvent) -> &'static str {
    match event {
        MarketEvent::Trade(_) => Dataset::Trades.dir(),
        MarketEvent::BookDelta(_) => Dataset::BookDeltas.dir(),
        MarketEvent::BookSnapshot(_) => Dataset::BookSnapshots.dir(),
        MarketEvent::Gap(_) => Dataset::Gaps.dir(),
    }
}

/// Which dataset an event belongs in.
#[must_use]
pub const fn dataset_of(event: &MarketEvent) -> Dataset {
    match event {
        MarketEvent::Trade(_) => Dataset::Trades,
        MarketEvent::BookDelta(_) => Dataset::BookDeltas,
        MarketEvent::BookSnapshot(_) => Dataset::BookSnapshots,
        MarketEvent::Gap(_) => Dataset::Gaps,
    }
}
