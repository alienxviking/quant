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

use std::fs::File;
use std::path::Path;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal64Type, TimestampNanosecondType, UInt64Type};
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use quant_core::event::{
    BookDelta, BookSnapshot, EventMeta, Gap, GapCause, Level, MarketEvent, Side, Trade,
};
use quant_core::instrument::InstrumentId;
use quant_core::time::Ts;
use quant_core::{Px, Qty};

use super::schema::Dataset;
use super::TierError;

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
