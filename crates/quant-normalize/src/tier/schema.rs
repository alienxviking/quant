//! The normalized tier's Parquet schema.
//!
//! # Why money is `DECIMAL(18,8)` and not `INT64`
//!
//! They are the same bytes: Parquet backs a decimal of precision ≤ 18 with an
//! `INT64`, so this costs nothing. What the decimal adds is **the scale, in the
//! schema**. Invariant 1 says money is integral and never passes through `f64`,
//! and that guarantee has so far only held inside our own process; the moment a
//! file leaves it, a bare `INT64` column requires every reader to know the `1e8`
//! convention out of band. The first one that does not is wrong by eight orders
//! of magnitude, and the first one that "helpfully" reads it as a double loses
//! precision silently on large notionals. `DuckDB`, Polars and pandas all read a
//! decimal column as an exact decimal. So the column type is how invariant 1
//! survives the process boundary.
//!
//! `Decimal64` rather than `Decimal128`: identical on disk, but `i64` in memory
//! instead of sixteen bytes, which is the difference between one and two copies
//! of every price while a batch of several million levels is being built.
//!
//! The bound this buys has to be enforced rather than assumed. Precision 18 at
//! scale 8 holds values below `10^10`; `i64` holds four times that. A price above
//! the bound is a loud error on write, per invariant 5 — never a truncation.
//!
//! # Why timestamps are `TIMESTAMP(NANOS, UTC)`
//!
//! Same argument. `Ts` is nanoseconds since the Unix epoch, which is exactly what
//! the logical type means, so writing it as a bare `INT64` would discard
//! information the format has a place for. Note this is the first time nanosecond
//! `Ts` values leave the raw tier — `quant-meta` deliberately stores microseconds
//! and calls them operational only. That is not a contradiction: Postgres holds an
//! *index*, and this holds a faithful re-derivation.
//!
//! # Why the instrument is not a column
//!
//! `InstrumentId` is a registry index and is **never persisted** — M0's rule, and
//! the reason `EventMeta` is the only part of an event this schema drops. Identity
//! lives in the partition path (`exchange=…/symbol=…`) and is reattached from
//! `(exchange, symbol)` on read, exactly as the raw tier does it. Writing the
//! index would produce files whose meaning depended on the registration order of
//! whatever process happened to write them.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use quant_core::SCALE_DECIMALS;

/// Decimal precision for every money column.
///
/// 18 is the largest precision Parquet stores as an `INT64`; 19 would silently
/// become a 16-byte `FIXED_LEN_BYTE_ARRAY`.
pub const MONEY_PRECISION: u8 = 18;

/// Decimal scale for every money column: the same `1e8` as `quant-core`.
///
/// Derived from `SCALE_DECIMALS` rather than written as `8`, and checked at
/// compile time rather than cast: if the fixed-point scale ever changed, a cast
/// would follow it silently past what an `i8` or Parquet can express, and every
/// file written afterwards would disagree with every file written before.
pub const MONEY_SCALE: i8 = {
    assert!(
        SCALE_DECIMALS <= i8::MAX as u32 && SCALE_DECIMALS < MONEY_PRECISION as u32,
        "the fixed-point scale no longer fits the declared decimal"
    );
    #[allow(
        clippy::cast_possible_truncation,
        reason = "bounded by the assertion above"
    )]
    let scale = SCALE_DECIMALS as i8;
    scale
};

/// Largest raw fixed-point value `MONEY_PRECISION` can represent: `10^18 - 1`.
///
/// About `9.2e18` of `i64` range is therefore unrepresentable here. In real
/// units that is any price or size at or above `10^10`, which no instrument this
/// platform will trade reaches — but "will not happen" is not a reason to write
/// a wrong number, so the writer refuses instead.
pub const MAX_MONEY_RAW: i64 = 999_999_999_999_999_999;

/// One of the four files a day of the normalized tier is stored in.
///
/// Split by event type rather than one file of a tagged union, per
/// `docs/data-contract.md` §5: the columns differ completely, and a query that
/// wants trades should not read past 11.6M book deltas to find them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dataset {
    Trades,
    BookDeltas,
    BookSnapshots,
    Gaps,
}

impl Dataset {
    /// Every dataset, so a caller cannot forget one.
    pub const ALL: [Self; 4] = [
        Self::Trades,
        Self::BookDeltas,
        Self::BookSnapshots,
        Self::Gaps,
    ];

    /// Directory name under a date partition, per the contract's §5 layout.
    #[must_use]
    pub const fn dir(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::BookDeltas => "book_deltas",
            Self::BookSnapshots => "book_snapshots",
            Self::Gaps => "gaps",
        }
    }

    /// Parse a directory name back into a dataset.
    #[must_use]
    pub fn from_dir(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|d| d.dir() == name)
    }

    /// The Arrow schema for this dataset's file.
    #[must_use]
    pub fn schema(self) -> SchemaRef {
        let mut fields = meta_fields();
        match self {
            Self::Trades => fields.extend([
                Field::new("px", money(), false),
                Field::new("qty", money(), false),
                Field::new("aggressor", DataType::Utf8, false),
                Field::new("venue_trade_id", DataType::UInt64, false),
            ]),
            Self::BookDeltas => fields.extend([
                Field::new("first_update_id", DataType::UInt64, false),
                Field::new("final_update_id", DataType::UInt64, false),
                Field::new("bids", levels(), false),
                Field::new("asks", levels(), false),
            ]),
            Self::BookSnapshots => fields.extend([
                Field::new("last_update_id", DataType::UInt64, false),
                Field::new("bids", levels(), false),
                Field::new("asks", levels(), false),
            ]),
            Self::Gaps => fields.extend([
                Field::new("cause", DataType::Utf8, false),
                Field::new("last_good_ts", timestamp(), false),
            ]),
        }
        Arc::new(Schema::new(fields))
    }
}

/// The columns every event carries.
///
/// `ingest_seq` first because it is the ordering key and the primary key: it is
/// session-scoped and strictly increasing, so a reader that sorts by it gets the
/// stream back exactly as recorded.
fn meta_fields() -> Vec<Field> {
    vec![
        Field::new("ingest_seq", DataType::UInt64, false),
        Field::new("local_recv_ts", timestamp(), false),
        Field::new("exchange_ts", timestamp(), false),
    ]
}

/// A money column: fixed-point, exact, self-describing.
#[must_use]
pub fn money() -> DataType {
    DataType::Decimal64(MONEY_PRECISION, MONEY_SCALE)
}

/// A timestamp column: nanoseconds since the Unix epoch, in UTC.
#[must_use]
pub fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

/// The `px`/`qty` pair a book level is made of.
#[must_use]
pub fn level_fields() -> Fields {
    Fields::from(vec![
        Field::new("px", money(), false),
        Field::new("qty", money(), false),
    ])
}

/// A side of a book update: `LIST<STRUCT<px, qty>>`.
///
/// Nested rather than exploded into one row per level. Exploding compresses
/// better and scans faster, but it destroys one-row-per-event and would need a
/// join to rebuild a delta — and M2's criterion is that events round-trip
/// exactly, so the encoding that preserves the event is the one that can be
/// checked.
#[must_use]
pub fn levels() -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(level_fields()),
        false,
    )))
}
