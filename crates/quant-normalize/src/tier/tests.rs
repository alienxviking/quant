//! Round-trip tests for the normalized tier.
//!
//! Every one of these writes a real Parquet file and reads it back with the
//! Parquet reader, rather than testing the column builders against themselves.
//! The criterion this tier exists to satisfy is that a replay from Parquet
//! agrees with a replay from raw, and a test that never serialises could pass
//! against a file nothing else can open.

use std::path::PathBuf;

use quant_core::event::{
    BookDelta, BookSnapshot, EventMeta, Gap, GapCause, Level, MarketEvent, Side, Trade,
};
use quant_core::instrument::{Exchange, InstrumentId};
use quant_core::time::{Ts, UtcDate};
use quant_core::{Px, Qty};

use super::{read_dataset, Dataset, DatasetWriter, TierError, TierTarget, MAX_MONEY_RAW};

/// Identity is reattached by the caller, per the schema module. Any id will do
/// here, so it comes from a registry rather than being constructed -- the field
/// is private on purpose.
fn instrument() -> InstrumentId {
    use quant_core::instrument::{InstrumentDef, InstrumentKind, InstrumentRegistry};
    InstrumentRegistry::new().register(InstrumentDef {
        exchange: Exchange::Binance,
        symbol: "BTCUSDT".to_owned(),
        base: "BTC".to_owned(),
        quote: "USDT".to_owned(),
        kind: InstrumentKind::Spot,
        tick_size: "0.01".parse().expect("tick"),
        lot_size: "0.00001".parse().expect("lot"),
        min_notional: "5".parse().expect("notional"),
    })
}

/// A temp file that removes itself.
struct Scratch {
    path: PathBuf,
}

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("quant-tier-{name}.parquet"));
        let _ = std::fs::remove_file(&path);
        Self { path }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn meta(seq: u64) -> EventMeta {
    let offset = i64::try_from(seq).expect("test sequence numbers are small");
    EventMeta {
        instrument: instrument(),
        // Deliberately different from each other and not round numbers: a
        // transposed pair of timestamp columns would otherwise pass.
        exchange_ts: Ts::from_nanos(1_787_315_652_514_000_001 + offset),
        local_recv_ts: Ts::from_nanos(1_787_315_652_601_000_007 + offset),
        ingest_seq: seq,
    }
}

fn level(px: &str, qty: &str) -> Level {
    Level {
        px: px.parse().expect("a valid price"),
        qty: qty.parse().expect("a valid quantity"),
    }
}

/// Write events and read them straight back.
fn round_trip(name: &str, dataset: Dataset, events: &[MarketEvent]) -> Vec<MarketEvent> {
    let scratch = Scratch::new(name);
    let file = std::fs::File::create(&scratch.path).expect("create");
    let mut writer = DatasetWriter::new(file, dataset, None).expect("writer");
    for event in events {
        writer.push(event).expect("push");
    }
    let (_sink, rows) = writer.finish().expect("finish");
    assert_eq!(rows, events.len() as u64, "finish reports what it wrote");
    read_dataset(&scratch.path, dataset, instrument()).expect("read")
}

#[test]
fn trades_round_trip_exactly() {
    let events: Vec<MarketEvent> = (0..3)
        .map(|i| {
            MarketEvent::Trade(Trade {
                meta: meta(i + 1),
                px: "76650.12345678".parse().expect("px"),
                qty: "0.00040000".parse().expect("qty"),
                // Both sides present, so a mapping stuck on one is caught.
                aggressor: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                venue_trade_id: 6_595_931_385 + i,
            })
        })
        .collect();
    assert_eq!(round_trip("trades", Dataset::Trades, &events), events);
}

#[test]
fn book_deltas_round_trip_exactly_including_their_level_lists() {
    // Uneven list lengths on purpose, and one empty side: a wrong offsets buffer
    // is invisible when every row has the same number of levels.
    let events = vec![
        MarketEvent::BookDelta(BookDelta {
            meta: meta(1),
            first_update_id: 98_853_022_585,
            final_update_id: 98_853_022_588,
            bids: vec![level("68985.00000000", "0.00160000")],
            asks: vec![],
        }),
        MarketEvent::BookDelta(BookDelta {
            meta: meta(2),
            first_update_id: 98_853_022_589,
            final_update_id: 98_853_022_589,
            bids: vec![],
            asks: vec![
                level("76651.00000000", "0.01000000"),
                level("76652.50000000", "1.23456789"),
                level("76653.00000000", "0.00000001"),
            ],
        }),
        MarketEvent::BookDelta(BookDelta {
            meta: meta(3),
            first_update_id: 98_853_022_590,
            final_update_id: 98_853_022_592,
            // A removal: quantity zero is meaningful, not missing data.
            bids: vec![level("68985.00000000", "0")],
            asks: vec![level("76651.00000000", "0")],
        }),
    ];
    assert_eq!(
        round_trip("deltas", Dataset::BookDeltas, &events),
        events,
        "levels must come back in order, per row, including empty and zero"
    );
}

#[test]
fn book_snapshots_round_trip_exactly() {
    let events = vec![MarketEvent::BookSnapshot(BookSnapshot {
        meta: meta(4),
        last_update_id: 98_853_022_644,
        bids: vec![
            level("76650.00000000", "0.00174000"),
            level("76649.99000000", "0.00049000"),
        ],
        asks: vec![level("76651.00000000", "0.01000000")],
    })];
    assert_eq!(
        round_trip("snapshots", Dataset::BookSnapshots, &events),
        events
    );
}

#[test]
fn every_gap_cause_round_trips() {
    // Driven by GapCause::ALL rather than a list written here, so a new cause
    // cannot be written without also being readable.
    let events: Vec<MarketEvent> = GapCause::ALL
        .into_iter()
        .enumerate()
        .map(|(i, cause)| {
            MarketEvent::Gap(Gap {
                meta: meta(i as u64 + 1),
                cause,
                last_good_ts: Ts::from_nanos(1_787_315_600_000_000_000),
            })
        })
        .collect();
    assert_eq!(round_trip("gaps", Dataset::Gaps, &events), events);
}

#[test]
fn a_batch_boundary_is_invisible() {
    // More events than BATCH_ROWS, so the file has several record batches and
    // the offsets of the second one start from zero again.
    let n = super::BATCH_ROWS as u64 + 17;
    let events: Vec<MarketEvent> = (0..n)
        .map(|i| {
            MarketEvent::BookDelta(BookDelta {
                meta: meta(i + 1),
                first_update_id: i + 1,
                final_update_id: i + 1,
                bids: vec![level("100.00000000", "1.00000000"); (i % 3) as usize],
                asks: vec![level("200.00000000", "2.00000000")],
            })
        })
        .collect();
    let back = round_trip("batches", Dataset::BookDeltas, &events);
    assert_eq!(back.len(), events.len());
    assert_eq!(back, events);
}

#[test]
fn an_event_of_the_wrong_kind_is_refused_not_dropped() {
    let scratch = Scratch::new("wrong-kind");
    let file = std::fs::File::create(&scratch.path).expect("create");
    let mut writer = DatasetWriter::new(file, Dataset::Trades, None).expect("writer");
    let err = writer
        .push(&MarketEvent::Gap(Gap {
            meta: meta(1),
            cause: GapCause::Disconnect,
            last_good_ts: Ts::from_nanos(1),
        }))
        .expect_err("a gap does not belong in trades");
    assert!(matches!(err, TierError::WrongDataset { .. }), "{err}");
}

#[test]
fn a_price_beyond_the_decimal_precision_is_an_error_not_a_truncation() {
    // Invariant 5: never round, never default. i64 holds about nine times what
    // DECIMAL(18,8) does, so the gap is reachable in principle and must fail.
    let scratch = Scratch::new("out-of-range");
    let file = std::fs::File::create(&scratch.path).expect("create");
    let mut writer = DatasetWriter::new(file, Dataset::Trades, None).expect("writer");
    let err = writer
        .push(&MarketEvent::Trade(Trade {
            meta: meta(1),
            px: Px::from_raw(MAX_MONEY_RAW + 1),
            qty: Qty::from_raw(1),
            aggressor: Side::Buy,
            venue_trade_id: 1,
        }))
        .expect_err("out of range must not be silently truncated");
    assert!(matches!(err, TierError::MoneyOutOfRange(_)), "{err}");
}

#[test]
fn the_largest_representable_price_is_accepted() {
    // The other half of the bound: refusing one too many must not mean refusing
    // the last valid value.
    let events = vec![MarketEvent::Trade(Trade {
        meta: meta(1),
        px: Px::from_raw(MAX_MONEY_RAW),
        qty: Qty::from_raw(-MAX_MONEY_RAW),
        aggressor: Side::Sell,
        venue_trade_id: 1,
    })];
    assert_eq!(round_trip("at-the-bound", Dataset::Trades, &events), events);
}

#[test]
fn an_empty_dataset_writes_a_readable_file() {
    // A day with no gaps still gets a gaps file. An absent file and an empty one
    // are different claims, and only the empty one says "nothing happened".
    let back = round_trip("empty", Dataset::Gaps, &[]);
    assert!(back.is_empty());
}

#[test]
fn a_written_path_parses_back_to_the_target_that_wrote_it() {
    for dataset in Dataset::ALL {
        let target = TierTarget {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            date: UtcDate {
                year: 2026,
                month: 8,
                day: 21,
            },
            dataset,
        };
        let path = target.file(std::path::Path::new("data"));
        assert_eq!(TierTarget::parse(&path).as_ref(), Some(&target));
    }
}

#[test]
fn the_path_matches_the_published_layout() {
    // Pinned against docs/data-contract.md §5, exactly as the raw tier's layout
    // is: every tool pointed at this directory reads the partition scheme.
    let target = TierTarget {
        exchange: Exchange::Binance,
        symbol: "BTCUSDT".to_owned(),
        date: UtcDate {
            year: 2026,
            month: 8,
            day: 21,
        },
        dataset: Dataset::BookDeltas,
    };
    let path = target.file(std::path::Path::new("data"));
    assert_eq!(
        path.to_string_lossy().replace('\\', "/"),
        "data/normalized/exchange=binance/symbol=BTCUSDT/date=2026-08-21/\
         book_deltas/part-00000.parquet"
    );
}

#[test]
fn money_is_stored_as_an_int64_backed_decimal() {
    // The claim the schema module makes -- that DECIMAL(18,8) costs nothing over
    // a bare INT64 -- is checked rather than asserted, because precision 19
    // would silently become a 16-byte FIXED_LEN_BYTE_ARRAY.
    use parquet::basic::{LogicalType, Type as PhysicalType};
    use parquet::file::reader::FileReader as _;

    let scratch = Scratch::new("physical-type");
    let file = std::fs::File::create(&scratch.path).expect("create");
    let mut writer = DatasetWriter::new(file, Dataset::Trades, None).expect("writer");
    writer
        .push(&MarketEvent::Trade(Trade {
            meta: meta(1),
            px: "1.00000000".parse().expect("px"),
            qty: "1.00000000".parse().expect("qty"),
            aggressor: Side::Buy,
            venue_trade_id: 1,
        }))
        .expect("push");
    writer.finish().expect("finish");

    let reader = parquet::file::reader::SerializedFileReader::new(
        std::fs::File::open(&scratch.path).expect("open"),
    )
    .expect("parquet reader");
    let schema = reader.metadata().file_metadata().schema_descr();
    let px = (0..schema.num_columns())
        .map(|i| schema.column(i))
        .find(|c| c.name() == "px")
        .expect("a px column");
    assert_eq!(px.physical_type(), PhysicalType::INT64);
    assert!(
        matches!(px.logical_type_ref(), Some(LogicalType::Decimal(d))
            if d.scale == i32::from(super::MONEY_SCALE)
                && d.precision == i32::from(super::MONEY_PRECISION)),
        "px must carry its scale in the schema, not by convention: {:?}",
        px.logical_type_ref()
    );
}
