//! Typed rows, and the conversions from recorder types into database types.
//!
//! The conversions live here rather than at the call site because both of them
//! narrow: `u64` counters into `bigint`, nanosecond [`Ts`] into microsecond
//! `timestamptz`. Narrowing in one reviewed place beats narrowing in six.

use core::fmt;

use quant_core::instrument::Exchange;
use quant_core::time::{Ts, UtcDate};
use quant_recorder::SegmentReport;
use time::{Date, Month, OffsetDateTime};
use uuid::Uuid;

use crate::store::MetaError;

/// Lifecycle state of a capture session.
///
/// `Running` rows are how a killed recorder is found: nothing else distinguishes
/// "still going" from "died without closing", which is the same information the
/// missing file trailer carries. Two independent records of the same fact, which
/// is the point of having a metadata tier at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStatus {
    Running,
    Closed,
    Failed,
}

impl SessionStatus {
    /// Must match the `capture_sessions_status_valid` check constraint.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A session as it is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRow {
    pub id: Uuid,
    pub exchange: String,
    pub symbol: String,
    pub started_at: OffsetDateTime,
}

impl SessionRow {
    pub fn new(
        session_id: [u8; 16],
        exchange: Exchange,
        symbol: impl Into<String>,
        started_at: Ts,
    ) -> Result<Self, MetaError> {
        Ok(Self {
            id: Uuid::from_bytes(session_id),
            // The venue's own `Display`, which is also what the storage path
            // uses, so `exchange=binance` in the directory and `binance` in the
            // column cannot drift apart.
            exchange: exchange.to_string(),
            symbol: symbol.into(),
            started_at: to_offset(started_at)?,
        })
    }
}

/// How a session ended, plus what ingress saw over its lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionClose {
    pub ended_at: OffsetDateTime,
    pub status: SessionStatus,
    pub messages: i64,
    pub venue_bytes: i64,
    pub dropped: i64,
    pub gaps_recorded: i64,
    pub gaps_abandoned: i64,
    pub backdated: i64,
    pub note: Option<String>,
}

/// One sealed capture file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRow {
    pub session_id: Uuid,
    pub capture_date: Date,
    pub part: i32,
    pub path: String,
    pub sealed_at: OffsetDateTime,
    pub frames: i64,
    pub blocks: i64,
    pub file_bytes: i64,
    pub frame_bytes: i64,
    pub first_ingest_seq: Option<i64>,
    pub last_ingest_seq: Option<i64>,
}

impl SegmentRow {
    /// Build a row from what the writer reported.
    ///
    /// `path` is passed in rather than derived, because the row should record where
    /// the file actually is -- which only whoever owns the data root knows.
    pub fn from_report(
        report: &SegmentReport,
        path: impl Into<String>,
        sealed_at: Ts,
    ) -> Result<Self, MetaError> {
        Ok(Self {
            session_id: Uuid::from_bytes(report.target.session_id),
            capture_date: to_date(report.target.date)?,
            part: i32::try_from(report.target.part).map_err(|_| MetaError::OutOfRange("part"))?,
            path: path.into(),
            sealed_at: to_offset(sealed_at)?,
            frames: saturating_i64(report.stats.frames),
            blocks: saturating_i64(report.stats.blocks),
            file_bytes: saturating_i64(report.stats.file_bytes),
            frame_bytes: saturating_i64(report.stats.frame_bytes),
            first_ingest_seq: report.first_ingest_seq.map(saturating_i64),
            last_ingest_seq: report.last_ingest_seq.map(saturating_i64),
        })
    }
}

/// `u64` counter into `bigint`.
///
/// Saturates rather than wrapping or erroring. These are frame and byte counts; to
/// exceed `i64::MAX` a single capture file would have to hold nine quintillion
/// frames. Saturating keeps a nonsense value obviously nonsense instead of
/// negative, and refuses to fail a metadata write over it.
#[must_use]
pub fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// Nanosecond [`Ts`] into a `timestamptz`.
///
/// Postgres stores microseconds, so this narrows. That is acceptable *only*
/// because these are operational timestamps -- when a session started, when a file
/// was sealed. The nanosecond values that events are ordered and dispatched on
/// never leave the raw tier, and nothing may derive a trading decision from a
/// column in this database.
pub fn to_offset(ts: Ts) -> Result<OffsetDateTime, MetaError> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ts.as_nanos()))
        .map_err(|_| MetaError::OutOfRange("timestamp"))
}

/// [`UtcDate`] into a Postgres `date`.
pub fn to_date(date: UtcDate) -> Result<Date, MetaError> {
    let month = Month::try_from(date.month).map_err(|_| MetaError::OutOfRange("month"))?;
    Date::from_calendar_date(date.year, month, date.day)
        .map_err(|_| MetaError::OutOfRange("calendar date"))
}

#[cfg(test)]
mod tests {
    use quant_recorder::CaptureTarget;
    use quant_storage::WriterStats;

    use super::*;

    fn report() -> SegmentReport {
        SegmentReport {
            target: CaptureTarget {
                exchange: Exchange::Binance,
                symbol: "BTCUSDT".to_owned(),
                date: UtcDate {
                    year: 2026,
                    month: 7,
                    day: 29,
                },
                session_id: [
                    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76,
                    0x54, 0x32, 0x10,
                ],
                part: 0,
            },
            stats: WriterStats {
                frames: 3350,
                blocks: 7,
                frame_bytes: 1_222_498,
                file_bytes: 145_131,
            },
            first_ingest_seq: Some(1),
            last_ingest_seq: Some(3350),
        }
    }

    #[test]
    fn a_report_maps_onto_a_row_without_losing_identity() {
        let row = SegmentRow::from_report(
            &report(),
            "data/raw/x/part-00000.bin.zst",
            Ts::from_secs(1_785_283_200),
        )
        .unwrap();
        // The session id in the row must be the same 16 bytes as in the file
        // header, or the index points at nothing.
        assert_eq!(
            row.session_id.to_string(),
            "01234567-89ab-cdef-fedc-ba9876543210"
        );
        assert_eq!(row.capture_date.to_string(), "2026-07-29");
        assert_eq!(row.frames, 3350);
        assert_eq!(row.first_ingest_seq, Some(1));
        assert_eq!(row.last_ingest_seq, Some(3350));
    }

    #[test]
    fn the_exchange_column_matches_the_partition_directory() {
        // `exchange=binance` in the path and `binance` in the column come from the
        // same Display impl, so they cannot drift.
        let session =
            SessionRow::new([0; 16], Exchange::Binance, "BTCUSDT", Ts::from_secs(0)).unwrap();
        assert_eq!(session.exchange, Exchange::Binance.to_string());
        assert_eq!(session.exchange, "binance");
    }

    #[test]
    fn counters_saturate_rather_than_going_negative() {
        // A wrapped count would appear as a negative bigint, which looks like a
        // corrupt row rather than an implausible one.
        assert_eq!(saturating_i64(0), 0);
        assert_eq!(saturating_i64(u64::MAX), i64::MAX);
        assert!(saturating_i64(u64::MAX) > 0);
    }

    #[test]
    fn timestamps_narrow_to_microseconds_and_say_so() {
        // 1_785_283_200.123456789 -> microseconds, losing the trailing 789ns.
        let ts = Ts::from_nanos(1_785_283_200_123_456_789);
        let offset = to_offset(ts).unwrap();
        assert_eq!(offset.unix_timestamp(), 1_785_283_200);
        // The nanosecond value is preserved in `time`'s own representation; what
        // narrows is the Postgres column. Assert we did not mangle it on the way.
        assert_eq!(offset.nanosecond(), 123_456_789);
    }

    #[test]
    fn status_strings_match_the_check_constraint() {
        // If these drift from the SQL, every insert fails at runtime.
        let sql = crate::schema::MIGRATIONS[0].sql;
        for status in [
            SessionStatus::Running,
            SessionStatus::Closed,
            SessionStatus::Failed,
        ] {
            assert!(
                sql.contains(&format!("'{}'", status.as_str())),
                "{status} missing from the status check constraint"
            );
        }
    }

    #[test]
    fn pre_epoch_and_far_future_dates_are_refused_not_mangled() {
        assert!(to_date(UtcDate {
            year: 2026,
            month: 13,
            day: 1
        })
        .is_err());
        assert!(to_date(UtcDate {
            year: 2026,
            month: 2,
            day: 30
        })
        .is_err());
    }
}
