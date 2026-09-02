//! What a normalized file says about where it came from.
//!
//! # Why a derived file has to name its source
//!
//! The normalized layout is `exchange / symbol / date`, per the contract's §5.
//! There is no session in that path, and there should not be — the tier is about
//! what the *market* did, and which of our capture runs happened to see it is a
//! capture-side concern that belongs to raw and to Postgres.
//!
//! But a recorder restart creates a new session, and both sessions can hold
//! segments for the same symbol on the same day. Without provenance, normalizing
//! the second one would publish over the first's partition and lose it silently
//! — the worst kind of failure this project has, because the file that remains
//! looks complete.
//!
//! So each file carries the session that wrote it in its Parquet footer, and the
//! writer refuses a partition another session owns. Refusing is not the eventual
//! answer: two sessions covering one day should be *merged*, ordered by
//! `local_recv_ts` since `ingest_seq` is session-scoped and cannot order across
//! them. That is real work and it is not this slice's. Refusing is what makes
//! deferring it safe rather than lossy.
//!
//! Footer key-values rather than a sidecar file, because a sidecar can be
//! separated from what it describes, and every Parquet reader in existence can
//! already see these.

use std::path::Path;

use parquet::file::metadata::KeyValue;
use quant_core::instrument::Exchange;
use quant_core::time::UtcDate;
use quant_recorder::{format_session_id, parse_session_id};

use super::TierError;

/// Footer key naming the capture session a file was derived from.
pub const SESSION_KEY: &str = "quant.session_id";
/// Footer key naming the venue.
pub const EXCHANGE_KEY: &str = "quant.exchange";
/// Footer key naming the venue's own symbol.
pub const SYMBOL_KEY: &str = "quant.symbol";
/// Footer key naming the UTC date partition.
pub const DATE_KEY: &str = "quant.date";

/// Where a normalized file came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provenance {
    pub session_id: [u8; 16],
    pub exchange: Exchange,
    pub symbol: String,
    pub date: UtcDate,
}

impl Provenance {
    /// The footer entries for this file.
    ///
    /// The exchange, symbol and date duplicate the partition path on purpose:
    /// a file moved or copied out of its directory still says what it is, which
    /// is the same reason a raw capture repeats its identity in its header.
    #[must_use]
    pub fn key_values(&self) -> Vec<KeyValue> {
        vec![
            KeyValue::new(SESSION_KEY.to_owned(), format_session_id(&self.session_id)),
            KeyValue::new(EXCHANGE_KEY.to_owned(), self.exchange.to_string()),
            KeyValue::new(SYMBOL_KEY.to_owned(), self.symbol.clone()),
            KeyValue::new(DATE_KEY.to_owned(), self.date.to_string()),
        ]
    }

    /// Read the session id out of an existing normalized file.
    ///
    /// `Ok(None)` means the file is readable but carries no session — written by
    /// an older build, or by something else entirely. Treated as "unknown", not
    /// as "mine": overwriting a file whose origin we cannot establish is the
    /// behaviour this module exists to prevent.
    pub fn session_of(path: &Path) -> Result<Option<[u8; 16]>, TierError> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
        let Some(entries) = builder.metadata().file_metadata().key_value_metadata() else {
            return Ok(None);
        };
        Ok(entries
            .iter()
            .find(|kv| kv.key == SESSION_KEY)
            .and_then(|kv| kv.value.as_deref())
            .and_then(parse_session_id))
    }
}
