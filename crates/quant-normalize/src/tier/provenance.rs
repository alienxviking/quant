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
//! So each file carries the session that wrote it in its Parquet footer. M2.d
//! used that to *refuse* the second session, which was safe rather than lossy but
//! left the day unnormalizable. The day is now a sequence of **parts**, one per
//! contributing session, and the footer is what makes that honest: every file
//! still names exactly one session, so nothing here has to describe two sources
//! at once. See [`super`] for why a concatenation is the right shape.
//!
//! Two things are recorded that are facts about the *rows* rather than about the
//! session, and so are appended once the part is written rather than handed to
//! the constructor:
//!
//! - `quant.part`, which earns its place by this module's own argument for
//!   repeating the exchange, symbol and date: a file moved out of its directory
//!   still says what it is.
//! - `quant.book_seq_first` / `quant.book_seq_last`, the venue's own update-id
//!   span over the part's book events.
//!
//! The span exists to order parts, and it is the venue's sequence rather than our
//! `local_recv_ts` **because our clock is not evidence about order**. It is
//! `SystemTime`, it steps, and it is not monotone even within one session —
//! `quant-recorder::segment` has a never-roll-backwards rule and a
//! `backdated_records` counter for exactly that reason. Ordering two sessions by
//! a quantity that can run backwards means a clock step at the seam either
//! refuses a healthy day or, worse, silently reverses them. The venue's update
//! ids are strictly increasing per symbol across disconnects and are immune to
//! anything our host does, so they are the honest discriminator.
//!
//! It is the *book* sequence and not the trade id because those are two different
//! namespaces, and a span that mixed them would compare numbers that mean
//! different things. Deltas and snapshots share one namespace, which
//! `quant-book` already relies on — and `BookDelta` carries the range in the M0
//! event contract, so using it here adds no venue knowledge to this crate.
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
/// Footer key naming which part of the day this file is.
pub const PART_KEY: &str = "quant.part";
/// Footer key holding the first venue book update id in this part.
pub const BOOK_SEQ_FIRST_KEY: &str = "quant.book_seq_first";
/// Footer key holding the last venue book update id in this part.
pub const BOOK_SEQ_LAST_KEY: &str = "quant.book_seq_last";

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
        Ok(Self::footer_value(path, SESSION_KEY)?
            .as_deref()
            .and_then(parse_session_id))
    }

    /// The venue book update-id span this part covers, if it saw any book event.
    ///
    /// `Ok(None)` for a part that saw only trades and gaps — possible in
    /// principle, and the reason callers must have an answer for "no span"
    /// rather than treating its absence as a defect.
    pub fn book_span_of(path: &Path) -> Result<Option<(u64, u64)>, TierError> {
        let first = Self::footer_value(path, BOOK_SEQ_FIRST_KEY)?;
        let last = Self::footer_value(path, BOOK_SEQ_LAST_KEY)?;
        match (first, last) {
            (Some(f), Some(l)) => Ok(f.parse().ok().zip(l.parse().ok())),
            _ => Ok(None),
        }
    }

    /// One footer value by key, or `None` if the file carries no such entry.
    ///
    /// One reader rather than one per key: three copies of "open the file, find
    /// the footer, match a key" is three places for the next key to be read
    /// slightly differently.
    fn footer_value(path: &Path, key: &str) -> Result<Option<String>, TierError> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
        let Some(entries) = builder.metadata().file_metadata().key_value_metadata() else {
            return Ok(None);
        };
        Ok(entries
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.clone()))
    }
}
