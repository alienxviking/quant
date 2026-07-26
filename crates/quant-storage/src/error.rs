//! Failure modes of the raw format.
//!
//! Every variant here means **this file is not what it claims to be**: wrong
//! magic, a checksum that does not match, a version we cannot interpret, a
//! sequence number that went backwards. They are all stop-and-look conditions.
//!
//! Note what is deliberately *absent*: a truncated tail. That is not an error,
//! it is the expected shape of a file whose writer was killed, and it is
//! reported as [`crate::Truncation`] instead. Conflating "the last block is
//! incomplete" with "this file is corrupt" would either make routine restarts
//! look like disasters, or -- much worse -- train us to ignore the error.

use core::fmt;
use std::io;

/// Result alias for the raw format.
pub type StorageResult<T> = Result<T, StorageError>;

/// Something is wrong with the file, not merely missing from the end of it.
#[derive(Debug)]
pub enum StorageError {
    /// Underlying I/O failure.
    Io(io::Error),

    /// The file did not begin with [`crate::MAGIC`].
    ///
    /// Almost always a path bug -- pointed at a Parquet file, a `.zst` that is
    /// not ours, or a directory listing artifact.
    NotARawFile { found: [u8; 8] },

    /// The file header's own checksum failed.
    HeaderChecksum { expected: u32, found: u32 },

    /// Written by a container format version this build does not understand.
    ///
    /// Refused rather than guessed at. The rule in `docs/data-contract.md` §6
    /// is that we retain the ability to read every version we have written, so
    /// hitting this means either a downgraded binary or a file from the future.
    UnsupportedContainerVersion { found: u16, supported: u16 },

    /// The header names a venue this build does not know.
    UnknownExchangeCode(u16),

    /// The file trailer's own checksum failed.
    TrailerChecksum { expected: u32, found: u32 },

    /// The trailer's declared totals disagree with what the reader actually
    /// decoded.
    ///
    /// The file contradicts itself, which means data went missing while the
    /// framing stayed valid -- precisely the failure a trailer exists to catch,
    /// and never something to shrug at.
    TrailerMismatch {
        field: &'static str,
        declared: u64,
        actual: u64,
    },

    /// Bytes followed the trailer.
    ///
    /// The trailer marks a closed file, so anything after it means two writers
    /// shared one path. `docs/data-contract.md` §5 forbids that: one file per
    /// capture session, never appended across a restart.
    DataAfterTrailer,

    /// The symbol field is unusable (empty, not UTF-8, or contains a NUL,
    /// which is the padding sentinel).
    InvalidSymbol(&'static str),

    /// Symbol does not fit the fixed-width field.
    ///
    /// A hard error rather than a truncation, because a silently shortened
    /// symbol would produce a file that claims to be about a different
    /// instrument than it is.
    SymbolTooLong { len: usize, max: usize },

    /// `ingest_seq` did not strictly increase.
    ///
    /// On write this is a recorder bug and we refuse rather than persist an
    /// unverifiable file. On read it means a file that passed its checksums
    /// still violates the ordering contract, which points at the writer.
    NonMonotonicIngestSeq { previous: u64, found: u64 },

    /// A single frame payload exceeded [`crate::MAX_PAYLOAD_LEN`].
    PayloadTooLarge { len: usize, max: u32 },

    /// A block, after decompression, was not the length its header declared.
    ///
    /// The block's checksum passed, so these really are our bytes -- meaning
    /// the writer and reader disagree about the format. That is a code bug,
    /// not a disk fault.
    BlockSizeMismatch { declared: u32, actual: usize },

    /// A frame header inside a checksum-verified block was cut short.
    ///
    /// Same reasoning as [`Self::BlockSizeMismatch`]: an intact block cannot
    /// contain a partial frame unless we wrote it wrong.
    MalformedFrame(&'static str),

    /// Unknown [`crate::FrameKind`] discriminant.
    UnknownFrameKind(u8),

    /// A control frame's JSON did not parse.
    Json(serde_json::Error),

    /// zstd refused to compress or decompress a block.
    Compression(String),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::NotARawFile { found } => {
                write!(
                    f,
                    "not a quant raw capture file: magic was {found:?}, expected {:?}",
                    crate::MAGIC
                )
            }
            Self::HeaderChecksum { expected, found } => {
                write!(
                    f,
                    "file header checksum mismatch: expected {expected:#010x}, found {found:#010x}"
                )
            }
            Self::UnsupportedContainerVersion { found, supported } => write!(
                f,
                "unsupported container version {found} (this build reads up to {supported})"
            ),
            Self::UnknownExchangeCode(code) => {
                write!(f, "unknown exchange wire code {code}")
            }
            Self::TrailerChecksum { expected, found } => write!(
                f,
                "file trailer checksum mismatch: expected {expected:#010x}, found {found:#010x}"
            ),
            Self::TrailerMismatch {
                field,
                declared,
                actual,
            } => write!(
                f,
                "file trailer declares {field} = {declared} but {actual} were decoded"
            ),
            Self::DataAfterTrailer => {
                write!(
                    f,
                    "bytes follow the file trailer: two writers shared a path"
                )
            }
            Self::InvalidSymbol(why) => write!(f, "invalid symbol field: {why}"),
            Self::SymbolTooLong { len, max } => {
                write!(f, "symbol is {len} bytes, field holds {max}")
            }
            Self::NonMonotonicIngestSeq { previous, found } => write!(
                f,
                "ingest_seq must strictly increase: {previous} was followed by {found}"
            ),
            Self::PayloadTooLarge { len, max } => {
                write!(f, "frame payload is {len} bytes, limit is {max}")
            }
            Self::BlockSizeMismatch { declared, actual } => write!(
                f,
                "block declared {declared} uncompressed bytes but decoded to {actual}"
            ),
            Self::MalformedFrame(why) => write!(f, "malformed frame: {why}"),
            Self::UnknownFrameKind(code) => write!(f, "unknown frame kind {code}"),
            Self::Json(e) => write!(f, "control record json error: {e}"),
            Self::Compression(msg) => write!(f, "compression error: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}
