//! The on-disk format for the **raw** capture tier.
//!
//! # Why this crate exists before the recorder
//!
//! Raw capture is the only artifact in this platform that cannot be
//! regenerated. A bug in the backtester costs an afternoon; a bug in this file
//! format costs however many weeks of market data we recorded before noticing.
//! Everything downstream -- the normalized Parquet tier, book reconstruction,
//! backtest results -- is derived state that can be rebuilt.
//!
//! So the format is specified, implemented and tested against synthetic bytes
//! *before* a socket is ever opened. This crate depends on no venue and no
//! network, which is what makes the nastiest property -- "a `SIGKILL` mid-write
//! leaves the file readable up to the last complete frame" -- testable as an
//! ordinary unit test instead of an operational anecdote.
//!
//! # Layout
//!
//! One file is one `(exchange, symbol, capture session)`. That matches the
//! Hive partitioning in `docs/data-contract.md` §5, and it makes `ingest_seq`
//! -- which is per `(recorder process, instrument)` -- strictly increasing
//! within a single file, so completeness is checkable per file with no
//! cross-file bookkeeping.
//!
//! ```text
//! file := FileHeader Block* FileTrailer?
//!
//! FileHeader (68 bytes, fixed)
//!   magic                 [8]   b"QUANTRAW"
//!   container_version     u16   this framing format's version
//!   event_schema_version  u16   quant_core::EVENT_SCHEMA_VERSION
//!   exchange              u16   Exchange::wire_code()
//!   flags                 u16   reserved, must be 0
//!   session_id            [16]  opaque capture-session identifier
//!   symbol                [32]  venue symbol, NUL-padded
//!   crc32                 u32   over the preceding 64 bytes
//!
//! Block
//!   sync                  u32   0x51524142, "QRAB"
//!   uncompressed_len      u32   byte length of the frame run below
//!   compressed_len        u32   byte length of the zstd payload
//!   crc32                 u32   over the compressed bytes
//!   compressed            [compressed_len]  zstd of: Frame*
//!
//! Frame (21-byte header, inside a block's uncompressed bytes)
//!   kind                  u8    0 = venue stream payload
//!                               1 = control record (ours, JSON)
//!                               2 = venue snapshot (REST body, verbatim)
//!   local_recv_ts         i64   epoch nanos, stamped at socket read
//!   ingest_seq            u64   ours, strictly increasing within the file
//!   payload_len           u32
//!   payload               [payload_len]
//!
//! FileTrailer (32 bytes; present only if the writer closed cleanly)
//!   sync                  u32   0x444E4551, "QEND"
//!   frames                u64
//!   blocks                u64
//!   last_ingest_seq       u64
//!   crc32                 u32   over the preceding 28 bytes
//! ```
//!
//! All multi-byte integers are little-endian. Stated rather than assumed, so a
//! big-endian reader fails loudly instead of misreading prices.
//!
//! # The four decisions worth arguing about
//!
//! ## 1. Compression is per *block*, not per frame or per file
//!
//! Per-frame zstd compresses a 200-byte depth message badly: there is no
//! context to find redundancy in, and every frame pays framing overhead.
//! Whole-file streaming zstd compresses best, but then a truncated file is a
//! truncated *codec stream*, and corruption anywhere destroys everything after
//! it.
//!
//! Batching frames into an independently compressed, independently checksummed
//! block gets nearly all of the streaming ratio -- consecutive Binance
//! messages are extremely repetitive, and a 256 KiB window sees plenty of that
//! -- while keeping any damage confined to one block. It is the same reason
//! Parquet has row groups and Kafka has record batches.
//!
//! Block size also bounds how much data a kill can lose, which is the real
//! argument against making blocks large.
//!
//! ## 2. Frames are typed, because gap events are not venue bytes
//!
//! The data contract says raw holds "exactly the bytes the venue sent". A
//! [`MarketEvent::Gap`] is not that -- we synthesized it. It still has to live
//! in the same file and the same sequence, because a gap record in a sidecar
//! file is a gap record that can disagree with the stream it describes.
//!
//! Hence [`FrameKind`]: `VenuePayload` frames are opaque bytes we never
//! interpret, `Control` frames are ours. See [`ControlRecord`].
//!
//! The same argument gives book snapshots their own kind rather than sharing
//! `VenuePayload`. A REST snapshot body is equally the venue's bytes, but it is a
//! different JSON shape answering a different question, and a snapshot misread as
//! a delta corrupts a book silently instead of failing. One byte of frame kind
//! replaces a try-parse-both on every frame in the file, forever.
//!
//! ## 3. Holes in `ingest_seq` are legal, and they are evidence
//!
//! `ingest_seq` is stamped at ingress, in the same breath as `local_recv_ts`,
//! *before* the bounded channel to the writer. So when the channel is full and
//! we drop a message, the sequence number assigned to it is simply never
//! written -- and the hole in the file, paired with a `Control` frame carrying
//! [`GapCause::LocalOverflow`], is the complete account of what was lost.
//!
//! The writer therefore enforces strictly *increasing*, not contiguous,
//! sequence numbers. Contiguity-or-explained is a stronger property that needs
//! to correlate holes against gap records; that belongs to the offline
//! verifier, not to the write path.
//!
//! ## 4. A torn tail is a report, not an error
//!
//! [`RawReader`] stops cleanly at a partial block and records a
//! [`Truncation`]. It does not error, because after a `SIGKILL` a torn tail is
//! the *expected* state of the file, and it does not silently ignore it
//! either, because "we lost the last 40 KiB" is exactly the kind of thing that
//! must never be invisible.
//!
//! A checksum failure is reported through the same channel but flagged by
//! [`TruncationReason::is_corruption`]: a torn tail after a crash is routine,
//! whereas bytes that are not the bytes we wrote means the disk lied and
//! somebody should be woken up.
//!
//! ## 5. The trailer, so a file can account for itself
//!
//! Without a trailer, the strongest completeness claim available is "I read to
//! the end and nothing was torn" -- and that is weaker than it sounds, because a
//! file cut exactly on a block boundary is byte-identical to one that ended
//! there deliberately. A capture killed a millisecond after it opened would be
//! indistinguishable from one that recorded nothing on purpose.
//!
//! [`RawWriter::finish`] therefore writes a [`FileTrailer`] stating how many
//! frames and blocks it wrote and the last `ingest_seq` it issued;
//! [`RawReader`] cross-checks all three against what it actually decoded and
//! errors on disagreement. So the M1 completeness criterion becomes an
//! independent two-sided check rather than an absence of complaints.
//!
//! The check lives in the file rather than in Postgres deliberately. The data
//! contract makes raw the source of truth and Postgres merely metadata; if
//! validating a capture required the database, that ordering would be inverted.
//!
//! An absent trailer is not an error -- a file still being written does not have
//! one yet. It does mean the file may not be reported as complete, which
//! [`RawReader::is_finalized`] exists to make unavoidable.
//!
//! [`FileTrailer`]: frame::FileTrailer
//!
//! ## Accepted limitation
//!
//! Damage in the *middle* of a file stops the read at that point; the reader
//! does not scan forward for the next block to salvage the remainder. Each
//! block is written with a sync word so that recovery is possible later
//! without a format change, but the code is not written until we have ever
//! observed mid-file corruption. Our realistic failure mode is a torn tail
//! from a kill, not bit-rot, and speculative recovery paths are untested
//! recovery paths.
//!
//! # Durability
//!
//! Nothing here calls `fsync`. A block is handed to the OS in one `write_all`
//! and is therefore intact in the page cache the moment the call returns, so
//! **process** death -- panic, `SIGKILL`, OOM kill -- cannot tear a block that
//! has been flushed. **Machine** death can, and the reader treats that as a
//! torn tail. Deciding when to `fsync` is the caller's business: it owns the
//! [`std::fs::File`], and paying a flush per block would cost more throughput
//! than a crypto recorder can spare for a guarantee we do not need.
//!
//! [`MarketEvent::Gap`]: quant_core::MarketEvent::Gap
//! [`GapCause::LocalOverflow`]: quant_core::GapCause::LocalOverflow

pub mod error;
pub mod frame;
pub mod reader;
pub mod writer;

pub use error::{StorageError, StorageResult};
pub use frame::{
    ControlRecord, FileHeader, FileTrailer, FrameHeader, FrameKind, RawFrame, SnapshotFailure,
    SnapshotPurpose, BLOCK_HEADER_LEN, BLOCK_SYNC, CONTAINER_VERSION, FILE_HEADER_LEN,
    FRAME_HEADER_LEN, MAGIC, MAX_BLOCK_UNCOMPRESSED, MAX_PAYLOAD_LEN, SYMBOL_FIELD_LEN,
    TRAILER_LEN, TRAILER_SYNC,
};
pub use reader::{RawReader, ReaderStats, Truncation, TruncationReason};
pub use writer::{RawWriter, WriterOptions, WriterStats};
