//! Writing raw capture files.
//!
//! Generic over [`Write`] rather than tied to [`std::fs::File`], which is why
//! the crash-recovery behaviour in this crate is testable against an in-memory
//! buffer instead of requiring a real process kill.

use core::fmt;
use std::io::Write;

use quant_core::time::Ts;

use crate::error::{StorageError, StorageResult};
use crate::frame::{
    ControlRecord, FileHeader, FileTrailer, FrameHeader, FrameKind, BLOCK_HEADER_LEN, BLOCK_SYNC,
    MAX_PAYLOAD_LEN, TRAILER_LEN,
};

/// Byte counters are `u64`; every platform we build for has `usize` no wider,
/// and a bare `as` cast would be one of the silent-truncation patterns this
/// codebase avoids on principle.
fn as_u64(n: usize) -> u64 {
    u64::try_from(n).expect("usize wider than u64")
}

/// Tuning for the write path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterOptions {
    /// Frames accumulate until the pending block reaches at least this many
    /// uncompressed bytes, then the block is sealed.
    pub target_block_bytes: usize,
    /// zstd compression level.
    pub zstd_level: i32,
}

impl Default for WriterOptions {
    /// 256 KiB blocks at zstd level 3.
    ///
    /// **Block size** trades three things off at once. Larger blocks compress
    /// better, because consecutive venue messages are near-identical and zstd
    /// needs a window wide enough to see that. Larger blocks also mean more
    /// unsealed data in memory -- and therefore more data lost if the machine
    /// dies -- and coarser recovery granularity. 256 KiB is a few hundred depth
    /// messages: enough context for the ratio, small enough that losing one
    /// block loses about a second of a busy market.
    ///
    /// **Level 3** is zstd's default and the right point on its curve for this
    /// workload: roughly 3x on JSON at a speed the recorder will never notice.
    /// Level 19 buys maybe another 15% for an order of magnitude more CPU, which
    /// is not a trade a process whose real job is draining a socket should make.
    fn default() -> Self {
        Self {
            target_block_bytes: 256 * 1024,
            zstd_level: 3,
        }
    }
}

/// Counters for the metrics that M1 has to expose anyway.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriterStats {
    pub frames: u64,
    pub blocks: u64,
    /// Uncompressed frame bytes sealed into blocks, frame headers included.
    pub frame_bytes: u64,
    /// Bytes handed to the sink: file header plus block headers plus compressed
    /// payloads. Compare against `frame_bytes` for the achieved ratio.
    pub file_bytes: u64,
}

/// Appends frames to a raw capture file.
///
/// # Finishing
///
/// [`RawWriter::finish`] seals the final block and writes the trailer that marks
/// the file closed. There is deliberately no `Drop` impl that does this: a
/// destructor cannot report an I/O failure, so a flushing `Drop` would trade a
/// loud error for silently losing the tail.
///
/// Forgetting to call it degrades to exactly the case we already handle and
/// test -- an unsealed tail and no trailer, which the reader reports -- rather
/// than to undefined behaviour or a file that lies about being complete.
pub struct RawWriter<W: Write> {
    out: W,
    header: FileHeader,
    /// Uncompressed frames awaiting a block seal.
    pending: Vec<u8>,
    opts: WriterOptions,
    last_seq: Option<u64>,
    stats: WriterStats,
}

impl<W: Write> fmt::Debug for RawWriter<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawWriter")
            .field("header", &self.header)
            .field("opts", &self.opts)
            .field("pending_bytes", &self.pending.len())
            .field("last_seq", &self.last_seq)
            .field("stats", &self.stats)
            // The sink and the pending block buffer are deliberately omitted;
            // `pending_bytes` above is the part worth seeing in a log line.
            .finish_non_exhaustive()
    }
}

impl<W: Write> RawWriter<W> {
    /// Write the file header and prepare to append frames.
    ///
    /// The header goes out immediately rather than being buffered, so a file
    /// that exists on disk is always identifiable -- even one that was killed
    /// before its first block.
    pub fn create(mut out: W, header: FileHeader, opts: WriterOptions) -> StorageResult<Self> {
        let bytes = header.encode()?;
        out.write_all(&bytes)?;
        Ok(Self {
            out,
            header,
            pending: Vec::with_capacity(opts.target_block_bytes + 64 * 1024),
            opts,
            last_seq: None,
            stats: WriterStats {
                file_bytes: as_u64(bytes.len()),
                ..WriterStats::default()
            },
        })
    }

    /// Append venue bytes, verbatim.
    ///
    /// `payload` is not parsed, validated or normalized. That is the whole
    /// bargain of the raw tier: if our understanding of the venue's dialect is
    /// wrong, the bytes are still here to re-derive from.
    pub fn write_venue_payload(
        &mut self,
        local_recv_ts: Ts,
        ingest_seq: u64,
        payload: &[u8],
    ) -> StorageResult<()> {
        self.write_frame(FrameKind::VenuePayload, local_recv_ts, ingest_seq, payload)
    }

    /// Append a record we authored -- currently only gaps.
    pub fn write_control(
        &mut self,
        local_recv_ts: Ts,
        ingest_seq: u64,
        record: &ControlRecord,
    ) -> StorageResult<()> {
        let payload = record.to_json()?;
        self.write_frame(FrameKind::Control, local_recv_ts, ingest_seq, &payload)
    }

    fn write_frame(
        &mut self,
        kind: FrameKind,
        local_recv_ts: Ts,
        ingest_seq: u64,
        payload: &[u8],
    ) -> StorageResult<()> {
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| StorageError::PayloadTooLarge {
                len: payload.len(),
                max: MAX_PAYLOAD_LEN,
            })?;
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(StorageError::PayloadTooLarge {
                len: payload.len(),
                max: MAX_PAYLOAD_LEN,
            });
        }

        // Strictly increasing, not contiguous. A hole means a message was
        // dropped at ingress after its sequence number was already assigned,
        // and that hole -- paired with a LocalOverflow gap record -- is the
        // evidence of the drop. Refusing holes here would force the ingress
        // path to either lie or renumber, and renumbering destroys the only
        // proof we have of what was lost.
        //
        // Going backwards, by contrast, is a recorder bug, and persisting an
        // unverifiable file is worse than failing the write.
        if let Some(previous) = self.last_seq {
            if ingest_seq <= previous {
                return Err(StorageError::NonMonotonicIngestSeq {
                    previous,
                    found: ingest_seq,
                });
            }
        }

        let header = FrameHeader {
            kind,
            local_recv_ts,
            ingest_seq,
            payload_len,
        };
        self.pending.extend_from_slice(&header.encode());
        self.pending.extend_from_slice(payload);
        self.last_seq = Some(ingest_seq);
        self.stats.frames += 1;

        if self.pending.len() >= self.opts.target_block_bytes {
            self.seal_block()?;
        }
        Ok(())
    }

    /// Compress and emit the pending frames as one block.
    ///
    /// The block header and its payload go out as two `write_all` calls, so a
    /// dying *machine* can tear a block; a dying *process* cannot, because both
    /// calls have already reached the page cache. Either way the reader sees a
    /// short block and reports a truncation rather than misreading it.
    fn seal_block(&mut self) -> StorageResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        let uncompressed_len = u32::try_from(self.pending.len())
            .map_err(|_| StorageError::Compression("pending block exceeds u32".to_owned()))?;
        let compressed = zstd::bulk::compress(&self.pending, self.opts.zstd_level)
            .map_err(|e| StorageError::Compression(e.to_string()))?;
        let compressed_len = u32::try_from(compressed.len())
            .map_err(|_| StorageError::Compression("compressed block exceeds u32".to_owned()))?;

        let mut header = [0_u8; BLOCK_HEADER_LEN];
        header[0..4].copy_from_slice(&BLOCK_SYNC.to_le_bytes());
        header[4..8].copy_from_slice(&uncompressed_len.to_le_bytes());
        header[8..12].copy_from_slice(&compressed_len.to_le_bytes());
        header[12..16].copy_from_slice(&crc32fast::hash(&compressed).to_le_bytes());

        self.out.write_all(&header)?;
        self.out.write_all(&compressed)?;

        self.stats.blocks += 1;
        self.stats.frame_bytes += u64::from(uncompressed_len);
        self.stats.file_bytes += as_u64(BLOCK_HEADER_LEN) + u64::from(compressed_len);
        self.pending.clear();
        Ok(())
    }

    /// Seal the pending block and flush the sink.
    ///
    /// Worth calling on a timer as well as on volume: a quiet instrument would
    /// otherwise leave frames unsealed for hours, and unsealed frames are frames
    /// a crash loses.
    pub fn flush(&mut self) -> StorageResult<()> {
        self.seal_block()?;
        self.out.flush()?;
        Ok(())
    }

    /// Seal the last block, write the trailer, and return the sink so the caller
    /// can `sync_all` or rename it.
    ///
    /// The trailer is what turns "I reached the end of the file" into "the
    /// writer says it wrote exactly this much" -- see [`FileTrailer`]. It is
    /// written only here, never by [`RawWriter::flush`], because flush runs on a
    /// timer and a file may be flushed thousands of times before it closes.
    ///
    /// Returns the final [`WriterStats`] as well as the sink. Reading them
    /// beforehand would miss the trailer's own bytes, and "what did this file
    /// actually cost" is a question worth being able to answer exactly.
    pub fn finish(mut self) -> StorageResult<(W, WriterStats)> {
        self.seal_block()?;
        let trailer = FileTrailer {
            frames: self.stats.frames,
            blocks: self.stats.blocks,
            last_ingest_seq: self.last_seq,
        };
        self.out.write_all(&trailer.encode())?;
        self.stats.file_bytes += as_u64(TRAILER_LEN);
        self.out.flush()?;
        Ok((self.out, self.stats))
    }

    #[must_use]
    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    #[must_use]
    pub fn stats(&self) -> WriterStats {
        self.stats
    }

    /// Uncompressed bytes not yet sealed into a block -- i.e. what a machine
    /// failure would lose right now.
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }

    /// Highest `ingest_seq` accepted so far.
    #[must_use]
    pub fn last_ingest_seq(&self) -> Option<u64> {
        self.last_seq
    }
}
