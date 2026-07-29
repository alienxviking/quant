//! Reading raw capture files, including the ones that were killed mid-write.
//!
//! # The contract this module implements
//!
//! From `docs/data-contract.md` §7: *"`SIGKILL` mid-write leaves the last file
//! readable up to the last complete frame — no partial-frame corruption."*
//!
//! Three behaviours together make that true:
//!
//! 1. A partial trailing block ends the read **cleanly**. It is not an error,
//!    because after a kill it is the file's expected shape.
//! 2. What was lost is **reported**, via [`RawReader::truncation`]. Silently
//!    swallowing a torn tail would make "we lost the last 40 KiB of Tuesday"
//!    invisible, and invisible data loss is the failure mode this whole tier
//!    exists to prevent.
//! 3. Damage that is *not* a torn tail -- a failed checksum, a missing sync word
//!    -- is reported through the same channel but flagged by
//!    [`TruncationReason::is_corruption`], because a crash is routine and a
//!    lying disk is not.
//!
//! A frame cut short *inside* a checksum-verified block is a different thing
//! again: the bytes are provably ours, so the writer and reader disagree about
//! the format. That is a code bug and it surfaces as
//! [`StorageError::MalformedFrame`], never as a truncation.

use core::fmt;
use std::io::{self, Read};

use crate::error::{StorageError, StorageResult};
use crate::frame::{
    FileHeader, FileTrailer, FrameHeader, RawFrame, BLOCK_HEADER_LEN, BLOCK_SYNC, FILE_HEADER_LEN,
    FRAME_HEADER_LEN, MAX_BLOCK_UNCOMPRESSED, TRAILER_LEN, TRAILER_SYNC,
};

/// Why the reader stopped before the end of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TruncationReason {
    /// Fewer bytes than a block header remained. Routine after a kill.
    ShortBlockHeader,
    /// A block header declared more bytes than the file actually contains.
    /// Routine after a kill.
    ShortBlockBody,
    /// A block did not begin with the sync word: we are misaligned, which means
    /// the bytes are not laid out the way we wrote them.
    BadSync,
    /// A block's checksum failed: what is on disk is not what we wrote.
    BlockChecksum,
    /// A block header declared an implausible decompressed size.
    ///
    /// Rejected *before* allocating, so a corrupt length field cannot turn into
    /// a multi-gigabyte allocation.
    ImplausibleBlockLength,
    /// The trailer itself was cut short: the writer died inside `finish`.
    /// Benign -- every frame before it is intact.
    ShortTrailer,
}

impl TruncationReason {
    /// Whether this indicates damage rather than an interrupted write.
    ///
    /// The operational difference is the whole point. A torn tail after a
    /// `SIGKILL` or a restart is expected and needs no more than a log line.
    /// Corruption means the storage layer returned bytes we did not write, and
    /// nothing else in the capture can be trusted until someone looks.
    #[must_use]
    pub const fn is_corruption(self) -> bool {
        matches!(
            self,
            Self::BadSync | Self::BlockChecksum | Self::ImplausibleBlockLength
        )
    }
}

impl fmt::Display for TruncationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::ShortBlockHeader => "incomplete block header",
            Self::ShortBlockBody => "incomplete block body",
            Self::BadSync => "block sync word missing",
            Self::BlockChecksum => "block checksum mismatch",
            Self::ImplausibleBlockLength => "implausible block length",
            Self::ShortTrailer => "incomplete file trailer",
        };
        f.write_str(s)
    }
}

/// What the reader could not use, and where it gave up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Truncation {
    pub reason: TruncationReason,
    /// Offset from the start of the file at which the unusable block began.
    pub byte_offset: u64,
    /// Bytes from `byte_offset` to end of file: everything we could not decode.
    pub bytes_discarded: u64,
}

impl fmt::Display for Truncation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at byte {}: {} bytes discarded",
            self.reason, self.byte_offset, self.bytes_discarded
        )
    }
}

/// What a completed read saw.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReaderStats {
    pub frames: u64,
    pub blocks: u64,
    pub bytes_consumed: u64,
}

/// Sequential reader over a raw capture file.
pub struct RawReader<R: Read> {
    src: R,
    header: FileHeader,
    /// Current decompressed block and cursor into it.
    block: Vec<u8>,
    cursor: usize,
    /// Byte offset in the file at which the current block's header started.
    block_offset: u64,
    pos: u64,
    finished: bool,
    truncation: Option<Truncation>,
    trailer: Option<FileTrailer>,
    last_seq: Option<u64>,
    stats: ReaderStats,
}

impl<R: Read> fmt::Debug for RawReader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawReader")
            .field("header", &self.header)
            .field("pos", &self.pos)
            .field("finished", &self.finished)
            .field("truncation", &self.truncation)
            .field("trailer", &self.trailer)
            .field("stats", &self.stats)
            // The decompressed block buffer is deliberately omitted: it holds up
            // to 64 MiB and dumping it into a log line helps nobody.
            .finish_non_exhaustive()
    }
}

impl<R: Read> RawReader<R> {
    /// Read and validate the file header.
    ///
    /// Fails if the header itself is absent or incomplete: a file too short to
    /// identify is not a capture file with no data, it is a file we cannot say
    /// anything about, and returning "zero frames, all fine" for it would be a
    /// lie of exactly the kind this tier must not tell.
    pub fn open(mut src: R) -> StorageResult<Self> {
        let mut bytes = [0_u8; FILE_HEADER_LEN];
        src.read_exact(&mut bytes)?;
        let header = FileHeader::decode(&bytes)?;
        Ok(Self {
            src,
            header,
            block: Vec::new(),
            cursor: 0,
            block_offset: as_u64(FILE_HEADER_LEN),
            pos: as_u64(FILE_HEADER_LEN),
            finished: false,
            truncation: None,
            trailer: None,
            last_seq: None,
            stats: ReaderStats {
                bytes_consumed: as_u64(FILE_HEADER_LEN),
                ..ReaderStats::default()
            },
        })
    }

    #[must_use]
    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    /// What was lost, if the read did not reach a clean end of file.
    ///
    /// Only meaningful once [`RawReader::next_frame`] has returned `None`.
    #[must_use]
    pub fn truncation(&self) -> Option<Truncation> {
        self.truncation
    }

    #[must_use]
    pub fn stats(&self) -> ReaderStats {
        self.stats
    }

    /// The trailer, if the writer closed this file cleanly.
    ///
    /// Only meaningful once [`RawReader::next_frame`] has returned `None`. Its
    /// presence is the strongest completeness statement available about a
    /// capture: the counts have already been cross-checked against what was
    /// actually decoded, so `Some` means the file accounts for itself.
    #[must_use]
    pub fn trailer(&self) -> Option<FileTrailer> {
        self.trailer
    }

    /// Whether the writer declared this file closed.
    ///
    /// `false` for a file still being written as well as for one whose recorder
    /// died, and those are not distinguishable from the bytes alone -- which is
    /// why the recorder also records session state in the metadata database.
    #[must_use]
    pub fn is_finalized(&self) -> bool {
        self.trailer.is_some()
    }

    /// Next frame, or `None` at end of data.
    ///
    /// `None` covers both a clean end of file and a stop caused by a torn or
    /// damaged tail; [`RawReader::truncation`] distinguishes them. Callers that
    /// must not silently accept partial data should check it -- the offline
    /// verifier does.
    pub fn next_frame(&mut self) -> Option<StorageResult<RawFrame>> {
        loop {
            if self.cursor < self.block.len() {
                let result = self.decode_frame();
                if result.is_err() {
                    self.finished = true;
                }
                return Some(result);
            }
            if self.finished {
                return None;
            }
            match self.load_block() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(e) => {
                    self.finished = true;
                    return Some(Err(e));
                }
            }
        }
    }

    /// Read every remaining frame, and report what was lost.
    ///
    /// Convenience for tests and for tools that want the whole file; a
    /// long-running consumer should iterate instead of buffering a day of ticks.
    pub fn read_all(&mut self) -> StorageResult<(Vec<RawFrame>, Option<Truncation>)> {
        let mut frames = Vec::new();
        while let Some(frame) = self.next_frame() {
            frames.push(frame?);
        }
        Ok((frames, self.truncation))
    }

    /// Decode one frame from the current decompressed block.
    ///
    /// Every failure here is a format bug, not a disk fault: the block's
    /// checksum has already passed, so these bytes are provably the ones we
    /// wrote.
    fn decode_frame(&mut self) -> StorageResult<RawFrame> {
        let available = self.block.len() - self.cursor;
        if available < FRAME_HEADER_LEN {
            return Err(StorageError::MalformedFrame(
                "block ended inside a frame header",
            ));
        }
        let raw: [u8; FRAME_HEADER_LEN] = self.block[self.cursor..self.cursor + FRAME_HEADER_LEN]
            .try_into()
            .expect("slice is FRAME_HEADER_LEN bytes");
        let header = FrameHeader::decode(&raw)?;
        self.cursor += FRAME_HEADER_LEN;

        let payload_len = usize::try_from(header.payload_len)
            .map_err(|_| StorageError::MalformedFrame("payload length exceeds usize"))?;
        if self.block.len() - self.cursor < payload_len {
            return Err(StorageError::MalformedFrame(
                "block ended inside a frame payload",
            ));
        }
        let payload = self.block[self.cursor..self.cursor + payload_len].to_vec();
        self.cursor += payload_len;

        // The write path refuses to emit these out of order, so a violation in a
        // checksum-clean file points at the writer. Checked here as well because
        // every consumer of raw capture comes through this method, which makes
        // it the one place the ordering property cannot be bypassed.
        if let Some(previous) = self.last_seq {
            if header.ingest_seq <= previous {
                return Err(StorageError::NonMonotonicIngestSeq {
                    previous,
                    found: header.ingest_seq,
                });
            }
        }
        self.last_seq = Some(header.ingest_seq);
        self.stats.frames += 1;

        Ok(RawFrame {
            kind: header.kind,
            local_recv_ts: header.local_recv_ts,
            ingest_seq: header.ingest_seq,
            payload,
        })
    }

    /// Load the next block, or consume the trailer. `Ok(false)` means stop --
    /// cleanly or with a recorded truncation.
    fn load_block(&mut self) -> StorageResult<bool> {
        self.block_offset = self.pos;

        // The sync word is read on its own because a block header and a trailer
        // are different lengths, and which one is coming is only knowable after
        // reading it.
        let mut sync_bytes = [0_u8; 4];
        let got = self.read_up_to(&mut sync_bytes)?;
        if got == 0 {
            // End of data with no trailer: everything decoded, but the writer
            // never declared the file closed. Either it is still being written
            // or it was killed. Reported, not silently treated as complete.
            self.finished = true;
            return Ok(false);
        }
        if got < 4 {
            return self.stop(TruncationReason::ShortBlockHeader, as_u64(got));
        }
        let sync = u32::from_le_bytes(sync_bytes);

        if sync == TRAILER_SYNC {
            return self.read_trailer(sync_bytes);
        }
        if sync != BLOCK_SYNC {
            return self.stop(TruncationReason::BadSync, as_u64(got));
        }

        let mut header = [0_u8; BLOCK_HEADER_LEN - 4];
        let got = self.read_up_to(&mut header)?;
        if got < header.len() {
            return self.stop(TruncationReason::ShortBlockHeader, as_u64(4 + got));
        }

        let uncompressed_len =
            u32::from_le_bytes(header[0..4].try_into().expect("slice is 4 bytes"));
        let compressed_len = u32::from_le_bytes(header[4..8].try_into().expect("slice is 4 bytes"));
        let expected_crc = u32::from_le_bytes(header[8..12].try_into().expect("slice is 4 bytes"));

        // Bounded before any allocation, on both lengths. A corrupt length field
        // must not be able to make us ask for gigabytes.
        if uncompressed_len == 0
            || uncompressed_len > MAX_BLOCK_UNCOMPRESSED
            || compressed_len == 0
            || compressed_len > MAX_BLOCK_UNCOMPRESSED
        {
            return self.stop(
                TruncationReason::ImplausibleBlockLength,
                as_u64(BLOCK_HEADER_LEN),
            );
        }

        let capacity = usize::try_from(compressed_len)
            .map_err(|_| StorageError::MalformedFrame("block length exceeds usize"))?;
        let mut compressed = vec![0_u8; capacity];
        let got = self.read_up_to(&mut compressed)?;
        if got < capacity {
            return self.stop(
                TruncationReason::ShortBlockBody,
                as_u64(BLOCK_HEADER_LEN) + as_u64(got),
            );
        }

        if crc32fast::hash(&compressed) != expected_crc {
            return self.stop(
                TruncationReason::BlockChecksum,
                as_u64(BLOCK_HEADER_LEN) + as_u64(capacity),
            );
        }

        let target = usize::try_from(uncompressed_len)
            .map_err(|_| StorageError::MalformedFrame("block length exceeds usize"))?;
        let decoded = zstd::bulk::decompress(&compressed, target)
            .map_err(|e| StorageError::Compression(e.to_string()))?;
        if decoded.len() != target {
            return Err(StorageError::BlockSizeMismatch {
                declared: uncompressed_len,
                actual: decoded.len(),
            });
        }

        self.block = decoded;
        self.cursor = 0;
        self.stats.blocks += 1;
        self.stats.bytes_consumed = self.pos;
        Ok(true)
    }

    /// Consume and verify the trailer, cross-checking it against what we decoded.
    ///
    /// A disagreement is a hard error, not a truncation. The framing was valid
    /// all the way through and the checksums passed, so a count that does not
    /// match means frames went missing without leaving any other trace -- which
    /// is the exact failure the trailer was added to catch.
    fn read_trailer(&mut self, sync_bytes: [u8; 4]) -> StorageResult<bool> {
        let mut rest = [0_u8; TRAILER_LEN - 4];
        let got = self.read_up_to(&mut rest)?;
        if got < rest.len() {
            // Killed during finish(): benign, and the frames before it are all
            // still good.
            return self.stop(TruncationReason::ShortTrailer, as_u64(4 + got));
        }

        let mut bytes = [0_u8; TRAILER_LEN];
        bytes[..4].copy_from_slice(&sync_bytes);
        bytes[4..].copy_from_slice(&rest);
        let trailer = FileTrailer::decode(&bytes)?;

        if trailer.frames != self.stats.frames {
            return Err(StorageError::TrailerMismatch {
                field: "frames",
                declared: trailer.frames,
                actual: self.stats.frames,
            });
        }
        if trailer.blocks != self.stats.blocks {
            return Err(StorageError::TrailerMismatch {
                field: "blocks",
                declared: trailer.blocks,
                actual: self.stats.blocks,
            });
        }
        if trailer.last_ingest_seq != self.last_seq {
            return Err(StorageError::TrailerMismatch {
                field: "last_ingest_seq",
                declared: trailer.last_ingest_seq.unwrap_or(0),
                actual: self.last_seq.unwrap_or(0),
            });
        }

        // Nothing may follow a closed file. One file per capture session, never
        // appended across a restart -- see `docs/data-contract.md` §5.
        if self.discard_rest()? != 0 {
            return Err(StorageError::DataAfterTrailer);
        }

        self.trailer = Some(trailer);
        self.finished = true;
        self.stats.bytes_consumed = self.pos;
        Ok(false)
    }

    /// Record a truncation, account for everything past it, and stop.
    fn stop(&mut self, reason: TruncationReason, consumed_in_block: u64) -> StorageResult<bool> {
        // Drain so `bytes_discarded` is the honest total, not just what we had
        // already read when we noticed. An operator asking "how much of Tuesday
        // did we lose?" needs the real number.
        let remaining = self.discard_rest()?;
        self.truncation = Some(Truncation {
            reason,
            byte_offset: self.block_offset,
            bytes_discarded: consumed_in_block + remaining,
        });
        self.finished = true;
        self.block.clear();
        self.cursor = 0;
        self.stats.bytes_consumed = self.block_offset;
        Ok(false)
    }

    /// Fill `buf` as far as the source allows, returning how many bytes arrived.
    ///
    /// Distinct from `read_exact`, which cannot tell us *how much* of a partial
    /// read succeeded -- and that count is precisely what we need in order to
    /// report a torn tail accurately.
    fn read_up_to(&mut self, buf: &mut [u8]) -> StorageResult<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.src.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(StorageError::Io(e)),
            }
        }
        self.pos += as_u64(filled);
        Ok(filled)
    }

    fn discard_rest(&mut self) -> StorageResult<u64> {
        // Heap rather than stack: this only runs on a truncation or at close, so
        // the allocation is free, and a 64 KiB stack frame is not worth it.
        let mut scratch = vec![0_u8; 64 * 1024];
        let mut total = 0;
        loop {
            match self.src.read(&mut scratch) {
                Ok(0) => return Ok(total),
                Ok(n) => total += as_u64(n),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(StorageError::Io(e)),
            }
        }
    }
}

fn as_u64(n: usize) -> u64 {
    u64::try_from(n).expect("usize wider than u64")
}

#[cfg(test)]
mod tests {
    use quant_core::event::GapCause;
    use quant_core::instrument::Exchange;
    use quant_core::time::Ts;

    use super::*;
    use crate::frame::{ControlRecord, FrameKind};
    use crate::writer::{RawWriter, WriterOptions};

    const SESSION: [u8; 16] = [7; 16];

    fn header() -> FileHeader {
        FileHeader::new(Exchange::Binance, "BTCUSDT", SESSION)
    }

    /// A depth message shaped like Binance's, so compression sees realistic
    /// redundancy rather than random noise.
    fn depth_payload(n: u64) -> Vec<u8> {
        format!(
            r#"{{"e":"depthUpdate","E":{},"s":"BTCUSDT","U":{},"u":{},"b":[["68123.45","0.5"]],"a":[["68123.46","1.25"]]}}"#,
            1_700_000_000_000_u64 + n,
            n * 10,
            n * 10 + 9
        )
        .into_bytes()
    }

    /// Write `count` venue payloads plus one gap record, returning the bytes.
    fn capture(count: u64, opts: WriterOptions) -> (Vec<u8>, Vec<RawFrame>) {
        let mut writer = RawWriter::create(Vec::new(), header(), opts).unwrap();
        let mut expected = Vec::new();

        for i in 0..count {
            let payload = depth_payload(i);
            let ts = Ts::from_millis(1_700_000_000_000 + i64::try_from(i).unwrap());
            writer.write_venue_payload(ts, i + 1, &payload).unwrap();
            expected.push(RawFrame {
                kind: FrameKind::VenuePayload,
                local_recv_ts: ts,
                ingest_seq: i + 1,
                payload,
            });
        }

        let gap = ControlRecord::Gap {
            cause: GapCause::Disconnect,
            exchange_ts: Ts::from_millis(1_700_000_100_000),
            last_good_ts: Ts::from_millis(1_700_000_099_000),
        };
        writer
            .write_control(Ts::from_millis(1_700_000_100_000), count + 1, &gap)
            .unwrap();
        expected.push(RawFrame {
            kind: FrameKind::Control,
            local_recv_ts: Ts::from_millis(1_700_000_100_000),
            ingest_seq: count + 1,
            payload: gap.to_json().unwrap(),
        });

        (writer.finish().unwrap().0, expected)
    }

    #[test]
    fn frames_round_trip_through_blocks() {
        let (bytes, expected) = capture(500, WriterOptions::default());
        let mut reader = RawReader::open(bytes.as_slice()).unwrap();
        assert_eq!(reader.header(), &header());

        let (frames, truncation) = reader.read_all().unwrap();
        assert_eq!(frames, expected);
        assert_eq!(truncation, None);
        assert_eq!(reader.stats().frames, 501);
        assert_eq!(reader.stats().bytes_consumed, as_u64(bytes.len()));

        // The file accounts for itself: the trailer's counts were cross-checked
        // against what was actually decoded before we got here.
        assert!(reader.is_finalized());
        let trailer = reader.trailer().unwrap();
        assert_eq!(trailer.frames, 501);
        assert_eq!(trailer.last_ingest_seq, Some(501));
        assert_eq!(trailer.blocks, reader.stats().blocks);
    }

    #[test]
    fn control_frames_are_recognisable_in_the_stream() {
        let (bytes, _) = capture(3, WriterOptions::default());
        let mut reader = RawReader::open(bytes.as_slice()).unwrap();
        let (frames, _) = reader.read_all().unwrap();

        let controls: Vec<_> = frames
            .iter()
            .filter_map(|f| f.control().transpose())
            .collect::<StorageResult<Vec<_>>>()
            .unwrap();
        assert_eq!(controls.len(), 1);
        assert!(matches!(
            controls[0],
            ControlRecord::Gap {
                cause: GapCause::Disconnect,
                ..
            }
        ));
        // Venue payloads stay uninterpreted bytes.
        assert!(frames[0].control().unwrap().is_none());
    }

    #[test]
    fn a_deliberately_empty_capture_is_distinguishable_from_a_killed_one() {
        let writer = RawWriter::create(Vec::new(), header(), WriterOptions::default()).unwrap();
        let closed = writer.finish().unwrap().0;
        assert_eq!(closed.len(), FILE_HEADER_LEN + TRAILER_LEN);

        let mut reader = RawReader::open(closed.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert!(frames.is_empty());
        assert_eq!(truncation, None, "an empty capture is not a truncated one");
        assert!(reader.is_finalized());
        assert_eq!(reader.trailer().unwrap().frames, 0);
        assert_eq!(reader.trailer().unwrap().last_ingest_seq, None);

        // The same file *without* its trailer -- a recorder killed the instant
        // after it opened the file. Byte-for-byte this was indistinguishable
        // from the deliberately-empty case before the trailer existed.
        let killed = &closed[..FILE_HEADER_LEN];
        let mut reader = RawReader::open(killed).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert!(frames.is_empty());
        assert_eq!(
            truncation, None,
            "nothing was torn: there was nothing there"
        );
        assert!(
            !reader.is_finalized(),
            "a file the writer never closed must not read as complete"
        );
    }

    #[test]
    fn blocks_are_sealed_at_the_size_threshold() {
        let opts = WriterOptions {
            target_block_bytes: 4 * 1024,
            ..WriterOptions::default()
        };
        let mut writer = RawWriter::create(Vec::new(), header(), opts).unwrap();
        for i in 0_u64..500 {
            writer
                .write_venue_payload(
                    Ts::from_millis(i64::try_from(i).unwrap()),
                    i + 1,
                    &depth_payload(i),
                )
                .unwrap();
        }
        let stats = writer.stats();
        assert!(stats.blocks > 1, "expected several blocks, got {stats:?}");
        // Repetitive depth messages should compress substantially; if this ever
        // stops holding, the block size or the codec has regressed.
        assert!(
            stats.file_bytes * 2 < stats.frame_bytes,
            "expected better than 2x on depth messages, got {stats:?}"
        );
        let _ = writer.finish().unwrap();
    }

    /// The headline test for `docs/data-contract.md` §7.
    ///
    /// Truncating at *every* byte offset is the cheap way to cover the whole
    /// space of "where could a kill have landed" -- mid block header, mid
    /// compressed body, exactly on a boundary -- without staging a real process
    /// kill and hoping it lands somewhere interesting.
    #[test]
    fn truncation_at_every_byte_offset_loses_only_the_tail() {
        // Small blocks so a few hundred bytes of file span many boundaries.
        let opts = WriterOptions {
            target_block_bytes: 1024,
            ..WriterOptions::default()
        };
        let (full, expected) = capture(200, opts);
        assert!(full.len() > 3 * 1024, "want a multi-block file to cut up");

        for cut in FILE_HEADER_LEN..=full.len() {
            let partial = &full[..cut];
            let mut reader = RawReader::open(partial).unwrap();
            let (frames, truncation) = reader
                .read_all()
                .unwrap_or_else(|e| panic!("cut at {cut} errored: {e}"));

            // Whatever survived must be an exact prefix of what we wrote. Never
            // a reordered, duplicated or invented frame.
            assert!(
                frames.len() <= expected.len(),
                "cut at {cut} produced extra frames"
            );
            assert_eq!(
                frames.as_slice(),
                &expected[..frames.len()],
                "cut at {cut} did not yield a prefix"
            );

            // The invariant that matters, and the reason the trailer exists:
            // only the byte-for-byte complete file may claim to be complete.
            // "Claims complete" means nothing torn *and* the writer said it
            // closed -- neither half suffices alone. A cut landing exactly on a
            // block boundary tears nothing, and a cut inside the trailer loses
            // no frames; without the trailer both would look finished.
            let claims_complete = truncation.is_none() && reader.is_finalized();
            assert_eq!(
                claims_complete,
                cut == full.len(),
                "cut at {cut}: claims_complete={claims_complete}, truncation={truncation:?}"
            );

            if cut == full.len() {
                assert_eq!(frames.as_slice(), expected.as_slice());
            } else if let Some(t) = truncation {
                assert!(
                    !t.reason.is_corruption(),
                    "cut at {cut} reported {} as corruption",
                    t.reason
                );
                assert_eq!(
                    t.byte_offset + t.bytes_discarded,
                    as_u64(cut),
                    "cut at {cut} miscounted the discarded tail"
                );
            }
        }
    }

    #[test]
    fn a_file_too_short_to_identify_is_an_error_not_an_empty_capture() {
        let (full, _) = capture(10, WriterOptions::default());
        for cut in 0..FILE_HEADER_LEN {
            assert!(
                RawReader::open(&full[..cut]).is_err(),
                "a {cut}-byte file must not read as a valid empty capture"
            );
        }
    }

    #[test]
    fn corrupt_block_is_flagged_as_corruption_not_a_torn_tail() {
        let (mut bytes, expected) = capture(200, WriterOptions::default());
        // Flip a bit inside the first block's compressed body.
        let target = FILE_HEADER_LEN + BLOCK_HEADER_LEN + 8;
        bytes[target] ^= 0x80;

        let mut reader = RawReader::open(bytes.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert!(frames.is_empty(), "decoded frames from a damaged block");
        assert!(frames.len() < expected.len());

        let t = truncation.expect("damage was not reported");
        assert_eq!(t.reason, TruncationReason::BlockChecksum);
        assert!(
            t.reason.is_corruption(),
            "a checksum failure must not be filed as a routine torn tail"
        );
        assert_eq!(t.byte_offset, as_u64(FILE_HEADER_LEN));
        assert_eq!(
            t.byte_offset + t.bytes_discarded,
            as_u64(bytes.len()),
            "discarded count must cover everything after the damage"
        );
    }

    #[test]
    fn missing_sync_word_is_flagged_as_corruption() {
        let (mut bytes, _) = capture(50, WriterOptions::default());
        bytes[FILE_HEADER_LEN] ^= 0xff;

        let mut reader = RawReader::open(bytes.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert!(frames.is_empty());
        let t = truncation.expect("misalignment was not reported");
        assert_eq!(t.reason, TruncationReason::BadSync);
        assert!(t.reason.is_corruption());
    }

    #[test]
    fn implausible_block_length_is_refused_before_allocating() {
        let (mut bytes, _) = capture(50, WriterOptions::default());
        // Claim a 4 GiB compressed block.
        let at = FILE_HEADER_LEN + 8;
        bytes[at..at + 4].copy_from_slice(&u32::MAX.to_le_bytes());

        let mut reader = RawReader::open(bytes.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert!(frames.is_empty());
        assert_eq!(
            truncation.unwrap().reason,
            TruncationReason::ImplausibleBlockLength
        );
    }

    #[test]
    fn writer_refuses_ingest_seq_going_backwards() {
        let mut writer = RawWriter::create(Vec::new(), header(), WriterOptions::default()).unwrap();
        writer.write_venue_payload(Ts::EPOCH, 10, b"first").unwrap();
        assert!(matches!(
            writer.write_venue_payload(Ts::EPOCH, 10, b"repeat"),
            Err(StorageError::NonMonotonicIngestSeq {
                previous: 10,
                found: 10
            })
        ));
        assert!(matches!(
            writer.write_venue_payload(Ts::EPOCH, 9, b"backwards"),
            Err(StorageError::NonMonotonicIngestSeq { .. })
        ));
        assert_eq!(writer.last_ingest_seq(), Some(10));
    }

    #[test]
    fn holes_in_ingest_seq_are_legal_because_a_hole_is_evidence_of_a_drop() {
        let mut writer = RawWriter::create(Vec::new(), header(), WriterOptions::default()).unwrap();
        writer.write_venue_payload(Ts::EPOCH, 1, b"kept").unwrap();
        // 2 and 3 were assigned at ingress and dropped by a full channel.
        writer
            .write_control(
                Ts::EPOCH,
                4,
                &ControlRecord::Gap {
                    cause: GapCause::LocalOverflow,
                    exchange_ts: Ts::EPOCH,
                    last_good_ts: Ts::EPOCH,
                },
            )
            .unwrap();
        writer.write_venue_payload(Ts::EPOCH, 5, b"kept").unwrap();
        let bytes = writer.finish().unwrap().0;

        let mut reader = RawReader::open(bytes.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert_eq!(truncation, None);
        assert_eq!(
            frames.iter().map(|f| f.ingest_seq).collect::<Vec<_>>(),
            vec![1, 4, 5],
            "the hole at 2..=3 must survive the round trip as the record of the drop"
        );
    }

    #[test]
    fn a_dropped_writer_loses_unsealed_frames_but_leaves_a_readable_file() {
        // A forgotten finish() must degrade to the case we already handle -- an
        // unsealed tail and no trailer -- not to a file that lies. Borrowing the
        // sink lets us inspect the bytes after the writer is gone.
        let mut sink = Vec::new();
        {
            let mut writer =
                RawWriter::create(&mut sink, header(), WriterOptions::default()).unwrap();
            writer
                .write_venue_payload(Ts::EPOCH, 1, b"never sealed")
                .unwrap();
            assert!(writer.pending_bytes() > 0);
            assert_eq!(writer.stats().blocks, 0);
        }
        assert_eq!(sink.len(), FILE_HEADER_LEN, "unsealed frames reached disk");

        let mut reader = RawReader::open(sink.as_slice()).unwrap();
        let (frames, truncation) = reader.read_all().unwrap();
        assert!(frames.is_empty());
        assert_eq!(truncation, None);
        assert!(
            !reader.is_finalized(),
            "a dropped writer must not leave a file claiming to be complete"
        );
    }

    #[test]
    fn a_trailer_that_disagrees_with_the_data_is_a_hard_error() {
        let (mut bytes, _) = capture(50, WriterOptions::default());
        let trailer_at = bytes.len() - TRAILER_LEN;

        // Claim one more frame than the file contains, and re-checksum so we are
        // testing the cross-check rather than the trailer's own CRC.
        let declared =
            u64::from_le_bytes(bytes[trailer_at + 4..trailer_at + 12].try_into().unwrap()) + 1;
        bytes[trailer_at + 4..trailer_at + 12].copy_from_slice(&declared.to_le_bytes());
        let crc = crc32fast::hash(&bytes[trailer_at..trailer_at + 28]);
        bytes[trailer_at + 28..].copy_from_slice(&crc.to_le_bytes());

        let mut reader = RawReader::open(bytes.as_slice()).unwrap();
        match reader.read_all() {
            Err(StorageError::TrailerMismatch {
                field: "frames",
                declared: d,
                actual,
            }) => {
                assert_eq!(d, 52);
                assert_eq!(actual, 51);
            }
            other => panic!("expected a frames TrailerMismatch, got {other:?}"),
        }
    }

    #[test]
    fn a_damaged_trailer_fails_its_own_checksum() {
        let (mut bytes, _) = capture(10, WriterOptions::default());
        let trailer_at = bytes.len() - TRAILER_LEN;
        bytes[trailer_at + 12] ^= 0xff;

        let mut reader = RawReader::open(bytes.as_slice()).unwrap();
        assert!(matches!(
            reader.read_all(),
            Err(StorageError::TrailerChecksum { .. })
        ));
    }

    #[test]
    fn appending_a_second_capture_to_one_path_is_refused() {
        // docs/data-contract.md §5: one file per capture session, never appended
        // across a restart. Two writers sharing a path must not silently produce
        // a file that reads as one clean capture.
        let (mut first, _) = capture(10, WriterOptions::default());
        let (second, _) = capture(10, WriterOptions::default());
        first.extend_from_slice(&second);

        let mut reader = RawReader::open(first.as_slice()).unwrap();
        assert!(matches!(
            reader.read_all(),
            Err(StorageError::DataAfterTrailer)
        ));
    }
}
