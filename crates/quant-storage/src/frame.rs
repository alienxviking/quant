//! Byte-level encoding of the raw format: file header, block header, frame.
//!
//! This module is pure encode/decode over slices -- no I/O, no compression, no
//! state. Everything that can go wrong with the *layout* is testable here
//! without touching a file, which is most of what can go wrong.
//!
//! See the crate-level docs for the format diagram and the reasoning behind it.

use core::fmt;

use quant_core::event::{EventMeta, Gap, GapCause, MarketEvent};
use quant_core::instrument::{Exchange, InstrumentId};
use quant_core::time::Ts;
use quant_core::EVENT_SCHEMA_VERSION;

use crate::error::{StorageError, StorageResult};

/// First bytes of every raw file. Identifies the file without reference to its
/// path or extension, so a capture that gets moved is still self-describing.
pub const MAGIC: [u8; 8] = *b"QUANTRAW";

/// Version of *this framing format* -- distinct from
/// [`quant_core::EVENT_SCHEMA_VERSION`], which versions the event vocabulary.
///
/// They move independently: adding a field to `MarketEvent` does not change how
/// bytes are framed, and switching compression codec does not change what an
/// event means. Conflating them would force a re-record for either change.
pub const CONTAINER_VERSION: u16 = 1;

/// Marks the start of a block: ASCII `QRAB`.
///
/// Not needed to read an intact file -- blocks are self-delimiting by length.
/// It is here so that a future reader can resynchronize after mid-file damage
/// by scanning for the next boundary, without a format change. Cheap insurance
/// at 4 bytes per quarter-megabyte.
///
/// Built from the bytes rather than written as a hex literal so that `QRAB`
/// appears literally in a hex dump, which is worth something at 3am.
pub const BLOCK_SYNC: u32 = u32::from_le_bytes(*b"QRAB");

/// Marks the file trailer: ASCII `QEND`.
pub const TRAILER_SYNC: u32 = u32::from_le_bytes(*b"QEND");

/// Size of the file trailer.
pub const TRAILER_LEN: usize = 32;

/// Fixed width of the symbol field, NUL-padded.
///
/// Fixed rather than length-prefixed so the whole header is one `read_exact` of
/// a known size. 32 bytes covers every venue symbol convention we care about
/// (`BTCUSDT`, `BTC-USD`, `XBT/USD`, `BTCUSDT_240329`); anything longer is a
/// loud error and a `CONTAINER_VERSION` bump, not a silent truncation.
pub const SYMBOL_FIELD_LEN: usize = 32;

/// Total size of the file header.
pub const FILE_HEADER_LEN: usize = 68;

/// Size of a block header: sync, uncompressed length, compressed length, CRC.
pub const BLOCK_HEADER_LEN: usize = 16;

/// Size of a frame header: kind, `local_recv_ts`, `ingest_seq`, payload length.
pub const FRAME_HEADER_LEN: usize = 21;

/// Ceiling on a block's decompressed size.
///
/// A declared length is attacker-adjacent input in the sense that matters here:
/// a corrupt 4-byte field could otherwise ask us to allocate gigabytes. Bounded
/// before allocation, always.
pub const MAX_BLOCK_UNCOMPRESSED: u32 = 64 * 1024 * 1024;

/// Ceiling on one frame's payload.
///
/// Generous by design: venue stream messages are kilobytes, but a REST depth
/// snapshot of a deep book can run to a megabyte, and we would rather record an
/// unexpectedly large payload than drop it.
pub const MAX_PAYLOAD_LEN: u32 = 16 * 1024 * 1024;

// Field offsets within the file header. Spelled out so the layout lives in one
// place and cannot drift between encode and decode.
const OFF_MAGIC: usize = 0;
const OFF_CONTAINER_VERSION: usize = 8;
const OFF_EVENT_SCHEMA_VERSION: usize = 10;
const OFF_EXCHANGE: usize = 12;
const OFF_FLAGS: usize = 14;
const OFF_SESSION_ID: usize = 16;
const OFF_SYMBOL: usize = 32;
const OFF_HEADER_CRC: usize = 64;

/// What a frame carries.
///
/// The distinction exists because [`Self::Control`] frames are the one thing in
/// the raw tier we authored ourselves. Keeping them in the same stream, ordered
/// by the same `ingest_seq`, is what makes a gap record impossible to lose or
/// contradict -- see the crate docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameKind {
    /// Venue bytes, verbatim. Never parsed by this crate.
    VenuePayload,
    /// A [`ControlRecord`] we synthesized, as JSON.
    Control,
}

impl FrameKind {
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::VenuePayload => 0,
            Self::Control => 1,
        }
    }

    #[must_use]
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::VenuePayload),
            1 => Some(Self::Control),
            _ => None,
        }
    }
}

/// Identifies what a raw file contains, independently of its path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub container_version: u16,
    pub event_schema_version: u16,
    pub exchange: Exchange,
    /// The venue's own symbol, verbatim -- the same string we send back to the
    /// venue, not a normalized form.
    pub symbol: String,
    /// Opaque capture-session identifier, joined against the metadata database.
    ///
    /// Opaque here on purpose: this crate has no opinion on how sessions are
    /// identified, so it does not take a UUID dependency to hold 16 bytes.
    pub session_id: [u8; 16],
}

impl FileHeader {
    /// Header for a file this build is about to write.
    #[must_use]
    pub fn new(exchange: Exchange, symbol: impl Into<String>, session_id: [u8; 16]) -> Self {
        Self {
            container_version: CONTAINER_VERSION,
            event_schema_version: EVENT_SCHEMA_VERSION,
            exchange,
            symbol: symbol.into(),
            session_id,
        }
    }

    /// Serialize to the fixed 68-byte on-disk form.
    pub fn encode(&self) -> StorageResult<[u8; FILE_HEADER_LEN]> {
        let symbol = self.symbol.as_bytes();
        if symbol.is_empty() {
            return Err(StorageError::InvalidSymbol("empty"));
        }
        if symbol.len() > SYMBOL_FIELD_LEN {
            return Err(StorageError::SymbolTooLong {
                len: symbol.len(),
                max: SYMBOL_FIELD_LEN,
            });
        }
        // NUL is the padding sentinel, so it cannot also be data.
        if symbol.contains(&0) {
            return Err(StorageError::InvalidSymbol("contains a NUL byte"));
        }

        let mut out = [0_u8; FILE_HEADER_LEN];
        out[OFF_MAGIC..OFF_CONTAINER_VERSION].copy_from_slice(&MAGIC);
        out[OFF_CONTAINER_VERSION..OFF_EVENT_SCHEMA_VERSION]
            .copy_from_slice(&self.container_version.to_le_bytes());
        out[OFF_EVENT_SCHEMA_VERSION..OFF_EXCHANGE]
            .copy_from_slice(&self.event_schema_version.to_le_bytes());
        out[OFF_EXCHANGE..OFF_FLAGS].copy_from_slice(&self.exchange.wire_code().to_le_bytes());
        out[OFF_FLAGS..OFF_SESSION_ID].copy_from_slice(&0_u16.to_le_bytes());
        out[OFF_SESSION_ID..OFF_SYMBOL].copy_from_slice(&self.session_id);
        out[OFF_SYMBOL..OFF_SYMBOL + symbol.len()].copy_from_slice(symbol);

        let crc = crc32fast::hash(&out[..OFF_HEADER_CRC]);
        out[OFF_HEADER_CRC..FILE_HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
        Ok(out)
    }

    /// Parse the fixed 68-byte on-disk form.
    ///
    /// Checks are ordered by how useful the resulting message is: wrong magic
    /// ("you pointed me at the wrong file") before checksum ("this file is
    /// damaged") before version and field decoding, so the first error a person
    /// sees is the one most likely to be their actual mistake.
    pub fn decode(bytes: &[u8; FILE_HEADER_LEN]) -> StorageResult<Self> {
        let magic: [u8; 8] = bytes[OFF_MAGIC..OFF_CONTAINER_VERSION]
            .try_into()
            .expect("slice is 8 bytes");
        if magic != MAGIC {
            return Err(StorageError::NotARawFile { found: magic });
        }

        let expected = crc32fast::hash(&bytes[..OFF_HEADER_CRC]);
        let found = u32::from_le_bytes(
            bytes[OFF_HEADER_CRC..FILE_HEADER_LEN]
                .try_into()
                .expect("slice is 4 bytes"),
        );
        if expected != found {
            return Err(StorageError::HeaderChecksum { expected, found });
        }

        let container_version = read_u16(bytes, OFF_CONTAINER_VERSION);
        if container_version > CONTAINER_VERSION {
            return Err(StorageError::UnsupportedContainerVersion {
                found: container_version,
                supported: CONTAINER_VERSION,
            });
        }

        let exchange_code = read_u16(bytes, OFF_EXCHANGE);
        let exchange = Exchange::from_wire_code(exchange_code)
            .ok_or(StorageError::UnknownExchangeCode(exchange_code))?;

        let padded = &bytes[OFF_SYMBOL..OFF_HEADER_CRC];
        let end = padded.iter().position(|&b| b == 0).unwrap_or(padded.len());
        if end == 0 {
            return Err(StorageError::InvalidSymbol("empty"));
        }
        let symbol = core::str::from_utf8(&padded[..end])
            .map_err(|_| StorageError::InvalidSymbol("not valid UTF-8"))?
            .to_owned();

        Ok(Self {
            container_version,
            event_schema_version: read_u16(bytes, OFF_EVENT_SCHEMA_VERSION),
            exchange,
            symbol,
            session_id: bytes[OFF_SESSION_ID..OFF_SYMBOL]
                .try_into()
                .expect("slice is 16 bytes"),
        })
    }
}

impl fmt::Display for FileHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{} (container v{}, events v{})",
            self.exchange, self.symbol, self.container_version, self.event_schema_version
        )
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("slice is 2 bytes"),
    )
}

/// Written by a writer that closed cleanly. Its absence means the writer did
/// not.
///
/// # Why a trailer earns its 32 bytes
///
/// Without one, "I read to the end of the file and nothing was torn" is the
/// strongest completeness claim available -- and it is weaker than it sounds,
/// because a file cut exactly on a block boundary looks identical to a file
/// that ended there on purpose. In particular a capture killed immediately
/// after creation would be indistinguishable from one that deliberately
/// recorded nothing.
///
/// With one, the claim becomes "I read 4,318,221 frames and the writer states
/// it wrote 4,318,221" -- an independent cross-check that catches a whole class
/// of bug in which framing stays valid but data is quietly lost.
///
/// The check deliberately lives *in the file* rather than in Postgres. The data
/// contract makes raw the source of truth and Postgres merely metadata; if
/// validating a capture required the database, that ordering would be inverted
/// and a raw file would stop being self-describing.
///
/// An absent trailer is not an error. A file still being written does not have
/// one yet, and neither does one whose recorder was killed -- both are read as
/// far as they go, with the tail reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileTrailer {
    pub frames: u64,
    pub blocks: u64,
    /// Highest `ingest_seq` written, or `None` if the capture held no frames.
    pub last_ingest_seq: Option<u64>,
}

impl FileTrailer {
    #[must_use]
    pub fn encode(&self) -> [u8; TRAILER_LEN] {
        let mut out = [0_u8; TRAILER_LEN];
        out[0..4].copy_from_slice(&TRAILER_SYNC.to_le_bytes());
        out[4..12].copy_from_slice(&self.frames.to_le_bytes());
        out[12..20].copy_from_slice(&self.blocks.to_le_bytes());
        // `frames` already distinguishes the empty case, so 0 here is unambiguous
        // filler rather than a sentinel that could collide with a real sequence
        // number.
        out[20..28].copy_from_slice(&self.last_ingest_seq.unwrap_or(0).to_le_bytes());
        let crc = crc32fast::hash(&out[..28]);
        out[28..32].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decode the trailer given its bytes *after* the sync word has been matched.
    pub fn decode(bytes: &[u8; TRAILER_LEN]) -> StorageResult<Self> {
        let expected = crc32fast::hash(&bytes[..28]);
        let found = u32::from_le_bytes(bytes[28..32].try_into().expect("slice is 4 bytes"));
        if expected != found {
            return Err(StorageError::TrailerChecksum { expected, found });
        }
        let frames = u64::from_le_bytes(bytes[4..12].try_into().expect("slice is 8 bytes"));
        let last = u64::from_le_bytes(bytes[20..28].try_into().expect("slice is 8 bytes"));
        Ok(Self {
            frames,
            blocks: u64::from_le_bytes(bytes[12..20].try_into().expect("slice is 8 bytes")),
            last_ingest_seq: if frames == 0 { None } else { Some(last) },
        })
    }
}

/// A frame's fixed-size header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub kind: FrameKind,
    /// When our process first saw these bytes. The only timestamp anything
    /// downstream may act on -- see `quant_core::time`.
    pub local_recv_ts: Ts,
    pub ingest_seq: u64,
    pub payload_len: u32,
}

impl FrameHeader {
    #[must_use]
    pub fn encode(&self) -> [u8; FRAME_HEADER_LEN] {
        let mut out = [0_u8; FRAME_HEADER_LEN];
        out[0] = self.kind.code();
        out[1..9].copy_from_slice(&self.local_recv_ts.as_nanos().to_le_bytes());
        out[9..17].copy_from_slice(&self.ingest_seq.to_le_bytes());
        out[17..21].copy_from_slice(&self.payload_len.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8; FRAME_HEADER_LEN]) -> StorageResult<Self> {
        let kind =
            FrameKind::from_code(bytes[0]).ok_or(StorageError::UnknownFrameKind(bytes[0]))?;
        let nanos = i64::from_le_bytes(bytes[1..9].try_into().expect("slice is 8 bytes"));
        let ingest_seq = u64::from_le_bytes(bytes[9..17].try_into().expect("slice is 8 bytes"));
        let payload_len = u32::from_le_bytes(bytes[17..21].try_into().expect("slice is 4 bytes"));
        Ok(Self {
            kind,
            local_recv_ts: Ts::from_nanos(nanos),
            ingest_seq,
            payload_len,
        })
    }
}

/// One decoded frame, owned.
///
/// The payload of a `VenuePayload` frame is still uninterpreted bytes at this
/// point. Turning it into a [`MarketEvent`] is the normalizer's job (M2), and
/// keeping that separate is what lets us re-derive the normalized tier when we
/// find a parser bug.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub kind: FrameKind,
    pub local_recv_ts: Ts,
    pub ingest_seq: u64,
    pub payload: Vec<u8>,
}

impl RawFrame {
    #[must_use]
    pub fn is_control(&self) -> bool {
        matches!(self.kind, FrameKind::Control)
    }

    /// Decode a control frame's payload.
    ///
    /// Returns `Ok(None)` for a venue payload rather than erroring, so a
    /// consumer can filter without matching on the kind first.
    pub fn control(&self) -> StorageResult<Option<ControlRecord>> {
        if self.is_control() {
            ControlRecord::from_json(&self.payload).map(Some)
        } else {
            Ok(None)
        }
    }
}

/// An event the *recorder* authored, as opposed to bytes the venue sent.
///
/// # Why this is not just a serialized [`MarketEvent`]
///
/// `MarketEvent`'s `EventMeta` carries an [`InstrumentId`], and
/// `quant_core::instrument` is explicit that an `InstrumentId` must never be
/// persisted: it is a registry index, assigned in registration order, stable
/// only within one process. Writing one into a file that outlives the process
/// would produce a record that silently means a different instrument the next
/// time the registry is built in a different order.
///
/// So the instrument is not stored. It is implied by the file header's
/// `(exchange, symbol)` pair -- exactly the persistable identity the contract
/// says to use -- and reattached on read by
/// [`ControlRecord::into_market_event`]. The frame header already carries
/// `local_recv_ts` and `ingest_seq`, so those are not duplicated either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRecord {
    /// Data is known to be missing. See `quant_core::event::Gap`.
    Gap {
        cause: GapCause,
        /// For a synthesized gap there is no venue timestamp; the recorder sets
        /// this to its own observation time. Stored rather than reconstructed so
        /// the record round-trips exactly and we never invent a value on read.
        exchange_ts: Ts,
        /// Last local time we are confident the stream was intact.
        last_good_ts: Ts,
    },
}

impl ControlRecord {
    /// JSON, because a raw file should stay greppable.
    ///
    /// Control frames are rare -- a handful per day -- so the cost is nil, and
    /// `zstd -d < part-00000.bin.zst | strings` remaining useful at 3am is
    /// worth more than the bytes. Venue payloads are already JSON, so the whole
    /// decompressed file reads as text.
    pub fn to_json(&self) -> StorageResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(StorageError::Json)
    }

    pub fn from_json(bytes: &[u8]) -> StorageResult<Self> {
        serde_json::from_slice(bytes).map_err(StorageError::Json)
    }

    /// Reattach the identity that was deliberately not persisted.
    ///
    /// `instrument` comes from looking up the file header's
    /// `(exchange, symbol)` in the current process's registry; `local_recv_ts`
    /// and `ingest_seq` come from the frame header.
    #[must_use]
    pub fn into_market_event(
        self,
        instrument: InstrumentId,
        local_recv_ts: Ts,
        ingest_seq: u64,
    ) -> MarketEvent {
        match self {
            Self::Gap {
                cause,
                exchange_ts,
                last_good_ts,
            } => MarketEvent::Gap(Gap {
                meta: EventMeta {
                    instrument,
                    exchange_ts,
                    local_recv_ts,
                    ingest_seq,
                },
                cause,
                last_good_ts,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SESSION: [u8; 16] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];

    fn header() -> FileHeader {
        FileHeader::new(Exchange::Binance, "BTCUSDT", SESSION)
    }

    #[test]
    fn file_header_round_trips() {
        let h = header();
        let bytes = h.encode().unwrap();
        assert_eq!(bytes.len(), FILE_HEADER_LEN);
        assert_eq!(FileHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn file_header_layout_is_pinned() {
        // These offsets are in every recorded file. If this test fails, the fix
        // is to restore the layout, not to update the expectations.
        let bytes = header().encode().unwrap();
        assert_eq!(&bytes[0..8], b"QUANTRAW");
        assert_eq!(u16::from_le_bytes([bytes[8], bytes[9]]), CONTAINER_VERSION);
        assert_eq!(
            u16::from_le_bytes([bytes[10], bytes[11]]),
            EVENT_SCHEMA_VERSION
        );
        assert_eq!(u16::from_le_bytes([bytes[12], bytes[13]]), 1); // Binance
        assert_eq!(u16::from_le_bytes([bytes[14], bytes[15]]), 0); // flags
        assert_eq!(&bytes[16..32], &SESSION);
        assert_eq!(&bytes[32..39], b"BTCUSDT");
        assert_eq!(&bytes[39..64], &[0_u8; 25]); // NUL padding
    }

    #[test]
    fn foreign_file_is_rejected_by_magic_not_misread() {
        let mut bytes = header().encode().unwrap();
        bytes[..8].copy_from_slice(b"PAR1\0\0\0\0");
        match FileHeader::decode(&bytes) {
            Err(StorageError::NotARawFile { found }) => assert_eq!(&found, b"PAR1\0\0\0\0"),
            other => panic!("expected NotARawFile, got {other:?}"),
        }
    }

    #[test]
    fn damaged_header_fails_checksum() {
        let mut bytes = header().encode().unwrap();
        bytes[OFF_SYMBOL] ^= 0xff;
        assert!(matches!(
            FileHeader::decode(&bytes),
            Err(StorageError::HeaderChecksum { .. })
        ));
    }

    #[test]
    fn future_container_version_is_refused_not_guessed_at() {
        let mut h = header();
        h.container_version = CONTAINER_VERSION + 1;
        let bytes = h.encode().unwrap();
        assert!(matches!(
            FileHeader::decode(&bytes),
            Err(StorageError::UnsupportedContainerVersion { .. })
        ));
    }

    #[test]
    fn unknown_venue_is_refused_not_defaulted() {
        let mut bytes = header().encode().unwrap();
        bytes[OFF_EXCHANGE..OFF_FLAGS].copy_from_slice(&999_u16.to_le_bytes());
        // Re-checksum so we are testing the venue check, not the CRC.
        let crc = crc32fast::hash(&bytes[..OFF_HEADER_CRC]);
        bytes[OFF_HEADER_CRC..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            FileHeader::decode(&bytes),
            Err(StorageError::UnknownExchangeCode(999))
        ));
    }

    #[test]
    fn symbol_field_limits_are_hard_errors() {
        let long = "X".repeat(SYMBOL_FIELD_LEN + 1);
        assert!(matches!(
            FileHeader::new(Exchange::Binance, long, SESSION).encode(),
            Err(StorageError::SymbolTooLong { .. })
        ));
        assert!(matches!(
            FileHeader::new(Exchange::Binance, "", SESSION).encode(),
            Err(StorageError::InvalidSymbol(_))
        ));
        assert!(matches!(
            FileHeader::new(Exchange::Binance, "BTC\0USDT", SESSION).encode(),
            Err(StorageError::InvalidSymbol(_))
        ));

        // Exactly filling the field is fine and leaves no padding to trim.
        let exact = "X".repeat(SYMBOL_FIELD_LEN);
        let bytes = FileHeader::new(Exchange::Binance, exact.clone(), SESSION)
            .encode()
            .unwrap();
        assert_eq!(FileHeader::decode(&bytes).unwrap().symbol, exact);
    }

    #[test]
    fn frame_header_round_trips() {
        let h = FrameHeader {
            kind: FrameKind::Control,
            local_recv_ts: Ts::from_millis(1_699_999_999_042),
            ingest_seq: u64::MAX,
            payload_len: 1234,
        };
        let bytes = h.encode();
        assert_eq!(bytes.len(), FRAME_HEADER_LEN);
        assert_eq!(FrameHeader::decode(&bytes).unwrap(), h);
    }

    #[test]
    fn unknown_frame_kind_is_refused() {
        let mut bytes = FrameHeader {
            kind: FrameKind::VenuePayload,
            local_recv_ts: Ts::EPOCH,
            ingest_seq: 0,
            payload_len: 0,
        }
        .encode();
        bytes[0] = 7;
        assert!(matches!(
            FrameHeader::decode(&bytes),
            Err(StorageError::UnknownFrameKind(7))
        ));
    }

    #[test]
    fn control_record_does_not_persist_an_instrument_id() {
        let rec = ControlRecord::Gap {
            cause: GapCause::LocalOverflow,
            exchange_ts: Ts::from_millis(1_700_000_000_000),
            last_good_ts: Ts::from_millis(1_699_999_999_000),
        };
        let json = rec.to_json().unwrap();
        let text = core::str::from_utf8(&json).unwrap();

        // The whole point: the registry index never reaches the disk.
        assert!(!text.contains("instrument"), "{text}");
        assert!(text.contains("\"type\":\"gap\""), "{text}");
        assert!(text.contains("\"cause\":\"local_overflow\""), "{text}");

        assert_eq!(ControlRecord::from_json(&json).unwrap(), rec);
    }

    #[test]
    fn control_record_reattaches_identity_on_read() {
        use quant_core::instrument::{InstrumentDef, InstrumentKind, InstrumentRegistry};

        let mut reg = InstrumentRegistry::new();
        let id = reg.register(InstrumentDef {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_owned(),
            base: "BTC".to_owned(),
            quote: "USDT".to_owned(),
            kind: InstrumentKind::Spot,
            tick_size: "0.01".parse().unwrap(),
            lot_size: "0.00001".parse().unwrap(),
            min_notional: "5".parse().unwrap(),
        });

        let rec = ControlRecord::Gap {
            cause: GapCause::Disconnect,
            exchange_ts: Ts::from_millis(1_700_000_000_000),
            last_good_ts: Ts::from_millis(1_699_999_999_000),
        };
        let ev = rec.into_market_event(id, Ts::from_millis(1_700_000_000_000), 99);

        let MarketEvent::Gap(gap) = ev else {
            panic!("expected a gap");
        };
        assert_eq!(gap.meta.instrument, id);
        assert_eq!(gap.meta.ingest_seq, 99);
        assert_eq!(gap.cause, GapCause::Disconnect);
        assert_eq!(gap.last_good_ts, Ts::from_millis(1_699_999_999_000));
        // Dispatch time is local receive time, in every mode.
        assert_eq!(
            MarketEvent::Gap(gap).dispatch_ts(),
            Ts::from_millis(1_700_000_000_000)
        );
    }
}
