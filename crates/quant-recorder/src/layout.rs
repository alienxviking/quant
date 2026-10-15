//! Where a capture file goes on disk.
//!
//! Implements the layout fixed in `docs/data-contract.md` §5. It lives in code
//! rather than being assembled ad hoc at each call site because the partition
//! scheme is a published interface: `DuckDB`, Polars, pandas, Spark and
//! `ClickHouse` all discover `key=value` directories by convention, and one
//! typo'd `symbol=btcusdt` would create a second partition that every one of
//! those tools reads as a different instrument.
//!
//! # Two trees, one per kind of stream
//!
//! ```text
//! <root>/raw/exchange=binance/symbol=BTCUSDT/date=2026-10-15/session=<uuid>/part-00000.bin.zst
//! <root>/account/exchange=binance/date=2026-10-15/session=<uuid>/part-00000.bin.zst
//! ```
//!
//! Market data is filed by instrument, because every question asked of it is
//! "this instrument, these days". The account stream -- the venue's reports on
//! the account's own orders, which M8 records before it parses them -- has no
//! instrument to file under: one socket carries every instrument the account
//! trades. So its path has no `symbol=`, and it lives in a tree of its own beside
//! `raw/` rather than inside it, which is `docs/live-run.md` §5's decision: every
//! reader of market data walks `raw/`, so none of them can ever be handed one of
//! its files, and a gap on the account's socket can never clear a book.
//!
//! The path needs no account key, because an M8 run root holds one account: the
//! session tells two runs apart. A second account under one root is when an
//! `account=` key would earn itself.

use core::fmt::Write as _;
use std::path::{Path, PathBuf};

use quant_core::instrument::Exchange;
use quant_core::time::UtcDate;

/// The tree market data is captured under.
pub const RAW_DIR: &str = "raw";

/// The account stream's name, wherever it needs one: the tree its captures
/// live under, beside [`RAW_DIR`], and the file header's symbol field.
///
/// That field is where a market capture names its instrument. It is
/// fixed-width and may not be empty, and the account stream has no instrument,
/// so its files name the stream there instead -- in lower case, which is not
/// how Binance writes a symbol, so a reader of the header alone cannot take it
/// for one.
pub const ACCOUNT_STREAM: &str = "account";

/// What a capture file records, which decides the tree it lives in.
///
/// A sum rather than a symbol and a flag, so that an account capture with a
/// symbol cannot be written down at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureStream {
    /// One instrument's market data -- trades, depth, book snapshots -- under
    /// [`RAW_DIR`].
    Market {
        /// The venue's own symbol, verbatim and in the venue's own case.
        symbol: String,
    },
    /// The account's own stream, under [`ACCOUNT_STREAM`]: what the venue says
    /// happened to the account's orders, on every instrument it trades.
    Account,
}

impl CaptureStream {
    #[must_use]
    pub fn market(symbol: impl Into<String>) -> Self {
        Self::Market {
            symbol: symbol.into(),
        }
    }

    /// The instrument, for market data. The account stream has none.
    #[must_use]
    pub fn symbol(&self) -> Option<&str> {
        match self {
            Self::Market { symbol } => Some(symbol),
            Self::Account => None,
        }
    }

    /// What this stream's files carry in their header's symbol field.
    ///
    /// One definition, because the session writes it and the verifier holds a
    /// file's header to its path by it, and two copies could disagree.
    #[must_use]
    pub fn header_symbol(&self) -> &str {
        match self {
            Self::Market { symbol } => symbol,
            Self::Account => ACCOUNT_STREAM,
        }
    }
}

/// Identifies one capture file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureTarget {
    pub exchange: Exchange,
    /// One instrument's market data, or the account stream.
    pub stream: CaptureStream,
    pub date: UtcDate,
    pub session_id: [u8; 16],
    /// Sequence within the session's day. Reserved for size-based rolling
    /// within a day, which has never been needed; today a session writes
    /// `part-00000` only.
    pub part: u32,
}

impl CaptureTarget {
    /// Directory holding this session's parts for its day.
    #[must_use]
    pub fn directory(&self, root: &Path) -> PathBuf {
        let exchange = format!("exchange={}", self.exchange);
        let stream = match &self.stream {
            CaptureStream::Market { symbol } => root
                .join(RAW_DIR)
                .join(exchange)
                .join(format!("symbol={symbol}")),
            CaptureStream::Account => root.join(ACCOUNT_STREAM).join(exchange),
        };
        stream
            .join(format!("date={}", self.date))
            .join(format!("session={}", format_session_id(&self.session_id)))
    }

    /// Full path of the capture file.
    ///
    /// `.bin.zst` because the contents are framed binary containing
    /// zstd-compressed blocks. Not a bare `.zst`: the file is not a zstd stream
    /// and `zstd -d` will not decompress it, so claiming otherwise would send
    /// whoever finds it down the wrong path.
    #[must_use]
    pub fn file(&self, root: &Path) -> PathBuf {
        self.directory(root)
            .join(format!("part-{:05}.bin.zst", self.part))
    }

    /// Read a capture path back into the identity that produced it.
    ///
    /// Lives here, beside [`CaptureTarget::file`], so the two cannot drift: a
    /// parser written wherever it happens to be needed is a second, unversioned
    /// copy of the partition scheme, and the first thing it does when the scheme
    /// changes is silently mis-attribute data.
    ///
    /// Returns `None` for anything that is not a capture file at the expected
    /// depth. The verifier treats that as a finding rather than skipping it
    /// quietly -- a stray file in a capture tree means either a bug or somebody
    /// tidying by hand, and both are worth hearing about.
    ///
    /// Exactly the last five components are read, so the data root may be
    /// anything and may itself contain `=`. For market data they are the four
    /// partition keys and the file. The account stream has one key fewer, so its
    /// fifth is the tree itself, and it must say `account`: that is the path
    /// stating what it holds, and without the check a market path that had lost
    /// its `symbol=` directory would read as the account's.
    #[must_use]
    pub fn parse(path: &Path) -> Option<Self> {
        let mut parts = path.components().rev().map(|c| c.as_os_str().to_str());
        let file = parts.next()??;
        let session = parts.next()??;
        let date = parts.next()??;
        let fourth = parts.next()??;
        let fifth = parts.next()??;

        // Market data names its symbol fourth from the end; the account stream
        // names its exchange there, with its own tree directly above. No
        // component starts with both `symbol=` and `exchange=`, so neither shape
        // can be taken for the other.
        let (exchange, stream) = match fourth.strip_prefix("symbol=") {
            Some(symbol) => (
                fifth.strip_prefix("exchange=")?,
                CaptureStream::market(symbol),
            ),
            None if fifth == ACCOUNT_STREAM => {
                (fourth.strip_prefix("exchange=")?, CaptureStream::Account)
            }
            None => return None,
        };

        let part = file
            .strip_prefix("part-")?
            .strip_suffix(".bin.zst")?
            .parse()
            .ok()?;

        Some(Self {
            exchange: Exchange::from_name(exchange)?,
            stream,
            date: parse_utc_date(date.strip_prefix("date=")?)?,
            session_id: parse_session_id(session.strip_prefix("session=")?)?,
            part,
        })
    }
}

/// `YYYY-MM-DD` back into a [`UtcDate`].
///
/// Deliberately strict about width: a two-digit year or a one-digit month would
/// sort wrongly in a directory listing, which is what the verifier relies on to
/// join a session's segments in order.
#[must_use]
pub fn parse_utc_date(text: &str) -> Option<UtcDate> {
    let bytes = text.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    Some(UtcDate {
        year: text.get(0..4)?.parse().ok()?,
        month: text.get(5..7)?.parse().ok()?,
        day: text.get(8..10)?.parse().ok()?,
    })
}

/// Inverse of [`format_session_id`].
///
/// Rejects uppercase hex, because [`format_session_id`] never emits it and
/// accepting both spellings would let one session appear as two on a
/// case-sensitive filesystem -- the very thing the lowercase rule prevents.
#[must_use]
pub fn parse_session_id(text: &str) -> Option<[u8; 16]> {
    if text.len() != 36 {
        return None;
    }
    let mut out = [0_u8; 16];
    let mut chars = text.chars();
    for (i, byte) in out.iter_mut().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) && chars.next() != Some('-') {
            return None;
        }
        let hi = hex_nibble(chars.next()?)?;
        let lo = hex_nibble(chars.next()?)?;
        *byte = (hi << 4) | lo;
    }
    chars.next().map_or(Some(out), |_| None)
}

const fn hex_nibble(c: char) -> Option<u8> {
    match c {
        '0'..='9' => Some(c as u8 - b'0'),
        'a'..='f' => Some(c as u8 - b'a' + 10),
        _ => None,
    }
}

/// Canonical UUID text for a session id.
///
/// Formatted here rather than via the `uuid` crate so this crate stays free of
/// dependencies; the 8-4-4-4-12 layout is frozen by RFC 4122 and is not going to
/// change under us. Lowercase, because mixed case would produce two directory
/// names for one session on a case-sensitive filesystem.
#[must_use]
pub fn format_session_id(id: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (i, byte) in id.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> CaptureTarget {
        CaptureTarget {
            exchange: Exchange::Binance,
            stream: CaptureStream::market("BTCUSDT"),
            date: UtcDate {
                year: 2026,
                month: 7,
                day: 29,
            },
            session_id: [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
                0x32, 0x10,
            ],
            part: 0,
        }
    }

    #[test]
    fn the_path_matches_the_published_layout() {
        // Pinned against docs/data-contract.md §5. If this changes, every tool
        // pointed at the data directory has to be told, so it changes with a
        // documentation update rather than quietly.
        let path = target().file(Path::new("data"));
        let text = path.to_string_lossy().replace('\\', "/");
        assert_eq!(
            text,
            "data/raw/exchange=binance/symbol=BTCUSDT/date=2026-07-29/\
             session=01234567-89ab-cdef-fedc-ba9876543210/part-00000.bin.zst"
        );
    }

    #[test]
    fn session_ids_format_as_canonical_lowercase_uuids() {
        assert_eq!(
            format_session_id(&[0; 16]),
            "00000000-0000-0000-0000-000000000000"
        );
        let formatted = format_session_id(&target().session_id);
        assert_eq!(formatted, "01234567-89ab-cdef-fedc-ba9876543210");
        assert_eq!(formatted.len(), 36);
        assert!(
            !formatted.chars().any(char::is_uppercase),
            "mixed case would create two directories for one session"
        );
    }

    #[test]
    fn parts_are_zero_padded_so_they_sort() {
        let mut t = target();
        t.part = 7;
        let seven = t.file(Path::new("d")).to_string_lossy().to_string();
        t.part = 42;
        let forty_two = t.file(Path::new("d")).to_string_lossy().to_string();
        assert!(seven.ends_with("part-00007.bin.zst"));
        assert!(forty_two.ends_with("part-00042.bin.zst"));
        // Unpadded names would order part-10 before part-7.
        assert!(seven < forty_two);
    }

    #[test]
    fn the_venue_symbol_keeps_its_own_case() {
        // Binance's stream names are lowercase but its symbol is BTCUSDT, and the
        // partition must match the symbol we send back to the venue -- not the
        // subscription spelling.
        let path = target().directory(Path::new("d"));
        assert!(path.to_string_lossy().contains("symbol=BTCUSDT"));
    }

    #[test]
    fn a_written_path_parses_back_to_the_target_that_wrote_it() {
        // The property that matters: `file` and `parse` are inverses. If they ever
        // stop being, the verifier attributes data to the wrong instrument.
        let original = target();
        for root in ["data", "d", "/var/lib/quant", "C:/caps", "weird=root/x"] {
            let path = original.file(Path::new(root));
            assert_eq!(
                CaptureTarget::parse(&path).as_ref(),
                Some(&original),
                "round trip failed under root {root}"
            );
        }

        let mut later = target();
        later.part = 42;
        later.date = UtcDate {
            year: 1999,
            month: 12,
            day: 31,
        };
        assert_eq!(
            CaptureTarget::parse(&later.file(Path::new("d"))),
            Some(later)
        );
    }

    #[test]
    fn anything_that_is_not_a_capture_file_is_refused_rather_than_guessed_at() {
        for bad in [
            "data/raw/exchange=binance/symbol=BTCUSDT/date=2026-07-29/session=01234567-89ab-cdef-fedc-ba9876543210/part-00000.parquet",
            // A venue this build does not know: refused, not defaulted.
            "data/raw/exchange=bitmex/symbol=BTCUSDT/date=2026-07-29/session=01234567-89ab-cdef-fedc-ba9876543210/part-00000.bin.zst",
            // Uppercase session hex would be a second directory for one session.
            "data/raw/exchange=binance/symbol=BTCUSDT/date=2026-07-29/session=01234567-89AB-cdef-fedc-ba9876543210/part-00000.bin.zst",
            "data/raw/exchange=binance/symbol=BTCUSDT/date=2026-7-29/session=01234567-89ab-cdef-fedc-ba9876543210/part-00000.bin.zst",
            "data/raw/exchange=binance/symbol=BTCUSDT/date=2026-07-29/part-00000.bin.zst",
            "part-00000.bin.zst",
            "data/raw/README.md",
        ] {
            assert_eq!(
                CaptureTarget::parse(Path::new(bad)),
                None,
                "should have been refused: {bad}"
            );
        }
    }

    fn account_target() -> CaptureTarget {
        CaptureTarget {
            stream: CaptureStream::Account,
            ..target()
        }
    }

    #[test]
    fn the_account_stream_has_a_tree_of_its_own_and_no_symbol() {
        // Pinned against docs/data-contract.md §5, as the market path is above.
        // Beside raw/ rather than inside it, so that nothing which walks raw/ for
        // market data is ever handed one of these files.
        let text = account_target()
            .file(Path::new("data"))
            .to_string_lossy()
            .replace('\\', "/");
        assert_eq!(
            text,
            "data/account/exchange=binance/date=2026-07-29/\
             session=01234567-89ab-cdef-fedc-ba9876543210/part-00000.bin.zst"
        );
    }

    #[test]
    fn an_account_path_parses_back_to_the_target_that_wrote_it() {
        // `file` and `parse` stay inverses for both streams, including under
        // roots that are themselves called `account` or `raw` -- where reading
        // the tree's name off the wrong component would swap one stream for the
        // other.
        for original in [account_target(), target()] {
            for root in [
                "data",
                "/var/lib/quant",
                "C:/caps",
                "weird=root/x",
                "account",
                "raw",
                "x/account/raw",
            ] {
                let path = original.file(Path::new(root));
                assert_eq!(
                    CaptureTarget::parse(&path).as_ref(),
                    Some(&original),
                    "round trip failed for {:?} under root {root}",
                    original.stream
                );
            }
        }
    }

    #[test]
    fn neither_stream_can_be_read_as_the_other() {
        // The account tree's own name is what marks its files, so a market path
        // that lost its `symbol=` directory is refused rather than read as the
        // account's, and so is a misnamed tree.
        for bad in [
            "data/raw/exchange=binance/date=2026-07-29/session=01234567-89ab-cdef-fedc-ba9876543210/part-00000.bin.zst",
            "data/accounts/exchange=binance/date=2026-07-29/session=01234567-89ab-cdef-fedc-ba9876543210/part-00000.bin.zst",
            "data/account/date=2026-07-29/session=01234567-89ab-cdef-fedc-ba9876543210/part-00000.bin.zst",
        ] {
            assert_eq!(
                CaptureTarget::parse(Path::new(bad)),
                None,
                "should have been refused: {bad}"
            );
        }
        // A market-shaped path is market data wherever it sits. The catalog
        // that walks the account tree is what refuses one found there.
        let market_shaped = "data/account/exchange=binance/symbol=BTCUSDT/date=2026-07-29/\
                             session=01234567-89ab-cdef-fedc-ba9876543210/part-00000.bin.zst";
        assert_eq!(
            CaptureTarget::parse(Path::new(market_shaped)).map(|t| t.stream),
            Some(CaptureStream::market("BTCUSDT"))
        );
    }

    #[test]
    fn a_header_names_the_instrument_or_the_account_stream() {
        assert_eq!(CaptureStream::market("BTCUSDT").header_symbol(), "BTCUSDT");
        assert_eq!(CaptureStream::Account.header_symbol(), "account");
        assert_eq!(CaptureStream::Account.symbol(), None);
        assert_eq!(CaptureStream::market("BTCUSDT").symbol(), Some("BTCUSDT"));
    }

    #[test]
    fn session_ids_round_trip_through_text() {
        for id in [[0_u8; 16], [0xff; 16], target().session_id] {
            assert_eq!(parse_session_id(&format_session_id(&id)), Some(id));
        }
        assert_eq!(parse_session_id("too-short"), None);
        assert_eq!(
            parse_session_id("0123456789abcdeffedcba9876543210"),
            None,
            "the hyphens are part of the canonical form"
        );
    }

    #[test]
    fn dates_sort_chronologically_as_text() {
        // The verifier joins a session's segments by sorting their paths, which is
        // only correct because ISO dates and zero-padded parts sort lexically.
        let mut a = target();
        let mut b = target();
        a.date = UtcDate {
            year: 2026,
            month: 9,
            day: 30,
        };
        b.date = UtcDate {
            year: 2026,
            month: 10,
            day: 1,
        };
        assert!(
            a.file(Path::new("d")).to_string_lossy() < b.file(Path::new("d")).to_string_lossy(),
            "September must sort before October"
        );
    }

    #[test]
    fn different_sessions_never_share_a_directory() {
        // §5: one file per capture session, so a crashed process can never
        // corrupt a file another process is reading.
        let mut a = target();
        let mut b = target();
        a.session_id[15] = 0x00;
        b.session_id[15] = 0x01;
        assert_ne!(a.directory(Path::new("d")), b.directory(Path::new("d")));
    }
}
