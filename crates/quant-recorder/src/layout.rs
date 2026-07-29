//! Where a capture file goes on disk.
//!
//! Implements the layout fixed in `docs/data-contract.md` §5. It lives in code
//! rather than being assembled ad hoc at each call site because the partition
//! scheme is a published interface: `DuckDB`, Polars, pandas, Spark and
//! `ClickHouse` all discover `key=value` directories by convention, and one
//! typo'd `symbol=btcusdt` would create a second partition that every one of
//! those tools reads as a different instrument.

use core::fmt::Write as _;
use std::path::{Path, PathBuf};

use quant_core::instrument::Exchange;
use quant_core::time::UtcDate;

/// Identifies one capture file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureTarget {
    pub exchange: Exchange,
    /// The venue's own symbol, verbatim and in the venue's own case.
    pub symbol: String,
    pub date: UtcDate,
    pub session_id: [u8; 16],
    /// Sequence within the session. Reserved for the size-based file rolling
    /// that arrives with M1.c; today a session writes `part-00000` only.
    pub part: u32,
}

impl CaptureTarget {
    /// Directory holding this session's parts.
    #[must_use]
    pub fn directory(&self, root: &Path) -> PathBuf {
        root.join("raw")
            .join(format!("exchange={}", self.exchange))
            .join(format!("symbol={}", self.symbol))
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
            symbol: "BTCUSDT".to_owned(),
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
