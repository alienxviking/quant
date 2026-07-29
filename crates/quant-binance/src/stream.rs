//! Which Binance streams we subscribe to, and the URL that does it.
//!
//! Pure string construction, so the part most likely to be wrong -- an endpoint
//! typo, the wrong depth interval, the wrong symbol case -- is checked by tests
//! rather than by watching a socket fail to deliver anything.

use core::fmt;

/// Binance spot market-data endpoint.
pub const SPOT_WS: &str = "wss://stream.binance.com:9443";

/// One subscribed stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamKind {
    /// Individual trade prints.
    ///
    /// Deliberately `@trade` and not `@aggTrade`. Aggregated trades merge fills
    /// that happened at the same price against the same resting order, which
    /// destroys exactly the microstructure detail order-flow work needs -- and it
    /// is not recoverable later, because the venue never sent it. The raw stream
    /// also carries the buyer-is-maker flag, which is what lets us derive the
    /// aggressor side rather than guessing at it.
    Trade,
    /// Incremental depth updates at the given cadence, in milliseconds.
    Depth { interval_ms: u32 },
}

impl fmt::Display for StreamKind {
    /// The suffix Binance expects after `<symbol>@`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Trade => f.write_str("trade"),
            // `@depth` alone means 1000 ms on spot; the interval is always
            // explicit here so the cadence is visible at the call site.
            Self::Depth { interval_ms } => write!(f, "depth@{interval_ms}ms"),
        }
    }
}

/// What to subscribe to for one instrument.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSpec {
    /// The venue's symbol in the venue's own case, e.g. `BTCUSDT`.
    ///
    /// Stored as the venue spells it because that is what goes in the capture
    /// file header and what we would send back on an order. Stream names need it
    /// lowercased, and that conversion happens at the last possible moment.
    pub symbol: String,
    pub streams: Vec<StreamKind>,
}

impl StreamSpec {
    /// Trades plus 100 ms depth: what M1 records.
    ///
    /// 100 ms is the fastest incremental depth Binance offers on spot. The
    /// alternative, 1000 ms, would coalesce ten times as much book movement into
    /// each message -- and unlike a parsing mistake, detail the venue never sent
    /// cannot be recovered from raw capture later. The extra bandwidth is the
    /// cheapest thing in this system.
    #[must_use]
    pub fn market_data(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            streams: vec![StreamKind::Trade, StreamKind::Depth { interval_ms: 100 }],
        }
    }

    /// Stream names as Binance names them.
    #[must_use]
    pub fn stream_names(&self) -> Vec<String> {
        let lower = self.symbol.to_lowercase();
        self.streams
            .iter()
            .map(|kind| format!("{lower}@{kind}"))
            .collect()
    }

    /// Combined-stream URL.
    ///
    /// The `/stream?streams=` form is used even for a single stream, for two
    /// reasons. It keeps one payload shape -- every message arrives wrapped as
    /// `{"stream":..,"data":..}` -- so the normalizer never has to guess which
    /// endpoint a recorded file came from. And that wrapper names the stream the
    /// message came from, which is provenance we get for free and would otherwise
    /// have to infer from the payload's own fields.
    #[must_use]
    pub fn url(&self, base: &str) -> String {
        format!(
            "{}/stream?streams={}",
            base.trim_end_matches('/'),
            self.stream_names().join("/")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_market_data_url_is_exactly_what_binance_expects() {
        let spec = StreamSpec::market_data("BTCUSDT");
        assert_eq!(
            spec.url(SPOT_WS),
            "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@depth@100ms"
        );
    }

    #[test]
    fn stream_names_are_lowercased_but_the_symbol_is_not() {
        // Binance rejects uppercase stream names, and the capture file header must
        // carry the symbol as the venue spells it. Both, from one field.
        let spec = StreamSpec::market_data("BTCUSDT");
        assert_eq!(
            spec.stream_names(),
            vec!["btcusdt@trade", "btcusdt@depth@100ms"]
        );
        assert_eq!(spec.symbol, "BTCUSDT", "the venue symbol keeps its case");
    }

    #[test]
    fn depth_cadence_is_always_explicit() {
        // A bare `@depth` would silently mean 1000 ms and quietly cost us a
        // tenfold reduction in book detail.
        assert_eq!(
            StreamKind::Depth { interval_ms: 100 }.to_string(),
            "depth@100ms"
        );
        assert_eq!(
            StreamKind::Depth { interval_ms: 1_000 }.to_string(),
            "depth@1000ms"
        );
        assert_eq!(StreamKind::Trade.to_string(), "trade");
    }

    #[test]
    fn a_single_stream_still_uses_the_combined_endpoint() {
        // So that every recorded file has the same wrapped payload shape,
        // whatever it was subscribed to.
        let spec = StreamSpec {
            symbol: "ETHUSDT".to_owned(),
            streams: vec![StreamKind::Trade],
        };
        assert_eq!(
            spec.url(SPOT_WS),
            "wss://stream.binance.com:9443/stream?streams=ethusdt@trade"
        );
    }

    #[test]
    fn a_trailing_slash_on_the_base_does_not_produce_a_double_slash() {
        let spec = StreamSpec::market_data("BTCUSDT");
        assert_eq!(
            spec.url("wss://example.test/"),
            spec.url("wss://example.test")
        );
    }
}
