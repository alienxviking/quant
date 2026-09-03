//! Record one Binance symbol to a raw capture file.
//!
//! ```text
//! record [SYMBOL] [DATA_ROOT] [SECONDS]
//! record BTCUSDT data        # until ctrl-c
//! record BTCUSDT data 30     # stop after 30 seconds
//! ```
//!
//! Argument parsing is deliberately primitive, and the database comes from
//! `QUANT_DATABASE_URL` rather than a flag. Real configuration -- symbol lists,
//! per-instrument policy -- is worth designing once there are several instruments
//! to configure, which is not yet.
//!
//! # The metadata tier is optional
//!
//! No `QUANT_DATABASE_URL`, or a database that will not answer, logs a warning and
//! records anyway. Market data cannot be regenerated and an index row can, so the
//! index is allowed to be missing and the capture is not allowed to stop. See
//! `quant-meta`'s crate docs.
//!
//! # Shutdown
//!
//! Ctrl-C -- or the optional duration limit -- races the connection loop. When
//! either wins, the connection future is dropped, which releases the borrow on
//! `Ingress`, which lets it be dropped, which closes the channel, which makes the
//! writer thread seal the file with a trailer. So a deliberate shutdown produces a
//! file that says it is complete, and a `SIGKILL` produces one that does not --
//! distinguishable later from the bytes alone, with no external bookkeeping.
//!
//! The duration limit exists because that distinction has to be *testable*.
//! Delivering a real console interrupt to a child process is awkward on Windows,
//! and a clean-shutdown path that is never exercised end to end is a clean-shutdown
//! path that does not work. It is also useful in its own right for bounded capture.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use quant_binance::capture::{self, CaptureConfig};
use tracing::error;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Before anything opens a socket. Left to rustls' own feature-based detection
    // this panics at the first TLS handshake -- in production, during a reconnect,
    // looking exactly like a venue outage.
    quant_binance::install_crypto_provider();

    let config = match parse_args() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    // The sink passes straight through: this binary has one consumer. A paper run
    // wraps it in a TeeSink instead, and that closure is the only difference
    // between the two.
    match capture::run(config, |tx| tx).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "recorder stopped");
            ExitCode::FAILURE
        }
    }
}

fn parse_args() -> Result<CaptureConfig, String> {
    let mut args = std::env::args().skip(1);
    let symbol = args.next().unwrap_or_else(|| "BTCUSDT".to_owned());
    let root = PathBuf::from(args.next().unwrap_or_else(|| "data".to_owned()));
    let mut config = CaptureConfig::new(symbol, root);
    if let Some(seconds) = args.next() {
        let seconds: u64 = seconds
            .parse()
            .map_err(|e| format!("SECONDS must be a whole number of seconds: {e}"))?;
        config = config.for_at_most(Duration::from_secs(seconds));
    }
    Ok(config)
}
