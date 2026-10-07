//! Prove the credentials, the signature and the clock — without placing an order.
//!
//! The preflight `docs/live-run.md` slice (b) names, and the only part of M8
//! that can be run end to end before any money is at risk. It makes exactly one
//! signed request, `GET /api/v3/account`, which is the cheapest there is: it
//! exercises the key, the secret, the signature construction, the header and the
//! clock in one round trip and places nothing.
//!
//! ```text
//! export BINANCE_API_KEY=...  BINANCE_API_SECRET=...
//! cargo run -p quant-binance --bin venue-check
//! ```
//!
//! Exit 0 ready, 1 not. Nothing it prints contains any part of the secret: see
//! `credentials.rs` for why that is a property of the type rather than of this
//! file's care.

use std::process::ExitCode;
use std::time::Duration;

use quant_binance::credentials::Credentials;
use quant_binance::rest::SnapshotClient;
use quant_binance::trade::{TradeClient, MAX_CLOCK_SKEW_MS};

const BASE: &str = "https://api.binance.com";

#[tokio::main]
async fn main() -> ExitCode {
    let credentials = match Credentials::from_env() {
        Ok(c) => c,
        Err(why) => {
            eprintln!("credentials  {why}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "credentials  {} is set, and so is the secret",
        Credentials::KEY_VAR
    );

    // The same round-trip-corrected measurement the recorder's startup check and
    // `ops/preflight.sh` use. Measuring `now - serverTime` naively folds one-way
    // latency into the offset, which on a slow connection blocks a run over a
    // clock that is fine -- seen live at M1: -1445 ms naive against +450 ms
    // corrected.
    let public = match SnapshotClient::new(BASE, Duration::from_secs(10)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("clock        could not build a client: {e}");
            return ExitCode::FAILURE;
        }
    };
    let before = millis();
    let offset = match public.clock_offset_millis(before).await {
        Ok(offset) => {
            let after = millis();
            // Half the round trip is the one-way leg the naive figure wrongly
            // attributes to the clock.
            let corrected = offset - (after - before) / 2;
            println!(
                "clock        {corrected} ms from the venue (raw {offset} ms, round trip {} ms), \
                 limit {MAX_CLOCK_SKEW_MS} ms",
                after - before
            );
            corrected
        }
        Err(e) => {
            eprintln!("clock        could not reach the venue: {e}");
            return ExitCode::FAILURE;
        }
    };

    let client = match TradeClient::new(BASE, Duration::from_secs(10), credentials) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("venue        could not build a client: {e}");
            return ExitCode::FAILURE;
        }
    };
    match client.check_ready(offset).await {
        Ok(()) => {
            println!("venue        a signed request was accepted");
            println!();
            println!("verdict      READY: the key, the secret and the clock all work.");
            println!("             Nothing was ordered, and this proves nothing about balances.");
            ExitCode::SUCCESS
        }
        Err(why) => {
            println!();
            eprintln!("verdict      NOT READY: {why}");
            ExitCode::FAILURE
        }
    }
}

fn millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
