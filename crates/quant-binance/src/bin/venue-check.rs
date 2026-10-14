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
use quant_binance::trade::{ClockSample, TradeClient, MAX_CLOCK_SKEW_MS, MIN_CLOCK_READINGS};

const BASE: &str = "https://api.binance.com";

/// How many readings of the venue's clock to try before believing one.
///
/// Five, as `ops/preflight.sh` takes. The first reading also opens the
/// connection, so DNS, TCP and TLS all land on its outbound leg -- exactly the
/// asymmetry a midpoint cannot correct -- and its round trip is the longer for
/// it, which is what lets [`ClockSample::tightest`] pass it over. The client
/// pools the connection, so the readings after it normally go out warm.
///
/// All five are tried whatever happens to any one of them, each bounded by the
/// client's timeout, and a verdict needs [`MIN_CLOCK_READINGS`] of them back.
/// Stopping at the first failure could leave the verdict resting on the one
/// reading that came back before it -- the cold one.
const CLOCK_READINGS: usize = 5;

// A run that could never collect enough readings could never be judged, and
// would say so only at run time, every time.
const _: () = assert!(MIN_CLOCK_READINGS <= CLOCK_READINGS);

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

    // The venue's clock, read the way NTP reads one: the host's clock either
    // side of `GET /api/v3/time`, the venue's reading taken to describe the
    // midpoint, and of several readings the one with the smallest round trip
    // believed. `ClockSample` holds the arithmetic and the reasons for it.
    //
    // This is not the recorder's startup check, and the two figures differ by
    // design. That one -- like `ops/preflight.ps1` and `ops/fix-clock.ps1` --
    // reads `before - serverTime` once and leaves it uncorrected
    // (`SnapshotClient::clock_offset_millis` says why), so it comes out short by
    // the outbound leg: on a slow link, enough to make a clock that is fine look
    // like one behind the venue. Tolerable in a warning about a metric; not in
    // the figure that decides whether this host may trade. `ops/preflight.sh`
    // and `ops/fix-clock.sh` compute the same midpoint as this, with the
    // opposite sign: positive there means the host is behind.
    let public = match SnapshotClient::new(BASE, Duration::from_secs(10)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("clock        could not build a client: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut readings = Vec::with_capacity(CLOCK_READINGS);
    let mut failures = Vec::new();
    for attempt in 1..=CLOCK_READINGS {
        let sent_at = millis();
        match public.server_time_millis().await {
            Ok(server_time) => readings.push(ClockSample {
                sent_at,
                server_time,
                received_at: millis(),
            }),
            // Noted and carried on from: one failed request says little about
            // the next, and the client's timeout bounds what waiting costs.
            Err(e) => failures.push((attempt, e)),
        }
    }
    let best = match ClockSample::best_of(&readings, CLOCK_READINGS) {
        Ok(best) => best,
        Err(why) => {
            for (attempt, e) in &failures {
                eprintln!("clock        reading {attempt} of {CLOCK_READINGS} failed: {e}");
            }
            println!();
            eprintln!("verdict      NOT READY: {why}");
            return ExitCode::FAILURE;
        }
    };
    // One binding, printed and gated on, so the two cannot disagree.
    let offset = best.offset_ms();
    println!(
        "clock        {offset} ms from the venue (positive: this host is ahead), limit \
         {MAX_CLOCK_SKEW_MS} ms"
    );
    println!(
        "             the tightest of the {} readings that came back, of {CLOCK_READINGS} tried: \
         round trip {} ms, naive {} ms",
        readings.len(),
        best.round_trip_ms(),
        best.naive_offset_ms()
    );
    for (attempt, e) in &failures {
        println!("             reading {attempt} of {CLOCK_READINGS} failed: {e}");
    }

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
