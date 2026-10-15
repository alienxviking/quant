//! Prove the credentials, the signature and the clock — without placing an order.
//!
//! The preflight `docs/live-run.md` slice (b) names, and the only part of M8
//! that can be run end to end before any money is at risk. It makes exactly one
//! signed request, `GET /api/v3/account`. That is not the cheapest signed call
//! -- its weight is 20 (rest-api.md:4180-4181), where `GET /api/v3/openOrders`
//! for one symbol is 6 (rest-api.md:4293-4294) -- but it is the one whose answer
//! says whether this key may trade at all (`canTrade`, rest-api.md:4207), and it
//! exercises the API key, the Ed25519 signature, the header and the clock in
//! one round trip and places nothing. Its body also lists the account's
//! balances, which this never prints.
//!
//! ```text
//! # bash
//! export BINANCE_API_KEY=...
//! export BINANCE_ED25519_PRIVATE_KEY="$(cat key.pem)"
//! # PowerShell
//! $env:BINANCE_API_KEY = "..."
//! $env:BINANCE_ED25519_PRIVATE_KEY = Get-Content -Raw key.pem
//!
//! cargo run -p quant-binance --bin venue-check               # the live venue
//! cargo run -p quant-binance --bin venue-check -- --testnet  # Spot Testnet
//! ```
//!
//! `--testnet` checks against Spot Testnet instead ([`SPOT_TESTNET_REST`]),
//! where `docs/live-run.md` §5 rehearses, with a key pair separate from the
//! live one.
//!
//! It prints the loaded key's public half, which is not secret, so it can be
//! compared with the one registered: a mismatch is otherwise a -1022 that says
//! only that the signature is not valid. Nothing it prints contains any part of
//! the private key: see `credentials.rs` for why that is a property of the type
//! rather than of this file's care, and for how to make the key.
//!
//! Exit 0 ready, 1 not, 2 for an argument it does not take.

use std::ffi::OsString;
use std::process::ExitCode;
use std::time::Duration;

use quant_binance::credentials::Credentials;
use quant_binance::rest::{SnapshotClient, SPOT_REST, SPOT_TESTNET_REST};
use quant_binance::trade::{
    ClockSample, NotReady, TradeClient, MAX_CLOCK_SKEW_MS, MIN_CLOCK_READINGS,
};

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

const USAGE: &str = "\
usage: venue-check [--testnet]

Makes one signed request, GET /api/v3/account, and places nothing. It reads two
variables, which the shell that runs it must export:

  bash:        export BINANCE_API_KEY=...
               export BINANCE_ED25519_PRIVATE_KEY=\"$(cat key.pem)\"
  PowerShell:  $env:BINANCE_API_KEY = \"...\"
               $env:BINANCE_ED25519_PRIVATE_KEY = Get-Content -Raw key.pem

key.pem is an Ed25519 private key, kept outside this repository:

  openssl genpkey -algorithm ed25519 -out key.pem
  openssl pkey -in key.pem -pubout     # the public key, to register with Binance

  --testnet   check Spot Testnet, https://testnet.binance.vision, instead of the
              live venue. A testnet key pair is separate from the live one:
              register one with the testnet and load that instead.

Exit 0 ready, 1 not ready, 2 for an argument it does not take.
";

/// Which venue to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Venue {
    Live,
    Testnet,
}

impl Venue {
    const fn base(self) -> &'static str {
        match self {
            Self::Live => SPOT_REST,
            Self::Testnet => SPOT_TESTNET_REST,
        }
    }
}

/// What the command line asks for.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Check(Venue),
    Help,
}

/// An argument `venue-check` does not take, deliberately not repeated.
#[derive(Debug, PartialEq, Eq)]
struct UnknownArgument;

impl core::fmt::Display for UnknownArgument {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(
            "venue-check takes --testnet and --help, and was given something else. It is not \
             repeated here, in case it was a key pasted in the wrong place.",
        )
    }
}

/// Read the command line.
///
/// An argument this does not know is refused without being echoed: a private
/// key pasted where an argument goes would otherwise be printed straight back,
/// to a terminal and whatever is recording it. Arguments are read as `OsString`
/// so one that is not valid text is refused too, rather than panicking the way
/// `std::env::args` does.
fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Command, UnknownArgument> {
    let mut venue = Venue::Live;
    for arg in args {
        match arg.to_str() {
            Some("--testnet") => venue = Venue::Testnet,
            Some("-h" | "--help") => return Ok(Command::Help),
            _ => return Err(UnknownArgument),
        }
    }
    Ok(Command::Check(venue))
}

#[tokio::main]
async fn main() -> ExitCode {
    let venue = match parse_args(std::env::args_os().skip(1)) {
        Ok(Command::Check(venue)) => venue,
        Ok(Command::Help) => {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(why) => {
            eprintln!("{why}");
            eprintln!();
            eprint!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match venue {
        Venue::Live => println!("venue        {}", venue.base()),
        Venue::Testnet => {
            println!("venue        {}, Spot Testnet", venue.base());
            println!("             a testnet key pair is separate from the live one");
        }
    }

    let credentials = match Credentials::from_env() {
        Ok(c) => c,
        Err(why) => {
            eprintln!("credentials  {why}");
            eprintln!();
            eprint!("{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "credentials  {} is set, and {} reads as an Ed25519 key. Its public half, which must be \
         the one registered for that API key:",
        Credentials::KEY_VAR,
        Credentials::PRIVATE_KEY_VAR
    );
    print!("{}", credentials.public_key_pem());

    let offset = match clock_offset(venue).await {
        Ok(offset) => offset,
        Err(code) => return code,
    };
    check(venue, credentials, offset).await
}

/// How far this host's clock is from the venue's, printed, or the exit code
/// when there is no verdict to be had.
async fn clock_offset(venue: Venue) -> Result<i64, ExitCode> {
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
    let public = match SnapshotClient::new(venue.base(), Duration::from_secs(10)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("clock        could not build a client: {e}");
            return Err(ExitCode::FAILURE);
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
            return Err(ExitCode::FAILURE);
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
    Ok(offset)
}

/// The signed request, and the verdict.
async fn check(venue: Venue, credentials: Credentials, offset: i64) -> ExitCode {
    let client = match TradeClient::new(venue.base(), Duration::from_secs(10), credentials) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("venue        could not build a client: {e}");
            return ExitCode::FAILURE;
        }
    };
    match client.check_ready(offset).await {
        Ok(()) => {
            println!("venue        a signed request was accepted, and the account can trade");
            println!();
            println!("verdict      READY: the API key, the Ed25519 key and the clock all work.");
            println!(
                "             Nothing was ordered, and this proves nothing about balances, \
                 which are not printed."
            );
            ExitCode::SUCCESS
        }
        Err(why) => {
            println!();
            eprintln!("verdict      NOT READY: {why}");
            if venue == Venue::Testnet && matches!(why, NotReady::BadCredentials { .. }) {
                eprintln!(
                    "             this was Spot Testnet, whose key pairs are registered with the \
                     testnet ({SPOT_TESTNET_REST}), not with the live venue"
                );
            }
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

#[cfg(test)]
mod tests {
    use super::{parse_args, Command, UnknownArgument, Venue, USAGE};
    use std::ffi::OsString;

    fn args(list: &[&str]) -> Vec<OsString> {
        list.iter().map(OsString::from).collect()
    }

    #[test]
    fn the_flags_it_takes_are_understood() {
        assert_eq!(parse_args(args(&[])), Ok(Command::Check(Venue::Live)));
        assert_eq!(
            parse_args(args(&["--testnet"])),
            Ok(Command::Check(Venue::Testnet))
        );
        assert_eq!(parse_args(args(&["--help"])), Ok(Command::Help));
        assert_eq!(parse_args(args(&["--testnet", "-h"])), Ok(Command::Help));
        assert_eq!(Venue::Live.base(), "https://api.binance.com");
        assert_eq!(Venue::Testnet.base(), "https://testnet.binance.vision");
    }

    #[test]
    fn an_argument_it_does_not_take_is_refused_without_being_repeated() {
        // The likeliest unknown argument worth worrying about: the private
        // key, pasted where an argument goes. Binance's published example key
        // stands in for it.
        let key = "MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u";
        for (given, unknown) in [
            (args(&[key]), key),
            (args(&["--testnet", key]), key),
            (args(&["--live"]), "--live"),
        ] {
            let why = parse_args(given).expect_err("refused");
            assert_eq!(why, UnknownArgument);
            // What would be printed, read off the error that came back rather
            // than off a fresh one, so an error that grew a field to carry the
            // argument would be caught here as well as at the comparison.
            let said = why.to_string();
            assert!(!said.contains(unknown), "{said}");
            for i in 0..=unknown.len().saturating_sub(8) {
                let piece = &unknown[i..(i + 8).min(unknown.len())];
                assert!(!said.contains(piece), "{piece} in {said}");
            }
        }
        // Text that is not valid text is refused rather than panicking.
        #[cfg(windows)]
        let odd = {
            use std::os::windows::ffi::OsStringExt;
            OsString::from_wide(&[0xd800])
        };
        #[cfg(unix)]
        let odd = {
            use std::os::unix::ffi::OsStringExt;
            OsString::from_vec(vec![0xff])
        };
        assert_eq!(parse_args([odd]), Err(UnknownArgument));
    }

    #[test]
    fn the_usage_shows_both_shells_how_to_make_a_key_and_the_testnet() {
        for needle in [
            "export BINANCE_API_KEY=",
            "export BINANCE_ED25519_PRIVATE_KEY=\"$(cat key.pem)\"",
            "$env:BINANCE_API_KEY = ",
            "$env:BINANCE_ED25519_PRIVATE_KEY = Get-Content -Raw key.pem",
            "openssl genpkey -algorithm ed25519 -out key.pem",
            "openssl pkey -in key.pem -pubout",
            "--testnet",
            "https://testnet.binance.vision",
            "A testnet key pair is separate from the live one",
        ] {
            assert!(USAGE.contains(needle), "{needle:?} missing from the usage");
        }
    }
}
