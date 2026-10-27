//! Prove the credentials, the signature, the clock and the key's permission to
//! trade — without placing an order.
//!
//! The preflight `docs/live-run.md` slice (b) names, and the only part of M8
//! that can be run end to end before any money is at risk. It makes three
//! signed requests, and none of them places anything:
//!
//! - **`GET /api/v3/account`** exercises the API key, the Ed25519 signature, the
//!   header and the clock in one round trip. That is not the cheapest signed
//!   call -- its weight is 20 (rest-api.md:4180-4181), where
//!   `GET /api/v3/openOrders` for one symbol is 6 (rest-api.md:4293-4294) -- but
//!   its answer carries the account's `canTrade` (rest-api.md:4207), which this
//!   reads. Its body also lists the account's balances, which this never prints.
//! - **`POST /api/v3/order/test`** proves the key may trade. The account
//!   endpoint is `USER_DATA` (rest-api.md:4174), and an API key cannot `TRADE`
//!   until trading is enabled for it in API Management (rest-api.md:194-196), so
//!   a key without that permission passes the first request all the same, and
//!   `canTrade` does not settle it either: the venue's documentation shows the
//!   field and never defines it. This endpoint is `TRADE`, weight 1, and
//!   *"Creates and validates a new order but does not send it into the matching
//!   engine"* (rest-api.md:2321-2333). It is sent the run's own order, a market
//!   buy of the checked symbol at the run's size, encoded as the run's orders
//!   are, and must accept it outright: [`probe`] says why any refusal of it, a
//!   filter failure included, is not ready.
//! - **`GET /api/v3/account/commission`** checks that the account does not pay
//!   its fees in BNB (rest-api.md:4797-4846). The user-data translator books a
//!   fee only in the pair's base or quote asset, so on a pair without BNB it
//!   refuses a fill whose fee was taken in BNB, and the run procedure turns the
//!   discount off. Its body holds the
//!   account's commission rates, of which this prints the one flag it reads.
//!
//! READY means all of it: the key, the signature and the clock work, the
//! account reports `canTrade`, the venue accepts the run's own order outright,
//! which shows the key may trade, and fees are not paid in BNB.
//!
//! ```text
//! # bash
//! export BINANCE_API_KEY=...
//! export BINANCE_ED25519_PRIVATE_KEY="$(cat "$HOME/.binance/ed25519.pem")"
//! # PowerShell
//! $env:BINANCE_API_KEY = "..."
//! $env:BINANCE_ED25519_PRIVATE_KEY = Get-Content -Raw "$env:USERPROFILE\.binance\ed25519.pem"
//!
//! cargo run -p quant-binance --bin venue-check               # the live venue
//! cargo run -p quant-binance --bin venue-check -- --testnet  # Spot Testnet
//! cargo run -p quant-binance --bin venue-check -- --symbol ETHUSDT
//! ```
//!
//! The key's path is absolute and outside the repository, because `cargo run`
//! works only inside the workspace and a relative path beside it would put the
//! private key in the repository. Never create it there.
//!
//! `--testnet` checks against Spot Testnet instead ([`SPOT_TESTNET_REST`]),
//! where `docs/live-run.md` §5 rehearses, with a key pair separate from the
//! live one. `--symbol` sets the symbol of the trade probe and of the
//! commission rates, [`DEFAULT_SYMBOL`] when not given.
//!
//! It prints the loaded key's public half, which is not secret, so it can be
//! compared with the one registered: a mismatch is otherwise a -1022 that says
//! only that the signature is not valid. Nothing it prints contains any part of
//! the private key: see `credentials.rs` for why that is a property of the type
//! rather than of this file's care, and for how to make the key.
//!
//! Three more flags exist for the PowerShell harness's preflights (item 8b).
//! `--qty` sets the trade probe's size, so that the probe is the run's own
//! order at any size the run is given and not only at M5's 0.001 the gate
//! inside `live` would otherwise be the first to judge. `--rest` points every
//! request at a local stand-in for a rehearsal, and only at one: the override
//! is held to `quant_binance::endpoint`'s loopback rule, the live binary's,
//! so it cannot point a check of one venue at the other. And `--clock` reads
//! the venue's clock alone, unsigned and with no credentials, round-trip
//! corrected by the same `read_clock` the live binary's first gate uses, so
//! that the record harness's preflight can ask it rather than measure
//! `now - serverTime` itself, which folds the one-way latency into the offset.
//!
//! Exit 0 ready, 1 not, 2 for an argument it does not take.

use std::ffi::OsString;
use std::process::ExitCode;
use std::time::Duration;

use quant_binance::credentials::Credentials;
use quant_binance::endpoint::local_override;
use quant_binance::orders::{EncodeError, NewOrder};
use quant_binance::rest::{SnapshotClient, SPOT_REST, SPOT_TESTNET_REST};
use quant_binance::trade::{
    read_clock, ClockSample, NotReady, TradeClient, CLOCK_READINGS, MAX_CLOCK_SKEW_MS,
};
use quant_core::fixed::Qty;

/// The symbol checked when `--symbol` is not given: the one M5 traded and M8
/// will.
const DEFAULT_SYMBOL: &str = "BTCUSDT";

/// How much the trade probe asks to buy: M5's `--qty`, the size the run trades
/// (`docs/live-run.md` §2), so that the venue validates the order the run would
/// send rather than one it never will.
const PROBE_QTY: &str = "0.001";

/// The help text. A raw string, so the commands in it read exactly as they are
/// to be typed, quotes and backslashes included.
const USAGE: &str = r#"usage: venue-check [--testnet] [--symbol SYMBOL] [--qty QTY] [--rest URL] [--clock]

Makes three signed requests and places nothing: GET /api/v3/account, for the
key, the signature, the clock and the account's canTrade; POST
/api/v3/order/test, which validates a market buy without sending it to the
matching engine, for this key's permission to trade; and GET
/api/v3/account/commission, for whether the account pays its fees in BNB. It
reads two variables, which the shell that runs it must export:

  bash:        export BINANCE_API_KEY=...
               export BINANCE_ED25519_PRIVATE_KEY="$(cat "$HOME/.binance/ed25519.pem")"
  PowerShell:  $env:BINANCE_API_KEY = "..."
               $env:BINANCE_ED25519_PRIVATE_KEY = Get-Content -Raw "$env:USERPROFILE\.binance\ed25519.pem"

The private key is an Ed25519 key in a file at that absolute path, outside this
repository. Never create it inside the repository, which is public, and never
paste key text into a shell, whose history keeps it: read it from the file, as
above. To make one:

  bash:        mkdir -p "$HOME/.binance"
               openssl genpkey -algorithm ed25519 -out "$HOME/.binance/ed25519.pem"
               openssl pkey -in "$HOME/.binance/ed25519.pem" -pubout
  PowerShell:  New-Item -ItemType Directory -Force "$env:USERPROFILE\.binance"
               openssl genpkey -algorithm ed25519 -out "$env:USERPROFILE\.binance\ed25519.pem"
               openssl pkey -in "$env:USERPROFILE\.binance\ed25519.pem" -pubout

The last command prints the public key, which is what to register with Binance.

  --testnet   check Spot Testnet, https://testnet.binance.vision, instead of the
              live venue. A testnet key pair is separate from the live one:
              register one with the testnet and load that instead.
  --symbol SYMBOL
              the symbol the trade probe names and the fee check reads, spelled
              as the venue spells it, in capital letters and digits: BTCUSDT
              when not given.
  --qty QTY   the size of the trade probe's market buy, the run's own size:
              0.001 when not given.
  --rest URL  send every request to a local stand-in for a rehearsal instead,
              on 127.0.0.1, ::1 or localhost and nowhere else.
  --clock     read the venue's clock only, round-trip corrected, unsigned and
              with no credentials: ready when it is within a second.

Exit 0 ready, 1 not ready, 2 for an argument it does not take.
"#;

/// What a pass prints, after a line for each of the three requests.
///
/// It names what was proved and that nothing was ordered. Balances and
/// commission rates were read and are not printed.
const READY: &str = "\
verdict      READY: the API key, the Ed25519 key, the signature and the clock work,
             the account reports canTrade, this API key may trade, the venue found
             nothing to refuse in a market buy at the run's size, and the account
             does not pay its fees in BNB. Nothing was ordered: the trade probe went
             to /api/v3/order/test, which validates an order without sending it to
             the matching engine. Balances and commission rates are not printed.
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

/// What a check is asked to check.
#[derive(Debug, PartialEq, Eq)]
struct Check {
    venue: Venue,
    symbol: String,
    /// The trade probe's size.
    qty: Qty,
    /// A local stand-in every request goes to instead of the venue's base.
    rest: Option<String>,
    /// The clock alone, with no credentials.
    clock_only: bool,
}

impl Check {
    /// Where every request goes.
    fn base(&self) -> &str {
        self.rest.as_deref().unwrap_or(self.venue.base())
    }
}

/// What the command line asks for.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Check(Check),
    Help,
}

/// An argument `venue-check` does not take, deliberately not repeated.
#[derive(Debug, PartialEq, Eq)]
struct UnknownArgument;

impl core::fmt::Display for UnknownArgument {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(
            "venue-check takes --testnet, --symbol followed by a symbol in capital letters and \
             digits, --qty followed by a quantity, --rest followed by a local stand-in's URL, \
             --clock, and --help, and was given something else. It is not repeated here, in case \
             it was a key pasted in the wrong place.",
        )
    }
}

/// Read the command line.
///
/// An argument this does not know is refused without being echoed: a private
/// key pasted where an argument goes would otherwise be printed straight back,
/// to a terminal and whatever is recording it. Arguments are read as `OsString`
/// so one that is not valid text is refused too, rather than panicking the way
/// `std::env::args` does. A symbol is printed and sent, so it is held to the
/// same rule: see [`parse_symbol`].
fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Command, UnknownArgument> {
    let mut venue = Venue::Live;
    let mut symbol = None;
    let mut qty = None;
    let mut rest = None;
    let mut clock_only = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--testnet") => venue = Venue::Testnet,
            Some("--symbol") => symbol = Some(parse_symbol(args.next().as_ref())?),
            Some("--qty") => qty = Some(parse_qty(args.next().as_ref())?),
            Some("--rest") => rest = Some(parse_rest(args.next().as_ref())?),
            Some("--clock") => clock_only = true,
            Some("-h" | "--help") => return Ok(Command::Help),
            _ => return Err(UnknownArgument),
        }
    }
    Ok(Command::Check(Check {
        venue,
        symbol: symbol.unwrap_or_else(|| DEFAULT_SYMBOL.to_owned()),
        qty: qty.unwrap_or_else(|| PROBE_QTY.parse().expect("a valid literal")),
        rest,
        clock_only,
    }))
}

/// The value given after `--qty`, if it is a positive quantity. Anything
/// else is refused without being echoed, as every argument this refuses is:
/// the alphabets a key comes in do not parse as a quantity.
fn parse_qty(given: Option<&OsString>) -> Result<Qty, UnknownArgument> {
    given
        .and_then(|value| value.to_str())
        .and_then(|text| text.parse::<Qty>().ok())
        .filter(|qty| *qty > Qty::ZERO)
        .ok_or(UnknownArgument)
}

/// The value given after `--rest`, if it is an `http` or `https` URL on a
/// loopback host (`quant_binance::endpoint`), so that no override can point a
/// check of one venue at the other.
fn parse_rest(given: Option<&OsString>) -> Result<String, UnknownArgument> {
    given
        .and_then(|value| value.to_str())
        .and_then(|text| local_override(text, &["http://", "https://"]).ok())
        .ok_or(UnknownArgument)
}

/// The value given after `--symbol`, if it is spelled as the venue spells the
/// symbols this project trades: capital letters and digits, `BTCUSDT`.
///
/// Anything else, a missing value included, is an argument this does not take,
/// and is refused without being echoed. A symbol is printed, so a key pasted
/// after `--symbol` would otherwise be printed back like any other; and the
/// alphabets a key comes in, an API key's mixed case and a PEM body's `+`, `/`
/// and `=`, all fail this.
fn parse_symbol(given: Option<&OsString>) -> Result<String, UnknownArgument> {
    match given.and_then(|value| value.to_str()) {
        Some(text)
            if !text.is_empty()
                && text
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit()) =>
        {
            Ok(text.to_owned())
        }
        _ => Err(UnknownArgument),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let asked = match parse_args(std::env::args_os().skip(1)) {
        Ok(Command::Check(asked)) => asked,
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
    let venue = asked.venue;
    match venue {
        Venue::Live => println!("venue        {}", venue.base()),
        Venue::Testnet => {
            println!("venue        {}, Spot Testnet", venue.base());
            println!("             a testnet key pair is separate from the live one");
        }
    }
    if let Some(rest) = &asked.rest {
        println!("             every request goes instead to {rest}, a local stand-in (--rest)");
    }
    if asked.clock_only {
        // The clock alone: unsigned, so no credentials are read, and nothing
        // about the account or the key is claimed.
        return match clock_offset(asked.base()).await {
            Ok(offset) if offset.unsigned_abs() <= MAX_CLOCK_SKEW_MS.unsigned_abs() => {
                println!(
                    "verdict      CLOCK READY: within {MAX_CLOCK_SKEW_MS} ms of the venue. Only \
                     the clock was checked (--clock): no key, no signature, no account"
                );
                ExitCode::SUCCESS
            }
            Ok(offset) => {
                println!();
                eprintln!(
                    "verdict      NOT READY: {}",
                    NotReady::ClockSkew {
                        offset_ms: offset,
                        allowed_ms: MAX_CLOCK_SKEW_MS,
                    }
                );
                ExitCode::FAILURE
            }
            Err(code) => code,
        };
    }
    let symbol = asked.symbol.as_str();
    println!("symbol       {symbol}, which the trade probe names and the fee check reads");

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

    let offset = match clock_offset(asked.base()).await {
        Ok(offset) => offset,
        Err(code) => return code,
    };
    check(&asked, credentials, offset).await
}

/// How far this host's clock is from the clock at `base`, printed, or the
/// exit code when there is no verdict to be had.
async fn clock_offset(base: &str) -> Result<i64, ExitCode> {
    // The venue's clock, read the way NTP reads one: the host's clock either
    // side of `GET /api/v3/time`, the venue's reading taken to describe the
    // midpoint, and of several readings the one with the smallest round trip
    // believed. `ClockSample` holds the arithmetic and the reasons for it.
    //
    // This is not the recorder's startup check, and the two figures differ by
    // design. That one reads `before - serverTime` once and leaves it
    // uncorrected (`SnapshotClient::clock_offset_millis` says why), so it comes
    // out short by the outbound leg: on a slow link, enough to make a clock
    // that is fine look like one behind the venue. Tolerable in a warning
    // about a metric; not in the figure that decides whether this host may
    // trade. `ops/preflight.ps1` and `ops/fix-clock.ps1` can read this one, with
    // `--clock`, rather than measure the naive figure themselves.
    // `ops/preflight.sh` and `ops/fix-clock.sh` compute the same midpoint as
    // this, with the opposite sign: positive there means the host is behind.
    let public = match SnapshotClient::new(base, Duration::from_secs(10)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("clock        could not build a client: {e}");
            return Err(ExitCode::FAILURE);
        }
    };
    let (readings, failures) = read_clock(&public, millis).await;
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

/// The order `POST /api/v3/order/test` is asked to validate: a market buy of
/// `qty` of `symbol`, [`PROBE_QTY`] unless `--qty` says otherwise, the shape `MaCrossover` sends, written by the
/// encoder the run's orders go through. `quant_binance::trade::probe` writes
/// it, and says why any refusal of it, a filter failure included, is not
/// ready; the live binary asks the venue the same question with the same
/// probe at its own size.
fn probe(symbol: &str, qty: Qty) -> Result<NewOrder, EncodeError> {
    quant_binance::trade::probe(symbol, qty)
}

/// The three signed requests, a line for each, and the verdict.
async fn check(asked: &Check, credentials: Credentials, offset: i64) -> ExitCode {
    let (venue, symbol, qty) = (asked.venue, asked.symbol.as_str(), asked.qty);
    let client = match TradeClient::new(asked.base(), Duration::from_secs(10), credentials) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("venue        could not build a client: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(why) = client.check_ready(offset).await {
        return not_ready(venue, &why);
    }
    println!("account      a signed request was accepted, and the account reports canTrade true");

    let probe = match probe(symbol, qty) {
        Ok(probe) => probe,
        Err(why) => {
            eprintln!("permission   could not write the trade probe: {why}");
            return ExitCode::FAILURE;
        }
    };
    // Ready only on the venue's acceptance outright: a refusal of the probe,
    // a filter failure included, is one the run's own orders would meet.
    if let Err(why) = client.check_trade_permission(&probe, offset).await {
        return not_ready(venue, &why);
    }
    println!(
        "permission   POST /api/v3/order/test accepted a market buy of {qty} {symbol}, the \
         run's own order, and refused nothing: this API key may trade"
    );

    if let Err(why) = client.check_bnb_discount_off(symbol, offset).await {
        return not_ready(venue, &why);
    }
    println!(
        "fees         the account does not pay its fees in BNB (discount.enabledForAccount false)"
    );
    print!("{READY}");
    ExitCode::SUCCESS
}

/// Print why, and the exit code for it.
fn not_ready(venue: Venue, why: &NotReady) -> ExitCode {
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

fn millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::{parse_args, probe, Check, Command, UnknownArgument, Venue, READY, USAGE};
    use quant_core::fixed::Qty;
    use std::ffi::OsString;

    fn args(list: &[&str]) -> Vec<OsString> {
        list.iter().map(OsString::from).collect()
    }

    fn check(venue: Venue, symbol: &str) -> Command {
        Command::Check(Check {
            venue,
            symbol: symbol.to_owned(),
            qty: qty("0.001"),
            rest: None,
            clock_only: false,
        })
    }

    fn qty(text: &str) -> Qty {
        text.parse().expect("a quantity")
    }

    fn asked(given: &[&str]) -> Check {
        match parse_args(args(given)) {
            Ok(Command::Check(asked)) => asked,
            other => panic!("{given:?}: {other:?}"),
        }
    }

    #[test]
    fn the_harness_flags_set_the_size_a_local_stand_in_and_the_clock_alone() {
        let plain = asked(&[]);
        assert_eq!(
            (plain.qty, &plain.rest, plain.clock_only),
            (qty("0.001"), &None, false)
        );
        assert_eq!(plain.base(), "https://api.binance.com");

        let given = asked(&[
            "--testnet",
            "--qty",
            "0.002",
            "--rest",
            "http://127.0.0.1:9",
        ]);
        assert_eq!(given.venue, Venue::Testnet);
        assert_eq!(given.qty, qty("0.002"));
        // The stand-in is where every request goes, whichever venue is named.
        assert_eq!(given.base(), "http://127.0.0.1:9");
        assert!(
            probe(&given.symbol, given.qty)
                .expect("encodes")
                .query()
                .contains("&quantity=0.002&"),
            "the probe is the run's size"
        );
        for local in [
            "http://localhost:1",
            "https://[::1]:2/api",
            "http://127.0.0.2",
        ] {
            assert_eq!(asked(&["--rest", local]).base(), local);
        }
        assert!(asked(&["--clock"]).clock_only);
    }

    #[test]
    fn a_stand_in_that_is_not_local_or_a_size_that_is_not_one_is_refused_unrepeated() {
        // Either venue named as a stand-in above all: a check of the testnet
        // that reached the live venue, or the other way round.
        let key = "MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u";
        for given in [
            args(&["--rest", "https://api.binance.com"]),
            args(&["--testnet", "--rest", "https://testnet.binance.vision"]),
            args(&["--rest", "http://127.0.0.1.example"]),
            args(&["--rest", "http://127.0.0.1@example.org"]),
            args(&["--rest", "ws://127.0.0.1:9"]),
            args(&["--rest", key]),
            args(&["--rest"]),
            args(&["--qty", key]),
            args(&["--qty", "0"]),
            args(&["--qty", "-0.001"]),
            args(&["--qty"]),
        ] {
            let why = parse_args(given.clone()).expect_err("refused");
            assert_eq!(why, UnknownArgument, "{given:?}");
            assert!(!why.to_string().contains(&key[..12]));
        }
    }

    #[test]
    fn the_flags_it_takes_are_understood() {
        assert_eq!(parse_args(args(&[])), Ok(check(Venue::Live, "BTCUSDT")));
        assert_eq!(
            parse_args(args(&["--testnet"])),
            Ok(check(Venue::Testnet, "BTCUSDT"))
        );
        assert_eq!(
            parse_args(args(&["--symbol", "ETHUSDT"])),
            Ok(check(Venue::Live, "ETHUSDT"))
        );
        assert_eq!(
            parse_args(args(&["--symbol", "1000SATSUSDT", "--testnet"])),
            Ok(check(Venue::Testnet, "1000SATSUSDT"))
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
        // stands in for it. A symbol is printed, so the same goes for a key
        // pasted after --symbol, and for an API key's mixed case there.
        let key = "MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u";
        // Binance's example API key (rest-api.md:563): mixed case.
        let api_key = "4yNzx3yWC5bS6YTwEkSRaC0nRmSQIIStAUOh1b6kqaBrTLIhjCpI5lJH8q8R8WNO";
        for (given, unknown) in [
            (args(&[key]), key),
            (args(&["--testnet", key]), key),
            (args(&["--live"]), "--live"),
            (args(&["--symbol", key]), key),
            (args(&["--symbol", api_key]), api_key),
            (args(&["--symbol", "btcusdt"]), "btcusdt"),
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
        // And a symbol that is missing, empty, or spelled with anything but
        // capital letters and digits.
        for given in [
            args(&["--symbol"]),
            args(&["--symbol", ""]),
            args(&["--symbol", "BTC-USDT"]),
            args(&["--symbol", "BTCUSDT "]),
            args(&["--symbol", "--testnet"]),
        ] {
            assert_eq!(parse_args(given.clone()), Err(UnknownArgument), "{given:?}");
        }
        // Text that is not valid text is refused rather than panicking, as an
        // argument and as a symbol.
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
        assert_eq!(parse_args([odd.clone()]), Err(UnknownArgument));
        assert_eq!(
            parse_args([OsString::from("--symbol"), odd]),
            Err(UnknownArgument)
        );
    }

    #[test]
    fn the_trade_probe_is_a_market_buy_of_the_symbol_at_the_runs_size() {
        // The run's shape and size, under an id no run of ours would send,
        // written by the encoder every order goes through.
        assert_eq!(
            probe("BTCUSDT", qty("0.001")).expect("encodes").query(),
            "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=0.001&newClientOrderId=q-venuecheck-0\
             &newOrderRespType=ACK"
        );
        assert!(probe("ETHUSDT", qty("0.001"))
            .expect("encodes")
            .query()
            .starts_with("symbol=ETHUSDT&"));
    }

    #[test]
    fn ready_says_what_it_proved_and_that_nothing_was_ordered() {
        // Each of the three requests has its clause, the permission to trade
        // among them now that a TRADE request proves it, and the pass says
        // where the probe went: an order that was only validated. READY
        // follows only the venue's acceptance of the probe outright, so it
        // says that too: a pass is a pass for the run's own order.
        let said = READY.split_whitespace().collect::<Vec<_>>().join(" ");
        for needle in [
            "READY: the API key, the Ed25519 key, the signature and the clock work",
            "the account reports canTrade",
            "this API key may trade",
            "the venue found nothing to refuse in a market buy at the run's size",
            "the account does not pay its fees in BNB",
            "Nothing was ordered",
            "/api/v3/order/test, which validates an order without sending it to the matching \
             engine",
            "Balances and commission rates are not printed",
        ] {
            assert!(said.contains(needle), "{needle:?} missing from: {said}");
        }
        // canTrade is the account's, and undefined; that the key may trade is
        // what the probe proves, and READY must not run the two together.
        assert!(!said.contains("the account can trade"), "{said}");
        assert!(!said.contains("NOT checked"), "{said}");
    }

    #[test]
    fn the_usage_shows_both_shells_how_to_make_a_key_the_testnet_and_the_symbol() {
        // Every path to the key absolute and outside the repository, in both
        // shells: these lines are run where `cargo run` runs, inside the
        // workspace, so a relative path would put the private key in it.
        for needle in [
            "export BINANCE_API_KEY=",
            r#"export BINANCE_ED25519_PRIVATE_KEY="$(cat "$HOME/.binance/ed25519.pem")""#,
            r#"mkdir -p "$HOME/.binance""#,
            r#"openssl genpkey -algorithm ed25519 -out "$HOME/.binance/ed25519.pem""#,
            r#"openssl pkey -in "$HOME/.binance/ed25519.pem" -pubout"#,
            "$env:BINANCE_API_KEY = ",
            r#"$env:BINANCE_ED25519_PRIVATE_KEY = Get-Content -Raw "$env:USERPROFILE\.binance\ed25519.pem""#,
            r#"New-Item -ItemType Directory -Force "$env:USERPROFILE\.binance""#,
            r#"openssl genpkey -algorithm ed25519 -out "$env:USERPROFILE\.binance\ed25519.pem""#,
            r#"openssl pkey -in "$env:USERPROFILE\.binance\ed25519.pem" -pubout"#,
            "Never create it inside the repository",
            "never\npaste key text into a shell",
            "--testnet",
            "https://testnet.binance.vision",
            "A testnet key pair is separate from the live one",
            "usage: venue-check [--testnet] [--symbol SYMBOL] [--qty QTY] [--rest URL] [--clock]\n",
            "\n  --rest URL  send every request to a local stand-in",
            "on 127.0.0.1, ::1 or localhost and nowhere else",
            "\n  --clock     read the venue's clock only",
            "\n  --symbol SYMBOL\n",
            "in capital letters and digits: BTCUSDT",
            "places nothing",
            "/api/v3/order/test",
        ] {
            assert!(USAGE.contains(needle), "{needle:?} missing from the usage");
        }
        assert!(!USAGE.contains("key.pem"), "a relative key path: {USAGE}");
    }
}
