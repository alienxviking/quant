//! Live trading: live prices, live fills, the record first.
//!
//! ```text
//! live --symbol SYM [--root PATH] [--env testnet|live] [--journal PATH]
//!      [--cash N] [--qty N] [--minutes N | --hours N]
//!      [--fast N] [--slow N] [--interval-secs N]
//!      [--max-order N] [--max-position N] [--max-daily-loss N] [--max-orders N]
//!      [--rest URL] [--ws-api URL] [--market-ws URL]
//!
//! live --symbol BTCUSDT                       # Spot Testnet, data/testnet
//! live --symbol BTCUSDT --env live            # real money, data/live
//! ```
//!
//! The run is `quant_backtest::live`, whose module docs give the gates, the
//! shutdown and why each is where it is; this is its command line. The
//! session underneath is paper's (`quant_backtest::session`) with `LiveVenue`
//! in place of the simulator.
//!
//! # Which venue, and why the default is not the live one
//!
//! `--env` picks every endpoint at once, Spot Testnet's or Binance's, so the
//! REST base, the WebSocket API and the market stream cannot point at two
//! venues by accident. **The default is `testnet`.** The owner decided that
//! every protocol path is rehearsed on Spot Testnet before any real order
//! (`docs/live-run.md` §5), so the run that needs no money is the one that
//! needs no flag, and real money is spent only by typing `--env live`. A
//! mistyped command then lands on the testnet, where a live key is refused at
//! the first check. Each endpoint can be overridden on its own (`--rest`,
//! `--ws-api`, `--market-ws`) for a local stand-in, and only for one: an
//! override must name a loopback host, so that no override can point a run
//! at the other environment's venue under this one's banner. The banner says
//! which were overridden.
//!
//! The data root defaults to `data/<env>`, so that a testnet market, which is
//! not the real one, never lands in the raw tier beside the real market's.
//! It is given with `--root` rather than bare, unlike `paper`'s, so that an
//! argument this does not take is refused without being printed back: a key
//! pasted in the wrong place would otherwise appear in the banner as a path.
//! A value a numeric flag refuses is not printed back either. What
//! `--root` and `--journal` are given is printed, on purpose: any text is a
//! legal path, so no rule could tell a key from a directory, and the banner
//! is where the operator reads where the run writes.
//!
//! # Credentials
//!
//! From the environment only, as `venue-check` reads them:
//! `BINANCE_API_KEY` and `BINANCE_ED25519_PRIVATE_KEY`. `venue-check --help`
//! says how to make the key, at an absolute path outside the repository.
//!
//! # Exit codes
//!
//! - **0**: it traded and stopped clean.
//! - **1**: the run could not go on: it could not start, a capture ended in
//!   error, or the journal failed, after which every order is withheld and no
//!   closing line is written.
//! - **2**: an argument it does not take.
//! - **3**: a gate refused, and it recorded the market and traded nothing.
//!   Not a failure: a supervisor that restarted it would meet the same
//!   refusal, so it must end supervision on 3 as on 0. Distinct from 0, so
//!   that a harness can tell a run that traded from one that only recorded
//!   without reading its output.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use quant_backtest::live::{self, Endpoints, Environment, LiveConfig, LiveRun, Outcome};
use quant_backtest::session::Settings;
use quant_binance::account::AccountPolicy;
use quant_binance::credentials::Credentials;
use quant_binance::endpoint::NotLocal;
use quant_core::fixed::{Notional, Qty};
use quant_core::time::SystemClock;
use quant_engine::Limits;
use quant_live::Deadlines;
use tracing::error;
use tracing_subscriber::EnvFilter;

/// The exit code of a run that recorded only because a gate refused.
const RECORDED_ONLY: u8 = 3;

/// How long a run captures when not told: an afternoon, which is what M8 plans
/// (`docs/live-run.md` §2), not a fortnight.
const DEFAULT_MINUTES: u64 = 240;

/// What the command line asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Args {
    environment: Environment,
    endpoints: Endpoints,
    /// Which endpoints were given rather than taken from the environment.
    overridden: Vec<&'static str>,
    root: PathBuf,
    journal: PathBuf,
    symbol: String,
    cash: Notional,
    qty: Qty,
    minutes: u64,
    /// The same duration, which `minutes` is checked to fit.
    limit: Duration,
    fast: usize,
    slow: usize,
    interval_secs: i64,
    limits: Limits,
}

/// An argument this does not take. Not repeated, in case it was a key pasted
/// in the wrong place, as `venue-check` refuses one.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BadArgument(String);

fn main() -> ExitCode {
    let args = match parse_args(std::env::args_os().skip(1)) {
        Ok(Some(args)) => args,
        Ok(None) => return ExitCode::SUCCESS,
        Err(BadArgument(why)) => {
            eprintln!("{why}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    // JSON, so the metrics line is a value rather than prose, as paper's is.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    quant_binance::install_crypto_provider();
    banner(&args);

    let journalled = match live::journal_at(&args.journal) {
        Ok(journalled) => journalled,
        Err(e) => {
            error!(error = %e, "the journal could not be read or opened");
            eprintln!(
                "the journal {} could not be read or opened: {e}",
                args.journal.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let config = LiveConfig {
        environment: args.environment,
        endpoints: args.endpoints.clone(),
        root: args.root.clone(),
        symbol: args.symbol.clone(),
        cash: args.cash,
        settings: Settings {
            fast: args.fast,
            slow: args.slow,
            interval: args.interval_secs * 1_000_000_000,
            qty: args.qty,
        },
        limits: args.limits,
        limit: Some(args.limit),
        account_policy: AccountPolicy::default(),
        deadlines: Deadlines::default(),
        clock: Arc::new(SystemClock),
    };
    let keys = Credentials::from_env;
    match live::run(&config, &keys, journalled, tokio::signal::ctrl_c()) {
        Ok(run) => {
            let (code, line) = verdict(&run);
            if code == 1 {
                error!(verdict = %line, "the live run could not go on");
            }
            println!("{line}");
            ExitCode::from(code)
        }
        Err(e) => {
            error!(error = %e, "the live run stopped");
            eprintln!("the live run stopped: {e}");
            ExitCode::FAILURE
        }
    }
}

/// How a run that returned ended: its verdict line and its exit code.
fn verdict(run: &LiveRun) -> (u8, String) {
    if let Err(e) = &run.capture {
        return (
            1,
            format!("verdict   FAILED: the market capture ended in error: {e}"),
        );
    }
    match &run.outcome {
        Outcome::RecordOnly(refusal) => {
            (RECORDED_ONLY, format!("verdict   RECORDED ONLY: {refusal}"))
        }
        Outcome::Traded(traded) => {
            let tag = traded.run.as_str();
            if traded.latched {
                return (
                    1,
                    format!(
                        "verdict   JOURNAL FAILED under run tag {tag}: every order after its                          first failed append was withheld and no closing line was written, so                          the next run records only until a person has looked"
                    ),
                );
            }
            if let Err(e) = &traded.account {
                return (
                    1,
                    format!(
                        "verdict   FAILED under run tag {tag}: the account capture ended in                          error: {e}"
                    ),
                );
            }
            (
                0,
                format!(
                    "verdict   STOPPED under run tag {tag}, {} order(s) working",
                    traded.drained.working.len()
                ),
            )
        }
    }
}

/// Everything the run is about to do, before it does any of it.
fn banner(args: &Args) {
    match args.environment {
        Environment::Live => {
            println!("venue     LIVE: Binance, with real money (--env live)");
        }
        Environment::Testnet => println!(
            "venue     Spot Testnet, no money at risk (the default; --env live for real money)"
        ),
    }
    let mark = |name: &str| {
        if args.overridden.contains(&name) {
            " (overridden)"
        } else {
            ""
        }
    };
    println!("rest      {}{}", args.endpoints.rest, mark("rest"));
    println!("ws-api    {}{}", args.endpoints.ws_api, mark("ws-api"));
    println!(
        "market    {}{}",
        args.endpoints.market_ws,
        mark("market-ws")
    );
    println!("root      {}", args.root.display());
    println!("journal   {}", args.journal.display());
    println!("symbol    {}", args.symbol);
    println!("duration  {} minutes", args.minutes);
    println!(
        "strategy  MA crossover, fast {} slow {} on {}s mids, {} per position",
        args.fast, args.slow, args.interval_secs, args.qty
    );
    println!("{}", quant_backtest::limits_line(args.limits));
    println!();
}

const USAGE: &str = "usage: live --symbol SYM [--root PATH] [--env testnet|live] [--journal PATH]
            [--cash N] [--qty N] [--minutes N | --hours N]
            [--fast N] [--slow N] [--interval-secs N]
            [--max-order N] [--max-position N] [--max-daily-loss N] [--max-orders N]
            [--rest URL] [--ws-api URL] [--market-ws URL]

--env picks every endpoint at once and defaults to testnet: real money is
spent only with --env live. --rest, --ws-api and --market-ws are for a local
stand-in and take only a loopback host. Credentials come from BINANCE_API_KEY and
BINANCE_ED25519_PRIVATE_KEY, as venue-check reads them.

Exit 0 when it traded and stopped clean; 1 when the run could not go on,
the journal's failure included; 2 for an argument it does not take; 3 when a
gate refused and it recorded only, which a supervisor must not restart.";

#[allow(
    clippy::too_many_lines,
    reason = "one arm per flag, which reads better as one list than as several"
)]
fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Option<Args>, BadArgument> {
    let unknown = || {
        BadArgument(
            "live was given an argument it does not take, not repeated here in case it was a \
             key pasted in the wrong place"
                .to_owned(),
        )
    };
    let mut environment = Environment::Testnet;
    let (mut rest, mut ws_api, mut market_ws) = (None, None, None);
    let (mut root, mut journal, mut symbol) = (None, None, None);
    let mut cash: Notional = "100".parse().expect("a valid amount");
    let mut qty: Qty = "0.001".parse().expect("a valid quantity");
    let mut minutes = DEFAULT_MINUTES;
    let (mut fast, mut slow, mut interval_secs) = (10_usize, 30_usize, 60_i64);
    // Paper's, sized for $100 of capital trading 0.001 BTC: explicit, because
    // `Limits::default()` permits everything.
    let mut limits = Limits {
        max_order_notional: Some("200".parse().expect("a valid amount")),
        max_position_notional: Some("200".parse().expect("a valid amount")),
        max_daily_loss: Some("20".parse().expect("a valid amount")),
        max_orders_per_day: Some(200),
    };
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let Some(flag) = arg.to_str().map(str::to_owned) else {
            return Err(unknown());
        };
        let mut value = || -> Result<String, BadArgument> {
            args.next()
                .and_then(|v| v.into_string().ok())
                .ok_or_else(|| BadArgument(format!("{flag} needs a value")))
        };
        // The value is not repeated: a key pasted after a flag that takes a
        // number is as much a key in the wrong place as a bare one.
        let number = |what: &str| {
            BadArgument(format!(
                "{what} is not given a number this takes; not repeated here, in case it was a                  key pasted in the wrong place"
            ))
        };
        match flag.as_str() {
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(None);
            }
            "--env" => {
                environment = match value()?.as_str() {
                    "testnet" => Environment::Testnet,
                    "live" => Environment::Live,
                    _ => return Err(BadArgument("--env is testnet or live".to_owned())),
                }
            }
            "--symbol" => {
                let text = value()?;
                if text.is_empty()
                    || !text
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
                {
                    return Err(BadArgument(
                        "--symbol is spelled as the venue spells it, in capital letters and \
                         digits"
                            .to_owned(),
                    ));
                }
                symbol = Some(text);
            }
            "--journal" => journal = Some(PathBuf::from(value()?)),
            "--root" => root = Some(PathBuf::from(value()?)),
            "--cash" => {
                let text = value()?;
                cash = text.parse().map_err(|_| number("--cash"))?;
            }
            "--qty" => {
                let text = value()?;
                qty = text.parse().map_err(|_| number("--qty"))?;
            }
            "--minutes" => {
                let text = value()?;
                minutes = text.parse().map_err(|_| number("--minutes"))?;
            }
            "--hours" => {
                let text = value()?;
                let hours: u64 = text.parse().map_err(|_| number("--hours"))?;
                // Checked: a product that wrapped would be a duration nobody
                // asked for, shown in the banner as though it were.
                minutes = hours.checked_mul(60).ok_or_else(|| number("--hours"))?;
            }
            "--fast" => {
                let text = value()?;
                fast = text.parse().map_err(|_| number("--fast"))?;
            }
            "--slow" => {
                let text = value()?;
                slow = text.parse().map_err(|_| number("--slow"))?;
            }
            "--interval-secs" => {
                let text = value()?;
                interval_secs = text.parse().map_err(|_| number("--interval-secs"))?;
            }
            "--max-order" => {
                let text = value()?;
                limits.max_order_notional = Some(text.parse().map_err(|_| number("--max-order"))?);
            }
            "--max-position" => {
                let text = value()?;
                limits.max_position_notional =
                    Some(text.parse().map_err(|_| number("--max-position"))?);
            }
            "--max-daily-loss" => {
                let text = value()?;
                limits.max_daily_loss = Some(text.parse().map_err(|_| number("--max-daily-loss"))?);
            }
            "--max-orders" => {
                let text = value()?;
                limits.max_orders_per_day = Some(text.parse().map_err(|_| number("--max-orders"))?);
            }
            "--rest" => rest = Some(url(&value()?, &["http://", "https://"])?),
            "--ws-api" => ws_api = Some(url(&value()?, &["ws://", "wss://"])?),
            "--market-ws" => market_ws = Some(url(&value()?, &["ws://", "wss://"])?),
            // A bare argument too: the data root is `--root`, so that a key
            // pasted where no flag is expected is refused, not printed back
            // as a path in the banner.
            _ => return Err(unknown()),
        }
    }
    let Some(symbol) = symbol else {
        return Err(BadArgument(
            "--symbol is required; a live run trades one instrument".to_owned(),
        ));
    };
    if minutes == 0 {
        return Err(BadArgument("the duration must be positive".to_owned()));
    }
    let Some(limit) = minutes.checked_mul(60).map(Duration::from_secs) else {
        return Err(BadArgument(
            "the duration is longer than this can count in seconds".to_owned(),
        ));
    };
    if fast == 0 || slow <= fast {
        return Err(BadArgument(
            "--slow must exceed --fast, or the averages never cross".to_owned(),
        ));
    }
    if interval_secs <= 0 {
        return Err(BadArgument("--interval-secs must be positive".to_owned()));
    }
    let mut endpoints = environment.endpoints();
    let mut overridden = Vec::new();
    if let Some(rest) = rest {
        endpoints.rest = rest;
        overridden.push("rest");
    }
    if let Some(ws_api) = ws_api {
        endpoints.ws_api = ws_api;
        overridden.push("ws-api");
    }
    if let Some(market_ws) = market_ws {
        endpoints.market_ws = market_ws;
        overridden.push("market-ws");
    }
    let root = root.unwrap_or_else(|| PathBuf::from("data").join(environment.name()));
    let journal = journal.unwrap_or_else(|| root.join(format!("live-{symbol}.jsonl")));
    Ok(Some(Args {
        environment,
        endpoints,
        overridden,
        root,
        journal,
        symbol,
        cash,
        qty,
        minutes,
        limit,
        fast,
        slow,
        interval_secs,
        limits,
    }))
}

/// An endpoint override, if it begins with one of `schemes` and names a
/// loopback host: a key pasted in its place does not, and is refused without
/// being repeated. The rule, and why it is loopback only, is
/// `quant_binance::endpoint`'s, which `venue-check` holds its override to as
/// well: `--env` is the one way to choose a venue.
fn url(text: &str, schemes: &[&str]) -> Result<String, BadArgument> {
    quant_binance::endpoint::local_override(text, schemes).map_err(|why| match why {
        NotLocal::Scheme => BadArgument(format!(
            "an endpoint begins with {}, and that one does not; not repeated here",
            schemes.join(" or ")
        )),
        NotLocal::Host => BadArgument(
            "an endpoint override names a local stand-in, on 127.0.0.1, ::1 or localhost, and \
             that one does not; not repeated here. --env chooses the venue"
                .to_owned(),
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_args, verdict, Args, BadArgument, RECORDED_ONLY};
    use quant_backtest::live::{Environment, Gate, LiveRun, Outcome, Refusal, Traded};
    use quant_binance::capture::AccountCaptured;
    use quant_binance::user_data::RunTag;
    use quant_core::execution::ClientOrderId;
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::time::Duration;

    fn parsed(list: &[&str]) -> Result<Args, BadArgument> {
        parse_args(list.iter().map(OsString::from)).map(|args| args.expect("not help"))
    }

    #[test]
    fn the_default_venue_is_the_testnet_and_the_live_one_needs_its_flag() {
        // Real money only by typing it. Every endpoint follows the
        // environment, so none points at the live venue without it.
        let args = parsed(&["--symbol", "BTCUSDT"]).expect("parses");
        assert_eq!(args.environment, Environment::Testnet);
        assert_eq!(args.endpoints, Environment::Testnet.endpoints());
        for endpoint in [
            &args.endpoints.rest,
            &args.endpoints.ws_api,
            &args.endpoints.market_ws,
        ] {
            assert!(endpoint.contains("testnet"), "{endpoint}");
        }
        assert_eq!(args.root, PathBuf::from("data").join("testnet"));
        assert_eq!(
            args.journal,
            PathBuf::from("data")
                .join("testnet")
                .join("live-BTCUSDT.jsonl")
        );
        assert_eq!(args.overridden, Vec::<&str>::new());

        let args = parsed(&["--symbol", "BTCUSDT", "--env", "live"]).expect("parses");
        assert_eq!(args.environment, Environment::Live);
        assert_eq!(args.endpoints.rest, "https://api.binance.com");
        assert_eq!(
            args.endpoints.ws_api,
            "wss://ws-api.binance.com:443/ws-api/v3"
        );
        assert_eq!(args.endpoints.market_ws, "wss://stream.binance.com:9443");
        assert_eq!(args.root, PathBuf::from("data").join("live"));
    }

    #[test]
    fn each_endpoint_can_be_pointed_at_a_local_stand_in_and_says_so() {
        let args = parsed(&[
            "--symbol",
            "BTCUSDT",
            "--rest",
            "http://127.0.0.1:1",
            "--ws-api",
            "ws://127.0.0.1:2",
            "--market-ws",
            "ws://127.0.0.1:3",
            "--root",
            "elsewhere",
        ])
        .expect("parses");
        assert_eq!(args.environment, Environment::Testnet);
        assert_eq!(args.endpoints.rest, "http://127.0.0.1:1");
        assert_eq!(args.endpoints.ws_api, "ws://127.0.0.1:2");
        assert_eq!(args.endpoints.market_ws, "ws://127.0.0.1:3");
        assert_eq!(args.overridden, ["rest", "ws-api", "market-ws"]);
        assert_eq!(args.root, PathBuf::from("elsewhere"));
        for local in [
            "ws://localhost:9",
            "ws://LOCALHOST/ws",
            "ws://[::1]:5/ws-api/v3",
            "ws://127.0.0.2:7?x=1",
            "wss://127.0.0.1",
        ] {
            let args = parsed(&["--symbol", "BTCUSDT", "--env", "live", "--ws-api", local])
                .unwrap_or_else(|e| panic!("{local}: {e:?}"));
            assert_eq!(args.endpoints.ws_api, local);
        }
    }

    #[test]
    fn an_override_that_is_not_a_local_stand_in_is_refused_without_being_repeated() {
        // The other environment's hosts above all: a testnet banner over the
        // live venue's orders, or real money on the testnet's prices.
        for (flag, remote) in [
            ("--rest", "https://api.binance.com"),
            ("--rest", "https://testnet.binance.vision"),
            ("--ws-api", "wss://ws-api.binance.com:443/ws-api/v3"),
            ("--market-ws", "wss://stream.testnet.binance.vision"),
            ("--market-ws", "wss://stream.binance.com:9443"),
            // Loopback in name only.
            ("--rest", "http://127.0.0.1.example"),
            ("--rest", "http://localhost.example:80"),
            ("--rest", "http://127.0.0.1@example.org"),
            ("--rest", "http://localhost:1@example.org"),
            ("--rest", "http://[::1].example"),
            ("--rest", "http://[::2]:80"),
            ("--rest", "http://0.0.0.0:80"),
            ("--rest", "http://"),
        ] {
            for env in ["testnet", "live"] {
                let BadArgument(said) =
                    parsed(&["--symbol", "BTCUSDT", "--env", env, flag, remote]).expect_err(remote);
                assert!(said.contains("local stand-in"), "{remote}: {said}");
                assert!(!said.contains(remote), "{said}");
            }
        }
    }

    #[test]
    fn what_it_does_not_take_is_refused_without_being_repeated() {
        // Binance's example private key, as `venue-check`'s test uses it.
        let key = "MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u";
        let pem = format!("-----BEGIN PRIVATE KEY-----\n{key}\n-----END PRIVATE KEY-----");
        for given in [
            vec!["--symbol", "BTCUSDT", key],
            vec!["--symbol", "BTCUSDT", pem.as_str()],
            vec!["--symbol", "BTCUSDT", "--rest", key],
            vec!["--symbol", key],
            vec!["--symbol", "BTCUSDT", "--env", key],
        ] {
            let BadArgument(said) = parsed(&given).expect_err("refused");
            assert!(!said.contains(key), "{said}");
            assert!(!said.contains(&key[..12]), "{said}");
        }
        assert!(parsed(&[]).is_err(), "a symbol is required");
    }

    /// Every flag that takes a number.
    const NUMERIC: [&str; 11] = [
        "--cash",
        "--qty",
        "--minutes",
        "--hours",
        "--fast",
        "--slow",
        "--interval-secs",
        "--max-order",
        "--max-position",
        "--max-daily-loss",
        "--max-orders",
    ];

    #[test]
    fn a_key_given_to_a_flag_that_takes_a_number_is_refused_without_being_repeated() {
        let key = "MC4CAQAwBQYDK2VwBCIEIIJEYWtGBrhACmb9Dvy+qa8WEf0lQOl1s4CLIAB9m89u";
        for flag in NUMERIC {
            let BadArgument(said) = parsed(&["--symbol", "BTCUSDT", flag, key]).expect_err(flag);
            assert!(said.contains(flag), "{said}");
            assert!(!said.contains(&key[..12]), "{flag}: {said}");
        }
    }

    #[test]
    fn a_duration_too_long_to_count_is_refused_and_the_longest_that_fits_is_taken() {
        // The largest number of minutes whose seconds fit, and of hours whose
        // minutes do: taken, and counted exactly.
        let most = u64::MAX / 60;
        let args = parsed(&["--symbol", "BTCUSDT", "--minutes", &most.to_string()]).expect("fits");
        assert_eq!(args.minutes, most);
        assert_eq!(args.limit, Duration::from_secs(most * 60));
        let args = parsed(&["--symbol", "BTCUSDT", "--hours", "2"]).expect("fits");
        assert_eq!((args.minutes, args.limit), (120, Duration::from_secs(7200)));
        // One more of either is refused rather than wrapped into a duration
        // nobody asked for. `--hours 307445734561825861` is review's: its
        // minutes fit, and its seconds wrap.
        for (flag, value) in [
            ("--minutes", (most + 1).to_string()),
            ("--hours", (u64::MAX / 60 + 1).to_string()),
            ("--hours", "307445734561825861".to_owned()),
        ] {
            parsed(&["--symbol", "BTCUSDT", flag, &value]).expect_err(&value);
        }
    }

    #[test]
    fn a_number_each_numeric_flag_takes_is_taken() {
        // The other side of the refusal above: each flag still parses the
        // value it is for, so the refusal is about the value and not the flag.
        for (flag, value) in [
            ("--cash", "250"),
            ("--qty", "0.002"),
            ("--minutes", "30"),
            ("--hours", "2"),
            ("--fast", "3"),
            ("--slow", "40"),
            ("--interval-secs", "5"),
            ("--max-order", "150"),
            ("--max-position", "150"),
            ("--max-daily-loss", "10"),
            ("--max-orders", "50"),
        ] {
            assert!(NUMERIC.contains(&flag));
            parsed(&["--symbol", "BTCUSDT", flag, value])
                .unwrap_or_else(|e| panic!("{flag} {value}: {e:?}"));
        }
    }

    fn traded(latched: bool, account: Result<AccountCaptured, String>) -> LiveRun {
        LiveRun {
            outcome: Outcome::Traded(Box::new(Traded {
                run: RunTag::new("r1").expect("a tag"),
                cancelled: Vec::new(),
                drained: quant_live::Drained {
                    working: vec![ClientOrderId(4)],
                    settled: false,
                    answered: true,
                    outstanding: 0,
                },
                stats: quant_engine::EngineStats::default(),
                venue: quant_live::LiveStats::default(),
                latched,
                account,
            })),
            capture: Ok(()),
            tee_dropped: 0,
        }
    }

    fn sealed() -> AccountCaptured {
        AccountCaptured {
            stopped: None,
            stats: quant_recorder::IngressStats::default(),
            segments: Vec::new(),
        }
    }

    fn refused() -> LiveRun {
        LiveRun {
            outcome: Outcome::RecordOnly(Refusal {
                gate: Gate::Size,
                why: "too small".to_owned(),
            }),
            capture: Ok(()),
            tee_dropped: 0,
        }
    }

    #[test]
    fn each_way_a_run_ends_has_its_own_exit_code() {
        // Traded and stopped clean: 0.
        let (code, line) = verdict(&traded(false, Ok(sealed())));
        assert_eq!(code, 0, "{line}");
        assert!(
            line.contains("STOPPED under run tag r1, 1 order(s) working"),
            "{line}"
        );
        // A gate refused: 3, which is neither a failure nor a trade.
        let (code, line) = verdict(&refused());
        assert_eq!((code, RECORDED_ONLY), (3, 3), "{line}");
        assert!(
            line.contains("RECORDED ONLY") && line.contains("size"),
            "{line}"
        );
        // The journal failed: 1, however clean the rest of the stop was.
        let (code, line) = verdict(&traded(true, Ok(sealed())));
        assert_eq!(code, 1, "{line}");
        assert!(line.contains("JOURNAL FAILED"), "{line}");
        // A capture that ended in error: 1, on either stream.
        let (code, line) = verdict(&traded(false, Err("disk".to_owned())));
        assert_eq!(code, 1, "{line}");
        assert!(line.contains("account capture"), "{line}");
        for run in [traded(false, Ok(sealed())), refused()] {
            let run = LiveRun {
                capture: Err("disk".to_owned()),
                ..run
            };
            let (code, line) = verdict(&run);
            assert_eq!(code, 1, "{line}");
            assert!(line.contains("market capture"), "{line}");
        }
    }
}
