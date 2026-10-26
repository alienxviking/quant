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
//! `--ws-api`, `--market-ws`) for a local stand-in, and the banner says which
//! were.
//!
//! The data root defaults to `data/<env>`, so that a testnet market, which is
//! not the real one, never lands in the raw tier beside the real market's.
//! It is given with `--root` rather than bare, unlike `paper`'s, so that an
//! argument this does not take is refused without being printed back: a key
//! pasted in the wrong place would otherwise appear in the banner as a path.
//!
//! # Credentials
//!
//! From the environment only, as `venue-check` reads them:
//! `BINANCE_API_KEY` and `BINANCE_ED25519_PRIVATE_KEY`. `venue-check --help`
//! says how to make the key, at an absolute path outside the repository.
//!
//! Exit 0 for a clean stop, whether it traded or recorded only (a refused gate
//! is said loudly and is not a crash, so a supervisor must not restart it into
//! the same refusal); 1 when the run could not go on; 2 for an argument it
//! does not take.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use quant_backtest::live::{self, Endpoints, Environment, LiveConfig, Outcome};
use quant_backtest::session::Settings;
use quant_binance::account::AccountPolicy;
use quant_binance::credentials::Credentials;
use quant_core::fixed::{Notional, Qty};
use quant_core::time::SystemClock;
use quant_engine::Limits;
use quant_live::Deadlines;
use tracing::error;
use tracing_subscriber::EnvFilter;

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
        limit: Some(Duration::from_secs(args.minutes * 60)),
        account_policy: AccountPolicy::default(),
        deadlines: Deadlines::default(),
        clock: Arc::new(SystemClock),
    };
    let keys = Credentials::from_env;
    match live::run(&config, &keys, journalled, tokio::signal::ctrl_c()) {
        Ok(run) => {
            if let Err(e) = &run.capture {
                error!(error = %e, "the market capture ended in error");
                return ExitCode::FAILURE;
            }
            match run.outcome {
                Outcome::RecordOnly(refusal) => {
                    println!("verdict   RECORDED ONLY: {refusal}");
                }
                Outcome::Traded(traded) => {
                    if let Err(e) = &traded.account {
                        error!(error = %e, "the account capture ended in error");
                        return ExitCode::FAILURE;
                    }
                    println!(
                        "verdict   STOPPED under run tag {}, {} order(s) working",
                        traded.run.as_str(),
                        traded.drained.working.len()
                    );
                }
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!(error = %e, "the live run stopped");
            eprintln!("the live run stopped: {e}");
            ExitCode::FAILURE
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
spent only with --env live. Credentials come from BINANCE_API_KEY and
BINANCE_ED25519_PRIVATE_KEY, as venue-check reads them.

Exit 0 for a clean stop, traded or recorded only; 1 when the run could not go
on; 2 for an argument it does not take.";

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
        let number = |what: &str, text: String| {
            BadArgument(format!("{what} is not a number this takes: {text:?}"))
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
                cash = text.parse().map_err(|_| number("--cash", text))?;
            }
            "--qty" => {
                let text = value()?;
                qty = text.parse().map_err(|_| number("--qty", text))?;
            }
            "--minutes" => {
                let text = value()?;
                minutes = text.parse().map_err(|_| number("--minutes", text))?;
            }
            "--hours" => {
                let text = value()?;
                let hours: u64 = text.parse().map_err(|_| number("--hours", text))?;
                minutes = hours * 60;
            }
            "--fast" => {
                let text = value()?;
                fast = text.parse().map_err(|_| number("--fast", text))?;
            }
            "--slow" => {
                let text = value()?;
                slow = text.parse().map_err(|_| number("--slow", text))?;
            }
            "--interval-secs" => {
                let text = value()?;
                interval_secs = text.parse().map_err(|_| number("--interval-secs", text))?;
            }
            "--max-order" => {
                let text = value()?;
                limits.max_order_notional =
                    Some(text.parse().map_err(|_| number("--max-order", text))?);
            }
            "--max-position" => {
                let text = value()?;
                limits.max_position_notional =
                    Some(text.parse().map_err(|_| number("--max-position", text))?);
            }
            "--max-daily-loss" => {
                let text = value()?;
                limits.max_daily_loss =
                    Some(text.parse().map_err(|_| number("--max-daily-loss", text))?);
            }
            "--max-orders" => {
                let text = value()?;
                limits.max_orders_per_day =
                    Some(text.parse().map_err(|_| number("--max-orders", text))?);
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
        fast,
        slow,
        interval_secs,
        limits,
    }))
}

/// An endpoint, if it begins with one of `schemes`: a key pasted in its place
/// does not, and is refused without being repeated.
fn url(text: &str, schemes: &[&str]) -> Result<String, BadArgument> {
    if schemes.iter().any(|scheme| text.starts_with(scheme)) {
        Ok(text.to_owned())
    } else {
        Err(BadArgument(format!(
            "an endpoint begins with {}, and that one does not; not repeated here",
            schemes.join(" or ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_args, Args, BadArgument};
    use quant_backtest::live::Environment;
    use std::ffi::OsString;
    use std::path::PathBuf;

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
}
