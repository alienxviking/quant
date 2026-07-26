# CLAUDE.md

Context for any Claude session working in this repo. Read `README.md` and
`docs/data-contract.md` too — this file is the working agreement; those are
the design.

## What this is

A quantitative trading platform, built as a long-term systems-engineering
project. The stated end goal is a miniature quant trading firm, not a crypto
bot. Crypto (Binance) is the first venue adapter because the infrastructure
barrier is lowest, not because the project is about crypto — the same engine
is meant to later serve stocks, futures and FX.

**The engine must outlive any strategy.** When a trade-off is between "ship
this strategy sooner" and "keep the platform correct", the platform wins.

## Working agreement

The user asked to be mentored through this, not handed code. That means:

- Explain the **why** before the how. Design rationale, trade-offs, and what
  breaks if we do it the other way.
- Keep it production-shaped from the start. No throwaway prototypes that
  quietly become load-bearing.
- Push back on shortcuts that compromise the platform, and say so plainly.
- Milestones have **acceptance criteria**, not feature lists. "Done" means
  the criterion passes.

Calibration: comfortable-ish with Rust (small projects, has fought the borrow
checker) — skip language basics, do explain non-obvious design choices
(tokio task structure, channel/backpressure choices, error strategy).
Budget ~10–15 hrs/week, so milestones are sized at about a week each. Keep
commits independently shippable so a gap never leaves them mid-refactor.

## The one architectural idea

One strategy binary, three worlds, no code changes between them:

```
              ┌── HistoricalSource   (Parquet replay, as fast as possible)
EventSource ──┼── ReplaySource       (raw capture, wall-clock paced)
              └── LiveSource         (venue WebSocket)
                        │
                        ▼
                     Engine ────────►  Strategy
                        │
                        ▼
                    RiskLayer         (mandatory chokepoint)
                        │
                        ▼
                 ┌── SimulatedVenue
ExecutionVenue ──┼── PaperVenue
                 └── LiveVenue
```

Backtest = Historical + Simulated · Paper = Live + Paper · Live = Live + Live.

**A strategy must not be able to tell which pair it is wired to.** If it can,
that is a bug. Three divergent code paths is how a backtest ends up
describing a system that was never run.

Risk sits *between* strategy and venue as a chokepoint every order must pass
through — not a module the strategy politely calls. Designed in at M3, empty
until M6, because retrofitting it means auditing every call site.

## Invariants (enforced, not conventional)

1. **Money is integral.** `Px` / `Qty` / `Notional` are `i64` scaled `1e8`.
   Venue decimal strings parse straight to fixed-point, never via `f64`.
   `clippy::float_arithmetic = "deny"` is set workspace-wide, so a stray
   float fails the build.
2. **Two timestamps on every event.** `exchange_ts` (what the venue said) and
   `local_recv_ts` (when we saw the bytes). The engine orders and dispatches
   on `local_recv_ts` **only**. Dispatching on `exchange_ts` is lookahead
   bias — invisible, and it inflates every metric.
3. **Gaps are recorded events.** `MarketEvent::Gap` distinguishes "the market
   was quiet" from "we were blind". `GapCause::LocalOverflow` must be
   recorded honestly; suppressing it just hides a capacity problem.
4. **Time is injected.** Components take a `&dyn Clock`. Anything calling the
   wall clock directly cannot be backtested — treat it as a bug.
5. **Parse failures are loud.** A malformed price means our model of the venue
   is wrong. Never round, never default to zero.

## Storage tiers

| Tier | Format | Rule |
|---|---|---|
| Raw | venue bytes + recv ts, framed, zstd | **Immutable.** Never parsed. Source of truth. |
| Normalized | Parquet, `exchange/symbol/date` | **Disposable.** Rebuilt from raw on demand. |
| Metadata | Postgres | Instruments, sessions, runs, orders, fills, results. |

Market data does **not** go in Postgres. ClickHouse later just points at the
same Parquet. Full reasoning in `docs/data-contract.md`.

## State

- **M0 complete** (2026-07-26): workspace, `crates/quant-core` (fixed-point
  money, `Ts`/`Clock`, instrument registry with venue filters, the
  `MarketEvent` contract), CI, docker-compose for Postgres, data contract
  written *before* the recorder exists. 19 tests green in debug and release,
  clippy and fmt clean.
- **M1 next**: the Binance market data recorder. Acceptance criteria are in
  `docs/data-contract.md` §7 and are deliberately harsher than "it connects".

Planned M1 shape: a read task that does nothing but stamp `local_recv_ts` and
push bytes into a **bounded** channel, plus a writer task that frames and
compresses. Bounded because an unbounded channel does not prevent overload,
it converts it into an OOM kill an hour later; on a full channel we drop and
emit `Gap{LocalOverflow}` rather than blocking the socket read. The fiddly
part is depth-stream resync (buffer deltas → REST snapshot → discard stale
deltas → verify the update-id chain joins → resume), correct on every
reconnect, unattended, at 3am.

Milestone table: see `README.md`.

## Conventions

- Commits are authored **alienxviking <sroy191006@gmail.com>** with **no**
  `Co-Authored-By: Claude` trailer. Already set in this repo's local git
  config. Personal project under the user's own GitHub identity.
- Commit messages explain *why*, in the body. The rationale is the point.
- Warnings are errors in CI (`RUSTFLAGS: -D warnings`). Run before committing:

  ```bash
  cargo fmt --all
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace --all-features
  ```

- Windows: cargo may not be on `PATH` in shells opened before Rust was
  installed. Prefix with
  `$env:Path = "$env:USERPROFILE\.cargo\bin;$env:Path"` if `cargo` is missing.
- Rust 1.97, edition 2021, stable channel (pinned in `rust-toolchain.toml`).
