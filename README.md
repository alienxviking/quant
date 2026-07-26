# quant

A quantitative trading platform, built as a long-term systems-engineering
project. Crypto is the first venue implementation, not the point of the
project — the engine is meant to outlive any single strategy or market.

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
                    RiskLayer         (mandatory chokepoint — not optional,
                        │              not called politely by the strategy)
                        ▼
                 ┌── SimulatedVenue   (fills, fees, slippage, latency model)
ExecutionVenue ──┼── PaperVenue       (live prices, simulated fills)
                 └── LiveVenue        (real orders)
```

| Mode | Source | Venue |
|---|---|---|
| Backtest | Historical | Simulated |
| Paper | Live | Paper |
| Live | Live | Live |

**A strategy must not be able to tell which pair it is wired to.** If it can,
that is a bug. Divergence between these three paths is how backtests end up
describing a system that was never run.

## Invariants

These are enforced by the compiler and the test suite, not by discipline:

1. **Money is integral.** Fixed-point `i64` scaled by `1e8`.
   `clippy::float_arithmetic` is denied workspace-wide.
2. **Two timestamps on every event**, and only `local_recv_ts` is ever
   dispatched on. Anything else is lookahead bias.
3. **Gaps are recorded events.** "We were not watching" is data, not silence.
4. **Time is injected.** Components take a `&dyn Clock`. Anything that calls
   the wall clock directly cannot be backtested.

## Layout

```
crates/
  quant-core/       types, time, instruments, the event contract  [M0]
  quant-storage/    the raw capture format: framing, blocks, torn-tail
                    recovery. Knows no venue and no network.       [M1a]
docs/
  data-contract.md  the on-disk format and its acceptance criteria
```

## Milestones

| # | Milestone | Done when |
|---|---|---|
| M0 | Foundation + data contract | CI green; contract written before the recorder exists |
| M1 | Binance market data recorder | Runs 7 days unattended; zero unexplained gaps |
| M2 | Normalizer + book reconstruction | Book invariants hold at every tick of a replayed day |
| M3 | Engine seam + SimulatedVenue + MA crossover | Equity curve produced, and it is unimpressive |
| M4 | Fee, slippage and latency modelling | Results degrade sensibly under realistic costs |
| M5 | Paper trading | 2 weeks live; P&L reconciles against an independent recompute |
| M6 | Risk engine + kill switch | Limits provably veto a misbehaving strategy, under test |
| M7 | Observability | "What was it doing at 03:14 last Tuesday?" answered in a minute |
| M8 | Live, tiny capital | Live fills reconcile to the paper model within tolerance |

## Development

```bash
docker compose up -d          # Postgres (metadata only)
cargo test --workspace        # all tests
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```
