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
  quant-recorder/   ingress stamping, sequencing, bounded-channel
                    overload policy, writer loop. Venue-agnostic.  [M1b]
  quant-binance/    the Binance WebSocket dialect, the REST snapshot
                    fetch, plus the `record` binary. The only crate
                    that knows a venue.                            [M1b]
  quant-meta/       Postgres: capture sessions and segments. Sits
                    above the recorder; optional and best-effort.   [M1c]
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

### Recording

```bash
# Record BTCUSDT until ctrl-c, or for a fixed number of seconds.
cargo run -p quant-binance --bin record -- BTCUSDT data
cargo run -p quant-binance --bin record -- BTCUSDT data 60

# Inspect what landed: frames, sequence holes, gap records, tail, trailer.
cargo run -p quant-storage --example dump -- \
  data/raw/exchange=binance/symbol=BTCUSDT/date=*/session=*/part-00000.bin.zst
```

A clean shutdown writes a trailer, so the file states its own frame and block
counts and the reader cross-checks them. A `SIGKILL` leaves no trailer, which is
how "complete" and "interrupted" stay distinguishable from the bytes alone.

Capture files roll at the UTC day boundary, driven by each record's own receive
timestamp rather than the writer's clock — so which partition a message lands in
is a property of the data, not of how busy the disk was.

A book snapshot is fetched over REST on every connect and hourly thereafter, and
written into the same file and the same sequence as the deltas. This is the one
thing the recorder cannot defer to the normalizer: Binance's depth stream is
incremental, `GET /api/v3/depth` serves only the book's *current* state, and a
snapshot not taken at a reconnect can never be taken. The fetch runs concurrently
with draining the socket, so the snapshot's `ingest_seq` lands between the deltas
it arrived between — which is what tells a book builder which deltas precede the
anchor and are stale. Nothing is discarded at capture time; that is M2's job,
where a mistake costs a re-derive rather than a re-record.

### Metadata (optional)

Set `QUANT_DATABASE_URL` and the recorder indexes itself into Postgres: one
`capture_sessions` row per run, one `capture_segments` row per sealed file.

```bash
docker compose up -d
export QUANT_DATABASE_URL=postgres://quant:quant_local_dev@localhost:5432/quant
cargo run -p quant-binance --bin record -- BTCUSDT data 60
```

Unset, or a database that will not answer, logs a warning and records anyway.
Market data cannot be regenerated and an index row can, so the index is allowed
to be missing and the capture is not allowed to stop. Everything in those tables
is reconstructible from the capture files themselves — otherwise Postgres would
quietly have become the source of truth for it.

The `quant-meta` tests need a database and skip themselves without one. CI always
provides it, which is the only reason skipping is acceptable.
