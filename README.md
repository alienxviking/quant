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
  quant-verify/     the offline verifier: turns the acceptance
                    criteria into a command with an exit code.
                    Top of the dependency graph.                   [M1d]
  quant-book/       order book reconstruction and its invariants.
                    Venue-agnostic; depends only on quant-core.    [M2b]
  quant-normalize/  a capture session back out as ordered events:
                    segments joined, book driven, and the Parquet
                    tier written and read back. `normalize` bin.  [M2c/d]
  quant-engine/     the seam: the loop, Strategy, RiskLayer,
                    ExecutionVenue, and the portfolio. No venue,
                    no file format, no network.                   [M3b]
  quant-sim/        the simulated counterparty. Every backtest
                    modelling assumption lives here.              [M3c]
  quant-backtest/   the wiring: a naive strategy, an equity curve,
                    and the `backtest` and `reconcile` binaries.  [M3d/M5c]
docs/
  data-contract.md    the on-disk format and its acceptance criteria
  engine-contract.md  the seam a strategy sees, and why it is shaped
                      that way. Written before the engine exists.
  acceptance-run.md   how the 7-day M1 run is conducted and judged
ops/
  preflight.ps1     refuse to waste a week: clock, disk, build, clean root
  start-run.ps1     start the acceptance run; supervisors + verify loop
  status.ps1        "how is it going" in one command
  stop-run.ps1      stop it in a way that still seals the files
```

## Milestones

| # | Milestone | Done when |
|---|---|---|
| M0 | Foundation + data contract | CI green; contract written before the recorder exists |
| M1 | Binance market data recorder | Runs 7 days unattended; zero unexplained gaps |

**Where this is:** M0 and M1 done. M1's acceptance run — the one criterion only
time can satisfy — was spent: seven days unattended on an Apple Silicon Mac,
2026-08-21 → 2026-08-28, `quant-verify` exit 0 with every discontinuity explained.
See `docs/acceptance-run.md` for the procedure, and "The acceptance run, and how it
went" in `CLAUDE.md` for the result.

**M2 is complete.** Both weeks of the acceptance capture replay as two joined 8-day
sessions with book invariants holding at all 70.5M ticks, **zero** deltas dropped
for want of an anchor, and zero chain breaks — where replaying one file at a time
had dropped 7,000 to 34,000 deltas on each day after the first. The normalized
Parquet tier is written (2.1 GB from 3.0 GB of raw), and **70,545,345 events
replayed from Parquet match the raw replay event for event**, which is what turns
"Normalized is disposable, rebuilt from raw" from a claim in a table into a fact.

**M3 is complete**, and its criterion is unusual on purpose: an equity curve is
produced, *and it is unimpressive*. A moving-average crossover over the acceptance
week takes $100 to **$97.53** — a 2.47% loss with no fees modelled at all. That is
the result to want. A crossover that looked profitable on its first run would mean
the harness was lying, and every way it could lie (dispatching on the wrong
timestamp, filling at prices the book never showed, trading through a gap) is
invisible in the output. See `docs/engine-contract.md`.

**M4 is complete**, and it answers the question the project was built to ask
honestly. At Binance spot's published 10 bps a side, the same week takes $100 to
**$64.74** — the strategy loses 2.5% on price and **33% on commission**. 418 fills
on a $76 position is 43% of position value in fees in a week, so gross returns
would have to beat that to break even. That is a real result: this strategy class,
at this turnover, at retail fees, cannot work. Learning it from recorded data cost
nothing.

One finding worth repeating: **latency is a variance, not a cost.** At 50 ms the
result got slightly *better*, because latency moves the fill to a later book and
over a 60-second horizon the sign of that move is a coin flip. So the tests assert
that latency changes something, and deliberately not which direction.

| M2 | Normalizer + book reconstruction | Book invariants hold at every tick of a replayed day |
| M3 | Engine seam + SimulatedVenue + MA crossover | Equity curve produced, and it is unimpressive |
| M4 | Fee, slippage and latency modelling | Results degrade sensibly under realistic costs |
| M5 | Paper trading | 2 weeks live; paper P&L matches a backtest over the same window |
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

While recording, one metrics line per minute says whether it is healthy:
messages and bytes per second, queue depth and its high-water mark against
capacity, messages dropped in the interval, venue-latency percentiles, and gap
counts by cause. Queue depth is the one that matters most — without it, a stalled
consumer and a quiet market look identical from outside.

Venue latency is `local_recv_ts - exchange_ts`, which measures the host clock as
much as the network, so the recorder checks itself against the venue's clock at
startup and warns above a one-second offset.

### Reconstructing a book

```bash
# parse every frame in a capture file; reports anything that will not parse
cargo run --release -p quant-binance --example parse_all -- <path to part-*.bin.zst>

# replay a capture file through a book and check the invariants at every tick
cargo run --release -p quant-binance --example replay -- <path to part-*.bin.zst>
```

The normalized tier holds **events, not books**: `trades/`, `book_deltas/`,
`book_snapshots/`, `gaps/`. A book is derived at replay time, never stored —
storing one would mean a state per delta for something re-derivable in seconds.

Reconstruction is where the three steps the recorder deliberately deferred live,
because a mistake here costs a re-derive rather than a week of re-recording:
buffer the deltas, discard the ones the snapshot supersedes, and check that the
chain joins. The buffering is not optional — the snapshot arrives *later in the
stream* than the deltas it supersedes, since the recorder fetches it concurrently
with draining the socket.

An invalidated book is **cleared, not flagged**. A flag can be ignored; an empty
book cannot be misread as prices, which is what makes "refuse to trade across a
gap" enforceable rather than advisory.

### Verifying

```bash
cargo run -p quant-verify --bin verify -- data
cargo run -p quant-verify --bin verify -- data --reconcile   # also check the index

# and replay it: is the capture complete, and does it reconstruct?
cargo run --release -p quant-normalize --bin normalize -- data
cargo run --release -p quant-normalize --bin normalize -- data --write   # write the Parquet tier
cargo run --release -p quant-normalize --bin normalize -- data --check   # raw vs Parquet, event by event

# and run a strategy over it
cargo run --release -p quant-backtest --bin backtest -- data --symbol BTCUSDT
cargo run --release -p quant-backtest --bin backtest -- data --equity-csv equity.csv
```

Exit 0 means every discontinuity in every session is explained by a record in the
capture; non-zero means it is not. That is what makes it runnable from cron
*during* a long capture rather than something a person reads afterwards.

Every check has the same shape — find a discontinuity, then ask whether the file
already explains it, and never infer an explanation:

| Discontinuity | Explained by |
|---|---|
| a skipped `ingest_seq` | the frame immediately after the hole is a gap record |
| a break in the venue's depth update-id chain | any gap record between the two deltas |
| depth deltas with no book behind them | a snapshot anywhere in that connection episode, or a recorded `SnapshotFailed` |
| no file trailer | being the last segment, i.e. still open or killed |

The unit is the **session**, not the file: `ingest_seq` spans a session, so a hole
straddling midnight is invisible to a per-file check. Errors fail the run and
warnings do not, because a torn tail on a file still being written is normal and
failing on it would train everyone to ignore the exit code.

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
