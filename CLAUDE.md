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
- **M1 in progress**: the Binance recorder. Acceptance criteria are in
  `docs/data-contract.md` §7 and are deliberately harsher than "it connects".
  Sliced into five independently shippable commits:

  | | Slice | Status |
  |---|---|---|
  | a | `quant-storage`: raw frame format, writer, reader | **done** |
  | b | `quant-recorder`: ingress, bounded channel, `Gap{LocalOverflow}`, writer loop | **done** |
  | b2 | `quant-binance`: WS connect, reconnect/backoff, stall detection, `record` bin | **done** |
  | c1 | UTC day rolling: `CaptureSession`, `SegmentStore`, per-segment reports | **done** |
  | c2 | Postgres `capture_sessions` + `capture_segments` | **done** |
  | d | Depth resync + offline verifier binary | next |
  | e | Metrics: msgs/sec, bytes/sec, queue depth, latency pcts, gaps by cause | |

  **It records.** `cargo run -p quant-binance --bin record -- BTCUSDT data 35`
  captured 1485 frames in 35 s at ~7x compression, `ingest_seq` 1..=1485 with
  zero holes, and closed with a trailer. `cargo run -p quant-storage --example
  dump -- <file>` summarises any capture file (frames, holes, gaps, tail,
  trailer) — a debugging aid, not the M1.d verifier.

  Built in this order because the raw tier is the only irreversible artifact in
  the project. The format was specified and tested against synthetic bytes
  before a socket existed, which is what makes "a `SIGKILL` mid-write leaves the
  file readable up to the last complete frame" an ordinary unit test — it cuts a
  capture at every byte offset — rather than an operational anecdote.

- **M1.a decisions** (all reasoned out in `crates/quant-storage/src/lib.rs`):
  compression is per ~256 KiB **block**, so damage stays local and a ratio is
  still achievable; frames are **typed**, because a synthesized `Gap` is not
  venue bytes and a sidecar gap file could disagree with the stream;
  `ingest_seq` is stamped at ingress so **holes are legal and are the evidence**
  of a drop (the writer enforces strictly increasing, not contiguous); a torn
  tail is a **report, not an error**, and is distinguished from corruption; and
  a 32-byte **trailer** lets a file account for itself, so completeness is a
  two-sided check rather than an absence of complaints.

- **M1.b decisions** (reasoned out in `crates/quant-recorder/src/lib.rs`): the
  ingress logic is **venue-agnostic**, so it lives in `quant-recorder`, not in
  the Binance adapter — a second venue inherits the overload behaviour instead
  of reinventing it. Read side and write side are split because framing plus
  zstd is blocking work of unbounded duration, and any time not spent draining
  the socket closes the TCP receive window until the venue disconnects us for
  being slow; a busy disk must not be able to make Binance hang up on us.
  A **full channel drops** rather than blocks — blocking would restore exactly
  that coupling *and* be dishonest, since the data would show no gap and
  `local_recv_ts` would record our stall as venue timing. Sequence numbers are
  consumed even by dropped messages, so **the width of the hole is the count**
  and no count field is needed on disk. Consecutive drops **coalesce** into one
  pending gap record, because one-record-per-lost-message would contend for the
  very capacity we just ran out of. Gap records are **never dropped**: they are
  held and retried, which terminates because every situation that generates one
  is a situation where inflow has stopped. Channel is
  `std::sync::mpsc::sync_channel`, chosen for exactly the two operations the
  design needs — non-blocking `try_send`, and `recv_timeout` so a quiet symbol
  still gets its block sealed on a timer. `RawWriter::finish` now returns
  `(sink, WriterStats)` so the trailer's own bytes are counted.

- **M1.b2 decisions**: one WS connection **per symbol**, not a combined stream —
  routing a combined stream means a JSON parse per message on the hot path just
  to find the symbol, and per-symbol sockets make the socket *be* the route,
  isolate failure, and keep `ingest_seq` naturally per-instrument. Revisit past
  ~50 symbols where connection rate limits bite; the change is contained in
  `quant-binance`. An **idle timeout** (120 s) exists because the failure that
  ruins a 7-day run is not a socket that closes but one that stays open and
  stops delivering; it must exceed the venue ping interval, since a spurious
  reconnect punches a real hole in good data while a slow-detected stall only
  delays noticing. Backoff jitter is **seeded per symbol** so a venue restart
  does not make every connection retry in lockstep. rustls' crypto provider is
  **installed explicitly** — its feature-based auto-detection panics at the
  first TLS handshake, i.e. in production, looking like a venue outage.
  `@trade` not `@aggTrade` and `depth@100ms` not `@depth`: detail the venue
  never sent cannot be recovered from raw capture later.

- **Bug found by running it** (worth remembering): the first flush policy sealed
  a block after N seconds of *silence*. A steady trickle never goes silent, and
  at ~4 KB/s never reaches the 256 KiB block threshold either — so the recorder
  connected, received data, and wrote nothing but a 68-byte header. Fixed by
  bounding **block age** instead of idle time. Regression test:
  `a_steady_trickle_is_sealed_even_though_it_never_goes_idle`. The general
  lesson: a timeout on the *wait* is not a timeout on the *work*.

- **Still M1's fiddly part**: depth-stream resync (buffer deltas → REST snapshot
  → discard stale deltas → verify the update-id chain joins → resume), correct
  on every reconnect, unattended, at 3am.

- **M1.c1 decisions** (reasoned out in `crates/quant-recorder/src/segment.rs`):
  day rolling is driven by the **record's `local_recv_ts`**, not the writer's
  clock — the writer runs behind by design, so rolling on its clock would file a
  23:59:59.9 message under the next day whenever our disk happened to be busy,
  making the partition a property of our load rather than of the data. The
  exception is an **idle stream**: timestamp-driven rolling alone leaves
  yesterday's file unsealed until the next message, and a trailerless file reads
  as "killed", so a healthy quiet symbol would look crashed —
  `roll_if_day_elapsed` closes it on the flush tick. It only *closes*; the next
  record opens the next segment, so a day with no data leaves **no empty file**.
  We never roll **backwards**: a record stamped before the open segment's day
  (an NTP step) is written to the open segment and counted as `backdated_records`
  rather than reopening a sealed day. `ingest_seq` spans the **session**, not the
  file, so a hole straddling midnight is still a hole — which means verification
  joins a session's files in order. `SegmentStore` is a trait so rolling is
  testable without waiting for midnight (`MemoryStore`); `FileStore` fsyncs once
  per sealed segment.

- **M1.c2 decisions** (reasoned out in `crates/quant-meta/src/lib.rs`): the
  metadata tier is **best-effort and optional**. No `QUANT_DATABASE_URL`, or a
  database that will not answer, logs a warning and records anyway — market data
  is irreplaceable and an index row is not, so `run_metadata` returns stats
  rather than a `Result` and nothing in it can fail the recorder. The paired
  obligation: everything stored must be **reconstructible from raw**, or Postgres
  quietly becomes the source of truth for it. Segment rows are reported over a
  bounded `tokio::sync::mpsc` with `try_send` from the writer thread, *never*
  inline — a hung DB connection would otherwise stall block sealing, back up the
  capture channel and drop ticks, letting a secondary concern damage the primary
  one. `quant-recorder` stays DB-free via `ObservedStore`, a plain on-seal
  callback, so the arrows only point down. `(session_id, capture_date, part)` is
  the segment PK because that triple *is* a file's identity in §5, which makes a
  retry an upsert rather than a duplicate. Migrations are versioned in
  `schema_migrations` (not `CREATE TABLE IF NOT EXISTS`) and hold a
  **`pg_advisory_lock`** — concurrent starters otherwise race between "which
  versions are applied" and "apply this one", which is real for two recorders
  launched together and showed up first as flaky tests. Timestamps are
  `timestamptz` (microseconds) and documented as **operational only**; the
  nanosecond `Ts` values never leave the raw tier.

- **Two bugs found by running it** (worth remembering): the recorder hung at
  shutdown because the observer closure inside the store held a metadata sender
  clone, so `recv()` never saw the channel close — all data written correctly, but
  the process never exited. Dropping `store` before awaiting the task fixes it.
  General lesson: with channel-driven shutdown, enumerate *every* holder of a
  sender. And concurrent `migrate()` calls raced, per above.

- **Known follow-up for M1.e**: `std::sync::mpsc` exposes no queue length, so
  "queue depth" from §7 needs an `AtomicUsize` incremented on send and
  decremented on receive. Deliberately deferred, not forgotten.

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
- Windows Smart App Control was **disabled** by the user on 2026-07-29 and the
  build is clean without it. While it was enforcing it intermittently blocked
  freshly downloaded crates' build scripts and proc-macro DLLs with
  `An Application Control policy has blocked this file. (os error 4551)`. If
  that error ever reappears: it clears on retry, so re-run the build rather than
  switching dependencies, and keep the target dir inside the repo — one under
  `AppData\Local\Temp` was blocked far more aggressively. SAC cannot be
  re-enabled without a Windows reinstall, so this should not recur.
- Rust 1.97, edition 2021, stable channel (pinned in `rust-toolchain.toml`).
