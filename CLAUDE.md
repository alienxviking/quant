# CLAUDE.md

Context for any Claude session working in this repo. Read `README.md`,
`docs/data-contract.md` and `docs/engine-contract.md` too — this file is the
working agreement; those are the design. The data contract governs data at rest;
the engine contract governs the seam a strategy sees. `docs/observability.md`
(M7) and `docs/paper-run.md` (M5) cover their own milestones.

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
EventSource ──┼── ReplaySource       (raw capture, wall-clock paced)  [not built]
              └── LiveSource         (venue WebSocket)
                        │
                        ▼
                     Engine ────────►  Strategy
                        │
                        ▼
                    RiskLayer         (mandatory chokepoint)
                        │
                        ▼
                 ┌── SimulatedVenue   (paper uses this one too)
ExecutionVenue ──┴── LiveVenue        (M8)
```

Backtest = Historical + Simulated · Paper = **Live + Simulated** · Live = Live + Live.

`ReplaySource` is marked because it **does not exist** — `HistoricalSource` and
`LiveSource` are the only `impl EventSource` in the workspace. It was in the M0
design as the wall-clock-paced rehearsal path, and M5 turned out not to need it:
paper is `LiveSource`, and a backtest wants Parquet at full speed. Marked rather
than deleted because it is still the obvious way to rehearse against a recorded
day in real time, and unmarked it made the diagram read as a description of what
is implemented — the same defect the project corrected for `PaperVenue`, found
the same way.

M5 established there is no `PaperVenue`: a paper venue fills against a
reconstructed book at prices the book showed, which is exactly `SimulatedVenue`.
What separates backtest from paper is the **source** and the **durability**, not
the matching.

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

**M0–M4, M6 and M7 are complete. M5's fortnight is running** (started
2026-09-18T14:46:54Z, ends 2026-10-02) **and is the one remaining criterion.**
414 tests green in debug and release, clippy and fmt clean, ~30,700 lines across
12 crates.

While it runs: **do not `git pull` or `cargo build` in the run's checkout.**
`supervise.sh` execs `target/release/paper` on every restart, so a rebuild would
silently continue the run on different code — and bash reads a script file
lazily, so editing `ops/*.sh` underneath the running supervisor can make its loop
jump mid-execution. Develop in a separate worktree (`git worktree add`), which is
where M2.e and M7 were built.

- **M0 complete** (2026-07-26): workspace, `crates/quant-core` (fixed-point
  money, `Ts`/`Clock`, instrument registry with venue filters, the
  `MarketEvent` contract), CI, docker-compose for Postgres, data contract
  written *before* the recorder exists. 19 tests green in debug and release,
  clippy and fmt clean.
- **M1 complete** (code 2026-08-11, acceptance run passed 2026-08-28): the Binance
  recorder. Every slice is written, tested and committed; 179 tests green in debug
  and release, clippy and fmt clean. Six of §7's seven criteria were settled by the
  test suite and `quant-verify`; the seventh — **7 consecutive days unattended** —
  has now been spent. See **"The acceptance run, and how it went"** at the bottom of
  this file. That section replaces the old "Picking up the acceptance run" notes,
  which described a run that had not started yet.

  Acceptance criteria are in `docs/data-contract.md` §7 and are deliberately
  harsher than "it connects". Sliced into independently shippable commits:

  | | Slice | Status |
  |---|---|---|
  | a | `quant-storage`: raw frame format, writer, reader | **done** |
  | b | `quant-recorder`: ingress, bounded channel, `Gap{LocalOverflow}`, writer loop | **done** |
  | b2 | `quant-binance`: WS connect, reconnect/backoff, stall detection, `record` bin | **done** |
  | c1 | UTC day rolling: `CaptureSession`, `SegmentStore`, per-segment reports | **done** |
  | c2 | Postgres `capture_sessions` + `capture_segments` | **done** |
  | d1 | Depth snapshot capture (resync + periodic) | **done** |
  | d2 | `quant-verify`: offline verifier binary | **done** |
  | e | Metrics: msgs/sec, bytes/sec, queue depth, latency pcts, gaps by cause | **done** |

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

- **M1.d1 decisions** — and first, a re-scoping. The plan said "depth resync:
  buffer deltas → REST snapshot → discard stale deltas → verify the chain joins →
  resume". That is Binance's *local order book* algorithm, and the recorder has no
  book. Sorted by which milestone can actually falsify each step, only **one** step
  is a capture-time obligation: the **fetch**, because the venue serves only the
  book's current state and a snapshot not taken at a reconnect can never be taken.
  Buffering is what the file already is; discarding stale deltas destroys evidence
  and is a pure function of raw; chain verification is offline. So M1.d split into
  d1 (capture the snapshot) and d2 (the verifier), and the discard/verify steps
  moved to M2 where a mistake costs a re-derive instead of a re-record.

  Snapshots get their own `FrameKind::VenueSnapshot` rather than sharing
  `VenuePayload` — same argument that made gap frames typed. Sniffing would mean a
  try-parse of two JSON shapes on every frame, forever, and a snapshot misread as
  a delta corrupts a book *silently* instead of failing. The payload stays
  **verbatim**: an envelope naming the endpoint would be our bytes labelled as the
  venue's, and `(exchange, symbol)` + kind already determine it for one snapshot
  kind per venue. Container version bumped **1 → 2**, migration note in
  `docs/data-contract.md` §6 — a v1 reader would otherwise get most of the way
  through a good v2 file and report `UnknownFrameKind`, indistinguishable from
  corruption; refusing at the header says what is actually wrong.

  The fetch is **concurrent with the drain**, not before it. Awaiting it first
  would stop reading the socket for a round trip, restoring exactly the coupling
  the read/write split exists to remove; fetching before the subscription is live
  would leave an unbridgeable hole between snapshot and first delta. Concurrency
  also buys the property that matters for free: **the snapshot's position in the
  sequence is the information** — it lands between the deltas it arrived between,
  which is what tells the book builder which deltas are stale. Verified live:
  anchors at `ingest_seq` 4, 371, 562, 1024. It is a plain boxed future, not a
  spawned task, so dropping the connection cancels it — a task could outlive its
  connection and deliver a snapshot into the *next* one, where its position would
  be a lie.

  A **dropped snapshot consumes no sequence number**, unlike a dropped message.
  A hole means "the venue sent something and we lost it" and the verifier reads it
  that way; a snapshot we fetched ourselves lost no stream data, so a hole would
  manufacture evidence of a drop *and* leave it unexplained. And the remedy for a
  full channel is to **re-fetch, not to retry the bytes** — a gap record must keep
  its original timestamp, but a snapshot's whole value is being current, so a fresh
  fetch is a strictly better record and holding a megabyte is the last thing to do
  when already behind.

  **Periodic snapshots (hourly) are not an optimization.** Without them one lost
  delta invalidates the book for the rest of the session, permanently; with an
  hourly anchor it re-synchronizes at the next one. Same "keep damage local"
  reasoning as per-block compression, and `BookSnapshot`'s M0 doc comment already
  anticipated it. Cost is ~320 KB/hour against several hundred MB/day of deltas.

  `SnapshotFailed{purpose, reason, attempts}` is recorded rather than left as a
  silence, because an absent snapshot frame cannot otherwise be told from a build
  that never fetched one. It is **not** a `Gap`: no messages were lost, and only a
  failed *resync* is serious — treating a failed periodic snapshot as a gap would
  make "refuse to trade across a gap" reject good data. `reason` is a closed set of
  coarse categories, not the venue's error text, which keeps `ControlRecord` `Copy`
  and keeps unbounded remote strings out of the immutable tier.

- **Two things running it taught us** (worth remembering): `reqwest` **panics**
  inside `Client::build` when no process-global rustls provider is installed — not
  at first handshake, so merely constructing a client in a unit test brought the
  test down. Hence `install_crypto_provider()`, called from both startup and
  `SnapshotClient::new`; a library constructor that panics unless its caller knew
  to install a cryptography backend first is a worse trade than a global side
  effect. Use the `rustls-tls-webpki-roots-no-provider` feature, or reqwest pulls
  `aws-lc-rs` (cmake + nasm on Windows) and fights the `ring` provider we install.
  And `gaps_recorded` initially counted snapshot-failure records too, so a run with
  one gap logged `gaps=2` — misleading in exactly the log line an operator reads
  first. Split into `gaps_recorded` and `snapshot_failures`.

- **M1.d2 decisions** (`quant-verify`, reasoned out in its `lib.rs`). One design
  idea: **find a discontinuity, then ask whether the capture already explains it**,
  and never infer an explanation. Every check has that shape, and the explanations
  are exactly the records M1 was built to write. Exit code, not prose, so it runs
  from cron *during* the 7-day run and from CI.

  The unit of verification is the **session**, not the file — `ingest_seq` is
  session-scoped, so a hole straddling midnight is invisible per-file because each
  file is internally contiguous. Segments are joined by **sorting their paths as
  text**, which is chronological only because the layout uses ISO dates and
  zero-padded parts; `CaptureTarget::parse` is the inverse of `::file` and lives
  beside it so the two cannot drift. Both the path and the header are read, and a
  disagreement is an error: a capture filed under the wrong instrument is worse than
  a missing one, because every downstream tool reads the directory.

  Findings are **capped at 5 per (code, session)** with the remainder counted. One
  systematic defect over hundreds of millions of frames would otherwise bury every
  other finding — the failure mode where a verifier is worse than none. The count is
  never dropped, because "and 3,201,884 more" is what says systematic rather than
  one-off. Warnings **do not fail the run**: a torn tail on a file still being
  written is normal, and failing on it teaches everyone to ignore the exit code.

  **No venue-abstraction trait, deliberately** — against this project's usual
  instinct, and worth re-reading before adding one. Three of the four checks need
  only the container format; the update-id chain needs Binance knowledge, so it went
  in `quant-binance::sequence` and `quant-verify` calls it. A trait designed from
  one implementation encodes one venue's assumptions and calls them universal;
  extracting it from two real ones later gives a better trait. A capture from an
  unknown venue is **reported as unchecked**, which is the part that matters.
  `sequence.rs` is also the first payload parsing in the project: strict about the
  four fields it claims to understand, tolerant of everything else, so a new Binance
  field cannot make a recorded file unverifiable.

  `--reconcile` is a flag, not the default, because §1 makes raw the source of truth
  and `FileTrailer`'s own docs commit to it. It compares frame counts, which two
  independent writers claim — the file's trailer and the `capture_segments` row —
  and would catch the metadata tier silently dropping reports under load, which its
  `try_send` design makes possible on purpose. It does *not* audit the index for
  rows whose files are missing: on any machine that has run the `quant-meta`
  integration tests that would report every fixture, and a tool that is noisy on a
  dev box gets ignored on a production one.

- **The verifier's first act was to find a bug in itself** (the most useful thing
  that happened this milestone): 296 errors against captures known to be good. The
  rule "every delta must be preceded by a snapshot" is **wrong**. The snapshot is
  fetched concurrently with the drain, so a handful of deltas always arrive before
  it — and Binance's algorithm discards deltas with `u <= lastUpdateId` and bridges
  the straddler, so a snapshot anchors the deltas *around* it, not only after it.
  The correct unit is the **episode**: one connection's worth of stream, bounded by
  the gaps that begin and end it. An episode with deltas needs an anchor somewhere
  inside it; an episode with none needs nothing — which is also the live edge case
  where a connection dies before delivering anything. Regression test:
  `deltas_before_the_snapshot_are_normal_and_not_a_defect`. General lesson: a
  verifier that cries wolf on good data is worse than no verifier, and the first
  thing to check when it fires is the rule, not the data.

  Two smaller things it taught: `deltas_before_anchor` is now a reported *total*
  rather than a finding, because it is the difference between "recorded" and
  "reconstructible" and a rising number means snapshots are landing late. And the
  "NOTHING VERIFIED" verdict had to be ordered *after* the error verdict — a file
  corrupt in its first block reads zero frames, and saying "no files found" about it
  sends whoever is on call to the wrong place.

- **Known follow-up the verifier surfaced**: the recorder does not re-snapshot after
  a `LocalOverflow`. Dropping messages invalidates the book exactly as a disconnect
  does, but only reconnects trigger a resync, so the book stays unusable until the
  next hourly anchor. Reported as `unanchored-after-overflow` at **warning** level,
  because the capture is telling the whole truth — there is a gap record and a hole
  of exactly the right width. Not fixed in d1 on purpose: under overload, fetching
  320 KB is the worst possible moment to add work and the channel that is full would
  drop the snapshot too, so a correct fix has to trigger *after* recovery. Worth
  doing if `dropped` is ever non-zero in practice; it has been 0 in every run.

- `examples/dump.rs` remains a single-file debugging aid, not the verifier.

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

- **M1.e decisions** (`quant-recorder/src/metrics.rs`). Counters are **atomic not
  because of contention** — ingress is the only writer of most — but because they
  must be *read* by a reporter while the connection loop holds `Ingress` borrowed
  forever. Queue depth is the one genuinely cross-thread counter, which is what
  M1.c2's note anticipated. `IngressStats` survives as a **snapshot type** built
  from the atomics, so quant-meta and the shutdown summary were untouched.

  `MetricsReporter` is a **plain struct, not a task or a thread**: rate computation
  is the only logic, and keeping it a pure function of two samples makes it
  testable without waiting for wall-clock seconds. The *when* stays in the binary,
  which already has a runtime — so `quant-recorder` is still async-free, as its
  crate docs promise.

  Latency percentiles need `exchange_ts`, which means **parsing on the read path** —
  the very cost that argued against combined streams. The distinction is
  consequence, not cost: routing needed the parse to decide *where bytes go*, so a
  failure damaged the capture; here a failure costs one data point. Every message
  is parsed rather than sampled, because at ~100 msg/s a microsecond parse is a
  rounding error — and the signal that this stopped being true is queue depth,
  which is one of the metrics being added.

  Ordering is load-bearing: §2 requires `local_recv_ts` stamped **before parsing**,
  so the adapter reads the clock, hands the stamp to the new `Ingress::accept_at`,
  and parses afterwards. Parsing first would fold our own parse time into the one
  timestamp everything dispatches on.

  The histogram is hand-rolled (16 sub-buckets per octave, ~6% error, pinned by a
  test) rather than `hdrhistogram`: it is forty lines and a metric that will drive
  an alert is worth understanding exactly. Reported values are each bucket's
  **upper** bound, so a percentile never understates. Window histogram resets per
  report (a degradation on day five must not be averaged away); a second lifetime
  histogram gives the whole-run number.

- **A real race, found by an existing test** (worth remembering): `CaptureSender`
  counted a record into the queue *after* handing it to the channel. The writer
  thread could receive and decrement first, so an unsigned depth went below zero,
  wrapped to `usize::MAX`, and panicked on the next increment — killing the
  recorder. Found by `a_steady_trickle_is_sealed_even_though_it_never_goes_idle`,
  which is multi-threaded, within minutes. Fix: count **before** the send and undo
  on failure, plus saturating arithmetic, because *a metric must never be able to
  take down a capture*. Regression test:
  `a_pop_that_beats_its_push_cannot_wrap_the_counter`.

- **The latency metric's first act was to find a broken clock** — this machine's is
  ~2 s **ahead** of Binance, so p50 "venue latency" read 2883 ms. The metric was
  right; the host was wrong. Hence a startup check against `/api/v3/time` warning
  above **1000 ms**, which is Binance's own tolerance for a signed request rather
  than a number invented here — so the clock this warns about is the same clock
  that would get an order rejected at M8. Not fatal: a wrong clock shifts every
  `local_recv_ts` equally, so ordering and dispatch are unaffected and only latency
  and cross-venue comparison break; refusing to start would cost data over a metric.
  **Before the 7-day run, sync the host clock.**

- **The acceptance run harness** (`ops/*.ps1`, procedure in
  `docs/acceptance-run.md`). Restarting is **outside** the recorder on purpose: a
  process that has decided it is in a fatal state should die, and the format was
  built for it — a new session id, its own files, and a `Gap{RecorderRestart}` in
  the first frame, which is exactly the record the verifier reads as the
  explanation. On a server `supervise.ps1` is `Restart=always` in a systemd unit.
  Verification runs **during** the capture every 6h, which is what the exit code
  was for. `preflight.ps1` refuses to start on a bad clock, a dirty capture root,
  or thin disk, and `-Force` writes that override into `run.json` so a forced run
  cannot be mistaken for a clean one later. `-Minutes` is a rehearsal mode,
  because a run harness that has never been run is not a harness.

- **PowerShell 5.1 traps that cost real time** (worth remembering before writing
  more ops scripts): `$PSScriptRoot` is **empty while `param()` defaults are being
  bound**, so a default computed from it silently becomes an empty string —
  resolve paths in the body. Redirecting a native command's stderr (`2>&1`, even
  `2>$null`) wraps every line in an `ErrorRecord`, which under
  `ErrorActionPreference = 'Stop'` turns cargo's ordinary progress output into a
  terminating error; `Start-Process -Wait` avoids that but **hung indefinitely**
  after cargo had already exited, so the working answer is `cmd /c "... > log
  2>&1"`, which keeps the native streams away from PowerShell entirely. `if` is a
  statement and cannot be a hashtable value. `Start-Process -ArgumentList` joins an
  array with spaces and quotes nothing, so every path argument needs its own
  quotes.

- **A metrics bug the rehearsal found**: a live line read
  `latency_p99_ms=3145 latency_max_ms=3071`. Both were "correct" — percentiles
  report a bucket's *upper* bound, which can exceed every value in it — and the
  pair is nonsense to anyone reading it. Percentiles are now clamped to the
  observed maximum, which keeps the never-understate guarantee (a percentile is
  always ≤ the max) and loses nothing. Test:
  `no_percentile_can_exceed_the_observed_maximum`. The general lesson is the same
  one as the verifier's false positives: a number that looks broken *is* broken,
  whatever its derivation says.

- **M2 complete** (2026-08-31 → 2026-09-02): the normalizer and book reconstruction.
  237 tests green in debug and release, clippy and fmt clean. Criterion — **book
  invariants hold at every tick of a replayed day** — met, along with all four
  checkboxes in `docs/data-contract.md` §8.

  | | Slice | Status |
  |---|---|---|
  | a | `quant-binance::parse`: payloads → `MarketEvent`, full fixed-point | **done** |
  | b | `quant-book`: apply, invariants, gap invalidation, resync | **done** |
  | c | `quant-normalize`: join a session's segments, replay, report | **done** |
  | d | Parquet output: the normalized tier on disk | **done** |

  **The numbers on the acceptance capture**, both symbols, both weeks: 8 segments
  joined per session, 70,545,346 frames read, **0** deltas dropped for want of an
  anchor, 0 chain breaks, 0 archive breaks, invariants holding at all 70.5M ticks.
  2.1 GB of Parquet written from 3.0 GB of raw in 2m27s, and **70,545,345 events
  replayed from Parquet match the raw replay event for event** (4m03s). The one
  frame difference is the day-3 `SnapshotFailed` control record, which is
  correctly not a market event.

  **The scoping call, made first.** The milestone reads "normalizer *and* book
  reconstruction", but those are two artifacts. The normalized tier holds
  **events, not books** — the contract's layout is `trades/`, `book_deltas/`,
  `book_snapshots/`, `gaps/`. Storing books would mean a state per delta (11.6M of
  them, thousands of levels each) for something re-derivable in seconds. So the
  criterion is a property of the *reconstruction*, validated by replaying, and not
  an output. Same class of re-scope as M1.d.

- **M2.a decisions** (`quant-binance/src/parse.rs`). Written against payloads
  copied **verbatim out of `data/acceptance`**, not from the venue's documentation
  — the dialect actually being spoken is the one worth being correct about. That is
  why `dump` grew `--sample N`, which prints whole payloads per frame kind.

  The **aggressor mapping inverts the venue's flag**. Binance sends `m`, "is the
  buyer the market maker", so `m: true` means a resting buyer was lifted and the
  *seller* crossed → `Side::Sell`. Backwards, this silently inverts order-flow
  imbalance, and a signal with the wrong sign looks *predictive* rather than
  broken. Pinned in both directions.

  `exchange_ts` comes from **`E`, not a trade's `T`**, for both event types. `E` is
  when the venue emitted the message, so `local_recv_ts - exchange_ts` stays a
  transport measurement; `T` would fold Binance's own match-to-publish delay into
  what we report as network latency. `T` remains in raw for anyone who needs it.
  A REST snapshot has **no venue timestamp at all**, so `exchange_ts` is our
  receive time — the same choice `ControlRecord::Gap` makes, for the same reason.

  There are now **two parsers for one dialect**, which is a real drift risk and is
  bounded rather than ignored. `sequence.rs` reads four fields for the verifier
  walking 70M frames; allocating a `Vec<Level>` per delta to learn two integers
  would be absurd. `both_parsers_agree_on_what_a_message_is` pins that they
  classify the same bytes the same way, so a divergence fails a test instead of a
  book quietly reconstructing from the wrong messages.

  Proven on the **corpus**, not the fixtures: a `parse_all` example ran every frame
  of all 16 segments — 58.9M trades, 11.6M deltas, 342 snapshots, **384M price
  levels through fixed-point, zero failures, zero unrecognised event types**. A day
  of BTCUSDT parses in 5 s.

- **M2.b decisions** (`crates/quant-book/src/lib.rs`). Venue-agnostic, and it *can*
  be: `BookDelta` carries the update id as a **range**, and Binance's rule is a
  statement about that range, so a venue with a single monotonic sequence sets both
  ends equal and it still holds. Generalising from one implementation is against
  this project's usual instinct; it earns its place because **M0 put the range in
  the event contract before any of this existed**.

  An invalid book is **cleared, not flagged**. A flag can be ignored; an empty book
  cannot be misread as prices. That is what makes "refuse to trade across a gap"
  enforceable rather than advisory — the same reasoning that puts the risk layer
  between strategy and venue rather than beside it.

  `check()` is **O(1)** (the crossed-book test) and runs every tick, which is the
  criterion. `audit()` is O(n) and runs at each snapshot and at the end; per-tick it
  would be tens of billions of comparisons for properties that hold by construction.

- **The replay proved my own module docs wrong** (the most useful thing in M2 so
  far). I had written "buffer the deltas — already done, the capture file *is* the
  buffer". True about the file, **false about the algorithm**: the snapshot arrives
  *later in the stream* than the deltas it supersedes, because the recorder fetches
  it concurrently with the drain, so a few hundred ms of messages land while the
  REST request is in flight. A replay that applied the snapshot and continued from
  the next frame gave **411,422 unanchored, 0 applied, 12 chain breaks** on a file
  the verifier had passed clean.

  So the book buffers while unanchored and replays on anchor — and **the buffering
  lives in the book, not the caller**, because a caller who must remember will
  forget. `MAX_PENDING_DELTAS` bounds it (a snapshot may never arrive;
  `SnapshotFailed` is a recorded outcome) and drops the **oldest**, since
  `lastUpdateId` lands near the recent end.

  Fixing it surfaced a second case the same argument covers: **a snapshot the book
  has already passed is ignored**, not applied. The recorder takes an hourly anchor
  whether the book needs one or not, so by the time one is written the live stream
  is further ahead than the `lastUpdateId` the venue served; applying it would move
  the book backwards. **11 of the 12 snapshots in a day of BTCUSDT are this case** —
  which is also why periodic anchors are not wasted: their value is for the book
  that *has* been invalidated.

  After the fix, all 16 segments replay with **0 chain breaks, 0 invalidations,
  invariants holding at every tick** (3.97M live-book ticks on day one alone). Two
  independently written checks agreeing: the verifier says the chain is
  contiguous-or-explained, and the book produces zero `Broken` outcomes.

- **A verifier sentence the book disproved**: `deltas_before_anchor` was described
  as *"recorded, but not reconstructible"*. They are in fact either superseded by
  the snapshot or replayed after it, and **both halves are needed**. Reworded in the
  report line and in `session.rs`.

- **Known artifact, now removed**: replaying *single files* dropped 7k–34k deltas on
  days 2–8, because each file starts mid-stream with no anchor and waits up to an
  hour for the next hourly snapshot. Day 1 dropped none — its resync arrives 300 ms
  in. M2.c joined the segments and drove it to **0** on both symbols, which is the
  slice's own acceptance evidence.

- **M2.c decisions** (`crates/quant-normalize/`, 2026-09-02). The slice's whole
  content is that **a UTC day boundary is a filing decision**: `ingest_seq` and the
  venue's update-id chain both span a *session*, so crossing into the next day's
  file must not reset anything. It works — both weeks, both symbols: **0 deltas
  dropped for want of an anchor** (against 7k–34k per day when replayed one file at
  a time), 0 chain breaks, 0 archive breaks, invariants holding at all 70.5M ticks.

  **The replay is an iterator of events, not a program that checks a book.** The
  book is the consumer that happens to exist first; M2.d's Parquet writer and M3's
  `HistoricalSource`/`ReplaySource` want the identical stream. Written the other way
  round, both would have to take it apart again to get at the events. `Book` driving
  lives in `replay_session`, which is one consumer and not the point.

  **The archive's own discontinuities are items in the stream, and they are `Break`s
  rather than synthesized `Gap`s.** A segment that will not open, a torn tail, a hole
  in `ingest_seq` — each means what follows does not continue what came before, and
  skipping quietly to the next readable frame would produce a book that *looks*
  continuous across data we never read. But `GapCause` is persisted in the immutable
  tier and describes the venue or the recorder; "I could not read this file just now"
  describes one replay on one machine, and a fifth cause for it would let a re-derive
  on a failing disk produce different bytes than one on a healthy disk. Downstream
  they mean the same thing — invalidate — and that obligation is discharged **inside**
  `replay_session`, on the M2.b principle that a caller who must remember will forget.

  **A hole breaks immediately, without waiting for a gap record to explain it.** The
  recorder writes that record *after* the hole it describes (the hole is the evidence,
  the record is the account), so waiting would mean handing out events across a known
  discontinuity in the hope of being forgiven. Invalidating twice costs nothing.

  **Session discovery moved down into `quant-recorder::catalog`** — the open question
  from the M2 handoff, now closed. `quant-verify` had owned it since M1.d2 and nothing
  may depend on `quant-verify`. Copying the fifteen lines was the easy call and the
  wrong one: if the two ever disagreed about which files form a session or what order
  they go in, the normalizer would replay a *different stream* than the one the
  verifier passed clean, and nothing downstream could notice. What did **not** move is
  the policy — `quant-recorder` has no idea what a finding is, so `catalog()` returns
  what it found, what did not fit the layout and what it could not read, and each
  caller decides. They genuinely differ: a stray file is a warning to the verifier and
  an unreadable segment invalidates the normalizer's book.

  **`quant-verify` and `quant-normalize` must never depend on each other**, in either
  direction. The verifier asks *is this capture complete*; the normalizer asks *what
  did the market do*. A capture with an honest recorded gap is complete and still goes
  dark for a while, so a tool that conflated them would have to call one a failure.
  That they now independently agree on **70,545,346 frames** while sharing no counting
  code is what makes the number worth anything — and BTCUSDT's single `ignored` frame
  is the day-3 `SnapshotFailed` record, the same one thing the verifier warns about.

  **`quant-binance/examples/replay.rs` was deleted**, not kept as a convenience. It
  did its job at M2.b (it is what found the buffering bug), but leaving it would mean
  two implementations of "apply recorded events to a book, check every tick", and the
  one nobody maintains is the one that quietly stops agreeing. `parse_all` stays: it
  holds no book logic, so there is nothing for it to drift against.

- **M2.d decisions** (`crates/quant-normalize/src/tier/`, 2026-09-02). Four that
  are hard to undo once a week of data is written in them.

  **Money on disk is `DECIMAL(18,8)`, not `INT64`.** Identical bytes — Parquet backs
  a decimal of precision ≤ 18 with an `INT64`, which a test asserts rather than
  trusting — but the decimal carries **the scale in the schema**. Invariant 1 says
  money never goes through `f64`, and until now that held only inside our own
  process; a bare `INT64` makes every reader responsible for knowing the `1e8`
  convention out of band, and the first that reads it as a double loses precision
  silently on large notionals. `Decimal64` not `Decimal128`: same on disk, `i64` in
  memory. The bound is **enforced, not assumed** — precision 18 holds values below
  `10^10` where `i64` holds nine times that, so a value past it is a loud error on
  write, per invariant 5. Both sides of the bound are tested. Timestamps are
  `TIMESTAMP(NANOS, UTC)` on the same argument; this is the first time nanosecond
  `Ts` leaves raw, which does not contradict `quant-meta`'s microseconds because
  Postgres holds an *index* and this holds a re-derivation. The **instrument is not
  a column** (M0: a registry index is never persisted) — identity comes from the
  partition path.

  **Levels are `LIST<STRUCT<px, qty>>`, one row per event.** Exploding to one row
  per level compresses better and scans faster and destroys one-row-per-event;
  since the criterion is that events round-trip exactly, the encoding that
  preserves the event is the one that can be checked.

  **A file names the session it came from**, in the Parquet footer, and the writer
  **refuses a partition another session owns**. Found by writing the real capture:
  the layout has no session dimension (correctly — the tier is about the market),
  but a recorder restart makes a new session and two can cover one symbol-day, so
  publishing the second over the first would lose a day and leave a file that looks
  complete. Merging them — ordered by `local_recv_ts`, since `ingest_seq` is
  session-scoped and cannot order across sessions — is the eventual answer and is
  **not implemented**; refusing is what makes deferring it safe rather than lossy.
  A file whose origin cannot be established is also left alone: *cannot tell is not
  permission.* Partitions are **published by rename** (`.tmp` sibling, moved once
  the footer lands), so a reader never finds a truncated file at a real path.

  **A `Break` abandons the day in progress rather than writing across it.** The book
  survives a discontinuity by clearing; a file cannot, because a partition written
  across one looks continuous forever after and nothing in Parquet can say
  otherwise. Raw is the source of truth and this tier is disposable, so the answer
  is to stop and re-derive. One exception: a torn tail on the **last** segment, the
  ordinary signature of a killed recorder — the same distinction `quant-verify`
  draws between a missing trailer on the final segment and one anywhere else.

- **The mistake M2.d's tests caught, and it is the interesting one**: my first
  partition writer **re-derived** the day from `local_recv_ts` with a never-backwards
  clamp — reimplementing M1.c1's rolling rule. Close enough to look right, and wrong
  in exactly the case that rule exists for: a record stamped before the open
  segment's day (an NTP step) is written to the open segment on purpose and counted
  as `backdated_records`. A re-derivation files it somewhere raw did not, and the two
  tiers stop lining up — taking with them the only cheap cross-check between them.
  **The day is now inherited**: `SessionReplay` exposes the segment's date and the
  writer is told where to file. General lesson: when another component has already
  decided something, *ask it* rather than reimplementing the decision, however short
  the reimplementation looks.

- **Two smaller bugs the tests found in M2.d**: `DatasetWriter::rows()` was read
  *before* `finish()` flushed the last batch, so every file under one batch
  reported zero (fixed by returning the count from `finish`, the shape
  `RawWriter::finish` already uses — *a count is only trustworthy once the thing
  counting has closed*). And the reader indexed columns by **position**, got `gaps`
  off by one, and reported a type error about a perfectly good file; it now looks
  columns up **by name**, because positional access is a second silent copy of the
  field order that only the reader knows.

- **M2.e complete** (2026-09-19): two sessions on one symbol-day are **merged**,
  not refused. M2.d left this as work owing and the fortnight made it due — one
  recorder restart inside a day would have made the whole window unjudgeable,
  because M5's criterion needs `normalize --write --check` over it.

  A restart is **sequential**: `supervise.sh` runs the recorder in the foreground
  of its restart loop and reads its exit code before respawning. So a shared day
  is a **concatenation, not an interleave**, and a file boundary is the exact and
  free encoding of one — `part-00000.parquet` is the first session's slice,
  `part-00001.parquet` the second's. Nothing inside a row changes, which is
  *forced* rather than chosen: `--check` compares whole `MarketEvent` values and
  `EventMeta` includes `ingest_seq`, so renumbering would fail at the first event
  with no tolerance available. Every file still names exactly one session, so no
  footer has to describe two sources.

  The ordering key is `(day, book_seq_first, ingest_seq)`, and **only the last is
  compared inside the four-way merge**. `ingest_seq` restarts at 1 for a new
  session, so merging across a part boundary on it would interleave the second
  session's opening events into the middle of the first.

  **This said `(day, part, ingest_seq)` and that was wrong — see the M2.e
  ordering defect below.** A part's index says which session was normalized
  first, not which recorded first.

  **The data contract's own sentence about this was wrong, and is corrected
  rather than implemented.** It said the merge should be "ordered by
  `local_recv_ts`". That is `SystemTime`: it steps, and it is **not monotone even
  within one session** — `quant-recorder::segment`'s never-roll-backwards rule
  and the `backdated_records` counter exist because of it. (That rule is
  documented in the code and in M1.c1's notes above, and **nowhere in the data
  contract** — this used to cite a §6 of it that does not say any such thing,
  and `docs/data-contract.md` carried the same dangling citation to its own §5.) Ordering two sessions by a
  quantity that can run backwards either refuses a healthy day after an NTP step
  or, worse, silently reverses them. Parts are ordered by the **venue's own
  update-id span**, recorded per part in the footer: strictly increasing per
  symbol across disconnects, immune to anything our clock does, and available
  from `BookDelta`'s range in the M0 event contract — so no venue knowledge
  enters `quant-normalize`. Overlapping spans mean the sessions were
  *concurrent*, which is refused: there is no ordering of two simultaneous
  recordings of the same messages that is the truth.

- **The M2.e ordering defect** (found 2026-09-20 by auditing this file against
  the code, and it is the reason that audit was worth running). M2.e shipped a
  merge that **refused the ordinary case about half the time** and would have
  read it back in a random order the rest of the time.

  A part's index is assigned by `place` in **arrival order**. Arrival order is
  the order `catalog` yields sessions, and `catalog` sorts paths whose session
  component is a **v4 UUID** — so for two perfectly sequential recorders, which
  one landed at part 0 was a coin flip. `check_follows` then demanded the
  incoming part start after every published part ended, so the *earlier* session
  was refused whenever it lost the flip — and `normalize` abandons the writer on
  a refusal, so the rest of that session's days went unwritten too, **including
  days it did not share**. The error said "these sessions were not sequential"
  about two sessions that were. One restart in the fortnight would have hit this
  with probability one half, and judging step 3 needs `normalize --write --check`
  over that window.

  The fix separates two things this slice had conflated. **Write time asks only
  what is checkable there: disjointness.** Overlap means the recorders ran at
  once, which is the one case with no true ordering, and `PartsConcurrent` now
  says that and nothing about arrival. **Read time takes the order off the spans
  already in every footer**, where it is a fact about the market rather than
  about the order we happened to derive things in. `order_by_span` charges no
  footer read for a day with one part, so every single-session day on disk reads
  exactly as before.

  **The lesson is about the test, not the code.** M2.e shipped with
  `two_sessions_on_one_day_are_merged_into_parts`, which always normalizes the
  earlier session first — it pinned one half of a coin flip and never took the
  other. Both new regression tests go red on the old semantics and both of M2.e's
  own tests stay **green** under that same sabotage. A fixture that fixes the
  order of an input whose order is the thing under test proves less than it
  looks like it does.

  **The span is a property of the part, not of each file.** Written into all four
  datasets including the empty ones — keyed to each file's own rows, the ordinary
  day with no gaps at all (the acceptance week had 38 gap frames in total) would
  leave its `gaps` file unable to say where it belongs and refuse the restart on
  a completely normal day. Found by an adversarial review before it shipped.

  Four guards exist because "open one exact path" became "enumerate a directory",
  which makes new things reachable: the four dataset directories must agree on
  which parts exist (the old per-path ownership check guarded a half-deleted day
  incidentally); indices must be contiguous from zero (a hole means a part was
  removed, or a rename died between unlink and rename — counting would overwrite
  an occupant); only names `part_file_name` emits are recognised (the `.tmp`
  sibling survives a `SIGKILL`); and a day resolving to no parts is an error, not
  a shorter stream.

  `check_session` reads back only its own session's parts, selected by the footer
  rather than by remembering which index was written, so both sides of the check
  stay independent.

  Verified on real captures rather than fixtures — two rehearsal sessions that
  genuinely share 2026-09-18 for BTCUSDT. The pre-merge binary reports `write
  ABANDONED … not implemented`; this one writes both, prints a `merged` line, and
  passes `--check` on each session independently (18,925 and 18,435 events). A
  backtest over the merged day sees **37,360 events — exactly the sum** — so the
  reader concatenates rather than truncating.

  **One procedural consequence.** A build from `m5-run-start` reads `part-00000`
  alone. If a recorder restarts mid-day, run judging steps 3 and 4 from a build
  containing the parts reader; `normalize --write` prints a `merged` line
  whenever that applies. `docs/paper-run.md` records why that does not breach the
  freeze: the freeze exists so the *system under comparison* does not change, and
  a reader that reads all of the data rather than some of it is not the system
  under test.

- **`normalize --check` is the criterion, and it is event by event.** Two
  reconstructions can produce identical book statistics from different events — a
  transposed pair of timestamps, a level moved from one delta to the next, an
  aggressor flipped on a trade later cancelled out — so a summary comparison would
  pass all of them. `TierReplay` is a **lazy iterator**, not a comparison routine,
  because that is the shape M3's `HistoricalSource` needs; the check is its first
  consumer, not its purpose. A day is a four-way merge on `ingest_seq` across the
  four dataset files, which is what makes `ingest_seq` the ordering key rather than
  an incidental column.

- **Book depth reaches 11k–34k levels** against the 5000-a-side snapshot window.
  Expected: deltas keep inserting levels outside the window and nothing removes
  them. The touch stays correct, which is what anything trading reads. Documented
  in `quant-book`'s crate docs as an accepted limitation — the venue cannot tell us
  more than 5000 levels.

- **M3 complete** (2026-09-02): the engine seam, the simulated venue, and a
  deliberately naive strategy. 301 tests green in debug and release, clippy and fmt
  clean. Criterion — **an equity curve is produced, and it is unimpressive** — met:
  BTCUSDT $100 → **$97.53** over the acceptance week (−2.47%, 4.40 max drawdown,
  209 round trips) with **no fees modelled at all**; ETHUSDT $100 → $99.18. All six
  of `docs/engine-contract.md` §7's criteria are ticked.

  | | Slice | Status |
  |---|---|---|
  | a | `quant-core::execution`: orders, fills, execution events | **done** |
  | b | `quant-engine`: the loop, `Strategy`, `RiskLayer`, `ExecutionVenue` | **done** |
  | c | `quant-sim`: the simulated venue and its fill model | **done** |
  | d | `quant-backtest`: portfolio, equity curve, MA crossover, `backtest` bin | **done** |

- **The engine contract was written before the engine** (`docs/engine-contract.md`),
  for the reason M0 wrote the data contract before the recorder: the seam is the
  expensive thing to change once anything depends on it.

  **Submission is fire-and-forget**, and this is the decision everything else
  follows from. `submit(order) -> Result<Fill>` is the obvious API and the most
  damaging thing that could have gone in that document: a live venue cannot answer
  without a round trip, so a synchronous return either blocks the engine or lies —
  while a *simulated* venue answers instantly, handing a strategy the outcome of its
  own order at the moment of placing it, **in backtest only**. Tuned on that, a
  strategy is tuned on a machine that does not exist. So outcomes come back as
  `ExecutionEvent`s into the same loop as market data, and the cost — a strategy must
  track its own outstanding orders — is charged identically in all three worlds,
  because production charges it anyway.

  **Two identifiers.** `ClientOrderId` is ours and exists before the request leaves;
  `VenueOrderId` is the venue's and may never exist, because a rejected request was
  never an order for the venue to name. With only the venue's, a rejection could not
  be correlated with the submission that caused it. `VenueOrderId` is a `String`
  because it is the venue's namespace: normalising it into a number we invented would
  destroy its one property, that pasting it into the venue's interface finds the order.

- **M3.b decisions** (`crates/quant-engine/`). **The step order inside one event is
  the part most able to lie**, so it is in the module docs and pinned by a test. For
  an event at `T`: the clock advances, the book updates, **the venue matches resting
  orders**, the strategy is told what happened to its orders, and *only then* does the
  strategy see the event. Step 5 after step 3 is the whole point — an order submitted
  on seeing a print is not eligible to match against that print. Reversing two lines
  lets a strategy trade on information at the instant it is created, which is invisible
  in the output and inflates every result.

  **The test for it was verified against a deliberately inverted loop**, where it fails
  with the fill landing at the timestamp of the event that prompted it. *A green test
  that cannot go red is worth nothing* — worth doing again for any property this load-
  bearing.

  **The clock is the event stream**, in all three worlds. No system call on this path,
  which makes a backtest deterministic and is still correct live. Invariant 4 arriving
  where it was going.

  **`Context` is the entire surface a strategy has**: a read-only book, the clock,
  submit/cancel, and its own position. No venue, no source, no wall clock — so there is
  no accessor that could reveal the wiring, and a test runs the *same strategy value*
  against a filling venue and an accept-only one and compares state. The absence of a
  venue reference is also what makes risk a chokepoint rather than a module the strategy
  politely calls: a test with a refuse-everything layer asserts the venue was never
  **told**, not merely that it declined.

  **`EventSource` lives in `quant-core`**, so crates that provide events need not depend
  on the engine, and returns `Option<Result<..>>`: the simpler signature has one failure
  mode and it is the worst available — a source that hits an unreadable file returns
  `None`, the engine sees a clean end of stream, and the backtest silently covers less
  data than it claims to.

- **M3.c decisions** (`crates/quant-sim/`). Separate crate so the engine stays a loop
  and four traits with no opinion about how an order becomes a fill; it is also where
  M4's cost models belong, beside the fill model they make less optimistic.

  **A market order walks the book** and pays the size-weighted average. Filling a whole
  order at the touch makes size free, and a strategy tuned on that learns to trade sizes
  that do not exist. At our capital an order will almost never leave the first level —
  a fact about our size, not a licence to skip the walk. (2 of 418 fills walked, in the
  real run.)

  **A resting limit fills only when the market trades *through* it** — strictly past the
  limit. The usual shortcut fills as soon as the best ask touches the limit, which
  silently assumes we were at the front of the queue at our own price; for a retail order
  arriving last that is close to the least likely outcome. This understates fills, and
  **understating is the safe direction**: a backtest that misses trades is disappointing,
  one that invents them is dangerous.

  **Fills happen only in `observe`, never in `submit`.** The engine's step order can only
  hold if the venue does not fill early — this is the other half of that guarantee.

  **`SimStats::caveats()` is a method, not a comment**, because the contract requires the
  absence of fees, latency, queue position and market impact to be in the *output*. The
  `backtest` binary prints it every run, not behind a flag.

- **M3.d decisions.** The **portfolio lives in `quant-engine`**, not the simulator: a
  live venue's fill report needs booking exactly as a simulated one does. Average cost
  rather than FIFO (path-independent, cannot be gamed by lot choice). **Realized is money
  that has moved; unrealized is an opinion about a price — and it goes away when the book
  does.** `equity()` returns `None` when there is a position and no mark, because an
  equity curve that interpolated through gaps would smooth over exactly the periods worth
  looking at. Fees are inside cash *and* totalled separately, which is what makes
  "profitable before costs and not after" visible rather than inferred — that sentence is
  the whole of M4.

  **`Recorded<S>` wraps any strategy** to sample equity: a decorator, not an engine
  feature. The engine has no business knowing what a report is, and a trade log or a risk
  observer would be another wrapper rather than another engine field.

  **A missing sample is kept as a hole**, and written to CSV as an empty field — not a
  zero (which looks like a wiped-out account) and not a carried value. Skipping would
  leave a gap a plotting tool draws a straight line across.

- **The property that composed itself** (worth remembering): 19 gaps in the run produced
  **zero** orders refused for want of a market, and `MaCrossover` contains no mention of
  gaps at all. A gap clears the book → there is no mid → no sample is taken → the
  indicator does not advance → no crossing can fire. Three independent decisions (M2.b's
  "cleared, not flagged"; sampling on a clock rather than per event; taking the mid from
  the book) compose into "do not trade across a gap" with nobody enforcing it. **This is
  the kind of property a later refactor removes by accident**, which is why it has a test
  and a paragraph.

- **Every number in the first run cross-checked**, which is what made the result
  believable rather than merely disappointing: 10,080 samples is exactly 7 days of
  minutes; 9,810 with a mid + 270 blind = 10,080; 418 crossings → 418 orders → 418 fills
  as 209 entries + 209 exits (so it never got stuck holding); realized P&L equals the cash
  change exactly and final equity equals cash because it ended flat. Two invocations are
  byte-identical. *When a result is bad, check that it is bad for the reasons you can
  account for.*

- **M4 complete** (2026-09-02): fees, latency, and the answer. 320 tests green in
  debug and release, clippy and fmt clean. Criterion — **results degrade sensibly
  under realistic costs** — met, with all five of `docs/engine-contract.md` §8's
  checkboxes ticked.

  | | Slice | Status |
  |---|---|---|
  | a | `Rate` + `FeeSchedule`, fees on fills | **done** |
  | b | Latency: outbound, inbound, cancels on the wire | **done** |
  | c | CLI flags, the degradation sweep, the caveat split | **done** |

  **The answer**, BTCUSDT over the acceptance week, 209 round trips:

  | costs | equity | fees | drawdown | gross P&L |
  |---|---|---|---|---|
  | free (= M3) | 97.53 | 0 | 4.40 | −2.47 |
  | 1 bps | 94.25 | 3.28 | 7.00 | −2.47 |
  | 7.5 bps | 72.94 | 24.59 | 27.30 | −2.47 |
  | **10 bps (Binance spot)** | **64.74** | **32.79** | **35.45** | −2.47 |
  | latency 50 ms | 97.57 | 0 | 4.37 | −2.43 |
  | `--realistic` | 64.79 | 32.79 | 35.41 | −2.43 |

  The strategy loses **2.5% on price and 33% on commission**. 418 fills at ten basis
  points on a $76 position is 43% of position value in a week, so gross returns
  would have to beat that to break even. That settles this strategy class at this
  turnover — which is what M4 was for, and it cost nothing to learn.

- **M4 decisions.** **"Degrades sensibly" had to be made falsifiable** or it is a
  vibe. Two properties: `Costs::NONE` reproduces M3 *to the last digit*, and fees
  are **monotone** — for rates `a < b` the result under `b` is never better. The
  second is the property a sign error on a fee breaks, and a sign error on a fee is
  otherwise invisible because it just looks like a surprisingly good strategy.

  **Costs are off by default**, so a bare `backtest` run is still M3's. Not
  laziness: the no-op property is what makes the models checkable, and it is easier
  to trust when it is what runs when you type nothing.

  **Fees are visible separately from the price result.** `realized` is price only,
  `fees` is its own total, `cash` carries both — so "profitable before costs and
  not after" is read off the output rather than inferred. And the **venue and the
  portfolio each tally the fees independently**, so a disagreement means one of them
  is wrong; neither would say so alone. (Same shape as M2.d's two readers agreeing
  on 70,545,346 frames.)

  **Fees are charged in the quote currency**, which is a simplification and is named
  as one in the caveat list: a spot venue takes its fee in the base, leaving the
  position a fraction smaller rather than the cash a fraction lower. Second-order at
  our size; modelling it properly needs two balances, which is M5's problem where a
  real statement can settle it.

  **`adverse_per_fill` is a stress knob, not a model** — zero by default, labelled
  in three places, and the binary prints a warning when it is non-zero. An
  uncalibrated number presented as a model is worse than no model, because the
  output looks equally authoritative either way.

- **Latency is a variance, not a cost** (the M4 finding worth remembering). At 50 ms
  the result got slightly **better**. That is not a bug: fees subtract a known
  amount, but latency moves the fill to a *later book*, and over a 60-second
  sampling horizon the sign of that move is a coin flip — 50 ms is one
  twelve-hundredth of a bar. So **"latency must never improve results" would have
  been a wrong criterion**, and the test asserts that latency *changes something* —
  a model that altered no fill would be a field, not a model — and deliberately
  asserts no direction. It becomes a systematic cost only for a strategy fast enough
  that the market's move during the round trip correlates with the reason it traded.

  Outbound and inbound are separate numbers because their effects are not
  symmetric: outbound changes **what price we get**, inbound changes **when we find
  out**, and a strategy that reconciles its position behaves differently under the
  two. **Cancels travel the wire too** — pretending cancellation is free is exactly
  the assumption that makes a market-making backtest look safe — and can therefore
  lose the race to a fill, which is why `cancel` was fire-and-forget from M3.
  Reports still in flight when the data ends are **counted, not flushed**: flushing
  would tell a strategy something it could not have known.

- **The M4 bug worth remembering, twice over.** The first version of the CLI parsed
  the flags, printed `fee taker 0.001`, and handed the venue `SimulatedVenue::new()`
  — `cargo fmt` had collapsed the `Engine::new` call onto one line and my edit
  silently missed it. So **the report announced ten basis points and the fills were
  free**: a cost model configured but not wired, producing a *confidently wrong*
  number, which is the exact failure this milestone exists to prevent.

  The fix is not the wiring. The report now reads costs back off
  `engine.venue().costs()`, so "configured" and "applied" cannot disagree again.
  **General lesson: print what was used, not what was asked for — ask the thing
  that did the work.**

  And the caveat list was lying in the *other* direction: it claimed "no fees" and
  "no latency" unconditionally, which became false the moment `Costs` existed. A
  caveat that errs that way invites a reader to discount a cost that was actually
  charged. Split into `caveats()` (cannot be modelled) and `switched_off(costs)`
  (modelled, and you set it to zero), with a test asserting the permanent list makes
  no claim a flag can change.

- **I reimplemented `Px::notional` twice** before noticing it had existed in
  `quant-core` since M0 — once in `quant-sim`, once in `quant-engine`'s portfolio,
  each with its own 128-bit `mul_div`. Two copies of a money calculation that must
  agree forever, beside a third that was there first. This is *the same lesson M2.d
  recorded* and I broke it inside one milestone. Both now call `Px::notional`, and
  its inverses (`Notional::per_unit`, `Notional::scaled_by`) live beside it so the
  set cannot drift. **Before writing arithmetic, grep `quant-core`.**

- **M5 code complete** (2026-09-02 → 2026-09-03): paper trading. 373 tests green
  in debug and release, clippy and fmt clean. Every slice is built and **rehearsed
  against the live venue**; the criterion is **the run**, deliberately deferred —
  the same shape M1 had, seventeen days between code complete and the acceptance
  run passing.

  | | Slice | Status |
  |---|---|---|
  | a | `TeeSink`: one ingress, two consumers | **done** |
  | b | `quant-binance::LiveSource`: the socket as an `EventSource` | **done** |
  | c | `quant-engine::journal` + the `reconcile` binary | **done** |
  | d | The `paper` binary: `record()` extracted so it can feed the tee | **done** |
  | e | Ops harness and rehearsal | **done** |
  | — | The fortnight itself | **the remaining criterion** |

- **M5.d/e decisions.** `record()`'s 160-line body moved to
  `quant-binance::capture`, parameterised by a closure that wraps the capture
  channel's sender: a recorder passes it through, a paper run returns a `TeeSink`.
  That closure is the *entire* difference, which keeps the seven-day-proven path
  and the paper path the same code. Everything else moved rather than being
  rewritten — that code has an acceptance run behind it and the M1 notes are full
  of things it learned the hard way.

  **Costs default to retail in `paper`**, the opposite of `backtest`. A paper
  session exists to resemble live trading, and run free it produces a number that
  looks like a paper result and is not.

  The harness is **parameterised, not duplicated**: `supervise.sh` takes a mode,
  `start-run.sh` takes `--paper`. Nine hundred lines copied for one changed argv
  would be two harnesses that must agree forever. The `.ps1` half is deliberately
  *not* updated — those exist for a Windows run that is not happening, and an
  untested paper mode there would be exactly the harness-never-run M1 warns about.

- **The rehearsal earned its keep** (three defects a fortnight would have found
  expensively). `.resuming()` replaces the portfolio outright, so calling it with
  a fresh journal's empty recompute reset starting capital to zero — the first
  rehearsal reported `cash 0 from 0` on a session configured with a hundred. No
  final checkpoint or `Stopped` was written, so `reconcile` would have exited 2 on
  a perfectly good session. And `NothingToCheck` was being reported as a
  disagreement, which it is not.

  End to end it now gives: 4552 frames captured, **4552 events reaching the
  engine** (the tee delivering everything, no drops either side), fills journalled,
  `verify` clean, `reconcile` AGREES. A harness round trip bought at 77,634.64 and
  sold at 77,637.67 — **+0.3 cents of price against 15.5 cents of fees**, which is
  M4's finding arriving from live data.

- **A known hole to close before M8**: a hard kill between a risk trip and
  shutdown loses the trip, because the switch is journalled at shutdown. Harmless
  in paper; before real money the day's tally must be recovered from the journal
  rather than only the switch.

- **M5's criterion was sharpened, and one of my claims was wrong.** "P&L
  reconciles against an independent recompute" checks arithmetic against itself
  and would pass on a system whose live and replay paths disagreed about what the
  market did. It is now: **paper P&L must match a backtest over the data captured
  during the same window**, exactly rather than within a tolerance. Achievable
  because the tee gives both paths identical events with identical stamps.

  And at the end of M4 I said M5 was the instrument for measuring queue position
  and market impact. **Wrong**: a paper venue uses simulated fills, so our orders
  are still not in the book and nobody is still reacting to them. Both remain
  unmeasured after M5, and **only M8 can measure them.**

- **There is no `PaperVenue`, and that is a finding.** The architecture diagram
  lists three venues; the middle one does not need to exist. A paper venue fills
  against a reconstructed book at prices the book showed, which is exactly what
  `SimulatedVenue` does — what separates backtest from paper is the **source** and
  the **durability**, not the matching. So paper is `LiveSource + SimulatedVenue`
  and the row is really two venues, simulated and live. Writing a second fill
  model to satisfy a diagram would have created two things that must agree forever
  with no way to notice when they stopped.

- **M5.a/b decisions.** `Ingress<S>` has been generic over its sink since M1.b, so
  a `TeeSink` feeds the capture writer *and* the engine with **identical**
  `local_recv_ts` and `ingest_seq` — no change to the recorder at all. Sameness is
  not tidiness, it is what makes the agreement criterion checkable: two
  subscriptions would stamp differently, a file tail would lag by a block.

  The capture is **primary**: market data is irreplaceable, a paper fill is not.
  A dead engine is **latched**, so a strategy that crashes on day three costs one
  failed send rather than one per message for eleven days, and recording carries
  on. The engine is never *told* it missed a record — it finds the hole in
  `ingest_seq`, the same mechanism `quant-verify` and `quant-normalize` already
  use. `GapCause::LocalOverflow` there means *this consumer* overflowed, and the
  capture for the same instant shows no gap: that difference is information.

  A payload that will not parse is also blindness (`SequenceGap`): nothing was
  dropped, we simply cannot read what arrived, and a message whose effect on the
  book is unknown is what a gap represents. Loud without killing a fortnight.

  And the six lines mapping a frame kind to a parser now live once, in
  `quant-binance::decode`, shared by the offline replay and the live source —
  extracted **before** the second copy existed, which is the M2.d/M4 lesson
  applied forwards for once.

- **M5.c decisions.** The journal is a **file, not the metadata tier**: M1.c2 made
  Postgres best-effort and optional, and a position that must survive a restart
  cannot depend on something optional. Raw's relationship with its index, applied
  to a second kind of irreplaceable data. JSON Lines against this project's grain,
  because a journal is hundreds of lines a week rather than millions a day — so
  density buys nothing and `jq` at 3am buys a lot; only the **last** line may be
  torn, since nothing writes into the middle of an append-only file.
  `(exchange, symbol)` and never an id, pinned by a test that replays one journal
  through two registries with different id assignments. Fills are journalled
  **before** the strategy is told, so a crash cannot erase a fill it acted on.

- **The vacuous check** (worth remembering). My first `reconcile` asserted
  `cash - starting == realized - fees` and exited 0. The recompute derives all
  three from the same fill lines, so that identity holds *by construction* and can
  never fail — decoration wearing the costume of evidence. A real check needs two
  **independent** computations, so the engine now writes `Checkpoint` entries
  claiming what it believes and the recompute must match them from the file alone.
  Three hand-built journals confirm it goes red: wrong cash exits 1, a fill never
  written down exits 1 and is named, and *no checkpoint* exits **2** — because
  "nobody disagreed" is not "two answers matched". **General lesson: before
  trusting a check, ask what input would make it fail.**

- **M6 complete** (2026-09-02): the risk engine and kill switch. 373 tests green
  in debug and release, clippy and fmt clean. Criterion — **limits provably veto a
  misbehaving strategy, under test** — met, with all five of
  `docs/engine-contract.md` §10's checkboxes ticked. Built **before** M5's run on
  purpose, so one fortnight exercises the limits too rather than needing a second.

- **M6 decisions.** **Risk keeps its own tally** rather than reading `Portfolio`.
  Deliberate duplication, and the point: a limit computed from the accounting can
  only be as correct as the accounting, so a portfolio bug would take the limits
  with it *precisely when something is already wrong*. Fourth time this pattern has
  paid. The tally is deliberately **simpler** than the portfolio's — a limit needs
  to be obviously right rather than exactly right.

  **A money limit refuses when there is no price.** You cannot size what you cannot
  price, and "assume the last price" makes the limit widest when the market is least
  understood. But an order with *no* money limit set passes through a gap: a limit
  nobody configured should not have an effect.

  **The position limit checks the position the order would create**, not the one we
  hold — the latter lets everything through up to the one that mattered. Exposure is
  absolute so a short counts, and **reducing an oversized position is always
  allowed**: a risk layer that trapped a position it thought too large would be the
  most dangerous thing here.

  **Fees count against the daily loss budget**, or the limit is reached late by
  exactly the amount M4 showed is most of the damage. Loss and order-count limits
  **trip** rather than refuse, because a loop does not stop being declined once; a
  refused order does **not** consume the daily count, or the count measures our
  refusals rather than the strategy's activity.

  **A tripped switch survives a restart** (`RiskEngine::recover`) because a kill
  switch that forgets is not a kill switch — the supervisor would re-arm it. **A new
  day resets the counters and not the switch**: resuming is a decision, not a
  timeout. The day never rolls backwards, so an NTP step cannot hand a stopped
  strategy a fresh budget.

  **A tripped switch refuses everything, including a flattening order.** A real
  trade-off, written down rather than discovered: closing out becomes manual, which
  is the right friction for something that has already gone wrong.

- **Two misbehaving strategies live in the test suite**, because the criterion says
  *provably, under test*: one buys 1000 units every event, one submits forever. The
  oversized order **never reaches the venue** — not declined by it, never told to
  it — and the runaway gets exactly its limit of orders out however long the run.
  And **limits that do not bind change nothing**, which is the other half: a risk
  layer that quietly altered a permitted run would make every backtest a different
  system from the one that trades.

  **All of it verified capable of failing.** With the limits neutered, three of
  these tests go red. That check is now a habit rather than an afterthought, and it
  came directly from M5.c's vacuous-identity lesson.

- **A bug M6 uncovered in M5.c**: the `FillObserver` was never actually called.
  `cargo fmt` had reformatted the block my edit targeted and the replacement
  silently missed, so fills would never have been journalled — the durability M5.c
  claimed did not exist. This is the *second* time a formatting-shifted edit has
  silently not applied (M4's cost flags were the first). **Lesson: after a scripted
  edit, grep for the thing that should now be there rather than trusting that the
  patch matched.**

- **M7 complete** (2026-09-19): observability. 411 tests green in debug and
  release, clippy and fmt clean. Criterion — *"what was it doing at 03:14 last
  Tuesday?" answered in a minute* — met, and measured against the fortnight
  **while it was running**. Full reasoning in `docs/observability.md`.

  **The scoping call, which re-scopes the milestone.** M7 is a **time cursor over
  artifacts that already exist — a reader, not a recorder and not an exporter.**
  Same shape as M1.d: sort the facts the question needs by which ones a later
  milestone can still recover, and keep only what this one must supply. The book,
  the mark, whether we were blind, our position and P&L are pure functions of raw
  plus the journal, and cheap — `normalize` replayed 3.4M frames of the live run
  in 4.70 s, so a symbol-day is about six seconds.

  The tempting conclusion is that what *isn't* recoverable (orders that never
  filled, refusals, strategy state) is urgent, so M7 should be a recorder. **That
  is right about the loss and wrong about the remedy:** the fortnight is pinned
  and rule 1 is never to pull there, so a run log merged tomorrow would not be
  written by the running process. That column's loss for this run is already
  sunk.

  **The defect fixed is not missing data. It is that nothing had a notion of a
  moment** — every tool was either present-tense (`status.sh`) or whole-run
  aggregate (`dump`, `normalize`, `verify`, `reconcile`). `reconcile` had no
  `--at`; `dump` printed no timestamps at all; and raw, the one time-indexed
  durable record, had no reader that took a timestamp.

  **Conventional metric export was argued down, not skipped.** No Prometheus, no
  OTel, no `/metrics`. It does not touch the criterion; a time-series store would
  be a fourth copy of numbers derived from artifacts we already keep, with no way
  to notice when it disagreed — the pattern refused three times already; and it
  puts a listening socket in a process built `panic = "abort"` whose own history
  includes **a metric that killed the recorder**. The good idea inside that
  argument is kept: M1.e's line mixes instantaneous, lifetime and 60-second-window
  figures unmarked, so the reader **labels each figure's time base**.

  | | Slice | Status |
  |---|---|---|
  | a | `Ts` ⇄ RFC 3339 in `quant-core` | **done** |
  | b | `explain --at`: market state from raw | **done** |
  | c | `explain --at`: our state from the journal | **done** |
  | d | The checkpoint agreement, and proof it can go red | **done** |
  | e | Operational state, with time bases labelled | **done** |
  | f | `--window`, refusal fixtures, the document | **done** |

  **`quant-explain` writes nothing** — no index, no cache, no sidecar. That is the
  one property keeping it structurally unable to become a second source of truth,
  and it is why M7 needed no change to the data contract's tier table. It sits at
  the top of the graph beside `quant-verify`; nothing may depend on it, and it
  must never become a dependency of `quant-verify` or `quant-normalize` for the
  reason those two must never depend on each other.

  **`--at` is `local_recv_ts`**, per invariant 2. Seeking on `exchange_ts` would
  answer a question the engine never asked.

  **The day rule is self-correcting.** A day's file begins mid-stream, so the book
  stays unanchored until the next snapshot — up to an hour. Read the instant's
  day; if still unanchored on arrival, read again including the previous day.
  Always reading two days doubles the cost of every query to fix a minority.

  **Every absence carries a reason.** A query tool's characteristic failure is
  *confabulation* — printing a stale book as though observed, which looks entirely
  plausible. M2.b already made that safe by clearing an invalid book rather than
  flagging it, so there is no stale state to print even by accident. Eight tests
  assert an absence *and* a reason rather than a value.

  **The agreement check is deliberately not against `reconcile`.** Both call
  `journal::replay`, so that would hold by construction — M5.c's vacuous identity
  in a new costume. The independent pair is already in the journal: a `Checkpoint`
  is what the engine **believed in memory**, written by a process now gone; a fold
  of the fill lines is what **the file says**. `--check-journal` compares every
  checkpoint against the entries it describes, and exits **2** rather than 0 on a
  journal with none.

  Criteria, measured against the live run: **P1** 20 random instants, cold, all
  five blocks present — worst 5799 ms of a 10 s budget. **P2** 24 of 24
  checkpoints across both journals agree. **P3** the refusal fixtures.

  **Both figures need re-confirming at judging, and this file and
  `docs/observability.md` disagree about them.** That file records worst 4951 ms
  and 18 of 18; `docs/overview.md` sides with it on the latency and gives no
  checkpoint count. Neither is checkable from this repository — the journals and
  capture are on the Mac — and the likeliest explanation is benign: they were
  measured hours apart on a run that was still accumulating checkpoints, so 18
  and 24 were each true when taken and neither was stamped. **Neither number was
  picked over the other**, because guessing between two measurements is exactly
  what this project does not do. Re-run both against the finished artifacts on
  2026-10-02 and record the time of measurement beside the value.

- **M7's first real use found something** (worth remembering). The health block
  read `latency p50 4194ms p99 14155ms` at 2026-09-19T10:00Z, against 57–73 ms
  for the rest of the run's first nineteen hours. **Benign, and checked rather
  than assumed**: `queue=0`, `dropped=0`, `clock_skew=0` throughout, so nothing
  was backed up, nothing lost, and the host clock is fine — transport delay, the
  same signature M1 saw on hostel Wi-Fi. The point is that finding it previously
  meant grepping 1388 log lines and knowing which to compare.

- **Two boundary decisions came from tests rather than from me** (M7). The window
  is half-open, `(at - span, at]` — inclusive at both ends puts a seam event into
  two adjacent windows, so stepping through a run five minutes at a time would
  count it twice. And RFC 3339 parsing does its arithmetic in `i128`: midnight of
  1677-09-21 is below `i64::MIN` even though instants later that day are
  representable, so checking each intermediate rejected valid input. `Ts` spans
  roughly 1677-09-21 to 2262-04-11, now pinned.

- **A status.sh line the run itself falsified** (2026-09-19). It counted
  `session=*` **directories**, and the raw layout nests session inside day — so an
  uninterrupted recorder gets a fresh directory at every UTC midnight. The line
  went from "2 sessions" to "4 sessions" with no restart, and would have read 28
  by day fourteen, which is indistinguishable from 26 restarts. Same class as the
  "no recorder running" line, found the same way: **by looking at what the real
  run printed rather than at what the code was supposed to do.**

Milestone table: see `README.md`.

## The acceptance run, and how it went

**M1's seventh criterion is spent.** The run happened on an Apple Silicon MacBook
Air, 2026-08-21 → 2026-08-28, and **passed**: `quant-verify` exit 0, verdict *"every
discontinuity is explained by a record in the capture"*. This section is the record
of it — what was built to make it possible, what the week actually did, and the
handful of things worth remembering. It replaces the old "picking up" notes, which
planned a run that had not started.

### Why it moved off Windows

The run was set up on the user's Windows laptop and deliberately **not started**
there: `w32time` was stopped and the host sat ~2 s ahead of Binance (an
Administrator fix that session lacked), and it was a daily-driver laptop whose
sleep and forced reboots would end a run. It moved to a fanless Apple Silicon
MacBook Air — no forced reboots, and macOS disciplines its clock by default, which
turned out to hold: `sntp` put the host ~145 ms off Apple time and ~0.5 s off
Binance the whole week, well inside the 1 s tolerance.

### The port (`ops/*.sh`, committed on branch `macos-acceptance-harness`)

`ops/` was PowerShell-only; the logic ported, the scripts did not. There are now
bash halves beside every `.ps1` — `preflight.sh`, `supervise.sh`, `verify-loop.sh`,
`start-run.sh`, `status.sh`, `stop-run.sh`, `fix-clock.sh` — each carrying the same
reasoning in its header. Three macOS choices were not arbitrary:

- **Supervision is a plain bash loop, not the `launchd`/`KeepAlive` job the old
  notes proposed.** `KeepAlive` would survive a reboot, but an acceptance run should
  be *watched* and should stop when told to rather than silently resurrect itself —
  the same stance the run's own "known limits" already took. Restart still lives
  outside the recorder; each supervisor holds a `caffeinate -dimsu -w $$` assertion
  for its lifetime so the Mac stays awake across every reconnect.
- **Clean stop is `SIGINT`, not `SIGTERM`.** The recorder's shutdown is
  `tokio::signal::ctrl_c`, which is SIGINT on Unix; SIGTERM is not caught and would
  leave the last segment trailerless — indistinguishable from a crash. `stop-run.sh`
  sends SIGINT. (In the end this path was not needed: the recorders hit their 7-day
  duration limit and exited cleanly on their own, sealing trailers.)
- **The clock check is round-trip corrected.** The PowerShell preflight measured
  `now - serverTime`, which folds one-way latency to the venue into the offset. From
  a hostel connection that latency was hundreds of ms and would have *blocked a run
  over a clock that was fine* (seen live: −1445 ms naive vs +450 ms corrected).
  `preflight.sh`/`fix-clock.sh` use the NTP midpoint estimate and confirm sync
  read-only with `sntp` — no sudo, and the same quantity `clock_skew` reflects.

`zstd-sys` compiles C, so Xcode Command Line Tools were required
(`xcode-select --install`); everything else on `aarch64-apple-darwin` built clean
(179 tests green). Note macOS ships **bash 3.2** — no `mapfile`.

### What the week did

Two symbols, one recorder process each, **zero restarts across all seven days** —
the processes themselves never fell over; disconnects were handled internally.
70.5M frames (BTC 39.3M messages + ETH 31.2M), ~2.9 GB compressed from ~23.5 GB of
raw venue bytes. **`dropped=0`** the whole run
(queue peaks 728/488 of 4096, never close). **38 gap frames, every one explained:**
36 `Disconnect`s from flaky hostel Wi-Fi (each with its reconnect and resync
snapshot) plus the 2 mandatory `RecorderRestart` first-frames. `missing 0`, 0
errors. The single **warning** was one `snapshot-failed` — a resync snapshot timed
out ×4 on day 3 (Wi-Fi), recorded honestly as a `SnapshotFailed` rather than left
as a silence; a warning never fails the run. `unanchored-after-overflow` never
fired because `dropped` stayed 0, as it always has.

### Worth remembering

- **A `msgs_per_sec=0` in the last metrics line is not a stall.** It is one quiet
  or blipped minute; the honest liveness signal is the capture *file growing*, which
  `status.sh` and a two-sample `stat` size check show directly. A mid-run scare
  resolved this way — file was gaining ~5 KB/s while the last line read 0.
- **A network outage looks like DNS, not a socket error.** The one that mattered
  logged `failed to lookup address information` — the Wi-Fi dropped hard enough that
  Binance's hostname would not resolve. The recorder retried with backoff (hundreds
  of `connect failed` attempts over the week) and recovered every time.
- Latency figures reflect a hostel connection: `latency_max` reached ~195 s (an
  outage-recovery outlier) and `clock_skew` spiked to seconds in the metrics line
  even though the true offset held near 0.5 s — a worst-case-per-window figure
  inflated by jitter, not clock drift. Affects no completeness criterion.

### Do not re-litigate

Decided with reasons written down; changing them needs a new argument, not a fresh
preference:

- Restart belongs **outside** the recorder (`ops/supervise.{ps1,sh}` headers).
- Verification runs **during** the capture, not after (`quant-verify`'s `lib.rs`).
- Warnings do not fail a verification run (`finding.rs`).
- Seven days is the criterion in `docs/data-contract.md` §7. It was spent in full,
  not shortened for convenience.

### Settled since

- **The harness is merged.** `macos-acceptance-harness` landed as PR #1 and the
  README followed as PR #2; `ops/` now carries a `.sh` beside every `.ps1`.
- **CI covers Apple Silicon** as of 2026-08-31, as a second job rather than a
  matrix leg. GitHub's `services:` containers are Linux-only, so a macOS leg could
  not have the database whatever it were written as — and macOS runners bill at 10x
  on a private repo, which makes running the whole gate twice a poor trade. So the
  macOS job proves only what Linux cannot: that the workspace builds and tests pass
  on `aarch64-apple-darwin`, guarding the C toolchain surface (`zstd-sys` compiles
  C, `ring` assembles per-architecture). Format, lints and the release run stay on
  Linux.
- **The capture is off the Mac and independently re-verified** (2026-08-31): 16
  segments, 3.0 GB, in `data/acceptance` on the Windows box. `quant-verify` exit 0,
  70,545,346 frames, `missing 0`, 0 errors. Two details worth noting, because both
  are the design working rather than luck. The `no-trailer` warnings the Mac's last
  in-run check reported are **gone** — those files were still open at 06:49 and the
  clean exit at 12:33 sealed them. And the far-side frame count is *higher* than the
  Mac's last report (70.5M vs 68.98M) simply because that check ran 5.75 hours
  before the run ended.

### The AppleDouble strays, and what they showed (2026-08-31)

The transferred capture arrived with 90 macOS **AppleDouble** stubs
(`._exchange=binance`, `._part-00000.bin.zst`, …), 51 of them inside `raw/`, which
the verifier reported as `stray-file`. Root cause: macOS `tar` archives extended
attributes as sibling `._` files unless `COPYFILE_DISABLE=1` is set — so the
documented transfer procedure was followed and *still* produced them.
`docs/acceptance-run.md` now sets it, and carries a `find … -name '._*' -delete`
for captures that already have them.

Deleted on 2026-08-31, with all 90 first confirmed AppleDouble by magic bytes
(`00 05 16 07`) and every one of the 16 capture files sha256'd before and after to
prove nothing else moved. Re-verified: exit 0, still 70,545,346 frames.

**Two things this settled, both worth keeping.**

The finding cap did its job unprompted. `stray-file` findings carry no session, so
all 51 shared one `(code, session)` bucket and only 5 printed, followed by *"and 46
more stray-file"* — which is precisely the burial the cap exists to prevent. The one
genuinely meaningful warning, a resync snapshot that timed out on day 3, stayed
visible throughout. After the cleanup it is the only warning left.

And the fix belongs at the source, not in the verifier. Teaching it to ignore a
shape of filename would have made the noise go away and made the check weaker;
something unexpected in the immutable tier should get a line of output whatever it
turns out to be.

---

## The paper run, in flight

**It started 2026-09-18T14:46:54Z and ends 2026-10-02.** Two symbols, one paper
process each, capturing and trading from one ingress. The sections below are the
record of how it was set up; the full procedure is `docs/paper-run.md`.

### Where things stand

M0–M4, M6 and M7 are complete. **M5's fortnight is the one remaining criterion
and it is spending its wall clock now.** 414 tests green in debug and release,
clippy and fmt clean, ~30,700 lines across 12 crates.

Watching it:

```bash
ops/status.sh --root ~/paper
explain ~/paper --at 2026-09-19T10:00:00Z --symbol BTCUSDT [--window 5m]
explain --check-journal ~/paper/paper-BTCUSDT.jsonl
```

The verify loop runs every six hours and reconciles both journals on the same
cadence, so the honest signal is that `~/paper/logs/verify.log` stays all-`OK`.
A `msgs_per_sec=0` line is not a stall; the capture file growing is.

### The three rules, and the third is the one that bites

1. **Pin the run to a tag** (`git tag m5-run-start && git push origin m5-run-start`),
   check the Mac out at it, and **never `git pull` there** until the run ends. The
   running process would not change; the ability to say which code produced the
   result would.
2. **Freeze the raw container version and the journal schema** for the fortnight.
   Everything else on the development machine is free — the Mac runs a binary that
   is already loaded. But the artifacts must stay *readable*, and a container bump
   mid-run (as M1.d1 did, 1 → 2) would leave two weeks of data newer tools refuse.
3. **Run the final comparison from that tag.** M5's criterion compares paper P&L
   against a backtest over the same window. If `quant-engine`, `quant-sim` or the
   strategy change while the run is in flight, that comparison is between **two
   different systems** and would fail for reasons unrelated to live-versus-replay,
   which is the only thing being asked.

### Building on `aarch64-apple-darwin`

Proven at M1. `zstd-sys` compiles C, so Xcode Command Line Tools are required
(`xcode-select --install`); everything else builds clean. macOS ships **bash 3.2**
— no `mapfile`, no associative arrays. CI has covered Apple Silicon since
2026-08-31 as a second job, guarding exactly this C-toolchain surface.

### Starting it

```bash
git checkout m5-run-start && cargo build --release

# Rehearse first. Not optional -- M1's lesson is that a harness which has never
# been run is not a harness, and the aggressive indicator settings are the point:
# the fortnight's 10/30 on one-minute mids needs half an hour before it can cross,
# so a short rehearsal on those settings would leave the journal, the fills and
# the reconciliation untested.
ops/start-run.sh --paper --minutes 10 --symbols "BTCUSDT" --root ~/paper-rehearsal \
    --paper-args "--qty 0.001 --fast 2 --slow 4 --interval-secs 5"

ops/start-run.sh --paper --days 14 --symbols "BTCUSDT ETHUSDT" --root ~/paper \
    --paper-args "--qty 0.001 --cash 100"
```

Two symbols means two paper processes, each capturing *and* trading its own
instrument from one ingress — which is what the agreement criterion needs, and
gives the fortnight two independent samples.

### macOS specifics that cost real time at M1

- **Clean stop is `SIGINT`, not `SIGTERM`.** The recorder's shutdown is
  `tokio::signal::ctrl_c`, which is SIGINT on Unix; SIGTERM is not caught and
  leaves the last segment trailerless — indistinguishable from a crash.
  `stop-run.sh` already sends SIGINT.
- **The clock check is round-trip corrected.** Measuring `now - serverTime`
  naively folds one-way latency into the offset and would block a run over a clock
  that is fine (seen live: −1445 ms naive vs +450 ms corrected).
- **`caffeinate -dimsu -w $$`** is held for each supervisor's lifetime, so the Mac
  stays awake across every reconnect. A reboot still ends the run, deliberately.
- **`COPYFILE_DISABLE=1 tar ...`** when bringing data back, or macOS writes
  AppleDouble `._*` stubs that the verifier reports as strays. `docs/acceptance-run.md`
  carries the cleanup for captures that already have them.
- **Supervision is a plain bash loop, not `launchd`/`KeepAlive`.** A run should be
  watched and should stop when told to, not silently resurrect itself.

### While it runs

**Build in a separate worktree** (`git worktree add ~/quant-dev`), never in the
run's checkout. Two reasons, and the second is easy to miss: `supervise.sh` execs
`target/release/paper` on every restart, so a rebuild would silently continue the
run on different code; and bash reads a script file *lazily, by byte offset*, so
changing `ops/*.sh` underneath the running supervisor can make its loop jump
mid-execution. Merging to `main` is safe — only pulling *there* is not.

M2.e and M7 were both built this way and neither touched the run. Development on
the *Windows* box is safer still and needs no worktree: it is a different machine
and cannot reach the Mac at all.

**Fix readers only, until 2026-10-02.** The freeze exists so the system under
comparison does not change, and that is a checkable property rather than a
matter of care: `git diff --stat m5-run-start..HEAD` must show **no change**
under `quant-engine`, `quant-sim`, `quant-backtest`, `quant-book`,
`quant-binance`, `quant-recorder` or `quant-storage`. It currently shows none —
`quant-core`'s only change is purely additive RFC 3339 support in `time.rs` — so
a judging `backtest` built at `HEAD` links byte-identical engine, sim and
strategy code to the pinned build. That is what licenses judging steps 3 and 4
from a newer build at all, and it stops being true the first time a "small"
engine fix lands. Readers (`quant-normalize`, `quant-explain`, `quant-verify`),
docs and `ops/` are free. Re-run the check before judging.

**A newer tool may be pointed at the run's artifacts; `normalize --write` may
not.** `explain` and `status.sh` write nothing, which is `quant-explain`'s whole
design constraint. But `TierTarget::directory` is `root.join("normalized")`, so
`normalize --write ~/paper` publishes **into the live run's directory** — parts
derived from a still-open capture, which then sit there looking complete. That
step belongs after the run ends, which is where `docs/paper-run.md` puts it.

**A restart forfeits M5's exact-match criterion, and this is not the problem
every document says it is.** Every one of them frames a mid-run restart as a
*reader* problem, which is what M2.e was for. It is a **state** problem.
`JournalEntry` has no strategy entry and `Engine::resuming` replaces only the
portfolio, so a restarted `paper` process comes back with the right cash and
position and with **empty `MaCrossover` windows, a zeroed `Recorded` sampler and
a zeroed daily risk tally**. The judging backtest runs all of that *continuously*
across the same boundary: different crossings, different fill counts, and the
exact comparison fails for reasons unrelated to live-versus-replay. It cannot be
repaired after the fact, and the fallback — compare per-segment *between*
restarts rather than end to end — is written down now, before there is a number
to rationalise. Whether it applies is checkable from the artifacts: a distinct
session count above one per symbol, a `Gap{RecorderRestart}` first-frame, or
more than one `Started` entry in a journal.

**Bring the logs back, not just the capture and the journals.** M5's criterion
assumes the tee dropped nothing, and the only evidence of that is two numbers
that live nowhere else: `paper` prints `events {N} reached the engine` to stdout
and the capture emits `records = {N}` through tracing. `TeeSink::secondary_dropped()`
is read by nothing but its own tests, so without `~/paper/logs/` that check
cannot be made at all.

Do **not** build M8 on top of an unvalidated live path — it depends on M5's
live-versus-replay agreement having actually passed. Note too that everything
`explain` says about the market is what the *replay* claims the book was, and
M5's criterion is exactly the claim that that equals what the engine saw: until
it passes, M7 is a debugger whose foundation is the thing under examination.

---

### Still open

- **M5's fortnight is the one remaining criterion, and it is running.** Started
  2026-09-18T14:46:54Z on the Mac, ends 2026-10-02, two symbols, one paper
  process each. Judge it per `docs/paper-run.md`. Until then: no `git pull` and
  no `cargo build` in the run's checkout, and the raw container version and
  journal schema stay frozen.

- **The run log is the next milestone**, and it starts when the freeze lifts.
  Orders that never filled, risk refusals, cancels and strategy state are
  recorded nowhere and are **not recoverable by any reader** — M7 established
  that boundary rather than crossing it, and building the reader first made
  concrete what those entries have to contain. It also unblocks three things
  below that are all waiting on the same unfreeze.
- **A hard kill between a risk trip and shutdown loses the trip**, because the
  kill switch is journalled at shutdown. Harmless in paper, where nothing is at
  stake; **must be fixed before M8**, by recovering the day's tally from the
  journal rather than only the switch.
- **Queue position and market impact remain unmeasured** after M5 and cannot be
  measured by it: a paper venue uses simulated fills, so our orders are still not
  in the book and nobody is still reacting to them. **Only M8 can measure them.**
- **The strategy question is answered and the answer is no.** A crossover at 209
  round trips a week cannot survive 10 bps a side. M5 and beyond are about the
  platform being trustworthy, not about this strategy — and a lower-turnover or
  maker-side idea is the shape that could work, which is a thing to try *after* the
  platform can measure it honestly.
- **Three defects M7's scoping surfaced, all waiting on the run log.** The
  journal's `client_order_id` is a *fill ordinal*, not the engine's id, because
  `FillObserver::on_fill` is never handed the real one — and refused orders
  consume an id before the risk check, so the first refusal desynchronises it
  permanently (harmless so far: nothing has been refused). `Started.at` is
  hard-coded to zero, so a journal cannot date its own beginning. And
  `TeeSink::secondary_dropped()` is called from nowhere, so nothing watches the
  tee *during* a run — it is checkable afterwards, since `events N reached the
  engine` against the capture's `records=N` would differ, but M5's criterion
  assumes it was zero and nothing says so.

- **`quant-explain::health` parses prose, and that is debt with a scheduled
  repayment.** An emitter and parser that must agree forever through a format
  neither owns, with no test that they agree, is the pattern this project
  refuses — accepted only because switching the emitter to `.json()` changes the
  running binary. **Switch the emitter and delete `health.rs` in the same commit
  as the run log**, and write the emitter↔parser test there: it cannot be written
  now, because pinning the emitter means running it and the emitter is frozen.

  Two of that debt's interest payments came due on 2026-09-20 and are paid. The
  module claimed "an unrecognised line is reported as unparseable rather than
  skipped, so the day the format changes is the day this says so" — **half true**.
  A line that *looked like* a metrics line and would not parse was loud, but a
  line was only examined if it contained `metrics symbol=`, and that literal is as
  much part of the borrowed format as the field names. Switch the emitter and
  every line stops matching at once, whereupon the old code fell through to
  `NotCovered`, whose message says the process may not have been running — blaming
  a healthy run for a stale parser. `NoHealth::FormatUnrecognised` now separates
  the two, and says in as many words that it is not a statement about the run.

  And `clock_skew` was **a sample count reported as milliseconds**.
  `quant-recorder::metrics` increments it once per message whose venue timestamp
  is ahead of ours and records no duration anywhere; `explain` called it
  `clock_skew_ms` and warned above 1000 "ms", citing Binance's tolerance for a
  *signed-request offset* against a *count*. So a thousand ordinary samples
  produced a clock alarm in units nothing had measured. Now `clock_skew_samples`,
  printed against `latency_samples` so it reads as a proportion. The millisecond
  offset it was mistaken for is a different measurement entirely and lives in
  `preflight.sh` and the recorder's startup check. **A tool whose whole purpose is
  answering "what was it doing at 03:14" must not invent the units of its own
  answer** — this is the same class of defect as the verifier crying wolf on good
  data, and it was found the same way: by reading what the code does rather than
  what its docs say.
- Minor: CI annotates `Node.js 20 is deprecated` for `actions/checkout@v4` on both
  jobs. Harmless; fixed by bumping to `@v5` whenever CI is next touched.

## Conventions

- **Commit often — at every point the tree is green.** (Set 2026-08-31.) Not once
  per milestone slice. The log is the record of what happened and why, and a slice
  squashed into one commit throws away the order things were learned in. For M2.a
  that would have been three commits (the `dump --sample` flag, the parser, the
  corpus validation) rather than one.

  "Green" is the operative bound: it builds, `cargo test` passes, clippy is clean.
  A commit that does not build is not a record, it is a hole in `git bisect` — and
  the existing rule that commits stay **independently shippable** is what makes a
  gap in the work safe. Small and green satisfies both; small and broken satisfies
  neither.

  With the PR rule below, this means **many small commits on a branch, one PR per
  slice**. Fine-grained history for the record, reviewable units for the merge.
- **Never push to `main`. Raise a pull request.** (Set 2026-08-31.) Branch, push
  the branch, open a PR with `gh pr create`, and let the user merge it. The first
  two merges into `main` went through PRs (#1, #2) and that is now the rule rather
  than the accident. CI runs on `pull_request`, so a PR is also what gets the
  `check` and `macos` jobs to vouch for a change before it lands.
- Commits are authored **alienxviking <sroy191006@gmail.com>** with **no**
  `Co-Authored-By: Claude` trailer. Already set in this repo's local git
  config. Personal project under the user's own GitHub identity.
- **Conventional commit subjects.** (Set 2026-09-03.) `type(scope): subject`,
  lowercase, no trailing full stop. Types: `feat`, `fix`, `refactor`, `test`,
  `docs`, `build`, `perf`, `chore`. Scope is the **milestone slice** for milestone
  work (`m5d`, `m6`) and the **crate or area** otherwise (`engine`, `sim`,
  `binance`) — the milestone tie is what makes this repo's log readable against
  `CLAUDE.md`, and the type is what makes the graph scannable by kind.

  Existing history is **not** rewritten. Relabelling merged commits on `main`
  would mean force-pushing shared history, which is a worse habit than an
  inconsistent log; the convention applies going forward.
- Commit messages explain *why*, in the body. The rationale is the point.
- **After a scripted edit, grep for what should now be there — and for what
  should now be *gone*.** (Set 2026-09-20.) The first half was recorded twice
  already, both times about code: M4's cost flags and M6's `FillObserver` were
  edits `cargo fmt` had shifted out from under, so the patch matched nothing and
  the change silently did not happen. The second half is the docs' version and
  cost a milestone's worth of documentation: PRs #25 and #26 replaced paragraphs
  with search-and-replace and **the tail of the replaced text survived** fourteen
  times over. `docs/paper-run.md` ended up saying M2.e closed the merge hole and,
  two lines later, that it was still open and "the likeliest thing to stop the
  fortnight being judgeable at all". `docs/overview.md` shipped an un-executed
  instruction to the author as body text. `README.md` grew two stray code fences
  that made its verify commands render as prose.

  None of it was caught by CI, because none of it is code. The check that works
  is cheap and is the same one either way: after replacing text, grep for a
  distinctive phrase from the **old** version. If it is still there, the edit
  took half.
- **A green test proves nothing until you have seen it go red.** (Set
  2026-09-20, and it is the M5.c lesson generalised.) Before trusting a test,
  break the thing it tests and watch it fail — and prefer breaking it in *both*
  directions where the test draws a boundary, so a new case cannot quietly
  swallow the old one. This has now paid three times: the engine's step order
  (verified against a deliberately inverted loop), M6's limits (neutered, three
  tests went red), and M2.e's ordering, where the two tests that **shipped with
  the slice stayed green** under the sabotage that reddened the new ones. That
  last one is the warning: a fixture that fixes the order of an input whose order
  is the thing under test proves much less than it appears to.
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
- Rust edition 2021 on the **stable channel**, pinned by `rust-toolchain.toml`
  (the channel is pinned, not a version — whatever stable is when you build).
- **`rust-version` is the floor our dependencies impose, not an aspiration.**
  (Set 2026-09-02, 1.75 → 1.85.) Cargo's MSRV-aware resolver honours it when
  choosing versions, so a stale one is not free: at 1.75 it silently held us to
  `parquet` 54 from early 2025 while 59 was current — for a promise nothing tests,
  since CI runs `stable`. Raise it when a dependency we want requires it. Bumping
  it also un-blocks `clippy::incompatible_msrv`, which had been rejecting std APIs
  stabilised years ago (`Option::is_none_or`, 1.82).
