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
- **M1 complete** (code 2026-08-11, acceptance run passed 2026-08-28): the Binance
  recorder. Every slice is written, tested and committed; 179 tests green in debug
  and release, clippy and fmt clean. Five of §7's six criteria were settled by the
  test suite and `quant-verify`; the sixth — **7 consecutive days unattended** —
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

### Still open

- Nothing blocking on M1 or M2. **Next is M3** — the engine seam, `SimulatedVenue`,
  and a deliberately unimpressive moving-average crossover.
- **Two sessions covering one symbol-day are refused, not merged** (M2.d). Cannot
  happen on the acceptance capture, where each symbol ran one session for the whole
  week; it will the first time a recorder restarts mid-day. The merge is ordered by
  `local_recv_ts`, since `ingest_seq` cannot order across sessions.
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
- Rust edition 2021 on the **stable channel**, pinned by `rust-toolchain.toml`
  (the channel is pinned, not a version — whatever stable is when you build).
- **`rust-version` is the floor our dependencies impose, not an aspiration.**
  (Set 2026-09-02, 1.75 → 1.85.) Cargo's MSRV-aware resolver honours it when
  choosing versions, so a stale one is not free: at 1.75 it silently held us to
  `parquet` 54 from early 2025 while 59 was current — for a promise nothing tests,
  since CI runs `stable`. Raise it when a dependency we want requires it. Bumping
  it also un-blocks `clippy::incompatible_msrv`, which had been rejecting std APIs
  stabilised years ago (`Option::is_none_or`, 1.82).
