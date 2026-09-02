<title>Quant Platform Orientation</title>

# The project, end to end

**A narrative orientation**, written to be read start to finish. It is not the
source of truth for anything — where it summarises a decision, the authoritative
version is named. Read those when they disagree.

| Authority on | Lives in |
|---|---|
| The on-disk format and acceptance criteria | `docs/data-contract.md` |
| Per-slice design decisions and their reasoning | `CLAUDE.md`, and the module docs in each crate |
| The milestone table | `README.md` |
| How the 7-day run is conducted and judged | `docs/acceptance-run.md` |

*State as of 2026-09-02: **M0, M1 and M2 complete.** M1's acceptance run was
spent in full, 2026-08-21 to 2026-08-28 on an Apple Silicon MacBook Air, and passed. Both
weeks now replay as two joined 8-day sessions with book invariants holding at all 70.5M
ticks, and the normalized Parquet tier reproduces the raw stream event for event. Next is
M3, the engine seam. 18,641 lines of Rust across 8 crates, 237 tests passing in debug and
release.*

---

## 1. What we are building, and what we are not

A **quantitative trading platform**: infrastructure to research, test, and
eventually execute systematic trading strategies. The stated end goal is a
miniature quant firm — the same engine should later serve equities, futures and FX.

Binance is the first venue adapter because the infrastructure barrier is lowest:
no market data vendor contract, no exchange membership, no colocation. It is not
because the project is about crypto.

The organising principle everything else follows from:

> **The engine must outlive any strategy.** When the trade-off is between shipping
> a strategy sooner and keeping the platform correct, the platform wins.

That is why, nine milestones in, there is still no strategy. A strategy is a
hypothesis with a shelf life; a platform that can honestly evaluate hypotheses is
the durable asset. Most people build these in the opposite order and end up unable
to tell whether a result is real.

### What this is not

- **Not a bot.** No "buy when RSI < 30" logic anywhere, by design.
- **Not a moneymaker yet.** Nothing built so far says anything about whether any
  strategy has edge. That question is not even asked until M3.
- **Not fast.** Latency is not the edge being pursued. The system is built to be
  *correct and honest* first; a colocated low-latency system is a different project
  with different economics.

---

## 2. The one architectural idea

One strategy binary, three worlds, no code changes between them.

```
              ┌── HistoricalSource   (Parquet replay, as fast as possible)
EventSource ──┼── ReplaySource       (raw capture, wall-clock paced)
              └── LiveSource         (venue WebSocket)
                        │
                        ▼
                     Engine ────────►  Strategy
                        │
                        ▼
                    RiskLayer         (mandatory chokepoint —
                        │              not a module the strategy calls politely)
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

**A strategy must not be able to tell which pair it is wired to.** If it can, that
is a bug, not a feature.

### Why this matters more than it looks

The failure this prevents is the industry's most expensive routine mistake: a
backtest that describes a system which was never run. It happens when the
backtesting path and the live path are separate code. They drift — a fill
assumption here, a timestamp there — and the backtest slowly becomes fiction while
continuing to produce plausible-looking equity curves.

Forcing one code path means a discrepancy is a *compile error or a test failure*,
not a slow divergence nobody notices until real money is involved.

### Why risk sits between, not beside

`RiskLayer` is a chokepoint every order must pass through. The alternative — a risk
module the strategy is expected to consult — fails the moment one code path forgets
to consult it, and you cannot prove the absence of that. It is designed in at M3
and left **empty** until M6, because retrofitting a chokepoint means auditing every
call site that ever bypassed it.

---

## 3. The invariants

These are enforced by the compiler and the test suite, not by discipline. Each one
exists because of a specific, expensive failure mode.

### 3.1 Money is integral

`Px`, `Qty`, `Notional` are `i64` scaled by `1e8`. Venue decimal strings parse
straight to fixed-point, **never via `f64`**.

`clippy::float_arithmetic = "deny"` is set workspace-wide, so a stray float fails
the build rather than being caught in review.

*Why:* floating-point drift in position accounting does not announce itself. It
surfaces weeks later as a reconciliation mismatch against the exchange, at which
point you cannot tell which of ten thousand trades introduced it.

### 3.2 Two timestamps, and only one is dispatchable

Every event carries both:

- **`exchange_ts`** — what the venue said. For latency measurement and cross-venue
  comparison.
- **`local_recv_ts`** — when *our process* first saw the bytes. Stamped as early as
  possible, before parsing, never recomputed.

**The engine orders and dispatches on `local_recv_ts` only, in every mode.**

*Why:* dispatching on `exchange_ts` in a backtest lets a strategy react to
information at the moment the venue generated it, rather than the moment it could
possibly have arrived. That is lookahead bias. It is invisible, it inflates every
performance metric, and it is the single most common reason a good-looking backtest
loses money live.

### 3.3 Gaps are recorded events

`MarketEvent::Gap` is written whenever data is known to be missing:
`Disconnect`, `SequenceGap`, `LocalOverflow`, `RecorderRestart`.

*Why:* this is the piece most homegrown recorders omit, and its absence is what
makes their data quietly untrustworthy. Without it, a backtest cannot distinguish
"the market was silent for forty seconds" from "we were blind for forty seconds" —
and a strategy will cheerfully learn to trade the hole. With it, the backtester can
refuse to trade across a gap and report how much of the sample it discarded.

`LocalOverflow` in particular must be recorded honestly. It means *we* could not
keep up. Suppressing it does not fix the capacity problem; it guarantees you find
out in production.

### 3.4 Time is injected

Components take a `&dyn Clock`. Anything that calls the wall clock directly cannot
be backtested — treat it as a bug.

### 3.5 Parse failures are loud

A malformed price means our model of the venue is wrong. That is a stop-and-look
moment, not a default-to-zero moment.

---

## 4. Storage: three tiers, three different contracts

| Tier | Contents | Format | Mutability |
|---|---|---|---|
| **Raw** | Exactly the bytes the venue sent, plus our receive timestamp and sequence | Framed, zstd | **Immutable, append-only. Forever.** |
| **Normalized** | `MarketEvent` values derived from raw | Parquet, partitioned | **Disposable. Rebuilt on demand.** |
| **Metadata** | Instruments, sessions, runs, orders, fills, results | PostgreSQL | Mutable, transactional |

### Why raw is never parsed

The recorder writes venue bytes verbatim. This costs disk and buys three things:

1. **Parser bugs become recoverable.** When you discover in month four that a field
   was mis-handled, you re-derive four months of Parquet overnight instead of
   losing four months.
2. **The contract can evolve.** Adding a field to `MarketEvent` does not require
   re-recording, because the source bytes still contain it.
3. **Disputes are settleable.** "Did the venue really print that?" has an answer.

### Why normalized is disposable

Because raw exists. Treating Parquet as derived state means partitioning,
compression, column layout, or even the storage engine can change without a
migration — you just rebuild. **Anything that cannot be rebuilt from raw does not
belong in the normalized tier.**

### Why market data is not in Postgres

Postgres is for data you JOIN, UPDATE and need transactions on: which instruments
exist, which session produced which file, which backtest used which config. That is
thousands to millions of rows with a relational shape.

Market data is hundreds of millions of append-only rows scanned in ranges and never
updated. Putting it in Postgres works for about two weeks and then teaches you a set
of habits — row-at-a-time access, timestamp indexes, `SELECT *` over ticks — that all
have to be unlearned. Columnar from the start.

### Layout on disk

```
data/raw/exchange=binance/symbol=BTCUSDT/date=2026-07-29/
    session=<uuid>/part-00000.bin.zst
```

Hive-style `key=value` directories, because DuckDB, Polars, pandas, Spark and
ClickHouse all discover them by convention — which keeps the research side free to
use whatever tool fits, with no schema registration.

---

## 5. The milestone map

| # | Milestone | Done when | Status |
|---|---|---|---|
| M0 | Foundation + data contract | CI green; contract written before the recorder | **done** |
| M1 | Binance market data recorder | 7 days unattended, zero unexplained gaps | **done** |
| M2 | Normalizer + book reconstruction | Book invariants hold at every tick of a replayed day | **done** |
| M3 | Engine seam + SimulatedVenue + MA crossover | An equity curve exists, **and it is unimpressive** | |
| M4 | Fee, slippage, latency modelling | Results degrade sensibly under realistic costs | |
| M5 | Paper trading | 2 weeks live; P&L reconciles against an independent recompute | |
| M6 | Risk engine + kill switch | Limits provably veto a misbehaving strategy, under test | |
| M7 | Observability | "What was it doing at 03:14 last Tuesday?" answered in a minute | |
| M8 | Live, tiny capital | Live fills reconcile to the paper model within tolerance | |

Milestones have **acceptance criteria, not feature lists**. "Done" means the
criterion passes. Each is sized at roughly a week at 10–15 hours.

---

## 6. M0 — Foundation (complete, 2026-07-26)

`crates/quant-core` — 1,582 lines:

- **Fixed-point money**: `Px`, `Qty`, `Notional`, parsed from decimal strings
- **`Ts` and `Clock`**: nanosecond timestamps, injectable clock, `UtcDate`
- **Instrument registry**: definitions with venue filters (tick size, lot size,
  min notional), and `InstrumentId` as a `Copy` handle
- **The `MarketEvent` contract**: `Trade`, `BookDelta`, `BookSnapshot`, `Gap` —
  the narrow waist every component speaks through

Plus CI, `docker-compose` for Postgres, and **the data contract written before the
recorder existed**.

### The one decision that shaped everything after

Writing `docs/data-contract.md` first. Recorded market data is the only asset in
this project that cannot be regenerated. A bug in the backtester costs an
afternoon; a bug in the recorder costs however many weeks of capture happened
before anyone noticed.

So the format was specified, implemented, and tested **against synthetic bytes
before a socket was ever opened**. That is what makes the nastiest property — *"a
`SIGKILL` mid-write leaves the file readable up to the last complete frame"* — an
ordinary unit test that cuts a capture at every byte offset, rather than an
operational anecdote.

### A subtlety worth knowing

`InstrumentId` is a registry index, assigned in registration order, stable only
within one process. It must **never be persisted** — a stored id would silently mean
a different instrument the next time the registry was built in a different order.
Persist the `(exchange, symbol)` pair instead. The raw format enforces this: control
records deliberately do not carry an `InstrumentId`, and identity is reattached on
read from the file header.

---

## 7. M1 — The Binance recorder (complete; code 2026-08-11, run passed 2026-08-28)

Eight slices, each an independently shippable commit.

| Slice | Crate | What it bought |
|---|---|---|
| **a** | `quant-storage` | The raw container format: writer, reader, torn-tail recovery |
| **b** | `quant-recorder` | Ingress stamping, sequencing, bounded-channel overload policy |
| **b2** | `quant-binance` | WebSocket dialect, reconnect/backoff, stall detection, `record` binary |
| **c1** | `quant-recorder` | UTC day rolling: `CaptureSession`, `SegmentStore` |
| **c2** | `quant-meta` | Postgres index of sessions and segments |
| **d1** | `quant-binance` | REST depth snapshot capture, resync and periodic |
| **d2** | `quant-verify` | The offline verifier |
| **e** | `quant-recorder` | Metrics: throughput, queue depth, latency percentiles, gaps by cause |

Built in this order because **the raw tier is the only irreversible artifact**.

### 7.1 The container format (M1.a)

```
file := FileHeader Block* FileTrailer?

FileHeader (68 bytes)   magic, container version, event schema version,
                        exchange, session id, symbol, CRC32
Block                   sync "QRAB", uncompressed len, compressed len,
                        CRC32, zstd payload of Frame*
Frame (21-byte header)  kind, local_recv_ts, ingest_seq, payload_len, payload
FileTrailer (32 bytes)  sync "QEND", frames, blocks, last_ingest_seq, CRC32
```

**Compression is per ~256 KiB block**, not per frame or per file. Per-frame zstd
compresses a 200-byte depth message badly — no context to find redundancy in.
Whole-file streaming zstd compresses best, but then a truncated file is a truncated
*codec stream* and corruption anywhere destroys everything after it. Blocks get
nearly all the streaming ratio while confining damage. Same reason Parquet has row
groups and Kafka has record batches. Achieved: **~7× on live Binance data.**

**Frames are typed**, because a synthesized `Gap` is not venue bytes. It still has
to live in the same file and the same sequence, because a gap record in a sidecar
file is a gap record that can disagree with the stream it describes.

**A torn tail is a report, not an error.** After a `SIGKILL` it is the file's
expected shape. But a checksum failure is flagged separately by
`TruncationReason::is_corruption()` — a crash is routine, a lying disk is not.

**The trailer earns its 32 bytes.** Without one, the strongest completeness claim
available is "I read to the end and nothing was torn" — and that is weaker than it
sounds, because a file cut exactly on a block boundary is byte-identical to one that
ended there deliberately. With a trailer the claim becomes "I read 4,318,221 frames
and the writer states it wrote 4,318,221": an independent two-sided check. It lives
*in the file* rather than in Postgres deliberately, because otherwise validating a
capture would require the database and raw would stop being self-describing.

### 7.2 Ingress and backpressure (M1.b)

```
  socket                    bounded channel                    disk
 ┌────────────────────┐    ┌───────────────┐    ┌──────────────────────┐
 │ read task (async)  │    │               │    │ writer thread        │
 │  stamp recv_ts     │───▶│  capacity N   │───▶│  frame + zstd + IO   │
 │  assign ingest_seq │    │               │    │  flush on a timer    │
 │  never blocks      │    └───────────────┘    └──────────────────────┘
 └────────────────────┘
```

**Why two halves.** The tempting design is one loop that reads the socket and writes
the file. It is wrong, and not subtly: framing and zstd-compressing a block is
*blocking work of unbounded duration* — a slow disk, a page-cache flush, an
antivirus scan. Any time spent in it is time the socket is not being drained. Stop
draining a TCP socket and the receive window closes; keep it closed and Binance
disconnects you for being a slow consumer. **So the venue punishes you for your disk
being busy**, which is an absurd coupling to accept.

**Why the channel is bounded.** An unbounded channel does not remove overload, it
converts a bounded, visible, recordable problem into an unbounded invisible one:
memory grows until the OOM killer arrives, typically an hour later, taking the whole
capture with it — and nothing in the data would say why.

**Why a full channel drops rather than blocks.** Blocking would restore exactly the
coupling the split exists to break, *and* be dishonest: the data would show no gap
at all, because everything was "successfully" recorded — just late, with
`local_recv_ts` values reflecting our own stall rather than the venue's timing.

**Why holes carry the count.** A sequence number is assigned to every message that
arrives, whether or not it survives the channel. So a drop leaves a hole, and the
hole's width *is* the number of messages lost. That is why the gap record has no
count field and needs none: one record marks the event, the sequence arithmetic
supplies the magnitude.

**Why gap records are never dropped.** Venue messages are droppable because we can
describe their absence. A gap record *is* the description. So it is held and retried
— which terminates, because every situation that generates one is a situation where
inflow has stopped.

### 7.3 The venue adapter (M1.b2)

**One WebSocket connection per symbol**, not a combined stream. Routing a
multiplexed stream means a JSON parse per message on the hot path just to discover
which symbol it belongs to — so the read task, whose entire job is to not be the
bottleneck, would be parsing. Per-symbol sockets make the socket *be* the route,
isolate failure, and keep `ingest_seq` naturally per-instrument. Revisit past ~50
symbols where connection rate limits bite.

**An idle timeout (120 s), because** the failure that ruins a 7-day run is not a
socket that closes — that is noticed immediately — but one that stays open and stops
delivering. A half-open TCP connection after a network path change looks perfectly
healthy from our side.

**Backoff jitter is seeded per symbol**, so a venue restart does not make every
connection retry in lockstep.

**`@trade` not `@aggTrade`; `depth@100ms` not `@depth`.** Aggregated trades merge
fills that happened at the same price against the same resting order, destroying
exactly the microstructure detail order-flow work needs. `@depth` alone means
1000 ms and would coalesce ten times as much book movement per message. **Detail
the venue never sent cannot be recovered from raw capture later** — unlike a parsing
mistake, which can.

### 7.4 Day rolling (M1.c1)

Files roll at the UTC boundary, driven by **the record's own `local_recv_ts`**, not
the writer's clock. The writer runs behind by design, so rolling on its clock would
file a 23:59:59.9 message under the next day whenever the disk happened to be busy —
making the partition a property of our load rather than of the data.

The exception is an **idle stream**: timestamp-driven rolling alone leaves
yesterday's file unsealed until the next message, and a trailerless file reads as
"killed". So a healthy quiet symbol would look crashed. A clock-driven check on the
flush tick closes it.

Rolling is **forward-only**. A record stamped before the open day (an NTP step) goes
in the open segment and increments `backdated_records` rather than reopening a
sealed day.

`ingest_seq` spans the **session**, not the file — so a hole straddling midnight is
still a hole. That single choice is why the verifier's unit of work is the session.

### 7.5 The metadata tier (M1.c2)

Postgres holds one `capture_sessions` row per run and one `capture_segments` row per
sealed file.

**It is best-effort and optional.** No `QUANT_DATABASE_URL`, or a database that will
not answer, logs a warning and records anyway. Market data is irreplaceable and an
index row is not, so nothing in the metadata path can fail the recorder.

**The paired obligation:** everything stored must be reconstructible from raw, or
Postgres quietly becomes the source of truth for it.

Segment rows are reported over a bounded channel with `try_send`, **never inline on
the writer thread** — a hung DB connection would otherwise stall block sealing, back
up the capture channel, and drop market data. Letting a secondary concern damage the
primary one is exactly the failure to design out.

### 7.6 Book snapshots (M1.d1)

The plan said: *buffer deltas → REST snapshot → discard stale deltas → verify the
chain joins → resume.* That is Binance's **local order book algorithm**, and the
recorder has no order book. Sorting each step by which milestone can actually
falsify it leaves exactly one capture-time obligation:

| Step | Belongs to | Why |
|---|---|---|
| buffer deltas | M2 | The file already *is* the buffer, and a durable one |
| **fetch the snapshot** | **M1** | **Irreversible.** The venue serves only the book's *current* state |
| discard stale deltas | M2 | A pure function of raw; discarding destroys evidence |
| verify the chain | M1, offline | The update ids are already in the bytes |
| resume | — | The recorder never stopped |

So M1.d1 does the fetch and nothing else. A snapshot not taken at a reconnect can
never be taken, and every delta after it stays unanchored forever. Everything else
is re-derivable, and belongs where a mistake costs a re-derive instead of a week of
re-recording.

**The fetch is concurrent with the drain**, not before it. Awaiting it first would
stop reading the socket for a round trip. Fetching before the subscription is live
would leave an unbridgeable hole. Concurrency also buys the property that matters
for free: **the snapshot's position in the sequence is the information** — it lands
between the deltas it arrived between, which is what tells a book builder which
deltas are stale.

**Hourly snapshots are not an optimization.** Without them a single lost delta
invalidates the book for the remainder of the session, permanently. With an hourly
anchor it re-synchronizes at the next one. Same "keep damage local" reasoning as
per-block compression. Cost: ~320 KB/hour against several hundred MB/day of deltas.

**A dropped snapshot consumes no sequence number**, unlike a dropped message. A hole
means "the venue sent something and we lost it"; a snapshot we fetched ourselves lost
no stream data, so a hole would manufacture evidence of a drop. And the remedy is to
**re-fetch, not retry the bytes** — a gap record must keep its original timestamp,
but a snapshot's whole value is being current.

### 7.7 The verifier (M1.d2)

`quant-verify` turns §7's acceptance criteria from claims into a command with an exit
code. **One design idea: find a discontinuity, then ask whether the capture already
explains it. Never infer an explanation.**

| Discontinuity | Explained by |
|---|---|
| a skipped `ingest_seq` | the frame *immediately after* the hole is a gap record |
| `U != previous u + 1` | any gap record between the two deltas |
| deltas with no book behind them | a snapshot anywhere in that connection episode, or a recorded `SnapshotFailed{Resync}` |
| no file trailer | being the last segment, i.e. still open or killed |

The first is precise for a reason: a dropped message burns its sequence number at
ingress and the gap record is written by the *next* successful enqueue, so on disk
the gap frame sits immediately after the hole. Checking merely that "a gap exists
somewhere in the session" would pass a file with one disconnect and a thousand
silent holes.

**Findings are capped** at 5 per (code, session) with the remainder counted. One
systematic defect over hundreds of millions of frames would otherwise bury every
other finding — the failure mode where a verifier is worse than none. The count is
never dropped, because "and 3,201,884 more" is what says *systematic* rather than
one-off.

**Warnings do not fail a run.** A torn tail on a file still being written is normal,
and failing on it would train everyone to ignore the exit code.

**No venue-abstraction trait, deliberately** — and against this project's usual
instinct. Three of four checks need only the container format; the update-id chain
needs Binance knowledge, so that lives in `quant-binance::sequence`. A trait designed
from one implementation encodes one venue's assumptions and calls them universal;
extracting it from two real ones later gives a better trait. A capture from an unknown
venue is **reported as unchecked**, which is the part that matters.

### 7.8 Metrics (M1.e)

One structured line per minute per symbol:

```
metrics symbol=BTCUSDT msgs_per_sec=37 bytes_per_sec=13070
        queue=0 queue_peak=18 queue_capacity=4096 dropped=0
        latency_p50_ms=41 latency_p90_ms=88 latency_p99_ms=140
        latency_samples=2276 clock_skew=0
        gap_disconnect=0 gap_overflow=0 gap_sequence=0
```

The contract puts the case plainly: *"if we cannot see queue depth we cannot tell the
difference between a quiet market and a stalled consumer."* Both look identical from
outside — no output, no errors, a process sitting there. One is a Sunday morning and
the other is losing data.

Read in this order: **`dropped`** (should always be 0), **`queue`/`queue_peak` against
capacity**, **`clock_skew`**, **`gap_disconnect`** (a few a day is Binance behaving as
documented), then the percentiles — where a step change matters more than the value.

Latency percentiles come from a hand-rolled log-bucketed histogram (16 sub-buckets per
octave, ~6% error, pinned by a test) rather than a dependency, because it is forty lines
and a metric that will drive an alert is worth understanding exactly. Reported values are
each bucket's **upper** bound so a percentile never understates — clamped to the observed
maximum, so it also never exceeds it.

---

## 8. What running it taught us

Every one of these was found by *operating* the system, not by testing it. Each
generalises.

### The recorder wrote nothing

It connected, received data for forty seconds, and wrote a 68-byte header and nothing
else. The flush policy sealed a block after N seconds of **silence** — but a steady
trickle never goes silent, and at ~4 KB/s it also never reaches the 256 KiB threshold.
Fixed by bounding **block age** instead.

> **A timeout on the wait is not a timeout on the work.**

### The verifier's first act was to find a bug in itself

296 errors against captures known to be good. The rule "every delta must be preceded by
a snapshot" is *wrong*: the snapshot is fetched concurrently with the drain, so a handful
of deltas always arrive first — and Binance's algorithm discards deltas with
`u <= lastUpdateId` and bridges the straddler. A snapshot anchors the deltas *around* it.
The correct unit is the **episode**: one connection's worth of stream.

> **A verifier that cries wolf on good data is worse than no verifier. When it fires,
> check the rule before the data.**

### The latency metric found a broken clock

p50 "venue latency" read 2883 ms, which is absurd for Binance. The metric was right; the
host was ~2 s fast with its time service stopped. Hence a startup check against the
venue's own clock, warning above 1000 ms — Binance's own tolerance for a signed request,
so the clock this warns about is the same clock that would get an order rejected at M8.

> **A number that looks broken is broken, whatever its derivation says.**

### A multi-threaded test caught a real race

The queue-depth counter incremented *after* handing the record to the channel, so the
writer thread could receive and decrement first — taking an unsigned counter below zero,
wrapping it to `usize::MAX`, and panicking on the next increment. That would have killed
the recorder.

> **A metric must never be able to take down a capture.**

### Two shutdown and concurrency bugs

The recorder hung at shutdown because an observer closure held a channel sender clone, so
`recv()` never saw the channel close — all data written correctly, process never exited.
And concurrent `migrate()` calls raced between "which versions are applied" and "apply
this one".

> **With channel-driven shutdown, enumerate every holder of a sender.**

---

## 9. What is left

### M2 — Normalizer + book reconstruction *(complete)*

**Criterion:** book invariants hold at every tick of a replayed day —
`best_bid < best_ask`, no zero-quantity levels retained, nothing non-positive.

Distinguish the two checks carefully: **update-id contiguity** (M1) answers "did we
receive everything the venue sent" — a *completeness* property of the capture.
**Book invariants** (M2) answer "does applying those messages in order produce a sane
book" — a *correctness* property of the reconstruction. A capture can be complete and
still reconstruct into nonsense if the normalizer is wrong.

| | Slice | State |
|---|---|---|
| a | `quant-binance::parse` — payloads to events, in fixed-point | **done** |
| b | `quant-book` — apply, invariants, gap invalidation, resync | **done** |
| c | `quant-normalize` — join a session's segments, replay, report | **done** |
| d | Parquet output — the normalized tier on disk | **done** |

**The scoping call.** The milestone reads "normalizer *and* book reconstruction", but
those are two artifacts. The normalized tier holds **events, not books** — the
contract's layout is `trades/`, `book_deltas/`, `book_snapshots/`, `gaps/`. Storing
books would mean a state per delta, 11.6M of them with thousands of levels each, for
something re-derivable in seconds. So the criterion is a property of the
*reconstruction*, validated by replaying, not an output on disk.

#### What the parser decided

Written against payloads copied **verbatim out of the acceptance capture**, not from
the venue's documentation — the dialect actually being spoken is the one worth being
correct about.

The **aggressor mapping inverts the venue's flag**. Binance sends `m`, "is the buyer
the market maker", so `m: true` means a resting buyer was lifted and the *seller*
crossed the spread. Backwards, this silently inverts order-flow imbalance — and a
signal with the wrong sign looks *predictive* rather than broken, which is the worse
failure.

`exchange_ts` comes from `E`, the message emission time, not a trade's `T`. `E` keeps
`local_recv_ts - exchange_ts` a transport measurement; `T` would fold Binance's own
match-to-publish delay into what we call network latency.

There are now **two parsers for one dialect** — a narrow one for the verifier walking
70M frames, and this full one. That is a genuine drift risk, bounded by a test that
pins them to classify the same bytes the same way.

Validated on the corpus rather than on fixtures: every frame of all 16 segments, 58.9M
trades, 11.6M deltas, **384 million price levels through fixed-point, zero failures**.

#### The finding that made the slice worth it

M1 deliberately deferred three steps to here: buffer the deltas, discard the ones the
snapshot supersedes, and check the chain joins. I wrote in the book's own docs that
the first was already handled — *"the capture file **is** the buffer"*.

That is true about the file and **false about the algorithm**. The snapshot arrives
*later in the stream* than the deltas it supersedes, because the recorder fetches it
concurrently with draining the socket, so a few hundred milliseconds of messages land
while the REST request is in flight. A replay that applied the snapshot and continued
from the next frame produced:

```
applied   0 deltas          discarded 411,422 unanchored
chain     12 breaks         on a file the verifier had passed clean
```

So the book buffers while it has no anchor and replays the buffer once it gets one —
and the buffering lives **in the book, not the caller**, because a caller who has to
remember will forget. The queue is bounded, because a snapshot may never arrive at all.

Fixing it surfaced a second case by the same argument: **a snapshot the book has
already passed is ignored**. The recorder takes an hourly anchor whether the book needs
one or not, so by the time one is written the live stream is further ahead than the
`lastUpdateId` the venue served; applying it would move the book backwards. Eleven of
the twelve snapshots in a day are this case — which is also why periodic anchors are
not wasted: their value is for the book that *has* been invalidated.

After the fix, all 16 segments replay with **0 chain breaks, 0 invalidations, and the
invariants holding at every tick**. Two independently written checks agreeing: the
verifier says the chain is contiguous-or-explained, and the book produces zero broken
outcomes.

It also disproved a sentence in the verifier. `deltas_before_anchor` was described as
*"recorded, but not reconstructible"* — they are in fact either superseded by the
snapshot or replayed after it, and both halves are needed.

#### Joining the days

Replaying files one at a time left a known artifact: days 2 through 8 dropped between
7,000 and 34,000 deltas apiece. Not a defect — each file starts mid-stream with no
anchor and waits up to an hour for the next periodic snapshot. Day 1 dropped none,
because its reconnect snapshot lands 300 ms in.

A UTC day boundary is a **filing decision**. `ingest_seq` spans a session and so does
the venue's update-id chain, so crossing from one day's file into the next must not
reset anything. `quant-normalize` joins them, and the artifact disappears: both weeks,
both symbols, **0 deltas dropped for want of an anchor**, 0 chain breaks, invariants
holding at all 70.5M ticks.

Three decisions inside it are worth keeping.

**The replay is an iterator of events, not a program that checks a book.** The book is
the consumer that happens to exist first; the Parquet writer at M2.d and the engine's
historical and replay sources at M3 want the identical stream. Written the other way,
both would have to take it apart again to get at the events.

**The archive's own discontinuities are items in that stream.** A segment that will not
open, a torn tail, a hole in `ingest_seq` — each means what follows does not continue
what came before, and skipping quietly to the next readable frame would produce a book
that *looks* continuous across data we never read. They are `Break`s and not
synthesized `Gap`s, because `GapCause` is persisted in the immutable tier and describes
the venue or the recorder, while "I could not read this file just now" describes one
replay on one machine.

**Session discovery moved down into `quant-recorder`.** The verifier had owned it since
M1.d2, and nothing may depend on the verifier. Duplicating the fifteen lines would have
been easy and wrong: if the two ever disagreed about which files form a session, the
normalizer would replay a different stream than the one the verifier passed clean, and
nothing downstream could notice. The two tools now share the walk and share nothing
else — which is why it means something that they independently agree on 70,545,346
frames.

`quant-verify` and `quant-normalize` are kept apart in both directions on purpose. The
verifier asks *is this capture complete*; the normalizer asks *what did the market do*.
A capture with an honest recorded gap is complete and still goes dark for a while, so a
tool that conflated the two would have to call one of them a failure.

#### The Parquet tier, and what makes "disposable" true

The storage-tier table has said since M0 that Normalized can be deleted and
rebuilt from raw. That was a promise about a derivation nobody had checked.
`normalize --check` checks it: 2.1 GB of Parquet from 3.0 GB of raw, and
**70,545,345 events replayed from Parquet match the raw replay event for event**.

Event by event, not by summary. Two reconstructions can produce identical book
statistics from different events — a transposed pair of timestamps, a level moved
from one delta to the next, an aggressor flipped on a trade later cancelled out.
Comparing summaries passes every one of those.

Four encoding decisions carry the weight.

**Money is `DECIMAL(18,8)`, not `INT64`.** The bytes are identical — Parquet backs
a decimal of precision ≤ 18 with an int64 — but the decimal carries the scale in
the schema. Invariant 1 says money never goes through `f64`, and until now that
held only inside our own process; a bare int64 makes every reader responsible for
knowing the `1e8` convention out of band, and the first that reads it as a double
loses precision silently. The column type is how the invariant crosses the process
boundary. The bound it imposes is enforced rather than assumed: a value past
`10^10` is a loud error on write.

**A file names the session it came from.** The layout has no session dimension, and
should not — this tier is about what the market did. But a recorder restart makes a
new session, and two sessions can cover one symbol-day; publishing the second over
the first would lose a day and leave a file that looks complete. Each file carries
its source session in the Parquet footer and the writer refuses a partition another
session owns. Merging two sessions into one day is the eventual answer; refusing is
what makes deferring it safe rather than lossy.

**The partition day is inherited from raw, not recomputed.** The first version
re-derived it from `local_recv_ts` with a "never backwards" clamp — reimplementing
the recorder's M1.c1 rule, and wrong in exactly the case that rule exists for.
The tests caught it.

**A break stops the write.** The book can survive a discontinuity by clearing; a
file cannot, because a partition written across one looks continuous forever after.
Raw is the source of truth and this tier is disposable, so the answer is to stop and
re-derive. The exception is a torn tail on the last segment — the ordinary signature
of a killed recorder.

### M3 — Engine seam + SimulatedVenue + a deliberately bad strategy

**Criterion:** an equity curve is produced, **and it is unimpressive.**

That criterion is not a joke. A moving-average crossover that looks profitable on first
run means the harness is lying — lookahead bias, survivorship, or missing costs. The
unimpressive equity curve is the *evidence* that the plumbing is honest. This is also
where the `RiskLayer` chokepoint is designed in, empty.

### M4 — Fee, slippage and latency modelling

**Criterion:** results degrade sensibly under realistic costs.

Most naive strategies are profitable before costs and unprofitable after. The purpose is
to make that visible early, before anyone becomes attached to a result.

### M5 — Paper trading

**Criterion:** two weeks live, and P&L reconciles against an independent recompute.

Live prices, simulated fills. The reconciliation matters more than the P&L: two
independent calculations agreeing is evidence; one calculation is an assertion.

### M6 — Risk engine + kill switch

**Criterion:** limits provably veto a misbehaving strategy, under test.

The chokepoint designed at M3 gets filled in: max order notional, max position, max daily
loss, kill switch. "Provably, under test" means a deliberately misbehaving strategy is
part of the suite.

### M7 — Observability

**Criterion:** "what was it doing at 03:14 last Tuesday?" answered in a minute.

The metrics from M1.e were the miniature version. This is the full story: structured
logs, metric export, run/order/fill history queryable.

### M8 — Live, tiny capital

**Criterion:** live fills reconcile to the paper model within tolerance.

Note what this criterion is *not*. It is not "makes money". It is a measurement of whether
the simulation was honest. See §11.

---

## 10. Where things actually stand

### Proven

- 214 tests passing in debug and release; clippy and fmt clean; CI green on Linux and
  Apple Silicon
- **Seven days unattended, and it passed.** See below.
- Corruption is detected and distinguished from a torn tail (verified by flipping a byte)
- The metadata index agrees with the files' own trailers, and disagreement is caught
- The run harness works end to end: supervise, verify mid-capture, self-terminate, seal
- **The week reconstructs.** Both 8-day sessions replay as one joined stream each, with
  book invariants holding at all 70.5M ticks, 0 chain breaks and 0 deltas dropped for
  want of an anchor — and `quant-normalize` and `quant-verify` independently agree on
  70,545,346 frames without sharing any counting code
- **The normalized tier is a re-derivation and not a second source of truth.**
  70,545,345 events replayed from Parquet match the raw replay event for event

### The acceptance run

Two symbols on a fanless M1 MacBook Air, 2026-08-21 → 2026-08-28.

| | |
|---|---|
| Frames | 70,545,346 — 11.6M depth deltas, 58.9M trades, 342 snapshots |
| Volume | ~2.9 GB compressed from ~23.5 GB of raw venue bytes |
| Messages dropped | **0** — queue peaked at 728 of 4096, never close |
| Sequence holes | **0** unexplained; `missing 0` |
| Gap frames | 38, every one explained: 36 `Disconnect` + 2 `RecorderRestart` |
| Restarts | **0** — one process per symbol, `exited cleanly after 604776s` |
| Errors | 0. One warning: a resync snapshot that timed out on day 3 |
| Verdict | `quant-verify` exit 0 |

The single warning is worth dwelling on, because it is the design working. A resync
snapshot timed out four times on flaky Wi-Fi. Rather than leaving a silence that could
never be told apart from a build that never fetched snapshots, the recorder wrote a
`SnapshotFailed` record saying so — and a warning does not fail a run.

The whole capture was then moved off the Mac and **independently re-verified** on
different hardware: exit 0 again, same frame count, zero errors. Two details from that
second check were themselves evidence:

- The `no-trailer` warnings the Mac's final in-run check had reported were **gone**. Those
  files were still open at the time; the clean exit sealed them. That is exactly the
  distinction the trailer exists to make, observed across a real transition.
- The far-side frame count came out *higher* than the Mac's last report — 70.5M against
  68.98M — because that check ran 5.75 hours before the run ended.

### Not proven

1. **Nothing at all is known about strategy edge.** No strategy exists. That question is
   not asked until M3, and not answered honestly until M4.
2. **The slippage model has no validation path at retail size.** Every order at $100 or
   $500 fills at top of book on a liquid pair, because top-of-book depth is tens of
   thousands of dollars. So the M4 model stays *modelled and unvalidated* until size
   grows — which is fine, because at sizes where slippage is unmeasurable it is also
   economically irrelevant. Worth writing down rather than discovering later.

### Honest proportion

By milestone count this is two of nine. By risk it is further along than that: the
irreversible part — the format that all future data is written in — is done, tested, and
proven over seven days of live capture, in the order where mistakes were cheapest. What
remains is mostly work where a mistake costs a re-derive.

---

## 11. Capital: how much, and when

**Not yet, and less than you would think.**

Capital becomes relevant only at **M8**, and M8's criterion is *reconciliation*, not
profit: do live fills match what the paper model predicted? That is a measurement, and
measurements do not get more accurate with more money at risk.

### Sizing for M8

The account needs to be:

- **Large enough that `min_notional` is not the binding constraint.** Binance spot requires
  roughly $5–10 per order on major pairs. If your position size is pinned at the exchange
  minimum, you are testing the minimum rather than your sizing logic.
- **Large enough that fees and slippage are real numbers**, not rounding. At 0.1% taker,
  the cost model only gets exercised if there is something to take a percentage of.
- **Small enough to lose entirely without it mattering.** At M8 the correct assumption is
  that a bug market-buys everything at the worst possible moment.

Those constraints land around **$200–500 equivalent** — roughly 20–50× the exchange minimum.
Enough for position sizing to be a real decision; small enough that total loss is a
disappointing evening, not a financial event.

### The number that matters more than the balance

The account size is the *least* interesting risk parameter. These matter more, and they are
what M6 exists to enforce:

- **Max notional per order** — bounds a single fat-finger or logic error
- **Max open position** — bounds accumulated exposure
- **Max daily loss, with a kill switch** — bounds a bad day, and stops a strategy that has
  started behaving differently from the one you tested
- **Max order rate** — bounds a runaway loop, which is how people discover their fee tier

A $300 account with a $50 per-order cap and a $30 daily-loss kill switch is a *better test*
than a $5,000 account with no limits, because it exercises the machinery that will still be
load-bearing at any size.

### Scaling afterwards

Do not scale on the strength of a backtest. Scale on the strength of **paper and live
agreeing** over a meaningful period — which is precisely why M5 runs two weeks and M8
reconciles rather than measures return.

And when you do consider real size, the binding constraints will be things none of the work
so far has touched: strategy capacity (how much size the edge survives), drawdown tolerance
(a naive crypto strategy can draw down 30–50%), and the plain fact that a systematic
strategy with no live track record is an unproven hypothesis regardless of how good the
infrastructure around it is.

> Only ever commit money you can lose completely. Nothing built or measured so far
> constitutes evidence that any strategy here will be profitable — and the platform is
> deliberately designed to tell you honestly when it is not.

---

## 12. Working on it

```bash
docker compose up -d                    # Postgres (metadata only)

cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features

# record, then verify
cargo run -p quant-binance --bin record -- BTCUSDT data 60
cargo run -p quant-verify  --bin verify -- data --reconcile

# inspect a single file (--sample N prints whole payloads)
cargo run -p quant-storage --example dump -- <path to part-00000.bin.zst>

# M2: parse every frame in a file; replay whole sessions through a book
cargo run --release -p quant-binance   --example parse_all -- <path to part-*.bin.zst>
cargo run --release -p quant-normalize --bin normalize     -- data
```

Warnings are errors in CI. Commits explain **why** in the body — the rationale is the
point, because the code shows the what.

### The crates

| Crate | Lines | Knows about |
|---|---|---|
| `quant-core` | 1,747 | Money, time, instruments, the event contract. No I/O. |
| `quant-storage` | 2,702 | The raw format. No venue, no network. |
| `quant-recorder` | 4,003 | Ingress, overload policy, day rolling. Venue-agnostic, async-free. |
| `quant-binance` | 2,477 | The only crate that knows a venue. |
| `quant-book` | 894 | Book reconstruction and its invariants. Depends only on `quant-core`. |
| `quant-meta` | 1,075 | Postgres. Sits above the recorder; optional. |
| `quant-verify` | 2,056 | Near the top. Asks whether the capture is complete. Nothing may depend on it. |
| `quant-normalize` | 3,687 | Near the top. Asks what the market did, and writes the normalized tier. |

The dependency arrows only point one way. That is checked by the fact that adding a second
venue should mean writing a new adapter and touching nothing else.
