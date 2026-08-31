# Data Contract v1

**Status:** active · **Applies from:** M0 · **Owner:** the recorder

This document is the reason M0 exists. Recorded market data is the one asset
in this project that cannot be regenerated: a bug in the backtester costs an
afternoon, a bug in the recorder costs however many weeks of capture we did
before noticing. So the format is decided, written down, and tested *before*
the recorder is switched on.

---

## 1. The three tiers

| Tier | Contents | Format | Mutability | Lifetime |
|---|---|---|---|---|
| **Raw** | Exactly the bytes the venue sent, plus our receive timestamp and ingest sequence | Framed, length-prefixed, zstd-compressed | **Immutable, append-only** | Forever |
| **Normalized** | `MarketEvent` values derived from raw | Parquet, partitioned | **Disposable** | Rebuilt on demand |
| **Metadata** | Instruments, capture sessions, runs, orders, fills, backtest results | PostgreSQL | Mutable, transactional | Forever, backed up |

### Why raw is never parsed

The recorder does not interpret payloads beyond what it needs to route them.
It writes the venue's bytes verbatim. This costs disk and buys three things:

1. **Parser bugs become recoverable.** When we discover in month four that
   we mis-handled a field, we re-derive four months of Parquet overnight
   instead of losing four months.
2. **The contract can evolve.** Adding a field to `MarketEvent` does not
   require re-recording, because the source bytes still contain it.
3. **Disputes are settleable.** "Did the venue really print that?" has an
   answer.

### Why normalized is disposable

Because raw exists. Treating Parquet as derived state means we can change
partitioning, compression, column layout or even the storage engine
(→ ClickHouse) without a migration — we just rebuild. Anything that cannot be
rebuilt from raw does not belong in the normalized tier.

### Why metadata is Postgres and market data is not

Postgres is for data we JOIN, UPDATE and need transactions on: which
instruments exist, which capture session produced which file, which backtest
run used which config, which orders got which fills. That is thousands to
millions of rows with a relational shape — exactly what it is good at.

Market data is hundreds of millions of append-only rows scanned in ranges and
never updated. Putting it in Postgres works for about two weeks and then
teaches you a set of habits (row-at-a-time access, indexes on timestamps,
`SELECT *` over ticks) that have to be unlearned later. Columnar from the
start.

---

## 2. Timestamps

Every event carries **both**:

| Field | Source | Use |
|---|---|---|
| `exchange_ts` | The venue's payload | Venue-side analysis, latency measurement, cross-venue comparison |
| `local_recv_ts` | Our process, stamped once at ingress | **The only timestamp anything may act on** |

- `i64` nanoseconds since the Unix epoch, UTC. No local time anywhere.
- `local_recv_ts` is stamped as early as possible — immediately on read from
  the socket, before parsing — and never recomputed.
- `local_recv_ts - exchange_ts` is our observed venue latency. It is signed;
  a negative value means clock skew, which is an alert, not something to
  clamp to zero.

**The rule that matters:** the engine orders and dispatches events by
`local_recv_ts`, in every mode. Dispatching on `exchange_ts` in a backtest
lets a strategy react to information before it could have arrived. That is
lookahead bias, it is invisible, and it inflates every performance metric.
It is the most common reason a good-looking backtest loses money live.

---

## 3. Sequencing and completeness

Three independent counters, all recorded:

| Counter | Whose | Answers |
|---|---|---|
| `ingest_seq` | Ours, per (recorder process, instrument) | "Is this file complete and in order?" |
| `venue_trade_id` | The venue's | "Did we double-count this trade across a reconnect?" |
| `first_update_id` / `final_update_id` | The venue's | "Is our order book still correct?" |

We record the venue's counters rather than only our own conclusion about
them, so book correctness can be **re-verified offline** against recorded
data. A recorder that checks sequencing but does not persist the evidence
gives us no way to audit it later.

### Book snapshots are recorded, not derived

Binance's depth stream is incremental, and an incremental stream is meaningless
without a book to apply it to. That book comes from `GET /api/v3/depth`, and
that endpoint serves only the **current** state.

This is the single exception to "the recorder does not talk to anything but the
stream", and it exists for exactly the reason the raw tier exists. Every other
interpretation of the venue is recoverable — the bytes are still on disk, so we
re-derive. A snapshot *not taken* at the moment of a reconnect can never be
taken, and every delta after that reconnect is unanchored forever.

So the recorder fetches one on every (re)connect, and hourly while connected,
and writes the response body verbatim into the same file and the same
`ingest_seq` sequence as everything else. Two consequences worth being explicit
about:

- **The snapshot's position in the sequence is data.** It is fetched
  concurrently with draining the socket, so it lands *between* the deltas it
  arrived between — which is precisely what tells the book builder which deltas
  precede the anchor and are therefore stale.
- **Nothing is discarded at capture time.** Buffering deltas, dropping the
  stale ones, and checking that the update-id chain joins are steps in
  Binance's *local order book* algorithm. They are pure functions of bytes we
  are about to write down, so they belong to the normalizer (M2), where a
  mistake is fixed by re-deriving rather than by re-recording a week. The one
  irreversible step is the fetch; only that step is M1's.

Periodic snapshots are not an optimization. Without them a single lost delta
invalidates the book for the remainder of the session, permanently; with an
hourly anchor the book re-synchronizes at the next one. Same reasoning as
per-block compression: keep damage local.

A snapshot we wanted and could not get is recorded as such
(`SnapshotFailed{purpose, reason, attempts}`). An absent snapshot frame is
otherwise ambiguous between "the venue refused us" and "this build never
fetched snapshots", and those mean opposite things. It is deliberately *not* a
`Gap`: no messages were lost, and only a failed **resync** is serious — a
failed periodic snapshot costs a recovery point and nothing else.

### Gaps are events

`MarketEvent::Gap` is written whenever data is known to be missing:
`Disconnect`, `SequenceGap`, `LocalOverflow`, `RecorderRestart`.

This is the piece most homegrown recorders omit, and its absence is what
makes their data quietly untrustworthy. Without it a backtest cannot tell
"the market was silent for 40 seconds" from "we were blind for 40 seconds" —
and a strategy will cheerfully learn to trade the hole. With it, the
backtester can refuse to trade across a gap and report how much of the
sample it discarded.

`LocalOverflow` in particular must be recorded honestly. It means *we* could
not keep up. Suppressing it does not fix the capacity problem, it just
guarantees we find out about it in production.

---

## 4. Numbers

- Prices, quantities and notionals are `i64` scaled by `1e8`
  (`quant_core::fixed`).
- Venue decimal strings are parsed directly to fixed-point. **Never via
  `f64`.**
- Parse failures are loud. A malformed price means our model of the venue is
  wrong; that is a stop-and-look moment, not a default-to-zero moment.
- The workspace sets `clippy::float_arithmetic = "deny"`, so this is enforced
  by the build rather than by discipline.

---

## 5. Layout on disk

```
data/
  raw/
    exchange=binance/symbol=BTCUSDT/date=2026-07-26/
      session=<uuid>/part-00000.bin.zst
  normalized/
    trades/exchange=binance/symbol=BTCUSDT/date=2026-07-26/part-*.parquet
    book_deltas/...
    book_snapshots/...
    gaps/...
```

Partitioning by `exchange / symbol / date` is chosen because every query the
backtester makes is "this instrument, this date range". Hive-style
directories are readable by DuckDB, Polars, pandas, Spark and ClickHouse
without any of them being told about our schema — which keeps the research
side of the project free to use whatever tool fits.

One file per capture session, never appended across a restart, so a crashed
process can never corrupt a file another process is reading.

---

## 6. Versioning

`EVENT_SCHEMA_VERSION` is stamped in every raw file header.

Rules:

- **The normalizer must retain the ability to read every version it has ever
  written.** A migration that cannot read last year's capture is a data-loss
  event.
- Additive changes (new optional field) bump the version but stay readable by
  the previous reader.
- Breaking changes require a written migration note in this file *and* a
  successful re-derive of the full normalized tier before the old reader is
  removed.

### Migration notes

**Container v1 → v2** (M1.d). Adds frame kind `2 = VenueSnapshot` and the
`SnapshotFailed` control record. A v2 reader reads v1 files unchanged; a v1
reader refuses a v2 file at the header.

The version was bumped rather than the frame kind quietly added, even though
nothing but the kind byte changed, because the alternative is worse
diagnostically: a v1 reader would get most of the way through a perfectly good
file and then report `UnknownFrameKind`, which is indistinguishable from
corruption. Refusing at the header says what is actually wrong.

No re-derive was required, because no archival capture had been written when the
change landed — the format was still inside the window §7 exists to protect. That
window is now closing, and the next container change will need the full
procedure.

---

## 7. Acceptance criteria for M1

The recorder is not done when it prints JSON. It is done when:

- [ ] It runs **7 consecutive days** unattended.
- [ ] Killing the network mid-stream produces a `Gap{Disconnect}`, an
      automatic reconnect with backoff, and a fresh snapshot resync.
- [ ] Every recorded depth delta is preceded, somewhere in its session, by a
      book snapshot — or by a `SnapshotFailed` record saying why not. A capture
      whose deltas can never be turned into a book is not a usable capture,
      however complete it is.
- [ ] `SIGKILL` mid-write leaves the last file readable up to the last
      complete frame — no partial-frame corruption.
- [ ] Replaying every recorded file shows `ingest_seq` contiguous within each
      session, with every discontinuity explained by a recorded `Gap`.
- [ ] Replaying every recorded file shows the depth **update-id chain**
      contiguous — each message's `first_update_id` continuing the previous
      message's `final_update_id` — with every discontinuity explained by a
      recorded `Gap`.
- [ ] Recorder-side metrics exist for: messages/sec, bytes/sec, queue depth,
      venue latency percentiles, gap count by cause.

The last one is not optional polish. If we cannot see queue depth we cannot
tell the difference between a quiet market and a stalled consumer.

### How the metrics are reported

One structured log line per minute, plus a whole-run summary at shutdown:

```text
metrics symbol=BTCUSDT msgs_per_sec=37 bytes_per_sec=13070
        queue=0 queue_peak=18 queue_capacity=4096 dropped=0
        latency_p50_ms=41 latency_p90_ms=88 latency_p99_ms=140
        latency_samples=2276 clock_skew=0
        gap_disconnect=0 gap_overflow=0 gap_sequence=0
```

Rates are deltas between two readings, not totals, because a total that has
stopped growing looks exactly like one that never grew. Latency percentiles come
from a log-bucketed histogram — exact percentiles would need every sample, which
over seven days is hundreds of millions of values — with the reported value being
each bucket's **upper** bound, so a percentile never understates what was
observed.

The histogram is reported per interval and reset, so a degradation that begins on
day five is visible rather than averaged away across the week; a second,
never-reset histogram supplies the whole-run figure. Queue depth and its
high-water mark are cumulative for the opposite reason: a spike between two
readings must still show up in the next one.

**A latency figure is only meaningful if the host clock is.** `local_recv_ts -
exchange_ts` measures the clock difference just as much as the network, so the
recorder checks itself against the venue's `/api/v3/time` at startup and warns
above a one-second offset — the venue's own tolerance for a signed request. A
negative latency is counted separately as `clock_skew` and never folded into the
histogram, per §2.

### How the replay criteria are actually checked

```bash
cargo run -p quant-verify --bin verify -- <data root>   # exit 0, or a report
```

Every check has the same shape: **find a discontinuity, then ask whether the
capture already explains it.** An explanation is never inferred.

| Discontinuity | Explained by |
|---|---|
| a skipped `ingest_seq` | the frame *immediately after* the hole is a gap record |
| `U != previous u + 1` | any gap record between the two deltas |
| deltas with no book behind them | a snapshot anywhere in that connection episode, or a recorded `SnapshotFailed{Resync}` |
| no file trailer | being the last segment, i.e. still open or killed |

Two things about it are worth stating, because both were mistakes first.

The unit of verification is the **session**, not the file. `ingest_seq` spans a
session, so a hole straddling midnight is invisible to a per-file check — each
file is internally contiguous. The segments have to be joined in write order,
which is why the layout uses ISO dates and zero-padded parts: sorting the paths
as text *is* sorting them chronologically.

The anchoring check counts by **episode**, not by delta. Requiring a snapshot
before each delta flags every healthy capture, because the snapshot is fetched
concurrently with the drain and a handful of deltas legitimately arrive first —
Binance's algorithm discards the stale ones. A verifier that cries wolf on good
data is worse than no verifier.

Errors fail the run; warnings do not. A torn tail on a file still being written
is the archetypal warning, and making it fail would train everyone to ignore the
exit code.

`--reconcile` additionally cross-checks the metadata index, and is a flag rather
than the default because §1 makes raw the source of truth: a capture that could
only be validated with Postgres running would have inverted that.

### What is deliberately *not* an M1 criterion

Full **book reconstruction** — `best_bid < best_ask`, levels monotone, depth
invariants holding at every tick — belongs to **M2**, with the normalizer that
builds the book. It is not an M1 criterion, because M1 has no book: the
recorder writes bytes and never maintains order-book state.

The distinction is worth being precise about, because the two checks are not
the same strength:

- **Update-id chain contiguity** (M1) is verifiable from raw alone. It answers
  "did we receive every message the venue sent?" — a *completeness* property
  of the capture.
- **Book invariants** (M2) answer "does applying those messages in order
  produce a sane book?" — a *correctness* property of the reconstruction.

A capture can be complete and still reconstruct into a nonsense book if the
normalizer is wrong, and a book can look sane while built from a capture with
a silent hole in it. Attributing each check to the milestone that can actually
falsify it is what keeps "done" meaning something.

---

## 8. Acceptance criteria for M2

The normalizer is done when:

- [ ] Every frame in the acceptance capture parses, with no unrecognised event
      types and no malformed numbers. *(met: 384M price levels through
      fixed-point, zero failures.)*
- [ ] A replayed day holds the book invariants at **every tick**:
      `best_bid < best_ask`, no zero-quantity level retained, nothing
      non-positive. *(met per segment; see below for the remaining gap.)*
- [ ] A session's segments are joined, so the book carries across midnight
      rather than restarting unanchored at each UTC day boundary.
- [ ] The normalized Parquet tier is written, partitioned
      `exchange / symbol / date`, and a replay from Parquet agrees with a replay
      from raw.

### The normalized tier holds events, not books

`trades/`, `book_deltas/`, `book_snapshots/`, `gaps/` — per §5. A book is
**derived at replay time and never stored**. Storing one would mean a state per
delta, which for the acceptance capture is 11.6M states of thousands of levels
each, for something re-derivable in seconds. So the book-invariant criterion is
a property of the *reconstruction*, not an artifact to inspect.

### The three steps the recorder deferred

M1 captured the snapshot and did nothing else with it, because the fetch is the
only irreversible step. The rest lands here, where a mistake costs a re-derive:

1. **Buffer the deltas.** Not optional, and not free. The snapshot arrives
   *later in the stream* than the deltas it supersedes — the recorder fetches it
   concurrently with draining the socket, so messages land while the request is
   in flight. A replay that applies the snapshot and continues from the next
   frame skips every delta between the venue's `lastUpdateId` and the snapshot's
   arrival.
2. **Discard the superseded ones.** A delta whose whole range is at or below
   `lastUpdateId` is accounted for by the snapshot.
3. **Check the chain joins.** The first delta after a snapshot may *straddle*
   it; every one after that must continue exactly.

A snapshot the book has already moved past is **ignored**, not applied — the
recorder takes an hourly anchor whether one is needed or not, and applying a
stale one would move the book backwards.

### An invalidated book is cleared, not flagged

A flag can be ignored; an empty book cannot be misread as prices. That is what
makes "refuse to trade across a gap" (§3) enforceable rather than advisory.
