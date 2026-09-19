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
    exchange=binance/symbol=BTCUSDT/date=2026-07-26/
      trades/part-00000.parquet
      book_deltas/part-00000.parquet
      book_snapshots/...
      gaps/...
```

The dataset is the **innermost** directory, below the partition keys, so both
tiers share the same `exchange / symbol / date` prefix and line up directory for
directory — which is the only cheap cross-check between them, and the reason §8
inherits a record's day from raw rather than re-deriving it.

A normalized day holds one **part per contributing capture session**: usually
just `part-00000`, and more only where a recorder restarted inside the day. Parts
are concatenated in **index order**, and what makes that order trustworthy is a
check when a part is published rather than a sort when the day is read: a part
reaches its real path only if the venue's update-id span recorded in its Parquet
footer begins after every already-published part's span ends. §8 has why a file
boundary is the right encoding of a restart, why that span is the only key that
can be trusted, and what the check refuses rather than reorders.

Partitioning by `exchange / symbol / date` is chosen because every query the
backtester makes is "this instrument, this date range". Hive-style
directories are readable by DuckDB, Polars, pandas, Spark and ClickHouse
without any of them being told about our schema — which keeps the research
side of the project free to use whatever tool fits.

One file per capture session **per UTC day**, never appended across a restart, so
a crashed process can never corrupt a file another process is reading. A session
that runs a week therefore leaves seven files and not one, and `ingest_seq` spans
the **session** rather than the file — which is why a hole straddling midnight is
invisible to a per-file check, and why §7's replay criteria join a session's
segments before they go looking for one. The `part` index in a *raw* file name
means something different from the one in a normalized day: it exists for the
size-based rolling that has never been needed, and today it is always `00000`.

**Which file a record lands in is decided by the record's own `local_recv_ts`,
and rolling is forward-only.** The writer runs behind the socket by design, so
rolling on the *writer's* clock would file a 23:59:59.9 message under the next
day whenever our disk happened to be busy — making the partition a property of
our load rather than of the data. Two clauses follow from that. An idle stream
still has to be sealed on a timer, because a timestamp can only roll a day when
a message arrives, and a trailerless file reads as "killed": a healthy quiet
symbol must not look crashed. And a record stamped *before* the open segment's
day — the wall clock stepping backwards across midnight, an NTP correction — is
written to the open segment and counted as `backdated_records`, never by
reopening a sealed day. The record still tells the truth about itself; only the
directory it can be found in is off, and a non-zero count is the signal to go
and look at the host's clock.

**That rule was cited twice in §8 before it was ever stated here.** Both
citations pointed into this document — first at §6, then at §5 — in each case at
a section that said nothing about rolling at the time. That is how a wrong
reference survives: it has the shape of a pointer, so nobody follows it. The
rule is implemented in `quant-recorder`'s `segment.rs` and narrated in
`docs/overview.md` §7.4, but it decides which file a byte is filed under, which
makes it a statement about data at rest and so this contract's business. §8's
two references now point at the paragraph above.

---

## 6. Versioning

**Two** versions are stamped in every raw file header, and they are independent.
`CONTAINER_VERSION` versions the file format — framing, block layout, which frame
kinds exist — and is currently **2**. `EVENT_SCHEMA_VERSION` versions the event
vocabulary and is currently **1**.

They are separate because a new frame kind and a new field on a `Trade` are not
the same kind of change. Conflating them would mean bumping a number that makes
older readers refuse a whole file for a change they could have read straight
through — which is the opposite of the diagnostic argument the v1 → v2 note below
makes.

The rules apply to both:

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
change landed — the format was still inside the window §7 exists to protect.
**That window is shut.** The acceptance week by itself is 3.0 GB of v2 capture
that nobody can re-record, so the next container change owes the full procedure:
a migration note here, and a successful re-derive of the whole normalized tier
before the v2 reader is removed.

This used to say "a week of acceptance capture **and a fortnight of paper
session** on disk", and the second half was not true when it was written. The
paper run started 2026-09-18T14:46:54Z and ends 2026-10-02; as this is written
there are two days of it and the recorder is still running. The conclusion does
not depend on it — one irreplaceable week shuts the window on its own — which is
precisely why a claim like that is easy to leave standing, and why it is worth
striking before some later decision is taken on the strength of capture we do
not have yet.

There is a narrower rule on top of that while a judged run is in flight: the
container version is **frozen for its duration** (`docs/paper-run.md`). Bumping
it would not change the running recorder — that is a binary already loaded — but
the artifacts have to stay readable by the tools that will judge the run, and two
weeks of capture that newer tools refuse is the same data loss arriving by a
different route.

---

## 7. Acceptance criteria for M1

The recorder is not done when it prints JSON. It is done when:

- [x] It runs **7 consecutive days** unattended. *(met: 2026-08-21 → 2026-08-28
      on an Apple Silicon Mac, two symbols, one recorder process each, and
      **zero restarts** — the restart path the format was built for went
      unused.)*
- [x] Killing the network mid-stream produces a `Gap{Disconnect}`, an
      automatic reconnect with backoff, and a fresh snapshot resync. *(met, and
      not by contrivance: 36 `Disconnect` gaps off flaky Wi-Fi, each with its
      reconnect and its resync snapshot.)*
- [x] Every recorded depth delta is preceded, somewhere in its **connection
      episode**, by a book snapshot — or by a `SnapshotFailed` record saying why
      not. A capture whose deltas can never be turned into a book is not a usable
      capture, however complete it is. *(met: zero errors, and the one episode
      that lost its resync snapshot — day 3, four timed-out attempts — carries
      the `SnapshotFailed{Resync}` the second half of this criterion asks for.)*
- [x] `SIGKILL` mid-write leaves the last file readable up to the last
      complete frame — no partial-frame corruption. *(met as an ordinary unit
      test rather than an operational anecdote:
      `truncation_at_every_byte_offset_loses_only_the_tail` cuts a capture at
      every byte offset it has.)*
- [x] Replaying every recorded file shows `ingest_seq` contiguous within each
      session, with every discontinuity explained by a recorded `Gap`. *(met:
      70,545,346 frames, `missing 0`, `dropped 0`, 38 gap frames and an
      explanation for every one.)*
- [x] Replaying every recorded file shows the depth **update-id chain**
      contiguous — each message's `first_update_id` continuing the previous
      message's `final_update_id` — with every discontinuity explained by a
      recorded `Gap`. *(met: zero chain breaks, and confirmed a second time in
      M2 by `quant-normalize`, which shares no counting code with the
      verifier.)*
- [x] Recorder-side metrics exist for: messages/sec, bytes/sec, queue depth,
      venue latency percentiles, gap count by cause. *(met at M1.e; the line
      below carries all five, and one like it went into the log every minute per
      symbol for the whole week.)*

The last one is not optional polish. If we cannot see queue depth we cannot
tell the difference between a quiet market and a stalled consumer.

**M1 is complete.** `quant-verify` exit 0, verdict *"every discontinuity is
explained by a record in the capture"*. `CLAUDE.md` carries the full account of
the week; what belongs here is that the criterion the whole format was designed
around — a gap is a record, not a silence — is the one the week actually
exercised, thirty-eight times, and none of those thirty-eight had to be
explained after the fact.

### How the metrics are reported

One structured log line per minute, plus a whole-run summary at shutdown:

```text
metrics symbol=BTCUSDT msgs_per_sec=37 bytes_per_sec=13070
        queue=0 queue_peak=18 queue_capacity=4096 dropped=0
        latency_p50_ms=41 latency_p90_ms=88 latency_p99_ms=140 latency_max_ms=612
        latency_samples=2276 clock_skew=0
        gap_disconnect=0 gap_overflow=0 gap_sequence=0
```

The three `gap_*` counters went missing from this sample in an editing pass, so
the criterion above asked for a gap count by cause and the document then printed
a line without one. They are back, because the **field set** is the contractual
part of this line: `quant-binance::capture` emits exactly these keys,
`docs/acceptance-run.md` prints the same line, and M7's reader looks them up by
name. The *numbers* are one minute's worth and illustrative, which is worth
saying plainly: this copy and the one in `docs/acceptance-run.md` disagree about
`latency_max_ms` (612 against 212), and nothing in this repository can say which
minute either was taken from. A real line, copied verbatim out of the running
fortnight's log, is pinned by `a_real_line_from_the_running_fortnight_parses` in
`quant-explain`; that is the copy to hold an emitter against.

Rates are deltas between two readings, not totals, because a total that has
stopped growing looks exactly like one that never grew. Latency percentiles come
from a log-bucketed histogram — exact percentiles would need every sample, which
over seven days is hundreds of millions of values — with the reported value being
each bucket's **upper** bound, so a percentile never understates what was
observed.

That guarantee needs one correction to stay readable, and it is a correction we
made after seeing it: a bucket's upper bound can exceed every value that landed
in it, so an unclamped p99 came out *above* the observed maximum in the same
line. Both numbers were defensible and the pair was nonsense to whoever read it.
Percentiles are now clamped to the maximum, which keeps the never-understate
property — a percentile is always ≤ the max — and loses nothing. Hence
`latency_max_ms` on the line: it is what the percentiles are held against, not
decoration.

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

`clock_skew` on the line is that **count of samples**, not an offset in
milliseconds. It sits among fields that all end in `_ms`, which makes the
misreading close to inviting — and M7's reader took the invitation:
`quant-explain::health` parsed the count into a field it called `clock_skew_ms`
and `explain` printed it as `clock skew Nms` once it passed a threshold of 1000,
so a minute in which 1001 messages carried a venue timestamp ahead of ours would
have been reported as a second of clock offset.

**Fixed 2026-09-20**, in the reader rather than here: the field is
`clock_skew_samples` and prints beside `latency_samples`. Kept in this contract
because this is where the field is *defined*, and the naming that invited the
mistake — one count among a dozen `_ms` figures — is a property of the line and
not of whoever read it.

The millisecond offset is a different measurement: the startup check above, and
`ops/preflight.sh` before a run. It never appears on this line, which is the
other half of why reading `clock_skew` as one was so easy to do.

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

- [x] Every frame in the acceptance capture parses, with no unrecognised event
      types and no malformed numbers. *(met: 384M price levels through
      fixed-point, zero failures. Confirmed a second time by `quant-normalize`,
      which read all 70,545,346 frames with no parse break.)*
- [x] A replayed day holds the book invariants at **every tick**:
      `best_bid < best_ask`, no zero-quantity level retained, nothing
      non-positive. *(met: 70,539,557 live-book ticks across both weeks, zero
      violations, zero chain breaks.)*
- [x] A session's segments are joined, so the book carries across midnight
      rather than restarting unanchored at each UTC day boundary. *(met:
      `quant-normalize`, 8 segments joined per session, **0** deltas dropped for
      want of an anchor where single-file replay dropped 7k-34k per day.)*
- [x] The normalized Parquet tier is written, partitioned
      `exchange / symbol / date`, and a replay from Parquet agrees with a replay
      from raw. *(met: 2.1 GB of Parquet from 3.0 GB of raw, and **70,545,345
      events match event for event** — `normalize --check`.)*

**M2 is complete.**

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

### The archive's own discontinuities are not `Gap`s

A segment that will not open, a torn tail, a hole in `ingest_seq` — each means
the events after it do not continue the ones before it, and the book must be
cleared exactly as a recorded `Gap` clears it. They are still **not** `Gap`s.

`GapCause` is a statement about what happened at the venue or in the recorder,
and it is persisted in the immutable tier under §1's rules. "I could not read
this file just now" is a statement about a particular replay on a particular
machine. Adding a fifth cause for it would put a replay-time condition into a
capture-time contract, and a re-derive on a healthy disk would then produce
different bytes from one on a failing disk. So `quant-normalize` reports them as
`Break`s, which live only in the replay.

A hole breaks the book **immediately**, without waiting to see whether a gap
record explains it. The recorder writes that record *after* the hole it
describes — the hole is the evidence, the record is the account — so waiting
would mean handing out events across a known discontinuity in the hope of being
forgiven. Invalidating twice costs nothing.

### Money on disk is `DECIMAL(18,8)`

Not `INT64`. The bytes are identical — Parquet backs a decimal of precision ≤ 18
with an `INT64` — but the decimal carries **the scale in the schema**. §1 says
money is integral and never passes through `f64`, and that guarantee has so far
only held inside our own process. A bare `INT64` column makes every reader
responsible for knowing the `1e8` convention out of band; the first that does
not is wrong by eight orders of magnitude, and the first that reads it as a
double loses precision silently on large notionals. The column type is how the
invariant survives the process boundary.

Precision 18 at scale 8 holds values below `10^10`, where `i64` holds nine times
that. A value past the bound is a **loud error on write**, per §4's "parse
failures are loud" — never a truncation.

Timestamps are `TIMESTAMP(NANOS, UTC)` on the same argument. This is the first
place nanosecond `Ts` values leave the raw tier; the metadata tier's microseconds
remain operational-only, which is not a contradiction, because Postgres holds an
*index* and this holds a re-derivation.

**The instrument is not a column.** `InstrumentId` is a registry index and is
never persisted; identity lives in the partition path and is reattached from
`(exchange, symbol)` on read.

### A partition names the session it came from

The normalized layout has no session in it, and should not: this tier is about
what the market did, and which capture run saw it belongs to raw and to the
metadata tier. But a recorder restart creates a new session, and two sessions can
hold segments for the same symbol on the same day.

So each file carries its source session in the Parquet footer. M2.d used that to
**refuse** a partition another session owned, which was safe rather than lossy
but left the day unnormalizable. Since M2.e the day is a sequence of **parts** —
`part-00000.parquet`, `part-00001.parquet`, … — one per contributing session, and
every file still names exactly one session, so nothing has to describe two
sources at once.

A restart is **sequential**: `ops/supervise.sh` runs the recorder in the
foreground of its restart loop and reads its exit code before respawning. So a
shared day is a **concatenation**, not an interleave, and a file boundary is the
exact and free encoding of one. Within a part, `ingest_seq` is one session's and
strictly increasing, which is what merges that part's four dataset files into one
stream; across parts it means nothing at all, because it restarts at 1 for every
new session. Nothing inside a row changes, which is forced rather than chosen —
`normalize --check` compares whole events, and `EventMeta` includes `ingest_seq`,
so renumbering would fail at the first event.

**This section used to say the merge is "ordered by `local_recv_ts`", and that
was wrong.** `local_recv_ts` is `SystemTime`: it steps, and it is not monotone
even within one session — §5's forward-only rolling rule and the
`backdated_records` counter exist because of it. Ordering two sessions by a
quantity that can run backwards either refuses a healthy day after an NTP step
or, worse, silently reverses them. Parts are ordered by the **venue's own
update-id span**, recorded per part in the footer, which is strictly increasing
per symbol across disconnects and is immune to anything our host's clock does.

**The full ordering key is `(day, book_seq_first, ingest_seq)`, and a part's
index is a name rather than a position.** A part's index is simply the order the
parts reached the writer, which is the order `catalog` yields sessions — a text
sort of paths, so within a shared day it is a text sort of v4 UUIDs and carries
no chronology of its own.

**M2.e tried to make index order mean capture order, and that was a defect**
corrected on 2026-09-20. A publish-time check (`check_follows`) published a part
only if its span began after every already-published part's span ended, and the
reader then concatenated in index order on the strength of it. Refusing genuine
overlap is right — two sessions whose spans intersect were concurrent rather
than sequential, and there is no ordering of two simultaneous recordings of the
same messages that is the truth. But that same check was the only thing standing
behind the index, so it *also* fired when two genuinely sequential sessions were
handed over in the wrong order, which UUID text decides and which is therefore a
coin toss. `normalize` drops its writer on a write error, so the rest of that
session's days went unwritten behind the refusal — and because the catalog's
order is deterministic, a re-derive reproduced the refusal exactly rather than
clearing it.

Nothing was ever lost when that happened — raw is intact and this tier is
disposable, per §1 — but the day did not normalize. The two questions are now
separated. **The writer refuses only genuine overlap** (`PartsConcurrent`),
which is the one refusal that is about the data rather than about the order we
happened to walk it in. **The reader orders the day's parts by their spans**,
where it holds every part at once and the footers already carry what it needs; a
day with a single part is charged no footer read, so the ordinary day is read
exactly as it was before.

A day's partition is also **published by rename**: written to a `.tmp` sibling
and moved into place once its footer lands, so a reader never finds a truncated
file at a real path.

**What the parts make whole is the data, and that is all they make whole.** This
section is easy to read as "a restart inside a day is handled", and for this
contract it is: the day normalizes, every event is there, and a replay across the
boundary sees them in the venue's own order. The *process* is a separate
question, and not this document's to answer. `JournalEntry` records fills,
checkpoints, a risk trip and a stop, but nothing of the strategy, and
`Engine::resuming` replaces only the portfolio — so a restarted paper process
comes back with the right cash and position and with empty indicator windows, a
zeroed equity sampler and a zeroed daily risk tally, while the backtest it is
compared against runs all three continuously across the same instant. That
belongs to M5 and to `docs/paper-run.md`; it is named here only so the merge is
not mistaken for a guarantee it does not make.

### The partition day is inherited, not re-derived

A record is filed under the day the **raw tier** filed it under, read off the
segment it came from — not recomputed from `local_recv_ts`. The two agree except
in the case §5's forward-only rolling rule carves out: a record stamped before
the open segment's day, after an NTP step, is written to the open segment on
purpose. A re-derivation would file it elsewhere, and the two tiers would stop
lining up directory for directory — which is the only cheap cross-check between
them.

### An invalidated book is cleared, not flagged

A flag can be ignored; an empty book cannot be misread as prices. That is what
makes "refuse to trade across a gap" (§3) enforceable rather than advisory.
