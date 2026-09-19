# M7 — Observability

The criterion is *"what was it doing at 03:14 last Tuesday?" answered in a
minute.* This is the scoping call, the acceptance criteria, and the slices.
Written before the code, for the reason `docs/data-contract.md` was written
before the recorder and `docs/engine-contract.md` before the engine: the shape is
the expensive thing to change once anything depends on it.

---

## 1. The scoping call

**M7 is a time cursor over artifacts that already exist. It is a reader — not a
recorder, and not an exporter.**

The re-scope is M1.d-shaped. Sort the facts the question needs by which ones a
later milestone can still recover, and keep only what this one must supply.

**Re-derivable, therefore a reader's job.** The book at an instant, the mark, the
spread, the last trade, whether we were blind and why; and our position, cash,
realized P&L and fees. All of it is a pure function of raw plus the journal.
Measured, not assumed: `normalize` replayed **3,438,259 frames across both
symbols of the live run in 4.70 s**, building a real book and checking invariants
at every tick. A full symbol-day extrapolates to about six seconds.

**Not re-derivable from anything.** Orders that did not become fills, risk
refusals, cancels, what the strategy believed, and whether the engine saw the
same stream the capture did. The only copy of those was in memory and is gone at
process exit.

It is tempting to conclude that the second column is urgent — *decisions are
being destroyed right now* — and therefore that M7 should be a recorder. That is
right about the loss and wrong about the remedy. The fortnight is pinned at
`aff848d` and rule 1 is never to pull there, so a run log merged tomorrow would
not be written by the running process. **The second column's loss for this run is
already sunk, and no version of M7 recovers it.** The first column is answerable
today, slowly, by hand — and can be answerable in seconds this week.

The freeze also positively licenses a reader, in the project's own words
(`docs/paper-run.md`): *the freeze exists so the system under comparison does not
change, and rule 3 names `quant-engine`, `quant-sim` and the strategy. A reader
that reads all of the data rather than some of it is not the system under test.*
M2.e already landed on `main` past the tag on exactly that licence.

### The defect this actually fixes

The system does not lack data. **It lacks a notion of a moment.** Every tool is
either present-tense (`ops/status.sh`, tailing the metrics line) or whole-run
aggregate (`dump`, `normalize`, `verify`, `reconcile`). `reconcile` has no
`--at`; `dump` prints no timestamps at all. The one time-indexed durable record —
raw — has no reader that accepts a timestamp.

So M7 supplies one: a new crate `quant-explain` with a binary `explain`, at the
top of the dependency graph beside `quant-verify`, depending on `SessionReplay`,
`Book` and `journal::replay` rather than reimplementing any of them.

---

## 2. What M7 is not

**Not metric export in the conventional sense.** No Prometheus, no OpenTelemetry,
no `/metrics`, no scrape. Four arguments:

1. *It does not touch the criterion.* Delete the endpoint and the reader still
   answers 03:14. A time-series database would be a fourth store of numbers
   derived from artifacts we already keep, with no way to notice when it
   disagreed — the pattern this project has refused three times already (no
   `PaperVenue`, no venue trait from one implementation, no duplicated ops
   harness).
2. *It is a listening socket inside a process built `panic = "abort"`.* M1's
   recorded lesson is literal: a metrics counter wrapped to `usize::MAX` and
   **killed the recorder**, and the rule written down was that a metric must
   never be able to take down a capture.
3. *Alerting is a control, not observability,* and this project separates those —
   the kill switch is a control and got its own milestone and its own proof.
   Alerting belongs on a host that is not the trading host, which is M8's problem.
4. *The good idea inside the export argument is not the exporter.* M1.e's metrics
   line mixes three time bases with nothing marking which is which: `queue` is
   instantaneous, `queue_peak` is lifetime, and `dropped`, the gap counts and
   every latency percentile are deltas over the last 60 seconds. A `queue_peak`
   frozen for six hours reads like a live number. **The fix is that the reader
   labels each figure's time base**, which costs nothing and is part of M7.

**Not a run log.** No new journal entry types for orders, refusals or cancels.
That is the real next milestone and it is deliberately after the freeze — and
building the reader first tells us what those entries must contain rather than
guessing a schema. M2's replay proved its own module docs wrong about buffering
*by being written*; the same discipline applies here.

**Not a time index on raw.** Measured at roughly six seconds per symbol-day, and
it does not grow with run length — day 13 costs what day 1 costs, because the day
partition bounds it. M0's Hive partitioning already *is* the index at day
granularity. The threshold is stated rather than implied: **the no-index design
holds while one symbol-day replays in well under a minute.** The block chain is
seek-walkable without decompressing (each 16-byte block header carries
`compressed_len`), so a bisecting `.idx` sidecar stays cheap when that stops
being true.

**Not JSON logging, not the Postgres `runs`/`orders`/`fills` tables, and not a
dashboard.** The tables have nothing to hold until a run log exists. A dashboard
nobody regenerates diverges from the metric names and then lies.

---

## 3. Decisions settled before the code

**`explain` reads raw, not the normalized tier.** The tier does not exist for the
live run and creating it would mean writing into the run root mid-fortnight. Raw
is the source of truth, `SessionReplay` already reads it, and 4.70 s for 3.4M
frames says cost is not the reason to prefer Parquet.

**The reader writes nothing — no index, no cache, no sidecar.** This is the one
property that makes it structurally unable to become a second source of truth,
and it is why this milestone needs no change to the data contract's tier table. A
cache is exactly the kind of thing that gets added "temporarily" in week two, so
it is refused now, in writing.

**`--at` is against `local_recv_ts`.** Invariant 2: the engine orders and
dispatches on `local_recv_ts` only. A reader that *seeks* on `exchange_ts` would
answer a question the engine never asked, and would quietly disagree with it
about which events preceded a decision.

**A commit mismatch warns; it does not refuse.** The running build's commit will
routinely differ from `run.json`'s — it already does, because `main` is past the
run's pinned `aff848d`, and that is the *expected* case for post-hoc analysis.
Refusing by default would make the tool unusable for its main purpose. This is
deliberately *not* the "an invalid book is cleared, not flagged" precedent: that
rule exists because stale prices can be mistaken for real ones and acted on,
whereas a provenance mismatch is a fact about the reader rather than a number
that could be traded.

**This paragraph used to end "both commits are printed in the provenance block,
every time, so the reader states the discrepancy rather than hiding it", and
that was never built.** `print_provenance` in
`crates/quant-explain/src/bin/explain.rs` prints the capture session id, every
segment path it read, and how many events it replayed to reach the instant —
and then discards its arguments outright (`let _ = args;`). Nothing in the
workspace reads `run.json` at all; `grep -rn run.json crates` comes back empty.
The decision above stands as a decision, and shipping it is **owed work**. Until
it is done, a report gives no way to tell which build produced it — the one
question a provenance block exists to answer — and it is worth doing *because*
the mismatch is the expected case rather than the alarming one, which is exactly
what makes it easy to stop noticing.

**The metrics reader parses prose, and that is named debt.** `tracing`'s default
`fmt` output is a format we do not own, and an emitter/parser pair that must
agree forever with no test that they do is the pattern this project refuses. It
is accepted here only because the alternative — switching the emitter to
`.json()` — cannot happen while the fortnight runs a pinned binary. **The
replacement is scheduled, not hoped for:** switch the emitter and delete the
parser in the same commit as the run log.

Until then the parser is strict in one place and silent in another, and the
sentence that used to stand here named only the first. A line that *looks* like
a metrics line and will not parse is reported as `NoHealth::Unparseable` rather
than skipped — that half is built. But a line is only looked at at all if it
contains the `metrics symbol=` sentinel, and that literal is itself part of the
format we do not own, so the day the emitter changes every line stops matching
at once and the reader falls through to `NoHealth::NotCovered` — *"no metrics
line covers this instant -- the process may not have been running"*. That is
P3's confabulation arriving through the one block whose figures cannot be
re-derived from any artifact: a stale reader blaming the run.

**The remedy is named here rather than claimed as built.** A log holding lines
of which none is recognisable should be its own absence — a fourth `NoHealth`
saying which log was read, how many lines it held, and that this reader no
longer recognises the format. `NoHealth` today has three variants, `NoLog`,
`NotCovered` and `Unparseable`, and that fourth is **owed work**. The freeze
does not block it: the sentinel lives in `quant-explain`, which is the reader
and not the system under comparison.

What is missing beyond that is a **test pinning the emitter to this parser**,
which is what would make the debt safe rather than merely declared. That one
*is* blocked — pinning the emitter means running it, and `quant-binance` is
frozen. It belongs in the same commit as the `.json()` switch, and is named here
so it is owed rather than forgotten.

---

## 4. The acceptance criteria

"Answered in a minute" is a vibe. Three properties that pass or fail, in the
shape M4 used to replace "degrades sensibly".

### P1 — the minute, measured

Twenty instants chosen at random from the finished run's span. For each, cold,
with no cache and no prebuilt index:

```bash
explain ~/paper --at <instant> --symbol BTCUSDT
```

must exit 0 in **under 10 seconds** — not sixty; the other fifty are for the
human reading it — and print five blocks, each named so that its absence is a
failure:

- the instant echoed in **both** the given offset and UTC;
- **market** — best bid/ask, spread, last trade, book validity, and the nearest
  gap either side with its cause;
- **ours** — position, average cost, cash, realized, fees, equity (or `no mark`
  *with the reason*), and the last fill before and next fill after with their
  deltas from T;
- **health** — queue, latency percentiles, drops and clock skew for the
  containing minute, **each figure labelled** window-delta, instantaneous or
  lifetime;
- **provenance** — every file read, the capture session id, `run.json`'s commit,
  and this build's commit.

*Fails if* the **slowest** of the twenty exceeds 10 s, any block is absent, or any
block is present but silently empty rather than stating a reason.

**Two of those bullets are not met as written, and §5a's results are scored
against the blocks that shipped.** The provenance block names the session, names
every segment path and says how many events were replayed to reach the instant;
of the four things asked for it prints two, neither commit among them, and it
reads no `run.json` — see §3. And the health block labels `queue` instantaneous
and `queue_peak` lifetime as asked, but prints the metrics line's `clock_skew` —
a *count of messages whose venue stamp was ahead of ours* — as `clock skew Nms`,
through a field this crate named `clock_skew_ms`. A figure printed in the wrong
unit is worse than an unlabelled one; §7 is where that was caught.

Both blocks are present rather than absent, so the timing figure stands. But the
criterion is the standard and by the criterion these parts are unfinished, and
they are recorded here rather than quietly rescoped: a criterion edited to match
what was built stops being a criterion.

### P2 — agreement, against a genuinely independent computation

Not against `reconcile`: both would call `journal::replay`, so agreement would
hold by construction — the vacuous identity M5.c already caught once.

The independent pair is already in the journal. A `Checkpoint` is what the
**running engine believed in memory at that instant**; a fold of the fill lines is
what **the file says** afterwards. So: for **every** `Checkpoint` in the
fortnight's journals, `explain --at <its timestamp>` must reproduce its `cash`,
`realized`, `fees` and `fills` **exactly** — same `i64`s, no tolerance. At the
observed cadence that is roughly 190 agreement points over the fortnight, and one
mismatch fails.

This strengthens `reconcile` as a by-product: that checks the last checkpoint,
this checks all of them.

**Verified capable of failing.** Three sabotages must turn P2 red — a deleted
`filled` line, a checkpoint altered by one satoshi, and a deliberately stale book
anchor. A check that cannot go red is worth nothing; M5.c's vacuous identity is
the standing reminder.

### P3 — it must say "unknown", never a number

A query tool's characteristic failure is confabulation: printing a stale book or
a zero position as though it had been observed. Four fixtures, each asserting the
**absence** of a figure and the **presence** of a reason:

- an instant before the session started → no position, no book, reason given;
- an instant inside a recorded `Gap` → book reported invalid, never last-known;
- an instant whose anchor is missing → book unavailable, reason names what is
  missing;
- a symbol-day with no capture → "no data", exit non-zero.

*Cannot tell is not permission* — M2.d's rule for refusing to publish over a
partition, applied to a reader.

---

## 5. Slices

| | Slice | Content |
|---|---|---|
| a | `Ts` ⇄ RFC3339 in `quant-core` | The parser and renderer, no new dependency |
| b | `explain --at`: market state from raw | `SessionReplay` + `Book` to the instant |
| c | `explain --at`: our state from the journal | `journal::replay`, never a second accounting |
| d | The checkpoint agreement, and proof it can go red | P2 plus its three sabotages |
| e | Operational state, with its time bases labelled | The prose metrics parser — **the cut line** |
| f | Refusal fixtures, provenance, and this document | P3, `--window`, the reasoning |

Slice (e) was the one to drop if the week ran long. It did not, and it turned out
to be the slice that found something — see §7.

---

## 5a. What it does, and how the criteria came out

All six slices landed, with two parts still owed: (f)'s provenance block does
not print the commits §4 asks it for, and (e)'s health block prints a count as a
duration. The command:

```bash
explain ~/paper --at 2026-09-19T10:00:00Z --symbol BTCUSDT [--window 5m]
explain --check-journal ~/paper/paper-BTCUSDT.jsonl
```

Measured against the fortnight *while it runs*, rather than against fixtures —
so P1 and P2 are readings taken on day two, not final results:

| | result |
|---|---|
| **P1** — 20 random instants, cold, 2026-09-19 | **worst 4951 ms**, mean 2031 ms, budget 10 s |
| **P2** — every checkpoint in both live journals, 2026-09-19 | **18 of 18 agree** |
| **P3** — refusals | 8 tests assert an absence *and* a reason |

**Those two figures disagree with the ones `CLAUDE.md` records — worst 5799 ms,
and 24 of 24 — and neither pair can be checked from this repository**, because
the journals and the capture are on the Mac and rule 1 keeps them there until the
run ends. The likeliest account is that they were taken hours apart on
2026-09-19 and both were true when taken: checkpoints accrue while the run goes,
so a later reading sees more of them, and a cold-cache timing is a property of
the machine at that moment. **Neither is picked over the other here**, because
picking would mean inventing the measurement that settles it. P1 and P2 are
re-taken at judging against the finished run and those are the figures that
count; the pair above stands as what day two looked like.

P2's independence is the part worth re-reading before changing it. Checking
`explain` against `reconcile` would hold **by construction** — both call
`journal::replay` — which is M5.c's vacuous identity in a new costume. The real
pair is a `Checkpoint` (what the engine believed in memory, written by a process
now gone) against a fold of the fill lines (what the file says). All three
sabotages turn it red: a checkpoint off by one satoshi, a deleted fill, and a
journal with no checkpoint at all, which exits 2 rather than 0 because "nobody
disagreed" is not "two answers matched".

**`--window` is the instant generalised, not a second mode.** The replay already
walks every event up to `at`, so summarising the tail of that walk costs one
comparison per event and no second pass; the instant is simply the window of
length zero. A separate window traversal would have been a second implementation
that must agree with the first forever.

The span is **half-open — `(at - span, at]`**. Inclusive at both ends would put
an event exactly on the seam into two adjacent windows, so stepping through a run
five minutes at a time would count it twice and the steps would not sum to the
whole. A window reaching back before the data is *not* an error: asking for the
last hour of a run that started ten minutes ago is ordinary, and the honest
answer is the ten minutes.

---

## 6. Three defects found while scoping

Recorded here rather than fixed silently, because two of them are freeze
questions.

**The journal's `client_order_id` is fabricated.** `paper.rs` writes
`ClientOrderId(self.fills)` — a *fill ordinal*, not the engine's id, because
`FillObserver::on_fill` is never handed the real one. Refused orders consume an
id before the risk check, so **the first refusal desynchronises it permanently**.
Harmless today because nothing has been refused, and `explain` will print it
labelled as an ordinal rather than as an id. Fix it with the run log.

**`Started.at` is hard-coded to zero.** Both live journals read `"at":0`, so the
journal cannot say when its session began. This used to end *"`explain` derives
the session start from the first raw frame instead"*, and that is not what it
does: `ours_at` is handed the journal path and no data root at all, and takes the
earliest instant the journal can speak for from the journal's *own* entries —
the first `Filled`, `Checkpoint`, `Tripped` or `Stopped` line, `Started` being
the one entry whose timestamp is the zero. No raw frame is consulted. An instant
before that is refused with a reason naming it, which is P3's rule and is what
makes the defect survivable rather than silent. What it costs is the stretch
between the process starting and its first journalled event: there the honest
answer is *nothing had happened yet*, where a dated `Started` would say *the
session was up and flat*. Fix it with the run log.

**Nothing watches the tee during a run.** `TeeSink::secondary_dropped()` exists
and is called from nowhere outside its own module, so no line of output says
whether the engine missed records the capture received. This matters because M5's
exact-agreement criterion silently assumes it was zero.

It *is* checkable after the fact, and the rehearsals did check it: the paper
binary prints `events N reached the engine` in its shutdown summary and the
capture's own summary carries `records=N`, and a dropped record makes the first
smaller — the engine finds the `ingest_seq` hole itself. So this is a gap in
*surveillance*, not in recoverability.

**This used to end "`explain` reports both numbers in its provenance block", and
it reports neither.** The `events_read` it does print is a third quantity
altogether — how many events *this replay* walked to reach the instant, over the
instant's day plus the previous one when the book needs it — so it is neither
the engine's whole-run count nor the capture's, and quoting it as either would
be worse than printing nothing. Both real numbers live in process output at
shutdown, which makes the after-the-fact check a matter of reading two log lines
rather than of running `explain`. Wiring the counter into the metrics line is a
freeze change and waits; teaching the provenance block to quote that pair is
not, and belongs with the commits it also does not print.

---

## 7. What it found on first use

The five-block report was run against the fortnight on its second day and the
health block read:

```
health    queue 0 now, 270 at its worst since start, of 4096 capacity
          in the 60s to 2026-09-19T10:00:52Z: 26 msg/s, 0 dropped,
          latency p50 4194ms p99 14155ms max 14808ms
```

Against 57–73 ms for the rest of the run's first nineteen hours. Two such
excursions so far, at 07:47 and 10:00, each recovering within the hour.

**Benign, and checked rather than assumed — but one leg of the check was weaker
than it read.** `queue=0` and `dropped=0` throughout, so nothing was backed up
and nothing was lost. The third figure was quoted as `clock_skew=0` meaning *the
host clock is fine*, and it does not mean that. `clock_skew` is a **count of
messages whose venue timestamp was ahead of ours**, not an offset: zero of them
rules out our clock running *behind* the venue and says nothing about it running
*ahead*, which is the direction that inflates every latency figure — M1's
machine sat ~2 s ahead and read p50 2883 ms for exactly that reason.

**And the reader repeats the mistake rather than having fixed it.**
`quant-explain` parses that count into a field it calls `clock_skew_ms` and
prints `clock skew Nms` once it passes a thousand — so a thousand skewed
messages would be reported as a second of offset. The emitter's own
`latency_samples`, which is what would make the count read as a proportion, is
not parsed at all. The millisecond offset the figure is mistaken for is a
different measurement entirely: taken round-trip-corrected against
`/api/v3/time` by `ops/preflight.sh` and the recorder's startup check, and never
on the metrics line. Renaming the field and printing it beside `latency_samples`
is **owed work** inside the reader, which the freeze does not block; §4 scores
P1 against it.

The conclusion survives the correction, on other evidence: the excursion
recovered within the hour and the nineteen hours around it read 57–73 ms, which
a clock offset does not do. So this is transport delay between the venue and us,
the same signature M1's acceptance run saw on hostel Wi-Fi. It affects no
completeness criterion. It does mean the strategy sampled slightly stale prices
for a few minutes, which `local_recv_ts` ordering handles correctly by
construction.

The point is that it was invisible before. Finding it meant grepping 1388 log
lines by hand and knowing which of them to compare, which is precisely the
question this milestone exists to make cheap.

---

## 8. What M7 did not do, and what is next

Unchanged from §2, and worth restating now that the reader exists:

- **The run log.** Orders that never filled, refusals, cancels, strategy state.
  Still not recorded anywhere, and still not recoverable by any reader. The
  immediate next milestone, to start when the freeze lifts. Building the reader
  first was the right order: it made concrete what those entries have to contain.
- **The prose metrics parser is debt with a scheduled repayment.** Switch the
  emitter to `.json()` and delete `health.rs` in the same commit as the run
  log, and write the emitter-to-parser test there too — §3 explains why it
  cannot be written until then.
- **The three scoping defects in §6** are unfixed by design, for the same reason.
- **Two things inside the health block are owed, and the freeze blocks neither.**
  It prints the metrics line's `clock_skew` count as `clock skew Nms`, a
  duration (§7), and a log whose lines it no longer recognises reads as *"the
  process may not have been running"* rather than as a stale reader (§3). Both
  live entirely in `quant-explain`.
- **The provenance block prints neither commit and reads no `run.json`.** §3 and
  §4 record it; it is the other part of P1 that is owed rather than done, and the
  freeze is not what blocks it — `quant-explain` is the reader, not the system
  under comparison, which is the same licence §1 claims for the whole milestone.
- **A doc comment in `explain.rs` is misfiled**, and it is the same defect this
  document was repaired for. The summary line and the paragraph explaining the
  health block's time bases — *"`queue_peak=270` beside `queue=0` is how an
  operator concludes the wrong thing at 3am"* — sit on `print_window`, with
  `print_window`'s own one-line summary stranded beneath them as the last line
  of the block, while `print_health`, which they describe, carries no doc
  comment at all. A search-and-replace whose tail survived, in code rather than
  in prose. Harmless to run and misleading to read, which is the whole objection
  to it.

The honest structural caveat, restated because it has not changed: `explain`'s
market answers are what the *replay* says the book was, not what the *engine*
saw. M5's criterion is the claim that those are identical, and it has not passed
yet — the fortnight started 2026-09-18T14:46:54Z and ends 2026-10-02. Until it
does, this is a debugger whose foundation is the thing under examination.

One thing learned since sharpens that caveat rather than softening it: **a
supervisor restart inside the fortnight forfeits the exact comparison outright**,
and nothing a reader does afterwards repairs it. `Engine::resuming` replaces the
portfolio and nothing else, and `JournalEntry` has no strategy entry — so a
restarted paper process comes back holding the right cash and position but with
empty crossover windows, a zeroed equity sampler and a zeroed daily risk tally,
while the judging backtest runs all three continuously across that same
boundary. The fill counts then differ for a reason that has nothing to do with
live-versus-replay, which is the only thing being asked. Where a mid-run restart
has been discussed it has been treated as a *reader* problem about M2.e's parts;
it is a **state** problem, and if one happens the honest report is that M5's
criterion was not testable on this run.
