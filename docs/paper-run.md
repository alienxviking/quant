# The M5 paper run

How the fortnight is conducted and judged. The companion to
`docs/acceptance-run.md`, which did the same for M1's seven days.

`docs/engine-contract.md` §9 has the criteria. This is the procedure.

**Where this stands, 2026-09-20.** The fortnight is *in flight*. It started
2026-09-18T14:46:54Z and ends 2026-10-02 — two symbols, one paper process each,
pinned to the tag `m5-run-start`, which is commit `aff848d`. Everything down to
*Stopping it* is the record of how it was set up and how it is watched. *Judging
it* has not been run against the fortnight at all; it has been run once, end to
end, against the 10-minute rehearsal of 2026-09-18, and every result it reports
is that rehearsal's. Any sentence here that reads as a past-tense account of the
fortnight is a document bug rather than a result.

---

## What is being tested

Not the strategy. M4 already settled that: a crossover at this turnover cannot
survive ten basis points a side, and the fortnight will confirm it in a slower
and more expensive way. What is being tested is **the platform**:

1. The same strategy binary runs against `LiveSource + SimulatedVenue` for two
   weeks with no code change from the backtest wiring.
2. **Paper P&L matches a backtest over the data captured during the same
   window.** Exactly, not within a tolerance.
3. P&L recomputed from the journal alone agrees with the engine's own
   checkpoints.
4. The run survives restarts with its position intact.

Only the first, third and fourth can be judged during the run. The second is the
point of it, and it is judged afterwards — see *Judging it* below.

The fourth is narrower than it sounds, and the second is what pays for it.
Position and cash come back across a restart; strategy state does not. So a
restart satisfies criterion 4 and **forfeits criterion 2** — written here before
there is a number to rationalise rather than after one. See *A restart forfeits
the exact comparison* under *Judging it*.

---

## Before starting

### Pin the commit, and mean it

```bash
git tag m5-run-start
git push origin m5-run-start
```

Check the Mac out at that tag and **never `git pull` there** until the run ends.
The running process would not change, but the ability to say which code produced
the result would be gone.

The tag matters more than it looks. Criterion 2 compares a paper session against
a backtest, and if `quant-engine`, `quant-sim` or the strategy change while the
run is in flight, that comparison is between **two different systems** — it would
fail for reasons that have nothing to do with live-versus-replay, which is the
only thing being asked. `start-run.sh` records the commit in `run.json`; the tag
is what makes it easy to get back to.

### Freeze two formats for the fortnight

The raw container version and the journal schema. Everything else can change on
the development machine — the Mac runs a binary that is already loaded and is
untouched by anything pushed elsewhere. But the artifacts have to remain
*readable* afterwards, and a container bump mid-run (as M1.d1 did, 1 → 2) would
leave two weeks of data that newer tools refuse.

### Fix readers, not the system under comparison

The freeze above is about **formats**, and it exists so the artifacts stay
readable. There is a second and tighter rule about **code**, and it comes from
criterion 2 rather than from readability: the judging comparison is only
legitimate if the thing being compared did not change while the run was in
flight.

So during the fortnight, development may touch **readers** — `quant-normalize`,
`quant-explain`, `quant-verify`, `docs/`, `ops/`. A reader that reads all of a
day's parts rather than the first one, or that stops printing a sample count as
though it were milliseconds, is not the system under test; fixing it makes the
judgement better rather than different. Development may **not** touch
`quant-engine`, `quant-sim`, `quant-backtest` — which holds the `backtest`
binary, the `paper` binary and `MaCrossover` itself — or anything they depend on
in a way that changes behaviour. Those *are* what step 4 runs.

The rule is stated as a check rather than as an intention, because an intention
cannot be re-run at judging time and this can:

```bash
tag=aff848d    # m5-run-start
git diff --numstat "$tag"..HEAD -- crates/quant-engine crates/quant-sim \
    crates/quant-backtest crates/quant-core crates/quant-book
```

It must print nothing, with one allowance that has to be read rather than
counted: `quant-core` sits beneath everything, so an *additive* change there — a
new module nothing on the engine path calls — does not alter behaviour. As of
2026-09-20 the entire output is one line, 347 added and 0 removed in
`crates/quant-core/src/time.rs` — M7's RFC 3339 conversion, four new functions
of which `quant-explain` is the only caller. If a line with a non-zero deletion
count ever appears, or any line at all under the other four crates, then step 4
has to run from the tag and the result has to say why it could not run from
`HEAD`.

### Sync the clock

macOS disciplines its clock by default and held to ~0.5 s against Binance for all
of M1's week. `ops/preflight.sh` checks it round-trip-corrected, which matters:
measuring `now - serverTime` naively folds one-way latency into the offset and
would block a run over a clock that is fine.

```bash
ops/fix-clock.sh          # read-only confirmation, no sudo
```

### Disk

Fourteen days of one symbol is about 3 GB compressed; two symbols about 6 GB.
Preflight refuses to start on a thin disk.

### Building on Apple Silicon

Proven at M1. `zstd-sys` compiles C, so Xcode Command Line Tools are required:

```bash
xcode-select --install
cargo build --release
```

Everything else builds clean on `aarch64-apple-darwin`, and CI has covered it as
a second job since 2026-08-31 — guarding exactly this C-toolchain surface, since
`zstd-sys` compiles C and `ring` assembles per architecture.

macOS ships **bash 3.2**: no `mapfile`, no associative arrays. The `ops/*.sh`
scripts are written to that, and the `.ps1` half is **recorder-only** — a paper
mode there would be a harness nobody has run.

---

## Starting it

```bash
cd ~/quant
git checkout m5-run-start
cargo build --release

# rehearse first -- a harness that has never been run is not a harness
ops/start-run.sh --paper --minutes 10 --symbols "BTCUSDT" \
    --root ~/paper-rehearsal --paper-args "--qty 0.001 --fast 2 --slow 4 --interval-secs 5"

# then the fortnight
ops/start-run.sh --paper --days 14 --symbols "BTCUSDT ETHUSDT" \
    --root ~/paper --paper-args "--qty 0.001 --cash 100"
```

The rehearsal is not optional and the aggressive indicator settings are the
point: the fortnight's 10/30 on one-minute mids needs half an hour before it can
cross, so a short rehearsal on those settings would exercise the capture and
leave the journal, the fills and the reconciliation untested.

Two symbols means two paper processes, each capturing and trading its own
instrument with its own journal. That is what the agreement criterion needs — one
ingress, two consumers — and it gives the fortnight two independent samples.

## Watching it

```bash
ops/status.sh --root ~/paper
tail -f ~/paper/logs/verify.log
cat ~/paper/paper-BTCUSDT.jsonl | jq -c 'select(.type=="filled")'

# M7's time cursor: what was it doing at an instant, and does the journal agree
explain ~/paper --at 2026-09-19T10:00:00Z --symbol BTCUSDT [--window 5m]
explain --check-journal ~/paper/paper-BTCUSDT.jsonl
```

The verify loop runs every six hours and now reconciles the journals on the same
cadence. `WAIT ... nothing to check yet` before the first *checkpoint* is normal
— `paper` writes one every five fills and one at shutdown, so a quiet start has
nothing to compare against yet. After that every pass should read `AGREES`, and
it is judged against the entries the checkpoint describes rather than the whole
file, so fills arriving after it are not a disagreement.

**A `msgs_per_sec=0` in the last metrics line is not a stall** — M1 learned this
the hard way. The honest liveness signal is the capture file growing, which
`status.sh` shows.

### What is safe to run against a live run root, and what is not

`ops/status.sh` and `explain` write **nothing at all**. `explain` keeps no index,
no cache and no sidecar — that is the property which stops it becoming a second
source of truth — and `status.sh` only reads and counts. `verify` and `reconcile`
read as well, which is what lets the six-hourly loop run against the live
directory while the recorders hold it open. All four are safe at any time.

**`normalize --write` is the exception, and the reason is that it writes into the
run root itself.** `TierTarget::directory` is `root.join("normalized")`, so
`normalize --write ~/paper` publishes Parquet *inside the live run's own
directory*. At judging time that is exactly right — step 3 below is that command
— but while the run is in flight it would derive parts from a capture that is
still open: the final segment has no trailer yet and its last block is unsealed,
so the day published would be a day still growing, and the partition it occupies
is thereafter owned by that session at that part index. Re-deriving is cheap and
the normalized tier is disposable, so this is recoverable — but it is recoverable
by deleting `~/paper/normalized` and doing it again, which is a worse place to
find yourself than simply not having done it.

Run `normalize` **without `--write`** for a mid-run reconstruction report. It
replays, checks the book's invariants, prints the same counts, and touches
nothing.

## Stopping it

```bash
ops/stop-run.sh --root ~/paper
```

`SIGINT`, not `SIGTERM`: the recorder's shutdown is `tokio::signal::ctrl_c`,
which is SIGINT on Unix. SIGTERM is not caught and would leave the last segment
trailerless — indistinguishable from a crash.

---

## Judging it

Bring the capture, the journals **and the whole of `~/paper/logs/`** back
(`COPYFILE_DISABLE=1 tar ...`, per `docs/acceptance-run.md` — macOS otherwise
writes AppleDouble stubs that the verifier reports as strays).

**The logs are evidence, not convenience.** Criterion 2 silently *assumes* the
tee delivered every record to both consumers, and the only thing that can
confirm it is a pair of numbers that live nowhere but those files. `TeeSink`
does count its own drops, but `TeeSink::secondary_dropped()` is called from
nothing except its own unit tests, so that counter never reaches an artifact and
never reaches a log line — it is one of the three defects M7's scoping surfaced
and it is not fixed inside the freeze. What *does* reach a file is the paper
binary's shutdown summary on stdout (`events N reached the engine`) and the
capture's own closing line through tracing (`records=N`), and `supervise.sh`
sends both streams of every attempt to `~/paper/logs/$SYMBOL-$STAMP.log`. Leave
the logs on the Mac and that check does not become harder, it becomes
impossible.

```bash
git checkout m5-run-start          # the comparison runs at the run's commit

# 0. did the tee drop anything? one pair of numbers per supervisor attempt
grep -h 'reached the engine' ~/paper/logs/BTCUSDT-*.log
grep -h 'capture closed'     ~/paper/logs/BTCUSDT-*.log | grep -o 'records=[0-9]*'

# 1. is the capture complete?
cargo run --release -p quant-verify --bin verify -- ~/paper

# 2. does the journal account for itself?
cargo run --release -p quant-backtest --bin reconcile -- ~/paper/paper-BTCUSDT.jsonl

# 3. build the normalized tier from what the run captured
cargo run --release -p quant-normalize --bin normalize -- ~/paper --write --check

# 4. the criterion: backtest the same strategy over the same window
cargo run --release -p quant-backtest --bin backtest -- ~/paper --symbol BTCUSDT \
    --realistic --qty 0.001 --cash 100 \
    --max-order 200 --max-position 200 --max-daily-loss 20 --max-orders 200
```

**Step 0 is the one that cannot be done later.** `events N reached the engine`
is the engine's own count; `records=N` is the writer's. Both are fed by one
`Ingress` through the `TeeSink`, so they are equal exactly when the engine's copy
was never dropped, and if they differ the difference *is* the drop count — a
number the run records nowhere else. Compare them **per attempt** rather than as
two sums: an attempt that was killed hard prints neither line, and an attempt
missing from the comparison has to be named as unjudgeable rather than averaged
into a total that happens to agree.

**One exception to running at the tag, and it is narrow.** If either recorder
restarted mid-day, that symbol-day holds more than one part (M2.e) and a build
from the tag reads `part-00000` alone — it would find the first session's slice,
see the streams exhausted, and move to the next day, silently under-reading the
very artifact the criterion is computed from. `normalize --write` prints a
`merged` line whenever this applies, so the run says so itself. In that case run
**steps 3 and 4 from a build that contains the parts reader**, and specifically
one containing the part-ordering fix of 2026-09-20 described below — without it
a perfectly sequential restart is refused about half the time rather than
merged, and the refusal abandons the writer for the rest of that session's days
as well. None of that breaches the freeze: the freeze exists so the *system
under comparison* does not change, and what it names is `quant-engine`,
`quant-sim` and the strategy, per *Fix readers, not the system under comparison*
above. A reader that reads all of the data rather than some of it is not the
system under test.

Step 4's result must match the paper session's. **Exactly** — both consumed the
same events with the same `local_recv_ts` and the same `ingest_seq`, because the
tee gave them the identical records. Any divergence is a bug, and finding out
which side is lying is then the whole job — with exactly one exception, and it
is the next section.

### A restart forfeits the exact comparison, and it is a state problem

Every account of a mid-run restart in this repository, the paragraph above
included, has framed it as a **reader** problem: two sessions share a symbol-day,
so read every part rather than `part-00000` alone. That is true, and it is the
smaller half. **The larger half is that a restarted paper process does not
resume the state the judging backtest never lost**, and this document used to
imply, by never saying otherwise, that surviving a restart with position intact
was enough. It is not.

`JournalEntry` has five variants — `Started`, `Filled`, `Checkpoint`, `Tripped`,
`Stopped` — and not one of them carries strategy state. `Engine::resuming`
replaces the portfolio and nothing else. So a process that comes back up after a
supervisor restart holds this:

| | recovered | not recovered |
|---|---|---|
| position and cash | from the journal, by `Engine::resuming` | — |
| the kill switch | `RiskEngine::recover` reads the last `Tripped` | — |
| `MaCrossover`'s fast and slow windows | — | empty; rebuilt from nothing |
| `Recorded`'s equity sampler | — | zeroed; the curve starts again |
| the day's risk tally (orders, realized) | — | zeroed by `RiskEngine::recover` |

The judging backtest of step 4 runs straight through that boundary with all of
it intact, because nothing ever interrupted it. So after a restart the two sides
are **no longer the same system**: the live strategy spends `slow × interval`
blind before it can cross again — half an hour at the fortnight's 10/30 on
one-minute mids — while the backtest keeps crossing throughout, and the fill
counts then diverge for a reason that has nothing whatever to do with
live-versus-replay. That is the only question criterion 2 asks, so **the exact
comparison is forfeit.**

It is forfeit *permanently*. The indicator windows and the sampler at the moment
of the restart were never written down, so no later tool can reconstruct what
the live strategy would have decided, and no amount of re-deriving the market
data recovers it. This is written here **before there is a number to
rationalise**, which is the whole point: a divergence found at judging time, with
a restart sitting in the logs, must not be argued into or out of being a platform
bug depending on which answer is the more convenient.

**Did a restart happen?** Three independent answers, from three artifacts, and
they should agree:

```bash
ops/status.sh --root ~/paper                     # "N files in M session(s)"
grep -c starting ~/paper/logs/supervisor-BTCUSDT.log
jq -c 'select(.type=="started")' ~/paper/paper-BTCUSDT.jsonl
```

`status.sh` counts distinct session **ids**, not `session=` directories: the raw
layout nests session inside day, so an uninterrupted recorder still gets a fresh
directory at every UTC midnight and a directory count would have read 28 by day
fourteen — the line the run itself falsified on 2026-09-19. One session per
symbol means no restart. Each new session additionally opens with a
`Gap{RecorderRestart}` first frame, which `quant-storage`'s `dump` example shows
per file and which `quant-verify` reads as the explanation for the
discontinuity, so the capture accounts for a restart independently of the
supervisor's own log. And the journal is appended to across restarts, so a
second `started` line is the engine's own account of the same event.

**The journal says *that* a restart happened, not *when*.** An earlier draft of
this section said the `started` line carries its timestamp. It does not: `paper`
writes `Started { at: Ts::from_nanos(0) }` — the hard-coded zero M7's scoping
surfaced alongside the uncalled tee counter above — so every `started` entry in
this run's journals reads as the epoch, and `quant-explain` already discards
that field rather than believing it. Count them in the journal; take the instant
from an artifact that has one. `supervise.sh` stamps every `attempt N starting`
line in UTC, and the new session's `Gap{RecorderRestart}` frame carries a real
`local_recv_ts` in raw (`dump` shows the record but prints no times, so that one
is a job for `explain --at`). Fixing the zero now would not date this run's
restarts anyway: the Mac is executing a binary built at the tag.

**If a restart did happen, judge what is left rather than nothing.** The
comparison is still exact *between* restarts: take the stretch of the run
between two of them — bounded by the instants just recovered, since the
`started` entries cannot supply them — backtest the capture over that same
window with the same flags, and require the same exact agreement there. It is a
weaker statement — each segment carries its own warm-up, so the strategy is
compared over less of the fortnight than was recorded — but it still asks the
live-versus-replay question, which is the one worth asking. Say in the result
which of the two was done, and how many segments there were. A criterion
reported as met when a weaker thing was measured is worse than one reported as
partly met.

### What a divergence would mean

- **Different fill prices** — the live book and the replayed book differ, which
  would mean the book reconstruction depends on something other than the events.
- **Different fill counts** — the strategy saw a different event sequence, which
  would point at the tee, at a gap handled differently, or at the engine's step
  order.
- **Same fills, different P&L** — the accounting differs between the two paths,
  which should be impossible since both use the same `Portfolio`.

### The judging pipeline has been rehearsed end to end (2026-09-18)

On a 10-minute paper session at `~/paper-rehearsal` (fast 2, slow 4, 5 s mids),
every step that existed at the time — 1 through 4 — ran, and **step 4 reproduced
the paper session exactly**:

| | paper | backtest |
|---|---|---|
| events | 21690, 1 gap, 44 execution events | 21690, 1 gap, 44 execution events |
| orders | 23 sent, 0 refused | 23 submitted, 0 refused |
| fills | 22 | 22 |
| cash | 98.35961601 from 100 | 98.35961601 from 100 |
| realized | 0.07729 | 0.07729 |
| fees | 1.71767399 | 1.71767399 |

So the plumbing works: one decoder (`quant-binance::decode`) serves the live
source and the offline replay, `normalize --check` closes the last link by
proving Parquet replay matches raw replay event for event (21690 of them), and
nothing on the path reads a wall clock.

**Step 0 was not part of that rehearsal**, which is why it is spelled out above
rather than assumed. The `events 21690` in the table is its left half; the
capture's matching `records=` was never copied into this document, and a
rehearsal watched live can get away with that in a way a fortnight judged from
its logs afterwards cannot.

**Carry the strategy flags into step 4.** The command above omits
`--fast/--slow/--interval-secs` because the fortnight's paper defaults (10/30/60)
happen to equal the backtest's. They are *not* equal for a rehearsal at other
settings, and running the bare command against this rehearsal samples at 60 s over
a 600 s window: slow=30 never warms up, 0 crossings, 0 fills, cash 100.00000000.
That reads as a catastrophic platform failure and is a missing flag. Diff the two
`strategy` banner lines character for character before believing any divergence.

**Pass `--realistic` and nothing else cost-related.** It assigns the cost model
wholesale, so `--fee-rate 0.001 --realistic` silently discards the fee rate. Then
read the `costs` line back — it is printed from `engine.venue().costs()`, not from
the parsed flags, which is the M4 fix. If a `switched off for this run` section
appears, a cost was zeroed.

**`venue` fills may exceed `fills`, and that is not a disagreement.** This
rehearsal ended with `venue 23 fills, charged 1.79581017` against the portfolio's
22 and 1.71767399 — one fill's worth. Order #23 matched, but with 50 ms each way
its report was still in flight when the data ran out, and a report in flight is
counted rather than flushed, because flushing would tell a strategy something it
could not have known. The `reports still in flight` count on the same line is what
accounts for the difference.

**What the rehearsal does not cover.** One session, one segment, one day
partition, and the only gap is the mandatory `RecorderRestart` first frame. Untested
here: joining segments across UTC midnight (M2.c's whole reason for existing), a
real `Disconnect` gap with its resync, `LocalOverflow`, `SnapshotFailed`, and the
hourly periodic and stale-snapshot paths.

**The one thing that would have stopped the fortnight being judgeable is closed,
and how it is closed changed on 2026-09-20.** A recorder restart producing two
sessions on one symbol-day used to make `normalize --write` refuse the day rather
than merge it — safe rather than lossy, and it would have cost the whole window.
M2.e made such a day a sequence of parts, one per contributing session.

This section used to say those parts are *"ordered by the venue's own update-id
span recorded in each part's footer"*, and that was half right in a way that
mattered: right about the quantity, wrong about where it was applied. A part's
**index** is assigned in arrival order, which is `catalog` order, which is a
text sort over paths whose distinguishing component is a v4 UUID — so the index
carries no chronological information whatever. The writer nevertheless demanded
that an incoming part begin after every published part ended, which a genuinely
sequential restart satisfies only when the UUID sort happens to agree with time:
about half of the time. And a refused day was never only one day, because
`normalize` abandons the writer on the refusal, so the rest of that session's
days went unwritten with it.

The fix separates the two questions that had been fused. The writer now refuses
only **genuine overlap** — `PartsConcurrent`, meaning the two sessions recorded
the same messages at the same time, for which there is no ordering that is the
truth — and **order is read off the venue update-id span in each file's Parquet
footer at read time**, never from the index. That is the right place for it: the
span is a fact about what the venue sent, recorded once per part, strictly
increasing per symbol across disconnects, and immune to anything our clock or our
directory enumeration does. `local_recv_ts` is `SystemTime`, steps, and is what
the data contract's own sentence about the merge had wrongly named; the index is
what this document's own sentence had wrongly trusted.

What is left of the problem is a **reading** obligation, and it is the narrow
exception above — plus the state problem, which no reader can fix and which is
*A restart forfeits the exact comparison*.

**The risk-limit hole is now closed.** `backtest` used to wire `AllowAll` while
`paper` wired a real `RiskEngine`, so one refusal or one kill-switch trip during
the fortnight would have made the exact comparison impossible — the backtest
would send an order paper had refused, and every number after it would differ
for a reason that is not a bug. `backtest` now always wires a `RiskEngine` and
takes `--max-order`, `--max-position`, `--max-daily-loss` and `--max-orders`,
spelled exactly as `paper`'s so a paper run's arguments replay here verbatim.

Limits are **off by default**, on the same argument that keeps costs off: a bare
run has to stay byte-identical to the one before the feature existed.
`Limits::default()` permits everything, and `unset_limits_reproduce_the_unrisked_backtest_exactly`
pins that the real crossover behind a permissive `RiskEngine` produces the same
cash, fills, realized P&L and curve as the unrisked wiring.

So **pass the paper run's limits into step 4**. They are printed in the paper
session's own log (`limits    order 200, position 200, daily loss 20,
orders/day 200`) and `backtest` prints the line back from
`engine.risk().limits()` — the layer that did the refusing, not the flags it was
handed, which is M4's lesson applied to the second thing that can be configured
and not wired.

---

## What this run cannot tell us

**Queue position and market impact.** A paper venue uses simulated fills, so our
orders are never in the book and nobody is reacting to them — present tense,
because the fortnight is running as this is written and finishing it changes
nothing about that. Both remain unmeasured after this run, and only M8 — real
orders, real money — can measure them.

An earlier draft of the engine contract claimed otherwise. The correction is
recorded there rather than quietly fixed, because the wrong version made this run
look like it settled something it cannot.

---

## Known limits, deliberately

- **A reboot ends the run.** `supervise.sh` is a plain loop holding a
  `caffeinate` assertion, not a `launchd` job with `KeepAlive`. A run should be
  watched and should stop when told to, not silently resurrect itself. Same
  stance as M1.
- **A supervisor restart keeps the position and loses the comparison.** Strategy
  state is not journalled, so a restarted process satisfies criterion 4 and
  forfeits criterion 2. Argued in full under *A restart forfeits the exact
  comparison*; listed here because it belongs with the limits we chose rather
  than with the bugs we found — fixing it means a new journal entry, and the
  journal schema is frozen for the fortnight.
- **A hard kill between a risk trip and shutdown loses the trip.** The kill
  switch is journalled at shutdown, so `SIGKILL` in that window would let the next
  session start un-tripped. Acceptable for paper, where nothing is at stake;
  **must be fixed before M8**, by recovering the day's tally from the journal
  rather than only the switch.
