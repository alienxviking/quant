# The M5 paper run

How the fortnight is conducted and judged. The companion to
`docs/acceptance-run.md`, which did the same for M1's seven days.

`docs/engine-contract.md` §9 has the criteria. This is the procedure.

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
```

The verify loop runs every six hours and now reconciles the journals on the same
cadence. `WAIT ... nothing to check yet` before the first fill is normal.

**A `msgs_per_sec=0` in the last metrics line is not a stall** — M1 learned this
the hard way. The honest liveness signal is the capture file growing, which
`status.sh` shows.

## Stopping it

```bash
ops/stop-run.sh --root ~/paper
```

`SIGINT`, not `SIGTERM`: the recorder's shutdown is `tokio::signal::ctrl_c`,
which is SIGINT on Unix. SIGTERM is not caught and would leave the last segment
trailerless — indistinguishable from a crash.

---

## Judging it

Bring the capture and the journals back (`COPYFILE_DISABLE=1 tar ...`, per
`docs/acceptance-run.md` — macOS otherwise writes AppleDouble stubs that the
verifier reports as strays).

```bash
git checkout m5-run-start          # the comparison runs at the run's commit

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

Step 4's result must match the paper session's. **Exactly** — both consumed the
same events with the same `local_recv_ts` and the same `ingest_seq`, because the
tee gave them the identical records. Any divergence is a bug, and finding out
which side is lying is then the whole job.

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
all four steps ran and **step 4 reproduced the paper session exactly**:

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
real `Disconnect` gap with its resync, `LocalOverflow`, `SnapshotFailed`, the hourly
periodic and stale-snapshot paths, and above all **a recorder restart producing two
sessions on one symbol-day, which `normalize --write` refuses rather than merges**
(M2.d, still open). That last one is the likeliest thing to stop the fortnight being
judgeable at all.

**That hole is now closed.** `backtest` used to wire `AllowAll` while `paper` wired
a real `RiskEngine`, so one refusal or one kill-switch trip during the fortnight
would have made the exact comparison impossible — the backtest would send an order
paper had refused, and every number after it would differ for a reason that is not
a bug. `backtest` now always wires a `RiskEngine` and takes `--max-order`,
`--max-position`, `--max-daily-loss` and `--max-orders`, spelled exactly as
`paper`'s so a paper run's arguments replay here verbatim.

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
orders were never in the book and nobody reacted to them. Both remain unmeasured
after this run, and only M8 — real orders, real money — can measure them.

An earlier draft of the engine contract claimed otherwise. The correction is
recorded there rather than quietly fixed, because the wrong version made this run
look like it settled something it cannot.

---

## Known limits, deliberately

- **A reboot ends the run.** `supervise.sh` is a plain loop holding a
  `caffeinate` assertion, not a `launchd` job with `KeepAlive`. A run should be
  watched and should stop when told to, not silently resurrect itself. Same
  stance as M1.
- **A hard kill between a risk trip and shutdown loses the trip.** The kill
  switch is journalled at shutdown, so `SIGKILL` in that window would let the next
  session start un-tripped. Acceptable for paper, where nothing is at stake;
  **must be fixed before M8**, by recovering the day's tally from the journal
  rather than only the switch.
