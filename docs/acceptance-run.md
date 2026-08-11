# The M1 acceptance run

**Status:** procedure · **Owner:** whoever starts it

Five of the six criteria in `data-contract.md` §7 are settled by a test suite and
a command. The sixth is not:

> It runs **7 consecutive days** unattended.

Nothing but time satisfies that, and this document is how it is spent.

---

## What is being claimed

That the recorder, left alone for a week against a live venue, produces a capture
in which **every discontinuity is explained by a record inside the capture
itself**. Not "no gaps" — gaps are expected, Binance closes connections every
twenty-four hours by design. The claim is that nothing is missing *without the
file saying so*.

That is a stronger and more useful property than uptime, and it is the one the
whole raw tier was built to make checkable.

---

## Running it

```powershell
# once, as Administrator, if preflight complains about the clock
powershell -ExecutionPolicy Bypass -File ops\fix-clock.ps1

# optional: the metadata index. The capture does not need it.
docker compose up -d
$env:QUANT_DATABASE_URL = 'postgres://quant:quant_local_dev@localhost:5432/quant'

powershell -ExecutionPolicy Bypass -File ops\start-run.ps1
```

Then, whenever you wonder:

```powershell
powershell -ExecutionPolicy Bypass -File ops\status.ps1
```

And to end it early:

```powershell
powershell -ExecutionPolicy Bypass -File ops\stop-run.ps1
```

`start-run.ps1` refuses to start on a preflight blocker. `-Force` overrides it and
records that it did so in `run.json`, because a run started over a known problem
should not be discovered to have been six months later.

---

## What each piece is for

| Script | Job |
|---|---|
| `preflight.ps1` | Refuse to waste a week. Clock, disk, build, clean root, power. |
| `supervise.ps1` | Keep one symbol recording; restart it if it dies. |
| `verify-loop.ps1` | Run the verifier every six hours, *during* the capture. |
| `status.ps1` | Answer "how is it going" in one command. |
| `stop-run.ps1` | Stop it in a way that still seals the files. |
| `fix-clock.ps1` | Start and sync `w32time`. Needs Administrator. |

### Restarting is outside the recorder, deliberately

The recorder exits only on a condition it has decided is fatal. That is correct:
a process that knows it can no longer do its job should stop rather than carry on
pretending. Keeping something running is a different concern with a different
lifetime, and it lives in the supervisor.

Nothing is lost across a restart, because the format was designed for it. A new
process takes a **new session id**, writes its own files, and puts a
`Gap{RecorderRestart}` in the very first frame — so a resumed capture states that
coverage was interrupted rather than quietly abutting two runs and looking
continuous. The verifier reads that record as the explanation for the
discontinuity it is about to find.

On a server this script is four lines of systemd (`Restart=always`). It is a
script because this host is Windows, not because supervision wants to be bespoke.

### Verification runs during, not after

This is the entire reason `quant-verify` reports through an exit code. Discovering
on day seven that the book stopped being anchored on day two costs six days;
discovering it six hours in costs six hours.

Expect warnings. The newest segment of each session is still open, so it has no
trailer and may have a torn tail — both are reported as warnings precisely so this
loop does not cry wolf every six hours. **Only errors fail the run.**

---

## Reading the metrics line

One line per minute per symbol:

```text
metrics symbol=BTCUSDT msgs_per_sec=37 bytes_per_sec=13070
        queue=0 queue_peak=18 queue_capacity=4096 dropped=0
        latency_p50_ms=41 latency_p90_ms=88 latency_p99_ms=140
        latency_samples=2276 clock_skew=0
        gap_disconnect=0 gap_overflow=0 gap_sequence=0
```

Read in this order:

- **`dropped`** — should be `0`, always. Anything else means we could not keep up.
  It is recorded honestly as a `LocalOverflow` gap and a hole of exactly the right
  width, so the capture stays trustworthy, but it is a capacity problem.
- **`queue` / `queue_peak` against `queue_capacity`** — the number §7 singles out.
  A steady zero means the writer keeps up. A peak approaching capacity means we
  came close to dropping without doing so.
- **`clock_skew`** — venue timestamps *ahead* of ours. Non-zero means the host
  clock is wrong, which makes every latency figure below it meaningless.
- **`gap_disconnect`** — a few a day is Binance behaving as documented. Dozens an
  hour is a network problem.
- **latency percentiles** — a step change matters more than the absolute value.

---

## When it finishes

```powershell
cargo run --release -p quant-verify --bin verify -- data\acceptance --reconcile
```

Exit `0` is the run passing. Then walk §7 explicitly:

- [ ] Seven consecutive days — `run.json` and the supervisor logs.
- [ ] Disconnects produced a gap, a backoff reconnect, and a fresh snapshot —
      `gap_disconnect` counts against `snapshots` in the logs, and the verifier
      finding no `unanchored-deltas`.
- [ ] `SIGKILL` mid-write leaves a readable file — already an ordinary unit test
      (`truncation_at_every_byte_offset_loses_only_the_tail`); if the run had a
      hard kill, the affected segment should verify with a `torn-tail` warning and
      no error.
- [ ] `ingest_seq` contiguous-or-explained — the verifier, exit `0`.
- [ ] Update-id chain contiguous-or-explained — the verifier, exit `0`.
- [ ] Metrics exist for all five quantities — the metrics lines above.

Keep `run.json`, the verifier's final report, and the supervisor logs with the
capture. A week of data whose provenance nobody can reconstruct is worth
noticeably less than one whose can.

---

## Known limits of a run on this host

Worth stating plainly rather than discovering.

- **A reboot ends the run.** Nothing here registers a service or a scheduled task.
  Adding that is easy and deliberately not done: an acceptance run should be
  watched, and something that silently resurrects itself is harder to reason about
  than something that stops.
- **Sleep looks exactly like a venue outage.** A gap, a reconnect, and no
  explanation beyond `Disconnect`. Preflight warns; the honest fix is a host that
  does not sleep.
- **Docker Desktop has stopped on its own twice** during development. The metadata
  tier is optional by design — the recorder logs a warning and keeps recording —
  so this costs index rows, never data. `--reconcile` will report exit `2`, which
  means *the check did not run*, not that the capture is bad.
- **One venue, two symbols.** Enough for one symbol's reconnect to be visible
  against the other still running, which is the property worth having. Scaling to
  fifty is where per-symbol connections stop being free, and that is noted in
  `quant-binance`'s crate docs as the point to revisit.
