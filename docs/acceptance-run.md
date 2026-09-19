# The M1 acceptance run

**Status:** procedure · **Owner:** whoever starts it

Six of the seven criteria in `data-contract.md` §7 are settled by a test suite and
a command. The seventh is not:
a command. The sixth is not:

> It runs **7 consecutive days** unattended.

Nothing but time satisfies that, and this document is how it was spent.

**It passed.** 2026-08-21 → 2026-08-28, two symbols on an Apple Silicon MacBook
Air, zero recorder restarts: `quant-verify` exit `0`, 70,545,346 frames,
`dropped=0`, 38 gap frames and every one of them explained by a record inside the
capture. The run's own account — what the week did, and the handful of things it
taught — is in `CLAUDE.md` under *The acceptance run, and how it went*.

What follows stays in the imperative, because it is the procedure for the *next*
run rather than a memoir of the first. M5's fortnight already borrows most of it.

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

The harness exists twice: PowerShell (`ops/*.ps1`) for the Windows host it was
written on, and a line-for-line macOS/bash port (`ops/*.sh`) for the Apple Silicon
machine the run actually moved to. The two are behaviourally the same; the reasons
are in each script's header and the design decisions did not change in the port.

### macOS (Apple Silicon)

```bash
# once, if preflight complains about the clock (the check itself needs no sudo):
sudo ops/fix-clock.sh

# optional: the metadata index. The capture does not need it.
docker compose up -d
export QUANT_DATABASE_URL='postgres://quant:quant_local_dev@localhost:5432/quant'

ops/start-run.sh
```

Then, whenever you wonder:

```bash
ops/status.sh
```

And to end it early:

```bash
ops/stop-run.sh
```

Rehearse the whole chain first — a harness that has never been run is not a
harness: Rehearse the whole chain first — a harness that has never been run is not a
harness: `ops/start-run.sh --minutes 5 --root ~/rehearsal` runs start → supervise
→ record → verify → status → stop in a few minutes. Pass `--root`: `--minutes`
shortens the run but does not move it, so a rehearsal without one files its frames
in `data/acceptance` and preflight blocks the real run on a root that already
holds captures — the check doing its job at the least convenient moment.
status → stop in a few minutes against a throwaway root.

macOS specifics, all handled by the scripts unless noted:

- **Prerequisites**: Xcode Command Line Tools (`xcode-select --install`, for the
  `zstd-sys` C build) and `rustup`. Both are cleanly removable afterwards.
- **Sleep**: each recorder runs under `caffeinate -dimsu` for its lifetime, so the
  system stays awake while it records — **but only with the lid open on mains.** A
  closed lid still sleeps unless you also run `sudo pmset -a disablesleep 1` (undo
  with `0`). Preflight reports the current sleep policy.
- **Clean shutdown is SIGINT.** `stop-run.sh` sends the recorder `SIGINT`
  (`tokio::signal::ctrl_c`), which seals the trailer; `SIGTERM`/`SIGKILL` would
  leave the last segment looking like a crash. Do not `kill` the recorder by hand.
- **The clock check is round-trip corrected.** From a home connection several
  thousand km from the venue, one-way latency alone can be hundreds of ms, so the
  naive `now - serverTime` the Windows script uses would block a run over a clock
  that is fine. `preflight.sh` and `fix-clock.sh` use the NTP midpoint estimate
  instead, and confirm sync read-only with `sntp` (no sudo).

### Windows

```powershell
# once, as Administrator, if preflight complains about the clock
powershell -ExecutionPolicy Bypass -File ops\fix-clock.ps1

# optional: the metadata index. The capture does not need it.
docker compose up -d
$env:QUANT_DATABASE_URL = 'postgres://quant:quant_local_dev@localhost:5432/quant'

powershell -ExecutionPolicy Bypass -File ops\start-run.ps1
```

Then `ops\status.ps1` to check, `ops\stop-run.ps1` to end it early.

`start-run` (either host) refuses to start on a preflight blocker. `--force`
(`-Force` on Windows) overrides it and records that it did so in `run.json`,
because a run started over a known problem should not be discovered to have been
six months later.

---

## What each piece is for

| Script | Job |
|---|---|
| Script | Job |
|---|---|
| `preflight.{ps1,sh}` | Refuse to waste a week. Clock, disk, build, clean root, power. `preflight.sh` builds `--workspace --bins` rather than a named list, because a list of what the run needs went stale the moment `--paper` existed. |
| `supervise.{ps1,sh}` | Keep one symbol recording; restart it if it dies. `supervise.sh` takes a mode, so it supervises a paper session the same way — a parameter rather than a second script, because two harnesses would have to agree forever. |
| `verify-loop.{ps1,sh}` | Run the verifier every six hours, *during* the capture. `verify-loop.sh` reconciles any paper journals under the root on the same tick, for the same reason. |
| `status.{ps1,sh}` | Answer "how is it going" in one command. |
| `stop-run.{ps1,sh}` | Stop it in a way that still seals the files. |
| `fix-clock.{ps1,sh}` | Turn network time on and step the clock: `w32time` on Windows (Administrator), `systemsetup` and `sntp` on macOS (sudo). |
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
        latency_p50_ms=41 latency_p90_ms=88 latency_p99_ms=140 latency_max_ms=212
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
  `latency_max_ms` is beside them because a percentile reports its bucket's
  *upper* bound; a line reading `p99=3145 max=3071` was correct by derivation and
  nonsense to read, which is why percentiles are now clamped to the observed
  maximum.

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

## Getting the data off the machine

The raw tier lives outside git (gitignored `/data/`), so it moves out-of-band. The
capture is a few GB, and the machine it ran on is often not the one it will be used
on — the 2026-08 run recorded on an Apple Silicon Mac and was carried back to a
Windows laptop. The method (USB drive, or a cloud service like Google Drive) does
not matter; getting the *same bytes* to the other side, provably, does.

**1. Stop cleanly first, then package as one file.** Only package after the
recorders have stopped and their trailers are sealed (a duration-limit exit or
`stop-run` both do this). One tarball is easier to move and verify than a deep
folder tree, and it preserves the `raw/exchange=…/symbol=…/date=…/session=…`
layout the verifier depends on. Don't re-compress — the payload is already zstd.

```bash
# macOS/Linux, from the capture root's parent (e.g. quant/data)
COPYFILE_DISABLE=1 tar cf ~/quant-acceptance.tar acceptance   # raw/ + logs/ + run.json, and a paper run's paper-*.jsonl
shasum -a 256 ~/quant-acceptance.tar | tee ~/quant-acceptance.tar.sha256
```

`COPYFILE_DISABLE=1` is not decoration. macOS `tar` archives each file's extended
attributes as a sibling **AppleDouble** stub — `._part-00000.bin.zst` beside the
real thing — and the 2026-08 transfer arrived with 90 of them, 51 inside `raw/`.
They are inert, but the verifier is right to notice: something unexpected in the
immutable tier gets a line of output whatever it turns out to be. Suppressing them
at the source is better than teaching the verifier to ignore a shape of file,
because a verifier that has learned to ignore things is how one stops catching
them.

If a capture already has them, they are safe to remove — nothing in the raw tier
depends on a resource fork:

```bash
find <capture root> -name '._*' -type f -delete
```

Carry the `.sha256` sidecar alongside the tarball — it is how the other side
proves the transfer was lossless. A flaky-network upload that silently corrupts a
byte is exactly the failure this whole project refuses to trust to luck.

**2. Verify and extract on the other machine.** On Windows, PowerShell has both a
hasher and `tar` built in (Windows 10+):

```powershell
# compare this against the value inside quant-acceptance.tar.sha256
certutil -hashfile quant-acceptance.tar SHA256
tar xf quant-acceptance.tar                        # restores the raw/ tree
```

**3. Re-verify the data itself — the gold standard.** A matching checksum proves
the bytes survived; running the verifier proves they still *mean* what they did.
Build `quant-verify` on the target machine and point it at the extracted root:

```powershell
cargo run --release -p quant-verify --bin verify -- .\acceptance
```

Exit `0` there is the strongest possible statement: the week of data is intact and
every discontinuity is still explained, on a machine that never saw it recorded.

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
