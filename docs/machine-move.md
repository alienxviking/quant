# Moving the artifacts to another machine

Written for the 2026-10 move of M5's fortnight from the MacBook Air to the HP
Omen, and general enough for the next one. `docs/acceptance-run.md` §"Getting the
data off the capture machine" is the ancestor of this procedure and still covers
M1's capture; this adds what a **migration** needs that a retrieval does not, and
what a **third party in the middle** changes.

---

## 1. What moves, and what deliberately does not

| | size | move it? |
|---|---|---|
| `raw/` | 4.5 G | **yes.** Immutable, irreplaceable, the source of truth |
| `normalized/` | 3.2 G | **no.** Disposable by contract — rebuild it there |
| `logs/` | 14 M | **yes.** M5's only evidence the tee dropped nothing |
| `paper-*.jsonl` | 356 K | **yes.** The journals. Irreplaceable |
| `run.json` | 4 K | yes. What the run was configured to do |

**4.6 GB moves, not 7.7**, and that is the storage-tier rule in
`docs/data-contract.md` §1 paying off in cash. The normalized tier is a *function*
of raw — `normalize --write` reproduces it, and M2 proved the reproduction is
exact: 70,545,345 events replayed from Parquet matched the raw replay event for
event. Copying it would mean moving a cache across a network and then trusting
the copy, when the thing it is a cache of is coming anyway.

There is a second reason, and it is the better one. If the Parquet arrives
corrupted in a way the checksum misses, every number derived from it is wrong and
nothing says so. If it is rebuilt, it is rebuilt from bytes the verifier has just
passed.

**Do not move `run.pids`.** It names processes on a machine that will no longer
exist, and `status.sh` reads it.

---

## 2. The rule that matters more than any step below

**Nothing is deleted from the source until the destination has verified.**

Not after the upload completes, not after the download completes, not after the
checksum matches — after `quant-verify` has read the extracted tree on the new
machine and exited 0. A checksum proves the bytes survived. The verifier proves
they still *mean* what they did, which is a different claim and the one that
matters.

The raw tier is the only irreversible artifact in this project. Everything else
in it can be rebuilt from raw; raw can be rebuilt from nothing.

---

## 3. Package

Only after the recorders have stopped and their trailers are sealed. A
duration-limit exit or `stop-run.sh` both do this; a `SIGKILL` leaves the last
segment trailerless, which is legal and reported but is not a thing to introduce
on purpose at the moment of moving.

```bash
cd ~
COPYFILE_DISABLE=1 tar czf ~/quant-m5-fortnight.tar.gz \
    paper/raw paper/logs paper/paper-BTCUSDT.jsonl paper/paper-ETHUSDT.jsonl paper/run.json
shasum -a 256 ~/quant-m5-fortnight.tar.gz | tee ~/quant-m5-fortnight.tar.gz.sha256
```

`COPYFILE_DISABLE=1` is not decoration. macOS `tar` archives extended attributes
as sibling **AppleDouble** stubs — `._part-00000.bin.zst` next to the real file —
and the 2026-08 transfer arrived with 90 of them, 51 inside `raw/`. The documented
procedure was followed and still produced them, which is why it is the first thing
in this section rather than a footnote. They are inert, and the verifier is right
to report them: something unexpected in the immutable tier gets a line of output
whatever it turns out to be.

If a tree already has them:

```bash
find ~/paper -name '._*' -type f -delete
```

`czf` rather than `cf` — the payload is already zstd so gzip buys little on the
capture itself, but the logs are 14 MB of text and compress by about ten to one,
and one file is one thing to checksum.

**Watch the disk.** This Mac has ~11 GB free against a 4.5 GB source, so the
archive fits but not comfortably. If it does not, pipe straight to the
destination instead of staging a file:

```bash
COPYFILE_DISABLE=1 tar czf - paper/raw paper/logs paper/*.jsonl paper/run.json \
  | ssh you@omen 'cat > quant-m5-fortnight.tar.gz'
```

That loses the convenient local checksum, so take one on each side of the pipe
instead (`tee >(shasum -a 256 …)`).

---

## 4. Through Google Drive, and what a third party changes

Drive is fine for this and introduces exactly one new failure mode worth naming:
**the file that arrives is not necessarily the file that left**, and nothing in
the browser will tell you. Resumable uploads can truncate, a sync client can
decide a partially-written file is complete, and a 4.6 GB transfer is long enough
for all of that.

So the checksum is not optional here, and it is the reason the sidecar exists:

1. Upload **both** `quant-m5-fortnight.tar.gz` and its `.sha256`.
2. Upload them as *files*, not into a folder Drive might "optimise". Do not let
   Drive unpack the archive.
3. On the Omen, download both and compare before extracting anything.

```powershell
# PowerShell 5.1+ has a hasher and tar built in (Windows 10+)
Get-FileHash quant-m5-fortnight.tar.gz -Algorithm SHA256
Get-Content quant-m5-fortnight.tar.gz.sha256     # compare by eye, or:
(Get-FileHash quant-m5-fortnight.tar.gz -Algorithm SHA256).Hash.ToLower() -eq `
    ((Get-Content quant-m5-fortnight.tar.gz.sha256) -split '\s+')[0]
```

A `False` there means download it again. It does not mean investigate — a
corrupted archive carries no information worth recovering, and the source still
exists because of §2.

---

## 5. Extract and verify

```powershell
tar xzf quant-m5-fortnight.tar.gz          # restores paper/raw, paper/logs, the journals
```

Then the three checks, in order of how much they prove:

```powershell
# 1. The bytes mean what they did. THIS is the gate -- exit 0, and the frame
#    count must read 97,937,822.
cargo run --release -p quant-verify -- .\paper

# 2. The journals still fold. 166 checkpoints each, both AGREE.
cargo run --release -p quant-backtest --bin reconcile -- .\paper\paper-BTCUSDT.jsonl
cargo run --release -p quant-explain --bin explain -- --check-journal .\paper\paper-BTCUSDT.jsonl

# 3. Rebuild the tier that was deliberately left behind.
cargo run --release -p quant-normalize --bin normalize -- .\paper --write --check
```

**And then the regression that is worth more than all three**, because it
exercises the whole stack end to end against a number this repository has
published:

```powershell
cargo run --release -p quant-backtest --bin backtest -- .\paper --symbol BTCUSDT `
    --realistic --cash 100 --qty 0.001 --fast 10 --slow 30 --interval-secs 60 `
    --max-order 200 --max-position 200 --max-daily-loss 20 --max-orders 200
```

It must print **824 fills** and **cash 31.64743842**. That number is M5's
published result, it is the no-op property every money-path change since has been
checked against, and reproducing it on a different machine, a different operating
system and a different CPU architecture is a stronger statement than any checksum:
it says the data survived *and* the platform is deterministic across all of that.

Only now is it safe to delete anything on the Mac.

---

## 6. What the Omen needs before it can do more than read

Moving the data is not moving the ability to run. Three things, in increasing
order of how much work they are.

**The toolchain.** Proven already — the project was built on Windows through M4
and `rust-toolchain.toml` pins the channel. `zstd-sys` compiles C and `ring`
assembles per-architecture, both of which worked there before. If cargo is missing
from an old shell, `$env:Path = "$env:USERPROFILE\.cargo\bin;$env:Path"`.

Smart App Control was disabled on 2026-07-29 and cannot be re-enabled without a
reinstall, so the `os error 4551` build failures should not recur.

**The clock, and this one is now blocking rather than cosmetic.** M1 declined to
start the acceptance run on this machine partly because `w32time` was stopped and
the host sat ~2 s ahead of Binance. That was tolerable then: a wrong clock costs a
capture a latency metric and nothing else, which is why the recorder *warns* above
1000 ms rather than refusing.

M8 changed that. A signed request carries a timestamp and Binance rejects it
outside `recvWindow`, so `TradeClient::check_ready` **refuses to trade** above
1000 ms. Check it before anything else:

```powershell
w32tm /query /status
w32tm /resync
cargo run --release -p quant-binance --bin venue-check    # needs the API keys
```

`venue-check` prints the round-trip-corrected offset against the venue's own
clock, which is the quantity that actually matters — measuring `now - serverTime`
naively folds one-way latency into the offset and would block a run over a clock
that is fine (seen live at M1: −1445 ms naive against +450 ms corrected).

**The ops harness, and this is the real gap.** `ops/*.ps1` is **record-only**.
The paper and live modes exist in the bash half alone, deliberately: *"an untested
paper mode there would be exactly the harness-never-run M1 warns about"*. So a
long M8 run on the Omen needs either WSL — where the bash harness runs unmodified
and is the proven path — or a PowerShell port that is then rehearsed, which is a
slice of work in its own right and must not be written the night before a run.

WSL is the cheaper answer and the one with evidence behind it. The thing to check
there is sleep: a 72-hour run needs the host awake, and `caffeinate` has no WSL
equivalent — that is Windows power settings, not a script.

---

## 7. Where development should happen, which is a separate question

Development on the Omen is not merely acceptable, it is what `CLAUDE.md` already
recommends. During the fortnight the rule was to build in a worktree on the Mac or
on the Windows box, and of the two the Windows box was called *"safer still and
needs no worktree: it is a different machine and cannot reach the Mac at all."*

That reasoning survives the move intact, with the machines swapped. If M8's run
ends up on the Mac — which the harness gap in §6 makes likely — then the Omen is
the development machine and the isolation is free.
