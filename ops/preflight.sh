#!/usr/bin/env bash
# preflight.sh -- check that this Mac can be trusted with a long unattended run.
#
# macOS port of ops/preflight.ps1. Written for M1's seven-day capture and used
# unchanged by M5's paper fortnight, which captures raw *and* trades it from one
# ingress -- so every check here applies to both worlds, and a check written as
# if only a recorder existed is a bug rather than a scope decision. That is not
# hypothetical: the build step below shipped a hand-written list of two binaries
# and stayed silent about the two a paper run launches.
#
# Every check exists because failing it wastes days rather than minutes. A
# process that starts and runs is not the same as one whose output will be worth
# anything at the end, and the difference is almost always knowable up front: a
# clock that is wrong, a disk that will fill on day five, a build that is not the
# one being tested.
#
# Blockers exit non-zero. Advisories print and continue. The distinction is
# whether the run would produce data that cannot be trusted (blocker) or data
# that is fine but less useful (advisory).
#
# Usage: preflight.sh [--root DIR] [--days N] [--symbols "BTCUSDT ETHUSDT"]
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"

root=""
days=7
symbols="BTCUSDT ETHUSDT"
while [ $# -gt 0 ]; do
    case "$1" in
        --root)    root="$2"; shift 2;;
        --days)    days="$2"; shift 2;;
        --symbols) symbols="$2"; shift 2;;
        *) echo "unknown argument: $1" >&2; exit 2;;
    esac
done
[ -n "$root" ] || root="$repo/data/acceptance"

# Budgeted well above the ~160 MB/day/symbol observed on a quiet market, because
# the point of a headroom check is to survive a busy one. A week of frantic
# trading is the run we most want to have recorded.
#
# A paper run is budgeted by the same number, and that is measured rather than
# assumed -- the question is obvious enough that the next person will ask it, and
# an unrecorded answer gets re-derived. M5's ten-minute rehearsal wrote 1.22 MB
# of raw -- 176 MB/day/symbol, which is the ~160 above and the same order either
# way -- against 4.5 KB of journal and 8 KB of logs. The journal grows by one
# line per fill plus one checkpoint per *five* of them -- this said 25, which was
# the cadence before 766b784 lowered it so that every six-hourly reconcile pass
# has something new to check. At M4's observed turnover of ~60 fills a day that
# is ~840 fills and ~170 checkpoints over a fortnight, so call it 200 KB rather
# than the 160 KB written here before. The conclusion is untouched: it is still
# four ten-thousandths of a single day's raw, which is why the stale number cost
# nothing and is corrected rather than merely deleted. Paper trades the stream it
# is already recording, so it writes no materially different volume and there is
# nothing here to parameterise by mode.
mb_per_symbol_per_day=1024
max_clock_offset_ms=1000

# shellcheck disable=SC2206
symbol_arr=($symbols)
nsym=${#symbol_arr[@]}

blockers=()
advisories=()

report() { # name  verdict(ok|advisory|blocker)  detail
    local name="$1" verdict="$2" detail="$3" mark
    case "$verdict" in
        ok)       mark="  ok  ";;
        advisory) mark=" note ";;
        blocker)  mark="BLOCK ";;
    esac
    printf '%s %-22s %s\n' "$mark" "$name" "$detail"
    [ "$verdict" = blocker ]  && blockers+=("$name : $detail")
    [ "$verdict" = advisory ] && advisories+=("$name : $detail")
    return 0
}

echo "preflight for a ${days}-day capture of ${symbols// /, }"
echo "repo   $repo"
echo "root   $root"
echo ""

# --- the build under test -------------------------------------------------
# Release, not debug: a run measured in days should be the binary we would
# deploy. Debug builds carry overflow checks and no optimisation, and a run that
# proves a debug build works has proved nothing about a release one.
#
# `--workspace --bins`, not a hand-written list of what this run needs. The list
# was `record` and `verify`, which was true while only the recorder ran
# unattended and became false the moment `--paper` existed: supervise.sh reaches
# for target/release/paper, verify-loop.sh for target/release/reconcile. This
# step could report "build ok" on a tree where the paper run cannot start, and it
# failed quietly in every direction -- supervise.sh's own -x check fires into a
# nohup'd log while start-run.sh prints "running", and verify-loop.sh does not
# check at all, so an absent `reconcile` exits 127 and logs FAIL against a
# healthy journal every cycle for a fortnight.
#
# Two shapes were rejected. Enumerating the four binaries a paper run needs fixes
# today's list and leaves tomorrow's to drift the same way. Selecting them from a
# --paper flag puts the build behind a caller remembering which world it is in --
# and start-run.sh does not pass the mode today, which is precisely how this was
# missed; a check that can quietly not happen is the worst outcome available. The
# workspace already knows what it can run, and asking it cannot go stale.
#
# This step does not only test, it *produces*: anything it skips is whatever was
# left in target/. A present-but-stale `paper` is worse than an absent one,
# because start-run.sh writes the commit preflight just checked into run.json, so
# the manifest would name code that did not produce the result. The rehearsal
# only got four current binaries because docs/paper-run.md tells you to run a
# bare `cargo build --release` first -- a documented manual step is exactly the
# thing preflight exists to stop depending on.
#
# Cost is near nothing over naming the four by hand: quant-backtest depends on
# quant-normalize, so `paper` drags in parquet/arrow either way, and `normalize`
# and `backtest` are then free. The strictness -- an unrelated broken binary
# blocks the run -- is right rather than collateral: a tag whose workspace does
# not build is not one to pin a fortnight to, and CI already compiles every bin.
export PATH="$HOME/.cargo/bin:$PATH"
build_log="$(mktemp -t quant-preflight-build.XXXXXX.log)"
if ( cd "$repo" && cargo build --release --workspace --bins ) >"$build_log" 2>&1; then
    # Ask the filesystem what is there rather than trusting that the build
    # command covered it -- M4's lesson, where the report announced ten basis
    # points that had never been wired to the venue. Print what was used, not
    # what was asked for. This is also what catches a renamed or moved binary,
    # which a green `cargo build` would say nothing about.
    missing=""
    for wanted in record verify paper reconcile; do
        [ -x "$repo/target/release/$wanted" ] || missing="$missing $wanted"
    done
    if [ -n "$missing" ]; then
        report 'build' blocker "cargo build succeeded but the harness launches these and they are not there:$missing"
    else
        report 'build' ok 'release binaries present: record, verify, paper, reconcile'
    fi
else
    report 'build' blocker "cargo build --release failed, see $build_log"
fi

# --- the host clock -------------------------------------------------------
# The one that caught the Windows machine out. Venue latency is
# local_recv_ts - exchange_ts, so a wrong clock reports its offset as latency on a
# healthy link. It does not corrupt the capture -- every timestamp is shifted
# equally, so ordering and dispatch are unaffected -- but it makes one of the six
# acceptance criteria unmeasurable, which is reason enough not to start a week on
# it.
#
# Difference from the PowerShell version, and deliberate: that script measured
# `before - serverTime`, which folds the one-way network latency to the venue
# into the number. On a low-latency link that is a few ms and harmless; from a
# home connection several thousand km from the venue it can be hundreds of ms and
# dominate the reading, blocking a run over a clock that is actually fine. This
# uses the NTP round-trip estimate -- serverTime minus the local midpoint of the
# request -- which cancels symmetric latency and measures the clock, not the
# link. It is also exactly the quantity `clock_skew` in the metrics line reflects.
clock_out="$(python3 - <<'PY' 2>/dev/null
import time, json, urllib.request
best=None
for _ in range(5):
    try:
        t0=time.time()*1000
        with urllib.request.urlopen("https://api.binance.com/api/v3/time", timeout=15) as r:
            t1=json.load(r)["serverTime"]
        t2=time.time()*1000
    except Exception:
        continue
    off=t1-(t0+t2)/2   # positive => host behind the venue
    rtt=t2-t0
    if best is None or rtt<best[1]:
        best=(off,rtt)   # the lowest-RTT sample carries the least asymmetry error
if best is None:
    print("ERR")
else:
    print(f"{best[0]:.0f} {best[1]:.0f}")
PY
)"
if [ "$clock_out" = "ERR" ] || [ -z "$clock_out" ]; then
    report 'host clock' blocker 'could not reach the venue to check'
else
    offset_ms=${clock_out%% *}
    rtt_ms=${clock_out##* }
    abs=${offset_ms#-}
    if [ "$abs" -gt "$max_clock_offset_ms" ]; then
        report 'host clock' blocker "${offset_ms}ms from the venue (rtt ${rtt_ms}ms, limit ${max_clock_offset_ms}ms) -- run: sudo ops/fix-clock.sh"
    else
        report 'host clock' ok "${offset_ms}ms from the venue (rtt ${rtt_ms}ms)"
    fi
fi

# --- the time service -----------------------------------------------------
# A clock that is right now but unsynchronised will drift over seven days, and a
# slow drift is harder to spot afterwards than a constant offset. The question is
# whether the clock is being *disciplined*, and the direct way to answer it needs
# no privileges: query an NTP server read-only with `sntp` and read the offset it
# reports. A small offset means `timed` is keeping the clock honest; a large one
# means it is not, whatever any setting claims.
#
# `systemsetup -getusingnetworktime` is only a fallback and is quietly broken
# without root -- it prints "You need administrator access ... exiting!" to stdout
# and still exits 0, so its output is checked for that sentinel rather than
# trusted blindly.
ntp_server='time.apple.com'
sntp_off="$(sntp "$ntp_server" 2>/dev/null | awk 'NF>=2 && ($1 ~ /^[-+]?[0-9]/){print $1; exit}')"
if [ -n "$sntp_off" ]; then
    off_ms="$(python3 -c "print(round(abs(float('$sntp_off'))*1000))" 2>/dev/null)"
    if [ -n "$off_ms" ] && [ "$off_ms" -le 250 ]; then
        report 'time service' ok "clock is disciplined (${off_ms}ms off ${ntp_server} via sntp)"
    elif [ -n "$off_ms" ] && [ "$off_ms" -le 1000 ]; then
        report 'time service' advisory "clock is ${off_ms}ms off ${ntp_server} -- synced but loose; sudo ops/fix-clock.sh to tighten it"
    else
        report 'time service' advisory "clock is ${off_ms:-?}ms off ${ntp_server} -- sudo ops/fix-clock.sh"
    fi
else
    nt="$(systemsetup -getusingnetworktime 2>/dev/null)"
    if echo "$nt" | grep -qi 'administrator\|exiting'; then
        report 'time service' advisory 'could not measure sync without sudo -- macOS syncs by default; sudo ops/fix-clock.sh to be sure'
    elif echo "$nt" | grep -qi 'network time: on'; then
        report 'time service' ok 'network time is on'
    else
        report 'time service' advisory "network time appears off -- sudo ops/fix-clock.sh"
    fi
fi

# --- disk -----------------------------------------------------------------
root_parent="$(dirname "$root")"
[ -d "$root_parent" ] || root_parent="$repo"
free_kb="$(df -k "$root_parent" | awk 'NR==2 {print $4}')"
free_gb=$(python3 -c "print(round($free_kb/1024/1024,1))")
need_gb=$(python3 -c "print(round($mb_per_symbol_per_day*$nsym*$days/1024,1))")
if python3 -c "import sys; sys.exit(0 if $free_kb*1024 >= $need_gb*1024**3 else 1)"; then
    report 'disk' ok "${free_gb}GB free, budgeting ${need_gb}GB"
else
    report 'disk' blocker "${free_gb}GB free, need about ${need_gb}GB"
fi

# --- a clean root ---------------------------------------------------------
# The run's verdict has to mean something. Pointing the verifier at a tree that
# already holds captures from an older build makes every report ambiguous -- which
# is exactly what the pre-snapshot sessions in this repo's data/raw do.
if [ -d "$root/raw" ]; then
    existing="$(find "$root/raw" -name 'part-*.bin.zst' 2>/dev/null | wc -l | tr -d ' ')"
    if [ "$existing" -gt 0 ]; then
        report 'capture root' blocker "$existing capture files already here -- the run's verdict would cover them too"
    else
        report 'capture root' ok 'empty'
    fi
else
    report 'capture root' ok 'will be created'
fi

# --- a clean root, part two: the journal ----------------------------------
# The check above walks $root/raw, which is everything a recorder leaves in the
# root and not everything a paper run does. The journal lands at
# $root/paper-SYMBOL.jsonl -- beside raw/, not inside it -- and nothing else
# looks there: quant-recorder::catalog walks only root/raw, so a stale journal is
# invisible to quant-verify. This is the one place it can be caught.
#
# And leaving one is not a stray file, it is a silently different run. The paper
# binary *resumes* a journal it finds: starting capital comes from the old file's
# first `Started` line, so --cash is ignored; position, realized, fees and the
# fill count are inherited; and a `Tripped` in it re-arms the kill switch, so the
# fortnight would refuse every order from its first second while looking healthy.
# M5's criterion compares paper P&L against a backtest over the data captured in
# the *same window*, and a resumed journal carries fills from outside it -- so the
# comparison would fail for a reason unrelated to live-versus-replay, which is
# the only thing being asked.
#
# Blocked in both modes rather than only under --paper, for two reasons. It is
# not mode-specific: verify-loop.sh globs $root/paper-*.jsonl whatever the run
# is, so a stale journal under a recorder run gets reconciled every cycle and
# logs a P&L against a session that did not produce it. And a check that only
# happens when a caller remembers to pass a flag is a check that can quietly not
# happen -- start-run.sh passes no mode today.
#
# Resuming on purpose stays available and stays on the record: --force writes
# preflight=forced into run.json, so a deliberately resumed run cannot later be
# mistaken for a fresh one. A restart *within* a run is untouched, because
# preflight runs once from start-run.sh and supervise.sh re-execs the binary
# directly. This only sees the default path; a --journal in --paper-args puts the
# file somewhere preflight cannot know about, which is the caller's to own.
journals="$(find "$root" -maxdepth 1 -name 'paper-*.jsonl' 2>/dev/null | wc -l | tr -d ' ')"
if [ "$journals" -gt 0 ]; then
    report 'paper journal' blocker "$journals journal(s) already in the root -- a paper run resumes them, so starting cash, position and any tripped kill switch would come from that session and not this one"
else
    report 'paper journal' ok 'no journal to resume'
fi

# --- metadata tier (optional by design) -----------------------------------
if [ -z "${QUANT_DATABASE_URL:-}" ]; then
    report 'metadata' advisory 'QUANT_DATABASE_URL unset -- recording without an index, which is allowed'
else
    if pg="$(docker ps --filter 'name=quant-postgres' --format '{{.Status}}' 2>/dev/null)"; then
        if [ -z "$pg" ]; then
            report 'metadata' advisory 'quant-postgres not running -- start it with docker compose up -d'
        else
            report 'metadata' ok "quant-postgres $pg"
        fi
    else
        report 'metadata' advisory 'docker unreachable -- the index will be missing, the capture will not be'
    fi
fi

# --- power / sleep --------------------------------------------------------
# A laptop that sleeps has stopped recording, and it looks exactly like a venue
# outage in the capture: a gap, a reconnect, and no explanation. The run launches
# each recorder under `caffeinate -dimsu`, which holds the system awake while it
# runs -- but only with the lid open on mains. A closed lid sleeps regardless
# unless `sudo pmset -a disablesleep 1` is set, so this stays an advisory.
sleep_ac="$(pmset -g custom 2>/dev/null | awk '/^AC Power/{ac=1} ac && $1=="sleep"{print $2; exit}')"
[ -n "$sleep_ac" ] || sleep_ac="$(pmset -g 2>/dev/null | awk '$1=="sleep"{print $2; exit}')"
if [ "${sleep_ac:-}" = "0" ]; then
    report 'sleep' ok 'system sleep is disabled on AC'
elif [ -n "${sleep_ac:-}" ]; then
    report 'sleep' advisory "system sleeps after ${sleep_ac}min on AC -- caffeinate holds it awake with the lid open on mains; a closed lid still sleeps"
else
    report 'sleep' advisory 'could not read the sleep setting -- keep the lid open on mains'
fi

echo ""
if [ ${#advisories[@]} -gt 0 ]; then
    echo "advisories:"
    for a in "${advisories[@]}"; do echo "  - $a"; done
fi
if [ ${#blockers[@]} -gt 0 ]; then
    echo ""
    echo "BLOCKED. Fix these, or pass --force to start-run.sh to accept them:"
    for b in "${blockers[@]}"; do echo "  - $b"; done
    exit 1
fi

echo "ready."
exit 0
