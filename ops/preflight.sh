#!/usr/bin/env bash
# preflight.sh -- check that this Mac can be trusted with a seven-day capture.
#
# macOS port of ops/preflight.ps1. Every check here exists because failing it
# wastes days rather than minutes. A recorder that starts and runs is not the
# same as one whose output will be worth anything at the end of the week, and the
# difference is almost always knowable up front: a clock that is wrong, a disk
# that will fill on day five, a build that is not the one being tested.
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
# Release, not debug: a seven-day run should be the binary we would deploy.
# Debug builds carry overflow checks and no optimisation, and a run that proves a
# debug build works has proved nothing about a release one.
export PATH="$HOME/.cargo/bin:$PATH"
build_log="$(mktemp -t quant-preflight-build.XXXXXX.log)"
if ( cd "$repo" && cargo build --release -p quant-binance --bin record -p quant-verify --bin verify ) >"$build_log" 2>&1; then
    report 'build' ok 'release binaries built'
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
