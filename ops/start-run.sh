#!/usr/bin/env bash
# start-run.sh -- start the M1 acceptance run: supervised recorders plus periodic verification.
#
# macOS port of ops/start-run.ps1. The last criterion in docs/data-contract.md §7
# is the one only time can satisfy -- seven consecutive days unattended, with zero
# unexplained gaps. This starts it.
#
# One supervised recorder per symbol, because the recorder is one process per
# instrument by design: routing a multiplexed stream would mean parsing every
# payload on the hot path, and separate sockets mean one symbol's reconnect blinds
# only that symbol. A verification loop runs alongside, during the capture rather
# than after it, so a defect costs hours instead of the week.
#
# Everything is detached with nohup, so closing this terminal does not stop the
# run. Nothing here survives a reboot -- see docs/acceptance-run.md for why that is
# a deliberate limit of a laptop run rather than something to paper over.
#
# Usage:
#   start-run.sh [--symbols "BTCUSDT ETHUSDT"] [--root DIR] [--days N]
#                [--verify-interval-hours H] [--minutes M] [--force]
#
#   --minutes M   Rehearsal. A run harness that has never been run is not a
#                 harness, and the worst moment to discover a typo in it is four
#                 days into a capture. Overrides --days and tightens the verifier
#                 cadence so a few minutes exercises every path.
#   --force       Start despite preflight blockers, recording that it did so.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"

symbols="BTCUSDT ETHUSDT"
root=""
days=7
verify_interval_hours=6
minutes=0
force=0
while [ $# -gt 0 ]; do
    case "$1" in
        --symbols) symbols="$2"; shift 2;;
        --root)    root="$2"; shift 2;;
        --days)    days="$2"; shift 2;;
        --verify-interval-hours) verify_interval_hours="$2"; shift 2;;
        --minutes) minutes="$2"; shift 2;;
        --force)   force=1; shift;;
        *) echo "unknown argument: $1" >&2; exit 2;;
    esac
done
[ -n "$root" ] || root="$repo/data/acceptance"
log_dir="$root/logs"
# shellcheck disable=SC2206
symbol_arr=($symbols)

# --- preflight ------------------------------------------------------------
"$here/preflight.sh" --root "$root" --days "$days" --symbols "$symbols"
preflight=$?
if [ "$preflight" -ne 0 ]; then
    if [ "$force" -ne 1 ]; then
        echo ""
        echo "not starting. Fix the blockers above, or re-run with --force to accept them."
        exit 1
    fi
    echo ""
    echo "--force: starting anyway, with the blockers above on the record."
fi

mkdir -p "$root" "$log_dir"
root="$(cd "$root" && pwd)"   # canonicalise now that it exists
log_dir="$root/logs"

# --- timing ---------------------------------------------------------------
started_epoch="$(date +%s)"
if [ "$minutes" -gt 0 ]; then
    rehearsal=1
    end_epoch=$(( started_epoch + minutes * 60 ))
    first_check_seconds=45
    verify_every=1
else
    rehearsal=0
    end_epoch=$(( started_epoch + days * 86400 ))
    first_check_seconds=300
    verify_every=$verify_interval_hours
fi
iso() { date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ; }

# --- git state ------------------------------------------------------------
commit="$(git -C "$repo" rev-parse HEAD 2>/dev/null || echo unknown)"
subject="$(git -C "$repo" log -1 --format=%s 2>/dev/null || echo '')"
dirty=false
[ -n "$(git -C "$repo" status --porcelain 2>/dev/null)" ] && dirty=true

database=none
[ -n "${QUANT_DATABASE_URL:-}" ] && database=configured
preflight_state=clean
[ "$preflight" -ne 0 ] && preflight_state=forced

# --- manifest: what this run was, written before it starts ----------------
# Seven days later the question "what was actually running?" should have an answer
# that does not depend on anybody's memory. Values go through the environment, not
# string interpolation, so a commit subject with a quote in it cannot corrupt the
# JSON.
Q_STARTED="$(iso "$started_epoch")" \
Q_ENDS="$(iso "$end_epoch")" \
Q_SYMBOLS="$symbols" \
Q_ROOT="$root" \
Q_DURATION="$(( end_epoch - started_epoch ))" \
Q_REHEARSAL="$rehearsal" \
Q_COMMIT="$commit" \
Q_SUBJECT="$subject" \
Q_DIRTY="$dirty" \
Q_DATABASE="$database" \
Q_PREFLIGHT="$preflight_state" \
Q_HOST="$(scutil --get ComputerName 2>/dev/null || hostname)" \
Q_PLATFORM="$(uname -sm)" \
python3 - "$root/run.json" <<'PY'
import json, os, sys
m = {
    "started_at": os.environ["Q_STARTED"],
    "ends_at": os.environ["Q_ENDS"],
    "symbols": os.environ["Q_SYMBOLS"].split(),
    "root": os.environ["Q_ROOT"],
    "duration_seconds": int(os.environ["Q_DURATION"]),
    "rehearsal": os.environ["Q_REHEARSAL"] == "1",
    "commit": os.environ["Q_COMMIT"],
    "commit_subject": os.environ["Q_SUBJECT"],
    "dirty": os.environ["Q_DIRTY"] == "true",
    "database": os.environ["Q_DATABASE"],
    "preflight": os.environ["Q_PREFLIGHT"],
    "host": os.environ["Q_HOST"],
    "platform": os.environ["Q_PLATFORM"],
}
json.dump(m, open(sys.argv[1], "w"), indent=2)
PY

echo ""
if [ "$rehearsal" -eq 1 ]; then
    echo "starting ${minutes}-minute REHEARSAL (not an acceptance run)"
else
    echo "starting ${days}-day run"
fi
echo "  symbols  ${symbols// /, }"
echo "  root     $root"
echo "  ends     $(iso "$end_epoch")"
echo "  commit   ${commit:0:9}  $subject"
[ "$dirty" = true ] && echo "  WARNING  working tree is dirty: the binary under test is not a committed state"
echo ""

# --- launch ---------------------------------------------------------------
# nohup + & detaches each child from this terminal. Each supervisor holds its own
# caffeinate assertion for its lifetime, so the system stays awake for the run.
pids=()
launch() { # label  script  args...
    local label="$1"; shift
    local script="$1"; shift
    nohup "$here/$script" "$@" >>"$log_dir/$(basename "$script" .sh).out" 2>&1 &
    local pid=$!
    disown "$pid" 2>/dev/null || true
    printf '  started %-24s pid %s\n' "$label" "$pid"
    pids+=("$pid")
}

for symbol in "${symbol_arr[@]}"; do
    launch "recorder $symbol" supervise.sh "$symbol" "$root" "$end_epoch" "$log_dir"
done
launch "verifier" verify-loop.sh "$root" "$end_epoch" "$log_dir" "$verify_every" "$first_check_seconds"

printf '%s\n' "${pids[@]}" >"$root/run.pids"

echo ""
echo "running. This terminal can be closed."
echo "  check    ops/status.sh"
echo "  stop     ops/stop-run.sh"
echo "  logs     $log_dir"
