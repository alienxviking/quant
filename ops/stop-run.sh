#!/usr/bin/env bash
# stop-run.sh -- stop the acceptance run cleanly.
#
# macOS port of ops/stop-run.ps1. Cleanly matters here, and it is the whole reason
# the recorder distinguishes the two shutdowns. A SIGINT makes the connection
# future drop, which closes the capture channel, which makes the writer seal the
# file with a trailer. A file with a trailer says "I am complete and I hold exactly
# this much"; a file without one says "my recorder died". Killing the process
# outright (SIGKILL, or even SIGTERM which the recorder does not catch) throws that
# distinction away and leaves every segment looking like a crash.
#
# So the supervisors are stopped first -- otherwise they would helpfully restart
# the recorder we are trying to stop -- and then each recorder is asked to finish
# with SIGINT rather than told to stop.
#
# Usage: stop-run.sh [--root DIR] [--grace SECONDS]
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
root=""
grace=30
while [ $# -gt 0 ]; do
    case "$1" in
        --root)  root="$2"; shift 2;;
        --grace) grace="$2"; shift 2;;
        *) echo "unknown argument: $1" >&2; exit 2;;
    esac
done
[ -n "$root" ] || root="$repo/data/acceptance"
[ -d "$root" ] || { echo "no run at $root"; exit 1; }
root="$(cd "$root" && pwd)"

# --- supervisors first ----------------------------------------------------
# Stopping a recorder while its supervisor is watching would simply produce
# another recorder. SIGTERM is right for the supervisors: they are shell loops, not
# the recorder, and killing one only stops the restart loop -- the recorder it
# launched keeps running as an orphan until we SIGINT it below.
pid_file="$root/run.pids"
if [ -f "$pid_file" ]; then
    while IFS= read -r id; do
        [[ "$id" =~ ^[0-9]+$ ]] || continue
        if kill -0 "$id" 2>/dev/null; then
            echo "stopping supervisor pid $id"
            kill -TERM "$id" 2>/dev/null || true
        fi
    done <"$pid_file"
else
    echo "no run.pids at $root; stopping supervisors by hand may be needed"
fi

# Give the supervisors a moment to die so they cannot race a restart in between.
sleep 1

# --- recorders: ask them to finish ---------------------------------------
# Plain word-splitting, not `mapfile`: macOS ships bash 3.2, where `mapfile` does
# not exist. pgrep prints one pid per line and pids never contain spaces.
recorders="$(pgrep -f 'target/release/record' 2>/dev/null || true)"
if [ -z "$recorders" ]; then
    echo "no recorder running"
    exit 0
fi

for pid in $recorders; do
    echo "asking record pid $pid to finish (SIGINT)"
    # SIGINT, not SIGTERM/SIGKILL: the recorder handles SIGINT as its clean
    # shutdown (tokio::signal::ctrl_c), which seals the last segment's trailer. A
    # killed recorder leaves that segment trailerless -- readable, but permanently
    # indistinguishable from a crash.
    kill -INT "$pid" 2>/dev/null || true
done

deadline=$(( $(date +%s) + grace ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ -z "$(pgrep -f 'target/release/record' 2>/dev/null || true)" ]; then
        echo "all recorders finished cleanly"
        exit 0
    fi
    sleep 0.5
done

echo ""
left="$(pgrep -f 'target/release/record' 2>/dev/null || true)"
n="$(echo "$left" | grep -c . || true)"
echo "$n recorder(s) did not finish within ${grace}s."
echo "Killing them now would leave their last segment without a trailer -- readable,"
echo "but permanently indistinguishable from a crash. To do it anyway:"
for pid in $left; do echo "  kill -INT $pid   # try SIGINT again first"; done
exit 1
