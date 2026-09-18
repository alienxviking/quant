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
# A paper session has a second artifact with exactly that property, and more to
# lose. Besides the segment's trailer it owes its journal a final checkpoint and a
# `Stopped` mark, and both are written only after a clean SIGINT has unwound the
# capture. Without the checkpoint `reconcile` exits 2 on a perfectly good
# fortnight -- "nobody disagreed", which M5.c is emphatic is not the same as "two
# answers matched" -- and without the `Stopped` the journal cannot say whether the
# last session ended or died. So a paper run is stopped the same way, which means
# this script has to be able to find one.
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

# --- what is this run made of? --------------------------------------------
# `record` and `paper` are different binaries, and until M5 taught start-run.sh a
# mode this script only ever looked for `record`. Against a paper run that matches
# nothing, so it prints "no recorder running", exits 0, and leaves the session
# orphaned -- still capturing and still trading, with its supervisor and that
# supervisor's caffeinate assertion already killed -- for as much of the fortnight
# as remained. Silent, and reported as success, which is the worst shape a stop
# can fail in.
#
# The mode is read rather than guessed: start-run.sh writes it into run.json for
# precisely this class of question, so that seven days later nothing depends on
# anybody's memory. python3 is already what start-run.sh writes that file with and
# what status.sh reads it with, so this adds no dependency the harness lacks; an
# unreadable or older manifest simply leaves the mode empty and falls through.
# With no manifest -- a run started by hand -- both binaries are matched, which is
# the safe direction: a SIGINT to a binary that is not running costs nothing,
# while missing one costs the run its clean ending. (Like the `record` match it
# replaces, this is machine-wide rather than scoped to --root: with one run on the
# box, which is what the harness supports, that is what finds both symbols.) macOS
# pgrep patterns are extended regular expressions, so the alternation is a pattern
# and not a literal.
mode="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("mode",""))' "$root/run.json" 2>/dev/null)"
case "$mode" in
    record) what="recorder";               pattern='target/release/record';;
    paper)  what="paper session";          pattern='target/release/paper';;
    *)      what="recorder/paper session"; pattern='target/release/(record|paper)';;
esac

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

# --- the run's own processes: ask them to finish --------------------------
# Plain word-splitting, not `mapfile`: macOS ships bash 3.2, where `mapfile` does
# not exist. pgrep prints one pid per line and pids never contain spaces.
running="$(pgrep -f "$pattern" 2>/dev/null || true)"
if [ -z "$running" ]; then
    echo "no $what running"
    exit 0
fi

for pid in $running; do
    echo "asking $what pid $pid to finish (SIGINT)"
    # SIGINT, not SIGTERM/SIGKILL: both binaries take SIGINT as their clean
    # shutdown (tokio::signal::ctrl_c), which seals the last segment's trailer --
    # and, in a paper run, is what lets the process go on to write its closing
    # checkpoint and `Stopped` mark once the capture has unwound. A killed process
    # leaves that segment trailerless -- readable, but permanently
    # indistinguishable from a crash -- and its journal claiming nothing.
    kill -INT "$pid" 2>/dev/null || true
done

deadline=$(( $(date +%s) + grace ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ -z "$(pgrep -f "$pattern" 2>/dev/null || true)" ]; then
        echo "all $what(s) finished cleanly"
        exit 0
    fi
    sleep 0.5
done

echo ""
left="$(pgrep -f "$pattern" 2>/dev/null || true)"
n="$(echo "$left" | grep -c . || true)"
echo "$n $what(s) did not finish within ${grace}s."
echo "Killing them now would leave their last segment without a trailer -- readable,"
echo "but permanently indistinguishable from a crash -- and, in a paper run, a"
echo "journal with no closing checkpoint for reconcile to check against. To do it"
echo "anyway:"
for pid in $left; do echo "  kill -INT $pid   # try SIGINT again first"; done
exit 1
