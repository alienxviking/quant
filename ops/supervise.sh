#!/usr/bin/env bash
# supervise.sh -- keep one symbol recording until a deadline, restarting it if it dies.
#
# macOS port of ops/supervise.ps1. "Runs seven days unattended" is a statement
# about the *system*, not about one process, and the two are deliberately
# different things.
#
# The recorder exits only on a condition it has decided is fatal -- the writer
# thread gone, a file it cannot create. That is the right behaviour: a process
# that knows it can no longer do its job should stop rather than carry on
# pretending. Restarting it is a separate concern with a separate lifetime, and it
# belongs out here.
#
# Nothing is lost across a restart, because the format was built for it. A new
# process takes a new session id, writes to its own files, and puts a
# `RecorderRestart` gap in the first frame -- so the capture states plainly that
# coverage was interrupted rather than quietly abutting two runs and looking
# continuous. The verifier reads that as the explanation for the discontinuity it
# is about to find.
#
# On Linux this whole script is `Restart=always` in a systemd unit; on macOS it
# could be a launchd job with KeepAlive. It is a plain loop instead because an
# acceptance run should be *watched*: it must stop when told to and not silently
# resurrect itself across a reboot (see docs/acceptance-run.md). The reboot limit
# is deliberate, not an oversight of this port.
#
# Usage: supervise.sh SYMBOL ROOT END_EPOCH LOGDIR [MODE] [EXTRA...]
#
#   MODE   record (default) or paper. A paper session captures raw *and* trades
#          it from the same ingress, so it replaces the recorder rather than
#          running beside one -- two processes would mean two subscriptions and
#          two sets of timestamps, and docs/engine-contract.md 9 needs them
#          identical. Everything else about supervising is the same, which is
#          why this is a parameter and not a second script.
#   EXTRA  passed through to the paper binary (--qty, --cash, --max-* and so on).
set -uo pipefail

symbol="${1:?symbol required}"
root="${2:?root required}"
end_epoch="${3:?end epoch (unix seconds) required}"
log_dir="${4:?logdir required}"
mode="${5:-record}"
if [ $# -gt 5 ]; then shift 5; extra=("$@"); else extra=(); fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
case "$mode" in
    record) exe="$repo/target/release/record";;
    paper)  exe="$repo/target/release/paper";;
    *) echo "unknown mode: $mode (want record or paper)" >&2; exit 2;;
esac

# The recorder logs through `tracing`, which emits ANSI colour even when its output
# is a file rather than a terminal. Over a week that is escape codes in every log
# line; NO_COLOR turns them off at the source so the per-attempt logs stay greppable.
export NO_COLOR=1

if [ ! -x "$exe" ]; then
    echo "no release binary at $exe -- run ops/preflight.sh first" >&2
    exit 1
fi
mkdir -p "$log_dir"

# Hold the system awake for as long as this supervisor lives, across every
# recorder restart. `-w $$` ties the assertion to this process, so it is released
# the instant the supervisor exits -- no assertion leaks past the run. Launched as
# a sibling (not wrapping the recorder) so the recorder stays a direct child we
# can deliver a clean SIGINT to. Lid open on mains is still required; caffeinate
# cannot beat a closed lid without `pmset disablesleep`.
caffeinate -dimsu -w $$ &

# Backoff between attempts. A recorder that dies instantly and repeatedly is
# misconfigured, and hammering it would bury the reason in a scrolling log while
# hitting the venue's connection limits on the way. It resets after any attempt
# that survived a while, so an unlucky night does not leave a healthy symbol
# waiting minutes to come back.
backoff=2
max_backoff=120
healthy_run=300

attempt=0
journal="$log_dir/supervisor-$symbol.log"

now_utc() { date -u +%Y-%m-%dT%H:%M:%SZ; }
note() {
    local line
    line="$(now_utc) $1"
    echo "$line"
    echo "$line" >>"$journal"
}

note "supervising $symbol in $mode mode until $(date -u -r "$end_epoch" +%Y-%m-%dT%H:%M:%SZ) into $root"

while [ "$(date +%s)" -lt "$end_epoch" ]; do
    remaining=$(( end_epoch - $(date +%s) ))
    [ "$remaining" -le 5 ] && break

    attempt=$(( attempt + 1 ))
    stamp="$(date -u +%Y%m%d-%H%M%S)"
    out="$log_dir/$symbol-$stamp.log"
    note "attempt $attempt starting, ${remaining}s remaining, log $(basename "$out")"

    started_at="$(date +%s)"
    # The recorder runs in the foreground of this loop; its stdout+stderr go to a
    # per-attempt file. Separate files per attempt, because a restart must not
    # erase the log that explains why the previous attempt ended. It handles its
    # own SIGINT (clean shutdown, sealing the trailer); we deliver that only from
    # stop-run.sh.
    if [ "$mode" = paper ]; then
        # The paper binary takes whole minutes, so the remaining seconds are
        # rounded *up*: rounding down would leave the last partial minute
        # unsupervised, and a restart loop with nothing left to do spins.
        minutes=$(( (remaining + 59) / 60 ))
        "$exe" --symbol "$symbol" "$root" --minutes "$minutes" "${extra[@]}" >"$out" 2>&1
    else
        "$exe" "$symbol" "$root" "$remaining" >"$out" 2>&1
    fi
    code=$?
    ran_for=$(( $(date +%s) - started_at ))

    if [ "$code" -eq 0 ]; then
        note "attempt $attempt exited cleanly after ${ran_for}s"
        # A clean exit within 30s of the deadline means the duration limit was
        # reached -- the run finishing, not failing. An earlier clean exit falls
        # through and respawns, exactly as the Windows supervisor does: the only
        # deliberate early clean exit is a SIGINT from stop-run.sh, and that path
        # kills this supervisor *first*, so a clean exit reaching this branch is
        # one we should recover from, not stop on.
        [ "$(date +%s)" -ge $(( end_epoch - 30 )) ] && break
    else
        note "attempt $attempt exited $code after ${ran_for}s -- see $(basename "$out")"
    fi

    [ "$ran_for" -ge "$healthy_run" ] && backoff=2

    remaining=$(( end_epoch - $(date +%s) ))
    [ "$remaining" -le 5 ] && break

    sleep_for=$(( backoff < remaining ? backoff : remaining ))
    note "restarting in ${sleep_for}s"
    sleep "$sleep_for"
    backoff=$(( backoff * 2 )); [ "$backoff" -gt "$max_backoff" ] && backoff=$max_backoff
done

note "supervisor for $symbol finished after $attempt attempt(s)"
