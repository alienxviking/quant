#!/usr/bin/env bash
# verify-loop.sh -- verify the capture periodically while it is still being written.
#
# macOS port of ops/verify-loop.ps1. The whole reason `quant-verify` reports
# through an exit code rather than prose is so it can run unattended, during the
# capture rather than after it. Finding on day seven that the recorder stopped
# anchoring its book on day two means six wasted days; finding it six hours in
# means fixing it and starting again.
#
# A torn tail and a missing trailer on the newest segment are expected here --
# those files are still open -- which is exactly why the verifier reports them as
# warnings and only errors fail the run. If that distinction were not there, this
# loop would cry wolf every six hours and be ignored by day two.
#
# Verifier exit codes, preserved in the log:
#   0  every discontinuity is explained
#   1  something is not
#   2  --reconcile could not run (a database problem, not a capture problem)
#
# Usage: verify-loop.sh ROOT END_EPOCH LOGDIR [INTERVAL] [FIRST_CHECK_SECONDS]
#
#   INTERVAL  How often to check: a whole number with an optional unit -- `6` or
#             `6h` hours, `90m` minutes, `60s` seconds. A bare number still means
#             hours, so the production call site stays the readable `6` and the
#             .ps1 half's `-IntervalHours 6` keeps meaning exactly what it means
#             today.
#
#             The suffix exists because an hour is the wrong floor for the one
#             case that needs a tighter one. A ten-minute rehearsal on an hourly
#             cadence gets exactly *one* pass, 45 s in -- before a fill can
#             exist -- so the journal branch below only ever reports "nothing to
#             check yet", and a harness proving less than it claims is worse than
#             one that claims less. Hours were not made fractional instead: bash
#             has no float arithmetic, and a cadence of `0.0166` would be
#             unreadable at both call sites.
#
#             Seconds and minutes are a rehearsal instrument, not a production
#             one. Every pass re-reads the whole capture, so the cost of a check
#             grows with the run; on a fortnight, six hours is still the answer.
set -uo pipefail

root="${1:?root required}"
end_epoch="${2:?end epoch (unix seconds) required}"
log_dir="${3:?logdir required}"
# Parsed to seconds once, here, so the sleep arithmetic below has one unit and no
# conversion left to get wrong. A malformed value is fatal rather than defaulted:
# a typo in a cadence would otherwise quietly become "never checked again", which
# is the single failure this loop exists to prevent. Invariant 5 -- parse failures
# are loud -- applied to the harness.
interval_seconds_from() {
    local spec="$1" number unit
    number="${spec%[hms]}"
    unit="${spec#"$number"}"
    case "$number" in
        ''|*[!0-9]*) return 1;;
    esac
    [ "$number" -gt 0 ] || return 1
    case "$unit" in
        s) echo "$number";;
        m) echo $(( number * 60 ));;
        h|'') echo $(( number * 3600 ));;
        *) return 1;;
    esac
}
interval_spec="${4:-6h}"
interval_seconds="$(interval_seconds_from "$interval_spec")" || {
    echo "bad interval '$interval_spec' -- want 6, 6h, 90m or 60s" >&2
    exit 2
}
# A bare number is hours; say so in the log rather than printing a cadence in
# units nobody chose. The log line is the only place the cadence is ever stated,
# and it is what made this defect visible at all -- a rehearsal log that read
# "every 1h" was telling the truth about a run that lasted ten minutes.
case "$interval_spec" in
    *[hms]) ;;
    *) interval_spec="${interval_spec}h";;
esac
# How long to wait before the first check. A misconfiguration shows up in the
# first few minutes, and that is the failure most worth catching early.
first_check_seconds="${5:-300}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
exe="$repo/target/release/verify"
# Present only for a paper run; a bare recorder run finds no journals to feed it.
reconcile="$repo/target/release/reconcile"
log="$log_dir/verify.log"

if [ ! -x "$exe" ]; then
    echo "no release binary at $exe -- run ops/preflight.sh first" >&2
    exit 1
fi
mkdir -p "$log_dir"

note() {
    local line
    line="$(date -u +%Y-%m-%dT%H:%M:%SZ) $1"
    echo "$line"
    echo "$line" >>"$log"
}

# Only reconcile if a database was configured. Without one the flag exits 2 every
# time, which would train whoever reads this log to ignore a non-zero code -- the
# same reason warnings do not fail a verification run.
use_reconcile=0
[ -n "${QUANT_DATABASE_URL:-}" ] && use_reconcile=1
note "verifying $root every $interval_spec until $(date -u -r "$end_epoch" +%Y-%m-%dT%H:%M:%SZ), reconcile=$use_reconcile"

# A first pass shortly after the start, because the failure worth catching early
# is a misconfiguration, and that shows up in the first few minutes.
#
# Never wait past the end of the run, though. A first check longer than the whole
# window would have the loop wake after the deadline and check nothing at all --
# leaving a log that says only that verification started and finished, which
# reads exactly like a clean run. Halving what is left keeps one pass, and keeps
# it late enough that there is something on disk to look at.
window=$(( end_epoch - $(date +%s) ))
if [ "$window" -gt 1 ] && [ "$first_check_seconds" -ge "$window" ]; then
    first_check_seconds=$(( window / 2 ))
    note "first check pulled in to ${first_check_seconds}s: the run is shorter than the configured wait"
fi
sleep "$first_check_seconds"

while [ "$(date +%s)" -lt "$end_epoch" ]; do
    stamp="$(date -u +%Y%m%d-%H%M%S)"
    out="$log_dir/verify-$stamp.txt"

    if [ "$use_reconcile" -eq 1 ]; then
        "$exe" "$root" --reconcile >"$out" 2>&1
    else
        "$exe" "$root" >"$out" 2>&1
    fi
    code=$?

    verdict="$(grep '^verdict' "$out" 2>/dev/null | tr '\n' ' ')"
    case "$code" in
        0) note "OK   $verdict";;
        2) note "SKIP reconcile could not run: $(grep -iE 'reconcile|database|postgres' "$out" 2>/dev/null | head -1)";;
        *)
            note "FAIL exit $code  $verdict"
            # Copied into the running log so the findings are in one place rather
            # than in a file somebody has to go and find at 3am.
            grep -E '^  (ERROR|warn|\.\.\.)' "$out" 2>/dev/null | while IFS= read -r l; do note "     $l"; done
            ;;
    esac

    # Any paper journals under this root get reconciled too, on the same
    # cadence and for the same reason: a live path and a replay path that have
    # started disagreeing should cost hours, not a fortnight. Absent journals
    # mean this is a bare recorder run, and the loop simply finds none.
    for journal in "$root"/paper-*.jsonl; do
        [ -e "$journal" ] || continue
        rout="$log_dir/reconcile-$(basename "$journal" .jsonl)-$stamp.txt"
        "$reconcile" "$journal" >"$rout" 2>&1
        rcode=$?
        rverdict="$(grep '^verdict' "$rout" 2>/dev/null | tr '
' ' ')"
        case "$rcode" in
            0) note "OK   $(basename "$journal"): $rverdict";;
            # Exit 2 is "no checkpoint yet", which is normal before the first
            # fill and is not a disagreement. Worth a line, not an alarm.
            2) note "WAIT $(basename "$journal"): nothing to check yet";;
            *) note "FAIL $(basename "$journal"): $rverdict";;
        esac
    done

    remaining=$(( end_epoch - $(date +%s) ))
    # Stop when the next pass would land on top of the deadline; the post-run
    # check covers the tail. The floor is the interval itself when that is
    # shorter than a minute, because a fixed 60 s would silently eat the last
    # pass of a rehearsal -- the pass most likely to have a fill behind it, and
    # therefore the one the rehearsal is being run for.
    tail_seconds=60
    [ "$interval_seconds" -lt "$tail_seconds" ] && tail_seconds=$interval_seconds
    [ "$remaining" -le "$tail_seconds" ] && break
    # Clamped to what is left, which is what makes the loop end with the run
    # rather than one interval after it. `step` is at least 1 s either way, so
    # every iteration moves towards `end_epoch` and the loop cannot spin.
    step=$interval_seconds
    [ "$step" -gt "$remaining" ] && step=$remaining
    sleep "$step"
done

note "verification loop finished"
