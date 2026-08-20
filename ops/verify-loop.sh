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
# Usage: verify-loop.sh ROOT END_EPOCH LOGDIR [INTERVAL_HOURS] [FIRST_CHECK_SECONDS]
set -uo pipefail

root="${1:?root required}"
end_epoch="${2:?end epoch (unix seconds) required}"
log_dir="${3:?logdir required}"
interval_hours="${4:-6}"
# How long to wait before the first check. A misconfiguration shows up in the
# first few minutes, and that is the failure most worth catching early.
first_check_seconds="${5:-300}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
exe="$repo/target/release/verify"
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
note "verifying $root every ${interval_hours}h until $(date -u -r "$end_epoch" +%Y-%m-%dT%H:%M:%SZ), reconcile=$use_reconcile"

# A first pass shortly after the start, because the failure worth catching early
# is a misconfiguration, and that shows up in the first few minutes.
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

    remaining=$(( end_epoch - $(date +%s) ))
    [ "$remaining" -le 60 ] && break
    step=$(( interval_hours * 3600 ))
    [ "$step" -gt "$remaining" ] && step=$remaining
    sleep "$step"
done

note "verification loop finished"
