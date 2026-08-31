#!/usr/bin/env bash
# status.sh -- how is the acceptance run going?
#
# macOS port of ops/status.ps1. A miniature of what M7 is meant to deliver: one
# command that answers "what is it doing right now" without anybody reading raw
# logs. If this cannot be answered in a few seconds, a seven-day run is not really
# being watched.
#
# Everything shown here comes from something the recorder or verifier already
# emits. Nothing is computed a second way, because a status view that derives its
# own numbers eventually disagrees with the thing it is reporting on.
#
# Usage: status.sh [--root DIR]
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/.." && pwd)"
root=""
[ "${1:-}" = "--root" ] && { root="$2"; shift 2; }
[ -n "$root" ] || root="$repo/data/acceptance"
if [ ! -d "$root" ]; then echo "no run at $root"; exit 1; fi
root="$(cd "$root" && pwd)"
log_dir="$root/logs"

fmt_dur() { # seconds -> Dd HH:MM:SS
    local s=$1
    printf '%dd %02d:%02d:%02d' $((s/86400)) $(((s%86400)/3600)) $(((s%3600)/60)) $((s%60))
}

# --- run manifest ---------------------------------------------------------
manifest="$root/run.json"
if [ -f "$manifest" ]; then
    eval "$(python3 - "$manifest" <<'PY'
import json, sys, calendar, time
m = json.load(open(sys.argv[1]))
def epoch(s):
    return calendar.timegm(time.strptime(s, "%Y-%m-%dT%H:%M:%SZ"))
print(f'M_KIND={"REHEARSAL" if m.get("rehearsal") else "run"}')
print(f'M_SYMBOLS="{", ".join(m["symbols"])}"')
print(f'M_COMMIT={m["commit"][:9]}')
print(f'M_PREFLIGHT={m.get("preflight","?")}')
print(f'M_STARTED={epoch(m["started_at"])}')
print(f'M_ENDS={epoch(m["ends_at"])}')
print(f'M_DURATION={m.get("duration_seconds",0)}')
PY
)"
    now=$(date +%s)
    elapsed=$(( now - M_STARTED ))
    left=$(( M_ENDS - now ))
    printf '%-9s %s  commit %s  preflight %s\n' "$M_KIND" "$M_SYMBOLS" "$M_COMMIT" "$M_PREFLIGHT"
    if [ "$left" -le 0 ]; then
        printf 'elapsed  %s of %s   FINISHED\n' "$(fmt_dur "$elapsed")" "$(fmt_dur "$M_DURATION")"
    else
        printf 'elapsed  %s of %s   remaining %s\n' "$(fmt_dur "$elapsed")" "$(fmt_dur "$M_DURATION")" "$(fmt_dur "$left")"
    fi
else
    echo "run      (no run.json -- started by hand?)"
fi

# --- processes ------------------------------------------------------------
echo ""
echo "processes"
recs="$(pgrep -f 'target/release/record' 2>/dev/null || true)"
if [ -z "$recs" ]; then
    echo "  no recorder running"
else
    for pid in $recs; do
        # etime = elapsed since start; rss in KB.
        info="$(ps -o etime=,rss= -p "$pid" 2>/dev/null)"
        et="$(echo "$info" | awk '{print $1}')"
        rss_mb="$(echo "$info" | awk '{printf "%d", $2/1024}')"
        printf '  record pid %-7s up %s  %s MB\n' "$pid" "$et" "$rss_mb"
    done
fi

# --- what each symbol last said ------------------------------------------
# The metrics line, straight from the recorder. Queue depth is the one to read
# first: without it a stalled writer and a quiet market look identical.
echo ""
echo "latest metrics"
found=0
for f in "$log_dir"/*.log; do
    [ -e "$f" ] || continue
    base="$(basename "$f")"
    case "$base" in supervisor-*|verify*) continue;; esac
    found=1
done
if [ "$found" -eq 0 ]; then
    echo "  none yet"
else
    # Group by symbol (leading token of the filename), newest file per symbol.
    for symbol in $(ls "$log_dir" 2>/dev/null | grep -vE '^(supervisor-|verify)' | grep '\.log$' | sed -E 's/-[0-9]{8}-[0-9]{6}\.log$//' | sort -u); do
        newest="$(ls -t "$log_dir/$symbol"-*.log 2>/dev/null | head -1)"
        [ -n "$newest" ] || continue
        line="$(grep 'metrics ' "$newest" 2>/dev/null | tail -1)"
        [ -n "$line" ] || line="$(tail -1 "$newest" 2>/dev/null)"
        # Strip the tracing ANSI colour codes -- the recorder writes them even to a
        # file -- and the leading whitespace, so the metrics read cleanly here.
        line="$(printf '%s' "$line" | perl -pe 's/\e\[[0-9;]*m//g' | sed -E 's/^[[:space:]]+//')"
        echo "  $symbol"
        echo "    $line"
    done
fi

# --- restarts -------------------------------------------------------------
# A restart is not a failure -- the format records one as a gap and the verifier
# reads it -- but a rising count is the signal that something is unhealthy.
echo ""
echo "restarts"
shopt -s nullglob
sup_logs=("$log_dir"/supervisor-*.log)
if [ ${#sup_logs[@]} -eq 0 ]; then
    echo "  no supervisor logs"
else
    for log in "${sup_logs[@]}"; do
        symbol="$(basename "$log" .log | sed 's/^supervisor-//')"
        # `grep -c` already prints 0 on no match; the `|| echo 0` that would seem to
        # guard it actually appends a *second* line, because grep exits 1 when the
        # count is zero -- which then splits the printf across two lines.
        attempts="$(grep -cE 'attempt [0-9]+ starting' "$log" 2>/dev/null)"; attempts="${attempts:-0}"
        failures="$(grep -cE 'exited [1-9]' "$log" 2>/dev/null)"; failures="${failures:-0}"
        printf '  %-10s %s attempt(s), %s non-zero exit(s)\n' "$symbol" "$attempts" "$failures"
    done
fi
shopt -u nullglob

# --- verification ---------------------------------------------------------
echo ""
echo "verification"
if [ -f "$log_dir/verify.log" ]; then
    tail -4 "$log_dir/verify.log" | sed 's/^/  /'
else
    echo "  not run yet"
fi

# --- on disk --------------------------------------------------------------
echo ""
files="$(find "$root/raw" -name 'part-*.bin.zst' 2>/dev/null | wc -l | tr -d ' ')"
mb="$(find "$root/raw" -name 'part-*.bin.zst' 2>/dev/null -exec stat -f '%z' {} + 2>/dev/null | awk '{s+=$1} END{printf "%.1f", s/1024/1024}')"
sessions="$(find "$root/raw" -type d -name 'session=*' 2>/dev/null | wc -l | tr -d ' ')"
printf 'capture  %s files in %s session(s), %s MB\n' "$files" "$sessions" "${mb:-0}"
free_gb="$(df -g "$root" | awk 'NR==2 {print $4}')"
printf 'disk     %s GB free\n' "$free_gb"
