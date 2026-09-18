#!/usr/bin/env bash
# status.sh -- how is the run going?
#
# macOS port of ops/status.ps1. A miniature of what M7 is meant to deliver: one
# command that answers "what is it doing right now" without anybody reading raw
# logs. If this cannot be answered in a few seconds, a seven-day run is not really
# being watched -- nor is M5's fortnight.
#
# Everything shown here comes from something the recorder, the engine, the verifier
# or reconcile already emits. Nothing is computed a second way, because a status
# view that derives its own numbers eventually disagrees with the thing it is
# reporting on. That rule is why the journal section below reads back reconcile's
# recorded verdict instead of doing its own arithmetic on the journal.
#
# Which *kind* of run this is comes from run.json's `mode`, written by start-run.sh
# before anything launches. Matching both binary names instead would be inferring
# something the manifest already states, and would then need a second, independent
# rule to label what it found -- two sources that can disagree. One source, taken
# once: the same reason M4's backtest report reads its costs back off the venue
# rather than off the flags it was handed.
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
# record | paper. A run.json without this field predates it, and the field landed in
# the same commit as --paper itself, so its absence *proves* a recorder run rather
# than merely being assumed to mean one.
#
# Assigned to a name first and interpolated as a bare {mode}, rather than calling
# .get() inside the f-string. bash 3.2 does not understand a here-document inside
# $( ): it scans this body as ordinary words, and a brace wrapping a comma inside
# literal double quotes is a brace-expansion candidate to it. It then runs python
# once per alternative -- two half-programs -- and M_MODE comes back as the tail of
# the expression rather than the value. Verified on macOS bash 3.2.57; a bare {mode}
# offers it nothing to expand. Keep any field added here in this two-line shape.
mode = m.get("mode", "record")
print(f'M_MODE="{mode}"')
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
    printf '%-6s %-9s %s  commit %s  preflight %s\n' "$M_MODE" "$M_KIND" "$M_SYMBOLS" "$M_COMMIT" "$M_PREFLIGHT"
    if [ "$left" -le 0 ]; then
        printf 'elapsed  %s of %s   FINISHED\n' "$(fmt_dur "$elapsed")" "$(fmt_dur "$M_DURATION")"
    else
        printf 'elapsed  %s of %s   remaining %s\n' "$(fmt_dur "$elapsed")" "$(fmt_dur "$M_DURATION")" "$(fmt_dur "$left")"
    fi
else
    echo "run      (no run.json -- started by hand?)"
fi

# --- processes ------------------------------------------------------------
# Which binary to look for, and what to call it, both come from run.json's `mode`.
# A paper session runs target/release/paper, so the fixed 'record' pattern matched
# nothing and printed "no recorder running" -- observed against the M5 rehearsal, and
# it would have been the most alarming line in this output, false at every invocation
# for the whole fortnight, which is how an operator learns to stop reading it.
# Pattern and label from one field means they cannot drift apart.
#
# Scoped to this run's root as well, because pgrep is machine-wide and --root is
# not. A rehearsal at ~/paper-rehearsal beside the fortnight at ~/paper is the
# expected M5 arrangement, and an unscoped match reports one run's process under the
# other's heading. supervise.sh passes the canonicalised root on the command line in
# both modes and this script canonicalises --root the same way, so the strings are
# identical. The pattern is quoted inside the case, so a root containing a glob
# metacharacter is compared literally rather than silently ceasing to match.
#
# No manifest means nobody canonicalised anything and the mode is genuinely unknown,
# so that path -- and only that path -- matches either binary, labels each from its
# own argv, and does not scope by root. Less precise, and honest about it.
echo ""
echo "processes"
scoped=1
case "${M_MODE:-}" in
    paper)  want=paper;  pattern='target/release/paper';;
    record) want=record; pattern='target/release/record';;
    *)      want="";     pattern='target/release/(record|paper)'; scoped=0;;
esac
shown=0
elsewhere=0
for pid in $(pgrep -f "$pattern" 2>/dev/null || true); do
    # -ww so a long argv is never truncated to the terminal width; the root filter
    # below reads the tail of the command line and a truncated one would look like a
    # different run.
    cmd="$(ps -ww -o command= -p "$pid" 2>/dev/null)"
    # Gone between the pgrep and the ps. Not ours to report either way.
    [ -n "$cmd" ] || continue
    if [ "$scoped" -eq 1 ]; then
        case "$cmd" in *"$root"*) ;; *) elsewhere=$(( elsewhere + 1 )); continue;; esac
    fi
    label="$want"
    if [ -z "$label" ]; then
        case "$cmd" in *target/release/paper*) label=paper;; *) label=record;; esac
    fi
    # etime = elapsed since start; rss in KB.
    info="$(ps -o etime=,rss= -p "$pid" 2>/dev/null)"
    et="$(echo "$info" | awk '{print $1}')"
    rss_mb="$(echo "$info" | awk '{printf "%d", $2/1024}')"
    printf '  %-6s pid %-7s up %s  %s MB\n' "$label" "$pid" "$et" "$rss_mb"
    shown=$(( shown + 1 ))
done
if [ "$shown" -eq 0 ]; then
    # What the root filter hid is reported rather than swallowed. Scoping can only
    # go wrong one way -- a root spelled differently from the one on the command
    # line -- and swallowing it would print the same confident "nothing running"
    # this section was fixed to stop printing. A count that does not add up is a
    # question; silence is a wrong answer.
    if [ "$elsewhere" -gt 0 ]; then
        printf '  no %s process under this root (%s running elsewhere)\n' \
            "${want:-record or paper}" "$elsewhere"
    else
        printf '  no %s process running\n' "${want:-record or paper}"
    fi
fi

# --- what each symbol last said ------------------------------------------
# The metrics line, straight from the capture. Queue depth is the one to read
# first: without it a stalled writer and a quiet market look identical.
#
# Deliberately *not* mode-aware, and that is worth writing down rather than leaving
# to look like luck. M5.d moved record()'s body into quant-binance::capture and
# parameterised only the sink, so a paper session emits the identical
# `capture: metrics` line under the identical $symbol-$stamp.log name. Confirmed
# against a paper rehearsal's log. If this section ever needs a mode test, the tee
# has stopped being one ingress -- which is the property the fortnight is measuring.
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
# Two different questions share verify.log -- is the capture complete, and does the
# journal still agree -- and splitting them is not cosmetic. On a failing check
# verify-loop.sh appends every finding as its own note line, so a four-line tail of
# the mixed file can contain no reconcile verdict at all: the signal a paper run
# exists to produce vanishes exactly when something is already wrong. The reconcile
# notes are the ones naming a journal, so '.jsonl:' separates them cleanly.
echo ""
echo "verification"
if [ -f "$log_dir/verify.log" ]; then
    grep -v '\.jsonl:' "$log_dir/verify.log" | tail -4 | sed 's/^/  /'
else
    echo "  not run yet"
fi

# --- journal (paper runs) -------------------------------------------------
# M5's criterion is that the live path and a replay of the same data agree, and
# `reconcile` is what says so. verify-loop.sh already runs it every cycle and
# records the *exit code* -- which is the authority, not the prose (M1.d2: an exit
# code so it can run unattended, and M5.c: "nobody disagreed" is not "two answers
# matched", which is why exit 2 is its own outcome). So this reads those notes back
# rather than re-running reconcile or re-parsing its report: a status view that
# reached its own verdict could disagree with the one the loop acted on.
#
# No mode test here. A recorder run has no journals under its root, the glob finds
# none, and the section is simply absent -- the same way verify-loop.sh finds none
# to reconcile. Plain glob-into-array, not mapfile: macOS ships bash 3.2.
shopt -s nullglob
journals=("$root"/paper-*.jsonl)
if [ ${#journals[@]} -gt 0 ]; then
    echo ""
    echo "journal"
    for j in "${journals[@]}"; do
        name="$(basename "$j")"
        last="$(grep -F "$name:" "$log_dir/verify.log" 2>/dev/null | tail -1)"
        if [ -n "$last" ]; then
            echo "  $last"
        else
            # Before the first cycle, or before the first fill. Not a problem, and
            # saying so beats a silent gap that reads like a missing journal.
            echo "  $name: no reconcile yet"
        fi
    done
fi
shopt -u nullglob

# --- on disk --------------------------------------------------------------
# Unchanged for paper, deliberately: M5.a makes the capture primary -- the market
# data is the irreplaceable half of a paper session and goes to the same raw tier
# through the same writer -- so "capture" is still the right word and the same find
# still counts it.
echo ""
files="$(find "$root/raw" -name 'part-*.bin.zst' 2>/dev/null | wc -l | tr -d ' ')"
mb="$(find "$root/raw" -name 'part-*.bin.zst' 2>/dev/null -exec stat -f '%z' {} + 2>/dev/null | awk '{s+=$1} END{printf "%.1f", s/1024/1024}')"
sessions="$(find "$root/raw" -type d -name 'session=*' 2>/dev/null | wc -l | tr -d ' ')"
printf 'capture  %s files in %s session(s), %s MB\n' "$files" "$sessions" "${mb:-0}"
free_gb="$(df -g "$root" | awk 'NR==2 {print $4}')"
printf 'disk     %s GB free\n' "$free_gb"
