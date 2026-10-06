#!/usr/bin/env bash
# test-supervise.sh -- exercise supervise.sh's restart decisions against a fake child.
#
# `ops/` had no tests, which is how the coda defect survived: the restart rule is
# five lines of shell that only ever ran inside a fortnight, where getting it
# wrong costs the run it is supervising. A fake binary and a ten-second deadline
# reproduce the decision in seconds.
#
# The fake stands in for `target/release/{record,paper}` by being installed at
# that path under a throwaway repo root, because that is how supervise.sh finds
# its binary -- relative to its own location. Nothing here touches the real tree.
#
# Usage: ops/test-supervise.sh
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pass=0; fail=0

check() {
    local what="$1" want="$2" got="$3"
    if [ "$want" = "$got" ]; then
        printf '  ok   %s\n' "$what"; pass=$(( pass + 1 ))
    else
        printf '  FAIL %s -- wanted %s, got %s\n' "$what" "$want" "$got"; fail=$(( fail + 1 ))
    fi
}

# Build a throwaway tree with a fake child that behaves as told.
#   $1 seconds to run, $2 exit code
stage() {
    local dir; dir="$(mktemp -d)"
    mkdir -p "$dir/ops" "$dir/target/release" "$dir/logs"
    cp "$here/supervise.sh" "$dir/ops/supervise.sh"
    for exe in record paper; do
        cat > "$dir/target/release/$exe" <<FAKE
#!/usr/bin/env bash
sleep $1
exit $2
FAKE
        chmod +x "$dir/target/release/$exe"
    done
    echo "$dir"
}

# `grep -c` exits 1 on zero matches, so a bare `|| echo 0` prints "0" *after*
# grep has already printed "0" -- two lines, and every numeric comparison then
# fails confusingly. Count with grep's output only, defaulting when the file does
# not exist.
attempts_in() {
    local log="$1/logs/supervisor-TEST.log"
    [ -f "$log" ] || { echo 0; return; }
    grep -c "attempt .* starting" "$log" 2>/dev/null | head -1
}

# 1. The coda, and the deadline here is the whole test.
#
#    The old rule broke only within 30s of the deadline. A fixture whose clean
#    exit lands *inside* that window passes on the broken code and proves
#    nothing -- which the first version of this test did, with a 20s deadline.
#    The fortnight's actual coda had **46s** remaining, outside the window, which
#    is why the supervisor started attempt 2. So the deadline must be far enough
#    out that the clean exit falls outside 30s, or this does not reproduce the
#    defect it was written for.
#
#    M2.e's lesson, arriving in a shell script: a fixture that fixes one side of
#    the boundary under test proves much less than it appears to.
dir="$(stage 2 0)"
"$dir/ops/supervise.sh" TEST "$dir" "$(( $(date +%s) + 45 ))" "$dir/logs" record >/dev/null 2>&1
check "a clean exit with time left is not restarted" 1 "$(attempts_in "$dir")"
grep -q "UNEXPECTED: clean exit" "$dir/logs/supervisor-TEST.log" \
    && check "and is reported as unexpected" yes yes \
    || check "and is reported as unexpected" yes no
rm -rf "$dir"

# 2. A child that *dies* is still restarted -- the whole point of a supervisor.
#    Without this the fix would be "never restart", which is not a fix.
dir="$(stage 1 3)"
"$dir/ops/supervise.sh" TEST "$dir" "$(( $(date +%s) + 20 ))" "$dir/logs" record >/dev/null 2>&1
[ "$(attempts_in "$dir")" -ge 2 ] \
    && check "a crashing child is restarted" yes yes \
    || check "a crashing child is restarted" yes "no ($(attempts_in "$dir") attempts)"
rm -rf "$dir"

# 3. Paper mode refuses an attempt it cannot fit. The binary's unit is whole
#    minutes, so starting one with 40s left can only overshoot the deadline.
dir="$(stage 1 0)"
"$dir/ops/supervise.sh" TEST "$dir" "$(( $(date +%s) + 40 ))" "$dir/logs" paper >/dev/null 2>&1
check "paper does not start an attempt shorter than its own unit" 0 "$(attempts_in "$dir")"
rm -rf "$dir"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
