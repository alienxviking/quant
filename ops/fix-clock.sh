#!/usr/bin/env bash
# fix-clock.sh -- turn on and force-sync macOS network time. Must be run with sudo.
#
# macOS port of ops/fix-clock.ps1. Venue latency is local_recv_ts - exchange_ts,
# so it measures the difference between two clocks as much as it measures a
# network. On the Windows machine this project started on, the time service was
# stopped and the host was about two seconds ahead of Binance -- which the recorder
# faithfully reported as two seconds of venue latency on a link that was fine.
#
# A wrong clock does not corrupt a capture: every timestamp is shifted by the same
# amount, so ordering and dispatch -- the only things the engine acts on -- are
# unaffected. What it ruins is every latency figure, any comparison against a
# second venue later, and the ability to line a capture up against anybody else's
# records. Over seven days it also drifts, which is harder to correct for
# afterwards than a constant offset.
#
# Turning sync on matters as much as stepping the clock once: a clock that is right
# today and unsynchronised for a week will not be right on day seven. macOS keeps
# it disciplined through `timed` once network time is enabled.
set -uo pipefail

server="${1:-time.apple.com}"

if [ "$(id -u)" -ne 0 ]; then
    echo "This needs root. Run it with sudo:"
    echo ""
    echo "    sudo ops/fix-clock.sh"
    echo ""
    exit 1
fi

echo "enabling network time (server: $server)"
systemsetup -setnetworktimeserver "$server" >/dev/null 2>&1 || true
systemsetup -setusingnetworktime on >/dev/null 2>&1 || true

echo "forcing an immediate resync"
# -s sets the system clock, -S steps it (rather than slewing), so a large offset is
# corrected now instead of over the next hour.
sntp -sS "$server" 2>&1 || echo "  sntp step failed; timed will still discipline the clock over the next minutes"

echo ""
echo "network time setting:"
systemsetup -getusingnetworktime 2>&1 || true

echo ""
python3 - "$server" <<'PY'
import time, json, urllib.request, sys
# Offset against the venue, round-trip corrected (positive => host behind).
best=None
for _ in range(5):
    try:
        t0=time.time()*1000
        with urllib.request.urlopen("https://api.binance.com/api/v3/time", timeout=15) as r:
            t1=json.load(r)["serverTime"]
        t2=time.time()*1000
    except Exception:
        continue
    off=t1-(t0+t2)/2; rtt=t2-t0
    if best is None or rtt<best[1]:
        best=(off,rtt)
if best is None:
    print("could not reach the venue to confirm")
else:
    off,rtt=best
    print(f"offset against the venue: {off:+.0f}ms (rtt {rtt:.0f}ms; positive means this host is behind)")
    if abs(off)<=1000:
        print("within the venue's own one-second tolerance. Good.")
    else:
        print("still outside one second. Check the time source and the network path.")
PY
