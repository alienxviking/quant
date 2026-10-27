<#
.SYNOPSIS
    Start and synchronise the Windows time service. Must be run as Administrator.

.DESCRIPTION
    Venue latency is `local_recv_ts - exchange_ts`, so it measures the difference
    between two clocks as much as it measures a network. On the machine this was
    written for, `w32time` was stopped and the host was about two seconds ahead of
    Binance -- which the recorder faithfully reported as two seconds of venue
    latency on a link that was fine.

    A wrong clock does not corrupt a capture: every timestamp is shifted by the
    same amount, so ordering and dispatch -- the only things the engine acts on --
    are unaffected. What it ruins is every latency figure, any comparison against
    a second venue later, and the ability to line a capture up against anybody
    else's records. Over a seven-day run it also drifts, which is harder to
    correct for afterwards than a constant offset.

    Setting the service to start automatically matters as much as starting it: a
    clock that is right today and unsynchronised for a week will not be right on
    day seven.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

$admin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $admin) {
    Write-Host "This needs Administrator. Open an elevated PowerShell and run:"
    Write-Host ""
    Write-Host "    powershell -ExecutionPolicy Bypass -File `"$PSCommandPath`""
    Write-Host ""
    exit 1
}

Write-Host "setting w32time to start automatically"
Set-Service -Name w32time -StartupType Automatic

Write-Host "starting w32time"
Start-Service -Name w32time
Start-Sleep -Seconds 2

Write-Host "resynchronising"
w32tm /resync /force
Start-Sleep -Seconds 2

Write-Host ""
w32tm /query /status

Write-Host ""
# Confirmed the way the live binary reads it, round-trip corrected, by
# venue-check --clock (venue-clock.ps1 says why). This used to be now minus
# serverTime from one request, which folds the one-way latency into the
# offset: the figure this script exists to fix, misread. Built first: this
# is step 1 of the 8c procedure, before anything else has built the
# binaries, and a venue-check from before --clock answers with its usage.
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'venue-clock.ps1')
$repo = Split-Path -Parent $here
$venueCheck = Join-Path $repo 'target\release\venue-check.exe'
Write-Host "building venue-check, to read the clock with"
$built = Update-VenueCheck -Repo $repo
if ($built.code -ne 0) {
    Write-Host "could not build venue-check (exit $($built.code), see $($built.log)); not confirming with an older build"
    exit 1
}
$clock = Read-VenueClock -Exe $venueCheck
if ($null -eq $clock.offset_ms) {
    Write-Host "could not confirm against the venue: $($clock.detail)"
    Write-Host "(then run ops\preflight.ps1, which reads it the same way)"
} else {
    Write-Host "offset against the venue: $($clock.detail) (positive means this host is ahead)"
    if ($clock.ready) {
        Write-Host "within the venue's own one-second tolerance. Good."
    } else {
        Write-Host "still outside one second. Check the configured time source: w32tm /query /source"
    }
}
