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
$before = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
try {
    $venue = (Invoke-RestMethod -Uri 'https://api.binance.com/api/v3/time' -TimeoutSec 15).serverTime
    $offset = $before - $venue
    Write-Host "offset against the venue: ${offset}ms (positive means this host is ahead)"
    if ([math]::Abs($offset) -le 1000) {
        Write-Host "within the venue's own one-second tolerance. Good."
    } else {
        Write-Host "still outside one second. Check the configured time source: w32tm /query /source"
    }
} catch {
    Write-Host "could not reach the venue to confirm: $_"
}
