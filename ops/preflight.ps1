<#
.SYNOPSIS
    Check that this machine can be trusted with a seven-day capture.

.DESCRIPTION
    Every check here exists because failing it wastes days rather than minutes.
    A recorder that starts and runs is not the same as one whose output will be
    worth anything at the end of the week, and the difference is almost always
    something knowable up front: a clock that is wrong, a disk that will fill on
    day five, a build that is not the one being tested.

    Blockers exit non-zero. Advisories print and continue. The distinction is
    whether the run would produce data that cannot be trusted (blocker) or data
    that is fine but less useful (advisory).

.PARAMETER Root
    Where the capture will be written.

.PARAMETER Days
    How long the run is planned for, used to size the disk estimate.

.PARAMETER Symbols
    Which instruments will be recorded, used to size the disk estimate.
#>
[CmdletBinding()]
param(
    [string]   $Root,
    [int]      $Days    = 7,
    [string[]] $Symbols = @('BTCUSDT', 'ETHUSDT')
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Resolve-Path (Join-Path $here '..')
if ([string]::IsNullOrWhiteSpace($Root)) { $Root = Join-Path $repo 'data\acceptance' }

# Budgeted well above the ~160 MB/day/symbol observed on a quiet market, because
# the point of a headroom check is to survive a busy one. A week of frantic
# trading is the run we most want to have recorded.
$MegabytesPerSymbolPerDay = 1024
$MaxClockOffsetMs = 1000

$blockers   = @()
$advisories = @()

function Report {
    param([string] $Name, [string] $Detail, [ValidateSet('ok', 'advisory', 'blocker')] [string] $Verdict)
    $mark = switch ($Verdict) { 'ok' { '  ok  ' } 'advisory' { ' note ' } 'blocker' { 'BLOCK ' } }
    Write-Host ("{0} {1,-22} {2}" -f $mark, $Name, $Detail)
    if ($Verdict -eq 'blocker')  { $script:blockers   += "$Name : $Detail" }
    if ($Verdict -eq 'advisory') { $script:advisories += "$Name : $Detail" }
}

Write-Host "preflight for a $Days-day capture of $($Symbols -join ', ')"
Write-Host "repo   $repo"
Write-Host "root   $Root"
Write-Host ""

# --- the build under test -------------------------------------------------
# Release, not debug: a seven-day run should be the binary we would deploy, and
# debug builds carry overflow checks and no optimisation. More to the point, a
# run that proves a debug build works has proved nothing about a release one.
$env:Path = "$env:USERPROFILE\.cargo\bin;$env:Path"
$buildLog = Join-Path $env:TEMP 'quant-preflight-build.log'
# Redirected by cmd, not by PowerShell, and deliberately.
#
# `cargo ... 2>&1` wraps every line cargo writes to stderr -- which is all of its
# ordinary progress output -- in an ErrorRecord, and under
# `ErrorActionPreference = Stop` that turns a successful build into a terminating
# error. `Start-Process -Wait` avoids the wrapping but hung here indefinitely
# after cargo had already exited. Handing the whole thing to cmd means PowerShell
# never sees the native streams at all, and `$LASTEXITCODE` still comes back.
Push-Location $repo
try {
    cmd /c "cargo build --release -p quant-binance --bin record -p quant-verify --bin verify > `"$buildLog`" 2>&1"
    $buildExit = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($buildExit -ne 0) {
    Report 'build' "cargo build --release failed, see $buildLog" 'blocker'
} else {
    Report 'build' 'release binaries built' 'ok'
}

# --- the host clock -------------------------------------------------------
# The one that caught us out. Venue latency is local_recv_ts - exchange_ts, so a
# clock that is two seconds fast reports two seconds of latency on a healthy
# link. It does not corrupt the capture -- every timestamp is shifted equally, so
# ordering and dispatch are unaffected -- but it makes one of the six acceptance
# criteria unmeasurable, which is reason enough not to start a week's run on it.
try {
    $before = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
    $venue  = (Invoke-RestMethod -Uri 'https://api.binance.com/api/v3/time' -TimeoutSec 15).serverTime
    $offset = $before - $venue
    if ([math]::Abs($offset) -gt $MaxClockOffsetMs) {
        Report 'host clock' "${offset}ms from the venue (limit ${MaxClockOffsetMs}ms) -- run ops\fix-clock.ps1 as admin" 'blocker'
    } else {
        Report 'host clock' "${offset}ms from the venue" 'ok'
    }
} catch {
    Report 'host clock' "could not reach the venue to check: $_" 'blocker'
}

$w32 = Get-Service w32time -ErrorAction SilentlyContinue
if ($null -eq $w32) {
    Report 'time service' 'w32time not present' 'advisory'
} elseif ($w32.Status -ne 'Running') {
    # A clock that is right now but unsynchronised will drift over seven days,
    # and a slow drift is harder to spot afterwards than a constant offset.
    Report 'time service' "w32time is $($w32.Status) -- the clock will drift over $Days days" 'blocker'
} else {
    Report 'time service' 'w32time running' 'ok'
}

# --- disk -----------------------------------------------------------------
$rootParent = Split-Path -Parent $Root
if (-not (Test-Path $rootParent)) { $rootParent = $repo }
$drive  = (Get-Item $rootParent).PSDrive
$freeGb = [math]::Round($drive.Free / 1GB, 1)
$needGb = [math]::Round(($MegabytesPerSymbolPerDay * $Symbols.Count * $Days) / 1024, 1)
if ($drive.Free -lt ($needGb * 1GB)) {
    Report 'disk' "${freeGb}GB free on $($drive.Name):, need about ${needGb}GB" 'blocker'
} else {
    Report 'disk' "${freeGb}GB free on $($drive.Name):, budgeting ${needGb}GB" 'ok'
}

# --- a clean root ---------------------------------------------------------
# The run's verdict has to mean something. Pointing the verifier at a tree that
# already holds captures from an older build makes every report ambiguous --
# which is exactly what happened with the pre-snapshot sessions in data\raw.
if (Test-Path (Join-Path $Root 'raw')) {
    $existing = @(Get-ChildItem -Path (Join-Path $Root 'raw') -Recurse -Filter 'part-*.bin.zst' -ErrorAction SilentlyContinue)
    if ($existing.Count -gt 0) {
        Report 'capture root' "$($existing.Count) capture files already here -- the run's verdict would cover them too" 'blocker'
    } else {
        Report 'capture root' 'empty' 'ok'
    }
} else {
    Report 'capture root' 'will be created' 'ok'
}

# --- metadata tier (optional by design) -----------------------------------
if ([string]::IsNullOrWhiteSpace($env:QUANT_DATABASE_URL)) {
    Report 'metadata' 'QUANT_DATABASE_URL unset -- recording without an index, which is allowed' 'advisory'
} else {
    # Same native-stderr trap as the build above: docker printing a connection
    # error must be an answer to this check, not a terminating error inside it.
    $pg = $null
    try {
        $ErrorActionPreference = 'Continue'
        $pg = (docker ps --filter 'name=quant-postgres' --format '{{.Status}}') -join ''
    } catch {
        $global:LASTEXITCODE = 1
    } finally {
        $ErrorActionPreference = 'Stop'
    }
    if ($LASTEXITCODE -ne 0) {
        Report 'metadata' 'docker unreachable -- the index will be missing, the capture will not be' 'advisory'
    } elseif ([string]::IsNullOrWhiteSpace($pg)) {
        Report 'metadata' 'quant-postgres not running -- start it with docker compose up -d' 'advisory'
    } else {
        Report 'metadata' "quant-postgres $pg" 'ok'
    }
}

# --- power ----------------------------------------------------------------
# A laptop that sleeps has stopped recording, and it will look exactly like a
# venue outage in the capture: a gap, a reconnect, and no explanation.
try {
    $scheme = (powercfg /getactivescheme) -replace '.*\(([^)]+)\).*', '$1'
    $sleepAc = (powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE | Select-String 'Current AC Power Setting Index').ToString()
    if ($sleepAc -match '0x00000000') {
        Report 'sleep' "'$scheme' does not sleep on AC" 'ok'
    } else {
        Report 'sleep' "'$scheme' sleeps on AC -- a sleeping host is an unexplained gap" 'advisory'
    }
} catch {
    Report 'sleep' 'could not read the power scheme' 'advisory'
}

Write-Host ""
if ($advisories.Count -gt 0) {
    Write-Host "advisories:"
    $advisories | ForEach-Object { Write-Host "  - $_" }
}
if ($blockers.Count -gt 0) {
    Write-Host ""
    Write-Host "BLOCKED. Fix these, or pass -Force to start-run.ps1 to accept them:"
    $blockers | ForEach-Object { Write-Host "  - $_" }
    exit 1
}

Write-Host "ready."
exit 0
