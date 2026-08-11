<#
.SYNOPSIS
    How is the acceptance run going?

.DESCRIPTION
    A miniature of what M7 is meant to deliver: one command that answers "what is
    it doing right now" without anybody reading raw logs. If this cannot be
    answered in a few seconds, a seven-day run is not really being watched.

    Everything shown here comes from something the recorder or verifier already
    emits. Nothing is computed a second way, because a status view that derives
    its own numbers eventually disagrees with the thing it is reporting on.
#>
[CmdletBinding()]
param(
    [string] $Root
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) { $Root = Join-Path (Join-Path $here '..') 'data\acceptance' }
if (-not (Test-Path $Root)) {
    Write-Host "no run at $Root"
    exit 1
}
$Root   = (Resolve-Path $Root).Path
$logDir = Join-Path $Root 'logs'

$manifestPath = Join-Path $Root 'run.json'
if (Test-Path $manifestPath) {
    $m = Get-Content $manifestPath -Raw | ConvertFrom-Json
    $started = [datetime]::Parse($m.started_at).ToUniversalTime()
    $ends    = [datetime]::Parse($m.ends_at).ToUniversalTime()
    $now     = (Get-Date).ToUniversalTime()
    $elapsed = $now - $started
    $left    = $ends - $now
    $kind = 'run'
    if ($m.rehearsal) { $kind = 'REHEARSAL' }
    $short = $m.commit
    if ($short.Length -ge 9) { $short = $short.Substring(0, 9) }
    Write-Host "$kind      $($m.symbols -join ', ')  commit $short  preflight $($m.preflight)"
    if ($left.TotalSeconds -le 0) {
        Write-Host ("elapsed  {0} of {1}   FINISHED" -f $elapsed.ToString('d\.hh\:mm\:ss'), $m.duration)
    } else {
        Write-Host ("elapsed  {0} of {1}   remaining {2}" -f `
            $elapsed.ToString('d\.hh\:mm\:ss'), $m.duration, $left.ToString('d\.hh\:mm\:ss'))
    }
} else {
    Write-Host "run      (no run.json -- started by hand?)"
}

# --- processes ------------------------------------------------------------
$recorders = @(Get-Process record -ErrorAction SilentlyContinue)
Write-Host ""
Write-Host "processes"
if ($recorders.Count -eq 0) {
    Write-Host "  no recorder running"
} else {
    foreach ($p in $recorders) {
        $up = (Get-Date) - $p.StartTime
        Write-Host ("  record pid {0,-7} up {1:0}h{2:00}m  {3} MB" -f `
            $p.Id, $up.TotalHours, $up.Minutes, [math]::Round($p.WorkingSet64 / 1MB))
    }
}

# --- what each symbol last said ------------------------------------------
# The metrics line, straight from the recorder. Queue depth is the one to read
# first: without it a stalled writer and a quiet market look identical.
Write-Host ""
Write-Host "latest metrics"
$symbolLogs = Get-ChildItem -Path $logDir -Filter '*.log' -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -notlike 'supervisor-*' -and $_.Name -notlike 'verify*' }
$bySymbol = $symbolLogs | Group-Object { ($_.BaseName -split '-')[0] }
if ($null -eq $bySymbol -or $bySymbol.Count -eq 0) {
    Write-Host "  none yet"
} else {
    foreach ($group in $bySymbol) {
        $newest = $group.Group | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        $line = Get-Content $newest.FullName -ErrorAction SilentlyContinue |
            Select-String 'metrics ' | Select-Object -Last 1
        if ($null -eq $line) {
            $line = Get-Content $newest.FullName -ErrorAction SilentlyContinue | Select-Object -Last 1
        }
        Write-Host "  $($group.Name)"
        Write-Host "    $($line -replace '^\s+', '')"
    }
}

# --- restarts -------------------------------------------------------------
# A restart is not a failure -- the format records one as a gap and the verifier
# reads it -- but a rising count is the signal that something is unhealthy.
Write-Host ""
Write-Host "restarts"
$supervisorLogs = @(Get-ChildItem -Path $logDir -Filter 'supervisor-*.log' -ErrorAction SilentlyContinue)
if ($supervisorLogs.Count -eq 0) {
    Write-Host "  no supervisor logs"
} else {
    foreach ($log in $supervisorLogs) {
        $attempts = @(Select-String -Path $log.FullName -Pattern 'attempt \d+ starting')
        $failures = @(Select-String -Path $log.FullName -Pattern 'exited [1-9]')
        $symbol = $log.BaseName -replace '^supervisor-', ''
        Write-Host ("  {0,-10} {1} attempt(s), {2} non-zero exit(s)" -f $symbol, $attempts.Count, $failures.Count)
    }
}

# --- verification ---------------------------------------------------------
Write-Host ""
Write-Host "verification"
$verifyLog = Join-Path $logDir 'verify.log'
if (Test-Path $verifyLog) {
    Get-Content $verifyLog | Select-Object -Last 4 | ForEach-Object { Write-Host "  $_" }
} else {
    Write-Host "  not run yet"
}

# --- on disk --------------------------------------------------------------
Write-Host ""
$files = @(Get-ChildItem -Path (Join-Path $Root 'raw') -Recurse -Filter 'part-*.bin.zst' -ErrorAction SilentlyContinue)
$bytes = ($files | Measure-Object -Property Length -Sum).Sum
$sessions = @(Get-ChildItem -Path (Join-Path $Root 'raw') -Recurse -Directory -Filter 'session=*' -ErrorAction SilentlyContinue)
Write-Host ("capture  {0} files in {1} session(s), {2} MB" -f `
    $files.Count, $sessions.Count, [math]::Round($bytes / 1MB, 1))
$drive = (Get-Item $Root).PSDrive
Write-Host ("disk     {0} GB free on {1}:" -f [math]::Round($drive.Free / 1GB, 1), $drive.Name)
