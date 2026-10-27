<#
.SYNOPSIS
    How is the live run going, or how did it end?

.DESCRIPTION
    status.ps1 hands a live run's root here, so that one command answers for
    either kind of run. Everything shown comes from something the harness or
    the binary already wrote: run.json (the supervisor's record of the
    outcome, in words), the journal's own last lines, the capture trees on
    disk, and the verify loop's and the after-run checks' logs. Nothing is
    computed a second way, because a status view that derives its own numbers
    eventually disagrees with the thing it reports on.

.PARAMETER Root
    The run's root.

.PARAMETER Lines
    How many of the journal's last lines to show.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Root,
    [int] $Lines = 6
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'live-common.ps1')
$Root = (Resolve-Path $Root).Path
$m = Read-RunJson -Root $Root
if ($null -eq $m -or $m.mode -ne 'live') {
    Write-Host "no live run.json at $Root"
    exit 1
}

function Age {
    param([datetime] $When)
    $span = (Get-Date) - $When
    if ($span.TotalSeconds -lt 120) { return "{0:0}s ago" -f $span.TotalSeconds }
    if ($span.TotalMinutes -lt 120) { return "{0:0}m ago" -f $span.TotalMinutes }
    return "{0:0.0}h ago" -f $span.TotalHours
}

# --- what this run is --------------------------------------------------------
$venue = 'Spot Testnet, no money at risk'
if ($m.real_money) { $venue = 'LIVE: BINANCE, WITH REAL MONEY' }
if ($m.stand_in) { $venue += ', on a local stand-in (a rehearsal)' }
$short = "$($m.commit)"
if ($short.Length -ge 9) { $short = $short.Substring(0, 9) }
Write-Host "live     $($m.symbol) on $($m.environment)  commit $short  preflight $($m.preflight)"
Write-Host "venue    $venue"
Write-Host "started  $($m.started_at), $($m.minutes) minute(s) asked"
Write-Host "journal  $($m.journal)"

# --- the outcome, or what is running ----------------------------------------
Write-Host ""
switch ("$($m.supervisor)") {
    'finished' {
        Write-Host "ENDED    $("$($m.outcome)".ToUpper()) (exit $($m.exit_code)) at $($m.ended_at), after $($m.ran_for_seconds)s"
        Write-Host "         $($m.outcome_meaning)"
        if ($m.verdict) { Write-Host "         $($m.verdict)" }
        if ($m.stop_requested_at) { Write-Host "         a stop was asked for at $($m.stop_requested_at)" }
        if ($m.checks) { Write-Host "checks   after-run checks $($m.checks)" } else { Write-Host 'checks   after-run checks still running' }
    }
    'refused' {
        Write-Host "REFUSED  the supervisor did not start the binary: $($m.outcome_meaning)"
    }
    'running' {
        $live = Get-Process -Id $m.live_pid -ErrorAction SilentlyContinue
        if ($null -ne $live) {
            $up = (Get-Date) - $live.StartTime
            Write-Host ("RUNNING  live pid {0}, up {1:0}h{2:00}m, {3} MB" -f $live.Id, [math]::Floor($up.TotalHours), $up.Minutes, [math]::Round($live.WorkingSet64 / 1MB))
        } else {
            Write-Host "ENDING   live pid $($m.live_pid) has exited; the supervisor has not yet recorded how"
        }
        if (Test-Path "$($m.stop_file)") {
            Write-Host "         a stop has been asked for (the stop file exists); the run is shutting down"
        }
    }
    default {
        Write-Host "STARTING the supervisor has not yet started the binary"
    }
}
$sup = $null
if ($m.supervisor_pid) { $sup = Get-Process -Id $m.supervisor_pid -ErrorAction SilentlyContinue }
if ($null -ne $sup) { Write-Host "         supervisor pid $($sup.Id) holds the one-live-run lock and the keep-awake request" }

# --- the journal's last lines -----------------------------------------------
Write-Host ""
Write-Host "journal, last $Lines line(s)"
if (Test-Path "$($m.journal)") {
    $file = Get-Item "$($m.journal)"
    $tail = @(Get-Content -Path $file.FullName -Tail $Lines -ErrorAction SilentlyContinue)
    foreach ($line in $tail) {
        if ($line.Length -gt 150) { $line = $line.Substring(0, 147) + '...' }
        Write-Host "  $line"
    }
    $total = @(Get-Content -Path $file.FullName -ErrorAction SilentlyContinue).Count
    Write-Host ("  ({0} line(s), last written {1})" -f $total, (Age $file.LastWriteTime))
} else {
    Write-Host '  none yet (a run journals once its gates pass; one that records only never does)'
}

# --- the two capture trees ---------------------------------------------------
Write-Host ""
foreach ($tree in @(@{ name = 'raw'; label = 'market ' }, @{ name = 'account'; label = 'account' })) {
    $files = @(Get-ChildItem -Path (Join-Path $Root $tree.name) -Recurse -Filter 'part-*.bin.zst' -ErrorAction SilentlyContinue)
    if ($files.Count -eq 0) {
        Write-Host "$($tree.label)  no capture yet"
        continue
    }
    $bytes = ($files | Measure-Object -Property Length -Sum).Sum
    $newest = $files | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    Write-Host ("{0}  {1} file(s), {2} KB, last written {3}" -f $tree.label, $files.Count, [math]::Round($bytes / 1KB, 1), (Age $newest.LastWriteTime))
}
# The account stream's own state, as the journal records it: the engine writes
# an `account` line when the stream is lost and when it is regained.
if (Test-Path "$($m.journal)") {
    $accountLine = Get-Content -Path "$($m.journal)" -ErrorAction SilentlyContinue |
        Where-Object { $_ -match '"type":"account"' } | Select-Object -Last 1
    if ($null -ne $accountLine) {
        $change = '?'
        if ($accountLine -match '"change":"([a-z_]+)"') { $change = $Matches[1] }
        Write-Host "         the account stream was last $change (the journal's last account line)"
    }
}

# --- verification ------------------------------------------------------------
Write-Host ""
Write-Host "verification while it runs"
$verifyLog = Join-Path $m.log_dir 'verify.log'
if (Test-Path $verifyLog) {
    Get-Content $verifyLog | Select-Object -Last 4 | ForEach-Object { Write-Host "  $_" }
} else {
    Write-Host '  not run yet'
}
$checkLog = Join-Path $m.log_dir 'check-live.log'
if (Test-Path $checkLog) {
    Write-Host ""
    Write-Host "after-run checks"
    Get-Content $checkLog | Select-Object -Last 6 | ForEach-Object { Write-Host "  $_" }
}
$drive = (Get-Item $Root).PSDrive
Write-Host ""
Write-Host ("disk     {0} GB free on {1}:" -f [math]::Round($drive.Free / 1GB, 1), $drive.Name)
exit 0
