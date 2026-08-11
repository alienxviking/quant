<#
.SYNOPSIS
    Start the M1 acceptance run: supervised recorders plus periodic verification.

.DESCRIPTION
    The last criterion in docs/data-contract.md §7 is the one only time can
    satisfy -- seven consecutive days unattended, with zero unexplained gaps.
    This starts it.

    One supervised recorder per symbol, because the recorder is one process per
    instrument by design: routing a multiplexed stream would mean parsing every
    payload on the hot path, and separate sockets mean one symbol's reconnect
    blinds only that symbol. A verification loop runs alongside, during the
    capture rather than after it, so a defect costs hours instead of the week.

    Everything is detached, so closing this window does not stop the run. Nothing
    here survives a reboot -- see docs/acceptance-run.md for why that is a
    deliberate limit of a laptop run rather than something to paper over.

.PARAMETER Symbols
    Instruments to record. Two liquid ones by default: enough for the reconnect
    behaviour of one to be visible against the other still running.

.PARAMETER Root
    Capture root. Defaults to a directory of its own, so the verifier's verdict
    covers this run and nothing else.

.PARAMETER Days
    Length of the run.

.PARAMETER Force
    Start despite preflight blockers. The run will still produce data; some of
    the acceptance criteria may not be measurable from it.
#>
[CmdletBinding()]
param(
    [string[]] $Symbols = @('BTCUSDT', 'ETHUSDT'),
    [string]   $Root,
    [int]      $Days    = 7,
    [int]      $VerifyIntervalHours = 6,
    # Rehearsal. A run harness that has never been run is not a harness, and the
    # worst moment to discover a typo in it is four days into a capture. Overrides
    # -Days and tightens the verifier's cadence so a few minutes exercises every
    # path: start, supervise, record, verify, status, stop.
    [int]      $Minutes = 0,
    [switch]   $Force
)

$ErrorActionPreference = 'Stop'
$here   = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo   = Resolve-Path (Join-Path $here '..')
if ([string]::IsNullOrWhiteSpace($Root)) { $Root = Join-Path $repo 'data\acceptance' }
$logDir = Join-Path $Root 'logs'

& (Join-Path $here 'preflight.ps1') -Root $Root -Days $Days -Symbols $Symbols
$preflight = $LASTEXITCODE
if ($preflight -ne 0) {
    if (-not $Force) {
        Write-Host ""
        Write-Host "not starting. Fix the blockers above, or re-run with -Force to accept them."
        exit 1
    }
    Write-Host ""
    Write-Host "-Force: starting anyway, with the blockers above on the record."
}

New-Item -ItemType Directory -Path $Root -Force | Out-Null
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

$startedAt = (Get-Date).ToUniversalTime()
$rehearsal = $Minutes -gt 0
if ($rehearsal) {
    $endAt = $startedAt.AddMinutes($Minutes)
    $firstCheckSeconds = 45
    $verifyEvery = 1
} else {
    $endAt = $startedAt.AddDays($Days)
    $firstCheckSeconds = 300
    $verifyEvery = $VerifyIntervalHours
}

# Computed before the hashtable, for two PowerShell 5.1 reasons: `if` is a
# statement and cannot be a hashtable value, and git writes to stderr for things
# as ordinary as a line-ending notice -- which under `ErrorActionPreference =
# Stop` would abort the run over a warning about CRLF.
$commit  = 'unknown'
$subject = ''
$dirty   = $false
try {
    $ErrorActionPreference = 'Continue'
    $commit  = (git -C $repo rev-parse HEAD | Select-Object -First 1)
    $subject = (git -C $repo log -1 --format=%s | Select-Object -First 1)
    $dirty   = [bool]((git -C $repo status --porcelain) -join '')
} catch {
    Write-Host "  note: could not read git state: $_"
} finally {
    $ErrorActionPreference = 'Stop'
}

$database  = 'none'
if (-not [string]::IsNullOrWhiteSpace($env:QUANT_DATABASE_URL)) { $database = 'configured' }
$preflightState = 'forced'
if ($preflight -eq 0) { $preflightState = 'clean' }

# A record of what this run was, written before it starts. Seven days later the
# question "what was actually running?" should have an answer that does not
# depend on anybody's memory.
$manifest = [ordered]@{
    started_at     = $startedAt.ToString('u')
    ends_at        = $endAt.ToString('u')
    symbols        = $Symbols
    root           = (Resolve-Path $Root).Path
    duration       = ($endAt - $startedAt).ToString()
    rehearsal      = $rehearsal
    commit         = $commit
    commit_subject = $subject
    dirty          = $dirty
    database       = $database
    preflight      = $preflightState
    host           = $env:COMPUTERNAME
}
$manifest | ConvertTo-Json | Set-Content -Path (Join-Path $Root 'run.json') -Encoding utf8

Write-Host ""
if ($rehearsal) {
    Write-Host "starting $Minutes-minute REHEARSAL (not an acceptance run)"
} else {
    Write-Host "starting $Days-day run"
}
Write-Host "  symbols  $($Symbols -join ', ')"
Write-Host "  root     $((Resolve-Path $Root).Path)"
Write-Host "  ends     $($endAt.ToString('u'))"
$shortCommit = $manifest.commit
if ($shortCommit.Length -ge 9) { $shortCommit = $shortCommit.Substring(0, 9) }
Write-Host "  commit   $shortCommit  $($manifest.commit_subject)"
if ($manifest.dirty) {
    Write-Host "  WARNING  working tree is dirty: the binary under test is not a committed state"
}
Write-Host ""

function Launch {
    param([string] $Script, [string[]] $Arguments, [string] $Label)
    # Every argument is quoted. Start-Process joins an ArgumentList array with
    # spaces and does no quoting of its own, so an unquoted path containing a
    # space silently becomes two arguments -- and a supervisor started with half
    # a path writes its capture somewhere nobody will look for it.
    $quoted = @("`"$(Join-Path $here $Script)`"")
    foreach ($argument in $Arguments) { $quoted += "`"$argument`"" }
    $all = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File') + $quoted
    $p = Start-Process -FilePath 'powershell.exe' -ArgumentList $all -WindowStyle Hidden -PassThru
    Write-Host ("  started {0,-24} pid {1}" -f $Label, $p.Id)
    return $p.Id
}

$pids = @()
foreach ($symbol in $Symbols) {
    $pids += Launch 'supervise.ps1' @(
        '-Symbol', $symbol,
        '-Root', (Resolve-Path $Root).Path,
        '-EndAt', $endAt.ToString('o'),
        '-LogDir', $logDir
    ) "recorder $symbol"
}
$pids += Launch 'verify-loop.ps1' @(
    '-Root', (Resolve-Path $Root).Path,
    '-EndAt', $endAt.ToString('o'),
    '-LogDir', $logDir,
    '-IntervalHours', $verifyEvery,
    '-FirstCheckSeconds', $firstCheckSeconds
) 'verifier'

Set-Content -Path (Join-Path $Root 'run.pids') -Value ($pids -join "`n") -Encoding utf8

Write-Host ""
Write-Host "running. This window can be closed."
Write-Host "  check    powershell -File ops\status.ps1"
Write-Host "  stop     powershell -File ops\stop-run.ps1"
Write-Host "  logs     $logDir"
