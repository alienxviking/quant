<#
.SYNOPSIS
    Start a live run: one symbol, one process, one attempt, on Spot Testnet
    unless real money is asked for by name.

.DESCRIPTION
    The live mode of the PowerShell harness, docs/live-run.md section 4 item
    8, part 8b, in the shape the owner settled on 2026-10-26 ("Where it
    runs"): PowerShell rather than WSL, and no automatic restart.

    A script of its own rather than a -Live switch on start-run.ps1, which
    the bash half's argument against duplication would suggest. That argument
    was that a paper run differs from a recorder run by one argv, so a second
    harness would be nine hundred lines that must agree for ever. A live run
    differs in almost everything start-run.ps1 decides: one symbol where it
    defaults to two and loops over them; one attempt where its supervisor's
    whole design is the restart loop; minutes where it counts days; a
    preflight that signs requests with a key where its own needs no
    credentials; a stop that cannot be a console event; and a refusal to run
    beside another live run anywhere on the host. A switch would put a branch
    in every one of those places, inside the scripts that ran M1's week, and
    "the record harness unchanged in behaviour" would become a claim about
    every branch rather than a diff that is empty. What is shared is shared
    by dot-sourcing live-common.ps1, and verify-loop.ps1 and status.ps1 are
    extended rather than copied, because there the difference is small.

    In order:
      1. Refuse a second live run on this host, whatever -Force says.
      2. preflight-live.ps1: the build, the clean root, the journal, disk,
         the key variables, venue-check as the binary's first gate asks it,
         the time service.
      3. run.json: the run's parameters, the binary's exact arguments, the
         commit, the preflight's verdict, written before anything launches.
      4. supervise-live.ps1 and verify-loop.ps1, detached and hidden, so this
         window can be closed; and a wait until the supervisor has the lock
         and the binary is running, so a refusal is said here.

    Stop it with ops\stop-live.ps1, which asks the binary to stop cleanly
    through its stop file. Watch it with ops\status.ps1.

.PARAMETER Symbol
    The one instrument, spelled as the venue spells it. One: two live
    processes would share one balance with two private loss budgets.

.PARAMETER RealMoney
    Trade on Binance itself, with real money. Without it the run is on Spot
    Testnet, as the binary's own default is, so that the run needing no money
    is the one needing no switch.

.PARAMETER Root
    The run's data root: data\testnet or data\live in the repository unless
    given, as the binary chooses.

.PARAMETER Minutes
    How long the binary captures before stopping by itself.

.PARAMETER Qty
    The size of each position, passed to the binary and to venue-check's
    trade probe alike, so the two cannot disagree.

.PARAMETER Rest
    With -WsApi and -MarketWs, a local stand-in for every endpoint, for a
    rehearsal (ops\rehearse-live.ps1). All three or none: a run half on a
    stand-in and half on a venue is not a rehearsal of anything.

.PARAMETER Resume
    Continue the journal, and the capture root, an earlier session left.

.PARAMETER Force
    Start despite preflight blockers, written into run.json as forced. Never
    overrides the one-live-run rule.
#>
[CmdletBinding()]
param(
    [string] $Symbol = 'BTCUSDT',
    [switch] $RealMoney,
    [string] $Root,
    [int]    $Minutes = 240,
    [string] $Qty = '0.001',
    [string] $Cash,
    [string] $Fast,
    [string] $Slow,
    [string] $IntervalSecs,
    [string] $MaxOrder,
    [string] $MaxPosition,
    [string] $MaxDailyLoss,
    [string] $MaxOrders,
    [string] $Rest,
    [string] $WsApi,
    [string] $MarketWs,
    [switch] $Resume,
    [switch] $Force,
    [int]    $VerifyEveryMinutes = 60,
    [int]    $FirstCheckSeconds = 300,
    # For the harness's own tests, which have no cargo build to wait for and
    # no venue to ask. Each is written into run.json when used.
    [switch] $SkipBuild,
    [switch] $SkipVenueCheck
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'live-common.ps1')
$repo = (Resolve-Path (Join-Path $here '..')).Path

# --- what is being asked -----------------------------------------------------
if ($Symbol -cnotmatch '^[A-Z0-9]+$') {
    Write-Host "not starting: -Symbol takes one symbol, spelled as the venue spells it (BTCUSDT). A live run trades one instrument: two processes would share one balance with two private loss budgets."
    exit 1
}
$overrides = @($Rest, $WsApi, $MarketWs | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
if ($overrides.Count -ne 0 -and $overrides.Count -ne 3) {
    Write-Host "not starting: -Rest, -WsApi and -MarketWs go together, for a local stand-in of every endpoint. A run half on a stand-in and half on a venue is not a rehearsal of anything."
    exit 1
}
$standIn = $overrides.Count -eq 3
if ($Minutes -le 0) { Write-Host 'not starting: -Minutes must be positive'; exit 1 }

$environment = 'testnet'
if ($RealMoney) { $environment = 'live' }
if ([string]::IsNullOrWhiteSpace($Root)) { $Root = Join-Path $repo "data\$environment" }
$Root = [System.IO.Path]::GetFullPath($Root)
$logDir = Join-Path $Root 'logs'

Write-Host ""
if ($RealMoney) {
    Write-Host "######################################################################"
    Write-Host "#  LIVE: BINANCE, WITH REAL MONEY (-RealMoney)                       #"
    Write-Host "#  Every order this run sends is a real order on a real account.     #"
    Write-Host "#  A tripped kill switch leaves a position open: a person closes it. #"
    Write-Host "######################################################################"
} else {
    Write-Host "venue    Spot Testnet, no money at risk (the default; -RealMoney for real money)"
}
if ($standIn) {
    Write-Host "         every endpoint overridden: a local stand-in, a REHEARSAL"
}
Write-Host ""

# --- one live run per host, not overridable ----------------------------------
if (Test-LiveLockHeld) {
    Write-Host "not starting: a live supervisor is already running on this host. One symbol, one process, one account (docs/live-run.md section 2); -Force does not override this."
    Write-Host "  see it: powershell -File ops\status.ps1 -Root <its root>"
    exit 1
}
$others = Get-LiveProcesses
if ($others.Count -gt 0) {
    Write-Host "not starting: a live.exe is already running (pid $(($others | ForEach-Object { $_.Id }) -join ', ')). One account, one process; -Force does not override this."
    exit 1
}

# --- preflight ---------------------------------------------------------------
$preflightArgs = @{
    Root = $Root; Symbol = $Symbol; Environment = $environment; Qty = $Qty; Minutes = $Minutes
}
if ($standIn) { $preflightArgs.Rest = $Rest }
if ($Resume) { $preflightArgs.Resume = $true }
if ($SkipBuild) { $preflightArgs.NoBuild = $true }
if ($SkipVenueCheck) { $preflightArgs.NoVenue = $true }
& (Join-Path $here 'preflight-live.ps1') @preflightArgs
$preflight = $LASTEXITCODE
if ($preflight -ne 0) {
    if (-not $Force) {
        Write-Host ""
        Write-Host "not starting. Fix the blockers above, or re-run with -Force to accept them on the record."
        exit 1
    }
    Write-Host ""
    Write-Host "-Force: starting anyway, with the blockers above on the record."
}

New-Item -ItemType Directory -Path $Root -Force | Out-Null
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

# --- the binary's arguments, exactly -----------------------------------------
$startedAt = (Get-Date).ToUniversalTime()
$stamp = $startedAt.ToString('yyyyMMdd-HHmmss')
$journal = Join-Path $Root "live-$Symbol.jsonl"
# A stop file of this run's own: one left by an earlier run cannot stop this
# one, and the binary refuses a stop file that already exists in any case.
$stopFile = Join-Path $Root "live-$stamp.stop"
$liveArgs = @(
    '--symbol', $Symbol,
    '--env', $environment,
    '--root', $Root,
    '--journal', $journal,
    '--stop-file', $stopFile,
    '--minutes', "$Minutes",
    '--qty', $Qty
)
$optional = [ordered]@{
    '--cash' = $Cash; '--fast' = $Fast; '--slow' = $Slow; '--interval-secs' = $IntervalSecs
    '--max-order' = $MaxOrder; '--max-position' = $MaxPosition
    '--max-daily-loss' = $MaxDailyLoss; '--max-orders' = $MaxOrders
}
foreach ($flag in $optional.Keys) {
    if (-not [string]::IsNullOrWhiteSpace($optional[$flag])) { $liveArgs += @($flag, $optional[$flag]) }
}
if ($standIn) { $liveArgs += @('--rest', $Rest, '--ws-api', $WsApi, '--market-ws', $MarketWs) }

# Computed before the manifest, for two PowerShell 5.1 reasons: if is a
# statement and cannot be a hashtable value, and git writes to stderr for
# things as ordinary as a line-ending notice, which under Stop would abort.
$commit = 'unknown'; $subject = ''; $dirty = $false
try {
    $ErrorActionPreference = 'Continue'
    # Strings only: a git that fails hands back ErrorRecords, which run.json
    # would otherwise hold as objects rather than as an unknown commit.
    # Wrapped twice: one line out of a filter is a bare string, and a bare
    # string indexed at 0 is its first character.
    $said = @(@(git -C $repo rev-parse HEAD 2>$null) | Where-Object { $_ -is [string] })
    if ($said.Count -gt 0) { $commit = [string]$said[0] }
    $said = @(@(git -C $repo log -1 --format=%s 2>$null) | Where-Object { $_ -is [string] })
    if ($said.Count -gt 0) { $subject = [string]$said[0] }
    $said = @(@(git -C $repo status --porcelain 2>$null) | Where-Object { $_ -is [string] })
    $dirty = $said.Count -gt 0
} catch {
    Write-Host "  note: could not read git state: $_"
} finally {
    $ErrorActionPreference = 'Stop'
}
$preflightState = 'clean'
if ($preflight -ne 0) { $preflightState = 'forced' }
$skipped = @()
if ($SkipBuild) { $skipped += 'build' }
if ($SkipVenueCheck) { $skipped += 'venue-check' }
# Long enough for the binary's own limit, its shutdown and the after-run
# checks; the verify loop runs until then, and stop-live.ps1 ends it sooner.
$endsBy = $startedAt.AddMinutes($Minutes + 15)

$manifest = [ordered]@{
    mode            = 'live'
    environment     = $environment
    real_money      = [bool]$RealMoney
    stand_in        = $standIn
    symbol          = $Symbol
    root            = $Root
    journal         = $journal
    stop_file       = $stopFile
    log_dir         = $logDir
    started_at      = $startedAt.ToString('yyyy-MM-ddTHH:mm:ssZ')
    minutes         = $Minutes
    ends_by         = $endsBy.ToString('yyyy-MM-ddTHH:mm:ssZ')
    qty             = $Qty
    resume          = [bool]$Resume
    live_args       = $liveArgs
    commit          = $commit
    commit_subject  = $subject
    dirty           = $dirty
    preflight       = $preflightState
    preflight_skipped = $skipped
    host            = $env:COMPUTERNAME
    supervisor      = 'starting'
}
Write-RunJson -Root $Root -Manifest $manifest

Write-Host ""
Write-Host "starting a $Minutes-minute live run of $Symbol on $environment"
Write-Host "  root     $Root"
Write-Host "  journal  $journal"
$short = "$commit"
if ($short.Length -ge 9) { $short = $short.Substring(0, 9) }
Write-Host "  commit   $short  $subject"
if ($dirty) { Write-Host "  WARNING  working tree is dirty: the binary under test is not a committed state" }
if ($preflightState -eq 'forced') { Write-Host "  WARNING  preflight was forced: its blockers are on the record in run.json" }
if ($skipped.Count -gt 0) { Write-Host "  WARNING  preflight skipped: $($skipped -join ', ')" }
Write-Host ""

function Launch {
    param([string] $Script, [string[]] $Arguments, [string] $Label)
    # Every argument quoted: Start-Process joins an ArgumentList with spaces
    # and quotes nothing.
    $quoted = @("`"$(Join-Path $here $Script)`"")
    foreach ($argument in $Arguments) { $quoted += "`"$argument`"" }
    $all = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File') + $quoted
    $p = Start-Process -FilePath 'powershell.exe' -ArgumentList $all -WindowStyle Hidden -PassThru
    Write-Host ("  started {0,-12} pid {1}" -f $Label, $p.Id)
    return $p
}

$supervisor = Launch 'supervise-live.ps1' @('-Root', $Root) 'supervisor'
$verifier = Launch 'verify-loop.ps1' @(
    '-Root', $Root,
    '-EndAt', $endsBy.ToString('o'),
    '-LogDir', $logDir,
    '-Journal', $journal,
    '-IntervalSeconds', ($VerifyEveryMinutes * 60),
    '-FirstCheckSeconds', $FirstCheckSeconds
) 'verifier'
Set-Content -Path (Join-Path $Root 'run.pids') -Value (@($supervisor.Id, $verifier.Id) -join "`n") -Encoding utf8

# Until the supervisor has the lock and the binary is running, or has said it
# will not: a detached refusal would otherwise be said to nobody.
$deadline = (Get-Date).AddSeconds(60)
$state = 'starting'
while ((Get-Date) -lt $deadline) {
    $now = Read-RunJson -Root $Root
    if ($null -ne $now -and $now.supervisor -ne 'starting') { $state = $now.supervisor; break }
    if ($supervisor.HasExited) { $state = 'exited'; break }
    Start-Sleep -Milliseconds 200
}
$now = Read-RunJson -Root $Root
Write-Host ""
switch ($state) {
    'running' {
        Write-Host "running: live pid $($now.live_pid). This window can be closed."
    }
    'finished' {
        Write-Host "the run has already ended: $($now.outcome) -- $($now.outcome_meaning)"
    }
    'refused' {
        Write-Host "NOT STARTED: the supervisor refused -- $($now.outcome_meaning)"
        exit 1
    }
    default {
        Write-Host "the supervisor has not reported ($state); read $logDir\supervisor-live.log"
        exit 1
    }
}
Write-Host "  watch    powershell -File ops\status.ps1 -Root `"$Root`""
Write-Host "  stop     powershell -File ops\stop-live.ps1 -Root `"$Root`""
Write-Host "  logs     $logDir"
exit 0
