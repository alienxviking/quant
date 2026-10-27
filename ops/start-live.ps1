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
         An earlier run's run.json and after-run log at this root, which
         -Resume or a run that never journalled leaves, are kept beside the
         logs under that run's start time rather than written over.
      4. supervise-live.ps1, detached and hidden, so this window can be
         closed; a wait until the supervisor has the lock and the binary is
         running, so a refusal is said here; and a wait until the binary's
         gates have answered, so that a run which will only record is said
         here too, rather than at the end of the afternoon.
      5. verify-loop.ps1, detached, once the binary runs, and only then: a
         refused start leaves nothing behind. It ends itself when run.json
         says the run has.

    Exit 0 when the binary runs and its gates passed, so it trades; 3 when it
    runs and a gate refused, so it records only (stop it with stop-live.ps1,
    or let it record); 4 when it runs and the gates had not answered within
    -GateSeconds; 1 when nothing was started.

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
    # How long to wait for the binary's gates to answer. They make signed
    # requests of the venue, each bounded by the binary's own timeouts.
    [int]    $GateSeconds = 180,
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
# One spelling of the root for every script and every command line: full, and
# without a trailing backslash, which C:\runs\one\ and C:\runs\one would
# otherwise make two roots to anything comparing them. A drive's own root
# keeps its backslash, since C: alone means that drive's current directory.
$Root = [System.IO.Path]::GetFullPath($Root)
if ($Root.Length -gt 3) { $Root = $Root.TrimEnd('\') }
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

# An earlier run's record at this root is kept, not written over: -Resume
# continues its journal, and a run that recorded only or never started leaves
# a run.json and its logs behind. run.json, check-live.log and verify.log go
# beside the logs under that run's own start time, so the root's copies are
# always this run's, and the status view never shows an earlier run's verdict
# under this one's.
$previous = $null
try { $previous = Read-RunJson -Root $Root } catch { $previous = $null }
if ($null -ne $previous -or (Test-Path (Join-Path $Root 'run.json'))) {
    $was = "$($previous.started_at)" -replace '[^0-9TZ]', ''
    if ([string]::IsNullOrWhiteSpace($was)) { $was = 'unknown-' + $stamp }
    $keptJson = Join-Path $logDir "run-$was.json"
    Move-Item -LiteralPath (Join-Path $Root 'run.json') -Destination $keptJson -Force
    foreach ($name in @('check-live', 'verify')) {
        $earlier = Join-Path $logDir "$name.log"
        if (Test-Path $earlier) { Move-Item -LiteralPath $earlier -Destination (Join-Path $logDir "$name-$was.log") -Force }
    }
    Write-Host "  kept the earlier run's record: $keptJson"
}
# A verify loop still watching this root belongs to an earlier run whose
# supervisor died before it could say the run had ended: no live run holds the
# lock and no live.exe runs, or the checks above would have refused. Found by
# its command line, never by a pid an earlier run wrote down.
$stale = @(Stop-VerifyLoops -Root $Root)
if ($stale.Count -gt 0) { Write-Host "  stopped an earlier run's verify loop: pid $($stale -join ', ')" }

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
    # Every argument quoted by the one rule (native-args.ps1): Start-Process
    # joins an ArgumentList with spaces and quotes nothing, and a root ending
    # in a backslash would otherwise escape its own closing quote.
    $all = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File') + (ConvertTo-NativeArguments (@((Join-Path $here $Script)) + $Arguments))
    $p = Start-Process -FilePath 'powershell.exe' -ArgumentList $all -WindowStyle Hidden -PassThru
    $null = $p.Handle
    Write-Host ("  started {0,-12} pid {1}" -f $Label, $p.Id)
    return $p
}

function Wait-RunJson {
    # Until Holds is true of run.json, the supervisor has exited, or Seconds
    # have passed: the manifest last read, and why the wait ended.
    param([scriptblock] $Holds, [int] $Seconds)
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ($true) {
        $now = Read-RunJson -Root $Root
        if ($null -ne $now -and (& $Holds $now)) { return @{ manifest = $now; why = 'held' } }
        if ($supervisor.HasExited) { return @{ manifest = (Read-RunJson -Root $Root); why = 'exited' } }
        if ((Get-Date) -gt $deadline) { return @{ manifest = $now; why = 'timeout' } }
        Start-Sleep -Milliseconds 200
    }
}

function Say-Commands {
    Write-Host "  watch    powershell -File ops\status.ps1 -Root `"$Root`""
    Write-Host "  stop     powershell -File ops\stop-live.ps1 -Root `"$Root`""
    Write-Host "  logs     $logDir"
}

$supervisor = Launch 'supervise-live.ps1' @('-Root', $Root) 'supervisor'

# Until the supervisor has the lock and the binary is running, or has said it
# will not: a detached refusal would otherwise be said to nobody.
$waited = Wait-RunJson -Seconds 60 -Holds { param($m) "$($m.supervisor)" -ne 'starting' }
$now = $waited.manifest
$state = "$($now.supervisor)"
if ($waited.why -ne 'held' -and $state -eq 'starting') { $state = $waited.why }
Write-Host ""
switch ($state) {
    'refused' {
        Write-Host "NOT STARTED: the supervisor refused -- $($now.outcome_meaning)"
        exit 1
    }
    'running' { }
    'finished' { }
    default {
        Write-Host "NOT KNOWN TO HAVE STARTED: the supervisor has not reported ($state); read $logDir\supervisor-live.log"
        exit 1
    }
}

# The binary runs. Before this window is closed, whether it will trade: its
# gates answer within seconds, and a refusal means the afternoon is a
# recording, which is cheap to know now and dear to find out at the end.
if ($state -eq 'running') {
    $verifier = Launch 'verify-loop.ps1' @(
        '-Root', $Root,
        '-EndAt', $endsBy.ToString('o'),
        '-LogDir', $logDir,
        '-Journal', $journal,
        '-IntervalSeconds', ($VerifyEveryMinutes * 60),
        '-FirstCheckSeconds', $FirstCheckSeconds,
        '-UntilRunEnds'
    ) 'verifier'
    Write-Host "  waiting up to ${GateSeconds}s for the binary's gates to answer"
    $waited = Wait-RunJson -Seconds $GateSeconds -Holds { param($m) $null -ne $m.gates -or "$($m.supervisor)" -eq 'finished' }
    $now = $waited.manifest
}
Write-Host ""
if ("$($now.supervisor)" -eq 'finished') {
    Write-Host "the run has already ended: $("$($now.outcome)".ToUpper()) -- $($now.outcome_meaning)"
    if ($now.verdict) { Write-Host "  $($now.verdict)" }
    Write-Host "  logs     $logDir"
    switch ("$($now.outcome)") {
        'stopped' { exit 0 }
        'recorded-only' { exit 3 }
        default { exit 1 }
    }
}
switch ("$($now.gates)") {
    'passed' {
        Write-Host "TRADING: live pid $($now.live_pid), gates passed, run tag $($now.run_tag). This window can be closed."
        Say-Commands
        exit 0
    }
    'refused' {
        Write-Host "######################################################################"
        Write-Host "#  RECORDING ONLY, NOT TRADING: a gate refused                       #"
        Write-Host "######################################################################"
        Write-Host "  $($now.gate_refusal)"
        Write-Host "  live pid $($now.live_pid) records the market for its $Minutes minute(s) and trades nothing."
        Write-Host "  Stop it now with stop-live.ps1 if a recording is not what the afternoon is for."
        Say-Commands
        exit 3
    }
    default {
        Write-Host "RUNNING, NOT YET KNOWN WHETHER IT TRADES: live pid $($now.live_pid) has not said whether its gates passed after ${GateSeconds}s."
        Write-Host "  status.ps1 shows it the moment it does."
        Say-Commands
        exit 4
    }
}
