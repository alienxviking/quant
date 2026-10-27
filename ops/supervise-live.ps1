<#
.SYNOPSIS
    Run one live session, once, and say how it ended. Never restart it.

.DESCRIPTION
    The record harness's supervisor restarts its child, because "runs seven
    days unattended" is a statement about the system and a recorder that died
    can be replaced by one that writes a RecorderRestart gap and carries on.
    A live run is the opposite case, and the owner settled it on 2026-10-26
    (docs/live-run.md, "Where it runs"): no automatic restart. Every way the
    binary ends is a reason not to start it again by itself:

      0  it traded and stopped cleanly. Done.
      1  it could not go on. A crash mid-order is criterion E's to exercise
         deliberately, not something to paper over with a respawn.
      2  it refused its arguments. The same arguments would be refused again.
      3  a gate refused and it recorded only. A restart meets the same gate.
      any other code: it was killed or crashed. The gates already refuse to
         trade after an unclean stop with orders working, so a restart would
         only record, and would bury the one log that says what happened.

    So this is not a loop. It takes the one-live-run lock, holds the host
    awake, runs target\release\live.exe with exactly the arguments run.json
    records, waits, writes the outcome in words into run.json and its log,
    runs the after-run checks (check-live.ps1), and lets go of everything.

    It reads what to run from run.json rather than from its own command line,
    for two reasons. powershell.exe -File cannot pass an array through
    Start-Process intact, so a list of arguments would arrive as one string;
    and run.json is the record of what was asked, so running exactly that
    means the record and the run cannot drift apart. What the binary says it
    used is copied into run.json too, from its banner: print what was used,
    not what was asked for.

    The one writer of run.json once the run has started, so that no two
    scripts race to rewrite it. stop-live.ps1 asks for a stop by creating the
    stop file and writes nothing here.

    Exit codes: the binary's own 0 to 3; 4 when it was killed or crashed; 5
    when this refused to start it (another live run holds the lock, a live.exe
    is already running, no binary, no manifest).

.PARAMETER Root
    The run's root, holding the run.json start-live.ps1 wrote.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Root
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'live-common.ps1')
$repo = (Resolve-Path (Join-Path $here '..')).Path
$exe  = Join-Path $repo 'target\release\live.exe'

$Root = (Resolve-Path $Root).Path
$manifest = Read-RunJson -Root $Root
if ($null -eq $manifest -or $manifest.mode -ne 'live') {
    Write-Host "no live run.json at $Root -- start a live run with ops\start-live.ps1"
    exit 5
}
$logDir = $manifest.log_dir
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
$log = Join-Path $logDir 'supervisor-live.log'

function Note {
    param([string] $Message)
    $line = "{0} {1}" -f (Get-UtcStamp), $Message
    Write-Host $line
    Add-Content -Path $log -Value $line -Encoding utf8
}

function Refuse {
    param([string] $Why)
    Note "REFUSED: $Why"
    Set-RunField -Root $Root -Fields @{
        supervisor = 'refused'
        outcome    = 'refused'
        outcome_meaning = $Why
        ended_at   = (Get-UtcStamp)
    }
    exit 5
}

Note "supervising live $($manifest.symbol) on $($manifest.environment) into $Root"

# One live run per host, and this is where it is enforced: the lock is held
# for this process's whole life. start-live.ps1 asks first, so that a second
# start is refused at the operator's prompt, and this asks again because two
# starts could both have asked before either supervisor existed.
$lock = Enter-LiveLock
if ($null -eq $lock) {
    Refuse 'another live run holds the one-live-run lock: one symbol, one process, one account (docs/live-run.md section 2)'
}
if ($script:LiveLockWasAbandoned) {
    Note 'the lock was abandoned by a supervisor that died without letting go; its run is over'
}

$awake = $null
$code = $null
try {
    $running = Get-LiveProcesses
    if ($running.Count -gt 0) {
        Refuse "a live.exe is already running (pid $(($running | ForEach-Object { $_.Id }) -join ', ')) outside this harness: one account, one process"
    }
    if (-not (Test-Path $exe)) {
        Refuse "no release binary at $exe -- run ops\preflight-live.ps1 first"
    }

    $held = Enable-KeepAwake
    $system = [QuantKeepAwake]::SystemState()
    if (($held -band $script:EsSystemRequired) -ne 0) {
        $awake = $held
        Note "keep-awake held: this thread asks Windows not to sleep (thread $(Format-ExecutionState $held), system $(Format-ExecutionState $system)), until this supervisor ends"
    } else {
        Note "WARNING: keep-awake could not be held (thread $(Format-ExecutionState $held)); keep this host awake by hand"
    }

    $env:NO_COLOR = '1'
    $stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')
    $out = Join-Path $logDir "live-$($manifest.symbol)-$stamp.log"
    $err = Join-Path $logDir "live-$($manifest.symbol)-$stamp.err"
    # Every argument is quoted. Start-Process joins an ArgumentList array with
    # spaces and quotes nothing, so an unquoted path with a space in it would
    # arrive as two arguments.
    $quoted = @()
    foreach ($argument in @($manifest.live_args)) { $quoted += "`"$argument`"" }

    $startedAt = Get-Date
    # Not -Wait: the pid is wanted before the wait, for run.json and for
    # stop-live.ps1. Reading .Handle at once keeps the process handle, without
    # which Windows PowerShell 5.1 reports no exit code for a process started
    # without -Wait.
    $process = Start-Process -FilePath $exe -ArgumentList $quoted `
        -RedirectStandardOutput $out -RedirectStandardError $err `
        -NoNewWindow -PassThru
    $null = $process.Handle
    Note "live pid $($process.Id) started, log $([System.IO.Path]::GetFileName($out))"
    Set-RunField -Root $Root -Fields @{
        supervisor     = 'running'
        supervisor_pid = $PID
        live_pid       = $process.Id
        live_log       = $out
        live_err       = $err
        live_started_at = (Get-UtcStamp)
    }

    # The banner: everything the binary is about to do, before it does any of
    # it. Copied into run.json once it is complete, which the blank line after
    # it says, or whatever of it there is if the binary ends first.
    $banner = @()
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        $lines = @(Read-SharedLines -Path $out | Where-Object { -not $_.StartsWith('{') })
        $blank = [array]::IndexOf($lines, '')
        if ($blank -ge 0) { $banner = $lines[0..($blank - 1)]; break }
        if ($process.HasExited) { $banner = $lines; break }
        Start-Sleep -Milliseconds 200
    }
    if ($banner.Count -gt 0) {
        Set-RunField -Root $Root -Fields @{ banner = $banner }
        foreach ($line in $banner) { Note "  $line" }
    }

    $process.WaitForExit()
    $code = $process.ExitCode
    $ranFor = [int]((Get-Date) - $startedAt).TotalSeconds
    $outcome = Get-LiveOutcome -Code $code
    $verdict = @(Read-SharedLines -Path $out | Where-Object { $_ -match '^verdict' }) -join ' '

    $stopFile = $manifest.stop_file
    $stopAsked = $null
    if (-not [string]::IsNullOrWhiteSpace($stopFile) -and (Test-Path $stopFile)) {
        $stopAsked = (Get-Item $stopFile).CreationTimeUtc.ToString('yyyy-MM-ddTHH:mm:ssZ')
    }

    Note "live exited $code after ${ranFor}s: $($outcome.outcome.ToUpper()) -- $($outcome.meaning)"
    if ($verdict) { Note "  $verdict" }
    if ($stopAsked) { Note "  a stop was asked for at $stopAsked (the stop file)" }
    Note 'not restarting: a live run is one attempt, whatever its ending (docs/live-run.md, "Where it runs")'
    Set-RunField -Root $Root -Fields @{
        supervisor      = 'finished'
        ended_at        = (Get-UtcStamp)
        exit_code       = $code
        outcome         = $outcome.outcome
        outcome_meaning = $outcome.meaning
        verdict         = $verdict
        stop_requested_at = $stopAsked
        ran_for_seconds = $ranFor
    }

    # The after-run checks, on what the run left: both capture trees verified,
    # and the journal reconciled, checked and replayed against the account
    # capture. Their verdict is recorded beside the outcome, never in place of
    # it: a run that stopped cleanly and a check that failed are two facts.
    Note 'after-run checks: ops\check-live.ps1'
    & (Join-Path $here 'check-live.ps1') -Root $Root
    $checks = 'failed'
    if ($LASTEXITCODE -eq 0) { $checks = 'clean' }
    Note "after-run checks $checks"
    Set-RunField -Root $Root -Fields @{ checks = $checks }
} finally {
    if ($null -ne $awake) {
        $was = Disable-KeepAwake
        $left = Get-ThreadAwakeState
        if (($was -band $script:EsSystemRequired) -ne 0) {
            Note "keep-awake released (this thread held $(Format-ExecutionState $was), and holds $(Format-ExecutionState $left) now)"
        } else {
            Note "WARNING: keep-awake release found $(Format-ExecutionState $was) on this thread, not the request made; Windows releases it when this process ends"
        }
    }
    Exit-LiveLock $lock
    Note 'supervisor finished'
}

if ($null -eq $code) { exit 5 }
if ($code -ge 0 -and $code -le 3) { exit $code }
exit 4
