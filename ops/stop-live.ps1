<#
.SYNOPSIS
    Stop a live run cleanly, and say how it ended.

.DESCRIPTION
    Cleanly is the whole point. A live run's clean stop is section 1's
    shutdown in docs/live-run.md: each resting order's cancel through the
    record, the settle, the closing checkpoint, `stopped` and the end
    balances, and both captures sealed with a trailer. A killed binary does
    none of it: orders it left working are watched by nobody, its journal has
    no `stopped` line, so the next run's last-session gate refuses to trade
    until a person has looked, and its segments look like a crash for ever.

    The binary stops cleanly on Ctrl-C, and on Windows no other process can
    give it one: it runs in the hidden console its supervisor was started in,
    a console control event reaches only the processes attached to the
    console it is raised in, and CloseMainWindow, which stop-run.ps1 uses on
    the recorder, finds no window to close (MainWindowHandle 0, checked
    against the real binary) and would raise CTRL_CLOSE_EVENT if it did. So
    the harness gives the binary a stop file, --stop-file, which it watches
    beside Ctrl-C on the same stop path, and this creates it.

    The binary refuses a stop file that already exists when it starts, as a
    bad argument, so the file is not created until the binary has read its
    arguments: while the supervisor is still starting it, this waits for its
    banner, which is printed only once the arguments are taken.

    Unlike stop-run.ps1 this does not stop the supervisor first. A live
    supervisor never restarts anything, so there is nothing to race; and it is
    the supervisor that records the outcome, runs the after-run checks and
    lets go of the keep-awake request and the one-live-run lock, so it is left
    to finish. The verify loop ends itself when run.json says the run has, and
    this ends it too, found by its command line and never by a pid written
    down earlier, which Windows may since have given to another process.

    A supervisor that is gone without recording how the run ended (killed, or
    its host's session ended) is said as that, with what the binary's own log
    says, rather than waited on for ever.

    What the operator types:

      powershell -File ops\stop-live.ps1 -Root data\testnet

    Exit 0 when the run ended cleanly (stopped, or recorded only), 1 when it
    did not end within the grace period or ended any other way, and 2 when
    there is no live run at the root.

.PARAMETER Root
    The run's root.

.PARAMETER GraceSeconds
    How long to wait for the binary to finish its shutdown. Its own two waits
    are bounded (quant_live::Deadlines), and the after-run checks follow.
#>
[CmdletBinding()]
param(
    [string] $Root,
    [int]    $GraceSeconds = 180
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'live-common.ps1')
if ([string]::IsNullOrWhiteSpace($Root)) {
    Write-Host 'usage: stop-live.ps1 -Root <the run''s root>'
    exit 2
}
if (-not (Test-Path $Root)) { Write-Host "no run at $Root"; exit 2 }
$Root = (Resolve-Path $Root).Path
$m = Read-RunJson -Root $Root
if ($null -eq $m -or $m.mode -ne 'live') {
    Write-Host "no live run.json at $Root"
    exit 2
}
# The root as start-live.ps1 spelt it, which is how its processes' command
# lines name it.
$runRoot = "$($m.root)"
if ([string]::IsNullOrWhiteSpace($runRoot)) { $runRoot = $Root }

function Stop-Verifier {
    foreach ($id in @(Stop-VerifyLoops -Root $runRoot)) { Write-Host "stopped the verify loop, pid $id" }
}

function Say-Outcome {
    param($Manifest)
    Write-Host ""
    Write-Host "ENDED  $("$($Manifest.outcome)".ToUpper()) (exit $($Manifest.exit_code))"
    Write-Host "       $($Manifest.outcome_meaning)"
    if ($Manifest.verdict) { Write-Host "       $($Manifest.verdict)" }
    if ($Manifest.checks) { Write-Host "       after-run checks $($Manifest.checks): $(Join-Path $Manifest.log_dir 'check-live.log')" }
}

function Test-SupervisorAlive {
    param($Manifest)
    if ("$($Manifest.supervisor_pid)" -notmatch '^\d+$') { return $true }
    return (Test-HarnessProcess -Id $Manifest.supervisor_pid -Script 'supervise-live.ps1' -Root $runRoot)
}

function Say-Lost {
    # The supervisor is gone and never wrote the ending: what the binary's own
    # log says instead, and what is left for a person to do.
    param($Manifest)
    Write-Host ""
    Write-Host "LOST   the supervisor, pid $($Manifest.supervisor_pid), is gone without recording how the run ended."
    $said = @()
    if ($Manifest.live_log) { $said = @(Read-SharedLines -Path "$($Manifest.live_log)") }
    $announced = Get-AnnouncedExit -Lines $said
    $verdict = @($said | Where-Object { $_ -match '^verdict' }) -join ' '
    $live = Get-RunLiveProcess $Manifest
    if ($null -ne $live) {
        Write-Host "       live pid $($live.Id) is still running, unsupervised; the stop file has been created and it will stop on it."
    } elseif ($null -ne $announced) {
        Write-Host "       the binary's log says it exited $announced. $verdict"
    } else {
        Write-Host "       the binary's log does not say it ended: it was killed too, or its log is gone."
    }
    Write-Host "       run.json still says '$($Manifest.supervisor)'. Run the after-run checks by hand:"
    Write-Host "         powershell -File ops\check-live.ps1 -Root `"$Root`""
}

if ("$($m.supervisor)" -eq 'finished' -or "$($m.supervisor)" -eq 'refused') {
    Write-Host "the live run at $Root is not running."
    if ("$($m.supervisor)" -eq 'finished') { Say-Outcome $m }
    Stop-Verifier
    exit 0
}

# The stop file, once the binary can hear it. Before its banner, the binary
# may not yet have read its arguments, and a stop file already there would be
# refused as one.
$stopFile = "$($m.stop_file)"
if ([string]::IsNullOrWhiteSpace($stopFile)) {
    Write-Host "run.json names no stop file: this run cannot be stopped cleanly from here."
    exit 1
}
$deadline = (Get-Date).AddSeconds($GraceSeconds)
$said = $false
$looked = (Get-Date).AddSeconds(-10)
$seenSupervisor = $false
$startedWaiting = Get-Date
while ((Get-Date) -lt $deadline) {
    $m = Read-RunJson -Root $Root
    $state = "$($m.supervisor)"
    if ($state -eq 'finished' -or $state -eq 'refused') { break }
    if ($state -eq 'running' -and (($null -ne $m.banner -and @($m.banner).Count -gt 0) -or $null -eq (Get-RunLiveProcess $m))) { break }
    if ($state -ne 'running' -and $state -ne 'starting') { break }
    if ($state -eq 'starting' -and ((Get-Date) - $looked).TotalSeconds -ge 2) {
        # A supervisor that never comes: start-live.ps1 launches it right
        # after writing run.json, so twenty seconds without one is an answer.
        $looked = Get-Date
        if (@(Find-HarnessProcesses -Script 'supervise-live.ps1' -Root $runRoot).Count -gt 0) { $seenSupervisor = $true }
        elseif (-not $seenSupervisor -and ((Get-Date) - $startedWaiting).TotalSeconds -ge 20) {
            Write-Host "run.json says the supervisor is starting, and no supervisor for this root is running: nothing to stop."
            exit 1
        }
    }
    if (-not $said) { Write-Host "waiting for the supervisor to start the binary before asking it to stop"; $said = $true }
    Start-Sleep -Milliseconds 250
}
if ("$($m.supervisor)" -eq 'refused') {
    Write-Host "the supervisor refused to start the binary: $($m.outcome_meaning)"
    Stop-Verifier
    exit 0
}

if (-not (Test-Path $stopFile)) {
    Set-Content -Path $stopFile -Value "stop asked for at $(Get-UtcStamp) by $env:USERNAME on $env:COMPUTERNAME" -Encoding utf8
}
Write-Host "asked the live run to stop: $stopFile"
$live = Get-RunLiveProcess $m
if ($null -ne $live) { Write-Host "waiting up to ${GraceSeconds}s for live pid $($live.Id) to finish its shutdown" }

$checked = Get-Date
while ((Get-Date) -lt $deadline) {
    $now = Read-RunJson -Root $Root
    if ($null -ne $now -and "$($now.supervisor)" -eq 'finished' -and $null -ne $now.checks) {
        Say-Outcome $now
        Stop-Verifier
        if ($now.outcome -eq 'stopped' -or $now.outcome -eq 'recorded-only') { exit 0 }
        exit 1
    }
    # Every few seconds, whether anyone is left to write that ending.
    if (((Get-Date) - $checked).TotalSeconds -ge 3 -and "$($now.supervisor)" -ne 'finished') {
        $checked = Get-Date
        if (-not (Test-SupervisorAlive $now)) {
            $now = Read-RunJson -Root $Root
            if ("$($now.supervisor)" -ne 'finished') {
                # Unsupervised, the binary still stops on the file: wait for
                # it, then say what it said.
                $orphan = Get-RunLiveProcess $now
                if ($null -ne $orphan) {
                    Write-Host "the supervisor is gone; waiting for live pid $($orphan.Id) itself"
                    [void]$orphan.WaitForExit([int][math]::Max(0, ($deadline - (Get-Date)).TotalMilliseconds))
                }
                Say-Lost $now
                Stop-Verifier
                exit 1
            }
        }
    }
    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "the run has not finished within ${GraceSeconds}s. The stop has been asked for, and a"
Write-Host "binary inside its shutdown is waiting on the venue within bounded deadlines: wait,"
Write-Host "and watch it with powershell -File ops\status.ps1 -Root `"$Root`"."
$live = Get-RunLiveProcess (Read-RunJson -Root $Root)
if ($null -ne $live) {
    Write-Host "Killing it would skip the cancels and the closing lines, leave orders working that"
    Write-Host "nobody watches, and make the next run record only. To do it anyway, knowing that:"
    Write-Host "  Stop-Process -Id $($live.Id) -Force"
} else {
    Write-Host "live.exe itself has exited; the supervisor is still recording how, or running the after-run checks."
}
exit 1
