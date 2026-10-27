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

    Unlike stop-run.ps1 this does not stop the supervisor first. A live
    supervisor never restarts anything, so there is nothing to race; and it is
    the supervisor that records the outcome, runs the after-run checks and
    lets go of the keep-awake request and the one-live-run lock, so it is left
    to finish. The verify loop is ended once the run has: its last word is the
    supervisor's after-run checks.

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

function Stop-Verifier {
    # The verify loop only reads, and is asleep between passes, so ending it
    # costs nothing; a verify.exe it started finishes on its own.
    $pidFile = Join-Path $Root 'run.pids'
    if (-not (Test-Path $pidFile)) { return }
    $ids = @(Get-Content $pidFile | Where-Object { $_ -match '^\d+$' })
    foreach ($id in $ids) {
        if ("$id" -eq "$($m.supervisor_pid)") { continue }
        $p = Get-Process -Id $id -ErrorAction SilentlyContinue
        if ($null -ne $p -and $p.ProcessName -eq 'powershell') {
            Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
            Write-Host "stopped the verify loop, pid $id"
        }
    }
}

function Say-Outcome {
    param($Manifest)
    Write-Host ""
    Write-Host "ENDED  $("$($Manifest.outcome)".ToUpper()) (exit $($Manifest.exit_code))"
    Write-Host "       $($Manifest.outcome_meaning)"
    if ($Manifest.verdict) { Write-Host "       $($Manifest.verdict)" }
    if ($Manifest.checks) { Write-Host "       after-run checks $($Manifest.checks): $(Join-Path $Manifest.log_dir 'check-live.log')" }
}

if ("$($m.supervisor)" -eq 'finished' -or "$($m.supervisor)" -eq 'refused') {
    Write-Host "the live run at $Root is not running."
    if ("$($m.supervisor)" -eq 'finished') { Say-Outcome $m }
    Stop-Verifier
    exit 0
}

$live = $null
if ($m.live_pid) { $live = Get-Process -Id $m.live_pid -ErrorAction SilentlyContinue }

# The stop itself: the file the binary was told to watch.
$stopFile = "$($m.stop_file)"
if ([string]::IsNullOrWhiteSpace($stopFile)) {
    Write-Host "run.json names no stop file: this run cannot be stopped cleanly from here."
    exit 1
}
if (-not (Test-Path $stopFile)) {
    Set-Content -Path $stopFile -Value "stop asked for at $(Get-UtcStamp) by $env:USERNAME on $env:COMPUTERNAME" -Encoding utf8
}
Write-Host "asked the live run to stop: $stopFile"
if ($null -ne $live) { Write-Host "waiting up to ${GraceSeconds}s for live pid $($live.Id) to finish its shutdown" }

$deadline = (Get-Date).AddSeconds($GraceSeconds)
while ((Get-Date) -lt $deadline) {
    $now = Read-RunJson -Root $Root
    if ($null -ne $now -and "$($now.supervisor)" -eq 'finished' -and $null -ne $now.checks) {
        Say-Outcome $now
        Stop-Verifier
        if ($now.outcome -eq 'stopped' -or $now.outcome -eq 'recorded-only') { exit 0 }
        exit 1
    }
    Start-Sleep -Milliseconds 500
}

Write-Host ""
Write-Host "the run has not finished within ${GraceSeconds}s. The stop has been asked for, and a"
Write-Host "binary inside its shutdown is waiting on the venue within bounded deadlines: wait,"
Write-Host "and watch it with powershell -File ops\status.ps1 -Root `"$Root`"."
Write-Host "Killing it would skip the cancels and the closing lines, leave orders working that"
Write-Host "nobody watches, and make the next run record only. To do it anyway, knowing that:"
if ($null -ne $live) { Write-Host "  Stop-Process -Id $($live.Id) -Force" }
exit 1
