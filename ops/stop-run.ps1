<#
.SYNOPSIS
    Stop the acceptance run cleanly.

.DESCRIPTION
    Cleanly matters here, and it is the whole reason the recorder distinguishes
    the two shutdowns. Ctrl-C -- which `CloseMainWindow` and, failing that, a
    console break stand in for -- makes the connection future drop, which closes
    the capture channel, which makes the writer seal the file with a trailer. A
    file with a trailer says "I am complete and I hold exactly this much"; a file
    without one says "my recorder died". Killing the process outright throws that
    distinction away and leaves every segment looking like a crash.

    So the supervisors are stopped first -- otherwise they would helpfully restart
    the recorder we are trying to stop -- and then each recorder is asked to
    finish rather than told to stop.
#>
[CmdletBinding()]
param(
    [string] $Root,
    [int]    $GraceSeconds = 30
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($Root)) { $Root = Join-Path (Join-Path $here '..') 'data\acceptance' }
$Root = (Resolve-Path $Root).Path

# A live run is stopped by stop-live.ps1, and must not reach the code below:
# it would kill the live supervisor, which records how the run ended and holds
# the host awake, then look for a recorder, find none, and report success
# while live.exe went on trading. A recorder run's run.json has no mode.
$probe = Join-Path $Root 'run.json'
if ((Test-Path $probe) -and ((Get-Content $probe -Raw | ConvertFrom-Json).mode -eq 'live')) {
    Write-Host 'a live run: stopping it with ops\stop-live.ps1'
    & (Join-Path $here 'stop-live.ps1') -Root $Root
    exit $LASTEXITCODE
}

# Supervisors first. Stopping a recorder while its supervisor is watching would
# simply produce another recorder.
$pidFile = Join-Path $Root 'run.pids'
if (Test-Path $pidFile) {
    foreach ($id in (Get-Content $pidFile | Where-Object { $_ -match '^\d+$' })) {
        $p = Get-Process -Id $id -ErrorAction SilentlyContinue
        if ($null -ne $p) {
            Write-Host "stopping supervisor pid $id"
            Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
        }
    }
} else {
    Write-Host "no run.pids at $Root; stopping any supervising powershell by hand may be needed"
}

$recorders = @(Get-Process record -ErrorAction SilentlyContinue)
if ($recorders.Count -eq 0) {
    Write-Host "no recorder running"
    exit 0
}

foreach ($p in $recorders) {
    Write-Host "asking record pid $($p.Id) to finish"
    # Not Stop-Process: that is a kill, and a killed recorder leaves its last
    # segment without a trailer, which is indistinguishable from a crash forever
    # afterwards.
    $null = $p.CloseMainWindow()
}

$deadline = (Get-Date).AddSeconds($GraceSeconds)
while ((Get-Date) -lt $deadline) {
    if (@(Get-Process record -ErrorAction SilentlyContinue).Count -eq 0) {
        Write-Host "all recorders finished cleanly"
        exit 0
    }
    Start-Sleep -Milliseconds 500
}

$left = @(Get-Process record -ErrorAction SilentlyContinue)
Write-Host ""
Write-Host "$($left.Count) recorder(s) did not finish within ${GraceSeconds}s."
Write-Host "Killing them now would leave their last segment without a trailer -- readable,"
Write-Host "but permanently indistinguishable from a crash. To do it anyway:"
foreach ($p in $left) { Write-Host "  Stop-Process -Id $($p.Id) -Force" }
exit 1
