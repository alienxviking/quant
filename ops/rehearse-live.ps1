<#
.SYNOPSIS
    Rehearse a live run end to end, the real harness and the real binaries,
    against a local fake Binance and nothing else.

.DESCRIPTION
    M1's lesson, for the live mode: a harness that has never been run is not
    a harness. This runs it the way an operator would, against
    quant-backtest's fake-binance example, which serves the REST side, the
    WebSocket API and the market stream on loopback ports and runs this
    script with their addresses and a key pair it generated in process in
    the environment. Nothing here can reach Binance: it refuses to start
    unless all three addresses are set, and the live binary and venue-check
    take an override only on a loopback host.

      cargo build --release -p quant-backtest --example fake-binance
      target\release\examples\fake-binance.exe -- powershell -NoProfile -ExecutionPolicy Bypass -File ops\rehearse-live.ps1

    In order, each step the operator's own:
      1. start-live.ps1, whose preflight builds the release binaries and runs
         venue-check against the fake, signed with the fake's key;
      2. a round trip: waits until the journal holds two fills;
      3. a pass of the verify loop, while the run is going;
      4. status.ps1, as the operator watches it;
      5. stop-live.ps1, the operator's stop;
      6. the after-run checks the supervisor ran: verify on both capture
         trees, reconcile, runlog check and replay-account.
    Exit 0 when the run stopped cleanly, traded a round trip, and every check
    was clean; 1 otherwise.

    A short crossover (2 over 4 on one-second mids) so a round trip comes in
    seconds on the fake's walking market. Not the run's settings: those are
    the binary's defaults, M5's, which need half an hour to cross.

.PARAMETER Root
    Where the rehearsal writes; a fresh directory under the system's temp
    directory unless given. It must not exist yet.
#>
[CmdletBinding()]
param(
    [string] $Root,
    [int]    $Minutes = 10
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'live-common.ps1')

foreach ($var in @('QUANT_FAKE_REST', 'QUANT_FAKE_WS_API', 'QUANT_FAKE_MARKET_WS')) {
    if ([string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($var))) {
        Write-Host "not rehearsing: $var is not set. Run this under the fake, which sets all three:"
        Write-Host '  target\release\examples\fake-binance.exe -- powershell -NoProfile -ExecutionPolicy Bypass -File ops\rehearse-live.ps1'
        exit 1
    }
}
if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = Join-Path ([System.IO.Path]::GetTempPath()) ("quant-rehearse-live-{0}" -f (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss'))
}
$Root = [System.IO.Path]::GetFullPath($Root)
if (Test-Path $Root) {
    Write-Host "not rehearsing: $Root already exists; a rehearsal starts from an empty root"
    exit 1
}

function Step {
    param([string] $Text)
    Write-Host ""
    Write-Host "=== $(Get-UtcStamp) $Text"
}

function Until {
    param([string] $What, [scriptblock] $Holds, [int] $Seconds)
    $deadline = (Get-Date).AddSeconds($Seconds)
    while (-not (& $Holds)) {
        if ((Get-Date) -gt $deadline) { return $false }
        Start-Sleep -Milliseconds 250
    }
    return $true
}

$failures = @()
$journal = Join-Path $Root 'live-BTCUSDT.jsonl'

Step "1. start-live.ps1 against the fake: $env:QUANT_FAKE_REST"
& (Join-Path $here 'start-live.ps1') -Root $Root -Minutes $Minutes -Fast 2 -Slow 4 -IntervalSecs 1 `
    -Rest $env:QUANT_FAKE_REST -WsApi $env:QUANT_FAKE_WS_API -MarketWs $env:QUANT_FAKE_MARKET_WS `
    -VerifyEveryMinutes 1 -FirstCheckSeconds 15
if ($LASTEXITCODE -ne 0) {
    Write-Host "start-live.ps1 exited ${LASTEXITCODE}: the rehearsal cannot go on"
    exit 1
}

Step '2. a round trip: two fills in the journal'
$traded = Until 'two fills' {
    (Test-Path $journal) -and @(Read-SharedLines -Path $journal | Where-Object { $_.StartsWith('{"type":"filled"') }).Count -ge 2
} 180
$fills = @(Read-SharedLines -Path $journal | Where-Object { $_.StartsWith('{"type":"filled"') })
foreach ($line in $fills | Select-Object -First 2) { Write-Host "  $($line.Substring(0, [math]::Min(140, $line.Length)))" }
if (-not $traded) { $failures += 'no round trip within three minutes' }

Step '3. the verify loop, while the run goes on'
$verifyLog = Join-Path $Root 'logs\verify.log'
$watched = Until 'a verify pass and a journal pass' {
    $lines = @(Read-SharedLines -Path $verifyLog)
    (@($lines | Where-Object { $_ -match ' OK   every| OK   verdict' }).Count -ge 1) -and
        (@($lines | Where-Object { $_ -match 'OK   reconcile' }).Count -ge 1)
} 150
Read-SharedLines -Path $verifyLog | ForEach-Object { Write-Host "  $_" }
if (-not $watched) { $failures += 'the verify loop did not pass verify and reconcile while the run went on' }

Step '4. status.ps1, while it runs'
& (Join-Path $here 'status.ps1') -Root $Root

Step '5. stop-live.ps1, the operator''s stop'
& (Join-Path $here 'stop-live.ps1') -Root $Root
$stopExit = $LASTEXITCODE
if ($stopExit -ne 0) { $failures += "stop-live.ps1 exited $stopExit" }

Step '6. how it ended, and the after-run checks'
& (Join-Path $here 'status.ps1') -Root $Root
$m = Read-RunJson -Root $Root
if ($m.outcome -ne 'stopped') { $failures += "the outcome was $($m.outcome), not stopped" }
if ($m.checks -ne 'clean') { $failures += "the after-run checks were $($m.checks)" }
Write-Host ""
Read-SharedLines -Path (Join-Path $Root 'logs\check-live.log') | ForEach-Object { Write-Host "  $_" }

Write-Host ""
if ($failures.Count -gt 0) {
    Write-Host "REHEARSAL FAILED:"
    $failures | ForEach-Object { Write-Host "  - $_" }
    Write-Host "everything it wrote is in $Root"
    exit 1
}
$all = @(Read-SharedLines -Path $journal | Where-Object { $_.StartsWith('{"type":"filled"') }).Count
Write-Host "REHEARSAL PASSED: started through the preflight, traded a round trip ($all fill(s) in all), watched, stopped cleanly by its stop file, and every after-run check clean"
Write-Host "everything it wrote is in $Root"
exit 0
