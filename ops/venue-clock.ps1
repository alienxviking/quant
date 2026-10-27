<#
.SYNOPSIS
    How far this host's clock is from the venue's, read the way the live
    binary reads it. Dot-sourced by preflight.ps1 and fix-clock.ps1.

.DESCRIPTION
    Both used to measure it themselves, as now minus serverTime from one
    request. That folds the one-way latency to the venue into the offset: on
    a slow link enough to make a clock that is fine look behind, which in a
    preflight blocks a run over nothing (seen live from a hostel connection:
    -1445 ms naive against -450 ms corrected). The bash half corrected it at
    M1 with its own midpoint arithmetic; this half never did.

    So neither computes it any more. They ask venue-check --clock, which reads
    the venue's clock five times with the host's either side of each request,
    takes each reading to describe the midpoint, and believes the one with the
    smallest round trip: read_clock and ClockSample::best_of in
    quant-binance, the very functions the live binary's first gate uses. One
    implementation of the figure that decides whether this host may trade,
    rather than a second one in PowerShell to agree with it for ever.
    Unsigned, so it needs no credentials, which a recorder run does not have.

    Positive offset: this host is ahead of the venue, venue-check's sign.

    Update-VenueCheck builds venue-check before it is asked, for fix-clock.ps1,
    which is the first thing the 8c procedure runs: a binary left from an
    earlier build may not know --clock at all, and then answers with its
    usage, which reads as no clock.
#>

. (Join-Path $PSScriptRoot 'native-args.ps1')

function Update-VenueCheck {
    # cargo build of venue-check alone, redirected by cmd rather than by
    # PowerShell (the 5.1 traps): its exit code and where its output went.
    param([Parameter(Mandatory)] [string] $Repo)
    $env:Path = "$env:USERPROFILE\.cargo\bin;$env:Path"
    $log = Join-Path $env:TEMP "quant-venue-check-build-$PID.log"
    Push-Location $Repo
    try {
        cmd /c "cargo build --release -p quant-binance --bin venue-check > `"$log`" 2>&1"
        $code = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    return @{ code = $code; log = $log }
}

function Read-VenueClock {
    # A hashtable: ready (within a second), offset_ms and round_trip_ms when a
    # reading came back, detail to print, and the exit code. Base is a local
    # stand-in for the harness's tests; empty means the live venue.
    param(
        [Parameter(Mandatory)] [string] $Exe,
        [string] $Base,
        [switch] $Testnet
    )
    if (-not (Test-Path $Exe)) {
        return @{ ready = $false; code = -1; offset_ms = $null; round_trip_ms = $null
            detail = "venue-check is not built at $Exe" }
    }
    $arguments = @('--clock')
    if ($Testnet) { $arguments += '--testnet' }
    if (-not [string]::IsNullOrWhiteSpace($Base)) { $arguments += @('--rest', $Base) }
    $stem = Join-Path $env:TEMP ("quant-venue-clock-{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N').Substring(0, 8))
    $out = "$stem.txt"
    $err = "$stem.err"
    # Start-Process with its handle held, not -Wait and not a pipeline: the
    # native streams never reach PowerShell, which in 5.1 would wrap stderr
    # lines in ErrorRecords under ErrorActionPreference Stop.
    $quoted = ConvertTo-NativeArguments $arguments
    $process = Start-Process -FilePath $Exe -ArgumentList $quoted `
        -RedirectStandardOutput $out -RedirectStandardError $err -NoNewWindow -PassThru
    $null = $process.Handle
    $process.WaitForExit()
    $said = @(Get-Content -Path $out -ErrorAction SilentlyContinue) + @(Get-Content -Path $err -ErrorAction SilentlyContinue)
    Remove-Item -Path $out, $err -ErrorAction SilentlyContinue

    $offset = $null; $rtt = $null
    foreach ($line in $said) {
        if ($line -match '^clock\s+(-?\d+) ms from the venue') { $offset = [int64]$Matches[1] }
        if ($line -match 'round trip (\d+) ms') { $rtt = [int64]$Matches[1] }
    }
    $verdict = @($said | Where-Object { $_ -match '^verdict' }) -join ' '
    $detail = $verdict
    if ([string]::IsNullOrWhiteSpace($detail)) {
        # No verdict at all: an argument refused, which a build older than
        # --clock does, or a crash. Its exit code and first line say which.
        $first = @($said | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -First 1)
        $detail = "venue-check exited $($process.ExitCode) without a verdict"
        if ($first.Count -gt 0) { $detail += ": $($first[0])" }
    }
    if ($null -ne $offset) { $detail = "${offset}ms from the venue, round-trip corrected (rtt ${rtt}ms)" }
    return @{
        ready = ($process.ExitCode -eq 0)
        code = $process.ExitCode
        offset_ms = $offset
        round_trip_ms = $rtt
        detail = $detail
        verdict = $verdict
    }
}
