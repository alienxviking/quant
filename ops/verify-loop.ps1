<#
.SYNOPSIS
    Verify the capture periodically while it is still being written.

.DESCRIPTION
    The whole reason `quant-verify` reports through an exit code rather than
    prose is so it can run unattended, during the capture rather than after it.
    Finding on day seven that the recorder stopped anchoring its book on day two
    means six wasted days; finding it six hours in means fixing it and starting
    again.

    A torn tail and a missing trailer on the newest segment are expected here --
    those files are still open -- which is exactly why the verifier reports them
    as warnings and only errors fail the run. If that distinction were not there,
    this loop would cry wolf every six hours and be ignored by day two.

    Exit codes from the verifier, preserved in the log:
      0  every discontinuity is explained
      1  something is not
      2  --reconcile could not run (a database problem, not a capture problem)
      3  nothing verified: no market frame under the root (logged as FAIL)

.PARAMETER Root
    Capture root to verify.

.PARAMETER EndAt
    UTC time to stop at.

.PARAMETER IntervalHours
    How often to check.

.PARAMETER LogDir
    Where the verification log goes.

.PARAMETER Journal
    A live run's journal (start-live.ps1 passes it). Each pass then also runs
    reconcile and runlog check on it, which is how a live run's record is
    watched while it is written: a journal that has stopped agreeing with
    itself should cost minutes, not the afternoon. Without it, nothing about
    the loop changes: a recorder run has no journal to read.

      reconcile     0 agrees, 2 no checkpoint yet (WAIT, not an alarm), 1 not
      runlog check  0 complete, 2 nothing to check yet (WAIT), 1 not

    A torn last line is normal for a journal being written, and both read it
    as the tail of an open file rather than as damage. The account replay
    needs the account capture sealed, so it runs once, after the run, in
    check-live.ps1.

.PARAMETER IntervalSeconds
    How often to check, in seconds, in place of IntervalHours: a live run is
    an afternoon, not a week, and its rehearsal is minutes. Zero, the
    default, leaves IntervalHours in charge.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]   $Root,
    [Parameter(Mandatory)] [datetime] $EndAt,
    [Parameter(Mandatory)] [string]   $LogDir,
    [int] $IntervalHours = 6,
    # How long to wait before the first check. A misconfiguration shows up in the
    # first few minutes, and that is the failure most worth catching early.
    [int] $FirstCheckSeconds = 300,
    [string] $Journal,
    [int] $IntervalSeconds = 0
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Resolve-Path (Join-Path $here '..')
$exe  = Join-Path $repo 'target\release\verify.exe'
$log  = Join-Path $LogDir 'verify.log'

if (-not (Test-Path $exe)) {
    Write-Error "no release binary at $exe -- run ops\preflight.ps1 first"
    exit 1
}
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }

function Note {
    param([string] $Message)
    $line = "{0} {1}" -f (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ'), $Message
    Write-Host $line
    Add-Content -Path $log -Value $line -Encoding utf8
}

# Only reconcile if a database was configured. Without one the flag exits 2 every
# time, which would train whoever reads this log to ignore a non-zero code -- the
# same reason warnings do not fail a verification run.
$useReconcile = -not [string]::IsNullOrWhiteSpace($env:QUANT_DATABASE_URL)
$intervalSecondsNow = $IntervalHours * 3600
$cadence = "${IntervalHours}h"
if ($IntervalSeconds -gt 0) { $intervalSecondsNow = $IntervalSeconds; $cadence = "${IntervalSeconds}s" }
$watchJournal = -not [string]::IsNullOrWhiteSpace($Journal)
$journalNote = ''
if ($watchJournal) { $journalNote = ", journal $Journal" }
Note "verifying $Root every $cadence until $($EndAt.ToUniversalTime().ToString('u')), reconcile=$useReconcile$journalNote"

# A live run's journal, read the two ways that can be read while it is still
# being written. Each binary's output kept in a file of its own, as verify's is.
function Watch-Journal {
    param([string] $Stamp)
    if (-not (Test-Path $Journal)) {
        Note "WAIT $([System.IO.Path]::GetFileName($Journal)): not written yet"
        return
    }
    $checks = @(
        @{ name = 'reconcile'; exe = (Join-Path $repo 'target\release\reconcile.exe'); arguments = @($Journal) },
        @{ name = 'runlog'; exe = (Join-Path $repo 'target\release\runlog.exe'); arguments = @('check', $Journal) }
    )
    foreach ($check in $checks) {
        $jout = Join-Path $LogDir "$($check.name)-$Stamp.txt"
        $jerr = Join-Path $LogDir "$($check.name)-$Stamp.err"
        if (-not (Test-Path $check.exe)) {
            Note "FAIL $($check.name): no release binary at $($check.exe)"
            continue
        }
        $quoted = @()
        foreach ($argument in $check.arguments) { $quoted += "`"$argument`"" }
        $p = Start-Process -FilePath $check.exe -ArgumentList $quoted `
            -RedirectStandardOutput $jout -RedirectStandardError $jerr `
            -NoNewWindow -Wait -PassThru
        $said = (Get-Content $jout -ErrorAction SilentlyContinue | Select-String '^verdict') -join ''
        switch ($p.ExitCode) {
            0 { Note "OK   $($check.name): $said" }
            2 { Note "WAIT $($check.name): nothing to check yet" }
            default { Note "FAIL $($check.name) exit $($p.ExitCode): $said" }
        }
    }
}

# A first pass shortly after the start, because the failure worth catching early
# is a misconfiguration, and that shows up in the first few minutes.
Start-Sleep -Seconds $FirstCheckSeconds

while ((Get-Date).ToUniversalTime() -lt $EndAt.ToUniversalTime()) {
    $stamp  = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')
    $out    = Join-Path $LogDir "verify-$stamp.txt"
    $errOut = Join-Path $LogDir "verify-$stamp.err"

    $arguments = @($Root)
    if ($useReconcile) { $arguments += '--reconcile' }

    $process = Start-Process -FilePath $exe -ArgumentList $arguments `
        -RedirectStandardOutput $out -RedirectStandardError $errOut `
        -NoNewWindow -Wait -PassThru

    $verdict = (Get-Content $out -ErrorAction SilentlyContinue | Select-String '^verdict') -join ''
    switch ($process.ExitCode) {
        0 { Note "OK   $verdict" }
        2 { Note "SKIP reconcile could not run: $(Get-Content $errOut -ErrorAction SilentlyContinue | Select-Object -First 1)" }
        default {
            Note "FAIL exit $($process.ExitCode)  $verdict"
            # Copied into the running log so the findings are in one place rather
            # than in a file somebody has to go and find at 3am.
            Get-Content $out -ErrorAction SilentlyContinue |
                Select-String '^  (ERROR|warn|\.\.\.)' |
                ForEach-Object { Note "     $_" }
        }
    }

    if ($watchJournal) {
        Watch-Journal $stamp
    }

    $remaining = ($EndAt.ToUniversalTime() - (Get-Date).ToUniversalTime()).TotalSeconds
    if ($remaining -le 60) { break }
    Start-Sleep -Seconds ([math]::Min($intervalSecondsNow, $remaining))
}

Note "verification loop finished"
