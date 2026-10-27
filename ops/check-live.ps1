<#
.SYNOPSIS
    The after-run checks on a live run: both captures verified, the journal
    reconciled, checked, and replayed against the account capture.

.DESCRIPTION
    Item 8's acceptance in docs/live-run.md section 4 names four checks, and
    each is a binary that already exists and reports through its exit code.
    This runs the four on what a live run left, says what each code means in
    words, and exits 1 if any of them found a real problem:

      verify ROOT              both capture trees, raw\ (the market) and
                               account\ (the account stream), in one pass:
                               verify walks both. 0 clean, 1 a finding, and
                               3 NOTHING VERIFIED, no market frame at all.
                               3 is a failure for a run that traded, which
                               must have captured the market it traded on,
                               and expected only of a run that never started.
      reconcile JOURNAL        the engine's checkpoints against a recompute
                               from the file: 0 agrees, 1 does not, 2 no
                               checkpoint to check.
      runlog check JOURNAL     whether the journal accounts for every
                               decision it implies: 0 complete, 1 not, 2
                               nothing to check.
      replay-account JOURNAL ROOT
                               the recorded account frames, replayed through
                               the translator, reproduce every line the venue
                               produced: 0 agrees, 1 does not, 2 nothing
                               compared.

    A run that recorded only, or refused its arguments, journals nothing: it
    leaves no journal, or an empty one, since the binary opens the file
    before its gates decide. So the three journal checks have nothing to read;
    that is said, and is not a failure. A run whose journal holds a line is
    held to all four.

    The supervisor runs this when the binary has ended. Run it by hand to see
    the same verdicts again:

      powershell -File ops\check-live.ps1 -Root data\testnet

    It writes its log beside the run's other logs and nothing else: run.json
    has one writer, the supervisor, which records this script's verdict.

.PARAMETER Root
    The run's root, holding its run.json.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Root
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'live-common.ps1')
$repo = (Resolve-Path (Join-Path $here '..')).Path
$bin  = Join-Path $repo 'target\release'

$Root = (Resolve-Path $Root).Path
$manifest = Read-RunJson -Root $Root
if ($null -eq $manifest -or $manifest.mode -ne 'live') {
    Write-Host "no live run.json at $Root"
    exit 2
}
$logDir = $manifest.log_dir
$journal = $manifest.journal
$log = Join-Path $logDir 'check-live.log'
$stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')

function Note {
    param([string] $Message)
    $line = "{0} {1}" -f (Get-UtcStamp), $Message
    Write-Host $line
    # A shared append (shared-log.ps1): the status view reads this log.
    if (-not (Add-SharedLine -Path $log -Line $line)) { Write-Host "  (the log could not take that line)" }
}

function Invoke-Check {
    # One checker, its output kept in a file of its own, and its exit code.
    # Start-Process with the handle held rather than -Wait, and the native
    # streams redirected by Start-Process rather than by PowerShell, which in
    # 5.1 would wrap each stderr line in an ErrorRecord.
    param([string] $Name, [string] $Exe, [string[]] $Arguments)
    $out = Join-Path $logDir "check-$Name-$stamp.txt"
    $err = Join-Path $logDir "check-$Name-$stamp.err"
    if (-not (Test-Path $Exe)) {
        return @{ code = -1; out = $out; missing = $true }
    }
    # Quoted by the one rule (native-args.ps1), so a root with a space in it
    # or ending in a backslash arrives as one argument.
    $quoted = ConvertTo-NativeArguments $Arguments
    $process = Start-Process -FilePath $Exe -ArgumentList $quoted `
        -RedirectStandardOutput $out -RedirectStandardError $err -NoNewWindow -PassThru
    $null = $process.Handle
    $process.WaitForExit()
    return @{ code = $process.ExitCode; out = $out; missing = $false }
}

function Verdict-Of {
    param([string] $Path)
    $lines = @(Get-Content -Path $Path -ErrorAction SilentlyContinue | Where-Object { $_ -match '^verdict' })
    return ($lines -join ' ')
}

$failures = 0
# Written, not merely there: a run that recorded only leaves the empty file the
# binary opened before its gates refused.
$journalled = (-not [string]::IsNullOrWhiteSpace($journal)) -and (Test-Path $journal) -and ((Get-Item $journal).Length -gt 0)
Note "checking the live run at $Root"

# --- both captures -----------------------------------------------------------
$v = Invoke-Check 'verify' (Join-Path $bin 'verify.exe') @($Root)
$sessions = @(Get-Content -Path $v.out -ErrorAction SilentlyContinue | Where-Object { $_ -match '^sessions' }) -join ''
if ($v.missing) {
    Note 'FAIL verify: not built -- run ops\preflight-live.ps1'
    $failures++
} else {
    switch ($v.code) {
        0 { Note "OK   verify: $(Verdict-Of $v.out)  [$sessions]" }
        3 {
            if ($journalled) {
                Note "FAIL verify: NOTHING VERIFIED, no market frame under the root, from a run that traded"
                $failures++
            } else {
                Note 'NONE verify: nothing verified, no market frame under the root (the run never started)'
            }
        }
        default {
            Note "FAIL verify exit $($v.code): $(Verdict-Of $v.out)  [$sessions]"
            Get-Content -Path $v.out -ErrorAction SilentlyContinue |
                Where-Object { $_ -match '^  (ERROR|warn|\.\.\.)' } |
                ForEach-Object { Note "     $_" }
            $failures++
        }
    }
}

# --- the journal -------------------------------------------------------------
if (-not $journalled) {
    Note "NONE journal: nothing is written at $journal -- a run that recorded only, or never started, journals nothing"
} else {
    $checks = @(
        @{ name = 'reconcile'; exe = 'reconcile.exe'; args = @($journal)
           ok = 'the checkpoints agree with a recompute'; none = 'no checkpoint to check' },
        @{ name = 'runlog'; exe = 'runlog.exe'; args = @('check', $journal)
           ok = 'the journal accounts for every decision'; none = 'nothing to check' },
        @{ name = 'replay-account'; exe = 'replay-account.exe'; args = @($journal, $Root, '--symbol', $manifest.symbol)
           ok = 'the account capture reproduces every venue line'; none = 'nothing compared' }
    )
    foreach ($check in $checks) {
        $r = Invoke-Check $check.name (Join-Path $bin $check.exe) $check.args
        if ($r.missing) {
            Note "FAIL $($check.name): not built -- run ops\preflight-live.ps1"
            $failures++
            continue
        }
        switch ($r.code) {
            0 { Note "OK   $($check.name): $($check.ok). $(Verdict-Of $r.out)" }
            2 { Note "NONE $($check.name): $($check.none). $(Verdict-Of $r.out)" }
            default {
                Note "FAIL $($check.name) exit $($r.code): $(Verdict-Of $r.out)"
                $failures++
            }
        }
    }
}

if ($failures -gt 0) {
    Note "after-run checks FAILED: $failures check(s) found a problem; each check's output is in $logDir"
    exit 1
}
Note 'after-run checks clean'
exit 0
