<#
.SYNOPSIS
    Exercise the live harness against a fake live.exe: every ending reported
    in words and never restarted, a second run refused, the stop, the
    keep-awake request, and the preflight's refusals.

.DESCRIPTION
    test-supervise.sh's argument, for the PowerShell half: the decisions a
    harness makes only ever run inside the run they are supervising, where
    getting one wrong costs that run. A fake child and a throwaway tree make
    each decision in seconds.

    The fake is a real Windows executable, compiled here from a few lines of
    C# by Add-Type, and installed as target\release\live.exe in a throwaway
    copy of the repository's ops\ and target\release\, because that is where
    supervise-live.ps1 looks for its binary. It is named live.exe so that the
    harness's one-live-run check sees it as it would see the real one. It
    prints a banner, watches the --stop-file it is given, and otherwise runs
    for FAKE_LIVE_SECONDS and exits FAKE_LIVE_EXIT. The real verify, reconcile,
    runlog, replay-account and venue-check are copied beside it, so the
    after-run checks run for real.

    The one-live-run lock is the host's, so this refuses to run while a live
    run holds it, and two copies of it cannot run at once: that is the rule
    under test, not a limitation of the test. The clock checks, which can,
    are in test-venue-clock.ps1, which this runs last.

    Placeholder values stand in for the key variables, for the preflight's
    presence check: text that is not a key, checked never to be printed.

    Exit 0 when every check passes, 1 when one fails, 2 when it cannot run.

.EXAMPLE
    powershell -NoProfile -ExecutionPolicy Bypass -File ops\test-live.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = (Resolve-Path (Join-Path $here '..')).Path
. (Join-Path $here 'live-common.ps1')

$real = Join-Path $repo 'target\release'
$needed = @('verify', 'reconcile', 'runlog', 'replay-account', 'venue-check')
$absent = @($needed | Where-Object { -not (Test-Path (Join-Path $real "$_.exe")) })
if ($absent.Count -gt 0) {
    Write-Host "cannot run: not built in target\release: $($absent -join ', ') (cargo build --release --workspace --bins)"
    exit 2
}
if ((Test-LiveLockHeld) -or (Get-LiveProcesses).Count -gt 0) {
    Write-Host 'cannot run: a live run is on this host, and these tests take its lock'
    exit 2
}

$script:pass = 0
$script:fail = 0
function Check {
    param([string] $What, $Want, $Got)
    if ("$Want" -eq "$Got") {
        Write-Host "  ok   $What"; $script:pass++
    } else {
        Write-Host "  FAIL $What -- wanted $Want, got $Got"; $script:fail++
    }
}

# --- the throwaway tree -------------------------------------------------------
# Under the system's temp directory, named for this process: never a path that
# an empty variable could turn into somewhere else before it is deleted.
$temp = [System.IO.Path]::GetTempPath()
if ([string]::IsNullOrWhiteSpace($temp) -or -not [System.IO.Path]::IsPathRooted($temp)) {
    Write-Host 'cannot run: no temp directory'
    exit 2
}
$stage = Join-Path $temp "quant-test-live-$PID"
if (Test-Path $stage) { Remove-Item -Recurse -Force -LiteralPath $stage }
New-Item -ItemType Directory -Path (Join-Path $stage 'ops') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $stage 'target\release') -Force | Out-Null
Copy-Item -Path (Join-Path $here '*.ps1') -Destination (Join-Path $stage 'ops')
foreach ($name in $needed) {
    Copy-Item -Path (Join-Path $real "$name.exe") -Destination (Join-Path $stage 'target\release')
}
$fake = @'
using System;
using System.IO;
using System.Threading;
public static class FakeLive {
    public static int Main(string[] args) {
        string stop = null;
        for (int i = 0; i + 1 < args.Length; i++) { if (args[i] == "--stop-file") { stop = args[i + 1]; } }
        Console.WriteLine("venue     a fake live.exe, for the harness tests");
        Console.WriteLine("args      " + string.Join(" ", args));
        Console.WriteLine();
        int seconds = Int32.Parse(Environment.GetEnvironmentVariable("FAKE_LIVE_SECONDS") ?? "1");
        int code = Int32.Parse(Environment.GetEnvironmentVariable("FAKE_LIVE_EXIT") ?? "0");
        DateTime until = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < until) {
            if (stop != null && File.Exists(stop)) {
                Console.WriteLine("verdict   STOPPED by the stop file");
                return 0;
            }
            Thread.Sleep(50);
        }
        Console.WriteLine("verdict   the fake exits " + code);
        return code;
    }
}
'@
Add-Type -TypeDefinition $fake -OutputAssembly (Join-Path $stage 'target\release\live.exe') -OutputType ConsoleApplication
$sops = Join-Path $stage 'ops'
# A repository of its own, so run.json's commit is read from a real git.
$ErrorActionPreference = 'Continue'
git -C $stage init -q 2>$null | Out-Null
git -C $stage -c user.name=harness -c user.email=harness@localhost commit -q --allow-empty -m 'the staged harness' 2>$null | Out-Null
$ErrorActionPreference = 'Stop'

$env:BINANCE_API_KEY = 'placeholder-api-key-not-a-key'
$env:BINANCE_ED25519_PRIVATE_KEY = 'placeholder-private-key-not-a-key'

$script:roots = 0
function New-Root {
    $script:roots++
    $root = Join-Path $stage "run-$script:roots"
    New-Item -ItemType Directory -Path (Join-Path $root 'logs') -Force | Out-Null
    return $root
}

function Write-Manifest {
    # What start-live.ps1 writes, as little of it as the supervisor reads.
    param([string] $Root)
    $stop = Join-Path $Root 'live-test.stop'
    Write-RunJson -Root $Root -Manifest ([ordered]@{
        mode = 'live'; environment = 'testnet'; real_money = $false; stand_in = $true
        symbol = 'BTCUSDT'; root = $Root; journal = (Join-Path $Root 'live-BTCUSDT.jsonl')
        stop_file = $stop; log_dir = (Join-Path $Root 'logs')
        live_args = @('--symbol', 'BTCUSDT', '--root', $Root, '--stop-file', $stop)
        supervisor = 'starting'
    })
}

function Start-Hidden {
    param([string] $Script, [string[]] $Arguments)
    $quoted = @("`"$(Join-Path $sops $Script)`"")
    foreach ($a in $Arguments) { $quoted += "`"$a`"" }
    $p = Start-Process -FilePath 'powershell.exe' -ArgumentList (@('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File') + $quoted) -WindowStyle Hidden -PassThru
    $null = $p.Handle
    return $p
}

function Run-Script {
    # A script of the staged harness, to its end: its exit code, and what it said.
    param([string] $Script, [string[]] $Arguments)
    $out = Join-Path $stage ("said-{0}.txt" -f [guid]::NewGuid().ToString('N').Substring(0, 8))
    $quoted = @("`"$(Join-Path $sops $Script)`"")
    foreach ($a in $Arguments) { $quoted += "`"$a`"" }
    $p = Start-Process -FilePath 'powershell.exe' -ArgumentList (@('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File') + $quoted) `
        -RedirectStandardOutput $out -RedirectStandardError "$out.err" -NoNewWindow -PassThru
    $null = $p.Handle
    $p.WaitForExit()
    $said = ((Get-Content $out -ErrorAction SilentlyContinue) + (Get-Content "$out.err" -ErrorAction SilentlyContinue)) -join "`n"
    return @{ code = $p.ExitCode; said = $said }
}

function Until {
    param([string] $What, [scriptblock] $Holds, [int] $Seconds = 60)
    $deadline = (Get-Date).AddSeconds($Seconds)
    while (-not (& $Holds)) {
        if ((Get-Date) -gt $deadline) { throw "gave up waiting for $What after ${Seconds}s" }
        Start-Sleep -Milliseconds 100
    }
}

function Attempts {
    param([string] $Root)
    $log = Join-Path $Root 'logs\supervisor-live.log'
    if (-not (Test-Path $log)) { return 0 }
    return @(Select-String -Path $log -Pattern 'live pid \d+ started').Count
}

function Supervisor-Log {
    param([string] $Root)
    return ((Get-Content (Join-Path $Root 'logs\supervisor-live.log') -ErrorAction SilentlyContinue) -join "`n")
}

function Fake {
    param([int] $Seconds, [int] $Exit)
    $env:FAKE_LIVE_SECONDS = "$Seconds"
    $env:FAKE_LIVE_EXIT = "$Exit"
}

function Clear-Stage {
    # Whatever this run of the tests started and left running, a failed check
    # or a thrown wait included: only powershell.exe and live.exe, and only
    # those whose command line names this run's throwaway tree. A leftover
    # supervisor would hold the one-live-run lock for the next run.
    Get-CimInstance Win32_Process -Filter "Name='powershell.exe' or Name='live.exe'" |
        Where-Object { $_.ProcessId -ne $PID -and "$($_.CommandLine)".Contains($stage) } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}

try {
# --- 1. every ending, in words, and one attempt -------------------------------
Write-Host 'each way the binary ends is reported in words and never restarted'
foreach ($case in @(
        @{ exit = 0; outcome = 'stopped'; supervisor = 0 },
        @{ exit = 1; outcome = 'failed'; supervisor = 1 },
        @{ exit = 2; outcome = 'bad-argument'; supervisor = 2 },
        @{ exit = 3; outcome = 'recorded-only'; supervisor = 3 },
        @{ exit = 7; outcome = 'killed'; supervisor = 4 })) {
    $root = New-Root
    Write-Manifest $root
    Fake 1 $case.exit
    $r = Run-Script 'supervise-live.ps1' @('-Root', $root)
    $m = Read-RunJson $root
    Check "exit $($case.exit): the supervisor exits $($case.supervisor)" $case.supervisor $r.code
    Check "exit $($case.exit): run.json says $($case.outcome)" $case.outcome $m.outcome
    Check "exit $($case.exit): run.json keeps the code" $case.exit $m.exit_code
    Check "exit $($case.exit): the meaning is written out" $true ($m.outcome_meaning.Length -gt 30)
    Check "exit $($case.exit): one attempt, no restart" 1 (Attempts $root)
    Check "exit $($case.exit): the log says it will not restart" $true ((Supervisor-Log $root) -match 'not restarting')
    Check "exit $($case.exit): the log names the outcome" $true ((Supervisor-Log $root) -match $case.outcome.ToUpper())
    Check "exit $($case.exit): what the binary said it used is in run.json" $true ("$($m.banner)" -match 'a fake live.exe')
}
# The meanings are distinct: four codes, four sentences.
$meanings = @(0, 1, 2, 3, 9 | ForEach-Object { (Get-LiveOutcome $_).meaning } | Sort-Object -Unique)
Check 'the five endings have five different meanings' 5 $meanings.Count

# --- 2. a killed binary ---------------------------------------------------------
Write-Host 'a binary that is killed is reported as killed, and not restarted'
$root = New-Root
Write-Manifest $root
Fake 60 0
$sup = Start-Hidden 'supervise-live.ps1' @('-Root', $root)
Until 'the binary to start' { $m = Read-RunJson $root; $null -ne $m -and $m.supervisor -eq 'running' }
$m = Read-RunJson $root
Stop-Process -Id $m.live_pid -Force
$sup.WaitForExit()
$m = Read-RunJson $root
Check 'killed: the supervisor exits 4' 4 $sup.ExitCode
Check 'killed: run.json says killed' 'killed' $m.outcome
Check 'killed: the meaning says the journal has no closing lines' $true ($m.outcome_meaning -match 'no closing lines')
Check 'killed: one attempt, no restart' 1 (Attempts $root)

# --- 3. one live run per host, and the stop --------------------------------------
Write-Host 'a second run is refused while one runs, and the first stops cleanly on its stop file'
$first = New-Root
Write-Manifest $first
Fake 90 0
$sup = Start-Hidden 'supervise-live.ps1' @('-Root', $first)
Until 'the first run to start' { $m = Read-RunJson $first; $null -ne $m -and $m.supervisor -eq 'running' }

$second = New-Root
Write-Manifest $second
$r = Run-Script 'supervise-live.ps1' @('-Root', $second)
$m2 = Read-RunJson $second
Check 'a second supervisor refuses, exit 5' 5 $r.code
Check 'and records refused' 'refused' $m2.outcome
Check 'and never starts the binary' 0 (Attempts $second)
Check 'and says why: the lock' $true ($m2.outcome_meaning -match 'one-live-run lock')

$third = New-Root
$r = Run-Script 'start-live.ps1' @('-Root', $third, '-SkipBuild', '-SkipVenueCheck',
    '-Rest', 'http://127.0.0.1:9', '-WsApi', 'ws://127.0.0.1:9', '-MarketWs', 'ws://127.0.0.1:9', '-Force')
Check 'start-live refuses a second run, even with -Force' 1 $r.code
Check 'and says so' $true ($r.said -match 'a live supervisor is already running')
Check 'and writes no run.json' $false (Test-Path (Join-Path $third 'run.json'))

$r = Run-Script 'preflight-live.ps1' @('-Root', $third, '-Symbol', 'BTCUSDT', '-Environment', 'testnet', '-Qty', '0.001', '-NoBuild', '-NoVenue', '-Rest', 'http://127.0.0.1:9')
Check 'preflight blocks on the running run' $true ($r.said -match 'BLOCK\s+one live run')

$started = Get-Date
$r = Run-Script 'stop-live.ps1' @('-Root', $first, '-GraceSeconds', '60')
$sup.WaitForExit()
$m = Read-RunJson $first
Check 'stop-live exits 0 on a clean stop' 0 $r.code
Check 'the run stopped well before its 90 seconds' $true (((Get-Date) - $started).TotalSeconds -lt 60)
Check 'run.json says stopped' 'stopped' $m.outcome
Check 'the binary saw the stop file' $true ($m.verdict -match 'STOPPED by the stop file')
Check 'run.json records when the stop was asked for' $true (-not [string]::IsNullOrWhiteSpace($m.stop_requested_at))
Check 'stop-live reports the outcome in words' $true ($r.said -match 'ENDED\s+STOPPED')
Check 'one attempt, no restart after the stop' 1 (Attempts $first)
Check 'the lock is free once the supervisor ends' $false (Test-LiveLockHeld)
$r = Run-Script 'stop-live.ps1' @('-Root', $first)
Check 'a second stop finds nothing running and says how it ended' $true ($r.code -eq 0 -and $r.said -match 'not running')

# --- 4. the keep-awake request -----------------------------------------------------
Write-Host 'the keep-awake request is held for the supervisor''s life and released'
$log = Supervisor-Log $first
Check 'the supervisor held ES_CONTINUOUS|ES_SYSTEM_REQUIRED on its thread' $true ($log -match 'keep-awake held: .*thread 0x80000001')
Check 'and released the request it made' $true ($log -match 'keep-awake released \(this thread held 0x80000001, and holds 0x80000000 now\)')
# In this process, the functions themselves, read back from the thread.
$held = Enable-KeepAwake
Check 'Enable-KeepAwake: this thread holds the request' '0x80000001' (Format-ExecutionState $held)
Check 'and it stays held' '0x80000001' (Format-ExecutionState (Get-ThreadAwakeState))
$was = Disable-KeepAwake
Check 'Disable-KeepAwake: it released what was held' '0x80000001' (Format-ExecutionState $was)
Check 'and nothing is held after' '0x80000000' (Format-ExecutionState (Get-ThreadAwakeState))

# --- 5. start-live, end to end, and the old scripts handing over ---------------
Write-Host 'start-live writes the run down, runs it, and stop-run, status and stop-live all reach it'
$root = New-Root
Fake 120 0
$r = Run-Script 'start-live.ps1' @('-Root', $root, '-SkipBuild', '-SkipVenueCheck', '-Minutes', '5', '-Qty', '0.002',
    '-Rest', 'http://127.0.0.1:9', '-WsApi', 'ws://127.0.0.1:9', '-MarketWs', 'ws://127.0.0.1:9', '-VerifyEveryMinutes', '1', '-FirstCheckSeconds', '1')
$m = Read-RunJson $root
Check 'start-live exits 0 once the binary runs' 0 $r.code
Check 'it says the venue is the testnet, no money at risk' $true ($r.said -match 'Spot Testnet, no money at risk')
Check 'and that this is a rehearsal' $true ($r.said -match 'REHEARSAL')
Check 'run.json: mode live, testnet, no real money' 'live testnet False' "$($m.mode) $($m.environment) $($m.real_money)"
Check 'run.json: the preflight skips are on the record' 'build venue-check' "$($m.preflight_skipped)"
Check 'run.json: the commit is the whole id' $true ("$($m.commit)" -match '^[0-9a-f]{40}$')
Check 'run.json: and its subject' 'the staged harness' "$($m.commit_subject)"
$argv = "$($m.live_args)"
Check 'the binary is told --env testnet' $true ($argv -match '--env testnet')
Check 'and the run''s size' $true ($argv -match '--qty 0\.002')
Check 'and its stop file' $true ($argv -match '--stop-file .*live-\d{8}-\d{6}\.stop')
# The supervisor copies the banner once the binary has printed it, after it
# has said it is running: waited for, not assumed.
Until 'the banner in run.json' { "$((Read-RunJson $root).banner)" -match 'a fake live.exe' } 30
Check 'the banner is copied into run.json' $true ("$((Read-RunJson $root).banner)" -match 'args\s+--symbol BTCUSDT --env testnet')
Check 'the key variables are named and never printed' $false ($r.said -match 'placeholder-')
Check 'run.json holds plain values, not PowerShell objects' $true ((Get-Item (Join-Path $root 'run.json')).Length -lt 16KB)
$status = Run-Script 'status.ps1' @('-Root', $root)
Check 'status.ps1 hands a live root to the live status' $true ($status.said -match 'RUNNING\s+live pid')
$r = Run-Script 'stop-run.ps1' @('-Root', $root)
$m = Read-RunJson $root
Check 'stop-run.ps1 hands a live run to stop-live and it stops cleanly' 0 $r.code
Check 'stopped, by the stop file' 'stopped' $m.outcome
Check 'the after-run checks ran and are recorded' 'clean' $m.checks
$status = Run-Script 'status.ps1' @('-Root', $root)
Check 'status shows how it ended' $true ($status.said -match 'ENDED\s+STOPPED \(exit 0\)')
Check 'and the after-run checks' $true ($status.said -match 'after-run checks clean')
Until 'the verify loop to be gone' { $null -eq (Get-Process -Id ((Get-Content (Join-Path $root 'run.pids'))[1]) -ErrorAction SilentlyContinue) } 30

$root = New-Root
Fake 1 0
$r = Run-Script 'start-live.ps1' @('-Root', $root, '-RealMoney', '-SkipBuild', '-SkipVenueCheck', '-Minutes', '1',
    '-Rest', 'http://127.0.0.1:9', '-WsApi', 'ws://127.0.0.1:9', '-MarketWs', 'ws://127.0.0.1:9', '-FirstCheckSeconds', '1')
Until 'the real-money run to finish' { $m = Read-RunJson $root; $null -ne $m -and $null -ne $m.checks }
$m = Read-RunJson $root
Check '-RealMoney is said loudly' $true ($r.said -match 'LIVE: BINANCE, WITH REAL MONEY')
Check '-RealMoney: the binary is told --env live' $true ("$($m.live_args)" -match '--env live')
Check '-RealMoney: run.json says live, real money' 'live True' "$($m.environment) $($m.real_money)"
$p = (Get-Content (Join-Path $root 'run.pids'))[1]
Stop-Process -Id $p -Force -ErrorAction SilentlyContinue

# --- 6. what start-live refuses before anything --------------------------------
Write-Host 'one symbol, spelled as the venue spells it, and a stand-in for all or none'
foreach ($bad in @('BTCUSDT,ETHUSDT', 'BTCUSDT ETHUSDT', 'btcusdt')) {
    $root = New-Root
    $r = Run-Script 'start-live.ps1' @('-Root', $root, '-Symbol', $bad, '-SkipBuild', '-SkipVenueCheck')
    Check "-Symbol '$bad' is refused" 1 $r.code
    Check "  and nothing is written" $false (Test-Path (Join-Path $root 'run.json'))
}
$root = New-Root
$r = Run-Script 'start-live.ps1' @('-Root', $root, '-Rest', 'http://127.0.0.1:9', '-SkipBuild', '-SkipVenueCheck')
Check 'a stand-in for one endpoint only is refused' $true ($r.code -eq 1 -and $r.said -match 'go together')

# --- 7. the preflight's refusals ----------------------------------------------
Write-Host 'the preflight refuses a dirty root, an existing journal and missing keys, and -Resume is explicit'
function Preflight {
    param([string] $Root, [string[]] $Extra = @())
    return Run-Script 'preflight-live.ps1' (@('-Root', $Root, '-Symbol', 'BTCUSDT', '-Environment', 'testnet', '-Qty', '0.001',
        '-NoBuild', '-NoVenue', '-Rest', 'http://127.0.0.1:9') + $Extra)
}
$root = New-Root
$r = Preflight $root
Check 'a clean root passes' 0 $r.code
foreach ($tree in @('raw', 'account')) {
    $root = New-Root
    $dir = Join-Path $root "$tree\exchange=binance\symbol=BTCUSDT\date=2026-10-27\session=x"
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
    Set-Content -Path (Join-Path $dir 'part-00000.bin.zst') -Value 'x'
    $r = Preflight $root
    Check "a capture under $tree\ blocks" $true ($r.code -eq 1 -and $r.said -match 'BLOCK\s+capture root')
}
$root = New-Root
Set-Content -Path (Join-Path $root 'live-BTCUSDT.jsonl') -Value '{}'
$r = Preflight $root
Check 'an existing journal blocks' $true ($r.code -eq 1 -and $r.said -match 'BLOCK\s+journal')
$r = Preflight $root @('-Resume')
Check 'and -Resume accepts it, on the record' $true ($r.code -eq 0 -and $r.said -match 'note\s+journal\s+resuming')
$root = New-Root
$r = Preflight $root @('-Resume')
Check '-Resume with nothing to resume blocks' $true ($r.code -eq 1 -and $r.said -match 'nothing|no journal')
$saved = $env:BINANCE_ED25519_PRIVATE_KEY
$env:BINANCE_ED25519_PRIVATE_KEY = '   '
$r = Preflight (New-Root)
$env:BINANCE_ED25519_PRIVATE_KEY = $saved
Check 'a missing private key blocks, and is named' $true ($r.code -eq 1 -and $r.said -match 'BLOCK\s+credentials\s+BINANCE_ED25519_PRIVATE_KEY is not set')
Check 'the key that is set is named, not printed' $true ($r.said -match 'BINANCE_API_KEY is set' -and $r.said -notmatch 'placeholder-')

# --- 8. the clock checks -------------------------------------------------------
Write-Host ''
$r = Run-Script 'test-venue-clock.ps1' @()
Write-Host $r.said
Check 'the clock checks pass' 0 $r.code

} finally {
    Clear-Stage
    Remove-Item -Recurse -Force -LiteralPath $stage -ErrorAction SilentlyContinue
}
Write-Host ""
Write-Host "$script:pass passed, $script:fail failed"
if ($script:fail -gt 0) { exit 1 }
exit 0
