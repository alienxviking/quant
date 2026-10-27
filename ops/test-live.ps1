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
    behaves as the real one does where the harness depends on it: it refuses
    a stop file that already exists, prints a banner and a blank line, says
    whether its gates passed, watches the --stop-file it is given, and ends
    with the `exit      N` line the real one ends with. Otherwise it runs for
    FAKE_LIVE_SECONDS and exits FAKE_LIVE_EXIT; FAKE_LIVE_GATE=refuse makes
    it record only, and FAKE_LIVE_SILENT=1 leaves out its last line, as a
    process ended from outside would. The real verify, reconcile, runlog,
    replay-account and venue-check are copied beside it, so the after-run
    checks run for real.

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
    static int End(int code) {
        if (Environment.GetEnvironmentVariable("FAKE_LIVE_SILENT") != "1") { Console.WriteLine("exit      " + code); }
        return code;
    }
    public static int Main(string[] args) {
        string stop = null;
        for (int i = 0; i + 1 < args.Length; i++) { if (args[i] == "--stop-file") { stop = args[i + 1]; } }
        // As the real binary: a stop file already there is a bad argument,
        // refused before anything else is said.
        if (stop != null && File.Exists(stop)) {
            Console.Error.WriteLine("--stop-file names a file that already exists");
            return End(2);
        }
        Console.WriteLine("venue     a fake live.exe, for the harness tests");
        Console.WriteLine("args      " + string.Join(" ", args));
        Console.WriteLine("argv      [" + string.Join("][", args) + "]");
        Console.WriteLine();
        string gate = Environment.GetEnvironmentVariable("FAKE_LIVE_GATE") ?? "pass";
        if (gate == "pass") { Console.WriteLine("trading   under run tag fakerun1"); }
        if (gate == "refuse") { Console.WriteLine("RECORD ONLY  the size gate refused: a fake refusal"); }
        int seconds = Int32.Parse(Environment.GetEnvironmentVariable("FAKE_LIVE_SECONDS") ?? "1");
        int code = Int32.Parse(Environment.GetEnvironmentVariable("FAKE_LIVE_EXIT") ?? "0");
        DateTime until = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < until) {
            if (stop != null && File.Exists(stop)) {
                Console.WriteLine("verdict   STOPPED by the stop file");
                return End(gate == "refuse" ? 3 : 0);
            }
            Thread.Sleep(50);
        }
        Console.WriteLine("verdict   the fake exits " + code);
        return End(code);
    }
}
'@
$fakeExe = Join-Path $stage 'target\release\live.exe'
Add-Type -TypeDefinition $fake -OutputAssembly $fakeExe -OutputType ConsoleApplication
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
    param([string] $Name)
    $script:roots++
    if ([string]::IsNullOrWhiteSpace($Name)) { $Name = "run-$script:roots" }
    $root = Join-Path $stage $Name
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

function Script-Arguments {
    # powershell.exe's own arguments, then the script's, by the harness's own
    # quoting rule: these tests launch the scripts as start-live.ps1 does.
    param([string] $Script, [string[]] $Arguments)
    return @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File') + (ConvertTo-NativeArguments (@((Join-Path $sops $Script)) + $Arguments))
}

function Start-Hidden {
    param([string] $Script, [string[]] $Arguments)
    $p = Start-Process -FilePath 'powershell.exe' -ArgumentList (Script-Arguments $Script $Arguments) -WindowStyle Hidden -PassThru
    $null = $p.Handle
    return $p
}

function Start-Logged {
    # A staged script started and left running, what it says kept in a file.
    param([string] $Script, [string[]] $Arguments)
    $out = Join-Path $stage ("said-{0}.txt" -f [guid]::NewGuid().ToString('N').Substring(0, 8))
    $p = Start-Process -FilePath 'powershell.exe' -ArgumentList (Script-Arguments $Script $Arguments) `
        -RedirectStandardOutput $out -RedirectStandardError "$out.err" -NoNewWindow -PassThru
    $null = $p.Handle
    return @{ process = $p; out = $out }
}

function Said {
    param($Logged)
    return ((@(Read-SharedLines -Path $Logged.out) + @(Read-SharedLines -Path "$($Logged.out).err")) -join "`n")
}

function Run-Script {
    # A script of the staged harness, to its end: its exit code, and what it said.
    param([string] $Script, [string[]] $Arguments)
    $run = Start-Logged $Script $Arguments
    $run.process.WaitForExit()
    return @{ code = $run.process.ExitCode; said = (Said $run) }
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
    return ((Read-SharedLines -Path (Join-Path $Root 'logs\supervisor-live.log')) -join "`n")
}

function Fake {
    param([int] $Seconds, [int] $Exit, [string] $Gate = 'pass', [switch] $Silent)
    $env:FAKE_LIVE_SECONDS = "$Seconds"
    $env:FAKE_LIVE_EXIT = "$Exit"
    $env:FAKE_LIVE_GATE = $Gate
    if ($Silent) { $env:FAKE_LIVE_SILENT = '1' } else { $env:FAKE_LIVE_SILENT = '' }
}

function Loops {
    # The verify loops watching a root, by their command line.
    param([string] $Root)
    return @(Find-HarnessProcesses -Script 'verify-loop.ps1' -Root $Root).Count
}

$script:decoys = @()
function Start-Decoy {
    # A powershell.exe that is nothing of this harness's: what a reused pid
    # would be. Not under the stage, so Clear-Stage cannot see it.
    $p = Start-Process -FilePath 'powershell.exe' -ArgumentList @('-NoProfile', '-Command', 'Start-Sleep -Seconds 300') -WindowStyle Hidden -PassThru
    $script:decoys += $p.Id
    return $p
}

function Clear-Stage {
    # Whatever this run of the tests started and left running, a failed check
    # or a thrown wait included: only powershell.exe and live.exe, and only
    # those whose command line names this run's throwaway tree, and the
    # decoys by the ids this run started them with. A leftover supervisor
    # would hold the one-live-run lock for the next run.
    Get-CimInstance Win32_Process -Filter "Name='powershell.exe' or Name='live.exe'" |
        Where-Object { $_.ProcessId -ne $PID -and "$($_.CommandLine)".Contains($stage) } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
    foreach ($id in $script:decoys) {
        $p = Get-CimInstance Win32_Process -Filter "ProcessId=$id" -ErrorAction SilentlyContinue
        if ($null -ne $p -and "$($p.CommandLine)".Contains('Start-Sleep -Seconds 300')) {
            Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
        }
    }
}

$standIn = @('-Rest', 'http://127.0.0.1:9', '-WsApi', 'ws://127.0.0.1:9', '-MarketWs', 'ws://127.0.0.1:9')

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
# The meanings are distinct: four codes, four sentences, and a kill.
$meanings = @(0, 1, 2, 3, 9 | ForEach-Object { (Get-LiveOutcome -Code $_ -Announced $_).meaning } | Sort-Object -Unique)
Check 'the five endings have five different meanings' 5 $meanings.Count

# An ending is the binary's only when it said so: the same code with no last
# line, or with a line naming another code, is a kill.
Write-Host 'an ending the binary did not announce is a kill, whatever its code'
Check 'exit 1 announced as 1 is a failure' 'failed' (Get-LiveOutcome -Code 1 -Announced 1).outcome
Check 'exit 1 never announced is a kill' 'killed' (Get-LiveOutcome -Code 1 -Announced $null).outcome
Check 'and its meaning names taskkill' $true ((Get-LiveOutcome -Code 1 -Announced $null).meaning -match 'taskkill /F')
Check 'exit 0 announced as 3 is a kill' 'killed' (Get-LiveOutcome -Code 0 -Announced 3).outcome
Check 'the last exit line is the one read' 3 (Get-AnnouncedExit -Lines @('exit      1', 'x', 'exit      3'))
Check 'no exit line reads as none' $true ($null -eq (Get-AnnouncedExit -Lines @('verdict   STOPPED', '{"exit":1}')))
$root = New-Root
Write-Manifest $root
Fake 1 1 -Silent
$r = Run-Script 'supervise-live.ps1' @('-Root', $root)
$m = Read-RunJson $root
Check 'a silent exit 1 is reported killed, not failed' 'killed' $m.outcome
Check '  and the supervisor exits 4' 4 $r.code
Check '  and run.json records that nothing was announced' $true ($null -eq $m.exit_announced)

# --- 2. a killed binary ---------------------------------------------------------
Write-Host 'a binary that is killed is reported as killed, and not restarted, by Stop-Process and by taskkill /F'
foreach ($how in @('Stop-Process', 'taskkill')) {
    $root = New-Root
    Write-Manifest $root
    Fake 60 0
    $sup = Start-Hidden 'supervise-live.ps1' @('-Root', $root)
    Until 'the binary to start' { $m = Read-RunJson $root; $null -ne $m -and $m.supervisor -eq 'running' }
    $m = Read-RunJson $root
    if ($how -eq 'Stop-Process') {
        Stop-Process -Id $m.live_pid -Force
    } else {
        cmd /c "taskkill /F /PID $($m.live_pid) > nul 2>&1"
    }
    $sup.WaitForExit()
    $m = Read-RunJson $root
    Check "${how}: the supervisor exits 4" 4 $sup.ExitCode
    Check "${how}: run.json says killed" 'killed' $m.outcome
    Check "${how}: the meaning says the journal has no closing lines" $true ($m.outcome_meaning -match 'no closing lines')
    Check "${how}: one attempt, no restart" 1 (Attempts $root)
}
Check 'taskkill /F ends a process with 1, which is why the code alone cannot tell' 1 $m.exit_code

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
$r = Run-Script 'start-live.ps1' (@('-Root', $third, '-SkipBuild', '-SkipVenueCheck', '-Force') + $standIn)
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

# --- 3b. a live.exe outside the harness ------------------------------------------
# The lock cannot see a binary started by hand, or one whose supervisor died
# (its lock released): only the process check can, in each of the three
# places that asks it.
Write-Host 'a live.exe running outside the harness is refused by the supervisor, start-live and the preflight'
Fake 120 0
$loose = Join-Path $stage 'loose.stop'
$outside = Start-Process -FilePath $fakeExe -ArgumentList (ConvertTo-NativeArguments @('--stop-file', $loose)) -WindowStyle Hidden -PassThru
$null = $outside.Handle
Until 'the outside live.exe to run' { (Get-LiveProcesses).Count -gt 0 }
Check 'it does not hold the lock' $false (Test-LiveLockHeld)
$root = New-Root
Write-Manifest $root
$r = Run-Script 'supervise-live.ps1' @('-Root', $root)
$m = Read-RunJson $root
Check 'the supervisor refuses, exit 5' 5 $r.code
Check '  and says a live.exe is already running' $true ($m.outcome_meaning -match 'a live\.exe is already running')
Check '  and starts nothing' 0 (Attempts $root)
$root = New-Root
$r = Run-Script 'start-live.ps1' (@('-Root', $root, '-SkipBuild', '-SkipVenueCheck', '-Force') + $standIn)
Check 'start-live refuses, even with -Force' 1 $r.code
Check '  and says a live.exe is already running' $true ($r.said -match 'a live\.exe is already running')
Check '  and writes no run.json' $false (Test-Path (Join-Path $root 'run.json'))
$r = Run-Script 'preflight-live.ps1' @('-Root', $root, '-Symbol', 'BTCUSDT', '-Environment', 'testnet', '-Qty', '0.001', '-NoBuild', '-NoVenue', '-Rest', 'http://127.0.0.1:9')
Check 'the preflight blocks on it' $true ($r.code -eq 1 -and $r.said -match 'BLOCK\s+one live run\s+a live\.exe is running')
Set-Content -Path $loose -Value 'stop'
$outside.WaitForExit()

# --- 3c. a reader holding run.json open --------------------------------------
Write-Host 'run.json is shared between its writer and its readers, and a reader cannot end the supervision'
# Many writes against many reads at once, one of the readers Get-Content
# itself, which opens the file without sharing delete and so blocks a replace
# while it reads: every write and every read must come through.
$root = New-Root
Write-Manifest $root
$writer = Join-Path $stage 'write-many.ps1'
Set-Content -Path $writer -Encoding ascii -Value @'
param([string] $Root, [string] $Result)
$ErrorActionPreference = 'Stop'
. (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'ops\live-common.ps1')
$failed = 0
for ($i = 0; $i -lt 300; $i++) {
    try { Set-RunField -Root $Root -Fields @{ n = $i } } catch { $failed++ }
}
Set-Content -Path $Result -Value $failed
'@
$getContent = Join-Path $stage 'get-content-many.ps1'
Set-Content -Path $getContent -Encoding ascii -Value @'
param([string] $Root, [string] $Until)
while (-not (Test-Path $Until)) { try { $null = Get-Content -Path (Join-Path $Root 'run.json') -Raw } catch { } }
'@
$result = Join-Path $root 'writes-failed.txt'
$hog = Start-Process -FilePath 'powershell.exe' -ArgumentList (@('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File') + (ConvertTo-NativeArguments @($getContent, $root, $result))) -WindowStyle Hidden -PassThru
$w = Start-Process -FilePath 'powershell.exe' -ArgumentList (@('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File') + (ConvertTo-NativeArguments @($writer, $root, $result))) -WindowStyle Hidden -PassThru
$null = $w.Handle
$reads = 0; $readFailures = 0
while (-not $w.HasExited) {
    try {
        $m = Read-RunJson $root
        if ($null -eq $m -or $m.mode -ne 'live') { $readFailures++ }
    } catch { $readFailures++ }
    $reads++
}
$w.WaitForExit()
Until 'the Get-Content reader to finish' { $hog.HasExited } 30
Check "300 writes, none failed, against $reads reads and a Get-Content reader" '0' "$(Get-Content $result)"
Check 'and every read found the manifest' 0 $readFailures
Check 'the reads were many' $true ($reads -gt 50)
Check 'and the last write is the one there' 299 (Read-RunJson $root).n

# The logs are read while they are written: a line appended while a reader
# holds the file, as Read-SharedLines holds it, must land. Add-Content's own
# failure there is checked too, as the reason the harness does not use it.
$sharedLog = Join-Path $root 'logs\shared.log'
[void](Add-SharedLine -Path $sharedLog -Line 'first')
$reader = New-Object System.IO.FileStream($sharedLog, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, ([System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete))
try {
    $appended = Add-SharedLine -Path $sharedLog -Line 'while read' -RetrySeconds 0
    $plain = $true
    try { Add-Content -Path $sharedLog -Value 'by Add-Content' -Encoding utf8 } catch { $plain = $false }
} finally {
    $reader.Dispose()
}
Check 'a line appended while a reader holds the log lands' $true $appended
Check '  where Add-Content fails' $false $plain
Check '  and the log holds both shared lines' 'first|while read' ((Read-SharedLines -Path $sharedLog) -join '|')

# The supervisor's own writes, with run.json held as Get-Content holds it for
# the whole of a short run: every one of them fails, and the supervisor must
# still wait for the binary, record its ending once the file is free, and
# exit with the binary's code.
$root = New-Root
Write-Manifest $root
Fake 3 0
$path = Join-Path $root 'run.json'
$held = New-Object System.IO.FileStream($path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
try {
    $logged = Start-Logged 'supervise-live.ps1' @('-Root', $root)
    $sup = $logged.process
    try {
        Until 'the supervisor to see the binary end' { (Supervisor-Log $root) -match 'live exited 0' } 120
    } catch {
        Write-Host "the supervisor's log so far:`n$(Supervisor-Log $root)"
        Write-Host "what it said:`n$(Said $logged)"
        Write-Host "the supervisor has exited: $($sup.HasExited)"
        throw
    }
    $log = Supervisor-Log $root
} finally {
    $held.Dispose()
}
$sup.WaitForExit()
$m = Read-RunJson $root
Check 'held open: the supervisor said it could not write the pid' $true ($log -match 'WARNING: could not write the binary''s pid into run\.json')
Check '  and still waited for the binary to end' $true ($log -match 'live exited 0 after')
Check '  and recorded the ending once the file was free' 'stopped' $m.outcome
Check '  and the after-run checks' $true ($null -ne $m.checks)
Check '  and exited with the binary''s code' 0 $sup.ExitCode
Check '  one attempt' 1 (Attempts $root)

# --- 4. the keep-awake request -----------------------------------------------------
Write-Host 'the keep-awake request is held for the supervisor''s life and released'
$log = Supervisor-Log $first
Check 'the supervisor held ES_CONTINUOUS|ES_SYSTEM_REQUIRED|ES_DISPLAY_REQUIRED on its thread' $true ($log -match 'keep-awake held: .*thread 0x80000003')
Check 'and released the request it made' $true ($log -match 'keep-awake released \(this thread held 0x80000003, and holds 0x80000000 now\)')
# In this process, the functions themselves, read back from the thread.
$held = Enable-KeepAwake
Check 'Enable-KeepAwake: this thread holds the request, the display included' '0x80000003' (Format-ExecutionState $held)
Check 'and it stays held' '0x80000003' (Format-ExecutionState (Get-ThreadAwakeState))
$was = Disable-KeepAwake
Check 'Disable-KeepAwake: it released what was held' '0x80000003' (Format-ExecutionState $was)
Check 'and nothing is held after' '0x80000000' (Format-ExecutionState (Get-ThreadAwakeState))
Check 'a state without the display request is not held' $false (Test-KeepAwakeHeld ([uint32]2147483649))

# --- 5. start-live, end to end, and the old scripts handing over ---------------
Write-Host 'start-live writes the run down, runs it, and stop-run, status and stop-live all reach it'
$root = New-Root
Fake 120 0
$r = Run-Script 'start-live.ps1' (@('-Root', $root, '-SkipBuild', '-SkipVenueCheck', '-Minutes', '5', '-Qty', '0.002',
    '-VerifyEveryMinutes', '1', '-FirstCheckSeconds', '1') + $standIn)
$m = Read-RunJson $root
Check 'start-live exits 0 once the binary trades' 0 $r.code
Check 'it says the gates passed and the run tag' $true ($r.said -match 'TRADING: live pid \d+, gates passed, run tag fakerun1')
Check 'it says the venue is the testnet, no money at risk' $true ($r.said -match 'Spot Testnet, no money at risk')
Check 'and that this is a rehearsal' $true ($r.said -match 'REHEARSAL')
Check 'run.json: mode live, testnet, no real money' 'live testnet False' "$($m.mode) $($m.environment) $($m.real_money)"
Check 'run.json: the preflight skips are on the record' 'build venue-check' "$($m.preflight_skipped)"
Check 'run.json: the commit is the whole id' $true ("$($m.commit)" -match '^[0-9a-f]{40}$')
Check 'run.json: and its subject' 'the staged harness' "$($m.commit_subject)"
Check 'run.json: the gates passed, under the binary''s run tag' 'passed fakerun1' "$($m.gates) $($m.run_tag)"
$argv = "$($m.live_args)"
Check 'the binary is told --env testnet' $true ($argv -match '--env testnet')
Check 'and the run''s size' $true ($argv -match '--qty 0\.002')
Check 'and its stop file' $true ($argv -match '--stop-file .*live-\d{8}-\d{6}\.stop')
Check 'the banner is copied into run.json' $true ("$($m.banner)" -match 'args\s+--symbol BTCUSDT --env testnet')
Check 'the key variables are named and never printed' $false ($r.said -match 'placeholder-')
Check 'run.json holds plain values, not PowerShell objects' $true ((Get-Item (Join-Path $root 'run.json')).Length -lt 16KB)
Check 'one verify loop watches it' 1 (Loops $root)
$status = Run-Script 'status.ps1' @('-Root', $root)
Check 'status.ps1 hands a live root to the live status' $true ($status.said -match 'RUNNING\s+live pid')
Check '  and shows the gates passed' $true ($status.said -match 'gates\s+passed: trading under run tag fakerun1')
Check '  and the supervisor, known by its command line' $true ($status.said -match 'supervisor pid \d+ holds the one-live-run lock')
$r = Run-Script 'stop-run.ps1' @('-Root', $root)
$m = Read-RunJson $root
Check 'stop-run.ps1 hands a live run to stop-live and it stops cleanly' 0 $r.code
Check 'stopped, by the stop file' 'stopped' $m.outcome
Check 'the after-run checks ran and are recorded' 'clean' $m.checks
$status = Run-Script 'status.ps1' @('-Root', $root)
Check 'status shows how it ended' $true ($status.said -match 'ENDED\s+STOPPED \(exit 0\)')
Check 'and the after-run checks' $true ($status.said -match 'after-run checks clean')
Until 'the verify loop to be gone' { (Loops $root) -eq 0 } 30

$root = New-Root
Fake 1 0
$r = Run-Script 'start-live.ps1' (@('-Root', $root, '-RealMoney', '-SkipBuild', '-SkipVenueCheck', '-Minutes', '1', '-FirstCheckSeconds', '1') + $standIn)
Until 'the real-money run to finish' { $m = Read-RunJson $root; $null -ne $m -and $null -ne $m.checks }
$m = Read-RunJson $root
Check '-RealMoney is said loudly' $true ($r.said -match 'LIVE: BINANCE, WITH REAL MONEY')
Check '-RealMoney: the binary is told --env live' $true ("$($m.live_args)" -match '--env live')
Check '-RealMoney: run.json says live, real money' 'live True' "$($m.environment) $($m.real_money)"
Until 'its verify loop to end with the run' { (Loops $root) -eq 0 } 30

# --- 5b. a gate that refuses is said at the prompt -------------------------------
Write-Host 'a run whose gates refuse is said as recording only, at the prompt and in the status view'
$root = New-Root
Fake 120 0 -Gate refuse
$r = Run-Script 'start-live.ps1' (@('-Root', $root, '-SkipBuild', '-SkipVenueCheck', '-Minutes', '5', '-FirstCheckSeconds', '600') + $standIn)
$m = Read-RunJson $root
Check 'start-live exits 3, the binary''s code for recording only' 3 $r.code
Check '  and says RECORDING ONLY with the refusal' $true ($r.said -match 'RECORDING ONLY, NOT TRADING' -and $r.said -match 'the size gate refused: a fake refusal')
Check '  run.json records the refusal' 'refused' "$($m.gates)"
$status = Run-Script 'status.ps1' @('-Root', $root)
Check '  and status shows it while the run records' $true ($status.said -match 'RUNNING' -and $status.said -match 'gates\s+REFUSED, RECORDING ONLY')
$r = Run-Script 'stop-live.ps1' @('-Root', $root, '-GraceSeconds', '60')
Check '  stop-live stops it, recorded only, exit 0' $true ($r.code -eq 0 -and $r.said -match 'ENDED\s+RECORDED-ONLY')

# --- 5c. the verify loop ends with the run -----------------------------------------
Write-Host 'the verify loop ends when the run does, however it ends, and only once the binary runs'
foreach ($how in @('Stop-Process', 'taskkill')) {
    $root = New-Root
    Fake 120 0
    $r = Run-Script 'start-live.ps1' (@('-Root', $root, '-SkipBuild', '-SkipVenueCheck', '-Minutes', '5', '-FirstCheckSeconds', '600') + $standIn)
    $m = Read-RunJson $root
    Check "${how}: a verify loop watches the run" 1 (Loops $root)
    if ($how -eq 'Stop-Process') { Stop-Process -Id $m.live_pid -Force } else { cmd /c "taskkill /F /PID $($m.live_pid) > nul 2>&1" }
    Until "the run to be recorded as killed ($how)" { "$((Read-RunJson $root).outcome)" -eq 'killed' } 60
    Until "its verify loop to end ($how)" { (Loops $root) -eq 0 } 30
    Check "${how}: the loop said why it stopped" $true (((Read-SharedLines -Path (Join-Path $root 'logs\verify.log')) -join "`n") -match 'the run has ended \(killed\)')
}

# A supervisor that refuses after start-live launched it leaves nothing.
$root = New-Root
Move-Item -LiteralPath $fakeExe -Destination "$fakeExe.away"
try {
    $r = Run-Script 'start-live.ps1' (@('-Root', $root, '-SkipBuild', '-SkipVenueCheck', '-Force', '-FirstCheckSeconds', '600') + $standIn)
} finally {
    Move-Item -LiteralPath "$fakeExe.away" -Destination $fakeExe
}
Check 'a supervisor that refuses (no binary): start-live exits 1' 1 $r.code
Check '  and says NOT STARTED' $true ($r.said -match 'NOT STARTED: the supervisor refused')
Check '  and leaves no verify loop behind' 0 (Loops $root)

# A supervisor that dies leaves its binary running: the loop ends, the status
# view says so, and stop-live still stops the binary and says what it said.
$root = New-Root
Fake 120 0
$r = Run-Script 'start-live.ps1' (@('-Root', $root, '-SkipBuild', '-SkipVenueCheck', '-Minutes', '5', '-FirstCheckSeconds', '600') + $standIn)
$m = Read-RunJson $root
Stop-Process -Id $m.supervisor_pid -Force
Until 'the verify loop to end with its supervisor' { (Loops $root) -eq 0 } 30
$status = Run-Script 'status.ps1' @('-Root', $root)
Check 'a killed supervisor: status says LOST, and that the binary runs on' $true ($status.said -match 'LOST\s+the supervisor' -and $status.said -match 'runs on unsupervised')
Check '  and no longer says it holds the lock' $false ($status.said -match 'holds the one-live-run lock')
$r = Run-Script 'stop-live.ps1' @('-Root', $root, '-GraceSeconds', '60')
Check '  stop-live says LOST and exits 1' $true ($r.code -eq 1 -and $r.said -match 'LOST\s+the supervisor')
Check '  and the binary stopped on the file' $true ($null -eq (Get-RunLiveProcess $m))
Check '  and it does not suggest killing a process that has exited' $false ($r.said -match 'Stop-Process -Id')

# --- 5d. a pid is not an identity ------------------------------------------------
Write-Host 'stop-live and status know the harness''s processes by their command line, not by a pid'
$decoy = Start-Decoy
$root = New-Root 'decoy'
$other = New-Root 'decoy0'
Write-Manifest $root
Set-RunField -Root $root -Fields @{ supervisor = 'finished'; outcome = 'stopped'; exit_code = 0; supervisor_pid = $decoy.Id }
Set-Content -Path (Join-Path $root 'run.pids') -Value "$($decoy.Id)`n$($decoy.Id)"
# A verify loop of another root, whose path begins with this one's.
$neighbour = Start-Hidden 'verify-loop.ps1' @('-Root', $other, '-EndAt', (Get-Date).AddMinutes(10).ToString('o'), '-LogDir', (Join-Path $other 'logs'), '-FirstCheckSeconds', '600')
Until 'the neighbour''s loop to run' { (Loops $other) -eq 1 }
$r = Run-Script 'stop-live.ps1' @('-Root', $root)
Check 'stop-live on a finished run exits 0' 0 $r.code
Check '  and leaves alone a powershell.exe whose pid run.json and run.pids name' $true ($null -ne (Get-Process -Id $decoy.Id -ErrorAction SilentlyContinue))
Check '  and the verify loop of a root whose name begins with this one''s' 1 (Loops $other)
Check '  and says it stopped nothing' $false ($r.said -match 'stopped the verify loop')
Set-RunField -Root $root -Fields @{ supervisor = 'running'; live_pid = $decoy.Id }
$status = Run-Script 'status.ps1' @('-Root', $root)
Check 'status does not take the decoy for the supervisor' $false ($status.said -match 'holds the one-live-run lock')
Check '  nor for live.exe' $false ($status.said -match 'RUNNING\s+live pid')
Check '  and says the supervisor is gone' $true ($status.said -match 'LOST\s+the supervisor')
Stop-Process -Id $neighbour.Id -Force -ErrorAction SilentlyContinue

# --- 5e. the stop waits for a binary that can hear it ------------------------------
Write-Host 'a stop asked for while the supervisor is starting waits for the binary to read its arguments'
$root = New-Root
Write-Manifest $root
Fake 60 0
$stopper = Start-Logged 'stop-live.ps1' @('-Root', $root, '-GraceSeconds', '90')
Until 'stop-live to be waiting' { (Said $stopper) -match 'waiting for the supervisor to start the binary' } 60
Check 'no stop file yet' $false (Test-Path (Join-Path $root 'live-test.stop'))
$sup = Start-Hidden 'supervise-live.ps1' @('-Root', $root)
$stopper.process.WaitForExit()
$sup.WaitForExit()
$m = Read-RunJson $root
Check 'stop-live then stops it, exit 0' 0 $stopper.process.ExitCode
Check '  stopped by the stop file, not refused as an argument' 'stopped' $m.outcome
Check '  the binary saw the stop file' $true ($m.verdict -match 'STOPPED by the stop file')

# --- 5f. a root that ends in a backslash, or holds a space -------------------------
Write-Host 'a root ending in a backslash and holding a space reaches every process as one argument'
$root = New-Root 'run with space'
Fake 120 0
$r = Run-Script 'start-live.ps1' (@('-Root', "$root\", '-SkipBuild', '-SkipVenueCheck', '-Minutes', '5', '-FirstCheckSeconds', '1', '-VerifyEveryMinutes', '1') + $standIn)
$m = Read-RunJson $root
Check 'start-live exits 0' 0 $r.code
Check '  run.json names the root without its trailing backslash' $root "$($m.root)"
Check '  the supervisor wrote its log' $true (Test-Path (Join-Path $root 'logs\supervisor-live.log'))
Check '  the binary got the root as one argument' $true ("$($m.banner)" -match [regex]::Escape("[--root][$root]"))
Until 'a verify pass' { @(Get-ChildItem -Path (Join-Path $root 'logs') -Filter 'verify-*.txt' -ErrorAction SilentlyContinue).Count -gt 0 } 60
$passFile = @(Get-ChildItem -Path (Join-Path $root 'logs') -Filter 'verify-*.txt')[0].FullName
Until 'verify to write its root line' { (Read-SharedLines -Path $passFile) -match '^root' } 30
Check '  and verify, run by the loop, was given the whole root' $true (@(Read-SharedLines -Path $passFile) -contains "root      $root")
$r = Run-Script 'stop-live.ps1' @('-Root', "$root\", '-GraceSeconds', '60')
Check '  stop-live, given the same spelling, stops it' $true ($r.code -eq 0 -and "$((Read-RunJson $root).outcome)" -eq 'stopped')
Until 'its verify loop to end' { (Loops $root) -eq 0 } 30
Check 'the quoting rule: a trailing backslash is doubled before the closing quote' '"C:\a b\\"' (ConvertTo-NativeArgument 'C:\a b\')
Check '  a quote inside is escaped' '"x\"y"' (ConvertTo-NativeArgument 'x"y')

# --- 5g. an earlier run's record is kept ----------------------------------------------
Write-Host 'a resumed run keeps the earlier run''s run.json and after-run log'
$root = New-Root
Fake 1 0
$r = Run-Script 'start-live.ps1' (@('-Root', $root, '-SkipBuild', '-SkipVenueCheck', '-Minutes', '1', '-FirstCheckSeconds', '600') + $standIn)
Until 'the first run to finish' { $m = Read-RunJson $root; $null -ne $m -and $null -ne $m.checks }
$firstRun = Read-RunJson $root
Set-Content -Path $firstRun.journal -Value '{}'
Fake 120 0
$r = Run-Script 'start-live.ps1' (@('-Root', $root, '-Resume', '-SkipBuild', '-SkipVenueCheck', '-Minutes', '5', '-FirstCheckSeconds', '600') + $standIn)
$kept = Join-Path $root ("logs\run-{0}.json" -f ("$($firstRun.started_at)" -replace '[^0-9TZ]', ''))
Check '-Resume: start-live exits 0' 0 $r.code
Check '  and says it kept the earlier record' $true ($r.said -match 'kept the earlier run''s record')
Check '  the earlier run.json is kept under its start time' $true (Test-Path $kept)
$keptRun = $null
if (Test-Path $kept) { $keptRun = Get-Content $kept -Raw | ConvertFrom-Json }
Check '  holding the earlier run''s ending' 'stopped finished' "$($keptRun.outcome) $($keptRun.supervisor)"
Check '  the new run.json is the resumed run''s' 'True running' "$((Read-RunJson $root).resume) $((Read-RunJson $root).supervisor)"
$status = Run-Script 'status.ps1' @('-Root', $root)
Check '  and the status view shows no after-run checks while it runs' $false ($status.said -match 'after-run checks')
$r = Run-Script 'stop-live.ps1' @('-Root', $root, '-GraceSeconds', '60')

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

# --- 7b. the after-run checks: what a run that traded is held to ----------------
Write-Host 'the after-run checks hold a run that journalled to a capture, and excuse one that did not'
$root = New-Root
Write-Manifest $root
Set-Content -Path (Join-Path $root 'live-BTCUSDT.jsonl') -Value '{"type":"started"}'
$r = Run-Script 'check-live.ps1' @('-Root', $root)
Check 'a journal with a line and no market capture fails' 1 $r.code
Check '  as nothing verified from a run that traded' $true ($r.said -match 'FAIL verify: NOTHING VERIFIED, no market frame under the root, from a run that traded')
$root = New-Root
Write-Manifest $root
[System.IO.File]::WriteAllText((Join-Path $root 'live-BTCUSDT.jsonl'), '')
$r = Run-Script 'check-live.ps1' @('-Root', $root)
Check 'an empty journal, as a run that recorded only leaves it, is no journal: exit 0' 0 $r.code
Check '  nothing verified is excused' $true ($r.said -match 'NONE verify: nothing verified')
Check '  and the journal checks are not run' $true ($r.said -match 'NONE journal: nothing is written')

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
