<#
.SYNOPSIS
    What the live harness's scripts share. Dot-sourced, never run.

.DESCRIPTION
    Five things more than one live script needs, kept once so that the
    supervisor, the scripts that start, stop and watch a run, and the tests
    cannot disagree about them:

    - what each of the live binary's endings means, in words;
    - the one-live-run lock, and how to ask whether a run holds it;
    - which process is which harness script for which run;
    - holding this host awake for a supervisor's lifetime;
    - reading and writing the run's run.json.

    Windows PowerShell 5.1 throughout: nothing here may need pwsh.
#>

# Beside this file. $PSScriptRoot is this file's own folder in a dot-sourced
# script's body; it is only while param() defaults bind that it is empty.
. (Join-Path $PSScriptRoot 'native-args.ps1')
. (Join-Path $PSScriptRoot 'shared-log.ps1')

# --- what an ending means ----------------------------------------------------
# The live binary's own codes (crates/quant-backtest/src/bin/live.rs, "Exit
# codes"). Every exit ends a live run: the owner ruled out an automatic restart
# on 2026-10-26 (docs/live-run.md, "Where it runs"), so the harness's whole job
# at the end is to say which ending this was, in words a person reads at the
# end of an afternoon, and never to try again.
#
# A code alone cannot say it, because Windows has no signals: a kill is an exit
# code someone else chose, and `taskkill /F` and Task Manager's End task both
# choose 1, the binary's own code for a run that could not go on. So the binary
# writes `exit      N` as its last line on every path that returns, and an
# ending it did not announce, or announced with another code, is a kill
# whatever the code. Announced is that number, or $null when no such line was
# written.
$script:ExitLinePattern = '^exit\s+(-?\d+)$'

function Get-AnnouncedExit {
    # The code the binary said it was exiting with, from its stdout lines, or
    # $null when it never said: the last such line, since it is the last
    # thing the binary writes.
    param([AllowEmptyCollection()] [string[]] $Lines)
    $announced = $null
    foreach ($line in @($Lines)) {
        if ($line -match $script:ExitLinePattern) { $announced = [int]$Matches[1] }
    }
    return $announced
}

function Get-LiveOutcome {
    param([Parameter(Mandatory)] [int] $Code, $Announced)
    if ($null -ne $Announced -and [int]$Announced -eq $Code) {
        switch ($Code) {
            0 { return [ordered]@{ code = 0; outcome = 'stopped'
                    meaning = 'traded and stopped cleanly: the cancels went, the closing lines are written and both captures are sealed' } }
            1 { return [ordered]@{ code = 1; outcome = 'failed'
                    meaning = 'the run could not go on: it could not start, a capture ended in error, or the journal failed, after which every order was withheld and no closing line written' } }
            2 { return [ordered]@{ code = 2; outcome = 'bad-argument'
                    meaning = 'the binary refused its arguments and nothing ran: nothing was recorded and nothing traded' } }
            3 { return [ordered]@{ code = 3; outcome = 'recorded-only'
                    meaning = 'a gate refused: the market was recorded and nothing was traded. Not a failure, and a restart would meet the same refusal' } }
        }
    }
    $how = "exit $Code, and the binary never said it was ending"
    if ($null -ne $Announced -and [int]$Announced -ne $Code) { $how = "exit $Code where the binary had said it would exit $Announced" }
    if ($Code -eq 1 -and $null -eq $Announced) { $how += ' (1 is what taskkill /F and Task Manager''s End task give)' }
    return [ordered]@{ code = $Code; outcome = 'killed'
        meaning = "killed or crashed ($how): no clean stop, so the cancels may not have gone, the journal has no closing lines, the captures' last segments have no trailer, and the next run's last-session gate will refuse to trade until a person has looked" }
}

# --- one live run per host ---------------------------------------------------
# docs/live-run.md section 2: one symbol, one process, one account. Two live
# processes share one exchange balance while each keeps a private portfolio
# and a private daily-loss budget, so each would permit its full budget against
# a balance the other is also spending. The account is not visible from here,
# so the host is the unit: a named mutex, held by the live supervisor for its
# whole life, which Windows releases (abandoned) if the supervisor dies however
# it dies. A second supervisor cannot take it, and start-live.ps1 asks before
# launching anything so that the refusal is said where the operator is looking.
$script:LiveLockName = 'Global\quant-live-run'

function Test-LiveLockHeld {
    # True when some live supervisor holds the lock now. Asked by taking it and
    # giving it straight back: a mutex has no "who holds you" query, and an
    # abandoned one (its holder died) is free, which taking it proves. Asked
    # from another process, as every caller does: a mutex is recursive, so the
    # thread holding it would be told it is free.
    $mutex = New-Object System.Threading.Mutex($false, $script:LiveLockName)
    try {
        $taken = $false
        try {
            $taken = $mutex.WaitOne(0)
        } catch [System.Threading.AbandonedMutexException] {
            $taken = $true
        }
        if ($taken) { $mutex.ReleaseMutex(); return $false }
        return $true
    } finally {
        $mutex.Dispose()
    }
}

function Enter-LiveLock {
    # The mutex if this process now holds it, or $null. Ownership belongs to
    # the calling thread, which for powershell.exe -File is the one thread the
    # whole script runs on, so it is released by Exit-LiveLock on that thread
    # or by Windows when the process ends.
    $mutex = New-Object System.Threading.Mutex($false, $script:LiveLockName)
    $taken = $false
    try {
        $taken = $mutex.WaitOne(0)
    } catch [System.Threading.AbandonedMutexException] {
        # The last holder died without letting go. Its run is over; ours may
        # begin, and the supervisor says so in its log.
        $taken = $true
        $script:LiveLockWasAbandoned = $true
    }
    if ($taken) { return $mutex }
    $mutex.Dispose()
    return $null
}

function Exit-LiveLock {
    param($Mutex)
    if ($null -eq $Mutex) { return }
    try { $Mutex.ReleaseMutex() } catch { }
    $Mutex.Dispose()
}

function Get-LiveProcesses {
    # Every live.exe on this host, whoever started it and from whichever
    # checkout: they would all share the one account.
    return @(Get-Process -Name 'live' -ErrorAction SilentlyContinue)
}

# --- which process is which --------------------------------------------------
# A pid is not an identity: Windows hands a dead process's id to the next one,
# so a pid written down for a run that ended an hour ago may now be the
# operator's own shell. A harness process is known by what it runs: its
# command line names the script and, quoted exactly as every launch here
# quotes it (ConvertTo-NativeArgument), the run's root. The quotes are what
# keep run-1 from matching run-10. Win32_Process gives the command line of
# this user's processes without Administrator.
function Test-HarnessCommandLine {
    param([string] $CommandLine, [Parameter(Mandatory)] [string] $Script, [Parameter(Mandatory)] [string] $Root)
    if ([string]::IsNullOrEmpty($CommandLine)) { return $false }
    $line = $CommandLine.ToLowerInvariant()
    $named = ('\' + $Script + '"').ToLowerInvariant()
    $root = (ConvertTo-NativeArgument $Root).ToLowerInvariant()
    return $line.Contains($named) -and $line.Contains($root)
}

function Find-HarnessProcesses {
    # Every powershell.exe running this harness script for this root.
    param([Parameter(Mandatory)] [string] $Script, [Parameter(Mandatory)] [string] $Root)
    return @(Get-CimInstance Win32_Process -Filter "Name='powershell.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.ProcessId -ne $PID -and (Test-HarnessCommandLine $_.CommandLine $Script $Root) })
}

function Test-HarnessProcess {
    # Whether the process with this id is running this harness script for
    # this root: the id alone is not enough, as above.
    param($Id, [Parameter(Mandatory)] [string] $Script, [Parameter(Mandatory)] [string] $Root)
    if ($null -eq $Id -or "$Id" -notmatch '^\d+$') { return $false }
    if ($null -eq (Get-Process -Id ([int]$Id) -ErrorAction SilentlyContinue)) { return $false }
    $cim = Get-CimInstance Win32_Process -Filter "ProcessId=$([int]$Id)" -ErrorAction SilentlyContinue
    if ($null -eq $cim) { return $false }
    return (Test-HarnessCommandLine $cim.CommandLine $Script $Root)
}

function Get-RunLiveProcess {
    # The run's live.exe, if the pid run.json names is still a live.exe that
    # started no earlier than the supervisor launched it; $null otherwise.
    param($Manifest)
    if ($null -eq $Manifest -or "$($Manifest.live_pid)" -notmatch '^\d+$') { return $null }
    $p = Get-Process -Id ([int]$Manifest.live_pid) -ErrorAction SilentlyContinue
    if ($null -eq $p -or $p.ProcessName -ne 'live') { return $null }
    if ($Manifest.live_started_at) {
        try {
            $launched = ([datetime]::Parse("$($Manifest.live_started_at)")).ToUniversalTime()
            if ($p.StartTime.ToUniversalTime() -lt $launched.AddSeconds(-5)) { return $null }
        } catch { }
    }
    return $p
}

function Stop-VerifyLoops {
    # The verify loops watching this root, found by their command line. Each
    # only reads, and sleeps between passes, so ending one costs nothing; a
    # verify.exe it started finishes on its own. Returns the ids stopped.
    param([Parameter(Mandatory)] [string] $Root)
    $stopped = @()
    foreach ($p in (Find-HarnessProcesses -Script 'verify-loop.ps1' -Root $Root)) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
        $stopped += $p.ProcessId
    }
    return $stopped
}

# --- keeping the host awake -------------------------------------------------
# The Windows half of caffeinate -dimsu -w $$: SetThreadExecutionState with
# ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_DISPLAY_REQUIRED asks Windows neither
# to sleep nor to turn the display off for idleness while the calling thread
# lives, and changes no setting at all. The display is held as caffeinate's -d
# holds it, and on this host it matters: its sleep is Modern Standby (S0 Low
# Power Idle, powercfg /a), which a laptop enters when the display goes off,
# so a request for the system alone is not known to keep it out. Nothing
# persists: it is released when the thread asks (Disable-KeepAwake), and by
# Windows when the thread or the process ends, a supervisor killed included.
#
# What it does not do, and the run procedure says so: it does not stop sleep
# that a person asks for, a closed lid, or a restart Windows Update forces.
# Those are the operator's to prevent for an afternoon (docs/live-run.md
# section 7).
$script:EsContinuous      = [uint32]2147483648   # 0x80000000
$script:EsSystemRequired  = [uint32]1            # 0x00000001
$script:EsDisplayRequired = [uint32]2            # 0x00000002
$script:EsHeld = $script:EsContinuous -bor $script:EsSystemRequired -bor $script:EsDisplayRequired

if (-not ('QuantKeepAwake' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class QuantKeepAwake {
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern uint SetThreadExecutionState(uint esFlags);

    [DllImport("powrprof.dll")]
    public static extern uint CallNtPowerInformation(int level, IntPtr inBuffer, uint inLength, out uint outBuffer, uint outLength);

    // SystemExecutionState: the execution state every thread on the system
    // has asked for, together. Readable without Administrator, unlike
    // powercfg /requests, and not attributable to a process.
    public static uint SystemState() {
        uint state;
        uint status = CallNtPowerInformation(16, IntPtr.Zero, 0, out state, 4);
        return status == 0 ? state : 0xFFFFFFFF;
    }
}
'@
}

function Format-ExecutionState {
    param([uint32] $State)
    return ('0x{0:X8}' -f $State)
}

function Test-KeepAwakeHeld {
    # Whether a state read back holds both requests made.
    param([uint32] $State)
    $both = $script:EsSystemRequired -bor $script:EsDisplayRequired
    return (($State -band $both) -eq $both)
}

function Enable-KeepAwake {
    # Returns what this thread now holds, read back rather than assumed: the
    # second call asks for the same state and returns the one it replaced.
    # A failed call returns 0, which the read-back then shows as a state
    # without the requests: the caller checks the bits, not the call.
    [void][QuantKeepAwake]::SetThreadExecutionState($script:EsHeld)
    return [QuantKeepAwake]::SetThreadExecutionState($script:EsHeld)
}

function Disable-KeepAwake {
    # Returns what this thread held until now: ES_CONTINUOUS with both
    # requests when Enable-KeepAwake ran on this same thread, which is the
    # check that the request being released is the one that was made.
    return [QuantKeepAwake]::SetThreadExecutionState($script:EsContinuous)
}

function Get-ThreadAwakeState {
    # What this thread holds, without changing it: ask for the same state and
    # read back the one replaced. Only meaningful after Enable or Disable, which
    # both leave ES_CONTINUOUS set.
    $current = [QuantKeepAwake]::SetThreadExecutionState($script:EsContinuous)
    [void][QuantKeepAwake]::SetThreadExecutionState($current)
    return $current
}

# --- run.json ----------------------------------------------------------------
# Written once by start-live.ps1 before anything launches, and after that only
# by the supervisor: one writer. One writer is not the whole of it, because
# start-live, stop-live, status and the verify loop all read it while the
# supervisor writes, and on Windows a reader can stop a writer: a file open
# without FILE_SHARE_DELETE cannot be replaced, and Move-Item -Force, which
# this used, deletes the old file and then moves the new one in, so a reader
# arriving between the two finds no file at all. Measured in tight loops: 140
# of 1500 writes and 618 of 21399 reads failed; and a reader holding the file
# as Get-Content does, at the moment the supervisor recorded the binary's pid,
# killed the supervisor and left live.exe trading with nobody watching.
#
# So, on both sides: every reader opens it sharing read, write and delete, as
# Read-SharedLines does, so no reader blocks a write; the writer swaps the new
# file in with File.Replace, which no reader sees half done; and both retry
# for a while, because something outside the harness (an editor, an indexer,
# a person's Get-Content) may hold it in a way no rule here controls. UTF-8
# without a byte order mark, so any reader takes it.
$script:RunJsonRetrySeconds = 10

function Read-SharedText {
    # A file's whole text, read sharing read, write and delete.
    param([Parameter(Mandatory)] [string] $Path)
    $share = [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
    $stream = New-Object System.IO.FileStream($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, $share)
    try {
        $reader = New-Object System.IO.StreamReader($stream, (New-Object System.Text.UTF8Encoding($false)))
        return $reader.ReadToEnd()
    } finally {
        $stream.Dispose()
    }
}

function Read-RunJson {
    # The manifest, or $null when there is none. A read that fails while the
    # file exists is retried; one still failing after the retries throws,
    # loudly, rather than reporting a run that is there as absent.
    param([Parameter(Mandatory)] [string] $Root, [int] $RetrySeconds = $script:RunJsonRetrySeconds)
    $path = Join-Path $Root 'run.json'
    $deadline = (Get-Date).AddSeconds($RetrySeconds)
    while ($true) {
        if (-not (Test-Path -LiteralPath $path)) {
            # Absent for good, or between a failed replace and its retry,
            # which is the one way a writer can leave no file for a moment.
            if ((Get-Date) -gt $deadline -or -not (Test-Path -LiteralPath "$path.writing")) { return $null }
        } else {
            try {
                $text = Read-SharedText -Path $path
                return ($text | ConvertFrom-Json)
            } catch {
                if ((Get-Date) -gt $deadline) { throw "run.json at $Root could not be read for ${RetrySeconds}s: $($_.Exception.Message)" }
            }
        }
        Start-Sleep -Milliseconds 50
    }
}

function Write-RunJson {
    param([Parameter(Mandatory)] [string] $Root, [Parameter(Mandatory)] $Manifest, [int] $RetrySeconds = $script:RunJsonRetrySeconds)
    $path = Join-Path $Root 'run.json'
    $text = $Manifest | ConvertTo-Json -Depth 6
    # A name of this writer's own, and a marker that a write is under way
    # (Read-RunJson's absent-for-a-moment case).
    $temp = "$path.$PID.tmp"
    $marker = "$path.writing"
    [System.IO.File]::WriteAllText($temp, $text, (New-Object System.Text.UTF8Encoding($false)))
    [System.IO.File]::WriteAllText($marker, "$PID")
    try {
        $deadline = (Get-Date).AddSeconds($RetrySeconds)
        while ($true) {
            try {
                if (Test-Path -LiteralPath $path) {
                    # No backup file. [NullString]::Value, not $null, which
                    # PowerShell hands a .NET string parameter as "", a path
                    # Replace refuses.
                    [System.IO.File]::Replace($temp, $path, [NullString]::Value)
                } else {
                    [System.IO.File]::Move($temp, $path)
                }
                return
            } catch {
                if ((Get-Date) -gt $deadline) {
                    throw "run.json at $Root could not be written for ${RetrySeconds}s: $($_.Exception.Message)"
                }
                Start-Sleep -Milliseconds 50
            }
        }
    } finally {
        Remove-Item -LiteralPath $temp -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $marker -ErrorAction SilentlyContinue
    }
}

function Set-RunField {
    param([Parameter(Mandatory)] [string] $Root, [Parameter(Mandatory)] [hashtable] $Fields, [int] $RetrySeconds = $script:RunJsonRetrySeconds)
    $manifest = Read-RunJson -Root $Root -RetrySeconds $RetrySeconds
    if ($null -eq $manifest) { $manifest = New-Object PSObject }
    foreach ($name in $Fields.Keys) {
        $manifest | Add-Member -NotePropertyName $name -NotePropertyValue $Fields[$name] -Force
    }
    Write-RunJson -Root $Root -Manifest $manifest -RetrySeconds $RetrySeconds
}

function Read-SharedLines {
    # A file's lines as plain strings, read while another process may still be
    # writing it. Not Get-Content, whose strings carry PSPath, PSDrive and the
    # rest as note properties, which ConvertTo-Json then writes into run.json
    # as hundreds of kilobytes of PowerShell's own objects.
    param([Parameter(Mandatory)] [string] $Path)
    if (-not (Test-Path $Path)) { return @() }
    $share = [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
    try {
        $stream = New-Object System.IO.FileStream($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, $share)
    } catch {
        return @()
    }
    try {
        $reader = New-Object System.IO.StreamReader($stream)
        $lines = New-Object System.Collections.Generic.List[string]
        while ($null -ne ($line = $reader.ReadLine())) { $lines.Add($line) }
        return $lines.ToArray()
    } finally {
        $stream.Dispose()
    }
}

function Get-UtcStamp {
    return (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
}
