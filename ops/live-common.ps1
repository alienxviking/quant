<#
.SYNOPSIS
    What the live harness's scripts share. Dot-sourced, never run.

.DESCRIPTION
    Four things more than one live script needs, kept once so that the
    supervisor, the scripts that start, stop and watch a run, and the tests
    cannot disagree about them:

    - what each of the live binary's exit codes means, in words;
    - the one-live-run lock, and how to ask whether a run holds it;
    - holding this host awake for a supervisor's lifetime;
    - reading and writing the run's run.json.

    Windows PowerShell 5.1 throughout: nothing here may need pwsh.
#>

# --- what an exit code means ------------------------------------------------
# The live binary's own codes (crates/quant-backtest/src/bin/live.rs, "Exit
# codes"). Every exit ends a live run: the owner ruled out an automatic restart
# on 2026-10-26 (docs/live-run.md, "Where it runs"), so the harness's whole job
# at the end is to say which ending this was, in words a person reads at the
# end of an afternoon, and never to try again.
function Get-LiveOutcome {
    param([Parameter(Mandatory)] [int] $Code)
    switch ($Code) {
        0 { return [ordered]@{ code = 0; outcome = 'stopped'
                meaning = 'traded and stopped cleanly: the cancels went, the closing lines are written and both captures are sealed' } }
        1 { return [ordered]@{ code = 1; outcome = 'failed'
                meaning = 'the run could not go on: it could not start, a capture ended in error, or the journal failed, after which every order was withheld and no closing line written' } }
        2 { return [ordered]@{ code = 2; outcome = 'bad-argument'
                meaning = 'the binary refused its arguments and nothing ran: nothing was recorded and nothing traded' } }
        3 { return [ordered]@{ code = 3; outcome = 'recorded-only'
                meaning = 'a gate refused: the market was recorded and nothing was traded. Not a failure, and a restart would meet the same refusal' } }
        default { return [ordered]@{ code = $Code; outcome = 'killed'
                meaning = "killed or crashed (exit $Code): no clean stop, so the cancels may not have gone, the journal has no closing lines, the captures' last segments have no trailer, and the next run's last-session gate will refuse to trade until a person has looked" } }
    }
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

# --- keeping the host awake -------------------------------------------------
# The Windows half of caffeinate -dimsu -w $$: SetThreadExecutionState with
# ES_CONTINUOUS | ES_SYSTEM_REQUIRED asks Windows not to sleep for idleness
# while the calling thread lives, and changes no setting at all. Nothing
# persists: it is released when the thread asks (Disable-KeepAwake), and by
# Windows when the thread or the process ends, a supervisor killed included.
#
# What it does not do, and the run procedure says so: it does not stop sleep
# that a person asks for, a closed lid, or a restart Windows Update forces.
# Those are the operator's to prevent for an afternoon (docs/live-run.md
# section 7).
$script:EsContinuous     = [uint32]2147483648   # 0x80000000
$script:EsSystemRequired = [uint32]1            # 0x00000001

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

function Enable-KeepAwake {
    # Returns what this thread now holds, read back rather than assumed: the
    # second call asks for the same state and returns the one it replaced.
    # A failed call returns 0, which the read-back then shows as a state
    # without ES_SYSTEM_REQUIRED: the caller checks the bit, not the call.
    $flags = $script:EsContinuous -bor $script:EsSystemRequired
    [void][QuantKeepAwake]::SetThreadExecutionState($flags)
    return [QuantKeepAwake]::SetThreadExecutionState($flags)
}

function Disable-KeepAwake {
    # Returns what this thread held until now: ES_CONTINUOUS|ES_SYSTEM_REQUIRED
    # when Enable-KeepAwake ran on this same thread, which is the check that
    # the request being released is the one that was made.
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
# by the supervisor: one writer, so no two scripts race to rewrite it. UTF-8
# without a byte order mark, so any reader takes it.
function Read-RunJson {
    param([Parameter(Mandatory)] [string] $Root)
    $path = Join-Path $Root 'run.json'
    if (-not (Test-Path $path)) { return $null }
    return (Get-Content -Path $path -Raw | ConvertFrom-Json)
}

function Write-RunJson {
    param([Parameter(Mandatory)] [string] $Root, [Parameter(Mandatory)] $Manifest)
    $path = Join-Path $Root 'run.json'
    $text = $Manifest | ConvertTo-Json -Depth 6
    $temp = "$path.tmp"
    [System.IO.File]::WriteAllText($temp, $text, (New-Object System.Text.UTF8Encoding($false)))
    Move-Item -Path $temp -Destination $path -Force
}

function Set-RunField {
    param([Parameter(Mandatory)] [string] $Root, [Parameter(Mandatory)] [hashtable] $Fields)
    $manifest = Read-RunJson -Root $Root
    if ($null -eq $manifest) { $manifest = New-Object PSObject }
    foreach ($name in $Fields.Keys) {
        $manifest | Add-Member -NotePropertyName $name -NotePropertyValue $Fields[$name] -Force
    }
    Write-RunJson -Root $Root -Manifest $manifest
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
