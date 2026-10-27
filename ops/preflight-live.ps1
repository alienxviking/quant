<#
.SYNOPSIS
    Check that this host can be trusted with a live run, before anything is
    started.

.DESCRIPTION
    The live binary has seven gates of its own, asked before its capture
    starts (crates/quant-backtest/src/live.rs), and this does not repeat them
    badly. Each check below is here for one of two reasons: it is something
    no gate can see, or it is a gate's question asked early, by the same code,
    so that a refusal costs a minute at the prompt instead of an afternoon of
    recording only.

    What the gates already cover, and so is not re-judged here:
      2  the instrument's rules from exchangeInfo;
      3  the run tag from the journal, and an untagged journal refused;
      4  a latch the journal carries (Diverged);
      5  a last session that did not stop cleanly or left orders working;
      6  a size whose floored exit would fall under the minimum notional;
      7  the baseline balances, and the credentials read once more.
    A refusal of any of them makes the run record only, exit 3, and the
    supervisor says so in words.

    Asked early, by the same code as the gate:
      the venue's checks (gate 1). This runs target\release\venue-check.exe,
      which calls the very functions gate 1 calls: the clock read the NTP way
      and round-trip corrected (read_clock, ClockSample::best_of), then the
      three signed requests: the account, the trade permission probe at the
      run's own --qty, and the BNB fee discount off. It replaces the naive
      now-minus-serverTime reading the record preflight took, and it signs,
      so it proves the key, the signature and the clock in one go. A host
      more than a second from the venue cannot trade at all.

    Not a gate's question at all:
      the build: the release binaries the harness runs, built now.
      one live run per host: a live supervisor or a live.exe already running.
      a clean capture root: raw\ and account\ both empty. verify walks both,
        so captures left by an earlier run would be counted in this run's
        verdict. -Resume allows it, on the record.
      the journal: the binary resumes any journal it finds, and a fresh run
        must not inherit one. -Resume asks for exactly that, on the record;
        a resumed journal is then judged by gates 3 to 5.
      disk, for the capture, prorated by the run's length.
      the key variables present, BINANCE_API_KEY and
        BINANCE_ED25519_PRIVATE_KEY, named and never printed. Presence is not
        proof; venue-check is the proof.
      the time service: w32time running, so the clock that passed now is
        still disciplined at the end of the afternoon. A blocker against a
        real venue; a note against a local stand-in, whose clock is this
        host's own.
      sleep: read only, an advisory. The supervisor holds the host awake.

    Blockers exit 1, advisories print and continue. -Force on start-live.ps1
    accepts the blockers and writes preflight=forced into run.json; it does
    not override the one-live-run rule, which start-live.ps1 enforces itself.

.PARAMETER Root
    The run's data root.

.PARAMETER Symbol
    The one instrument.

.PARAMETER Environment
    testnet or live, as the binary's --env.

.PARAMETER Qty
    The run's size, which the trade probe is sent at.

.PARAMETER Minutes
    The run's length, for the disk estimate.

.PARAMETER Rest
    A local stand-in's REST address, for a rehearsal: venue-check is pointed
    at it with --rest.

.PARAMETER Resume
    Continue the journal and the capture root an earlier session left.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Root,
    [Parameter(Mandatory)] [string] $Symbol,
    [Parameter(Mandatory)] [ValidateSet('testnet', 'live')] [string] $Environment,
    [Parameter(Mandatory)] [string] $Qty,
    [int]    $Minutes = 240,
    [string] $Rest,
    [switch] $Resume,
    # For the harness's own tests: everything but the build and the venue.
    [switch] $NoBuild,
    [switch] $NoVenue
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here 'live-common.ps1')
$repo = (Resolve-Path (Join-Path $here '..')).Path
$bin  = Join-Path $repo 'target\release'
$standIn = -not [string]::IsNullOrWhiteSpace($Rest)

# Budgeted as the record harness budgets a day of one symbol, prorated, with a
# floor: a live run records the same market streams plus an account stream a
# thousand times smaller.
$MegabytesPerSymbolPerDay = 1024
$FloorMegabytes = 1024

$blockers   = @()
$advisories = @()

function Report {
    param([string] $Name, [string] $Detail, [ValidateSet('ok', 'advisory', 'blocker')] [string] $Verdict)
    $mark = switch ($Verdict) { 'ok' { '  ok  ' } 'advisory' { ' note ' } 'blocker' { 'BLOCK ' } }
    Write-Host ("{0} {1,-22} {2}" -f $mark, $Name, $Detail)
    if ($Verdict -eq 'blocker')  { $script:blockers   += "$Name : $Detail" }
    if ($Verdict -eq 'advisory') { $script:advisories += "$Name : $Detail" }
}

Write-Host "preflight for a live run of $Symbol on $Environment, $Minutes minute(s)"
Write-Host "repo   $repo"
Write-Host "root   $Root"
if ($standIn) { Write-Host "venue  a local stand-in at $Rest (a rehearsal)" }
Write-Host ""

# --- the build under test ----------------------------------------------------
# The whole workspace's binaries, as preflight.sh builds them, and then the
# filesystem asked which of the ones this harness launches are there: a build
# that succeeded and a binary that is present are different claims.
$wanted = @('live', 'venue-check', 'verify', 'reconcile', 'runlog', 'replay-account')
if (-not $NoBuild) {
    $env:Path = "$env:USERPROFILE\.cargo\bin;$env:Path"
    $buildLog = Join-Path $env:TEMP "quant-preflight-live-build-$PID.log"
    # Redirected by cmd, not by PowerShell: under ErrorActionPreference Stop,
    # PowerShell 5.1 turns each line cargo writes to stderr, which is all of its
    # progress, into a terminating error; and Start-Process -Wait hung here
    # after cargo had exited (CLAUDE.md, the PowerShell 5.1 traps).
    Push-Location $repo
    try {
        cmd /c "cargo build --release --workspace --bins > `"$buildLog`" 2>&1"
        $buildExit = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    if ($buildExit -ne 0) {
        Report 'build' "cargo build --release failed, see $buildLog" 'blocker'
    }
}
$missing = @($wanted | Where-Object { -not (Test-Path (Join-Path $bin "$_.exe")) })
if ($missing.Count -gt 0) {
    Report 'build' "the harness launches these and they are not in target\release: $($missing -join ', ')" 'blocker'
} else {
    Report 'build' "release binaries present: $($wanted -join ', ')" 'ok'
}

# --- one live run per host ---------------------------------------------------
$others = Get-LiveProcesses
if (Test-LiveLockHeld) {
    Report 'one live run' 'a live supervisor is running on this host: one symbol, one process, one account' 'blocker'
} elseif ($others.Count -gt 0) {
    Report 'one live run' "a live.exe is running (pid $(($others | ForEach-Object { $_.Id }) -join ', ')): one account, one process" 'blocker'
} else {
    Report 'one live run' 'no other live run on this host' 'ok'
}

# --- a clean root, both capture trees ---------------------------------------
$existing = 0
foreach ($tree in @('raw', 'account')) {
    $path = Join-Path $Root $tree
    if (Test-Path $path) {
        $existing += @(Get-ChildItem -Path $path -Recurse -Filter 'part-*.bin.zst' -ErrorAction SilentlyContinue).Count
    }
}
if ($existing -eq 0) {
    Report 'capture root' 'raw\ and account\ hold no capture' 'ok'
} elseif ($Resume) {
    Report 'capture root' "$existing capture file(s) already here; -Resume: verify will cover them with this run's" 'advisory'
} else {
    Report 'capture root' "$existing capture file(s) already in raw\ or account\ -- this run's verdict would cover them too. Use a fresh -Root, or -Resume" 'blocker'
}

# --- the journal -------------------------------------------------------------
$journal = Join-Path $Root "live-$Symbol.jsonl"
if (Test-Path $journal) {
    if ($Resume) {
        Report 'journal' "resuming ${journal}: its run tag, its position and its risk tally carry on, and gates 3 to 5 judge it" 'advisory'
    } else {
        Report 'journal' "$journal already exists, and the binary resumes any journal it finds: its run tag, position and risk tally would be this run's. Use a fresh -Root, or -Resume" 'blocker'
    }
} elseif ($Resume) {
    Report 'journal' "-Resume, and there is no journal at $journal to resume" 'blocker'
} else {
    Report 'journal' 'none yet: a fresh journal and a fresh run tag' 'ok'
}

# --- disk --------------------------------------------------------------------
$probe = $Root
while (-not (Test-Path $probe)) {
    $parent = Split-Path -Parent $probe
    if ([string]::IsNullOrWhiteSpace($parent) -or $parent -eq $probe) { $probe = $repo; break }
    $probe = $parent
}
$drive  = (Get-Item $probe).PSDrive
$needMb = [math]::Max($FloorMegabytes, [math]::Ceiling($MegabytesPerSymbolPerDay * $Minutes / 1440))
$freeGb = [math]::Round($drive.Free / 1GB, 1)
if ($drive.Free -lt ($needMb * 1MB)) {
    Report 'disk' "${freeGb}GB free on $($drive.Name):, need about ${needMb}MB" 'blocker'
} else {
    Report 'disk' "${freeGb}GB free on $($drive.Name):, budgeting ${needMb}MB" 'ok'
}

# --- the key variables, present -----------------------------------------------
# Names only, never any part of a value. Whitespace alone counts as unset, as
# credentials.rs reads it.
$keysPresent = $true
foreach ($var in @('BINANCE_API_KEY', 'BINANCE_ED25519_PRIVATE_KEY')) {
    $value = [Environment]::GetEnvironmentVariable($var)
    if ([string]::IsNullOrWhiteSpace($value)) {
        $keysPresent = $false
        $how = 'set it in this shell before starting'
        if ($var -eq 'BINANCE_ED25519_PRIVATE_KEY') {
            $how = "load it from a file outside the repository: `$env:$var = Get-Content -Raw `"`$env:USERPROFILE\.binance\ed25519.pem`" (venue-check --help says how to make one)"
            if (-not [string]::IsNullOrWhiteSpace($env:BINANCE_API_SECRET)) {
                $how += '. BINANCE_API_SECRET is set, and the live path no longer signs with an HMAC secret'
            }
        }
        Report 'credentials' "$var is not set -- $how" 'blocker'
    } else {
        Report 'credentials' "$var is set" 'ok'
    }
    $value = $null
}

# --- the venue's checks, as gate 1 asks them -----------------------------------
if ($NoVenue) {
    Report 'venue-check' 'skipped (-NoVenue, the harness tests)' 'advisory'
} elseif (-not $keysPresent) {
    Report 'venue-check' 'not run: the key variables above are missing' 'blocker'
} elseif (-not (Test-Path (Join-Path $bin 'venue-check.exe'))) {
    Report 'venue-check' 'not run: venue-check.exe is not built' 'blocker'
} else {
    $arguments = @('--symbol', $Symbol, '--qty', $Qty)
    if ($Environment -eq 'testnet') { $arguments += '--testnet' }
    if ($standIn) { $arguments += @('--rest', $Rest) }
    $out = Join-Path $env:TEMP "quant-preflight-venue-check-$PID.txt"
    $err = Join-Path $env:TEMP "quant-preflight-venue-check-$PID.err"
    $quoted = ConvertTo-NativeArguments $arguments
    $process = Start-Process -FilePath (Join-Path $bin 'venue-check.exe') -ArgumentList $quoted `
        -RedirectStandardOutput $out -RedirectStandardError $err -NoNewWindow -PassThru
    $null = $process.Handle
    $process.WaitForExit()
    # Its lines as it printed them: the clock with its round trip, then one
    # line per signed request. Nothing it prints holds any part of a private
    # key; the public half it prints is not secret, and is not repeated here.
    $said = @(Get-Content -Path $out -ErrorAction SilentlyContinue) + @(Get-Content -Path $err -ErrorAction SilentlyContinue)
    foreach ($line in $said) {
        if ($line -match '^(clock|account|permission|fees|verdict)\s') { Write-Host "         $line" }
    }
    $verdict = @($said | Where-Object { $_ -match '^verdict' }) -join ' '
    if ($process.ExitCode -eq 0) {
        Report 'venue-check' 'READY: clock, key, signature, permission to trade at this size, fees not in BNB' 'ok'
    } else {
        Report 'venue-check' "exit $($process.ExitCode): $verdict (full output in $out)" 'blocker'
    }
}

# --- the time service --------------------------------------------------------
$w32 = Get-Service w32time -ErrorAction SilentlyContinue
$serviceVerdict = 'blocker'
if ($standIn) { $serviceVerdict = 'advisory' }
if ($null -eq $w32) {
    Report 'time service' 'w32time not present' $serviceVerdict
} elseif ($w32.Status -ne 'Running') {
    Report 'time service' "w32time is $($w32.Status): the clock venue-check passed will drift through the run. As Administrator: ops\fix-clock.ps1" $serviceVerdict
} else {
    Report 'time service' 'w32time running' 'ok'
}

# --- sleep -------------------------------------------------------------------
try {
    $sleepAc = (powercfg /query SCHEME_CURRENT SUB_SLEEP STANDBYIDLE | Select-String 'Current AC Power Setting Index').ToString()
    if ($sleepAc -match '0x00000000') {
        Report 'sleep' 'does not sleep on AC; the supervisor also holds a keep-awake request' 'ok'
    } else {
        Report 'sleep' 'sleeps when idle on AC: the supervisor holds a keep-awake request for the run, which a closed lid or a forced restart still beats' 'advisory'
    }
} catch {
    Report 'sleep' 'could not read the power scheme; the supervisor holds a keep-awake request for the run' 'advisory'
}

Write-Host ""
if ($advisories.Count -gt 0) {
    Write-Host "advisories:"
    $advisories | ForEach-Object { Write-Host "  - $_" }
}
if ($blockers.Count -gt 0) {
    Write-Host ""
    Write-Host "BLOCKED. Fix these, or pass -Force to start-live.ps1 to accept them on the record:"
    $blockers | ForEach-Object { Write-Host "  - $_" }
    exit 1
}
Write-Host "ready."
exit 0
