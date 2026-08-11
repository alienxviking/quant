<#
.SYNOPSIS
    Keep one symbol recording until a deadline, restarting it if it dies.

.DESCRIPTION
    "Runs seven days unattended" is a statement about the *system*, not about one
    process, and the two are deliberately different things.

    The recorder exits only on a condition it has decided is fatal -- the writer
    thread gone, a file it cannot create. That is the right behaviour: a process
    that knows it can no longer do its job should stop rather than carry on
    pretending. Restarting it is a separate concern with a separate lifetime, and
    it belongs out here.

    Nothing is lost across a restart, because the format was built for it. A new
    process takes a new session id, writes to its own files, and puts a
    `RecorderRestart` gap in the first frame -- so the capture states plainly that
    coverage was interrupted rather than quietly abutting two runs and looking
    continuous. The verifier reads that as the explanation for the discontinuity
    it is about to find.

    On Linux this whole script is `Restart=always` in a systemd unit. It exists
    because this machine is Windows, not because supervision wants to be bespoke.

.PARAMETER Symbol
    The venue's symbol, e.g. BTCUSDT.

.PARAMETER Root
    Capture root. The recorder appends `raw/exchange=.../` beneath it.

.PARAMETER EndAt
    UTC time to stop at. The remaining seconds are handed to each attempt, so the
    run ends on schedule regardless of how many restarts it took to get there.

.PARAMETER LogDir
    Where per-attempt logs go.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]   $Symbol,
    [Parameter(Mandatory)] [string]   $Root,
    [Parameter(Mandatory)] [datetime] $EndAt,
    [Parameter(Mandatory)] [string]   $LogDir
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Resolve-Path (Join-Path $here '..')
$exe  = Join-Path $repo 'target\release\record.exe'

if (-not (Test-Path $exe)) {
    Write-Error "no release binary at $exe -- run ops\preflight.ps1 first"
    exit 1
}
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }

# Backoff between attempts. A recorder that dies instantly and repeatedly is
# misconfigured, and hammering it would bury the reason in a scrolling log while
# hitting the venue's connection limits on the way. It resets after any attempt
# that survived a while, so an unlucky night does not leave a healthy symbol
# waiting minutes to come back.
$backoffSeconds    = 2
$maxBackoffSeconds = 120
$healthyRunSeconds = 300

$attempt = 0
$journal = Join-Path $LogDir "supervisor-$Symbol.log"

function Note {
    param([string] $Message)
    $line = "{0} {1}" -f (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ'), $Message
    Write-Host $line
    Add-Content -Path $journal -Value $line -Encoding utf8
}

Note "supervising $Symbol until $($EndAt.ToUniversalTime().ToString('u')) into $Root"

while ((Get-Date).ToUniversalTime() -lt $EndAt.ToUniversalTime()) {
    $remaining = [int]([math]::Floor(($EndAt.ToUniversalTime() - (Get-Date).ToUniversalTime()).TotalSeconds))
    if ($remaining -le 5) { break }

    $attempt++
    $stamp  = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')
    $out    = Join-Path $LogDir "$Symbol-$stamp.log"
    $err    = Join-Path $LogDir "$Symbol-$stamp.err"
    Note "attempt $attempt starting, ${remaining}s remaining, log $([System.IO.Path]::GetFileName($out))"

    $startedAt = Get-Date

    # Start-Process rather than a pipeline: PowerShell 5.1 wraps a native
    # command's stderr in ErrorRecords when redirected inline, which mangles the
    # log and sets $? to false even on a clean exit. Separate files per attempt,
    # because Start-Process truncates what it redirects to and a restart must not
    # erase the log that explains why the previous attempt ended.
    $process = Start-Process -FilePath $exe `
        -ArgumentList @($Symbol, $Root, $remaining) `
        -RedirectStandardOutput $out `
        -RedirectStandardError $err `
        -NoNewWindow -Wait -PassThru

    $ranFor = [int]((Get-Date) - $startedAt).TotalSeconds
    if ($process.ExitCode -eq 0) {
        Note "attempt $attempt exited cleanly after ${ranFor}s"
        # A clean exit means the duration limit was reached, which is the run
        # finishing rather than failing.
        if ((Get-Date).ToUniversalTime() -ge $EndAt.ToUniversalTime().AddSeconds(-30)) { break }
    } else {
        Note "attempt $attempt exited $($process.ExitCode) after ${ranFor}s -- see $([System.IO.Path]::GetFileName($err))"
    }

    if ($ranFor -ge $healthyRunSeconds) { $backoffSeconds = 2 }

    $remaining = [int]([math]::Floor(($EndAt.ToUniversalTime() - (Get-Date).ToUniversalTime()).TotalSeconds))
    if ($remaining -le 5) { break }

    Note "restarting in ${backoffSeconds}s"
    Start-Sleep -Seconds ([math]::Min($backoffSeconds, $remaining))
    $backoffSeconds = [math]::Min($backoffSeconds * 2, $maxBackoffSeconds)
}

Note "supervisor for $Symbol finished after $attempt attempt(s)"
