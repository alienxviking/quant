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

.PARAMETER Root
    Capture root to verify.

.PARAMETER EndAt
    UTC time to stop at.

.PARAMETER IntervalHours
    How often to check.

.PARAMETER LogDir
    Where the verification log goes.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]   $Root,
    [Parameter(Mandatory)] [datetime] $EndAt,
    [Parameter(Mandatory)] [string]   $LogDir,
    [int] $IntervalHours = 6,
    # How long to wait before the first check. A misconfiguration shows up in the
    # first few minutes, and that is the failure most worth catching early.
    [int] $FirstCheckSeconds = 300
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
Note "verifying $Root every ${IntervalHours}h until $($EndAt.ToUniversalTime().ToString('u')), reconcile=$useReconcile"

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

    $remaining = ($EndAt.ToUniversalTime() - (Get-Date).ToUniversalTime()).TotalSeconds
    if ($remaining -le 60) { break }
    Start-Sleep -Seconds ([math]::Min($IntervalHours * 3600, $remaining))
}

Note "verification loop finished"
