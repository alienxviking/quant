<#
.SYNOPSIS
    The clock checks the harness relies on, both ways, against a local clock
    that is right and one that is five seconds ahead.

.DESCRIPTION
    venue-check --clock is what preflight.ps1, fix-clock.ps1 and
    preflight-live.ps1 ask for the host's offset from the venue, and
    Read-VenueClock (venue-clock.ps1) is how the two record scripts read its
    answer. Neither may contact Binance from a test, so this serves
    GET /api/v3/time itself, from a listener on 127.0.0.1 port 0 in a
    background runspace, answering with this host's clock plus a skew, and
    points the real venue-check at it with --rest. It waits on observable
    facts only: the port the listener says it bound, and each process's exit.

    Both sides of the boundary: a venue on this host's clock is within a
    second and READY; one five seconds behind this host reads as this host
    five seconds ahead, NOT READY, and the offset Read-VenueClock reports
    carries venue-check's sign (positive: this host is ahead).

    And a venue-check that answers with no verdict, as one built before
    --clock does: not ready, with its exit code and first line as the detail,
    which is what fix-clock.ps1 prints.

    Needs target\release\venue-check.exe. Safe to run several copies at once:
    every listener is on a port of its own and nothing here is shared.

    Exit 0 when every check passes, 1 when one fails, 2 when it cannot run.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = (Resolve-Path (Join-Path $here '..')).Path
$exe  = Join-Path $repo 'target\release\venue-check.exe'
. (Join-Path $here 'venue-clock.ps1')

if (-not (Test-Path $exe)) {
    Write-Host "cannot run: no $exe (cargo build --release -p quant-binance --bin venue-check)"
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

# A clock server: GET /api/v3/time answered with this host's milliseconds plus
# $Skew, Connection: close, for as many requests as come within its lifetime.
$serve = {
    param([long] $Skew, [int] $LifetimeSeconds, $Shared)
    $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $Shared.Port = $listener.LocalEndpoint.Port
    $deadline = (Get-Date).AddSeconds($LifetimeSeconds)
    try {
        while ((Get-Date) -lt $deadline -and -not $Shared.Stop) {
            if (-not $listener.Pending()) { Start-Sleep -Milliseconds 5; continue }
            $client = $listener.AcceptTcpClient()
            try {
                $client.ReceiveTimeout = 5000
                $stream = $client.GetStream()
                $buffer = New-Object byte[] 4096
                $head = ''
                while ($head.IndexOf("`r`n`r`n") -lt 0) {
                    $n = $stream.Read($buffer, 0, $buffer.Length)
                    if ($n -le 0) { break }
                    $head += [System.Text.Encoding]::ASCII.GetString($buffer, 0, $n)
                }
                $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() + $Skew
                $body = '{"serverTime":' + $now + '}'
                $reply = "HTTP/1.1 200 OK`r`nContent-Type: application/json`r`nContent-Length: $($body.Length)`r`nConnection: close`r`n`r`n$body"
                $bytes = [System.Text.Encoding]::ASCII.GetBytes($reply)
                $stream.Write($bytes, 0, $bytes.Length)
                $stream.Flush()
                $Shared.Served++
            } catch {
            } finally {
                $client.Close()
            }
        }
    } finally {
        $listener.Stop()
    }
}

function Start-ClockServer {
    param([long] $Skew)
    $shared = [hashtable]::Synchronized(@{ Port = 0; Served = 0; Stop = $false })
    $ps = [powershell]::Create()
    [void]$ps.AddScript($serve).AddArgument($Skew).AddArgument(60).AddArgument($shared)
    $handle = $ps.BeginInvoke()
    $deadline = (Get-Date).AddSeconds(20)
    while ($shared.Port -eq 0) {
        if ((Get-Date) -gt $deadline -or $handle.IsCompleted) { throw 'the clock server did not bind' }
        Start-Sleep -Milliseconds 10
    }
    return @{ ps = $ps; handle = $handle; shared = $shared; base = "http://127.0.0.1:$($shared.Port)" }
}

function Stop-ClockServer {
    param($Server)
    $Server.shared.Stop = $true
    try { [void]$Server.ps.EndInvoke($Server.handle) } catch { }
    $Server.ps.Dispose()
}

Write-Host 'venue-check --clock against a local clock'

# 1. On time: READY, and an offset well inside a second.
$server = Start-ClockServer -Skew 0
try {
    $clock = Read-VenueClock -Exe $exe -Base $server.base
} finally {
    Stop-ClockServer $server
}
Check 'a venue on this host''s clock is ready' $true $clock.ready
Check 'and venue-check exits 0' 0 $clock.code
Check 'an offset is read' $true ($null -ne $clock.offset_ms)
Check 'well inside a second' $true ($null -ne $clock.offset_ms -and [math]::Abs($clock.offset_ms) -lt 500)
Check 'every reading was served' $true ($server.shared.Served -ge 5)

# 2. Five seconds out: NOT READY, with the sign that says which way.
$server = Start-ClockServer -Skew -5000
try {
    $clock = Read-VenueClock -Exe $exe -Base $server.base
} finally {
    Stop-ClockServer $server
}
Check 'a venue five seconds behind this host is not ready' $false $clock.ready
Check 'and venue-check exits 1' 1 $clock.code
Check 'the offset reads this host as ahead, near +5000ms' $true ($null -ne $clock.offset_ms -and $clock.offset_ms -gt 4500 -and $clock.offset_ms -lt 5500)
Check 'and the verdict says NOT READY' $true ($clock.verdict -match 'NOT READY')

# 3. Nothing listening: no offset, not ready, and no reading invented.
$probe = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, 0)
$probe.Start(); $deadPort = $probe.LocalEndpoint.Port; $probe.Stop()
$clock = Read-VenueClock -Exe $exe -Base "http://127.0.0.1:$deadPort"
Check 'a venue that does not answer is not ready' $false $clock.ready
Check 'and gives no offset' $true ($null -eq $clock.offset_ms)

# 4. A venue-check that does not know --clock, as one built before it did:
#    it prints its usage and no verdict. The detail must still say what
#    happened, with the code, rather than nothing at all.
$old = Join-Path ([System.IO.Path]::GetTempPath()) ("quant-old-venue-check-{0}-{1}" -f $PID, [guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Path $old -Force | Out-Null
try {
    $oldExe = Join-Path $old 'venue-check.exe'
    Add-Type -OutputAssembly $oldExe -OutputType ConsoleApplication -TypeDefinition @'
public static class OldVenueCheck {
    public static int Main(string[] args) {
        System.Console.Error.WriteLine("venue-check: an argument it does not take");
        return 2;
    }
}
'@
    $clock = Read-VenueClock -Exe $oldExe
} finally {
    Remove-Item -Recurse -Force -LiteralPath $old -ErrorAction SilentlyContinue
}
Check 'a venue-check without --clock is not ready' $false $clock.ready
Check 'and the detail says it exited 2 without a verdict, and what it said' $true ($clock.detail -match 'exited 2 without a verdict: venue-check: an argument it does not take')

Write-Host ""
Write-Host "$script:pass passed, $script:fail failed"
if ($script:fail -gt 0) { exit 1 }
exit 0
