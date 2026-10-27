<#
.SYNOPSIS
    Append a line to a log that other processes are reading. Dot-sourced,
    never run.

.DESCRIPTION
    The live harness's logs are read while they are written: the status view,
    stop-live.ps1 and the tests all read supervisor-live.log, check-live.log
    and verify.log while the supervisor and the verify loop append to them.
    Add-Content cannot be used for that. In Windows PowerShell 5.1 it opens
    the file without sharing it with a reader, so a reader that has it open at
    that moment makes the append fail, and the line is lost. Measured in
    ops\test-live.ps1: across four runs one lost `live exited 0` and another
    lost `live pid ... started`, which is the line that counts the run's
    attempts, so a run would have read as never started.

    So the append opens the file sharing read, write and delete, which no
    reader here refuses, and retries for a few seconds against anything else
    that holds it. UTF-8, no byte order mark on a file it creates.

    Shared by the live harness and verify-loop.ps1, whose verify.log the
    record harness also writes: a line it appends is the same line.
#>

function Add-SharedLine {
    # True when the line was appended; false when the file stayed refused for
    # the whole retry, so a caller can still say the line somewhere.
    param([Parameter(Mandatory)] [string] $Path, [Parameter(Mandatory)] [AllowEmptyString()] [string] $Line, [int] $RetrySeconds = 5)
    $share = [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
    $bytes = (New-Object System.Text.UTF8Encoding($false)).GetBytes($Line + "`r`n")
    $deadline = (Get-Date).AddSeconds($RetrySeconds)
    while ($true) {
        try {
            $stream = New-Object System.IO.FileStream($Path, [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, $share)
            try {
                $stream.Write($bytes, 0, $bytes.Length)
            } finally {
                $stream.Dispose()
            }
            return $true
        } catch {
            if ((Get-Date) -gt $deadline) { return $false }
            Start-Sleep -Milliseconds 20
        }
    }
}
