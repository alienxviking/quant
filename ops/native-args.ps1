<#
.SYNOPSIS
    One rule for writing an argument onto a Windows command line. Dot-sourced,
    never run.

.DESCRIPTION
    Start-Process joins an -ArgumentList array with spaces and quotes nothing
    (CLAUDE.md, the PowerShell 5.1 traps), so every script that starts a
    process quotes its arguments itself. Wrapping each one in plain quotes,
    as they all used to, is not enough. The program at the other end splits
    its command line by the C runtime's rule (CommandLineToArgvW, which
    powershell.exe and every Rust binary here follow), and under that rule a
    backslash in front of a quote escapes it. So a root given as
    C:\runs\one\ became "C:\runs\one\", whose last two characters read as a
    literal quote: the supervisor was handed a root ending in a quote and died
    before it wrote its log, and every argument after it ran together into
    one.

    The rule, from the C runtime's documentation: backslashes are literal
    unless they come before a quote. Before a quote, 2n backslashes stand for
    n of them and an odd count escapes the quote. So a run of backslashes is
    doubled where a quote follows it, the closing quote included, and a quote
    inside the value is escaped.

    Shared by the live harness and verify-loop.ps1, which the record harness
    also runs: for a value with no quote, no space and no trailing backslash,
    which is every argument the record harness passes, the result is the same
    characters in quotes as before.
#>

function ConvertTo-NativeArgument {
    param([AllowEmptyString()] [string] $Value)
    $out = New-Object System.Text.StringBuilder
    [void]$out.Append('"')
    $slashes = 0
    foreach ($c in $Value.ToCharArray()) {
        if ($c -eq [char]'\') { $slashes++; continue }
        if ($c -eq [char]'"') {
            [void]$out.Append([char]'\', (2 * $slashes) + 1)
            [void]$out.Append([char]'"')
        } else {
            if ($slashes -gt 0) { [void]$out.Append([char]'\', $slashes) }
            [void]$out.Append($c)
        }
        $slashes = 0
    }
    # The closing quote follows whatever backslashes end the value.
    if ($slashes -gt 0) { [void]$out.Append([char]'\', 2 * $slashes) }
    [void]$out.Append('"')
    return $out.ToString()
}

function ConvertTo-NativeArguments {
    # Each argument by the rule above, as an array for -ArgumentList.
    param([AllowEmptyCollection()] [string[]] $Values)
    $quoted = @()
    foreach ($value in @($Values)) { $quoted += (ConvertTo-NativeArgument $value) }
    return , $quoted
}
