param(
    [Parameter(Mandatory = $true)]
    [string] $LocalRef,
    [Parameter(Mandatory = $true)]
    [string] $LocalSha,
    [Parameter(Mandatory = $true)]
    [string] $RemoteRef,
    [Parameter(Mandatory = $true)]
    [string] $RemoteSha
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Invoke-GitCapture {
    param([Parameter(Mandatory = $true)][string[]] $Arguments)

    $output = & git -C $repoRoot @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        $joined = $Arguments -join ' '
        throw "git $joined failed`n$output"
    }

    return [string]::Join("`n", @($output))
}

$forbiddenPatterns = @(
    'logs/**',
    'ci-artifacts/**',
    'playback-proof-artifacts/**',
    'login_page.html',
    '.release-please-manifest.json'
)

if ($LocalRef -notlike 'refs/heads/*') {
    return
}

$zeroSha = '0000000000000000000000000000000000000000'
if ($LocalSha -eq $zeroSha) {
    return
}

$sameTarget = ($RemoteSha -ne $zeroSha) -and ($RemoteSha -eq $LocalSha)
if ($sameTarget) {
    return
}

$rangeArgs = @('diff-tree', '--no-commit-id', '--name-status', '-r')
if ($RemoteSha -eq $zeroSha -or [string]::IsNullOrWhiteSpace($RemoteSha)) {
    $rangeArgs += $LocalSha
}
else {
    $rangeArgs += @("$RemoteSha..$LocalSha")
}

$entriesText = Invoke-GitCapture -Arguments $rangeArgs
$entries = @($entriesText -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
if ($entries.Count -eq 0) {
    return
}

$violations = New-Object System.Collections.Generic.List[string]
$blockedStatuses = @('A', 'C', 'M', 'R', 'T')
foreach ($entry in $entries) {
    $parts = @($entry -split "`t" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($parts.Count -lt 2) {
        continue
    }

    $status = $parts[0]
    $path = $parts[-1]
    if ($blockedStatuses -notcontains $status.Substring(0, 1)) {
        continue
    }

    foreach ($pattern in $forbiddenPatterns) {
        if ($path -like $pattern) {
            $violations.Add($path)
            break
        }
    }
}

if ($violations.Count -gt 0) {
    $joined = ($violations | Sort-Object -Unique) -join ', '
    throw "Push blocked: outbound commits include local-only tracking or generated/runtime paths that must never be published: $joined"
}
