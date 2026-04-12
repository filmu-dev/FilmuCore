param(
    [string] $HooksPath = '.githooks'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Invoke-GitCapture {
    param([Parameter(Mandatory = $true)][string[]] $Arguments)

    $output = & git @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        $joined = $Arguments -join ' '
        throw "git $joined failed`n$output"
    }

    return [string]::Join("`n", @($output))
}

$repoRoot = (Invoke-GitCapture -Arguments @('rev-parse', '--show-toplevel')).Trim()
if ([string]::IsNullOrWhiteSpace($repoRoot)) {
    throw 'Could not resolve repository root.'
}

$resolvedHooksPath = Join-Path $repoRoot $HooksPath
if (-not (Test-Path -LiteralPath $resolvedHooksPath)) {
    throw "Hooks path '$resolvedHooksPath' does not exist."
}

Invoke-GitCapture -Arguments @('config', 'core.hooksPath', $HooksPath) | Out-Null

Write-Output "Configured git hooks path: $HooksPath"
Write-Output 'Commit messages will now be normalized to conventional prefixes automatically.'
Write-Output 'Pre-push will now block forbidden generated artifacts and run branch hygiene automatically.'
