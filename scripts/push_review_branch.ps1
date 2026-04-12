param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string] $RemoteBranch,
    [string] $Remote = 'origin',
    [string] $BaseBranch = 'main',
    [switch] $NoFetch,
    [switch] $DryRun
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

$currentBranch = (Invoke-GitCapture -Arguments @('branch', '--show-current')).Trim()
if ([string]::IsNullOrWhiteSpace($currentBranch)) {
    throw 'Cannot push review branches from a detached HEAD.'
}
if ($currentBranch -ne 'main') {
    throw "Review branch pushes must come from local 'main'. Current branch is '$currentBranch'."
}

$hygieneArgs = @(
    '-NoProfile',
    '-File',
    (Join-Path $repoRoot 'scripts/check_branch_hygiene.ps1'),
    '-Branch',
    $currentBranch,
    '-ReviewBranch',
    $RemoteBranch,
    '-Remote',
    $Remote,
    '-BaseBranch',
    $BaseBranch
)
if ($NoFetch) {
    $hygieneArgs += '-NoFetch'
}

& pwsh @hygieneArgs
if ($LASTEXITCODE -ne 0) {
    throw 'Branch hygiene failed. Sync local main or use a fresh remote review branch before pushing.'
}

$result = [ordered]@{
    source_branch = $currentBranch
    remote_branch = $RemoteBranch
    base_branch = "$Remote/$BaseBranch"
    fetched_base = (-not $NoFetch)
    dry_run = [bool] $DryRun
}

if ($DryRun) {
    $result | ConvertTo-Json -Depth 4
    return
}

Invoke-GitCapture -Arguments @('push', $Remote, "HEAD:refs/heads/$RemoteBranch") | Out-Null
Write-Output "Pushed local '$currentBranch' to '$Remote/$RemoteBranch'."
