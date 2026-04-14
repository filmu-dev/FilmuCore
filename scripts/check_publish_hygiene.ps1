param(
    [string] $BaseRef = '',
    [string] $HeadRef = 'HEAD'
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

function Test-GitRefExists {
    param([Parameter(Mandatory = $true)][string] $Ref)

    $null = & git -C $repoRoot rev-parse --verify --quiet $Ref 2>$null
    return ($LASTEXITCODE -eq 0)
}

function Get-ChangedPaths {
    param(
        [Parameter(Mandatory = $true)][string] $Base,
        [Parameter(Mandatory = $true)][string] $Head
    )

    $pathsText = Invoke-GitCapture -Arguments @('diff', '--name-only', '--diff-filter=ACMRTUXB', "$Base...$Head")
    if ([string]::IsNullOrWhiteSpace($pathsText)) {
        return @()
    }

    return @(
        $pathsText -split "`r?`n" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            ForEach-Object { $_.Trim() }
    )
}

if ([string]::IsNullOrWhiteSpace($BaseRef)) {
    if (-not [string]::IsNullOrWhiteSpace([string] $env:GITHUB_BASE_REF)) {
        $BaseRef = "origin/$($env:GITHUB_BASE_REF)"
    }
    else {
        $BaseRef = 'origin/main'
    }
}

if (-not (Test-GitRefExists -Ref $BaseRef)) {
    throw "Base ref '$BaseRef' does not exist. Fetch refs first, or pass -BaseRef explicitly."
}

$branchName = ''
if (-not [string]::IsNullOrWhiteSpace([string] $env:GITHUB_HEAD_REF)) {
    $branchName = [string] $env:GITHUB_HEAD_REF
}

function Test-PathMatchesPattern {
    param(
        [Parameter(Mandatory = $true)][string] $Path,
        [Parameter(Mandatory = $true)][string] $Pattern
    )

    return ($Path -like $Pattern) -or ($Path -replace '\\', '/' -like $Pattern)
}
if ([string]::IsNullOrWhiteSpace($branchName)) {
    $branchName = (Invoke-GitCapture -Arguments @('branch', '--show-current')).Trim()
}

$isReleasePleaseBranch = $branchName -like 'release-please--*'
$changedPaths = Get-ChangedPaths -Base $BaseRef -Head $HeadRef
$alwaysForbiddenPatterns = @(
    'logs/**',
    'ci-artifacts/**',
    'playback-proof-artifacts/**',
    '*.md',
    'login_page.html'
)
$releaseManagedPaths = @(
    '.release-please-manifest.json',
    'package.json',
    'pyproject.toml',
    'rust/filmuvfs/Cargo.toml'
)

$violations = New-Object System.Collections.Generic.List[string]
foreach ($path in $changedPaths) {
    foreach ($pattern in $alwaysForbiddenPatterns) {
        if (Test-PathMatchesPattern -Path $path -Pattern $pattern) {
            $violations.Add($path)
            break
        }
    }
}

if ($isReleasePleaseBranch) {
    foreach ($path in $changedPaths) {
        if ($path -notin $releaseManagedPaths) {
            $violations.Add($path)
        }
    }
}
else {
    foreach ($path in $changedPaths) {
        if ($path -eq '.release-please-manifest.json') {
            $violations.Add($path)
        }
    }
}

if ($violations.Count -gt 0) {
    $joined = ($violations | Sort-Object -Unique) -join ', '
    throw "Publish hygiene failed for branch '$branchName': forbidden paths changed: $joined"
}

Write-Output "[publish-hygiene] PASS (base=$BaseRef head=$HeadRef release_please=$isReleasePleaseBranch)"
