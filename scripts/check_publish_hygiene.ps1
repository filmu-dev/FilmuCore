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

    $pathsText = Invoke-GitCapture -Arguments @('diff', '--name-only', "$Base...$Head")
    if ([string]::IsNullOrWhiteSpace($pathsText)) {
        return @()
    }

    return @(
        $pathsText -split "`r?`n" |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            ForEach-Object { $_.Trim() }
    )
}

function Assert-NoDuplicateChangelogSections {
    param([Parameter(Mandatory = $true)][string] $Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    $versionCounts = @{}
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match '^## \[(?<version>\d+\.\d+\.\d+)\]') {
            $version = $Matches.version
            if ($versionCounts.ContainsKey($version)) {
                $versionCounts[$version] += 1
            }
            else {
                $versionCounts[$version] = 1
            }
        }
    }

    $duplicates = @(
        $versionCounts.GetEnumerator() |
            Where-Object { $_.Value -gt 1 } |
            Sort-Object Name
    )
    if ($duplicates.Count -gt 0) {
        $formatted = @(
            $duplicates | ForEach-Object {
                "$($_.Name) ($($_.Value)x)"
            }
        ) -join ', '
        throw "Duplicate release sections found in CHANGELOG.md: $formatted"
    }
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
if ([string]::IsNullOrWhiteSpace($branchName)) {
    $branchName = (Invoke-GitCapture -Arguments @('branch', '--show-current')).Trim()
}

$isReleasePleaseBranch = $branchName -like 'release-please--*'

if (-not $isReleasePleaseBranch) {
    $forbiddenPatterns = @(
        'logs/**',
        'ci-artifacts/**',
        'playback-proof-artifacts/**',
        'docs/**',
        'README.md',
        'CHANGELOG.md',
        'QUICK_START.md',
        'WINDOWS_README.md',
        'LINUX_UNIX_README.md',
        'login_page.html',
        '.release-please-manifest.json'
    )

    $changedPaths = Get-ChangedPaths -Base $BaseRef -Head $HeadRef
    $violations = New-Object System.Collections.Generic.List[string]
    foreach ($path in $changedPaths) {
        foreach ($pattern in $forbiddenPatterns) {
            if ($path -like $pattern) {
                $violations.Add($path)
                break
            }
        }
    }

    if ($violations.Count -gt 0) {
        $joined = ($violations | Sort-Object -Unique) -join ', '
        throw "Publish hygiene failed for branch '$branchName': forbidden paths changed: $joined"
    }
}

Assert-NoDuplicateChangelogSections -Path (Join-Path $repoRoot 'CHANGELOG.md')
Write-Output "[publish-hygiene] PASS (base=$BaseRef head=$HeadRef release_please=$isReleasePleaseBranch)"
