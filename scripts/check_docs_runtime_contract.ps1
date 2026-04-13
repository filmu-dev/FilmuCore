param(
    [string] $RepoRoot = '',
    [string] $OutputPath = '',
    [switch] $AsJson
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
$RepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $RepoRoot 'playback-proof-artifacts\docs-runtime-contract.json'
}

function Add-Check {
    param(
        [System.Collections.Generic.List[object]] $Checks,
        [string] $Name,
        [bool] $Passed,
        [object] $Observed,
        [object] $Expected
    )

    $Checks.Add([pscustomobject]@{
        name = $Name
        passed = $Passed
        observed = $Observed
        expected = $Expected
    })
}

function Test-Regex {
    param(
        [Parameter(Mandatory = $true)][string] $Content,
        [Parameter(Mandatory = $true)][string] $Pattern
    )

    return [System.Text.RegularExpressions.Regex]::IsMatch(
        $Content,
        $Pattern,
        [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor
        [System.Text.RegularExpressions.RegexOptions]::Singleline
    )
}

function Invoke-GitCapture {
    param([Parameter(Mandatory = $true)][string[]] $Arguments)

    $output = & git -C $RepoRoot @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        return $null
    }

    return [string]::Join("`n", @($output))
}

function Get-TrackedDocChanges {
    $changed = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
    $candidates = @(
        @('diff', '--name-only', 'HEAD~1...HEAD'),
        @('diff', '--name-only', 'origin/main...HEAD'),
        @('diff', '--name-only', 'main...HEAD'),
        @('diff', '--name-only', 'HEAD')
    )

    foreach ($arguments in $candidates) {
        $output = Invoke-GitCapture -Arguments $arguments
        if ($null -eq $output) {
            continue
        }

        foreach ($line in ($output -split "`r?`n")) {
            $relative = $line.Trim()
            if (-not [string]::IsNullOrWhiteSpace($relative)) {
                [void] $changed.Add($relative)
            }
        }
        return $changed
    }

    return $null
}

$checks = [System.Collections.Generic.List[object]]::new()
$trackedDocChanges = Get-TrackedDocChanges

$configPath = Join-Path $RepoRoot 'rust\filmuvfs\src\config.rs'
$configRaw = Get-Content -LiteralPath $configPath -Raw

$windowsDefaultPattern = 'default_for_platform\(\).*?#\[cfg\(target_os = "windows"\)\]\s*\{\s*Self::Winfsp\s*\}'
$windowsAutoResolvePattern = 'Self::Auto\s*=>\s*\{.*?#\[cfg\(target_os = "windows"\)\]\s*\{\s*Ok\(ResolvedMountAdapterKind::Winfsp\)\s*\}'

Add-Check -Checks $checks -Name 'runtime.windows_default_adapter' `
    -Passed:(Test-Regex -Content $configRaw -Pattern $windowsDefaultPattern) `
    -Observed:'parsed rust/filmuvfs/src/config.rs' `
    -Expected:'MountAdapterKind::default_for_platform() returns Winfsp on Windows'
Add-Check -Checks $checks -Name 'runtime.auto_resolves_windows_winfsp' `
    -Passed:(Test-Regex -Content $configRaw -Pattern $windowsAutoResolvePattern) `
    -Observed:'parsed rust/filmuvfs/src/config.rs' `
    -Expected:'MountAdapterKind::Auto resolves to ResolvedMountAdapterKind::Winfsp on Windows'

$docContractTargets = @(
    'README.md',
    'WINDOWS_README.md',
    'QUICK_START.md',
    'docs/README.md',
    'docs/WINDOWS_README.md',
    'docs/QUICK_START.md'
)
$statusRelative = 'docs/STATUS.md'
$docsTouched = $false
if ($null -ne $trackedDocChanges) {
    foreach ($path in @($docContractTargets + @($statusRelative))) {
        if ($trackedDocChanges.Contains($path)) {
            $docsTouched = $true
            break
        }
    }
}
$forbiddenClaims = @(
    'auto\s+.*resolves\s+to\s+[`'']?projfs',
    'projfs\s+remains\s+the\s+policy/default'
)
$requiredClaims = @(
    'auto\s+.*resolves\s+to\s+[`'']?winfsp[`'']?.*windows',
    '(windows\s+default|default\s+windows).*winfsp|winfsp.*(windows\s+default|default\s+windows)'
)

if ($docsTouched) {
    foreach ($relative in $docContractTargets) {
        if ($null -ne $trackedDocChanges -and -not $trackedDocChanges.Contains($relative)) {
            continue
        }

        $path = Join-Path $RepoRoot $relative
        if (-not (Test-Path -LiteralPath $path)) {
            Add-Check -Checks $checks -Name ("docs.exists::{0}" -f $relative) `
                -Passed:$false -Observed:$false -Expected:$true
            continue
        }

        $raw = Get-Content -LiteralPath $path -Raw
        Add-Check -Checks $checks -Name ("docs.exists::{0}" -f $relative) `
            -Passed:$true -Observed:$true -Expected:$true

        foreach ($pattern in $forbiddenClaims) {
            $containsForbidden = Test-Regex -Content $raw -Pattern $pattern
            Add-Check -Checks $checks -Name ("docs.no_forbidden_claim::{0}::{1}" -f $relative, $pattern) `
                -Passed:(-not $containsForbidden) -Observed:$containsForbidden -Expected:$false
        }

        foreach ($pattern in $requiredClaims) {
            $containsRequired = Test-Regex -Content $raw -Pattern $pattern
            Add-Check -Checks $checks -Name ("docs.contains_runtime_claim::{0}::{1}" -f $relative, $pattern) `
                -Passed:$containsRequired -Observed:$containsRequired -Expected:$true
        }
    }
}
else {
    Add-Check -Checks $checks -Name 'docs.contract_claims_skipped_no_doc_delta' `
        -Passed:$true -Observed:$false -Expected:$false
}

$statusPath = Join-Path $RepoRoot 'docs\STATUS.md'
$statusChanged = $docsTouched -and ($null -eq $trackedDocChanges -or $trackedDocChanges.Contains($statusRelative))
$statusRaw = if ($statusChanged -and (Test-Path -LiteralPath $statusPath)) {
    Get-Content -LiteralPath $statusPath -Raw
} else {
    ''
}
$legacyBoardReference = 'TODOS/NEXT_15_SLICES_EXECUTION_BOARD.md'
$currentBoardReference = 'TODOS/COMPLETED/NEXT_15_SLICES_EXECUTION_BOARD.md'
Add-Check -Checks $checks -Name 'docs.status_no_legacy_execution_board_link' `
    -Passed:(-not $statusChanged -or -not ($statusRaw -match [regex]::Escape($legacyBoardReference))) `
    -Observed:($statusChanged -and ($statusRaw -match [regex]::Escape($legacyBoardReference))) `
    -Expected:$false
Add-Check -Checks $checks -Name 'docs.status_has_current_execution_board_link' `
    -Passed:(-not $statusChanged -or ($statusRaw -match [regex]::Escape($currentBoardReference))) `
    -Observed:($statusChanged -and ($statusRaw -match [regex]::Escape($currentBoardReference))) `
    -Expected:$true

$linkedDocs = @(
    'docs/STATUS.md',
    'docs/EXECUTION_PLAN.md',
    'docs/TODOS/PLAYBACK_PROOF_IMPLEMENTATION_PLAN.md',
    'docs/TODOS/FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md'
)
foreach ($relative in $linkedDocs) {
    $path = Join-Path $RepoRoot $relative
    Add-Check -Checks $checks -Name ("docs.link_target_exists::{0}" -f $relative) `
        -Passed:(Test-Path -LiteralPath $path) `
        -Observed:(Test-Path -LiteralPath $path) `
        -Expected:$true
}

$failedChecks = @($checks | Where-Object { -not $_.passed })
$result = [ordered]@{
    timestamp = (Get-Date).ToString('o')
    repo_root = $RepoRoot
    status = if ($failedChecks.Count -eq 0) { 'passed' } else { 'failed' }
    checks = $checks
    failed_checks = $failedChecks
}

$outputDirectory = Split-Path -Parent $OutputPath
if (-not [string]::IsNullOrWhiteSpace($outputDirectory)) {
    New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null
}
$result | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath -Encoding UTF8

if ($AsJson) {
    $result | ConvertTo-Json -Depth 8
}

if ($failedChecks.Count -gt 0) {
    throw ("[docs-runtime-contract] contract drift detected; summary written to {0}" -f $OutputPath)
}

Write-Host ("[docs-runtime-contract] PASS. Summary: {0}" -f $OutputPath) -ForegroundColor Green
