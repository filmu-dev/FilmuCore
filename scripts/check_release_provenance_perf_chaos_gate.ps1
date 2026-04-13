param(
    [string] $RepoRoot = '',
    [string] $ArtifactsDir = 'ci-artifacts/release-gates',
    [string[]] $SbomPaths = @('ci-artifacts/release-gates/sbom-python.cdx.json', 'ci-artifacts/release-gates/sbom-rust.spdx.json'),
    [string[]] $ProvenancePaths = @('ci-artifacts/release-gates/provenance.intoto.jsonl', 'ci-artifacts/release-gates/signature-verification.json'),
    [string[]] $ChaosSummaryPaths = @(),
    [int] $MaxReconnectIncidents = 0,
    [int] $MaxProviderPressureIncidents = 0,
    [int] $MaxFatalErrorIncidents = 0,
    [switch] $AllowMissingEvidence
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
$RepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)

$ArtifactsDir = if ([System.IO.Path]::IsPathRooted($ArtifactsDir)) {
    $ArtifactsDir
} else {
    Join-Path $RepoRoot $ArtifactsDir
}
New-Item -ItemType Directory -Force -Path $ArtifactsDir | Out-Null

if ($ChaosSummaryPaths.Count -eq 0) {
    $chaosRoot = Join-Path $RepoRoot 'playback-proof-artifacts\windows-native-stack'
    if (Test-Path -LiteralPath $chaosRoot) {
        $ChaosSummaryPaths = @(
            Get-ChildItem -LiteralPath $chaosRoot -Filter 'soak-stability-*.json' -File |
                Sort-Object LastWriteTimeUtc -Descending |
                Select-Object -First 5 |
                Select-Object -ExpandProperty FullName
        )
    }
    $contractSummary = Join-Path $RepoRoot 'ci-artifacts/release-gates/chaos-contract-summary.json'
    if ((Test-Path -LiteralPath $contractSummary) -and -not ($ChaosSummaryPaths -contains $contractSummary)) {
        $ChaosSummaryPaths += $contractSummary
    }
}

$checks = [System.Collections.Generic.List[object]]::new()
function Add-Check {
    param([string]$Name, [bool]$Passed, [object]$Observed, [object]$Expected)
    $checks.Add([pscustomobject]@{
        name = $Name
        passed = $Passed
        observed = $Observed
        expected = $Expected
    })
}

function Resolve-PathFromRepo {
    param([string]$MaybeRelativePath)
    if ([System.IO.Path]::IsPathRooted($MaybeRelativePath)) {
        return $MaybeRelativePath
    }
    return Join-Path $RepoRoot $MaybeRelativePath
}

function Get-JsonFromPath {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    try {
        return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

foreach ($path in $SbomPaths) {
    $resolved = Resolve-PathFromRepo -MaybeRelativePath $path
    $exists = Test-Path -LiteralPath $resolved
    $sbomExistsExpected = if ($AllowMissingEvidence) { 'exists_or_allowed_missing' } else { 'exists' }
    Add-Check -Name ("sbom_exists::{0}" -f $path) `
        -Passed:($exists -or $AllowMissingEvidence) `
        -Observed:$exists `
        -Expected:$sbomExistsExpected
    if ($exists) {
        $sbomJson = Get-JsonFromPath -Path $resolved
        $hasSbomShape = $false
        $sbomShapeExpected = if ($AllowMissingEvidence) { 'cyclonedx_or_spdx_or_allowed_missing' } else { 'cyclonedx_or_spdx' }
        if ($null -ne $sbomJson) {
            $hasCycloneDx = ($sbomJson.PSObject.Properties.Name -contains 'bomFormat') -and ([string]$sbomJson.bomFormat -eq 'CycloneDX')
            $hasSpdx = ($sbomJson.PSObject.Properties.Name -contains 'spdxVersion')
            $hasSbomShape = $hasCycloneDx -or $hasSpdx
        }
        Add-Check -Name ("sbom_structure::{0}" -f $path) `
            -Passed:($hasSbomShape -or $AllowMissingEvidence) `
            -Observed:$hasSbomShape `
            -Expected:$sbomShapeExpected
    }
}

foreach ($path in $ProvenancePaths) {
    $resolved = Resolve-PathFromRepo -MaybeRelativePath $path
    $exists = Test-Path -LiteralPath $resolved
    $provenanceExistsExpected = if ($AllowMissingEvidence) { 'exists_or_allowed_missing' } else { 'exists' }
    Add-Check -Name ("provenance_exists::{0}" -f $path) `
        -Passed:($exists -or $AllowMissingEvidence) `
        -Observed:$exists `
        -Expected:$provenanceExistsExpected
    if ($exists -and $path -like '*.jsonl') {
        $raw = Get-Content -LiteralPath $resolved -Raw
        $provenance = $null
        $provenanceShapeExpected = if ($AllowMissingEvidence) { 'in-toto-or-allowed_missing' } else { 'in-toto' }
        if (-not [string]::IsNullOrWhiteSpace([string]$raw)) {
            $candidateLines = @($raw -split "`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
            foreach ($line in $candidateLines) {
                try {
                    $provenance = $line | ConvertFrom-Json
                    if ($null -ne $provenance) {
                        break
                    }
                }
                catch {
                    continue
                }
            }
            if ($null -eq $provenance) {
                try {
                    $provenance = $raw | ConvertFrom-Json
                }
                catch {
                    $provenance = $null
                }
            }
        }
        $hasShape = $false
        if ($null -ne $provenance) {
            $hasShape = ($provenance.PSObject.Properties.Name -contains '_type') -and
                ($provenance.PSObject.Properties.Name -contains 'subject') -and
                (@($provenance.subject).Count -gt 0)
        }
        Add-Check -Name ("provenance_structure::{0}" -f $path) `
            -Passed:($hasShape -or $AllowMissingEvidence) `
            -Observed:$hasShape `
            -Expected:$provenanceShapeExpected
    }
    if ($exists -and $path -like '*signature-verification.json') {
        $signatureSummary = Get-JsonFromPath -Path $resolved
        $signaturePassed = $false
        $signatureExpected = if ($AllowMissingEvidence) { 'passed_or_allowed_missing' } else { 'passed' }
        if ($null -ne $signatureSummary -and ($signatureSummary.PSObject.Properties.Name -contains 'status')) {
            $signaturePassed = [string]$signatureSummary.status -eq 'passed'
        }
        Add-Check -Name ("signature_verification_status::{0}" -f $path) `
            -Passed:($signaturePassed -or $AllowMissingEvidence) `
            -Observed:$signaturePassed `
            -Expected:$signatureExpected
    }
}

if ($ChaosSummaryPaths.Count -eq 0) {
    $chaosExpected = if ($AllowMissingEvidence) { '>=0' } else { '>=1' }
    Add-Check -Name 'chaos_summary_available' `
        -Passed:$AllowMissingEvidence `
        -Observed:0 `
        -Expected:$chaosExpected
} else {
    $maxReconnect = 0
    $maxProviderPressure = 0
    $maxFatalErrors = 0
    foreach ($summaryPath in $ChaosSummaryPaths) {
        if (-not (Test-Path -LiteralPath $summaryPath)) {
            continue
        }
        $summary = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
        $maxReconnect = [Math]::Max($maxReconnect, [int]($summary.max_reconnect_incidents ?? 0))
        $maxProviderPressure = [Math]::Max($maxProviderPressure, [int]($summary.max_provider_pressure_incidents ?? 0))
        $maxFatalErrors = [Math]::Max($maxFatalErrors, [int]($summary.max_fatal_error_incidents ?? 0))
    }
    Add-Check -Name 'chaos.max_reconnect_incidents' `
        -Passed:($maxReconnect -le $MaxReconnectIncidents) `
        -Observed:$maxReconnect `
        -Expected:("<= {0}" -f $MaxReconnectIncidents)
    Add-Check -Name 'chaos.max_provider_pressure_incidents' `
        -Passed:($maxProviderPressure -le $MaxProviderPressureIncidents) `
        -Observed:$maxProviderPressure `
        -Expected:("<= {0}" -f $MaxProviderPressureIncidents)
    Add-Check -Name 'chaos.max_fatal_error_incidents' `
        -Passed:($maxFatalErrors -le $MaxFatalErrorIncidents) `
        -Observed:$maxFatalErrors `
        -Expected:("<= {0}" -f $MaxFatalErrorIncidents)
}

$failedChecks = @($checks | Where-Object { -not $_.passed })
$summary = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString('o')
    repo_root = $RepoRoot
    allow_missing_evidence = [bool]$AllowMissingEvidence
    sbom_paths = $SbomPaths
    provenance_paths = $ProvenancePaths
    chaos_summary_paths = $ChaosSummaryPaths
    checks = $checks
    failed_checks = $failedChecks
    status = if ($failedChecks.Count -eq 0) { 'passed' } else { 'failed' }
}

$summaryPath = Join-Path $ArtifactsDir 'release-provenance-perf-chaos-gate.json'
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8
Write-Host ("Release provenance/perf/chaos gate summary: {0}" -f $summaryPath)

if ($summary.status -eq 'failed') {
    exit 1
}
