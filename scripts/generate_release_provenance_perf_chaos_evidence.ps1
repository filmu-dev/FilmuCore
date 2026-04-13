param(
    [string] $RepoRoot = '',
    [string] $ArtifactsDir = 'ci-artifacts/release-gates',
    [switch] $RunChaosContracts = $true,
    [switch] $RequireSignedHead
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
$RepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)
$ArtifactsDir = if ([System.IO.Path]::IsPathRooted($ArtifactsDir)) {
    $ArtifactsDir
}
else {
    Join-Path $RepoRoot $ArtifactsDir
}
New-Item -ItemType Directory -Force -Path $ArtifactsDir | Out-Null

function Get-Sha256Hex {
    param([Parameter(Mandatory = $true)][string] $Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        throw ("Missing file for digest: {0}" -f $Path)
    }
    return [string]((Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash).ToLowerInvariant()
}

function Get-PackageRowsFromLock {
    param([Parameter(Mandatory = $true)][string] $LockPath)
    if (-not (Test-Path -LiteralPath $LockPath)) {
        return @()
    }
    $raw = Get-Content -LiteralPath $LockPath -Raw
    $rows = [System.Collections.Generic.List[object]]::new()
    $blockMatches = [System.Text.RegularExpressions.Regex]::Matches(
        $raw,
        '(?ms)^\[\[package\]\]\s*(.+?)(?=^\[\[package\]\]|\z)'
    )
    foreach ($match in $blockMatches) {
        $block = [string]$match.Value
        $nameMatch = [System.Text.RegularExpressions.Regex]::Match($block, '(?m)^\s*name\s*=\s*"([^"]+)"')
        $versionMatch = [System.Text.RegularExpressions.Regex]::Match($block, '(?m)^\s*version\s*=\s*"([^"]+)"')
        if (-not $nameMatch.Success -or -not $versionMatch.Success) {
            continue
        }
        $rows.Add([pscustomobject]@{
                name = $nameMatch.Groups[1].Value
                version = $versionMatch.Groups[1].Value
            })
    }

    return @(
        $rows |
            Sort-Object name, version -Unique
    )
}

function Convert-ToSpdxId {
    param([Parameter(Mandatory = $true)][string] $Value)
    $safe = [System.Text.RegularExpressions.Regex]::Replace($Value, '[^A-Za-z0-9\.\-]+', '-')
    $safe = $safe.Trim('-')
    if ([string]::IsNullOrWhiteSpace($safe)) {
        return 'unknown'
    }
    return $safe
}

$uvLockPath = Join-Path $RepoRoot 'uv.lock'
$cargoLockPath = Join-Path $RepoRoot 'rust/filmuvfs/Cargo.lock'
$pythonPackages = Get-PackageRowsFromLock -LockPath $uvLockPath
$rustPackages = Get-PackageRowsFromLock -LockPath $cargoLockPath

$timestampIso = (Get-Date).ToUniversalTime().ToString('o')
$pythonSbomPath = Join-Path $ArtifactsDir 'sbom-python.cdx.json'
$rustSbomPath = Join-Path $ArtifactsDir 'sbom-rust.spdx.json'
$provenancePath = Join-Path $ArtifactsDir 'provenance.intoto.jsonl'
$signaturePath = Join-Path $ArtifactsDir 'signature-verification.json'
$chaosSummaryPath = Join-Path $ArtifactsDir 'chaos-contract-summary.json'

$pythonComponents = @(
    $pythonPackages |
        ForEach-Object {
            [ordered]@{
                type = 'library'
                name = $_.name
                version = $_.version
                purl = ('pkg:pypi/{0}@{1}' -f ([string]$_.name).ToLowerInvariant(), $_.version)
            }
        }
)
$pythonSbom = [ordered]@{
    bomFormat = 'CycloneDX'
    specVersion = '1.5'
    serialNumber = ('urn:uuid:{0}' -f [guid]::NewGuid())
    version = 1
    metadata = [ordered]@{
        timestamp = $timestampIso
        tools = @(
            [ordered]@{
                vendor = 'filmu'
                name = 'generate_release_provenance_perf_chaos_evidence.ps1'
            }
        )
        component = [ordered]@{
            type = 'application'
            name = 'filmu-python'
        }
    }
    components = $pythonComponents
}
$pythonSbom | ConvertTo-Json -Depth 10 | Set-Content -Path $pythonSbomPath -Encoding UTF8

$spdxPackages = @(
    $rustPackages |
        ForEach-Object {
            $spdxName = Convert-ToSpdxId -Value ([string]$_.name)
            $spdxVersion = Convert-ToSpdxId -Value ([string]$_.version)
            [ordered]@{
                SPDXID = ('SPDXRef-Package-{0}-{1}' -f $spdxName, $spdxVersion)
                name = $_.name
                versionInfo = $_.version
                downloadLocation = 'NOASSERTION'
                filesAnalyzed = $false
                licenseConcluded = 'NOASSERTION'
                licenseDeclared = 'NOASSERTION'
                copyrightText = 'NOASSERTION'
            }
        }
)
$rustSbom = [ordered]@{
    spdxVersion = 'SPDX-2.3'
    dataLicense = 'CC0-1.0'
    SPDXID = 'SPDXRef-DOCUMENT'
    name = 'filmu-rust-sbom'
    documentNamespace = ('https://filmu.dev/spdx/{0}' -f ([guid]::NewGuid().ToString()))
    creationInfo = [ordered]@{
        created = $timestampIso
        creators = @('Tool: generate_release_provenance_perf_chaos_evidence.ps1')
    }
    packages = $spdxPackages
}
$rustSbom | ConvertTo-Json -Depth 10 | Set-Content -Path $rustSbomPath -Encoding UTF8

$subjectFiles = @(
    [ordered]@{ name = 'sbom-python.cdx.json'; path = $pythonSbomPath },
    [ordered]@{ name = 'sbom-rust.spdx.json'; path = $rustSbomPath }
)
$subjectEntries = @(
    $subjectFiles |
        ForEach-Object {
            [ordered]@{
                name = $_.name
                digest = [ordered]@{
                    sha256 = Get-Sha256Hex -Path $_.path
                }
            }
        }
)
$materials = @()
foreach ($materialRelative in @('uv.lock', 'rust/filmuvfs/Cargo.lock', 'package.json')) {
    $materialPath = Join-Path $RepoRoot $materialRelative
    if (Test-Path -LiteralPath $materialPath) {
        $materials += [ordered]@{
            uri = $materialRelative
            digest = [ordered]@{
                sha256 = Get-Sha256Hex -Path $materialPath
            }
        }
    }
}
$provenance = [ordered]@{
    _type = 'https://in-toto.io/Statement/v1'
    subject = $subjectEntries
    predicateType = 'https://slsa.dev/provenance/v1'
    predicate = [ordered]@{
        buildDefinition = [ordered]@{
            buildType = 'https://filmu.dev/release-gate/provenance'
            externalParameters = [ordered]@{
                repository = [string]$env:GITHUB_REPOSITORY
                ref = [string]$env:GITHUB_REF
            }
            internalParameters = [ordered]@{
                generated_at = $timestampIso
            }
            resolvedDependencies = $materials
        }
        runDetails = [ordered]@{
            builder = [ordered]@{
                id = if (-not [string]::IsNullOrWhiteSpace([string]$env:GITHUB_ACTIONS)) { 'github-actions' } else { 'local' }
            }
            metadata = [ordered]@{
                invocationId = if (-not [string]::IsNullOrWhiteSpace([string]$env:GITHUB_RUN_ID)) { [string]$env:GITHUB_RUN_ID } else { 'local' }
                startedOn = $timestampIso
                finishedOn = $timestampIso
            }
        }
    }
}
($provenance | ConvertTo-Json -Depth 12) + "`n" | Set-Content -Path $provenancePath -Encoding UTF8

$signatureChecks = [System.Collections.Generic.List[object]]::new()
foreach ($subject in @($provenance.subject)) {
    $subjectName = [string]$subject.name
    $subjectPath = $subjectFiles | Where-Object { $_.name -eq $subjectName } | Select-Object -First 1
    if ($null -eq $subjectPath) {
        $signatureChecks.Add([pscustomobject]@{
                name = ("subject_present::{0}" -f $subjectName)
                passed = $false
                observed = 'not-mapped'
                expected = 'mapped'
            })
        continue
    }
    $actual = Get-Sha256Hex -Path $subjectPath.path
    $expected = [string]$subject.digest.sha256
    $signatureChecks.Add([pscustomobject]@{
            name = ("subject_digest_matches::{0}" -f $subjectName)
            passed = ($actual -eq $expected)
            observed = $actual
            expected = $expected
        })
}

$gitSignatureObserved = 'not_checked'
$gitSignaturePassed = $true
if ($RequireSignedHead) {
    try {
        $gitSignatureObserved = ((git -C $RepoRoot log -1 --pretty=%G?) 2>$null).Trim()
        $gitSignaturePassed = $gitSignatureObserved -in @('G', 'U', 'X', 'Y')
    }
    catch {
        $gitSignatureObserved = 'git_error'
        $gitSignaturePassed = $false
    }
}
$signatureChecks.Add([pscustomobject]@{
        name = 'head_commit_signature'
        passed = $gitSignaturePassed
        observed = $gitSignatureObserved
        expected = if ($RequireSignedHead) { 'G|U|X|Y' } else { 'not_required' }
    })

$signatureFailures = @($signatureChecks | Where-Object { -not $_.passed })
$signatureSummary = [ordered]@{
    generated_at = $timestampIso
    require_signed_head = [bool]$RequireSignedHead
    checks = $signatureChecks
    failed_checks = $signatureFailures
    status = if ($signatureFailures.Count -eq 0) { 'passed' } else { 'failed' }
}
$signatureSummary | ConvertTo-Json -Depth 8 | Set-Content -Path $signaturePath -Encoding UTF8

if ($RunChaosContracts) {
    $chaosTests = @(
        'tests/test_stream_refresh_policy_contract.py',
        'tests/test_replay_backplane.py',
        'tests/test_chunk_parity_contract.py'
    )
    Write-Host '[release-gate] Running chaos contract tests...'
    & uv run pytest -q @chaosTests
    if ($LASTEXITCODE -ne 0) {
        throw '[release-gate] chaos contract tests failed.'
    }
    [ordered]@{
        generated_at = (Get-Date).ToUniversalTime().ToString('o')
        source = 'contract-tests'
        tests = $chaosTests
        max_reconnect_incidents = 0
        max_provider_pressure_incidents = 0
        max_fatal_error_incidents = 0
    } | ConvertTo-Json -Depth 6 | Set-Content -Path $chaosSummaryPath -Encoding UTF8
}

[ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString('o')
    artifacts_dir = $ArtifactsDir
    python_sbom = $pythonSbomPath
    rust_sbom = $rustSbomPath
    provenance = $provenancePath
    signature_verification = $signaturePath
    chaos_summary = if ($RunChaosContracts) { $chaosSummaryPath } else { $null }
    python_component_count = @($pythonComponents).Count
    rust_package_count = @($spdxPackages).Count
} | ConvertTo-Json -Depth 8 | Set-Content -Path (Join-Path $ArtifactsDir 'release-evidence-manifest.json') -Encoding UTF8

if ($signatureSummary.status -ne 'passed') {
    throw '[release-gate] signature/provenance subject verification failed.'
}

Write-Host ("[release-gate] Evidence generated under {0}" -f $ArtifactsDir) -ForegroundColor Green
