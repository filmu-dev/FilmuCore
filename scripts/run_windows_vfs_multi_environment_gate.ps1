param(
    [string] $ArtifactsRoot = '',
    [string[]] $SummaryPaths = @(),
    [string[]] $RequiredProfiles = @('continuous', 'seek', 'concurrent', 'full'),
    [string[]] $RequiredEnvironmentClasses = @(),
    [int] $MinimumEnvironmentCount = 2,
    [switch] $RequireRuntimeCapture,
    [switch] $RequireBackendStatusCapture,
    [string] $ContractPath = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ($MinimumEnvironmentCount -lt 1) {
    throw 'MinimumEnvironmentCount must be at least 1.'
}

function Get-DefaultArtifactsRoot {
    $repoRoot = Split-Path -Parent $PSScriptRoot
    Join-Path $repoRoot 'playback-proof-artifacts\windows-native-stack'
}

function Get-NormalizedStringList {
    param(
        [string[]] $Values,
        [switch] $Lowercase
    )

    $normalized = [System.Collections.Generic.List[string]]::new()
    foreach ($entry in $Values) {
        foreach ($token in ([string]$entry).Split(',', [System.StringSplitOptions]::RemoveEmptyEntries)) {
            $trimmed = $token.Trim()
            if ([string]::IsNullOrWhiteSpace($trimmed)) {
                continue
            }
            if ($Lowercase) {
                $trimmed = $trimmed.ToLowerInvariant()
            }
            if ($normalized -notcontains $trimmed) {
                $normalized.Add($trimmed)
            }
        }
    }
    return @($normalized)
}

if ([string]::IsNullOrWhiteSpace($ArtifactsRoot)) {
    $ArtifactsRoot = Get-DefaultArtifactsRoot
}
$ArtifactsRoot = [System.IO.Path]::GetFullPath($ArtifactsRoot)

if ([string]::IsNullOrWhiteSpace($ContractPath)) {
    $ContractPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'ops\rollout\windows-vfs-soak-program.contract.json'
}
$contract = $null
if (Test-Path -LiteralPath $ContractPath) {
    $ContractPath = [System.IO.Path]::GetFullPath($ContractPath)
    $contract = Get-Content -LiteralPath $ContractPath -Raw | ConvertFrom-Json
    if ($null -ne $contract) {
        if ($contract.PSObject.Properties.Name -contains 'required_profiles') {
            $RequiredProfiles = @($contract.required_profiles)
        }
        if (
            @($RequiredEnvironmentClasses).Count -eq 0 -and
            ($contract.PSObject.Properties.Name -contains 'required_environment_classes')
        ) {
            $RequiredEnvironmentClasses = @($contract.required_environment_classes)
        }
        if ($contract.PSObject.Properties.Name -contains 'minimum_environment_count') {
            $MinimumEnvironmentCount = [int]$contract.minimum_environment_count
        }
        if ($contract.PSObject.Properties.Name -contains 'require_runtime_capture') {
            $RequireRuntimeCapture = [bool]$contract.require_runtime_capture
        }
        if ($contract.PSObject.Properties.Name -contains 'require_backend_status_capture') {
            $RequireBackendStatusCapture = [bool]$contract.require_backend_status_capture
        }
    }
}

$RequiredProfiles = @(Get-NormalizedStringList -Values $RequiredProfiles -Lowercase)
$RequiredEnvironmentClasses = @(Get-NormalizedStringList -Values $RequiredEnvironmentClasses)

if ($SummaryPaths.Count -eq 0) {
    if (-not (Test-Path -LiteralPath $ArtifactsRoot)) {
        throw ("Artifacts root does not exist: {0}" -f $ArtifactsRoot)
    }
    $SummaryPaths = @(
        Get-ChildItem -LiteralPath $ArtifactsRoot -Filter 'soak-stability-*.json' -File -ErrorAction Stop |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -ExpandProperty FullName
    )
}

$SummaryPaths = @(Get-NormalizedStringList -Values $SummaryPaths)
if ($SummaryPaths.Count -eq 0) {
    throw ("No soak stability summaries found under {0}" -f $ArtifactsRoot)
}

$loadedSummaries = [System.Collections.Generic.List[object]]::new()
foreach ($summaryPath in $SummaryPaths) {
    if (-not (Test-Path -LiteralPath $summaryPath)) {
        throw ("Summary path does not exist: {0}" -f $summaryPath)
    }
    $summary = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
    if ($null -eq $summary) {
        throw ("Summary path did not deserialize: {0}" -f $summaryPath)
    }
    $environmentClass = if ($summary.PSObject.Properties.Name -contains 'environment_class') {
        [string]$summary.environment_class
    } else {
        ''
    }
    if ([string]::IsNullOrWhiteSpace($environmentClass)) {
        throw ("Summary path is missing environment_class: {0}" -f $summaryPath)
    }
    $loadedSummaries.Add([pscustomobject]@{
        path = $summaryPath
        summary = $summary
        environment_class = $environmentClass.Trim()
    })
}

$environmentRollups = [System.Collections.Generic.List[object]]::new()
foreach ($group in ($loadedSummaries | Group-Object -Property environment_class)) {
    $summaries = @($group.Group | ForEach-Object { $_.summary })
    $results = @($summaries | ForEach-Object { @($_.results) } | Where-Object { $null -ne $_ })
    $profilesCovered = @(
        $results |
            ForEach-Object { [string]($_.profile ?? '') } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            ForEach-Object { $_.ToLowerInvariant() } |
            Select-Object -Unique
    )
    $missingProfiles = @($RequiredProfiles | Where-Object { $profilesCovered -notcontains $_ })
    $criticalCachePressureRuns = @($results | Where-Object { [string]($_.runtime_cache_pressure_class ?? '') -eq 'critical' }).Count
    $criticalChunkCoalescingRuns = @($results | Where-Object { [string]($_.runtime_chunk_coalescing_pressure_class ?? '') -eq 'critical' }).Count
    $criticalUpstreamWaitRuns = @($results | Where-Object { [string]($_.runtime_upstream_wait_class ?? '') -eq 'critical' }).Count
    $criticalRefreshPressureRuns = @($results | Where-Object { [string]($_.runtime_refresh_pressure_class ?? '') -eq 'critical' }).Count
    $runtimeCapturedAll = (@($results | Where-Object { -not [bool]($_.runtime_captured ?? $false) })).Count -eq 0
    $backendCapturedAll = (@($results | Where-Object { -not [bool]($_.backend_status_captured ?? $false) })).Count -eq 0
    $allGreen = (@($summaries | Where-Object { -not [bool]($_.all_green ?? $false) })).Count -eq 0
    $environmentRollups.Add([pscustomobject]@{
        environment_class = $group.Name
        summary_paths = @($group.Group | ForEach-Object { $_.path })
        summary_count = $group.Count
        profiles_covered = $profilesCovered
        missing_profiles = $missingProfiles
        runtime_captured_all = $runtimeCapturedAll
        backend_status_captured_all = $backendCapturedAll
        all_green = $allGreen
        max_reconnect_incidents = @($summaries | ForEach-Object { [int]($_.max_reconnect_incidents ?? 0) } | Measure-Object -Maximum).Maximum
        max_provider_pressure_incidents = @($summaries | ForEach-Object { [int]($_.max_provider_pressure_incidents ?? 0) } | Measure-Object -Maximum).Maximum
        max_fatal_error_incidents = @($summaries | ForEach-Object { [int]($_.max_fatal_error_incidents ?? 0) } | Measure-Object -Maximum).Maximum
        critical_cache_pressure_runs = $criticalCachePressureRuns
        critical_chunk_coalescing_pressure_runs = $criticalChunkCoalescingRuns
        critical_upstream_wait_runs = $criticalUpstreamWaitRuns
        critical_refresh_pressure_runs = $criticalRefreshPressureRuns
    })
}

$observedEnvironmentClasses = @($environmentRollups | ForEach-Object { $_.environment_class })
$missingEnvironmentClasses = @($RequiredEnvironmentClasses | Where-Object { $observedEnvironmentClasses -notcontains $_ })
$policyChecks = [System.Collections.Generic.List[object]]::new()
$policyChecks.Add([pscustomobject]@{
    name = 'minimum_environment_count'
    passed = ($environmentRollups.Count -ge $MinimumEnvironmentCount)
    observed = $environmentRollups.Count
    threshold = $MinimumEnvironmentCount
})
$policyChecks.Add([pscustomobject]@{
    name = 'required_environment_classes'
    passed = (@($missingEnvironmentClasses).Count -eq 0)
    observed = $observedEnvironmentClasses
    threshold = if (@($RequiredEnvironmentClasses).Count -gt 0) { $RequiredEnvironmentClasses } else { @() }
})

foreach ($environment in $environmentRollups) {
    $policyChecks.Add([pscustomobject]@{
        name = ("profiles_present::{0}" -f $environment.environment_class)
        passed = (@($environment.missing_profiles).Count -eq 0)
        observed = $environment.profiles_covered
        threshold = $RequiredProfiles
    })
    $policyChecks.Add([pscustomobject]@{
        name = ("summaries_green::{0}" -f $environment.environment_class)
        passed = [bool]$environment.all_green
        observed = [bool]$environment.all_green
        threshold = $true
    })
    if ($RequireRuntimeCapture) {
        $policyChecks.Add([pscustomobject]@{
            name = ("runtime_capture::{0}" -f $environment.environment_class)
            passed = [bool]$environment.runtime_captured_all
            observed = [bool]$environment.runtime_captured_all
            threshold = $true
        })
    }
    if ($RequireBackendStatusCapture) {
        $policyChecks.Add([pscustomobject]@{
            name = ("backend_status_capture::{0}" -f $environment.environment_class)
            passed = [bool]$environment.backend_status_captured_all
            observed = [bool]$environment.backend_status_captured_all
            threshold = $true
        })
    }
    foreach ($pressurePolicy in @(
        @{ Name = 'critical_cache_pressure_runs'; Observed = $environment.critical_cache_pressure_runs },
        @{ Name = 'critical_chunk_coalescing_pressure_runs'; Observed = $environment.critical_chunk_coalescing_pressure_runs },
        @{ Name = 'critical_upstream_wait_runs'; Observed = $environment.critical_upstream_wait_runs },
        @{ Name = 'critical_refresh_pressure_runs'; Observed = $environment.critical_refresh_pressure_runs }
    )) {
        $policyChecks.Add([pscustomobject]@{
            name = ("{0}::{1}" -f $pressurePolicy.Name, $environment.environment_class)
            passed = ([int]$pressurePolicy.Observed -eq 0)
            observed = [int]$pressurePolicy.Observed
            threshold = 0
        })
    }
}

$failedChecks = @($policyChecks | Where-Object { -not $_.passed })
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$summaryPath = Join-Path $ArtifactsRoot ("multi-environment-vfs-summary-{0}.json" -f $timestamp)
$summary = [ordered]@{
    timestamp = (Get-Date).ToString('o')
    artifacts_root = $ArtifactsRoot
    contract_path = if ($null -ne $contract) { $ContractPath } else { $null }
    minimum_environment_count = $MinimumEnvironmentCount
    required_profiles = $RequiredProfiles
    required_environment_classes = $RequiredEnvironmentClasses
    require_runtime_capture = [bool]$RequireRuntimeCapture
    require_backend_status_capture = [bool]$RequireBackendStatusCapture
    observed_environment_count = $environmentRollups.Count
    observed_environment_classes = $observedEnvironmentClasses
    missing_environment_classes = $missingEnvironmentClasses
    policy_checks = $policyChecks
    failed_checks = $failedChecks
    all_green = ($failedChecks.Count -eq 0)
    environments = $environmentRollups
    source_summaries = @($loadedSummaries | ForEach-Object { $_.path })
}
$summary | ConvertTo-Json -Depth 10 | Set-Content -Path $summaryPath -Encoding UTF8

if (-not $summary.all_green) {
    throw ("[windows-vfs-multi-environment] one or more policy checks failed; summary written to {0}" -f $summaryPath)
}

Write-Host ("[windows-vfs-multi-environment] PASS. Summary: {0}" -f $summaryPath) -ForegroundColor Green
