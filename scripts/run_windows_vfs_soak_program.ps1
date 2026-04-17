param(
    [string[]] $EnvironmentClasses = @(),
    [string[]] $Profiles = @('continuous', 'seek', 'concurrent', 'full'),
    [int] $RepeatCount = 1,
    [switch] $RequireRuntimeCapture,
    [switch] $RequireBackendStatusCapture,
    [int] $MinimumEnvironmentCount = 2,
    [int] $MaxReconnectIncidents = 0,
    [int] $MaxProviderPressureIncidents = 0,
    [int] $MaxFatalErrorIncidents = 0,
    [string] $ArtifactsRoot = '',
    [string] $HistoryRoot = '',
    [string] $ContractPath = '',
    [switch] $AllowTrendBootstrap,
    [switch] $FailFast
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ($RepeatCount -lt 1) {
    throw 'RepeatCount must be at least 1.'
}

function Add-UniqueString {
    param(
        [Parameter(Mandatory = $true)]
        [System.Collections.Generic.List[string]] $Values,
        [string] $Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return
    }

    $trimmed = $Value.Trim()
    if ($Values -notcontains $trimmed) {
        $Values.Add($trimmed)
    }
}

function Get-UtcTimestamp {
    param([datetime] $Value)
    return $Value.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
}

function Read-JsonFile {
    param([string] $Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
}

$contractSchemaVersion = 1
$artifactKind = 'windows_vfs_soak_program'
$FreshnessWindowHours = 72

if ([string]::IsNullOrWhiteSpace($ArtifactsRoot)) {
    $ArtifactsRoot = Join-Path (Split-Path -Parent $PSScriptRoot) 'playback-proof-artifacts\windows-native-stack'
}
$ArtifactsRoot = [System.IO.Path]::GetFullPath($ArtifactsRoot)
New-Item -ItemType Directory -Force -Path $ArtifactsRoot | Out-Null

if ([string]::IsNullOrWhiteSpace($ContractPath)) {
    $ContractPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'ops\rollout\windows-vfs-soak-program.contract.json'
}
$contract = $null
if (Test-Path -LiteralPath $ContractPath) {
    $ContractPath = [System.IO.Path]::GetFullPath($ContractPath)
    $contract = Get-Content -LiteralPath $ContractPath -Raw | ConvertFrom-Json
    if ($null -ne $contract) {
        if (
            @($EnvironmentClasses).Count -eq 0 -and
            ($contract.PSObject.Properties.Name -contains 'required_environment_classes')
        ) {
            $EnvironmentClasses = @($contract.required_environment_classes)
        }
        if ($contract.PSObject.Properties.Name -contains 'required_profiles') {
            $Profiles = @($contract.required_profiles)
        }
        if ($contract.PSObject.Properties.Name -contains 'schema_version') {
            $contractSchemaVersion = [int]$contract.schema_version
        }
        if ($contract.PSObject.Properties.Name -contains 'artifact_kind') {
            $artifactKind = [string]$contract.artifact_kind
        }
        if ($contract.PSObject.Properties.Name -contains 'freshness_window_hours') {
            $FreshnessWindowHours = [int]$contract.freshness_window_hours
        }
        if ($contract.PSObject.Properties.Name -contains 'repeat_count') {
            $RepeatCount = [int]$contract.repeat_count
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
        if ($contract.PSObject.Properties.Name -contains 'max_reconnect_incidents') {
            $MaxReconnectIncidents = [int]$contract.max_reconnect_incidents
        }
        if ($contract.PSObject.Properties.Name -contains 'max_provider_pressure_incidents') {
            $MaxProviderPressureIncidents = [int]$contract.max_provider_pressure_incidents
        }
        if ($contract.PSObject.Properties.Name -contains 'max_fatal_error_incidents') {
            $MaxFatalErrorIncidents = [int]$contract.max_fatal_error_incidents
        }
    }
}

if ($EnvironmentClasses.Count -eq 0) {
    $fromEnv = [string]::Join(',', @([string]$env:FILMU_VFS_ENVIRONMENT_CLASSES))
    if (-not [string]::IsNullOrWhiteSpace($fromEnv)) {
        $EnvironmentClasses = @(
            $fromEnv.Split(',', [System.StringSplitOptions]::RemoveEmptyEntries) |
                ForEach-Object { $_.Trim() } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )
    }
}
if ($EnvironmentClasses.Count -eq 0) {
    $EnvironmentClasses = @([string]$env:COMPUTERNAME)
}

$stabilityScript = Join-Path $PSScriptRoot 'run_windows_vfs_soak_stability.ps1'
$multiEnvScript = Join-Path $PSScriptRoot 'run_windows_vfs_multi_environment_gate.ps1'
$trendScript = Join-Path $PSScriptRoot 'check_windows_vfs_soak_trends.ps1'

$summaryPaths = [System.Collections.Generic.List[string]]::new()
$results = [System.Collections.Generic.List[object]]::new()
$loadedSummaries = [System.Collections.Generic.List[object]]::new()
$failureReasons = [System.Collections.Generic.List[string]]::new()
$requiredActions = [System.Collections.Generic.List[string]]::new()

foreach ($environmentClass in $EnvironmentClasses) {
    $before = @(
        Get-ChildItem -LiteralPath $ArtifactsRoot -Filter 'soak-stability-*.json' -File -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty FullName
    )

    $arguments = @(
        '-NoProfile',
        '-File',
        $stabilityScript,
        '-ArtifactsRoot',
        $ArtifactsRoot,
        '-Profiles',
        ([string]::Join(',', $Profiles)),
        '-RepeatCount',
        $RepeatCount,
        '-EnvironmentClass',
        $environmentClass,
        '-RequireFilmuvfs'
    )
    if ($RequireRuntimeCapture) { $arguments += '-RequireRuntimeCapture' }
    if ($RequireBackendStatusCapture) { $arguments += '-RequireBackendStatusCapture' }
    if ($MaxReconnectIncidents -ge 0) {
        $arguments += @('-MaxReconnectIncidents', $MaxReconnectIncidents)
    }
    if ($MaxProviderPressureIncidents -ge 0) {
        $arguments += @('-MaxProviderPressureIncidents', $MaxProviderPressureIncidents)
    }
    if ($MaxFatalErrorIncidents -ge 0) {
        $arguments += @('-MaxFatalErrorIncidents', $MaxFatalErrorIncidents)
    }
    if ($FailFast) { $arguments += '-FailFast' }

    Write-Host ("[windows-vfs-soak-program] running environment class '{0}'" -f $environmentClass) -ForegroundColor Cyan
    & pwsh @arguments
    $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }

    $after = @(
        Get-ChildItem -LiteralPath $ArtifactsRoot -Filter 'soak-stability-*.json' -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -ExpandProperty FullName
    )
    $newSummaryPath = $after | Where-Object { $before -notcontains $_ } | Select-Object -First 1
    if ([string]::IsNullOrWhiteSpace([string]$newSummaryPath) -and $after.Count -gt 0) {
        $newSummaryPath = $after[0]
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$newSummaryPath)) {
        $summaryPaths.Add($newSummaryPath)
    }
    $results.Add([pscustomobject]@{
        environment_class = $environmentClass
        exit_code = $exitCode
        status = if ($exitCode -eq 0) { 'passed' } else { 'failed' }
        summary_path = $newSummaryPath
    })

    if ($exitCode -ne 0) {
        Add-UniqueString -Values $failureReasons -Value 'windows_vfs_soak_run_failed'
        Add-UniqueString -Values $requiredActions -Value 'run_windows_vfs_soak_all_profiles'
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$newSummaryPath)) {
        $summaryPayload = Read-JsonFile -Path $newSummaryPath
        if ($null -ne $summaryPayload) {
            $loadedSummaries.Add($summaryPayload)
        }
    }

    if ($exitCode -ne 0 -and $FailFast) {
        break
    }
}

if ($summaryPaths.Count -eq 0) {
    Add-UniqueString -Values $failureReasons -Value 'windows_vfs_soak_summary_missing'
    Add-UniqueString -Values $requiredActions -Value 'run_windows_vfs_soak_all_profiles'
}

$multiEnvironmentExitCode = -1
$trendExitCode = -1
if ($summaryPaths.Count -gt 0) {
    & pwsh -NoProfile -File $multiEnvScript `
        -ArtifactsRoot $ArtifactsRoot `
        -SummaryPaths @($summaryPaths) `
        -MinimumEnvironmentCount $MinimumEnvironmentCount `
        -ContractPath $ContractPath `
        -RequireRuntimeCapture:$RequireRuntimeCapture `
        -RequireBackendStatusCapture:$RequireBackendStatusCapture
    $multiEnvironmentExitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }
    if ($multiEnvironmentExitCode -ne 0) {
        Add-UniqueString -Values $failureReasons -Value 'windows_vfs_soak_environment_policy_failed'
        Add-UniqueString -Values $requiredActions -Value 'rerun_windows_vfs_soak_multi_environment_gate'
    }

    & pwsh -NoProfile -File $trendScript `
        -ArtifactsRoot $ArtifactsRoot `
        -SummaryPaths @($summaryPaths) `
        -HistoryRoot $HistoryRoot `
        -ContractPath $ContractPath `
        -AllowBootstrap:$AllowTrendBootstrap
    $trendExitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }
    if ($trendExitCode -ne 0) {
        Add-UniqueString -Values $failureReasons -Value 'windows_vfs_soak_trend_regression_detected'
        Add-UniqueString -Values $requiredActions -Value 'refresh_windows_vfs_soak_trend_history'
    }
}

$capturedAt = (Get-Date).ToUniversalTime()
$timestamp = $capturedAt.ToString('yyyyMMdd-HHmmss')
$latestMultiEnvironmentSummary = @(
    Get-ChildItem -LiteralPath $ArtifactsRoot -Filter 'multi-environment-vfs-summary-*.json' -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
)
$latestTrendSummary = @(
    Get-ChildItem -LiteralPath $ArtifactsRoot -Filter 'soak-trend-summary-*.json' -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
)
$programSummaryPath = Join-Path $ArtifactsRoot ("soak-program-summary-{0}.json" -f $timestamp)
$multiEnvironmentSummaryPath = if ($latestMultiEnvironmentSummary.Count -gt 0) { $latestMultiEnvironmentSummary[0].FullName } else { $null }
$trendSummaryPath = if ($latestTrendSummary.Count -gt 0) { $latestTrendSummary[0].FullName } else { $null }
$multiEnvironmentSummary = if ($null -ne $multiEnvironmentSummaryPath) { Read-JsonFile -Path $multiEnvironmentSummaryPath } else { $null }
$trendSummary = if ($null -ne $trendSummaryPath) { Read-JsonFile -Path $trendSummaryPath } else { $null }

$profileCoverage = [System.Collections.Generic.List[string]]::new()
$observedEnvironmentClasses = [System.Collections.Generic.List[string]]::new()
foreach ($summary in $loadedSummaries) {
    Add-UniqueString -Values $observedEnvironmentClasses -Value ([string]$summary.environment_class)
    foreach ($profile in @($summary.profiles)) {
        Add-UniqueString -Values $profileCoverage -Value ([string]$profile)
    }
}

$missingProfiles = @(
    @($Profiles) |
        Where-Object {
            $normalized = ([string]$_).Trim().ToLowerInvariant()
            $profileCoverage -notcontains $normalized
        }
)
$missingEnvironmentClasses = @(
    @($EnvironmentClasses) |
        Where-Object {
            $normalized = ([string]$_).Trim()
            $observedEnvironmentClasses -notcontains $normalized
        }
)
$profileCoverageComplete = $missingProfiles.Count -eq 0
$environmentCoverageComplete = (
    ($observedEnvironmentClasses.Count -ge $MinimumEnvironmentCount) -and
    ($missingEnvironmentClasses.Count -eq 0)
)

if (-not $profileCoverageComplete) {
    Add-UniqueString -Values $failureReasons -Value 'windows_vfs_soak_profile_coverage_incomplete'
    Add-UniqueString -Values $requiredActions -Value 'run_windows_vfs_soak_all_profiles'
}
if (-not $environmentCoverageComplete) {
    Add-UniqueString -Values $failureReasons -Value 'windows_vfs_soak_environment_coverage_incomplete'
    Add-UniqueString -Values $requiredActions -Value 'rerun_windows_vfs_soak_multi_environment_gate'
}

$multiEnvironmentStatus = if ($null -eq $multiEnvironmentSummary) {
    'missing'
}
elseif ([bool]($multiEnvironmentSummary.all_green ?? $false)) {
    'passed'
}
else {
    'failed'
}
if ($multiEnvironmentStatus -eq 'missing') {
    Add-UniqueString -Values $failureReasons -Value 'windows_vfs_soak_environment_summary_missing'
    Add-UniqueString -Values $requiredActions -Value 'rerun_windows_vfs_soak_multi_environment_gate'
}

$trendStatus = if ($null -eq $trendSummary) {
    'missing'
}
else {
    [string]($trendSummary.status ?? 'missing')
}
if ($trendStatus -eq 'missing') {
    Add-UniqueString -Values $failureReasons -Value 'windows_vfs_soak_trend_summary_missing'
    Add-UniqueString -Values $requiredActions -Value 'refresh_windows_vfs_soak_trend_history'
}

$ready = $failureReasons.Count -eq 0
$summaryStatus = if ($ready) { 'passed' } else { 'failed' }

[ordered]@{
    schema_version = $contractSchemaVersion
    artifact_kind = $artifactKind
    timestamp = Get-UtcTimestamp -Value $capturedAt
    captured_at = Get-UtcTimestamp -Value $capturedAt
    expires_at = Get-UtcTimestamp -Value $capturedAt.AddHours([Math]::Max($FreshnessWindowHours, 1))
    freshness_window_hours = [Math]::Max($FreshnessWindowHours, 1)
    status = $summaryStatus
    ready = $ready
    contract_path = if ($null -ne $contract) { $ContractPath } else { $null }
    environment_classes = $EnvironmentClasses
    environment_count = @($EnvironmentClasses).Count
    observed_environment_classes = @($observedEnvironmentClasses)
    missing_environment_classes = $missingEnvironmentClasses
    environment_coverage_complete = $environmentCoverageComplete
    profiles = @($Profiles)
    required_profiles = @($Profiles)
    profile_coverage = @($profileCoverage)
    profile_coverage_complete = $profileCoverageComplete
    repeat_count = $RepeatCount
    minimum_environment_count = $MinimumEnvironmentCount
    require_runtime_capture = [bool]$RequireRuntimeCapture
    require_backend_status_capture = [bool]$RequireBackendStatusCapture
    max_reconnect_incidents = $MaxReconnectIncidents
    max_provider_pressure_incidents = $MaxProviderPressureIncidents
    max_fatal_error_incidents = $MaxFatalErrorIncidents
    allow_trend_bootstrap = [bool]$AllowTrendBootstrap
    multi_environment_status = $multiEnvironmentStatus
    multi_environment_summary_path = $multiEnvironmentSummaryPath
    trend_status = $trendStatus
    trend_summary_path = $trendSummaryPath
    failure_reasons = @($failureReasons)
    required_actions = @($requiredActions)
    results = $results
    summary_paths = @($summaryPaths)
} | ConvertTo-Json -Depth 10 | Set-Content -Path $programSummaryPath -Encoding UTF8

if (-not $ready) {
    throw ("[windows-vfs-soak-program] FAIL. Summary: {0}" -f $programSummaryPath)
}

Write-Host ("[windows-vfs-soak-program] PASS. Summary: {0}" -f $programSummaryPath) -ForegroundColor Green
