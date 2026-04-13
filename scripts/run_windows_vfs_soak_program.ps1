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
    [switch] $AllowTrendBootstrap,
    [switch] $FailFast
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ($RepeatCount -lt 1) {
    throw 'RepeatCount must be at least 1.'
}

if ([string]::IsNullOrWhiteSpace($ArtifactsRoot)) {
    $ArtifactsRoot = Join-Path (Split-Path -Parent $PSScriptRoot) 'playback-proof-artifacts\windows-native-stack'
}
$ArtifactsRoot = [System.IO.Path]::GetFullPath($ArtifactsRoot)
New-Item -ItemType Directory -Force -Path $ArtifactsRoot | Out-Null

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
        summary_path = $newSummaryPath
    })

    if ($exitCode -ne 0 -and $FailFast) {
        throw ("[windows-vfs-soak-program] failed for environment '{0}'" -f $environmentClass)
    }
}

if ($summaryPaths.Count -eq 0) {
    throw '[windows-vfs-soak-program] no soak stability summaries were produced.'
}

& pwsh -NoProfile -File $multiEnvScript `
    -ArtifactsRoot $ArtifactsRoot `
    -SummaryPaths @($summaryPaths) `
    -MinimumEnvironmentCount $MinimumEnvironmentCount `
    -RequireRuntimeCapture:$RequireRuntimeCapture `
    -RequireBackendStatusCapture:$RequireBackendStatusCapture
$multiEnvironmentExitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }
if ($multiEnvironmentExitCode -ne 0) {
    throw ("[windows-vfs-soak-program] multi-environment gate failed with exit code {0}" -f $multiEnvironmentExitCode)
}

& pwsh -NoProfile -File $trendScript `
    -ArtifactsRoot $ArtifactsRoot `
    -SummaryPaths @($summaryPaths) `
    -HistoryRoot $HistoryRoot `
    -AllowBootstrap:$AllowTrendBootstrap
$trendExitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }
if ($trendExitCode -ne 0) {
    throw ("[windows-vfs-soak-program] trend gate failed with exit code {0}" -f $trendExitCode)
}

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
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
[ordered]@{
    timestamp = (Get-Date).ToString('o')
    status = 'passed'
    environment_classes = $EnvironmentClasses
    environment_count = @($EnvironmentClasses).Count
    profiles = $Profiles
    repeat_count = $RepeatCount
    minimum_environment_count = $MinimumEnvironmentCount
    require_runtime_capture = [bool]$RequireRuntimeCapture
    require_backend_status_capture = [bool]$RequireBackendStatusCapture
    max_reconnect_incidents = $MaxReconnectIncidents
    max_provider_pressure_incidents = $MaxProviderPressureIncidents
    max_fatal_error_incidents = $MaxFatalErrorIncidents
    allow_trend_bootstrap = [bool]$AllowTrendBootstrap
    multi_environment_summary_path = if ($latestMultiEnvironmentSummary.Count -gt 0) { $latestMultiEnvironmentSummary[0].FullName } else { $null }
    trend_summary_path = if ($latestTrendSummary.Count -gt 0) { $latestTrendSummary[0].FullName } else { $null }
    results = $results
    summary_paths = @($summaryPaths)
} | ConvertTo-Json -Depth 8 | Set-Content -Path $programSummaryPath -Encoding UTF8

Write-Host ("[windows-vfs-soak-program] PASS. Summary: {0}" -f $programSummaryPath) -ForegroundColor Green
