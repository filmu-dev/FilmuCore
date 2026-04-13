param(
    [string[]] $Profiles = @('continuous', 'seek', 'concurrent', 'full'),
    [int] $RepeatCount = 1,
    [string] $EnvironmentClass = '',
    [string] $MountPath = '',
    [string] $BackendUrl = 'http://localhost:8000',
    [string] $ApiKey = '',
    [string] $TargetFile = '',
    [string] $RemuxTargetFile = '',
    [string] $FfmpegPath = '',
    [switch] $RequireRuntimeCapture,
    [switch] $RequireBackendStatusCapture,
    [int] $MaxReconnectIncidents = 0,
    [int] $MaxProviderPressureIncidents = 0,
    [int] $MaxFatalErrorIncidents = 0,
    [switch] $RequireFilmuvfs,
    [switch] $FailFast
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ($RepeatCount -lt 1) {
    throw 'RepeatCount must be at least 1.'
}

$allowedProfiles = @('continuous', 'seek', 'concurrent', 'full')
$normalizedProfiles = [System.Collections.Generic.List[string]]::new()
foreach ($profileEntry in $Profiles) {
    foreach ($profileToken in ([string] $profileEntry).Split(',',[System.StringSplitOptions]::RemoveEmptyEntries)) {
        $trimmed = $profileToken.Trim().ToLowerInvariant()
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            continue
        }
        if ($allowedProfiles -notcontains $trimmed) {
            throw ("Unsupported soak profile '{0}'." -f $trimmed)
        }
        $normalizedProfiles.Add($trimmed)
    }
}
$Profiles = @($normalizedProfiles)
if ($Profiles.Count -eq 0) {
    throw 'At least one soak profile is required.'
}

if ([string]::IsNullOrWhiteSpace($EnvironmentClass)) {
    $EnvironmentClass = "{0}:{1}" -f $env:COMPUTERNAME, [System.Environment]::OSVersion.VersionString
}

$scriptRoot = $PSScriptRoot
$singleRunScript = Join-Path $scriptRoot 'run_windows_vfs_soak.ps1'
$artifactsRoot = Join-Path (Split-Path -Parent $scriptRoot) 'playback-proof-artifacts\windows-native-stack'
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$summaryPath = Join-Path $artifactsRoot ("soak-stability-{0}.json" -f $timestamp)
New-Item -ItemType Directory -Force -Path $artifactsRoot | Out-Null
$shellExecutable = (Get-Process -Id $PID).Path
if ([string]::IsNullOrWhiteSpace($shellExecutable)) {
    $shellExecutable = 'pwsh'
}

$results = [System.Collections.Generic.List[object]]::new()
$stopRequested = $false

foreach ($profile in $Profiles) {
    for ($runIndex = 1; $runIndex -le $RepeatCount; $runIndex++) {
        $before = @()
        if (Test-Path -LiteralPath $artifactsRoot) {
            $before = @(Get-ChildItem -LiteralPath $artifactsRoot -Directory | Sort-Object LastWriteTimeUtc -Descending | Select-Object -ExpandProperty FullName)
        }

        $arguments = [System.Collections.Generic.List[string]]::new()
        $arguments.Add('-NoProfile')
        $arguments.Add('-File')
        $arguments.Add($singleRunScript)
        $arguments.Add('-SoakProfile')
        $arguments.Add($profile)
        if (-not [string]::IsNullOrWhiteSpace($MountPath)) {
            $arguments.Add('-MountPath')
            $arguments.Add($MountPath)
        }
        if (-not [string]::IsNullOrWhiteSpace($BackendUrl)) {
            $arguments.Add('-BackendUrl')
            $arguments.Add($BackendUrl)
        }
        if (-not [string]::IsNullOrWhiteSpace($ApiKey)) {
            $arguments.Add('-ApiKey')
            $arguments.Add($ApiKey)
        }
        if (-not [string]::IsNullOrWhiteSpace($TargetFile)) {
            $arguments.Add('-TargetFile')
            $arguments.Add($TargetFile)
        }
        if (-not [string]::IsNullOrWhiteSpace($RemuxTargetFile)) {
            $arguments.Add('-RemuxTargetFile')
            $arguments.Add($RemuxTargetFile)
        }
        if (-not [string]::IsNullOrWhiteSpace($FfmpegPath)) {
            $arguments.Add('-FfmpegPath')
            $arguments.Add($FfmpegPath)
        }
        if ($RequireFilmuvfs) {
            $arguments.Add('-RequireFilmuvfs')
        }

        Write-Host ("[windows-vfs-soak-stability] profile '{0}' run {1}/{2}" -f $profile, $runIndex, $RepeatCount) -ForegroundColor Cyan
        & $shellExecutable @arguments
        $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int] $LASTEXITCODE }

        $after = @()
        if (Test-Path -LiteralPath $artifactsRoot) {
            $after = @(Get-ChildItem -LiteralPath $artifactsRoot -Directory | Sort-Object LastWriteTimeUtc -Descending | Select-Object -ExpandProperty FullName)
        }
        $artifactDir = $after | Where-Object { $before -notcontains $_ } | Select-Object -First 1
        if ([string]::IsNullOrWhiteSpace([string] $artifactDir) -and $after.Count -gt 0) {
            $artifactDir = $after[0]
        }

        $summaryExists = $false
        $thresholdFailureCount = $null
        $mountSurvived = $null
        $reconnectIncidents = $null
        $providerPressureIncidents = $null
        $fatalErrorIncidents = $null
        $runtimeCaptured = $false
        $backendCaptured = $false
        $runtimeCachePressureClass = $null
        $runtimeChunkCoalescingPressureClass = $null
        $runtimeUpstreamWaitClass = $null
        $runtimeRefreshPressureClass = $null
        if (-not [string]::IsNullOrWhiteSpace([string] $artifactDir)) {
            $artifactSummaryPath = Join-Path $artifactDir 'summary.json'
            if (Test-Path -LiteralPath $artifactSummaryPath) {
                $summaryExists = $true
                $artifactSummary = Get-Content -LiteralPath $artifactSummaryPath -Raw | ConvertFrom-Json
                $thresholdFailures = if ($artifactSummary.PSObject.Properties.Name -contains 'threshold_failures') {
                    @($artifactSummary.threshold_failures | Where-Object { $null -ne $_ })
                } else {
                    @()
                }
                $thresholdFailureCount = $thresholdFailures.Count
                $mountSurvived = [bool]$artifactSummary.mount_survived
                $reconnectIncidents = [int]($artifactSummary.diagnostics.reconnect_incidents ?? 0)
                $providerPressureIncidents = [int]($artifactSummary.runtime_diagnostics.provider_pressure_incidents ?? 0)
                $fatalErrorIncidents = [int]($artifactSummary.runtime_diagnostics.fatal_error_incidents ?? 0)
                $runtimeCaptured = [bool]($artifactSummary.runtime_diagnostics.captured)
                $backendCaptured = [bool]($artifactSummary.backend_stream_status_after.captured)
                $runtimeCachePressureClass = [string]($artifactSummary.runtime_diagnostics.cache_pressure_class ?? '')
                $runtimeChunkCoalescingPressureClass = [string]($artifactSummary.runtime_diagnostics.chunk_coalescing_pressure_class ?? '')
                $runtimeUpstreamWaitClass = [string]($artifactSummary.runtime_diagnostics.upstream_wait_class ?? '')
                $runtimeRefreshPressureClass = [string]($artifactSummary.runtime_diagnostics.refresh_pressure_class ?? '')
            }
        }

        $runtimeCaptureOk = (-not $RequireRuntimeCapture) -or $runtimeCaptured
        $backendCaptureOk = (-not $RequireBackendStatusCapture) -or $backendCaptured
        $reconnectOk = ($null -eq $reconnectIncidents) -or ($reconnectIncidents -le $MaxReconnectIncidents)
        $providerPressureOk = ($null -eq $providerPressureIncidents) -or ($providerPressureIncidents -le $MaxProviderPressureIncidents)
        $fatalErrorsOk = ($null -eq $fatalErrorIncidents) -or ($fatalErrorIncidents -le $MaxFatalErrorIncidents)
        $passed = (
            ($exitCode -eq 0) -and
            $summaryExists -and
            ($thresholdFailureCount -eq 0) -and
            $mountSurvived -and
            $runtimeCaptureOk -and
            $backendCaptureOk -and
            $reconnectOk -and
            $providerPressureOk -and
            $fatalErrorsOk
        )
        $result = [pscustomobject]@{
            environment_class = $EnvironmentClass
            profile = $profile
            run = $runIndex
            exit_code = $exitCode
            status = if ($passed) { 'passed' } else { 'failed' }
            artifact_dir = $artifactDir
            summary_exists = $summaryExists
            threshold_failure_count = $thresholdFailureCount
            mount_survived = $mountSurvived
            reconnect_incidents = $reconnectIncidents
            provider_pressure_incidents = $providerPressureIncidents
            fatal_error_incidents = $fatalErrorIncidents
            runtime_cache_pressure_class = $runtimeCachePressureClass
            runtime_chunk_coalescing_pressure_class = $runtimeChunkCoalescingPressureClass
            runtime_upstream_wait_class = $runtimeUpstreamWaitClass
            runtime_refresh_pressure_class = $runtimeRefreshPressureClass
            runtime_captured = $runtimeCaptured
            backend_status_captured = $backendCaptured
            runtime_capture_ok = $runtimeCaptureOk
            backend_status_capture_ok = $backendCaptureOk
            reconnect_within_threshold = $reconnectOk
            provider_pressure_within_threshold = $providerPressureOk
            fatal_errors_within_threshold = $fatalErrorsOk
        }
        $results.Add($result)

        if (($result.status -eq 'failed') -and $FailFast) {
            $stopRequested = $true
            break
        }
    }

    if ($stopRequested) {
        break
    }
}

$summary = [ordered]@{
    timestamp = (Get-Date).ToString('o')
    environment_class = $EnvironmentClass
    repeat_count = $RepeatCount
    profiles = $Profiles
    enterprise_policy = [ordered]@{
        require_runtime_capture = [bool] $RequireRuntimeCapture
        require_backend_status_capture = [bool] $RequireBackendStatusCapture
        max_reconnect_incidents = $MaxReconnectIncidents
        max_provider_pressure_incidents = $MaxProviderPressureIncidents
        max_fatal_error_incidents = $MaxFatalErrorIncidents
    }
    all_green = (@($results | Where-Object { $_.status -ne 'passed' })).Count -eq 0
    max_reconnect_incidents = @($results | ForEach-Object { [int]($_.reconnect_incidents ?? 0) } | Measure-Object -Maximum).Maximum
    max_provider_pressure_incidents = @($results | ForEach-Object { [int]($_.provider_pressure_incidents ?? 0) } | Measure-Object -Maximum).Maximum
    max_fatal_error_incidents = @($results | ForEach-Object { [int]($_.fatal_error_incidents ?? 0) } | Measure-Object -Maximum).Maximum
    critical_cache_pressure_runs = @($results | Where-Object { $_.runtime_cache_pressure_class -eq 'critical' }).Count
    critical_chunk_coalescing_pressure_runs = @($results | Where-Object { $_.runtime_chunk_coalescing_pressure_class -eq 'critical' }).Count
    critical_upstream_wait_runs = @($results | Where-Object { $_.runtime_upstream_wait_class -eq 'critical' }).Count
    critical_refresh_pressure_runs = @($results | Where-Object { $_.runtime_refresh_pressure_class -eq 'critical' }).Count
    results = $results
}
$summary | ConvertTo-Json -Depth 10 | Set-Content -Path $summaryPath -Encoding UTF8

if (-not $summary.all_green) {
    throw ("[windows-vfs-soak-stability] one or more runs failed; summary written to {0}" -f $summaryPath)
}

Write-Host ("[windows-vfs-soak-stability] PASS. Summary: {0}" -f $summaryPath) -ForegroundColor Green
