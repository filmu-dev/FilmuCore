param(
    [int] $RepeatCount = 3,
    [string] $TmdbId = '550',
    [string] $Title = 'Fight Club',
    [ValidateSet('movie', 'tv')]
    [string] $MediaType = 'movie',
    [string] $BackendUrl = 'http://localhost:8000',
    [string] $FrontendUrl = 'http://localhost:3000',
    [string] $FrontendUsername = '',
    [string] $FrontendPassword = '',
    [string] $PreferredClientBrowserContainer = '',
    [string] $PreferredClientBrowserExecutable = '',
    [string] $BackendContainerName = '',
    [string] $ApiKey = '',
    [string] $JellyfinApiKey = '',
    [string] $MediaServerUrl = 'http://localhost:8096',
    [int] $PreferredClientPlaybackTimeoutSeconds = 90,
    [string] $EnvironmentClass = '',
    [int] $MaxRunDurationSeconds = 0,
    [int] $MaxReconnectIncidentCount = 0,
    [switch] $ProofStaleDirectRefresh,
    [switch] $RequirePreferredClientPlayback,
    [switch] $RequireMediaServerProof,
    [switch] $ReuseExistingItem,
    [switch] $RequireCompletedState,
    [switch] $StopWhenDone,
    [switch] $DryRun
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Get-OptionalObjectPropertyValue {
    param(
        [AllowNull()][object] $InputObject,
        [Parameter(Mandatory = $true)][string] $Name
    )

    if ($null -eq $InputObject) {
        return $null
    }

    if ($InputObject.PSObject.Properties.Name -contains $Name) {
        return $InputObject.$Name
    }

    return $null
}

if ($RepeatCount -lt 1) {
    throw 'RepeatCount must be at least 1.'
}

if ([string]::IsNullOrWhiteSpace($EnvironmentClass)) {
    $EnvironmentClass = "{0}:{1}" -f $env:COMPUTERNAME, [System.Environment]::OSVersion.VersionString
}

$scriptRoot = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptRoot
$singleRunScript = Join-Path $scriptRoot 'run_playback_proof.ps1'
$artifactsRoot = Join-Path $repoRoot 'playback-proof-artifacts'
$runTimestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$stabilitySummaryPath = Join-Path $artifactsRoot ("stability-summary-{0}.json" -f $runTimestamp)

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    $ApiKey = if ([string]::IsNullOrWhiteSpace($env:FILMU_PY_API_KEY)) {
        '32_character_filmu_api_key_local_'
    }
    else {
        $env:FILMU_PY_API_KEY
    }
}

if ([string]::IsNullOrWhiteSpace($JellyfinApiKey)) {
    $JellyfinApiKey = [string] $env:JELLYFIN_API_KEY
}

if ([string]::IsNullOrWhiteSpace($FrontendUsername)) {
    $FrontendUsername = [string] $env:FILMU_FRONTEND_USERNAME
}

if ([string]::IsNullOrWhiteSpace($FrontendPassword)) {
    $FrontendPassword = [string] $env:FILMU_FRONTEND_PASSWORD
}

if ([string]::IsNullOrWhiteSpace($PreferredClientBrowserContainer)) {
    $PreferredClientBrowserContainer = [string] $env:FILMU_PLAYWRIGHT_CONTAINER_NAME
}

if ([string]::IsNullOrWhiteSpace($PreferredClientBrowserExecutable)) {
    $PreferredClientBrowserExecutable = [string] $env:FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE
}

if ([string]::IsNullOrWhiteSpace($BackendContainerName)) {
    $BackendContainerName = [string] $env:FILMU_BACKEND_CONTAINER_NAME
}

$shellExecutable = (Get-Process -Id $PID).Path
if ([string]::IsNullOrWhiteSpace($shellExecutable)) {
    $shellExecutable = 'pwsh'
}

$isWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
    [System.Runtime.InteropServices.OSPlatform]::Windows
)

$results = [System.Collections.Generic.List[object]]::new()

for ($index = 1; $index -le $RepeatCount; $index++) {
    $artifactNamesBefore = @(
        Get-ChildItem $artifactsRoot -Directory -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty Name
    )

    $arguments = @(
        '-NoProfile',
        '-File', $singleRunScript,
        '-TmdbId', $TmdbId,
        '-Title', $Title,
        '-MediaType', $MediaType,
        '-BackendUrl', $BackendUrl,
        '-FrontendUrl', $FrontendUrl,
        '-ApiKey', $ApiKey,
        '-SkipStart'
    )
    if ($isWindowsHost) {
        $arguments = @('-ExecutionPolicy', 'Bypass') + $arguments
    }

    if (-not [string]::IsNullOrWhiteSpace($FrontendUsername)) {
        $arguments += @('-FrontendUsername', $FrontendUsername)
    }
    if (-not [string]::IsNullOrWhiteSpace($FrontendPassword)) {
        $arguments += @('-FrontendPassword', $FrontendPassword)
    }
    if (-not [string]::IsNullOrWhiteSpace($PreferredClientBrowserContainer)) {
        $arguments += @('-PreferredClientBrowserContainer', $PreferredClientBrowserContainer)
    }
    if (-not [string]::IsNullOrWhiteSpace($PreferredClientBrowserExecutable)) {
        $arguments += @('-PreferredClientBrowserExecutable', $PreferredClientBrowserExecutable)
    }
    if (-not [string]::IsNullOrWhiteSpace($BackendContainerName)) {
        $arguments += @('-BackendContainerName', $BackendContainerName)
    }
    if ($PreferredClientPlaybackTimeoutSeconds -gt 0) {
        $arguments += @('-PreferredClientPlaybackTimeoutSeconds', $PreferredClientPlaybackTimeoutSeconds)
    }

    if (-not [string]::IsNullOrWhiteSpace($JellyfinApiKey)) {
        $arguments += @('-JellyfinApiKey', $JellyfinApiKey)
        $arguments += @('-MediaServerUrl', $MediaServerUrl)
    }

    if ($DryRun) {
        $arguments += '-DryRun'
    }

    if ($ProofStaleDirectRefresh) {
        $arguments += '-ProofStaleDirectRefresh'
    }
    if ($RequirePreferredClientPlayback) {
        $arguments += '-RequirePreferredClientPlayback'
    }

    if ($ReuseExistingItem) {
        $arguments += '-ReuseExistingItem'
    }

    Write-Host ("[playback-proof-stability] run {0}/{1}" -f $index, $RepeatCount) -ForegroundColor Cyan
    & $shellExecutable @arguments
    $exitCode = $LASTEXITCODE

    $artifactNamesAfter = @(
        Get-ChildItem $artifactsRoot -Directory -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty Name
    )
    $newArtifacts = @(
        $artifactNamesAfter | Where-Object { $_ -notin $artifactNamesBefore }
    )
    $artifactName = $newArtifacts | Sort-Object | Select-Object -Last 1
    $summaryPath = if ($null -ne $artifactName) {
        Join-Path (Join-Path $artifactsRoot $artifactName) 'summary.json'
    }
    else {
        $null
    }

    $summaryExists = $false
    $summary = $null
    $completedState = $null
    $staleRefreshStatus = $null
    $preferredClientStatus = $null
    $mediaServerVisibilityStatus = $null
    $mediaServerStreamOpenStatus = $null
    $durationSeconds = $null
    $reconnectIncidentCount = $null
    $failureClassesObserved = @()
    if (-not [string]::IsNullOrWhiteSpace($summaryPath) -and (Test-Path $summaryPath)) {
        $summaryExists = $true
        $summary = Get-Content $summaryPath -Raw | ConvertFrom-Json
        $completedState = [string] $summary.movie.final_state
        $staleRefreshStatus = [string] $summary.media_server.stale_refresh_status
        $preferredClient = Get-OptionalObjectPropertyValue -InputObject $summary -Name 'preferred_client'
        $mediaServer = Get-OptionalObjectPropertyValue -InputObject $summary -Name 'media_server'
        $hardening = Get-OptionalObjectPropertyValue -InputObject $summary -Name 'hardening'
        $preferredClientStatus = [string] (Get-OptionalObjectPropertyValue -InputObject $preferredClient -Name 'status')
        $mediaServerVisibilityStatus = [string] (Get-OptionalObjectPropertyValue -InputObject $mediaServer -Name 'visibility_status')
        $mediaServerStreamOpenStatus = [string] (Get-OptionalObjectPropertyValue -InputObject $mediaServer -Name 'stream_open_status')
        $durationSeconds = Get-OptionalObjectPropertyValue -InputObject $hardening -Name 'duration_seconds'
        $reconnectIncidentCount = Get-OptionalObjectPropertyValue -InputObject $hardening -Name 'reconnect_incident_count'
        $failureClassesObserved = @(
            Get-OptionalObjectPropertyValue -InputObject $hardening -Name 'failure_classes_observed'
        )
    }

    $runPassed = $exitCode -eq 0 -and $summaryExists
    if ($RequireCompletedState -and -not $DryRun) {
        $runPassed = $runPassed -and ($completedState -eq 'Completed')
    }
    if ($ProofStaleDirectRefresh -and -not $DryRun) {
        $runPassed = $runPassed -and (-not [string]::IsNullOrWhiteSpace($staleRefreshStatus))
    }
    if ($RequirePreferredClientPlayback -and -not $DryRun) {
        $runPassed = $runPassed -and ($preferredClientStatus -eq 'playing')
    }
    if ($RequireMediaServerProof -and -not $DryRun) {
        $runPassed = $runPassed -and (
            ($mediaServerVisibilityStatus -eq 'visible') -and
            ($mediaServerStreamOpenStatus -eq 'stream_open')
        )
    }
    if (($MaxRunDurationSeconds -gt 0) -and -not $DryRun -and ($null -ne $durationSeconds)) {
        $runPassed = $runPassed -and ([double] $durationSeconds -le $MaxRunDurationSeconds)
    }
    # Zero is an active enterprise threshold: the gate passes 0 to require no reconnect incidents.
    if (($MaxReconnectIncidentCount -ge 0) -and -not $DryRun) {
        $runPassed = $runPassed -and (
            ($null -ne $reconnectIncidentCount) -and
            ([int] $reconnectIncidentCount -le $MaxReconnectIncidentCount)
        )
    }

    $results.Add(
        [pscustomobject]@{
            environment_class = $EnvironmentClass
            run = $index
            exit_code = $exitCode
            timestamp = (Get-Date).ToString('o')
            passed = $runPassed
            artifact = $artifactName
            summary_path = $summaryPath
            summary_exists = $summaryExists
            final_state = $completedState
            stale_refresh_status = $staleRefreshStatus
            preferred_client_status = $preferredClientStatus
            media_server_visibility_status = $mediaServerVisibilityStatus
            media_server_stream_open_status = $mediaServerStreamOpenStatus
            duration_seconds = $durationSeconds
            reconnect_incident_count = $reconnectIncidentCount
            run_duration_within_threshold = if ($MaxRunDurationSeconds -gt 0) {
                (($null -ne $durationSeconds) -and ([double] $durationSeconds -le $MaxRunDurationSeconds))
            } else {
                $true
            }
            reconnect_within_threshold = if ($DryRun) {
                $true
            } elseif ($MaxReconnectIncidentCount -ge 0) {
                (($null -ne $reconnectIncidentCount) -and ([int] $reconnectIncidentCount -le $MaxReconnectIncidentCount))
            } else {
                $true
            }
            failure_classes_observed = $failureClassesObserved
        }
    )

    if (-not $runPassed) {
        break
    }
}

$summary = [ordered]@{
    timestamp = (Get-Date).ToString('o')
    environment_class = $EnvironmentClass
    repeat_count = $RepeatCount
    dry_run = [bool] $DryRun
    tmdb_id = $TmdbId
    title = $Title
    media_type = $MediaType
    jellyfin_enabled = -not [string]::IsNullOrWhiteSpace($JellyfinApiKey)
    proof_stale_direct_refresh = [bool] $ProofStaleDirectRefresh
    require_preferred_client_playback = [bool] $RequirePreferredClientPlayback
    preferred_client_browser_executable = $PreferredClientBrowserExecutable
    backend_container_name = $BackendContainerName
    reuse_existing_item = [bool] $ReuseExistingItem
    require_completed_state = [bool] $RequireCompletedState
    enterprise_policy = [ordered]@{
        require_completed_state = [bool] $RequireCompletedState
        proof_stale_direct_refresh = [bool] $ProofStaleDirectRefresh
        require_preferred_client_playback = [bool] $RequirePreferredClientPlayback
        require_media_server_proof = [bool] $RequireMediaServerProof
        max_run_duration_seconds = if ($MaxRunDurationSeconds -gt 0) { $MaxRunDurationSeconds } else { $null }
        max_reconnect_incident_count = if ($MaxReconnectIncidentCount -ge 0) { $MaxReconnectIncidentCount } else { $null }
    }
    max_run_duration_seconds = @($results | ForEach-Object {
        if ($null -ne $_.duration_seconds) { [double] $_.duration_seconds } else { [double] 0 }
    } | Measure-Object -Maximum).Maximum
    max_reconnect_incident_count = @($results | ForEach-Object {
        if ($null -ne $_.reconnect_incident_count) { [int] $_.reconnect_incident_count } else { [int] 0 }
    } | Measure-Object -Maximum).Maximum
    all_green = (@($results | Where-Object { -not $_.passed })).Count -eq 0
    runs = $results
}

$summary | ConvertTo-Json -Depth 10 | Set-Content -Path $stabilitySummaryPath -Encoding UTF8

if ($StopWhenDone) {
    & (Join-Path $repoRoot 'stop_local_stack.ps1')
}

if (-not $summary.all_green) {
    throw ("[playback-proof-stability] one or more runs failed; summary written to {0}" -f $stabilitySummaryPath)
}

Write-Host ("[playback-proof-stability] PASS. Summary: {0}" -f $stabilitySummaryPath) -ForegroundColor Green
