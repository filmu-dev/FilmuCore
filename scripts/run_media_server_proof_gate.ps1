param(
    [string[]] $Providers = @('plex', 'emby'),
    [int] $RepeatCount = 1,
    [string] $EnvironmentClass = '',
    [string] $TmdbId = '603',
    [string] $Title = 'The Matrix',
    [ValidateSet('movie', 'tv')]
    [string] $MediaType = 'movie',
    [switch] $ReuseExistingItem,
    [switch] $SkipStart,
    [switch] $ProofStaleDirectRefresh,
    [switch] $StopWhenDone,
    [switch] $DryRun,
    [switch] $FailFast
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ($RepeatCount -lt 1) {
    throw 'RepeatCount must be at least 1.'
}

$scriptRoot = $PSScriptRoot
$proofScript = Join-Path $scriptRoot 'run_playback_proof.ps1'
$artifactsRoot = Join-Path (Split-Path -Parent $scriptRoot) 'playback-proof-artifacts'
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$summaryPath = Join-Path $artifactsRoot ("media-server-gate-{0}.json" -f $timestamp)
New-Item -ItemType Directory -Force -Path $artifactsRoot | Out-Null
$shellExecutable = (Get-Process -Id $PID).Path
if ([string]::IsNullOrWhiteSpace($shellExecutable)) {
    $shellExecutable = 'pwsh'

}
$normalizedProviders = [System.Collections.Generic.List[string]]::new()
foreach ($providerEntry in $Providers) {
    foreach ($providerToken in ([string] $providerEntry).Split(',',[System.StringSplitOptions]::RemoveEmptyEntries)) {
        $trimmedProvider = $providerToken.Trim()
        if (-not [string]::IsNullOrWhiteSpace($trimmedProvider)) {
            $normalizedProviders.Add($trimmedProvider)
        }
    }
}
$Providers = @($normalizedProviders)

if ($Providers.Count -eq 0) {
    throw 'At least one provider is required.'
}

if ([string]::IsNullOrWhiteSpace($EnvironmentClass)) {
    $EnvironmentClass = "{0}:{1}" -f $env:COMPUTERNAME, [System.Environment]::OSVersion.VersionString
}

function Get-StepStatus {
    param(
        [object] $Summary,
        [string] $StepName
    )

    if ($null -eq $Summary -or -not ($Summary.PSObject.Properties.Name -contains 'steps')) {
        return $null
    }

    $step = @($Summary.steps | Where-Object { [string] $_.name -eq $StepName } | Select-Object -First 1)[0]
    if ($null -eq $step) {
        return $null
    }
    return [string] $step.status
}


$results = [System.Collections.Generic.List[object]]::new()
$sharedReuse = $ReuseExistingItem
$sharedSkipStart = $SkipStart
$stopRequested = $false

foreach ($provider in $Providers) {
    $knownProviders = @('plex', 'emby')
    if ($knownProviders -notcontains $provider) {
        throw ("Unsupported provider '{0}'." -f $provider)
    }

    for ($runIndex = 1; $runIndex -le $RepeatCount; $runIndex++) {
        $before = @()
        if (Test-Path -LiteralPath $artifactsRoot) {
            $before = @(Get-ChildItem -LiteralPath $artifactsRoot -Directory | Sort-Object LastWriteTimeUtc -Descending | Select-Object -ExpandProperty FullName)
        }

        $argList = [System.Collections.Generic.List[string]]::new()
        $argList.Add('-NoProfile')
        $argList.Add('-File')
        $argList.Add($proofScript)
        $argList.Add('-MediaServerProvider')
        $argList.Add($provider)
        $argList.Add('-TmdbId')
        $argList.Add($TmdbId)
        $argList.Add('-Title')
        $argList.Add($Title)
        $argList.Add('-MediaType')
        $argList.Add($MediaType)
        if ($sharedReuse) { $argList.Add('-ReuseExistingItem') }
        if ($sharedSkipStart) { $argList.Add('-SkipStart') }
        if ($ProofStaleDirectRefresh) { $argList.Add('-ProofStaleDirectRefresh') }
        if ($DryRun) { $argList.Add('-DryRun') }

        Write-Host ("[media-server-gate] Running provider '{0}' ({1}/{2})..." -f $provider, $runIndex, $RepeatCount)
        & $shellExecutable @argList
        $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int] $LASTEXITCODE }

        $after = @()
        if (Test-Path -LiteralPath $artifactsRoot) {
            $after = @(Get-ChildItem -LiteralPath $artifactsRoot -Directory | Sort-Object LastWriteTimeUtc -Descending | Select-Object -ExpandProperty FullName)
        }
        $artifactDir = $after | Where-Object { $before -notcontains $_ } | Select-Object -First 1
        if ([string]::IsNullOrWhiteSpace([string] $artifactDir) -and $after.Count -gt 0) {
            $artifactDir = $after[0]
        }

        $result = [pscustomobject]@{
            environment_class = $EnvironmentClass
            provider     = $provider
            run          = $runIndex
            exit_code    = $exitCode
            status       = 'failed'
            artifact_dir = $artifactDir
            topology     = $null
            summary_exists = $false
            playback_start_status = $null
            stale_refresh_status = $null
            docker_wsl_mount_visibility = $null
            docker_wsl_host_binary_freshness = $null
            docker_wsl_refresh_identity_evidence = $null
            docker_wsl_foreground_fetch_evidence = $null
        }

        $summaryExists = $false
        $artifactSummary = $null
        if (-not [string]::IsNullOrWhiteSpace([string] $artifactDir)) {
            $artifactSummaryPath = Join-Path $artifactDir 'summary.json'
            if (Test-Path -LiteralPath $artifactSummaryPath) {
                $summaryExists = $true
                $artifactSummary = Get-Content -LiteralPath $artifactSummaryPath -Raw | ConvertFrom-Json
                $result.topology = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'topology') { [string] $artifactSummary.media_server.topology } else { $null }
                $result.playback_start_status = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'playback_start_status') { [string] $artifactSummary.media_server.playback_start_status } else { $null }
                $result.stale_refresh_status = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'stale_refresh_status') { [string] $artifactSummary.media_server.stale_refresh_status } else { $null }
                $result.docker_wsl_mount_visibility = Get-StepStatus -Summary $artifactSummary -StepName 'plex_wsl_mount_visibility'
                $result.docker_wsl_host_binary_freshness = Get-StepStatus -Summary $artifactSummary -StepName 'plex_wsl_host_binary_freshness'
                $result.docker_wsl_refresh_identity_evidence = Get-StepStatus -Summary $artifactSummary -StepName 'plex_wsl_refresh_identity_evidence'
                $result.docker_wsl_foreground_fetch_evidence = Get-StepStatus -Summary $artifactSummary -StepName 'plex_wsl_foreground_fetch_evidence'
            }
        }
        $result.summary_exists = $summaryExists

        $explicitDockerPlexEvidencePassed = $true
        if ($provider -eq 'plex' -and $result.topology -eq 'docker_wsl') {
            $explicitDockerPlexEvidencePassed = (
                ($result.docker_wsl_mount_visibility -eq 'passed') -and
                ($result.docker_wsl_host_binary_freshness -eq 'passed') -and
                ($result.docker_wsl_refresh_identity_evidence -eq 'passed') -and
                ($result.docker_wsl_foreground_fetch_evidence -eq 'passed')
            )
        }
        $result.status = if (($exitCode -eq 0) -and $summaryExists -and $explicitDockerPlexEvidencePassed) { 'passed' } else { 'failed' }

        $results.Add($result)

        if ($result.status -ne 'passed' -and $FailFast) {
            $stopRequested = $true
            break
        }

        $sharedReuse = $true
        $sharedSkipStart = $true
    }

    if ($stopRequested) {
        break
    }
}

$summary = [pscustomobject]@{
    timestamp  = (Get-Date).ToString('o')
    environment_class = $EnvironmentClass
    providers  = $Providers
    repeat_count = $RepeatCount
    tmdb_id    = $TmdbId
    title      = $Title
    media_type = $MediaType
    all_green  = (@($results | Where-Object { $_.status -ne 'passed' })).Count -eq 0
    results    = $results
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8

$failed = @($results | Where-Object { $_.status -ne 'passed' })
if ($failed.Count -gt 0) {
    Write-Host ("[media-server-gate] FAIL. Summary: {0}" -f $summaryPath)
    foreach ($failure in $failed) {
        Write-Host ("[media-server-gate] {0} failed; status={1}; exit_code={2}; artifact={3}" -f $failure.provider, $failure.status, $failure.exit_code, $failure.artifact_dir)
    }
    exit 1
}

if ($StopWhenDone) {
    $stopScript = Join-Path $scriptRoot 'stop_local_stack.ps1'
    if (Test-Path -LiteralPath $stopScript) {
        & $shellExecutable -NoProfile -File $stopScript
    }
}

Write-Host ("[media-server-gate] PASS. Summary: {0}" -f $summaryPath)


