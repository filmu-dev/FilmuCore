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
    [switch] $FailFast,
    [string] $ContractPath = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ($RepeatCount -lt 1) {
    throw 'RepeatCount must be at least 1.'
}

$scriptRoot = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptRoot
$proofScript = Join-Path $scriptRoot 'run_playback_proof.ps1'
$artifactsRoot = Join-Path $repoRoot 'playback-proof-artifacts'
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$summaryPath = Join-Path $artifactsRoot ("media-server-gate-{0}.json" -f $timestamp)
New-Item -ItemType Directory -Force -Path $artifactsRoot | Out-Null
$shellExecutable = (Get-Process -Id $PID).Path
if ([string]::IsNullOrWhiteSpace($shellExecutable)) {
    $shellExecutable = 'pwsh'

}

$contract = $null
if ([string]::IsNullOrWhiteSpace($ContractPath)) {
    $ContractPath = Join-Path $repoRoot 'ops\rollout\media-server-provider-parity.contract.json'
}
if (Test-Path -LiteralPath $ContractPath) {
    $ContractPath = [System.IO.Path]::GetFullPath($ContractPath)
    $contract = Get-Content -LiteralPath $ContractPath -Raw | ConvertFrom-Json
}
$schemaVersion = if (($null -ne $contract) -and ($contract.PSObject.Properties.Name -contains 'schema_version')) {
    [int] $contract.schema_version
}
else {
    1
}
$artifactKind = if (($null -ne $contract) -and ($contract.PSObject.Properties.Name -contains 'artifact_kind')) {
    [string] $contract.artifact_kind
}
else {
    'media_server_provider_parity'
}
$freshnessWindowHours = if (($null -ne $contract) -and ($contract.PSObject.Properties.Name -contains 'freshness_window_hours')) {
    [int] $contract.freshness_window_hours
}
else {
    24
}
if ($freshnessWindowHours -lt 1) {
    $freshnessWindowHours = 1
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

function Add-UniqueString {
    param(
        [System.Collections.Generic.List[string]] $List,
        [string] $Value
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return
    }
    if (-not $List.Contains($Value)) {
        $List.Add($Value)
    }
}

function Add-FailureActionPair {
    param(
        [System.Collections.Generic.List[string]] $FailureReasons,
        [System.Collections.Generic.List[string]] $RequiredActions,
        [string] $FailureReason,
        [string] $RequiredAction
    )

    Add-UniqueString -List $FailureReasons -Value $FailureReason
    Add-UniqueString -List $RequiredActions -Value $RequiredAction
}

function Get-ProviderGateClassification {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Result
    )

    $failureReasons = [System.Collections.Generic.List[string]]::new()
    $requiredActions = [System.Collections.Generic.List[string]]::new()

    if (-not [bool] $Result.summary_exists) {
        Add-FailureActionPair `
            -FailureReasons $failureReasons `
            -RequiredActions $requiredActions `
            -FailureReason 'provider_gate_summary_missing' `
            -RequiredAction 'rerun_media_server_provider_gate'
        return [pscustomobject]@{
            failure_reasons = @($failureReasons)
            required_actions = @($requiredActions)
        }
    }

    if (
        ($Result.provider -eq 'plex') -and
        ($Result.topology -eq 'docker_wsl') -and
        (-not [string]::IsNullOrWhiteSpace([string] $Result.playback_path)) -and
        (-not ([string] $Result.playback_path -like '/mnt/filmuvfs/*'))
    ) {
        Add-FailureActionPair `
            -FailureReasons $failureReasons `
            -RequiredActions $requiredActions `
            -FailureReason 'provider_gate_docker_plex_mount_path_drift' `
            -RequiredAction 'realign_docker_plex_mount_path'
    }

    if (($Result.provider -eq 'plex') -and ($Result.topology -eq 'docker_wsl')) {
        switch ([string] $Result.docker_wsl_mount_status) {
            'missing' {
                Add-FailureActionPair `
                    -FailureReasons $failureReasons `
                    -RequiredActions $requiredActions `
                    -FailureReason 'provider_gate_wsl_host_mount_missing' `
                    -RequiredAction 'restore_wsl_host_mount_projection'
            }
        }

        switch ([string] $Result.docker_wsl_host_binary_status) {
            'missing' {
                Add-FailureActionPair `
                    -FailureReasons $failureReasons `
                    -RequiredActions $requiredActions `
                    -FailureReason 'provider_gate_wsl_host_binary_missing' `
                    -RequiredAction 'rebuild_wsl_host_mount_binary'
            }
            'stale' {
                Add-FailureActionPair `
                    -FailureReasons $failureReasons `
                    -RequiredActions $requiredActions `
                    -FailureReason 'provider_gate_wsl_host_binary_stale' `
                    -RequiredAction 'rebuild_wsl_host_mount_binary'
            }
            'unknown' {
                Add-FailureActionPair `
                    -FailureReasons $failureReasons `
                    -RequiredActions $requiredActions `
                    -FailureReason 'provider_gate_wsl_host_binary_freshness_unknown' `
                    -RequiredAction 'record_wsl_host_binary_freshness'
            }
        }

        if ([string] $Result.docker_wsl_refresh_identity_status -eq 'not_observed') {
            Add-FailureActionPair `
                -FailureReasons $failureReasons `
                -RequiredActions $requiredActions `
                -FailureReason 'provider_gate_entry_id_refresh_identity_missing' `
                -RequiredAction 'restore_entry_id_bound_refresh_identity'
        }

        if ([string] $Result.docker_wsl_foreground_fetch_status -eq 'not_observed') {
            Add-FailureActionPair `
                -FailureReasons $failureReasons `
                -RequiredActions $requiredActions `
                -FailureReason 'provider_gate_foreground_fetch_signal_missing' `
                -RequiredAction 'record_foreground_fetch_coalescing_signal'
        }
    }

    if (
        (-not [string]::IsNullOrWhiteSpace([string] $Result.playback_start_status)) -and
        ([string] $Result.playback_start_status -ne 'started')
    ) {
        Add-FailureActionPair `
            -FailureReasons $failureReasons `
            -RequiredActions $requiredActions `
            -FailureReason 'provider_gate_playback_start_not_confirmed' `
            -RequiredAction 'repair_media_server_playback_start_route'
    }

    if (
        (-not [string]::IsNullOrWhiteSpace([string] $Result.stale_refresh_status)) -and
        ([string] $Result.stale_refresh_status -notlike 'refreshed_*') -and
        ([string] $Result.stale_refresh_status -notlike 'recovered_after_retry_*')
    ) {
        Add-FailureActionPair `
            -FailureReasons $failureReasons `
            -RequiredActions $requiredActions `
            -FailureReason 'provider_gate_stale_refresh_not_confirmed' `
            -RequiredAction 'repair_inline_stale_refresh_path'
    }

    if (($Result.exit_code -ne 0) -and ($failureReasons.Count -eq 0)) {
        Add-FailureActionPair `
            -FailureReasons $failureReasons `
            -RequiredActions $requiredActions `
            -FailureReason 'provider_gate_playback_proof_failed' `
            -RequiredAction 'repair_media_server_playback_proof'
    }

    if (($Result.status -ne 'passed') -and ($requiredActions.Count -eq 0)) {
        Add-UniqueString -List $requiredActions -Value 'rerun_media_server_provider_gate'
    }

    return [pscustomobject]@{
        failure_reasons = @($failureReasons)
        required_actions = @($requiredActions)
    }
}


$results = [System.Collections.Generic.List[object]]::new()
$summaryFailureReasons = [System.Collections.Generic.List[string]]::new()
$summaryRequiredActions = [System.Collections.Generic.List[string]]::new()
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
            playback_path = $null
            summary_exists = $false
            playback_start_status = $null
            stale_refresh_status = $null
            docker_wsl_mount_status = $null
            docker_wsl_host_binary_status = $null
            docker_wsl_refresh_identity_status = $null
            docker_wsl_foreground_fetch_status = $null
            docker_wsl_mount_visibility = $null
            docker_wsl_host_binary_freshness = $null
            docker_wsl_refresh_identity_evidence = $null
            docker_wsl_foreground_fetch_evidence = $null
            failure_reasons = @()
            required_actions = @()
        }

        $summaryExists = $false
        $artifactSummary = $null
        if (-not [string]::IsNullOrWhiteSpace([string] $artifactDir)) {
            $artifactSummaryPath = Join-Path $artifactDir 'summary.json'
            if (Test-Path -LiteralPath $artifactSummaryPath) {
                $summaryExists = $true
                $artifactSummary = Get-Content -LiteralPath $artifactSummaryPath -Raw | ConvertFrom-Json
                $result.topology = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'topology') { [string] $artifactSummary.media_server.topology } else { $null }
                $result.playback_path = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'playback_path') { [string] $artifactSummary.media_server.playback_path } else { $null }
                $result.playback_start_status = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'playback_start_status') { [string] $artifactSummary.media_server.playback_start_status } else { $null }
                $result.stale_refresh_status = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'stale_refresh_status') { [string] $artifactSummary.media_server.stale_refresh_status } else { $null }
                $result.docker_wsl_mount_status = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'wsl_host_mount_status') { [string] $artifactSummary.media_server.wsl_host_mount_status } else { $null }
                $result.docker_wsl_host_binary_status = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'wsl_host_binary_status') { [string] $artifactSummary.media_server.wsl_host_binary_status } else { $null }
                $result.docker_wsl_refresh_identity_status = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'refresh_identity_status') { [string] $artifactSummary.media_server.refresh_identity_status } else { $null }
                $result.docker_wsl_foreground_fetch_status = if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'foreground_fetch_status') { [string] $artifactSummary.media_server.foreground_fetch_status } else { $null }
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
        $classification = Get-ProviderGateClassification -Result $result
        $result.failure_reasons = @($classification.failure_reasons)
        $result.required_actions = @($classification.required_actions)
        foreach ($failureReason in @($classification.failure_reasons)) {
            Add-UniqueString -List $summaryFailureReasons -Value ([string] $failureReason)
        }
        foreach ($requiredAction in @($classification.required_actions)) {
            Add-UniqueString -List $summaryRequiredActions -Value ([string] $requiredAction)
        }

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

$capturedAt = (Get-Date).ToString('o')
$summaryReady = (@($results | Where-Object { $_.status -ne 'passed' })).Count -eq 0
$summaryStatus = if ($summaryReady) { 'passed' } else { 'failed' }
$summary = [ordered]@{
    schema_version = $schemaVersion
    artifact_kind = $artifactKind
    contract_path = if ($null -ne $contract) { $ContractPath } else { $null }
    timestamp  = $capturedAt
    captured_at = $capturedAt
    expires_at = (Get-Date).AddHours($freshnessWindowHours).ToString('o')
    freshness_window_hours = $freshnessWindowHours
    environment_class = $EnvironmentClass
    providers  = $Providers
    repeat_count = $RepeatCount
    tmdb_id    = $TmdbId
    title      = $Title
    media_type = $MediaType
    status = $summaryStatus
    ready = $summaryReady
    all_green  = $summaryReady
    failure_reasons = @($summaryFailureReasons)
    required_actions = @($summaryRequiredActions)
    results    = $results
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8

$failed = @($results | Where-Object { $_.status -ne 'passed' })
if ($failed.Count -gt 0) {
    Write-Host ("[media-server-gate] FAIL. Summary: {0}" -f $summaryPath)
    Write-Host ("[media-server-gate] failure_reasons={0}" -f ((@($summaryFailureReasons) -join ',')))
    Write-Host ("[media-server-gate] required_actions={0}" -f ((@($summaryRequiredActions) -join ',')))
    foreach ($failure in $failed) {
        Write-Host ("[media-server-gate] {0} failed; status={1}; exit_code={2}; reasons={3}; actions={4}; artifact={5}" -f $failure.provider, $failure.status, $failure.exit_code, ((@($failure.failure_reasons) -join ',')), ((@($failure.required_actions) -join ',')), $failure.artifact_dir)
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


