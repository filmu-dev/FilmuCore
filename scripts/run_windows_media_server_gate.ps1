param(
    [string[]] $Providers = @('jellyfin', 'emby', 'plex'),
    [int] $RepeatCount = 1,
    [string] $EnvironmentClass = '',
    [string] $MountPath = '',
    [string] $ArtifactsRoot = '',
    [string] $ContractPath = '',
    [string] $TmdbId = '603',
    [string] $Title = 'The Matrix',
    [ValidateSet('movie', 'tv')]
    [string] $MediaType = 'movie',
    [switch] $ReuseExistingItem,
    [switch] $SkipStart,
    [switch] $StopWhenDone,
    [switch] $FailFast,
    [switch] $RequireFilmuvfs,
    [switch] $DryRun
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

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

function Get-DotEnvMap {
    param([Parameter(Mandatory = $true)][string] $Path)

    $map = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $map
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#')) {
            continue
        }
        $idx = $trimmed.IndexOf('=')
        if ($idx -lt 1) {
            continue
        }
        $key = $trimmed.Substring(0, $idx).Trim()
        $value = $trimmed.Substring($idx + 1)
        $map[$key] = $value
    }

    return $map
}

function Get-DefaultMountPath {
    $systemDrive = [System.Environment]::GetEnvironmentVariable('SystemDrive')
    if ([string]::IsNullOrWhiteSpace($systemDrive)) {
        $systemDrive = 'C:'
    }
    return Join-Path $systemDrive 'FilmuCoreVFS'
}

function Get-NativePlexUrl {
    return 'http://127.0.0.1:32400'
}

function Get-NativePlexLocalAdminToken {
    $localAppData = [System.Environment]::GetEnvironmentVariable('LOCALAPPDATA')
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        return ''
    }

    $tokenPath = Join-Path $localAppData 'Plex Media Server\.LocalAdminToken'
    if (-not (Test-Path -LiteralPath $tokenPath)) {
        return ''
    }

    return [string](Get-Content -LiteralPath $tokenPath -Raw).Trim()
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
            Add-UniqueString -Values $normalized -Value $trimmed
        }
    }

    return @($normalized)
}

function Test-ProviderConfigured {
    param(
        [Parameter(Mandatory = $true)][string] $Provider,
        [Parameter(Mandatory = $true)][hashtable] $DotEnv
    )

    switch ($Provider) {
        'jellyfin' {
            return (
                -not [string]::IsNullOrWhiteSpace([string]$env:JELLYFIN_API_KEY) -or
                ($DotEnv.ContainsKey('JELLYFIN_API_KEY') -and -not [string]::IsNullOrWhiteSpace([string]$DotEnv['JELLYFIN_API_KEY']))
            )
        }
        'emby' {
            return (
                -not [string]::IsNullOrWhiteSpace([string]$env:EMBY_API_KEY) -or
                ($DotEnv.ContainsKey('EMBY_API_KEY') -and -not [string]::IsNullOrWhiteSpace([string]$DotEnv['EMBY_API_KEY']))
            )
        }
        'plex' {
            return (
                -not [string]::IsNullOrWhiteSpace([string]$env:PLEX_TOKEN) -or
                ($DotEnv.ContainsKey('PLEX_TOKEN') -and -not [string]::IsNullOrWhiteSpace([string]$DotEnv['PLEX_TOKEN'])) -or
                (-not [string]::IsNullOrWhiteSpace((Get-NativePlexLocalAdminToken)))
            )
        }
        default {
            throw ("Unsupported provider '{0}'." -f $Provider)
        }
    }
}

if ($RepeatCount -lt 1) {
    throw 'RepeatCount must be at least 1.'
}

$scriptRoot = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptRoot
$proofScript = Join-Path $scriptRoot 'run_playback_proof.ps1'
if ([string]::IsNullOrWhiteSpace($ArtifactsRoot)) {
    $ArtifactsRoot = Join-Path $repoRoot 'playback-proof-artifacts'
}
$ArtifactsRoot = [System.IO.Path]::GetFullPath($ArtifactsRoot)
New-Item -ItemType Directory -Force -Path $ArtifactsRoot | Out-Null

$contract = $null
$contractSchemaVersion = 1
$artifactKind = 'windows_native_media_proof'
$FreshnessWindowHours = 72
$RequiredProviders = @('emby', 'plex')
$RequiredMediaTypes = @('movie', 'tv')
$RequiredTopology = 'native_windows'
if ([string]::IsNullOrWhiteSpace($ContractPath)) {
    $ContractPath = Join-Path $repoRoot 'ops\rollout\windows-native-media-proof.contract.json'
}
if (Test-Path -LiteralPath $ContractPath) {
    $ContractPath = [System.IO.Path]::GetFullPath($ContractPath)
    $contract = Get-Content -LiteralPath $ContractPath -Raw | ConvertFrom-Json
    if ($null -ne $contract) {
        if ($contract.PSObject.Properties.Name -contains 'schema_version') {
            $contractSchemaVersion = [int]$contract.schema_version
        }
        if ($contract.PSObject.Properties.Name -contains 'artifact_kind') {
            $artifactKind = [string]$contract.artifact_kind
        }
        if ($contract.PSObject.Properties.Name -contains 'freshness_window_hours') {
            $FreshnessWindowHours = [int]$contract.freshness_window_hours
        }
        if ($contract.PSObject.Properties.Name -contains 'required_providers') {
            $RequiredProviders = @(Get-NormalizedStringList -Values @($contract.required_providers) -Lowercase)
            if (-not $PSBoundParameters.ContainsKey('Providers')) {
                $Providers = @($RequiredProviders)
            }
        }
        if ($contract.PSObject.Properties.Name -contains 'required_media_types') {
            $RequiredMediaTypes = @(Get-NormalizedStringList -Values @($contract.required_media_types) -Lowercase)
        }
        if ($contract.PSObject.Properties.Name -contains 'required_topology') {
            $RequiredTopology = ([string]$contract.required_topology).Trim().ToLowerInvariant()
        }
        if (
            $contract.PSObject.Properties.Name -contains 'repeat_count' -and
            -not $PSBoundParameters.ContainsKey('RepeatCount')
        ) {
            $RepeatCount = [int]$contract.repeat_count
        }
        if (
            $contract.PSObject.Properties.Name -contains 'require_filmuvfs' -and
            -not $PSBoundParameters.ContainsKey('RequireFilmuvfs')
        ) {
            $RequireFilmuvfs = [bool]$contract.require_filmuvfs
        }
    }
}

if ($RepeatCount -lt 1) {
    throw 'RepeatCount must be at least 1.'
}

$dotEnv = Get-DotEnvMap -Path (Join-Path $repoRoot '.env')
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$summaryPath = Join-Path $ArtifactsRoot ("windows-media-server-gate-{0}.json" -f $timestamp)
$shellExecutable = (Get-Process -Id $PID).Path
if ([string]::IsNullOrWhiteSpace($shellExecutable)) {
    $shellExecutable = 'pwsh'
}

$Providers = @(Get-NormalizedStringList -Values $Providers -Lowercase)
$RequiredProviders = @(Get-NormalizedStringList -Values $RequiredProviders -Lowercase)
$RequiredMediaTypes = @(Get-NormalizedStringList -Values $RequiredMediaTypes -Lowercase)
if ($Providers.Count -eq 0) {
    throw 'At least one provider is required.'
}
if ($RequiredProviders.Count -eq 0) {
    $RequiredProviders = @($Providers)
}

if ([string]::IsNullOrWhiteSpace($EnvironmentClass)) {
    $EnvironmentClass = "{0}:{1}" -f $env:COMPUTERNAME, [System.Environment]::OSVersion.VersionString
}
if ([string]::IsNullOrWhiteSpace($MountPath)) {
    $MountPath = Get-DefaultMountPath
}

$mountPathExists = Test-Path -LiteralPath $MountPath
$filmuvfsProcess = Get-Process -Name filmuvfs -ErrorAction SilentlyContinue | Select-Object -First 1
$results = [System.Collections.Generic.List[object]]::new()
$failureReasons = [System.Collections.Generic.List[string]]::new()
$requiredActions = [System.Collections.Generic.List[string]]::new()

if (-not $mountPathExists) {
    Add-UniqueString -Values $failureReasons -Value 'native_windows_mount_path_missing'
    Add-UniqueString -Values $requiredActions -Value 'mount_native_windows_vfs'
}
if ($RequireFilmuvfs -and $null -eq $filmuvfsProcess) {
    Add-UniqueString -Values $failureReasons -Value 'filmuvfs_not_running'
    Add-UniqueString -Values $requiredActions -Value 'start_filmuvfs_native_windows_stack'
}

$sharedReuse = $ReuseExistingItem
$sharedSkipStart = $SkipStart
$stopRequested = $false

foreach ($provider in $Providers) {
    $providerConfigured = Test-ProviderConfigured -Provider $provider -DotEnv $dotEnv
    if (-not $providerConfigured) {
        $results.Add([pscustomobject]@{
            environment_class = $EnvironmentClass
            media_type = $MediaType
            provider = $provider
            run = $null
            status = 'skipped'
            exit_code = $null
            topology = $null
            dry_run = [bool]$DryRun
            artifact_dir = $null
            summary_exists = $false
            playback_start_status = $null
            details = 'Provider is not configured in env/.env for this host.'
        })
        continue
    }

    for ($runIndex = 1; $runIndex -le $RepeatCount; $runIndex++) {
        if ($failureReasons.Count -gt 0 -and (-not $mountPathExists -or ($RequireFilmuvfs -and $null -eq $filmuvfsProcess))) {
            $results.Add([pscustomobject]@{
                environment_class = $EnvironmentClass
                media_type = $MediaType
                provider = $provider
                run = $runIndex
                status = 'failed'
                exit_code = $null
                topology = $null
                dry_run = [bool]$DryRun
                artifact_dir = $null
                summary_exists = $false
                playback_start_status = $null
                details = 'Native Windows playback preconditions are not satisfied.'
            })
            if ($FailFast) {
                $stopRequested = $true
                break
            }
            continue
        }

        $before = @()
        if (Test-Path -LiteralPath $ArtifactsRoot) {
            $before = @(
                Get-ChildItem -LiteralPath $ArtifactsRoot -Directory |
                    Sort-Object LastWriteTimeUtc -Descending |
                    Select-Object -ExpandProperty FullName
            )
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
        if ($provider -eq 'plex') {
            $argList.Add('-MediaServerUrl')
            $argList.Add((Get-NativePlexUrl))
            $nativePlexToken = Get-NativePlexLocalAdminToken
            if (-not [string]::IsNullOrWhiteSpace($nativePlexToken)) {
                $argList.Add('-MediaServerToken')
                $argList.Add($nativePlexToken)
            }
        }
        if ($sharedReuse) {
            $argList.Add('-ReuseExistingItem')
        }
        if ($sharedSkipStart) {
            $argList.Add('-SkipStart')
        }
        if ($DryRun) {
            $argList.Add('-DryRun')
        }

        Write-Host ("[windows-media-gate] Running provider '{0}' ({1}/{2})..." -f $provider, $runIndex, $RepeatCount)
        & $shellExecutable @argList
        $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { [int]$LASTEXITCODE }

        $after = @()
        if (Test-Path -LiteralPath $ArtifactsRoot) {
            $after = @(
                Get-ChildItem -LiteralPath $ArtifactsRoot -Directory |
                    Sort-Object LastWriteTimeUtc -Descending |
                    Select-Object -ExpandProperty FullName
            )
        }
        $artifactDir = $after | Where-Object { $before -notcontains $_ } | Select-Object -First 1
        if ([string]::IsNullOrWhiteSpace([string]$artifactDir) -and $after.Count -gt 0) {
            $artifactDir = $after[0]
        }

        $topology = $null
        $details = $null
        $summaryExists = $false
        $playbackStartStatus = $null
        if (-not [string]::IsNullOrWhiteSpace([string]$artifactDir)) {
            $artifactSummaryPath = Join-Path $artifactDir 'summary.json'
            if (Test-Path -LiteralPath $artifactSummaryPath) {
                $summaryExists = $true
                $artifactSummary = Get-Content -LiteralPath $artifactSummaryPath -Raw | ConvertFrom-Json
                if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'topology') {
                    $topology = [string]$artifactSummary.media_server.topology
                }
                if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'playback_start_status') {
                    $playbackStartStatus = [string]$artifactSummary.media_server.playback_start_status
                }
                if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'playback_start_details') {
                    $details = [string]$artifactSummary.media_server.playback_start_details
                }
                if ([string]::IsNullOrWhiteSpace($details) -and ($artifactSummary.PSObject.Properties.Name -contains 'steps')) {
                    $failedStep = @($artifactSummary.steps) | Where-Object { [string]$_.status -eq 'failed' } | Select-Object -Last 1
                    if (($null -ne $failedStep) -and ($failedStep.PSObject.Properties.Name -contains 'details')) {
                        $details = [string]$failedStep.details
                    }
                }
            }
        }

        $topologySatisfied = $DryRun -or ([string]$topology).Trim().ToLowerInvariant() -eq $RequiredTopology
        $status = if (($exitCode -eq 0) -and $summaryExists -and $topologySatisfied) { 'passed' } else { 'failed' }
        $results.Add([pscustomobject]@{
            environment_class = $EnvironmentClass
            media_type = $MediaType
            provider = $provider
            run = $runIndex
            status = $status
            exit_code = $exitCode
            topology = $topology
            dry_run = [bool]$DryRun
            artifact_dir = $artifactDir
            summary_exists = $summaryExists
            playback_start_status = $playbackStartStatus
            details = $details
        })

        if (($status -eq 'failed') -and $FailFast) {
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

$coverage = [System.Collections.Generic.List[string]]::new()
$nonNativeTopologyDetected = $false
$failedProviders = [System.Collections.Generic.List[string]]::new()
$missingConfiguredProviders = [System.Collections.Generic.List[string]]::new()
foreach ($provider in $RequiredProviders) {
    $providerResults = @(
        $results |
            Where-Object {
                ([string]$_.provider).Trim().ToLowerInvariant() -eq $provider -and
                ([string]$_.media_type).Trim().ToLowerInvariant() -eq $MediaType
            }
    )
    if ($providerResults.Count -eq 0) {
        continue
    }
    if (@($providerResults | Where-Object { [string]$_.status -eq 'passed' }).Count -gt 0) {
        Add-UniqueString -Values $coverage -Value ("{0}:{1}" -f $provider, $MediaType)
    } elseif (@($providerResults | Where-Object { [string]$_.status -eq 'skipped' }).Count -gt 0) {
        Add-UniqueString -Values $missingConfiguredProviders -Value $provider
    } else {
        Add-UniqueString -Values $failedProviders -Value $provider
    }
    if (@($providerResults | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_.topology) -and ([string]$_.topology).Trim().ToLowerInvariant() -ne $RequiredTopology }).Count -gt 0) {
        $nonNativeTopologyDetected = $true
    }
}

$requiredCoverage = @($RequiredProviders | ForEach-Object { "{0}:{1}" -f $_, $MediaType })
$coverageComplete = @($requiredCoverage | Where-Object { $coverage -notcontains $_ }).Count -eq 0
if ($missingConfiguredProviders.Count -gt 0) {
    Add-UniqueString -Values $failureReasons -Value 'native_windows_media_provider_credentials_missing'
    Add-UniqueString -Values $requiredActions -Value 'configure_native_windows_media_provider_credentials'
}
if ($failedProviders.Count -gt 0) {
    Add-UniqueString -Values $failureReasons -Value 'native_windows_media_proof_failed'
    Add-UniqueString -Values $requiredActions -Value ("rerun_native_windows_provider_proof_{0}" -f $MediaType)
}
if ($nonNativeTopologyDetected) {
    Add-UniqueString -Values $failureReasons -Value ("windows_provider_{0}_non_native_topology" -f $MediaType)
    Add-UniqueString -Values $requiredActions -Value ("rerun_native_windows_provider_proof_{0}" -f $MediaType)
}
if (-not $coverageComplete) {
    Add-UniqueString -Values $failureReasons -Value ("windows_provider_{0}_coverage_incomplete" -f $MediaType)
    Add-UniqueString -Values $requiredActions -Value ("rerun_native_windows_provider_proof_{0}" -f $MediaType)
}

$capturedAt = (Get-Date).ToUniversalTime()
$ready = $failureReasons.Count -eq 0
$summary = [ordered]@{
    schema_version = $contractSchemaVersion
    artifact_kind = $artifactKind
    timestamp = Get-UtcTimestamp -Value $capturedAt
    captured_at = Get-UtcTimestamp -Value $capturedAt
    expires_at = Get-UtcTimestamp -Value $capturedAt.AddHours([Math]::Max($FreshnessWindowHours, 1))
    freshness_window_hours = [Math]::Max($FreshnessWindowHours, 1)
    status = if ($ready) { 'passed' } else { 'failed' }
    ready = $ready
    contract_path = if ($null -ne $contract) { $ContractPath } else { $null }
    mount_path = $MountPath
    mount_path_exists = $mountPathExists
    filmuvfs_running = ($null -ne $filmuvfsProcess)
    filmuvfs_pid = if ($null -ne $filmuvfsProcess) { [int]$filmuvfsProcess.Id } else { $null }
    environment_class = $EnvironmentClass
    providers = $Providers
    required_providers = $RequiredProviders
    required_media_types = $RequiredMediaTypes
    required_topology = $RequiredTopology
    repeat_count = $RepeatCount
    tmdb_id = $TmdbId
    title = $Title
    media_type = $MediaType
    required_coverage = $requiredCoverage
    coverage = @($coverage)
    coverage_complete = $coverageComplete
    failure_reasons = @($failureReasons)
    required_actions = @($requiredActions)
    results = $results
}
$summary | ConvertTo-Json -Depth 10 | Set-Content -Path $summaryPath -Encoding UTF8

if ($StopWhenDone) {
    $stopScript = Join-Path $scriptRoot 'stop_windows_stack.ps1'
    if (Test-Path -LiteralPath $stopScript) {
        & $shellExecutable -NoProfile -File $stopScript
    }
}

if (-not $ready) {
    Write-Host ("[windows-media-gate] FAIL. Summary: {0}" -f $summaryPath)
    foreach ($failure in @($results | Where-Object { [string]$_.status -eq 'failed' })) {
        Write-Host ("[windows-media-gate] {0} failed; topology={1}; artifact={2}" -f $failure.provider, $failure.topology, $failure.artifact_dir)
    }
    exit 1
}

Write-Host ("[windows-media-gate] PASS. Summary: {0}" -f $summaryPath)
