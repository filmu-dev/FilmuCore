param(
    [string[]] $Providers = @('jellyfin', 'emby', 'plex'),
    [string] $MountPath = '',
    [string] $TmdbId = '603',
    [string] $Title = 'The Matrix',
    [ValidateSet('movie', 'tv')]
    [string] $MediaType = 'movie',
    [switch] $ReuseExistingItem,
    [switch] $SkipStart,
    [switch] $StopWhenDone,
    [switch] $FailFast,
    [switch] $RequireFilmuvfs
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Get-DotEnvMap {
    param([Parameter(Mandatory = $true)][string] $Path)
    $map = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $map
    }
    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#')) { continue }
        $idx = $trimmed.IndexOf('=')
        if ($idx -lt 1) { continue }
        $key = $trimmed.Substring(0, $idx).Trim()
        $value = $trimmed.Substring($idx + 1)
        $map[$key] = $value
    }
    return $map
}

function Get-DefaultMountPath {
    $systemDrive = [System.Environment]::GetEnvironmentVariable('SystemDrive')
    if ([string]::IsNullOrWhiteSpace($systemDrive)) { $systemDrive = 'C:' }
    Join-Path $systemDrive 'FilmuCoreVFS'
}

function Test-ProviderConfigured {
    param(
        [Parameter(Mandatory = $true)][string] $Provider,
        [Parameter(Mandatory = $true)][hashtable] $DotEnv
    )

    switch ($Provider) {
        'jellyfin' {
            return -not [string]::IsNullOrWhiteSpace([string] $env:JELLYFIN_API_KEY) -or ($DotEnv.ContainsKey('JELLYFIN_API_KEY') -and -not [string]::IsNullOrWhiteSpace([string] $DotEnv['JELLYFIN_API_KEY']))
        }
        'emby' {
            return -not [string]::IsNullOrWhiteSpace([string] $env:EMBY_API_KEY) -or ($DotEnv.ContainsKey('EMBY_API_KEY') -and -not [string]::IsNullOrWhiteSpace([string] $DotEnv['EMBY_API_KEY']))
        }
        'plex' {
            return -not [string]::IsNullOrWhiteSpace([string] $env:PLEX_TOKEN) -or ($DotEnv.ContainsKey('PLEX_TOKEN') -and -not [string]::IsNullOrWhiteSpace([string] $DotEnv['PLEX_TOKEN']))
        }
        default {
            throw ("Unsupported provider '{0}'." -f $Provider)
        }
    }
}

$scriptRoot = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptRoot
$proofScript = Join-Path $scriptRoot 'run_playback_proof.ps1'
$artifactsRoot = Join-Path $repoRoot 'playback-proof-artifacts'
$dotEnv = Get-DotEnvMap -Path (Join-Path $repoRoot '.env')
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$summaryPath = Join-Path $artifactsRoot ("windows-media-server-gate-{0}.json" -f $timestamp)
$shellExecutable = (Get-Process -Id $PID).Path
if ([string]::IsNullOrWhiteSpace($shellExecutable)) { $shellExecutable = 'pwsh' }

$normalizedProviders = [System.Collections.Generic.List[string]]::new()
foreach ($providerEntry in $Providers) {
    foreach ($providerToken in ([string] $providerEntry).Split(',',[System.StringSplitOptions]::RemoveEmptyEntries)) {
        $trimmed = $providerToken.Trim().ToLowerInvariant()
        if (-not [string]::IsNullOrWhiteSpace($trimmed)) {
            $normalizedProviders.Add($trimmed)
        }
    }
}
$Providers = @($normalizedProviders)
if ($Providers.Count -eq 0) { throw 'At least one provider is required.' }

if ([string]::IsNullOrWhiteSpace($MountPath)) {
    $MountPath = Get-DefaultMountPath
}
if (-not (Test-Path -LiteralPath $MountPath)) {
    throw ("Native Windows mount path '{0}' does not exist." -f $MountPath)
}
$filmuvfsProcess = Get-Process -Name filmuvfs -ErrorAction SilentlyContinue | Select-Object -First 1
if ($RequireFilmuvfs -and $null -eq $filmuvfsProcess) {
    throw 'filmuvfs is not running.'
}

$results = [System.Collections.Generic.List[object]]::new()
$sharedReuse = $ReuseExistingItem
$sharedSkipStart = $SkipStart

foreach ($provider in $Providers) {
    if (-not (Test-ProviderConfigured -Provider $provider -DotEnv $dotEnv)) {
        $results.Add([pscustomobject]@{
            provider = $provider
            status = 'skipped'
            exit_code = $null
            topology = $null
            artifact_dir = $null
            details = 'Provider is not configured in env/.env for this host.'
        })
        continue
    }

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

    Write-Host ("[windows-media-gate] Running provider '{0}'..." -f $provider)
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

    $topology = $null
    $details = $null
    if (-not [string]::IsNullOrWhiteSpace([string] $artifactDir)) {
        $artifactSummaryPath = Join-Path $artifactDir 'summary.json'
        if (Test-Path -LiteralPath $artifactSummaryPath) {
            $artifactSummary = Get-Content -LiteralPath $artifactSummaryPath -Raw | ConvertFrom-Json
            if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'topology') {
                $topology = [string] $artifactSummary.media_server.topology
            }
            if ($artifactSummary.media_server.PSObject.Properties.Name -contains 'playback_start_details') {
                $details = [string] $artifactSummary.media_server.playback_start_details
            }
        }
    }

    $status = if (($exitCode -eq 0) -and ($topology -eq 'native_windows')) { 'passed' } else { 'failed' }
    $results.Add([pscustomobject]@{
        provider = $provider
        status = $status
        exit_code = $exitCode
        topology = $topology
        artifact_dir = $artifactDir
        details = $details
    })

    if (($status -eq 'failed') -and $FailFast) { break }

    $sharedReuse = $true
    $sharedSkipStart = $true
}

$summary = [pscustomobject]@{
    timestamp = (Get-Date).ToString('o')
    mount_path = $MountPath
    filmuvfs_running = ($null -ne $filmuvfsProcess)
    filmuvfs_pid = if ($null -ne $filmuvfsProcess) { [int] $filmuvfsProcess.Id } else { $null }
    providers = $Providers
    tmdb_id = $TmdbId
    title = $Title
    media_type = $MediaType
    results = $results
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8

$failures = @($results | Where-Object { $_.status -eq 'failed' })
if ($failures.Count -gt 0) {
    Write-Host ("[windows-media-gate] FAIL. Summary: {0}" -f $summaryPath)
    foreach ($failure in $failures) {
        Write-Host ("[windows-media-gate] {0} failed; topology={1}; artifact={2}" -f $failure.provider, $failure.topology, $failure.artifact_dir)
    }
    exit 1
}

if ($StopWhenDone) {
    $stopScript = Join-Path $scriptRoot 'stop_windows_stack.ps1'
    if (Test-Path -LiteralPath $stopScript) {
        & $shellExecutable -NoProfile -File $stopScript
    }
}

Write-Host ("[windows-media-gate] PASS. Summary: {0}" -f $summaryPath)
