param(
    [string[]] $Providers = @('plex', 'emby'),
    [int] $RepeatCount = 1,
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
            provider     = $provider
            run          = $runIndex
            exit_code    = $exitCode
            status       = if ($exitCode -eq 0) { 'passed' } else { 'failed' }
            artifact_dir = $artifactDir
        }
        $results.Add($result)

        if ($exitCode -ne 0 -and $FailFast) {
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
    providers  = $Providers
    repeat_count = $RepeatCount
    tmdb_id    = $TmdbId
    title      = $Title
    media_type = $MediaType
    all_green  = (@($results | Where-Object { $_.exit_code -ne 0 })).Count -eq 0
    results    = $results
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8

$failed = @($results | Where-Object { $_.exit_code -ne 0 })
if ($failed.Count -gt 0) {
    Write-Host ("[media-server-gate] FAIL. Summary: {0}" -f $summaryPath)
    foreach ($failure in $failed) {
        Write-Host ("[media-server-gate] {0} failed; artifact={1}" -f $failure.provider, $failure.artifact_dir)
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


