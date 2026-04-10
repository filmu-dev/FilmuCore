param(
    [switch] $RequireProviderGate,
    [switch] $AsJson
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
        $index = $trimmed.IndexOf('=')
        if ($index -lt 1) { continue }
        $map[$trimmed.Substring(0, $index).Trim()] = $trimmed.Substring($index + 1)
    }
    return $map
}

function Get-EnvValue {
    param(
        [Parameter(Mandatory = $true)][string] $Name,
        [Parameter(Mandatory = $true)][hashtable] $DotEnv
    )
    $processValue = [System.Environment]::GetEnvironmentVariable($Name)
    if (-not [string]::IsNullOrWhiteSpace($processValue)) { return [string] $processValue }
    if ($DotEnv.ContainsKey($Name) -and -not [string]::IsNullOrWhiteSpace([string] $DotEnv[$Name])) { return [string] $DotEnv[$Name] }
    return ''
}

function Test-CommandAvailable {
    param([Parameter(Mandatory = $true)][string] $Name)
    return ($null -ne (Get-Command $Name -ErrorAction SilentlyContinue))
}

function Resolve-BrowserExecutablePath {
    param(
        [AllowEmptyString()][string] $ConfiguredPath
    )

    if (-not [string]::IsNullOrWhiteSpace($ConfiguredPath) -and (Test-Path -LiteralPath $ConfiguredPath)) {
        return $ConfiguredPath
    }

    $candidateCommands = @(
        'google-chrome'
        'google-chrome-stable'
        'chromium'
        'chromium-browser'
        'microsoft-edge'
        'msedge'
    )

    foreach ($candidate in $candidateCommands) {
        $command = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($null -ne $command -and -not [string]::IsNullOrWhiteSpace($command.Source)) {
            return [string] $command.Source
        }
    }

    return ''
}

$scriptRoot = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptRoot
$dotEnv = Get-DotEnvMap -Path (Join-Path $repoRoot '.env')
$isLinuxHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Linux)

$frontendContext = Get-EnvValue -Name 'FILMU_FRONTEND_CONTEXT' -DotEnv $dotEnv
$browserPath = Resolve-BrowserExecutablePath -ConfiguredPath (Get-EnvValue -Name 'FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE' -DotEnv $dotEnv)
$tmdbKey = Get-EnvValue -Name 'TMDB_API_KEY' -DotEnv $dotEnv
$plexToken = Get-EnvValue -Name 'PLEX_TOKEN' -DotEnv $dotEnv
$embyApiKey = Get-EnvValue -Name 'EMBY_API_KEY' -DotEnv $dotEnv
$debridKeys = @(
    (Get-EnvValue -Name 'FILMU_PY_REALDEBRID_API_TOKEN' -DotEnv $dotEnv)
    (Get-EnvValue -Name 'REAL_DEBRID_API_KEY' -DotEnv $dotEnv)
    (Get-EnvValue -Name 'FILMU_PY_ALLDEBRID_API_TOKEN' -DotEnv $dotEnv)
    (Get-EnvValue -Name 'ALL_DEBRID_API_KEY' -DotEnv $dotEnv)
    (Get-EnvValue -Name 'FILMU_PY_DEBRIDLINK_API_TOKEN' -DotEnv $dotEnv)
    (Get-EnvValue -Name 'DEBRID_LINK_API_KEY' -DotEnv $dotEnv)
) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
$debridKeys = @($debridKeys)

$checks = [System.Collections.Generic.List[object]]::new()

$checks.Add([pscustomobject]@{ name = 'docker'; required = $true; ok = (Test-CommandAvailable -Name 'docker'); details = 'docker must be installed on the runner.' })
$checks.Add([pscustomobject]@{ name = 'curl'; required = $true; ok = (Test-CommandAvailable -Name 'curl'); details = 'curl must be installed on the runner.' })
$checks.Add([pscustomobject]@{ name = 'pwsh'; required = $true; ok = (Test-CommandAvailable -Name 'pwsh'); details = 'PowerShell 7+ must be installed on the runner.' })
$checks.Add([pscustomobject]@{ name = 'frontend_context'; required = $true; ok = (-not [string]::IsNullOrWhiteSpace($frontendContext) -and (Test-Path -LiteralPath $frontendContext)); details = 'FILMU_FRONTEND_CONTEXT must point to a readable frontend checkout.' })
$checks.Add([pscustomobject]@{ name = 'browser_executable'; required = $true; ok = (-not [string]::IsNullOrWhiteSpace($browserPath) -and (Test-Path -LiteralPath $browserPath)); details = 'FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE must point to an executable browser.' })
$checks.Add([pscustomobject]@{ name = 'tmdb_api_key'; required = $true; ok = (-not [string]::IsNullOrWhiteSpace($tmdbKey)); details = 'TMDB_API_KEY is required.' })
$checks.Add([pscustomobject]@{ name = 'debrid_provider_token'; required = $true; ok = ($debridKeys.Count -gt 0); details = 'At least one debrid provider token/key is required.' })
$checks.Add([pscustomobject]@{ name = 'linux_fuse'; required = $true; ok = ($isLinuxHost -and (Test-Path -LiteralPath '/dev/fuse')); details = 'The playback gate requires a Linux runner with /dev/fuse.' })
$checks.Add([pscustomobject]@{ name = 'plex_token'; required = $RequireProviderGate.IsPresent; ok = (-not [string]::IsNullOrWhiteSpace($plexToken)); details = 'PLEX_TOKEN enables the provider parity gate.' })
$checks.Add([pscustomobject]@{ name = 'emby_api_key'; required = $RequireProviderGate.IsPresent; ok = (-not [string]::IsNullOrWhiteSpace($embyApiKey)); details = 'EMBY_API_KEY enables the provider parity gate.' })

$requiredFailures = @($checks | Where-Object { $_.required -and -not $_.ok })
$optionalWarnings = @($checks | Where-Object { -not $_.required -and -not $_.ok })
$status = if ($requiredFailures.Count -eq 0) { 'ready' } else { 'not_ready' }

$result = [pscustomobject]@{
    timestamp = (Get-Date).ToString('o')
    repo_root = $repoRoot
    require_provider_gate = $RequireProviderGate.IsPresent
    browser_executable_path = $browserPath
    status = $status
    checks = $checks
}

if ($AsJson) {
    $result | ConvertTo-Json -Depth 6
}
else {
    Write-Host ("[playback-gate-runner] status: {0}" -f $status)
    foreach ($check in $checks) {
        $label = if ($check.ok) { 'OK' } else { if ($check.required) { 'FAIL' } else { 'WARN' } }
        Write-Host ("[playback-gate-runner] {0} {1} - {2}" -f $label, $check.name, $check.details)
    }
}

if ($requiredFailures.Count -gt 0) {
    exit 1
}


