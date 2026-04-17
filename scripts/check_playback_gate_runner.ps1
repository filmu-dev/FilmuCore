param(
    [switch] $RequireProviderGate,
    [switch] $AsJson,
    [switch] $NoExitOnFailure,
    [string] $ContractPath = '',
    [string] $OutputPath = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot

function Get-DefaultContractPath {
    return (Join-Path $repoRoot 'ops\rollout\playback-gate-runner-readiness.contract.json')
}

function Read-ContractFile {
    param([AllowEmptyString()][string] $Path)

    $resolvedPath = $Path
    if ([string]::IsNullOrWhiteSpace($resolvedPath)) {
        $resolvedPath = Get-DefaultContractPath
    }
    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        throw ("Playback-gate runner contract file was not found: {0}" -f $resolvedPath)
    }

    try {
        return Get-Content -LiteralPath $resolvedPath -Raw | ConvertFrom-Json -Depth 8
    }
    catch {
        throw ("Playback-gate runner contract file is not valid JSON: {0}" -f $_.Exception.Message)
    }
}

function Format-UtcTimestamp {
    param([Parameter(Mandatory = $true)][datetime] $Value)

    return $Value.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
}

function Convert-ToReasonToken {
    param([Parameter(Mandatory = $true)][string] $Value)

    return (($Value.ToLowerInvariant() -replace '[^a-z0-9]+', '_').Trim('_'))
}

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

function Test-LinuxFuseAvailable {
    param([Parameter(Mandatory = $true)][bool] $IsLinuxHost)

    if (-not $IsLinuxHost) {
        return $false
    }

    if (Test-Path -LiteralPath '/dev/fuse') {
        return $true
    }

    foreach ($shellName in @('bash', 'sh')) {
        if (-not (Test-CommandAvailable -Name $shellName)) {
            continue
        }

        & $shellName -lc 'test -e /dev/fuse'
        if ($LASTEXITCODE -eq 0) {
            return $true
        }
    }

    return $false
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
$resolvedContractPath = $ContractPath
if ([string]::IsNullOrWhiteSpace($resolvedContractPath)) {
    $resolvedContractPath = Get-DefaultContractPath
}
$contract = Read-ContractFile -Path $resolvedContractPath
$capturedAt = (Get-Date).ToUniversalTime()
$freshnessWindowHours = 24
if ($contract.PSObject.Properties.Name -contains 'freshness_window_hours') {
    $freshnessWindowHours = [Math]::Max(1, [int] $contract.freshness_window_hours)
}
$dotEnv = Get-DotEnvMap -Path (Join-Path $repoRoot '.env')
$isLinuxHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Linux)

$runnerEnvironment = [string] [System.Environment]::GetEnvironmentVariable('RUNNER_ENVIRONMENT')
$runnerName = [string] [System.Environment]::GetEnvironmentVariable('RUNNER_NAME')
$runnerOs = [string] [System.Environment]::GetEnvironmentVariable('RUNNER_OS')
$githubActions = [string] [System.Environment]::GetEnvironmentVariable('GITHUB_ACTIONS')
$frontendContext = Get-EnvValue -Name 'FILMU_FRONTEND_CONTEXT' -DotEnv $dotEnv
$browserPath = Resolve-BrowserExecutablePath -ConfiguredPath (Get-EnvValue -Name 'FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE' -DotEnv $dotEnv)
$tmdbKey = Get-EnvValue -Name 'TMDB_API_KEY' -DotEnv $dotEnv
$plexToken = Get-EnvValue -Name 'PLEX_TOKEN' -DotEnv $dotEnv
$embyApiKey = Get-EnvValue -Name 'EMBY_API_KEY' -DotEnv $dotEnv
$policyAdminToken = Get-EnvValue -Name 'FILMU_POLICY_ADMIN_TOKEN' -DotEnv $dotEnv
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
$checks.Add([pscustomobject]@{ name = 'github_hosted_runner'; required = $true; ok = ([string]::Equals($githubActions, 'true', [System.StringComparison]::OrdinalIgnoreCase) -and [string]::Equals($runnerEnvironment, 'github-hosted', [System.StringComparison]::OrdinalIgnoreCase)); details = 'The readiness artifact must be captured from a GitHub-hosted runner.' })
$checks.Add([pscustomobject]@{ name = 'frontend_context'; required = $true; ok = (-not [string]::IsNullOrWhiteSpace($frontendContext) -and (Test-Path -LiteralPath $frontendContext)); details = 'FILMU_FRONTEND_CONTEXT must point to a readable frontend checkout.' })
$checks.Add([pscustomobject]@{ name = 'browser_executable'; required = $true; ok = (-not [string]::IsNullOrWhiteSpace($browserPath) -and (Test-Path -LiteralPath $browserPath)); details = 'FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE must point to an executable browser.' })
$checks.Add([pscustomobject]@{ name = 'tmdb_api_key'; required = $true; ok = (-not [string]::IsNullOrWhiteSpace($tmdbKey)); details = 'TMDB_API_KEY is required.' })
$checks.Add([pscustomobject]@{ name = 'debrid_provider_token'; required = $true; ok = ($debridKeys.Count -gt 0); details = 'At least one debrid provider token/key is required.' })
$checks.Add([pscustomobject]@{ name = 'linux_fuse'; required = $true; ok = (Test-LinuxFuseAvailable -IsLinuxHost $isLinuxHost); details = 'The playback gate requires a Linux runner with /dev/fuse.' })
$checks.Add([pscustomobject]@{ name = 'policy_admin_token'; required = $true; ok = (-not [string]::IsNullOrWhiteSpace($policyAdminToken)); details = 'FILMU_POLICY_ADMIN_TOKEN is required for live protected-branch validation.' })
$checks.Add([pscustomobject]@{ name = 'plex_token'; required = $RequireProviderGate.IsPresent; ok = (-not [string]::IsNullOrWhiteSpace($plexToken)); details = 'PLEX_TOKEN enables the provider parity gate.' })
$checks.Add([pscustomobject]@{ name = 'emby_api_key'; required = $RequireProviderGate.IsPresent; ok = (-not [string]::IsNullOrWhiteSpace($embyApiKey)); details = 'EMBY_API_KEY enables the provider parity gate.' })

$requiredFailures = @($checks | Where-Object { $_.required -and -not $_.ok })
$optionalWarnings = @($checks | Where-Object { -not $_.required -and -not $_.ok })
$status = if ($requiredFailures.Count -eq 0) { 'ready' } else { 'not_ready' }
$failureReasons = @(
    $requiredFailures | ForEach-Object {
        "{0}_missing_or_unready" -f (Convert-ToReasonToken -Value ([string] $_.name))
    }
)
$requiredActions = @(
    $requiredFailures | ForEach-Object {
        "repair_{0}" -f (Convert-ToReasonToken -Value ([string] $_.name))
    }
)

$result = [pscustomobject]@{
    schema_version = if ($contract.PSObject.Properties.Name -contains 'schema_version') { [int] $contract.schema_version } else { 1 }
    artifact_kind = if ($contract.PSObject.Properties.Name -contains 'artifact_kind') { [string] $contract.artifact_kind } else { 'playback_gate_runner_readiness' }
    timestamp = Format-UtcTimestamp -Value $capturedAt
    captured_at = Format-UtcTimestamp -Value $capturedAt
    expires_at = Format-UtcTimestamp -Value ($capturedAt.AddHours($freshnessWindowHours))
    freshness_window_hours = $freshnessWindowHours
    contract_path = [string] (Resolve-Path -LiteralPath $resolvedContractPath -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Path -First 1)
    repo_root = $repoRoot
    require_provider_gate = $RequireProviderGate.IsPresent
    runner_environment = if (-not [string]::IsNullOrWhiteSpace($runnerEnvironment)) { $runnerEnvironment } else { 'unknown' }
    runner_name = if (-not [string]::IsNullOrWhiteSpace($runnerName)) { $runnerName } else { 'unknown' }
    runner_os = if (-not [string]::IsNullOrWhiteSpace($runnerOs)) { $runnerOs } else { [System.Environment]::OSVersion.Platform.ToString() }
    browser_executable_path = $browserPath
    status = $status
    required_failure_count = $requiredFailures.Count
    optional_warning_count = $optionalWarnings.Count
    required_actions = $requiredActions
    failure_reasons = $failureReasons
    checks = $checks
}

if (-not [string]::IsNullOrWhiteSpace($OutputPath)) {
    $directory = Split-Path -Parent $OutputPath
    if (-not [string]::IsNullOrWhiteSpace($directory)) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }
    $serialized = $result | ConvertTo-Json -Depth 6
    $tempOutputPath = "{0}.{1}.{2}.tmp" -f $OutputPath, $PID, ([guid]::NewGuid().ToString('N'))
    try {
        $serialized | Set-Content -Path $tempOutputPath -Encoding UTF8
        $writeSucceeded = $false
        for ($attempt = 0; $attempt -lt 5; $attempt++) {
            try {
                Move-Item -LiteralPath $tempOutputPath -Destination $OutputPath -Force
                $writeSucceeded = $true
                break
            }
            catch {
                if ($attempt -eq 4) {
                    throw
                }
                Start-Sleep -Milliseconds (100 * ($attempt + 1))
            }
        }
        if (-not $writeSucceeded) {
            throw ("Failed to persist playback gate runner readiness artifact: {0}" -f $OutputPath)
        }
    }
    finally {
        if (Test-Path -LiteralPath $tempOutputPath) {
            Remove-Item -LiteralPath $tempOutputPath -Force -ErrorAction SilentlyContinue
        }
    }
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
    if ($NoExitOnFailure) {
        return
    }
    if ([string]::Equals([string] $env:GITHUB_ACTIONS, 'true', [System.StringComparison]::OrdinalIgnoreCase)) {
        foreach ($failure in $requiredFailures) {
            Write-Host ("::error title=playback gate readiness::{0} - {1}" -f $failure.name, $failure.details)
        }
    }
    exit 1
}


