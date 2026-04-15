param(
    [string] $RepoRoot = '',
    [string] $BackendUrl = 'http://localhost:8000',
    [string] $ApiKey = '',
    [string] $PlaybackArtifactsRoot = '',
    [string] $ArtifactDir = '',
    [switch] $SkipProviderGateReadiness,
    [switch] $SkipGithubMainPolicyValidation,
    [switch] $AllowOfflineBackend,
    [switch] $AsJson,
    [string] $OutputPath = ''
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
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#')) {
            continue
        }
        $index = $trimmed.IndexOf('=')
        if ($index -lt 1) {
            continue
        }
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
    if (-not [string]::IsNullOrWhiteSpace($processValue)) {
        return [string] $processValue
    }
    if ($DotEnv.ContainsKey($Name) -and -not [string]::IsNullOrWhiteSpace([string] $DotEnv[$Name])) {
        return [string] $DotEnv[$Name]
    }
    return ''
}

function Get-JsonEnvValue {
    param(
        [Parameter(Mandatory = $true)][string] $Name,
        [Parameter(Mandatory = $true)][hashtable] $DotEnv
    )

    $raw = Get-EnvValue -Name $Name -DotEnv $DotEnv
    if ([string]::IsNullOrWhiteSpace($raw)) {
        return $null
    }

    try {
        return $raw | ConvertFrom-Json -Depth 12
    }
    catch {
        throw ("[enterprise-rollout-evidence] {0} is not valid JSON: {1}" -f $Name, $_.Exception.Message)
    }
}

function ConvertTo-Base64Url {
    param([Parameter(Mandatory = $true)][byte[]] $Bytes)

    $encoded = [Convert]::ToBase64String($Bytes)
    return $encoded.TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

function ConvertFrom-Base64Url {
    param([Parameter(Mandatory = $true)][string] $Value)

    $normalized = $Value.Replace('-', '+').Replace('_', '/')
    $padding = $normalized.Length % 4
    if ($padding -ne 0) {
        $normalized += ('=' * (4 - $padding))
    }
    return [Convert]::FromBase64String($normalized)
}

function New-Hs256Jwt {
    param(
        [Parameter(Mandatory = $true)][string] $Issuer,
        [Parameter(Mandatory = $true)][string] $Audience,
        [Parameter(Mandatory = $true)][byte[]] $SymmetricKeyBytes,
        [Parameter(Mandatory = $true)][string] $Subject
    )

    $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $headerJson = [ordered]@{
        alg = 'HS256'
        kid = 'local-enterprise-rollout'
        typ = 'JWT'
    } | ConvertTo-Json -Compress
    $payloadJson = [ordered]@{
        iss = $Issuer
        sub = $Subject
        aud = $Audience
        exp = $now + 3600
        iat = $now
        tenant_id = 'global'
        actor_type = 'service'
        roles = @('platform:admin')
        authorized_tenants = @('global')
        scope = 'library:write playback:operate settings:write security:policy.approve'
    } | ConvertTo-Json -Compress -Depth 8

    $headerSegment = ConvertTo-Base64Url -Bytes ([Text.Encoding]::UTF8.GetBytes($headerJson))
    $payloadSegment = ConvertTo-Base64Url -Bytes ([Text.Encoding]::UTF8.GetBytes($payloadJson))
    $signingInput = '{0}.{1}' -f $headerSegment, $payloadSegment
    $hmac = [System.Security.Cryptography.HMACSHA256]::new($SymmetricKeyBytes)
    try {
        $signatureBytes = $hmac.ComputeHash([Text.Encoding]::ASCII.GetBytes($signingInput))
    }
    finally {
        $hmac.Dispose()
    }
    $signatureSegment = ConvertTo-Base64Url -Bytes $signatureBytes
    return '{0}.{1}' -f $signingInput, $signatureSegment
}

function Get-BackendHeaders {
    param(
        [Parameter(Mandatory = $true)][hashtable] $DotEnv,
        [Parameter(Mandatory = $true)][string] $ResolvedApiKey
    )

    $oidc = Get-JsonEnvValue -Name 'FILMU_PY_OIDC' -DotEnv $DotEnv
    if ($null -eq $oidc -or -not [bool] $oidc.enabled -or [bool] $oidc.allow_api_key_fallback) {
        if ([string]::IsNullOrWhiteSpace($ResolvedApiKey)) {
            throw "[enterprise-rollout-evidence] FILMU_PY_API_KEY is required to capture backend governance endpoints."
        }
        return @{ 'x-api-key' = $ResolvedApiKey }
    }

    $allowedAlgorithms = @($oidc.allowed_algorithms)
    if ('HS256' -notin $allowedAlgorithms) {
        throw '[enterprise-rollout-evidence] Local OIDC evidence capture requires HS256 in FILMU_PY_OIDC.allowed_algorithms.'
    }
    if ([string]::IsNullOrWhiteSpace([string] $oidc.issuer) -or [string]::IsNullOrWhiteSpace([string] $oidc.audience)) {
        throw '[enterprise-rollout-evidence] FILMU_PY_OIDC issuer and audience must be configured for bearer-token capture.'
    }

    $jwksKeys = @()
    if ($null -ne $oidc.jwks_json -and $null -ne $oidc.jwks_json.keys) {
        $jwksKeys = @($oidc.jwks_json.keys)
    }
    $octKey = $jwksKeys | Where-Object { $_.kty -eq 'oct' -and -not [string]::IsNullOrWhiteSpace([string] $_.k) } | Select-Object -First 1
    if ($null -eq $octKey) {
        throw '[enterprise-rollout-evidence] Local OIDC evidence capture requires one oct key in FILMU_PY_OIDC.jwks_json.keys.'
    }

    $rawKeyBytes = ConvertFrom-Base64Url -Value ([string] $octKey.k)
    $jwt = New-Hs256Jwt `
        -Issuer ([string] $oidc.issuer) `
        -Audience ([string] $oidc.audience) `
        -SymmetricKeyBytes $rawKeyBytes `
        -Subject 'ops://enterprise-rollout-capture'
    return @{ authorization = ('Bearer {0}' -f $jwt) }
}

function Write-JsonFile {
    param(
        [Parameter(Mandatory = $true)][string] $Path,
        [Parameter(Mandatory = $true)][object] $Payload
    )

    $directory = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($directory)) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }
    $Payload | ConvertTo-Json -Depth 12 | Set-Content -Path $Path -Encoding UTF8
}

function Read-JsonFile {
    param([Parameter(Mandatory = $true)][string] $Path)

    return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
}

function Add-Check {
    param(
        [Parameter(Mandatory = $true)][object] $Checks,
        [Parameter(Mandatory = $true)][string] $Name,
        [Parameter(Mandatory = $true)][bool] $Passed,
        [Parameter(Mandatory = $true)][object] $Observed,
        [Parameter(Mandatory = $true)][object] $Expected
    )

    $Checks.Add(
        [pscustomobject]@{
            name     = $Name
            passed   = $Passed
            observed = $Observed
            expected = $Expected
        }
    ) | Out-Null
}

function Parse-EvidenceMap {
    param([object] $Evidence)

    $map = @{}
    if ($Evidence -isnot [System.Array] -and $Evidence -isnot [System.Collections.IEnumerable]) {
        return $map
    }

    foreach ($entry in $Evidence) {
        if ($entry -isnot [string]) {
            continue
        }
        $separatorIndex = $entry.IndexOf('=')
        if ($separatorIndex -lt 1) {
            continue
        }
        $key = $entry.Substring(0, $separatorIndex).Trim()
        $value = $entry.Substring($separatorIndex + 1).Trim()
        if (-not [string]::IsNullOrWhiteSpace($key)) {
            $map[$key] = $value
        }
    }

    return $map
}

function Convert-ToInt {
    param([object] $Value)

    if ($Value -is [int]) {
        return $Value
    }
    if ($Value -is [long]) {
        return [int] $Value
    }
    if ($Value -is [double] -or $Value -is [decimal]) {
        return [int] [Math]::Round([double] $Value)
    }
    if ($Value -is [string] -and -not [string]::IsNullOrWhiteSpace($Value)) {
        try {
            return [int] $Value
        }
        catch {
            return 0
        }
    }
    return 0
}

function Invoke-BackendCapture {
    param(
        [Parameter(Mandatory = $true)][string] $Name,
        [Parameter(Mandatory = $true)][string] $RelativePath,
        [Parameter(Mandatory = $true)][string] $DestinationPath,
        [Parameter(Mandatory = $true)][string] $BaseUrl,
        [Parameter(Mandatory = $true)][hashtable] $Headers
    )

    $uri = "{0}{1}" -f $BaseUrl.TrimEnd('/'), $RelativePath
    $payload = Invoke-RestMethod -Method Get -Uri $uri -Headers $Headers -TimeoutSec 20
    Write-JsonFile -Path $DestinationPath -Payload $payload
    return [pscustomobject]@{
        name = $Name
        uri = $uri
        path = $DestinationPath
        payload = $payload
    }
}

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
$RepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)

$dotEnv = Get-DotEnvMap -Path (Join-Path $RepoRoot '.env')

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    $ApiKey = Get-EnvValue -Name 'FILMU_PY_API_KEY' -DotEnv $dotEnv
}

if ([string]::IsNullOrWhiteSpace($PlaybackArtifactsRoot)) {
    $PlaybackArtifactsRoot = Join-Path $RepoRoot 'playback-proof-artifacts'
}
$PlaybackArtifactsRoot = [System.IO.Path]::GetFullPath($PlaybackArtifactsRoot)
New-Item -ItemType Directory -Force -Path $PlaybackArtifactsRoot | Out-Null

if ([string]::IsNullOrWhiteSpace($ArtifactDir)) {
    $ArtifactDir = Join-Path $RepoRoot 'artifacts\operations\enterprise-rollout'
}
$ArtifactDir = [System.IO.Path]::GetFullPath($ArtifactDir)
New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path $ArtifactDir 'rollout-evidence-summary.json'
}

$runnerArtifactPath = Join-Path $PlaybackArtifactsRoot 'playback-gate-runner-readiness.json'
$githubPolicyArtifactPath = Join-Path $PlaybackArtifactsRoot 'github-main-policy-current.json'
$authPolicyArtifactPath = Join-Path $ArtifactDir 'auth-policy-snapshot.json'
$pluginGovernanceArtifactPath = Join-Path $ArtifactDir 'plugin-governance-snapshot.json'
$operationsGovernanceArtifactPath = Join-Path $ArtifactDir 'operations-governance-snapshot.json'

$checks = [System.Collections.Generic.List[object]]::new()
$captures = [ordered]@{
    playback_gate_runner = $runnerArtifactPath
    github_main_policy = $githubPolicyArtifactPath
    auth_policy = $authPolicyArtifactPath
    plugin_governance = $pluginGovernanceArtifactPath
    operations_governance = $operationsGovernanceArtifactPath
}

$runnerScriptPath = Join-Path $PSScriptRoot 'check_playback_gate_runner.ps1'
$runnerArgs = @(
    '-NoProfile',
    '-File',
    $runnerScriptPath,
    '-NoExitOnFailure',
    '-AsJson',
    '-OutputPath',
    $runnerArtifactPath
)
if (-not $SkipProviderGateReadiness) {
    $runnerArgs += '-RequireProviderGate'
}
& pwsh @runnerArgs | Out-Null
$runnerPayload = Read-JsonFile -Path $runnerArtifactPath
$runnerStatus = [string] $runnerPayload.status
$runnerFailureCount = @(
    $runnerPayload.checks | Where-Object {
        [bool] $_.required -and -not [bool] $_.ok
    }
).Count
Add-Check -Checks $checks -Name 'playback_gate_runner_ready' `
    -Passed:($runnerStatus -eq 'ready') `
    -Observed:$runnerStatus `
    -Expected:'ready'
Add-Check -Checks $checks -Name 'playback_gate_runner_required_failures' `
    -Passed:($runnerFailureCount -eq 0) `
    -Observed:$runnerFailureCount `
    -Expected:0

$githubPolicyPayload = $null
$githubValidationStatus = 'skipped'
if (-not $SkipGithubMainPolicyValidation) {
    $policyScriptPath = Join-Path $PSScriptRoot 'check_github_main_policy.ps1'
    $policyArgs = @(
        '-NoProfile',
        '-File',
        $policyScriptPath,
        '-RequirePlaybackGate',
        '-RequireProviderGate',
        '-RequireWindowsVfsGate',
        '-RequireWindowsVfsProvidersGate',
        '-MinimumApprovingReviewCount',
        '1',
        '-RequireAdminsEnforced',
        '-ValidateCurrent',
        '-OutputPath',
        $githubPolicyArtifactPath,
        '-AsJson'
    )
    & pwsh @policyArgs | Out-Null
    if (Test-Path -LiteralPath $githubPolicyArtifactPath) {
        $githubPolicyPayload = Read-JsonFile -Path $githubPolicyArtifactPath
        if ($null -ne $githubPolicyPayload.validation) {
            $githubValidationStatus = [string] $githubPolicyPayload.validation.status
        }
        else {
            $githubValidationStatus = 'unverified'
        }
    }
    else {
        $githubValidationStatus = 'missing_artifact'
    }
    Add-Check -Checks $checks -Name 'github_main_policy_ready' `
        -Passed:($githubValidationStatus -eq 'ready') `
        -Observed:$githubValidationStatus `
        -Expected:'ready'
}
else {
    Add-Check -Checks $checks -Name 'github_main_policy_ready' `
        -Passed:$true `
        -Observed:'skipped' `
        -Expected:'ready_or_explicitly_skipped'
}

$authPolicyPayload = $null
$pluginGovernancePayload = $null
$operationsGovernancePayload = $null
$backendCaptureError = $null
$backendHeaders = Get-BackendHeaders -DotEnv $dotEnv -ResolvedApiKey $ApiKey

try {
    $authPolicyCapture = Invoke-BackendCapture `
        -Name 'auth_policy' `
        -RelativePath '/api/v1/auth/policy' `
        -DestinationPath $authPolicyArtifactPath `
        -BaseUrl $BackendUrl `
        -Headers $backendHeaders
    $pluginGovernanceCapture = Invoke-BackendCapture `
        -Name 'plugin_governance' `
        -RelativePath '/api/v1/plugins/governance' `
        -DestinationPath $pluginGovernanceArtifactPath `
        -BaseUrl $BackendUrl `
        -Headers $backendHeaders
    $operationsGovernanceCapture = Invoke-BackendCapture `
        -Name 'operations_governance' `
        -RelativePath '/api/v1/operations/governance' `
        -DestinationPath $operationsGovernanceArtifactPath `
        -BaseUrl $BackendUrl `
        -Headers $backendHeaders

    $authPolicyPayload = $authPolicyCapture.payload
    $pluginGovernancePayload = $pluginGovernanceCapture.payload
    $operationsGovernancePayload = $operationsGovernanceCapture.payload
}
catch {
    $backendCaptureError = $_.Exception.Message
    if (-not $AllowOfflineBackend) {
        Add-Check -Checks $checks -Name 'backend_governance_capture' `
            -Passed:$false `
            -Observed:$backendCaptureError `
            -Expected:'reachable backend governance endpoints'
    }
}

if ($null -ne $authPolicyPayload) {
    Add-Check -Checks $checks -Name 'auth_policy_capture' `
        -Passed:$true `
        -Observed:$authPolicyArtifactPath `
        -Expected:'captured'
    Add-Check -Checks $checks -Name 'oidc_rollout_ready' `
        -Passed:([string] $authPolicyPayload.oidc_rollout_status -eq 'ready') `
        -Observed:([string] $authPolicyPayload.oidc_rollout_status) `
        -Expected:'ready'
    Add-Check -Checks $checks -Name 'oidc_rollout_evidence_retained' `
        -Passed:(@($authPolicyPayload.oidc_rollout_evidence_refs).Count -gt 0) `
        -Observed:(@($authPolicyPayload.oidc_rollout_evidence_refs).Count) `
        -Expected:'>0'
}
elseif ($AllowOfflineBackend) {
    Add-Check -Checks $checks -Name 'auth_policy_capture' `
        -Passed:$true `
        -Observed:'offline_allowed' `
        -Expected:'captured_or_offline_allowed'
}

if ($null -ne $pluginGovernancePayload) {
    Add-Check -Checks $checks -Name 'plugin_governance_capture' `
        -Passed:$true `
        -Observed:$pluginGovernanceArtifactPath `
        -Expected:'captured'
    Add-Check -Checks $checks -Name 'plugin_runtime_isolation_ready' `
        -Passed:([bool] $pluginGovernancePayload.summary.runtime_isolation_ready) `
        -Observed:([bool] $pluginGovernancePayload.summary.runtime_isolation_ready) `
        -Expected:$true
}
elseif ($AllowOfflineBackend) {
    Add-Check -Checks $checks -Name 'plugin_governance_capture' `
        -Passed:$true `
        -Observed:'offline_allowed' `
        -Expected:'captured_or_offline_allowed'
}

$operationsEvidenceStatus = 'unavailable'
$operatorLogPipelineStatus = 'unavailable'
$rankNoWinnerTotal = 0
$debridRateLimitedTotal = 0
if ($null -ne $operationsGovernancePayload) {
    $operationsEvidenceStatus = [string] $operationsGovernancePayload.operational_evidence.status
    $operatorLogPipelineStatus = [string] $operationsGovernancePayload.operator_log_pipeline.status
    $operationalEvidenceMap = Parse-EvidenceMap -Evidence $operationsGovernancePayload.operational_evidence.evidence
    $rankNoWinnerTotal = Convert-ToInt -Value $operationalEvidenceMap['rank_streams_no_winner_total']
    $debridRateLimitedTotal = Convert-ToInt -Value $operationalEvidenceMap['debrid_rate_limited_total']

    Add-Check -Checks $checks -Name 'operations_governance_capture' `
        -Passed:$true `
        -Observed:$operationsGovernanceArtifactPath `
        -Expected:'captured'
    Add-Check -Checks $checks -Name 'operator_log_pipeline_ready' `
        -Passed:($operatorLogPipelineStatus -eq 'ready') `
        -Observed:$operatorLogPipelineStatus `
        -Expected:'ready'
    Add-Check -Checks $checks -Name 'operational_evidence_ready' `
        -Passed:($operationsEvidenceStatus -eq 'ready') `
        -Observed:$operationsEvidenceStatus `
        -Expected:'ready'
    Add-Check -Checks $checks -Name 'rank_streams_no_winner_clear' `
        -Passed:($rankNoWinnerTotal -eq 0) `
        -Observed:$rankNoWinnerTotal `
        -Expected:0
    Add-Check -Checks $checks -Name 'debrid_rate_limited_clear' `
        -Passed:($debridRateLimitedTotal -eq 0) `
        -Observed:$debridRateLimitedTotal `
        -Expected:0
}
elseif ($AllowOfflineBackend) {
    Add-Check -Checks $checks -Name 'operations_governance_capture' `
        -Passed:$true `
        -Observed:'offline_allowed' `
        -Expected:'captured_or_offline_allowed'
}

$failedChecks = @($checks | Where-Object { -not $_.passed })
$summary = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString('o')
    repo_root = $RepoRoot
    backend_url = $BackendUrl
    playback_artifacts_root = $PlaybackArtifactsRoot
    artifact_dir = $ArtifactDir
    captures = $captures
    observed = [ordered]@{
        playback_gate_runner_status = $runnerStatus
        playback_gate_runner_required_failures = $runnerFailureCount
        github_main_policy_validation_status = $githubValidationStatus
        oidc_rollout_status = if ($null -ne $authPolicyPayload) { [string] $authPolicyPayload.oidc_rollout_status } else { 'unavailable' }
        oidc_rollout_evidence_count = if ($null -ne $authPolicyPayload) { @($authPolicyPayload.oidc_rollout_evidence_refs).Count } else { 0 }
        plugin_runtime_isolation_ready = if ($null -ne $pluginGovernancePayload) { [bool] $pluginGovernancePayload.summary.runtime_isolation_ready } else { $false }
        operator_log_pipeline_status = $operatorLogPipelineStatus
        operational_evidence_status = $operationsEvidenceStatus
        rank_streams_no_winner_total = $rankNoWinnerTotal
        debrid_rate_limited_total = $debridRateLimitedTotal
        backend_capture_error = $backendCaptureError
    }
    checks = $checks
    status = if ($failedChecks.Count -eq 0) { 'passed' } else { 'failed' }
}

Write-JsonFile -Path $OutputPath -Payload $summary

if ($AsJson) {
    $summary | ConvertTo-Json -Depth 12
}
else {
    Write-Host ("[enterprise-rollout-evidence] status: {0}" -f $summary.status)
    Write-Host ("[enterprise-rollout-evidence] summary: {0}" -f $OutputPath)
    foreach ($check in $checks) {
        $label = if ($check.passed) { 'PASS' } else { 'FAIL' }
        Write-Host ("[enterprise-rollout-evidence] {0} {1} observed={2} expected={3}" -f $label, $check.name, $check.observed, $check.expected)
    }
}

if ($failedChecks.Count -gt 0) {
    exit 1
}
