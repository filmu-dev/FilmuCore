param(
    [string] $Repository = 'filmu-dev/FilmuCore',
    [string] $Branch = 'main',
    [switch] $RequirePlaybackGate,
    [switch] $RequireProviderGate,
    [switch] $RequireWindowsVfsGate,
    [switch] $RequireWindowsVfsProvidersGate,
    [int] $MinimumApprovingReviewCount = 1,
    [switch] $RequireAdminsEnforced,
    [switch] $ValidateCurrent,
    [string] $ContractPath = '',
    [string] $OutputPath = '',
    [switch] $AsJson
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'rollout_script_helpers.ps1')

function Get-DefaultContractPath {
    return (Join-Path $repoRoot 'ops\rollout\github-main-policy.contract.json')
}

function Read-ContractFile {
    param([AllowEmptyString()][string] $Path)

    $resolvedPath = $Path
    if ([string]::IsNullOrWhiteSpace($resolvedPath)) {
        $resolvedPath = Get-DefaultContractPath
    }
    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        throw ("GitHub main-policy contract file was not found: {0}" -f $resolvedPath)
    }

    try {
        return Get-Content -LiteralPath $resolvedPath -Raw | ConvertFrom-Json -Depth 8
    }
    catch {
        throw ("GitHub main-policy contract file is not valid JSON: {0}" -f $_.Exception.Message)
    }
}

function Format-UtcTimestamp {
    param([Parameter(Mandatory = $true)][datetime] $Value)

    return $Value.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
}

function Test-CommandAvailable {
    param([Parameter(Mandatory = $true)][string] $Name)
    return ($null -ne (Get-Command $Name -ErrorAction SilentlyContinue))
}

function Invoke-GhApiJson {
    param(
        [Parameter(Mandatory = $true)][string] $Path,
        [Parameter(Mandatory = $true)][hashtable] $DotEnv
    )

    if (Test-GhAuthenticated) {
        $output = & gh api $Path 2>$null
        if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace([string] $output)) {
            try {
                return $output | ConvertFrom-Json
            }
            catch {
                return $null
            }
        }
    }

    $token = Get-EnvValue -Name 'GH_TOKEN' -DotEnv $DotEnv
    if ([string]::IsNullOrWhiteSpace($token)) {
        $token = Get-EnvValue -Name 'GITHUB_TOKEN' -DotEnv $DotEnv
    }
    if ([string]::IsNullOrWhiteSpace($token)) {
        $token = Get-EnvValue -Name 'FILMU_POLICY_ADMIN_TOKEN' -DotEnv $DotEnv
    }
    if ([string]::IsNullOrWhiteSpace($token)) {
        return $null
    }

    $normalizedPath = $Path.TrimStart('/')
    $uri = if ($normalizedPath.StartsWith('http://') -or $normalizedPath.StartsWith('https://')) {
        $normalizedPath
    }
    else {
        "https://api.github.com/$normalizedPath"
    }
    $headers = @{
        Authorization = "Bearer $token"
        Accept = 'application/vnd.github+json'
        'User-Agent' = 'filmu-github-main-policy-check'
        'X-GitHub-Api-Version' = '2022-11-28'
    }

    try {
        return Invoke-RestMethod -Method Get -Uri $uri -Headers $headers
    }
    catch {
        return $null
    }
}

function New-ExpectedPolicy {
    param(
        [string] $Branch,
        [switch] $RequirePlaybackGate,
        [switch] $RequireProviderGate,
        [switch] $RequireWindowsVfsGate,
        [switch] $RequireWindowsVfsProvidersGate,
        [int] $MinimumApprovingReviewCount,
        [switch] $RequireAdminsEnforced
    )

    $requiredChecks = @(
        'Verify - Python Lint / Python Lint'
        'Verify - Python Tests / Python Tests'
        'Verify - Rust Format / Rust Format'
        'Verify - Rust Check / Rust Check'
        'Verify - Rust Tests / Rust Tests'
        'PR Title / Semantic PR Title'
        'Validate Platform Stack / Validate Platform Stack'
    )
    if ($RequirePlaybackGate) {
        $requiredChecks += 'Playback Gate / Playback Gate'
    }

    return [ordered]@{
        repository = [ordered]@{
            allow_squash_merge = $true
            allow_merge_commit = $false
            allow_rebase_merge = $false
        }
        branch_protection = [ordered]@{
            branch = $Branch
            require_pull_request = $true
            require_up_to_date_branches = $true
            minimum_approving_review_count = $MinimumApprovingReviewCount
            require_admins_enforced = [bool] $RequireAdminsEnforced
            required_status_checks = $requiredChecks
            proof_profiles = [ordered]@{
                playback_gate_required = [bool] $RequirePlaybackGate
                provider_gate_expected_inside_playback_gate = [bool] $RequireProviderGate
                windows_vfs_gate_expected_inside_playback_gate = [bool] $RequireWindowsVfsGate
                windows_provider_gate_expected_inside_playback_gate = [bool] $RequireWindowsVfsProvidersGate
            }
        }
        gh_commands = [ordered]@{
            inspect_repository = "gh api repos/$Repository"
            inspect_branch_protection = "gh api repos/$Repository/branches/$([System.Uri]::EscapeDataString($Branch))/protection"
        }
    }
}

function Test-MappingKey {
    param(
        [object] $Mapping,
        [Parameter(Mandatory = $true)][string] $Key
    )

    if ($null -eq $Mapping) {
        return $false
    }

    if ($Mapping -is [System.Collections.IDictionary]) {
        return $Mapping.Contains($Key)
    }

    return $Mapping.PSObject.Properties.Name -contains $Key
}

function Get-MappingValue {
    param(
        [object] $Mapping,
        [Parameter(Mandatory = $true)][string] $Key
    )

    if (-not (Test-MappingKey -Mapping $Mapping -Key $Key)) {
        return $null
    }

    if ($Mapping -is [System.Collections.IDictionary]) {
        return $Mapping[$Key]
    }

    return $Mapping.$Key
}

function New-ValidationActionSet {
    return [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase
    )
}

function Get-ValidationExitCode {
    param([object] $Validation)

    if ($null -eq $Validation) {
        return 0
    }

    if ([string] (Get-MappingValue -Mapping $Validation -Key 'status') -eq 'ready') {
        return 0
    }

    return 1
}

function Write-PolicyArtifactIfRequested {
    param(
        [Parameter(Mandatory = $true)][object] $Payload,
        [AllowEmptyString()][string] $Path = ''
    )

    if ([string]::IsNullOrWhiteSpace($Path)) {
        return
    }

    $directory = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($directory)) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }

    $Payload | ConvertTo-Json -Depth 10 | Set-Content -Path $Path -Encoding UTF8
}

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
$expected = New-ExpectedPolicy `
    -Branch $Branch `
    -RequirePlaybackGate:$RequirePlaybackGate `
    -RequireProviderGate:$RequireProviderGate `
    -RequireWindowsVfsGate:$RequireWindowsVfsGate `
    -RequireWindowsVfsProvidersGate:$RequireWindowsVfsProvidersGate `
    -MinimumApprovingReviewCount $MinimumApprovingReviewCount `
    -RequireAdminsEnforced:$RequireAdminsEnforced
$escapedBranch = [System.Uri]::EscapeDataString($Branch)
$result = [ordered]@{
    schema_version = if ($contract.PSObject.Properties.Name -contains 'schema_version') { [int] $contract.schema_version } else { 1 }
    artifact_kind = if ($contract.PSObject.Properties.Name -contains 'artifact_kind') { [string] $contract.artifact_kind } else { 'github_main_policy_validation' }
    timestamp = Format-UtcTimestamp -Value $capturedAt
    captured_at = Format-UtcTimestamp -Value $capturedAt
    expires_at = Format-UtcTimestamp -Value ($capturedAt.AddHours($freshnessWindowHours))
    freshness_window_hours = $freshnessWindowHours
    contract_path = [string] (Resolve-Path -LiteralPath $resolvedContractPath -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Path -First 1)
    repository = $Repository
    branch = $Branch
    require_playback_gate = [bool] $RequirePlaybackGate
    require_provider_gate = [bool] $RequireProviderGate
    require_windows_vfs_gate = [bool] $RequireWindowsVfsGate
    require_windows_vfs_providers_gate = [bool] $RequireWindowsVfsProvidersGate
    expected = $expected
    required_actions = @()
    failure_reasons = @()
    validation = $null
}

if ($ValidateCurrent) {
    $repoPayload = Invoke-GhApiJson -Path "repos/$Repository" -DotEnv $dotEnv
    $branchProtectionPayload = Invoke-GhApiJson -Path "repos/$Repository/branches/$escapedBranch/protection" -DotEnv $dotEnv

    $canValidate = ($null -ne $repoPayload) -and ($null -ne $branchProtectionPayload)
    if (-not $canValidate) {
        $failureReasons = @('live_policy_validation_unavailable')
        $requiredActions = @('validate_github_main_policy_from_admin_authenticated_host')
        $result.validation = [ordered]@{
            status = 'unverified'
            details = 'Current GitHub repository policy could not be validated from this environment. Install/authenticate gh with repo-admin access, then rerun with -ValidateCurrent.'
            gh_available = (Test-CommandAvailable -Name 'gh')
            gh_authenticated = (Test-GhAuthenticated)
            stale = $false
            failure_reasons = $failureReasons
            required_actions = $requiredActions
        }
        $result.failure_reasons = $failureReasons
        $result.required_actions = $requiredActions
    }
    else {
        $actualChecks = @($branchProtectionPayload.required_status_checks.contexts)
        $missingChecks = @(
            $expected.branch_protection.required_status_checks |
                Where-Object { $_ -notin $actualChecks }
        )
        $unexpectedChecks = @(
            $actualChecks | Where-Object { $_ -notin $expected.branch_protection.required_status_checks }
        )
        $mergePolicy = [ordered]@{
            allow_squash_merge = [bool] $repoPayload.allow_squash_merge
            allow_merge_commit = [bool] $repoPayload.allow_merge_commit
            allow_rebase_merge = [bool] $repoPayload.allow_rebase_merge
        }
        $mergePolicyOk = (
            ($mergePolicy.allow_squash_merge -eq $expected.repository.allow_squash_merge) -and
            ($mergePolicy.allow_merge_commit -eq $expected.repository.allow_merge_commit) -and
            ($mergePolicy.allow_rebase_merge -eq $expected.repository.allow_rebase_merge)
        )
        $pullRequestRequired = $null -ne $branchProtectionPayload.required_pull_request_reviews
        $requiredReviewCount = if ($pullRequestRequired -and $branchProtectionPayload.required_pull_request_reviews.PSObject.Properties.Name -contains 'required_approving_review_count') {
            [int] $branchProtectionPayload.required_pull_request_reviews.required_approving_review_count
        } else {
            0
        }
        $reviewsOk = $requiredReviewCount -ge $expected.branch_protection.minimum_approving_review_count
        $strictChecks = [bool] $branchProtectionPayload.required_status_checks.strict
        $adminsEnforced = [bool] (
            ($branchProtectionPayload.PSObject.Properties.Name -contains 'enforce_admins' -and $null -ne $branchProtectionPayload.enforce_admins) -or
            ($branchProtectionPayload.PSObject.Properties.Name -contains 'enforce_admins_url' -and -not [string]::IsNullOrWhiteSpace([string] $branchProtectionPayload.enforce_admins_url))
        )
        $adminsOk = if ($expected.branch_protection.require_admins_enforced) { $adminsEnforced } else { $true }
        $failureReasonSet = New-ValidationActionSet
        $requiredActionSet = New-ValidationActionSet
        if (-not $mergePolicyOk) {
            $null = $failureReasonSet.Add('merge_policy_mismatch')
            $null = $requiredActionSet.Add('align_repository_merge_policy')
        }
        if (-not $pullRequestRequired) {
            $null = $failureReasonSet.Add('pull_request_reviews_not_required')
            $null = $requiredActionSet.Add('require_pull_request_reviews')
        }
        if (-not $reviewsOk) {
            $null = $failureReasonSet.Add('insufficient_approving_review_count')
            $null = $requiredActionSet.Add('raise_minimum_approving_review_count')
        }
        if (-not $adminsOk) {
            $null = $failureReasonSet.Add('admins_enforcement_missing')
            $null = $requiredActionSet.Add('enforce_admins_for_protected_branch')
        }
        if (-not $strictChecks) {
            $null = $failureReasonSet.Add('up_to_date_branch_checks_disabled')
            $null = $requiredActionSet.Add('require_up_to_date_branch_before_merge')
        }
        if ($missingChecks.Count -gt 0) {
            $null = $failureReasonSet.Add('required_status_checks_missing')
            $null = $requiredActionSet.Add('restore_required_status_checks')
        }
        $status = if ($mergePolicyOk -and $pullRequestRequired -and $reviewsOk -and $adminsOk -and $strictChecks -and $missingChecks.Count -eq 0) {
            'ready'
        } else {
            'not_ready'
        }
        $failureReasons = @($failureReasonSet)
        $requiredActions = @($requiredActionSet)
        $result.validation = [ordered]@{
            status = $status
            stale = $false
            merge_policy = $mergePolicy
            merge_policy_ok = $mergePolicyOk
            pull_request_required = $pullRequestRequired
            required_approving_review_count = $requiredReviewCount
            minimum_approving_review_count_ok = $reviewsOk
            admins_enforced = $adminsEnforced
            admins_enforced_ok = $adminsOk
            up_to_date_branches_required = $strictChecks
            actual_required_checks = $actualChecks
            missing_required_checks = $missingChecks
            unexpected_required_checks = $unexpectedChecks
            proof_profiles = $expected.branch_protection.proof_profiles
            failure_reasons = $failureReasons
            required_actions = $requiredActions
        }
        $result.failure_reasons = $failureReasons
        $result.required_actions = $requiredActions
    }
}

if ($AsJson) {
    Write-PolicyArtifactIfRequested -Payload $result -Path $OutputPath
    $result | ConvertTo-Json -Depth 10
    exit (Get-ValidationExitCode -Validation $result.validation)
}

Write-PolicyArtifactIfRequested -Payload $result -Path $OutputPath

Write-Host ("[github-main-policy] repository: {0}" -f $Repository)
Write-Host ("[github-main-policy] branch: {0}" -f $Branch)
Write-Host ("[github-main-policy] require playback gate: {0}" -f ([bool] $RequirePlaybackGate))
Write-Host ("[github-main-policy] require provider parity inside playback gate: {0}" -f ([bool] $RequireProviderGate))
Write-Host ("[github-main-policy] require windows VFS gate evidence inside playback gate: {0}" -f ([bool] $RequireWindowsVfsGate))
Write-Host ("[github-main-policy] require windows provider parity inside playback gate: {0}" -f ([bool] $RequireWindowsVfsProvidersGate))
Write-Host ("[github-main-policy] minimum approving reviews: {0}" -f $MinimumApprovingReviewCount)
Write-Host ("[github-main-policy] require admins enforced: {0}" -f ([bool] $RequireAdminsEnforced))
Write-Host "[github-main-policy] expected required checks:"
foreach ($check in $expected.branch_protection.required_status_checks) {
    Write-Host ("[github-main-policy] - {0}" -f $check)
}
Write-Host ("[github-main-policy] expected merge policy: squash={0} merge_commit={1} rebase={2}" -f $expected.repository.allow_squash_merge, $expected.repository.allow_merge_commit, $expected.repository.allow_rebase_merge)

if ($null -ne $result.validation) {
    Write-Host ("[github-main-policy] validation status: {0}" -f (Get-MappingValue -Mapping $result.validation -Key 'status'))
    if (Test-MappingKey -Mapping $result.validation -Key 'details') {
        Write-Host ("[github-main-policy] {0}" -f (Get-MappingValue -Mapping $result.validation -Key 'details'))
    }
    elseif (
        (Test-MappingKey -Mapping $result.validation -Key 'missing_required_checks') -and
        (@(Get-MappingValue -Mapping $result.validation -Key 'missing_required_checks')).Count -gt 0
    ) {
        foreach ($check in @(Get-MappingValue -Mapping $result.validation -Key 'missing_required_checks')) {
            Write-Host ("[github-main-policy] missing required check: {0}" -f $check)
        }
    }
}

exit (Get-ValidationExitCode -Validation $result.validation)
