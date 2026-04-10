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
    [switch] $AsJson
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Test-CommandAvailable {
    param([Parameter(Mandatory = $true)][string] $Name)
    return ($null -ne (Get-Command $Name -ErrorAction SilentlyContinue))
}

function Invoke-GhApiJson {
    param([Parameter(Mandatory = $true)][string] $Path)

    if (-not (Test-CommandAvailable -Name 'gh')) {
        return $null
    }

    $output = & gh api $Path 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace([string] $output)) {
        return $null
    }

    try {
        return $output | ConvertFrom-Json
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
    param([switch] $RequirePlaybackGate)

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
            required_status_checks = $requiredChecks
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

$expected = New-ExpectedPolicy `
    -Branch $Branch `
    -RequirePlaybackGate:$RequirePlaybackGate `
    -RequireProviderGate:$RequireProviderGate `
    -RequireWindowsVfsGate:$RequireWindowsVfsGate `
    -RequireWindowsVfsProvidersGate:$RequireWindowsVfsProvidersGate `
    -MinimumApprovingReviewCount $MinimumApprovingReviewCount `
    -RequireAdminsEnforced:$RequireAdminsEnforced
$expected = New-ExpectedPolicy -RequirePlaybackGate:$RequirePlaybackGate
$escapedBranch = [System.Uri]::EscapeDataString($Branch)
$result = [ordered]@{
    timestamp = (Get-Date).ToString('o')
    repository = $Repository
    branch = $Branch
    require_playback_gate = [bool] $RequirePlaybackGate
    require_provider_gate = [bool] $RequireProviderGate
    require_windows_vfs_gate = [bool] $RequireWindowsVfsGate
    require_windows_vfs_providers_gate = [bool] $RequireWindowsVfsProvidersGate
    expected = $expected
    validation = $null
}

if ($ValidateCurrent) {
    $repoPayload = Invoke-GhApiJson -Path "repos/$Repository"
    $branchProtectionPayload = Invoke-GhApiJson -Path "repos/$Repository/branches/$escapedBranch/protection"

    $canValidate = ($null -ne $repoPayload) -and ($null -ne $branchProtectionPayload)
    if (-not $canValidate) {
        $result.validation = [ordered]@{
            status = 'unverified'
            details = 'Current GitHub repository policy could not be validated from this environment. Install/authenticate gh with repo-admin access, then rerun with -ValidateCurrent.'
            gh_available = (Test-CommandAvailable -Name 'gh')
        }
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
        $status = if ($mergePolicyOk -and $pullRequestRequired -and $reviewsOk -and $adminsOk -and $strictChecks -and $missingChecks.Count -eq 0) {
        $strictChecks = [bool] $branchProtectionPayload.required_status_checks.strict
        $status = if ($mergePolicyOk -and $pullRequestRequired -and $strictChecks -and $missingChecks.Count -eq 0) {
            'ready'
        } else {
            'not_ready'
        }
        $result.validation = [ordered]@{
            status = $status
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
        }
    }
}

if ($AsJson) {
    $result | ConvertTo-Json -Depth 10
    exit (Get-ValidationExitCode -Validation $result.validation)
}

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
