param(
    [string] $Branch = '',
    [string] $Commitish = '',
    [string] $ReviewBranch = '',
    [string] $Remote = 'origin',
    [string] $BaseBranch = 'main',
    [string] $Repository = '',
    [switch] $NoFetch,
    [switch] $AsJson,
    [bool] $LocalSourceOfTruth = $true
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$allowedSemanticReviewBranchPrefixes = @(
    'fix/',
    'feat/',
    'chore/',
    'refactor/',
    'docs/',
    'test/',
    'perf/',
    'build/',
    'ci/',
    'revert/'
)
$permanentlyBlockedReviewBranches = @{
    'codex/windows-vfs-rollout-20260415' = 'Previously-used review branch retained only for history. Push local main to a fresh remote review branch instead.'
}

function Invoke-GitCapture {
    param([Parameter(Mandatory = $true)][string[]] $Arguments)

    $output = & git @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        $joined = $Arguments -join ' '
        throw "git $joined failed`n$output"
    }

    return [string]::Join("`n", @($output))
}

function Resolve-GitHubRepository {
    param([Parameter(Mandatory = $true)][string] $RemoteName)

    $remoteUrl = (Invoke-GitCapture -Arguments @('remote', 'get-url', $RemoteName)).Trim()
    if ($remoteUrl -match 'github\.com[:/](?<owner>[^/]+)/(?<repo>[^/.]+?)(?:\.git)?$') {
        return "$($Matches.owner)/$($Matches.repo)"
    }

    return ''
}

function Get-SuggestedPrTitle {
    param([Parameter(Mandatory = $true)][string] $ReviewBranchName)

    $prefix = $allowedSemanticReviewBranchPrefixes | Where-Object { $ReviewBranchName.StartsWith($_) } | Select-Object -First 1
    if ($null -eq $prefix) {
        return ''
    }

    $kind = $prefix.TrimEnd('/')
    $subject = $ReviewBranchName.Substring($prefix.Length).Trim()
    if ([string]::IsNullOrWhiteSpace($subject)) {
        return ''
    }

    $subject = ($subject -replace '[-_]+', ' ').Trim()
    if ([string]::IsNullOrWhiteSpace($subject)) {
        return ''
    }

    return "$kind`: $subject"
}

$resolvedBranch = $Branch
if ([string]::IsNullOrWhiteSpace($resolvedBranch)) {
    $resolvedBranch = (Invoke-GitCapture -Arguments @('branch', '--show-current')).Trim()
}
if ([string]::IsNullOrWhiteSpace($Commitish)) {
    if ([string]::IsNullOrWhiteSpace($resolvedBranch)) {
        $Commitish = 'HEAD'
    }
    else {
        $Commitish = $resolvedBranch
    }
}
Invoke-GitCapture -Arguments @('rev-parse', '--verify', $Commitish) | Out-Null

if ([string]::IsNullOrWhiteSpace($ReviewBranch)) {
    if ([string]::IsNullOrWhiteSpace($resolvedBranch)) {
        $ReviewBranch = $Commitish
    }
    else {
        $ReviewBranch = $resolvedBranch
    }
}

if (-not $NoFetch) {
    Invoke-GitCapture -Arguments @('fetch', $Remote, $BaseBranch, '--prune') | Out-Null
}

$baseRef = "refs/remotes/$Remote/$BaseBranch"
Invoke-GitCapture -Arguments @('rev-parse', '--verify', $baseRef) | Out-Null

$counts = (Invoke-GitCapture -Arguments @('rev-list', '--left-right', '--count', "$baseRef...$Commitish")).Trim() -split '\s+'
if ($counts.Count -lt 2) {
    throw "Could not parse ahead/behind counts from git rev-list output: $($counts -join ' ')"
}

$behindBy = [int] $counts[0]
$aheadBy = [int] $counts[1]
$mergeCommitCount = [int] (Invoke-GitCapture -Arguments @('rev-list', '--count', '--min-parents=2', "$baseRef..$Commitish")).Trim()
$mergeCommitSamples = @(
    (Invoke-GitCapture -Arguments @('log', '--oneline', '--no-decorate', '--max-count', '5', '--min-parents=2', "$baseRef..$Commitish")) -split "`r?`n" |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
)
$sourceLabel = if ([string]::IsNullOrWhiteSpace($resolvedBranch)) { $Commitish } else { $resolvedBranch }
$repositoryName = if ([string]::IsNullOrWhiteSpace($Repository)) {
    Resolve-GitHubRepository -RemoteName $Remote
} else {
    $Repository
}
$owner = if ([string]::IsNullOrWhiteSpace($repositoryName)) { '' } else { $repositoryName.Split('/')[0] }
$releasePleaseBranch = $ReviewBranch -like 'release-please--*'
$closedReuse = $null
$openPr = $null
$reuseCheckStatus = if ($releasePleaseBranch) { 'skipped_release_please' } elseif ([string]::IsNullOrWhiteSpace($repositoryName)) { 'repository_unknown' } else { 'checked' }

if ($reuseCheckStatus -eq 'checked') {
    $headFilter = [System.Uri]::EscapeDataString("${owner}:$ReviewBranch")
    $uri = "https://api.github.com/repos/$repositoryName/pulls?state=all&head=$headFilter&per_page=100"

    try {
        $response = Invoke-RestMethod -Headers @{
            'User-Agent' = 'Codex'
            'Accept' = 'application/vnd.github+json'
        } -Uri $uri

        $open = @($response | Where-Object { $_.state -eq 'open' } | Sort-Object number -Descending)
        if ($open.Count -gt 0) {
            $latestOpen = $open[0]
            $openPr = [ordered]@{
                number = [int] $latestOpen.number
                state = [string] $latestOpen.state
                merged = [bool] ($null -ne $latestOpen.merged_at)
            }
        }

        $closed = @($response | Where-Object { $_.state -eq 'closed' } | Sort-Object updated_at -Descending)
        if ($closed.Count -gt 0) {
            $latest = $closed[0]
            $closedReuse = [ordered]@{
                number = [int] $latest.number
                state = [string] $latest.state
                merged = [bool] ($null -ne $latest.merged_at)
                closed_at = [string] $latest.closed_at
            }
        }
    }
    catch {
        $reuseCheckStatus = 'github_lookup_unavailable'
    }
}

$actions = New-Object System.Collections.Generic.List[string]
$advisories = New-Object System.Collections.Generic.List[string]
$suggestedPrTitle = Get-SuggestedPrTitle -ReviewBranchName $ReviewBranch
if ($permanentlyBlockedReviewBranches.ContainsKey($ReviewBranch)) {
    $actions.Add("Review branch '$ReviewBranch' is permanently blocked for this repository. $($permanentlyBlockedReviewBranches[$ReviewBranch])")
}
if (-not $releasePleaseBranch -and -not ($allowedSemanticReviewBranchPrefixes | Where-Object { $ReviewBranch.StartsWith($_) })) {
    $allowedPrefixesLabel = $allowedSemanticReviewBranchPrefixes -join ', '
    $actions.Add("Review branch '$ReviewBranch' must start with a semantic prefix. Use one of: $allowedPrefixesLabel")
}
if ($behindBy -gt 0) {
    if ($LocalSourceOfTruth) {
        $advisories.Add("Branch '$sourceLabel' differs from '$Remote/$BaseBranch' by $behindBy commit(s). Local '$sourceLabel' remains authoritative; review mergeability in GitHub without rebasing from remote main.")
    }
    else {
        $actions.Add("Branch '$sourceLabel' is behind '$Remote/$BaseBranch' by $behindBy commit(s). Rebase or recreate it from current main before opening or merging a PR.")
    }
}
if ($aheadBy -eq 0) {
    $actions.Add("Branch '$sourceLabel' has no commits beyond '$Remote/$BaseBranch'. Push the intended change from a real feature branch instead.")
}
if ($mergeCommitCount -gt 0) {
    $sampleLabel = ($mergeCommitSamples | Select-Object -First 3) -join '; '
    $actions.Add("Review branch '$ReviewBranch' must keep a linear history relative to '$Remote/$BaseBranch'. Found $mergeCommitCount merge commit(s) beyond the base. Recreate the review branch from the current local source branch or replay only the intended commits without merging '$Remote/$BaseBranch' into it. Examples: $sampleLabel")
}
if ($null -ne $closedReuse) {
    $stateLabel = if ($closedReuse.merged) { 'merged' } else { 'closed' }
    $actions.Add("Review branch '$ReviewBranch' was already used by $stateLabel PR #$($closedReuse.number). Create a fresh single-use remote review branch from the current local source branch instead of reusing it.")
}
if (-not [string]::IsNullOrWhiteSpace($suggestedPrTitle)) {
    $advisories.Add("Suggested PR title: '$suggestedPrTitle'")
}
if ([string]::IsNullOrWhiteSpace($resolvedBranch)) {
    $advisories.Add("Detached HEAD source detected; evaluated commit '$Commitish' for review branch '$ReviewBranch'.")
}

$status = if ($actions.Count -eq 0) { 'ready' } else { 'not_ready' }
$result = [ordered]@{
    branch = if ([string]::IsNullOrWhiteSpace($resolvedBranch)) { $null } else { $resolvedBranch }
    source = $sourceLabel
    commitish = $Commitish
    review_branch = $ReviewBranch
    base_branch = "$Remote/$BaseBranch"
    ahead_by = $aheadBy
    behind_by = $behindBy
    merge_commit_count = $mergeCommitCount
    merge_commit_samples = @($mergeCommitSamples)
    local_source_of_truth = [bool] $LocalSourceOfTruth
    release_please_branch = $releasePleaseBranch
    repository = if ([string]::IsNullOrWhiteSpace($repositoryName)) { $null } else { $repositoryName }
    reuse_check_status = $reuseCheckStatus
    open_pr = $openPr
    closed_branch_reuse = $closedReuse
    suggested_pr_title = if ([string]::IsNullOrWhiteSpace($suggestedPrTitle)) { $null } else { $suggestedPrTitle }
    status = $status
    advisories = @($advisories)
    actions = @($actions)
}

if ($AsJson) {
    $result | ConvertTo-Json -Depth 6
}
else {
    Write-Output "branch=$($result.source)"
    Write-Output "commitish=$($result.commitish)"
    Write-Output "base_branch=$($result.base_branch)"
    Write-Output "ahead_by=$($result.ahead_by)"
    Write-Output "behind_by=$($result.behind_by)"
    Write-Output "merge_commit_count=$($result.merge_commit_count)"
    Write-Output "local_source_of_truth=$($result.local_source_of_truth)"
    Write-Output "reuse_check_status=$($result.reuse_check_status)"
    Write-Output "status=$($result.status)"
    if ($null -ne $openPr) {
        Write-Output "open_pr=#$($openPr.number)"
    }
    if ($null -ne $closedReuse) {
        $stateLabel = if ($closedReuse.merged) { 'merged' } else { 'closed' }
        Write-Output "closed_branch_reuse=#$($closedReuse.number) $stateLabel at $($closedReuse.closed_at)"
    }
    foreach ($mergeCommit in $result.merge_commit_samples) {
        Write-Output "merge_commit=$mergeCommit"
    }
    if (-not [string]::IsNullOrWhiteSpace($result.suggested_pr_title)) {
        Write-Output "suggested_pr_title=$($result.suggested_pr_title)"
    }
    foreach ($action in $result.actions) {
        Write-Output "action=$action"
    }
    foreach ($advisory in $result.advisories) {
        Write-Output "advisory=$advisory"
    }
}

if ($status -ne 'ready') {
    exit 1
}
