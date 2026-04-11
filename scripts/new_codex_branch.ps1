param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string] $Topic,
    [string] $Remote = 'origin',
    [string] $BaseBranch = 'main',
    [switch] $NoFetch,
    [switch] $DryRun
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Invoke-GitCapture {
    param([Parameter(Mandatory = $true)][string[]] $Arguments)

    $output = & git @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        $joined = $Arguments -join ' '
        throw "git $joined failed`n$output"
    }

    return [string]::Join("`n", @($output))
}

function Get-SanitizedSlug {
    param([Parameter(Mandatory = $true)][string] $Value)

    $slug = $Value.ToLowerInvariant()
    $slug = [regex]::Replace($slug, '[^a-z0-9]+', '-')
    $slug = [regex]::Replace($slug, '-{2,}', '-').Trim('-')
    if ([string]::IsNullOrWhiteSpace($slug)) {
        throw 'Topic resolves to an empty branch slug after sanitization.'
    }

    return $slug
}

$currentBranch = (Invoke-GitCapture -Arguments @('branch', '--show-current')).Trim()
if ([string]::IsNullOrWhiteSpace($currentBranch)) {
    throw 'Cannot create a new branch from a detached HEAD. Check out a branch first.'
}

$dirtyStatus = [string]::Join("`n", @(& git status --porcelain --untracked-files=no 2>&1))
if ($LASTEXITCODE -ne 0) {
    throw "git status failed`n$dirtyStatus"
}
if ((-not $DryRun) -and -not [string]::IsNullOrWhiteSpace($dirtyStatus)) {
    throw 'Refusing to create a new Codex branch with tracked changes present. Commit, stash, or discard the current work first.'
}

if (-not $NoFetch) {
    Invoke-GitCapture -Arguments @('fetch', $Remote, $BaseBranch, '--prune') | Out-Null
}

$baseRef = "refs/remotes/$Remote/$BaseBranch"
Invoke-GitCapture -Arguments @('rev-parse', '--verify', $baseRef) | Out-Null

$slug = Get-SanitizedSlug -Value $Topic
$timestamp = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')
$newBranch = "codex/$slug-$timestamp"

& git show-ref --verify --quiet "refs/heads/$newBranch"
if ($LASTEXITCODE -eq 0) {
    throw "Local branch '$newBranch' already exists."
}

& git show-ref --verify --quiet "refs/remotes/$Remote/$newBranch"
if ($LASTEXITCODE -eq 0) {
    throw "Remote branch '$Remote/$newBranch' already exists."
}

$result = [ordered]@{
    current_branch = $currentBranch
    base_branch = "$Remote/$BaseBranch"
    new_branch = $newBranch
    fetched_base = (-not $NoFetch)
    tracked_changes_present = (-not [string]::IsNullOrWhiteSpace($dirtyStatus))
    dry_run = [bool] $DryRun
}

if ($DryRun) {
    $result | ConvertTo-Json -Depth 4
    return
}

Invoke-GitCapture -Arguments @('switch', '-c', $newBranch, $baseRef) | Out-Null

Write-Output "Created and checked out $newBranch from $Remote/$BaseBranch."
Write-Output "Next step: git push -u $Remote $newBranch"
