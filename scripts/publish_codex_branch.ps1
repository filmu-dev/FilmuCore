param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string] $Topic,
    [string] $SourceBranch = '',
    [string] $Remote = 'origin',
    [string] $BaseBranch = 'main',
    [string] $CommitMessage = '',
    [switch] $NoFetch,
    [switch] $Push,
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

function Remove-TemporaryPatch {
    param([string] $Path)

    if (-not [string]::IsNullOrWhiteSpace($Path) -and (Test-Path -LiteralPath $Path)) {
        Remove-Item -LiteralPath $Path -Force
    }
}

if ([string]::IsNullOrWhiteSpace($SourceBranch)) {
    $SourceBranch = (Invoke-GitCapture -Arguments @('branch', '--show-current')).Trim()
}
if ([string]::IsNullOrWhiteSpace($SourceBranch)) {
    throw 'Cannot publish from a detached HEAD.'
}

$dirtyStatus = [string]::Join("`n", @(& git status --porcelain --untracked-files=no 2>&1))
if ($LASTEXITCODE -ne 0) {
    throw "git status failed`n$dirtyStatus"
}
if ((-not $DryRun) -and -not [string]::IsNullOrWhiteSpace($dirtyStatus)) {
    throw 'Refusing to publish from a branch with tracked changes present. Commit, stash, or discard the current work first.'
}

if (-not $NoFetch) {
    Invoke-GitCapture -Arguments @('fetch', $Remote, $BaseBranch, '--prune') | Out-Null
}

$baseRef = "refs/remotes/$Remote/$BaseBranch"
Invoke-GitCapture -Arguments @('rev-parse', '--verify', $baseRef) | Out-Null
Invoke-GitCapture -Arguments @('rev-parse', '--verify', $SourceBranch) | Out-Null

$changedFiles = (Invoke-GitCapture -Arguments @('diff', '--name-only', $baseRef, $SourceBranch)).Trim()
if ([string]::IsNullOrWhiteSpace($changedFiles)) {
    throw "Branch '$SourceBranch' has no net content changes relative to '$Remote/$BaseBranch'."
}

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

$resolvedCommitMessage = $CommitMessage.Trim()
if ([string]::IsNullOrWhiteSpace($resolvedCommitMessage)) {
    $resolvedCommitMessage = (Invoke-GitCapture -Arguments @('log', '-1', '--pretty=%s', $SourceBranch)).Trim()
}
if ([string]::IsNullOrWhiteSpace($resolvedCommitMessage)) {
    throw 'Could not resolve a commit message for the publish commit.'
}

$result = [ordered]@{
    source_branch = $SourceBranch
    base_branch = "$Remote/$BaseBranch"
    new_branch = $newBranch
    commit_message = $resolvedCommitMessage
    push = [bool] $Push
    dry_run = [bool] $DryRun
}

if ($DryRun) {
    $result | ConvertTo-Json -Depth 4
    return
}

$startingBranch = (Invoke-GitCapture -Arguments @('branch', '--show-current')).Trim()
$temporaryPatch = [System.IO.Path]::GetTempFileName()

try {
    $patch = Invoke-GitCapture -Arguments @('diff', '--binary', '--full-index', $baseRef, $SourceBranch)
    [System.IO.File]::WriteAllText($temporaryPatch, $patch, [System.Text.UTF8Encoding]::new($false))

    Invoke-GitCapture -Arguments @('switch', '-c', $newBranch, $baseRef) | Out-Null
    Invoke-GitCapture -Arguments @('apply', '--index', '--3way', $temporaryPatch) | Out-Null
    Invoke-GitCapture -Arguments @('commit', '-m', $resolvedCommitMessage) | Out-Null

    if ($Push) {
        Invoke-GitCapture -Arguments @('push', '-u', $Remote, $newBranch) | Out-Null
    }
}
catch {
    $currentBranch = ''
    try {
        $currentBranch = (Invoke-GitCapture -Arguments @('branch', '--show-current')).Trim()
    }
    catch {
        $currentBranch = ''
    }

    if ($currentBranch -eq $newBranch) {
        & git switch $startingBranch | Out-Null
        & git branch -D $newBranch | Out-Null
    }

    throw
}
finally {
    Remove-TemporaryPatch -Path $temporaryPatch
}

Write-Output "Created publish branch $newBranch from $Remote/$BaseBranch using the net diff from $SourceBranch."
if ($Push) {
    Write-Output "Pushed $newBranch to $Remote."
}
else {
    Write-Output "Next step: git push -u $Remote $newBranch"
}
