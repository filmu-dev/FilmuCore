param(
    [Parameter(Mandatory = $true)]
    [string] $MessageFile
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

if (-not (Test-Path -LiteralPath $MessageFile)) {
    throw "Commit message file '$MessageFile' does not exist."
}

$allowedTypes = @('feat', 'fix', 'docs', 'build', 'test', 'refactor', 'chore')
$allowedPrefixPattern = '^(?:' + (($allowedTypes | ForEach-Object { [Regex]::Escape($_) }) -join '|') + ')(?:\([^)]+\))?!?: '
$skipPatterns = @(
    '^Merge ',
    '^Revert ',
    '^fixup!',
    '^squash!'
)

$lines = @(Get-Content -LiteralPath $MessageFile)
if ($lines.Count -eq 0) {
    return
}

$subjectIndex = -1
for ($i = 0; $i -lt $lines.Count; $i++) {
    $candidate = $lines[$i].Trim()
    if ($candidate.Length -gt 0 -and -not $candidate.StartsWith('#')) {
        $subjectIndex = $i
        break
    }
}

if ($subjectIndex -lt 0) {
    return
}

$subject = $lines[$subjectIndex]
if ($subject -match $allowedPrefixPattern) {
    return
}

foreach ($pattern in $skipPatterns) {
    if ($subject -match $pattern) {
        return
    }
}

function Invoke-GitCapture {
    param([Parameter(Mandatory = $true)][string[]] $Arguments)

    $output = & git -C $repoRoot @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        $joined = $Arguments -join ' '
        throw "git $joined failed`n$output"
    }

    return [string]::Join("`n", @($output))
}

$stagedText = Invoke-GitCapture -Arguments @('diff', '--cached', '--name-only', '--diff-filter=ACMRD')
$stagedPaths = @($stagedText -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })

function Test-AllMatch {
    param(
        [string[]] $Items,
        [scriptblock] $Predicate
    )

    if ($Items.Count -eq 0) {
        return $false
    }

    foreach ($item in $Items) {
        if (-not (& $Predicate $item)) {
            return $false
        }
    }

    return $true
}

$inferredType = 'chore'
if (Test-AllMatch -Items $stagedPaths -Predicate { param($path) $path -match '^(docs/|README\.md$|CHANGELOG\.md$|.*\.md$)' }) {
    $inferredType = 'docs'
}
elseif (Test-AllMatch -Items $stagedPaths -Predicate { param($path) $path -match '^(tests/|.*test.*\.py$|.*_test\.rs$)' }) {
    $inferredType = 'test'
}
elseif (Test-AllMatch -Items $stagedPaths -Predicate { param($path) $path -match '^(\.github/|\.githooks/|package\.json$|pnpm-lock\.yaml$|pyproject\.toml$|uv\.lock$|rust/filmuvfs/Cargo\.(toml|lock)$|scripts/.*)' }) {
    $inferredType = 'build'
}
elseif ($stagedPaths | Where-Object { $_ -match '^(filmu_py/|rust/filmuvfs/|proto/)' }) {
    $inferredType = 'fix'
}

$lines[$subjectIndex] = "${inferredType}: $subject"
Set-Content -LiteralPath $MessageFile -Value $lines
