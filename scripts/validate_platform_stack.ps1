param(
    [switch] $SkipComposeConfig
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

function Test-PowerShellScriptSyntax {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    $errors = $null
    [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$null, [ref]$errors) | Out-Null
    if ($errors.Count -gt 0) {
        $messages = $errors | ForEach-Object { $_.ToString() }
        throw ("PowerShell syntax validation failed for {0}:{1}{2}" -f $Path, [Environment]::NewLine, ($messages -join [Environment]::NewLine))
    }
}

function Assert-FileExists {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        throw ("Required file is missing: {0}" -f $Path)
    }
}

function Test-DockerComposeConfig {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ComposeFile
    )

    docker compose -f $ComposeFile config | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw ("docker compose config failed for {0}" -f $ComposeFile)
    }
}

$powerShellScripts = @(
    'scripts\start_local_stack.ps1',
    'scripts\status_local_stack.ps1',
    'scripts\stop_local_stack.ps1',
    'scripts\start_windows_stack.ps1',
    'check_windows_stack.ps1',
    'scripts\status_windows_stack.ps1',
    'scripts\stop_windows_stack.ps1'
) | ForEach-Object { Join-Path $repoRoot $_ }

$composeFiles = @(
    (Join-Path $repoRoot 'docker-compose.yml'),
    (Join-Path $repoRoot 'docker-compose.windows.yml')
)

$documentationFiles = @(
    (Join-Path $repoRoot 'README.md'),
    (Join-Path $repoRoot 'QUICK_START.md'),
    (Join-Path $repoRoot 'WINDOWS_README.md'),
    (Join-Path $repoRoot 'LINUX_UNIX_README.md'),
    (Join-Path $repoRoot 'docs\LOCAL_DOCKER_STACK.md')
)

Write-Host '==> Validating FilmuCore platform stack split' -ForegroundColor Cyan
Write-Host ''

Write-Host '[1/4] Checking required files...' -ForegroundColor Yellow
foreach ($path in ($powerShellScripts + $composeFiles + $documentationFiles + (Join-Path $repoRoot 'package.json'))) {
    Assert-FileExists -Path $path
}
Write-Host '      ✓ Required files are present' -ForegroundColor Green

Write-Host ''
Write-Host '[2/4] Validating helper script syntax...' -ForegroundColor Yellow
foreach ($path in $powerShellScripts) {
    Test-PowerShellScriptSyntax -Path $path
}
Write-Host '      ✓ Helper scripts parsed successfully' -ForegroundColor Green

Write-Host ''
Write-Host '[3/4] Validating package.json...' -ForegroundColor Yellow
Get-Content (Join-Path $repoRoot 'package.json') -Raw | ConvertFrom-Json | Out-Null
Write-Host '      ✓ package.json is valid JSON' -ForegroundColor Green

Write-Host ''
Write-Host '[4/4] Validating compose files...' -ForegroundColor Yellow
if ($SkipComposeConfig) {
    Write-Host '      ⚠ Skipped docker compose config validation' -ForegroundColor Yellow
}
else {
    foreach ($composeFile in $composeFiles) {
        Test-DockerComposeConfig -ComposeFile $composeFile
    }
    Write-Host '      ✓ docker compose config passed for Linux and Windows files' -ForegroundColor Green
}

Write-Host ''
Write-Host '==> Platform stack validation passed' -ForegroundColor Green
