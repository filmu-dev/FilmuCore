$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Convert-ToWslPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $WindowsPath
    )

    $resolved = [System.IO.Path]::GetFullPath($WindowsPath)
    $drive = $resolved.Substring(0, 1).ToLowerInvariant()
    $suffix = $resolved.Substring(2).Replace('\', '/')
    return "/mnt/$drive$suffix"
}

$scriptRoot = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptRoot
$wslDistro = 'Ubuntu-22.04'
$wslRepoRoot = Convert-ToWslPath -WindowsPath $repoRoot
$mountStopScript = "$wslRepoRoot/rust/filmuvfs/scripts/stop_persistent_mount.sh"

Write-Host '==> Stopping FilmuCore local stack' -ForegroundColor Cyan
Write-Host ''

Write-Host '[1/2] Stopping persistent WSL FilmuVFS mount...' -ForegroundColor Yellow
wsl.exe -d $wslDistro -u root -- bash $mountStopScript
if ($LASTEXITCODE -ne 0) {
    Write-Host '      ⚠ WSL mount stop returned non-zero; continuing with Docker shutdown' -ForegroundColor Yellow
} else {
    Write-Host '      ✓ FilmuVFS mount stopped' -ForegroundColor Green
}

Write-Host ''
Write-Host '[2/2] Stopping Docker Compose services...' -ForegroundColor Yellow
wsl.exe -d $wslDistro -- bash -lc "cd '$wslRepoRoot' && docker compose -f docker-compose.yml down"
if ($LASTEXITCODE -ne 0) {
    throw 'docker compose failed to stop the backend stack'
}
Write-Host '      ✓ Docker services stopped' -ForegroundColor Green

Write-Host ''
Write-Host '==> ✅ FilmuCore stack stopped successfully' -ForegroundColor Green
