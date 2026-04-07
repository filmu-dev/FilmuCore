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
$mountStatusScript = "$wslRepoRoot/rust/filmuvfs/scripts/persistent_mount_status.sh"
$windowsMountRoot = ('\\wsl.localhost\{0}\mnt\filmuvfs' -f $wslDistro)
$windowsMoviesPath = Join-Path $windowsMountRoot 'movies'
$windowsShowsPath = Join-Path $windowsMountRoot 'shows'

Write-Host '==> FilmuCore local stack status' -ForegroundColor Cyan
Write-Host ''

Write-Host '[Docker Compose]' -ForegroundColor Yellow
wsl.exe -d $wslDistro -- bash -lc "cd '$wslRepoRoot' && docker compose -f docker-compose.yml ps"

Write-Host ''
Write-Host '[Backend API]' -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri 'http://localhost:8000/openapi.json' -UseBasicParsing -TimeoutSec 3
    Write-Host ('  [OK] HTTP ready ({0})' -f $response.StatusCode) -ForegroundColor Green
}
catch {
    Write-Host '  [FAIL] HTTP not ready' -ForegroundColor Red
}

Write-Host ''
Write-Host '[gRPC Catalog]' -ForegroundColor Yellow
try {
    $client = [System.Net.Sockets.TcpClient]::new()
    $async = $client.BeginConnect('127.0.0.1', 50051, $null, $null)
    if ($async.AsyncWaitHandle.WaitOne(2000) -and $client.Connected) {
        $client.EndConnect($async)
        $client.Close()
        Write-Host '  [OK] localhost:50051 reachable' -ForegroundColor Green
    }
    else {
        $client.Close()
        Write-Host '  [FAIL] localhost:50051 not reachable' -ForegroundColor Red
    }
}
catch {
    Write-Host '  [FAIL] localhost:50051 not reachable' -ForegroundColor Red
}

Write-Host ''
Write-Host '[WSL Mount]' -ForegroundColor Yellow
wsl.exe -d $wslDistro -u root -- bash $mountStatusScript

Write-Host ''
Write-Host '[Windows Visibility]' -ForegroundColor Yellow
Write-Host ('  Path:    {0}' -f $windowsMountRoot) -ForegroundColor White
Write-Host ('  Visible: {0}' -f (Test-Path $windowsMountRoot)) -ForegroundColor White
Write-Host ('  Movies:  {0}' -f (Test-Path $windowsMoviesPath)) -ForegroundColor White
Write-Host ('  Shows:   {0}' -f (Test-Path $windowsShowsPath)) -ForegroundColor White

Write-Host ''
Write-Host '[Local Media Servers]' -ForegroundColor Yellow
foreach ($probe in @(
    @{ Name = 'Plex'; Uri = 'http://localhost:32401/web/index.html' },
    @{ Name = 'Emby'; Uri = 'http://localhost:8097' }
)) {
    try {
        $response = Invoke-WebRequest -Uri $probe.Uri -UseBasicParsing -TimeoutSec 3
        Write-Host ('  [OK] {0} reachable ({1})' -f $probe.Name, $response.StatusCode) -ForegroundColor Green
    }
    catch {
        if ($_.Exception.Response) {
            $statusCode = [int] $_.Exception.Response.StatusCode.value__
            if ($statusCode -lt 500) {
                Write-Host ('  [OK] {0} reachable ({1})' -f $probe.Name, $statusCode) -ForegroundColor Green
                continue
            }
        }
        Write-Host ('  [FAIL] {0} not reachable' -f $probe.Name) -ForegroundColor Red
    }
}

