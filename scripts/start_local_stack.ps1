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

function Wait-HttpReady {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Uri,
        [Parameter(Mandatory = $true)]
        [int] $TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 3
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                return $true
            }
        }
        catch {
            Start-Sleep -Seconds 1
        }
    }

    return $false
}

function Test-MountHealth {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Distro
    )

    $listing = wsl.exe -d $Distro -u root -- bash -lc "ls /mnt/filmuvfs 2>/dev/null || true"
    return ($listing -match '(^|\r?\n)(movies|shows)(\r?\n|$)')
}

function Repair-MountRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Distro
    )

    $repairCommand = @'
set -u

# Best-effort stale-endpoint cleanup first (handles ENOTCONN states).
fusermount3 -uz /mnt/filmuvfs >/dev/null 2>&1 || umount -l /mnt/filmuvfs >/dev/null 2>&1 || true
sleep 1

# Ensure mount root exists. If mkdir/list fails, retry one more cleanup cycle.
mkdir -p /mnt/filmuvfs >/dev/null 2>&1 || true
if ! timeout 2 ls /mnt/filmuvfs >/dev/null 2>&1; then
  fusermount3 -uz /mnt/filmuvfs >/dev/null 2>&1 || umount -l /mnt/filmuvfs >/dev/null 2>&1 || true
  sleep 1
  rm -rf /mnt/filmuvfs >/dev/null 2>&1 || true
  mkdir -p /mnt/filmuvfs >/dev/null 2>&1 || true
fi

# Final sanity: path must be listable as a plain directory before Docker bind validation.
if ! timeout 2 ls /mnt/filmuvfs >/dev/null 2>&1; then
  echo "[repair] /mnt/filmuvfs remains unhealthy" >&2
  exit 1
fi
'@

    # Normalize line endings and run through base64 to avoid bash -lc quoting/newline issues.
    $repairCommand = $repairCommand -replace "`r", ""
    $encodedRepairCommand = [Convert]::ToBase64String(
        [System.Text.Encoding]::UTF8.GetBytes($repairCommand)
    )

    wsl.exe -d $Distro -u root -- bash -lc "echo '$encodedRepairCommand' | base64 -d | bash"
    if ($LASTEXITCODE -ne 0) {
        throw 'failed to repair /mnt/filmuvfs mount root in WSL'
    }
}

$scriptRoot = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptRoot
$wslDistro = 'Ubuntu-22.04'
$wslRepoRoot = Convert-ToWslPath -WindowsPath $repoRoot
$mountStartScript = "$wslRepoRoot/rust/filmuvfs/scripts/start_persistent_mount.sh"
$windowsMountRoot = "\\wsl.localhost\$wslDistro\mnt\filmuvfs"

Write-Host '==> Starting FilmuCore local stack' -ForegroundColor Cyan
Write-Host ''

Write-Host '[0/3] Repairing WSL mount root preflight (/mnt/filmuvfs)...' -ForegroundColor Yellow
Repair-MountRoot -Distro $wslDistro
Write-Host '      ✓ Mount root preflight complete' -ForegroundColor Green

Write-Host ''

Write-Host '[1/3] Starting Docker Compose services...' -ForegroundColor Yellow
$composeStarted = $false
for ($attempt = 1; $attempt -le 2; $attempt++) {
    wsl.exe -d $wslDistro -- bash -lc "cd '$wslRepoRoot' && docker compose -f docker-compose.yml up -d"
    if ($LASTEXITCODE -eq 0) {
        $composeStarted = $true
        break
    }

    if ($attempt -eq 1) {
        Write-Host '      [!] Docker compose failed. Repairing mount root and retrying once...' -ForegroundColor Yellow
        Repair-MountRoot -Distro $wslDistro
        Start-Sleep -Seconds 2
    }
}

if (-not $composeStarted) {
    throw 'docker compose failed to start the backend stack after automatic mount-root repair retry'
}
Write-Host '      ✓ Docker services started' -ForegroundColor Green

Write-Host ''
Write-Host '[2/3] Waiting for backend API and gRPC supplier...' -ForegroundColor Yellow
if (-not (Wait-HttpReady -Uri 'http://localhost:8000/openapi.json' -TimeoutSeconds 45)) {
    throw 'backend API did not become ready at http://localhost:8000/openapi.json'
}

$grpcReady = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        $async = $client.BeginConnect('127.0.0.1', 50051, $null, $null)
        if ($async.AsyncWaitHandle.WaitOne(2000) -and $client.Connected) {
            $client.EndConnect($async)
            $client.Close()
            $grpcReady = $true
            break
        }
        $client.Close()
    }
    catch {
        Start-Sleep -Seconds 1
    }
}

if (-not $grpcReady) {
    throw 'gRPC catalog supplier did not become ready on localhost:50051'
}
Write-Host '      ✓ Backend API and gRPC supplier are ready' -ForegroundColor Green

Write-Host ''
Write-Host '[3/3] Starting persistent FilmuVFS mount in WSL...' -ForegroundColor Yellow
wsl.exe -d $wslDistro -u root -- bash $mountStartScript
if ($LASTEXITCODE -ne 0) {
    throw 'persistent WSL FilmuVFS mount failed to start'
}

if (-not (Test-MountHealth -Distro $wslDistro)) {
    Write-Host '      [!] Mount appears stale, forcing cleanup and retry...' -ForegroundColor Yellow
    wsl.exe -d $wslDistro -u root -- bash -lc "fusermount3 -uz /mnt/filmuvfs 2>/dev/null || umount -l /mnt/filmuvfs 2>/dev/null || true; sleep 2"
    wsl.exe -d $wslDistro -u root -- bash $mountStartScript
    if ($LASTEXITCODE -ne 0) {
        throw 'persistent WSL FilmuVFS mount failed during retry'
    }

    Start-Sleep -Seconds 2
    if (-not (Test-MountHealth -Distro $wslDistro)) {
        throw 'mount is present but unhealthy after retry; inspect /tmp/filmuvfs_persistent.log in WSL'
    }
}

Write-Host '      ✓ FilmuVFS mount started' -ForegroundColor Green

Start-Sleep -Seconds 1
$windowsVisible = Test-Path $windowsMountRoot

Write-Host ''
Write-Host '==> ✅ FilmuCore stack started successfully' -ForegroundColor Green
Write-Host ''
Write-Host 'Services:' -ForegroundColor Cyan
Write-Host '  Backend API:   http://localhost:8000' -ForegroundColor White
Write-Host '  API Docs:      http://localhost:8000/docs' -ForegroundColor White
Write-Host '  gRPC Catalog:  localhost:50051' -ForegroundColor White
Write-Host '  Plex:          http://localhost:32401/web' -ForegroundColor White
Write-Host '  Emby:          http://localhost:8097' -ForegroundColor White
Write-Host ''
Write-Host 'Mount:' -ForegroundColor Cyan
Write-Host '  WSL Path:      /mnt/filmuvfs' -ForegroundColor White
Write-Host "  Windows Path:  $windowsMountRoot" -ForegroundColor White
Write-Host "  Windows Seen:  $windowsVisible" -ForegroundColor White
Write-Host ''
Write-Host 'Library Paths:' -ForegroundColor Cyan
Write-Host "  Movies:        $windowsMountRoot\movies" -ForegroundColor White
Write-Host "  Shows:         $windowsMountRoot\shows" -ForegroundColor White
Write-Host ''
Write-Host 'Management:' -ForegroundColor Cyan
Write-Host '  Status:        .\status_local_stack.ps1' -ForegroundColor White
Write-Host '  Stop:          .\stop_local_stack.ps1' -ForegroundColor White
Write-Host ''
