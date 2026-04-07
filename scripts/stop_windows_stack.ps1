param(
    [switch] $LeaveBackendRunning
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Read-State {
    param(
        [Parameter(Mandatory = $true)]
        [string] $StatePath
    )

    if (-not (Test-Path -LiteralPath $StatePath)) {
        return $null
    }

    return (Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json)
}

function Resolve-ManagedMountPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $MountPath
    )

    $resolvedPath = [System.IO.Path]::GetFullPath($MountPath)
    $rootPath = [System.IO.Path]::GetPathRoot($resolvedPath)
    if ([string]::IsNullOrWhiteSpace($resolvedPath) -or $resolvedPath -eq $rootPath) {
        throw ("Refusing to manage mount root '{0}'. Choose a dedicated directory such as C:\FilmuCoreVFS." -f $resolvedPath)
    }

    return $resolvedPath
}

function Clear-MountPathContents {
    param(
        [Parameter(Mandatory = $true)]
        [string] $MountPath
    )

    $resolvedPath = Resolve-ManagedMountPath -MountPath $MountPath
    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        return
    }

    $children = @(Get-ChildItem -LiteralPath $resolvedPath -Force -ErrorAction SilentlyContinue)
    foreach ($child in $children) {
        $removed = $false
        for ($attempt = 1; $attempt -le 5 -and -not $removed; $attempt++) {
            try {
                Remove-Item -LiteralPath $child.FullName -Recurse -Force -ErrorAction Stop
                $removed = $true
            }
            catch {
                if ($attempt -eq 5) {
                    throw
                }
                Start-Sleep -Milliseconds 500
            }
        }
    }
}

function Try-Clear-MountPathContents {
    param(
        [Parameter(Mandatory = $true)]
        [string] $MountPath
    )

    try {
        Clear-MountPathContents -MountPath $MountPath
        return $null
    }
    catch {
        return $_.Exception.Message
    }
}

function Try-Remove-MountPathDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string] $MountPath
    )

    try {
        $resolvedPath = Resolve-ManagedMountPath -MountPath $MountPath
        if (-not (Test-Path -LiteralPath $resolvedPath)) {
            return $null
        }

        Remove-Item -LiteralPath $resolvedPath -Force -ErrorAction Stop
        return $null
    }
    catch {
        return $_.Exception.Message
    }
}

function Normalize-DriveLetter {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DriveLetter
    )

    $normalized = $DriveLetter.Trim().TrimEnd(':').ToUpperInvariant()
    if ($normalized -notmatch '^[A-Z]$') {
        throw ("DriveLetter must be a single letter such as X or X:, got '{0}'" -f $DriveLetter)
    }

    return $normalized
}

function Remove-DriveLetterMapping {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DriveLetter
    )

    cmd /c subst ('{0}:' -f $DriveLetter) /D | Out-Null
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'docker-compose.windows.yml'
$stateDirectory = Join-Path $repoRoot 'playback-proof-artifacts\windows-native-stack'
$statePath = Join-Path $stateDirectory 'filmuvfs-windows-state.json'
$state = Read-State -StatePath $statePath

Write-Host '==> Stopping FilmuCore Windows-native stack' -ForegroundColor Cyan
Write-Host ''

Write-Host '[1/2] Stopping Windows-native filmuvfs mount...' -ForegroundColor Yellow
if ($null -eq $state) {
    Write-Host '      ⚠ No managed Windows filmuvfs state file was found' -ForegroundColor Yellow
}
else {
    $managedPid = [long]$state.pid
    $process = Get-Process -Id $managedPid -ErrorAction SilentlyContinue
    if ($null -eq $process) {
        Write-Host ("      ⚠ No running process found for pid {0}" -f $managedPid) -ForegroundColor Yellow
    }
    else {
        Stop-Process -Id $managedPid -Force -ErrorAction Stop
        Write-Host ("      ✓ Windows filmuvfs process {0} stopped" -f $managedPid) -ForegroundColor Green
    }
    Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue

    if (
        (-not [string]::IsNullOrWhiteSpace([string]$state.drive_letter)) -and
        (
            ($state.PSObject.Properties.Match('drive_mapping_kind').Count -eq 0) -or
            ([string]$state.drive_mapping_kind -eq 'subst')
        )
    ) {
        $driveLetter = Normalize-DriveLetter -DriveLetter ([string]$state.drive_letter)
        Remove-DriveLetterMapping -DriveLetter $driveLetter
        Write-Host ("      ✓ Removed drive alias {0}:\" -f $driveLetter) -ForegroundColor Green
    }

    if (-not [string]::IsNullOrWhiteSpace([string]$state.mount_path)) {
        if ([string]$state.mount_adapter -eq 'winfsp') {
            $removeError = Try-Remove-MountPathDirectory -MountPath ([string]$state.mount_path)
            if ($null -eq $removeError) {
                Write-Host ("      ✓ Removed managed WinFSP mount path {0}" -f ([string]$state.mount_path)) -ForegroundColor Green
            }
            else {
                Write-Host ("      ⚠ Managed WinFSP mount path could not be removed from {0}" -f ([string]$state.mount_path)) -ForegroundColor Yellow
                Write-Host ("        {0}" -f $removeError) -ForegroundColor Yellow
            }
        }
        else {
            $clearError = Try-Clear-MountPathContents -MountPath ([string]$state.mount_path)
            if ($null -eq $clearError) {
                Write-Host ("      ✓ Cleared projected mount contents from {0}" -f ([string]$state.mount_path)) -ForegroundColor Green
            }
            else {
                Write-Host ("      ⚠ Mount contents could not be fully cleared from {0}" -f ([string]$state.mount_path)) -ForegroundColor Yellow
                Write-Host ("        {0}" -f $clearError) -ForegroundColor Yellow
                Write-Host '        Windows clients may still be holding projected files open. Release those handles before the next restart if the mountpoint stays populated.' -ForegroundColor Yellow
            }
        }
    }
}

Write-Host ''
if ($LeaveBackendRunning) {
    Write-Host '[2/2] Leaving Docker backend services running...' -ForegroundColor Yellow
    Write-Host '      ✓ Docker backend services left running' -ForegroundColor Green
}
else {
    Write-Host '[2/2] Stopping Docker backend services...' -ForegroundColor Yellow
    docker compose -f $composeFile down
    if ($LASTEXITCODE -ne 0) {
        throw 'docker compose failed to stop the Windows backend services'
    }
    Write-Host '      ✓ Docker backend services stopped' -ForegroundColor Green
}

Write-Host ''
Write-Host '==> ✅ FilmuCore Windows-native stack stopped successfully' -ForegroundColor Green


