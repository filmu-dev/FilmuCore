param(
    [string] $MountPath = '',
    [string] $MountAdapter = 'auto',
    [string] $DriveLetter = '',
    [string] $GrpcServer = 'http://127.0.0.1:50051',
    [int] $SummaryIntervalSeconds = 300,
    [int] $PrefetchMinChunks = 0,
    [int] $PrefetchMaxChunks = 0,
    [int] $PrefetchStartupChunks = 0,
    [int] $ScanChunkSizeKb = 0,
    [switch] $SkipProjFsAutoEnable,
    [switch] $SkipWinFspAutoInstall,
    [switch] $ProjFsAutoEnableAttempted,
    [switch] $WinFspAutoInstallAttempted,
    [switch] $WinFspMountElevationAttempted,
    [string] $ProjFsAutoEnableResultPath = '',
    [string] $WinFspAutoInstallResultPath = '',
    [switch] $SkipBackendStart,
    [switch] $SkipBuild
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Get-DefaultMountPath {
    $systemDrive = [System.Environment]::GetEnvironmentVariable('SystemDrive')
    if ([string]::IsNullOrWhiteSpace($systemDrive)) {
        $systemDrive = 'C:'
    }
    return (Join-Path $systemDrive 'FilmuCoreVFS')
}

function Normalize-MountAdapter {
    param(
        [Parameter(Mandatory = $true)]
        [string] $MountAdapter
    )

    $normalized = $MountAdapter.Trim().ToLowerInvariant()
    switch ($normalized) {
        '' { return 'auto' }
        'auto' { return 'auto' }
        'projfs' { return 'projfs' }
        'projectedfs' { return 'projfs' }
        'projected-filesystem' { return 'projfs' }
        'winfsp' { return 'winfsp' }
        default { throw ("MountAdapter must be one of: auto, projfs, winfsp. Got '{0}'" -f $MountAdapter) }
    }
}

function Test-IsAdministrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

function Invoke-SelfElevatedForProjFs {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ScriptPath,
        [Parameter(Mandatory = $true)]
        [string] $MountPath,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $DriveLetter,
        [Parameter(Mandatory = $true)]
        [string] $GrpcServer,
        [Parameter(Mandatory = $true)]
        [int] $SummaryIntervalSeconds,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMinChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMaxChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchStartupChunks,
        [Parameter(Mandatory = $true)]
        [int] $ScanChunkSizeKb,
        [Parameter(Mandatory = $true)]
        [string] $ResultPath,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBackendStart,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBuild
    )

    $pwshPath = (Get-Process -Id $PID).Path
    if ([string]::IsNullOrWhiteSpace($pwshPath)) {
        throw 'Unable to resolve the current PowerShell executable for automatic elevation.'
    }

    $argumentList = @(
        '-NoProfile',
        '-File', $ScriptPath,
        '-MountPath', $MountPath,
        '-GrpcServer', $GrpcServer,
        '-SummaryIntervalSeconds', $SummaryIntervalSeconds.ToString(),
        '-PrefetchMinChunks', $PrefetchMinChunks.ToString(),
        '-PrefetchMaxChunks', $PrefetchMaxChunks.ToString(),
        '-PrefetchStartupChunks', $PrefetchStartupChunks.ToString(),
        '-ScanChunkSizeKb', $ScanChunkSizeKb.ToString(),
        '-ProjFsAutoEnableResultPath', $ResultPath,
        '-ProjFsAutoEnableAttempted'
    )

    if (-not [string]::IsNullOrWhiteSpace($DriveLetter)) {
        $argumentList += @('-DriveLetter', $DriveLetter)
    }
    if ($SkipBackendStart) {
        $argumentList += '-SkipBackendStart'
    }
    if ($SkipBuild) {
        $argumentList += '-SkipBuild'
    }

    Write-Host '      Projected File System is missing; requesting elevation to enable Client-ProjFS automatically...' -ForegroundColor Yellow

    try {
        $process = Start-Process -FilePath $pwshPath -Verb RunAs -ArgumentList $argumentList -WindowStyle Hidden -Wait -PassThru
    }
    catch {
        throw 'Automatic Projected File System enablement was cancelled or could not request elevation.'
    }

    if ($process.ExitCode -ne 0) {
        if (Test-Path -LiteralPath $ResultPath) {
            $resultMessage = (Get-Content -LiteralPath $ResultPath -Raw).Trim()
            if (-not [string]::IsNullOrWhiteSpace($resultMessage)) {
                throw $resultMessage
            }
        }
        throw ("Automatic Projected File System enablement exited with code {0}" -f $process.ExitCode)
    }

    if (Test-Path -LiteralPath $ResultPath) {
        $resultMessage = (Get-Content -LiteralPath $ResultPath -Raw).Trim()
        if (-not [string]::IsNullOrWhiteSpace($resultMessage)) {
            Write-Host ("      {0}" -f $resultMessage) -ForegroundColor Yellow
        }
    }

    exit $process.ExitCode
}

function Get-ProjFsLibraryPath {
    return 'C:\Windows\System32\projectedfslib.dll'
}

function Test-ProjFsAvailable {
    return (Test-Path -LiteralPath (Get-ProjFsLibraryPath))
}

function Get-WinFspSearchRoots {
    $roots = [System.Collections.Generic.List[string]]::new()

    $envRoots = @(
        [System.Environment]::GetEnvironmentVariable('WINFSP_INSTALL_DIR'),
        [System.Environment]::GetEnvironmentVariable('ProgramFiles'),
        [System.Environment]::GetEnvironmentVariable('ProgramFiles(x86)')
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

    foreach ($root in $envRoots) {
        $fullRoot = [System.IO.Path]::GetFullPath($root)
        if (-not $roots.Contains($fullRoot)) {
            $roots.Add($fullRoot)
        }
    }

    return $roots
}

function Get-WinFspInstallRoot {
    foreach ($root in (Get-WinFspSearchRoots)) {
        if (-not (Test-Path -LiteralPath $root)) {
            continue
        }

        if ((Split-Path -Leaf $root) -match '^WinFsp') {
            return $root
        }

        $candidate = Get-ChildItem -LiteralPath $root -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -match '^WinFsp' } |
            Sort-Object Name -Descending |
            Select-Object -First 1
        if ($null -ne $candidate) {
            return $candidate.FullName
        }
    }

    return $null
}

function Get-WinFspBinPath {
    $installRoot = Get-WinFspInstallRoot
    if ([string]::IsNullOrWhiteSpace($installRoot)) {
        return $null
    }

    $binPath = Join-Path $installRoot 'bin'
    if (Test-Path -LiteralPath $binPath) {
        return $binPath
    }

    return $null
}

function Get-WinFspLibraryPath {
    $installRoot = Get-WinFspInstallRoot
    if ([string]::IsNullOrWhiteSpace($installRoot)) {
        return $null
    }

    foreach ($candidate in @(
            (Join-Path $installRoot 'bin\winfsp-x64.dll'),
            (Join-Path $installRoot 'bin\winfsp.dll')
        )) {
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }

    return $null
}

function Test-WinFspAvailable {
    $libraryPath = Get-WinFspLibraryPath
    return (-not [string]::IsNullOrWhiteSpace($libraryPath))
}

function Add-WinFspBinToPath {
    $binPath = Get-WinFspBinPath
    if ([string]::IsNullOrWhiteSpace($binPath)) {
        return
    }

    $pathEntries = @($env:PATH -split ';') | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    if ($pathEntries -contains $binPath) {
        return
    }

    $env:PATH = '{0};{1}' -f $binPath, $env:PATH
}

function Test-WingetAvailable {
    return ($null -ne (Get-Command winget.exe -ErrorAction SilentlyContinue))
}

function Get-FilmuVfsCapabilities {
    param(
        [Parameter(Mandatory = $true)]
        [string] $BinaryPath
    )

    if (-not (Test-Path -LiteralPath $BinaryPath)) {
        return $null
    }

    try {
        $raw = & $BinaryPath --print-capabilities 2>$null
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return $null
        }
        return ($raw | ConvertFrom-Json)
    }
    catch {
        return $null
    }
}

function Enable-ProjFsFeature {
    if (-not (Test-IsAdministrator)) {
        Write-ProjFsAutoEnableResult -ResultPath $script:ProjFsAutoEnableResultPath -Message 'Projected File System is not enabled and automatic elevation is unavailable in this session.'
        throw 'Projected File System is not enabled and automatic elevation is unavailable in this session.'
    }

    Write-Host '      Projected File System is missing; enabling Client-ProjFS...' -ForegroundColor Yellow
    & dism.exe /online /Enable-Feature /FeatureName:Client-ProjFS /All /NoRestart
    $exitCode = $LASTEXITCODE

    if (($exitCode -ne 0) -and ($exitCode -ne 3010)) {
        Write-ProjFsAutoEnableResult -ResultPath $script:ProjFsAutoEnableResultPath -Message ("DISM failed while enabling Client-ProjFS (exit code {0})" -f $exitCode)
        throw ("DISM failed while enabling Client-ProjFS (exit code {0})" -f $exitCode)
    }

    if ($exitCode -eq 3010) {
        Write-ProjFsAutoEnableResult -ResultPath $script:ProjFsAutoEnableResultPath -Message 'Client-ProjFS was enabled successfully, but Windows reported that a reboot is required before FilmuVFS can mount. Reboot Windows and run start_windows_stack.ps1 again.'
        throw 'Client-ProjFS was enabled successfully, but Windows reported that a reboot is required before FilmuVFS can mount. Reboot Windows and re-run start_windows_stack.ps1.'
    }

    if (-not (Test-ProjFsAvailable)) {
        Write-ProjFsAutoEnableResult -ResultPath $script:ProjFsAutoEnableResultPath -Message 'Client-ProjFS was enabled, but projectedfslib.dll is still unavailable. Reboot Windows and run start_windows_stack.ps1 again.'
        throw 'Client-ProjFS was enabled, but projectedfslib.dll is still unavailable. Reboot Windows and re-run start_windows_stack.ps1.'
    }

    Write-ProjFsAutoEnableResult -ResultPath $script:ProjFsAutoEnableResultPath -Message 'Client-ProjFS was enabled successfully. Reboot Windows and run start_windows_stack.ps1 again so the native Windows mount can start cleanly.'
    throw 'Client-ProjFS was enabled successfully. Reboot Windows and re-run start_windows_stack.ps1 so the native Windows mount can start cleanly.'
}

function Write-ProjFsAutoEnableResult {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ResultPath,
        [Parameter(Mandatory = $true)]
        [string] $Message
    )

    if ([string]::IsNullOrWhiteSpace($ResultPath)) {
        return
    }

    Set-Content -LiteralPath $ResultPath -Value $Message -Encoding UTF8
}

function Write-WinFspAutoInstallResult {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ResultPath,
        [Parameter(Mandatory = $true)]
        [string] $Message
    )

    if ([string]::IsNullOrWhiteSpace($ResultPath)) {
        return
    }

    Set-Content -LiteralPath $ResultPath -Value $Message -Encoding UTF8
}

function Ensure-ProjFsAvailable {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ScriptPath,
        [Parameter(Mandatory = $true)]
        [string] $MountPath,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $DriveLetter,
        [Parameter(Mandatory = $true)]
        [string] $GrpcServer,
        [Parameter(Mandatory = $true)]
        [int] $SummaryIntervalSeconds,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMinChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMaxChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchStartupChunks,
        [Parameter(Mandatory = $true)]
        [int] $ScanChunkSizeKb,
        [Parameter(Mandatory = $true)]
        [string] $ProjFsAutoEnableResultPath,
        [Parameter(Mandatory = $true)]
        [bool] $ProjFsAutoEnableAttempted,
        [Parameter(Mandatory = $true)]
        [bool] $SkipAutoEnable,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBackendStart,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBuild
    )

    if (Test-ProjFsAvailable) {
        return
    }

    if ($SkipAutoEnable) {
        throw 'Projected File System is not enabled on this host. Enable Client-ProjFS or rerun without -SkipProjFsAutoEnable so the helper can attempt it automatically.'
    }

    if ((-not (Test-IsAdministrator)) -and (-not $ProjFsAutoEnableAttempted)) {
        if (-not [string]::IsNullOrWhiteSpace($ProjFsAutoEnableResultPath)) {
            Remove-Item -LiteralPath $ProjFsAutoEnableResultPath -Force -ErrorAction SilentlyContinue
        }
        Invoke-SelfElevatedForProjFs `
            -ScriptPath $ScriptPath `
            -MountPath $MountPath `
            -DriveLetter $DriveLetter `
            -GrpcServer $GrpcServer `
            -SummaryIntervalSeconds $SummaryIntervalSeconds `
            -PrefetchMinChunks $PrefetchMinChunks `
            -PrefetchMaxChunks $PrefetchMaxChunks `
            -PrefetchStartupChunks $PrefetchStartupChunks `
            -ScanChunkSizeKb $ScanChunkSizeKb `
            -ResultPath $ProjFsAutoEnableResultPath `
            -SkipBackendStart $SkipBackendStart `
            -SkipBuild $SkipBuild
    }

    Enable-ProjFsFeature
}

function Invoke-SelfElevatedForWinFsp {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ScriptPath,
        [Parameter(Mandatory = $true)]
        [string] $MountPath,
        [Parameter(Mandatory = $true)]
        [string] $MountAdapter,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $DriveLetter,
        [Parameter(Mandatory = $true)]
        [string] $GrpcServer,
        [Parameter(Mandatory = $true)]
        [int] $SummaryIntervalSeconds,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMinChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMaxChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchStartupChunks,
        [Parameter(Mandatory = $true)]
        [int] $ScanChunkSizeKb,
        [Parameter(Mandatory = $true)]
        [string] $ResultPath,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBackendStart,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBuild
    )

    $pwshPath = (Get-Process -Id $PID).Path
    if ([string]::IsNullOrWhiteSpace($pwshPath)) {
        throw 'Unable to resolve the current PowerShell executable for automatic elevation.'
    }

    $argumentList = @(
        '-NoProfile',
        '-File', $ScriptPath,
        '-MountPath', $MountPath,
        '-MountAdapter', $MountAdapter,
        '-GrpcServer', $GrpcServer,
        '-SummaryIntervalSeconds', $SummaryIntervalSeconds.ToString(),
        '-PrefetchMinChunks', $PrefetchMinChunks.ToString(),
        '-PrefetchMaxChunks', $PrefetchMaxChunks.ToString(),
        '-PrefetchStartupChunks', $PrefetchStartupChunks.ToString(),
        '-ScanChunkSizeKb', $ScanChunkSizeKb.ToString(),
        '-WinFspAutoInstallResultPath', $ResultPath,
        '-WinFspAutoInstallAttempted'
    )

    if (-not [string]::IsNullOrWhiteSpace($DriveLetter)) {
        $argumentList += @('-DriveLetter', $DriveLetter)
    }
    if ($SkipBackendStart) {
        $argumentList += '-SkipBackendStart'
    }
    if ($SkipBuild) {
        $argumentList += '-SkipBuild'
    }

    Write-Host '      WinFSP runtime is missing; requesting elevation to install it automatically...' -ForegroundColor Yellow

    try {
        $process = Start-Process -FilePath $pwshPath -Verb RunAs -ArgumentList $argumentList -WindowStyle Hidden -Wait -PassThru
    }
    catch {
        throw 'Automatic WinFSP installation was cancelled or could not request elevation.'
    }

    if ($process.ExitCode -ne 0) {
        if (Test-Path -LiteralPath $ResultPath) {
            $resultMessage = (Get-Content -LiteralPath $ResultPath -Raw).Trim()
            if (-not [string]::IsNullOrWhiteSpace($resultMessage)) {
                throw $resultMessage
            }
        }
        throw ("Automatic WinFSP installation exited with code {0}" -f $process.ExitCode)
    }

    if (Test-Path -LiteralPath $ResultPath) {
        $resultMessage = (Get-Content -LiteralPath $ResultPath -Raw).Trim()
        if (-not [string]::IsNullOrWhiteSpace($resultMessage)) {
            Write-Host ("      {0}" -f $resultMessage) -ForegroundColor Yellow
        }
    }

    exit $process.ExitCode
}

function Install-WinFspRuntime {
    if (-not (Test-IsAdministrator)) {
        Write-WinFspAutoInstallResult -ResultPath $script:WinFspAutoInstallResultPath -Message 'WinFSP is not installed and automatic elevation is unavailable in this session.'
        throw 'WinFSP is not installed and automatic elevation is unavailable in this session.'
    }

    if (-not (Test-WingetAvailable)) {
        Write-WinFspAutoInstallResult -ResultPath $script:WinFspAutoInstallResultPath -Message 'WinFSP is not installed and winget is not available on this host for automatic installation.'
        throw 'WinFSP is not installed and winget is not available on this host for automatic installation.'
    }

    Write-Host '      WinFSP runtime is missing; installing WinFSP.WinFsp with winget...' -ForegroundColor Yellow
    winget install --id WinFsp.WinFsp --exact --silent --accept-package-agreements --accept-source-agreements
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        Write-WinFspAutoInstallResult -ResultPath $script:WinFspAutoInstallResultPath -Message ("winget failed while installing WinFSP (exit code {0})" -f $exitCode)
        throw ("winget failed while installing WinFSP (exit code {0})" -f $exitCode)
    }

    if (-not (Test-WinFspAvailable)) {
        Write-WinFspAutoInstallResult -ResultPath $script:WinFspAutoInstallResultPath -Message 'winget reported success, but WinFSP could not be discovered afterwards. Reopen the terminal or verify the WinFSP install before retrying.'
        throw 'winget reported success, but WinFSP could not be discovered afterwards. Reopen the terminal or verify the WinFSP install before retrying.'
    }

    $libraryPath = Get-WinFspLibraryPath
    Write-WinFspAutoInstallResult -ResultPath $script:WinFspAutoInstallResultPath -Message ("WinFSP runtime installed successfully at {0}" -f $libraryPath)
}

function Invoke-SelfElevatedForWinFspMount {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ScriptPath,
        [Parameter(Mandatory = $true)]
        [string] $MountPath,
        [Parameter(Mandatory = $true)]
        [string] $MountAdapter,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $DriveLetter,
        [Parameter(Mandatory = $true)]
        [string] $GrpcServer,
        [Parameter(Mandatory = $true)]
        [int] $SummaryIntervalSeconds,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMinChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMaxChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchStartupChunks,
        [Parameter(Mandatory = $true)]
        [int] $ScanChunkSizeKb,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBackendStart,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBuild
    )

    $pwshPath = (Get-Process -Id $PID).Path
    if ([string]::IsNullOrWhiteSpace($pwshPath)) {
        throw 'Unable to resolve the current PowerShell executable for automatic elevation.'
    }

    $argumentList = @(
        '-NoProfile',
        '-File', $ScriptPath,
        '-MountPath', $MountPath,
        '-MountAdapter', $MountAdapter,
        '-GrpcServer', $GrpcServer,
        '-SummaryIntervalSeconds', $SummaryIntervalSeconds.ToString(),
        '-PrefetchMinChunks', $PrefetchMinChunks.ToString(),
        '-PrefetchMaxChunks', $PrefetchMaxChunks.ToString(),
        '-PrefetchStartupChunks', $PrefetchStartupChunks.ToString(),
        '-ScanChunkSizeKb', $ScanChunkSizeKb.ToString(),
        '-WinFspMountElevationAttempted'
    )

    if (-not [string]::IsNullOrWhiteSpace($DriveLetter)) {
        $argumentList += @('-DriveLetter', $DriveLetter)
    }
    if ($SkipBackendStart) {
        $argumentList += '-SkipBackendStart'
    }
    if ($SkipBuild) {
        $argumentList += '-SkipBuild'
    }

    Write-Host '      WinFSP folder mounts require elevation on this host; requesting Administrator approval automatically...' -ForegroundColor Yellow

    try {
        $process = Start-Process -FilePath $pwshPath -Verb RunAs -ArgumentList $argumentList -WindowStyle Hidden -Wait -PassThru
    }
    catch {
        throw 'Automatic WinFSP mount elevation was cancelled or could not request elevation.'
    }

    if ($process.ExitCode -ne 0) {
        throw ("Automatic WinFSP mount elevation exited with code {0}" -f $process.ExitCode)
    }

    exit $process.ExitCode
}

function Ensure-WinFspAvailable {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ScriptPath,
        [Parameter(Mandatory = $true)]
        [string] $MountPath,
        [Parameter(Mandatory = $true)]
        [string] $MountAdapter,
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string] $DriveLetter,
        [Parameter(Mandatory = $true)]
        [string] $GrpcServer,
        [Parameter(Mandatory = $true)]
        [int] $SummaryIntervalSeconds,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMinChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchMaxChunks,
        [Parameter(Mandatory = $true)]
        [int] $PrefetchStartupChunks,
        [Parameter(Mandatory = $true)]
        [int] $ScanChunkSizeKb,
        [Parameter(Mandatory = $true)]
        [string] $WinFspAutoInstallResultPath,
        [Parameter(Mandatory = $true)]
        [bool] $WinFspAutoInstallAttempted,
        [Parameter(Mandatory = $true)]
        [bool] $SkipAutoInstall,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBackendStart,
        [Parameter(Mandatory = $true)]
        [bool] $SkipBuild
    )

    if (Test-WinFspAvailable) {
        return
    }

    if ($SkipAutoInstall) {
        throw 'WinFSP is not installed on this host. Install WinFSP or rerun without -SkipWinFspAutoInstall so the helper can attempt it automatically.'
    }

    if ((-not (Test-IsAdministrator)) -and (-not $WinFspAutoInstallAttempted)) {
        if (-not [string]::IsNullOrWhiteSpace($WinFspAutoInstallResultPath)) {
            Remove-Item -LiteralPath $WinFspAutoInstallResultPath -Force -ErrorAction SilentlyContinue
        }
        Invoke-SelfElevatedForWinFsp `
            -ScriptPath $ScriptPath `
            -MountPath $MountPath `
            -MountAdapter $MountAdapter `
            -DriveLetter $DriveLetter `
            -GrpcServer $GrpcServer `
            -SummaryIntervalSeconds $SummaryIntervalSeconds `
            -PrefetchMinChunks $PrefetchMinChunks `
            -PrefetchMaxChunks $PrefetchMaxChunks `
            -PrefetchStartupChunks $PrefetchStartupChunks `
            -ScanChunkSizeKb $ScanChunkSizeKb `
            -ResultPath $WinFspAutoInstallResultPath `
            -SkipBackendStart $SkipBackendStart `
            -SkipBuild $SkipBuild
    }

    Install-WinFspRuntime
}

function Resolve-EffectiveMountAdapter {
    param(
        [Parameter(Mandatory = $true)]
        [string] $RequestedMountAdapter,
        [Parameter(Mandatory = $false)]
        $Capabilities = $null
    )

    if ($RequestedMountAdapter -eq 'auto') {
        if (($null -ne $Capabilities) -and $Capabilities.windows_winfsp_compiled) {
            return 'winfsp'
        }

        return 'projfs'
    }

    return $RequestedMountAdapter
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

function Ensure-DriveLetterAvailable {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DriveLetter
    )

    $driveRoot = '{0}:\' -f $DriveLetter
    if (Test-Path -LiteralPath $driveRoot) {
        throw ("Drive letter {0}: is already in use" -f $DriveLetter)
    }
}

function Get-DefaultWinFspDriveLetter {
    return 'X'
}

function Add-DriveLetterMapping {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DriveLetter,
        [Parameter(Mandatory = $true)]
        [string] $MountPath
    )

    cmd /c subst ('{0}:' -f $DriveLetter) $MountPath | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw ("Failed to map drive letter {0}: to {1}" -f $DriveLetter, $MountPath)
    }
}

function Get-DirectoryListing {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    # Use .NET API directly — cmd /c dir and PowerShell's Get-ChildItem both issue
    # different IRPs that can fail with ERROR_INVALID_FUNCTION on WinFSP drive-letter
    # mounts in non-interactive sessions. [IO.Directory]::GetFileSystemEntries uses
    # NtQueryDirectoryFile via the CLR which works correctly with WinFSP.
    try {
        Write-Host ("      [dir] GetFileSystemEntries('{0}')" -f $Path) -ForegroundColor DarkGray
        $entries = [System.IO.Directory]::GetFileSystemEntries($Path)
        if ($null -eq $entries) {
            Write-Host "      [dir] returned null" -ForegroundColor DarkGray
            return $null
        }
        $names = @($entries | ForEach-Object { [System.IO.Path]::GetFileName($_) } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        Write-Host ("      [dir] got {0} entries: {1}" -f $names.Count, ($names -join ', ')) -ForegroundColor DarkGray
        return $names
    }
    catch {
        Write-Host ("      [dir] caught {0}: {1}" -f $_.Exception.GetType().Name, $_.Exception.Message) -ForegroundColor DarkYellow
        return $null
    }
}


function Get-DirectoryListingViaCmd {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    try {
        Write-Host ("      [dir-cmd] dir /b '{0}'" -f $Path) -ForegroundColor DarkGray
        $output = & cmd /c "dir /b `"$Path`"" 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host ("      [dir-cmd] exit={0} output={1}" -f $LASTEXITCODE, ($output -join ' ')) -ForegroundColor DarkYellow
            return $null
        }

        $entries = @($output | ForEach-Object { ([string]$_).Trim() } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        Write-Host ("      [dir-cmd] got {0} entries: {1}" -f $entries.Count, ($entries -join ', ')) -ForegroundColor DarkGray
        return $entries
    }
    catch {
        Write-Host ("      [dir-cmd] caught {0}: {1}" -f $_.Exception.GetType().Name, $_.Exception.Message) -ForegroundColor DarkYellow
        return $null
    }
}
function Test-MountOperational {
    param(
        [Parameter(Mandatory = $true)]
        [string] $MountPath,
        [int] $Attempts = 3,
        [int] $BackoffMs = 1000,
        [string] $DiagnosticsPath = ''
    )

    $diagnostics = @()
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $output = & cmd /c "dir `"$MountPath`"" 2>&1
            $exitCode = $LASTEXITCODE
            $joinedOutput = ($output -join [Environment]::NewLine)
            $success = ($exitCode -eq 0) -and ($joinedOutput -notmatch 'File Not Found')
            $diagnostics += [pscustomobject]@{
                attempt = $attempt
                timestamp = (Get-Date).ToString('o')
                exit_code = $exitCode
                success = $success
                output = $joinedOutput
            }
            if ($success) {
                return [pscustomobject]@{
                    success = $true
                    diagnostics = $diagnostics
                }
            }
        }
        catch {
            $diagnostics += [pscustomobject]@{
                attempt = $attempt
                timestamp = (Get-Date).ToString('o')
                exit_code = -1
                success = $false
                output = $_.Exception.Message
            }
        }

        if ($attempt -lt $Attempts) {
            Start-Sleep -Milliseconds $BackoffMs
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($DiagnosticsPath)) {
        try {
            $diagnostics | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $DiagnosticsPath -Encoding UTF8
        }
        catch {
            Write-Host ("      [diag] failed to write diagnostics to {0}: {1}" -f $DiagnosticsPath, $_.Exception.Message) -ForegroundColor DarkYellow
        }
    }

    return [pscustomobject]@{
        success = $false
        diagnostics = $diagnostics
    }
}
function Test-WinFspVolumeOnline {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DriveLetter
    )

    # fsutil volume diskfree probes the volume via a DeviceIoControl IOCTL path
    # that is independent of directory enumeration, confirming the WinFSP kernel
    # device is accessible even when GetFileSystemEntries is still warming up.
    try {
        Write-Host ("      [fsutil] volume diskfree {0}:" -f $DriveLetter) -ForegroundColor DarkGray
        $fsutilOutput = & fsutil.exe volume diskfree ('{0}:' -f $DriveLetter) 2>&1
        $exitCode = $LASTEXITCODE
        Write-Host ("      [fsutil] exit={0} output={1}" -f $exitCode, ($fsutilOutput -join ' ')) -ForegroundColor DarkGray
        return ($exitCode -eq 0)
    }
    catch {
        Write-Host ("      [fsutil] caught {0}: {1}" -f $_.Exception.GetType().Name, $_.Exception.Message) -ForegroundColor DarkYellow
        return $false
    }
}

function Get-WinFspVolumeName {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DriveLetter
    )

    try {
        Write-Host ("      [mountvol] {0}: /L" -f $DriveLetter) -ForegroundColor DarkGray
        $mountvolOutput = & mountvol.exe ('{0}:' -f $DriveLetter) /L 2>&1
        $exitCode = $LASTEXITCODE
        Write-Host ("      [mountvol] exit={0} output={1}" -f $exitCode, ($mountvolOutput -join ' ')) -ForegroundColor DarkGray
        if ($exitCode -ne 0) {
            return $null
        }

        foreach ($line in @($mountvolOutput)) {
            $trimmed = ([string]$line).Trim()
            if ($trimmed -match '^\\\\\?\\Volume\{[0-9A-Fa-f-]+\}\\$') {
                return $trimmed
            }
        }
    }
    catch {
        Write-Host ("      [mountvol] caught {0}: {1}" -f $_.Exception.GetType().Name, $_.Exception.Message) -ForegroundColor DarkYellow
    }

    return $null
}

function Wait-LogSentinel {
    param(
        [Parameter(Mandatory = $true)]
        [string] $LogPath,
        [Parameter(Mandatory = $true)]
        [string] $Sentinel,
        [Parameter(Mandatory = $true)]
        [System.Diagnostics.Process] $Process,
        [Parameter(Mandatory = $true)]
        [int] $TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $pollCount = 0
    while ((Get-Date) -lt $deadline) {
        if ($Process.HasExited) {
            Write-Host ("      [sentinel] process exited after {0} polls" -f $pollCount) -ForegroundColor DarkYellow
            return $false
        }
        if (Test-Path -LiteralPath $LogPath) {
            $content = Get-Content -LiteralPath $LogPath -Raw -ErrorAction SilentlyContinue
            if (-not [string]::IsNullOrEmpty($content) -and $content -match [regex]::Escape($Sentinel)) {
                Write-Host ("      [sentinel] found after {0} polls in '{1}'" -f $pollCount, $LogPath) -ForegroundColor DarkGray
                return $true
            }
        }
        elseif ($pollCount -eq 0) {
            Write-Host ("      [sentinel] log file does not yet exist: '{0}'" -f $LogPath) -ForegroundColor DarkGray
        }
        $pollCount++
        Start-Sleep -Milliseconds 250
    }
    Write-Host ("      [sentinel] timed out after {0} polls ({1}s); log exists={2}" -f $pollCount, $TimeoutSeconds, (Test-Path -LiteralPath $LogPath)) -ForegroundColor DarkYellow
    return $false
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

function Test-GrpcReady {
    param(
        [Parameter(Mandatory = $true)]
        [string] $HostName,
        [Parameter(Mandatory = $true)]
        [int] $Port,
        [Parameter(Mandatory = $true)]
        [int] $TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $client = [System.Net.Sockets.TcpClient]::new()
            $async = $client.BeginConnect($HostName, $Port, $null, $null)
            if ($async.AsyncWaitHandle.WaitOne(2000) -and $client.Connected) {
                $client.EndConnect($async)
                $client.Close()
                return $true
            }
            $client.Close()
        }
        catch {
        }

        Start-Sleep -Seconds 1
    }

    return $false
}

function Wait-ContainerHealthy {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ContainerName,
        [Parameter(Mandatory = $true)]
        [int] $TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $status = (& docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $ContainerName 2>$null | Select-Object -First 1).Trim()
            if ($status -eq 'healthy') {
                return $true
            }
        }
        catch {
        }

        Start-Sleep -Seconds 1
    }

    return $false
}

function Get-StateDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string] $RepoRoot
    )

    return (Join-Path $RepoRoot 'playback-proof-artifacts\windows-native-stack')
}

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

function Ensure-MountPathClear {
    param(
        [Parameter(Mandatory = $true)]
        [string] $MountPath
    )

    try {
        Clear-MountPathContents -MountPath $MountPath
    }
    catch {
        throw ("Mount path '{0}' still contains projected entries that Windows clients are holding open. Close active Jellyfin/Plex/Emby scans or ffmpeg/ffprobe readers and retry. Original error: {1}" -f $MountPath, $_.Exception.Message)
    }
}

function Remove-WinFspFolderMount {
    param(
        [Parameter(Mandatory = $true)]
        [string] $MountPath
    )

    $resolvedPath = Resolve-ManagedMountPath -MountPath $MountPath
    try {
        Write-Host ("      [mountvol] {0} /D" -f $resolvedPath) -ForegroundColor DarkGray
        $mountvolOutput = & mountvol.exe $resolvedPath /D 2>&1
        $exitCode = $LASTEXITCODE
        Write-Host ("      [mountvol] exit={0} output={1}" -f $exitCode, ($mountvolOutput -join ' ')) -ForegroundColor DarkGray
        if (($exitCode -ne 0) -and (Test-Path -LiteralPath $resolvedPath)) {
            Write-Host ("      [mountvol] folder unmount returned non-zero for '{0}'" -f $resolvedPath) -ForegroundColor DarkYellow
        }
    }
    catch {
        Write-Host ("      [mountvol] remove caught {0}: {1}" -f $_.Exception.GetType().Name, $_.Exception.Message) -ForegroundColor DarkYellow
    }
}

function Prepare-WinFspMountPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $MountPath
    )

    $resolvedPath = Resolve-ManagedMountPath -MountPath $MountPath
    $parentPath = Split-Path -Path $resolvedPath -Parent
    if (-not [string]::IsNullOrWhiteSpace($parentPath)) {
        New-Item -ItemType Directory -Path $parentPath -Force | Out-Null
    }

    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        return
    }

    $item = Get-Item -LiteralPath $resolvedPath -Force -ErrorAction Stop
    if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
        try {
            Remove-Item -LiteralPath $resolvedPath -Force -Recurse -ErrorAction Stop
            return
        }
        catch {
            throw ("Failed to remove stale WinFSP mountpoint '{0}': {1}" -f $resolvedPath, $_.Exception.Message)
        }
    }

    if (-not $item.PSIsContainer) {
        try {
            Remove-Item -LiteralPath $resolvedPath -Force -ErrorAction Stop
            return
        }
        catch {
            throw ("Failed to remove stale WinFSP mount placeholder at '{0}': {1}" -f $resolvedPath, $_.Exception.Message)
        }
    }

    Ensure-MountPathClear -MountPath $resolvedPath
    try {
        Remove-Item -LiteralPath $resolvedPath -Force -ErrorAction Stop
    }
    catch {
        throw ("WinFSP requires the mount directory path to be absent before mount startup. Failed to remove '{0}': {1}" -f $resolvedPath, $_.Exception.Message)
    }
}

function New-ManagedMountLink {
    param(
        [Parameter(Mandatory = $true)]
        [string] $LinkPath,
        [Parameter(Mandatory = $true)]
        [string] $TargetPath
    )

    $resolvedLinkPath = Resolve-ManagedMountPath -MountPath $LinkPath
    $normalizedTarget = $TargetPath.Trim().TrimEnd('\')
    if ($normalizedTarget -notmatch '^[A-Z]:$') {
        throw ("WinFSP managed folder mounts require a drive-letter runtime target, got '{0}'" -f $TargetPath)
    }

    $driveLetter = Normalize-DriveLetter -DriveLetter $normalizedTarget
    Prepare-WinFspMountPath -MountPath $resolvedLinkPath

    $volumeName = Get-WinFspVolumeName -DriveLetter $driveLetter
    if ([string]::IsNullOrWhiteSpace($volumeName)) {
        throw ("WinFSP volume name for {0}: was not available via mountvol after startup" -f $driveLetter)
    }

    Write-Host ("      [mountvol] attaching volume '{0}' to '{1}'" -f $volumeName, $resolvedLinkPath) -ForegroundColor DarkGray
    $mountvolOutput = & mountvol.exe $resolvedLinkPath $volumeName 2>&1
    $exitCode = $LASTEXITCODE
    Write-Host ("      [mountvol] exit={0} output={1}" -f $exitCode, ($mountvolOutput -join ' ')) -ForegroundColor DarkGray
    if ($exitCode -ne 0) {
        throw ("Failed to attach WinFSP volume {0} to {1}" -f $volumeName, $resolvedLinkPath)
    }

    Write-Host ("      [mountvol] attached volume '{0}' to '{1}'" -f $volumeName, $resolvedLinkPath) -ForegroundColor DarkGray
}

function Test-ManagedProcessAlive {
    param(
        [Parameter(Mandatory = $true)]
        [long] $ProcessId
    )

    $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    return ($null -ne $process)
}

function Get-StderrTail {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return ''
    }

    return ((Get-Content -LiteralPath $Path -Tail 40) -join [Environment]::NewLine)
}

function Wait-WindowsMountReady {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ProbePath,
        [Parameter(Mandatory = $true)]
        [int] $TimeoutSeconds,
        [Parameter(Mandatory = $true)]
        [System.Diagnostics.Process] $Process,
        [Parameter(Mandatory = $true)]
        [string] $StderrPath,
        [Parameter(Mandatory = $true)]
        [string] $StdoutPath,
        [Parameter(Mandatory = $false)]
        [string] $MountAdapter = '',
        [Parameter(Mandatory = $false)]
        [string] $DriveLetter = '',
        [Parameter(Mandatory = $false)]
        [string] $DiagnosticsPath = ''
    )

    # Phase 1 (WinFSP only): poll the process stdout for the sentinel line the
    # binary emits after
    # FspFileSystemStartDispatcher succeeds AND the initial catalog snapshot is
    # loaded from the gRPC supplier. This signal is reliable regardless of whether
    # directory enumeration IRPs work in the current session context.
    $winfspSentinelSeen = $false
    if ($MountAdapter -eq 'winfsp') {
        $sentinel = 'host filesystem adapter mounted successfully'
        $remainingSeconds = [math]::Max(1, $TimeoutSeconds - 2)
        Write-Host ("      [ready] waiting for WinFSP stdout sentinel: {0}" -f $sentinel) -ForegroundColor DarkGray
        $winfspSentinelSeen = Wait-LogSentinel `
            -LogPath $StdoutPath `
            -Sentinel $sentinel `
            -Process $Process `
            -TimeoutSeconds $remainingSeconds

        if ($Process.HasExited) {
            $stderrTail = Get-StderrTail -Path $StderrPath
            if ([string]::IsNullOrWhiteSpace($stderrTail)) { $stderrTail = 'no stderr output' }
            throw ("filmuvfs.exe exited early with code {0}: {1}" -f $Process.ExitCode, $stderrTail)
        }

        if (-not $winfspSentinelSeen) {
            $stderrTail = Get-StderrTail -Path $StderrPath
            if ([string]::IsNullOrWhiteSpace($stderrTail)) { $stderrTail = 'no stderr output' }
            throw ("Windows mountpoint did not become ready within {0}s (WinFSP sentinel not seen in stdout): {1}" -f $TimeoutSeconds, $stderrTail)
        }

        Write-Host '      [ready] WinFSP stdout sentinel observed' -ForegroundColor DarkGray

        # Brief stabilization: WinFSP kernel device registration can trail the
        # dispatcher startup by a small margin before it responds to user-space IRPs.
        Start-Sleep -Milliseconds 500
    }

    # Phase 2: once the WinFSP sentinel is observed, the mount is considered ready.
    # We still attempt a best-effort directory listing, but lack of enumeration in
    # this session should not block the managed start path.
    if ($winfspSentinelSeen) {
        for ($attempt = 1; $attempt -le 3; $attempt++) {
            $entries = Get-DirectoryListing -Path $ProbePath
            if ($null -ne $entries -and $entries.Count -gt 0) {
                Write-Host ("      [ready] enumerated runtime mount root: {0}" -f (($entries | Select-Object -First 10) -join ', ')) -ForegroundColor DarkGray
                return @($entries | Select-Object -First 10)
            }
            if ($attempt -lt 3) {
                Start-Sleep -Milliseconds 500
            }
        }

        $mountOperational = Test-MountOperational -MountPath $ProbePath -Attempts 3 -BackoffMs 1000 -DiagnosticsPath $DiagnosticsPath
        if ($mountOperational.success) {
            Write-Host ("      [ready] runtime mount '{0}' passed command-level operational probe" -f $ProbePath) -ForegroundColor DarkGray
            return @('movies', 'shows')
        }

        Write-Host ("      [ready] runtime mount '{0}' did not enumerate and failed command-level probes in this launcher context" -f $ProbePath) -ForegroundColor DarkYellow
        if (-not [string]::IsNullOrWhiteSpace($DiagnosticsPath)) {
            throw ("WinFSP signaled startup, but directory enumeration and operational probes failed for {0}. Diagnostics: {1}" -f $ProbePath, $DiagnosticsPath)
        }
        throw ("WinFSP signaled startup, but directory enumeration and operational probes failed for {0}" -f $ProbePath)
    }


    # Phase 2 (ProjFS): confirm the mount by enumerating root entries.
    $probeTimeoutSeconds = $TimeoutSeconds
    $deadline = (Get-Date).AddSeconds($probeTimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if ($Process.HasExited) {
            $stderrTail = Get-StderrTail -Path $StderrPath
            if ([string]::IsNullOrWhiteSpace($stderrTail)) { $stderrTail = 'no stderr output' }
            throw ("filmuvfs.exe exited early with code {0}: {1}" -f $Process.ExitCode, $stderrTail)
        }

        try {
            $entries = Get-DirectoryListing -Path $ProbePath
            if ($null -ne $entries) {
                $rootEntries = @($entries | Where-Object { $_ -in @('movies', 'shows') })
                if ($rootEntries.Count -gt 0) {
                    return $rootEntries
                }
            }
        }
        catch {
        }


        try {
            $cmdEntries = Get-DirectoryListingViaCmd -Path $ProbePath
            if ($null -ne $cmdEntries) {
                $rootEntries = @($cmdEntries | Where-Object { $_ -in @('movies', 'shows') })
                if ($rootEntries.Count -gt 0) {
                    return $rootEntries
                }
            }
        }
        catch {
        }
        Start-Sleep -Seconds 1
    }

    $stderrTail = Get-StderrTail -Path $StderrPath
    if ([string]::IsNullOrWhiteSpace($stderrTail)) {
        $stderrTail = 'no stderr output'
    }
    throw ("Windows mountpoint did not become ready within {0}s: {1}" -f $TimeoutSeconds, $stderrTail)
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot 'docker-compose.windows.yml'
$stateDirectory = Get-StateDirectory -RepoRoot $repoRoot
$statePath = Join-Path $stateDirectory 'filmuvfs-windows-state.json'
$stdoutPath = Join-Path $stateDirectory 'filmuvfs-windows-stdout.log'
$stderrPath = Join-Path $stateDirectory 'filmuvfs-windows-stderr.log'
$callbackTracePath = Join-Path $stateDirectory 'filmuvfs-windows-callbacks.log'
$wrapperTracePath = Join-Path $stateDirectory 'filmuvfs-winfsp-wrapper.log'
$mountDiagnosticsPath = Join-Path $stateDirectory 'mount-operational-diagnostics.json'
$runtimeStatusPath = Join-Path $stateDirectory 'filmuvfs-runtime-status.json'
$managerLogPath = Join-Path $stateDirectory 'start_windows_stack.log'
$binaryPath = Join-Path $repoRoot 'rust\filmuvfs\target\release\filmuvfs.exe'
if ([string]::IsNullOrWhiteSpace($ProjFsAutoEnableResultPath)) {
    $ProjFsAutoEnableResultPath = Join-Path $stateDirectory 'projfs-auto-enable-result.txt'
}
if ([string]::IsNullOrWhiteSpace($WinFspAutoInstallResultPath)) {
    $WinFspAutoInstallResultPath = Join-Path $stateDirectory 'winfsp-auto-install-result.txt'
}
New-Item -ItemType Directory -Path $stateDirectory -Force | Out-Null
try {
    Start-Transcript -LiteralPath $managerLogPath -Append -Force | Out-Null
}
catch {
}

trap {
    $errorRecord = $_
    try {
        Stop-Transcript | Out-Null
    }
    catch {
    }
    Write-Error -ErrorRecord $errorRecord
    exit 1
}

if ([string]::IsNullOrWhiteSpace($MountPath)) {
    $MountPath = Get-DefaultMountPath
}
$MountPath = Resolve-ManagedMountPath -MountPath $MountPath
$requestedMountAdapter = Normalize-MountAdapter -MountAdapter $MountAdapter
$filmuvfsCapabilities = Get-FilmuVfsCapabilities -BinaryPath $binaryPath
$effectiveMountAdapter = Resolve-EffectiveMountAdapter -RequestedMountAdapter $requestedMountAdapter -Capabilities $filmuvfsCapabilities

$normalizedDriveLetter = $null
if (-not [string]::IsNullOrWhiteSpace($DriveLetter)) {
    Write-Host ("      [config] DriveLetter '{0}' ignored; using folder-path mount only." -f $DriveLetter) -ForegroundColor DarkYellow
    $DriveLetter = ''
}

if ($effectiveMountAdapter -eq 'projfs') {
    Write-Host '[0/4] Verifying Windows Projected File System feature...' -ForegroundColor Yellow
    Ensure-ProjFsAvailable `
        -ScriptPath $PSCommandPath `
        -MountPath $MountPath `
        -DriveLetter $DriveLetter `
        -GrpcServer $GrpcServer `
        -SummaryIntervalSeconds $SummaryIntervalSeconds `
        -PrefetchMinChunks $PrefetchMinChunks `
        -PrefetchMaxChunks $PrefetchMaxChunks `
        -PrefetchStartupChunks $PrefetchStartupChunks `
        -ScanChunkSizeKb $ScanChunkSizeKb `
        -ProjFsAutoEnableResultPath $ProjFsAutoEnableResultPath `
        -ProjFsAutoEnableAttempted $ProjFsAutoEnableAttempted.IsPresent `
        -SkipAutoEnable $SkipProjFsAutoEnable.IsPresent `
        -SkipBackendStart $SkipBackendStart.IsPresent `
        -SkipBuild $SkipBuild.IsPresent
    Write-Host '      [OK] Projected File System is available' -ForegroundColor Green
    Write-Host ''
}

if ($effectiveMountAdapter -eq 'winfsp') {
    Write-Host '[0/4] Inspecting WinFSP backend support...' -ForegroundColor Yellow
    if (($null -eq $filmuvfsCapabilities) -or (-not $filmuvfsCapabilities.windows_winfsp_compiled)) {
        Write-Host '      Current binary does not advertise a WinFSP backend yet; deferring runtime installation until after the build check.' -ForegroundColor Yellow
    }
    else {
        Write-Host '      [OK] Current binary advertises a WinFSP backend' -ForegroundColor Green
    }
    Write-Host ''
}
elseif ($requestedMountAdapter -eq 'auto') {
    Write-Host '[0/4] Resolving Windows mount adapter...' -ForegroundColor Yellow
    Write-Host ("      [OK] Requested adapter: {0}" -f $requestedMountAdapter) -ForegroundColor Green
    Write-Host ("      [OK] Effective adapter: {0}" -f $effectiveMountAdapter) -ForegroundColor Green
    if (($null -eq $filmuvfsCapabilities) -or (-not $filmuvfsCapabilities.windows_winfsp_compiled)) {
        Write-Host '      WinFSP backend is unavailable in the current binary; auto falls back to projfs on Windows.' -ForegroundColor Yellow
    }
    Write-Host ''
}

if ((($requestedMountAdapter -eq 'winfsp') -or ($effectiveMountAdapter -eq 'winfsp')) -and ((-not $SkipBuild) -or (-not (Test-Path -LiteralPath $binaryPath)))) {
    Write-Host '[0a/4] Verifying WinFSP runtime for Windows builds...' -ForegroundColor Yellow
    Ensure-WinFspAvailable `
        -ScriptPath $PSCommandPath `
        -MountPath $MountPath `
        -MountAdapter $requestedMountAdapter `
        -DriveLetter $DriveLetter `
        -GrpcServer $GrpcServer `
        -SummaryIntervalSeconds $SummaryIntervalSeconds `
        -PrefetchMinChunks $PrefetchMinChunks `
        -PrefetchMaxChunks $PrefetchMaxChunks `
        -PrefetchStartupChunks $PrefetchStartupChunks `
        -ScanChunkSizeKb $ScanChunkSizeKb `
        -WinFspAutoInstallResultPath $WinFspAutoInstallResultPath `
        -WinFspAutoInstallAttempted $WinFspAutoInstallAttempted.IsPresent `
        -SkipAutoInstall $SkipWinFspAutoInstall.IsPresent `
        -SkipBackendStart $SkipBackendStart.IsPresent `
        -SkipBuild $SkipBuild.IsPresent
    Write-Host '      [OK] WinFSP runtime is available for native Windows builds' -ForegroundColor Green
    Write-Host ''
}

$runtimeMountPath = $MountPath
$driveMappingKind = $null
if ($effectiveMountAdapter -eq 'winfsp') {
    $normalizedDriveLetter = $null
}
elseif ($null -ne $normalizedDriveLetter) {
    $driveMappingKind = 'subst'
}

if ($null -ne $normalizedDriveLetter) {
    Ensure-DriveLetterAvailable -DriveLetter $normalizedDriveLetter
}

$existingState = Read-State -StatePath $statePath
if ($null -ne $existingState -and (Test-ManagedProcessAlive -ProcessId ([long]$existingState.pid))) {
    throw ("A managed Windows filmuvfs process is already running (pid {0}) for mountpoint {1}. Use .\status_windows_stack.ps1 or .\stop_windows_stack.ps1 first." -f $existingState.pid, $existingState.mount_path)
}

New-Item -ItemType Directory -Path $stateDirectory -Force | Out-Null
if ($effectiveMountAdapter -eq 'winfsp') {
    Prepare-WinFspMountPath -MountPath $MountPath
}
else {
    New-Item -ItemType Directory -Path $MountPath -Force | Out-Null
    Ensure-MountPathClear -MountPath $MountPath
}

Write-Host '==> Starting FilmuCore Windows-native stack' -ForegroundColor Cyan
Write-Host ''

if (-not $SkipBackendStart) {
    Write-Host '[1/4] Starting Docker backend services...' -ForegroundColor Yellow
    docker compose -f $composeFile up -d postgres redis zilean-postgres zilean filmu-python arq-worker frontend prowlarr
    if ($LASTEXITCODE -ne 0) {
        throw 'docker compose failed to start the Windows backend services'
    }
    Write-Host '      [OK] Docker backend services started' -ForegroundColor Green
}
else {
    Write-Host '[1/4] Skipping Docker backend start...' -ForegroundColor Yellow
    Write-Host '      [OK] Using existing backend services' -ForegroundColor Green
}

Write-Host ''
Write-Host '[2/4] Waiting for backend API and gRPC supplier...' -ForegroundColor Yellow
if (-not (Wait-HttpReady -Uri 'http://localhost:8000/openapi.json' -TimeoutSeconds 45)) {
    throw 'backend API did not become ready at http://localhost:8000/openapi.json'
}
if (-not (Test-GrpcReady -HostName '127.0.0.1' -Port 50051 -TimeoutSeconds 45)) {
    throw 'gRPC catalog supplier did not become ready on localhost:50051'
}
if (-not $SkipBackendStart) {
    if (-not (Wait-ContainerHealthy -ContainerName 'filmu-python' -TimeoutSeconds 45)) {
        throw 'filmu-python did not report a healthy container state after backend startup'
    }
}
Write-Host '      [OK] Backend API and gRPC supplier are ready' -ForegroundColor Green

Write-Host ''
Write-Host '[3/4] Preparing Windows-native filmuvfs host binary...' -ForegroundColor Yellow
if ((-not (Test-Path -LiteralPath $binaryPath)) -and $SkipBuild) {
    throw ("filmuvfs.exe was not found at {0} and -SkipBuild was set" -f $binaryPath)
}
if (-not (Test-Path -LiteralPath $binaryPath)) {
    cargo build --release --manifest-path (Join-Path $repoRoot 'rust\filmuvfs\Cargo.toml')
    if ($LASTEXITCODE -ne 0) {
        throw 'cargo build failed for rust/filmuvfs'
    }
}
$filmuvfsCapabilities = Get-FilmuVfsCapabilities -BinaryPath $binaryPath
if ($requestedMountAdapter -eq 'auto') {
    $effectiveMountAdapter = Resolve-EffectiveMountAdapter -RequestedMountAdapter $requestedMountAdapter -Capabilities $filmuvfsCapabilities
}
Write-Host '      [OK] Windows host binary is ready' -ForegroundColor Green

if ($effectiveMountAdapter -eq 'winfsp') {
    if (($null -eq $filmuvfsCapabilities) -or (-not $filmuvfsCapabilities.windows_winfsp_compiled)) {
        throw 'MountAdapter winfsp was selected, but the current filmuvfs.exe build does not include the WinFSP host backend yet. Use -MountAdapter auto or projfs for this build.'
    }

    Ensure-WinFspAvailable `
        -ScriptPath $PSCommandPath `
        -MountPath $MountPath `
        -MountAdapter $requestedMountAdapter `
        -DriveLetter $DriveLetter `
        -GrpcServer $GrpcServer `
        -SummaryIntervalSeconds $SummaryIntervalSeconds `
        -PrefetchMinChunks $PrefetchMinChunks `
        -PrefetchMaxChunks $PrefetchMaxChunks `
        -PrefetchStartupChunks $PrefetchStartupChunks `
        -ScanChunkSizeKb $ScanChunkSizeKb `
        -WinFspAutoInstallResultPath $WinFspAutoInstallResultPath `
        -WinFspAutoInstallAttempted $WinFspAutoInstallAttempted.IsPresent `
        -SkipAutoInstall $SkipWinFspAutoInstall.IsPresent `
        -SkipBackendStart $SkipBackendStart.IsPresent `
        -SkipBuild $SkipBuild.IsPresent

    Add-WinFspBinToPath

    if ((-not (Test-IsAdministrator)) -and (-not $WinFspMountElevationAttempted.IsPresent)) {
        Write-Host '      WinFSP is in experimental mode and this session is non-elevated.' -ForegroundColor DarkYellow
        Write-Host '      For diagnostic comparison only, rerun from an Administrator PowerShell session.' -ForegroundColor DarkYellow
    }
}

Write-Host ''
Write-Host ("[4/4] Starting Windows-native {0} mount..." -f $effectiveMountAdapter) -ForegroundColor Yellow
if (Test-Path -LiteralPath $stdoutPath) {
    Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
}
if (Test-Path -LiteralPath $stderrPath) {
    Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
}
if (Test-Path -LiteralPath $callbackTracePath) {
    Remove-Item -LiteralPath $callbackTracePath -Force -ErrorAction SilentlyContinue
}
if (Test-Path -LiteralPath $wrapperTracePath) {
    Remove-Item -LiteralPath $wrapperTracePath -Force -ErrorAction SilentlyContinue
}
if (Test-Path -LiteralPath $mountDiagnosticsPath) {
    Remove-Item -LiteralPath $mountDiagnosticsPath -Force -ErrorAction SilentlyContinue
}
if (Test-Path -LiteralPath $runtimeStatusPath) {
    Remove-Item -LiteralPath $runtimeStatusPath -Force -ErrorAction SilentlyContinue
}
$env:FILMUVFS_WINDOWS_TRACE_PATH = $callbackTracePath
$env:FILMUVFS_WINFSP_WRAPPER_LOG = $wrapperTracePath
$startArguments = [System.Collections.Generic.List[string]]::new()
$startArguments.Add('--mountpoint')
$startArguments.Add($runtimeMountPath)
$startArguments.Add('--mount-adapter')
$startArguments.Add($effectiveMountAdapter)
$startArguments.Add('--grpc-server')
$startArguments.Add($GrpcServer)
$startArguments.Add('--windows-projfs-summary-interval-seconds')
$startArguments.Add($SummaryIntervalSeconds.ToString())
if ($PrefetchMinChunks -gt 0) {
    $startArguments.Add('--prefetch-min-chunks')
    $startArguments.Add($PrefetchMinChunks.ToString())
}
if ($PrefetchMaxChunks -gt 0) {
    $startArguments.Add('--prefetch-max-chunks')
    $startArguments.Add($PrefetchMaxChunks.ToString())
}
if ($PrefetchStartupChunks -gt 0) {
    $startArguments.Add('--prefetch-startup-chunks')
    $startArguments.Add($PrefetchStartupChunks.ToString())
}
if ($ScanChunkSizeKb -gt 0) {
    $startArguments.Add('--chunk-size-scan-kb')
    $startArguments.Add($ScanChunkSizeKb.ToString())
}

$process = Start-Process -FilePath $binaryPath `
    -ArgumentList $startArguments `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath `
    -Environment @{
        FILMUVFS_WINDOWS_TRACE_PATH = $callbackTracePath
        FILMUVFS_WINFSP_WRAPPER_LOG = $wrapperTracePath
        FILMUVFS_RUNTIME_STATUS_PATH = $runtimeStatusPath
        FILMUVFS_RUNTIME_STATUS_INTERVAL_SECONDS = $SummaryIntervalSeconds.ToString()
    } `
    -WindowStyle Hidden `
    -PassThru

$state = [pscustomobject]@{
    pid = $process.Id
    mount_path = $MountPath
    runtime_mount_path = $runtimeMountPath
    requested_mount_adapter = $requestedMountAdapter
    mount_adapter = $effectiveMountAdapter
    binary_capabilities = $filmuvfsCapabilities
    drive_letter = $normalizedDriveLetter
    drive_mapping_kind = $driveMappingKind
    grpc_server = $GrpcServer
    summary_interval_seconds = $SummaryIntervalSeconds
    prefetch_min_chunks = $PrefetchMinChunks
    prefetch_max_chunks = $PrefetchMaxChunks
    prefetch_startup_chunks = $PrefetchStartupChunks
    scan_chunk_size_kb = $ScanChunkSizeKb
    stdout_path = $stdoutPath
    stderr_path = $stderrPath
    callback_trace_path = $callbackTracePath
    wrapper_trace_path = $wrapperTracePath
    mount_diagnostics_path = $mountDiagnosticsPath
    runtime_status_path = $runtimeStatusPath
    manager_log_path = $managerLogPath
    mount_status = 'starting'
    started_at = (Get-Date).ToString('o')
}
$state | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $statePath -Encoding UTF8

try {
    $probePath = $MountPath
    $readinessDriveLetter = ''
    if (($effectiveMountAdapter -ne 'winfsp') -and ($null -ne $normalizedDriveLetter)) {
        $readinessDriveLetter = $normalizedDriveLetter
    }
    $rootEntries = Wait-WindowsMountReady `
        -ProbePath $probePath `
        -TimeoutSeconds 45 `
        -Process $process `
        -StderrPath $stderrPath `
        -StdoutPath $stdoutPath `
        -MountAdapter $effectiveMountAdapter `
        -DriveLetter $readinessDriveLetter `
        -DiagnosticsPath $mountDiagnosticsPath
    if (($effectiveMountAdapter -ne 'winfsp') -and ($null -ne $normalizedDriveLetter)) {
        try {
            Add-DriveLetterMapping -DriveLetter $normalizedDriveLetter -MountPath $MountPath
        }
        catch {
            Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            throw
        }
    }

    $state | Add-Member -NotePropertyName root_entries -NotePropertyValue @($rootEntries) -Force
    $state.mount_status = 'ready'
    $state | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $statePath -Encoding UTF8
}
catch {
    $state.mount_status = 'failed'
    $state | Add-Member -NotePropertyName last_error -NotePropertyValue $_.Exception.Message -Force
    $state | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $statePath -Encoding UTF8
    throw
}

Write-Host ("      [OK] Windows-native {0} mount started" -f $effectiveMountAdapter) -ForegroundColor Green

Write-Host ''
Write-Host '==> [OK] FilmuCore Windows-native stack started successfully' -ForegroundColor Green
Write-Host ''
Write-Host 'Services:' -ForegroundColor Cyan
Write-Host '  Backend API:   http://localhost:8000' -ForegroundColor White
Write-Host '  API Docs:      http://localhost:8000/docs' -ForegroundColor White
Write-Host '  gRPC Catalog:  localhost:50051' -ForegroundColor White
Write-Host ''
Write-Host 'Mount:' -ForegroundColor Cyan
Write-Host ("  Windows Path:  {0}" -f $MountPath) -ForegroundColor White
if ($runtimeMountPath -ne $MountPath) {
    Write-Host ("  Runtime Path:  {0}" -f $runtimeMountPath) -ForegroundColor White
}
Write-Host ("  Requested:     {0}" -f $requestedMountAdapter) -ForegroundColor White
Write-Host ("  Adapter:       {0}" -f $effectiveMountAdapter) -ForegroundColor White
Write-Host ("  Root Entries:  {0}" -f (($rootEntries | Select-Object -First 10) -join ', ')) -ForegroundColor White
if ($null -ne $normalizedDriveLetter) {
    if ($driveMappingKind -eq 'native') {
        Write-Host ("  Drive Mount:   {0}:\" -f $normalizedDriveLetter) -ForegroundColor White
    }
    else {
        Write-Host ("  Drive Alias:   {0}:\" -f $normalizedDriveLetter) -ForegroundColor White
    }
}
Write-Host ''
Write-Host 'Library Paths:' -ForegroundColor Cyan
Write-Host ("  Movies:        {0}" -f (Join-Path $MountPath 'movies')) -ForegroundColor White
Write-Host ("  Shows:         {0}" -f (Join-Path $MountPath 'shows')) -ForegroundColor White
if ($null -ne $normalizedDriveLetter) {
    Write-Host ("  Movies Alias:  {0}:\movies" -f $normalizedDriveLetter) -ForegroundColor White
    Write-Host ("  Shows Alias:   {0}:\shows" -f $normalizedDriveLetter) -ForegroundColor White
}
Write-Host ''
Write-Host 'Management:' -ForegroundColor Cyan
Write-Host '  Status:        .\status_windows_stack.ps1' -ForegroundColor White
Write-Host '  Healthcheck:   .\check_windows_stack.ps1' -ForegroundColor White
Write-Host '  Stop:          .\stop_windows_stack.ps1' -ForegroundColor White
Write-Host ("  Stdout Log:    {0}" -f $stdoutPath) -ForegroundColor White
Write-Host ("  Stderr Log:    {0}" -f $stderrPath) -ForegroundColor White
Write-Host ("  Manager Log:   {0}" -f $managerLogPath) -ForegroundColor White

try {
    Stop-Transcript | Out-Null
}
catch {
}
















