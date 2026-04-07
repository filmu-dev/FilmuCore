param(
    [string] $MountPath = ''
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

function Get-DirectoryListing {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    try {
        $entries = [System.IO.Directory]::GetFileSystemEntries($Path)
        if ($null -eq $entries) {
            return $null
        }
        return @($entries | ForEach-Object { [System.IO.Path]::GetFileName($_) } | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) })
    }
    catch {
        return $null
    }
}

function Get-WinFspVolumeName {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DriveLetter
    )

    try {
        $mountvolOutput = & mountvol.exe ('{0}:' -f $DriveLetter) /L 2>&1
        if ($LASTEXITCODE -ne 0) {
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
    }

    return $null
}

function Test-WinFspVolumeOnline {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DriveLetter
    )

    try {
        & fsutil.exe volume diskfree ('{0}:' -f $DriveLetter) *> $null
        return ($LASTEXITCODE -eq 0)
    }
    catch {
        return $false
    }
}

function Test-GrpcReady {
    try {
        $client = [System.Net.Sockets.TcpClient]::new()
        $async = $client.BeginConnect('127.0.0.1', 50051, $null, $null)
        if ($async.AsyncWaitHandle.WaitOne(2000) -and $client.Connected) {
            $client.EndConnect($async)
            $client.Close()
            return $true
        }
        $client.Close()
    }
    catch {
    }

    return $false
}

function Test-HttpReady {
    try {
        $response = Invoke-WebRequest -Uri 'http://localhost:8000/openapi.json' -UseBasicParsing -TimeoutSec 3
        return ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500)
    }
    catch {
        return $false
    }
}

function Get-JellyfinLogDirectory {
    $localAppData = [System.Environment]::GetEnvironmentVariable('LOCALAPPDATA')
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        return $null
    }

    return (Join-Path $localAppData 'jellyfin\log')
}

function Get-LatestJellyfinDirectStreamDiagnostic {
    param(
        [int] $LookbackMinutes = 60
    )

    $logDirectory = Get-JellyfinLogDirectory
    if ([string]::IsNullOrWhiteSpace($logDirectory) -or (-not (Test-Path -LiteralPath $logDirectory))) {
        return $null
    }

    $cutoff = (Get-Date).AddMinutes(-1 * [Math]::Abs($LookbackMinutes))
    $logFile = Get-ChildItem -LiteralPath $logDirectory -Filter 'FFmpeg.DirectStream-*.log' -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -ge $cutoff } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($null -eq $logFile) {
        return $null
    }

    $lines = @(Get-Content -LiteralPath $logFile.FullName -ErrorAction SilentlyContinue)
    if ($lines.Count -eq 0) {
        return [pscustomobject]@{
            status              = 'empty'
            log_path            = $logFile.FullName
            source_path         = $null
            cached_video_codec  = $null
            actual_video_codec  = $null
            bitstream_filter    = $null
            details             = 'directstream log file is empty'
        }
    }

    $sourcePath = $null
    $cachedVideoCodec = $null
    try {
        $metadata = $lines[0] | ConvertFrom-Json
        $sourcePath = [string] $metadata.Path
        $videoStream = @($metadata.MediaStreams | Where-Object { $_.Type -eq 1 }) | Select-Object -First 1
        if ($null -ne $videoStream) {
            $cachedVideoCodec = [string] $videoStream.Codec
        }
    }
    catch {
    }

    $actualVideoCodec = $null
    foreach ($line in $lines) {
        if ($line -match '^\s*Stream #0:\d+(?:\([^)]+\))?: Video: ([^ ,]+)') {
            $actualVideoCodec = [string] $Matches[1]
            break
        }
    }

    $bitstreamFilter = $null
    foreach ($line in $lines) {
        if ($line -match "bitstream filter '([^']+)'") {
            $bitstreamFilter = [string] $Matches[1]
            break
        }
    }

    $errorLines = @($lines | Where-Object {
        $_ -match 'Error initializing bitstream filter' -or
        $_ -match 'Error opening output file' -or
        $_ -match 'Error opening output files' -or
        $_ -match 'Conversion failed' -or
        $_ -match 'Invalid argument'
    })

    $status = 'ok'
    $details = 'recent directstream log shows no terminal ffmpeg error'
    if ($errorLines.Count -gt 0) {
        $status = 'failed'
        $details = ($errorLines | Select-Object -Last 1)
    }

    if ((-not [string]::IsNullOrWhiteSpace($cachedVideoCodec)) -and (-not [string]::IsNullOrWhiteSpace($actualVideoCodec)) -and ($cachedVideoCodec.ToLowerInvariant() -ne $actualVideoCodec.ToLowerInvariant())) {
        $status = 'metadata_mismatch'
        $details = "Jellyfin cached codec '$cachedVideoCodec' but ffmpeg saw '$actualVideoCodec'"
    }
    elseif (($status -eq 'failed') -and (-not [string]::IsNullOrWhiteSpace($bitstreamFilter))) {
        $details = "DirectStream failed in ffmpeg with bitstream filter '$bitstreamFilter': $details"
    }

    return [pscustomobject]@{
        status              = $status
        log_path            = $logFile.FullName
        source_path         = $sourcePath
        cached_video_codec  = $cachedVideoCodec
        actual_video_codec  = $actualVideoCodec
        bitstream_filter    = $bitstreamFilter
        details             = $details
    }
}

$repoRoot = $PSScriptRoot
$stateDirectory = Join-Path $repoRoot 'playback-proof-artifacts\windows-native-stack'
$statePath = Join-Path $stateDirectory 'filmuvfs-windows-state.json'
$state = Read-State -StatePath $statePath

if ([string]::IsNullOrWhiteSpace($MountPath)) {
    if ($null -ne $state -and -not [string]::IsNullOrWhiteSpace([string]$state.mount_path)) {
        $MountPath = [string]$state.mount_path
    }
    else {
        $MountPath = Get-DefaultMountPath
    }
}
$MountPath = [System.IO.Path]::GetFullPath($MountPath)

$driveLetter = $null
if ($null -ne $state -and -not [string]::IsNullOrWhiteSpace([string]$state.drive_letter)) {
    $driveLetter = Normalize-DriveLetter -DriveLetter ([string]$state.drive_letter)
}
$runtimeMountPath = $MountPath
if ($null -ne $state -and $state.PSObject.Properties.Match('runtime_mount_path').Count -gt 0 -and -not [string]::IsNullOrWhiteSpace([string]$state.runtime_mount_path)) {
    $runtimeMountPath = [string]$state.runtime_mount_path
}
$driveMappingKind = $null
if ($null -ne $state -and $state.PSObject.Properties.Match('drive_mapping_kind').Count -gt 0 -and -not [string]::IsNullOrWhiteSpace([string]$state.drive_mapping_kind)) {
    $driveMappingKind = [string]$state.drive_mapping_kind
}
$requestedMountAdapter = $null
if ($null -ne $state -and -not [string]::IsNullOrWhiteSpace([string]$state.requested_mount_adapter)) {
    $requestedMountAdapter = ([string]$state.requested_mount_adapter).Trim().ToLowerInvariant()
}
$mountAdapter = $null
if ($null -ne $state -and -not [string]::IsNullOrWhiteSpace([string]$state.mount_adapter)) {
    $mountAdapter = ([string]$state.mount_adapter).Trim().ToLowerInvariant()
}
$binaryCapabilities = $null
if ($null -ne $state -and $null -ne $state.binary_capabilities) {
    $binaryCapabilities = $state.binary_capabilities
}

$failures = 0

function Assert-Check {
    param(
        [Parameter(Mandatory = $true)]
        [bool] $Condition,
        [Parameter(Mandatory = $true)]
        [string] $Label,
        [Parameter(Mandatory = $true)]
        [string] $SuccessMessage,
        [Parameter(Mandatory = $true)]
        [string] $FailureMessage
    )

    if ($Condition) {
        Write-Host ("[OK] {0}: {1}" -f $Label, $SuccessMessage) -ForegroundColor Green
    }
    else {
        Write-Host ("[FAIL] {0}: {1}" -f $Label, $FailureMessage) -ForegroundColor Red
        $script:failures++
    }
}

Write-Host '==> FilmuCore Windows-native stack healthcheck' -ForegroundColor Cyan
Write-Host ''

Assert-Check -Condition (Test-HttpReady) `
    -Label 'Backend API' `
    -SuccessMessage 'http://localhost:8000/openapi.json reachable' `
    -FailureMessage 'http://localhost:8000/openapi.json unreachable'

if ($mountAdapter -in @($null, 'auto', 'projfs')) {
    Assert-Check -Condition (Test-ProjFsAvailable) `
        -Label 'Projected File System' `
        -SuccessMessage ("projectedfslib.dll present at {0}" -f (Get-ProjFsLibraryPath)) `
        -FailureMessage 'Client-ProjFS is not enabled on this Windows host'
}
elseif ($mountAdapter -eq 'winfsp') {
    Write-Host '[INFO] Projected File System: skipped for selected winfsp adapter' -ForegroundColor Yellow
    Assert-Check -Condition (Test-WinFspAvailable) `
        -Label 'WinFSP Runtime' `
        -SuccessMessage ("WinFSP runtime present at {0}" -f (Get-WinFspLibraryPath)) `
        -FailureMessage 'WinFSP runtime is not installed on this Windows host'
}

Assert-Check -Condition (Test-GrpcReady) `
    -Label 'gRPC Catalog' `
    -SuccessMessage 'localhost:50051 reachable' `
    -FailureMessage 'localhost:50051 unreachable'

Assert-Check -Condition ($null -ne $state) `
    -Label 'Managed State' `
    -SuccessMessage ("state file present at {0}" -f $statePath) `
    -FailureMessage 'managed Windows state file not found'

if ($null -ne $state) {
    $managedPid = [long]$state.pid
    $process = Get-Process -Id $managedPid -ErrorAction SilentlyContinue
    if ($null -ne $requestedMountAdapter) {
        Write-Host ("[INFO] Requested Adapter: {0}" -f $requestedMountAdapter) -ForegroundColor Yellow
    }
    if ($null -ne $mountAdapter) {
        Write-Host ("[INFO] Effective Adapter: {0}" -f $mountAdapter) -ForegroundColor Yellow
    }
    if ($null -ne $binaryCapabilities) {
        Write-Host ("[INFO] Binary WinFSP Backend: {0}" -f ([bool]$binaryCapabilities.windows_winfsp_compiled)) -ForegroundColor Yellow
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$state.manager_log_path)) {
        Write-Host ("[INFO] Manager Log: {0}" -f ([string]$state.manager_log_path)) -ForegroundColor Yellow
    }
    Assert-Check -Condition ($null -ne $process) `
        -Label 'Windows filmuvfs process' `
        -SuccessMessage ("pid {0} is running" -f $managedPid) `
        -FailureMessage ("pid {0} is not running" -f $managedPid)
}

Assert-Check -Condition (Test-Path -LiteralPath $MountPath) `
    -Label 'Mount Root' `
    -SuccessMessage ("mount path visible at {0}" -f $MountPath) `
    -FailureMessage ("mount path missing at {0}" -f $MountPath)

Assert-Check -Condition (Test-Path -LiteralPath $runtimeMountPath) `
    -Label 'Runtime Mount' `
    -SuccessMessage ("runtime mount visible at {0}" -f $runtimeMountPath) `
    -FailureMessage ("runtime mount missing at {0}" -f $runtimeMountPath)

$runtimeEntries = Get-DirectoryListing -Path $runtimeMountPath
Assert-Check -Condition ($null -ne $runtimeEntries -and ($runtimeEntries -contains 'movies')) `
    -Label 'Movies Library' `
    -SuccessMessage 'movies directory visible' `
    -FailureMessage 'movies directory missing'

Assert-Check -Condition ($null -ne $runtimeEntries -and ($runtimeEntries -contains 'shows')) `
    -Label 'Shows Library' `
    -SuccessMessage 'shows directory visible' `
    -FailureMessage 'shows directory missing'

if ($null -ne $driveLetter) {
    $driveRoot = '{0}:\' -f $driveLetter
    Assert-Check -Condition (Test-Path -LiteralPath $driveRoot) `
        -Label 'Drive Root' `
        -SuccessMessage ("drive root visible at {0}" -f $driveRoot) `
        -FailureMessage ("drive root missing at {0}" -f $driveRoot)

    if ($driveMappingKind -eq 'subst') {
        Assert-Check -Condition (Test-Path -LiteralPath ('{0}:\movies' -f $driveLetter)) `
            -Label 'Drive Movies' `
            -SuccessMessage 'drive movies path visible' `
            -FailureMessage 'drive movies path missing'

        Assert-Check -Condition (Test-Path -LiteralPath ('{0}:\shows' -f $driveLetter)) `
            -Label 'Drive Shows' `
            -SuccessMessage 'drive shows path visible' `
            -FailureMessage 'drive shows path missing'
    }

    if ($mountAdapter -eq 'winfsp') {
        $volumeName = Get-WinFspVolumeName -DriveLetter $driveLetter
        Assert-Check -Condition (-not [string]::IsNullOrWhiteSpace($volumeName)) `
            -Label 'WinFSP Volume Name' `
            -SuccessMessage ("mountvol reports {0}" -f $volumeName) `
            -FailureMessage ("mountvol could not resolve a volume GUID for {0}:" -f $driveLetter)

        Assert-Check -Condition (Test-WinFspVolumeOnline -DriveLetter $driveLetter) `
            -Label 'WinFSP Volume Probe' `
            -SuccessMessage ("fsutil volume diskfree succeeded for {0}:" -f $driveLetter) `
            -FailureMessage ("fsutil volume diskfree failed for {0}:" -f $driveLetter)
    }
}
else {
    Write-Host '[INFO] Drive Alias: no managed drive-letter alias configured' -ForegroundColor Yellow
}

$jellyfinDirectStream = Get-LatestJellyfinDirectStreamDiagnostic
if ($null -eq $jellyfinDirectStream) {
    Write-Host '[INFO] Jellyfin DirectStream: no recent FFmpeg.DirectStream log found' -ForegroundColor Yellow
}
else {
    switch ([string] $jellyfinDirectStream.status) {
        'ok' {
            Write-Host ("[INFO] Jellyfin DirectStream: latest log is clean ({0})" -f $jellyfinDirectStream.log_path) -ForegroundColor Yellow
        }
        'metadata_mismatch' {
            Write-Host ("[WARN] Jellyfin DirectStream: {0}; source={1}; log={2}" -f $jellyfinDirectStream.details, $jellyfinDirectStream.source_path, $jellyfinDirectStream.log_path) -ForegroundColor Yellow
        }
        'failed' {
            Write-Host ("[WARN] Jellyfin DirectStream: {0}; source={1}; log={2}" -f $jellyfinDirectStream.details, $jellyfinDirectStream.source_path, $jellyfinDirectStream.log_path) -ForegroundColor Yellow
        }
        default {
            Write-Host ("[INFO] Jellyfin DirectStream: {0}; log={1}" -f $jellyfinDirectStream.details, $jellyfinDirectStream.log_path) -ForegroundColor Yellow
        }
    }
}

Write-Host ''
if ($failures -gt 0) {
    Write-Host ("==> Windows-native stack healthcheck failed with {0} issue(s)" -f $failures) -ForegroundColor Red
    exit 1
}

Write-Host '==> Windows-native stack healthcheck passed' -ForegroundColor Green

