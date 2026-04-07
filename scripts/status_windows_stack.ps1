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

$repoRoot = $PSScriptRoot
$composeFile = Join-Path $repoRoot 'docker-compose.windows.yml'
$stateDirectory = Join-Path $repoRoot 'playback-proof-artifacts\windows-native-stack'
$statePath = Join-Path $stateDirectory 'filmuvfs-windows-state.json'
$state = Read-State -StatePath $statePath

if ([string]::IsNullOrWhiteSpace($MountPath)) {
    if ($null -ne $state -and -not [string]::IsNullOrWhiteSpace([string] $state.mount_path)) {
        $MountPath = [string] $state.mount_path
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
    $requestedMountAdapter = [string]$state.requested_mount_adapter
}
$mountAdapter = $null
if ($null -ne $state -and -not [string]::IsNullOrWhiteSpace([string]$state.mount_adapter)) {
    $mountAdapter = [string]$state.mount_adapter
}
$binaryCapabilities = $null
if ($null -ne $state -and $null -ne $state.binary_capabilities) {
    $binaryCapabilities = $state.binary_capabilities
}

Write-Host '==> FilmuCore Windows-native stack status' -ForegroundColor Cyan
Write-Host ''

Write-Host '[Docker Compose]' -ForegroundColor Yellow
docker compose -f $composeFile ps postgres redis zilean-postgres zilean filmu-python arq-worker frontend prowlarr

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
if (Test-GrpcReady) {
    Write-Host '  [OK] localhost:50051 reachable' -ForegroundColor Green
}
else {
    Write-Host '  [FAIL] localhost:50051 not reachable' -ForegroundColor Red
}

Write-Host ''
Write-Host '[Windows filmuvfs process]' -ForegroundColor Yellow
if ($null -eq $state) {
    Write-Host '  [INFO] No managed state file found' -ForegroundColor Yellow
}
else {
    $managedPid = [long]$state.pid
    $process = Get-Process -Id $managedPid -ErrorAction SilentlyContinue
    Write-Host ("  State Path:    {0}" -f $statePath) -ForegroundColor White
    Write-Host ("  PID:           {0}" -f $managedPid) -ForegroundColor White
    Write-Host ("  Running:       {0}" -f ($null -ne $process)) -ForegroundColor White
    if ($null -ne $requestedMountAdapter) {
        Write-Host ("  Requested:     {0}" -f $requestedMountAdapter) -ForegroundColor White
    }
    if ($null -ne $mountAdapter) {
        Write-Host ("  Adapter:       {0}" -f $mountAdapter) -ForegroundColor White
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$state.mount_status)) {
        Write-Host ("  Mount Status:  {0}" -f ([string]$state.mount_status)) -ForegroundColor White
    }
    if ($null -ne $binaryCapabilities) {
        Write-Host ("  WinFSP Built:  {0}" -f ([bool]$binaryCapabilities.windows_winfsp_compiled)) -ForegroundColor White
    }
    Write-Host ("  Started At:    {0}" -f ([string] $state.started_at)) -ForegroundColor White
    Write-Host ("  Stdout Log:    {0}" -f ([string] $state.stdout_path)) -ForegroundColor White
    Write-Host ("  Stderr Log:    {0}" -f ([string] $state.stderr_path)) -ForegroundColor White
    if (-not [string]::IsNullOrWhiteSpace([string]$state.callback_trace_path)) {
        Write-Host ("  Callback Log:  {0}" -f ([string] $state.callback_trace_path)) -ForegroundColor White
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$state.manager_log_path)) {
        Write-Host ("  Manager Log:   {0}" -f ([string] $state.manager_log_path)) -ForegroundColor White
    }
    if ($state.PSObject.Properties.Match('last_error').Count -gt 0 -and -not [string]::IsNullOrWhiteSpace([string]$state.last_error)) {
        Write-Host ("  Last Error:    {0}" -f ([string] $state.last_error)) -ForegroundColor Yellow
    }
}

Write-Host ''
Write-Host '[Windows Runtime Prereqs]' -ForegroundColor Yellow
$winfspLibraryPath = Get-WinFspLibraryPath
if ([string]::IsNullOrWhiteSpace($winfspLibraryPath)) {
    Write-Host '  WinFSP:        not installed' -ForegroundColor White
}
else {
    Write-Host ("  WinFSP:        {0}" -f $winfspLibraryPath) -ForegroundColor White
}

Write-Host ''
Write-Host '[Windows Mount]' -ForegroundColor Yellow
Write-Host ("  Path:          {0}" -f $MountPath) -ForegroundColor White
if ($runtimeMountPath -ne $MountPath) {
    Write-Host ("  Runtime Path:  {0}" -f $runtimeMountPath) -ForegroundColor White
}
Write-Host ("  Visible:       {0}" -f (Test-Path -LiteralPath $MountPath)) -ForegroundColor White
Write-Host ("  Movies:        {0}" -f (Test-Path -LiteralPath (Join-Path $MountPath 'movies'))) -ForegroundColor White
Write-Host ("  Shows:         {0}" -f (Test-Path -LiteralPath (Join-Path $MountPath 'shows'))) -ForegroundColor White
if ($null -ne $driveLetter) {
    $driveRoot = '{0}:\' -f $driveLetter
    if ($driveMappingKind -eq 'native') {
        Write-Host ("  Drive Mount:   {0}" -f $driveRoot) -ForegroundColor White
    }
    else {
        Write-Host ("  Drive Alias:   {0}" -f $driveRoot) -ForegroundColor White
    }
    Write-Host ("  Alias Visible: {0}" -f (Test-Path -LiteralPath $driveRoot)) -ForegroundColor White
    Write-Host ("  Alias Movies:  {0}" -f (Test-Path -LiteralPath ('{0}:\movies' -f $driveLetter))) -ForegroundColor White
    Write-Host ("  Alias Shows:   {0}" -f (Test-Path -LiteralPath ('{0}:\shows' -f $driveLetter))) -ForegroundColor White
    if ($mountAdapter -eq 'winfsp') {
        $volumeName = Get-WinFspVolumeName -DriveLetter $driveLetter
        Write-Host ("  Volume Name:   {0}" -f $(if ([string]::IsNullOrWhiteSpace($volumeName)) { '<unavailable>' } else { $volumeName })) -ForegroundColor White
        Write-Host ("  Volume Probe:  {0}" -f (Test-WinFspVolumeOnline -DriveLetter $driveLetter)) -ForegroundColor White
    }
}
try {
    $entries = Get-DirectoryListing -Path $runtimeMountPath
    if ($null -eq $entries) {
        throw "directory enumeration failed"
    }
    Write-Host ("  Root Entries:  {0}" -f ($entries -join ', ')) -ForegroundColor White
}
catch {
    Write-Host ("  Root Entries:  unavailable ({0})" -f $_.Exception.Message) -ForegroundColor Yellow
}
