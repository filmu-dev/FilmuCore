param(
    [string] $MountPath = '',
    [string] $TargetFile = '',
    [string] $RemuxTargetFile = '',
    [string] $FfmpegPath = '',
    [string] $BackendUrl = 'http://localhost:8000',
    [string] $ApiKey = '',
    [ValidateSet('custom', 'continuous', 'seek', 'concurrent', 'full')]
    [string] $SoakProfile = 'custom',
    [int] $SequentialMinutes = 10,
    [int] $SeekIterations = 24,
    [int] $SeekMinutes = 0,
    [int] $ConcurrentReaders = 3,
    [int] $ConcurrentIterations = 48,
    [int] $ConcurrentMinutes = 0,
    [int] $SequentialBlockSizeKb = 1024,
    [int] $SeekReadSizeKb = 512,
    [int] $ConcurrentBlockSizeKb = 512,
    [int] $RemuxSeconds = 30,
    [int] $RemuxSeekSeconds = 120,
    [int] $RemuxTimeoutSeconds = 180,
    [switch] $SkipSequential,
    [switch] $SkipSeek,
    [switch] $SkipConcurrent,
    [switch] $SkipRemux,
    [switch] $RequireRemux,
    [switch] $RequireFilmuvfs,
    [int] $MaxReconnectIncidents = -1,
    [int] $MaxUnrecoveredStaleRefreshIncidents = -1,
    [int] $MaxCacheColdFetchIncidents = -1,
    [double] $MinCacheHitRatio = -1,
    [int] $MaxProviderPressureIncidents = -1,
    [int] $MaxFatalErrorIncidents = -1
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Get-DefaultMountPath {
    $systemDrive = [System.Environment]::GetEnvironmentVariable('SystemDrive')
    if ([string]::IsNullOrWhiteSpace($systemDrive)) {
        $systemDrive = 'C:'
    }
    Join-Path $systemDrive 'FilmuCoreVFS'
}

function Get-StateDirectory {
    $repoRoot = Split-Path -Parent $PSScriptRoot
    Join-Path $repoRoot 'playback-proof-artifacts\windows-native-stack'
}

function Get-StatePath {
    Join-Path (Get-StateDirectory) 'filmuvfs-windows-state.json'
}

function Resolve-BackendApiKey {
    param([string] $ExplicitKey)

    if (-not [string]::IsNullOrWhiteSpace($ExplicitKey)) {
        return $ExplicitKey
    }
    $envKey = [System.Environment]::GetEnvironmentVariable('FILMU_PY_API_KEY')
    if (-not [string]::IsNullOrWhiteSpace($envKey)) {
        return $envKey
    }
    return $null
}

function Get-DotEnvMap {
    param([Parameter(Mandatory = $true)][string] $Path)

    $map = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $map
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#')) {
            continue
        }
        $index = $trimmed.IndexOf('=')
        if ($index -lt 1) {
            continue
        }
        $map[$trimmed.Substring(0, $index).Trim()] = $trimmed.Substring($index + 1)
    }

    return $map
}

function Get-JsonObjectFromString {
    param([string] $Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $null
    }

    try {
        return $Value | ConvertFrom-Json -Depth 12
    }
    catch {
        throw ("[windows-vfs-soak] Invalid JSON configuration payload: {0}" -f $_.Exception.Message)
    }
}

function ConvertTo-Base64Url {
    param([byte[]] $Bytes)

    $encoded = [Convert]::ToBase64String($Bytes)
    return $encoded.TrimEnd('=').Replace('+', '-').Replace('/', '_')
}

function ConvertFrom-Base64Url {
    param([string] $Value)

    $normalized = $Value.Replace('-', '+').Replace('_', '/')
    $padding = $normalized.Length % 4
    if ($padding -ne 0) {
        $normalized += ('=' * (4 - $padding))
    }
    return [Convert]::FromBase64String($normalized)
}

function New-Hs256Jwt {
    param(
        [string] $Issuer,
        [string] $Audience,
        [byte[]] $SymmetricKeyBytes,
        [string] $KeyId = 'local-vfs-soak',
        [string] $Subject
    )

    $now = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $headerJson = [ordered]@{
        alg = 'HS256'
        kid = $KeyId
        typ = 'JWT'
    } | ConvertTo-Json -Compress
    $payloadJson = [ordered]@{
        iss = $Issuer
        sub = $Subject
        aud = $Audience
        exp = $now + 3600
        iat = $now
        tenant_id = 'global'
        actor_type = 'service'
        authorized_tenants = @('global')
        roles = @('platform:admin')
        scope = 'library:write playback:operate settings:write security:policy.approve'
    } | ConvertTo-Json -Compress -Depth 8

    $headerSegment = ConvertTo-Base64Url -Bytes ([Text.Encoding]::UTF8.GetBytes($headerJson))
    $payloadSegment = ConvertTo-Base64Url -Bytes ([Text.Encoding]::UTF8.GetBytes($payloadJson))
    $signingInput = '{0}.{1}' -f $headerSegment, $payloadSegment
    $hmac = [System.Security.Cryptography.HMACSHA256]::new($SymmetricKeyBytes)
    try {
        $signatureBytes = $hmac.ComputeHash([Text.Encoding]::ASCII.GetBytes($signingInput))
    }
    finally {
        $hmac.Dispose()
    }
    $signatureSegment = ConvertTo-Base64Url -Bytes $signatureBytes
    return '{0}.{1}' -f $signingInput, $signatureSegment
}

function Resolve-BackendHeaders {
    param(
        [string] $ApiKey,
        [string] $RepoRoot
    )

    $dotEnv = Get-DotEnvMap -Path (Join-Path $RepoRoot '.env')
    if ($dotEnv.ContainsKey('FILMU_PY_OIDC')) {
        $oidcConfig = Get-JsonObjectFromString -Value ([string] $dotEnv['FILMU_PY_OIDC'])
        if ($null -ne $oidcConfig -and [bool] $oidcConfig.enabled -and -not [bool] $oidcConfig.allow_api_key_fallback) {
            $octKey = @($oidcConfig.jwks_json.keys) | Where-Object {
                $_.kty -eq 'oct' -and -not [string]::IsNullOrWhiteSpace([string] $_.k)
            } | Select-Object -First 1
            if ($null -eq $octKey) {
                throw '[windows-vfs-soak] FILMU_PY_OIDC requires one oct JWKS key for local bearer-token traffic.'
            }
            $jwt = New-Hs256Jwt `
                -Issuer ([string] $oidcConfig.issuer) `
                -Audience ([string] $oidcConfig.audience) `
                -SymmetricKeyBytes (ConvertFrom-Base64Url -Value ([string] $octKey.k)) `
                -KeyId ([string] $octKey.kid) `
                -Subject 'ops://windows-vfs-soak'
            return @{ authorization = "Bearer $jwt" }
        }
    }

    if ([string]::IsNullOrWhiteSpace($ApiKey)) {
        return @{}
    }
    return @{ 'x-api-key' = $ApiKey }
}

function Resolve-FfmpegPath {
    param([string] $ExplicitPath)

    $candidates = [System.Collections.Generic.List[string]]::new()
    if (-not [string]::IsNullOrWhiteSpace($ExplicitPath)) {
        $candidates.Add($ExplicitPath)
    }
    foreach ($envName in @('FILMU_WINDOWS_FFMPEG_PATH', 'JELLYFIN_FFMPEG_PATH')) {
        $value = [System.Environment]::GetEnvironmentVariable($envName)
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            $candidates.Add($value)
        }
    }
    foreach ($path in @(
        'E:\jellyfin\ffmpeg.exe',
        'C:\Program Files\Jellyfin\Server\ffmpeg.exe',
        'C:\Program Files\Jellyfin\ffmpeg.exe',
        'C:\Program Files\ffmpeg\bin\ffmpeg.exe'
    )) {
        $candidates.Add($path)
    }

    foreach ($candidate in $candidates) {
        if ([string]::IsNullOrWhiteSpace($candidate)) {
            continue
        }
        if (Test-Path -LiteralPath $candidate) {
            return [System.IO.Path]::GetFullPath($candidate)
        }
    }

    $whereResult = Get-Command ffmpeg.exe -ErrorAction SilentlyContinue
    if ($null -ne $whereResult -and -not [string]::IsNullOrWhiteSpace($whereResult.Source)) {
        return [System.IO.Path]::GetFullPath($whereResult.Source)
    }

    return $null
}

function Apply-SoakProfile {
    param([string] $Profile)

    switch ($Profile) {
        'continuous' {
            $script:SequentialMinutes = 60
            $script:SeekMinutes = 0
            $script:ConcurrentMinutes = 0
            $script:SkipSeek = $true
            $script:SkipConcurrent = $true
            $script:SkipRemux = $true
            if ($script:MaxReconnectIncidents -lt 0) { $script:MaxReconnectIncidents = 1 }
            if ($script:MaxUnrecoveredStaleRefreshIncidents -lt 0) { $script:MaxUnrecoveredStaleRefreshIncidents = 0 }
            if ($script:MaxFatalErrorIncidents -lt 0) { $script:MaxFatalErrorIncidents = 0 }
        }
        'seek' {
            $script:SeekMinutes = 15
            $script:SkipSequential = $true
            $script:SkipConcurrent = $true
            $script:SkipRemux = $true
            if ($script:MaxReconnectIncidents -lt 0) { $script:MaxReconnectIncidents = 1 }
            if ($script:MaxUnrecoveredStaleRefreshIncidents -lt 0) { $script:MaxUnrecoveredStaleRefreshIncidents = 0 }
            if ($script:MaxFatalErrorIncidents -lt 0) { $script:MaxFatalErrorIncidents = 0 }
        }
        'concurrent' {
            $script:ConcurrentReaders = [Math]::Max($script:ConcurrentReaders, 3)
            $script:ConcurrentMinutes = 15
            $script:SkipSequential = $true
            $script:SkipSeek = $true
            $script:SkipRemux = $true
            if ($script:MaxReconnectIncidents -lt 0) { $script:MaxReconnectIncidents = 1 }
            if ($script:MinCacheHitRatio -lt 0) { $script:MinCacheHitRatio = 0.65 }
            if ($script:MaxProviderPressureIncidents -lt 0) { $script:MaxProviderPressureIncidents = 0 }
            if ($script:MaxFatalErrorIncidents -lt 0) { $script:MaxFatalErrorIncidents = 0 }
        }
        'full' {
            $script:SequentialMinutes = 60
            $script:SeekMinutes = 15
            $script:ConcurrentReaders = [Math]::Max($script:ConcurrentReaders, 3)
            $script:ConcurrentMinutes = 15
            $script:SkipRemux = $true
            if ($script:MaxReconnectIncidents -lt 0) { $script:MaxReconnectIncidents = 1 }
            if ($script:MaxUnrecoveredStaleRefreshIncidents -lt 0) { $script:MaxUnrecoveredStaleRefreshIncidents = 0 }
            if ($script:MinCacheHitRatio -lt 0) { $script:MinCacheHitRatio = 0.80 }
            if ($script:MaxProviderPressureIncidents -lt 0) { $script:MaxProviderPressureIncidents = 0 }
            if ($script:MaxFatalErrorIncidents -lt 0) { $script:MaxFatalErrorIncidents = 0 }
        }
    }
}

function Get-SeekTimestamp {
    param([int] $TotalSeconds)

    $span = [TimeSpan]::FromSeconds([Math]::Max($TotalSeconds, 0))
    return ('{0:00}:{1:00}:{2:00}' -f [int]$span.Hours, [int]$span.Minutes, [int]$span.Seconds)
}

function Get-LogPath {
    param([Parameter(Mandatory = $true)][string] $Name)
    Join-Path (Get-StateDirectory) $Name
}

function Read-State {
    $statePath = Get-StatePath
    if (-not (Test-Path -LiteralPath $statePath)) {
        return $null
    }
    Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
}

function Get-FilmuvfsState {
    param([string] $MountPath)

    $state = Read-State
    $process = Get-Process -Name filmuvfs -ErrorAction SilentlyContinue | Select-Object -First 1
    [pscustomobject]@{
        timestamp = (Get-Date).ToString('o')
        mount_path = $MountPath
        mount_exists = Test-Path -LiteralPath $MountPath
        pid = if ($null -ne $process) { [int]$process.Id } else { $null }
        running = ($null -ne $process)
        state_mount_status = if ($null -ne $state -and $state.PSObject.Properties.Match('mount_status').Count -gt 0) { [string]$state.mount_status } else { $null }
        state_mount_adapter = if ($null -ne $state -and $state.PSObject.Properties.Match('mount_adapter').Count -gt 0) { [string]$state.mount_adapter } else { $null }
        state_runtime_status_path = if ($null -ne $state -and $state.PSObject.Properties.Match('runtime_status_path').Count -gt 0) { [string]$state.runtime_status_path } else { $null }
    }
}

function Assert-LiveMount {
    param(
        [Parameter(Mandatory = $true)][string] $MountPath,
        [switch] $RequireFilmuvfs
    )

    $state = Get-FilmuvfsState -MountPath $MountPath
    if (-not $state.mount_exists) {
        throw ("Mounted path is not live: {0}" -f $MountPath)
    }
    if ($RequireFilmuvfs -and -not $state.running) {
        throw 'filmuvfs is not running.'
    }
    return $state
}

function Resolve-TargetFiles {
    param(
        [Parameter(Mandatory = $true)][string] $MountPath,
        [string] $ExplicitFile,
        [int] $DesiredCount = 3
    )

    if (-not [string]::IsNullOrWhiteSpace($ExplicitFile)) {
        if (-not (Test-Path -LiteralPath $ExplicitFile)) {
            throw ("TargetFile does not exist: {0}" -f $ExplicitFile)
        }
        return ,([System.IO.Path]::GetFullPath($ExplicitFile))
    }

    $moviesRoot = Join-Path $MountPath 'movies'
    if (-not (Test-Path -LiteralPath $moviesRoot)) {
        throw ("Movies root not found under mount: {0}" -f $moviesRoot)
    }

    $files = Get-ChildItem -LiteralPath $moviesRoot -File -Recurse -ErrorAction Stop |
        Where-Object { @('.mkv', '.mp4', '.avi', '.m4v', '.ts') -contains $_.Extension.ToLowerInvariant() } |
        Sort-Object @(
            @{ Expression = 'Length'; Descending = $true },
            @{ Expression = 'FullName'; Descending = $false }
        ) |
        Select-Object -First ([Math]::Max($DesiredCount, 1))

    if ($null -eq $files -or $files.Count -eq 0) {
        throw ("No media files found under {0}" -f $moviesRoot)
    }

    return @($files | ForEach-Object { $_.FullName })
}

function Resolve-RemuxTargetFile {
    param(
        [Parameter(Mandatory = $true)][string] $MountPath,
        [string] $ExplicitFile
    )

    if (-not [string]::IsNullOrWhiteSpace($ExplicitFile)) {
        if (-not (Test-Path -LiteralPath $ExplicitFile)) {
            throw ("RemuxTargetFile does not exist: {0}" -f $ExplicitFile)
        }
        return [System.IO.Path]::GetFullPath($ExplicitFile)
    }

    $moviesRoot = Join-Path $MountPath 'movies'
    $files = @(Get-ChildItem -LiteralPath $moviesRoot -File -Recurse -ErrorAction Stop |
        Where-Object { @('.mkv', '.mp4', '.m4v') -contains $_.Extension.ToLowerInvariant() } |
        Sort-Object @(
            @{ Expression = 'Length'; Descending = $false },
            @{ Expression = 'FullName'; Descending = $false }
        ))

    if ($files.Count -eq 0) {
        throw ("No remux candidates found under {0}" -f $moviesRoot)
    }

    $preferred = @($files | Where-Object {
        $_.Name -notmatch '(?i)remux|2160p|uhd|bluray|bdrip|dv|hdr'
    })

    if ($preferred.Count -gt 0) {
        return $preferred[0].FullName
    }

    return $files[0].FullName
}

function Invoke-FfmpegCommand {
    param(
        [Parameter(Mandatory = $true)][string] $ExecutablePath,
        [Parameter(Mandatory = $true)][string[]] $Arguments,
        [Parameter(Mandatory = $true)][string] $StdoutPath,
        [Parameter(Mandatory = $true)][string] $StderrPath,
        [Parameter(Mandatory = $true)][int] $TimeoutSeconds
    )

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $ExecutablePath
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($argument in $Arguments) {
        [void]$startInfo.ArgumentList.Add([string]$argument)
    }

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start()) {
        throw ("Failed to start ffmpeg process: {0}" -f $ExecutablePath)
    }

    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $timedOut = -not $process.WaitForExit([Math]::Max($TimeoutSeconds, 1) * 1000)
    if ($timedOut) {
        try {
            $process.Kill($true)
        }
        catch {
            try {
                $process.Kill()
            }
            catch {
            }
        }
        $null = $process.WaitForExit(5000)
    }
    else {
        $process.WaitForExit()
    }

    $stdout = $stdoutTask.GetAwaiter().GetResult()
    $stderr = $stderrTask.GetAwaiter().GetResult()
    Set-Content -Path $StdoutPath -Value $stdout -Encoding UTF8
    Set-Content -Path $StderrPath -Value $stderr -Encoding UTF8

    [pscustomobject]@{
        exit_code = if ($timedOut) { $null } else { [int]$process.ExitCode }
        timed_out = [bool]$timedOut
        pid = [int]$process.Id
    }
}

function Invoke-SequentialScenario {
    param(
        [Parameter(Mandatory = $true)][string] $FilePath,
        [Parameter(Mandatory = $true)][TimeSpan] $Duration,
        [Parameter(Mandatory = $true)][int] $BlockSizeBytes,
        [Parameter(Mandatory = $true)][string] $MountPath,
        [switch] $RequireFilmuvfs
    )

    $buffer = New-Object byte[] $BlockSizeBytes
    $stream = [System.IO.File]::Open($FilePath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    $startedAt = Get-Date
    $bytesRead = [int64]0
    $rewinds = 0
    $reads = 0
    try {
        while (((Get-Date) - $startedAt) -lt $Duration) {
            $read = $stream.Read($buffer, 0, $buffer.Length)
            if ($read -le 0) {
                $null = $stream.Seek(0, [System.IO.SeekOrigin]::Begin)
                $rewinds++
                continue
            }
            $bytesRead += $read
            $reads++
            if (($reads % 128) -eq 0) {
                $null = Assert-LiveMount -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs
            }
        }
    }
    finally {
        $stream.Dispose()
    }

    $elapsedSeconds = ((Get-Date) - $startedAt).TotalSeconds
    [pscustomobject]@{
        scenario = 'sequential'
        file_path = $FilePath
        duration_seconds = [math]::Round($elapsedSeconds, 3)
        bytes_read = $bytesRead
        rewinds = $rewinds
        reads = $reads
        throughput_mib_per_sec = if ($elapsedSeconds -gt 0) {
            [math]::Round(($bytesRead / 1MB) / $elapsedSeconds, 3)
        } else {
            0
        }
        status = 'ok'
    }
}

function Invoke-SeekScenario {
    param(
        [Parameter(Mandatory = $true)][string] $FilePath,
        [int] $Iterations,
        [TimeSpan] $Duration = [TimeSpan]::Zero,
        [Parameter(Mandatory = $true)][int] $ReadSizeBytes,
        [Parameter(Mandatory = $true)][string] $MountPath,
        [switch] $RequireFilmuvfs
    )

    $stream = [System.IO.File]::Open($FilePath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    $buffer = New-Object byte[] $ReadSizeBytes
    $patterns = @(0.0, 0.05, 0.95, 0.25, 0.75, 0.5, 0.10, 0.90)
    $reads = 0
    $bytesRead = [int64]0
    $startedAt = Get-Date
    try {
        $maxOffset = [Math]::Max([int64]0, $stream.Length - $ReadSizeBytes)
        $index = 0
        while ($true) {
            if ($Duration -gt [TimeSpan]::Zero) {
                if (((Get-Date) - $startedAt) -ge $Duration) {
                    break
                }
            }
            elseif ($index -ge $Iterations) {
                break
            }
            $ratio = $patterns[$index % $patterns.Count]
            $offset = [int64][Math]::Floor($maxOffset * $ratio)
            $null = $stream.Seek($offset, [System.IO.SeekOrigin]::Begin)
            $read = $stream.Read($buffer, 0, $buffer.Length)
            if ($read -le 0) {
                throw ("Seek scenario read returned {0} at offset {1}" -f $read, $offset)
            }
            $bytesRead += $read
            $reads++
            $null = Assert-LiveMount -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs
            $index++
        }
    }
    finally {
        $stream.Dispose()
    }

    [pscustomobject]@{
        scenario = 'seek_resume'
        file_path = $FilePath
        iterations = $reads
        target_iterations = if ($Duration -gt [TimeSpan]::Zero) { $null } else { $Iterations }
        duration_target_seconds = if ($Duration -gt [TimeSpan]::Zero) { [math]::Round($Duration.TotalSeconds, 3) } else { $null }
        reads = $reads
        bytes_read = $bytesRead
        duration_seconds = [math]::Round(((Get-Date) - $startedAt).TotalSeconds, 3)
        status = 'ok'
    }
}

function Invoke-ConcurrentScenario {
    param(
        [Parameter(Mandatory = $true)][string[]] $FilePaths,
        [Parameter(Mandatory = $true)][int] $Readers,
        [int] $Iterations,
        [int] $DurationSeconds = 0,
        [Parameter(Mandatory = $true)][int] $BlockSizeBytes,
        [Parameter(Mandatory = $true)][string] $MountPath,
        [switch] $RequireFilmuvfs
    )

    $startedAt = Get-Date
    $jobScript = {
        param($Path, $Iterations, $DurationSeconds, $BlockSizeBytes, $ReaderIndex)
        $buffer = New-Object byte[] $BlockSizeBytes
        $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
        $bytesRead = [int64]0
        $reads = 0
        $maxOffset = [Math]::Max([int64]0, $stream.Length - $BlockSizeBytes)
        $startedAt = Get-Date
        try {
            $i = 0
            while ($true) {
                if ($DurationSeconds -gt 0) {
                    if (((Get-Date) - $startedAt).TotalSeconds -ge $DurationSeconds) {
                        break
                    }
                }
                elseif ($i -ge $Iterations) {
                    break
                }
                $offset = if ($maxOffset -le 0) {
                    0
                } else {
                    [int64](($i * ($ReaderIndex + 1) * $BlockSizeBytes) % $maxOffset)
                }
                $null = $stream.Seek($offset, [System.IO.SeekOrigin]::Begin)
                $read = $stream.Read($buffer, 0, $buffer.Length)
                if ($read -le 0) {
                    throw ("Concurrent reader {0} received {1} bytes at iteration {2}" -f $ReaderIndex, $read, $i)
                }
                $bytesRead += $read
                $reads++
                $i++
            }
            [pscustomobject]@{
                reader = $ReaderIndex
                file_path = $Path
                reads = $reads
                bytes_read = $bytesRead
                duration_seconds = [math]::Round(((Get-Date) - $startedAt).TotalSeconds, 3)
                status = 'ok'
            }
        }
        finally {
            $stream.Dispose()
        }
    }

    $jobs = @()
    for ($index = 0; $index -lt $Readers; $index++) {
        $filePath = $FilePaths[$index % $FilePaths.Count]
        $jobs += Start-Job -ScriptBlock $jobScript -ArgumentList $filePath, $Iterations, $DurationSeconds, $BlockSizeBytes, $index
    }

    try {
        $waitTimeoutSeconds = if ($DurationSeconds -gt 0) {
            [Math]::Max($DurationSeconds + 180, 600)
        }
        else {
            600
        }
        $null = Wait-Job -Job $jobs -Timeout $waitTimeoutSeconds
        $results = @()
        foreach ($job in $jobs) {
            if ($job.State -ne 'Completed') {
                throw ("Concurrent reader job {0} did not complete (state={1})" -f $job.Id, $job.State)
            }
            $results += Receive-Job -Job $job -ErrorAction Stop
        }
        $null = Assert-LiveMount -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs
        [pscustomobject]@{
            scenario = 'concurrent_readers'
            readers = $Readers
            iterations = if ($DurationSeconds -gt 0) { $null } else { $Iterations }
            duration_target_seconds = if ($DurationSeconds -gt 0) { $DurationSeconds } else { $null }
            duration_seconds = [math]::Round(((Get-Date) - $startedAt).TotalSeconds, 3)
            total_bytes_read = @($results | Measure-Object -Property bytes_read -Sum).Sum
            total_reads = @($results | Measure-Object -Property reads -Sum).Sum
            readers_detail = $results
            status = 'ok'
        }
    }
    finally {
        $jobs | Remove-Job -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-RemuxScenario {
    param(
        [Parameter(Mandatory = $true)][string] $FfmpegPath,
        [Parameter(Mandatory = $true)][string] $FilePath,
        [Parameter(Mandatory = $true)][int] $DurationSeconds,
        [Parameter(Mandatory = $true)][int] $SeekSeconds,
        [Parameter(Mandatory = $true)][int] $TimeoutSeconds,
        [Parameter(Mandatory = $true)][string] $ArtifactRoot,
        [Parameter(Mandatory = $true)][string] $MountPath,
        [switch] $RequireFilmuvfs
    )

    $invocations = [System.Collections.Generic.List[object]]::new()
    $startedAt = Get-Date
    $targets = @(
        [pscustomobject]@{
            name = 'start'
            output = (Join-Path $ArtifactRoot 'ffmpeg-remux-start.mkv')
            stderr = (Join-Path $ArtifactRoot 'ffmpeg-remux-start.stderr.log')
            stdout = (Join-Path $ArtifactRoot 'ffmpeg-remux-start.stdout.log')
            arguments = @(
                '-v', 'error',
                '-nostdin',
                '-y',
                '-i', $FilePath,
                '-ignore_unknown',
                '-map', '0:v:0',
                '-map', '0:a?',
                '-c', 'copy',
                '-sn',
                '-dn',
                '-t', $DurationSeconds,
                '-f', 'matroska',
                (Join-Path $ArtifactRoot 'ffmpeg-remux-start.mkv')
            )
        },
        [pscustomobject]@{
            name = 'seeked'
            output = (Join-Path $ArtifactRoot 'ffmpeg-remux-seeked.mkv')
            stderr = (Join-Path $ArtifactRoot 'ffmpeg-remux-seeked.stderr.log')
            stdout = (Join-Path $ArtifactRoot 'ffmpeg-remux-seeked.stdout.log')
            arguments = @(
                '-v', 'error',
                '-nostdin',
                '-y',
                '-ss', (Get-SeekTimestamp -TotalSeconds $SeekSeconds),
                '-i', $FilePath,
                '-ignore_unknown',
                '-map', '0:v:0',
                '-map', '0:a?',
                '-c', 'copy',
                '-sn',
                '-dn',
                '-t', $DurationSeconds,
                '-f', 'matroska',
                (Join-Path $ArtifactRoot 'ffmpeg-remux-seeked.mkv')
            )
        }
    )

    foreach ($target in $targets) {
        foreach ($path in @($target.output, $target.stderr, $target.stdout)) {
            if (Test-Path -LiteralPath $path) {
                Remove-Item -LiteralPath $path -Force
            }
        }

        $commandResult = Invoke-FfmpegCommand -ExecutablePath $FfmpegPath -Arguments $target.arguments -StdoutPath $target.stdout -StderrPath $target.stderr -TimeoutSeconds $TimeoutSeconds

        $null = Assert-LiveMount -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs
        $outputExists = Test-Path -LiteralPath $target.output
        $outputSize = if ($outputExists) { (Get-Item -LiteralPath $target.output).Length } else { 0 }
        $status = if ($commandResult.timed_out) { 'timeout' } elseif ($commandResult.exit_code -eq 0 -and $outputExists -and $outputSize -gt 0) { 'ok' } else { 'error' }
        $invocations.Add([pscustomobject]@{
            name = $target.name
            exit_code = $commandResult.exit_code
            timed_out = $commandResult.timed_out
            pid = $commandResult.pid
            timeout_seconds = $TimeoutSeconds
            output_path = $target.output
            output_exists = $outputExists
            output_bytes = [int64]$outputSize
            stderr_path = $target.stderr
            stdout_path = $target.stdout
            status = $status
        })

        if ($commandResult.timed_out) {
            throw ("FFmpeg remux scenario '{0}' timed out after {1}s. stderr={2}" -f $target.name, $TimeoutSeconds, $target.stderr)
        }
        if ($commandResult.exit_code -ne 0) {
            throw ("FFmpeg remux scenario '{0}' failed with exit code {1}. stderr={2}" -f $target.name, $commandResult.exit_code, $target.stderr)
        }
        if (-not $outputExists -or $outputSize -le 0) {
            throw ("FFmpeg remux scenario '{0}' produced no output: {1}" -f $target.name, $target.output)
        }
    }

    [pscustomobject]@{
        scenario = 'directstream_remux'
        ffmpeg_path = $FfmpegPath
        file_path = $FilePath
        duration_seconds = [math]::Round(((Get-Date) - $startedAt).TotalSeconds, 3)
        remux_seconds = $DurationSeconds
        seek_seconds = $SeekSeconds
        invocations = $invocations
        status = 'ok'
    }
}

function Get-LogLineCount {
    param(
        [Parameter(Mandatory = $true)][string] $Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return 0
    }

    return @(Get-Content -LiteralPath $Path -ErrorAction SilentlyContinue).Count
}

function Get-LogDeltaLines {
    param(
        [Parameter(Mandatory = $true)][string] $Path,
        [int] $StartLineExclusive = 0
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return @()
    }

    $allLines = @(Get-Content -LiteralPath $Path -ErrorAction SilentlyContinue)
    if ($StartLineExclusive -le 0) {
        return $allLines
    }
    if ($StartLineExclusive -ge $allLines.Count) {
        return @()
    }
    return @($allLines | Select-Object -Skip $StartLineExclusive)
}

function Get-LogTailSnapshot {
    param(
        [Parameter(Mandatory = $true)][string] $Path,
        [int] $TailLines = 80,
        [int] $StartLineExclusive = 0
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return [pscustomobject]@{
            path = $Path
            exists = $false
            line_count = 0
            delta_line_count = 0
            tail = @()
            stale_mentions = 0
            prefetch_mentions = 0
            error_mentions = 0
        }
    }

    $deltaLines = @(Get-LogDeltaLines -Path $Path -StartLineExclusive $StartLineExclusive)
    $tail = if ($deltaLines.Count -gt 0) { @($deltaLines | Select-Object -Last $TailLines) } else { @() }
    [pscustomobject]@{
        path = $Path
        exists = $true
        line_count = Get-LogLineCount -Path $Path
        delta_line_count = $deltaLines.Count
        tail = $tail
        stale_mentions = @($tail | Select-String -Pattern 'stale|ESTALE|inline refresh' -SimpleMatch:$false).Count
        prefetch_mentions = @($tail | Select-String -Pattern 'prefetch|chunk_engine.read plan' -SimpleMatch:$false).Count
        error_mentions = @($tail | Select-String -Pattern 'error|failed|panic|allocation|STATUS_' -SimpleMatch:$false).Count
    }
}

function Get-PatternMatchCount {
    param(
        [string[]] $Lines,
        [string[]] $Patterns
    )

    $count = 0
    foreach ($pattern in $Patterns) {
        $count += @($Lines | Select-String -Pattern $pattern -SimpleMatch:$false -ErrorAction SilentlyContinue).Count
    }
    return $count
}

function Get-BackendStreamStatusSnapshot {
    param(
        [string] $BackendUrl,
        [string] $ApiKey
    )

    if ([string]::IsNullOrWhiteSpace($BackendUrl)) {
        return [ordered]@{
            captured = $false
            reason = 'backend_url_not_configured'
            stream_status_url = $null
            governance = $null
        }
    }
    $backendHeaders = Resolve-BackendHeaders `
        -ApiKey $ApiKey `
        -RepoRoot (Split-Path -Parent $PSScriptRoot)
    if ($backendHeaders.Count -eq 0) {
        return [ordered]@{
            captured = $false
            reason = 'backend_auth_missing'
            stream_status_url = $null
            governance = $null
        }
    }

    $streamStatusUrl = $BackendUrl.TrimEnd('/') + '/api/v1/stream/status'
    try {
        $response = Invoke-RestMethod -Uri $streamStatusUrl -Headers $backendHeaders -Method Get -TimeoutSec 15
        return [ordered]@{
            captured = $true
            reason = $null
            stream_status_url = $streamStatusUrl
            governance = if ($null -ne $response -and $response.PSObject.Properties.Match('governance').Count -gt 0) { $response.governance } else { $null }
        }
    }
    catch {
        return [ordered]@{
            captured = $false
            reason = 'request_failed'
            stream_status_url = $streamStatusUrl
            governance = $null
            error = $_.Exception.Message
        }
    }
}

function Get-VfsGovernanceDelta {
    param(
        $BeforeGovernance,
        $AfterGovernance
    )

    $keys = @(
        'vfs_catalog_watch_sessions_started',
        'vfs_catalog_watch_sessions_completed',
        'vfs_catalog_watch_sessions_active',
        'vfs_catalog_reconnect_requested',
        'vfs_catalog_reconnect_delta_served',
        'vfs_catalog_reconnect_snapshot_fallback',
        'vfs_catalog_reconnect_failures',
        'vfs_catalog_snapshots_served',
        'vfs_catalog_deltas_served',
        'vfs_catalog_heartbeats_served',
        'vfs_catalog_problem_events',
        'vfs_catalog_request_stream_failures',
        'vfs_catalog_snapshot_build_failures',
        'vfs_catalog_delta_build_failures',
        'vfs_catalog_refresh_attempts',
        'vfs_catalog_refresh_succeeded',
        'vfs_catalog_refresh_provider_failures',
        'vfs_catalog_refresh_empty_results',
        'vfs_catalog_refresh_validation_failed',
        'vfs_catalog_refresh_skipped_no_provider',
        'vfs_catalog_refresh_skipped_no_restricted_url',
        'vfs_catalog_refresh_skipped_no_client',
        'vfs_catalog_refresh_skipped_fresh',
        'vfs_catalog_inline_refresh_requests',
        'vfs_catalog_inline_refresh_succeeded',
        'vfs_catalog_inline_refresh_failed',
        'vfs_catalog_inline_refresh_not_found'
    )

    $delta = [ordered]@{}
    foreach ($key in $keys) {
        $beforeValue = 0
        $afterValue = 0
        if ($null -ne $BeforeGovernance -and $BeforeGovernance.PSObject.Properties.Match($key).Count -gt 0) {
            $beforeValue = [int]$BeforeGovernance.$key
        }
        if ($null -ne $AfterGovernance -and $AfterGovernance.PSObject.Properties.Match($key).Count -gt 0) {
            $afterValue = [int]$AfterGovernance.$key
        }
        $delta[$key] = $afterValue - $beforeValue
    }
    return $delta
}

function Get-FilmuvfsRuntimeStatusSnapshot {
    param($FilmuvfsState)

    $path = if ($null -ne $FilmuvfsState -and $FilmuvfsState.PSObject.Properties.Match('state_runtime_status_path').Count -gt 0 -and -not [string]::IsNullOrWhiteSpace([string]$FilmuvfsState.state_runtime_status_path)) {
        [string]$FilmuvfsState.state_runtime_status_path
    } else {
        Join-Path (Get-StateDirectory) 'filmuvfs-runtime-status.json'
    }

    if (-not (Test-Path -LiteralPath $path)) {
        return [ordered]@{
            captured = $false
            path = $path
            reason = 'runtime_status_missing'
            snapshot = $null
        }
    }

    try {
        $snapshot = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
        return [ordered]@{
            captured = $true
            path = $path
            reason = $null
            snapshot = $snapshot
        }
    }
    catch {
        return [ordered]@{
            captured = $false
            path = $path
            reason = 'runtime_status_parse_failed'
            error = $_.Exception.Message
            snapshot = $null
        }
    }
}

function Get-NestedRuntimeMetric {
    param(
        $Snapshot,
        [string[]] $Path
    )

    $current = $Snapshot
    foreach ($segment in $Path) {
        if ($null -eq $current -or $current.PSObject.Properties.Match($segment).Count -eq 0) {
            return 0
        }
        $current = $current.$segment
    }
    if ($null -eq $current) {
        return 0
    }
    return [int64]$current
}

function Get-NestedRuntimeFloat {
    param(
        $Snapshot,
        [string[]] $Path
    )

    $current = $Snapshot
    foreach ($segment in $Path) {
        if ($null -eq $current -or $current.PSObject.Properties.Match($segment).Count -eq 0) {
            return 0.0
        }
        $current = $current.$segment
    }
    if ($null -eq $current) {
        return 0.0
    }
    return [double]$current
}

function Get-NestedRuntimeText {
    param(
        $Snapshot,
        [string[]] $Path,
        [string] $Default = ''
    )

    $current = $Snapshot
    foreach ($segment in $Path) {
        if ($null -eq $current -or $current.PSObject.Properties.Match($segment).Count -eq 0) {
            return $Default
        }
        $current = $current.$segment
    }
    if ($null -eq $current) {
        return $Default
    }
    $text = [string]$current
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $Default
    }
    return $text.Trim()
}

function Get-SafeRatio {
    param(
        [double] $Numerator,
        [double] $Denominator
    )

    if ($Denominator -le 0) {
        return 0.0
    }
    return [math]::Round(($Numerator / $Denominator), 4)
}

function Get-WeightedAverageDelta {
    param(
        [double] $BeforeAverage,
        [int64] $BeforeTotal,
        [double] $AfterAverage,
        [int64] $AfterTotal
    )

    $deltaTotal = $AfterTotal - $BeforeTotal
    if ($deltaTotal -le 0) {
        return 0.0
    }

    $beforeWeighted = $BeforeAverage * $BeforeTotal
    $afterWeighted = $AfterAverage * $AfterTotal
    return [math]::Round((($afterWeighted - $beforeWeighted) / $deltaTotal), 3)
}

function Get-NewMaximumDelta {
    param(
        [double] $BeforeMaximum,
        [double] $AfterMaximum
    )

    if ($AfterMaximum -le $BeforeMaximum) {
        return 0.0
    }
    return [math]::Round($AfterMaximum, 3)
}

function Get-PressureClass {
    param(
        [bool] $Critical,
        [bool] $Warning
    )

    if ($Critical) {
        return 'critical'
    }
    if ($Warning) {
        return 'warning'
    }
    return 'healthy'
}

function Get-BackendVfsPressureSnapshot {
    param($Governance)

    if ($null -eq $Governance) {
        return $null
    }

    $result = [ordered]@{}
    foreach ($name in @(
        'vfs_runtime_cache_pressure_class',
        'vfs_runtime_cache_pressure_reasons',
        'vfs_runtime_chunk_coalescing_pressure_class',
        'vfs_runtime_chunk_coalescing_pressure_reasons',
        'vfs_runtime_upstream_wait_class',
        'vfs_runtime_upstream_wait_reasons',
        'vfs_runtime_refresh_pressure_class',
        'vfs_runtime_refresh_pressure_reasons'
    )) {
        if ($Governance.PSObject.Properties.Match($name).Count -gt 0) {
            $result[$name] = $Governance.$name
        }
    }
    return $result
}

function Get-FilmuvfsRuntimeDelta {
    param(
        $BeforeSnapshot,
        $AfterSnapshot
    )

    if ($null -eq $BeforeSnapshot -or $null -eq $AfterSnapshot) {
        return $null
    }

    return [ordered]@{
        handle_startup_total = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('handle_startup', 'total')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('handle_startup', 'total'))
        handle_startup_ok = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('handle_startup', 'ok')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('handle_startup', 'ok'))
        handle_startup_error = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('handle_startup', 'error')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('handle_startup', 'error'))
        handle_startup_estale = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('handle_startup', 'estale')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('handle_startup', 'estale'))
        handle_startup_average_duration_ms = Get-WeightedAverageDelta -BeforeAverage (Get-NestedRuntimeFloat -Snapshot $BeforeSnapshot -Path @('handle_startup', 'average_duration_ms')) -BeforeTotal (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('handle_startup', 'total')) -AfterAverage (Get-NestedRuntimeFloat -Snapshot $AfterSnapshot -Path @('handle_startup', 'average_duration_ms')) -AfterTotal (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('handle_startup', 'total'))
        handle_startup_new_max_duration_ms = Get-NewMaximumDelta -BeforeMaximum (Get-NestedRuntimeFloat -Snapshot $BeforeSnapshot -Path @('handle_startup', 'max_duration_ms')) -AfterMaximum (Get-NestedRuntimeFloat -Snapshot $AfterSnapshot -Path @('handle_startup', 'max_duration_ms'))
        mounted_reads_total = ([int64]$AfterSnapshot.mounted_reads.total) - ([int64]$BeforeSnapshot.mounted_reads.total)
        mounted_reads_ok = ([int64]$AfterSnapshot.mounted_reads.ok) - ([int64]$BeforeSnapshot.mounted_reads.ok)
        mounted_reads_error = ([int64]$AfterSnapshot.mounted_reads.error) - ([int64]$BeforeSnapshot.mounted_reads.error)
        mounted_reads_estale = ([int64]$AfterSnapshot.mounted_reads.estale) - ([int64]$BeforeSnapshot.mounted_reads.estale)
        upstream_fetch_operations = ([int64]$AfterSnapshot.upstream_fetch.operations) - ([int64]$BeforeSnapshot.upstream_fetch.operations)
        upstream_fetch_bytes_total = ([int64]$AfterSnapshot.upstream_fetch.bytes_total) - ([int64]$BeforeSnapshot.upstream_fetch.bytes_total)
        upstream_fetch_average_duration_ms = Get-WeightedAverageDelta -BeforeAverage (Get-NestedRuntimeFloat -Snapshot $BeforeSnapshot -Path @('upstream_fetch', 'average_duration_ms')) -BeforeTotal (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_fetch', 'operations')) -AfterAverage (Get-NestedRuntimeFloat -Snapshot $AfterSnapshot -Path @('upstream_fetch', 'average_duration_ms')) -AfterTotal (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_fetch', 'operations'))
        upstream_fetch_new_max_duration_ms = Get-NewMaximumDelta -BeforeMaximum (Get-NestedRuntimeFloat -Snapshot $BeforeSnapshot -Path @('upstream_fetch', 'max_duration_ms')) -AfterMaximum (Get-NestedRuntimeFloat -Snapshot $AfterSnapshot -Path @('upstream_fetch', 'max_duration_ms'))
        upstream_fail_invalid_url = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_failures', 'invalid_url')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_failures', 'invalid_url'))
        upstream_fail_build_request = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_failures', 'build_request')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_failures', 'build_request'))
        upstream_fail_network = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_failures', 'network')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_failures', 'network'))
        upstream_fail_stale_status = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_failures', 'stale_status')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_failures', 'stale_status'))
        upstream_fail_unexpected_status = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_failures', 'unexpected_status')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_failures', 'unexpected_status'))
        upstream_fail_unexpected_status_too_many_requests = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_failures', 'unexpected_status_too_many_requests')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_failures', 'unexpected_status_too_many_requests'))
        upstream_fail_unexpected_status_server_error = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_failures', 'unexpected_status_server_error')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_failures', 'unexpected_status_server_error'))
        upstream_fail_read_body = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_failures', 'read_body')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_failures', 'read_body'))
        upstream_retryable_network = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_retryable_events', 'network')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_retryable_events', 'network'))
        upstream_retryable_read_body = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_retryable_events', 'read_body')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_retryable_events', 'read_body'))
        upstream_retryable_status_too_many_requests = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_retryable_events', 'status_too_many_requests')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_retryable_events', 'status_too_many_requests'))
        upstream_retryable_status_server_error = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('upstream_retryable_events', 'status_server_error')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('upstream_retryable_events', 'status_server_error'))
        backend_fallback_attempts = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'attempts')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'attempts'))
        backend_fallback_success = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'success')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'success'))
        backend_fallback_failure = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'failure')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'failure'))
        backend_fallback_attempts_direct_read_failure = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'attempts_direct_read_failure')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'attempts_direct_read_failure'))
        backend_fallback_attempts_inline_refresh_unavailable = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'attempts_inline_refresh_unavailable')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'attempts_inline_refresh_unavailable'))
        backend_fallback_attempts_post_inline_refresh_failure = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'attempts_post_inline_refresh_failure')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'attempts_post_inline_refresh_failure'))
        backend_fallback_success_direct_read_failure = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'success_direct_read_failure')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'success_direct_read_failure'))
        backend_fallback_success_inline_refresh_unavailable = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'success_inline_refresh_unavailable')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'success_inline_refresh_unavailable'))
        backend_fallback_success_post_inline_refresh_failure = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'success_post_inline_refresh_failure')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'success_post_inline_refresh_failure'))
        backend_fallback_failure_direct_read_failure = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'failure_direct_read_failure')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'failure_direct_read_failure'))
        backend_fallback_failure_inline_refresh_unavailable = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'failure_inline_refresh_unavailable')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'failure_inline_refresh_unavailable'))
        backend_fallback_failure_post_inline_refresh_failure = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('backend_fallback', 'failure_post_inline_refresh_failure')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('backend_fallback', 'failure_post_inline_refresh_failure'))
        chunk_cache_hits = ([int64]$AfterSnapshot.chunk_cache.hits) - ([int64]$BeforeSnapshot.chunk_cache.hits)
        chunk_cache_misses = ([int64]$AfterSnapshot.chunk_cache.misses) - ([int64]$BeforeSnapshot.chunk_cache.misses)
        chunk_cache_inserts = ([int64]$AfterSnapshot.chunk_cache.inserts) - ([int64]$BeforeSnapshot.chunk_cache.inserts)
        chunk_cache_memory_hits = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_cache', 'memory_hits')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_cache', 'memory_hits'))
        chunk_cache_memory_misses = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_cache', 'memory_misses')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_cache', 'memory_misses'))
        chunk_cache_disk_hits = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_cache', 'disk_hits')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_cache', 'disk_hits'))
        chunk_cache_disk_misses = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_cache', 'disk_misses')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_cache', 'disk_misses'))
        chunk_cache_disk_writes = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_cache', 'disk_writes')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_cache', 'disk_writes'))
        chunk_cache_disk_write_errors = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_cache', 'disk_write_errors')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_cache', 'disk_write_errors'))
        chunk_cache_disk_evictions = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_cache', 'disk_evictions')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_cache', 'disk_evictions'))
        prefetch_concurrency_limit = Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('prefetch', 'concurrency_limit')
        prefetch_available_permits = Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('prefetch', 'available_permits')
        prefetch_active_permits = Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('prefetch', 'active_permits')
        prefetch_active_background_tasks = Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('prefetch', 'active_background_tasks')
        prefetch_background_spawned = ([int64]$AfterSnapshot.prefetch.background_spawned) - ([int64]$BeforeSnapshot.prefetch.background_spawned)
        prefetch_background_backpressure = ([int64]$AfterSnapshot.prefetch.background_backpressure) - ([int64]$BeforeSnapshot.prefetch.background_backpressure)
        prefetch_background_error = ([int64]$AfterSnapshot.prefetch.background_error) - ([int64]$BeforeSnapshot.prefetch.background_error)
        chunk_coalescing_in_flight_chunks = Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_coalescing', 'in_flight_chunks')
        chunk_coalescing_waits_total = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_coalescing', 'waits_total')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_coalescing', 'waits_total'))
        chunk_coalescing_waits_hit = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_coalescing', 'waits_hit')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_coalescing', 'waits_hit'))
        chunk_coalescing_waits_miss = (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_coalescing', 'waits_miss')) - (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_coalescing', 'waits_miss'))
        chunk_coalescing_wait_average_duration_ms = Get-WeightedAverageDelta -BeforeAverage (Get-NestedRuntimeFloat -Snapshot $BeforeSnapshot -Path @('chunk_coalescing', 'wait_average_duration_ms')) -BeforeTotal (Get-NestedRuntimeMetric -Snapshot $BeforeSnapshot -Path @('chunk_coalescing', 'waits_total')) -AfterAverage (Get-NestedRuntimeFloat -Snapshot $AfterSnapshot -Path @('chunk_coalescing', 'wait_average_duration_ms')) -AfterTotal (Get-NestedRuntimeMetric -Snapshot $AfterSnapshot -Path @('chunk_coalescing', 'waits_total'))
        chunk_coalescing_wait_new_max_duration_ms = Get-NewMaximumDelta -BeforeMaximum (Get-NestedRuntimeFloat -Snapshot $BeforeSnapshot -Path @('chunk_coalescing', 'wait_max_duration_ms')) -AfterMaximum (Get-NestedRuntimeFloat -Snapshot $AfterSnapshot -Path @('chunk_coalescing', 'wait_max_duration_ms'))
        inline_refresh_success = ([int64]$AfterSnapshot.inline_refresh.success) - ([int64]$BeforeSnapshot.inline_refresh.success)
        inline_refresh_no_url = ([int64]$AfterSnapshot.inline_refresh.no_url) - ([int64]$BeforeSnapshot.inline_refresh.no_url)
        inline_refresh_error = ([int64]$AfterSnapshot.inline_refresh.error) - ([int64]$BeforeSnapshot.inline_refresh.error)
        inline_refresh_timeout = ([int64]$AfterSnapshot.inline_refresh.timeout) - ([int64]$BeforeSnapshot.inline_refresh.timeout)
        windows_callbacks_error = ([int64]$AfterSnapshot.windows_projfs.callbacks_error) - ([int64]$BeforeSnapshot.windows_projfs.callbacks_error)
        windows_callbacks_estale = ([int64]$AfterSnapshot.windows_projfs.callbacks_estale) - ([int64]$BeforeSnapshot.windows_projfs.callbacks_estale)
    }
}

function Get-RuntimeDiagnostics {
    param(
        $RuntimeDelta,
        $RuntimeAfterSnapshot
    )

    if ($null -eq $RuntimeDelta) {
        return [ordered]@{
            captured = $false
            peak_open_handles = $null
            peak_active_reads = $null
            chunk_cache_backend = $null
            chunk_cache_memory_bytes = $null
            chunk_cache_memory_max_bytes = $null
            chunk_cache_disk_bytes = $null
            chunk_cache_disk_max_bytes = $null
            chunk_cache_memory_hits = $null
            chunk_cache_memory_misses = $null
            chunk_cache_disk_hits = $null
            chunk_cache_disk_misses = $null
            chunk_cache_disk_writes = $null
            chunk_cache_disk_write_errors = $null
            chunk_cache_disk_evictions = $null
            cache_cold_fetch_incidents = $null
            cache_hit_ratio = $null
            cache_pressure_incidents = $null
            prefetch_concurrency_limit = $null
            prefetch_available_permits = $null
            prefetch_active_permits = $null
            prefetch_active_background_tasks = $null
            prefetch_peak_active_background_tasks = $null
            chunk_coalescing_in_flight_chunks = $null
            chunk_coalescing_peak_in_flight_chunks = $null
            chunk_coalescing_waits_total = $null
            chunk_coalescing_waits_hit = $null
            chunk_coalescing_waits_miss = $null
            chunk_coalescing_wait_average_duration_ms = $null
            chunk_coalescing_wait_new_max_duration_ms = $null
            provider_pressure_incidents = $null
            unrecovered_stale_refresh_incidents = $null
            upstream_fetch_average_duration_ms = $null
            upstream_fetch_new_max_duration_ms = $null
            fatal_error_incidents = $null
            cache_pressure_class = $null
            cache_pressure_reasons = $null
            chunk_coalescing_pressure_class = $null
            chunk_coalescing_pressure_reasons = $null
            upstream_wait_class = $null
            upstream_wait_reasons = $null
            refresh_pressure_class = $null
            refresh_pressure_reasons = $null
            failure_classifications = $null
        }
    }

    $failureClassifications = [ordered]@{
        mounted_read_error = [int64]$RuntimeDelta.mounted_reads_error
        mounted_read_estale = [int64]$RuntimeDelta.mounted_reads_estale
        upstream_fail_invalid_url = [int64]$RuntimeDelta.upstream_fail_invalid_url
        upstream_fail_build_request = [int64]$RuntimeDelta.upstream_fail_build_request
        upstream_fail_network = [int64]$RuntimeDelta.upstream_fail_network
        upstream_fail_stale_status = [int64]$RuntimeDelta.upstream_fail_stale_status
        upstream_fail_unexpected_status = [int64]$RuntimeDelta.upstream_fail_unexpected_status
        upstream_fail_unexpected_status_too_many_requests = [int64]$RuntimeDelta.upstream_fail_unexpected_status_too_many_requests
        upstream_fail_unexpected_status_server_error = [int64]$RuntimeDelta.upstream_fail_unexpected_status_server_error
        upstream_fail_read_body = [int64]$RuntimeDelta.upstream_fail_read_body
        backend_fallback_failure = [int64]$RuntimeDelta.backend_fallback_failure
        windows_callback_error = [int64]$RuntimeDelta.windows_callbacks_error
        windows_callback_estale = [int64]$RuntimeDelta.windows_callbacks_estale
    }

    $fatalIncidents = (
        [int64]$RuntimeDelta.mounted_reads_error +
        [int64]$RuntimeDelta.upstream_fail_invalid_url +
        [int64]$RuntimeDelta.upstream_fail_build_request +
        [int64]$RuntimeDelta.upstream_fail_network +
        [int64]$RuntimeDelta.upstream_fail_unexpected_status +
        [int64]$RuntimeDelta.upstream_fail_read_body +
        [int64]$RuntimeDelta.backend_fallback_failure +
        [int64]$RuntimeDelta.windows_callbacks_error
    )
    $providerPressureIncidents = (
        [int64]$RuntimeDelta.upstream_retryable_status_too_many_requests +
        [int64]$RuntimeDelta.upstream_retryable_status_server_error +
        [int64]$RuntimeDelta.upstream_fail_unexpected_status_too_many_requests +
        [int64]$RuntimeDelta.upstream_fail_unexpected_status_server_error
    )
    $cachePressureIncidents = (
        [int64]$RuntimeDelta.prefetch_background_backpressure +
        [int64]$RuntimeDelta.prefetch_background_error
    )
    $cacheMemoryPressureRatio = Get-SafeRatio -Numerator (Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('chunk_cache', 'memory_bytes')) -Denominator (Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('chunk_cache', 'memory_max_bytes'))
    $cacheDiskPressureRatio = Get-SafeRatio -Numerator (Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('chunk_cache', 'disk_bytes')) -Denominator (Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('chunk_cache', 'disk_max_bytes'))
    $cacheHitRatio = Get-SafeRatio -Numerator ([double]$RuntimeDelta.chunk_cache_hits) -Denominator ([double]([int64]$RuntimeDelta.chunk_cache_hits + [int64]$RuntimeDelta.chunk_cache_misses))
    $cachePressureReasons = [System.Collections.Generic.List[string]]::new()
    if ([int64]$RuntimeDelta.chunk_cache_disk_write_errors -gt 0) {
        [void]$cachePressureReasons.Add('disk_write_errors')
    }
    if ([math]::Max($cacheMemoryPressureRatio, $cacheDiskPressureRatio) -ge 0.85) {
        [void]$cachePressureReasons.Add('cache_capacity_high')
    }
    if ([int64]$RuntimeDelta.chunk_cache_disk_evictions -gt 0) {
        [void]$cachePressureReasons.Add('disk_evictions_observed')
    }

    $chunkCoalescingPressureReasons = [System.Collections.Generic.List[string]]::new()
    if ([int64]$RuntimeDelta.chunk_coalescing_waits_miss -gt 0) {
        [void]$chunkCoalescingPressureReasons.Add('coalescing_wait_misses')
    }
    if ([double]$RuntimeDelta.chunk_coalescing_wait_average_duration_ms -ge 10.0) {
        [void]$chunkCoalescingPressureReasons.Add('coalescing_wait_latency_high')
    }
    if ([double]$RuntimeDelta.chunk_coalescing_wait_new_max_duration_ms -ge 250.0) {
        [void]$chunkCoalescingPressureReasons.Add('coalescing_wait_spike')
    }

    $upstreamWaitReasons = [System.Collections.Generic.List[string]]::new()
    if ($providerPressureIncidents -gt 0) {
        [void]$upstreamWaitReasons.Add('provider_pressure_incidents')
    }
    if ([int64]$RuntimeDelta.upstream_retryable_network -gt 0) {
        [void]$upstreamWaitReasons.Add('retryable_network_wait')
    }
    if ([int64]$RuntimeDelta.upstream_retryable_read_body -gt 0) {
        [void]$upstreamWaitReasons.Add('retryable_read_body_wait')
    }
    if ([double]$RuntimeDelta.upstream_fetch_average_duration_ms -ge 100.0) {
        [void]$upstreamWaitReasons.Add('average_fetch_latency_high')
    }
    if ([double]$RuntimeDelta.upstream_fetch_new_max_duration_ms -ge 1000.0) {
        [void]$upstreamWaitReasons.Add('max_fetch_latency_high')
    }

    $refreshPressureReasons = [System.Collections.Generic.List[string]]::new()
    if ([int64]$RuntimeDelta.backend_fallback_failure -gt 0) {
        [void]$refreshPressureReasons.Add('backend_fallback_failures')
    }
    if ([int64]$RuntimeDelta.inline_refresh_error -gt 0) {
        [void]$refreshPressureReasons.Add('inline_refresh_errors')
    }
    if ([int64]$RuntimeDelta.inline_refresh_timeout -gt 0) {
        [void]$refreshPressureReasons.Add('inline_refresh_timeouts')
    }
    if ([int64]$RuntimeDelta.backend_fallback_attempts -gt 0) {
        [void]$refreshPressureReasons.Add('backend_fallback_activity')
    }

    return [ordered]@{
        captured = $true
        peak_open_handles = Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('runtime', 'peak_open_handles')
        peak_active_reads = Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('runtime', 'peak_active_reads')
        chunk_cache_backend = Get-NestedRuntimeText -Snapshot $RuntimeAfterSnapshot -Path @('chunk_cache', 'backend') -Default 'unknown'
        chunk_cache_memory_bytes = Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('chunk_cache', 'memory_bytes')
        chunk_cache_memory_max_bytes = Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('chunk_cache', 'memory_max_bytes')
        chunk_cache_disk_bytes = Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('chunk_cache', 'disk_bytes')
        chunk_cache_disk_max_bytes = Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('chunk_cache', 'disk_max_bytes')
        chunk_cache_memory_hits = [int64]$RuntimeDelta.chunk_cache_memory_hits
        chunk_cache_memory_misses = [int64]$RuntimeDelta.chunk_cache_memory_misses
        chunk_cache_disk_hits = [int64]$RuntimeDelta.chunk_cache_disk_hits
        chunk_cache_disk_misses = [int64]$RuntimeDelta.chunk_cache_disk_misses
        chunk_cache_disk_writes = [int64]$RuntimeDelta.chunk_cache_disk_writes
        chunk_cache_disk_write_errors = [int64]$RuntimeDelta.chunk_cache_disk_write_errors
        chunk_cache_disk_evictions = [int64]$RuntimeDelta.chunk_cache_disk_evictions
        cache_cold_fetch_incidents = [int64]$RuntimeDelta.chunk_cache_misses
        cache_hit_ratio = $cacheHitRatio
        cache_pressure_incidents = $cachePressureIncidents
        prefetch_concurrency_limit = [int64]$RuntimeDelta.prefetch_concurrency_limit
        prefetch_available_permits = [int64]$RuntimeDelta.prefetch_available_permits
        prefetch_active_permits = [int64]$RuntimeDelta.prefetch_active_permits
        prefetch_active_background_tasks = [int64]$RuntimeDelta.prefetch_active_background_tasks
        prefetch_peak_active_background_tasks = Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('prefetch', 'peak_active_background_tasks')
        chunk_coalescing_in_flight_chunks = [int64]$RuntimeDelta.chunk_coalescing_in_flight_chunks
        chunk_coalescing_peak_in_flight_chunks = Get-NestedRuntimeMetric -Snapshot $RuntimeAfterSnapshot -Path @('chunk_coalescing', 'peak_in_flight_chunks')
        chunk_coalescing_waits_total = [int64]$RuntimeDelta.chunk_coalescing_waits_total
        chunk_coalescing_waits_hit = [int64]$RuntimeDelta.chunk_coalescing_waits_hit
        chunk_coalescing_waits_miss = [int64]$RuntimeDelta.chunk_coalescing_waits_miss
        chunk_coalescing_wait_average_duration_ms = [double]$RuntimeDelta.chunk_coalescing_wait_average_duration_ms
        chunk_coalescing_wait_new_max_duration_ms = [double]$RuntimeDelta.chunk_coalescing_wait_new_max_duration_ms
        provider_pressure_incidents = $providerPressureIncidents
        unrecovered_stale_refresh_incidents = [int64]$RuntimeDelta.mounted_reads_estale
        handle_startup_total = [int64]$RuntimeDelta.handle_startup_total
        handle_startup_failures = ([int64]$RuntimeDelta.handle_startup_error + [int64]$RuntimeDelta.handle_startup_estale)
        handle_startup_average_duration_ms = [int64]$RuntimeDelta.handle_startup_average_duration_ms
        handle_startup_max_duration_ms = [double]$RuntimeDelta.handle_startup_new_max_duration_ms
        upstream_fetch_average_duration_ms = [double]$RuntimeDelta.upstream_fetch_average_duration_ms
        upstream_fetch_new_max_duration_ms = [double]$RuntimeDelta.upstream_fetch_new_max_duration_ms
        backend_fallback_attempts = [int64]$RuntimeDelta.backend_fallback_attempts
        backend_fallback_success = [int64]$RuntimeDelta.backend_fallback_success
        backend_fallback_failure = [int64]$RuntimeDelta.backend_fallback_failure
        fatal_error_incidents = $fatalIncidents
        cache_pressure_class = Get-PressureClass -Critical (([int64]$RuntimeDelta.chunk_cache_disk_write_errors -gt 0) -or ([math]::Max($cacheMemoryPressureRatio, $cacheDiskPressureRatio) -ge 1.10)) -Warning ($cachePressureReasons.Count -gt 0)
        cache_pressure_reasons = @($cachePressureReasons)
        chunk_coalescing_pressure_class = Get-PressureClass -Critical (([int64]$RuntimeDelta.chunk_coalescing_waits_miss -ge 5) -or ([double]$RuntimeDelta.chunk_coalescing_wait_average_duration_ms -ge 1000.0) -or ([double]$RuntimeDelta.chunk_coalescing_wait_new_max_duration_ms -ge 2000.0)) -Warning ($chunkCoalescingPressureReasons.Count -gt 0)
        chunk_coalescing_pressure_reasons = @($chunkCoalescingPressureReasons)
        upstream_wait_class = Get-PressureClass -Critical (($providerPressureIncidents -ge 10) -or ([double]$RuntimeDelta.upstream_fetch_average_duration_ms -ge 250.0) -or (([double]$RuntimeDelta.upstream_fetch_average_duration_ms -ge 100.0) -and ([double]$RuntimeDelta.upstream_fetch_new_max_duration_ms -ge 5000.0))) -Warning ($upstreamWaitReasons.Count -gt 0)
        upstream_wait_reasons = @($upstreamWaitReasons)
        refresh_pressure_class = Get-PressureClass -Critical (([int64]$RuntimeDelta.backend_fallback_failure -gt 0) -or ([int64]$RuntimeDelta.inline_refresh_timeout -ge 3)) -Warning ($refreshPressureReasons.Count -gt 0)
        refresh_pressure_reasons = @($refreshPressureReasons)
        failure_classifications = $failureClassifications
    }
}

function Get-LogDiagnostics {
    param($LogWindows)

    $reconnectPatterns = @(
        'catalog watch session failed; retrying after backoff',
        'WatchCatalog stream ended; reconnecting without unmounting'
    )
    $staleAttemptPatterns = @(
        'attempting inline refresh before surfacing read failure'
    )
    $staleFailurePatterns = @(
        'inline refresh returned another stale URL; returning ESTALE',
        'direct upstream retry after inline refresh failed',
        'backend HTTP fallback failed after inline refresh retry'
    )
    $cacheColdFetchPatterns = @(
        'chunk_engine.read cache miss; fetching foreground chunk',
        'miss_after_wait',
        'miss_after_inflight_wait'
    )
    $cacheHitPatterns = @(
        'chunk_engine.read served chunk from cache',
        'chunk_engine.read resolved from cache after in-flight wait',
        'chunk_engine.read foreground wait resolved from cache'
    )
    $providerPressurePatterns = @(
        '429',
        'Too Many Requests',
        'rate limit',
        'rate_limit',
        'circuit open'
    )
    $lines = [System.Collections.Generic.List[string]]::new()
    foreach ($window in $LogWindows) {
        foreach ($line in @(Get-LogDeltaLines -Path $window.Path -StartLineExclusive $window.StartLineExclusive)) {
            [void]$lines.Add([string]$line)
        }
    }
    $allLines = @($lines)
    $failureClassifications = [ordered]@{
        panic = (Get-PatternMatchCount -Lines $allLines -Patterns @('panic', 'panicked at'))
        mounted_read_failure = (Get-PatternMatchCount -Lines $allLines -Patterns @('vfs.read.fail', 'I/O failure while reading'))
        inline_refresh_failure = (Get-PatternMatchCount -Lines $allLines -Patterns $staleFailurePatterns)
        callback_error = (Get-PatternMatchCount -Lines $allLines -Patterns @('projfs.get_file_data open failed', 'callback failed'))
        ntstatus_failure = (Get-PatternMatchCount -Lines $allLines -Patterns @('STATUS_', 'NTSTATUS'))
    }

    $fatalIncidents = 0
    foreach ($value in $failureClassifications.Values) {
        $fatalIncidents += [int]$value
    }

    return [ordered]@{
        reconnect_incidents = (Get-PatternMatchCount -Lines $allLines -Patterns $reconnectPatterns)
        stale_refresh_attempts = (Get-PatternMatchCount -Lines $allLines -Patterns $staleAttemptPatterns)
        unrecovered_stale_refresh_incidents = (Get-PatternMatchCount -Lines $allLines -Patterns $staleFailurePatterns)
        cache_cold_fetch_incidents = (Get-PatternMatchCount -Lines $allLines -Patterns $cacheColdFetchPatterns)
        cache_hit_incidents = (Get-PatternMatchCount -Lines $allLines -Patterns $cacheHitPatterns)
        provider_pressure_incidents = (Get-PatternMatchCount -Lines $allLines -Patterns $providerPressurePatterns)
        fatal_error_incidents = $fatalIncidents
        failure_classifications = $failureClassifications
    }
}

function Add-ThresholdCheck {
    param(
        [System.Collections.Generic.List[object]] $Checks,
        [string] $Name,
        [bool] $Passed,
        [object] $Observed,
        [object] $Threshold
    )

    $Checks.Add([pscustomobject]@{
        name = $Name
        passed = $Passed
        observed = $Observed
        threshold = $Threshold
    })
}

function New-EmptyRuntimeStatusCapture {
    [pscustomobject]@{
        captured = $false
        path = $null
        snapshot = $null
        error = $null
    }
}

function New-EmptyBackendStatusCapture {
    [pscustomobject]@{
        captured = $false
        url = $null
        governance = $null
        error = $null
    }
}

Apply-SoakProfile -Profile $SoakProfile

if ([string]::IsNullOrWhiteSpace($MountPath)) {
    $state = Read-State
    if ($null -ne $state -and $state.PSObject.Properties.Match('mount_path').Count -gt 0 -and -not [string]::IsNullOrWhiteSpace([string]$state.mount_path)) {
        $MountPath = [string]$state.mount_path
    }
    else {
        $MountPath = Get-DefaultMountPath
    }
}
$MountPath = [System.IO.Path]::GetFullPath($MountPath)
$resolvedFfmpegPath = Resolve-FfmpegPath -ExplicitPath $FfmpegPath
$resolvedApiKey = Resolve-BackendApiKey -ExplicitKey $ApiKey

$artifactRoot = Join-Path (Get-StateDirectory) ("soak-{0}" -f (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $artifactRoot -Force | Out-Null

$summaryPath = Join-Path $artifactRoot 'summary.json'
$beforeState = $null
$afterState = $null
$runtimeStatusBefore = New-EmptyRuntimeStatusCapture
$runtimeStatusAfter = New-EmptyRuntimeStatusCapture
$backendStatusBefore = New-EmptyBackendStatusCapture
$backendStatusAfter = New-EmptyBackendStatusCapture
$runtimeStatusDelta = $null
$runtimeDiagnostics = [ordered]@{
    captured = $false
    peak_open_handles = $null
    peak_active_reads = $null
    chunk_cache_backend = $null
    chunk_cache_memory_bytes = $null
    chunk_cache_memory_max_bytes = $null
    chunk_cache_disk_bytes = $null
    chunk_cache_disk_max_bytes = $null
    chunk_cache_memory_hits = $null
    chunk_cache_memory_misses = $null
    chunk_cache_disk_hits = $null
    chunk_cache_disk_misses = $null
    chunk_cache_disk_writes = $null
    chunk_cache_disk_write_errors = $null
    chunk_cache_disk_evictions = $null
    cache_cold_fetch_incidents = $null
    cache_pressure_incidents = $null
    prefetch_concurrency_limit = $null
    prefetch_available_permits = $null
    prefetch_active_permits = $null
    prefetch_active_background_tasks = $null
    prefetch_peak_active_background_tasks = $null
    chunk_coalescing_in_flight_chunks = $null
    chunk_coalescing_peak_in_flight_chunks = $null
    chunk_coalescing_waits_total = $null
    chunk_coalescing_waits_hit = $null
    chunk_coalescing_waits_miss = $null
    chunk_coalescing_wait_average_duration_ms = $null
    chunk_coalescing_wait_max_duration_ms = $null
    provider_pressure_incidents = $null
    unrecovered_stale_refresh_incidents = $null
    handle_startup_total = $null
    handle_startup_failures = $null
    handle_startup_average_duration_ms = $null
    handle_startup_max_duration_ms = $null
    backend_fallback_attempts = $null
    backend_fallback_success = $null
    backend_fallback_failure = $null
    fatal_error_incidents = $null
    cache_pressure_class = $null
    cache_pressure_reasons = $null
    chunk_coalescing_pressure_class = $null
    chunk_coalescing_pressure_reasons = $null
    upstream_wait_class = $null
    upstream_wait_reasons = $null
    refresh_pressure_class = $null
    refresh_pressure_reasons = $null
    failure_classifications = $null
}
$stdoutSnapshot = [ordered]@{ path = (Get-LogPath -Name 'filmuvfs-windows-stdout.log'); exists = $false; line_count = 0; tail = @() }
$stderrSnapshot = [ordered]@{ path = (Get-LogPath -Name 'filmuvfs-windows-stderr.log'); exists = $false; line_count = 0; tail = @() }
$callbacksSnapshot = [ordered]@{ path = (Get-LogPath -Name 'filmuvfs-windows-callbacks.log'); exists = $false; line_count = 0; tail = @() }
$logWindows = @()
$logDiagnostics = [ordered]@{
    reconnect_incidents = 0
    stale_refresh_attempts = 0
    unrecovered_stale_refresh_incidents = 0
    cache_cold_fetch_incidents = 0
    cache_hit_incidents = 0
    provider_pressure_incidents = 0
    fatal_error_incidents = 0
    failure_classifications = [ordered]@{
        panic = 0
        mounted_read_failure = 0
        inline_refresh_failure = 0
        callback_error = 0
        ntstatus_failure = 0
    }
}
$scenarioResults = [System.Collections.Generic.List[object]]::new()
$failed = $false
$failureMessage = $null
$thresholdChecks = [System.Collections.Generic.List[object]]::new()
$thresholdFailures = @()
$targetFiles = @()
$primaryFile = $null
$remuxFile = $null

try {
    $beforeState = Assert-LiveMount -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs
    $runtimeStatusBefore = Get-FilmuvfsRuntimeStatusSnapshot -FilmuvfsState $beforeState
    $backendStatusBefore = Get-BackendStreamStatusSnapshot -BackendUrl $BackendUrl -ApiKey $resolvedApiKey
    $logWindows = @(
        @{ Name = 'stdout'; Path = (Get-LogPath -Name 'filmuvfs-windows-stdout.log'); StartLineExclusive = (Get-LogLineCount -Path (Get-LogPath -Name 'filmuvfs-windows-stdout.log')) },
        @{ Name = 'stderr'; Path = (Get-LogPath -Name 'filmuvfs-windows-stderr.log'); StartLineExclusive = (Get-LogLineCount -Path (Get-LogPath -Name 'filmuvfs-windows-stderr.log')) },
        @{ Name = 'callbacks'; Path = (Get-LogPath -Name 'filmuvfs-windows-callbacks.log'); StartLineExclusive = (Get-LogLineCount -Path (Get-LogPath -Name 'filmuvfs-windows-callbacks.log')) }
    )
    $targetFiles = @(Resolve-TargetFiles -MountPath $MountPath -ExplicitFile $TargetFile -DesiredCount ([Math]::Max($ConcurrentReaders, 1)))
    $primaryFile = $targetFiles[0]
    $remuxFile = Resolve-RemuxTargetFile -MountPath $MountPath -ExplicitFile $RemuxTargetFile

    if (-not $SkipSequential) {
        $scenarioResults.Add((Invoke-SequentialScenario -FilePath $primaryFile -Duration ([TimeSpan]::FromMinutes($SequentialMinutes)) -BlockSizeBytes ($SequentialBlockSizeKb * 1024) -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs))
    }
    if (-not $SkipSeek) {
        $seekDuration = if ($SeekMinutes -gt 0) { [TimeSpan]::FromMinutes($SeekMinutes) } else { [TimeSpan]::Zero }
        $scenarioResults.Add((Invoke-SeekScenario -FilePath $primaryFile -Iterations $SeekIterations -Duration $seekDuration -ReadSizeBytes ($SeekReadSizeKb * 1024) -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs))
    }
    if (-not $SkipConcurrent) {
        $concurrentDurationSeconds = if ($ConcurrentMinutes -gt 0) { $ConcurrentMinutes * 60 } else { 0 }
        $scenarioResults.Add((Invoke-ConcurrentScenario -FilePaths $targetFiles -Readers $ConcurrentReaders -Iterations $ConcurrentIterations -DurationSeconds $concurrentDurationSeconds -BlockSizeBytes ($ConcurrentBlockSizeKb * 1024) -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs))
    }
    if (-not $SkipRemux) {
        if ([string]::IsNullOrWhiteSpace($resolvedFfmpegPath)) {
            if ($RequireRemux) {
                throw 'FFmpeg path could not be resolved for remux validation.'
            }
            $scenarioResults.Add([pscustomobject]@{
                scenario = 'directstream_remux'
                status = 'skipped'
                reason = 'ffmpeg_not_found'
            })
        }
        else {
            $scenarioResults.Add((Invoke-RemuxScenario -FfmpegPath $resolvedFfmpegPath -FilePath $remuxFile -DurationSeconds $RemuxSeconds -SeekSeconds $RemuxSeekSeconds -TimeoutSeconds $RemuxTimeoutSeconds -ArtifactRoot $artifactRoot -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs))
        }
    }
}
catch {
    $failed = $true
    $failureMessage = $_.Exception.Message
}
finally {
    try {
        $afterState = Get-FilmuvfsState -MountPath $MountPath
    }
    catch {
        $afterState = [pscustomobject]@{
            timestamp = (Get-Date).ToString('o')
            mount_path = $MountPath
            mount_exists = $false
            pid = $null
            running = $false
            state_mount_status = $null
            state_mount_adapter = $null
            state_runtime_status_path = $null
        }
    }

    try {
        $runtimeStatusAfter = Get-FilmuvfsRuntimeStatusSnapshot -FilmuvfsState $afterState
    }
    catch {
        $runtimeStatusAfter = New-EmptyRuntimeStatusCapture
        $runtimeStatusAfter.error = $_.Exception.Message
    }

    try {
        $backendStatusAfter = Get-BackendStreamStatusSnapshot -BackendUrl $BackendUrl -ApiKey $resolvedApiKey
    }
    catch {
        $backendStatusAfter = New-EmptyBackendStatusCapture
        $backendStatusAfter.error = $_.Exception.Message
    }

    if ($runtimeStatusBefore.captured -and $runtimeStatusAfter.captured) {
        $runtimeStatusDelta = Get-FilmuvfsRuntimeDelta -BeforeSnapshot $runtimeStatusBefore.snapshot -AfterSnapshot $runtimeStatusAfter.snapshot
    }
    else {
        $runtimeStatusDelta = $null
    }
    $runtimeDiagnostics = Get-RuntimeDiagnostics -RuntimeDelta $runtimeStatusDelta -RuntimeAfterSnapshot $runtimeStatusAfter.snapshot
    $stdoutBaseline = $logWindows | Where-Object { $_.Name -eq 'stdout' } | Select-Object -First 1
    $stderrBaseline = $logWindows | Where-Object { $_.Name -eq 'stderr' } | Select-Object -First 1
    $callbacksBaseline = $logWindows | Where-Object { $_.Name -eq 'callbacks' } | Select-Object -First 1
    $stdoutSnapshot = Get-LogTailSnapshot -Path (Get-LogPath -Name 'filmuvfs-windows-stdout.log') -StartLineExclusive $stdoutBaseline.StartLineExclusive
    $stderrSnapshot = Get-LogTailSnapshot -Path (Get-LogPath -Name 'filmuvfs-windows-stderr.log') -StartLineExclusive $stderrBaseline.StartLineExclusive
    $callbacksSnapshot = Get-LogTailSnapshot -Path (Get-LogPath -Name 'filmuvfs-windows-callbacks.log') -StartLineExclusive $callbacksBaseline.StartLineExclusive
    $logDiagnostics = Get-LogDiagnostics -LogWindows $logWindows
    $thresholdChecks = [System.Collections.Generic.List[object]]::new()

    if (-not $SkipSequential) {
        $sequentialScenario = $scenarioResults | Where-Object { $_.scenario -eq 'sequential' } | Select-Object -First 1
        $sequentialObserved = if ($null -ne $sequentialScenario) { $sequentialScenario.duration_seconds } else { $null }
        Add-ThresholdCheck -Checks $thresholdChecks -Name 'continuous_playback_duration' -Passed ([bool]($null -ne $sequentialScenario -and $sequentialScenario.duration_seconds -ge ($SequentialMinutes * 60))) -Observed $sequentialObserved -Threshold ($SequentialMinutes * 60)
    }
    if (-not $SkipSeek) {
        $seekScenario = $scenarioResults | Where-Object { $_.scenario -eq 'seek_resume' } | Select-Object -First 1
        $seekThreshold = if ($SeekMinutes -gt 0) { $SeekMinutes * 60 } else { $SeekIterations }
        $seekObserved = if ($SeekMinutes -gt 0 -and $null -ne $seekScenario) { $seekScenario.duration_seconds } elseif ($null -ne $seekScenario) { $seekScenario.reads } else { $null }
        $seekPassed = if ($SeekMinutes -gt 0) { [bool]($null -ne $seekScenario -and $seekScenario.duration_seconds -ge ($SeekMinutes * 60)) } else { [bool]($null -ne $seekScenario -and $seekScenario.reads -ge $SeekIterations) }
        Add-ThresholdCheck -Checks $thresholdChecks -Name 'interactive_seek_soak' -Passed $seekPassed -Observed $seekObserved -Threshold $seekThreshold
    }
    if (-not $SkipConcurrent) {
        $concurrentScenario = $scenarioResults | Where-Object { $_.scenario -eq 'concurrent_readers' } | Select-Object -First 1
        $observedReaderCount = if ($null -ne $concurrentScenario) { @($concurrentScenario.readers_detail).Count } else { $null }
        $readerCountPassed = [bool](
            $null -ne $concurrentScenario -and
            $null -ne $observedReaderCount -and
            $observedReaderCount -eq $ConcurrentReaders -and
            $observedReaderCount -ge 3
        )
        Add-ThresholdCheck -Checks $thresholdChecks -Name 'concurrent_reader_count' -Passed $readerCountPassed -Observed $observedReaderCount -Threshold $ConcurrentReaders
        if ($ConcurrentMinutes -gt 0) {
            $minObservedReaderReads = if ($null -ne $concurrentScenario) { (@($concurrentScenario.readers_detail | Measure-Object -Property reads -Minimum).Minimum) } else { $null }
            $concurrentDurationObserved = if ($null -ne $concurrentScenario) { $concurrentScenario.duration_seconds } else { $null }
            Add-ThresholdCheck -Checks $thresholdChecks -Name 'concurrent_pressure_duration' -Passed ([bool]($null -ne $concurrentScenario -and $concurrentScenario.duration_seconds -ge ($ConcurrentMinutes * 60))) -Observed $concurrentDurationObserved -Threshold ($ConcurrentMinutes * 60)
            Add-ThresholdCheck -Checks $thresholdChecks -Name 'concurrent_reader_activity' -Passed ([bool]($null -ne $concurrentScenario -and $minObservedReaderReads -gt 0)) -Observed $minObservedReaderReads -Threshold 'all readers produced reads for a timed run'
        }
        else {
            $concurrentReadsObserved = if ($null -ne $concurrentScenario) { $concurrentScenario.total_reads } else { $null }
            Add-ThresholdCheck -Checks $thresholdChecks -Name 'concurrent_iterations' -Passed ([bool]($null -ne $concurrentScenario -and $concurrentScenario.total_reads -ge ($ConcurrentReaders * $ConcurrentIterations))) -Observed $concurrentReadsObserved -Threshold ($ConcurrentReaders * $ConcurrentIterations)
        }
    }
    Add-ThresholdCheck -Checks $thresholdChecks -Name 'mount_survived' -Passed ([bool]($afterState.mount_exists -and ((-not $RequireFilmuvfs) -or $afterState.running))) -Observed ([bool]($afterState.mount_exists -and ((-not $RequireFilmuvfs) -or $afterState.running))) -Threshold $true
    if ($MaxReconnectIncidents -ge 0) {
        Add-ThresholdCheck -Checks $thresholdChecks -Name 'reconnect_incidents' -Passed ([bool]($logDiagnostics.reconnect_incidents -le $MaxReconnectIncidents)) -Observed $logDiagnostics.reconnect_incidents -Threshold $MaxReconnectIncidents
    }
    if ($MaxUnrecoveredStaleRefreshIncidents -ge 0) {
        Add-ThresholdCheck -Checks $thresholdChecks -Name 'unrecovered_stale_refresh_incidents' -Passed ([bool]($logDiagnostics.unrecovered_stale_refresh_incidents -le $MaxUnrecoveredStaleRefreshIncidents)) -Observed $logDiagnostics.unrecovered_stale_refresh_incidents -Threshold $MaxUnrecoveredStaleRefreshIncidents
        if ($runtimeDiagnostics.captured) {
            Add-ThresholdCheck -Checks $thresholdChecks -Name 'runtime_unrecovered_stale_refresh_incidents' -Passed ([bool]($runtimeDiagnostics.unrecovered_stale_refresh_incidents -le $MaxUnrecoveredStaleRefreshIncidents)) -Observed $runtimeDiagnostics.unrecovered_stale_refresh_incidents -Threshold $MaxUnrecoveredStaleRefreshIncidents
        }
    }
    if ($MaxCacheColdFetchIncidents -ge 0) {
        if ($runtimeDiagnostics.captured) {
            Add-ThresholdCheck -Checks $thresholdChecks -Name 'runtime_cache_cold_fetch_incidents' -Passed ([bool]($runtimeDiagnostics.cache_cold_fetch_incidents -le $MaxCacheColdFetchIncidents)) -Observed $runtimeDiagnostics.cache_cold_fetch_incidents -Threshold $MaxCacheColdFetchIncidents
        }
    }
    if ($MinCacheHitRatio -ge 0) {
        $observedCacheHitRatio = if ($runtimeDiagnostics.captured) { $runtimeDiagnostics.cache_hit_ratio } else { $null }
        Add-ThresholdCheck -Checks $thresholdChecks -Name 'runtime_cache_hit_ratio' -Passed ([bool]($runtimeDiagnostics.captured -and $runtimeDiagnostics.cache_hit_ratio -ge $MinCacheHitRatio)) -Observed $observedCacheHitRatio -Threshold $MinCacheHitRatio
    }
    if ($MaxProviderPressureIncidents -ge 0) {
        if ($runtimeDiagnostics.captured) {
            Add-ThresholdCheck -Checks $thresholdChecks -Name 'runtime_provider_pressure_incidents' -Passed ([bool]($runtimeDiagnostics.provider_pressure_incidents -le $MaxProviderPressureIncidents)) -Observed $runtimeDiagnostics.provider_pressure_incidents -Threshold $MaxProviderPressureIncidents
        }
    }
    if ($MaxFatalErrorIncidents -ge 0) {
        Add-ThresholdCheck -Checks $thresholdChecks -Name 'fatal_error_incidents' -Passed ([bool]($logDiagnostics.fatal_error_incidents -le $MaxFatalErrorIncidents)) -Observed $logDiagnostics.fatal_error_incidents -Threshold $MaxFatalErrorIncidents
        if ($runtimeDiagnostics.captured) {
            Add-ThresholdCheck -Checks $thresholdChecks -Name 'runtime_fatal_error_incidents' -Passed ([bool]($runtimeDiagnostics.fatal_error_incidents -le $MaxFatalErrorIncidents)) -Observed $runtimeDiagnostics.fatal_error_incidents -Threshold $MaxFatalErrorIncidents
        }
    }
    if ($runtimeDiagnostics.captured) {
        foreach ($pressureCheck in @(
            @{ Name = 'runtime_cache_pressure_class'; Observed = $runtimeDiagnostics.cache_pressure_class },
            @{ Name = 'runtime_chunk_coalescing_pressure_class'; Observed = $runtimeDiagnostics.chunk_coalescing_pressure_class },
            @{ Name = 'runtime_upstream_wait_class'; Observed = $runtimeDiagnostics.upstream_wait_class },
            @{ Name = 'runtime_refresh_pressure_class'; Observed = $runtimeDiagnostics.refresh_pressure_class }
        )) {
            Add-ThresholdCheck -Checks $thresholdChecks -Name $pressureCheck.Name -Passed ([string]$pressureCheck.Observed -ne 'critical') -Observed $pressureCheck.Observed -Threshold 'not_critical'
        }
    }
    $thresholdFailures = @($thresholdChecks | Where-Object { -not $_.passed })

    $summary = [ordered]@{
        soak_profile = $SoakProfile
        mount_path = $MountPath
        target_files = @($targetFiles)
        primary_file = $primaryFile
        remux_file = $remuxFile
        ffmpeg_path = $resolvedFfmpegPath
        before = $beforeState
        after = $afterState
        runtime_status_before = $runtimeStatusBefore
        runtime_status_after = $runtimeStatusAfter
        runtime_status_delta = $runtimeStatusDelta
        runtime_diagnostics = $runtimeDiagnostics
        backend_stream_status_before = $backendStatusBefore
        backend_stream_status_after = $backendStatusAfter
        backend_vfs_governance_delta = if ($backendStatusBefore.captured -and $backendStatusAfter.captured) {
            Get-VfsGovernanceDelta -BeforeGovernance $backendStatusBefore.governance -AfterGovernance $backendStatusAfter.governance
        } else {
            $null
        }
        backend_vfs_pressure_before = if ($backendStatusBefore.captured) {
            Get-BackendVfsPressureSnapshot -Governance $backendStatusBefore.governance
        } else {
            $null
        }
        backend_vfs_pressure_after = if ($backendStatusAfter.captured) {
            Get-BackendVfsPressureSnapshot -Governance $backendStatusAfter.governance
        } else {
            $null
        }
        scenarios = $scenarioResults
        mount_survived = [bool]($afterState.mount_exists -and ((-not $RequireFilmuvfs) -or $afterState.running))
        failed = $failed
        failure_message = $failureMessage
        thresholds = [ordered]@{
            sequential_minutes = if (-not $SkipSequential) { $SequentialMinutes } else { $null }
            seek_minutes = if (-not $SkipSeek -and $SeekMinutes -gt 0) { $SeekMinutes } else { $null }
            seek_iterations = if (-not $SkipSeek -and $SeekMinutes -le 0) { $SeekIterations } else { $null }
            concurrent_minutes = if (-not $SkipConcurrent -and $ConcurrentMinutes -gt 0) { $ConcurrentMinutes } else { $null }
            concurrent_iterations = if (-not $SkipConcurrent -and $ConcurrentMinutes -le 0) { $ConcurrentIterations } else { $null }
            concurrent_readers = if (-not $SkipConcurrent) { $ConcurrentReaders } else { $null }
            max_reconnect_incidents = if ($MaxReconnectIncidents -ge 0) { $MaxReconnectIncidents } else { $null }
            max_unrecovered_stale_refresh_incidents = if ($MaxUnrecoveredStaleRefreshIncidents -ge 0) { $MaxUnrecoveredStaleRefreshIncidents } else { $null }
            max_cache_cold_fetch_incidents = if ($MaxCacheColdFetchIncidents -ge 0) { $MaxCacheColdFetchIncidents } else { $null }
            min_cache_hit_ratio = if ($MinCacheHitRatio -ge 0) { $MinCacheHitRatio } else { $null }
            max_provider_pressure_incidents = if ($MaxProviderPressureIncidents -ge 0) { $MaxProviderPressureIncidents } else { $null }
            max_fatal_error_incidents = if ($MaxFatalErrorIncidents -ge 0) { $MaxFatalErrorIncidents } else { $null }
        }
        threshold_checks = $thresholdChecks
        threshold_failures = $thresholdFailures
        diagnostics = $logDiagnostics
        log_summary = [ordered]@{
            stdout = $stdoutSnapshot
            stderr = $stderrSnapshot
            callbacks = $callbacksSnapshot
        }
        generated_at = (Get-Date).ToString('o')
    }

    $summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8

    ($stdoutSnapshot.tail -join [Environment]::NewLine) | Set-Content -Path (Join-Path $artifactRoot 'filmuvfs-windows-stdout.tail.log') -Encoding UTF8
    ($stderrSnapshot.tail -join [Environment]::NewLine) | Set-Content -Path (Join-Path $artifactRoot 'filmuvfs-windows-stderr.tail.log') -Encoding UTF8
    ($callbacksSnapshot.tail -join [Environment]::NewLine) | Set-Content -Path (Join-Path $artifactRoot 'filmuvfs-windows-callbacks.tail.log') -Encoding UTF8
}

if ($failed -or -not $summary.mount_survived -or $thresholdFailures.Count -gt 0) {
    throw ("[windows-vfs-soak] FAIL. Summary: {0}" -f $summaryPath)
}

Write-Host ("[windows-vfs-soak] PASS. Summary: {0}" -f $summaryPath) -ForegroundColor Green





