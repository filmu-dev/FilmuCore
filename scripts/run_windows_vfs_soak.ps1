param(
    [string] $MountPath = '',
    [string] $TargetFile = '',
    [string] $RemuxTargetFile = '',
    [string] $FfmpegPath = '',
    [int] $SequentialMinutes = 10,
    [int] $SeekIterations = 24,
    [int] $ConcurrentReaders = 3,
    [int] $ConcurrentIterations = 48,
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
    [switch] $RequireFilmuvfs
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
        [Parameter(Mandatory = $true)][int] $Iterations,
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
        for ($index = 0; $index -lt $Iterations; $index++) {
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
        }
    }
    finally {
        $stream.Dispose()
    }

    [pscustomobject]@{
        scenario = 'seek_resume'
        file_path = $FilePath
        iterations = $Iterations
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
        [Parameter(Mandatory = $true)][int] $Iterations,
        [Parameter(Mandatory = $true)][int] $BlockSizeBytes,
        [Parameter(Mandatory = $true)][string] $MountPath,
        [switch] $RequireFilmuvfs
    )

    $jobScript = {
        param($Path, $Iterations, $BlockSizeBytes, $ReaderIndex)
        $buffer = New-Object byte[] $BlockSizeBytes
        $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
        $bytesRead = [int64]0
        $reads = 0
        $maxOffset = [Math]::Max([int64]0, $stream.Length - $BlockSizeBytes)
        try {
            for ($i = 0; $i -lt $Iterations; $i++) {
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
            }
            [pscustomobject]@{
                reader = $ReaderIndex
                file_path = $Path
                reads = $reads
                bytes_read = $bytesRead
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
        $jobs += Start-Job -ScriptBlock $jobScript -ArgumentList $filePath, $Iterations, $BlockSizeBytes, $index
    }

    try {
        $null = Wait-Job -Job $jobs -Timeout 600
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
            iterations = $Iterations
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

function Get-LogTailSnapshot {
    param(
        [Parameter(Mandatory = $true)][string] $Path,
        [int] $TailLines = 80
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return [pscustomobject]@{
            path = $Path
            exists = $false
            tail = @()
            stale_mentions = 0
            prefetch_mentions = 0
            error_mentions = 0
        }
    }

    $tail = @(Get-Content -LiteralPath $Path -Tail $TailLines -ErrorAction SilentlyContinue)
    [pscustomobject]@{
        path = $Path
        exists = $true
        tail = $tail
        stale_mentions = @($tail | Select-String -Pattern 'stale|ESTALE|inline refresh' -SimpleMatch:$false).Count
        prefetch_mentions = @($tail | Select-String -Pattern 'prefetch|chunk_engine.read plan' -SimpleMatch:$false).Count
        error_mentions = @($tail | Select-String -Pattern 'error|failed|panic|allocation|STATUS_' -SimpleMatch:$false).Count
    }
}

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

$artifactRoot = Join-Path (Get-StateDirectory) ("soak-{0}" -f (Get-Date -Format 'yyyyMMdd-HHmmss'))
New-Item -ItemType Directory -Path $artifactRoot -Force | Out-Null

$beforeState = Assert-LiveMount -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs
$targetFiles = Resolve-TargetFiles -MountPath $MountPath -ExplicitFile $TargetFile -DesiredCount ([Math]::Max($ConcurrentReaders, 1))
$primaryFile = $targetFiles[0]
$remuxFile = Resolve-RemuxTargetFile -MountPath $MountPath -ExplicitFile $RemuxTargetFile

$scenarioResults = [System.Collections.Generic.List[object]]::new()
$failed = $false
$failureMessage = $null

try {
    if (-not $SkipSequential) {
        $scenarioResults.Add((Invoke-SequentialScenario -FilePath $primaryFile -Duration ([TimeSpan]::FromMinutes($SequentialMinutes)) -BlockSizeBytes ($SequentialBlockSizeKb * 1024) -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs))
    }
    if (-not $SkipSeek) {
        $scenarioResults.Add((Invoke-SeekScenario -FilePath $primaryFile -Iterations $SeekIterations -ReadSizeBytes ($SeekReadSizeKb * 1024) -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs))
    }
    if (-not $SkipConcurrent) {
        $scenarioResults.Add((Invoke-ConcurrentScenario -FilePaths $targetFiles -Readers $ConcurrentReaders -Iterations $ConcurrentIterations -BlockSizeBytes ($ConcurrentBlockSizeKb * 1024) -MountPath $MountPath -RequireFilmuvfs:$RequireFilmuvfs))
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

$afterState = Get-FilmuvfsState -MountPath $MountPath
$stdoutSnapshot = Get-LogTailSnapshot -Path (Get-LogPath -Name 'filmuvfs-windows-stdout.log')
$stderrSnapshot = Get-LogTailSnapshot -Path (Get-LogPath -Name 'filmuvfs-windows-stderr.log')
$callbacksSnapshot = Get-LogTailSnapshot -Path (Get-LogPath -Name 'filmuvfs-windows-callbacks.log')

$summary = [ordered]@{
    mount_path = $MountPath
    target_files = $targetFiles
    primary_file = $primaryFile
    remux_file = $remuxFile
    ffmpeg_path = $resolvedFfmpegPath
    before = $beforeState
    after = $afterState
    scenarios = $scenarioResults
    mount_survived = [bool]($afterState.mount_exists -and ((-not $RequireFilmuvfs) -or $afterState.running))
    failed = $failed
    failure_message = $failureMessage
    log_summary = [ordered]@{
        stdout = $stdoutSnapshot
        stderr = $stderrSnapshot
        callbacks = $callbacksSnapshot
    }
    generated_at = (Get-Date).ToString('o')
}

$summaryPath = Join-Path $artifactRoot 'summary.json'
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8

($stdoutSnapshot.tail -join [Environment]::NewLine) | Set-Content -Path (Join-Path $artifactRoot 'filmuvfs-windows-stdout.tail.log') -Encoding UTF8
($stderrSnapshot.tail -join [Environment]::NewLine) | Set-Content -Path (Join-Path $artifactRoot 'filmuvfs-windows-stderr.tail.log') -Encoding UTF8
($callbacksSnapshot.tail -join [Environment]::NewLine) | Set-Content -Path (Join-Path $artifactRoot 'filmuvfs-windows-callbacks.tail.log') -Encoding UTF8

if ($failed -or -not $summary.mount_survived) {
    throw ("[windows-vfs-soak] FAIL. Summary: {0}" -f $summaryPath)
}

Write-Host ("[windows-vfs-soak] PASS. Summary: {0}" -f $summaryPath) -ForegroundColor Green





