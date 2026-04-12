param(
    [string] $TmdbId = '603',
    [string] $TvdbId = '',
    [string] $Title = 'The Matrix',
    [ValidateSet('movie', 'tv')]
    [string] $MediaType = 'movie',
    [ValidateSet('', 'plex', 'jellyfin', 'emby')]
    [string] $MediaServerProvider = '',
    [string] $MediaServerUrl = '',
    [string] $MediaServerToken = '',
    [string] $MediaServerRequestLogPath = '',
    [string] $JellyfinApiKey = '',
    [string] $JellyfinUserId = '',
    [string] $JellyfinLibraryId = '',
    [string] $JellyfinSearchTerm = '',
    [string] $BackendUrl = 'http://localhost:8000',
    [string] $FrontendUrl = 'http://localhost:3000',
    [string] $ApiKey = '',
    [string] $BackendActorId = 'automation:playback-proof',
    [string] $BackendActorType = 'service',
    [string] $BackendActorRoles = 'platform:admin,settings:write,playback:operator',
    [string] $BackendActorScopes = '',
    [string] $WslDistro = 'Ubuntu-22.04',
    [string] $MountRoot = '/mnt/filmuvfs',
    [int] $FrontendTimeoutSeconds = 60,
    [int] $AcquisitionTimeoutSeconds = 900,
    [int] $MediaServerTimeoutSeconds = 180,
    [int] $MountVisibilityTimeoutSeconds = 60,
    [int] $PollIntervalSeconds = 5,
    [int] $MountedReadBytes = 1048576,
    [switch] $ProofStaleDirectRefresh,
    [switch] $ReuseExistingItem,
    [switch] $SkipStart,
    [switch] $StopWhenDone,
    [switch] $DryRun
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

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
            $response = Invoke-WebRequest -Uri $Uri -UseBasicParsing -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 300) {
                return $true
            }
        }
        catch {
            Start-Sleep -Seconds 1
        }
    }

    return $false
}

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

function Invoke-WslBash {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Command
    )

    return & wsl.exe -d $WslDistro -u root -- bash -lc "$Command"
}


function Refresh-WslPersistentMount {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ArtifactDir
    )

    $startScript = "$script:WslRepoRoot/rust/filmuvfs/scripts/start_persistent_mount.sh"
    $preflightLogPath = Join-Path $ArtifactDir 'wsl-mount-preflight.txt'
    $quotedStartScript = ConvertTo-BashSingleQuoted -Value $startScript
    $output = & wsl.exe -d $WslDistro -u root -- bash -lc "tr -d '\r' < $quotedStartScript | bash" 2>&1
    (($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine) | Set-Content -Path $preflightLogPath -Encoding UTF8

    if ($LASTEXITCODE -ne 0) {
        throw "failed to refresh the WSL persistent mount via $startScript"
    }

    $quotedMountRoot = ConvertTo-BashSingleQuoted -Value $MountRoot
    $listingText = (Invoke-WslBash -Command "ls $quotedMountRoot 2>/dev/null || true" | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    if ($listingText -notmatch '(^|\r?\n)(movies|shows)(\r?\n|$)') {
        throw "WSL persistent mount preflight did not expose movies/shows under $MountRoot"
    }

    return [pscustomobject]@{
        log_path = '/tmp/filmuvfs_persistent.log'
        details  = 'Refreshed the WSL persistent mount so the mounted proof path uses the current filmuvfs binary and a fresh persistent log.'
    }
}
function ConvertTo-BashSingleQuoted {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Value
    )

    $singleQuote = [string][char]39
    $doubleQuote = [string][char]34
    $replacement = $singleQuote + $doubleQuote + $singleQuote + $doubleQuote + $singleQuote
    $escaped = $Value.Replace($singleQuote, $replacement)
    return $singleQuote + $escaped + $singleQuote
}

function Get-MediaServerTimeoutMilliseconds {
    $seconds = [Math]::Max(5, [int] $MediaServerTimeoutSeconds)
    return $seconds * 1000
}

function Write-JsonFile {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [object] $Value
    )

    $Value | ConvertTo-Json -Depth 20 | Set-Content -Path $Path -Encoding UTF8
}

function Convert-ToCompactJsonValue {
    param(
        [Parameter(Mandatory = $true)]
        [AllowNull()]
        [object] $Value
    )

    if ($null -eq $Value) {
        return 'null'
    }

    return (ConvertTo-Json -InputObject $Value -Compress)
}


function Get-DotEnvMap {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    $result = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $result
    }

    foreach ($rawLine in Get-Content -LiteralPath $Path) {
        $line = [string] $rawLine
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }

        $trimmed = $line.Trim()
        if ($trimmed.StartsWith('#')) {
            continue
        }

        $delimiterIndex = $trimmed.IndexOf('=')
        if ($delimiterIndex -lt 1) {
            continue
        }

        $name = $trimmed.Substring(0, $delimiterIndex).Trim()
        $value = $trimmed.Substring($delimiterIndex + 1)

        if ($value.Length -ge 2) {
            if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
                $value = $value.Substring(1, $value.Length - 2)
            }
        }

        $result[$name] = $value
    }

    return $result
}

function Get-TmdbApiKey {
    if (-not [string]::IsNullOrWhiteSpace([string] $env:TMDB_API_KEY)) {
        return [string] $env:TMDB_API_KEY
    }
    if ($script:DotEnv.ContainsKey('TMDB_API_KEY') -and -not [string]::IsNullOrWhiteSpace([string] $script:DotEnv['TMDB_API_KEY'])) {
        return [string] $script:DotEnv['TMDB_API_KEY']
    }
    return ''
}

function Resolve-TvdbIdForProof {
    param(
        [Parameter(Mandatory = $true)]
        [string] $TmdbId
    )

    if (-not [string]::IsNullOrWhiteSpace($TvdbId)) {
        return $TvdbId
    }

    $tmdbApiKey = Get-TmdbApiKey
    if ([string]::IsNullOrWhiteSpace($tmdbApiKey)) {
        throw 'TMDB_API_KEY is required to resolve a TVDB identifier for tv playback proof runs.'
    }

    $uri = "https://api.themoviedb.org/3/tv/$TmdbId/external_ids?api_key=$tmdbApiKey"
    $response = Invoke-RestMethod -Method Get -Uri $uri -TimeoutSec 20
    $resolvedTvdbId = [string] $response.tvdb_id
    if ([string]::IsNullOrWhiteSpace($resolvedTvdbId)) {
        throw ("Unable to resolve a TVDB identifier from TMDB id {0} for tv playback proof." -f $TmdbId)
    }
    return $resolvedTvdbId
}

function Write-SummaryFile {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path,
        [Parameter(Mandatory = $true)]
        [hashtable] $Summary
    )

    $summaryDir = Split-Path -Parent $Path
    $summaryStagePath = Join-Path $summaryDir 'summary-stage.txt'
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:start'

    $stepSnapshot = @(
        $script:StepResults | ForEach-Object {
            [pscustomobject]@{
                name        = $_.name
                status      = $_.status
                details     = $_.details
                recorded_at = $_.recorded_at
            }
        }
    )
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:step-snapshot'

    $timestampJson = Convert-ToCompactJsonValue -Value $Summary.timestamp
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:timestamp'
    $artifactDirJson = Convert-ToCompactJsonValue -Value $Summary.artifact_dir
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:artifact-dir'
    $parametersJson = ConvertTo-Json -InputObject $Summary.parameters -Depth 20
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:parameters'
    $movieJson = ConvertTo-Json -InputObject $Summary.movie -Depth 20
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:movie'
    $mountJson = ConvertTo-Json -InputObject $Summary.mount -Depth 20
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:mount'
    $mediaServerLines = @(
        '{'
        ('  "provider": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.provider))
        ('  "topology": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.topology))
        ('  "configured": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.configured))
        ('  "signal_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.signal_status))
        ('  "signal_container": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.signal_container))
        ('  "visibility_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.visibility_status))
        ('  "item_id": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.item_id))
        ('  "library_id": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.library_id))
        ('  "playback_info_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.playback_info_status))
        ('  "playback_media_source_id": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.playback_media_source_id))
        ('  "playback_path": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.playback_path))
        ('  "stream_open_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stream_open_status))
        ('  "stream_open_status_code": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stream_open_status_code))
        ('  "stream_open_bytes_read": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stream_open_bytes_read))
        ('  "stream_open_content_range": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stream_open_content_range))
        ('  "playback_start_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.playback_start_status))
        ('  "playback_start_status_code": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.playback_start_status_code))
        ('  "playback_start_bytes_read": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.playback_start_bytes_read))
        ('  "playback_start_content_type": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.playback_start_content_type))
        ('  "playback_start_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.playback_start_details))
        ('  "playback_start_log_path": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.playback_start_log_path))
        ('  "session_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.session_status))
        ('  "session_started_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.session_started_status))
        ('  "session_progress_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.session_progress_status))
        ('  "session_stopped_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.session_stopped_status))
        ('  "wsl_host_mount_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.wsl_host_mount_status))
        ('  "wsl_host_mount_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.wsl_host_mount_details))
        ('  "wsl_persistent_log_path": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.wsl_persistent_log_path))
        ('  "plex_wsl_evidence_path": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.plex_wsl_evidence_path))
        ('  "wsl_mount_check_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.wsl_mount_check_status))
        ('  "wsl_mount_check_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.wsl_mount_check_details))
        ('  "wsl_host_binary_check_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.wsl_host_binary_check_status))
        ('  "wsl_host_binary_check_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.wsl_host_binary_check_details))
        ('  "refresh_identity_check_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.refresh_identity_check_status))
        ('  "refresh_identity_check_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.refresh_identity_check_details))
        ('  "foreground_fetch_check_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.foreground_fetch_check_status))
        ('  "foreground_fetch_check_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.foreground_fetch_check_details))
        ('  "plex_wsl_evidence_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.plex_wsl_evidence_status))
        ('  "plex_wsl_evidence_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.plex_wsl_evidence_details))
        ('  "wsl_host_binary_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.wsl_host_binary_status))
        ('  "wsl_host_binary_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.wsl_host_binary_details))
        ('  "refresh_identity_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.refresh_identity_status))
        ('  "refresh_identity_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.refresh_identity_details))
        ('  "foreground_fetch_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.foreground_fetch_status))
        ('  "foreground_fetch_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.foreground_fetch_details))
        ('  "jellyfin_visibility_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_visibility_status))
        ('  "jellyfin_item_id": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_item_id))
        ('  "jellyfin_library_id": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_library_id))
        ('  "jellyfin_playback_info_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_playback_info_status))
        ('  "jellyfin_playback_media_source_id": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_playback_media_source_id))
        ('  "jellyfin_playback_path": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_playback_path))
        ('  "jellyfin_stream_open_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_stream_open_status))
        ('  "jellyfin_stream_open_status_code": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_stream_open_status_code))
        ('  "jellyfin_stream_open_bytes_read": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_stream_open_bytes_read))
        ('  "jellyfin_stream_open_content_range": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_stream_open_content_range))
        ('  "jellyfin_session_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_session_status))
        ('  "jellyfin_session_started_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_session_started_status))
        ('  "jellyfin_session_progress_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_session_progress_status))
        ('  "jellyfin_session_stopped_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_session_stopped_status))
        ('  "jellyfin_directstream_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_directstream_status))
        ('  "jellyfin_directstream_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_directstream_details))
        ('  "jellyfin_directstream_log_path": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_directstream_log_path))
        ('  "jellyfin_directstream_cached_codec": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_directstream_cached_codec))
        ('  "jellyfin_directstream_actual_codec": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_directstream_actual_codec))
        ('  "jellyfin_directstream_refresh_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_directstream_refresh_status))
        ('  "jellyfin_directstream_refresh_details": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_directstream_refresh_details))
        ('  "jellyfin_directstream_refreshed_codec": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.jellyfin_directstream_refreshed_codec))
        ('  "stale_refresh_status": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stale_refresh_status))
        ('  "stale_refresh_entry_id": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stale_refresh_entry_id))
        ('  "stale_refresh_initial_status_code": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stale_refresh_initial_status_code))
        ('  "stale_refresh_initial_body": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stale_refresh_initial_body))
        ('  "stale_refresh_route_status_code": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stale_refresh_route_status_code))
        ('  "stale_refresh_bytes_read": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stale_refresh_bytes_read))
        ('  "stale_refresh_content_range": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stale_refresh_content_range))
        ('  "stale_refresh_attempt_count": {0},' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stale_refresh_attempt_count))
        ('  "stale_refresh_recovered_url": {0}' -f (Convert-ToCompactJsonValue -Value $Summary.media_server.stale_refresh_recovered_url))
        '}'
    )
    $mediaServerJson = $mediaServerLines -join [Environment]::NewLine
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:media-server'
    $stepsJson = ConvertTo-Json -InputObject $stepSnapshot -Depth 20
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:steps-json'

    $json = @(
        '{'
        ('  "timestamp": {0},' -f $timestampJson)
        ('  "artifact_dir": {0},' -f $artifactDirJson)
        ('  "parameters": {0},' -f $parametersJson)
        ('  "movie": {0},' -f $movieJson)
        ('  "mount": {0},' -f $mountJson)
        ('  "media_server": {0},' -f $mediaServerJson)
        ('  "steps": {0}' -f $stepsJson)
        '}'
    ) -join [Environment]::NewLine
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:json-built'

    Set-Content -Path $Path -Encoding UTF8 -Value $json
    Set-Content -Path $summaryStagePath -Encoding UTF8 -Value 'stage:summary-written'
}

function Add-StepResult {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Name,
        [Parameter(Mandatory = $true)]
        [ValidateSet('passed', 'failed', 'skipped', 'pending')]
        [string] $Status,
        [string] $Details = ''
    )

    $script:StepResults.Add(
        [pscustomobject]@{
            name        = $Name
            status      = $Status
            details     = $Details
            recorded_at = (Get-Date).ToString('o')
        }
    )
}

function Get-BackendJson {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Uri
    )

    return Invoke-RestMethod -Method Get -Uri $Uri -Headers $script:BackendHeaders -TimeoutSec 15
}

function Get-BackendHeaders {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ApiKey,
        [string] $ActorId = '',
        [string] $ActorType = '',
        [string] $ActorRoles = '',
        [string] $ActorScopes = ''
    )

    $headers = @{ 'x-api-key' = $ApiKey }
    if (-not [string]::IsNullOrWhiteSpace($ActorId)) {
        $headers['x-actor-id'] = $ActorId
    }
    if (-not [string]::IsNullOrWhiteSpace($ActorType)) {
        $headers['x-actor-type'] = $ActorType
    }
    if (-not [string]::IsNullOrWhiteSpace($ActorRoles)) {
        $headers['x-actor-roles'] = $ActorRoles
    }
    if (-not [string]::IsNullOrWhiteSpace($ActorScopes)) {
        $headers['x-actor-scopes'] = $ActorScopes
    }
    return $headers
}

function Post-BackendJson {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Uri,
        [Parameter(Mandatory = $true)]
        [object] $Body
    )

    $jsonBody = $Body | ConvertTo-Json -Depth 10
    return Invoke-RestMethod -Method Post -Uri $Uri -Headers $script:BackendHeaders -ContentType 'application/json' -Body $jsonBody -TimeoutSec 30
}

function Delete-BackendJson {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Uri,
        [Parameter(Mandatory = $true)]
        [object] $Body
    )

    $jsonBody = $Body | ConvertTo-Json -Depth 10
    return Invoke-RestMethod -Method Delete -Uri $Uri -Headers $script:BackendHeaders -ContentType 'application/json' -Body $jsonBody -TimeoutSec 30
}

function Get-JellyfinJson {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Uri,
        [Parameter(Mandatory = $true)]
        [string] $ApiKey
    )

    return Invoke-RestMethod -Method Get -Uri $Uri -Headers @{ 'X-Emby-Token' = $ApiKey } -TimeoutSec $MediaServerTimeoutSeconds
}

function Resolve-JellyfinContext {
    if ([string]::IsNullOrWhiteSpace($JellyfinApiKey)) {
        return $null
    }

    $baseUrl = if ([string]::IsNullOrWhiteSpace($MediaServerUrl)) { if (-not [string]::IsNullOrWhiteSpace([string] $env:JELLYFIN_URL)) { [string] $env:JELLYFIN_URL.TrimEnd('/') } elseif ($script:DotEnv.ContainsKey('JELLYFIN_URL') -and -not [string]::IsNullOrWhiteSpace([string] $script:DotEnv['JELLYFIN_URL'])) { [string] $script:DotEnv['JELLYFIN_URL'].TrimEnd('/') } else { 'http://localhost:8096' } } else { $MediaServerUrl.TrimEnd('/') }
    $resolvedUserId = $JellyfinUserId
    if ([string]::IsNullOrWhiteSpace($resolvedUserId)) {
        $usersResponse = Get-JellyfinJson -Uri "$baseUrl/Users" -ApiKey $JellyfinApiKey
        $users = @($usersResponse)
        $resolvedUser = $users | Select-Object -First 1
        if ($null -eq $resolvedUser) {
            throw 'Unable to resolve a Jellyfin user for the playback proof.'
        }
        $resolvedUserId = [string] $resolvedUser.Id
    }

    $resolvedLibraryId = $JellyfinLibraryId
    if ([string]::IsNullOrWhiteSpace($resolvedLibraryId)) {
        $views = Get-JellyfinJson -Uri "$baseUrl/Users/$resolvedUserId/Views" -ApiKey $JellyfinApiKey
        $expectedCollectionType = if ($MediaType -eq 'tv') { 'tvshows' } else { 'movies' }
        $resolvedLibrary = @($views.Items) | Where-Object {
            $_.CollectionType -eq $expectedCollectionType -and $_.LocationType -eq 'FileSystem'
        } | Select-Object -First 1
        if ($null -eq $resolvedLibrary) {
            throw ("Unable to resolve a Jellyfin {0} library view for the playback proof." -f $MediaType)
        }
        $resolvedLibraryId = [string] $resolvedLibrary.Id
    }

    return [pscustomobject]@{
        base_url   = $baseUrl
        api_key    = $JellyfinApiKey
        user_id    = $resolvedUserId
        library_id = $resolvedLibraryId
    }
}

function Get-JellyfinVisibilitySignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $SearchTerm
    )

    $encodedSearch = [uri]::EscapeDataString($SearchTerm)
    $uri = "$($Context.base_url)/Users/$($Context.user_id)/Items?ParentId=$($Context.library_id)&Recursive=true&SearchTerm=$encodedSearch"
    $result = Get-JellyfinJson -Uri $uri -ApiKey $Context.api_key
    $items = @($result.Items)
    if ($items.Count -gt 0) {
        return [pscustomobject]@{
            status = 'visible'
            item   = $items[0]
        }
    }

    return $null
}

function Get-JellyfinPlaybackInfo {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $JellyfinItemId
    )

    $uri = "$($Context.base_url)/Items/$JellyfinItemId/PlaybackInfo?UserId=$($Context.user_id)"
    $result = Get-JellyfinJson -Uri $uri -ApiKey $Context.api_key
    $mediaSources = @($result.MediaSources)
    if ($mediaSources.Count -gt 0) {
        return [pscustomobject]@{
            status          = 'ready'
            media_source    = $mediaSources[0]
            play_session_id = [string] $result.PlaySessionId
        }
    }

    return $null
}

function Get-JellyfinStreamOpenSignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $JellyfinItemId,
        [Parameter(Mandatory = $true)]
        [string] $MediaSourceId,
        [Parameter(Mandatory = $true)]
        [string] $Container,
        [Parameter(Mandatory = $true)]
        [string] $PlaySessionId
    )

    $query = @(
        'container=' + [uri]::EscapeDataString($Container)
        'static=true'
        'mediaSourceId=' + [uri]::EscapeDataString($MediaSourceId)
        'playSessionId=' + [uri]::EscapeDataString($PlaySessionId)
        'api_key=' + [uri]::EscapeDataString($Context.api_key)
    ) -join '&'

    $uri = "$($Context.base_url)/Videos/$JellyfinItemId/stream?$query"
    $request = [System.Net.HttpWebRequest]::Create($uri)
    $request.Method = 'GET'
    $request.Timeout = Get-MediaServerTimeoutMilliseconds
    $request.ReadWriteTimeout = Get-MediaServerTimeoutMilliseconds
    $request.AddRange(0, 1023)

    try {
        $response = [System.Net.HttpWebResponse] $request.GetResponse()
        try {
            $contentRange = $response.Headers['Content-Range']
            $acceptRanges = $response.Headers['Accept-Ranges']
            $contentType = $response.ContentType
            $statusCode = [int] $response.StatusCode
            $stream = $response.GetResponseStream()
            $buffer = New-Object byte[] 1024
            $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
            return [pscustomobject]@{
                status        = 'opened'
                uri           = $uri
                status_code   = $statusCode
                bytes_read    = $bytesRead
                content_type  = $contentType
                content_range = $contentRange
                accept_ranges = $acceptRanges
            }
        }
        finally {
            $response.Close()
        }
    }
    catch [System.Net.WebException] {
        $response = $_.Exception.Response
        if ($null -ne $response) {
            $httpResponse = [System.Net.HttpWebResponse] $response
            try {
                return [pscustomobject]@{
                    status        = 'failed'
                    uri           = $uri
                    status_code   = [int] $httpResponse.StatusCode
                    bytes_read    = 0
                    content_type  = $httpResponse.ContentType
                    content_range = $httpResponse.Headers['Content-Range']
                    accept_ranges = $httpResponse.Headers['Accept-Ranges']
                }
            }
            finally {
                $httpResponse.Close()
            }
        }
        throw
    }
}

function Invoke-JellyfinSessionProof {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $JellyfinItemId,
        [Parameter(Mandatory = $true)]
        [string] $MediaSourceId,
        [Parameter(Mandatory = $true)]
        [string] $PlaySessionId,
        [Int64] $PositionTicks = 0
    )

    $headers = @{ 'X-Emby-Token' = $Context.api_key }
    $payload = @{
        ItemId        = $JellyfinItemId
        MediaSourceId = $MediaSourceId
        PlaySessionId = $PlaySessionId
        CanSeek       = $true
        PositionTicks = $PositionTicks
        IsPaused      = $false
        IsMuted       = $false
    } | ConvertTo-Json -Depth 6

    $results = [ordered]@{}
    $steps = [ordered]@{
        started  = 'Sessions/Playing'
        progress = 'Sessions/Playing/Progress'
        stopped  = 'Sessions/Playing/Stopped'
    }

    foreach ($entry in $steps.GetEnumerator()) {
        $uri = "$($Context.base_url)/$($entry.Value)"
        $response = Invoke-WebRequest -Method Post -Uri $uri -Headers $headers -ContentType 'application/json' -Body $payload -UseBasicParsing -TimeoutSec $MediaServerTimeoutSeconds
        $results[$entry.Key] = [int] $response.StatusCode
    }

    return [pscustomobject]@{
        status          = 'reported'
        started_status  = $results.started
        progress_status = $results.progress
        stopped_status  = $results.stopped
    }
}

function Get-JellyfinHostLogDirectory {
    $localAppData = [System.Environment]::GetEnvironmentVariable('LOCALAPPDATA')
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        return $null
    }

    return (Join-Path $localAppData 'jellyfin\log')
}

function Get-LatestJellyfinDirectStreamDiagnostic {
    param(
        [datetime] $Since = [datetime]::MinValue,
        [string] $ArtifactDir = ''
    )

    $logDirectory = Get-JellyfinHostLogDirectory
    if ([string]::IsNullOrWhiteSpace($logDirectory) -or (-not (Test-Path -LiteralPath $logDirectory))) {
        return $null
    }

    $logFile = Get-ChildItem -LiteralPath $logDirectory -Filter 'FFmpeg.DirectStream-*.log' -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -ge $Since } |
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
        $details = "Jellyfin cached codec '$cachedVideoCodec' but ffmpeg saw '$actualVideoCodec'; force a full Jellyfin metadata refresh for this item before treating the failure as VFS corruption"
    }
    elseif (($status -eq 'failed') -and (-not [string]::IsNullOrWhiteSpace($bitstreamFilter))) {
        $details = "DirectStream failed in ffmpeg with bitstream filter '$bitstreamFilter': $details"
    }

    if (-not [string]::IsNullOrWhiteSpace($ArtifactDir)) {
        Copy-Item -LiteralPath $logFile.FullName -Destination (Join-Path $ArtifactDir $logFile.Name) -Force
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

function Invoke-JellyfinMetadataRefresh {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $JellyfinItemId,
        [string] $ExpectedVideoCodec = ''
    )

    $refreshUri = "$($Context.base_url)/Items/$JellyfinItemId/Refresh?MetadataRefreshMode=FullRefresh&ImageRefreshMode=FullRefresh&ReplaceAllMetadata=true&ReplaceAllImages=true"
    $response = Invoke-WebRequest -Method Post -Uri $refreshUri -Headers @{ 'X-Emby-Token' = $Context.api_key } -UseBasicParsing -TimeoutSec $MediaServerTimeoutSeconds
    $statusCode = [int] $response.StatusCode

    $result = [ordered]@{
        status               = 'requested'
        details              = "Requested full Jellyfin metadata refresh for item $JellyfinItemId."
        refresh_status_code  = $statusCode
        refreshed_video_codec = $null
    }

    if (-not [string]::IsNullOrWhiteSpace($ExpectedVideoCodec)) {
        $deadline = (Get-Date).AddSeconds(90)
        while ((Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 5
            try {
                $playbackInfo = Get-JellyfinPlaybackInfo -Context $Context -JellyfinItemId $JellyfinItemId
                if ($null -eq $playbackInfo) {
                    continue
                }

                $videoStream = @($playbackInfo.media_source.MediaStreams | Where-Object { $_.Type -eq 1 }) | Select-Object -First 1
                if ($null -eq $videoStream) {
                    continue
                }

                $refreshedCodec = [string] $videoStream.Codec
                $result.refreshed_video_codec = $refreshedCodec
                if ((-not [string]::IsNullOrWhiteSpace($refreshedCodec)) -and $refreshedCodec.ToLowerInvariant() -eq $ExpectedVideoCodec.ToLowerInvariant()) {
                    $result.status = 'refreshed'
                    $result.details = "Jellyfin metadata refresh completed; cached codec now matches '$ExpectedVideoCodec'."
                    break
                }
            }
            catch {
            }
        }

        if ($result.status -ne 'refreshed') {
            $currentCodec = [string] $result.refreshed_video_codec
            if ([string]::IsNullOrWhiteSpace($currentCodec)) {
                $result.status = 'pending'
                $result.details = "Requested Jellyfin metadata refresh for item $JellyfinItemId, but codec verification did not complete within the polling window."
            }
            else {
                $result.status = 'pending'
                $result.details = "Requested Jellyfin metadata refresh for item $JellyfinItemId, but cached codec is still '$currentCodec' instead of '$ExpectedVideoCodec'."
            }
        }
    }

    return [pscustomobject] $result
}


function Get-EmbyBaseUrl {
    param(
        [Parameter(Mandatory = $true)]
        [string] $BaseUrl
    )

    $trimmed = $BaseUrl.TrimEnd('/')
    if ($trimmed.ToLowerInvariant().EndsWith('/emby')) {
        return $trimmed
    }

    return ($trimmed + '/emby')
}

function Get-EmbyJson {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Uri,
        [Parameter(Mandatory = $true)]
        [string] $ApiKey
    )

    return Invoke-RestMethod -Method Get -Uri $Uri -Headers @{ 'X-Emby-Token' = $ApiKey } -TimeoutSec $MediaServerTimeoutSeconds
}

function New-EmbyAuthorizationHeader {
    param(
        [string] $UserId = '',
        [string] $Token = ''
    )

    $parts = @(
        'Emby'
        ('UserId="{0}"' -f $UserId)
        'Client="FilmuPlaybackProof"'
        'Device="Codex"'
        'DeviceId="filmucore-playback-proof"'
        'Version="1.0.0"'
    )
    if (-not [string]::IsNullOrWhiteSpace($Token)) {
        $parts += ('Token="{0}"' -f $Token)
    }
    return ($parts -join ', ')
}

function Invoke-EmbyAuthenticateByName {
    param(
        [Parameter(Mandatory = $true)]
        [string] $BaseUrl,
        [Parameter(Mandatory = $true)]
        [string] $Username,
        [Parameter(Mandatory = $true)]
        [string] $Password
    )

    $headers = @{ 'X-Emby-Authorization' = (New-EmbyAuthorizationHeader) }
    $body = @{ Username = $Username; Pw = $Password } | ConvertTo-Json -Depth 4
    return Invoke-RestMethod -Method Post -Uri "$BaseUrl/Users/AuthenticateByName" -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec $MediaServerTimeoutSeconds
}

function Resolve-EmbyContext {
    if (($MediaServerProvider -ne 'emby') -and [string]::IsNullOrWhiteSpace($MediaServerToken)) {
        return $null
    }

    $rawBaseUrl = if (-not [string]::IsNullOrWhiteSpace($MediaServerUrl)) { $MediaServerUrl.TrimEnd('/') } elseif (-not [string]::IsNullOrWhiteSpace([string] $env:EMBY_URL)) { [string] $env:EMBY_URL.TrimEnd('/') } elseif ($script:DotEnv.ContainsKey('EMBY_URL') -and -not [string]::IsNullOrWhiteSpace([string] $script:DotEnv['EMBY_URL'])) { [string] $script:DotEnv['EMBY_URL'].TrimEnd('/') } else { 'http://localhost:8096' }
    $baseUrl = Get-EmbyBaseUrl -BaseUrl $rawBaseUrl
    $apiKey = if (-not [string]::IsNullOrWhiteSpace($MediaServerToken)) { $MediaServerToken } elseif (-not [string]::IsNullOrWhiteSpace([string] $env:EMBY_API_KEY)) { [string] $env:EMBY_API_KEY } elseif ($script:DotEnv.ContainsKey('EMBY_API_KEY')) { [string] $script:DotEnv['EMBY_API_KEY'] } else { '' }
    $resolvedUserId = if (-not [string]::IsNullOrWhiteSpace([string] $env:EMBY_USER_ID)) { [string] $env:EMBY_USER_ID } elseif ($script:DotEnv.ContainsKey('EMBY_USER_ID')) { [string] $script:DotEnv['EMBY_USER_ID'] } else { '' }

    if ([string]::IsNullOrWhiteSpace($apiKey)) {
        $username = if (-not [string]::IsNullOrWhiteSpace([string] $env:EMBY_USERNAME)) { [string] $env:EMBY_USERNAME } elseif ($script:DotEnv.ContainsKey('EMBY_USERNAME')) { [string] $script:DotEnv['EMBY_USERNAME'] } else { '' }
        $password = if (-not [string]::IsNullOrWhiteSpace([string] $env:EMBY_PASSWORD)) { [string] $env:EMBY_PASSWORD } elseif ($script:DotEnv.ContainsKey('EMBY_PASSWORD')) { [string] $script:DotEnv['EMBY_PASSWORD'] } else { '' }
        if ([string]::IsNullOrWhiteSpace($username) -or [string]::IsNullOrWhiteSpace($password)) {
            return $null
        }

        $authResult = Invoke-EmbyAuthenticateByName -BaseUrl $baseUrl -Username $username -Password $password
        $apiKey = [string] $authResult.AccessToken
        if ([string]::IsNullOrWhiteSpace($resolvedUserId) -and $null -ne $authResult.User) {
            $resolvedUserId = [string] $authResult.User.Id
        }
        if ([string]::IsNullOrWhiteSpace($MediaServerToken)) {
            $script:MediaServerToken = $apiKey
        }
    }

    if ([string]::IsNullOrWhiteSpace($apiKey)) {
        return $null
    }

    if ([string]::IsNullOrWhiteSpace($resolvedUserId)) {
        $usersResponse = Get-EmbyJson -Uri "$baseUrl/Users/Public" -ApiKey $apiKey
        $users = @($usersResponse)
        $resolvedUser = $users | Select-Object -First 1
        if ($null -eq $resolvedUser) {
            $usersResponse = Get-EmbyJson -Uri "$baseUrl/Users" -ApiKey $apiKey
            $users = @($usersResponse)
            $resolvedUser = $users | Select-Object -First 1
        }
        if ($null -eq $resolvedUser) {
            throw 'Unable to resolve an Emby user for the playback proof.'
        }
        $resolvedUserId = [string] $resolvedUser.Id
    }

    $resolvedLibraryId = if (-not [string]::IsNullOrWhiteSpace([string] $env:EMBY_LIBRARY_ID)) { [string] $env:EMBY_LIBRARY_ID } elseif ($script:DotEnv.ContainsKey('EMBY_LIBRARY_ID')) { [string] $script:DotEnv['EMBY_LIBRARY_ID'] } else { '' }
    if ([string]::IsNullOrWhiteSpace($resolvedLibraryId)) {
        $views = Get-EmbyJson -Uri "$baseUrl/Users/$resolvedUserId/Views" -ApiKey $apiKey
        $expectedCollectionType = if ($MediaType -eq 'tv') { 'tvshows' } else { 'movies' }
        $resolvedLibrary = @($views.Items) | Where-Object {
            $locationType = if ($_.PSObject.Properties.Name -contains 'LocationType') { [string] $_.LocationType } else { '' }
            $_.CollectionType -eq $expectedCollectionType -and ($locationType -eq 'FileSystem' -or [string]::IsNullOrWhiteSpace($locationType))
        } | Select-Object -First 1
        if ($null -eq $resolvedLibrary) {
            throw ("Unable to resolve an Emby {0} library view for the playback proof." -f $MediaType)
        }
        $resolvedLibraryId = [string] $resolvedLibrary.Id
    }

    return [pscustomobject]@{
        provider   = 'emby'
        base_url   = $baseUrl
        api_key    = $apiKey
        user_id    = $resolvedUserId
        library_id = $resolvedLibraryId
    }
}

function Get-EmbyVisibilitySignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $SearchTerm
    )

    $encodedSearch = [uri]::EscapeDataString($SearchTerm)
    $uri = "$($Context.base_url)/Users/$($Context.user_id)/Items?ParentId=$($Context.library_id)&Recursive=true&SearchTerm=$encodedSearch"
    $result = Get-EmbyJson -Uri $uri -ApiKey $Context.api_key
    $items = @($result.Items)
    if ($items.Count -gt 0) {
        return [pscustomobject]@{
            status = 'visible'
            item   = $items[0]
        }
    }

    return $null
}

function Get-EmbyPlaybackInfo {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $EmbyItemId
    )

    $uri = "$($Context.base_url)/Items/$EmbyItemId/PlaybackInfo?UserId=$($Context.user_id)"
    $result = Get-EmbyJson -Uri $uri -ApiKey $Context.api_key
    $mediaSources = @($result.MediaSources)
    if ($mediaSources.Count -gt 0) {
        return [pscustomobject]@{
            status          = 'ready'
            media_source    = $mediaSources[0]
            play_session_id = [string] $result.PlaySessionId
        }
    }

    return $null
}

function Get-EmbyStreamOpenSignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $EmbyItemId,
        [Parameter(Mandatory = $true)]
        [object] $MediaSource,
        [Parameter(Mandatory = $true)]
        [string] $PlaySessionId
    )

    $directStreamUrl = if ($MediaSource.PSObject.Properties.Name -contains 'DirectStreamUrl') { [string] $MediaSource.DirectStreamUrl } else { '' }
    if ([string]::IsNullOrWhiteSpace($directStreamUrl)) {
        $container = [string] $MediaSource.Container
        if ([string]::IsNullOrWhiteSpace($container)) {
            $container = 'mkv'
        }
        $directStreamUrl = "/Videos/$EmbyItemId/stream.${container}?static=true&MediaSourceId=$([uri]::EscapeDataString([string] $MediaSource.Id))&PlaySessionId=$([uri]::EscapeDataString($PlaySessionId))&UserId=$([uri]::EscapeDataString($Context.user_id))"
    }

    $uri = if ($directStreamUrl.StartsWith('http://') -or $directStreamUrl.StartsWith('https://')) { $directStreamUrl } else { "$($Context.base_url.TrimEnd('/'))/$($directStreamUrl.TrimStart('/'))" }
    if ($uri -notmatch 'api_key=') {
        $separator = if ($uri.Contains('?')) { '&' } else { '?' }
        $uri = "$uri${separator}api_key=$([uri]::EscapeDataString($Context.api_key))"
    }

    $request = [System.Net.HttpWebRequest]::Create($uri)
    $request.Method = 'GET'
    $request.Timeout = Get-MediaServerTimeoutMilliseconds
    $request.ReadWriteTimeout = Get-MediaServerTimeoutMilliseconds
    $request.AddRange(0, 1023)

    try {
        $response = [System.Net.HttpWebResponse] $request.GetResponse()
        try {
            $contentRange = $response.Headers['Content-Range']
            $statusCode = [int] $response.StatusCode
            $stream = $response.GetResponseStream()
            $buffer = New-Object byte[] 1024
            $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
            return [pscustomobject]@{
                status        = 'opened'
                uri           = $uri
                status_code   = $statusCode
                bytes_read    = $bytesRead
                content_type  = $response.ContentType
                content_range = $contentRange
                accept_ranges = $response.Headers['Accept-Ranges']
            }
        }
        finally {
            $response.Close()
        }
    }
    catch [System.Net.WebException] {
        $response = $_.Exception.Response
        if ($null -ne $response) {
            $httpResponse = [System.Net.HttpWebResponse] $response
            try {
                return [pscustomobject]@{
                    status        = 'failed'
                    uri           = $uri
                    status_code   = [int] $httpResponse.StatusCode
                    bytes_read    = 0
                    content_type  = $httpResponse.ContentType
                    content_range = $httpResponse.Headers['Content-Range']
                    accept_ranges = $httpResponse.Headers['Accept-Ranges']
                }
            }
            finally {
                $httpResponse.Close()
            }
        }
        throw
    }
}

function Invoke-EmbySessionProof {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $EmbyItemId,
        [Parameter(Mandatory = $true)]
        [string] $MediaSourceId,
        [Parameter(Mandatory = $true)]
        [string] $PlaySessionId,
        [Int64] $PositionTicks = 0
    )

    $headers = @{ 'X-Emby-Token' = $Context.api_key }
    $payload = @{
        ItemId        = $EmbyItemId
        MediaSourceId = $MediaSourceId
        PlaySessionId = $PlaySessionId
        CanSeek       = $true
        PositionTicks = $PositionTicks
        IsPaused      = $false
        IsMuted       = $false
    } | ConvertTo-Json -Depth 6

    $results = [ordered]@{}
    $steps = [ordered]@{
        started  = 'Sessions/Playing'
        progress = 'Sessions/Playing/Progress'
        stopped  = 'Sessions/Playing/Stopped'
    }

    foreach ($entry in $steps.GetEnumerator()) {
        $uri = "$($Context.base_url)/$($entry.Value)"
        $response = Invoke-WebRequest -Method Post -Uri $uri -Headers $headers -ContentType 'application/json' -Body $payload -UseBasicParsing -TimeoutSec $MediaServerTimeoutSeconds
        $results[$entry.Key] = [int] $response.StatusCode
    }

    return [pscustomobject]@{
        status          = 'reported'
        started_status  = $results.started
        progress_status = $results.progress
        stopped_status  = $results.stopped
    }
}

function Test-IsNativePlexUrl {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Url
    )

    return [bool] ($Url -match '://(localhost|127\.0\.0\.1):32400(?:/|$)')
}

function Get-NativePlexUrl {
    return 'http://127.0.0.1:32400'
}

function Get-NativePlexLocalAdminToken {
    $localAppData = [System.Environment]::GetEnvironmentVariable('LOCALAPPDATA')
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        return ''
    }

    $tokenPath = Join-Path $localAppData 'Plex Media Server\.LocalAdminToken'
    if (-not (Test-Path -LiteralPath $tokenPath)) {
        return ''
    }

    return [string] (Get-Content -LiteralPath $tokenPath -Raw).Trim()
}

function Get-NativePlexLogPath {
    $localAppData = [System.Environment]::GetEnvironmentVariable('LOCALAPPDATA')
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        return $null
    }

    $logPath = Join-Path $localAppData 'Plex Media Server\Logs\Plex Media Server.log'
    if (Test-Path -LiteralPath $logPath) {
        return $logPath
    }

    return $null
}


function Get-NativePlexSetupState {
    $localAppData = [System.Environment]::GetEnvironmentVariable('LOCALAPPDATA')
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        return $null
    }

    $setupPath = Join-Path $localAppData 'Plex Media Server\Setup Plex.html'
    if (-not (Test-Path -LiteralPath $setupPath)) {
        return $null
    }

    $content = [string] (Get-Content -LiteralPath $setupPath -Raw)
    $redirectUrl = ''
    $match = [regex]::Match($content, 'URL=(?<url>[^"]+)', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($match.Success) {
        $redirectUrl = [string] $match.Groups['url'].Value.Trim()
    }

    return [pscustomobject]@{
        setup_path = $setupPath
        redirect_url = $redirectUrl
        awaiting_claim = ($redirectUrl -match '/await(?:$|[/?#])')
    }
}
function Get-WebExceptionResponseBody {
    param(
        [object] $Response
    )

    if ($null -eq $Response) {
        return ''
    }

    if ($Response -is [System.Net.Http.HttpResponseMessage]) {
        try {
            if ($null -ne $Response.Content) {
                return [string] $Response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            }

            return ''
        }
        catch {
            return ''
        }
    }

    if ($Response.PSObject.Methods.Name -contains 'GetResponseStream') {
        try {
            $stream = $Response.GetResponseStream()
            if ($null -eq $stream) {
                return ''
            }

            $reader = New-Object System.IO.StreamReader($stream)
            try {
                return $reader.ReadToEnd()
            }
            finally {
                $reader.Dispose()
            }
        }
        catch {
            return ''
        }
    }

    return ''
}

function Invoke-PlexWebRequest {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Uri,
        [Parameter(Mandatory = $true)]
        [string] $Token,
        [int] $TimeoutSec = 20,
        [int] $ReadyWaitSec = 240,
        [int] $PollIntervalSec = 5
    )

    if (($ReadyWaitSec -eq 240) -and (-not [string]::IsNullOrWhiteSpace([string] $env:FILMU_PLEX_READY_WAIT_SECONDS))) {
        $ReadyWaitSec = [int] $env:FILMU_PLEX_READY_WAIT_SECONDS
    }

    $deadline = (Get-Date).AddSeconds($ReadyWaitSec)

    while ($true) {
        try {
            return Invoke-WebRequest -Method Get -Uri $Uri -Headers @{ 'X-Plex-Token' = $Token } -UseBasicParsing -TimeoutSec $TimeoutSec
        }
        catch {
            $response = $null
            if ($_.Exception.PSObject.Properties.Name -contains 'Response') {
                $response = $_.Exception.Response
            }

            $statusCode = $null
            if (($null -ne $response) -and ($response.PSObject.Properties.Name -contains 'StatusCode')) {
                $statusCode = [int] $response.StatusCode
            }

            $responseBody = if (($_.ErrorDetails -ne $null) -and (-not [string]::IsNullOrWhiteSpace([string] $_.ErrorDetails.Message))) { [string] $_.ErrorDetails.Message } else { Get-WebExceptionResponseBody -Response $response }
            $isMaintenance = ($statusCode -eq 503) -and (($responseBody -match 'startup maintenance tasks') -or ($responseBody -match 'title="Maintenance"') -or ($responseBody -match 'currently running startup maintenance'))
            if ($isMaintenance -and (Test-IsNativePlexUrl -Url $Uri) -and ((Get-Date) -lt $deadline)) {
                Start-Sleep -Seconds $PollIntervalSec
                continue
            }

            if ($isMaintenance -and (Test-IsNativePlexUrl -Url $Uri)) {
                $logPath = Get-NativePlexLogPath
                $setupState = Get-NativePlexSetupState
                $bodySnippet = ($responseBody -replace '\\s+', ' ').Trim()
                if ($bodySnippet.Length -gt 200) {
                    $bodySnippet = $bodySnippet.Substring(0, 200)
                }

                $message = "Native Plex at $Uri stayed in startup maintenance for $ReadyWaitSec seconds."
                if (($null -ne $setupState) -and $setupState.awaiting_claim) {
                    $message += " The local Plex install still appears to be in the initial claim/setup flow."
                }
                if (($null -ne $setupState) -and (-not [string]::IsNullOrWhiteSpace([string] $setupState.redirect_url))) {
                    $message += " Setup redirect: $($setupState.redirect_url)"
                }
                if (($null -ne $setupState) -and (-not [string]::IsNullOrWhiteSpace([string] $setupState.setup_path))) {
                    $message += " Setup file: $($setupState.setup_path)"
                }
                if (-not [string]::IsNullOrWhiteSpace([string] $logPath)) {
                    $message += " Log: $logPath."
                }
                if (-not [string]::IsNullOrWhiteSpace($bodySnippet)) {
                    $message += " Last response: $bodySnippet"
                }

                throw $message
            }

            throw
        }
    }
}

function Get-PlexXml {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Uri,
        [Parameter(Mandatory = $true)]
        [string] $Token
    )

    $response = Invoke-PlexWebRequest -Uri $Uri -Token $Token
    return [xml] $response.Content
}

function Resolve-PlexContext {
    if (($MediaServerProvider -ne 'plex') -and [string]::IsNullOrWhiteSpace($MediaServerToken)) {
        return $null
    }

    $baseUrl = if (-not [string]::IsNullOrWhiteSpace($MediaServerUrl)) { $MediaServerUrl.TrimEnd('/') } elseif (-not [string]::IsNullOrWhiteSpace([string] $env:PLEX_URL)) { [string] $env:PLEX_URL.TrimEnd('/') } elseif ($script:DotEnv.ContainsKey('PLEX_URL') -and -not [string]::IsNullOrWhiteSpace([string] $script:DotEnv['PLEX_URL'])) { [string] $script:DotEnv['PLEX_URL'].TrimEnd('/') } else { Get-NativePlexUrl }
    $localAdminToken = if (Test-IsNativePlexUrl -Url $baseUrl) { Get-NativePlexLocalAdminToken } else { '' }
    $token = if (-not [string]::IsNullOrWhiteSpace($MediaServerToken)) { $MediaServerToken } elseif (-not [string]::IsNullOrWhiteSpace($localAdminToken)) { $localAdminToken } elseif (-not [string]::IsNullOrWhiteSpace([string] $env:PLEX_TOKEN)) { [string] $env:PLEX_TOKEN } elseif ($script:DotEnv.ContainsKey('PLEX_TOKEN')) { [string] $script:DotEnv['PLEX_TOKEN'] } else { '' }
    if ([string]::IsNullOrWhiteSpace($token)) {
        return $null
    }

    $resolvedLibraryId = if (-not [string]::IsNullOrWhiteSpace([string] $env:PLEX_LIBRARY_ID)) { [string] $env:PLEX_LIBRARY_ID } elseif ($script:DotEnv.ContainsKey('PLEX_LIBRARY_ID')) { [string] $script:DotEnv['PLEX_LIBRARY_ID'] } else { '' }
    if ([string]::IsNullOrWhiteSpace($resolvedLibraryId)) {
        $sectionsXml = Get-PlexXml -Uri "$baseUrl/library/sections" -Token $token
        $expectedType = if ($MediaType -eq 'tv') { 'show' } else { 'movie' }
        $resolvedLibrary = @($sectionsXml.MediaContainer.Directory) | Where-Object { [string] $_.type -eq $expectedType } | Select-Object -First 1
        if ($null -eq $resolvedLibrary) {
            throw ("Unable to resolve a Plex {0} library section for the playback proof." -f $MediaType)
        }
        $resolvedLibraryId = [string] $resolvedLibrary.key
    }

    return [pscustomobject]@{
        provider   = 'plex'
        base_url   = $baseUrl
        api_key    = $token
        library_id = $resolvedLibraryId
    }
}

function Get-PlexVisibilitySignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $SearchTerm
    )

    $sectionPath = if ($MediaType -eq 'tv') { 'allLeaves' } else { 'all' }
    $uri = "$($Context.base_url)/library/sections/$($Context.library_id)/$sectionPath"
    $xml = Get-PlexXml -Uri $uri -Token $Context.api_key
    $target = $SearchTerm.ToLowerInvariant()
    $normalizedTarget = ($target -replace '^[^a-z0-9]+|[^a-z0-9]+$', '') -replace '[^a-z0-9]+', ' '
    $articleTrimmedTarget = ($normalizedTarget -replace '^(the|a|an)\s+', '').Trim()
    $items = @()
    if ($xml.MediaContainer.PSObject.Properties.Name -contains 'Video') {
        $items = @($xml.MediaContainer.Video)
    }
    if (($items.Count -eq 0) -and ($xml.MediaContainer.PSObject.Properties.Name -contains 'Directory')) {
        $items = @($xml.MediaContainer.Directory)
    }

    $match = $items | Where-Object {
        $title = [string] $_.title
        $normalizedTitle = (($title.ToLowerInvariant() -replace '^[^a-z0-9]+|[^a-z0-9]+$', '') -replace '[^a-z0-9]+', ' ').Trim()
        $articleTrimmedTitle = ($normalizedTitle -replace '^(the|a|an)\s+', '').Trim()
        $filePath = ''
        if ($_.PSObject.Properties.Name -contains 'Media') {
            $firstMedia = @($_.Media) | Select-Object -First 1
            if (($null -ne $firstMedia) -and ($firstMedia.PSObject.Properties.Name -contains 'Part')) {
                $firstPart = @($firstMedia.Part) | Select-Object -First 1
                if ($null -ne $firstPart) {
                    $filePath = [string] $firstPart.file
                }
            }
        }
        $normalizedFilePath = (($filePath.ToLowerInvariant() -replace '[\\/_\.\-\(\)\[\]]+', ' ') -replace '\s+', ' ').Trim()

        $title.ToLowerInvariant().Contains($target) -or
        $normalizedTitle.Contains($normalizedTarget) -or
        (($articleTrimmedTarget.Length -gt 0) -and $articleTrimmedTitle.Contains($articleTrimmedTarget)) -or
        (($normalizedTarget.Length -gt 0) -and $normalizedFilePath.Contains($normalizedTarget))
    } | Select-Object -First 1

    if ($null -ne $match) {
        return [pscustomobject]@{
            status = 'visible'
            item   = [pscustomobject]@{
                Id        = [string] $match.ratingKey
                Title     = [string] $match.title
                RatingKey = [string] $match.ratingKey
                Key       = [string] $match.key
            }
        }
    }

    return $null
}

function Get-PlexPlaybackInfo {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $PlexItemId
    )

    $uri = "$($Context.base_url)/library/metadata/$PlexItemId"
    $xml = Get-PlexXml -Uri $uri -Token $Context.api_key
    $video = @($xml.MediaContainer.Video) | Select-Object -First 1
    if ($null -eq $video) {
        return $null
    }

    $media = @($video.Media) | Select-Object -First 1
    $part = if ($null -ne $media) { @($media.Part) | Select-Object -First 1 } else { $null }
    if ($null -eq $part) {
        return $null
    }

    return [pscustomobject]@{
        status          = 'ready'
        media_source    = [pscustomobject]@{
            Id         = [string] $part.id
            Path       = [string] $part.file
            Container  = [string] $media.container
            StreamKey  = [string] $part.key
        }
        play_session_id = $null
    }
}

function Get-PlexStreamOpenSignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [object] $MediaSource
    )

    $streamKey = [string] $MediaSource.StreamKey
    if ([string]::IsNullOrWhiteSpace($streamKey)) {
        return [pscustomobject]@{
            status        = 'failed'
            uri           = $null
            status_code   = $null
            bytes_read    = 0
            content_type  = $null
            content_range = $null
            accept_ranges = $null
        }
    }

    $uri = if ($streamKey.StartsWith('http://') -or $streamKey.StartsWith('https://')) { $streamKey } else { "$($Context.base_url.TrimEnd('/'))$streamKey" }
    if ($uri -notmatch 'X-Plex-Token=') {
        $separator = if ($uri.Contains('?')) { '&' } else { '?' }
        $uri = "$uri${separator}X-Plex-Token=$([uri]::EscapeDataString($Context.api_key))"
    }

    $request = [System.Net.HttpWebRequest]::Create($uri)
    $request.Method = 'GET'
    $request.Timeout = Get-MediaServerTimeoutMilliseconds
    $request.ReadWriteTimeout = Get-MediaServerTimeoutMilliseconds
    $request.AddRange(0, 1023)
    $request.Headers['X-Plex-Token'] = $Context.api_key

    try {
        $response = [System.Net.HttpWebResponse] $request.GetResponse()
        try {
            $contentRange = $response.Headers['Content-Range']
            $statusCode = [int] $response.StatusCode
            $stream = $response.GetResponseStream()
            $buffer = New-Object byte[] 1024
            $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
            return [pscustomobject]@{
                status        = 'opened'
                uri           = $uri
                status_code   = $statusCode
                bytes_read    = $bytesRead
                content_type  = $response.ContentType
                content_range = $contentRange
                accept_ranges = $response.Headers['Accept-Ranges']
            }
        }
        finally {
            $response.Close()
        }
    }
    catch [System.Net.WebException] {
        $response = $_.Exception.Response
        if ($null -ne $response) {
            $httpResponse = [System.Net.HttpWebResponse] $response
            try {
                return [pscustomobject]@{
                    status        = 'failed'
                    uri           = $uri
                    status_code   = [int] $httpResponse.StatusCode
                    bytes_read    = 0
                    content_type  = $httpResponse.ContentType
                    content_range = $httpResponse.Headers['Content-Range']
                    accept_ranges = $httpResponse.Headers['Accept-Ranges']
                }
            }
            finally {
                $httpResponse.Close()
            }
        }
        throw
    }
}

function Get-MediaServerTopology {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [string] $PlaybackPath = ''
    )

    $normalizedPath = [string] $PlaybackPath
    $baseUrl = [string] $Context.base_url
    switch ([string] $Context.provider) {
        'plex' {
            if ($normalizedPath -like '/mnt/filmuvfs/*') { return 'docker_wsl' }
            if ($normalizedPath -match '^[A-Za-z]:\\FilmuCoreVFS(\\|$)') { return 'native_windows' }
            if ($baseUrl -match '://localhost:32401/?$') { return 'docker_wsl' }
            if ($baseUrl -match '://(localhost|127\.0\.0\.1):32400/?$') { return 'native_windows' }
            return 'unknown'
        }
        'emby' {
            if ($normalizedPath -like '/mnt/filmuvfs/*') { return 'docker_wsl' }
            if ($normalizedPath -match '^[A-Za-z]:\\FilmuCoreVFS(\\|$)') { return 'native_windows' }
            if ($baseUrl -match '://localhost:8097/?$') { return 'docker_wsl' }
            if ($baseUrl -match '://(localhost|127\.0\.0\.1):8096(/emby)?/?$') { return 'native_windows' }
            return 'unknown'
        }
        'jellyfin' {
            if ($normalizedPath -like '/mnt/filmuvfs/*') { return 'docker_wsl' }
            if ($normalizedPath -match '^[A-Za-z]:\\FilmuCoreVFS(\\|$)') { return 'native_windows' }
            return 'unknown'
        }
        default {
            return 'unknown'
        }
    }
}

function Invoke-HttpPlaybackStartSignal {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Uri,
        [hashtable] $Headers = @{},
        [int] $ReadBytes = 65536
    )

    $request = [System.Net.HttpWebRequest]::Create($Uri)
    $request.Method = 'GET'
    $request.Timeout = Get-MediaServerTimeoutMilliseconds
    $request.ReadWriteTimeout = Get-MediaServerTimeoutMilliseconds
    foreach ($entry in $Headers.GetEnumerator()) {
        $request.Headers[[string] $entry.Key] = [string] $entry.Value
    }

    try {
        $response = [System.Net.HttpWebResponse] $request.GetResponse()
        try {
            $stream = $response.GetResponseStream()
            $buffer = New-Object byte[] $ReadBytes
            $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
            return [pscustomobject]@{
                status        = 'started'
                uri           = $Uri
                status_code   = [int] $response.StatusCode
                bytes_read    = $bytesRead
                content_type  = $response.ContentType
                content_range = $response.Headers['Content-Range']
                details       = "Started playback route and read $bytesRead bytes."
            }
        }
        finally {
            $response.Close()
        }
    }
    catch [System.Net.WebException] {
        $response = $_.Exception.Response
        if ($null -ne $response) {
            $httpResponse = [System.Net.HttpWebResponse] $response
            try {
                return [pscustomobject]@{
                    status        = 'failed'
                    uri           = $Uri
                    status_code   = [int] $httpResponse.StatusCode
                    bytes_read    = 0
                    content_type  = $httpResponse.ContentType
                    content_range = $httpResponse.Headers['Content-Range']
                    details       = "Playback-start route returned HTTP $([int] $httpResponse.StatusCode)."
                }
            }
            finally {
                $httpResponse.Close()
            }
        }
        throw
    }
}

function Get-JellyfinPlaybackStartSignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $JellyfinItemId,
        [Parameter(Mandatory = $true)]
        [object] $PlaybackInfo
    )

    $query = @(
        'container=' + [uri]::EscapeDataString([string] $PlaybackInfo.media_source.Container)
        'static=true'
        'mediaSourceId=' + [uri]::EscapeDataString([string] $PlaybackInfo.media_source.Id)
        'playSessionId=' + [uri]::EscapeDataString([string] $PlaybackInfo.play_session_id)
        'api_key=' + [uri]::EscapeDataString($Context.api_key)
    ) -join '&'
    $uri = "$($Context.base_url)/Videos/$JellyfinItemId/stream?$query"
    return Invoke-HttpPlaybackStartSignal -Uri $uri
}

function Resolve-EmbyStreamUri {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $EmbyItemId,
        [Parameter(Mandatory = $true)]
        [object] $MediaSource,
        [Parameter(Mandatory = $true)]
        [string] $PlaySessionId
    )

    $directStreamUrl = if ($MediaSource.PSObject.Properties.Name -contains 'DirectStreamUrl') { [string] $MediaSource.DirectStreamUrl } else { '' }
    if ([string]::IsNullOrWhiteSpace($directStreamUrl)) {
        $container = [string] $MediaSource.Container
        if ([string]::IsNullOrWhiteSpace($container)) {
            $container = 'mkv'
        }
        $directStreamUrl = "/Videos/$EmbyItemId/stream.${container}?static=true&MediaSourceId=$([uri]::EscapeDataString([string] $MediaSource.Id))&PlaySessionId=$([uri]::EscapeDataString($PlaySessionId))&UserId=$([uri]::EscapeDataString($Context.user_id))"
    }

    $uri = if ($directStreamUrl.StartsWith('http://') -or $directStreamUrl.StartsWith('https://')) { $directStreamUrl } else { "$($Context.base_url.TrimEnd('/'))/$($directStreamUrl.TrimStart('/'))" }
    if ($uri -notmatch 'api_key=') {
        $separator = if ($uri.Contains('?')) { '&' } else { '?' }
        $uri = "$uri${separator}api_key=$([uri]::EscapeDataString($Context.api_key))"
    }
    return $uri
}

function Get-EmbyPlaybackStartSignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $EmbyItemId,
        [Parameter(Mandatory = $true)]
        [object] $PlaybackInfo
    )

    $uri = Resolve-EmbyStreamUri -Context $Context -EmbyItemId $EmbyItemId -MediaSource $PlaybackInfo.media_source -PlaySessionId ([string] $PlaybackInfo.play_session_id)
    return Invoke-HttpPlaybackStartSignal -Uri $uri
}

function Get-PlexPlaybackStartSignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [object] $PlaybackInfo
    )

    $streamKey = [string] $PlaybackInfo.media_source.StreamKey
    if ([string]::IsNullOrWhiteSpace($streamKey)) {
        return [pscustomobject]@{
            status        = 'failed'
            uri           = $null
            status_code   = $null
            bytes_read    = 0
            content_type  = $null
            content_range = $null
            details       = 'Plex playback-start route was missing a stream key.'
        }
    }

    $uri = if ($streamKey.StartsWith('http://') -or $streamKey.StartsWith('https://')) { $streamKey } else { "$($Context.base_url.TrimEnd('/'))$streamKey" }
    if ($uri -notmatch 'X-Plex-Token=') {
        $separator = if ($uri.Contains('?')) { '&' } else { '?' }
        $uri = "$uri${separator}X-Plex-Token=$([uri]::EscapeDataString($Context.api_key))"
    }

    return Invoke-HttpPlaybackStartSignal -Uri $uri -Headers @{ 'X-Plex-Token' = $Context.api_key }
}

function Capture-PlexPlaybackLogEvidence {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Topology,
        [Parameter(Mandatory = $true)]
        [string] $ArtifactDir
    )

    if ($Topology -eq 'docker_wsl') {
        $artifactPath = Join-Path $ArtifactDir 'plex-playback-tail.log'
        try {
            $logTail = & docker exec filmu-local-plex sh -lc "tail -n 120 '/config/Library/Application Support/Plex Media Server/Logs/Plex Media Server.log'" 2>&1
            Set-Content -Path $artifactPath -Encoding UTF8 -Value (($logTail | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine)
            return $artifactPath
        }
        catch {
            return $null
        }
    }

    if ($Topology -eq 'native_windows') {
        $localAppData = [System.Environment]::GetEnvironmentVariable('LOCALAPPDATA')
        if (-not [string]::IsNullOrWhiteSpace($localAppData)) {
            $sourcePath = Join-Path $localAppData 'Plex\Plex Media Server\Logs\Plex Media Server.log'
            if (Test-Path -LiteralPath $sourcePath) {
                $artifactPath = Join-Path $ArtifactDir 'plex-playback-tail.log'
                Get-Content -LiteralPath $sourcePath -Tail 120 | Set-Content -Path $artifactPath -Encoding UTF8
                return $artifactPath
            }
        }
    }

    return $null
}

function Capture-EmbyPlaybackLogEvidence {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Topology,
        [Parameter(Mandatory = $true)]
        [string] $ArtifactDir
    )

    if ($Topology -eq 'docker_wsl') {
        $artifactPath = Join-Path $ArtifactDir 'emby-playback-tail.log'
        try {
            $logTail = & docker exec filmu-local-emby sh -lc "tail -n 120 /config/log/embyserver.txt" 2>&1
            Set-Content -Path $artifactPath -Encoding UTF8 -Value (($logTail | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine)
            return $artifactPath
        }
        catch {
            return $null
        }
    }

    $hostLogCandidates = @(
        'E:\Emby\programdata\logs\embyserver.txt',
        (Join-Path $env:APPDATA 'Emby-Server\logs\embyserver.txt')
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace([string] $_) }

    foreach ($candidate in $hostLogCandidates) {
        if (Test-Path -LiteralPath $candidate) {
            $artifactPath = Join-Path $ArtifactDir 'emby-playback-tail.log'
            Get-Content -LiteralPath $candidate -Tail 120 | Set-Content -Path $artifactPath -Encoding UTF8
            return $artifactPath
        }
    }

    return $null
}

function Get-MediaServerPlaybackStartSignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $ItemId,
        [Parameter(Mandatory = $true)]
        [object] $PlaybackInfo,
        [Parameter(Mandatory = $true)]
        [string] $Topology,
        [Parameter(Mandatory = $true)]
        [string] $ArtifactDir
    )

    $result = switch ([string] $Context.provider) {
        'jellyfin' { Get-JellyfinPlaybackStartSignal -Context $Context -JellyfinItemId $ItemId -PlaybackInfo $PlaybackInfo }
        'emby' { Get-EmbyPlaybackStartSignal -Context $Context -EmbyItemId $ItemId -PlaybackInfo $PlaybackInfo }
        'plex' { Get-PlexPlaybackStartSignal -Context $Context -PlaybackInfo $PlaybackInfo }
        default { throw ("Unsupported media server provider for playback-start proof: {0}" -f $Context.provider) }
    }

    $logPath = $null
    switch ([string] $Context.provider) {
        'plex' { $logPath = Capture-PlexPlaybackLogEvidence -Topology $Topology -ArtifactDir $ArtifactDir }
        'emby' { $logPath = Capture-EmbyPlaybackLogEvidence -Topology $Topology -ArtifactDir $ArtifactDir }
    }

    $result | Add-Member -NotePropertyName log_path -NotePropertyValue $logPath -Force
    return $result
}

function Get-PlexWslEvidence {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ArtifactDir
    )

    $listingPath = Join-Path $ArtifactDir 'wsl-mount-root-listing.txt'
    $persistentLogPath = Join-Path $ArtifactDir 'wsl-filmuvfs-persistent.log'
    $binaryEvidencePath = Join-Path $ArtifactDir 'wsl-filmuvfs-host-binary.txt'
    $filmuvfsTailPath = Join-Path $ArtifactDir 'filmuvfs-tail-for-plex-proof.log'

    $evidencePath = Join-Path $ArtifactDir 'plex-wsl-evidence.json'
    $listingText = (Invoke-WslBash -Command 'ls /mnt/filmuvfs 2>/dev/null || true' | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    Set-Content -Path $listingPath -Encoding UTF8 -Value $listingText
    $mountStatus = if ($listingText -match '(^|\r?\n)(movies|shows)(\r?\n|$)') { 'visible' } else { 'missing' }
    $mountDetails = if ($mountStatus -eq 'visible') { 'WSL host mount exposes the expected catalog root.' } else { 'WSL host mount listing did not expose movies/shows.' }

    $persistentText = (Invoke-WslBash -Command 'tail -n 400 /tmp/filmuvfs_persistent.log 2>/dev/null || true' | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    Set-Content -Path $persistentLogPath -Encoding UTF8 -Value $persistentText

    $sourcePaths = @(
        'E:\Dev\Filmu\FilmuCore\rust\filmuvfs\Cargo.toml',
        'E:\Dev\Filmu\FilmuCore\rust\filmuvfs\build.rs',
        'E:\Dev\Filmu\FilmuCore\proto\filmuvfs\catalog\v1\catalog.proto'
    )
    $sourceFiles = @()
    foreach ($sourcePath in $sourcePaths) {
        if (Test-Path -LiteralPath $sourcePath) {
            $sourceFiles += Get-Item -LiteralPath $sourcePath
        }
    }
    if (Test-Path -LiteralPath 'E:\Dev\Filmu\FilmuCore\rust\filmuvfs\src') {
        $sourceFiles += Get-ChildItem -LiteralPath 'E:\Dev\Filmu\FilmuCore\rust\filmuvfs\src' -Recurse -File
    }
    $newestSourceSeconds = if ($sourceFiles.Count -gt 0) {
        [int64][Math]::Floor((( $sourceFiles | Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1).LastWriteTimeUtc - [datetime]'1970-01-01T00:00:00Z').TotalSeconds)
    }
    else {
        $null
    }

    $binaryEpochText = (Invoke-WslBash -Command 'if [ -x /tmp/filmuvfs-target/release/filmuvfs ]; then stat -c %Y /tmp/filmuvfs-target/release/filmuvfs; else echo missing; fi' | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    $binaryStatText = (Invoke-WslBash -Command 'stat /tmp/filmuvfs-target/release/filmuvfs 2>/dev/null || true' | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    $binaryText = @( 
        ('binary_epoch={0}' -f $binaryEpochText.Trim()),
        ('newest_source_epoch={0}' -f $newestSourceSeconds),
        $binaryStatText
    ) -join [Environment]::NewLine
    Set-Content -Path $binaryEvidencePath -Encoding UTF8 -Value $binaryText
    if ([string]::IsNullOrWhiteSpace($binaryEpochText) -or ($binaryEpochText.Trim() -eq 'missing')) {
        $binaryStatus = 'missing'
        $binaryDetails = 'WSL host mount binary was missing when the proof captured evidence.'
    }
    elseif ([string]::IsNullOrWhiteSpace($newestSourceSeconds)) {
        $binaryStatus = 'unknown'
        $binaryDetails = 'Could not determine the newest Rust/proto source timestamp for WSL host-binary freshness.'
    }
    elseif ([int64]$binaryEpochText.Trim() + 1 -ge [int64]$newestSourceSeconds) {
        $binaryStatus = 'current'
        $binaryDetails = 'WSL host mount is using a rebuilt binary that is at least as new as the Rust/proto sources.'
    }
    else {
        $binaryStatus = 'stale'
        $binaryDetails = 'WSL host mount binary is older than the newest Rust/proto source input.'
    }

    $filmuvfsLogText = $persistentText
    Set-Content -Path $filmuvfsTailPath -Encoding UTF8 -Value $filmuvfsLogText
    $evidenceLogText = [regex]::Replace($filmuvfsLogText, ([string][char]27 + '\[[0-9;]*[A-Za-z]'), '')

    $refreshIdentityStatus = if (($evidenceLogText -match 'entry_id') -and ($evidenceLogText -match 'file_id\s*=\s*file:')) { 'entry_id_bound' } else { 'not_observed' }
    $refreshIdentityDetails = if ($refreshIdentityStatus -eq 'entry_id_bound') { 'Mounted read logging shows entry_id-based cache identity, which guards against provider_file_id refresh collisions.' } else { 'Did not observe entry_id-based read-plan logging in the captured persistent filmuvfs tail.' }

    if ($evidenceLogText -match 'waiting for in-flight foreground fetch') {
        $foregroundFetchStatus = 'coalesced_wait_observed'
        $foregroundFetchDetails = 'Observed in-flight foreground fetch coalescing in the persistent filmuvfs log.'
    }
    elseif ($evidenceLogText -match 'pattern\s*=\s*"cache_hit"') {
        $foregroundFetchStatus = 'no_duplicate_fetch_signal_observed'
        $foregroundFetchDetails = 'Captured read plans were cache hits with no duplicate foreground fetch signal in the persistent filmuvfs log.'
    }
    else {
        $foregroundFetchStatus = 'not_observed'
        $foregroundFetchDetails = 'Did not observe cache-hit or in-flight fetch-coalescing evidence in the captured persistent filmuvfs tail.'
    }


    $mountCheckStatus = if ($mountStatus -eq 'visible') { 'passed' } else { 'failed' }
    $mountCheckDetails = $mountDetails
    $hostBinaryCheckStatus = if ($binaryStatus -eq 'current') { 'passed' } else { 'failed' }
    $hostBinaryCheckDetails = $binaryDetails
    $refreshIdentityCheckStatus = if ($refreshIdentityStatus -eq 'entry_id_bound') { 'passed' } else { 'failed' }
    $refreshIdentityCheckDetails = $refreshIdentityDetails
    $foregroundFetchCheckStatus = if ($foregroundFetchStatus -in @('coalesced_wait_observed', 'no_duplicate_fetch_signal_observed')) { 'passed' } else { 'failed' }
    $foregroundFetchCheckDetails = $foregroundFetchDetails
    $overallCheckStatus = if (($mountCheckStatus -eq 'passed') -and ($hostBinaryCheckStatus -eq 'passed') -and ($refreshIdentityCheckStatus -eq 'passed') -and ($foregroundFetchCheckStatus -eq 'passed')) { 'passed' } else { 'failed' }
    $overallCheckDetails = "WSL mount=$mountStatus; binary=$binaryStatus; refresh_identity=$refreshIdentityStatus; foreground_fetch=$foregroundFetchStatus."

    $evidence = [ordered]@{
        mount_status                    = $mountStatus
        mount_details                   = $mountDetails
        persistent_log_path             = $persistentLogPath
        host_binary_status              = $binaryStatus
        host_binary_details             = $binaryDetails
        refresh_identity_status         = $refreshIdentityStatus
        refresh_identity_details        = $refreshIdentityDetails
        foreground_fetch_status         = $foregroundFetchStatus
        foreground_fetch_details        = $foregroundFetchDetails
        mount_check_status              = $mountCheckStatus
        mount_check_details             = $mountCheckDetails
        host_binary_check_status        = $hostBinaryCheckStatus
        host_binary_check_details       = $hostBinaryCheckDetails
        refresh_identity_check_status   = $refreshIdentityCheckStatus
        refresh_identity_check_details  = $refreshIdentityCheckDetails
        foreground_fetch_check_status   = $foregroundFetchCheckStatus
        foreground_fetch_check_details  = $foregroundFetchCheckDetails
        overall_check_status            = $overallCheckStatus
        overall_check_details           = $overallCheckDetails
        listing_path                    = $listingPath
        filmuvfs_tail_path              = $filmuvfsTailPath
        binary_evidence_path            = $binaryEvidencePath
    }
    $evidence | ConvertTo-Json -Depth 8 | Set-Content -Path $evidencePath -Encoding UTF8
    $evidence['evidence_path'] = $evidencePath
    return [pscustomobject]@{
        mount_status                    = $mountStatus
        mount_details                   = $mountDetails
        persistent_log_path             = $persistentLogPath
        host_binary_status              = $binaryStatus
        host_binary_details             = $binaryDetails
        refresh_identity_status         = $refreshIdentityStatus
        refresh_identity_details        = $refreshIdentityDetails
        foreground_fetch_status         = $foregroundFetchStatus
        foreground_fetch_details        = $foregroundFetchDetails
        mount_check_status              = $mountCheckStatus
        mount_check_details             = $mountCheckDetails
        host_binary_check_status        = $hostBinaryCheckStatus
        host_binary_check_details       = $hostBinaryCheckDetails
        refresh_identity_check_status   = $refreshIdentityCheckStatus
        refresh_identity_check_details  = $refreshIdentityCheckDetails
        foreground_fetch_check_status   = $foregroundFetchCheckStatus
        foreground_fetch_check_details  = $foregroundFetchCheckDetails
        overall_check_status            = $overallCheckStatus
        overall_check_details           = $overallCheckDetails
        evidence_path                   = $evidencePath
    }
}

function Resolve-MediaServerProofContext {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Provider
    )

    switch ($Provider) {
        'jellyfin' { return Resolve-JellyfinContext }
        'emby' { return Resolve-EmbyContext }
        'plex' { return Resolve-PlexContext }
        default { return $null }
    }
}

function Get-MediaServerSearchTerm {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Provider,
        [Parameter(Mandatory = $true)]
        [string] $FallbackSearchTerm
    )

    if ($Provider -eq 'jellyfin' -and -not [string]::IsNullOrWhiteSpace($JellyfinSearchTerm)) {
        return $JellyfinSearchTerm
    }
    if ($Provider -eq 'emby') {
        if (-not [string]::IsNullOrWhiteSpace([string] $env:EMBY_SEARCH_TERM)) { return [string] $env:EMBY_SEARCH_TERM }
        if ($script:DotEnv.ContainsKey('EMBY_SEARCH_TERM') -and -not [string]::IsNullOrWhiteSpace([string] $script:DotEnv['EMBY_SEARCH_TERM'])) { return [string] $script:DotEnv['EMBY_SEARCH_TERM'] }
    }
    if ($Provider -eq 'plex') {
        if (-not [string]::IsNullOrWhiteSpace([string] $env:PLEX_SEARCH_TERM)) { return [string] $env:PLEX_SEARCH_TERM }
        if ($script:DotEnv.ContainsKey('PLEX_SEARCH_TERM') -and -not [string]::IsNullOrWhiteSpace([string] $script:DotEnv['PLEX_SEARCH_TERM'])) { return [string] $script:DotEnv['PLEX_SEARCH_TERM'] }
    }

    return $FallbackSearchTerm
}

function Get-MediaServerVisibilitySignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $SearchTerm
    )

    switch ([string] $Context.provider) {
        'jellyfin' { return Get-JellyfinVisibilitySignal -Context $Context -SearchTerm $SearchTerm }
        'emby' { return Get-EmbyVisibilitySignal -Context $Context -SearchTerm $SearchTerm }
        'plex' { return Get-PlexVisibilitySignal -Context $Context -SearchTerm $SearchTerm }
        default { throw ("Unsupported media server provider for visibility proof: {0}" -f $Context.provider) }
    }
}

function Get-MediaServerPlaybackInfo {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $ItemId
    )

    switch ([string] $Context.provider) {
        'jellyfin' { return Get-JellyfinPlaybackInfo -Context $Context -JellyfinItemId $ItemId }
        'emby' { return Get-EmbyPlaybackInfo -Context $Context -EmbyItemId $ItemId }
        'plex' { return Get-PlexPlaybackInfo -Context $Context -PlexItemId $ItemId }
        default { throw ("Unsupported media server provider for playback-info proof: {0}" -f $Context.provider) }
    }
}

function Get-MediaServerStreamOpenSignal {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $ItemId,
        [Parameter(Mandatory = $true)]
        [object] $PlaybackInfo
    )

    switch ([string] $Context.provider) {
        'jellyfin' {
            return Get-JellyfinStreamOpenSignal -Context $Context -JellyfinItemId $ItemId -MediaSourceId ([string] $PlaybackInfo.media_source.Id) -Container ([string] $PlaybackInfo.media_source.Container) -PlaySessionId ([string] $PlaybackInfo.play_session_id)
        }
        'emby' {
            return Get-EmbyStreamOpenSignal -Context $Context -EmbyItemId $ItemId -MediaSource $PlaybackInfo.media_source -PlaySessionId ([string] $PlaybackInfo.play_session_id)
        }
        'plex' {
            return Get-PlexStreamOpenSignal -Context $Context -MediaSource $PlaybackInfo.media_source
        }
        default { throw ("Unsupported media server provider for stream-open proof: {0}" -f $Context.provider) }
    }
}

function Invoke-MediaServerSessionProof {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Context,
        [Parameter(Mandatory = $true)]
        [string] $ItemId,
        [Parameter(Mandatory = $true)]
        [object] $PlaybackInfo
    )

    switch ([string] $Context.provider) {
        'jellyfin' {
            return Invoke-JellyfinSessionProof -Context $Context -JellyfinItemId $ItemId -MediaSourceId ([string] $PlaybackInfo.media_source.Id) -PlaySessionId ([string] $PlaybackInfo.play_session_id)
        }
        'emby' {
            return Invoke-EmbySessionProof -Context $Context -EmbyItemId $ItemId -MediaSourceId ([string] $PlaybackInfo.media_source.Id) -PlaySessionId ([string] $PlaybackInfo.play_session_id)
        }
        'plex' {
            return [pscustomobject]@{
                status          = 'skipped'
                started_status  = $null
                progress_status = $null
                stopped_status  = $null
            }
        }
        default { throw ("Unsupported media server provider for session proof: {0}" -f $Context.provider) }
    }
}

function Force-ActiveDirectMediaEntryStale {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ItemId,
        [Parameter(Mandatory = $true)]
        [string] $StaleUrl
    )

    $scriptPath = '/app/tests/fixtures/force_media_entry_unrestricted_stale.py'
    $entryOutput = & docker exec filmu-python python $scriptPath $ItemId $StaleUrl 2>&1
    if ($LASTEXITCODE -ne 0) {
        $details = ($entryOutput | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
        throw ("Failed to force stale direct media entry for item {0}: {1}" -f $ItemId, $details)
    }

    $entryId = ($entryOutput | Select-Object -First 1)
    if ($null -eq $entryId) {
        throw ("Failed to resolve the target direct media entry id for item {0}." -f $ItemId)
    }

    return $entryId.ToString().Trim()
}

function Invoke-DirectPlaybackRouteRangeRead {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ItemId,
        [Parameter(Mandatory = $true)]
        [string] $ApiKey,
        [int] $Bytes = 1024,
        [string] $ArtifactPrefix = 'stale-refresh-route'
    )

    $url = "$BackendUrl/api/v1/stream/file/$ItemId"
    $headersPath = Join-Path $artifactDir ("{0}-headers.txt" -f $ArtifactPrefix)
    $bodyPath = Join-Path $artifactDir ("{0}-body.bin" -f $ArtifactPrefix)
    if (Test-Path $headersPath) { Remove-Item $headersPath -Force }
    if (Test-Path $bodyPath) { Remove-Item $bodyPath -Force }

    $curlArgs = @(
        '-sS',
        '-D', $headersPath,
        '-H', ("x-api-key: {0}" -f $ApiKey),
        '-H', ("x-actor-id: {0}" -f $BackendActorId),
        '-H', ("x-actor-type: {0}" -f $BackendActorType),
        '-H', ("x-actor-roles: {0}" -f $BackendActorRoles),
        '-H', ("Range: bytes=0-{0}" -f ($Bytes - 1)),
        $url,
        '-o', $bodyPath
    )
    if (-not [string]::IsNullOrWhiteSpace($BackendActorScopes)) {
        $curlArgs = @($curlArgs[0..5] + @('-H', ("x-actor-scopes: {0}" -f $BackendActorScopes)) + $curlArgs[6..($curlArgs.Count - 1)])
    }

    & curl.exe @curlArgs
    if ($LASTEXITCODE -ne 0) {
        throw 'direct playback route request failed during stale-refresh proof'
    }

    $headerLines = Get-Content $headersPath
    $statusLine = $headerLines | Select-Object -First 1
    $statusCode = if ($statusLine -match '\s(\d{3})\s') { [int] $Matches[1] } else { 0 }
    $contentRange = ($headerLines | Where-Object { $_ -match '^Content-Range:' } | Select-Object -First 1)
    $length = (Get-Item $bodyPath).Length
    $bodyPreview = $null
    if ($statusCode -ge 400 -and $length -gt 0 -and $length -le 4096) {
        $bodyPreview = Get-Content $bodyPath -Raw -Encoding UTF8
    }

    return [pscustomobject]@{
        status_code   = $statusCode
        bytes_read    = $length
        content_range = $contentRange
        body_preview  = $bodyPreview
    }
}

function Wait-DirectPlaybackRouteRecovery {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ItemId,
        [Parameter(Mandatory = $true)]
        [string] $ApiKey,
        [Parameter(Mandatory = $true)]
        [string] $StaleUrl,
        [int] $TimeoutSeconds = 30,
        [int] $PollSeconds = 2,
        [int] $Bytes = 1024
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $attempt = 0
    $lastProbe = $null
    $lastDetail = $null
    $lastUrl = $null

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds $PollSeconds
        $attempt += 1
        $lastProbe = Invoke-DirectPlaybackRouteRangeRead -ItemId $ItemId -ApiKey $ApiKey -Bytes $Bytes -ArtifactPrefix ("stale-refresh-retry-{0:d2}" -f $attempt)
        $lastDetail = Get-ProofItemDetail -ItemId $ItemId
        $activeDirectEntry = @($lastDetail.media_entries | Where-Object { [bool] $_.active_for_direct }) | Select-Object -First 1
        $lastUrl = if ($null -ne $activeDirectEntry) { [string] $activeDirectEntry.unrestricted_url } else { $null }
        $persistedUrlChanged = (-not [string]::IsNullOrWhiteSpace($lastUrl)) -and ($lastUrl -ne $StaleUrl)

        if (@(200, 206) -contains [int] $lastProbe.status_code) {
            return [pscustomobject]@{
                status                = if ($persistedUrlChanged) { 'recovered_persisted' } else { 'recovered_route_only' }
                attempt_count         = $attempt
                probe                 = $lastProbe
                detail                = $lastDetail
                refreshed_url         = $lastUrl
                persisted_url_changed = $persistedUrlChanged
            }
        }
    }

    return [pscustomobject]@{
        status                = 'timeout'
        attempt_count         = $attempt
        probe                 = $lastProbe
        detail                = $lastDetail
        refreshed_url         = $lastUrl
        persisted_url_changed = $false
    }
}

function Get-CurrentSettings {
    return Get-BackendJson -Uri "$BackendUrl/api/v1/settings"
}

function Set-AllSettings {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Payload
    )

    return Post-BackendJson -Uri "$BackendUrl/api/v1/settings/set/all" -Body $Payload
}

function Copy-SettingsPayload {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Payload
    )

    return ($Payload | ConvertTo-Json -Depth 50 | ConvertFrom-Json)
}

function Configure-MediaServerUpdater {
    if ([string]::IsNullOrWhiteSpace($MediaServerProvider)) {
        return $null
    }

    if ([string]::IsNullOrWhiteSpace($MediaServerUrl)) {
        throw 'MediaServerUrl is required when MediaServerProvider is supplied.'
    }

    if ([string]::IsNullOrWhiteSpace($MediaServerToken)) {
        throw 'MediaServerToken is required when MediaServerProvider is supplied.'
    }

    $originalSettings = Get-CurrentSettings
    $updatedSettings = Copy-SettingsPayload -Payload $originalSettings

    if ($null -eq $updatedSettings.updaters) {
        $updatedSettings | Add-Member -NotePropertyName updaters -NotePropertyValue ([pscustomobject]@{})
    }

    $updatedSettings.updaters.plex = [pscustomobject]@{
        enabled = $false
        token   = ''
        url     = 'http://localhost:32400'
    }
    $updatedSettings.updaters.jellyfin = [pscustomobject]@{
        enabled = $false
        api_key = ''
        url     = 'http://localhost:8096'
    }
    $updatedSettings.updaters.emby = [pscustomobject]@{
        enabled = $false
        api_key = ''
        url     = 'http://localhost:8097'
    }

    if ($MediaServerProvider -eq 'plex') {
        $updatedSettings.updaters.plex = [pscustomobject]@{
            enabled = $true
            token   = $MediaServerToken
            url     = $MediaServerUrl
        }
    }
    elseif ($MediaServerProvider -eq 'jellyfin') {
        $updatedSettings.updaters.jellyfin = [pscustomobject]@{
            enabled = $true
            api_key = $MediaServerToken
            url     = $MediaServerUrl
        }
    }
    elseif ($MediaServerProvider -eq 'emby') {
        $updatedSettings.updaters.emby = [pscustomobject]@{
            enabled = $true
            api_key = $MediaServerToken
            url     = $MediaServerUrl
        }
    }

    Set-AllSettings -Payload $updatedSettings | Out-Null
    return $originalSettings
}

function Restore-SettingsPayload {
    param(
        [Parameter(Mandatory = $true)]
        [object] $Payload
    )

    Set-AllSettings -Payload $Payload | Out-Null
}

function Get-MediaServerSignal {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ItemId
    )

    $containers = @('filmu-arq-worker', 'filmu-python')
    foreach ($container in $containers) {
        $output = Get-DockerLogsText -Container $container -Tail 400
        $pattern = 'media_server\.notification_summary item_id=' + [regex]::Escape($ItemId) + ' triggered=([^\s]+) failed=([^\s]+) skipped=([^\s]+)'
        $match = [regex]::Match($output, $pattern)
        if ($match.Success) {
            $triggered = $match.Groups[1].Value
            $failed = $match.Groups[2].Value
            $skipped = $match.Groups[3].Value
            if ($triggered -ne 'none') {
                return [pscustomobject]@{ status = 'triggered'; container = $container; triggered = $triggered; failed = $failed; skipped = $skipped }
            }
            if ($failed -ne 'none') {
                return [pscustomobject]@{ status = 'failed'; container = $container; triggered = $triggered; failed = $failed; skipped = $skipped }
            }
            return [pscustomobject]@{ status = 'skipped'; container = $container; triggered = $triggered; failed = $failed; skipped = $skipped }
        }
    }

    return $null
}

function Get-MediaServerRequestSignal {
    if ([string]::IsNullOrWhiteSpace($MediaServerRequestLogPath)) {
        return $null
    }

    if (-not (Test-Path $MediaServerRequestLogPath)) {
        return $null
    }

    $text = Get-Content $MediaServerRequestLogPath -Raw -ErrorAction SilentlyContinue
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }

    if ($MediaServerProvider -eq 'plex') {
        $hasSections = $text -match '/library/sections'
        $hasRefresh = $text -match '/library/sections/1/refresh'
        if ($hasSections -and $hasRefresh) {
            return [pscustomobject]@{ status = 'triggered'; container = 'external-request-log'; triggered = 'plex'; failed = 'none'; skipped = 'none' }
        }
    }

    return $null
}

function Get-ProofItemSummary {
    $itemsResponse = Get-BackendJson -Uri "$BackendUrl/api/v1/items?type=$MediaType&limit=100&page=1"
    $items = @($itemsResponse.items)
    return $items | Where-Object { $_.tmdb_id -eq $TmdbId } | Select-Object -First 1
}

function Get-ProofItemDetail {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ItemId
    )

    return Get-BackendJson -Uri "$BackendUrl/api/v1/items/${ItemId}?media_type=$MediaType&extended=true"
}

function Get-DockerLogsText {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Container,
        [int] $Tail = 400
    )

    $output = & docker logs --tail $Tail $Container 2>&1
    return ($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
}

function Restart-WorkerForUpdatedSettings {
    docker restart filmu-arq-worker | Out-Null

    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        $status = (docker inspect -f '{{.State.Status}}' filmu-arq-worker 2>$null | Select-Object -First 1)
        if ($status -eq 'running') {
            return
        }
        Start-Sleep -Seconds 1
    }

    throw 'filmu-arq-worker did not return to running state after restart.'
}

function Save-DockerEvidence {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ArtifactDir
    )

    docker compose -f $script:ComposeFile ps | Set-Content -Path (Join-Path $ArtifactDir 'docker-compose-ps.txt') -Encoding UTF8

    $containers = @(
        'filmu-python',
        'filmu-arq-worker',
        'filmuvfs',
        'frontend',
        'filmu-local-plex',
        'filmu-local-emby'
    )

    foreach ($container in $containers) {
        $logPath = Join-Path $ArtifactDir ("{0}.log" -f $container)
        try {
            $logOutput = Get-DockerLogsText -Container $container -Tail 300
            Set-Content -Path $logPath -Encoding UTF8 -Value $logOutput
        }
        catch {
            Set-Content -Path $logPath -Encoding UTF8 -Value ("[playback-proof] failed to capture docker logs for {0}: {1}" -f $container, $_.Exception.Message)
        }
    }
}

$scriptRoot = $PSScriptRoot
$repoRoot = Split-Path -Parent $scriptRoot
$script:WslRepoRoot = Convert-ToWslPath -WindowsPath $repoRoot
$script:ComposeFile = Join-Path $repoRoot 'docker-compose.local.yml'
$script:StartScript = Join-Path $scriptRoot 'start_local_stack.ps1'
$script:StopScript = Join-Path $scriptRoot 'stop_local_stack.ps1'
$script:DotEnv = Get-DotEnvMap -Path (Join-Path $repoRoot '.env')
$artifactsRoot = Join-Path $repoRoot 'playback-proof-artifacts'
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$artifactDir = Join-Path $artifactsRoot $timestamp
$script:StepResults = [System.Collections.Generic.List[object]]::new()
$originalSettingsPayload = $null

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    $ApiKey = if (-not [string]::IsNullOrWhiteSpace([string] $env:FILMU_PY_API_KEY)) {
        [string] $env:FILMU_PY_API_KEY
    }
    elseif ($script:DotEnv.ContainsKey('FILMU_PY_API_KEY') -and -not [string]::IsNullOrWhiteSpace([string] $script:DotEnv['FILMU_PY_API_KEY'])) {
        [string] $script:DotEnv['FILMU_PY_API_KEY']
    }
    else {
        '32_character_filmu_api_key_local_'
    }
}

if ([string]::IsNullOrWhiteSpace($JellyfinApiKey)) {
    if (-not [string]::IsNullOrWhiteSpace([string] $env:JELLYFIN_API_KEY)) {
        $JellyfinApiKey = [string] $env:JELLYFIN_API_KEY
    }
    elseif ($script:DotEnv.ContainsKey('JELLYFIN_API_KEY')) {
        $JellyfinApiKey = [string] $script:DotEnv['JELLYFIN_API_KEY']
    }
}

if ([string]::IsNullOrWhiteSpace($JellyfinUserId)) {
    if (-not [string]::IsNullOrWhiteSpace([string] $env:JELLYFIN_USER_ID)) {
        $JellyfinUserId = [string] $env:JELLYFIN_USER_ID
    }
    elseif ($script:DotEnv.ContainsKey('JELLYFIN_USER_ID')) {
        $JellyfinUserId = [string] $script:DotEnv['JELLYFIN_USER_ID']
    }
}

if ([string]::IsNullOrWhiteSpace($JellyfinLibraryId)) {
    if (-not [string]::IsNullOrWhiteSpace([string] $env:JELLYFIN_LIBRARY_ID)) {
        $JellyfinLibraryId = [string] $env:JELLYFIN_LIBRARY_ID
    }
    elseif ($script:DotEnv.ContainsKey('JELLYFIN_LIBRARY_ID')) {
        $JellyfinLibraryId = [string] $script:DotEnv['JELLYFIN_LIBRARY_ID']
    }
}

if ([string]::IsNullOrWhiteSpace($JellyfinSearchTerm)) {
    if (-not [string]::IsNullOrWhiteSpace([string] $env:JELLYFIN_SEARCH_TERM)) {
        $JellyfinSearchTerm = [string] $env:JELLYFIN_SEARCH_TERM
    }
    elseif ($script:DotEnv.ContainsKey('JELLYFIN_SEARCH_TERM')) {
        $JellyfinSearchTerm = [string] $script:DotEnv['JELLYFIN_SEARCH_TERM']
    }
}


if (-not [string]::IsNullOrWhiteSpace($MediaServerProvider)) {
    if ([string]::IsNullOrWhiteSpace($MediaServerUrl)) {
        if ($MediaServerProvider -eq 'plex') {
            $MediaServerUrl = if (-not [string]::IsNullOrWhiteSpace([string] $env:PLEX_URL)) { [string] $env:PLEX_URL } elseif ($script:DotEnv.ContainsKey('PLEX_URL')) { [string] $script:DotEnv['PLEX_URL'] } else { Get-NativePlexUrl }
        }
        elseif ($MediaServerProvider -eq 'jellyfin') {
            $MediaServerUrl = if (-not [string]::IsNullOrWhiteSpace([string] $env:JELLYFIN_URL)) { [string] $env:JELLYFIN_URL } elseif ($script:DotEnv.ContainsKey('JELLYFIN_URL')) { [string] $script:DotEnv['JELLYFIN_URL'] } else { 'http://localhost:8096' }
        }
        elseif ($MediaServerProvider -eq 'emby') {
            $MediaServerUrl = if (-not [string]::IsNullOrWhiteSpace([string] $env:EMBY_URL)) { [string] $env:EMBY_URL } elseif ($script:DotEnv.ContainsKey('EMBY_URL')) { [string] $script:DotEnv['EMBY_URL'] } else { 'http://localhost:8097' }
        }
    }

    if ([string]::IsNullOrWhiteSpace($MediaServerToken)) {
        if ($MediaServerProvider -eq 'plex') {
            $nativePlexToken = if (Test-IsNativePlexUrl -Url $MediaServerUrl) { Get-NativePlexLocalAdminToken } else { '' }
            if (-not [string]::IsNullOrWhiteSpace($nativePlexToken)) {
                $MediaServerToken = $nativePlexToken
            }
            elseif (-not [string]::IsNullOrWhiteSpace([string] $env:PLEX_TOKEN)) {
                $MediaServerToken = [string] $env:PLEX_TOKEN
            }
            elseif ($script:DotEnv.ContainsKey('PLEX_TOKEN')) {
                $MediaServerToken = [string] $script:DotEnv['PLEX_TOKEN']
            }
        }
        elseif ($MediaServerProvider -eq 'jellyfin') {
            if (-not [string]::IsNullOrWhiteSpace([string] $env:JELLYFIN_API_KEY)) {
                $MediaServerToken = [string] $env:JELLYFIN_API_KEY
            }
            elseif ($script:DotEnv.ContainsKey('JELLYFIN_API_KEY')) {
                $MediaServerToken = [string] $script:DotEnv['JELLYFIN_API_KEY']
            }
        }
        elseif ($MediaServerProvider -eq 'emby') {
            if (-not [string]::IsNullOrWhiteSpace([string] $env:EMBY_API_KEY)) {
                $MediaServerToken = [string] $env:EMBY_API_KEY
            }
            elseif ($script:DotEnv.ContainsKey('EMBY_API_KEY')) {
                $MediaServerToken = [string] $script:DotEnv['EMBY_API_KEY']
            }
        }
    }
}

$script:BackendHeaders = Get-BackendHeaders `
    -ApiKey $ApiKey `
    -ActorId $BackendActorId `
    -ActorType $BackendActorType `
    -ActorRoles $BackendActorRoles `
    -ActorScopes $BackendActorScopes

New-Item -ItemType Directory -Path $artifactDir -Force | Out-Null

$summary = [ordered]@{
    timestamp    = (Get-Date).ToString('o')
    artifact_dir = $artifactDir
    parameters   = [ordered]@{
        tmdb_id                          = $TmdbId
        title                            = $Title
        media_type                       = $MediaType
        media_server_provider            = $MediaServerProvider
        media_server_url                 = $MediaServerUrl
        jellyfin_enabled                 = -not [string]::IsNullOrWhiteSpace($JellyfinApiKey)
        backend_url                      = $BackendUrl
        frontend_url                     = $FrontendUrl
        wsl_distro                       = $WslDistro
        mount_root                       = $MountRoot
        acquisition_timeout_seconds      = $AcquisitionTimeoutSeconds
        media_server_timeout_seconds     = $MediaServerTimeoutSeconds
        mount_visibility_timeout_seconds = $MountVisibilityTimeoutSeconds
        poll_interval_seconds            = $PollIntervalSeconds
        mounted_read_bytes               = $MountedReadBytes
        proof_stale_direct_refresh       = [bool] $ProofStaleDirectRefresh
        reuse_existing_item              = [bool] $ReuseExistingItem
        skip_start                       = [bool] $SkipStart
        stop_when_done                   = [bool] $StopWhenDone
        dry_run                          = [bool] $DryRun
    }
    movie        = [ordered]@{
        item_id           = $null
        resolved_title    = $null
        final_state       = $null
        media_entry_count = 0
        direct_ready      = $false
    }
    mount        = [ordered]@{
        file_path       = $null
        stat_size_bytes = $null
        bytes_read      = $null
    }
    media_server = [ordered]@{
        provider                           = $MediaServerProvider
        topology                           = $null
        configured                         = $false
        signal_status                      = $null
        signal_container                   = $null
        visibility_status                  = $null
        item_id                            = $null
        library_id                         = $null
        playback_info_status               = $null
        playback_media_source_id           = $null
        playback_path                      = $null
        stream_open_status                 = $null
        stream_open_status_code            = $null
        stream_open_bytes_read             = $null
        stream_open_content_range          = $null
        playback_start_status              = $null
        playback_start_status_code         = $null
        playback_start_bytes_read          = $null
        playback_start_content_type        = $null
        playback_start_details             = $null
        playback_start_log_path            = $null
        session_status                     = $null
        session_started_status             = $null
        session_progress_status            = $null
        session_stopped_status             = $null
        wsl_host_mount_status              = $null
        wsl_host_mount_details             = $null
        wsl_persistent_log_path            = $null
        plex_wsl_evidence_path             = $null
        wsl_mount_check_status             = $null
        wsl_mount_check_details            = $null
        wsl_host_binary_check_status       = $null
        wsl_host_binary_check_details      = $null
        refresh_identity_check_status      = $null
        refresh_identity_check_details     = $null
        foreground_fetch_check_status      = $null
        foreground_fetch_check_details     = $null
        plex_wsl_evidence_status           = $null
        plex_wsl_evidence_details          = $null
        wsl_host_binary_status             = $null
        wsl_host_binary_details            = $null
        refresh_identity_status            = $null
        refresh_identity_details           = $null
        foreground_fetch_status            = $null
        foreground_fetch_details           = $null
        jellyfin_visibility_status         = $null
        jellyfin_item_id                   = $null
        jellyfin_library_id                = $null
        jellyfin_playback_info_status      = $null
        jellyfin_playback_media_source_id  = $null
        jellyfin_playback_path             = $null
        jellyfin_stream_open_status        = $null
        jellyfin_stream_open_status_code   = $null
        jellyfin_stream_open_bytes_read    = $null
        jellyfin_stream_open_content_range = $null
        jellyfin_session_status            = $null
        jellyfin_session_started_status    = $null
        jellyfin_session_progress_status   = $null
        jellyfin_session_stopped_status    = $null
        jellyfin_directstream_status       = $null
        jellyfin_directstream_details      = $null
        jellyfin_directstream_log_path     = $null
        jellyfin_directstream_cached_codec = $null
        jellyfin_directstream_actual_codec = $null
        jellyfin_directstream_refresh_status = $null
        jellyfin_directstream_refresh_details = $null
        jellyfin_directstream_refreshed_codec = $null
        stale_refresh_status               = $null
        stale_refresh_entry_id             = $null
        stale_refresh_initial_status_code  = $null
        stale_refresh_initial_body         = $null
        stale_refresh_route_status_code    = $null
        stale_refresh_bytes_read           = $null
        stale_refresh_content_range        = $null
        stale_refresh_attempt_count        = $null
        stale_refresh_recovered_url        = $null
    }
    steps        = @()
}

if ($DryRun) {
    Add-StepResult -Name 'dry_run' -Status 'passed' -Details 'Validated script parameters and planned playback-proof flow without changing the running stack.'
    Write-SummaryFile -Path (Join-Path $artifactDir 'summary.json') -Summary $summary
    Write-Host ('[playback-proof] Dry run complete. Artifact directory: {0}' -f $artifactDir) -ForegroundColor Green
    exit 0
}

try {
    if (-not $SkipStart) {
        & $script:StartScript
        Add-StepResult -Name 'stack_start' -Status 'passed' -Details 'Started local stack and persistent FilmuVFS mount.'
    }
    else {
        Add-StepResult -Name 'stack_start' -Status 'skipped' -Details 'Skipped start_local_stack.ps1 by request.'
    }

    if (-not (Wait-HttpReady -Uri "$BackendUrl/openapi.json" -TimeoutSeconds 45)) {
        throw 'Backend OpenAPI endpoint did not become ready.'
    }
    Add-StepResult -Name 'backend_ready' -Status 'passed' -Details $BackendUrl

    if (-not (Wait-HttpReady -Uri $FrontendUrl -TimeoutSeconds $FrontendTimeoutSeconds)) {
        throw 'Frontend did not become ready.'
    }
    Add-StepResult -Name 'frontend_ready' -Status 'passed' -Details $FrontendUrl

    $wslMountPreflight = Refresh-WslPersistentMount -ArtifactDir $artifactDir
    Add-StepResult -Name 'wsl_mount_preflight' -Status 'passed' -Details $wslMountPreflight.details

    $itemSummary = $null
    $existingItem = Get-ProofItemSummary
    if ($ReuseExistingItem -and $null -ne $existingItem) {
        $itemSummary = $existingItem
        Add-StepResult -Name 'reuse_existing_item' -Status 'passed' -Details ("Reusing pre-existing proof item {0} in state {1}." -f $existingItem.id, $existingItem.state)
        Add-StepResult -Name 'remove_existing_item' -Status 'skipped' -Details 'Skipped removal because ReuseExistingItem was supplied.'
    }
    else {
        Add-StepResult -Name 'reuse_existing_item' -Status 'skipped' -Details 'ReuseExistingItem not used or no matching existing item was found.'
        if ($null -ne $existingItem) {
            Delete-BackendJson -Uri "$BackendUrl/api/v1/items/remove" -Body ([ordered]@{ ids = @($existingItem.id) }) | Out-Null
            $removeDeadline = (Get-Date).AddSeconds(60)
            while ((Get-Date) -lt $removeDeadline) {
                $remaining = Get-ProofItemSummary
                if ($null -eq $remaining) {
                    break
                }
                Start-Sleep -Seconds 2
            }
            Add-StepResult -Name 'remove_existing_item' -Status 'passed' -Details ("Removed pre-existing proof item {0}." -f $existingItem.id)
        }
        else {
            Add-StepResult -Name 'remove_existing_item' -Status 'skipped' -Details 'No pre-existing proof item matched the requested TMDB id.'
        }
    }

    if (-not [string]::IsNullOrWhiteSpace($MediaServerProvider)) {
        $originalSettingsPayload = Configure-MediaServerUpdater
        $summary.media_server.configured = $true
        Restart-WorkerForUpdatedSettings
        Add-StepResult -Name 'media_server_configured' -Status 'passed' -Details ("Configured {0} updater via backend settings and restarted worker for fresh runtime settings." -f $MediaServerProvider)
    }
    else {
        Add-StepResult -Name 'media_server_configured' -Status 'skipped' -Details 'No media server provider configured for this proof run.'
    }

    $initialStreamStatus = Get-BackendJson -Uri "$BackendUrl/api/v1/stream/status"
    Write-JsonFile -Path (Join-Path $artifactDir 'stream-status-initial.json') -Value $initialStreamStatus
    Add-StepResult -Name 'stream_status_initial' -Status 'passed' -Details 'Captured initial `/api/v1/stream/status` snapshot.'

    $deadline = (Get-Date).AddSeconds($AcquisitionTimeoutSeconds)
    if ($null -eq $itemSummary) {
        $addPayload = [ordered]@{
            media_type = $MediaType
            tmdb_ids   = @()
            tvdb_ids   = @()
        }
        if ($MediaType -eq 'tv') {
            $resolvedTvdbId = Resolve-TvdbIdForProof -TmdbId $TmdbId
            $addPayload.tvdb_ids = @($resolvedTvdbId)
        }
        else {
            $addPayload.tmdb_ids = @($TmdbId)
        }
        $addResponse = Post-BackendJson -Uri "$BackendUrl/api/v1/items/add" -Body $addPayload
        Write-JsonFile -Path (Join-Path $artifactDir 'add-response.json') -Value $addResponse
        if ($MediaType -eq 'tv') {
            Add-StepResult -Name 'request_movie' -Status 'passed' -Details ("Submitted TMDB {0} / TVDB {1} ({2})." -f $TmdbId, $resolvedTvdbId, $Title)
        }
        else {
            Add-StepResult -Name 'request_movie' -Status 'passed' -Details ("Submitted TMDB {0} ({1})." -f $TmdbId, $Title)
        }

        while ((Get-Date) -lt $deadline) {
            $itemSummary = Get-ProofItemSummary
            if ($null -ne $itemSummary) {
                break
            }
            Start-Sleep -Seconds $PollIntervalSeconds
        }
    }
    else {
        Add-StepResult -Name 'request_movie' -Status 'skipped' -Details ("Reused existing item {0} without re-submitting TMDB {1}." -f $itemSummary.id, $TmdbId)
    }

    if ($null -eq $itemSummary) {
        throw ("Timed out waiting for requested movie TMDB {0} to appear in `/api/v1/items`." -f $TmdbId)
    }

    $summary.movie.item_id = $itemSummary.id
    $summary.movie.resolved_title = $itemSummary.title
    Write-JsonFile -Path (Join-Path $artifactDir 'item-summary.json') -Value $itemSummary
    Add-StepResult -Name 'item_discovered' -Status 'passed' -Details ("Resolved item id {0}." -f $itemSummary.id)

    $itemDetail = $null
    while ((Get-Date) -lt $deadline) {
        $itemDetail = Get-ProofItemDetail -ItemId $itemSummary.id
        $mediaEntries = @($itemDetail.media_entries)
        $directReady = [bool] $itemDetail.resolved_playback.direct_ready
        if ($mediaEntries.Count -gt 0 -or $directReady) {
            break
        }
        Start-Sleep -Seconds $PollIntervalSeconds
    }

    if ($null -eq $itemDetail) {
        throw ("Timed out waiting for item detail on {0}." -f $itemSummary.id)
    }

    $mediaEntries = @($itemDetail.media_entries)
    $directReady = [bool] $itemDetail.resolved_playback.direct_ready
    $summary.movie.final_state = $itemDetail.state
    $summary.movie.media_entry_count = $mediaEntries.Count
    $summary.movie.direct_ready = $directReady
    Write-JsonFile -Path (Join-Path $artifactDir 'item-detail.json') -Value $itemDetail

    if ($mediaEntries.Count -le 0 -and -not $directReady) {
        throw ("Item {0} never reached media-entry or direct-ready state before timeout. Last state: {1}" -f $itemSummary.id, $itemDetail.state)
    }

    Add-StepResult -Name 'item_ready_for_mount' -Status 'passed' -Details ("State={0}; media_entries={1}; direct_ready={2}" -f $itemDetail.state, $mediaEntries.Count, $directReady)

    $resolvedTitle = if ([string]::IsNullOrWhiteSpace($itemSummary.title)) { $Title } else { $itemSummary.title }
    $summary.movie.resolved_title = $resolvedTitle
    $mountCategory = if ($MediaType -eq 'tv') { 'shows' } else { 'movies' }
    $mountArtifactLabel = if ($MediaType -eq 'tv') { 'show episode file' } else { 'movie file' }
    $quotedMountRoot = ConvertTo-BashSingleQuoted -Value $MountRoot
    $quotedTitle = ConvertTo-BashSingleQuoted -Value $resolvedTitle
    $mountListingCommand = "find $quotedMountRoot/$mountCategory -type f 2>/dev/null | grep -i --fixed-strings -- $quotedTitle | head -n 1"
    $mountDeadline = (Get-Date).AddSeconds($MountVisibilityTimeoutSeconds)
    $mountedFile = $null
    while ((Get-Date) -lt $mountDeadline) {
        $mountedFileCandidate = Invoke-WslBash -Command $mountListingCommand | Select-Object -First 1
        if ($null -ne $mountedFileCandidate) {
            $mountedFile = $mountedFileCandidate.ToString().Trim()
            if (-not [string]::IsNullOrWhiteSpace($mountedFile)) {
                break
            }
        }
        Start-Sleep -Seconds 2
    }
    if ([string]::IsNullOrWhiteSpace($mountedFile)) {
        Invoke-WslBash -Command "find $quotedMountRoot -maxdepth 3 -type f 2>/dev/null | head -n 50" | Set-Content -Path (Join-Path $artifactDir 'mount-find.txt') -Encoding UTF8
        throw ("No mounted {0} matching title '{1}' was found under {2}." -f $mountArtifactLabel, $resolvedTitle, $MountRoot)
    }

    $summary.mount.file_path = $mountedFile
    Set-Content -Path (Join-Path $artifactDir 'mount-file.txt') -Encoding UTF8 -Value $mountedFile

    $quotedMountedFile = ConvertTo-BashSingleQuoted -Value $mountedFile
    $statSizeRaw = Invoke-WslBash -Command "stat -c '%s' $quotedMountedFile" | Select-Object -First 1
    if ($null -eq $statSizeRaw) {
        throw ("Failed to stat mounted file {0}." -f $mountedFile)
    }
    $bytesReadRaw = Invoke-WslBash -Command "head -c $MountedReadBytes $quotedMountedFile | wc -c" | Select-Object -First 1
    if ($null -eq $bytesReadRaw) {
        throw ("Failed to read mounted file {0}." -f $mountedFile)
    }
    $statSize = $statSizeRaw.ToString().Trim()
    $bytesRead = $bytesReadRaw.ToString().Trim()

    $summary.mount.stat_size_bytes = $statSize
    $summary.mount.bytes_read = $bytesRead

    Set-Content -Path (Join-Path $artifactDir 'mount-stat-size.txt') -Encoding UTF8 -Value $statSize
    Set-Content -Path (Join-Path $artifactDir 'mount-bytes-read.txt') -Encoding UTF8 -Value $bytesRead

    if ([int64]$bytesRead -lt [Math]::Min([int64]$MountedReadBytes, [int64]$statSize)) {
        throw ("Mounted read returned fewer bytes than expected. Requested {0}, got {1}." -f $MountedReadBytes, $bytesRead)
    }

    Add-StepResult -Name 'mounted_read' -Status 'passed' -Details ("Mounted file {0}; size={1}; bytes_read={2}" -f $mountedFile, $statSize, $bytesRead)

    if ($summary.media_server.configured) {
        $signalDeadline = (Get-Date).AddSeconds($MediaServerTimeoutSeconds)
        $signal = $null
        while ((Get-Date) -lt $signalDeadline) {
            $signal = Get-MediaServerSignal -ItemId $summary.movie.item_id
            if ($null -eq $signal) {
                $signal = Get-MediaServerRequestSignal
            }
            if ($null -ne $signal) {
                break
            }
            Start-Sleep -Seconds 2
        }
        if ($null -eq $signal) {
            $summary.media_server.signal_status = 'skipped'
            $summary.media_server.signal_container = 'none'
            Add-StepResult -Name 'media_server_scan_signal' -Status 'skipped' -Details ('No media-server notifier signal was observed for provider ''{0}'' within the polling window; continuing with direct provider proof.' -f $MediaServerProvider)
        }
        else {
            $summary.media_server.signal_status = $signal.status
            $summary.media_server.signal_container = $signal.container

            if ($signal.status -ne 'triggered') {
                throw ("Media-server notifier reported '{0}' for item {1}." -f $signal.status, $summary.movie.item_id)
            }

            Add-StepResult -Name 'media_server_scan_signal' -Status 'passed' -Details ("Observed {0} signal in {1}; triggered={2}; failed={3}; skipped={4}." -f $signal.status, $signal.container, $signal.triggered, $signal.failed, $signal.skipped)
        }

    }
    else {
        Add-StepResult -Name 'media_server_scan_signal' -Status 'skipped' -Details 'No media server provider configured for this proof run.'
    }

    if (-not [string]::IsNullOrWhiteSpace($MediaServerProvider)) {
        $providerContext = Resolve-MediaServerProofContext -Provider $MediaServerProvider
        if ($null -eq $providerContext) {
            throw ("Unable to resolve media-server proof context for provider '{0}'." -f $MediaServerProvider)
        }

        $summary.media_server.library_id = if ($providerContext.PSObject.Properties.Name -contains 'library_id') { [string] $providerContext.library_id } else { $null }
        $providerSearchTerm = Get-MediaServerSearchTerm -Provider $MediaServerProvider -FallbackSearchTerm $summary.movie.resolved_title
        $providerDeadline = (Get-Date).AddSeconds($MediaServerTimeoutSeconds)
        $providerSignal = $null
        while ((Get-Date) -lt $providerDeadline) {
            $providerSignal = Get-MediaServerVisibilitySignal -Context $providerContext -SearchTerm $providerSearchTerm
            if ($null -ne $providerSignal) {
                break
            }
            Start-Sleep -Seconds 2
        }

        if ($null -eq $providerSignal) {
            throw ("Timed out waiting for {0} visibility proof for search term '{1}'." -f $MediaServerProvider, $providerSearchTerm)
        }

        $providerItemId = if ($providerSignal.item.PSObject.Properties.Name -contains 'Id') { [string] $providerSignal.item.Id } elseif ($providerSignal.item.PSObject.Properties.Name -contains 'RatingKey') { [string] $providerSignal.item.RatingKey } else { '' }
        $summary.media_server.visibility_status = $providerSignal.status
        $summary.media_server.item_id = $providerItemId
        Add-StepResult -Name 'media_server_visibility' -Status 'passed' -Details ("Resolved {0} item {1} in library {2} for '{3}'." -f $MediaServerProvider, $providerItemId, $summary.media_server.library_id, $providerSearchTerm)

        $providerPlaybackInfo = Get-MediaServerPlaybackInfo -Context $providerContext -ItemId $providerItemId
        if ($null -eq $providerPlaybackInfo) {
            throw ("{0} playback-info proof returned no media sources for item {1}." -f $MediaServerProvider, $providerItemId)
        }

        $providerMediaSourceId = if ($providerPlaybackInfo.media_source.PSObject.Properties.Name -contains 'Id') { [string] $providerPlaybackInfo.media_source.Id } else { $null }
        $providerPlaybackPath = if ($providerPlaybackInfo.media_source.PSObject.Properties.Name -contains 'Path') { [string] $providerPlaybackInfo.media_source.Path } else { $null }
        $summary.media_server.playback_info_status = $providerPlaybackInfo.status
        $summary.media_server.playback_media_source_id = $providerMediaSourceId
        $summary.media_server.playback_path = $providerPlaybackPath
        $summary.media_server.topology = Get-MediaServerTopology -Context $providerContext -PlaybackPath $providerPlaybackPath
        Add-StepResult -Name 'media_server_playback_info' -Status 'passed' -Details ("Resolved {0} playback info for item {1}; media_source_id={2}; topology={3}." -f $MediaServerProvider, $providerItemId, $providerMediaSourceId, $summary.media_server.topology)

        $providerStreamSignal = Get-MediaServerStreamOpenSignal -Context $providerContext -ItemId $providerItemId -PlaybackInfo $providerPlaybackInfo
        if ($null -eq $providerStreamSignal) {
            throw ("{0} stream-open proof returned no response for item {1}." -f $MediaServerProvider, $providerItemId)
        }

        $summary.media_server.stream_open_status = $providerStreamSignal.status
        $summary.media_server.stream_open_status_code = $providerStreamSignal.status_code
        $summary.media_server.stream_open_bytes_read = $providerStreamSignal.bytes_read
        $summary.media_server.stream_open_content_range = $providerStreamSignal.content_range
        if ($providerStreamSignal.status -ne 'opened') {
            throw ("{0} stream-open proof failed for item {1} with status {2}." -f $MediaServerProvider, $providerItemId, $providerStreamSignal.status)
        }
        Add-StepResult -Name 'media_server_stream_open' -Status 'passed' -Details ("Opened {0} stream for item {1}; status_code={2}; bytes_read={3}." -f $MediaServerProvider, $providerItemId, $providerStreamSignal.status_code, $providerStreamSignal.bytes_read)

        $providerPlaybackStartSignal = Get-MediaServerPlaybackStartSignal -Context $providerContext -ItemId $providerItemId -PlaybackInfo $providerPlaybackInfo -Topology ([string] $summary.media_server.topology) -ArtifactDir $artifactDir
        if ($null -eq $providerPlaybackStartSignal) {
            throw ("{0} playback-start proof returned no response for item {1}." -f $MediaServerProvider, $providerItemId)
        }

        $summary.media_server.playback_start_status = $providerPlaybackStartSignal.status
        $summary.media_server.playback_start_status_code = $providerPlaybackStartSignal.status_code
        $summary.media_server.playback_start_bytes_read = $providerPlaybackStartSignal.bytes_read
        $summary.media_server.playback_start_content_type = $providerPlaybackStartSignal.content_type
        $summary.media_server.playback_start_details = $providerPlaybackStartSignal.details
        $summary.media_server.playback_start_log_path = $providerPlaybackStartSignal.log_path
        if ($providerPlaybackStartSignal.status -ne 'started') {
            throw ("{0} playback-start proof failed for item {1} with status {2}." -f $MediaServerProvider, $providerItemId, $providerPlaybackStartSignal.status)
        }
        Add-StepResult -Name 'media_server_playback_start' -Status 'passed' -Details ("Started {0} playback route for item {1}; status_code={2}; bytes_read={3}." -f $MediaServerProvider, $providerItemId, $providerPlaybackStartSignal.status_code, $providerPlaybackStartSignal.bytes_read)

        if (($MediaServerProvider -eq 'plex') -and ([string] $summary.media_server.topology -eq 'docker_wsl')) {
            $plexWslEvidence = Get-PlexWslEvidence -ArtifactDir $artifactDir
            $summary.media_server.wsl_host_mount_status = $plexWslEvidence.mount_status
            $summary.media_server.wsl_host_mount_details = $plexWslEvidence.mount_details
            $summary.media_server.wsl_persistent_log_path = $plexWslEvidence.persistent_log_path
            $summary.media_server.plex_wsl_evidence_path = $plexWslEvidence.evidence_path
            $summary.media_server.wsl_mount_check_status = $plexWslEvidence.mount_check_status
            $summary.media_server.wsl_mount_check_details = $plexWslEvidence.mount_check_details
            $summary.media_server.wsl_host_binary_status = $plexWslEvidence.host_binary_status
            $summary.media_server.wsl_host_binary_details = $plexWslEvidence.host_binary_details
            $summary.media_server.wsl_host_binary_check_status = $plexWslEvidence.host_binary_check_status
            $summary.media_server.wsl_host_binary_check_details = $plexWslEvidence.host_binary_check_details
            $summary.media_server.refresh_identity_status = $plexWslEvidence.refresh_identity_status
            $summary.media_server.refresh_identity_details = $plexWslEvidence.refresh_identity_details
            $summary.media_server.refresh_identity_check_status = $plexWslEvidence.refresh_identity_check_status
            $summary.media_server.refresh_identity_check_details = $plexWslEvidence.refresh_identity_check_details
            $summary.media_server.foreground_fetch_status = $plexWslEvidence.foreground_fetch_status
            $summary.media_server.foreground_fetch_details = $plexWslEvidence.foreground_fetch_details
            $summary.media_server.foreground_fetch_check_status = $plexWslEvidence.foreground_fetch_check_status
            $summary.media_server.foreground_fetch_check_details = $plexWslEvidence.foreground_fetch_check_details
            $summary.media_server.plex_wsl_evidence_status = $plexWslEvidence.overall_check_status
            $summary.media_server.plex_wsl_evidence_details = $plexWslEvidence.overall_check_details
            Add-StepResult -Name 'plex_wsl_mount_visibility' -Status $plexWslEvidence.mount_check_status -Details $plexWslEvidence.mount_check_details
            Add-StepResult -Name 'plex_wsl_host_binary_freshness' -Status $plexWslEvidence.host_binary_check_status -Details $plexWslEvidence.host_binary_check_details
            Add-StepResult -Name 'plex_wsl_refresh_identity_evidence' -Status $plexWslEvidence.refresh_identity_check_status -Details $plexWslEvidence.refresh_identity_check_details
            Add-StepResult -Name 'plex_wsl_foreground_fetch_evidence' -Status $plexWslEvidence.foreground_fetch_check_status -Details $plexWslEvidence.foreground_fetch_check_details
            Add-StepResult -Name 'plex_wsl_evidence' -Status $plexWslEvidence.overall_check_status -Details $plexWslEvidence.overall_check_details
        }
        else {
            Add-StepResult -Name 'plex_wsl_mount_visibility' -Status 'skipped' -Details 'WSL/Docker Plex evidence is only recorded for Plex playback proofs on the docker_wsl topology.'
            Add-StepResult -Name 'plex_wsl_host_binary_freshness' -Status 'skipped' -Details 'WSL/Docker Plex evidence is only recorded for Plex playback proofs on the docker_wsl topology.'
            Add-StepResult -Name 'plex_wsl_refresh_identity_evidence' -Status 'skipped' -Details 'WSL/Docker Plex evidence is only recorded for Plex playback proofs on the docker_wsl topology.'
            Add-StepResult -Name 'plex_wsl_foreground_fetch_evidence' -Status 'skipped' -Details 'WSL/Docker Plex evidence is only recorded for Plex playback proofs on the docker_wsl topology.'
            Add-StepResult -Name 'plex_wsl_evidence' -Status 'skipped' -Details 'WSL/Docker Plex evidence is only recorded for Plex playback proofs on the docker_wsl topology.'
        }

        $providerSessionSignal = Invoke-MediaServerSessionProof -Context $providerContext -ItemId $providerItemId -PlaybackInfo $providerPlaybackInfo
        $summary.media_server.session_status = $providerSessionSignal.status
        $summary.media_server.session_started_status = $providerSessionSignal.started_status
        $summary.media_server.session_progress_status = $providerSessionSignal.progress_status
        $summary.media_server.session_stopped_status = $providerSessionSignal.stopped_status
        if ([string] $providerSessionSignal.status -eq 'skipped') {
            Add-StepResult -Name 'media_server_session_reporting' -Status 'skipped' -Details ("No native session proof is implemented yet for provider '{0}'." -f $MediaServerProvider)
        }
        else {
            Add-StepResult -Name 'media_server_session_reporting' -Status 'passed' -Details ("Reported {0} playback session lifecycle; started={1}, progress={2}, stopped={3}." -f $MediaServerProvider, $providerSessionSignal.started_status, $providerSessionSignal.progress_status, $providerSessionSignal.stopped_status)
        }
    }
    else {
        Add-StepResult -Name 'media_server_visibility' -Status 'skipped' -Details 'No media server provider configured for this proof run.'
        Add-StepResult -Name 'media_server_playback_info' -Status 'skipped' -Details 'No media server provider configured for this proof run.'
        Add-StepResult -Name 'media_server_stream_open' -Status 'skipped' -Details 'No media server provider configured for this proof run.'
        Add-StepResult -Name 'media_server_playback_start' -Status 'skipped' -Details 'No media server provider configured for this proof run.'
        Add-StepResult -Name 'plex_wsl_evidence' -Status 'skipped' -Details 'No media server provider configured for this proof run.'
        Add-StepResult -Name 'media_server_session_reporting' -Status 'skipped' -Details 'No media server provider configured for this proof run.'
    }

    if (([string]::IsNullOrWhiteSpace($MediaServerProvider) -or ($MediaServerProvider -eq 'jellyfin')) -and -not [string]::IsNullOrWhiteSpace($JellyfinApiKey)) {
        $jellyfinContext = Resolve-JellyfinContext
        $jellyfinFfmpegSince = Get-Date
        $summary.media_server.jellyfin_library_id = $jellyfinContext.library_id
        $searchTerm = if ([string]::IsNullOrWhiteSpace($JellyfinSearchTerm)) { $summary.movie.resolved_title } else { $JellyfinSearchTerm }
        $jellyfinDeadline = (Get-Date).AddSeconds($MediaServerTimeoutSeconds)
        $jellyfinSignal = $null
        while ((Get-Date) -lt $jellyfinDeadline) {
            $jellyfinSignal = Get-JellyfinVisibilitySignal -Context $jellyfinContext -SearchTerm $searchTerm
            if ($null -ne $jellyfinSignal) {
                break
            }
            Start-Sleep -Seconds 2
        }

        if ($null -eq $jellyfinSignal) {
            throw ("Timed out waiting for Jellyfin visibility proof for search term '{0}'." -f $searchTerm)
        }

        $summary.media_server.jellyfin_visibility_status = $jellyfinSignal.status
        $summary.media_server.jellyfin_item_id = [string] $jellyfinSignal.item.Id
        $summary.media_server.jellyfin_library_id = $jellyfinContext.library_id
        Add-StepResult -Name 'jellyfin_visibility' -Status 'passed' -Details ("Resolved Jellyfin item {0} in library {1} for '{2}'." -f $jellyfinSignal.item.Id, $jellyfinContext.library_id, $searchTerm)

        $playbackInfo = Get-JellyfinPlaybackInfo -Context $jellyfinContext -JellyfinItemId ([string] $jellyfinSignal.item.Id)
        if ($null -eq $playbackInfo) {
            throw ("Jellyfin playback-info proof returned no media sources for item {0}." -f $jellyfinSignal.item.Id)
        }

        $summary.media_server.jellyfin_playback_info_status = $playbackInfo.status
        $summary.media_server.jellyfin_playback_media_source_id = [string] $playbackInfo.media_source.Id
        $summary.media_server.jellyfin_playback_path = [string] $playbackInfo.media_source.Path
        Add-StepResult -Name 'jellyfin_playback_info' -Status 'passed' -Details ("Resolved Jellyfin playback info for item {0}; media_source_id={1}." -f $jellyfinSignal.item.Id, $playbackInfo.media_source.Id)

        $container = [string] $playbackInfo.media_source.Container
        $playSessionId = [string] $playbackInfo.play_session_id
        $streamSignal = Get-JellyfinStreamOpenSignal -Context $jellyfinContext -JellyfinItemId ([string] $jellyfinSignal.item.Id) -MediaSourceId ([string] $playbackInfo.media_source.Id) -Container $container -PlaySessionId $playSessionId
        if ($null -eq $streamSignal) {
            throw ("Jellyfin stream-open proof returned no response for item {0}." -f $jellyfinSignal.item.Id)
        }

        $summary.media_server.jellyfin_stream_open_status = $streamSignal.status
        $summary.media_server.jellyfin_stream_open_status_code = $streamSignal.status_code
        $summary.media_server.jellyfin_stream_open_bytes_read = $streamSignal.bytes_read
        $summary.media_server.jellyfin_stream_open_content_range = $streamSignal.content_range

        if ($streamSignal.status -ne 'opened') {
            throw ("Jellyfin stream-open proof failed for item {0} with status {1}." -f $jellyfinSignal.item.Id, $streamSignal.status)
        }

        Add-StepResult -Name 'jellyfin_stream_open' -Status 'passed' -Details ("Opened Jellyfin stream for item {0}; status_code={1}; bytes_read={2}." -f $jellyfinSignal.item.Id, $streamSignal.status_code, $streamSignal.bytes_read)

        $sessionSignal = Invoke-JellyfinSessionProof -Context $jellyfinContext -JellyfinItemId ([string] $jellyfinSignal.item.Id) -MediaSourceId ([string] $playbackInfo.media_source.Id) -PlaySessionId $playSessionId
        $summary.media_server.jellyfin_session_status = $sessionSignal.status
        $summary.media_server.jellyfin_session_started_status = $sessionSignal.started_status
        $summary.media_server.jellyfin_session_progress_status = $sessionSignal.progress_status
        $summary.media_server.jellyfin_session_stopped_status = $sessionSignal.stopped_status
        Add-StepResult -Name 'jellyfin_session_reporting' -Status 'passed' -Details ("Reported Jellyfin playback session lifecycle; started={0}, progress={1}, stopped={2}." -f $sessionSignal.started_status, $sessionSignal.progress_status, $sessionSignal.stopped_status)
        $jellyfinDirectStream = Get-LatestJellyfinDirectStreamDiagnostic -Since $jellyfinFfmpegSince -ArtifactDir $artifactDir
        if ($null -eq $jellyfinDirectStream) {
            Add-StepResult -Name 'jellyfin_directstream_diagnostic' -Status 'skipped' -Details 'No recent Jellyfin DirectStream ffmpeg log was produced during this proof window.'
        }
        else {
            $summary.media_server.jellyfin_directstream_status = $jellyfinDirectStream.status
            $summary.media_server.jellyfin_directstream_details = $jellyfinDirectStream.details
            $summary.media_server.jellyfin_directstream_log_path = $jellyfinDirectStream.log_path
            $summary.media_server.jellyfin_directstream_cached_codec = $jellyfinDirectStream.cached_video_codec
            $summary.media_server.jellyfin_directstream_actual_codec = $jellyfinDirectStream.actual_video_codec
            $jellyfinDirectStreamStepStatus = if (@('failed', 'metadata_mismatch') -contains [string] $jellyfinDirectStream.status) { 'failed' } else { 'passed' }
            $jellyfinDirectStreamDetails = if ([string]::IsNullOrWhiteSpace([string] $jellyfinDirectStream.details)) { 'Captured recent Jellyfin DirectStream ffmpeg log.' } else { [string] $jellyfinDirectStream.details }
            Add-StepResult -Name 'jellyfin_directstream_diagnostic' -Status $jellyfinDirectStreamStepStatus -Details $jellyfinDirectStreamDetails

            if (($jellyfinDirectStream.status -eq 'metadata_mismatch') -and (-not [string]::IsNullOrWhiteSpace([string] $jellyfinSignal.item.Id))) {
                $refreshResult = Invoke-JellyfinMetadataRefresh -Context $jellyfinContext -JellyfinItemId ([string] $jellyfinSignal.item.Id) -ExpectedVideoCodec ([string] $jellyfinDirectStream.actual_video_codec)
                $summary.media_server.jellyfin_directstream_refresh_status = $refreshResult.status
                $summary.media_server.jellyfin_directstream_refresh_details = $refreshResult.details
                $summary.media_server.jellyfin_directstream_refreshed_codec = $refreshResult.refreshed_video_codec
                $refreshStepStatus = if ([string] $refreshResult.status -eq 'refreshed') { 'passed' } else { 'failed' }
                Add-StepResult -Name 'jellyfin_directstream_refresh' -Status $refreshStepStatus -Details ([string] $refreshResult.details)
            }
            else {
                Add-StepResult -Name 'jellyfin_directstream_refresh' -Status 'skipped' -Details 'DirectStream codec cache recovery was not needed for this proof window.'
            }
        }
    }
    else {
        Add-StepResult -Name 'jellyfin_visibility' -Status 'skipped' -Details 'No Jellyfin API key configured for this proof run.'
        Add-StepResult -Name 'jellyfin_playback_info' -Status 'skipped' -Details 'No Jellyfin API key configured for this proof run.'
        Add-StepResult -Name 'jellyfin_stream_open' -Status 'skipped' -Details 'No Jellyfin API key configured for this proof run.'
        Add-StepResult -Name 'jellyfin_session_reporting' -Status 'skipped' -Details 'No Jellyfin API key configured for this proof run.'
        Add-StepResult -Name 'jellyfin_directstream_diagnostic' -Status 'skipped' -Details 'No Jellyfin API key configured for this proof run.'
        Add-StepResult -Name 'jellyfin_directstream_refresh' -Status 'skipped' -Details 'No Jellyfin API key configured for this proof run.'
    }

    if ($ProofStaleDirectRefresh) {
        $activeDirectEntry = @($itemDetail.media_entries | Where-Object { [bool] $_.active_for_direct }) | Select-Object -First 1
        if ($null -eq $activeDirectEntry) {
            throw ("Cannot run stale direct-refresh proof for item {0} because no active direct media entry is exposed in item detail." -f $summary.movie.item_id)
        }

        $originalUnrestrictedUrl = [string] $activeDirectEntry.unrestricted_url
        $staleUrl = "http://127.0.0.1:1/playback-proof-stale/$($summary.movie.item_id)"
        $staleEntryId = Force-ActiveDirectMediaEntryStale -ItemId $summary.movie.item_id -StaleUrl $staleUrl
        if ([string]::IsNullOrWhiteSpace($staleEntryId)) {
            throw ("Failed to identify the active direct media entry while forcing stale unrestricted_url for item {0}." -f $summary.movie.item_id)
        }

        $summary.media_server.stale_refresh_entry_id = $staleEntryId
        Add-StepResult -Name 'stale_direct_refresh_mutation' -Status 'passed' -Details (
            "Forced active direct media entry {0} from unrestricted_url={1} to stale_url={2}." -f $staleEntryId, $originalUnrestrictedUrl, $staleUrl
        )

        $staleRefreshSignal = Invoke-DirectPlaybackRouteRangeRead -ItemId $summary.movie.item_id -ApiKey $ApiKey -Bytes 1024 -ArtifactPrefix 'stale-refresh-initial'
        $summary.media_server.stale_refresh_initial_status_code = $staleRefreshSignal.status_code
        $summary.media_server.stale_refresh_initial_body = $staleRefreshSignal.body_preview

        if (@(200, 206) -contains [int] $staleRefreshSignal.status_code) {
            $summary.media_server.stale_refresh_route_status_code = $staleRefreshSignal.status_code
            $summary.media_server.stale_refresh_bytes_read = $staleRefreshSignal.bytes_read
            $summary.media_server.stale_refresh_content_range = $staleRefreshSignal.content_range
            $summary.media_server.stale_refresh_attempt_count = 1

            $itemDetailAfterStaleRefresh = Get-ProofItemDetail -ItemId $summary.movie.item_id
            Write-JsonFile -Path (Join-Path $artifactDir 'item-detail-after-stale-refresh.json') -Value $itemDetailAfterStaleRefresh
            $refreshedActiveDirectEntry = @($itemDetailAfterStaleRefresh.media_entries | Where-Object { [bool] $_.active_for_direct }) | Select-Object -First 1
            $refreshedUnrestrictedUrl = if ($null -ne $refreshedActiveDirectEntry) { [string] $refreshedActiveDirectEntry.unrestricted_url } else { $null }
            $summary.media_server.stale_refresh_recovered_url = $refreshedUnrestrictedUrl

            if (-not [string]::IsNullOrWhiteSpace($refreshedUnrestrictedUrl) -and $refreshedUnrestrictedUrl -ne $staleUrl) {
                $summary.media_server.stale_refresh_status = 'refreshed_inline_persisted'
                Add-StepResult -Name 'stale_direct_refresh' -Status 'passed' -Details (
                    "Route reopened direct playback inline after forced stale URL and persisted a refreshed unrestricted_url; status_code={0}; bytes_read={1}; refreshed_url={2}." -f $staleRefreshSignal.status_code, $staleRefreshSignal.bytes_read, $refreshedUnrestrictedUrl
                )
            }
            else {
                $summary.media_server.stale_refresh_status = 'refreshed_inline_unpersisted'
                Add-StepResult -Name 'stale_direct_refresh' -Status 'passed' -Details (
                    "Route reopened direct playback inline after forced stale URL, but persisted item detail still reports the stale unrestricted_url; status_code={0}; bytes_read={1}; current_url={2}." -f $staleRefreshSignal.status_code, $staleRefreshSignal.bytes_read, $refreshedUnrestrictedUrl
                )
            }
        }
        elseif ([int] $staleRefreshSignal.status_code -eq 503) {
            Add-StepResult -Name 'stale_direct_refresh_initial_probe' -Status 'passed' -Details (
                "Initial stale direct probe returned 503 with stable detail; waiting for recovery path to refresh the selected direct lease. body={0}" -f $staleRefreshSignal.body_preview
            )

            $recovery = Wait-DirectPlaybackRouteRecovery -ItemId $summary.movie.item_id -ApiKey $ApiKey -StaleUrl $staleUrl -TimeoutSeconds 30 -PollSeconds 2 -Bytes 1024
            $summary.media_server.stale_refresh_attempt_count = 1 + [int] $recovery.attempt_count
            if ($null -ne $recovery.probe) {
                $summary.media_server.stale_refresh_route_status_code = $recovery.probe.status_code
                $summary.media_server.stale_refresh_bytes_read = $recovery.probe.bytes_read
                $summary.media_server.stale_refresh_content_range = $recovery.probe.content_range
            }

            if ($null -ne $recovery.detail) {
                Write-JsonFile -Path (Join-Path $artifactDir 'item-detail-after-stale-refresh.json') -Value $recovery.detail
            }

            if ($recovery.status -eq 'timeout') {
                $lastRecoveryStatus = if ($null -ne $recovery.probe) { $recovery.probe.status_code } else { 'none' }
                $lastRecoveryUrl = if ($null -ne $recovery.refreshed_url) { $recovery.refreshed_url } else { 'none' }
                $timeoutMessage = "Stale direct-refresh proof did not recover within timeout. initial_status={0}; last_status={1}; last_url={2}" -f $staleRefreshSignal.status_code, $lastRecoveryStatus, $lastRecoveryUrl
                throw $timeoutMessage
            }

            if ($recovery.status -eq 'recovered_persisted') {
                $summary.media_server.stale_refresh_status = 'recovered_after_retry_persisted'
            }
            else {
                $summary.media_server.stale_refresh_status = 'recovered_after_retry_unpersisted'
            }
            $summary.media_server.stale_refresh_recovered_url = [string] $recovery.refreshed_url
            $recoveryAttempts = 1 + [int] $recovery.attempt_count
            if ($recovery.status -eq 'recovered_persisted') {
                $recoveryMessage = "Initial stale direct probe returned 503, then recovered after retry and persisted a refreshed unrestricted_url; final_status={0}; bytes_read={1}; refreshed_url={2}; attempts={3}." -f $recovery.probe.status_code, $recovery.probe.bytes_read, $recovery.refreshed_url, $recoveryAttempts
            }
            else {
                $recoveryMessage = "Initial stale direct probe returned 503, then the route recovered after retry, but persisted item detail still reports the stale unrestricted_url; final_status={0}; bytes_read={1}; current_url={2}; attempts={3}." -f $recovery.probe.status_code, $recovery.probe.bytes_read, $recovery.refreshed_url, $recoveryAttempts
            }
            Add-StepResult -Name 'stale_direct_refresh' -Status 'passed' -Details $recoveryMessage
        }
        else {
            throw ("Stale direct-refresh proof returned unexpected initial route status {0} for item {1}." -f $staleRefreshSignal.status_code, $summary.movie.item_id)
        }
    }
    else {
        Add-StepResult -Name 'stale_direct_refresh' -Status 'skipped' -Details 'Stale direct-refresh proof disabled for this run.'
    }

    $finalStreamStatus = Get-BackendJson -Uri "$BackendUrl/api/v1/stream/status"
    Write-JsonFile -Path (Join-Path $artifactDir 'stream-status-final.json') -Value $finalStreamStatus
    Add-StepResult -Name 'stream_status_final' -Status 'passed' -Details 'Captured final `/api/v1/stream/status` snapshot.'

    Set-Content -Path (Join-Path $artifactDir 'final-stage-before-summary.txt') -Encoding UTF8 -Value 'before-summary'
    Write-SummaryFile -Path (Join-Path $artifactDir 'summary.json') -Summary $summary
    Set-Content -Path (Join-Path $artifactDir 'final-stage-after-summary.txt') -Encoding UTF8 -Value 'after-summary'
    Save-DockerEvidence -ArtifactDir $artifactDir
    Set-Content -Path (Join-Path $artifactDir 'final-stage-after-docker-evidence.txt') -Encoding UTF8 -Value 'after-docker-evidence'

    Write-Host ('[playback-proof] PASS. Artifact directory: {0}' -f $artifactDir) -ForegroundColor Green
    Write-Host ('[playback-proof] Item ID: {0}' -f $summary.movie.item_id) -ForegroundColor White
    Write-Host ('[playback-proof] Final state: {0}' -f $summary.movie.final_state) -ForegroundColor White
    Write-Host ('[playback-proof] Mounted file: {0}' -f $summary.mount.file_path) -ForegroundColor White
}
catch {
    Add-StepResult -Name 'playback_proof' -Status 'failed' -Details $_.Exception.Message
    Set-Content -Path (Join-Path $artifactDir 'final-stage-before-fail-summary.txt') -Encoding UTF8 -Value 'before-fail-summary'
    Write-SummaryFile -Path (Join-Path $artifactDir 'summary.json') -Summary $summary
    Set-Content -Path (Join-Path $artifactDir 'final-stage-after-fail-summary.txt') -Encoding UTF8 -Value 'after-fail-summary'
    Save-DockerEvidence -ArtifactDir $artifactDir
    Set-Content -Path (Join-Path $artifactDir 'final-stage-after-fail-docker-evidence.txt') -Encoding UTF8 -Value 'after-fail-docker-evidence'
    Write-Host ('[playback-proof] FAIL. Artifact directory: {0}' -f $artifactDir) -ForegroundColor Red
    throw
}
finally {
    if ($null -ne $originalSettingsPayload) {
        try {
            Restore-SettingsPayload -Payload $originalSettingsPayload
        }
        catch {
            Write-Warning ("[playback-proof] failed to restore original settings payload: {0}" -f $_.Exception.Message)
        }
    }

    if ($StopWhenDone) {
        & $script:StopScript
    }
}


