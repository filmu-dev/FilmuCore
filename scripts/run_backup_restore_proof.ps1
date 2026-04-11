param(
    [string]$ArtifactDir = "artifacts/operations/backup-restore",
    [string]$PostgresDsn = $env:FILMU_PY_POSTGRES_DSN,
    [string]$RestorePostgresDsn = $env:FILMU_PY_RESTORE_POSTGRES_DSN,
    [string]$SettingsPath = "settings.json",
    [switch]$SkipDatabase,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$startedAt = Get-Date
New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null

$summary = [ordered]@{
    started_at = $startedAt.ToUniversalTime().ToString("o")
    completed_at = $null
    status = "running"
    artifact_dir = (Resolve-Path $ArtifactDir).Path
    database_backup = [ordered]@{
        requested = -not $SkipDatabase.IsPresent
        captured = $false
        restored = $false
        source_configured = -not [string]::IsNullOrWhiteSpace($PostgresDsn)
        restore_target_configured = -not [string]::IsNullOrWhiteSpace($RestorePostgresDsn)
        artifact = $null
        error = $null
    }
    settings_backup = [ordered]@{
        requested = Test-Path -LiteralPath $SettingsPath
        captured = $false
        artifact = $null
        error = $null
    }
    smoke_checks = @()
}

function Add-SmokeCheck {
    param(
        [string]$Name,
        [string]$Status,
        [string]$Details
    )
    $script:summary.smoke_checks += [ordered]@{
        name = $Name
        status = $Status
        details = $Details
    }
}

try {
    if (Test-Path -LiteralPath $SettingsPath) {
        $settingsArtifact = Join-Path $ArtifactDir "settings.backup.json"
        Copy-Item -LiteralPath $SettingsPath -Destination $settingsArtifact -Force
        $summary.settings_backup.captured = $true
        $summary.settings_backup.artifact = $settingsArtifact
        Add-SmokeCheck -Name "settings_backup_exists" -Status "passed" -Details $settingsArtifact
    } else {
        Add-SmokeCheck -Name "settings_backup_exists" -Status "skipped" -Details "settings file not present"
    }

    if (-not $SkipDatabase.IsPresent) {
        if ([string]::IsNullOrWhiteSpace($PostgresDsn)) {
            Add-SmokeCheck -Name "database_dsn_configured" -Status "failed" -Details "FILMU_PY_POSTGRES_DSN is empty"
        } elseif ($DryRun.IsPresent) {
            Add-SmokeCheck -Name "database_backup_capture" -Status "skipped" -Details "dry run"
        } else {
            $pgDump = Get-Command pg_dump -ErrorAction SilentlyContinue
            if ($null -eq $pgDump) {
                Add-SmokeCheck -Name "pg_dump_available" -Status "failed" -Details "pg_dump is not on PATH"
            } else {
                $dbArtifact = Join-Path $ArtifactDir "filmu-backup.sql"
                & $pgDump.Source --dbname $PostgresDsn --file $dbArtifact --no-owner --no-privileges
                $summary.database_backup.captured = Test-Path -LiteralPath $dbArtifact
                $summary.database_backup.artifact = $dbArtifact
                Add-SmokeCheck -Name "database_backup_capture" -Status "passed" -Details $dbArtifact
            }
        }

        if (-not [string]::IsNullOrWhiteSpace($RestorePostgresDsn) -and $summary.database_backup.captured) {
            $psql = Get-Command psql -ErrorAction SilentlyContinue
            if ($null -eq $psql) {
                Add-SmokeCheck -Name "isolated_restore" -Status "failed" -Details "psql is not on PATH"
            } elseif ($DryRun.IsPresent) {
                Add-SmokeCheck -Name "isolated_restore" -Status "skipped" -Details "dry run"
            } else {
                & $psql.Source $RestorePostgresDsn -v ON_ERROR_STOP=1 -f $summary.database_backup.artifact
                $summary.database_backup.restored = $true
                Add-SmokeCheck -Name "isolated_restore" -Status "passed" -Details "restore target accepted backup SQL"
            }
        } else {
            Add-SmokeCheck -Name "isolated_restore" -Status "skipped" -Details "restore DSN or backup artifact missing"
        }
    }

    $failed = @($summary.smoke_checks | Where-Object { $_.status -eq "failed" })
    $summary.status = if ($failed.Count -eq 0) { "passed" } else { "failed" }
} catch {
    $summary.status = "failed"
    Add-SmokeCheck -Name "proof_exception" -Status "failed" -Details $_.Exception.Message
} finally {
    $summary.completed_at = (Get-Date).ToUniversalTime().ToString("o")
    $summaryPath = Join-Path $ArtifactDir "restore-summary.json"
    $summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8
    Write-Host "Backup/restore proof summary: $summaryPath"
    if ($summary.status -ne "passed") {
        exit 1
    }
}
