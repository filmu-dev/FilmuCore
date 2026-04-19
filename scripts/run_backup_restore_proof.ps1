param(
    [string]$ArtifactDir = "artifacts/operations/backup-restore",
    [string]$PostgresDsn = $env:FILMU_PY_POSTGRES_DSN,
    [string]$RestorePostgresDsn = $env:FILMU_PY_RESTORE_POSTGRES_DSN,
    [string]$SettingsPath = "settings.json",
    [string]$ContractPath = "",
    [string]$PythonExecutable = "python",
    [string]$MigrationRevision = "head",
    [switch]$SkipDatabase,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($ContractPath)) {
    $ContractPath = Join-Path $repoRoot "ops\rollout\backup-restore-rehearsal.contract.json"
}
$contract = $null
$schemaVersion = 1
$artifactKind = "backup_restore_rehearsal"
$freshnessWindowHours = 24
$requiredTables = @(
    "alembic_version",
    "settings",
    "media_items",
    "streams",
    "item_requests",
    "control_plane_subscribers",
    "item_workflow_checkpoints"
)
if (Test-Path -LiteralPath $ContractPath) {
    $ContractPath = [System.IO.Path]::GetFullPath($ContractPath)
    $contract = Get-Content -LiteralPath $ContractPath -Raw | ConvertFrom-Json
    if ($contract.PSObject.Properties.Name -contains "schema_version") {
        $schemaVersion = [int]$contract.schema_version
    }
    if ($contract.PSObject.Properties.Name -contains "artifact_kind") {
        $artifactKind = [string]$contract.artifact_kind
    }
    if ($contract.PSObject.Properties.Name -contains "freshness_window_hours") {
        $freshnessWindowHours = [Math]::Max(1, [int]$contract.freshness_window_hours)
    }
    if ($contract.PSObject.Properties.Name -contains "migration_revision") {
        $MigrationRevision = [string]$contract.migration_revision
    }
    if ($contract.PSObject.Properties.Name -contains "required_tables") {
        $requiredTables = @($contract.required_tables | Where-Object {
                -not [string]::IsNullOrWhiteSpace([string]$_)
            } | ForEach-Object { [string]$_ })
    }
}

function Add-FailureReason {
    param([string]$Code)

    if (-not [string]::IsNullOrWhiteSpace($Code)) {
        $null = $script:failureReasonSet.Add($Code)
    }
}

function Add-RequiredAction {
    param([string]$Action)

    if (-not [string]::IsNullOrWhiteSpace($Action)) {
        $null = $script:requiredActionSet.Add($Action)
    }
}

function Add-SmokeCheck {
    param(
        [string]$Name,
        [string]$Status,
        [string]$Details,
        [string]$FailureReason = "",
        [string]$RequiredAction = ""
    )

    $script:summary.smoke_checks += [ordered]@{
        name = $Name
        status = $Status
        details = $Details
    }
    if ($Status -eq "failed") {
        Add-FailureReason -Code $FailureReason
        Add-RequiredAction -Action $RequiredAction
    }
}

function Invoke-RehearsalCommand {
    param([string[]]$Arguments)

    $previousPythonPath = $env:PYTHONPATH
    try {
        if ([string]::IsNullOrWhiteSpace($previousPythonPath)) {
            $env:PYTHONPATH = $repoRoot
        } else {
            $env:PYTHONPATH = "{0};{1}" -f $repoRoot, $previousPythonPath
        }
        $output = & $PythonExecutable @Arguments
        if ($LASTEXITCODE -ne 0) {
            throw ("python rehearsal command failed with exit code {0}" -f $LASTEXITCODE)
        }
        $text = ($output | Out-String).Trim()
        if ([string]::IsNullOrWhiteSpace($text)) {
            throw "python rehearsal command produced no JSON output"
        }
        return $text | ConvertFrom-Json -Depth 12
    } finally {
        if ($null -eq $previousPythonPath) {
            Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
        } else {
            $env:PYTHONPATH = $previousPythonPath
        }
    }
}

function Get-TableCountMap {
    param([object]$Snapshot)

    $map = @{}
    if ($null -eq $Snapshot) {
        return $map
    }
    foreach ($table in @($Snapshot.tables)) {
        $name = [string]$table.name
        if (-not [string]::IsNullOrWhiteSpace($name)) {
            $map[$name] = [int64]$table.row_count
        }
    }
    return $map
}

function Compare-SnapshotParity {
    param(
        [object]$SourceSnapshot,
        [object]$RestoreSnapshot,
        [string[]]$RequiredTables
    )

    if ($null -eq $SourceSnapshot -or $null -eq $RestoreSnapshot) {
        return [ordered]@{
            status = "skipped"
            required_tables = @($RequiredTables)
            compared_tables = @()
            missing_tables = @()
            row_count_mismatches = @()
            source_only_tables = @()
            restore_only_tables = @()
            source_revision = $null
            restored_revision = $null
            revision_observed = $false
            revision_match = $false
        }
    }

    $sourceCounts = Get-TableCountMap -Snapshot $SourceSnapshot
    $restoreCounts = Get-TableCountMap -Snapshot $RestoreSnapshot
    $expectedTables = if ($RequiredTables.Count -gt 0) {
        @($RequiredTables | Sort-Object -Unique)
    } else {
        @($sourceCounts.Keys | Sort-Object)
    }
    $comparedTables = @($sourceCounts.Keys | Where-Object { $restoreCounts.ContainsKey($_) } | Sort-Object)
    $missingTables = @($expectedTables | Where-Object { -not $restoreCounts.ContainsKey($_) })
    $sourceOnlyTables = @($sourceCounts.Keys | Where-Object { -not $restoreCounts.ContainsKey($_) } | Sort-Object)
    $restoreOnlyTables = @($restoreCounts.Keys | Where-Object { -not $sourceCounts.ContainsKey($_) } | Sort-Object)
    $mismatches = @()
    foreach ($tableName in $comparedTables) {
        if ($sourceCounts[$tableName] -ne $restoreCounts[$tableName]) {
            $mismatches += [ordered]@{
                table_name = $tableName
                source_row_count = $sourceCounts[$tableName]
                restored_row_count = $restoreCounts[$tableName]
            }
        }
    }
    $revisionObserved = (
        ($SourceSnapshot.PSObject.Properties.Name -contains "revision") -and
        ($RestoreSnapshot.PSObject.Properties.Name -contains "revision") -and
        $null -ne $SourceSnapshot.revision -and
        $null -ne $RestoreSnapshot.revision
    )
    $revisionMatch = $revisionObserved -and ([string]$SourceSnapshot.revision -eq [string]$RestoreSnapshot.revision)
    $status = if (($missingTables.Count -eq 0) -and ($mismatches.Count -eq 0) -and (($revisionObserved -eq $false) -or $revisionMatch)) {
        "passed"
    } else {
        "failed"
    }
    return [ordered]@{
        status = $status
        required_tables = $expectedTables
        compared_tables = $comparedTables
        missing_tables = $missingTables
        row_count_mismatches = $mismatches
        source_only_tables = $sourceOnlyTables
        restore_only_tables = $restoreOnlyTables
        source_revision = if ($SourceSnapshot.PSObject.Properties.Name -contains "revision") { $SourceSnapshot.revision } else { $null }
        restored_revision = if ($RestoreSnapshot.PSObject.Properties.Name -contains "revision") { $RestoreSnapshot.revision } else { $null }
        revision_observed = $revisionObserved
        revision_match = $revisionMatch
    }
}

$startedAt = Get-Date
New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null
$artifactPath = (Resolve-Path -LiteralPath $ArtifactDir).Path
$capturedAt = $startedAt.ToUniversalTime().ToString("o")
$expiresAt = $startedAt.ToUniversalTime().AddHours($freshnessWindowHours).ToString("o")
$failureReasonSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$requiredActionSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

$summary = [ordered]@{
    schema_version = $schemaVersion
    artifact_kind = $artifactKind
    captured_at = $capturedAt
    expires_at = $expiresAt
    freshness_window_hours = $freshnessWindowHours
    started_at = $capturedAt
    completed_at = $null
    status = "running"
    contract_path = if ($null -ne $contract) { $ContractPath } else { $null }
    artifact_dir = $artifactPath
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
    source_snapshot = $null
    restore_snapshot = $null
    snapshot_parity = [ordered]@{
        status = "skipped"
        required_tables = @($requiredTables)
        compared_tables = @()
        missing_tables = @()
        row_count_mismatches = @()
        source_only_tables = @()
        restore_only_tables = @()
        source_revision = $null
        restored_revision = $null
        revision_observed = $false
        revision_match = $false
    }
    migration_rehearsal = [ordered]@{
        requested = -not $SkipDatabase.IsPresent
        attempted = $false
        requested_revision = $MigrationRevision
        status = "skipped"
        before_revision = $null
        after_revision = $null
        changed = $false
        error = $null
    }
    smoke_checks = @()
    failure_reasons = @()
    required_actions = @()
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
            Add-SmokeCheck -Name "database_dsn_configured" -Status "failed" `
                -Details "FILMU_PY_POSTGRES_DSN is empty" `
                -FailureReason "backup_restore_source_dsn_missing" `
                -RequiredAction "configure_source_database_dsn"
        } else {
            if ($DryRun.IsPresent) {
                Add-SmokeCheck -Name "source_snapshot_capture" -Status "skipped" -Details "dry run"
            } else {
                try {
                    $summary.source_snapshot = Invoke-RehearsalCommand -Arguments @(
                        "-m", "filmu_py.db.rehearsal", "snapshot", "--dsn", $PostgresDsn
                    )
                    Add-SmokeCheck -Name "source_snapshot_capture" -Status "passed" `
                        -Details ("captured {0} tables at revision {1}" -f `
                            [int]$summary.source_snapshot.table_count, [string]$summary.source_snapshot.revision)
                } catch {
                    $summary.database_backup.error = $_.Exception.Message
                    Add-SmokeCheck -Name "source_snapshot_capture" -Status "failed" `
                        -Details $_.Exception.Message `
                        -FailureReason "backup_restore_source_snapshot_failed" `
                        -RequiredAction "repair_source_snapshot_capture"
                }
            }

            if ($DryRun.IsPresent) {
                Add-SmokeCheck -Name "database_backup_capture" -Status "skipped" -Details "dry run"
            } else {
                $pgDump = Get-Command pg_dump -ErrorAction SilentlyContinue
                if ($null -eq $pgDump) {
                    Add-SmokeCheck -Name "pg_dump_available" -Status "failed" `
                        -Details "pg_dump is not on PATH" `
                        -FailureReason "backup_restore_pg_dump_missing" `
                        -RequiredAction "install_postgres_client_tools"
                } else {
                    $dbArtifact = Join-Path $ArtifactDir "filmu-backup.sql"
                    & $pgDump.Source --dbname $PostgresDsn --file $dbArtifact --no-owner --no-privileges
                    if ($LASTEXITCODE -ne 0) {
                        $summary.database_backup.error = "pg_dump exited with code $LASTEXITCODE"
                        Add-SmokeCheck -Name "database_backup_capture" -Status "failed" `
                            -Details $summary.database_backup.error `
                            -FailureReason "backup_restore_pg_dump_failed" `
                            -RequiredAction "repair_source_backup_capture"
                    } else {
                        $summary.database_backup.captured = Test-Path -LiteralPath $dbArtifact
                        $summary.database_backup.artifact = $dbArtifact
                        if ($summary.database_backup.captured) {
                            Add-SmokeCheck -Name "database_backup_capture" -Status "passed" -Details $dbArtifact
                        } else {
                            $summary.database_backup.error = "pg_dump completed but did not create $dbArtifact"
                            Add-SmokeCheck -Name "database_backup_capture" -Status "failed" `
                                -Details $summary.database_backup.error `
                                -FailureReason "backup_restore_pg_dump_artifact_missing" `
                                -RequiredAction "repair_source_backup_capture"
                        }
                    }
                }
            }
        }

        if (-not [string]::IsNullOrWhiteSpace($RestorePostgresDsn) -and $summary.database_backup.captured) {
            if ($DryRun.IsPresent) {
                Add-SmokeCheck -Name "isolated_restore" -Status "skipped" -Details "dry run"
            } else {
                $psql = Get-Command psql -ErrorAction SilentlyContinue
                if ($null -eq $psql) {
                    Add-SmokeCheck -Name "isolated_restore" -Status "failed" `
                        -Details "psql is not on PATH" `
                        -FailureReason "backup_restore_psql_missing" `
                        -RequiredAction "install_postgres_client_tools"
                } else {
                    & $psql.Source $RestorePostgresDsn -v ON_ERROR_STOP=1 -f $summary.database_backup.artifact
                    if ($LASTEXITCODE -ne 0) {
                        $summary.database_backup.error = "psql exited with code $LASTEXITCODE"
                        Add-SmokeCheck -Name "isolated_restore" -Status "failed" `
                            -Details $summary.database_backup.error `
                            -FailureReason "backup_restore_restore_failed" `
                            -RequiredAction "repair_restore_target_or_backup"
                    } else {
                        $summary.database_backup.restored = $true
                        Add-SmokeCheck -Name "isolated_restore" -Status "passed" `
                            -Details "restore target accepted backup SQL"
                    }
                }
            }
        } else {
            Add-SmokeCheck -Name "isolated_restore" -Status "skipped" `
                -Details "restore DSN or backup artifact missing"
        }

        if ($summary.database_backup.restored -and -not $DryRun.IsPresent) {
            try {
                $summary.migration_rehearsal = Invoke-RehearsalCommand -Arguments @(
                    "-m", "filmu_py.db.rehearsal", "migrate",
                    "--dsn", $RestorePostgresDsn,
                    "--revision", $MigrationRevision
                )
                if ([string]$summary.migration_rehearsal.status -eq "passed") {
                    Add-SmokeCheck -Name "migration_rehearsal" -Status "passed" `
                        -Details ("before={0}; after={1}" -f `
                            [string]$summary.migration_rehearsal.before_revision,
                            [string]$summary.migration_rehearsal.after_revision)
                } else {
                    Add-SmokeCheck -Name "migration_rehearsal" -Status "failed" `
                        -Details ([string]$summary.migration_rehearsal.error) `
                        -FailureReason "backup_restore_migration_rehearsal_failed" `
                        -RequiredAction "repair_restore_migration_path"
                }
            } catch {
                $summary.migration_rehearsal = [ordered]@{
                    requested = $true
                    attempted = $true
                    requested_revision = $MigrationRevision
                    status = "failed"
                    before_revision = $null
                    after_revision = $null
                    changed = $false
                    error = $_.Exception.Message
                }
                Add-SmokeCheck -Name "migration_rehearsal" -Status "failed" `
                    -Details $_.Exception.Message `
                    -FailureReason "backup_restore_migration_rehearsal_failed" `
                    -RequiredAction "repair_restore_migration_path"
            }

            try {
                $summary.restore_snapshot = Invoke-RehearsalCommand -Arguments @(
                    "-m", "filmu_py.db.rehearsal", "snapshot", "--dsn", $RestorePostgresDsn
                )
                Add-SmokeCheck -Name "restore_snapshot_capture" -Status "passed" `
                    -Details ("captured {0} tables at revision {1}" -f `
                        [int]$summary.restore_snapshot.table_count, [string]$summary.restore_snapshot.revision)
            } catch {
                Add-SmokeCheck -Name "restore_snapshot_capture" -Status "failed" `
                    -Details $_.Exception.Message `
                    -FailureReason "backup_restore_restore_snapshot_failed" `
                    -RequiredAction "repair_restore_snapshot_capture"
            }
        }

        if ($null -ne $summary.source_snapshot -and $null -ne $summary.restore_snapshot) {
            $summary.snapshot_parity = Compare-SnapshotParity `
                -SourceSnapshot $summary.source_snapshot `
                -RestoreSnapshot $summary.restore_snapshot `
                -RequiredTables $requiredTables
            if ([string]$summary.snapshot_parity.status -eq "passed") {
                Add-SmokeCheck -Name "snapshot_parity" -Status "passed" `
                    -Details ("compared={0}; required={1}" -f `
                        @($summary.snapshot_parity.compared_tables).Count,
                        @($summary.snapshot_parity.required_tables).Count)
            } else {
                Add-SmokeCheck -Name "snapshot_parity" -Status "failed" `
                    -Details (
                        "missing={0}; mismatches={1}; revision_match={2}" -f `
                        @($summary.snapshot_parity.missing_tables).Count,
                        @($summary.snapshot_parity.row_count_mismatches).Count,
                        [string]$summary.snapshot_parity.revision_match
                    ) `
                    -FailureReason "backup_restore_snapshot_parity_failed" `
                    -RequiredAction "repair_restore_snapshot_parity"
            }
        } elseif (-not $DryRun.IsPresent) {
            Add-SmokeCheck -Name "snapshot_parity" -Status "skipped" `
                -Details "source or restore snapshot unavailable"
        }
    }

    $failed = @($summary.smoke_checks | Where-Object { $_.status -eq "failed" })
    $summary.failure_reasons = @($failureReasonSet | Sort-Object)
    $summary.required_actions = @($requiredActionSet | Sort-Object)
    $summary.status = if ($failed.Count -eq 0) { "passed" } else { "failed" }
} catch {
    $summary.status = "failed"
    Add-SmokeCheck -Name "proof_exception" -Status "failed" `
        -Details $_.Exception.Message `
        -FailureReason "backup_restore_proof_exception" `
        -RequiredAction "inspect_backup_restore_proof_runner"
} finally {
    $summary.completed_at = (Get-Date).ToUniversalTime().ToString("o")
    $summary.failure_reasons = @($failureReasonSet | Sort-Object)
    $summary.required_actions = @($requiredActionSet | Sort-Object)
    $summaryPath = Join-Path $ArtifactDir "restore-summary.json"
    $summary | ConvertTo-Json -Depth 12 | Set-Content -Path $summaryPath -Encoding UTF8
    Write-Host "Backup/restore proof summary: $summaryPath"
    if ($summary.status -ne "passed") {
        exit 1
    }
}
