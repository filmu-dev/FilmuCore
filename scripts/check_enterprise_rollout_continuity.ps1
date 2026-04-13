param(
    [string] $RepoRoot = '',
    [string] $WindowsSoakArtifactsRoot = '',
    [string] $WindowsSoakHistoryRoot = '',
    [string] $WindowsSoakContractPath = '',
    [string] $PlaybackArtifactsRoot = '',
    [string] $PlaybackHistoryRoot = '',
    [string] $OperatorArtifactDir = '',
    [string] $OperatorHistoryRoot = '',
    [string] $OperatorRolloutContractPath = '',
    [int] $MaxEvidenceAgeHours = 72,
    [int] $MinimumSoakTrendRecords = 3,
    [int] $MinimumPlaybackTrendRecords = 3,
    [int] $MinimumOperatorTrendRecords = 3,
    [switch] $ProbeOperatorNow,
    [switch] $AllowOfflineOperator,
    [switch] $AllowBootstrap,
    [string] $OutputPath = 'artifacts/operations/enterprise-rollout/continuity-summary.json'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
$RepoRoot = [System.IO.Path]::GetFullPath($RepoRoot)

if ([string]::IsNullOrWhiteSpace($PlaybackArtifactsRoot)) {
    $PlaybackArtifactsRoot = Join-Path $RepoRoot 'playback-proof-artifacts'
}
$PlaybackArtifactsRoot = [System.IO.Path]::GetFullPath($PlaybackArtifactsRoot)

if ([string]::IsNullOrWhiteSpace($WindowsSoakArtifactsRoot)) {
    $WindowsSoakArtifactsRoot = Join-Path $PlaybackArtifactsRoot 'windows-native-stack'
}
$WindowsSoakArtifactsRoot = [System.IO.Path]::GetFullPath($WindowsSoakArtifactsRoot)

if ([string]::IsNullOrWhiteSpace($WindowsSoakHistoryRoot)) {
    $WindowsSoakHistoryRoot = [string]$env:FILMU_SOAK_HISTORY_ROOT
}
if ([string]::IsNullOrWhiteSpace($WindowsSoakHistoryRoot)) {
    $WindowsSoakHistoryRoot = Join-Path $WindowsSoakArtifactsRoot 'trend-history'
}
$WindowsSoakHistoryRoot = [System.IO.Path]::GetFullPath($WindowsSoakHistoryRoot)

if ([string]::IsNullOrWhiteSpace($WindowsSoakContractPath)) {
    $WindowsSoakContractPath = Join-Path $RepoRoot 'ops\rollout\windows-vfs-soak-program.contract.json'
}
if (Test-Path -LiteralPath $WindowsSoakContractPath) {
    $WindowsSoakContractPath = [System.IO.Path]::GetFullPath($WindowsSoakContractPath)
}

if ([string]::IsNullOrWhiteSpace($PlaybackHistoryRoot)) {
    $PlaybackHistoryRoot = [string]$env:FILMU_PLAYBACK_STABILITY_HISTORY_ROOT
}
if ([string]::IsNullOrWhiteSpace($PlaybackHistoryRoot)) {
    $PlaybackHistoryRoot = Join-Path $PlaybackArtifactsRoot 'stability-trend-history'
}
$PlaybackHistoryRoot = [System.IO.Path]::GetFullPath($PlaybackHistoryRoot)

if ([string]::IsNullOrWhiteSpace($OperatorArtifactDir)) {
    $OperatorArtifactDir = Join-Path $RepoRoot 'artifacts/operations/log-pipeline'
}
$OperatorArtifactDir = [System.IO.Path]::GetFullPath($OperatorArtifactDir)

if ([string]::IsNullOrWhiteSpace($OperatorHistoryRoot)) {
    $OperatorHistoryRoot = [string]$env:FILMU_LOG_PIPELINE_HISTORY_ROOT
}
if ([string]::IsNullOrWhiteSpace($OperatorHistoryRoot)) {
    $OperatorHistoryRoot = Join-Path $OperatorArtifactDir 'history'
}
$OperatorHistoryRoot = [System.IO.Path]::GetFullPath($OperatorHistoryRoot)

if ([string]::IsNullOrWhiteSpace($OperatorRolloutContractPath)) {
    $OperatorRolloutContractPath = Join-Path $RepoRoot 'ops\rollout\operator-log-pipeline.contract.json'
}
$operatorRolloutContract = $null
if (Test-Path -LiteralPath $OperatorRolloutContractPath) {
    $OperatorRolloutContractPath = [System.IO.Path]::GetFullPath($OperatorRolloutContractPath)
    $operatorRolloutContract = Get-Content -LiteralPath $OperatorRolloutContractPath -Raw | ConvertFrom-Json
}
$requiredOperatorGreenStreak = if (
    $null -ne $operatorRolloutContract -and
    ($operatorRolloutContract.PSObject.Properties.Name -contains 'minimum_green_streak')
) {
    [int]$operatorRolloutContract.minimum_green_streak
} else {
    $MinimumOperatorTrendRecords
}

$checks = [System.Collections.Generic.List[object]]::new()
function Add-Check {
    param(
        [string] $Section,
        [string] $Name,
        [bool] $Passed,
        [object] $Observed,
        [object] $Expected
    )
    $checks.Add([pscustomobject]@{
            section  = $Section
            name     = $Name
            passed   = $Passed
            observed = $Observed
            expected = $Expected
        })
}

function Get-LatestByPattern {
    param([string] $Root, [string] $Filter)
    if (-not (Test-Path -LiteralPath $Root)) {
        return $null
    }
    return Get-ChildItem -LiteralPath $Root -Filter $Filter -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
}

function Get-FileAgeHours {
    param([string] $Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }
    $lastWrite = (Get-Item -LiteralPath $Path).LastWriteTimeUtc
    return [Math]::Round(((Get-Date).ToUniversalTime() - $lastWrite).TotalHours, 3)
}

function Read-JsonFile {
    param([string] $Path)
    return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
}

function Get-ConsecutivePassedHistoryStreak {
    param(
        [string] $HistoryRoot,
        [string] $Filter
    )

    if (-not (Test-Path -LiteralPath $HistoryRoot)) {
        return 0
    }

    $records = @(
        Get-ChildItem -LiteralPath $HistoryRoot -Filter $Filter -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTimeUtc -Descending
    )
    $streak = 0
    foreach ($recordFile in $records) {
        $record = Read-JsonFile -Path $recordFile.FullName
        if ([string]($record.status ?? '') -ne 'passed') {
            break
        }
        $streak += 1
    }
    return $streak
}

function Get-LatestPlaybackStabilitySummaryPaths {
    param([string] $Root)

    if (-not (Test-Path -LiteralPath $Root)) {
        return @()
    }

    $latestByEnvironment = @{}
    $summaryFiles = @(
        Get-ChildItem -LiteralPath $Root -Filter 'stability-summary-*.json' -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTimeUtc -Descending
    )
    foreach ($summaryFile in $summaryFiles) {
        $environmentClass = '__unknown__'
        try {
            $summary = Read-JsonFile -Path $summaryFile.FullName
            $environmentClass = [string]($summary.environment_class ?? '')
        }
        catch {
            $environmentClass = '__unknown__'
        }
        if ([string]::IsNullOrWhiteSpace($environmentClass)) {
            $environmentClass = '__unknown__'
        }
        if (-not $latestByEnvironment.ContainsKey($environmentClass)) {
            $latestByEnvironment[$environmentClass] = $summaryFile.FullName
        }
    }

    return @($latestByEnvironment.Values)
}

$decompositionBudgets = [ordered]@{
    (Join-Path $RepoRoot 'filmu_py/services/media.py') = 5800
    (Join-Path $RepoRoot 'filmu_py/services/playback.py') = 4900
    (Join-Path $RepoRoot 'filmu_py/workers/tasks.py') = 3800
    (Join-Path $RepoRoot 'filmu_py/api/routes/stream.py') = 1600
}

foreach ($entry in $decompositionBudgets.GetEnumerator()) {
    $filePath = [string]$entry.Key
    $budget = [int]$entry.Value
    $exists = Test-Path -LiteralPath $filePath
    Add-Check -Section 'decomposition' -Name ("file_exists::{0}" -f (Split-Path -Leaf $filePath)) `
        -Passed:$exists -Observed:$exists -Expected:$true
    if (-not $exists) {
        continue
    }
    $lineCount = @(Get-Content -LiteralPath $filePath).Count
    Add-Check -Section 'decomposition' -Name ("line_budget::{0}" -f (Split-Path -Leaf $filePath)) `
        -Passed:($lineCount -le $budget) `
        -Observed:$lineCount `
        -Expected:("<={0}" -f $budget)
}

$boundaryImports = @(
    [pscustomobject]@{
        section = 'decomposition'
        file = Join-Path $RepoRoot 'filmu_py/services/media.py'
        token = 'media_path_inference'
        name = 'media_path_inference_boundary'
    }
    [pscustomobject]@{
        section = 'decomposition'
        file = Join-Path $RepoRoot 'filmu_py/services/media.py'
        token = 'media_stream_candidates'
        name = 'media_stream_candidates_boundary'
    }
    [pscustomobject]@{
        section = 'decomposition'
        file = Join-Path $RepoRoot 'filmu_py/services/playback.py'
        token = 'resolve_refresh_controller'
        name = 'playback_refresh_dispatch_boundary'
    }
    [pscustomobject]@{
        section = 'decomposition'
        file = Join-Path $RepoRoot 'filmu_py/workers/tasks.py'
        token = 'stage_isolation'
        name = 'worker_stage_isolation_boundary'
    }
    [pscustomobject]@{
        section = 'decomposition'
        file = Join-Path $RepoRoot 'filmu_py/workers/tasks.py'
        token = 'stage_scope'
        name = 'worker_stage_scope_boundary'
    }
    [pscustomobject]@{
        section = 'decomposition'
        file = Join-Path $RepoRoot 'filmu_py/api/routes/stream.py'
        token = 'runtime_status_payload'
        name = 'stream_runtime_status_boundary'
    }
    [pscustomobject]@{
        section = 'decomposition'
        file = Join-Path $RepoRoot 'filmu_py/api/routes/stream.py'
        token = 'stream_direct_serving'
        name = 'stream_direct_serving_boundary'
    }
)
foreach ($check in $boundaryImports) {
    $exists = Test-Path -LiteralPath $check.file
    if (-not $exists) {
        Add-Check -Section $check.section -Name $check.name -Passed:$false -Observed:'missing_file' -Expected:'token_present'
        continue
    }
    $source = Get-Content -LiteralPath $check.file -Raw
    $present = $source.Contains([string]$check.token)
    Add-Check -Section $check.section -Name $check.name -Passed:$present -Observed:$present -Expected:$true
}

Add-Check -Section 'soak_trends' -Name 'rollout_contract_present' `
    -Passed:(Test-Path -LiteralPath $WindowsSoakContractPath) `
    -Observed:$WindowsSoakContractPath `
    -Expected:'existing soak rollout contract'
Add-Check -Section 'operator_rollout' -Name 'rollout_contract_present' `
    -Passed:(Test-Path -LiteralPath $OperatorRolloutContractPath) `
    -Observed:$OperatorRolloutContractPath `
    -Expected:'existing operator rollout contract'

if ($ProbeOperatorNow) {
    $operatorScript = Join-Path $PSScriptRoot 'check_operator_log_pipeline_rollout.ps1'
    $operatorArgs = @(
        '-NoProfile',
        '-File',
        $operatorScript,
        '-ContractPath',
        $OperatorRolloutContractPath,
        '-ArtifactDir',
        $OperatorArtifactDir,
        '-HistoryRoot',
        $OperatorHistoryRoot
    )
    if ($AllowOfflineOperator) {
        $operatorArgs += '-AllowOffline'
    }
    & pwsh @operatorArgs
    if ($LASTEXITCODE -ne 0) {
        throw '[enterprise-rollout-continuity] operator rollout probe failed.'
    }
}

$soakTrendSummaryFile = Get-LatestByPattern -Root $WindowsSoakArtifactsRoot -Filter 'soak-trend-summary-*.json'
$soakTrendSummaryPath = if ($null -ne $soakTrendSummaryFile) { $soakTrendSummaryFile.FullName } else { $null }
$soakProgramSummaryFile = Get-LatestByPattern -Root $WindowsSoakArtifactsRoot -Filter 'soak-program-summary-*.json'
$soakProgramSummaryPath = if ($null -ne $soakProgramSummaryFile) { $soakProgramSummaryFile.FullName } else { $null }
Add-Check -Section 'soak_trends' -Name 'program_summary_present' `
    -Passed:(($null -ne $soakProgramSummaryPath) -or $AllowBootstrap) `
    -Observed:$soakProgramSummaryPath `
    -Expected:'latest soak-program-summary-*.json'
if ($null -ne $soakProgramSummaryPath) {
    $soakProgramSummary = Read-JsonFile -Path $soakProgramSummaryPath
    $soakProgramAgeHours = Get-FileAgeHours -Path $soakProgramSummaryPath
    $programContractPath = if ($soakProgramSummary.PSObject.Properties.Name -contains 'contract_path') {
        [string]$soakProgramSummary.contract_path
    } else {
        ''
    }
    Add-Check -Section 'soak_trends' -Name 'program_summary_status' `
        -Passed:([string]$soakProgramSummary.status -eq 'passed') `
        -Observed:([string]$soakProgramSummary.status) `
        -Expected:'passed'
    Add-Check -Section 'soak_trends' -Name 'program_contract_path_match' `
        -Passed:($programContractPath -eq $WindowsSoakContractPath -or $AllowBootstrap) `
        -Observed:$programContractPath `
        -Expected:$WindowsSoakContractPath
    Add-Check -Section 'soak_trends' -Name 'program_summary_freshness_hours' `
        -Passed:($null -ne $soakProgramAgeHours -and $soakProgramAgeHours -le $MaxEvidenceAgeHours) `
        -Observed:$soakProgramAgeHours `
        -Expected:("<={0}" -f $MaxEvidenceAgeHours)
    Add-Check -Section 'soak_trends' -Name 'program_environment_count' `
        -Passed:([int]($soakProgramSummary.environment_count ?? 0) -ge 2 -or $AllowBootstrap) `
        -Observed:([int]($soakProgramSummary.environment_count ?? 0)) `
        -Expected:'>=2'
}
Add-Check -Section 'soak_trends' -Name 'trend_summary_present' `
    -Passed:(($null -ne $soakTrendSummaryPath) -or $AllowBootstrap) `
    -Observed:$soakTrendSummaryPath `
    -Expected:'latest soak-trend-summary-*.json'
if ($null -ne $soakTrendSummaryPath) {
    $soakSummary = Read-JsonFile -Path $soakTrendSummaryPath
    $soakAgeHours = Get-FileAgeHours -Path $soakTrendSummaryPath
    Add-Check -Section 'soak_trends' -Name 'trend_summary_status' `
        -Passed:([string]$soakSummary.status -eq 'passed') `
        -Observed:([string]$soakSummary.status) `
        -Expected:'passed'
    Add-Check -Section 'soak_trends' -Name 'trend_summary_freshness_hours' `
        -Passed:($null -ne $soakAgeHours -and $soakAgeHours -le $MaxEvidenceAgeHours) `
        -Observed:$soakAgeHours `
        -Expected:("<={0}" -f $MaxEvidenceAgeHours)
}
$soakHistoryCount = if (Test-Path -LiteralPath $WindowsSoakHistoryRoot) {
    @(
        Get-ChildItem -LiteralPath $WindowsSoakHistoryRoot -Filter 'soak-trend-record-*.json' -File -ErrorAction SilentlyContinue
    ).Count
}
else {
    0
}
Add-Check -Section 'soak_trends' -Name 'trend_history_depth' `
    -Passed:($soakHistoryCount -ge $MinimumSoakTrendRecords -or $AllowBootstrap) `
    -Observed:$soakHistoryCount `
    -Expected:(">={0}" -f $MinimumSoakTrendRecords)

$playbackSummaryPaths = @(Get-LatestPlaybackStabilitySummaryPaths -Root $PlaybackArtifactsRoot)
$playbackTrendSummaryFile = Get-LatestByPattern -Root $PlaybackArtifactsRoot -Filter 'playback-stability-trend-summary-*.json'
$playbackTrendSummaryPath = if ($null -ne $playbackTrendSummaryFile) { $playbackTrendSummaryFile.FullName } else { $null }
if ($null -eq $playbackTrendSummaryPath) {
    $playbackStabilitySummaryCount = @($playbackSummaryPaths).Count
    Add-Check -Section 'playback_trends' -Name 'stability_summary_inputs_present' `
        -Passed:($playbackStabilitySummaryCount -gt 0 -or $AllowBootstrap) `
        -Observed:$playbackStabilitySummaryCount `
        -Expected:'>=1'
    if ($playbackStabilitySummaryCount -gt 0) {
        $playbackTrendScript = Join-Path $PSScriptRoot 'check_playback_stability_trends.ps1'
        $playbackTrendArgs = @(
            '-NoProfile',
        '-File',
        $playbackTrendScript,
        '-ArtifactsRoot',
        $PlaybackArtifactsRoot,
        '-HistoryRoot',
        $PlaybackHistoryRoot
        )
        if ($playbackSummaryPaths.Count -gt 0) {
            $playbackTrendArgs += '-SummaryPaths'
            $playbackTrendArgs += @($playbackSummaryPaths)
        }
        if ($AllowBootstrap) {
            $playbackTrendArgs += '-AllowBootstrap'
        }
        & pwsh @playbackTrendArgs
        if ($LASTEXITCODE -ne 0) {
            throw '[enterprise-rollout-continuity] playback stability trend probe failed.'
        }
        $playbackTrendSummaryFile = Get-LatestByPattern -Root $PlaybackArtifactsRoot -Filter 'playback-stability-trend-summary-*.json'
        $playbackTrendSummaryPath = if ($null -ne $playbackTrendSummaryFile) { $playbackTrendSummaryFile.FullName } else { $null }
    }
}

Add-Check -Section 'playback_trends' -Name 'trend_summary_present' `
    -Passed:(($null -ne $playbackTrendSummaryPath) -or $AllowBootstrap) `
    -Observed:$playbackTrendSummaryPath `
    -Expected:'latest playback-stability-trend-summary-*.json'
if ($null -ne $playbackTrendSummaryPath) {
    $playbackSummary = Read-JsonFile -Path $playbackTrendSummaryPath
    $playbackAgeHours = Get-FileAgeHours -Path $playbackTrendSummaryPath
    Add-Check -Section 'playback_trends' -Name 'trend_summary_status' `
        -Passed:([string]$playbackSummary.status -eq 'passed') `
        -Observed:([string]$playbackSummary.status) `
        -Expected:'passed'
    Add-Check -Section 'playback_trends' -Name 'trend_summary_freshness_hours' `
        -Passed:($null -ne $playbackAgeHours -and $playbackAgeHours -le $MaxEvidenceAgeHours) `
        -Observed:$playbackAgeHours `
        -Expected:("<={0}" -f $MaxEvidenceAgeHours)
}
$playbackHistoryCount = if (Test-Path -LiteralPath $PlaybackHistoryRoot) {
    @(
        Get-ChildItem -LiteralPath $PlaybackHistoryRoot -Filter 'playback-stability-trend-record-*.json' -File -ErrorAction SilentlyContinue
    ).Count
}
else {
    0
}
Add-Check -Section 'playback_trends' -Name 'trend_history_depth' `
    -Passed:($playbackHistoryCount -ge $MinimumPlaybackTrendRecords -or $AllowBootstrap) `
    -Observed:$playbackHistoryCount `
    -Expected:(">={0}" -f $MinimumPlaybackTrendRecords)

$operatorSummaryPath = Join-Path $OperatorArtifactDir 'log-pipeline-rollout-summary.json'
$operatorGreenStreak = Get-ConsecutivePassedHistoryStreak `
    -HistoryRoot $OperatorHistoryRoot `
    -Filter 'log-pipeline-rollout-record-*.json'
Add-Check -Section 'operator_rollout' -Name 'rollout_summary_present' `
    -Passed:((Test-Path -LiteralPath $operatorSummaryPath) -or $AllowBootstrap) `
    -Observed:$operatorSummaryPath `
    -Expected:'log-pipeline-rollout-summary.json present'
if (Test-Path -LiteralPath $operatorSummaryPath) {
    $operatorSummary = Read-JsonFile -Path $operatorSummaryPath
    $operatorAgeHours = Get-FileAgeHours -Path $operatorSummaryPath
    $operatorContractPath = if ($operatorSummary.PSObject.Properties.Name -contains 'contract_path') {
        [string]$operatorSummary.contract_path
    } else {
        ''
    }
    Add-Check -Section 'operator_rollout' -Name 'rollout_summary_status' `
        -Passed:([string]$operatorSummary.status -eq 'passed') `
        -Observed:([string]$operatorSummary.status) `
        -Expected:'passed'
    Add-Check -Section 'operator_rollout' -Name 'rollout_contract_path_match' `
        -Passed:($operatorContractPath -eq $OperatorRolloutContractPath -or $AllowBootstrap) `
        -Observed:$operatorContractPath `
        -Expected:$OperatorRolloutContractPath
    Add-Check -Section 'operator_rollout' -Name 'rollout_summary_freshness_hours' `
        -Passed:($null -ne $operatorAgeHours -and $operatorAgeHours -le $MaxEvidenceAgeHours) `
        -Observed:$operatorAgeHours `
        -Expected:("<={0}" -f $MaxEvidenceAgeHours)
    if ($operatorSummary.PSObject.Properties.Name -contains 'green_streak') {
        $operatorGreenStreak = [int]($operatorSummary.green_streak ?? $operatorGreenStreak)
    }
    Add-Check -Section 'operator_rollout' -Name 'rollout_green_streak' `
        -Passed:($operatorGreenStreak -ge $requiredOperatorGreenStreak -or $AllowBootstrap) `
        -Observed:$operatorGreenStreak `
        -Expected:(">={0}" -f $requiredOperatorGreenStreak)
}
$operatorHistoryCount = if (Test-Path -LiteralPath $OperatorHistoryRoot) {
    @(
        Get-ChildItem -LiteralPath $OperatorHistoryRoot -Filter 'log-pipeline-rollout-record-*.json' -File -ErrorAction SilentlyContinue
    ).Count
}
else {
    0
}
Add-Check -Section 'operator_rollout' -Name 'rollout_history_depth' `
    -Passed:($operatorHistoryCount -ge $MinimumOperatorTrendRecords -or $AllowBootstrap) `
    -Observed:$operatorHistoryCount `
    -Expected:(">={0}" -f $MinimumOperatorTrendRecords)

$failedChecks = @($checks | Where-Object { -not $_.passed })
$summary = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString('o')
    repo_root = $RepoRoot
    windows_soak_artifacts_root = $WindowsSoakArtifactsRoot
    windows_soak_history_root = $WindowsSoakHistoryRoot
    windows_soak_contract_path = $WindowsSoakContractPath
    playback_artifacts_root = $PlaybackArtifactsRoot
    playback_history_root = $PlaybackHistoryRoot
    operator_artifact_dir = $OperatorArtifactDir
    operator_history_root = $OperatorHistoryRoot
    operator_rollout_contract_path = $OperatorRolloutContractPath
    max_evidence_age_hours = $MaxEvidenceAgeHours
    minimum_soak_trend_records = $MinimumSoakTrendRecords
    minimum_playback_trend_records = $MinimumPlaybackTrendRecords
    minimum_operator_trend_records = $MinimumOperatorTrendRecords
    allow_bootstrap = [bool]$AllowBootstrap
    checks = $checks
    failed_checks = $failedChecks
    status = if ($failedChecks.Count -eq 0) { 'passed' } else { 'failed' }
}

$absoluteOutputPath = if ([System.IO.Path]::IsPathRooted($OutputPath)) {
    $OutputPath
}
else {
    Join-Path $RepoRoot $OutputPath
}
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $absoluteOutputPath) | Out-Null
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $absoluteOutputPath -Encoding UTF8
Write-Host ("[enterprise-rollout-continuity] Summary: {0}" -f $absoluteOutputPath)

if ($summary.status -eq 'failed') {
    throw ("[enterprise-rollout-continuity] continuity gate failed; see {0}" -f $absoluteOutputPath)
}

Write-Host '[enterprise-rollout-continuity] PASS' -ForegroundColor Green
