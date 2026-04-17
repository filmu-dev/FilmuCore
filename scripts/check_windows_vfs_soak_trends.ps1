param(
    [string] $ArtifactsRoot = '',
    [string[]] $SummaryPaths = @(),
    [string] $HistoryRoot = '',
    [int] $LookbackRuns = 10,
    [double] $RegressionFactor = 1.25,
    [double] $AbsoluteRegressionBuffer = 1.0,
    [string] $ContractPath = '',
    [switch] $AllowBootstrap
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ($LookbackRuns -lt 1) {
    throw 'LookbackRuns must be at least 1.'
}
if ($RegressionFactor -lt 1.0) {
    throw 'RegressionFactor must be >= 1.0.'
}
if ($AbsoluteRegressionBuffer -lt 0.0) {
    throw 'AbsoluteRegressionBuffer must be >= 0.'
}

function Get-DefaultArtifactsRoot {
    $repoRoot = Split-Path -Parent $PSScriptRoot
    return Join-Path $repoRoot 'playback-proof-artifacts\windows-native-stack'
}

function Get-UtcTimestamp {
    param([datetime] $Value)
    return $Value.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
}

function Ensure-FullPath {
    param([string] $Path)
    return [System.IO.Path]::GetFullPath($Path)
}

function Get-MetricSnapshot {
    param([object] $Summary)

    return [ordered]@{
        max_reconnect_incidents = [double]($Summary.max_reconnect_incidents ?? 0)
        max_provider_pressure_incidents = [double]($Summary.max_provider_pressure_incidents ?? 0)
        max_fatal_error_incidents = [double]($Summary.max_fatal_error_incidents ?? 0)
        critical_cache_pressure_runs = [double]($Summary.critical_cache_pressure_runs ?? 0)
        critical_chunk_coalescing_pressure_runs = [double]($Summary.critical_chunk_coalescing_pressure_runs ?? 0)
        critical_upstream_wait_runs = [double]($Summary.critical_upstream_wait_runs ?? 0)
        critical_refresh_pressure_runs = [double]($Summary.critical_refresh_pressure_runs ?? 0)
    }
}

function Get-MetricKeys {
    return @(
        'max_reconnect_incidents',
        'max_provider_pressure_incidents',
        'max_fatal_error_incidents',
        'critical_cache_pressure_runs',
        'critical_chunk_coalescing_pressure_runs',
        'critical_upstream_wait_runs',
        'critical_refresh_pressure_runs'
    )
}

if ([string]::IsNullOrWhiteSpace($ArtifactsRoot)) {
    $ArtifactsRoot = Get-DefaultArtifactsRoot
}
$ArtifactsRoot = Ensure-FullPath -Path $ArtifactsRoot

if ([string]::IsNullOrWhiteSpace($HistoryRoot)) {
    $HistoryRoot = Join-Path $ArtifactsRoot 'trend-history'
}
$HistoryRoot = Ensure-FullPath -Path $HistoryRoot
New-Item -ItemType Directory -Force -Path $HistoryRoot | Out-Null

$contract = $null
$contractSchemaVersion = 1
$FreshnessWindowHours = 72
if ([string]::IsNullOrWhiteSpace($ContractPath)) {
    $ContractPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'ops\rollout\windows-vfs-soak-program.contract.json'
}
if (Test-Path -LiteralPath $ContractPath) {
    $ContractPath = Ensure-FullPath -Path $ContractPath
    $contract = Get-Content -LiteralPath $ContractPath -Raw | ConvertFrom-Json
    if ($null -ne $contract) {
        if ($contract.PSObject.Properties.Name -contains 'schema_version') {
            $contractSchemaVersion = [int]$contract.schema_version
        }
        if ($contract.PSObject.Properties.Name -contains 'freshness_window_hours') {
            $FreshnessWindowHours = [int]$contract.freshness_window_hours
        }
    }
}

if ($SummaryPaths.Count -eq 0) {
    if (-not (Test-Path -LiteralPath $ArtifactsRoot)) {
        throw ("Artifacts root does not exist: {0}" -f $ArtifactsRoot)
    }
    $SummaryPaths = @(
        Get-ChildItem -LiteralPath $ArtifactsRoot -Filter 'soak-stability-*.json' -File |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -ExpandProperty FullName
    )
}

if ($SummaryPaths.Count -eq 0) {
    throw ("No soak stability summaries found under {0}" -f $ArtifactsRoot)
}

$currentByEnvironment = @{}
foreach ($summaryPath in $SummaryPaths) {
    if (-not (Test-Path -LiteralPath $summaryPath)) {
        throw ("Summary path does not exist: {0}" -f $summaryPath)
    }
    $summary = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
    $environmentClass = [string]($summary.environment_class ?? '')
    if ([string]::IsNullOrWhiteSpace($environmentClass)) {
        throw ("Summary path is missing environment_class: {0}" -f $summaryPath)
    }
    $currentByEnvironment[$environmentClass] = [ordered]@{
        source_summary = $summaryPath
        metrics = Get-MetricSnapshot -Summary $summary
    }
}

$historyRecords = @(
    Get-ChildItem -LiteralPath $HistoryRoot -Filter 'soak-trend-record-*.json' -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First $LookbackRuns
)

$historicalByEnvironment = @{}
foreach ($recordFile in $historyRecords) {
    $record = Get-Content -LiteralPath $recordFile.FullName -Raw | ConvertFrom-Json
    if ($null -eq $record -or -not $record.environments) {
        continue
    }
    foreach ($environment in @($record.environments)) {
        $name = [string]($environment.environment_class ?? '')
        if ([string]::IsNullOrWhiteSpace($name)) {
            continue
        }
        if (-not $historicalByEnvironment.ContainsKey($name)) {
            $historicalByEnvironment[$name] = [System.Collections.Generic.List[object]]::new()
        }
        $historicalByEnvironment[$name].Add($environment.metrics)
    }
}

$checks = [System.Collections.Generic.List[object]]::new()
$failedChecks = [System.Collections.Generic.List[object]]::new()

foreach ($environmentClass in $currentByEnvironment.Keys) {
    $currentMetrics = $currentByEnvironment[$environmentClass].metrics
    $historicalMetrics = @()
    if ($historicalByEnvironment.ContainsKey($environmentClass)) {
        $historicalMetrics = @($historicalByEnvironment[$environmentClass])
    }

    if ($historicalMetrics.Count -eq 0) {
        $passed = [bool]$AllowBootstrap
        $check = [pscustomobject]@{
            environment_class = $environmentClass
            metric = 'baseline_available'
            passed = $passed
            observed = 0
            expected = if ($AllowBootstrap) { 'bootstrap_allowed' } else { '>=1 historical run' }
        }
        $checks.Add($check)
        if (-not $check.passed) {
            $failedChecks.Add($check)
        }
        continue
    }

    foreach ($metric in (Get-MetricKeys)) {
        $baselineValues = @(
            $historicalMetrics |
                ForEach-Object { [double]($_.$metric ?? 0) }
        )
        $baselineAverage = if ($baselineValues.Count -gt 0) {
            ($baselineValues | Measure-Object -Average).Average
        } else {
            0.0
        }
        $currentValue = [double]($currentMetrics.$metric ?? 0)
        $allowedMax = ($baselineAverage * $RegressionFactor) + $AbsoluteRegressionBuffer
        $passed = $currentValue -le $allowedMax
        $check = [pscustomobject]@{
            environment_class = $environmentClass
            metric = $metric
            passed = $passed
            observed = $currentValue
            baseline_average = [double]::Parse($baselineAverage.ToString('0.###'))
            threshold_max = [double]::Parse($allowedMax.ToString('0.###'))
            regression_factor = $RegressionFactor
            absolute_buffer = $AbsoluteRegressionBuffer
        }
        $checks.Add($check)
        if (-not $check.passed) {
            $failedChecks.Add($check)
        }
    }
}

$recordTimestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$recordPath = Join-Path $HistoryRoot ("soak-trend-record-{0}.json" -f $recordTimestamp)
$summaryPath = Join-Path $ArtifactsRoot ("soak-trend-summary-{0}.json" -f $recordTimestamp)
$capturedAt = (Get-Date).ToUniversalTime()
$failureReasons = [System.Collections.Generic.List[string]]::new()
$requiredActions = [System.Collections.Generic.List[string]]::new()
$nonBaselineFailedChecks = @(
    $failedChecks | Where-Object { [string]($_.metric ?? '') -ne 'baseline_available' }
)
if (@($failedChecks | Where-Object { [string]($_.metric ?? '') -eq 'baseline_available' }).Count -gt 0) {
    $failureReasons.Add('windows_vfs_soak_trend_baseline_missing')
    $requiredActions.Add('refresh_windows_vfs_soak_trend_history')
}
if ($nonBaselineFailedChecks.Count -gt 0 -and $failureReasons -notcontains 'windows_vfs_soak_trend_regression_detected') {
    $failureReasons.Add('windows_vfs_soak_trend_regression_detected')
    $requiredActions.Add('investigate_windows_vfs_soak_regressions')
}
$record = [ordered]@{
    schema_version = $contractSchemaVersion
    artifact_kind = 'windows_vfs_soak_trend_record'
    timestamp = Get-UtcTimestamp -Value $capturedAt
    captured_at = Get-UtcTimestamp -Value $capturedAt
    environments = @(
        $currentByEnvironment.GetEnumerator() |
            Sort-Object Name |
            ForEach-Object {
                [ordered]@{
                    environment_class = $_.Name
                    source_summary = $_.Value.source_summary
                    metrics = $_.Value.metrics
                }
            }
    )
}
$summary = [ordered]@{
    schema_version = $contractSchemaVersion
    artifact_kind = 'windows_vfs_soak_trend_summary'
    timestamp = Get-UtcTimestamp -Value $capturedAt
    captured_at = Get-UtcTimestamp -Value $capturedAt
    expires_at = Get-UtcTimestamp -Value $capturedAt.AddHours([Math]::Max($FreshnessWindowHours, 1))
    freshness_window_hours = [Math]::Max($FreshnessWindowHours, 1)
    artifacts_root = $ArtifactsRoot
    history_root = $HistoryRoot
    contract_path = if ($null -ne $contract) { $ContractPath } else { $null }
    lookback_runs = $LookbackRuns
    regression_factor = $RegressionFactor
    absolute_regression_buffer = $AbsoluteRegressionBuffer
    allow_bootstrap = [bool]$AllowBootstrap
    checks = $checks
    failed_checks = $failedChecks
    failure_reasons = @($failureReasons)
    required_actions = @($requiredActions)
    status = if ($failedChecks.Count -eq 0) { 'passed' } else { 'failed' }
}

$record | ConvertTo-Json -Depth 8 | Set-Content -Path $recordPath -Encoding UTF8
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8

if ($failedChecks.Count -gt 0) {
    throw ("[windows-vfs-soak-trends] regression gate failed; summary written to {0}" -f $summaryPath)
}

Write-Host ("[windows-vfs-soak-trends] PASS. Summary: {0}" -f $summaryPath) -ForegroundColor Green
