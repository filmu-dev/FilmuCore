param(
    [string] $ArtifactsRoot = '',
    [string[]] $SummaryPaths = @(),
    [string] $HistoryRoot = '',
    [int] $LookbackRuns = 10,
    [double] $RegressionFactor = 1.25,
    [double] $AbsoluteRegressionBuffer = 1.0,
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

function Ensure-FullPath {
    param([string] $Path)
    return [System.IO.Path]::GetFullPath($Path)
}

function Get-MetricSnapshot {
    param([object] $Summary)

    $runCount = @($Summary.runs).Count
    $failedRuns = @($Summary.runs | Where-Object { -not [bool]($_.passed) }).Count
    return [ordered]@{
        repeat_count = [double]($Summary.repeat_count ?? $runCount)
        failed_run_count = [double]$failedRuns
        max_run_duration_seconds = [double]($Summary.max_run_duration_seconds ?? 0)
        max_reconnect_incident_count = [double]($Summary.max_reconnect_incident_count ?? 0)
    }
}

function Get-MetricKeys {
    return @(
        'failed_run_count',
        'max_run_duration_seconds',
        'max_reconnect_incident_count'
    )
}

if ([string]::IsNullOrWhiteSpace($ArtifactsRoot)) {
    $repoRoot = Split-Path -Parent $PSScriptRoot
    $ArtifactsRoot = Join-Path $repoRoot 'playback-proof-artifacts'
}
$ArtifactsRoot = Ensure-FullPath -Path $ArtifactsRoot

if ([string]::IsNullOrWhiteSpace($HistoryRoot)) {
    $HistoryRoot = Join-Path $ArtifactsRoot 'stability-trend-history'
}
$HistoryRoot = Ensure-FullPath -Path $HistoryRoot
New-Item -ItemType Directory -Force -Path $HistoryRoot | Out-Null

if ($SummaryPaths.Count -eq 0) {
    if (-not (Test-Path -LiteralPath $ArtifactsRoot)) {
        throw ("Artifacts root does not exist: {0}" -f $ArtifactsRoot)
    }
    $SummaryPaths = @(
        Get-ChildItem -LiteralPath $ArtifactsRoot -Filter 'stability-summary-*.json' -File |
            Sort-Object LastWriteTimeUtc -Descending |
            Select-Object -ExpandProperty FullName
    )
}

if ($SummaryPaths.Count -eq 0) {
    throw ("No playback stability summaries found under {0}" -f $ArtifactsRoot)
}

$resolvedSummaryEntries = [System.Collections.Generic.List[object]]::new()
foreach ($summaryPath in @($SummaryPaths)) {
    $fullSummaryPath = Ensure-FullPath -Path ([string]$summaryPath)
    if (-not (Test-Path -LiteralPath $fullSummaryPath)) {
        throw ("Summary path does not exist: {0}" -f $fullSummaryPath)
    }
    $summaryItem = Get-Item -LiteralPath $fullSummaryPath
    $resolvedSummaryEntries.Add([pscustomobject]@{
        path = $fullSummaryPath
        last_write_time_utc = $summaryItem.LastWriteTimeUtc
    })
}

$orderedSummaryPaths = @(
    $resolvedSummaryEntries |
        Sort-Object last_write_time_utc -Descending |
        Select-Object -ExpandProperty path
)

$currentByEnvironment = @{}
foreach ($summaryPath in $orderedSummaryPaths) {
    $summary = Get-Content -LiteralPath $summaryPath -Raw | ConvertFrom-Json
    $environmentClass = [string]($summary.environment_class ?? '')
    if ([string]::IsNullOrWhiteSpace($environmentClass)) {
        throw ("Summary path is missing environment_class: {0}" -f $summaryPath)
    }
    if ($currentByEnvironment.ContainsKey($environmentClass)) {
        continue
    }
    $currentByEnvironment[$environmentClass] = [ordered]@{
        source_summary = $summaryPath
        all_green = [bool]($summary.all_green)
        metrics = Get-MetricSnapshot -Summary $summary
    }
}

$historyRecords = @(
    Get-ChildItem -LiteralPath $HistoryRoot -Filter 'playback-stability-trend-record-*.json' -File -ErrorAction SilentlyContinue |
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
    $entry = $currentByEnvironment[$environmentClass]
    $greenCheck = [pscustomobject]@{
        environment_class = $environmentClass
        metric = 'all_green'
        passed = [bool]$entry.all_green
        observed = [bool]$entry.all_green
        expected = $true
    }
    $checks.Add($greenCheck)
    if (-not $greenCheck.passed) {
        $failedChecks.Add($greenCheck)
    }

    $currentMetrics = $entry.metrics
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
        }
        else {
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
$recordPath = Join-Path $HistoryRoot ("playback-stability-trend-record-{0}.json" -f $recordTimestamp)
$summaryPath = Join-Path $ArtifactsRoot ("playback-stability-trend-summary-{0}.json" -f $recordTimestamp)
$record = [ordered]@{
    timestamp = (Get-Date).ToString('o')
    environments = @(
        $currentByEnvironment.GetEnumerator() |
            Sort-Object Name |
            ForEach-Object {
                [ordered]@{
                    environment_class = $_.Name
                    source_summary = $_.Value.source_summary
                    all_green = $_.Value.all_green
                    metrics = $_.Value.metrics
                }
            }
    )
}
$summary = [ordered]@{
    timestamp = (Get-Date).ToString('o')
    artifacts_root = $ArtifactsRoot
    history_root = $HistoryRoot
    lookback_runs = $LookbackRuns
    regression_factor = $RegressionFactor
    absolute_regression_buffer = $AbsoluteRegressionBuffer
    allow_bootstrap = [bool]$AllowBootstrap
    checks = $checks
    failed_checks = $failedChecks
    status = if ($failedChecks.Count -eq 0) { 'passed' } else { 'failed' }
}

$record | ConvertTo-Json -Depth 8 | Set-Content -Path $recordPath -Encoding UTF8
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8

if ($failedChecks.Count -gt 0) {
    throw ("[playback-stability-trends] regression gate failed; summary written to {0}" -f $summaryPath)
}

Write-Host ("[playback-stability-trends] PASS. Summary: {0}" -f $summaryPath) -ForegroundColor Green
