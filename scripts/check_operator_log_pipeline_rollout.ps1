param(
    [string] $EnvironmentName = '',
    [string] $HealthEndpoint = '',
    [string] $SearchEndpoint = '',
    [string] $AlertEndpoint = '',
    [string] $ContractPath = '',
    [string] $ArtifactDir = 'artifacts/operations/log-pipeline',
    [int] $MaxHealthLatencyMs = 5000,
    [int] $MaxSearchLatencyMs = 5000,
    [int] $MaxAlertLatencyMs = 5000,
    [int] $MaxActiveAlerts = -1,
    [string] $HistoryRoot = '',
    [int] $HistoryKeepLatest = 240,
    [switch] $AllowOffline
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($EnvironmentName)) {
    $EnvironmentName = [string] $env:FILMU_LOG_PIPELINE_ENVIRONMENT
}
if ([string]::IsNullOrWhiteSpace($HealthEndpoint)) {
    $HealthEndpoint = [string] $env:FILMU_LOG_PIPELINE_HEALTH_ENDPOINT
}
if ([string]::IsNullOrWhiteSpace($SearchEndpoint)) {
    $SearchEndpoint = [string] $env:FILMU_LOG_PIPELINE_SEARCH_ENDPOINT
}
if ([string]::IsNullOrWhiteSpace($AlertEndpoint)) {
    $AlertEndpoint = [string] $env:FILMU_LOG_PIPELINE_ALERT_ENDPOINT
}

if ([string]::IsNullOrWhiteSpace($ContractPath)) {
    $ContractPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'ops\rollout\operator-log-pipeline.contract.json'
}

$contract = $null
$schemaVersion = 1
$artifactKind = 'operator_log_pipeline_rollout'
$freshnessWindowHours = 24
$minimumGreenStreak = 3
$requiredLogFields = @(
    '@timestamp',
    'log.level',
    'message',
    'service.name',
    'trace.id',
    'span.id',
    'labels.tenant_id'
)
if (Test-Path -LiteralPath $ContractPath) {
    $ContractPath = [System.IO.Path]::GetFullPath($ContractPath)
    $contract = Get-Content -LiteralPath $ContractPath -Raw | ConvertFrom-Json
    if ($contract.PSObject.Properties.Name -contains 'schema_version') {
        $schemaVersion = [int] $contract.schema_version
    }
    if ($contract.PSObject.Properties.Name -contains 'artifact_kind') {
        $artifactKind = [string] $contract.artifact_kind
    }
    if ($contract.PSObject.Properties.Name -contains 'freshness_window_hours') {
        $freshnessWindowHours = [Math]::Max(1, [int] $contract.freshness_window_hours)
    }
    if ($contract.PSObject.Properties.Name -contains 'minimum_green_streak') {
        $minimumGreenStreak = [Math]::Max(1, [int] $contract.minimum_green_streak)
    }
    if ($contract.PSObject.Properties.Name -contains 'required_log_fields') {
        $requiredLogFields = @(
            $contract.required_log_fields |
                Where-Object { -not [string]::IsNullOrWhiteSpace([string] $_) } |
                ForEach-Object { [string] $_ }
        )
    } elseif ($contract.PSObject.Properties.Name -contains 'required_fields') {
        $contractFields = @(
            $contract.required_fields |
                Where-Object { -not [string]::IsNullOrWhiteSpace([string] $_) } |
                ForEach-Object { [string] $_ }
        )
        if ($contractFields -contains '@timestamp') {
            $requiredLogFields = $contractFields
        }
    }
    if ($contract.PSObject.Properties.Name -contains 'max_health_latency_ms') {
        $MaxHealthLatencyMs = [int] $contract.max_health_latency_ms
    }
    if ($contract.PSObject.Properties.Name -contains 'max_search_latency_ms') {
        $MaxSearchLatencyMs = [int] $contract.max_search_latency_ms
    }
    if ($contract.PSObject.Properties.Name -contains 'max_alert_latency_ms') {
        $MaxAlertLatencyMs = [int] $contract.max_alert_latency_ms
    }
    if ($MaxActiveAlerts -lt 0 -and ($contract.PSObject.Properties.Name -contains 'max_active_alerts')) {
        $MaxActiveAlerts = [int] $contract.max_active_alerts
    }
}

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null
if ([string]::IsNullOrWhiteSpace($HistoryRoot)) {
    $HistoryRoot = Join-Path $ArtifactDir 'history'
}
New-Item -ItemType Directory -Force -Path $HistoryRoot | Out-Null

$failureReasonSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$requiredActionSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
$checks = [System.Collections.Generic.List[object]]::new()

function Add-FailureReason {
    param([string] $Code)

    if (-not [string]::IsNullOrWhiteSpace($Code)) {
        $null = $script:failureReasonSet.Add($Code)
    }
}

function Add-RequiredAction {
    param([string] $Action)

    if (-not [string]::IsNullOrWhiteSpace($Action)) {
        $null = $script:requiredActionSet.Add($Action)
    }
}

function Add-Check {
    param(
        [string] $Name,
        [bool] $Passed,
        [object] $Observed,
        [object] $Expected,
        [string] $FailureReason = '',
        [string] $RequiredAction = ''
    )

    $checks.Add([pscustomobject]@{
            name     = $Name
            passed   = $Passed
            observed = $Observed
            expected = $Expected
        })
    if (-not $Passed) {
        Add-FailureReason -Code $FailureReason
        Add-RequiredAction -Action $RequiredAction
    }
}

function Test-JsonFieldPresent {
    param([object] $Json, [string] $Field)

    if ($Json.PSObject.Properties.Name -contains $Field) {
        return $true
    }
    $cursor = $Json
    foreach ($part in $Field.Split('.')) {
        if ($null -eq $cursor -or -not ($cursor.PSObject.Properties.Name -contains $part)) {
            return $false
        }
        $cursor = $cursor.$part
    }
    return $true
}

function Get-AlertEntries {
    param([object] $AlertResponse)

    if ($AlertResponse -is [System.Array]) {
        return @($AlertResponse)
    }
    if ($AlertResponse.PSObject.Properties.Name -contains 'active_alerts') {
        return @($AlertResponse.active_alerts)
    }
    return @()
}

function Get-AlertSeverityCounts {
    param([object[]] $AlertEntries)

    $counts = @{}
    foreach ($entry in $AlertEntries) {
        $severity = ''
        if ($entry.PSObject.Properties.Name -contains 'severity') {
            $severity = [string] $entry.severity
        } elseif (
            $entry.PSObject.Properties.Name -contains 'labels' -and
            $entry.labels -ne $null -and
            $entry.labels.PSObject.Properties.Name -contains 'severity'
        ) {
            $severity = [string] $entry.labels.severity
        }
        if ([string]::IsNullOrWhiteSpace($severity)) {
            $severity = 'unspecified'
        }
        if (-not $counts.ContainsKey($severity)) {
            $counts[$severity] = 0
        }
        $counts[$severity] += 1
    }
    return $counts
}

function Get-ConsecutivePassedHistoryStreak {
    param([object[]] $HistoryRecords)

    $streak = 0
    foreach ($recordFile in $HistoryRecords) {
        $record = Get-Content -LiteralPath $recordFile.FullName -Raw | ConvertFrom-Json
        if ([string]($record.status ?? '') -ne 'passed') {
            break
        }
        $streak += 1
    }
    return $streak
}

$reachableExpected = if ($AllowOffline) { 'reachable_or_allowed_offline' } else { 'reachable' }
$recordsExpected = if ($AllowOffline) { '>=0' } else { '>=1' }
$contractObservedPath = if ($null -ne $contract) { $ContractPath } else { $null }
$maxActiveAlertsObserved = if ($MaxActiveAlerts -ge 0) { $MaxActiveAlerts } else { $null }
$capturedAt = (Get-Date).ToUniversalTime().ToString('o')
$expiresAt = (Get-Date).ToUniversalTime().AddHours($freshnessWindowHours).ToString('o')

Add-Check -Name 'contract_present' `
    -Passed:($null -ne $contract) `
    -Observed:$contractObservedPath `
    -Expected:'existing contract path' `
    -FailureReason:'operator_log_pipeline_contract_missing' `
    -RequiredAction:'restore_log_pipeline_rollout_contract'
Add-Check -Name 'environment_name_present' `
    -Passed:(-not [string]::IsNullOrWhiteSpace($EnvironmentName)) `
    -Observed:$EnvironmentName `
    -Expected:'non-empty' `
    -FailureReason:'operator_log_pipeline_environment_missing' `
    -RequiredAction:'configure_log_pipeline_environment_name'
Add-Check -Name 'health_endpoint_present' `
    -Passed:(-not [string]::IsNullOrWhiteSpace($HealthEndpoint)) `
    -Observed:$HealthEndpoint `
    -Expected:'non-empty' `
    -FailureReason:'operator_log_pipeline_health_endpoint_missing' `
    -RequiredAction:'configure_log_pipeline_health_endpoint'
Add-Check -Name 'search_endpoint_present' `
    -Passed:(-not [string]::IsNullOrWhiteSpace($SearchEndpoint)) `
    -Observed:$SearchEndpoint `
    -Expected:'non-empty' `
    -FailureReason:'operator_log_pipeline_search_endpoint_missing' `
    -RequiredAction:'configure_log_pipeline_search_endpoint'
Add-Check -Name 'alert_endpoint_present' `
    -Passed:(-not [string]::IsNullOrWhiteSpace($AlertEndpoint)) `
    -Observed:$AlertEndpoint `
    -Expected:'non-empty' `
    -FailureReason:'operator_log_pipeline_alert_endpoint_missing' `
    -RequiredAction:'configure_log_pipeline_alert_endpoint'

$healthStatus = 'unreachable'
$healthLatencyMs = $null
if (-not [string]::IsNullOrWhiteSpace($HealthEndpoint)) {
    try {
        $healthWatch = [System.Diagnostics.Stopwatch]::StartNew()
        $healthResponse = Invoke-RestMethod -Method Get -Uri $HealthEndpoint -TimeoutSec 10
        $healthWatch.Stop()
        $healthLatencyMs = [int] [Math]::Round($healthWatch.Elapsed.TotalMilliseconds)
        $healthStatus = [string] ($healthResponse.status ?? 'ok')
        Add-Check -Name 'health_endpoint_reachable' `
            -Passed:$true `
            -Observed:$healthStatus `
            -Expected:'reachable'
        Add-Check -Name 'health_endpoint_latency_ms' `
            -Passed:($healthLatencyMs -le $MaxHealthLatencyMs) `
            -Observed:$healthLatencyMs `
            -Expected:("<={0}" -f $MaxHealthLatencyMs) `
            -FailureReason:'operator_log_pipeline_health_latency_budget_exceeded' `
            -RequiredAction:'tune_log_pipeline_health_latency'
    } catch {
        Add-Check -Name 'health_endpoint_reachable' `
            -Passed:$AllowOffline `
            -Observed:($_.Exception.Message) `
            -Expected:$reachableExpected `
            -FailureReason:'operator_log_pipeline_health_unreachable' `
            -RequiredAction:'repair_log_pipeline_health_endpoint'
    }
}

$searchRecords = @()
$searchLatencyMs = $null
$searchRecordCount = 0
if (-not [string]::IsNullOrWhiteSpace($SearchEndpoint)) {
    try {
        $searchWatch = [System.Diagnostics.Stopwatch]::StartNew()
        $searchResponse = Invoke-RestMethod -Method Get -Uri $SearchEndpoint -TimeoutSec 20
        $searchWatch.Stop()
        $searchLatencyMs = [int] [Math]::Round($searchWatch.Elapsed.TotalMilliseconds)
        if ($searchResponse -is [System.Array]) {
            $searchRecords = @($searchResponse)
        } elseif ($searchResponse.PSObject.Properties.Name -contains 'records') {
            $searchRecords = @($searchResponse.records)
        } else {
            $searchRecords = @($searchResponse)
        }
        $searchRecordCount = @($searchRecords).Count
        Add-Check -Name 'search_endpoint_reachable' `
            -Passed:$true `
            -Observed:$searchRecordCount `
            -Expected:'>=1'
        Add-Check -Name 'search_endpoint_latency_ms' `
            -Passed:($searchLatencyMs -le $MaxSearchLatencyMs) `
            -Observed:$searchLatencyMs `
            -Expected:("<={0}" -f $MaxSearchLatencyMs) `
            -FailureReason:'operator_log_pipeline_search_latency_budget_exceeded' `
            -RequiredAction:'tune_log_pipeline_search_latency'
    } catch {
        Add-Check -Name 'search_endpoint_reachable' `
            -Passed:$AllowOffline `
            -Observed:($_.Exception.Message) `
            -Expected:$reachableExpected `
            -FailureReason:'operator_log_pipeline_search_unreachable' `
            -RequiredAction:'repair_log_pipeline_search_endpoint'
    }
}

if ($searchRecords.Count -gt 0) {
    foreach ($field in $requiredLogFields) {
        $present = @($searchRecords | Where-Object { Test-JsonFieldPresent -Json $_ -Field $field }).Count -gt 0
        Add-Check -Name ("correlation_field::{0}" -f $field) `
            -Passed:$present `
            -Observed:$present `
            -Expected:$true `
            -FailureReason:'operator_log_pipeline_search_contract_missing_field' `
            -RequiredAction:'restore_log_pipeline_correlation_fields'
    }
} else {
    Add-Check -Name 'search_records_available' `
        -Passed:$AllowOffline `
        -Observed:0 `
        -Expected:$recordsExpected `
        -FailureReason:'operator_log_pipeline_search_records_missing' `
        -RequiredAction:'record_log_pipeline_search_evidence'
}

$alertStatus = 'unreachable'
$alertLatencyMs = $null
$activeAlertCount = $null
$alertEntries = @()
$alertSeverityCounts = @{}
$alertContractReady = $false
$alertBudgetStatus = if ($MaxActiveAlerts -ge 0) { 'within_budget' } else { 'not_configured' }
if (-not [string]::IsNullOrWhiteSpace($AlertEndpoint)) {
    try {
        $alertWatch = [System.Diagnostics.Stopwatch]::StartNew()
        $alertResponse = Invoke-RestMethod -Method Get -Uri $AlertEndpoint -TimeoutSec 20
        $alertWatch.Stop()
        $alertLatencyMs = [int] [Math]::Round($alertWatch.Elapsed.TotalMilliseconds)
        $alertEntries = @(Get-AlertEntries -AlertResponse $alertResponse)
        $alertSeverityCounts = Get-AlertSeverityCounts -AlertEntries $alertEntries
        if ($alertResponse -is [System.Array]) {
            $activeAlertCount = @($alertResponse).Count
            $alertStatus = ("records:{0}" -f $activeAlertCount)
        } elseif ($alertResponse.PSObject.Properties.Name -contains 'active_alerts') {
            $activeAlertCount = @($alertResponse.active_alerts).Count
            $alertStatus = ("active_alerts:{0}" -f $activeAlertCount)
            $alertContractReady = $true
        } else {
            $activeAlertCount = @($alertEntries).Count
            $alertStatus = 'reachable'
        }
        Add-Check -Name 'alert_endpoint_reachable' `
            -Passed:$true `
            -Observed:$alertStatus `
            -Expected:'reachable'
        Add-Check -Name 'alert_endpoint_latency_ms' `
            -Passed:($alertLatencyMs -le $MaxAlertLatencyMs) `
            -Observed:$alertLatencyMs `
            -Expected:("<={0}" -f $MaxAlertLatencyMs) `
            -FailureReason:'operator_log_pipeline_alert_latency_budget_exceeded' `
            -RequiredAction:'tune_log_pipeline_alert_latency'
        Add-Check -Name 'alert_contract_present' `
            -Passed:$alertContractReady `
            -Observed:$alertContractReady `
            -Expected:$true `
            -FailureReason:'operator_log_pipeline_alert_contract_missing' `
            -RequiredAction:'restore_log_pipeline_alert_contract'
        if ($MaxActiveAlerts -ge 0) {
            $alertBudgetStatus = if ($activeAlertCount -le $MaxActiveAlerts) { 'within_budget' } else { 'exceeded' }
            Add-Check -Name 'alert_active_budget' `
                -Passed:($activeAlertCount -le $MaxActiveAlerts) `
                -Observed:$activeAlertCount `
                -Expected:("<={0}" -f $MaxActiveAlerts) `
                -FailureReason:'operator_log_pipeline_alert_budget_exceeded' `
                -RequiredAction:'tune_log_pipeline_alert_rules'
        }
    } catch {
        Add-Check -Name 'alert_endpoint_reachable' `
            -Passed:$AllowOffline `
            -Observed:($_.Exception.Message) `
            -Expected:$reachableExpected `
            -FailureReason:'operator_log_pipeline_alert_unreachable' `
            -RequiredAction:'repair_log_pipeline_alert_endpoint'
    }
}

$historyRecords = @(
    Get-ChildItem -LiteralPath $HistoryRoot -Filter 'log-pipeline-rollout-record-*.json' -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending
)
$priorGreenStreak = Get-ConsecutivePassedHistoryStreak -HistoryRecords $historyRecords
$failedChecks = @($checks | Where-Object { -not $_.passed })
$currentStatus = if ($failedChecks.Count -eq 0) { 'passed' } else { 'failed' }
$greenStreak = if ($currentStatus -eq 'passed') { $priorGreenStreak + 1 } else { 0 }
$historyRecordCount = $historyRecords.Count + 1
Add-Check -Name 'rollout_green_streak' `
    -Passed:($greenStreak -ge $minimumGreenStreak) `
    -Observed:$greenStreak `
    -Expected:(">={0}" -f $minimumGreenStreak) `
    -FailureReason:'operator_log_pipeline_green_streak_below_target' `
    -RequiredAction:'continue_log_pipeline_rollout_runs_until_stable'

$summary = [ordered]@{
    schema_version = $schemaVersion
    artifact_kind = $artifactKind
    captured_at = $capturedAt
    expires_at = $expiresAt
    generated_at = $capturedAt
    environment = $EnvironmentName
    contract_path = $contractObservedPath
    health_endpoint = $HealthEndpoint
    search_endpoint = $SearchEndpoint
    alert_endpoint = $AlertEndpoint
    required_log_fields = $requiredLogFields
    latency_ms = [ordered]@{
        health = $healthLatencyMs
        search = $searchLatencyMs
        alert = $alertLatencyMs
    }
    search_record_count = $searchRecordCount
    active_alert_count = $activeAlertCount
    max_active_alerts = $maxActiveAlertsObserved
    active_alerts_by_severity = [ordered]@{}
    alert_contract_ready = $alertContractReady
    alert_budget_status = $alertBudgetStatus
    minimum_green_streak = $minimumGreenStreak
    history_record_count = $historyRecordCount
    green_streak = $greenStreak
    allow_offline = [bool] $AllowOffline
    checks = $checks
    failed_checks = $failedChecks
    failed_check_names = @($failedChecks | ForEach-Object { [string] $_.name })
    failure_reasons = @($failureReasonSet | Sort-Object)
    required_actions = @($requiredActionSet | Sort-Object)
    status = $currentStatus
}
foreach ($key in @($alertSeverityCounts.Keys | Sort-Object)) {
    $summary.active_alerts_by_severity[$key] = [int] $alertSeverityCounts[$key]
}

$summaryPath = Join-Path $ArtifactDir 'log-pipeline-rollout-summary.json'
$recordName = "log-pipeline-rollout-record-{0}.json" -f (Get-Date -Format 'yyyyMMdd-HHmmss')
$recordPath = Join-Path $HistoryRoot $recordName

[ordered]@{
    schema_version = $schemaVersion
    artifact_kind = $artifactKind
    captured_at = $capturedAt
    expires_at = $expiresAt
    environment = $EnvironmentName
    status = $currentStatus
    green_streak = $greenStreak
    minimum_green_streak = $minimumGreenStreak
    failed_check_count = @($failedChecks).Count
    failed_check_names = @($failedChecks | ForEach-Object { [string] $_.name })
    failure_reasons = @($failureReasonSet | Sort-Object)
    required_actions = @($requiredActionSet | Sort-Object)
    latency_ms = $summary.latency_ms
    search_record_count = $searchRecordCount
    active_alert_count = $activeAlertCount
    max_active_alerts = $maxActiveAlertsObserved
    active_alerts_by_severity = $summary.active_alerts_by_severity
    alert_budget_status = $alertBudgetStatus
    contract_path = $contractObservedPath
    summary_path = $summaryPath
} | ConvertTo-Json -Depth 8 | Set-Content -Path $recordPath -Encoding UTF8

$updatedHistoryRecords = @(
    Get-ChildItem -LiteralPath $HistoryRoot -Filter 'log-pipeline-rollout-record-*.json' -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending
)
if ($HistoryKeepLatest -gt 0 -and $updatedHistoryRecords.Count -gt $HistoryKeepLatest) {
    $updatedHistoryRecords | Select-Object -Skip $HistoryKeepLatest | Remove-Item -Force
}

$summary | ConvertTo-Json -Depth 10 | Set-Content -Path $summaryPath -Encoding UTF8
Write-Host ("Operator log pipeline rollout summary: {0}" -f $summaryPath)

if ($summary.status -eq 'failed') {
    exit 1
}
