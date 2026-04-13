param(
    [string] $EnvironmentName = '',
    [string] $HealthEndpoint = '',
    [string] $SearchEndpoint = '',
    [string] $AlertEndpoint = '',
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

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null
if ([string]::IsNullOrWhiteSpace($HistoryRoot)) {
    $HistoryRoot = Join-Path $ArtifactDir 'history'
}
New-Item -ItemType Directory -Force -Path $HistoryRoot | Out-Null

$requiredFields = @(
    '@timestamp',
    'log.level',
    'message',
    'service.name',
    'trace.id',
    'span.id',
    'labels.tenant_id'
)

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

$checks = [System.Collections.Generic.List[object]]::new()
function Add-Check {
    param([string] $Name, [bool] $Passed, [object] $Observed, [object] $Expected)
    $checks.Add([pscustomobject]@{
            name     = $Name
            passed   = $Passed
            observed = $Observed
            expected = $Expected
        })
}
$reachableExpected = if ($AllowOffline) { 'reachable_or_allowed_offline' } else { 'reachable' }
$recordsExpected = if ($AllowOffline) { '>=0' } else { '>=1' }

Add-Check -Name 'environment_name_present' `
    -Passed:(-not [string]::IsNullOrWhiteSpace($EnvironmentName)) `
    -Observed:$EnvironmentName -Expected:'non-empty'
Add-Check -Name 'health_endpoint_present' `
    -Passed:(-not [string]::IsNullOrWhiteSpace($HealthEndpoint)) `
    -Observed:$HealthEndpoint -Expected:'non-empty'
Add-Check -Name 'search_endpoint_present' `
    -Passed:(-not [string]::IsNullOrWhiteSpace($SearchEndpoint)) `
    -Observed:$SearchEndpoint -Expected:'non-empty'
Add-Check -Name 'alert_endpoint_present' `
    -Passed:(-not [string]::IsNullOrWhiteSpace($AlertEndpoint)) `
    -Observed:$AlertEndpoint -Expected:'non-empty'

$healthStatus = 'unreachable'
$healthLatencyMs = $null
if (-not [string]::IsNullOrWhiteSpace($HealthEndpoint)) {
    try {
        $healthWatch = [System.Diagnostics.Stopwatch]::StartNew()
        $healthResponse = Invoke-RestMethod -Method Get -Uri $HealthEndpoint -TimeoutSec 10
        $healthWatch.Stop()
        $healthLatencyMs = [int] [Math]::Round($healthWatch.Elapsed.TotalMilliseconds)
        $healthStatus = [string] ($healthResponse.status ?? 'ok')
        Add-Check -Name 'health_endpoint_reachable' -Passed:$true -Observed:$healthStatus -Expected:'reachable'
        Add-Check -Name 'health_endpoint_latency_ms' `
            -Passed:($healthLatencyMs -le $MaxHealthLatencyMs) `
            -Observed:$healthLatencyMs `
            -Expected:("<={0}" -f $MaxHealthLatencyMs)
    }
    catch {
        Add-Check -Name 'health_endpoint_reachable' -Passed:$AllowOffline `
            -Observed:($_.Exception.Message) `
            -Expected:$reachableExpected
    }
}

$searchRecords = @()
$searchLatencyMs = $null
if (-not [string]::IsNullOrWhiteSpace($SearchEndpoint)) {
    try {
        $searchWatch = [System.Diagnostics.Stopwatch]::StartNew()
        $searchResponse = Invoke-RestMethod -Method Get -Uri $SearchEndpoint -TimeoutSec 20
        $searchWatch.Stop()
        $searchLatencyMs = [int] [Math]::Round($searchWatch.Elapsed.TotalMilliseconds)
        if ($searchResponse -is [System.Array]) {
            $searchRecords = @($searchResponse)
        }
        elseif ($searchResponse.PSObject.Properties.Name -contains 'records') {
            $searchRecords = @($searchResponse.records)
        }
        else {
            $searchRecords = @($searchResponse)
        }
        Add-Check -Name 'search_endpoint_reachable' -Passed:$true -Observed:$searchRecords.Count -Expected:'>=1'
        Add-Check -Name 'search_endpoint_latency_ms' `
            -Passed:($searchLatencyMs -le $MaxSearchLatencyMs) `
            -Observed:$searchLatencyMs `
            -Expected:("<={0}" -f $MaxSearchLatencyMs)
    }
    catch {
        Add-Check -Name 'search_endpoint_reachable' -Passed:$AllowOffline `
            -Observed:($_.Exception.Message) `
            -Expected:$reachableExpected
    }
}

if ($searchRecords.Count -gt 0) {
    foreach ($field in $requiredFields) {
        $present = @($searchRecords | Where-Object { Test-JsonFieldPresent -Json $_ -Field $field }).Count -gt 0
        Add-Check -Name ("correlation_field::{0}" -f $field) -Passed:$present -Observed:$present -Expected:$true
    }
}
else {
    Add-Check -Name 'search_records_available' -Passed:$AllowOffline -Observed:0 `
        -Expected:$recordsExpected
}

$alertStatus = 'unreachable'
$alertLatencyMs = $null
$activeAlertCount = $null
if (-not [string]::IsNullOrWhiteSpace($AlertEndpoint)) {
    try {
        $alertWatch = [System.Diagnostics.Stopwatch]::StartNew()
        $alertResponse = Invoke-RestMethod -Method Get -Uri $AlertEndpoint -TimeoutSec 20
        $alertWatch.Stop()
        $alertLatencyMs = [int] [Math]::Round($alertWatch.Elapsed.TotalMilliseconds)
        if ($alertResponse -is [System.Array]) {
            $alertCount = @($alertResponse).Count
            $alertStatus = ("records:{0}" -f $alertCount)
        }
        elseif ($alertResponse.PSObject.Properties.Name -contains 'active_alerts') {
            $alertCount = @($alertResponse.active_alerts).Count
            $alertStatus = ("active_alerts:{0}" -f $alertCount)
        }
        else {
            $alertCount = 1
            $alertStatus = 'reachable'
        }
        $activeAlertCount = $alertCount
        Add-Check -Name 'alert_endpoint_reachable' -Passed:$true -Observed:$alertStatus -Expected:'reachable'
        Add-Check -Name 'alert_endpoint_latency_ms' `
            -Passed:($alertLatencyMs -le $MaxAlertLatencyMs) `
            -Observed:$alertLatencyMs `
            -Expected:("<={0}" -f $MaxAlertLatencyMs)
        Add-Check -Name 'alert_contract_present' `
            -Passed:($alertResponse.PSObject.Properties.Name -contains 'active_alerts') `
            -Observed:($alertResponse.PSObject.Properties.Name -contains 'active_alerts') `
            -Expected:$true
        if ($MaxActiveAlerts -ge 0) {
            Add-Check -Name 'alert_active_budget' `
                -Passed:($alertCount -le $MaxActiveAlerts) `
                -Observed:$alertCount `
                -Expected:("<={0}" -f $MaxActiveAlerts)
        }
    }
    catch {
        Add-Check -Name 'alert_endpoint_reachable' -Passed:$AllowOffline `
            -Observed:($_.Exception.Message) `
            -Expected:$reachableExpected
    }
}

$failedChecks = @($checks | Where-Object { -not $_.passed })
$historyRecords = @(
    Get-ChildItem -LiteralPath $HistoryRoot -Filter 'log-pipeline-rollout-record-*.json' -File -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTimeUtc -Descending
)
$greenStreak = 0
foreach ($recordFile in $historyRecords) {
    $record = Get-Content -LiteralPath $recordFile.FullName -Raw | ConvertFrom-Json
    if ([string]($record.status ?? '') -ne 'passed') {
        break
    }
    $greenStreak += 1
}
$currentStatus = if ($failedChecks.Count -eq 0) { 'passed' } else { 'failed' }
if ($currentStatus -eq 'passed') {
    $greenStreak += 1
}
$summary = [ordered]@{
    generated_at       = (Get-Date).ToUniversalTime().ToString('o')
    environment        = $EnvironmentName
    health_endpoint    = $HealthEndpoint
    search_endpoint    = $SearchEndpoint
    alert_endpoint     = $AlertEndpoint
    latency_ms         = [ordered]@{
        health = $healthLatencyMs
        search = $searchLatencyMs
        alert  = $alertLatencyMs
    }
    active_alert_count = $activeAlertCount
    max_active_alerts  = if ($MaxActiveAlerts -ge 0) { $MaxActiveAlerts } else { $null }
    history_record_count = $historyRecords.Count
    green_streak      = $greenStreak
    allow_offline      = [bool] $AllowOffline
    required_fields    = $requiredFields
    checks             = $checks
    failed_checks      = $failedChecks
    status             = $currentStatus
}

$summaryPath = Join-Path $ArtifactDir 'log-pipeline-rollout-summary.json'
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8
Write-Host ("Operator log pipeline rollout summary: {0}" -f $summaryPath)

$recordName = "log-pipeline-rollout-record-{0}.json" -f (Get-Date -Format 'yyyyMMdd-HHmmss')
$recordPath = Join-Path $HistoryRoot $recordName
[ordered]@{
    generated_at        = $summary.generated_at
    environment         = $EnvironmentName
    status              = $summary.status
    failed_check_count  = @($failedChecks).Count
    latency_ms          = $summary.latency_ms
    active_alert_count  = $summary.active_alert_count
    max_active_alerts   = $summary.max_active_alerts
    summary_path        = $summaryPath
} | ConvertTo-Json -Depth 6 | Set-Content -Path $recordPath -Encoding UTF8

if ($HistoryKeepLatest -gt 0) {
    if ($historyRecords.Count -gt $HistoryKeepLatest) {
        $historyRecords | Select-Object -Skip $HistoryKeepLatest | Remove-Item -Force
    }
}

if ($summary.status -eq 'failed') {
    exit 1
}
