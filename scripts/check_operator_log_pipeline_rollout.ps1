param(
    [string] $EnvironmentName = '',
    [string] $HealthEndpoint = '',
    [string] $SearchEndpoint = '',
    [string] $AlertEndpoint = '',
    [string] $ArtifactDir = 'artifacts/operations/log-pipeline',
    [switch] $AllowOffline
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($EnvironmentName)) {
    $EnvironmentName = [string]$env:FILMU_LOG_PIPELINE_ENVIRONMENT
}
if ([string]::IsNullOrWhiteSpace($HealthEndpoint)) {
    $HealthEndpoint = [string]$env:FILMU_LOG_PIPELINE_HEALTH_ENDPOINT
}
if ([string]::IsNullOrWhiteSpace($SearchEndpoint)) {
    $SearchEndpoint = [string]$env:FILMU_LOG_PIPELINE_SEARCH_ENDPOINT
}
if ([string]::IsNullOrWhiteSpace($AlertEndpoint)) {
    $AlertEndpoint = [string]$env:FILMU_LOG_PIPELINE_ALERT_ENDPOINT
}

New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null

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
    param([object]$Json, [string]$Field)

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
    param([string]$Name, [bool]$Passed, [object]$Observed, [object]$Expected)
    $checks.Add([pscustomobject]@{
        name = $Name
        passed = $Passed
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
if (-not [string]::IsNullOrWhiteSpace($HealthEndpoint)) {
    try {
        $healthResponse = Invoke-RestMethod -Method Get -Uri $HealthEndpoint -TimeoutSec 10
        $healthStatus = [string]($healthResponse.status ?? 'ok')
        Add-Check -Name 'health_endpoint_reachable' -Passed:$true -Observed:$healthStatus -Expected:'reachable'
    }
    catch {
        Add-Check -Name 'health_endpoint_reachable' -Passed:$AllowOffline `
            -Observed:($_.Exception.Message) `
            -Expected:$reachableExpected
    }
}

$searchRecords = @()
if (-not [string]::IsNullOrWhiteSpace($SearchEndpoint)) {
    try {
        $searchResponse = Invoke-RestMethod -Method Get -Uri $SearchEndpoint -TimeoutSec 20
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
} else {
    Add-Check -Name 'search_records_available' -Passed:$AllowOffline -Observed:0 `
        -Expected:$recordsExpected
}

$alertStatus = 'unreachable'
if (-not [string]::IsNullOrWhiteSpace($AlertEndpoint)) {
    try {
        $alertResponse = Invoke-RestMethod -Method Get -Uri $AlertEndpoint -TimeoutSec 20
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
        Add-Check -Name 'alert_endpoint_reachable' -Passed:$true -Observed:$alertStatus -Expected:'reachable'
        Add-Check -Name 'alert_contract_present' `
            -Passed:($alertResponse.PSObject.Properties.Name -contains 'active_alerts') `
            -Observed:($alertResponse.PSObject.Properties.Name -contains 'active_alerts') `
            -Expected:$true
    }
    catch {
        Add-Check -Name 'alert_endpoint_reachable' -Passed:$AllowOffline `
            -Observed:($_.Exception.Message) `
            -Expected:$reachableExpected
    }
}

$failedChecks = @($checks | Where-Object { -not $_.passed })
$summary = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString('o')
    environment = $EnvironmentName
    health_endpoint = $HealthEndpoint
    search_endpoint = $SearchEndpoint
    alert_endpoint = $AlertEndpoint
    allow_offline = [bool]$AllowOffline
    required_fields = $requiredFields
    checks = $checks
    failed_checks = $failedChecks
    status = if ($failedChecks.Count -eq 0) { 'passed' } else { 'failed' }
}

$summaryPath = Join-Path $ArtifactDir 'log-pipeline-rollout-summary.json'
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8
Write-Host ("Operator log pipeline rollout summary: {0}" -f $summaryPath)

if ($summary.status -eq 'failed') {
    exit 1
}
