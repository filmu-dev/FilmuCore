param(
    [string]$LogPath = "logs/ecs.json",
    [string]$ArtifactDir = "artifacts/operations/log-pipeline"
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path $ArtifactDir | Out-Null

$requiredFields = @("@timestamp", "log.level", "message", "service.name")
$summary = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    status = "running"
    log_path = $LogPath
    field_mapping_version = "filmu-ecs-v1"
    required_fields = $requiredFields
    checked_records = 0
    missing_fields = @()
    records_missing_required_fields = 0
    search_contract = [ordered]@{
        timestamp_field = "@timestamp"
        severity_field = "log.level"
        message_field = "message"
        service_field = "service.name"
        tenant_field = "labels.tenant_id"
        trace_fields = @("trace.id", "span.id")
    }
}

function Test-JsonFieldPresent {
    param(
        [object]$Json,
        [string]$Field
    )

    if ($Json.PSObject.Properties.Name -contains $Field) {
        return $true
    }

    $cursor = $Json
    foreach ($part in $Field.Split(".")) {
        if ($null -eq $cursor -or -not ($cursor.PSObject.Properties.Name -contains $part)) {
            return $false
        }
        $cursor = $cursor.$part
    }
    return $true
}

if (-not (Test-Path -LiteralPath $LogPath)) {
    $summary.status = "skipped"
    $summary.missing_fields = @("log file not found")
} else {
    $records = Get-Content -LiteralPath $LogPath -Tail 25 | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    $observedFields = @{}
    foreach ($record in $records) {
        try {
            $json = $record | ConvertFrom-Json -ErrorAction Stop
            $summary.checked_records++
            $recordMissing = $false
            foreach ($field in $requiredFields) {
                $fieldPresent = Test-JsonFieldPresent -Json $json -Field $field
                if ($fieldPresent) {
                    $observedFields[$field] = $true
                } else {
                    $recordMissing = $true
                }
            }
            if ($recordMissing) {
                $summary.records_missing_required_fields++
            }
        } catch {
            $summary.missing_fields += "invalid_json"
        }
    }
    foreach ($field in $requiredFields) {
        if (-not $observedFields.ContainsKey($field)) {
            $summary.missing_fields += $field
        }
    }
    $summary.missing_fields = @($summary.missing_fields | Sort-Object -Unique)
    $summary.status = if ($summary.missing_fields.Count -eq 0 -and $summary.checked_records -gt 0) {
        "passed"
    } else {
        "failed"
    }
}

$summaryPath = Join-Path $ArtifactDir "log-pipeline-contract.json"
$summary | ConvertTo-Json -Depth 8 | Set-Content -Path $summaryPath -Encoding UTF8
Write-Host "Log pipeline contract summary: $summaryPath"
if ($summary.status -eq "failed") {
    exit 1
}
