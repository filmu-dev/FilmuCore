Set-StrictMode -Version Latest

function ConvertFrom-DotEnvValue {
    param([AllowEmptyString()][string] $Value)

    $trimmed = $Value.Trim()
    if ($trimmed.Length -ge 2) {
        $quote = $trimmed.Substring(0, 1)
        if (($quote -eq '"' -or $quote -eq "'") -and $trimmed.EndsWith($quote, [System.StringComparison]::Ordinal)) {
            return $trimmed.Substring(1, $trimmed.Length - 2)
        }
    }

    return $trimmed
}

function Get-DotEnvMap {
    param([Parameter(Mandatory = $true)][string] $Path)

    $map = @{}
    if (-not (Test-Path -LiteralPath $Path)) {
        return $map
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#')) {
            continue
        }
        $index = $trimmed.IndexOf('=')
        if ($index -lt 1) {
            continue
        }

        $name = $trimmed.Substring(0, $index).Trim()
        $rawValue = $trimmed.Substring($index + 1)
        $map[$name] = ConvertFrom-DotEnvValue -Value $rawValue
    }

    return $map
}

function Get-EnvValue {
    param(
        [Parameter(Mandatory = $true)][string] $Name,
        [Parameter(Mandatory = $true)][hashtable] $DotEnv
    )

    $processValue = [System.Environment]::GetEnvironmentVariable($Name)
    if (-not [string]::IsNullOrWhiteSpace($processValue)) {
        return [string] $processValue
    }
    if ($DotEnv.ContainsKey($Name) -and -not [string]::IsNullOrWhiteSpace([string] $DotEnv[$Name])) {
        return [string] $DotEnv[$Name]
    }

    return ''
}

function Test-GhAuthenticated {
    if ($null -eq (Get-Command gh -ErrorAction SilentlyContinue)) {
        return $false
    }

    & gh auth status 1>$null 2>$null
    return ($LASTEXITCODE -eq 0)
}

function Test-GithubMainPolicyValidationAvailable {
    param([Parameter(Mandatory = $true)][hashtable] $DotEnv)

    if (Test-GhAuthenticated) {
        return $true
    }

    foreach ($name in @('GH_TOKEN', 'GITHUB_TOKEN', 'FILMU_POLICY_ADMIN_TOKEN')) {
        if (-not [string]::IsNullOrWhiteSpace((Get-EnvValue -Name $name -DotEnv $DotEnv))) {
            return $true
        }
    }

    return $false
}
