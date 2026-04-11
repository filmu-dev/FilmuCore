$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Test-IsWindowsHost {
    return [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
}

function Get-EnvSettingFromFile {
    param(
        [Parameter(Mandatory = $true)]
        [string] $RepoRoot,
        [Parameter(Mandatory = $true)]
        [string] $Name
    )

    $envPath = Join-Path $RepoRoot '.env'
    if (-not (Test-Path -LiteralPath $envPath)) {
        return $null
    }

    foreach ($line in Get-Content -LiteralPath $envPath) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#')) {
            continue
        }
        if ($trimmed -notmatch '^(?<key>[A-Za-z_][A-Za-z0-9_]*)=(?<value>.*)$') {
            continue
        }
        if ($Matches.key -ne $Name) {
            continue
        }

        $value = [string] $Matches.value
        if (
            ($value.Length -ge 2) -and
            (
                ($value.StartsWith('"') -and $value.EndsWith('"')) -or
                ($value.StartsWith("'") -and $value.EndsWith("'"))
            )
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        return $value.Trim()
    }

    return $null
}

function Resolve-ConfiguredVfsMode {
    param(
        [Parameter(Mandatory = $true)]
        [string] $RepoRoot,
        [AllowEmptyString()]
        [string] $RequestedMode = ''
    )

    $rawMode = $RequestedMode
    if ([string]::IsNullOrWhiteSpace($rawMode)) {
        $rawMode = [System.Environment]::GetEnvironmentVariable('FILMU_STACK_VFS_MODE')
    }
    if ([string]::IsNullOrWhiteSpace($rawMode)) {
        $rawMode = Get-EnvSettingFromFile -RepoRoot $RepoRoot -Name 'FILMU_STACK_VFS_MODE'
    }
    if ([string]::IsNullOrWhiteSpace($rawMode)) {
        $rawMode = 'auto'
    }

    $normalized = $rawMode.Trim().ToLowerInvariant()
    switch ($normalized) {
        'auto' {
            if (Test-IsWindowsHost) {
                return 'windows'
            }
            return 'unix'
        }
        'windows' { return 'windows' }
        'unix' { return 'unix' }
        default {
            throw ("FILMU_STACK_VFS_MODE must be one of: auto, windows, unix. Got '{0}'." -f $rawMode)
        }
    }
}
