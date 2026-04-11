param(
    [string] $VfsMode = '',
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $PassThroughArgs
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot 'stack_mode_common.ps1')

$repoRoot = Split-Path -Parent $PSScriptRoot
$resolvedMode = Resolve-ConfiguredVfsMode -RepoRoot $repoRoot -RequestedMode $VfsMode

Write-Host ("==> FilmuCore stack status (vfs_mode={0})" -f $resolvedMode) -ForegroundColor Cyan

switch ($resolvedMode) {
    'windows' {
        if (-not (Test-IsWindowsHost)) {
            throw 'windows VFS mode requires a Windows host.'
        }
        & pwsh -NoProfile -File (Join-Path $PSScriptRoot 'status_windows_stack.ps1') @PassThroughArgs
    }
    'unix' {
        if (Test-IsWindowsHost) {
            & pwsh -NoProfile -File (Join-Path $PSScriptRoot 'status_local_stack.ps1') @PassThroughArgs
        }
        else {
            & bash (Join-Path $PSScriptRoot 'status_unix_stack.sh') @PassThroughArgs
        }
    }
}

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
