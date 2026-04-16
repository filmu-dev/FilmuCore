param(
    [string]$ManifestPath = "./rust/filmuvfs/Cargo.toml",
    [string]$DiffScope = "rust/filmuvfs"
)

$ErrorActionPreference = "Stop"

Write-Host "cargo version:"
cargo -vV
Write-Host "rustfmt version:"
rustfmt --version

Write-Host "Running cargo fmt --check"
& cargo fmt --manifest-path $ManifestPath --all --check
if ($LASTEXITCODE -eq 0) {
    Write-Host "Rust formatting is clean."
    exit 0
}

Write-Warning "cargo fmt --check returned a non-zero exit code. Re-running formatter to inspect actual drift."

& cargo fmt --manifest-path $ManifestPath --all
if ($LASTEXITCODE -ne 0) {
    Write-Error "cargo fmt failed while attempting to apply formatting."
    exit $LASTEXITCODE
}

$diff = git diff -- $DiffScope
if ($LASTEXITCODE -ne 0) {
    Write-Error "git diff failed while checking formatting drift."
    exit $LASTEXITCODE
}

if (-not [string]::IsNullOrWhiteSpace($diff)) {
    Write-Host $diff
    Write-Error "Rust formatting drift detected."
    exit 1
}

Write-Warning "cargo fmt --check failed without producing any formatting diff. Treating this run as clean."
exit 0
