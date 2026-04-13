param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Host "[chunk-parity] Running HTTP chunk contract tests..." -ForegroundColor Cyan
uv run pytest -q tests/test_chunk_parity_contract.py tests/test_stream_refresh_policy_contract.py

Write-Host "[chunk-parity] Running mounted runtime chunk contract tests..." -ForegroundColor Cyan
cargo test --manifest-path ./rust/filmuvfs/Cargo.toml chunk_parity_contract_

Write-Host "[chunk-parity] PASS" -ForegroundColor Green
