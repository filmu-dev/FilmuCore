#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${CI_ARTIFACT_DIR:-ci-artifacts/python-tests}"
mkdir -p "$ARTIFACT_DIR"

export PYTHONHASHSEED="${PYTHONHASHSEED:-0}"
export PYTEST_ADDOPTS="${PYTEST_ADDOPTS:-}"
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-.venv-ci-tests}"

if command -v uv >/dev/null 2>&1; then
  UV_BIN="uv"
elif command -v uv.exe >/dev/null 2>&1; then
  UV_BIN="uv.exe"
else
  echo "::error title=uv missing::uv was not found on PATH for the Python test job."
  exit 127
fi

run_attempt() {
  local attempt="$1"
  local verbosity="$2"
  local junit_path="$ARTIFACT_DIR/pytest-attempt-${attempt}.xml"
  local log_path="$ARTIFACT_DIR/pytest-attempt-${attempt}.log"

  echo "== Python test attempt ${attempt} =="
  echo "Python: $($UV_BIN run python --version 2>&1)"
  echo "uv: $($UV_BIN --version 2>&1)"
  echo "Writing junit report to ${junit_path}"

  set +e
  $UV_BIN run pytest ${verbosity} --junitxml="$junit_path" 2>&1 | tee "$log_path"
  local status=${PIPESTATUS[0]}
  set -e

  return "$status"
}

if run_attempt "1" "-q"; then
  exit 0
fi

echo "::warning title=python tests retry::First pytest attempt failed on CI; retrying once with verbose output."

if run_attempt "2" "-vv --maxfail=1"; then
  echo "::warning title=python tests flaky recovery::Python tests passed on the second CI attempt. Review ci-artifacts/python-tests for the first failure log."
  exit 0
fi

echo "::error title=python tests failed::Python tests failed on both CI attempts. Review ci-artifacts/python-tests artifacts for diagnostics."
exit 1
