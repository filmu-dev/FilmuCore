#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${1:-/mnt/e/Dev/Filmu/FilmuCore}"
TEST_TIMEOUT_SECONDS="${TEST_TIMEOUT_SECONDS:-120}"

cleanup() {
  pkill -f "cargo test --manifest-path rust/filmuvfs/Cargo.toml --features integration-tests mount_lifecycle" >/dev/null 2>&1 || true
  pkill -f "/target/debug/deps/mount_lifecycle-" >/dev/null 2>&1 || true

  shopt -s nullglob
  for mountpoint in /tmp/filmuvfs_test_*; do
    if mountpoint -q "$mountpoint"; then
      fusermount3 -uz "$mountpoint" >/dev/null 2>&1 || true
    fi
  done
}

trap cleanup EXIT

echo "[filmuvfs] cleaning any stale mount_lifecycle processes and temporary mounts"
cleanup

if [[ ! -e /dev/fuse ]]; then
  echo "[filmuvfs] /dev/fuse is missing; Linux FUSE validation cannot run" >&2
  exit 1
fi

if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck disable=SC1090
  source "$HOME/.cargo/env"
fi

cd "$REPO_ROOT"

echo "[filmuvfs] running gated mount lifecycle test from $REPO_ROOT"
echo "[filmuvfs] timeout: ${TEST_TIMEOUT_SECONDS}s"

timeout --foreground --kill-after=10s "${TEST_TIMEOUT_SECONDS}s" \
  cargo test --manifest-path rust/filmuvfs/Cargo.toml \
  --features integration-tests \
  mount_lifecycle -- --nocapture
