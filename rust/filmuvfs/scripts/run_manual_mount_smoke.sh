#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${1:-/mnt/e/Dev/Filmu/FilmuCore}"
GRPC_ENDPOINT="${GRPC_ENDPOINT:-http://127.0.0.1:50051}"
MOUNTPOINT="${MOUNTPOINT:-/tmp/filmuvfs_manual_smoke}"
LOG_PATH="${LOG_PATH:-/tmp/filmuvfs_manual_smoke.log}"

FILMUVFS_PID=""

cleanup() {
  if [[ -n "$FILMUVFS_PID" ]]; then
    kill "$FILMUVFS_PID" >/dev/null 2>&1 || true
    wait "$FILMUVFS_PID" >/dev/null 2>&1 || true
  fi

  if mountpoint -q "$MOUNTPOINT"; then
    fusermount3 -uz "$MOUNTPOINT" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

if [[ ! -e /dev/fuse ]]; then
  echo "[filmuvfs] /dev/fuse is missing; cannot run manual mount smoke" >&2
  exit 1
fi

if [[ -f "$HOME/.cargo/env" ]]; then
  # shellcheck disable=SC1090
  source "$HOME/.cargo/env"
fi

cd "$REPO_ROOT"
mkdir -p "$MOUNTPOINT"
rm -f "$LOG_PATH"

echo "[filmuvfs] starting sidecar against $GRPC_ENDPOINT"
./rust/filmuvfs/target/debug/filmuvfs \
  --mountpoint "$MOUNTPOINT" \
  --grpc-server "$GRPC_ENDPOINT" \
  >"$LOG_PATH" 2>&1 &
FILMUVFS_PID="$!"

for _ in $(seq 1 40); do
  if mountpoint -q "$MOUNTPOINT"; then
    break
  fi

  if ! kill -0 "$FILMUVFS_PID" >/dev/null 2>&1; then
    echo "[filmuvfs] sidecar exited before mount completed" >&2
    cat "$LOG_PATH" >&2 || true
    exit 1
  fi

  sleep 0.25
done

if ! mountpoint -q "$MOUNTPOINT"; then
  echo "[filmuvfs] mountpoint did not become active" >&2
  cat "$LOG_PATH" >&2 || true
  exit 1
fi

echo "[filmuvfs] mounted at $MOUNTPOINT"
echo "=== root ==="
ls -la "$MOUNTPOINT"
echo "=== movies ==="
ls -la "$MOUNTPOINT/movies" || true
echo "=== shows ==="
ls -la "$MOUNTPOINT/shows" || true

FIRST_MEDIA_FILE="$(find "$MOUNTPOINT" -type f | head -n 1 || true)"
if [[ -z "$FIRST_MEDIA_FILE" ]]; then
  echo "NO_MEDIA_FILES_FOUND"
  echo "=== sidecar log ==="
  cat "$LOG_PATH"
  exit 2
fi

echo "FIRST_MEDIA_FILE=$FIRST_MEDIA_FILE"
stat "$FIRST_MEDIA_FILE"
BYTES_READ="$(head -c 1000 "$FIRST_MEDIA_FILE" | wc -c)"
echo "BYTES_READ=$BYTES_READ"

echo "=== sidecar log ==="
cat "$LOG_PATH"
