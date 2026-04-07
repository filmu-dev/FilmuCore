#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="${1:-/mnt/e/Dev/Filmu/FilmuCore}"
DEFAULT_GRPC_ENDPOINT="http://127.0.0.1:50051"
if [[ -f /.dockerenv ]]; then
  DEFAULT_GRPC_ENDPOINT="http://filmu-python:50051"
fi
GRPC_ENDPOINT="${FILMUVFS_GRPC_SERVER:-${GRPC_ENDPOINT:-$DEFAULT_GRPC_ENDPOINT}}"
MOUNTPOINT="${MOUNTPOINT:-/mnt/filmuvfs}"
LOG_PATH="${LOG_PATH:-/tmp/filmuvfs_persistent.log}"
PID_PATH="${PID_PATH:-/tmp/filmuvfs_persistent.pid}"
WAIT_SECONDS="${WAIT_SECONDS:-15}"
TARGET_DIR="${FILMUVFS_TARGET_DIR:-/tmp/filmuvfs-target}"
BINARY_PATH="$TARGET_DIR/release/filmuvfs"

require_fuse() {
  if [[ ! -e /dev/fuse ]]; then
    echo "[filmuvfs] /dev/fuse is missing; cannot start persistent mount" >&2
    exit 1
  fi
}

ensure_binary() {
  if [[ -x "$BINARY_PATH" ]] && ! find \
    "$REPO_ROOT/rust/filmuvfs/src" \
    "$REPO_ROOT/rust/filmuvfs/Cargo.toml" \
    "$REPO_ROOT/rust/filmuvfs/build.rs" \
    "$REPO_ROOT/proto/filmuvfs/catalog/v1/catalog.proto" \
    -newer "$BINARY_PATH" -print -quit 2>/dev/null | grep -q .
  then
    return
  fi

  if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1090
    source "$HOME/.cargo/env"
  fi

  echo "[filmuvfs] release binary missing; building it first"
  RUSTFLAGS="${RUSTFLAGS:-} -A dead_code" cargo build --release --target-dir "$TARGET_DIR" --manifest-path "$REPO_ROOT/rust/filmuvfs/Cargo.toml"
}

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

has_healthy_catalog_root() {
  local mountpoint="$1"
  local listing
  listing="$(timeout 2 ls "$mountpoint" 2>/dev/null || true)"
  printf '%s\n' "$listing" | grep -qE '^(movies|shows)$'
}

cleanup_stale_mount() {
  local mountpoint="$1"

  if mountpoint -q "$mountpoint" >/dev/null 2>&1; then
    local listing
    listing="$(timeout 2 ls "$mountpoint" 2>/dev/null || true)"
    if ! printf '%s\n' "$listing" | grep -qE '^(movies|shows)$'; then
      echo "[filmuvfs] stale mount detected at $mountpoint — force cleaning"
      fusermount3 -uz "$mountpoint" >/dev/null 2>&1 \
        || umount -l "$mountpoint" >/dev/null 2>&1 \
        || true
      sleep 1
    fi
  fi

  pkill -f "filmuvfs --mountpoint $mountpoint" >/dev/null 2>&1 || true
  sleep 1
}

wait_for_mount() {
  local deadline=$((SECONDS + WAIT_SECONDS))
  while (( SECONDS < deadline )); do
    if mountpoint -q "$MOUNTPOINT" || has_healthy_catalog_root "$MOUNTPOINT"; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

require_fuse
ensure_binary

mkdir -p "$MOUNTPOINT"
cleanup_stale_mount "$MOUNTPOINT"

if [[ -f "$PID_PATH" ]]; then
  EXISTING_PID="$(cat "$PID_PATH" 2>/dev/null || true)"
  if is_pid_running "$EXISTING_PID" && mountpoint -q "$MOUNTPOINT"; then
    listing="$(timeout 2 ls "$MOUNTPOINT" 2>/dev/null || true)"
    if printf '%s\n' "$listing" | grep -qE '^(movies|shows)$'; then
      echo "[filmuvfs] already mounted at $MOUNTPOINT (pid $EXISTING_PID)"
      exit 0
    fi
    echo "[filmuvfs] mountpoint is active but catalog tree is stale; cleaning before restart"
  fi
  rm -f "$PID_PATH"
fi

if mountpoint -q "$MOUNTPOINT"; then
  listing="$(timeout 2 ls "$MOUNTPOINT" 2>/dev/null || true)"
  if printf '%s\n' "$listing" | grep -qE '^(movies|shows)$'; then
    echo "[filmuvfs] $MOUNTPOINT is already mounted and healthy but no managed pid was found" >&2
  else
    echo "[filmuvfs] $MOUNTPOINT is mounted but stale; cleaning before start" >&2
  fi
  fusermount3 -uz "$MOUNTPOINT" >/dev/null 2>&1 \
    || umount -l "$MOUNTPOINT" >/dev/null 2>&1 \
    || true
  sleep 1
fi

rm -f "$LOG_PATH"

echo "[filmuvfs] starting persistent mount"
echo "[filmuvfs] repo: $REPO_ROOT"
echo "[filmuvfs] grpc: $GRPC_ENDPOINT"
echo "[filmuvfs] mountpoint: $MOUNTPOINT"
echo "[filmuvfs] log: $LOG_PATH"

nohup "$BINARY_PATH" \
  --mountpoint "$MOUNTPOINT" \
  --allow-other \
  --grpc-server "$GRPC_ENDPOINT" \
  >"$LOG_PATH" 2>&1 < /dev/null &

FILMUVFS_PID="$!"
echo "$FILMUVFS_PID" > "$PID_PATH"

if ! wait_for_mount; then
  echo "[filmuvfs] mountpoint did not become active within ${WAIT_SECONDS}s" >&2
  if is_pid_running "$FILMUVFS_PID"; then
    kill "$FILMUVFS_PID" >/dev/null 2>&1 || true
    wait "$FILMUVFS_PID" >/dev/null 2>&1 || true
  fi
  rm -f "$PID_PATH"
  cat "$LOG_PATH" >&2 || true
  exit 1
fi

echo "[filmuvfs] mounted successfully at $MOUNTPOINT"
echo "[filmuvfs] pid: $FILMUVFS_PID"
echo "[filmuvfs] open from WSL/Linux: $MOUNTPOINT"
