#!/usr/bin/env bash

set -euo pipefail

MOUNTPOINT="${MOUNTPOINT:-/mnt/filmuvfs}"
PID_PATH="${PID_PATH:-/tmp/filmuvfs_persistent.pid}"
LOG_PATH="${LOG_PATH:-/tmp/filmuvfs_persistent.log}"

check_mount_health() {
  local mountpoint="$1"

  if ! mountpoint -q "$mountpoint" >/dev/null 2>&1; then
    echo "MOUNT_HEALTH=not-mounted"
    return 1
  fi

  local listing
  listing="$(timeout 2 ls "$mountpoint" 2>/dev/null || true)"
  if printf '%s\n' "$listing" | grep -qE '^(movies|shows)$'; then
    echo "MOUNT_HEALTH=healthy"
    return 0
  fi

  echo "MOUNT_HEALTH=stale"
  return 2
}

PID=""
if [[ -f "$PID_PATH" ]]; then
  PID="$(cat "$PID_PATH" 2>/dev/null || true)"
fi

echo "MOUNTPOINT=$MOUNTPOINT"
echo "MOUNT_ACTIVE=$([[ -d "$MOUNTPOINT" ]] && mountpoint -q "$MOUNTPOINT" && echo true || echo false)"
check_mount_health "$MOUNTPOINT" || true
echo "PID_FILE=$PID_PATH"
echo "PID=${PID:-none}"

if [[ -n "$PID" ]] && kill -0 "$PID" >/dev/null 2>&1; then
  echo "PID_RUNNING=true"
else
  echo "PID_RUNNING=false"
fi

echo "LOG_PATH=$LOG_PATH"

if [[ -d "$MOUNTPOINT" ]] && mountpoint -q "$MOUNTPOINT"; then
  echo "=== root ==="
  ls -la "$MOUNTPOINT"
  echo "=== movies ==="
  ls -la "$MOUNTPOINT/movies" || true
  echo "=== shows ==="
  ls -la "$MOUNTPOINT/shows" || true
fi
