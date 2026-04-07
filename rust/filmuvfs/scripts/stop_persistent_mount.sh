#!/usr/bin/env bash

set -euo pipefail

MOUNTPOINT="${MOUNTPOINT:-/mnt/filmuvfs}"
PID_PATH="${PID_PATH:-/tmp/filmuvfs_persistent.pid}"

PID=""
if [[ -f "$PID_PATH" ]]; then
  PID="$(cat "$PID_PATH" 2>/dev/null || true)"
fi

if mountpoint -q "$MOUNTPOINT"; then
  echo "[filmuvfs] unmounting $MOUNTPOINT"
  fusermount3 -u "$MOUNTPOINT" >/dev/null 2>&1 \
    || fusermount3 -uz "$MOUNTPOINT" >/dev/null 2>&1 \
    || umount -l "$MOUNTPOINT" >/dev/null 2>&1 \
    || true
fi

if [[ -n "$PID" ]] && kill -0 "$PID" >/dev/null 2>&1; then
  echo "[filmuvfs] stopping pid $PID"
  kill "$PID" >/dev/null 2>&1 || true
  sleep 1
  kill -0 "$PID" >/dev/null 2>&1 && kill -9 "$PID" >/dev/null 2>&1 || true
fi

rm -f "$PID_PATH"

echo "[filmuvfs] stopped"
