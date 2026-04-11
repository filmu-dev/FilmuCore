#!/usr/bin/env bash

set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_ROOT/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"
MOUNT_START_SCRIPT="$REPO_ROOT/rust/filmuvfs/scripts/start_persistent_mount.sh"

wait_http_ready() {
  local uri="$1"
  local timeout_seconds="$2"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if curl --silent --show-error --fail --max-time 3 "$uri" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_grpc_ready() {
  local timeout_seconds="$1"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if timeout 2 bash -lc ':</dev/tcp/127.0.0.1/50051' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "==> Starting FilmuCore unix stack"
echo

echo "[1/3] Starting Docker Compose services..."
docker compose -f "$COMPOSE_FILE" up -d
echo "      [OK] Docker services started"

echo
echo "[2/3] Waiting for backend API and gRPC supplier..."
if ! wait_http_ready "http://127.0.0.1:8000/openapi.json" 45; then
  echo "backend API did not become ready at http://127.0.0.1:8000/openapi.json" >&2
  exit 1
fi
if ! wait_grpc_ready 45; then
  echo "gRPC catalog supplier did not become ready on localhost:50051" >&2
  exit 1
fi
echo "      [OK] Backend API and gRPC supplier are ready"

echo
echo "[3/3] Starting persistent FilmuVFS mount..."
bash "$MOUNT_START_SCRIPT" "$REPO_ROOT"
echo "      [OK] FilmuVFS mount started"

echo
echo "==> FilmuCore unix stack started successfully"
