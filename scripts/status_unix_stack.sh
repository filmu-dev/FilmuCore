#!/usr/bin/env bash

set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_ROOT/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"
MOUNT_STATUS_SCRIPT="$REPO_ROOT/rust/filmuvfs/scripts/persistent_mount_status.sh"

check_http() {
  if curl --silent --show-error --fail --max-time 3 "http://127.0.0.1:8000/openapi.json" >/dev/null 2>&1; then
    echo "  [OK] HTTP ready"
  else
    echo "  [FAIL] HTTP not ready"
  fi
}

check_grpc() {
  if timeout 2 bash -lc ':</dev/tcp/127.0.0.1/50051' >/dev/null 2>&1; then
    echo "  [OK] localhost:50051 reachable"
  else
    echo "  [FAIL] localhost:50051 not reachable"
  fi
}

echo "==> FilmuCore unix stack status"
echo
echo "[Docker Compose]"
docker compose -f "$COMPOSE_FILE" ps

echo
echo "[Backend API]"
check_http

echo
echo "[gRPC Catalog]"
check_grpc

echo
echo "[Unix Mount]"
bash "$MOUNT_STATUS_SCRIPT"
