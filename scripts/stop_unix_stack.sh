#!/usr/bin/env bash

set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_ROOT/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"
MOUNT_STOP_SCRIPT="$REPO_ROOT/rust/filmuvfs/scripts/stop_persistent_mount.sh"

echo "==> Stopping FilmuCore unix stack"
echo

echo "[1/2] Stopping persistent FilmuVFS mount..."
bash "$MOUNT_STOP_SCRIPT" || true
echo "      [OK] FilmuVFS mount stop attempted"

echo
echo "[2/2] Stopping Docker Compose services..."
docker compose -f "$COMPOSE_FILE" down
echo "      [OK] Docker services stopped"

echo
echo "==> FilmuCore unix stack stopped successfully"
