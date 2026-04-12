#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_root"

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "[playback-gate-ci] missing required command: $command_name" >&2
    exit 1
  fi
}

require_nonempty_env() {
  local env_name="$1"
  if [[ -z "${!env_name:-}" ]]; then
    echo "[playback-gate-ci] missing required environment variable: $env_name" >&2
    exit 1
  fi
}

resolve_browser_executable() {
  local candidates=(
    "${FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE:-}"
    "$(command -v google-chrome 2>/dev/null || true)"
    "$(command -v google-chrome-stable 2>/dev/null || true)"
    "$(command -v chromium 2>/dev/null || true)"
    "$(command -v chromium-browser 2>/dev/null || true)"
    "$(command -v microsoft-edge 2>/dev/null || true)"
    "$(command -v msedge 2>/dev/null || true)"
  )

  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  return 1
}

require_command docker
require_command curl
require_command pwsh
docker compose version >/dev/null

export FILMU_PY_API_KEY="${FILMU_PY_API_KEY:-32_character_filmu_api_key_local_}"
export FILMU_FRONTEND_USERNAME="${FILMU_FRONTEND_USERNAME:-dashadmin}"
export FILMU_FRONTEND_PASSWORD="${FILMU_FRONTEND_PASSWORD:-1234}"
export FILMU_FRONTEND_CONTEXT="${FILMU_FRONTEND_CONTEXT:-../../Triven_riven-fork/Triven_frontend}"
export FILMU_BACKEND_CONTAINER_NAME="${FILMU_BACKEND_CONTAINER_NAME:-filmu-python}"
export PLEX_URL="${PLEX_URL:-http://localhost:32401}"
export EMBY_URL="${EMBY_URL:-http://localhost:8097}"
export FILMU_REQUIRE_PROVIDER_GATE="${FILMU_REQUIRE_PROVIDER_GATE:-1}"
export FILMU_PLAYBACK_REQUIRED_CHECK_NAME="${FILMU_PLAYBACK_REQUIRED_CHECK_NAME:-Playback Gate / Playback Gate}"
export FILMU_PLAYBACK_DRY_RUN="${FILMU_PLAYBACK_DRY_RUN:-0}"
export FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE="${FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE:-}"

if [[ -z "$FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE" ]]; then
  FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE="$(resolve_browser_executable || true)"
  export FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE
fi

if [[ "$FILMU_PLAYBACK_DRY_RUN" != "1" ]]; then
  require_nonempty_env FILMU_FRONTEND_CONTEXT
  require_nonempty_env FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE
  require_nonempty_env TMDB_API_KEY
  if [[ ! -d "$FILMU_FRONTEND_CONTEXT" ]]; then
    echo "[playback-gate-ci] frontend source path not found: $FILMU_FRONTEND_CONTEXT" >&2
    exit 1
  fi

  if [[ ! -x "$FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE" ]]; then
    echo "[playback-gate-ci] browser executable not found or not executable: $FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE" >&2
    exit 1
  fi

  if [[ -z "${FILMU_PY_REALDEBRID_API_TOKEN:-}${REAL_DEBRID_API_KEY:-}${FILMU_PY_ALLDEBRID_API_TOKEN:-}${ALL_DEBRID_API_KEY:-}${FILMU_PY_DEBRIDLINK_API_TOKEN:-}${DEBRID_LINK_API_KEY:-}" ]]; then
    echo "[playback-gate-ci] at least one debrid provider token is required for the playback gate" >&2
    exit 1
  fi

  if [[ ! -e /dev/fuse ]]; then
    echo "[playback-gate-ci] /dev/fuse is required on the Linux playback runner" >&2
    exit 1
  fi
fi

readiness_args=(-NoProfile -File ./scripts/check_playback_gate_runner.ps1)
if [[ "$FILMU_REQUIRE_PROVIDER_GATE" == "1" ]]; then
  readiness_args+=(-RequireProviderGate)
fi

mkdir -p playback-proof-artifacts

if [[ "$FILMU_PLAYBACK_DRY_RUN" == "1" ]]; then
  echo "[playback-gate-ci] base playback secrets are unavailable; running dry-run fallback"
  pwsh -NoProfile -File ./scripts/run_playback_proof_stability.ps1 \
    -RepeatCount 1 \
    -DryRun

  cat > playback-proof-artifacts/ci-execution-summary.json <<EOF
{
  "required_check_name": "${FILMU_PLAYBACK_REQUIRED_CHECK_NAME}",
  "gate_mode": "dry_run",
  "provider_gate_required": false,
  "provider_gate_ran": false
}
EOF
  exit 0
fi

echo "[playback-gate-ci] validating runner readiness"
pwsh "${readiness_args[@]}"

mkdir -p /mnt/filmuvfs

cleanup() {
  docker compose -f docker-compose.local.yml down --remove-orphans || true
}
trap cleanup EXIT

echo "[playback-gate-ci] validating compose configuration"
docker compose -f docker-compose.local.yml config >/dev/null

echo "[playback-gate-ci] starting playback stack"
docker compose -f docker-compose.local.yml up -d postgres redis filmu-python arq-worker frontend filmuvfs plex emby

echo "[playback-gate-ci] waiting for backend"
for _attempt in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:8000/openapi.json >/dev/null; then
    break
  fi
  sleep 2
done
curl -fsS http://127.0.0.1:8000/openapi.json >/dev/null

echo "[playback-gate-ci] waiting for frontend"
for _attempt in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:3000 >/dev/null; then
    break
  fi
  sleep 2
done
curl -fsS http://127.0.0.1:3000 >/dev/null

echo "[playback-gate-ci] running playback gate"
pwsh -NoProfile -File ./scripts/run_playback_proof_stability.ps1 \
  -RepeatCount 2 \
  -ProofStaleDirectRefresh \
  -RequirePreferredClientPlayback \
  -ReuseExistingItem \
  -RequireCompletedState \
  -TmdbId 603 \
  -Title "The Matrix" \
  -FrontendUsername "$FILMU_FRONTEND_USERNAME" \
  -FrontendPassword "$FILMU_FRONTEND_PASSWORD" \
  -PreferredClientBrowserExecutable "$FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE"

if [[ -n "${PLEX_TOKEN:-}" && -n "${EMBY_API_KEY:-}" ]]; then
  echo "[playback-gate-ci] running provider parity gate"
  pwsh -NoProfile -File ./scripts/run_media_server_proof_gate.ps1 \
    -Providers plex,emby \
    -RepeatCount 2 \
    -FailFast \
    -SkipStart \
    -ReuseExistingItem \
    -TmdbId 603 \
    -Title "The Matrix"
elif [[ "$FILMU_REQUIRE_PROVIDER_GATE" == "1" ]]; then
  echo "[playback-gate-ci] provider parity gate was required but PLEX_TOKEN and/or EMBY_API_KEY were missing" >&2
  exit 1
else
  echo "[playback-gate-ci] skipping provider parity gate because PLEX_TOKEN and/or EMBY_API_KEY are not configured on this runner"
fi

cat > playback-proof-artifacts/ci-execution-summary.json <<EOF
{
  "required_check_name": "${FILMU_PLAYBACK_REQUIRED_CHECK_NAME}",
  "gate_mode": "full",
  "provider_gate_required": ${FILMU_REQUIRE_PROVIDER_GATE},
  "provider_gate_ran": $(if [[ -n "${PLEX_TOKEN:-}" && -n "${EMBY_API_KEY:-}" ]]; then echo true; else echo false; fi)
}
EOF
