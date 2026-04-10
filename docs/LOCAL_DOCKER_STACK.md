# Local Docker Stack

## Purpose

Describe the local Docker stack for meaningful frontend + `filmu-python` integration testing without bringing in production-only services.

Platform-specific operator guides now live in:

- [WINDOWS_README.md](/E:/Dev/Filmu/FilmuCore/WINDOWS_README.md)
- [LINUX_UNIX_README.md](/E:/Dev/Filmu/FilmuCore/LINUX_UNIX_README.md)

Windows-native helper scripts:

- [../start_windows_stack.ps1](../start_windows_stack.ps1)
- [../check_windows_stack.ps1](../check_windows_stack.ps1)
- [../status_windows_stack.ps1](../status_windows_stack.ps1)
- [../stop_windows_stack.ps1](../stop_windows_stack.ps1)

Cross-platform validation helper:

- [../validate_platform_stack.ps1](../validate_platform_stack.ps1)

Linux default stack file:

- [`../docker-compose.yml`](../docker-compose.yml)

Windows backend-only stack file:

- [`../docker-compose.windows.yml`](../docker-compose.windows.yml)

Legacy compatibility stack file:

- [`../docker-compose.local.yml`](../docker-compose.local.yml)

The Linux default stack is intentionally limited to:

- [`postgres`](../docker-compose.local.yml)
- [`redis`](../docker-compose.local.yml)
- [`zilean-postgres`](../docker-compose.local.yml)
- [`zilean`](../docker-compose.local.yml)
- [`filmu-python`](../docker-compose.local.yml)
- [`arq-worker`](../docker-compose.local.yml)
- [`filmuvfs`](../docker-compose.local.yml)
- [`frontend`](../docker-compose.local.yml)
- [`plex`](../docker-compose.local.yml)
- [`emby`](../docker-compose.local.yml)
- [`prowlarr`](../docker-compose.local.yml)

Currently pinned local image versions:

- PostgreSQL: `postgres:18`
- Redis: `redis:8.6.1-alpine`

PostgreSQL 18 note:

- the local stack now mounts the Postgres volume at `/var/lib/postgresql`, not `/var/lib/postgresql/data`
- this matches the newer official PostgreSQL 18 container layout and avoids the restart loop that happens when reusing the older mount path with 18+
- if you previously ran the older `postgres:16` stack, treat this as a fresh local-dev volume transition

It does **not** include:

- NATS
- Temporal
- any other production-only dependencies

## Files

- Linux default stack file: [`../docker-compose.yml`](../docker-compose.yml)
- Windows backend-only stack file: [`../docker-compose.windows.yml`](../docker-compose.windows.yml)
- Legacy compatibility stack file: [`../docker-compose.local.yml`](../docker-compose.local.yml)
- Backend env template: [`.env.example`](../.env.example)
- Backend Docker image: [`Dockerfile.local`](../Dockerfile.local)
- FilmuVFS sidecar image: [`../rust/filmuvfs/Dockerfile.local`](../rust/filmuvfs/Dockerfile.local)
- FilmuVFS Docker ignore rules: [`../rust/filmuvfs/.dockerignore`](../rust/filmuvfs/.dockerignore)
- Frontend reference build context: `E:/Dev/Triven_riven-fork/Triven_frontend`

The [`arq-worker`](../docker-compose.local.yml) service uses the same local backend image definition as [`filmu-python`](../docker-compose.local.yml): same build context, same [`Dockerfile.local`](../Dockerfile.local), same source mount, and the same Docker-network host overrides for PostgreSQL and Redis.

The [`filmuvfs`](../docker-compose.local.yml) service now builds from the narrowed [`../rust/filmuvfs`](../rust/filmuvfs) context, so local Rust build artifacts no longer inflate the image-transfer payload during `docker compose build`.

## Required environment setup

Before running the stack, either:

1. create a real [`.env`](../.env) file at the backend project root, or
2. export the same variables from your shell/session

The recommended path is still a local [`.env`](../.env) file.

Recommended starting point:

```powershell
Copy-Item .env.example .env
```

## Required vs optional environment variables

Strictly required:

- `FILMU_PY_API_KEY` — **must be at least 32 characters** (validated by Pydantic on startup); the backend will crash-loop with a `ValidationError` if this is shorter
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- backend Redis connectivity via `FILMU_PY_REDIS_URL` when running outside Compose, or the built-in `redis` service override inside [`docker-compose.local.yml`](../docker-compose.local.yml)
- at least one debrid provider key: `REAL_DEBRID_API_KEY`, `ALL_DEBRID_API_KEY`, or `DEBRID_LINK_API_KEY`
- `FRONTEND_AUTH_SECRET`
- `FRONTEND_ORIGIN`

Strongly recommended:

- `TMDB_API_KEY` — without it, RTN title similarity matching during scrape-time parsing falls back to raw IDs instead of backend-side TMDB-assisted title matching
- `TMDB_API_KEY` is also what allows request-time TMDB enrichment to populate library-card poster metadata in the full-stack path; without it newly added items fall back to placeholder titles and empty poster fields

Optional, but stage-dependent:

- scraper provider URLs and keys are configured through the persisted frontend settings payload under [`Settings.scraping`](../filmu_py/config.py:539), not through dedicated bootstrap env vars; the scrape worker reads the current runtime [`ScrapingSettings`](../filmu_py/config.py:408) hydrated from the saved compatibility blob
- local Zilean database defaults can be overridden with `ZILEAN_POSTGRES_DB`, `ZILEAN_POSTGRES_USER`, and `ZILEAN_POSTGRES_PASSWORD`
- `FILMU_PY_VERSION` is optional and defaults to the package version used by the backend

Recommended local values:

- `POSTGRES_DB=filmu`
- `POSTGRES_USER=postgres`
- `POSTGRES_PASSWORD=postgres`
- `FRONTEND_ORIGIN=http://localhost:3000`

Important behavior notes:

- [`../docker-compose.yml`](../docker-compose.yml) and [`../docker-compose.windows.yml`](../docker-compose.windows.yml) derive `BACKEND_API_KEY` for the frontend from `FILMU_PY_API_KEY`, so those values stay aligned automatically.
- Both compose files use Compose variable substitution rather than a service-level `env_file`, so they work with either a copied [`.env`](../.env) or shell-provided variables while still staying aligned with [`.env.example`](../.env.example).
- The backend container overrides the DSN/Redis host values to use the Docker service names `postgres` and `redis`.
- Scraper runtime configuration comes from the persisted compatibility settings blob loaded through [`Settings.from_compatibility_dict()`](../filmu_py/config.py:726) and exposed at runtime as typed [`ScrapingSettings`](../filmu_py/config.py:408). Values saved through the frontend settings page are therefore the scrape-stage source of truth.
- Debrid bootstrap env vars remain useful for initial local setup, but once the user saves downloader settings through the frontend, runtime behavior follows [`settings.downloaders`](../filmu_py/config.py:531) rather than reading those env vars directly inside the worker path.
- [`TMDB_API_KEY`](../filmu_py/config.py:516) remains env-only for now. Startup/runtime hydration preserves that env value whenever persisted settings contain an empty `tmdb_api_key`, so backend poster/title enrichment can still work in the full-stack path.
- The backend healthcheck now probes [`/openapi.json`](../filmu_py/app.py:287) instead of [`/api/v1/health`](../filmu_py/api/routes/default.py:53), because the compatibility health route is auth-gated and otherwise causes false `unhealthy` states in local Docker.
- The frontend can show `Unable to fetch stats data!` even while containers are healthy if the persisted compatibility `api_key` in the database no longer matches the frontend [`BACKEND_API_KEY`](../docker-compose.local.yml). The backend runtime hydrates from persisted settings during startup via [`Settings.from_compatibility_dict()`](../filmu_py/config.py:727), so persisted values override the bootstrap env key until the stack is restarted with aligned values.
- Library posters can disappear in the local stack when items have a `tmdb_id` but no persisted `poster_path` and the backend has no runtime [`TMDB_API_KEY`](../filmu_py/config.py:514). The backend now backfills missing library posters during [`MediaService.search_items()`](../filmu_py/services/media.py:2575) through [`MediaService._hydrate_summary_records()`](../filmu_py/services/media.py:2627), but that recovery still requires a real TMDB key in the backend container env.
- The [`arq-worker`](../docker-compose.local.yml) container inherits the same backend environment block and therefore uses the same queue identity as the web service, sourced from `FILMU_PY_ARQ_QUEUE_NAME` and aligned with [`_queue_name()`](../filmu_py/workers/tasks.py:1011), [`AppResources.arq_queue_name`](../filmu_py/resources.py:48), and the app startup queue wiring in [`_arq_queue_name()`](../filmu_py/app.py:56).
- The worker startup command uses [`run_worker_entrypoint()`](../filmu_py/workers/tasks.py) via `python -c "from filmu_py.workers.tasks import run_worker_entrypoint; run_worker_entrypoint()"`. Using `python -m filmu_py.workers.tasks` triggers a `RuntimeWarning: found in sys.modules` from `runpy` that causes the worker to exit immediately — use the `python -c` form instead.
- The backend startup path now supports async PostgreSQL DSNs during Alembic startup, so the local container stack can boot cleanly with the default `postgresql+asyncpg://...` DSN shape from [`.env.example`](../.env.example).
- The local backend image now includes `ffmpeg`, so the container can exercise generated-HLS routes instead of stopping at the old local-image `ffmpeg`-missing boundary.
- The backend runs with local source mounted into `/app` and uses `uvicorn --reload` for a faster local feedback loop.
- The Linux default stack now also includes the [`filmuvfs`](../docker-compose.yml) sidecar container. It connects to [`filmu-python`](../docker-compose.yml) over the Compose network at `http://filmu-python:50051` and mounts the host path `/mnt/filmuvfs` inside the container for the Linux/FUSE validation path.
- The same Linux default stack now also includes [`zilean`](../docker-compose.yml) plus [`zilean-postgres`](../docker-compose.yml). From inside [`filmu-python`](../docker-compose.yml), the correct scraper URL is `http://zilean:8181`.
- Do **not** point the backend scraper setting at `http://localhost:8181`; inside the backend container that resolves back to the backend container itself, not to the Zilean service.
- On Windows + WSL, you must start [`docker compose`](../docker-compose.yml) from WSL if you want the containerized Linux/FUSE validation path to resolve `/mnt/filmuvfs` correctly. The same command run from the Windows-side Docker CLI does not see that WSL path in the same host namespace.
- The WSL host mountpoint `/mnt/filmuvfs` must already exist and be a shared bind mount before the stack starts. See [README.md](../README.md) for the exact setup commands.
- That containerized path is still the Linux-adapter validation topology. It is not the recommended playback topology for a Windows-hosted media server.
- The frontend now expects its TMDB read-access bearer token from `PUBLIC_TMDB_READ_ACCESS_TOKEN`, documented in [`docs/FRONTEND_API_KEYS.md`](./FRONTEND_API_KEYS.md). `TMDB_API_KEY` remains a separate backend-only key required for scrape-time metadata enrichment and title matching.
- In the full-stack compose flow outside this workspace root, the backend and worker containers must both receive `TMDB_API_KEY`; otherwise request-time enrichment in [`MediaService._fetch_request_metadata()`](../filmu_py/services/media.py:1972) cannot populate [`poster_path`](../filmu_py/services/media.py:1965) for library cards.

## Start the stack

```bash
cd /mnt/e/Dev/Filmu/FilmuCore
docker compose up --build
```

Once healthy, the services are available at:

- frontend: `http://localhost:3000`
- backend: `http://localhost:8000`
- zilean: `http://localhost:8181`
- FilmuVFS gRPC catalog supplier: `http://localhost:50051`
- FilmuVFS mount in WSL/Linux: `/mnt/filmuvfs`
- FilmuVFS mount from Windows through WSL UNC: `\\wsl.localhost\Ubuntu-22.04\mnt\filmuvfs\`
- postgres: `localhost:5432`
- redis: `localhost:6379`
- plex: `http://localhost:32401/web`
- emby: `http://localhost:8097`
- prowlarr: `http://localhost:9696`

Windows host guidance:

- Use the WSL UNC path only for debugging or ad hoc inspection.
- Do not treat `\\wsl.localhost\Ubuntu-22.04\mnt\filmuvfs\` as the product-grade playback path for Windows-hosted Jellyfin/Plex/Emby.
- The supported direction for Windows-hosted media servers is the native Windows adapter boundary, not the UNC bridge into a WSL FUSE mount.
- `auto` still resolves to ProjFS by policy/default on Windows, but the currently verified native playback path is the raw WinFSP folder mount in [`../rust/filmuvfs/src/windows_winfsp.rs`](../rust/filmuvfs/src/windows_winfsp.rs) using the canonical folder path `C:\FilmuCoreVFS`.
- The local Docker stack remains useful for the Linux/FUSE validation leg even after the Windows adapter landed.

## Playback-proof harness baseline

The repository now also includes a first playback-proof harness baseline in [`../run_playback_proof.ps1`](../run_playback_proof.ps1).

Current implemented behavior:

- can start or reuse the local stack
- verifies frontend and backend readiness
- captures [`/api/v1/stream/status`](../filmu_py/api/routes/stream.py) before and after the proof run
- submits a real movie request through [`POST /api/v1/items/add`](../filmu_py/api/routes/items.py)
- polls the public item APIs until media-entry or direct-ready state exists
- verifies mounted file visibility and performs a mounted byte-read proof
- writes evidence bundles under `playback-proof-artifacts/`
- can optionally configure one media-server updater target (`plex`, `jellyfin`, or `emby`) through the backend settings API and wait for a backend-side scan signal after completion
- can now resolve local Plex/Emby/Jellyfin URLs and auth tokens from [`.env`](../.env) instead of requiring manual host-side export before every proof run

Convenience entrypoint:

```powershell
pnpm run proof:playback
```

Gate-oriented wrapper:

```powershell
pnpm run proof:playback:gate
```

Important scope note:

- the harness now has a real live green run for request -> acquisition -> mount -> mounted byte-read proof
- the optional Plex-compatible media-server stage is now live-green against the stub target in [`../tests/fixtures/plex_stub_server.py`](../tests/fixtures/plex_stub_server.py)
- the local Docker stack now also provisions isolated real Plex (`http://localhost:32401/web`) and real Emby (`http://localhost:8097`) containers with the mounted `/mnt/filmuvfs` tree bound into each container for parity testing
- the playback-proof harness now auto-loads `PLEX_URL`, `PLEX_TOKEN`, `JELLYFIN_URL`, `JELLYFIN_API_KEY`, `EMBY_URL`, and `EMBY_API_KEY` from [`.env`](../.env) before falling back to manual script args
- real Jellyfin library visibility is now live-green against the existing server on `localhost:8096`
- real Jellyfin playback-info resolution is now also live-green against the existing server on `localhost:8096`
- real Jellyfin stream-open behavior is now also live-green against the existing server on `localhost:8096`
- real Jellyfin playback-session reporting is now also live-green against the existing server on `localhost:8096`
- real Plex and Emby containers are now part of the local stack
- the Docker Plex path is now materially fixed: `/mnt/filmuvfs` is visible inside the container, library scans repopulate correctly, direct part-stream requests return `206`, and recent Plex logs show real transcode/playback startup against mounted files
- full harness-grade Plex playback proof is still an active follow-up; the current evidence is operator-validated container playback rather than an already-encoded proof-step in [`../run_playback_proof.ps1`](../run_playback_proof.ps1)
- the harness now also has a live stale-link proof path through [`../tests/fixtures/force_media_entry_unrestricted_stale.py`](../tests/fixtures/force_media_entry_unrestricted_stale.py) and [`../run_playback_proof.ps1`](../run_playback_proof.ps1), reusing a completed item and forcing the selected direct media entry to a dead localhost URL before probing [`/api/v1/stream/file/{item_id}`](../filmu_py/api/routes/stream.py)
- current stale-link evidence now proves both **route-level recovery** (`206 Partial Content`) and durable persisted lease repair for the selected direct media-entry path, with refreshed lease state mirrored back onto the linked attachment so later detail projections can observe the repaired `unrestricted_url`
- the previous late-stage harness completion hang is now fixed, and successful stale-refresh runs once again write `summary.json` and print the terminal PASS line
- true preferred-client playback is now live-green through the authenticated frontend client on the local stack
- repeated `proof:playback:gate` runs are now also live-green locally
- host-browser proof execution is now explicit and portable: use `-PreferredClientBrowserExecutable` or `FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE` when the container browser lacks the required codec support, while the container-browser path remains available as a fallback
- the GitHub-hosted Linux playback-gate workflow now exists in [`.github/workflows/playback-gate.yml`](../.github/workflows/playback-gate.yml), is driven by [`../run_playback_gate_ci.sh`](../run_playback_gate_ci.sh), and has already gone green on the last merged playback PR before landing in `main`
- the remaining playback-proof step is repeated stability evidence plus explicit validation of live GitHub required-check policy from an admin-authenticated host, not first preferred-client playback
- the current local Docker flow still validates the Linux/FUSE side of FilmuVFS. Windows-native ProjFS validation should be treated as a separate host test leg rather than assumed from the WSL UNC path.

## Stop the stack

Keep volumes:

```bash
docker compose down
```

Remove volumes too:

```bash
docker compose down -v
```

## Health and startup ordering

- `postgres` uses `pg_isready`
- `redis` uses `redis-cli ping`
- `filmu-python` waits for both and exposes its own healthcheck at [`/api/v1/health`](../filmu_py/api/routes/default.py), with the Compose probe sending the configured `FILMU_PY_API_KEY`
- `arq-worker` waits for both `postgres` and `redis` to be healthy before starting; it does **not** wait for `filmu-python` to be healthy (the backend healthcheck at `/api/v1/health` is auth-gated, making a pure curl-based compose healthcheck unreliable — the worker can self-manage reconnection to Postgres/Redis via ARQ's built-in retry)
- `filmuvfs` waits for [`filmu-python`](../docker-compose.yml) to be healthy before starting, because it mounts only after the Python-side gRPC supplier is available
- `zilean` waits for [`zilean-postgres`](../docker-compose.yml) to be healthy before starting
- The backend must have `FILMU_PY_ARQ_ENABLED=true` for the immediate-enqueue paths in both `POST /api/v1/items/add` and the missing-item compatibility flow in `POST /api/v1/scrape/auto` to work; without it the backend skips creating the ARQ Redis pool and new requests fall back to the next `retry_library` cron cycle (every 15 min) rather than immediate scrape scheduling
- `frontend` waits for `filmu-python` to become healthy before starting
- `prowlarr` has no dependency ordering — it starts independently and is available once its own container is ready

Current verified state:

- the Linux default stack now renders successfully through [`docker compose config`](../docker-compose.yml)
- the backend health endpoint returns `200` with the configured API key
- the [`arq-worker`](../docker-compose.yml) service is defined in the Linux default stack and shares the same local backend image/runtime wiring as [`filmu-python`](../docker-compose.yml)
- the [`filmuvfs`](../docker-compose.yml) image now builds from a small Rust-only context instead of transferring gigabytes of local Rust artifact data
- the same Linux default stack now also defines [`zilean-postgres`](../docker-compose.yml) and [`zilean`](../docker-compose.yml) for local scraper/index validation
- the frontend also responds on `http://localhost:3000`

## Red-team notes

- Secrets are **not** hardcoded in [`../docker-compose.yml`](../docker-compose.yml) or [`../docker-compose.windows.yml`](../docker-compose.windows.yml); they come from [`.env`](../.env).
- The [`filmuvfs`](../docker-compose.yml) sidecar is now part of the Linux default stack, but that compose topology depends on the WSL-side shared mount `/mnt/filmuvfs` being prepared before startup because it is exercising the Linux adapter path.
- The stack is for local integration only and should not be treated as a production deployment artifact.

## Troubleshooting reference

For the recent local incidents and recovery workflow, see [`LOCAL_DOCKER_TROUBLESHOOTING.md`](./LOCAL_DOCKER_TROUBLESHOOTING.md).
