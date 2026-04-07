# AGENTS.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project scope
- This repository is a Python-first backend (`filmu_py`) with a Rust FilmuVFS sidecar (`rust/filmuvfs`).
- The backend exposes two API surfaces:
  - REST compatibility surface under `/api/v1/*`
  - GraphQL surface under `/graphql`
- Core startup wiring is in `filmu_py/app.py` and runtime entrypoints are in `filmu_py/main.py` and `filmu_py/workers/tasks.py`.

## Common commands
- Install Python dev dependencies:
  - `python -m pip install -e .[dev]`
- Run API service locally:
  - `python -m filmu_py.main`
  - or `pnpm run dev`
- Run ARQ worker locally:
  - `python -m filmu_py.workers.tasks`
  - or `pnpm run worker`
- Run lint/type checks:
  - `pnpm run lint`
- Run format checks:
  - `pnpm run format:check`
- Run all tests:
  - `uv run --extra dev pytest -q`
- Run a single Python test file:
  - `uv run --extra dev pytest -q tests/test_stream_routes.py`
- Run a single Python test case:
  - `uv run --extra dev pytest -q tests/test_stream_routes.py::test_stream_status_route_includes_governance_fields`
- Rust sidecar checks/tests:
  - `pnpm run rust:fmt`
  - `pnpm run rust:check`
  - `pnpm run rust:test`
- Run a single Rust test:
  - `cargo test --manifest-path ./rust/filmuvfs/Cargo.toml <test_name>`
- Security/quality commands:
  - `pnpm run security:audit`
  - `pnpm run security:bandit`
  - `pnpm run perf:bench`
- Regenerate Python protobuf bindings for FilmuVFS catalog:
  - `python -m filmu_py.proto_codegen`
- Local full stack (backend + worker + postgres + redis + frontend):
  - `docker compose -f docker-compose.local.yml up --build`

## High-level architecture

### 1) App composition and runtime resources
- `filmu_py/app.py` builds the FastAPI app, router wiring, GraphQL router, observability, and lifespan resource lifecycle.
- `AppResources` (`filmu_py/resources.py`) is the central runtime container (settings, Redis, DB runtime, cache, rate limiter, event bus, media service, plugin registry, playback services, ARQ client, VFS catalog supplier/server).
- On startup, persisted settings are loaded from DB and replace bootstrap env defaults when available.

### 2) API surfaces and service boundary
- REST compatibility routes are under `filmu_py/api/routes/` and mounted by `filmu_py/api/router.py` under `/api/v1` with API-key dependency.
- GraphQL schema is built in `filmu_py/graphql/schema.py` with plugin-aware resolver composition.
- Both REST and GraphQL project from shared domain/service logic (primarily `filmu_py/services/media.py`, plus playback/settings/services modules) rather than duplicating business rules per surface.

### 3) Persistence and state model
- Async SQLAlchemy runtime is in `filmu_py/db/runtime.py`; schema evolution is via Alembic in `filmu_py/db/alembic/versions/`.
- Domain lifecycle is state-machine driven (`filmu_py/state/item.py`) and persisted via item/events plus related media tables (`filmu_py/db/models.py`).
- Runtime settings are modeled in `filmu_py/config.py` and exposed via compatibility translation methods used by settings routes.

### 4) Orchestration pipeline (ARQ)
- Worker pipeline in `filmu_py/workers/tasks.py` is stage-based:
  - `scrape_item` -> `parse_scrape_results` -> `rank_streams` -> `debrid_item` -> `finalize_item`
- Jobs use stable IDs and queue checks for idempotent enqueue behavior.
- Retry/dead-letter behavior is centralized in `filmu_py/workers/retry.py`.
- Cron jobs also handle retry-library and outbox publication stages.

### 5) Playback and streaming path
- Stream/HLS compatibility routes live in `filmu_py/api/routes/stream.py`.
- Shared byte-serving and serving-governance primitives live in `filmu_py/core/byte_streaming.py`.
- Playback source selection and lease/refresh orchestration are in `filmu_py/services/playback.py`.
- Item detail projections include playback/media-entry ownership views sourced from persisted media entries and active stream relations.

### 6) FilmuVFS Python↔Rust boundary
- Proto contract source of truth: `proto/filmuvfs/catalog/v1/catalog.proto`.
- Python-side catalog projection supplier: `filmu_py/services/vfs_catalog.py`.
- Python gRPC server bridge for catalog watch: `filmu_py/services/vfs_server.py`.
- Rust sidecar runtime:
  - bootstrap: `rust/filmuvfs/src/main.rs`
  - runtime orchestration: `rust/filmuvfs/src/runtime.rs`
  - watch client/state: `rust/filmuvfs/src/catalog/client.rs`, `rust/filmuvfs/src/catalog/state.rs`
- Rust mount behavior is now adapter-based:
  - Linux hosts use the `fuse3` adapter in `rust/filmuvfs/src/mount.rs`
  - Windows hosts have both the ProjFS adapter in `rust/filmuvfs/src/windows_projfs.rs` and the WinFSP adapter in `rust/filmuvfs/src/windows_winfsp.rs`
  - adapter selection is configured through `FILMUVFS_MOUNT_ADAPTER` / `--mount-adapter` and resolved in `rust/filmuvfs/src/config.rs`
- Windows adapter policy vs. current validation status:
  - `auto` still resolves to `projfs` by policy/default on Windows
  - current native Windows-host playback hardening is centered on the raw WinFSP folder-mount path at `C:\FilmuCoreVFS`
  - when diagnosing Jellyfin/Plex/Emby behavior on Windows, verify actual OS state first (`filmuvfs` PID alive, `C:\FilmuCoreVFS` exists) before drawing conclusions from media-server logs
- The catalog/watch/runtime control plane remains cross-platform, while the host-filesystem adapter is platform-specific.

### 7) Plugin system
- Plugin loading is manifest-driven and safe-by-default in `filmu_py/plugins/loader.py`.
- Manifest/contracts: `filmu_py/plugins/manifest.py`, `filmu_py/plugins/interfaces.py`.
- Registry and capability access: `filmu_py/plugins/registry.py`.
- GraphQL plugin resolver registration is merged into schema construction via `GraphQLPluginRegistry`.

## Testing notes tied to this repository
- Tests are in `tests/` and use pytest.
- Many route/service tests construct lightweight runtime stubs rather than requiring real external services.
- The test fixture in `tests/conftest.py` resets runtime settings state automatically around each test.
