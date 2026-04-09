# filmu-python

 Python compatibility backend for a Filmu-style runtime, with a real provider-backed worker pipeline and a working Rust FilmuVFS sidecar.

## Objectives

- Preserve frontend compatibility via `/api/v1/*` and `/openapi.json`.
- Introduce Python++ runtime foundations:
  - FastAPI + orjson/msgspec
  - Redis primitives (rate limiting, queues, cache)
  - Temporal-ready orchestration interfaces
  - Tenacity retries + PyBreaker circuit breakers
  - OpenTelemetry + Prometheus + Sentry hooks

## Technology strategy (performance/security first)

- Runtime and I/O performance:
  - `uvloop` + `httptools` + `orjson`/`msgspec`
  - optional DNS acceleration with `aiodns`
  - efficient compression/hash paths (`zstandard`, `lz4`, `xxhash`)
- Security hardening:
  - strict API-key handling with masked secrets
  - optional Argon2/JWT stack (`argon2-cffi`, `python-jose`)
  - dependency and static security checks (`pip-audit`, `bandit`)
- Reliability and observability:
  - distributed rate limiting + cache + startup health checks
  - OpenTelemetry + Prometheus + structured logging (`structlog`)

These are additive foundations intended to outperform the current TypeScript runtime under load while improving operational safety.

## Orchestration & queue model

- **State progression**: typed deterministic transitions with persisted event history.
- **Queue/workers (current)**: ARQ Redis workers for `scrape_item -> parse_scrape_results -> rank_streams -> debrid_item -> finalize_item`, plus recovery, content-service, and outbox control-plane jobs.
- **Durability layer (planned)**: Temporal for long-running, compensating workflows.

This two-tier approach (ARQ + Temporal) is intended to exceed queue-only orchestration by combining high throughput with stronger recovery guarantees.

See:

- `docs/ORCHESTRATION.md`
- `docs/ARCHITECTURE.md`

## VFS status

FilmuVFS now exists as a Rust sidecar with platform-specific host adapters:

- Linux and Unix-like hosts use the traditional `fuse3` mount path.
- Windows hosts now go through an explicit native adapter boundary and mount into a normal Windows folder chosen by the operator.

Current Windows build status:

- `projfs` remains the policy/default Windows-native adapter.
- `auto` is the default helper mode and still resolves to `projfs` on Windows.
- `winfsp` still requires explicit opt-in with `FILMUVFS_ENABLE_EXPERIMENTAL_WINFSP=1`, but the raw WinFSP folder-mount path in [`rust/filmuvfs/src/windows_winfsp.rs`](/E:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/windows_winfsp.rs) now has a verified Windows-host playback path at `C:\FilmuCoreVFS`.
- Current verified Windows playback result: the WinFSP path now survives the native soak/remux gate on `C:\FilmuCoreVFS`, Jellyfin reaches sustained mounted reads and successful software transcode, native Emby playback/probe/stream-open checks succeed through the real provider proof on the same mount, and native Windows Plex now also reran green through the real local PMS at `http://127.0.0.1:32400` using the local admin token. The repo exposes that native Windows provider-gate surface in [`scripts/run_windows_media_server_gate.ps1`](/E:/Dev/Filmu/FilmuCore/scripts/run_windows_media_server_gate.ps1). The remaining Windows Plex work is no longer claim/setup bring-up; it is keeping the now-green proof path stable through repeatable reruns and CI/merge-policy promotion.
- Current verified Linux/WSL parity result: the isolated Docker Plex instance now works against the shared `/mnt/filmuvfs` mount and the direct provider proof gate reran green on April 9, 2026 for Docker Plex plus native Windows Emby. The playback-proof bundle now emits explicit Docker/WSL Plex evidence in `summary.json` plus a dedicated `plex-wsl-evidence.json` artifact covering mount visibility, host-binary freshness, refresh-identity visibility, and foreground-fetch/coalescing visibility. Those checks are now explicit rather than warning-only and reran green twice on April 9, 2026 after adding the WSL mount preflight plus ANSI-safe evidence parsing. Native Windows Plex parity is now also green through the real local PMS on `http://127.0.0.1:32400`, so the remaining Plex work is CI/merge-policy promotion plus longer-running stability hardening rather than first native parity.

The remaining VFS work is longer-running hardening, richer observability, and broader playback/read-path validation. Current streaming/VFS status is documented in:

- `docs/VFS.md`
- `docs/STATUS.md`

## Local setup

1. Copy `.env.example` to `.env`.
2. Install dependencies:
   - `python -m pip install -e .[dev]`
3. Run the app:
   - `python -m filmu_py.main`

## Platform guides

- Windows hosts: [WINDOWS_README.md](/E:/Dev/Filmu/FilmuCore/WINDOWS_README.md)
- Linux and Unix-like hosts: [LINUX_UNIX_README.md](/E:/Dev/Filmu/FilmuCore/LINUX_UNIX_README.md)
- Local stack details: [docs/LOCAL_DOCKER_STACK.md](/E:/Dev/Filmu/FilmuCore/docs/LOCAL_DOCKER_STACK.md)
- Quick chooser: [QUICK_START.md](/E:/Dev/Filmu/FilmuCore/QUICK_START.md)

Compose files:

- Linux default: [docker-compose.yml](/E:/Dev/Filmu/FilmuCore/docker-compose.yml)
- Windows backend-only: [docker-compose.windows.yml](/E:/Dev/Filmu/FilmuCore/docker-compose.windows.yml)

Windows helper entrypoints:

- start native mount: [start_windows_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/start_windows_stack.ps1)
- check native mount: [check_windows_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/check_windows_stack.ps1)
- inspect native mount: [status_windows_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/status_windows_stack.ps1)
- stop native mount: [stop_windows_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/stop_windows_stack.ps1)
- validate the split stack wiring: [validate_platform_stack.ps1](/E:/Dev/Filmu/FilmuCore/scripts/validate_platform_stack.ps1)

The managed Windows start path defaults to `-MountAdapter auto`, records requested and effective adapters in state, and preflights `Client-ProjFS` before backend startup when needed. Startup warmup uses a dedicated startup-prefetch window so media handles can stage initial bytes before playback settles into steady-state adaptive prefetch behavior. For native Windows-host playback validation, the canonical folder mount path is `C:\FilmuCoreVFS`; drive-letter aliases are intentionally not part of the managed path.

## FilmuVFS overview

FilmuVFS exposes debrid-backed media files as a virtual filesystem so Jellyfin, Plex, and Emby can use normal library paths.

- On Linux and Unix-like hosts, FilmuVFS mounts into paths like `/mnt/filmuvfs`.
- On Windows hosts, FilmuVFS mounts into any normal folder path you choose, with `C:\FilmuCoreVFS` as the canonical helper-managed example.
- For isolated local Plex parity in this workspace, Docker Plex runs separately at `http://localhost:32401/web` with its own config/database volume so it does not touch any existing Windows Plex installation.

Files only appear in the VFS after FilmuCore completes the acquisition pipeline for the requested item.

## Linting / quality

- Lint + types: `pnpm run lint`
- Format check: `pnpm run format:check`
- Security audit: `pnpm run security:audit`
- Security static scan: `pnpm run security:bandit`

## Status

This is a working compatibility backend with real acquisition, playback, and FilmuVFS slices, but it is not yet full upstream parity or fully production-hardened across every playback/VFS path.



