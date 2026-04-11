# March 2026 Completed Slice Closeout — VFS Hardening, Plugin Runtime, Observability Layer 1

## Scope

This note records the now-completed delivery of the recent multi-slice backend work that landed across the VFS/runtime, plugin platform, and observability tracks.

It exists so the current planning docs can stay forward-looking while this file preserves the delivered closeout summary.

## Completed slices

### 1. FilmuVFS runtime hardening

Delivered:

- generation-aware reconnect handling in [`../../filmu_py/services/vfs_catalog.py`](../../filmu_py/services/vfs_catalog.py) and [`../../filmu_py/services/vfs_server.py`](../../filmu_py/services/vfs_server.py)
- forced stale-link refresh through `RefreshCatalogEntry` in [`../../proto/filmuvfs/catalog/v1/catalog.proto`](../../proto/filmuvfs/catalog/v1/catalog.proto)
- inline stale-read recovery in [`../../rust/filmuvfs/src/mount.rs`](../../rust/filmuvfs/src/mount.rs)
- async cache migration to `moka::future::Cache` in [`../../rust/filmuvfs/src/chunk_engine.rs`](../../rust/filmuvfs/src/chunk_engine.rs)
- stable assigned inode preservation plus collision fallback in [`../../rust/filmuvfs/src/catalog/state.rs`](../../rust/filmuvfs/src/catalog/state.rs)

Validation delivered:

- focused Python coverage in [`../../tests/test_vfs_catalog.py`](../../tests/test_vfs_catalog.py) and [`../../tests/test_vfs_server.py`](../../tests/test_vfs_server.py)
- focused Rust coverage in [`../../rust/filmuvfs/tests/read_path.rs`](../../rust/filmuvfs/tests/read_path.rs) and [`../../rust/filmuvfs/tests/catalog_state.rs`](../../rust/filmuvfs/tests/catalog_state.rs)
- Rust validation through `rust:fmt`, `rust:check`, and `rust:test`

### 2. Plugin runtime/platform expansion

Delivered:

- plugin-scoped settings registry in [`../../filmu_py/plugins/settings.py`](../../filmu_py/plugins/settings.py)
- datasource-aware runtime context construction in [`../../filmu_py/plugins/context.py`](../../filmu_py/plugins/context.py)
- typed event-hook worker registration/execution in [`../../filmu_py/plugins/hooks.py`](../../filmu_py/plugins/hooks.py)
- namespaced publishable-event governance in [`../../filmu_py/core/event_bus.py`](../../filmu_py/core/event_bus.py) and [`../../filmu_py/plugins/registry.py`](../../filmu_py/plugins/registry.py)
- runtime visibility through [`../../filmu_py/api/routes/default.py`](../../filmu_py/api/routes/default.py) for both `/api/v1/plugins` and `/api/v1/plugins/events`
- built-in MDBList, StremThru, and webhook notification stubs in [`../../filmu_py/plugins/builtin/mdblist.py`](../../filmu_py/plugins/builtin/mdblist.py), [`../../filmu_py/plugins/builtin/stremthru.py`](../../filmu_py/plugins/builtin/stremthru.py), and [`../../filmu_py/plugins/builtin/notifications.py`](../../filmu_py/plugins/builtin/notifications.py)

Closeout hardening also delivered:

- undeclared namespaced plugin events now drop-and-warn instead of silently fanning out
- hanging hooks now time out explicitly
- built-in stubs now emit explicit readiness warnings when not configured
- worker plugin contexts now hydrate from persisted plugin settings payload semantics

### 3. Observability Layer 1

Delivered:

- template-based route metrics in [`../../filmu_py/api/router.py`](../../filmu_py/api/router.py)
- worker stage/retry/DLQ metrics plus contextvars correlation in [`../../filmu_py/workers/retry.py`](../../filmu_py/workers/retry.py) and [`../../filmu_py/workers/tasks.py`](../../filmu_py/workers/tasks.py)
- cache hit/miss/invalidation/stale counters in [`../../filmu_py/core/cache.py`](../../filmu_py/core/cache.py)
- plugin load and hook execution/duration metrics in [`../../filmu_py/plugins/loader.py`](../../filmu_py/plugins/loader.py) and [`../../filmu_py/plugins/hooks.py`](../../filmu_py/plugins/hooks.py)
- dedicated regression coverage in [`../../tests/test_observability.py`](../../tests/test_observability.py)

Validation delivered:

- `ruff check .`
- `mypy --strict filmu_py/`
- `pytest -q` (`436 passed`)

## Documentation refresh completed alongside this closeout

The primary forward-looking docs were refreshed to reflect this delivered state:

- [`../../STATUS.md`](../../STATUS.md)
- [`../../ARCHITECTURE.md`](../../ARCHITECTURE.md)
- [`../../VFS.md`](../../VFS.md)
- [`../../PLUGINS.md`](../../PLUGINS.md)
- [`../../PLUGIN_SDK.md`](../../PLUGIN_SDK.md)
- [`../../ORCHESTRATION.md`](../../ORCHESTRATION.md)
- [`../../EXECUTION_PLAN.md`](../../EXECUTION_PLAN.md)
- [`../../STATUS.md`](../../STATUS.md)
- [`../PLUGIN_CAPABILITY_MODEL_MATRIX.md`](../PLUGIN_CAPABILITY_MODEL_MATRIX.md)
- [`../ORCHESTRATION_BREADTH_MATRIX.md`](../ORCHESTRATION_BREADTH_MATRIX.md)
- [`../FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md`](../FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md)
- [`../OBSERVABILITY_MATURITY_MATRIX.md`](../OBSERVABILITY_MATURITY_MATRIX.md)

## Remaining forward frontier after these slices

The next priorities are now clearly narrower:

1. playback parity polish and full frontend/BFF validation
2. mount-side shared chunk-engine adoption plus optional disk cache/prefetch
3. stronger plugin compatibility policy and real non-stub built-in integrations
4. observability beyond layer 1, especially rate-limiter, GraphQL, queue-lag, and mounted data-plane visibility
