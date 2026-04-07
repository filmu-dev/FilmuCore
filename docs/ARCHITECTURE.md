# Filmu Python Compatibility Architecture

## Goal

Create a Python backend that preserves frontend compatibility with `/api/v1/*` while adopting stronger runtime primitives (distributed rate limits, durable orchestration, resilience policies, caching, and observability).

## filmu-ts → Python mapping (updated)

| filmu-ts component                | Python equivalent (target)                        |
| --------------------------------- | ------------------------------------------------- |
| `@apollo/server` + `type-graphql` | `strawberry-graphql` (+ FastAPI integration)      |
| `@mikro-orm/postgresql`           | SQLAlchemy + Alembic                              |
| `bullmq`                          | ARQ (Redis-backed)                                |
| `xstate`                          | `transitions` or custom typed async state machine |
| `@zkochan/fuse-native`            | Rust FilmuVFS sidecar + gRPC catalog contract     |
| `zod`                             | Pydantic                                          |
| `bullmq-otel`                     | OpenTelemetry                                     |
| `winston`                         | `structlog`                                       |

## Current gap split: GraphQL parity vs current frontend enablement

The current `filmu-python` backend is REST-first for the actively used frontend integration path. Upstream `filmu-ts` is GraphQL-first and runs codegen from a local schema endpoint ([`codegen.ts`](../../../.tmp-filmu-ts/apps/filmu/codegen.ts:6)).

However, the current [`Triven_frontend`](../../../Triven_frontend) is presently BFF/REST-driven against `/api/v1/*` routes rather than directly consuming GraphQL from the backend. That means:

- **GraphQL parity** remains strategically important for platform parity with `filmu-ts`, plugin architecture, and future frontend evolution.
- **REST/SSE breadth** is the more immediate unblocker for meaningful local testing of the current frontend against `filmu-python`.

Planned parity path:

1. Add `strawberry` schema root and mount GraphQL router.
2. Implement Query/Mutation baseline for items/settings/requests.
3. Add Subscription channel for state changes (WebSocket-based).
4. Keep REST compatibility routes until migration is complete.

Current frontend-enablement path:

1. Expand `/api/v1/*` breadth for the route families the current frontend already calls.
2. Keep SSE compatibility aligned with the frontend proxy/store expectations.
3. Use GraphQL expansion as a strategic platform track rather than assuming it is the current frontend's immediate dependency.

Additional re-audit findings from the original TS backend:

- The TS platform already has a richer plugin system than a simple resolver registry: plugin datasources, validators, event hooks, plugin-specific queues/workers, and runtime GraphQL context composition are all part of the current model.
- The Python platform now covers the first real runtime slice of that breadth too: plugin-scoped settings, datasource injection, event hook workers, publishable-event governance, real built-in MDBList/StremThru/notification integrations, and runtime event visibility are implemented; the remaining gap is distributable/plugin-policy depth, not first runtime context.
- The TS orchestration breadth is also deeper than a single linear queue: it includes request-content-services intake, index, scrape, scrape parsing, ranking, download fan-out, and retry-library recovery paths.
- The Python roadmap should capture those as explicit platform capabilities rather than assuming they will emerge automatically from a smaller worker graph.

## API surface strategy

The backend now follows a deliberate **dual-surface API strategy**.

### 1. [`/api/v1/*`](../filmu_py/api/routes/default.py) is the compatibility surface

- shape-stable
- riven-compatible
- supports the current frontend and its generated OpenAPI client
- preserves the keyed/dict-heavy and legacy field-shape constraints that the current frontend already depends on

This surface exists to keep the current frontend working without forcing product/API cleanup to happen inside compatibility handlers.

### 2. [`/graphql`](../filmu_py/graphql/schema.py) is the intentional surface

- schema-driven
- richer and more explicit
- unconstrained by the current riven REST compatibility contract
- designed for a future frontend or richer internal product clients

This surface is where the long-term product API should evolve deliberately rather than inheriting the quirks of the current compatibility routes.

### 3. Both surfaces call the same service layer

- REST compatibility routes call the shared service layer in [`../filmu_py/services/media.py`](../filmu_py/services/media.py)
- GraphQL resolvers also call that same service layer in [`../filmu_py/graphql/resolvers.py`](../filmu_py/graphql/resolvers.py)
- the backend should not duplicate business logic between REST and GraphQL

This keeps the compatibility surface and the intentional surface as **different projections of the same domain/service layer**, not as competing implementations.

Retry and reset now also follow that dual-surface rule:

- REST [`/api/v1/items/retry`](../filmu_py/api/routes/items.py) and [`/api/v1/items/reset`](../filmu_py/api/routes/items.py) remain the compatibility surface and preserve [`ItemActionResponse`](../filmu_py/api/models.py)
- GraphQL [`retryItem`](../filmu_py/graphql/resolvers.py) and [`resetItem`](../filmu_py/graphql/resolvers.py) are the richer intentional mutations
- both surfaces call [`MediaService.retry_item()`](../filmu_py/services/media.py) and [`MediaService.reset_item()`](../filmu_py/services/media.py)
- both paths perform IMDb enrichment before re-queuing when `attributes.imdb_id` is absent

### 4. Deprecation path

When a future frontend is built on GraphQL:

- the compatibility `/api/v1/*` routes can be deprecated gradually
- those routes can eventually be removed
- the shared service layer should remain intact

That means route deprecation should be a surface-level change, not a service/domain rewrite.

### 5. Long-term product stance

The GraphQL schema should be treated as the **long-term product API** and evolved deliberately:

- explicit schema evolution
- typed richer projections
- stable product-facing naming
- no compatibility-only constraints unless intentionally preserved

### 4a. GraphQL subscription surface strategy (recorded Slice E)

GraphQL subscriptions are being built as a **compatibility layer first**:

- Current subscriptions keep a dual-surface strategy:
  - [`itemStateChanged`](../filmu_py/graphql/schema.py) and [`notifications`](../filmu_py/graphql/schema.py) still mirror the existing SSE payloads exactly.
  - [`logStream`](../filmu_py/graphql/schema.py) is now the intentional richer structured log surface, while [`/api/v1/logs`](../filmu_py/api/routes/default.py) and the existing SSE logging stream remain the stable compatibility surfaces.
- SSE routes (`/api/v1/stream/*`) remain the primary surface for the current riven frontend and are frozen (no new features).
- REST routes (`/api/v1/*`) are also frozen — plugins and external tools may continue to use them, but no new UI features land there.
- When the future frontend is built, further rich fields land in GraphQL first and the REST/SSE routes receive `Deprecation` + `Sunset` headers.
- REST/SSE routes are removed after the future frontend ships and the grace period passes.
- All new capability lands in GraphQL first.

### Mutation surface (added Slice F)

GraphQL mutations now cover the three primary write operations:

- `requestItem` — create/upsert a media item request, including partial season support.
- `itemAction` — trigger retry/reset/remove on an existing item.
- `updateSetting` — update one settings value by dot-separated path.

REST [`/api/v1/*`](../filmu_py/api/router.py) routes handling the same operations remain frozen and unchanged.
All new write capability lands in GraphQL mutations from this point forward.

### Playback compatibility contract freeze (March 2026)

The playback path is now treated as a **stability-first compatibility surface** while the team closes end-to-end playback proof.

Frozen playback-facing contracts for the next 6 months:

- [`/api/v1/stream/file/{item_id}`](../filmu_py/api/routes/stream.py)
- [`/api/v1/stream/hls/{item_id}/*`](../filmu_py/api/routes/stream.py)
- the current FilmuVFS catalog + mount-facing compatibility contracts consumed by active clients

Rules during this freeze window:

- no breaking URL, query-parameter, or header changes on the current playback routes
- no breaking client-visible failure-contract changes without an explicit versioned migration path
- proto and mount-contract changes should be additive wherever possible
- internal hardening, observability, retry, cleanup, and resilience work remain allowed and encouraged

This freeze exists so the team can prove playback reliability through the current frontend -> backend -> FilmuVFS path without destabilizing the active consumers while that proof work is in flight.

Execution ownership, harness requirements, and the immediate playback-proof sequence are defined in [`TODOS/PLAYBACK_PROOF_IMPLEMENTATION_PLAN.md`](TODOS/PLAYBACK_PROOF_IMPLEMENTATION_PLAN.md).

## Settings schema strategy within the dual-surface model

Settings now follow the same dual-surface rule as the rest of the backend.

### Compatibility settings surface

- the `/api/v1/settings/*` handlers must keep the original `settings.json` contract exactly shape-compatible for the current frontend
- field names, nesting, list/dict layout, and scalar types are compatibility requirements rather than cleanup opportunities
- the original riven schema remains the compatibility source of truth for this surface

### Internal settings model

- the runtime model in [`../filmu_py/config.py`](../filmu_py/config.py) is now the clean internal representation
- nested settings sections are modeled as typed Pydantic objects instead of leaving the backend dependent on ad-hoc compatibility dicts
- this internal model is the correct place to evolve future backend logic and a future GraphQL-first frontend without inheriting legacy naming quirks unnecessarily

### Translation boundary

- [`Settings.to_compatibility_dict()`](../filmu_py/config.py) is the compatibility projection used by the current REST settings surface
- [`Settings.from_compatibility_dict()`](../filmu_py/config.py) is the inverse hydration path used to ingest the original riven-compatible JSON shape
- this is the same architectural pattern as the earlier downloader `_settings_dump()` bridge, but generalized across the full settings schema

### Product implication

- compatibility cleanup should happen only inside the translation layer, not by mutating the public `/api/v1/settings/*` contract
- GraphQL and future frontend work can consume the typed internal model directly or expose deliberate product-facing projections from it
- persistence and runtime mutation semantics can now be added later without redoing the schema-modeling foundation

### Runtime source-of-truth rule

- backend runtime code must read scraper and downloader configuration from the active typed [`Settings`](../filmu_py/config.py:496) object, not by re-reading bootstrap env vars inside service clients or workers
- for scraper execution specifically, runtime source of truth is the persisted [`settings.scraping`](../filmu_py/config.py:539) compatibility block hydrated into typed [`ScrapingSettings`](../filmu_py/config.py:408)
- for debrid execution specifically, runtime source of truth is the persisted [`settings.downloaders`](../filmu_py/config.py:531) block hydrated into typed [`DownloadersSettings`](../filmu_py/config.py:241)
- env-backed secrets may still provide bootstrap defaults before a user saves settings, but once the frontend settings page persists values through [`/api/v1/settings/*`](../filmu_py/api/routes/settings.py:125), worker/runtime behavior must follow the persisted settings model
- [`TMDB_API_KEY`](../filmu_py/config.py:516) is the current env-only exception: persisted compatibility blobs may contain an empty `tmdb_api_key`, and runtime hydration must preserve the environment value in that case.

## High-level layers

1. **Compatibility API Layer (FastAPI)**
   - Exposes legacy-compatible REST + SSE contract.
   - Maintains OpenAPI shape stability.
   - Will host GraphQL router for filmu-ts parity.
   - Already includes a growing compatibility surface for the current frontend without locking the backend into that frontend's current UX limitations.
   - Uses a backend-side API key model suitable for the current BFF architecture; see [`AUTH.md`](AUTH.md).
2. **Core Runtime Services**
   - Queue orchestration hooks.
   - Rate limiting and resilience policies.
   - Cache and data access abstractions.
   - Manifest-driven and packaged plugin discovery with runtime capability registration, plugin-scoped settings, datasource injection, typed hook execution, and publishable-event governance.
   - Bounded in-memory log history and live event fan-out for compatibility streaming.
3. **Infrastructure Integrations**
   - Redis (rate limits + cache + queue control data).
   - PostgreSQL (state and metadata persistence).
   - Temporal (optional durable workflow orchestration).
4. **Observability**
   - Prometheus metrics endpoint plus route, worker, cache, and plugin telemetry.
   - OTLP traces (optional).
   - Sentry error capture (optional).

## FilmuVFS process-boundary note

- FilmuVFS is now planned as a **separate Rust process**, not an embedded Python `pyfuse3` worker.
- The Python backend remains the source of truth for provider clients, link resolution, lease refresh, orchestration, rate limiting, and API behavior.
- The Rust sidecar consumes Python-supplied catalog state over a gRPC contract and owns the mount/data plane.

## Current runtime status

- Environment and settings model is in place.
- Basic logging and observability bootstrap added.
- Redis token-bucket rate limiter primitive added.
- Two-layer cache primitive added.
- Compatibility API endpoints now cover settings, logs/SSE, dashboard, items, calendar, scrape, and legacy watch-alias baselines for the current frontend.
- GraphQL layer now runs on Strawberry with plugin-aware schema composition.
- Plugin discovery now supports both filesystem manifests and packaged entry points, while the runtime capability model now includes plugin-scoped settings, datasource injection, typed hook workers, publishable-event governance, and real built-in MDBList/StremThru/notification capability implementations.
- Runtime plugin visibility now includes both [`GET /api/v1/plugins`](../filmu_py/api/routes/default.py) and [`GET /api/v1/plugins/events`](../filmu_py/api/routes/default.py) so operators can inspect loaded capabilities plus declared publishable/subscribed events.
- Historical log compatibility is available through `/api/v1/logs` with bounded in-memory retention.
- SSE compatibility is available through `/api/v1/stream/event_types` and `/api/v1/stream/{event_type}` for current log/notification flows.
- Completion notifications currently emit frontend-compatible payloads while keeping the backend event model simple enough to support a stronger future frontend.
- SQLAlchemy async runtime and Alembic baseline were added for media/state persistence.
- ARQ worker graph is now live for scrape -> parse-scrape-results -> rank-streams -> debrid -> finalize transitions while still remaining compatibility-first for the current frontend.
- TMDB request-time enrichment now includes a secondary `/external_ids` recovery path for missing IMDb mappings, and the worker surface now includes a manual IMDb-backfill task to repair previously persisted items that entered the scrape path without `attributes.imdb_id`.
- A shared serving substrate now backs direct-file streaming, partial HLS flows, remote proxying, and serving-runtime visibility under `/api/v1/stream/status`.
- The first general observability layer is now live across HTTP routes, ARQ stages, cache activity, and plugin loading/hook execution through [`../filmu_py/api/router.py`](../filmu_py/api/router.py), [`../filmu_py/workers/retry.py`](../filmu_py/workers/retry.py), [`../filmu_py/core/cache.py`](../filmu_py/core/cache.py), [`../filmu_py/plugins/loader.py`](../filmu_py/plugins/loader.py), and [`../filmu_py/plugins/hooks.py`](../filmu_py/plugins/hooks.py).
- Log observability now follows the same dual-surface strategy used elsewhere in the stack: the compatibility surfaces at [`/api/v1/logs`](../filmu_py/api/routes/default.py) and the existing SSE logging stream remain stable for the current frontend, while the GraphQL [`logStream`](../filmu_py/graphql/schema.py) subscription is the intentional richer surface for future consumers. Both surfaces read from the same bounded in-memory log broker in [`../filmu_py/core/log_stream.py`](../filmu_py/core/log_stream.py) rather than duplicating logging infrastructure.
- A proto-first Rust-sidecar catalog contract now lives at [`proto/filmuvfs/catalog/v1/catalog.proto`](../proto/filmuvfs/catalog/v1/catalog.proto), and the Python-side catalog supplier/runtime now lives at [`filmu_py/services/vfs_catalog.py`](../filmu_py/services/vfs_catalog.py) plus [`filmu_py/services/vfs_server.py`](../filmu_py/services/vfs_server.py) with generation-aware reconnect deltas and `RefreshCatalogEntry` support.
- A Rust runtime now also exists at [`rust/filmuvfs/Cargo.toml`](../rust/filmuvfs/Cargo.toml), consuming the catalog contract through generated `tonic` bindings and feeding the mounted Unix-only `fuse3` path.
- A first Rust mount-facing lifecycle layer now also exists in [`rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs), covering catalog-backed `getattr`, `readdir`, `open`, `read`, and `release` semantics, and those operations are now exercised through real WSL/Linux mounted execution.
- The Rust sidecar now also hardens mounted reads with inline stale-link refresh, async `moka::future` chunk caching, and stable-assigned inode fallback on collisions in [`../rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs), [`../rust/filmuvfs/src/chunk_engine.rs`](../rust/filmuvfs/src/chunk_engine.rs), and [`../rust/filmuvfs/src/catalog/state.rs`](../rust/filmuvfs/src/catalog/state.rs).
- A Unix-only `fuse3` adapter and mount bootstrap seam now also exist in [`rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs), including a stable inode model over the catalog hierarchy; automated WSL/Linux lifecycle validation, manual mount/read smoke, and Plex/Emby playback validation now pass, while the next VFS frontier is longer-running data-plane hardening, mounted observability, rollout controls, and HTTP/VFS semantic convergence.

## Reordered implementation plan (updated)

1. GraphQL parity core (Phase A) and plugin runtime boundaries (Phase B) run in parallel.
2. REST/SSE compatibility surface (Phase C).
3. ARQ hardening for deterministic short/medium workflow execution (Phase D1).
4. FilmuVFS-first stream platform with shared HTTP compatibility path under `/api/v1/stream/*` (Phase E).
5. Durable orchestration bridge (Temporal/equivalent) as conditional Phase D2.

Reference sequencing and decision gates are maintained in [`EXECUTION_PLAN.md`](EXECUTION_PLAN.md).

Supporting documents:

- [`AUTH.md`](AUTH.md)
- [`LOCAL_FRONTEND_TESTING_READINESS.md`](LOCAL_FRONTEND_TESTING_READINESS.md)
- [`HYBRID_EVENT_BACKPLANE_RESEARCH.md`](HYBRID_EVENT_BACKPLANE_RESEARCH.md)

## Design principles

- Keep request/response compatibility first.
- Favor idempotent operations.
- Make external provider calls rate-aware and retry-safe.
- Keep optional integrations failure-tolerant at boot.
- Prefer explicit manifest-declared plugin exports over implicit host introspection.
- Keep compatibility payloads structured and evolvable so the backend can serve the current frontend while leaving room for a better future frontend.
- Treat FilmuVFS as a core product capability for Plex-like clients, with HTTP streaming as a companion surface rather than the primary design center.

## TS backend audit reference

See [`RIVEN_TS_AUDIT.md`](RIVEN_TS_AUDIT.md) for the March-April 2026 TypeScript-backend audit findings and the explicit "how Filmu should exceed `riven-ts`" direction.
