# filmu-python — Extensive Execution Plan

## 1) Scope lock (no assumptions)

This plan is constrained to currently documented state in:

- [`docs/STATUS.md`](docs/STATUS.md)
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/ORCHESTRATION.md`](docs/ORCHESTRATION.md)
- [`docs/VFS.md`](docs/VFS.md)
- [`docs/AUTH.md`](docs/AUTH.md)
- [`docs/LOCAL_FRONTEND_TESTING_READINESS.md`](docs/LOCAL_FRONTEND_TESTING_READINESS.md)
- [`docs/HYBRID_EVENT_BACKPLANE_RESEARCH.md`](docs/HYBRID_EVENT_BACKPLANE_RESEARCH.md)

## Detailed planning artifacts

For the current detailed work breakdowns, use:

- [`STATUS.md`](STATUS.md) - current top-level priority and posture reference
- [`ARCHITECTURE.md`](ARCHITECTURE.md) - platform quality bar and active architectural gaps
- [`DOMAIN_MODEL_EXPANSION_MATRIX.md`](TODOS/DOMAIN_MODEL_EXPANSION_MATRIX.md) - domain-backing gaps for compatibility routes
- [`ORCHESTRATION_BREADTH_MATRIX.md`](TODOS/ORCHESTRATION_BREADTH_MATRIX.md) - orchestration/decomposition gaps
- [`PLUGIN_CAPABILITY_MODEL_MATRIX.md`](TODOS/PLUGIN_CAPABILITY_MODEL_MATRIX.md) - plugin/runtime capability gaps
- [`FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md`](TODOS/FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md) - stream/VFS platform work
- [`OBSERVABILITY_MATURITY_MATRIX.md`](TODOS/OBSERVABILITY_MATURITY_MATRIX.md) - observability growth plan

## These planning artifacts are subordinate to this execution plan and should stay aligned with [`STATUS.md`](STATUS.md) and [`LOCAL_FRONTEND_TESTING_READINESS.md`](LOCAL_FRONTEND_TESTING_READINESS.md).

## Canonical status map

Use the docs below as the current source of truth for done/partial/missing state:

- [`STATUS.md`](STATUS.md) - top-level current posture and cross-cutting "done vs still missing" summary
- compatibility-route breadth is complete enough that it is no longer an active planning track; current playback maturity now lives in the playback and VFS docs
- [`TODOS/DOMAIN_MODEL_EXPANSION_MATRIX.md`](TODOS/DOMAIN_MODEL_EXPANSION_MATRIX.md) - entity and projection depth; entity layer is effectively done, read-model deepening remains
- [`TODOS/ORCHESTRATION_BREADTH_MATRIX.md`](TODOS/ORCHESTRATION_BREADTH_MATRIX.md) - worker graph and recovery breadth; Wave 3 is repo-closed and remaining work is operational soak/proof plus future platform deepening
- [`TODOS/PLUGIN_CAPABILITY_MODEL_MATRIX.md`](TODOS/PLUGIN_CAPABILITY_MODEL_MATRIX.md) - plugin/runtime capability depth; Wave 4 is repo-closed and remaining work is recurring rollout evidence plus broader future plugin breadth
- [`TODOS/FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md`](TODOS/FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md) - VFS/runtime maturity; mounted path is real, rollout/telemetry/canary hardening remains
- [`TODOS/OBSERVABILITY_MATURITY_MATRIX.md`](TODOS/OBSERVABILITY_MATURITY_MATRIX.md) - observability maturity; local reference stack is done, environment rollout and deeper correlation remain
- [`TODOS/PLAYBACK_PROOF_IMPLEMENTATION_PLAN.md`](TODOS/PLAYBACK_PROOF_IMPLEMENTATION_PLAN.md) - playback proof and gate posture; proof baseline is done, live GitHub policy validation and repeated soak hardening remain

Historical audits and dated comparison notes remain useful context, but they are not the canonical current-state checklist unless a current-state doc explicitly points to them.

No undocumented requirements are assumed.

---

## 2) Audit outcomes applied in this revision

1. Phase-ordering inconsistency removed: plugin runtime boundaries now run alongside GraphQL parity, not after REST/SSE.
2. Queue/orchestration scope split into two phases:
   - **D1:** ARQ hardening (immediate, low risk)
   - **D2:** Temporal durable bridge (conditional, higher-risk)
3. Added explicit persistence/schema-evolution policy across all phases.
4. Quality/security/perf hardening is now a cross-cutting track, not a final catch-all phase.
5. Strategic platform parity remains GraphQL-first, but re-audit of the current frontend shows local frontend enablement is primarily blocked by REST/SSE breadth.

---

## 3) Program objectives

1. Make the backend a strong execution motor for the frontend, not a UI-shaped monolith.
2. Reach practical parity with upstream contracts (GraphQL + `/api/v1/*` + stream behavior) needed for the current frontend to function locally.
3. Improve reliability incrementally (ARQ first, durable orchestration bridge second).
4. Keep schema evolution safe and reversible via explicit migration policy.
5. Enforce local + CI quality gates continuously per phase.
6. Build Filmu to be enterprise-grade and state-of-the-art across identity, security, operations, plugins, orchestration, and VFS, not merely parity-complete with current `riven-ts`.

### Architectural stance

- The backend is the **motor** that powers frontend capabilities such as requesting, discovery/exploration, and streaming.
- Frontend UX can evolve independently, so backend contracts should remain stable, explicit, and reusable across the current frontend and better future frontends.
- Streaming/VFS decisions must preserve this separation: backend streaming primitives and FilmuVFS should be reusable by multiple frontend experiences, not tailored to one current UI.

---

## 4) Delivery phases

## Phase A — GraphQL parity completion (frontend-unblock first)

### Deliverables

- Resolver families needed for frontend-unblock domains (settings/content/profile/core query/mutation/subscription).
- Stable GraphQL naming and error contract.

### Work packages

1. Expand core query/mutation/subscription operations.
2. Split resolvers by bounded context.
3. Add contract validation and operation-level tests.
4. Version/schema snapshot governance for compatibility checks.

### Current baseline delivered

- Strawberry GraphQL router and schema wiring are in place.
- Core `settings { filmu { version apiKey logLevel } }` parity is implemented.
- Plugin-contributed nested `settings { ... }` fields are supported.
- Core item query/mutation flow now exists for list/get/request/action behavior.
- Compatibility GraphQL subscriptions now exist for item-state changes, notifications, and structured log streaming.

### Main missing pieces

- Broader upstream resolver families beyond core settings/items.
- Contract snapshot governance.
- Wider platform parity across content/profile domains.

### Exit criteria

- Required frontend-unblock GraphQL operations implemented.
- Schema diff is intentional and documented.
- Phase quality gates pass (Section 6).

### Re-audit note

- GraphQL remains the strategic parity track with `filmu-ts`, but it is not the main blocker for testing the current REST/BFF-driven frontend locally.
- The shared media-domain seam is now deeper across graph, mounted-read, and the key REST compatibility paths: specialization-backed service projections feed GraphQL calendar/detail consumers directly, the GraphQL `items` list now reuses `search_items()` instead of flattening from raw records, the FilmuVFS catalog supplier now prefers specialization-backed hierarchy over metadata-first path shaping, REST item-detail season coverage now prefers persisted `Show -> Season` rows, and the extended compatibility `metadata` blob is normalized from the same specialization record. The remaining domain gap is now the narrower compatibility tail.

---

## Phase B — Plugin runtime boundaries (runs in parallel with Phase A)

### Deliverables

- Plugin discovery + manifest validation + safe loading lifecycle.
- Resolver contribution pipeline with isolation/failure containment.
- Plugin datasource/context injection and hook-worker capability boundaries.

### Work packages

1. Implement entrypoint discovery and manifest checks.
2. Define host capability boundary for plugin context injection.
3. Define datasource injection and plugin GraphQL/runtime context composition explicitly.
4. Add plugin hook-worker registration model and publishable-event governance.
5. Isolate failures (invalid plugin cannot block startup).
6. Emit plugin telemetry for load and registration outcomes.

### Current baseline delivered

- Filesystem plugin discovery is implemented.
- Packaged entry-point discovery is implemented.
- Manifest validation and safe module loading are implemented.
- Resolver contribution for query/settings extensions is implemented.
- Plugin-scoped settings registry, datasource-aware context injection, and runtime capability registration beyond GraphQL are implemented.
- Typed event hook execution, namespaced publishable-event governance, built-in capability registration, and runtime event visibility are implemented.
- First plugin load and hook telemetry is implemented.
- Invalid plugin skip behavior is implemented and tested.

### Main missing pieces

- Stronger compatibility/version policy and manifest/schema validation beyond the current minimal fields.
- Richer external-author packaging/distribution guidance.
- Deeper compatibility/version policy, runtime isolation, and operator health around the now-real MDBList/StremThru/notification integrations.
- Decide whether durable queue-backed hook execution is required beyond the current in-process executor.

### Exit criteria

- Deterministic plugin discovery and resolver registration.
- Invalid plugin skip behavior is tested and observable.
- **Dependency gate:** Phase A cannot be marked fully stable until B resolver-boundary contract is complete.
- Phase quality gates pass (Section 6).

---

## Phase C — REST/SSE compatibility surface

### Deliverables

- `/api/v1/*` compatibility routes beyond root/health.
- SSE endpoints aligned with frontend reconnect/error expectations.

### Work packages

1. Implement missing compatibility routers + response models.
2. Add SSE contract behavior (event names, reconnect semantics, fail-safe handling, bounded history where needed).
3. Enforce OpenAPI operation-id consistency.

### Current baseline delivered

- `/api/v1/settings/*` compatibility baseline is implemented.
- the full original riven settings schema is now modeled in the typed runtime settings layer, with explicit translation through [`Settings.to_compatibility_dict()`](../filmu_py/config.py) and [`Settings.from_compatibility_dict()`](../filmu_py/config.py)
- persisted settings storage is now in place with startup hydration into the runtime settings object, so `/api/v1/settings` reads come from the active in-memory instance while writes persist the full compatibility blob for future boots
- `/api/v1/logs` bounded historical log endpoint is in place for the current frontend log view.
- `/api/v1/stream/event_types` and `/api/v1/stream/{event_type}` are in place for compatibility SSE topics.
- Logs and notifications now have a minimal compatibility path that is intentionally simple and extensible.
- Phase C core breadth is effectively delivered for dashboard/library/calendar/items/scrape/watch-alias compatibility; the remaining practical gaps are playback routes and deeper orchestration behind those surfaces.
- Local frontend-readiness thresholds and current blockers are tracked in [`LOCAL_FRONTEND_TESTING_READINESS.md`](LOCAL_FRONTEND_TESTING_READINESS.md).

### Settings modeling note

- schema modeling is now complete for the current compatibility surface, but persisted mutation/runtime apply remains a later step
- compatibility REST should continue returning the exact original `settings.json` shape, while future GraphQL/product work should build on the typed internal settings model instead of reusing compatibility dicts as the domain model

### Settings persistence note

- settings persistence is now deliberately single-row and compatibility-blob based rather than field-by-field normalized storage
- startup hydrates the persisted compatibility payload into the runtime [`Settings`](../filmu_py/config.py) object when a row exists and otherwise falls back cleanly to environment defaults
- worker-side runtime resolution now prefers the persisted compatibility blob when no explicit settings instance has been injected into the job context

### Re-audit note

- For the current frontend, Phase C breadth is no longer the main practical blocker; the frontier has moved to playback hardening and the FilmuVFS-serving path.

### Local frontend-readiness reality check

- Threshold B is effectively reached: the current Python backend can now support meaningful local frontend + backend testing for dashboard, library, calendar, settings, logs/notifications, scrape compatibility, and the legacy watch alias.
- The main remaining local-frontend blocker is no longer broad Phase C route coverage; it is playback hardening across direct-file resolution, HLS lifecycle governance, and end-to-end player/BFF validation.
- [`LOCAL_FRONTEND_TESTING_READINESS.md`](LOCAL_FRONTEND_TESTING_READINESS.md) is the authoritative source of truth for local frontend route coverage, testing thresholds, and current blockers. Other documents should reference it rather than duplicate route inventories.

### Exit criteria

- Frontend compatibility calls succeed without contract drift.
- Operation-id and response-shape checks pass.
- Phase quality gates pass (Section 6).

---

## Phase D1 — ARQ hardening (short/medium-lived jobs)

### Deliverables

- Deterministic ARQ retry/idempotency behavior.
- DLQ and replay-safe worker semantics.

### Work packages

1. Per-stage retry policy classes.
2. Idempotency keys and dedup boundaries.
3. DLQ routing + triage metadata.
4. Transactional outbox for state/event publication consistency.
5. Capture the original TS queue breadth explicitly in Python planning:
   - content-service intake
   - index
   - scrape + parse
   - ranking + container selection
   - download fan-out
   - retry-library recovery

### Current baseline delivered

- Retry/dead-letter baseline exists for the current worker graph.
- Scrape -> parse-scrape-results -> rank-streams -> debrid -> finalize now runs as a real provider-backed worker pipeline with stable job IDs and persisted state transitions.
- Retry-library recovery and transactional outbox publication are implemented.
- A first-class scheduled metadata reindex/reconciliation program now runs above `index_item`, including index re-entry for `partially_completed` / `ongoing` items, metadata refresh reconciliation for `completed` items, repair of identifier gaps on repairable `failed` items with immediate re-entry into `index_item`, and bounded operator rollups on `/api/v1/workers/metadata-reindex` plus `/api/v1/workers/metadata-reindex/history`.
- Heavy-stage isolation now enforces a stricter enterprise baseline: spawn-required process-backed execution, bounded worker ceiling, recycle budget validation, and explicit policy-violation reporting on `/api/v1/stream/status` plus `/api/v1/operations/governance`.
- Workers now resolve persisted runtime settings and execute real built-in scraper and downloader provider paths.
- Worker observability baseline now exists through stage duration, retry, and dead-letter metrics plus correlation contextvars, and queue-history operator surfaces now include dead-letter age/reason rollups plus bounded filters for replay triage.

### Main missing pieces

- Stronger stage-idempotency and enqueue-dedup boundaries across the broader queue graph.
- Content-service intake and richer queue graph parity beyond the current scrape/download plus scheduled-reindex baseline.
- Queue lag/backlog/operator ergonomics beyond the new dead-letter age/reason rollups and bounded queue-history controls.
- Broader post-download/container execution and compensation semantics beyond the current `downloaded` handoff.
- Broader worker/database isolation and sandboxed heavy-job families beyond the current spawn-required worker-ceiling/recycle baseline.

### Exit criteria

- Retries are deterministic.
- Duplicate execution does not corrupt state transitions.
- Phase quality gates pass (Section 6).

---

## Phase D2 — Durable orchestration bridge (conditional)

### Deliverables

- Temporal (or approved equivalent durable workflow engine) bridge for long-running recoverable workflows.

### Work packages

1. Workflow checkpoint model and replay-safe activities.
2. Compensation handlers for partial failures.
3. Restart/recovery integration tests.
4. Evaluate whether the durable event backbone should adopt the hybrid pattern documented in [`HYBRID_EVENT_BACKPLANE_RESEARCH.md`](HYBRID_EVENT_BACKPLANE_RESEARCH.md):
   - NATS JetStream as durable source of truth
   - Redis Streams as hot short-retention execution relay
   - FilmuVFS as a required control-plane constraint, never putting byte-serving reads onto the broker path

### Current status

- Not started.
- Architectural direction is documented, but implementation should remain conditional and deliberate.

### Exit criteria

- Long-running workflows survive restart/failure scenarios.
- Durability boundaries are documented and tested.
- Phase quality gates pass (Section 6).

---

## Phase E — VFS/stream compatibility

### Deliverables

- FilmuVFS mount worker as a first-class product path for Plex/Emby-style and filesystem-oriented consumers.
- `/api/v1/stream/*` HTTP compatibility path with range support for frontend and non-mount clients.
- HLS playlist/segment behavior with bounded ffmpeg concurrency.
- FilmuVFS is treated as a hard product constraint for both the current frontend and future better frontends, with the byte path kept direct and the event backbone limited to control-plane concerns.

### Work packages

1. FilmuVFS mount worker core (path model, open/read/readdir/release lifecycle).
2. Link resolver abstraction.
3. Shared byte-range/chunk engine used by both FilmuVFS and HTTP streaming (implemented in Python and now adopted in the live HTTP direct-play range path; mount adoption still remains).
4. Byte-range proxy with strict header behavior.
5. HLS playlist + segment pipeline.
6. Stream metrics and concurrency controls.
7. Canary controls for stream rollout.
8. Ensure any future hybrid event backplane integration follows the FilmuVFS constraint documented in [`HYBRID_EVENT_BACKPLANE_RESEARCH.md`](HYBRID_EVENT_BACKPLANE_RESEARCH.md).

### Current baseline delivered

- A real shared serving substrate exists in [`filmu_py/core/byte_streaming.py`](../filmu_py/core/byte_streaming.py) and is already reused by the current stream routes.
- A shared chunk engine now also exists in [`filmu_py/core/chunk_engine.py`](../filmu_py/core/chunk_engine.py), covering chunk geometry, 6-way read classification, in-memory chunk caching, ordered chunk resolution, validated upstream range fetch/stitch behavior, and dedicated Prometheus metrics.
- [`/api/v1/stream/file/{item_id}`](../filmu_py/api/routes/stream.py) has an explicit byte-range direct-play baseline with governed sessions and lease-refresh integration.
- [`/api/v1/stream/hls/{item_id}/*`](../filmu_py/api/routes/stream.py) has an implemented baseline for local generation, upstream proxying, and `remote-direct` transcode fallback with explicit lifecycle governance.
- Playback attachment/source resolution now also lives behind [`filmu_py/api/playback_resolution.py`](../filmu_py/api/playback_resolution.py) rather than remaining route-local.
- [`/api/v1/stream/status`](../filmu_py/api/routes/stream.py) exposes the current serving-runtime/governance state, including mounted cache/chunk-coalescing/upstream-wait/refresh pressure classes and machine-readable reasons derived from the Rust runtime snapshot.
- An async gRPC catalog bridge now exists in [`filmu_py/services/vfs_server.py`](../filmu_py/services/vfs_server.py), accepting subscribe/ack/heartbeat traffic, serving initial snapshots, serving reconnect deltas when possible, and exposing `RefreshCatalogEntry` for forced provider-link refresh.
- The Rust sidecar now has catalog client, in-memory state, and a mount-facing lifecycle layer with `getattr`/`readdir`/`open`/`read`/`release` behavior.
- Mounted stale reads now retry inline through the refresh RPC, the Rust chunk cache now uses `moka::future::Cache`, and catalog state now preserves stable assigned inodes with fallback allocation on collisions.
- The Python catalog supplier now also normalizes mounted show layout to `Show Title (Year)/Season XX/<sanitized source filename>`, emits removals when an existing catalog `entry_id` changes visible path, and now infers season placement for `S05x08`-style provider filenames on show-level media entries.
- Live validation on the mounted WSL path now confirms season-grouped output for real shows such as `Stranger Things (2016)` instead of a flat root-level file dump.
- Link resolution is implemented through built-in Real-Debrid / AllDebrid / Debrid-Link clients with persisted media-entry leases and provider-backed refresh orchestration.
- Linux-target compile validation, WSL/Linux mount lifecycle validation, manual mounted-read smoke, and Plex/Emby playback validation all pass for the current Rust sidecar path.
- Robust stream/VFS Prometheus metrics exist for the HTTP playback path, while mounted data-plane metrics now also feed operator-facing pressure classes on the API surfaces and the Windows soak artifacts. A first repo-level multi-environment gate now aggregates `soak-stability-*.json` across distinct environment classes; the remaining gap is real environment breadth and repeated evidence collection rather than missing aggregation code.

### Main missing pieces

- Stronger direct-file source/link resolution and broader HLS governance on top of the current substrate.
- Mount/HTTP convergence on the shared chunk engine semantics for mounted reads.
- Decide whether the current canonical-plus-alias mounted browse policy should stop here or grow into a fully separate id-keyed tree, and what broader queue-backed/orchestrated resolver workflow should exist above the current mount-side inline refresh dedup.
- Optional disk/persistent cache and smarter prefetch evolution above the now-async Rust cache.
- Broader long-running soak/backpressure validation and real multi-environment mounted data-plane breadth on top of the new aggregation gate.
- VFS rollout controls.

### Exit criteria

- FilmuVFS mount behavior is stable and performant for Plex-like consumers.
- Contract tests pass for range and HLS behavior.
- Stream reliability and failure telemetry are visible.
- Phase quality gates pass (Section 6).

---

## 5) Cross-cutting tracks (all phases)

## A) Persistence and migration policy

1. Every persistence-shape change requires an Alembic revision.
2. Revisions must include explicit upgrade/downgrade intent and compatibility notes.
3. Destructive migrations require guarded rollout strategy (feature flags/canary/backfill path).
4. Each phase must include schema-drift validation in its exit checks.

## B) Continuous hardening (not deferred to a final phase)

1. Contract tests are added as surfaces are delivered (GraphQL/REST/SSE/stream).
2. Security scans and performance checks run incrementally per touched area.
3. Reliability regressions block phase completion.

---

## 6) Mandatory quality gates (local + CI, per phase)

## Local developer gate (fast feedback)

Run before commit:

1. `pnpm run lint`
2. `pnpm run format:check`
3. For security/perf-sensitive changes, also run:
   - `pnpm run security:audit`
   - `pnpm run security:bandit`
   - `pnpm run perf:bench` (when performance paths are changed)

## CI gate (merge blocker)

All commands above are enforced in CI with phase-appropriate coverage.

If workspace-level `pnpm` install is blocked by native `fuse-native`/WinFsp constraints, equivalent app-scope `uv` checks are required and the exception must be documented in the phase report.

---

## 7) Risk register and mitigations

1. **Contract drift (GraphQL/REST/SSE):** snapshots + compatibility tests + operation-id governance.
2. **Workflow inconsistency (queue vs state):** idempotency keys + deterministic transitions + outbox pattern.
3. **Stream reliability (range/HLS/ffmpeg):** bounded concurrency + timeout budgets + canary rollout + metrics.
4. **Plugin safety:** strict manifest validation + failure isolation + minimal host capability surface.
5. **Schema evolution regressions:** mandatory migration policy + drift checks per phase.
6. **Backend/Frontend overcoupling:** keep backend contracts stable and UI-independent so the backend remains the motor for the current frontend and any future better frontend.
7. **Event-backplane misuse for FilmuVFS data path:** keep JetStream/Redis Streams on the control plane only; never insert the broker into direct byte-serving reads.

---

## 8) Milestone map (implementation-ready)

1. **A + B in parallel**
   - A delivers frontend-unblock GraphQL contract.
   - B locks plugin/resolver runtime boundary required for schema stability.
2. **C** (REST/SSE compatibility surface)
   - Delivered the route surface needed for meaningful local frontend + python-backend testing.
3. **D1** (ARQ hardening)
4. **E** (HTTP stream/VFS compatibility path)
5. **D2 (conditional)** durable orchestration bridge

Current execution emphasis:

- If the goal is immediate usefulness with the current frontend, advance Phase E playback hardening and VFS-serving work aggressively.
- Treat the normalized mounted show layout work as complete baseline rather than open scope; the next VFS work is deeper path semantics, mounted observability, and longer-running runtime hardening.
- If the goal is long-term platform parity with `filmu-ts`, continue Phase A/B in parallel rather than abandoning them.
- Keep D1 orchestration hardening moving so the serving/runtime path does not outpace control-plane reliability.

Continuous hardening and migration governance apply throughout, not as an end phase.

---

## 9) Decision gates (explicit confirmation policy)

1. **Temporal adoption timing (D2):** still open; run only when explicitly approved.
2. **VFS delivery mode:** resolved as FilmuVFS-first with HTTP compatibility alongside it; HTTP is complementary, not the substitute for the mount product path.
3. **Compatibility priority:** resolved as GraphQL-first for strategic platform parity, but REST/SSE breadth first for current-frontend local enablement.
4. **Native dependency CI policy:** strict-fail vs documented conditional exception must remain explicit per run.
5. **Lease field placement:** resolved as lease state living on persisted `media_entries` only; `active_streams` remain pure selectors pointing at leased entries rather than carrying their own lease metadata.
6. **Playback failure policy for failed leases:** resolved as fail-closed `503` behavior when the selected media-entry lease is in `failed` state or refresh cannot recover a fresh playable locator in time; do not silently degrade to a stale restricted fallback.
