# Filmu Enterprise-Grade Audit — 2026-04-09

## Scope

This audit updates the current Filmu baseline against:

- the current local FilmuCore codebase
- the current local `riven-ts` checkout at `E:\Dev\Filmu\.tmp-riven-ts-current`
- the current upstream `rivenmedia/riven-ts` GitHub repository and recent `main` commits:
  - [`e64604a`](https://github.com/rivenmedia/riven-ts/commit/e64604af46d95d6afd0f51dd29ed411d47ab9c35) `chore: modify in place in reducers (#70)`
  - [`8a5dd00`](https://github.com/rivenmedia/riven-ts/commit/8a5dd00ddc5cea75de39c0e7cccfe78fc2c9c7c9) `feat(core): use worker threads for CPU-intensive jobs (#69)`
  - [`bceeb9e`](https://github.com/rivenmedia/riven-ts/commit/bceeb9ebe71b9f4a5d9a96a0cef58e3ac1d1f2aa) `chore: improve scrape performance (#68)`

The result is not just a parity note. It is a direction-setting audit for building Filmu past `riven-ts`.

## Strategic bar

Filmu should be treated as an **enterprise-grade, state-of-the-art media orchestration and delivery platform** across every major concern:

- identity and access
- tenancy and policy
- orchestration and recovery
- plugin trust and isolation
- observability and operability
- data correctness and schema evolution
- playback and VFS data-plane behavior
- release engineering, validation, and disaster recovery

Matching current `riven-ts` is the floor.
Surpassing it requires Filmu to be stronger not only in VFS architecture, but also in platform governance, operations, and safety.

## Where Filmu is already strong

Filmu already has real platform depth in areas where it can plausibly surpass `riven-ts`:

- a Python backend plus Rust FilmuVFS sidecar split instead of a single-process Node/FUSE runtime
- native Windows and Linux mount adapters in the Rust sidecar
- a real shared playback substrate, playback proof scripts, and native Windows proof coverage
- a dual-surface API strategy with frozen REST compatibility and richer GraphQL growth
- a real plugin runtime baseline with manifests, capabilities, hooks, settings, and built-in integrations
- a first meaningful observability layer across routes, workers, cache, plugins, and VFS telemetry

Those are real advantages. They are not yet enough to call the overall system enterprise-grade.

## Underdocumented gaps that must now be first-class

### 1. Enterprise identity, tenancy, and authorization are still missing

Current code still enforces one backend API key in [`filmu_py/api/deps.py`](../filmu_py/api/deps.py), accepting `x-api-key`, bearer token, or query parameter forms of the same shared secret.

That is enough for local compatibility.
It is not enough for enterprise-grade operation.

Missing first-class work:

- multi-user identity
- service-to-service identity
- RBAC / ABAC
- tenant isolation
- admin/operator separation
- audit logs for privileged actions
- SSO / OIDC / SAML readiness
- API key rotation policy with actor attribution

This is only lightly implied in current docs and is not treated as a top-level program track.

### 2. Multi-node control-plane semantics are not established

Filmu still uses process-local primitives for important runtime surfaces:

- [`filmu_py/core/event_bus.py`](../filmu_py/core/event_bus.py) is explicitly in-memory and process-local
- [`filmu_py/core/log_stream.py`](../filmu_py/core/log_stream.py) is in-memory and bounded per process

That is reasonable for the current single-node shape.
It means Filmu does not yet define enterprise behavior for:

- multi-node event fan-out
- cross-node subscription delivery
- active/active API replicas
- worker/API/VFS coordination across nodes
- failover and split-brain handling
- replayable operational event streams

Current docs talk about observability and orchestration, but they do not elevate this to a top-level availability and scale requirement.

### 2a. The operator log pipeline is still below current upstream `riven-ts`

Current upstream `riven-ts` now has a materially stronger logging stack:

- Winston transports
- ECS-formatted `ecs.json`
- durable file outputs
- Sentry trace/span enrichment
- source and worker tagging
- Filebeat shipping into local Elasticsearch/Kibana

Filmu currently has useful structured logs and a compatibility-friendly bounded in-memory broker, but it does not yet match that operator story.

Missing first-class work:

- durable structured log files outside process memory
- retention and rotation policy
- shipper-friendly structured format
- trace/span correlation fields embedded in log events
- local and production log shipping/search workflow

This gap matters both for enterprise operations and for honest comparison against current `riven-ts`.

### 3. Plugin trust is ahead of parity, but still below enterprise

The plugin manifest and loader are intentionally safe-by-default:

- [`filmu_py/plugins/manifest.py`](../filmu_py/plugins/manifest.py)
- [`filmu_py/plugins/loader.py`](../filmu_py/plugins/loader.py)

Current protections are still mostly:

- shape validation
- export-symbol validation
- host-version minimum checks
- safe registration / skip behavior

Missing enterprise plugin controls:

- signed plugins and provenance verification
- capability permission scopes
- network / filesystem / process isolation
- sandbox execution for untrusted extensions
- policy enforcement before load
- plugin revocation and quarantine
- compatibility certification and release channels

This gap is larger than the current docs imply.

### 4. Heavy-job isolation is still behind current upstream `riven-ts`

Current upstream `riven-ts` `main` now explicitly runs heavy work through sandboxed or worker-thread style execution for:

- `scrape-item.parse-scrape-results`
- `download-item.map-items-to-files`
- `download-item.validate-torrent-files`

Filmu has a real ARQ stage graph in [`filmu_py/workers/tasks.py`](../filmu_py/workers/tasks.py), but it still does not provide an equivalent isolated execution model for CPU-heavy or high-risk parsing/validation stages.

That matters for:

- tail-latency control
- noisy-neighbor containment
- crash isolation
- bounded memory growth
- future multi-tenant operation

This should now be treated as an enterprise requirement, not just a parity nicety.

### 5. Filmu lacks an explicit SRE / operations program

The repository has strong local proof scripts and validation work.
It still does not document an enterprise operations bar for:

- SLOs and error budgets
- backup and restore
- disaster recovery
- migration rollback policy
- capacity planning
- controlled rollouts / canaries
- chaos testing
- load testing targets
- incident response and operator runbooks

Current docs mention observability and quality, but not an overall production operations program.

### 6. FilmuVFS is strategically strong but not yet enterprise-complete

The Rust sidecar now has meaningful read-path maturity across:

- `lookup`
- `getattr`
- `open`
- `read`
- `release`
- `opendir`
- `readdir`
- `readdirplus`
- `releasedir`
- `statfs`

See [`rust/filmuvfs/src/mount.rs`](../rust/filmuvfs/src/mount.rs).

That is a good baseline.
It is still not the same thing as a fully enterprise-grade filesystem delivery plane.

Remaining platform-class work includes:

- broader filesystem semantics and compatibility expectations
- explicit rollout and downgrade strategy per mount backend
- sustained soak and fault-injection coverage
- cache correctness under pressure
- stronger multi-reader fairness and backpressure policy
- better control-plane/data-plane correlation
- operational contracts for upgrades, reconnects, and recovery under real load

### 7. Metadata/index governance is still below the final target

Filmu has request-time enrichment, backfills, partial show handling, ongoing polling, and content-service polling.
That is materially better than a thin compatibility shell.

What is still not framed strongly enough in the docs:

- dedicated indexing as a first-class pipeline
- reindex scheduling and replay
- canonical-identifier reconciliation policy
- source-confidence scoring
- provenance tracking across metadata mutations
- large-library repair tooling
- operator-visible drift detection between upstream metadata providers and persisted rows

This is one of the areas where Filmu should exceed `riven-ts`, not merely imitate it.

## Updated conclusion

Filmu is no longer a thin compatibility experiment.
It already contains real architectural strengths, especially around the Rust sidecar, Windows support, playback proofing, and API separation.

The current strategic risk is different:

- too much of the remaining roadmap is still framed as parity or local hardening
- not enough is framed as enterprise platform governance

From this point forward, Filmu docs and TODOs should treat the following as mandatory top-level workstreams:

1. enterprise identity / tenancy / authorization
2. distributed control-plane and HA semantics
3. plugin trust, policy, and isolation
4. heavy-stage sandboxing and workload isolation
5. SRE, disaster recovery, and release governance
6. enterprise VFS/data-plane maturity
7. metadata/index governance and replayability

That is the path to surpassing current `riven-ts` status instead of only catching up to it.
