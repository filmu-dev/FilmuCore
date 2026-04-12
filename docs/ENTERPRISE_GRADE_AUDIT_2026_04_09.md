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

## 2026-04-11 source re-audit note

Parts of this document remain historically useful, but some of its gap statements are now operationally stale.

- authz is no longer a missing future capability: current source now includes OIDC/JWKS validation, tenant auth context propagation, access-policy revisions, and `/api/v1/auth/policy`
- plugin governance is no longer only aspirational future work: current source now includes trust-store verification, quarantine and override controls, and `/api/v1/plugins/governance`
- the active `riven-ts` comparison baseline for current planning is upstream `main` at `f98cc31`
- use `RIVEN_TS_AUDIT.md` and `STATUS.md` as the current source-of-truth for present-state comparison; use this document mainly for strategic framing and historical rationale

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

### 1. Enterprise identity, tenancy, and authorization are no longer missing in-repo

Current source now has a real identity and authorization baseline rather than only one shared backend API key:

- validated OIDC/JWKS bearer-token support
- delegated tenant authorization context
- route and resource-scope ABAC constraints
- persisted access-policy revisions and approval flows
- durable authorization-decision audit retention/search
- actor-aware API-key rotation posture and alert candidates
- operator-facing `/api/v1/auth/policy`, `/api/v1/auth/policy/revisions`, and `/api/v1/auth/policy/audit`

The remaining work is operational rather than primitive-missing:

- environment-owned OIDC/SSO activation and subject rollout
- recurring operator evidence on real hosts
- deeper product-facing ownership semantics above the delegated-tenant baseline
- long-lived service-to-service rollout discipline and frontend session integration

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

### 2a. The operator log pipeline is no longer missing in-repo, but still trails in environment execution

Current upstream `riven-ts` now has a materially stronger logging stack:

- Winston transports
- ECS-formatted `ecs.json`
- durable file outputs
- Sentry trace/span enrichment
- source and worker tagging
- Filebeat shipping into local Elasticsearch/Kibana

Filmu no longer stops at useful structured logs and an in-memory broker. The repo now has structured log-shipping policy, cross-process trace-correlation policy, and governance exit gates for the operator log pipeline.

The remaining work is rollout and operations execution:

- recurring shipper/search deployment in real environments
- retention and alert-tuning discipline on top of the landed structured outputs
- externally hosted search/export and day-2 operator workflows
- deeper trace/span rollout coverage across all long-running environments

This gap matters both for enterprise operations and for honest comparison against current `riven-ts`.

### 3. Plugin trust is ahead of parity and now materially stronger in-repo

The plugin manifest and loader are intentionally safe-by-default:

- [`filmu_py/plugins/manifest.py`](../filmu_py/plugins/manifest.py)
- [`filmu_py/plugins/loader.py`](../filmu_py/plugins/loader.py)

Current protections now include:

- manifest and compatibility validation
- trust-store-backed signature verification
- provenance and source-digest checks
- quarantine, revocation, and operator override controls
- enforceable non-builtin runtime policy and governance visibility
- safe registration / skip behavior

Remaining work is narrower and more operational:

- recurring external-author and runtime evidence
- stricter deployment/runtime sandbox ceilings where environments require them
- broader plugin/package breadth without weakening the now-landed policy model
- compatibility certification and release-process maturity for external authors

### 4. Heavy-job isolation baseline now exists, but deeper sandboxing still trails current upstream `riven-ts`

Current upstream `riven-ts` `main` now explicitly runs heavy work through sandboxed or worker-thread style execution for:

- `scrape-item.parse-scrape-results`
- `download-item.map-items-to-files`
- `download-item.validate-torrent-files`

Filmu now has a real ARQ stage graph in [`filmu_py/workers/tasks.py`](../filmu_py/workers/tasks.py) and a Wave 3 baseline for bounded isolated execution on `index_item`, `parse_scrape_results`, and `rank_streams`.

That matters for:

- tail-latency control
- noisy-neighbor containment
- crash isolation
- bounded memory growth
- future multi-tenant operation

The remaining gap is stricter sandbox/process ceilings, broader heavy-job breadth, and real-environment evidence, not the absence of any heavy-stage isolation model.

### 5. Filmu now has an explicit SRE / operations program baseline

The repository no longer stops at local proof scripts. It now documents an enterprise operations bar, but still needs recurring execution evidence for:

- SLOs and error budgets
- backup and restore
- disaster recovery
- migration rollback policy
- capacity planning
- controlled rollouts / canaries
- chaos testing
- load testing targets
- incident response and operator runbooks

Current docs now include an overall production operations program; the remaining gap is disciplined execution and evidence retention in real environments.

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

### 7. Metadata/index governance is now materially stronger, but still below the final target

Filmu has request-time enrichment, backfills, partial show handling, ongoing polling, and content-service polling.
That is materially better than a thin compatibility shell.

What still remains beyond the now-landed dedicated `index_item` stage:

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
