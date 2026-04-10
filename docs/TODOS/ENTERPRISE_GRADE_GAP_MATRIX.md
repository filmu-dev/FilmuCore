# Enterprise-Grade Gap Matrix

## Purpose

This matrix captures the important gaps that are either missing from the current TODO set or not yet elevated enough.

It exists to make one rule explicit:

**Filmu should be enterprise-grade and state-of-the-art across all major areas, not only feature-parity complete with `riven-ts`.**

See also:

- [`../ENTERPRISE_GRADE_AUDIT_2026_04_09.md`](../ENTERPRISE_GRADE_AUDIT_2026_04_09.md)
- [`NEXT_IMPLEMENTATION_PRIORITIES.md`](NEXT_IMPLEMENTATION_PRIORITIES.md)
- [`OBSERVABILITY_MATURITY_MATRIX.md`](OBSERVABILITY_MATURITY_MATRIX.md)
- [`FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md`](FILMUVFS_BYTE_SERVING_PLATFORM_MATRIX.md)

## Matrix

| Workstream | Current evidence | Gap | Why it matters | Priority |
| --- | --- | --- | --- | --- |
| Enterprise identity and authz | [`filmu_py/api/deps.py`](../../filmu_py/api/deps.py) now carries additive actor/tenant/role/scope request context with no implicit admin fallback, computes `effective_permissions`, persists delegated tenant scope plus OIDC-ready identity fields (`authorized_tenant_ids`, `authorization_tenant_scope`, `oidc_issuer`, `oidc_subject`), [`filmu_py/authz.py`](../../filmu_py/authz.py) now evaluates tenant-aware authorization decisions, [`filmu_py/services/identity.py`](../../filmu_py/services/identity.py) persists tenant/principal/service-account inventory, actor-aware API-key rotation now also rotates `api_key_id`, [`GET /api/v1/auth/context`](../../filmu_py/api/routes/default.py) exposes the resolved actor mapping, tenant scope, and effective permissions, and [`GET /api/v1/auth/policy`](../../filmu_py/api/routes/default.py) exposes standard authorization probes plus warnings/gaps, but authentication is still anchored on one shared API key | Add actual OIDC/SSO integration, deeper RBAC/ABAC policy enforcement across more resources, and persisted operator-visible access policy configuration | Enterprise-grade control plane requires traceable identity and policy, not one shared backend secret plus additive headers | P0 |
| Tenancy model | [`TenantORM`](../../filmu_py/db/models.py) now exists, `tenant_id` ownership covers the main `media_items` and `item_requests` lifecycle carriers, tenant-scoped authorization now reaches item/calendar/stats routes, worker context binds tenant ownership, and plugin load visibility now exposes tenancy posture, but quotas, plugin execution, VFS governance, and metrics ownership are still not fully tenant-aware | Define tenant boundary, tenant-aware quota policy, plugin/VFS/control-plane attribution, and tenant-scoped observability | Surpassing `riven-ts` means Filmu can safely scale beyond single-operator deployment assumptions | P0 |
| Distributed control plane | [`filmu_py/core/event_bus.py`](../../filmu_py/core/event_bus.py) and [`filmu_py/core/log_stream.py`](../../filmu_py/core/log_stream.py) are process-local/in-memory | Add cross-node eventing, replayable streams, subscription durability, node coordination, failover semantics, and HA readiness | Single-process signaling is a ceiling on scale, availability, and operator confidence | P0 |
| SRE / production operations program | Strong local proof scripts exist, but no top-level SLO/DR program is documented | Define SLOs, error budgets, rollback rules, backup/restore, DR objectives, incident runbooks, canary/rollout policy, and capacity review cadence | Enterprise-grade systems are judged by operability under failure, not only by feature breadth | P0 |
| Durable operator log pipeline | [`filmu_py/logging.py`](../../filmu_py/logging.py) now provides rotating ECS/NDJSON-style file output plus the bounded in-memory broker in [`filmu_py/core/log_stream.py`](../../filmu_py/core/log_stream.py), and `/api/v1/workers/queue/history` now exposes bounded queue-history summaries instead of raw samples only | Add shipper/search workflow, broader trace/span adoption, and documented export/search operations comparable to the current `riven-ts` ECS + Filebeat + Elastic stack | Enterprise operations require searchable durable logs, not only live process-local compatibility streams | P0 |
| Plugin trust and isolation | [`filmu_py/plugins/manifest.py`](../../filmu_py/plugins/manifest.py), [`filmu_py/plugins/loader.py`](../../filmu_py/plugins/loader.py), and [`filmu_py/plugins/trust.py`](../../filmu_py/plugins/trust.py) now provide host-version/policy validation plus provenance metadata, digest validation, trust-store-backed signature verification, publisher lifecycle/distribution/scope policy enforcement, minimum-trust checks, tenancy-mode governance, sandbox posture, and quarantine refusal, while `/api/v1/plugins` surfaces publisher-policy/quarantine state and `/api/v1/plugins/governance` summarizes signature, publisher-policy, sandbox, tenancy, readiness, and recommended operator-action posture | Add stronger runtime sandboxing, richer artifact provenance/distribution controls, and operator quarantine/revocation workflows | Plugin extensibility becomes a liability without a trust model | P1 |
| Heavy-stage workload isolation | [`filmu_py/workers/tasks.py`](../../filmu_py/workers/tasks.py) has real stages, but no `riven-ts`-style worker-thread/sandboxed heavy jobs | Split parse/map/validate/index-style heavy work into isolated workers or sandboxes with bounded CPU/memory and crash containment | Current upstream `riven-ts` raised the baseline here; Filmu should exceed it, not trail it | P1 |
| Metadata/index governance | Request-time enrichment and repair exist, but indexing is not yet a first-class program track | Add dedicated index pipeline, reindex scheduling, provenance, confidence scoring, reconciliation audits, and drift tooling | Metadata correctness is a platform capability, not a request-time convenience | P1 |
| VFS enterprise data-plane | [`rust/filmuvfs/src/mount.rs`](../../rust/filmuvfs/src/mount.rs), [`rust/filmuvfs/src/prefetch.rs`](../../rust/filmuvfs/src/prefetch.rs), and [`rust/filmuvfs/src/telemetry.rs`](../../rust/filmuvfs/src/telemetry.rs) now cover core read-path operations, adapters, a per-handle prefetch fairness cap, and explicit fairness/global-backpressure denial counters surfaced through [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py), which now also derives cache/fallback/prefetch pressure ratios plus `vfs_runtime_rollout_readiness`, `vfs_runtime_rollout_reasons`, and `vfs_runtime_rollout_next_action` | Add stronger soak/chaos coverage, rollout policy per backend, broader fairness/backpressure budgets, cache correctness guarantees, and richer operator controls | FilmuVFS is the clearest chance to beat `riven-ts`; it needs platform-grade operational maturity | P1 |
| Release engineering and supply chain | [`../../.github/workflows/release.yml`](../../.github/workflows/release.yml) now defaults to read-only workflow permissions, scopes `actions: write` to the release-PR verify redispatch job, and explicitly re-triggers verify on open release-please PR branches so required checks do not stall, but artifact-level provenance and promotion policy are still incomplete | Add SBOM/signing policy, dependency risk review, release promotion stages, artifact provenance, and upgrade/downgrade contracts | State-of-the-art systems treat delivery trust as part of product quality | P1 |
| Performance and chaos discipline | Bench/perf commands exist, but there is no explicit benchmark/chaos program | Define latency budgets, load-test profiles, chaos scenarios, regression thresholds, and nightly benchmark comparisons against prior builds and upstream behavior | “Fast enough locally” is not an enterprise or state-of-the-art bar | P1 |

## Immediate actions

1. Continue the new effective-permission, tenant-aware authorization, delegated tenant scope, OIDC-ready identity shape, actor-aware key-rotation, and tenant-persistence baseline into a real enterprise identity/tenancy/authz platform rather than treating the current header-carried context as the end state.
2. Add a distributed control-plane plan instead of leaving eventing/logging as a known single-process detail.
3. Promote plugin trust/isolation and heavy-job sandboxing into the near-term roadmap.
4. Add an SRE/DR/release-governance track so Filmu quality is measured operationally as well as functionally.
5. Keep VFS work focused on enterprise data-plane maturity, because that is where Filmu can most clearly surpass `riven-ts`.

## Success condition

This matrix is complete only when Filmu can honestly claim all of the following:

- feature breadth at or above current `riven-ts`
- stronger platform governance than current `riven-ts`
- stronger playback/VFS architecture than current `riven-ts`
- enterprise-grade operations, security, and recovery posture
- state-of-the-art engineering discipline across backend, orchestration, plugins, and data plane
