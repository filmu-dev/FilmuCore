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
| Enterprise identity and authz | [`filmu_py/api/deps.py`](../../filmu_py/api/deps.py) still validates one shared API key | Add OIDC/SSO readiness, user/service identity, RBAC/ABAC, tenant scoping, privileged-action audit logs, actor-aware key rotation | Enterprise-grade control plane requires traceable identity and policy, not one shared backend secret | P0 |
| Tenancy model | No first-class tenant/org model across API, workers, plugins, VFS, or metrics | Define tenant boundary, tenant-aware resource ownership, quota policy, and tenant-scoped observability | Surpassing `riven-ts` means Filmu can safely scale beyond single-operator deployment assumptions | P0 |
| Distributed control plane | [`filmu_py/core/event_bus.py`](../../filmu_py/core/event_bus.py) and [`filmu_py/core/log_stream.py`](../../filmu_py/core/log_stream.py) are process-local/in-memory | Add cross-node eventing, replayable streams, subscription durability, node coordination, failover semantics, and HA readiness | Single-process signaling is a ceiling on scale, availability, and operator confidence | P0 |
| SRE / production operations program | Strong local proof scripts exist, but no top-level SLO/DR program is documented | Define SLOs, error budgets, rollback rules, backup/restore, DR objectives, incident runbooks, canary/rollout policy, and capacity review cadence | Enterprise-grade systems are judged by operability under failure, not only by feature breadth | P0 |
| Durable operator log pipeline | Filmu currently relies on stdlib/`structlog` plus the bounded in-memory broker in [`filmu_py/core/log_stream.py`](../../filmu_py/core/log_stream.py) | Add durable structured file output, retention/rotation, trace/span-enriched records, and shipper/search workflow comparable to the current `riven-ts` ECS + Filebeat + Elastic stack | Enterprise operations require searchable durable logs, not only live process-local compatibility streams | P0 |
| Plugin trust and isolation | [`filmu_py/plugins/manifest.py`](../../filmu_py/plugins/manifest.py) and [`filmu_py/plugins/loader.py`](../../filmu_py/plugins/loader.py) provide manifest and host-version validation | Add plugin signing, provenance, capability permission scopes, runtime sandboxing, quarantine/revocation, and release channels | Plugin extensibility becomes a liability without a trust model | P1 |
| Heavy-stage workload isolation | [`filmu_py/workers/tasks.py`](../../filmu_py/workers/tasks.py) has real stages, but no `riven-ts`-style worker-thread/sandboxed heavy jobs | Split parse/map/validate/index-style heavy work into isolated workers or sandboxes with bounded CPU/memory and crash containment | Current upstream `riven-ts` raised the baseline here; Filmu should exceed it, not trail it | P1 |
| Metadata/index governance | Request-time enrichment and repair exist, but indexing is not yet a first-class program track | Add dedicated index pipeline, reindex scheduling, provenance, confidence scoring, reconciliation audits, and drift tooling | Metadata correctness is a platform capability, not a request-time convenience | P1 |
| VFS enterprise data-plane | [`rust/filmuvfs/src/mount.rs`](../../rust/filmuvfs/src/mount.rs) now covers core read-path operations and adapters | Add stronger soak/chaos coverage, rollout policy per backend, fairness/backpressure budgets, cache correctness guarantees, and richer operator controls | FilmuVFS is the clearest chance to beat `riven-ts`; it needs platform-grade operational maturity | P1 |
| Release engineering and supply chain | Current docs mention quality gates, but not full release governance | Add SBOM/signing policy, dependency risk review, release promotion stages, artifact provenance, and upgrade/downgrade contracts | State-of-the-art systems treat delivery trust as part of product quality | P1 |
| Performance and chaos discipline | Bench/perf commands exist, but there is no explicit benchmark/chaos program | Define latency budgets, load-test profiles, chaos scenarios, regression thresholds, and nightly benchmark comparisons against prior builds and upstream behavior | “Fast enough locally” is not an enterprise or state-of-the-art bar | P1 |

## Immediate actions

1. Add enterprise identity/tenancy/authz as a top-level roadmap track rather than a future nice-to-have.
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
