# Next 15 Slices Execution Board

## Purpose

Turn the current open posture in [`../STATUS.md`](../STATUS.md), [`../EXECUTION_PLAN.md`](../EXECUTION_PLAN.md), and the active TODO matrices into one ordered execution board for the next 15 implementation slices.

This document is intentionally execution-shaped:

- ordered by dependency safety
- explicit about ownership and acceptance
- biased toward enterprise-grade hardening rather than new surface-area sprawl

Use this board as a delivery index. Do not treat it as a second source of truth that can drift from the canonical status docs.

---

## Enterprise Quality Bar

Every slice in this board must ship with the same minimum quality floor:

1. code, tests, metrics, logs, audit signals, and operator-facing status updates land together
2. negative-path coverage is required, not only happy-path validation
3. frozen playback, auth, plugin, and operator contracts do not regress
4. [`/api/v1/operations/governance`](../../filmu_py/api/routes/default.py) remains aligned with the actual rollout posture
5. playback/VFS promotion remains blocked when required proof or soak gates are red
6. a runbook, rollout note, or operator doc update lands in the same slice when operational behavior changes

Additional bar for enterprise-grade claims:

- no slice is considered complete until failure behavior is observable and attributable
- no risky runtime slice is promoted without proof, soak, or failure-injection evidence
- no new extensibility or orchestration breadth is promoted ahead of its isolation and observability model

---

## Priority Policy

- `P0` means release-governing and promotion-blocking.
- `P1` means important follow-on work that can overlap only when ownership and write surfaces are disjoint.
- Dependency order wins over convenience. Do not pull later slices forward just because they are easier.

Current incremental note:

- `Slice G` now has an initial baseline landed: the enterprise `vfs_data_plane` governance slice consumes the same live runtime rollout posture already exposed by `/api/v1/stream/status`, including runtime-snapshot availability, rollout readiness, rollout reasons, cache/fallback/prefetch ratios, and provider/fairness pressure incidents.
- `Slice H` now has an initial runtime hardening baseline landed in the Rust sidecar: mounted foreground reads inherit explicit per-handle cancellation, released handles can no longer repopulate chunk-engine tracking state after an interrupted read, ProjFS command cancellation is wired into the async callback path, and cancelled read outcomes now surface in runtime status and `/api/v1/stream/status`.

---

## Execution Board

| Slice | Priority | Owner | Depends On | Core Deliverables | Required Tests | Definition of Done |
| --- | --- | --- | --- | --- | --- | --- |
| `G` Mounted telemetry rollups | `P0` | VFS / Data Plane | Current runtime snapshot baseline | Roll up cache pressure, prefetch depth, read amplification, inline refresh outcomes, provider wait, and mounted-read failure classes into `stream/status`, `operations/governance`, and soak artifacts | unit tests for rollups, status route contract tests, soak artifact assertions | Operators can classify mounted failures from status and artifact surfaces without log forensics |
| `H` Abort-safe mounted reads | `P0` | VFS / Data Plane | `G` | Explicit abort, cancel, release, and interrupted-read cleanup semantics for mounted reads and handle lifecycle | seek/resume interruption tests, concurrent abort tests, handle-leak regressions | Interrupted reads do not leak handles, poison cache state, or force remount/restart recovery |
| `I` VFS canary and rollback controls | `P0` | Platform / SRE | `G`, `H` | Machine-shaped rollout thresholds, readiness states, rollback triggers, and governance blocking rules | promotion-rule tests, blocked-state tests, failure-injection checks | VFS rollout promotion and rollback are explicit, enforceable, and visible on governance surfaces |
| `J` Repeated playback gate promotion | `P0` | Playback / Platform | `G`, `H`, `I` | Repeated Docker Plex plus native Windows Emby/Plex plus soak evidence in the gate, stable required-check names, admin-authenticated branch-policy validation | repeated gate runs, artifact validation tests, policy-check script validation | Playback proof is repeatable, policy-backed, and enforced as a real merge gate |
| `K` ABAC expansion across control plane | `P0` | Authz / Platform | Current access-policy revision baseline | Tenant and actor-aware authorization across items, streams, plugins, governance, and operator/admin routes | authz matrix tests, deny/allow audit tests, tenant-boundary tests | Privileged behavior no longer relies on implicit admin assumptions or shallow permission string checks |
| `L` OIDC/SSO rollout completion | `P0` | Identity / Platform | `K` | Final issuer, audience, and claims mapping, subject-to-tenant rules, rollout docs, and operator smoke paths | token validation tests, bad-claims tests, tenant-mapping tests, smoke tests | OIDC bearer-token auth is operationally usable and diagnosable in production terms |
| `M` Policy audit retention and search | `P1` | Identity / Ops | `K`, `L` | Durable allow/deny retention, bounded query/search surfaces, and alert hooks for repeated denials and risky overrides | retention tests, search/filter tests, alert-condition tests | Policy decisions are searchable by actor, tenant, permission, route, and outcome |
| `N` Formal runtime lifecycle graph | `P1` | Platform / Runtime | None | Explicit bootstrap, plugin registration, steady-state, and degraded-state lifecycle modeling with observable transitions | startup transition tests, degraded-mode tests, status-surface tests | Runtime transitions are modeled, observable, and testable instead of implicit in startup code and logs |
| `O` Dedicated index stage | `P1` | Orchestration / Domain | `N` | Real `index_item` stage for metadata enrichment, canonical identifier reconciliation, and reindex scheduling | stage transition tests, retry tests, metadata enrichment tests | Metadata enrichment is a first-class stage with its own retry, observability, and replay semantics |
| `P` Idempotency and replay hardening | `P1` | Orchestration / Platform | `N`, `O` | Stage-wide idempotency keys, stronger dedup boundaries, richer DLQ reason taxonomy, and broader replay history | duplicate-execution tests, DLQ taxonomy tests, replay tests | Retries and replays cannot corrupt lifecycle state or duplicate side effects |
| `Q` Heavy-stage isolation beyond rank | `P1` | Orchestration / Performance | `O`, `P` | Move parse, map, and validate style heavy work into bounded isolated executors or sandboxed workers | crash-isolation tests, timeout tests, noisy-neighbor tests | Heavy jobs cannot materially degrade unrelated worker throughput or runtime stability |
| `R` Queued stream-link resolver path | `P1` | Playback / VFS | `P`, `Q` | Optional queued resolver path, explicit VFS lease/control read model, and off-read-path refresh handling for pressure cases | resolver queue tests, latency-path tests, stale-link recovery tests | Refresh pressure can be shifted off the mounted read path without correctness loss |
| `S` Plugin health rollups | `P1` | Plugin / Platform | Current plugin governance baseline | Per-plugin readiness, timeout and error rate, quarantine and override state, and last-failure summaries on operator surfaces | plugin rollup tests, operator route tests, timeout-state tests | Operators can identify unhealthy plugins immediately from status and governance surfaces |
| `T` Non-builtin runtime isolation | `P0` | Plugin / Security | `S` | Stronger sandbox or process boundaries, policy-enforced capability restrictions, and stricter runtime containment for community plugins | sandbox policy tests, escape/failure-containment tests, quarantine tests | A bad community plugin cannot materially degrade or overreach the host runtime |
| `U` Environment log/search and trace convergence | `P0` | Observability / SRE | `G`, `K`, `S`, `T` | Promote the local Vector/OpenSearch reference stack into environment-owned shipping, alerts, and Python-to-Rust trace correlation | shipper contract tests, alert wiring tests, trace propagation tests | Logs, traces, auth, queue, VFS, and plugin failures correlate across processes and environments |

---

## Wave Plan

### Wave 1

Slices:

- `G`
- `H`
- `I`
- `J`

Goal:

- make playback and VFS promotion trustworthy

Exit gate:

- repeated playback gate green
- soak evidence classified
- VFS rollout blocking wired to governance posture

### Wave 2

Slices:

- `K`
- `L`
- `M`

Goal:

- make enterprise identity and policy operable rather than partially implemented

Exit gate:

- ABAC enforced across the control plane
- OIDC rollout path validated
- policy audit search and retention live

### Wave 3

Slices:

- `N`
- `O`
- `P`
- `Q`
- `R`

Goal:

- make orchestration explicit, replay-safe, and isolated

Exit gate:

- runtime lifecycle graph live
- dedicated index stage active
- idempotency and replay hardening enforced
- heavy jobs isolated
- queued resolver path available when mounted pressure requires it

### Wave 4

Slices:

- `S`
- `T`
- `U`

Goal:

- make plugin and observability posture enterprise-operable

Exit gate:

- plugin health and isolation are enforceable
- cross-process log and trace convergence exists outside the local reference stack

---

## Sequencing Rules

1. Do not start `Q`, `R`, `T`, or `U` before their observability and policy prerequisites exist.
2. Do not promote any slice that changes runtime risk without proof, soak, or failure-injection evidence.
3. Do not expand extensibility breadth before plugin isolation and operator health visibility deepen.
4. Do not expand orchestration breadth before idempotency and replay boundaries are explicit.
5. Do not claim enterprise-grade readiness from local reference assets alone; environment-owned rollout and operator evidence are required.

---

## Anti-Goals

- Do not spend these slices on broad new REST surface expansion.
- Do not prioritize new first-class entity families over hardening and read-model depth.
- Do not reopen playback contracts during hardening unless an additive compatibility path exists.
- Do not duplicate the canonical posture already maintained in [`../STATUS.md`](../STATUS.md); update both documents together when slice state changes.

---

## Working Rule

The practical interpretation of "state of the art enterprise grade" for this repository is:

- safer rollout
- stricter policy
- stronger isolation
- better replay and recovery semantics
- better proof
- better observability

If a slice adds power without adding one of those control layers, it is not ready for promotion.
