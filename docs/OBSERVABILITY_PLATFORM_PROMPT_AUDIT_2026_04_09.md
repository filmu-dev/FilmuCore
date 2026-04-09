# Observability Platform Prompt Audit (2026-04-09)

## Purpose

Audit the current `riven-ts` logging system, then compare the current Filmu source tree against the target observability platform prompt for an enterprise-grade, state-of-the-art Python control plane plus Rust VFS sidecar.

This document is intentionally practical. It is not a general observability essay. It answers:

- what `riven-ts` actually does today
- what Filmu actually does today
- where Filmu already exceeds `riven-ts`
- where Filmu is still materially behind the prompt and behind an enterprise-grade bar
- what must exist before Filmu can credibly claim that it surpasses the current `riven-ts` logging design

---

## Upstream `riven-ts` logging audit

Current upstream logging is centered around Winston and a local Elastic/Filebeat workflow.

Primary upstream source files:

- `apps/riven/lib/utilities/logger/logger.ts`
- `apps/riven/lib/utilities/logger/formatters/console.format.ts`
- `apps/riven/lib/utilities/logger/formatters/ecs-file.format.ts`
- `apps/riven/lib/utilities/logger/formatters/file.format.ts`
- `apps/riven/lib/utilities/logger/formatters/sentry-meta.format.ts`
- `apps/riven/lib/sentry.ts`
- `elastic-local/config/filebeat.yaml`

### What `riven-ts` currently does well

- Uses a real transport-based logging stack rather than an in-memory compatibility surface.
- Writes ECS-formatted NDJSON to `logs/ecs.json` whenever logging is enabled.
- Can also write durable local `combined.log`, `error.log`, and `exceptions.log`.
- Rotates local files with `maxsize`, `maxFiles`, and archive support.
- Enriches logs with Sentry trace/span metadata before formatting.
- Tags console output with source-oriented fields such as `riven.log.source` and `riven.worker.id`.
- Ships ECS NDJSON into Elasticsearch via Filebeat in the local operator stack.
- Wires queue and worker error paths into the same logging surface.

### What `riven-ts` does not represent

It is stronger than a toy logger, but it is still not the target architecture Filmu should adopt.

It is not:

- a canonical internal event model owned by the application
- a multi-signal observability platform with logs, traces, and metrics under one schema contract
- an OTLP-first architecture
- a backend-neutral event adapter system
- an explicit redaction, sampling, retention, and backpressure policy framework
- a unified Python-plus-Rust vocabulary, because `riven-ts` is fundamentally a TypeScript service with worker/runtime tagging rather than a shared cross-runtime semantic model

`riven-ts` should therefore be treated as the current baseline to beat, not the destination.

---

## Current Filmu observability baseline

### Python control plane

Current Filmu Python observability is split across:

- `filmu_py/logging.py`
- `filmu_py/observability.py`
- `filmu_py/middleware.py`
- `filmu_py/workers/retry.py`
- `filmu_py/core/log_stream.py`
- `filmu_py/core/event_bus.py`
- `tests/test_observability.py`

Current strengths:

- `structlog` plus stdlib logging is already present.
- request-scoped context propagation exists through `contextvars`
- worker correlation keys are already bound in retry/background paths
- Prometheus metrics already exist across route, cache, worker, plugin, serving, and stream layers
- optional OpenTelemetry tracing bootstrap exists for the FastAPI app
- optional Sentry integration exists
- the frontend compatibility log stream already has a bounded in-memory broker and live fan-out path

Current limitations:

- JSON logging uses `structlog.processors.JSONRenderer()`, not an `orjson` renderer
- there is no `QueueHandler` / `QueueListener` or equivalent non-blocking emission path for hot paths
- there is no rotating NDJSON fallback file
- there is no durable structured log retention outside process memory
- there is no explicit log schema owned by Filmu
- there is no trace/span injection into every emitted log record
- there is no redaction/classification processor before export
- there is no event priority or sampling policy
- there is no explicit dropped-event accounting path
- there is no backend adapter layer for ECS, ClickHouse-first, or Elastic-compatible export profiles

### Rust sidecar

Current Filmu Rust sidecar observability is centered around:

- `rust/filmuvfs/src/telemetry.rs`

Current strengths:

- `tracing`, `tracing-subscriber`, and `opentelemetry-otlp` are already in use
- OTLP traces and metrics are already supported
- the sidecar already emits a materially stronger VFS metric surface than `riven-ts`
- runtime status snapshots already exist for mounted reads, upstream fetches, prefetch, cache, and Windows adapter behavior
- Windows-specific operator summary logs already exist for ProjFS callback behavior

Current limitations:

- there is no canonical sidecar event schema shared with Python
- there is no JSON event sink or OTLP log export path with explicit event families
- there is no Unix domain socket event emitter for same-host ingestion
- sidecar semantics are metrics-heavy and partially log-based, not a full event platform
- there is no schema versioning contract for VFS event families
- there is no explicit backpressure, sampling, or audit-priority policy

### Net current-state conclusion

Filmu already has stronger VFS metrics and sidecar runtime introspection than `riven-ts`.

Filmu does **not** yet have a stronger operator logging architecture, nor a stronger unified observability platform, than the target prompt requires.

Today Filmu is:

- ahead of `riven-ts` in some data-plane metrics depth
- behind `riven-ts` in durable operator logging and local searchable log pipeline maturity
- materially behind the target prompt in canonical schema design, OTLP-first logging architecture, backend-neutral adapters, redaction policy, reliability policy, and shared Python/Rust event vocabulary

---

## Comparison against the target prompt

Status meanings:

- `Implemented`: clearly present in current source
- `Partial`: meaningful baseline exists, but prompt bar is not met
- `Missing`: not present in a meaningful way

### Target architecture principles

| Principle | Current Filmu status | Notes |
| --- | --- | --- |
| Canonical internal event model owned by Filmu | Missing | No shared event envelope or versioned schema exists across Python and Rust. |
| OpenTelemetry as correlation model | Partial | FastAPI tracing and Rust OTLP exist, but logs are not fully trace/span correlated. |
| Structured logging everywhere | Partial | Python uses `structlog`; Rust uses `tracing`; semantics are not unified. |
| OTLP as primary export path | Partial | Present for traces and metrics, not for the total telemetry system. |
| Rotating NDJSON as fallback only | Missing | No durable fallback log file path in Python. |
| Logs, traces, and metrics as one system | Partial | Signals exist, but schema, routing, and policy are fragmented. |
| Multiple backend adapters | Missing | No explicit adapter/mapping layer. |
| Explicit redaction, retention, sampling, backpressure | Missing | No coherent policy implementation exists. |
| Queue and VFS as first-class event families | Partial | Metrics exist, but not typed semantic event families. |
| Shared Python and Rust observability vocabulary | Missing | Current fields and semantics are runtime-local. |

### Required deliverables from the prompt

| Deliverable | Current Filmu status | Notes |
| --- | --- | --- |
| High-level architecture document | Missing | Existing docs discuss observability maturity, but not the required platform architecture. |
| Recommended repository layout | Missing | No dedicated observability package/layout exists. |
| Python logging/config/context/redaction/domain events/dual sink code | Missing | Pieces exist, but not as the required platform scaffold. |
| Rust telemetry/event definitions/VFS semantic events/emitter | Partial | Telemetry exists, but not the prompt's event model and delivery design. |
| OpenTelemetry Collector configuration | Missing | No collector config exists in-repo. |
| Optional Vector configuration | Missing | No Vector edge agent config exists. |
| Docker Compose observability stack | Missing | Local compose exists for app dependencies, not for the observability platform. |
| Event schema definitions and versioning strategy | Missing | No canonical event schema contract exists. |
| Tests for schema/correlation/redaction/JSON/schema stability | Missing | Current tests cover metrics, not the prompt's telemetry contract. |
| Docs for dev/prod/ops/retention/sampling/troubleshooting | Missing | Existing docs do not yet cover the full observability platform lifecycle. |

### Python implementation requirements

| Requirement | Current Filmu status | Notes |
| --- | --- | --- |
| `structlog` configuration | Implemented | Present in `filmu_py/logging.py`. |
| stdlib logging integration | Implemented | Present. |
| `orjson` renderer | Missing | Dependency exists, current logger does not use it. |
| `QueueHandler` / `QueueListener` | Missing | No non-blocking queue-based sink exists. |
| `contextvars` metadata binding | Partial | Present for request/worker correlation, not as a full platform context layer. |
| trace/span injection into logs | Missing | OTel traces exist, but log records are not consistently enriched. |
| typed domain event helpers | Missing | No `event.name`/family-based helper layer exists. |
| stdout JSON sink | Partial | JSON stdout exists, but without canonical schema guarantees. |
| rotating NDJSON fallback | Missing | No fallback file sink exists. |
| redaction processors | Missing | No explicit redaction layer exists. |

### Rust implementation requirements

| Requirement | Current Filmu status | Notes |
| --- | --- | --- |
| `tracing` setup | Implemented | Present in `rust/filmuvfs/src/telemetry.rs`. |
| structured JSON output or OTLP export | Partial | OTLP exists for traces/metrics; JSON event/log architecture is not complete. |
| VFS event structs | Missing | Metrics and snapshots exist, but not typed event families. |
| semantic event emission for filesystem/cache/provider activity | Partial | Behavior is observable indirectly, but not through a formal event taxonomy. |
| correlation field support | Partial | Some service/session fields exist, not the shared prompt envelope. |
| Unix domain socket emitter or OTLP log exporter | Missing | Not implemented. |

### Collector / Vector / backend profile requirements

| Requirement | Current Filmu status | Notes |
| --- | --- | --- |
| OTLP receiver config | Missing | No collector config exists. |
| memory limiter / batch / enrichment processors | Missing | No collector config exists. |
| ClickHouse-first backend profile | Missing | No profile or mapping exists. |
| Elastic-compatible backend profile | Missing | No profile or mapping exists. |
| ECS adapter isolated from canonical model | Missing | No canonical model exists yet. |
| Vector edge buffering/transforms/spooling | Missing | No Vector config exists. |

### Reliability / redaction / policy requirements

| Requirement | Current Filmu status | Notes |
| --- | --- | --- |
| header allowlist/denylist | Missing | No general log redaction policy exists. |
| token/password/cookie masking | Missing | No central processor exists. |
| nested payload traversal | Missing | No central processor exists. |
| payload truncation | Missing | No central processor exists. |
| classification tags | Missing | No field classification system exists. |
| bounded in-memory queue | Partial | Exists for frontend log streaming, not the main operator logging pipeline. |
| dropped-event counters | Missing | No explicit telemetry for observability backpressure exists. |
| preserve audit/security events under pressure | Missing | No priority policy exists. |
| optional disk buffering via Vector or file fallback | Missing | No edge buffering stack exists. |
| graceful degradation policy | Missing | No defined behavior contract exists. |

---

## Required event families versus current Filmu

### Request plane

Current status: `Partial`

- Request metrics and request IDs exist.
- Formal events such as `http.request.received`, `http.request.completed`, `auth.denied`, and `rate_limit.hit` do not exist as first-class typed event helpers.

### Queue plane

Current status: `Partial`

- Worker metrics exist for enqueue, retry, DLQ, and stage duration.
- Prompt-required semantic events such as `queue.job.claimed`, `queue.job.timeout`, and `queue.job.dead_lettered` do not exist as canonical events shared across backends.

### Media plane

Current status: `Partial`

- Media resolution and serving metrics exist.
- There is no canonical semantic family for `media.resolve.started`, `media.resolve.completed`, `media.stream.stalled`, or `media.stream.buffer_drop`.

### VFS plane

Current status: `Partial`

- Rust sidecar metrics already cover cache, upstream, prefetch, mounted reads, and adapter summaries.
- Prompt-required typed events such as `vfs.lookup`, `vfs.open`, `vfs.read.start`, `vfs.read.done`, `vfs.provider.fetch`, `vfs.provider.error`, and `vfs.prefetch.completed` are not yet modeled as a versioned event family.

### Security / audit plane

Current status: `Missing`

- There is no dedicated audit/security event family with never-sampled persistence policy.
- There is no implementation of events such as `security.access_denied`, `security.redaction.applied`, `audit.config.changed`, or `audit.operator.action`.

---

## Where Filmu can surpass `riven-ts`

Filmu should not beat `riven-ts` by only copying Winston plus ECS plus Filebeat. That would be strategic underreach.

Filmu can surpass the current `riven-ts` design by doing all of the following:

- make OTLP the first-class export path instead of treating logs as primarily file artifacts
- define one canonical Filmu event model and treat ECS as only one adapter
- unify Python control-plane and Rust sidecar semantics under one schema and correlation model
- keep file-backed NDJSON only as a fallback and forensic layer
- make queue, stream, media, and VFS events explicit typed families rather than ad hoc log strings
- implement redaction, classification, and reliability policies as code, not tribal process
- support both ClickHouse-first and Elastic-compatible backend profiles without rewriting the app
- preserve enterprise audit and security events even when the system is under backpressure

That is the real state-of-the-art bar.

---

## Current judgment against the prompt

If the target prompt is treated as the acceptance bar, current Filmu is **not** close to complete.

Practical judgment:

- Python platform: foundational but far below prompt-complete
- Rust sidecar: stronger than Python on metrics and OTLP wiring, but still below prompt-complete
- pipeline layer: mostly absent
- backend adapter layer: absent
- policy layer: absent
- schema/versioning layer: absent
- operations package: absent

The current Filmu codebase therefore does **not** yet implement the required observability platform. It implements useful observability fragments.

---

## Enterprise-grade requirements that must now be explicit

Filmu should be documented and built as:

- enterprise-grade
- state-of-the-art
- operator-friendly
- backend-neutral
- audit-aware
- cross-runtime by default

For observability specifically, that means Filmu should not stop at parity with `riven-ts`.

The minimum winning condition is:

1. a canonical Filmu event schema
2. a unified Python plus Rust vocabulary
3. OTLP-first transport and collector routing
4. durable fallback logs
5. explicit policy for redaction, sampling, retention, and backpressure
6. backend adapters for ClickHouse-first and Elastic-compatible deployments
7. tests that lock the schema and correlation model

---

## Decision

The prompt should be treated as an approved implementation target, not as aspirational prose.

The next required artifact is an execution matrix that turns this audit into concrete repository scaffolding, phases, and file targets.
