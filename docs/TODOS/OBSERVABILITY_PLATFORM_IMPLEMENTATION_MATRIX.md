# Observability Platform Implementation Matrix

## Goal

Turn the observability platform prompt into a concrete implementation program for Filmu.

This plan assumes the quality bar is:

- enterprise-grade
- state-of-the-art
- stronger than current `riven-ts` Winston + ECS + Filebeat

This is the implementation matrix that should drive the next observability work, not an optional wishlist.

---

## Design constraints

- Canonical event model belongs to Filmu, not ECS.
- OpenTelemetry is the correlation and export backbone.
- Python and Rust must share one event vocabulary.
- Structured logs are mandatory.
- File-backed NDJSON is fallback and forensic, not the system of record.
- Sentry is an exception-analysis side tool, not the primary telemetry store.
- Queue, stream, media, VFS, security, and audit events are first-class semantic families.

---

## Recommended repository layout

```text
docs/
  observability-architecture.md
  observability-operations.md
  observability-schema.md
  observability-backend-profiles.md

filmu_py/observability/
  __init__.py
  logger.py
  context.py
  otel.py
  redaction.py
  schema.py
  adapters/
    __init__.py
    ecs.py
    otlp.py
  events/
    __init__.py
    request.py
    queue.py
    media.py
    security.py
    audit.py

rust/filmuvfs/src/observability/
  mod.rs
  schema.rs
  context.rs
  telemetry.rs
  events.rs
  transport.rs
  adapters.rs

ops/
  otel-collector.yaml
  vector.yaml
  clickhouse/
  elastic/

tests/observability/
  test_schema.py
  test_redaction.py
  test_log_correlation.py
  test_json_rendering.py
  test_sampling_policy.py
  test_backend_adapters.py

rust/filmuvfs/tests/
  observability_schema.rs
  observability_transport.rs
  observability_correlation.rs

docker-compose.observability.yml
```

---

## Current code that should be evolved, not replaced blindly

Python baseline to evolve:

- `filmu_py/logging.py`
- `filmu_py/observability.py`
- `filmu_py/middleware.py`
- `filmu_py/workers/retry.py`
- `filmu_py/core/log_stream.py`

Rust baseline to evolve:

- `rust/filmuvfs/src/telemetry.rs`

Tests to expand:

- `tests/test_observability.py`

These files already contain useful foundations. They should be refactored into the new layout rather than reimplemented with throwaway glue.

---

## Phase plan

## Phase 1: Canonical event model and policy layer

Status: not started

Deliver:

- `docs/observability-schema.md`
- Python `schema.py`
- Rust `schema.rs`
- versioned event envelope
- field classification model
- sampling rules
- retention rules
- severity taxonomy
- event family registry

Required core envelope fields:

- `timestamp`
- `severity`
- `message`
- `event.name`
- `event.version`
- `service.name`
- `service.namespace`
- `deployment.environment.name`
- `component`
- `subsystem`
- `trace_id`
- `span_id`
- `request_id`
- `correlation_id`
- `tenant_id`
- `actor_id`
- `source_kind`
- `host.name`
- `process.pid`
- `error.type`
- `error.message`
- `error.stack`
- `outcome`

Domain extensions required in the base schema set:

- queue fields
- media fields
- VFS fields
- security/audit fields

Gate:

- both Python and Rust can serialize the same event envelope shape
- schema versioning rules are documented

## Phase 2: Python control-plane logging platform

Status: not started

Deliver:

- `filmu_py/observability/logger.py`
- `filmu_py/observability/context.py`
- `filmu_py/observability/otel.py`
- `filmu_py/observability/redaction.py`
- `filmu_py/observability/events/*.py`

Implementation requirements:

- `structlog` + stdlib logging integration
- `orjson` renderer
- `QueueHandler` / `QueueListener`
- bounded in-memory queue
- explicit dropped-event counters
- stdout structured JSON sink
- rotating NDJSON fallback sink
- trace/span injection from OpenTelemetry context
- `contextvars` binding for request/job/session/provider/VFS correlation
- redaction and field classification before any sink/export
- typed helpers for:
  - request plane events
  - queue plane events
  - media plane events
  - security/audit events

Gate:

- hot-path logging is non-blocking
- JSON output is schema-valid
- trace/span IDs appear in emitted records
- secrets are masked before output

## Phase 3: Rust sidecar semantic telemetry

Status: not started

Deliver:

- `rust/filmuvfs/src/observability/mod.rs`
- `rust/filmuvfs/src/observability/schema.rs`
- `rust/filmuvfs/src/observability/context.rs`
- `rust/filmuvfs/src/observability/events.rs`
- `rust/filmuvfs/src/observability/transport.rs`

Implementation requirements:

- preserve existing `tracing` and OTLP strengths
- add typed VFS semantic events
- add shared envelope fields and correlation context
- emit event families for:
  - `vfs.lookup`
  - `vfs.open`
  - `vfs.read.start`
  - `vfs.read.done`
  - `vfs.write`
  - `vfs.cache.hit`
  - `vfs.cache.miss`
  - `vfs.provider.fetch`
  - `vfs.provider.error`
  - `vfs.prefetch.scheduled`
  - `vfs.prefetch.completed`
- support OTLP export and one local same-host transport
  - preferred: Unix domain socket on Unix and named-pipe-equivalent strategy on Windows if needed
  - acceptable fallback: direct OTLP log/event export

Gate:

- Rust sidecar emits schema-compatible events
- Python and Rust share correlation keys
- VFS activity is queryable as semantic events rather than only metrics

## Phase 4: Collector and edge routing

Status: not started

Deliver:

- `ops/otel-collector.yaml`
- optional `ops/vector.yaml`

Collector must include:

- OTLP receivers
- memory limiter
- batch processor
- resource/attribute enrichment
- exporter wiring for at least one backend profile

Vector should be included when local buffering and spooling are needed:

- local ingestion
- transform/remap
- durable disk buffering
- explicit redaction or enrichment only if it materially improves edge safety

Gate:

- local stack receives telemetry from Python and Rust
- backpressure behavior is defined and measurable

## Phase 5: Backend profiles

Status: not started

Deliver:

- `docs/observability-backend-profiles.md`
- `ops/clickhouse/*`
- `ops/elastic/*`

Profile A:

- OpenTelemetry Collector
- ClickHouse
- Grafana or HyperDX
- Sentry

Profile B:

- structured JSON or adapted ECS output
- Collector and/or Vector
- Elastic
- Kibana
- Sentry

Non-negotiable rule:

- ECS mapping must live in an adapter layer
- ECS must not become Filmu's canonical internal schema

Gate:

- one event can be exported to both profiles without application code rewrite

## Phase 6: Tests and schema locks

Status: not started

Deliver:

- schema-presence tests
- JSON validity tests
- trace/span correlation tests
- redaction tests
- schema stability tests
- backend-adapter mapping tests
- sampling and priority policy tests

Required policy assertions:

- `audit.*` never sampled and always persisted
- `security.*` never sampled and always persisted
- `queue.job.retried` warning-or-above and never sampled
- `vfs.cache.hit` debug and sampleable
- `http.request.completed` info and sampleable

Gate:

- observability contract is test-locked, not convention-based

## Phase 7: Documentation and operations package

Status: not started

Deliver:

- `docs/observability-architecture.md`
- `docs/observability-operations.md`
- `docs/observability-schema.md`
- `docker-compose.observability.yml`

Operations docs must include:

- local development flow
- production deployment patterns
- retention rules
- sampling rules
- troubleshooting guide
- redaction model
- failure and degradation behavior
- how Filmu surpasses `riven-ts`

Gate:

- operators can run the local stack
- engineers can add a new event family without reverse-engineering the platform

---

## Event priority policy

The policy below should become executable code, not only documentation.

| Event class | Sampling | Persistence | Minimum severity | Notes |
| --- | --- | --- | --- | --- |
| `audit.*` | never sampled | always persisted | `info` | compliance and operator accountability |
| `security.*` | never sampled | always persisted | `warning` | incident and abuse response |
| `queue.job.retried` | never sampled | always persisted | `warning` | retry pressure must be visible |
| `queue.job.dead_lettered` | never sampled | always persisted | `error` | operator action required |
| `vfs.provider.error` | never sampled | always persisted | `error` | external dependency failure |
| `http.request.completed` | sampleable | normal policy | `info` | high-volume request plane |
| `vfs.cache.hit` | sampleable | normal policy | `debug` | high-volume performance signal |

---

## Backpressure and reliability requirements

These need to exist as code and tests:

- non-blocking emission in hot paths
- bounded in-memory queue for log/event handoff
- explicit dropped-event counters
- policy-based preferential preservation of audit/security/error events
- optional disk buffering through Vector or fallback NDJSON
- graceful degradation when collector/backend is unavailable

If these are not implemented, Filmu will not have an enterprise-grade telemetry platform regardless of how nice the JSON looks.

---

## Key architectural trade-offs

- Canonical schema first, adapters second:
  Filmu keeps control over semantics and can support both ClickHouse-first and Elastic-compatible deployments without field drift taking over the codebase.

- OTLP-first, file-second:
  This is more future-proof and stronger than centering the design around ECS files, but it requires collector discipline and backpressure policy.

- Shared Python/Rust vocabulary:
  This adds upfront design cost, but it is the only credible way to make cross-runtime debugging and VFS-aware operations first-class.

- Vector optional, not mandatory:
  The collector should remain the core router. Vector is useful at the edge when spooling and transforms matter, but it should not complicate the minimum viable platform.

---

## Success criteria

This workstream is complete only when Filmu has all of the following:

1. a concrete architecture document
2. Python telemetry scaffolds aligned to the canonical schema
3. Rust sidecar semantic telemetry aligned to the same schema
4. collector config
5. optional vector config
6. local observability compose stack
7. schema, redaction, correlation, and stability tests
8. clear backend profile adapters
9. documentation proving why Filmu now surpasses current `riven-ts`

Until then, observability should be considered improved but not enterprise-complete.
