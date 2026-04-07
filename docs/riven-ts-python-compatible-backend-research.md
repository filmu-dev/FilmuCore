# Riven-TS-Inspired Backend (Python) — Compatibility & Performance Research

## Context

Goal: design a **new backend** that mimics `riven-ts` architecture patterns while staying **fully backward-compatible** with the existing frontend contract.

Primary constraints:

- Keep current frontend working without breaking changes.
- Improve operational outcomes:
  - lower failure/retry rate
  - faster scrape/download latency
  - faster recovery after outages
  - higher items/minute throughput
  - avoid debrid rate-limit hits

---

## What was audited

### Current frontend compatibility contract (must remain stable)

The frontend is strongly bound to:

- OpenAPI-based type generation from backend `/openapi.json`
- REST endpoints under `/api/v1/*`
- SSE endpoint behavior and payload shape
- API key auth through backend/BFF flow

Practical implication: a new backend can change internals, but the API/SSE contract must remain equivalent.

### Current Python backend runtime model

Current runtime is event/state-transition driven with:

- program thread loop
- event queue + service executors
- state transition pipeline (Requested -> Indexed -> Scraped -> Downloaded -> Symlinked -> Completed)
- SSE manager for event fan-out

### `riven-ts` architecture patterns worth replicating

`riven-ts` emphasizes:

- explicit state machines (program/bootstrap/main-runner/plugin-registrar)
- typed event contracts
- queue/flow workers
- plugin registration/validation lifecycle
- plugin settings scoping + lock after bootstrap
- controlled event broadcast to plugin subscribers

---

## Key conclusion

## ✅ Recommended direction

Build a **new Python core** that mirrors `riven-ts` architecture principles, while preserving a strict compatibility boundary:

- **Compatibility API layer**: keep `/api/v1/*`, `/openapi.json`, auth, and SSE behavior stable.
- **New execution core**: durable workflow/flow orchestration, better queueing, better backpressure and retries.

This gives modern architecture + zero frontend disruption.

---

## Proposed target architecture

### 1) Compatibility Facade (unchanged contract)

- FastAPI continues exposing the same routes and response shapes.
- Same auth semantics (x-api-key/bearer/query compatibility).
- Same SSE channels/event names and payload shape.
- OpenAPI remains stable so frontend generation still works.

### 2) New Orchestration Core (Riven-TS-inspired)

- Event-driven lifecycle modeled explicitly (state machine/workflow graph).
- Flow workers for indexing/scraping/downloading/ranking.
- Durable retries and idempotent transitions.
- Parent-child fan-out behavior preserved for show/season/episode trees.

### 3) Plugin Runtime

- Plugin discovery + validation lifecycle.
- Event subscription model for plugin hooks.
- Typed event envelope for plugin handlers.
- Plugin settings namespace + lock-after-bootstrap behavior.

### 4) Streaming + SSE Stability

- Preserve stream URLs and HLS endpoints.
- Keep SSE wire format and event types consistent.
- Internals can be optimized independently.

---

## Technology stack to make it superior (Python + frontend compatible)

## API / Serialization

- FastAPI + Pydantic v2
- `orjson` or `msgspec` for faster JSON serialization/deserialization

## Workflow / Queue

- **Preferred**: Temporal (Python SDK) for durable workflows, retries, and recovery
- **Alternative (lighter ops)**: Arq + Redis Streams
- Redis for distributed coordination, locks, idempotency keys, token buckets

## Resilience / Retry / Rate Limits

- `tenacity` for structured retry policies with jittered exponential backoff
- `pybreaker` for provider-level circuit breakers
- Redis Lua token-bucket limiter per provider/endpoint
- adaptive throttling from `Retry-After` and provider-specific headers

## Throughput / Latency

- async `httpx` clients with tuned pools and keepalive
- priority queues (hot/recent requests first)
- workload partitioning by provider/media type
- in-memory + Redis two-layer caching

## Observability

- OpenTelemetry tracing
- Prometheus metrics + Grafana dashboards
- Loki/Tempo (or equivalent) for logs/traces
- Sentry for error aggregation

## Quality / Regression Safety

- Schemathesis for OpenAPI contract tests
- golden-response tests for critical endpoints
- SSE payload snapshot/contract tests
- canary rollout + shadow traffic + feature flags

---

## How this hits your requested outcomes

## 1) Lower failure/retry rate

- Circuit breakers prevent repeated failure storms.
- Better classification of transient/permanent errors.
- Idempotent jobs prevent duplicate retries and reprocessing.

## 2) Faster scrape/download latency

- Priority queue scheduling and tuned async clients.
- Provider-aware concurrency limits to maximize useful parallelism.
- Cache hot paths reduce repeated external calls.

## 3) Faster recovery

- Durable workflow state allows safe resume after crash/restart.
- Queue persistence + retry orchestration restore service quickly.

## 4) Zero regressions

- Keep API/SSE contracts frozen.
- Enforce contract test gates in CI before deployment.

## 5) More items/min without hitting debrid limits

- Distributed token buckets + adaptive throttling.
- Fair-share scheduler across providers/queues.
- Dynamic concurrency based on live error and limit signals.

---

## Recommended phased implementation

### Phase 0 — Contract freeze & test harness

- Snapshot current OpenAPI and SSE payload schemas.
- Build compatibility regression suite.

### Phase 1 — Compatibility shell + new core skeleton

- Keep existing endpoints; route internals to feature-flagged new execution path.

### Phase 2 — Migrate critical flows

- Migrate scrape/download/index flows to new worker model.
- Add distributed rate-limiter and circuit-breakers.

### Phase 3 — Plugin runtime modernization

- Introduce plugin registry, validation, subscriptions, and settings isolation.

### Phase 4 — Hardening and rollout

- Shadow traffic, canary rollout, metric-based promotion.

---

## Suggested KPIs (hard targets)

- Debrid rate-limit hit ratio: **< 1%**
- Retry rate: **-40% to -70%** from baseline
- Scrape p95 latency: **-30%+**
- Download-start p95 latency: **-25%+**
- Recovery time after outage: **< 2 minutes**
- Throughput (items/min): **+50% to +150%**
- Frontend compatibility regressions: **0** (contract suite)

---

## Final recommendation

Do **not** replace frontend contracts.
Build a **new Python execution core** that adopts `riven-ts` strengths (state machines, typed events, plugin lifecycle, queue flows), and expose it through the existing API/SSE compatibility boundary.

This is the safest path to become more powerful while remaining fully compatible.
