# Operator Log Pipeline

## Purpose

FilmuCore now writes rotating ECS/NDJSON-style logs and keeps compatibility live-log streams. This document defines how those logs should graduate into an enterprise operator pipeline with shipping, search, replay taxonomy, and trace correlation.

## Current Baseline

Current evidence:

- Structured logs are emitted by `filmu_py/logging.py`.
- The default structured file is `logs/ecs.json`.
- Rotating retention is controlled by `Settings.logging`.
- API, worker, plugin, tenant, request, item, and trace context can be attached to log records when available.
- `/api/v1/logs` and SSE streams remain compatibility surfaces.
- `/api/v1/operations/governance` reports structured log posture, OTLP configuration state, log shipper settings, field mapping version, and durable replay posture, while plugin/runtime-isolation posture is now a separate adjacent governance slice.
- [`../ops/vector/filmu-ndjson-vector.toml`](../ops/vector/filmu-ndjson-vector.toml) is the first concrete Vector tailing configuration for `logs/ecs.json*`.
- [`../ops/log-pipeline/docker-compose.yml`](../ops/log-pipeline/docker-compose.yml) now provisions a local operator-owned OpenSearch plus Vector stack for search/collector validation.
- [`../ops/opensearch/filmu-index-template.json`](../ops/opensearch/filmu-index-template.json) defines the baseline searchable field mapping contract used by that stack.
- `pnpm proof:operations:log-pipeline` runs [`../scripts/check_log_pipeline_contract.ps1`](../scripts/check_log_pipeline_contract.ps1) and writes a search-contract artifact under `artifacts/operations/log-pipeline/`.

## Required Shipper Contract

The log shipper should:

- Read every `logs/ecs.json*` file as NDJSON.
- Preserve `@timestamp`, `log.level`, `message`, `event.original`, `request.id`, `item.id`, `worker.stage`, `worker.job_id`, `worker.id`, `plugin.name`, `trace.id`, and `span.id`.
- Add deployment metadata outside the application payload: environment, node id, image/tag, branch, and commit SHA.
- Backpressure safely without deleting unshipped logs.
- Alert when shipping is stalled longer than one rotation interval.

Recommended local adapters:

- Vector, Filebeat, Fluent Bit, or an equivalent NDJSON file tailer.
- OpenTelemetry Collector for traces and future log/trace convergence.

Provisioned local reference stack:

- `docker compose -f ops/log-pipeline/docker-compose.yml up -d` starts OpenSearch, OpenSearch Dashboards, and Vector with the checked-in field contract.
- Vector tails `logs/ecs.json*`, preserves the ECS/Filmu fields already emitted by the app, and writes them into the `filmu-logs-*` index pattern.
- The OpenSearch index template is repo-owned so operator search fields stop depending on ad hoc environment clicks.

## Search Index Expectations

Minimum searchable fields:

- `@timestamp`
- `log.level`
- `message`
- `request.id`
- `tenant.id` or `structlog.tenant_id`
- `actor.id` or `structlog.actor_id`
- `item.id`
- `worker.stage`
- `plugin.name`
- `trace.id`
- `span.id`

Minimum saved searches:

- Auth denials by tenant and actor.
- Playback/VFS failures by route and item.
- Queue critical warnings and dead-letter events.
- Plugin trust/publisher/quarantine decisions.
- Provider throttling and refresh failures.
- Migration/startup errors.

## Replay Taxonomy

Replay-related evidence should use stable event classes:

- `queue.enqueue_decision`
- `queue.stale_result_cleanup`
- `queue.dead_letter`
- `worker.stage_started`
- `worker.stage_completed`
- `worker.stage_failed`
- `playback.refresh_requested`
- `playback.refresh_succeeded`
- `playback.refresh_failed`
- `vfs.inline_refresh_requested`
- `vfs.inline_refresh_failed`
- `plugin.policy_rejected`
- `plugin.quarantine_recommended`

These event classes are a contract for future durable replay streams. Structured logs remain the primary forensic record until the Redis Streams replay baseline reaches full HA maturity and can serve as the operational system of record.

Current replay baseline:

- [`../filmu_py/core/replay.py`](../filmu_py/core/replay.py) provides a Redis Streams journal with event ids, tenant id, payload JSON, offset-based replay, and consumer-group create/read/ack primitives.
- That replay client now also emits durable subscriber delivery/ack/error observations into the control-plane ledger, and [`GET /api/v1/operations/control-plane/subscribers`](../filmu_py/api/routes/default.py) exposes those rows for operators.
- [`../filmu_py/core/event_bus.py`](../filmu_py/core/event_bus.py) can attach that replay journal while preserving process-local subscribers.
- `FILMU_PY_CONTROL_PLANE.event_backplane=redis_stream` enables the replay journal at startup.
- Redis Streams is the first durable baseline, not the final HA story; durable subscriber resume persistence, node coordination, and failover promotion remain the next depth layer.

## Alerting Baseline

Alert on:

- No logs shipped for a live node for more than two expected write intervals.
- Any SEV1 incident event.
- Queue `critical` state lasting longer than 15 minutes.
- VFS rollout readiness becoming `blocked`.
- Playback gate failure on protected-branch traffic.
- Plugin governance recommending quarantine for a critical plugin.
- Repeated auth policy denials for the same actor/tenant pair.

## Remaining Gaps

- The repo now owns a local collector/search stack, but environment promotion and secret management still need environment-specific rollout.
- Trace/span coverage is partial.
- Replay taxonomy has a Redis Streams plus subscriber-ledger baseline but still needs HA failover automation.
- Cross-node log-stream fanout is still not HA.
