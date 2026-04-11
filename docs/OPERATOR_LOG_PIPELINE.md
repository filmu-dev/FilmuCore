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
- `/api/v1/operations/governance` reports structured log posture and OTLP configuration state.

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

These event classes are a contract for future durable replay streams. Until a replayable event backend exists, structured logs are forensic evidence, not the system of record.

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

- Logs are not yet shipped by this service.
- Search backend provisioning is external.
- Trace/span coverage is partial.
- Replay taxonomy is documented but not backed by a durable stream.
- Cross-node log-stream fanout is still not HA.
