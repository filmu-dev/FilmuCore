# Operations Program

## Purpose

This runbook defines the minimum enterprise operations program for FilmuCore. It complements the proof scripts and the operator APIs by making SLOs, rollback, disaster recovery, incident handling, rollout policy, and capacity review explicit.

## Service Objectives

Initial production objectives:

- API availability: 99.5% monthly for authenticated control-plane routes.
- Playback proof availability: playback gate must stay green for protected-branch merges.
- Queue health: no sustained critical queue alert for more than 15 minutes during normal provider availability.
- VFS data plane: no fatal mounted-read failures during promoted soak profiles.
- Recovery objective: restore database plus settings to a usable control-plane state within 4 hours after operator-confirmed data loss.

Initial error-budget rules:

- Freeze non-critical feature merges when playback gate or required verify checks are red on `main`.
- Freeze playback/VFS rollout promotion when `vfs_runtime_rollout_readiness` is `blocked`.
- Treat repeated queue `critical` history points as release-blocking until replay/recovery evidence exists.

## Rollback Policy

Rollback is preferred over hot patching when:

- Required CI checks are red after merge.
- Playback gate promotion breaks protected-branch proof.
- Migration or settings changes make API-key/OIDC access unrecoverable.
- VFS runtime status reports blocking mounted-read or cache-write failures after deployment.

Rollback steps:

1. Stop new rollout promotion and mark the incident owner.
2. Capture `/api/v1/operations/governance`, `/api/v1/auth/policy`, `/api/v1/workers/queue/history`, and `/api/v1/stream/status`.
3. Revert or redeploy the last known-good artifact.
4. Re-run required verify checks plus playback gate.
5. Record the root cause and add a regression test or proof threshold before re-promoting.

## Backup And Restore

Minimum backup scope:

- Postgres database.
- Runtime settings payload.
- Plugin trust store and plugin manifest sources.
- Playback/VFS proof artifacts needed for compliance evidence.
- Structured log archives for the retained incident window.

Restore proof cadence:

- Development: ad hoc before risky schema work.
- Staging: at least monthly.
- Production: before major schema migrations and after backup tooling changes.

Restore success criteria:

- App starts with migrations applied.
- `/api/v1/health` is healthy or has only documented degraded dependencies.
- `/api/v1/auth/context` resolves the operator/service account.
- `/api/v1/workers/queue` can read queue posture.
- A playback proof can run against an existing completed item or a seeded fixture.

## Incident Runbook

Severity classes:

- SEV1: playback unavailable for promoted paths, data loss, auth lockout, or unrecoverable VFS mount failures.
- SEV2: worker backlog stuck, provider-wide failed refreshes, repeated playback proof failures, or plugin governance rejecting critical built-ins.
- SEV3: degraded observability, isolated provider failures, non-critical plugin failures, or documentation/proof drift.

Incident flow:

1. Assign one incident owner.
2. Capture the governance surfaces listed in rollback policy.
3. Classify user impact and tenant scope.
4. Stabilize first: rollback, disable unsafe plugin, pause rollout, or reduce queue pressure.
5. Preserve evidence in proof/log artifacts.
6. Close only after a regression test, proof threshold, or runbook update lands.

## Canary And Rollout Policy

Promotion sequence:

1. Local focused tests and lint pass.
2. Required Python/Rust verify checks pass.
3. Playback gate is green.
4. `/api/v1/operations/governance` shows no `blocked` slice.
5. VFS rollout readiness is not `blocked`.
6. One environment class is promoted at a time.

Abort rollout when:

- Playback gate fails.
- Queue alert is `critical`.
- VFS rollout readiness is `blocked`.
- New plugin governance recommends quarantine for a critical plugin.
- Structured logs show repeated auth lockout or migration failures.

## Capacity Review

Review at least monthly or before major usage expansion:

- Queue depth, ready-job age, retry/dead-letter trend.
- API latency and error metrics.
- Playback/VFS proof duration and failure reasons.
- VFS cache hit ratio, fallback success ratio, prefetch pressure, and mounted-read errors.
- Redis/Postgres storage and connection limits.
- Structured log volume and retention.

The current machine-readable summary is exposed at `/api/v1/operations/governance`.
