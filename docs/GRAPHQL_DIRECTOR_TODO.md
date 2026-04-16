# GraphQL Director TODO

## Status

Filmu Director is GraphQL-first.
REST compatibility remains transitional and should only be preserved long enough to avoid breaking the legacy frontend during migration.
New product-facing runtime, governance, browse, search, and readiness work should land on GraphQL-backed read models first.

## Landed In Repo

- Typed GraphQL observability convergence with explicit pipeline-stage, proof-artifact, and missing-field posture.
- Typed GraphQL control-plane posture for summary, automation, replay-backplane, subscriber, and recovery-readiness views.
- Typed GraphQL plugin integration readiness with retained contract-proof and soak-proof posture for Comet, Seerr, Listrr, and Plex.
- Typed GraphQL VFS browse/search/detail shaping, including file-focused context, breadcrumb-ready navigation, blocked-item filters, and screen-oriented overview projections.
- Shared service builders for operator posture so GraphQL does not depend on route-local shaping as REST is decommissioned.

## Next GraphQL Slices

1. Move the remaining Director screens that still rely on compatibility REST browse/detail payloads onto `vfsOverview`, `vfsSearch`, `vfsFileContext`, and the typed blocked-item graph.
2. Add GraphQL-native calendar/detail/library projections for any remaining Director screens that still depend on compatibility route shaping.
3. Expose GraphQL-native downloader execution history and provider failover evidence so Director can inspect orchestration results without REST.
4. Add GraphQL-native plugin runtime/event feeds that summarize recent plugin activity, health, and retry posture.
5. Add GraphQL-native governance evidence for rollout gates, playback proof posture, and retained operational artifacts.
6. Add GraphQL-native search facets and selection metadata so Director can drive richer browse/search UX without route-specific adapters.
7. Expand VFS graph projections for mount-specific diagnostics and catalog delta visibility needed by Director operator screens.
8. Add GraphQL-native trace/export posture summaries for Python -> gRPC -> Rust observability rollout evidence.
9. Add GraphQL-native control-plane replay ergonomics that expose pending claim posture, stale ownership transfer posture, and retained recovery evidence cleanly.
10. Remove GraphQL dependencies on any remaining REST helper imports by promoting shared read-model builders into services first.

## Gate-Closure Work Still Outstanding

### In Repo

- Finish broader Director adoption of the GraphQL-native VFS browse/detail/search projections.
- Add the remaining GraphQL-native governance, downloader-runtime, and plugin-runtime views that still only exist through compatibility shims or partial posture builders.
- Continue replacing any route-local shaping with shared service read models directly consumed by GraphQL.

### Production Proof Required

- End-to-end observability is not gate-closed until real production proof exists for Python -> gRPC -> Rust trace propagation, OTLP export, search indexing, and alert rollout.
- Control-plane recovery is not gate-closed until retained evidence exists from live Redis consumer groups showing replay, pending-claim, and recovery flows working in production.
- Plugin breadth is not gate-closed until Seerr, Listrr, Comet, and Plex each have retained real-environment contract evidence and soak evidence.
- Downloader orchestration is not gate-closed until live provider-fleet proof confirms failover and multi-provider posture under real load.

### Outside The Repo

- Protected-branch enforcement.
- Runner readiness and CI environment proof.
- Authenticated production evidence collection and retention.

## Decommissioning Policy

- Do not add new frontend-facing REST routes for Filmu Director.
- Keep compatibility REST stable only where the legacy frontend still depends on it.
- Prefer deleting REST after GraphQL parity is proven screen by screen, not by one large rewrite.
