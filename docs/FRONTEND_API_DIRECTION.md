# Frontend API Direction

## Decision

Filmu Director is the new frontend direction for this project.

From this point forward, frontend product development is GraphQL-first.
The existing REST surface exists to keep the current compatibility frontend working during transition, but it is no longer the target architecture for new frontend feature work.
For Filmu Director work, treat GraphQL as the only product API surface to extend.
REST should be considered decommissioning-only unless an existing compatibility client would break without a temporary shim.

## Policy

The repo now follows these rules:

1. New frontend-facing product capabilities should be added to GraphQL first.
2. Existing REST endpoints should be treated as compatibility endpoints unless they are clearly operator or admin surfaces.
3. REST should not be expanded for new Filmu Director product flows unless there is a temporary migration blocker and no safe GraphQL path exists.
4. When the same domain capability exists in both surfaces, GraphQL is the source of truth for future shaping and richer typed projections.
5. REST compatibility routes may continue to exist during migration, but they should be narrowed, frozen, and removed gradually once Filmu Director no longer depends on them.
6. New Director runtime/governance posture should prefer shared service read-model builders consumed by GraphQL directly, not GraphQL adapters over REST route helpers.

## Scope Split

### GraphQL

GraphQL is the long-term client contract for:

- library and item detail experiences
- calendar and release projections
- VFS catalog projections
- screen-oriented VFS overview and browse projections, including breadcrumbs and file-focused directory context
- VFS search, sibling navigation, filtered directory listings, and screen-native browse/detail context
- file-focused VFS context, including directory ownership plus previous/next file navigation
- playback and recovery control-plane actions
- Director-facing operator posture read models, including control-plane recovery, replay-backplane proof, observability pipeline proof, plugin readiness, and retained evidence artifacts
- downloader orchestration, plugin events, plugin governance, and enterprise operations posture
- plugin-backed product capabilities
- future live frontend subscriptions

### REST

REST remains temporarily acceptable for:

- current compatibility frontend support
- operational and admin endpoints
- narrow migration shims where GraphQL parity is not yet landed

REST should be considered transitional for product-facing frontend work.

## Implementation Guidance

When implementing new frontend-facing work:

- prefer new GraphQL types over JSON-string placeholders
- prefer typed GraphQL projections over compatibility payload mirroring
- avoid adding new frontend-only REST routes
- when Director needs operator posture or runtime-readiness data, land it on GraphQL first and keep REST as a temporary shim only if an existing client still depends on it
- when Director needs gate-closure evidence, expose typed proof refs, contract-validation posture, soak posture, and pending-backlog visibility on GraphQL instead of pushing that logic into ad hoc frontend heuristics
- prefer browse/detail GraphQL projections that can serve both directory and selected-file screens from one query rather than forcing the frontend to stitch multiple compatibility endpoints together
- prefer graph-native search and sibling-navigation queries for browse/detail screens instead of frontends reconstructing local navigation state
- prefer typed proof/evidence artifacts on GraphQL instead of raw string arrays once a posture surface is part of Director runtime governance
- prefer filtered graph queries for operator consoles instead of shipping large unfiltered posture payloads and making the frontend post-process them
- prefer sharing one read-model builder across GraphQL and compatibility REST instead of duplicating governance logic in both surfaces
- when retiring REST-backed logic, move the source builder into services first and let GraphQL consume that builder directly; do not create new route-only shaping logic
- keep GraphQL naming intentional rather than inherited from legacy REST aliases
- preserve tenant and authorization rules when moving logic from REST to GraphQL
- treat REST deletions as phased cleanups after Director adoption, not as a one-shot rewrite

## Migration Plan

The migration should happen in waves:

1. Land first-class GraphQL projections for every Director screen.
2. Move new frontend work to GraphQL only.
3. Add file-focused browse/detail graph projections before attempting to remove any REST screen shims.
4. Replace GraphQL dependencies on compatibility route helpers with shared service builders.
5. Keep REST stable only where the old frontend still depends on it.
6. Remove compatibility REST routes once they have no active frontend consumer.

## Exit Criteria

The frontend API transition is considered complete when:

- Filmu Director no longer depends on compatibility REST routes
- product-facing frontend features have GraphQL-native coverage
- GraphQL no longer relies on REST route-local shaping for Director runtime/governance data
- Director runtime/governance screens consume typed graph evidence artifacts rather than compatibility-string payloads
- remaining REST endpoints are only operator/admin surfaces or are explicitly retired

## Working Ledger

The active GraphQL-first migration and gate-closure ledger lives in `docs/GRAPHQL_DIRECTOR_TODO.md`.
Update that file as Director GraphQL parity expands and compatibility REST shrinks.
