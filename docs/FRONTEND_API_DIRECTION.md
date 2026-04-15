# Frontend API Direction

## Decision

Filmu Director is the new frontend direction for this project.

From this point forward, frontend product development is GraphQL-first.
The existing REST surface exists to keep the current compatibility frontend working during transition, but it is no longer the target architecture for new frontend feature work.

## Policy

The repo now follows these rules:

1. New frontend-facing product capabilities should be added to GraphQL first.
2. Existing REST endpoints should be treated as compatibility endpoints unless they are clearly operator or admin surfaces.
3. REST should not be expanded for new Filmu Director product flows unless there is a temporary migration blocker and no safe GraphQL path exists.
4. When the same domain capability exists in both surfaces, GraphQL is the source of truth for future shaping and richer typed projections.
5. REST compatibility routes may continue to exist during migration, but they should be narrowed, frozen, and removed gradually once Filmu Director no longer depends on them.

## Scope Split

### GraphQL

GraphQL is the long-term client contract for:

- library and item detail experiences
- calendar and release projections
- VFS catalog projections
- playback and recovery control-plane actions
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
- keep GraphQL naming intentional rather than inherited from legacy REST aliases
- preserve tenant and authorization rules when moving logic from REST to GraphQL
- treat REST deletions as phased cleanups after Director adoption, not as a one-shot rewrite

## Migration Plan

The migration should happen in waves:

1. Land first-class GraphQL projections for every Director screen.
2. Move new frontend work to GraphQL only.
3. Keep REST stable only where the old frontend still depends on it.
4. Remove compatibility REST routes once they have no active frontend consumer.

## Exit Criteria

The frontend API transition is considered complete when:

- Filmu Director no longer depends on compatibility REST routes
- product-facing frontend features have GraphQL-native coverage
- remaining REST endpoints are only operator/admin surfaces or are explicitly retired
