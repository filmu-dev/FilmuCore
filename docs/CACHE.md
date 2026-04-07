# Cache Strategy

## Purpose

Reduce repeated provider requests, improve tail latency, and absorb temporary upstream instability.

## Model

Two-layer cache:

1. **Local in-process TTL cache**
   - Fast path for repeated access on the same worker.
2. **Redis cache**
   - Shared across workers/instances.

Implemented primitive: [`CacheManager`](../filmu_py/core/cache.py).

## Correctness model

Cache is an optimisation layer, not an authority.

Authoritative sources remain:

- persisted application state in the database
- explicit runtime state machines and worker transitions
- upstream providers for external metadata and provider-owned objects

The cache must never become the only place where correctness lives.

## Rules

- Cache only deterministic responses.
- Use short TTLs for volatile metadata.
- Keep provider-specific keys namespaced, e.g. `provider:resource:id`.
- Never cache auth secrets.

## Where stale-while-revalidate is appropriate

Safe candidates:

- read-mostly provider metadata
- slow external lookups where brief staleness is acceptable
- derived read models that are not authoritative for workflow transitions

Unsafe candidates:

- state-transition decisions
- active playback link leases
- mutation acknowledgements
- anything that can cause duplicate or invalid worker actions if stale

## Invalidation constraints

Selective invalidation on state transitions is required because the same item may appear in:

- item-detail views
- item-list projections
- stats aggregates
- stream availability views
- plugin/materialized read models later on

That means invalidation needs to be driven by explicit transition semantics, not generic cache clears.

Recommended direction:

1. invalidate by domain object and projection type
2. invalidate on committed transitions, not speculative ones
3. keep invalidation idempotent
4. prefer small targeted invalidations over global clears

## Observability requirements

Cache correctness is load-bearing for provider rate limits and streaming behavior.

So the cache layer should expose at least:

- hit/miss counts
- local vs shared cache hit ratios
- stale serve counts
- invalidation counts by reason
- revalidation latency

## Interaction with orchestration and future event backplane

As orchestration matures:

- ARQ/Temporal transitions should become the trigger points for targeted invalidation
- any future durable event backbone may carry invalidation/control-plane events
- FilmuVFS and stream paths must still treat cached stream/link state carefully, because stale playback state is more damaging than stale metadata

## Next steps

- Add cache hit/miss metrics.
- Add stale-while-revalidate policy for expensive provider lookups.
- Add selective invalidation on state transitions.
