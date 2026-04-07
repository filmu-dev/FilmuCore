# Hybrid Event Backplane Research for `filmu-python`

## Goal

Evaluate whether this hybrid pattern can help `filmu-python` outperform the original TypeScript `riven` backend in durability, architecture, replayability, and operational capability:

```text
Producers -> NATS JetStream (source of truth, durable, replayable)
                      ↓
             Stream consumer bridge
                      ↓
            Redis Streams (hot cache, short retention, real-time workers)
```

This document treats **FilmuVFS as a required design constraint**.

That means any event architecture must support:

- the current frontend
- future better frontends
- HTTP range/HLS streaming
- a required first-class FilmuVFS mount/runtime path
- low-latency stream control-plane reactions without coupling byte transport to the event bus

---

## Short answer

**Yes, this is possible, and yes, it can help outperform `riven-ts`** — but only if it is used as a **control-plane and event-sourcing architecture**, not as a replacement for direct streaming I/O.

The hybrid can beat the current TypeScript architecture in these areas:

- durable event history and replay
- multi-consumer fan-out without overloading the hot path
- safer rebuild/recovery workflows
- better separation between durable truth and low-latency execution
- cleaner support for multiple frontends, workers, plugins, and FilmuVFS control signals

It will **not** automatically outperform `riven-ts` if:

- every event is duplicated into both systems without discipline
- ordering/idempotency rules are not explicit
- the VFS byte path is forced through the broker
- operational complexity is added before the product actually needs it

---

## Why the pattern is attractive

## 1. JetStream is strong as a source of truth

Current JetStream capabilities relevant to this design include:

- durable streams
- replayable consumers
- retention policies
- durable and ephemeral consumers
- explicit acknowledgements
- ordered consumption modes
- deduplication support on publish
- double-ack patterns for stronger delivery guarantees

This makes JetStream a good fit for:

- domain-event history
- auditability
- replay after bugs or schema fixes
- rebuilding projections/materialized views
- safer plugin/event evolution over time

In other words: JetStream is a good **system of record for events**.

---

## 2. Redis Streams are strong as a hot operational layer

Redis Streams are useful for:

- low-latency consumer groups
- bounded hot retention via `MAXLEN`
- near-term replay from stream IDs
- pending-entry inspection via `XPENDING`
- operational lag/consumer visibility via `XINFO GROUPS`

This makes Redis Streams a good fit for:

- real-time workers
- short-lived fan-out
- current-session notifications
- recent operational windows
- fast, bounded-memory consumption for the active system

In other words: Redis Streams are a good **hot execution/cache layer for recent events**.

---

## 3. The combination solves different problems cleanly

JetStream answers:

- What happened?
- Can we replay it later?
- Can we rebuild state from history?
- Can we audit plugin or workflow behavior?

Redis Streams answers:

- What is happening right now?
- How do workers react quickly?
- How do we keep recent event windows cheap and bounded?

That separation is exactly why the hybrid is appealing.

---

## How this could outperform `riven-ts`

The original TypeScript `riven` backend is strong in:

- BullMQ-based orchestration
- plugin hook workers
- native VFS path
- GraphQL-first composition

But its eventing/orchestration model is more tightly coupled to:

- runtime queue topology
- startup-time plugin wiring
- worker registration
- state-machine orchestration

The hybrid model can outperform it by adding capabilities that are difficult to get cleanly from queue-only orchestration:

### A. Better replay and recovery

With JetStream as durable truth:

- projections can be rebuilt
- stream indexes can be regenerated
- plugin failures can be replayed after fixes
- notification and VFS side effects can be recomputed from domain history

That is a major architectural advantage over a queue-first design.

### B. Better multi-surface support

`filmu-python` needs to support more than one consumer class:

- current frontend
- future frontend(s)
- worker pipelines
- plugin reactions
- metrics/materializers
- FilmuVFS control-plane subscribers

The hybrid pattern makes that easier because the source-of-truth event stream and hot-consumer layer are intentionally separated.

### C. Better operational isolation

If Redis is trimmed aggressively for hot processing, you do not lose the historical event record.

If JetStream is under replay/rebuild load, you do not need every real-time consumer to operate directly against long-retention storage.

That reduces blast radius.

### D. Better backend evolution path

This fits the current `filmu-python` direction well:

- FastAPI and Strawberry remain contract surfaces.
- ARQ can remain the short-job execution lane.
- Temporal can still become the durable workflow lane later.
- JetStream can become the domain event backbone.
- Redis Streams can remain the hot operational relay.

That gives a stronger long-term system than simply cloning `riven-ts` internals.

---

## FilmuVFS requirement

## Hard rule

**FilmuVFS must not depend on the event backbone for the byte-serving data path.**

The event architecture may coordinate VFS behavior, but it must not sit inline with file reads.

### Byte path must stay direct

FilmuVFS performance depends on:

- direct range reads
- chunk planning
- read-ahead
- cache hits
- low-latency cancellation
- minimal per-read coordination overhead

Those operations should remain in the direct streaming stack:

- HTTP range
- HLS pipeline
- FilmuVFS mount/FUSE path
- shared chunk/cache engine

### Event backbone should be control plane only

The hybrid pattern is valuable for FilmuVFS only if it handles things like:

- the Rust VFS sidecar communicating with the Python backend over a direct gRPC catalog channel (`WatchCatalog`-style), not over the event backplane
- stream-link resolution requests
- link-refresh and lease invalidation
- cache invalidation signals
- playback session lifecycle events
- mount/session metadata events
- stream failure telemetry
- provider backpressure and circuit-breaker updates
- analytics and debugging events

The gRPC catalog channel is the **data-plane supplier path** between Python and the Rust sidecar.
It is separate from and lower-level than the event backplane.
The event backplane stays above that boundary for control-plane coordination such as lease invalidation, cache invalidation signals, and session lifecycle events.

### Why this matters

If FilmuVFS uses the broker for every read decision or byte fetch, it will likely become slower than `riven-ts`.

If FilmuVFS uses the broker only for control-plane coordination, replay, and recovery, then the backend can surpass `riven-ts` in:

- resilience
- diagnosability
- rebuildability
- multi-consumer support

while keeping the actual read path fast.

---

## Recommended architecture shape

## 1. Durable domain events in JetStream

Examples:

- `filmu.item.requested`
- `filmu.item.indexed`
- `filmu.item.scraped`
- `filmu.item.downloaded`
- `filmu.item.completed`
- `filmu.item.failed`
- `filmu.stream.link.requested`
- `filmu.stream.link.refreshed`
- `filmu.vfs.session.started`
- `filmu.vfs.session.ended`
- `filmu.notification.completed`

These are authoritative and replayable.

## 2. Bridge selected events into Redis Streams

Only project the events that need hot, short-retention fan-out.

Examples:

- `stream:notifications`
- `stream:logging`
- `stream:vfs-control`
- `stream:worker-hot`

Use strict trimming and bounded retention.

## 3. Keep direct execution lanes where appropriate

- ARQ for hot short-lived work
- Temporal for long-running durable workflows
- direct HTTP/VFS read path for byte serving

The event backplane should connect these lanes, not replace them.

---

## Where the hybrid fits well

This design is a strong fit if Filmu is expected to have:

- multiple worker classes
- multiple UI consumers over time
- plugin ecosystem growth
- replay and audit requirements
- VFS + HTTP stream coexistence
- long-lived domain evolution where rebuilding projections matters

It is especially strong if the backend becomes a **platform**, not just a single app server.

---

## Risks and failure modes

## 1. Dual-write inconsistency

If producers write independently to JetStream and Redis Streams, the system becomes fragile.

### Required rule

JetStream must be the authoritative write.

The bridge writes to Redis Streams **only after** the JetStream publish is accepted.

---

## 2. Duplicate deliveries

The bridge can re-deliver on retries, consumer restarts, or replay.

### Required mitigation

All downstream consumers must be idempotent.

Use:

- event IDs
- item IDs + version counters
- replay-aware projections
- de-dup windows where necessary

---

## 3. Ordering assumptions

Global ordering is the wrong expectation.

### Required mitigation

Define ordering only where it matters:

- per item
- per playback session
- per stream-link lease

Partition subjects/keys accordingly.

---

## 4. Operational complexity

This adds another serious system to operate.

You now have to manage:

- NATS/JetStream cluster health
- Redis health and trimming behavior
- bridge lag
- replay semantics
- dead-letter or poison-event handling

So this architecture should be adopted deliberately, not because it sounds elegant.

---

## 5. Replay storms

If JetStream replay is pointed directly at hot consumers, Redis and worker fleets can be flooded.

### Required mitigation

The bridge needs:

- replay throttling
- subject filtering
- consumer isolation
- backpressure controls
- separate rebuild consumers where needed

---

## Recommended guardrails

If this is adopted, these should be mandatory:

1. JetStream is the only authoritative event write path.
2. Redis Streams are explicitly short-retention and disposable.
3. Consumers are idempotent by design.
4. Event schemas are versioned.
5. Replay paths are separated from hot production paths.
6. FilmuVFS read/byte transport stays off the broker.
7. Stream-link and cache invalidation events are treated as first-class VFS control-plane events.
8. Lag, pending, redelivery, replay, and trim metrics are mandatory.

---

## Performance assessment

## Where the hybrid can improve performance

- less Redis memory pressure from long retention
- faster hot-consumer loops when Redis only carries recent events
- replay and rebuild work moved off the hot path
- lower coupling between audit/history and real-time consumers
- better scaling separation between durable and low-latency workloads

## Where it can hurt performance

- extra hop through a bridge
- more serialization/deserialization
- more infra round trips
- more operational tuning burden

### Net conclusion on performance

The hybrid improves **system-level performance and scalability** when the platform has enough event volume and enough consumer diversity.

It does **not** improve single-message latency automatically.

It wins by improving:

- isolation
- replayability
- retention economics
- rebuild safety
- multi-consumer architecture

---

## Recommendation for `filmu-python`

## Adopt only under these conditions

Use this hybrid if Filmu is intentionally evolving into:

- a durable event-driven platform
- a multi-frontend backend
- a richer plugin/runtime ecosystem
- a FilmuVFS + HTTP streaming platform with strong observability and replay needs

## Do not adopt yet if

- the current system still lacks core compatibility surfaces
- worker semantics are not yet stable
- event schemas are still too immature
- the team is not ready to operate both NATS and Redis correctly

---

## Best adoption path

### Phase 1

- keep current API + ARQ foundations
- define domain event taxonomy
- standardize event IDs and schema versioning

### Phase 2

- add JetStream as source of truth for domain/control-plane events
- do not change byte-serving path yet

### Phase 3

- add bridge into Redis Streams for selected hot subjects only
- start with logs, notifications, and VFS control-plane events

### Phase 4

- add projection rebuilds, replay tools, and plugin event consumers

### Phase 5

- integrate with FilmuVFS control-plane workflows
- keep HTTP range/HLS and FilmuVFS reads direct

---

## Final judgment

**Yes, the hybrid NATS JetStream -> Redis Streams pattern is possible and can help `filmu-python` outperform `riven-ts`.**

But the win comes from using it correctly:

- JetStream for durable truth
- Redis Streams for hot bounded fan-out
- ARQ/Temporal for execution
- FilmuVFS as a hard requirement with broker usage limited to the control plane

If implemented with those boundaries, this architecture can surpass `riven-ts` in:

- replayability
- auditability
- multi-consumer support
- backend evolvability
- platform-level reliability

If implemented without those boundaries, it can easily become more complex than the problem justifies.
