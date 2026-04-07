# Rate Limiting Strategy

## Objective

Increase throughput without triggering debrid/provider abuse controls.

## Distributed token bucket

Implemented primitive: [`DistributedRateLimiter`](../filmu_py/core/rate_limiter.py).

- Uses Redis Lua for atomic token-bucket updates.
- Supports global multi-worker coordination.
- Returns allow/deny + remaining token count.

## Bucketing model

Recommended key shape:

- `ratelimit:{provider}:{endpoint}`

Controls:

- Capacity = provider burst allowance
- Refill rate = sustained provider quota per second

## Adaptive behavior and design direction

Recommended behavior:

1. **Header-aware cooldowns**
   - If a provider returns `Retry-After`, promote that into a temporary cooldown window for the affected bucket.
   - Cooldowns should be bucket-specific, not global unless the provider failure mode is clearly global.

2. **Bucket separation by operation class**
   - Keep metadata lookups, scrape/index operations, and stream-link refresh operations on different logical buckets.
   - Example shapes:
   - `ratelimit:{provider}:metadata`
   - `ratelimit:{provider}:scrape`
   - `ratelimit:{provider}:stream_link_refresh`
   - `ratelimit:{provider}:download`

3. **Pressure-aware refill tuning**
   - Refill rates should only move downward automatically when the system observes repeated denials or quota exhaustion.
   - Recovery upward should be slower than backoff downward to avoid oscillation.

4. **Single-flight for expensive refreshes**
   - Concurrent identical refresh attempts should collapse into one upstream refresh where possible.
   - This matters most for active playback link refreshes and provider metadata hot spots.

## Stream-link pressure and FilmuVFS

FilmuVFS and direct stream playback introduce a stricter requirement than ordinary metadata traffic.

When a playback link is close to expiry:

- link refresh should use a dedicated limiter bucket
- active playback refreshes should be treated as higher-priority control-plane operations than background metadata fetches
- the system should avoid synchronized refresh storms across many active sessions

Recommended behavior under limiter denial for link refresh:

1. if an existing valid lease still exists, continue serving it
2. if refresh is denied, emit explicit metrics/logging for playback-risk pressure
3. if refresh cannot be obtained in time, fail clearly and count it as a stream-control failure, not a generic metadata miss

This is one of the main places where rate limiting, cache correctness, and FilmuVFS reliability intersect.

## Safety rules

- Never bypass limiter for production provider calls.
- Use jitter in retries to avoid synchronized spikes.
- Track denied calls as a first-class SLO metric.
- Treat stream-link refresh denials as a separate operational signal from background metadata denials.

## Current implementation notes

- The playback refresh path already uses provider-specific stream-link refresh buckets via [`ratelimit:{provider}:stream_link_refresh`](../filmu_py/services/playback.py).
- The inline direct-play lease resolver in [`LinkResolver`](../filmu_py/services/playback.py) now uses that exact `ratelimit:{provider}:stream_link_refresh` bucket before any provider unrestrict/download-refresh call is attempted for [`/api/v1/stream/file/{item_id}`](../filmu_py/api/routes/stream.py).
- When the limiter denies an inline refresh, [`LinkResolver`](../filmu_py/services/playback.py) now serves the currently cached lease if it is still usable; otherwise it emits the existing `playback_risk` signal and fails closed with `503`, preventing thundering-herd refresh storms against debrid providers.
- Provider-backed inline playback refreshes now also run behind a small per-provider circuit breaker in [`ProviderCircuitBreaker`](../filmu_py/services/playback.py), and circuit-open failures increment [`PROVIDER_CIRCUIT_OPEN_EVENTS`](../filmu_py/services/playback.py) instead of repeatedly hammering a degraded upstream.
- The direct-file route in [`filmu_py/api/routes/stream.py`](../filmu_py/api/routes/stream.py) now performs a short HEAD reachability probe for provider-backed `remote-direct` media-entry winners before proxying bytes. Failed probes trigger one resolver-managed refresh attempt and otherwise return `503` with `Retry-After: 10` so the frontend keeps a stable failure contract while avoiding obviously broken stream handoff.
- The HLS playlist and generated-child routes in [`filmu_py/api/routes/stream.py`](../filmu_py/api/routes/stream.py) now reuse that same inline resolver path when a `media-entry`-backed `remote-direct` winner is about to be used as the ffmpeg transcode input, so the transcode path gets the same limiter bucket, circuit-breaker policy, cached-lease refresh semantics, and `Retry-After: 10` failure contract instead of bypassing provider-pressure governance.
- The downloader/debrid client layer now also uses separate provider-specific download buckets via [`ratelimit:{provider}:download`](../filmu_py/services/debrid.py) for magnet intake, torrent-info polling, file selection, and download-link resolution.
