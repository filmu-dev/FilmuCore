# FilmuVFS Enterprise Architecture & Vanguardist Improvements

> Historical research note
>
> This document is a research snapshot, not the canonical current-status document.
> Several limitations discussed below were valid at the time of writing but have since been addressed in the implementation.
> For current FilmuVFS status and active next steps, use [`STATUS.md`](STATUS.md), [`EXECUTION_PLAN.md`](EXECUTION_PLAN.md), and the active planning matrices under [`TODOS/`](TODOS).

## 1. Current Architecture Audit
The current FilmuVFS (Rust) is a well-structured, async-first implementation tailored for streaming over HTTP.
- **FUSE Layer**: Uses `fuse3` to bridge kernel requests into Tokio async tasks. Keeps states of handles and mappings to the gRPC Catalog.
- **Upstream Layer**: [UpstreamReader](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/upstream.rs#97-100) uses `hyper` to send exact [Range](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/upstream.rs#11-16) requests to CDNs.
- **Chunk Engine**: [ChunkPlanner](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/chunk_planner.rs#92-95) intelligently splits reads into configurable chunk sizes depending on the pattern (`HeaderScan`, `SequentialScan`, `TailProbe`).
- **Caching**: Uses `moka::sync::Cache` for in-memory L1 caching of downloaded chunks (default 500MB).
- **Prefetching**: Uses a simple semaphore-bounded background task system to prefetch the next `N` sequential chunks.

## 2. Identified Bottlenecks & Limitations
While highly functional, the current system lacks attributes of "enterprise-grade" CDNs and large-scale video delivery networks:
1. **Memory-Only Caching Strategy**: Streaming 4K Remux files (40GB-100GB) rapidly saturates the 500MB `moka` cache. Scrubbing back in a video causes expensive re-downloads from the external CDN, wasting bandwidth and introducing buffering.
2. **Synchronous Cache in Async Context**: Using `moka::sync::Cache` in Tokio worker threads can block the async executor during heavy cache evictions or lock contention, leading to latency spikes in FUSE [read](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/chunk_engine.rs#159-226) ops.
3. **Static Prefetching**: Prefetching is fixed (e.g., prefetch next 2 chunks). It doesn't adapt to the user's available bandwidth or the player's read velocity.
4. **Resilience Gaps**: FUSE operations immediately fail (`EIO`) if a connection drops or upstream returns a 5xx. If a link expires (`ESTALE`), there is no inline seamless refresh.

## 3. Vanguardist / Enterprise Grade Improvements

### A. Async Multi-Tiered Hybrid Caching (L1 + Opt-in L2)
**Concept**: Integrate an optional Tiered Storage Cache. Fast, small memory cache (L1) as the primary engine, with a configurable, persistent Disk cache (L2) for users with available NVMe/SSD space.
- **Implementation**: Migrate from `moka::sync` to an async-aware caching architecture using `moka::future` for L1. Provide an optional Disk LRU layer (e.g., using `foyer` or a custom implementation) that can be enabled via configuration.
- **Benefit**: Ensures a perfectly optimized, low-memory footprint for standard end-users running on typical hardware. For power users, enabling the L2 disk cache retains blocks of watched video on local disk, serving stream restarts or heavy scrubbing instantly without re-fetching from the external CDN.

### B. Adaptive Velocity-Based Prefetching
**Concept**: Evolve [PrefetchScheduler](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/prefetch.rs#17-21) from a static semaphore to a dynamic, velocity-driven engine.
- **Implementation**: Track FUSE read speed (bytes/sec) per [handle](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/catalog/client.rs#198-227). If the player reads sequentially fast, exponentially increase the prefetch window and concurrency (similar to TCP Slow Start) to fully saturate the WAN link and fill the L1/L2 caches ahead of time. Scale down on irregular reads to save bandwidth.

### C. Seamless Inline Link Refresh & Circuit Breaking
**Concept**: Enterprise systems hide upstream volatility from the client.
- **Implementation**: Wrap `UpstreamReader::fetch_range` with an asynchronous retry layer (e.g., `reqwest-retry` backoff logic). If Real-Debrid throws a 401/403 (Stale Link), instead of returning `ESTALE` to FUSE, the Rust sidecar should `await` a gRPC call to Python (`RefreshCatalogEntry`), get the new URL, and retry the chunk fetch *without* dropping the FUSE file descriptor. The media player will just see a slight latency bump, not a stream crash.

### D. Zero-Copy & `io_uring` Future-Proofing
**Concept**: Extreme high-throughput FUSE.
- **Implementation**: Currently FUSE copies data between kernel and userspace. Preparing the internal byte bridging to utilize `io_uring` (via updated FUSE kernels or alternative bindings) and avoiding `BytesMut` allocations where possible will reduce CPU overhead during heavy concurrently streams.

### E. Extensibility: Trait-based Modular Source Code over Plugins
**Concept**: Allowing the VFS to be extended with custom backends (like the Opt-in L2) or modular upstream providers.
- **Design Decision**: Instead of relying on dynamic plugins (`.so`/`.dll` loading) or WebAssembly modules—which introduce significant overhead on the hot-path, unsafe memory boundaries, and complex distribution for end-users—FilmuVFS should employ a **Trait-based architecture** compiled directly into the monolithic sidecar binary.
- **Implementation**: Define core Rust Traits like `CacheEngine` and `UpstreamProvider`. Both the `MemoryCache` and `HybridDiskCache` implementations are compiled in the source. At startup, the sidecar reads the user's config file and injects the requested cache trait object into the VFS core.
- **Benefit**: Retains maximum bare-metal Rust performance (zero-cost abstractions) while providing modular, opt-in features without requiring end-users to manage a confusing ecosystem of plugin files.

## 4. Rigorous Deep Audit Findings (No Shortcuts)
A line-by-line architectural analysis revealed several critical engineering limitations that must be addressed for enterprise stability:

### FUSE to gRPC Translation Gaps
- **Expensive Reconnections ([vfs_server.py](file:///e:/Dev/Filmu/FilmuCore/filmu_py/services/vfs_server.py) & [client.rs](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/catalog/client.rs))**: When the Rust sidecar reconnects (e.g., due to a temporary network blip or backend restart), [client.rs](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/catalog/client.rs) sends `last_applied_generation_id`. However, `vfs_server.py:FilmuVfsCatalogGrpcServicer.WatchCatalog` ignores this value and *always* generates a full snapshot (`await self._supplier.build_snapshot()`) instead of a compute-cheap delta. This causes massive CPU spikes on the Python backend if multiple edge nodes reconnect simultaneously with large catalogs.
- **Missing Inline Refresh Trigger ([mount.rs](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/mount.rs) & [upstream.rs](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/upstream.rs))**: If an upstream provider (Real-Debrid) returns a 401/403/410, [UpstreamReader](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/upstream.rs#97-100) correctly throws `StaleStatus`. [mount.rs](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/mount.rs) returns `ESTALE` to the OS, breaking the user's video playback instantly. There is no mechanism in `filmuvfs` to signal the Python backend to renew the link. For enterprise playback, [mount.rs](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/mount.rs) should pause the FUSE [read()](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/chunk_engine.rs#159-226), trigger a refresh via gRPC, wait for the [CatalogDelta](file:///e:/Dev/Filmu/FilmuCore/proto/filmuvfs/catalog/v1/catalog.proto#55-62) event containing the new URL, and automatically retry the read.

### FUSE Concurrency & Inode Management
- **Deterministic 64-bit Inode Collisions ([state.rs](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/catalog/state.rs))**: Inodes are computed using a 64-bit FNV-1a hash (`fn inode_for_entry_id`). While FNV is fast, a 64-bit hash space will probabilistically collide (the Birthday Paradox) when the catalog size breaches millions of entries. If a collision occurs, `CatalogStateInner::build_state` throws `DuplicateInode` and crashes the entire catalog sync. An enterprise VFS must either use a perfect hash, a wider hash (128-bit Murmur3 hashed down with a tie-breaker table), or a persistent central ID allocator in the SQLite/Postgres backend exported via the Proto schema.
- **Thread Blocking ([chunk_engine.rs](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/chunk_engine.rs))**: [load_chunk_foreground](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/chunk_engine.rs#227-246) accesses a synchronous `moka::sync::Cache` within a `tokio::spawn` asynchronous context. Under heavy eviction pressure, this blocks the Tokio worker thread, severely limiting concurrent streaming capability across multiple files.

### Streaming Pipeline Precision
- **Sub-optimal Chunk Arithmetic ([chunk_planner.rs](file:///e:/Dev/Filmu/FilmuCore/rust/filmuvfs/src/chunk_planner.rs))**: The planner aligns read chunks to predefined block sizes but lacks logic to eagerly prefetch the moov atom (metadata) positioned at the end of MP4 files, resulting in unnecessary sequential scans over the network instead of targeted random reads for the footer.

## 5. Summary
To elevate FilmuVFS to an enterprise-grade Vanguardist technology, the next immediate steps for development are:
1. Swap `moka::sync` for `moka::future` to prevent async blocking.
2. Build an **Optional Tier 2 Disk Cache** tightly coupled with the in-memory chunks, allowing users to leverage local storage for better scrubbing without imposing strict SSD requirements.
3. Implement **Adaptive Prefetching** and **Inline Link Refreshes** for bulletproof resiliency.
