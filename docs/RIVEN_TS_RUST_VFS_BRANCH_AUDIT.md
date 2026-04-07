# `riven-ts` `feat/rust-vfs` Branch Audit

## Scope

This document captures a March 2026 source audit of the upstream [`rivenmedia/riven-ts`](https://github.com/rivenmedia/riven-ts/tree/feat/rust-vfs) `feat/rust-vfs` branch at commit `f2ec9ce417279cbd5c31663e00afd1e867c5e05b`, compared against the current local TypeScript backend at `E:/Dev/Triven_riven-fork/Triven_backend - ts`.

Branch-specific scope note:

- this audit remains about the separate `feat/rust-vfs` branch
- current upstream `main` has since advanced to `e64604a` without merging this daemon topology
- see [`RIVEN_TS_UPSTREAM_DELTA_2026_04_06.md`](RIVEN_TS_UPSTREAM_DELTA_2026_04_06.md) for the verified current-main delta

This is a **static source audit**, not an end-to-end runtime certification. The goal is to understand what the branch changes architecturally, what it currently regresses relative to the local backend, and what Filmu should adopt or reject from it.

## Executive summary

The upstream branch is a **meaningful architecture pivot**, but it is **not a feature-complete superset** of the current local TS backend.

What it gets right:

- moves the FUSE mount responsibility out of the Node.js process and into a Rust daemon
- introduces a typed gRPC contract for file streaming and catalog updates
- creates a cleaner control-plane / data-plane boundary than the embedded local VFS design

What it gets wrong today:

- removes the mature in-process TS VFS stack before equivalent behavior has been reintroduced
- ships a backend gRPC server that is still largely placeholder logic
- currently exposes a much flatter and thinner filesystem model than the local backend
- contains material documentation/configuration drift that overstates readiness

The correct Filmu takeaway is:

1. **Copy the architectural direction** where it is strong: explicit protocol, daemon boundary, hot catalog updates.
2. **Do not copy the current behavior regression**: flat root-only catalog, placeholder streaming, config drift, and removal of mature read heuristics without replacement.

## Verified structural delta

| Area | Upstream `feat/rust-vfs` | Current local TS backend | Filmu implication |
| --- | --- | --- | --- |
| Mount topology | Rust daemon under `apps/vfs-daemon` mounts FUSE and talks to Node over gRPC | Node process mounts FUSE directly with `@zkochan/fuse-native` | The daemon boundary is promising and worth studying |
| App dependency boundary | `apps/riven/package.json` adds `@repo/feature-vfs`, `nice-grpc`, `@grpc/grpc-js`, `@grpc/proto-loader`; removes `@zkochan/fuse-native` | `apps/riven/package.json` still depends on `@zkochan/fuse-native` and embedded VFS helpers | This is a genuine runtime topology change, not just a refactor |
| Backend lifecycle | `initialise-vfs.actor.ts` starts a gRPC server; `shutdown-vfs.actor.ts` shuts it down | `initialise-vfs.actor.ts` mounts FUSE; `unmount-vfs.actor.ts` unmounts it | Filmu can use the same lifecycle split concept |
| VFS contract | New `packages/feature-vfs` package defines protobuf + TS/Rust generated surface | No separate transport contract; VFS behavior is in-process function calls | Explicit contracts are worth borrowing |
| Filesystem model | Rust daemon currently exposes a flat root with catalog-backed files | Current TS backend has path parsing for `/movies` / `/shows` and richer directory semantics | The upstream branch is currently less capable for media browsing |
| Read path | One gRPC `StreamFile` call per read, fixed chunk-size input, minimal handle lifecycle | Current TS backend has read-type detection, chunk planning, chunk cache, body-read stitching, hidden-path handling | The upstream branch is currently a behavioral regression |
| Catalog updates | Bidirectional `WatchCatalog` stream with subscribe/ack semantics | Local TS backend resolves VFS state from app-local runtime and DB-driven helpers | The branch introduces a strong control-plane idea that Filmu should keep in mind |
| Documentation integrity | Quick-start and daemon docs claim options and readiness not reflected by code | Local backend docs are less ambitious but closer to the real implementation shape | Filmu docs should explicitly flag the branch as exploratory rather than parity-ready |

## Verified source findings

### 1. The branch introduces a real backend/daemon contract

The new `packages/feature-vfs` protobuf defines two core RPCs:

- `StreamFile` — server-side streaming of file bytes
- `WatchCatalog` — bidirectional stream for daemon subscribe/ack and server-pushed catalog updates

That is the strongest design improvement in the branch. It creates a real transport boundary instead of baking FUSE behavior straight into the backend process.

The contract already models useful primitives:

- byte-range-ish streaming inputs (`url`, `offset`, `length`, `chunk_size`, `timeout_seconds`)
- file metadata via `FileEntry`
- daemon identity and update acknowledgement via `SubscribeCommand` / `AckCommand`

That is a better architectural seam than the local backend's current in-process coupling.

### 2. The backend gRPC server is still placeholder-grade

The new backend implementation under `apps/riven/lib/grpc-vfs-server.ts` is **not yet equivalent to the local backend's production VFS behavior**.

Current behavior includes:

- `streamFile(...)` sleeping for one second and then yielding a fixed `Hello, world!` buffer rather than real media bytes
- `watchCatalog(...)` generating random file additions/removals instead of projecting real catalog state
- a hard-coded server bind on `localhost:50051`

This means the branch currently has a strong *shape* but a weak *implementation*.

### 3. The Rust daemon is real, but still thin compared to the local TS VFS

The daemon side under `apps/vfs-daemon` is not imaginary. It does mount FUSE and implements `lookup`, `getattr`, `readdir`, `open`, `read`, and `release`.

However, the current implementation is much thinner than the local TS backend:

- directory listing is root-only today
- the source still contains `TODO: Enable directories other than root`
- `read(...)` issues a single gRPC `stream_file(url, offset, length, 1048576)` call for the requested window
- there is no reintroduced equivalent of the local TS chunk planner, cache, scan heuristics, or media-aware path model

So the branch does not merely move the same behavior into Rust. It currently **resets the filesystem layer to a simpler baseline**.

### 4. The current local TS backend remains substantially more mature in VFS behavior

Compared to the upstream branch, the local backend still has the more complete media-serving filesystem behavior today.

Verified local strengths include:

- embedded FUSE mount lifecycle via `initialise-vfs.actor.ts` and `unmount-vfs.actor.ts`
- explicit VFS operation wiring in `apps/riven/lib/vfs/index.ts`
- read-type classification in `detect-read-type.ts`
- chunk planning in `calculate-file-chunks.ts`
- cache-aware body-read stitching in `perform-body-read.ts`
- path parsing for `/movies` and `/shows` in `path-info.schema.ts`

That means the remote branch should be understood as an **architectural experiment** rather than a clean upgrade over the current local backend.

### 5. The branch documentation materially overclaims current readiness

This is the most important adversarial finding.

The branch-level `QUICK_START.md` states that the implementation is "Complete and ready to use" and documents:

- `ENABLE_GRPC_VFS`
- `GRPC_VFS_SERVER_ADDR`
- `GRPC_VFS_CHUNK_SIZE`
- daemon flags such as `--catalog`, `--allow-other`, and `--auto-unmount`

But the currently audited code shows:

- the backend server binds to `localhost:50051` directly rather than reading the documented env vars
- `apps/vfs-daemon/src/main.rs` only parses `mountpoint` and `--grpc-server`
- the daemon docs still reference `packages/proto-vfs`, while the actual package is `packages/feature-vfs`

In other words, the branch documentation is describing the **intended system**, not the **verified system**.

## Regression matrix: upstream branch vs current local backend

| Capability | Upstream `feat/rust-vfs` | Current local TS backend | Assessment |
| --- | --- | --- | --- |
| Separate daemon process | ✅ Yes | ❌ No | Upstream win |
| Explicit transport contract | ✅ Yes | ❌ No | Upstream win |
| Real mount lifecycle in app | 🔶 gRPC server lifecycle only | ✅ Embedded mount/unmount lifecycle | Mixed |
| Hierarchical media paths | ❌ Flat root catalog | ✅ Movies/shows path parsing | Local win |
| Read heuristics | ❌ Not evident | ✅ `detectReadType` | Local win |
| Chunk planning | ❌ Not evident | ✅ `calculateFileChunks` | Local win |
| Chunk cache reuse | ❌ Not evident | ✅ Present | Local win |
| Body-read stitching | ❌ Not evident | ✅ Present | Local win |
| Hidden-path tolerance | ❌ Not evident | ✅ Present | Local win |
| Config/doc alignment | ❌ Drifted | 🔶 Better aligned | Local win |
| Backend implementation completeness | ❌ Placeholder-like | ✅ Real media-serving path | Local win |

## Main risks and migration blockers

### 1. Behavior regression risk

If the upstream branch replaced the local TS VFS wholesale today, it would likely lose important real-world player behavior handling:

- scan/read heuristics
- chunk reuse
- media-semantic directory structure
- hidden-path tolerance

That is a real product regression even if the process architecture is cleaner.

### 2. Catalog modeling is not yet equivalent to the local path model

The new contract can push file updates, but the currently visible daemon model is a **flat file catalog**, whereas the local backend already encodes movie/show semantics into path parsing and traversal.

Filmu should not underestimate the amount of design work required to map a real media library model onto a remote daemon catalog.

### 3. Control-plane quality is ahead of data-plane quality

The subscribe/ack catalog channel is directionally strong, but the actual byte-serving path is still a thin streaming wrapper without the current TS backend's mature read behavior.

For Filmu this means the branch is a better inspiration for **control-plane boundaries** than for **data-plane algorithms**.

### 4. Config drift is severe enough to mislead implementation planning

Because the docs and code diverge, anyone planning around the branch could make incorrect assumptions about:

- supported daemon CLI flags
- whether runtime config is env-driven
- whether the system is already production-ready
- whether real catalog loading is actually implemented as documented

That must be called out explicitly in Filmu docs to prevent architectural overfitting to aspirational upstream notes.

### 5. Security and operability are still underdefined

The audited branch does not yet show a hardened stance for:

- authenticated daemon-to-backend communication
- TLS or mTLS
- catalog persistence/version replay guarantees
- structured observability beyond basic logging
- runtime backpressure and flow control for large real catalogs

Those concerns matter even more once VFS is split across processes.

## What Filmu should copy from this branch

### 1. Explicit cross-process contract

Filmu should strongly consider a typed control/data contract between a future mount worker and the Python backend, even if the eventual transport is not identical to this branch.

### 2. Mount isolation

Moving filesystem concerns out of the main backend process remains appealing for:

- fault isolation
- easier backend restarts
- clearer operational boundaries
- language/runtime specialization where justified

### 3. Hot catalog update channel

The `WatchCatalog` subscribe/ack idea is useful. Filmu will eventually need a way to push VFS catalog changes without requiring full remounts or coarse polling.

## What Filmu should **not** copy from this branch yet

### 1. Placeholder streaming semantics

Do not anchor Filmu design to the current upstream `streamFile(...)` behavior. The contract is interesting; the current implementation is not the right baseline.

### 2. Flat root-only catalog assumptions

Filmu needs a richer path model than the branch currently shows if the real target remains Plex/Emby-friendly library browsing.

### 3. Regression from mature heuristics to naive read forwarding

Filmu should preserve and reuse its now-implemented shared chunk/range engine rather than downgrading to simple per-read forwarding without classification and reuse.

### 4. Overclaiming documentation

Filmu docs should distinguish clearly between:

- what is implemented
- what is scaffolded
- what is planned

The upstream branch is a useful reminder that these must not be collapsed into one status label.

## Filmu documentation consequence

For Filmu planning purposes, the right interpretation is:

- **Behavior baseline:** the current local TS backend still represents the stronger VFS behavior baseline
- **Architecture signal:** the upstream `feat/rust-vfs` branch represents a promising process-boundary direction
- **Design strategy:** Filmu should combine its now-implemented shared byte-serving core with an explicit mount-worker contract, not blindly mirror either the local TS embedding model or the current upstream branch's simplifications

## Bottom line

`feat/rust-vfs` is worth tracking, but it should currently be treated as:

- **architecturally important**
- **behaviorally incomplete**
- **documentation-drifted**
- **not yet the new parity target**

Filmu should borrow the branch's **separation-of-concerns idea** while continuing to benchmark real behavior against the **current local TS backend's richer VFS implementation**.
