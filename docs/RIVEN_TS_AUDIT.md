# FilmuCore vs `riven-ts` Audit

## Purpose

This is the single canonical comparison doc for FilmuCore versus the current `riven-ts` baseline.

It replaces the previously split top-level comparison notes:

- broader capability comparison
- VFS and streaming comparison
- TS baseline audit notes

It also now absorbs the older:

- upstream-delta note for the `c98c672` -> April 2026 `main` transition
- branch-specific `feat/rust-vfs` audit

The older Python-compatible backend research note remains separate because it is design-history input rather than an active comparison source of truth.

## Baseline checked

Verified on 2026-04-11:

- local `Triven_backend - ts`: `f98cc3132f9b6e3d9d8d3f79fcbf2b9a9a4f2ec5`
- upstream `rivenmedia/riven-ts` `main`: `f98cc3132f9b6e3d9d8d3f79fcbf2b9a9a4f2ec5`
- the local comparison checkout matches upstream `main`
- a duplicate clean mirror also exists at `E:/Dev/Filmu/.tmp-riven-ts-current`

The practical consequence is simple: Filmu parity and differentiation claims should be judged against current upstream `main`, not against older notes or the upstream `feat/rust-vfs` branch.

## Current verdict

FilmuCore now has a real end-to-end request to playback path on validated local proof topologies:

- request content
- scrape
- parse and rank
- debrid and persist media entries
- mount through the Rust sidecar
- serve Plex, Jellyfin, and Emby through the mounted or HTTP paths

The remaining gap is no longer first pipeline completeness or first mounted playback success. The active gap is enterprise hardening and breadth beyond the now-landed repo baseline:

- metadata-reconciliation depth and multi-environment/provider breadth above the now-landed scheduled reindex baseline
- broader plugin and package breadth
- stricter sandbox/process isolation beyond the current spawn-required heavy-stage ceiling/recycle baseline
- environment-owned searchable observability
- HA and distributed control-plane maturity

This comparison is now source-backed against the current Filmu Rust/Python tree and the current `riven-ts` monorepo tree, not just older branch notes. The active delta below was re-checked against `rust/filmuvfs`, `filmu_py`, `apps/riven`, `packages/util-plugin-sdk`, and the currently wired TS plugin entrypoints.

The closed-PR audit that explains how the current TS baseline got here now lives in [`RIVEN_TS_CLOSED_PRS_AUDIT_2026_04_12.md`](RIVEN_TS_CLOSED_PRS_AUDIT_2026_04_12.md).

## Audited `riven-ts` baseline

The current TypeScript backend is:

- GraphQL-first on `@apollo/server` with `type-graphql`
- coordinated through `xstate` state machines
- backed by `bullmq` flows and workers
- backed by `mikro-orm` on PostgreSQL with dataloader-enabled entity loading
- fronted by Redis-backed Apollo response caching through `KeyvAdapter` / `@keyv/redis`
- still using `@zkochan/fuse-native` for the running VFS on `main`
- broader than the older single-app picture, now sitting inside a larger pnpm monorepo with plugin, utility, and shared core packages

Current local baseline details that matter for Filmu planning:

- the app dependency graph now includes the newer plugin lineup such as `plugin-stremthru`, `plugin-comet`, `plugin-mdblist`, and `plugin-notifications`
- the workspace still physically retains `packages/plugin-realdebrid`
- CPU-heavy stages now exist as sandboxed jobs in the TS tree
- the local main-runner actor set includes the newer orchestration and scheduling pieces
- the local baseline also includes stronger CI and local observability scaffolding
- the app lifecycle is explicitly split across `program`, `bootstrap`, `plugin-registrar`, and `main-runner` state machines
- queue-backed plugin workers are wired around typed event publication and worker registration
- the VFS `open` path still uses queue-backed stream-link resolution and dedup
- verified plugin hook families include TMDB indexing, Seerr intake, Plex post-download hooks, and notification fan-out

## Side-by-side capability summary

| Area | `riven-ts` | FilmuCore |
| --- | --- | --- |
| Request intake | Overseerr/Plex/Listrr plugin flows into DB | Real request pipeline with persisted item and request rows plus downstream triggers |
| Index and metadata | Dedicated indexing flow with plugin-backed metadata sources | Dedicated `index_item` worker stage now exists, scheduled metadata reconciliation now covers partial/ongoing/completed plus repairable failed items, but metadata-provider breadth is still narrower |
| Scrape and ranking | Mature BullMQ scrape plus parse plus ranking flow | Real `scrape_item` -> `parse_scrape_results` -> `rank_streams` path with built-in Torrentio and RTN-compatible ranking |
| Debrid and download | Newer plugin lineup plus sandboxed validation jobs | Built-in Real-Debrid, AllDebrid, and Debrid-Link clients for both pipeline and playback refresh |
| VFS mount | Running `fuse-native` VFS on `main` | Real Rust sidecar mount runtime with Linux `fuse3`, Windows ProjFS/WinFSP adapters, and validated playback proof paths |
| HTTP playback | Compatibility path secondary to mount | Real `/api/v1/stream/*` path with direct-file, HLS, and status/governance surfaces |
| Plugin ecosystem | Broader package/plugin breadth | Narrower breadth, but stronger trust, publisher, tenancy, and quarantine policy |
| Queue durability | BullMQ with richer long-running workflow breadth | ARQ with real job graph, retry/dead-letter behavior, transactional outbox, DLQ age/reason operator rollups, bounded replay filters, and recovery seams |
| Observability | Stronger monorepo-local observability stack and Elastic local baseline | Stronger operator-facing policy/governance APIs, but environment-owned search/export still remains |

## Where FilmuCore is already ahead

- cross-process Rust sidecar VFS architecture instead of keeping the byte path in the main app runtime
- Windows-native WinFSP and ProjFS adapter boundary
- hybrid cache with optional disk-backed L2 instead of an in-process-only byte path
- chunk coalescing, in-flight dedup, and adaptive prefetch in the mounted data plane
- generation-aware gRPC catalog watch, snapshot/delta delivery, and inline refresh semantics between control plane and mount plane
- operator-facing playback governance on `/api/v1/stream/status`
- persisted authz and policy posture on `/api/v1/auth/policy`
- tenant quota visibility and first request/worker enforcement
- plugin trust, publisher, quarantine, and tenancy governance
- enterprise posture surface on `/api/v1/operations/governance`
- broader REST and SSE compatibility surface
- graph-first specialization-backed calendar/detail/list lineage on the shared media-domain service seam

## Where FilmuCore still trails

- deeper multi-actor lifecycle orchestration comparable to the TS `program` / `bootstrap` / `plugin-registrar` / `main-runner` hierarchy
- broader scheduled reindex / reconciliation depth, especially pluginized metadata-index breadth and multi-environment/provider coverage
- stricter sandbox/process isolation for heavy parse/map/validate stages beyond Filmu's bounded executor baseline
- broader worker/database isolation around those background runtimes
- queue-backed plugin execution breadth across more hook families
- broader queue-backed stream-link orchestration breadth beyond Filmu's current optional queued refresh path
- broader plugin and integration ecosystem breadth, including Seerr/Listrr intake, Comet scraping, Plex post-download hooks, and pluginized TMDB/TVDB indexing
- GraphQL-first cached control-plane breadth with Redis-backed response caching and dataloader-shaped ORM access
- GraphQL VFS/control-plane breadth, including VFS-backed directory/stat queries and stream-url persistence mutation
- database seed/factory tooling in the main app
- environment-owned searchable log and trace infrastructure
- more complete HA and distributed control-plane operating model

## Verified source-backed delta

What current `riven-ts` still has that Filmu does not yet match:

- deeper XState app lifecycle orchestration across startup, plugin registration, and steady-state execution
- broader metadata reconciliation depth around scheduled reindexing, especially pluginized index breadth and multi-environment/provider coverage
- broader BullMQ flow isolation, including sandboxed parse/map/validate workers beyond Filmu's spawn-required bounded executor/process-policy baseline
- broader worker/database isolation around those background runtimes
- queue-backed plugin worker fan-out across a larger typed event surface
- queue-backed stream-link request handling directly on the VFS open path rather than Filmu's current optional queued control-plane dispatch
- broader verified integration inventory: `seerr`, `listrr`, `comet`, `plex`, pluginized `tmdb`, pluginized `tvdb`, `torrentio`, `stremthru`, `mdblist`, notifications
- GraphQL-first Redis-cached control-plane composition with dataloader-oriented ORM access
- VFS-backed GraphQL directory/stat access plus stream-url persistence mutation
- DB seeds/factories for repeatable local and test-state setup

What Filmu already does better:

- a cross-process Rust sidecar instead of an in-process Node mount path
- Windows-native WinFSP/ProjFS support in the real mount architecture
- mount-plane chunk coalescing, adaptive prefetch, and optional disk-backed cache layers
- generation-aware gRPC catalog watching plus inline stale-link refresh on the byte path
- explicit authz, tenant, plugin-governance, and operations-governance surfaces
- safer plugin trust, quarantine, signature, and operator-override controls than the current TS baseline

## VFS and streaming comparison

### What the current TS VFS still does well

- mature read-type classification for media workloads
- discrete chunk caching and chunk-boundary reuse
- queue-backed link-resolution dedup
- explicit hidden-path filtering and abort-safe read behavior
- a battle-tested in-process mount path on current `main`

### What FilmuCore now does better

- one cross-surface serving model spanning HTTP direct-play, HLS, and the mounted control plane
- first-class session, handle, and path accounting instead of scattered per-handler maps
- operator-visible runtime and governance state on `/api/v1/stream/status`, now including mounted cache/chunk-coalescing/upstream-wait/refresh pressure classes on top of the earlier rollout ratios/reasons
- cross-platform mount architecture with Linux and native Windows adapters
- a cleaner separation between control plane and byte-serving plane

### What still remains on the Filmu VFS side

- deeper mounted observability
- broader soak and backpressure validation across environment classes
- stronger rollout and canary criteria
- clearer policy on whether mounted browsing should remain alias-based or grow into a fully separate id-keyed tree
- broader resolver orchestration above the now-landed mount-side inline refresh dedup baseline

## Planning consequence

Filmu should not copy `riven-ts` blindly.

The right target is:

1. match the useful runtime breadth that still matters
2. keep the better Filmu architecture decisions
3. exceed the TS baseline in operator clarity, isolation, and reliability

That means the active comparison-driven priorities are:

1. deepen the indexing stage that is now present in-repo into a first-class reindex / reconciliation program
2. decide which indexing and metadata-enrichment paths should be pluginized instead of staying as host-only services
3. deepen heavy-stage isolation from the current spawn-required worker-ceiling/recycle policy baseline into stricter sandbox/process ceilings where warranted
4. harden the mounted data plane into repeatable rollout policy
5. broaden plugin/package breadth without weakening trust and governance
6. decide deliberately how far queue-backed stream-link resolution should expand beyond the current optional queued dispatch plus inline refresh/dedup path
7. add GraphQL control-plane surfaces only where they materially improve operator workflows
8. complete the environment-owned log/search/export story
9. strengthen HA and failover posture for the distributed control plane

## Historical upstream delta note

The older local audit baseline at `c98c672` is no longer the active comparison target, but the verified delta from that snapshot still matters because it raised the parity bar.

Important upstream changes between that older snapshot and the now-current clean baseline included:

- workspace hardening around Node 24.14, stricter engine enforcement, and broader tooling maturity
- official plugin-lineup changes, including `plugin-comet`, `plugin-mdblist`, `plugin-notifications`, and `plugin-stremthru`, while `plugin-realdebrid` dropped out of the main app dependency graph
- broader queue and orchestration topology, including sandboxed parse, map, and validate jobs plus scheduled re-index support
- stronger local observability and operator scaffolding, including structured ECS logging and a local Elastic stack
- VFS hardening on `main` without replacing the in-process `fuse-native` topology

The planning consequence is that Filmu should benchmark against the current clean `main` checkout, not against stale notes that predate those changes.

## Historical `feat/rust-vfs` branch audit

The upstream `feat/rust-vfs` branch remains important as an architectural signal, but it is still not the behavior baseline.

What the branch got right:

- explicit backend and daemon separation
- a typed gRPC contract for streaming and catalog updates
- a cleaner control-plane and data-plane boundary than the in-process Node VFS topology

What it got wrong at the audited snapshot:

- placeholder-grade backend gRPC behavior
- flatter and thinner filesystem behavior than current `main`
- regression from mature read heuristics and chunk reuse to simpler forwarding semantics
- documentation that overclaimed readiness relative to the verified implementation

The right Filmu takeaway remains:

1. copy the process-boundary and protocol direction where it is strong
2. do not copy the branch's behavior regressions or documentation overclaims
3. keep current upstream `main` as the parity target for actual behavior
