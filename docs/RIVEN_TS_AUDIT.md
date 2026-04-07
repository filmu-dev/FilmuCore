# `riven-ts` Audit Notes for Filmu

## Scope

This document started as a March 2026 audit of the current local TypeScript backend at `E:/Dev/Triven_riven-fork/Triven_backend - ts` and now also incorporates an April 6, 2026 upstream refresh against `rivenmedia/riven-ts` `main`.

The comparison checkout at `E:/Dev/Triven_riven-fork/Triven_backend - ts` is now aligned directly to `rivenmedia/riven-ts` `main` at `e64604af46d95d6afd0f51dd29ed411d47ab9c35`. A duplicate clean mirror also exists at `E:/Dev/Filmu/.tmp-riven-ts-current`.

The goal is not to copy `riven-ts` blindly. The goal is to **exceed it deliberately**: match the useful runtime breadth, then surpass it with clearer contracts, stronger observability, safer failure isolation, and a better stream/VFS architecture.

Related deeper comparison notes:

- [`filmcore_vs_riven_ts_audit.md`](filmcore_vs_riven_ts_audit.md) — broader FilmuCore vs `riven-ts` capability gap analysis
- [`filmcore_vs_ts_vfs.md`](filmcore_vs_ts_vfs.md) — detailed VFS/streaming comparison focused on the serving substrate versus the running TS VFS
- [`RIVEN_TS_RUST_VFS_BRANCH_AUDIT.md`](RIVEN_TS_RUST_VFS_BRANCH_AUDIT.md) — dedicated audit of the upstream `feat/rust-vfs` branch versus the current local TS backend
- [`RIVEN_TS_UPSTREAM_DELTA_2026_04_06.md`](RIVEN_TS_UPSTREAM_DELTA_2026_04_06.md) — verified upstream-main delta from local `c98c672` to current `e64604a`
- [`riven-ts-python-compatible-backend-research.md`](riven-ts-python-compatible-backend-research.md) — earlier parity/performance research including KPI targets that still inform the longer-term direction

## April 2026 current-baseline reality check

Verified on 2026-04-06:

- the local `Triven_backend - ts` checkout is now on `e64604af46d95d6afd0f51dd29ed411d47ab9c35`
- upstream `rivenmedia/riven-ts` `main` is now `e64604af46d95d6afd0f51dd29ed411d47ab9c35`
- the local comparison checkout is now clean and matches upstream `main`
- a duplicate clean mirror of that exact upstream head also exists at `E:/Dev/Filmu/.tmp-riven-ts-current`

The important practical consequence is that Filmu docs must now treat:

1. the local `Triven_backend - ts` checkout as the exact current `riven-ts` baseline
2. upstream GitHub `main` as the same source-of-truth lineage
3. the auxiliary mirror at `E:/Dev/Filmu/.tmp-riven-ts-current` only as a duplicate clean copy

The old dirty hybrid interpretation no longer applies to the comparison checkout.

## Verified architecture snapshot

The audited TypeScript backend is currently:

- GraphQL-first on `@apollo/server` with `type-graphql`
- bootstrapped and coordinated through `xstate` state machines
- backed by `bullmq` flows/workers for orchestration
- using `@zkochan/fuse-native` for VFS integration
- running a plugin-first architecture rather than a monolithic resolver registry

March-April 2026 re-audit note:

- the current local TS workspace has expanded substantially beyond the earlier single-app baseline into a larger pnpm monorepo with `apps/*`, `packages/*`, and `packages/core/*` entries in [`pnpm-workspace.yaml`](../../Triven_riven-fork/Triven_backend%20-%20ts/pnpm-workspace.yaml)
- the app still remains GraphQL-first and BullMQ/xstate-driven, but it now sits on top of a broader package ecosystem that includes plugin packages, reusable utility packages, and shared core packages
- upstream `main` has since pushed that platform further with Node 24.14 workspace enforcement, MikroORM v7, explicit seeders/factories, stronger CI/verification workflows, and an Elastic/Kibana local observability stack
- the current clean checkout includes `.github/workflows/verify.yaml`, `.github/actions/*`, `elastic-local/`, sandboxed job files, and the newer VFS hardening helpers directly in the checked-out tree

## March 2026 upstream `feat/rust-vfs` branch conclusion

A subsequent upstream branch audit changed one important planning assumption: the upstream Rust-VFS work should currently be treated as an **architectural signal**, not as the new behavior baseline.

The branch does introduce valuable ideas:

- an explicit protobuf/gRPC contract under `packages/feature-vfs`
- a separate Rust FUSE daemon under `apps/vfs-daemon`
- app lifecycle changes that start/stop a gRPC server instead of mounting FUSE in-process

However, compared to the current local TS backend, the branch currently also:

- removes the mature `apps/riven/lib/vfs/**` stack before equivalent behavior is restored
- replaces rich path-aware, cache-aware, read-aware VFS behavior with a much thinner remote read path
- ships docs that overstate readiness and document flags/settings not reflected by the verified code

That means Filmu should continue to benchmark **behavioral parity** against the current local TS backend, while selectively borrowing **process-boundary and protocol ideas** from the upstream Rust-VFS branch.

See [`RIVEN_TS_RUST_VFS_BRANCH_AUDIT.md`](RIVEN_TS_RUST_VFS_BRANCH_AUDIT.md) for the full comparison.

## Verified plugin-runtime findings

The current TS plugin runtime already includes more than GraphQL resolver contribution:

- dependency-based discovery of installed `@repo/plugin-*` packages from the app manifest
- plugin validation before activation
- shared `PluginSettings` parsing/locking
- per-plugin datasource construction
- optional `plugin.context` runtime context enrichment for GraphQL and hook execution
- queue-backed typed event hooks
- publishable-event gating so only subscribed events are broadcast to plugin queues

The current local workspace now materially broadens the plugin/package surface compared to the earlier audit snapshot:

- the current local `apps/riven/package.json` dependency graph now includes `plugin-comet`, `plugin-listrr`, `plugin-mdblist`, `plugin-notifications`, `plugin-plex`, `plugin-seerr`, `plugin-stremthru`, `plugin-tmdb`, `plugin-torrentio`, and `plugin-tvdb`
- the same workspace still physically contains [`packages/plugin-realdebrid`](../../Triven_riven-fork/Triven_backend%20-%20ts/packages/plugin-realdebrid), even though the main app dependency graph has moved to `plugin-stremthru` and the newer plugin lineup
- a reusable SDK layer now exists under [`packages/util-plugin-sdk`](../../Triven_riven-fork/Triven_backend%20-%20ts/packages/util-plugin-sdk), plus plugin-test helpers in [`packages/util-plugin-testing`](../../Triven_riven-fork/Triven_backend%20-%20ts/packages/util-plugin-testing)
- a standalone RTN-oriented utility package now exists under [`packages/util-rank-torrent-name`](../../Triven_riven-fork/Triven_backend%20-%20ts/packages/util-rank-torrent-name), which significantly raises the parity bar for Filmu's torrent parsing/ranking documentation
- shared core utility packages now also exist (for example GraphQL schema helpers, Kubb/OpenAPI config helpers, ESLint/Vitest/TypeScript shared config packages), which means the TS backend is no longer best described as just one backend app plus plugins

## Verified orchestration findings

The current TS orchestration surface is broader than a simple `index -> scrape -> download -> complete` pipeline.

Verified breadth includes:

- request-content-services intake
- index requests
- scrape requests
- parse-scrape-results stage
- download flow
- map-items-to-files stage
- find-valid-torrent stage
- rank-streams stage
- retry-library recovery

The current re-audit also confirms that the TS orchestration surface is now backed by a much broader explicit worker/flow tree under [`apps/riven/lib/message-queue/flows/`](../../Triven_riven-fork/Triven_backend%20-%20ts/apps/riven/lib/message-queue/flows), including dedicated processors, schemas, enqueue helpers, and utility modules per stage rather than only a thin app-local queue wrapper.

The April 2026 re-audit also shows that the current clean checkout contains the newer orchestration pieces directly:

- a sandboxed job tree exists on disk for `parse-scrape-results`, `map-items-to-files`, and `validate-torrent-files`
- the download flow tree now centers on `find-valid-torrent` plus sandboxed `map-items-to-files` / `validate-torrent-files`
- explicit `bootstrap-sandboxed-workers`, `event-scheduler`, `job-enqueuer`, and `schedule-reindex` actors now exist in the main-runner actor set

## Verified season / episode filtering in current `riven-ts`

The current local TS backend does more than simple season-overlap filtering.

Verified source points:

- [`validate-torrent.ts`](../../Triven_riven-fork/Triven_backend%20-%20ts/apps/riven/lib/message-queue/flows/scrape-item/steps/parse-scrape-results/utilities/validate-torrent.ts)
- [`validate-torrent-files.ts`](../../Triven_riven-fork/Triven_backend%20-%20ts/apps/riven/lib/message-queue/flows/download-item/steps/find-valid-torrent/utilities/validate-torrent-files.ts)

What the TS flow currently enforces:

1. **Movie requests**
   - Reject any torrent that parses as show-like (`seasons` or `episodes` present).

2. **Show requests**
   - Reject torrents with no parsed seasons/episodes.
   - If parsed seasons exist, require intersection with the indexed show's seasons and enforce an expected season count threshold.
   - For single-season shows with parsed episode ranges, require the parsed absolute episode set to cover the whole indexed season.

3. **Season requests**
   - If no parsed season exists but parsed episodes do, allow absolute-number matching for anime/absolute-episode style packs.
   - Otherwise require the parsed season number to match exactly.
   - When parsed episode numbers exist, require the relative episode set to cover the whole season.

4. **Episode requests**
   - Require either a matching parsed episode number (relative or absolute) or a matching parsed season.
   - Reject torrents with incorrect season, incorrect episode, or no season/episode metadata at all.

5. **Download/torrent-file validation**
   - After parse/rank, TS also validates actual torrent-file contents against expected episode counts before final selection.
   - This is especially important for season torrents and large packs, where title parsing alone is not treated as sufficient proof of completeness.

## Filmu alignment note

FilmuCore still intentionally follows a dual-layer strategy:

1. **Layer 1** — preserve the current frontend compatibility surface.
2. **Layer 2** — expand the internal graph/domain model for a future richer frontend.

That means Filmu should borrow the **semantic strictness** of the TS filtering path without collapsing everything into frontend-shaped compatibility handlers. The right long-term target is:

- compatibility-safe REST/SSE behavior for the current frontend
- stronger graph/domain-backed scope semantics underneath
- future GraphQL/frontend consumers reusing the same stricter orchestration rules instead of re-implementing compatibility-specific heuristics

## Corrections to earlier assumptions

The audit corrected one important assumption in the Filmu docs:

- current `riven-ts` plugin discovery is **not** a generalized language-level entry-point model
- it is currently based on scanning installed plugin dependencies from the application manifest

That means packaged entry-point discovery remains a valid place for Filmu to improve beyond the TS backend rather than merely catching up to it.

## How Filmu should exceed `riven-ts`

Filmu should aim for the following upgrades over the current TS backend:

1. **Dual plugin distribution model**
   - keep safe filesystem-manifest discovery
   - add packaged entry-point discovery
   - support both without weakening startup safety
   - see `RIVEN_TS_RUST_VFS_BRANCH_AUDIT.md` for more details on upstream plugin breadth

2. **Stricter capability contracts**
   - make plugin capabilities explicit (`graphql`, `settings`, `datasources`, `event_hooks`, future stream/admin capabilities)
   - validate those capabilities at startup
   - version them clearly

3. **Stronger operability**
   - richer plugin telemetry and health visibility
   - clearer runtime status for active capabilities and publishable events
   - better failure containment and operator diagnostics
   - see `RIVEN_TS_RUST_VFS_BRANCH_AUDIT.md` for more details on upstream orchestration


4. **Safer orchestration semantics**
   - persisted domain transitions
   - idempotent event application
   - transactional outbox and replay-safe semantics where needed
   - see `RIVEN_TS_RUST_VFS_BRANCH_AUDIT.md` for more details on upstream VFS reliability
   

5. **Better streaming architecture**
   - keep the FilmuCoreVFS-first direction
   - treat HTTP compatibility endpoints as companion surfaces
   - make stream correctness and operability a product advantage, not just parity work

## April 2026 Upstream vs Local Interpretation

The upstream-main refresh from local `c98c672` to current `e64604a` changed several practical planning assumptions, and the comparison checkout is now aligned directly to that current upstream state:

1. **Plugin reality is broader than older Filmu notes assumed**: the current app manifest carries `plugin-comet`, `plugin-mdblist`, `plugin-notifications`, and `plugin-stremthru`, while the workspace still also retains `packages/plugin-realdebrid`.
2. **Heavy queue stages are part of the clean current baseline**: sandboxed job files for `parse-scrape-results`, `map-items-to-files`, and `validate-torrent-files` are checked in on current `main`.
3. **Show lifecycle follow-up is broader**: `schedule-reindex` and the newer main-runner actors are part of the current checked-out tree.
4. **Current VFS already includes the newer hardening files**: `FuseError` and `withVfsScope()` are part of the current baseline.
5. **Ops/observability are broader**: verification workflows, reusable GitHub Actions, and `elastic-local/` are part of the current baseline.

For the full verified file/commit delta, see [`RIVEN_TS_UPSTREAM_DELTA_2026_04_06.md`](RIVEN_TS_UPSTREAM_DELTA_2026_04_06.md).
