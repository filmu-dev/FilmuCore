# `riven-ts` Upstream Delta — 2026-04-06

## Scope

This note captures the verified upstream delta from the older local audit baseline `c98c672c2a05f40afa96a25c82eadbe3559a03e1` to current upstream `rivenmedia/riven-ts` `origin/main` at `e64604af46d95d6afd0f51dd29ed411d47ab9c35`.

The comparison checkout at `E:/Dev/Triven_riven-fork/Triven_backend - ts` has now been reset to that same `e64604af46d95d6afd0f51dd29ed411d47ab9c35` head, so this file should now be read as a historical delta note explaining what changed between the earlier snapshot and the current baseline.

## Verified upstream changes since `c98c672`

### 1. Platform / workspace maturity

- `pnpm-workspace.yaml` still spans `apps/*`, `packages/*`, and `packages/core/*`, but upstream has now hardened the workspace further with `engineStrict`, `nodeVersion/useNodeVersion = 24.14.0`, and broader catalog/tooling upgrades.
- `@mikro-orm/*` moved to v7, and the app now carries explicit seeders/factories for test and dev data.
- CI/ops scaffolding expanded with `CONTRIBUTING.md`, `dependabot.yml`, and a dedicated `verify.yaml` PR workflow that also runs on draft PRs.

### 2. Official plugin lineup changed materially

Verified from `origin/main:apps/riven/package.json` and `origin/main:packages/*`:

- added to the main app dependency graph: `plugin-comet`, `plugin-mdblist`, `plugin-notifications`, `plugin-stremthru`
- retained: `plugin-listrr`, `plugin-plex`, `plugin-seerr`, `plugin-tmdb`, `plugin-torrentio`, `plugin-tvdb`
- retained utility packages: `util-plugin-sdk`, `util-plugin-testing`, `util-rank-torrent-name`
- removed from the main app dependency graph: `plugin-realdebrid`

That means older Filmu notes that still describe `plugin-realdebrid` as part of the current official upstream app surface are now stale.

### 3. Queue/orchestration topology broadened again

Verified from `origin/main:apps/riven/lib/message-queue/` and `origin/main:apps/riven/lib/state-machines/main-runner/`:

- CPU-heavy stages now run behind sandboxed workers / worker-thread-style boundaries:
  - `scrape-item.parse-scrape-results`
  - `download-item.map-items-to-files`
  - `download-item.validate-torrent-files`
- the download pipeline now pivots through `find-valid-torrent` rather than the older `find-valid-torrent-container` center of gravity
- show/request lifecycle support is broader:
  - partial show requests are still present
  - `requestedSeasons` now appears in the entity/subscriber path
  - `schedule-reindex.actor.ts` adds explicit future re-index scheduling for show/movie release follow-up

### 4. VFS changed, but only as hardening on `main`

Upstream `main` still uses in-process `@zkochan/fuse-native`. The Rust VFS daemon work has **not** landed on `main`.

Verified `main`-branch VFS deltas since `c98c672` are mostly hardening:

- typed `FuseError` boundary in `apps/riven/lib/vfs/errors/fuse-error.ts`
- `withVfsScope()` Sentry tagging in `apps/riven/lib/vfs/utilities/with-vfs-scope.ts`
- targeted changes in `open.ts`, `read.ts`, `readdir.ts`, `release.ts`, `getattr.ts`, and `request-agent.ts`
- Docker/FUSE operational fixes (`fix(core): docker fuse`)

So Filmu should still treat the current TS VFS behavior as the behavioral baseline, while treating `feat/rust-vfs` as a separate branch-level architecture experiment.

### 5. Observability / operator surface broadened

Verified from `origin/main`:

- chalk logging + richer logger formatting/schema files landed in `apps/riven/lib/utilities/logger/`
- a local Elastic stack now exists under `elastic-local/` with Elasticsearch, Kibana, and Filebeat wiring
- the app package now depends on `@elastic/ecs-winston-format`

## Filmu implications

1. Benchmark against the current clean checkout at `E:/Dev/Triven_riven-fork/Triven_backend - ts`, which now matches upstream `main`.

2. Stop describing the current upstream plugin surface as if `plugin-realdebrid` were still canonical.

3. Treat sandboxed heavy-stage execution and scheduled re-indexing as part of the current upstream orchestration bar.

4. Keep the current TS VFS as the behavior baseline, but note that upstream `main` is still on the Node/FUSE topology and has only hardened that path so far.
