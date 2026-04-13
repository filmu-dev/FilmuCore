# `riven-ts` Closed PR Audit — 2026-04-12

## Scope

This document audits the current closed PR history of `rivenmedia/riven-ts` against the current Filmu source tree.

Update note (2026-04-13):

- Filmu has since landed a first-class scheduled metadata reindex/reconciliation worker cron (`scheduled_metadata_reindex_reconciliation`).
- Treat any “missing first-class scheduled reindex program” statements below as historical-at-audit-time context, not current state.

Primary GitHub inputs:

- closed PR list: <https://github.com/rivenmedia/riven-ts/pulls?q=is%3Apr+is%3Aclosed>
- explicitly reviewed PRs: `#73`, `#71`, `#70`, `#69`, `#68`, `#67`, `#66`, `#65`, `#46`, `#45`, `#42`, `#41`
- additional capability PRs reviewed from the closed list because they still shape the present baseline: `#53`, `#61`, `#60`, `#52`, `#48`, `#34`, `#33`, `#32`, `#30`, `#28`, `#19`, `#18`, `#11`, `#10`, `#7`, `#6`, `#5`, `#3`, `#2`

Comparison inputs:

- current local `riven-ts` checkout at `E:\\Dev\\Triven_riven-fork\\Triven_backend - ts`
- current Filmu source under `filmu_py` and `rust/filmuvfs`

This is not a commit-by-commit replay of every dependency bump PR. It is a capability audit of the closed PR history that still materially affects current parity, functionality, integrations, performance, or operator posture.

## Executive result

The closed PR history shows that Filmu already matches more of current `riven-ts` than the older notes claimed:

- Filmu already has a real `index_item` worker stage.
- Filmu already has partial show request scope tracking (`requested_seasons`, `requested_episodes`, `is_partial`).
- Filmu already has a repo-owned log/search pipeline baseline (`Vector` + `OpenSearch` + dashboards assets), so the remaining observability gap is rollout and operations, not “no local searchable log pipeline”.
- Filmu is still ahead on the mounted data plane, cross-process Rust VFS architecture, Windows-native mount support, chunk-engine depth, authz/governance posture, and plugin trust controls.

The closed PR history also confirms that `riven-ts` still has several meaningful gaps Filmu has not yet closed:

- a more explicit runtime lifecycle graph (`program` / `bootstrap` / `plugin-registrar` / `main-runner`)
- a scheduled reindex/reconciliation program
- broader worker/database isolation and sandboxed parse/map/validate execution
- GraphQL-first VFS/control-plane breadth, including VFS-backed GraphQL queries and stream-url persistence mutation
- broader packaged integration/plugin breadth (`seerr`, `listrr`, `comet`, `plex`, pluginized `tmdb`, pluginized `tvdb`)
- a DB seeding/factory story in the main app

## PR-by-PR capability findings

### `#73` `chore(core): migrate more to gql`

What landed in `riven-ts`:

- more of the app moved behind GraphQL
- VFS-backed GraphQL endpoints for directory listings, entry stats, and file-access views
- a GraphQL mutation to save stream URLs
- startup wipe switches for Redis and the database
- UUID migration for media IDs and related entities

Filmu comparison:

- **still missing:** GraphQL VFS/control-plane breadth equivalent to TS VFS directory/stat access and stream-url persistence
- **not a current gap:** Filmu does not need to copy the exact UUID migration unless the domain model requires it
- **low-priority tooling gap:** no source-backed equivalent for the unsafe startup wipe switches was found

Judgement: **partial parity, with real GraphQL-control-plane breadth still missing**

### `#71` `feat(core): isolate workers from database connections`

What landed in `riven-ts`:

- worker/database isolation tightened
- dataloader-backed GraphQL lookups widened
- sandboxes and worker boot flow were pushed further into a dedicated bundle/runtime model

Filmu comparison:

- Filmu now isolates heavy stages with bounded execution budgets for `index_item`, `parse_scrape_results`, and `rank_streams`
- Filmu still does **not** have the same explicit “worker bundle with isolated DB connection lifecycle” model

Judgement: **Filmu is closer than before, but still trails on worker-runtime isolation**

### `#70` `chore: modify in place in reducers`

What landed in `riven-ts`:

- allocation-reduction and in-place reducer optimizations across flows, ranking, indexing, and request processing

Filmu comparison:

- no distinct feature gap
- this is a performance/profiling reminder, not a parity blocker

Judgement: **no must-copy feature gap**

### `#69` `feat(core): use worker threads for CPU-intensive jobs`

What landed in `riven-ts`:

- worker-thread-backed execution for heavier CPU paths
- better isolation and observability around those workers

Filmu comparison:

- Filmu now has bounded isolated execution for important heavy stages
- Filmu still lacks the broader TS-style sandboxed map/validate job model and dedicated worker-runtime identity around those stages

Judgement: **partial parity; still missing broader heavy-stage isolation**

### `#68` `chore: improve scrape performance`

What landed in `riven-ts`:

- scrape-path performance work across processors and reduction logic

Filmu comparison:

- no discrete missing product feature was identified from this PR family alone
- remaining value is in profiling and tuning, not in a named missing subsystem

Judgement: **profiling item, not a feature delta**

### `#67` `fix(core): uninitialised entity in state transition`

What landed in `riven-ts`:

- state-transition correctness hardening around uninitialized entities

Filmu comparison:

- Filmu already has deterministic item-state transitions and a more explicit state-oriented worker pipeline
- no direct missing feature was identified from this PR alone

Judgement: **correctness watchpoint, not a named missing feature**

### `#66` `fix(mdblist): mdblist null fields`
### `#46` `fix(mdblist): make rank field nullable in schema`

What landed in `riven-ts`:

- MDBList schema hardening for nullable remote fields

Filmu comparison:

- Filmu’s built-in MDBList content-service already does null-tolerant extraction for key fields like `tmdb_id` and `title`
- no urgent source-backed gap was found that matches the exact TS rank-nullability issue
- there is still value in adding broader schema-resilience tests around remote nullable fields

Judgement: **mostly matched at the functional level; testing depth can still improve**

### `#65` `chore: add scopes to plugin test context items`

What landed in `riven-ts`:

- stronger plugin-test-context scope modeling

Filmu comparison:

- Filmu is ahead in runtime plugin policy, trust, quarantine, and permission-scope enforcement
- Filmu does not appear to have an equivalent plugin-test-context scope helper story

Judgement: **runtime posture ahead, test-harness ergonomics behind**

### `#45` `feat(analytics): add kibana`

What landed in `riven-ts`:

- repo-owned Elastic/Filebeat/Kibana local analytics stack

Filmu comparison:

- Filmu now has a repo-owned `Vector` + `OpenSearch` + dashboards pipeline baseline
- the remaining gap is rollout and operator execution, not absence of in-repo log/search assets

Judgement: **feature class matched; environment execution still open**

### `#42` `feat(core): add show reindexing`

What landed in `riven-ts`:

- scheduled reindexing/reconciliation path for shows

Filmu comparison:

- Filmu now has `index_item`
- Filmu now also has a first-class scheduled metadata reindex/reconciliation cron above `index_item`, now widened to repair failed-item identifier gaps and emit operator rollups, though the TS baseline still remains broader in surrounding actor/runtime depth and overall metadata/provider breadth

Judgement: **baseline matched; surrounding breadth still trails**

### `#41` `chore(core): add vfs error handling`

What landed in `riven-ts`:

- more explicit FUSE/VFS error handling and `FuseError` plumbing

Filmu comparison:

- Filmu’s Rust sidecar, telemetry, fallback accounting, inline refresh accounting, and Windows/Linux adapter boundaries are already beyond this baseline

Judgement: **not a Filmu gap; Filmu is ahead**

## PRs `#40` through `#1` recheck matrix

This section rechecks the earlier merged PR range directly against the current local `riven-ts` tree and the current Filmu source state.

| PR | Title | Current relevance | Filmu status |
| --- | --- | --- | --- |
| `#40` | `fix(core): docker fuse` | Docker/FUSE packaging hardening for the TS in-process mount path | not a strategic Filmu gap; Filmu uses a different Rust sidecar + native adapter model |
| `#39` | `chore(core): remove stremthru docker` | packaging cleanup | no meaningful feature gap |
| `#38` | `chore(core): add env files and docker data to .dockerignore` | packaging hygiene | no meaningful feature gap |
| `#37` | `fix(core): docker build` | packaging/build fix | no meaningful feature gap |
| `#36` | `fix(core): allow show year in torrent names` | ranking/parser correctness | likely already covered by Filmu’s current parse/rank path; worth keeping as a parser test reminder, not a named platform gap |
| `#35` | `chore: add CONTRIBUTING.md and use .env.seerr for seerr` | contributor workflow and Seerr packaging convenience | no product gap beyond the still-missing Seerr integration itself |
| `#34` | `feat(notifications): add notification plugin based on apprise-like urls` | notification plugin breadth | broadly matched by Filmu webhook/Discord notification support |
| `#33` | `feat(core): allow partial show requests` | partial request scope | matched; Filmu has `requested_seasons`, `requested_episodes`, and `is_partial` |
| `#32` | `feat(comet): add comet scraper` | scraper/integration breadth | still missing in Filmu |
| `#30` | `feat(core): integrate stremthru` | downloader integration breadth | matched at the core capability level |
| `#29` | `chore(core): improve download performance for large torrents` | downloader-path performance tuning | profiling reminder, not a named missing subsystem |
| `#28` | `feat(core): add partial download success event` | event taxonomy richness | partially matched; Filmu has durable outbox/eventing, but no explicitly verified equivalent event contract was confirmed in this audit |
| `#26` | `fix(core): provide aliases to title checker` | title-matching correctness | parser/ranking test reminder, not a clearly separate platform gap |
| `#24` | `chore(core): sort torrents by resolution` | ranking/presentation behavior | matched in substance by Filmu’s ranking/selection pipeline |
| `#23` | `fix(core): show fanout` | show-request orchestration correctness | partially matched; Filmu has a real multi-stage pipeline but not the same reindex/fanout breadth |
| `#22` | `chore(core): add chalk logging` | console-log ergonomics | no strategic gap |
| `#20` | `fix(core): uninitialised episode collection` | state/index correctness hardening | correctness watchpoint, not a currently distinct Filmu gap |
| `#19` | `feat(core): add show download fanout` | show orchestration breadth | partially matched; current Filmu still trails on broader reconciliation/reindex/fanout behavior |
| `#18` | `feat(plugin-seerr): add seerr content plugin` | content-service integration breadth | still missing in Filmu |
| `#17` | `chore: move events & errors to flow processors` | workflow/event architecture cleanup | no direct feature gap, but it reinforces TS flow/actor explicitness |
| `#16` | `chore: colocate flow utilities` | codebase organization | no meaningful feature gap |
| `#15` | `fix(plex): update libraries for shows` | Plex hook/integration correctness | partially matched at the media-server integration level; still missing as a packaged post-download plugin flow |
| `#14` | `fix: update Dockerfile to use node:24-alpine, improve user/group setup and solve schema build error` | packaging/build fix | no meaningful feature gap |
| `#13` | `feat: add Sentry spotlight` | observability tooling breadth | partially matched; Filmu has strong observability growth, but not this exact Sentry-focused operator path |
| `#12` | `chore: allow unknown season counts to pass show torrent validation` | parser/validator correctness | test and validation reminder, not a clearly separate platform gap |
| `#11` | `feat: add torrent ranking` | ranking subsystem | matched at the core capability level |
| `#10` | `feat: add tvdb indexer` | index-provider breadth | partially matched; Filmu uses TVDB host services, not pluginized index-provider equivalents |
| `#9` | `feat(core): add core settings` | settings/control-plane baseline | broadly matched |
| `#8` | `feat(sdk): add plugin settings parser` | plugin settings infrastructure | broadly matched |
| `#7` | `feat(mdblist): add mdblist plugin` | content-service integration breadth | matched at the core capability level |
| `#6` | `feat(plex): add plex plugin` | Plex integration breadth | partially matched; Filmu has Plex service integration but not a source-verified packaged Plex plugin/hook path |
| `#5` | `feat: add streaming` | initial streaming/VFS capability | matched and exceeded by Filmu’s current Rust sidecar and serving substrate |
| `#4` | `feat: add streaming` | early streaming baseline | matched and exceeded |
| `#3` | `feat: use mikro-orm` | ORM/control-plane foundation | not a one-to-one stack gap, but TS still keeps an advantage in GraphQL/dataloader-shaped control-plane composition |
| `#2` | `feat: add vfs` | initial VFS baseline | matched and exceeded |
| `#1` | `Feat/rust vfs` | historical branch signal, not merged baseline | useful historically, but not part of current upstream `main` parity claims |

## Net result from PRs `#41` to `#1`

After rechecking the older PR range, the current Filmu gaps are now the same five categories:

1. broader metadata-reconciliation depth and multi-environment/provider breadth above the now-landed scheduled reindex baseline
2. broader worker/database isolation plus sandboxed heavy-stage execution
3. broader packaged integration/plugin breadth (`seerr`, `listrr`, `comet`, Plex post-download hooks, pluginized `tmdb` / `tvdb`)
4. GraphQL-first control-plane/VFS breadth where it is actually useful
5. DB seed/factory tooling and a few remaining event-taxonomy/test-harness conveniences

Most of the rest of the `#41` through `#1` PR range falls into one of three buckets:

- already matched by current Filmu
- matched in substance but through different architecture
- correctness or packaging improvements that should influence tests and profiling, not roadmap shape

## Additional closed PRs that still matter

### Already effectively matched or exceeded by current Filmu

- `#53`, `#61`, `#60` CI / verify workflow posture
- `#45` local searchable log pipeline baseline
- `#41` VFS error handling
- `#33` partial show requests
- `#11` torrent ranking
- `#30` StremThru integration
- `#7` MDBList content-service integration
- `#5` streaming / VFS baseline
- `#2` VFS baseline

### Still meaningful gaps

- `#52` database seeds: Filmu still lacks an equivalent main-app seeding/factory story
- `#48` MikroORM v7 / dataloader-shape migration: not a one-to-one stack gap, but TS still has the stronger GraphQL/dataloader control-plane shape
- `#34` notifications plugin: Filmu has webhook notifications, so this is broadly matched
- `#32` Comet scraper: Filmu still lacks an equivalent built-in or packaged Comet integration
- `#28` partial download success event: Filmu has durable outbox/eventing, but no explicitly verified equivalent event contract was found in this audit
- `#19` show download fanout: Filmu has real pipeline stages, but the TS actor/flow breadth for show fanout and reindex/reconciliation still remains broader
- `#18` Seerr content plugin: Filmu still lacks a Seerr/Overseerr-style packaged content-service plugin equivalent
- `#10` TVDB pluginized indexer: Filmu uses TVDB as a host service, not a pluginized index-provider
- `#6` Plex plugin: Filmu has Plex media-server integration, but no source-backed packaged Plex post-download plugin equivalent was found
- `#73` GraphQL VFS endpoints and stream-url persistence mutation remain open gaps

## Actionable gap rollup for Filmu

Highest-signal additions Filmu can still take from the closed PR history:

1. deepen metadata reconciliation from the new scheduled baseline into broader multi-environment/provider coverage and richer evidence packs
2. deepen heavy-stage isolation from bounded executors into a broader sandboxed/isolated worker-runtime model where profiling justifies it
3. add GraphQL VFS/control-plane surfaces where they improve operator workflows: directory listing, entry stat, and explicit stream-link persistence are the clearest candidates
4. close the integration breadth gap with Seerr/Listrr intake, Comet scraping, a Plex post-download hook path, and pluginized TMDB/TVDB indexing
5. decide whether queue-backed stream-link refresh should become a first-class runtime contract instead of remaining partly configured and partly inline
6. add a DB seeding/factory story for repeatable local and test-state setup

## Things Filmu should not copy blindly

- reducer micro-optimizations before profiling proves they matter
- stack-specific migrations like TS UUID naming changes without a Filmu-side domain reason
- Elastic/Kibana specifically, since Filmu already has a credible Vector/OpenSearch direction
- GraphQL-first everything; only the operator/useful surfaces should be promoted
