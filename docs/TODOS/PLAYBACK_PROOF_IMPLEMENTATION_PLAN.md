# Playback Proof Implementation Plan

## Mission

Prove that Filmu can execute the full playback path reliably through the mounted VFS path:

1. frontend request path
2. backend playback resolution
3. FilmuVFS catalog + mount path
4. mounted file playback without interruption

This plan is intentionally execution-first.

The immediate goal is not broader feature breadth.
The immediate goal is a hard playback proof that reduces uncertainty and prevents the team from continuing to build around an unproven integration path.

---

## Primary success criteria

Sprint 1 is successful only if all of the following are true:

1. One movie plays from the mounted path **100% without interruption**.
2. The proof runs through the real integrated stack, not through isolated mocks.
3. The flow is reproducible through one scripted integration harness.
4. The current playback compatibility surfaces remain backward-compatible.
5. The Tuesday risk sprint produces a written risk register and a confirmed or revised 8-week timeline.

Secondary success criteria:

- the playback owner can reproduce the proof without ad hoc manual recovery
- the VFS/infrastructure owner can run the harness locally and in CI
- the team has a shared failure taxonomy for the playback path

## Current implementation status

The first implementation slice for this plan is now in the repository.

### Execution status board

#### Completed now

- [x] playback-proof harness baseline implemented in [`../../scripts/run_playback_proof.ps1`](../../scripts/run_playback_proof.ps1)
- [x] repo entrypoint added in [`../../package.json`](../../package.json) as `proof:playback`
- [x] gate-oriented stability entrypoint added in [`../../package.json`](../../package.json) as `proof:playback:gate`
- [x] local-stack API-key alignment fixed in [`../../docker-compose.local.yml`](../../docker-compose.local.yml)
- [x] compose render validation completed
- [x] harness dry-run validation completed
- [x] first live harness run completed successfully for request -> acquisition -> mount -> mounted byte-read proof
- [x] optional Plex-compatible media-server proof stage implemented
- [x] optional Plex-compatible media-server proof stage live-validated against the stub target in [`../../tests/fixtures/plex_stub_server.py`](../../tests/fixtures/plex_stub_server.py)
- [x] real Jellyfin library-visibility proof stage implemented and live-validated against the existing server on `localhost:8096`
- [x] real Jellyfin playback-info proof stage implemented and live-validated against the existing server on `localhost:8096`
- [x] real Jellyfin stream-open proof stage implemented and live-validated against the existing server on `localhost:8096`
- [x] real Jellyfin playback-session reporting proof stage implemented and live-validated against the existing server on `localhost:8096`
- [x] local Docker stack now provisions isolated real Plex (`http://localhost:32401/web`) and real Emby (`http://localhost:8097`) containers with `/mnt/filmuvfs` mounted for parity testing
- [x] playback-proof harness now auto-loads local Plex/Jellyfin/Emby URLs and auth tokens from [`.env`](../../.env)
- [x] native Windows WinFSP soak/remux gate now passes on `C:\FilmuCoreVFS`
- [x] native Windows Emby API/playback-info/stream-open validation now passes across a sampled multi-title set on `C:\FilmuCoreVFS`
- [x] Emby fallback stream URL generation in [`../../scripts/run_playback_proof.ps1`](../../scripts/run_playback_proof.ps1) fixed to use `stream.${container}` rather than the malformed `stream.$container` form
- [x] native Windows chunk-engine read pressure now coalesces in-flight foreground chunk fetches in [`../../rust/filmuvfs/src/chunk_engine.rs`](../../rust/filmuvfs/src/chunk_engine.rs), reducing duplicate upstream work and improving Emby buffering
- [x] Docker Plex now reaches real mounted playback/transcode startup against the WSL host mount after fixing WSL host-mount visibility, stale host-binary reuse, entry-id refresh collisions, and duplicate foreground chunk fetches
- [x] true preferred-client playback proof now passes through the real authenticated frontend client against the live local stack
- [x] repeated green runs now pass through [`../../scripts/run_playback_proof_stability.ps1`](../../scripts/run_playback_proof_stability.ps1) and [`../../package.json`](../../package.json) `proof:playback:gate`
- [x] first self-hosted Linux CI wiring now exists in [`.github/workflows/playback-gate.yml`](../../.github/workflows/playback-gate.yml) via [`../../run_playback_gate_ci.sh`](../../run_playback_gate_ci.sh)
- [x] the proof runner now supports portable host-browser execution through an explicit browser path / env var while keeping the container-browser fallback available
- [x] media-center parity gate entrypoint added in [`../../package.json`](../../package.json) as `proof:playback:providers:gate`, backed by [`../../scripts/run_media_server_proof_gate.ps1`](../../scripts/run_media_server_proof_gate.ps1), and live-validated for Docker Plex + native Windows Emby
- [x] the proof runner no longer depends on WSL-only mounted-read execution; it now falls back to host bash or backend-container shell execution as needed
- [x] Windows-native VFS soak/regression runner implemented in [../../scripts/run_windows_vfs_soak.ps1](../../scripts/run_windows_vfs_soak.ps1)
- [x] package entrypoints added in [../../package.json](../../package.json) as proof:windows:vfs:soak and proof:windows:vfs:gate
- [x] Rust mounted-read telemetry now emits read-duration, chunk-cache, read-pattern, prefetch, inline-refresh, and chunk-cache-size metrics from the live sidecar path

#### Still open

- [x] stale-link / refresh-path proof during active playback (route-level recovery proven live; persisted item-detail lease projection still needs follow-up hardening)
- [x] harness completion hang removal so successful proof runs now emit `summary.json` and exit cleanly
- [ ] promotion of the harness from a locally green gate into an enforced playback-path merge requirement
- [ ] runner provisioning / secrets rollout for the new self-hosted CI execution path, including `PLEX_TOKEN` and `EMBY_API_KEY` so the Linux CI helper can activate the provider parity gate automatically; [`../../scripts/check_playback_gate_runner.ps1`](../../scripts/check_playback_gate_runner.ps1) is now the authoritative readiness check and should fail only for real runner gaps
- [ ] keep the new Windows-native soak gate green on `C:\FilmuCoreVFS` under long playback, seek/resume, and concurrent-reader pressure
- [x] add a real Docker Plex playback-start proof stage to [`../../scripts/run_playback_proof.ps1`](../../scripts/run_playback_proof.ps1) so the currently operator-validated path becomes repeatable artifacted evidence
- [ ] add native Windows Plex proof on top of the same `C:\FilmuCoreVFS` path when a real local Plex Media Server target exists so all three first-class native Windows media centers have equivalent recorded evidence through [`../../scripts/run_windows_media_server_gate.ps1`](../../scripts/run_windows_media_server_gate.ps1)

### Next concrete step

The next step from this plan is:

1. keep [`../../scripts/run_playback_proof.ps1`](../../scripts/run_playback_proof.ps1) and [`../../scripts/run_playback_proof_stability.ps1`](../../scripts/run_playback_proof_stability.ps1) green as the playback-path regression harness
2. provision the self-hosted Linux runner and secrets required by [`.github/workflows/playback-gate.yml`](../../.github/workflows/playback-gate.yml), validate them with [`../../scripts/check_playback_gate_runner.ps1`](../../scripts/check_playback_gate_runner.ps1) and [`../PLAYBACK_GATE_RUNNER_SETUP.md`](../PLAYBACK_GATE_RUNNER_SETUP.md), then promote it into an enforced merge requirement for playback-path changes
3. keep the new provider-parity gate green for Docker Plex + native Windows Emby while CI/merge-policy promotion is prepared, then close native Windows Plex parity when a real local PMS target exists

Implemented baseline:

- [`../../scripts/run_playback_proof.ps1`](../../scripts/run_playback_proof.ps1) now provides a reproducible playback-proof harness baseline.
- [`../../package.json`](../../package.json) now exposes the harness as `proof:playback`.
- [`../../package.json`](../../package.json) now also exposes a stricter gate-oriented wrapper as `proof:playback:gate` via [`../../scripts/run_playback_proof_stability.ps1`](../../scripts/run_playback_proof_stability.ps1).
- [`.github/workflows/playback-gate.yml`](../../.github/workflows/playback-gate.yml) now wires that gate into a first self-hosted Linux CI path through [`../../run_playback_gate_ci.sh`](../../run_playback_gate_ci.sh).
- [`../../package.json`](../../package.json) now uses `pwsh` entrypoints for the proof scripts so the same script surface stays viable on Windows and Linux hosts.
- [`../../docker-compose.local.yml`](../../docker-compose.local.yml) now sources the [`FILMU_PY_API_KEY`](../../docker-compose.local.yml) for [`filmu-python`](../../docker-compose.local.yml) from the same environment-driven value already used by the worker and frontend, removing one avoidable auth-drift class from proof runs.

What the harness currently does:

1. optionally starts the stack through [`../../start_local_stack.ps1`](../../start_local_stack.ps1)
2. verifies frontend and backend readiness
3. captures an initial [`/api/v1/stream/status`](../../filmu_py/api/routes/stream.py) snapshot
4. submits a real movie request through [`POST /api/v1/items/add`](../../filmu_py/api/routes/items.py)
5. polls the public item APIs until the item reaches media-entry or direct-ready state
6. verifies mounted file visibility and performs a mounted byte-read proof
7. captures final evidence into a timestamped `playback-proof-artifacts/` bundle

Optional proof support now also exists for a configured media-server stage:

- the harness can enable one updater target (`plex`, `jellyfin`, or `emby`) through the backend settings surface before item completion
- it can wait for a backend-side media-server scan signal after the item completes
- it can also prove real Jellyfin library visibility through the current live server when `JELLYFIN_API_KEY` is configured
- it can also prove real Jellyfin playback-info resolution for the mounted item through the current live server when `JELLYFIN_API_KEY` is configured
- it can also prove real Jellyfin stream-open behavior for the mounted item through the current live server when `JELLYFIN_API_KEY` is configured
- it now resolves local Plex/Jellyfin/Emby URLs and auth tokens from [`.env`](../../.env) before falling back to explicit script args
- it can also force one selected direct media entry stale through [`../../tests/fixtures/force_media_entry_unrestricted_stale.py`](../../tests/fixtures/force_media_entry_unrestricted_stale.py) and probe route-level recovery through [`/api/v1/stream/file/{item_id}`](../../filmu_py/api/routes/stream.py)
- it can also reuse an already completed item for stale-link proof instead of re-requesting the same TMDB row
- it restores the prior settings payload after the run

Current verified status of this slice:

- [`../../docker-compose.local.yml`](../../docker-compose.local.yml) render path validated through `docker compose -f ./docker-compose.local.yml config`
- [`../../scripts/run_playback_proof.ps1`](../../scripts/run_playback_proof.ps1) dry-run path validated successfully
- first live harness run now passes for request -> provider-backed acquisition -> mounted-file discovery -> mounted byte-read proof
- optional media-server proof support is now implemented and live-validated against the stub target in [`../../tests/fixtures/plex_stub_server.py`](../../tests/fixtures/plex_stub_server.py)
- real Jellyfin library visibility is now also live-green against the existing server on `localhost:8096`
- real Jellyfin playback-info resolution is now also live-green against the existing server on `localhost:8096`
- real Plex and Emby containers are now part of the local Compose stack at `http://localhost:32401/web` and `http://localhost:8097`, with proof-runner defaults ready for the next parity-validation slice
- native Windows WinFSP now also has a green soak/remux gate on `C:\FilmuCoreVFS`
- native Windows Emby now also has recorded visibility/playback-info/stream-open success across a sampled set of mounted titles on `C:\FilmuCoreVFS`
- the proof harness no longer produces false Emby failures from a malformed fallback stream URL, and the native Windows chunk engine now coalesces in-flight foreground chunk fetches to reduce duplicate upstream work during media-server playback
- Docker Plex now also has operator-validated mounted playback/transcode startup against the shared WSL host mount, so its remaining gap is harness/gate promotion rather than first playback bring-up
- route-level stale-link recovery is now also proven live by forcing the selected direct media entry stale and verifying that [`/api/v1/stream/file/{item_id}`](../../filmu_py/api/routes/stream.py) still serves `206 Partial Content`
- that stale-link proof currently demonstrates **route recovery, not yet durable persisted lease repair**: live item detail can still report the stale `unrestricted_url` even after the route recovers and serves bytes
- the harness now distinguishes persisted vs unpersisted stale-refresh recovery, and the late-stage completion hang has been fixed so successful proof runs now write `summary.json` and print the final PASS line again
- true preferred-client playback is now live-green through the real authenticated frontend client: a Chrome-backed host-browser run reached the real playing state end-to-end against the local stack, and a standalone Edge-backed run also passed after the runner was hardened for explicit host-browser CDP attachment
- repeated local gate validation is now also live-green: [`../../scripts/run_playback_proof_stability.ps1`](../../scripts/run_playback_proof_stability.ps1) passed `2/2` repeated runs with `-ProofStaleDirectRefresh -RequirePreferredClientPlayback -ReuseExistingItem -RequireCompletedState`
- a first CI execution path now exists for self-hosted Linux runners, with explicit prerequisites for Docker, `pwsh`, a real browser executable, debrid/TMDB secrets, and a reachable frontend source tree
- the runner is now less machine-shaped: explicit host browser selection comes from `-PreferredClientBrowserExecutable` or `FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE`, mounted-read proof no longer depends on WSL alone, and the container-browser path remains available as a fallback
- the remaining playback-proof gap is no longer first preferred-client playback, first native Windows Emby proof, or first Docker Plex playback-start evidence. It is promoting the locally green gates into CI/merge policy, continuing longer-running stability hardening, and later closing native Windows Plex parity.

## Post-proof operational hardening gate

Playback proof is no longer the only question.
The next question is whether the mounted playback path stays stable under duration, seeks, and pressure.

The next hardening pass should therefore run these explicit scenarios:

### Scenario A — continuous mounted playback soak

- one movie
- mounted playback path
- 60 minutes minimum
- expected result: no crash, no mount disconnect, no unrecovered stale-read failure, and no manual recovery

### Scenario B — interactive seek and resume soak

- one movie
- repeated forward/backward seek behavior for 15 minutes minimum
- expected result: no handle corruption, no stuck reads, no repeated range-open collapse, and no need to remount or restart the sidecar

### Scenario C — concurrent mounted-reader pressure

- at least 3 concurrent readers
- 15 minutes minimum
- expected result: no unbounded reconnect churn, no runaway cache behavior, and no mounted-read fatal error under steady pressure

### Operational thresholds for this plan

- fatal mounted-read errors: `0`
- sidecar crash/restart count: `0`
- unrecovered stale-link incidents during the run: `0` when provider refresh succeeds
- reconnect churn: no repeating reconnect loop and no more than one isolated reconnect incident per run
- operator diagnosis: failure mode must be attributable from logs, `/api/v1/stream/status`, and mounted metrics without code-level forensics

### Readiness rubric

- **Experimental**
  - proof run passes
  - soak scenarios are not yet consistently green
  - operational gaps still require maintainer intuition
- **Local-stable**
  - scenarios A, B, and C all pass locally
  - fatal mounted-read failures remain at zero
  - stale-link, reconnect, and backpressure failures are observable from the current runtime surfaces
- **Rollout-ready**
  - the soak scenarios repeat cleanly across more than one environment class
  - operators can diagnose failure class from existing signals
  - mounted-path behavior is stable enough to trust as an operational substrate rather than only a proof artifact

### Required evidence bundle additions

The playback proof artifacts should continue to include:

- backend logs
- worker logs
- filmuvfs logs
- `/api/v1/stream/status` snapshots

For the hardening gate they should additionally include:

- soak duration achieved
- reconnect incident count
- stale-refresh incident count and recovery outcome
- any mounted-read failure classification observed during the run

---

## Non-negotiable decisions

## 1. Assign Playback Proof Owner — today

One engineer must be assigned as the **Playback Proof Owner** for Weeks 1-2.

This person is full-time on the integrated playback path and owns:

- frontend -> backend playback request flow
- playback resolution through the current stream surfaces
- FilmuVFS mount behavior needed for playback proof
- reproduction, diagnosis, and closure of playback-blocking defects

This role is not part-time.

If this engineer cannot be freed up immediately, leadership should treat that as a scheduling failure rather than a normal staffing inconvenience.

### Playback Proof Owner responsibilities

- maintain the end-to-end playback test checklist
- drive the daily playback standup update
- approve or reject playback-path changes during Sprint 1
- coordinate with the VFS/infrastructure owner on harness failures
- escalate blockers the same day instead of carrying silent risk

---

## 2. Establish the integration harness — today to tomorrow

The team should treat the integration harness as a product-quality gate, not as optional tooling.

### Owner

Assigned VFS + infrastructure engineer.

### Required stack

The harness must run the real integrated environment:

- frontend
- backend
- worker
- FilmuVFS sidecar
- supporting infra already required by the local stack

The baseline should build on the current local stack in [`../LOCAL_DOCKER_STACK.md`](../LOCAL_DOCKER_STACK.md) and [`../../docker-compose.local.yml`](../../docker-compose.local.yml), not create a second disconnected environment.

### Required scripted flow

The first scripted proof flow is:

1. add one movie
2. scrape candidates
3. select and download through the current debrid path
4. expose the file through the mount
5. play the mounted file successfully

### Required outputs

The harness must emit enough evidence to debug failures quickly:

- frontend logs
- backend logs
- worker logs
- FilmuVFS logs
- mount tree snapshot
- playback status snapshot from `/api/v1/stream/status`
- explicit pass/fail result for each step in the scripted flow

### CI/CD policy

The target state is a blocking merge gate.

Practical rollout:

1. **Day 0-Day 1**: harness is created and made reproducible locally.
2. **As soon as the first green run exists**: PRs touching playback, stream, VFS, mount, or playback-resolution code require the harness to pass before merge.
3. **After the harness is stable for 2-3 consecutive days**: promote it to a general merge gate for `main`.

This avoids a fake “required” gate that nobody can currently pass while still converging rapidly to the user's required blocking policy.

Current state:

- the harness entrypoint now exists
- dry-run validation now exists
- mounted-read proof, stale-link route recovery proof, and true preferred-client playback proof now all have live-green local evidence
- the local gate wrapper is now green, and the first self-hosted CI workflow exists, but it is **not** yet an enforced merge requirement because runner/secrets rollout and merge-policy promotion are still open

---

## 3. Run the risk assessment sprint — Tuesday

### Duration

2-3 hours.

### Participants

- tech lead
- playback proof owner
- VFS/infrastructure owner
- remaining engineers who touch playback, stream, orchestration, or mount behavior

### Mandatory walkthrough path

Walk the active playback path from:

- [`LinkResolver`](../../filmu_py/services/playback.py)
- [`mount.rs`](../../rust/filmuvfs/src/mount.rs)
- [`chunk_engine`](../../rust/filmuvfs/src/chunk_engine.rs)

### Questions to answer explicitly

1. What can break in normal playback?
2. What can break under retries, stale links, or transient network instability?
3. What do we currently not know?
4. Which failures are product failures versus operator failures?
5. Which assumptions are still unproven?

### Output

The sprint must produce:

- a written risk register
- named owners per risk
- a severity label per risk
- a go / no-go view on the current 8-week timeline
- a revised timeline if the current 8-week target is no longer credible

If the result is that the timeline must change, change it immediately. Do not keep an unrealistic schedule for morale reasons.

---

## 4. Freeze the playback API — today

Starting immediately, the following are treated as frozen compatibility surfaces for the next 6 months:

- `/api/v1/stream/file/{item_id}`
- `/api/v1/stream/hls/{item_id}/*`
- FilmuVFS mount-facing compatibility contracts used by current consumers

### Freeze rules

- no breaking URL changes
- no breaking query parameter changes
- no breaking response-header changes relied on by the current frontend or playback clients
- no breaking client-visible failure-contract changes without a versioned migration path
- no breaking mount-root contract changes for current consumers
- proto and mount contract changes must be additive wherever possible

### What is still allowed

- additive fields
- internal refactors
- observability additions
- stronger retry, limiter, circuit-breaker, and cleanup behavior
- additive metrics and governance signals
- implementation hardening behind the same public contract

### Approval rule during Sprint 1

Any proposed playback-contract change requires explicit sign-off from:

- playback proof owner
- tech lead

If both are not available, the change waits.

---

## 5. Kick off Sprint 1 — Monday

### Playback Proof Owner work

The playback proof owner starts on the integrated playback path immediately and stays on that path through Weeks 1-2.

### Remaining team work

The rest of the team should continue existing workstreams **without forced context switching**, provided those workstreams do not destabilize the frozen playback surfaces.

Safe parallel work:

- documentation cleanup
- observability additions that do not alter the playback contract
- plugin policy/versioning work
- non-breaking orchestration hardening outside the frozen playback path

Unsafe parallel work during Sprint 1:

- playback API redesign
- mount contract reshaping
- LinkResolver contract changes
- major refactors across the playback path without owner approval

### Daily operating cadence

Run a daily playback standup focused only on:

- current blocker
- last 24-hour playback progress
- harness status
- decision needed today

This should be short and execution-oriented.

---

## Sprint 1 execution plan

## Day 0 — today

1. assign the playback proof owner
2. assign the VFS/infrastructure harness owner
3. freeze the playback API and contracts
4. publish this plan to the team
5. define the pass/fail evidence checklist for playback proof

### Day 0 deliverables

- named owners
- published playback freeze policy
- proof checklist
- harness scope agreed

---

## Day 1 — tomorrow

1. bring the existing Docker stack into a reproducible integrated harness flow
2. script the movie add -> scrape -> download -> mount -> play flow
3. capture logs and runtime evidence in one place
4. identify the first blocking failure if the proof is not yet green

### Day 1 deliverables

- one-command or one-script local harness run
- deterministic evidence output bundle
- first red/green baseline result

Current state:

- the one-script harness baseline now exists via [`../../scripts/run_playback_proof.ps1`](../../scripts/run_playback_proof.ps1)
- deterministic evidence output now lands under `playback-proof-artifacts/`
- the first verified result now includes a real live harness green run for [`The Matrix`](../../scripts/run_playback_proof.ps1), reaching `Completed` state with a mounted file path and successful mounted byte-read proof
- preferred-client playback is still the next open stage after the mounted-read proof

---

## Tuesday — risk sprint

1. walk the real code path
2. classify unknowns and failure modes
3. decide whether the 8-week timeline stands
4. publish updated milestone dates if needed

### Tuesday deliverables

- risk register
- owner list
- timeline confirmation or revision

---

## Monday -> end of Week 2

### Workstream A — playback proof

Owned by the playback proof owner.

Target:

- uninterrupted mounted movie playback proof

### Workstream B — harness + CI stabilization

Owned by the VFS/infrastructure owner.

Target:

- reliable harness execution locally
- CI job integrated
- blocking merge policy enabled once stable

### Workstream C — blocker removal

Owned by the rest of the team under tech-lead triage.

Target:

- fix only what blocks playback proof or harness stability

This keeps Sprint 1 focused on proof rather than general backlog consumption.

---

## Provisional post-proof plan (Weeks 3-8)

This remains provisional until the Tuesday risk sprint confirms the timeline.

## Weeks 3-4

- extend proof from “single movie uninterrupted” to seek, pause/resume, and replay stability
- widen the harness to cover a representative show episode path
- promote the harness from playback-path merge gate to broader `main` merge gate if stable
- add the missing mount and playback telemetry discovered during Sprint 1

## Weeks 5-6

- tighten long-running mount soak validation
- harden provider stale-link recovery and failure taxonomy
- eliminate remaining manual recovery steps from the proof path

## Weeks 7-8

- convert proof into repeatable release criteria
- document operational runbooks and rollback rules
- reopen broader playback enhancement work only after the proof path is stable

---

## Decision gates

## Gate 1 — owner assignment

If the playback proof owner is not assigned today, the plan is already off track.

## Gate 2 — harness existence

If the harness is not running locally by tomorrow, stop calling the schedule aggressive and call it at risk.

## Gate 3 — risk sprint credibility check

If Tuesday identifies unknowns that invalidate the 8-week target, revise the timeline immediately.

## Gate 4 — sprint-1 proof check

If no uninterrupted mounted movie playback proof exists by the end of Week 2, leadership should treat playback proof as the critical path and rebalance staffing accordingly.

---

## What this plan intentionally does not do

- it does not broaden the playback contract during the proof window
- it does not make GraphQL expansion the current critical path
- it does not reopen large architectural debates that the codebase has already settled well
- it does not assume the team can out-organize a missing full-time playback owner

---

## Bottom line

The team should now behave as if playback proof is the critical-path program.

The architecture is already strong enough.
The remaining question is execution discipline:

- one owner
- one integrated harness
- one frozen contract
- one risk sprint
- one proof target

That is the shortest reliable path to move Filmu from “architecturally ahead” to “operationally proven.”

The first harness slice is now complete.
The next concrete step is no longer harness design.
It is executing that harness against the live stack and closing the remaining open proof stages:

- preferred-client playback proof
- stale-link and failure-mode validation
- repeated green runs and eventual CI gate promotion








