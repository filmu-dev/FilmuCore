# Playback Gate Runner Setup

This runbook is the operator path for promoting the playback gates into enforced CI.

Current gate surfaces:

- full playback gate: [`../package.json`](../package.json) `proof:playback:gate`
- media-center parity gate: [`../package.json`](../package.json) `proof:playback:providers:gate`
- repeated media-center parity stability gate: [`../package.json`](../package.json) `proof:playback:providers:stability`
- native Windows media-center gate: [`../package.json`](../package.json) `proof:windows:vfs:providers:gate`
- repeated native Windows media-center stability gate: [`../package.json`](../package.json) `proof:windows:vfs:providers:stability`
- repeated Windows soak stability gate: [`../package.json`](../package.json) `proof:windows:vfs:soak:stability`
- full Windows soak stability gate: [`../package.json`](../package.json) `proof:windows:vfs:soak:stability:full`

## 1. Check Linux runner readiness

From the repository root on the GitHub-hosted Linux runner job, or on a matching local Linux host when validating the same prerequisites manually:

```bash
pwsh -NoProfile -File ./scripts/check_playback_gate_runner.ps1
pwsh -NoProfile -File ./scripts/check_playback_gate_runner.ps1 -RequireProviderGate
```

Or via package scripts:

```bash
npm run proof:playback:ci:readiness
npm run proof:playback:ci:readiness:providers
```

Required prerequisites for the base playback gate:

> Note: this readiness check is intentionally Linux-runner-targeted. Running it on a Windows development host is expected to report `not_ready` because `/dev/fuse` is a required gate prerequisite.

- `docker`
- `curl`
- `pwsh`
- Linux host with `/dev/fuse`
- `TMDB_API_KEY`
- at least one debrid provider token/key
- `FILMU_FRONTEND_CONTEXT`
- a Chromium/Chrome/Edge executable discoverable on the runner, or `FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE`

Additional prerequisites for the provider parity gate:

- `PLEX_TOKEN`
- `EMBY_API_KEY`

## 2. Configure GitHub runner inputs

The workflow [`../.github/workflows/playback-gate.yml`](../.github/workflows/playback-gate.yml) expects:

Secrets:

- `TMDB_API_KEY`
- one or more debrid secrets:
  - `FILMU_PY_REALDEBRID_API_TOKEN` or `REAL_DEBRID_API_KEY`
  - `FILMU_PY_ALLDEBRID_API_TOKEN` or `ALL_DEBRID_API_KEY`
  - `FILMU_PY_DEBRIDLINK_API_TOKEN` or `DEBRID_LINK_API_KEY`
- `FILMU_FRONTEND_PASSWORD`
- `FILMU_PY_API_KEY`
- optional provider-parity secrets:
  - `PLEX_TOKEN`
  - `EMBY_API_KEY`

Variables:

- optional `FILMU_FRONTEND_CONTEXT`
- `FILMU_FRONTEND_REPOSITORY`
- optional `FILMU_FRONTEND_REF`
- optional `FILMU_FRONTEND_USERNAME`

GitHub-hosted frontend checkout note:

- the workflow now runs on `ubuntu-latest`
- when `FILMU_FRONTEND_REPOSITORY` is set, the workflow checks out that external frontend repo into `${GITHUB_WORKSPACE}/Triven_frontend`
- `FILMU_FRONTEND_CONTEXT` now defaults to that checkout path when no explicit override is provided
- the workflow now auto-discovers a Chrome/Chromium/Edge binary on the hosted Ubuntu image and exports it into `FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE` when the repo variable is unset
- the workflow also attempts `sudo modprobe fuse` before readiness validation so `/dev/fuse` is not treated as a manual pre-step on the standard GitHub-hosted Linux runner
- if `FILMU_FRONTEND_REPOSITORY` is unset, readiness will fail unless `FILMU_FRONTEND_CONTEXT` already points to a readable frontend checkout inside the runner workspace

Required-check promotion note:

- the workflow code path is already in place for `pull_request`, `push` to `main`, and `merge_group`
- once the target GitHub-hosted runner configuration and secrets are available and the workflow has produced green runs on the protected branch path, mark the GitHub required check for this workflow, typically shown as `Playback Gate / Playback Gate`
- this remaining step is GitHub repository policy setup, not an additional code change in the workflow itself
- the workflow and [`../run_playback_gate_ci.sh`](../run_playback_gate_ci.sh) now also emit the canonical required-check name into the CI artifact bundle so the repo-settings step can key off the exact check label instead of manual memory

## 2a. Validate GitHub `main` policy deterministically

The repository now also carries a policy checker for the external GitHub settings step:

```bash
pwsh -NoProfile -File ./scripts/check_github_main_policy.ps1 -RequirePlaybackGate
pwsh -NoProfile -File ./scripts/check_github_main_policy.ps1 -RequirePlaybackGate -ValidateCurrent
```

Or via package scripts:

```bash
npm run proof:playback:policy
npm run proof:playback:policy:validate
```

What it does:

- prints the canonical required-check set for `main`
- prints the expected merge-method policy (`squash` on, `merge commit` off, `rebase` off)
- when `gh` is installed and authenticated with repo-admin access, validates the current GitHub repository and branch-protection state instead of relying on manual memory
- exits non-zero for `-ValidateCurrent` when the live policy is `not_ready` or `unverified`, so the checker can be used as a real CI gate instead of a report-only helper
- keeps the print-only mode (`proof:playback:policy` without `-ValidateCurrent`) exit-zero so operators can inspect the canonical expected policy on machines that do not have `gh` configured

## 3. CI behavior

[`../run_playback_gate_ci.sh`](../run_playback_gate_ci.sh) now does this:

1. validates base runner prerequisites
2. starts the local playback stack
3. runs the full Jellyfin/preferred-client playback gate
4. starts Plex + Emby containers
5. runs the provider parity gate automatically when `PLEX_TOKEN` and `EMBY_API_KEY` are present
6. repeats that provider parity gate twice with fail-fast behavior so the parity path is treated as a stability gate instead of a single-run probe
7. writes `playback-proof-artifacts/ci-execution-summary.json` with the canonical required-check name plus whether provider parity actually ran on that workflow execution

So the expected rollout order is:

1. make `proof:playback:gate` green on the runner
2. add `PLEX_TOKEN` and `EMBY_API_KEY`
3. confirm provider parity gate runs green in CI
4. mark the `Playback Gate / Playback Gate` check required for playback-path changes

## 4. What still remains after runner rollout

- keep `proof:windows:vfs:gate` green on the native Windows path
- keep `proof:playback:providers:gate` and `proof:playback:providers:stability` green for repeated Docker Plex + native Windows Emby parity evidence
- keep the explicit thresholded Windows soak profiles green on `C:\FilmuCoreVFS` (`proof:windows:vfs:soak:continuous`, `proof:windows:vfs:soak:seek`, `proof:windows:vfs:soak:concurrent`, and `proof:windows:vfs:soak:full`)
- keep the repeated Windows soak stability wrappers green on real hosts so threshold tuning is based on repeated artifact evidence instead of one-off operator runs
- native Windows Plex evidence is already present through [`../scripts/run_windows_media_server_gate.ps1`](../scripts/run_windows_media_server_gate.ps1); the remaining work is repeatability and policy promotion, not first bring-up
