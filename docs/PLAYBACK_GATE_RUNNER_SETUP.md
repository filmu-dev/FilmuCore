# Playback Gate Runner Setup

This runbook is the operator path for promoting the playback gates into enforced CI.

Current gate surfaces:

- full playback gate: [`../package.json`](../package.json) `proof:playback:gate`
- media-center parity gate: [`../package.json`](../package.json) `proof:playback:providers:gate`
- native Windows media-center gate: [`../package.json`](../package.json) `proof:windows:vfs:providers:gate`

## 1. Check Linux runner readiness

From the repository root on the self-hosted Linux runner:

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
- `FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE`

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

- `FILMU_FRONTEND_CONTEXT`
- `FILMU_PREFERRED_CLIENT_BROWSER_EXECUTABLE`
- optional `FILMU_FRONTEND_USERNAME`

Required-check promotion note:

- the workflow code path is already in place for `pull_request`, `push` to `main`, and `merge_group`
- once the target self-hosted runner and secrets are available and the workflow has produced green runs on the protected branch path, mark the GitHub required check for this workflow, typically shown as `Playback Gate / Playback Gate`
- this remaining step is GitHub repository policy setup, not an additional code change in the workflow itself
- the workflow and [`../run_playback_gate_ci.sh`](../run_playback_gate_ci.sh) now also emit the canonical required-check name into the CI artifact bundle so the repo-settings step can key off the exact check label instead of manual memory

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
- keep `proof:playback:providers:gate` green for Docker Plex + native Windows Emby parity
- keep the explicit thresholded Windows soak profiles green on `C:\FilmuCoreVFS` (`proof:windows:vfs:soak:continuous`, `proof:windows:vfs:soak:seek`, `proof:windows:vfs:soak:concurrent`, and `proof:windows:vfs:soak:full`)
- native Windows Plex evidence is already present through [`../scripts/run_windows_media_server_gate.ps1`](../scripts/run_windows_media_server_gate.ps1); the remaining work is repeatability and policy promotion, not first bring-up
