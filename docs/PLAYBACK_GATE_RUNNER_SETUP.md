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

## 3. CI behavior

[`../run_playback_gate_ci.sh`](../run_playback_gate_ci.sh) now does this:

1. validates base runner prerequisites
2. starts the local playback stack
3. runs the full Jellyfin/preferred-client playback gate
4. starts Plex + Emby containers
5. runs the provider parity gate automatically when `PLEX_TOKEN` and `EMBY_API_KEY` are present

So the expected rollout order is:

1. make `proof:playback:gate` green on the runner
2. add `PLEX_TOKEN` and `EMBY_API_KEY`
3. confirm provider parity gate runs green in CI
4. mark the workflow required for playback-path changes

## 4. What still remains after runner rollout

- keep `proof:windows:vfs:gate` green on the native Windows path
- keep `proof:playback:providers:gate` green for Docker Plex + native Windows Emby parity
- later, add real native Windows Plex evidence through [`../scripts/run_windows_media_server_gate.ps1`](../scripts/run_windows_media_server_gate.ps1) once a local Plex Media Server exists against `C:\FilmuCoreVFS`
