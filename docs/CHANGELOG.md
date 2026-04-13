# Changelog

## [Unreleased]

### Features

* add release provenance/perf/chaos merge gate with generated SBOM + provenance evidence and chaos contract thresholds
* enforce tenant-safe mounted runtime telemetry rollups and pressure-policy queued refresh dispatch
* add Redis Streams consumer claim/fencing baseline for control-plane replay ownership transfer
* add mount-vs-HTTP chunk parity harness with Python/Rust contract coverage and CI gate integration
* decompose worker task internals with extracted stage observability and stable stage/job-id helper modules
* extract stream refresh-dispatch policy selection into runtime refresh governance module
* add enterprise continuity gate across decomposition budgets and sustained soak/playback/operator evidence posture
* add playback stability trend-regression checks with optional environment-owned history roots
* extend operator log rollout checks with latency/active-alert budgets and persisted rollout-history records
* continue oversized-module decomposition with extracted `runtime_status_payload`, `stage_isolation`, `playback_refresh_dispatch`, and `media_path_inference` boundaries

### Documentation

* clarify platform-aware stack startup: `pnpm run stack:start` is now documented as the canonical auto-detect entrypoint, while `docker-compose.windows.yml` is explicitly documented as backend-only on Windows and not responsible for starting the native `filmuvfs.exe` mount
* reconcile stale playback-proof/TODO posture after live branch-policy validation moved into required CI gate execution
* update status/execution/TODO matrices for continuity-gate rollout and expanded large-file decomposition boundaries

## [0.14.0](https://github.com/filmu-dev/FilmuCore/compare/v0.13.0...v0.14.0) (2026-04-12)


### Features

* Codex/wave4 plugin observability 20260412 ([#39](https://github.com/filmu-dev/FilmuCore/issues/39)) ([95bdb24](https://github.com/filmu-dev/FilmuCore/commit/95bdb2433c5b34bb91a99d5b61d2fad152897bee))

## [0.13.0](https://github.com/filmu-dev/FilmuCore/compare/v0.12.0...v0.13.0) (2026-04-12)


### Features

* Codex/wave3 orchestration hardening 20260412 ([#37](https://github.com/filmu-dev/FilmuCore/issues/37)) ([3398527](https://github.com/filmu-dev/FilmuCore/commit/3398527cb3f2ea7c8c4ca20b576c0b6f9efec657))

## [0.12.0](https://github.com/filmu-dev/FilmuCore/compare/v0.11.0...v0.12.0) (2026-04-12)


### Features

* complete wave 2 identity exit gates ([#35](https://github.com/filmu-dev/FilmuCore/issues/35)) ([641bbbd](https://github.com/filmu-dev/FilmuCore/commit/641bbbd5bba63f77efe4b81d09a74c662d1072e6))

## [0.11.0](https://github.com/filmu-dev/FilmuCore/compare/v0.10.0...v0.11.0) (2026-04-12)


### Features

* Codex/wave2 identity governance 20260412 ([#32](https://github.com/filmu-dev/FilmuCore/issues/32)) ([da477bb](https://github.com/filmu-dev/FilmuCore/commit/da477bb0154b7223f7f5be479380b02e9b14d909))

## [0.10.0](https://github.com/filmu-dev/FilmuCore/compare/v0.9.0...v0.10.0) (2026-04-12)


### Features

* Codex/wave1 rollout governance 20260412 ([#30](https://github.com/filmu-dev/FilmuCore/issues/30)) ([cf21b13](https://github.com/filmu-dev/FilmuCore/commit/cf21b13d0cc35cbaed15c982f25e2425a69fde1e))

## [0.9.0](https://github.com/filmu-dev/FilmuCore/compare/v0.8.0...v0.9.0) (2026-04-12)


### Features

* Codex/abort safe mounted reads 20260412 ([#28](https://github.com/filmu-dev/FilmuCore/issues/28)) ([dfe2c69](https://github.com/filmu-dev/FilmuCore/commit/dfe2c69de27650e1a6b286ca971d96791320b7c7))

## [0.8.0](https://github.com/filmu-dev/FilmuCore/compare/v0.7.0...v0.8.0) (2026-04-11)


### Features

* add platform-aware stack lifecycle wrappers ([#24](https://github.com/filmu-dev/FilmuCore/issues/24)) ([726ad70](https://github.com/filmu-dev/FilmuCore/commit/726ad70e3a60ca1ee5330bc142b6875eec096aec))

## [0.7.0](https://github.com/filmu-dev/FilmuCore/compare/v0.6.0...v0.7.0) (2026-04-11)


### Features

* Codex/enterprise governance hardening 20260411 2205 ([#21](https://github.com/filmu-dev/FilmuCore/issues/21)) ([4a4be56](https://github.com/filmu-dev/FilmuCore/commit/4a4be563fad95b73c92bbe554989f5422be95bf6))

## [0.6.0](https://github.com/filmu-dev/FilmuCore/compare/v0.5.0...v0.6.0) (2026-04-11)


### Features

* add enterprise policy revision governance ([#17](https://github.com/filmu-dev/FilmuCore/issues/17)) ([14fba28](https://github.com/filmu-dev/FilmuCore/commit/14fba28e0e230e1fe83eb186eb93abae14af35a1))

## [0.5.0](https://github.com/filmu-dev/FilmuCore/compare/v0.4.0...v0.5.0) (2026-04-11)


### Features

* Codex/vfs semantic alias browse ([#12](https://github.com/filmu-dev/FilmuCore/issues/12)) ([ea8eba1](https://github.com/filmu-dev/FilmuCore/commit/ea8eba1a4652e868e49c797168ad1241e649656c))


### Bug Fixes

* address enterprise review findings ([#14](https://github.com/filmu-dev/FilmuCore/issues/14)) ([24b771a](https://github.com/filmu-dev/FilmuCore/commit/24b771a11ee439f0931bd74bf64af0a0289f12fc))

## [0.4.0](https://github.com/filmu-dev/FilmuCore/compare/v0.3.0...v0.4.0) (2026-04-11)


### Features

* add enterprise identity and operations baselines ([#11](https://github.com/filmu-dev/FilmuCore/issues/11)) ([f6a5097](https://github.com/filmu-dev/FilmuCore/commit/f6a50978556e83758a16505121502134222ffe70))
* **control-plane:** harden authz release governance and tenancy ([#9](https://github.com/filmu-dev/FilmuCore/issues/9)) ([badc672](https://github.com/filmu-dev/FilmuCore/commit/badc6723b126907b46e10f59dadaa590ed4e2341))

## [0.3.0](https://github.com/filmu-dev/FilmuCore/compare/v0.2.0...v0.3.0) (2026-04-10)


### Features

* Codex/vfs semantic alias browse ([#8](https://github.com/filmu-dev/FilmuCore/issues/8)) ([878bc55](https://github.com/filmu-dev/FilmuCore/commit/878bc5503eeb6c7052a3a0c491a129ba3ef20b72))
* **playback:** harden gate and soak stability ([#6](https://github.com/filmu-dev/FilmuCore/issues/6)) ([d6e3a12](https://github.com/filmu-dev/FilmuCore/commit/d6e3a12937486fd024d729850ae5916132f86c1e))

## [0.2.0](https://github.com/filmu-dev/FilmuCore/compare/v0.1.0...v0.2.0) (2026-04-08)


### Features

* initial import ([bbd3687](https://github.com/filmu-dev/FilmuCore/commit/bbd3687ca674e0ab3d25fd92b0607fbfc015ff9c))


### Bug Fixes

* gate publishing behind explicit opt-in ([55dd4a9](https://github.com/filmu-dev/FilmuCore/commit/55dd4a9f78c85090b1218ec9f79aba1e81dbb733))
* preflight docker publish secrets ([673f0c3](https://github.com/filmu-dev/FilmuCore/commit/673f0c377a21ffaceb0ebf4c2f7cbd7e7e273103))
* skip docker publish without secrets ([9051d48](https://github.com/filmu-dev/FilmuCore/commit/9051d482089eb7daedbca2df59095b273232299c))
* use github token for release-please by default ([eea81e0](https://github.com/filmu-dev/FilmuCore/commit/eea81e0860c63b5d778b5e7a28b33dd86cdbd0fb))

## Changelog

All notable changes to this project will be documented in this file.
