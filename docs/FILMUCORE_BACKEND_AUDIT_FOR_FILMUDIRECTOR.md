# FilmuCore Backend Audit For FilmuDirector

## Status

- Approved to support **FilmuDirector** as a parallel frontend foundation and BFF programme.
- Approved for early contract testing, UI foundation work, settings studio work, plugin administration work, and playback-shell integration work.
- Not approved as a fully frozen, enterprise-complete replacement backend for retiring the current frontend yet.
- Not approved for claiming that FilmuDirector can safely ditch the current frontend yet.

## Document Purpose

This document records the audited backend findings for **FilmuCore** so the new frontend, **FilmuDirector**, can be built deliberately against the real platform rather than against assumptions.

It is written to support:

- frontend architecture and BFF design
- backend-contract understanding
- migration discipline
- enterprise-grade readiness decisions
- direct competition with the current Riven frontend on product quality, operational quality, and platform breadth

The central backend conclusion is:

**FilmuCore is strong enough to begin FilmuDirector now, but it is not yet at the point where the current frontend should be retired.**

## 1. Executive Decision

### Decision

The correct strategic move is to start FilmuDirector now as a **parallel frontend platform track** against FilmuCore.

That decision is correct because FilmuCore already has:

- a real backend architecture rather than scaffolding
- a real BFF-compatible auth model
- a real GraphQL surface
- a real plugin runtime
- a typed settings/configuration model
- real playback and FilmuVFS capabilities
- real observability, governance, and operator-facing control-plane surfaces

### Replacement Policy

FilmuCore should **not** yet be treated as a fully frozen enterprise-grade replacement backend for removing the current frontend.

The current frontend should only be retired once:

- FilmuCore reaches enterprise-grade operational readiness
- backend contracts are frozen for successor use
- the BFF/auth/session contract is explicitly validated end to end
- migration inventory and journey parity are complete
- FilmuDirector has passed replacement readiness review against those gates

That distinction is essential.

## 2. Audit Scope

This audit covers the active FilmuCore backend platform, including:

- Python API, services, workers, auth, plugins, and persistence
- Rust FilmuVFS sidecar and mounted data plane
- documentation and TODO matrices
- CI, proof, soak, governance, and operations surfaces

### Audited repository signals

Current backend breadth in this workspace:

- `56` REST route handlers under `filmu_py/api/routes`
- `16` GraphQL fields, mutations, and subscriptions in the active schema layer
- `26` Alembic migrations
- `59` Python test files under `tests/`
- `7` built-in plugin modules
- `8` GitHub Actions workflows
- `50` documentation files under `docs/`

### Focused verification run

On April 12, 2026, the following focused backend verification pass completed successfully:

- `tests/test_authz.py`
- `tests/test_oidc.py`
- `tests/test_plugin_trust.py`
- `tests/test_graphql_mutations.py`
- `tests/test_replay_backplane.py`
- `tests/test_stream_routes.py`
- `tests/test_db_migrations.py`

Result:

- `238 passed in 23.19s`

This does not prove every backend behavior, but it does provide direct validation across the highest-signal platform surfaces for FilmuDirector:

- authz and access policy
- OIDC readiness
- plugin trust and governance
- GraphQL mutation baseline
- replay/control-plane durability baseline
- stream/playback route behavior
- schema migration integrity

## 3. Backend Findings

## 3.1 Platform shape

FilmuCore is no longer just a compatibility API.

It is a multi-surface backend platform with:

- FastAPI REST compatibility routes
- Strawberry GraphQL intentional product surface
- ARQ worker orchestration
- SQLAlchemy/Alembic persistence
- Redis-backed rate limiting, queue, replay, and control-plane primitives
- plugin runtime, trust, governance, and eventing
- playback and byte-serving runtime
- Rust FilmuVFS sidecar over gRPC
- governance, policy, and operator operations surfaces

This is important because FilmuDirector should treat FilmuCore as a platform backend, not just a route collection.

## 3.2 API surface

FilmuCore already exposes meaningful frontend-facing breadth.

### REST

The REST compatibility surface includes:

- health and root readiness
- auth context and auth policy
- policy revision and approval flows
- queue status and queue history
- plugin status, events, and governance
- operations governance and runtime lifecycle
- stats and calendar
- settings schema, get, set, save, load
- item list, item detail, add, retry, reset, remove
- scrape routes and session routes
- stream status, direct-file serving, HLS serving, and event streams
- webhook intake

This means FilmuDirector can start real BFF and UX work immediately without waiting for backend bring-up.

### GraphQL

The GraphQL layer is no longer theoretical.

It already contains:

- core query projections
- request, retry, reset, and settings mutations
- subscription surfaces for item-state changes, notifications, and structured log streaming
- plugin-aware schema composition via registry-based extension

That gives FilmuDirector a credible intentional contract surface to build toward.

## 3.3 Domain and persistence model

The persistence layer is now broad enough for a serious product frontend.

Core backend persistence includes:

- settings
- tenants
- principals
- service accounts
- access-policy revisions
- authorization decision audit
- plugin governance overrides
- control-plane subscribers
- media items
- playback attachments
- item requests
- subtitle entries
- movie, show, season, and episode specializations
- streams, stream relations, and stream blacklists
- scrape candidates
- media entries
- active streams
- state events
- outbox events

This means FilmuDirector does not need to build itself around a flat or toy data model.

It can support:

- deep item detail
- active playback state
- policy-aware admin UX
- tenant and actor-aware operator surfaces
- plugin governance workflows
- richer future graph-based product UI

## 3.4 Orchestration model

FilmuCore already has a real staged orchestration backbone.

Current stage model includes:

- request intake
- index
- scrape
- parse scrape results
- rank streams
- debrid download execution
- finalize
- replay and retry support
- content-service intake and outbox control-plane work

This is materially stronger than a single linear background task model.

It means FilmuDirector can expose:

- stage-aware item progress
- explicit retry and reset flows
- queue and replay operator surfaces
- future orchestration dashboards

without inventing platform semantics that the backend does not already have.

## 3.5 Identity, auth, and access policy

FilmuCore is beyond the earlier “shared API key only” stage, but it is not finished.

Current strengths:

- BFF-compatible backend authentication model
- explicit actor, role, and tenant request context
- first-class tenant, principal, and service-account persistence
- policy revision model
- policy approval workflow
- authorization decision audit persistence
- OIDC implementation baseline
- auth context and auth policy inspection routes

Current limitation:

- the platform is still in transition from shared backend-secret assumptions to full enterprise-grade identity and policy enforcement

This matters directly to FilmuDirector because the new frontend will depend on:

- stable session and auth mediation
- admin and operator policy UX
- tenant- and role-aware navigation
- predictable mutation authorization rules

## 3.6 Plugin platform

FilmuCore already has a real plugin platform, not just plug points.

Current capability includes:

- manifests
- capability declarations
- packaged and filesystem discovery
- scoped settings
- datasource-aware context injection
- hook execution
- publishable-event governance
- trust metadata
- source digest validation
- signature verification
- trust-store support
- quarantine and publisher-policy posture
- operator-facing plugin governance routes

Built-in plugin baseline includes:

- torrentio
- prowlarr
- rarbg
- mdblist
- stremthru
- notifications

This is strategically important because FilmuDirector can build:

- plugin administration
- plugin health
- plugin trust and quarantine UX
- future extension dashboards

against a real backend capability model.

## 3.7 Playback and FilmuVFS

This is one of FilmuCore’s strongest competitive differentiators versus a thin frontend-led product.

Current backend playback/data-plane strengths:

- direct file serving with range support
- HLS playlist and segment serving
- route-level playback governance and status
- media-entry and active-stream-aware playback selection
- provider-backed refresh handling
- playback proof and preferred-client proof harnesses
- Windows and Linux/WSL playback evidence
- Rust FilmuVFS sidecar with:
  - catalog client
  - chunk engine
  - cache
  - prefetch
  - telemetry
  - Unix mount path
  - Windows ProjFS path
  - Windows WinFSP path

This is not just an implementation detail.

For FilmuDirector, this means the backend can support:

- a serious playback shell
- operator-grade playback diagnostics
- VFS-aware product experiences
- direct competition with the Riven frontend on streaming and mounted-media posture

## 3.8 Observability and operations

FilmuCore now has meaningful operator and SRE depth.

Current platform evidence includes:

- structured logging
- Prometheus metrics
- OpenTelemetry support
- Sentry hooks
- queue status and history
- operations governance endpoint
- runtime lifecycle endpoint
- control-plane subscriber visibility
- operations program documentation
- operator log pipeline documentation
- backup/restore proof script
- log-pipeline proof script

This is highly relevant to FilmuDirector because the new frontend should expose operator and admin value that can compete directly with the Riven frontend, not just browse media items.

## 3.9 CI, release, and proof discipline

FilmuCore is already oriented toward proof-backed promotion rather than “works on one machine” confidence.

Current evidence includes:

- verify workflow
- playback-gate workflow
- release workflow
- docker publish workflow
- branch hygiene workflow
- platform validation workflow
- repeated playback proof scripts
- Windows VFS soak and provider proof scripts
- branch-policy validation scripts
- operations proof scripts

This is a strong backend maturity signal and is one of the reasons FilmuDirector can start now without waiting for backend basic hygiene work.

## 4. What Is Already Strong Enough To Justify FilmuDirector Now

The following backend aspects are already strong enough to support immediate frontend foundation work.

### 4.1 Contract reality

FilmuCore already has enough real API and service surface to support:

- shell and authenticated route design
- settings studio
- plugin administration
- queue and operator views
- playback shell
- GraphQL contract work

### 4.2 BFF compatibility

The backend is already aligned with a BFF/session model rather than requiring a browser-to-backend direct model.

That means FilmuDirector can and should be built as a first-class BFF frontend.

### 4.3 Typed configuration model

The backend settings model is already strong enough to justify a serious admin/settings product surface.

### 4.4 Plugin and governance posture

FilmuCore already exposes enough plugin/runtime/policy state to justify building a differentiated product UI around governance, extension management, and platform operations.

### 4.5 Playback and VFS differentiation

The backend already has enough playback and mounted-runtime substance to justify FilmuDirector building toward a stronger product experience than the current frontend, not merely parity.

## 5. What Is Still Missing

FilmuCore is not yet “backend-frozen for full frontend replacement”.

The remaining blockers are structural.

### 5.1 Identity and authz depth

The platform has a serious authz baseline now, but full enterprise identity maturity is still open:

- stronger OIDC rollout completion
- deeper tenant-aware policy
- broader ABAC coverage and operational evidence
- less dependence on compatibility-era assumptions

### 5.2 Tenancy depth

Tenancy exists in persistence and request context, but is not yet fully platform-deep across every worker, plugin, VFS, quota, and observability concern.

### 5.3 Distributed control-plane maturity

Replay and control-plane durability exist, but the system is not yet fully at the “distributed enterprise control plane” end state.

### 5.4 Plugin isolation depth

Plugin trust and policy are real, but runtime isolation remains an active maturity track.

### 5.5 VFS operational maturity

Playback and VFS are real, but the long-run enterprise hardening story is still active:

- soak evidence
- rollout policy
- pressure classification
- data-plane failure attribution
- repeated production-like stability evidence

### 5.6 Performance and chaos discipline

Performance and resilience intent exist, but the full budgeted benchmark and chaos programme is still not fully closed.

### 5.7 Source-of-truth hygiene

The backend docs are broad and strong, but planning hygiene still needs to stay disciplined so FilmuDirector is built against one authoritative contract and rollout posture.

## 6. Strategic Conclusion

### Final backend judgement

The right call is:

**Use FilmuCore now as the backend and platform foundation for FilmuDirector.**

The wrong call would be:

**Treat FilmuCore as already finished enough to justify retiring the current frontend immediately.**

### Why this is the correct posture

This allows FilmuDirector to move fast on:

- design system
- BFF
- shell
- settings
- plugin management
- playback shell
- operator surfaces

without making an unsafe replacement claim before the enterprise-grade gate is complete.

## 7. Backend Role In FilmuDirector

## 7.1 Architectural position

FilmuCore must be treated as the **product backend and platform motor** for FilmuDirector.

FilmuDirector should not be designed as a thin UI over random endpoints.

It should be designed against FilmuCore as:

- system of record
- orchestration engine
- policy and governance engine
- playback and VFS runtime
- plugin host
- operator control plane

## 7.2 BFF responsibilities

The FilmuDirector BFF is not optional glue.

It should be a first-class architectural layer that is responsible for:

- session-aware request orchestration
- auth and route protection enforcement
- cookie and CSRF boundary handling
- token and backend-secret isolation away from the browser
- backend response shaping for frontend needs
- mediation between browser UX and backend GraphQL or service contracts
- compatibility handling where legacy REST/BFF flows still exist
- request consolidation and fan-in/fan-out policy
- error normalization and operational failure mapping
- caching and edge policy where product-safe
- rollout-safe fallback behavior during migration periods

This is especially important because FilmuDirector is intended to compete directly with the Riven frontend, not merely proxy it.

## 7.3 Product implication

The backend already supports a frontend that can compete on more than aesthetics.

FilmuDirector should leverage backend strength in:

- settings and admin depth
- plugin governance
- operator visibility
- playback diagnostics
- platform maturity
- VFS-backed product differentiation

## 8. Recommended Implementation Areas For FilmuDirector Right Now

The following frontend work can begin immediately against the current backend.

### 8.1 Application shell

- authenticated shell
- route structure
- layout system
- navigation primitives
- environment bootstrapping

### 8.2 BFF foundation

- route handlers
- session model
- backend request mediation
- contract normalization
- auth enforcement

### 8.3 Settings studio

- typed settings editing
- settings schema exploration
- save/load/validation workflows
- operator-safe configuration surfaces

### 8.4 Plugin administration

- list and inspect plugins
- capability visibility
- trust and quarantine posture
- governance and override workflows

### 8.5 Operator surfaces

- queue status and queue history
- runtime lifecycle
- operations governance
- auth/policy inspection
- control-plane subscriber visibility

### 8.6 Playback shell

- player boundary
- playback diagnostics
- stream-status awareness
- operator-facing playback failure context

### 8.7 GraphQL contract pipeline

- schema snapshot
- generated operations
- route-handler integration rules
- server-component access strategy

## 9. What Must Not Happen Yet

The following should not happen yet:

- FilmuDirector being declared the successor frontend
- the current frontend being retired
- forced GraphQL-only migration before BFF and auth contract maturity
- uncontrolled browser plugin execution
- assumption that backend compatibility routes are the long-term product model
- assumption that backend enterprise-grade maturity is already complete

## 10. Backend Readiness Gates For FilmuDirector Replacement

The current frontend should only be retired once the backend passes all of the following replacement gates.

## 10.1 Product and migration readiness

- full current frontend route inventory
- full current frontend journey inventory
- parity classification by route and workflow
- migration sequencing by product surface
- documented legacy-flow deprecation path

## 10.2 BFF and auth readiness

- frozen session lifecycle contract
- validated cookie and CSRF model
- documented route protection rules
- documented admin and impersonation behavior, if applicable
- stable BFF request/response mediation rules
- test coverage for auth and session edge cases

## 10.3 GraphQL and service-contract readiness

- stable schema snapshot workflow
- codegen pipeline in CI
- typed operation ownership model
- fragment conventions
- backend compatibility guarantees
- explicit fallback policy for non-GraphQL flows

## 10.4 Design-system and operator-surface readiness

- stable primitives
- accessibility baseline
- form and table architecture
- operational state components
- settings and governance component coverage
- Storybook-backed state documentation

## 10.5 Plugin governance readiness

- extension slot contract
- plugin capability boundaries
- signed or versioned bundle model
- lifecycle governance
- admin and operator visibility

## 10.6 Enterprise backend readiness

FilmuCore must be enterprise-grade in the areas that materially affect FilmuDirector and its BFF:

- identity and access control
- tenancy model
- distributed control plane maturity
- SRE and disaster recovery posture
- plugin isolation depth
- VFS operational maturity
- performance testing discipline
- resilience and chaos validation
- observability and audit completeness

## 10.7 Operational rollout readiness

- end-to-end observability across frontend, BFF, backend, and VFS
- structured audit coverage
- rollback and migration runbooks
- release gating posture
- migration monitoring
- operator dashboards for cutover health

Only after these gates are satisfied should the organisation start ditching the current frontend.

## 11. Definition Of Enterprise-Grade Backend Support For Frontend Replacement

For this programme, FilmuCore is only enterprise-grade enough for frontend replacement when it is not merely feature-rich, but contractually and operationally safe.

That means the backend is:

- secure
- auth-governed
- tenant-aware where required
- operationally observable
- auditable
- plugin-governed
- replay-safe
- migration-safe
- contract-tested
- resilient under load and failure
- maintainable across multiple teams
- backed by explicit rollback and recovery strategy
- proven through real playback, VFS, and operational evidence

If those conditions are not met, FilmuCore is still a platform in progress, not a backend-frozen foundation for retiring the current frontend.

## 12. Recommended Backend-Aware Implementation Sequence

### Phase 1 - FilmuDirector foundation

Build now against the current backend:

- app shell
- BFF base
- design system
- settings foundation
- plugin admin foundation
- operator dashboard foundation
- playback shell foundation

### Phase 2 - Contract hardening

Then complete:

- auth/BFF contract freeze
- route and journey inventory
- GraphQL snapshot and codegen pipeline
- migration map
- plugin capability boundary definition
- frontend observability baseline

### Phase 3 - Enterprise-grade validation

Then validate:

- performance
- security
- auditability
- auth and tenancy edge cases
- resilience
- runbooks
- release and rollback discipline

### Phase 4 - Backend freeze review

Only then assess:

- route parity
- workflow parity
- supportability
- enterprise backend readiness
- migration risk
- cutover sequencing

### Phase 5 - Controlled successor rollout

Only after the review passes:

- replace validated journeys incrementally
- monitor migration health
- retire the current frontend in controlled sequence
- decommission legacy routes only after successor proof exists

## 13. Final Recommendation

### Business recommendation

Proceed now with FilmuDirector as a parallel frontend foundation programme on top of FilmuCore.

### Engineering recommendation

Treat FilmuCore as a serious platform backend and design FilmuDirector as a BFF-led, server-first, contract-governed frontend that competes directly with the Riven frontend on platform quality, not just UI styling.

### Replacement recommendation

Do not retire the current frontend until:

- FilmuCore is enterprise-grade end to end in the areas that matter to frontend replacement
- the BFF and auth contracts are frozen
- migration inventory is complete
- GraphQL and service contracts are stable
- replacement readiness is formally validated

## 14. Short Conclusion

FilmuCore is already strong enough to justify building FilmuDirector now.

It is not yet strong enough to justify ditching the current frontend now.

The correct move is:

**Start FilmuDirector immediately.**

**Retire the current frontend only after FilmuCore and the FilmuDirector BFF path pass enterprise-grade replacement readiness.**
