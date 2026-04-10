# Frontend Platform Strategy

## Purpose

Record the current frontend decision for FilmuCore after auditing the active backend, documentation, and TODO set.

This document intentionally excludes the legacy compatibility surface as the long-term product target.

The compatibility REST/SSE surface still matters operationally for the current frontend, but it is **not** the design center for the new frontend platform.

The new frontend platform will live at:

- `E:\Dev\Filmu\Filmu-Frontend`

## Executive Decision

Start the new frontend **now** as a parallel platform track.

Do **not** position it as the successor frontend yet.

Do **not** retire the current frontend until the platform reaches the enterprise-grade gate and the replacement contract is explicitly frozen.

## Decision Summary

The current backend is mature enough to support serious frontend foundation work:

- the backend already has a deliberate dual-surface model: compatibility REST/SSE plus an intentional GraphQL surface
- the auth model is already BFF/session-oriented rather than browser-to-backend direct
- the plugin runtime is real and manifest-driven, not conceptual
- the settings model is typed internally and already suitable for a deep configuration UI
- playback and FilmuVFS are now real platform capabilities rather than placeholders

However, the repository is **not** yet in a state where a clean rip-and-replace decision is disciplined:

- the current frontend source and complete route/journey baseline remain external to this workspace
- the auth/BFF contract is documented, but not yet frozen as a frontend replacement contract
- the GraphQL surface exists, but replacement-grade schema/codegen boundaries still need to be formalized
- enterprise-grade platform gaps are still open in identity/authz, tenancy, distributed control plane, SRE/DR, plugin isolation depth, VFS maturity, and performance/chaos discipline
- documentation is strong but still has source-of-truth hygiene debt; for example, active planning material contains duplicated priority blocks

## Audit Basis

This recommendation is based on the active backend and planning documents, especially:

- [`ARCHITECTURE.md`](./ARCHITECTURE.md)
- [`AUTH.md`](./AUTH.md)
- [`STATUS.md`](./STATUS.md)
- [`LOCAL_FRONTEND_TESTING_READINESS.md`](./LOCAL_FRONTEND_TESTING_READINESS.md)
- [`EXECUTION_PLAN.md`](./EXECUTION_PLAN.md)
- [`TODOS/NEXT_IMPLEMENTATION_PRIORITIES.md`](./TODOS/NEXT_IMPLEMENTATION_PRIORITIES.md)
- [`TODOS/ENTERPRISE_GRADE_GAP_MATRIX.md`](./TODOS/ENTERPRISE_GRADE_GAP_MATRIX.md)
- [`PLUGIN_SDK.md`](./PLUGIN_SDK.md)

## What The Backend Direction Already Supports

### 1. Future-facing application contract

The backend already distinguishes between:

- compatibility REST/SSE for the current frontend
- intentional GraphQL for the future product surface

That is the right foundation for a new frontend that should eventually distance itself from the old UI rather than remain trapped by legacy payload shapes.

### 2. Server-session / BFF architecture

The documented auth model is already split correctly:

- the frontend owns user session behavior
- the backend authenticates the frontend server/BFF layer
- the backend API key stays out of the browser

This makes a server-first frontend architecture the natural fit.

### 3. Typed settings and deep configuration

The backend already has:

- typed internal settings models
- compatibility translation boundaries
- persisted runtime configuration

That is exactly the backend shape needed for an advanced settings studio, admin UX, and future enterprise operator surfaces.

### 4. Manifest-driven plugin platform

The backend plugin system already provides:

- manifests
- version and capability policy
- scoped settings
- event model
- trust metadata
- signature and digest validation baseline

This is strong enough to justify a frontend plugin-management and extension-surface strategy now, without starting from arbitrary remote browser plugins.

### 5. Real playback and VFS platform

Playback and FilmuVFS are now real platform workstreams, not placeholders.

That means the future frontend can be designed against:

- serious playback control surfaces
- operational playback visibility
- VFS-backed browsing and streaming direction

instead of being shaped around temporary scaffolding.

## What Is Still Missing Before Replacement Is Safe

The new frontend should not be declared the successor until the following are true.

### Enterprise-grade platform gate

The following platform gaps remain active and are not optional:

- enterprise identity and authz
- tenancy boundary and tenant-aware ownership/quota/observability
- distributed control plane and replayable eventing
- SRE/DR program, SLOs, rollback, incident policy
- durable searchable operator log/export workflow
- deeper plugin trust, isolation, and publisher lifecycle control
- stronger VFS operational maturity and rollout policy
- explicit performance budgets, benchmark discipline, and chaos testing

These are tracked in [`TODOS/ENTERPRISE_GRADE_GAP_MATRIX.md`](./TODOS/ENTERPRISE_GRADE_GAP_MATRIX.md).

### Replacement contract gate

The following frontend replacement prerequisites are also required:

- full current frontend route and journey inventory
- frozen auth/BFF contract
- frozen session/cookie/CSRF behavior
- migration-ready GraphQL schema and code generation workflow
- source-of-truth cleanup across active docs and branch snapshots
- explicit plugin capability boundaries for frontend extension points

## Frontend Programme Decision

### Start now

The following work should start immediately in parallel:

- application shell
- navigation and information architecture
- design system and token system
- settings studio foundation
- plugin administration surface
- observability/operator console shell
- playback shell and player integration foundation
- GraphQL contract and code generation pipeline

The implementation home for this work is:

- `E:\Dev\Filmu\Filmu-Frontend`

### Do not claim successor status yet

The following should **not** happen yet:

- retiring the current frontend
- declaring parity-complete replacement
- forcing all flows to move to GraphQL before auth/BFF and journey contracts are frozen
- treating the compatibility surface as the long-term product model

## Recommended Frontend Stack

### Core stack

- Next.js App Router
- TypeScript
- Server Components by default
- Route Handlers and Server Functions for BFF concerns
- GraphQL-generated typed operations
- Storybook for design-system and state coverage
- a disciplined component primitive layer based on Radix-style primitives
- narrowly scoped client-side server-state tooling only where genuinely needed
- manifest-based frontend extension slots rather than arbitrary remote browser plugins

### Why this is the right stack

This stack fits the actual platform direction:

- server-first rendering matches the BFF/session model
- route-level loading/streaming works well for operator and admin-heavy surfaces
- TypeScript is the obvious baseline for a large modular frontend
- Storybook supports a proper enterprise-grade component system
- a Radix-style primitive layer gives long-term ownership without locking the product to a monolithic UI framework
- generated GraphQL operations create better contract discipline than hand-written ad hoc fetch code

## Architectural Guardrails

### 1. GraphQL-preferred, not GraphQL-pure on day one

The new platform should be GraphQL-preferred.

It should **not** become artificially blocked if some early BFF or route-handler flows still need to bridge into existing runtime seams.

### 2. Freeze auth/BFF contract before replacement claims

Before any successor declaration, explicitly freeze:

- login/logout flow
- session lifecycle
- cookie model
- CSRF model
- protected-route rules
- admin and impersonation behavior, if introduced
- backend credential rollover and operational rotation rules

### 3. Choose the component foundation early

Do not leave the primitive/component strategy implicit.

The application framework is only the shell.

The programme must also choose and standardize:

- component primitives
- design tokens
- layout primitives
- navigation primitives
- operator/admin patterns

### 4. Formalize code generation boundaries

If the new frontend is GraphQL-preferred, define early:

- schema snapshot source
- operation ownership rules
- fragment conventions
- generated file location rules
- whether Route Handlers call GraphQL directly or through an internal SDK
- whether Server Components call GraphQL directly or through an application service layer

### 5. Extension slots before arbitrary browser plugins

Frontend extensibility should begin with:

- manifest-declared extension points
- signed or versioned internal bundles
- bounded capability exposure
- operator-governed enablement

It should **not** begin with unrestricted remote plugin execution in the browser.

## Delivery Shape

### Phase 0 - Contract and inventory freeze

- inventory current frontend journeys and routes
- define replacement-critical auth/BFF contract
- define GraphQL/codegen boundary
- document migration sequencing by surface area
- clean source-of-truth ambiguity in docs where needed

### Phase 1 - Frontend foundation

- app shell
- routing model
- layout system
- token system
- component primitives
- Storybook workspace
- generated GraphQL client pipeline

This phase should be created in the clean frontend workspace at:

- `E:\Dev\Filmu\Filmu-Frontend`

### Phase 2 - Platform surfaces

- settings studio
- plugin management
- operator dashboards
- playback shell
- admin identity and tenancy views

### Phase 3 - Migration readiness

- journey-by-journey parity mapping
- contract verification
- performance and observability verification
- replacement rollback plan

### Phase 4 - Successor declaration gate

Only after the enterprise-grade and replacement-contract gates are satisfied should the new frontend be declared the successor and the current frontend be put on a retirement path.

## Explicit Non-Goals

The new frontend programme should not:

- optimize around preserving old compatibility quirks as the product target
- collapse the product back into REST compatibility handlers
- assume the current frontend’s route shape is the future information architecture
- introduce remote arbitrary browser plugins as the first extension model
- rely on a broad client-side data layer by default when server-first rendering is the better fit

## Board-Style Summary

Business decision:

- start the new frontend now
- keep it parallel
- do not retire the current frontend yet

Engineering decision:

- use Next.js App Router with TypeScript
- build server-first
- keep BFF/session semantics explicit
- prefer GraphQL for the future platform
- standardize a disciplined design-system primitive layer

Replacement rule:

- the current frontend is only retired after the enterprise-grade gate and the frontend replacement contract gate are both met

## Current Status Label

The correct label today is:

**Begin the new frontend foundation now.**

It is **not** yet correct to say:

**The platform is ready for a full frontend replacement now.**
