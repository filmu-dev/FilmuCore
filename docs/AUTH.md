# Backend Authentication Model

## Purpose

Describe how authentication and authorization currently work between the frontend and `filmu-python`, and clarify which layer is responsible for user sessions versus backend service access.

External dependency note:

- The `Triven_frontend` paths referenced in this document point to an external frontend repository and are not part of this workspace.
- This document keeps those references for architecture context, but they should not be read as implying the frontend source is available locally inside this repo.

---

## Current model

The current system uses a split auth model:

1. **Frontend user authentication**
   - handled by the frontend application and its protected-route/session model
   - the current frontend does this in [`hooks.server.ts`](../../../Triven_frontend/src/hooks.server.ts)

2. **Backend API authentication**
   - handled by a backend API key by default, or by validated OIDC bearer tokens when `FILMU_PY_OIDC` is enabled
   - validated by [`verify_api_key()`](../filmu_py/api/deps.py)
   - now also persisted into first-class tenant/principal/service-account records through [`SecurityIdentityService`](../filmu_py/services/identity.py)

This means the backend currently authenticates the frontend server/BFF layer, not the end user directly.

---

## Why this split exists

This fits the current architecture principle that the backend is the motor and the frontend is the primary UX layer.

The frontend:

- owns user login/session behavior
- controls protected route access
- proxies backend requests through a server-side layer
- keeps the backend API key out of the browser

The backend:

- exposes stable execution contracts
- validates trusted server-to-server access
- does not yet implement a user-facing session model of its own

---

## Backend-side API key validation

The backend currently accepts the API key from standard compatibility locations in [`verify_api_key()`](../filmu_py/api/deps.py):

- `x-api-key` header
- `Authorization: Bearer ...` header
- `api_key` query parameter

All `/api/v1/*` routes are protected by router-level dependency wiring in [`create_api_router()`](../filmu_py/api/router.py).

When `FILMU_PY_OIDC.enabled=true`, `Authorization: Bearer ...` can instead carry an OIDC JWT. The backend validates:

- configured issuer and audience
- allowed signing algorithm
- JWKS from either inline `jwks_json` or discovered/cached `jwks_url`
- token signature, expiry, subject, and claims before building the actor context

Invalid OIDC bearer tokens fail closed with `401`; they are not treated as unsigned identity hints.

The request dependency also now persists a first-class identity-plane baseline:

- [`TenantORM`](../filmu_py/db/models.py), [`PrincipalORM`](../filmu_py/db/models.py), and [`ServiceAccountORM`](../filmu_py/db/models.py) are created by migration [`20260410_0022_identity_and_tenancy.py`](../filmu_py/db/alembic/versions/20260410_0022_identity_and_tenancy.py)
- the startup path bootstraps the default `global` tenant plus the primary service account in [`create_app()`](../filmu_py/app.py)
- authenticated requests now upsert tenant/principal/service-account metadata for auditability instead of leaving actor headers purely ephemeral
- operators can inspect the resolved request identity on [`GET /api/v1/auth/context`](../filmu_py/api/routes/default.py)

This is still not full authz. It is a persisted control-plane baseline above the earlier header-only model.

The control plane is also stricter than the earlier baseline:

- privileged compatibility mutations now require explicit `x-actor-roles` values such as `platform:admin`
- API-key authentication no longer implies admin privileges automatically
- the backend now computes `effective_permissions` from roles, scopes, and settings-backed role grants and exposes them on [`GET /api/v1/auth/context`](../filmu_py/api/routes/default.py)
- the backend now also evaluates tenant-aware authorization decisions through [`evaluate_permissions()`](../filmu_py/authz.py) instead of only checking whether a permission string exists
- [`GET /api/v1/auth/policy`](../filmu_py/api/routes/default.py) exposes standard authorization probes, matched and missing permissions, tenant-scope classification, OIDC validation state, access-policy version/source, role grants, warnings, and remaining policy gaps for the current actor
- startup now bootstraps a persisted access-policy revision through [`AccessPolicyService`](../filmu_py/services/access_policy.py), and settings saves refresh the active revision instead of leaving policy inventory purely process-local
- privileged authorization checks now emit allow/deny audit events when policy decision auditing is enabled, so policy evaluation is observable rather than only implicit in HTTP results
- tenant-aware intake paths now persist the resolved `tenant_id` on created `media_items` and `item_requests`
- tenant-scoped reads now also reach item detail/listing, calendar, and stats surfaces instead of stopping at write-time persistence

The request identity surface now also carries delegated tenant scope and OIDC identity fields:

- `x-actor-authorized-tenants` can declare delegated tenant scope for cross-tenant control-plane reads
- `x-auth-issuer` and `x-auth-subject` remain compatibility hints for API-key gateway traffic, but they are explicitly reported as not token-validated
- validated OIDC tokens can derive actor id, tenant id, authorized tenants, roles, and scopes from configured claims
- [`GET /api/v1/auth/context`](../filmu_py/api/routes/default.py) now returns `authorized_tenant_ids`, `authorization_tenant_scope`, `oidc_issuer`, `oidc_subject`, `oidc_token_validated`, `access_policy_version`, and `quota_policy_version` alongside `effective_permissions`

Remaining identity gaps: OIDC/SSO is now real but still rollout-gated; persisted policy revisions exist but operator-managed CRUD/version approval workflows do not; ABAC is still mostly permission plus tenant-scope based; frontend session-to-backend subject mapping still needs product-specific rollout.

---

## Frontend-to-backend flow

In the current frontend:

- protected routes require a frontend user session in [`hooks.server.ts`](../../../Triven_frontend/src/hooks.server.ts)
- backend credentials are injected into server locals in [`configureLocals`](../../../Triven_frontend/src/hooks.server.ts)
- server routes and BFF handlers call the backend using `BACKEND_URL` + `BACKEND_API_KEY`

So the browser does not directly hold the backend key.

---

## Rotation caveat

API key rotation is operationally sensitive because the frontend BFF and backend must stay aligned.

If the backend key changes but the frontend server environment is not updated at the same time, protected backend calls will fail until the BFF side is updated and restarted/redeployed.

Practical recovery path:

1. rotate the backend API key deliberately, not as part of an unplanned generic settings mutation
2. update `BACKEND_API_KEY` in the frontend/BFF environment
3. restart or redeploy the frontend server layer so it begins using the new key
4. verify protected backend calls recover through the BFF

Separate historical planning notes exist outside this workspace, but the operational consequence is summarized here so this document remains self-contained.

Current compatibility note:

- [`/api/v1/generateapikey`](../filmu_py/api/routes/default.py) now rotates the live backend runtime key immediately and persists it through the backend settings store
- that same route now rotates and returns `api_key_id`, and the identity baseline persists the new key identifier onto the resolved service account when actor context is available
- the response also includes an explicit operator warning telling the caller to update `BACKEND_API_KEY` in the frontend/BFF environment and restart the frontend server before making the next protected request
- this means rotation is now real, but rollover is still **operator-coordinated**, not automatically synchronized across backend and frontend
- generic settings persistence should still treat API key changes as an operationally sensitive workflow, not as a casual background mutation

---

## Local testing implications

For local frontend + python-backend testing:

- frontend auth/session can already work independently on the frontend side
- backend testing requires a valid `BACKEND_API_KEY` configured in the frontend environment
- local integration is therefore possible without implementing a separate backend user/session model first

Readiness details are maintained in the authoritative document [`LOCAL_FRONTEND_TESTING_READINESS.md`](LOCAL_FRONTEND_TESTING_READINESS.md).

---

## Future evolution

The current API-key model is acceptable for the present BFF architecture, but the backend should stay open to future evolution if needed:

- stronger service-to-service auth
- scoped machine credentials above the new persisted `ServiceAccountORM` baseline
- internal admin/service roles
- broader RBAC/ABAC policy evaluation above the current `effective_permissions`, persisted policy revision, and tenant-aware authorization baseline
- plugin/service capability boundaries
- eventual user-aware audit trails, real OIDC/SSO integration, and tenant-scoped authorization beyond the current delegated-tenant compatibility headers

That evolution should not break the current principle that frontend sessions and backend execution auth are separate concerns unless there is a clear product reason to merge them.
