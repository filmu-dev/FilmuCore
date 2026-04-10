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
   - handled by a backend API key
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

The request dependency also now persists a first-class identity-plane baseline:

- [`TenantORM`](../filmu_py/db/models.py), [`PrincipalORM`](../filmu_py/db/models.py), and [`ServiceAccountORM`](../filmu_py/db/models.py) are created by migration [`20260410_0022_identity_and_tenancy.py`](../filmu_py/db/alembic/versions/20260410_0022_identity_and_tenancy.py)
- the startup path bootstraps the default `global` tenant plus the primary service account in [`create_app()`](../filmu_py/app.py)
- authenticated requests now upsert tenant/principal/service-account metadata for auditability instead of leaving actor headers purely ephemeral
- operators can inspect the resolved request identity on [`GET /api/v1/auth/context`](../filmu_py/api/routes/default.py)

This is still not full authz. It is a persisted control-plane baseline above the earlier header-only model.

The control plane is also stricter than the earlier baseline:

- privileged compatibility mutations now require explicit `x-actor-roles` values such as `platform:admin`
- API-key authentication no longer implies admin privileges automatically
- tenant-aware intake paths now persist the resolved `tenant_id` on created `media_items` and `item_requests`

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
- plugin/service capability boundaries
- eventual user-aware audit trails and tenant-scoped authorization above the new persisted identity catalog

That evolution should not break the current principle that frontend sessions and backend execution auth are separate concerns unless there is a clear product reason to merge them.
