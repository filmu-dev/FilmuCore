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
- scoped machine credentials
- internal admin/service roles
- plugin/service capability boundaries
- eventual user-aware audit trails where appropriate

That evolution should not break the current principle that frontend sessions and backend execution auth are separate concerns unless there is a clear product reason to merge them.
