# Frontend API Keys

This document outlines the frontend-facing API keys and authorization tokens used by the `Triven_frontend` codebase. Some remain hardcoded, while others are now expected to come from environment variables exposed to the frontend/BFF runtime.

## 1. TMDB Read Access Token (JWT)

**Location:** `src/lib/providers/index.ts` (Lines 30-32)

**Current behavior:**

- The frontend now expects `PUBLIC_TMDB_READ_ACCESS_TOKEN` from the environment.
- In the current local stack, [`docker-compose.local.yml`](../docker-compose.local.yml) forwards that value into the [`frontend`](../docker-compose.local.yml) container.
- The token is then sent as a bearer token from [`src/lib/providers/index.ts`](../../Triven_riven-fork/Triven_frontend/src/lib/providers/index.ts).

**Why it exists:**
The frontend fetches TMDB data (trending movies, popular shows, top rated lists, and item details) directly from `https://api.themoviedb.org/3`. Using a read-access bearer token keeps the frontend/BFF on the TMDB-supported bearer flow for v3 requests.

## 2. TVDB API Key

**Location:** `src/routes/(protected)/details/media/[id]/[mediaType]/+page.server.ts` (Line 19)

**Value:**
```typescript
const TVDB_API_KEY = "6be85335-5c4f-4d8d-b945-d3ed0eb8cdce";
```

**Why it exists:**
When the frontend resolves details for a TV show, it sometimes needs to interact with the TVDB API (v4). The TVDB v4 API requires an authentication flow: the frontend sends this API key to `/login` to receive a short-lived bearer token (which is then stored in a cookie `tvdb_cookie`). This hardcoded key allows out-of-the-box metadata resolution for episodes, seasons, and artwork without user configuration.

## 3. Trakt Client ID

**Location:** `src/lib/providers/index.ts` (Lines 47-49)

**Value:**
```typescript
"trakt-api-key":
    env.PUBLIC_TRAKT_CLIENT_ID ||
    "0183a05ad97098d87287fe46da4ae286f434f32e8e951caad4cc147c947d79a3"
```

**Why it exists:**
If the frontend integrates with Trakt (e.g., for scrobbling or syncing watch states, or displaying Trakt ratings), it must provide a Client ID (`trakt-api-key` header). This built-in client ID allows default Trakt integrations to function out-of-the-box. It can be overridden via the `PUBLIC_TRAKT_CLIENT_ID` environment variable.

## 4. Rotten Tomatoes / Algolia API Key

**Location:** `src/routes/(protected)/api/ratings/[tmdbId]/+server.ts` (Line 13)

**Value:**
```typescript
const RT_ALGOLIA_API_KEY = "175588f6e5f8319b27702e4cc4013561";
```

**Why it exists:**
To retrieve Rotten Tomatoes ratings, the frontend performs an internal proxy fetch to Algolia (which powers Rotten Tomatoes' search). This specific Algolia API key acts as the authorization token (`x-algolia-api-key`) required to query the Rotten Tomatoes index (`flixster_movies_prod`) directly from the browser/BFF, bypassing the need for a dedicated ratings scraper backend.

---

### Architectural Note for FilmuCore

Because the frontend still calls some third-party metadata surfaces directly, the FilmuCore backend does not strictly need to proxy these specific image/metadata resolutions.

However, if FilmuCore needs to perform backend-side operations (like TMDB ID resolution during scraping or metadata enrichment in `request_item`), it must maintain its own backend TMDB API key through [`TMDB_API_KEY`](../filmu_py/config.py:542). The frontend TMDB bearer token and the backend TMDB API key are separate credentials for separate runtime paths.

For scraper and downloader integrations, FilmuCore follows the dual-surface settings model documented in [`docs/ARCHITECTURE.md`](./ARCHITECTURE.md): values edited through the frontend settings page are persisted through [`/api/v1/settings/*`](../filmu_py/api/routes/settings.py:125), hydrated back into typed runtime settings, and then consumed by worker/runtime code as the source of truth. Bootstrap env vars are not supposed to override saved scraper/downloader settings at scrape time.

For library cards specifically, the current compatibility backend still needs backend-side TMDB enrichment to write [`poster_path`](../filmu_py/services/media.py:1965) into item metadata. The frontend TMDB bearer token does not backfill that field into the backend library payload automatically.
