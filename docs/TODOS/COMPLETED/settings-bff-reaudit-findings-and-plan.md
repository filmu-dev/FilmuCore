# Settings Re-Audit (BFF-Aware): Findings and Implementation Plan

## Scope
- Re-audit of frontend + backend settings behavior, including BFF credential flow.
- Focus on runtime reflection, non-breaking rollout, and operational safety.

## Confirmed Findings

### 1) API key must be handled as a rotation flow (not normal settings save)
- Frontend BFF uses env-based backend key via server locals.
- Settings and API calls go through BFF with that key.
- Backend auth validates against live runtime settings key.
- If key changes in backend settings, existing BFF key becomes stale and protected calls fail.

**Impact:** High. Can break settings page/API operations after key mutation.

### 2) Import-time/class-level settings capture causes stale runtime behavior
- Scraper and ranking settings are captured at import scope.
- Downloader proxy is captured at class scope.
- Valid video extensions are captured at module import.

**Impact:** High. Settings persist but may not apply immediately until process/module refresh.

### 3) Scheduler intervals are startup-bound
- Function and service jobs are scheduled at startup.
- No explicit settings-driven reschedule/rebind path.

**Impact:** High. Interval updates do not reliably reflect at runtime.

### 4) Observer side effects can disrupt in-flight work
- Settings load/save emits observer notifications.
- Existing code comments already indicate observer-triggered reinit can disrupt active operations.

**Impact:** High. Blanket reinit on every settings update is unsafe.

### 5) Library Profiles tab dirty-state baseline is not reset after successful save
- Dirty-state baseline is derived from initial props only.
- Success handler does not update baseline.

**Impact:** Medium. UX reports false unsaved state until reload.

### 6) Logger runtime reconfiguration is not explicit
- Logger configured at import/startup.
- Changes are persisted, but immediate runtime reconfigure path is not guaranteed.

**Impact:** Medium. Operational confusion; delayed effect expectations.

---

## Revised Non-Breaking Implementation Plan

## Phase A — Low-risk correctness fixes
1. **Library Profiles dirty-state fix**
   - Introduce mutable `savedBaselineJson` state.
   - On successful save, set baseline to current serialized local profiles.

2. **Remove import-time settings capture**
   - Replace module/class constants with call-time getters/properties for:
     - scraper settings
     - ranking settings
     - downloader proxy
     - valid video extensions

## Phase B — BFF/API key hardening (highest operational priority)
3. **Separate API key rotation from generic settings save path**
   - Keep dedicated key-rotation endpoint workflow.
   - Exclude direct `api_key` mutation from generic settings submit path.

4. **Strengthen UX and operational messaging**
   - Warn that backend key rotation requires BFF credential alignment (env update/redeploy/restart path).
   - Confirm key copy flow and post-rotation operational steps.

## Phase C — Scoped runtime reflection (avoid global teardown)
5. **Diff-aware settings apply pipeline**
   - Compute changed paths/sections.
   - Trigger only targeted refresh handlers.

6. **Scheduler refresh API**
   - Add controlled `refresh_from_settings(changed_paths)`.
   - Rebind only interval-related jobs and service schedules affected by changed settings.

7. **Logger reconfigure hook**
   - Add idempotent runtime `reconfigure_logger_from_settings()`.
   - Trigger only on log-level/logging section changes.

## Phase D — Stability controls
8. **Coalesce/debounce repeated settings updates** to prevent thrash.
9. **No-op on no-effective-change** (deep compare old/new section payload).

---

## Verification Matrix

### Unit
- Dynamic getter paths reflect settings updates without restart (scrapers/downloaders).
- Library Profiles dirty-state resets immediately after successful save.

### Integration
- Change scheduler intervals and verify APScheduler jobs are rebound live.
- Change log settings and verify runtime logger behavior updates immediately.

### BFF/Operational
- Rotate API key and verify expected behavior:
  - old BFF key fails (expected)
  - documented recovery path succeeds after key alignment.

### Regression
- Settings GET/SET contracts remain unchanged for non-API-key paths.
- No unintended service interruption during routine settings changes.

---

## Risk-Ranked Rollout Order
1. Library Profiles dirty-state fix.
2. Import-time capture fixes (dynamic reads).
3. API key rotation hardening + UX guardrails.
4. Scheduler scoped refresh + logger reconfigure hooks.
5. Full integration/regression pass.

---

## Delivery Notes
- Plan prioritizes runtime safety and BFF continuity.
- Avoids broad observer-triggered global service reinitialization.
- Preserves existing settings API behavior wherever possible.
