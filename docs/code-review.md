# Code Review

_Updated: 2026-03-06_

## Change Log

### 2026-03-06 — Fix issues #1 and #2

**Issue #1 — N+1 query in `ingestSource`** ([repository.js:1133](../src/lib/repository.js#L1133))

Moved `resolveTargetsForSource(source)` to execute once before `runInTransaction`, storing the result in `targets`. The inner instance loop now iterates over that resolved array instead of re-querying the DB on every instance.

**Issue #2 — `rrule` not persisted** ([repository.js:1179](../src/lib/repository.js#L1179))

The `INSERT INTO canonical_events` VALUES clause previously had `NULL` hardcoded at the `rrule` column position, causing the bound `event.rrule` value to be silently ignored. Changed `NULL` to `?` so the bound value is used. Also changed the bind from `event.rrule` to `event.rrule || null` to ensure a proper SQL null (not an empty string) when no rrule is present.

Verification: `npm test -- --run` — 18 tests passed.

---

## Review Result

No blocking issues. Several low-risk correctness gaps and one real performance problem worth addressing before production traffic increases.

---

## What Is Working Well

- Auth is solid. JWT verification, JWKS caching, audience/issuer validation, key rotation retry, and dev-header bypass are all correct and well-tested.
- `source_target_links` per-target decoration is correctly wired end-to-end: stored in migration 0002, joined in `generateFeed` with a proper COALESCE fallback chain, and consumed by `decorateEventSummary`.
- Google target deletion cascades correctly to `source_target_links`, `output_rules`, and `google_event_links`.
- `resolveTargetsForSource` falls back to legacy boolean columns when no links exist — clean bridge for bootstrapped legacy sources.
- ICS parser handles folded lines, DATE-only values, floating-time, and UTC-Z forms.
- Test coverage is meaningful: root redirect, per-target rule persistence, target delete cleanup, per-target feed decoration, JWT auth, and ingest path all have assertions.

---

## Issues Found

### 1. N+1 Query in `ingestSource` — Medium Priority

**File:** [repository.js:1236](src/lib/repository.js#L1236)

`resolveTargetsForSource(source)` is called inside the per-instance loop inside a transaction. For a source with 50 events × 10 instances each, this fires 500 DB queries.

```js
// Current (inside instance loop):
for (const target of await this.resolveTargetsForSource(source)) { ... }
```

Fix: resolve targets once before the staged-events loop begins. The targets don't change during a single ingest.

```js
const targets = await this.resolveTargetsForSource(source);
for (const instance of instances) {
  for (const target of targets) { ... }
}
```

---

### 2. `rrule` Not Persisted on `canonical_events` — Low Priority (Now), Potential Future Bug

**File:** [repository.js:1176](src/lib/repository.js#L1176)

The `INSERT INTO canonical_events` VALUES clause hardcodes `NULL` at the `rrule` position. The bind list passes `event.rrule` as the 14th argument, but because the SQL literal `NULL` occupies that column slot, the value is never stored.

```sql
-- rrule column gets NULL regardless of event.rrule:
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, 0, ?, ?, ?)
```

This is not currently a functional bug because feed generation reads from pre-expanded `event_instances`, not from `canonical_events.rrule`. But the column is useless as-is, `event_kind = 'series'` gets set correctly while `rrule` stays NULL, and any future feature that reads it back will be broken. Fix by replacing the `NULL` literal with a `?` and binding `event.rrule` in the correct position.

---

### 3. `ensureSupportTables` Called on Every Request — Low Priority

**File:** [repository.js:1270-1281](src/lib/repository.js#L1270)

`createRepository` is called on every HTTP request and calls `ensureSupportTables`, which fires `CREATE TABLE IF NOT EXISTS`, `CREATE UNIQUE INDEX IF NOT EXISTS`, and three `ALTER TABLE` statements on each request. These are no-ops after first run but still cost real D1 round trips.

Options:
- Use a module-level flag to skip after first success within a Worker isolate lifetime.
- Accept the cost as a deploy-time migration step and remove `ensureSupportTables` from the hot path once 0002 migration is applied to production.

---

### 4. `getSourceById` Omits `target_links` — Low Priority

**File:** [repository.js:828](src/lib/repository.js#L828)

`listSources` returns sources enriched with `target_links` and `target_keys`. `getSourceById` returns a raw row with neither. `createSource` and `updateSource` both return via `getSourceById`, so the API response for POST and PATCH omits target link data even when links were just written.

This is a minor API inconsistency — the caller can see the source was created but can't see what links were created without a separate list call.

---

### 5. Hardcoded Host in `getFeedContract` — Fixed ✓

**File:** [repository.js:574](src/lib/repository.js#L574)

**Fixed 2026-03-06.** `getFeedContract(requestUrl)` now derives the host from `env.PUBLIC_HOST` if set, falling back to the origin of the incoming request URL, and finally to the hardcoded production hostname as a last resort.

---

### 6. Admin Shell Auth — ~~Fixed~~ ✓

**Fixed 2026-03-06.** `GET /admin` now enforces `requireRole(['admin','editor'])` in the Worker before returning HTML. Both the network-layer Cloudflare Access check and the Worker-level role check are active in production.

Note: local dev with `ALLOW_DEV_ROLE_HEADER=true` still requires the `x-user-role` header to be present on `/admin` requests.

---

### 7. XSS Risk in Admin Shell `innerHTML` Construction — Very Low Risk

**File:** [index.js:535-547](src/index.js#L535)

The target rule card HTML is built via string concatenation:

```js
'<strong>' + option.label + '</strong>'
```

`option.label` comes from `target_key` which is normalized through `normalizeTargetKey` (alphanumeric, underscore, hyphen only), so injection is not possible in practice. However, Google target labels use the raw `option.textContent`, which is populated from server data. If a label were ever set through a path that skips normalization it could XSS the admin. Low risk, but a textContent assignment or DOM API instead of innerHTML would remove the class of risk entirely.

---

### 8. FakeDb Feed Query Does Not Filter `source_deleted` — Test Fidelity Gap

**File:** [test/index.spec.js:706-728](test/index.spec.js#L706)

The `FakeDb.runAll` handler for the feed generation query (`FROM output_rules JOIN canonical_events`) does not filter on `canonical_events.source_deleted = 0` or `event_instances.source_deleted = 0`, unlike the real SQL at [repository.js:609-610](src/lib/repository.js#L609). Deleted events would appear in test feeds that shouldn't have them. The test for `disableSource` currently doesn't verify feed output after disable, so this gap isn't caught.

---

### 9. `poll_interval_minutes` Is Inert — Low Priority

**File:** [repository.js ~line 1020](src/lib/repository.js#L1020), [index.js ~line 1431](src/index.js#L1431)

The `poll_interval_minutes` field is documented as controlling how often a source is re-ingested. In practice the cron handler enqueues every active source on every tick unconditionally. The field is stored and displayed but has no effect at runtime.

Fix: filter the source list in the cron handler to only enqueue sources whose `last_fetched_at` is older than their `poll_interval_minutes` value.

---

### 10. `deleteSource()` Cascade Is Non-Atomic — Fixed ✓

**File:** [repository.js ~line 390](src/lib/repository.js#L390)

**Fixed 2026-03-06.** Converted to a single `db.batch([...])` call. All 9 cascade deletes are now atomic.

---

### 11. Source Status Display Used Wrong `parse_status` Values — Fixed ✓

**Fixed 2026-03-06.** The admin Sources table showed "error" for all sources because the UI only checked for `'ok'` and `'success'`, while the ingest code writes `'parsed'` and `'parsed_no_blob'`. Fixed: `parseOk` now accepts all four values.

---

## Remaining Product Work

These are implementation gaps, not review findings:

1. Google Calendar outbound sync is not implemented.
2. Recurrence exception/remap (`RECURRENCE-ID` events) are parsed but not remapped to existing instances — they create new canonical events instead.
3. Prune execution is scaffolded and tested against FakeDb but not validated against real D1 + R2.
4. Admin UI has no edit workflow for existing sources or existing target rules — only create and disable.

---

## Current Risk Level

### Low

1. Auth correctness and JWT validation path.
2. Per-target rule storage and feed rendering.
3. Google target delete cascade.
4. Root redirect and admin routing.

### Medium

1. N+1 query in `ingestSource` — tolerable at current scale, becomes a problem with many recurring sources.
2. `rrule` not persisted — silent data loss that will matter if recurrence hardening reads the stored value.
3. Real-environment D1 migration/upgrade path — `ensureSupportTables` works but is not a production migration strategy.

### Deferred Until Google Sync

1. Google outbound publish behavior and conflict handling.
2. `google_event_links` sync state lifecycle.

---

## Verification

Verified in this pass:

1. `npm test -- --run`
2. 18 tests passed
3. Full read of `src/index.js`, `src/lib/repository.js`, `src/lib/auth.js`, `src/lib/ics.js`, `src/lib/presentation.js`, `src/lib/constants.js`, `migrations/0001_init.sql`, `migrations/0002_source_target_links.sql`, `test/index.spec.js`
