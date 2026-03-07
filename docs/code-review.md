# Code Review

_Updated: 2026-03-07 (session 5 — post-refactor)_

## Change Log

### 2026-03-07 — Session 5 review pass

Full read of `src/index.js`, `src/lib/repository.js`, `src/lib/google-calendar.js`, `src/lib/constants.js`, `migrations/0003_output_targets.sql`, `migrations/0004_output_targets_cutover.sql`, `test/index.spec.js`.

Verified: `npm test -- --run` — 25/25 passing.

---

## Review Result

The previous priority items are fixed. Remaining items are scale and cleanup concerns, not correctness blockers.

---

## What Is Working Well

- **`index.js` is now 300 lines of pure routing.** The 1,200-line `renderAdminShell()` template literal is gone. Admin UI is served from static assets behind the auth gate. P1 admin migration is complete.
- **All major write paths use `db.batch()`**: `setSourceTargets`, `disableSource`, `deleteSource`, `deleteTarget`, `syncSourceConfig`, `ingestSource`. Transaction tech debt is resolved across all high-traffic paths.
- **`poll_interval_minutes` is now enforced.** `listActiveSources()` filters with `datetime('now', '-' || s.poll_interval_minutes || ' minutes')` — correct SQLite expression.
- **`getSourceById` returns `target_links`.** Previous review issue #4 is fixed — POST and PATCH responses now include link data.
- **Google sync is hash-idempotent.** `last_synced_hash` comparison skips API calls when event content hasn't changed.
- **Dual-mode COALESCE fallback is consistent.** All reads use `COALESCE(output_targets.slug, source_target_links.target_key)` — degrades correctly for rows not yet migrated.
- **`RECURRENCE-ID` exception handling.** `compareEventsForIngest` sorts exceptions after non-exceptions so the parent canonical event exists before the exception shares it. `canonicalEventId` uses UID-only key for exception events — correct.
- **Migration 0004** is safe. Guards inserts with `WHERE NOT EXISTS`, backfills `target_id` on all three dependent tables by slug match, and drops `google_targets` at the end.
- **Auth is solid.** JWT verification, JWKS caching, audience/issuer validation, key rotation retry, and dev-header bypass remain correct and well-tested.

---

## Issues Found

### 1. Ingest Batch Size Is Unbounded — Medium

**File:** [repository.js:1752](../src/lib/repository.js#L1752)

`ingestSource` builds all statements into a single `db.batch([...])`. For a source with 200 events × 10 instances × 5 targets: 10,000+ output_rule inserts plus source_events and event_instances — one batch. D1 doesn't document a hard batch limit, but large batches have been observed to time out in production.

**Fix:** Chunk the batch — flush every 500–1,000 statements and reset.

---

### 2. `ensureSupportTables` / `bootstrapOutputTargets` Run Per-Request — Low

**File:** [repository.js:194](../src/lib/repository.js#L194), [repository.js:1905](../src/lib/repository.js#L1905)

`createRepository` → `ensureSupportTables` → `bootstrapOutputTargets` fires on every HTTP request. After migration 0004 is confirmed deployed, the `bootstrapOutputTargets` backfill loop (which queries for `target_id IS NULL` rows) will always return zero rows and the DDL statements are all no-ops. Still costs real D1 round trips.

**Fix:** Add a module-level flag to skip after first success within a Worker isolate lifetime.

---

### 3. `bootstrapOutputTargets` Backfill Has N+1 Pattern — Low

**File:** [repository.js:336](../src/lib/repository.js#L336)

The backfill loop fetches all `source_target_links WHERE target_id IS NULL`, then calls `getOutputTargetBySlug` per row individually. On a clean post-0004 system this is a no-op. On first deploy with many legacy links it could be slow.

**Fix:** Replace the loop with a single `UPDATE source_target_links SET target_id = (SELECT id FROM output_targets WHERE slug = target_key) WHERE target_id IS NULL`.

---

### 4. `TARGETS` / `logicalTargets` Still Exposed via API — Low

**File:** [index.js:131](../src/index.js#L131)

```js
return json({
  targets: await repo.listTargets(),
  logicalTargets: TARGETS,
});
```

System ICS targets are already in `listTargets()` with `is_system: 1`. The `logicalTargets` field exposes the hardcoded array redundantly. If the admin UI still consumes `logicalTargets`, that dependency should be removed and the field dropped.

---

### 5. `&&` Side-Effect Pattern in `listInstances` — Low

**File:** [repository.js:716](../src/lib/repository.js#L716)

```js
if (future) conditions.push('event_instances.occurrence_start_at >= ?') && binds.push(now);
```

`&&` chains `binds.push` as a side effect of the truthy return of `conditions.push`. It works but is non-obvious. Two separate statements is clearer.

---

## Remaining Product Work

1. Prune execution passes FakeDb tests but has not been validated against real D1 + R2 at scale.
2. Admin UI has no edit workflow for existing sources or existing target rules beyond current create/disable flows.

---

## Current Risk Level

### Fix Before Merge

None.

### Medium (Address Before Scale)

1. Ingest batch unbounded — large sources with many targets risk D1 timeout.

### Low (Post-Deploy Cleanup)

1. `ensureSupportTables` per-request cost — remove after 0004 is confirmed deployed.
2. `bootstrapOutputTargets` N+1 backfill — replace with a batch UPDATE.
3. `logicalTargets` legacy exposure in `/api/targets` response.
4. `&&` side-effect in `listInstances`.

---

## Verification

1. `npm test -- --run`
2. 25/25 tests passed
3. Full read: `src/index.js`, `src/lib/repository.js`, `src/lib/google-calendar.js`, `src/lib/constants.js`, `migrations/0003_output_targets.sql`, `migrations/0004_output_targets_cutover.sql`, `test/index.spec.js`
