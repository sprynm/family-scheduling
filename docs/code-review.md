# Code Review

_Updated: 2026-03-08 (session 7 — timezone fix)_

## Change Log

### 2026-03-07 — Session 5 review pass

Full read of `src/index.js`, `src/lib/repository.js`, `src/lib/google-calendar.js`, `src/lib/constants.js`, `migrations/0003_output_targets.sql`, `migrations/0004_output_targets_cutover.sql`, `test/index.spec.js`.

Verified: `npm test -- --run` — 25/25 passing.

### 2026-03-08 — Session 7 timezone fix

Fixed floating-time ICS event timezone handling. `X-WR-TIMEZONE` is now read from the VCALENDAR header and used as the fallback timezone for events with no `TZID` param. Floating local datetimes no longer have `Z` appended, so Google Calendar receives `dateTime` without a UTC marker and interprets it correctly in the given `timeZone`.

Verified:
- `npm test -- --run` — 35/35 passing
- Added end-to-end regression coverage for floating-time Google sync payloads

**After deploying:** queue a rebuild for all sources — existing D1 rows store the wrong UTC offset for floating-time events and must be re-ingested.

### 2026-03-08 — Session 6 live-ops review pass

Reviewed live D1 job history and Google sync state after real rebuilds.

Verified:
- `npm test -- --run` — 30/30 passing
- Remote D1 job records now persist real Google errors in `google_event_links.last_error`
- Migration `0005_google_event_links_nullable.sql` applied remotely

---

## Review Result

The previous correctness blockers are fixed. Remaining issues are operational scale concerns and cleanup items, not immediate correctness blockers.

---

## Learnings

- **Calendar-level timezone declarations matter.** A large class of ICS feeds rely on `X-WR-TIMEZONE` plus floating local datetimes instead of per-event `TZID` or UTC `Z` values. Treating those as UTC silently shifts real-world schedules.
- **End-to-end tests are required for time handling.** Parser-only assertions were not enough; the important proof was the actual Google event payload shape (`dateTime` without `Z`, paired with `timeZone`).
- **JOIN-based status aggregates require realistic fixtures.** `google_sync_counts` is intentionally driven by desired output rules, not raw error links. Tests must seed the relational model the same way the application does.
- **Operational errors need to be preserved, not collapsed.** Making `google_event_links.google_event_id` nullable and surfacing `last_error` turned vague “error” states into actionable diagnosis.

---

## What Is Working Well

- **Floating-time ICS events now carry the correct timezone.** `parseICS` reads `X-WR-TIMEZONE` from the VCALENDAR header and passes it as a fallback to `parseDateValue`. Floating local datetimes are stored without a `Z` suffix, so Google Calendar receives them as wall-clock times in the declared timezone rather than as UTC.
- **`index.js` is now 300 lines of pure routing.** The 1,200-line `renderAdminShell()` template literal is gone. Admin UI is served from static assets behind the auth gate. P1 admin migration is complete.
- **All major write paths use `db.batch()`**: `setSourceTargets`, `disableSource`, `deleteSource`, `deleteTarget`, `syncSourceConfig`, `ingestSource`. Transaction tech debt is resolved across all high-traffic paths.
- **`poll_interval_minutes` is now enforced.** `listActiveSources()` filters with `datetime('now', '-' || s.poll_interval_minutes || ' minutes')` — correct SQLite expression.
- **`getSourceById` returns `target_links`.** Previous review issue #4 is fixed — POST and PATCH responses now include link data.
- **Google sync is hash-idempotent.** `last_synced_hash` comparison skips API calls when event content hasn't changed.
- **Dual-mode COALESCE fallback is consistent.** All reads use `COALESCE(output_targets.slug, source_target_links.target_key)` — degrades correctly for rows not yet migrated.
- **`RECURRENCE-ID` exception handling.** `compareEventsForIngest` sorts exceptions after non-exceptions so the parent canonical event exists before the exception shares it. `canonicalEventId` uses UID-only key for exception events — correct.
- **Migration 0004** is safe. Guards inserts with `WHERE NOT EXISTS`, backfills `target_id` on all three dependent tables by slug match, and drops `google_targets` at the end.
- **Migration 0005** fixes a real live-schema mismatch. `google_event_links.google_event_id` is now nullable, allowing failed sync attempts to be recorded instead of crashing the job.
- **Auth is solid.** JWT verification, JWKS caching, audience/issuer validation, key rotation retry, and dev-header bypass remain correct and well-tested.
- **Admin status is now operationally meaningful.** Source rows surface the latest job and Google sync error detail inline, and the dashboard auto-refreshes while jobs are active.
- **Google sync no longer thrashes on quota errors.** Rate-limited writes retry briefly, then defer the rest of the source run instead of continuing to hammer Google.

---

## Issues Found

### 1. Google Sync Still Needs Deeper Batch Scheduling At Scale — Medium

**Files:** [repository.js](../src/lib/repository.js), [google-calendar.js](../src/lib/google-calendar.js)

Live testing showed Google `Rate Limit Exceeded` on larger rebuilds. The new mitigation is correct and necessary:
- brief retry on rate-limit responses
- defer the remainder of the source run after persistent rate-limit failure

That prevents quota hammering, but it does not fully solve catch-up throughput for very large source backfills.

**Recommended next step:** split Google output reconciliation into smaller queueable batches or per-target follow-up jobs so large rebuilds can drain over time without holding a single Worker invocation open.

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

### 4. `&&` Side-Effect Pattern in `listInstances` — Low

**File:** [repository.js:716](../src/lib/repository.js#L716)

```js
if (future) conditions.push('event_instances.occurrence_start_at >= ?') && binds.push(now);
```

`&&` chains `binds.push` as a side effect of the truthy return of `conditions.push`. It works but is non-obvious. Two separate statements is clearer.

---

## Remaining Product Work

1. Prune execution passes FakeDb tests but has not been validated against real D1 + R2 at scale.
2. Admin UI still lacks deeper operator tooling for sync recovery beyond rebuild and status inspection.
3. Large Google backfills still need staged/batched delivery rather than a single source-run attempt.

---

## Current Risk Level

### Operational Follow-Up

1. Queue a full source rebuild after deploying the timezone fix — existing D1 rows store the wrong UTC offset for floating-time events.

### Medium (Address Before Scale)

1. Google sync still needs deeper batch scheduling for large backfills and sustained quota pressure.

### Low (Post-Deploy Cleanup)

1. `ensureSupportTables` per-request cost — remove after 0004 is confirmed deployed.
2. `bootstrapOutputTargets` N+1 backfill — replace with a batch UPDATE.
3. `&&` side-effect in `listInstances`.

---

## Verification

1. `npm test -- --run`
2. 35/35 passing
3. Full read: `src/lib/ics.js`, `src/lib/repository.js`, `test/index.spec.js`
