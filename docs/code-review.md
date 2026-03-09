# Code Review

_Updated: 2026-03-09 (session 11 — override immediacy and mobile cleanup)_

## Change Log

### 2026-03-07 — Session 5 review pass

Full read of `src/index.js`, `src/lib/repository.js`, `src/lib/google-calendar.js`, `src/lib/constants.js`, `migrations/0003_output_targets.sql`, `migrations/0004_output_targets_cutover.sql`, `test/index.spec.js`.

Verified: `npm test -- --run` — 25/25 passing.

### 2026-03-08 — Session 6 live-ops review pass

Reviewed live D1 job history and Google sync state after real rebuilds.

Verified:
- `npm test -- --run` — 30/30 passing
- Remote D1 job records now persist real Google errors in `google_event_links.last_error`
- Migration `0005_google_event_links_nullable.sql` applied remotely

### 2026-03-08 — Session 7 timezone fix

Fixed floating-time ICS event timezone handling. `X-WR-TIMEZONE` is now read from the VCALENDAR header and used as the fallback timezone for events with no `TZID` param. Floating local datetimes no longer have `Z` appended, so Google Calendar receives `dateTime` without a UTC marker and interprets it correctly in the given `timeZone`.

Verified:
- `npm test -- --run` — 35/35 passing
- Added end-to-end regression coverage for floating-time Google sync payloads

**After deploying:** queue a rebuild for all sources — existing D1 rows store the wrong UTC offset for floating-time events and must be re-ingested.

### 2026-03-08 — Session 8 parser normalization

Fixed RFC5545 text unescaping on ingest. `parseICS` now unescapes escaped text in `SUMMARY`, `DESCRIPTION`, and `LOCATION`, so upstream feeds that encode line breaks as `\\n` and punctuation as `\\,` / `\\;` display correctly throughout the system.

Verified:
- `npm test -- --run` — 36/36 passing
- Added regression coverage for RFC5545 text unescaping

### 2026-03-08 — Session 9 mobile event workflow

Added a dedicated `/admin/events` page served from static assets behind the existing auth gate. This gives event modification a focused mobile-first surface instead of forcing phone users through the full configuration console.

Verified:
- `npm test -- --run` — 38/38 passing
- Added route coverage for `/admin/events`

### 2026-03-08 — Session 10 mobile events UI review

Reviewed `public/admin-events.html`, `public/admin-events.js`, and `public/admin.css` against the live behavior. The main actionable issues are duplicate instance rows and full-list rerender on drawer toggle. Several other findings from the first pass were reclassified as lower-value UX follow-up rather than bugs.

### 2026-03-08 — Session 10 asset binding fix

Fixed the runtime issue where `/admin` and `/admin/events` returned `Admin assets binding is required`. `wrangler.jsonc` now binds static assets explicitly as `ASSETS`.

### 2026-03-09 — Session 11 immediate override and mobile cleanup

Fixed the session-10 mobile defects and the follow-on repository issues:
- single-instance overrides now update derived output state immediately and queue Google reconciliation without waiting for cron or rebuild
- `/admin/events` deduplicates multi-output instance rows and toggles drawers in place
- repository bootstrap now short-circuits after first success in an isolate
- legacy target backfill now uses set-based updates
- `listInstances` no longer relies on `conditions.push(...) && binds.push(...)`
- normalized system target identity so output-rule upserts do not create duplicate rows when legacy key-based rows meet ID-based reconciles

Verified:
- `npm test -- --run` — 39/39 passing

---

## Review Result

The previous correctness blockers and the session-10 mobile defects are fixed. Remaining work is operational follow-through, not an identified application bug.

---

## Learnings

- **Calendar-level timezone declarations matter.** A large class of ICS feeds rely on `X-WR-TIMEZONE` plus floating local datetimes instead of per-event `TZID` or UTC `Z` values. Treating those as UTC silently shifts real-world schedules.
- **End-to-end tests are required for time handling.** Parser-only assertions were not enough; the important proof was the actual Google event payload shape (`dateTime` without `Z`, paired with `timeZone`).
- **JOIN-based status aggregates require realistic fixtures.** `google_sync_counts` is intentionally driven by desired output rules, not raw error links. Tests must seed the relational model the same way the application does.
- **Operational errors need to be preserved, not collapsed.** Making `google_event_links.google_event_id` nullable and surfacing `last_error` turned vague `error` states into actionable diagnosis.
- **Parser normalization should happen once, at ingest.** Upstream ICS escaping rules are part of the data contract; if they are not normalized in the parser, bad literals leak into admin, generated ICS, and Google output sync alike.
- **Primary mobile workflows deserve their own surface.** Event correction is operational work, not configuration. A split admin is cleaner than forcing one dense desktop-first page to do both jobs well on a phone.
- **Authenticated static assets need an explicit binding.** In this Worker setup, serving the admin shell through the route/auth layer requires `assets.binding = "ASSETS"`; the directory setting alone is insufficient.
- **Bug review and UX review are not the same thing.** Session 10 was useful, but the first pass mixed two real defects with several preference-level UX suggestions. Distinguishing those keeps the backlog focused.
- **Immediate admin actions need immediate derived-state updates.** For event correction, persisting an override without recomputing its output scope is effectively a broken interaction, even if the row is stored correctly.
- **System target identity has to be canonicalized.** Mixing slug-based and ID-based identities for built-in outputs created duplicate `output_rules` during override reconciliation. The fix was to normalize system targets to the same stable `output_targets.id` path everywhere.

---

## What Is Working Well

- **Floating-time ICS events now carry the correct timezone.** `parseICS` reads `X-WR-TIMEZONE` from the VCALENDAR header and passes it as a fallback to `parseDateValue`. Floating local datetimes are stored without a `Z` suffix, so Google Calendar receives them as wall-clock times in the declared timezone rather than as UTC.
- **RFC5545 escaped text now round-trips correctly.** `parseICS` unescapes `\\n`, `\\,`, and `\\;` in `SUMMARY`, `DESCRIPTION`, and `LOCATION`, while `buildICS` still escapes outbound values correctly.
- **Admin UI is served from static assets behind the auth gate.** The template-literal shell is gone, and the Worker/asset binding now matches the runtime contract.
- **All major write paths use `db.batch()`.** `setSourceTargets`, `disableSource`, `deleteSource`, `deleteTarget`, `syncSourceConfig`, and `ingestSource` are off the old faux-transaction pattern.
- **`poll_interval_minutes` is enforced.** `listActiveSources()` now filters source selection by scheduler timing.
- **`getSourceById` returns `target_links`.** POST and PATCH responses now include link data.
- **Google sync is hash-idempotent.** `last_synced_hash` comparison skips API calls when event content hasn't changed.
- **Migration `0005` fixed a real live-schema mismatch.** Failed sync attempts can now be recorded instead of crashing the job.
- **Admin status is operationally meaningful.** Source rows surface latest job and Google sync detail inline, and the dashboard auto-refreshes while jobs are active.
- **Google sync no longer thrashes on quota errors.** Rate-limited writes retry briefly, then defer the rest of the source run instead of continuing to hammer Google.
- **Event modification has a dedicated mobile path.** `/admin/events` uses the existing instance/override APIs with a focused layout rather than the configuration-heavy main admin page.
- **Single-instance override effects are immediate.** Override create/delete now recomputes the affected instance scope immediately and queues Google reconciliation on the existing sync path, so the admin no longer waits for a rebuild to see the result.
- **The mobile event page no longer duplicates or flashes rows.** `/admin/events` now deduplicates multi-output instances and toggles drawers in place.
- **Repository bootstrap overhead is reduced.** Support-table/bootstrap work is skipped after first success within an isolate, and the legacy backfill path is set-based rather than N+1.

---

## Issues Found

No active code defects were identified in this pass after the session-11 fixes. The remaining work is operational and product follow-through:

1. Import any still-relevant legacy sources from `cals.txt`.
2. Validate prune behavior against real D1 + R2.
3. Write the production migration runbook.
4. Add richer operator sync drill-down if the current inline summary is still insufficient in practice.

---

## Remaining Product Work

1. Prune execution passes FakeDb tests but has not been validated against real D1 + R2 at scale.
2. Admin UI still lacks deeper operator tooling for sync recovery beyond rebuild and status inspection.
3. Existing sources affected by the timezone fix still need rebuilds so corrected values replace already-ingested rows.
4. Legacy source inventory in `cals.txt` still needs to be reconciled against the new system.
5. `reconcileOverrideScope` currently writes per-instance x per-target `output_rules` on each override action. That is acceptable for current scale, but it should be watched if single events ever fan out into very large instance sets.

---

## Current Risk Level

### Operational Follow-Up

1. Queue a full source rebuild after deploying the timezone fix — existing D1 rows store the wrong UTC offset for floating-time events.

### Medium (Operational Completion)

1. Validate prune behavior on real infrastructure.
2. Document the production migration/deploy runbook.
3. Reconcile legacy source inventory against the new system.

---

## Verification

1. `npm test -- --run`
2. 39/39 passing
3. Full read: `src/index.js`, `src/lib/ics.js`, `src/lib/repository.js`, `public/admin.html`, `public/admin-events.html`, `public/admin-events.js`, `public/admin.css`, `wrangler.jsonc`, `test/index.spec.js`
