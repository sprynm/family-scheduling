# Todo

Prioritized work items. Completed work stays below as historical reference; only active work remains in the backlog.

---

## Active Tickets

### Ticket: Import missing legacy sources from `cals.txt`
**Why:** The legacy system's source list in `cals.txt` may not be fully migrated to the new system. Any missing sources mean those calendars are not being ingested.

**Work:**
1. Read `C:\Dev\ics-merge\cals.txt`.
2. Cross-reference against existing sources in the admin UI.
3. Add any missing sources in Configure Sources.
4. Assign outputs and queue a rebuild for each imported source.

**Done when:** The active source inventory in the new system matches the legacy source list that still matters operationally.

### Ticket: Validate prune against real D1 and R2
**Work:**
1. Run prune against real D1 + R2 with representative stale snapshots and stale event rows.
2. Record before/after counts for D1 rows and R2 objects.
3. Confirm stale data is removed and active data remains intact.
4. Document the exact validation steps and observed counts.

**Done when:** Real prune behavior is validated, measured, and documented.

### Ticket: Write the production migration runbook
**Work:**
1. Document exact migration order and deploy order for production.
2. Include rollback and failure notes for the `google_targets` cutover and `0005` schema repair.
3. Verify the runbook against current `wrangler.jsonc` bindings and current migration files.

**Done when:** A current operator can execute the migration/deploy sequence from the document without filling in missing steps.

### Ticket: Add richer operator sync drill-down
**Why:** Inline source status is materially better now, but operators still have to infer too much from a compact summary.

**Work:**
1. Add a source-centric history or detail drawer.
2. Show latest queued/running/completed job timestamps.
3. Show Google sync counts (`synced`, `deferred`, `errors`).
4. Show latest source fetch error and latest Google sync error.

**Done when:** Operators can inspect sync progress and failure state without leaving the admin or inferring from abbreviated summary text.

---

## Completed Functional Work

### Migrate remaining `runInTransaction()` callers to `db.batch()`
Completed. The remaining high-write source sync paths were converted to `db.batch()`.

**Ref:** `TECH_DEBT.md`, `C:\Dev\Agents\antipatterns\d1-sql-transactions.md`

### Implement `poll_interval_minutes`
Completed. `listActiveSources()` now filters by `last_fetched_at` and `poll_interval_minutes`.

### Google Calendar outbound sync
Completed. Ingest/source sync now reconciles linked Google outputs using `GOOGLE_SERVICE_ACCOUNT_JSON` and persists sync state in `google_event_links`.

### Google sync schema repair + live diagnostics
Completed. Added `0005_google_event_links_nullable.sql`, runtime compatibility repair, actionable source status messaging, auto-refresh while jobs are active, and rate-limit mitigation for Google writes.

### Make Google sync subrequest-safe for large backfills
Completed. Google reconciliation now runs as queued `sync_google_target` jobs with per-source+target fan-out, chunked processing (`GOOGLE_SYNC_JOB_CHUNK_SIZE`), `has_more` re-enqueue, and delete-idempotency handling via the queue-driven path.

### Fix timezone handling for floating-time ICS events
Completed. `parseICS` now reads calendar-level `X-WR-TIMEZONE`, floating local datetimes no longer get coerced to UTC with a fake trailing `Z`, and Google sync preserves wall-clock `dateTime` plus `timeZone` for those events.

### Unescape RFC5545 text fields from upstream ICS feeds
Completed. `parseICS` now unescapes escaped text in `SUMMARY`, `DESCRIPTION`, and `LOCATION`, so feeds that encode line breaks as `\\n` and punctuation as `\\,` / `\\;` display correctly in admin, generated ICS, and Google output sync.

### Limit Google sync to the same lookback window as ICS feeds
Completed. `listDesiredGoogleSyncRows` now filters by `DEFAULT_LOOKBACK_DAYS`, so Google reconciliation only manages the same recent event window that ICS feeds expose. This reduces unnecessary writes and lowers subrequest pressure during rebuilds.

### Split admin into configuration and mobile-first event pages
Completed. `/admin` remains the configuration console, and `/admin/events` is now a focused event-modification page optimized for phone use. The event page uses the existing instance/override APIs with a mobile-first layout and direct navigation from the main admin.

### Fix static asset binding for authenticated admin pages
Completed. `wrangler.jsonc` now binds static assets as `ASSETS`, which is required for `/admin` and `/admin/events` to load through `serveAdminAsset()`.

### Make single-instance overrides visible immediately
Completed. Override create/delete now recomputes the affected instance scope immediately, updates `output_rules` without a rebuild, reflects in ICS output on the next feed request, and queues Google reconciliation on the existing sync path. Covered by regression tests for immediate remove/restore behavior.

### Tighten `/admin/events` mobile interaction
Completed. The event page now deduplicates instance rows, toggles drawers in place instead of rerendering the full list, removes the dead outer `.catch()`, and simplifies source labels to user-facing names.

### Reduce repository bootstrap overhead after cutover
Completed. Repository bootstrap now uses an isolate-level success guard, legacy target backfill is set-based instead of N+1, and `listInstances` no longer uses the `&&` side-effect pattern.

### Improve icon selection and custom-output icon rendering
Completed. Source-to-output icon selection now uses a dropdown of common icons in the admin UI, and custom Google output titles now include the configured icon in synced event summaries.

### Hide stale timezone-migration instance rows from `/admin/events`
Completed. `listInstances` now excludes `source_deleted` canonical events and `source_deleted` event instances, so the mobile/admin event list does not show stale pre-fix rows alongside corrected instances.

### Harden title rewrite rules and admin override drawers
Completed. Source title rewrite rules now reject unsafe regex patterns, source-config title backfills run set-based instead of N+1 updates, the orphaned `0006` migration file was removed, and both admin surfaces now toggle modified-event drawers client-side using the same context shape.

### Reduce unnecessary queue traffic and retry amplification
Completed. Cron enqueue failures are now non-fatal and logged, active queue work is best-effort deduped, retryable consumer failures use delayed retries with attempt metadata, and unchanged-source ingests no longer fan out Google sync work.

---

## Done

- [x] `deleteSource()` cascade made atomic with `db.batch()` - 2026-03-06
- [x] `/admin` route auth guard added (`requireRole`) - 2026-03-06
- [x] Source status `parse_status` display fixed (`parsed`/`parsed_no_blob` recognized as ok) - 2026-03-06
- [x] Feed subscription URLs exposed in admin UI - 2026-03-06
- [x] Per-source status columns (event count, last fetch, error) - 2026-03-06
- [x] Modify Events redesigned as instance-first UI - 2026-03-06
- [x] `getFeedContract` host derived from request origin / `PUBLIC_HOST` env var - 2026-03-06
- [x] `runInTransaction()` non-atomicity documented in `TECH_DEBT.md` - 2026-03-06
- [x] Admin UI migrated to static assets behind `/admin` auth gate - 2026-03-07
- [x] Unified `output_targets` model implemented in repository/admin UI/tests - 2026-03-07
- [x] `poll_interval_minutes` enforced in scheduler source selection - 2026-03-07
- [x] `RECURRENCE-ID` remap updates existing series instances instead of creating duplicates - 2026-03-07
- [x] FakeDb feed filtering now respects `source_deleted` and scheduler timing - 2026-03-07
- [x] Google Calendar outbound sync implemented for linked Google outputs - 2026-03-07
- [x] Repository cut over from `google_targets` to `output_targets` with legacy migration - 2026-03-07
- [x] `google_event_links.google_event_id` made nullable so failed Google sync attempts persist real errors - 2026-03-08
- [x] Admin source status now shows latest job / Google sync detail inline and auto-refreshes while jobs are active - 2026-03-08
- [x] Google write loop now retries quota errors briefly and defers remaining work after persistent rate-limit failures - 2026-03-08
- [x] Floating-time ICS events now respect `X-WR-TIMEZONE` and preserve local wall-clock times into Google sync - 2026-03-08
- [x] RFC5545 escaped text now unescapes correctly on ingest (`\\n`, `\\,`, `\\;`) - 2026-03-08
- [x] Google sync now respects the same lookback window as ICS feeds - 2026-03-08
- [x] Ingest now skips single events older than 30 days past and the admin labels Google-only sync metrics explicitly - 2026-03-11
- [x] `/admin/events` added as a focused mobile-first event modification page - 2026-03-08
- [x] Static asset binding added as `ASSETS` so `/admin` and `/admin/events` load in production - 2026-03-08
- [x] Clipboard copy flow now handles denied permissions and resets button state correctly - 2026-03-08
- [x] Top-level `loadDashboard()` call is now guarded with `.catch(...)` - 2026-03-08
- [x] Feed cache TTL already uses `FEED_CACHE_MAX_AGE_DEFAULT` / `env.FEED_CACHE_MAX_AGE`; old inline-cache TODO retired - 2026-03-08
- [x] Title rewrite rules hardened and modified-event drawers refactored to client-side toggle - 2026-03-15
- [x] Queue production, retries, and unchanged-source sync fanout reduced to cut repeated Cloudflare Queue messages - 2026-03-18
