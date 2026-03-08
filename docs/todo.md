# Todo

Prioritized work items. Update this file as items are completed or reprioritized.

---

## Remaining

### Enter sources from legacy `cals.txt`
**Why:** The legacy system's source list in `cals.txt` may not be fully migrated to the new system. Any missing sources mean those calendars are not being ingested.

**Fix:** Read `C:\Dev\ics-merge\cals.txt`, cross-reference against existing sources in the admin UI, and add any missing ones (Configure Sources → Add Source). Assign outputs and queue a rebuild for each.

### Limit Google sync to the same lookback window as ICS feeds
**Why:** `listDesiredGoogleSyncRows` has no date filter — it writes every `event_instances` row that has an output rule, regardless of age. The ingest window is controlled by `DEFAULT_LOOKBACK_DAYS` (default 7), but past that window old event instances can accumulate in D1 and get written to Google unnecessarily on each rebuild.

**Fix:** Add a `WHERE event_instances.occurrence_end_at >= ?` filter to `listDesiredGoogleSyncRows`, bound to `now - DEFAULT_LOOKBACK_DAYS`. This keeps Google sync consistent with what ICS feeds already expose and reduces subrequest pressure on large sources.

**Note:** Events older than the lookback that already exist in Google Calendar will be treated as orphans and deleted on the next sync run. That is the correct behavior — they are outside the managed window.

### Prune validation against real D1 + R2
Prune logic passes FakeDb tests but hasn't been verified against real infrastructure at scale.

### Production migration runbook
Run the D1 migrations through production and confirm the `google_targets` cutover migration completes before deploying code that no longer reads the legacy table.

### Google sync follow-up: subrequest-safe batching for large backfills
**Why:** Live testing confirmed that the next real blocker is no longer only Google quota. Large source rebuilds can still fail with `Too many subrequests.`, which is a Worker execution limit. Current mitigation handles Google rate limits better, but it still performs too much Google reconciliation in one source run.

**Execution plan:**
1. **Separate ingest from Google reconciliation**
   - Keep ICS fetch/parse/materialize in the existing source job
   - Stop doing full Google reconciliation inline at the end of `ingestSource()`
2. **Add queued Google sync jobs**
   - enqueue **per-source+target** Google sync jobs after ingest
   - this isolates one Google calendar's quota/failure state from another and keeps chunk sizing predictable
   - include cursor/offset state so work can continue across multiple runs
3. **Process Google sync in bounded chunks**
   - each job handles a fixed number of Google writes/deletes
   - persist progress and enqueue the next chunk until drained
   - treat Google delete `404` as success so queue retries do not fail on already-removed remote events
4. **Keep status counters source-centric**
   - continue showing `Events`, `Sync'd`, `Deferred`, `Errors`
   - add “job still running” / “continuing sync” messaging while chunks are draining
5. **Add tests for chunk continuation**
   - verify partial chunk completion
   - verify follow-up job enqueue
   - verify no duplicate event creation across retries
   - verify mid-chunk failure + re-enqueue resumes safely
   - verify repeated delete chunk retries treat remote `404` as success

**Acceptance:** A large source rebuild no longer ends with `Too many subrequests.` and can catch up over multiple queue runs.

### Operator visibility: richer job timeline / source sync drill-down
**Why:** Inline source status is now materially better, but operators still have to infer progress from a compact summary. For longer rebuilds and partial failures, deeper visibility would reduce confusion.

**Fix:** Add a source-centric job history or detail drawer showing:
- latest queued/running/completed job timestamps
- Google sync counts (`synced`, `updated`, `skipped`, `failed`, `deferred`)
- latest source fetch error
- latest Google sync error

---

## Completed Refactor Work

The split model has been replaced in code by a unified `output_targets` table using immutable IDs. Google Calendar outbound sync now runs against that model.

**Sequence matters — do in order:**

### 1. Add `output_targets` table and seed system records (migration)
New table columns:
- `id TEXT PRIMARY KEY` — UUID, immutable relational key
- `target_type TEXT NOT NULL` — `ics` or `google`
- `slug TEXT NOT NULL UNIQUE` — human handle / URL segment (e.g. `family`, `grayson-clubs`)
- `display_name TEXT NOT NULL` — shown in UI (e.g. `Family Feed`, `Grayson Clubs`)
- `calendar_id TEXT` — Google Calendar ID, null for ICS outputs
- `ownership_mode TEXT` — `managed_output` for Google; null for ICS
- `is_system INTEGER NOT NULL DEFAULT 0` — 1 for the three built-in ICS outputs
- `is_active INTEGER NOT NULL DEFAULT 1`
- `created_at TEXT`, `updated_at TEXT`

Seed system rows for `family`, `grayson`, `naomi` ICS outputs with `is_system = 1`.

### 2. Migrate link and rule tables from `target_key` to `target_id` (migration)
Add `target_id TEXT REFERENCES output_targets(id)` to:
- `source_target_links`
- `output_rules`
- `google_event_links`

Backfill `target_id` from system ICS rows (by slug match) and from `google_targets` rows. Update unique constraints. Remove `target_key` columns after cutover.

### 3. Fold `google_targets` into `output_targets` (migration + repository)
Completed in code and migration. Repository now reads/writes `output_targets` only. Legacy `google_targets` rows are migrated forward in `0004_output_targets_cutover.sql`.

### 4. Refactor repository to use explicit target records (code)
- Replace `TARGETS.includes(targetKey) ? 'ics' : 'google'` inference with explicit `target_type` from `output_targets`
- Source API responses: `target_links` carry `target_id`, `target_type`, `display_name`, `icon`, `prefix`
- Feed generation and output rule computation join by `target_id`
- Collapse `listTargets()` into a single read from `output_targets`
- Fix `getSourceById` to return link data (currently omits it — code-review issue 4)

### 5. Update admin UI to select outputs by label (code)
- `/api/targets` returns unified list: `id`, `target_type`, `display_name`, `slug`, `is_system`
- Configure Sources uses `target_id` values for selections
- Grouped selector: **ICS Feeds** / **Google Calendars**
- Display human labels only — no slugs or IDs visible to users

### 6. Add tests for unified target identity (tests)
- Source links stored and retrieved by `target_id`
- Mixed ICS + Google targets on one source
- System outputs and user-created outputs coexisting
- Create/delete/list target API round-trips
- Legacy `target_key` backfill correctness

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

### Fix timezone handling for floating-time ICS events
Completed. `parseICS` now reads calendar-level `X-WR-TIMEZONE`, floating local datetimes no longer get coerced to UTC with a fake trailing `Z`, and Google sync preserves wall-clock `dateTime` plus `timeZone` for those events.

---

---

## Done

- [x] `deleteSource()` cascade made atomic with `db.batch()` — 2026-03-06
- [x] `/admin` route auth guard added (`requireRole`) — 2026-03-06
- [x] Source status `parse_status` display fixed (`parsed`/`parsed_no_blob` recognized as ok) — 2026-03-06
- [x] Feed subscription URLs exposed in admin UI — 2026-03-06
- [x] Per-source status columns (event count, last fetch, error) — 2026-03-06
- [x] Modify Events redesigned as instance-first UI — 2026-03-06
- [x] `getFeedContract` host derived from request origin / `PUBLIC_HOST` env var — 2026-03-06
- [x] `runInTransaction()` non-atomicity documented in `TECH_DEBT.md` — 2026-03-06
- [x] Admin UI migrated to static assets behind `/admin` auth gate — 2026-03-07
- [x] Unified `output_targets` model implemented in repository/admin UI/tests — 2026-03-07
- [x] `poll_interval_minutes` enforced in scheduler source selection — 2026-03-07
- [x] `RECURRENCE-ID` remap updates existing series instances instead of creating duplicates — 2026-03-07
- [x] FakeDb feed filtering now respects `source_deleted` and scheduler timing — 2026-03-07
- [x] Google Calendar outbound sync implemented for linked Google outputs — 2026-03-07
- [x] Repository cut over from `google_targets` to `output_targets` with legacy migration — 2026-03-07
- [x] `google_event_links.google_event_id` made nullable so failed Google sync attempts persist real errors — 2026-03-08
- [x] Admin source status now shows latest job / Google sync detail inline and auto-refreshes while jobs are active — 2026-03-08
- [x] Google write loop now retries quota errors briefly and defers remaining work after persistent rate-limit failures — 2026-03-08
- [x] Floating-time ICS events now respect `X-WR-TIMEZONE` and preserve local wall-clock times into Google sync — 2026-03-08
- [x] Clipboard copy flow now handles denied permissions and resets button state correctly — 2026-03-08
- [x] Top-level `loadDashboard()` call is now guarded with `.catch(...)` — 2026-03-08
- [x] Feed cache TTL already uses `FEED_CACHE_MAX_AGE_DEFAULT` / `env.FEED_CACHE_MAX_AGE`; old inline-cache TODO retired — 2026-03-08
