# Todo

Prioritized work items. Update this file as items are completed or reprioritized.

---

## Remaining

### Enter sources from legacy `cals.txt`
**Why:** The legacy system's source list in `cals.txt` may not be fully migrated to the new system. Any missing sources mean those calendars are not being ingested.

**Fix:** Read `C:\Dev\ics-merge\cals.txt`, cross-reference against existing sources in the admin UI, and add any missing ones (Configure Sources → Add Source). Assign outputs and queue a rebuild for each.

### Prune validation against real D1 + R2
Prune logic passes FakeDb tests but hasn't been verified against real infrastructure at scale.

### Production migration runbook
Run the D1 migrations through production and confirm the `google_targets` cutover migration completes before deploying code that no longer reads the legacy table.

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

---

## P3 — Code Quality (from JS standards scan)

### Fix clipboard `.catch()` missing — [index.js:807](../src/index.js#L807)
`navigator.clipboard.writeText(...).then(...)` has no `.catch()`. If the clipboard API is denied (non-HTTPS, permission blocked), the rejection is silent and the button gives no feedback.

Fix: add `.catch(() => { btn.textContent = 'Failed'; setTimeout(...) })` after the `.then()`.

### Add error handling to top-level `loadDashboard()` call — [index.js:1148](../src/index.js#L1148)
`loadDashboard()` is called fire-and-forget on page load. It has internal `try/catch` so it won't throw under normal conditions, but the call site is unguarded. Wrap in `.catch(err => setStatus(err.message, true))`.

### Extract magic numbers to named constants — [index.js:810](../src/index.js#L810), [index.js:1194](../src/index.js#L1194)
- `1500` (ms clipboard reset) — add an inline comment at minimum
- `max-age=300` (ICS feed cache TTL) — move to `env.FEED_CACHE_MAX_AGE` or a named constant in `constants.js`

**Ref:** `docs/coding-standards.md`

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
