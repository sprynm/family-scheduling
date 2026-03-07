# Todo

Prioritized work items. Update this file as items are completed or reprioritized.

---

## P1 — Do Soon

### Migrate admin UI to static assets
**Why:** `renderAdminShell()` is a 1,200-line template literal inside `index.js`. Client-side JS lives in a string — no linting, no editor intelligence, escaping bugs recur (apostrophes, backticks). This is the single largest source of defects in the project.

**Fix:** Use Cloudflare Workers Static Assets with a Worker auth gateway — static assets bypass Worker logic by default, so `/admin` must stay behind an explicit auth check in the Worker.

1. Add `"assets": { "directory": "./public" }` to `wrangler.jsonc`
2. Move HTML/CSS/JS out of `renderAdminShell()` into `public/admin.html`, `public/admin.js`, `public/admin.css`
3. Keep the `/admin` route in `index.js` as a thin auth-then-proxy handler:
   ```js
   if (pathname === '/admin' || pathname === '/admin/') {
     const authError = await requireRole(request, env, ['admin', 'editor']);
     if (authError) return authError;
     return env.ASSETS.fetch(request); // serve from static assets after auth
   }
   ```
4. Worker handles `/admin` (auth gate), `/api/*`, and `/*.ics` — static asset binding serves the file after auth passes
5. Delete `renderAdminShell()` — `index.js` drops to ~250 lines of actual logic

**Auth contract preserved:** `requireRole` stays on the `/admin` route. The static asset is never reachable without passing auth.

**Result:** Real JS file = proper linting, no escaping bugs, `index.js` is clean routing only.

**Ref:** https://developers.cloudflare.com/workers/static-assets/

---

### Enter sources from legacy `cals.txt`
**Why:** The legacy system's source list in `cals.txt` may not be fully migrated to the new system. Any missing sources mean those calendars are not being ingested.

**Fix:** Read `C:\Dev\ics-merge\cals.txt`, cross-reference against existing sources in the admin UI, and add any missing ones (Configure Sources → Add Source). Assign outputs and queue a rebuild for each.

---

## P2 — Output Target Refactor (do before Google sync)

The current model splits built-in ICS feeds (`TARGETS` constant) from `google_targets` rows, with all relations keying off a string `target_key`. This makes target identity implicit, collision-prone, and impossible to evolve without code changes. The refactor replaces it with a unified `output_targets` table using immutable IDs. Google Calendar outbound sync should be built on this foundation, not the old model.

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
Migrate all `google_targets` rows into `output_targets`. Update create/update/delete/list flows to read and write `output_targets`. Remove all `google_targets` reads from repository. Drop table after verified cutover.

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

## P2 — Other

### Migrate remaining `runInTransaction()` callers to `db.batch()`
Five callers remain non-atomic: `setSourceTargets` (~350), `disableSource` (~374), `deleteTarget` (~458), `syncSourceConfig` (~948), `ingestSource` (~1238 — largest, may have inter-statement dependencies that require RETURNING or restructuring).

**Ref:** `TECH_DEBT.md`, `C:\Dev\Agents\antipatterns\d1-sql-transactions.md`

### Implement `poll_interval_minutes`
Cron handler enqueues every active source unconditionally. Fix: filter `listActiveSources()` to sources where `last_fetched_at IS NULL OR last_fetched_at < (now - poll_interval_minutes * 60)`.

### Google Calendar outbound sync
Build on top of the unified `output_targets` model (above). After ingest, write/update/delete events via Calendar API for all `google`-type outputs linked to the source. See `docs/google-auth-decision.md`.

---

## P3 — Follow-on

### Recurrence exception handling
`RECURRENCE-ID` exception events create new canonical events instead of updating the existing series instance. Fix: detect `RECURRENCE-ID` in `ingestSource` and update the matching instance.

### Prune validation against real D1 + R2
Prune logic passes FakeDb tests but hasn't been verified against real infrastructure at scale.

### FakeDb `source_deleted` filter gap
`FakeDb.runAll` feed query does not filter `source_deleted = 0`. Add the filter and a test verifying feed output after `disableSource`.

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
