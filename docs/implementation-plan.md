# Implementation Plan

_Updated: 2026-03-09 (session 11)_

## Vocabulary

| Term | Meaning |
|---|---|
| **Source** | An upstream calendar feed the system ingests |
| **Output** | A delivered calendar service — an ICS feed or a managed Google Calendar |
| **Source -> Output link** | The assignment connecting a source to an output, with per-link rendering rules |
| **Override** | An admin-applied change to how a specific event or instance is distributed |

The codebase is now centered on the unified `output_targets` model. Legacy `target` / `google_targets` naming remains only where compatibility cleanup is still pending.

---

## What Is Shipped

1. Legacy ICS feed contracts preserved: `family.ics`, `grayson.ics`, `naomi.ics` (and `.isc` aliases).
2. Cloudflare Worker + D1 + Queue + cron scaffolding running.
3. Admin protected via Cloudflare Access JWT validation with email-to-role mapping.
4. Admin shells at `/admin` (configuration) and `/admin/events` (mobile-first event modification), public ICS feeds token-protected at `?token=`.
5. ICS source create, update, disable, list, and rebuild.
6. Google output create, update, delete, and list.
7. Many-to-many source -> output assignment via `source_target_links`.
8. Per-link rendering rules: `icon`, `prefix`, `sort_order`.
9. Feed rendering driven by per-link rules with fallback to source-level values.
10. Google output deletion cascades to dependent source -> output links and output rules.
11. Admin shell protected at route level via `requireRole(['admin','editor'])`.
12. ICS feed subscription URLs exposed in admin UI with copy-to-clipboard support.
13. Per-source status in admin: active event count, last fetch timestamp, latest job state (`queued`, `running`, `partial`, `error`, `ok`) with inline actionable detail.
14. Modify Events redesigned as instance-first UI, with `/admin/events` as the focused mobile workflow.
15. New API routes: `GET /api/instances` and `GET /api/overrides`.
16. `deleteSource()` now cascades correctly through all dependent tables.
17. `google_event_links.google_event_id` is now nullable so failed Google sync attempts can be persisted instead of crashing the job.
18. Google Calendar sync retries brief rate-limit responses, slows write bursts, and defers the remainder of a source run after the first persistent rate-limit error.
19. Google reconciliation is queue-driven and chunked per source+target via `sync_google_target` jobs with `has_more` re-enqueue behavior.
20. Admin auto-refreshes while jobs are queued or running so rebuild progress is visible without manual refresh.
21. ICS ingest reads calendar-level `X-WR-TIMEZONE` and preserves floating local datetimes as wall-clock times instead of coercing them to UTC.
22. ICS ingest unescapes RFC5545 text fields (`\\n`, `\\,`, `\\;`) for `SUMMARY`, `DESCRIPTION`, and `LOCATION`.
23. Google reconciliation respects `DEFAULT_LOOKBACK_DAYS`, so it only manages the same recent event window that ICS feeds expose.
24. Static assets are bound as `ASSETS`, which is required for `/admin` and `/admin/events` to load through the Worker route/auth layer.
25. Single-instance overrides now recompute the affected output scope immediately and queue follow-up Google reconciliation without a source rebuild.
26. `/admin/events` now deduplicates instance rows and toggles drawers in place without full-list rerender.
27. Repository bootstrap work is isolate-cached, legacy target backfill is set-based, and `listInstances` no longer uses side-effect chaining.

---

## Data Model

### `sources`

One row per upstream calendar feed.

| Field | Purpose |
|---|---|
| `url` | ICS feed URL to poll |
| `owner_type` | `grayson`, `naomi`, or `family` |
| `source_category` | `sports`, `personal`, `shared`, `general` |
| `icon`, `prefix` | Fallback decoration if no per-link rule overrides |
| `is_active` | Whether the source is polled and its events included |
| `poll_interval_minutes` | Poll frequency for cron-driven source selection; enforced by `listActiveSources()` |

### `source_target_links`

One row per source -> output assignment. This is a join table; new rule types belong here, not in a new relationship model.

| Field | Purpose |
|---|---|
| `source_id` | Source being assigned |
| `target_id` | Output being assigned |
| `target_type` | `ics` or `google` |
| `icon` | Per-link icon rule |
| `prefix` | Per-link prefix rule |
| `sort_order` | Display ordering within the output |
| `is_enabled` | Whether this assignment is active |

### `output_targets`

One row per output target. Google outputs live here now; `google_targets` is retired.

| Field | Purpose |
|---|---|
| `id` | UUID, immutable relational key |
| `slug` | Unique slug identifying this output |
| `display_name` | Human label shown in admin |
| `target_type` | `ics` or `google` |
| `calendar_id` | Google Calendar ID to write to |
| `ownership_mode` | `managed_output` for Google; null for ICS |
| `is_system` | 1 for built-in ICS outputs |
| `is_active` | Whether the output is active |

### `canonical_events` + `event_instances`

Normalized event storage. One canonical event per unique event identity; one instance row per occurrence.

### `output_rules`

Derived rows that record whether each event instance should appear on each output. Re-derived on every ingest and on source config change.

### `google_event_links`

Tracks Google Calendar reconciliation state per event instance and output.

| Field | Purpose |
|---|---|
| `google_event_id` | Remote Google event ID; nullable when a sync attempt failed before creation |
| `sync_status` | `synced`, `error`, or pending transitional state |
| `last_error` | Latest Google/API error message for that instance/output |
| `last_synced_hash` | Hash of last successfully synced payload to avoid redundant writes |

### `event_overrides`

Admin-applied changes to event handling.

| Override type | Effect |
|---|---|
| `skip` | Remove a specific instance from all outputs |
| `hidden` | Suppress from a specific output |
| `maybe` | Flag as uncertain |
| `note` | Attach context without changing distribution |

---

## Remaining Work

### Next priority
1. **Source migration** — enter sources from `C:\Dev\ics-merge\cals.txt` into the new system.
2. **Prune validation** — verify prune behavior against real D1 + R2, not only FakeDb.
3. **Production migration runbook** — document exact production migration and deploy order against the current environment.
4. **Operator drill-down** — deeper source/job history beyond the inline status summary.

See [todo.md](/c:/Dev/family-scheduling/docs/todo.md) for the active backlog and acceptance criteria.

---

## Legacy Reference

`C:\Dev\ics-merge` is the legacy production worker. Use it only for:

- current production feed contract reference
- comparing old and new rendering behavior
- source URL inventory (`cals.txt`)

Do not implement new features there.

---

## Changelog

### 2026-03-06 — Session 3

**Features**
- Modify Events UI redesigned as instance-first model: source picker auto-loads, flat dated instance list, inline drawer below clicked row, override form inside drawer, and a future-overrides table below.
- ICS feed subscription URLs shown in admin Feeds table with full URL and copy-to-clipboard support.
- Per-source status columns in admin Sources table: active event count, last fetch timestamp, and HTTP/parse status with error tooltip on hover.
- New API routes: `GET /api/instances` and `GET /api/overrides`.

**Bug fixes**
- `deleteSource()` crashed with `no such column: event_id`; the actual column is `canonical_event_id`.
- `/admin` HTML route was served without authentication; fixed with `requireRole`.
- Future overrides table was filtering by override `created_at` instead of instance `occurrence_start_at`.
- Admin JS syntax error caused by escaped quotes inside template literals.
- Wrong column names in `listActiveOverrides`: `payload` -> `payload_json`, `actor_role` -> `created_by`.
- Removed duplicate drawer-load call on row click.

### 2026-03-07 — Session 4

**Bug fixes**
- Source status showed `error` for all sources because `parse_status` handling was too narrow.
- `deleteSource()` cascade converted from sequential `.run()` calls to a single `db.batch()`.
- Feed contract URLs now derive from request origin or `PUBLIC_HOST` instead of a hardcoded production hostname.

**Decisions**
- Confirmed direction: unified `output_targets` table replaces `google_targets` plus hardcoded ICS constants.
- Admin UI template-literal shell was retired in favor of static assets.

### 2026-03-07 — Session 5

**Features**
- Scheduler now respects `poll_interval_minutes` when selecting active sources for cron-driven ingest.
- Google Calendar outbound sync added for linked Google outputs using `GOOGLE_SERVICE_ACCOUNT_JSON`.
- Recurrence exception handling now remaps `RECURRENCE-ID` updates onto the existing series instance.

**Refactor**
- Repository cut over to `output_targets` as the live target model.
- Legacy `google_targets` migration added in `0004_output_targets_cutover.sql`.

### 2026-03-08 — Session 6

**Operational findings**
- The Worker could reach Google successfully, but writes initially failed because the Google Calendar API was not enabled on project `famcal-489417`.
- Once the API was enabled, live sync confirmed a second production issue: large rebuilds hit Google `Rate Limit Exceeded`.
- A generic `error` label in `/admin` hid actionable details; the data existed in D1, but the UI was not surfacing it.

**Bug fixes**
- Added `0005_google_event_links_nullable.sql` because the live schema still had `google_event_links.google_event_id NOT NULL`, which prevented failed sync attempts from being recorded.
- Added runtime schema repair in `ensureSupportTables()` so older environments self-heal even before migrations are applied.
- Fixed browser-side admin bug `TARGETS is not defined` after the target API cleanup.

**Behavior changes**
- Admin source rows now show the latest source job state and error detail inline.
- Dashboard auto-refreshes while jobs are queued or running.
- Google Calendar writes now retry brief rate-limit responses and defer the remainder of a source run after the first persistent quota failure.

**Learnings**
- Persisted job state is not enough if the UI collapses it into a generic `error`; operators need the exact failing subsystem and message in the primary table.
- Google output sync is operationally distinct from ICS ingest. A source can ingest successfully and still partially fail on outbound Google writes.
- Schema compatibility for error-state tables matters as much as success-path schema.

### 2026-03-08 — Session 7

**Bug fixes**
- Floating-time ICS datetimes no longer have a fake trailing `Z` appended during ingest.
- `parseICS` now reads `X-WR-TIMEZONE` from the VCALENDAR header and uses it as the fallback timezone when an event-level `TZID` is absent.
- Google sync now sends floating local events to Google as wall-clock `dateTime` values paired with the parsed `timeZone`.

**Operational follow-up**
- Existing D1 rows produced before this fix need a rebuild to correct stored offsets and resync Google output events at the proper local time.

**Learnings**
- Calendar-level timezone metadata is not optional.
- Time handling needs end-to-end tests, not only parser assertions.

### 2026-03-08 — Session 8

**Bug fixes**
- RFC5545 escaped text is now unescaped on ingest for `SUMMARY`, `DESCRIPTION`, and `LOCATION`.
- Google reconciliation now filters desired rows by `DEFAULT_LOOKBACK_DAYS`, preventing old instances outside the managed feed window from being re-written to Google on every rebuild.

**Learnings**
- Text normalization belongs at ingest, not in scattered renderers.
- Escaping is symmetric work: if the system emits escaped ICS text, it also has to unescape the same format when reading upstream feeds.
- Google sync scope should match the public feed contract.

### 2026-03-08 — Session 9

**Features**
- Added `/admin/events` as a dedicated event-modification surface for mobile use.
- Main `/admin` now links directly to the focused event page instead of forcing all work through the configuration-heavy console.

**Learnings**
- Event correction is the dominant mobile workflow, not source configuration.
- A split admin is cleaner than trying to force one dense desktop page to become fully mobile-friendly for every task.

### 2026-03-08 — Session 10

**Operational fix**
- Added `assets.binding = "ASSETS"` to `wrangler.jsonc` so the Worker can actually serve `/admin` and `/admin/events` through the authenticated asset path.

**Review findings**
- The mobile event page is the right architectural direction, but the first pass still needs polish:
  - deduplicate event rows when one instance belongs to multiple outputs
  - fix drawer behavior so toggling one card does not rerender the full list
  - remove a small amount of avoidable code/UI noise from the page

**Learnings**
- Worker-side auth plus static assets requires an explicit asset binding; a directory alone is not enough.
- Splitting the mobile workflow into its own page solved the larger usability problem, but it also made the next set of UI defects much easier to isolate and fix.

### 2026-03-09 — Session 11

**Behavior changes**
- Override create/delete now reconciles the affected instance scope immediately instead of waiting for the next source rebuild or cron cycle.
- `/admin/events` deduplicates multi-output rows and opens/closes drawers in place without rebuilding the whole list.
- Repository bootstrap work now short-circuits after first success in an isolate, and legacy target backfill is set-based.

**Bug fixes**
- Normalized system target identity so output-rule upserts do not create duplicate rows when legacy key-based records meet ID-based reconciles.
- Removed the `&&` side-effect pattern from `listInstances`.

**Learnings**
- Immediate admin actions need immediate persistence and immediate derived-state recompute; “saved now, visible later” is the wrong contract for manual correction tools.
- Stable relational identity must not depend on whether a system target is referenced by slug or by `output_targets.id`; both paths have to converge on the same canonical ID.
- The subrequest-safe Google batching work is already present in code; docs need to be verified against implementation before carrying old tickets forward.
