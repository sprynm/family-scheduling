# Implementation Plan

_Updated: 2026-03-07 (session 4)_

## Vocabulary

| Term | Meaning |
|---|---|
| **Source** | An upstream calendar feed the system ingests |
| **Output** | A delivered calendar service — an ICS feed or a managed Google Calendar |
| **Source → Output link** | The assignment connecting a source to an output, with per-link rendering rules |
| **Override** | An admin-applied change to how a specific event or instance is distributed |

Code currently uses `target` and `google_targets` in places. These will be replaced by a unified `output_targets` table — see Planned Data Model Changes below.

---

## What Is Shipped

1. Legacy ICS feed contracts preserved: `family.ics`, `grayson.ics`, `naomi.ics` (and `.isc` aliases).
2. Cloudflare Worker + D1 + Queue + cron scaffolding running.
3. Admin protected via Cloudflare Access JWT validation with email-to-role mapping.
4. Admin shell at `/admin`, public ICS feeds token-protected at `?token=`.
5. ICS source create, update, disable, list, and rebuild.
6. Google output create, update, delete, and list.
7. Many-to-many source → output assignment via `source_target_links`.
8. Per-link rendering rules: `icon`, `prefix`, `sort_order`.
9. Feed rendering driven by per-link rules with fallback to source-level values.
10. Google output deletion cascades to dependent source → output links and output rules.
11. Admin shell protected at route level via `requireRole(['admin','editor'])` — HTML is not served unauthenticated.
12. ICS feed subscription URLs exposed in admin UI (full URL with token, copy-to-clipboard button).
13. Per-source status in admin: active event count, last fetch timestamp, last HTTP/parse status with error tooltip.
14. Modify Events redesigned as instance-first UI: source picker auto-loads a flat dated instance list; clicking a row opens an inline drawer with the override form; "Future Events with Modifications" table shows all upcoming instances with active overrides.
15. New API routes: `GET /api/instances` (filterable by source, output, future, limit) and `GET /api/overrides` (future instances with active overrides).
16. `deleteSource()` now cascades correctly through all dependent tables (`event_overrides`, `google_event_links`, `output_rules`, `event_instances`, `canonical_events`, `source_events`, `source_snapshots`, `source_target_links`).

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
| `poll_interval_minutes` | Intended poll frequency — **not currently enforced** (all active sources are enqueued on every cron tick regardless of this value) |

### `source_target_links` _(to be renamed `source_output_links`)_

One row per source → output assignment. This is a join table — adding new rule types means adding columns here, not restructuring the relationship.

| Field | Purpose |
|---|---|
| `source_id` | Source being assigned |
| `target_key` | Output being assigned to |
| `target_type` | `ics` or `google` |
| `icon` | Per-link icon rule |
| `prefix` | Per-link prefix rule |
| `sort_order` | Display ordering within the output |
| `is_enabled` | Whether this assignment is active |

### `google_targets` _(represents Google Calendar outputs)_

One row per managed Google Calendar output.

| Field | Purpose |
|---|---|
| `target_key` | Unique slug identifying this output |
| `calendar_id` | Google Calendar ID to write to |
| `ownership_mode` | `managed_output` — this system writes to it |

### `canonical_events` + `event_instances`

Normalised event storage. One canonical event per unique event identity; one instance row per occurrence (handles recurring events).

### `output_rules`

Derived rows that record whether each event instance should appear on each output. Re-derived on every ingest and on source config change.

### `event_overrides`

Admin-applied changes to event handling.

| Override type | Effect |
|---|---|
| `skip` | Remove a specific instance from all outputs (e.g. can't attend this week's practice) |
| `hidden` | Suppress from a specific output |
| `maybe` | Flag as uncertain |
| `note` | Attach context without changing distribution |

---

## Planned Data Model Changes — Unified Output Targets

**Confirmed direction (2026-03-07).** Replace the current split model (hardcoded ICS constants + `google_targets` table + string `target_key` joins) with a unified `output_targets` table using immutable IDs.

### New `output_targets` table

| Field | Purpose |
|---|---|
| `id` | UUID, immutable relational key — the only FK used by dependent tables |
| `target_type` | `ics` or `google` — explicit, never inferred |
| `slug` | Human handle / URL segment (e.g. `family`, `grayson-clubs`) — for debugging only |
| `display_name` | Shown in admin UI (e.g. `Family Feed`, `Grayson Clubs`) |
| `calendar_id` | Google Calendar ID; null for ICS outputs |
| `ownership_mode` | `managed_output` for Google; null for ICS |
| `is_system` | 1 for the three built-in ICS outputs — cannot be deleted |
| `is_active` | Whether the output is active |

Built-in system outputs seeded at migration time:

| slug | display_name | target_type | is_system |
|---|---|---|---|
| `family` | Family Feed | ics | 1 |
| `grayson` | Grayson Feed | ics | 1 |
| `naomi` | Naomi Feed | ics | 1 |

**Benefits:**
- No implicit type inference — `target_type` is first-class data
- No key collisions between ICS and Google outputs
- New feeds can be created without code changes
- Admin UI shows human labels, never internal keys
- `source_target_links` and `output_rules` join by `target_id`, not string

**Migration sequence:** See `docs/todo.md` P2 — Output Target Refactor.

---

## Source Types (Current and Planned)

| Source type | Status |
|---|---|
| ICS URL (TeamSnap, Google Calendar ICS export, any `.ics`) | Live |
| Google Calendar API (direct read) | Deferred — ICS export from Google Calendar is sufficient for now and avoids a second OAuth scope |

---

## Remaining Work

### Next priority
1. **Source migration** — enter sources from `C:\Dev\ics-merge\cals.txt` into the new system.
2. **Google Calendar outbound sync** — write `output_rules` results to managed Google Calendars via the Calendar API.

### Follow-on
3. **Recurrence exception handling** — remap `RECURRENCE-ID` exception events to their parent instance rather than creating new canonical events.
4. **Output target refactor** — unified `output_targets` table (see Planned Data Model Changes above). Required before Google outbound sync.
5. **Google Calendar outbound sync** — build on unified output model.
6. **`runInTransaction()` → `db.batch()`** — 5 callers still non-atomic (see `TECH_DEBT.md`).
7. **`poll_interval_minutes`** — implement actual frequency filtering in cron handler.

### P3
8. Recurrence exception handling, prune validation, test fidelity gaps.

See `docs/todo.md` for the full detail on each item.

---

## Legacy Reference

`C:\Dev\ics-merge` is the legacy production worker. Use it only for:

- Current production feed contract reference
- Comparing old and new rendering behaviour
- Source URL inventory (`cals.txt`)

Do not implement new features there.

---

## Changelog

### 2026-03-06 — Session 3

**Features**
- Modify Events UI redesigned as instance-first model: source picker auto-loads, flat dated instance list, inline drawer below clicked row, override form inside drawer, "Future Events with Modifications" table below.
- ICS feed subscription URLs shown in admin Feeds table — full URL with embedded token, copy-to-clipboard button.
- Per-source status columns in admin Sources table: active event count, last fetch timestamp, HTTP/parse status with error tooltip on hover.
- New API routes: `GET /api/instances` and `GET /api/overrides`.

**Bug fixes**
- `deleteSource()` crashed with `no such column: event_id` — column is `canonical_event_id`. Also added missing cascade deletes for `google_event_links` and `source_snapshots`.
- `/admin` HTML route was served without authentication — anyone with the URL could load the shell. Fixed by adding `requireRole` guard.
- "Future Events with Modifications" table was never populated — was filtering by override `created_at` instead of instance `occurrence_start_at`. Rewrote `listActiveOverrides()` to join on future instances.
- Admin JS syntax error on load — template literal evaluated `\'` to `'` before the browser received it, breaking a single-quoted attribute value. Fixed with `&#39;`.
- Wrong column names in `listActiveOverrides`: `payload` → `payload_json`, `actor_role` → `created_by`.
- Double `loadDrawerContent` call on row click — removed redundant second call.
- FakeDb test pattern failed after `listSources` query gained JOINs — updated match predicate.

**Tech debt documented**
- `runInTransaction()` has no atomicity on D1 (D1 rejects `BEGIN`/`COMMIT`). Five callers documented in `TECH_DEBT.md` with correct `db.batch()` migration pattern.

### 2026-03-07 — Session 4

**Bug fixes**
- Source status showed "error" for all sources — `parseOk` only checked `'ok'`/`'success'` but ingest writes `'parsed'`/`'parsed_no_blob'`. Fixed to accept all four values.
- `deleteSource()` cascade converted from 9 sequential `.run()` calls to a single `db.batch()` — now atomic.
- Feed contract URLs now derived from request origin (or `PUBLIC_HOST` env var) instead of hardcoded production hostname.

**Decisions**
- Confirmed direction: unified `output_targets` table replaces `google_targets` + hardcoded ICS constants. Full design documented in this file and `docs/todo.md`.
- Admin UI structural problem documented: `renderAdminShell()` template literal is the root cause of recurring escaping bugs; migration to Cloudflare Static Assets added as P1 work item.
