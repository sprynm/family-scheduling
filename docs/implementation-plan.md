# Implementation Plan

_Updated: 2026-03-06 (session 3)_

## Vocabulary

| Term | Meaning |
|---|---|
| **Source** | An upstream calendar feed the system ingests |
| **Output** | A delivered calendar service — an ICS feed or a managed Google Calendar |
| **Source → Output link** | The assignment connecting a source to an output, with per-link rendering rules |
| **Override** | An admin-applied change to how a specific event or instance is distributed |

Code currently uses `target` and `google_targets` in places. These will be normalized to `output` as the UI and schema evolve.

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

## ICS Output Configuration (Planned)

The three ICS feeds (`family`, `grayson`, `naomi`) are currently hardcoded in the route matcher (`index.js`) and constants (`constants.js`). The intended direction is to store these as configurable output rows in D1 alongside Google outputs, so:

- New named ICS feeds can be created without a code change
- The same source → output link model applies uniformly to both ICS and Google outputs
- Feed URLs and calendar names become admin-configurable

This requires unifying `google_targets` and the hardcoded ICS constants into a single `outputs` table. Not in scope for the current pass.

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
4. **ICS output configuration** — move hardcoded feed names to DB-driven output rows.
5. **Richer admin edit workflows** — edit existing sources and source → output link rules in-place (currently only create and disable are supported).
6. **Prune validation** — confirm stale data pruning behaves correctly against real D1 and R2 at scale.
7. **`runInTransaction()` → `db.batch()`** — migrate 5 callers to atomic batches (see `TECH_DEBT.md`). Low priority while paths remain admin-only with low concurrency.

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
