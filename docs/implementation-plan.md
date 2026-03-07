# Implementation Plan

_Updated: 2026-03-06_

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
| `poll_interval_minutes` | How often to re-ingest |

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
1. **Google Calendar outbound sync** — write `output_rules` results to managed Google Calendars via the Calendar API.

### Follow-on
2. **Recurrence exception handling** — remap `RECURRENCE-ID` exception events to their parent instance rather than creating new canonical events.
3. **ICS output configuration** — move hardcoded feed names to DB-driven output rows.
4. **Richer admin edit workflows** — edit existing sources and source → output link rules in-place (currently only create and disable are supported).
5. **Prune validation** — confirm stale data pruning behaves correctly against real D1 and R2 at scale.

---

## Legacy Reference

`C:\Dev\ics-merge` is the legacy production worker. Use it only for:

- Current production feed contract reference
- Comparing old and new rendering behaviour
- Source URL inventory (`cals.txt`)

Do not implement new features there.
