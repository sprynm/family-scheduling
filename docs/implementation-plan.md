# Implementation Plan

_Updated: 2026-03-18_

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
28. Source-to-output icon selection in admin uses a dropdown of common options instead of a free-text emoji field.
29. Custom Google outputs now include configured icons in synced event summaries.
30. `/api/instances` excludes stale `source_deleted` rows so corrected timezone rebuilds do not show duplicate pre-fix instances in admin.
31. Source title rewrite rules reject unsafe regex patterns and preserve `source_title_raw` so title changes can be backfilled and reverted safely.
32. Source config title backfills are set-based, and modified-event drawers on both admin pages now toggle client-side without refetching `/api/overrides`.
33. Cron queue production is now failure-tolerant: enqueue failures are logged with context, later sources continue processing, and each cron run emits a queue summary instead of failing on the first queue `429`.
34. Active queue work is best-effort deduped for source ingest, source rebuild, Google target sync, and prune jobs so the worker avoids sending repeat messages for work already in flight.
35. Queue consumer retries are now delayed and bounded with persisted retry metadata (`attempt_count`, `last_error_kind`) instead of immediate retry storms.
36. Source snapshots now store `payload_hash`, ingest uses conditional fetch headers when available, and unchanged fetches (`304` or identical payload hash) exit before parse, row rewrites, or downstream Google sync enqueue.
37. Google sync fanout is now gated on effective source-state changes, so changed fetches that do not alter canonical events, instances, or output rules do not emit new sync jobs.

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
| `title_rewrite_rules_json` | Per-source regex rewrite rules, validated conservatively before save |
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

## Recent Learnings

1. Persisted job state is only useful if the primary admin table surfaces the failing subsystem and message directly.
2. Google output sync is operationally distinct from ICS ingest. A source can ingest cleanly and still fail partially on outbound Google writes.
3. Calendar-level timezone metadata is required. `X-WR-TIMEZONE` plus floating local datetimes must be preserved as wall-clock times, not coerced to UTC.
4. Parser normalization belongs at ingest. RFC5545 escaped text and other wire-format concerns should be corrected once, not patched in multiple renderers.
5. Event correction is the dominant mobile workflow. A split admin with a focused `/admin/events` surface is more workable than forcing one dense desktop page to do everything.
6. Worker-side auth plus static assets requires an explicit `ASSETS` binding; the directory setting alone is insufficient.
7. Immediate admin actions need immediate derived-state updates. Event overrides are not complete until their output scope is recomputed.
8. Built-in system outputs must canonicalize to one stable identity. Mixing slug-based and ID-based references creates duplicate derived rows.
9. Custom outputs should follow the same visual-decoration rules as system outputs unless there is a deliberate product reason not to.
10. Timezone-correction rebuilds can leave stale deleted instance rows behind; admin queries must filter `source_deleted` state consistently.
11. Operator-supplied regex is a feature, not a trust boundary. Title rewrite rules need conservative safety checks because Worker regex execution has no practical timeout control.
12. Queue volume is a product behavior, not only an infrastructure concern. The worker has to decide deliberately when a message is necessary and suppress repeated no-op work at ingest and sync boundaries.

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
