# Project Decisions

> Durable project decisions and their rationale.

## 2026-03-11 - Drop stale historical single events at ingest

Decision:
Do not write one-off ICS events into D1 when their effective end time is more than 30 days in the past. Keep the retention window configurable via `INGEST_PAST_RETENTION_DAYS`, defaulting to `30`.

Rationale:
Large upstream feeds can include deep history that is operationally meaningless for this product but still creates canonical rows, instances, output rules, and rebuild cost. The app already limits generated ICS and Google sync to recent windows; ingest should not retain clearly stale single events that will never be surfaced again.

Consequences:
Historical single events older than the configured retention window are ignored during ingest and do not contribute to event counts, output rules, or Google sync work. Recurring events are still evaluated through recurrence expansion, and recurrence series with no retained instances are skipped entirely.

Status:
active

Revisit When:
If operators need deeper in-app historical search/audit than the current snapshot retention and output windows provide.

## 2026-03-11 - Make source sync metrics explicitly Google-only

Decision:
Admin source-row sync metrics must describe Google outbound sync only, and sources without Google targets must display `Google sync: n/a` rather than `Sync'd: 0`.

Rationale:
The previous label implied that no events were active or imported when the underlying metric was actually counting only rows synced to Google-managed outputs. Family-only or ICS-only sources could therefore show `0 sync'd` despite having valid upcoming events.

Consequences:
Operator status is less misleading during source review. The dashboard now separates feed event counts from Google-specific output sync state and avoids false alarms on sources that intentionally have no Google targets.

Status:
active

Revisit When:
If the admin UI gains a deeper source detail view with separate ICS, ingest, and Google pipeline metrics.

## 2026-03-21 - Add source-level UID fallback for unstable upstream feeds

Decision:
Sources may opt into UID fallback matching when a two-fetch diagnostic shows that upstream `UID`s change across identical standalone events. The fallback state is stored on the source row and surfaced in admin.

Rationale:
Some upstream calendar providers emit stable event content but regenerate `UID`s on every request. Trusting those `UID`s creates duplicate canonical rows and duplicate Google writes even though the real-world event has not changed.

Consequences:
Operators can run a diagnostic after saving a source, see whether the feed is stable or unstable, and enable fallback only when needed. Normal sources continue to use UID-based matching. The admin now records the latest diagnostic result so the behavior is visible, not implicit.

Status:
active

Revisit When:
If more providers show UID churn and the fallback heuristic needs to move from source-specific toggles to a broader automated policy.

## 2026-03-22 - Treat 304 Not Modified and unchanged payloads as healthy source states

Decision:
Source status reporting must treat `304 Not Modified` and unchanged-payload fetches as successful no-op outcomes, not errors.

Rationale:
Conditional fetches are a core optimization for stable calendar feeds. When the upstream responds `304` or the payload hash matches the previous fetch, the worker has successfully avoided unnecessary parsing and downstream sync work.

Consequences:
The admin status view now reports these states as healthy, and operators can distinguish them from genuine fetch or parse failures. The ingest pipeline continues to skip unnecessary work on these paths.

Status:
active

Revisit When:
If the admin console gains a separate cache-efficiency panel that makes the no-op fetch states redundant in the primary source list.

## 2026-03-22 - Source list payload must include all source-edit fields

Decision:
`listSources()` is a contract for source editor rehydration and must include all mutable source configuration fields used by the admin edit form, including `title_rewrite_rules_json` and `uid_fallback_*`.

Rationale:
The admin save workflow refreshes source rows and then repopulates form state from list payloads. If editable fields are omitted from that payload, the UI rehydrates stale/blank values and a later save can unintentionally overwrite persisted settings.

Consequences:
Source list projection changes are now treated as write-path sensitive, not read-only display tweaks. Tests must verify the presence of edit-critical fields in list responses to prevent silent config drift.

Status:
active

Revisit When:
If the admin architecture moves to source-detail fetches for form rehydration instead of list-source payloads.

## 2026-03-22 - Top-level 500 responses are role-aware

Decision:
Top-level unhandled exceptions must return generic `500` messages to unauthenticated or non-admin callers, while authenticated admin callers may receive the specific error message for debugging.

Rationale:
Feed routes and other non-admin surfaces should not leak internal implementation details through unexpected exceptions. Admin users still need actionable diagnostics when troubleshooting configuration or runtime problems.

Consequences:
The Worker now treats its final `catch` as a public security boundary, not just a convenience response formatter. Route-local bad-request responses can stay detailed where explicitly intended, but unhandled `500`s are now role-sensitive.

Status:
active

Revisit When:
If the app gains structured server-side error IDs and an operator-visible error log that makes raw admin error messages unnecessary.
