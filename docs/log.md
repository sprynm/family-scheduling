# Development Log

> Write here frequently during active work. Compact this file at closeout.

NOTE:
This file should be written to frequently during development.
At closeout of a unit of work, insights should be compacted into:
- state.md
- documentation.md
- decisions.md
- plan.md
- external knowledge base if appropriate

## 2026-03-11

### Session Notes

- Investigated a reported import of `1763` feed items for the family source.
- Confirmed the active app already limits recurring expansion to a 180-day horizon and limits Google/ICS outputs to the configured lookback window, but ingest was still writing stale historical single events into D1.
- Confirmed the admin `Sync'd` number was Google-target-specific, not a general indicator of imported or upcoming events.

### Work Completed

- Added an ingest guard that skips one-off events whose end time is older than the configured retention window and skips recurrence series that produce no retained instances.
- Added `INGEST_PAST_RETENTION_DAYS` to runtime configuration with a default of `30`.
- Updated the admin source metrics to display `Google sync: n/a` for sources without Google targets and to label synced counts as Google-specific.
- Added a regression test covering the stale-event ingest cutoff.
- Deployed the worker successfully via Wrangler. Current version: `0ad8b503-2c78-41db-ab08-79a1675ec014`. Published URL: `https://family-scheduling.lance-e35.workers.dev`.

### Discoveries

- `eventsParsed` reflects raw parsed feed events before retention filtering, so it can remain high even when fewer rows are persisted.
- The existing prune path currently targets old source snapshots; it does not remove canonical event history created by prior ingest runs.

### Next Steps

- Rebuild the affected family source in production so the oversized historical import is reprocessed under the new ingest retention window.

## 2026-03-18

### Session Notes

- Investigated production Cloudflare Queue volume and confirmed the immediate failure mode was cron producer `429 Too Many Requests`, with additional message churn coming from immediate retry behavior and unchanged-source fanout.
- Split the fix into queue stability and deliberate message suppression so the worker only emits jobs that move state forward.

### Work Completed

- Added non-fatal cron enqueue handling with structured `cron_enqueue_failed` and `cron_enqueue_summary` logs.
- Added best-effort active-job dedupe for `ingest_source`, `rebuild_source`, `sync_google_target`, and `prune_stale_data`.
- Replaced immediate queue retries with delayed, bounded retries and persisted retry metadata on `sync_jobs`.
- Added `payload_hash` to `source_snapshots`, conditional fetch headers for sources that support them, and early exits for `304` and unchanged payloads.
- Gated Google sync enqueueing on effective source-state changes so no-op ingests do not fan out downstream sync jobs.
- Added and documented a queue-reduction testing plan, then ran the full `vitest` suite successfully.
- Merged the feature branch into `main` and deployed the worker via Wrangler.

### Discoveries

- Queue volume is primarily a behavior problem at cron and ingest boundaries, not just a provider limit issue.
- Best-effort dedupe is useful but not authoritative because D1 read-before-write races can still allow occasional duplicate attempts.
- `hashValue()` is deterministic across restarts; the operational risk is not hash instability but computing state fingerprints more often than necessary.

### Release Result

- Deployment completed successfully on 2026-03-18.
- Current version: `42dc99fc-4c0a-491b-85ce-75ca730b040d`.
- Published URL: `https://family-scheduling.lance-e35.workers.dev`.
- Post-deploy validation target: watch `cron_enqueue_summary`, `cron_enqueue_failed`, `queue_job_retry_scheduled`, `queue_job_failed`, and daily Cloudflare Queue message volume for the next full cron cycle.

## 2026-03-19

### Session Notes

- Investigated whether the large `sync_jobs` count represented live calendar size or retained operational history.
- Confirmed the queue cleanup gap was in job-history retention, not in event/output state.

### Work Completed

- Added `sync_jobs` retention to the daily prune path.
- Configured prune to delete completed jobs older than 7 days and failed jobs older than 30 days while leaving queued/running jobs untouched.
- Added regression coverage for snapshot and job-history pruning.
- Cleaned the stale queued outage-era rows from production and verified the queue recovered to zero queued jobs.
- Deployed the retention update via Wrangler.

### Discoveries

- `sync_jobs` has real short-term operator value, but most of its growth is operational exhaust rather than durable business state.
- Recent Cloudflare D1 reads can lag immediately after writes; verification queries may need a short delay after manual cleanup.

### Release Result

- Deployment completed successfully on 2026-03-19.
- Current version: `05d22fe1-be10-4380-aee1-cca664194c6e`.
- Published URL: `https://family-scheduling.lance-e35.workers.dev`.

## 2026-03-21

### Session Notes

- Investigated a Ramp Interactive lacrosse feed that appeared to duplicate one-off events in Google Calendar.
- Confirmed the feed uses nonrepeating `VEVENT`s, but the upstream `UID`s can churn across fetches even when the event content stays the same.
- Added a source-level UID fallback toggle plus a two-fetch diagnostic so operators can see whether a source is stable or needs fallback matching.

### Work Completed

- Added `uid_fallback_enabled`, `uid_fallback_checked_at`, and `uid_fallback_report_json` to source persistence.
- Added `POST /api/sources/:id/diagnose` to compare two fetches and report UID stability findings.
- Updated source save flow in the admin UI so it runs the diagnostic immediately after submit and displays the report inline.
- Wired ingest to honor the source-level fallback toggle for unstable standalone events.
- Added regression coverage for UID churn detection, fallback persistence, and fallback matching.
- Deployed the worker successfully via Wrangler. Current version: `8168daf1-aa74-4f85-b971-8afc5f62cbb2`. Published URL: `https://family-scheduling.lance-e35.workers.dev`.

### Discoveries

- A nonrepeating `VEVENT` having a `UID` is normal; stability across fetches is the real contract.
- The current misbehavior is a feed identity problem, not a recurrence-expansion problem.
- A single operator-visible report is more useful than an implicit auto-fix because it shows when the system is falling back and why.

## 2026-03-22

### Session Notes

- Investigated an admin source-submit regression where saving the Tiger feed cleared the editor state and made the UID fallback diagnostic panel overwhelm the form.
- Confirmed the underlying ingest behavior was still correct: `304 Not Modified` and unchanged payloads are successful no-op fetch states, not errors.
- Tightened the admin source editor so submit preserves the current form values and only updates the diagnostic output inline.

### Work Completed

- Removed the post-save source form reset so editing state, selected outputs, and the UID fallback checkbox remain intact after submit.
- Constrained the UID fallback diagnostic block in the source form so long reports do not break the layout.
- Preserved `not_modified` and `unchanged_payload` as healthy fetch states in the admin source status display.
- Ran the full test suite successfully.
- Deployed the worker successfully via Wrangler. Current version: `7df561ab-db1b-4a67-8ffb-add6f66d8b11`. Published URL: `https://family-scheduling.lance-e35.workers.dev`.

### Discoveries

- The admin save flow was the source of the visual breakage, not the feed ingest or diagnostic endpoint.
- `304 Not Modified` is a performance optimization signal and should remain visible as a successful cache hit, not a failure.
- Keeping the form populated after submit is important because operators often need to flip UID fallback immediately after reviewing the diagnostic.

## 2026-03-22 (QA Regression Hardening)

### Session Notes

- Performed a QA-style debug pass on the source editor after repeated operator reports that UID fallback and title rewrite settings were not persisting reliably.
- Traced regressions to list-projection gaps: source list responses were missing fields required to rehydrate edit form state.
- Completed UX adjustments on status rows to reduce noise and better distinguish healthy cache/no-change outcomes from real errors.

### Work Completed

- Fixed source list projection to include `uid_fallback_*` fields so fallback state survives post-save refresh and edit-mode rehydration.
- Fixed source list projection to include `title_rewrite_rules_json` so regex rewrite rules are not blanked in the edit form and then accidentally overwritten on save.
- Added regression tests for both field families (`uid_fallback_*` and rewrite rule text) in list-source responses.
- Hardened admin source form behavior so save remains successful even if diagnostic fetch fails, with a separate diagnostic warning.
- Updated source status UI:
  - `ok` rows are collapsed by default with click-to-open details.
  - `304 Not Modified`/unchanged payload details use neutral styling, not error styling.
  - Removed noisy `UID fallback: off, not yet tested.` text from healthy untested feeds.
- Deployed successive fixes via Wrangler.
  - `31b3478b-c599-4ddb-8b71-68dab0a1fbf5` (save/edit-state workflow hardening)
  - `ca7652de-224a-4e5e-8e19-a12a6f560ef0` (status UX collapse + neutral 304 detail + fallback summary cleanup)
  - `2c782eaf-d428-4b01-affb-68049c0a81f5` (title rewrite projection fix)

### Discoveries

- In this admin architecture, list-source payload completeness is part of write safety because the edit form is rehydrated from list data after save.
- Field omissions in source list responses can silently mutate unrelated source settings on subsequent saves.
- Operator trust improves when status UI is dense by default but expandable, with error colors reserved for true fault states.

## 2026-03-22 (Review Follow-Up)

### Session Notes

- Validated an external review against the live codebase and separated immediate fixes from longer-horizon maintainability work.
- Confirmed several findings were real low-risk wins: top-level error exposure, per-request legacy bootstrap probing, JavaScript-only feed lookback filtering, pretty-printed JSON responses, stale tech-debt docs, and hardcoded Google write spacing.

### Work Completed

- Restricted top-level `500` response detail to authenticated admin callers; non-admin callers now get a generic internal error message.
- Switched feed token comparison to timing-safe equality.
- Cached legacy source bootstrap per D1 binding, matching the existing support-table bootstrap pattern.
- Moved feed lookback filtering into SQL in `generateFeed()`.
- Switched API JSON responses to compact serialization.
- Made Google write spacing configurable via `GOOGLE_WRITE_SPACING_MS` with a default of `75`.
- Removed the stale `runInTransaction()` debt claim from `TECH_DEBT.md` and refreshed `docs/code-review.md`.
- Added regression coverage for non-admin/admin `500` behavior and bootstrap caching.

### Discoveries

- `listSources()` is effectively a form-state contract, while the top-level fetch catch is effectively a public error boundary; both need to be reviewed as API contracts, not helper details.
- Bootstrap paths that are cheap once can still matter when they run on every feed request.
- The review was directionally strong: the highest-value fixes were not architectural rewrites, but small contract corrections at boundaries.
