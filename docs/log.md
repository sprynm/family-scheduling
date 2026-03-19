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
