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
