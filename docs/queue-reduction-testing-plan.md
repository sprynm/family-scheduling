# Queue Reduction Testing Plan

## Goals

Validate that queue changes reduce unnecessary message traffic without breaking ingest, sync, or feed generation behavior. The test plan is organized by implementation epic so each commit has a matching verification pass.

## Epic 1: Docs Alignment

- Confirm [queue-reduction-plan.md](C:/Dev/family-scheduling/docs/queue-reduction-plan.md) reflects the "Measure First" framing.
- Confirm this testing plan stays aligned with the implementation phases and acceptance criteria.

## Epic 2: Cron Hardening, Logging, And Best-Effort Dedupe

### Automated tests

- cron continues processing sources after one queue send throws `429`
- cron emits a prune enqueue failure log without crashing the invocation
- enqueueing an `ingest_source` job reuses an equivalent queued or running job
- enqueueing a `sync_google_target` job reuses an equivalent queued or running job
- existing admin rebuild endpoints still return `202`

### Manual checks

- trigger the `*/30` cron in staging and verify the invocation succeeds even if queue send is rate-limited
- confirm logs include source id, job type, and actual queue producer error text
- inspect the jobs table and confirm duplicate active work is reduced but not assumed impossible

## Epic 3: Delayed Retries And Retry Metadata

### Automated tests

- retryable queue-consumer errors call delayed retry instead of immediate retry
- non-retryable queue-consumer errors mark the job failed without retry
- attempt count increments on retryable failure
- retry stops after the configured max attempt count
- retry delay honors Google `Retry-After` when present
- sync job rows preserve summary/error payloads alongside retry metadata

### Manual checks

- force a transient downstream failure and confirm the queue message is delayed instead of immediately re-delivered
- inspect job rows for `attempt_count` and `last_error_kind`
- verify logs show retry classification and delay seconds

## Epic 4: Unchanged-Source Suppression And No-Op Google Sync Gating

### Automated tests

- a `304 Not Modified` fetch records a snapshot and exits before parse or Google sync enqueue
- a `200` response with the same payload hash records `unchanged_payload` and exits before Google sync enqueue
- a changed payload still performs ingest and enqueues downstream sync
- an unchanged source does not rewrite source-derived rows or enqueue Google sync work
- fetch errors still produce snapshot rows and preserve current failure semantics

### Manual checks

- sample a real source that returns validators and confirm conditional requests are used correctly
- sample a real source without validators and confirm payload-hash suppression still avoids no-op downstream work
- compare daily queue message counts before and after rollout for unchanged sources

## Epic 5: Job History Retention

### Automated tests

- daily prune deletes source snapshots older than `PRUNE_AFTER_DAYS`
- daily prune deletes completed `sync_jobs` older than `JOB_HISTORY_RETAIN_DAYS_COMPLETED`
- daily prune deletes failed `sync_jobs` older than `JOB_HISTORY_RETAIN_DAYS_FAILED`
- queued and running jobs survive prune unchanged

### Manual checks

- inspect `sync_jobs` counts before and after the daily prune window
- confirm recent failed jobs remain available for troubleshooting
- confirm old completed job history drops over time instead of growing indefinitely

## Rollout Verification

- Compare daily Cloudflare Queue message volume before and after deployment.
- Confirm cron invocations no longer fail with uncaught queue producer `429` exceptions.
- Confirm queue retry storms are eliminated during transient failures.
- Confirm feed routes and Google sync outputs remain correct for `family`, `grayson`, and `naomi`.
- Confirm `sync_jobs` count trends downward after retention begins pruning historical rows.
