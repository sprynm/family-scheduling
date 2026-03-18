# Queue Reduction Plan: Measure First, Then Suppress Unnecessary Work

## Summary

The goal is to make queue usage deliberate: only emit messages that cause useful new work, avoid repetitive work, and reduce daily queue volume enough to stop repeated 10,000-message days. The plan is split into a narrow first phase for correctness and queue stability, followed by a measured second phase for steady-state volume reduction.

This revision removes speculative work from the first implementation and defers higher-risk optimizations until the system's actual queue behavior is measured.

## Phase 0: Verify Operational Constraints First

Before implementing behavior changes, gather these facts from production or staging:

- actual count of active sources and typical count of due sources per `*/30` cron run
- whether real upstream ICS sources return `ETag` and/or `Last-Modified`
- Cloudflare Queue native retry API behavior:
  whether delayed retry is supported in this environment
  max supported `delaySeconds`
  whether retry redelivery preserves the same message payload and embedded `jobId`
- current Google sync fanout:
  average and worst-case `sync_google_target` jobs per ingest
  average and worst-case chunk follow-up count
- current message mix:
  cron-produced source jobs
  Google sync jobs
  retries
  duplicate active work

This phase is required because several design choices depend on actual platform behavior, not assumptions.

## Phase 1: Immediate Stability Fix

### 1. Make cron producer failures non-fatal

Update `src/index.js`:

- Wrap cron-side `enqueueJob()` calls in `try/catch`.
- Log actual enqueue failures with structured fields.
- Continue processing later sources after a failed enqueue instead of letting the cron invocation fail.
- Emit a per-run cron summary.

Do not add producer pacing yet unless Phase 0 shows current due-source bursts are large enough to justify it.

Conditional addition:
- only add `CRON_QUEUE_SEND_SPACING_MS` if the measured due-source burst size shows that a short paced send materially reduces producer 429s.

### 2. Add best-effort active-job dedupe

Update `src/lib/repository.js`:

- Add best-effort dedupe on active equivalent jobs:
  `job_type + scope_type + scope_id + status in ('queued', 'running')`
- Apply to:
  `ingest_source`
  `rebuild_source`
  `sync_google_target`
  `prune_stale_data`

Important note:
- Treat dedupe metrics as advisory only.
- Do not use dedupe counts alone for capacity planning because read-before-write races will undercount real duplicate attempts.

### 3. Replace immediate retry storms with bounded delayed retries

Update queue consumer handling in `src/index.js`:

- Remove unconditional immediate `message.retry()`.
- Use native delayed retry only if Phase 0 confirms delay support and limit shape.
- Bound retries by attempt count and error classification.
- Permanent failures should stop immediately.

Retry policy:
- retryable: network, upstream `5xx`, Google rate limit/quota, transient queue-send fanout failures
- non-retryable: missing/inactive source, malformed source URL, deterministic parse failure, missing config, non-retryable upstream `4xx`

Backoff schedule:
- must be chosen only after verifying Cloudflare's actual `delaySeconds` ceiling
- if the platform ceiling is lower than the proposed ladder, clamp the schedule intentionally and document it

### 4. Clarify retry identity before adding attempt tracking

Only after Phase 0 confirms retry delivery semantics:

- If delayed retry redelivers the same embedded `jobId`, store `attempt_count` on `sync_jobs`.
- If retries effectively create a fresh delivery that should not mutate the original job row semantics, add a correlation key and track attempts against that retry lineage instead.

This is a design gate, not an implementation detail. The schema choice depends on actual queue semantics.

### 5. Add structured logs

Add logs for:
- cron producer failures
- cron summary
- retry decision
- permanent failure classification

This should ship with Phase 1 so the next phase is driven by observed behavior.

## Phase 2: Reduce Baseline Daily Queue Volume

### 6. Add unchanged-source suppression

Extend `ingestSource()` in `src/lib/repository.js`:

- First preference: use `If-None-Match` / `If-Modified-Since` only if Phase 0 shows upstream sources actually support them often enough to matter.
- Required fallback: compute and store a full-payload hash for the fetched ICS body.
- If the payload is unchanged from the latest successful fetch, stop before parse, DB rewrite, or Google sync enqueue.

Schema addition:
- `source_snapshots.payload_hash`

Do not overinvest in conditional-request support if real sources do not return validators reliably.

### 7. Define "effective change" explicitly and use it to suppress Google sync

This step needs to be decision-complete before coding.

Chosen design:
- `ingestSource()` will compute change counts inline during the existing write pipeline rather than query before/after snapshots.
- The ingest result will explicitly return:
  `fetch_state`
  `canonical_events_inserted`
  `canonical_events_updated`
  `canonical_events_soft_deleted`
  `instances_inserted`
  `instances_updated`
  `instances_soft_deleted`
  `output_rules_inserted`
  `output_rules_updated`
  `output_rules_deleted`
  `data_changed`

Definition of `data_changed`:
- true if any source-derived canonical event, instance, or output rule is inserted, updated, or soft-deleted
- false for `not_modified` and `unchanged_payload`
- false if ingest completes but all persisted source-derived state is identical to the previous state

Google sync enqueue rule:
- only enqueue Google sync if `data_changed === true`

This keeps the decision local to ingest and avoids expensive before/after comparison queries.

### 8. Defer source-target aggregate hashing

Do not include aggregate per-source-target desired-output hashing in the next implementation pass.

Reason:
- highest correctness risk
- requires deterministic serialization discipline
- not needed until simpler suppression measures are measured in production

Only revisit if unchanged-source suppression plus `data_changed` gating still leaves queue volume too high.

### 9. Do not change Google sync chunk size until quotas are measured

Keep current chunk size initially.

Only consider changing `GOOGLE_SYNC_JOB_CHUNK_SIZE` after measuring:
- Worker runtime behavior
- Google API call count per chunk
- quota/rate-limit response pattern

If tuning is needed later:
- make the chunk size configurable
- choose the new default from measured quota behavior, not by assumption

## Public Interfaces / Config

Only add config that is justified by verified behavior:

- `JOB_RETRY_MAX_ATTEMPTS`
- `JOB_RETRY_JITTER_PCT`

Conditional config, only if Phase 0 justifies it:
- `CRON_QUEUE_SEND_SPACING_MS`
- `CRON_MAX_ENQUEUES_PER_RUN`

Schema additions:
- `source_snapshots.payload_hash`
- retry metadata only after retry identity semantics are confirmed

Use the existing repository-init `ALTER TABLE ... ADD COLUMN` pattern rather than a new migration file unless there is a clear reason to change migration strategy.

## Test Plan

### Phase 1

- cron does not fail when one queue send throws `429`
- cron continues after one enqueue failure
- active equivalent jobs are best-effort deduped
- retryable failures use delayed retry if supported
- non-retryable failures do not retry
- retry attempt behavior matches verified queue semantics
- structured logs are emitted for producer failure and retry decisions

### Phase 2

- unchanged payload exits before parse and before Google sync enqueue
- `304` suppression is only tested if validator support is actually implemented
- changed payload still performs full ingest
- `data_changed=false` suppresses Google sync enqueue
- source changes that modify persisted source-derived state still queue sync correctly

Acceptance criteria:
- cron no longer fails with uncaught queue producer rate-limit exceptions
- immediate retry storms are eliminated
- unchanged source checks no longer create downstream queue work
- daily queue volume drops measurably from the current baseline

## Assumptions

- The first implementation should optimize for correctness and observability, not speculative tuning.
- Best-effort dedupe is sufficient because handlers remain idempotent.
- Aggregate-hash suppression and chunk-size tuning are deferred until simpler reductions are measured.
- The real target is not just "fewer crashes"; it is fewer unnecessary queue messages overall.
