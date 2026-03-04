# Code Review: `family-scheduling` vs. Implementation Plan

_Initial review: 2026-03-03 | Updated: 2026-03-03 (post-fix pass)_

---

## Resolved

| # | Item | Notes |
|---|---|---|
| 1 | Auth header guard | `x-user-role` now requires `ALLOW_DEV_ROLE_HEADER=true`; production denies without real auth |
| 2 | Queue consumer missing | Consumer and scheduled handlers added; `wrangler.jsonc` updated |
| 3 | Source ingestion unimplemented | Basic ingest path added: fetch → snapshot → parse → materialize → derive output rules |
| 4 | Cron trigger not configured | Added to `wrangler.jsonc` |
| 5 | `seedSampleData()` on every request | Now gated behind `SEED_SAMPLE_DATA=true` |
| 6 | `lookback` not passed through | Passed from route handler through to `generateFeed()` |
| 7 | `implementation-plan.md` was a stub | Fleshed out; no longer a placeholder |
| 8 | Auth guard test coverage | Extended in `index.spec.js` |
| 9 | Ingest safety on upstream failure | Ingest now fetches/parses first and only mutates published state inside a rollback-safe transaction boundary when available; failed fetches preserve prior published state |

**Review corrections (accepted):**
- Feed token was already strict equality against `env.TOKEN`, not just non-empty — the concern was invalid.
- `makeId()` randomness did not affect `identity_key`, which was already deterministic. `stableId()` added for derived records as a clarifying convention.

---

## Still Open

### 🔴 Real Google sign-in not implemented
Current state is production-safe for the protected API surface because admin routes deny by default, but the production admin APIs are unusable until OAuth is wired. This is the next hard blocker after ingest safety for any real deployment.

**Required:** Google OAuth flow, token verification, role mapping from Google identity to `admin`/`editor`.

### 🟡 Google Calendar outbound sync not implemented
`google_targets` and `google_event_links` tables are defined. No Google API client or sync logic exists. Per the plan this is output-only in v1 — write path only, no read-back.

### 🟡 Prune job scaffolded but not executing
Retention delete logic is still pending. `PRUNE_AFTER_DAYS=30` is configured but the actual `DELETE` statements are not in place. `source_snapshots` and `source_events` will grow unbounded until this lands.

### 🟡 Recurrence remap policy unresolved
Series master representation, per-instance override attachment to remapped upstream instances, and upstream rewrite detection are not fully designed or enforced. This is a pre-Phase-3 design prereq — the schema supports it but the logic does not exist.

### 🟢 Test coverage still shallow
Auth guard tests were added. The identity/deduplication, recurrence expansion, override derivation, and real ingest path remain largely untested beyond the routing layer. The `FakeDb` mock does not enforce real D1 SQL semantics, so current tests should be treated as worker-shape verification rather than persistence correctness.

---

## Recommended Next Step

Recommended sequence:

1. **Implement Google sign-in next.**
   - It is still the gating dependency for production use of the protected admin APIs.
   - It is self-contained enough to implement without blocking on Google sync or recurrence remap work.

2. **Then choose between Google outbound sync and prune execution.**
   - Google sync adds end-user value.
   - Prune execution closes the main remaining retention/operations gap.

Google sign-in is now the recommended next bounded feature.

---

## Post-Review Update: Ingest Safety Remediation

After the updated review, ingest safety was implemented to address the most serious remaining runtime correctness issue.

### What changed

- `ingestSource()` no longer marks existing source records as deleted before upstream fetches succeed.
- Upstream URLs are now fetched and parsed first.
- If any fetch fails, the ingest run throws before the currently published state is mutated.
- Published-state mutation now happens only after staging succeeds.
- Where the D1 binding supports `exec()`, the publish step runs inside a transaction boundary with rollback on failure.
- Where transactional support is not available, the code still uses the safer staged-before-mutate flow so a simple statement reorder is not mistaken for a full safety fix.

### Why this matters

Before this change, a temporary upstream failure could remove a source's events from generated feeds even though the last known good state should have remained visible.

The fix makes the intended behavior explicit:

- fetch/parse failure should preserve prior published state
- successful staging is required before promotion into the active published view
- rollback-safe publish behavior is the requirement, not merely “do the delete later”

### Effect on priority

This remediation changes the next-step priority order.

Before the fix:

- ingest safety was the highest-severity remaining blocker
- Google sign-in was important, but not the first issue to address

After the fix:

- Google sign-in is now the best next bounded feature
- the remaining major gaps are auth, Google outbound sync, prune execution, and recurrence remap policy

### Files updated for this remediation

- `src/lib/repository.js`
- `docs/code-review.md`
