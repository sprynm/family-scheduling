# Code Review: `family-scheduling` vs. Implementation Plan

_Initial review: 2026-03-03 | Updated: 2026-03-04 (source model refactor pass)_

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
| 10 | Bucket-level source model | Refactored to one real source per `sources` row with per-source URL, icon, prefix, inclusion flags, and CRUD API support |
| 11 | Output rendering contract drift | Feed decoration now explicit via `presentation.js`; child feeds render icon-only, family feed renders prefix + icon + title |
| 12 | Source CRUD API missing | `POST /api/sources`, `PATCH /api/sources/:id`, `DELETE /api/sources/:id` added with input validation and 400 errors |
| 13 | Source-config propagation | `syncSourceConfig()` propagates icon/prefix changes and re-derives output rules for existing instances on update |
| 14 | `syncSourceConfig` transaction ordering | Confirmed correct — `source_deleted = 0` write precedes `listSourceInstances` read within the same transaction |
| 15 | Override actor role provenance | Override records now store the verified role from `getRoleFromRequest()` rather than the raw request header |
| 16 | Source disable operational consistency | `disableSource()` now marks `source_events.is_deleted_upstream = 1` so operational history matches published state |

**Review corrections (accepted):**
- Feed token was already strict equality against `env.TOKEN`, not just non-empty — the concern was invalid.
- `makeId()` randomness did not affect `identity_key`, which was already deterministic. `stableId()` added for derived records as a clarifying convention.
- `ingest safety` note on staged-before-mutate language updated: the flow provides meaningful protection against the failure mode but does not provide atomic rollback guarantees where `exec()` is unavailable.

---

## Still Open

### 🔴 Real Google sign-in not implemented
Current state is production-safe — admin routes deny by default. But the admin APIs are unusable in production until OAuth is wired.

**Required:** Google OAuth flow, token verification, role mapping from Google identity to `admin`/`editor`.

### 🟡 Google Calendar outbound sync not implemented
`google_targets` and `google_event_links` tables are defined. No Google API client or sync logic exists. Per the plan this is output-only in v1 — write path only, no read-back.

### 🟡 Prune job scaffolded but not executing
`PRUNE_AFTER_DAYS=30` is configured but the actual `DELETE` statements are not in place. `source_snapshots` and `source_events` will grow unbounded until this lands.

### 🟡 Recurrence remap policy unresolved
Series master representation, per-instance override attachment to remapped upstream instances, and upstream rewrite detection are not fully designed or enforced. Pre-Phase-3 design prereq — schema supports it, logic does not exist.

### 🟢 Test coverage still shallow
Suite covers auth guard, source CRUD, feed rendering, and per-source ingest shape. Identity/deduplication, recurrence expansion, override derivation, and real D1 SQL semantics remain only partially covered. `FakeDb` means the suite is strong worker-shape verification, not full persistence correctness.

---

## Recommended Next Step

1. **Implement Google sign-in** — gating dependency for any production use of the admin APIs.
2. **Then choose between Google outbound sync and prune execution.**
   - Google sync adds end-user value.
   - Prune closes the main remaining retention/operations gap.
