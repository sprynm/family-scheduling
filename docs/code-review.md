# Code Review

_Updated: 2026-03-22_

## Review Result

Current review status is good, with a small set of concrete follow-ups and one larger structural concern:

1. Source save/admin workflow regressions around `uid_fallback_*` and `title_rewrite_rules_json` were real and are now fixed.
2. `304 Not Modified` and unchanged payload handling are now correctly treated as healthy no-op states.
3. Feed identity diagnostics and source-level UID fallback are functioning, but they increased the importance of source-list payload completeness.
4. `src/lib/repository.js` is still too large and remains the main maintainability risk.

## What Changed Since The Previous Review

- Added source-level UID fallback diagnostics and persistence.
- Added inline source diagnostics in admin after save.
- Fixed admin rehydration bugs caused by missing source-list fields.
- Collapsed healthy status rows by default and removed error styling from cache-hit/no-change status details.
- Pushed feed lookback filtering into SQL.
- Cached legacy bootstrap by isolate instead of doing a `COUNT(*)` probe on every request.
- Switched API JSON responses to compact serialization.
- Made Google write spacing configurable via `GOOGLE_WRITE_SPACING_MS`.
- Tightened top-level 500 responses so non-admin callers get a generic message.

## Current Findings

### Fixed In This Pass

- Top-level 500 responses no longer expose raw exception messages to non-admin callers.
- Feed token comparison now uses timing-safe equality.
- Legacy bootstrap is now cached like support-table bootstrap.
- Feed lookback is applied in SQL instead of only in JavaScript.
- Source-list responses now include the full set of edit-critical fields used for form rehydration.
- `TECH_DEBT.md` no longer claims `runInTransaction()` is active debt.

### Still Worth Doing

1. Split `D1Repository` by domain. It still owns too many responsibilities.
2. Replace order-dependent route branching in `src/index.js` with a clearer router shape as the API grows.
3. Consider batching cold-start `ALTER TABLE ... ADD COLUMN` probes if D1 behavior remains acceptable under partial batch failures.
4. Decide whether bootstrap/sample-data insert loops should be normalized to `db.batch()` for consistency, even though they are low-frequency.

## Learnings

- In this admin architecture, list-source payloads are part of the write path because the form rehydrates from them after save.
- Operational status UX needs to distinguish “healthy and unchanged” from “healthy but action required” and from true errors.
- Regex/title rewrite safety is not just parser logic; it also depends on reliable form-state round-tripping.

## Verification

1. `npm test -- --run`
2. `60/60` passing
3. Reviewed current `src/index.js`, `src/lib/repository.js`, `src/lib/responses.js`, `public/admin.js`, `public/admin.css`, `TECH_DEBT.md`, and `test/index.spec.js`
