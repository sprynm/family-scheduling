# Tech Debt

## `runInTransaction()` migration — Fixed

The original `runInTransaction()` debt item is closed. The high-write repository paths were migrated to `db.batch()`, and the old helper no longer exists in `src/`.

The remaining debt is structural rather than transactional:

1. `src/lib/repository.js` is still carrying too many responsibilities and should be split by domain.
2. Route matching in `src/index.js` is still order-dependent and should eventually move to a clearer router shape.
3. Support-table bootstrap still uses sequential `ALTER TABLE ... ADD COLUMN` probes on cold start.

See `docs/todo.md` and `docs/decisions.md` for current active follow-up work.

## deleteSource() — non-atomic cascade — Fixed ✓ 2026-03-06

**File:** `src/lib/repository.js`
**Function:** `deleteSource` (~line 390)

`deleteSource()` executes 9 sequential `.run()` calls to cascade-delete all child
rows before removing the source row. There is no `db.batch()` wrapper. If the
Worker crashes or D1 times out mid-sequence, the source is left partially deleted —
child rows removed, parent row still present, or vice versa.

**Most exposed step:** The delete of `canonical_events` after its children are gone.
A crash between `DELETE FROM event_instances` and `DELETE FROM canonical_events`
leaves orphaned canonical events with no instances.

**Correct fix:** Collect all 9 statements as prepared (un-run) statements and pass
them to `db.batch([...])`. No statement in the cascade depends on the result of a
prior statement (all use the same `sourceId` or a subquery), so they can all be
batched in one call.

**Fixed:** Converted to a single `db.batch([...])` call. All 9 statements run atomically.
