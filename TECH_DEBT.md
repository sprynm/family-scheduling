# Tech Debt

## runInTransaction() — migrate callers to db.batch()

**File:** `src/lib/repository.js`
**Lines:** 329, 353, 435, 924, 1214

`runInTransaction()` was written to wrap multi-statement writes in a SQL transaction.
D1 does not support `BEGIN`/`COMMIT`, so the implementation was gutted to just call
`work()` directly. The callers now run their statements as sequential, independent
writes with no atomicity.

**Risk:** If the Worker crashes or D1 times out mid-sequence, the database can be
left in a partially-written state. The most exposed caller is `setSourceTargets`,
which deletes all target links then re-inserts them — a crash between those two
steps leaves a source with no output mappings.

**Correct fix:** Replace each `runInTransaction(async () => { ... })` body with a
`db.batch([stmt1, stmt2, ...])` call. All prepared statements must be built without
`.run()` and passed as an array. Statements that depend on the result of a prior
statement within the same sequence cannot be batched — those need to be restructured
(e.g. using RETURNING to get inserted IDs).

See `C:\Dev\Agents\antipatterns\d1-sql-transactions.md` for the full pattern.

**Priority:** Low — the failure window is narrow and the path is admin-only with
low concurrency. Address before any high-volume or user-facing write paths are added.

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
