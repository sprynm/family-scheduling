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
