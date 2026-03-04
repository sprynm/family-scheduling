# Repository Guidance

## Role Of This Repo

This is the active implementation repo for the new family scheduling system.

Use this repo for:

- new application code
- database schema and migrations
- auth and admin console work
- ingest, recurrence, prune, and sync implementation
- tests for the new system
- operational and implementation docs for the new system

## Workspace Context

This workspace also contains the legacy repo at `C:\Dev\ics-merge`.

Use `C:\Dev\ics-merge` only for:

- legacy feed contract reference
- existing production route and token behavior reference
- migration comparison
- emergency fixes to the old worker if explicitly required

Do not implement new platform features in the legacy repo.

## Version Control Expectations

- `family-scheduling` is its own git repo and is the primary repo for ongoing work.
- Create feature branches here for new work.
- Keep `main` stable.
- Treat `ics-merge` as a separate legacy repo with separate history.

## Development Boundary

When working on behavior that must remain compatible with the old system:

- verify against `C:\Dev\ics-merge\cals.txt`
- verify against `C:\Dev\ics-merge\wrangler.jsonc`
- preserve the `family`, `grayson`, and `naomi` feed contracts

## Current Priorities

1. Production Google sign-in and role enforcement
2. Google outbound sync
3. Prune execution
4. Recurrence remap policy and tests
