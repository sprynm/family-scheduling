# AGENTS

## Git Mode

**Strict** — Deploys on push to Cloudflare Workers (`wrangler.jsonc`) — personal, but a commit on `main` reaches production.

Branch from a Notion Execution Task before implementation work; merge `--no-ff` and delete the branch in the same session.

With a second agent working this repo at the same time, split by file ownership
or use a separate worktree. See `C:\Users\lance\.agents\policies\git-branch-decision.md`.

## Skill routing

All shared skills are managed globally. Follow the boot sequence:

1. Read `C:\Users\lance\.agents\domains-index.yaml` — match task to a domain by triggers/anti_triggers.
2. Read the matched `domains/<name>.md` — find the right skill and chain order.
3. Open `C:\Users\lance\.agents\skills\<skill>\SKILL.md` — follow the workflow.

This repo maps to the **node** domain.

## Project context

- Owner: personal
- Stack: Cloudflare Workers, ICS/iCalendar, Node
- Role: active implementation repo for the new family scheduling system
- Legacy repo: `C:\_Dev-archive\ics-merge` — reference only, not for new features

## Repo scope

Use this repo for:
- new application code, schema, migrations
- auth and admin console work
- ingest, recurrence, prune, and sync implementation
- tests and operational docs

Use `C:\_Dev-archive\ics-merge` only for:
- legacy feed contract reference (`cals.txt`, `wrangler.jsonc`)
- migration comparison
- emergency fixes to the old worker if explicitly required

## Local rules

- `family-scheduling` is the primary git repo — create feature branches here
- Keep `main` stable — it deploys to Cloudflare Workers on push, so a commit on
  `main` is a release. Verify over HTTP after merging; a green build is not delivery.
- Preserve the `family`, `grayson`, and `naomi` feed contracts when touching compatibility-sensitive code
- UIDs must be stable across syncs — never depend on source ICS UIDs being consistent
