# AGENTS

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
- Legacy repo: `C:\Dev\ics-merge` — reference only, not for new features

## Repo scope

Use this repo for:
- new application code, schema, migrations
- auth and admin console work
- ingest, recurrence, prune, and sync implementation
- tests and operational docs

Use `C:\Dev\ics-merge` only for:
- legacy feed contract reference (`cals.txt`, `wrangler.jsonc`)
- migration comparison
- emergency fixes to the old worker if explicitly required

## Local rules

- `family-scheduling` is the primary git repo — create feature branches here
- Keep `main` stable
- Preserve the `family`, `grayson`, and `naomi` feed contracts when touching compatibility-sensitive code
- UIDs must be stable across syncs — never depend on source ICS UIDs being consistent
