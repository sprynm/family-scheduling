# Claude Instructions

## Project Status

This repo is the active codebase for the new family scheduling platform.

The legacy worker repo lives at `C:\Dev\ics-merge` and remains available in the workspace for reference only.

## Where To Make Changes

Make new implementation changes in this repo unless the task is explicitly about:

- legacy production support in `ics-merge`
- comparing old and new feed behavior
- reading old docs or route contracts

## Legacy Compatibility Rules

The migration-critical external contracts are:

- `/family.isc`
- `/grayson.isc`
- `/naomi.isc`

Plus `.ics` aliases and the existing tokenized access model.

When changing feed behavior here, check legacy references in:

- `C:\Dev\ics-merge\cals.txt`
- `C:\Dev\ics-merge\wrangler.jsonc`
- `C:\Dev\ics-merge\docs\context.md`

## Repo Separation

- `family-scheduling` is the active repo
- `ics-merge` is the legacy repo
- do not blur histories or move new work back into `ics-merge`

## Immediate Focus

The next bounded production feature is Google sign-in.

The remaining follow-on work is:

- Google outbound sync
- prune execution
- recurrence remap policy and deeper tests
