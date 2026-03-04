# Implementation Plan

This project implements the new family scheduling system in a new sibling folder and leaves the legacy worker in `C:\Dev\ics-merge` unchanged.

The implemented baseline currently covers:

- DB-backed generated compatibility feeds
- preserved `family`, `grayson`, and `naomi` feed contracts
- Cloudflare Worker, D1, R2, Queue, and cron wiring
- admin API baseline with dev-only local auth headers
- source ingestion scaffold from configured ICS secret lists

The remaining major implementation steps are:

- production Google sign-in and role enforcement
- full Google Calendar outbound sync
- recurrence remap policy finalization for ambiguous series rewrites
- prune execution beyond queue/schedule scaffolding

Related legacy context:

- [Legacy worker context](C:/Dev/ics-merge/docs/context.md)
- [Migration review](C:/Dev/ics-merge/docs/google-api-migration-review.md)
- [QA worksheet](C:/Dev/ics-merge/docs/QA%20worksheet.md)
- [Legacy feed URLs](C:/Dev/ics-merge/cals.txt)
