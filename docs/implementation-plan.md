# Implementation Plan

This project implements the new family scheduling system in a new sibling folder and leaves the legacy worker in `C:\Dev\ics-merge` unchanged.

The implemented baseline currently covers:

- DB-backed generated compatibility feeds
- preserved `family`, `grayson`, and `naomi` feed contracts
- Cloudflare Worker, D1, R2, Queue, and cron wiring
- admin API baseline with dev-only local auth headers
- per-source ICS ingestion using stored source URLs and metadata
- source create, update, disable, and rebuild API flows

The remaining major implementation steps, in order, are:

1. production admin authentication via Cloudflare Access with Google as the identity provider
   this is the gate for production admin use
2. prune execution beyond queue/schedule scaffolding
   this closes the main operational retention gap
3. recurrence remap policy finalization for ambiguous series rewrites
   this is the main remaining correctness/design gap in event handling
4. full Google Calendar outbound sync
   this is the largest remaining implementation surface

## Auth Decision

The admin surface will use Cloudflare Access with Google as the identity provider.

This project will not build a custom Google OAuth session system inside the Worker for v1.

Auth separation rules:

- public ICS feeds remain token-protected and publicly reachable
- admin shell and admin APIs are protected by Cloudflare Access
- the Worker validates Access identity and maps verified users to `admin` or `editor`

Recommended deployment split:

- public feeds on `ics.sprynewmedia.com`
- admin app on a dedicated admin hostname protected by Access

See [Google auth decision](C:/Dev/family-scheduling/docs/google-auth-decision.md).

## Interaction Model

### Google Calendar

Google Calendar is the primary consumption and distribution surface in v1, not the primary editing surface.

Use it for:

- day-to-day viewing
- reminders and notification delivery
- shared calendar visibility
- distribution to family members who already live in Google Calendar

In v1:

- the system writes Google calendar state
- users do not manage synced events in Google
- manual Google edits are unsupported and may be overwritten

### ICS

ICS is the compatibility and integration surface.

Use it for:

- grandparents' subscriptions
- Home Assistant
- stable external calendar-feed contracts
- clients that can only consume ICS

In v1:

- ICS feeds are generated from DB state
- they are read-only
- they preserve the legacy `family`, `grayson`, and `naomi` contracts

### Legacy feed rendering contract

The legacy child and family feeds do not render identical titles.

This behavior must be preserved intentionally because downstream consumers use the feeds differently.

Required rendering progression:

- a source event first belongs to a child-oriented feed such as `naomi.ics` or `grayson.ics`
- in the child feed, the event gets its configured icon or activity marker
- when that same event is included in `family.ics`, the family view adds the child prefix such as `N:` or `G:`

Examples:

- `naomi gcal + hockey.ics + baseball.ics -> naomi.ics`
- `hockey.ics + baseball.ics -> naomi-sports Google calendar`
- child feed rendering: icon only
- family feed rendering: child prefix plus icon

This is not only a cosmetic rule. It is part of the compatibility contract because Home Assistant uses `naomi.ics` and `grayson.ics` as child-specific snapshot feeds.

Required output-specific decoration rules:

- `naomi.ics` and `grayson.ics` apply icon decoration but do not prepend child initials
- `family.ics` applies child prefix decoration in addition to icon decoration
- managed child Google output calendars such as `naomi-sports` and `grayson-sports` must use their own output rules and should not automatically inherit the same rendering as ICS

This decoration logic must be data-driven and output-specific. It must not depend on hard-coded string rules embedded in route handlers.

### Web App / PWA

The web app is the primary management surface.

Use it for:

- event review
- manual overrides
- source management
- rebuild and diagnostics
- sync visibility
- admin and editor workflows

The intended path is web app first, with PWA capability as an enhancement. A native Android app is not a v1 requirement.

### Android App

Native Android is deferred.

It should only be considered later if the product needs:

- richer mobile UX than a PWA can provide
- offline-first behavior
- tighter device-native integration

## Source Management Status

### Required parent-admin use case

A non-technical parent must be able to manage source calendars without editing code, touching Wrangler config, or updating secrets by hand.

That includes:

- adding a new ICS calendar feed
- removing or disabling an existing ICS calendar feed
- assigning the feed to `Naomi`, `Grayson`, `Family`, or another supported audience
- choosing a display prefix such as `N:` or `G:`
- choosing an icon for a sport or activity
- editing the display name shown in the admin UI

This should be treated as a first-class product requirement, not an operator-only convenience.

Admin support for adding new ICS feeds is now covered at the API and data-model level, but not yet at the polished parent-facing UI level.

Covered now:

- the schema already models `sources`
- sources carry child ownership, category, URL, icon, prefix, and output-inclusion metadata
- the runtime ingests from one stored URL per source row
- the admin API can create, update, disable, list, and rebuild sources
- source presentation and feed-routing rules are stored as data rather than hard-coded in route handlers

Not covered yet:

- polished parent-facing UI for creating/editing sources directly
- Google-authenticated production access to those source-management routes
- curated icon/prefix pickers and source templates for non-technical users
- richer validation and onboarding around new ICS URL entry

Conclusion:

- assigning a feed to a child is now part of the actual admin workflow model
- assigning a prefix such as `N:` or `G:` is explicitly modeled and persisted as source data
- assigning a source icon is explicitly modeled and persisted as source data
- the remaining work is making that workflow comfortable for a non-technical parent, not inventing the data model

Recommended follow-up:

Add parent-friendly source-management UX for:

- icon selection
- prefix selection
- display-name editing
- friendly child/audience assignment
- safe create/update/disable actions in the web app

### Configuration direction

To avoid hard-coding, source presentation and routing rules should be data-driven.

Recommended source metadata additions:

- `display_name`
- `icon`
- `prefix`
- `audience_key` or equivalent replacement for the current narrow `owner_type`
- `source_category`
- `is_enabled`
- `sort_order`
- `include_in_child_ics`
- `include_in_family_ics`
- `include_in_child_google_output`

Recommended behavior:

- feed-to-child assignment comes from source metadata, not route-specific code branches
- prefix assignment comes from source metadata, not hard-coded constants
- sport or activity icon comes from source metadata, not summary regex rules
- the admin UI should offer simple controlled choices for common icons and prefixes, but persist them as data
- the same event may render differently by output surface based on those stored rules

This preserves flexibility without requiring non-technical users to understand implementation details.

Related legacy context:

- [Legacy worker context](C:/Dev/ics-merge/docs/context.md)
- [Migration review](C:/Dev/ics-merge/docs/google-api-migration-review.md)
- [QA worksheet](C:/Dev/ics-merge/docs/QA%20worksheet.md)
- [Legacy feed URLs](C:/Dev/ics-merge/cals.txt)
