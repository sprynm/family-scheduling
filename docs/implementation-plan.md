# Implementation Plan

_Updated: 2026-03-06_

This project implements the new family scheduling system in `C:\Dev\family-scheduling` and leaves the legacy worker in `C:\Dev\ics-merge` as reference only.

## Completed Now

1. Preserve the legacy public feed contracts for `family`, `grayson`, and `naomi`.
2. Run the app on Cloudflare Worker + D1 + Queue + cron scaffolding.
3. Protect admin APIs with Cloudflare Access JWT validation and app-level role mapping.
4. Serve the admin shell at `/admin` and keep public ICS feeds token-protected outside Access.
5. Support ICS source create, update, disable, list, and rebuild flows.
6. Support Google target create, update, delete, and list flows.
7. Support explicit many-to-many source-to-target assignment through `source_target_links`.
8. Store per-target rule metadata on source-target links:
   - `icon`
   - `prefix`
   - `sort_order`
9. Use per-target rule metadata during feed rendering instead of relying only on source-level decoration fields.
10. Clean up dependent assignments when a Google target is deleted.

## Current Scope

### Upstream sources

Do now:

1. Treat upstream sources as ICS feeds only.
2. Store one real upstream calendar per `sources` row.
3. Keep source ingest behavior and source identity on `sources`.

Not in scope now:

1. Google upstream source ingest.

### Output surfaces

Do now:

1. Keep public compatibility ICS outputs:
   - `family.ics` / `family.isc`
   - `grayson.ics` / `grayson.isc`
   - `naomi.ics` / `naomi.isc`
2. Keep Google outputs modeled as `google_targets`.
3. Assign one source to multiple outputs and one output to multiple sources.

## Current Data Model

### `sources`

Use for:

1. upstream source identity
2. ingest URL
3. ownership/category metadata
4. active state and ingest settings

### `source_target_links`

Use for:

1. source-to-output assignment
2. per-target `icon`
3. per-target `prefix`
4. target type (`ics` or `google`)
5. target ordering

### `output_rules`

Use for:

1. derived inclusion state per event instance and target
2. feed generation and downstream publish preparation

## Admin Flow

Do now in this order:

1. Authenticate through Cloudflare Access.
2. Create Google targets.
3. Add ICS sources.
4. Assign each source to one or more ICS and/or Google targets.
5. Configure icon/prefix per selected target.
6. Rebuild and review/debug output.

## Auth Model

The admin surface uses Cloudflare Access with Google as the identity provider.

Do now:

1. Keep admin shell on `/admin`.
2. Keep admin APIs on `/api/*`.
3. Validate `Cf-Access-Jwt-Assertion` in the Worker.
4. Map verified email to `admin` or `editor`.
5. Keep public ICS routes outside Access and protected only by feed token.

See [Google auth decision](/c:/Dev/family-scheduling/docs/google-auth-decision.md).

## Rendering Contract

The legacy feed rendering contract remains required.

Do now:

1. Child feeds (`naomi.ics`, `grayson.ics`) render icon + title.
2. `family.ics` renders prefix + icon + title.
3. Per-target rule data must drive icon/prefix behavior.
4. Fallback sport-icon detection remains available when no explicit icon is stored on the target rule.

## Action Items

### Closed in this pass

1. Restrict the admin shell to `/admin` and remove direct shell serving from `/`.
2. Move source-to-output decoration behavior to `source_target_links`.
3. Cascade Google target deletion into assignment cleanup.
4. Add test coverage for `source_target_links` behavior and route handling.
5. Align the docs with the shipped scope: ICS upstream sources now, Google upstream sources deferred.

### Remaining

1. Implement Google Calendar outbound sync.
2. Harden recurrence exception/remap behavior.
3. Continue operational prune validation beyond scaffolding.
4. Improve parent-facing edit workflows for existing sources and target rules.

## Legacy Reference

Use `C:\Dev\ics-merge` only for:

1. current production behavior reference
2. feed contract comparison
3. migration context

Do not implement new scheduling-platform features there.
