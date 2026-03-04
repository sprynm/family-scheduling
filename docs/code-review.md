# Code Review Status

_Updated: 2026-03-04_

## Resolved

- Auth header guard: `x-user-role` only works when `ALLOW_DEV_ROLE_HEADER=true`.
- Queue consumer and cron wiring exist.
- Ingest exists and is rollback-safer than the original destructive refresh path.
- `lookback` is passed through to feed generation.
- Sample seeding is gated behind `SEED_SAMPLE_DATA=true`.
- Feed rendering matches the legacy contract:
  - child feeds render icon + title
  - `family.ics` renders prefix + icon + title
- Source model is refactored to one real upstream calendar per `sources` row.
- Source CRUD exists:
  - `POST /api/sources`
  - `PATCH /api/sources/:id`
  - `DELETE /api/sources/:id`
- Source config changes propagate into derived output state.
- Override records now store the verified actor role rather than the raw header value.
- Disabling a source now also marks `source_events.is_deleted_upstream = 1`.

## Still Open

### Production admin auth

Admin routes deny by default, which is safe, but production admin use is still blocked.

Settled direction:

- use Cloudflare Access
- use Google as the identity provider
- validate Access identity in the Worker
- map verified email to `admin` / `editor`

### Google Calendar outbound sync

Schema exists, but no Google Calendar write path is implemented yet.

### Prune execution

Cron and queue scaffolding exist, but retention deletes are not implemented yet.

### Recurrence exceptions and remap policy

Recurring-event exception handling and ambiguous series rewrite behavior are still the weakest part of the design/runtime.

### Test depth

Tests now cover:

- auth guard behavior
- source CRUD
- feed rendering contract
- per-source ingest shape

They still do not prove full D1 correctness or robust recurrence behavior.

## Recommended Next Step

1. Implement Cloudflare Access-backed Google auth.
2. Then choose between Google outbound sync and prune execution.
3. After that, harden recurrence exceptions/remap behavior.

## Claude Review Brief

Use this as review context for the current refactor.

### Architectural shift

The app no longer treats one `sources` row as a bucket of URLs.

It now treats one `sources` row as one actual calendar source with:

- URL
- owner/audience
- category
- icon
- prefix
- output inclusion flags
- display metadata

### Main code changes

#### `src/lib/repository.js`

- source CRUD added
- per-source URL ingest added
- legacy env buckets only remain as bootstrap/migration input
- source config changes re-derive output rules
- ingest safety preserved

#### `src/index.js`

- real source-management API routes added
- bad input returns `400`

#### `src/lib/presentation.js`

- output-specific decoration logic extracted

#### `migrations/0001_init.sql`

- source metadata expanded
- canonical event decoration fields added

### Preserved behavior

- public feed contracts remain `family`, `grayson`, `naomi`
- child feeds stay icon-only
- `family.ics` adds child prefix decoration
- failed ingest should not wipe previously published state

### Remaining review focus

- Access JWT validation and role mapping implementation
- recurrence exception handling
- whether some source config changes should trigger re-ingest instead of only output re-derivation
- real D1/integration coverage beyond the fake DB
