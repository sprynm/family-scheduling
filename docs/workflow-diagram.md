# System Workflow Diagram

## Core Pipeline

```mermaid
flowchart LR
    subgraph Triggers
        CRON[Cron schedule]
        QUEUE[Queue consumer]
    end

    subgraph ExternalSources["External calendar sources"]
        EXT1[ICS and Google source feeds]
    end

    subgraph Ingest
        FETCH[Fetch active sources]
        NORM[Normalize to canonical events and instances]
        DERIVE[Derive output rules from routing and overlays]
    end

    subgraph D1["D1 data model"]
        SRC[(sources)]
        OUT[(output_calendars)]
        JOIN[(source_output_rules)]
        CE[(canonical_events)]
        EI[(event_instances)]
        OV[(event_overrides)]
        OR[(output_rules)]
    end

    subgraph Outputs
        FEEDROUTE[Public feed routes family grayson naomi]
        ICS[ICS outputs .ics and .isc]
        GCSYNC[Google sync worker]
        GCAPI[Google Calendar API]
        GCCALS[Google managed calendars]
    end

    subgraph Consumers
        HA[Home Assistant]
        GP[Grandparents]
        GCV[Google Calendar viewers]
    end

    CRON --> QUEUE
    QUEUE --> FETCH
    EXT1 --> FETCH

    SRC --> FETCH
    FETCH --> NORM
    NORM --> CE
    NORM --> EI

    OUT --> DERIVE
    JOIN --> DERIVE
    CE --> DERIVE
    EI --> DERIVE
    OV --> DERIVE
    DERIVE --> OR

    OR --> FEEDROUTE
    FEEDROUTE --> ICS
    ICS --> HA
    ICS --> GP

    OR --> GCSYNC
    GCSYNC --> GCAPI
    GCAPI --> GCCALS
    GCCALS --> GCV
```

## Admin Auth Flow

```mermaid
flowchart LR
    USER[Admin user] --> ACCESS[Cloudflare Access policy]
    ACCESS --> GOOGLE[Google identity provider]
    GOOGLE --> ACCESS
    ACCESS --> ROUTES[Protected routes /admin/* and /api/*]
    ACCESS --> JWT[CF_Authorization token]

    JWT --> VERIFY[Worker validates Cf-Access-Jwt-Assertion]
    JWKS[Access JWKS certs endpoint] --> CACHE[JWKS cache]
    CACHE --> VERIFY
    VERIFY --> ROLE[Map verified email to admin or editor]

    ROLE --> WEBAPP[Web app /admin]
    ROLE --> API[Admin API /api/*]
    WEBAPP --> API

    API --> OUT[(output_calendars)]
    API --> SRC[(sources)]
    API --> JOIN[(source_output_rules)]
    API --> OV[(event_overrides)]
    API --> OR[(output_rules)]
```

## Admin Flow

1. Create output calendars (`naomi`, `grayson`, `family`, plus managed Google outputs as needed).
2. Add source calendars (ICS or Google).
3. Connect each source to one or more outputs using `source_output_rules` with per-relationship rules (`prefix`, `icon`, `is_enabled`, and optional rendering controls).

## Routing Model

| Table | Responsibility |
|---|---|
| `sources` | One row per upstream feed/calendar |
| `output_calendars` | One row per output surface |
| `source_output_rules` | Many-to-many join + per-relationship rendering/routing rules |

## Decoration Rules

| Output | Icon | Child prefix (e.g. `N:`) |
|---|---|---|
| `naomi.ics` | ✅ | ❌ |
| `grayson.ics` | ✅ | ❌ |
| `family.ics` | ✅ | ✅ |
| Google output calendars | per-output rule | per-output rule |

Icon precedence:

1. explicit icon on `source_output_rules`
2. explicit icon on `sources`
3. fallback sport detection regex on event summary/title (legacy parity behavior for feeds like school calendars without dedicated ICS streams)

## Auth Boundary

| Surface | Auth |
|---|---|
| Public ICS feeds (`.ics` + `.isc`) | `?token=` required; supports `?lookback=` and `?name=` |
| Admin API + Web App (`/api/*`, `/admin/*`) | Cloudflare Access challenge + Google login + Worker JWT validation + role mapping |
| Access JWT verification | Validate `Cf-Access-Jwt-Assertion` using Access JWKS with cache + refresh on `kid` miss |
| Local dev | `ALLOW_DEV_ROLE_HEADER=true` |

## Status Notes

- Google outbound sync path is shown as target architecture; full production write-sync implementation is still tracked as open work.
- Cron plus queue nodes are intentionally distinct because retention/pruning and retry behavior run through queue execution.
