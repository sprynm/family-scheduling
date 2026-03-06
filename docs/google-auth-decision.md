# Google Auth Decision

## Decision

Use **Cloudflare Access** in front of the admin surface, with **Google** configured as the identity provider.

Do **not** build a custom Google OAuth session system inside the Worker for v1.

## Why

This project already runs on Cloudflare Workers and needs only a small authenticated admin/editor surface.

Cloudflare Access already provides:

- Google login integration
- session handling and cookies
- policy enforcement in front of the Worker
- MFA and Zero Trust policy options
- authenticated user identity passed to the application

Building Google OAuth directly in the Worker would require the app to own:

- OAuth redirect flow
- CSRF and state handling
- session cookies
- token exchange
- ID token validation
- JWKS rotation and caching
- logout/session invalidation behavior

That is unnecessary complexity for this project.

## Public vs protected surfaces

The public ICS feeds must remain public except for their existing token contract.

That means:

- `family.ics` / `family.isc`
- `grayson.ics` / `grayson.isc`
- `naomi.ics` / `naomi.isc`

must **not** be placed behind Cloudflare Access.

Only the admin surface should be protected by Access.

## Recommended deployment model

Use one hostname with route-based separation:

- `ics.sprynewmedia.com`
  - public compatibility feeds stay token-protected
  - admin shell lives under `/admin`
  - admin APIs live under `/api/*`
  - Cloudflare Access protects only `/admin/*` and `/api/*`

This keeps feed URLs stable while still enforcing strong auth on the management surface.

## Authentication flow

1. User visits an Access-protected admin route (`/admin/*` or `/api/*`).
2. Cloudflare Access challenges unauthenticated users.
3. Access redirects to Google login.
4. Google authenticates the user.
5. Access issues the `CF_Authorization` application token.
6. Access forwards the request to the Worker.
7. The Worker validates the Access JWT and extracts the authenticated email.
8. The Worker maps that identity to a project role.

## Role model

Access answers the question: "Is this a real authenticated user allowed to reach the admin app?"

The Worker still answers the question: "What role does this authenticated user have inside this application?"

Use app-level role mapping:

- `admin`
- `editor`

Initial v1 role source:

- `ADMIN_EMAILS`
- `EDITOR_EMAILS`

stored as secrets or environment variables.

Later this can move into D1-backed user/role management if needed.

## Verification model

The Worker should validate the Access JWT presented in `Cf-Access-Jwt-Assertion`.

Do not trust only raw forwarded headers.

The Worker should verify:

- JWT signature
- expected issuer
- expected Access application audience
- expiry / not-before
- presence of authenticated email

Then it should derive the role from the verified identity.

### JWT key source

The Access JWT signature should be verified against the Cloudflare Access JWKS published at:

- `https://<team>.cloudflareaccess.com/cdn-cgi/access/certs`

Implementation expectation:

- fetch the JWKS from the Access certs endpoint
- cache the parsed keys so every request does not trigger a network fetch
- prefer in-memory per Worker instance and/or Cache API depending on runtime behavior
- refresh cached keys when a matching `kid` is missing or verification fails due to key rotation

## Local development model

Local development keeps the current `ALLOW_DEV_ROLE_HEADER=true` path.

That means:

- local dev can still use `x-user-role`
- deployed environments should disable that path
- production auth should come only from verified Access identity

## Required configuration

### Google side

- create OAuth client for Cloudflare Access
- configure the Access callback URL
- use Google as the upstream identity provider

### Cloudflare side

- configure Google as an Access identity provider
- create an Access application policy for `/admin/*` and `/api/*`
- define allow policies for the intended users
- capture the Access app audience for Worker-side JWT validation

### Worker side

- add verified Access JWT parsing/validation
- add role mapping from verified email to `admin` / `editor`
- leave public ICS routes on token auth only

## Non-goals

- no Google Calendar API auth here
- no bidirectional user-calendar auth
- no end-user sign-in for public feed consumption
- no custom Google OAuth session implementation in the Worker for v1

## Result

This gives the project:

- a small and secure admin auth surface
- clear separation between public calendar feeds and private admin APIs
- less custom security code in the Worker
- a cleaner path to production than hand-rolling Google OAuth
