# Offline Actions

## Production Deployment Runbook

Run this document top-to-bottom. Do not reorder steps.

## 0. Prerequisites

You must have:

1. Cloudflare account access for `ics.sprynewmedia.com` and Zero Trust.
2. Google Cloud Console access to create OAuth credentials.
3. Local terminal access to `C:\Dev\family-scheduling`.
4. Wrangler authenticated to the correct Cloudflare account.

Command:

```powershell
npx wrangler whoami
```

Expected result: correct account shown.

## 1. Lock route model

Use this exact route model:

1. Public feeds:
`https://ics.sprynewmedia.com/{family|grayson|naomi}.{ics|isc}`
2. Protected admin app:
`https://ics.sprynewmedia.com/admin/*`
3. Protected admin API:
`https://ics.sprynewmedia.com/api/*`

Constraint:
Cloudflare Access protection applies only to `/admin/*` and `/api/*`.

## 2. Create Google OAuth credentials

In Google Cloud Console:

1. Open `APIs & Services` -> `OAuth consent screen`.
2. Set user type to `External`.
3. Complete required consent fields and save.
4. Open `APIs & Services` -> `Credentials`.
5. Click `Create credentials` -> `OAuth client ID`.
6. Select application type `Web application`.
7. Name it `cloudflare-access-family-scheduling`.
8. Add authorized redirect URI provided by Cloudflare Access Google IdP setup screen.
9. Save.

Record:

1. Google Client ID
2. Google Client Secret

Expected result: OAuth client appears under `OAuth 2.0 Client IDs` with no warning icon.

## 3. Configure Google IdP in Cloudflare Zero Trust

In Cloudflare Zero Trust dashboard:

1. Open `Settings` -> `Authentication`.
2. Open the `Login methods` or `Identity providers` section.
3. Add provider `Google`.
4. Paste Google Client ID and Client Secret from Step 2.
5. Save.
6. Run built-in test login flow.

Expected result: successful Google login for an approved account.

## 4. Create Access app for admin routes

In Cloudflare Zero Trust:

1. Open `Access` -> `Applications`.
2. Create `Self-hosted` application.
3. App domain: `ics.sprynewmedia.com`.
4. Add path include rule: `/admin/*`.
5. Add path include rule: `/api/*`.
6. Add allow policy with only approved admin/editor emails.
7. Save.
8. Open app details and copy `AUD` value.

Record:

1. `CLOUDFLARE_ACCESS_AUD`
2. `CLOUDFLARE_ACCESS_TEAM_DOMAIN` in form `https://<team>.cloudflareaccess.com`

Expected result: Access app active and route scope limited to `/admin/*` and `/api/*`.

## 5. Set production Worker secrets

From `C:\Dev\family-scheduling` run:

```powershell
npx wrangler secret put TOKEN
npx wrangler secret put CLOUDFLARE_ACCESS_AUD
npx wrangler secret put CLOUDFLARE_ACCESS_TEAM_DOMAIN
npx wrangler secret put ADMIN_EMAILS
npx wrangler secret put EDITOR_EMAILS
```

Input format requirements:

1. `ADMIN_EMAILS`: comma-separated emails.
2. `EDITOR_EMAILS`: comma-separated emails.

Expected result: each command returns success.

## 6. Confirm runtime vars

Confirm `wrangler.jsonc` contains exactly:

1. `CALENDAR_NAME_FAMILY=Family Combined`
2. `CALENDAR_NAME_GRAYSON=Grayson Combined`
3. `CALENDAR_NAME_NAOMI=Naomi Combined`
4. `DEFAULT_LOOKBACK_DAYS=7`
5. `RECURRENCE_HORIZON_DAYS=180`
6. `PRUNE_AFTER_DAYS=30`
7. `ALLOW_DEV_ROLE_HEADER=false`
8. `SEED_SAMPLE_DATA=false`

Expected result: file matches values above before deploy.

## 7. Deploy

From `C:\Dev\family-scheduling` run:

```powershell
npm test -- --run
npx wrangler deploy
```

Stop condition:
If tests fail or deploy fails, stop and fix before proceeding.

Expected result: tests pass and Wrangler reports successful deployment.

## 8. Verify public feed contract

Replace `<TOKEN>` and run:

```powershell
curl -i "https://ics.sprynewmedia.com/family.isc?token=<TOKEN>"
curl -i "https://ics.sprynewmedia.com/grayson.ics?token=<TOKEN>&lookback=7&name=Grayson%20Combined"
curl -i "https://ics.sprynewmedia.com/naomi.ics?token=<TOKEN>&lookback=7&name=Naomi%20Combined"
```

Required checks for all 3 requests:

1. HTTP status `200`.
2. `content-type` contains `text/calendar`.
3. body contains `BEGIN:VCALENDAR`.
4. request is not Access-challenged.

## 9. Verify admin protection

Browser test sequence:

1. Open private/incognito window.
2. Go to `https://ics.sprynewmedia.com/admin/`.
3. Confirm Access login challenge appears.
4. Sign in with approved account and confirm app loads.
5. Sign out.
6. Repeat with non-approved account and confirm access denied.

API test:

```powershell
curl -i "https://ics.sprynewmedia.com/api/events"
```

Expected result: unauthenticated request is blocked by Access.

## 10. Release gate (all must pass)

1. Public feed routes work with token and are not Access-challenged.
2. `/admin/*` and `/api/*` are Access-protected.
3. Worker uses verified Access JWT (`Cf-Access-Jwt-Assertion`) for role mapping.
4. `ALLOW_DEV_ROLE_HEADER=false` in deployed environment.
5. `ADMIN_EMAILS` and `EDITOR_EMAILS` contain only approved identities.

Deployment is complete only when all 5 checks pass.
