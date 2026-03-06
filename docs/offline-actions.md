# Offline Actions

## Google Auth Setup

This project will use **Cloudflare Access** for the admin surface, with **Google** as the identity provider.

The public feed host remains outside Access.

Recommended route split on one host:

- `ics.sprynewmedia.com`
  - public `.ics` / `.isc` feeds
  - existing token contract remains
  - admin shell under `/admin`
  - admin APIs under `/api/*`
  - `/admin/*` and `/api/*` protected by Cloudflare Access

## Decision About Your Google Accounts
Answer: a personal Gmail account will be fine. 

Recommended approach:

- use the standard **Google** identity provider path in Cloudflare Access
- use **email-based role mapping** in the Worker
- configure the Google OAuth consent/app setup so both account types can authenticate if you want both to be usable

Why:

- this keeps the auth model simple
- it avoids needing Workspace-group integration for v1
- it allows exact-email role control in the app

Recommended v1 role model:

- `ADMIN_EMAILS`
- `EDITOR_EMAILS`

as verified email allowlists in the Worker configuration.

## What You Need To Configure

### 1. Confirm admin route scope

Choose the protected route scopes on the existing host.

Example:

- `https://ics.sprynewmedia.com/admin/*`
- `https://ics.sprynewmedia.com/api/*`

Only these route scopes should go behind Cloudflare Access.

### 2. Create the Google OAuth app

In Google Cloud Console:

- create or select a project
- open `APIs & Services` -> `Credentials`
- configure the OAuth consent screen
- use **External** if you want both personal Gmail and Workspace accounts to log in
- create the OAuth client that Cloudflare Access will use

Output from this step:

- Google client ID
- Google client secret

### 3. Add Google as an identity provider in Cloudflare Access

In Cloudflare Zero Trust:

- add a new identity provider
- choose Google
- paste the client ID and client secret from Google Cloud
- save and test the provider

### 4. Create the Access application for the admin routes

In Cloudflare Access:

- create a self-hosted application
- set policy/application scope to the admin routes
  - `/admin/*`
  - `/api/*`
- create allow policies for the people who should be able to reach the admin app

At minimum, allow the verified Google accounts you want to use.

### 5. Copy the Access application audience

Each Access application has an audience value used for JWT validation.

You will need to copy the **AUD** tag from the Access application details.

Output from this step:

- `CLOUDFLARE_ACCESS_AUD`

### 6. Identify your Cloudflare Access team domain

You will need the Access team domain used for JWT verification.

Format:

- `https://<team>.cloudflareaccess.com`

Output from this step:

- `CLOUDFLARE_ACCESS_TEAM_DOMAIN`

### 7. Decide the admin/editor email lists

Decide which verified Google identities should map to which app roles.

Output from this step:

- `ADMIN_EMAILS`
- `EDITOR_EMAILS`

Suggested format:

- comma-separated email list

### 8. Keep local dev auth separate

Local development should continue using:

- `ALLOW_DEV_ROLE_HEADER=true`

Deployed environments should use:

- `ALLOW_DEV_ROLE_HEADER=false`

Production admin access should come only from verified Cloudflare Access identity.

## Worker Configuration Needed Later

Once the code is ready, configure these values for the Worker:

- `CLOUDFLARE_ACCESS_TEAM_DOMAIN`
- `CLOUDFLARE_ACCESS_AUD`
- `ADMIN_EMAILS`
- `EDITOR_EMAILS`

Keep or continue using:

- `TOKEN`
- calendar-name variables
- source/feed configuration variables as needed

## Notes

- Personal Gmail plus Workspace is acceptable with this model.
- The app should map exact verified emails to roles rather than depending on Workspace-specific group integration in v1.
- If you later want Workspace-group-based authorization, that can be added as a later enhancement.
