# Legacy Feed Contract

These URLs are migration-critical and must remain valid at cutover:

- `https://ics.sprynewmedia.com/family.isc?token=...`
- `https://ics.sprynewmedia.com/grayson.isc?token=...`
- `https://ics.sprynewmedia.com/naomi.isc?token=...`

Equivalent `.ics` aliases are also supported by this project:

- `/family.ics`
- `/grayson.ics`
- `/naomi.ics`

Compatibility requirements:

- `token` remains required
- `lookback` is accepted
- `name` is accepted
- feeds are generated from database state
- public feed requests do not fetch upstream sources live

Authoritative historical references:

- `C:\Dev\ics-merge\cals.txt`
- `C:\Dev\ics-merge\wrangler.jsonc`
