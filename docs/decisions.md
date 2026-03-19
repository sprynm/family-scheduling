# Project Decisions

> Durable project decisions and their rationale.

## 2026-03-11 - Drop stale historical single events at ingest

Decision:
Do not write one-off ICS events into D1 when their effective end time is more than 30 days in the past. Keep the retention window configurable via `INGEST_PAST_RETENTION_DAYS`, defaulting to `30`.

Rationale:
Large upstream feeds can include deep history that is operationally meaningless for this product but still creates canonical rows, instances, output rules, and rebuild cost. The app already limits generated ICS and Google sync to recent windows; ingest should not retain clearly stale single events that will never be surfaced again.

Consequences:
Historical single events older than the configured retention window are ignored during ingest and do not contribute to event counts, output rules, or Google sync work. Recurring events are still evaluated through recurrence expansion, and recurrence series with no retained instances are skipped entirely.

Status:
active

Revisit When:
If operators need deeper in-app historical search/audit than the current snapshot retention and output windows provide.

## 2026-03-11 - Make source sync metrics explicitly Google-only

Decision:
Admin source-row sync metrics must describe Google outbound sync only, and sources without Google targets must display `Google sync: n/a` rather than `Sync'd: 0`.

Rationale:
The previous label implied that no events were active or imported when the underlying metric was actually counting only rows synced to Google-managed outputs. Family-only or ICS-only sources could therefore show `0 sync'd` despite having valid upcoming events.

Consequences:
Operator status is less misleading during source review. The dashboard now separates feed event counts from Google-specific output sync state and avoids false alarms on sources that intentionally have no Google targets.

Status:
active

Revisit When:
If the admin UI gains a deeper source detail view with separate ICS, ingest, and Google pipeline metrics.
