ALTER TABLE sources ADD COLUMN uid_fallback_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sources ADD COLUMN uid_fallback_checked_at TEXT;
ALTER TABLE sources ADD COLUMN uid_fallback_report_json TEXT;
