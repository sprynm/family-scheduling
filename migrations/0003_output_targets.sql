CREATE TABLE IF NOT EXISTS output_targets (
  id TEXT PRIMARY KEY,
  target_type TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  calendar_id TEXT,
  ownership_mode TEXT,
  is_system INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

ALTER TABLE source_target_links ADD COLUMN target_id TEXT REFERENCES output_targets(id);
ALTER TABLE output_rules ADD COLUMN target_id TEXT REFERENCES output_targets(id);
ALTER TABLE google_event_links ADD COLUMN target_id TEXT REFERENCES output_targets(id);

CREATE UNIQUE INDEX IF NOT EXISTS source_target_links_source_target_id_idx
  ON source_target_links(source_id, target_id);

CREATE INDEX IF NOT EXISTS output_rules_target_id_idx
  ON output_rules(target_id);

CREATE INDEX IF NOT EXISTS google_event_links_target_id_idx
  ON google_event_links(target_id);
