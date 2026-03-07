CREATE TABLE IF NOT EXISTS source_target_links (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  target_key TEXT NOT NULL,
  target_type TEXT NOT NULL,
  icon TEXT NOT NULL DEFAULT '',
  prefix TEXT NOT NULL DEFAULT '',
  sort_order INTEGER NOT NULL DEFAULT 0,
  is_enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS source_target_links_source_target_idx
  ON source_target_links(source_id, target_key);
