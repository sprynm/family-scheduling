ALTER TABLE source_snapshots ADD COLUMN content_fingerprint TEXT;

CREATE INDEX IF NOT EXISTS source_snapshots_source_fetched_idx
  ON source_snapshots(source_id, fetched_at);
