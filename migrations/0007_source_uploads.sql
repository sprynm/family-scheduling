ALTER TABLE sources ADD COLUMN upload_revision INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS source_uploads (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  blob_ref TEXT NOT NULL UNIQUE,
  file_name TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  uploaded_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  applied_at TEXT,
  last_error TEXT,
  UNIQUE (source_id, revision),
  FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE INDEX IF NOT EXISTS source_uploads_source_status_revision_idx
  ON source_uploads(source_id, status, revision);

CREATE TABLE IF NOT EXISTS source_ingest_locks (
  source_id TEXT PRIMARY KEY,
  lock_token TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  FOREIGN KEY (source_id) REFERENCES sources(id)
);
