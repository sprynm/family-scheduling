CREATE TABLE google_event_links_v2 (
  id TEXT PRIMARY KEY,
  target_key TEXT NOT NULL,
  canonical_event_id TEXT,
  event_instance_id TEXT,
  google_event_id TEXT,
  google_etag TEXT,
  last_synced_hash TEXT,
  last_synced_at TEXT,
  sync_status TEXT NOT NULL DEFAULT 'pending',
  last_error TEXT,
  target_id TEXT REFERENCES output_targets(id),
  FOREIGN KEY (canonical_event_id) REFERENCES canonical_events(id),
  FOREIGN KEY (event_instance_id) REFERENCES event_instances(id)
);

INSERT INTO google_event_links_v2 (
  id,
  target_key,
  canonical_event_id,
  event_instance_id,
  google_event_id,
  google_etag,
  last_synced_hash,
  last_synced_at,
  sync_status,
  last_error,
  target_id
)
SELECT
  id,
  target_key,
  canonical_event_id,
  event_instance_id,
  google_event_id,
  google_etag,
  last_synced_hash,
  last_synced_at,
  sync_status,
  last_error,
  target_id
FROM google_event_links;

DROP TABLE google_event_links;

ALTER TABLE google_event_links_v2 RENAME TO google_event_links;

CREATE INDEX IF NOT EXISTS google_event_links_target_id_idx
  ON google_event_links(target_id);
