CREATE TABLE IF NOT EXISTS sources (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  provider_type TEXT NOT NULL DEFAULT 'ics',
  owner_type TEXT NOT NULL,
  fetch_url_secret_ref TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  poll_interval_minutes INTEGER NOT NULL DEFAULT 30,
  quality_profile TEXT NOT NULL DEFAULT 'standard',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_snapshots (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  http_status INTEGER,
  etag TEXT,
  last_modified TEXT,
  payload_blob_ref TEXT,
  parse_status TEXT NOT NULL,
  parse_error_summary TEXT,
  FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE IF NOT EXISTS source_events (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  provider_uid TEXT,
  provider_recurrence_id TEXT,
  provider_etag TEXT,
  raw_hash TEXT NOT NULL,
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  is_deleted_upstream INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE IF NOT EXISTS canonical_events (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  identity_key TEXT NOT NULL,
  event_kind TEXT NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  location TEXT,
  start_at TEXT NOT NULL,
  end_at TEXT NOT NULL,
  timezone TEXT,
  status TEXT NOT NULL DEFAULT 'confirmed',
  rrule TEXT,
  series_until TEXT,
  source_deleted INTEGER NOT NULL DEFAULT 0,
  needs_review INTEGER NOT NULL DEFAULT 0,
  source_changed_since_overlay INTEGER NOT NULL DEFAULT 0,
  last_source_change_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS canonical_events_identity_idx
  ON canonical_events(identity_key);

CREATE TABLE IF NOT EXISTS event_instances (
  id TEXT PRIMARY KEY,
  canonical_event_id TEXT NOT NULL,
  occurrence_start_at TEXT NOT NULL,
  occurrence_end_at TEXT NOT NULL,
  recurrence_instance_key TEXT NOT NULL,
  provider_recurrence_id TEXT,
  status TEXT NOT NULL DEFAULT 'confirmed',
  source_deleted INTEGER NOT NULL DEFAULT 0,
  needs_review INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (canonical_event_id) REFERENCES canonical_events(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS event_instances_recurrence_key_idx
  ON event_instances(canonical_event_id, recurrence_instance_key);

CREATE TABLE IF NOT EXISTS event_overrides (
  id TEXT PRIMARY KEY,
  scope_type TEXT NOT NULL,
  canonical_event_id TEXT,
  event_instance_id TEXT,
  override_type TEXT NOT NULL,
  payload_json TEXT NOT NULL DEFAULT '{}',
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  cleared_at TEXT,
  FOREIGN KEY (canonical_event_id) REFERENCES canonical_events(id),
  FOREIGN KEY (event_instance_id) REFERENCES event_instances(id)
);

CREATE TABLE IF NOT EXISTS output_rules (
  id TEXT PRIMARY KEY,
  canonical_event_id TEXT NOT NULL,
  event_instance_id TEXT,
  target_key TEXT NOT NULL,
  include_state TEXT NOT NULL,
  derived_reason TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (canonical_event_id) REFERENCES canonical_events(id),
  FOREIGN KEY (event_instance_id) REFERENCES event_instances(id)
);

CREATE TABLE IF NOT EXISTS google_targets (
  id TEXT PRIMARY KEY,
  target_key TEXT NOT NULL UNIQUE,
  calendar_id TEXT NOT NULL,
  ownership_mode TEXT NOT NULL DEFAULT 'managed_output',
  is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS google_event_links (
  id TEXT PRIMARY KEY,
  target_key TEXT NOT NULL,
  canonical_event_id TEXT,
  event_instance_id TEXT,
  google_event_id TEXT NOT NULL,
  google_etag TEXT,
  last_synced_hash TEXT,
  last_synced_at TEXT,
  sync_status TEXT NOT NULL DEFAULT 'pending',
  last_error TEXT,
  FOREIGN KEY (canonical_event_id) REFERENCES canonical_events(id),
  FOREIGN KEY (event_instance_id) REFERENCES event_instances(id)
);

CREATE TABLE IF NOT EXISTS sync_jobs (
  id TEXT PRIMARY KEY,
  job_type TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  scope_id TEXT,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  summary_json TEXT,
  error_json TEXT
);
