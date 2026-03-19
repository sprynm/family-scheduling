INSERT INTO output_targets (
  id,
  target_type,
  slug,
  display_name,
  calendar_id,
  ownership_mode,
  is_system,
  is_active,
  created_at,
  updated_at
)
SELECT
  COALESCE(id, 'outt_' || lower(hex(randomblob(12)))),
  'google',
  target_key,
  replace(replace(target_key, '_', ' '), '-', ' '),
  calendar_id,
  ownership_mode,
  0,
  COALESCE(is_active, 1),
  datetime('now'),
  datetime('now')
FROM google_targets
WHERE NOT EXISTS (
  SELECT 1
  FROM output_targets
  WHERE output_targets.slug = google_targets.target_key
);

UPDATE source_target_links
SET target_id = (
  SELECT output_targets.id
  FROM output_targets
  WHERE output_targets.slug = source_target_links.target_key
)
WHERE target_id IS NULL;

UPDATE output_rules
SET target_id = (
  SELECT output_targets.id
  FROM output_targets
  WHERE output_targets.slug = output_rules.target_key
)
WHERE target_id IS NULL;

UPDATE google_event_links
SET target_id = (
  SELECT output_targets.id
  FROM output_targets
  WHERE output_targets.slug = google_event_links.target_key
)
WHERE target_id IS NULL;

DROP TABLE IF EXISTS google_targets;
