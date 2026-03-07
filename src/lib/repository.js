import crypto from 'node:crypto';
import { TARGETS, TARGET_LABELS } from './constants.js';
import { buildICS, expandRecurringEvent, parseICS } from './ics.js';
import { decorateEventSummary } from './presentation.js';

function nowIso() {
  return new Date().toISOString();
}

function makeOpaqueId(prefix, value = '') {
  const seed = `${prefix}:${value}:${Math.random()}:${Date.now()}`;
  return `${prefix}_${crypto.createHash('sha1').update(seed).digest('hex').slice(0, 16)}`;
}

function hashValue(value) {
  return crypto.createHash('sha1').update(String(value)).digest('hex');
}

function stableId(prefix, value = '') {
  return `${prefix}_${hashValue(value).slice(0, 24)}`;
}

function mapRow(row) {
  if (!row) return null;
  return Object.fromEntries(Object.entries(row));
}

function toBoolInt(value, fallback = 0) {
  if (value === undefined || value === null || value === '') return fallback;
  return value ? 1 : 0;
}

function deriveSourceCategory(url, ownerType) {
  if (/calendar\.google\.com/i.test(url) && (ownerType === 'naomi' || ownerType === 'grayson')) {
    return 'personal';
  }
  if (/teamsnap|powerup|sport|hockey|baseball|basketball|soccer|lacrosse/i.test(url)) {
    return 'sports';
  }
  if (ownerType === 'family') return 'shared';
  return 'general';
}

function deriveDefaultGoogleInclusion(sourceCategory, ownerType) {
  if (ownerType === 'family') return 0;
  if (sourceCategory === 'personal') return 0;
  return 1;
}

function buildLegacyDisplayName(ownerType, url, index) {
  try {
    const parsed = new URL(url);
    return `${ownerType}-${parsed.hostname}-${index + 1}`;
  } catch {
    return `${ownerType}-source-${index + 1}`;
  }
}

function ensureHttpsUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const parsed = new URL(raw);
  if (!['http:', 'https:'].includes(parsed.protocol)) {
    throw new Error('Source URL must use http or https');
  }
  parsed.protocol = 'https:';
  return parsed.toString();
}

function normalizeOwnerType(value) {
  const ownerType = String(value || '').trim().toLowerCase();
  if (!TARGETS.includes(ownerType)) {
    throw new Error(`Unsupported owner_type: ${value}`);
  }
  return ownerType;
}

function parsePositiveInt(value, fallback) {
  const num = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(num) && num > 0 ? num : fallback;
}

function normalizeTargetKeys(values) {
  if (!Array.isArray(values)) return [];
  return [...new Set(values.map((v) => String(v || '').trim().toLowerCase()).filter(Boolean))];
}

function cleanRuleValue(value) {
  return String(value ?? '').trim();
}

function buildDefaultTargetRule(targetKey, ownerType, { icon = '', prefix = '' } = {}) {
  return {
    target_key: String(targetKey || '').trim().toLowerCase(),
    target_type: TARGETS.includes(targetKey) ? 'ics' : 'google',
    icon: cleanRuleValue(icon),
    prefix: cleanRuleValue(prefix),
  };
}

function normalizeTargetLinks(input, ownerType, defaults = {}) {
  const items = Array.isArray(input) ? input : [];
  const normalized = [];
  const seen = new Set();

  for (const [index, item] of items.entries()) {
    const rawTargetKey =
      typeof item === 'string' ? item : typeof item?.target_key === 'string' ? item.target_key : '';
    const targetKey = String(rawTargetKey || '').trim().toLowerCase();
    if (!targetKey || seen.has(targetKey)) continue;
    seen.add(targetKey);

    const fallback = buildDefaultTargetRule(targetKey, ownerType, defaults);
    const icon = typeof item === 'object' && item !== null && 'icon' in item ? cleanRuleValue(item.icon) : fallback.icon;
    const prefix =
      typeof item === 'object' && item !== null && 'prefix' in item
        ? cleanRuleValue(item.prefix)
        : fallback.prefix;

    normalized.push({
      target_key: targetKey,
      target_type: TARGETS.includes(targetKey) ? 'ics' : 'google',
      icon,
      prefix,
      sort_order: index,
    });
  }

  return normalized;
}

function normalizeTargetKey(value) {
  const key = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '_')
    .replace(/^_+|_+$/g, '');
  if (!key) throw new Error('target_key is required');
  return key;
}

export class D1Repository {
  constructor(db, env) {
    this.db = db;
    this.env = env;
  }

  async runInTransaction(work) {
    // TECH DEBT: D1 does not support SQL BEGIN/COMMIT. This wrapper provides
    // no atomicity — callers run as sequential independent writes. Migrate
    // each caller to db.batch(). See TECH_DEBT.md.
    return work();
  }

  async ensureSupportTables() {
    await this.db.prepare(
      `CREATE TABLE IF NOT EXISTS source_target_links (
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
      )`
    ).run();
    await this.db.prepare(
      `CREATE UNIQUE INDEX IF NOT EXISTS source_target_links_source_target_idx
       ON source_target_links(source_id, target_key)`
    ).run();

    const ensureColumn = async (column, definition) => {
      try {
        await this.db.prepare(`ALTER TABLE source_target_links ADD COLUMN ${column} ${definition}`).run();
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (!/duplicate column name|already exists/i.test(message)) throw error;
      }
    };

    await ensureColumn('icon', "TEXT NOT NULL DEFAULT ''");
    await ensureColumn('prefix', "TEXT NOT NULL DEFAULT ''");
    await ensureColumn('sort_order', 'INTEGER NOT NULL DEFAULT 0');
  }

  async listSources() {
    const result = await this.db.prepare(
      `SELECT
         s.id, s.name, s.display_name, s.owner_type, s.provider_type, s.source_category,
         s.url, s.icon, s.prefix,
         s.include_in_child_ics, s.include_in_family_ics, s.include_in_child_google_output,
         s.is_active, s.sort_order, s.poll_interval_minutes, s.quality_profile, s.updated_at,
         snap.fetched_at AS last_fetched_at,
         snap.http_status AS last_http_status,
         snap.parse_status AS last_parse_status,
         snap.parse_error_summary AS last_parse_error,
         COALESCE(ec.event_count, 0) AS event_count
       FROM sources s
       LEFT JOIN (
         SELECT source_id, MAX(fetched_at) AS max_fetched_at
         FROM source_snapshots
         GROUP BY source_id
       ) latest ON latest.source_id = s.id
       LEFT JOIN source_snapshots snap
         ON snap.source_id = s.id AND snap.fetched_at = latest.max_fetched_at
       LEFT JOIN (
         SELECT source_id, COUNT(*) AS event_count
         FROM canonical_events
         WHERE source_deleted = 0
         GROUP BY source_id
       ) ec ON ec.source_id = s.id
       ORDER BY s.owner_type, s.sort_order, COALESCE(s.display_name, s.name), s.name`
    ).all();
    const sources = result.results || [];
    const linksResult = await this.db.prepare(
      `SELECT source_id, target_key, target_type, icon, prefix, sort_order
       FROM source_target_links
       WHERE is_enabled = 1`
    ).all();
    const links = linksResult.results || [];
    const bySource = new Map();
    for (const link of links) {
      if (!bySource.has(link.source_id)) bySource.set(link.source_id, []);
      bySource.get(link.source_id).push({
        target_key: link.target_key,
        target_type: link.target_type,
        icon: cleanRuleValue(link.icon),
        prefix: cleanRuleValue(link.prefix),
        sort_order: Number(link.sort_order || 0),
      });
    }
    return sources.map((source) => ({
      ...source,
      target_links: (bySource.get(source.id) || []).sort(
        (a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0) || String(a.target_key).localeCompare(String(b.target_key))
      ),
      target_keys: (bySource.get(source.id) || []).map((link) => link.target_key),
    }));
  }

  async createSource(input) {
    const timestamp = nowIso();
    const url = ensureHttpsUrl(input.url);
    const ownerType = normalizeOwnerType(input.owner_type);
    const sourceCategory = String(input.source_category || deriveSourceCategory(url, ownerType)).trim().toLowerCase();
    const displayName = String(input.display_name || input.name || buildLegacyDisplayName(ownerType, url, 0)).trim();
    const name = String(input.name || displayName).trim();
    const sourceId = stableId('src', `${ownerType}:${url}`);
    await this.db.prepare(
      `INSERT INTO sources (
        id, name, display_name, provider_type, owner_type, source_category, url, icon, prefix, fetch_url_secret_ref,
        include_in_child_ics, include_in_family_ics, include_in_child_google_output,
        is_active, sort_order, poll_interval_minutes, quality_profile, created_at, updated_at
      ) VALUES (?, ?, ?, 'ics', ?, ?, ?, ?, ?, NULL, ?, ?, ?, 1, ?, ?, ?, ?, ?)`
    ).bind(
      sourceId,
      name,
      displayName,
      ownerType,
      sourceCategory,
      url,
      input.icon || '',
      input.prefix ?? this.derivePrefix(ownerType),
      toBoolInt(input.include_in_child_ics, 1),
      toBoolInt(input.include_in_family_ics, 1),
      toBoolInt(input.include_in_child_google_output, deriveDefaultGoogleInclusion(sourceCategory, ownerType)),
      Number(input.sort_order || 0),
      Number(input.poll_interval_minutes || 30),
      input.quality_profile || 'standard',
      timestamp,
      timestamp
    ).run();
    const targetLinksInput = Array.isArray(input.target_links) ? input.target_links : input.target_keys;
    if (Array.isArray(targetLinksInput)) {
      await this.setSourceTargets(sourceId, targetLinksInput, {
        ownerType,
        icon: input.icon || '',
        prefix: input.prefix ?? this.derivePrefix(ownerType),
      });
    }
    return this.getSourceById(sourceId);
  }

  async updateSource(sourceId, input) {
    const existing = await this.getSourceById(sourceId);
    if (!existing) return null;
    const ownerType = input.owner_type !== undefined ? normalizeOwnerType(input.owner_type) : existing.owner_type;
    const url = input.url !== undefined ? ensureHttpsUrl(input.url) : existing.url;
    const sourceCategory = String(input.source_category || deriveSourceCategory(url, ownerType)).trim().toLowerCase();
    const updated = {
      name: String(input.name ?? existing.name ?? '').trim(),
      display_name: String(input.display_name ?? existing.display_name ?? input.name ?? existing.name ?? '').trim(),
      owner_type: ownerType,
      source_category: sourceCategory,
      url,
      icon: input.icon ?? existing.icon,
      prefix: input.prefix ?? existing.prefix,
      include_in_child_ics: toBoolInt(input.include_in_child_ics, existing.include_in_child_ics),
      include_in_family_ics: toBoolInt(input.include_in_family_ics, existing.include_in_family_ics),
      include_in_child_google_output: toBoolInt(input.include_in_child_google_output, existing.include_in_child_google_output),
      is_active: toBoolInt(input.is_active, existing.is_active),
      sort_order: Number(input.sort_order ?? existing.sort_order ?? 0),
      poll_interval_minutes: Number(input.poll_interval_minutes ?? existing.poll_interval_minutes ?? 30),
      quality_profile: input.quality_profile ?? existing.quality_profile,
    };
    await this.db.prepare(
      `UPDATE sources
       SET name = ?, display_name = ?, owner_type = ?, source_category = ?, url = ?, icon = ?, prefix = ?,
           include_in_child_ics = ?, include_in_family_ics = ?, include_in_child_google_output = ?,
           is_active = ?, sort_order = ?, poll_interval_minutes = ?, quality_profile = ?, updated_at = ?
       WHERE id = ?`
    ).bind(
      updated.name,
      updated.display_name,
      updated.owner_type,
      updated.source_category,
      updated.url,
      updated.icon,
      updated.prefix,
      updated.include_in_child_ics,
      updated.include_in_family_ics,
      updated.include_in_child_google_output,
      updated.is_active,
      updated.sort_order,
      updated.poll_interval_minutes,
      updated.quality_profile,
      nowIso(),
      sourceId
    ).run();
    const targetLinksInput = Array.isArray(input.target_links) ? input.target_links : input.target_keys;
    if (Array.isArray(targetLinksInput)) {
      await this.setSourceTargets(sourceId, targetLinksInput, {
        ownerType: updated.owner_type,
        icon: input.icon ?? existing.icon ?? '',
        prefix: input.prefix ?? existing.prefix ?? this.derivePrefix(updated.owner_type),
      });
    }
    const source = await this.getSourceById(sourceId);
    await this.syncSourceConfig(source);
    return source;
  }

  async setSourceTargets(sourceId, targetLinksInput, defaults = {}) {
    const targetLinks = normalizeTargetLinks(targetLinksInput, defaults.ownerType || 'family', defaults);
    const timestamp = nowIso();
    await this.runInTransaction(async () => {
      await this.db.prepare(`DELETE FROM source_target_links WHERE source_id = ?`).bind(sourceId).run();
      for (const targetLink of targetLinks) {
        await this.db.prepare(
          `INSERT INTO source_target_links (
            id, source_id, target_key, target_type, icon, prefix, sort_order, is_enabled, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`
        ).bind(
          stableId('stl', `${sourceId}:${targetLink.target_key}`),
          sourceId,
          targetLink.target_key,
          targetLink.target_type,
          targetLink.icon || '',
          targetLink.prefix || '',
          Number(targetLink.sort_order || 0),
          timestamp,
          timestamp
        ).run();
      }
    });
  }

  async disableSource(sourceId) {
    const timestamp = nowIso();
    await this.runInTransaction(async () => {
      await this.db.prepare(`UPDATE sources SET is_active = 0, updated_at = ? WHERE id = ?`).bind(timestamp, sourceId).run();
      await this.db.prepare(`UPDATE canonical_events SET source_deleted = 1, updated_at = ? WHERE source_id = ?`).bind(timestamp, sourceId).run();
      await this.db.prepare(
        `UPDATE event_instances
         SET source_deleted = 1, updated_at = ?
         WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`
      ).bind(timestamp, sourceId).run();
      await this.db.prepare(
        `UPDATE source_events SET is_deleted_upstream = 1, last_seen_at = ? WHERE source_id = ?`
      ).bind(timestamp, sourceId).run();
      await this.db.prepare(`DELETE FROM output_rules WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`).bind(sourceId).run();
    });
    return this.getSourceById(sourceId);
  }

  async deleteSource(sourceId) {
    const existing = await this.getSourceById(sourceId);
    if (!existing) return null;
    // All deletes are independent (same sourceId / subquery) — batch for atomicity
    await this.db.batch([
      this.db.prepare(`DELETE FROM output_rules WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`).bind(sourceId),
      this.db.prepare(`DELETE FROM event_overrides WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`).bind(sourceId),
      this.db.prepare(`DELETE FROM google_event_links WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`).bind(sourceId),
      this.db.prepare(`DELETE FROM event_instances WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`).bind(sourceId),
      this.db.prepare(`DELETE FROM canonical_events WHERE source_id = ?`).bind(sourceId),
      this.db.prepare(`DELETE FROM source_events WHERE source_id = ?`).bind(sourceId),
      this.db.prepare(`DELETE FROM source_snapshots WHERE source_id = ?`).bind(sourceId),
      this.db.prepare(`DELETE FROM source_target_links WHERE source_id = ?`).bind(sourceId),
      this.db.prepare(`DELETE FROM sources WHERE id = ?`).bind(sourceId),
    ]);
    return existing;
  }

  async listJobs(limit = 50) {
    const result = await this.db.prepare(
      `SELECT id, job_type, scope_type, scope_id, status, started_at, finished_at, summary_json, error_json
       FROM sync_jobs ORDER BY started_at DESC LIMIT ?`
    ).bind(limit).all();
    return result.results || [];
  }

  async listTargets() {
    const result = await this.db.prepare(
      `SELECT target_key, calendar_id, ownership_mode, is_active FROM google_targets ORDER BY target_key`
    ).all();
    return result.results || [];
  }

  async upsertTarget(input) {
    const targetKey = normalizeTargetKey(input.target_key);
    const calendarId = String(input.calendar_id || '').trim();
    if (!calendarId) {
      throw new Error('calendar_id is required');
    }
    const ownershipMode = String(input.ownership_mode || 'managed_output').trim().toLowerCase();
    if (!['managed_output', 'external'].includes(ownershipMode)) {
      throw new Error(`Unsupported ownership_mode: ${ownershipMode}`);
    }
    const isActive = toBoolInt(input.is_active, 1);
    const id = stableId('gt', targetKey);
    await this.db.prepare(
      `INSERT INTO google_targets (id, target_key, calendar_id, ownership_mode, is_active)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(target_key) DO UPDATE SET
         calendar_id = excluded.calendar_id,
         ownership_mode = excluded.ownership_mode,
         is_active = excluded.is_active`
    ).bind(id, targetKey, calendarId, ownershipMode, isActive).run();
    const row = await this.db.prepare(
      `SELECT target_key, calendar_id, ownership_mode, is_active FROM google_targets WHERE target_key = ?`
    ).bind(targetKey).first();
    return mapRow(row);
  }

  async deleteTarget(targetKeyInput) {
    const targetKey = normalizeTargetKey(targetKeyInput);
    const existing = await this.db.prepare(
      `SELECT target_key, calendar_id, ownership_mode, is_active FROM google_targets WHERE target_key = ?`
    ).bind(targetKey).first();
    if (!existing) return null;
    await this.runInTransaction(async () => {
      await this.db.prepare(`DELETE FROM source_target_links WHERE target_key = ? AND target_type = 'google'`).bind(targetKey).run();
      await this.db.prepare(`DELETE FROM output_rules WHERE target_key = ?`).bind(targetKey).run();
      await this.db.prepare(`DELETE FROM google_event_links WHERE target_key = ?`).bind(targetKey).run();
      await this.db.prepare(`DELETE FROM google_targets WHERE target_key = ?`).bind(targetKey).run();
    });
    return mapRow(existing);
  }

  async listInstances({ sourceId, outputKey, future, limit = 200 }) {
    const now = new Date().toISOString();
    let sql = `
      SELECT
        event_instances.id,
        event_instances.occurrence_start_at,
        event_instances.occurrence_end_at,
        canonical_events.id AS canonical_event_id,
        canonical_events.title,
        sources.name AS source_name,
        sources.owner_type
      FROM event_instances
      JOIN canonical_events ON canonical_events.id = event_instances.canonical_event_id
      JOIN sources ON sources.id = canonical_events.source_id
    `;
    const binds = [];
    const conditions = [];
    if (future) conditions.push('event_instances.occurrence_start_at >= ?') && binds.push(now);
    if (sourceId) conditions.push('canonical_events.source_id = ?') && binds.push(sourceId);
    if (outputKey) {
      sql += ` JOIN output_rules ON output_rules.canonical_event_id = canonical_events.id AND output_rules.target_key = ? AND output_rules.include_state = 'included'`;
      binds.unshift(outputKey);
    }
    if (conditions.length) sql += ' WHERE ' + conditions.join(' AND ');
    sql += ' ORDER BY event_instances.occurrence_start_at ASC LIMIT ?';
    binds.push(limit);
    const result = await this.db.prepare(sql).bind(...binds).all();
    return result.results || [];
  }

  async listActiveOverrides({ now }) {
    // Show overrides whose associated event instance is in the future.
    // For instance-specific overrides: the specific instance must be future.
    // For series-level overrides (event_instance_id IS NULL): show if any
    // future instance exists for the canonical event.
    const result = await this.db.prepare(`
      SELECT
        ov.id,
        ov.override_type,
        ov.payload_json,
        ov.event_instance_id,
        ov.created_by,
        ov.created_at,
        ce.id AS canonical_event_id,
        ce.title,
        s.name AS source_name,
        s.owner_type,
        COALESCE(ei.occurrence_start_at, next_ei.min_start) AS event_date
      FROM event_overrides ov
      JOIN canonical_events ce ON ce.id = ov.canonical_event_id
      JOIN sources s ON s.id = ce.source_id
      LEFT JOIN event_instances ei
        ON ei.id = ov.event_instance_id
        AND ei.occurrence_start_at >= ?
      LEFT JOIN (
        SELECT canonical_event_id, MIN(occurrence_start_at) AS min_start
        FROM event_instances
        WHERE occurrence_start_at >= ?
        GROUP BY canonical_event_id
      ) next_ei
        ON next_ei.canonical_event_id = ce.id
        AND ov.event_instance_id IS NULL
      WHERE ov.cleared_at IS NULL
        AND COALESCE(ei.occurrence_start_at, next_ei.min_start) IS NOT NULL
      ORDER BY COALESCE(ei.occurrence_start_at, next_ei.min_start) ASC
      LIMIT 100
    `).bind(now, now).all();
    return (result.results || []).map((row) => ({
      ...row,
      payload: typeof row.payload_json === 'string' ? JSON.parse(row.payload_json || '{}') : (row.payload_json || {}),
    }));
  }

  async listEvents({ target, limit = 200 }) {
    let sql = `
      SELECT
        canonical_events.id,
        canonical_events.title,
        canonical_events.location,
        canonical_events.status,
        canonical_events.source_deleted,
        canonical_events.needs_review,
        canonical_events.updated_at,
        canonical_events.source_id,
        sources.name AS source_name,
        sources.owner_type,
        MIN(event_instances.occurrence_start_at) AS next_occurrence
      FROM canonical_events
      JOIN sources ON sources.id = canonical_events.source_id
      LEFT JOIN event_instances ON event_instances.canonical_event_id = canonical_events.id
    `;
    const binds = [];
    if (target) {
      sql += ` JOIN output_rules ON output_rules.canonical_event_id = canonical_events.id AND output_rules.target_key = ? AND output_rules.include_state = 'included'`;
      binds.push(target);
    }
    sql += `
      GROUP BY canonical_events.id
      ORDER BY next_occurrence ASC NULLS LAST, canonical_events.updated_at DESC
      LIMIT ?
    `;
    binds.push(limit);
    const result = await this.db.prepare(sql).bind(...binds).all();
    return result.results || [];
  }

  async getEvent(id) {
    const eventResult = await this.db.prepare(
      `SELECT canonical_events.*, sources.name AS source_name, sources.owner_type
       FROM canonical_events
       JOIN sources ON sources.id = canonical_events.source_id
       WHERE canonical_events.id = ?`
    ).bind(id).first();
    if (!eventResult) return null;

    const instancesResult = await this.db.prepare(
      `SELECT * FROM event_instances WHERE canonical_event_id = ? ORDER BY occurrence_start_at ASC LIMIT 200`
    ).bind(id).all();
    const overridesResult = await this.db.prepare(
      `SELECT * FROM event_overrides
       WHERE canonical_event_id = ? AND cleared_at IS NULL
       ORDER BY created_at DESC`
    ).bind(id).all();
    const outputsResult = await this.db.prepare(
      `SELECT target_key, include_state, derived_reason
       FROM output_rules WHERE canonical_event_id = ? ORDER BY target_key`
    ).bind(id).all();
    const linksResult = await this.db.prepare(
      `SELECT target_key, google_event_id, sync_status, last_synced_at, last_error
       FROM google_event_links WHERE canonical_event_id = ? ORDER BY target_key`
    ).bind(id).all();

    return {
      ...mapRow(eventResult),
      instances: instancesResult.results || [],
      overrides: overridesResult.results || [],
      outputs: outputsResult.results || [],
      googleLinks: linksResult.results || [],
    };
  }

  async createOverride({ eventId, eventInstanceId = null, overrideType, payload, actorRole }) {
    const timestamp = nowIso();
    const overrideId = makeOpaqueId('ovr', `${eventId}:${overrideType}`);
    await this.db.prepare(
      `INSERT INTO event_overrides (
        id, scope_type, canonical_event_id, event_instance_id, override_type, payload_json,
        created_by, created_at, updated_at, cleared_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`
    ).bind(
      overrideId,
      eventInstanceId ? 'instance' : 'event',
      eventId,
      eventInstanceId,
      overrideType,
      JSON.stringify(payload || {}),
      actorRole,
      timestamp,
      timestamp
    ).run();

    await this.db.prepare(
      `UPDATE canonical_events
       SET needs_review = 0, source_changed_since_overlay = 0, updated_at = ?
       WHERE id = ?`
    ).bind(timestamp, eventId).run();

    return this.getEvent(eventId);
  }

  async clearOverride(overrideId) {
    const timestamp = nowIso();
    const override = await this.db.prepare(
      `SELECT canonical_event_id FROM event_overrides WHERE id = ?`
    ).bind(overrideId).first();
    if (!override) return null;
    await this.db.prepare(
      `UPDATE event_overrides SET cleared_at = ?, updated_at = ? WHERE id = ?`
    ).bind(timestamp, timestamp, overrideId).run();
    return this.getEvent(override.canonical_event_id);
  }

  async enqueueJob({ jobType, scopeType, scopeId }) {
    const timestamp = nowIso();
    const jobId = makeOpaqueId('job', `${jobType}:${scopeType}:${scopeId || ''}`);
    await this.db.prepare(
      `INSERT INTO sync_jobs (id, job_type, scope_type, scope_id, status, started_at)
       VALUES (?, ?, ?, ?, 'queued', ?)`
    ).bind(jobId, jobType, scopeType, scopeId || null, timestamp).run();
    if (this.env.JOBS_QUEUE?.send) {
      await this.env.JOBS_QUEUE.send({
        jobId,
        jobType,
        scopeType,
        scopeId: scopeId || null,
      });
    }
    return {
      id: jobId,
      job_type: jobType,
      scope_type: scopeType,
      scope_id: scopeId || null,
      status: 'queued',
      started_at: timestamp,
    };
  }

  async getFeedContract(requestUrl) {
    const host = this.env.PUBLIC_HOST || (requestUrl ? new URL(requestUrl).origin : 'https://ics.sprynewmedia.com');
    const token = this.env.TOKEN || '';
    return {
      host,
      token,
      requiredToken: true,
      acceptedParams: ['token', 'lookback', 'name'],
      contracts: TARGETS.map((target) => ({
        target,
        url: token ? `${host}/${target}.ics?token=${encodeURIComponent(token)}` : `${host}/${target}.ics`,
      })),
    };
  }

  async generateFeed({ target, calendarName, lookbackDays = 7 }) {
    const result = await this.db.prepare(
      `SELECT
        canonical_events.id AS canonical_event_id,
        canonical_events.title,
        COALESCE(NULLIF(source_target_links.icon, ''), canonical_events.source_icon, sources.icon, '') AS source_icon,
        COALESCE(NULLIF(source_target_links.prefix, ''), canonical_events.source_prefix, sources.prefix, '') AS source_prefix,
        canonical_events.description,
        canonical_events.location,
        canonical_events.status,
        event_instances.id AS event_instance_id,
        event_instances.occurrence_start_at,
        event_instances.occurrence_end_at
      FROM output_rules
      JOIN canonical_events ON canonical_events.id = output_rules.canonical_event_id
      JOIN sources ON sources.id = canonical_events.source_id
      JOIN event_instances ON event_instances.id = output_rules.event_instance_id
      LEFT JOIN source_target_links
        ON source_target_links.source_id = canonical_events.source_id
       AND source_target_links.target_key = output_rules.target_key
       AND source_target_links.is_enabled = 1
      WHERE output_rules.target_key = ?
        AND output_rules.include_state = 'included'
        AND canonical_events.source_deleted = 0
        AND event_instances.source_deleted = 0
      ORDER BY event_instances.occurrence_start_at ASC`
    ).bind(target).all();

    const rows = result.results || [];
    const lookbackCutoff = new Date();
    lookbackCutoff.setUTCDate(lookbackCutoff.getUTCDate() - lookbackDays);
    const events = rows
      .filter((row) => new Date(row.occurrence_end_at) >= lookbackCutoff)
      .map((row) => ({
        uid: `${row.event_instance_id}@family-scheduling`,
        startAt: row.occurrence_start_at,
        endAt: row.occurrence_end_at,
        summary: decorateEventSummary({
          target,
          title: row.title,
          sourceIcon: row.source_icon,
          sourcePrefix: row.source_prefix,
        }),
        description: row.description,
        location: row.location,
        status: row.status,
      }));

    return buildICS({
      calendarName,
      description: `${TARGET_LABELS[target]} generated compatibility feed`,
      events,
    });
  }

  async bootstrapLegacySources() {
    const existing = await this.db.prepare(`SELECT COUNT(*) AS count FROM sources`).first();
    if ((existing?.count || 0) > 0) return;

    const timestamp = nowIso();
    const buckets = [
      ['grayson', 'GRAYSON_URLS'],
      ['naomi', 'NAOMI_URLS'],
      ['family', 'FAMILY_URLS'],
    ];

    for (const [ownerType, secretRef] of buckets) {
      const entries = this.parseSourceSecret(this.env[secretRef] || '');
      for (const [index, entry] of entries.entries()) {
        const sourceCategory = deriveSourceCategory(entry.url, ownerType);
        const sourceId = stableId('src', `${ownerType}:${entry.url}`);
        await this.db.prepare(
          `INSERT OR IGNORE INTO sources (
            id, name, display_name, provider_type, owner_type, source_category, url, icon, prefix, fetch_url_secret_ref,
            include_in_child_ics, include_in_family_ics, include_in_child_google_output,
            is_active, sort_order, poll_interval_minutes, quality_profile, created_at, updated_at
          ) VALUES (?, ?, ?, 'ics', ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 30, 'standard', ?, ?)`
        ).bind(
          sourceId,
          buildLegacyDisplayName(ownerType, entry.url, index),
          buildLegacyDisplayName(ownerType, entry.url, index),
          ownerType,
          sourceCategory,
          entry.url,
          entry.emoji || '',
          this.derivePrefix(ownerType),
          secretRef,
          ownerType === 'family' ? 0 : 1,
          1,
          deriveDefaultGoogleInclusion(sourceCategory, ownerType),
          index,
          timestamp,
          timestamp
        ).run();
      }
    }
  }

  async seedSampleData() {
    await this.bootstrapLegacySources();
    const existing = await this.db.prepare(`SELECT COUNT(*) AS count FROM canonical_events`).first();
    if ((existing?.count || 0) > 0) return;

    const timestamp = nowIso();
    const sampleSources = [
      {
        id: stableId('src', 'grayson:sample:hockey'),
        name: 'sample-grayson-hockey',
        display_name: 'Sample Grayson Hockey',
        owner_type: 'grayson',
        source_category: 'sports',
        url: 'https://example.com/grayson-hockey.ics',
        icon: '🏒',
        prefix: 'G:',
        include_in_child_ics: 1,
        include_in_family_ics: 1,
        include_in_child_google_output: 1,
      },
      {
        id: stableId('src', 'naomi:sample:basketball'),
        name: 'sample-naomi-basketball',
        display_name: 'Sample Naomi Basketball',
        owner_type: 'naomi',
        source_category: 'sports',
        url: 'https://example.com/naomi-basketball.ics',
        icon: '🏀',
        prefix: 'N:',
        include_in_child_ics: 1,
        include_in_family_ics: 1,
        include_in_child_google_output: 1,
      },
      {
        id: stableId('src', 'family:sample:holiday'),
        name: 'sample-family-holiday',
        display_name: 'Sample Family Holiday',
        owner_type: 'family',
        source_category: 'shared',
        url: 'https://example.com/family-holiday.ics',
        icon: '',
        prefix: '',
        include_in_child_ics: 0,
        include_in_family_ics: 1,
        include_in_child_google_output: 0,
      },
    ];
    for (const source of sampleSources) {
      await this.db.prepare(
        `INSERT OR IGNORE INTO sources (
          id, name, display_name, provider_type, owner_type, source_category, url, icon, prefix, fetch_url_secret_ref,
          include_in_child_ics, include_in_family_ics, include_in_child_google_output,
          is_active, sort_order, poll_interval_minutes, quality_profile, created_at, updated_at
        ) VALUES (?, ?, ?, 'ics', ?, ?, ?, ?, ?, NULL, ?, ?, ?, 1, 0, 30, 'standard', ?, ?)`
      ).bind(
        source.id,
        source.name,
        source.display_name,
        source.owner_type,
        source.source_category,
        source.url,
        source.icon,
        source.prefix,
        source.include_in_child_ics,
        source.include_in_family_ics,
        source.include_in_child_google_output,
        timestamp,
        timestamp
      ).run();
    }

    const samples = [
      {
        id: 'evt_grayson_hockey',
        sourceId: stableId('src', 'grayson:sample:hockey'),
        title: 'Hockey Practice',
        icon: '🏒',
        prefix: 'G:',
        startAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString(),
        endAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000 + 60 * 60 * 1000).toISOString(),
        ownerTargets: ['grayson', 'family'],
      },
      {
        id: 'evt_naomi_basketball',
        sourceId: stableId('src', 'naomi:sample:basketball'),
        title: 'Basketball Game',
        icon: '🏀',
        prefix: 'N:',
        startAt: new Date(Date.now() + 3 * 24 * 60 * 60 * 1000).toISOString(),
        endAt: new Date(Date.now() + 3 * 24 * 60 * 60 * 1000 + 90 * 60 * 1000).toISOString(),
        ownerTargets: ['naomi', 'family'],
      },
      {
        id: 'evt_family_holiday',
        sourceId: stableId('src', 'family:sample:holiday'),
        title: 'Family Holiday',
        icon: '',
        prefix: '',
        startAt: new Date(Date.now() + 5 * 24 * 60 * 60 * 1000).toISOString(),
        endAt: new Date(Date.now() + 5 * 24 * 60 * 60 * 1000 + 2 * 60 * 60 * 1000).toISOString(),
        ownerTargets: ['family'],
      },
    ];

    for (const sample of samples) {
      const identityKey = hashValue(`${sample.sourceId}:${sample.title}:${sample.startAt}`);
      await this.db.batch([
        this.db.prepare(
          `INSERT INTO canonical_events (
            id, source_id, identity_key, event_kind, title, source_icon, source_prefix, description, location, start_at, end_at, timezone,
            status, rrule, series_until, source_deleted, needs_review, source_changed_since_overlay,
            last_source_change_at, created_at, updated_at
          ) VALUES (?, ?, ?, 'single', ?, ?, ?, '', '', ?, ?, 'UTC', 'confirmed', NULL, NULL, 0, 0, 0, ?, ?, ?)`
        ).bind(
          sample.id,
          sample.sourceId,
          identityKey,
          sample.title,
          sample.icon,
          sample.prefix,
          sample.startAt,
          sample.endAt,
          timestamp,
          timestamp,
          timestamp
        ),
        this.db.prepare(
          `INSERT INTO event_instances (
            id, canonical_event_id, occurrence_start_at, occurrence_end_at, recurrence_instance_key,
            provider_recurrence_id, status, source_deleted, needs_review, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, NULL, 'confirmed', 0, 0, ?, ?)`
        ).bind(`${sample.id}_inst1`, sample.id, sample.startAt, sample.endAt, sample.startAt, timestamp, timestamp),
      ]);

      for (const target of sample.ownerTargets) {
        await this.db.prepare(
          `INSERT INTO output_rules (
            id, canonical_event_id, event_instance_id, target_key, include_state, derived_reason, created_at, updated_at
          ) VALUES (?, ?, ?, ?, 'included', 'seeded sample data', ?, ?)`
        ).bind(stableId('out', `${sample.id}:${target}`), sample.id, `${sample.id}_inst1`, target, timestamp, timestamp).run();
      }
    }
  }

  async getSourceById(id) {
    return this.db.prepare(`SELECT * FROM sources WHERE id = ?`).bind(id).first();
  }

  async listSourceInstances(sourceId) {
    const result = await this.db.prepare(
      `SELECT event_instances.id, event_instances.canonical_event_id
       FROM event_instances
       JOIN canonical_events ON canonical_events.id = event_instances.canonical_event_id
       WHERE canonical_events.source_id = ? AND canonical_events.source_deleted = 0 AND event_instances.source_deleted = 0`
    ).bind(sourceId).all();
    return result.results || [];
  }

  async syncSourceConfig(source) {
    if (!source) return;
    const timestamp = nowIso();
    await this.runInTransaction(async () => {
      await this.db.prepare(
        `UPDATE canonical_events
         SET source_icon = ?, source_prefix = ?, updated_at = ?
         WHERE source_id = ?`
      ).bind(source.icon || '', source.prefix || '', timestamp, source.id).run();

      await this.db.prepare(
        `DELETE FROM output_rules WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`
      ).bind(source.id).run();

      if (!Number(source.is_active)) {
        await this.db.prepare(`UPDATE canonical_events SET source_deleted = 1, updated_at = ? WHERE source_id = ?`).bind(timestamp, source.id).run();
        await this.db.prepare(
          `UPDATE event_instances
           SET source_deleted = 1, updated_at = ?
           WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`
        ).bind(timestamp, source.id).run();
        return;
      }

      await this.db.prepare(`UPDATE canonical_events SET source_deleted = 0, updated_at = ? WHERE source_id = ?`).bind(timestamp, source.id).run();
      await this.db.prepare(
        `UPDATE event_instances
         SET source_deleted = 0, updated_at = ?
         WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`
      ).bind(timestamp, source.id).run();

      const targets = await this.resolveTargetsForSource(source);
      const instances = await this.listSourceInstances(source.id);
      for (const instance of instances) {
        for (const target of targets) {
          await this.db.prepare(
            `INSERT INTO output_rules (
              id, canonical_event_id, event_instance_id, target_key, include_state, derived_reason, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'included', ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              include_state = excluded.include_state,
              derived_reason = excluded.derived_reason,
              updated_at = excluded.updated_at`
          ).bind(
            stableId('out', `${instance.id}:${target.target_key}`),
            instance.canonical_event_id,
            instance.id,
            target.target_key,
            'derived from source configuration',
            timestamp,
            timestamp
          ).run();
        }
      }
    });
  }

  async markJobStatus(jobId, status, { summary = null, error = null } = {}) {
    const finishedAt = ['completed', 'failed'].includes(status) ? nowIso() : null;
    await this.db.prepare(
      `UPDATE sync_jobs
       SET status = ?, finished_at = ?, summary_json = COALESCE(?, summary_json), error_json = COALESCE(?, error_json)
       WHERE id = ?`
    ).bind(
      status,
      finishedAt,
      summary ? JSON.stringify(summary) : null,
      error ? JSON.stringify(error) : null,
      jobId
    ).run();
  }

  async listActiveSources() {
    const result = await this.db.prepare(
      `SELECT * FROM sources WHERE is_active = 1 AND url IS NOT NULL ORDER BY owner_type, sort_order, name`
    ).all();
    return result.results || [];
  }

  async pruneStaleData() {
    const retainDays = parsePositiveInt(this.env.PRUNE_AFTER_DAYS, 30);
    const cutoff = new Date(Date.now() - retainDays * 24 * 60 * 60 * 1000).toISOString();
    const batchSize = 500;
    let blobKeysDeleted = 0;

    // Delete old snapshot blobs in batches first.
    while (this.env.SNAPSHOTS?.delete) {
      const batch = await this.db.prepare(
        `SELECT payload_blob_ref
         FROM source_snapshots
         WHERE payload_blob_ref IS NOT NULL AND fetched_at < ?
         LIMIT ?`
      ).bind(cutoff, batchSize).all();
      const keys = (batch.results || []).map((row) => row.payload_blob_ref).filter(Boolean);
      if (!keys.length) break;
      await this.env.SNAPSHOTS.delete(keys);
      blobKeysDeleted += keys.length;
      if (keys.length < batchSize) break;
    }

    const oldCountRow = await this.db.prepare(
      `SELECT COUNT(*) AS count FROM source_snapshots WHERE fetched_at < ?`
    ).bind(cutoff).first();
    const rowsDeleted = Number(oldCountRow?.count || 0);
    await this.db.prepare(`DELETE FROM source_snapshots WHERE fetched_at < ?`).bind(cutoff).run();

    return {
      retainDays,
      cutoff,
      snapshotRowsDeleted: rowsDeleted,
      snapshotBlobKeysDeleted: blobKeysDeleted,
    };
  }

  parseSourceSecret(secretValue) {
    return String(secretValue || '')
      .split(/\r?\n|,/)
      .map((line) => line.trim())
      .filter(Boolean)
      .filter((line) => /^https?:\/\//i.test(line))
      .map((line) => {
        const [url, emoji = ''] = line.split('|').map((part) => part.trim());
        return { url: url.replace(/^http:\/\//i, 'https://'), emoji };
      });
  }

  deriveLegacyTargetsForSource(source) {
    const targets = [];
    if (source.owner_type === 'grayson' && Number(source.include_in_child_ics)) {
      targets.push(buildDefaultTargetRule('grayson', source.owner_type, { icon: source.icon || '', prefix: '' }));
    }
    if (source.owner_type === 'naomi' && Number(source.include_in_child_ics)) {
      targets.push(buildDefaultTargetRule('naomi', source.owner_type, { icon: source.icon || '', prefix: '' }));
    }
    if (Number(source.include_in_family_ics)) {
      targets.push(
        buildDefaultTargetRule('family', source.owner_type, {
          icon: source.icon || '',
          prefix: source.prefix || this.derivePrefix(source.owner_type),
        })
      );
    }
    return targets;
  }

  async resolveTargetsForSource(source) {
    const links = await this.db.prepare(
      `SELECT target_key, target_type, icon, prefix, sort_order
       FROM source_target_links
       WHERE source_id = ? AND is_enabled = 1
       ORDER BY sort_order ASC, target_key ASC`
    ).bind(source.id).all();
    const targetLinks = (links.results || []).map((row) => ({
      target_key: row.target_key,
      target_type: row.target_type || (TARGETS.includes(row.target_key) ? 'ics' : 'google'),
      icon: cleanRuleValue(row.icon),
      prefix: cleanRuleValue(row.prefix),
      sort_order: Number(row.sort_order || 0),
    }));
    if (targetLinks.length) return targetLinks;
    return this.deriveLegacyTargetsForSource(source);
  }

  derivePrefix(ownerType) {
    if (ownerType === 'grayson') return 'G:';
    if (ownerType === 'naomi') return 'N:';
    return '';
  }

  async ingestSource(sourceId) {
    const source = await this.getSourceById(sourceId);
    if (!source) throw new Error(`Unknown source: ${sourceId}`);
    if (!Number(source.is_active)) {
      throw new Error(`Source is inactive: ${sourceId}`);
    }
    const sourceUrl = ensureHttpsUrl(source.url);
    const fetchedAt = nowIso();
    const horizonDays = parseInt(this.env.RECURRENCE_HORIZON_DAYS || '180', 10);
    const lookbackDays = parseInt(this.env.DEFAULT_LOOKBACK_DAYS || '7', 10);

    const stagedEvents = [];
    let totalEvents = 0;
    let totalInstances = 0;
    let response;
    let body = '';
    try {
      response = await fetch(sourceUrl, { cf: { cacheTtl: 0 } });
      body = await response.text();
    } catch (error) {
      const snapshotId = stableId('snap', `${sourceId}:${sourceUrl}:${fetchedAt}:error`);
      await this.db.prepare(
        `INSERT INTO source_snapshots (
          id, source_id, fetched_at, http_status, etag, last_modified, payload_blob_ref, parse_status, parse_error_summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).bind(
        snapshotId,
        sourceId,
        fetchedAt,
        null,
        null,
        null,
        null,
        'fetch_error',
        error instanceof Error ? error.message : String(error)
      ).run();
      throw error;
    }

    const snapshotId = stableId('snap', `${sourceId}:${sourceUrl}:${fetchedAt}`);
    const blobRef = `snapshots/${sourceId}/${snapshotId}.ics`;
    const snapshotsEnabled = String(this.env.SNAPSHOTS_ENABLED ?? 'true').toLowerCase() !== 'false';
    const maxSnapshotBytes = parsePositiveInt(this.env.SNAPSHOTS_MAX_BYTES, 1024 * 1024);
    const maxSnapshotRecords = parsePositiveInt(this.env.SNAPSHOTS_MAX_RECORDS, 5000);
    const payloadBytes = new TextEncoder().encode(body).length;
    const snapshotCountRow = await this.db.prepare(
      `SELECT COUNT(*) AS count FROM source_snapshots WHERE payload_blob_ref IS NOT NULL`
    ).first();
    const snapshotCount = Number(snapshotCountRow?.count || 0);
    const skipForSize = payloadBytes > maxSnapshotBytes;
    const skipForCount = snapshotCount >= maxSnapshotRecords;
    const canStoreSnapshot = snapshotsEnabled && !!this.env.SNAPSHOTS?.put && !skipForSize && !skipForCount;
    let storedBlobRef = null;

    if (canStoreSnapshot) {
      await this.env.SNAPSHOTS.put(blobRef, body, {
        httpMetadata: { contentType: 'text/calendar; charset=utf-8' },
      });
      storedBlobRef = blobRef;
    }

    await this.db.prepare(
      `INSERT INTO source_snapshots (
        id, source_id, fetched_at, http_status, etag, last_modified, payload_blob_ref, parse_status, parse_error_summary
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    ).bind(
      snapshotId,
      sourceId,
      fetchedAt,
      response.status,
      response.headers.get('etag'),
      response.headers.get('last-modified'),
      storedBlobRef,
      response.ok ? (storedBlobRef ? 'parsed' : 'parsed_no_blob') : 'fetch_error',
      response.ok
        ? storedBlobRef
          ? null
          : skipForSize
          ? `Snapshot skipped: payload ${payloadBytes} bytes exceeds ${maxSnapshotBytes}`
          : skipForCount
          ? `Snapshot skipped: snapshot cap ${maxSnapshotRecords} reached`
          : snapshotsEnabled
          ? 'Snapshot skipped: storage unavailable'
          : 'Snapshot skipped: disabled by SNAPSHOTS_ENABLED'
        : `HTTP ${response.status}`
    ).run();

    if (!response.ok) {
      throw new Error(`Source fetch failed for ${sourceUrl}: HTTP ${response.status}`);
    }

    const events = parseICS(body);
    for (const event of events) {
      totalEvents += 1;
      const providerKey = `${sourceId}:${event.uid}:${event.recurrenceId || ''}`;
      const fallbackKey = `${sourceId}:${event.summary}:${event.startAt}:${event.endAt}:${event.timezone}:${event.location}`;
      const identityKey = hashValue(event.uid ? providerKey : fallbackKey);
      const canonicalEventId = stableId('evt', identityKey);
      const sourceEventId = stableId('sev', providerKey || fallbackKey);
      const instances = expandRecurringEvent(event, {
        horizonDays,
        lookbackDays,
        now: new Date(),
      }).map((instance) => {
        totalInstances += 1;
        return {
          id: stableId('inst', `${canonicalEventId}:${instance.recurrenceInstanceKey}`),
          ...instance,
        };
      });

      stagedEvents.push({
        event,
        sourceIcon: source.icon || '',
        sourcePrefix: source.prefix ?? this.derivePrefix(source.owner_type),
        sourceEventId,
        canonicalEventId,
        identityKey,
        instances,
      });
    }

    const targets = await this.resolveTargetsForSource(source);

    await this.runInTransaction(async () => {
      await this.db.prepare(
        `UPDATE canonical_events SET source_deleted = 1, updated_at = ? WHERE source_id = ?`
      ).bind(fetchedAt, sourceId).run();
      await this.db.prepare(
        `UPDATE event_instances
         SET source_deleted = 1, updated_at = ?
         WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`
      ).bind(fetchedAt, sourceId).run();
      await this.db.prepare(
        `UPDATE source_events SET is_deleted_upstream = 1, last_seen_at = ? WHERE source_id = ?`
      ).bind(fetchedAt, sourceId).run();
      await this.db.prepare(
        `DELETE FROM output_rules WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)`
      ).bind(sourceId).run();

      for (const staged of stagedEvents) {
        const { event, sourceIcon, sourcePrefix, sourceEventId, canonicalEventId, identityKey, instances } = staged;

        await this.db.prepare(
          `INSERT INTO source_events (
            id, source_id, provider_uid, provider_recurrence_id, provider_etag, raw_hash, first_seen_at, last_seen_at, is_deleted_upstream
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
          ON CONFLICT(id) DO UPDATE SET
            provider_etag = excluded.provider_etag,
            raw_hash = excluded.raw_hash,
            last_seen_at = excluded.last_seen_at,
            is_deleted_upstream = 0`
        ).bind(
          sourceEventId,
          sourceId,
          event.uid,
          event.recurrenceId,
          event.lastModified,
          hashValue(JSON.stringify(event.raw)),
          fetchedAt,
          fetchedAt
        ).run();

        await this.db.prepare(
          `INSERT INTO canonical_events (
            id, source_id, identity_key, event_kind, title, source_icon, source_prefix, description, location, start_at, end_at, timezone,
            status, rrule, series_until, source_deleted, needs_review, source_changed_since_overlay,
            last_source_change_at, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, 0, ?, ?, ?)
          ON CONFLICT(id) DO UPDATE SET
            title = excluded.title,
            source_icon = excluded.source_icon,
            source_prefix = excluded.source_prefix,
            description = excluded.description,
            location = excluded.location,
            start_at = excluded.start_at,
            end_at = excluded.end_at,
            timezone = excluded.timezone,
            status = excluded.status,
            rrule = excluded.rrule,
            source_deleted = 0,
            last_source_change_at = excluded.last_source_change_at,
            updated_at = excluded.updated_at`
        ).bind(
          canonicalEventId,
          sourceId,
          identityKey,
          event.rrule ? 'series' : 'single',
          event.summary,
          sourceIcon,
          sourcePrefix,
          event.description,
          event.location,
          event.startAt,
          event.endAt,
          event.timezone,
          event.status,
          event.rrule || null,
          fetchedAt,
          fetchedAt,
          fetchedAt
        ).run();

        for (const instance of instances) {
          await this.db.prepare(
            `INSERT INTO event_instances (
              id, canonical_event_id, occurrence_start_at, occurrence_end_at, recurrence_instance_key,
              provider_recurrence_id, status, source_deleted, needs_review, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              occurrence_start_at = excluded.occurrence_start_at,
              occurrence_end_at = excluded.occurrence_end_at,
              status = excluded.status,
              source_deleted = 0,
              updated_at = excluded.updated_at`
          ).bind(
            instance.id,
            canonicalEventId,
            instance.occurrenceStartAt,
            instance.occurrenceEndAt,
            instance.recurrenceInstanceKey,
            event.recurrenceId,
            event.status,
            fetchedAt,
            fetchedAt
          ).run();

          for (const target of targets) {
            await this.db.prepare(
              `INSERT INTO output_rules (
                id, canonical_event_id, event_instance_id, target_key, include_state, derived_reason, created_at, updated_at
              ) VALUES (?, ?, ?, ?, 'included', ?, ?, ?)
              ON CONFLICT(id) DO UPDATE SET
                include_state = excluded.include_state,
                derived_reason = excluded.derived_reason,
                updated_at = excluded.updated_at`
            ).bind(
              stableId('out', `${instance.id}:${target.target_key}`),
              canonicalEventId,
              instance.id,
              target.target_key,
              `derived from ${source.owner_type} source`,
              fetchedAt,
              fetchedAt
            ).run();
          }
        }
      }
    });

    return {
      sourceId,
      fetchedAt,
      urlsFetched: 1,
      eventsParsed: totalEvents,
      instancesMaterialized: totalInstances,
      publishStrategy: typeof this.db.exec === 'function' ? 'transaction' : 'staged-before-mutate',
    };
  }
}

export async function createRepository(env) {
  if (!env.APP_DB) {
    throw new Error('APP_DB binding is required for repository access');
  }
  const repo = new D1Repository(env.APP_DB, env);
  await repo.ensureSupportTables();
  await repo.bootstrapLegacySources();
  if (String(env.SEED_SAMPLE_DATA || '').toLowerCase() === 'true') {
    await repo.seedSampleData();
  }
  return repo;
}
