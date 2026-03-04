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

export class D1Repository {
  constructor(db, env) {
    this.db = db;
    this.env = env;
  }

  async runInTransaction(work) {
    if (typeof this.db.exec !== 'function') {
      return work();
    }

    await this.db.exec('BEGIN');
    try {
      const result = await work();
      await this.db.exec('COMMIT');
      return result;
    } catch (error) {
      try {
        await this.db.exec('ROLLBACK');
      } catch {
        // Ignore rollback failures so the original error is preserved.
      }
      throw error;
    }
  }

  async listSources() {
    const result = await this.db.prepare(
      `SELECT id, name, display_name, owner_type, provider_type, source_category, url, icon, prefix,
              include_in_child_ics, include_in_family_ics, include_in_child_google_output,
              is_active, sort_order, poll_interval_minutes, quality_profile, updated_at
       FROM sources ORDER BY owner_type, sort_order, COALESCE(display_name, name), name`
    ).all();
    return result.results || [];
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
    const source = await this.getSourceById(sourceId);
    await this.syncSourceConfig(source);
    return source;
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

  async getFeedContract() {
    return {
      host: 'https://ics.sprynewmedia.com',
      requiredToken: true,
      acceptedParams: ['token', 'lookback', 'name'],
      endpoints: TARGETS.flatMap((target) => [`/${target}.isc`, `/${target}.ics`]),
      priority: {
        family: 'grandparents',
        grayson: 'home-assistant',
        naomi: 'home-assistant',
      },
    };
  }

  async generateFeed({ target, calendarName, lookbackDays = 7 }) {
    const result = await this.db.prepare(
      `SELECT
        canonical_events.id AS canonical_event_id,
        canonical_events.title,
        COALESCE(sources.icon, canonical_events.source_icon, '') AS source_icon,
        COALESCE(sources.prefix, canonical_events.source_prefix, '') AS source_prefix,
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

      const targets = this.deriveTargetsForSource(source);
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
            stableId('out', `${instance.id}:${target}`),
            instance.canonical_event_id,
            instance.id,
            target,
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

  deriveTargetsForSource(source) {
    const targets = [];
    if (source.owner_type === 'grayson' && Number(source.include_in_child_ics)) targets.push('grayson');
    if (source.owner_type === 'naomi' && Number(source.include_in_child_ics)) targets.push('naomi');
    if (Number(source.include_in_family_ics)) targets.push('family');
    return targets;
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

    if (this.env.SNAPSHOTS?.put) {
      await this.env.SNAPSHOTS.put(blobRef, body, {
        httpMetadata: { contentType: 'text/calendar; charset=utf-8' },
      });
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
      blobRef,
      response.ok ? 'parsed' : 'fetch_error',
      response.ok ? null : `HTTP ${response.status}`
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
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, 0, ?, ?, ?)
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
          event.rrule,
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

          for (const target of this.deriveTargetsForSource(source)) {
            await this.db.prepare(
              `INSERT INTO output_rules (
                id, canonical_event_id, event_instance_id, target_key, include_state, derived_reason, created_at, updated_at
              ) VALUES (?, ?, ?, ?, 'included', ?, ?, ?)
              ON CONFLICT(id) DO UPDATE SET
                include_state = excluded.include_state,
                derived_reason = excluded.derived_reason,
                updated_at = excluded.updated_at`
            ).bind(
              stableId('out', `${instance.id}:${target}`),
              canonicalEventId,
              instance.id,
              target,
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
  await repo.bootstrapLegacySources();
  if (String(env.SEED_SAMPLE_DATA || '').toLowerCase() === 'true') {
    await repo.seedSampleData();
  }
  return repo;
}
