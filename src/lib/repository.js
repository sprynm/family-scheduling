import crypto from 'node:crypto';
import { TARGETS, TARGET_LABELS } from './constants.js';
import { buildICS, expandRecurringEvent, parseICS } from './ics.js';

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
      `SELECT id, name, owner_type, provider_type, is_active, poll_interval_minutes, quality_profile, updated_at
       FROM sources ORDER BY owner_type, name`
    ).all();
    return result.results || [];
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
        canonical_events.description,
        canonical_events.location,
        canonical_events.status,
        event_instances.id AS event_instance_id,
        event_instances.occurrence_start_at,
        event_instances.occurrence_end_at
      FROM output_rules
      JOIN canonical_events ON canonical_events.id = output_rules.canonical_event_id
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
      summary: row.title,
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

  async seedSourceCatalog() {
    const timestamp = nowIso();
    const sourceRows = [
      ['source_grayson_primary', 'Grayson Sources', 'grayson', 'GRAYSON_URLS'],
      ['source_naomi_primary', 'Naomi Sources', 'naomi', 'NAOMI_URLS'],
      ['source_family_primary', 'Family Sources', 'family', 'FAMILY_URLS'],
    ];
    for (const [id, name, ownerType, secretRef] of sourceRows) {
      await this.db.prepare(
        `INSERT OR IGNORE INTO sources (
          id, name, provider_type, owner_type, fetch_url_secret_ref, is_active,
          poll_interval_minutes, quality_profile, created_at, updated_at
        ) VALUES (?, ?, 'ics', ?, ?, 1, 30, 'standard', ?, ?)`
      ).bind(id, name, ownerType, secretRef, timestamp, timestamp).run();
    }
  }

  async seedSampleData() {
    await this.seedSourceCatalog();
    const existing = await this.db.prepare(`SELECT COUNT(*) AS count FROM canonical_events`).first();
    if ((existing?.count || 0) > 0) return;

    const timestamp = nowIso();
    const samples = [
      {
        id: 'evt_grayson_hockey',
        sourceId: 'source_grayson_primary',
        title: 'Hockey Practice',
        startAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString(),
        endAt: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000 + 60 * 60 * 1000).toISOString(),
        ownerTargets: ['grayson', 'family'],
      },
      {
        id: 'evt_naomi_basketball',
        sourceId: 'source_naomi_primary',
        title: 'Basketball Game',
        startAt: new Date(Date.now() + 3 * 24 * 60 * 60 * 1000).toISOString(),
        endAt: new Date(Date.now() + 3 * 24 * 60 * 60 * 1000 + 90 * 60 * 1000).toISOString(),
        ownerTargets: ['naomi', 'family'],
      },
      {
        id: 'evt_family_holiday',
        sourceId: 'source_family_primary',
        title: 'Family Holiday',
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
            id, source_id, identity_key, event_kind, title, description, location, start_at, end_at, timezone,
            status, rrule, series_until, source_deleted, needs_review, source_changed_since_overlay,
            last_source_change_at, created_at, updated_at
          ) VALUES (?, ?, ?, 'single', ?, '', '', ?, ?, 'UTC', 'confirmed', NULL, NULL, 0, 0, 0, ?, ?, ?)`
        ).bind(sample.id, sample.sourceId, identityKey, sample.title, sample.startAt, sample.endAt, timestamp, timestamp, timestamp),
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
      `SELECT * FROM sources WHERE is_active = 1 ORDER BY owner_type, name`
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

  deriveTargets(ownerType) {
    if (ownerType === 'grayson') return ['grayson', 'family'];
    if (ownerType === 'naomi') return ['naomi', 'family'];
    return ['family'];
  }

  async ingestSource(sourceId) {
    const source = await this.getSourceById(sourceId);
    if (!source) throw new Error(`Unknown source: ${sourceId}`);

    const rawSecret = this.env[source.fetch_url_secret_ref] || '';
    const urls = this.parseSourceSecret(rawSecret);
    const fetchedAt = nowIso();
    const horizonDays = parseInt(this.env.RECURRENCE_HORIZON_DAYS || '180', 10);
    const lookbackDays = parseInt(this.env.DEFAULT_LOOKBACK_DAYS || '7', 10);

    const stagedEvents = [];
    let totalEvents = 0;
    let totalInstances = 0;

    for (const entry of urls) {
      let response;
      let body = '';
      try {
        response = await fetch(entry.url, { cf: { cacheTtl: 0 } });
        body = await response.text();
      } catch (error) {
        const snapshotId = stableId('snap', `${sourceId}:${entry.url}:${fetchedAt}:error`);
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

      const snapshotId = stableId('snap', `${sourceId}:${entry.url}:${fetchedAt}`);
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
        throw new Error(`Source fetch failed for ${entry.url}: HTTP ${response.status}`);
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
          sourceEventId,
          canonicalEventId,
          identityKey,
          instances,
        });
      }
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
        const { event, sourceEventId, canonicalEventId, identityKey, instances } = staged;

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
            id, source_id, identity_key, event_kind, title, description, location, start_at, end_at, timezone,
            status, rrule, series_until, source_deleted, needs_review, source_changed_since_overlay,
            last_source_change_at, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, 0, ?, ?, ?)
          ON CONFLICT(id) DO UPDATE SET
            title = excluded.title,
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

          for (const target of this.deriveTargets(source.owner_type)) {
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
      urlsFetched: urls.length,
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
  await repo.seedSourceCatalog();
  if (String(env.SEED_SAMPLE_DATA || '').toLowerCase() === 'true') {
    await repo.seedSampleData();
  }
  return repo;
}
