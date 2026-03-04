import { env, createExecutionContext, waitOnExecutionContext } from 'cloudflare:test';
import { beforeEach, describe, expect, it } from 'vitest';
import worker from '../src/index.js';

class FakeStatement {
  constructor(db, sql) {
    this.db = db;
    this.sql = sql;
    this.values = [];
  }

  bind(...values) {
    this.values = values;
    return this;
  }

  async all() {
    return { results: this.db.runAll(this.sql, this.values) };
  }

  async first() {
    return this.db.runFirst(this.sql, this.values);
  }

  async run() {
    this.db.run(this.sql, this.values);
    return { success: true };
  }
}

class FakeDb {
  constructor() {
    this.sources = [];
    this.canonicalEvents = [];
    this.eventInstances = [];
    this.eventOverrides = [];
    this.outputRules = [];
    this.googleTargets = [];
    this.googleEventLinks = [];
    this.syncJobs = [];
  }

  prepare(sql) {
    return new FakeStatement(this, sql);
  }

  batch(statements) {
    return Promise.all(statements.map((statement) => statement.run()));
  }

  run(sql, values) {
    if (sql.includes('INSERT OR IGNORE INTO sources')) {
      const [id, name, ownerType, secretRef, createdAt, updatedAt] = values;
      if (!this.sources.find((row) => row.id === id)) {
        this.sources.push({
          id,
          name,
          provider_type: 'ics',
          owner_type: ownerType,
          fetch_url_secret_ref: secretRef,
          is_active: 1,
          poll_interval_minutes: 30,
          quality_profile: 'standard',
          created_at: createdAt,
          updated_at: updatedAt,
        });
      }
      return;
    }

    if (sql.includes('INSERT INTO canonical_events')) {
      const [id, sourceId, identityKey, title, startAt, endAt, lastSourceChangeAt, createdAt, updatedAt] = values;
      this.canonicalEvents.push({
        id,
        source_id: sourceId,
        identity_key: identityKey,
        event_kind: 'single',
        title,
        description: '',
        location: '',
        start_at: startAt,
        end_at: endAt,
        timezone: 'UTC',
        status: 'confirmed',
        rrule: null,
        series_until: null,
        source_deleted: 0,
        needs_review: 0,
        source_changed_since_overlay: 0,
        last_source_change_at: lastSourceChangeAt,
        created_at: createdAt,
        updated_at: updatedAt,
      });
      return;
    }

    if (sql.includes('INSERT INTO event_instances')) {
      const [id, canonicalEventId, occurrenceStartAt, occurrenceEndAt, recurrenceInstanceKey, createdAt, updatedAt] = values;
      this.eventInstances.push({
        id,
        canonical_event_id: canonicalEventId,
        occurrence_start_at: occurrenceStartAt,
        occurrence_end_at: occurrenceEndAt,
        recurrence_instance_key: recurrenceInstanceKey,
        provider_recurrence_id: null,
        status: 'confirmed',
        source_deleted: 0,
        needs_review: 0,
        created_at: createdAt,
        updated_at: updatedAt,
      });
      return;
    }

    if (sql.includes('INSERT INTO output_rules')) {
      const [id, canonicalEventId, eventInstanceId, targetKey, createdAt, updatedAt] = values;
      this.outputRules.push({
        id,
        canonical_event_id: canonicalEventId,
        event_instance_id: eventInstanceId,
        target_key: targetKey,
        include_state: 'included',
        derived_reason: 'seeded sample data',
        created_at: createdAt,
        updated_at: updatedAt,
      });
      return;
    }

    if (sql.includes('INSERT INTO event_overrides')) {
      const [id, scopeType, canonicalEventId, eventInstanceId, overrideType, payloadJson, createdBy, createdAt, updatedAt] = values;
      this.eventOverrides.push({
        id,
        scope_type: scopeType,
        canonical_event_id: canonicalEventId,
        event_instance_id: eventInstanceId,
        override_type: overrideType,
        payload_json: payloadJson,
        created_by: createdBy,
        created_at: createdAt,
        updated_at: updatedAt,
        cleared_at: null,
      });
      return;
    }

    if (sql.includes('UPDATE canonical_events')) {
      const [updatedAt, eventId] = values;
      const row = this.canonicalEvents.find((event) => event.id === eventId);
      if (row) {
        row.needs_review = 0;
        row.source_changed_since_overlay = 0;
        row.updated_at = updatedAt;
      }
      return;
    }

    if (sql.includes('UPDATE event_overrides SET cleared_at')) {
      const [clearedAt, updatedAt, overrideId] = values;
      const row = this.eventOverrides.find((override) => override.id === overrideId);
      if (row) {
        row.cleared_at = clearedAt;
        row.updated_at = updatedAt;
      }
      return;
    }

    if (sql.includes('INSERT INTO sync_jobs')) {
      const [id, jobType, scopeType, scopeId, startedAt] = values;
      this.syncJobs.push({
        id,
        job_type: jobType,
        scope_type: scopeType,
        scope_id: scopeId,
        status: 'queued',
        started_at: startedAt,
        finished_at: null,
        summary_json: null,
        error_json: null,
      });
    }
  }

  runAll(sql, values) {
    if (sql.includes('FROM sources ORDER BY')) return this.sources;
    if (sql.includes('FROM sync_jobs ORDER BY')) return this.syncJobs.slice(0, values[0] || 50);
    if (sql.includes('FROM google_targets')) return this.googleTargets;
    if (sql.includes('FROM canonical_events') && sql.includes('GROUP BY canonical_events.id')) {
      const target = values.length === 2 ? values[0] : null;
      const rows = this.canonicalEvents
        .filter((event) => !target || this.outputRules.some((rule) => rule.canonical_event_id === event.id && rule.target_key === target && rule.include_state === 'included'))
        .map((event) => {
          const source = this.sources.find((row) => row.id === event.source_id);
          const firstInstance = this.eventInstances.find((instance) => instance.canonical_event_id === event.id);
          return {
            id: event.id,
            title: event.title,
            location: event.location,
            status: event.status,
            source_deleted: event.source_deleted,
            needs_review: event.needs_review,
            updated_at: event.updated_at,
            source_id: event.source_id,
            source_name: source?.name || '',
            owner_type: source?.owner_type || '',
            next_occurrence: firstInstance?.occurrence_start_at || null,
          };
        });
      return rows.slice(0, values.at(-1) || 200);
    }
    if (sql.includes('FROM event_instances WHERE canonical_event_id')) {
      return this.eventInstances.filter((row) => row.canonical_event_id === values[0]);
    }
    if (sql.includes('FROM event_overrides')) {
      return this.eventOverrides.filter((row) => row.canonical_event_id === values[0] && !row.cleared_at);
    }
    if (sql.includes('FROM output_rules WHERE canonical_event_id')) {
      return this.outputRules
        .filter((row) => row.canonical_event_id === values[0])
        .map((row) => ({
          target_key: row.target_key,
          include_state: row.include_state,
          derived_reason: row.derived_reason,
        }));
    }
    if (sql.includes('FROM google_event_links WHERE canonical_event_id')) {
      return this.googleEventLinks.filter((row) => row.canonical_event_id === values[0]);
    }
    if (sql.includes('FROM output_rules') && sql.includes('JOIN canonical_events')) {
      const target = values[0];
      return this.outputRules
        .filter((row) => row.target_key === target && row.include_state === 'included')
        .map((rule) => {
          const event = this.canonicalEvents.find((row) => row.id === rule.canonical_event_id);
          const instance = this.eventInstances.find((row) => row.id === rule.event_instance_id);
          return {
            canonical_event_id: event.id,
            title: event.title,
            description: event.description,
            location: event.location,
            status: event.status,
            event_instance_id: instance.id,
            occurrence_start_at: instance.occurrence_start_at,
            occurrence_end_at: instance.occurrence_end_at,
          };
        });
    }
    return [];
  }

  runFirst(sql, values) {
    if (sql.includes('SELECT COUNT(*) AS count FROM canonical_events')) {
      return { count: this.canonicalEvents.length };
    }
    if (sql.includes('FROM canonical_events') && sql.includes('JOIN sources')) {
      const event = this.canonicalEvents.find((row) => row.id === values[0]);
      if (!event) return null;
      const source = this.sources.find((row) => row.id === event.source_id);
      return {
        ...event,
        source_name: source?.name || '',
        owner_type: source?.owner_type || '',
      };
    }
    if (sql.includes('SELECT canonical_event_id FROM event_overrides')) {
      const override = this.eventOverrides.find((row) => row.id === values[0]);
      return override ? { canonical_event_id: override.canonical_event_id } : null;
    }
    return null;
  }
}

beforeEach(() => {
  env.TOKEN = 'test-token';
  env.CALENDAR_NAME_FAMILY = 'Family Combined';
  env.CALENDAR_NAME_GRAYSON = 'Grayson Combined';
  env.CALENDAR_NAME_NAOMI = 'Naomi Combined';
  env.ALLOW_DEV_ROLE_HEADER = 'true';
  env.SEED_SAMPLE_DATA = 'true';
  env.APP_DB = new FakeDb();
});

describe('family-scheduling worker', () => {
  it('rejects missing feed token', async () => {
    const request = new Request('http://example.com/family.isc');
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    expect(response.status).toBe(401);
  });

  it('serves legacy compatible family feed', async () => {
    const request = new Request('http://example.com/family.isc?token=test-token');
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.text();
    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('text/calendar');
    expect(body).toContain('BEGIN:VCALENDAR');
    expect(body).toContain('X-WR-CALNAME:Family Combined');
    expect(body).toContain('SUMMARY:Basketball Game');
  });

  it('lists events for authenticated admin routes', async () => {
    const request = new Request('http://example.com/api/events', {
      headers: { 'x-user-role': 'admin' },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(200);
    expect(body.events.length).toBeGreaterThan(0);
  });

  it('rejects role header auth when dev header support is disabled', async () => {
    env.ALLOW_DEV_ROLE_HEADER = 'false';
    const request = new Request('http://example.com/api/events', {
      headers: { 'x-user-role': 'admin' },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    expect(response.status).toBe(403);
  });

  it('creates override records', async () => {
    const request = new Request('http://example.com/api/events/evt_grayson_hockey/overrides', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-user-role': 'editor',
      },
      body: JSON.stringify({
        overrideType: 'skip',
        payload: { reason: 'Travel conflict' },
      }),
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(201);
    expect(body.overrides[0].override_type).toBe('skip');
  });

  it('queues full rebuild jobs for admins', async () => {
    const request = new Request('http://example.com/api/rebuild/full', {
      method: 'POST',
      headers: { 'x-user-role': 'admin' },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(202);
    expect(body.job.job_type).toBe('rebuild_system');
  });
});
