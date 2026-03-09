import { env, createExecutionContext, waitOnExecutionContext } from 'cloudflare:test';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import worker from '../src/index.js';
import { clearAuthCachesForTest } from '../src/lib/auth.js';
import { D1Repository } from '../src/lib/repository.js';
import { parseICS } from '../src/lib/ics.js';

async function drainQueue(env) {
  while (env.JOBS_QUEUE.sent.length) {
    const messages = env.JOBS_QUEUE.sent.splice(0).map((body) => ({
      body,
      ack: vi.fn(),
      retry: vi.fn(),
    }));
    await worker.queue({ messages }, env);
  }
}

function base64UrlEncodeJson(value) {
  const json = JSON.stringify(value);
  return Buffer.from(json, 'utf8').toString('base64url');
}

async function createAccessJwt({ privateKey, kid, payload }) {
  const header = { alg: 'RS256', typ: 'JWT', kid };
  const encodedHeader = base64UrlEncodeJson(header);
  const encodedPayload = base64UrlEncodeJson(payload);
  const signedPart = `${encodedHeader}.${encodedPayload}`;
  const signature = await crypto.subtle.sign(
    { name: 'RSASSA-PKCS1-v1_5' },
    privateKey,
    new TextEncoder().encode(signedPart)
  );
  return `${signedPart}.${Buffer.from(signature).toString('base64url')}`;
}

async function createServiceAccountJson() {
  const keyPair = await crypto.subtle.generateKey(
    {
      name: 'RSASSA-PKCS1-v1_5',
      modulusLength: 2048,
      publicExponent: new Uint8Array([1, 0, 1]),
      hash: 'SHA-256',
    },
    true,
    ['sign', 'verify']
  );
  const pkcs8 = await crypto.subtle.exportKey('pkcs8', keyPair.privateKey);
  const privateKeyPem = `-----BEGIN PRIVATE KEY-----\n${Buffer.from(pkcs8).toString('base64')}\n-----END PRIVATE KEY-----\n`;
  return JSON.stringify({
    type: 'service_account',
    client_email: 'worker@example.com',
    private_key: privateKeyPem,
    token_uri: 'https://oauth2.googleapis.com/token',
  });
}

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
    this.outputTargets = [];
    this.sourceTargetLinks = [];
    this.sourceSnapshots = [];
    this.sourceEvents = [];
    this.canonicalEvents = [];
    this.eventInstances = [];
    this.eventOverrides = [];
    this.outputRules = [];
    this.googleEventLinks = [];
    this.syncJobs = [];
    this.googleEventLinkGoogleEventIdNotNull = 0;
    this.googleEventLinksV2 = null;
  }

  prepare(sql) {
    return new FakeStatement(this, sql);
  }

  batch(statements) {
    return Promise.all(statements.map((statement) => statement.run()));
  }

  run(sql, values) {
    if (sql.includes('CREATE TABLE IF NOT EXISTS output_targets')) {
      return;
    }

    if (sql.includes('CREATE TABLE IF NOT EXISTS source_target_links')) {
      return;
    }

    if (sql.includes('CREATE UNIQUE INDEX IF NOT EXISTS source_target_links_source_target_idx')) {
      return;
    }

    if (sql.includes('ALTER TABLE source_target_links ADD COLUMN')) {
      return;
    }

    if (sql.includes('ALTER TABLE output_rules ADD COLUMN target_id')) {
      return;
    }

    if (sql.includes('ALTER TABLE google_event_links ADD COLUMN target_id')) {
      return;
    }

    if (sql.includes('CREATE TABLE google_event_links_v2')) {
      this.googleEventLinksV2 = [];
      return;
    }

    if (sql.includes('INSERT INTO google_event_links_v2')) {
      this.googleEventLinksV2 = this.googleEventLinks.map((row) => ({ ...row }));
      return;
    }

    if (sql.includes('DROP TABLE google_event_links')) {
      this.googleEventLinks = [];
      return;
    }

    if (sql.includes('ALTER TABLE google_event_links_v2 RENAME TO google_event_links')) {
      this.googleEventLinks = [...(this.googleEventLinksV2 || [])];
      this.googleEventLinksV2 = null;
      this.googleEventLinkGoogleEventIdNotNull = 0;
      return;
    }

    if (sql.includes('CREATE UNIQUE INDEX IF NOT EXISTS source_target_links_source_target_id_idx')) {
      return;
    }

    if (sql.includes('CREATE INDEX IF NOT EXISTS output_rules_target_id_idx')) {
      return;
    }

    if (sql.includes('CREATE INDEX IF NOT EXISTS google_event_links_target_id_idx')) {
      return;
    }

    if (sql.includes('INSERT OR IGNORE INTO sources')) {
      const [
        id,
        name,
        displayName,
        ownerType,
        sourceCategory,
        url,
        icon,
        prefix,
        secretRef,
        includeInChildIcs,
        includeInFamilyIcs,
        includeInChildGoogleOutput,
        sortOrder,
        createdAt,
        updatedAt,
      ] = values;
      if (!this.sources.find((row) => row.id === id)) {
        this.sources.push({
          id,
          name,
          display_name: displayName,
          provider_type: 'ics',
          owner_type: ownerType,
          source_category: sourceCategory,
          url,
          icon,
          prefix,
          fetch_url_secret_ref: secretRef,
          include_in_child_ics: includeInChildIcs,
          include_in_family_ics: includeInFamilyIcs,
          include_in_child_google_output: includeInChildGoogleOutput,
          is_active: 1,
          sort_order: sortOrder,
          poll_interval_minutes: 30,
          quality_profile: 'standard',
          created_at: createdAt,
          updated_at: updatedAt,
        });
      }
      return;
    }

    if (sql.includes('INSERT INTO sources')) {
      const [
        id,
        name,
        displayName,
        ownerType,
        sourceCategory,
        url,
        icon,
        prefix,
        includeInChildIcs,
        includeInFamilyIcs,
        includeInChildGoogleOutput,
        sortOrder,
        pollIntervalMinutes,
        qualityProfile,
        createdAt,
        updatedAt,
      ] = values;
      this.sources.push({
        id,
        name,
        display_name: displayName,
        provider_type: 'ics',
        owner_type: ownerType,
        source_category: sourceCategory,
        url,
        icon,
        prefix,
        fetch_url_secret_ref: null,
        include_in_child_ics: includeInChildIcs,
        include_in_family_ics: includeInFamilyIcs,
        include_in_child_google_output: includeInChildGoogleOutput,
        is_active: 1,
        sort_order: sortOrder,
        poll_interval_minutes: pollIntervalMinutes,
        quality_profile: qualityProfile,
        created_at: createdAt,
        updated_at: updatedAt,
      });
      return;
    }

    if (sql.includes('INSERT INTO source_target_links')) {
      const [id, sourceId, targetId, targetKey, targetType, icon, prefix, sortOrder, createdAt, updatedAt] = values;
      const existing = this.sourceTargetLinks.find((row) => row.id === id || (row.source_id === sourceId && row.target_key === targetKey));
      if (existing) {
        Object.assign(existing, {
          target_id: targetId,
          target_type: targetType,
          icon,
          prefix,
          sort_order: sortOrder,
          is_enabled: 1,
          updated_at: updatedAt,
        });
      } else {
        this.sourceTargetLinks.push({
          id,
          source_id: sourceId,
          target_id: targetId,
          target_key: targetKey,
          target_type: targetType,
          icon,
          prefix,
          sort_order: sortOrder,
          is_enabled: 1,
          created_at: createdAt,
          updated_at: updatedAt,
        });
      }
      return;
    }

    if (sql.includes('INSERT INTO output_targets')) {
      const isGoogleInsert = sql.includes("VALUES (?, 'google', ?, ?, ?, ?, 0, ?, ?, ?)");
      const [id, ...rest] = values;
      const targetType = isGoogleInsert ? 'google' : rest[0];
      const slug = isGoogleInsert ? rest[0] : rest[1];
      const displayName = isGoogleInsert ? rest[1] : rest[2];
      const calendarId = isGoogleInsert ? rest[2] : rest[3];
      const ownershipMode = isGoogleInsert ? rest[3] : rest[4];
      const isSystem = isGoogleInsert ? 0 : rest[5];
      const isActive = isGoogleInsert ? rest[4] : rest[6];
      const createdAt = isGoogleInsert ? rest[5] : rest[7];
      const updatedAt = isGoogleInsert ? rest[6] : rest[8];
      const existing = this.outputTargets.find((row) => row.id === id || row.slug === slug);
      if (existing) {
        Object.assign(existing, {
          target_type: targetType,
          slug,
          display_name: displayName,
          calendar_id: calendarId,
          ownership_mode: ownershipMode,
          is_system: isSystem,
          is_active: isActive,
          updated_at: updatedAt,
        });
      } else {
        this.outputTargets.push({
          id,
          target_type: targetType,
          slug,
          display_name: displayName,
          calendar_id: calendarId,
          ownership_mode: ownershipMode,
          is_system: isSystem,
          is_active: isActive,
          created_at: createdAt,
          updated_at: updatedAt,
        });
      }
      return;
    }

    if (sql.includes('UPDATE source_target_links') && sql.includes('SET target_id = (')) {
      for (const row of this.sourceTargetLinks.filter((entry) => !entry.target_id)) {
        const target = this.outputTargets.find((entry) => entry.slug === row.target_key);
        if (target) row.target_id = target.id;
      }
      return;
    }

    if (sql.includes('UPDATE output_rules') && sql.includes('SET target_id = (')) {
      for (const row of this.outputRules.filter((entry) => !entry.target_id)) {
        const target = this.outputTargets.find((entry) => entry.slug === row.target_key);
        if (target) row.target_id = target.id;
      }
      return;
    }

    if (sql.includes('UPDATE google_event_links') && sql.includes('SET target_id = (')) {
      for (const row of this.googleEventLinks.filter((entry) => !entry.target_id)) {
        const target = this.outputTargets.find((entry) => entry.slug === row.target_key);
        if (target) row.target_id = target.id;
      }
      return;
    }

    if (sql.includes('INSERT INTO source_snapshots')) {
      const [id, sourceId, fetchedAt, httpStatus, etag, lastModified, payloadBlobRef, parseStatus, parseErrorSummary] = values;
      this.sourceSnapshots.push({
        id,
        source_id: sourceId,
        fetched_at: fetchedAt,
        http_status: httpStatus,
        etag,
        last_modified: lastModified,
        payload_blob_ref: payloadBlobRef,
        parse_status: parseStatus,
        parse_error_summary: parseErrorSummary,
      });
      return;
    }

    if (sql.includes('INSERT INTO source_events')) {
      const [id, sourceId, providerUid, providerRecurrenceId, providerEtag, rawHash, firstSeenAt, lastSeenAt] = values;
      const existing = this.sourceEvents.find((row) => row.id === id);
      if (existing) {
        Object.assign(existing, {
          provider_etag: providerEtag,
          raw_hash: rawHash,
          last_seen_at: lastSeenAt,
          is_deleted_upstream: 0,
        });
      } else {
        this.sourceEvents.push({
          id,
          source_id: sourceId,
          provider_uid: providerUid,
          provider_recurrence_id: providerRecurrenceId,
          provider_etag: providerEtag,
          raw_hash: rawHash,
          first_seen_at: firstSeenAt,
          last_seen_at: lastSeenAt,
          is_deleted_upstream: 0,
        });
      }
      return;
    }

    if (sql.includes('INSERT INTO canonical_events')) {
      if (sql.includes('ON CONFLICT(id) DO UPDATE')) {
        const [
          id,
          sourceId,
          identityKey,
          eventKind,
          title,
          sourceIcon,
          sourcePrefix,
          description,
          location,
          startAt,
          endAt,
          timezone,
          status,
          rrule,
          lastSourceChangeAt,
          createdAt,
          updatedAt,
        ] = values;
        const existing = this.canonicalEvents.find((row) => row.id === id);
        if (existing) {
          Object.assign(existing, {
            title,
            source_icon: sourceIcon,
            source_prefix: sourcePrefix,
            description,
            location,
            start_at: startAt,
            end_at: endAt,
            timezone,
            status,
            rrule,
            source_deleted: 0,
            last_source_change_at: lastSourceChangeAt,
            updated_at: updatedAt,
          });
        } else {
          this.canonicalEvents.push({
            id,
            source_id: sourceId,
            identity_key: identityKey,
            event_kind: eventKind,
            title,
            source_icon: sourceIcon,
            source_prefix: sourcePrefix,
            description: description || '',
            location: location || '',
            start_at: startAt,
            end_at: endAt,
            timezone: timezone || 'UTC',
            status: status || 'confirmed',
            rrule: rrule || null,
            series_until: null,
            source_deleted: 0,
            needs_review: 0,
            source_changed_since_overlay: 0,
            last_source_change_at: lastSourceChangeAt,
            created_at: createdAt,
            updated_at: updatedAt,
          });
        }
      } else {
        const isSeedInsert = values.length === 11;
        const [
          id,
          sourceId,
          identityKey,
          title,
          sourceIcon,
          sourcePrefix,
          maybeDescriptionOrStartAt,
          maybeLocationOrEndAt,
          maybeStartAtOrLastSourceChangeAt,
          maybeEndAtOrCreatedAt,
          maybeTimezoneOrUpdatedAt,
          status,
          _rrule,
          lastSourceChangeAt,
          createdAt,
          updatedAt,
        ] = values;
        const description = isSeedInsert ? '' : maybeDescriptionOrStartAt;
        const location = isSeedInsert ? '' : maybeLocationOrEndAt;
        const startAt = isSeedInsert ? maybeDescriptionOrStartAt : maybeStartAtOrLastSourceChangeAt;
        const endAt = isSeedInsert ? maybeLocationOrEndAt : maybeEndAtOrCreatedAt;
        const timezone = isSeedInsert ? 'UTC' : maybeTimezoneOrUpdatedAt;
        this.canonicalEvents.push({
          id,
          source_id: sourceId,
          identity_key: identityKey,
          event_kind: 'single',
          title,
          source_icon: sourceIcon,
          source_prefix: sourcePrefix,
          description: description || '',
          location: location || '',
          start_at: startAt,
          end_at: endAt,
          timezone: timezone || 'UTC',
          status: isSeedInsert ? 'confirmed' : status || 'confirmed',
          rrule: null,
          series_until: null,
          source_deleted: 0,
          needs_review: 0,
          source_changed_since_overlay: 0,
          last_source_change_at: isSeedInsert ? maybeStartAtOrLastSourceChangeAt : lastSourceChangeAt,
          created_at: isSeedInsert ? maybeEndAtOrCreatedAt : createdAt,
          updated_at: isSeedInsert ? maybeTimezoneOrUpdatedAt : updatedAt,
        });
      }
      return;
    }

    if (sql.includes('INSERT INTO event_instances')) {
      if (sql.includes('ON CONFLICT(id) DO UPDATE')) {
        const [id, canonicalEventId, occurrenceStartAt, occurrenceEndAt, recurrenceInstanceKey, providerRecurrenceId, status, createdAt, updatedAt] = values;
        const existing = this.eventInstances.find((row) => row.id === id);
        if (existing) {
          Object.assign(existing, {
            occurrence_start_at: occurrenceStartAt,
            occurrence_end_at: occurrenceEndAt,
            status,
            source_deleted: 0,
            updated_at: updatedAt,
          });
        } else {
          this.eventInstances.push({
            id,
            canonical_event_id: canonicalEventId,
            occurrence_start_at: occurrenceStartAt,
            occurrence_end_at: occurrenceEndAt,
            recurrence_instance_key: recurrenceInstanceKey,
            provider_recurrence_id: providerRecurrenceId,
            status: status || 'confirmed',
            source_deleted: 0,
            needs_review: 0,
            created_at: createdAt,
            updated_at: updatedAt,
          });
        }
      } else {
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
      }
      return;
    }

    if (sql.includes('INSERT INTO output_rules')) {
      if (sql.includes('ON CONFLICT(id) DO UPDATE')) {
        const hasExplicitIncludeState = values.length === 9;
        const [id, canonicalEventId, eventInstanceId, targetId, targetKey] = values;
        const includeState = hasExplicitIncludeState ? values[5] : 'included';
        const derivedReason = hasExplicitIncludeState ? values[6] : values[5];
        const createdAt = hasExplicitIncludeState ? values[7] : values[6];
        const updatedAt = hasExplicitIncludeState ? values[8] : values[7];
        const existing = this.outputRules.find((row) => row.id === id);
        if (existing) {
          Object.assign(existing, {
            target_id: targetId,
            target_key: targetKey,
            include_state: includeState,
            derived_reason: derivedReason,
            updated_at: updatedAt,
          });
        } else {
          this.outputRules.push({
            id,
            canonical_event_id: canonicalEventId,
            event_instance_id: eventInstanceId,
            target_id: targetId,
            target_key: targetKey,
            include_state: includeState,
            derived_reason: derivedReason,
            created_at: createdAt,
            updated_at: updatedAt,
          });
        }
      } else {
        const [id, canonicalEventId, eventInstanceId, targetId, targetKey, createdAt, updatedAt] = values;
        this.outputRules.push({
          id,
          canonical_event_id: canonicalEventId,
          event_instance_id: eventInstanceId,
          target_id: targetId,
          target_key: targetKey,
          include_state: 'included',
          derived_reason: 'seeded sample data',
          created_at: createdAt,
          updated_at: updatedAt,
        });
      }
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

    if (sql.includes('UPDATE canonical_events') && sql.includes('SET source_icon = ?')) {
      const [sourceIcon, sourcePrefix, updatedAt, sourceId] = values;
      this.canonicalEvents
        .filter((event) => event.source_id === sourceId)
        .forEach((event) => {
          event.source_icon = sourceIcon;
          event.source_prefix = sourcePrefix;
          event.updated_at = updatedAt;
        });
      return;
    }

    if (sql.includes('UPDATE canonical_events SET source_deleted =')) {
      const [updatedAt, sourceId] = values;
      const sourceDeleted = sql.includes('SET source_deleted = 1') ? 1 : 0;
      this.canonicalEvents
        .filter((event) => event.source_id === sourceId)
        .forEach((event) => {
          event.source_deleted = sourceDeleted;
          event.updated_at = updatedAt;
        });
      return;
    }

    if (sql.includes('UPDATE event_instances') && sql.includes('SET source_deleted =')) {
      const [updatedAt, sourceId] = values;
      const sourceDeleted = sql.includes('SET source_deleted = 1') ? 1 : 0;
      const canonicalIds = new Set(this.canonicalEvents.filter((event) => event.source_id === sourceId).map((event) => event.id));
      this.eventInstances
        .filter((instance) => canonicalIds.has(instance.canonical_event_id))
        .forEach((instance) => {
          instance.source_deleted = sourceDeleted;
          instance.updated_at = updatedAt;
        });
      return;
    }

    if (sql.includes('UPDATE source_events SET is_deleted_upstream = 1')) {
      const [lastSeenAt, sourceId] = values;
      this.sourceEvents
        .filter((row) => row.source_id === sourceId)
        .forEach((row) => {
          row.is_deleted_upstream = 1;
          row.last_seen_at = lastSeenAt;
        });
      return;
    }

    if (sql.includes('DELETE FROM output_rules WHERE canonical_event_id IN (SELECT id FROM canonical_events WHERE source_id = ?)')) {
      const [sourceId] = values;
      const canonicalIds = new Set(this.canonicalEvents.filter((event) => event.source_id === sourceId).map((event) => event.id));
      this.outputRules = this.outputRules.filter((row) => !canonicalIds.has(row.canonical_event_id));
      return;
    }

    if (sql.includes('DELETE FROM source_target_links WHERE source_id = ?')) {
      const [sourceId] = values;
      this.sourceTargetLinks = this.sourceTargetLinks.filter((row) => row.source_id !== sourceId);
      return;
    }

    if (sql.includes('UPDATE source_target_links SET target_id = ? WHERE id = ?')) {
      const [targetId, linkId] = values;
      const row = this.sourceTargetLinks.find((link) => link.id === linkId);
      if (row) row.target_id = targetId;
      return;
    }

    if (sql.includes('UPDATE output_rules SET target_id = ? WHERE id = ?')) {
      const [targetId, ruleId] = values;
      const row = this.outputRules.find((rule) => rule.id === ruleId);
      if (row) row.target_id = targetId;
      return;
    }

    if (sql.includes('UPDATE google_event_links SET target_id = ? WHERE id = ?')) {
      const [targetId, linkId] = values;
      const row = this.googleEventLinks.find((link) => link.id === linkId);
      if (row) row.target_id = targetId;
      return;
    }

    if (sql.includes('DELETE FROM source_target_links WHERE target_id = ? OR')) {
      const [targetId, targetKey] = values;
      this.sourceTargetLinks = this.sourceTargetLinks.filter((row) => !(row.target_id === targetId || (!row.target_id && row.target_key === targetKey && row.target_type === 'google')));
      return;
    }

    if (sql.includes('DELETE FROM output_rules WHERE target_id = ? OR')) {
      const [targetId, targetKey] = values;
      this.outputRules = this.outputRules.filter((row) => !(row.target_id === targetId || (!row.target_id && row.target_key === targetKey)));
      return;
    }

    if (sql.includes('DELETE FROM google_event_links WHERE target_id = ? OR')) {
      const [targetId, targetKey] = values;
      this.googleEventLinks = this.googleEventLinks.filter((row) => !(row.target_id === targetId || (!row.target_id && row.target_key === targetKey)));
      return;
    }

    if (sql.includes('INSERT INTO google_event_links')) {
      const [id, targetId, targetKey, canonicalEventId, eventInstanceId, googleEventId, googleEtag, lastSyncedHash, lastSyncedAt, syncStatus, lastError] = values;
      const existing = this.googleEventLinks.find((row) => row.id === id);
      if (existing) {
        Object.assign(existing, {
          target_id: targetId,
          target_key: targetKey,
          canonical_event_id: canonicalEventId,
          event_instance_id: eventInstanceId,
          google_event_id: googleEventId,
          google_etag: googleEtag,
          last_synced_hash: lastSyncedHash,
          last_synced_at: lastSyncedAt,
          sync_status: syncStatus,
          last_error: lastError,
        });
      } else {
        this.googleEventLinks.push({
          id,
          target_id: targetId,
          target_key: targetKey,
          canonical_event_id: canonicalEventId,
          event_instance_id: eventInstanceId,
          google_event_id: googleEventId,
          google_etag: googleEtag,
          last_synced_hash: lastSyncedHash,
          last_synced_at: lastSyncedAt,
          sync_status: syncStatus,
          last_error: lastError,
        });
      }
      return;
    }

    if (sql.includes('DELETE FROM google_event_links WHERE id = ?')) {
      const [linkId] = values;
      this.googleEventLinks = this.googleEventLinks.filter((row) => row.id !== linkId);
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
      return;
    }

    if (sql.includes('DELETE FROM output_targets WHERE id = ?')) {
      const [targetId] = values;
      this.outputTargets = this.outputTargets.filter((row) => row.id !== targetId);
      return;
    }

    if (sql.includes('UPDATE sources SET is_active = 0')) {
      const [updatedAt, sourceId] = values;
      const source = this.sources.find((row) => row.id === sourceId);
      if (source) {
        source.is_active = 0;
        source.updated_at = updatedAt;
      }
      return;
    }

    if (sql.includes('UPDATE sources') && sql.includes('display_name')) {
      const [name, displayName, ownerType, sourceCategory, url, icon, prefix, includeInChildIcs, includeInFamilyIcs, includeInChildGoogleOutput, isActive, sortOrder, pollIntervalMinutes, qualityProfile, updatedAt, sourceId] = values;
      const source = this.sources.find((row) => row.id === sourceId);
      if (source) {
        Object.assign(source, {
          name,
          display_name: displayName,
          owner_type: ownerType,
          source_category: sourceCategory,
          url,
          icon,
          prefix,
          include_in_child_ics: includeInChildIcs,
          include_in_family_ics: includeInFamilyIcs,
          include_in_child_google_output: includeInChildGoogleOutput,
          is_active: isActive,
          sort_order: sortOrder,
          poll_interval_minutes: pollIntervalMinutes,
          quality_profile: qualityProfile,
          updated_at: updatedAt,
        });
      }
      return;
    }
  }

  runAll(sql, values) {
    if (sql.includes('PRAGMA table_info(google_event_links)')) {
      return [
        { name: 'id', notnull: 0 },
        { name: 'google_event_id', notnull: this.googleEventLinkGoogleEventIdNotNull },
        { name: 'target_id', notnull: 0 },
      ];
    }
    if (sql.includes('FROM sources s') && sql.includes('LEFT JOIN (') && sql.includes('last_fetched_at') && sql.includes('WHERE s.is_active = 1')) {
      const now = Date.now();
      return this.sources
        .filter((source) => {
          if (!source.is_active || !source.url) return false;
          const snapshots = this.sourceSnapshots
            .filter((row) => row.source_id === source.id)
            .sort((a, b) => String(b.fetched_at).localeCompare(String(a.fetched_at)));
          const latest = snapshots[0];
          if (!latest?.fetched_at) return true;
          const lastFetchedAt = new Date(latest.fetched_at).getTime();
          if (!Number.isFinite(lastFetchedAt)) return true;
          return lastFetchedAt <= now - Number(source.poll_interval_minutes || 30) * 60 * 1000;
        })
        .sort((a, b) => String(a.owner_type).localeCompare(String(b.owner_type)) || Number(a.sort_order || 0) - Number(b.sort_order || 0) || String(a.name).localeCompare(String(b.name)));
    }
    if (sql.includes('FROM sources') && sql.includes('ORDER BY') && sql.includes('owner_type')) return this.sources;
    if (sql.includes('FROM source_target_links') && sql.includes('WHERE source_id = ?')) {
      return this.sourceTargetLinks
        .filter((row) => row.source_id === values[0] && row.is_enabled === 1)
        .sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0) || String(a.target_key).localeCompare(String(b.target_key)))
        .map((row) => {
          const target = this.outputTargets.find((entry) => entry.id === row.target_id);
          return {
            ...row,
            target_key: target?.slug || row.target_key,
            target_type: target?.target_type || row.target_type,
            display_name: target?.display_name || row.target_key,
          };
        });
    }
    if (sql.includes('FROM source_target_links') && sql.includes('WHERE is_enabled = 1')) {
      return this.sourceTargetLinks
        .filter((row) => row.is_enabled === 1)
        .map((row) => {
          const target = this.outputTargets.find((entry) => entry.id === row.target_id);
          return {
            ...row,
            target_key: target?.slug || row.target_key,
            target_type: target?.target_type || row.target_type,
            display_name: target?.display_name || row.target_key,
          };
        });
    }
    if (sql.includes('FROM sync_jobs ORDER BY')) return this.syncJobs.slice(0, values[0] || 50);
    if (sql.includes('FROM source_target_links') && sql.includes('DISTINCT output_targets.id')) {
      return this.sourceTargetLinks
        .filter((row) => row.source_id === values[0] && row.is_enabled === 1)
        .map((row) => {
          const target = this.outputTargets.find((entry) => entry.id === row.target_id);
          return target && target.target_type === 'google' && target.is_active ? {
            id: target.id,
            slug: target.slug,
            display_name: target.display_name,
          } : null;
        })
        .filter(Boolean);
    }
    if (sql.includes('FROM sync_jobs') && sql.includes("scope_type IN ('source', 'source_target')")) {
      return this.syncJobs.map((job) => ({
        scope_type: job.scope_type,
        scope_id: job.scope_id,
        job_type: job.job_type,
        status: job.status,
        started_at: job.started_at,
        finished_at: job.finished_at,
        summary_json: job.summary_json || null,
        error_json: job.error_json || null,
      }));
    }
    if (sql.includes('FROM output_targets')) {
      let rows = [...this.outputTargets];
      if (sql.includes('WHERE slug = ?')) rows = rows.filter((row) => row.slug === values[0]);
      return rows;
    }
    if (sql.includes('FROM event_instances') && sql.includes('JOIN canonical_events ON canonical_events.id = event_instances.canonical_event_id')) {
      return this.eventInstances
        .filter((instance) => {
          const event = this.canonicalEvents.find((row) => row.id === instance.canonical_event_id);
          return event && event.source_id === values[0] && !event.source_deleted && !instance.source_deleted;
        })
        .map((instance) => ({
          id: instance.id,
          canonical_event_id: instance.canonical_event_id,
        }));
    }
    if (sql.includes('FROM canonical_events') && sql.includes('GROUP BY canonical_events.id')) {
      const target = values.length > 1 ? values[0] : null;
      const rows = this.canonicalEvents
        .filter((event) => !target || this.outputRules.some((rule) => {
          const outputTarget = this.outputTargets.find((entry) => entry.id === rule.target_id);
          const slug = outputTarget?.slug || rule.target_key;
          return rule.canonical_event_id === event.id && slug === target && rule.include_state === 'included';
        }))
        .map((event) => {
          const source = this.sources.find((row) => row.id === event.source_id);
          const firstInstance = this.eventInstances.find((instance) => instance.canonical_event_id === event.id);
          return {
            id: event.id,
            title: event.title,
            source_icon: event.source_icon,
            source_prefix: event.source_prefix,
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
    if (sql.includes('FROM event_instances') && sql.includes('canonical_event_id = ?')) {
      return this.eventInstances.filter((row) => row.canonical_event_id === values[0] && (!values[1] || row.id === values[1]));
    }
    if (sql.includes('FROM event_overrides')) {
      return this.eventOverrides.filter((row) => row.canonical_event_id === values[0] && !row.cleared_at);
    }
    if (sql.includes('FROM output_rules WHERE canonical_event_id')) {
      return this.outputRules
        .filter((row) => row.canonical_event_id === values[0])
        .map((row) => ({
          target_key: this.outputTargets.find((target) => target.id === row.target_id)?.slug || row.target_key,
          include_state: row.include_state,
          derived_reason: row.derived_reason,
        }));
    }
    if (sql.includes('FROM google_event_links WHERE canonical_event_id')) {
      return this.googleEventLinks
        .filter((row) => row.canonical_event_id === values[0])
        .map((row) => ({
          ...row,
          target_key: this.outputTargets.find((target) => target.id === row.target_id)?.slug || row.target_key,
        }));
    }
    if (sql.includes('FROM google_event_links') && sql.includes('GROUP BY canonical_events.source_id, google_event_links.last_error')) {
      const grouped = new Map();
      for (const row of this.googleEventLinks.filter((entry) => entry.sync_status === 'error')) {
        const event = this.canonicalEvents.find((candidate) => candidate.id === row.canonical_event_id);
        if (!event) continue;
        const key = `${event.source_id}:${row.last_error || ''}`;
        const current = grouped.get(key);
        if (current) {
          current.error_count += 1;
          if (String(row.last_synced_at || '') > String(current.last_synced_at || '')) {
            current.last_synced_at = row.last_synced_at || null;
          }
        } else {
          grouped.set(key, {
            source_id: event.source_id,
            error_count: 1,
            last_synced_at: row.last_synced_at || null,
            last_error: row.last_error || null,
          });
        }
      }
      return Array.from(grouped.values());
    }
    if (
      sql.includes('COUNT(output_rules.id) AS desired_count') &&
      sql.includes("google_event_links.sync_status = 'synced'") &&
      sql.includes('FROM output_rules')
    ) {
      const grouped = new Map();
      for (const rule of this.outputRules) {
        const event = this.canonicalEvents.find((candidate) => candidate.id === rule.canonical_event_id);
        if (!event) continue;
        const target = this.outputTargets.find((entry) => entry.id === rule.target_id);
        if (!target || target.target_type !== 'google' || rule.include_state !== 'included' || event.source_deleted) continue;
        const current = grouped.get(event.source_id) || {
          source_id: event.source_id,
          desired_count: 0,
          synced_count: 0,
          error_count: 0,
        };
        current.desired_count += 1;
        const link = this.googleEventLinks.find((entry) => entry.canonical_event_id === rule.canonical_event_id && entry.event_instance_id === rule.event_instance_id && ((entry.target_id && entry.target_id === rule.target_id) || entry.target_key === rule.target_key));
        if (link?.sync_status === 'synced' && link.google_event_id) {
          current.synced_count += 1;
        } else if (link?.sync_status === 'error' && !String(link.last_error || '').startsWith('Deferred after rate limit:')) {
          current.error_count += 1;
        }
        grouped.set(event.source_id, current);
      }
      return Array.from(grouped.values());
    }
    if (sql.includes('FROM google_event_links') && sql.includes('JOIN canonical_events ON canonical_events.id = google_event_links.canonical_event_id')) {
      return this.googleEventLinks
        .filter((row) => {
          const event = this.canonicalEvents.find((candidate) => candidate.id === row.canonical_event_id);
          if (!event || event.source_id !== values[0]) return false;
          return values[1] ? row.target_id === values[1] : true;
        })
        .map((row) => {
          const target = this.outputTargets.find((entry) => entry.id === row.target_id);
          return {
            ...row,
            target_key: target?.slug || row.target_key,
            calendar_id: target?.calendar_id || null,
          };
        });
    }
    if (sql.includes('FROM output_rules') && sql.includes('JOIN canonical_events')) {
      if (sql.includes('output_targets.display_name') && sql.includes("output_targets.target_type = 'google'")) {
        return this.outputRules
          .filter((rule) => {
            const event = this.canonicalEvents.find((row) => row.id === rule.canonical_event_id);
            const instance = this.eventInstances.find((row) => row.id === rule.event_instance_id);
            const target = this.outputTargets.find((entry) => entry.id === rule.target_id);
            const cutoff = values[1];
            const targetId = values.length > 2 ? values[2] : null;
            return event &&
              instance &&
              target &&
              target.target_type === 'google' &&
              event.source_id === values[0] &&
              !event.source_deleted &&
              !instance.source_deleted &&
              rule.include_state === 'included' &&
              (!cutoff || String(instance.occurrence_end_at || '') >= String(cutoff)) &&
              (!targetId || rule.target_id === targetId);
          })
          .map((rule) => {
            const event = this.canonicalEvents.find((row) => row.id === rule.canonical_event_id);
            const instance = this.eventInstances.find((row) => row.id === rule.event_instance_id);
            const source = this.sources.find((row) => row.id === event.source_id);
            const target = this.outputTargets.find((entry) => entry.id === rule.target_id);
            const targetLink = this.sourceTargetLinks.find((row) => row.source_id === event.source_id && ((row.target_id && row.target_id === rule.target_id) || row.target_key === target?.slug) && row.is_enabled === 1);
            return {
              output_rule_id: rule.id,
              target_id: rule.target_id,
              target_key: target.slug,
              display_name: target.display_name,
              calendar_id: target.calendar_id,
              canonical_event_id: event.id,
              title: event.title,
              description: event.description,
              location: event.location,
              status: event.status,
              timezone: event.timezone,
              source_icon: targetLink?.icon || event.source_icon || source?.icon || '',
              source_prefix: targetLink?.prefix || event.source_prefix || source?.prefix || '',
              event_instance_id: instance.id,
              occurrence_start_at: instance.occurrence_start_at,
              occurrence_end_at: instance.occurrence_end_at,
            };
          });
      }
      const target = values[0];
      return this.outputRules
        .filter((row) => {
          const outputTarget = this.outputTargets.find((entry) => entry.id === row.target_id);
          const slug = outputTarget?.slug || row.target_key;
          return slug === target && row.include_state === 'included';
        })
        .map((rule) => {
          const event = this.canonicalEvents.find((row) => row.id === rule.canonical_event_id);
          const instance = this.eventInstances.find((row) => row.id === rule.event_instance_id);
          if (!event || !instance || event.source_deleted || instance.source_deleted) return null;
          const source = this.sources.find((row) => row.id === event.source_id);
          const targetLink = this.sourceTargetLinks.find((row) => row.source_id === event.source_id && ((row.target_id && row.target_id === rule.target_id) || row.target_key === target) && row.is_enabled === 1);
          return {
            canonical_event_id: event.id,
            title: event.title,
            source_icon: targetLink?.icon || event.source_icon || source?.icon || '',
            source_prefix: targetLink?.prefix || event.source_prefix || source?.prefix || '',
            description: event.description,
            location: event.location,
            status: event.status,
            event_instance_id: instance.id,
            occurrence_start_at: instance.occurrence_start_at,
            occurrence_end_at: instance.occurrence_end_at,
          };
        })
        .filter(Boolean);
    }
    return [];
  }

  runFirst(sql, values) {
    if (sql.includes('SELECT COUNT(*) AS count FROM canonical_events')) {
      return { count: this.canonicalEvents.length };
    }
    if (sql.includes('SELECT COUNT(*) AS count FROM sources')) {
      return { count: this.sources.length };
    }
    if (sql.includes('SELECT canonical_events.id, canonical_events.source_id')) {
      const event = this.canonicalEvents.find((row) => row.id === values[0]);
      return event ? { id: event.id, source_id: event.source_id } : null;
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
    if (sql.includes('SELECT canonical_event_id, event_instance_id FROM event_overrides')) {
      const override = this.eventOverrides.find((row) => row.id === values[0]);
      return override ? { canonical_event_id: override.canonical_event_id, event_instance_id: override.event_instance_id } : null;
    }
    if (sql.includes('SELECT canonical_event_id FROM event_overrides')) {
      const override = this.eventOverrides.find((row) => row.id === values[0]);
      return override ? { canonical_event_id: override.canonical_event_id } : null;
    }
    if (sql.includes('SELECT * FROM sources WHERE id = ?')) {
      return this.sources.find((row) => row.id === values[0]) || null;
    }
    if (sql.includes('FROM output_targets') && sql.includes('WHERE slug = ?')) {
      return this.outputTargets.find((row) => row.slug === values[0]) || null;
    }
    if (sql.includes('FROM output_targets') && sql.includes('WHERE id = ?')) {
      return this.outputTargets.find((row) => row.id === values[0]) || null;
    }
    return null;
  }
}

beforeEach(() => {
  env.TOKEN = 'test-token';
  env.CALENDAR_NAME_FAMILY = 'Family Combined';
  env.CALENDAR_NAME_GRAYSON = 'Grayson Combined';
  env.CALENDAR_NAME_NAOMI = 'Naomi Combined';
  env.ADMIN_EMAILS = 'lance.long@gmail.com';
  env.EDITOR_EMAILS = 'lance.long@gmail.com,nursesmiff@gmail.com';
  env.ALLOW_DEV_ROLE_HEADER = 'true';
  env.SEED_SAMPLE_DATA = 'true';
  env.APP_DB = new FakeDb();
  env.ASSETS = {
    fetch: vi.fn(async (request) => {
      const url = new URL(request.url);
      if (url.pathname === '/admin.html') {
        return new Response('<!doctype html><html><body><h1>Admin Console</h1></body></html>', {
          headers: { 'content-type': 'text/html; charset=utf-8' },
        });
      }
      if (url.pathname === '/admin-events.html') {
        return new Response('<!doctype html><html><body><h1>Modify Events</h1></body></html>', {
          headers: { 'content-type': 'text/html; charset=utf-8' },
        });
      }
      return new Response('Not found', { status: 404 });
    }),
  };
  env.JOBS_QUEUE = {
    sent: [],
    send: vi.fn(async (body) => {
      env.JOBS_QUEUE.sent.push(body);
    }),
  };
  clearAuthCachesForTest();
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe('family-scheduling worker', () => {
  it('repairs legacy google_event_links schema to allow null google_event_id', async () => {
    const db = new FakeDb();
    db.googleEventLinkGoogleEventIdNotNull = 1;
    db.googleEventLinks = [
      {
        id: 'gel_1',
        target_key: 'grayson_clubs',
        canonical_event_id: 'ce_1',
        event_instance_id: 'ei_1',
        google_event_id: 'abc123',
        google_etag: null,
        last_synced_hash: null,
        last_synced_at: null,
        sync_status: 'synced',
        last_error: null,
        target_id: 'outt_1',
      },
    ];
    const repo = new D1Repository(db, env);

    await repo.ensureSupportTables();

    expect(db.googleEventLinkGoogleEventIdNotNull).toBe(0);
    expect(db.googleEventLinks).toHaveLength(1);
    expect(db.googleEventLinks[0].google_event_id).toBe('abc123');
  });

  it('enriches sources with latest job and google sync error details', async () => {
    const db = new FakeDb();
    db.sources.push({
      id: 'src_google_status',
      name: 'Grayson Kings',
      display_name: 'Grayson Kings',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/grayson.ics',
      icon: '🏒',
      prefix: '',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      updated_at: '2026-03-08T15:44:00.000Z',
    });
    db.sourceSnapshots.push({
      id: 'snap_1',
      source_id: 'src_google_status',
      fetched_at: '2026-03-08T15:44:11.507Z',
      http_status: 200,
      etag: null,
      last_modified: null,
      payload_blob_ref: null,
      parse_status: 'parsed',
      parse_error_summary: null,
    });
    db.syncJobs.push({
      id: 'job_1',
      job_type: 'rebuild_source',
      scope_type: 'source',
      scope_id: 'src_google_status',
      status: 'completed',
      started_at: '2026-03-08T15:44:04.510Z',
      finished_at: '2026-03-08T15:44:16.420Z',
      summary_json: JSON.stringify({ googleSync: { failed: 127 } }),
      error_json: null,
    });
    db.canonicalEvents.push({
      id: 'ce_1',
      source_id: 'src_google_status',
      identity_key: 'id_1',
      event_kind: 'single',
      title: 'Practice',
      source_icon: '🏒',
      source_prefix: '',
      description: '',
      location: '',
      start_at: '2026-03-10T01:00:00.000Z',
      end_at: '2026-03-10T02:00:00.000Z',
      timezone: 'UTC',
      status: 'confirmed',
      rrule: null,
      series_until: null,
      source_deleted: 0,
      needs_review: 0,
      source_changed_since_overlay: 0,
      last_source_change_at: '2026-03-08T15:44:00.000Z',
      created_at: '2026-03-08T15:44:00.000Z',
      updated_at: '2026-03-08T15:44:00.000Z',
    });
    db.eventInstances.push({
      id: 'ei_1',
      canonical_event_id: 'ce_1',
      occurrence_start_at: '2026-03-10T01:00:00.000Z',
      occurrence_end_at: '2026-03-10T02:00:00.000Z',
      recurrence_instance_key: '2026-03-10T01:00:00.000Z',
      provider_recurrence_id: null,
      status: 'confirmed',
      source_deleted: 0,
      needs_review: 0,
      created_at: '2026-03-08T15:44:00.000Z',
      updated_at: '2026-03-08T15:44:00.000Z',
    });
    db.outputTargets.push({
      id: 'outt_grayson_clubs',
      target_type: 'google',
      slug: 'grayson_clubs',
      display_name: 'Grayson Clubs',
      calendar_id: 'grayson-clubs@group.calendar.google.com',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-08T15:44:00.000Z',
      updated_at: '2026-03-08T15:44:00.000Z',
    });
    db.outputRules.push({
      id: 'out_1',
      canonical_event_id: 'ce_1',
      event_instance_id: 'ei_1',
      target_id: 'outt_grayson_clubs',
      target_key: 'grayson_clubs',
      include_state: 'included',
      derived_reason: 'derived from grayson source',
      created_at: '2026-03-08T15:44:00.000Z',
      updated_at: '2026-03-08T15:44:00.000Z',
    });
    db.googleEventLinks.push({
      id: 'gel_1',
      target_id: 'outt_grayson_clubs',
      target_key: 'grayson_clubs',
      canonical_event_id: 'ce_1',
      event_instance_id: 'ei_1',
      google_event_id: null,
      google_etag: null,
      last_synced_hash: null,
      last_synced_at: '2026-03-08T15:44:13.031Z',
      sync_status: 'error',
      last_error: 'Google Calendar API is disabled.',
    });

    const repo = new D1Repository(db, env);
    const [source] = await repo.listSources();

    expect(source.latest_job?.status).toBe('completed');
    expect(source.latest_job?.summary?.googleSync?.failed).toBe(127);
    expect(source.latest_google_sync_error?.message).toContain('disabled');
    expect(source.latest_google_sync_error?.error_count).toBe(1);
    expect(source.google_sync_counts).toEqual({ synced: 0, deferred: 0, errors: 1 });
  });

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
    expect(body).toContain('SUMMARY:N: 🏀 Basketball Game');
  });

  it('serves admin shell on /admin', async () => {
    const request = new Request('http://example.com/admin', { headers: { 'x-user-role': 'admin' } });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.text();
    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('text/html');
    expect(body).toContain('Admin Console');
    expect(env.ASSETS.fetch).toHaveBeenCalledTimes(1);
  });

  it('serves the focused event admin shell on /admin/events', async () => {
    const request = new Request('http://example.com/admin/events', { headers: { 'x-user-role': 'editor' } });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.text();
    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('text/html');
    expect(body).toContain('Modify Events');
  });

  it('redirects the root path to /admin', async () => {
    const request = new Request('http://example.com/');
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    expect(response.status).toBe(302);
    expect(response.headers.get('location')).toBe('http://example.com/admin');
  });

  it('renders child feeds with icon but without family prefix', async () => {
    const request = new Request('http://example.com/naomi.isc?token=test-token');
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.text();
    expect(response.status).toBe(200);
    expect(body).toContain('SUMMARY:🏀 Basketball Game');
    expect(body).not.toContain('SUMMARY:N: 🏀 Basketball Game');
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

  it('allows editor access via validated Cloudflare Access JWT', async () => {
    env.ALLOW_DEV_ROLE_HEADER = 'false';
    env.CLOUDFLARE_ACCESS_AUD = 'test-access-aud';
    env.CLOUDFLARE_ACCESS_TEAM_DOMAIN = 'https://spry.cloudflareaccess.com';

    const keyPair = await crypto.subtle.generateKey(
      {
        name: 'RSASSA-PKCS1-v1_5',
        modulusLength: 2048,
        publicExponent: new Uint8Array([1, 0, 1]),
        hash: 'SHA-256',
      },
      true,
      ['sign', 'verify']
    );
    const publicJwk = await crypto.subtle.exportKey('jwk', keyPair.publicKey);
    const now = Math.floor(Date.now() / 1000);
    const token = await createAccessJwt({
      privateKey: keyPair.privateKey,
      kid: 'test-key-1',
      payload: {
        aud: ['test-access-aud'],
        iss: 'https://spry.cloudflareaccess.com',
        email: 'nursesmiff@gmail.com',
        exp: now + 60,
        nbf: now - 60,
      },
    });

    vi.stubGlobal(
      'fetch',
      vi.fn(async (url) => {
        if (String(url) === 'https://spry.cloudflareaccess.com/cdn-cgi/access/certs') {
          return new Response(
            JSON.stringify({
              keys: [{ ...publicJwk, kid: 'test-key-1', kty: 'RSA' }],
            }),
            { status: 200, headers: { 'content-type': 'application/json' } }
          );
        }
        throw new Error(`Unexpected fetch URL: ${url}`);
      })
    );

    const request = new Request('http://example.com/api/events', {
      headers: { 'Cf-Access-Jwt-Assertion': token },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    expect(response.status).toBe(200);

    const adminOnlyRequest = new Request('http://example.com/api/rebuild/full', {
      method: 'POST',
      headers: { 'Cf-Access-Jwt-Assertion': token },
    });
    const adminCtx = createExecutionContext();
    const adminResponse = await worker.fetch(adminOnlyRequest, env, adminCtx);
    await waitOnExecutionContext(adminCtx);
    expect(adminResponse.status).toBe(403);
  });

  it('rejects invalid Cloudflare Access JWT on protected routes', async () => {
    env.ALLOW_DEV_ROLE_HEADER = 'false';
    env.CLOUDFLARE_ACCESS_AUD = 'test-access-aud';
    env.CLOUDFLARE_ACCESS_TEAM_DOMAIN = 'https://spry.cloudflareaccess.com';

    const keyPair = await crypto.subtle.generateKey(
      {
        name: 'RSASSA-PKCS1-v1_5',
        modulusLength: 2048,
        publicExponent: new Uint8Array([1, 0, 1]),
        hash: 'SHA-256',
      },
      true,
      ['sign', 'verify']
    );
    const publicJwk = await crypto.subtle.exportKey('jwk', keyPair.publicKey);
    const now = Math.floor(Date.now() / 1000);
    const token = await createAccessJwt({
      privateKey: keyPair.privateKey,
      kid: 'test-key-2',
      payload: {
        aud: ['wrong-audience'],
        iss: 'https://spry.cloudflareaccess.com',
        email: 'lance.long@gmail.com',
        exp: now + 60,
        nbf: now - 60,
      },
    });

    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(
          JSON.stringify({
            keys: [{ ...publicJwk, kid: 'test-key-2', kty: 'RSA' }],
          }),
          { status: 200, headers: { 'content-type': 'application/json' } }
        )
      )
    );

    const request = new Request('http://example.com/api/events', {
      headers: { 'Cf-Access-Jwt-Assertion': token },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    expect(response.status).toBe(401);
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
    expect(body.overrides[0].created_by).toBe('editor');
  });

  it('creates and updates sources through the admin API with per-target rules', async () => {
    env.APP_DB.outputTargets.push({
      id: 'outt_naomi_clubs',
      target_type: 'google',
      slug: 'naomi_clubs',
      display_name: 'Naomi Clubs',
      calendar_id: 'naomi-clubs@example.test',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-06T00:00:00.000Z',
      updated_at: '2026-03-06T00:00:00.000Z',
    });
    const createRequest = new Request('http://example.com/api/sources', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-user-role': 'admin',
      },
      body: JSON.stringify({
        owner_type: 'naomi',
        display_name: 'Naomi Volleyball',
        url: 'http://example.com/naomi-volleyball.ics',
        include_in_child_ics: true,
        include_in_family_ics: true,
        include_in_child_google_output: true,
        target_links: [
          { target_key: 'naomi', icon: '🏐', prefix: '' },
          { target_key: 'family', icon: '🏐', prefix: 'N:' },
          { target_key: 'naomi_clubs', icon: '🏐', prefix: 'Clubs:' },
        ],
      }),
    });
    const ctx = createExecutionContext();
    const createResponse = await worker.fetch(createRequest, env, ctx);
    await waitOnExecutionContext(ctx);
    const created = await createResponse.json();
    expect(createResponse.status).toBe(201);
    expect(created.source.url).toBe('https://example.com/naomi-volleyball.ics');
    expect(env.APP_DB.sourceTargetLinks.filter((row) => row.source_id === created.source.id)).toHaveLength(3);
    expect(created.source.target_links.every((link) => typeof link.target_id === 'string' && link.target_id.length > 0)).toBe(true);

    const patchRequest = new Request(`http://example.com/api/sources/${created.source.id}`, {
      method: 'PATCH',
      headers: {
        'content-type': 'application/json',
        'x-user-role': 'admin',
      },
      body: JSON.stringify({
        target_links: [
          { target_key: 'naomi', icon: '🏆', prefix: '' },
          { target_key: 'family', icon: '🏆', prefix: 'N:' },
        ],
      }),
    });
    const patchCtx = createExecutionContext();
    const patchResponse = await worker.fetch(patchRequest, env, patchCtx);
    await waitOnExecutionContext(patchCtx);
    const updated = await patchResponse.json();
    expect(patchResponse.status).toBe(200);
    const updatedLinks = env.APP_DB.sourceTargetLinks.filter((row) => row.source_id === created.source.id);
    expect(updatedLinks).toHaveLength(2);
    expect(updatedLinks.find((row) => row.target_key === 'naomi')?.icon).toBe('🏆');
    expect(updatedLinks.some((row) => row.target_key === 'naomi_clubs')).toBe(false);
  });

  it('creates google targets through admin API', async () => {
    const request = new Request('http://example.com/api/targets', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-user-role': 'admin',
      },
      body: JSON.stringify({
        target_key: 'grayson_clubs',
        calendar_id: 'grayson-clubs@example.test',
        ownership_mode: 'managed_output',
      }),
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(201);
    expect(body.target.target_key).toBe('grayson_clubs');
    expect(body.target.calendar_id).toBe('grayson-clubs@example.test');
    expect(body.target.target_type).toBe('google');
  });

  it('lists unified output targets with system ics outputs and google outputs', async () => {
    const createRequest = new Request('http://example.com/api/targets', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-user-role': 'admin',
      },
      body: JSON.stringify({
        display_name: 'Naomi Clubs',
        calendar_id: 'naomi-clubs@example.test',
        ownership_mode: 'managed_output',
      }),
    });
    const createCtx = createExecutionContext();
    const createResponse = await worker.fetch(createRequest, env, createCtx);
    await waitOnExecutionContext(createCtx);
    expect(createResponse.status).toBe(201);

    const request = new Request('http://example.com/api/targets', {
      headers: { 'x-user-role': 'admin' },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(200);
    expect(body.targets.some((target) => target.target_type === 'ics' && target.slug === 'family')).toBe(true);
    expect(body.targets.some((target) => target.target_type === 'google' && target.slug === 'naomi_clubs')).toBe(true);
  });

  it('rejects google targets that collide with built-in ics feed keys', async () => {
    const request = new Request('http://example.com/api/targets', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-user-role': 'admin',
      },
      body: JSON.stringify({
        target_key: 'grayson',
        calendar_id: 'grayson@example.test',
        ownership_mode: 'managed_output',
      }),
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(400);
    expect(body.message).toContain('reserved for built-in ICS feeds');
  });

  it('deletes google targets through admin API', async () => {
    env.APP_DB.outputTargets.push({
      id: 'outt_naomi_clubs',
      target_type: 'google',
      slug: 'naomi_clubs',
      display_name: 'Naomi Clubs',
      calendar_id: 'naomi-clubs@example.test',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-06T00:00:00.000Z',
      updated_at: '2026-03-06T00:00:00.000Z',
    });
    env.APP_DB.sourceTargetLinks.push({
      id: 'stl_naomi_clubs',
      source_id: 'src_seed',
      target_id: 'outt_naomi_clubs',
      target_key: 'naomi_clubs',
      target_type: 'google',
      icon: '🏐',
      prefix: '',
      sort_order: 0,
      is_enabled: 1,
      created_at: '2026-03-06T00:00:00.000Z',
      updated_at: '2026-03-06T00:00:00.000Z',
    });
    env.APP_DB.outputRules.push({
      id: 'out_naomi_clubs',
      canonical_event_id: 'evt_naomi_basketball',
      event_instance_id: 'evt_naomi_basketball_inst1',
      target_id: 'outt_naomi_clubs',
      target_key: 'naomi_clubs',
      include_state: 'included',
      derived_reason: 'test',
      created_at: '2026-03-06T00:00:00.000Z',
      updated_at: '2026-03-06T00:00:00.000Z',
    });
    const request = new Request('http://example.com/api/targets/naomi_clubs', {
      method: 'DELETE',
      headers: { 'x-user-role': 'admin' },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(200);
    expect(body.deleted.target_key).toBe('naomi_clubs');
    expect(env.APP_DB.outputTargets.some((row) => row.slug === 'naomi_clubs')).toBe(false);
    expect(env.APP_DB.sourceTargetLinks.some((row) => row.target_key === 'naomi_clubs')).toBe(false);
    expect(env.APP_DB.outputRules.some((row) => row.target_key === 'naomi_clubs')).toBe(false);
  });

  it('rejects deletion of system output targets', async () => {
    const request = new Request('http://example.com/api/targets/family', {
      method: 'DELETE',
      headers: { 'x-user-role': 'admin' },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(400);
    expect(body.message).toContain('Cannot delete system output target');
  });

  it('disables sources and removes their output rules', async () => {
    const listRequest = new Request('http://example.com/api/sources', {
      headers: { 'x-user-role': 'admin' },
    });
    const listCtx = createExecutionContext();
    const listResponse = await worker.fetch(listRequest, env, listCtx);
    await waitOnExecutionContext(listCtx);
    const { sources } = await listResponse.json();
    const sourceId = sources.find((row) => row.owner_type === 'naomi').id;
    env.APP_DB.sourceEvents.push({
      id: 'sev_disable_test',
      source_id: sourceId,
      provider_uid: 'seed-uid',
      provider_recurrence_id: null,
      provider_etag: null,
      raw_hash: 'hash',
      first_seen_at: '2026-03-04T00:00:00.000Z',
      last_seen_at: '2026-03-04T00:00:00.000Z',
      is_deleted_upstream: 0,
    });
    const deleteRequest = new Request(`http://example.com/api/sources/${sourceId}`, {
      method: 'DELETE',
      headers: { 'x-user-role': 'admin' },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(deleteRequest, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(200);
    expect(body.source.is_active).toBe(0);
    expect(env.APP_DB.outputRules.some((row) => row.target_key === 'naomi')).toBe(false);
    expect(env.APP_DB.sourceEvents.filter((row) => row.source_id === sourceId).every((row) => row.is_deleted_upstream === 1)).toBe(true);
  });

  it('ingests a source from its stored url and source metadata', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_custom_naomi',
      name: 'naomi-volleyball',
      display_name: 'Naomi Volleyball',
      provider_type: 'ics',
      owner_type: 'naomi',
      source_category: 'sports',
      url: 'https://example.com/naomi-volleyball.ics',
      icon: '🏐',
      prefix: 'N:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    vi.stubGlobal(
      'fetch',
      vi.fn(async (url) =>
        new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:naomi-vball-1',
            'SUMMARY:Volleyball Game',
            'DTSTART:20260310T010000Z',
            'DTEND:20260310T023000Z',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          {
            status: 200,
            headers: {
              etag: 'abc123',
              'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT',
            },
          }
        )
      )
    );

    const repo = new D1Repository(db, env);
    const result = await repo.ingestSource('src_custom_naomi');
    expect(result.urlsFetched).toBe(1);
    expect(global.fetch).toHaveBeenCalledWith('https://example.com/naomi-volleyball.ics', { cf: { cacheTtl: 0 } });

    const familyFeed = await repo.generateFeed({
      target: 'family',
      calendarName: 'Family Combined',
      lookbackDays: 30,
    });
    const naomiFeed = await repo.generateFeed({
      target: 'naomi',
      calendarName: 'Naomi Combined',
      lookbackDays: 30,
    });

    expect(familyFeed).toContain('SUMMARY:N: 🏐 Volleyball Game');
    expect(naomiFeed).toContain('SUMMARY:🏐 Volleyball Game');
  });

  it('applies fallback sport icon detection when source icon is not configured', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_custom_naomi_no_icon',
      name: 'naomi-school-events',
      display_name: 'Naomi School Events',
      provider_type: 'ics',
      owner_type: 'naomi',
      source_category: 'general',
      url: 'https://example.com/naomi-school-events.ics',
      icon: '',
      prefix: 'N:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:naomi-bball-1',
            'SUMMARY:Basketball Practice',
            'DTSTART:20260310T010000Z',
            'DTEND:20260310T023000Z',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          {
            status: 200,
            headers: {
              etag: 'abc123',
              'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT',
            },
          }
        )
      )
    );

    const repo = new D1Repository(db, env);
    await repo.ingestSource('src_custom_naomi_no_icon');

    const familyFeed = await repo.generateFeed({
      target: 'family',
      calendarName: 'Family Combined',
      lookbackDays: 30,
    });
    const naomiFeed = await repo.generateFeed({
      target: 'naomi',
      calendarName: 'Naomi Combined',
      lookbackDays: 30,
    });

    expect(familyFeed).toContain('SUMMARY:N: 🏀 Basketball Practice');
    expect(naomiFeed).toContain('SUMMARY:🏀 Basketball Practice');
  });

  it('renders per-target rule decoration from source_target_links', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_rule_test',
      name: 'grayson-hockey',
      display_name: 'Grayson Hockey',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/grayson-hockey.ics',
      icon: '',
      prefix: '',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sourceTargetLinks.push(
      {
        id: 'stl_grayson',
        source_id: 'src_rule_test',
        target_key: 'grayson',
        target_type: 'ics',
        icon: '🏒',
        prefix: '',
        sort_order: 0,
        is_enabled: 1,
        created_at: '2026-03-03T00:00:00.000Z',
        updated_at: '2026-03-03T00:00:00.000Z',
      },
      {
        id: 'stl_family',
        source_id: 'src_rule_test',
        target_key: 'family',
        target_type: 'ics',
        icon: '🏒',
        prefix: 'G:',
        sort_order: 1,
        is_enabled: 1,
        created_at: '2026-03-03T00:00:00.000Z',
        updated_at: '2026-03-03T00:00:00.000Z',
      }
    );

    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:grayson-hockey-1',
            'SUMMARY:Hockey Practice',
            'DTSTART:20260310T010000Z',
            'DTEND:20260310T023000Z',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        )
      )
    );

    const repo = new D1Repository(db, env);
    await repo.ingestSource('src_rule_test');
    const familyFeed = await repo.generateFeed({ target: 'family', calendarName: 'Family Combined', lookbackDays: 30 });
    const graysonFeed = await repo.generateFeed({ target: 'grayson', calendarName: 'Grayson Combined', lookbackDays: 30 });

    expect(familyFeed).toContain('SUMMARY:G: 🏒 Hockey Practice');
    expect(graysonFeed).toContain('SUMMARY:🏒 Hockey Practice');
  });

  it('remaps RECURRENCE-ID updates onto the existing series instance', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_recurrence_test',
      name: 'grayson-recurring',
      display_name: 'Grayson Recurring',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/grayson-recurring.ics',
      icon: '🏒',
      prefix: 'G:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:grayson-series-1',
            'SUMMARY:Hockey Practice',
            'DTSTART:20260310T010000Z',
            'DTEND:20260310T020000Z',
            'RRULE:FREQ=DAILY;COUNT=2',
            'END:VEVENT',
            'BEGIN:VEVENT',
            'UID:grayson-series-1',
            'RECURRENCE-ID:20260311T010000Z',
            'SUMMARY:Hockey Practice - Moved',
            'DTSTART:20260311T030000Z',
            'DTEND:20260311T040000Z',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        )
      )
    );

    const repo = new D1Repository(db, env);
    await repo.ingestSource('src_recurrence_test');

    expect(db.canonicalEvents).toHaveLength(1);
    expect(db.eventInstances).toHaveLength(2);
    expect(db.eventInstances.some((instance) => instance.recurrence_instance_key === '2026-03-11T01:00:00.000Z' && instance.occurrence_start_at === '2026-03-11T03:00:00.000Z')).toBe(true);
  });

  it('omits disabled source events from generated feeds', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_disabled_feed',
      name: 'grayson-disabled',
      display_name: 'Grayson Disabled',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/grayson-disabled.ics',
      icon: '🏒',
      prefix: 'G:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:grayson-disabled-1',
            'SUMMARY:Hockey Practice',
            'DTSTART:20260310T010000Z',
            'DTEND:20260310T023000Z',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        )
      )
    );

    const repo = new D1Repository(db, env);
    await repo.ingestSource('src_disabled_feed');
    await repo.disableSource('src_disabled_feed');

    const familyFeed = await repo.generateFeed({ target: 'family', calendarName: 'Family Combined', lookbackDays: 30 });
    expect(familyFeed).not.toContain('Hockey Practice');
  });

  it('listActiveSources respects poll_interval_minutes against last snapshot time', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push(
      {
        id: 'src_due',
        name: 'due-source',
        display_name: 'Due Source',
        provider_type: 'ics',
        owner_type: 'family',
        source_category: 'shared',
        url: 'https://example.com/due.ics',
        icon: '',
        prefix: '',
        fetch_url_secret_ref: null,
        include_in_child_ics: 0,
        include_in_family_ics: 1,
        include_in_child_google_output: 0,
        is_active: 1,
        sort_order: 0,
        poll_interval_minutes: 30,
        quality_profile: 'standard',
        created_at: '2026-03-03T00:00:00.000Z',
        updated_at: '2026-03-03T00:00:00.000Z',
      },
      {
        id: 'src_recent',
        name: 'recent-source',
        display_name: 'Recent Source',
        provider_type: 'ics',
        owner_type: 'family',
        source_category: 'shared',
        url: 'https://example.com/recent.ics',
        icon: '',
        prefix: '',
        fetch_url_secret_ref: null,
        include_in_child_ics: 0,
        include_in_family_ics: 1,
        include_in_child_google_output: 0,
        is_active: 1,
        sort_order: 1,
        poll_interval_minutes: 30,
        quality_profile: 'standard',
        created_at: '2026-03-03T00:00:00.000Z',
        updated_at: '2026-03-03T00:00:00.000Z',
      }
    );
    db.sourceSnapshots.push(
      {
        id: 'snap_due',
        source_id: 'src_due',
        fetched_at: new Date(Date.now() - 45 * 60 * 1000).toISOString(),
        http_status: 200,
        etag: 'due',
        last_modified: null,
        payload_blob_ref: null,
        parse_status: 'parsed',
        parse_error_summary: null,
      },
      {
        id: 'snap_recent',
        source_id: 'src_recent',
        fetched_at: new Date(Date.now() - 10 * 60 * 1000).toISOString(),
        http_status: 200,
        etag: 'recent',
        last_modified: null,
        payload_blob_ref: null,
        parse_status: 'parsed',
        parse_error_summary: null,
      }
    );

    const repo = new D1Repository(db, env);
    const sources = await repo.listActiveSources();

    expect(sources.map((source) => source.id)).toEqual(['src_due']);
  });

  it('syncs linked google output events after ingest and updates them on change', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.GOOGLE_SERVICE_ACCOUNT_JSON = await createServiceAccountJson();
    const db = new FakeDb();
    env.APP_DB = db;
    db.outputTargets.push({
      id: 'outt_google_grayson_clubs',
      target_type: 'google',
      slug: 'grayson_clubs',
      display_name: 'Grayson Clubs',
      calendar_id: 'grayson-clubs@group.calendar.google.com',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sources.push({
      id: 'src_google_sync',
      name: 'grayson-hockey',
      display_name: 'Grayson Hockey',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/grayson-google.ics',
      icon: '🏒',
      prefix: 'G:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sourceTargetLinks.push({
      id: 'stl_google_sync',
      source_id: 'src_google_sync',
      target_id: 'outt_google_grayson_clubs',
      target_key: 'grayson_clubs',
      target_type: 'google',
      icon: '🏒',
      prefix: '',
      sort_order: 0,
      is_enabled: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    let icsSummary = 'Hockey Practice';
    const fetchMock = vi.fn(async (url, options = {}) => {
      if (String(url) === 'https://example.com/grayson-google.ics') {
        return new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:grayson-google-1',
            `SUMMARY:${icsSummary}`,
            'DTSTART:20260310T010000Z',
            'DTEND:20260310T023000Z',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        );
      }
      if (String(url) === 'https://oauth2.googleapis.com/token') {
        return new Response(JSON.stringify({ access_token: 'google-token' }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
      if (String(url) === 'https://www.googleapis.com/calendar/v3/calendars/grayson-clubs%40group.calendar.google.com/events' && options.method === 'POST') {
        return new Response(JSON.stringify({ id: 'google-event-1', etag: 'etag-1' }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
      if (String(url) === 'https://www.googleapis.com/calendar/v3/calendars/grayson-clubs%40group.calendar.google.com/events/google-event-1' && options.method === 'PUT') {
        return new Response(JSON.stringify({ id: 'google-event-1', etag: 'etag-2' }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
      throw new Error(`Unexpected fetch: ${url} ${options.method || 'GET'}`);
    });
    vi.stubGlobal('fetch', fetchMock);

    const repo = new D1Repository(db, env);
    const firstSync = await repo.ingestSource('src_google_sync');
    expect(firstSync.googleSync.queued_jobs).toBe(1);
    await drainQueue(env);
    expect(db.googleEventLinks).toHaveLength(1);
    expect(db.googleEventLinks[0].google_event_id).toBe('google-event-1');

    icsSummary = 'Hockey Practice Updated';
    const secondSync = await repo.ingestSource('src_google_sync');
    expect(secondSync.googleSync.queued_jobs).toBe(1);
    await drainQueue(env);
    expect(db.googleEventLinks[0].google_etag).toBe('etag-2');
    expect(fetchMock).toHaveBeenCalledWith(
      'https://www.googleapis.com/calendar/v3/calendars/grayson-clubs%40group.calendar.google.com/events/google-event-1',
      expect.objectContaining({ method: 'PUT' })
    );
  });

  it('applies skip overrides immediately to feeds and queued google sync', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.GOOGLE_SERVICE_ACCOUNT_JSON = await createServiceAccountJson();
    const db = new FakeDb();
    env.APP_DB = db;
    db.outputTargets.push({
      id: 'outt_google_override',
      target_type: 'google',
      slug: 'grayson_clubs',
      display_name: 'Grayson Clubs',
      calendar_id: 'grayson-clubs@group.calendar.google.com',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sources.push({
      id: 'src_override_sync',
      name: 'grayson-override',
      display_name: 'Grayson Override',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/grayson-override.ics',
      icon: '🏒',
      prefix: 'G:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sourceTargetLinks.push(
      {
        id: 'stl_override_family',
        source_id: 'src_override_sync',
        target_id: null,
        target_key: 'family',
        target_type: 'ics',
        icon: '🏒',
        prefix: 'G:',
        sort_order: 0,
        is_enabled: 1,
        created_at: '2026-03-03T00:00:00.000Z',
        updated_at: '2026-03-03T00:00:00.000Z',
      },
      {
        id: 'stl_override_google',
        source_id: 'src_override_sync',
        target_id: 'outt_google_override',
        target_key: 'grayson_clubs',
        target_type: 'google',
        icon: '🏒',
        prefix: '',
        sort_order: 1,
        is_enabled: 1,
        created_at: '2026-03-03T00:00:00.000Z',
        updated_at: '2026-03-03T00:00:00.000Z',
      }
    );

    let createdCount = 0;
    const fetchMock = vi.fn(async (url, options = {}) => {
      if (String(url) === 'https://example.com/grayson-override.ics') {
        return new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:grayson-override-1',
            'SUMMARY:Hockey Practice',
            'DTSTART:20260310T010000Z',
            'DTEND:20260310T023000Z',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        );
      }
      if (String(url) === 'https://oauth2.googleapis.com/token') {
        return new Response(JSON.stringify({ access_token: 'google-token' }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
      if (String(url) === 'https://www.googleapis.com/calendar/v3/calendars/grayson-clubs%40group.calendar.google.com/events' && options.method === 'POST') {
        createdCount += 1;
        return new Response(JSON.stringify({ id: `google-event-${createdCount}`, etag: `etag-${createdCount}` }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
      if (String(url) === 'https://www.googleapis.com/calendar/v3/calendars/grayson-clubs%40group.calendar.google.com/events/google-event-1' && options.method === 'DELETE') {
        return new Response(null, { status: 204 });
      }
      throw new Error(`Unexpected fetch: ${url} ${options.method || 'GET'}`);
    });
    vi.stubGlobal('fetch', fetchMock);

    const repo = new D1Repository(db, env);
    await repo.ingestSource('src_override_sync');
    await drainQueue(env);

    const eventId = db.canonicalEvents[0].id;
    const instanceId = db.eventInstances[0].id;
    let familyFeed = await repo.generateFeed({ target: 'family', calendarName: 'Family Combined', lookbackDays: 30 });
    expect(familyFeed).toContain('SUMMARY:G: 🏒 Hockey Practice');
    expect(db.googleEventLinks).toHaveLength(1);

    await repo.createOverride({
      eventId,
      eventInstanceId: instanceId,
      overrideType: 'skip',
      payload: { reason: 'Travel conflict' },
      actorRole: 'editor',
    });

    expect(db.outputRules.find((rule) => rule.event_instance_id === instanceId && rule.target_key === 'family')?.include_state).toBe('excluded');
    familyFeed = await repo.generateFeed({ target: 'family', calendarName: 'Family Combined', lookbackDays: 30 });
    expect(familyFeed).not.toContain('SUMMARY:G: 🏒 Hockey Practice');

    await drainQueue(env);
    expect(db.googleEventLinks).toHaveLength(0);
    expect(fetchMock).toHaveBeenCalledWith(
      'https://www.googleapis.com/calendar/v3/calendars/grayson-clubs%40group.calendar.google.com/events/google-event-1',
      expect.objectContaining({ method: 'DELETE' })
    );

    const overrideId = db.eventOverrides[0].id;
    await repo.clearOverride(overrideId);

    expect(db.outputRules.find((rule) => rule.event_instance_id === instanceId && rule.target_key === 'family')?.include_state).toBe('included');
    familyFeed = await repo.generateFeed({ target: 'family', calendarName: 'Family Combined', lookbackDays: 30 });
    expect(familyFeed).toContain('SUMMARY:G: 🏒 Hockey Practice');

    await drainQueue(env);
    expect(db.googleEventLinks).toHaveLength(1);
    expect(db.googleEventLinks[0].google_event_id).toBe('google-event-2');
  });

  it('deletes stale google output events when a source is disabled', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.GOOGLE_SERVICE_ACCOUNT_JSON = await createServiceAccountJson();
    const db = new FakeDb();
    env.APP_DB = db;
    db.outputTargets.push({
      id: 'outt_google_family_clubs',
      target_type: 'google',
      slug: 'family_clubs',
      display_name: 'Family Clubs',
      calendar_id: 'family-clubs@group.calendar.google.com',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sources.push({
      id: 'src_google_disable',
      name: 'family-shared',
      display_name: 'Family Shared',
      provider_type: 'ics',
      owner_type: 'family',
      source_category: 'shared',
      url: 'https://example.com/family-google.ics',
      icon: '',
      prefix: '',
      fetch_url_secret_ref: null,
      include_in_child_ics: 0,
      include_in_family_ics: 1,
      include_in_child_google_output: 0,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sourceTargetLinks.push({
      id: 'stl_google_disable',
      source_id: 'src_google_disable',
      target_id: 'outt_google_family_clubs',
      target_key: 'family_clubs',
      target_type: 'google',
      icon: '',
      prefix: '',
      sort_order: 0,
      is_enabled: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.canonicalEvents.push({
      id: 'evt_google_disable',
      source_id: 'src_google_disable',
      identity_key: 'identity',
      event_kind: 'single',
      title: 'Family Dinner',
      source_icon: '',
      source_prefix: '',
      description: '',
      location: '',
      start_at: '2026-03-10T01:00:00.000Z',
      end_at: '2026-03-10T02:00:00.000Z',
      timezone: 'UTC',
      status: 'confirmed',
      rrule: null,
      series_until: null,
      source_deleted: 0,
      needs_review: 0,
      source_changed_since_overlay: 0,
      last_source_change_at: '2026-03-03T00:00:00.000Z',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.eventInstances.push({
      id: 'inst_google_disable',
      canonical_event_id: 'evt_google_disable',
      occurrence_start_at: '2026-03-10T01:00:00.000Z',
      occurrence_end_at: '2026-03-10T02:00:00.000Z',
      recurrence_instance_key: '2026-03-10T01:00:00.000Z',
      provider_recurrence_id: null,
      status: 'confirmed',
      source_deleted: 0,
      needs_review: 0,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.googleEventLinks.push({
      id: 'gel_google_disable',
      target_id: 'outt_google_family_clubs',
      target_key: 'family_clubs',
      canonical_event_id: 'evt_google_disable',
      event_instance_id: 'inst_google_disable',
      google_event_id: 'google-event-delete',
      google_etag: 'etag-delete',
      last_synced_hash: 'hash-delete',
      last_synced_at: '2026-03-03T00:00:00.000Z',
      sync_status: 'synced',
      last_error: null,
    });

    vi.stubGlobal(
      'fetch',
      vi.fn(async (url, options = {}) => {
        if (String(url) === 'https://oauth2.googleapis.com/token') {
          return new Response(JSON.stringify({ access_token: 'google-token' }), { status: 200, headers: { 'content-type': 'application/json' } });
        }
        if (String(url) === 'https://www.googleapis.com/calendar/v3/calendars/family-clubs%40group.calendar.google.com/events/google-event-delete' && options.method === 'DELETE') {
          return new Response('', { status: 204 });
        }
        throw new Error(`Unexpected fetch: ${url} ${options.method || 'GET'}`);
      })
    );

    const repo = new D1Repository(db, env);
    await repo.disableSource('src_google_disable');
    await drainQueue(env);

    expect(db.googleEventLinks).toHaveLength(0);
  });

  it('records google sync errors without aborting ingest', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.GOOGLE_SERVICE_ACCOUNT_JSON = await createServiceAccountJson();
    const db = new FakeDb();
    env.APP_DB = db;
    db.outputTargets.push({
      id: 'outt_google_error',
      target_type: 'google',
      slug: 'naomi_clubs',
      display_name: 'Naomi Clubs',
      calendar_id: 'naomi-clubs@group.calendar.google.com',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sources.push({
      id: 'src_google_error',
      name: 'naomi-volleyball',
      display_name: 'Naomi Volleyball',
      provider_type: 'ics',
      owner_type: 'naomi',
      source_category: 'sports',
      url: 'https://example.com/naomi-google-error.ics',
      icon: '🏐',
      prefix: 'N:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sourceTargetLinks.push({
      id: 'stl_google_error',
      source_id: 'src_google_error',
      target_id: 'outt_google_error',
      target_key: 'naomi_clubs',
      target_type: 'google',
      icon: '🏐',
      prefix: '',
      sort_order: 0,
      is_enabled: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    vi.stubGlobal(
      'fetch',
      vi.fn(async (url, options = {}) => {
        if (String(url) === 'https://example.com/naomi-google-error.ics') {
          return new Response(
            [
              'BEGIN:VCALENDAR',
              'BEGIN:VEVENT',
              'UID:naomi-error-1',
              'SUMMARY:Volleyball Practice',
              'DTSTART:20260310T010000Z',
              'DTEND:20260310T023000Z',
              'END:VEVENT',
              'END:VCALENDAR',
            ].join('\r\n'),
            { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
          );
        }
        if (String(url) === 'https://oauth2.googleapis.com/token') {
          return new Response(JSON.stringify({ access_token: 'google-token' }), { status: 200, headers: { 'content-type': 'application/json' } });
        }
        if (String(url) === 'https://www.googleapis.com/calendar/v3/calendars/naomi-clubs%40group.calendar.google.com/events' && options.method === 'POST') {
          return new Response(JSON.stringify({ error: { message: 'calendar write failed' } }), { status: 500, headers: { 'content-type': 'application/json' } });
        }
        throw new Error(`Unexpected fetch: ${url} ${options.method || 'GET'}`);
      })
    );

    const repo = new D1Repository(db, env);
    const result = await repo.ingestSource('src_google_error');
    expect(result.googleSync.queued_jobs).toBe(1);
    await drainQueue(env);

    expect(db.googleEventLinks).toHaveLength(1);
    expect(db.googleEventLinks[0].sync_status).toBe('error');
    expect(db.googleEventLinks[0].last_error).toContain('calendar write failed');
  });

  it('limits desired Google sync rows to the configured lookback window', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.DEFAULT_LOOKBACK_DAYS = '7';
    const db = new FakeDb();
    env.APP_DB = db;
    db.outputTargets.push({
      id: 'outt_google_lookback',
      target_type: 'google',
      slug: 'family_output',
      display_name: 'Family Output',
      calendar_id: 'family-output@group.calendar.google.com',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sources.push({
      id: 'src_google_lookback',
      name: 'family-source',
      display_name: 'Family Source',
      provider_type: 'ics',
      owner_type: 'family',
      source_category: 'shared',
      url: 'https://example.com/family-source.ics',
      icon: '',
      prefix: '',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sourceTargetLinks.push({
      id: 'stl_google_lookback',
      source_id: 'src_google_lookback',
      target_id: 'outt_google_lookback',
      target_key: 'family_output',
      target_type: 'google',
      icon: '',
      prefix: '',
      sort_order: 0,
      is_enabled: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.canonicalEvents.push(
      {
        id: 'ce_old',
        source_id: 'src_google_lookback',
        identity_key: 'old',
        event_kind: 'single',
        title: 'Old Event',
        source_icon: '',
        source_prefix: '',
        description: '',
        location: '',
        start_at: '2025-01-01T10:00:00.000Z',
        end_at: '2025-01-01T11:00:00.000Z',
        timezone: 'UTC',
        status: 'confirmed',
        rrule: null,
        series_until: null,
        source_deleted: 0,
        needs_review: 0,
        source_changed_since_overlay: 0,
        last_source_change_at: '2025-01-01T09:00:00.000Z',
        created_at: '2025-01-01T09:00:00.000Z',
        updated_at: '2025-01-01T09:00:00.000Z',
      },
      {
        id: 'ce_new',
        source_id: 'src_google_lookback',
        identity_key: 'new',
        event_kind: 'single',
        title: 'Upcoming Event',
        source_icon: '',
        source_prefix: '',
        description: '',
        location: '',
        start_at: '2099-01-01T10:00:00.000Z',
        end_at: '2099-01-01T11:00:00.000Z',
        timezone: 'UTC',
        status: 'confirmed',
        rrule: null,
        series_until: null,
        source_deleted: 0,
        needs_review: 0,
        source_changed_since_overlay: 0,
        last_source_change_at: '2099-01-01T09:00:00.000Z',
        created_at: '2099-01-01T09:00:00.000Z',
        updated_at: '2099-01-01T09:00:00.000Z',
      }
    );
    db.eventInstances.push(
      {
        id: 'ei_old',
        canonical_event_id: 'ce_old',
        occurrence_start_at: '2025-01-01T10:00:00.000Z',
        occurrence_end_at: '2025-01-01T11:00:00.000Z',
        recurrence_instance_key: '2025-01-01T10:00:00.000Z',
        provider_recurrence_id: null,
        status: 'confirmed',
        source_deleted: 0,
        needs_review: 0,
        created_at: '2025-01-01T09:00:00.000Z',
        updated_at: '2025-01-01T09:00:00.000Z',
      },
      {
        id: 'ei_new',
        canonical_event_id: 'ce_new',
        occurrence_start_at: '2099-01-01T10:00:00.000Z',
        occurrence_end_at: '2099-01-01T11:00:00.000Z',
        recurrence_instance_key: '2099-01-01T10:00:00.000Z',
        provider_recurrence_id: null,
        status: 'confirmed',
        source_deleted: 0,
        needs_review: 0,
        created_at: '2099-01-01T09:00:00.000Z',
        updated_at: '2099-01-01T09:00:00.000Z',
      }
    );
    db.outputRules.push(
      {
        id: 'out_old',
        canonical_event_id: 'ce_old',
        event_instance_id: 'ei_old',
        target_id: 'outt_google_lookback',
        target_key: 'family_output',
        include_state: 'included',
        derived_reason: 'seed',
        created_at: '2025-01-01T09:00:00.000Z',
        updated_at: '2025-01-01T09:00:00.000Z',
      },
      {
        id: 'out_new',
        canonical_event_id: 'ce_new',
        event_instance_id: 'ei_new',
        target_id: 'outt_google_lookback',
        target_key: 'family_output',
        include_state: 'included',
        derived_reason: 'seed',
        created_at: '2099-01-01T09:00:00.000Z',
        updated_at: '2099-01-01T09:00:00.000Z',
      }
    );

    const repo = new D1Repository(db, env);
    const rows = await repo.listDesiredGoogleSyncRows('src_google_lookback');

    expect(rows).toHaveLength(1);
    expect(rows[0].canonical_event_id).toBe('ce_new');
    expect(rows[0].event_instance_id).toBe('ei_new');
  });

  it('sends floating-time events to Google using the calendar timezone without forcing UTC', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.GOOGLE_SERVICE_ACCOUNT_JSON = await createServiceAccountJson();
    const db = new FakeDb();
    env.APP_DB = db;
    db.outputTargets.push({
      id: 'outt_google_floating',
      target_type: 'google',
      slug: 'naomi_school',
      display_name: 'Naomi School',
      calendar_id: 'naomi-school@group.calendar.google.com',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sources.push({
      id: 'src_google_floating',
      name: 'naomi-school',
      display_name: 'Naomi School',
      provider_type: 'ics',
      owner_type: 'naomi',
      source_category: 'school',
      url: 'https://example.com/naomi-school-floating.ics',
      icon: '',
      prefix: '',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sourceTargetLinks.push({
      id: 'stl_google_floating',
      source_id: 'src_google_floating',
      target_id: 'outt_google_floating',
      target_key: 'naomi_school',
      target_type: 'google',
      icon: '',
      prefix: '',
      sort_order: 0,
      is_enabled: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    let postedGoogleEvent = null;
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url, options = {}) => {
        if (String(url) === 'https://example.com/naomi-school-floating.ics') {
          return new Response(
            [
              'BEGIN:VCALENDAR',
              'VERSION:2.0',
              'X-WR-TIMEZONE:America/Vancouver',
              'BEGIN:VEVENT',
              'UID:naomi-school-assembly-1',
              'SUMMARY:Assembly',
              'DTSTART:20260312T090000',
              'DTEND:20260312T100000',
              'END:VEVENT',
              'END:VCALENDAR',
            ].join('\r\n'),
            {
              status: 200,
              headers: {
                etag: 'float123',
                'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT',
              },
            }
          );
        }
        if (String(url) === 'https://oauth2.googleapis.com/token') {
          return new Response(JSON.stringify({ access_token: 'google-token' }), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          });
        }
        if (String(url) === 'https://www.googleapis.com/calendar/v3/calendars/naomi-school%40group.calendar.google.com/events' && options.method === 'POST') {
          postedGoogleEvent = JSON.parse(String(options.body || '{}'));
          return new Response(JSON.stringify({ id: 'google-school-1', etag: '"etag-1"' }), {
            status: 200,
            headers: { 'content-type': 'application/json' },
          });
        }
        throw new Error(`Unexpected fetch: ${url} ${options.method || 'GET'}`);
      })
    );

    const repo = new D1Repository(db, env);
    const result = await repo.ingestSource('src_google_floating');
    expect(result.googleSync.queued_jobs).toBe(1);
    await drainQueue(env);

    expect(postedGoogleEvent).toMatchObject({
      start: {
        dateTime: '2026-03-12T09:00:00.000',
        timeZone: 'America/Vancouver',
      },
      end: {
        dateTime: '2026-03-12T10:00:00.000',
        timeZone: 'America/Vancouver',
      },
    });
    expect(postedGoogleEvent.start.dateTime.endsWith('Z')).toBe(false);
  });

  it('stops additional google writes after rate limiting and defers remaining events', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.GOOGLE_SERVICE_ACCOUNT_JSON = await createServiceAccountJson();
    const db = new FakeDb();
    env.APP_DB = db;
    db.outputTargets.push({
      id: 'outt_google_rate_limit',
      target_type: 'google',
      slug: 'grayson_clubs',
      display_name: 'Grayson Clubs',
      calendar_id: 'grayson-clubs@group.calendar.google.com',
      ownership_mode: 'managed_output',
      is_system: 0,
      is_active: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sources.push({
      id: 'src_google_rate_limit',
      name: 'grayson-hockey',
      display_name: 'Grayson Hockey',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/grayson-google-rate-limit.ics',
      icon: '🏒',
      prefix: '',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 1,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.sourceTargetLinks.push({
      id: 'stl_google_rate_limit',
      source_id: 'src_google_rate_limit',
      target_id: 'outt_google_rate_limit',
      target_key: 'grayson_clubs',
      target_type: 'google',
      icon: '🏒',
      prefix: '',
      sort_order: 0,
      is_enabled: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    const fetchMock = vi.fn(async (url, options = {}) => {
      if (String(url) === 'https://example.com/grayson-google-rate-limit.ics') {
        return new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:grayson-rate-limit-1',
            'SUMMARY:Practice One',
            'DTSTART:20260310T010000Z',
            'DTEND:20260310T020000Z',
            'END:VEVENT',
            'BEGIN:VEVENT',
            'UID:grayson-rate-limit-2',
            'SUMMARY:Practice Two',
            'DTSTART:20260311T010000Z',
            'DTEND:20260311T020000Z',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        );
      }
      if (String(url) === 'https://oauth2.googleapis.com/token') {
        return new Response(JSON.stringify({ access_token: 'google-token' }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
      if (String(url) === 'https://www.googleapis.com/calendar/v3/calendars/grayson-clubs%40group.calendar.google.com/events' && options.method === 'POST') {
        return new Response(JSON.stringify({ error: { message: 'Rate Limit Exceeded' } }), { status: 429, headers: { 'content-type': 'application/json' } });
      }
      throw new Error(`Unexpected fetch: ${url} ${options.method || 'GET'}`);
    });
    vi.stubGlobal('fetch', fetchMock);

    const repo = new D1Repository(db, env);
    const result = await repo.ingestSource('src_google_rate_limit');
    expect(result.googleSync.queued_jobs).toBe(1);
    await drainQueue(env);

    expect(db.googleEventLinks).toHaveLength(2);
    expect(db.googleEventLinks.some((row) => String(row.last_error || '').includes('Deferred after rate limit'))).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(5);
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

describe('parseICS timezone handling', () => {
  it('unescapes RFC5545 text fields from upstream feeds', () => {
    const ics = [
      'BEGIN:VCALENDAR',
      'BEGIN:VEVENT',
      'UID:text-escape-1',
      'SUMMARY:U11A\\nGrayson Long\\nhot dog fundraiser \\n1 hour shifts required',
      'DESCRIPTION:Line one\\nLine two\\, with comma\\; and semicolon',
      'LOCATION:Rink\\, Lobby',
      'DTSTART:20240315T190000Z',
      'DTEND:20240315T200000Z',
      'END:VEVENT',
      'END:VCALENDAR',
    ].join('\r\n');

    const events = parseICS(ics);
    expect(events).toHaveLength(1);
    expect(events[0].summary).toBe('U11A\nGrayson Long\nhot dog fundraiser \n1 hour shifts required');
    expect(events[0].description).toBe('Line one\nLine two, with comma; and semicolon');
    expect(events[0].location).toBe('Rink, Lobby');
  });

  it('floating local time with X-WR-TIMEZONE uses calendar timezone and no trailing Z', () => {
    const ics = [
      'BEGIN:VCALENDAR',
      'X-WR-TIMEZONE:America/New_York',
      'BEGIN:VEVENT',
      'UID:float-tz-1',
      'SUMMARY:Morning Meeting',
      'DTSTART:20240315T090000',
      'DTEND:20240315T100000',
      'END:VEVENT',
      'END:VCALENDAR',
    ].join('\r\n');

    const events = parseICS(ics);
    expect(events).toHaveLength(1);
    expect(events[0].startAt).toBe('2024-03-15T09:00:00.000');
    expect(events[0].startAt.endsWith('Z')).toBe(false);
    expect(events[0].timezone).toBe('America/New_York');
  });

  it('floating local time without X-WR-TIMEZONE defaults to UTC and no trailing Z', () => {
    const ics = [
      'BEGIN:VCALENDAR',
      'BEGIN:VEVENT',
      'UID:float-no-tz-1',
      'SUMMARY:All Hands',
      'DTSTART:20240315T140000',
      'DTEND:20240315T150000',
      'END:VEVENT',
      'END:VCALENDAR',
    ].join('\r\n');

    const events = parseICS(ics);
    expect(events).toHaveLength(1);
    expect(events[0].startAt).toBe('2024-03-15T14:00:00.000');
    expect(events[0].startAt.endsWith('Z')).toBe(false);
    expect(events[0].timezone).toBe('UTC');
  });

  it('explicit UTC time (ends in Z) is unchanged', () => {
    const ics = [
      'BEGIN:VCALENDAR',
      'X-WR-TIMEZONE:America/Chicago',
      'BEGIN:VEVENT',
      'UID:utc-explicit-1',
      'SUMMARY:UTC Event',
      'DTSTART:20240315T190000Z',
      'DTEND:20240315T200000Z',
      'END:VEVENT',
      'END:VCALENDAR',
    ].join('\r\n');

    const events = parseICS(ics);
    expect(events).toHaveLength(1);
    expect(events[0].startAt).toBe('2024-03-15T19:00:00.000Z');
    expect(events[0].startAt.endsWith('Z')).toBe(true);
    expect(events[0].timezone).toBe('UTC');
  });

  it('TZID param on DTSTART takes precedence over X-WR-TIMEZONE', () => {
    const ics = [
      'BEGIN:VCALENDAR',
      'X-WR-TIMEZONE:America/New_York',
      'BEGIN:VEVENT',
      'UID:tzid-param-1',
      'SUMMARY:West Coast Call',
      'DTSTART;TZID=America/Los_Angeles:20240315T080000',
      'DTEND;TZID=America/Los_Angeles:20240315T090000',
      'END:VEVENT',
      'END:VCALENDAR',
    ].join('\r\n');

    const events = parseICS(ics);
    expect(events).toHaveLength(1);
    expect(events[0].startAt).toBe('2024-03-15T08:00:00.000');
    expect(events[0].startAt.endsWith('Z')).toBe(false);
    expect(events[0].timezone).toBe('America/Los_Angeles');
  });
});
