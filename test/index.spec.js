import { env, createExecutionContext, waitOnExecutionContext } from 'cloudflare:test';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import worker from '../src/index.js';
import { clearAuthCachesForTest } from '../src/lib/auth.js';
import { createRepository, D1Repository } from '../src/lib/repository.js';
import { parseICS } from '../src/lib/ics.js';

async function drainQueue(env) {
  while (env.JOBS_QUEUE.sent.length) {
    const messages = env.JOBS_QUEUE.sent.splice(0).map((body) => ({
      body,
      attempts: body.attemptCount || 1,
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

    if (sql.includes('ALTER TABLE sources ADD COLUMN title_rewrite_rules_json')) {
      return;
    }

    if (sql.includes('ALTER TABLE canonical_events ADD COLUMN source_title_raw')) {
      return;
    }

    if (sql.includes('ALTER TABLE source_snapshots ADD COLUMN payload_hash')) {
      return;
    }

    if (sql.includes('ALTER TABLE sync_jobs ADD COLUMN attempt_count')) {
      return;
    }

    if (sql.includes('ALTER TABLE sync_jobs ADD COLUMN last_error_kind')) {
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
        titleRewriteRulesJson,
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
        title_rewrite_rules_json: titleRewriteRulesJson,
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
      const [id, sourceId, fetchedAt, httpStatus, etag, lastModified, payloadBlobRef, payloadHash, parseStatus, parseErrorSummary] = values;
      this.sourceSnapshots.push({
        id,
        source_id: sourceId,
        fetched_at: fetchedAt,
        http_status: httpStatus,
        etag,
        last_modified: lastModified,
        payload_blob_ref: payloadBlobRef,
        payload_hash: payloadHash,
        parse_status: parseStatus,
        parse_error_summary: parseErrorSummary,
      });
      return;
    }

    if (sql.includes('DELETE FROM source_snapshots WHERE fetched_at < ?')) {
      const [cutoff] = values;
      this.sourceSnapshots = this.sourceSnapshots.filter((row) => String(row.fetched_at) >= String(cutoff));
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
          sourceTitleRaw,
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
            source_title_raw: sourceTitleRaw,
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
          sourceTitleRaw,
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
          source_title_raw: sourceTitleRaw,
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

    if (sql.includes('UPDATE canonical_events') && sql.includes('SET title = CASE id')) {
      const eventIdCount = (sql.match(/WHERE id IN \(([^)]+)\)/)?.[1].match(/\?/g) || []).length;
      const titlePairs = values.slice(0, eventIdCount * 2);
      const rawTitlePairs = values.slice(eventIdCount * 2, eventIdCount * 4);
      const sourceIcon = values[eventIdCount * 4];
      const sourcePrefix = values[eventIdCount * 4 + 1];
      const timestamp = values[eventIdCount * 4 + 2];
      const eventIds = values.slice(values.length - eventIdCount);
      const titleMap = new Map();
      const rawTitleMap = new Map();
      for (let index = 0; index < titlePairs.length; index += 2) {
        titleMap.set(titlePairs[index], titlePairs[index + 1]);
      }
      for (let index = 0; index < rawTitlePairs.length; index += 2) {
        rawTitleMap.set(rawTitlePairs[index], rawTitlePairs[index + 1]);
      }
      for (const eventId of eventIds) {
        const event = this.canonicalEvents.find((row) => row.id === eventId);
        if (!event) continue;
        if (titleMap.has(eventId)) event.title = titleMap.get(eventId);
        if (rawTitleMap.has(eventId)) event.source_title_raw = rawTitleMap.get(eventId);
        event.source_icon = sourceIcon;
        event.source_prefix = sourcePrefix;
        event.updated_at = timestamp;
      }
      return;
    }

    if (sql.includes('UPDATE canonical_events') && sql.includes('SET title = ?')) {
      const [title, sourceTitleRaw, sourceIcon, sourcePrefix, updatedAt, eventId] = values;
      const event = this.canonicalEvents.find((row) => row.id === eventId);
      if (event) {
        event.title = title;
        event.source_title_raw = sourceTitleRaw;
        event.source_icon = sourceIcon;
        event.source_prefix = sourcePrefix;
        event.updated_at = updatedAt;
      }
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

    if (sql.includes('UPDATE event_overrides') && sql.includes('SET event_instance_id = ?')) {
      const [eventInstanceId, updatedAt, overrideId] = values;
      const row = this.eventOverrides.find((override) => override.id === overrideId);
      if (row) {
        row.event_instance_id = eventInstanceId;
        row.scope_type = 'instance';
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
        attempt_count: 0,
        last_error_kind: null,
      });
      return;
    }

    if (sql.includes("SET status = 'queued', finished_at = NULL, attempt_count = ?")) {
      const [attemptCount, lastErrorKind, errorJson, jobId] = values;
      const job = this.syncJobs.find((row) => row.id === jobId);
      if (job) {
        job.status = 'queued';
        job.finished_at = null;
        job.attempt_count = attemptCount;
        job.last_error_kind = lastErrorKind;
        job.error_json = errorJson ?? job.error_json;
      }
      return;
    }

    if (sql.includes('UPDATE sync_jobs')) {
      const hasRetryMetadata = values.length >= 7;
      const [status, finishedAt, summaryJson, errorJson] = values;
      const jobId = hasRetryMetadata ? values[6] : values[4];
      const job = this.syncJobs.find((row) => row.id === jobId);
      if (job) {
        job.status = status;
        job.finished_at = finishedAt;
        job.summary_json = summaryJson ?? job.summary_json;
        job.error_json = errorJson ?? job.error_json;
        if (hasRetryMetadata) {
          job.attempt_count = values[4] ?? job.attempt_count;
          job.last_error_kind = values[5] ?? job.last_error_kind;
        }
      }
      return;
    }

    if (sql.includes("DELETE FROM sync_jobs") && sql.includes("status = 'completed'")) {
      const [cutoff] = values;
      this.syncJobs = this.syncJobs.filter((row) => !(row.status === 'completed' && String(row.finished_at || row.started_at || '') < String(cutoff)));
      return;
    }

    if (sql.includes("DELETE FROM sync_jobs") && sql.includes("status = 'failed'")) {
      const [cutoff] = values;
      this.syncJobs = this.syncJobs.filter((row) => !(row.status === 'failed' && String(row.finished_at || row.started_at || '') < String(cutoff)));
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
      const [name, displayName, ownerType, sourceCategory, url, icon, prefix, includeInChildIcs, includeInFamilyIcs, includeInChildGoogleOutput, titleRewriteRulesJson, isActive, sortOrder, pollIntervalMinutes, qualityProfile, updatedAt, sourceId] = values;
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
          title_rewrite_rules_json: titleRewriteRulesJson,
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
    if (sql.includes('SELECT payload_blob_ref') && sql.includes('FROM source_snapshots') && sql.includes('fetched_at < ?')) {
      const [cutoff, limit] = values;
      return this.sourceSnapshots
        .filter((row) => row.payload_blob_ref && String(row.fetched_at) < String(cutoff))
        .slice(0, limit || Infinity)
        .map((row) => ({ payload_blob_ref: row.payload_blob_ref }));
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
    if (sql.includes('FROM canonical_events') && sql.includes('WHERE source_id = ?') && sql.includes('ORDER BY id ASC')) {
      return this.canonicalEvents
        .filter((row) => row.source_id === values[0])
        .sort((a, b) => String(a.id).localeCompare(String(b.id)))
        .map((row) => ({
          id: row.id,
          title: row.title,
          source_title_raw: row.source_title_raw,
          source_icon: row.source_icon,
          source_prefix: row.source_prefix,
          description: row.description,
          location: row.location,
          start_at: row.start_at,
          end_at: row.end_at,
          timezone: row.timezone,
          status: row.status,
          rrule: row.rrule,
          source_deleted: row.source_deleted,
        }));
    }
    if (sql.includes('FROM event_instances') && sql.includes('JOIN canonical_events') && sql.includes('ORDER BY event_instances.id ASC')) {
      return this.eventInstances
        .filter((row) => {
          const event = this.canonicalEvents.find((candidate) => candidate.id === row.canonical_event_id);
          return event?.source_id === values[0];
        })
        .sort((a, b) => String(a.id).localeCompare(String(b.id)))
        .map((row) => ({
          id: row.id,
          canonical_event_id: row.canonical_event_id,
          occurrence_start_at: row.occurrence_start_at,
          occurrence_end_at: row.occurrence_end_at,
          recurrence_instance_key: row.recurrence_instance_key,
          provider_recurrence_id: row.provider_recurrence_id,
          status: row.status,
          source_deleted: row.source_deleted,
        }));
    }
    if (sql.includes('FROM output_rules') && sql.includes('ORDER BY output_rules.id ASC')) {
      return this.outputRules
        .filter((row) => {
          const event = this.canonicalEvents.find((candidate) => candidate.id === row.canonical_event_id);
          return event?.source_id === values[0];
        })
        .sort((a, b) => String(a.id).localeCompare(String(b.id)))
        .map((row) => ({
          id: row.id,
          canonical_event_id: row.canonical_event_id,
          event_instance_id: row.event_instance_id,
          target_id: row.target_id,
          target_key: row.target_key,
          include_state: row.include_state,
          derived_reason: row.derived_reason,
        }));
    }
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
        attempt_count: job.attempt_count || 0,
        last_error_kind: job.last_error_kind || null,
      }));
    }
    if (sql.includes('FROM output_targets')) {
      let rows = [...this.outputTargets];
      if (sql.includes('WHERE slug = ?')) rows = rows.filter((row) => row.slug === values[0]);
      return rows;
    }
    if (sql.includes('SELECT\n        event_instances.id,') && sql.includes('sources.name AS source_name')) {
      let rows = this.eventInstances
        .map((instance) => {
          const event = this.canonicalEvents.find((row) => row.id === instance.canonical_event_id);
          const source = event ? this.sources.find((row) => row.id === event.source_id) : null;
          if (!event || !source) return null;
          return {
            id: instance.id,
            occurrence_start_at: instance.occurrence_start_at,
            occurrence_end_at: instance.occurrence_end_at,
            canonical_event_id: event.id,
            title: event.title,
            source_name: source.name,
            owner_type: source.owner_type,
            event_source_id: event.source_id,
            event_source_deleted: event.source_deleted,
            instance_source_deleted: instance.source_deleted,
          };
        })
        .filter(Boolean);

      if (sql.includes('event_instances.occurrence_start_at >= ?')) {
        const now = String(values[0] || '');
        rows = rows.filter((row) => String(row.occurrence_start_at || '') >= now);
      }
      if (sql.includes('canonical_events.source_id = ?')) {
        const sourceId = values[sql.includes('event_instances.occurrence_start_at >= ?') ? 1 : 0];
        rows = rows.filter((row) => row.event_source_id === sourceId);
      }
      if (sql.includes('canonical_events.source_deleted = 0')) {
        rows = rows.filter((row) => !row.event_source_deleted);
      }
      if (sql.includes('event_instances.source_deleted = 0')) {
        rows = rows.filter((row) => !row.instance_source_deleted);
      }
      return rows
        .sort((a, b) => String(a.occurrence_start_at).localeCompare(String(b.occurrence_start_at)))
        .slice(0, values.at(-1) || 200)
        .map(({ event_source_id, event_source_deleted, instance_source_deleted, ...row }) => row);
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
      return this.eventInstances.filter((row) =>
        row.canonical_event_id === values[0] &&
        (!sql.includes('source_deleted = 0') || !row.source_deleted) &&
        (!values[1] || row.id === values[1])
      );
    }
    if (sql.includes('SELECT id, title, source_title_raw') && sql.includes('FROM canonical_events')) {
      return this.canonicalEvents
        .filter((row) => row.source_id === values[0])
        .map((row) => ({ id: row.id, title: row.title, source_title_raw: row.source_title_raw || null }));
    }
    if (sql.includes('SELECT DISTINCT event_overrides.canonical_event_id')) {
      return [...new Set(
        this.eventOverrides
          .filter((row) => !row.cleared_at)
          .filter((row) => {
            const event = this.canonicalEvents.find((candidate) => candidate.id === row.canonical_event_id);
            return event?.source_id === values[0];
          })
          .map((row) => row.canonical_event_id)
      )].map((canonicalEventId) => ({ canonical_event_id: canonicalEventId }));
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
      sql.includes('COUNT(output_rules.id) AS in_scope_count') &&
      sql.includes("google_event_links.sync_status = 'synced'") &&
      sql.includes('JOIN event_instances ON event_instances.id = output_rules.event_instance_id') &&
      sql.includes('FROM output_rules')
    ) {
      const lookbackCutoff = String(values[0] || '');
      const grouped = new Map();
      for (const rule of this.outputRules) {
        const event = this.canonicalEvents.find((candidate) => candidate.id === rule.canonical_event_id);
        const instance = this.eventInstances.find((candidate) => candidate.id === rule.event_instance_id);
        if (!event) continue;
        const target = this.outputTargets.find((entry) => entry.id === rule.target_id);
        if (!instance) continue;
        if (!target || target.target_type !== 'google' || rule.include_state !== 'included' || event.source_deleted || instance.source_deleted) continue;
        if (lookbackCutoff && String(instance.occurrence_end_at || '') < lookbackCutoff) continue;
        const current = grouped.get(event.source_id) || {
          source_id: event.source_id,
          in_scope_count: 0,
          synced_count: 0,
          deferred_count: 0,
          error_count: 0,
        };
        current.in_scope_count += 1;
        const link = this.googleEventLinks.find((entry) => entry.canonical_event_id === rule.canonical_event_id && entry.event_instance_id === rule.event_instance_id && ((entry.target_id && entry.target_id === rule.target_id) || entry.target_key === rule.target_key));
        if (link?.sync_status === 'synced' && link.google_event_id) {
          current.synced_count += 1;
        } else if (link?.sync_status === 'error' && String(link.last_error || '').startsWith('Deferred after rate limit:')) {
          current.deferred_count += 1;
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
      return event ? { id: event.id, source_id: event.source_id, event_kind: event.event_kind } : null;
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
    if (sql.includes("SELECT COUNT(*) AS count") && sql.includes("FROM source_snapshots") && sql.includes("fetched_at < ?")) {
      const [cutoff] = values;
      return { count: this.sourceSnapshots.filter((row) => String(row.fetched_at) < String(cutoff)).length };
    }
    if (sql.includes('FROM source_snapshots') && sql.includes('ORDER BY fetched_at DESC')) {
      return this.sourceSnapshots
        .filter((row) => row.source_id === values[0])
        .sort((a, b) => String(b.fetched_at).localeCompare(String(a.fetched_at)))[0] || null;
    }
    if (sql.includes("SELECT COUNT(*) AS count") && sql.includes("FROM sync_jobs") && sql.includes("status = 'completed'")) {
      const [cutoff] = values;
      return {
        count: this.syncJobs.filter((row) => row.status === 'completed' && String(row.finished_at || row.started_at || '') < String(cutoff)).length,
      };
    }
    if (sql.includes("SELECT COUNT(*) AS count") && sql.includes("FROM sync_jobs") && sql.includes("status = 'failed'")) {
      const [cutoff] = values;
      return {
        count: this.syncJobs.filter((row) => row.status === 'failed' && String(row.finished_at || row.started_at || '') < String(cutoff)).length,
      };
    }
    if (sql.includes('FROM sync_jobs') && sql.includes("status IN ('queued', 'running')")) {
      return this.syncJobs
        .filter((row) => row.job_type === values[0] && row.scope_type === values[1] && row.scope_id === values[2] && (row.status === 'queued' || row.status === 'running'))
        .sort((a, b) => String(b.started_at).localeCompare(String(a.started_at)))
        .map((row) => ({
          ...row,
          summary_json: row.summary_json || null,
          error_json: row.error_json || null,
          attempt_count: row.attempt_count || 0,
          last_error_kind: row.last_error_kind || null,
        }))[0] || null;
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
        return new Response('<!doctype html><html><head><meta name="robots" content="noindex, nofollow"></head><body><h1>Admin Console</h1></body></html>', {
          headers: { 'content-type': 'text/html; charset=utf-8' },
        });
      }
      if (url.pathname === '/admin-events.html') {
        return new Response('<!doctype html><html><head><meta name="robots" content="noindex, nofollow"></head><body><h1>Modify Events</h1></body></html>', {
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
  it('continues cron enqueueing after a queue producer rate limit error', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push(
      {
        id: 'src_one',
        name: 'one',
        display_name: 'One',
        provider_type: 'ics',
        owner_type: 'family',
        source_category: 'shared',
        url: 'https://example.com/one.ics',
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
        id: 'src_two',
        name: 'two',
        display_name: 'Two',
        provider_type: 'ics',
        owner_type: 'family',
        source_category: 'shared',
        url: 'https://example.com/two.ics',
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
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const consoleLog = vi.spyOn(console, 'log').mockImplementation(() => {});
    let sendCount = 0;
    env.JOBS_QUEUE.send = vi.fn(async (body) => {
      sendCount += 1;
      if (sendCount === 1) {
        throw new Error('Queue send failed: Too Many Requests');
      }
      env.JOBS_QUEUE.sent.push(body);
    });

    await worker.scheduled({ cron: '*/30 * * * *', scheduledTime: 1773869446000 }, env);

    expect(env.JOBS_QUEUE.sent).toHaveLength(1);
    expect(db.syncJobs).toHaveLength(2);
    expect(consoleError).toHaveBeenCalled();
    expect(consoleLog).toHaveBeenCalled();
  });

  it('dedupes equivalent active source jobs before sending a new queue message', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    const repo = await createRepository(env);

    const first = await repo.enqueueJob({
      jobType: 'ingest_source',
      scopeType: 'source',
      scopeId: 'src_same',
    });
    const second = await repo.enqueueJob({
      jobType: 'ingest_source',
      scopeType: 'source',
      scopeId: 'src_same',
    });

    expect(first.deduped).toBe(false);
    expect(second.deduped).toBe(true);
    expect(second.id).toBe(first.id);
    expect(env.JOBS_QUEUE.send).toHaveBeenCalledTimes(1);
    expect(db.syncJobs).toHaveLength(1);
  });

  it('dedupes equivalent active source-target jobs before sending a new queue message', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    const repo = await createRepository(env);

    const first = await repo.enqueueJob({
      jobType: 'sync_google_target',
      scopeType: 'source_target',
      scopeId: 'src_a|target_a|sync',
      payload: {
        sourceId: 'src_a',
        targetId: 'target_a',
        mode: 'sync',
      },
    });
    const second = await repo.enqueueJob({
      jobType: 'sync_google_target',
      scopeType: 'source_target',
      scopeId: 'src_a|target_a|sync',
      payload: {
        sourceId: 'src_a',
        targetId: 'target_a',
        mode: 'sync',
      },
    });

    expect(first.deduped).toBe(false);
    expect(second.deduped).toBe(true);
    expect(env.JOBS_QUEUE.send).toHaveBeenCalledTimes(1);
    expect(db.syncJobs).toHaveLength(1);
  });

  it('schedules delayed retries for retryable queue job failures and records retry metadata', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.JOB_RETRY_JITTER_PCT = '0';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_retryable',
      name: 'retryable-source',
      display_name: 'Retryable Source',
      provider_type: 'ics',
      owner_type: 'family',
      source_category: 'shared',
      url: 'https://example.com/retryable.ics',
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
    vi.stubGlobal('fetch', vi.fn(async () => { throw new TypeError('network down'); }));
    const repo = await createRepository(env);
    const job = await repo.enqueueJob({
      jobType: 'ingest_source',
      scopeType: 'source',
      scopeId: 'src_retryable',
      dedupe: false,
    });
    const ack = vi.fn();
    const retry = vi.fn();

    await worker.queue({
      messages: [{
        body: env.JOBS_QUEUE.sent.shift(),
        attempts: 1,
        ack,
        retry,
      }],
    }, env);

    expect(retry).toHaveBeenCalledWith({ delaySeconds: 60 });
    expect(ack).not.toHaveBeenCalled();
    const storedJob = db.syncJobs.find((row) => row.id === job.id);
    expect(storedJob?.status).toBe('queued');
    expect(storedJob?.attempt_count).toBe(1);
    expect(storedJob?.last_error_kind).toBe('network_error');
  });

  it('marks non-retryable queue job failures as failed without retrying', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.JOB_RETRY_JITTER_PCT = '0';
    const db = new FakeDb();
    env.APP_DB = db;
    const repo = await createRepository(env);
    const job = await repo.enqueueJob({
      jobType: 'ingest_source',
      scopeType: 'source',
      scopeId: 'missing_source',
      dedupe: false,
    });
    const ack = vi.fn();
    const retry = vi.fn();

    await worker.queue({
      messages: [{
        body: env.JOBS_QUEUE.sent.shift(),
        attempts: 1,
        ack,
        retry,
      }],
    }, env);

    expect(retry).not.toHaveBeenCalled();
    expect(ack).toHaveBeenCalled();
    const storedJob = db.syncJobs.find((row) => row.id === job.id);
    expect(storedJob?.status).toBe('failed');
    expect(storedJob?.attempt_count).toBe(1);
    expect(storedJob?.last_error_kind).toBe('source_state_error');
  });

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
      start_at: '2099-03-10T01:00:00.000Z',
      end_at: '2099-03-10T02:00:00.000Z',
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
      occurrence_start_at: '2099-03-10T01:00:00.000Z',
      occurrence_end_at: '2099-03-10T02:00:00.000Z',
      recurrence_instance_key: '2099-03-10T01:00:00.000Z',
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

  it('counts only in-scope rate-limited rows as deferred google sync work', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.DEFAULT_LOOKBACK_DAYS = '7';
    const db = new FakeDb();
    env.APP_DB = db;

    db.sources.push({
      id: 'src_sync_counts',
      name: 'grayson-hockey',
      display_name: 'Grayson Hockey',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/grayson-hockey.ics',
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
    db.outputTargets.push({
      id: 'outt_sync_counts',
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

    db.canonicalEvents.push(
      {
        id: 'ce_synced',
        source_id: 'src_sync_counts',
        identity_key: 'synced',
        event_kind: 'single',
        title: 'Upcoming Synced',
        source_icon: '🏒',
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
      },
      {
        id: 'ce_deferred',
        source_id: 'src_sync_counts',
        identity_key: 'deferred',
        event_kind: 'single',
        title: 'Upcoming Deferred',
        source_icon: '🏒',
        source_prefix: '',
        description: '',
        location: '',
        start_at: '2099-01-02T10:00:00.000Z',
        end_at: '2099-01-02T11:00:00.000Z',
        timezone: 'UTC',
        status: 'confirmed',
        rrule: null,
        series_until: null,
        source_deleted: 0,
        needs_review: 0,
        source_changed_since_overlay: 0,
        last_source_change_at: '2099-01-02T09:00:00.000Z',
        created_at: '2099-01-02T09:00:00.000Z',
        updated_at: '2099-01-02T09:00:00.000Z',
      },
      {
        id: 'ce_error',
        source_id: 'src_sync_counts',
        identity_key: 'error',
        event_kind: 'single',
        title: 'Upcoming Error',
        source_icon: '🏒',
        source_prefix: '',
        description: '',
        location: '',
        start_at: '2099-01-03T10:00:00.000Z',
        end_at: '2099-01-03T11:00:00.000Z',
        timezone: 'UTC',
        status: 'confirmed',
        rrule: null,
        series_until: null,
        source_deleted: 0,
        needs_review: 0,
        source_changed_since_overlay: 0,
        last_source_change_at: '2099-01-03T09:00:00.000Z',
        created_at: '2099-01-03T09:00:00.000Z',
        updated_at: '2099-01-03T09:00:00.000Z',
      },
      {
        id: 'ce_old',
        source_id: 'src_sync_counts',
        identity_key: 'old',
        event_kind: 'single',
        title: 'Old Past Event',
        source_icon: '🏒',
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
      }
    );

    db.eventInstances.push(
      {
        id: 'ei_synced',
        canonical_event_id: 'ce_synced',
        occurrence_start_at: '2099-01-01T10:00:00.000Z',
        occurrence_end_at: '2099-01-01T11:00:00.000Z',
        recurrence_instance_key: '2099-01-01T10:00:00.000Z',
        provider_recurrence_id: null,
        status: 'confirmed',
        source_deleted: 0,
        needs_review: 0,
        created_at: '2099-01-01T09:00:00.000Z',
        updated_at: '2099-01-01T09:00:00.000Z',
      },
      {
        id: 'ei_deferred',
        canonical_event_id: 'ce_deferred',
        occurrence_start_at: '2099-01-02T10:00:00.000Z',
        occurrence_end_at: '2099-01-02T11:00:00.000Z',
        recurrence_instance_key: '2099-01-02T10:00:00.000Z',
        provider_recurrence_id: null,
        status: 'confirmed',
        source_deleted: 0,
        needs_review: 0,
        created_at: '2099-01-02T09:00:00.000Z',
        updated_at: '2099-01-02T09:00:00.000Z',
      },
      {
        id: 'ei_error',
        canonical_event_id: 'ce_error',
        occurrence_start_at: '2099-01-03T10:00:00.000Z',
        occurrence_end_at: '2099-01-03T11:00:00.000Z',
        recurrence_instance_key: '2099-01-03T10:00:00.000Z',
        provider_recurrence_id: null,
        status: 'confirmed',
        source_deleted: 0,
        needs_review: 0,
        created_at: '2099-01-03T09:00:00.000Z',
        updated_at: '2099-01-03T09:00:00.000Z',
      },
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
      }
    );

    db.outputRules.push(
      {
        id: 'out_synced',
        canonical_event_id: 'ce_synced',
        event_instance_id: 'ei_synced',
        target_id: 'outt_sync_counts',
        target_key: 'grayson_clubs',
        include_state: 'included',
        derived_reason: 'seed',
        created_at: '2099-01-01T09:00:00.000Z',
        updated_at: '2099-01-01T09:00:00.000Z',
      },
      {
        id: 'out_deferred',
        canonical_event_id: 'ce_deferred',
        event_instance_id: 'ei_deferred',
        target_id: 'outt_sync_counts',
        target_key: 'grayson_clubs',
        include_state: 'included',
        derived_reason: 'seed',
        created_at: '2099-01-02T09:00:00.000Z',
        updated_at: '2099-01-02T09:00:00.000Z',
      },
      {
        id: 'out_error',
        canonical_event_id: 'ce_error',
        event_instance_id: 'ei_error',
        target_id: 'outt_sync_counts',
        target_key: 'grayson_clubs',
        include_state: 'included',
        derived_reason: 'seed',
        created_at: '2099-01-03T09:00:00.000Z',
        updated_at: '2099-01-03T09:00:00.000Z',
      },
      {
        id: 'out_old',
        canonical_event_id: 'ce_old',
        event_instance_id: 'ei_old',
        target_id: 'outt_sync_counts',
        target_key: 'grayson_clubs',
        include_state: 'included',
        derived_reason: 'seed',
        created_at: '2025-01-01T09:00:00.000Z',
        updated_at: '2025-01-01T09:00:00.000Z',
      }
    );

    db.googleEventLinks.push(
      {
        id: 'gel_synced',
        target_id: 'outt_sync_counts',
        target_key: 'grayson_clubs',
        canonical_event_id: 'ce_synced',
        event_instance_id: 'ei_synced',
        google_event_id: 'google-synced',
        google_etag: '"etag-1"',
        last_synced_hash: 'hash-1',
        last_synced_at: '2099-01-01T12:00:00.000Z',
        sync_status: 'synced',
        last_error: null,
      },
      {
        id: 'gel_deferred',
        target_id: 'outt_sync_counts',
        target_key: 'grayson_clubs',
        canonical_event_id: 'ce_deferred',
        event_instance_id: 'ei_deferred',
        google_event_id: null,
        google_etag: null,
        last_synced_hash: null,
        last_synced_at: '2099-01-02T12:00:00.000Z',
        sync_status: 'error',
        last_error: 'Deferred after rate limit: Rate Limit Exceeded',
      },
      {
        id: 'gel_error',
        target_id: 'outt_sync_counts',
        target_key: 'grayson_clubs',
        canonical_event_id: 'ce_error',
        event_instance_id: 'ei_error',
        google_event_id: null,
        google_etag: null,
        last_synced_hash: null,
        last_synced_at: '2099-01-03T12:00:00.000Z',
        sync_status: 'error',
        last_error: 'Google Calendar API is disabled.',
      },
      {
        id: 'gel_old',
        target_id: 'outt_sync_counts',
        target_key: 'grayson_clubs',
        canonical_event_id: 'ce_old',
        event_instance_id: 'ei_old',
        google_event_id: null,
        google_etag: null,
        last_synced_hash: null,
        last_synced_at: '2025-01-01T12:00:00.000Z',
        sync_status: 'error',
        last_error: 'Deferred after rate limit: old row should not count',
      }
    );

    const repo = new D1Repository(db, env);
    const [source] = await repo.listSources();

    expect(source.google_sync_counts).toEqual({ synced: 1, deferred: 1, errors: 1 });
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
    expect(response.headers.get('x-robots-tag')).toBe('noindex, nofollow');
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
    expect(response.headers.get('x-robots-tag')).toBe('noindex, nofollow');
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

  it('omits source_deleted instance rows from /api/instances', async () => {
    const db = env.APP_DB;
    db.sources.push({
      id: 'src_instances_filter',
      name: 'instances-filter',
      display_name: 'Instances Filter',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/filter.ics',
      icon: '🏒',
      prefix: 'G:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 0,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });
    db.canonicalEvents.push({
      id: 'evt_instances_filter',
      source_id: 'src_instances_filter',
      identity_key: 'instances-filter',
      event_kind: 'single',
      title: 'Filter Me Once',
      source_icon: '🏒',
      source_prefix: 'G:',
      description: '',
      location: '',
      start_at: '2099-01-01T18:00:00.000Z',
      end_at: '2099-01-01T19:00:00.000Z',
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
    const source = db.sources.find((row) => row.id === 'src_instances_filter');
    const event = db.canonicalEvents.find((row) => row.id === 'evt_instances_filter');
    const activeInstance = {
      id: 'inst_instances_filter_active',
      canonical_event_id: event.id,
      occurrence_start_at: '2099-01-01T18:00:00.000Z',
      occurrence_end_at: '2099-01-01T19:00:00.000Z',
      recurrence_instance_key: '2099-01-01T18:00:00.000Z',
      provider_recurrence_id: null,
      status: 'confirmed',
      source_deleted: 0,
      needs_review: 0,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    };
    db.eventInstances.push(activeInstance);
    db.eventInstances.push({
      id: `${activeInstance.id}_stale`,
      canonical_event_id: event.id,
      occurrence_start_at: activeInstance.occurrence_start_at,
      occurrence_end_at: activeInstance.occurrence_end_at,
      recurrence_instance_key: `${activeInstance.recurrence_instance_key}_stale`,
      provider_recurrence_id: null,
      status: activeInstance.status,
      source_deleted: 1,
      needs_review: 0,
      created_at: activeInstance.created_at,
      updated_at: activeInstance.updated_at,
    });

    const request = new Request(`http://example.com/api/instances?source=${encodeURIComponent(source.id)}&future=1&limit=200`, {
      headers: { 'x-user-role': 'admin' },
    });
    const ctx = createExecutionContext();
    const response = await worker.fetch(request, env, ctx);
    await waitOnExecutionContext(ctx);
    const body = await response.json();
    expect(response.status).toBe(200);
    expect(body.instances.filter((row) => row.canonical_event_id === event.id)).toHaveLength(1);
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

  it('rejects duplicate active overrides for the same item', async () => {
    const firstRequest = new Request('http://example.com/api/events/evt_grayson_hockey/overrides', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-user-role': 'editor',
      },
      body: JSON.stringify({
        overrideType: 'note',
        eventInstanceId: 'inst_grayson_hockey_1',
        payload: { note: 'Need to leave early' },
      }),
    });
    const firstCtx = createExecutionContext();
    const firstResponse = await worker.fetch(firstRequest, env, firstCtx);
    await waitOnExecutionContext(firstCtx);
    expect(firstResponse.status).toBe(201);

    const duplicateRequest = new Request('http://example.com/api/events/evt_grayson_hockey/overrides', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-user-role': 'editor',
      },
      body: JSON.stringify({
        overrideType: 'note',
        eventInstanceId: 'inst_grayson_hockey_1',
        payload: { note: 'Need to leave early' },
      }),
    });
    const duplicateCtx = createExecutionContext();
    const duplicateResponse = await worker.fetch(duplicateRequest, env, duplicateCtx);
    await waitOnExecutionContext(duplicateCtx);
    const duplicateBody = await duplicateResponse.json();

    expect(duplicateResponse.status).toBe(400);
    expect(duplicateBody.message).toContain('already active');
  });

  it('applies source title rewrite rules on ingest and restores original titles when cleared', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_title_rules',
      name: 'grayson-tigers',
      display_name: 'Grayson Tigers',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/tigers.ics',
      icon: '',
      prefix: 'G:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 0,
      title_rewrite_rules_json: JSON.stringify([{ pattern: 'u11a', flags: 'gi', replacement: 'Tigers' }]),
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
            'UID:tigers-1',
            'SUMMARY:U11A Practice',
            'DTSTART:20990101T180000Z',
            'DTEND:20990101T190000Z',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        )
      )
    );

    const repo = new D1Repository(db, env);
    await repo.ensureSupportTables();
    await repo.ingestSource('src_title_rules');

    expect(db.canonicalEvents).toHaveLength(1);
    expect(db.canonicalEvents[0].source_title_raw).toBe('U11A Practice');
    expect(db.canonicalEvents[0].title).toBe('Tigers Practice');

    await repo.updateSource('src_title_rules', {
      title_rewrite_rules_text: '/Practice/g => Game'
    });

    expect(db.canonicalEvents[0].source_title_raw).toBe('U11A Practice');
    expect(db.canonicalEvents[0].title).toBe('U11A Game');

    await repo.updateSource('src_title_rules', {
      title_rewrite_rules_text: ''
    });

    expect(db.canonicalEvents[0].title).toBe('U11A Practice');
  });

  it('rejects unsafe title rewrite rules', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_unsafe_rules',
      name: 'unsafe-rules',
      display_name: 'Unsafe Rules',
      provider_type: 'ics',
      owner_type: 'grayson',
      source_category: 'sports',
      url: 'https://example.com/unsafe.ics',
      icon: '',
      prefix: '',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 0,
      include_in_child_google_output: 0,
      title_rewrite_rules_json: '[]',
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    const repo = new D1Repository(db, env);
    await repo.ensureSupportTables();

    await expect(repo.updateSource('src_unsafe_rules', {
      title_rewrite_rules_text: '/(a+)+$/ => Tigers',
    })).rejects.toThrow(/unsafe regex/i);
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
            'DTSTART:20990310T010000Z',
            'DTEND:20990310T023000Z',
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
    expect(global.fetch).toHaveBeenCalledWith(
      'https://example.com/naomi-volleyball.ics',
      expect.objectContaining({ cf: { cacheTtl: 0 } })
    );

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

  it('skips ingest and google sync when the upstream source returns 304 not modified', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_not_modified',
      name: 'family-cacheable',
      display_name: 'Family Cacheable',
      provider_type: 'ics',
      owner_type: 'family',
      source_category: 'shared',
      url: 'https://example.com/family-cacheable.ics',
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
    db.sourceSnapshots.push({
      id: 'snap_prev',
      source_id: 'src_not_modified',
      fetched_at: '2026-03-03T00:00:00.000Z',
      http_status: 200,
      etag: '"etag-prev"',
      last_modified: 'Mon, 03 Mar 2026 00:00:00 GMT',
      payload_blob_ref: null,
      payload_hash: 'hash-prev',
      parse_status: 'parsed',
      parse_error_summary: null,
    });
    const fetchMock = vi.fn(async (_url, options = {}) => {
      expect(options.headers['If-None-Match']).toBe('"etag-prev"');
      expect(options.headers['If-Modified-Since']).toBe('Mon, 03 Mar 2026 00:00:00 GMT');
      return new Response(null, { status: 304 });
    });
    vi.stubGlobal('fetch', fetchMock);

    const repo = new D1Repository(db, env);
    const result = await repo.ingestSource('src_not_modified');

    expect(result.fetchState).toBe('not_modified');
    expect(result.dataChanged).toBe(false);
    expect(result.googleSync.queued_jobs).toBe(0);
    expect(env.JOBS_QUEUE.sent).toHaveLength(0);
    expect(db.sourceSnapshots.at(-1)?.parse_status).toBe('not_modified');
  });

  it('skips ingest and google sync when a 200 response has the same payload hash', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_same_payload',
      name: 'family-same-payload',
      display_name: 'Family Same Payload',
      provider_type: 'ics',
      owner_type: 'family',
      source_category: 'shared',
      url: 'https://example.com/family-same-payload.ics',
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
    const payload = [
      'BEGIN:VCALENDAR',
      'BEGIN:VEVENT',
      'UID:family-same-payload-1',
      'SUMMARY:Family Dinner',
      'DTSTART:20260610T010000Z',
      'DTEND:20260610T020000Z',
      'END:VEVENT',
      'END:VCALENDAR',
    ].join('\r\n');
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(payload, {
          status: 200,
          headers: { etag: '"etag-same"', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' },
        })
      )
    );

    const repo = new D1Repository(db, env);
    const first = await repo.ingestSource('src_same_payload');
    expect(first.fetchState).toBe('changed');
    env.JOBS_QUEUE.sent = [];
    const second = await repo.ingestSource('src_same_payload');

    expect(second.fetchState).toBe('unchanged_payload');
    expect(second.dataChanged).toBe(false);
    expect(second.googleSync.queued_jobs).toBe(0);
    expect(env.JOBS_QUEUE.sent).toHaveLength(0);
  });

  it('skips google sync when payload changes but effective source state does not', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.DEFAULT_LOOKBACK_DAYS = '36500';
    env.GOOGLE_SERVICE_ACCOUNT_JSON = await createServiceAccountJson();
    const db = new FakeDb();
    env.APP_DB = db;
    db.outputTargets.push({
      id: 'outt_google_same_state',
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
      id: 'src_same_state',
      name: 'family-same-state',
      display_name: 'Family Same State',
      provider_type: 'ics',
      owner_type: 'family',
      source_category: 'shared',
      url: 'https://example.com/family-same-state.ics',
      icon: '',
      prefix: '',
      fetch_url_secret_ref: null,
      include_in_child_ics: 0,
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
      id: 'stl_same_state',
      source_id: 'src_same_state',
      target_id: 'outt_google_same_state',
      target_key: 'family_clubs',
      target_type: 'google',
      icon: '',
      prefix: '',
      sort_order: 0,
      is_enabled: 1,
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    let variant = 1;
    vi.stubGlobal('fetch', vi.fn(async (url, options = {}) => {
      if (String(url) === 'https://example.com/family-same-state.ics') {
        const lines = variant === 1
          ? [
              'BEGIN:VCALENDAR',
              'BEGIN:VEVENT',
              'UID:family-same-state-1',
              'SUMMARY:Family Dinner',
              'DTSTART:20260610T010000Z',
              'DTEND:20260610T020000Z',
              'END:VEVENT',
              'END:VCALENDAR',
            ]
          : [
              'BEGIN:VCALENDAR',
              'BEGIN:VEVENT',
              'UID:family-same-state-1',
              'DTEND:20260610T020000Z',
              'DTSTART:20260610T010000Z',
              'SUMMARY:Family Dinner',
              'END:VEVENT',
              'END:VCALENDAR',
            ];
        return new Response(lines.join('\r\n'), {
          status: 200,
          headers: { etag: `"state-${variant}"`, 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' },
        });
      }
      if (String(url) === 'https://oauth2.googleapis.com/token') {
        return new Response(JSON.stringify({ access_token: 'google-token' }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
      if (String(url) === 'https://www.googleapis.com/calendar/v3/calendars/family-clubs%40group.calendar.google.com/events' && options.method === 'POST') {
        return new Response(JSON.stringify({ id: 'google-same-state-1', etag: 'etag-1' }), { status: 200, headers: { 'content-type': 'application/json' } });
      }
      throw new Error(`Unexpected fetch: ${url} ${options.method || 'GET'}`);
    }));

    const repo = new D1Repository(db, env);
    const first = await repo.ingestSource('src_same_state');
    expect(first.googleSync.queued_jobs).toBe(1);
    await drainQueue(env);
    env.JOBS_QUEUE.sent = [];

    variant = 2;
    const second = await repo.ingestSource('src_same_state');

    expect(second.fetchState).toBe('changed');
    expect(second.dataChanged).toBe(false);
    expect(second.googleSync.queued_jobs).toBe(0);
    expect(env.JOBS_QUEUE.sent).toHaveLength(0);
  });

  it('prunes old sync job history while keeping current queued and running jobs', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.PRUNE_AFTER_DAYS = '30';
    env.JOB_HISTORY_RETAIN_DAYS_COMPLETED = '7';
    env.JOB_HISTORY_RETAIN_DAYS_FAILED = '30';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sourceSnapshots.push(
      {
        id: 'snap_old',
        source_id: 'src_prune',
        fetched_at: '2026-01-15T00:00:00.000Z',
        http_status: 200,
        etag: null,
        last_modified: null,
        payload_blob_ref: 'snapshots/src_prune/snap_old.ics',
        payload_hash: 'hash-old',
        parse_status: 'parsed',
        parse_error_summary: null,
      },
      {
        id: 'snap_recent',
        source_id: 'src_prune',
        fetched_at: '2026-03-18T00:00:00.000Z',
        http_status: 200,
        etag: null,
        last_modified: null,
        payload_blob_ref: null,
        payload_hash: 'hash-recent',
        parse_status: 'parsed',
        parse_error_summary: null,
      }
    );
    db.syncJobs.push(
      {
        id: 'job_completed_old',
        job_type: 'sync_google_target',
        scope_type: 'source_target',
        scope_id: 'scope_old_completed',
        status: 'completed',
        started_at: '2026-03-01T00:00:00.000Z',
        finished_at: '2026-03-01T00:05:00.000Z',
        summary_json: null,
        error_json: null,
        attempt_count: 0,
        last_error_kind: null,
      },
      {
        id: 'job_completed_recent',
        job_type: 'sync_google_target',
        scope_type: 'source_target',
        scope_id: 'scope_recent_completed',
        status: 'completed',
        started_at: '2026-03-18T00:00:00.000Z',
        finished_at: '2026-03-18T00:05:00.000Z',
        summary_json: null,
        error_json: null,
        attempt_count: 0,
        last_error_kind: null,
      },
      {
        id: 'job_failed_old',
        job_type: 'ingest_source',
        scope_type: 'source',
        scope_id: 'scope_old_failed',
        status: 'failed',
        started_at: '2026-02-01T00:00:00.000Z',
        finished_at: '2026-02-01T00:01:00.000Z',
        summary_json: null,
        error_json: '{"message":"old failed"}',
        attempt_count: 1,
        last_error_kind: 'network_error',
      },
      {
        id: 'job_failed_recent',
        job_type: 'ingest_source',
        scope_type: 'source',
        scope_id: 'scope_recent_failed',
        status: 'failed',
        started_at: '2026-03-18T00:00:00.000Z',
        finished_at: '2026-03-18T00:01:00.000Z',
        summary_json: null,
        error_json: '{"message":"recent failed"}',
        attempt_count: 1,
        last_error_kind: 'network_error',
      },
      {
        id: 'job_queued_current',
        job_type: 'ingest_source',
        scope_type: 'source',
        scope_id: 'scope_queued',
        status: 'queued',
        started_at: '2026-03-19T00:00:00.000Z',
        finished_at: null,
        summary_json: null,
        error_json: null,
        attempt_count: 0,
        last_error_kind: null,
      },
      {
        id: 'job_running_current',
        job_type: 'sync_google_target',
        scope_type: 'source_target',
        scope_id: 'scope_running',
        status: 'running',
        started_at: '2026-03-19T00:00:00.000Z',
        finished_at: null,
        summary_json: null,
        error_json: null,
        attempt_count: 0,
        last_error_kind: null,
      }
    );
    env.SNAPSHOTS = {
      delete: vi.fn(async () => undefined),
    };

    const repo = new D1Repository(db, env);
    const summary = await repo.pruneStaleData();

    expect(summary.snapshotRowsDeleted).toBe(1);
    expect(summary.snapshotBlobKeysDeleted).toBe(1);
    expect(summary.completedJobsDeleted).toBe(1);
    expect(summary.failedJobsDeleted).toBe(1);
    expect(summary.completedJobRetainDays).toBe(7);
    expect(summary.failedJobRetainDays).toBe(30);
    expect(db.sourceSnapshots.map((row) => row.id)).toEqual(['snap_recent']);
    expect(db.syncJobs.map((row) => row.id)).toEqual([
      'job_completed_recent',
      'job_failed_recent',
      'job_queued_current',
      'job_running_current',
    ]);
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
            'DTSTART:20260610T010000Z',
            'DTEND:20260610T020000Z',
            'RRULE:FREQ=DAILY;COUNT=2',
            'END:VEVENT',
            'BEGIN:VEVENT',
            'UID:grayson-series-1',
            'RECURRENCE-ID:20260611T010000Z',
            'SUMMARY:Hockey Practice - Moved',
            'DTSTART:20260611T030000Z',
            'DTEND:20260611T040000Z',
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
    expect(db.eventInstances.some((instance) => instance.recurrence_instance_key === '2026-06-11T01:00:00.000Z' && instance.occurrence_start_at === '2026-06-11T03:00:00.000Z')).toBe(true);
  });

  it('skips ingesting single events older than the past retention window', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    env.INGEST_PAST_RETENTION_DAYS = '30';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_old_history',
      name: 'family-history',
      display_name: 'Family History',
      provider_type: 'ics',
      owner_type: 'family',
      source_category: 'shared',
      url: 'https://example.com/family-history.ics',
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

    const toIcsUtc = (date) => date.toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
    const oldStart = new Date(Date.now() - 40 * 24 * 60 * 60 * 1000);
    const oldEnd = new Date(oldStart.getTime() + 60 * 60 * 1000);

    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:family-history-1',
            'SUMMARY:Old Family Event',
            `DTSTART:${toIcsUtc(oldStart)}`,
            `DTEND:${toIcsUtc(oldEnd)}`,
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        )
      )
    );

    const repo = new D1Repository(db, env);
    const result = await repo.ingestSource('src_old_history');

    expect(result.eventsParsed).toBe(1);
    expect(result.instancesMaterialized).toBe(0);
    expect(db.canonicalEvents).toHaveLength(0);
    expect(db.eventInstances).toHaveLength(0);
    expect(db.outputRules).toHaveLength(0);
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
    env.DEFAULT_LOOKBACK_DAYS = '36500';
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
            'DTSTART:20990310T010000Z',
            'DTEND:20990310T023000Z',
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
    const createCall = fetchMock.mock.calls.find(
      ([url, options]) =>
        String(url) === 'https://www.googleapis.com/calendar/v3/calendars/grayson-clubs%40group.calendar.google.com/events' &&
        options?.method === 'POST'
    );
    expect(createCall).toBeTruthy();
    expect(JSON.parse(createCall[1].body).summary).toBe('🏒 Hockey Practice');

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
            'DTSTART:20990310T010000Z',
            'DTEND:20990310T023000Z',
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

  it('reapplies active skip overrides after ingest rebuilds output rules', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_override_rebuild',
      name: 'naomi-stormfest',
      display_name: 'Naomi Stormfest',
      provider_type: 'ics',
      owner_type: 'naomi',
      source_category: 'sports',
      url: 'https://example.com/stormfest.ics',
      icon: '',
      prefix: 'N:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 0,
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
            'UID:stormfest-1',
            'SUMMARY:C/R Stormfest 2026 - Reign U13C at Oceanside',
            'DTSTART:20260320T174527',
            'DTEND:20260320T190027',
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        )
      )
    );

    const repo = new D1Repository(db, env);
    await repo.ensureSupportTables();
    await repo.ingestSource('src_override_rebuild');

    const event = db.canonicalEvents[0];
    const instance = db.eventInstances[0];
    await repo.createOverride({
      eventId: event.id,
      eventInstanceId: instance.id,
      overrideType: 'skip',
      payload: {},
      actorRole: 'editor',
    });

    expect(db.outputRules.every((rule) => rule.include_state === 'excluded')).toBe(true);

    await repo.ingestSource('src_override_rebuild');

    expect(db.outputRules.every((rule) => rule.include_state === 'excluded')).toBe(true);
    expect(db.outputRules.every((rule) => rule.derived_reason === 'override:skip')).toBe(true);
  });

  it('keeps single-event skip overrides when the upstream time changes', async () => {
    env.SEED_SAMPLE_DATA = 'false';
    const db = new FakeDb();
    env.APP_DB = db;
    db.sources.push({
      id: 'src_override_remap',
      name: 'naomi-stormfest-remap',
      display_name: 'Naomi Stormfest Remap',
      provider_type: 'ics',
      owner_type: 'naomi',
      source_category: 'sports',
      url: 'https://example.com/stormfest-remap.ics',
      icon: '',
      prefix: 'N:',
      fetch_url_secret_ref: null,
      include_in_child_ics: 1,
      include_in_family_ics: 1,
      include_in_child_google_output: 0,
      is_active: 1,
      sort_order: 0,
      poll_interval_minutes: 30,
      quality_profile: 'standard',
      created_at: '2026-03-03T00:00:00.000Z',
      updated_at: '2026-03-03T00:00:00.000Z',
    });

    let startStamp = '20260320T174527';
    let endStamp = '20260320T190027';
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(
          [
            'BEGIN:VCALENDAR',
            'BEGIN:VEVENT',
            'UID:stormfest-remap-1',
            'SUMMARY:C/R Stormfest 2026 - Reign U13C at Oceanside',
            `DTSTART:${startStamp}`,
            `DTEND:${endStamp}`,
            'END:VEVENT',
            'END:VCALENDAR',
          ].join('\r\n'),
          { status: 200, headers: { etag: 'abc123', 'last-modified': 'Mon, 03 Mar 2026 00:00:00 GMT' } }
        )
      )
    );

    const repo = new D1Repository(db, env);
    await repo.ensureSupportTables();
    await repo.ingestSource('src_override_remap');

    const event = db.canonicalEvents[0];
    const originalInstanceId = db.eventInstances[0].id;
    await repo.createOverride({
      eventId: event.id,
      eventInstanceId: originalInstanceId,
      overrideType: 'skip',
      payload: {},
      actorRole: 'editor',
    });

    startStamp = '20260320T184527';
    endStamp = '20260320T200027';
    await repo.ingestSource('src_override_remap');

    const movedInstance = db.eventInstances.find((row) => !row.source_deleted);
    expect(movedInstance.id).not.toBe(originalInstanceId);
    expect(db.eventOverrides[0].event_instance_id).toBe(movedInstance.id);
    expect(db.outputRules.filter((rule) => rule.event_instance_id === movedInstance.id).every((rule) => rule.include_state === 'excluded')).toBe(true);
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
      start_at: '2099-03-10T01:00:00.000Z',
      end_at: '2099-03-10T02:00:00.000Z',
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
      occurrence_start_at: '2099-03-10T01:00:00.000Z',
      occurrence_end_at: '2099-03-10T02:00:00.000Z',
      recurrence_instance_key: '2099-03-10T01:00:00.000Z',
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
    env.DEFAULT_LOOKBACK_DAYS = '36500';
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
              'DTSTART:20990310T010000Z',
              'DTEND:20990310T023000Z',
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
    env.DEFAULT_LOOKBACK_DAYS = '36500';
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
            'DTSTART:20990310T010000Z',
            'DTEND:20990310T020000Z',
            'END:VEVENT',
            'BEGIN:VEVENT',
            'UID:grayson-rate-limit-2',
            'SUMMARY:Practice Two',
            'DTSTART:20990311T010000Z',
            'DTEND:20990311T020000Z',
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
