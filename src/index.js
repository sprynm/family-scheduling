import { timingSafeEqual } from 'node:crypto';
import { getRoleFromRequest, requireRole } from './lib/auth.js';
import { ADMIN_ROLES, FEED_CACHE_MAX_AGE_DEFAULT } from './lib/constants.js';
import { createRepository } from './lib/repository.js';
import { json, text } from './lib/responses.js';
import { DEFAULT_ICS_UPLOAD_MAX_BYTES, normalizeAndValidateUploadedICS } from './lib/source-upload.js';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseOptionalPositiveInt(value) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function logEvent(level, event, fields = {}) {
  const logger = level === 'error' ? console.error : console.log;
  logger(JSON.stringify({ event, ...fields }));
}

function getRetryDelaySchedule(env) {
  const configured = String(env.JOB_RETRY_DELAYS_SECONDS || '').trim();
  if (!configured) return [60, 300, 900, 3600, 10800];
  return configured
    .split(',')
    .map((value) => Number.parseInt(value.trim(), 10))
    .filter((value) => Number.isFinite(value) && value > 0);
}

function getMaxRetryAttempts(env) {
  const schedule = getRetryDelaySchedule(env);
  const configured = Number.parseInt(String(env.JOB_RETRY_MAX_ATTEMPTS || ''), 10);
  if (Number.isFinite(configured) && configured > 0) {
    return configured;
  }
  return schedule.length;
}

function getRetryJitterPct(env) {
  const configured = Number.parseFloat(String(env.JOB_RETRY_JITTER_PCT || '0.2'));
  return Number.isFinite(configured) && configured >= 0 ? configured : 0.2;
}

function addRetryJitter(delaySeconds, jitterPct) {
  if (!(delaySeconds > 0) || !(jitterPct > 0)) return delaySeconds;
  const jitterSeconds = Math.floor(Math.random() * Math.max(1, Math.round(delaySeconds * jitterPct)));
  return delaySeconds + jitterSeconds;
}

function classifyQueueError(error) {
  const message = error instanceof Error ? error.message : String(error);
  const status = Number(error?.status || 0);
  const retryAfterSeconds = Number(error?.retryAfterSeconds || 0) || 0;

  if (error?.googleRateLimited) {
    return { retryable: true, errorKind: 'google_rate_limit', retryAfterSeconds };
  }
  if (/queue send failed: too many requests/i.test(message)) {
    return { retryable: true, errorKind: 'queue_rate_limit', retryAfterSeconds };
  }
  if (/Source fetch failed .* HTTP 5\d\d/i.test(message) || status >= 500) {
    return { retryable: true, errorKind: 'upstream_5xx', retryAfterSeconds };
  }
  if (error instanceof TypeError) {
    return { retryable: true, errorKind: 'network_error', retryAfterSeconds };
  }
  if (/GOOGLE_SERVICE_ACCOUNT_JSON is required/i.test(message) || /missing required/i.test(message)) {
    return { retryable: false, errorKind: 'config_error', retryAfterSeconds };
  }
  if (/Unknown source|Source is inactive|Source URL must use http or https/i.test(message)) {
    return { retryable: false, errorKind: 'source_state_error', retryAfterSeconds };
  }
  if (/Source fetch failed .* HTTP 4\d\d/i.test(message) || (status >= 400 && status < 500)) {
    return { retryable: false, errorKind: 'upstream_4xx', retryAfterSeconds };
  }
  return { retryable: false, errorKind: 'processing_error', retryAfterSeconds };
}

function computeRetryDelaySeconds(env, attemptCount, retryAfterSeconds = 0) {
  const schedule = getRetryDelaySchedule(env);
  const baseDelay = schedule[Math.max(0, Math.min(attemptCount - 1, schedule.length - 1))] || schedule[schedule.length - 1] || 60;
  const delayed = Math.max(baseDelay, Number(retryAfterSeconds || 0) || 0);
  return addRetryJitter(delayed, getRetryJitterPct(env));
}

function getRouteTarget(pathname) {
  const match = pathname.toLowerCase().match(/^\/(family|grayson|naomi)\.(ics|isc)$/);
  return match?.[1] || null;
}

function getCalendarName(env, target, requestedName) {
  if (requestedName) return requestedName;
  if (target === 'family') return env.CALENDAR_NAME_FAMILY || 'Family Combined';
  if (target === 'grayson') return env.CALENDAR_NAME_GRAYSON || 'Grayson Combined';
  return env.CALENDAR_NAME_NAOMI || 'Naomi Combined';
}

function timingSafeTokenEquals(actual, expected) {
  const encoder = new TextEncoder();
  const actualBytes = encoder.encode(String(actual ?? ''));
  const expectedBytes = encoder.encode(String(expected ?? ''));
  const maxLength = Math.max(actualBytes.length, expectedBytes.length, 1);
  const left = new Uint8Array(maxLength);
  const right = new Uint8Array(maxLength);
  left.set(actualBytes);
  right.set(expectedBytes);
  return timingSafeEqual(Buffer.from(left), Buffer.from(right)) && actualBytes.length === expectedBytes.length;
}

function requireFeedToken(request, env) {
  const configuredToken = env.TOKEN || '';
  if (!configuredToken) return null;
  const token = new URL(request.url).searchParams.get('token');
  if (!timingSafeTokenEquals(token, configuredToken)) {
    return text('Unauthorized', { status: 401 });
  }
  return null;
}

const UNPROTECTED_PAGE_REDIRECTS = {
  '/': '/admin/',
  '/admin': '/admin/',
  '/admin.html': '/admin/',
  '/admin-events': '/admin/events',
  '/admin-events.html': '/admin/events',
  '/admin-feeds': '/admin/feeds',
  '/admin-feeds.html': '/admin/feeds',
};

function getAdminAssetPath(pathname) {
  if (pathname === '/admin/events' || pathname === '/admin/events/') {
    return '/admin-events.html';
  }
  if (pathname === '/admin/feeds' || pathname === '/admin/feeds/') {
    return '/admin-feeds.html';
  }
  return '/admin.html';
}

function getFeedCacheMaxAge(env) {
  const value = Number.parseInt(String(env.FEED_CACHE_MAX_AGE || FEED_CACHE_MAX_AGE_DEFAULT), 10);
  return Number.isFinite(value) && value > 0 ? value : FEED_CACHE_MAX_AGE_DEFAULT;
}

function buildAdminAssetRequest(request) {
  const url = new URL(request.url);
  url.pathname = getAdminAssetPath(url.pathname);
  url.search = '';
  return new Request(url.toString(), request);
}

async function serveAdminAsset(request, env) {
  if (!env.ASSETS?.fetch) {
    return text('Admin assets binding is required', { status: 500 });
  }
  const response = await env.ASSETS.fetch(buildAdminAssetRequest(request));
  const headers = new Headers(response.headers);
  headers.set('X-Robots-Tag', 'noindex, nofollow');
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function asBadRequest(error) {
  return json(
    {
      error: 'Bad request',
      message: error instanceof Error ? error.message : String(error),
    },
    { status: 400 }
  );
}

class UploadRequestError extends Error {
  constructor(message, status = 400) {
    super(message);
    this.name = 'UploadRequestError';
    this.status = status;
  }
}

function requireSameOriginUploadRequest(request) {
  const origin = request.headers.get('origin');
  const fetchSite = request.headers.get('sec-fetch-site');
  if (fetchSite && fetchSite.toLowerCase() === 'cross-site') {
    return json({ error: 'Forbidden', message: 'Upload requests must come from the admin site' }, { status: 403 });
  }
  if (!origin) return null;
  try {
    if (new URL(origin).origin === new URL(request.url).origin) return null;
  } catch {}
  return json({ error: 'Forbidden', message: 'Upload requests must come from the admin site' }, { status: 403 });
}

async function readRequestBodyLimited(request, maxBytes) {
  const reader = request.body?.getReader();
  if (!reader) throw new UploadRequestError('Expected a multipart ICS upload body');
  const chunks = [];
  let totalBytes = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      totalBytes += value.byteLength;
      if (totalBytes > maxBytes) {
        try { await reader.cancel(); } catch {}
        throw new UploadRequestError(`Upload request exceeds the ${maxBytes}-byte request limit`, 413);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const body = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function sanitizeUploadFilename(value) {
  const basename = String(value || '').split(/[\\/]/).pop().replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 200);
  if (!basename || !/\.ics$/i.test(basename)) {
    throw new UploadRequestError('Choose an .ics calendar file');
  }
  return basename;
}

async function parseIcsUploadRequest(request, env) {
  const maxFileBytes = parseOptionalPositiveInt(env.ICS_UPLOAD_MAX_BYTES) || DEFAULT_ICS_UPLOAD_MAX_BYTES;
  const maxRequestBytes = maxFileBytes + 64 * 1024;
  const contentLength = Number.parseInt(String(request.headers.get('content-length') || ''), 10);
  if (Number.isFinite(contentLength) && contentLength > maxRequestBytes) {
    throw new UploadRequestError(`Upload request exceeds the ${maxRequestBytes}-byte request limit`, 413);
  }
  if (!String(request.headers.get('content-type') || '').toLowerCase().startsWith('multipart/form-data;')) {
    throw new UploadRequestError('Upload must use multipart/form-data');
  }

  const body = await readRequestBodyLimited(request, maxRequestBytes);
  let form;
  try {
    form = await new Request(request.url, {
      method: request.method,
      headers: request.headers,
      body,
    }).formData();
  } catch {
    throw new UploadRequestError('Could not read the multipart ICS upload');
  }
  const metadataText = form.get('metadata');
  let metadata;
  try {
    metadata = JSON.parse(String(metadataText || ''));
  } catch {
    throw new UploadRequestError('Upload metadata must be valid JSON');
  }
  if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) {
    throw new UploadRequestError('Upload metadata must be a JSON object');
  }
  const file = form.get('file');
  if (!file || typeof file.text !== 'function' || typeof file.size !== 'number') {
    throw new UploadRequestError('Choose an .ics calendar file');
  }
  if (file.size < 1) throw new UploadRequestError('The ICS file is empty');
  if (file.size > maxFileBytes) throw new UploadRequestError(`ICS file exceeds the ${maxFileBytes}-byte file limit`, 413);
  const fileName = sanitizeUploadFilename(file.name);
  const rawText = await file.text();
  const validated = normalizeAndValidateUploadedICS(rawText, {
    defaultFloatingTimeZone: env.DEFAULT_FLOATING_TIMEZONE || 'UTC',
    horizonDays: Number.parseInt(String(env.RECURRENCE_HORIZON_DAYS || '180'), 10) || 180,
    lookbackDays: Number.parseInt(String(env.DEFAULT_LOOKBACK_DAYS || '7'), 10) || 7,
    maxBytes: maxFileBytes,
    maxEvents: parseOptionalPositiveInt(env.ICS_UPLOAD_MAX_EVENTS) || 5000,
    maxInstances: parseOptionalPositiveInt(env.ICS_UPLOAD_MAX_INSTANCES) || 5000,
  });
  return { metadata, fileName, ...validated };
}

async function buildInternalErrorResponse(request, env, error) {
  let includeDetails = false;
  try {
    const role = await getRoleFromRequest(request, env);
    includeDetails = ADMIN_ROLES.includes(role);
  } catch {}
  return json(
    {
      error: 'Internal error',
      message: includeDetails
        ? (error instanceof Error ? error.message : String(error))
        : 'An internal error occurred.',
    },
    { status: 500 }
  );
}

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const target = getRouteTarget(pathname);

      // Cloudflare Access protects /admin/*, not these bare page URLs; send them under it before anything renders.
      const protectedPagePath = UNPROTECTED_PAGE_REDIRECTS[pathname];
      if (protectedPagePath) {
        return Response.redirect(`${url.origin}${protectedPagePath}${url.search}`, 302);
      }

      if (
        pathname === '/admin/' ||
        pathname === '/admin/events' ||
        pathname === '/admin/events/' ||
        pathname === '/admin/feeds' ||
        pathname === '/admin/feeds/'
      ) {
        const authError = await requireRole(request, env, ADMIN_ROLES);
        if (authError) return authError;
        return serveAdminAsset(request, env);
      }

      if (target) {
        const authError = requireFeedToken(request, env);
        if (authError) return authError;
        const repo = await createRepository(env);
        const name = getCalendarName(env, target, url.searchParams.get('name'));
        const lookbackDays = Number(url.searchParams.get('lookback') || env.DEFAULT_LOOKBACK_DAYS || 7);
        const body = await repo.generateFeed({ target, calendarName: name, lookbackDays });
        return new Response(body, {
          headers: {
            'content-type': 'text/calendar; charset=utf-8',
            'content-disposition': `attachment; filename="${target}.ics"`,
            'cache-control': `public, max-age=${getFeedCacheMaxAge(env)}`,
          },
        });
      }

      if (pathname === '/api/feed-contracts' && request.method === 'GET') {
        const authError = await requireRole(request, env, ADMIN_ROLES);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json(await repo.getFeedContract(request.url));
      }

      if (pathname === '/api/feed-preview' && request.method === 'GET') {
        const authError = await requireRole(request, env, ADMIN_ROLES);
        if (authError) return authError;
        const target = String(url.searchParams.get('target') || '').trim().toLowerCase();
        if (!target || !['family', 'grayson', 'naomi'].includes(target)) {
          return json({ error: 'Bad request', message: 'target must be family, grayson, or naomi' }, { status: 400 });
        }
        const repo = await createRepository(env);
        const contract = await repo.getFeedContract(request.url);
        const calendarName = getCalendarName(env, target, url.searchParams.get('name'));
        const lookbackDays = Number(url.searchParams.get('lookback') || env.DEFAULT_LOOKBACK_DAYS || 7);
        const events = await repo.listFeedPreview({ target, lookbackDays });
        return json({
          target,
          calendarName,
          lookbackDays,
          feedUrl: (contract.contracts || []).find((item) => item.target === target)?.url || null,
          events,
        });
      }

      if (pathname === '/api/sources' && request.method === 'GET') {
        const authError = await requireRole(request, env, ADMIN_ROLES);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({ sources: await repo.listSources() });
      }

      if (pathname === '/api/sources' && request.method === 'POST') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        try {
          const repo = await createRepository(env);
          const payload = await request.json();
          return json({ source: await repo.createSource(payload) }, { status: 201 });
        } catch (error) {
          return asBadRequest(error);
        }
      }

      if (pathname === '/api/sources/upload' && request.method === 'POST') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        const originError = requireSameOriginUploadRequest(request);
        if (originError) return originError;
        try {
          const upload = await parseIcsUploadRequest(request, env);
          const repo = await createRepository(env);
          const result = await repo.createUploadedSource(upload.metadata, upload);
          return json(result, { status: 202 });
        } catch (error) {
          const status = Number(error?.status) || (/storage is unavailable/i.test(String(error?.message || '')) ? 503 : 400);
          return json({ error: status === 413 ? 'Payload too large' : 'Upload failed', message: error instanceof Error ? error.message : String(error) }, { status });
        }
      }

      const sourceUploadMatch = pathname.match(/^\/api\/sources\/([^/]+)\/upload$/);
      if (sourceUploadMatch && request.method === 'POST') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        const originError = requireSameOriginUploadRequest(request);
        if (originError) return originError;
        try {
          const sourceId = decodeURIComponent(sourceUploadMatch[1]);
          const upload = await parseIcsUploadRequest(request, env);
          const repo = await createRepository(env);
          const existing = await repo.getSourceById(sourceId);
          if (!existing) return json({ error: 'Not found' }, { status: 404 });
          const metadata = { ...upload.metadata };
          delete metadata.provider_type;
          delete metadata.url;
          const result = await repo.replaceSourceUpload(sourceId, metadata, upload);
          if (!result) return json({ error: 'Not found' }, { status: 404 });
          return json(result, { status: 202 });
        } catch (error) {
          const status = Number(error?.status) || (/storage is unavailable/i.test(String(error?.message || '')) ? 503 : 400);
          return json({ error: status === 413 ? 'Payload too large' : 'Upload failed', message: error instanceof Error ? error.message : String(error) }, { status });
        }
      }

      if (pathname === '/api/jobs' && request.method === 'GET') {
        const authError = await requireRole(request, env, ADMIN_ROLES);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({ jobs: await repo.listJobs() });
      }

      if (pathname === '/api/targets' && request.method === 'GET') {
        const authError = await requireRole(request, env, ADMIN_ROLES);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({ targets: await repo.listTargets() });
      }

      if (pathname === '/api/targets' && request.method === 'POST') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        try {
          const repo = await createRepository(env);
          const payload = await request.json();
          return json({ target: await repo.upsertTarget(payload) }, { status: 201 });
        } catch (error) {
          return asBadRequest(error);
        }
      }

      if (pathname.startsWith('/api/targets/') && request.method === 'DELETE') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        try {
          const targetKey = decodeURIComponent(pathname.split('/').filter(Boolean)[2] || '');
          const repo = await createRepository(env);
          const removed = await repo.deleteTarget(targetKey);
          if (!removed) return json({ error: 'Not found' }, { status: 404 });
          return json({ deleted: removed });
        } catch (error) {
          return asBadRequest(error);
        }
      }

      if (pathname === '/api/instances' && request.method === 'GET') {
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({
          instances: await repo.listInstances({
            sourceId: url.searchParams.get('source') || null,
            outputKey: url.searchParams.get('output') || null,
            future: url.searchParams.get('future') === '1',
            limit: Number(url.searchParams.get('limit') || 200),
          }),
        });
      }

      if (pathname === '/api/overrides' && request.method === 'GET') {
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        const now = new Date().toISOString();
        return json({ overrides: await repo.listActiveOverrides({ now }) });
      }

      if (pathname === '/api/events' && request.method === 'GET') {
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({
          events: await repo.listEvents({
            target: url.searchParams.get('target') || null,
            limit: Number(url.searchParams.get('limit') || 200),
          }),
        });
      }

      if (pathname.startsWith('/api/events/') && request.method === 'GET') {
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        const segments = pathname.split('/').filter(Boolean);
        const eventId = segments[2];
        if (segments[3] === 'instances') {
          const event = await repo.getEvent(eventId);
          if (!event) return json({ error: 'Not found' }, { status: 404 });
          return json({ instances: event.instances });
        }
        const event = await repo.getEvent(eventId);
        if (!event) return json({ error: 'Not found' }, { status: 404 });
        return json(event);
      }

      if (pathname.startsWith('/api/events/') && pathname.endsWith('/overrides') && request.method === 'POST') {
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        try {
          const segments = pathname.split('/').filter(Boolean);
          const eventId = segments[2];
          const payload = await request.json();
          const repo = await createRepository(env);
          const event = await repo.createOverride({
            eventId,
            eventInstanceId: payload.eventInstanceId || null,
            overrideType: payload.overrideType,
            payload: payload.payload || {},
            actorRole: (await getRoleFromRequest(request, env)) || 'editor',
          });
          return json(event, { status: 201 });
        } catch (error) {
          return asBadRequest(error);
        }
      }

      if (pathname.startsWith('/api/overrides/') && request.method === 'DELETE') {
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const overrideId = pathname.split('/').filter(Boolean)[2];
        const repo = await createRepository(env);
        const event = await repo.clearOverride(overrideId);
        if (!event) return json({ error: 'Not found' }, { status: 404 });
        return json(event);
      }

      if (pathname.startsWith('/api/sources/') && pathname.endsWith('/rebuild') && request.method === 'POST') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        const sourceId = pathname.split('/').filter(Boolean)[2];
        const repo = await createRepository(env);
        const job = await repo.enqueueJob({
          jobType: 'rebuild_source',
          scopeType: 'source',
          scopeId: sourceId,
        });
        return json({ job }, { status: 202 });
      }

      if (pathname.startsWith('/api/sources/') && pathname.endsWith('/diagnose') && request.method === 'POST') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        const sourceId = pathname.split('/').filter(Boolean)[2];
        const repo = await createRepository(env);
        const diagnostic = await repo.diagnoseSourceIdentity(sourceId);
        return json({ diagnostic });
      }

      if (pathname.startsWith('/api/sources/') && request.method === 'PATCH') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        try {
          const sourceId = pathname.split('/').filter(Boolean)[2];
          const repo = await createRepository(env);
          const payload = await request.json();
          const source = await repo.updateSource(sourceId, payload);
          if (!source) return json({ error: 'Not found' }, { status: 404 });
          return json({ source });
        } catch (error) {
          const status = Number(error?.status) === 503 ? 503 : 400;
          return json({ error: status === 503 ? 'Temporarily unavailable' : 'Bad request', message: error instanceof Error ? error.message : String(error) }, { status });
        }
      }

      if (pathname.startsWith('/api/sources/') && request.method === 'DELETE') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        try {
          const sourceId = pathname.split('/').filter(Boolean)[2];
          const repo = await createRepository(env);
          if (url.searchParams.get('permanent') === 'true') {
            const source = await repo.deleteSource(sourceId);
            if (!source) return json({ error: 'Not found' }, { status: 404 });
            return json({ deleted: source });
          }
          const source = await repo.disableSource(sourceId);
          if (!source) return json({ error: 'Not found' }, { status: 404 });
          return json({ source });
        } catch (error) {
          const status = Number(error?.status) === 503 ? 503 : 500;
          return json({ error: status === 503 ? 'Temporarily unavailable' : 'Delete failed', message: error instanceof Error ? error.message : String(error) }, { status });
        }
      }

      if (pathname === '/api/rebuild/full' && request.method === 'POST') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
        const repo = await createRepository(env);
        const job = await repo.enqueueJob({
          jobType: 'rebuild_system',
          scopeType: 'system',
          scopeId: null,
        });
        return json({ job }, { status: 202 });
      }

      return text('Not found', { status: 404 });
    } catch (error) {
      return buildInternalErrorResponse(request, env, error);
    }
  },
  async queue(batch, env) {
    const repo = await createRepository(env);
    for (const message of batch.messages) {
      const job = message.body || {};
      try {
        await repo.markJobStatus(job.jobId, 'running');
        let summary = { status: 'noop' };
        if (job.jobType === 'rebuild_source' || job.jobType === 'ingest_source') {
          summary = await repo.ingestSource(job.scopeId, {
            forceRefresh: job.jobType === 'rebuild_source',
            uploadId: job.uploadId || null,
            uploadRevision: job.uploadRevision ?? null,
          });
        } else if (job.jobType === 'sync_google_target') {
          summary = await repo.syncGoogleOutputsForTargetChunk(job.sourceId, job.targetId, {
            mode: job.mode || 'sync',
          });
          if (summary.has_more) {
            await repo.enqueueJob({
              jobType: 'sync_google_target',
              scopeType: 'source_target',
              scopeId: job.scopeId,
              dedupe: false,
              payload: {
                sourceId: job.sourceId,
                targetId: job.targetId,
                mode: job.mode || 'sync',
              },
            });
          }
        } else if (job.jobType === 'rebuild_system') {
          const sources = await repo.listActiveSources({ includeAll: true });
          const results = [];
          const failures = [];
          // One source failing (a lock held by its own ingest, an upstream outage) must not stop the rest.
          for (const source of sources) {
            try {
              results.push(await repo.ingestSource(source.id, { forceRefresh: true }));
            } catch (error) {
              failures.push({ sourceId: source.id, message: error instanceof Error ? error.message : String(error) });
            }
          }
          summary = { sourcesProcessed: results.length, sourcesFailed: failures.length, results, failures };
        } else if (job.jobType === 'prune_stale_data') {
          summary = await repo.pruneStaleData();
        }
        await repo.markJobStatus(job.jobId, 'completed', { summary });
        message.ack();
      } catch (error) {
        const attemptCount = Math.max(1, Number(message.attempts || job.attemptCount || 1));
        const { retryable, errorKind, retryAfterSeconds } = classifyQueueError(error);
        const maxAttempts = getMaxRetryAttempts(env);
        if (error?.sourceUploadId) {
          // Retries still ahead: keep the upload pending with the latest error. Out of retries or not
          // retryable: mark it failed so it stops blocking the source.
          const uploadErrorMessage = error instanceof Error ? error.message : String(error);
          try {
            if (retryable && attemptCount < maxAttempts) await repo.markSourceUploadError(error.sourceUploadId, uploadErrorMessage);
            else await repo.markSourceUploadFailed(error.sourceUploadId, uploadErrorMessage);
          } catch {}
        }
        const errorPayload = {
          message: error instanceof Error ? error.message : String(error),
          kind: errorKind,
        };

        if (retryable && attemptCount < maxAttempts) {
          const delaySeconds = computeRetryDelaySeconds(env, attemptCount, retryAfterSeconds);
          await repo.recordJobRetry(job.jobId, {
            attemptCount,
            lastErrorKind: errorKind,
            error: errorPayload,
          });
          logEvent('info', 'queue_job_retry_scheduled', {
            job_id: job.jobId || null,
            job_type: job.jobType || null,
            scope_type: job.scopeType || null,
            scope_id: job.scopeId || null,
            attempt_count: attemptCount,
            delay_seconds: delaySeconds,
            error_kind: errorKind,
          });
          message.retry({ delaySeconds });
          continue;
        }

        logEvent('error', 'queue_job_failed', {
          job_id: job.jobId || null,
          job_type: job.jobType || null,
          scope_type: job.scopeType || null,
          scope_id: job.scopeId || null,
          attempt_count: attemptCount,
          error_kind: errorKind,
          message: errorPayload.message,
        });
        await repo.markJobStatus(job.jobId, 'failed', {
          error: errorPayload,
          attemptCount,
          lastErrorKind: errorKind,
        });
        message.ack();
      }
    }
  },
  async scheduled(controller, env) {
    const repo = await createRepository(env);
    let staleExpired = 0;
    try {
      // Expire before enqueueing so a lost job cannot dedupe away this run's ingest.
      staleExpired = await repo.expireStaleJobs();
    } catch (error) {
      logEvent('error', 'cron_stale_job_expiry_failed', {
        cron: controller.cron,
        scheduled_time: controller.scheduledTime,
        message: error instanceof Error ? error.message : String(error),
      });
    }
    const sources = await repo.listActiveSources();
    const maxEnqueuesPerRun = parseOptionalPositiveInt(env.CRON_MAX_ENQUEUES_PER_RUN);
    const sendSpacingMs = parseOptionalPositiveInt(env.CRON_QUEUE_SEND_SPACING_MS);
    const stats = {
      cron: controller.cron,
      scheduled_time: controller.scheduledTime,
      sources_due: sources.length,
      stale_expired: staleExpired,
      enqueued: 0,
      deduped: 0,
      rate_limited: 0,
      failed: 0,
      capped: 0,
      prune_enqueued: 0,
      prune_failed: 0,
    };
    for (const source of sources) {
      if (maxEnqueuesPerRun > 0 && stats.enqueued >= maxEnqueuesPerRun) {
        stats.capped += 1;
        continue;
      }
      try {
        const job = await repo.enqueueJob({
          jobType: 'ingest_source',
          scopeType: 'source',
          scopeId: source.id,
        });
        if (job.deduped) {
          stats.deduped += 1;
        } else {
          stats.enqueued += 1;
          if (sendSpacingMs > 0) {
            await sleep(sendSpacingMs);
          }
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        stats.failed += 1;
        if (/too many requests/i.test(message)) {
          stats.rate_limited += 1;
        }
        logEvent('error', 'cron_enqueue_failed', {
          cron: controller.cron,
          scheduled_time: controller.scheduledTime,
          job_type: 'ingest_source',
          scope_type: 'source',
          scope_id: source.id,
          message,
        });
      }
    }
    if (controller.cron === '17 3 * * *') {
      try {
        const job = await repo.enqueueJob({
          jobType: 'prune_stale_data',
          scopeType: 'system',
          scopeId: null,
        });
        if (job.deduped) {
          stats.deduped += 1;
        } else {
          stats.prune_enqueued += 1;
        }
      } catch (error) {
        stats.prune_failed += 1;
        logEvent('error', 'cron_enqueue_failed', {
          cron: controller.cron,
          scheduled_time: controller.scheduledTime,
          job_type: 'prune_stale_data',
          scope_type: 'system',
          scope_id: null,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
    logEvent('info', 'cron_enqueue_summary', stats);
  },
};
