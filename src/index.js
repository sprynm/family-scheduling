import { getRoleFromRequest, requireRole } from './lib/auth.js';
import { ADMIN_ROLES, FEED_CACHE_MAX_AGE_DEFAULT } from './lib/constants.js';
import { createRepository } from './lib/repository.js';
import { json, text } from './lib/responses.js';

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

function requireFeedToken(request, env) {
  const configuredToken = env.TOKEN || '';
  if (!configuredToken) return null;
  const token = new URL(request.url).searchParams.get('token');
  if (token !== configuredToken) {
    return text('Unauthorized', { status: 401 });
  }
  return null;
}

function getAdminAssetPath(pathname) {
  if (pathname === '/admin/events' || pathname === '/admin/events/') {
    return '/admin-events.html';
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

export default {
  async fetch(request, env) {
    try {
      const url = new URL(request.url);
      const pathname = url.pathname;
      const target = getRouteTarget(pathname);

      if (pathname === '/') {
        return Response.redirect(`${url.origin}/admin`, 302);
      }

      if (pathname === '/admin' || pathname === '/admin/' || pathname === '/admin/events' || pathname === '/admin/events/') {
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
          return asBadRequest(error);
        }
      }

      if (pathname.startsWith('/api/sources/') && request.method === 'DELETE') {
        const authError = await requireRole(request, env, ['admin']);
        if (authError) return authError;
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
      return json(
        {
          error: 'Internal error',
          message: error instanceof Error ? error.message : String(error),
        },
        { status: 500 }
      );
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
          summary = await repo.ingestSource(job.scopeId);
        } else if (job.jobType === 'sync_google_target') {
          summary = await repo.syncGoogleOutputsForTargetChunk(job.sourceId, job.targetId, {
            mode: job.mode || 'sync',
          });
          if (summary.has_more) {
            await repo.enqueueJob({
              jobType: 'sync_google_target',
              scopeType: 'source_target',
              scopeId: job.scopeId,
              payload: {
                sourceId: job.sourceId,
                targetId: job.targetId,
                mode: job.mode || 'sync',
              },
            });
          }
        } else if (job.jobType === 'rebuild_system') {
          const sources = await repo.listActiveSources();
          const results = [];
          for (const source of sources) {
            results.push(await repo.ingestSource(source.id));
          }
          summary = { sourcesProcessed: results.length, results };
        } else if (job.jobType === 'prune_stale_data') {
          summary = await repo.pruneStaleData();
        }
        await repo.markJobStatus(job.jobId, 'completed', { summary });
        message.ack();
      } catch (error) {
        const attemptCount = Math.max(1, Number(message.attempts || job.attemptCount || 1));
        const { retryable, errorKind, retryAfterSeconds } = classifyQueueError(error);
        const maxAttempts = getMaxRetryAttempts(env);
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
    const sources = await repo.listActiveSources();
    const maxEnqueuesPerRun = parseOptionalPositiveInt(env.CRON_MAX_ENQUEUES_PER_RUN);
    const sendSpacingMs = parseOptionalPositiveInt(env.CRON_QUEUE_SEND_SPACING_MS);
    const stats = {
      cron: controller.cron,
      scheduled_time: controller.scheduledTime,
      sources_due: sources.length,
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
