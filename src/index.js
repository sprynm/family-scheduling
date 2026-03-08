import { getRoleFromRequest, requireRole } from './lib/auth.js';
import { ADMIN_ROLES, FEED_CACHE_MAX_AGE_DEFAULT } from './lib/constants.js';
import { createRepository } from './lib/repository.js';
import { json, text } from './lib/responses.js';

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

const ADMIN_ASSET_PATH = '/admin.html';

function getFeedCacheMaxAge(env) {
  const value = Number.parseInt(String(env.FEED_CACHE_MAX_AGE || FEED_CACHE_MAX_AGE_DEFAULT), 10);
  return Number.isFinite(value) && value > 0 ? value : FEED_CACHE_MAX_AGE_DEFAULT;
}

function buildAdminAssetRequest(request) {
  const url = new URL(request.url);
  url.pathname = ADMIN_ASSET_PATH;
  url.search = '';
  return new Request(url.toString(), request);
}

async function serveAdminAsset(request, env) {
  if (!env.ASSETS?.fetch) {
    return text('Admin assets binding is required', { status: 500 });
  }
  return env.ASSETS.fetch(buildAdminAssetRequest(request));
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

      if (pathname === '/admin' || pathname === '/admin/') {
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
        await repo.markJobStatus(job.jobId, 'failed', {
          error: { message: error instanceof Error ? error.message : String(error) },
        });
        message.retry();
      }
    }
  },
  async scheduled(controller, env) {
    const repo = await createRepository(env);
    const sources = await repo.listActiveSources();
    for (const source of sources) {
      await repo.enqueueJob({
        jobType: 'ingest_source',
        scopeType: 'source',
        scopeId: source.id,
      });
    }
    if (controller.cron === '17 3 * * *') {
      await repo.enqueueJob({
        jobType: 'prune_stale_data',
        scopeType: 'system',
        scopeId: null,
      });
    }
  },
};
