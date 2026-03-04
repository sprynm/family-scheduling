import { getRoleFromRequest, requireRole } from './lib/auth.js';
import { TARGETS } from './lib/constants.js';
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

function renderAdminShell() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Family Scheduling Admin</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f1e8;
      --panel: #fffaf1;
      --ink: #1b2430;
      --muted: #5b6470;
      --line: #d7d0c2;
      --accent: #a34a28;
    }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: radial-gradient(circle at top, #fff7e7 0, var(--bg) 55%);
      color: var(--ink);
    }
    main {
      max-width: 980px;
      margin: 0 auto;
      padding: 40px 20px 72px;
    }
    .hero, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 20px 60px rgba(27, 36, 48, 0.08);
    }
    .hero {
      padding: 28px;
      margin-bottom: 24px;
    }
    .eyebrow {
      font-size: 12px;
      letter-spacing: 0.16em;
      text-transform: uppercase;
      color: var(--accent);
      margin: 0 0 8px;
    }
    h1 {
      margin: 0 0 10px;
      font-size: clamp(2rem, 4vw, 3.2rem);
      line-height: 1;
    }
    p {
      color: var(--muted);
      line-height: 1.5;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
    }
    .panel {
      padding: 20px;
    }
    code {
      background: #f7e7de;
      padding: 2px 6px;
      border-radius: 8px;
    }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <p class="eyebrow">Family Scheduling</p>
      <h1>Admin Console Baseline</h1>
      <p>This worker owns the compatibility feed layer and the admin API baseline. Use the JSON APIs under <code>/api</code> for sources, events, jobs, targets, rebuild, and feed contracts.</p>
      <p>Local development uses the <code>x-user-role</code> header. Production auth is intended to use Google sign-in with admin/editor role enforcement.</p>
    </section>
    <section class="grid">
      <article class="panel">
        <h2>Feeds</h2>
        <p>Legacy-compatible public routes: <code>/family.isc</code>, <code>/grayson.isc</code>, <code>/naomi.isc</code>, plus <code>.ics</code> aliases.</p>
      </article>
      <article class="panel">
        <h2>Admin APIs</h2>
        <p>Protected APIs: <code>/api/events</code>, <code>/api/sources</code>, <code>/api/jobs</code>, <code>/api/targets</code>, and rebuild endpoints.</p>
      </article>
      <article class="panel">
        <h2>Runtime</h2>
        <p>Cloudflare Worker + D1 + R2 + Queue baseline with DB-generated outputs and no request-time upstream merge on public reads.</p>
      </article>
    </section>
  </main>
</body>
</html>`;
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
        return new Response(renderAdminShell(), {
          headers: { 'content-type': 'text/html; charset=utf-8' },
        });
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
            'cache-control': 'public, max-age=300',
          },
        });
      }

      if (pathname === '/api/feed-contracts' && request.method === 'GET') {
        const authError = requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json(await repo.getFeedContract());
      }

      if (pathname === '/api/sources' && request.method === 'GET') {
        const authError = requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({ sources: await repo.listSources() });
      }

      if (pathname === '/api/sources' && request.method === 'POST') {
        const authError = requireRole(request, env, ['admin']);
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
        const authError = requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({ jobs: await repo.listJobs() });
      }

      if (pathname === '/api/targets' && request.method === 'GET') {
        const authError = requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({
          targets: await repo.listTargets(),
          logicalTargets: TARGETS,
        });
      }

      if (pathname === '/api/events' && request.method === 'GET') {
        const authError = requireRole(request, env, ['admin', 'editor']);
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
        const authError = requireRole(request, env, ['admin', 'editor']);
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
        const authError = requireRole(request, env, ['admin', 'editor']);
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
          actorRole: getRoleFromRequest(request, env) || 'editor',
        });
        return json(event, { status: 201 });
      }

      if (pathname.startsWith('/api/overrides/') && request.method === 'DELETE') {
        const authError = requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const overrideId = pathname.split('/').filter(Boolean)[2];
        const repo = await createRepository(env);
        const event = await repo.clearOverride(overrideId);
        if (!event) return json({ error: 'Not found' }, { status: 404 });
        return json(event);
      }

      if (pathname.startsWith('/api/sources/') && pathname.endsWith('/rebuild') && request.method === 'POST') {
        const authError = requireRole(request, env, ['admin']);
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
        const authError = requireRole(request, env, ['admin']);
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
        const authError = requireRole(request, env, ['admin']);
        if (authError) return authError;
        const sourceId = pathname.split('/').filter(Boolean)[2];
        const repo = await createRepository(env);
        const source = await repo.disableSource(sourceId);
        if (!source) return json({ error: 'Not found' }, { status: 404 });
        return json({ source });
      }

      if (pathname === '/api/rebuild/full' && request.method === 'POST') {
        const authError = requireRole(request, env, ['admin']);
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
          summary = { status: 'scheduled', note: 'Prune execution stubbed pending retention implementation.' };
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
