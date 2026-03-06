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
      --bg: #f4f7fb;
      --panel: #ffffff;
      --ink: #162033;
      --muted: #57607a;
      --line: #d3dbe9;
      --accent: #0d7a63;
      --accent-2: #1251c2;
      --warn: #8f2c0f;
    }
    body {
      margin: 0;
      font-family: "Segoe UI", Tahoma, sans-serif;
      background:
        radial-gradient(1200px 500px at 10% -10%, #dff8ef 0, transparent 60%),
        radial-gradient(1000px 450px at 100% -10%, #dfebff 0, transparent 55%),
        var(--bg);
      color: var(--ink);
    }
    main {
      max-width: 1120px;
      margin: 0 auto;
      padding: 24px 18px 48px;
    }
    .hero, .panel, .table-panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: 0 10px 30px rgba(21, 40, 70, 0.08);
    }
    .hero {
      padding: 20px;
      margin-bottom: 16px;
    }
    .eyebrow {
      font-size: 12px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--accent);
      margin: 0 0 8px;
      font-weight: 700;
    }
    h1 {
      margin: 0 0 8px;
      font-size: clamp(1.4rem, 3.2vw, 2.1rem);
      line-height: 1.1;
    }
    p, li, td, th {
      color: var(--muted);
      line-height: 1.4;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 12px;
      margin-top: 12px;
    }
    .panel {
      padding: 14px;
    }
    .metric {
      margin: 0;
      font-size: 1.4rem;
      color: var(--ink);
      font-weight: 700;
    }
    .metric-label {
      margin: 0;
      font-size: 0.83rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 16px 0;
    }
    .inline-form {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 0 0 16px;
      align-items: center;
    }
    .form-stack {
      display: grid;
      gap: 12px;
      margin-bottom: 16px;
    }
    .step {
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--accent-2);
      font-weight: 700;
      margin: 0 0 8px;
    }
    input, select {
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      padding: 8px 10px;
      border-radius: 10px;
      font-size: 0.88rem;
      min-width: 170px;
    }
    select[multiple] {
      min-width: 230px;
      min-height: 90px;
    }
    .target-rules {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    }
    .target-rule {
      border: 1px solid #dce5f4;
      border-radius: 12px;
      background: #f8fbff;
      padding: 12px;
      display: grid;
      gap: 8px;
    }
    .target-rule strong {
      color: var(--ink);
      font-size: 0.9rem;
    }
    .target-rule label {
      display: grid;
      gap: 4px;
      color: var(--muted);
      font-size: 0.8rem;
    }
    .target-rule input {
      min-width: 0;
      width: 100%;
      box-sizing: border-box;
    }
    .hint {
      margin: 0;
      font-size: 0.82rem;
      color: var(--muted);
    }
    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    button {
      border: 1px solid var(--line);
      background: #f5f8ff;
      color: var(--ink);
      padding: 8px 12px;
      border-radius: 10px;
      font-size: 0.88rem;
      cursor: pointer;
      font-weight: 600;
    }
    button.primary {
      background: linear-gradient(135deg, var(--accent-2), #0d8bdb);
      color: #fff;
      border-color: transparent;
    }
    button.warn {
      background: #fff6f3;
      color: var(--warn);
      border-color: #e8cfc6;
    }
    button.danger {
      background: #fff1f0;
      color: #a6251b;
      border-color: #efc5c2;
      padding: 6px 9px;
      font-size: 0.8rem;
    }
    button:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
    .table-wrap {
      overflow-x: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9rem;
    }
    th, td {
      padding: 8px 6px;
      text-align: left;
      border-bottom: 1px solid #ecf1f8;
      white-space: nowrap;
    }
    th {
      color: var(--ink);
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .status {
      margin: 0 0 12px;
      padding: 10px 12px;
      border-radius: 10px;
      background: #eff5ff;
      color: #22488c;
      border: 1px solid #d5e2fb;
      font-size: 0.9rem;
    }
    .status.error {
      background: #fff5f1;
      color: #8d2a14;
      border-color: #f2d1c8;
    }
    .section-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }
    #debug-output {
      margin: 8px 0 0;
      border: 1px solid #dde6f5;
      border-radius: 10px;
      background: #f8fbff;
      color: #24406f;
      padding: 10px;
      font-size: 0.83rem;
      overflow: auto;
      max-height: 260px;
      white-space: pre-wrap;
    }
    @media (min-width: 980px) {
      .section-grid {
        grid-template-columns: 1.2fr 1fr;
      }
    }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <p class="eyebrow">Family Scheduling</p>
      <h1>Admin Console</h1>
      <p>Live operational view for sources, events, jobs, and feed contracts.</p>
      <div class="summary-grid">
        <article class="panel">
          <p class="metric-label">Sources</p>
          <p class="metric" id="metric-sources">-</p>
        </article>
        <article class="panel">
          <p class="metric-label">Events</p>
          <p class="metric" id="metric-events">-</p>
        </article>
        <article class="panel">
          <p class="metric-label">Jobs</p>
          <p class="metric" id="metric-jobs">-</p>
        </article>
        <article class="panel">
          <p class="metric-label">Outputs</p>
          <p class="metric" id="metric-targets">-</p>
        </article>
      </div>
    </section>

    <p id="status" class="status">Loading dashboard data...</p>
    <div class="toolbar">
      <button id="refresh" class="primary">Refresh Data</button>
      <button id="rebuild" class="warn">Queue Full Rebuild</button>
    </div>
    <p class="step">Step 1: Create Google Outputs</p>
    <form id="target-form" class="inline-form">
      <input id="target-key" type="text" placeholder="output_key (e.g. grayson_clubs)" required />
      <input id="target-calendar" type="text" placeholder="calendar_id" required />
      <input type="hidden" id="target-mode" value="managed_output" />
      <button id="target-save" type="submit">Add / Update Google Output</button>
    </form>

    <p class="step">Step 2: Add Sources</p>
    <form id="source-form" class="form-stack">
      <div class="inline-form">
        <input id="source-name" type="text" placeholder="display name" required />
        <input id="source-url" type="url" placeholder="https://...ics" required />
        <select id="source-google-output">
          <option value="1">google output: yes</option>
          <option value="0">google output: no</option>
        </select>
      </div>
      <div class="inline-form">
        <select id="source-ics-targets" multiple>
          <option value="family" selected>ICS Feed: family</option>
          <option value="grayson">ICS Feed: grayson</option>
          <option value="naomi">ICS Feed: naomi</option>
        </select>
        <select id="source-google-targets" multiple></select>
      </div>
      <p class="hint">Configure icon and prefix per selected output. Prefix is typically only used on the family feed, but can be stored on any output link.</p>
      <div id="source-target-rules" class="target-rules"></div>
      <div class="actions">
        <button id="source-save" type="submit">Add ICS Source</button>
      </div>
    </form>

    <section class="section-grid">
      <article class="table-panel panel">
        <h2>Sources</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Owner</th>
                <th>Assigned Outputs</th>
                <th>Category</th>
                <th>Active</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody id="sources-body"></tbody>
          </table>
        </div>
      </article>

      <article class="table-panel panel">
        <h2>Google Outputs</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Output Key</th>
                <th>Calendar ID</th>
                <th>Active</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody id="targets-body"></tbody>
          </table>
        </div>
      </article>

      <article class="table-panel panel">
        <h2>Feed Contracts</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Feed</th>
                <th>Primary</th>
                <th>Alias</th>
              </tr>
            </thead>
            <tbody id="contracts-body"></tbody>
          </table>
        </div>
      </article>

      <article class="table-panel panel">
        <h2>Recent Jobs</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Type</th>
                <th>Scope</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody id="jobs-body"></tbody>
          </table>
        </div>
      </article>

      <article class="table-panel panel">
        <h2>Review / Debug Output</h2>
        <div class="inline-form">
          <select id="debug-target">
            <option value="family">family</option>
            <option value="grayson">grayson</option>
            <option value="naomi">naomi</option>
          </select>
          <button id="debug-load" type="button" class="primary">Load Output Events</button>
        </div>
        <pre id="debug-output">Select an output and click "Load Output Events".</pre>
      </article>

      <article class="table-panel panel">
        <h2>Recent Events</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Title</th>
                <th>Source</th>
                <th>Owner</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody id="events-body"></tbody>
          </table>
        </div>
      </article>
    </section>
  </main>
  <script>
    const statusEl = document.getElementById('status');
    const refreshBtn = document.getElementById('refresh');
    const rebuildBtn = document.getElementById('rebuild');
    const sourceForm = document.getElementById('source-form');
    const sourceNameInput = document.getElementById('source-name');
    const sourceUrlInput = document.getElementById('source-url');
    const sourceIcsTargetsInput = document.getElementById('source-ics-targets');
    const sourceGoogleTargetsInput = document.getElementById('source-google-targets');
    const sourceTargetRulesEl = document.getElementById('source-target-rules');
    const sourceGoogleOutputInput = document.getElementById('source-google-output');
    const sourceSaveBtn = document.getElementById('source-save');
    const debugTargetInput = document.getElementById('debug-target');
    const debugLoadBtn = document.getElementById('debug-load');
    const debugOutputEl = document.getElementById('debug-output');
    const targetForm = document.getElementById('target-form');
    const targetKeyInput = document.getElementById('target-key');
    const targetCalendarInput = document.getElementById('target-calendar');
    const targetModeInput = document.getElementById('target-mode');
    const targetSaveBtn = document.getElementById('target-save');

    const metricSources = document.getElementById('metric-sources');
    const metricEvents = document.getElementById('metric-events');
    const metricJobs = document.getElementById('metric-jobs');
    const metricTargets = document.getElementById('metric-targets');

    const sourcesBody = document.getElementById('sources-body');
    const contractsBody = document.getElementById('contracts-body');
    const targetsBody = document.getElementById('targets-body');
    const jobsBody = document.getElementById('jobs-body');
    const eventsBody = document.getElementById('events-body');
    const sourceRuleState = new Map();

    function setStatus(message, isError) {
      statusEl.textContent = message;
      statusEl.className = isError ? 'status error' : 'status';
    }

    function renderRows(body, rowsHtml, colCount = 4) {
      body.innerHTML = rowsHtml || '<tr><td colspan="' + colCount + '">No records</td></tr>';
    }

    function getSelectedValues(selectEl) {
      return Array.from(selectEl.selectedOptions || []).map((option) => option.value).filter(Boolean);
    }

    function getSelectedTargetKeys() {
      return [...new Set([...getSelectedValues(sourceIcsTargetsInput), ...getSelectedValues(sourceGoogleTargetsInput)])];
    }

    function deriveOwnerTypeFromSelection() {
      const selectedIcsTargets = getSelectedValues(sourceIcsTargetsInput);
      if (selectedIcsTargets.includes('grayson')) return 'grayson';
      if (selectedIcsTargets.includes('naomi')) return 'naomi';
      return 'family';
    }

    function getDefaultPrefix(targetKey, ownerType) {
      if (targetKey !== 'family') return '';
      if (ownerType === 'grayson') return 'G:';
      if (ownerType === 'naomi') return 'N:';
      return '';
    }

    function syncTargetRuleInputs() {
      const ownerType = deriveOwnerTypeFromSelection();
      const optionsByKey = new Map();
      Array.from(sourceIcsTargetsInput.options).forEach((option) => {
        optionsByKey.set(option.value, { label: option.textContent || option.value, type: 'ics' });
      });
      Array.from(sourceGoogleTargetsInput.options).forEach((option) => {
        if (!option.value) return;
        optionsByKey.set(option.value, { label: option.textContent || option.value, type: 'google' });
      });

      const selectedTargetKeys = getSelectedTargetKeys();
      const nextState = new Map();
      for (const targetKey of selectedTargetKeys) {
        const existing = sourceRuleState.get(targetKey) || {};
        nextState.set(targetKey, {
          icon: existing.icon || '',
          prefix: existing.prefix !== undefined ? existing.prefix : getDefaultPrefix(targetKey, ownerType),
        });
      }
      sourceRuleState.clear();
      nextState.forEach((value, key) => sourceRuleState.set(key, value));

      sourceTargetRulesEl.innerHTML = selectedTargetKeys.length
        ? selectedTargetKeys
            .map((targetKey) => {
              const option = optionsByKey.get(targetKey) || { label: targetKey, type: 'google' };
              const state = sourceRuleState.get(targetKey) || { icon: '', prefix: '' };
              return '<div class="target-rule"><strong>' +
                option.label +
                '</strong><label>Icon<input type="text" data-rule-target="' +
                targetKey +
                '" data-rule-field="icon" value="' +
                (state.icon || '').replace(/"/g, '&quot;') +
                '" placeholder="optional icon" /></label><label>Prefix<input type="text" data-rule-target="' +
                targetKey +
                '" data-rule-field="prefix" value="' +
                (state.prefix || '').replace(/"/g, '&quot;') +
                '" placeholder="' +
                (option.type === 'ics' && targetKey === 'family' ? 'for example N:' : 'optional prefix') +
                '" /></label></div>';
            })
            .join('')
        : '<p class="hint">Select at least one output to configure output rules.</p>';

      sourceTargetRulesEl.querySelectorAll('input[data-rule-target]').forEach((input) => {
        input.addEventListener('input', () => {
          const targetKey = input.getAttribute('data-rule-target');
          const field = input.getAttribute('data-rule-field');
          if (!targetKey || !field) return;
          const current = sourceRuleState.get(targetKey) || { icon: '', prefix: '' };
          current[field] = input.value;
          sourceRuleState.set(targetKey, current);
        });
      });
    }

    function buildSourceTargetLabels(source) {
      const links = Array.isArray(source.target_links) ? source.target_links : [];
      if (links.length) {
        return links.map((link) => {
          const typeLabel = link.target_type === 'ics' ? 'ICS' : 'Google';
          const ruleBits = [];
          if (link.prefix) ruleBits.push('prefix ' + link.prefix);
          if (link.icon) ruleBits.push('icon ' + link.icon);
          return typeLabel + ': ' + link.target_key + (ruleBits.length ? ' (' + ruleBits.join(', ') + ')' : '');
        });
      }
      const labels = [];
      if (source.owner_type === 'grayson' && Number(source.include_in_child_ics)) labels.push('ICS: grayson');
      if (source.owner_type === 'naomi' && Number(source.include_in_child_ics)) labels.push('ICS: naomi');
      if (Number(source.include_in_family_ics)) labels.push('ICS: family');
      return labels;
    }

    async function fetchJson(url, options) {
      const response = await fetch(url, options);
      if (!response.ok) {
        let message = 'Request failed';
        try {
          const payload = await response.json();
          message = payload.message || payload.error || message;
        } catch {
          message = response.status + ' ' + response.statusText;
        }
        throw new Error(message);
      }
      return response.json();
    }

    async function loadDashboard() {
      refreshBtn.disabled = true;
      rebuildBtn.disabled = true;
      setStatus('Loading dashboard data...', false);
      try {
        const [sourcesPayload, eventsPayload, jobsPayload, targetsPayload, contractsPayload] = await Promise.all([
          fetchJson('/api/sources'),
          fetchJson('/api/events?limit=15'),
          fetchJson('/api/jobs'),
          fetchJson('/api/targets'),
          fetchJson('/api/feed-contracts')
        ]);

        const sources = sourcesPayload.sources || [];
        const events = eventsPayload.events || [];
        const jobs = jobsPayload.jobs || [];
        const logicalTargets = targetsPayload.logicalTargets || [];
        const registeredTargets = targetsPayload.targets || [];
        const contracts = contractsPayload.contracts || [];

        metricSources.textContent = String(sources.length);
        metricEvents.textContent = String(events.length);
        metricJobs.textContent = String(jobs.length);
        metricTargets.textContent = String(registeredTargets.length);

        const googleTargetOptions = registeredTargets
          .map((target) => '<option value="' + (target.target_key || '') + '">Google Calendar: ' + (target.target_key || '') + '</option>')
          .join('');
        sourceGoogleTargetsInput.innerHTML = googleTargetOptions || '<option value="" disabled>No Google outputs configured</option>';
        debugTargetInput.innerHTML = [
          logicalTargets.map((targetKey) => '<option value="' + targetKey + '">' + targetKey + '</option>').join(''),
          registeredTargets.map((target) => '<option value="' + (target.target_key || '') + '">' + (target.target_key || '') + '</option>').join('')
        ].join('');
        syncTargetRuleInputs();

        const sortedSources = [...sources].sort((a, b) => {
          const aLabel = buildSourceTargetLabels(a).join(', ');
          const bLabel = buildSourceTargetLabels(b).join(', ');
          if (aLabel !== bLabel) return aLabel.localeCompare(bLabel);
          return String(a.display_name || a.name || '').localeCompare(String(b.display_name || b.name || ''));
        });

        renderRows(
          sourcesBody,
          sortedSources
            .slice(0, 20)
            .map((source) =>
              '<tr><td>' + (source.display_name || source.name || '') + '</td><td>' + (source.owner_type || '') + '</td><td>' + buildSourceTargetLabels(source).join(', ') + '</td><td>' + (source.source_category || '') + '</td><td>' + (source.is_active ? 'yes' : 'no') + '</td><td><div class="actions"><button class="primary rebuild-source" data-source-id="' + (source.id || '') + '">Rebuild</button><button class="danger disable-source" data-source-id="' + (source.id || '') + '">Disable</button></div></td></tr>'
            )
            .join(''),
          6
        );

        document.querySelectorAll('.rebuild-source').forEach((button) => {
          button.addEventListener('click', async () => {
            const sourceId = button.getAttribute('data-source-id');
            if (!sourceId) return;
            button.disabled = true;
            setStatus('Queueing rebuild for source ' + sourceId + '...', false);
            try {
              const payload = await fetchJson('/api/sources/' + encodeURIComponent(sourceId) + '/rebuild', { method: 'POST' });
              setStatus('Rebuild queued for source. Job: ' + (payload.job?.id || 'unknown'), false);
              await loadDashboard();
            } catch (error) {
              setStatus('Unable to queue source rebuild: ' + (error.message || String(error)), true);
              button.disabled = false;
            }
          });
        });

        document.querySelectorAll('.disable-source').forEach((button) => {
          button.addEventListener('click', async () => {
            const sourceId = button.getAttribute('data-source-id');
            if (!sourceId) return;
            button.disabled = true;
            setStatus('Disabling source ' + sourceId + '...', false);
            try {
              await fetchJson('/api/sources/' + encodeURIComponent(sourceId), { method: 'DELETE' });
              setStatus('Source disabled.', false);
              await loadDashboard();
            } catch (error) {
              setStatus('Unable to disable source: ' + (error.message || String(error)), true);
              button.disabled = false;
            }
          });
        });

        renderRows(
          contractsBody,
          contracts
            .map((contract) => '<tr><td>' + contract.target + '</td><td>' + contract.primary_path + '</td><td>' + (contract.alias_path || '') + '</td></tr>')
            .join(''),
          3
        );

        renderRows(
          targetsBody,
          registeredTargets
            .map((target) =>
              '<tr><td>' + (target.target_key || '') + '</td><td>' + (target.calendar_id || '') + '</td><td>' + (Number(target.is_active) ? 'yes' : 'no') + '</td><td><button class="danger delete-target" data-target-key="' + (target.target_key || '') + '">Delete</button></td></tr>'
            )
            .join(''),
          5
        );

        document.querySelectorAll('.delete-target').forEach((button) => {
          button.addEventListener('click', async () => {
            const targetKey = button.getAttribute('data-target-key');
            if (!targetKey) return;
            button.disabled = true;
            setStatus('Deleting Google target ' + targetKey + '...', false);
            try {
              await fetchJson('/api/targets/' + encodeURIComponent(targetKey), { method: 'DELETE' });
              setStatus('Deleted Google target ' + targetKey + '.', false);
              await loadDashboard();
            } catch (error) {
              setStatus('Unable to delete Google target: ' + (error.message || String(error)), true);
              button.disabled = false;
            }
          });
        });

        renderRows(
          jobsBody,
          jobs
            .slice(0, 20)
            .map((job) => '<tr><td>' + (job.job_type || '') + '</td><td>' + (job.scope_type || 'system') + '</td><td>' + (job.status || '') + '</td></tr>')
            .join('')
        );

        renderRows(
          eventsBody,
          events
            .slice(0, 20)
            .map((event) => '<tr><td>' + (event.title || '') + '</td><td>' + (event.source_name || '') + '</td><td>' + (event.owner_type || '') + '</td><td>' + (event.status || '') + '</td></tr>')
            .join('')
        );

        setStatus('Dashboard loaded.', false);
      } catch (error) {
        setStatus('Unable to load dashboard: ' + (error.message || String(error)), true);
      } finally {
        refreshBtn.disabled = false;
        rebuildBtn.disabled = false;
      }
    }

    refreshBtn.addEventListener('click', loadDashboard);
    sourceIcsTargetsInput.addEventListener('change', syncTargetRuleInputs);
    sourceGoogleTargetsInput.addEventListener('change', syncTargetRuleInputs);
    rebuildBtn.addEventListener('click', async () => {
      rebuildBtn.disabled = true;
      setStatus('Queueing full rebuild...', false);
      try {
        const payload = await fetchJson('/api/rebuild/full', { method: 'POST' });
        setStatus('Full rebuild queued. Job: ' + (payload.job?.id || 'unknown'), false);
        await loadDashboard();
      } catch (error) {
        setStatus('Unable to queue rebuild: ' + (error.message || String(error)), true);
      } finally {
        rebuildBtn.disabled = false;
      }
    });

    targetForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      targetSaveBtn.disabled = true;
      setStatus('Saving Google target...', false);
      try {
        const calendarId = String(targetCalendarInput.value || '').trim();
        if (!calendarId.includes('@')) {
          throw new Error('calendar_id should look like a full Google Calendar ID (contains "@").');
        }
        await fetchJson('/api/targets', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            target_key: targetKeyInput.value,
            calendar_id: calendarId,
            ownership_mode: targetModeInput.value
          })
        });
        targetKeyInput.value = '';
        targetCalendarInput.value = '';
        targetModeInput.value = 'managed_output';
        setStatus('Google target saved.', false);
        await loadDashboard();
      } catch (error) {
        setStatus('Unable to save Google target: ' + (error.message || String(error)), true);
      } finally {
        targetSaveBtn.disabled = false;
      }
    });

    sourceForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      sourceSaveBtn.disabled = true;
      setStatus('Saving ICS source...', false);
      try {
        const sourceUrl = String(sourceUrlInput.value || '').trim();
        const selectedIcsTargets = getSelectedValues(sourceIcsTargetsInput);
        const selectedGoogleTargets = getSelectedValues(sourceGoogleTargetsInput);

        if (!sourceUrl.startsWith('http://') && !sourceUrl.startsWith('https://')) {
          throw new Error('Source URL must start with http:// or https://');
        }
        if (!selectedIcsTargets.length && !selectedGoogleTargets.length) {
          throw new Error('Select at least one target (ICS or Google).');
        }
        if (selectedIcsTargets.includes('grayson') && selectedIcsTargets.includes('naomi')) {
          throw new Error('A source cannot target both grayson and naomi ICS feeds in one row.');
        }

        const ownerType = selectedIcsTargets.includes('grayson')
          ? 'grayson'
          : selectedIcsTargets.includes('naomi')
          ? 'naomi'
          : 'family';
        const includeInChildIcs = selectedIcsTargets.includes('grayson') || selectedIcsTargets.includes('naomi');
        const includeInFamilyIcs = selectedIcsTargets.includes('family');
        const allTargetKeys = [...new Set([...selectedIcsTargets, ...selectedGoogleTargets])];
        const targetLinks = allTargetKeys.map((targetKey) => {
          const rule = sourceRuleState.get(targetKey) || { icon: '', prefix: getDefaultPrefix(targetKey, ownerType) };
          return {
            target_key: targetKey,
            icon: rule.icon || '',
            prefix: rule.prefix || '',
          };
        });

        await fetchJson('/api/sources', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            owner_type: ownerType,
            display_name: sourceNameInput.value,
            url: sourceUrl,
            include_in_child_ics: includeInChildIcs,
            include_in_family_ics: includeInFamilyIcs,
            include_in_child_google_output: sourceGoogleOutputInput.value === '1',
            target_links: targetLinks
          })
        });
        sourceNameInput.value = '';
        sourceUrlInput.value = '';
        Array.from(sourceIcsTargetsInput.options).forEach((option) => {
          option.selected = option.value === 'family';
        });
        Array.from(sourceGoogleTargetsInput.options).forEach((option) => {
          option.selected = false;
        });
        sourceRuleState.clear();
        syncTargetRuleInputs();
        setStatus('ICS source saved. Queue a rebuild to ingest now.', false);
        await loadDashboard();
      } catch (error) {
        setStatus('Unable to save ICS source: ' + (error.message || String(error)), true);
      } finally {
        sourceSaveBtn.disabled = false;
      }
    });

    debugLoadBtn.addEventListener('click', async () => {
      debugLoadBtn.disabled = true;
      const target = debugTargetInput.value;
      setStatus('Loading debug output for target ' + target + '...', false);
      try {
        const payload = await fetchJson('/api/events?target=' + encodeURIComponent(target) + '&limit=50');
        const events = payload.events || [];
        const preview = {
          target,
          count: events.length,
          sample: events.slice(0, 10),
        };
        debugOutputEl.textContent = JSON.stringify(preview, null, 2);
        setStatus('Debug output loaded for ' + target + '.', false);
      } catch (error) {
        debugOutputEl.textContent = '';
        setStatus('Unable to load debug output: ' + (error.message || String(error)), true);
      } finally {
        debugLoadBtn.disabled = false;
      }
    });

    loadDashboard();
  </script>
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
        return Response.redirect(`${url.origin}/admin`, 302);
      }

      if (pathname === '/admin' || pathname === '/admin/') {
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
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json(await repo.getFeedContract());
      }

      if (pathname === '/api/sources' && request.method === 'GET') {
        const authError = await requireRole(request, env, ['admin', 'editor']);
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
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({ jobs: await repo.listJobs() });
      }

      if (pathname === '/api/targets' && request.method === 'GET') {
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
        const repo = await createRepository(env);
        return json({
          targets: await repo.listTargets(),
          logicalTargets: TARGETS,
        });
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
        const targetKey = decodeURIComponent(pathname.split('/').filter(Boolean)[2] || '');
        const repo = await createRepository(env);
        const removed = await repo.deleteTarget(targetKey);
        if (!removed) return json({ error: 'Not found' }, { status: 404 });
        return json({ deleted: removed });
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
