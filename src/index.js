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
      max-width: 1100px;
      margin: 0 auto;
      padding: 24px 18px 48px;
      display: grid;
      gap: 20px;
    }
    h1, h2, h3 { margin: 0; line-height: 1.2; }
    h1 { font-size: clamp(1.4rem, 3vw, 2rem); }
    h2 { font-size: 1.1rem; color: var(--ink); }
    h3 { font-size: 0.95rem; color: var(--ink); }
    p { margin: 0; color: var(--muted); line-height: 1.5; }
    td, th { color: var(--muted); }
    .eyebrow {
      font-size: 11px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--accent);
      font-weight: 700;
    }
    /* panels */
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: 0 6px 20px rgba(21,40,70,0.07);
      padding: 20px;
    }
    .card-header {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 4px;
    }
    .section-desc {
      font-size: 0.85rem;
      color: var(--muted);
      margin-bottom: 16px !important;
    }
    .subsection {
      border-top: 1px solid var(--line);
      margin-top: 18px;
      padding-top: 16px;
    }
    .subsection-header {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 10px;
    }
    .badge {
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      padding: 2px 7px;
      border-radius: 20px;
      background: #e8f0fe;
      color: var(--accent-2);
    }
    .badge.ics { background: #e6f7f0; color: var(--accent); }
    .badge.planned { background: #f4f4f5; color: #888; }
    /* summary strip */
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px;
      margin-top: 12px;
    }
    .metric-card {
      background: #f7faff;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 14px;
    }
    .metric { font-size: 1.5rem; font-weight: 700; color: var(--ink); }
    .metric-label { font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }
    /* toolbar */
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
    }
    /* forms */
    .inline-form {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: flex-start;
    }
    .form-row { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-start; }
    .field-group { display: grid; gap: 4px; }
    .field-label { font-size: 0.78rem; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; }
    input[type="text"], input[type="url"], input[type="search"], select:not(.checkbox-wrap select) {
      border: 1px solid var(--line);
      background: #fff;
      color: var(--ink);
      padding: 8px 10px;
      border-radius: 8px;
      font-size: 0.88rem;
    }
    input[type="text"] { min-width: 160px; }
    input[type="url"] { min-width: 280px; }
    input[type="search"] { min-width: 200px; }
    select { min-width: 140px; }
    /* checkbox output picker */
    .checkbox-wrap {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
    }
    .checkbox-wrap label {
      display: flex;
      align-items: center;
      gap: 5px;
      font-size: 0.85rem;
      color: var(--ink);
      background: #f4f7fb;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 5px 10px;
      cursor: pointer;
      user-select: none;
    }
    .checkbox-wrap input[type="checkbox"] { accent-color: var(--accent-2); }
    /* output rules */
    .target-rules {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      margin-top: 10px;
    }
    .target-rule {
      border: 1px solid #dce5f4;
      border-radius: 10px;
      background: #f8fbff;
      padding: 12px;
      display: grid;
      gap: 8px;
    }
    .target-rule strong { color: var(--ink); font-size: 0.88rem; }
    .target-rule label { display: grid; gap: 3px; color: var(--muted); font-size: 0.78rem; }
    .target-rule input { min-width: 0; width: 100%; box-sizing: border-box; }
    /* buttons */
    button {
      border: 1px solid var(--line);
      background: #f5f8ff;
      color: var(--ink);
      padding: 8px 14px;
      border-radius: 8px;
      font-size: 0.88rem;
      cursor: pointer;
      font-weight: 600;
      white-space: nowrap;
    }
    button.primary { background: linear-gradient(135deg, var(--accent-2), #0d8bdb); color: #fff; border-color: transparent; }
    button.warn { background: #fff6f3; color: var(--warn); border-color: #e8cfc6; }
    button.danger { background: #fff1f0; color: #a6251b; border-color: #efc5c2; padding: 5px 9px; font-size: 0.8rem; }
    button:disabled { opacity: 0.5; cursor: not-allowed; }
    /* tables */
    .table-wrap { overflow-x: auto; margin-top: 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    th, td { padding: 8px 8px; text-align: left; border-bottom: 1px solid #ecf1f8; white-space: nowrap; }
    th { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.07em; color: var(--ink); }
    td .actions { display: flex; gap: 6px; }
    /* status */
    .status {
      padding: 10px 14px;
      border-radius: 8px;
      background: #eff5ff;
      color: #22488c;
      border: 1px solid #d5e2fb;
      font-size: 0.88rem;
    }
    .status.error { background: #fff5f1; color: #8d2a14; border-color: #f2d1c8; }
    .hint { font-size: 0.82rem; color: var(--muted); }
    /* instance list (Modify Events) */
    .inst-results { display: grid; gap: 2px; margin-top: 10px; }
    .inst-row {
      display: grid;
      grid-template-columns: 140px 1fr auto;
      align-items: center;
      gap: 10px;
      padding: 6px 10px;
      border: 1px solid var(--line);
      border-radius: 5px;
      background: #fff;
      cursor: pointer;
      font-size: 0.88rem;
    }
    .inst-row:hover { background: #f0f5ff; border-color: #bcd0f5; }
    .inst-row.open { background: #e8f0fe; border-color: var(--accent-2); border-bottom-left-radius: 0; border-bottom-right-radius: 0; }
    .inst-row-date { font-size: 0.78rem; color: var(--muted); white-space: nowrap; }
    .inst-row-title { font-weight: 600; color: var(--ink); }
    .inst-row-meta { font-size: 0.76rem; color: var(--muted); white-space: nowrap; }
    .inst-drawer {
      border: 1px solid var(--accent-2);
      border-top: none;
      border-radius: 0 0 6px 6px;
      background: #f5f8ff;
      padding: 14px 16px;
      margin-bottom: 6px;
    }
    .inst-drawer h4 { font-size: 0.85rem; font-weight: 600; margin: 0 0 10px; color: var(--ink); }
    .inst-drawer .override-form { display: flex; flex-wrap: wrap; gap: 8px; align-items: flex-end; margin: 0; }
    .inst-drawer .override-list { margin-top: 10px; display: grid; gap: 6px; }
    .inst-drawer .override-item {
      display: flex; align-items: center; gap: 10px;
      padding: 6px 10px; border: 1px solid var(--line); border-radius: 5px;
      background: #fff; font-size: 0.85rem;
    }
    .inst-drawer .override-item-info { flex: 1; }
    .inst-drawer .override-item-meta { font-size: 0.78rem; color: var(--muted); }
    .hidden { display: none !important; }
  </style>
</head>
<body>
  <main>

    <!-- Header -->
    <div class="card">
      <p class="eyebrow">Family Scheduling</p>
      <h1>Admin Console</h1>
      <div class="summary-grid">
        <div class="metric-card"><p class="metric-label">Sources</p><p class="metric" id="metric-sources">-</p></div>
        <div class="metric-card"><p class="metric-label">Events</p><p class="metric" id="metric-events">-</p></div>
        <div class="metric-card"><p class="metric-label">Google Outputs</p><p class="metric" id="metric-targets">-</p></div>
        <div class="metric-card"><p class="metric-label">Jobs</p><p class="metric" id="metric-jobs">-</p></div>
      </div>
      <div class="toolbar">
        <button id="refresh" class="primary">Refresh</button>
        <button id="rebuild" class="warn">Queue Full Rebuild</button>
      </div>
    </div>

    <p id="status" class="status">Loading...</p>

    <!-- Section 1: Configure Outputs -->
    <div class="card">
      <div class="card-header">
        <h2>Configure Outputs</h2>
      </div>
      <p class="section-desc">Outputs are delivered calendar services. Sources are assigned to one or more outputs.</p>

      <!-- Google Calendar outputs -->
      <div class="subsection">
        <div class="subsection-header">
          <h3>Google Calendar</h3>
          <span class="badge">Google</span>
        </div>
        <p class="hint" style="margin-bottom:12px">Connect a Google Calendar that this system will write to on each sync.</p>
        <form id="target-form" class="inline-form">
          <div class="field-group">
            <span class="field-label">Display name / key</span>
            <input id="target-key" type="text" placeholder="e.g. grayson-clubs" required />
          </div>
          <div class="field-group">
            <span class="field-label">Google Calendar ID</span>
            <input id="target-calendar" type="text" placeholder="name@group.calendar.google.com" required />
          </div>
          <input type="hidden" id="target-mode" value="managed_output" />
          <div class="field-group" style="justify-content:flex-end">
            <span class="field-label">&nbsp;</span>
            <button id="target-save" class="primary" type="submit">Add</button>
          </div>
        </form>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Name / Key</th><th>Calendar ID</th><th>Active</th><th>Actions</th></tr></thead>
            <tbody id="targets-body"></tbody>
          </table>
        </div>
      </div>

      <!-- ICS feed outputs -->
      <div class="subsection">
        <div class="subsection-header">
          <h3>ICS Feeds</h3>
          <span class="badge ics">ICS</span>
          <span class="badge planned">Configurable feeds planned</span>
        </div>
        <p class="hint" style="margin-bottom:12px">Shareable calendar URLs. Currently fixed to three feeds — adding named feeds without a code change is planned.</p>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Feed</th><th>Subscription URL</th><th></th></tr></thead>
            <tbody id="contracts-body"></tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Section 2: Configure Sources -->
    <div class="card">
      <div class="card-header">
        <h2>Configure Sources</h2>
      </div>
      <p class="section-desc">Add an ICS URL and choose which outputs its events feed into.</p>

      <form id="source-form" style="display:grid;gap:14px">
        <div class="form-row">
          <div class="field-group">
            <span class="field-label">Friendly name</span>
            <input id="source-name" type="text" placeholder="e.g. Grayson Hockey" required />
          </div>
          <div class="field-group">
            <span class="field-label">ICS URL</span>
            <input id="source-url" type="url" placeholder="https://..." required />
          </div>
        </div>
        <div>
          <p class="field-label" style="margin-bottom:6px">ICS Feeds</p>
          <div class="checkbox-wrap" id="source-ics-outputs">
            <label><input type="checkbox" name="ics-target" value="family"> family</label>
            <label><input type="checkbox" name="ics-target" value="grayson"> grayson</label>
            <label><input type="checkbox" name="ics-target" value="naomi"> naomi</label>
          </div>
        </div>
        <div>
          <p class="field-label" style="margin-bottom:6px">Google Calendar Outputs</p>
          <div class="checkbox-wrap" id="source-google-outputs"></div>
          <p class="hint" id="no-google-hint">No Google outputs configured yet.</p>
        </div>
        <div id="source-target-rules" class="target-rules"></div>
        <div>
          <button id="source-save" class="primary" type="submit">Add Source</button>
        </div>
      </form>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Owner</th>
              <th>Outputs</th>
              <th>Events</th>
              <th>Last Fetch</th>
              <th>Status</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody id="sources-body"></tbody>
        </table>
      </div>
    </div>

    <!-- Section 3: Modify Events -->
    <div class="card">
      <div class="card-header">
        <h2>Modify Events</h2>
      </div>

      <div class="form-row" style="margin-top:12px">
        <div class="field-group">
          <span class="field-label">Source</span>
          <select id="event-source-filter">
            <option value="">— pick a source —</option>
          </select>
        </div>
        <div class="field-group">
          <span class="field-label">Filter by output</span>
          <select id="event-output-filter">
            <option value="">All outputs</option>
          </select>
        </div>
        <div class="field-group">
          <span class="field-label">Search</span>
          <input type="search" id="event-search" placeholder="Title keyword..." />
        </div>
      </div>

      <div id="inst-results" class="inst-results"><p class="hint" style="padding:8px 0">Pick a source to see its upcoming events.</p></div>

      <div id="modified-events-section" style="margin-top:20px;border-top:1px solid var(--line);padding-top:16px">
        <h3 style="font-size:0.95rem;font-weight:600;margin-bottom:8px">Future Events with Modifications</h3>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Event Date</th><th>Event</th><th>Source</th><th>Override</th><th>Note</th><th></th></tr></thead>
            <tbody id="modified-events-body"></tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- System -->
    <div class="card">
      <h2>Recent Jobs</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Type</th><th>Scope</th><th>Status</th></tr></thead>
          <tbody id="jobs-body"></tbody>
        </table>
      </div>
    </div>

  </main>
  <script>
    const statusEl = document.getElementById('status');
    const refreshBtn = document.getElementById('refresh');
    const rebuildBtn = document.getElementById('rebuild');

    // outputs
    const targetForm = document.getElementById('target-form');
    const targetKeyInput = document.getElementById('target-key');
    const targetCalendarInput = document.getElementById('target-calendar');
    const targetModeInput = document.getElementById('target-mode');
    const targetsBody = document.getElementById('targets-body');
    const contractsBody = document.getElementById('contracts-body');

    // sources
    const sourceForm = document.getElementById('source-form');
    const sourceNameInput = document.getElementById('source-name');
    const sourceUrlInput = document.getElementById('source-url');
    const sourceIcsOutputsEl = document.getElementById('source-ics-outputs');
    const sourceGoogleOutputsEl = document.getElementById('source-google-outputs');
    const noGoogleHint = document.getElementById('no-google-hint');
    const sourceTargetRulesEl = document.getElementById('source-target-rules');
    const sourcesBody = document.getElementById('sources-body');

    // events / overrides
    const eventSearchInput = document.getElementById('event-search');
    const eventSourceFilter = document.getElementById('event-source-filter');
    const eventOutputFilter = document.getElementById('event-output-filter');
    const instResultsEl = document.getElementById('inst-results');
    const modifiedEventsBody = document.getElementById('modified-events-body');

    // metrics
    const metricSources = document.getElementById('metric-sources');
    const metricEvents = document.getElementById('metric-events');
    const metricJobs = document.getElementById('metric-jobs');
    const metricTargets = document.getElementById('metric-targets');
    const jobsBody = document.getElementById('jobs-body');

    const sourceRuleState = new Map();
    let openDrawerInstId = null;
    let allInstances = [];

    function setStatus(message, isError) {
      statusEl.textContent = message;
      statusEl.className = isError ? 'status error' : 'status';
    }

    function renderRows(body, rowsHtml, colCount) {
      body.innerHTML = rowsHtml || '<tr><td colspan="' + (colCount || 4) + '" style="color:#aaa;font-style:italic">None</td></tr>';
    }

    function getCheckedValues(containerEl) {
      return Array.from(containerEl.querySelectorAll('input[type="checkbox"]:checked')).map((cb) => cb.value);
    }

    function getAllCheckboxValues(containerEl) {
      return Array.from(containerEl.querySelectorAll('input[type="checkbox"]')).map((cb) => ({ value: cb.value, label: cb.closest('label')?.textContent?.trim() || cb.value }));
    }

    function deriveOwnerType(selectedIcs) {
      if (selectedIcs.includes('grayson')) return 'grayson';
      if (selectedIcs.includes('naomi')) return 'naomi';
      return 'family';
    }

    function getDefaultPrefix(targetKey, ownerType) {
      if (targetKey !== 'family') return '';
      if (ownerType === 'grayson') return 'G:';
      if (ownerType === 'naomi') return 'N:';
      return '';
    }

    function syncTargetRuleInputs() {
      const selectedIcs = getCheckedValues(sourceIcsOutputsEl);
      const selectedGoogle = getCheckedValues(sourceGoogleOutputsEl);
      const ownerType = deriveOwnerType(selectedIcs);
      const allSelected = [...selectedIcs, ...selectedGoogle];

      // label map
      const labelMap = new Map();
      getAllCheckboxValues(sourceIcsOutputsEl).forEach(({ value, label }) => labelMap.set(value, { label: 'ICS: ' + label, type: 'ics' }));
      getAllCheckboxValues(sourceGoogleOutputsEl).forEach(({ value, label }) => labelMap.set(value, { label: 'Google: ' + label, type: 'google' }));

      const nextState = new Map();
      for (const key of allSelected) {
        const existing = sourceRuleState.get(key) || {};
        nextState.set(key, {
          icon: existing.icon || '',
          prefix: existing.prefix !== undefined ? existing.prefix : getDefaultPrefix(key, ownerType),
        });
      }
      sourceRuleState.clear();
      nextState.forEach((v, k) => sourceRuleState.set(k, v));

      sourceTargetRulesEl.innerHTML = allSelected.length
        ? allSelected.map((key) => {
            const info = labelMap.get(key) || { label: key, type: 'google' };
            const state = sourceRuleState.get(key) || { icon: '', prefix: '' };
            return '<div class="target-rule"><strong>' + info.label + '</strong>' +
              '<label>Icon<input type="text" data-rule-target="' + key + '" data-rule-field="icon" value="' + (state.icon || '').replace(/"/g, '&quot;') + '" placeholder="optional emoji" /></label>' +
              '<label>Prefix<input type="text" data-rule-target="' + key + '" data-rule-field="prefix" value="' + (state.prefix || '').replace(/"/g, '&quot;') + '" placeholder="' + (info.type === 'ics' && key === 'family' ? 'e.g. G: or N:' : 'optional') + '" /></label>' +
              '</div>';
          }).join('')
        : '';

      sourceTargetRulesEl.querySelectorAll('input[data-rule-target]').forEach((input) => {
        input.addEventListener('input', () => {
          const key = input.getAttribute('data-rule-target');
          const field = input.getAttribute('data-rule-field');
          if (!key || !field) return;
          const current = sourceRuleState.get(key) || { icon: '', prefix: '' };
          current[field] = input.value;
          sourceRuleState.set(key, current);
        });
      });
    }

    function buildOutputLabels(source) {
      const links = Array.isArray(source.target_links) ? source.target_links : [];
      if (links.length) {
        return links.map((link) => {
          const type = link.target_type === 'ics' ? 'ICS' : 'GCal';
          return type + ':' + link.target_key;
        }).join(', ');
      }
      const parts = [];
      if (source.owner_type === 'grayson' && Number(source.include_in_child_ics)) parts.push('ICS:grayson');
      if (source.owner_type === 'naomi' && Number(source.include_in_child_ics)) parts.push('ICS:naomi');
      if (Number(source.include_in_family_ics)) parts.push('ICS:family');
      return parts.join(', ');
    }

    function buildIconLabel(source) {
      const links = Array.isArray(source.target_links) ? source.target_links : [];
      const icons = [...new Set(links.map((l) => l.icon).filter(Boolean))];
      return icons.join(' ') || (source.icon || '');
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
      setStatus('Loading...', false);
      try {
        const [sourcesPayload, eventsPayload, jobsPayload, targetsPayload, contractsPayload] = await Promise.all([
          fetchJson('/api/sources'),
          fetchJson('/api/events?limit=15'),
          fetchJson('/api/jobs'),
          fetchJson('/api/targets'),
          fetchJson('/api/feed-contracts'),
        ]);

        const sources = sourcesPayload.sources || [];
        const events = eventsPayload.events || [];
        const jobs = jobsPayload.jobs || [];
        const registeredTargets = targetsPayload.targets || [];
        const logicalTargets = targetsPayload.logicalTargets || [];
        metricSources.textContent = String(sources.length);
        metricEvents.textContent = String(events.length);
        metricJobs.textContent = String(jobs.length);
        metricTargets.textContent = String(registeredTargets.length);

        // populate google outputs checkboxes in source form
        if (registeredTargets.length) {
          noGoogleHint.classList.add('hidden');
          sourceGoogleOutputsEl.innerHTML = registeredTargets
            .map((t) => '<label><input type="checkbox" name="google-target" value="' + (t.target_key || '') + '"> ' + (t.target_key || '') + '</label>')
            .join('');
        } else {
          noGoogleHint.classList.remove('hidden');
          sourceGoogleOutputsEl.innerHTML = '';
        }

        // populate source filter (Modify Events)
        const prevSource = eventSourceFilter.value;
        eventSourceFilter.innerHTML = '<option value="">— pick a source —</option>' +
          sources.map((s) => '<option value="' + (s.id || '') + '"' + (s.id === prevSource ? ' selected' : '') + '>' + (s.display_name || s.name || '') + ' (' + (s.owner_type || '') + ')</option>').join('');

        // populate event output filter
        const allOutputKeys = [...logicalTargets, ...registeredTargets.map((t) => t.target_key || '')].filter(Boolean);
        eventOutputFilter.innerHTML = '<option value="">All outputs</option>' +
          allOutputKeys.map((k) => '<option value="' + k + '">' + k + '</option>').join('');

        // load modified events table
        await loadModifiedEvents();

        syncTargetRuleInputs();

        // sources table
        const sortedSources = [...sources].sort((a, b) => {
          const al = buildOutputLabels(a), bl = buildOutputLabels(b);
          if (al !== bl) return al.localeCompare(bl);
          return String(a.display_name || a.name || '').localeCompare(String(b.display_name || b.name || ''));
        });
        renderRows(
          sourcesBody,
          sortedSources.slice(0, 30).map((s) => {
            const lastFetch = s.last_fetched_at ? new Date(s.last_fetched_at).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) : '—';
            const parseOk = ['ok', 'success', 'parsed', 'parsed_no_blob'].includes(s.last_parse_status);
            const statusCell = !s.last_parse_status ? '<span class="hint">never</span>'
              : parseOk ? '<span style="color:#1a7f37">ok</span>'
              : '<span style="color:#a00" title="' + (s.last_parse_error || '').replace(/"/g, '&quot;') + '">error</span>';
            return '<tr>' +
            '<td>' + (s.display_name || s.name || '') + '</td>' +
            '<td>' + (s.owner_type || '') + '</td>' +
            '<td>' + buildOutputLabels(s) + '</td>' +
            '<td>' + (s.event_count || 0) + '</td>' +
            '<td style="white-space:nowrap;font-size:0.8rem">' + lastFetch + '</td>' +
            '<td>' + statusCell + '</td>' +
            '<td><div class="actions">' +
            '<button class="primary rebuild-source" data-source-id="' + (s.id || '') + '">Rebuild</button>' +
            '<button class="change-source" data-source-id="' + (s.id || '') + '" data-source-name="' + (s.display_name || s.name || '').replace(/"/g, '&quot;') + '" data-source-url="' + (s.url || '').replace(/"/g, '&quot;') + '" data-source-links="' + JSON.stringify(Array.isArray(s.target_links) ? s.target_links : []).replace(/"/g, '&quot;') + '">Change</button>' +
            '<button class="danger disable-source" data-source-id="' + (s.id || '') + '">Disable</button>' +
            '<button class="danger delete-source" data-source-id="' + (s.id || '') + '">Delete</button>' +
            '</div></td></tr>';
          }).join(''),
          6
        );

        document.querySelectorAll('.rebuild-source').forEach((btn) => {
          btn.addEventListener('click', async () => {
            const id = btn.getAttribute('data-source-id');
            if (!id) return;
            btn.disabled = true;
            setStatus('Queueing rebuild...', false);
            try {
              const p = await fetchJson('/api/sources/' + encodeURIComponent(id) + '/rebuild', { method: 'POST' });
              setStatus('Rebuild queued. Job: ' + (p.job?.id || 'unknown'), false);
              await loadDashboard();
            } catch (err) {
              setStatus('Rebuild failed: ' + (err.message || String(err)), true);
              btn.disabled = false;
            }
          });
        });

        document.querySelectorAll('.change-source').forEach((btn) => {
          btn.addEventListener('click', () => {
            const id = btn.getAttribute('data-source-id');
            const name = btn.getAttribute('data-source-name') || '';
            const url = btn.getAttribute('data-source-url') || '';
            let links = [];
            try { links = JSON.parse(btn.getAttribute('data-source-links') || '[]'); } catch {}
            sourceNameInput.value = name;
            sourceUrlInput.value = url;
            // check ICS outputs
            sourceIcsOutputsEl.querySelectorAll('input[type="checkbox"]').forEach((cb) => {
              cb.checked = links.some((l) => l.target_type === 'ics' && l.target_key === cb.value);
            });
            // check Google outputs
            sourceGoogleOutputsEl.querySelectorAll('input[type="checkbox"]').forEach((cb) => {
              cb.checked = links.some((l) => l.target_type === 'google' && l.target_key === cb.value);
            });
            // restore icon/prefix state
            sourceRuleState.clear();
            links.forEach((l) => sourceRuleState.set(l.target_key, { icon: l.icon || '', prefix: l.prefix || '' }));
            syncTargetRuleInputs();
            sourceForm.setAttribute('data-editing-id', id);
            document.getElementById('source-save').textContent = 'Update Source';
            sourceNameInput.focus();
            sourceForm.scrollIntoView({ behavior: 'smooth', block: 'start' });
          });
        });

        document.querySelectorAll('.disable-source').forEach((btn) => {
          btn.addEventListener('click', async () => {
            const id = btn.getAttribute('data-source-id');
            if (!id) return;
            btn.disabled = true;
            try {
              await fetchJson('/api/sources/' + encodeURIComponent(id), { method: 'DELETE' });
              setStatus('Source disabled.', false);
              await loadDashboard();
            } catch (err) {
              setStatus('Disable failed: ' + (err.message || String(err)), true);
              btn.disabled = false;
            }
          });
        });

        document.querySelectorAll('.delete-source').forEach((btn) => {
          btn.addEventListener('click', async () => {
            const id = btn.getAttribute('data-source-id');
            if (!id) return;
            if (!confirm('Permanently delete this source and all its events? This cannot be undone.')) return;
            btn.disabled = true;
            try {
              await fetchJson('/api/sources/' + encodeURIComponent(id) + '?permanent=true', { method: 'DELETE' });
              setStatus('Source deleted.', false);
              await loadDashboard();
            } catch (err) {
              setStatus('Delete failed: ' + (err.message || String(err)), true);
              btn.disabled = false;
            }
          });
        });

        // google outputs table
        renderRows(
          targetsBody,
          registeredTargets.map((t) =>
            '<tr><td>' + (t.target_key || '') +
            '</td><td style="max-width:260px;overflow:hidden;text-overflow:ellipsis">' + (t.calendar_id || '') +
            '</td><td>' + (Number(t.is_active) ? 'yes' : 'no') +
            '</td><td><button class="danger delete-target" data-target-key="' + (t.target_key || '') + '">Delete</button></td></tr>'
          ).join(''),
          4
        );

        document.querySelectorAll('.delete-target').forEach((btn) => {
          btn.addEventListener('click', async () => {
            const key = btn.getAttribute('data-target-key');
            if (!key) return;
            btn.disabled = true;
            try {
              await fetchJson('/api/targets/' + encodeURIComponent(key), { method: 'DELETE' });
              setStatus('Deleted ' + key + '.', false);
              await loadDashboard();
            } catch (err) {
              setStatus('Delete failed: ' + (err.message || String(err)), true);
              btn.disabled = false;
            }
          });
        });

        // ics feeds table
        const feedContracts = contractsPayload.contracts || [];
        renderRows(
          contractsBody,
          feedContracts.map((c) =>
            '<tr>' +
            '<td><strong>' + c.target + '</strong></td>' +
            '<td style="font-size:0.8rem;word-break:break-all"><a href="' + c.url + '" target="_blank" rel="noopener">' + c.url + '</a></td>' +
            '<td><button class="copy-feed-url" data-url="' + c.url.replace(/"/g, '&quot;') + '">Copy</button></td>' +
            '</tr>'
          ).join(''),
          3
        );
        document.querySelectorAll('.copy-feed-url').forEach((btn) => {
          btn.addEventListener('click', () => {
            navigator.clipboard.writeText(btn.getAttribute('data-url')).then(() => {
              const orig = btn.textContent;
              btn.textContent = 'Copied!';
              setTimeout(() => { btn.textContent = orig; }, 1500);
            });
          });
        });

        // jobs
        renderRows(
          jobsBody,
          jobs.slice(0, 20).map((j) =>
            '<tr><td>' + (j.job_type || '') + '</td><td>' + (j.scope_type || 'system') + '</td><td>' + (j.status || '') + '</td></tr>'
          ).join(''),
          3
        );

        setStatus('Loaded.', false);
      } catch (err) {
        setStatus('Load failed: ' + (err.message || String(err)), true);
      } finally {
        refreshBtn.disabled = false;
        rebuildBtn.disabled = false;
      }
    }

    // --- Instance list & overrides ---

    async function loadInstances() {
      const sourceId = eventSourceFilter.value;
      const outputKey = eventOutputFilter.value;
      if (!sourceId && !outputKey) {
        instResultsEl.innerHTML = '<p class="hint" style="padding:8px 0">Pick a source to see its upcoming events.</p>';
        allInstances = [];
        return;
      }
      instResultsEl.innerHTML = '<p class="hint" style="padding:8px 0">Loading...</p>';
      openDrawerInstId = null;
      try {
        let url = '/api/instances?future=1&limit=200';
        if (sourceId) url += '&source=' + encodeURIComponent(sourceId);
        if (outputKey) url += '&output=' + encodeURIComponent(outputKey);
        const payload = await fetchJson(url);
        allInstances = payload.instances || [];
        renderInstances();
      } catch (err) {
        instResultsEl.innerHTML = '<p class="hint" style="padding:8px 0;color:#a00">Failed to load instances.</p>';
      }
    }

    function renderInstances() {
      const query = eventSearchInput.value.trim().toLowerCase();
      let rows = allInstances;
      if (query) rows = rows.filter((i) => (i.title || '').toLowerCase().includes(query));
      if (!rows.length) {
        instResultsEl.innerHTML = '<p class="hint" style="padding:8px 0">No upcoming events found.</p>';
        return;
      }
      instResultsEl.innerHTML = rows.slice(0, 100).map((inst) => {
        const dt = inst.occurrence_start_at ? new Date(inst.occurrence_start_at).toLocaleString(undefined, { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit' }) : '—';
        const meta = (inst.owner_type || '') + ' · ' + (inst.source_name || '');
        return '<div class="inst-row' + (inst.id === openDrawerInstId ? ' open' : '') + '" data-inst-id="' + (inst.id || '') + '" data-canonical-id="' + (inst.canonical_event_id || '') + '" data-title="' + (inst.title || '').replace(/"/g, '&quot;') + '" data-date="' + dt.replace(/"/g, '&quot;') + '" data-meta="' + meta.replace(/"/g, '&quot;') + '">' +
          '<span class="inst-row-date">' + dt + '</span>' +
          '<span class="inst-row-title">' + (inst.title || '') + '</span>' +
          '<span class="inst-row-meta">' + meta + '</span>' +
          '</div>' +
          (inst.id === openDrawerInstId ? '<div class="inst-drawer" id="drawer-' + inst.id + '"><p class="hint">Loading...</p></div>' : '');
      }).join('');

      instResultsEl.querySelectorAll('.inst-row').forEach((row) => {
        row.addEventListener('click', () => {
          const id = row.getAttribute('data-inst-id');
          const canonicalId = row.getAttribute('data-canonical-id');
          const title = row.getAttribute('data-title');
          const date = row.getAttribute('data-date');
          const meta = row.getAttribute('data-meta');
          if (openDrawerInstId === id) {
            openDrawerInstId = null;
            renderInstances();
          } else {
            openDrawerInstId = id;
            renderInstances(); // loads drawer content at the end
          }
        });
      });

      if (openDrawerInstId) {
        const drawerEl = document.getElementById('drawer-' + openDrawerInstId);
        if (drawerEl) loadDrawerContent(openDrawerInstId, drawerEl);
      }
    }

    async function loadDrawerContent(instanceId, drawerEl, canonicalId, title, date, meta) {
      if (!canonicalId) {
        // find from allInstances
        const inst = allInstances.find((i) => i.id === instanceId);
        if (inst) { canonicalId = inst.canonical_event_id; title = inst.title; }
      }
      try {
        const eventPayload = await fetchJson('/api/events/' + encodeURIComponent(canonicalId));
        const overrides = (eventPayload.overrides || [])
          .filter((ov) => !ov.event_instance_id || ov.event_instance_id === instanceId)
          .map((ov) => ({ ...ov, payload: typeof ov.payload_json === 'string' ? JSON.parse(ov.payload_json || '{}') : (ov.payload_json || {}) }));
        drawerEl.innerHTML =
          '<h4>' + (title || 'Event') + ' <span class="hint" style="font-weight:400">' + (date || '') + '</span></h4>' +
          '<form class="override-form" data-canonical-id="' + (canonicalId || '') + '" data-instance-id="' + instanceId + '">' +
          '<div class="field-group"><span class="field-label">Override type</span>' +
          '<select name="override-type">' +
          '<option value="skip">Skip — remove from all outputs</option>' +
          '<option value="hidden">Hidden — suppress from a specific output</option>' +
          '<option value="maybe">Maybe — flag as uncertain</option>' +
          '<option value="note">Note — attach context only</option>' +
          '</select></div>' +
          '<div class="field-group"><span class="field-label">Note (optional)</span><input type="text" name="override-note" placeholder="e.g. Can&#39;t make it this week" /></div>' +
          '<div class="field-group" style="justify-content:flex-end"><span class="field-label">&nbsp;</span><button class="primary" type="submit">Apply</button></div>' +
          '</form>' +
          (overrides.length ? '<div class="override-list">' + overrides.map((ov) =>
            '<div class="override-item" data-override-id="' + (ov.id || '') + '">' +
            '<div class="override-item-info"><strong>' + (ov.override_type || '') + '</strong>' +
            (ov.payload?.note ? ' — ' + ov.payload.note : '') +
            '<div class="override-item-meta">' + (ov.event_instance_id ? 'This instance' : 'All instances') + ' · by ' + (ov.created_by || '') + '</div></div>' +
            '<button class="danger remove-override" data-override-id="' + (ov.id || '') + '">Remove</button>' +
            '</div>'
          ).join('') + '</div>' : '');

        const form = drawerEl.querySelector('form');
        form.addEventListener('submit', async (e) => {
          e.preventDefault();
          const btn = form.querySelector('button[type="submit"]');
          btn.disabled = true;
          try {
            await fetchJson('/api/events/' + encodeURIComponent(canonicalId) + '/overrides', {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({
                overrideType: form.querySelector('[name="override-type"]').value,
                eventInstanceId: instanceId,
                payload: form.querySelector('[name="override-note"]').value ? { note: form.querySelector('[name="override-note"]').value } : {},
              }),
            });
            setStatus('Override applied.', false);
            await loadDrawerContent(instanceId, drawerEl, canonicalId, title, date, meta);
            await loadModifiedEvents();
          } catch (err) {
            setStatus('Override failed: ' + (err.message || String(err)), true);
            btn.disabled = false;
          }
        });

        drawerEl.querySelectorAll('.remove-override').forEach((btn) => {
          btn.addEventListener('click', async () => {
            const id = btn.getAttribute('data-override-id');
            if (!id) return;
            btn.disabled = true;
            try {
              await fetchJson('/api/overrides/' + encodeURIComponent(id), { method: 'DELETE' });
              setStatus('Override removed.', false);
              await loadDrawerContent(instanceId, drawerEl, canonicalId, title, date, meta);
              await loadModifiedEvents();
            } catch (err) {
              setStatus('Remove failed: ' + (err.message || String(err)), true);
              btn.disabled = false;
            }
          });
        });
      } catch (err) {
        drawerEl.innerHTML = '<p class="hint" style="color:#a00">Failed to load event detail.</p>';
      }
    }

    async function loadModifiedEvents() {
      try {
        const payload = await fetchJson('/api/overrides');
        const overrides = payload.overrides || [];
        if (!overrides.length) {
          modifiedEventsBody.innerHTML = '<tr><td colspan="6" class="hint" style="padding:8px">No future events with modifications.</td></tr>';
          return;
        }
        modifiedEventsBody.innerHTML = overrides.map((ov) => {
          const eventDt = ov.event_date ? new Date(ov.event_date).toLocaleString(undefined, { month: 'short', day: 'numeric', year: 'numeric', hour: 'numeric', minute: '2-digit' }) : '—';
          return '<tr>' +
            '<td style="white-space:nowrap">' + eventDt + '</td>' +
            '<td>' + (ov.title || '') + '</td>' +
            '<td>' + (ov.owner_type || '') + ' · ' + (ov.source_name || '') + '</td>' +
            '<td>' + (ov.override_type || '') + '</td>' +
            '<td>' + (ov.payload?.note || '') + '</td>' +
            '<td><button class="danger remove-modified" data-override-id="' + (ov.id || '') + '">Remove</button></td>' +
            '</tr>';
        }).join('');
        modifiedEventsBody.querySelectorAll('.remove-modified').forEach((btn) => {
          btn.addEventListener('click', async () => {
            const id = btn.getAttribute('data-override-id');
            if (!id) return;
            btn.disabled = true;
            try {
              await fetchJson('/api/overrides/' + encodeURIComponent(id), { method: 'DELETE' });
              setStatus('Override removed.', false);
              await loadModifiedEvents();
            } catch (err) {
              setStatus('Remove failed: ' + (err.message || String(err)), true);
              btn.disabled = false;
            }
          });
        });
      } catch (err) {
        // non-fatal, modified events table is secondary
      }
    }

    eventSourceFilter.addEventListener('change', loadInstances);
    eventOutputFilter.addEventListener('change', loadInstances);
    eventSearchInput.addEventListener('input', renderInstances);

    // --- Source form ---
    sourceIcsOutputsEl.addEventListener('change', syncTargetRuleInputs);
    sourceGoogleOutputsEl.addEventListener('change', syncTargetRuleInputs);

    function resetSourceForm() {
      sourceNameInput.value = '';
      sourceUrlInput.value = '';
      sourceIcsOutputsEl.querySelectorAll('input[type="checkbox"]').forEach((cb) => { cb.checked = false; });
      sourceGoogleOutputsEl.querySelectorAll('input[type="checkbox"]').forEach((cb) => { cb.checked = false; });
      sourceRuleState.clear();
      syncTargetRuleInputs();
      sourceForm.removeAttribute('data-editing-id');
      document.getElementById('source-save').textContent = 'Add Source';
    }

    sourceForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const saveBtn = document.getElementById('source-save');
      saveBtn.disabled = true;
      const editingId = sourceForm.getAttribute('data-editing-id') || null;
      setStatus(editingId ? 'Updating source...' : 'Saving source...', false);
      try {
        const sourceUrl = String(sourceUrlInput.value || '').trim();
        const selectedIcs = getCheckedValues(sourceIcsOutputsEl);
        const selectedGoogle = getCheckedValues(sourceGoogleOutputsEl);

        if (!sourceUrl.startsWith('http://') && !sourceUrl.startsWith('https://')) {
          throw new Error('Source URL must start with http:// or https://');
        }
        if (!selectedIcs.length && !selectedGoogle.length) {
          throw new Error('Select at least one output (ICS or Google).');
        }
        if (selectedIcs.includes('grayson') && selectedIcs.includes('naomi')) {
          throw new Error('A source cannot target both grayson and naomi ICS feeds.');
        }

        const ownerType = deriveOwnerType(selectedIcs);
        const allKeys = [...new Set([...selectedIcs, ...selectedGoogle])];
        const targetLinks = allKeys.map((key) => {
          const rule = sourceRuleState.get(key) || { icon: '', prefix: getDefaultPrefix(key, ownerType) };
          return { target_key: key, icon: rule.icon || '', prefix: rule.prefix || '' };
        });

        const body = {
          owner_type: ownerType,
          display_name: sourceNameInput.value,
          url: sourceUrl,
          include_in_child_ics: selectedIcs.includes('grayson') || selectedIcs.includes('naomi'),
          include_in_family_ics: selectedIcs.includes('family'),
          include_in_child_google_output: selectedGoogle.length > 0,
          target_links: targetLinks,
        };

        if (editingId) {
          await fetchJson('/api/sources/' + encodeURIComponent(editingId), {
            method: 'PATCH',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(body),
          });
          setStatus('Source updated.', false);
        } else {
          await fetchJson('/api/sources', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(body),
          });
          setStatus('Source saved. Queue a rebuild to ingest it now.', false);
        }

        resetSourceForm();
        await loadDashboard();
      } catch (err) {
        setStatus('Save failed: ' + (err.message || String(err)), true);
      } finally {
        saveBtn.disabled = false;
      }
    });

    // --- Google output form ---
    targetForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const saveBtn = document.getElementById('target-save');
      saveBtn.disabled = true;
      setStatus('Saving Google output...', false);
      try {
        const calendarId = String(targetCalendarInput.value || '').trim();
        if (!calendarId.includes('@')) {
          throw new Error('Calendar ID should be a full Google Calendar ID (contains "@").');
        }
        await fetchJson('/api/targets', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            target_key: targetKeyInput.value,
            calendar_id: calendarId,
            ownership_mode: targetModeInput.value,
          }),
        });
        targetKeyInput.value = '';
        targetCalendarInput.value = '';
        setStatus('Google output saved.', false);
        await loadDashboard();
      } catch (err) {
        setStatus('Save failed: ' + (err.message || String(err)), true);
      } finally {
        saveBtn.disabled = false;
      }
    });

    refreshBtn.addEventListener('click', loadDashboard);
    rebuildBtn.addEventListener('click', async () => {
      rebuildBtn.disabled = true;
      setStatus('Queueing full rebuild...', false);
      try {
        const p = await fetchJson('/api/rebuild/full', { method: 'POST' });
        setStatus('Full rebuild queued. Job: ' + (p.job?.id || 'unknown'), false);
        await loadDashboard();
      } catch (err) {
        setStatus('Rebuild failed: ' + (err.message || String(err)), true);
      } finally {
        rebuildBtn.disabled = false;
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
        const authError = await requireRole(request, env, ['admin', 'editor']);
        if (authError) return authError;
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
        return json(await repo.getFeedContract(request.url));
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
