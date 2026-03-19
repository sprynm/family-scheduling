const statusEl = document.getElementById('event-page-status');
const eventSearchInput = document.getElementById('event-search');
const eventSourceFilter = document.getElementById('event-source-filter');
const eventOutputFilter = document.getElementById('event-output-filter');
const instResultsEl = document.getElementById('inst-results');
const modifiedEventsListEl = document.getElementById('modified-events-list');

let allInstances = [];
let openDrawerInstId = null;
let openModifiedOverrideId = null;
let activeOverrides = [];

function setStatus(message, isError) {
  statusEl.textContent = message;
  statusEl.className = isError ? 'status error' : 'status';
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
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

function padDatePart(value) {
  return String(value).padStart(2, '0');
}

function formatInstanceDate(value, { includeTime = false } = {}) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '—';
  const stamp = [
    date.getFullYear(),
    padDatePart(date.getMonth() + 1),
    padDatePart(date.getDate()),
  ].join('.');
  if (!includeTime) return stamp;
  return stamp + ' ' + padDatePart(date.getHours()) + ':' + padDatePart(date.getMinutes());
}

function normalizeNote(value) {
  return String(value || '').trim();
}

function buildDrawerContext({ instanceId = '', canonicalId = '', title = '', date = '' } = {}) {
  return { instanceId, canonicalId, title, date };
}

function getOverridesForContext(overrides, instanceId) {
  return (overrides || []).filter((override) => (instanceId ? (!override.event_instance_id || override.event_instance_id === instanceId) : !override.event_instance_id));
}

function getMatchingOverrides(instance) {
  return activeOverrides.filter((override) => {
    if (override.event_instance_id) {
      return override.event_instance_id === instance.id;
    }
    return override.canonical_event_id === instance.canonical_event_id;
  });
}

function getInstanceStatusBadges(instance) {
  const matching = getMatchingOverrides(instance);
  const uniqueTypes = [...new Set(matching.map((override) => String(override.override_type || '').trim()).filter(Boolean))];
  return uniqueTypes;
}

function renderStatusBadges(types) {
  if (!types.length) return '';
  return (
    '<div class="inst-card-statuses">' +
    types
      .map((type) => '<span class="status-pill status-pill-' + escapeHtml(type) + '">' + escapeHtml(type) + '</span>')
      .join('') +
    '</div>'
  );
}

function hasMatchingOverride(overrides, overrideType, note) {
  const normalizedNote = normalizeNote(note);
  return overrides.some((override) => {
    const payload =
      typeof override.payload_json === 'string'
        ? JSON.parse(override.payload_json || '{}')
        : override.payload || override.payload_json || {};
    return String(override.override_type || '') === overrideType && normalizeNote(payload.note) === normalizedNote;
  });
}

async function loadFilters() {
  const [sourcesPayload, targetsPayload] = await Promise.all([
    fetchJson('/api/sources'),
    fetchJson('/api/targets'),
  ]);

  const sources = sourcesPayload.sources || [];
  const outputs = targetsPayload.targets || [];

  eventSourceFilter.innerHTML =
    '<option value="">— pick a source —</option>' +
    sources
      .map((source) =>
        '<option value="' +
        escapeHtml(source.id || '') +
        '">' +
        escapeHtml(source.display_name || source.name || '') +
        '</option>'
      )
      .join('');

  eventOutputFilter.innerHTML =
    '<option value="">All outputs</option>' +
    outputs
      .map((target) => {
        const slug = target.slug || target.target_key || '';
        return '<option value="' + escapeHtml(slug) + '">' + escapeHtml(target.display_name || slug) + '</option>';
      })
      .join('');
}

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
  } catch (error) {
    instResultsEl.innerHTML = '<p class="hint" style="padding:8px 0;color:#a00">Failed to load instances.</p>';
  }
}

function getVisibleInstances() {
  const query = eventSearchInput.value.trim().toLowerCase();
  const deduped = new Map();
  for (const instance of allInstances) {
    if (query && !(instance.title || '').toLowerCase().includes(query)) continue;
    if (!deduped.has(instance.id)) {
      deduped.set(instance.id, instance);
    }
  }
  return [...deduped.values()];
}

function findInstance(instanceId) {
  return allInstances.find((item) => item.id === instanceId) || null;
}

function closeDrawer(instanceId) {
  const card = instResultsEl.querySelector(`[data-inst-id="${CSS.escape(instanceId)}"]`);
  if (!card) return;
  card.classList.remove('open');
  const drawerEl = card.querySelector('.inst-drawer');
  if (drawerEl) drawerEl.remove();
}

function attachDrawerHandlers(card, instanceId) {
  card.addEventListener('click', () => {
    if (openDrawerInstId === instanceId) {
      closeDrawer(instanceId);
      openDrawerInstId = null;
      return;
    }

    if (openDrawerInstId) {
      closeDrawer(openDrawerInstId);
    }

    openDrawerInstId = instanceId;
    card.classList.add('open');
    const drawerEl = document.createElement('div');
    drawerEl.className = 'inst-drawer mobile-drawer';
    drawerEl.id = 'drawer-' + instanceId;
    drawerEl.innerHTML = '<p class="hint">Loading...</p>';
    drawerEl.addEventListener('click', (event) => event.stopPropagation());
    card.appendChild(drawerEl);
    const instance = findInstance(instanceId);
    loadDrawerContent(buildDrawerContext({
      instanceId,
      canonicalId: instance?.canonical_event_id || '',
      title: instance?.title || '',
      date: formatInstanceDate(instance?.occurrence_start_at, { includeTime: true }),
    }), drawerEl);
  });
}

function renderInstances() {
  const rows = getVisibleInstances();

  if (!rows.length) {
    instResultsEl.innerHTML = '<p class="hint" style="padding:8px 0">No upcoming events found.</p>';
    return;
  }

  instResultsEl.innerHTML = rows
    .slice(0, 100)
    .map((instance) => {
      const dateLabel = formatInstanceDate(instance.occurrence_start_at);
      const timeLabel = formatInstanceDate(instance.occurrence_start_at, { includeTime: true }).split(' ')[1] || '';
      const meta = [timeLabel, instance.source_name || ''].filter(Boolean).join(' · ');
      const statusBadges = renderStatusBadges(getInstanceStatusBadges(instance));
      return (
        '<div class="inst-card' +
        (instance.id === openDrawerInstId ? ' open' : '') +
        '" data-inst-id="' +
        escapeHtml(instance.id || '') +
        '" data-canonical-id="' +
        escapeHtml(instance.canonical_event_id || '') +
        '" data-title="' +
        escapeHtml(instance.title || '') +
        '">' +
        '<div class="inst-card-date">' +
        escapeHtml(dateLabel) +
        '</div>' +
        '<div class="inst-card-main">' +
        '<div class="inst-card-title">' +
        escapeHtml(instance.title || '') +
        '</div>' +
        statusBadges +
        '</div>' +
        '<div class="inst-card-meta">' +
        escapeHtml(meta) +
        '</div>' +
        '</div>'
      );
    })
    .join('');

  instResultsEl.querySelectorAll('.inst-card').forEach((row) => {
    const instanceId = row.getAttribute('data-inst-id');
    if (!instanceId) return;
    attachDrawerHandlers(row, instanceId);
  });

  if (openDrawerInstId) {
    const openCard = instResultsEl.querySelector(`[data-inst-id="${CSS.escape(openDrawerInstId)}"]`);
    if (!openCard) {
      openDrawerInstId = null;
      return;
    }
    const drawerEl = document.createElement('div');
    drawerEl.className = 'inst-drawer mobile-drawer';
    drawerEl.id = 'drawer-' + openDrawerInstId;
    drawerEl.innerHTML = '<p class="hint">Loading...</p>';
    drawerEl.addEventListener('click', (event) => event.stopPropagation());
    openCard.appendChild(drawerEl);
    const instance = findInstance(openDrawerInstId);
    loadDrawerContent(buildDrawerContext({
      instanceId: openDrawerInstId,
      canonicalId: instance?.canonical_event_id || '',
      title: instance?.title || '',
      date: formatInstanceDate(instance?.occurrence_start_at, { includeTime: true }),
    }), drawerEl);
  }
}

async function loadDrawerContent(context, drawerEl) {
  const instanceId = context?.instanceId || '';
  let canonicalId = context?.canonicalId || '';
  let title = context?.title || '';
  let date = context?.date || '';
  if (!canonicalId && instanceId) {
    const instance = findInstance(instanceId);
    if (instance) {
      canonicalId = instance.canonical_event_id;
      title = instance.title;
      date = formatInstanceDate(instance.occurrence_start_at, { includeTime: true });
    }
  }
  if (!canonicalId) {
    drawerEl.innerHTML = '<p class="hint" style="color:#a00">Instance not found.</p>';
    return;
  }
  try {
    const eventPayload = await fetchJson('/api/events/' + encodeURIComponent(canonicalId));
    const overrides = getOverridesForContext(eventPayload.overrides || [], instanceId)
      .map((override) => ({
        ...override,
        payload:
          typeof override.payload_json === 'string'
            ? JSON.parse(override.payload_json || '{}')
            : override.payload_json || {},
      }));

    drawerEl.innerHTML =
      '<h4>' +
      escapeHtml(title || 'Event') +
      '</h4>' +
      '<p class="hint" style="margin-bottom:10px">' +
      escapeHtml(date) +
      '</p>' +
      '<form class="override-form mobile-override-form" data-canonical-id="' +
      escapeHtml(canonicalId || '') +
      '" data-instance-id="' +
      escapeHtml(instanceId) +
      '">' +
      '<div class="field-group"><span class="field-label">Override type</span>' +
      '<select name="override-type">' +
      '<option value="skip">Skip — remove from all outputs</option>' +
      '<option value="hidden">Hidden — suppress from a specific output</option>' +
      '<option value="maybe">Maybe — flag as uncertain</option>' +
      '<option value="note">Note — attach context only</option>' +
      '</select></div>' +
      '<div class="field-group"><span class="field-label">Note</span><input type="text" name="override-note" placeholder="Optional note" /></div>' +
      '<button class="primary" type="submit">Apply Override</button>' +
      '</form>' +
      (overrides.length
        ? '<div class="override-list">' +
          overrides
            .map(
              (override) =>
                '<div class="override-item">' +
                '<div class="override-item-info"><strong>' +
                escapeHtml(override.override_type || '') +
                '</strong>' +
                (override.payload?.note ? ' — ' + escapeHtml(override.payload.note) : '') +
                '<div class="override-item-meta">' +
                escapeHtml((override.event_instance_id ? 'This instance' : 'All instances') + ' · by ' + (override.created_by || '')) +
                '</div></div>' +
                '<button class="danger remove-override" data-override-id="' +
                escapeHtml(override.id || '') +
                '">Remove</button>' +
                '</div>'
            )
            .join('') +
          '</div>'
        : '')
      ;

    const form = drawerEl.querySelector('form');
    form.addEventListener('submit', async (event) => {
      event.preventDefault();
      const button = form.querySelector('button[type="submit"]');
      button.disabled = true;
      try {
        const overrideType = form.querySelector('[name="override-type"]').value;
        const note = normalizeNote(form.querySelector('[name="override-note"]').value);
        if (hasMatchingOverride(overrides, overrideType, note)) {
          setStatus('That override is already active for this item.', true);
          button.disabled = false;
          return;
        }
        await fetchJson('/api/events/' + encodeURIComponent(canonicalId) + '/overrides', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            overrideType,
            eventInstanceId: instanceId || undefined,
            payload: note
              ? { note }
              : {},
          }),
        });
        setStatus('Override applied.', false);
        await loadDrawerContent(buildDrawerContext({ instanceId, canonicalId, title, date }), drawerEl);
        await loadModifiedEvents();
        renderInstances();
      } catch (error) {
        setStatus('Override failed: ' + (error.message || String(error)), true);
        button.disabled = false;
      }
    });

    drawerEl.querySelectorAll('.remove-override').forEach((button) => {
      button.addEventListener('click', async () => {
        const overrideId = button.getAttribute('data-override-id');
        if (!overrideId) return;
        button.disabled = true;
        try {
          await fetchJson('/api/overrides/' + encodeURIComponent(overrideId), { method: 'DELETE' });
          setStatus('Override removed.', false);
          await loadDrawerContent(buildDrawerContext({ instanceId, canonicalId, title, date }), drawerEl);
          await loadModifiedEvents();
          renderInstances();
        } catch (error) {
          setStatus('Remove failed: ' + (error.message || String(error)), true);
          button.disabled = false;
        }
      });
    });
  } catch (error) {
    drawerEl.innerHTML = '<p class="hint" style="color:#a00">Failed to load event detail.</p>';
  }
}

function renderModifiedEvents() {
  if (!activeOverrides.length) {
    modifiedEventsListEl.innerHTML = '<p class="hint" style="padding:8px 0">No future events with modifications.</p>';
    renderInstances();
    return;
  }

  // Toggling drawers stays client-side; only real override mutations refetch /api/overrides.
  modifiedEventsListEl.innerHTML = activeOverrides
    .map((override) => {
      const metaParts = [override.source_name || ''].filter(Boolean);
      const note = normalizeNote(override.payload?.note);
      const isOpen = override.id === openModifiedOverrideId;
      return (
        '<div class="modified-item' + (isOpen ? ' open' : '') + '" data-override-id="' + escapeHtml(override.id || '') + '">' +
        '<div class="modified-item-main">' +
        '<div class="modified-item-date">' +
        escapeHtml(formatInstanceDate(override.event_date)) +
        '</div>' +
        '<div class="modified-item-title">' +
        escapeHtml(override.title || '') +
        '</div>' +
        '<span class="status-pill status-pill-' +
        escapeHtml(override.override_type || '') +
        '">' +
        escapeHtml(override.override_type || '') +
        '</span>' +
        '</div>' +
        '<div class="modified-item-meta">' +
        escapeHtml([...metaParts, note].filter(Boolean).join(' · ')) +
        '</div>' +
        (isOpen
          ? '<div class="modified-item-drawer"><div class="inst-drawer mobile-drawer" id="modified-drawer-' + escapeHtml(override.id || '') + '"><p class="hint">Loading...</p></div></div>'
          : '') +
        '</div>'
      );
    })
    .join('');
  renderInstances();

  modifiedEventsListEl.querySelectorAll('.modified-item').forEach((item) => {
    item.addEventListener('click', () => {
      const overrideId = item.getAttribute('data-override-id');
      if (!overrideId) return;
      openModifiedOverrideId = openModifiedOverrideId === overrideId ? null : overrideId;
      renderModifiedEvents();
    });
  });

  if (openModifiedOverrideId) {
    const override = activeOverrides.find((item) => item.id === openModifiedOverrideId);
    const drawerEl = document.getElementById('modified-drawer-' + openModifiedOverrideId);
    if (override && drawerEl) {
      drawerEl.addEventListener('click', (event) => event.stopPropagation());
      loadDrawerContent(buildDrawerContext({
        instanceId: override.event_instance_id || '',
        canonicalId: override.canonical_event_id || '',
        title: override.title || '',
        date: formatInstanceDate(override.event_date, { includeTime: true }),
      }), drawerEl);
    }
  }
}

async function loadModifiedEvents() {
  try {
    const payload = await fetchJson('/api/overrides');
    activeOverrides = payload.overrides || [];
    renderModifiedEvents();
  } catch (error) {
    modifiedEventsListEl.innerHTML = '<p class="hint" style="padding:8px 0;color:#a00">Failed to load modifications.</p>';
  }
}

async function loadPage() {
  setStatus('Loading...', false);
  try {
    await loadFilters();
    await loadModifiedEvents();
    setStatus('Loaded.', false);
  } catch (error) {
    setStatus('Load failed: ' + (error.message || String(error)), true);
  }
}

eventSourceFilter.addEventListener('change', loadInstances);
eventOutputFilter.addEventListener('change', loadInstances);
eventSearchInput.addEventListener('input', renderInstances);

loadPage();
