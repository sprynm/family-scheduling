const statusEl = document.getElementById('event-page-status');
const eventSearchInput = document.getElementById('event-search');
const eventSourceFilter = document.getElementById('event-source-filter');
const eventOutputFilter = document.getElementById('event-output-filter');
const instResultsEl = document.getElementById('inst-results');
const modifiedEventsListEl = document.getElementById('modified-events-list');

let allInstances = [];
let openDrawerInstId = null;

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

function formatInstanceDate(value) {
  if (!value) return '—';
  return new Date(value).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
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
    loadDrawerContent(instanceId, drawerEl);
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
      const meta = instance.source_name || '';
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
        '<div class="inst-card-title">' +
        escapeHtml(instance.title || '') +
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
    loadDrawerContent(openDrawerInstId, drawerEl);
  }
}

async function loadDrawerContent(instanceId, drawerEl) {
  const instance = findInstance(instanceId);
  if (!instance) {
    drawerEl.innerHTML = '<p class="hint" style="color:#a00">Instance not found.</p>';
    return;
  }
  try {
    const eventPayload = await fetchJson('/api/events/' + encodeURIComponent(instance.canonical_event_id));
    const overrides = (eventPayload.overrides || [])
      .filter((override) => !override.event_instance_id || override.event_instance_id === instanceId)
      .map((override) => ({
        ...override,
        payload:
          typeof override.payload_json === 'string'
            ? JSON.parse(override.payload_json || '{}')
            : override.payload_json || {},
      }));

    drawerEl.innerHTML =
      '<h4>' +
      escapeHtml(instance.title || 'Event') +
      '</h4>' +
      '<p class="hint" style="margin-bottom:10px">' +
      escapeHtml(formatInstanceDate(instance.occurrence_start_at)) +
      '</p>' +
      '<form class="override-form mobile-override-form" data-canonical-id="' +
      escapeHtml(instance.canonical_event_id || '') +
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
        await fetchJson('/api/events/' + encodeURIComponent(instance.canonical_event_id) + '/overrides', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            overrideType: form.querySelector('[name="override-type"]').value,
            eventInstanceId: instanceId,
            payload: form.querySelector('[name="override-note"]').value
              ? { note: form.querySelector('[name="override-note"]').value }
              : {},
          }),
        });
        setStatus('Override applied.', false);
        await loadDrawerContent(instanceId, drawerEl);
        await loadModifiedEvents();
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
          await loadDrawerContent(instanceId, drawerEl);
          await loadModifiedEvents();
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

async function loadModifiedEvents() {
  try {
    const payload = await fetchJson('/api/overrides');
    const overrides = payload.overrides || [];
    if (!overrides.length) {
      modifiedEventsListEl.innerHTML = '<p class="hint" style="padding:8px 0">No future events with modifications.</p>';
      return;
    }

    modifiedEventsListEl.innerHTML = overrides
      .map((override) => {
        const metaParts = [override.source_name || ''].filter(Boolean);
        return (
          '<div class="modified-item">' +
          '<div class="modified-item-date">' +
          escapeHtml(formatInstanceDate(override.event_date)) +
          '</div>' +
          '<div class="modified-item-title">' +
          escapeHtml(override.title || '') +
          '</div>' +
          '<div class="modified-item-meta">' +
          escapeHtml(metaParts.join(' · ')) +
          '</div>' +
          '<div class="modified-item-note"><strong>' +
          escapeHtml(override.override_type || '') +
          '</strong>' +
          (override.payload?.note ? ' — ' + escapeHtml(override.payload.note) : '') +
          '</div>' +
          '<button class="danger remove-modified" data-override-id="' +
          escapeHtml(override.id || '') +
          '">Remove</button>' +
          '</div>'
        );
      })
      .join('');

    modifiedEventsListEl.querySelectorAll('.remove-modified').forEach((button) => {
      button.addEventListener('click', async () => {
        const overrideId = button.getAttribute('data-override-id');
        if (!overrideId) return;
        button.disabled = true;
        try {
          await fetchJson('/api/overrides/' + encodeURIComponent(overrideId), { method: 'DELETE' });
          setStatus('Override removed.', false);
          await loadModifiedEvents();
          if (openDrawerInstId) {
            const drawerEl = document.getElementById('drawer-' + openDrawerInstId);
            if (drawerEl) await loadDrawerContent(openDrawerInstId, drawerEl);
          }
        } catch (error) {
          setStatus('Remove failed: ' + (error.message || String(error)), true);
          button.disabled = false;
        }
      });
    });
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
