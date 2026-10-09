// Event planner: a phone-first view of upcoming events where anyone in the family can mark
// an event "not going", flag it as maybe, add a note, or hide it from one calendar, with undo.
// Mounts into every [data-event-planner] element. Uses the same API as the admin console.
(() => {
  // Toggle filters: none selected shows everyone; select one or more to narrow the list.
  const PEOPLE = [
    { key: 'grayson', label: 'Grayson' },
    { key: 'naomi', label: 'Naomi' },
    { key: 'family', label: 'Family' },
  ];
  const PERSON_LABEL = { grayson: 'Grayson', naomi: 'Naomi', family: 'Family' };
  const CHANGE_LABEL = { skip: 'Not going', maybe: 'Maybe', note: 'Note', hidden: 'Hidden' };
  const STORAGE_KEY = 'eventPlanner.people';
  const TOAST_MS = 7000;
  const DAY_MS = 86400000;

  const timeFormat = new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit' });
  const dayFormat = new Intl.DateTimeFormat(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
  const dayFormatWithYear = new Intl.DateTimeFormat(undefined, { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });
  const utcDayFormat = new Intl.DateTimeFormat(undefined, { weekday: 'short', month: 'short', day: 'numeric', timeZone: 'UTC' });

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  async function fetchJson(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) {
      let message = response.status + ' ' + response.statusText;
      try {
        const payload = await response.json();
        message = payload.message || payload.error || message;
      } catch {}
      throw new Error(message);
    }
    return response.json();
  }

  function readStoredPeople() {
    try { return new Set(String(localStorage.getItem(STORAGE_KEY) || '').split(',').filter((key) => PERSON_LABEL[key])); } catch { return new Set(); }
  }

  function storePeople(people) {
    try { localStorage.setItem(STORAGE_KEY, [...people].join(',')); } catch {}
  }

  function listNames(keys) {
    const names = keys.map((key) => PERSON_LABEL[key] || key);
    return names.length > 1 ? names.slice(0, -1).join(', ') + ' and ' + names[names.length - 1] : names[0] || '';
  }

  // All-day events are stored as UTC midnight spanning whole days; show them by their UTC date.
  function isAllDay(startIso, endIso) {
    if (!/T00:00:00(\.000)?Z$/.test(String(startIso || ''))) return false;
    const span = Date.parse(endIso) - Date.parse(startIso);
    return Number.isFinite(span) && span >= DAY_MS && span % DAY_MS === 0;
  }

  function dayKey(date, allDay) {
    return allDay ? date.toISOString().slice(0, 10) : [date.getFullYear(), date.getMonth() + 1, date.getDate()].map((n) => String(n).padStart(2, '0')).join('-');
  }

  function describeWhen(startIso, endIso) {
    const start = new Date(startIso);
    if (Number.isNaN(start.getTime())) return { key: 'unknown', day: 'Date unknown', time: '' };
    const allDay = isAllDay(startIso, endIso);
    const key = dayKey(start, allDay);
    const today = dayKey(new Date(), false);
    const tomorrow = dayKey(new Date(Date.now() + DAY_MS), false);
    const sameYear = start.getFullYear() === new Date().getFullYear();
    const dateText = allDay ? utcDayFormat.format(start) : (sameYear ? dayFormat : dayFormatWithYear).format(start);
    const day = key === today ? 'Today' : key === tomorrow ? 'Tomorrow' : dateText;
    let time = 'All day';
    if (!allDay) {
      const end = new Date(endIso);
      time = timeFormat.format(start) + (end.getTime() > start.getTime() ? ' – ' + timeFormat.format(end) : '');
    }
    return { key, day, dateText, time, startTime: allDay ? 'All day' : timeFormat.format(start) };
  }

  let mountCount = 0;

  function sourceLabel(item) {
    const source = String(item.source_name || '').trim();
    return source && source !== PERSON_LABEL[item.owner_type] ? source : '';
  }

  function mount(root) {
    if (!root.id) root.id = 'event-planner-' + (++mountCount);
    const sticky = root.hasAttribute('data-planner-sticky');
    const state = {
      instances: [],
      overrides: [],
      targets: null,
      people: readStoredPeople(),
      query: '',
      tab: 'upcoming',
      loading: true,
      error: '',
      openEvent: null,
      busy: false,
    };

    root.classList.add('planner');
    if (sticky) root.classList.add('planner-sticky');
    root.innerHTML =
      '<div class="planner-controls">' +
        '<div class="planner-people" role="group" aria-label="Show only these people. None selected shows everyone."></div>' +
        '<div class="planner-search">' +
          '<label class="visually-hidden" for="planner-search-' + root.id + '">Search events</label>' +
          '<input type="search" id="planner-search-' + root.id + '" placeholder="Search events" autocomplete="off" enterkeyhint="search" />' +
        '</div>' +
        '<div class="planner-tabbar">' +
          '<div class="planner-tabs" role="tablist" aria-label="Event lists">' +
            '<button type="button" role="tab" id="' + root.id + '-tab-upcoming" aria-controls="' + root.id + '-list" data-planner-tab="upcoming">Upcoming</button>' +
            '<button type="button" role="tab" id="' + root.id + '-tab-changed" aria-controls="' + root.id + '-list" data-planner-tab="changed">Changed</button>' +
          '</div>' +
          '<button type="button" class="planner-refresh" data-planner-refresh>Refresh</button>' +
        '</div>' +
      '</div>' +
      '<div class="planner-list" id="' + root.id + '-list" role="tabpanel" aria-busy="true"></div>' +
      '<dialog class="planner-sheet" aria-labelledby="planner-sheet-title-' + root.id + '"></dialog>' +
      '<div class="planner-toast" role="status" aria-live="polite" hidden></div>';

    const peopleEl = root.querySelector('.planner-people');
    const searchEl = root.querySelector('.planner-search input');
    const listEl = root.querySelector('.planner-list');
    const sheetEl = root.querySelector('.planner-sheet');
    const toastEl = root.querySelector('.planner-toast');
    let toastTimer = null;


    // --- data ---

    function changesFor(instance) {
      return state.overrides.filter((override) => (override.event_instance_id
        ? override.event_instance_id === instance.id
        : override.canonical_event_id === instance.canonical_event_id));
    }

    function noteOf(override) {
      return String(override?.payload?.note || '').trim();
    }

    function hiddenTargetLabel(override) {
      const key = override?.payload?.target_key;
      const target = (state.targets || []).find((t) => (t.slug || t.target_key) === key || t.id === override?.payload?.target_id);
      return target?.display_name || key || 'one calendar';
    }

    function changeText(override) {
      if (override.override_type === 'note') return 'Note: ' + noteOf(override);
      if (override.override_type === 'hidden') return 'Hidden from ' + hiddenTargetLabel(override);
      const label = CHANGE_LABEL[override.override_type] || override.override_type;
      return noteOf(override) ? label + ' – ' + noteOf(override) : label;
    }

    async function loadAll() {
      state.loading = true;
      state.error = '';
      render();
      try {
        const [instancesPayload, overridesPayload] = await Promise.all([
          fetchJson('/api/instances?future=1&limit=500'),
          fetchJson('/api/overrides'),
        ]);
        state.instances = instancesPayload.instances || [];
        state.overrides = overridesPayload.overrides || [];
      } catch (error) {
        state.error = error.message || String(error);
      } finally {
        state.loading = false;
        render();
      }
    }

    async function reloadOverrides() {
      const payload = await fetchJson('/api/overrides');
      state.overrides = payload.overrides || [];
    }

    async function loadTargets() {
      if (state.targets) return state.targets;
      const payload = await fetchJson('/api/targets');
      state.targets = payload.targets || [];
      return state.targets;
    }

    // --- rendering ---

    function visibleInstances() {
      const query = state.query.trim().toLowerCase();
      return state.instances.filter((instance) => {
        if (!showsPerson(instance.owner_type)) return false;
        if (query && !String(instance.title || '').toLowerCase().includes(query) && !String(instance.source_name || '').toLowerCase().includes(query)) return false;
        return true;
      });
    }

    function showsPerson(owner) {
      return !state.people.size || state.people.has(owner);
    }

    function personCounts() {
      const counts = {};
      for (const instance of state.instances) counts[instance.owner_type] = (counts[instance.owner_type] || 0) + 1;
      return counts;
    }

    function renderPeople() {
      const counts = personCounts();
      peopleEl.innerHTML = PEOPLE
        .map((person) => '<button type="button" class="planner-chip" data-person="' + person.key + '" data-owner="' + person.key + '" aria-pressed="' + state.people.has(person.key) + '">' +
          '<span class="planner-chip-name">' + escapeHtml(person.label) + '</span>' +
          '<span class="planner-chip-count">' + (state.loading ? '' : (counts[person.key] || 0) + ((counts[person.key] || 0) === 1 ? ' event' : ' events')) + '</span></button>')
        .join('');
    }

    function renderTabs() {
      const changedCount = state.overrides.filter((override) => showsPerson(override.owner_type)).length;
      root.querySelectorAll('[data-planner-tab]').forEach((tab) => {
        const selected = tab.dataset.plannerTab === state.tab;
        tab.setAttribute('aria-selected', String(selected));
        tab.tabIndex = selected ? 0 : -1;
        if (tab.dataset.plannerTab === 'changed') tab.textContent = 'Changed' + (changedCount ? ' (' + changedCount + ')' : '');
      });
    }

    function eventButton(instance) {
      const when = describeWhen(instance.occurrence_start_at, instance.occurrence_end_at);
      const changes = changesFor(instance);
      const skipped = changes.some((change) => change.override_type === 'skip');
      const badges = changes.map((change) => '<span class="planner-badge" data-change="' + escapeHtml(change.override_type) + '">' + escapeHtml(CHANGE_LABEL[change.override_type] || change.override_type) + '</span>').join('');
      const notes = changes.filter((change) => noteOf(change)).map((change) => '<span class="planner-event-note">' + escapeHtml(noteOf(change)) + '</span>').join('');
      return '<li><button type="button" class="planner-event' + (skipped ? ' is-skipped' : '') + '" data-instance-id="' + escapeHtml(instance.id) + '">' +
        '<span class="planner-event-time">' + escapeHtml(when.startTime) + '</span>' +
        '<span class="planner-event-body">' +
          '<span class="planner-event-title">' + escapeHtml(instance.title || 'Untitled event') + '</span>' +
          '<span class="planner-event-meta"><span class="planner-person" data-owner="' + escapeHtml(instance.owner_type) + '">' + escapeHtml(PERSON_LABEL[instance.owner_type] || instance.owner_type || '') + '</span>' +
          '<span class="planner-source">' + escapeHtml(sourceLabel(instance)) + '</span></span>' +
          notes +
        '</span>' +
        (badges ? '<span class="planner-badges">' + badges + '</span>' : '') +
      '</button></li>';
    }

    function groupByDay(items, getWhen) {
      const groups = [];
      let current = null;
      for (const item of items) {
        const when = getWhen(item);
        if (!current || current.key !== when.key) {
          current = { key: when.key, day: when.day, dateText: when.dateText, items: [] };
          groups.push(current);
        }
        current.items.push(item);
      }
      return groups;
    }

    function dayHeading(group) {
      const extra = group.day !== group.dateText && group.dateText ? '<span class="planner-day-date">' + escapeHtml(group.dateText) + '</span>' : '';
      return '<h3 class="planner-day">' + escapeHtml(group.day) + extra + '</h3>';
    }

    function renderUpcoming() {
      const items = visibleInstances();
      if (!items.length) {
        if (state.query) {
          return '<div class="planner-empty"><p>No events match “' + escapeHtml(state.query) + '”.</p><button type="button" data-planner-clear-search>Clear search</button></div>';
        }
        if (state.people.size) {
          return '<div class="planner-empty"><p>Nothing coming up for ' + escapeHtml(listNames([...state.people])) + '.</p><button type="button" data-planner-show-everyone>Show everyone</button></div>';
        }
        return '<div class="planner-empty"><p>No upcoming events.</p></div>';
      }
      return groupByDay(items, (instance) => describeWhen(instance.occurrence_start_at, instance.occurrence_end_at))
        .map((group) => '<section class="planner-group">' + dayHeading(group) + '<ul class="planner-events">' + group.items.map(eventButton).join('') + '</ul></section>')
        .join('');
    }

    function renderChanged() {
      const items = state.overrides
        .filter((override) => showsPerson(override.owner_type))
        .sort((x, y) => String(x.event_date || '').localeCompare(String(y.event_date || '')));
      if (!items.length) {
        return '<div class="planner-empty"><p>No changes yet. Tap any upcoming event to mark it not going, maybe, or add a note.</p></div>';
      }
      return groupByDay(items, (override) => describeWhen(override.event_date, override.event_date))
        .map((group) => '<section class="planner-group">' + dayHeading(group) + '<ul class="planner-changes">' + group.items.map((override) => {
          const when = describeWhen(override.event_date, override.event_date);
          return '<li class="planner-change">' +
            '<span class="planner-event-time">' + escapeHtml(when.startTime) + '</span>' +
            '<span class="planner-event-body">' +
              '<span class="planner-event-title">' + escapeHtml(override.title || 'Event') + '</span>' +
              '<span class="planner-event-meta"><span class="planner-person" data-owner="' + escapeHtml(override.owner_type) + '">' + escapeHtml(PERSON_LABEL[override.owner_type] || override.owner_type || '') + '</span>' +
              '<span class="planner-change-text" data-change="' + escapeHtml(override.override_type) + '">' + escapeHtml(changeText(override)) + (override.event_instance_id ? '' : ' (every time)') + '</span></span>' +
            '</span>' +
            '<button type="button" class="planner-undo" data-undo="' + escapeHtml(override.id) + '" aria-label="Undo ' + escapeHtml(changeText(override)) + ' for ' + escapeHtml(override.title || 'event') + '">Undo</button>' +
          '</li>';
        }).join('') + '</ul></section>')
        .join('');
    }

    function render() {
      renderPeople();
      renderTabs();
      listEl.setAttribute('aria-busy', String(state.loading));
      listEl.setAttribute('aria-labelledby', root.id + '-tab-' + state.tab);
      if (state.loading && !state.instances.length) {
        listEl.innerHTML = '<p class="planner-loading">Loading events…</p>';
        return;
      }
      if (state.error) {
        listEl.innerHTML = '<div class="planner-empty planner-error"><p>Couldn’t load events: ' + escapeHtml(state.error) + '</p><button type="button" data-planner-refresh>Try again</button></div>';
        return;
      }
      listEl.innerHTML = state.tab === 'changed' ? renderChanged() : renderUpcoming();
    }

    // --- sheet ---

    function sheetHtml(instance) {
      const when = describeWhen(instance.occurrence_start_at, instance.occurrence_end_at);
      const changes = changesFor(instance);
      const skipped = changes.some((change) => change.override_type === 'skip');
      const maybe = changes.some((change) => change.override_type === 'maybe');
      const disabled = state.busy ? ' disabled' : '';
      const current = changes.length
        ? '<div class="sheet-current"><h3>Current changes</h3><ul>' + changes.map((change) =>
            '<li><span data-change="' + escapeHtml(change.override_type) + '">' + escapeHtml(changeText(change)) + (change.event_instance_id ? '' : ' (every time)') + '</span>' +
            '<button type="button" class="planner-undo" data-undo="' + escapeHtml(change.id) + '"' + disabled + '>Undo</button></li>').join('') + '</ul></div>'
        : '';
      return '<div class="sheet-inner">' +
        '<div class="sheet-head">' +
          '<h2 id="planner-sheet-title-' + root.id + '" tabindex="-1">' + escapeHtml(instance.title || 'Event') + '</h2>' +
          '<button type="button" class="sheet-close" data-sheet-close aria-label="Close">Close</button>' +
        '</div>' +
        '<p class="sheet-when">' + escapeHtml(when.day + (when.day !== when.dateText ? ', ' + when.dateText : '') + ' · ' + when.time) + '</p>' +
        '<p class="sheet-who"><span class="planner-person" data-owner="' + escapeHtml(instance.owner_type) + '">' + escapeHtml(PERSON_LABEL[instance.owner_type] || instance.owner_type || '') + '</span> ' + escapeHtml(sourceLabel(instance)) + '</p>' +
        current +
        '<div class="sheet-actions">' +
          (skipped ? '' :
            '<button type="button" class="sheet-action sheet-action-skip" data-apply="skip"' + disabled + '>' +
              '<span class="sheet-action-label">Not going</span>' +
              '<span class="sheet-action-hint">Removes it from every calendar</span>' +
            '</button>') +
          (maybe ? '' :
            '<button type="button" class="sheet-action" data-apply="maybe"' + disabled + '>' +
              '<span class="sheet-action-label">Maybe</span>' +
              '<span class="sheet-action-hint">Adds “Maybe:” to the title on the calendars. Only this date changes.</span>' +
            '</button>') +
          '<form class="sheet-note" data-note-form>' +
            '<label for="planner-note-' + root.id + '">Add a note</label>' +
            '<p class="sheet-action-hint">Shows in this event’s details on the calendars. Only this date changes.</p>' +
            '<div class="sheet-note-row">' +
              '<input type="text" id="planner-note-' + root.id + '" name="note" maxlength="200" placeholder="e.g. Grandma driving" autocomplete="off" enterkeyhint="done" />' +
              '<button type="submit" class="sheet-note-save"' + disabled + '>Save</button>' +
            '</div>' +
          '</form>' +
        '</div>' +
        '<details class="sheet-more" data-sheet-more>' +
          '<summary>More options</summary>' +
          '<p class="sheet-action-hint">Hide this event from one calendar but keep it on the others.</p>' +
          '<div class="sheet-targets" data-sheet-targets><p class="sheet-action-hint">Loading calendars…</p></div>' +
        '</details>' +
        '<p class="sheet-error" role="alert" hidden></p>' +
      '</div>';
    }

    function openSheet(instance, { focusSelector = '' } = {}) {
      state.openEvent = instance;
      sheetEl.innerHTML = sheetHtml(instance);
      if (!sheetEl.open) sheetEl.showModal();
      // Land on the title so screen readers announce the event; the actions follow in reading order.
      const focusTarget = focusSelector ? sheetEl.querySelector(focusSelector) : null;
      (focusTarget || sheetEl.querySelector('.sheet-head h2'))?.focus();
    }

    function refreshSheet() {
      if (!state.openEvent || !sheetEl.open) return;
      const moreOpen = sheetEl.querySelector('[data-sheet-more]')?.open;
      openSheet(state.openEvent);
      if (moreOpen) {
        const more = sheetEl.querySelector('[data-sheet-more]');
        more.open = true;
        renderTargets();
      }
    }

    function closeSheet() {
      if (sheetEl.open) sheetEl.close();
    }

    function showSheetError(message) {
      const errorEl = sheetEl.querySelector('.sheet-error');
      if (!errorEl) return;
      errorEl.textContent = message;
      errorEl.hidden = false;
    }

    async function renderTargets() {
      const container = sheetEl.querySelector('[data-sheet-targets]');
      if (!container || !state.openEvent) return;
      try {
        const targets = await loadTargets();
        const owner = state.openEvent.owner_type;
        // Offer the calendars this person's events can appear on: the family feed plus their own feeds and Google calendars.
        const relevant = targets.filter((target) => {
          const slug = target.slug || target.target_key || '';
          return slug === 'family' || owner === 'family' || slug === owner || slug.startsWith(owner + '_');
        });
        const hidden = changesFor(state.openEvent).filter((change) => change.override_type === 'hidden').map((change) => change.payload?.target_key);
        container.innerHTML = relevant.length
          ? relevant.map((target) => {
              const slug = target.slug || target.target_key || '';
              const isHidden = hidden.includes(slug);
              return '<button type="button" class="sheet-target" data-hide-target="' + escapeHtml(slug) + '" data-hide-target-id="' + escapeHtml(target.id || '') + '"' + (isHidden || state.busy ? ' disabled' : '') + '>' +
                escapeHtml(target.display_name || slug) + (isHidden ? ' (hidden)' : '') + '</button>';
            }).join('')
          : '<p class="sheet-action-hint">No calendars found.</p>';
      } catch (error) {
        container.innerHTML = '<p class="sheet-action-hint">Couldn’t load calendars: ' + escapeHtml(error.message || String(error)) + '</p>';
      }
    }

    // --- actions ---

    function findNewChange(instance, overrideType, payload) {
      const candidates = changesFor(instance).filter((change) => change.override_type === overrideType
        && (change.event_instance_id || '') === instance.id
        && noteOf(change) === String(payload.note || '').trim()
        && (overrideType !== 'hidden' || change.payload?.target_key === payload.target_key));
      return candidates.sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')))[0] || null;
    }

    async function applyChange(instance, overrideType, payload = {}) {
      if (state.busy) return;
      state.busy = true;
      sheetEl.querySelectorAll('button, input').forEach((el) => { el.disabled = true; });
      try {
        await fetchJson('/api/events/' + encodeURIComponent(instance.canonical_event_id) + '/overrides', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ overrideType, eventInstanceId: instance.id, payload }),
        });
        await reloadOverrides();
        state.busy = false;
        const created = findNewChange(instance, overrideType, payload);
        closeSheet();
        render();
        const title = instance.title || 'Event';
        const message = overrideType === 'skip'
          ? title + ' removed from calendars.'
          : overrideType === 'hidden'
            ? title + ' hidden from ' + hiddenTargetLabel(created || { payload }) + '.'
            : overrideType === 'maybe'
              ? title + ' marked maybe on the calendars.'
              : 'Note added to ' + title + ' on the calendars.';
        showToast(message, created ? created.id : null);
      } catch (error) {
        state.busy = false;
        refreshSheet();
        showSheetError('Couldn’t save: ' + (error.message || String(error)));
      }
    }

    async function undoChange(overrideId, { fromToast = false } = {}) {
      if (!overrideId || state.busy) return;
      state.busy = true;
      if (sheetEl.open) sheetEl.querySelectorAll('button, input').forEach((el) => { el.disabled = true; });
      try {
        await fetchJson('/api/overrides/' + encodeURIComponent(overrideId), { method: 'DELETE' });
        await reloadOverrides();
        state.busy = false;
        render();
        refreshSheet();
        showToast(fromToast ? 'Undone.' : 'Change removed.', null);
      } catch (error) {
        state.busy = false;
        refreshSheet();
        if (sheetEl.open) showSheetError('Couldn’t undo: ' + (error.message || String(error)));
        else showToast('Couldn’t undo: ' + (error.message || String(error)), null, { error: true });
      }
    }

    function showToast(message, undoId, { error = false } = {}) {
      clearTimeout(toastTimer);
      toastEl.classList.toggle('is-error', error);
      toastEl.innerHTML = '<span>' + escapeHtml(message) + '</span>' +
        (undoId ? '<button type="button" data-toast-undo="' + escapeHtml(undoId) + '">Undo</button>' : '');
      toastEl.hidden = false;
      toastTimer = setTimeout(() => { toastEl.hidden = true; }, undoId ? TOAST_MS + 3000 : TOAST_MS);
    }

    // --- events ---

    peopleEl.addEventListener('click', (event) => {
      const chip = event.target.closest('[data-person]');
      if (!chip) return;
      const key = chip.dataset.person;
      if (state.people.has(key)) state.people.delete(key);
      else state.people.add(key);
      storePeople(state.people);
      render();
      peopleEl.querySelector('[data-person="' + key + '"]')?.focus();
    });

    searchEl.addEventListener('input', () => {
      state.query = searchEl.value;
      if (state.tab !== 'upcoming') state.tab = 'upcoming';
      render();
    });

    root.querySelector('.planner-tabs').addEventListener('click', (event) => {
      const tab = event.target.closest('[data-planner-tab]');
      if (!tab) return;
      state.tab = tab.dataset.plannerTab;
      render();
    });
    root.querySelector('.planner-tabs').addEventListener('keydown', (event) => {
      if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return;
      state.tab = state.tab === 'upcoming' ? 'changed' : 'upcoming';
      render();
      root.querySelector('[data-planner-tab="' + state.tab + '"]').focus();
    });

    root.addEventListener('click', (event) => {
      if (event.target.closest('[data-planner-refresh]')) { loadAll(); return; }
      if (event.target.closest('[data-planner-clear-search]')) {
        searchEl.value = '';
        state.query = '';
        render();
        searchEl.focus();
        return;
      }
      if (event.target.closest('[data-planner-show-everyone]')) {
        state.people.clear();
        storePeople(state.people);
        render();
        return;
      }
      const eventButtonEl = event.target.closest('.planner-event');
      if (eventButtonEl) {
        const instance = state.instances.find((item) => item.id === eventButtonEl.dataset.instanceId);
        if (instance) openSheet(instance);
        return;
      }
      const undo = event.target.closest('.planner-list [data-undo]');
      if (undo) { undoChange(undo.dataset.undo); return; }
      const toastUndo = event.target.closest('[data-toast-undo]');
      if (toastUndo) {
        toastEl.hidden = true;
        undoChange(toastUndo.dataset.toastUndo, { fromToast: true });
      }
    });

    sheetEl.addEventListener('click', (event) => {
      if (event.target === sheetEl) { closeSheet(); return; }
      if (event.target.closest('[data-sheet-close]')) { closeSheet(); return; }
      const instance = state.openEvent;
      if (!instance) return;
      const apply = event.target.closest('[data-apply]');
      if (apply) { applyChange(instance, apply.dataset.apply); return; }
      const hide = event.target.closest('[data-hide-target]');
      if (hide) {
        applyChange(instance, 'hidden', { target_key: hide.dataset.hideTarget, target_id: hide.dataset.hideTargetId || undefined });
        return;
      }
      const undo = event.target.closest('[data-undo]');
      if (undo) undoChange(undo.dataset.undo);
    });

    sheetEl.addEventListener('submit', (event) => {
      if (!event.target.matches('[data-note-form]')) return;
      event.preventDefault();
      const note = String(event.target.note.value || '').trim();
      if (!note) {
        showSheetError('Type a note first.');
        event.target.note.focus();
        return;
      }
      applyChange(state.openEvent, 'note', { note });
    });

    sheetEl.addEventListener('toggle', (event) => {
      if (event.target.matches('[data-sheet-more]') && event.target.open) renderTargets();
    }, true);

    sheetEl.addEventListener('close', () => {
      const instanceId = state.openEvent?.id;
      state.openEvent = null;
      // Return focus to the event that opened the sheet.
      if (instanceId) root.querySelector('.planner-event[data-instance-id="' + CSS.escape(instanceId) + '"]')?.focus();
    });

    render();
    loadAll();
  }

  document.querySelectorAll('[data-event-planner]').forEach(mount);
})();
