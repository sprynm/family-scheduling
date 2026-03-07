const COPY_RESET_MS = 1500;

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
        const selectableGoogleTargets = registeredTargets.filter((t) => !logicalTargets.includes(t.target_key || ''));
        metricSources.textContent = String(sources.length);
        metricEvents.textContent = String(events.length);
        metricJobs.textContent = String(jobs.length);
        metricTargets.textContent = String(registeredTargets.length);

        // populate google outputs checkboxes in source form
        if (selectableGoogleTargets.length) {
          noGoogleHint.classList.add('hidden');
          sourceGoogleOutputsEl.innerHTML = selectableGoogleTargets
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
        const allOutputKeys = [...new Set([...logicalTargets, ...registeredTargets.map((t) => t.target_key || '')].filter(Boolean))];
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
            const orig = btn.textContent;
            navigator.clipboard.writeText(btn.getAttribute('data-url'))
              .then(() => {
                btn.textContent = 'Copied!';
                setTimeout(() => { btn.textContent = orig; }, COPY_RESET_MS);
              })
              .catch(() => {
                btn.textContent = 'Failed';
                setTimeout(() => { btn.textContent = orig; }, COPY_RESET_MS);
                setStatus('Clipboard permission denied.', true);
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
        const targetKey = String(targetKeyInput.value || '').trim().toLowerCase();
        const calendarId = String(targetCalendarInput.value || '').trim();
        if ([...TARGETS].includes(targetKey)) {
          throw new Error('Google output keys cannot be family, grayson, or naomi. Use a distinct key such as ' + targetKey + '_clubs.');
        }
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

    loadDashboard().catch((err) => {
      setStatus(err.message || String(err), true);
    });
