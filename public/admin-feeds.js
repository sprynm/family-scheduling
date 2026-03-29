const statusEl = document.getElementById('feed-debug-status');
const targetEl = document.getElementById('feed-target');
const lookbackEl = document.getElementById('feed-lookback');
const nameEl = document.getElementById('feed-name');
const loadBtn = document.getElementById('feed-load');
const openLinkEl = document.getElementById('feed-open-link');
const copyBtn = document.getElementById('feed-copy-link');
const feedUrlTextEl = document.getElementById('feed-url-text');
const countEl = document.getElementById('feed-summary-count');
const targetSummaryEl = document.getElementById('feed-summary-target');
const nameSummaryEl = document.getElementById('feed-summary-name');
const lookbackSummaryEl = document.getElementById('feed-summary-lookback');
const bodyEl = document.getElementById('feed-preview-body');

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

function formatDate(value) {
  if (!value) return '—';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return escapeHtml(value);
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const mi = String(date.getMinutes()).padStart(2, '0');
  return `${yyyy}.${mm}.${dd} ${hh}:${mi}`;
}

function buildPreviewUrl() {
  const params = new URLSearchParams();
  params.set('target', targetEl.value);
  const lookback = Number.parseInt(String(lookbackEl.value || ''), 10);
  params.set('lookback', String(Number.isFinite(lookback) && lookback > 0 ? lookback : 7));
  const requestedName = String(nameEl.value || '').trim();
  if (requestedName) params.set('name', requestedName);
  return `/api/feed-preview?${params.toString()}`;
}

function updateFeedLink(feedUrl) {
  const safeUrl = feedUrl || '#';
  openLinkEl.href = safeUrl;
  openLinkEl.setAttribute('aria-disabled', feedUrl ? 'false' : 'true');
  feedUrlTextEl.textContent = feedUrl || '-';
}

function renderRows(events) {
  if (!events.length) {
    bodyEl.innerHTML = '<tr><td colspan="6" style="color:#aaa;font-style:italic">No rows match the current feed preview.</td></tr>';
    return;
  }

  bodyEl.innerHTML = events.map((event) => (
    '<tr>' +
      '<td class="feed-cell-time">' + escapeHtml(formatDate(event.startAt)) + '</td>' +
      '<td class="feed-cell-time">' + escapeHtml(formatDate(event.endAt)) + '</td>' +
      '<td class="feed-cell-summary">' +
        '<div class="feed-summary-primary">' + escapeHtml(event.summary || '—') + '</div>' +
        '<div class="feed-summary-secondary">' + escapeHtml(event.description || event.sourceTitle || '') + '</div>' +
      '</td>' +
      '<td>' + escapeHtml(event.location || '—') + '</td>' +
      '<td><span class="feed-status-tag">' + escapeHtml(event.status || 'confirmed') + '</span></td>' +
      '<td class="feed-cell-uid"><code>' + escapeHtml(event.uid || '') + '</code></td>' +
    '</tr>'
  )).join('');
}

async function loadPreview() {
  loadBtn.disabled = true;
  copyBtn.disabled = true;
  setStatus('Loading feed preview...', false);
  try {
    const payload = await fetchJson(buildPreviewUrl());
    targetSummaryEl.textContent = String(payload.target || '—').toUpperCase();
    countEl.textContent = String((payload.events || []).length);
    nameSummaryEl.textContent = payload.calendarName || '—';
    lookbackSummaryEl.textContent = `${payload.lookbackDays || 0} days`;
    updateFeedLink(payload.feedUrl || null);
    renderRows(payload.events || []);
    setStatus(`Loaded ${payload.events?.length || 0} rows for ${payload.target || 'feed'}.`, false);
  } catch (error) {
    renderRows([]);
    updateFeedLink(null);
    countEl.textContent = '-';
    targetSummaryEl.textContent = '-';
    nameSummaryEl.textContent = '-';
    lookbackSummaryEl.textContent = '-';
    setStatus('Feed preview failed: ' + (error.message || String(error)), true);
  } finally {
    loadBtn.disabled = false;
    copyBtn.disabled = false;
  }
}

copyBtn.addEventListener('click', async () => {
  const url = openLinkEl.href && openLinkEl.href !== '#' ? openLinkEl.href : '';
  if (!url) {
    setStatus('No feed URL is available to copy yet.', true);
    return;
  }
  try {
    await navigator.clipboard.writeText(url);
    setStatus('Feed URL copied.', false);
  } catch {
    setStatus('Copy failed.', true);
  }
});

loadBtn.addEventListener('click', loadPreview);
targetEl.addEventListener('change', loadPreview);
lookbackEl.addEventListener('change', loadPreview);
nameEl.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    loadPreview();
  }
});

loadPreview();
