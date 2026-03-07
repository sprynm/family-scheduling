import { RRule } from 'rrule';

function unfoldLines(text) {
  return String(text || '')
    .replace(/\r\n[ \t]/g, '')
    .replace(/\n[ \t]/g, '');
}

function parsePropertyLine(line) {
  const idx = line.indexOf(':');
  if (idx === -1) return null;
  const left = line.slice(0, idx);
  const value = line.slice(idx + 1);
  const [name, ...paramParts] = left.split(';');
  const params = {};
  for (const part of paramParts) {
    const eqIdx = part.indexOf('=');
    if (eqIdx === -1) continue;
    params[part.slice(0, eqIdx).toUpperCase()] = part.slice(eqIdx + 1);
  }
  return { name: name.toUpperCase(), params, value };
}

function parseDateValue(raw, tzid) {
  if (!raw) return null;
  if (/^\d{8}$/.test(raw)) {
    return { iso: `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T00:00:00.000Z`, tzid: tzid || 'UTC', isDateOnly: true };
  }
  if (/^\d{8}T\d{6}Z$/.test(raw)) {
    const iso = `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T${raw.slice(9, 11)}:${raw.slice(11, 13)}:${raw.slice(13, 15)}.000Z`;
    return { iso, tzid: tzid || 'UTC', isDateOnly: false };
  }
  if (/^\d{8}T\d{6}$/.test(raw)) {
    const iso = `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T${raw.slice(9, 11)}:${raw.slice(11, 13)}:${raw.slice(13, 15)}.000Z`;
    return { iso, tzid: tzid || 'UTC', isDateOnly: false };
  }
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return null;
  return { iso: date.toISOString(), tzid: tzid || 'UTC', isDateOnly: false };
}

export function parseICS(text) {
  const unfolded = unfoldLines(text);
  const lines = unfolded.split(/\r?\n/);
  const events = [];
  let current = null;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;
    if (line === 'BEGIN:VEVENT') {
      current = { raw: [] };
      continue;
    }
    if (line === 'END:VEVENT') {
      if (current) events.push(current);
      current = null;
      continue;
    }
    if (!current) continue;
    current.raw.push(line);
    const parsed = parsePropertyLine(line);
    if (!parsed) continue;
    const { name, params, value } = parsed;
    if (name === 'UID') current.uid = value;
    if (name === 'SUMMARY') current.summary = value;
    if (name === 'DESCRIPTION') current.description = value;
    if (name === 'LOCATION') current.location = value;
    if (name === 'STATUS') current.status = value.toLowerCase();
    if (name === 'RRULE') current.rrule = value;
    if (name === 'DTSTART') current.dtstart = parseDateValue(value, params.TZID);
    if (name === 'DTEND') current.dtend = parseDateValue(value, params.TZID);
    if (name === 'RECURRENCE-ID') current.recurrenceId = parseDateValue(value, params.TZID)?.iso || value;
    if (name === 'LAST-MODIFIED') current.lastModified = parseDateValue(value, params.TZID)?.iso || value;
  }

  return events
    .filter((event) => event.uid && event.dtstart)
    .map((event) => ({
      uid: event.uid,
      summary: event.summary || '(untitled event)',
      description: event.description || '',
      location: event.location || '',
      status: event.status || 'confirmed',
      startAt: event.dtstart.iso,
      endAt: event.dtend?.iso || event.dtstart.iso,
      timezone: event.dtstart.tzid || 'UTC',
      rrule: event.rrule || null,
      recurrenceId: event.recurrenceId || null,
      lastModified: event.lastModified || null,
      raw: event.raw,
    }));
}

function inferDurationMs(event) {
  const start = new Date(event.startAt).getTime();
  const end = new Date(event.endAt).getTime();
  if (Number.isFinite(start) && Number.isFinite(end) && end > start) return end - start;
  return 60 * 60 * 1000;
}

export function expandRecurringEvent(event, { horizonDays = 180, lookbackDays = 7, now = new Date() } = {}) {
  if (!event.rrule) {
    return [
      {
        recurrenceInstanceKey: event.recurrenceId || event.startAt,
        occurrenceStartAt: event.startAt,
        occurrenceEndAt: event.endAt,
      },
    ];
  }

  const start = new Date(event.startAt);
  const durationMs = inferDurationMs(event);
  const windowStart = new Date(now);
  windowStart.setUTCDate(windowStart.getUTCDate() - lookbackDays);
  const windowEnd = new Date(now);
  windowEnd.setUTCDate(windowEnd.getUTCDate() + horizonDays);

  const ruleOptions = RRule.parseString(event.rrule);
  const rule = new RRule({ ...ruleOptions, dtstart: start });
  const occurrences = rule.between(windowStart, windowEnd, true);

  return occurrences.map((occurrence) => {
    const occurrenceEnd = new Date(occurrence.getTime() + durationMs);
    return {
      recurrenceInstanceKey: occurrence.toISOString(),
      occurrenceStartAt: occurrence.toISOString(),
      occurrenceEndAt: occurrenceEnd.toISOString(),
    };
  });
}

export function escapeText(value) {
  return String(value || '')
    .replace(/\\/g, '\\\\')
    .replace(/\r?\n/g, '\\n')
    .replace(/,/g, '\\,')
    .replace(/;/g, '\\;');
}

function foldLine(line) {
  if (line.length <= 75) return line;
  const parts = [];
  let remaining = line;
  while (remaining.length > 75) {
    parts.push(remaining.slice(0, 75));
    remaining = ` ${remaining.slice(75)}`;
  }
  parts.push(remaining);
  return parts.join('\r\n');
}

export function buildICS({ calendarName, description, events }) {
  const lines = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//Family Scheduling//Generated Calendar//EN',
    'CALSCALE:GREGORIAN',
    'METHOD:PUBLISH',
    `X-WR-CALNAME:${escapeText(calendarName)}`,
    `X-WR-CALDESC:${escapeText(description || calendarName)}`,
  ];

  for (const event of events) {
    lines.push('BEGIN:VEVENT');
    lines.push(`UID:${event.uid}`);
    lines.push(`DTSTAMP:${formatIcsDate(new Date())}`);
    lines.push(`DTSTART:${formatIcsDate(new Date(event.startAt))}`);
    lines.push(`DTEND:${formatIcsDate(new Date(event.endAt))}`);
    lines.push(`SUMMARY:${escapeText(event.summary)}`);
    if (event.description) lines.push(`DESCRIPTION:${escapeText(event.description)}`);
    if (event.location) lines.push(`LOCATION:${escapeText(event.location)}`);
    if (event.status && event.status !== 'confirmed') lines.push(`STATUS:${event.status.toUpperCase()}`);
    lines.push('END:VEVENT');
  }

  lines.push('END:VCALENDAR');
  return lines.map(foldLine).join('\r\n');
}

export function formatIcsDate(date) {
  const pad = (value) => String(value).padStart(2, '0');
  return `${date.getUTCFullYear()}${pad(date.getUTCMonth() + 1)}${pad(date.getUTCDate())}T${pad(date.getUTCHours())}${pad(date.getUTCMinutes())}${pad(date.getUTCSeconds())}Z`;
}
