import { datetime, RRule } from 'rrule';

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

function unescapeText(value) {
  return String(value || '')
    .replace(/\\n/gi, '\n')
    .replace(/\\,/g, ',')
    .replace(/\\;/g, ';')
    .replace(/\\\\/g, '\\');
}

function normalizeTimeZone(timeZone) {
  const value = String(timeZone || '').trim();
  return value || 'UTC';
}

function buildNumericDateTimeParts(parts) {
  if (!parts) return null;
  return {
    year: Number(parts.year),
    month: Number(parts.month),
    day: Number(parts.day),
    hour: Number(parts.hour),
    minute: Number(parts.minute),
    second: Number(parts.second),
  };
}

function parseDateTimeParts(value) {
  const match = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{3}))?(Z)?$/);
  if (!match) return null;
  return {
    year: match[1],
    month: match[2],
    day: match[3],
    hour: match[4],
    minute: match[5],
    second: match[6],
    millisecond: match[7] || '000',
    isUtc: Boolean(match[8]),
  };
}

function partsFromInstantInTimeZone(date, timeZone) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: normalizeTimeZone(timeZone),
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  const parts = {};
  for (const part of formatter.formatToParts(date)) {
    if (part.type === 'literal') continue;
    parts[part.type] = part.value;
  }
  return {
    year: parts.year || '1970',
    month: parts.month || '01',
    day: parts.day || '01',
    hour: parts.hour || '00',
    minute: parts.minute || '00',
    second: parts.second || '00',
    millisecond: '000',
  };
}

function formatDateTimeParts(parts, style) {
  if (style === 'google') {
    return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}.000`;
  }
  return `${parts.year}${parts.month}${parts.day}T${parts.hour}${parts.minute}${parts.second}`;
}

const TIMEZONE_FORMATTER_CACHE = new Map();

function getTimeZoneFormatter(timeZone) {
  const zone = normalizeTimeZone(timeZone);
  if (!TIMEZONE_FORMATTER_CACHE.has(zone)) {
    // Let Intl reject unsupported TZIDs here instead of silently coercing them.
    // If we later add upstream timezone validation, centralize that behavior at this boundary.
    TIMEZONE_FORMATTER_CACHE.set(
      zone,
      new Intl.DateTimeFormat('en-US', {
        timeZone: zone,
        hour12: false,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
      })
    );
  }
  return TIMEZONE_FORMATTER_CACHE.get(zone);
}

function formatInTimeZoneParts(timestamp, timeZone) {
  const formatter = getTimeZoneFormatter(timeZone);
  if (!formatter) return null;
  const parts = formatter.formatToParts(new Date(timestamp));
  const values = {};
  for (const part of parts) {
    if (part.type === 'literal') continue;
    values[part.type] = part.value;
  }
  return {
    year: Number(values.year),
    month: Number(values.month),
    day: Number(values.day),
    hour: Number(values.hour),
    minute: Number(values.minute),
    second: Number(values.second),
  };
}

function datePartsToEpochUtc(parts) {
  if (!parts) return Number.NaN;
  return Date.UTC(
    parts.year,
    (parts.month || 1) - 1,
    parts.day || 1,
    parts.hour || 0,
    parts.minute || 0,
    parts.second || 0
  );
}

function zonedDateTimeToEpoch(parts, timeZone) {
  const targetPseudo = datePartsToEpochUtc(parts);
  let guess = targetPseudo;
  // Ambiguous local times during DST fall-back map to two valid instants.
  // This converges to whichever instant Intl resolves first for the repeated wall-clock hour.
  // We accept that bias here because compatibility feeds only need a stable absolute instant.
  for (let i = 0; i < 3; i += 1) {
    const zonedParts = formatInTimeZoneParts(guess, timeZone);
    if (!zonedParts) break;
    const zonedPseudo = datePartsToEpochUtc(zonedParts);
    const delta = targetPseudo - zonedPseudo;
    guess += delta;
    if (Math.abs(delta) < 60000) break;
  }
  return guess;
}

function formatFloatingIsoFromDate(date) {
  const pad = (value) => String(value).padStart(2, '0');
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}T${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}.000`;
}

function resolveFloatingTimeZone(rawValue, timeZone, floatingTimeZoneFallback) {
  const zone = normalizeTimeZone(timeZone);
  const parsed = parseDateTimeParts(rawValue);
  if (!parsed || parsed.isUtc || zone !== 'UTC') {
    return zone;
  }
  return normalizeTimeZone(floatingTimeZoneFallback);
}

function toAbsoluteDate(value, timeZone, { floatingTimeZoneFallback = 'UTC' } = {}) {
  const raw = String(value || '');
  if (!raw) return null;

  const parsed = parseDateTimeParts(raw);
  if (!parsed) {
    const date = new Date(raw);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  if (parsed.isUtc) {
    const date = new Date(raw);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  const numericParts = buildNumericDateTimeParts(parsed);
  const zone = resolveFloatingTimeZone(raw, timeZone, floatingTimeZoneFallback);
  if (zone === 'UTC') {
    return new Date(datePartsToEpochUtc(numericParts));
  }
  return new Date(zonedDateTimeToEpoch(numericParts, zone));
}

export function formatEventDateTimeValue(value, timeZone, { style = 'ics', timedStyle = 'preserve', floatingTimeZoneFallback = 'UTC' } = {}) {
  const raw = String(value || '');
  if (!raw) return null;

  const parsed = parseDateTimeParts(raw);
  if (!parsed) {
    const date = new Date(raw);
    if (Number.isNaN(date.getTime())) return null;
    return {
      type: 'dateTime',
      value: style === 'google' ? date.toISOString() : formatIcsDate(date),
      timeZone: 'UTC',
    };
  }

  if (timedStyle === 'utc') {
    const date = toAbsoluteDate(raw, timeZone, { floatingTimeZoneFallback });
    if (!date || Number.isNaN(date.getTime())) return null;
    return {
      type: 'dateTime',
      value: style === 'google' ? date.toISOString() : formatIcsDate(date),
      timeZone: 'UTC',
    };
  }

  const zone = resolveFloatingTimeZone(raw, timeZone, floatingTimeZoneFallback);
  let parts = parsed;
  let useUtcSuffix = zone === 'UTC' || parsed.isUtc;

  if (zone !== 'UTC' && parsed.isUtc) {
    parts = partsFromInstantInTimeZone(new Date(raw), zone);
    useUtcSuffix = false;
  }

  const valueText = `${formatDateTimeParts(parts, style)}${useUtcSuffix ? 'Z' : ''}`;
  return {
    type: 'dateTime',
    value: valueText,
    timeZone: zone,
  };
}

function parseDateValue(raw, tzid, fallbackTz, defaultFloatingTimeZone = 'UTC') {
  if (!raw) return null;
  if (/^\d{8}$/.test(raw)) {
    return { iso: `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T00:00:00.000Z`, tzid: tzid || 'UTC', isDateOnly: true };
  }
  if (/^\d{8}T\d{6}Z$/.test(raw)) {
    const iso = `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T${raw.slice(9, 11)}:${raw.slice(11, 13)}:${raw.slice(13, 15)}.000Z`;
    return { iso, tzid: tzid || 'UTC', isDateOnly: false };
  }
  if (/^\d{8}T\d{6}$/.test(raw)) {
    // Floating local time — no Z suffix so the consumer can treat it as wall-clock
    const iso = `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}T${raw.slice(9, 11)}:${raw.slice(11, 13)}:${raw.slice(13, 15)}.000`;
    return { iso, tzid: tzid || fallbackTz || defaultFloatingTimeZone || 'UTC', isDateOnly: false };
  }
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return null;
  return { iso: date.toISOString(), tzid: tzid || 'UTC', isDateOnly: false };
}

export function parseICS(text, { defaultFloatingTimeZone = 'UTC' } = {}) {
  const unfolded = unfoldLines(text);
  const lines = unfolded.split(/\r?\n/);
  const events = [];
  let current = null;

  // Extract X-WR-TIMEZONE from calendar header (lines before any VEVENT)
  let calendarTimezone = null;
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (line === 'BEGIN:VEVENT') break;
    if (line.startsWith('X-WR-TIMEZONE:')) {
      calendarTimezone = line.slice('X-WR-TIMEZONE:'.length).trim() || null;
    }
  }

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
    if (name === 'SUMMARY') current.summary = unescapeText(value);
    if (name === 'DESCRIPTION') current.description = unescapeText(value);
    if (name === 'LOCATION') current.location = unescapeText(value);
    if (name === 'STATUS') current.status = value.toLowerCase();
    if (name === 'RRULE') current.rrule = value;
    if (name === 'DTSTART') current.dtstart = parseDateValue(value, params.TZID, calendarTimezone, defaultFloatingTimeZone);
    if (name === 'DTEND') current.dtend = parseDateValue(value, params.TZID, calendarTimezone, defaultFloatingTimeZone);
    if (name === 'RECURRENCE-ID') current.recurrenceId = parseDateValue(value, params.TZID, calendarTimezone, defaultFloatingTimeZone)?.iso || value;
    if (name === 'LAST-MODIFIED') current.lastModified = parseDateValue(value, params.TZID, calendarTimezone, defaultFloatingTimeZone)?.iso || value;
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

  const startParts = parseDateTimeParts(event.startAt);
  const start = startParts
    ? datetime(
        Number(startParts.year),
        Number(startParts.month),
        Number(startParts.day),
        Number(startParts.hour),
        Number(startParts.minute),
        Number(startParts.second)
      )
    : new Date(event.startAt);
  const durationMs = inferDurationMs(event);
  const windowStart = new Date(now);
  windowStart.setUTCDate(windowStart.getUTCDate() - lookbackDays);
  const windowEnd = new Date(now);
  windowEnd.setUTCDate(windowEnd.getUTCDate() + horizonDays);

  const ruleOptions = RRule.parseString(event.rrule);
  const tzid = normalizeTimeZone(event.timezone);
  const rule = new RRule({ ...ruleOptions, dtstart: start });
  const occurrences = rule.between(windowStart, windowEnd, true);

  return occurrences.map((occurrence) => {
    const occurrenceEnd = new Date(occurrence.getTime() + durationMs);
    const isUtcSeries = tzid === 'UTC';
    return {
      recurrenceInstanceKey: isUtcSeries ? occurrence.toISOString() : formatFloatingIsoFromDate(occurrence),
      occurrenceStartAt: isUtcSeries ? occurrence.toISOString() : formatFloatingIsoFromDate(occurrence),
      occurrenceEndAt: isUtcSeries ? occurrenceEnd.toISOString() : formatFloatingIsoFromDate(occurrenceEnd),
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

export function buildICS({ calendarName, description, events, floatingTimeZoneFallback = 'UTC' }) {
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
    // Compatibility feeds emit timed events as absolute UTC instants rather than TZID-qualified
    // local times. This trades original timezone fidelity for more consistent subscriber behavior
    // across clients that disagree on floating/TZID handling.
    const start = formatEventDateTimeValue(event.startAt, event.timezone, {
      style: 'ics',
      timedStyle: 'utc',
      floatingTimeZoneFallback,
    });
    const end = formatEventDateTimeValue(event.endAt, event.timezone, {
      style: 'ics',
      timedStyle: 'utc',
      floatingTimeZoneFallback,
    });
    if (start?.type === 'date') {
      lines.push(`DTSTART;VALUE=DATE:${start.value}`);
    } else if (start?.value) {
      lines.push(`DTSTART:${start.value}`);
    }
    if (end?.type === 'date') {
      lines.push(`DTEND;VALUE=DATE:${end.value}`);
    } else if (end?.value) {
      lines.push(`DTEND:${end.value}`);
    }
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
