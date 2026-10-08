import { expandRecurringEvent, parseICS } from './ics.js';

export const DEFAULT_ICS_UPLOAD_MAX_BYTES = 1024 * 1024;
const MAX_UPLOAD_COMPONENTS = 20000;

function normalizeComponentMarkers(line) {
  return line.replace(/^(BEGIN|END):([^:]+)$/i, (_match, boundary, component) => `${boundary.toUpperCase()}:${component.toUpperCase()}`);
}

function validateCalendarStructure(body) {
  const unfolded = body
    .replace(/\r\n[ \t]/g, '')
    .replace(/\n[ \t]/g, '');
  const lines = unfolded.split(/\r?\n/);
  const stack = [];
  let rootOpened = false;
  let rootClosed = false;
  let eventCount = 0;
  let componentCount = 0;

  for (const rawLine of lines) {
    const line = normalizeComponentMarkers(rawLine.trim());
    if (!line) continue;
    const boundary = line.match(/^(BEGIN|END):([A-Z0-9-]+)$/);
    if (boundary?.[1] === 'BEGIN') {
      const component = boundary[2];
      componentCount += 1;
      if (componentCount > MAX_UPLOAD_COMPONENTS) throw new Error('ICS contains too many calendar components');
      if (!rootOpened) {
        if (component !== 'VCALENDAR') throw new Error('ICS must begin with VCALENDAR');
        rootOpened = true;
      } else if (rootClosed || component === 'VCALENDAR') {
        throw new Error('ICS must contain exactly one VCALENDAR');
      }
      if (component === 'VEVENT') {
        if (stack[stack.length - 1] !== 'VCALENDAR') throw new Error('VEVENT components must be inside VCALENDAR');
        eventCount += 1;
      }
      stack.push(component);
      continue;
    }
    if (boundary?.[1] === 'END') {
      const component = boundary[2];
      const openComponent = stack.pop();
      if (openComponent !== component) throw new Error(`ICS has an unbalanced ${component} component`);
      if (component === 'VCALENDAR') rootClosed = true;
      continue;
    }
    if (!stack.length || rootClosed) throw new Error('ICS contains content outside VCALENDAR');
  }

  if (!rootOpened || !rootClosed || stack.length) throw new Error('ICS is incomplete; expected a closed VCALENDAR');
  if (eventCount < 1) throw new Error('ICS must contain at least one VEVENT');
  return eventCount;
}

export function normalizeAndValidateUploadedICS(input, {
  defaultFloatingTimeZone = 'UTC',
  horizonDays = 180,
  lookbackDays = 7,
  maxBytes = DEFAULT_ICS_UPLOAD_MAX_BYTES,
  maxEvents = 5000,
  maxInstances = 5000,
  now = new Date(),
} = {}) {
  const source = String(input || '').replace(/^\uFEFF/, '').replace(/\r\n?/g, '\n');
  const body = source.split('\n').map(normalizeComponentMarkers).join('\r\n');
  const byteLength = new TextEncoder().encode(body).byteLength;
  if (byteLength > maxBytes) throw new Error(`ICS file exceeds the ${maxBytes}-byte upload limit`);

  const rawEventCount = validateCalendarStructure(body);
  if (rawEventCount > maxEvents) throw new Error(`ICS contains more than ${maxEvents} events`);
  const events = parseICS(body, { defaultFloatingTimeZone, strictDateProperties: true });
  if (events.length !== rawEventCount) {
    throw new Error('Every VEVENT must have a valid UID and DTSTART. No events were skipped.');
  }

  const identities = new Set();
  let expandedCount = 0;
  for (const event of events) {
    const start = Date.parse(event.startAt);
    const end = Date.parse(event.endAt);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
      throw new Error(`Event ${event.uid} has an invalid date range`);
    }
    const identity = `${event.uid}\u0000${event.recurrenceId || ''}`;
    if (identities.has(identity)) throw new Error(`ICS contains a duplicate UID/RECURRENCE-ID: ${event.uid}`);
    identities.add(identity);

    const remainingInstances = Math.max(1, maxInstances - expandedCount);
    const instances = expandRecurringEvent(event, {
      horizonDays,
      lookbackDays,
      now,
      maxOccurrences: remainingInstances,
    });
    expandedCount += instances.length;
    if (expandedCount > maxInstances) throw new Error(`ICS expands to more than ${maxInstances} occurrences`);
  }

  return { body, byteLength, events, eventCount: events.length, instanceCount: expandedCount };
}
