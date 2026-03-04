function joinParts(parts) {
  return parts
    .map((part) => String(part || '').trim())
    .filter(Boolean)
    .join(' ');
}

export function decorateEventSummary({ target, title, sourceIcon = '', sourcePrefix = '' }) {
  if (target === 'family') {
    return joinParts([sourcePrefix, sourceIcon, title]);
  }

  if (target === 'grayson' || target === 'naomi') {
    return joinParts([sourceIcon, title]);
  }

  return String(title || '').trim();
}
