function joinParts(parts) {
  return parts
    .map((part) => String(part || '').trim())
    .filter(Boolean)
    .join(' ');
}

const AUTO_ICON_RULES = [
  { icon: '𝄞', patterns: [/\bband\b/i, /\bchoir\b/i] },
  { icon: '🏀', patterns: [/\bbball\b/i, /\bbasketball\b/i] },
  { icon: '🏐', patterns: [/\bvball\b/i, /\bvolleyball\b/i] },
  { icon: '⚾', patterns: [/\bfastball\b/i, /\bbaseball\b/i] },
  { icon: '🏒', patterns: [/\bhockey\b/i, /\bskating\b/i] },
  { icon: '🥍', patterns: [/\blax\b/i, /\blacrosse\b/i] },
];

function detectFallbackIcon(title) {
  const text = String(title || '');
  for (const rule of AUTO_ICON_RULES) {
    if (rule.patterns.some((pattern) => pattern.test(text))) {
      return rule.icon;
    }
  }
  return '';
}

export function decorateEventSummary({ target, title, sourceIcon = '', sourcePrefix = '' }) {
  const resolvedIcon = String(sourceIcon || '').trim() || detectFallbackIcon(title);
  const resolvedPrefix = String(sourcePrefix || '').trim();

  // Per-child ICS feeds already say whose events they are, so they never carry a prefix.
  if (target === 'grayson' || target === 'naomi') {
    return joinParts([resolvedIcon, title]);
  }

  // The family feed and Google outputs apply the per-link prefix.
  return joinParts([resolvedPrefix, resolvedIcon, title]);
}

// Notes added in the planner go at the top of the event description, one "Note:" line each,
// so they show on calendars without touching the title.
export function addNotesToDescription(description, notes) {
  const lines = String(notes || '').split('\n').map((note) => note.trim()).filter(Boolean);
  if (!lines.length) return description || '';
  const noteText = lines.map((note) => 'Note: ' + note).join('\n');
  return description ? noteText + '\n\n' + description : noteText;
}

// A "maybe" change marks one occurrence on the calendars: a ❓ leads the title (visible in every
// app, even in narrow month cells) and the status becomes tentative for apps that style it.
export function applyMaybe({ summary, status }, maybe) {
  if (!maybe) return { summary, status };
  return {
    summary: '❓ ' + summary,
    status: status === 'cancelled' ? 'cancelled' : 'tentative',
  };
}
