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

  if (target === 'family') {
    return joinParts([resolvedPrefix, resolvedIcon, title]);
  }

  if (target === 'grayson' || target === 'naomi') {
    return joinParts([resolvedIcon, title]);
  }

  return joinParts([resolvedIcon, title]);
}
