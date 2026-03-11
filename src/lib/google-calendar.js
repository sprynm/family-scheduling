
const GOOGLE_RATE_LIMIT_MESSAGE = /rate limit|quota/i;
const GOOGLE_RETRYABLE_STATUS = new Set([403, 429]);

function base64UrlEncode(value) {
  return Buffer.from(value)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function pemToArrayBuffer(pem) {
  const normalized = String(pem || '')
    .replace(/-----BEGIN PRIVATE KEY-----/g, '')
    .replace(/-----END PRIVATE KEY-----/g, '')
    .replace(/\s+/g, '');
  return Uint8Array.from(Buffer.from(normalized, 'base64')).buffer;
}

async function signJwtAssertion(serviceAccount, scope) {
  const issuedAt = Math.floor(Date.now() / 1000);
  const header = { alg: 'RS256', typ: 'JWT' };
  const payload = {
    iss: serviceAccount.client_email,
    scope,
    aud: serviceAccount.token_uri,
    exp: issuedAt + 3600,
    iat: issuedAt,
  };
  const encodedHeader = base64UrlEncode(JSON.stringify(header));
  const encodedPayload = base64UrlEncode(JSON.stringify(payload));
  const signingInput = `${encodedHeader}.${encodedPayload}`;
  const key = await crypto.subtle.importKey(
    'pkcs8',
    pemToArrayBuffer(serviceAccount.private_key),
    {
      name: 'RSASSA-PKCS1-v1_5',
      hash: 'SHA-256',
    },
    false,
    ['sign']
  );
  const signature = await crypto.subtle.sign(
    { name: 'RSASSA-PKCS1-v1_5' },
    key,
    new TextEncoder().encode(signingInput)
  );
  return `${signingInput}.${base64UrlEncode(Buffer.from(signature))}`;
}

export function parseServiceAccount(jsonValue) {
  const parsed = JSON.parse(String(jsonValue || '{}'));
  if (!parsed.client_email || !parsed.private_key || !parsed.token_uri) {
    throw new Error('GOOGLE_SERVICE_ACCOUNT_JSON is missing required service account fields');
  }
  return parsed;
}

export async function getGoogleAccessToken(serviceAccountJson) {
  const serviceAccount = parseServiceAccount(serviceAccountJson);
  const assertion = await signJwtAssertion(serviceAccount, 'https://www.googleapis.com/auth/calendar');
  const response = await fetch(serviceAccount.token_uri, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded',
    },
    body: new URLSearchParams({
      grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
      assertion,
    }).toString(),
  });
  if (!response.ok) {
    throw new Error(`Google token request failed: HTTP ${response.status}`);
  }
  const data = await response.json();
  if (!data?.access_token) {
    throw new Error('Google token response did not include access_token');
  }
  return data.access_token;
}

async function parseGoogleResponse(response) {
  const text = await response.text();
  let parsed = null;
  if (text) {
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = null;
    }
  }
  if (!response.ok) {
    const message = parsed?.error?.message || parsed?.error_description || `Google Calendar API failed: HTTP ${response.status}`;
    const error = new Error(message);
    error.status = response.status;
    error.retryAfterSeconds = Number.parseInt(response.headers.get('retry-after') || '', 10) || 0;
    error.googleRateLimited = GOOGLE_RETRYABLE_STATUS.has(response.status) && GOOGLE_RATE_LIMIT_MESSAGE.test(message);
    throw error;
  }
  return parsed;
}

export function isGoogleRateLimitError(error) {
  return Boolean(error && typeof error === 'object' && error.googleRateLimited);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchGoogleCalendarWithRetry(url, options, { allowDeleteMissing = false } = {}) {
  const maxAttempts = 3;
  let attempt = 0;
  while (true) {
    attempt += 1;
    const response = await fetch(url, options);
    if (allowDeleteMissing && response.status === 404) {
      return { deleted: true, missing: true };
    }
    try {
      return await parseGoogleResponse(response);
    } catch (error) {
      if (!isGoogleRateLimitError(error) || attempt >= maxAttempts) {
        throw error;
      }
      const retryAfterMs = Math.max(0, Number(error.retryAfterSeconds || 0) * 1000);
      const backoffMs = retryAfterMs || 500 * 2 ** (attempt - 1);
      await sleep(backoffMs);
    }
  }
}

export async function createGoogleCalendarEvent({ accessToken, calendarId, event }) {
  return fetchGoogleCalendarWithRetry(
    `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events`,
    {
      method: 'POST',
      headers: {
        authorization: `Bearer ${accessToken}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify(event),
    }
  );
}

export async function updateGoogleCalendarEvent({ accessToken, calendarId, eventId, event }) {
  return fetchGoogleCalendarWithRetry(
    `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`,
    {
      method: 'PUT',
      headers: {
        authorization: `Bearer ${accessToken}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify(event),
    }
  );
}

export async function deleteGoogleCalendarEvent({ accessToken, calendarId, eventId }) {
  const result = await fetchGoogleCalendarWithRetry(
    `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events/${encodeURIComponent(eventId)}`,
    {
      method: 'DELETE',
      headers: {
        authorization: `Bearer ${accessToken}`,
      },
    },
    { allowDeleteMissing: true }
  );
  return result?.deleted ? result : { deleted: true, missing: false };
}
