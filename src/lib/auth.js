import { ADMIN_ROLES } from './constants.js';

const JWKS_CACHE_TTL_MS = 5 * 60 * 1000;
const jwtVerificationCache = new WeakMap();
const jwksCache = new Map();

function allowsDevRoleHeader(env) {
  return String(env.ALLOW_DEV_ROLE_HEADER || '').toLowerCase() === 'true';
}

function jsonResponse(status, message) {
  return new Response(
    JSON.stringify({
      error: status === 401 ? 'Unauthorized' : 'Forbidden',
      message,
    }),
    {
      status,
      headers: { 'content-type': 'application/json; charset=utf-8' },
    }
  );
}

function fromBase64Url(input) {
  const normalized = input.replace(/-/g, '+').replace(/_/g, '/');
  const padLength = (4 - (normalized.length % 4)) % 4;
  return `${normalized}${'='.repeat(padLength)}`;
}

function decodeBase64UrlToString(input) {
  const binary = atob(fromBase64Url(input));
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

function parseEmailSet(raw) {
  return new Set(
    String(raw || '')
      .split(',')
      .map((value) => value.trim().toLowerCase())
      .filter(Boolean)
  );
}

function normalizeTeamDomain(raw) {
  const value = String(raw || '').trim();
  if (!value) return '';
  try {
    return new URL(value).origin;
  } catch {
    return '';
  }
}

function toDerLength(length) {
  if (length < 0x80) return [length];
  if (length <= 0xff) return [0x81, length];
  if (length <= 0xffff) return [0x82, (length >> 8) & 0xff, length & 0xff];
  return null;
}

function derSequence(content) {
  const length = toDerLength(content.length);
  if (!length) return null;
  return new Uint8Array([0x30, ...length, ...content]);
}

function derInteger(bytes) {
  const needsLeadingZero = bytes[0] >= 0x80;
  const body = needsLeadingZero ? new Uint8Array([0x00, ...bytes]) : bytes;
  const length = toDerLength(body.length);
  if (!length) return null;
  return new Uint8Array([0x02, ...length, ...body]);
}

function derBitString(bytes) {
  const body = new Uint8Array([0x00, ...bytes]);
  const length = toDerLength(body.length);
  if (!length) return null;
  return new Uint8Array([0x03, ...length, ...body]);
}

function jwkToSpkiBuffer(jwk) {
  if (!jwk?.n || !jwk?.e) return null;
  const n = Uint8Array.from(atob(fromBase64Url(jwk.n)), (char) => char.charCodeAt(0));
  const e = Uint8Array.from(atob(fromBase64Url(jwk.e)), (char) => char.charCodeAt(0));

  const modulus = derInteger(n);
  const exponent = derInteger(e);
  if (!modulus || !exponent) return null;

  const rsaPublicKey = derSequence(new Uint8Array([...modulus, ...exponent]));
  if (!rsaPublicKey) return null;

  const algorithmIdentifier = new Uint8Array([
    0x30,
    0x0d,
    0x06,
    0x09,
    0x2a,
    0x86,
    0x48,
    0x86,
    0xf7,
    0x0d,
    0x01,
    0x01,
    0x01,
    0x05,
    0x00,
  ]);

  const subjectPublicKey = derBitString(rsaPublicKey);
  if (!subjectPublicKey) return null;

  const spki = derSequence(new Uint8Array([...algorithmIdentifier, ...subjectPublicKey]));
  return spki ? spki.buffer : null;
}

async function importVerificationKey(jwk) {
  const spkiBuffer = jwkToSpkiBuffer(jwk);
  if (!spkiBuffer) return null;
  return crypto.subtle.importKey('spki', spkiBuffer, { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['verify']);
}

async function fetchJwks(teamDomain, { forceRefresh = false } = {}) {
  const now = Date.now();
  const cached = jwksCache.get(teamDomain);
  if (!forceRefresh && cached && cached.expiresAt > now) return cached.keysByKid;

  const response = await fetch(`${teamDomain}/cdn-cgi/access/certs`);
  if (!response.ok) {
    throw new Error(`Unable to fetch Cloudflare Access JWKS (${response.status})`);
  }

  const body = await response.json();
  const keys = Array.isArray(body?.keys) ? body.keys : [];
  const keysByKid = new Map();

  for (const key of keys) {
    if (!key?.kid || key.kty !== 'RSA') continue;
    const cryptoKey = await importVerificationKey(key);
    if (cryptoKey) {
      keysByKid.set(key.kid, cryptoKey);
    }
  }

  jwksCache.set(teamDomain, {
    keysByKid,
    expiresAt: now + JWKS_CACHE_TTL_MS,
  });

  return keysByKid;
}

async function verifyAccessJwt(token, teamDomain, expectedAud) {
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('Invalid JWT format');

  const [encodedHeader, encodedPayload, encodedSignature] = parts;
  const header = JSON.parse(decodeBase64UrlToString(encodedHeader));
  const payload = JSON.parse(decodeBase64UrlToString(encodedPayload));
  const message = new TextEncoder().encode(`${encodedHeader}.${encodedPayload}`);
  const signature = Uint8Array.from(atob(fromBase64Url(encodedSignature)), (char) => char.charCodeAt(0));

  if (header.alg !== 'RS256' || !header.kid) {
    throw new Error('Unsupported JWT header');
  }

  const audValues = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
  if (!audValues.includes(expectedAud)) {
    throw new Error('Invalid JWT audience');
  }

  if (payload.iss !== teamDomain) {
    throw new Error('Invalid JWT issuer');
  }

  const nowSeconds = Math.floor(Date.now() / 1000);
  if (typeof payload.exp === 'number' && nowSeconds >= payload.exp) {
    throw new Error('JWT has expired');
  }
  if (typeof payload.nbf === 'number' && nowSeconds < payload.nbf) {
    throw new Error('JWT not active yet');
  }

  const verifyWithKeys = async (keysByKid) => {
    const key = keysByKid.get(header.kid);
    if (!key) return false;
    return crypto.subtle.verify({ name: 'RSASSA-PKCS1-v1_5' }, key, signature, message);
  };

  let keysByKid = await fetchJwks(teamDomain);
  let valid = await verifyWithKeys(keysByKid);
  if (!valid) {
    keysByKid = await fetchJwks(teamDomain, { forceRefresh: true });
    valid = await verifyWithKeys(keysByKid);
  }

  if (!valid) {
    throw new Error('JWT signature verification failed');
  }

  const email = String(payload.email || '').trim().toLowerCase();
  if (!email) {
    throw new Error('JWT email claim is required');
  }

  return { email };
}

async function getAuthContextFromRequest(request, env) {
  if (jwtVerificationCache.has(request)) {
    return jwtVerificationCache.get(request);
  }

  const promise = (async () => {
    if (allowsDevRoleHeader(env)) {
      const role = request.headers.get('x-user-role')?.trim().toLowerCase();
      if (ADMIN_ROLES.includes(role)) return { role, source: 'dev-header' };
    }

    const teamDomain = normalizeTeamDomain(env.CLOUDFLARE_ACCESS_TEAM_DOMAIN);
    const expectedAud = String(env.CLOUDFLARE_ACCESS_AUD || '').trim();
    const jwt = request.headers.get('Cf-Access-Jwt-Assertion')?.trim() || '';

    if (!teamDomain || !expectedAud || !jwt) return null;

    const { email } = await verifyAccessJwt(jwt, teamDomain, expectedAud);
    const adminEmails = parseEmailSet(env.ADMIN_EMAILS);
    const editorEmails = parseEmailSet(env.EDITOR_EMAILS);

    if (adminEmails.has(email)) return { role: 'admin', email, source: 'access-jwt' };
    if (editorEmails.has(email)) return { role: 'editor', email, source: 'access-jwt' };
    return { role: null, email, source: 'access-jwt' };
  })();

  jwtVerificationCache.set(request, promise);
  return promise;
}

export async function getRoleFromRequest(request, env) {
  const auth = await getAuthContextFromRequest(request, env);
  if (!auth?.role || !ADMIN_ROLES.includes(auth.role)) return null;
  return auth.role;
}

export async function requireRole(request, env, allowedRoles) {
  try {
    const role = await getRoleFromRequest(request, env);
    if (!role || !allowedRoles.includes(role)) {
      return jsonResponse(
        403,
        allowsDevRoleHeader(env)
          ? 'This route requires Google-authenticated access. Use x-user-role for local development only.'
          : 'This route requires Google-authenticated access.'
      );
    }
    return null;
  } catch {
    return jsonResponse(401, 'Invalid Cloudflare Access token.');
  }
}

export function clearAuthCachesForTest() {
  jwksCache.clear();
}
