import { ADMIN_ROLES } from './constants.js';

function allowsDevRoleHeader(env) {
  return String(env.ALLOW_DEV_ROLE_HEADER || '').toLowerCase() === 'true';
}

export function getRoleFromRequest(request, env) {
  if (!allowsDevRoleHeader(env)) return null;
  const role = request.headers.get('x-user-role')?.trim().toLowerCase();
  if (ADMIN_ROLES.includes(role)) return role;
  return null;
}

export function requireRole(request, env, allowedRoles) {
  const role = getRoleFromRequest(request, env);
  if (!role || !allowedRoles.includes(role)) {
    return new Response(
      JSON.stringify({
        error: 'Forbidden',
        message: allowsDevRoleHeader(env)
          ? 'This route requires Google-authenticated access. Use x-user-role for local development only.'
          : 'This route requires Google-authenticated access.',
      }),
      {
        status: 403,
        headers: { 'content-type': 'application/json; charset=utf-8' },
      }
    );
  }
  return null;
}
