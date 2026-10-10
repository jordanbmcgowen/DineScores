/**
 * GET/POST /api/relay?url=<https://portal/...> — an egress hop for the two
 * inspection portals that answer 403 to every GitHub Actions runner range
 * (inspections.myhealthdepartment.com, and the Southern Nevada Health
 * District API, which 401s runners after a few dozen pages). The weekly
 * refresh (.github/workflows/refresh-data.yml) sends those requests here and
 * this function fetches them from Cloudflare's network, passing the portal's
 * own status and body straight back so the pipeline's 403/429 retry ladders,
 * its circuit breaker and its detail-ban cooldowns behave exactly as they do
 * on a direct request.
 *
 * Who may call it: a GitHub Actions job of THIS repository, proven by a
 * GitHub-signed OIDC token (header X-Relay-Token). The job mints the token
 * itself (`id-token: write`), so nothing has to be configured anywhere: no
 * repository secret, no Pages variable. Optionally a static RELAY_TOKEN
 * Pages variable is accepted too, for runs outside Actions. Only the two
 * portal hostnames can be fetched; everything else is refused, so a leaked
 * URL is not an open proxy.
 *
 * Deploys with the site like every other Pages Function. Not wrapped in
 * edgeCached on purpose: every call must reach the portal.
 */

const ALLOWED_HOSTS = new Set([
  'inspections.myhealthdepartment.com',
  'www.southernnevadahealthdistrict.org',
]);

// The GitHub Actions OIDC issuer and the audience the pipeline asks for
// (RELAY_OIDC_AUDIENCE in dinescores_pipeline.py — change both together).
const GITHUB_ISSUER = 'https://token.actions.githubusercontent.com';
const GITHUB_JWKS_URL = `${GITHUB_ISSUER}/.well-known/jwks`;
const AUDIENCE = 'dinescores-portal-relay';
// The repository whose workflows may use the relay. Forks mint tokens with
// their own `repository` claim, so this is what keeps the hop private.
const DEFAULT_ALLOWED_REPO = 'jordanbmcgowen/DineScores';

// Hop-by-hop and Cloudflare-added headers that must not be replayed upstream.
const STRIP_REQUEST_HEADERS = [
  'x-relay-token', 'host', 'cf-connecting-ip', 'cf-ipcountry', 'cf-ray',
  'cf-visitor', 'cf-worker', 'x-forwarded-for', 'x-forwarded-proto',
  'x-real-ip', 'connection', 'keep-alive', 'transfer-encoding', 'upgrade',
  'accept-encoding', 'cf-ew-via', 'cdn-loop',
];

const deny = (status, message) =>
  new Response(`${message}\n`, { status, headers: { 'content-type': 'text/plain', 'cache-control': 'no-store' } });

let jwksCache = { keys: null, fetchedAt: 0 };

async function githubKeys() {
  if (jwksCache.keys && Date.now() - jwksCache.fetchedAt < 6 * 3600 * 1000) return jwksCache.keys;
  const r = await fetch(GITHUB_JWKS_URL, { cf: { cacheTtl: 3600 } });
  if (!r.ok) throw new Error(`jwks ${r.status}`);
  const { keys } = await r.json();
  jwksCache = { keys, fetchedAt: Date.now() };
  return keys;
}

function b64urlDecode(s) {
  s = s.replace(/-/g, '+').replace(/_/g, '/');
  s += '='.repeat((4 - (s.length % 4)) % 4);
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

/**
 * Verify a GitHub Actions OIDC token: RS256 signature against GitHub's
 * published keys, issuer, audience, validity window, and the repository
 * claim. Returns the payload or throws.
 */
async function verifyGithubToken(token, allowedRepo) {
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('malformed token');
  const header = JSON.parse(new TextDecoder().decode(b64urlDecode(parts[0])));
  const payload = JSON.parse(new TextDecoder().decode(b64urlDecode(parts[1])));
  if (header.alg !== 'RS256') throw new Error('unexpected alg');

  let keys = await githubKeys();
  let jwk = keys.find(k => k.kid === header.kid);
  if (!jwk) {
    // Key rotation: refetch once before giving up.
    jwksCache = { keys: null, fetchedAt: 0 };
    keys = await githubKeys();
    jwk = keys.find(k => k.kid === header.kid);
    if (!jwk) throw new Error('unknown kid');
  }
  const key = await crypto.subtle.importKey(
    'jwk', jwk, { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['verify']);
  const ok = await crypto.subtle.verify(
    'RSASSA-PKCS1-v1_5', key, b64urlDecode(parts[2]),
    new TextEncoder().encode(`${parts[0]}.${parts[1]}`));
  if (!ok) throw new Error('bad signature');

  const now = Math.floor(Date.now() / 1000);
  if (payload.iss !== GITHUB_ISSUER) throw new Error('bad issuer');
  const aud = Array.isArray(payload.aud) ? payload.aud : [payload.aud];
  if (!aud.includes(AUDIENCE)) throw new Error('bad audience');
  if (typeof payload.exp !== 'number' || payload.exp < now - 60) throw new Error('expired');
  if (typeof payload.nbf === 'number' && payload.nbf > now + 60) throw new Error('not yet valid');
  if ((payload.repository || '').toLowerCase() !== allowedRepo.toLowerCase()) {
    throw new Error('wrong repository');
  }
  return payload;
}

async function authorize(request, env) {
  const presented = request.headers.get('x-relay-token') || '';
  if (!presented) return 'missing X-Relay-Token';
  if (env.RELAY_TOKEN && presented === env.RELAY_TOKEN) return null;
  try {
    await verifyGithubToken(presented, env.RELAY_ALLOWED_REPO || DEFAULT_ALLOWED_REPO);
    return null;
  } catch (e) {
    return `token rejected: ${e.message}`;
  }
}

export async function onRequest({ request, env }) {
  const denied = await authorize(request, env);
  if (denied) return deny(403, denied);

  const raw = new URL(request.url).searchParams.get('url');
  if (!raw) return deny(400, 'missing url parameter');
  let target;
  try {
    target = new URL(raw);
  } catch {
    return deny(400, 'malformed url parameter');
  }
  if (target.protocol !== 'https:') return deny(400, 'only https targets are allowed');
  if (!ALLOWED_HOSTS.has(target.hostname)) return deny(403, `host not allowed: ${target.hostname}`);

  const headers = new Headers(request.headers);
  for (const h of STRIP_REQUEST_HEADERS) headers.delete(h);

  const hasBody = !['GET', 'HEAD'].includes(request.method);
  let upstream;
  try {
    upstream = await fetch(target.toString(), {
      method: request.method,
      headers,
      body: hasBody ? await request.arrayBuffer() : undefined,
      redirect: 'follow',
    });
  } catch (e) {
    // 502 rather than a thrown error: the caller treats it as a failed
    // fetch and retries, which is the correct handling either way.
    return deny(502, `upstream fetch failed: ${e}`);
  }

  // Pass the portal's status through untouched. Cookies are dropped so one
  // caller's session never leaks to the next; the body is re-framed by the
  // runtime, so the upstream's encoding/length headers must go too.
  const out = new Headers(upstream.headers);
  for (const h of ['set-cookie', 'transfer-encoding', 'content-encoding', 'content-length']) out.delete(h);
  out.set('cache-control', 'no-store');
  out.set('x-relay-upstream-status', String(upstream.status));
  return new Response(upstream.body, { status: upstream.status, headers: out });
}
