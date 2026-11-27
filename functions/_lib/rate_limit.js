/**
 * Per-client rate limiting for the public API and the MCP server.
 *
 * Two layers, both optional to configure:
 *   - a Workers Rate Limiting binding, if one is bound as env.API_RATE_LIMITER
 *     (shared across Cloudflare's network; see docs/public-api-and-mcp.md);
 *   - otherwise a fixed one-minute window per client IP in this isolate's
 *     memory. Isolates are many and short-lived, so this is a brake on a
 *     runaway loop, not an exact quota. The edge cache already makes repeats
 *     cheap; this keeps a scraper from walking the database through misses.
 *
 * Limits per minute: PUBLIC_API_RATE_PER_MIN (default 60) for /api/v1 and
 * MCP_RATE_PER_MIN (default 300) for /mcp. The MCP default is higher because
 * ChatGPT's and Claude's connector traffic reaches us from a handful of their
 * own server addresses, shared by every one of their users.
 */

const WINDOW_MS = 60_000;
const MAX_TRACKED = 5000;
const windows = new Map(); // key -> { start, count }

export function clientKey(request) {
  return request.headers.get('cf-connecting-ip')
    || (request.headers.get('x-forwarded-for') || '').split(',')[0].trim()
    || 'unknown';
}

/** Returns null when the request may proceed, else { retryAfter } seconds. */
export async function checkRateLimit(request, env, { scope, perMinute, now = Date.now() }) {
  const key = `${scope}:${clientKey(request)}`;
  if (env && env.API_RATE_LIMITER && typeof env.API_RATE_LIMITER.limit === 'function') {
    try {
      const { success } = await env.API_RATE_LIMITER.limit({ key });
      return success ? null : { retryAfter: 60 };
    } catch {
      // A binding failure must never take the API down; fall through.
    }
  }
  let w = windows.get(key);
  if (!w || now - w.start >= WINDOW_MS) {
    if (windows.size >= MAX_TRACKED) {
      for (const [k, v] of windows) if (now - v.start >= WINDOW_MS) windows.delete(k);
      if (windows.size >= MAX_TRACKED) windows.clear();
    }
    w = { start: now, count: 0 };
    windows.set(key, w);
  }
  w.count += 1;
  if (w.count > perMinute) {
    return { retryAfter: Math.max(1, Math.ceil((w.start + WINDOW_MS - now) / 1000)) };
  }
  return null;
}

export function limitFor(env, name, fallback) {
  const n = parseInt(env && env[name], 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

/** Test hook. */
export function resetRateLimits() {
  windows.clear();
}
