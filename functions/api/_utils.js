/**
 * Shared helpers for DineScores API functions (Cloudflare Pages Functions + D1).
 */

export function jsonResponse(data, { status = 200, maxAge = 3600 } = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      // Data refreshes weekly; browsers and the edge cache (see edgeCached)
      // may keep API responses for an hour.
      'Cache-Control': `public, max-age=${maxAge}`,
      'Access-Control-Allow-Origin': '*',
    },
  });
}

/**
 * Serve a GET from Cloudflare's edge cache when possible; otherwise run the
 * handler and store its response for repeat requests.
 *
 * Pages Functions responses are NOT cached by the CDN on their own: the
 * Cache-Control header only reaches browsers and the edge reports DYNAMIC.
 * So every visitor's whole-database sync (ten 30k-row pages) and cities
 * index (a scan of every restaurant) went to D1, roughly 560k rows read per
 * visit, which is what exhausted the free plan's daily read quota. The Cache
 * API keeps the response in the datacenter that served it for the max-age
 * jsonResponse already sets (an hour; data changes weekly).
 *
 * Only successful responses with a positive max-age are stored, so a 503
 * while D1 is down or a 400 for a bad query never gets pinned for an hour.
 * Responses carry X-Edge-Cache: HIT or MISS for checking from the outside.
 */
export async function edgeCached(context, handler) {
  const { request } = context;
  if (request.method !== 'GET' || typeof caches === 'undefined') {
    return handler(context);
  }
  const cache = caches.default;
  const key = new Request(new URL(request.url).toString(), { method: 'GET' });
  const hit = await cache.match(key);
  if (hit) {
    const served = new Response(hit.body, hit);
    served.headers.set('X-Edge-Cache', 'HIT');
    return served;
  }
  let response = await handler(context);
  const cacheControl = response.headers.get('Cache-Control') || '';
  if (response.ok && /max-age=[1-9]/.test(cacheControl)) {
    response = new Response(response.body, response);
    response.headers.set('X-Edge-Cache', 'MISS');
    context.waitUntil(cache.put(key, response.clone()));
  }
  return response;
}

export function dbUnavailable() {
  return jsonResponse(
    { error: 'database_unavailable' },
    { status: 503, maxAge: 0 }
  );
}

function parseJson(text, fallback) {
  if (!text) return fallback;
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

/**
 * Shape a D1 restaurants row into the compact record format used by
 * data.js / the frontend (same field names, so components are agnostic
 * about which source a record came from).
 */
export function toCompactRecord(row) {
  return {
    i: row.id,
    n: row.name,
    a: row.address,
    c: row.city,
    s: row.state,
    z: row.zip,
    lt: row.lat,
    ln: row.lng,
    m: row.metro,
    d: row.inspection_date,
    os: row.original_score,
    rs: row.risk_score,
    ws: row.weighted_score,
    vg: row.vetted_grade,
    inf: parseJson(row.infractions, []),
    vs: parseJson(row.summaries, []),
    ic: row.inspection_count,
    src: row.source,
    url: row.source_url,
  };
}

/**
 * Lite variant for bulk transfers (whole-city loads): everything the map,
 * cards, and filters need, but WITHOUT the violation summaries — by far the
 * heaviest field. Records deliberately omit the `vs` key entirely (rather
 * than sending vs: []) so the client can tell "not loaded yet" apart from
 * "genuinely no violations" and fetch the detail record before display.
 */
export function toLiteRecord(row) {
  return {
    i: row.id,
    n: row.name,
    a: row.address,
    c: row.city,
    s: row.state,
    z: row.zip,
    lt: row.lat,
    ln: row.lng,
    m: row.metro,
    d: row.inspection_date,
    os: row.original_score,
    rs: row.risk_score,
    ws: row.weighted_score,
    vg: row.vetted_grade,
    inf: parseJson(row.infractions, []),
    ic: row.inspection_count,
    src: row.source,
    url: row.source_url,
  };
}

export const LITE_COLUMNS =
  'id,name,address,city,state,zip,lat,lng,metro,inspection_date,' +
  'original_score,risk_score,weighted_score,vetted_grade,infractions,' +
  'inspection_count,source,source_url';

export { parseJson };
