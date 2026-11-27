/**
 * The public, read-only DineScores API, version 1. Free, no key; documented
 * at /developers/ and as OpenAPI at /api/v1/openapi.json.
 *
 *   GET /api/v1                              index of endpoints
 *   GET /api/v1/restaurants/search?name=&city=&state=&limit=
 *   GET /api/v1/restaurants/near?lat=&lng=|address=&radius_miles=&grade=&since_months=&limit=
 *   GET /api/v1/restaurants/lowest?city=&state=&limit=
 *   GET /api/v1/restaurants/<id>             grade, violations, history
 *   GET /api/v1/coverage?state=
 *   GET /api/v1/openapi.json
 *
 * Every answer carries an `attribution` block (source, disclaimer, terms).
 * Rate limited per client (functions/_lib/rate_limit.js), then edge cached
 * like the rest of the API (edgeCached). The MCP server (functions/mcp.js)
 * calls handleApi directly, so its tool calls share these cache entries.
 */
import { jsonResponse, dbUnavailable, edgeCached } from '../_utils.js';
import { onRequestGet as citiesGet } from '../cities.js';
import {
  ATTRIBUTION, API_VERSION, ApiError, getRestaurant, searchRestaurants,
  restaurantsNear, lowestInCity, coverageFrom, geocodeAddress,
} from '../../_lib/public_api.js';
import { openApiSpec } from '../../_lib/openapi.js';
import { checkRateLimit, limitFor } from '../../_lib/rate_limit.js';
import { SITE } from '../../_lib/seo.js';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400',
};

export const onRequestOptions = () => new Response(null, { status: 204, headers: CORS });

export async function onRequestGet(context) {
  const limited = await checkRateLimit(context.request, context.env, {
    scope: 'api', perMinute: limitFor(context.env, 'PUBLIC_API_RATE_PER_MIN', 60),
  });
  if (limited) {
    return new Response(JSON.stringify({
      error: 'rate_limited',
      message: `Too many requests. Try again in ${limited.retryAfter} seconds.`,
    }), {
      status: 429,
      headers: { 'Content-Type': 'application/json; charset=utf-8', 'Retry-After': String(limited.retryAfter), 'Cache-Control': 'no-store', ...CORS },
    });
  }
  return handleApi(context);
}

/** Edge-cached API handler without the rate limit (the MCP server's entry). */
export const handleApi = context => edgeCached(context, route);

const ok = payload => jsonResponse({ ...payload, attribution: ATTRIBUTION });

function fail(status, code, message) {
  return jsonResponse({ error: code, message, docs: `${SITE}/developers/` }, { status, maxAge: 0 });
}

function segments(url) {
  return url.pathname.replace(/^\/api\/v1\/?/, '').split('/').filter(Boolean);
}

export async function route(context) {
  const { request, env } = context;
  const url = new URL(request.url);
  const p = url.searchParams;
  const parts = segments(url);

  try {
    if (parts.length === 0) {
      return ok({
        name: 'DineScores public API',
        version: API_VERSION,
        docs: `${SITE}/developers/`,
        openapi: `${SITE}/api/v1/openapi.json`,
        mcp: `${SITE}/mcp`,
        endpoints: {
          search: '/api/v1/restaurants/search?name=&city=&state=',
          near: '/api/v1/restaurants/near?lat=&lng= (or address=)&radius_miles=&grade=',
          lowest: '/api/v1/restaurants/lowest?city=&state=',
          restaurant: '/api/v1/restaurants/{id}',
          coverage: '/api/v1/coverage?state=',
        },
      });
    }
    if (parts.length === 1 && parts[0] === 'openapi.json') {
      return jsonResponse(openApiSpec(), { maxAge: 86400 });
    }
    if (parts.length === 1 && parts[0] === 'coverage') {
      const origin = url.origin;
      const res = await citiesGet({ ...context, request: new Request(`${origin}/api/cities`) });
      if (!res.ok) return dbUnavailable();
      return ok(coverageFrom(await res.json(), { state: p.get('state') }));
    }
    if (parts[0] !== 'restaurants' || parts.length !== 2) {
      return fail(404, 'unknown_endpoint', 'See /api/v1 for the list of endpoints.');
    }
    if (!env.DB) return dbUnavailable();
    const db = env.DB;

    switch (parts[1]) {
      case 'search':
        return ok(await searchRestaurants(db, {
          name: p.get('name') || p.get('q'), city: p.get('city'), state: p.get('state'), limit: p.get('limit'),
        }));
      case 'near': {
        let lat = p.get('lat');
        let lng = p.get('lng');
        let geocoded = null;
        if ((lat === null || lng === null) && p.get('address')) {
          geocoded = await geocodeAddress(p.get('address'));
          if (!geocoded) {
            return fail(422, 'address_not_found', 'Could not place that address. Pass lat= and lng= instead, or a fuller US street address.');
          }
          ({ lat, lng } = geocoded);
        }
        if (lat === null || lng === null) {
          return fail(400, 'missing_location', 'Pass lat= and lng=, or address=.');
        }
        const out = await restaurantsNear(db, {
          lat, lng, radiusMiles: p.get('radius_miles'), grades: p.get('grade'),
          limit: p.get('limit'), sinceMonths: parseInt(p.get('since_months'), 10) || null,
        });
        if (geocoded) out.query.matched_address = geocoded.matched_address;
        return ok(out);
      }
      case 'lowest':
        return ok(await lowestInCity(db, { city: p.get('city'), state: p.get('state'), limit: p.get('limit') }));
      default:
        return ok(await getRestaurant(db, parts[1]));
    }
  } catch (e) {
    if (e instanceof ApiError) return fail(e.status, e.code, e.message);
    console.error('api/v1 error', e && e.message);
    return fail(500, 'internal_error', 'Something went wrong. Please try again.');
  }
}
