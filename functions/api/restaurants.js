/**
 * GET /api/restaurants — query the D1 restaurants table.
 *
 * Query params (at least one of city/metro/bbox/q/cursor required):
 *   city=Dallas                 restaurants in a city (indexed)
 *   metro=DFW                   restaurants in a metro area
 *   bbox=west,south,east,north  restaurants inside a map viewport
 *   q=chipotle                  name search (optionally scoped by city/bbox)
 *   grade=F                     filter by vetted grade
 *   fields=lite                 omit violation summaries (bulk transfers —
 *                               whole-city loads; higher row limit applies)
 *   cursor=<id>                 full-database sync page (lite only): keyset
 *                               pagination ordered by id; pass '' for the
 *                               first page, then the last id of each page.
 *                               Deterministic URLs, so the CDN caches the
 *                               whole chain between weekly refreshes.
 *   limit=2000                  max rows (default 2000; cap 5000 full,
 *                               30000 lite)
 *
 * Returns compact records with the same field names as data.js.
 */
import {
  jsonResponse, dbUnavailable, toCompactRecord, toLiteRecord, LITE_COLUMNS,
} from './_utils.js';

const MAX_LIMIT_FULL = 5000;
const MAX_LIMIT_LITE = 30000;

export async function onRequestGet({ request, env }) {
  if (!env.DB) return dbUnavailable();

  const url = new URL(request.url);
  const city = url.searchParams.get('city');
  const metro = url.searchParams.get('metro');
  const bbox = url.searchParams.get('bbox');
  const q = url.searchParams.get('q');
  const grade = url.searchParams.get('grade');
  const cursor = url.searchParams.get('cursor'); // null = absent, '' = first page
  const lite = url.searchParams.get('fields') === 'lite';
  const maxLimit = lite ? MAX_LIMIT_LITE : MAX_LIMIT_FULL;
  const limit = Math.min(
    parseInt(url.searchParams.get('limit') || '2000', 10) || 2000,
    maxLimit
  );

  // Full-database sync: lite-only keyset pagination over the id primary key
  // (stable order, no OFFSET scans). Used by the client to converge the map
  // to the complete dataset right after first paint.
  if (cursor !== null) {
    if (!lite) {
      return jsonResponse(
        { error: 'cursor_requires_lite', hint: 'pass fields=lite' },
        { status: 400, maxAge: 0 }
      );
    }
    const { results } = await env.DB.prepare(
      `SELECT ${LITE_COLUMNS} FROM restaurants WHERE id > ? ORDER BY id LIMIT ?`
    ).bind(cursor, limit).all();
    return jsonResponse(results.map(toLiteRecord));
  }

  const where = [];
  const binds = [];

  if (city) {
    where.push('city = ?');
    binds.push(city);
  }
  if (metro) {
    where.push('metro = ?');
    binds.push(metro);
  }
  if (bbox) {
    const parts = bbox.split(',').map(Number);
    if (parts.length !== 4 || parts.some(Number.isNaN)) {
      return jsonResponse({ error: 'invalid_bbox' }, { status: 400, maxAge: 0 });
    }
    const [west, south, east, north] = parts;
    where.push('lat BETWEEN ? AND ? AND lng BETWEEN ? AND ?');
    binds.push(south, north, west, east);
  }
  if (q) {
    if (q.length < 2) {
      return jsonResponse({ error: 'query_too_short' }, { status: 400, maxAge: 0 });
    }
    where.push('name LIKE ?');
    binds.push(`%${q}%`);
  }
  if (grade) {
    where.push('vetted_grade = ?');
    binds.push(grade);
  }
  if (where.length === 0) {
    return jsonResponse(
      { error: 'missing_filter', hint: 'pass city=, metro=, bbox=, or q=' },
      { status: 400, maxAge: 0 }
    );
  }

  const columns = lite ? LITE_COLUMNS : '*';
  const sql =
    `SELECT ${columns} FROM restaurants WHERE ` +
    where.join(' AND ') +
    ' ORDER BY inspection_date DESC LIMIT ?';
  binds.push(limit);

  const { results } = await env.DB.prepare(sql).bind(...binds).all();
  return jsonResponse(results.map(lite ? toLiteRecord : toCompactRecord));
}
