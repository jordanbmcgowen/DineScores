/**
 * GET /api/restaurants — query the D1 restaurants table.
 *
 * Query params (at least one of city/bbox/q required):
 *   city=Dallas               restaurants in a city (indexed)
 *   bbox=west,south,east,north  restaurants inside a map viewport
 *   q=chipotle                name search (optionally scoped by city/bbox)
 *   grade=F                   filter by vetted grade
 *   limit=2000                max rows (default 2000, cap 5000)
 *
 * Returns compact records with the same field names as data.js.
 */
import { jsonResponse, dbUnavailable, toCompactRecord } from './_utils.js';

const MAX_LIMIT = 5000;

export async function onRequestGet({ request, env }) {
  if (!env.DB) return dbUnavailable();

  const url = new URL(request.url);
  const city = url.searchParams.get('city');
  const bbox = url.searchParams.get('bbox');
  const q = url.searchParams.get('q');
  const grade = url.searchParams.get('grade');
  const limit = Math.min(
    parseInt(url.searchParams.get('limit') || '2000', 10) || 2000,
    MAX_LIMIT
  );

  const where = [];
  const binds = [];

  if (city) {
    where.push('city = ?');
    binds.push(city);
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
      { error: 'missing_filter', hint: 'pass city=, bbox=, or q=' },
      { status: 400, maxAge: 0 }
    );
  }

  const sql =
    'SELECT * FROM restaurants WHERE ' +
    where.join(' AND ') +
    ' ORDER BY inspection_date DESC LIMIT ?';
  binds.push(limit);

  const { results } = await env.DB.prepare(sql).bind(...binds).all();
  return jsonResponse(results.map(toCompactRecord));
}
