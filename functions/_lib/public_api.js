/**
 * The public, read-only DineScores API (/api/v1/*) and the logic the MCP
 * server (/mcp) calls. One place for the queries and the public record shape,
 * so a REST call and an MCP tool call answer the same question the same way
 * and share the same edge-cache entries.
 *
 * Exports no onRequest handler, so Pages routes nothing here. Plain ES with no
 * Node APIs: tests/test_public_api.mjs runs it under Node with a fake D1.
 *
 * D1 reads are metered (CLAUDE.md), so every query is bounded: a city filter
 * uses idx_restaurants_city, a location search a small lat/lng box on
 * idx_restaurants_geo, an id the primary key. A name search with no city is a
 * table scan and is capped at 25 rows; the edge cache and the rate limiter
 * keep repeats cheap.
 */
import { SITE, STATE_NAMES, displayName, restaurantPath } from './seo.js';
import { findingTitle, severityLabel } from '../../src/findingText.js';
import { parseJson } from '../api/_utils.js';

export const API_VERSION = '1.0.0';

export const ATTRIBUTION = {
  source: 'DineScores (https://dinescores.com), compiled from official city, county and state health department inspection records.',
  cite_as: 'DineScores, dinescores.com',
  disclaimer: 'Grades are DineScores\' summary of what inspectors recorded on the days they visited. They are not a health department\'s own rating, a review of the food, or a guarantee. Scoring scales differ between departments. Check official_report_url for the full record. DineScores is not affiliated with any health department.',
  how_grades_work: `${SITE}/about/`,
  terms: `${SITE}/developers/#terms`,
};

export const GRADE_MEANINGS = {
  A: 'A (90-100): few or minor violations',
  B: 'B (80-89): some violations worth a look',
  C: 'C (70-79): several or serious violations',
  F: 'F (below 70): many serious violations, active pests or sewage, or a closure',
};

const GRADES = ['A', 'B', 'C', 'F'];
const MILES_PER_DEG_LAT = 69.0;
export const LIMITS = {
  search: { def: 10, max: 25 },
  near: { def: 20, max: 50 },
  lowest: { def: 20, max: 50 },
  radiusMiles: { def: 1, max: 10 },
  history: 20,
  violations: 25,
};
const NEAR_SCAN_ROWS = 2000;     // rows pulled from the box before distance sort
const LOW_SCORE_MONTHS = 18;     // "lowest scores" counts recent inspections only

const LIST_COLUMNS =
  'id,name,address,city,state,zip,lat,lng,inspection_date,original_score,' +
  'risk_score,weighted_score,vetted_grade,infractions,inspection_count,' +
  'source,source_url';

export class ApiError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

const bad = (code, message) => new ApiError(400, code, message);

// ---------------------------------------------------------------- inputs

export function clampInt(value, { def, max, min = 1 }) {
  const n = parseInt(value, 10);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, n));
}

/** 'TX', 'tx', 'Texas' -> 'TX'; '' -> ''; anything else -> null. */
export function normalizeState(value) {
  const s = String(value || '').trim();
  if (!s) return '';
  const up = s.toUpperCase();
  if (STATE_NAMES[up]) return up;
  const hit = Object.entries(STATE_NAMES).find(([, name]) => name.toLowerCase() === s.toLowerCase());
  return hit ? hit[0] : null;
}

/**
 * The spellings a stored city may have. The pipeline labels cities with
 * Python's str.title() ("Mckinney", "Lee'S Summit"), so try that, a plain
 * capitalize-each-word, and the text as typed: an IN over the three keeps the
 * city index usable (a NOCASE compare would not).
 */
export function cityVariants(value) {
  const raw = String(value || '').trim().replace(/\s+/g, ' ').replace(/[’‘]/g, "'");
  if (!raw) return [];
  const lower = raw.toLowerCase();
  const pyTitle = lower.replace(/(^|[^a-z])([a-z])/g, (_, pre, ch) => pre + ch.toUpperCase());
  const words = lower.replace(/(^|[\s\-/.])([a-z])/g, (_, pre, ch) => pre + ch.toUpperCase());
  return [...new Set([pyTitle, words, raw])];
}

/**
 * Name words for a LIKE-per-word match ("pizza joe's" and "joes pizza" both
 * find "Joe'S Pizza"). Apostrophes are dropped on both sides (NAME_EXPR).
 */
export function nameTokens(value) {
  return String(value || '')
    .replace(/[’‘]/g, "'")
    .toLowerCase()
    .replace(/'/g, '')
    .split(/[^a-z0-9&]+/)
    .filter(t => t.length >= 2)
    .slice(0, 6);
}

const NAME_EXPR = "replace(lower(name), '''', '')";
const likeEscape = s => s.replace(/[\\%_]/g, c => `\\${c}`);

function parseGrades(value) {
  if (!value) return [];
  const list = String(value).toUpperCase().split(/[\s,]+/).filter(Boolean);
  const bad_ = list.filter(g => !GRADES.includes(g));
  if (bad_.length) throw bad('invalid_grade', 'grade must be one or more of A, B, C, F');
  return [...new Set(list)];
}

function parseCoord(value, lo, hi, name) {
  const n = Number(value);
  if (value === null || value === undefined || value === '' || !Number.isFinite(n) || n < lo || n > hi) {
    throw bad('invalid_location', `${name} must be a number between ${lo} and ${hi}`);
  }
  return n;
}

// ---------------------------------------------------------------- shapes

export function distanceMiles(lat1, lng1, lat2, lng2) {
  const toRad = d => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return 3958.8 * 2 * Math.asin(Math.min(1, Math.sqrt(a)));
}

/** One D1 restaurants row as a self-explanatory public record. */
export function publicRestaurant(row, { history = null, distance = null } = {}) {
  const grade = GRADES.includes(row.vetted_grade) ? row.vetted_grade : null;
  const score = Number.isFinite(row.weighted_score) ? row.weighted_score
    : Number.isFinite(row.risk_score) ? row.risk_score : null;
  const path = restaurantPath(row.id, row.name);
  const out = {
    id: row.id,
    name: displayName(row.name),
    address: row.address || null,
    city: row.city || null,
    state: row.state || null,
    zip: row.zip || null,
    latitude: Number.isFinite(row.lat) ? row.lat : null,
    longitude: Number.isFinite(row.lng) ? row.lng : null,
    grade,
    grade_meaning: grade ? GRADE_MEANINGS[grade] : 'Not yet graded',
    score,
    latest_inspection_date: row.inspection_date || null,
    latest_inspection_score: Number.isFinite(row.risk_score) ? row.risk_score : null,
    official_score: Number.isFinite(row.original_score) ? row.original_score : null,
    inspections_on_file: row.inspection_count || 1,
    issue_types: parseJson(row.infractions, []),
    inspected_by: row.source || null,
    official_report_url: row.source_url || null,
    dinescores_url: SITE + path,
    map_url: `${SITE}/#r/${row.id}`,
  };
  if (grade === 'F' && Number.isFinite(score) && score >= 70) {
    out.grade_note = 'Graded F despite the score because the report notes pests, sewage, a closure or a failed inspection.';
  }
  if (distance !== null) out.distance_miles = Math.round(distance * 100) / 100;
  if (history) {
    out.violations = parseJson(row.summaries, []).slice(0, LIMITS.violations).map(v => ({
      severity: severityLabel(v.severity),
      finding: findingTitle(v),
      inspector_notes: v.verbatim || v.text || '',
    }));
    out.inspection_history = history.map(h => ({
      date: h.inspection_date,
      score: Number.isFinite(h.risk_score) ? h.risk_score : null,
      type: h.inspection_type || null,
      result: h.results || null,
      report_url: h.source_url || null,
    }));
  }
  return out;
}

// ---------------------------------------------------------------- queries

export async function getRestaurant(db, id) {
  const clean = String(id || '').trim().toLowerCase().match(/([a-f0-9]{16})$/);
  if (!clean) throw bad('invalid_id', 'id is the 16-character DineScores restaurant id');
  const row = await db.prepare('SELECT * FROM restaurants WHERE id = ?').bind(clean[1]).first();
  if (!row) throw new ApiError(404, 'not_found', 'No restaurant with that id');
  const { results } = await db.prepare(
    `SELECT inspection_date, risk_score, inspection_type, results, source_url
       FROM inspections WHERE restaurant_id = ?
      ORDER BY inspection_date DESC LIMIT ${LIMITS.history}`
  ).bind(row.id).all();
  return { restaurant: publicRestaurant(row, { history: results || [] }) };
}

export async function searchRestaurants(db, { name, city, state, limit }) {
  const tokens = nameTokens(name);
  if (!tokens.length) throw bad('missing_name', 'name needs at least two letters');
  const st = normalizeState(state);
  if (st === null) throw bad('invalid_state', 'state must be a US state name or two-letter code');
  const cities = cityVariants(city);
  const n = clampInt(limit, LIMITS.search);

  const where = [];
  const binds = [];
  for (const t of tokens) {
    where.push(`${NAME_EXPR} LIKE ? ESCAPE '\\'`);
    binds.push(`%${likeEscape(t)}%`);
  }
  if (cities.length) {
    where.push(`city IN (${cities.map(() => '?').join(',')})`);
    binds.push(...cities);
  }
  if (st) {
    where.push('state = ?');
    binds.push(st);
  }
  const phrase = String(name).trim().toLowerCase().replace(/['’‘]/g, '').replace(/\s+/g, ' ');
  const sql =
    `SELECT ${LIST_COLUMNS} FROM restaurants WHERE ${where.join(' AND ')}
      ORDER BY (${NAME_EXPR} = ?) DESC, (${NAME_EXPR} LIKE ? ESCAPE '\\') DESC, inspection_date DESC
      LIMIT ?`;
  binds.push(phrase, `${likeEscape(tokens[0])}%`, n);
  const { results } = await db.prepare(sql).bind(...binds).all();
  return {
    query: { name: String(name), city: city || null, state: st || null },
    count: results.length,
    results: results.map(r => publicRestaurant(r)),
  };
}

export async function restaurantsNear(db, { lat, lng, radiusMiles, grades, limit, sinceMonths }) {
  const la = parseCoord(lat, -90, 90, 'lat');
  const lo = parseCoord(lng, -180, 180, 'lng');
  const r = Math.min(Math.max(Number(radiusMiles) || LIMITS.radiusMiles.def, 0.05), LIMITS.radiusMiles.max);
  const gs = parseGrades(grades);
  const n = clampInt(limit, LIMITS.near);
  const dLat = r / MILES_PER_DEG_LAT;
  const dLng = r / (MILES_PER_DEG_LAT * Math.max(Math.cos((la * Math.PI) / 180), 0.05));

  const where = ['lat BETWEEN ? AND ?', 'lng BETWEEN ? AND ?'];
  const binds = [la - dLat, la + dLat, lo - dLng, lo + dLng];
  if (gs.length) {
    where.push(`vetted_grade IN (${gs.map(() => '?').join(',')})`);
    binds.push(...gs);
  }
  const since = sinceMonths ? monthsAgo(sinceMonths) : null;
  if (since) {
    where.push('inspection_date >= ?');
    binds.push(since);
  }
  const { results } = await db.prepare(
    `SELECT ${LIST_COLUMNS} FROM restaurants WHERE ${where.join(' AND ')} LIMIT ${NEAR_SCAN_ROWS}`
  ).bind(...binds).all();
  const placed = results
    .map(row => ({ row, d: distanceMiles(la, lo, row.lat, row.lng) }))
    .filter(x => x.d <= r)
    .sort((a, b) => a.d - b.d)
    .slice(0, n);
  return {
    query: { lat: la, lng: lo, radius_miles: r, grades: gs.length ? gs : null, since: since || null },
    count: placed.length,
    results: placed.map(x => publicRestaurant(x.row, { distance: x.d })),
  };
}

export function monthsAgo(months, now = new Date()) {
  const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - months, now.getUTCDate()));
  return d.toISOString().slice(0, 10);
}

export async function lowestInCity(db, { city, state, limit, now }) {
  const cities = cityVariants(city);
  if (!cities.length) throw bad('missing_city', 'city is required');
  const st = normalizeState(state);
  if (st === null) throw bad('invalid_state', 'state must be a US state name or two-letter code');
  const n = clampInt(limit, LIMITS.lowest);
  const since = monthsAgo(LOW_SCORE_MONTHS, now);
  const binds = [...cities, since];
  let sql = `SELECT ${LIST_COLUMNS} FROM restaurants
     WHERE city IN (${cities.map(() => '?').join(',')})
       AND vetted_grade IN ('C','F') AND inspection_date >= ?`;
  if (st) {
    sql += ' AND state = ?';
    binds.push(st);
  }
  sql += " ORDER BY (vetted_grade = 'F') DESC, weighted_score ASC, inspection_date DESC LIMIT ?";
  binds.push(n);
  const { results } = await db.prepare(sql).bind(...binds).all();
  return {
    query: { city: String(city), state: st || null, grades: ['C', 'F'], since },
    count: results.length,
    note: results.length ? undefined : 'No C or F grades from recent inspections here, or the city is not covered. See /api/v1/coverage.',
    results: results.map(r => publicRestaurant(r)),
  };
}

/** Coverage from the /api/cities index (cities with 25+ mapped restaurants). */
export function coverageFrom(index, { state } = {}) {
  const st = normalizeState(state);
  if (st === null) throw bad('invalid_state', 'state must be a US state name or two-letter code');
  const cities = (index.cities || []).filter(c => !st || c.state === st);
  const byState = new Map();
  for (const c of index.cities || []) {
    const s = byState.get(c.state) || { state: c.state, state_name: STATE_NAMES[c.state] || c.state, restaurants: 0, cities: 0, latest_inspection: '' };
    s.restaurants += c.restaurant_count || 0;
    s.cities += 1;
    if ((c.latest_inspection || '') > s.latest_inspection) s.latest_inspection = c.latest_inspection;
    byState.set(c.state, s);
  }
  const shown = st ? cities : cities.slice(0, 100);
  return {
    total_restaurants: index.total || 0,
    states: [...byState.values()].sort((a, b) => b.restaurants - a.restaurants),
    cities: shown.map(c => ({
      city: c.city, state: c.state, restaurants: c.restaurant_count,
      grade_a: c.grade_a, grade_f: c.grade_f, latest_inspection: c.latest_inspection,
    })),
    note: st ? undefined : 'The 100 largest cities. Pass state= for every city in one state. Cities with fewer than 25 mapped restaurants are covered but not listed.',
  };
}

// ---------------------------------------------------------------- geocoding

const CENSUS_GEOCODER = 'https://geocoding.geo.census.gov/geocoder/locations/onelineaddress';

/**
 * A US street address to coordinates with the Census Bureau's free geocoder
 * (no key, US addresses only). Only the address text is sent. Fails soft to
 * null so the caller can ask for coordinates instead.
 */
export async function geocodeAddress(address, fetchImpl = fetch) {
  const text = String(address || '').trim().slice(0, 200);
  if (text.length < 5) return null;
  const url = `${CENSUS_GEOCODER}?benchmark=Public_AR_Current&format=json&address=${encodeURIComponent(text)}`;
  try {
    const init = { cf: { cacheTtl: 86400, cacheEverything: true } };
    if (typeof AbortSignal !== 'undefined' && AbortSignal.timeout) init.signal = AbortSignal.timeout(8000);
    const res = await fetchImpl(url, init);
    if (!res.ok) return null;
    const data = await res.json();
    const m = data?.result?.addressMatches?.[0];
    if (!m || !Number.isFinite(m.coordinates?.y) || !Number.isFinite(m.coordinates?.x)) return null;
    return { lat: m.coordinates.y, lng: m.coordinates.x, matched_address: m.matchedAddress || text };
  } catch {
    return null;
  }
}
