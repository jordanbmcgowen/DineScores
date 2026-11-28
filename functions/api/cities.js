/**
 * GET /api/cities — database total plus a city index (counts, grade
 * breakdown, bounding box per city). Returns { total, cities }; `total`
 * counts EVERY restaurant, including coordless rows and cities too small
 * for the index.
 *
 * Reads the precomputed `city_index` / `city_index_meta` tables, which every
 * SQL file the pipeline writes rebuilds (D1_CITY_INDEX_SQL). Grouping the
 * whole restaurants table per request took ~3 s at 500k rows and failed
 * uncached requests with Worker error 1101 (2026-11-28). The live query is
 * only the fallback for a database that has no index yet.
 */
import { jsonResponse, dbUnavailable, edgeCached } from './_utils.js';

export const onRequestGet = context => edgeCached(context, handleGet);

const LIVE_CITIES_SQL = `SELECT city, state,
              COUNT(*) AS restaurant_count,
              SUM(vetted_grade = 'A') AS grade_a,
              SUM(vetted_grade = 'F') AS grade_f,
              ROUND(AVG(weighted_score), 1) AS avg_score,
              MIN(lat) AS south, MAX(lat) AS north,
              MIN(lng) AS west, MAX(lng) AS east,
              MAX(inspection_date) AS latest_inspection
         FROM restaurants
        WHERE lat IS NOT NULL
        GROUP BY city, state
       HAVING COUNT(*) >= 25
        ORDER BY restaurant_count DESC`;

export async function readCityIndex(db) {
  try {
    const [{ results: meta }, { results: cities }] = await Promise.all([
      db.prepare('SELECT total FROM city_index_meta WHERE id = 1').all(),
      db.prepare(`SELECT city, state, restaurant_count, grade_a, grade_f, avg_score,
                         south, north, west, east, latest_inspection
                    FROM city_index ORDER BY restaurant_count DESC`).all(),
    ]);
    if (meta.length && cities.length) return { total: meta[0].total ?? 0, cities };
  } catch {
    // no index tables yet: fall through to the live query
  }
  return null;
}

async function handleGet({ env }) {
  if (!env.DB) return dbUnavailable();
  try {
    const indexed = await readCityIndex(env.DB);
    if (indexed) return jsonResponse(indexed);
    const [{ results: totals }, { results: cities }] = await Promise.all([
      env.DB.prepare('SELECT COUNT(*) AS total FROM restaurants').all(),
      env.DB.prepare(LIVE_CITIES_SQL).all(),
    ]);
    return jsonResponse({ total: totals[0]?.total ?? 0, cities });
  } catch {
    return dbUnavailable();
  }
}
