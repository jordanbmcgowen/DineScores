/**
 * GET /api/cities — database total plus a city index (counts, grade
 * breakdown, bounding box per city). Returns { total, cities }; `total`
 * counts EVERY restaurant, including coordless rows and cities too small
 * for the index.
 */
import { jsonResponse, dbUnavailable } from './_utils.js';

export async function onRequestGet({ env }) {
  if (!env.DB) return dbUnavailable();

  const [{ results: totals }, { results: cities }] = await Promise.all([
    env.DB.prepare('SELECT COUNT(*) AS total FROM restaurants').all(),
    env.DB.prepare(
      `SELECT city, state,
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
        ORDER BY restaurant_count DESC`
    ).all(),
  ]);

  return jsonResponse({ total: totals[0]?.total ?? 0, cities });
}
