/**
 * GET /api/cities — city index: counts, grade breakdown, and bounding box
 * per city. Used for city pickers and future viewport-driven loading.
 */
import { jsonResponse, dbUnavailable } from './_utils.js';

export async function onRequestGet({ env }) {
  if (!env.DB) return dbUnavailable();

  const { results } = await env.DB.prepare(
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
  ).all();

  return jsonResponse(results);
}
