/**
 * GET /api/restaurants/:id/history — full inspection history for one
 * restaurant, newest first. Shaped like the modal's history rows.
 */
import { jsonResponse, dbUnavailable, parseJson } from '../../_utils.js';

export async function onRequestGet({ params, env }) {
  if (!env.DB) return dbUnavailable();

  const id = params.id;
  if (!id || !/^[a-f0-9]{16}$/.test(id)) {
    return jsonResponse({ error: 'invalid_id' }, { status: 400, maxAge: 0 });
  }

  const { results } = await env.DB.prepare(
    `SELECT id, inspection_date, risk_score, original_score,
            inspection_type, results, violations
       FROM inspections
      WHERE restaurant_id = ?
      ORDER BY inspection_date DESC
      LIMIT 50`
  ).bind(id).all();

  return jsonResponse(
    results.map(row => ({
      id: row.id,
      date: row.inspection_date || '',
      rs: row.risk_score || 0,
      os: row.original_score,
      type: row.inspection_type || '',
      result: row.results || '',
      v: parseJson(row.violations, []),
    }))
  );
}
