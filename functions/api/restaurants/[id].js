/**
 * GET /api/restaurants/:id — one restaurant's full compact record, including
 * the violation summaries omitted from lite bulk transfers.
 */
import { jsonResponse, dbUnavailable, toCompactRecord } from '../_utils.js';

export async function onRequestGet({ params, env }) {
  if (!env.DB) return dbUnavailable();

  const id = params.id;
  if (!id || !/^[a-f0-9]{16}$/.test(id)) {
    return jsonResponse({ error: 'invalid_id' }, { status: 400, maxAge: 0 });
  }

  const row = await env.DB.prepare('SELECT * FROM restaurants WHERE id = ?')
    .bind(id).first();
  if (!row) {
    return jsonResponse({ error: 'not_found' }, { status: 404, maxAge: 0 });
  }
  return jsonResponse(toCompactRecord(row));
}
