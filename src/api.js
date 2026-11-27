/**
 * DineScores API client (Cloudflare Pages Functions backed by D1).
 * All helpers fail soft (return empty results) so callers can fall back
 * to other data sources.
 */

import { bboxParam, snapBbox } from './areaGrid.js';

async function getJson(path) {
  const res = await fetch(path, { headers: { Accept: 'application/json' } });
  if (!res.ok) throw new Error(`API ${path} -> ${res.status}`);
  return res.json();
}

/** Full inspection history for a restaurant, newest first. */
export async function fetchHistoryFromApi(restaurantId) {
  try {
    const rows = await getJson(`/api/restaurants/${restaurantId}/history`);
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
  }
}

/**
 * Every restaurant in a city or metro, in lite form (no violation summaries —
 * those load per-restaurant when a detail panel opens). Lite records omit the
 * `vs` key entirely so callers can distinguish "not loaded" from "none".
 */
export async function fetchAreaFromApi({ city, metro }, limit = 30000) {
  const param = metro
    ? `metro=${encodeURIComponent(metro)}`
    : `city=${encodeURIComponent(city)}`;
  try {
    const rows = await getJson(`/api/restaurants?${param}&fields=lite&limit=${limit}`);
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
  }
}

/**
 * Stream the ENTIRE database as lite records, page by page (keyset cursor).
 * Calls onBatch(records) as each page arrives so the map converges to the
 * complete dataset progressively. Resolves true when the last page landed,
 * false if the sync aborted mid-way (a later retry can start over — the
 * client dedups by id, so re-fetched pages are cheap).
 */
export async function fetchAllFromApi(onBatch, { pageSize = 30000, maxPages = 20 } = {}) {
  let cursor = '';
  for (let page = 0; page < maxPages; page++) {
    let rows;
    try {
      rows = await getJson(
        `/api/restaurants?fields=lite&cursor=${encodeURIComponent(cursor)}&limit=${pageSize}`
      );
    } catch {
      return false;
    }
    if (!Array.isArray(rows)) return false;
    if (rows.length > 0) onBatch(rows);
    if (rows.length < pageSize) return true; // final (possibly empty) page
    cursor = rows[rows.length - 1].i;
  }
  return false; // maxPages exceeded — treat as incomplete
}

/**
 * Name search across the ENTIRE database (not just loaded records), lite
 * records. Used by the search box's suggestion dropdown. With a `bbox`
 * ({ w, s, e, n }) only matches inside it come back: the server orders by
 * inspection date, not distance, so a nationwide "pizza" never includes the
 * one down the street unless the search is scoped.
 */
export async function fetchSearchFromApi(q, limit = 10, bbox = null) {
  if (!q || q.length < 2) return [];
  const box = bbox
    ? `&bbox=${bboxParam(bbox)}`
    : '';
  try {
    const rows = await getJson(
      `/api/restaurants?q=${encodeURIComponent(q)}${box}&fields=lite&limit=${limit}`
    );
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
  }
}

/** One restaurant's full record (including violation summaries). */
export async function fetchRestaurantDetail(id) {
  try {
    const rec = await getJson(`/api/restaurants/${id}`);
    return rec && rec.i ? rec : null;
  } catch {
    return null;
  }
}

/**
 * Restaurants within a map bounding box (compact data.js-shaped records).
 * bbox = { w, s, e, n }. Returns { records, truncated } — truncated is true
 * when the result hit the row limit (the viewport is denser than one page,
 * so it should be re-queried at a tighter zoom rather than cached as complete),
 * and box is the grid-snapped area actually requested.
 */
export async function fetchBboxFromApi(bbox, limit = 5000) {
  try {
    // Snapped to a coarse grid (areaGrid.js): never a box around the user
    const rows = await getJson(
      `/api/restaurants?bbox=${bboxParam(bbox)}&fields=lite&limit=${limit}`
    );
    const records = Array.isArray(rows) ? rows : [];
    return { records, truncated: records.length >= limit, box: snapBbox(bbox) };
  } catch {
    return { records: [], truncated: false, box: snapBbox(bbox) };
  }
}

/**
 * One-time probe: is the D1-backed API reachable? Returns the database's
 * TRUE restaurant total when it is, or null in environments (local dev,
 * offline) where only the embedded data.js exists. Tolerates both response
 * shapes ({ total, cities } and the legacy bare array) so a stale cached
 * response can't break the probe.
 */
export async function probeApi() {
  try {
    const body = await getJson('/api/cities');
    if (body && typeof body.total === 'number' && body.total > 0) return body.total;
    const rows = Array.isArray(body) ? body : body?.cities;
    if (Array.isArray(rows) && rows.length > 0) {
      return rows.reduce((sum, c) => sum + (c.restaurant_count || 0), 0);
    }
    return null;
  } catch {
    return null;
  }
}
