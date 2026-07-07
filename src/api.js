/**
 * DineScores API client (Cloudflare Pages Functions backed by D1).
 * All helpers fail soft (return empty results) so callers can fall back
 * to other data sources.
 */

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

/** One restaurant's full record (including violation summaries). */
export async function fetchRestaurantDetail(id) {
  try {
    const rec = await getJson(`/api/restaurants/${id}`);
    return rec && rec.i ? rec : null;
  } catch {
    return null;
  }
}

/** City index with counts and bounding boxes. */
export async function fetchCitiesFromApi() {
  try {
    const rows = await getJson('/api/cities');
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
  }
}

/**
 * Restaurants within a map bounding box (compact data.js-shaped records).
 * bbox = { w, s, e, n }. Returns { records, truncated } — truncated is true
 * when the result hit the row limit (the viewport is denser than one page,
 * so it should be re-queried at a tighter zoom rather than cached as complete).
 */
export async function fetchBboxFromApi(bbox, limit = 5000) {
  const { w, s, e, n } = bbox;
  try {
    const rows = await getJson(
      `/api/restaurants?bbox=${w},${s},${e},${n}&fields=lite&limit=${limit}`
    );
    const records = Array.isArray(rows) ? rows : [];
    return { records, truncated: records.length >= limit };
  } catch {
    return { records: [], truncated: false };
  }
}

/**
 * One-time probe: is the D1-backed API reachable? Returns the cities index
 * (true per-city totals) when it is, or null in environments (local dev,
 * offline) where only the embedded data.js exists.
 */
export async function probeApi() {
  try {
    const rows = await getJson('/api/cities');
    return Array.isArray(rows) && rows.length > 0 ? rows : null;
  } catch {
    return null;
  }
}
