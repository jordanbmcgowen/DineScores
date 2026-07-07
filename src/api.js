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

/** Restaurants for a city (compact data.js-shaped records). */
export async function fetchCityFromApi(city, limit = 5000) {
  try {
    const rows = await getJson(
      `/api/restaurants?city=${encodeURIComponent(city)}&limit=${limit}`
    );
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
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
export async function fetchBboxFromApi(bbox, limit = 3000) {
  const { w, s, e, n } = bbox;
  try {
    const rows = await getJson(
      `/api/restaurants?bbox=${w},${s},${e},${n}&limit=${limit}`
    );
    const records = Array.isArray(rows) ? rows : [];
    return { records, truncated: records.length >= limit };
  } catch {
    return { records: [], truncated: false };
  }
}

/**
 * One-time probe: is the D1-backed API reachable? Used to decide whether to
 * enable viewport lazy-loading. Returns false in environments (local dev,
 * offline) where only the embedded data.js exists.
 */
export async function probeApi() {
  try {
    const res = await fetch('/api/cities', { headers: { Accept: 'application/json' } });
    return res.ok;
  } catch {
    return false;
  }
}
