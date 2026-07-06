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
