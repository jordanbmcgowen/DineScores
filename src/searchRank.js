// Search suggestions, nearest first. Pure (no DOM), tested by
// `node tests/test_search_rank.mjs`.
//
// The dropdown used to take the first six name matches in data order, so
// "pizza" typed over downtown Dallas offered Ohio, California and Florida.
// Matches are now ranked from an origin (the middle of the map, which is
// where the user is looking; their GPS position when the map has not
// reported yet):
//   1. matches within NEARBY_MILES, name matches before address-only
//      matches, then by distance;
//   2. everything else, name-prefix matches first, then by distance.
// With no origin the old order stands (prefix matches, then the rest).

export const NEARBY_MILES = 30;

const MILES_PER_DEG_LAT = 69.0;

/** Miles between two points (equirectangular: exact enough to rank by). */
export function milesBetween(lat1, lng1, lat2, lng2) {
  const dLat = (lat2 - lat1) * MILES_PER_DEG_LAT;
  const dLng = (lng2 - lng1) * MILES_PER_DEG_LAT *
    Math.cos(((lat1 + lat2) / 2) * Math.PI / 180);
  return Math.sqrt(dLat * dLat + dLng * dLng);
}

/** How a record matches the (lower-cased) query: 0 name prefix, 1 name, 2 address, -1 none. */
export function matchKind(r, q) {
  const name = (r.n || '').toLowerCase();
  const at = name.indexOf(q);
  if (at === 0) return 0;
  if (at > 0) return 1;
  return (r.a || '').toLowerCase().includes(q) ? 2 : -1;
}

function distanceFrom(origin, r) {
  if (!origin || !r.lt || !r.ln) return Infinity;
  return milesBetween(origin.lat, origin.lng, r.lt, r.ln);
}

function compare(a, b) {
  if (a.near !== b.near) return a.near ? -1 : 1;
  if (a.near) {
    // Close by: a name match beats an address match, then the closer wins
    const an = a.kind === 2 ? 1 : 0;
    const bn = b.kind === 2 ? 1 : 0;
    if (an !== bn) return an - bn;
  } else if (a.kind !== b.kind) {
    return a.kind - b.kind;
  }
  if (a.dist !== b.dist) return a.dist < b.dist ? -1 : 1;
  return a.seq - b.seq; // stable
}

/**
 * The best `limit` records matching `q` (already trimmed and lower-cased),
 * nearest to `origin` ({lat, lng} or null) first. Duplicated ids are kept
 * once. Each result is { r, dist } with dist in miles (Infinity = unknown).
 */
export function rankMatches(records, q, origin, limit = 6) {
  if (!q || q.length < 2) return [];
  const best = [];
  const seen = new Set();
  let seq = 0;
  for (const r of records) {
    const kind = matchKind(r, q);
    if (kind < 0) continue;
    if (r.i != null) {
      if (seen.has(r.i)) continue;
      seen.add(r.i);
    }
    const dist = distanceFrom(origin, r);
    const m = { r, kind, dist, near: dist <= NEARBY_MILES, seq: seq++ };
    // Keep only the best `limit` as we go: the pool can be 400k records
    if (best.length === limit && compare(m, best[limit - 1]) >= 0) continue;
    let at = best.length;
    while (at > 0 && compare(m, best[at - 1]) < 0) at--;
    best.splice(at, 0, m);
    if (best.length > limit) best.pop();
  }
  return best.map(({ r, dist }) => ({ r, dist }));
}

/** A box of roughly NEARBY_MILES around the origin, for the API's bbox=. */
export function nearbyBbox(origin) {
  if (!origin) return null;
  const dLat = NEARBY_MILES / MILES_PER_DEG_LAT;
  const dLng = dLat / Math.max(0.2, Math.cos(origin.lat * Math.PI / 180));
  return {
    w: origin.lng - dLng, s: origin.lat - dLat,
    e: origin.lng + dLng, n: origin.lat + dLat,
  };
}

/** "0.3 mi", "12 mi"; '' when unknown or too far to mean anything. */
export function formatMiles(dist) {
  if (!Number.isFinite(dist) || dist > 500) return '';
  return dist < 10 ? `${dist.toFixed(1)} mi` : `${Math.round(dist)} mi`;
}
