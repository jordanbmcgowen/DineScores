/**
 * "Near me" low-score logic: which low-scoring restaurants are around the
 * user, and whether the user appears to be standing in one. Pure functions
 * (no DOM, no React) so tests/test_near_me.mjs can check them under node.
 *
 * A false "you're in a bad restaurant" warning is worse than a missed one, so
 * every rule leans quiet:
 * - Low score means the grade the map already shows: C or F.
 * - The inspection must be recent (LOW_SCORE_MAX_AGE_DAYS). A two-year-old C
 *   says little about tonight's kitchen.
 * - "At" needs a GPS fix good enough to tell neighbours apart. A fix worse
 *   than AT_MAX_ACCURACY_M never raises the at-restaurant alert.
 * - If a restaurant with a good grade is closer than the low scorer, the
 *   alert says "next door", not "you're at".
 */

export const LOW_GRADES = new Set(['C', 'F']);
export const LOW_SCORE_MAX_AGE_DAYS = 548; // ~18 months
export const NEAR_RADIUS_M = 800; // ~half a mile: the "near me" list
export const AT_MIN_RADIUS_M = 30;
export const AT_MAX_RADIUS_M = 60;
export const AT_MAX_ACCURACY_M = 100;
const NEXT_DOOR_MARGIN_M = 10;

/** Metres between a {lat, lng} and a record's lt/ln (equirectangular). */
export function distanceMeters(pos, r) {
  const dLat = (r.lt - pos.lat) * 111320;
  const dLng = (r.ln - pos.lng) * 111320 * Math.cos((pos.lat * Math.PI) / 180);
  return Math.hypot(dLat, dLng);
}

function daysSince(dateStr, now) {
  if (!dateStr) return Infinity;
  const t = Date.parse(dateStr + 'T00:00:00Z');
  if (Number.isNaN(t)) return Infinity;
  return (now - t) / 86400000;
}

/** True when a record counts as a recent low score. */
export function isRecentLowScore(r, now = Date.now()) {
  return LOW_GRADES.has(r.vg) && daysSince(r.d, now) <= LOW_SCORE_MAX_AGE_DAYS;
}

/**
 * Recent low scorers within `radius` metres, nearest first, each as
 * { r, dist }. Records without coordinates are skipped.
 */
export function lowScoresNear(records, pos, { radius = NEAR_RADIUS_M, now = Date.now() } = {}) {
  if (!pos) return [];
  // Cheap box pre-filter before the real distance (records can be ~400k)
  const dLat = radius / 111320;
  const dLng = dLat / Math.max(0.2, Math.cos((pos.lat * Math.PI) / 180));
  const out = [];
  for (const r of records) {
    if (!r.lt || !r.ln) continue;
    if (Math.abs(r.lt - pos.lat) > dLat || Math.abs(r.ln - pos.lng) > dLng) continue;
    if (!isRecentLowScore(r, now)) continue;
    const dist = distanceMeters(pos, r);
    if (dist <= radius) out.push({ r, dist });
  }
  return out.sort((a, b) => a.dist - b.dist);
}

/** How close counts as "at" a restaurant for a fix of this accuracy. */
export function atRadius(accuracy) {
  if (!(accuracy > 0)) return AT_MIN_RADIUS_M + 10; // accuracy unknown
  return Math.min(AT_MAX_RADIUS_M, Math.max(AT_MIN_RADIUS_M, accuracy));
}

/**
 * The low scorer the user appears to be in (or beside), or null.
 * Returns { r, dist, nextDoor, others } where `others` counts further low
 * scorers inside the same radius (a food court, a strip mall).
 */
export function lowScoreAt(records, pos, { now = Date.now(), dismissed = null } = {}) {
  if (!pos) return null;
  if (pos.accuracy > AT_MAX_ACCURACY_M) return null;
  const radius = atRadius(pos.accuracy);
  const low = lowScoresNear(records, pos, { radius, now })
    .filter(x => !(dismissed && dismissed.has(x.r.i)));
  if (low.length === 0) return null;
  const hit = low[0];
  // Any restaurant (any grade) clearly closer than the low scorer means the
  // user is more likely in that one.
  const dLat = radius / 111320;
  let nextDoor = false;
  for (const r of records) {
    if (!r.lt || !r.ln || r.i === hit.r.i || LOW_GRADES.has(r.vg)) continue;
    if (Math.abs(r.lt - pos.lat) > dLat * 2 || Math.abs(r.ln - pos.lng) > dLat * 2) continue;
    if (distanceMeters(pos, r) + NEXT_DOOR_MARGIN_M < hit.dist) { nextDoor = true; break; }
  }
  return { r: hit.r, dist: hit.dist, nextDoor, others: low.length - 1 };
}
