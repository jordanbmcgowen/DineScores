/**
 * Which areas' health departments are publishing late.
 *
 * Some sources post weeks behind (LA County quarterly, Austin about six
 * weeks, Napa and Louisiana several weeks). A restaurant there can show an
 * old "last inspected" date only because nothing newer is public yet, which
 * reads as "this place hasn't been inspected in months". The embedded
 * dataset keeps every city's NEWEST records, so the newest date in an area
 * is the newest its source has published; an area whose newest record is
 * over LAG_DAYS old gets a note in the report.
 *
 * Pure functions, no DOM: node tests/test_source_lag.mjs.
 */

export const LAG_DAYS = 21;
// Small towns go weeks without an inspection for no reason but size, so
// only areas with this many embedded records are judged.
export const MIN_RECORDS = 100;

const DAY_MS = 86400000;

/** The area a record belongs to: its metro, else its city, plus the state. */
export function areaKey(r) {
  return `${(r && (r.m || r.c)) || ''}|${(r && r.s) || ''}`;
}

/** Map of area key -> { newest, count } over records with a date. */
export function newestByArea(records) {
  const areas = new Map();
  for (const r of records || []) {
    if (!r || !r.d) continue;
    const k = areaKey(r);
    const a = areas.get(k);
    if (!a) areas.set(k, { newest: r.d, count: 1 });
    else {
      a.count += 1;
      if (r.d > a.newest) a.newest = r.d;
    }
  }
  return areas;
}

/**
 * { newest, days } when the restaurant's area has published nothing in
 * LAG_DAYS, else null.
 */
export function sourceLag(r, areas, now = Date.now()) {
  if (!r || !areas) return null;
  const a = areas.get(areaKey(r));
  if (!a || a.count < MIN_RECORDS) return null;
  const t = Date.parse(`${a.newest}T00:00:00Z`);
  if (Number.isNaN(t)) return null;
  const days = Math.floor((now - t) / DAY_MS);
  return days > LAG_DAYS ? { newest: a.newest, days } : null;
}
