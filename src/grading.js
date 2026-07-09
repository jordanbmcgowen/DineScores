/**
 * Client-side grading utilities for fallback data (window.DATA) that may not
 * have pre-computed vetted fields. Also used for display helpers.
 */

// Automatic-F trigger: ACTIVE pest evidence or sewage problems (mirrors the
// pipeline's has_active_pest_or_sewage — an evidence word must precede the
// pest word so titles like "RODENTS NOT PRESENT" don't misfire).
const PEST_EVIDENCE_RE = /(?:evidence of|live|dead|observed|found|fresh|infestation|activity of|droppings?|excreta|feces)[^.]{0,60}?\b(?:rats?|mice|mouse|roach(?:es)?|cockroach(?:es)?|rodents?|vermin)\b/i;
const SEWAGE_ISSUE_RE = /sewage[^.]{0,40}(?:back|overflow|leak|expos|floor|discharg)|(?:back|overflow|leak)[^.]{0,40}sewage/i;

export function calculateGrade(score, violationDesc = '') {
  const desc = violationDesc || '';
  const hasBad = PEST_EVIDENCE_RE.test(desc) || SEWAGE_ISSUE_RE.test(desc);
  if (score < 70 || hasBad) return 'F';
  if (score >= 90) return 'A';
  if (score >= 80) return 'B';
  if (score >= 70) return 'C';
  return 'F';
}

export function detectInfractions(text) {
  const lower = (text || '').toLowerCase();
  const infractions = [];
  if (/\b(vermin|roach|rodent|rats?|mice|insect|fly|flies|gnat|pest)\b/.test(lower)) infractions.push('pests');
  if (/(temp|cool|heat|thaw|thermometer|refrigerat|hot|cold|hold)/.test(lower)) infractions.push('temp');
  if (/(hand|glove|hair|eat|drink|tobacco|fingernail|hygiene|wash)/.test(lower)) infractions.push('hygiene');
  if (/(sink|plumbing|water|equipment|warewash|surface|repair|door|wall|ceiling|floor|light|vent|clean)/.test(lower)) infractions.push('equipment');
  if (/(permit|sign|post|certified|manager|knowledge|certificate)/.test(lower)) infractions.push('docs');
  return infractions;
}

export function buildViolationText(violations) {
  if (!violations || !Array.isArray(violations)) return '';
  return violations
    .map(v => (Array.isArray(v) && v.length >= 3) ? v[2] : '')
    .filter(Boolean)
    .join('|||');
}

// Coordinate sanity (mirrors the pipeline's coords_plausible): a wrong point
// is worse than none — a record with scrubbed coords stays in the list but
// off the map, instead of appearing in the ocean. Boxes are generous
// (±0.6° slack) so metro suburbs and state borders never misfire.
const CONUS_BOUNDS = [24.3, 49.5, -125.5, -66.5]; // s, n, w, e
const STATE_BOUNDS = {
  NY: [40.45, 45.05, -79.85, -71.75],
  TX: [25.75, 36.55, -106.70, -93.45],
  WA: [45.50, 49.05, -124.90, -116.85],
  NV: [34.95, 42.05, -120.10, -113.95],
  NC: [33.75, 36.65, -84.40, -75.35],
  CA: [32.45, 42.05, -124.55, -114.05],
  MA: [41.15, 42.95, -73.60, -69.85],
  IL: [36.90, 42.55, -91.60, -87.00],
  DC: [38.75, 39.05, -77.15, -76.85],
  FL: [24.35, 31.05, -87.70, -79.95],
};
const COORD_MARGIN = 0.6;

function coordsPlausible(lat, lng, state) {
  const [s, n, w, e] = CONUS_BOUNDS;
  if (!(lat >= s && lat <= n && lng >= w && lng <= e)) return false;
  let st = (state || '').trim().toUpperCase();
  if (st === 'TEXAS') st = 'TX';
  const box = STATE_BOUNDS[st];
  if (!box) return true;
  const [bs, bn, bw, be] = box;
  const m = COORD_MARGIN;
  return lat >= bs - m && lat <= bn + m && lng >= bw - m && lng <= be + m;
}

/**
 * Ensure a restaurant record from window.DATA has vetted fields, and scrub
 * obviously-wrong coordinates. If the pipeline already computed the vetted
 * fields (ws, vg, inf, vs), use those; otherwise compute from risk_score
 * and violations.
 */
export function ensureVettedFields(rec) {
  if (rec.lt && rec.ln && !coordsPlausible(rec.lt, rec.ln, rec.s)) {
    rec = { ...rec, lt: 0, ln: 0 };
  }
  if (rec.vg) return rec; // already has vetted grade

  const violationText = buildViolationText(rec.v);
  const score = rec.rs || 0;
  const grade = calculateGrade(score, violationText);
  const infractions = detectInfractions(violationText);

  return {
    ...rec,
    ws: rec.ws || score,
    vg: grade,
    inf: rec.inf || infractions,
    vs: rec.vs || [],
    it: rec.it || '',
  };
}
