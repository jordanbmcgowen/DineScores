/**
 * Client-side grading utilities for fallback data (window.DATA) that may not
 * have pre-computed vetted fields. Also used for display helpers.
 */

// Automatic-F trigger: ACTIVE pest evidence or sewage problems. A port of
// the pipeline's has_active_pest_or_sewage (dinescores_pipeline.py, the
// source of truth: change both together). An evidence word must precede the
// pest word so titles like "RODENTS NOT PRESENT" don't misfire; code titles
// that list every kind of pest ("Dead or trapped birds, insects, rodents and
// other pests", "Presence of live insects, rodents, and other pests") are
// removed first; "rodent droppings" written pest-first counts unless a
// negation precedes it in the same clause.
const PEST_EVIDENCE_RE = /(?:evidence of|live|dead|observed|found|fresh|infestation|activity of|crawling|droppings?|excreta|feces)[^.]{0,60}?\b(?:rats?|mice|mouse|roach(?:es)?|cockroach(?:es)?|rodents?|vermin|ants?|maggots?|larvae?)\b|\b(?:ants?|roach(?:es)?|maggots?)\b[^.]{0,40}?(?:crawling|in(?:side)? (?:the |of )?(?:food|ice|soda|drink|prep|machine|container))/i;
const SEWAGE_ISSUE_RE = /sewage[^.]{0,40}(?:back|overflow|leak|expos|floor|discharg)|(?:back|overflow|leak)[^.]{0,40}sewage/i;
const DEAD_PEST_TITLE_RE = /\b(?:remov(?:e|ing)\s+)?dead(?:\s*(?:or|\/)\s*trapped)?\s+(?:(?:flies|insects?|rodents?|birds?|animals?)\s*(?:,|;|\/|&|\band\b|\bor\b)\s*(?:(?:and|or)\s+)?){2,}(?:other\s+(?:pests?|animals?|vermin)|pests?\b)/gi;
const LISTED_PEST_TITLE_RE = /\b(?:no\s+)?(?:evidence\s+of|presence\s+of)(?:\s+live)?\s+(?:(?:insects?(?:\s+contamination)?|rodents?|birds?|animals?|flies)\s*(?:,|;|\/|&|\band\b|\bor\b)\s*(?:(?:and|or)\s+)?){2,}other\s+(?:pests?|animals?|vermin)\b/gi;
const PEST_AFTER_RE = /\b(?:rats?|mice|mouse|rodents?|roach(?:es)?|cockroach(?:es)?)\s+(?:droppings?|feces|fecal\s+matter|excreta)\b/gi;
const PEST_NEGATION_RE = /(?:\bno\b|\bnot\b|\bnor\b|\bnever\b|\bfree\s+of\b|\bwithout\b|\babsence\s+of\b|\bprevent\w*|\bcheck\w*\s+for\b)[^.;|:]{0,40}$/i;

export function hasActivePestOrSewage(text) {
  const t = (text || '').replace(DEAD_PEST_TITLE_RE, ' ').replace(LISTED_PEST_TITLE_RE, ' ');
  if (PEST_EVIDENCE_RE.test(t) || SEWAGE_ISSUE_RE.test(t)) return true;
  for (const m of t.matchAll(PEST_AFTER_RE)) {
    if (!PEST_NEGATION_RE.test(t.slice(Math.max(0, m.index - 60), m.index))) return true;
  }
  return false;
}

export function calculateGrade(score, violationDesc = '') {
  const hasBad = hasActivePestOrSewage(violationDesc || '');
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
  // 2026-11 expansion states; mirrors STATE_BOUNDS in dinescores_pipeline.py.
  GA: [30.35, 35.00, -85.65, -80.80],
  LA: [28.90, 33.05, -94.05, -88.80],
  AZ: [31.30, 37.00, -114.85, -109.00],
  VA: [36.50, 39.50, -83.70, -75.20],
  OH: [38.40, 42.00, -84.85, -80.50],
  CO: [36.95, 41.05, -109.10, -102.00],
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
