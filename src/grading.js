/**
 * Client-side grading utilities for fallback data (window.DATA) that may not
 * have pre-computed vetted fields. Also used for display helpers.
 */

const BAD_WORDS = ['vermin', 'roach', 'rodent', 'sewage'];

export function calculateGrade(score, violationDesc = '') {
  const desc = (violationDesc || '').toLowerCase();
  const hasBad = BAD_WORDS.some(w => desc.includes(w));
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

/**
 * Ensure a restaurant record from window.DATA has vetted fields.
 * If the pipeline already computed them (ws, vg, inf, vs), use those.
 * Otherwise compute from risk_score and violations.
 */
export function ensureVettedFields(rec) {
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
