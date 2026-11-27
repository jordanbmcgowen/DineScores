// Plain-language wording for a violation summary (`vs` item) on the
// restaurant card. Pure presentation: grading never reads this.
//
// Many sources carry no plain phrase, so the summary text is the start of
// the code citation ("15: 3717-1-03.2(J) Food in contact with an un...").
// For those, take the description from the full verbatim with the item
// number and code citations stripped, up to the first sentence.

export const SEVERITY_LABEL = {
  CRITICAL: 'Serious',
  WARNING: 'Moderate',
  INFO: 'Minor',
};

export function severityLabel(sev) {
  return SEVERITY_LABEL[sev] || 'Minor';
}

const ITEM_NO_RE = /^\s*(?:item\s*)?\d{1,3}[a-z]?\s*[:.)-]\s+/i;
// One leading citation token: a section number (3717-1-03.2(J), 228.31(d),
// 5-205.11, 4-601.11(A)), a bracketed tag ((Repeat), (Pf), (P), [C]) or a
// "Repeat"/"Corrected" marker, or the commas and "and" between
// several of them ("(A),(B), and (C)(1),(4)-(16)"). Applied repeatedly.
const CITE_RE = /^\s*(?:\d+[A-Za-z]?(?:[-.]\d+[A-Za-z]?)+(?:\s*\([A-Za-z0-9.]{1,6}\))*|\([^)]{1,14}\)(?:\s*-\s*\([^)]{1,14}\))?|\[[^\]]{1,14}\]|(?:and|or)(?=\s*\()|(?:MFC|JCC|FC|TFER|CRFC)\s+\d[\d.-]*(?:\s*\([^)]{1,14}\))*|\d{1,2}\.(?=\s)|corrective actions:|[,;]|repeat|corrected on site|cos)\s*[:,;-]?\s*/i;
const MAX_LEN = 80;

function stripCodes(s) {
  let out = s.replace(ITEM_NO_RE, '');
  for (let i = 0; i < 16; i++) {
    const next = out.replace(CITE_RE, '');
    if (next === out) break;
    out = next;
  }
  return out.trim();
}

// A code citation or "Corrective Actions:" after the title starts the
// citation text; keep the title before it.
const MID_CITE_RE = /\s*(?:corrective actions:|:?\s+\d+-\d+\.\d+)/i;

function firstSentence(s) {
  // The inspector's comment usually follows "**"; the code title comes first.
  let t = s.split('**')[0].trim() || s.replace(/\*\*/g, '').trim();
  const mid = t.search(MID_CITE_RE);
  if (mid >= 12) t = t.slice(0, mid);
  const m = t.match(/^(.+?[a-z0-9)%])\s*\.(\s|$)/);
  if (m && m[1].length >= 12) t = m[1];
  return t
    .replace(/\s*\((?:OUT|IN|N\/O|N\/A)\)/g, '')
    .replace(/:\s*\d{1,2}$/, '')
    .replace(/[.;:,\s]+$/, '');
}

function clip(s) {
  if (s.length <= MAX_LEN) return s;
  const cut = s.slice(0, MAX_LEN);
  const sp = cut.lastIndexOf(' ');
  return `${(sp > 40 ? cut.slice(0, sp) : cut).replace(/[.,;:\s]+$/, '')}…`;
}

function capitalize(s) {
  if (!s) return s;
  // All-caps titles (Chicago, some Texas forms) read as shouting.
  const letters = s.replace(/[^A-Za-z]/g, '');
  if (letters.length > 8 && letters === letters.toUpperCase()) s = s.toLowerCase();
  return s[0].toUpperCase() + s.slice(1);
}

const looksRaw = t =>
  /(\.\.\.|…)\s*$/.test(t) || ITEM_NO_RE.test(t) || CITE_RE.test(t);

export function findingTitle(item) {
  const text = String(item?.text || '').trim();
  const verbatim = String(item?.verbatim || '').trim();
  if (text && !looksRaw(text)) return text;
  const source = verbatim || text.replace(/(\.\.\.|…)\s*$/, '');
  const cleaned = clip(capitalize(firstSentence(stripCodes(source))));
  return cleaned || text;
}
