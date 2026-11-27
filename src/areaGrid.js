// Area requests never reveal where the user is.
//
// Every box the map sends to /api/restaurants (viewport loads, the boxes
// around a GPS fix, the search box's "nearby" scope) is first grown outward
// to a coarse grid, so the server only ever sees a grid-aligned area of at
// least GRID_MIN_STEP degrees (~5 km / 3 miles), never a box centred on a
// device's position. Exact distances, sorting and the "near me" alert are
// all worked out in the browser from the records that come back.
//
// The cell size grows with the box (a power of two times the minimum, at
// least a quarter of the box's longer side), so a large box snaps to large
// cells and nearby visitors send the same boxes, which the edge cache can
// then answer.

export const GRID_MIN_STEP = 0.05;

export function gridStep(box) {
  const span = Math.max(box.e - box.w, box.n - box.s, 0);
  let step = GRID_MIN_STEP;
  while (step < span / 4 && step < 45) step *= 2;
  return step;
}

const round2 = v => Math.round(v * 100) / 100;
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

/** The smallest grid-aligned box containing `box` ({ w, s, e, n }). */
export function snapBbox(box) {
  if (!box || ![box.w, box.s, box.e, box.n].every(Number.isFinite)) return box;
  const step = gridStep(box);
  const down = v => round2(Math.floor(round2(v / step * 1e6) / 1e6) * step);
  const up = v => round2(Math.ceil(round2(v / step * 1e6) / 1e6) * step);
  let w = down(box.w), s = down(box.s), e = up(box.e), n = up(box.n);
  // A box exactly on a grid line still gets a cell's width
  if (e <= w) e = round2(w + step);
  if (n <= s) n = round2(s + step);
  return {
    w: clamp(w, -180, 180), s: clamp(s, -90, 90),
    e: clamp(e, -180, 180), n: clamp(n, -90, 90),
  };
}

/** The bbox= value for a snapped box: "w,s,e,n", two decimals. */
export function bboxParam(box) {
  const b = snapBbox(box);
  return [b.w, b.s, b.e, b.n].map(v => v.toFixed(2)).join(',');
}
