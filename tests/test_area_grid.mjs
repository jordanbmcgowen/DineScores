// node tests/test_area_grid.mjs
// The map's area requests must never be a box around the user's position.
import assert from 'node:assert/strict';
import { snapBbox, bboxParam, gridStep, GRID_MIN_STEP } from '../src/areaGrid.js';
import { nearbyBbox } from '../src/searchRank.js';

let tests = 0;
const test = (name, fn) => { fn(); tests++; };
const onGrid = (v, step) => Math.abs(v / step - Math.round(v / step)) < 1e-6;
const contains = (a, b) => a.w <= b.w && a.s <= b.s && a.e >= b.e && a.n >= b.n;
const around = (lat, lng, d) => ({ w: lng - d, s: lat - d, e: lng + d, n: lat + d });

test('a small box snaps outward to the minimum grid and still contains it', () => {
  const box = around(32.7876, -96.7994, 0.012);
  const s = snapBbox(box);
  assert.ok(contains(s, box));
  for (const v of [s.w, s.s, s.e, s.n]) assert.ok(onGrid(v, GRID_MIN_STEP), String(v));
  assert.ok(s.e - s.w >= GRID_MIN_STEP - 1e-9 && s.n - s.s >= GRID_MIN_STEP - 1e-9);
});

test('two positions in the same cell send the identical request', () => {
  const a = bboxParam(around(32.7710, -96.7910, 0.008));
  const b = bboxParam(around(32.7790, -96.7830, 0.008));
  assert.equal(a, b);
});

test('the request does not move with the user inside a cell', () => {
  // Walk ~1.5 km in 50 m steps; the request changes only at cell edges
  const seen = new Set();
  for (let i = 0; i <= 30; i++) seen.add(bboxParam(around(32.76 + i * 0.00045, -96.79, 0.008)));
  assert.ok(seen.size <= 3, `${seen.size} distinct requests`);
});

test('a larger box snaps to larger cells', () => {
  const box = nearbyBbox({ lat: 32.7876, lng: -96.7994 }); // ~30 mi
  const step = gridStep(box);
  assert.ok(step >= 0.2, String(step));
  const s = snapBbox(box);
  assert.ok(contains(s, box));
  for (const v of [s.w, s.s, s.e, s.n]) assert.ok(onGrid(v, step), String(v));
});

test('the parameter carries two decimals only', () => {
  const p = bboxParam(around(40.712776, -74.005974, 0.01));
  assert.match(p, /^-?\d+\.\d{2}(,-?\d+\.\d{2}){3}$/);
  assert.ok(!p.includes('40.71'), p); // not the position, rounded or otherwise
});

test('a box on a grid line keeps a cell of width, and the poles clamp', () => {
  const s = snapBbox({ w: -96.8, s: 32.75, e: -96.8, n: 32.75 });
  assert.ok(s.e > s.w && s.n > s.s);
  const p = snapBbox({ w: -10, s: 89.99, e: 10, n: 90 });
  assert.ok(p.n <= 90 && p.s < 90);
});

test('a bad box is passed through', () => {
  assert.equal(snapBbox(null), null);
});

console.log(`${tests} area grid tests passed`);
