// node tests/test_search_rank.mjs
import assert from 'node:assert/strict';
import { rankMatches, matchKind, milesBetween, nearbyBbox, formatMiles, NEARBY_MILES } from '../src/searchRank.js';

const dallas = { lat: 32.7876, lng: -96.7994 };
const rec = (i, n, lt, ln, a = '1 Main St') => ({ i, n, a, lt, ln });
const pool = [
  rec('oh', 'Pizza By Robert', 41.46, -81.95),
  rec('ca', 'Pizza Hut #24892', 33.84, -117.92),
  rec('fl', 'Pizza Hut', 28.06, -81.79),
  rec('d-far', 'Pizza Hut #1', 32.95, -96.75),         // ~11 mi north
  rec('d-gina', "Gina's Pizza", 32.789, -96.80),       // a block away, not a prefix
  rec('d-addr', 'Corner Deli', 32.788, -96.799, '12 Pizza Ln'), // closest, address only
  rec('d-leila', 'Pizza Leila', 32.790, -96.797),
  rec('nopin', 'Pizza Nowhere', 0, 0),
];

let tests = 0;
const test = (name, fn) => { fn(); tests++; };

test('nearby matches come before far ones, closest first', () => {
  const ids = rankMatches(pool, 'pizza', dallas, 6).map(x => x.r.i);
  assert.deepEqual(ids.slice(0, 4), ['d-gina', 'd-leila', 'd-far', 'd-addr']);
  assert.ok(!ids.slice(0, 4).includes('oh'));
});

test('a nearby address-only match ranks after nearby name matches, before far ones', () => {
  const ids = rankMatches(pool, 'pizza', dallas, 8).map(x => x.r.i);
  assert.ok(ids.indexOf('d-addr') > ids.indexOf('d-far'));
  assert.ok(ids.indexOf('d-addr') < ids.indexOf('fl'));
});

test('far matches: prefix first, then nearest; no pin goes last', () => {
  const far = [rec('x', 'Best Pizza', 33.0, -110), rec('fl', 'Pizza Hut', 28.06, -81.79),
    rec('oh', 'Pizza By Robert', 41.46, -81.95), rec('nopin', 'Pizza Nowhere', 0, 0)];
  assert.deepEqual(rankMatches(far, 'pizza', dallas, 6).map(x => x.r.i), ['fl', 'oh', 'nopin', 'x']);
});

test('no origin keeps the old order: prefix matches, then the rest, in data order', () => {
  const ids = rankMatches(pool, 'pizza', null, 4).map(x => x.r.i);
  assert.deepEqual(ids, ['oh', 'ca', 'fl', 'd-far']);
});

test('limit, short queries and duplicate ids', () => {
  assert.equal(rankMatches(pool, 'pizza', dallas, 3).length, 3);
  assert.deepEqual(rankMatches(pool, 'p', dallas), []);
  const dup = [pool[4], pool[4], pool[6]];
  assert.deepEqual(rankMatches(dup, 'pizza', dallas).map(x => x.r.i), ['d-gina', 'd-leila']);
});

test('matches the same set a full sort would pick', () => {
  const many = [];
  for (let k = 0; k < 500; k++) {
    many.push(rec(`r${k}`, k % 3 ? `Taco ${k}` : `El Taco ${k}`,
      32 + ((k * 37) % 100) / 10, -97 + ((k * 53) % 100) / 10));
  }
  const top = rankMatches(many, 'taco', dallas, 6);
  const near = top.filter(x => x.dist <= NEARBY_MILES);
  for (let k = 1; k < near.length; k++) assert.ok(near[k - 1].dist <= near[k].dist);
  const closest = Math.min(...many.map(r => milesBetween(dallas.lat, dallas.lng, r.lt, r.ln)));
  assert.equal(top[0].dist, closest);
});

test('matchKind, distance, bbox and labels', () => {
  assert.equal(matchKind({ n: 'Pizza Hut' }, 'pizza'), 0);
  assert.equal(matchKind({ n: "Gina's Pizza" }, 'pizza'), 1);
  assert.equal(matchKind({ n: 'Deli', a: '12 Pizza Ln' }, 'pizza'), 2);
  assert.equal(matchKind({ n: 'Deli', a: '1 Main' }, 'pizza'), -1);
  const d = milesBetween(32.7876, -96.7994, 32.7555, -97.3308); // Dallas to Fort Worth
  assert.ok(d > 29 && d < 33, String(d));
  const b = nearbyBbox(dallas);
  assert.ok(b.w < dallas.lng && b.e > dallas.lng && b.s < dallas.lat && b.n > dallas.lat);
  assert.ok(Math.abs((b.n - b.s) * 69 - 2 * NEARBY_MILES) < 0.01);
  assert.equal(nearbyBbox(null), null);
  assert.equal(formatMiles(0.26), '0.3 mi');
  assert.equal(formatMiles(11.4), '11 mi');
  assert.equal(formatMiles(Infinity), '');
  assert.equal(formatMiles(900), '');
});

console.log(`${tests} search rank tests passed`);
