import assert from 'node:assert/strict';
import { lowScoresNear, lowScoreAt, isRecentLowScore, atRadius } from '../src/nearMe.js';

const now = Date.parse('2026-11-26T12:00:00Z');
const pos = { lat: 32.7800, lng: -96.8000, accuracy: 20 };
// ~1 m of latitude in degrees
const M = 1 / 111320;
const rec = (i, vg, dm, d = '2026-10-01') => ({ i, vg, d, lt: pos.lat + dm * M, ln: pos.lng });

// Grade and age rules
assert(isRecentLowScore(rec('a', 'F', 0), now));
assert(isRecentLowScore(rec('a', 'C', 0), now));
assert(!isRecentLowScore(rec('a', 'B', 0), now));
assert(!isRecentLowScore(rec('a', 'F', 0, '2024-01-10'), now)); // too old
assert(!isRecentLowScore(rec('a', 'F', 0, ''), now));

// Near list: nearest first, radius and coords respected
const near = lowScoresNear([
  rec('far', 'F', 900), rec('mid', 'C', 300), rec('close', 'F', 50),
  rec('good', 'A', 10), { i: 'nocoord', vg: 'F', d: '2026-10-01', lt: 0, ln: 0 },
], pos, { now });
assert.deepEqual(near.map(x => x.r.i), ['close', 'mid']);

// At: inside the radius, alone
let at = lowScoreAt([rec('f', 'F', 15)], pos, { now });
assert.equal(at.r.i, 'f');
assert.equal(at.nextDoor, false);
assert.equal(at.others, 0);

// A better-graded place clearly closer: "next door"
at = lowScoreAt([rec('f', 'F', 25), rec('a', 'A', 5)], pos, { now });
assert.equal(at.nextDoor, true);

// Two low scorers in one food court
at = lowScoreAt([rec('f', 'F', 10), rec('c', 'C', 20)], pos, { now });
assert.equal(at.r.i, 'f');
assert.equal(at.others, 1);

// Outside the at radius: no alert
assert.equal(lowScoreAt([rec('f', 'F', 120)], pos, { now }), null);
// A poor fix never alerts
assert.equal(lowScoreAt([rec('f', 'F', 5)], { ...pos, accuracy: 250 }, { now }), null);
// Dismissed restaurants stay quiet
assert.equal(lowScoreAt([rec('f', 'F', 5)], pos, { now, dismissed: new Set(['f']) }), null);
// Stale low score: no alert
assert.equal(lowScoreAt([rec('f', 'F', 5, '2023-05-01')], pos, { now }), null);

// Radius follows accuracy within bounds
assert.equal(atRadius(5), 30);
assert.equal(atRadius(45), 45);
assert.equal(atRadius(90), 60);
assert.equal(atRadius(undefined), 40);

console.log('Near me checks passed');
