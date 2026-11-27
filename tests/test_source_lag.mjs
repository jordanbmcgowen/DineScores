import assert from 'node:assert/strict';
import { areaKey, newestByArea, sourceLag, MIN_RECORDS } from '../src/sourceLag.js';

const now = Date.parse('2026-11-27T12:00:00Z');
const many = (n, base) => Array.from({ length: n }, (_, i) => ({ ...base, i: `${base.c}${i}` }));

// Metro wins over city, state keeps same-named towns apart
assert.equal(areaKey({ m: 'Los Angeles', c: 'Pasadena', s: 'CA' }), 'Los Angeles|CA');
assert.equal(areaKey({ m: '', c: 'Portland', s: 'ME' }), 'Portland|ME');

const la = many(MIN_RECORDS, { m: 'Los Angeles', c: 'Burbank', s: 'CA', d: '2026-08-01' });
la[5].d = '2026-09-30';
const dallas = many(MIN_RECORDS, { m: 'Dallas', c: 'Dallas', s: 'TX', d: '2026-11-20' });
const town = many(MIN_RECORDS - 1, { m: '', c: 'Tiny', s: 'MT', d: '2026-06-01' });
const areas = newestByArea([...la, ...dallas, ...town, { c: 'NoDate', s: 'TX' }]);

// The area's newest date is what counts, not the restaurant's own
const lag = sourceLag({ m: 'Los Angeles', c: 'Burbank', s: 'CA', d: '2026-08-01' }, areas, now);
assert.deepEqual(lag, { newest: '2026-09-30', days: 58 });

// Current areas, small areas and unknown areas get no note
assert.equal(sourceLag(dallas[0], areas, now), null);
assert.equal(sourceLag(town[0], areas, now), null);
assert.equal(sourceLag({ c: 'Nowhere', s: 'ZZ' }, areas, now), null);
assert.equal(sourceLag(null, areas, now), null);
assert.equal(sourceLag(dallas[0], null, now), null);

// The 21-day line: 21 days is current, 22 is late
const edge = newestByArea(many(MIN_RECORDS, { c: 'Edge', s: 'TX', d: '2026-11-06' }));
assert.equal(sourceLag({ c: 'Edge', s: 'TX' }, edge, now), null);
const late = newestByArea(many(MIN_RECORDS, { c: 'Edge', s: 'TX', d: '2026-11-05' }));
assert.equal(sourceLag({ c: 'Edge', s: 'TX' }, late, now).days, 22);

console.log('source lag: all tests passed');
