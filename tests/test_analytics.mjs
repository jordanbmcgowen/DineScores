// node tests/test_analytics.mjs
import assert from 'node:assert/strict';
import { cleanSearchTerm, cleanParams, restaurantParams, GA_ID, track, initAnalytics } from '../src/analytics.js';
import { withAnalytics } from '../functions/_lib/seo.js';

let tests = 0;
const test = (name, fn) => { fn(); tests++; };

test('search terms that are names pass, normalised', () => {
  assert.equal(cleanSearchTerm('  Pizza   HUT '), 'pizza hut');
  assert.equal(cleanSearchTerm('tacos'), 'tacos');
  assert.equal(cleanSearchTerm('75201'), '75201'); // a zip is a place
  assert.equal(cleanSearchTerm("Gina's #2"), "gina's #2");
  assert.equal(cleanSearchTerm('x'.repeat(90)).length, 60);
});

test('anything that could be personal is dropped', () => {
  assert.equal(cleanSearchTerm('me@example.com'), '');
  assert.equal(cleanSearchTerm('(214) 555-0199'), '');
  assert.equal(cleanSearchTerm('+1 214 555 0199'), '');
  assert.equal(cleanSearchTerm('1234 Elm Street'), '');
  assert.equal(cleanSearchTerm('12 main'), '');
  assert.equal(cleanSearchTerm('a'), '');
  assert.equal(cleanSearchTerm(''), '');
  assert.equal(cleanSearchTerm(null), '');
});

test('params keep strings, numbers and booleans, clipped to 100', () => {
  const p = cleanParams({ a: 'x'.repeat(150), b: 3, c: true, d: null, e: { lat: 1 }, f: '' });
  assert.equal(p.a.length, 100);
  assert.equal(p.b, 3);
  assert.equal(p.c, true);
  assert.deepEqual(Object.keys(p).sort(), ['a', 'b', 'c']);
});

test('restaurant params carry public facts only, never coordinates or ids', () => {
  const p = restaurantParams({ n: 'Taco Spot', vg: 'B', c: 'Dallas', s: 'TX', lt: 32.7, ln: -96.8, i: 'abc', a: '1 Main St' });
  assert.deepEqual(p, { restaurant_name: 'Taco Spot', grade: 'B', city: 'Dallas', state: 'TX' });
  assert.deepEqual(restaurantParams(null), {});
});

test('without a measurement id nothing loads and track is a no-op', () => {
  assert.equal(GA_ID, '');
  initAnalytics();
  track('search', { search_term: 'pizza' }); // must not throw
});

test('static and server pages get the tag only for a real G- id', () => {
  const page = '<html><head><title>x</title></head><body></body></html>';
  assert.equal(withAnalytics(page, ''), page);
  assert.equal(withAnalytics(page, undefined), page);
  assert.equal(withAnalytics(page, 'UA-12345-1'), page);
  assert.equal(withAnalytics(page, 'G-ABC"><script>'), page);
  const tagged = withAnalytics(page, ' G-ABC123XYZ ');
  assert.match(tagged, /googletagmanager\.com\/gtag\/js\?id=G-ABC123XYZ"/);
  assert.match(tagged, /allow_google_signals:false/);
  assert.ok(tagged.indexOf('gtag/js') < tagged.indexOf('</head>'));
});

console.log(`test_analytics: ${tests} tests passed`);
