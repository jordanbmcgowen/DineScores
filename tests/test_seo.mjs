// Crawlable pages: node tests/test_seo.mjs (no network, no D1).
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { slugify, displayName, restaurantPath, esc, jsonLd, renderPage } from '../functions/_lib/seo.js';
import { handleGet as restaurantGet, renderRestaurant } from '../functions/restaurant/[slug].js';
import { handleGet as sitemapGet, renderSitemap } from '../functions/sitemaps/[file].js';
import { buildSeo, groupCities, MIN_INDEXED_CITY } from '../scripts/seo/build_seo.mjs';

let passed = 0;
async function test(name, fn) {
  try { await fn(); passed += 1; } catch (e) { console.error(`FAIL ${name}`); throw e; }
}

const ROW = {
  id: '0123456789abcdef', name: "Joe'S <Grill> & Bar", address: '1 Main St', city: 'Dallas',
  state: 'TX', zip: '75201', lat: 32.78, lng: -96.8, inspection_date: '2026-11-20',
  risk_score: 72, weighted_score: 68, vetted_grade: 'F', inspection_count: 3,
  summaries: JSON.stringify([{ text: 'Roaches seen', verbatim: 'Live roaches </script> in kitchen', severity: 'CRITICAL' }]),
  source: 'Dallas Code Compliance', source_url: 'https://example.gov/r/1',
};

function fakeDb({ row = ROW, history = [], rows = [] } = {}) {
  const calls = [];
  return {
    calls,
    prepare(sql) {
      return {
        bind(...args) {
          calls.push({ sql, args });
          return {
            first: async () => (args[0] === row?.id ? row : null),
            all: async () => ({ results: /FROM inspections/.test(sql) ? history : rows }),
          };
        },
      };
    },
  };
}

function assets(index) {
  return { fetch: async () => new Response(JSON.stringify(index), { headers: { 'Content-Type': 'application/json' } }) };
}

await test('slugify and display names', () => {
  assert.equal(slugify("Lee's Summit"), 'lees-summit');
  assert.equal(slugify('St. Louis'), 'st-louis');
  assert.equal(slugify('Cañon City'), 'canon-city');
  assert.equal(slugify('A & W'), 'a-and-w');
  assert.equal(displayName("Mcdonald'S Restaurant"), "Mcdonald's Restaurant");
  assert.equal(restaurantPath('0123456789abcdef', "Joe'S Grill"), '/restaurant/joes-grill-0123456789abcdef');
  assert.equal(restaurantPath('0123456789abcdef', '***'), '/restaurant/0123456789abcdef');
});

await test('escaping: HTML and JSON-LD cannot be broken out of', () => {
  assert.equal(esc('<a href="x">\'&'), '&lt;a href=&quot;x&quot;&gt;&#39;&amp;');
  assert.ok(!jsonLd({ a: '</script>' }).includes('</script>'));
  const page = renderRestaurant(ROW, [], null);
  assert.ok(!page.includes('<Grill>'));
  assert.equal((page.match(/<\/script>/g) || []).length, (page.match(/<script/g) || []).length);
  for (const m of page.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)) JSON.parse(m[1]);
});

await test('restaurant page: canonical URL renders, with grade, violations and history', async () => {
  const canonical = restaurantPath(ROW.id, ROW.name);
  const env = {
    DB: fakeDb({ history: [{ inspection_date: '2026-11-20', risk_score: 72, inspection_type: 'Routine', results: 'Fail', source_url: null }] }),
    ASSETS: assets({ dallas: 'Dallas' }),
  };
  const res = await restaurantGet({ request: new Request(`https://dinescores.com${canonical}`), params: { slug: canonical.split('/').pop() }, env });
  assert.equal(res.status, 200);
  assert.match(res.headers.get('Content-Type'), /text\/html/);
  assert.match(res.headers.get('Cache-Control'), /max-age=[1-9]/);
  const body = await res.text();
  assert.ok(body.includes(`<link rel="canonical" href="https://dinescores.com${canonical}">`));
  assert.ok(body.includes('Grade F'));
  assert.ok(body.includes('Live roaches &lt;/script&gt; in kitchen'));
  assert.ok(body.includes('href="/inspections/tx/dallas/"'), 'links the city page when it exists');
  assert.ok(body.includes('"@type":"Restaurant"'));
  assert.ok(body.includes('Routine'));
  assert.ok(!body.includes('noindex'));
});

await test('restaurant page: bare id or stale slug 301s to the canonical URL', async () => {
  const env = { DB: fakeDb(), ASSETS: assets({}) };
  for (const slug of [ROW.id, `old-name-${ROW.id}`]) {
    const res = await restaurantGet({ request: new Request(`https://dinescores.com/restaurant/${slug}`), params: { slug }, env });
    assert.equal(res.status, 301);
    assert.equal(res.headers.get('Location'), restaurantPath(ROW.id, ROW.name));
  }
});

await test('restaurant page: unknown id and junk are real 404s, noindex, never cached', async () => {
  const env = { DB: fakeDb(), ASSETS: assets({}) };
  for (const slug of ['nope', 'x-ffffffffffffffff']) {
    const res = await restaurantGet({ request: new Request(`https://dinescores.com/restaurant/${slug}`), params: { slug }, env });
    assert.equal(res.status, 404);
    assert.equal(res.headers.get('Cache-Control'), 'no-store');
    assert.match(await res.text(), /noindex/);
  }
});

await test('restaurant page: no city link when the city page does not exist; 503 without D1', async () => {
  const page = renderRestaurant(ROW, [], { plano: 'Plano' });
  assert.ok(!page.includes('/inspections/tx/dallas/'));
  const res = await restaurantGet({ request: new Request('https://dinescores.com/restaurant/a-0123456789abcdef'), params: { slug: 'a-0123456789abcdef' }, env: {} });
  assert.equal(res.status, 503);
  assert.equal(res.headers.get('Cache-Control'), 'no-store');
});

await test('restaurant sitemaps: one hex shard, a primary-key range, valid XML', async () => {
  const db = fakeDb({ rows: [{ id: 'a000000000000000', name: 'Taco & Co', inspection_date: '2026-11-01' }, { id: 'a000000000000001', name: '', inspection_date: null }] });
  const res = await sitemapGet({ params: { file: 'restaurants-a.xml' }, env: { DB: db } });
  assert.equal(res.status, 200);
  assert.deepEqual(db.calls[0].args, ['a', 'b']);
  const xml = await res.text();
  assert.ok(xml.includes('<loc>https://dinescores.com/restaurant/taco-and-co-a000000000000000</loc><lastmod>2026-11-01</lastmod>'));
  assert.equal((xml.match(/<url>/g) || []).length, 1, 'nameless rows are left out');
  const last = await sitemapGet({ params: { file: 'restaurants-f.xml' }, env: { DB: fakeDb() } });
  assert.equal(last.status, 200);
  assert.equal((await sitemapGet({ params: { file: 'restaurants-g.xml' }, env: { DB: fakeDb() } })).status, 404);
  assert.ok(renderSitemap([]).includes('<urlset'));
});

await test('cities merge spellings that slug alike and keep the common one', () => {
  const recs = [
    { i: '1', n: 'A', c: 'St. Louis', s: 'MO', d: '2026-11-01', vg: 'A', ws: 95 },
    { i: '2', n: 'B', c: 'St. Louis', s: 'MO', d: '2026-11-02', vg: 'F', ws: 50 },
    { i: '3', n: 'C', c: 'St Louis', s: 'MO', d: '2026-11-03', vg: 'B', ws: 85 },
    { i: '4', n: 'D', c: 'Nowhere', s: 'ZZ', d: '2026-11-03' },
  ];
  const states = groupCities(recs);
  assert.deepEqual([...states.keys()], ['MO']);
  const city = states.get('MO').get('st-louis');
  assert.equal(city.name, 'St. Louis');
  assert.equal(city.records.length, 3);
  assert.equal(city.records[0].i, '3', 'newest first');
  assert.equal(city.indexed, 3 >= MIN_INDEXED_CITY);
});

await test('build: pages, robots, sitemaps and llms.txt from a data.js', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'seo-'));
  const recs = [];
  for (let k = 0; k < 4; k += 1) recs.push({ i: `d${k}`.padEnd(16, '0'), n: `Place ${k}`, a: `${k} Elm`, c: 'Dallas', s: 'TX', d: `2026-11-0${k + 1}`, vg: k ? 'A' : 'F', ws: k ? 96 : 55, rs: 90, src: 'Dallas' });
  recs.push({ i: 'e'.padEnd(16, '0'), n: 'Lone <Diner>', c: 'Tiny', s: 'TX', d: '2026-10-01', vg: 'A', ws: 100 });
  const data = path.join(dir, 'data.js');
  fs.writeFileSync(data, `/* DineScores embedded data — auto-generated 2026-11-26 */\nwindow.DATA = ${JSON.stringify(recs)};\n`);
  const out = path.join(dir, 'out');
  const summary = buildSeo({ dataFile: data, out });
  assert.deepEqual(summary, { states: 1, cities: 2, records: 5 });
  const dallas = fs.readFileSync(path.join(out, 'inspections/tx/dallas/index.html'), 'utf8');
  assert.ok(dallas.includes('<h1>Dallas, TX restaurant health inspections</h1>'));
  assert.ok(dallas.includes('/restaurant/place-0-d000000000000000'));
  assert.ok(!dallas.includes('noindex'));
  const tiny = fs.readFileSync(path.join(out, 'inspections/tx/tiny/index.html'), 'utf8');
  assert.ok(tiny.includes('noindex, follow'), 'thin city pages are not indexed');
  assert.ok(tiny.includes('Lone &lt;Diner&gt;'));
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(out, 'inspections/tx/cities.json'), 'utf8')), { dallas: 'Dallas', tiny: 'Tiny' });
  const pages = fs.readFileSync(path.join(out, 'sitemap-pages.xml'), 'utf8');
  assert.ok(pages.includes('https://dinescores.com/inspections/tx/dallas/'));
  assert.ok(!pages.includes('/tiny/'));
  const index = fs.readFileSync(path.join(out, 'sitemap.xml'), 'utf8');
  assert.equal((index.match(/<sitemap>/g) || []).length, 17);
  const robots = fs.readFileSync(path.join(out, 'robots.txt'), 'utf8');
  assert.match(robots, /Disallow: \/api\//);
  assert.match(robots, /Sitemap: https:\/\/dinescores.com\/sitemap.xml/);
  assert.match(fs.readFileSync(path.join(out, 'llms.txt'), 'utf8'), /^# DineScores/);
  assert.ok(fs.existsSync(path.join(out, 'about/index.html')));
  const dev = fs.readFileSync(path.join(out, 'developers/index.html'), 'utf8');
  assert.ok(dev.includes('https://dinescores.com/mcp'));
  assert.ok(dev.includes('id="terms"'));
  assert.ok(fs.existsSync(path.join(out, 'privacy/index.html')));
  assert.match(fs.readFileSync(path.join(out, 'llms.txt'), 'utf8'), /\/api\/v1\/openapi\.json/);
  assert.ok(pages.includes('https://dinescores.com/developers/'));
  fs.rmSync(dir, { recursive: true, force: true });
});

await test('renderPage: one canonical, one title', () => {
  const page = renderPage({ title: 't', description: 'd', path: '/x/', body: '' });
  assert.equal((page.match(/rel="canonical"/g) || []).length, 1);
  assert.equal((page.match(/<title>/g) || []).length, 1);
});

console.log(`test_seo: ${passed} passed`);
