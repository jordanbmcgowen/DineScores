// Public API (/api/v1) and MCP server (/mcp): node tests/test_public_api.mjs
// No network. D1 is stood in for by node:sqlite with the pipeline's schema.
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { onRequestGet as apiGet, route } from '../functions/api/v1/[[path]].js';
import { onRequestPost as mcpPost, onRequestGet as mcpGet, TOOLS, parseFreeText, SUPPORTED_PROTOCOL_VERSIONS } from '../functions/mcp.js';
import {
  cityVariants, nameTokens, normalizeState, distanceMiles, publicRestaurant,
  coverageFrom, geocodeAddress, monthsAgo, ATTRIBUTION,
} from '../functions/_lib/public_api.js';
import { openApiSpec } from '../functions/_lib/openapi.js';
import { resetRateLimits, checkRateLimit } from '../functions/_lib/rate_limit.js';

let passed = 0;
async function test(name, fn) {
  try { await fn(); passed += 1; } catch (e) { console.error(`FAIL ${name}`); throw e; }
}

const SCHEMA = `
CREATE TABLE restaurants (id TEXT PRIMARY KEY, name TEXT NOT NULL, address TEXT, city TEXT NOT NULL,
  state TEXT, zip TEXT, lat REAL, lng REAL, metro TEXT, inspection_date TEXT, original_score INTEGER,
  risk_score INTEGER, weighted_score INTEGER, vetted_grade TEXT, infractions TEXT, summaries TEXT,
  inspection_count INTEGER DEFAULT 1, source TEXT, source_url TEXT, updated_at TEXT);
CREATE INDEX idx_restaurants_city ON restaurants(city);
CREATE INDEX idx_restaurants_geo ON restaurants(lat, lng);
CREATE TABLE inspections (id TEXT PRIMARY KEY, restaurant_id TEXT NOT NULL, inspection_date TEXT,
  risk_score INTEGER, original_score INTEGER, inspection_type TEXT, results TEXT, violations TEXT,
  source_id TEXT, source_url TEXT);`;

const recent = monthsAgo(1);
const old = '2020-01-01';
const R = (id, name, city, state, lat, lng, grade, score, date = recent, extra = {}) => ({
  id, name, address: `${id.slice(0, 3)} Main St`, city, state, zip: '75201', lat, lng, metro: '',
  inspection_date: date, original_score: null, risk_score: score, weighted_score: score, vetted_grade: grade,
  infractions: '["pests"]', summaries: JSON.stringify([{ text: '15: 3717-1-03.2(J) Food in contact with an un...', verbatim: '15: 3717-1-03.2(J) Food in contact with an unclean surface. **Observed cut broccoli', severity: 'WARNING' }]),
  inspection_count: 2, source: 'Dallas Code Compliance', source_url: 'https://example.gov/r/1', ...extra,
});

const ROWS = [
  R('aaaaaaaaaaaaaaa1', "Joe'S Pizza", 'Dallas', 'TX', 32.7800, -96.8000, 'A', 95),
  R('aaaaaaaaaaaaaaa2', "Joe'S Pizza Express", 'Dallas', 'TX', 32.7810, -96.8010, 'F', 74),
  R('aaaaaaaaaaaaaaa3', 'Pizza Hut', 'Dallas', 'TX', 32.7900, -96.8000, 'C', 72),
  R('aaaaaaaaaaaaaaa4', "Joe'S Pizza", 'Austin', 'TX', 30.2700, -97.7400, 'B', 85),
  R('aaaaaaaaaaaaaaa5', 'Taco Stand', 'Mckinney', 'TX', 33.1972, -96.6397, 'F', 55),
  R('aaaaaaaaaaaaaaa6', 'Old Diner', 'Dallas', 'TX', 32.7805, -96.8005, 'F', 50, old),
  R('aaaaaaaaaaaaaaa7', '100% Burger_Bar', 'Dallas', 'TX', 32.7, -96.7, 'A', 99),
];

function makeDb(rows = ROWS) {
  const sqlite = new DatabaseSync(':memory:');
  sqlite.exec(SCHEMA);
  const cols = Object.keys(rows[0]);
  const ins = sqlite.prepare(`INSERT INTO restaurants (${cols}) VALUES (${cols.map(() => '?')})`);
  for (const r of rows) ins.run(...cols.map(c => r[c]));
  sqlite.prepare("INSERT INTO inspections (id, restaurant_id, inspection_date, risk_score, inspection_type, results) VALUES ('i1','aaaaaaaaaaaaaaa2',?,74,'Routine','Pests seen')").run(recent);
  const queries = [];
  return {
    queries,
    prepare(sql) {
      const stmt = sqlite.prepare(sql);
      let args = [];
      const api = {
        bind(...a) { args = a; return api; },
        async all() { queries.push(sql); return { results: stmt.all(...args).map(r => ({ ...r })) }; },
        async first() { queries.push(sql); const r = stmt.get(...args); return r ? { ...r } : null; },
      };
      return api;
    },
  };
}

let ipCounter = 0;
function ctx(path, { env = { DB: makeDb() }, method = 'GET', body, ip } = {}) {
  const headers = { 'cf-connecting-ip': ip || `10.0.0.${(ipCounter += 1) % 250}` };
  const init = { method, headers };
  if (body !== undefined) {
    init.body = typeof body === 'string' ? body : JSON.stringify(body);
    headers['content-type'] = 'application/json';
  }
  return { request: new Request(`https://dinescores.com${path}`, init), env, waitUntil() {} };
}

const getJson = async path => {
  const res = await apiGet(ctx(path));
  return { status: res.status, body: await res.json(), res };
};

await test('input helpers', () => {
  assert.deepEqual(cityVariants('mckinney'), ['Mckinney', 'mckinney']);
  assert.ok(cityVariants("lee's summit").includes("Lee'S Summit"));
  assert.ok(cityVariants("lee's summit").includes("Lee's Summit"));
  assert.ok(cityVariants('st. louis').includes('St. Louis'));
  assert.deepEqual(nameTokens("Joe’s  Pizza"), ['joes', 'pizza']);
  assert.equal(normalizeState('texas'), 'TX');
  assert.equal(normalizeState('tx'), 'TX');
  assert.equal(normalizeState(''), '');
  assert.equal(normalizeState('Narnia'), null);
  assert.ok(Math.abs(distanceMiles(32.78, -96.8, 32.79, -96.8) - 0.69) < 0.01);
});

await test('search: every word must match, scoped by city and state, exact name first', async () => {
  const { status, body } = await getJson('/api/v1/restaurants/search?name=joes%20pizza&city=dallas&state=Texas');
  assert.equal(status, 200);
  assert.deepEqual(body.results.map(r => r.id), ['aaaaaaaaaaaaaaa1', 'aaaaaaaaaaaaaaa2']);
  const joe = body.results[0];
  assert.equal(joe.name, "Joe's Pizza");
  assert.equal(joe.grade, 'A');
  assert.match(joe.grade_meaning, /^A \(90-100\)/);
  assert.equal(joe.dinescores_url, 'https://dinescores.com/restaurant/joes-pizza-aaaaaaaaaaaaaaa1');
  assert.equal(joe.official_report_url, 'https://example.gov/r/1');
  assert.equal(joe.violations, undefined, 'lists stay lean');
  assert.deepEqual(body.attribution, ATTRIBUTION);
  const all = await getJson('/api/v1/restaurants/search?name=pizza%20joe');
  assert.equal(all.body.count, 3, 'word order does not matter; no city = all cities');
  const mck = await getJson('/api/v1/restaurants/search?name=taco&city=McKinney');
  assert.equal(mck.body.count, 1, 'McKinney finds the stored Mckinney');
});

await test('search: LIKE wildcards are word breaks, never patterns', async () => {
  assert.deepEqual(nameTokens('100% Burger_Bar'), ['100', 'burger', 'bar']);
  assert.equal((await getJson('/api/v1/restaurants/search?name=100%25%20burger_bar')).body.count, 1);
  assert.equal((await getJson('/api/v1/restaurants/search?name=%25%25')).body.error, 'missing_name');
});

await test('search: errors are 400 with a code, never cached', async () => {
  const a = await getJson('/api/v1/restaurants/search?name=a');
  assert.equal(a.status, 400);
  assert.equal(a.body.error, 'missing_name');
  assert.equal(a.res.headers.get('Cache-Control'), 'public, max-age=0');
  const b = await getJson('/api/v1/restaurants/search?name=pizza&state=Narnia');
  assert.equal(b.body.error, 'invalid_state');
  const c = await getJson('/api/v1/nope');
  assert.equal(c.status, 404);
});

await test('restaurant: violations in plain words, history, grade note', async () => {
  const { status, body } = await getJson('/api/v1/restaurants/aaaaaaaaaaaaaaa2');
  assert.equal(status, 200);
  const r = body.restaurant;
  assert.equal(r.grade, 'F');
  assert.match(r.grade_note, /pests, sewage/);
  assert.equal(r.violations[0].severity, 'Moderate');
  assert.equal(r.violations[0].finding, 'Food in contact with an unclean surface');
  assert.equal(r.inspection_history.length, 1);
  assert.equal(r.inspection_history[0].result, 'Pests seen');
  const slug = await getJson('/api/v1/restaurants/joes-pizza-express-aaaaaaaaaaaaaaa2');
  assert.equal(slug.status, 200, 'a full page slug works as an id');
  assert.equal((await getJson('/api/v1/restaurants/ffffffffffffffff')).status, 404);
  assert.equal((await getJson('/api/v1/restaurants/xyz')).status, 400);
});

await test('near: radius, nearest first, grade and recency filters', async () => {
  const { body } = await getJson('/api/v1/restaurants/near?lat=32.78&lng=-96.80&radius_miles=1');
  assert.deepEqual(body.results.map(r => r.id), ['aaaaaaaaaaaaaaa1', 'aaaaaaaaaaaaaaa6', 'aaaaaaaaaaaaaaa2', 'aaaaaaaaaaaaaaa3']);
  assert.ok(body.results[0].distance_miles <= body.results[1].distance_miles);
  const low = await getJson('/api/v1/restaurants/near?lat=32.78&lng=-96.80&grade=C,F&since_months=18');
  assert.deepEqual(low.body.results.map(r => r.id), ['aaaaaaaaaaaaaaa2', 'aaaaaaaaaaaaaaa3']);
  const capped = await getJson('/api/v1/restaurants/near?lat=32.78&lng=-96.80&radius_miles=500');
  assert.equal(capped.body.query.radius_miles, 10);
  assert.equal((await getJson('/api/v1/restaurants/near?lat=95&lng=0')).body.error, 'invalid_location');
  assert.equal((await getJson('/api/v1/restaurants/near')).body.error, 'missing_location');
  assert.equal((await getJson('/api/v1/restaurants/near?lat=1&lng=1&grade=Z')).body.error, 'invalid_grade');
});

await test('lowest: recent C and F only, F first', async () => {
  const { body } = await getJson('/api/v1/restaurants/lowest?city=Dallas&state=TX');
  assert.deepEqual(body.results.map(r => r.id), ['aaaaaaaaaaaaaaa2', 'aaaaaaaaaaaaaaa3'], 'the 2020 F is too old');
  assert.equal((await getJson('/api/v1/restaurants/lowest')).body.error, 'missing_city');
});

await test('queries stay on indexes or are capped', async () => {
  const env = { DB: makeDb() };
  await apiGet(ctx('/api/v1/restaurants/near?lat=32.78&lng=-96.8', { env }));
  await apiGet(ctx('/api/v1/restaurants/lowest?city=Dallas', { env }));
  for (const sql of env.DB.queries) assert.match(sql, /LIMIT/);
});

await test('coverage: from the cities index, filtered by state', () => {
  const index = { total: 900, cities: [
    { city: 'Dallas', state: 'TX', restaurant_count: 500, grade_a: 400, grade_f: 10, latest_inspection: '2026-11-20' },
    { city: 'Austin', state: 'TX', restaurant_count: 300, grade_a: 200, grade_f: 5, latest_inspection: '2026-10-01' },
    { city: 'Reno', state: 'NV', restaurant_count: 100, grade_a: 90, grade_f: 1, latest_inspection: '2026-11-01' },
  ] };
  const all = coverageFrom(index);
  assert.equal(all.states[0].state, 'TX');
  assert.equal(all.states[0].restaurants, 800);
  assert.equal(all.states[0].latest_inspection, '2026-11-20');
  assert.equal(coverageFrom(index, { state: 'nevada' }).cities.length, 1);
});

await test('database down: 503, not cached', async () => {
  const res = await apiGet(ctx('/api/v1/restaurants/search?name=pizza', { env: {} }));
  assert.equal(res.status, 503);
});

await test('geocoder: Census answer parsed; failures are null', async () => {
  const ok = async () => new Response(JSON.stringify({ result: { addressMatches: [{ matchedAddress: '1 MAIN ST, DALLAS, TX', coordinates: { x: -96.8, y: 32.78 } }] } }));
  assert.deepEqual(await geocodeAddress('1 Main St Dallas TX', ok), { lat: 32.78, lng: -96.8, matched_address: '1 MAIN ST, DALLAS, TX' });
  assert.equal(await geocodeAddress('1 Main St Dallas TX', async () => new Response('{"result":{"addressMatches":[]}}')), null);
  assert.equal(await geocodeAddress('1 Main St Dallas TX', async () => { throw new Error('down'); }), null);
  assert.equal(await geocodeAddress('x'), null);
});

await test('rate limit: per client, per minute, 429 with Retry-After', async () => {
  resetRateLimits();
  const env = { DB: makeDb(), PUBLIC_API_RATE_PER_MIN: '3' };
  const codes = [];
  for (let i = 0; i < 5; i++) codes.push((await apiGet(ctx('/api/v1', { env, ip: '1.2.3.4' }))).status);
  assert.deepEqual(codes, [200, 200, 200, 429, 429]);
  const r = await apiGet(ctx('/api/v1', { env, ip: '1.2.3.4' }));
  assert.ok(Number(r.headers.get('Retry-After')) >= 1);
  assert.equal((await apiGet(ctx('/api/v1', { env, ip: '5.6.7.8' }))).status, 200, 'other clients unaffected');
  assert.equal(await checkRateLimit(new Request('https://x/'), {}, { scope: 't', perMinute: 1, now: 0 }), null);
  assert.ok(await checkRateLimit(new Request('https://x/'), {}, { scope: 't', perMinute: 1, now: 1 }));
  assert.equal(await checkRateLimit(new Request('https://x/'), {}, { scope: 't', perMinute: 1, now: 60001 }), null, 'window resets');
  const bound = { API_RATE_LIMITER: { limit: async () => ({ success: false }) } };
  assert.deepEqual(await checkRateLimit(new Request('https://x/'), bound, { scope: 'b', perMinute: 99 }), { retryAfter: 60 });
  resetRateLimits();
});

await test('openapi: every documented path answers', async () => {
  const spec = openApiSpec();
  assert.equal(spec.openapi, '3.1.0');
  const samples = {
    '/api/v1/restaurants/search': '?name=pizza',
    '/api/v1/restaurants/near': '?lat=32.78&lng=-96.8',
    '/api/v1/restaurants/lowest': '?city=Dallas',
    '/api/v1/restaurants/{id}': '',
  };
  for (const p of Object.keys(spec.paths)) {
    if (p === '/api/v1/coverage') continue; // needs the cities index; covered above
    const path = p.replace('{id}', 'aaaaaaaaaaaaaaa1') + samples[p];
    const res = await route(ctx(path));
    assert.equal(res.status, 200, path);
  }
  const served = await getJson('/api/v1/openapi.json');
  assert.equal(served.body.info.title, 'DineScores public API');
});

// ------------------------------------------------------------------ MCP

const rpc = async (body, env) => {
  const res = await mcpPost(ctx('/mcp', { method: 'POST', body, env }));
  return { status: res.status, body: res.status === 202 ? null : await res.json(), res };
};

await test('mcp: initialize negotiates the protocol version', async () => {
  const { body, res } = await rpc({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 't', version: '1' } } });
  assert.equal(body.result.protocolVersion, '2025-06-18');
  assert.deepEqual(body.result.capabilities, { tools: { listChanged: false } });
  assert.equal(body.result.serverInfo.name, 'dinescores');
  assert.match(body.result.instructions, /dinescores_url/);
  assert.equal(res.headers.get('Access-Control-Allow-Origin'), '*');
  const future = await rpc({ jsonrpc: '2.0', id: 2, method: 'initialize', params: { protocolVersion: '2099-01-01' } });
  assert.equal(future.body.result.protocolVersion, SUPPORTED_PROTOCOL_VERSIONS[0]);
});

await test('mcp: notifications get 202, unknown methods -32601, bad JSON -32700', async () => {
  assert.equal((await rpc({ jsonrpc: '2.0', method: 'notifications/initialized' })).status, 202);
  assert.equal((await rpc({ jsonrpc: '2.0', id: 3, method: 'nope' })).body.error.code, -32601);
  assert.equal((await rpc('{not json')).body.error.code, -32700);
  assert.equal((await rpc({ jsonrpc: '2.0', id: 4, method: 'ping' })).body.result.constructor, Object);
  const batch = await rpc([{ jsonrpc: '2.0', id: 5, method: 'ping' }, { jsonrpc: '2.0', method: 'notifications/initialized' }]);
  assert.equal(batch.body.length, 1);
  assert.equal(mcpGet().status, 405);
});

await test('mcp: tools are listed read-only with valid schemas', async () => {
  const { body } = await rpc({ jsonrpc: '2.0', id: 6, method: 'tools/list' });
  const names = body.result.tools.map(t => t.name);
  assert.deepEqual(names, ['search_restaurants', 'get_restaurant', 'find_restaurants_near', 'lowest_scores_in_city', 'get_coverage', 'search', 'fetch']);
  for (const t of TOOLS) {
    assert.equal(t.inputSchema.type, 'object');
    assert.equal(t.annotations.readOnlyHint, true);
    assert.ok(t.description.length > 20);
  }
});

const call = async (name, args, env) => (await rpc({ jsonrpc: '2.0', id: 7, method: 'tools/call', params: { name, arguments: args } }, env)).body;

await test('mcp: search_restaurants and get_restaurant return the API records', async () => {
  const s = await call('search_restaurants', { name: "Joe's Pizza", city: 'Dallas', state: 'TX' });
  assert.equal(s.result.isError, undefined);
  assert.equal(s.result.structuredContent.results[0].id, 'aaaaaaaaaaaaaaa1');
  assert.deepEqual(JSON.parse(s.result.content[0].text), s.result.structuredContent);
  const g = await call('get_restaurant', { id: 'aaaaaaaaaaaaaaa2' });
  assert.equal(g.result.structuredContent.restaurant.grade, 'F');
  assert.ok(g.result.structuredContent.attribution.disclaimer);
});

await test('mcp: tool errors are isError results, not protocol errors', async () => {
  const bad = await call('get_restaurant', { id: 'nope' });
  assert.equal(bad.result.isError, true);
  assert.match(bad.result.content[0].text, /16-character/);
  const near = await call('find_restaurants_near', {});
  assert.equal(near.result.isError, true);
  const unknown = await call('drop_tables', {});
  assert.equal(unknown.error.code, -32602);
  const down = await call('search_restaurants', { name: 'pizza' }, {});
  assert.match(down.result.content[0].text, /temporarily unavailable/);
});

await test('mcp: near and lowest', async () => {
  const n = await call('find_restaurants_near', { latitude: 32.78, longitude: -96.8, grades: ['C', 'F'], recent_only: true });
  assert.deepEqual(n.result.structuredContent.results.map(r => r.id), ['aaaaaaaaaaaaaaa2', 'aaaaaaaaaaaaaaa3']);
  const l = await call('lowest_scores_in_city', { city: 'dallas' });
  assert.equal(l.result.structuredContent.results[0].grade, 'F');
});

await test('mcp: free-text search and fetch (ChatGPT shape)', async () => {
  assert.deepEqual(parseFreeText("Joe's Pizza Dallas TX")[1], { name: "Joe's Pizza", city: 'Dallas', state: 'TX' });
  assert.deepEqual(parseFreeText('taco stand in mckinney, texas'), [{ name: 'taco stand', city: 'mckinney', state: 'TX' }]);
  assert.deepEqual(parseFreeText('pizza hut new york'), [
    { name: 'pizza hut', city: '', state: 'NY' }, { name: 'pizza', city: 'hut', state: 'NY' },
  ]);
  const s = await call('search', { query: "Joe's Pizza Austin TX" });
  const results = JSON.parse(s.result.content[0].text).results;
  assert.equal(results.length, 1);
  assert.equal(results[0].id, 'aaaaaaaaaaaaaaa4');
  assert.match(results[0].title, /grade B/);
  assert.match(results[0].url, /^https:\/\/dinescores\.com\/restaurant\//);
  const f = await call('fetch', { id: results[0].id });
  const doc = JSON.parse(f.result.content[0].text);
  assert.equal(doc.id, 'aaaaaaaaaaaaaaa4');
  assert.match(doc.text, /DineScores grade: B/);
  assert.match(doc.text, /not affiliated/);
  assert.equal(doc.metadata.grade, 'B');
});

await test('mcp: rate limited with a JSON-RPC error', async () => {
  resetRateLimits();
  const env = { DB: makeDb(), MCP_RATE_PER_MIN: '1' };
  const first = await mcpPost(ctx('/mcp', { method: 'POST', body: { jsonrpc: '2.0', id: 1, method: 'ping' }, env, ip: '9.9.9.9' }));
  assert.equal(first.status, 200);
  const second = await mcpPost(ctx('/mcp', { method: 'POST', body: { jsonrpc: '2.0', id: 1, method: 'ping' }, env, ip: '9.9.9.9' }));
  assert.equal(second.status, 429);
  assert.equal((await second.json()).error.code, -32000);
  resetRateLimits();
});

await test('publicRestaurant: missing fields are null, not undefined', () => {
  const r = publicRestaurant({ id: 'aaaaaaaaaaaaaaa9', name: 'X', city: 'Y' });
  assert.equal(r.grade, null);
  assert.equal(r.score, null);
  assert.equal(r.latitude, null);
  assert.deepEqual(r.issue_types, []);
});

console.log(`ok ${passed} public API / MCP tests`);
