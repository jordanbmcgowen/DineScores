#!/usr/bin/env node
/**
 * Build the crawlable side of the site from public/data.js, after `vite build`.
 *
 * The map is a client-rendered SPA, so before this every URL answered the same
 * empty shell (Pages falls back to index.html) and crawlers saw one page with
 * no data. This writes plain HTML that needs no JavaScript:
 *
 *   /about/                        how grades work, sources, update schedule
 *   /inspections/                  every state
 *   /inspections/<st>/             every city in a state
 *   /inspections/<st>/<city>/      a city's recent inspections, lowest and
 *                                  highest scores, links to each restaurant
 *   /inspections/<st>/cities.json  slug -> name, read by the restaurant pages
 *   /robots.txt, /sitemap.xml, /sitemap-pages.xml, /llms.txt
 *
 * Restaurant pages (/restaurant/<slug>-<id>) and their sitemaps come from D1
 * in Pages Functions (functions/restaurant, functions/sitemaps): there are
 * ~376k of them, far over Pages' 20,000-file limit.
 *
 * Everything written here is build output (gitignored), regenerated on every
 * deploy, so it follows each weekly data.js commit.
 *
 *   node scripts/seo/build_seo.mjs [--data public/data.js] [--out public]
 */
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {
  SITE, STATE_NAMES, stateName, esc, slugify, displayName, restaurantPath,
  statePath, cityPath, gradeBadge, formatDate, renderPage,
} from '../../functions/_lib/seo.js';

// Pages with fewer restaurants than this are kept (links resolve) but marked
// noindex and left out of the sitemap: a page about one taco truck is thin.
export const MIN_INDEXED_CITY = 3;
export const RESTAURANT_SITEMAP_SHARDS = '0123456789abcdef'.split('');
const RECENT_ROWS = 60;
const LIST_ROWS = 15;

function arg(name, fallback) {
  const i = process.argv.indexOf(name);
  return i > -1 ? process.argv[i + 1] : fallback;
}

export function loadData(file) {
  const text = fs.readFileSync(file, 'utf8');
  const sandbox = { window: {} };
  vm.runInNewContext(text, sandbox, { filename: file });
  const generated = (text.slice(0, 200).match(/auto-generated (\d{4}-\d{2}-\d{2})/) || [])[1]
    || new Date().toISOString().slice(0, 10);
  return { records: sandbox.window.DATA || [], generated };
}

function score(r) {
  return Number.isFinite(r.ws) ? r.ws : r.rs;
}

function mostCommon(values) {
  const counts = new Map();
  for (const v of values) if (v) counts.set(v, (counts.get(v) || 0) + 1);
  return [...counts.entries()].sort((a, b) => b[1] - a[1]).map(([v]) => v);
}

function pct(n, total) {
  return total ? Math.round((100 * n) / total) : 0;
}

/** Group records into states -> cities keyed by slug (spellings that slug alike merge). */
export function groupCities(records) {
  const states = new Map();
  for (const r of records) {
    const st = String(r.s || '').toUpperCase();
    if (!STATE_NAMES[st] || !r.c || !r.i) continue;
    const slug = slugify(r.c);
    if (!slug) continue;
    if (!states.has(st)) states.set(st, new Map());
    const cities = states.get(st);
    if (!cities.has(slug)) cities.set(slug, { slug, state: st, names: [], records: [] });
    const city = cities.get(slug);
    city.names.push(r.c);
    city.records.push(r);
  }
  for (const cities of states.values()) {
    for (const city of cities.values()) {
      city.name = mostCommon(city.names)[0];
      delete city.names;
      city.records.sort((a, b) => (b.d || '').localeCompare(a.d || ''));
      city.latest = city.records[0]?.d || '';
      city.indexed = city.records.length >= MIN_INDEXED_CITY;
    }
  }
  return states;
}

function gradeCounts(records) {
  const c = { A: 0, B: 0, C: 0, F: 0 };
  for (const r of records) if (c[r.vg] !== undefined) c[r.vg] += 1;
  return c;
}

function rowsTable(records, { showCity = false } = {}) {
  if (!records.length) return '<p class="mut">None in this set.</p>';
  const rows = records.map(r => `<tr><td><a href="${esc(restaurantPath(r.i, r.n))}">${esc(displayName(r.n))}</a><br><span class="mut">${esc(r.a || '')}${showCity ? `, ${esc(r.c)}` : ''}</span></td><td>${gradeBadge(r.vg, score(r))}</td><td>${esc(formatDate(r.d))}</td></tr>`).join('\n');
  return `<table><thead><tr><th>Restaurant</th><th>Grade</th><th>Inspected</th></tr></thead><tbody>\n${rows}\n</tbody></table>`;
}

const FAQ_HTML = `
<h2>About these grades</h2>
<p><strong>How is the grade worked out?</strong> Each inspection is scored 0 to 100 from the violations the inspector recorded, or from the health department's own score where it publishes one. The grade blends the last three inspections (60%, 30%, 10%): A is 90 or above, B 80 to 89, C 70 to 79 and F below 70. Signs of active pests or sewage make it an F whatever the score. <a href="/about/">More on the method</a>.</p>
<p><strong>Where does the data come from?</strong> Official inspection records published by the local health department, linked from every restaurant's page.</p>
<p><strong>How current is it?</strong> DineScores refreshes every week. Some departments post their reports days or weeks after the visit.</p>`;

function cityPage(city, generated) {
  const { name, state, records } = city;
  const sn = stateName(state);
  const total = records.length;
  const counts = gradeCounts(records);
  const sources = mostCommon(records.map(r => r.src)).slice(0, 3);
  const recent = records.slice(0, RECENT_ROWS);
  const rank = { F: 0, C: 1, B: 2, A: 3 };
  const lowest = records.filter(r => score(r) < 80 || r.vg === 'F')
    .sort((a, b) => (rank[a.vg] ?? 4) - (rank[b.vg] ?? 4) || score(a) - score(b)
      || (b.d || '').localeCompare(a.d || ''))
    .slice(0, LIST_ROWS);
  const best = records.filter(r => score(r) >= 100)
    .slice(0, LIST_ROWS);
  const oldest = records[records.length - 1]?.d || '';
  const path = cityPath(state, city.slug);
  const title = `${name}, ${state} Restaurant Health Inspections and Grades | DineScores`;
  const description = `Health inspection scores and grades for ${total} recently inspected restaurants in ${name}, ${sn}: ${pct(counts.A, total)}% grade A, ${pct(counts.F, total)}% F. Latest inspection ${formatDate(city.latest)}.`;
  const body = `
<h1>${esc(name)}, ${esc(state)} restaurant health inspections</h1>
<p class="mut">Updated ${esc(formatDate(generated))}${sources.length ? ` &middot; Source: ${sources.map(esc).join(', ')}` : ''}</p>
<div class="card">
<p>DineScores has ${total} recently inspected restaurants in ${esc(name)}, ${esc(sn)}, inspected between ${esc(formatDate(oldest))} and ${esc(formatDate(city.latest))}. ${counts.A} (${pct(counts.A, total)}%) earned an A, ${counts.B + counts.C} (${pct(counts.B + counts.C, total)}%) a B or C, and ${counts.F} (${pct(counts.F, total)}%) an F.</p>
<p><a href="/">Open the map</a> to see every ${esc(name)} restaurant, including older inspections, and filter by grade.</p>
</div>
<h2>Lowest recent scores in ${esc(name)}</h2>
${rowsTable(lowest)}
<h2>Perfect recent scores in ${esc(name)}</h2>
${rowsTable(best)}
<h2>Most recent inspections in ${esc(name)}</h2>
${rowsTable(recent)}
${FAQ_HTML}`;
  const structured = [{
    '@context': 'https://schema.org',
    '@type': 'CollectionPage',
    name: `${name}, ${state} restaurant health inspections`,
    description,
    url: SITE + path,
    dateModified: generated,
    about: { '@type': 'City', name, containedInPlace: { '@type': 'State', name: sn } },
    mainEntity: {
      '@type': 'ItemList',
      numberOfItems: recent.length,
      itemListElement: recent.slice(0, 20).map((r, i) => ({
        '@type': 'ListItem', position: i + 1, url: SITE + restaurantPath(r.i, r.n), name: displayName(r.n),
      })),
    },
  }];
  return renderPage({
    title, description, path, body, structured, noindex: !city.indexed,
    breadcrumbs: [
      { name: 'Home', path: '/' },
      { name: 'Inspections', path: '/inspections/' },
      { name: sn, path: statePath(state) },
      { name, path },
    ],
  });
}

function statePage(state, cities, generated) {
  const sn = stateName(state);
  const list = [...cities.values()];
  const total = list.reduce((n, c) => n + c.records.length, 0);
  const big = [...list].sort((a, b) => b.records.length - a.records.length).slice(0, 12);
  const all = [...list].sort((a, b) => a.name.localeCompare(b.name));
  const path = statePath(state);
  const title = `${sn} Restaurant Health Inspections by City | DineScores`;
  const description = `Restaurant health inspection scores and A to F grades in ${list.length} ${sn} cities and towns, from official health department records. Updated ${formatDate(generated)}.`;
  const body = `
<h1>${esc(sn)} restaurant health inspections</h1>
<p class="mut">Updated ${esc(formatDate(generated))} &middot; ${total} recently inspected restaurants in ${list.length} cities and towns</p>
<h2>Largest cities</h2>
<ul class="cols">${big.map(c => `<li><a href="${esc(cityPath(state, c.slug))}">${esc(c.name)}</a> <span class="mut">(${c.records.length})</span></li>`).join('')}</ul>
<h2>All ${esc(sn)} cities and towns</h2>
<ul class="cols">${all.map(c => `<li><a href="${esc(cityPath(state, c.slug))}">${esc(c.name)}</a></li>`).join('')}</ul>
${FAQ_HTML}`;
  return renderPage({
    title, description, path, body,
    breadcrumbs: [
      { name: 'Home', path: '/' },
      { name: 'Inspections', path: '/inspections/' },
      { name: sn, path },
    ],
  });
}

function indexPage(states, generated, total) {
  const codes = [...states.keys()].sort((a, b) => stateName(a).localeCompare(stateName(b)));
  const top = [];
  for (const cities of states.values()) top.push(...cities.values());
  top.sort((a, b) => b.records.length - a.records.length);
  const title = 'Restaurant Health Inspection Scores by State and City | DineScores';
  const description = `Look up restaurant health inspection scores and A to F grades from official health department records in ${codes.length} states and ${top.length} cities. Updated weekly.`;
  const body = `
<h1>Restaurant health inspections by state and city</h1>
<p class="mut">Updated ${esc(formatDate(generated))} &middot; ${total} recently inspected restaurants listed here, and about 376,000 on the map</p>
<h2>States</h2>
<ul class="cols">${codes.map(st => `<li><a href="${esc(statePath(st))}">${esc(stateName(st))}</a> <span class="mut">(${states.get(st).size} cities)</span></li>`).join('')}</ul>
<h2>Popular cities</h2>
<ul class="cols">${top.slice(0, 60).map(c => `<li><a href="${esc(cityPath(c.state, c.slug))}">${esc(c.name)}, ${esc(c.state)}</a></li>`).join('')}</ul>
${FAQ_HTML}`;
  return renderPage({
    title, description, path: '/inspections/', body,
    breadcrumbs: [{ name: 'Home', path: '/' }, { name: 'Inspections', path: '/inspections/' }],
  });
}

function aboutPage(generated, states) {
  const stateList = [...states.keys()].map(stateName).sort().join(', ');
  const title = 'How DineScores Grades Restaurant Health Inspections';
  const description = 'How DineScores turns official restaurant health inspection reports into A, B, C and F grades: the 0 to 100 score, the three-inspection blend, pest and sewage rules, sources and update schedule.';
  const body = `
<h1>How DineScores grades restaurants</h1>
<p class="mut">Updated ${esc(formatDate(generated))}</p>
<p>DineScores collects restaurant health inspection results from city, county and state health departments and shows them on one map with a simple letter grade, so you can check a restaurant before you go.</p>
<h2>The score</h2>
<p>Every inspection gets a score from 0 to 100. Where the health department publishes its own 0 to 100 score, DineScores uses it. Where it does not, the score is worked out from the violations the inspector recorded, weighted by how serious each one is: 5 points for a priority violation (one that can directly make someone sick, such as food held at the wrong temperature), 2 for a priority foundation item (such as missing thermometers or soap), and 1 for a core item (general upkeep and cleaning).</p>
<h2>The grade</h2>
<p>The grade blends a restaurant's last three inspections, 60% for the latest, 30% for the one before and 10% for the third, so one bad day does not define a place and a long record counts.</p>
<table><thead><tr><th>Grade</th><th>Blended score</th><th>Meaning</th></tr></thead><tbody>
<tr><td>${gradeBadge('A')}</td><td>90 to 100</td><td>Few or minor violations</td></tr>
<tr><td>${gradeBadge('B')}</td><td>80 to 89</td><td>Some violations worth a look</td></tr>
<tr><td>${gradeBadge('C')}</td><td>70 to 79</td><td>Several or serious violations</td></tr>
<tr><td>${gradeBadge('F')}</td><td>Below 70</td><td>Many serious violations, active pests or sewage, or a closure</td></tr>
</tbody></table>
<p>An inspector's note of live roaches, rodents or their droppings, or sewage, makes the grade an F whatever the score. Where a department closes a restaurant or fails an inspection, that also counts as an F.</p>
<h2>Where the data comes from</h2>
<p>Only official public inspection records, from the health departments that publish them. Each restaurant page links to the department's own report. Coverage today includes ${esc(stateList)}. See <a href="/inspections/">every state and city</a>.</p>
<h2>How often it updates</h2>
<p>Weekly. Some departments publish days or weeks after the inspection, and the site notes when an area's department is running late.</p>
<h2>What a grade is not</h2>
<p>A grade is a summary of what inspectors recorded on the days they visited. It is not a health department's own rating, a review of the food, or a guarantee. Read the full report for the details.</p>`;
  return renderPage({
    title, description, path: '/about/', body,
    breadcrumbs: [{ name: 'Home', path: '/' }, { name: 'How grades work', path: '/about/' }],
    structured: [{
      '@context': 'https://schema.org',
      '@type': 'FAQPage',
      mainEntity: [
        ['How does DineScores grade restaurants?', 'Each inspection is scored 0 to 100 from the health department score or the recorded violations. The grade blends the last three inspections (60/30/10): A is 90+, B 80-89, C 70-79, F below 70. Active pests or sewage make it an F.'],
        ['Where does DineScores get its data?', 'From official public restaurant inspection records published by city, county and state health departments.'],
        ['How often is DineScores updated?', 'Weekly.'],
      ].map(([q, a]) => ({ '@type': 'Question', name: q, acceptedAnswer: { '@type': 'Answer', text: a } })),
    }],
  });
}

function robotsTxt() {
  return `# DineScores: restaurant health inspection grades from official records.
# Search engines and AI assistants are welcome to read and cite these pages.
User-agent: *
Allow: /
Disallow: /api/

Sitemap: ${SITE}/sitemap.xml
`;
}

function sitemapIndex(generated) {
  const maps = ['/sitemap-pages.xml', ...RESTAURANT_SITEMAP_SHARDS.map(h => `/sitemaps/restaurants-${h}.xml`)];
  return `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${maps.map(p => `<sitemap><loc>${SITE}${p}</loc><lastmod>${generated}</lastmod></sitemap>`).join('\n')}
</sitemapindex>
`;
}

function pagesSitemap(states, generated) {
  const urls = [['/', generated], ['/about/', generated], ['/inspections/', generated]];
  for (const [st, cities] of states) {
    urls.push([statePath(st), generated]);
    for (const c of cities.values()) if (c.indexed) urls.push([cityPath(st, c.slug), c.latest || generated]);
  }
  return `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map(([p, d]) => `<url><loc>${esc(SITE + p)}</loc><lastmod>${d}</lastmod></url>`).join('\n')}
</urlset>
`;
}

function llmsTxt(states, generated, total) {
  const top = [];
  for (const cities of states.values()) top.push(...cities.values());
  top.sort((a, b) => b.records.length - a.records.length);
  const codes = [...states.keys()].sort((a, b) => stateName(a).localeCompare(stateName(b)));
  return `# DineScores

> DineScores (${SITE}) shows restaurant health inspection results from official city, county and state health department records across the United States, with an A, B, C or F grade for each restaurant. About 376,000 restaurants in ${codes.length} states, refreshed weekly (last update ${generated}).

Each restaurant has a page at ${SITE}/restaurant/<name>-<id> with its grade, latest inspection date, the violations recorded and its inspection history, plus a link to the health department's own report. City pages list a city's lowest and highest recent scores.

Grading: each inspection is scored 0 to 100 (the department's own score where published, otherwise computed from violations: 5 points per priority, 2 per priority foundation, 1 per core violation). The grade blends the last three inspections 60/30/10. A = 90-100, B = 80-89, C = 70-79, F = below 70. Active pests or sewage, a closure, or a failed inspection make it an F. DineScores is not affiliated with any health department.

## Key pages

- [How grades work](${SITE}/about/): method, sources, update schedule
- [Browse by state and city](${SITE}/inspections/)
- [Interactive map](${SITE}/): every restaurant, filterable by grade

## States

${codes.map(st => `- [${stateName(st)}](${SITE}${statePath(st)})`).join('\n')}

## Largest cities

${top.slice(0, 80).map(c => `- [${c.name}, ${c.state}](${SITE}${cityPath(c.state, c.slug)})`).join('\n')}
`;
}

function write(out, rel, content) {
  const file = path.join(out, rel);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content);
}

export function buildSeo({ dataFile, out }) {
  const { records, generated } = loadData(dataFile);
  const states = groupCities(records);
  const total = [...states.values()].reduce((n, cs) => n + [...cs.values()].reduce((m, c) => m + c.records.length, 0), 0);

  fs.rmSync(path.join(out, 'inspections'), { recursive: true, force: true });
  let pages = 0;
  for (const [st, cities] of states) {
    write(out, `${statePath(st)}index.html`, statePage(st, cities, generated));
    const index = {};
    for (const c of cities.values()) {
      write(out, `${cityPath(st, c.slug)}index.html`, cityPage(c, generated));
      index[c.slug] = c.name;
      pages += 1;
    }
    write(out, `${statePath(st)}cities.json`, JSON.stringify(index));
  }
  write(out, 'inspections/index.html', indexPage(states, generated, total));
  write(out, 'about/index.html', aboutPage(generated, states));
  write(out, 'robots.txt', robotsTxt());
  write(out, 'sitemap.xml', sitemapIndex(generated));
  write(out, 'sitemap-pages.xml', pagesSitemap(states, generated));
  write(out, 'llms.txt', llmsTxt(states, generated, total));
  return { states: states.size, cities: pages, records: total };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const summary = buildSeo({
    dataFile: arg('--data', 'public/data.js'),
    out: arg('--out', 'public'),
  });
  console.log(`SEO pages: ${summary.states} states, ${summary.cities} cities, ${summary.records} records`);
}
