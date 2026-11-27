/**
 * GET /restaurant/<name-slug>-<id> — a crawlable HTML page for one restaurant,
 * rendered from D1: grade, latest inspection, violations, history and a link
 * to the health department's own report. The map itself is a client-rendered
 * SPA that search engines and AI crawlers cannot read; these pages are what
 * they index (listed in /sitemaps/restaurants-<h>.xml).
 *
 * A bare id, or a stale name slug, 301s to the canonical URL. Edge cached like
 * the API (edgeCached): two indexed reads per miss.
 */
import { edgeCached, parseJson } from '../api/_utils.js';
import {
  SITE, esc, displayName, restaurantPath, statePath, cityPath, stateName,
  slugify, gradeBadge, formatDate, renderPage, withAnalytics, GRADE_WORDS,
} from '../_lib/seo.js';

const MAX_AGE = 21600; // six hours; data changes weekly
const HISTORY_ROWS = 20;
const VIOLATION_ROWS = 25;

export const onRequestGet = context => edgeCached(context, handleGet);

function html(body, status = 200, maxAge = MAX_AGE) {
  return new Response(body, {
    status,
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': maxAge > 0 ? `public, max-age=${maxAge}` : 'no-store',
    },
  });
}

function notFound(message) {
  return html(renderPage({
    title: 'Restaurant not found | DineScores',
    description: message,
    path: '/inspections/',
    noindex: true,
    body: `<h1>Restaurant not found</h1><p>${esc(message)}</p><p><a href="/inspections/">Browse by state and city</a> or <a href="/">search the map</a>.</p>`,
  }), 404, 0);
}

/** The state's city index (built from data.js), to link the city page only if it exists. */
async function cityIndex(env, request, state) {
  if (!env.ASSETS || !state) return null;
  try {
    const res = await env.ASSETS.fetch(new URL(`${statePath(state)}cities.json`, request.url));
    if (!res.ok || !(res.headers.get('Content-Type') || '').includes('json')) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function handleGet({ request, params, env }) {
  const slug = String(params.slug || '');
  const m = slug.match(/(?:^|-)([a-f0-9]{16})$/);
  if (!m) return notFound('That link does not point to a restaurant.');
  const id = m[1];
  if (!env.DB) {
    return html('<!DOCTYPE html><title>DineScores</title><p>Temporarily unavailable. Please try again shortly.</p>', 503, 0);
  }

  const row = await env.DB.prepare('SELECT * FROM restaurants WHERE id = ?').bind(id).first();
  if (!row) return notFound('We could not find that restaurant. It may have closed or been renamed.');

  const canonical = restaurantPath(row.id, row.name);
  const url = new URL(request.url);
  if (url.pathname !== canonical) {
    return new Response(null, {
      status: 301,
      headers: { Location: canonical, 'Cache-Control': `public, max-age=${MAX_AGE}` },
    });
  }

  const [{ results: history }, cities] = await Promise.all([
    env.DB.prepare(
      `SELECT inspection_date, risk_score, inspection_type, results, source_url
         FROM inspections WHERE restaurant_id = ?
        ORDER BY inspection_date DESC LIMIT ${HISTORY_ROWS}`
    ).bind(id).all(),
    cityIndex(env, request, row.state),
  ]);

  return html(withAnalytics(renderRestaurant(row, history || [], cities), env.VITE_GA_MEASUREMENT_ID));
}

export function renderRestaurant(row, history, cities) {
  const name = displayName(row.name);
  const state = String(row.state || '').toUpperCase();
  const sn = stateName(state);
  const city = row.city || '';
  const grade = ['A', 'B', 'C', 'F'].includes(row.vetted_grade) ? row.vetted_grade : '';
  const score = Number.isFinite(row.weighted_score) ? row.weighted_score : row.risk_score;
  const date = formatDate(row.inspection_date);
  const summaries = parseJson(row.summaries, []).slice(0, VIOLATION_ROWS);
  const path = restaurantPath(row.id, row.name);
  const place = [city, state].filter(Boolean).join(', ');
  const citySlug = slugify(city);
  const cityLink = cities && cities[citySlug] ? cityPath(state, citySlug) : null;

  const title = `${name}${place ? `, ${place}` : ''} Health Inspection${grade ? `: Grade ${grade}` : ''} | DineScores`;
  const description = grade
    ? `${name} at ${row.address || place} has a DineScores grade of ${grade} (${score}/100) from official health inspection records. Last inspected ${date}${summaries.length ? `, ${summaries.length} violation${summaries.length === 1 ? '' : 's'} noted` : ', no violations noted'}.`
    : `Health inspection history for ${name}${place ? ` in ${place}` : ''} from official health department records.`;

  const violations = summaries.length
    ? `<ul>${summaries.map(v => `<li>${esc(v.verbatim || v.text || '')}${v.severity === 'CRITICAL' ? ' <strong>(serious)</strong>' : ''}</li>`).join('')}</ul>`
    : '<p>No violations were recorded at the latest inspection.</p>';

  const historyTable = history.length
    ? `<table><thead><tr><th>Date</th><th>Score</th><th>Type</th><th>Result</th></tr></thead><tbody>${history.map(h => `<tr><td>${esc(formatDate(h.inspection_date))}</td><td>${Number.isFinite(h.risk_score) ? esc(h.risk_score) : ''}</td><td>${esc(h.inspection_type || '')}</td><td>${esc(h.results || '')}${h.source_url ? ` <a href="${esc(h.source_url)}" rel="nofollow noopener">report</a>` : ''}</td></tr>`).join('')}</tbody></table>`
    : '<p class="mut">No earlier inspections on file.</p>';

  const body = `
<h1>${esc(name)} health inspection</h1>
<p class="mut">${esc(row.address || '')}${row.address ? ', ' : ''}${esc(place)}${row.zip ? ` ${esc(row.zip)}` : ''}</p>
<div class="card">
<div class="big">${gradeBadge(grade, score)}</div>
<p>${grade ? `DineScores grade <strong>${esc(GRADE_WORDS[grade])}</strong>, ${(row.inspection_count || 1) > 1 ? `a blend of the last ${Math.min(row.inspection_count, 3)} inspections` : 'from its latest inspection'}.` : 'Not yet graded.'}${grade === 'F' && score >= 70 ? ' The grade is an F despite the score because the report notes pests, sewage, a closure or a failed inspection.' : ''} Latest inspection ${esc(date) || 'date unknown'}${row.source ? ` by ${esc(row.source)}` : ''}.</p>
<p><a href="/#r/${esc(row.id)}">Open on the map</a>${row.source_url ? ` &middot; <a href="${esc(row.source_url)}" rel="nofollow noopener">Official inspection report</a>` : ''}</p>
</div>
<h2>Violations at the latest inspection</h2>
${violations}
<h2>Inspection history</h2>
${historyTable}
<p class="mut"><a href="/about/">How DineScores grades work</a>${cityLink ? ` &middot; <a href="${esc(cityLink)}">More ${esc(city)} restaurants</a>` : ''}</p>`;

  const structured = [{
    '@context': 'https://schema.org',
    '@type': 'Restaurant',
    '@id': `${SITE}${path}#restaurant`,
    name,
    url: SITE + path,
    address: {
      '@type': 'PostalAddress',
      streetAddress: row.address || undefined,
      addressLocality: city || undefined,
      addressRegion: state || undefined,
      postalCode: row.zip || undefined,
      addressCountry: 'US',
    },
    ...(Number.isFinite(row.lat) && Number.isFinite(row.lng)
      ? { geo: { '@type': 'GeoCoordinates', latitude: row.lat, longitude: row.lng } }
      : {}),
    additionalProperty: [
      grade && { '@type': 'PropertyValue', name: 'DineScores health inspection grade', value: grade },
      Number.isFinite(score) && { '@type': 'PropertyValue', name: 'DineScores health inspection score', value: score, minValue: 0, maxValue: 100 },
      row.inspection_date && { '@type': 'PropertyValue', name: 'Latest health inspection date', value: row.inspection_date },
    ].filter(Boolean),
  }];

  const breadcrumbs = [
    { name: 'Home', path: '/' },
    { name: 'Inspections', path: '/inspections/' },
    ...(sn && state ? [{ name: sn, path: statePath(state) }] : []),
    ...(cityLink ? [{ name: city, path: cityLink }] : []),
    { name, path },
  ];

  return renderPage({
    title, description, path, body, structured, breadcrumbs, mapLink: `/#r/${row.id}`,
  });
}
