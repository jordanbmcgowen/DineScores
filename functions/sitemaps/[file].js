/**
 * GET /sitemaps/restaurants-<h>.xml — every restaurant page whose id starts
 * with hex digit <h>. Ids are hashes, so the 16 shards split the ~376k
 * restaurants evenly (~24k each, under the sitemap protocol's 50,000 URLs).
 * Listed by /sitemap.xml (built by scripts/seo/build_seo.mjs). One primary-key
 * range scan per shard, edge cached for a day.
 */
import { edgeCached } from '../api/_utils.js';
import { SITE, esc, restaurantPath } from '../_lib/seo.js';

const MAX_AGE = 86400;
const MAX_URLS = 50000;

export const onRequestGet = context => edgeCached(context, handleGet);

export async function handleGet({ params, env }) {
  const m = String(params.file || '').match(/^restaurants-([0-9a-f])\.xml$/);
  if (!m) return new Response('Not found', { status: 404 });
  if (!env.DB) return new Response('Unavailable', { status: 503, headers: { 'Retry-After': '3600' } });

  const lo = m[1];
  const hi = lo === 'f' ? 'g' : (parseInt(lo, 16) + 1).toString(16);
  const { results } = await env.DB.prepare(
    `SELECT id, name, inspection_date FROM restaurants
      WHERE id >= ? AND id < ? ORDER BY id LIMIT ${MAX_URLS}`
  ).bind(lo, hi).all();

  return new Response(renderSitemap(results || []), {
    headers: {
      'Content-Type': 'application/xml; charset=utf-8',
      'Cache-Control': `public, max-age=${MAX_AGE}`,
    },
  });
}

export function renderSitemap(rows) {
  const urls = rows
    .filter(r => r.id && r.name)
    .map(r => {
      const lastmod = /^\d{4}-\d{2}-\d{2}$/.test(r.inspection_date || '') ? `<lastmod>${r.inspection_date}</lastmod>` : '';
      return `<url><loc>${esc(SITE + restaurantPath(r.id, r.name))}</loc>${lastmod}</url>`;
    });
  return `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls.join('\n')}\n</urlset>\n`;
}
