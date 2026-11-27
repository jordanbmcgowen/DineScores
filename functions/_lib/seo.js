/**
 * Shared HTML for the crawlable pages (search engines and AI answer engines
 * cannot run the map, which is a client-rendered SPA).
 *
 * Used by two callers, so it must stay plain ES with no Workers or Node APIs:
 *   - scripts/seo/build_seo.mjs (Node, at build time): the static about,
 *     state and city pages, robots.txt, llms.txt and the page sitemap
 *   - functions/restaurant/[slug].js and functions/sitemaps/[file].js
 *     (Pages Functions, from D1): one page per restaurant and its sitemaps
 *
 * Exports no onRequest handler, so Pages routes nothing here.
 */

import { DEFAULT_GA_ID, LIVE_HOSTS } from './analytics_id.js';

export const SITE = 'https://dinescores.com';
export const SITE_NAME = 'DineScores';

export const STATE_NAMES = {
  AL: 'Alabama', AK: 'Alaska', AZ: 'Arizona', AR: 'Arkansas', CA: 'California',
  CO: 'Colorado', CT: 'Connecticut', DE: 'Delaware', DC: 'District of Columbia',
  FL: 'Florida', GA: 'Georgia', HI: 'Hawaii', ID: 'Idaho', IL: 'Illinois',
  IN: 'Indiana', IA: 'Iowa', KS: 'Kansas', KY: 'Kentucky', LA: 'Louisiana',
  ME: 'Maine', MD: 'Maryland', MA: 'Massachusetts', MI: 'Michigan',
  MN: 'Minnesota', MS: 'Mississippi', MO: 'Missouri', MT: 'Montana',
  NE: 'Nebraska', NV: 'Nevada', NH: 'New Hampshire', NJ: 'New Jersey',
  NM: 'New Mexico', NY: 'New York', NC: 'North Carolina', ND: 'North Dakota',
  OH: 'Ohio', OK: 'Oklahoma', OR: 'Oregon', PA: 'Pennsylvania',
  RI: 'Rhode Island', SC: 'South Carolina', SD: 'South Dakota', TN: 'Tennessee',
  TX: 'Texas', UT: 'Utah', VT: 'Vermont', VA: 'Virginia', WA: 'Washington',
  WV: 'West Virginia', WI: 'Wisconsin', WY: 'Wyoming',
};

export function stateName(code) {
  return STATE_NAMES[(code || '').toUpperCase()] || code || '';
}

export function esc(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/** JSON for a <script type="application/ld+json"> block: '<' can never close it. */
export function jsonLd(data) {
  return JSON.stringify(data).replace(/</g, '\\u003c');
}

export function slugify(text) {
  return String(text || '')
    .normalize('NFD').replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/['’]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80)
    .replace(/-+$/g, '');
}

/** Source names arrive upper-cased and .title()d ("Mcdonald'S"); tidy for display. */
export function displayName(name) {
  return String(name || '').replace(/([A-Za-z])'S\b/g, "$1's").replace(/\s{2,}/g, ' ').trim();
}

export function restaurantPath(id, name) {
  const slug = slugify(displayName(name));
  return `/restaurant/${slug ? slug + '-' : ''}${id}`;
}

export function statePath(state) {
  return `/inspections/${String(state).toLowerCase()}/`;
}

export function cityPath(state, city) {
  return `${statePath(state)}${slugify(city)}/`;
}

export const GRADE_WORDS = { A: 'A (90-100)', B: 'B (80-89)', C: 'C (70-79)', F: 'F (below 70)' };

export function gradeBadge(grade, score) {
  const g = ['A', 'B', 'C', 'F'].includes(grade) ? grade : '';
  const label = g ? `Grade ${g}` : 'Unrated';
  const num = Number.isFinite(score) ? `<span class="sc">${esc(score)}</span>` : '';
  return `<span class="g g-${g || 'x'}" title="${esc(label)}">${g || '?'}</span>${num}`;
}

export function formatDate(iso) {
  if (!iso || !/^\d{4}-\d{2}-\d{2}/.test(iso)) return '';
  const [y, m, d] = iso.slice(0, 10).split('-').map(Number);
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  return `${months[m - 1]} ${d}, ${y}`;
}

const CSS = `
:root{color-scheme:light dark;--fg:#0f172a;--mut:#475569;--bg:#fff;--card:#f8fafc;--line:#e2e8f0;--acc:#0d9488}
@media (prefers-color-scheme:dark){:root{--fg:#f1f5f9;--mut:#94a3b8;--bg:#0f172a;--card:#1e293b;--line:#334155;--acc:#2dd4bf}}
*{box-sizing:border-box}body{margin:0;font:16px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif;color:var(--fg);background:var(--bg)}
a{color:var(--acc)}header,main,footer{max-width:960px;margin:0 auto;padding:16px 20px}
header{display:flex;align-items:center;justify-content:space-between;gap:12px;border-bottom:1px solid var(--line)}
.brand{font-weight:800;font-size:20px;color:var(--fg);text-decoration:none;letter-spacing:-.02em}
.cta{background:var(--acc);color:#fff;padding:8px 14px;border-radius:999px;text-decoration:none;font-weight:600;font-size:14px;white-space:nowrap}
h1{font-size:28px;line-height:1.2;margin:18px 0 6px}h2{font-size:20px;margin:28px 0 10px}
.mut{color:var(--mut)}.crumbs{font-size:14px;color:var(--mut);margin-top:8px}.crumbs a{color:var(--mut)}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:4px 16px;margin:12px 0}
table{width:100%;border-collapse:collapse;font-size:15px}th,td{text-align:left;padding:8px 6px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:13px;color:var(--mut);font-weight:600}
.g{display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border-radius:7px;color:#fff;font-weight:800;font-size:14px}
.g-A{background:#059669}.g-B,.g-C{background:#d97706}.g-F{background:#dc2626}.g-x{background:#94a3b8}
.sc{margin-left:6px;font-weight:600}.big .g{width:56px;height:56px;font-size:30px;border-radius:14px}.big .sc{font-size:22px}
ul.cols{columns:3 200px;padding-left:18px}ul.cols li{margin:2px 0}
footer{border-top:1px solid var(--line);font-size:14px;color:var(--mut);margin-top:32px}
`;

/**
 * Google Analytics 4 tag for a server-rendered or static page, inserted
 * before </head>. `id` overrides the default stream (VITE_GA_MEASUREMENT_ID,
 * as for the map in src/analytics.js); anything that is not a G- id adds
 * nothing. The tag loads only on the live hosts, so a Pages preview reports
 * nothing. Same settings as the map: no Google signals, no ad
 * personalisation.
 */
export function withAnalytics(html, id = DEFAULT_GA_ID) {
  const ga = String(id || DEFAULT_GA_ID).trim();
  if (!/^G-[A-Z0-9]{4,}$/.test(ga)) return html;
  const tag = `<script>(function(){if(${JSON.stringify(LIVE_HOSTS)}.indexOf(location.hostname)<0)return;var n=navigator;if(n.globalPrivacyControl===true||n.doNotTrack==='1'||n.doNotTrack==='yes')return;window.dataLayer=window.dataLayer||[];window.gtag=function(){dataLayer.push(arguments);};gtag('js',new Date());gtag('config','${ga}',{allow_google_signals:false,allow_ad_personalization_signals:false});var s=document.createElement('script');s.async=true;s.src='https://www.googletagmanager.com/gtag/js?id=${ga}';document.head.appendChild(s);})();</script>
`;
  return html.replace('</head>', `${tag}</head>`);
}

/**
 * A complete HTML document. `breadcrumbs` is [{name, path}] from the root;
 * it renders visibly and as BreadcrumbList structured data.
 */
export function renderPage({
  title, description, path, body, structured = [], breadcrumbs = [],
  noindex = false, mapLink = '/',
}) {
  const url = SITE + path;
  const ld = [...structured];
  if (breadcrumbs.length) {
    ld.push({
      '@context': 'https://schema.org',
      '@type': 'BreadcrumbList',
      itemListElement: breadcrumbs.map((b, i) => ({
        '@type': 'ListItem', position: i + 1, name: b.name, item: SITE + b.path,
      })),
    });
  }
  const crumbs = breadcrumbs.length
    ? `<nav class="crumbs" aria-label="Breadcrumb">${breadcrumbs
      .map((b, i) => (i === breadcrumbs.length - 1
        ? esc(b.name)
        : `<a href="${esc(b.path)}">${esc(b.name)}</a>`))
      .join(' &rsaquo; ')}</nav>`
    : '';
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(title)}</title>
<meta name="description" content="${esc(description)}">
<link rel="canonical" href="${esc(url)}">
${noindex ? '<meta name="robots" content="noindex, follow">\n' : ''}<meta property="og:type" content="website">
<meta property="og:site_name" content="${SITE_NAME}">
<meta property="og:title" content="${esc(title)}">
<meta property="og:description" content="${esc(description)}">
<meta property="og:url" content="${esc(url)}">
<meta property="og:image" content="${SITE}/og-image.png">
<meta name="twitter:card" content="summary_large_image">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Cpath d='M16 2L4 8v8c0 7.18 5.12 13.88 12 16 6.88-2.12 12-8.82 12-16V8L16 2z' fill='%230d9488'/%3E%3Ctext x='16' y='21' text-anchor='middle' fill='white' font-size='13' font-weight='700' font-family='system-ui'%3EA%3C/text%3E%3C/svg%3E">
<style>${CSS}</style>
${ld.map(d => `<script type="application/ld+json">${jsonLd(d)}</script>`).join('\n')}
</head>
<body>
<header><a class="brand" href="/">DineScores</a><a class="cta" href="${esc(mapLink)}">Open the map</a></header>
<main>
${crumbs}
${body}
</main>
<footer>
<p>DineScores shows restaurant health inspection results from official city, county and state health department records, with a simple A, B, C or F grade. Not affiliated with any health department; always check the official report for the full record.</p>
<p><a href="/">Map</a> &middot; <a href="/inspections/">Browse by state and city</a> &middot; <a href="/about/">How grades work</a> &middot; <a href="/developers/">API and AI assistants</a> &middot; <a href="/privacy/">Privacy</a></p>
</footer>
</body>
</html>
`;
}
