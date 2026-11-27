// Usage analytics (Google Analytics 4), off unless a measurement id is set.
//
// The id comes from VITE_GA_MEASUREMENT_ID, a Cloudflare Pages build-time
// variable like VITE_CARTO_API_KEY ("G-XXXXXXXXXX"); unset, nothing loads and
// every call below is a no-op, so dev builds and forks send nothing.
//
// What is sent, and what never is:
// - Page views, and named events for what people do on the map (searches,
//   area picks, grade/issue filters, "Low scores near me", restaurant
//   previews and full reports, sort, theme, share, directions).
// - A restaurant event carries the restaurant's public name, grade, city and
//   state: the same facts the site itself publishes.
// - NEVER the user's location. "Near me" and the GPS button report only that
//   they were used and whether a fix was found; no coordinates, no distances.
// - Search text only when it looks like a restaurant or place name (see
//   cleanSearchTerm): anything with an @, a long number or a street-address
//   shape is dropped, so a typed address or phone number never leaves.
// - Google signals and ad personalisation are off; GA4 does not store IPs.

const RAW_ID = ((import.meta.env && import.meta.env.VITE_GA_MEASUREMENT_ID) || '').trim();
export const GA_ID = /^G-[A-Z0-9]{4,}$/.test(RAW_ID) ? RAW_ID : '';

let started = false;

export function analyticsEnabled() {
  return !!GA_ID && typeof window !== 'undefined';
}

export function initAnalytics() {
  if (started || !analyticsEnabled()) return;
  started = true;
  window.dataLayer = window.dataLayer || [];
  // gtag must push the arguments object itself, not an array
  window.gtag = function gtag() { window.dataLayer.push(arguments); };
  window.gtag('js', new Date());
  window.gtag('config', GA_ID, {
    allow_google_signals: false,
    allow_ad_personalization_signals: false,
    // The report's #r/<id> hash is a shareable link, not a separate page
    page_location: window.location.origin + window.location.pathname,
  });
  const s = document.createElement('script');
  s.async = true;
  s.src = `https://www.googletagmanager.com/gtag/js?id=${encodeURIComponent(GA_ID)}`;
  document.head.appendChild(s);
}

// GA4 caps event parameter values at 100 characters
function clip(v) {
  if (typeof v === 'string') return v.slice(0, 100);
  if (typeof v === 'number' || typeof v === 'boolean') return v;
  return undefined;
}

export function cleanParams(params) {
  const out = {};
  for (const [k, v] of Object.entries(params || {})) {
    const c = clip(v);
    if (c !== undefined && c !== '') out[k] = c;
  }
  return out;
}

export function track(event, params) {
  if (!analyticsEnabled() || !started || typeof window.gtag !== 'function') return;
  try { window.gtag('event', event, cleanParams(params)); } catch { /* never break the app */ }
}

// Set once and on change: lets every report be split by light/dark
export function setUserProperties(props) {
  if (!analyticsEnabled() || !started || typeof window.gtag !== 'function') return;
  try { window.gtag('set', 'user_properties', cleanParams(props)); } catch { /* ignore */ }
}

// A restaurant record's public facts, for event parameters
export function restaurantParams(r) {
  if (!r) return {};
  return {
    restaurant_name: r.n || '',
    grade: r.vg || '',
    city: r.c || '',
    state: r.s || '',
  };
}

// Search text worth reporting: a restaurant, cuisine or place name. Returns
// '' for anything that could be personal (an email, a phone number, a
// street address) or too short to mean anything.
export function cleanSearchTerm(term) {
  const t = String(term || '').trim().replace(/\s+/g, ' ').toLowerCase();
  if (t.length < 2) return '';
  if (t.includes('@')) return '';
  if (/\d{6,}/.test(t.replace(/[\s().+-]/g, ''))) return ''; // a phone number (a zip is fine)
  if (/^\d+\s+\S+/.test(t)) return ''; // "123 main st"
  return t.slice(0, 60);
}
