// Fetch relay for the inspection portals that block GitHub Actions.
//
// The MyHealthDepartment portal answers 403 to every GitHub-hosted runner
// range, and the Southern Nevada Health District API has failed from them
// too, so the weekly refresh cannot reach DFW, the Portland metro, the
// Colorado Front Range, Utah County, Yolo or Las Vegas from CI. The block is
// scoped to GitHub's ranges rather than datacenter IPs in general, so any
// unblocked hop is enough — this Worker is the cheapest one, running on the
// account that already serves the site.
//
// A Worker cannot serve HTTP CONNECT, so this is not an HTTP proxy. The
// pipeline puts the target in a `url` query parameter and this forwards it,
// passing the portal's own status and body straight back so the caller's
// 403/429 retry ladders and detail-ban cooldowns behave exactly as they do
// on a direct request.
//
// Deploy and configure: see README.md in this directory.

// Only these hosts may be fetched. Without this the Worker is an open proxy
// that anyone who learns its URL can point at any host on the internet.
const ALLOWED_HOSTS = new Set([
  'inspections.myhealthdepartment.com',
  'www.southernnevadahealthdistrict.org',
]);

// Hop-by-hop and Cloudflare-added headers that must not be replayed upstream.
const STRIP_REQUEST_HEADERS = [
  'x-relay-token', 'host', 'cf-connecting-ip', 'cf-ipcountry', 'cf-ray',
  'cf-visitor', 'cf-worker', 'x-forwarded-for', 'x-forwarded-proto',
  'x-real-ip', 'connection', 'keep-alive', 'transfer-encoding', 'upgrade',
];

const deny = (status, message) =>
  new Response(`${message}\n`, { status, headers: { 'content-type': 'text/plain' } });

export default {
  async fetch(request, env) {
    // A missing RELAY_TOKEN is a misconfiguration, not a reason to run open.
    if (!env.RELAY_TOKEN) return deny(500, 'relay not configured: set the RELAY_TOKEN secret');
    if (request.headers.get('x-relay-token') !== env.RELAY_TOKEN) return deny(403, 'forbidden');

    const raw = new URL(request.url).searchParams.get('url');
    if (!raw) return deny(400, 'missing url parameter');

    let target;
    try {
      target = new URL(raw);
    } catch {
      return deny(400, 'malformed url parameter');
    }
    if (target.protocol !== 'https:') return deny(400, 'only https targets are allowed');
    if (!ALLOWED_HOSTS.has(target.hostname)) return deny(403, `host not allowed: ${target.hostname}`);

    const headers = new Headers(request.headers);
    for (const h of STRIP_REQUEST_HEADERS) headers.delete(h);

    const hasBody = !['GET', 'HEAD'].includes(request.method);
    let upstream;
    try {
      upstream = await fetch(target.toString(), {
        method: request.method,
        headers,
        body: hasBody ? await request.arrayBuffer() : undefined,
        redirect: 'follow',
      });
    } catch (e) {
      // 502 rather than a thrown error: the caller treats it as a failed
      // fetch and retries, which is the correct handling either way.
      return deny(502, `upstream fetch failed: ${e}`);
    }

    // Pass the portal's status through untouched. Cookies are dropped so one
    // caller's session never leaks to the next.
    const out = new Headers(upstream.headers);
    out.delete('set-cookie');
    out.delete('transfer-encoding');
    out.delete('content-encoding');
    return new Response(upstream.body, { status: upstream.status, headers: out });
  },
};
