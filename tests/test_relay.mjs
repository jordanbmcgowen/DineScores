// Tests for functions/api/relay.js: GitHub OIDC verification (signature,
// issuer, audience, expiry, repository name AND ids), the static-token
// alternative, the host allowlist, redirect handling and header stripping.
// fetch is mocked for GitHub's JWKS and the portals. Run: node tests/test_relay.mjs
import { webcrypto } from 'node:crypto';
import { onRequest } from '../functions/api/relay.js';

const subtle = webcrypto.subtle;
const b64url = buf => Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
const enc = s => b64url(Buffer.from(s));

const { publicKey, privateKey } = await subtle.generateKey(
  { name: 'RSASSA-PKCS1-v1_5', modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: 'SHA-256' },
  true, ['sign', 'verify']);
const jwk = await subtle.exportKey('jwk', publicKey);
jwk.kid = 'test-kid'; jwk.alg = 'RS256'; jwk.use = 'sig';
delete jwk.key_ops; delete jwk.ext;

async function sign(payload, { kid = 'test-kid', key = privateKey } = {}) {
  const header = enc(JSON.stringify({ alg: 'RS256', typ: 'JWT', kid }));
  const body = enc(JSON.stringify(payload));
  const sig = await subtle.sign('RSASSA-PKCS1-v1_5', key, Buffer.from(`${header}.${body}`));
  return `${header}.${body}.${b64url(sig)}`;
}

const now = Math.floor(Date.now() / 1000);
const good = {
  iss: 'https://token.actions.githubusercontent.com', aud: 'dinescores-portal-relay',
  exp: now + 300, nbf: now - 10, iat: now - 10,
  repository: 'jordanbmcgowen/DineScores', repository_owner: 'jordanbmcgowen',
  repository_id: '1163315559', repository_owner_id: '244274424',
  ref: 'refs/heads/main', workflow: 'Refresh DineScores Data', event_name: 'schedule',
};

let upstreamCalls = [];
globalThis.fetch = async (url, init = {}) => {
  url = String(url);
  if (url === 'https://token.actions.githubusercontent.com/.well-known/jwks') {
    return new Response(JSON.stringify({ keys: [jwk] }), { headers: { 'content-type': 'application/json' } });
  }
  upstreamCalls.push({ url, method: init.method || 'GET', headers: Object.fromEntries(new Headers(init.headers)), body: init.body ? Buffer.from(init.body).toString() : null });
  if (url === 'https://inspections.myhealthdepartment.com/redirect-me') {
    return new Response('', { status: 302, headers: { location: 'https://evil.example/steal' } });
  }
  if (url === 'https://inspections.myhealthdepartment.com/hop') {
    return new Response('', { status: 302, headers: { location: '/dallas' } });
  }
  if (url.startsWith('https://inspections.myhealthdepartment.com/')) {
    return new Response('[{"ok":true}]', { status: 200, headers: { 'content-type': 'application/json', 'set-cookie': 'leak=1', 'content-encoding': 'gzip' } });
  }
  return new Response('nope', { status: 418 });
};

async function call({ token, url, method = 'GET', body, env = {} }) {
  const headers = { 'x-relay-token': token, 'cf-connecting-ip': '1.2.3.4', 'x-forwarded-for': '1.2.3.4', 'origin': 'https://inspections.myhealthdepartment.com' };
  if (body) headers['content-type'] = 'application/json';
  const req = new Request(`https://dinescores.com/api/relay?url=${encodeURIComponent(url)}`, { method, headers, body });
  const res = await onRequest({ request: req, env });
  return { status: res.status, text: await res.text(), headers: Object.fromEntries(res.headers) };
}

const results = [];
const check = (name, cond, detail) => { results.push(`${cond ? 'PASS' : 'FAIL'} ${name}${detail ? ' - ' + detail : ''}`); if (!cond) process.exitCode = 1; };

const target = 'https://inspections.myhealthdepartment.com/';
let r = await call({ token: await sign(good), url: target, method: 'POST', body: '{"task":"searchInspections"}' });
check('valid token, allowed host, POST passes through', r.status === 200 && r.text === '[{"ok":true}]', `${r.status} ${r.text}`);
check('upstream got the POST body', upstreamCalls.at(-1)?.body === '{"task":"searchInspections"}');
check('upstream did not get the token or CF headers', !('x-relay-token' in upstreamCalls.at(-1).headers) && !('cf-connecting-ip' in upstreamCalls.at(-1).headers) && !('x-forwarded-for' in upstreamCalls.at(-1).headers), JSON.stringify(upstreamCalls.at(-1).headers));
check('set-cookie and content-encoding stripped from response', !('set-cookie' in r.headers) && !('content-encoding' in r.headers) && r.headers['x-relay-upstream-status'] === '200');

r = await call({ token: await sign(good), url: 'https://example.com/x' });
check('disallowed host refused', r.status === 403 && /host not allowed/.test(r.text), `${r.status} ${r.text}`);

r = await call({ token: await sign(good), url: 'http://inspections.myhealthdepartment.com/' });
check('plain http target refused', r.status === 400, `${r.status} ${r.text}`);

r = await call({ token: '', url: target });
check('missing token refused', r.status === 403, `${r.status} ${r.text}`);

r = await call({ token: await sign({ ...good, repository: 'someone-else/DineScores' }), url: target });
check('fork repository refused', r.status === 403 && /wrong repository/.test(r.text), r.text);

r = await call({ token: await sign({ ...good, aud: 'other-audience' }), url: target });
check('wrong audience refused', r.status === 403 && /audience/.test(r.text), r.text);

r = await call({ token: await sign({ ...good, exp: now - 600 }), url: target });
check('expired token refused', r.status === 403 && /expired/.test(r.text), r.text);

r = await call({ token: await sign({ ...good, iss: 'https://evil.example' }), url: target });
check('wrong issuer refused', r.status === 403 && /issuer/.test(r.text), r.text);

const other = await subtle.generateKey({ name: 'RSASSA-PKCS1-v1_5', modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: 'SHA-256' }, true, ['sign', 'verify']);
r = await call({ token: await sign(good, { key: other.privateKey }), url: target });
check('token signed by another key refused', r.status === 403 && /bad signature/.test(r.text), r.text);

r = await call({ token: await sign(good, { kid: 'unknown-kid' }), url: target });
check('unknown kid refused', r.status === 403 && /unknown kid/.test(r.text), r.text);

const tampered = (await sign(good)).split('.');
tampered[1] = enc(JSON.stringify({ ...good, repository: 'x/y' }));
r = await call({ token: tampered.join('.'), url: target });
check('tampered payload refused', r.status === 403, r.text);

r = await call({ token: 'static-secret', url: target, env: { RELAY_TOKEN: 'static-secret' } });
check('static RELAY_TOKEN accepted when configured', r.status === 200, `${r.status} ${r.text}`);

r = await call({ token: 'static-secret', url: target, env: {} });
check('static token refused when not configured', r.status === 403, `${r.status}`);

r = await call({ token: await sign(good), url: target, env: { RELAY_ALLOWED_REPO: 'another/repo', RELAY_ALLOWED_REPO_ID: '42' } });
check('RELAY_ALLOWED_REPO override is honoured', r.status === 403 && /wrong repository/.test(r.text), r.text);

r = await call({ token: await sign({ ...good, repository_id: '999999999', repository_owner_id: '888888888' }), url: target });
check('re-registered namespace (same name, new ids) refused', r.status === 403 && /repository id/.test(r.text), r.text);

r = await call({ token: await sign({ ...good, repository_owner_id: '888888888' }), url: target });
check('wrong owner id refused', r.status === 403 && /owner id/.test(r.text), r.text);

r = await call({ token: await sign({ ...good, repository_id: 1163315559, repository_owner_id: 244274424 }), url: target });
check('numeric id claims accepted', r.status === 200, `${r.status} ${r.text}`);

r = await call({ token: await sign(good), url: target, env: { RELAY_ALLOWED_REPO: 'jordanbmcgowen/DineScores' } });
check('name override without an id is refused as misconfigured', r.status === 403 && /misconfigured/.test(r.text), r.text);

r = await call({ token: await sign({ ...good, repository: 'new/Name', repository_id: '777' }), url: target,
                 env: { RELAY_ALLOWED_REPO: 'new/Name', RELAY_ALLOWED_REPO_ID: '777' } });
check('moved repository works with both overrides', r.status === 200, `${r.status} ${r.text}`);

r = await call({ token: await sign(good), url: 'https://inspections.myhealthdepartment.com/redirect-me' });
check('upstream redirect off the allowlist is refused, not followed', r.status === 502 && !upstreamCalls.some(c => c.url.startsWith('https://evil.example')), `${r.status} ${r.text}`);

r = await call({ token: await sign(good), url: 'https://inspections.myhealthdepartment.com/hop' });
check('a redirect within the allowlist is followed', r.status === 200 && upstreamCalls.at(-1).url === 'https://inspections.myhealthdepartment.com/dallas', `${r.status} ${upstreamCalls.at(-1).url}`);

console.log(results.join('\n'));
