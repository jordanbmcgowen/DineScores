/**
 * POST /mcp — the DineScores MCP server (Model Context Protocol, Streamable
 * HTTP transport, stateless). Lets Claude, ChatGPT, Gemini and any other MCP
 * client look up restaurant health inspection grades live.
 *
 * Read-only, no sign-in. Each tool call becomes a GET on the public API
 * (functions/api/v1/[[path]].js -> handleApi), so tool answers and REST
 * answers are the same records and share the edge cache. Stateless: no
 * session id is issued, every POST stands alone, and GET answers 405 (no
 * server-initiated stream), as the transport allows.
 *
 * `search` and `fetch` follow the shape ChatGPT's connectors and deep
 * research expect; the other tools are the descriptive ones every client
 * can use.
 */
import { handleApi } from './api/v1/[[path]].js';
import { ATTRIBUTION, API_VERSION, normalizeState } from './_lib/public_api.js';
import { checkRateLimit, limitFor } from './_lib/rate_limit.js';
import { SITE } from './_lib/seo.js';

export const SUPPORTED_PROTOCOL_VERSIONS = ['2025-11-25', '2025-06-18', '2025-03-26', '2024-11-05'];
const MAX_BODY_BYTES = 64 * 1024;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Accept, Authorization, Mcp-Session-Id, Mcp-Protocol-Version, Last-Event-ID',
  'Access-Control-Expose-Headers': 'Mcp-Session-Id, Mcp-Protocol-Version',
  'Access-Control-Max-Age': '86400',
};

const INSTRUCTIONS = `DineScores has restaurant health inspection grades from official US city, county and state health department records (about 376,000 restaurants, refreshed weekly).

Grades: A 90-100 (few or minor violations), B 80-89, C 70-79, F below 70. Active pests, sewage, a closure or a failed inspection make it an F whatever the score. The score blends the last three inspections 60/30/10.

How to use: find a restaurant with search_restaurants (name plus city or state), then get_restaurant for its violations and history. find_restaurants_near takes coordinates or a US street address; pass grades ["C","F"] for low scores nearby. lowest_scores_in_city lists a city's worst recent grades. get_coverage says which places are covered; if a city is missing, say DineScores does not cover it rather than guessing.

When answering: give the grade, the latest inspection date and the inspecting department, link the dinescores_url (and official_report_url when present), and say grades summarise what inspectors recorded on the days they visited; they are not an official rating or a review of the food.`;

const READ_ONLY = { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false };

const GRADE_ENUM = { type: 'string', enum: ['A', 'B', 'C', 'F'] };

export const TOOLS = [
  {
    name: 'search_restaurants',
    title: 'Search restaurants by name',
    description: 'Find restaurants by name and get each one\'s health inspection grade (A, B, C or F), score, latest inspection date and links. Add city and/or state whenever you know them; every word of the name must appear in the restaurant\'s name.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Restaurant name, e.g. "Joe\'s Pizza"' },
        city: { type: 'string', description: 'City, e.g. "Dallas"' },
        state: { type: 'string', description: 'US state name or two-letter code' },
        limit: { type: 'integer', minimum: 1, maximum: 25, description: 'Max results (default 10)' },
      },
      required: ['name'],
      additionalProperties: false,
    },
    annotations: { title: 'Search restaurants by name', ...READ_ONLY },
  },
  {
    name: 'get_restaurant',
    title: 'Get a restaurant\'s inspection report',
    description: 'One restaurant\'s full record: grade, score, the violations from its latest inspection (Serious / Moderate / Minor, with the inspector\'s notes), its inspection history and a link to the health department\'s own report.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string', description: 'Restaurant id from search_restaurants (16 hex characters)' } },
      required: ['id'],
      additionalProperties: false,
    },
    annotations: { title: 'Get a restaurant\'s inspection report', ...READ_ONLY },
  },
  {
    name: 'find_restaurants_near',
    title: 'Find restaurants near a location',
    description: 'Restaurants near a point or a US street address, nearest first, each with its grade and distance. Pass grades ["C","F"] and recent_only true for "low scores near me".',
    inputSchema: {
      type: 'object',
      properties: {
        latitude: { type: 'number', minimum: -90, maximum: 90 },
        longitude: { type: 'number', minimum: -180, maximum: 180 },
        address: { type: 'string', description: 'US street address, used when latitude/longitude are not given' },
        radius_miles: { type: 'number', minimum: 0.05, maximum: 10, description: 'Default 1' },
        grades: { type: 'array', items: GRADE_ENUM, description: 'Only these grades' },
        recent_only: { type: 'boolean', description: 'Only places inspected in the last 18 months' },
        limit: { type: 'integer', minimum: 1, maximum: 50, description: 'Max results (default 20)' },
      },
      additionalProperties: false,
    },
    annotations: { title: 'Find restaurants near a location', ...READ_ONLY },
  },
  {
    name: 'lowest_scores_in_city',
    title: 'Lowest health grades in a city',
    description: 'The restaurants with the lowest recent health inspection grades (F first, then C, lowest score first) in a city, from inspections in the last 18 months.',
    inputSchema: {
      type: 'object',
      properties: {
        city: { type: 'string' },
        state: { type: 'string', description: 'US state name or two-letter code (recommended: many city names repeat)' },
        limit: { type: 'integer', minimum: 1, maximum: 50, description: 'Max results (default 20)' },
      },
      required: ['city'],
      additionalProperties: false,
    },
    annotations: { title: 'Lowest health grades in a city', ...READ_ONLY },
  },
  {
    name: 'get_coverage',
    title: 'Which places DineScores covers',
    description: 'States and cities DineScores has inspection data for, with restaurant counts and the latest inspection date. Pass a state for every city in it.',
    inputSchema: {
      type: 'object',
      properties: { state: { type: 'string', description: 'US state name or two-letter code' } },
      additionalProperties: false,
    },
    annotations: { title: 'Which places DineScores covers', ...READ_ONLY },
  },
  {
    name: 'search',
    title: 'Search DineScores',
    description: 'Free-text search for restaurants, e.g. "Joe\'s Pizza Dallas TX" or "taco bell in Austin". Returns ids, titles and URLs; use fetch for the full inspection record.',
    inputSchema: {
      type: 'object',
      properties: { query: { type: 'string', description: 'Restaurant name, optionally followed by a city and state' } },
      required: ['query'],
      additionalProperties: false,
    },
    annotations: { title: 'Search DineScores', ...READ_ONLY },
  },
  {
    name: 'fetch',
    title: 'Fetch a DineScores record',
    description: 'The full inspection record for a restaurant id returned by search.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string', description: 'Restaurant id from search' } },
      required: ['id'],
      additionalProperties: false,
    },
    annotations: { title: 'Fetch a DineScores record', ...READ_ONLY },
  },
];

const TOOL_NAMES = new Set(TOOLS.map(t => t.name));

// ------------------------------------------------------------ HTTP entry

export const onRequestOptions = () => new Response(null, { status: 204, headers: CORS });

export function onRequestGet() {
  return new Response(JSON.stringify({
    name: 'DineScores MCP server',
    message: 'This is a Model Context Protocol endpoint. Add https://dinescores.com/mcp to your AI assistant as a custom connector (no sign-in). It speaks Streamable HTTP over POST; there is no server-initiated stream.',
    docs: `${SITE}/developers/`,
  }), {
    status: 405,
    headers: { 'Content-Type': 'application/json; charset=utf-8', Allow: 'POST, OPTIONS', 'Cache-Control': 'no-store', ...CORS },
  });
}

export const onRequestDelete = () => new Response(null, { status: 405, headers: { Allow: 'POST, OPTIONS', ...CORS } });

function reply(body, status = 200, extra = {}) {
  if (body === null) return new Response(null, { status: 202, headers: { ...CORS, ...extra } });
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', ...CORS, ...extra },
  });
}

const rpcError = (id, code, message) => ({ jsonrpc: '2.0', id: id ?? null, error: { code, message } });

export async function onRequestPost(context) {
  const { request, env } = context;
  const limited = await checkRateLimit(request, env, {
    scope: 'mcp', perMinute: limitFor(env, 'MCP_RATE_PER_MIN', 300),
  });
  if (limited) {
    return reply(rpcError(null, -32000, `Rate limited. Try again in ${limited.retryAfter} seconds.`), 429,
      { 'Retry-After': String(limited.retryAfter) });
  }

  const text = await request.text();
  if (text.length > MAX_BODY_BYTES) return reply(rpcError(null, -32600, 'Request too large'), 413);
  let message;
  try {
    message = JSON.parse(text);
  } catch {
    return reply(rpcError(null, -32700, 'Parse error'), 400);
  }

  if (Array.isArray(message)) {
    if (!message.length || message.length > 20) return reply(rpcError(null, -32600, 'Invalid batch'), 400);
    const out = (await Promise.all(message.map(m => handleMessage(m, context)))).filter(Boolean);
    return reply(out.length ? out : null);
  }
  const out = await handleMessage(message, context);
  return reply(out);
}

// ------------------------------------------------------------ JSON-RPC

export async function handleMessage(msg, context) {
  if (!msg || typeof msg !== 'object' || msg.jsonrpc !== '2.0' || typeof msg.method !== 'string') {
    // A response or junk: nothing to answer unless it carried an id.
    return msg && msg.id !== undefined && !('result' in msg) && !('error' in msg)
      ? rpcError(msg.id, -32600, 'Invalid request') : null;
  }
  const isNotification = msg.id === undefined || msg.id === null;
  if (isNotification) return null;
  const { id, method, params = {} } = msg;

  switch (method) {
    case 'initialize': {
      const asked = params && params.protocolVersion;
      const protocolVersion = SUPPORTED_PROTOCOL_VERSIONS.includes(asked) ? asked : SUPPORTED_PROTOCOL_VERSIONS[0];
      return {
        jsonrpc: '2.0', id,
        result: {
          protocolVersion,
          capabilities: { tools: { listChanged: false } },
          serverInfo: { name: 'dinescores', title: 'DineScores', version: API_VERSION, websiteUrl: SITE },
          instructions: INSTRUCTIONS,
        },
      };
    }
    case 'ping':
      return { jsonrpc: '2.0', id, result: {} };
    case 'tools/list':
      return { jsonrpc: '2.0', id, result: { tools: TOOLS } };
    case 'tools/call': {
      const name = params && params.name;
      if (!TOOL_NAMES.has(name)) return rpcError(id, -32602, `Unknown tool: ${name}`);
      const args = (params.arguments && typeof params.arguments === 'object') ? params.arguments : {};
      try {
        return { jsonrpc: '2.0', id, result: await callTool(name, args, context) };
      } catch (e) {
        console.error('mcp tool error', name, e && e.message);
        return { jsonrpc: '2.0', id, result: toolError('Something went wrong looking that up. Please try again.') };
      }
    }
    case 'resources/list':
      return { jsonrpc: '2.0', id, result: { resources: [] } };
    case 'prompts/list':
      return { jsonrpc: '2.0', id, result: { prompts: [] } };
    default:
      return rpcError(id, -32601, `Method not found: ${method}`);
  }
}

// ------------------------------------------------------------ tools

function toolResult(payload) {
  return {
    content: [{ type: 'text', text: JSON.stringify(payload) }],
    structuredContent: payload,
  };
}

function toolError(message) {
  return { content: [{ type: 'text', text: message }], isError: true };
}

/** GET a public API path through the shared, edge-cached handler. */
export async function apiGet(context, path, params = {}) {
  const origin = new URL(context.request.url).origin;
  const url = new URL(path, origin);
  for (const key of Object.keys(params).sort()) {
    const v = params[key];
    if (v !== undefined && v !== null && v !== '') url.searchParams.set(key, String(v));
  }
  const res = await handleApi({ ...context, request: new Request(url.toString(), { method: 'GET' }) });
  let body = null;
  try {
    body = await res.json();
  } catch {
    body = null;
  }
  return { status: res.status, body };
}

const failText = ({ status, body }) =>
  (body && body.message) || (status === 503 ? 'The DineScores database is temporarily unavailable.' : `Request failed (${status}).`);

export async function callTool(name, args, context) {
  switch (name) {
    case 'search_restaurants': {
      const r = await apiGet(context, '/api/v1/restaurants/search', {
        name: args.name, city: args.city, state: args.state, limit: args.limit,
      });
      return r.status === 200 ? toolResult(r.body) : toolError(failText(r));
    }
    case 'get_restaurant': {
      const r = await apiGet(context, `/api/v1/restaurants/${encodeURIComponent(String(args.id || '').trim())}`);
      return r.status === 200 ? toolResult(r.body) : toolError(failText(r));
    }
    case 'find_restaurants_near': {
      const hasPoint = Number.isFinite(args.latitude) && Number.isFinite(args.longitude);
      if (!hasPoint && !args.address) return toolError('Give latitude and longitude, or a US street address.');
      const r = await apiGet(context, '/api/v1/restaurants/near', {
        lat: hasPoint ? args.latitude : undefined,
        lng: hasPoint ? args.longitude : undefined,
        address: hasPoint ? undefined : args.address,
        radius_miles: args.radius_miles,
        grade: Array.isArray(args.grades) ? args.grades.join(',') : args.grades,
        since_months: args.recent_only ? 18 : undefined,
        limit: args.limit,
      });
      return r.status === 200 ? toolResult(r.body) : toolError(failText(r));
    }
    case 'lowest_scores_in_city': {
      const r = await apiGet(context, '/api/v1/restaurants/lowest', { city: args.city, state: args.state, limit: args.limit });
      return r.status === 200 ? toolResult(r.body) : toolError(failText(r));
    }
    case 'get_coverage': {
      const r = await apiGet(context, '/api/v1/coverage', { state: args.state });
      return r.status === 200 ? toolResult(r.body) : toolError(failText(r));
    }
    case 'search':
      return freeTextSearch(String(args.query || ''), context);
    case 'fetch':
      return fetchRecord(String(args.id || ''), context);
    default:
      return toolError(`Unknown tool: ${name}`);
  }
}

/**
 * "Joe's Pizza Dallas TX", "taco bell in austin, texas": split off a trailing
 * state, then try the whole text as a name, then the last one or two words as
 * the city. At most three queries.
 */
export function parseFreeText(query) {
  let text = query.replace(/[’‘]/g, "'").replace(/\s+/g, ' ').trim();
  let state = '';
  const parts = text.split(/\s*,\s*|\s+/);
  for (const take of [2, 1]) {
    if (parts.length > take) {
      const st = normalizeState(parts.slice(-take).join(' '));
      if (st) {
        state = st;
        parts.splice(-take, take);
        break;
      }
    }
  }
  text = parts.join(' ');
  const inMatch = text.match(/^(.*\S)\s+(?:in|near|at)\s+(.+)$/i);
  if (inMatch) return [{ name: inMatch[1], city: inMatch[2], state }];
  const words = text.split(' ');
  const tries = [{ name: text, city: '', state }];
  for (const k of [1, 2]) {
    if (words.length > k) tries.push({ name: words.slice(0, -k).join(' '), city: words.slice(-k).join(' '), state });
  }
  return tries;
}

async function freeTextSearch(query, context) {
  if (query.trim().length < 2) return toolError('Give a restaurant name to search for.');
  let results = [];
  for (const t of parseFreeText(query)) {
    const r = await apiGet(context, '/api/v1/restaurants/search', { name: t.name, city: t.city, state: t.state, limit: 10 });
    if (r.status === 503) return toolError(failText(r));
    if (r.status === 200 && r.body.results.length) {
      results = r.body.results;
      break;
    }
  }
  const payload = {
    results: results.map(x => ({
      id: x.id,
      title: `${x.name}, ${[x.address, x.city, x.state].filter(Boolean).join(', ')}: grade ${x.grade || 'not rated'}${x.latest_inspection_date ? `, inspected ${x.latest_inspection_date}` : ''}`,
      url: x.dinescores_url,
    })),
  };
  return { content: [{ type: 'text', text: JSON.stringify(payload) }], structuredContent: payload };
}

async function fetchRecord(id, context) {
  const r = await apiGet(context, `/api/v1/restaurants/${encodeURIComponent(id.trim())}`);
  if (r.status !== 200) return toolError(failText(r));
  const x = r.body.restaurant;
  const lines = [
    `${x.name}`,
    `${[x.address, x.city, x.state, x.zip].filter(Boolean).join(', ')}`,
    `DineScores grade: ${x.grade || 'not rated'}${x.score !== null ? ` (score ${x.score}/100)` : ''}. ${x.grade_meaning}.`,
    x.grade_note || '',
    `Latest inspection: ${x.latest_inspection_date || 'unknown'}${x.inspected_by ? ` by ${x.inspected_by}` : ''}. Inspections on file: ${x.inspections_on_file}.`,
    x.official_report_url ? `Official report: ${x.official_report_url}` : '',
    '',
    x.violations.length ? 'Violations at the latest inspection:' : 'No violations recorded at the latest inspection.',
    ...x.violations.map(v => `- [${v.severity}] ${v.finding}${v.inspector_notes && v.inspector_notes !== v.finding ? `: ${v.inspector_notes}` : ''}`),
    '',
    x.inspection_history.length ? 'Inspection history:' : '',
    ...x.inspection_history.map(h => `- ${h.date}: score ${h.score ?? 'n/a'}${h.type ? `, ${h.type}` : ''}${h.result ? `, ${h.result}` : ''}`),
    '',
    `Source: ${ATTRIBUTION.source}`,
    ATTRIBUTION.disclaimer,
  ].filter((l, i, a) => l !== '' || (a[i - 1] !== '' && i > 0));
  const doc = {
    id: x.id,
    title: `${x.name} health inspection, ${[x.city, x.state].filter(Boolean).join(', ')}`,
    text: lines.join('\n').trim(),
    url: x.dinescores_url,
    metadata: {
      grade: x.grade, score: x.score, latest_inspection_date: x.latest_inspection_date,
      inspected_by: x.inspected_by, official_report_url: x.official_report_url,
    },
  };
  return { content: [{ type: 'text', text: JSON.stringify(doc) }], structuredContent: doc };
}
