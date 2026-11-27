# Public API and MCP server (2026-11-27)

DineScores answers AI assistants and apps directly instead of being scraped:

- **MCP server**: `https://dinescores.com/mcp` (`functions/mcp.js`). Streamable
  HTTP, stateless, no auth, JSON responses only (GET answers 405, as the
  transport allows). Tools: `search_restaurants`, `get_restaurant`,
  `find_restaurants_near`, `lowest_scores_in_city`, `get_coverage`, plus
  `search` / `fetch` in the shape ChatGPT deep research expects. Every tool
  is annotated read-only with a title (a directory requirement).
- **JSON API**: `https://dinescores.com/api/v1` (`functions/api/v1/[[path]].js`,
  logic in `functions/_lib/public_api.js`, OpenAPI in `functions/_lib/openapi.js`
  served at `/api/v1/openapi.json`).
- **Pages**: `/developers/` (docs and terms) and `/privacy/`, built by
  `scripts/seo/build_seo.mjs`; `llms.txt` points assistants at both.

Each MCP tool is a GET on the API through `handleApi`, so both share the edge
cache. Every API answer carries an `attribution` block (source, disclaimer,
terms). Tests: `node tests/test_public_api.mjs` (node:sqlite stands in for D1).

## Cost and limits

- Queries are bounded: city lookups use `idx_restaurants_city` (cities are
  matched as `IN (title-cased, word-capitalised, as typed)` so the index stays
  usable), nearby uses a lat/lng box on `idx_restaurants_geo` (max 10 miles,
  2,000 rows scanned), ids the primary key. A name search with no city or
  state scans the table (~376k rows read, ~$0.0004 on Workers Paid) and is
  capped at 25 rows; the edge cache absorbs repeats.
- Rate limits (`functions/_lib/rate_limit.js`): 60/min per IP for the API,
  300/min for MCP (ChatGPT and Claude call from a few shared server IPs).
  Override with Pages variables `PUBLIC_API_RATE_PER_MIN` / `MCP_RATE_PER_MIN`.
  The in-memory window is per isolate, so it is a brake, not a quota. For a
  network-wide limit, add a Workers Rate Limiting binding named
  `API_RATE_LIMITER` (wrangler.toml `[[ratelimits]]`); the code uses it when
  present and falls back otherwise.
- `address=` on `near` is geocoded by the US Census Bureau's free geocoder
  (no key; only the address text is sent; cached a day).

## Connecting it (no listing needed)

- Claude: Settings > Connectors > Add custom connector > `https://dinescores.com/mcp`.
- ChatGPT: Settings > Apps & Connectors > Advanced > Developer mode, then
  Create, URL `https://dinescores.com/mcp`, authentication None.
- Claude Code: `claude mcp add --transport http dinescores https://dinescores.com/mcp`.
- Gemini CLI / Cursor / VS Code: add a remote HTTP server with the same URL.
- A ChatGPT custom GPT can import `https://dinescores.com/api/v1/openapi.json`
  as an Action.

## Directory listings (not submitted; owner decision)

Nothing has been submitted anywhere. What each needs:

| Directory | Where | Needs from the owner |
|---|---|---|
| Official MCP Registry (feeds many clients) | `mcp-publisher publish docs/mcp/server.json` | Generate an Ed25519 key, serve the public key at `https://dinescores.com/.well-known/mcp-registry-auth`, `mcp-publisher login http --domain dinescores.com` |
| Claude Connectors Directory | claude.ai/directory/manage > Submit new > MCP connector | Paid Claude plan, icon, support contact; docs URL `/developers/`, privacy URL `/privacy/` |
| ChatGPT plugin/app directory | OpenAI Platform dashboard > Plugins | Individual or business verification, domain token at `/.well-known/openai-apps-challenge`, terms URL (`/developers/#terms`), 5 positive and 3 negative test prompts, a short walkthrough video |
| Gemini | No consumer directory; Gemini CLI and Gemini Enterprise take the URL directly | Nothing |
| Smithery, Glama, mcp.so | Indexes; most pick up the official registry | Optional |

### Listing copy

**Name**: DineScores

**Short description** (under 100 characters): US restaurant health inspection
grades (A/B/C/F) from official health department records.

**Long description**: Check a restaurant before you go. DineScores collects
health inspection results from official city, county and state health
departments across the US (over 400,000 restaurants, refreshed weekly) and
gives each an A, B, C or F grade. Ask for a restaurant's grade, what the
inspector found, its inspection history, the lowest scores in a city, or low
scores near an address. Every answer links the health department's own
report. Read-only, no sign-in.

**Test prompts (positive)**:
1. What's the health inspection grade for Pecan Lodge in Dallas?
2. What violations did the inspector find at the latest inspection of [a restaurant from step 1]?
3. Which restaurants near 1500 Marilla St, Dallas TX have low health scores?
4. What are the lowest-graded restaurants in Austin, Texas right now?
5. Does DineScores cover restaurants in Montana?

**Test prompts (negative, the connector should not be used or should say
it does not cover it)**:
1. What's the best pizza in Dallas? (a review question, not an inspection one)
2. Health grade for a restaurant in London, UK (not covered)
3. Book a table at Pecan Lodge (no write actions)
