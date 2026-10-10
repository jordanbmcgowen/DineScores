# DineScores — Restaurant Health Inspection Transparency

Multi-city restaurant health inspection scores with proprietary safety grading.
Shows weighted scores, grade badges (Safe/Evaluate/Avoid), infraction detection,
and violation summaries for ~50 metros and four whole states: NYC and New York
State, Chicago, San Francisco, San Jose, Los Angeles and San Diego counties,
Sacramento, Orange County, Seattle, Portland, Las Vegas, Houston, Dallas-Fort
Worth, Austin, San Antonio, Boston, Washington DC, Nashville, Memphis,
Omaha, Atlanta's Gwinnett County, all of Florida, Delaware and South Carolina,
and more (see *Data sources* below).

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│  Python Pipeline (dinescores_pipeline.py)         │
│  • Scrapes 9 sources: Socrata/CKAN APIs + portals  │
│  • Classifies violations, computes scores/grades  │
│  • Weekly via GitHub Actions                      │
└──────┬──────────────────────────┬────────────────┘
       │                          │
  Cloudflare D1              public/data.js
  (full dataset:             (embedded map data:
   restaurants +              most recent 1,000
   inspection history)        per city)
       │                          │
┌──────┴───────────┐              │
│ /api/* Functions │              │
│ • /restaurants   │              │
│   city/bbox/q    │              │
│ • /:id/history   │              │
│ • /cities        │              │
└──────┬───────────┘              │
       │                          │
┌──────┴──────────────────────────┴────────────────┐
│  React Frontend (Vite build → public/)            │
│  • MapLibre GL JS map with clustering             │
│  • Map paints from embedded data.js               │
│  • Inspection history via D1 API                  │
│  • Grade badges, filters, dark mode, responsive   │
└───────────────────────────────────────────────────┘
              │
     Cloudflare Pages (auto-deploys from main)
```

## Tech Stack

- **Backend**: Python pipeline → Cloudflare D1 (full dataset + history) + embedded `data.js` (map paint)
- **API**: Cloudflare Pages Functions (`functions/api/*`) querying D1
- **Frontend**: React (JSX) + Tailwind CSS (build-time) + MapLibre GL JS
- **Build**: Vite → outputs to `public/`
- **Hosting**: Cloudflare Pages (auto-deploys from `main`)
- **CI**: GitHub Actions weekly refresh of every source (see *Weekly refresh health* below — the DFW/Portland/Colorado/Utah portals and the Las Vegas API refuse GitHub runners, so those requests go through the site's own relay function)

---

## Quick Start

### 0. Basemap API key (CARTO)

The map's background tiles come from CARTO, which has required an API key
since August 2026; without one the tiles load with an "API KEY REQUIRED"
watermark. Request a free key (5M tiles/month, no account needed) at
https://carto.com/basemaps/apikey, then:

- **Production (Cloudflare Pages):** Workers & Pages → `dinescores` →
  Settings → Variables and Secrets → Add → name `VITE_CARTO_API_KEY`, value
  = the key, environment Production (add Preview too if you want preview
  deploys unwatermarked) → Save. The key is read at build time, so trigger a
  build afterwards (merge/push to `main`, or Deployments → Retry deployment).
- **Local dev:** put `VITE_CARTO_API_KEY=...` in `.env.local` (gitignored).

The key ends up in the browser bundle, as any tile key does; CARTO ties it
to the domain given when it was requested. Keep the CARTO/OpenStreetMap
attribution on the map: it is the condition of the free tier.

### 1. Install Dependencies

```bash
# Frontend
npm install

# Pipeline
pip install requests
```

### 2. Development

```bash
npm run dev     # Vite dev server on port 3000
```

### 3. Production Build

```bash
npm run build   # Outputs to public/ (build output is gitignored)
```

### 4. Deploy

Merge to `main` — Cloudflare Pages runs the build and deploys it
automatically (~2 min). Build artifacts are never committed; the only
committed file in `public/` besides `CNAME` is the pipeline-generated
`data.js`.

---

## Data Pipeline

```bash
# Full load (Socrata cities 2024+, DFW cities current year)
python dinescores_pipeline.py --mode full --cities chicago nyc sf dallas plano \
  --output-data-js public/data.js \
  --merge-existing-data-js public/data.js

# Weekly refresh (last 8 days per source, widened automatically from the
# source's last recorded inspection via --freshness-file; ALWAYS pass
# --merge-existing-data-js so the partial pull merges into the accumulated
# dataset instead of replacing it)
python dinescores_pipeline.py --mode weekly --cities chicago nyc sf dallas plano \
  --output-data-js public/data.js \
  --merge-existing-data-js public/data.js \
  --output-d1-sql /tmp/weekly.sql \
  --freshness-file data/source_freshness.json

# Test run (25 records per city)
python dinescores_pipeline.py --mode test
```

### Data sources & scraping notes

| Source | Method |
|--------|--------|
| Chicago / NYC / SF | Socrata open-data APIs, paginated. Set `SOCRATA_APP_TOKEN` env var for higher rate limits (optional). Fetched concurrently. |
| Austin (Travis County) | Socrata (`datahub.austintexas.gov`). Scores only (no violation text published); Census-geocoded. |
| Boston | CKAN datastore SQL API (`data.boston.gov`), updated daily. One row per violation; `*`/`**`/`***` levels map to severity. |
| Seattle (King County) | Socrata (`data.kingcounty.gov` dataset `r878-4sxa`; the county republished under this id in 2026 and the old `f29f-zza5` now requires a login), ~30 King County cities. Violation POINTS (lower = better) converted to 100-scale; RED = priority, BLUE = core. The new dataset publishes no coordinates, so new restaurants are Census-geocoded, and it formats addresses differently, so `data/king_county_address_map.json` (business id → stored address) keeps existing restaurants on their ids. |
| Dallas / Plano / Frisco (DFW) | MyHealthDepartment portal JSON search API in 7-day windows (auto-bisected when the ~225-record query cap is hit), then each inspection's public detail page is scraped for violation observations — they are rendered server-side in the HTML (or inline JS for Frisco), so no browser is needed. Frisco scores are demerit-based (lower = better) and are converted. The portal blocks GitHub runner IPs (the weekly job reaches it through `/api/relay`) and rate-bans detail pages — see *Weekly refresh health*. |
| Houston | Tyler `healthinspections.us` portal: session-based date-window searches (bisected at the 500-row cap); each inspection's detail page carries full ordinance text per violation in its tooltip markup. |
| Washington DC | Tyler portal (different template): monthly window searches; report pages publish OFFICIAL Priority/Priority Foundation/Core counts + observation text. DC-located records only (mobile-vendor commissary addresses in VA/MD are skipped). |
| Florida (statewide) | Florida DBPR CSV extracts for all seven districts — Miami, Tampa, Orlando, Jacksonville, Fort Lauderdale, St. Petersburg, and every other FL metro — with official High Priority/Intermediate/Basic counts. Extracts roll over each July 1 with the state fiscal year, so history accumulates weekly from FY start. |
| New York State | NYSDOH open data (`health.data.ny.gov`): every active permitted facility statewide with its latest inspection — Rochester, Syracuse, Albany, Yonkers, Long Island. Official critical/non-critical counts, violation text, and coordinates (typo'd source coords are repaired or re-geocoded). Excludes NYC (own richer DOHMH feed) and Erie County/Buffalo (independent system, no public feed). |
| Raleigh (Wake County) | County ArcGIS open-data service, updated daily: restaurants layer (with coordinates), inspections layer carrying the OFFICIAL North Carolina 0-100 sanitation score, and a violations layer with item text + point deductions (severity anchored to points assessed). |
| San Jose / Santa Clara County | County LIVES feed on Socrata (`data.sccgov.org`, three datasets joined: inspections, violations, businesses), refreshed daily. OFFICIAL 0-100 score plus the county's `critical` flag per violation and coordinates for every business; a red (closed) placard caps the grade at F and a yellow (conditional pass) placard at C. |
| San Diego County | County DEH public site (`sdfoodinfo.org`) JSON search, one form-encoded POST per 1,000 facilities for a date window; each facility carries its full inspection history with violations inline. OFFICIAL 0-100 score and A/B/C grade on routine inspections, the county's major/minor flag per violation, coordinates. Closures ("Ordered Closed"/"Self Closed") cap the grade at F; "No Access" visits are skipped. Routine and re-inspections only. |
| Omaha / Douglas County | County ArcGIS layer (one row per inspection since Dec 2025, with coordinates and the county's A-F rating) plus each inspection's public PDF report, parsed with the standard library for the inspector's P / PF / C totals and the OUT items; `risk = 100 - 5P - 2PF - C`. An unreadable PDF drops the inspection rather than shipping it clean (the county's endpoint 500s on ~2% of reports). |
| Orange County, CA (Anaheim, Santa Ana, Irvine, ...) | The county's MyHealthDepartment portal ignores every date filter and answers only its newest 225 inspections (about 1.7 days), so a separate workflow (`poll-orange-county.yml`) polls it every Monday-Saturday morning and keeps the last ten days in `data/orange_county_inspections.jsonl`; the Sunday refresh rebuilds data.js from that sidecar. The county publishes no numeric score, only Pass / Reinspection / Closed (mapped to 90 / 75 / 50); each inspection's printable PDF report is parsed with the standard library for its violations and the county's MAJ/MIN/OUT tags. An inspection ships once its printable has been read (after five days of failed retries it ships with the county's result alone). |
| San Antonio (Metro Health) | Tyler `healthinspections.us` portal (a different template from Houston's): one-day searches list every facility inspected that day (100-result cap, partitioned by zip when hit); the facility page carries each inspection's OFFICIAL 0-100 score (100 minus demerits) and violation text in four layouts. Census-geocoded. |
| Nashville, Memphis, Knoxville, Chattanooga | Tennessee Department of Health on the MyHealthDepartment portal, slug `tennessee`. Statewide queries exceed the portal's 225-row cap every weekday and its pagination is unstable, so each metro is fetched in "bucket mode": one-day windows x city-name search terms x purpose (Routine / Follow-Up), rows kept only when their city is in the metro's allowlist and the program is Food Service Establishment. OFFICIAL 100-point score in the search rows; observations from the detail pages. |
| Gwinnett County, GA (Atlanta suburbs) | GNR Health (Gwinnett, Newton, Rockdale) on the MyHealthDepartment portal: OFFICIAL 0-100 score and A/B/C/U grade, detail-page observations. Atlanta proper (Fulton, DeKalb, Cobb) is not on that portal. |
| Las Vegas (SNHD) | Southern Nevada Health District live JSON API (~18k permits across Clark County): official grade + demerits + coordinates per permit, violation descriptions with demerit values, and prior-inspection history. Demerit bands map exactly onto `risk = 100 - demerits` (A: 0-10, B: 11-20, C: 21-40). Permits with unreadable details are skipped, never recorded as clean. |
| Geocoding | Census Bureau batch geocoder (thousands of addresses per request), Nominatim fallback for stragglers. |

### Weekly refresh health

The Sunday workflow is failure-tolerant: a source that errors, blocks the
runner, or misses the time budget is skipped and keeps its existing records.
Three things keep those gaps visible and self-healing:

- `data/source_freshness.json` (maintained by `--freshness-file`) records each
  source's newest shipped inspection and last-run outcome. Weekly runs look
  back from that date (bounded at 400 days for open-data APIs/CSVs, 90 for
  scraped portals) instead of a flat 8 days, so a lost week, or a source
  that publishes quarterly, is refetched the next time it answers.
- The workflow's **Report source outcomes** step renders that file into the
  run summary and raises a warning annotation for any source that failed this
  run or whose newest inspection is older than 21 days.
- The run **fails** (after committing whatever did ship) when any source
  group was blocked, errored or skipped, so a quiet gap becomes a red run
  and a notification instead of a green job that shipped nothing.

Known constraints:

| Source | Constraint |
|--------|------------|
| MyHealthDepartment portal (Dallas, Plano, Frisco, Fort Worth, Tarrant County, Portland metro, Colorado Front Range, Utah County, Yolo) | Returns 403 to every GitHub-hosted runner type (Linux, macOS and Windows, probed 2026-09-07). The weekly job therefore sends these requests through the site's own relay function, `functions/api/relay.js` (`https://dinescores.com/api/relay?url=`), which fetches from Cloudflare's network — verified reachable 2026-10-10. The job authenticates with a GitHub OIDC token it mints itself, so no secret is needed; a static `RELAY_TOKEN` Pages variable + `PORTAL_RELAY_TOKEN` repo secret, or an HTTP proxy in `PORTAL_PROXY_URL`, are the alternatives. Detail pages are also rate-banned after a few hundred per ~15 minutes; the fetcher waits the ban out (`MHD_BAN_COOLDOWN` seconds, `MHD_BAN_COOLDOWNS` per run, default 16) within the fetch deadline, works jurisdictions stalest-first, and drops anything it could not read for the next run to pick up. |
| Las Vegas (SNHD) | The district's API answers 401 to a client that makes a few hundred requests in a few minutes (GitHub runners hit it after ~60 list pages). Requests go through the relay too, the detail phase is paced (`SNHD_DETAIL_WORKERS`, `SNHD_DETAIL_DELAY`), and a 401/429 pauses every worker (`SNHD_RATE_LIMIT_WAIT`) before retrying. |
| Austin | The source dataset runs weeks behind; the report notes the source's own newest row. |
| LA County | Publishes quarterly into the same ArcGIS hub items (the title's end date rolls forward); the fetcher re-resolves the items each run and the 400-day lookback reads the whole new quarter. |
| Detroit, Fairfax County | Publish in batches with several weeks of lag; the freshness lookback picks the batches up when they land. |

Run **Probe data sources** (Actions tab) to see which portals answer from
each runner type, directly and through the relay. A committed D1 seed
(`data/d1_seed*.sql.gz`) is loaded automatically by **Load D1 seeds** when
it lands on `main`.

Data hygiene: placeholder dates (NYC `1900-01-01` = not yet inspected) and
future-dated typos in source data are dropped; Chicago severity is bounded by
the official violation number ranges (1-29 risk factors, 30+ good retail
practices).

---

## Database (Cloudflare D1)

The full dataset (all restaurants + accumulated inspection history) lives in a
Cloudflare D1 SQL database, queried by the `/api/*` Pages Functions. The
embedded `data.js` remains the map's initial paint (most recent 1,000
restaurants per city); the API serves everything else and scales to hundreds
of cities.

**One-time setup (browser only, no local tools):**
1. Create an API token at dash.cloudflare.com → My Profile → API Tokens →
   Create Token → Custom token, with permissions **Account | D1 | Edit** and
   **Account | Cloudflare Pages | Read**.
2. Add two repo secrets (GitHub → Settings → Secrets and variables → Actions):
   `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` (account ID is in the
   right sidebar of any domain page on the Cloudflare dashboard).
3. Run the **Setup D1 Database** workflow (GitHub → Actions → Run workflow).
   It creates the database, loads the committed seed
   (`data/d1_seed.sql.gz`, ~54k restaurants), and commits a `wrangler.toml`
   with the D1 binding — the next deploy makes the API live.
4. Verify: `https://dinescores.com/api/cities`

After setup, the weekly refresh workflow updates D1 automatically (it skips
the D1 step silently if the secrets are absent).

**API endpoints** (GET responses are cached at Cloudflare's edge for an
hour; check `X-Edge-Cache: HIT|MISS` on a response):

| Endpoint | Purpose |
|----------|---------|
| `/api/restaurants?city=Dallas` | Restaurants in a city (also `bbox=w,s,e,n`, `q=name`, `grade=F`, `limit=`) |
| `/api/restaurants/{id}/history` | Full inspection history with violations |
| `/api/cities` | City index: counts, grade breakdown, bounding boxes |

### Pipeline Fields

The pipeline computes these fields for each restaurant:

| Field | Description |
|-------|-------------|
| `risk_score` | 0-100 score based on violation severity |
| `weighted_score` | Weighted average: 60% latest + 30% 2nd + 10% 3rd inspection |
| `vetted_grade` | A (90+, no bad words), B (80-89), C (70-79), F (<70 or bad words) |
| `infractions` | Array of detected categories: pests, temp, hygiene, equipment, docs |
| `violation_summaries` | Top 5 violations with category, severity, paraphrased text, verbatim |

---

## File Reference

| File | Purpose |
|------|---------|
| `dinescores_pipeline.py` | Multi-city data pipeline (fetch + grade + emit data.js / D1 SQL) |
| `src/` | React frontend source (JSX components) |
| `src/App.jsx` | Main app: map, sidebar, filters, bottom sheet |
| `src/api.js` | D1 API client (fail-soft helpers) |
| `src/components/GradeBadge.jsx` | Grade badge + shared grade colors/labels |
| `src/components/RestaurantMap.jsx` | MapLibre GL JS map with clustering |
| `src/components/InspectionModal.jsx` | Detail modal with summaries + history |
| `src/components/FilterBar.jsx` | City dropdown + grade/infraction filters |
| `src/components/BottomSheet.jsx` | Draggable mobile results sheet |
| `src/grading.js` | Client-side grading for fallback data |
| `functions/api/` | Cloudflare Pages Functions (D1-backed API) |
| `vite.config.js` | Vite build config (outputs to public/) |
| `.github/workflows/refresh-data.yml` | Weekly automated refresh |
| `.github/workflows/setup-database.yml` | One-time D1 bulk load |
| `.github/workflows/probe-sources.yml` | Manual check of which portals answer from GitHub runners |
| `public/data.js` | Auto-generated embedded dataset |
| `data/source_freshness.json` | Per-source newest inspection + last refresh outcome (drives the lookback window and the run summary) |
| `data/king_county_address_map.json` | King County business id → stored address, so the republished feed keeps restaurant ids stable |
