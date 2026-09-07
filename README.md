# DineScores — Restaurant Health Inspection Transparency

Multi-city restaurant health inspection scores with proprietary safety grading.
Shows weighted scores, grade badges (Safe/Evaluate/Avoid), infraction detection,
and violation summaries for Chicago, NYC, San Francisco, and DFW metro.

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
- **CI**: GitHub Actions weekly refresh of every source (see *Weekly refresh health* below — the DFW/Portland/Colorado/Utah portals block GitHub runner IPs and need `PORTAL_PROXY_URL`)

---

## Quick Start

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
| Dallas / Plano / Frisco (DFW) | MyHealthDepartment portal JSON search API in 7-day windows (auto-bisected when the ~225-record query cap is hit), then each inspection's public detail page is scraped for violation observations — they are rendered server-side in the HTML (or inline JS for Frisco), so no browser is needed. Frisco scores are demerit-based (lower = better) and are converted. The portal blocks GitHub runner IPs and rate-bans detail pages — see *Weekly refresh health*. |
| Houston | Tyler `healthinspections.us` portal: session-based date-window searches (bisected at the 500-row cap); each inspection's detail page carries full ordinance text per violation in its tooltip markup. |
| Washington DC | Tyler portal (different template): monthly window searches; report pages publish OFFICIAL Priority/Priority Foundation/Core counts + observation text. DC-located records only (mobile-vendor commissary addresses in VA/MD are skipped). |
| Florida (statewide) | Florida DBPR CSV extracts for all seven districts — Miami, Tampa, Orlando, Jacksonville, Fort Lauderdale, St. Petersburg, and every other FL metro — with official High Priority/Intermediate/Basic counts. Extracts roll over each July 1 with the state fiscal year, so history accumulates weekly from FY start. |
| New York State | NYSDOH open data (`health.data.ny.gov`): every active permitted facility statewide with its latest inspection — Rochester, Syracuse, Albany, Yonkers, Long Island. Official critical/non-critical counts, violation text, and coordinates (typo'd source coords are repaired or re-geocoded). Excludes NYC (own richer DOHMH feed) and Erie County/Buffalo (independent system, no public feed). |
| Raleigh (Wake County) | County ArcGIS open-data service, updated daily: restaurants layer (with coordinates), inspections layer carrying the OFFICIAL North Carolina 0-100 sanitation score, and a violations layer with item text + point deductions (severity anchored to points assessed). |
| Las Vegas (SNHD) | Southern Nevada Health District live JSON API (~18k permits across Clark County): official grade + demerits + coordinates per permit, violation descriptions with demerit values, and prior-inspection history. Demerit bands map exactly onto `risk = 100 - demerits` (A: 0-10, B: 11-20, C: 21-40). Permits with unreadable details are skipped, never recorded as clean. |
| Geocoding | Census Bureau batch geocoder (thousands of addresses per request), Nominatim fallback for stragglers. |

### Weekly refresh health

The Sunday workflow is failure-tolerant: a source that errors, blocks the
runner, or misses the time budget is skipped and keeps its existing records,
so the job stays green even when half the sources shipped nothing. Two
things keep those gaps visible and self-healing:

- `data/source_freshness.json` (maintained by `--freshness-file`) records each
  source's newest shipped inspection and last-run outcome. Weekly runs look
  back from that date (up to 45 days for open-data APIs, 21 for scraped
  portals) instead of a flat 8 days, so a lost week is refetched the next
  time the source answers.
- The workflow's **Report source outcomes** step renders that file into the
  run summary and raises a warning annotation for any source that failed this
  run or whose newest inspection is older than 21 days.

Known constraints:

| Source | Constraint |
|--------|------------|
| MyHealthDepartment portal (Dallas, Plano, Frisco, Fort Worth, Tarrant County, Portland metro, Colorado Front Range, Utah County, Yolo) | Returns 403 to every GitHub-hosted runner type (Linux, macOS and Windows, probed 2026-09-07). Refreshing these metros needs an unblocked egress: set the `PORTAL_PROXY_URL` repo secret (`http://user:pass@host:port`, a residential/ISP proxy) or run the pipeline locally. Detail pages are also rate-banned after a few hundred per ~15 minutes; the fetcher waits the ban out (`MHD_BAN_COOLDOWN` seconds, `MHD_BAN_COOLDOWNS` per run) within the fetch deadline instead of dropping the metro. |
| Las Vegas (SNHD) | The list endpoint has failed from runners on some Sundays; failures now log their HTTP status, and `PORTAL_PROXY_URL` applies here too. |
| Austin | The source dataset stopped updating on 2026-05-22. |
| LA County | Publishes fiscal-year CSV extracts as new hub items; the fetcher looks up the newest file on the county's ArcGIS hub each run. |
| Detroit, Fairfax County | Publish in batches with several weeks of lag; the freshness lookback picks the batches up when they land. |

Run **Probe data sources** (Actions tab) to see which portals answer from
each runner type, and through the proxy once it is configured.

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

**API endpoints:**

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
