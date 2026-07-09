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
- **CI**: GitHub Actions weekly refresh (Chicago, NYC, SF, Austin, Boston, Seattle, Houston, DC, Miami/Tampa/Orlando + DFW metro)

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
npm run build   # Outputs to public/ (committed — Cloudflare Pages serves it as-is)
```

### 4. Deploy

Merge to `main` — Cloudflare Pages auto-deploys the committed `public/`
directory (~2 min).

---

## Data Pipeline

```bash
# Full load (Socrata cities 2024+, DFW cities current year)
python dinescores_pipeline.py --mode full --cities chicago nyc sf dallas plano \
  --output-data-js public/data.js \
  --merge-existing-data-js public/data.js

# Weekly refresh (last 8 days; ALWAYS pass --merge-existing-data-js so the
# partial pull merges into the accumulated dataset instead of replacing it)
python dinescores_pipeline.py --mode weekly --cities chicago nyc sf dallas plano \
  --output-data-js public/data.js \
  --merge-existing-data-js public/data.js \
  --output-d1-sql /tmp/weekly.sql

# Test run (25 records per city)
python dinescores_pipeline.py --mode test
```

### Data sources & scraping notes

| Source | Method |
|--------|--------|
| Chicago / NYC / SF | Socrata open-data APIs, paginated. Set `SOCRATA_APP_TOKEN` env var for higher rate limits (optional). Fetched concurrently. |
| Austin (Travis County) | Socrata (`datahub.austintexas.gov`). Scores only (no violation text published); Census-geocoded. |
| Boston | CKAN datastore SQL API (`data.boston.gov`), updated daily. One row per violation; `*`/`**`/`***` levels map to severity. |
| Seattle (King County) | Socrata (`data.kingcounty.gov`), ~30 King County cities. Violation POINTS (lower = better) converted to 100-scale; RED = priority, BLUE = core. County feed last updated 2025-11; weekly refresh auto-resumes if publication restarts. |
| Dallas / Plano / Frisco (DFW) | MyHealthDepartment portal JSON search API in 7-day windows (auto-bisected when the ~225-record query cap is hit), then each inspection's public detail page is scraped for violation observations — they are rendered server-side in the HTML (or inline JS for Frisco), so no browser is needed. Frisco scores are demerit-based (lower = better) and are converted. |
| Houston | Tyler `healthinspections.us` portal: session-based date-window searches (bisected at the 500-row cap); each inspection's detail page carries full ordinance text per violation in its tooltip markup. |
| Washington DC | Tyler portal (different template): monthly window searches; report pages publish OFFICIAL Priority/Priority Foundation/Core counts + observation text. DC-located records only (mobile-vendor commissary addresses in VA/MD are skipped). |
| Florida (statewide) | Florida DBPR CSV extracts for all seven districts — Miami, Tampa, Orlando, Jacksonville, Fort Lauderdale, St. Petersburg, and every other FL metro — with official High Priority/Intermediate/Basic counts. Extracts roll over each July 1 with the state fiscal year, so history accumulates weekly from FY start. |
| New York State | NYSDOH open data (`health.data.ny.gov`): every active permitted facility statewide with its latest inspection — Rochester, Syracuse, Albany, Yonkers, Long Island. Official critical/non-critical counts, violation text, and coordinates (typo'd source coords are repaired or re-geocoded). Excludes NYC (own richer DOHMH feed) and Erie County/Buffalo (independent system, no public feed). |
| Raleigh (Wake County) | County ArcGIS open-data service, updated daily: restaurants layer (with coordinates), inspections layer carrying the OFFICIAL North Carolina 0-100 sanitation score, and a violations layer with item text + point deductions (severity anchored to points assessed). |
| Las Vegas (SNHD) | Southern Nevada Health District live JSON API (~18k permits across Clark County): official grade + demerits + coordinates per permit, violation descriptions with demerit values, and prior-inspection history. Demerit bands map exactly onto `risk = 100 - demerits` (A: 0-10, B: 11-20, C: 21-40). Permits with unreadable details are skipped, never recorded as clean. |
| Geocoding | Census Bureau batch geocoder (thousands of addresses per request), Nominatim fallback for stragglers. |

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
| `public/data.js` | Auto-generated embedded dataset |
