# DineScores — Restaurant Health Inspection Transparency

Multi-city restaurant health inspection scores with proprietary safety grading.
Shows weighted scores, grade badges (Safe/Evaluate/Avoid), infraction detection,
and violation summaries for ~60 metros and seven whole states: NYC and New York
State, Chicago, San Francisco, San Jose, Los Angeles and San Diego counties,
Sacramento, Orange County, Seattle, Portland, Las Vegas, Phoenix, Houston,
Dallas-Fort Worth, Austin, San Antonio, Boston, Washington DC, Richmond and
Hampton Roads, Nashville, Memphis, Omaha, Cleveland's suburbs, Denver's
southern suburbs, Atlanta and all of Georgia, New Orleans and all of
Louisiana, all of Virginia, Florida, Delaware and South Carolina, and more
(see *Data sources* below).

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
| McKinney / Lewisville / Denton (DFW cities off the MHD portal) | McKinney: the city's ArcGIS MapServer (`maps.mckinneytexas.org`; inspections layer with a server-side date filter and a count check, permit layers for coordinates and scope). The city publishes TX-form DEMERIT points per item (3 / 2 / 1 → priority / priority foundation / core) and `risk = 100 - demerits` (the TX form score, as Richardson, Dallas and Plano ship); every inspection's item points must add up to the city's total or it is dropped and held. Inspector observations are parsed from the report text; school cafeterias, mobile and temporary vendors are dropped. Lewisville: one JSON web-service call (`query.cityoflewisville.com`), OFFICIAL 0-100 score, coordinates, no violations; the host omits its TLS intermediate, so requests verify against the committed `data/certs/lewisville_ca_bundle.pem`. Denton: CKAN DataStore SQL (`data.cityofdenton.com`, OFFICIAL 0-100 score, WAF-safe query shapes, DISTINCT pages). All three read the whole window or raise. |
| Williamson County, TX (Round Rock, Georgetown, Cedar Park, Leander, Hutto, Taylor) | WCCHD's Yelp-LIVES-style zip on Google Cloud Storage (`storage.googleapis.com/ehyelp/`): one GET per run holds the whole history since 2021-12; the weekly window is a client-side date filter that never starts later than 180 days ago, because a new restaurant's pre-opening inspections appear months before its business row. OFFICIAL 100-point Inspection Score (100 − demerits; < 70 = Fail); an IHH closure caps the grade at F; school kitchens, mobile units and temporary booths are dropped. No violation text, coordinates or zip are published (Census-geocoded). |
| Phoenix / Maricopa County, AZ (Phoenix, Mesa, Chandler, Scottsdale, Glendale, Gilbert, Tempe, Peoria, Surprise, ...) | County "Restaurant Ratings Tool" at `envapp.maricopa.gov`, scraped on captcha-free paths only, three phases per run: a JSON permit inventory (one `POST /Search/Results` per 5-digit zip, ~400 zips, ~21.7k food permits; mobile units, daycare and non-retail permit types left out), one permit page per permit (its 3-year inspection list; the only date filter is client-side, so every weekly run re-reads all ~21.7k pages at ~7/s, ~52 min), then the detail page of each new inspection (~3 s each, 2 workers). No numeric score: `risk = 100 - 5P - 2PF - C` from the Priority / Priority Foundation / Core tier the county states on every code citation (one line per citation). The county's voluntary grade card (A/B/C/D, Cutting Edge A+, Not Participating, R) is shown as the result. Coordinates from the county's public ArcGIS facilities layer joined on permit id (99%+), the rest geocoded. Rows post ~5 days late, so each run reaches back `MCESD_POSTING_LAG_DAYS` (21) and skips inspections already stored. |
| Georgia (statewide, 156 counties incl. Atlanta's Fulton/DeKalb/Cobb; Gwinnett/Newton/Rockdale stay on the MHD `gwinnett` slug) | Tyler "publicportals" JSON API at `ga.healthinspections.us/stateofgeorgia`: one single-day facility search per day of the window, paged 5 rows at a time to the `[]` terminator with the ordering invariant checked (a shifted day is re-read, then the group errors rather than ship a partial window), one `inspectionsData` call per listed facility (oldest first, 2 workers); a facility whose history cannot be read is dropped and held. OFFICIAL 0-100 score (A 90+/B 80-89/C 70-79, the state's own cutoffs); severity from the state's (p)/(pf)/(c) citation designations with a Points fallback; text = form item + the inspector's observation with corrective-action and rule boilerplate stripped. 'Temporary' event-booth inspections and institutional kitchens (schools, hospitals, senior centers, by name) are skipped; the three GNR counties are re-checked every run. No coordinates: Census-geocoded. metro 'Atlanta' for the nine core counties (from the row's county EH phone). |
| Louisiana (statewide, 64 parishes; 'New Orleans' and 'Baton Rouge' chips) | The same Tyler API at `la.healthinspections.us` (date bounds strictly exclusive, the `keyword` key mandatory), same window/hold rules. LDH publishes no score: `risk = 100 - 5*Critical - 1*Non-Critical` over the state's own two categories, `{'0': ['None']}` is the explicit clean marker and a malformed entry is never read as clean; the inspector's narrative comes from each inspection's printable "Notice of Violations" (one rate-limited worker, Retry-After honoured; a printable that cannot be read ships the item titles, never a hold). Institutional kitchens skipped by name; 'Summer Feeding' visits skipped. LDH posts about a week late. Census-geocoded. |
| Fort Bend County TX, Arapahoe County CO, Douglas County CO, Cuyahoga County OH (Cleveland suburbs), Virginia (33 VDH health districts: Richmond, Hampton Roads, Northern Virginia outside Fairfax, and the rest of the state) | MyHealthDepartment portal (`inspections.myhealthdepartment.com`, slugs `fbceh`, `acph`, `dchd`, `ccbh`, `va-*`) via the relay, like DFW. One-day search windows sorted on inspectionID (the portal's unsorted paging drops 6-9% of rows and any multi-day window keeps one row per permit — measured 2026-11-03); a page sequence that repeats a row fails the window instead of shipping it short. Fort Bend publishes an OFFICIAL 0-100 score; Arapahoe and Douglas publish a demerit risk index (Pass 0-49 / Re-Inspection Required 50-109 / Closed 110+) and Cuyahoga and VDH publish no score, so for those `risk = 100 - 5P - 2PF - C` is computed from the detail page's observations (severity from the text classifier/Jev; the counties print no tier). No coordinates anywhere: Census-geocoded. Arapahoe mobile units (no address) are dropped; pools, child care, body art and schools are dropped by the programName allowlist. An unreadable detail page drops the inspection and holds the window. Cuyahoga and the VDH districts run in the 'MHD metros (midweek)' fetch group on the Wednesday cron (~1,200 detail pages a week). |
| Tucson / Pima County AZ, Lubbock TX, Mesquite TX (Hedgerow "Hedgehog" portals) | One JSON client for `healthinspect.pima.gov`, `clhd-portal.hedgerowsoftware.com` and `mesquite.hedgerowsoftware.com`: facility list → per-facility inspection list → per-inspection checklist, no server-side date filter (the window is applied client side; Pima's 5,177-row list only answers when sorted by id, Lubbock's pages newest-inspected first, Mesquite's hides dates so all 689 facilities are re-read each week). Pima publishes no score: `risk = 100 - 5P - 2PF - C` from the county's Priority / Priority Foundation / Core citations, the categorical rating in results, Fail/IHH/Probation cap the grade at F. Lubbock and Mesquite publish an OFFICIAL 0-100 score (Lubbock's letter follows its verified 90/80/70 bounds). Severity is the agency's per-citation tag; the Texas form's pest criterion ships as "Pest control: <observation>". Coordinates come from the portals (Mesquite's missing 39% are Census-geocoded). Temporary-event booths, schools and mobile units (pinned at commissaries otherwise) are dropped. An unread checklist drops the inspection rather than shipping it clean; an unread facility list holds the window; a deadline cut reports the group skipped. |
| Harris County, TX (unincorporated Harris County + 23 contract cities: Katy, Humble, Tomball, La Porte, Deer Park, Bellaire, Seabrook, Jersey Village, ...; the City of Houston is covered separately) | Harris County Public Health's FSP portal JSON API (`secureweb.hcphtx.org/fspportal/api`, no auth): an establishment search filtered on `lastCompletedInspectionDate`, then each establishment's inspection list and each inspection's OUT items. That search field trails the inspection list (stuck on an earlier visit for ~2% of establishments), so every window is read from `HARRIS_LC_LAG_DAYS` (28) before its start. Score is the county's OFFICIAL `100 − demerits` (verified on 98.4% of inspections; the rest sit 2 points above); severity from the county's P / F / C violation groups (never re-tiered); violation text is the TFER item title (the county publishes no observation narrative). Ships Routine, Permitting (the annual permit inspection of an operating place), Complaint, Food Borne Illness and follow-ups of operating places; drops Pre-opening, Non-permitted, Consultation and school/day-care kitchen visits; skips schools, day care, institutions, pantries, commissaries and mobile units. Coordinates exist on 0.4% of establishments, the rest are Census/Nominatim-geocoded (~80-87%). A county-ordered closure caps the grade at F. A search page or inspection list that cannot be read fails the window; an unreadable item list drops the inspection and holds the window; a 404 inspection list skips and holds that establishment; 3 exhausted requests in a row trip a breaker ('blocked'). |
| Geocoding | Census Bureau batch geocoder (thousands of addresses per request), Nominatim fallback for stragglers. |

### Weekly refresh health

The refresh workflow (Sundays for every source, Wednesdays for the heavy
MyHealthDepartment group: Cuyahoga County and the Virginia VDH districts) is
failure-tolerant: a source that errors, blocks the runner, or misses the time
budget is skipped and keeps its existing records.
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
| MyHealthDepartment portal (Dallas, Plano, Frisco, Fort Worth, Tarrant County, Portland metro, Colorado Front Range, Utah County, Yolo, Fort Bend, Arapahoe and Douglas CO, Cuyahoga, the Virginia VDH districts) | Returns 403 to every GitHub-hosted runner type (Linux, macOS and Windows, probed 2026-09-07). The weekly job therefore sends these requests through the site's own relay function, `functions/api/relay.js` (`https://dinescores.com/api/relay?url=`), which fetches from Cloudflare's network — verified reachable 2026-10-10. The job authenticates with a GitHub OIDC token it mints itself, so no secret is needed; a static `RELAY_TOKEN` Pages variable + `PORTAL_RELAY_TOKEN` repo secret, or an HTTP proxy in `PORTAL_PROXY_URL`, are the alternatives. Detail pages are also rate-banned after a few hundred per ~15 minutes; the fetcher waits the ban out (`MHD_BAN_COOLDOWN` seconds, `MHD_BAN_COOLDOWNS` per run, default 16) within the fetch deadline, works jurisdictions stalest-first, and drops anything it could not read for the next run to pick up. |
| Las Vegas (SNHD) | The district's API answers 401 to a client that makes a few hundred requests in a few minutes (GitHub runners hit it after ~60 list pages). Requests go through the relay too, the detail phase is paced (`SNHD_DETAIL_WORKERS`, `SNHD_DETAIL_DELAY`), and a 401/429 pauses every worker (`SNHD_RATE_LIMIT_WAIT`) before retrying. |
| Austin | The source dataset runs weeks behind; the report notes the source's own newest row. |
| LA County | Publishes quarterly into the same ArcGIS hub items (the title's end date rolls forward); the fetcher re-resolves the items each run and the 400-day lookback reads the whole new quarter. |
| Detroit, Fairfax County | Publish in batches with several weeks of lag; the freshness lookback picks the batches up when they land. |
| Lewisville | The city's web service keeps only its trailing 365 days, so a lapse of a year loses history for good; its host sends no intermediate certificate, so the fetcher verifies against `data/certs/lewisville_ca_bundle.pem` (certifi plus GoDaddy's public G2 intermediate) and goes red, never insecure, if that chain changes. |
| Williamson County | A snapshot file with no zip codes: about one address in seven misses the Census geocoder (highway-style Texas addresses) and ships without coordinates until a later run finds them; the 180-day re-read margin sends them back every week. |
| MyHealthDepartment search paging | The portal's default order pages unstably on same-day ties and ANY multi-day window returns one row per permit (its latest inspection), so the old weekly 7-day read lost 6-9% of each week's rows plus every Routine that earned a Follow-Up the same week (measured 2026-11-03 over 16 jurisdictions). Every plain jurisdiction is now searched in one-day windows sorted on inspectionID; a sorted query whose pages repeat a row is re-read once and then fails the window (the group goes red) instead of shipping short. |
| Maricopa County | No server-side date filter outside a reCAPTCHA-gated report that is never used or worked around: every run re-lists all ~21.7k permit pages (~52 min) before any detail page, and a deadline reached during the listing ships nothing for Phoenix that week (held, 'skipped'). Rows post ~5 days late. |
| Georgia, Louisiana (Tyler publicportals) | The search's date filter is on the FACILITY's last inspection, so windows are single days and every listed facility costs one history call; a facility inspected twice in a window is listed once and its earlier visit comes from the history. Louisiana's printable notices sit behind a ~4-request token bucket (instant 503s with Retry-After), so the narrative pass runs on one worker (~47 min a week) and is capped by `LA_PRINTABLE_MAX_PER_RUN`; LDH posts about a week late. A Georgia row carries no county: the county EH phone (`GA_ATLANTA_EH_PHONES`) is the only 'Atlanta' signal. |
| Pima County, Lubbock, Mesquite | No server-side date filter: Pima's whole facility list (5 MB) answers only sorted by id and the county publishes ~16 days late (the report notes the lag); Mesquite's list hides dates and types, so every facility's inspection list is read each week (~700 requests) and its schools ship. |
| Harris County | Only ACTIVE permits are searchable (a closed place's history is unreachable) and the search's `lastCompletedInspectionDate` can stay stuck on an earlier visit, so every window reaches 28 days back and most of a week's upserts are idempotent re-writes; coordinates are almost never published, so ~15% of records wait for a later geocoding pass. |

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
