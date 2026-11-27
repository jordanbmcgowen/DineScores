# DineScores

Restaurant health-inspection scores on a map: https://dinescores.com
~68k restaurants embedded client-side (the 24 MB cap; each metro's ~610
newest), ~376k in the database, covering ~100 metros (NYC, Chicago, LA County, SF, Seattle, Boston,
Austin, Houston, DC, DFW incl. McKinney/Lewisville/Denton, Las Vegas,
Raleigh, Miami/Tampa/Orlando, Louisville, Minneapolis, Cincinnati,
Montgomery County MD, Detroit, Sacramento, Fairfax County, Portland OR
metro incl. Clark County WA, Colorado Springs/Fort Collins/Greeley/Adams
County CO, Arapahoe/Douglas County CO, Provo/Orem, Yolo County, San Jose/
Santa Clara County, San Diego County, Omaha/Douglas County, Orange County
CA, San Antonio, Williamson County TX, Fort Bend County TX, Harris County
TX (unincorporated + Katy, Humble, Tomball, La Porte, Deer Park), Nashville/
Memphis/Knoxville/Chattanooga, Phoenix/Maricopa County AZ, Tucson/Pima
County AZ, Lubbock TX, Mesquite TX, Cuyahoga County OH; and, since
2026-11-22, El Paso, Corpus Christi, Garland, Waco, Amarillo, Tyler/Smith
County, Abilene, Wichita Falls and Orange County in Texas, Montana
statewide (the state portal's counties: Billings, Helena, Great Falls,
Butte; Missoula, Bozeman and Kalispell from their counties' own Paragon
portals), Cheyenne, Casper and Jackson WY, Minot ND, St. Louis, Kansas City
and Jackson County MO (Lee's Summit, Blue Springs), the Colorado counties CDPHE
inspects (Vail, Aspen, Breckenridge, Durango, Steamboat), Pueblo, Grand
Junction and Broomfield CO; and, since 2026-11-26, Napa County CA: Napa,
Yountville, St. Helena, Calistoga, American Canyon; and Sonoma County CA:
Santa Rosa, Petaluma, Healdsburg, Sonoma, Sebastopol, Windsor, Rohnert
Park, Glen Ellen; and, since 2026-11-27, Hawaii statewide (Honolulu,
Kailua-Kona, Hilo, Maui, Kauai), Akron/Summit County OH, Placer County CA
(Roseville, Rocklin, Auburn, Truckee) and Platte County MO (Parkville,
Platte City, Riverside)) plus all of Florida, New York State, Delaware, South Carolina, Georgia
(Atlanta: Fulton/DeKalb/Cobb and the other 153 counties; Gwinnett/Newton/
Rockdale on the MHD `gwinnett` slug), Louisiana (New Orleans, Baton Rouge,
Shreveport, Lafayette) and Virginia (33 VDH districts; Fairfax County is
its own source).

## Architecture

```
dinescores_pipeline.py  (Python, stdlib-only scrapers)
   ├─> public/data.js       embedded dataset the map loads instantly
   └─> D1 SQL               full dataset + inspection history
Cloudflare Pages builds on deploy, serves public/
   ├─ functions/api/*.js    Pages Functions backed by D1 (env.DB binding)
   └─ src/ (React+Vite)     builds INTO public/ (build output gitignored)
```

- **Frontend**: React 19 + Vite + Tailwind + MapLibre GL. Entry `src/main.jsx`
  → `App.jsx`. Map is `src/components/RestaurantMap.jsx` (donut-chart
  cluster markers, GPS-centered start). The GPS button is our own
  `src/locateControl.js`, not `maplibregl.GeolocateControl`: the stock one
  disables itself until a reload after a single PERMISSION_DENIED (a
  dismissed prompt, iOS Location Services off), which read as "the GPS
  button can't be clicked". Ours never disables, re-asks on every tap,
  zooms to street level and follows live until the user pans; a tap that
  finds no location shows a notice saying why. "Low scores near me" (a
  filter-rail chip) and the at-restaurant alert live in `src/nearMe.js`
  (pure, `node tests/test_near_me.mjs`): recent C/F grades only (18
  months), an "at" radius of 30-60 m from the fix's accuracy, no alert on a
  fix worse than 100 m, "Next door" when a better-graded place is closer.
  Location is read only while the page is open; nothing is stored or sent.
  The report says when an area's health department is publishing late
  (`src/sourceLag.js`, `node tests/test_source_lag.mjs`): an area (metro,
  else city, plus state) with 100+ embedded records whose newest is over 21
  days old gets an amber note, so LA County's quarterly posting or Austin's
  six-week lag does not read as "not inspected in months". It is computed
  from the embedded set only, which keeps every city's newest records.
  Data sources, in order: embedded
  `public/data.js` (top 1,000/city, bounded by EMBED_BUDGET and the 24 MB cap), then viewport lazy-loading via
  `src/api.js` → `/api/restaurants?bbox=`, history via
  `/api/restaurants/:id/history`. All API helpers fail soft so the map
  works even if D1 is down. Basemap tiles are CARTO raster tiles, which
  require an API key since Aug 2026 (`VITE_CARTO_API_KEY`, a Cloudflare
  Pages build-time variable; `.env.local` for dev). Without it the map still
  renders but every tile carries an "API KEY REQUIRED" watermark, and the
  console warns. The key is baked into the bundle at build time, so a key
  change needs a rebuild, not just a redeploy of the same build.
- **API**: `functions/api/` — `cities.js`, `restaurants.js` (requires a
  `city=`, `metro=`, `bbox=`, or `q=` filter), `restaurants/[id].js`.
  D1 binding comes from `wrangler.toml` (generated by the setup workflow).
  Every GET is wrapped in `edgeCached` (`_utils.js`): Pages Functions
  responses are NOT cached by the CDN on their own, so without it each
  visitor's whole-database sync plus the cities index cost ~560k D1 rows
  read. Responses carry `X-Edge-Cache: HIT|MISS`; only 2xx responses with a
  positive max-age are stored (a 503 while D1 is down must never be pinned
  for an hour), and a fresh deploy or an hour clears them.
- **Crawlable pages (SEO / AI search)**: the map is an SPA that crawlers
  cannot read, and Pages answers every unknown path with index.html, so
  without these the site was one empty page to Google. `npm run build` runs
  `scripts/seo/build_seo.mjs` after Vite: from `public/data.js` it writes
  static `/about/`, `/inspections/`, `/inspections/<st>/` and
  `/inspections/<st>/<city>/` pages (city pages under 3 restaurants are
  `noindex`), `robots.txt` (disallows `/api/`), `sitemap.xml`,
  `sitemap-pages.xml` and `llms.txt` (all gitignored build output).
  `functions/restaurant/[slug].js` renders `/restaurant/<name>-<id>` from D1
  (a stale name or bare id 301s to the canonical URL; Restaurant JSON-LD,
  no aggregateRating: a grade is not a review), and
  `functions/sitemaps/[file].js` lists them in 16 shards by the id's first
  hex digit. Shared template: `functions/_lib/seo.js`. `index.html` carries
  the meta/OG/JSON-LD and a crawlable summary inside `#root` that React
  replaces on mount. Tests: `node tests/test_seo.mjs`.
- **Pipeline**: one file, `dinescores_pipeline.py`. One `fetch_<city>()`
  per source (Socrata, ArcGIS, state portals, SNHD, MyHealthDepartment
  scraper for DFW). Seattle/King County is Socrata dataset `r878-4sxa`
  (the old `f29f-zza5` requires a login since 2026); LA County discovers
  its quarterly inspection/violation CSVs and inventory CSV or Feature Service
  by publisher and title (`_la_hub_items`). Normalizes everything to `risk_score` 0-100 and grades
  A/B/C/F (`calculate_vetted_grade`; an F can be forced by severe violation
  text regardless of score). `write_data_js` merges into the existing
  data.js at restaurant level; `write_d1_sql` emits idempotent SQL
  (INSERT-upserts; inspections keyed by restaurant+date).

## Deploy model

Pushing to `main` triggers a Cloudflare Pages deploy (~2 min). Pages runs
`npm run build` itself and serves the fresh output, so build artifacts
(`public/assets/`, `public/index.html`) are gitignored — never commit
them. `public/data.js` IS committed: it is pipeline-generated data, not
build output, and the build leaves it alone (`vite.config.js` sets
`emptyOutDir: false`). Verified 2026-07: the live site's bundle hash
differs from any local build, while live `data.js` matches `main` — Pages
builds from git on deploy. `npm run dev` serves data.js through a dev
middleware.

## Workflows (.github/workflows/)

- `refresh-data.yml` — two crons, both 09:17 UTC: Sundays run every source
  but the midweek group; Wednesdays run the `MHD metros (midweek)` fetch
  group alone: `cuyahoga virginia` (Cuyahoga County and the 33 Virginia VDH
  districts, ~1,200 detail pages = ~5 fifteen-minute ban windows, which
  would not fit next to DFW on Sunday) and the fourteen jurisdictions added
  2026-11-22 (`montana amarillo tyler orangecountytx wyoming minot stlouis
  jacksoncountymo coloradostate pueblo grandjunction broomfield`, ~650 more
  detail pages a week plus ~35 Montana printables at ~20 s each) and the
  three added 2026-11-27 (`hawaii akron placer`, ~750 more detail pages a
  week; each new jurisdiction's first run reads only the last 8 days). The
  Wednesday run took 54 of its 300 minutes before them. The "Pick the sources for this run"
  step sets `--cities` from the weekday (a manual run takes a `cities`
  input); both runs share the same merge/commit flow and freshness file.
  The Wednesday run also re-fetches a Sunday MHD group (DFW, 'MHD metros')
  whose last run, within five days, was blocked, skipped or errored
  (`scripts/midweek_catchup.py`), so a lost Sunday heals in three days
  instead of seven; a good Sunday adds nothing.
  Pulls the recent window from the chosen sources, merges data.js, applies
  incremental SQL to D1, commits, and then FAILS the run if any fetch group
  was blocked, errored or skipped (the last step, after the commit, so a red
  run never costs data). Fetchers are
  failure-tolerant and skip rather than ship partial data (the merge
  preserves existing records). Two guards keep a blocked portal from timing
  out the whole job (which ships nothing): a run-wide MHD circuit breaker
  (`_MHD_PORTAL_BLOCKED` — first exhausted 403 retry on a SEARCH skips the
  portal's remaining jurisdictions) and `--deadline-minutes` (past the
  budget, remaining fetch groups are skipped and outputs still get written).
  - **A green job used to mean nothing about coverage** (2026-09-06 through
    2026-10-04 every scheduled run shipped 0 records for DFW, Portland, the
    CO Front Range, Utah County, Yolo and Las Vegas and still passed). The
    pipeline records every fetch group's outcome in
    `data/source_freshness.json` (`--freshness-file`); the "Report source
    outcomes" step renders it into the run summary with warning annotations
    for groups that failed or whose newest inspection is >21 days old, and
    the final step turns blocked/error/skipped groups into a failed run so
    the owner is notified. Look at the summary first when "the refresh
    isn't working".
  - **A stale source is not always a broken one.** A fetcher can leave a
    one-line reason in `_GROUP_NOTES` (`_note_source_lag` for the common
    "the source itself is behind" case); `_record_group` picks it up and
    the report prints it next to the STALE/FAILED flag. **LA County**
    publishes quarterly into the SAME hub items (the title's end date rolls:
    "... 07/01/2023 to 06/30/2026" became "... 10/01/2023 to 09/30/2026" on
    2026-10-06), so it looks stale for up to three months and then catches
    up in one run; `_note_source_lag` says so in the report. Its inventory
    did move: the CSV item was deleted for a Feature Service around
    2026-10-13, failing the 2026-10-18 run. Discovery follows such moves;
    the `LA_*_ITEM` constants are only the fallback when the hub search
    fails, so keep them on live items. **Austin** simply runs weeks behind
    its own dataset.
  - **Self-healing windows**: weekly mode starts each group's window the day
    before its recorded latest SHIPPED inspection (`_lookback_since`)
    instead of a flat 8 days, so a lost week refetches itself. The caps
    (400 days for API/CSV sources, 90 for scraped portals) are safety
    bounds, not the normal window: the old 45/21-day caps silently dropped
    LA County's July-August 2026 when the quarterly extract appeared, and
    would have abandoned every MHD week older than three weeks once the
    portal was reachable again. `--since-date` still overrides everything.
    Only shipped records advance the recorded latest, so a portal backlog
    drains oldest-first across runs; `fetch_dfw` orders jurisdictions
    stalest-first (and `main()` orders the DFW / MHD-metros groups the same
    way) so a run that hits its deadline never starves the same metro twice.
  - **MHD blocks every GitHub-hosted runner type** (ubuntu, macOS, windows
    all get 403 — probed 2026-09-07 with `probe-sources.yml`) and **SNHD
    (Las Vegas) answers 401** to a runner after a few dozen list pages. Both
    now go through the site's own relay, `functions/api/relay.js`
    (`/api/relay?url=`), which fetches from Cloudflare's network. Verified
    2026-10-10 from a Pages preview deployment: MHD searches (Dallas, Plano)
    return real rows, detail pages load, and 70 rapid SNHD list pages all
    returned 200. The portal's two-factor block (egress IP AND client TLS
    fingerprint — Node's undici is refused even from an accepted IP,
    measured 2026-09-15) does not apply to Cloudflare's fetch.
    - **No secrets.** The job mints a GitHub OIDC token (`id-token: write`,
      audience `dinescores-portal-relay`, re-minted by `_relay_token()`
      before expiry) and the function verifies the signature against
      GitHub's JWKS plus issuer, audience, expiry, `repository ==
      jordanbmcgowen/DineScores` AND the immutable `repository_id`
      (1163315559) and `repository_owner_id` (244274424) — names can be
      re-registered after an account rename, ids cannot. If the repository
      moves, set both `RELAY_ALLOWED_REPO` and `RELAY_ALLOWED_REPO_ID` as
      Pages variables (a name override without an id is refused). A static
      `RELAY_TOKEN` Pages variable is accepted as an alternative
      (`PORTAL_RELAY_TOKEN` on the client side). Only the two portal
      hostnames can be fetched, and redirects are followed by hand only
      while they stay on those hosts over https. The relay deploys with
      every Pages build, so a broken relay shows up as a red refresh run,
      not a silent one. `PORTAL_RELAY_URL` defaults to
      `https://dinescores.com/api/relay` in the workflows; the
      `PORTAL_RELAY_URL`/`PORTAL_RELAY_TOKEN`/`PORTAL_PROXY_URL` secrets only
      override it. `tests/test_refresh_hardening.py` covers the client side
      and `node tests/test_relay.mjs` the function (JWT verification,
      forged/expired/foreign-repo/re-registered-namespace tokens, host
      allowlist, redirects, header stripping).
    - **MHD allows ~250 searches per TCP connection**, then 403s that
      connection (measured 2026-10-10: 252 searches, 403; cookies cleared,
      still 403; a new connection, 200 at once). `_mhd_rotate_connection`
      reconnects every `MHD_SEARCHES_PER_CONNECTION` (150) searches and on
      the first 403 of a search; only a second 403 goes to the retry ladder
      and the breaker. Tennessee's bucket mode (30 searches a day per metro)
      tripped the breaker in five minutes before this.
    - **A search 403 that survives the reconnect and the retry ladder gets
      one ban cooldown before the breaker trips** (`_mhd_take_search_cooldown`,
      same budget and deadline rule as the detail cooldowns). 2026-11-22: a
      ~310-page detail burst (Clackamas, Multnomah) was followed by a 403 on
      the next search; three minutes of retries later the breaker wrote off
      DFW and 15 metros for the week. A block that outlasts the cooldown
      still trips it.
    - The cooldown budget `MHD_BAN_COOLDOWNS` defaults to 16 (was 3): a
      normal week of every MHD jurisdiction is well over 1,000 detail pages,
      five or more 15-minute ban windows, which the old budget could never
      finish. The job's 300-minute fetch deadline (350-minute job) is what
      bounds a CI run; both crons fire at 09:17 UTC (Sunday, Wednesday), so
      the hours are free.
    - SNHD is rate-limited, not IP-blocked: a 401/429 pauses every SNHD
      worker (`_snhd_pause_all`, `SNHD_RATE_LIMIT_WAIT`) instead of
      retrying three seconds later, and the detail phase runs at
      `SNHD_DETAIL_WORKERS=2` / `SNHD_DETAIL_DELAY=0.5`. A throttle that
      does not lift is a block: once one unbroken throttling episode has
      paused `SNHD_MAX_TOTAL_PAUSE` (15 min), or `SNHD_BREAKER_TRIP` (3)
      requests in a row exhaust their retries for ANY reason (401/429, a 403
      block, 5xx, timeouts), `_SNHD_BLOCKED` fails every further SNHD
      request at once, so Las Vegas can never eat the whole fetch deadline.
      A success resets both counters; a 400/404/410 is that one permit's
      problem and is neither retried, counted nor held. Permits run
      oldest-first; each one not read holds Las Vegas on its own with
      `_hold_window_start(..., advance=True)` (key = permit number), which
      also RAISES the recorded latest to the hold (everything before it was
      read), so the next window starts exactly at the oldest unread permit's
      date and a run the breaker cut short still moves forward by what it
      read. A re-read permit ships its previous inspections back to
      `SNHD_PREVIOUS_MARGIN_DAYS` (14) before the window, or before
      `backlog_from` (the first window start of an unbroken series of cut
      runs, kept in the freshness file) while a backlog drains, so a permit
      re-inspected mid-backlog still ships the downgrade a cut run never
      read. Holding at the window start slid it back two days a run (the
      lookback reaches a day past the cap), and folding every unread permit
      into one hold froze a backlog. A breaker trip reports 'blocked'.
- `poll-orange-county.yml` — cron, 05:00 UTC Monday-Saturday. Orange
  County's MHD jurisdiction ignores every date filter and returns only its
  newest 225 inspections (~1.7 days; measured 2026-10-10, see
  `fetch_orange_county`), so it is polled daily into the sidecar
  `data/orange_county_inspections.jsonl` (last 10 days) and upserted into
  D1; the Sunday refresh ships the whole sidecar to rebuild data.js and
  commits the sidecar too. Both workflows check out `main`'s tip and share
  the `data-refresh` concurrency group so they never push at once; the
  poll commits the sidecar BEFORE its D1 update (an uncommitted day is
  lost for good, a missed D1 update heals on the next poll) and records its
  outcome in a /tmp copy of the freshness file, never the tracked one (a
  dirty tracked file stops its `git pull --rebase`, and only the weekly
  commits freshness). The weekly commits the sidecar and the Jev cache even
  when its D1 update fails, but then leaves data.js and the freshness file
  out, so next week refetches that window into both. Committing data.js
  alone would not work: next week's MHD fetch drops inspections data.js
  already holds (`--skip-known-inspections`), so D1 would never get them.
  The sidecar is pruned to 10 days before its newest inspection but never
  before Orange County's recorded latest (`OC_SIDECAR_MAX_DAYS`=35 bounds
  it), so the days a failed weekly held back are still there next week. It runs Jev like the weekly, so the daily upsert does not flip
  D1's grades back to the regex. OC publishes no score: Pass/Reinspection/
  Closed map to 90/75/50, and violations come from the inspection's
  printable PDF (stdlib parser, county MAJ/MIN/OUT tags set the severity).
  An inspection ships only once its printable is read; a PDF that parses
  without a Result counts as unread (a layout change must not read as
  zero violations). One whose printable failed `OC_PDF_GIVE_UP_TRIES` (3)
  times AND was first polled `OC_PDF_GIVE_UP_DAYS` (5) days ago ships with
  the county's result alone (age alone is not enough: a week the endpoint
  was blocked is not a failed read). Reported as an error: more than half
  the printables unreadable, or over 90% of the read ones showing no
  violations (both need `OC_PDF_MASS_FAILURE_MIN`=20); a printable phase
  the block breaker or the deadline cut short is 'blocked'/'skipped'.
- `load-d1-seeds.yml` — on push to `main` touching `data/d1_seed*.sql.gz`
  (and manual with a glob): loads the seeds that push added or changed.
  Committing a seed IS loading it; the four 2026-09-15 catch-up seeds sat
  unloaded for a month because the setup workflow had to be run by hand,
  leaving D1 nine days behind data.js for DFW. It fetches the push's base
  commit, so a rebase-merge or multi-commit push loads every seed it
  added, and fails loudly if that base cannot be fetched.
- `export-d1.yml` — manual, read-only. `scripts/d1_export.py` pages through
  the live database and saves every restaurant's stored row plus its latest
  inspection as a three-day artifact, so a regrade reads D1's own text, not
  the seeds (which lag it). With the `sources` input ("A|B") it exports only
  those source labels' restaurants, each with EVERY stored inspection (a
  regrade that moves scores needs the history; the six Front Range counties
  are 4 MB and 30 seconds). Changes go back as a committed seed, never from
  this workflow.
- `probe-sources.yml` — manual. Curls each portal from ubuntu/macOS/windows
  runners, directly and through the relay (with a minted OIDC token) or
  `PORTAL_PROXY_URL`, and prints the HTTP status.
- `setup-database.yml` — manual (workflow_dispatch). Bulk-loads
  `data/d1_seed*.sql.gz` into D1 and writes `wrangler.toml`. Idempotent.
  For a single new seed prefer `load-d1-seeds.yml` (automatic on merge).

## Candidate sources surveyed but not added (2026-11-02)

A 28-source live survey (per-source reports under
`docs/expansion-2026-11/`) fed the November 2026 expansion; what shipped is
in the coverage list above. Still worth building, roughly in order of
people covered per unit of work:

- **Snohomish County WA (Everett, 864k) and Anchorage AK (290k)**: the same
  Accela EnvisionConnect "PressAgent" JSON API (`/api/pressAgentClient/`,
  PressAgentOid in the root HTML); no date filter, so a weekly run re-reads
  every program's inspection list (~4,700 requests Snohomish, ~2,100
  Anchorage); search is capped at 50 rows with no paging (adaptive name
  prefix splitting enumerates facilities); Washington violation points like
  King County; Anchorage needs an Alaska box in `coords_plausible`, which
  rejects everything outside CONUS today.
- **Alabama statewide (4.1M; not Birmingham or Mobile)**: ADPH
  `foodscores.state.al.us`, a WebForms county sweep (66 requests), OFFICIAL
  0-100 score only, latest inspection per establishment, no violations, no
  coordinates, no history; Tallapoosa County 500s.
- Akron/Summit County OH ('scph') and Placer County CA ('pchd') shipped
  2026-11-27 (see the placard invariant), as did Amarillo ('aaphd')
  2026-11-22. The statewide
  'virginia' slug is NOT usable (capped, unstable paging, bucket mode lossy);
  the 33 `va-*` district sub-sites are ordinary uncapped jurisdictions and
  are what ships. Toledo ('oh_toledolucas') is
  NOT buildable: its date filter is ignored, observations are hidden and the
  printable endpoint times out.
- **Oklahoma City (816k)**: Inspect2Go, every inspection a 600 KB PDF
  (CID fonts, new parser needed), ids dense in upload order; **Tulsa
  (693k)**: Tyler ColdFusion portal with an inert date filter, inspection-id
  enumeration only, every result 'Compliant'; **Pasadena CA (138k)**: JSON
  permits + SSRS PDF reports; **Iowa statewide**: the new SAFHER (Appian)
  portal, a fragile SAIL protocol plus PDFs; **Columbus/Franklin County OH**:
  Columbus Public Health's PressAgent portal was down (522) during the
  survey and Franklin County's Accela carries PDFs only; **Reno/Washoe**:
  Accela, ~4,000 permit pages per run; **Columbia MO**: Tyler publicportals
  with severity only in PDFs (the publicportals search does have a
  server-side last-inspection-date filter at the facility level, as Georgia
  and Louisiana use; only coordinates are missing).
- **Blocked or empty**: Kansas statewide (KDA portal behind reCAPTCHA v3 —
  never work around it; ask KDA for an extract), Irving TX (two abandoned
  ArcGIS snapshots; the live page is behind a Cloudflare challenge),
  Carrollton TX (Akamai 403), Allen TX ('scores unavailable'), Grand Prairie TX
  (letter grades only, no ids or dates), Collin and Denton county programs,
  Hays County TX, Long Beach CA (nothing online), Cleveland proper (own
  department, not on the county portal). Earlier findings stand: Boulder
  County stale, Alameda County quarterly violation rows only, Allegheny
  placards only, Hawaii's Tyler host resets connections, Albuquerque's CSV
  header-only since 2020; no machine-readable source for Denver, Salt Lake
  County, Charlotte, Milwaukee, Madison,
  Philadelphia, Baltimore, Riverside/San Bernardino, Spokane, Tacoma.
  (Kansas City MO turned out to be on Paragon Inspections Online and
  shipped 2026-11-23.)

### Second survey, 2026-11-22 (`docs/expansion-2026-11b/`)

The MyHealthDepartment portal's root page lists every tenant (341 slugs);
probing each one, plus five regional surveys of Texas and Montana, fed the
2026-11-22 group. `docs/expansion-2026-11b/README.md` ranks what is left;
`mhd.md` there lists the ~120 portal tenants that answer rows and are not
read yet. The ones worth building next:

- **Austin on MHD ('aph')**: official scores and observation text, current
  to within days, where the Socrata dataset runs six weeks behind with
  scores only. NOT a config entry: the portal's names and addresses hash to
  different restaurant ids than the stored ones (1 match in 74), so it needs
  an id map from a D1 export first, as King County did.
- **Galveston County** (Salesforce guest endpoint, scores only, posting
  looked stalled), **Bell County** (Killeen/Temple: weekly PDFs, weeks
  late), **Brazos County** (Inspect2Go PDFs, no date filter), **Beaumont**
  (ArcGIS plus a PDF per inspection, no address in the layer), **New
  Braunfels / Seguin / Nacogdoches / Rowlett** (small score-only sources),
  **Little Elm / Sachse / Pilot Point / Justin** (InstantInspector HTML; its
  robots.txt asks AI crawlers to stay out, an owner decision).
- Hawaii statewide ('soh') and Platte County MO (Paragon,
  `mo/platteplattecity/inspect.nsf`) shipped 2026-11-27.
- More MHD tenants with live rows: Hamilton County OH and ~35 other Ohio
  counties (no score, every row a detail page), Benton-Franklin WA, Yavapai
  AZ, Marin and San Luis Obispo CA, Union County NC, St. Clair County IL,
  Somerset County NJ, West Virginia counties, Wisconsin DATCP.
- **Blocked, stale or nothing online** (checked 2026-11-22): Laredo (Tyler
  publicportals frozen at 2026-01-30, `inspectionsData` 500s), Odessa,
  Brownsville, McAllen/Hidalgo, Midland, Montgomery County TX, Brazoria
  County, Pasadena TX, Baytown, Port Arthur, Missouri City, San Marcos/Hays
  (login), Texarkana, Texas DSHS (publishes nothing for the areas it
  inspects), Irving, Carrollton, Grand Prairie, Allen, Mansfield; Ravalli,
  Carbon and Fergus counties MT. MHD slugs that exist and answer no rows:
  `tx-dallas-county` (Dallas County's contract cities, empty since 2025),
  `ut-saltlake-county`, `oh_cleveland`, `riverside-county-ca`, `long-beach`,
  `mecklenburg`, `davidson-metro`.

## Hard-won invariants (violate at your peril)

- **Cloudflare Pages rejects any asset over 25 MiB**, and `public/data.js`
  is the asset that grows. `write_data_js` bounds it (EMBED_BUDGET global
  cap + EMBED_CITY_FLOOR per-city floor, cities keyed by (name, state))
  and then enforces a hard `DATA_JS_MAX_BYTES` (24,000,000); both trims
  keep the same FRACTION of every city's newest records, never below the
  floor (`_embed_trim`; the 2026-11-03 rebuild, 4,202 cities: every metro
  keeps its 496 newest of up to 1,000; the 2026-11-23 rebuild, 4,779
  cities: 73% each, New York 610, a new city at the 1,000 cap 730). Dropping the globally
  oldest records instead let a few hundred small towns' newest inspections
  push the metros out one record at a time (Dallas would have kept ~150),
  levelling every city to the same COUNT gave each metro ~158, and the
  record budget alone let the file creep to 24.8 MB as new towns added
  floors. A deploy that fails with no error on the commit is usually this.
  The full dataset lives in D1.
- **D1 rejects any SQL statement over 100KB** (`SQLITE_TOOBIG`), and its
  file imports are atomic. `write_d1_sql` caps multi-row INSERTs at 40 rows
  AND 80KB (`_d1_row_batches`) — keep both caps on anything that emits SQL.
- **D1 rows read/written are metered; scope every statement.** The
  inspection_count recount at the end of each SQL file is limited to the
  restaurants in that file (`D1_RECOUNT_IDS` per statement). Its old
  unscoped form rewrote all 280k restaurants weekly (2.5M rows read, 315k
  written), which by itself exceeded the free plan's daily write cap. The
  account is on Workers Paid since 2026-09-10 (the database is 594 MB,
  over the free plan's 500 MB per-database limit).
- **The OpenStreetMap geocoder identifies itself by the site, not a
  person.** `NOMINATIM_USER_AGENT` (default `DineScores/1.0
  (+https://dinescores.com)`; a repository variable overrides it in
  `refresh-data.yml`). The source carried the owner's address until
  2026-11-25; a test keeps any address out of it.
- **Pins found after a backfill go back as coordinates only.**
  `scripts/backfill/pins_seed.py` writes one `UPDATE ... AND lat IS NULL`
  per restaurant and fills the same embedded records; re-absorbing the
  placed records instead would rebuild each restaurant's weighted score from
  the single inspection in the file. A pin more than 40 km from its town's
  other restaurants is left out (the geocoder is asked free text and the
  pipeline's own check is only the state's box). 2026-11-25: 579 of 1,085
  placed for the 2026-11-22 sources, none left out; the Census batch call
  hung for 20 minutes on that list of known misses, so the OpenStreetMap
  half was run on its own. Corpus Christi was not tried (no town on its
  records).
- **Never lose coordinates.** Geocoding can be down while sources are up;
  both merge paths coalesce lat/lng (data.js merge in `write_data_js`, D1
  upsert via `COALESCE(excluded.lat, restaurants.lat)`). Any new merge path
  must do the same.
- **Never record a blocked detail fetch as clean.** MHD portals
  intermittently 403; a fetch failure must yield "drop the inspection", not
  "no violations". Fetchers retry failures once, then drop the record.
- **MHD detail-page bans are temporary, search blocks are not.** The portal
  bans detail pages after a few hundred per ~15 min window and lifts it
  ~15 min later (measured Aug 2026); `_mhd_fetch_details_bulk` waits it
  out (`MHD_BAN_COOLDOWN`=900s, `MHD_BAN_COOLDOWNS`=16 per run, only inside
  the fetch deadline) and only then trips `_MHD_PORTAL_BLOCKED`. Don't turn
  a detail ban into a run-wide skip again.
- **King County ids depend on `data/king_county_address_map.json`.** The
  county's republished feed formats addresses differently from the retired
  one, and ids hash name+address+city; the map (new `business_id` → stored
  address) is what keeps re-inspected restaurants on their existing ids and
  coordinates. `fetch_seattle` warns loudly if it's missing. Unmapped
  businesses are new since Nov 2025 and get fresh ids; if the county
  reformats again, rebuild the map by matching on name+city+house number
  against D1 before shipping a refresh.
- **Only shipped records advance freshness, and a drop holds it.**
  `_record_group` computes a group's latest date from the records it
  returns, so undetailed batches get refetched by the lookback. A fetcher
  that drops an inspection for a TRANSIENT reason (detail ban, timeout,
  5xx, deadline) while shipping later days must call
  `_hold_freshness(source, date)`: the source's latest is then capped at
  the day before the earliest drop (and pulled back if already past it),
  or the later days would carry the latest beyond the drop and lose it
  for good. Each dropped record (`key=` its inspection or permit id) keeps
  its own count of runs in a row in which it was TRIED and failed
  (`'hold': {'date', 'failures': {'date|key': runs}}` in the freshness
  file); at `FRESHNESS_HOLD_GIVE_UP_RUNS` (3) it is given up and remembered
  (`'given_up'`), so a re-read that fails again cannot restart its count,
  two bad records a day apart cannot take turns holding the window open,
  and a bad record cannot take a newer record's first failure with it. Drops the deadline, a breaker or a block
  caused pass `counted=False`: they hold but never count, so a backlog that
  drains over several runs is never given up. No age limit. A fetcher that
  re-reads a whole window rather than one day holds with
  `_hold_window_start`, which keeps the hold date the same run to run. Omaha's two known per-record PDF crashes
  (`OMAHA_PERMANENT_500S`, matched on the 500's body) are not held; any
  other 500 is. An MHD jurisdiction the deadline cut is reported
  'skipped' (`_MHD_DEADLINE_CUT`). Fetchers that ship part of their window
  report it with `_set_group_status` so the run goes red.
  MHD details run oldest-first so a ban or deadline cut drops the newest
  tail. Register every new fetch group with `_record_group`.
- **MHD history backfills need score calibration.** Search-only backfills
  (no detail pages) are safe ONLY where the portal's `score` field is a
  real 0-100 score (Dallas, Plano, Frisco, Fort Worth, Portland-metro OR,
  Clark WA, Yolo — verified ≤4pt bias vs detail-computed scores). The
  `100−2×demerit` interim mapping runs 9-27 points HARSH for the CO
  counties and Utah County, and Tarrant's pre-2026 `score` mixes scales
  (a <50 demerit-like cluster) — backfilling those without the detail
  pass paints false F's. Detail pages rate-ban ~250 fetches/run (searches
  tolerate ~10k/day at `MHD_DELAY=3`).
- **data.js compact field names**: `n`ame, `a`ddress, `c`ity, `s`tate,
  `z`ip, `lt`/`ln` coords, `d`ate, `os` official score, `rs` risk score,
  `ws` weighted, `vg` grade, `inf` infraction chips, `vs` violation
  summaries, `h` score history, `i` restaurant id, `ic` inspection count.
  `make_restaurant_id(name, address, city)` is the stable join key across
  data.js, D1, and the API — don't change its inputs.
- **One inspection per restaurant per day: the worst.** Several permits can
  share a name and an address (Disneyland's food stands, a grocery's deli and
  bakery counters), so they share a restaurant id, and D1 keys inspections by
  restaurant + date. `_same_day_rank` picks the worst of that day (an F
  first, then the lowest score) in both output paths, so the pick no longer
  depends on fetch order. Records kept from earlier runs keep their history
  until the restaurant is fetched again.
- **A snapshot source re-reads instead of holding.** Williamson County's
  zip is one GET holding the whole history, and a pre-opening inspection
  appears in it months before its business row, so the fetcher drops the
  orphan WITHOUT a hold and instead never starts its window later than
  `WM_REREAD_DAYS` (180) ago: a hold would pin the group's latest at the
  orphan's date (a false STALE flag) and give the record up after three runs,
  sooner than the source resolves it. Re-shipping a few hundred rows a week
  costs nothing; the merge and the D1 upserts are idempotent.
- **McKinney's `total_score` is demerit points, and the item columns must add
  up.** The city's ArcGIS layer carries 47 TX-form item columns that are
  each 0 or the item's weight (3 / 2 / 1 → priority / priority foundation /
  core; verified on every live row) and a demerit total; the fetcher ships
  `risk_score = 100 − demerits` (the TX form score Dallas, Plano and
  Richardson ship) and drops and holds any inspection whose points do not
  reproduce the total, raising when more than 5% of a window disagree, so an
  emptied column can never read as a clean 100. Permit layers 0-3 are part
  of the complete-or-raise read: without them schools, mobile units and
  splash pads would ship as restaurants for good. The city label stays
  `Mckinney` (plain `.title()`, the label the Plano portal's rows already
  carry); `McKinney` would split the town.
- **Never disable TLS verification; ship the chain instead.** Lewisville's
  web service omits its GoDaddy G2 intermediate; `fetch_lewisville` verifies
  against `data/certs/lewisville_ca_bundle.pem` (certifi plus that public
  intermediate, provenance in the file header) for that host only, and a
  chain change makes the group red, never insecure.
- **MHD searches are one-day windows sorted on inspectionID.** Measured
  2026-11-03 over 16 jurisdictions x 3 weeks: the portal's default order
  (`'sort': {}`) pages unstably on inspectionDate ties — the weekly unsorted
  7-day read lost 6-9% of each week's rows (Dallas 28/256, Fort Worth
  22/192, Tarrant 27/190), always as rows repeated across pages — and ANY
  multi-day window returns one row per permit (its latest inspection), so a
  permit's Routine that earned a Follow-Up vanished (Plano 11/week).
  `MHD_WINDOW_DAYS` is 1 and `MHD_DEFAULT_SORT` is inspectionID asc for every
  plain jurisdiction (bucket entries keep the portal order; Orange County's
  newest-225 poll must stay unsorted). `_mhd_pages_consistent` fails a sorted
  query whose pages repeat a row (after one re-read) and the window then
  RAISES so the group goes red — do not check key order: Gwinnett's sorted
  pages are disjoint and complete but not in GUID order. Per-config `group`
  puts heavy jurisdictions in 'MHD metros (midweek)' (one fetch group per
  name, `_mhd_metro_groups`); per-config `exclude` drops rows by field regex;
  `_mhd_group_lag_note` leaves the group's source-lag note. The 2026-11-03
  backfill re-read the existing jurisdictions' last 90 days to recover the
  lost rows.
- **Maricopa County has no date filter outside a reCAPTCHA-gated report,
  which is never used or worked around.** Every run re-lists all ~21.7k
  permit pages (~52 min at `MCESD_LISTING_RATE`=7/s) before any detail page:
  a listing cut ships nothing (held, 'skipped'), a detail cut drops the
  newest tail (held). The county posts ~5 days late: `MCESD_POSTING_LAG_DAYS`
  (21) widens every window and `--skip-known-inspections` keeps the re-read
  cheap for embedded restaurants. One county item can cite several Food Code
  sections, each its own violation line; a header Priority tally above the
  citations read means a missed citation (page unread), below means a
  suspension/re-inspection/corrected-on-site item the county does not tally
  (logged). Grades are the county's voluntary cards, so `results` says 'Not
  Participating' for ~a third of inspections while risk_score is computed.
  Freshness has no per-permit scope: an unread permit page in a backfill run
  holds the window at the backfill's start; `scripts/backfill/freshness_copy.py`
  reports such a pinned entry, and for an existing group refuses to move it
  backwards.
- **Incomplete Maricopa reports are never scored by guessing.** Some pages
  print free-text observations or local MCEHC citations without a P/PF/C
  tier, and a header can be followed by `Data Not Currently Available`.
  These remain dropped, with an explicit reason, not clean. Answered but
  incomplete reports persist in the source's `pending_details` freshness
  metadata without the generic three-run expiry. Every weekly retry includes
  them outside the normal date window, even if their listing disappears or
  a different same-day inspection is already embedded. Only a complete read
  removes one; do not clear the queue or move freshness forward by hand.
  Since 2026-11-26 a queued report does NOT also hold the window (the queue
  already retries it; the hold pinned the county at 2026-09-20 for good) and
  only a NEW pending report, or `MCESD_UNREAD_ERROR_MIN` fresh unread pages,
  turns the run red; the known queue is a note on the report row.
  Florida's rows with blank or unreconciled official counts work the same
  way: `pending_rejected` in its freshness entry, re-checked every run
  whatever the window, red only when new, never a hold.
  See `docs/maricopa-incomplete-reports-2026-11-08.md` for the 12 live cases.
- **Tyler publicportals (Georgia, Louisiana) filter on the FACILITY's last
  inspection.** Every listed facility costs one `inspectionsData` call, a
  facility inspected twice in the window is listed once and its earlier
  visit comes from the history, which is why windows are single days and
  inspections are deduped by id. Louisiana's search answers 500 without the
  `keyword` key and treats both date bounds as strictly exclusive (one day =
  from day-1, to day+1); its printable notices are a ~4-request token bucket
  (one worker, Retry-After, `LA_PRINTABLE_MAX_PER_RUN`; a failed printable
  ships the item titles, never a hold). A Georgia row has no county: the
  county EH phone (`GA_ATLANTA_EH_PHONES`) is the only 'Atlanta' signal, and
  Gwinnett/Newton/Rockdale are re-checked by county filter every run so they
  stay on the MHD `gwinnett` slug. **A list page that 500s on every try while
  the next page reads is a poison row**: skipped, held per day
  (`list-page:<day>`, counted) and red until given up (Louisiana's
  2026-11-05 list did this for three weeks, and raising on it shipped
  nothing for the state twice); two bad pages in a row still raise.
  **Every facility whose history failed or held an inspection is read a
  second time** at the end of the run (`TP_REREAD_PAUSE`, at most
  `TP_REREAD_MAX`): Georgia serves ~0.5% of histories incomplete under load,
  all complete on a re-read, and each run's earliest one had held the
  state's freshness at the window start since September.
- **A pest item TITLE must never reach the text-F regex on its own.** The
  Texas TFER criterion 'No evidence of Insect Contamination, Rodent, Other
  Animals' trips the regex (an evidence word before a pest word) by itself
  and across the '|||' join when 'Rodent' is left in a heading: 10/72
  Lubbock and 8/136 Mesquite records were false F's before the Hedgerow
  fetchers started shipping that criterion as 'Pests and animals:
  <observation>' (`HH_PEST_TITLE`). Fort Bend prints the Food Code dead-pest
  title (6-501.112) under whichever pest code the inspector cited, and that
  wording is registered in `CITED_PEST_ITEM_TITLES` (`_item_key` drops the
  item number and page reference); so are the bare TFER/FDA item 38 title
  'Insects, rodents, and animals not present' that Harris County and Georgia
  (empty notes) print, Harris's older item-34 title and McKinney's
  no-observation placeholder: pest chip and 'Pests found', no F, whole-line
  matches only. The regex itself removes code titles that list every kind of
  pest after an evidence word (`DEAD_PEST_TITLE_RE`, `LISTED_PEST_TITLE_RE`:
  "Dead or trapped birds, insects, rodents and other pests", "No Evidence of
  Insect contamination, rodent/other animals", Ohio's "Presence of live
  insects, rodents, and other pests" that Cuyahoga prints before every pest
  line) and reads droppings written pest-first ("rodent droppings") unless a
  negation precedes them in the clause (`PEST_AFTER_RE`); measured 2026-11-03
  over 244,538 distinct lines: 228 F's gone, all from those titles, 230 come
  in, all recorded droppings. `src/grading.js` mirrors the rule (41,952
  lines checked identical). A new source that prints form titles with pest
  words needs either a neutral heading or an entry in
  `CITED_PEST_ITEM_TITLES`.
- **Hedgerow portals have no server-side date filter.** Pima's whole
  facility list (5,177 rows, 5 MB) answers only sorted by id (the SPA's own
  name sort 500s) and the county publishes ~16 days late; Mesquite's list
  hides `lastInspectionDate` and `facilityType`, so every facility's
  inspection list is read each week (~700 requests) and its schools ship;
  Lubbock pages newest-inspected first and stops at the window. Mobile units
  are dropped by type (`HH_MOBILE_TYPE_RE`; 84 Pima trucks share one
  commissary address). An unread checklist drops the inspection; an unread
  facility list holds the whole window.
- **Harris County's search field trails its inspection lists.** HCPH's
  `lastCompletedInspectionDate` stays stuck on an earlier visit for ~2% of
  establishments (visits 5-11 days later never reflected, months on; probed
  2026-11-03 over 390 establishments), so `fetch_harris` reads every window
  from `HARRIS_LC_LAG_DAYS` (28) before its start, search filter and kept
  inspections alike — do not optimise the margin away; an unreflected visit
  more than ~28 days after the previous reflected one is still missed until
  the next reflected inspection. Only ACTIVE permits are searchable (a 404
  inspection list is skipped and held; closed places' history is
  unreachable). A score of 100 has no OUT items, so its item list is not
  requested. 'Permitting' is the annual permit inspection of an operating
  place, not the pre-opening visit, and ships. The county's score sits 2
  points above 100 − demerits on ~1.5% of inspections (county score kept).
  metro is '' like `fetch_houston`'s records: set both to 'Houston' together
  or neither.
- **Backfills are chunked and absorbed, not refetched.** `--output-records`
  dumps a run's inspection records and `--input-records` feeds such files
  into a later run's data.js merge and D1 SQL with nothing fetched
  (`--cities` defaults to nothing then); a record that already carries a
  decided Jev reading keeps it, the rest are read in the absorbing run, and
  absorbed records never touch the freshness file. `scripts/backfill/`
  (README there) is the tooling: `chunk.sh` runs one window in a detached
  worktree and hands the records to `absorb.sh`, which re-merges data.js
  (to a scratch path, moved into place after the seed and size checks), lets
  Jev read the lines, writes `data/d1_seed_<date>_<tag>_<since>.sql.gz` from
  the same records and copies the chunk's freshness entries
  (`freshness_copy.py`: an existing group never moves backwards or picks up
  a backfill hold; a new group keeps its hold and the first weekly runs
  re-read from it). A hold from a long backfill can pin the first weekly runs
  at the oldest unreadable record of the whole history (Pima's 746
  unpublished checklists reached back to 2024-12): `trim_holds.py` keeps
  only the last 45 days of failures and records the rest as given up. The
  2026-11 backfills ran this way, one worktree per vendor family
  (`docs/expansion-2026-11/`), and data.js was then rebuilt once from
  `main`'s file plus every chunk's records. Do the same at the end of any
  chunk-by-chunk absorb: each absorb trims every city again, so the
  established metros are squeezed once per chunk and the city absorbed
  last hardly at all (2026-11-23, after 21 absorbs: New York 507, Kansas
  City 882; one rebuild: 610 and 730), and a branch that lives across a
  Sunday refresh conflicts on data.js and has to rebuild from `main`'s new
  file anyway (the freshness file is then `main`'s plus the new sources
  and groups). Seeds in one push load in path
  order, so a chunk that must win (Louisiana's narratives over the
  titles-only history) needs a tag that sorts last.
- **Hawaii is outside CONUS; its box is the only place its pins may sit.**
  `coords_plausible` rejects everything outside the continental box except a
  state in `OUTSIDE_CONUS_BOUNDS` (HI), which is judged on its own box
  alone, both ways: a Honolulu point is wrong for a Texas record and a
  Dallas point for a Hawaii one. `src/grading.js` mirrors it (the frontend
  zeroes implausible coordinates too). Anchorage would need an AK entry.
- **Green / yellow / red placards cap the score** (Hawaii DOH, Placer
  County; `placard_caps` in an MHD config, `_mhd_apply_placards`). Yellow
  is a conditional pass (two or more major violations), capped at 79; red
  is a closure, capped at 65 (an F, like Colorado's Closed and Waco's
  FAILED); a cap only lowers. Placer sends the placard in the row's
  `InspectionOutcome` (`placard_field`, blank on ~15% of rows), Hawaii only
  on the detail page (`MHD_PAGE_PLACARD_RE`, `_MHD_PAGE_PLACARDS`). The
  placard goes to `results`. When most of a jurisdiction's records show no
  placard the template changed and the fetch raises: a closure must never
  read as an A. Hawaii's clean reports print the whole IN/OUT grid, so an
  empty observation block there is a real clean read.
- **States are two-letter codes on output.** Portals sometimes spell the
  state out (38 VDH rows said 'VIRGINIA'), which split a town into two
  (city, state) buckets and skipped the state's coordinate box;
  `state_code()` maps names to codes in `write_data_js` (kept records heal on
  the next write) and `write_d1_sql`.

- **MHD jurisdictions carry their own row filters; do not loosen them.**
  Besides `programs` and `exclude`: `inspection_types` (Amarillo's vended
  water and farmers-market forms share the food program on other scales and
  would grade F; Minot's FOOD ESTABLISHMENT form runs under three programs),
  `purpose_allow` (read through `_mhd_row_purpose`: NET Health sends
  `purpose_retail_food`, Amarillo `purpose1`/`purpose2`), `city_map` (St.
  Louis arrives in three spellings and the id hashes the city),
  `observation_exclude` (Cheyenne's item-999 line is the inspector's praise,
  not a violation) and the `tx-demerit` scale (Smith County: the Texas
  form's demerit total, shipped as 100 − demerits and trusted like a '100'
  score; `MHD_TRUSTED_SCORE_SCALES`). The 2026-11-22 entries also drop mobile
  units by permit type and pre-opening visits by purpose.
- **A published 0 on a 0-100 MHD scale is "not scored", never a zero.**
  Orange County TX sends 0 on every `Visit` row and on unscored follow-ups,
  Dallas on its 'Retail Food Establishment (No Score)' visits, the
  Portland-metro counties on lodging visits; each shipped as an F at risk 0
  with nothing cited. `_fetch_mhd_jurisdiction_api` now grades such a row
  from its observations and leaves it out when it has none
  (`unscored_zero`); a window that is mostly zeros raises. The `tx-demerit`
  and `demerit` scales keep their 0 (no demerits). Multnomah, Washington and
  Clackamas counties also publish hotel, RV park, organised camp and pool
  inspections on the restaurant slugs (`MHD_OR_NON_FOOD_TYPE_RE`, an
  `exclude` on `inspectionType`; every food type starts with 'Food'). The
  stored rows were cleaned on 2026-11-26 (`scripts/unscored_cleanup.py`,
  export built from the site's own `/api/restaurants/:id` and `/history`):
  265 places deleted (257 of them F's: motels, RV parks, camps, and 8 Dallas
  places with only unscored visits), 846 inspections removed, 194 rows
  rebuilt or re-weighted (F's among them 89 to 7, the 7 with a pest line).
  A 0 also left a sticky F behind: a kept data.js record never upgrades an F
  without new text (`_apply_grades`), so the cleanup re-reads the latest
  lines' text decision rather than keeping the stored F.
- **Colorado's state library gives every cited provision a tier; score on
  it.** CDPHE's template prints it ("This is a Core item 6-501.111(A-B), and
  (D) ..."), and the keyword classifier ignored it: 405 of 836 Core lines
  and 894 of 1,238 Priority Foundation lines counted as Priority, so 119 of
  806 inspections were F's, 61 of them on a state verdict of Pass (found
  2026-11-23, before anything shipped). `'tiers': 'colorado'` in a config
  makes `_co_line_tier` read the printed designation, or for Pueblo, Mesa
  County and Broomfield (same library, section printed without the
  designation) the tier CDPHE states for that section (`CO_SECTION_TIERS`,
  184 sections). The six Front Range counties are on it since 2026-11-25,
  and how many lines STATE a tier depends on how each county writes: El Paso
  County (Colorado Springs) prints the designation in brackets after the
  cited provision's title ("3-501.17 Ready-To-Eat ..., Date Marking (Pf).";
  `CO_PRINTED_TIER_RE`, 94% of its lines; the bracket belongs to the
  provision quoted, which is not always the library section the line is
  filed under, and the bracket wins), Douglas cites the library section on
  74% of lines, Broomfield 31%, Adams 12%, Arapahoe 10%, Larimer 3%, Weld 2%.
- **A Colorado line that states nothing takes its form item's usual tier,
  not a keyword tier.** Those inspectors write the form's item number and a
  sentence ("13: Three dented cans ..."). The form is one statewide form, so
  `CO_ITEM_TIERS` gives 49 of its items the tier most of the state's lines
  under that item carry (counted over the 12,970 lines of the ten portals
  that state theirs; an item needs four stated lines and a majority; item 29
  has none and stays on keywords). Tested held out (table from CDPHE, Pueblo
  and Mesa; scored on the other counties' stated lines): tier right on
  80-92% of lines, inspection letter right on 83-95%, score off by 0.6-1.4
  points; keywords got 37-41%, 56-69% and 3.7-5.8. **The violation reader's
  category does not fix this** (tier right on 42-56%): `SEVERITY_MAP` gives a
  category one tier, and "no certified food protection manager" is Core on
  this form. `_co_tier` returns (tier, 'stated' | 'item'); the fetch log
  prints how many lines went each way and how many fell to keywords. It is
  an estimate, weakest on the split items (13, 14, 18, 28, 33, 51: 59-69%),
  and it lines up with the counties' own demerit totals: F's on inspections
  a county scored in its Pass band went 99 to 27 in Adams, 84 to 31 in
  Arapahoe, 65 to 8 in Larimer, 23 to 0 in Weld. Never read another
  program's report this way (a child-care item 22 is not cold holding): the
  portals are `programs: ['Retail Food']`. All ten Colorado sources are
  `tier_policy` 'fixed' in `jev_reader`, so a category can never move these
  tiers. Stored rows were regraded twice on 2026-11-25 with
  `scripts/co_regrade.py` from a per-source D1 export (stated tiers: 1,710
  restaurants; then item tiers: 3,309 more changes; F's across the ten
  sources 398 to 206, Adams 111 to 38, Arapahoe 102 to 47, Larimer 72 to 26,
  Weld 26 to 12; `docs/co-frontrange-regrade-2026-11-25.md`). Re-run it the
  same way if a table grows: export with `sources`, dry run, then `--seed`
  and `--write-data-js`.
- **A Colorado inspection with the verdict Closed is an F** (owner decision
  2026-11-25; `closed_is_f` on the ten Colorado portal configs,
  `MHD_CLOSED_RISK_CAP` 65, like Waco's FAILED). Closed means the place may
  not operate until re-inspected (110+ risk-index points or an imminent
  hazard) and can cite a single line: a water-line-break closure graded A on
  its lines. The cap only lowers a score; Re-Inspection Required caps
  nothing. The verdict arrives in `result` (Arapahoe) or `scoreDisplay`
  (CDPHE, Pueblo, Mesa County); Douglas, Larimer, Weld and Broomfield
  publish none.
- **The Front Range portals carry more than restaurants.** El Paso County,
  Adams, Weld and Larimer publish Child Care, Body Art, Waste Tire and pool
  inspections on the same slugs, and until 2026-11-25 those shipped as
  restaurants (a tyre dealer and a preschool pass `_mhd_is_food_record`).
  All six are `programs: ['Retail Food']` now. The 945 such places stored
  when this was found (601 child care, 173 body art, 162 waste tyre, 9
  pools) were deleted the same day on the owner's word
  (`scripts/co_nonfood.py`: a place is deleted only if none of its stored
  inspections is a food one, guarded again in SQL; two schools with a
  cafeteria AND a child-care licence kept their rows, rebuilt from the food
  inspection). The stored inspection type names the program on three of the
  portals; Larimer leaves it empty on child-care rows, which was checked
  against the portal row by row (218 of 220 confirmed; one is a Retail Food
  test record, excepted by id).
- **Teton County WY follow-up reports reprint the routine inspection's
  lines** (12 lines in, 12 lines out, a dated note on the corrected ones),
  so a follow-up grades like the visit it follows. That is the county's
  record, not a parser fault; Jackson's low grades (about a third F) come
  from its inspectors citing a median of five items a visit, and hold under
  form-section tiers and distinct-item counting alike (checked 2026-11-23).
- **Montana's state portal logs things that are not inspection reports.**
  40-55% of its retail-food rows are purpose `Other`: licence notes and the
  placeholders of the counties that keep inspections in their own systems
  (Missoula, Gallatin, Flathead: the Paragon source; Ravalli, Carbon and
  Fergus publish nothing). They have no observations and would each ship as
  a clean 100, so `purpose_allow` keeps Routine and Follow-up only. A
  `Routine` row can be a logged paper report too, so a zero-observation
  Montana inspection ships only when its printable PDF shows the compliance
  grid (`clean_proof: 'printable-grid'`, `_mhd_prove_clean`: 29 IN/OUT/N/A/N/O
  marks on a real report, 0 on a logged one); no grid = dropped for good, an
  unread printable = dropped and held. The portal builds each PDF on request
  (~20 s), one at a time. Itemised reports only became common in August 2026.
- **Waco: a FAILED inspection is an F** (`WAC_FAILED_RISK_CAP` 65; owner
  decision 2026-11-23). The layer carries a demerit total and a status, no
  items. FAILED is the district's verdict that a re-inspection is required
  (items or an imminent hazard not corrected on the spot, repeat violations,
  or more than 30 demerits), which the total does not carry: scored on
  demerits alone not one Waco restaurant was an F while 21% of inspections
  failed. `-INACTIVE` names (closed permits), mobile units and dated events
  are skipped; the layer's OBJECTIDs are renumbered on reload and are never
  an id.
- **El Paso and Waco publish a score and nothing else**, and neither holds
  freshness: each reads its whole window or raises, and re-reads a margin
  every week (35 and 60 days). El Paso's form sits behind Cloudflare: a
  challenge or a 401/403 reports the group `blocked` (never zero rows), and
  a window with too few rows or a changed table header is an `error`. Its
  `toDate` is exclusive. If GitHub's runners are refused, the fix is the
  relay (add the host to `ALLOWED_HOSTS` in `functions/api/relay.js`).
- **Garland is a snapshot: the layer keeps each permit's latest inspection
  only.** Every run re-reads 90 days and never holds; a missed week loses
  the earlier visit of any permit inspected twice. An inspection ships only
  when its item points add up to 100 − the city's score (item weights are
  Garland's own: items 1-20 are 3 points, 21-33 are 2, 34-47 are 1).
- **Napa County CA is a snapshot of each facility's latest inspection.**
  `fetch_napa` reads the county's Tyler healthinspections.us portal
  (`ca.healthinspections.us/napa`, the older Garrison template): the search
  filters on the facility's LAST inspection date and pages ten facilities at
  a time; each row's report prints the county's Total Points Deducted (A
  0-10, B 11-20, C 21-30, F over 30, which is DineScores' bands on 100 -
  points) and the points of every OUT item (6 major / 2 minor risk factor,
  1 good retail practice). Every run re-reads `NAPA_REREAD_DAYS` (90) and
  never holds; the first weekly run after the source's `history_from` is
  missing from the freshness file reads the whole snapshot from
  `NAPA_HISTORY_START` and sets it (the first version keyed this on "no
  limit", which CI never is: the 2026-11-26 run read 90 days). An inspection ships only when its
  item points add up to the printed total (a 2024 form printed item 23
  twice and does not); a report with no total (re-opening after a closure)
  is skipped; a checked Facility Closed box caps the score at 65. Mobile
  units, schools (NVUSD names, one spelled 'ELEMENTERY') and caterers listed
  outside the county are left out. The county posts weeks late (newest row
  2026-10-28 on 2026-11-26).
- **Sonoma County CA publishes report PDFs and no score.** The county's
  Socrata dataset (`8r44-w5qd`) stopped in 2018; `fetch_sonoma` reads its
  OnBase Public Access API (`onbaseprodext.sonoma-county.org/EHSEP5/api`):
  custom query 204, a wildcard Facility Name keyword search filtered on the
  date a document was STORED, 15-day chunks (~1,000 documents each, not
  capped), `Inspections - FOOD` rows only, then one PDF per report
  (`GET /Document/{id}/`, ~5 s each, `SONOMA_WORKERS`=3). The listing's
  inspection date is the report's, so the search reaches back
  `SONOMA_STORED_MARGIN_DAYS` (21) for late filing and a report listed more
  than 7 days before the window is never downloaded. Items print
  `Status: MAJ|MIN`; MAJ is Priority, MIN on CalCode items 1-23 Priority
  Foundation, 24+ Core, scored 5/2/1 by the fetcher (no score published;
  the green/yellow/red placard is not in the text). Two or more majors cap
  the score at 79 ('Conditional pass', the county's yellow-placard rule),
  a closure written in General Comments at 55. A report is unread, dropped
  and held when an item heading has no Status, the Status lines and items
  disagree, or more items are cited ("Inspector Comments"/"Violation Code")
  than read: a shape change must never read as a clean 100. Kept out
  without a hold: scans (DisplayType not `OleActivePage`), text with no
  program element (lab reports), mobile (3M), school (3S), cottage food
  (3C) and caterer (3R40) elements, and attempted visits ("inspection was
  attempted": the "facility is closed" there is the owner's, not the
  county's). The empty General Comments section once swallowed every item
  (`\s*` ate the newline before Observations), so "keep doors closed" read
  as a closure; the regex uses `[ \t]*`. First run: the last 90 days.
- **Corpus Christi (InspectHub) has two page shapes that are read and
  skipped, never held:** a food visit with no Score line, and a score
  printed with "violation information is not available" (holding a shape
  that never changes would pin every window). An unpublished id answers 200
  with no name and IS held. The listing row, the page's Score line and 100
  − the item points must agree. No page carries a town: every record is
  'Corpus Christi' (Port Aransas and Robstown included), which is why about
  a fifth do not geocode.
- **Abilene's `requestedDate` is the next visit's due date, not the
  inspection date.** The fetcher searches requested dates from 60 days
  before the window with no upper bound and ships by COMPLETED date (the
  longest gap seen was 49 days; a visit done later than 60 days after its
  due date is missed). The date filter silently drops rows with no scheduled
  date, so each new food type is also listed unfiltered. The score counts
  each distinct form item once (100 − 3P − 2PF − C); a mismatch drops and
  holds. ~600 requests a week.
- **Wichita Falls is a Hedgerow tenant whose citations state no severity.**
  The tier comes from the category's (P)/(PF)/(C) suffix, with the two
  measured corrections in the config; a citation with no readable tier drops
  and holds its inspection, it never becomes core. The differences are
  config keys that Pima, Lubbock and Mesquite do not carry.
- **Paragon Inspections Online (Missoula, Gallatin, Flathead MT; Kansas City
  MO) prints rows that are not findings and a tier that is not a severity.**
  Flathead records its bonus credit for temperature logs as Risk Factor rows
  ("this facility exceeds regulatory requirements"; a third of its
  higher-tier rows): `IO_COMMENDATION_RE` drops them AFTER the report is
  checked against the document's counters, which count them. Montana's
  higher tier is a section of the state form holding Priority and Priority
  Foundation provisions alike, so the standard titles in `IO_MT_PF_TITLES`
  (provisions the FDA Food Code marks Pf at most) ship as
  priority_foundation; scored 5 points each they made 11% of Missoula's
  inspections F while the county's own letters had none below C. An unknown
  title stays priority. Kansas City's Critical is the city's own severity
  and is never re-tiered. Flathead's published letter only caps
  (`IO_GRADE_RISK_CAP`: B 89, C 79, D and F 69; an unknown letter is an
  unread inspection). The vendor score column the agencies switched off is
  never shipped. A report with fewer rows than its counters is unread
  (dropped and held), more rows ship. One host for all four databases: one
  shared throttle, 1 request a second; each window starts 14 days early.

## Jev violation reader

`jev_reader.py` hands the READING of each violation line to TypeSafe's Jev
model (category, hazard, summary phrase, all picked from fixed lists) and
leaves the SCORING in code: `SEVERITY_MAP`, the 5/2/1 points, the A/B/C/F
cutoffs and `F_FORCING_HAZARDS` never move because of an answer. The
weekly workflow runs it with `JEV_MODE=decide` and `JEV_DECIDE=hazard,summary`:
Jev decides the text-forced F and the public summary phrases; category
(which moves points on the computed-score sources) stays in shadow, asked
and cached but not applied. `docs/jev-shadow-report.md` has the measured
impact and the open policy decisions; `scripts/jev_shadow.py` reproduces it.

- **One pass, after the fetch phase.** `_run_jev_pass` in `main()` calls
  `jev_reader.apply_to_inspections` over every fetched record on its own
  `--jev-deadline-minutes` budget (20 in CI, inside the 350-minute job), then
  the output paths read the per-record `_jev` result (forces_f, chips,
  summaries). Nothing in the fetchers changed. A data.js record kept from an
  earlier run keeps the text F decision of the run that last read its full
  text (`_apply_grades`): re-reading its 200-character verbatims with the
  regex undid Jev's F's a week later. Score rules still re-apply to it.
- **Fail open.** Any error, timeout, 429/529 after retries, malformed answer
  or a top answer below `CONFIDENCE_FLOORS` (0.6) means today's regex answers
  that line; a record that raises inside the pass is left as the regex read
  it; a failure of the whole pass is logged and the run continues. The
  refresh must never fail or ship less because of Jev.
- **Policy lives in code.** `F_FORCING_HAZARDS` (vermin, sewage; not flies,
  not `pests_unspecified`), `SCORE_FROM_TIERS_SOURCES` / `SOURCE_TIER_SCORE_SOURCES`
  (score computed from tiers the source states: Boston, Detroit, Louisiana,
  Maricopa, Pima, the four Paragon portals and the ten Colorado portals) /
  `SOURCE_TIER_SOURCES`
  / `BOUNDED_TIER_SOURCES` (which sources may be re-scored or re-tiered), the
  chip and summary-severity maps. A source-stated tier never moves (Omaha,
  Santa Clara, San Diego, Orange County, McKinney, Georgia, Louisiana,
  Maricopa County, Harris County (P/F/C violation groups), the Hedgerow
  portals (Pima County, Lubbock, Mesquite, Wichita Falls), Garland, Corpus
  Christi and Abilene (Texas-form item points or stated groups), Napa
  County (the county's points per CalCode item), Sonoma County (MAJ/MIN
  by CalCode item, scored by the fetcher), the Paragon
  portals (tier fixed by `_io_shipped_rows`) and the ten Colorado portals
  (the state library's designation, or the form item's usual tier where a
  line states none: `_co_tier`) included: their agencies state it;
  McKinney's TX-form item
  points are the city's tiers and its `risk_score` is the form score 100 −
  demerits, never recomputed; Georgia's (p)/(pf)/(c) designations carry its
  official score; Louisiana's Critical/Non-Critical and Maricopa's
  Priority/Priority Foundation/Core citations are scored `100 − 5P − 2PF − C`
  by the fetcher, never re-tiered).
- **An F needs a named vermin and a firm answer.** A `vermin_observed` answer
  counts only when the line names one (`NAMED_VERMIN_RE`, after removing the
  negated titles counties print on every pest item and generic code-title
  lists like "flies, insects, rodents and other pests"); otherwise the regex
  answers that line. Jev had read crickets, bees, spiders, pill bugs,
  grasshoppers and a WD-40 can as vermin. An answer that would ADD an F the
  regex does not give must clear `F_ADDING_HAZARD_FLOOR` (0.7); removing one
  needs only the 0.6 floor. Measured 2026-10-10: Jev's added F's between 0.6
  and 0.7 were mostly drains read as sewage, its removed F's mostly right.
- **Public wording and form titles live in code too.** `SUMMARY_DISPLAY_NAMES`
  (pipeline) renames two phrases on output only ("Roaches found", "Rodents or
  droppings found"); Jev's option list keeps the old wording so the cache
  stays valid, and `data/d1_seed_2026-10-10_summary_display_names.sql.gz`
  renamed the stored rows. `FORM_TITLE_PHRASES` gives a plain phrase to the
  bare item titles some forms cite with no observation (San Diego, Omaha, LA
  County), only when Jev declines and only for exact title wordings; pest,
  flies and sewage titles are deliberately not in it. A cited pest item printed
  without an observation is a finding of unnamed kind (`CITED_PEST_ITEM_TITLES`,
  `CITED_PEST_ITEM_PREFIXES`): pest chip and "Pests found" (NYC's flies item:
  "Flies/Gnats present"), no F, like Montgomery County's item. That covers San
  Francisco's and NYC's standard descriptions (their feeds never carry the
  observation), the "No rodents, insects, birds or animals" title (LA County,
  San Diego, Santa Clara) and the Food Code pest sections Houston, Delaware,
  Detroit and Cincinnati print. Titles match the whole line only: a title
  followed by an observation stays with Jev, so a named roach keeps its F.
- **Pinned model + versioned questions.** `JEV_MODEL` and
  `QUESTION_SET_VERSION` are part of every cache key; a wording change must
  bump the version (a fingerprint test enforces it). Cache files under
  `data/jev_judgments/` are append-only gzipped JSONL, loaded in name order,
  later file wins per key; never edit one. The weekly run commits its new
  file with data.js.
- **No F from a batched answer.** A pest/sewage hazard (or a `none` on a
  text naming one) returned inside a multi-line request is re-asked alone
  and the single-line answer is cached. Jev mixed up neighbouring lines in
  ~0.05% of batched texts, once on a line stored 1,088 times.
- **Only the violation line is sent** (no name, address, id, city, date).
  The key lives in `TYPESAFE_API_KEY` only (an Actions secret; unset = the
  reader switches itself off); it is scrubbed from logs and exceptions, and
  request bodies are never logged. Stdlib only (`urllib`).
- **Coverage is in the run summary.** The pass writes a `jev` block into
  `data/source_freshness.json` (lines judged, cached, fell back and why,
  decisions per question, F against the regex, re-tiered/re-scored counts)
  and the "Report source outcomes" step renders it; a failed pass or a run
  that fell back on most lines raises a warning annotation.
- **Regrade from stored text** (`scripts/jev_regrade.py`, run 2026-10-11).
  Rows written before decide mode carried the regex's reading; the regrade
  re-read every restaurant's stored latest inspection (exported from D1 by
  `export-d1.yml`) with the weekly decide rules and shipped the result as a
  seed of guarded UPDATEs (`id` + `inspection_date` + `updated_at` must still
  match the export, so a row the weekly rewrote since is skipped and a reload
  changes nothing more) plus the same decision for embedded data.js records
  holding the same inspection. Scores never move. D1 keeps 20 lines per
  inspection, 400 characters each, so an F changes only where today's regex
  reproduces the stored F decision from the stored lines and no line was cut
  (San Francisco, NYC and Houston print only standard wording, so a cut there
  hides nothing); chips and summaries follow the same completeness rule.
  Re-run it the same way if a rule change ever needs to reach old rows:
  export, `--judge` for the uncached lines, then `--seed` and `--data-js`.
  `docs/jev-regrade-2026-10-11.md` has the run's numbers and checks.
- Tests: `python3 -m unittest tests.test_jev_reader` (no network).

## Tests

`python3 -m unittest discover -s tests` (no network: every fetcher test
uses captured fixtures under `tests/fixtures/`) and `node
tests/test_relay.mjs` for the relay function (plus `node tests/test_places.mjs`,
`node tests/test_near_me.mjs`, `node tests/test_source_lag.mjs` and
`node tests/test_seo.mjs` for the frontend logic and crawlable pages). Run them before
pushing.

## Local commands

```
npm run dev                      # Vite dev server :3000 (uses public/data.js)
npm run build                    # REQUIRED after src/ changes; commit output
python3 dinescores_pipeline.py --mode weekly --cities <slugs...> \
    --output /tmp/out.json --output-data-js public/data.js \
    --merge-existing-data-js public/data.js \
    --freshness-file data/source_freshness.json
```

Pipeline has no third-party deps. City slugs are in the `--cities`
dispatch at the bottom of the pipeline file.

## Conventions

- Work on a feature branch, PR to `main`, squash-merge. Pushing to `main`
  triggers a Pages deploy (~2 min).
- The data pipeline is stdlib-only Python; keep it that way (GitHub runner
  installs nothing).
- Grades: A=90+, B=80-89, C=70-79, F=<70 on `risk_score` after weighting;
  see `grading.js` (frontend mirror) and `calculate_vetted_grade` (source
  of truth) — change both together.
