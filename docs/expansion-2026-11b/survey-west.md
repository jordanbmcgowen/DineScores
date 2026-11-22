# West Texas and border: inspection-source feasibility survey

Probed live on 2026-11-22 (04:25-04:55 UTC) with curl and a Chrome User-Agent, through this session's agent proxy.
Everything below was fetched by me unless it is marked **unverified**. Raw captures are under
`(raw captures, not kept in the repo)`
(file names are given per section); the throwaway parsers are in `.../survey/west/scripts/`.

Populations are 2020 Census counts quoted from memory (the Census API now answers "Missing Key", so they were
not re-checked live). Treat them as approximate.

## Ranking (people covered per unit of build work)

| # | Jurisdiction | Verdict | Platform | Pop. (approx.) | Score | Inspections/week | Biggest risk |
|---|---|---|---|---|---|---|---|
| 1 | El Paso city + county | **BUILD** | City-built PHP form over Accela data (`foodscores.elpasotexas.gov`) | 866k county | Official 0-100, higher better (36-100 seen) | ~120-175 | Score only: no violations, no inspection id; Cloudflare-fronted |
| 2 | Abilene / Taylor County | **BUILD** (new JSON client) | Tyler EnerGov "Civic Access" (`civicapi`) | 143k county | Official 0-100 = 100 - 3P - 2PF - C (66-97 seen) | ~30-45 | Undocumented API; street-only addresses; public list may be incomplete |
| 3 | San Angelo / Tom Green | **HARD** | Monthly PDF (Excel export) on CivicPlus | 120k county | 0-100, higher better (52-100 seen) | ~30-35 (latest visit per place only) | Monthly snapshot, no violations, no ids, no history |
| 4 | Laredo / Webb County | **BLOCKED** (stale + broken) | Tyler "publicportals" JSON API (same family as Georgia/Louisiana) | 267k county | Could not be read (detail endpoint 500s) | ~100 while it was live | Nothing published after 2026-01-30; `inspectionsData` answers 500 |
| 5 | Odessa / Ector County | **BLOCKED** (portal offline) | Legacy Accela EnvisionConnect PressAgent (PHP) | 165k county | 0-100 (unverified) | unknown | `pressagent.envisionconnect.com` no longer resolves (NXDOMAIN) |
| 6 | Brownsville / Cameron County | **NOTHING ONLINE** (and blocked) | none; Accela portal EnvHealth module is login-only | 421k county | 0-100 (unverified, media) | n/a | City site returns a Cloudflare block page |
| 7 | McAllen / Hidalgo County, Edinburg, Mission, Pharr, Harlingen | **NOTHING ONLINE** | none (Edinburg/Pharr permit portals carry no scored inspections) | 871k county | n/a | n/a | Reports only by public information request |
| 8 | Midland | **NOTHING ONLINE** | none | 170k county | n/a | n/a | Reports only on request |

Build order suggestion: El Paso first (one POST per week, largest population in the set), then Abilene (the only
source here with full violation text and stated tiers). San Angelo is cheap in requests but poor in content.
Re-check Laredo and Ector in a few months: both would be near-free on existing or planned fetcher families
if their portals come back.

---

## El Paso (City of El Paso Department of Public Health; also El Paso County)

**1. Agency, population, URL.** City of El Paso Department of Public Health, Food Inspection program. The portal's
own disclaimer says it covers "the City of El Paso, Anthony, Clint, Horizon City, Socorro, Vinton and County of
El Paso" (city ~679k, county ~866k). Public page: `https://foodscores.elpasotexas.gov/` (linked from
`elpasotexas.gov/public-health/services/food-inspection` as "Check Food Establishment Scores").

**2. Platform.** A custom city PHP app (`x-powered-by: PHP/8.4.28`, nginx behind Cloudflare). The Permit IDs
(`HFFX11-00254`) are Accela record ids, so the data is an extract of the city's Accela system, but the page is not
Accela Citizen Access. Not one of the pipeline's existing vendor families.
`inspections.myhealthdepartment.com/epcph` is **El Paso County, Colorado** (Colorado Springs, already covered), not
this agency; one third-party blog claims otherwise and is wrong (verified: the page title is "El Paso County Public
Health", address Colorado Springs).

**3. Machine endpoint.** One endpoint, a form POST to the page itself, returning the whole result as an HTML table
(DataTables pages it client-side; the server sends every row).

```
curl -A "<Chrome UA>" -X POST https://foodscores.elpasotexas.gov/ \
  --form-string businessName= --form-string address= --form-string city= --form-string zipCode= \
  --form-string fromDate=2026-11-09 --form-string toDate=2026-11-15 \
  --form-string minScore= --form-string maxScore= --form-string search=
```

- Status 200, `text/html`, 118 KB for a week, ~12 s server time regardless of window size.
- `multipart/form-data` (what the form declares); `fromDate`/`toDate` are `YYYY-MM-DD` and are the only required fields.
  Optional: `businessName`, `address`, `city`, `zipCode`, `minScore`, `maxScore`, `mobile=Mobile`.
- No cookie, token or session needed (a `PHPSESSID` is set but was never sent back).
- Table `#resultsTable`, 8 `<td>` per row: Permit ID, Date (`MM/DD/YYYY`), Business Name, Address, City, Zip Code,
  Sanitary Code (inspection type), Score.
- A "Download CSV" button fetches `https://foodscores.elpasotexas.gov/output.csv` (200, same 8 columns). It is a
  single server-side file rewritten by the last search of *any* visitor, not per session: do not use it, parse the HTML.

Sample row (raw/elpaso_w1.html):

```
HFFX11-00387 | 11/13/2026 | LIN'S GRAND BUFFET | 1921 ZARAGOZA RD, BLDG# C, EL PASO, TX 79938 | EL PASO | 79938 | 1310 Routine Inspection | 90
```

**4. Date filter, paging, caps.** Server-side date filter, proven:

| Window | Rows | Dates in the rows |
|---|---|---|
| 2026-11-09 .. 2026-11-15 | 163 | 11/09-11/14 only (none on 11/11, Veterans Day) |
| 2026-11-16 .. 2026-11-22 | 175 | 11/16-11/20 only |
| 2026-10-05 .. 2026-10-11 | 117 | 10/05-10/09 only |
| 2026-08-24 .. 2026-11-22 (90 d) | 2,013 | 1.3 MB, 13 s |
| 2021-11-01 .. 2021-12-15 | 925 | |
| 2019-01-01 .. 2021-11-30 | 14,521 | 9.5 MB, 22 s |
| 2010-01-01 .. 2018-12-31 | 29,942 | 19.6 MB, 34 s |

No paging and no row cap observed (29,942 rows in one response).

**5. Score.** Official numeric score, 0-100, higher is better. The portal states: 70 or above passes, 69 or below
initiates closure action, 60 or below closure. Observed: 60-100 in the last 90 days (1,220 in the 90s, 411 at 100,
302 in the 80s, 50 in the 70s, 4 in the 60s); 36-100 over 2010-2018. No letter grade. `1300 Site Assessment` rows
have a blank score (26 of 2,013), so skip them.

**6. Violations.** Not published. The disclaimer says the scores "are a summary only" and the full report is an Open
Records request (`cityrecords-elpaso.govqa.us`, fees apply). No severity tiers. The city's Accela Citizen Access
(`aca-prod.accela.com/ELPASO`) lists modules Building, City, EPDOT, Enforcement, Fire, Licenses, Planning, Police and
no Health module; whether HFFX records are reachable under "Licenses" was not tested (**unverified**), and it would
be a WebForms scrape in any case.

**7. Coordinates / address.** No coordinates. The Address cell is the full string
(`8825 NORTH LOOP DR 125, EL PASO, TX 79907`) and City and Zip are repeated in their own columns. City values in 90
days: EL PASO 1,716, SOCORRO 58, CANUTILLO 49, HORIZON CITY 40 (+3 "HORIZON"), ANTHONY 38, SAN ELIZARIO 36,
EL PASO COUNTY 24, FABENS 20, CLINT 15, VINTON 7, TORNILLO 6, and one typo `CANUTILLO79835`. "EL PASO COUNTY" is a
placeholder, not a town: take the city from the address string or fall back to El Paso for geocoding.

**8. Ids.** Facility: Permit ID (`HFFX11-00254`; also `HFRC`/`HFSV` prefixes before 2022). 1,972 distinct permits
in 90 days; 5,012 across 2019-2021. Inspection id: none. Key an inspection on permit + date + type code; (permit,
date, type) repeated twice in 2,013 rows, so dedupe. Two permits can share one name and address on one day
(MONTANA VISTA FOOD STORE, HFFX11-00318 = 84 and HFFX11-00319 = 93 on 11/13/2026), which the existing
worst-of-the-day rule handles.

**9. Volume, history, freshness.** 117-175 rows a week, 2,013 in 90 days (~157/week), almost all `1310 Routine
Inspection` (1,982), with `1320 Re-inspection` (5) and `1300 Site Assessment` (26). History reaches back to
2010-01-04. Newest inspection seen: **2026-11-20** (two days before the probe), so posting lag is a day or two.

**10. Requests.** Weekly: 1 POST (use a 14-28 day window for late postings; it costs the same). 90-day backfill:
1 POST. Whole history since 2010: 2-3 POSTs.

**11. Blockers and risks.**
- No login, no CAPTCHA, `robots.txt` is 404. Cloudflare is in front and injects its JavaScript-detection snippet,
  but every curl request was served 200. Not tested from GitHub-hosted runners; if Cloudflare starts challenging,
  that is a block, not something to work around.
- Score-only: a restaurant gets the city's number and nothing else (no chips, no summaries, no text-forced F).
- No facility type: convenience stores, dollar stores, adult day cares and schools are in the list. The Tyler
  fetchers' institution denylist would apply.
- The "Only display mobile establishments" filter returned 0 rows for 2026-10-01..11-22 (meaning unknown).
- `output.csv` is shared between visitors (race), so never read it.

**12. Verdict: BUILD.** A one-request date-window HTML table. Closest existing shapes: `fetch_dc` (window search
returning all rows in one page) for the fetch, and the score-only handling planned for Alabama. Parse with a regex
over `<tr>`/`<td>`; ship `risk_score = score`, `metro = 'El Paso'`.

Requests made: 3 GET + 9 POST. Captures: `elpaso_home.html`, `elpaso_w1.html`, `elpaso_w2.html`, `elpaso_w3.html`,
`elpaso_90d.html`, `elpaso_old1..3.html`, `elpaso_mobile.html`, `elpaso_output.csv`.

---

## Laredo / Webb County

**1. Agency, population, URL.** City of Laredo Public Health Department, Environmental Health Services (Laredo
~255k, Webb County ~267k). The portal's City filter lists Laredo plus Webb County communities (Rio Bravo, El Cenizo,
Bruni, Mirando City, Oilton, Aguilares and others), so the city program covers the county.
Public URL: `https://laredo-tx.healthinspections.us/`.

**2. Platform.** Tyler Technologies "publicportals" SPA (Backbone/RequireJS, `powered by Tyler Technologies`), the
same JSON API family as `ga.healthinspections.us` and `la.healthinspections.us` that `_tp_fetch` already reads. It
would be a config entry, not a new fetcher.

**3. Endpoints** (API root `https://laredo-tx.healthinspections.us/API/index.cfm/`, headers as `TP_HEADERS`):

| Call | Result |
|---|---|
| `GET clientData` | 200. `recaptchaActive: "no"`, map centre 27.5036,-99.5076 |
| `GET filters` | 200. City dropdown (21 values), Permit Type (`Food`, `Mobile Food`, `Temp Food`), `Last Inspection Date` date filter |
| `GET facilities/0` | 200, 5 rows, newest first |
| `GET search/{json}/{page}` | 200, 5 rows per page, `[]` past the end |
| `GET inspectionsData/{id}` | **500 "Internal server error"** for every facility tried |

Search URL exactly as `_tp_search_url` builds it, for example January 2026 page 0:

```
https://laredo-tx.healthinspections.us/API/index.cfm/search/%7B%22keyword%22%3A%22%22%2C%22from%22%3A%22MjAyNi0wMS0wMQ%3D%3D%22%2C%22to%22%3A%22MjAyNi0wMS0zMQ%3D%3D%22%7D/0
```

Sample row (raw/laredo_fac0.json):

```json
{"mapAddress":"210  Salinas     \r\nLaredo, TX 78040 ",
 "columns":{"0":"210  Salinas     \r\nLaredo, TX 78040 ","1":"Phone Number: [phone removed]",
            "2":"Permit Type: Food","3":"Last Inspection Date: 01-30-2026"},
 "id":"Mjk3Mw==","name":"La Unica"}
```

**4. Date filter, paging, caps.** Server-side filter on the facility's last inspection date (the publicportals
behaviour), proven: `from=2026-01-01,to=2026-01-31` returns January rows; `from=2025-12-01,to=2025-12-31` returns
December rows; `from=2026-02-01,to=2026-11-22` returns `[]`. 5 rows per page, 0-based, no cap seen (page 64 of the
December window still returned rows).

**5. Score.** Could not be read: scores live in `inspectionsData`, which fails. A third-party aggregator describes a
0-100 score plus a letter "sanitation grade" (**unverified**).

**6. Violations.** Same: not readable live. The aggregator reports per-violation rows with corrected-on-site and
repeat tags (**unverified**), which is what this API family returns elsewhere.

**7. Coordinates / address.** No coordinates in the list rows. `mapAddress` is `STREET \r\nCITY, TX ZIP`, the format
`_tp_parse_address` already handles.

**8. Ids.** Facility: base64 of a number (`Mjk3Mw==` = 2973). Inspection ids are in the failing endpoint.

**9. Volume, history, freshness.** The newest inspection in the portal is **2026-01-30**; nothing later exists
(empty result for February onward). The data is ten months stale. While it was live it was busy: the December 2025
window still returned rows dated 12-04 at page 64, so more than 325 facilities had their last inspection that
month (roughly 100 a week). History depth unknown (the aggregator holds rows from 2021-11, **unverified**).

**10. Requests.** If it worked: the Georgia/Louisiana pattern, one single-day search per day (about 20 pages a day)
plus one `inspectionsData` per listed facility, so ~250 requests a week and ~3,000 for 90 days. Today: zero useful.

**11. Blockers and risks.**
- **Stale**: nothing published since 2026-01-30.
- **Broken detail endpoint**: `inspectionsData` answered HTTP 500 (IIS error page, header `x-taffy-reloaded: true`)
  for five different facility ids and again 25 minutes later. Control: the identical call on
  `la.healthinspections.us` answered 200 in the same minute, so it is this tenant, not the client.
- No login, no CAPTCHA (`recaptchaActive: no`), `robots.txt` 404.
- `www.cityoflaredo.com` (the city's own site) returns **Akamai 403 "Access Denied"** to this client, so the city
  pages could not be read directly; I stopped there. Search-engine excerpts of the Health Department pages show no
  link to a newer portal (**unverified**). The Laredo open-data portal (`data.openlaredo.com`, CKAN) has no food
  inspection dataset (searched `food`, `restaurant`, `health inspection`: 0, 0 and 1 unrelated result).

**12. Verdict: BLOCKED** (stale since 2026-01-30 and details unreadable). If the tenant is repaired it becomes
**BUILD** as a third config on the Tyler publicportals family (`_tp_fetch`), like Georgia and Louisiana. Worth one
`facilities/0` + one `inspectionsData` probe every few months.

Requests made: ~30. Captures: `laredo_home.html`, `laredo_main.js`, `laredo_backbone-app_*.js`,
`laredo_clientData.json`, `laredo_filters.json`, `laredo_fac0.json`, `laredo_s_*.json`, `laredo_dec_p*.json`,
`laredo_insp_*.json` (the 500 bodies), `la_control*.json`.

---

## Brownsville / Cameron County

**1. Agencies, population, URLs.** City of Brownsville Department of Health, Wellness and Animal Services, Health
Inspections division (city ~187k; the page says it regulates "over 2,000 food facilities"):
`https://www.brownsvilletx.gov/621/Health-Inspections`. Cameron County Public Health, Environmental Health Program
(county ~421k; unincorporated areas and towns without their own program):
`https://www.cameroncountytx.gov/publichealth/environmental-health/eating-drinking-food-service/`.

**2. Platform.** None for inspection results. Brownsville runs Accela Citizen Access
(`https://aca-prod.accela.com/BROWNSVILLE/`, modules Building, EnvHealth, Licenses, Planning, PublicWorks).

**3. Endpoints.**
- `https://www.brownsvilletx.gov/621/Health-Inspections` -> **HTTP 403, Cloudflare "Attention Required!" block page**
  (`server: cloudflare`, `cf-ray` present). This is the site refusing the client, not the agent proxy. I stopped on
  that host. The search-engine excerpt of the page lists program text and sub-pages (Inspection Process, Foodborne
  Illnesses, Establishment Closure, Mobile Units...) and no results list (**unverified**).
- `https://aca-prod.accela.com/BROWNSVILLE/Cap/CapHome.aspx?module=EnvHealth&TabName=EnvHealth` -> 200 after a
  redirect to `Login.aspx?ReturnUrl=...`: the Environmental Health module requires an account. Not pursued.
- Cameron County page -> 200 (596 KB). Its only links are application forms (Establishment Application, Temporary
  Permit, Portable Food Booth requirements) and food-safety guides. No inspection results.

**4-10.** Not applicable: there is no published data to window, page, score or count. For reference only
(**unverified**, from KVEO "Food 4 Thought" reports): Brownsville scores on a 0-100 scale (53, 60, 79, 88 and 100
quoted in 2025-2026 stories) and the station obtains reports from the departments.

**11. Blockers.** Cloudflare block on the city site; login wall on the Accela EnvHealth module; nothing published by
the county.

**12. Verdict: NOTHING ONLINE** (Brownsville additionally BLOCKED at the web tier). A public-information request to
the city for a periodic extract is the only route.

Requests made: 2 to brownsvilletx.gov (both 403), 2 to Accela, 1 to the county. Captures: `brownsville_621.html`
(the block page), `brownsville_aca.html`, `brownsville_aca_eh.html`, `cameron_food.html`.

---

## McAllen / Hidalgo County (plus Edinburg, Mission, Pharr, Harlingen)

Six agencies, one finding: none publishes scored inspection results. Hidalgo County ~871k people in total.

**Hidalgo County Health & Human Services, Environmental Health Division** (unincorporated county and
"municipalities that do not operate their own health department services"):
`https://www.hchd.org/156/Food-Regulation` -> 200. Links: Opening a Food Establishment, Temporary Food Permits,
Mobile Food Vendors, Fee Schedule, Cottage Food, FAQs, Report an Issue. No results. A KVEO story states its
reporters "obtained the inspection report via a public information request" (**unverified**, media); the same
stories quote county results as "36 violations" and "a score of 27", which reads like demerits (**unverified**).

**City of McAllen Health & Code Enforcement** (~142k): `https://www.mcallen.net/departments/code` -> 200. Only forms
and applications. Its "Permit Portal" (`https://onlinepermits.mcallen.net/Portal/default.aspx`) is a contractor
building-permit portal (guides: Contractor Registration, Sub-Permits, Request Inspections), not health results.
Media quote 0-100 scores (**unverified**).

**City of Edinburg Health Division** (~100k):
`https://www.cityofedinburg.com/departments/health_code_animal_care_services/health_division.php` -> 200. The city
has a classic Tyler EnerGov Self-Service portal, `https://development.cityofedinburg.com/EnerGov_Prod/SelfService`,
which answers anonymously:
- `GET /api/tenants/gettenantslist` -> tenant `EdinburgTXProd`
- `GET /api/energov/search/criteria` -> the search template
- `POST /api/energov/search/search` (headers `tenantId: 1`, `tenantName: EdinburgTXProd`,
  `Tyler-TenantUrl: EdinburgTXProd`; body = the template with `FilterModule` 1..12, `PageNumber>=1`, `PageSize`,
  `SortBy`) -> 200
- `GET /api/energov/inspections/getById/{id}` -> 200

It holds health **permits** (`HLTH-2026-2891`, "Health Permit - Food Establishment"; temporary event permits) and
"Health Department" **inspections** (`IHLTH-2026-196630`, status Passed/Requested, linked to a permit). The
inspection record has dates, address, status and the linked permit number and nothing else: no score, no
violations, no business name (`InspectionTypeModuleId` 1, a generic permit inspection). The one opened was a
temporary-event permit inspection. Not a source of restaurant scores.

**City of Mission Health Department** (~86k): `https://missiontexas.us/176/Health-Department` -> 200. Lists "Health
Permits, Health Inspections, Mosquito Spraying" as services; no results link.

**City of Pharr** (~80k): `https://pharr-tx.gov/government/departments/public-health/health-inspections/` -> 200,
body text: "COMING SOON! Health Inspections will soon be under the Public Health department." Pharr's Accela
Citizen Access has a public (no login) Environmental Health module:
`https://aca-prod.accela.com/PHARR/Cap/CapHome.aspx?module=EnvHealth&TabName=EnvHealth` with record types such as
`EnvHealth/Food/Restaurant/Permit`. A WebForms postback search for Restaurant Permits filed 01/01/2026-11/22/2026
returned 21 records (`EH2026-00057`, BLACK DIAMOND BBQ - PHARR, 1617 W Polk Ave, Active...). The record detail page
has an Inspections section that read "There are no completed inspections on this record." So it is a permit
registry ("Only permits received after December 7th, 2020 are available online"), not an inspection-result source.

**City of Harlingen Environmental Health Department** (~72k, Cameron County):
`https://www.harlingentx.gov/departments/health/index.php` -> 200. Program description only; related pages are
Animal Care, Food Permits, Ordinances, Vital Statistics. No results.

**Points 4-10** do not apply (nothing to filter, score or count).

**11. Blockers.** No data. No technical blocks on these hosts.

**12. Verdict: NOTHING ONLINE** for all six. Pharr and Edinburg expose permit registries that could seed a facility
list if a results feed ever appears. Pharr's page says results are coming; re-check in 2027.

Captures: `hidalgo_156.html`, `mcallen_code.html`, `mcallen_portal.html`, `edinburg_health.html`,
`edinburg_tenants.json`, `edinburg_criteria.json`, `edinburg_s1..4.json`, `edinburg_insp.json`, `mission_176.html`,
`pharr_hi.html`, `pharr_aca_eh.html`, `pharr_aca_search.html`, `pharr_aca_detail.html`, `harlingen_health.html`.

---

## Midland

**1. Agency, population, URL.** City of Midland Health Services Department, Environmental Section; the city states
it is "the local health entity for both the city and county" (city ~133k, Midland County ~170k).
`https://www.midlandtexas.gov/749/Environmental-Services` and `https://www.midlandtexas.gov/232/Health-Services`.

**2. Platform.** None. CivicPlus site with application forms only.

**3. Endpoints.** Both pages -> 200. Links on the Environmental Services page: Food Establishment Requirements,
permit application, mobile-unit checklists, temporary-event rules, septic application. No inspection scores, no
portal, no document list. ArcGIS Online search for Midland inspection items: 0 results. No MyHealthDepartment tenant
(the MHD directory page lists 341 tenants; none is Midland).

**4-10.** Not applicable. A third-party summary of the city code (Sec. 8-1-5) says completed reports are public
documents available on request (**unverified**).

**11. Blockers.** No data online.

**12. Verdict: NOTHING ONLINE.**

Captures: `midland_749.html`, `midland_232.html`.

---

## Odessa / Ector County

**1. Agency, population, URL.** Ector County Health Department, Environmental Section (Odessa ~114k, Ector County
~165k). County page: `https://www.ectorcountytx.gov/311/Restaurant-Swimming-Pool-Inspection-Repo`.

**2. Platform.** Legacy Accela EnvisionConnect "PressAgent" (the old PHP pages, `main.phtml` / `results.phtml`), not
the newer `/api/pressAgentClient/` JSON API surveyed for Snohomish and Anchorage.

**3. Endpoints.** The county page (200) contains nothing but an iframe:

```html
<iframe title="RestLink" src="https://pressagent.envisionconnect.com/main.phtml?agency=ect" ...>
```

and that host is gone:
- curl through the agent proxy: `CONNECT tunnel failed, response 502`, proxy note `connect_unreachable` ("the host is
  allowed for this session, but the egress proxy could not connect to it"). That is not a policy denial.
- Public DNS agrees: `dns.google` and `cloudflare-dns.com` both answer **NXDOMAIN** (Status 3) for
  `pressagent.envisionconnect.com`. `anchorageonline.envisionconnect.com` still resolves, so the zone is alive and
  the legacy host was removed.
- `ector.envisionconnect.com` does resolve (CNAME `azapp1.envisionconnect.com`, 48.202.212.62) but answers 404
  "Not Found" (`Microsoft-HTTPAPI/2.0`, no site bound) on `/`, `/main.phtml?agency=ECT` and
  `/api/pressAgentClient/programs`. `ectoronline.`, `ectorcounty.`, `echd.`, `odessa.` do not resolve.

So the county's embedded search is dead as of 2026-11-22. A search-engine copy of the old page (fetched 2026-02-14)
shows what it was (**all unverified now**): "most recent inspection results for approximately 700 fixed food
facilities and 95 ... swimming pools", "updated weekly and ... current through last Friday", with facility name and
address, date and type of last inspection, results and "identification of major violations" since 2015-07-01;
mobile units excluded; pools mixed in; an A-Z list at
`results.phtml?agency=ECT&...&offset=N&sort=FACILITY_NAME` paging by 50.

**4-9.** Not measurable. Old shape (**unverified**): latest inspection per facility only, no date filter, offset
paging, 100-point score with 3/2/1-point Priority / Priority Foundation / Core items (the Odessa American's weekly
"Ector County Restaurant Report" describes the scale that way).

**10. Requests.** If the legacy pages returned: ~25 list pages plus one detail page per facility (~800) every week,
since there was no date filter.

**11. Blockers.** The portal host no longer exists. `ector.envisionconnect.com` resolving suggests a migration to
the newer EnvisionConnect online app is under way (**my inference, unverified**).

**12. Verdict: BLOCKED** (portal offline). If it reappears as `ector...envisionconnect.com/#/pa1/search` it will be
the PressAgent JSON API family already scoped for Snohomish County and Anchorage, and would ride on that fetcher
once it exists. Re-probe `https://ector.envisionconnect.com/` when that work starts.

Captures: `ector_311.html`.

---

## San Angelo / Tom Green County

**1. Agency, population, URL.** City of San Angelo Health Services, Environmental Health Division (the local paper
calls it the San Angelo/Tom Green County Health Department; city ~100k, county ~120k).
`https://www.sanangelo.gov/326/Health-Inspections`.

**2. Platform.** A CivicPlus page with one PDF in "Related Documents": a monthly list exported from Excel
(PDF producer "Microsoft Excel for Microsoft 365").

**3. Endpoints.**
- `GET https://www.sanangelo.gov/326/Health-Inspections` -> 200; the only document link is
  `/DocumentCenter/View/2418/Retail-Food-Establishment-Grades-October-2026-PDF`.
- `GET https://www.sanangelo.gov/DocumentCenter/View/2418/Retail-Food-Establishment-Grades-October-2026-PDF`
  -> 200, `application/pdf`, 490 KB, 17 pages, `filename=Retail Food Establishment Grades_ OCT_2026.pdf`,
  PDF CreationDate 2026-11-04. `last-modified` is the request time, so it cannot serve as a change signal.

Text-layer PDF (not scanned). Columns: Establishment Name, house number, direction, Street, Grade, Date of
Inspection. Sample rows (`pdftotext -layout`):

```
ARC LIGHT SPORTS BAR & GRILL                          441        RIO CONCHO DR.      79         10/22/2026
                             ARC LIGHT SPORTS BAR                                    97         10/22/2026
```

Indented rows are sub-units of the establishment above (bar, deli, sushi, grill): scored separately, with no
address of their own (165 of 703 dated rows).

**4. Date filter, paging, caps.** None. It is a snapshot: one row per establishment or sub-unit holding its
**latest** inspection.

**5. Score.** "Grade" is a number, 0-100, higher is better. Observed 52-100: 404 rows at 100, 274 in the 90s, 18 in
the 80s, 5 in the 70s, one 67 (Rosa's #3, 10/26/2026) and one 52 (Roxie's Diner, 10/13/2026). The page text
describes the underlying demerit scale (47 items of 1-3 demerits; 0-10 excellent, 11-20 good, 21-30 acceptable,
31+ poor), so Grade = 100 - demerits. No letter grades.

**6. Violations.** None published. The local paper states it gets the one-page reports by open records request.

**7. Coordinates / address.** No coordinates. House number, direction and street in three columns; no city, no ZIP
(everything is San Angelo).

**8. Ids.** None at all: no permit number, no inspection id. Identity would be name + address.

**9. Volume, history, freshness.** 729 data lines: 703 with a score and date, 26 with neither (new or seasonal
places such as "HTEAO (OP 11/2026)", and several Wal-Mart/HEB/Sam's parent rows whose sub-units carry the scores).
Latest-inspection dates by month: Oct 2026 124, Sep 152, Aug 133, Jul 50, Jun 113, May 93, earlier 39; that is
about 30-35 a week, undercounted because a re-inspected place shows only its latest visit. Newest inspection
**2026-10-29**; the October file was produced on 2026-11-04, so the lag is up to five weeks. No history: earlier
months' files are not linked (whether old DocumentCenter ids still resolve was not tested).

**10. Requests.** 2 per run (page, then PDF). A weekly run would re-read an unchanged file three weeks out of four.
A 90-day backfill is not possible beyond what the snapshot holds.

**11. Blockers and risks.**
- `robots.txt` allows `/DocumentCenter` for `*` with `Crawl-delay: 10`; AI crawlers are named in a separate group
  that repeats the same disallow list. No login or CAPTCHA.
- PDF table parsing with the stdlib (the pipeline already carries a stdlib PDF reader for Orange County and Omaha).
  Hand-typed data: one date reads `914/2026`, names carry notes like "(OP 6/16/26)".
- The document id and title change monthly, so the link must be discovered from page 326 each time.
- Score-only, monthly, snapshot, no ids, no ZIP.

**12. Verdict: HARD.** Feasible, but it needs a new parser (an Excel-exported PDF table with parent/sub-unit rows)
for a monthly, score-only snapshot of ~120k people. Low priority.

Requests made: 3. Captures: `sanangelo_326.html`, `sanangelo_robots.txt`, `sanangelo_pdf/grades_2026_10.pdf`,
`sanangelo_pdf/out.txt`.

---

## Abilene / Taylor County

**1. Agency, population, URL.** City of Abilene Environmental Health Division (Abilene-Taylor County Public Health
District; city ~125k, county ~143k). `https://www.abilenetx.gov/366/Environmental-Health` says: "Visit our new Civic
Access portal to apply for new permits, pay invoices, and view inspection reports." Portal:
`https://cityofabilenetx-energovweb.tylerhost.net/apps/civic-access-public/home`.

**2. Platform.** Tyler EnerGov "Civic Access Public" (Angular SPA, `AppConfig` version `2026.2.2.2724`, tenant
`cityofabilenetxprod`). A new vendor family for the pipeline, but it is plain JSON with no HTML or PDF parsing, and
this tenant holds only Environmental Health records (food, pools, daycare).

**3. Endpoints.** Base `https://cityofabilenetx-energovweb.tylerhost.net/apps/civic-access-public/civicapi/`.
All anonymous: no cookie, no token, no tenant header. Send `Accept: application/json` and, on POSTs,
`Content-Type: application/json`.

| Call | Purpose | Result |
|---|---|---|
| `GET public-records/public-record-modules` | module ids | Inspections = 4, Licenses = 10, Operational Permits = 12 |
| `POST public-records-search/status-type-info` body `{"filterModule":4}` | type and status ids | 74 types, 27 statuses |
| `POST search-inspections` | inspection list | 200, paged |
| `GET inspection/{caseId}` | header: name, dates, status, permit link | 200 |
| `GET inspection/inspection-score-violation/{caseId}` | score + counts per tier | 200 |
| `POST violations-information` body `{"inspectionId":"<caseId>","isInViolation":true,"module":7,"inspectionViolationItemId":""}` | violation lines | 200 (`module` 4 -> 500) |
| `POST search-licenses` with `"filterModule":12` | operational permits (the facility list) | 200 |
| `GET inspection-checklist/{caseId}` | checklist | 200 but empty arrays; not needed |

Inspection type ids (from `status-type-info`):

| Type | typeId | Rows today |
|---|---|---|
| RFE-Routine | `120346cb-0c05-2aa9-bff3-500dc39b5f3c` | 251 |
| RFE-Reinspection | `863a15ad-2c97-858b-5ab1-3de679e08c05` | 3 |
| RFE-Bi-Annual School | `bb08dbb2-43e4-32c0-f68e-f3cf48f3eaaa` | 22 |
| MF-Routine (mobile food) | `3a146c5a-2a05-8c47-6297-1b96d8ec1d47` | 0 |
| Legacy Retail Food Establishment | `bfe2b159-00f9-1aa0-975e-6565770a3e71` | 16,780 |

Also present and to be ignored: Temporary Food Service, Legacy Mobile Food, POOL-*, DAY-*, PIWF-*, and ~55
`zzz...` building types.

List request (one week of routine inspections):

```
curl -A "<Chrome UA>" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  -X POST https://cityofabilenetx-energovweb.tylerhost.net/apps/civic-access-public/civicapi/search-inspections \
  --data '{"publicRecordsCommonSearchCriteria":{"caseNumber":"","caseTypeId":"120346cb-0c05-2aa9-bff3-500dc39b5f3c",
           "caseTypeWorkClassId":"","caseStatusId":"","projectName":"","address":"","parcelNumber":"","description":""},
           "requestedDateFrom":"2026-11-09T00:00:00","requestedDateTo":"2026-11-13T23:59:59",
           "scheduledDateFrom":null,"scheduledDateTo":null,
           "sortBy":0,"pageNumber":1,"pageSize":100,"value":"","isSortedInAscendingOrder":false,"sortField":""}'
```

Response: `{"entityPublicSearchResults":[...],"pageNumber":0,"pageSize":100,"totalCount":20}`. One list row
(trimmed):

```json
{"moduleName":"Inspection","caseId":"2e0ad5f2-56d9-4ed0-abb2-298ac102043c","caseNumber":"RFEI-ROUT-000414-2026",
 "caseStatus":"Failed","caseTypeWorkClassFriendlyName":"RFE-Routine","displayAddress":"4534 BUFFALO GAP RD",
 "searchResultFields":[{"label":"Requested Date","value":"2026-11-17T00:00:00"},
                       {"label":"Scheduled Date","value":"2026-11-17T00:00:00"},
                       {"label":"Completed Date","value":null}]}
```

The list row has **no business name**; it comes from `GET inspection/{caseId}`:

```json
{"inspectionTypeName":"RFE-Routine","inspectionStatusName":"Failed","requestedDate":"2026-11-17T00:00:00",
 "completedDate":"2026-11-19T13:00:00","companyName":"GALVESTON SEAFOOD & GRILL","dba":"",
 "linkId":"FA6A5DC6-7673-4291-8927-444E5D217921","linkNumber":"2109","linkModule":15,
 "comments":"Will reinspect in 2 days due to failing score. $75.00 reinspection fee.",
 "detailsHeaderInfo":{"caseNumber":"RFEI-ROUT-000414-2026","mainAddress":"4534 BUFFALO GAP RD"}}
```

`GET inspection/inspection-score-violation/{caseId}`:

```json
{"scoreDetail":{"violationScore":66.00,"violationRating":""},
 "inspectionViolationItems":[{"severityName":"Core","violationCount":9},
                             {"severityName":"Priority","violationCount":7},
                             {"severityName":"Priority Foundation","violationCount":2}]}
```

`POST violations-information` returns one object per cited item (18 for this inspection):

```json
{"inspectionNumber":"RFEI-ROUT-000414-2026","itemReference":"2","sortOrder":2,
 "violationItemText":"Proper Cold Holding temperature(41°F/ 45°F)","observationType":"OUT",
 "violationCategoryGroupName":"Priority Items (3 Points)",
 "violationCategoryName":"Time and Temperature for Food Safety (F = degrees Fahrenheit)",
 "inspectorNotes":"Observed raw fish stored in bucket of water at 64F for less than 4 hours. COS",
 "violationItemId":"01d116ae-f516-48cf-a038-159a9e8fc2a6"}
```

**4. Date filter, paging, caps.**
- `requestedDateFrom` / `requestedDateTo` work server-side, proven on RFE-Routine: 2026-11-09..11-13 -> 20 rows
  (all requested 11/09-11/13); 2026-10-05..10-09 -> 7 rows (10/05-10/08); no dates -> 251.
- `scheduledDateFrom` / `scheduledDateTo` are **ignored**: two different scheduled windows returned the identical
  251-row list.
- The filter is on the *requested* date; the visit can complete days later (requested 11/17, completed 11/19 above).
  Use a lookback margin and dedupe on `caseId`. The list is small enough to re-read whole instead (3 pages).
- Paging: `pageNumber` is 1-based in the request and echoed 0-based; `pageSize` 100 accepted; pages 1-3 gave
  100 + 100 + 51 = 251 distinct `caseId`s. No cap seen. The order is not by date, so always page to the end.
- A keyword search (`POST public-records-search`, `value: "McDonald"`) returned 0 in every module, so drive the
  fetch from the type filter, not from names.

**5. Score.** Official `violationScore`, 0-100, higher is better; 70 is the pass line (the 66 above is "Failed" with
a reinspection ordered). Observed in six inspections: 66, 85, 87, 95, 95, 97. It equals 100 - 3 x Priority -
2 x Priority Foundation - 1 x Core on all four new-format inspections checked (97, 95, 87, 66). On one *Legacy*
inspection it does not (95 with 6 Core items), so ship the agency's number and never recompute. No letter grade
(`violationRating` is empty).

**6. Violations.** Full text per inspection: `violationItemText` (the Texas form item title), `inspectorNotes` (the
observation; up to 215 characters in the sample) and `observationType` (`OUT`). Severity is stated by the agency:
`violationCategoryGroupName` is "Priority Items (3 Points)", "Priority Foundation Items (2 Points)" or "Core Items
(1 Point)". `itemReference` is the TX form item number (1-47).
Two titles need the pipeline's existing care:
- Item 34 prints "No Evidence of Insect contamination, rodent/other animals" as its title even when the observation
  is "kitchen back door does not fully self close". Ship it as `Pests and animals: <observation>` (the
  `HH_PEST_TITLE` treatment) or it will force false F's.
- Item 20 prints "Approved Sewage/Wastewater Disposal System, proper disposal" above an observation about a mop
  bucket. Same treatment for the sewage rule.

**7. Coordinates / address.** No coordinates. Street address only (`4534 BUFFALO GAP RD`, `202 Pine ST Unit: STE
201`); no city, no ZIP anywhere in these payloads. Default the city to Abilene and geocode.

**8. Ids.** Inspection: `caseId` (GUID) and `caseNumber` (`RFEI-ROUT-000414-2026`). Facility: `linkNumber`
(operational permit number, "2109") and `linkId` (GUID). `POST search-licenses` with `filterModule: 12` and
`caseNumber: "2109"` returns the permit: type "Retail Food Establishment - Full Service", Business Name, status
"Permit Active", street address. That is the facility inventory and the fallback when `companyName` is blank (it
was blank on 1 of 5 inspections opened; that record had `linkModule` 0).

**9. Volume, history, freshness.** RFE-Routine by ISO week of requested/scheduled date: 23, 39, 7, 31, 26, 4, 59, 21,
20 for weeks 39-47 of 2026 (about 25 a week), plus schools and reinspections. The city went live on the new record
types in September 2026; before that the same visits are "Legacy Retail Food Establishment" (540 with requested
dates from 2026-06-01, about 36 a week). So expect 30-45 a week in total. History: 16,780 legacy inspections back
to at least March 2019, with names and scores (one checked: THE SHED MARKET, 2026-09-09, score 95). Newest seen:
`RFEI-REIN-000871-2026` requested **2026-11-19**, and the failed routine above completed 2026-11-19 13:00. Fresh to
within three days.

**10. Requests.** Weekly: 3-4 list POSTs (one per food type; add the Legacy type only for backfills) plus 3 calls
per inspection (header, score, violations; the violations call can be skipped at score 100) -> about 100-140
requests. 90-day backfill: ~450 inspections -> ~1,400 requests. Full legacy history: ~50,000 requests; optional.
Responses took 0.15-0.5 s.

**11. Blockers and risks.**
- No login, no CAPTCHA, no WAF seen (IIS on `tylerhost.net`), `robots.txt` 404. Not tested from GitHub runners.
- Undocumented API inside a vendor SPA; a Tyler release can rename routes. Fail the group loudly on 404/500.
- Completeness cannot be proven from outside. Case numbers run to `-000886-2026` while only ~276 food inspections
  are listed. The sequence looks shared across pools, daycare and not-yet-completed visits (only Completed / Passed
  / Failed appear), but that is **my inference**. One listed row is dated 2027-05-01.
- Street-only addresses: a Taylor County address outside Abilene would geocode badly with a default city.
- The header payload carries the inspector's name and e-mail. Do not ship them.
- `GET operational-permit/{linkId}` is 404 (use `search-licenses`). The permit-level
  `operational-permit/inspection-score-violation/{linkId}` returned 85 for the facility whose routine scored 66; it
  is not the routine score, so do not use it.
- The pest-title and sewage-title traps above.

**12. Verdict: BUILD**, as a new small JSON client. Shape: `fetch_harris` (JSON search, then per-inspection item
calls, agency-stated P/PF/C tiers, official 100 - demerits score kept) with the TX-form handling McKinney and the
Hedgerow fetchers already have. The same client should fit other EnerGov "civic-access-public" tenants that expose
Environmental Health.

Requests made: ~55, including 12 JavaScript chunks to find the routes; all at least 1 s apart. Captures:
`abilene_366.html`, `abilene_appconfig.json`, `abilene_modules.json`, `abilene_sti.json`, `abilene_d1..d4.json`,
`abilene_pg2.json`, `abilene_pg3.json`, `abilene_t_*.json`, `abilene_insp_*.json`, `abilene_score_*.json`,
`abilene_viol_414_m7.json`, `abilene_leg_*.json`, `abilene_lic.json`, `abilene_chunk_*.js`.

---

## Cross-cutting notes

- **Proxy vs site.** Three refusals came from the sites themselves, not the agent proxy: `www.brownsvilletx.gov`
  (Cloudflare 403 block page), `www.cityoflaredo.com` (Akamai 403 "Access Denied"), and the Laredo portal's
  `inspectionsData` (IIS 500). One was a dead host: `pressagent.envisionconnect.com` (proxy 502
  `connect_unreachable`, public DNS NXDOMAIN). No request was denied by proxy policy. I did not retry or route
  around any of them.
- **MyHealthDepartment.** None of these jurisdictions is an MHD tenant. An unknown slug on
  `inspections.myhealthdepartment.com/<anything>` returns a 341-tenant directory page (341 KB). Its Texas entries
  include slugs that a grep of `dinescores_pipeline.py` does not find: `net` (Northeast Texas Public Health
  District, Tyler), `mptx` (Mount Pleasant), `aph` (Austin Public Health), `tda` (Texas Department of Agriculture),
  `tx-dallas-county`, `tx_southplains` (South Plains Public Health District, Seminole), `tx_jaspernewton`. Not
  probed; outside this survey's area.
- **ArcGIS Online** search for inspection items in El Paso, Laredo, Brownsville, McAllen, Hidalgo County, Midland,
  Abilene and San Angelo found nothing relevant.
- **Not tested anywhere:** behaviour from GitHub-hosted runner IPs, which is where the weekly job runs.
