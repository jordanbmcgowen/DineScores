# DFW cities not yet covered: live source survey (2026-11-22)

Probed live from the session container on 2026-11-22 with curl (Chrome UA, TLS verified, about one
request per second, through the agent proxy). Raw captures are under
`(raw captures, not kept in the repo)`.
Nothing in the repository was modified.

Conventions used below:

- **Verified** = I fetched it myself today. **Unverified** = from a search-engine excerpt or from memory;
  said so each time.
- Populations are 2020 census figures quoted from memory (approximate, not re-verified today).
- No proxy refusals occurred: every 403 below came from the site itself (Akamai `AkamaiGHost`,
  Cloudflare "Just a moment..." / "Sorry, you have been blocked"). I stopped on each of those hosts.
- Two hosts have a broken TLS setup (not a proxy problem): `www.rockwall.com` serves only its leaf
  certificate (fixable by shipping the public intermediate, as for Lewisville), and
  `mansfieldtexashealth.com` presents a self-signed DreamHost certificate on 443.

## Ranking (people covered per unit of build work)

| # | Jurisdiction | Verdict | Platform | Pop. | Score | Insp./week | Biggest risk |
|---|---|---|---|---|---|---|---|
| 1 | Garland | **BUILD** (McKinney family) | City ArcGIS MapServer, layer + violations table | 246k | official 0-100 = 100 - TX points | ~30 (10-70) | Snapshot of the latest inspection per permit only: no history, a missed week is lost |
| 2 | Little Elm + Sachse + Pilot Point + Justin | **BUILD / light HARD** (new small HTML parser, Hedgerow-like crawl) | InstantInspector `pir.php` | 46k + 27k + 4k + 4k | official 0-100 | ~8 + ~4 + ~2 + ~1 | No date filter and no date in the list: one request per facility per week (~410) |
| 3 | Rowlett | **BUILD** (scores only, Lewisville/Denton family) | Google Sheet (uploaded XLSX) | 62k | official 0-100 | ~5 | Hand-maintained sheet, no ids/coords/violations; file id can change |
| 4 | Rockwall (city) | **BUILD** for letter grades, HARD for scores | ArcGIS FeatureServer (Cityworks) + PDFs | 47-51k | letter A-D (numeric only in PDF) | ~5-6 | Letter grade only; newest row 2026-10-16 (5 weeks behind) |
| 5 | Farmers Branch | **HARD** (new Power BI query client, small) | Power BI "publish to web" report | 36k | official 0-100 | ~7 | Undocumented API; resource key / model id change if the report is republished |
| 6 | Flower Mound | **HARD** (PDF, CID fonts) | Monthly SSRS PDF | 76k | official 0-100 | ~13 | PDF only, monthly, no coordinates |
| 7 | Burleson | **HARD** (PDF per inspection) | Inspect2Go public portal | 48k | official 0-100 (in PDF) | ~8-10 | Score and violations exist only inside 0.35-3 MB PDFs; ~400 facility pages per week |
| 8 | The Colony | HARD, low value | Monthly PDF | 45k | 0-100 + letter | ~8 | No address in the list |
| 9 | Midlothian (Ellis Co.) | HARD, low value | Monthly PDF grid | 35k | 0-100 | ~8 | PDF table layout |
| 10 | Dallas County HHS contract cities | **NOT YET** (watch) | MyHealthDepartment `tx-dallas-county` | ~140-150k | - | ~30 once live | Tenant exists since ~2025-02 but has published zero inspections |
| - | Grapevine, Bedford, Hurst | **ALREADY COVERED** | MHD `tarrant` | 51k + 50k + 40k | - | - | - |
| - | Irving | BLOCKED | Cloudflare challenge; stale ArcGIS | 257k | letters | - | Bot challenge |
| - | Grand Prairie | STALE (not worth) | Tyler healthinspections.us | 196k | letters only | 0 since 2026-04 | Nothing newer than spring 2026 |
| - | Carrollton | BLOCKED | Akamai 403 | 133k | ? | - | Bot protection |
| - | Allen | NOTHING ONLINE | "unavailable due to system updates" | 105k | - | - | - |
| - | Mansfield | BLOCKED | OpenGov portal behind Cloudflare Turnstile | 73k | 0-100 | - | Turnstile on the public record search |
| - | North Richland Hills | BLOCKED from here | CivicPlus behind Cloudflare WAF; monthly PDFs (unverified) | 70k | 0-100 | ~15 (unverified) | WAF block |
| - | Euless | BLOCKED / probably nothing | Akamai 403 | 61k | - | - | - |
| - | Weatherford | BLOCKED | Permit portal behind Cloudflare challenge | 31k | ? | - | Bot challenge |
| - | Cleburne | BLOCKED from here | CivicPlus behind Cloudflare WAF; monthly PDFs (unverified) | 31k | 0-100 | ? | WAF block |
| - | Addison | BLOCKED / probably nothing | Akamai 403 | 17k | - | - | - |
| - | Wylie (Collin County program) | NOTHING USABLE | Annual PDF, no dates, no addresses | 58k | 0-100 | - | No inspection date |
| - | University Park | PDFs ONLY (scans) | Laserfiche WebLink | 25k | demerits (in scan) | ~2-3 | Scanned images |
| - | Coppell, Cedar Hill, DeSoto, Duncanville, Lancaster, Highland Park, Waxahachie, Royse City, Ennis, Kaufman (city) | NOTHING ONLINE or name-only lists | - | - | - | - | - |
| - | Collin County, Denton County, Rockwall County, Ellis County, Kaufman County, Johnson County, Parker County programs | NOTHING ONLINE | DSHS Region 2/3 or county without publication | - | - | - | - |

Build first: Garland alone adds 246k people for about the work of the McKinney fetcher. The
InstantInspector parser is the next best unit (four towns, full item-level violations with TX tiers).

---

## Dallas County Health and Human Services (MyHealthDepartment `tx-dallas-county`)

1. **Agency**: DCHHS Environmental Health Division, 2377 N Stemmons Fwy, Dallas. Public URL
   https://inspections.myhealthdepartment.com/tx-dallas-county (HTTP 200, title "Home | Dallas County
   Health and Human Services Environmental Health Division | My Health Department").
2. **Platform**: MyHealthDepartment (already supported: `DFW_JURISDICTIONS`).
3. **Endpoints (verified)**:
   - `POST https://inspections.myhealthdepartment.com/` with
     `{"data":{"path":"tx-dallas-county","programName":"","filters":{"date":"2026-11-10 to 2026-11-10","purpose":""},"start":0,"count":25,"searchStr":"","lat":0,"lng":0,"sort":{"field":"inspectionID","direction":"asc"}},"task":"searchInspections"}`
     -> `200`, body `[]`.
   - Same request with single days 2026-11-18, 2026-11-03, 2026-10-15, 2026-08-12, 2026-03-11,
     2025-09-17 -> `[]` every time. Windows `2026-11-01 to 2026-11-22`, `2015-01-01 to 2026-11-22`,
     an empty date, `searchStr` "a" and "mcdonald" -> `[]`. (Per the pipeline's own note an unknown
     slug answers `{"err":true,...}`, so `[]` means "known tenant, no rows"; I did not re-test an
     unknown slug today.)
   - Control, same payload with `path":"dallas"` for 2026-11-10 -> 25 rows (first row: SHERATON
     DALLAS-BANQUET HALL, score 92, Routine), so the request shape is right.
   - Tenant config: `POST /` `{"task":"readOne","dataObjectName":"jurisdictions","data":{"path":"tx-dallas-county"}}`
     -> `displaySettings: {showObservations:true, allowPDFPrint:true, showScore:true}`,
     `links.default` = the DCHHS environmental health page, logo under `client_cdt0/`.
4. **Since when**: never. The slug returns zero rows for every window. The tenant itself was set up
   around **2025-02-14** (Last-Modified of its logo on the vendor's S3 bucket); 21 months later it is
   still empty, and the DCHHS environmental health page does not link to it (verified: no
   `myhealthdepartment` link on https://www.dallascounty.org/departments/dchhs/public-health/environmental-health/).
   The landing page's Inspection Purpose select is empty.
5. **Which cities DCHHS inspects**:
   - DSHS's jurisdiction layer (verified today, `County_Jurisdiction_1051/FeatureServer/2`): Dallas
     County = "County Health Department(s) Provides Services", Dallas County Health and Human Services.
   - DSHS's city layer (verified, `City_Jurisdiction_Contact_Information___Update_Aug_2026/FeatureServer/10`)
     lists these Dallas County cities as running their OWN program: Dallas, Garland, Irving, Mesquite,
     Richardson, Carrollton, Farmers Branch, Coppell, Addison, Duncanville, DeSoto, Cedar Hill,
     Seagoville. Everything else falls to the county.
   - FY2022 interlocal agreement (search excerpt of the county's own court document, unverified by
     fetch): Balch Springs, DeSoto, Glenn Heights, Highland Park, Hutchins, Lancaster, Sachse,
     Seagoville, University Park, Wilmer, Cockrell Hill.
   - FY2025-26 agreements found for Hutchins, Wilmer, Lancaster and University Park (search excerpts
     of council packets, unverified by fetch). University Park's and Lancaster's own pages say Dallas
     County inspects (verified).
   - Changes since 2022 seen live today: DeSoto has its own Environmental Health department; Sachse
     publishes on InstantInspector (see below); Rowlett and Farmers Branch publish their own data.
     A Dallas County management report (search excerpt, unverified) still lists Duncanville, Farmers
     Branch, Rowlett, Sachse and Sunnyvale as contract cities, so the county's lists are not reliable.
   - Likely current set (unverified as a whole): Balch Springs (27k), Lancaster (41k), University
     Park (25k), Highland Park (9k), Glenn Heights (16k), Hutchins (6k), Wilmer (5k), Cockrell Hill
     (4k), Sunnyvale (8k) and unincorporated Dallas County: roughly 140-150k people.
6. **Volume when it goes live**: DCHHS reports 1,590 food establishment inspections in FY2025 and
   1,130 in the first three quarters of FY2026 (search excerpt of the county's metrics PDF,
   unverified by fetch): about 30 a week.
7. **Verdict**: **NOTHING ONLINE yet; config-only BUILD the day rows appear.** Add
   `'tx-dallas-county': {'display_name': 'Dallas County', 'default_city': 'Dallas County', 'state': 'TX'}`
   then, after calibrating `score` on detail pages (University Park and Highland Park describe a
   demerit scale 0-30+, lower is better, so expect `score_scale: 'demerit'`; unverified for the
   portal). Cheap watch: one search POST per week; alert on a non-empty list.
8. **Side note on the portal** (verified): the directory at https://inspections.myhealthdepartment.com/
   lists 277 tenants; the Texas ones are `tarrant`, `fort-worth-texas`, `plano`, `dallas`, `frisco`,
   `tx-dallas-county`, `net` (NET Health, Tyler), `aph` (Austin Public Health), `tda`, `aaphd`
   (Amarillo), `fbceh` (Fort Bend), `mptx` (Mount Pleasant), `orangecotx`. No other DFW tenant
   exists. The landing-page controller now carries a client-side reCAPTCHA overlay
   (`captcha-overlay`, `captchaInspectionsLandingPageVerified`); the Dallas County landing HTML has no
   such element and the JSON search answered without it, but it is a vendor-wide risk to watch.

---

## Garland

**Verdict: BUILD (McKinney family). The feed has resumed, on a new and much richer endpoint.**

1. **Agency**: City of Garland Health Department (Environmental Health), ~246k residents, "around
   1,200 permitted food establishments". Public pages: https://www.garlandtx.gov/680/Food ->
   https://maps2.garlandtx.gov/sites/GarlandRestaurantScores.html (meta-refresh to an Experience
   Builder app `616cdfb766544672aba5ab82cea769b5`, created 2026-02-04, web map
   `50f1961887e546d2b9f9061c0b5ae998` modified 2026-08-27).
2. **Platform**: city-hosted ArcGIS Enterprise 11.5 MapServer (same family as McKinney).
   The old WebForms grid https://foodindex.garlandtx.gov/ is still up but frozen: re-verified today,
   window 1/1/2026-11/21/2026 with all 12 categories returns 377 records, newest 2026-06-30
   (needs `--http1.1`; HTTP/2 streams are cancelled).
3. **Endpoints (all verified, no token, no robots.txt (404))**:
   - Base: `https://maps2.garlandtx.gov/server2/rest/services/Health/Restaurant_Scores/MapServer`
     ("Restaurant scores from Health department inspections.", capabilities Map,Query,Data,
     maxRecordCount 2000, pagination and statistics supported).
   - **Layer 4 "Restaurant Scores"** (points, SR 4326), one row per PERMIT with its latest inspection:
     `OBJECTID, FACILITY_ID, PERMIT_NUMBER, PROGRAM_IDENTIFIER, NAME, ADDRESS, GIS_LATITUDE,
     GIS_LONGITUDE, INSPECTION_DATE (esriFieldTypeDate), SCORE (double), PE`.
     Sample: `{"OBJECTID":320,"FACILITY_ID":"FA0001543","PERMIT_NUMBER":"PT1681","PROGRAM_IDENTIFIER":"0","NAME":"LA MEDIA LUNA","ADDRESS":"3460 W WALNUT ST","GIS_LATITUDE":32.9155686,"GIS_LONGITUDE":-96.6815045,"INSPECTION_DATE":1795132800000 (2026-11-20),"SCORE":97.0,"PE":"1603"}`.
   - **Table 3 "Restaurant violations"**, one row per violation of that same latest inspection:
     `OBJECTID, FACILITY_ID, NAME, LOCATION, CITY, STATE, ZIP, PERMIT_NUMBER, INSPECTION_DATE, SCORE,
     VIOLATION_CODE, POINTS, VIOLATION_DESCRIPTION, V_MEMO`.
     Sample: `{"FACILITY_ID":"FA0002499","NAME":"GARLAND HOMEBOY'S","LOCATION":"450 E INTERSTATE 30","CITY":"GARLAND","STATE":"TX","ZIP":"75043","PERMIT_NUMBER":"PT3161","INSPECTION_DATE":"2026-11-16","SCORE":95.0,"VIOLATION_CODE":"1618","POINTS":3.0,"VIOLATION_DESCRIPTION":"Toxic Substances Properly Identified, Stored and Used","V_MEMO":"Observed chemical spray bottle under prep cooler at front counter without a label ..."}`.
   - The city's own report viewer
     (`https://maps2.garlandtx.gov/customgispages/restaurantviolations/?PERMIT=PT2787&FACILITY_ID=FA0002499`)
     is a static page that queries table 3 client-side with
     `where=FACILITY_ID='..' AND PERMIT_NUMBER='..'&outFields=*&orderByFields=INSPECTION_DATE DESC, VIOLATION_CODE ASC`.
     It shows a disclaimer: "This report data may be under review and may be revised."
   - Sample requests:
     `GET .../MapServer/4/query?where=INSPECTION_DATE >= DATE '2026-11-13'&outFields=*&orderByFields=OBJECTID&f=json`
     `GET .../MapServer/3/query?where=INSPECTION_DATE >= DATE '2026-11-13'&outFields=*&orderByFields=OBJECTID&resultOffset=0&resultRecordCount=2000&f=json`
     `GET .../MapServer/4/query?where=1=1&returnCountOnly=true&f=json` -> `{"count":1143}`; table 3 -> `{"count":7478}`.
4. **Date filter (verified, server-side)**: layer 4, `INSPECTION_DATE >= DATE '2026-11-13' AND < DATE '2026-11-21'`
   -> 20; `>= DATE '2026-10-01' AND < DATE '2026-11-01'` -> 93; `>= DATE '2026-07-01'` -> 723.
   Paging by `resultOffset`/`resultRecordCount` (table 3 read in 4 pages: 2000, 2000, 2000, 1478,
   7,478 distinct OBJECTIDs, `exceededTransferLimit` true on the first three). Dates are midnight UTC
   (all 1,143 rows hour 0), so the UTC date is the inspection date.
5. **Score**: official 0-100, higher is better (median 90, 11 permits below 70). Verified on the
   whole table: `SCORE == 100 - sum(POINTS)` for 1,218 of 1,248 permit inspections. The 30
   exceptions are 23 unscored rows (SCORE 0 with a null violation code, dated 2023-2025 and absent
   from layer 4) and seven data-entry oddities (e.g. PT2896: score 89 with 12 points; stray POINTS
   values of 79, 5 and 0 exist in the table). Same arithmetic as McKinney: ship `risk_score = SCORE`
   and use the same "points must reproduce the score" tripwire.
6. **Violations**: yes, in table 3. `VIOLATION_CODE` 1601-1647 = TX form items 1-47,
   `VIOLATION_DESCRIPTION` = the form title, `V_MEMO` = the inspector's observation plus corrective
   text (multi-line, `\r\n`). **Severity is stated**: POINTS 3 (items 1-20, priority) / 2 (21-33,
   priority foundation) / 1 (34-47, core); verified per code over 7,478 rows (two stray rows only).
   A clean inspection is one row with null code, null points and SCORE 100 (29 of the 33 score-100
   permits). Item 1634's title is "No Evidence of Insect Contamination, Rodent/other Animals": ship
   it as `Pests and animals: <V_MEMO>` like the Hedgerow/McKinney fetchers (sample memos: "Numerous
   gnats were observed throughout the establishment...", "Gaps of 1/4-inch or greater were found
   around the receiving doors...").
7. **Coordinates**: `GIS_LATITUDE`/`GIS_LONGITUDE` and point geometry in 4326; 25 of 1,143 null.
   Address: one street string in layer 4; table 3 adds CITY/STATE/ZIP.
8. **Ids**: `FACILITY_ID` (FA0001543) and `PERMIT_NUMBER` (PT1681); 1,143 permits over 1,022
   facilities (grocery departments, two permits at one address). **No inspection id**: key on
   permit + date. `PE` is the program element: 1601 FullService, 1602 FastService, 1603 SnackBar,
   1604 Convenience, 1605 Supermarket, 1606 Processor, 1607 Daycare, 1608 School, 1609 Warehouse,
   1610 Mobile, 1611 AssistedLiving, 1612 Medical (mapping read from the old grid's checkboxes);
   counts today 1602: 386, 1604: 189, 1603: 183, 1601: 170, 1608: 67, 1605: 54, 1607: 42. Drop
   1607/1608/1610 to match the McKinney scope rule.
9. **Volume and history**: newest inspection 2026-11-20 (two days old). Latest-inspection dates by
   ISO week since September: 32, 31, 71, 64, 21, 20, 14, 10, 43, 44, 19, 20 (about 30 a week; a
   lower bound because a re-inspection overwrites the row). 723 of 1,143 permits were last inspected
   since 2026-07-01. **No history**: every permit has exactly one inspection date in table 3
   (verified: 0 permits with more than one). Oldest row 2023-10-04.
10. **Requests**: weekly 3-4 (count, layer-4 window, table-3 window; plus a count check on table 3).
    Backfill: not possible beyond the snapshot; one full read (1 + 4 requests) yields each permit's
    latest inspection, which covers 723 permits inspected since July.
11. **Risks**: snapshot-only (a facility inspected twice between runs keeps only the second visit, and
    a missed week cannot be refetched once the place is re-inspected); data "may be revised"; no
    inspection id; a single city GIS host; 105 table-3 permits are absent from layer 4 (unscored or
    retired permits: read layer 4 first and join table 3 on permit + date); reachability from GitHub
    runners unverified.
12. **Build notes**: reuse `fetch_mckinney`'s shape: count first, page until complete or raise,
    violations joined by (PERMIT_NUMBER, INSPECTION_DATE), tier from POINTS, title + memo text, drop
    and hold an inspection whose points do not reproduce its score. City label `Garland`.

---

## Little Elm, Sachse, Pilot Point, Justin (InstantInspector)

**Verdict: BUILD with a new small HTML parser (crawl shape resembles the Hedgerow fetchers).**
Found through Little Elm's site search; this vendor was not on the known-platform list.

1. **Agencies**: Town of Little Elm (46k in 2020, about 60k now), City of Sachse (27k; formerly
   inspected by Dallas County), City of Pilot Point (4k), City of Justin (4k). Also Fannin County
   (`c=33`, outside DFW).
2. **Platform**: InstantInspector "Public Inspection Reports" (`www.instantinspector.com`, PHP, behind
   Cloudflare but not challenged).
3. **Endpoints (verified)**:
   - Directory: `GET https://www.instantinspector.com/app/pir.php?a=list&c=<client>` -> one HTML table,
     all locations on one page: Location, Address, Score, link `pir.php?a=1&b=<location id>&detail=1`.
     Public client ids found by trying 1-40: **6 Little Elm (211 rows), 8 Pilot Point (47),
     25 Sachse (110), 28 Justin (37), 33 Fannin County (63)**. Other ids answer 302 to the client's
     own site (private directory) and ids 36-40 say "This public directory link is not valid".
   - Report: `GET https://www.instantinspector.com/app/pir.php?a=1&b=1168&detail=1` -> plain HTML:
     name, Score, Address + "City, TX zip", Permit Number, "Date of Inspection" (`08/29/2026 12:14 pm`),
     Inspection Type (Routine), then all 47 TX form items under three headings ("Priority Items
     3 points each", "Priority Foundation Items 2 points each", "Core Items 1 point each"), each with
     IN or OUT and, for OUT, the inspector's comment.
   - Sample (Little Elm, b=1168): `A2B | score 84 | 2587 FM 423, Little Elm, TX 75034 | 08/29/2026 12:14 pm | Routine`;
     `OUT 3. Proper Hot Holding temperature (135F). - "Hot hold item observed at 114. Discard after 4 hours"`;
     `OUT 34. No evidence of Insect contamination, rodent/other animals. - "Cockroaches observed in dish room. Contact pest control"`.
   - Sample (Sachse, b=1497): `Allsup's | 85 | 11/21/2026 9:44 am | Routine`, 10 OUT items; the
     points reproduce the score (1x3 + 3x2 + 6x1 = 15 -> 85).
4. **Date filter**: none. The list has no date column, so every location's report must be read to
   learn its date. No paging, no caps seen.
5. **Score**: official 0-100, higher better. Sampled values 80-100 (Little Elm), 81-95 (Sachse).
6. **Violations**: full item-level text with a stated tier (the heading the item sits under). Item 34
   prints the TX pest title: ship as `Pests and animals: <comment>` (the Sachse sample's item-34
   comment is "Keep pest control service records on site", which must not become an F).
7. **Coordinates**: none. Street address + city/zip on the report (one sampled Little Elm row has the
   city typed as "Little").
8. **Ids**: location id `b` (stable in the URL); "Permit Number" was 0 on all 17 sampled reports; no
   inspection id (key on location + date).
9. **Volume, history, freshness**: latest inspection only. 17 sampled reports: Little Elm dates
   2026-03-24 to 2026-11-13, Sachse 2026-06-13 to 2026-11-21 (yesterday). At two inspections a
   year: about 8 a week for Little Elm, 4 for Sachse, 2 for Pilot Point, 1 for Justin (estimated
   from location counts, not measured).
10. **Requests**: weekly 1 + N per tenant: 212 + 111 + 48 + 38 = about 410 small HTML pages
    (52 KB each). 90-day backfill: the same crawl; nothing older than the latest report exists.
11. **Risks**: no history and no date filter (a location inspected twice between runs loses the
    first visit); no coordinates; vendor directory ids could be made private at any time (five of 35
    are public); robots.txt allows `User-agent: *` with `Content-Signal: search=yes,ai-train=no,use=reference`
    and disallows named AI crawlers (ClaudeBot, GPTBot, ...): a policy question for the owner before
    sending these lines to Jev, even though that is reading and not training.
12. **Build notes**: one parametrised fetcher (client id, city label). Unread report -> drop and hold
    with `key=` the location id; unread directory -> hold the window. Tiers are source-stated
    (SOURCE_TIER_SOURCES); score official.

---

## Rowlett

**Verdict: BUILD (scores only; Lewisville/Denton family).**

1. **Agency**: City of Rowlett Environmental Health Division, 62k. Page
   https://www.rowletttx.gov/164/Food-Establishment-Health-Scores (verified) links
   `https://docs.google.com/spreadsheets/d/1Nmjv_FBmVAhXO7ruzY90d4zuN3-oze0p/edit?...` ("Public Report
   Scores (1).xlsx", an uploaded Excel file viewed in Google Sheets).
2. **Platform**: Google Sheets export of a staff-maintained workbook.
3. **Endpoints (verified)**:
   - `GET https://docs.google.com/spreadsheets/d/1Nmjv_FBmVAhXO7ruzY90d4zuN3-oze0p/export?format=csv`
     -> 307 to `doc-*-sheets.googleusercontent.com/export/...` -> 200 `text/csv`, 12 KB, first tab only.
   - `...export?format=xlsx` -> 200, 126 KB, four tabs named `2026`, `2025`, `2024`, `2023`
     (stdlib `zipfile` + XML is enough).
   - Columns: `Establishment Name, Address, Number of Inspections Per Year, Date, Score, Date, Score, Date, Score, Date, Score`.
   - Sample: `AFC Sushi,6706 Dalrock Road,3,1/12/26,100,11/12/26,99,,,,`
4. **Date filter**: none (whole file each time; 206 rows).
5. **Score**: official 0-100; 2026 tab range 70-100, median 96; all numeric.
6. **Violations**: none. 7. **Coordinates**: none; street address without city/zip.
8. **Ids**: none (name + address).
9. **Volume**: 238 inspections in 2026 so far over 203 establishments; by month Jan 36, Feb 35,
   Mar 35, Apr 35, May 15, Jun 13, Jul 2, Aug 20, Sep 17, Oct 10, Nov 20: about 5 a week. Newest
   2026-11-19 (three days old). History: tabs back to 2023.
10. **Requests**: 2 a week (city page to rediscover the link, then the export). Backfill: the same
    file.
11. **Risks**: hand-typed sheet (dates as `M/D/YY`, layout can change); the file id changes if staff
    upload a new copy (rediscover it from the city page, and fail red when the link is gone); no ids,
    so a renamed row becomes a new restaurant; geocoding needed for all 206 addresses.

---

## Rockwall (city) and Rockwall County

**City verdict: BUILD for letter grades (one ArcGIS request); HARD if numeric scores and
observations are wanted (one PDF per inspection).**

1. **Agency**: City of Rockwall Neighborhood Improvement Services, Health Services, 47k (the city
   quotes 51,171). Dashboard https://rockwall.maps.arcgis.com/apps/dashboards/7f8da2e82d564a94b595789f11eb85dc
   ("Food Inspection Scores", public). The city page https://www.rockwall.com/nis_health.asp could not
   be fetched with default trust (see TLS note).
2. **Platform**: ArcGIS Online dashboard over a Cityworks-fed feature layer, reached through an AGOL
   stored-credential proxy.
3. **Endpoints (verified)**:
   - `https://utility.arcgis.com/usrsvcs/servers/b563ef7a6db849caa48ae0bab6ef094e/rest/services/FoodInspections/FeatureServer/0`
     (layer `Rockwall.DBO.Food_Inspection_Results`, capability Query, maxRecordCount 1000, SR 102738).
   - `GET .../FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&orderByFields=OBJECTID&f=json`
     -> 200 rows in one response (789 KB).
   - Fields: `OBJECTID_1, Address, CA_OBJECT_ID, RestaurantName, InspectionCaseNumber, ASSET_ID, Type,
     OBJECTID`, then a WIDE layout `Inspection1_2020 ... Inspection2_2040` (text
     `"<letter> — <YYYY-MM-DD>"`, or `" — "` when empty) and `InspectionFile1_2020 ... InspectionFile2_2040`
     (PDF URL).
   - Sample: `{"RestaurantName":"EDOHANA - HIBACHI , SUSHI BAR & MAIN BAR","Address":"716 E INTERSTATE 30","InspectionCaseNumber":"FSI-002232","ASSET_ID":"ADD.LOCR0017337","Type":"Asian","Inspection1_2025":"C — 2025-02-08","Inspection2_2025":"C — 2025-07-31","InspectionFile2_2025":"https://www.rockwall.com/pz/GIS/RestaurantInspections/1116042_Routine Food Service Inspection_Edohana Hibachi, Sushi, Bar Routine.pdf"}`, geometry `{"x":-96.4652,"y":32.8992}`.
4. **Date filter**: none useful (dates are inside text cells); the whole layer is one request.
5. **Score**: letter only in the layer: A 1,236, B 542, C 83, D 2 over 1,863 inspection cells. The PDF
   carries demerits/score/letter (sample "4/96/A").
6. **Violations**: only in the PDF (TX form EH-06 filled in an editor; text extracts with
   `pdftotext`: item marks, temperatures, and an "OBSERVATIONS AND CORRECTIVE ACTIONS" list where
   item numbers such as "32/45" precede some lines and compliments such as "Using day dots, great
   date labels" have none). No machine tier beyond the item number.
7. **Coordinates**: point geometry for all 200 rows. Address: street only.
8. **Ids**: `InspectionCaseNumber` (FSI-002232), `ASSET_ID`, `CA_OBJECT_ID`; PDF names start with an
   inspection number (1116042).
9. **Volume and history**: 200 restaurants; two slots a year back to 2020 (oldest 2020-04-16).
   2026 by month: 14, 23, 19, 29, 18, 36, 30, 24, 24, then 4 in October: about 5-6 a week. **Newest
   2026-10-16**: five weeks behind today, so either the feed lags or publication slowed.
10. **Requests**: 1 a week. Backfill: the same request returns everything since 2020. With PDFs:
    +1 per new inspection (the sample was 1.9 MB, 2 pages).
11. **Risks**: letter grade only without the PDFs; a third inspection in a year has no slot
    (follow-ups are not shown); the schema stops at 2040 and the popup is hand-built per year;
    **TLS**: `www.rockwall.com` sends only its leaf certificate (`*.rockwall.com`, issuer "Network
    Solutions RSA OV SSL CA 3"); verification succeeds once that public intermediate
    (`http://crt.netsolssl.com/NetworkSolutionsRSAOVSSLCA3.crt`, sha256
    `90:33:34:50:A3:AF:1F:42:DB:59:54:F8:49:14:B1:86:27:71:EA:B4:66:CE:52:FD:D1:9E:AA:C3:59:4E:DD:3B`,
    valid to 2033-08-01) is added to the bundle, exactly the Lewisville pattern. I fetched one PDF that
    way, never with verification off. The ArcGIS endpoint itself has a normal chain.

**Rockwall County**: DSHS's jurisdiction layer (verified) says "Regional Headquarters Provides
Services" (DSHS Region 2/3) for the county; the county's Environmental Health Coordinator page
covers septic and nuisances only (search excerpt). City programs listed by DSHS: Rockwall, Fate,
Royse City, McLendon-Chisholm. Royse City posts a one-page monthly PDF
(https://www.roysecity.com/DocumentCenter/View/1974/Restaurant-Scores, verified: 50 rows of name,
most recent inspection date, score + letter; newest 2026-09-30; **no addresses**). Verdict for the
county: **NOTHING ONLINE** (DSHS has no public search and its reports carry no score).

---

## Farmers Branch

**Verdict: HARD (new, small Power BI query client). Scores only.**

1. **Agency**: City of Farmers Branch Environmental Health (Community Services), 36k. Page
   https://www.farmersbranchtx.gov/food-inspection-scores -> OpenGov CKAN dataset
   https://farmersbranchtxprod.ogopendata.com/dataset/health-inspection-scores-food-establishments,
   whose only resource is an HTML link to a Power BI report (no DataStore:
   `package_show` verified, `datastore_active: false`).
2. **Platform**: Power BI "publish to web" on the US Government cloud.
3. **Endpoints (verified)**:
   - Report shell: `https://app.powerbigov.us/view?r=eyJrIjoiODY4OTA4NDMtMzhjNy00MzM1LWEzMjYtYTIwMGU1NWE0NzBlIiwidCI6IjkzNzA4MjNmLTM0MzYtNDg0OS04OWMyLWI0ZTU2NTNmODY4OCJ9`
     (the `r` value is base64 JSON: resource key `86890843-38c7-4335-a326-a200e55a470e`).
   - Schema: `GET https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net/public/reports/86890843-38c7-4335-a326-a200e55a470e/modelsAndExploration?preferReadOnlySession=true`
     with header `X-PowerBI-ResourceKey: 86890843-38c7-4335-a326-a200e55a470e` -> model id `1939046`,
     report "Public Health Scores", last refresh `2026-11-21T06:14:12` (daily), one table visual with
     `Business Name, Address, Inspection No, Inspection Date, Inspection Type, Inspection Status, Health Score`.
   - Data: `POST https://wabi-us-gov-virginia-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true`
     (same header, `Content-Type: application/json`) with a SemanticQueryDataShapeCommand:
     `From: [{"Name":"p","Entity":"Public Health Scores","Type":0}]`, one `Column` select per field,
     optional `Where` comparisons on `Inspection Date`
     (`ComparisonKind` 2 = >=, 3 = <, literal `datetime'2026-10-01T00:00:00'`), `OrderBy` on the date,
     `Binding.DataReduction.Primary.Window.Count: 500`, `"modelId":1939046`. The exact request bodies
     are saved as `raw/farmersbranch/q1.json` and `q_oct.json`; a 40-line decoder for the answer
     (`dsr.DS[0].PH[0].DM0` rows with `C` values, `R` repeat mask, `Ø` null mask, `ValueDicts`) is in
     the sibling folder `survey/dfw/scripts/pbi_decode.py`.
   - Sample row: `["KITCHEN SOCIAL","4887 ALPHA RD 288","IHLTH-021440-2026","2026-10-30","HLTH: Health Inspection","Passed",90]`.
4. **Date filter (verified, server-side)**: 2026-10-01..2026-11-01 -> 49 rows; 2026-08-10..2026-08-24
   -> 61 rows; before 2025-05-09 -> 16 rows (oldest 2025-02-12). Unfiltered: 500 rows plus a
   `RestartTokens` entry (the window cap; continue by restart token or by narrower date windows).
   About 516 rows in all.
5. **Score**: official 0-100, higher better; observed 58-100. Status values: Passed 472,
   Re-inspection required (with fee) 17, Re-inspection required 5, Partial Pass 3, Scheduled 3 (per
   500 newest rows). Types: Health Inspection 469, Follow-Up 20, Complaint 11.
6. **Violations**: none. 7. **Coordinates**: none; street address only.
8. **Ids**: `Inspection No` (IHLTH-021902-2026); no facility id.
9. **Volume**: by ISO week since August: 6, 25, 36, 20, 4, 2, 17, 3, 1, 18, 9, 9, 13, 1: about 7 a
   week. Newest 2026-11-05 (17 days old). History from 2025-02-12 (the dataset says inspections on
   or after 2024-10-01).
10. **Requests**: weekly 2 (schema to rediscover the model id, one query). 90-day backfill: 1 query.
11. **Risks**: undocumented API; the resource key and model id change if the city republishes the
    report (rediscover both from the CKAN `package_show` resource URL); scores only; "Scheduled"
    rows must be dropped; geocoding needed.

---

## Flower Mound

**Verdict: HARD (PDF parser for an SSRS report with a CID font). Scores only, monthly.**

1. **Agency**: Town of Flower Mound Environmental Health Services, 76k. Page
   https://www.flowermound.gov/380/Food-Inspections links "View the Latest Flower Mound Food
   Inspection Scores" -> `/DocumentCenter/View/54384` (today: "November Food Scores.pdf").
2. **Platform**: PDF rendered by Microsoft Reporting Services 2022 (CivicPlus DocumentCenter).
3. **Verified content**: 50 pages, "FOOD ESTABLISHMENT INSPECTION REPORT AS OF 11/02/2026"; per
   establishment: name, location address, license number, and its last four inspections (type, date,
   score). Sample: `151 COFFEE | 3343 LONG PRAIRIE RD | FE17-2908 | FE INSPECTION 1/20/2026 97 | 7/7/2025 90 | 7/8/2024 92 | 1/3/2024 94`.
   394 establishments, 1,414 inspection rows.
4. **Date filter**: none (whole PDF). The document id changes every month: rediscover from the page.
5. **Score**: official 0-100 (converted from demerits by the town); observed 73-100, median 99.
6. **Violations**: none. 7. **Coordinates**: none; street address.
8. **Ids**: license number (FE17-2908); no inspection id.
9. **Volume**: 2026 by month: 59, 44, 59, 60, 28, 56, 72, 34, 50, 64: about 13 a week. Newest
   2026-11-02 (the report date). History: the last four inspections per place (oldest 2018-08-31).
10. **Requests**: 2 a month. Backfill: free (four inspections per establishment in every file).
11. **Risks**: PDF only; text font is "CID TrueType, Identity-H" with a ToUnicode map (a stdlib
    parser must decode CMaps and rebuild rows from coordinates; `pdftotext -layout` reads it
    cleanly); monthly cadence means the source is up to five weeks behind by design (needs a
    `_note_source_lag`); no coordinates.

---

## Burleson (Johnson and Tarrant counties)

**Verdict: HARD (Inspect2Go; score and violations only inside one PDF per inspection). Same vendor
family as the Oklahoma City candidate, but with ordinary fonts.**

1. **Agency**: City of Burleson Environmental Services, 48k. Tarrant County Public Health's page
   lists Burleson among the cities it does NOT regulate (verified), although a stale city FAQ still
   says the county inspects; the city page keeps "restaurant scores prior to June 1, 2020" in a
   separate archive, so the portal holds inspections from that date at most. Page
   https://www.burlesontx.com/2077/Restaurants -> "Food Inspection Scores" ->
   https://1904.i2g20.com/public-portal.
2. **Platform**: Inspect2Go public portal (Laravel; cookies `XSRF-TOKEN` and `burleson_session`, no
   login, no CAPTCHA).
3. **Endpoints (verified)**:
   - Search: `GET https://1904.i2g20.com/public-portal?keyword=a&search_by=establishment_name&permission_type=food&search=true`
     -> 200, one HTML table, 319 rows, no paging: `#, Establishment Name, Address, View` with
     `https://1904.i2g20.com/view-inspection/food/<facility id>`. `keyword` is required;
     `search_by` in {establishment_name, establishment_address}; `permission_type` in {food, mobile,
     temporaryFood, pool, pwif, operationSite}. (Wrong enum values answer HTTP 500.)
   - Facility: `GET /view-inspection/food/2127` -> 302 -> `/food-inspection/2127` -> name, permit
     `FD-002127-1-2026`, and a table `#, Inspection Date, Purpose, View Pdf` with
     `https://1904.i2g20.com/food-inspection-pdf/<inspection id>`.
     Sample: `CHURCH'S TEXAS CHICKEN | FD-002127-1-2026 | 09/08/2026 Routine -> /food-inspection-pdf/3615 | 09/03/2026 Visit -> /food-inspection-pdf/3611`.
   - PDF: `GET /food-inspection-pdf/3654` -> `application/pdf`, 3 pages, produced by dompdf 0.8.6 with
     standard Type 1 fonts (Times/Helvetica, WinAnsi), 0.35 MB (3615) and 2.98 MB (3654, photos).
     Text extracts cleanly: date, permit, establishment, address, `TOTAL/SCORE 97`, counts of repeat
     and corrected violations, the 47 TX items, then "Violation comments" pairs such as
     `34. No Evidence of Insect contamination, rodent/other animals` -> "Observed gnats in the front
     counter area. Observed pests around the can storage rack".
4. **Date filter**: none. 5. **Score**: official 0-100 in the PDF (100 and 97 seen).
6. **Violations**: in the PDF, item title + comment; the tier follows from the TX item number
   (1-20 = 3 points, 21-33 = 2, 34-47 = 1). The IN/OUT marks are images, so only the comment
   section is usable. Item 34 again needs the neutral pest heading.
7. **Coordinates**: none; full address with city and zip in the list.
8. **Ids**: facility id (2127), permit `FD-00<id>-1-<year>`, inspection id (3654; dense, in entry
   order: 3615 on 2026-09-08, 3654 on 2026-10-02, so about 11 inspection records of all purposes
   per week).
9. **History**: the sampled facility pages list 2026 inspections only (permit-year scoped; whether
   older years are reachable is unverified). Newest seen 2026-10-02 (three pages sampled).
10. **Requests**: weekly: a few keyword searches to enumerate (one vowel returned 319; the total is
    unverified), about 350-400 facility pages, 8-12 PDFs. 90-day backfill: the same crawl plus about
    120 PDFs (40-350 MB).
11. **Risks**: PDF-only scores (needs a stdlib dompdf text extractor with positions, since the
    comment section is two-column); large PDFs; enumeration by keyword; no coordinates.

---

## Grapevine, Bedford, Hurst

**Verdict: ALREADY COVERED by `tarrant` (MyHealthDepartment).**

- Tarrant County Public Health's contract list (verified,
  https://www.tarrantcountytx.gov/en/public-health/health-protection-and-response/environmental-health-promotion/food/consumer-health-contract-cities.html):
  Azle, **Bedford**, Benbrook, Blue Mound, Colleyville, Crowley, Dalworthington Gardens, DFW Airport,
  Edgecliff Village, Everman, Forest Hill, **Grapevine**, Haltom City, Haslet, **Hurst**, Keller,
  Kennedale, Lakeside, Lake Worth, Pantego, Pelican Bay, Richland Hills, River Oaks, Saginaw, Sansom
  Park, Southlake, Watauga, Westlake, Westworth Village, White Settlement. "We do NOT regulate":
  Arlington, Burleson, Euless, Fort Worth, Grand Prairie, Mansfield, North Richland Hills.
- Live check of the slug for 2026-11-17 and 2026-11-18 (60 rows): Grapevine 18, Southlake 11,
  Hurst 7, Bedford 5, Haltom City 5, Crowley 4, Azle 3, ...

---

## Irving

**Verdict: BLOCKED (unchanged).**

- https://irvingtx.gov/health-inspections (the city's new domain) and
  https://www.cityofirving.org/333/Health-Inspections (redirects to www.irvingtx.gov) both answer
  **403 with Cloudflare's "Just a moment..." challenge** (`challenge-platform`). Stopped there.
- ArcGIS Online org `OfsJXUlu8pSkbl7B` re-checked: no new food item (88 items searched; newest food
  item is still `RestaurantInspections_gdb`, last edit 2024-09-19, max `inspdate` 2024-06-05,
  565 rows; `Food_Establishment_Scores` last edit 2020-02-04). Letter grades only, no violations.
- DSHS lists Irving as running its own program (verified). 257k people. Ask the city for an export.

## Grand Prairie

**Verdict: STALE, letter grades only (unchanged; not worth building).**

- https://grandprairie-tx.healthinspections.us/ (Tyler ColdFusion) is up:
  `search.cfm?searchType=alpha&letter=M` and `&start=11` paging work (10 per page; "30 facilities
  found starting with M").
- Five letters sampled today (B, C, M, S, T; first pages, 50 facilities): "Last Inspection Date"
  from 2025-06-06 to **2026-03-27**; nothing after April 2026 (the earlier full crawl found
  2026-04-17). Letter grade only (A+ ... F from demerit bands), latest inspection only, no ids, no
  coordinates, no violations. 196k people.

## Carrollton

**Verdict: BLOCKED (unchanged).** `https://www.cityofcarrollton.com/.../restaurant-scores` answers
403 "Access Denied" from `AkamaiGHost` (reference #18.b1aa3717...). Stopped. 133k people.

## Allen

**Verdict: NOTHING ONLINE (unchanged).** The Food Safety page still says "Current health
inspection scores are unavailable at this time due to system updates" (verified). The last
quarterly PDF is orphaned at
`https://cms3.revize.com/revize/allentx/Documents/Departments/Community%20Enhancement/Health%20and%20Food%20Safety/Food%20Safety/Inspection%20Scores%20July-Sept%202025.pdf`
(200, 134 KB; rating, name, address, date; 279 inspections in the quarter per the search excerpt);
guessed names for later quarters return 404. The city moved permitting to Tyler EnerGov CSS
(`cityofallentx-energovweb.tylerhost.net`) on 2026-08-01. 105k people.

## Mansfield

**Verdict: BLOCKED (bot challenge on the new portal).**

- The custom score database at `mansfieldtexashealth.com` is gone: every path now 301-redirects to
  https://mansfieldtx.portal.opengov.com/ (OpenGov Permitting and Licensing, "Regulatory Compliance
  Portal", launched August 2026 per the portal's own text), and the old host's port 443 presents a
  self-signed certificate.
- The portal's public API (`https://api-east.viewpointcloud.com/v2/mansfieldtx/...`) serves
  categories and record types anonymously; category 1086 "Food Inspection Scores" tells residents to
  use "Search" -> location -> Records -> "Food Establishment Inspection" records.
- The search itself is closed to scripts: `GET .../v2/mansfieldtx/search_results?criteria=all&key=mcdonald`
  -> **403 `{"errors":["forbidden"]}`**, and the portal bundle shows why: the "Search for public
  records" page mounts a Cloudflare **Turnstile** widget and the client attaches a Turnstile token or
  clearance to `/records`, `/records/:id` and `/locations/:id` (the code calls it a scrape defence).
  Not worked around. 73k people; scores are 0-100.

## North Richland Hills

**Verdict: BLOCKED from this container.** https://www.nrhtx.com/230/Restaurant-Scores answers 403
"Attention Required! | Cloudflare - Sorry, you have been blocked". Per search excerpts (unverified):
monthly PDFs in the CivicPlus Archive Center (`Archive.aspx?AMID=79`), e.g. "NRH RESTAURANT
INSPECTION SCORES - FEBRUARY 2026" with Site Name, Phone, Address, Service Date, Score (0-100 since
2023; examples 66-98), roughly 60-65 routine inspections a month. If reachable from the runner or
the relay it would be a HARD monthly-PDF source for 70k people; the host allowlist of the relay does
not cover it today.

## Euless

**Verdict: BLOCKED; probably NOTHING ONLINE.** `www.eulesstx.gov` answers 403 "Access Denied" from
Akamai for every path. DSHS and Tarrant County both say Euless runs its own program (verified). A
resident's 2025 comment in a search excerpt says Euless results are not visible online
(unverified). 61k people.

## Addison

**Verdict: BLOCKED; probably NOTHING ONLINE.** `developmentservices.addisontx.gov` answers 403
"Access Denied" (Akamai). The search-engine copy of its Environmental Health page lists licensing
and inspection duties but no scores (unverified). 17k residents, about 280 food licences.

## Weatherford and Parker County

**Verdict: BLOCKED.** The city's Consumer Health page (verified) links "Current Food Establishment
Scores" to
`https://permits.weatherfordtx.gov/ActivitySearchTool?Filter=Address&SearchText=&ContentType=license&Type=Food%20Establishment%20Operating%20License&SubType=&IsHideClosedStatus=false&IsSearch=true&Days=90`,
which answers **403 "Just a moment..." (Cloudflare managed challenge)**. Stopped. 31k people.
Parker County itself: DSHS Region 2/3 provides services; DSHS notes "Aledo, Azle, Mineral Wells,
Springtown, Weatherford, Willow Park, and Hudson Oaks have their own food programs" (verified in
the DSHS county layer). County verdict: NOTHING ONLINE.

## Cleburne and Johnson County

**Cleburne: BLOCKED from this container** (https://www.cleburne.net/1138/Restaurant-Inspection-Scores
-> 403 Cloudflare "Sorry, you have been blocked"). Per search excerpts (unverified): monthly PDFs
"Health Inspections by Date Range" with inspection ID, date, establishment, address, result
(IN/NOT IN COMPLIANCE) and score, listed through August 2026. 31k people.
**Johnson County**: DSHS Region 2/3 (verified); city programs listed by DSHS: Cleburne, Alvarado,
Godley; Burleson is above. County verdict: NOTHING ONLINE.

## Wylie and the Collin County program

**Verdict: NOTHING USABLE.** Wylie's page (verified) says Collin County inspects; it links yearly
PDFs. `WYLIE FOOD HEALTH SCORES 2026.pdf` (verified, created 2026-09-15, 3 pages, 76 rows) has only
`Company | City | Score`: **no inspection date and no address**. Collin County's Food Service page
(verified) still publishes no scores and points to the EnerGov Citizen Self-Service portal
(`collincountytx-energovweb.tylerhost.net`); the county program covers unincorporated areas plus
Wylie, Melissa, Lowry Crossing, Weston, Josephine and Farmersville.

## Denton County program

**Verdict: NOTHING ONLINE (unchanged).** The Environmental Health Division page (verified) has no
food, restaurant or score text. DSHS's layer says DSHS Region 2/3 provides retail food services for
the county outside the city programs (Denton, Lewisville, Flower Mound, Little Elm, The Colony,
Corinth, Highland Village, Justin, Pilot Point, Sanger, ...).

## The Colony

**Verdict: HARD and low value (PDF, no addresses).** Page
https://www.thecolonytx.gov/369/Food-Service-Inspections-Scores -> `/DocumentCenter/View/18642/Health-Scores-10-2026`
(verified: Excel-to-PDF, 7 pages, created 2026-10-15, WinAnsi TrueType fonts). 252 rows of
`ESTABLISHMENT NAME | INSPECTION DATE | SCORE/LETTER`, e.g. `ANOTHER BROKEN EGG CAFE 10/09/2026 83/B`.
Scores 76-100; newest 2026-10-09; latest inspection only; monthly document with a new id each
time. No address (a few names carry one in brackets), no ids, no violations. 45k people.

## University Park and Highland Park (Dallas County HHS)

- **University Park: PDFs ONLY (scans).** Laserfiche WebLink
  `https://weblink.uptexas.org/WebLink/Browse.aspx?id=565&dbid=0&repo=Laserfiche`; the folder JSON
  (`POST /WebLink/FolderListingService.aspx/GetFolderListing2` with
  `{"repoName":"Laserfiche","folderId":565,...}`) lists year folders 2005-2026; folder 2026
  (`329513`) holds 120 three-page PDFs named `<ESTABLISHMENT> YYYY.MM.DD` (e.g. `AMORE RESTAURANT
  2026.05.07`), uploaded in batches (June, July, August 2026). They are scans of the county's paper
  form: names and dates are machine-readable, scores (demerits, 0 best) are not. 25k people.
- **Highland Park: NOTHING ONLINE.** The page (verified) explains the demerit scale but no longer
  links a score list. 9k people.
- Both would be covered by `tx-dallas-county` if it ever publishes.

## Lancaster, DeSoto, Duncanville, Cedar Hill

- **Lancaster (41k): NOTHING ONLINE.** The page sends complaints to Dallas County (verified); a
  FY2025-26 interlocal agreement exists (search excerpt). Would come with `tx-dallas-county`.
- **DeSoto (56k): NOTHING ONLINE.** The city now has its own Environmental Health department; its
  pages list permits, fees and a quarterly donut chart, no scores (verified on /507; /444 by search
  excerpt).
- **Duncanville (41k): NOTHING ONLINE.** The old page with monthly HTML tables (last table
  "February 2024", per search excerpts) is gone: the site moved to CivicEngage and both old URLs
  return the custom 404 page; the new site's search finds nothing (verified).
- **Cedar Hill (49k): NOTHING ONLINE.** The page describes the programme only; the one score
  document a search finds is from 2018; the site search finds nothing (verified).

## Coppell

**Verdict: NOTHING ONLINE.** The page is titled "Food Safety & Restaurant Scores" but contains no
scores, link, embed or document, and the site search for "restaurant scores" returns nothing
(verified). 43k people.

## Waxahachie and Ellis County

- **Waxahachie (41k): NOTHING ONLINE** (the Health Inspection page lists applications and guides
  only; verified).
- **Ellis County**: DSHS Region 2/3 provides services (verified). City programs: Waxahachie, Red
  Oak, Midlothian, Italy, Ennis.
- **Midlothian (35k): HARD, low value.** Monthly PDF in the Archive Center
  (`https://www.midlothian.tx.us/Archive.aspx?AMID=75&Type=Recent` -> `/ArchiveCenter/ViewFile/Item/9534`,
  "Scores updated --- November 01, 2026", Excel-to-PDF, 5 pages): name, address and a grid of
  half-year columns holding `MM.DD.YYYY score` cells back to 2023.
- **Ennis (20k): name-only HTML.** https://www.ennistx.gov/foodinspectionsscore is one HTML table
  (verified: 254 rows `Establishment Name | Total Score | Date Inspected`, 2025-10-01 to
  2026-10-28, scores 66-100) with **no addresses**.

## Kaufman County

**Verdict: NOTHING ONLINE.** DSHS Region 2/3 provides services; DSHS notes "The Cities of
Seagoville, Forney, Kaufman, and Terrell have their own food programs" (verified). The City of
Kaufman's page explains a demerit scale but links no scores (verified); Terrell's page has permits
only (search excerpt).

---

## Notes for the integrator

- **Fetch-group order of value**: Garland (ArcGIS, violations with tiers) >> InstantInspector
  towns (violations with tiers) > Rowlett, Rockwall, Farmers Branch (scores or letters only).
- **Snapshot sources** (Garland, InstantInspector, Rowlett's current tab, The Colony): the source
  keeps only the latest inspection, so the weekly run IS the history. Treat them like Williamson
  County's re-read: never hold on a row that disappeared, re-ship the whole snapshot (the merge and
  the D1 upserts are idempotent), and make a failed read red.
- **Pest titles**: Garland 1634, InstantInspector item 34 and Burleson item 34 all print the TX
  title "No Evidence of Insect contamination, rodent/other animals"; use the `HH_PEST_TITLE`
  heading.
- **Blocked hosts seen today** (site-side, not the proxy): Akamai on cityofcarrollton.com,
  eulesstx.gov, developmentservices.addisontx.gov; Cloudflare challenge on irvingtx.gov and
  permits.weatherfordtx.gov; Cloudflare WAF block on nrhtx.com and cleburne.net; Turnstile on the
  OpenGov portal API used by Mansfield.
- **Where the raw evidence is**: `raw/garland/` (layer JSON, full table-3 slim pages),
  `raw/instantinspector/` (five directories and 18 reports), `raw/rowlett/` (CSV and XLSX),
  `raw/rockwall/` (layer JSON, certificate files, one PDF), `raw/farmersbranch/` (request bodies and
  answers), `raw/burleson/` (HTML and two PDFs), `raw/flowermound/`, `raw/mhd/` (Dallas County
  probes, directory), `raw/dshs/` (jurisdiction layers), `raw/pages/` (city pages).
