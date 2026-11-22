# Texas Gulf coast and Houston ring: inspection data source survey

Survey date: 2026-11-22. All probing was done live with `curl` and a desktop Chrome
User-Agent, about one request per second, a few dozen requests per host at most. Nothing
in the repository was changed. Raw responses are under
`(raw captures, not kept in the repo)`
(one folder per jurisdiction); the throwaway parsing scripts are in
`.../survey/gulf/scripts/`.

Conventions used below:

- "Verified" means I made the request myself in this session and read the response.
- "Unverified" means it comes from a search-engine excerpt, a third-party site or my own
  recall, and I did not confirm it against the source.
- Populations are 2020 Census figures recalled from memory, rounded. They are NOT
  verified live (the Census API now answers "Missing Key" without an API key). Treat
  them as order-of-magnitude.
- No request in this survey failed because of the agent proxy. Every refusal recorded
  below came from the site itself (Akamai on `gchd.org`, a Cloudflare challenge on
  `missouricitytx.gov`).

## Ranking (people covered per unit of build work)

| # | Jurisdiction | Verdict | Platform | Pop. (approx.) | Score | Per week | Biggest risk |
|---|---|---|---|---|---|---|---|
| 1 | Corpus Christi / Nueces County | BUILD | InspectHub (ColdFusion HTML) | 353k | 100 − demerits, 75-100 seen | ~75 (39-113) | Addresses carry no city or zip; an unknown id answers 200 "no violations" |
| 2 | Galveston County Health District | BUILD (score only) | Salesforce Experience Cloud, guest Apex call | 351k | 0-100 higher better, 59-100 seen | ~22 in Sept, ~1.5 since Oct 2 | Almost nothing posted after 2026-10-02; no violations |
| 3 | Beaumont | BUILD | ArcGIS MapServer (Cityworks) + Crystal PDF per inspection | 115k | 100 − demerits, 77-100 seen, city grade | ~25 (34 last week) | No address in the layer (PDF needed); 1 of 5 report links pointed at another case |
| 4 | Victoria County PHD (4 counties) | HARD | CivicPlus Document Center, weekly Word PDF | ~150k (Victoria Co. 91k) | Demerits, 0 best, 0-12 seen | ~20-40 | No ids, listing endpoint sits under `/Admin` (robots disallows `/admin`) |
| 5 | Sugar Land | HARD | CivicPlus, quarterly PDF table | 111k | 0-100, 75-100 seen | ~21 | Published once a quarter (0-3 months late); column-ordered PDF needs a positional parser |
| 6 | Pearland (own program) | BUILD, weak | City MVC list + stale ArcGIS layer | 126k | Letter only (A/B/C/FAIL) | ~25 | Newest inspection 2026-09-18; no number, no violations |
| 7 | Montgomery County TX | NOTHING ONLINE | — | 620k | — | — | Open-records only |
| 8 | Brazoria County (excl. Pearland) | NOTHING ONLINE | — | ~250k | — | — | Open-records only |
| 9 | Pasadena TX | NOTHING ONLINE | — | 152k | — | — | — |
| 10 | Baytown | NOTHING ONLINE | Tyler EnerGov CSS (permits only) | 84k | — | — | Scores page removed from the city site |
| 11 | Missouri City | BLOCKED | CivicPlus behind Cloudflare challenge; PDF | 74k | 0-100 + letter (unverified) | — | Cloudflare managed challenge |
| 12 | Port Arthur | NOTHING ONLINE | CivicPlus news flash, selected scores | 56k | 0-100 + letter | ~10 a month, selective | Not a record set: no dates, no ids, hand-picked |

"City of Pearland runs its own program" is confirmed (see its section); Brazoria County
Environmental Health covers the rest of the county.

---

## Corpus Christi / Nueces County

1. **Agency, population, URL.** Corpus Christi-Nueces County Public Health District,
   Environmental and Consumer Health (1702 Horne Rd). Nueces County ~353k (Corpus
   Christi ~318k), unverified. Public page (verified, 200):
   `https://www.corpuschristitx.gov/department-directory/health-district/food-and-environmental-inspections/restaurant-inspection-scores`
   which links to `http://inspecthub.com/remote.cfm?k=743FNC060E68BGYIBTL4YZ0U75O1U6H0&s=100`.
2. **Platform.** InspectHub (`app.inspecthub.com`, ColdFusion behind nginx). Not one of
   the families the pipeline has, but it is plain server-rendered HTML with no script,
   cookie or token requirement. Closest existing shape: a "one list + one detail page
   per inspection" portal (the Tyler ColdFusion and Hedgerow Lubbock fetchers).
3. **Endpoints (all verified).**
   - List: `GET https://app.inspecthub.com/public/remote.cfm?k=743FNC060E68BGYIBTL4YZ0U75O1U6H0&s=100`
     → 200, `text/html`, 527 KB, ONE response holding every location: 2,203 rows, each
     `<tr onClick="window.location='remote.cfm?k=…&s=100&i=<id>'" class="link"><td>NAME - STREET</td><td>SCORE</td><td>FORM</td><td>Month DD, YYYY</td></tr>`.
     It is the location's LATEST inspection only. The search box filters client-side.
   - Detail: `GET …/remote.cfm?k=…&s=100&i=<id>` → 200, 3-12 KB. `<h3>NAME - STREET</h3>`,
     `Date: November, 19 2026`, `Occupancy:` (empty), `Score : 94`, then a table
     `Points Deducted | Food Safety Requirements | Corrective Action | Corrected during Inspection`,
     then `Other Inspections for <location>`: one `<tr onClick=…&i=<older id>>` per earlier
     inspection with date and score. History is therefore reachable by link.
   - Print: `GET …/remote.cfm?a=print&k=…&i=<id>&s=100` → `application/pdf` (iText, 2
     pages), same rows plus inspector name. Not needed.
   - `s` is the score base: `s=100` renders `100 − points deducted`; `s=50` rendered the raw
     demerit totals (916 zeros). Keep `s=100`.
   - Sample (i=119211): `HUNAN EXPRESS #3 - 5614 MCARDLE ST.`, `Date: November, 03 2026`,
     `Score: 89`, rows `3 | 15. Food separated and protected | Food must be stored 6inches off the ground walk-in freezer. | No`,
     `3 | 28. Toxic substances properly identified, stored, used | Improperly storing chemicals… | No`,
     `2 | 10. Adequate handwashing facilities supplied & accessible | …does not have hot water… | No`,
     `2 | 23. Proper date marking and disposition | …`, `1 | 44. Utensils, equipment & linens… | Can opener needs to be cleaned. | No`.
     (3+3+2+2+1 = 11, 100 − 11 = 89.) The Caller-Times printed the same visit as 89.
4. **Date filter, paging, caps.** None of the three. The list is always the whole
   county in one response; there is no date parameter to test. A weekly run filters the
   list on its date column client-side, then follows each detail's history links for an
   earlier visit in the same window.
5. **Score.** Official, `100 − demerits` (47-item Texas form, 3/2/1 points). List range
   75-100: 1,289 of 2,203 are 100. The city's own bands are stated in demerits (0-10
   excellent, 11-20 good, 21-30 acceptable, 31+ poor). No letter grade. Two things that
   are not scores: pool/spa inspections print no score, and some visits print `Score : 0`
   with an empty table (i=116357, Alamo Drafthouse, 2026-04-03, between a 100 and a 98):
   treat `0` with no rows as an unscored visit, never as a zero.
6. **Violations.** In the detail HTML for every inspection: points (3/2/1), the form's
   item number and title, the inspector's free-text note (labelled "Corrective Action")
   and a corrected-on-site flag. The tier is the point value (3 and 2 "major", 1 minor,
   per the city page). I did not see the pest item's printed title; on the Texas form it
   is the title `CLAUDE.md` warns about, so ship it under a neutral heading.
7. **Coordinates / address.** No coordinates. The address is the street line only: no
   city, state or zip anywhere (list, detail or PDF). The district covers Corpus
   Christi, Port Aransas, Robstown and the rest of the county (`PORT A BEACH LODGE - 2016 ON THE BEACH`),
   so the city has to come from geocoding with a Nueces County bias.
8. **Ids.** Inspection id `i` (integer, increasing, 68849-119447 in the list). No
   facility or permit id: the location is the `NAME - STREET` string. Two rows can share
   one (`361- BLUE - SNACK BAR - 320`, ids 118171 and 118172).
9. **Volume and freshness.** Food-form rows whose latest inspection fell in each of the
   last 13 weeks: 45, 57, 62, 100, 63, 113, 108, 58, 90, 84, 39, 80, 76 (median 76).
   1,011 locations had their latest inspection in the last 90 days; about 1,300 ids were
   issued in that time (pools and unpublished included). Newest inspection seen:
   2026-11-21. History tables reach back to 2015.
10. **Requests.** Weekly: 1 list + roughly 80-160 details, about 3 minutes at 1/s.
    90-day backfill: 1 list + ~1,000 details + the superseded visits reached through
    history links, about 1,300 requests.
11. **Risks.**
    - An id that is not published answers **HTTP 200 with "This inspection does not have
      any violations."** and no name or score (i=119426, 119446). A detail page is read
      only if the `<h3>` name and the score are present; otherwise it is unread, not clean.
    - No city or zip (id stability and geocoding).
    - Pools, spas and splash pads share the list: keep the food forms
      (`2024 Inspections-Restaurants`, `Restaurant`, `Flea Market Food Vendors`, `Inspection - Restaurant 2024`).
      The form name carries a year and will be renamed.
    - The `k` tenant key lives in the city's link; re-read it from the city page if the
      list stops answering.
    - No `robots.txt` (404). No login, CAPTCHA or challenge seen. No rate limit met in 13
      requests.
12. **Verdict: BUILD.** New small HTML fetcher, list + detail, no PDF. Best ratio in
    this batch: full violation text with stated points for ~350k people.

---

## Galveston County Health District (League City, Galveston, Texas City)

1. **Agency, population, URL.** Galveston County Health District, Consumer Health
   Services. County ~351k (unverified). The district's page
   `https://www.gchd.org/.../restaurant-inspection-scores` answered **403 "Access Denied"
   from Akamai** (the site, not the agent proxy); its link target, taken from a search
   excerpt and then verified directly, is
   `https://gchdanalytics.my.site.com/gchdForms/s/gchdinspectionsearch` (200).
2. **Platform.** Salesforce Experience Cloud (Aura site, one custom LWC,
   `c:gchdPublicInspectionSearch`, calling the Apex method
   `GCHDPublicInspectionSearchController.search`). New to the pipeline. The district left
   Accela Citizen Access: `https://aca-prod.accela.com/GALVESTONCO/GeneralProperty/PropertyLookUp.aspx?isFoodFacility=Y`
   still answers 200, and a third party reports its newest inspection as 2026-01-06
   (unverified; I did not post its WebForms search).
3. **Endpoint (verified, guest access, no cookie needed).**
   ```
   POST https://gchdanalytics.my.site.com/gchdForms/s/sfsites/aura?r=1&aura.ApexAction.execute=1
   Content-Type: application/x-www-form-urlencoded; charset=UTF-8
   Referer: https://gchdanalytics.my.site.com/gchdForms/s/gchdinspectionsearch

   message={"actions":[{"id":"2;a","descriptor":"aura://ApexActionController/ACTION$execute",
     "callingDescriptor":"UNKNOWN","params":{"namespace":"","classname":"GCHDPublicInspectionSearchController",
     "method":"search","params":{"searchTerm":"77573"},"cacheable":false,"isContinuation":false}}]}
   aura.context={"mode":"PROD","fwuid":"<FWUID>","app":"siteforce:communityApp",
     "loaded":{"APPLICATION@markup://siteforce:communityApp":"<APP_HASH>"},"dn":[],"globals":{},"uad":true}
   aura.pageURI=/gchdForms/s/gchdinspectionsearch
   aura.token=null
   ```
   `<FWUID>` and `<APP_HASH>` are in the search page's HTML (`"fwuid":"…"` and
   `"APPLICATION@markup://siteforce:communityApp":"…"`); today they were
   `ZjkyMTR4bndvTWpXNFdfN1kxVEkxd2tVMjdnTGFERUU2S3FfSVdrcU92bkExNS4zLjE2MDEx` and
   `1919_p_UNHwnUfNHtXRD9ejXneg`. They change with every Salesforce release and site
   publish, so read them from the page on every run. A stale pair answers HTTP 200 with
   `"state":"ERROR"` and `aura:clientOutOfSync`.
   Response: `actions[0].returnValue.returnValue = {hasMoreThan100Matches, results[]}`;
   each result is one permit ("case"):
   `caseId, orgName, street, cityStateZip, address, caseType, caseStatus, totalInspections, lastInspection{…}, inspections[{inspectionId, inspectionDate, inspectionType, score, grade, gradeKey, year}]`.
   Sample: `a0LQP00000XyeY22AJ | WHATABURGER #40 | 2411 Palmer | TEXAS CITY, TX 77590 | Establishment | Active | totalInspections 18`,
   first inspection `{"inspectionId":"a0WQP00000E4wzp2AB","inspectionDate":"2026-08-27","inspectionType":"Regular Inspection","score":99,"grade":"Pass"}`.
4. **Date filter, paging, caps.** The method takes one argument, `searchTerm` (2+
   characters, matched against name and the full address string, zip and "TX"
   included). No date parameter, no paging, **hard cap of 100 cases** with
   `hasMoreThan100Matches`. What makes it usable: results come back ordered by last
   inspection date, newest first (rows with a null date sort ahead), and every case
   carries its whole history (in all 194 distinct cases read, `totalInspections` equalled
   the list length, up to 27). Two terms showing the order: `TX` → 100 cases, last
   inspections 2026-11-18 back to 2026-09-08; `77573` → 100 cases, 2026-11-18 back to
   2025-11-19. So `77` or `TX` is a county-wide "newest 100 facilities" poll, the Orange
   County pattern, and per-zip terms partition it when a week exceeds 100.
5. **Score.** Official, 0-100, higher is better (the page labels it "Score / 100").
   It is `100 − demerits`: LAPALAPA, 2025-12-10, is 97 here and 3 on the old portal
   (old value unverified, from a third-party excerpt). Range seen 59-100. `grade` is the
   district's outcome: `Pass` (970 of 983), `Reinspection Required` (12), `Scheduled` (1);
   the UI also defines "Enforcement". No letter grade.
6. **Violations.** None published. Score, outcome and inspection type only. (The old
   Accela portal carried none either, per a 2020 Chronicle note: "specific descriptions
   are available at the health district office", unverified.)
7. **Coordinates / address.** No coordinates. `street` plus `cityStateZip`; data entry
   is uneven (`League City, Texas 77573`, an empty `cityStateZip` with the zip inside
   `street`, `LAMARQUE`), so run it through `state_code()` and a city normaliser.
8. **Ids.** `caseId` (Salesforce id of the permit, stable) and `inspectionId`
   (Salesforce id). Both look durable; migrated history kept ids.
9. **Volume, history, freshness.** History back to 2018 and earlier per case.
   County-wide facilities by week of last inspection (term `77`): Sept 7: 21, Sept 14:
   19, Sept 21: 28, Sept 28: 19, then Oct 12: 2, Oct 19: 4, Oct 26: 1, Nov 16: 3.
   **Only 10 inspections are dated after 2026-10-02.** Newest seen: 2026-11-18 (two
   pre-opening visits); newest "Regular Inspection": 2026-10-28. Under the old system
   League City alone logged 33 inspections in December 2025, so the new portal is either
   posting in batches or carrying a fraction. There is also a hole from January to June
   2026 (the system change).
10. **Requests.** Weekly: 1 page GET + 1-3 searches (`77`, `TX`, `Texas`), or ~30
    per-zip searches to be safe. 90-day backfill: the same ~30 per-zip searches (each
    returns full history). A complete facility list needs finer terms (zip + street
    digit), a few hundred searches, once.
11. **Risks.** Posting gap above (the main one). Unversioned private Apex method: a
    rename breaks it silently into `state: ERROR`, which must be reported as an error,
    never as zero rows. Null-date and `Scheduled` rows must be dropped. Pools, septic,
    day cares and schools share the list (`caseType`): keep the food types. The page's
    CSP names reCAPTCHA, but no CAPTCHA, login or challenge was presented on the search
    (17 requests). `robots.txt` at `/gchdForms/robots.txt` is Salesforce's default
    "Allow: /". `www.gchd.org` itself is closed to plain clients (Akamai 403); the
    fetcher never needs it.
12. **Verdict: BUILD, score only.** JSON, no parser; a small Aura client plus an Orange
    County style newest-N poll. Worth re-checking the posting cadence for two or three
    weeks before building: if October and November never fill in, this is a stale source.

---

## Montgomery County TX (Conroe, The Woodlands)

1. **Agency, population.** Montgomery County Environmental Health Services, Consumer
   Health and Food Division (501 N Thompson, Conroe). County ~620k in 2020 (unverified;
   larger now).
2-10. **Nothing published.** Verified by reading five county pages (all 200):
   `…/environmental_health/index.php`, `consumer_health_and_food.php`,
   `consumer_important_links.php`, `important_links.php`, `permitting/consumer_health.php`.
   They hold applications, fee schedules and a "Request for Public Information Form";
   no inspection search, list or PDF, and the words "inspection report/score/result"
   appear nowhere as a link. Two web searches found no portal either. The only
   published results are newspaper summaries from 2013-2015 built from county-supplied
   lists (unverified). I did not guess vendor slugs.
11. **Blockers.** No source. Results are an open-records request.
12. **Verdict: NOTHING ONLINE.** The largest population in this batch; worth a records
    request or an email to the Chief Sanitarian asking for a recurring extract.

---

## Brazoria County (Lake Jackson, Angleton, Alvin; not Pearland)

1. **Agency, population.** Brazoria County Environmental Health Department (Angleton).
   County ~372k minus Pearland's ~126k (unverified).
2-10. **Nothing published.** Verified on the department's pages (all 200):
   `departments/environmental_health/index.php`, `resources.php`, `permits.php`,
   `regulations.php`. Content is permit packets, ordinances and fee schedules; no
   inspection results, and no "score" text. The food staff is a supervisor and two
   inspectors (unverified, from a search excerpt of the contact page).
12. **Verdict: NOTHING ONLINE.**

---

## Pearland (city program)

1. **Agency, population, URL.** City of Pearland, Community Development, Health
   Services (the ordinance, Ch. 11, has city health officers inspect every
   establishment at least yearly and post a letter grade; unverified excerpt). ~126k
   (unverified). Public pages: dashboard
   `https://www.arcgis.com/apps/dashboards/da47eda7507b4a91ab5a808f7016ff1b` and
   "Restaurant Report Cards" `https://gis.pearlandtx.gov/Web/RestaurantOpenData`.
2. **Platform.** Two copies of one export: an ArcGIS Online hosted feature layer
   (ArcGIS family, supported) and a city ASP.NET MVC page (custom HTML).
3. **Endpoints (verified).**
   - `https://services5.arcgis.com/iJ6DPCVBtManqbJU/arcgis/rest/services/Health_Inspections/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json`
     → 200, 537 point features, `maxRecordCount` 2000. Fields: `SITE_ALTERNATE_ID,
     LICENSE_NO, RestaurantName, RestaurantAddress, RestaurantCity, RestaurantZip,
     InspectionStatus, InspectorName, Result, Score, InspectionReport, AppliedDate,
     IssuedDate, ExprirationDate, DateofInspection` (all strings). Sample:
     `LICENSE_NO FP20-00020 | POPEYE'S | 12833 SHADOW CREEK PKWY | City Limit | 77584 | ACTIVE LIC | Result A | Score null | DateofInspection 12/12/2025 | x -95.41902 y 29.57795`.
     **`dataLastEditDate` is 2026-03-11**; newest `DateofInspection` 2026-03-10.
   - `GET https://gis.pearlandtx.gov/Web/RestaurantOpenData?sortOrder=date_desc&page=N`
     → 200, 20 rows a page, "Page 1 of 25". Columns: name, street, Result, date, license
     number linking to `https://gis.pearlandtx.gov/web/Health_Inspection_Reports/<LICENSE>.pdf`.
     Sample: `Snack and Sip | 11200 BROADWAY ST | A | 9/18/2026 | FP26-0051`.
     `SearchString=` filters by name; `sortOrder` is `Date`, `date_desc`, `name_desc`.
   - Report PDFs: `FP20-00247.pdf` → 200, Crystal Reports text PDF with `SCORE: 93`, the
     56-item form with point values and item-numbered comments, but it is the
     **2025-03-31** report (Last-Modified 2025-06-03) for a place the list says was
     inspected 9/11/2026. `FP26-0051`, `FP20-00391`, `FP20-00207`, `FP20-00020` → 404.
     The PDFs are abandoned.
4. **Date filter, paging.** No date filter in either copy (the layer's date is a string
   field). The list pages by `page=`; the layer needs one request.
5. **Score.** Letter only for current inspections: A 482, B 45, C 9, FAIL 1 in the
   layer. The city's scale (A 100-92, B 91-84, C 83-76, 75 and below fails; unverified
   excerpt). The layer's `Score` column is null in all 537 rows.
6. **Violations.** Not available (stale or missing PDFs).
7. **Coordinates / address.** The layer has point geometry for all 537 rows; street and
   zip; `RestaurantCity` is the literal `City Limit`. The web list has street only.
8. **Ids.** `LICENSE_NO` (FP20-00247 style) in both; `SITE_ALTERNATE_ID` in the layer.
   No inspection id.
9. **Volume and freshness.** About 500 licences, one row each (latest inspection). The
   newest 100 rows of the web list span 2026-08-20 to **2026-09-18**, about 25 a week.
   Nothing newer on 2026-11-22: nine weeks stale. The layer is eight months stale.
10. **Requests.** Weekly: 2-3 list pages (or all 25) + 1 layer query. Backfill: none
    possible beyond the current snapshot (one row per licence).
11. **Risks.** Staleness in both copies; letters without numbers; no history. No
    `robots.txt` rules on `gis.pearlandtx.gov` (it serves the hub page), no login or
    challenge.
12. **Verdict: BUILD, weak.** Trivial to fetch (HTML list joined to the layer's
    coordinates by licence), but it would ship a letter mapped to a band midpoint with
    no violations, from a list that has not moved since September. Not worth building
    until the city resumes publishing; then it is an afternoon.

---

## Sugar Land

1. **Agency, population, URL.** City of Sugar Land, Food Inspection Program
   (Environmental and Neighborhood Services). ~111k (unverified).
   `https://www.sugarlandtx.gov/2417/Food-Inspection-Scores` (200).
2. **Platform.** CivicPlus page linking quarterly PDFs in the Document Center. PDFs
   only.
3. **Endpoints (verified).** The page lists 19 quarters, JAN-MAR 2022 through
   `JUL 2026 - SEP 2026` → `/DocumentCenter/View/43470/FOOD-INSPECTION-SCORES_JUL-SEP-2026`
   (200, `application/pdf`, 1.2 MB, 18 pages, created 2026-10-06 by Acrobat Distiller
   from an Excel sheet; text, not a scan). The document id is not predictable: read it
   from the page. Columns: `Establishment Name | Establishment Address | Inspection Date |
   Inspection Purpose | Inspection Score | Violation Codes`. Sample:
   `99 RANCH MARKET | 3430 HWY 6 SUGAR LAND, TX 77478 | 8/10/2026 | Routine | 92 | 3-501.19 (A-D)Time as a Public Health Control; 3-305.11- Preventing Contamination -Food Storage.; … 6-403.11 Designated Areas-eating/drinking/smoking`.
4. **Date filter, paging.** Not applicable: one file per calendar quarter.
5. **Score.** Official 0-100, higher better, no letter (the city says so). Q3 2026:
   75-100 over 278 rows (41 at 100).
6. **Violations.** Food Code / TFER section number plus its short title, separated by
   semicolons. No inspector narrative, no tier, no points. The full report is a records
   request. Titles include `6-501.111- Controlling Pests` and
   `6-501.112- Removing Dead or Trapped Birds`, both title-only pest citations that need
   `CITED_PEST_ITEM_TITLES` treatment (pest chip, no F).
7. **Coordinates / address.** No coordinates. Full address with city and zip.
8. **Ids.** None. Name + address (277 distinct pairs in 278 rows).
9. **Volume and freshness.** 278 inspections in Q3 2026 (276 Routine, 2 Reinspection),
   11-35 a week, ~21 average. Newest inspection 2026-09-30; the next file is due early
   January 2027. History: 19 quarterly files back to January 2022.
10. **Requests.** Weekly: 1 page GET, plus 1 PDF four times a year. 90-day backfill:
    1-2 PDFs. Full history: 19 PDFs.
11. **Risks.** Built-in lag of up to 3 months plus a week. The PDF's content stream is
    column-ordered (all names, then all addresses, …) and the violation cell wraps above
    its own row, so rows can only be rebuilt from text positions: a positional table
    parser, new work for a stdlib pipeline. Fonts are WinAnsi TrueType subsets with
    ToUnicode. `robots.txt` disallows `/admin` and `/Search` only; `/DocumentCenter/View`
    is open. No login or challenge.
12. **Verdict: HARD.** New positional PDF table parser; quarterly cadence means the
    freshness report will flag it STALE most of the year (needs a `_note_source_lag`
    like LA County).

---

## Missouri City

1. **Agency, population, URL.** City of Missouri City, Neighborhood Services, Code
   Enforcement and Health Division. ~74k (unverified).
   `https://www.missouricitytx.gov/1355/Inspection-Rating-Score-Cards`.
2-10. **Not probed past the front door.** That URL answered **HTTP 403 with
   `cf-mitigated: challenge`** ("Just a moment… Enable JavaScript and cookies"): a
   Cloudflare managed challenge from the site, not the agent proxy. I stopped there, as
   instructed. From a search-engine excerpt of the same page (unverified): scores are
   0-100 with letters (A 90-100, B 80-89, C 70-79, the number alone below 70),
   establishments are inspected twice a year, and results are offered as two documents,
   "Restaurant Scores (PDF)" and "Clean Restaurant Awards (PDF)".
11. **Blockers.** Cloudflare challenge on the whole site; PDF-only behind it.
12. **Verdict: BLOCKED.** Do not work around it. If wanted, ask the Health Division
    [phone removed]) to email the scores PDF or publish it somewhere crawlable.

---

## Pasadena TX

1. **Agency, population.** City of Pasadena Health Department (1149 Ellsworth Dr).
   ~152k (unverified).
2-10. **Nothing published.** Verified on `pasadenatx.gov/315/Health`,
   `/317/Health-Food-Establishment-Permits`, `/2019/Health-Permits` (all 200): permit
   applications only, no link or text about inspection results. Note the trap:
   `healthinspectionreports.cityofpasadena.net` is Pasadena, **California** (already in
   `CLAUDE.md`'s candidate list), not this city. An online permitting portal launched in
   April 2026 (unverified excerpt); it is for applications.
12. **Verdict: NOTHING ONLINE.**

---

## Baytown

1. **Agency, population.** City of Baytown Health Department, Environmental Health
   (220 W Defee). ~84k (unverified).
2. **Platform.** City site on CivicPlus; permits on Tyler EnerGov Citizen Self Service
   (`https://css.baytown.org/Energov_Prod/SelfService`). EnerGov is not a family the
   pipeline has.
3-9. **No inspection results found.** A search excerpt of the Health Department page
   lists a "Restaurant Scores" navigation item (unverified); the live page
   (`/1043/Health-Department`), the Environmental Health page (`/1175`) and the site map
   (`/sitemap`, 369 KB) contain no such link or the word "score" today (verified). The
   EnerGov public search does answer without a login (verified):
   `POST …/SelfService/api/energov/search/search` with the JSON from
   `GET …/api/energov/search/criteria` and `Keyword=whataburger` returned 53 hits, among
   them operational permits such as
   `PT0000621 | Food Permit - Retail | Issued | 8730 N SH 146 BAYTOWN TX 77523 | WHATABURGER #1072`,
   and `InspectionsFound: 0`. So food permits are searchable, inspections by name are
   not. My two attempts at the per-record inspection endpoint failed on my own request
   shape (HTTP 500 and a server exception echoed back), so whether a permit's inspection
   tab shows a score is **not established**; EnerGov normally shows pass/fail status
   without checklist scores.
11. **Blockers.** No published scores; an unfamiliar API whose inspection data, if any,
    is unlikely to carry scores or violations.
12. **Verdict: NOTHING ONLINE** (scores). If Baytown matters, open one food permit in a
    browser and look at its Inspections tab before spending time on EnerGov.

---

## Beaumont

1. **Agency, population, URL.** Beaumont Public Health Department, Environmental
   Health Division (3040 College St). ~115k (unverified). Public map (launched December
   2023): `https://gis.beaumonttexas.gov/portal/apps/webappviewer/index.html?id=d2bcc8639e2940f8ac87e7abaf74b968`.
2. **Platform.** ArcGIS Enterprise MapServer on the city's own server, fed from
   Cityworks (layer name `BeaumontGIS.SDE.CW_FOOD_EST_INSP_RESULTS`). ArcGIS family
   (the McKinney fetcher's shape), plus one Crystal Reports PDF per inspection.
3. **Endpoints (verified).**
   - `https://gis.beaumonttexas.gov/arcgis/rest/services/Beaumont_Health_Inspection_Results/MapServer/1/query?where=1%3D1&outFields=*&outSR=4326&orderByFields=OBJECTID_1&f=json`
     → 200, 943 KB, 1,933 features in one response (`maxRecordCount` 2000, pagination
     supported). Fields: `OBJECTID_1, CASE_NUMBER, CASE_NAME, STATUS_CODE,
     INSPECTION_RESULT, LOCATION, INSPECTION_DATE (date), INSPECTION_SCORE (int),
     InspectionReport, OBJECTID, X, Y, ASSET_ID, FLAG_DESC` + point geometry.
     Sample: `CASE_NUMBER EHI2026-15149 | CASE_NAME La Morenita Meat Market-deli/ meat market | STATUS_CODE CLOSED | INSPECTION_RESULT GRADEA | LOCATION La Morenita Meat Market | INSPECTION_DATE 1795162854000 (2026-11-20T08:20:54Z) | INSPECTION_SCORE 92 | InspectionReport https://cityworks.beaumonttexas.gov/attachments\599342\FoodInspectionRpt.pdf | ASSET_ID FOOD.ESTA.A0001185 | FLAG_DESC Deli Establishment | geometry x -94.14209, y 30.12002`.
   - Report: the `InspectionReport` value with backslashes turned into slashes, e.g.
     `https://cityworks.beaumonttexas.gov/attachments/608999/FoodInspectionRpt%20-%202026-11-21T122446.517.pdf`
     → 200, `application/pdf`, ~110 KB, 4 pages, text. It prints `Site Name`,
     `Address: 2810 EASTEX FWY, BEAUMONT, 77703`, `Case Number`, `Score`, then
     `3 Point Demerit` / `2 Point Demerit` / `1 Point Demerit` groups of
     `OUT  28. Proper Date Marking and disposition` + the inspector's observation + the
     rule text, and `Total Demerits`.
   - Do not use `…/AGOL/Food_Establishment_Inspection_Results/MapServer/0`: every query
     on it answers `{"error":{"code":400,"message":"Failed to execute query."}}` today.
     The ArcGIS Online copy `Inspection_Results_WFL1` is a 2022 snapshot.
4. **Date filter, paging.** Server-side filter works:
   `where=INSPECTION_DATE >= DATE '2026-11-09' AND INSPECTION_DATE < DATE '2026-11-16'`
   → count 37; the same for 2026-10-05..12 → 23; `>= timestamp '2026-11-16 00:00:00'` →
   90. `resultOffset`/`resultRecordCount` with `orderByFields` work. The whole layer
   fits in one request anyway.
5. **Score.** Official `100 − total demerits` (99 with 1 demerit, 92 with 8, checked in
   two PDFs). Range 77-100 over 1,933 rows. `INSPECTION_RESULT` is the city's label:
   `GRADEA` 1,712, `GRADEB` 156, `SUPERIOR` 46, `GRADEC` 11, `ABOVEAVG` 4, `CLOSURE` 4.
   The label does not follow the number strictly (GRADEB spans 80-100, CLOSURE 94-100,
   older rows use SUPERIOR/ABOVEAVG), so ship the number and keep the label as `results`.
6. **Violations.** Only in the PDF: item number and title, free-text observation, and
   the demerit tier (3/2/1) as the group heading. 741 of 891 cases carry a link (276 of
   the 317 from the last 90 days), so about one recent inspection in eight has no
   report. The rule text that follows each observation is boilerplate and should be cut.
7. **Coordinates / address.** Point geometry on every row, identical across a case's
   rows (use the geometry: the `X`/`Y` attributes are null on 247 rows). **No address
   field**: `LOCATION` is the establishment's name, filled on the `FOOD.ESTA` row of 890
   of 891 cases and null on the others. The street, city and zip are in the PDF only.
8. **Ids.** `CASE_NUMBER` (`EHI2026-15149`) is the inspection case. The establishment
   is the `ASSET_ID` of the case's `FOOD.ESTA.A…` row (607 distinct; 890 of 891 cases
   have one). `OBJECTID` usually repeats that number but not always (37462 on the sample
   row), so do not key on it. One establishment can have several cases on a day (deli,
   retail, concession permits at one store), which the one-inspection-per-day rule
   already handles.
9. **Volume, history, freshness.** Each case appears 2-3 times (once per linked asset:
   `FOOD.ESTA…`, `ADDR.PNTS…`, a numeric id); dedupe on `CASE_NUMBER` → 891 cases.
   Distinct cases: 34 in the last 7 days, 50 in 14, 94 in 30, 317 in 90 (~25 a week).
   Newest 2026-11-21. It is a snapshot of the latest case per permit, mostly May 2026
   onward (402 EHI2026, 389 EHI2025, 100 older), not a history table.
   `INSPECTION_DATE` is local wall-clock time stored as UTC (11:52 am in the PDF is
   11:52Z in the layer): take the UTC date as the local date.
10. **Requests.** Weekly: 1 query + ~25-35 PDFs. 90-day backfill: 1 query + ~320 PDFs.
11. **Risks.**
    - One of five sampled report links was the wrong document: the row for
      `EHI2026-16495` (score 99) links a PDF that prints `Case Number: EHI2026-15139`,
      `Score: 92`. A PDF is attached only when its case number and score match the row;
      otherwise the violations are unread (ship the score without text or drop and
      hold, as policy prefers), never clean.
    - No address without the PDF, and 13% of recent cases have no PDF link at all: those
      have a name, a score and coordinates only, so `make_restaurant_id` needs a decision
      (reverse-geocode, or hold until a later inspection of the place brings an address).
    - `cityworks.beaumonttexas.gov` is behind Cloudflare (`server: cloudflare`). It
      served plain curl here; a GitHub runner may be treated differently. Probe it from
      `probe-sources.yml` first.
    - Complaint and imminent-hazard inspections are deliberately not published
      (Beaumont Enterprise interview, unverified).
    - The sibling service already broke once (the `AGOL/…` 400). No `robots.txt` on the
      GIS host (404). No login.
12. **Verdict: BUILD.** ArcGIS query (McKinney family) for score, label, id and
    coordinates; the Orange County printable pattern (stdlib PDF text, WinAnsi TrueType
    with ToUnicode) for address and violations.

---

## Port Arthur

1. **Agency, population.** City of Port Arthur Health Department, Sanitation Division
   (5860 Ninth Ave). ~56k (unverified).
2-9. **Selected scores in a news post, nothing else.** Verified:
   `https://www.portarthurtx.gov/CivicAlerts.aspx?AID=2629` (archived news flash,
   October 2025): "reporting scores for selected area restaurants", twelve entries of
   name, street and score with letter (`Vallarta's Mexican Grill | 4751 Gulfway Drive | 73(D)`),
   no inspection dates, no ids, no violations. The April 2025 post says the division
   "randomly selects restaurant scores to post" (unverified excerpt). The current news
   list and archive index show no restaurant-score post today (verified). The Beaumont
   Enterprise wrote in November 2025 that "the city does not have an online way to
   access inspections, so we file a records request" (unverified).
12. **Verdict: NOTHING ONLINE** as a record set. A dozen hand-picked, undated scores a
    month cannot feed a map.

---

## Victoria (Victoria County Public Health Department)

1. **Agency, population, URL.** Victoria County Public Health Department,
   Environmental Services. It inspects Victoria, DeWitt, Jackson and Calhoun counties
   (per the Victoria Advocate, unverified; the PDF's city headings include Cuero,
   Yoakum, Yorktown, Inez, Placedo). Victoria County ~91k, the four counties ~150k
   (unverified). `vcphd.org` now redirects to `https://www.victoriacountytx.gov/286/Public-Health`;
   the reports are the Document Center folder
   `https://www.victoriacountytx.gov/DocumentCenter/Index/278` ("Restaurant Inspection
   Reports", linked from `/303/Environmental-Services`).
2. **Platform.** CivicPlus Document Center holding one PDF per week. PDFs only.
3. **Endpoints (verified).** The folder page is script-driven; it calls:
   - Sub-folders: `POST https://www.victoriacountytx.gov/Admin/DocumentCenter/Home/_AjaxLoading`
     body `value=278&loadSource=7&checkBoxes=false&docType=0`, header
     `X-Requested-With: XMLHttpRequest` → JSON list of `{Text, Value}`: 2022=283,
     2023=282, 2024=281, 2025=280, 2026=279. `value=279` → the months
     (`November 2026`=620, `October 2026`=568, …).
   - Documents: `POST …/Admin/DocumentCenter/Home/Document_AjaxBinding?renderMode=0&loadSource=7`
     body `folderId=620&getDocuments=1&imageRepo=false&renderMode=0&loadSource=7&requestingModuleID=75&searchString=&pageNumber=1&rowsPerPage=100&sortColumn=UploadDate&sortOrder=1`
     → `{"Documents":[{ID, DisplayName, URL, FileSize, LastModifiedDateString, …}], "TotalCount":3}`.
     November 2026: `2945 October 29 - November 4`, `2949 November 5 - November 11`,
     `2961 November 12 - November 18` (modified Nov 20).
   - File: `GET https://www.victoriacountytx.gov/DocumentCenter/View/2961/November-12---November-18-PDF`
     → 200, 36 KB, 8 pages, Microsoft Word 2019 export, Arial WinAnsi, plain text.
   - Layout: page header `Inspections Between (inclusive): 11/12/2026 and 11/18/2026`,
     a city heading, then per establishment `NAME … STREET ZIP`, then per visit
     `DATE  TYPE  SCORE  TIME-IN … DATE-CLOSED`, the inspector's item-numbered notes,
     and the cited form items as `Title - points`. Sample:
     `Heathenz | 2107 Lone Tree Rd 77901 | 11/12/2026 | Routine | 1 | "34. Pest found in ladies restroom." | No Evidence of Insect contamination, rodent / other animals - 1`.
4. **Date filter, paging.** Not applicable: one file per Thursday-to-Wednesday week,
   filed by year and month.
5. **Score.** Demerits, lower is better (0 perfect; the Advocate describes 3/2/1 for
   Priority / Priority Foundation / Core). Seen 0-12 in this file, up to 23 in recent
   newspaper reprints (unverified). `risk_score = 100 − demerits` matches the other
   Texas-form sources. Follow-up visits print 0 and are not scored inspections.
6. **Violations.** Yes, twice: the inspector's free-text notes keyed by item number,
   and the form item titles with their points (the stated tier). The pest item prints
   as the exact title `CLAUDE.md` warns about (`No Evidence of Insect contamination,
   rodent / other animals`), so it must ship under a neutral heading.
7. **Coordinates / address.** No coordinates. Street and zip on the name line; the city
   is the section heading above it.
8. **Ids.** None at all: name + address. Mobile units print a name and zip with no
   street (unverified, from newspaper reprints).
9. **Volume, history, freshness.** This week's file: 21 visits (12 Routine, 9
   Violations Followup). Newspaper reprints of other weeks say 18 and 39 (unverified).
   Folders run 2022-2026. Newest inspection 2026-11-18, file posted 2026-11-20: two days
   behind.
10. **Requests.** Weekly: 2 listing POSTs + 1 PDF. 90-day backfill: ~6 listing POSTs +
    13 PDFs.
11. **Risks.** The two listing calls live under `/Admin/…` and the site's `robots.txt`
    says `Disallow: /admin` for all agents; they are the page's own public calls, but
    it is a courtesy question to settle before automating (asking the department for
    the weekly file by email is the clean alternative). A record that crosses a page
    break is printed again under a repeated header with the remaining items (La Cantina:
    7 points on page 4, 5 on page 5, total 12), so items must be merged per visit and
    checked against the printed score. No ids. A hand-made Word export can change
    without notice. No login or challenge.
12. **Verdict: HARD.** New PDF text parser (easy fonts, awkward layout) and name-based
    identity, for ~150k people across four counties.

---

## Appendix: what was fetched

| Host | Requests | Outcome |
|---|---|---|
| app.inspecthub.com | 13 | all 200 (robots.txt 404) |
| gchdanalytics.my.site.com | 17 | all 200; two were my own malformed Aura calls |
| www.gchd.org | 1 | 403 Akamai "Access Denied" (site) |
| aca-prod.accela.com | 1 | 200 (old Galveston search page only) |
| www.mctx.org | 5 | 200 |
| www.brazoriacountytx.gov | 4 | 200 |
| www.arcgis.com, services5.arcgis.com (Pearland) | 9 | 200 |
| gis.pearlandtx.gov | 13 | list 200; 4 of 5 report PDFs 404 |
| www.sugarlandtx.gov | 3 | 200 |
| www.missouricitytx.gov | 2 | 403 Cloudflare challenge (site); stopped |
| www.pasadenatx.gov | 3 | 200 |
| www.baytown.org, css.baytown.org | 4 + 6 | 200; two EnerGov calls failed on my request shape |
| gis.beaumonttexas.gov, cityworks.beaumonttexas.gov | 17 + 5 | 200; the `AGOL/…` layer returns a 400 error body |
| www.portarthurtx.gov | 4 | 200 |
| www.victoriacountytx.gov | 11 | 200 |
| api.census.gov | 2 | redirected to "Missing Key"; populations left unverified |
