# Texas (central, north, east) source survey — live probe 2026-11-22

Probed from the cloud container with `curl` (Chrome 130 User-Agent), about one
request a second, 1-30 requests per host. Raw captures are under
`(raw captures, not kept in the repo)`.
Nothing in the repo was touched.

Conventions in this report:

- "Verified" means I fetched it myself today. Anything taken from a search
  snippet or a third-party site is labelled **unverified**.
- Populations are from the Census Bureau's Vintage 2024 files, downloaded
  today (`www2.census.gov/programs-surveys/popest/datasets/2020-2024/`,
  `co-est2024-alldata.csv` and `sub-est2024_48.csv`): the 2020 base and the
  July 2024 estimate.
- No request failed at the agent proxy. Every 403 below came from the site
  itself (Akamai, Cloudflare WAF or a JS bot challenge) and is named as such.
  I did not work around any of them.

## Ranking (people covered per unit of build work)

| # | Jurisdiction | Verdict | Platform | People (2024 est.) | Score | Inspections / week | Biggest risk |
|---|---|---|---|---|---|---|---|
| 1 | Wichita Falls-Wichita County | **BUILD** (Hedgerow, Lubbock clone) | Hedgerow "Hedgehog" portal | 130k county | official 0-100 | ~15-45 | tier is in the category suffix, not `severity`; type is in the name suffix |
| 2 | Waco-McLennan County | **BUILD** (ArcGIS, McKinney-style demerits) | ArcGIS Online Feature Service | 270k county | demerits 0-50, lower better | ~45-60 | no violations, no ids; 20% of rows are FAILED (policy call) |
| 3 | New Braunfels (Comal County) | **BUILD** (ArcGIS snapshot) | ArcGIS Server MapServer | 116k city (county 202k) | official 0-100 | ~6-24 | latest inspection only: a missed week is lost; no violations |
| 4 | Bell County (Killeen, Temple, Belton) | **HARD** (new PDF layout parser) | weekly PDF, MS Access report | 400k county | official 0-100 + item points | ~70-85 | posted by hand in batches, 3-6 weeks late; no ids, no coordinates |
| 5 | Brazos County (Bryan, College Station) | **HARD** (new PDF layout parser + full list re-read) | Inspect2Go / GovOwl | 250k county | official 0-100 (PDF only) | ~55 (all programs) | no date filter: ~1,560 list calls a week; score and items only in a PDF |
| 6 | Seguin (Guadalupe County) | **BUILD** (ArcGIS, scores only) | ArcGIS Server Feature Service fed by Tyler EnerGov | 39k city (county 195k) | official 0-100 | ~10 | layer date is the SCHEDULED date; checklist is behind a login |
| 7 | Nacogdoches | **BUILD** (one CSV, scores only) | published Google Sheet | 32k city | demerits 0-21 | ~8-13 | hand-typed sheet, latest visit only, no ids |
| 8 | Longview (Gregg County) | **HARD**, low value | Crystal Reports PDFs in Laserfiche WebLink | 84k city | letter A-D only | ~21 | no numeric score, no violations, weekly file overwritten |
| 9 | San Marcos / Hays County | **BLOCKED** (login) / nothing found | MyGovernmentOnline "MGO Connect" | 74k city (county 292k) | n/a | n/a | scores report needs an MGO account; county site is behind a Cloudflare WAF |
| 10 | Texarkana / Bowie County | **NOTHING ONLINE** | - | 36k city (county 92k) | n/a | n/a | - |
| 11 | Texas DSHS (state-inspected areas) | **NOTHING ONLINE** | - | - | no score on the state form | n/a | open-records requests only |

Comal County outside New Braunfels and most of Guadalupe County outside its
cities are inspected by DSHS (verified on the DSHS Region 8 page), so they fall
under row 11.

---

## Wichita Falls (Wichita Falls-Wichita County Public Health District)

**Verdict: BUILD.** A fourth tenant for the existing Hedgerow family
(`HH_PORTALS`), configured like Lubbock (`'list': 'paged'`, sorted
`lastInspectionDate` desc, official score). Two small code differences, below.

1. **Agency, population, URL.** Wichita Falls-Wichita County Public Health
   District, Environmental Health. Wichita County 129,360 (2020) / 129,984
   (2024); Wichita Falls city 102,372, Burkburnett 11,101, Iowa Park 6,583.
   Public portal: <https://wfeh-portal.hedgerowsoftware.com/> (food page:
   `#/d88b111d-edad-49f8-83a8-8c1d7931f99b/...`). The district's press release
   (2024-10-14, search snippet, **unverified** first-hand) says the scores moved
   to this Hedgerow portal in October 2024.
2. **Platform.** Hedgerow Software "Hedgehog Portal" (the portal footer in the
   search index says v5.7.4.4780, **unverified**; Lubbock runs 5.7.7). The
   front end proxies the API to `wfeh-app.hedgerowsoftware.com` (a 404 body
   echoes that host). `startup` says `isCaptchaConfigured: false`.
3. **Endpoints (all 200, no auth, JSON).**
   - `GET https://wfeh-portal.hedgerowsoftware.com/api/v0/portal/siteInfo?locale=en`
     → menu items; the `menuType 5` item "Food Safety Inspections" has id
     **`d88b111d-edad-49f8-83a8-8c1d7931f99b`** (the same GUID Lubbock uses).
     Other programs: Aquatic `82d244ac-…`, Lodging `5a7e3c28-…` (never needed).
   - `GET …/api/v0/portal/startup?locale=en` (53,847 B) → configuration,
     `disclosureConfiguration.programSettings`.
   - `POST …/api/v0/portal/disclosure/program/facilities` with
     `{"pageNumber":0,"pageSize":500,"criteria":"","sort":[{"field":"lastInspectionDate","order":"desc"}],"disclosureProgramId":"d88b111d-edad-49f8-83a8-8c1d7931f99b","fields":[],"filters":[]}`
     → `{"pageNumber":0,"pageSize":500,"totalNumberOfRecords":764,"result":[…]}`
     (453,591 B, 1.25 s). Page 1 returned the other 264 rows.
   - `GET …/disclosure/program/d88b111d-edad-49f8-83a8-8c1d7931f99b/facility/{facilityId}/inspectionDetails/`
     → list of that facility's inspections (0.5 s).
   - `GET …/disclosure/program/inspectionResults/{inspectionId}` → cited items;
     `[]` for a clean visit (verified on a score-100 inspection).

   Sample facility row (trimmed):
   ```json
   {"id":"3b325b5a-e53d-4c33-ba38-eee651c7d141","hazardScore":96,"hazardRating":"A",
    "facilityType":null,"lastInspectionDate":"2026-11-19T14:45:26.9591709-06:00",
    "facilityName":"Taco Casa #73 - Retail Food Establishment (Medium)",
    "siteAddress":"3404 MAPLEWOOD DRIVE, Wichita Falls TX 76308",
    "closure":false,"latitude":33.8736118,"longitude":-98.5382038}
   ```
   Sample inspection row:
   ```json
   {"id":"816f3485-5ba7-4056-a026-1bf03a5a6c16","inspectionDate":"2026-11-19T14:45:26.9591709-06:00",
    "inspectionType":"Permit Inspection","inspectionReason":" Permit","criticalInfractionCount":0,
    "inspectionNumber":"INSP-022432","nonCriticalInfractionCount":2,"hazardScore":96,
    "hazardRating":"INFO_REDACTED","hasInfractions":null}
   ```
   Sample cited item:
   ```json
   {"description":"Proper Cold Holding Temperature (<41°F or 45°F for shelled eggs)",
    "result":"Corrected During Inspection","isCritical":false,
    "category":{"description":"Time and Temperature for Food Safety (P)"},
    "cannedComments":[{"description":"See violation observations above","severity":null,
      "observationComment":"Food on the cold hold rail had temperatures from 49F - 51F (41F or less is required)."}]}
   ```
4. **Date filter, paging, caps.** No date filter (as on every Hedgerow
   portal). The list honours `pageSize` 500 and the `lastInspectionDate desc`
   sort: all 762 dated rows were in descending order across both pages, the two
   never-inspected rows came last, 764 distinct ids. So the Lubbock read
   (stop at the first page whose last row is older than the window) works
   as is.
5. **Score.** Official 0-100 `hazardScore`, higher is better, the Texas form's
   3/2/1 deductions (the sample: one Priority item + one Core item = 96).
   Observed range on the facility list 84-100 (742 A, 20 B). The letter is
   published only for a facility's newest inspection; per inspection it is the
   literal `INFO_REDACTED`, exactly as in Lubbock.
6. **Violations.** Per inspection, with the inspector's observation text
   (`cannedComments[].observationComment`). The tier IS stated, but not where
   the fetcher looks: `cannedComments[].severity` is `null` here and the tier is
   the suffix of `category.description` — `(P)`, `(PF)`, `(C)`. `_hh_violations`
   today takes `HH_SEVERITY.get(c.get('severity'))` and would fall back to the
   classifier, so it needs a small addition: read the suffix when `severity` is
   empty. Item titles are the Texas form's; the pest title is handled by the
   existing `_HH_PEST_TITLE_RE`.
7. **Coordinates, address.** `latitude`/`longitude` on 738 of 764 rows.
   `siteAddress` is one string, `'STREET, City TX 76308'` (26 rows null: pop-up
   vendors). Cities seen: Wichita Falls 613, Burkburnett 55, Iowa Park 34,
   Electra 16.
8. **Ids.** Facility GUID; inspection GUID plus `inspectionNumber`
   (`INSP-022432`).
9. **Volume, history, freshness.** 764 facilities. Facilities whose LATEST
   inspection fell in each recent week (a lower bound on inspections):
   10-19: 44, 10-26: 22, 11-02: 30, 11-09: 15, 11-16 (to Thursday): 17. Newest
   inspection seen **2026-11-19** (three days old). History starts at the
   October 2024 go-live: the sampled facility's oldest visit is 2024-10-21;
   717 of 764 facilities were last inspected in 2026.
10. **Requests.** Weekly: 1 list page + one `inspectionDetails` per facility in
    the window (~15-45) + one `inspectionResults` per inspection ≈ 60-100.
    90 days: 2 list pages + ~390 facility lists + ~450 result reads ≈ 850.
    Whole history: 2 + 764 + ~2,300.
11. **Risks.** `facilityType` is `null` on every row, so the mobile / pop-up
    drop cannot use `HH_MOBILE_TYPE_RE` on the type: the type is the suffix of
    `facilityName` after the last `' - '` (622 "Retail Food Establishment
    (Small/Medium/Large/Restricted Operation)", 29 pop-up/mobile vendors, the
    rest grocery departments, concession stands, etc.). The suffix must also be
    stripped from the name, and a grocery with "(Add-on)" departments yields
    several facilities at one address (the same-day-worst rule covers it).
    `/robots.txt` answers with the SPA shell (no robots file). Undocumented
    vendor API, same caveats as Lubbock.
12. **Verdict.** BUILD — Hedgerow family, Lubbock configuration:
    `base https://wfeh-portal.hedgerowsoftware.com/api/v0/portal/`, `program
    d88b111d-edad-49f8-83a8-8c1d7931f99b`, `list paged`, `list_sort
    lastInspectionDate`, `dated_list True`, `official_score True`, `results
    grade`, `history_start 2024-10-01`, `lag_margin_days 0`, plus (a) tier from
    the category suffix and (b) type from the name suffix.

---

## Waco / McLennan County (Waco-McLennan County Public Health District)

**Verdict: BUILD.** One ArcGIS Online layer, scores only, with coordinates.
The closest existing fetcher is McKinney's (ArcGIS layer query, demerit total
shipped as `100 − demerits`), minus the item columns.

1. **Agency, population, URL.** Waco-McLennan County Public Health District,
   Environmental Health (a City of Waco department serving the county).
   McLennan County 260,582 (2020) / 270,358 (2024); Waco 146,608. Public page:
   <https://www.waco-texas.com/Departments/Health-District/Environmental-Health/Food-Inspection-Scores>.
   **That page answers 403 "Access Denied" (Akamai, `errors.edgesuite.net`
   reference) to curl from this container** — the site refusing, not the proxy.
   I did not work around it; the data is on a different, open host.
2. **Platform.** ArcGIS Online hosted Feature Service (view), org
   `oUXiR7ziAPAzGw6X` (WacoGIS). Item `ea352c6df0af40e2b60166276059c36f`
   "foodScoresView", public. The city's "Resturaunt Inspection Scores"
   Experience (item `8634ffe3e87e4459979385586b3558df`) reads the same layer
   (verified in its config).
3. **Endpoint.**
   `https://services2.arcgis.com/oUXiR7ziAPAzGw6X/arcgis/rest/services/foodScoresView/FeatureServer/0/query`
   (capabilities `Query`, `maxRecordCount` 2000, pagination, orderBy and
   statistics supported; point geometry). Fields: `Date` (date), `Business`,
   `Address`, `City`, `Zipcode`, `TotalScore` (int), `Status`, `IORDER` (int),
   `SVALUE` (inspection type), `mapDate` (always null), `OBJECTID`.

   Sample request and row (`outSR=4326`):
   `…/query?where=1=1&outFields=*&orderByFields=Date DESC&resultRecordCount=5&outSR=4326&f=json`
   ```json
   {"Date":1794586551000,"Business":"SIP CITY","Address":"9111  JORDAN LN","City":"WOODWAY",
    "Zipcode":"76712","TotalScore":6,"Status":"PASSED","IORDER":1,"SVALUE":"RENEWAL",
    "OBJECTID":490170}  geometry {"x":-97.22241272930366,"y":31.497162121177606}
   ```
4. **Date filter, paging, caps.** Server-side date filter works:
   `where=Date >= DATE '2026-11-09' AND Date < DATE '2026-11-16'` → 54;
   `… '2026-10-05' … '2026-10-12'` → 60. Paging with
   `resultOffset`/`resultRecordCount` (2000 a page; the whole layer is 3 pages:
   2000 + 2000 + 1066, `exceededTransferLimit` true on the first two, 5,066
   distinct OBJECTIDs).
5. **Score.** `TotalScore` is **demerit points, lower is better** (the
   district's own page text, via the search index: "The smaller the score the
   better … total number of demerits"). Observed 0-50: 1,616 zeros, median
   around 2, 31 rows above 30. No letter grade. `Status` is the district's
   verdict: PASSED 4,033 (demerits 0-23, mean 2.6), FAILED 695 (0-50, mean
   14.1), FAILED WITH PENALTY BLIC 317 (0-47, mean 16.8), COMPLETED 12, PASSED WITH
   EXCEPTIONS 3, PARTIAL PASS 3, CANCELED 2, REQUESTED 1. `SVALUE`: RENEWAL
   2,387, ROUTINE 1,052, REINSPECTION 990, PERMIT 594, COMPLAINT 42, OTHER 1.
6. **Violations.** None published. The layer has no observation, item or
   tier field and there is no related table.
7. **Coordinates, address.** Point geometry on every row (0 missing); street,
   city and zip fields. `City` carries variants such as
   `WACO (UNINCORPORATED)`, `ELM MOTT (UNINCORPORATED)` — strip the suffix.
   581 rows have "INACTIVE" in the business name (the district's own marker).
8. **Ids.** None stable. There is no permit or inspection number, and
   `OBJECTID` is not trustworthy: 5,066 rows are numbered 482,697-490,201, so
   the table is evidently reloaded rather than appended (not verified across
   two days). The usable key is name + address + city + inspection date; 145
   name+address+day groups have more than one row (several permits at one
   address, or two visits a day).
9. **Volume, history, freshness.** 5,066 rows from 2025-01-02 to
   **2026-11-13**; the layer's `dataLastEditDate` is 2026-11-17 15:20 UTC, so
   it is refreshed in batches and was 9 days behind today. Rows per week
   (Monday weeks): 09-21: 44, 09-28: 46, 10-05: 60, 10-12: 43, 10-19: 60,
   10-26: 38, 11-02: 48, 11-09: 54 — about 200-270 a month, steady since
   January 2025. 1,758 distinct name+address+city.
10. **Requests.** Weekly: 1 (a windowed query). 90 days: 1 (~650 rows). Whole
    layer: 3.
11. **Risks.**
    - **Time zone trap:** `Date` holds LOCAL wall-clock time written as if it
      were UTC (the UTC-hour histogram peaks 08-17, nothing at night). Take the
      UTC calendar date as the inspection day; do not convert to Central.
    - The start of history (2025-01-02) looks like a rolling window; whether
      old rows age out is unknown.
    - No violations means no text-forced F and nothing for Jev: this is a
      scores-only source like Williamson County.
    - Policy call for the owner: 20% of rows are FAILED / FAILED WITH PENALTY
      at modest demerit counts (287 of the 1,012 failed rows have ≤10
      demerits), so `100 − demerits` alone would show most of them as A or B.
      REINSPECTION rows (990) carry their own, usually lower, demerit count.
      "BLIC" is not explained anywhere I could read.
    - The Akamai block is on the city's web pages only; `services2.arcgis.com`
      answered every request.
12. **Verdict.** BUILD — ArcGIS Feature Service query like McKinney's
    (demerit total → `risk_score = 100 − TotalScore`), scores only like
    Williamson County. State `TX`, metro none.

---

## Bell County (Killeen, Temple, Belton; Bell County Public Health District)

**Verdict: HARD.** The data is rich (score, narrative, item deductions) but it
exists only as weekly PDFs posted by hand, weeks late.

1. **Agency, population, URL.** Bell County Public Health District. Bell
   County 370,790 (2020) / 399,578 (2024): Killeen 160,616, Temple 96,267,
   Harker Heights 35,263, Belton 25,635. Page:
   <https://www.bellcountyhealth.org/environmental_health/retail_food/inspections.php>.
2. **Platform.** Revize CMS "Document Center" listing PDFs. Each PDF is a
   Microsoft Access report export ("Microsoft® Access® for Microsoft 365"),
   text-based (Arial, one Type0 font with a ToUnicode map), not a scan. No
   portal, no API. (A third-party site refers to a Bell County "portal"; the
   district's retail-food pages link to nothing but these PDFs — verified on
   the index page.)
3. **Endpoints.**
   - `GET https://www.bellcountyhealth.org/environmental_health/retail_food/inspections.php`
     (200, 376 KB): 119 weekly report links grouped by month, July 2024 to
     October 2026. Link form:
     `https://www.bellcountyhealth.org/Documents/Environmental Health/Retail Food/Inspection Reports/<name>.pdf?t=<upload stamp YYYYMMDDhhmmss0>`
     which 302-redirects to
     `https://cms3.revize.com/revize/bellcountyhealthtx/Documents/…`.
   - Example (200, 235,110 B, 36 pages):
     `https://www.bellcountyhealth.org/Documents/Environmental%20Health/Retail%20Food/Inspection%20Reports/Oct%2018%20thru%2024.pdf?t=202611161309320`

   One inspection as `pdftotext -layout` shows it:
   ```
   Bell County Public Health District                              10/26/2026
   Inspections Between (inclusive): 10/18/2026 and 10/24/2026   Follow up  Date Insp.
   Establishment   Type                        Score   In / Out   Reqd.     Closed
   BELTON
   CLEM MIKESKAS BBQ - BELTON                     96    11:45 AM
    2170 B NORTH MAIN 76513
    10/22/2026     Permit                               12:30 PM            10/23/2026
   From 9/26/23,10/23/24
   49. Clean shelving for tables as needed-RPT
   …
   2 No CFM in facility during inspection. A CFM must be in the building during hours of operation.
      Non-food-contact surfaces clean - 1
      Physical facilities installed, maintained, and clean - 1
      Certified Food Protection Manager / Food Handler Certification - 2
   ```
4. **Date filter, paging, caps.** None; one PDF is one week. The authoritative
   window is the line inside the PDF ("Inspections Between (inclusive): …"),
   not the link text or file name: the link titled "September 20, 2026 through
   September 26, 2026" pointed (per the search index copy, **unverified**
   first-hand) at a report for 8/20-8/26, and file names follow no pattern
   (`Oct 18 thru 24.pdf`, `714thru72124.pdf`, `Inspection Scores for the
   newspaper.pdf`).
5. **Score.** Official 0-100, higher is better; verified to be 100 minus the
   listed item points (96 = 1 + 1 + 2). Follow-up visits print 0 (no score).
   Range in the 10/18-10/24 report: 84-100 scored (18 at 100), five 0s. No
   letter grade.
6. **Violations.** Yes, per inspection, in the PDF: the inspector's narrative
   (numbered by form item, including carried-over items "From 9/26/23…",
   "RPT" repeats, "Corrected") and then one line per cited form item with its
   points, `<item title> - <points>`. The points are the tier: 3/2/1 on the
   47-item form used until 2026-04-11 (a March 2026 report had 28 three-point,
   39 two-point, 84 one-point lines) and 2/1 on the 56-item DSHS form used
   since 2026-04-12 (October: 52 two-point, 106 one-point). Tier names
   (priority / foundation / core) are not printed.
7. **Coordinates, address.** No coordinates. Street + zip under the name;
   city is the group heading (KILLEEN, TEMPLE, BELTON, HARKER HEIGHTS, SALADO,
   NOLANVILLE, HOLLAND, ROGERS, "No City" for event vendors).
8. **Ids.** None (no permit number, no inspection number). Key is name +
   address + date.
9. **Volume, history, freshness.** Inspections per weekly report: 71
   (10/18-10/24/2026), 72 (9/13-9/19/2026), 83 (3/1-3/7/2026), 37
   (7/14-7/21/2024). Types in October: Permit 38, Routine 28, Violations
   Followup 3, Complaint Followup 2. History on this page from July 2024
   (119 files); an older "Past Inspection Reports" list, August 2023 to June
   2024 (34 files), is on the Food Safety & Sanitation page (search index,
   **unverified** first-hand). **Newest inspection published: 2026-10-23** — a
   month old. The report was generated 2026-10-26 and linked on 2026-11-16;
   upload stamps of the last batches: 08-06, 09-03, 09-30, 11-16.
10. **Requests.** Weekly: 1 page + 0-4 new PDFs. 90 days: 1 + ~13. All:
    1 + 119 (+34).
11. **Risks.**
    - Posted by hand in batches, 3-6 weeks behind: the group would read STALE
      most weeks (a `_note_source_lag` case).
    - Human file names and occasional wrong links; duplicates across reports
      are likely (the heading, not the name, is the key).
    - Layout parser needed: rows wrap, a page break can split an inspection
      (the page header repeats mid-record), and the July 2024 layout differs
      from the 2026 one (my quick regex found 0 score rows there).
    - Narrative lines mention pests in passing ("to prevent vector entry") and
      the form title "Insects, rodents, and animals not present - 1" appears as
      a deduction line: it is already in `CITED_PEST_ITEM_TITLES`, but the
      trailing "- 1" must be stripped before the whole-line match.
    - No coordinates: every record depends on geocoding.
    - `robots.txt` redirects to `cms3.revize.com/…/robots.txt` (not read).
12. **Verdict.** HARD — new parser: weekly-PDF discovery from the page plus a
    positional text parser built on the existing stdlib PDF helpers
    (`_pdf_objects`, `_pdf_tounicode`, text rows as in `_oc_pdf_text_rows`).
    Largest population in this batch, real violation text.

---

## Brazos County (Bryan, College Station; Brazos County Health District)

**Verdict: HARD.** JSON lists are easy, but the score and the cited items are
only in a per-inspection PDF and nothing is date-filterable.

1. **Agency, population, URL.** Brazos County Health District. Brazos County
   233,849 (2020) / 249,624 (2024): College Station 128,023, Bryan 91,541.
   Portal: <https://brazos.inspect2go33.com/Establishments> ("Restaurant
   Inspections (public portal)" on the district's Retail Food page). The
   district's own site `health.brazoscountytx.gov` answered **403 with a
   "Checking your browser…" JS bot challenge** (the site, not the proxy); the
   portal URL came from the search index and the portal itself is open.
2. **Platform.** Inspect2Go / "GovOwl" ASP.NET app ("BrazosApp", IIS, Plesk).
   Not the Inspect2Go product seen in Oklahoma City: this one has a JSON list
   and small one-page PDFs made with wkhtmltopdf.
3. **Endpoints (no auth, and no cookie: the server set none on any
   response).**
   - `POST https://brazos.inspect2go33.com/SearchEstablishments`, form body
     `estName=&permitNo=&estAddress=&estType=RF` → `{"data":[…]}`, **all 1,555
     retail-food establishments in one response** (358 KB, 1.2 s). `estType`
     values: RF retail food, MF mobile, TF temporary, DS day care, FH foster
     home, PL pools.
     ```json
     {"id":778,"permitNumber":"RF-000778","name":"HEB #543 PRODUCE/CENTER STORE",
      "address":"1900 TEXAS AVE S","applicationCode":"RF","encryptedId":"YcD2WkDJc2xOPxBCPfDx5Q"}
     ```
   - `GET https://brazos.inspect2go33.com/GetAllInspections?id=<establishment encryptedId>`
     → `{"data":[…]}`, every inspection of that establishment, newest first
     (0.4 s):
     ```json
     {"id":4172,"inspectionDate":"2026-05-21T00:00:00","inspectedBy":"…","purpose":"Routine",
      "applicationForCode":"RF","constrctId":0,"encryptedId":"tDZ1Qk3nN0dOjVe5C40V_A"}
     ```
   - `GET https://brazos.inspect2go33.com/DownloadInspectionCheckListPdf?id=<inspection encryptedId>`
     → the report PDF (148 KB, one page, 3-8 s). Other purposes use other
     paths (from `/js/SearchInspection/SearchInspection.js`): "Opening
     Inspection" → `/DownloadPreOpeningCheckListPdf`, "Walk Through" →
     `/DownloadtWalkThroughCheckListPdf`.
4. **Date filter, paging, caps.** No date filter anywhere: the search takes
   name, permit number, address and type only, and the establishment row has
   no last-inspection date. No paging (whole list at once). So a weekly run has
   to read every establishment's inspection list.
5. **Score.** Official 0-100 "TOTAL SCORE", higher is better, **only inside
   the PDF**; the JSON list has no score field. Verified 100 − item points on
   one report (92 = 3 + 2 + 1 + 1 + 1). Seen: 92 and a 63 (2026-11-19, "Follow-Up:
   Yes"). No letter grade. Per the local TV station (**unverified**): non-scored
   visits are "N/S", and a permit suspension follows a score below 70.
6. **Violations.** The PDF is page 1 of the Texas 47-item form: each item has
   a number and a title, and a cited item shows its points in the OUT column
   (3 = Priority, 2 = Priority Foundation, 1 = Core — the section headings
   state it), with COS and repeat marks. **No inspector observations** are
   published (page 2 of the packet is not online). So the violation text is
   the item titles plus a source-stated tier, as for McKinney.
7. **Coordinates, address.** No coordinates. The list has the street only;
   the PDF has "Physical Address: 1900 TEXAS AVE S, TX, COLLEGE STATION, 77840"
   (city and zip).
8. **Ids.** Establishment `id` (int) and `permitNumber` (`RF-000778`; 47 rows
   have none); inspection `id` (dense integer in entry order across all
   programs) and an opaque `encryptedId` for each.
9. **Volume, history, freshness.** 1,555 RF establishments. History starts
   2024-10-30 (inspection id 14): the system's go-live. Inspection ids ran from
   6001 (2026-10-09) to 6338 (2026-11-20): **~56 a week across all programs**;
   the retail-food share was not measured (a third-party site says 55 "this
   week", **unverified**). Sampled establishments have 1-5 inspections each.
   Newest inspection seen **2026-11-20** (two days old), so the portal is
   current.
10. **Requests.** Weekly: 1 list + 1,555 inspection lists + ~60 PDFs ≈ 1,620
    (about 30 minutes at one a second). 90 days: 1 + 1,555 + ~700 PDFs. Whole
    history: 1 + 1,555 + roughly 4,500 PDFs.
11. **Risks.**
    - 1,555 list calls every week for ~60 new inspections; no cheaper path
      (the PDF needs the `encryptedId`, which only the list gives).
    - PDF layout parsing: wkhtmltopdf output with six Type0/CIDFontType2 fonts
      and ToUnicode maps (the existing `_pdf_tounicode` path), two columns of
      items, points to be matched to items by position.
    - The form's pest title "34. No Evidence of Insect contamination,
      rodent/other animals" is exactly the wording that trips the text-F regex:
      ship it under the neutral heading the Hedgerow fetchers use, or register
      it.
    - No coordinates.
    - A form change (47 → 56 items) would change the layout.
    - No `robots.txt` (404). No CAPTCHA seen.
12. **Verdict.** HARD — new fetcher: Mesquite-style "read every facility's
    list each run" plus a new PDF layout parser on the existing stdlib PDF
    helpers; tiers are source-stated (McKinney-style item points).

---

## Comal County / New Braunfels

**Verdict: BUILD** for the City of New Braunfels; nothing for the rest of the
county (DSHS inspects it).

1. **Agency, population, URL.** City of New Braunfels, Neighborhood Services,
   Health and Food Safety. New Braunfels 90,441 (2020) / 116,477 (2024); Comal
   County 201,628. City page <https://www.newbraunfels.gov/3438/Food-Safety> →
   map
   <https://newbraunfels.maps.arcgis.com/apps/webappviewer/index.html?id=0a11716ac5414162899085011caf0a8b>.
   Outside the city: Comal County Environmental Health is on-site sewage only
   (verified, no food program on its page) and the DSHS Region 8 page assigns
   Comal to a state sanitarian "where local inspectors do not" (verified).
2. **Platform.** City-hosted ArcGIS Server 10.91 MapServer behind an ArcGIS
   Web AppBuilder app (web map `fa7926df1698484696708ebb7910698b`).
3. **Endpoint.**
   `https://gismaps.newbraunfels.gov/arcserverwa22/rest/services/MapServices/RestaurantMap/MapServer/0/query`
   (capabilities `Map,Query,Data`, `maxRecordCount` 2000, pagination, JSON /
   geoJSON / PBF). Fields: `BUS_NAME`, `LOCATION`, `CASE_NUMBER`,
   `InspectionPurpose`, `Inspection_Score` (int), `Inspection_Result`,
   `FoodEstType`, `Is_Latest_Insp`, `PublicLoad_dtt`, `FEATURE_ASSET_ID`,
   `Inspection_Date`. The city's map applies
   `FoodEstType <> 'Central Preparation Facility'`.
   ```json
   {"OBJECTID":522,"BUS_NAME":"Elevenly McDonalds 39483","LOCATION":"304 FM 306,  78130",
    "CASE_NUMBER":"RES2022-0110","InspectionPurpose":"Routine","Inspection_Score":98,
    "Inspection_Result":"Inspection Passed","FoodEstType":"Restaurant","Is_Latest_Insp":1,
    "PublicLoad_dtt":1795208100000,"FEATURE_ASSET_ID":"91039204","Inspection_Date":1795046400000}
   geometry (outSR=4326) {"x":-98.08162097259076,"y":29.732547018013577}
   ```
4. **Date filter, paging, caps.** Date filter works:
   `where=Inspection_Date >= DATE '2026-11-09' AND Inspection_Date < DATE '2026-11-16'`
   → 8; the 10-05..10-11 window → 19. All 758 rows fit in one response.
5. **Score.** Official 0-100, higher is better (the map's own text: "100
   being a perfect score … less than 70 will automatically receive a
   reinspection within 10 business days"). Observed 70-100 plus one 6 (surely a
   typo); 139 at 100; 24 rows have no score. `Inspection_Result` is "Inspection
   Passed" (732) or "Not Available" (26). No letter grade.
6. **Violations.** None published.
7. **Coordinates, address.** Point geometry on all rows. `LOCATION` is the
   street, sometimes with a suite or zip ("304 FM 306,  78130"); no city field
   (all New Braunfels).
8. **Ids.** `CASE_NUMBER` is the permit case (`RES2022-0110`; 754 distinct in
   758 rows); `FEATURE_ASSET_ID` is the address id. No inspection id (case +
   date).
9. **Volume, history, freshness.** **Latest inspection only**:
   `Is_Latest_Insp` = 1 on all 758 rows (the map says so too). Latest-visit
   dates: 2026: 530, 2025: 142, 2024: 62, 2023: 1; 23 rows have no date. Rows
   whose latest visit fell in a week: 10-05: 19, 10-12: 23, 10-19: 22, 10-26:
   16, 11-02: 6, 11-09: 8, 11-16: 10. Newest inspection **2026-11-19**; the
   whole table was reloaded 2026-11-20 20:55 UTC (`PublicLoad_dtt`, identical
   on every row).
10. **Requests.** 1 a week. There is no backfill beyond the snapshot: history
    accrues only by polling.
11. **Risks.** A re-inspection replaces the earlier visit, so a skipped week
    can lose an inspection for good (the Williamson "snapshot re-read" and the
    Orange County sidecar are the nearest precedents; here the cheap answer is
    to read the whole layer every run). `Inspection_Date` is a date at
    00:00 UTC: use the UTC date, do not shift it. Score outliers (6) and
    null-score "Not Available" rows must be dropped. City-run server (10.91):
    outages are the city's.
12. **Verdict.** BUILD — ArcGIS layer snapshot, scores only (Williamson
    County's shape with McKinney's request code). About 116k people for a
    very small fetcher.

---

## Hays County / San Marcos (re-check of the 2026-11-02 finding)

**Verdict: BLOCKED (login) for San Marcos; nothing published for Hays County.**
The earlier "nothing published" needs one correction: San Marcos does have a
scores report, but only behind an account.

1. **Agencies, population.** City of San Marcos Environmental Health & Food
   Safety (inside the city; 74,316 in 2024) and Hays County Development
   Services (unincorporated county plus Buda, Kyle and Wimberley; county
   292,029, Kyle 65,833, Buda 16,090).
2. **San Marcos.** <https://www.sanmarcostx.gov/1311/Reports> (200, verified)
   lists "Fixed Food Establishment Score Reports - Displays Inspection Scores
   for Fixed Food Establishments in San Marcos", reached through "MGO Connect
   Advanced Reporting",
   `https://www.mgoconnect.org/cp/custom-reports?JID=43`, with the instruction
   "Login to your MGO account, or create a new account". The MGO Connect SPA
   loads its report list from
   `https://api.mgoconnect.org/…/jpv2/reports/{UserID}` — keyed to the
   signed-in user (read in `main.js`). **That is a login wall; I did not create
   an account or call the API.** Platform: MyGovernmentOnline (MGO Connect).
3. **Hays County.** `https://www.hayscountytx.gov/food-establishments` still
   answers **403 "Attention Required! | Cloudflare"** to this container (the
   site's WAF, not the proxy), as on 2026-11-02. The search-index copy of the
   page (**unverified** first-hand) describes permits only — no scores, no
   portal — and says permits are applied for online (MGO as well).
4. **Everything else (endpoints, score, violations, volume).** Not reachable
   without the login. Whether the MGO report carries violations is unknown.
5. **Verdict.** BLOCKED. The route forward is a human one: ask San Marcos
   ([email removed], [phone removed]) whether the score report can be
   published without a login or sent as an extract; ask Hays County Development
   Services ([email removed]) the same. Do not automate an MGO
   account.

---

## Guadalupe County / Seguin

**Verdict: BUILD** for the City of Seguin (scores only); the rest of the county
has nothing usable.

1. **Agency, population, URL.** City of Seguin, Planning & Codes,
   Environmental Health. Seguin 29,447 (2020) / 38,789 (2024); Guadalupe
   County 195,166. Page <https://www.seguintexas.gov/468/Environmental-Health>
   → app
   <https://seguin-utility.maps.arcgis.com/apps/instant/sidebar/index.html?appid=51fd9eb251574a7992fabb560da849df>.
   Elsewhere in the county: Cibolo (37,816) posts a hand-kept HTML list of
   letter grades with no dates (verified: 68 "Grade A", 2 "Coming Soon" —
   useless); Schertz (44,428) publishes nothing I could find (search only,
   **unverified**); the unincorporated county is DSHS (Region 8 page, verified).
2. **Platform.** City-hosted ArcGIS Server 11.5 Feature Service, fed from the
   city's Tyler EnerGov system (each row links to the EnerGov Citizen Self
   Service inspection page).
3. **Endpoints.**
   - `https://gis.seguintexas.gov/arcgis/rest/services/Health_Inspections/Health_Inspections/FeatureServer/0/query`
     (capabilities `Query,Extract`, `maxRecordCount` 2000, pagination). Fields:
     `InspectionNumber`, `InspectionDate`, `InspectionStatus`, `Address`,
     `Link`, `CompanyName`, `InspectionScore` (int), `InspectionType`.
     ```json
     {"OBJECTID":649,"InspectionNumber":"6MInsp-181290-2026","InspectionDate":1795195140000,
      "InspectionStatus":"Passed","Address":"1339 EASTWOOD DR",
      "Link":"https://eportal.seguintexas.gov/EnerGov_Prod/SelfService/#/inspectionDetail/inspection/5a2e5460-93bb-48c0-8f23-7cf59928e1a3",
      "CompanyName":"RIVER BEND OPCO, LLC","InspectionScore":96,"InspectionType":"6-Month Inspection"}
     geometry (outSR=4326) {"x":-97.94547900005263,"y":29.563280999938044}
     ```
   - Optional, anonymous (returns `"IsLoggedIn":false`):
     `GET https://eportal.seguintexas.gov/energov_prod/selfservice/api/energov/inspections/getById/{guid}`
     with headers `tenantId: 1`, `tenantName: City of Seguin`,
     `Tyler-TenantUrl: Home`, `Tyler-Tenant-Culture: en-US` → `ActualDate`,
     `ScheduledDate`, `LinkNumber` (the business licence, e.g.
     `FE000400-02-2025`), `MainAddress`, `IsReinspection`, `Comment` (empty on
     all five I read).
   - `POST …/api/energov/entity/checklist/search` (the inspection's checklist)
     answers `412 "You must be a contact on this record to see this
     information"` — **login-gated; stopped there.**
4. **Date filter, paging, caps.** Date filter works:
   `where=InspectionDate >= DATE '2026-11-09' AND InspectionDate < DATE '2026-11-16'`
   → 10; the 10-05..10-11 window → 2. All 1,887 rows fit in one response.
5. **Score.** Official 0-100, higher is better (the city's older monthly PDFs
   say each inspection "starts with a perfect score of 100"). Observed 67-100
   (384 at 100) plus 111 rows with 0, which are unscored visits (87 "Passed",
   23 "Re-Inspection Required", 1 "Requested"): drop them. `InspectionStatus`:
   Passed 1,663, Re-Inspection Required 223, Requested 1. Types: "Health
   Inspection" 1,194, "6-Month Inspection" 693. No letter grade.
6. **Violations.** Not available without a login (see 3). The city's 2023
   monthly PDFs carried one-line findings, but that series stopped (search
   index, **unverified**).
7. **Coordinates, address.** Point geometry on 1,882 of 1,887 rows. `Address`
   is the street only (all Seguin).
8. **Ids.** `InspectionNumber` (`HInsp-181726-2026`, 1,887 distinct) and the
   inspection GUID in `Link`; no facility id on the layer (383 distinct
   company + address) — the licence number comes from `getById`.
9. **Volume, history, freshness.** 1,887 inspections, September 2022 onward
   (one April 2022 row), 25-54 a month; recent weeks 10-19: 16, 10-26: 6,
   11-02: 13, 11-09: 10, 11-16: 9. Newest completed row **2026-11-20**; one
   "Requested" row is dated 2026-12-09.
10. **Requests.** Weekly: 1 (plus ~10 `getById` calls if the true date and
    licence number are wanted). 90 days: 1 (+~130). All: 1 (+1,887).
11. **Risks.**
    - **`InspectionDate` is the SCHEDULED date, not the inspection date.** Of
      five rows checked against `getById`, four matched the actual day and one
      was ten days late (layer 2026-11-20, `ActualDate` 2026-11-10); a
      future-dated "Requested" row proves the point. Either read `ActualDate`
      per new inspection or accept a small date error and drop non-final
      statuses and future dates.
    - These timestamps are real UTC (hours 13-23, and 06:00 for date-only
      rows): convert to Central before taking the date. This is the opposite of
      Waco's layer.
    - `CompanyName` is the licence holder's legal name ("CST SERVICES, LLC",
      "RIVER BEND OPCO, LLC"), not always the storefront name.
    - Scores only; small city.
12. **Verdict.** BUILD — ArcGIS Feature Service query, scores only
    (Williamson-like), optionally one anonymous EnerGov call per new
    inspection for the actual date and licence number.

---

## Longview / Gregg County

**Verdict: HARD and low value.** Letter grades only, in PDFs.

1. **Agency, population, URL.** City of Longview Environmental Health.
   Longview 81,765 (2020) / 83,668 (2024); Gregg County 126,679. Page
   <https://www.longviewtexas.gov/2852/Food-Service>. Kilgore (13,583) runs its
   own program and publishes nothing (search only, **unverified**).
2. **Platform.** Crystal Reports PDFs ("Powered By Crystal") stored at fixed
   document ids in the city's Laserfiche WebLink archive (behind Cloudflare,
   which served curl normally).
3. **Endpoints.**
   - Weekly, by date:
     `https://archive.longviewtexas.gov/WebLink/ElectronicFile.aspx?docid=1163114&repo=CityofLongview&dbid=0`
     (200, 59 KB, one page).
   - Monthly:
     `https://archive.longviewtexas.gov/WebLink/ElectronicFile.aspx?docid=1244447&repo=CityofLongview&dbid=0`
     (200, 96 KB, 13 pages).
   - Weekly, alphabetical (`docid=1529568`) returned a Laserfiche error page
     today ("The system has encountered an error…").
   ```
   Weekly Food Service Inspection Report  Sorted By Date   11/5/2026 to 11/13/2026
   ESTABLISHMENT          ADDRESS            SCORE   INSPECTION DATE
   USA Donut              1006 PINE TREE RD    B        11/12/2026
   ```
   The monthly file adds permit type, inspection type (Quarterly / Biannual /
   Annual / Initial), the full address with zip, and the last TWO inspections
   of each establishment.
4. **Date filter, paging.** None. The same URL is overwritten each week.
5. **Score.** **Letter grade only**: A, B, C, D (monthly file: A 279, B 115,
   C 13, D 3; weekly: 20 A, 1 B). No numeric score anywhere, and the city's
   page does not publish the letter-to-points scale.
6. **Violations.** None.
7. **Coordinates, address.** None; street (weekly) or street + zip (monthly).
8. **Ids.** None.
9. **Volume, history, freshness.** 21 inspections in the 11/5-11/13 report,
   newest **2026-11-12**. The monthly file covers 221 establishments (410
   inspection rows, 214 dated 2026). No archive of past weekly files.
10. **Requests.** 2 a week. No backfill.
11. **Risks.** A grade is not a score: it would have to be mapped to fixed
    numbers (as Orange County's Pass / Reinspection / Closed are), with no
    violations to refine it. A missed week is partly recoverable from the
    monthly file only. One of the three documents was erroring today.
12. **Verdict.** HARD (PDF text parser) for letter grades only. Recommend
    skipping unless grade-only cities are wanted.

---

## Texarkana / Bowie County

**Verdict: NOTHING ONLINE.**

- City of Texarkana, Texas (36,243 in 2020 / 35,971 in 2024), Inspections and
  Code Enforcement: <https://www.texarkanatexas.gov/549/Health-Inspections>
  (200, verified) says establishments are inspected at least once a year and
  offers only the blank inspection form (PDF) and a phone number
  [phone removed]). Its "MyGov TXK" portal (`public.mygov.us/tx_texarkana`,
  verified) covers permits, code cases and requests — no health module.
- Bowie County (92,892 / 91,992) Health Inspections Department:
  <https://www.bowiecounty.org/1227/Health-Inspections-Department> (200,
  verified) lists a permit application and fees only.
- No score list, portal, PDF series or dataset turned up in search either.
  (The Texarkana-Bowie County Family Health Center is the clinical health
  department; food inspection sits with the city and the county.)

---

## Nacogdoches

**Verdict: BUILD** — one CSV, but small and hand-kept.

1. **Agency, population, URL.** City of Nacogdoches Inspection Services
   (one health inspector). City 32,153 (2020) / 32,381 (2024); the county
   (65,728) outside the city was not found to publish anything. Page:
   <https://www.nactx.us/954/View-Restaurant-Health-Scores>.
2. **Platform.** A Google Sheet ("INSPECTION SCORES FOR WEB") published to the
   web and embedded in an iframe.
3. **Endpoint.**
   `https://docs.google.com/spreadsheets/d/e/2PACX-1vSpwqkwJSfozkQhfA9IjvY9qyzxLC5yhGRv33TCxN3uEglmz_dW6EymB55ZBGc7W112Lkx0etcCG1Lq/pub?output=csv`
   (200 after a redirect to `googleusercontent.com`, `text/csv`, 15 KB,
   `Cache-Control: max-age=300`). Five columns; the header row is literally
   `3/22/2022,Address,Type of Service,Demerits,Column 1` (name, address, type,
   demerits, date).
   ```
   North Food Mart,5105 North Street,Food Service/Sales,18,11/12/2026
   ```
4. **Date filter, paging.** None; the whole sheet (247 rows, 239 with data)
   in one response, newest first with a few stragglers at the bottom.
5. **Score.** **Demerits, lower is better**: 0-21 observed (38 zeros; two
   rows blank). No letter grade, no pass/fail.
6. **Violations.** None in the sheet. (A local news site republishes the
   inspector's weekly report with finding text — third party, **unverified**,
   not a source to build on.)
7. **Coordinates, address.** No coordinates; street only, no city or zip.
8. **Ids.** None. 238 distinct name + address in 239 rows.
9. **Volume, history, freshness.** One row per establishment — its latest
   visit. 2026-06 through 2026-11 hold 208 of the rows (29-43 a month); recent
   weeks 10-12: 14, 10-19: 11, 10-26: 3, 11-02: 13, 11-09: 9. Newest
   **2026-11-19**.
10. **Requests.** 1 a week. No backfill.
11. **Risks.** Hand-typed ("Food Serrvice", a date in the header cell), so
    column order or date format can change without notice; a row is replaced
    when the place is re-inspected (poll weekly or lose it); types include
    "Food Truck" (22) and "Roadside Vendor" (10) to drop; no ids; needs
    geocoding. The publish link can be withdrawn at any time.
12. **Verdict.** BUILD — a CSV snapshot like Williamson County's, shipped as
    `100 − demerits`, scores only. Cheap, but about 32k people.

---

## Texas DSHS state program

**Verdict: NOTHING ONLINE.** DSHS does not publish retail-food inspection
results for the areas it inspects itself.

- DSHS "permits and inspects retail food facilities in areas of the state
  where no local health authority regulates" (Region 8 retail food page,
  verified). In this batch that means Comal County outside New Braunfels and
  Guadalupe County outside its cities, among others.
- <https://www.dshs.texas.gov/retail-food-establishments> and its FAQ (both
  200, verified) link to: the licensing portal
  (`vo.ras.dshs.state.tx.us/datamart`, a cookie-gated licence search and
  renewal site — licence status, not inspections), the jurisdiction map
  (ArcGIS web app `52506ccc6cfb4722b2d65860fa1387ab`: who inspects where, no
  results), and an "Open Records" page: inspection reports are obtained by a
  written Public Information Act request to `[email removed]`.
- The only enforcement data online is a PDF,
  `https://www.dshs.texas.gov/sites/default/files/foodestablishments/pdf/PDF/DiscActions/retailfooddisciplinaryactions.pdf`
  (200, 327 KB, 50 pages, last modified 2026-11-13): compliance actions since
  2021 — name, licence number, city, rule cited, action, date. It is a few
  dozen orders a year (the FY26 analysis PDF counts 72 orders and agreements),
  not inspections.
- The Texas open-data portal has no DSHS retail-food dataset: the Socrata
  catalog for `data.texas.gov` returned 0 results for "food establishment
  inspection" and "DSHS inspection", and 2 unrelated hits for "retail food".
- The state inspection form (56 items, In / Out / NO / NA) carries no score
  (a third-party guide says so; I did not read the marking instructions
  myself — **unverified**), so even an extract would need a computed score.
- If DSHS coverage is ever wanted, the only route is a records request for a
  periodic extract.

---

## Appendix: what was fetched

Request counts are approximate (counted from my command log, not a proxy log).

| Host | Requests | Outcome |
|---|---|---|
| wfeh-portal.hedgerowsoftware.com | 12 | 200, except three guessed paths (404, 401, 404) |
| services2.arcgis.com, www.arcgis.com (Waco) | 14 | all 200 |
| www.waco-texas.com / www.wacotx.gov | 2 | 403 Akamai "Access Denied" (site) |
| gismaps.newbraunfels.gov, www.newbraunfels.gov | 7 | all 200 |
| gis.seguintexas.gov, eportal.seguintexas.gov, seguin-utility.maps.arcgis.com | 20 | all 200; checklist search answers 412 (login) |
| brazos.inspect2go33.com | 30 | all 200 (robots.txt 404) |
| health.brazoscountytx.gov | 1 | 403 JS bot challenge (site) |
| www.bellcountyhealth.org → cms3.revize.com | 7 | all 200 |
| www.longviewtexas.gov, archive.longviewtexas.gov | 5 | 200; one document returns an error page |
| www.nactx.us, docs.google.com | 4 | all 200 |
| www.texarkanatexas.gov, www.bowiecounty.org, public.mygov.us | 3 | all 200 |
| www.hayscountytx.gov | 1 | 403 Cloudflare WAF (site) |
| www.sanmarcostx.gov, www.mgoconnect.org | 3 | all 200 (static pages and bundle only) |
| www.dshs.texas.gov, vo.ras.dshs.state.tx.us, api.us.socrata.com | 12 | 200 (one guessed URL 404) |
| www.comalcounty.gov, www.cibolotx.gov | 2 | all 200 |
| www2.census.gov | 2 | 200 (api.census.gov wants a key: not used) |
