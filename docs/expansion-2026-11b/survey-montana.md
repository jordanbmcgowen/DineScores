# Montana county-run inspection systems: live survey (2026-11-22)

Scope: the Montana counties whose rows in the state MyHealthDepartment portal are empty
placeholders (Missoula, Gallatin, Flathead, Ravalli, Carbon, Fergus). Everything below
was fetched live on 2026-11-22 with curl (Chrome UA, about one request a second) unless
it is marked **unverified**. Raw responses are under
`(raw captures, not kept in the repo)`
(file names are given per finding).

## Headline

- **Missoula, Gallatin and Flathead all publish on one vendor platform: Paragon
  "Inspections Online"** (`inspectionsonline.us`, IBM/HCL Lotus Domino, one `inspect.nsf`
  database per county). This is the "Paragon" the state placeholder comment refers to.
  One config-driven fetcher covers all three (~355k people).
- The platform answers a **server-side inspection-date filter** (Domino full-text query
  on `[fld_InspectionDate]`), gives **full violation text with a two-level tier**
  (Risk Factor/Intervention vs. other), split address fields, and stable 32-hex ids.
  No login, no CAPTCHA, no bot challenge, no robots.txt.
- **Flathead publishes an official letter grade** (A+ to F, Board of Health policy
  2010-01 EH, demerit bands). Missoula and Gallatin publish no score; a grade/score
  column is in their HTML but the county has it switched off (hidden), so it is not
  official.
- **Ravalli, Carbon and Fergus publish nothing online** beyond the state portal's
  placeholder rows.
- Not asked for, found on the way: **Kansas City, MO runs on the same platform** and the
  same date query answered (82 inspections for 2026-11-16..17). CLAUDE.md lists Kansas
  City as "no machine-readable source". See "Same platform elsewhere".

Not verified by this survey: reachability from a GitHub-hosted runner (all requests went
through this session's egress proxy, which also re-terminates TLS, so the origin's
certificate chain was not checked independently).

---

## Paragon "Inspections Online" platform (shared by Missoula, Gallatin, Flathead)

Host `https://inspectionsonline.us` (also answers on `www.`). `Server: Lotus-Domino`.
`/robots.txt` is 404. The root page is a 2005 FrontPage document by Paragon
(`[email removed]`). Not one of the families the pipeline supports today.

Database path per county (`<db>`):

| County | `<db>` | Public wrapper page |
|---|---|---|
| Missoula | `mt/missoulamissoula/inspect.nsf` | `/foodsafety/mtmissoulamissoula/search.htm` |
| Gallatin | `mt/gallatinbozeman/inspect.nsf` | `/foodsafety/mtgallatinbozeman/search.htm` |
| Flathead | `mt/flatheadkalispell/inspect.nsf` | `/foodsafety/mtflatheadkalispell/search.htm` |

### Endpoints

All are anonymous GETs. E1-E5 are what the public pages themselves call (read from
`jsh_Search` and `jsh_PublicSearchSummaryNT`, saved as `missoula_jsh_*.js`). E6-E7 are
standard Domino URL commands on the same public views; the UI does not issue them, the
server answers them.

**E1 search form** `GET /<db>/Search?OpenForm` (HTML; hidden `fld_Program` empty, which
the JS turns into `kw_Food`).

**E2 facility search (the UI's own)**
```
GET /<db>/SearchEstabNT?SearchView&Query=[fld_Program]+CONTAINS+kw_Food+AND+([fld_EstabName]+CONTAINS+pizza+OR+[fld_FaciName]+CONTAINS+pizza)&SearchOrder=4&SearchWV=TRUE&SearchFuzzy=TRUE
```
200, HTML. `<h4>23 documents found</h4>` then one `<TABLE>` per facility:
`<A HREF='/<db>/vw_InspectionsPubSumm-NT?OpenView&RestrictToCategory=<FACILITY_UNID>'>NAME - FACILITY NAME</A>` and
`STREET, CITY ZIP`. Sample (Missoula): `BIGA PIZZA | 241 W MAIN ST, MISSOULA 59802 | 9EB9E5F1EE7416ED862574D7005CBE90`.

**E3 facility history** `GET /<db>/vw_InspectionsPubSumm-NT?OpenView&RestrictToCategory=<FACILITY_UNID>`
200, HTML, one `<DIV class='gt row'>` per inspection, newest first:
type (link to E5 with `pUNID=<INSPECTION_UNID>`), date `MM/DD/YYYY`, `N RFI` (older rows
`N critical`), and a fourth column `scoreCol` that is rendered `display:none` unless the
page's hidden input `xx_SrchScorDspOpt` is `kw_Yes`.

**E4 field values** (plain text, `~`-separated, leading `~`, multi-values `|`, keywords
prefixed `kw_`, names space-padded, `Content-Type: text/plain; charset=iso-8859-1`)
```
GET /<db>/(ag_getDocValues)?OpenAgent&xx_UNID=<INSPECTION_UNID>&xx_fldList=fld_EstabName~fld_FaciName~fld_FStreetNo~fld_FStreetName~fld_FCity~fld_FProv~fld_FPCode~fld_EstType~fld_SCPhone~fld_InspectionDate~fld_AllcvCounter~fld_AllvCounter~fld_EstTypeAlt~fld_RFvCountsUseVLib~fld_QRFICounts
```
That is exactly the list the public page sends. `fld_Program` (the field the search JS
filters on) also answers: `...&xx_fldList=fld_EstabName~fld_FaciName~fld_Program~fld_EstType`
returned `~Firebrand Lounge, The~Food Service~kw_Food~kw_1. Food Service Establishment`.
The agent takes any caller-supplied field list; I did not probe field names the public
pages do not use (so: no test for coordinate or licence-number fields).

**E5 inspection report** `GET /<db>/(ag_dspPubDetail)?OpenAgent&pUNID=<INSPECTION_UNID>`
200, HTML (UTF-8). Header table: `NAME - Inspection Report`, `Address:` (one string:
`241 W MAIN ST MISSOULA MT 59802`, city not separable, use E4 for split fields),
`Establishment Type:`, `Phone No.:`, `Inspection Date` (`13 March, 2026`),
`Inspection Type`, `Risk Factor/Intervention (RFI) Violations` (count). Then one
`<TR onMouseOver=...>` per violation:
```
<TD ALIGN=center CLASS=bS>3-501.18</TD><TD CLASS=bS><TABLE ...>
 <TR><TD COLSPAN=2><I>{violation title}</I></TD></TR>
 <TR><TD><B>RISK FACTOR/INTERVENTION</B></TD><TD><B>Corrected On Site</B></TD></TR>   <- <B></B> when not RFI; 2nd TD absent when not COS
 <TR><TD COLSPAN=2><B>RECOMMENDED RESOLUTION:</B> {code text}</TD></TR>
 <TR><TD COLSPAN=2><B>OBSERVATIONS & CORRECTIVE ACTIONS:</B> {inspector's text}</TD></TR>
</TABLE></TD></TR>
```
A clean inspection prints `Violation Details` / `No Violations` (`missoula_detail0.htm`).
Integrity check available: violation rows == E4 `fld_AllvCounter`, RFI rows ==
`fld_AllcvCounter` (verified: Gallatin sample 7 rows / 5 RFI vs. `5~7`; Flathead sample
9 rows / 4 RFI vs. `4~9`). A fetch that parses fewer rows than the counter is unread,
not clean.

**E6 date-filtered inspection list (the weekly entry point)**
```
GET /<db>/vw_InspectionsPubSumm-NT?SearchView&SearchOrder=4&SearchMax=0&Query=[fld_Program]+CONTAINS+kw_Food+AND+[fld_InspectionDate]%3E=11/08/2026+AND+[fld_InspectionDate]%3C=11/14/2026
```
200, HTML: `<h4>12 documents found</h4><hr>REF` followed by the same row DIVs as E3
(inspection UNID, type, date, `N RFI`, score column). The rows carry **no facility name
or id**: follow each UNID to E4 (+E5). Brackets were sent raw, `>` `<` percent-encoded.

**E7 structured view dump** `GET /<db>/vw_InspectionsPubSumm-NT?ReadViewEntries&ExpandView&Count=1000&OutputFormat=JSON`
200, JSON (or XML without `OutputFormat`): `@toplevelentries` (facility categories),
`viewentry[]` with category entries (`entrydata[0].text` = facility UNID, `@children`)
followed by their inspection entries (`@unid`, column HTML as in E3). 1,000 entries per
page, ~1 MB (Flathead: 5,755 categories, first page covered 133, so ~43 pages for
everything). `SearchEstabNT?ReadViewEntries&OutputFormat=JSON` lists facilities the same
way (Missoula `@toplevelentries` 1,362, all programs). This is the fallback if E6 ever
stops answering, and the only public inspection-to-facility-UNID map.

### Date filter, paging, caps (measured)

- Two windows, same query, all programs (files `<county>_ftA.htm`, `_ftB.htm`):

  | County | 11/01-11/07 | 11/08-11/14 |
  |---|---|---|
  | Missoula | 12 docs, dates 11/02-11/05 | 13 docs, dates 11/10-11/13 |
  | Gallatin | 33 docs, dates 11/02-11/06 | 19 docs, dates 11/10-11/13 |
  | Flathead | 40 docs, dates 11/02-11/06 | 34 docs, dates 11/09-11/13 |

- Bounds are inclusive: Missoula `>=11/12/2026 AND <=11/12/2026` returned 7, the same 7
  the week listing shows for that day.
- **Cap: 1,000.** A query matching more reports `N documents returned; 1000 found`
  (all-time food query on each county). Year-sized windows stayed under it
  (Missoula 2025: 558). Treat a header of exactly 1000 as "split the window".
- **Do not page.** `&Start=6&Count=5` on a 13-row result returned rows 3, 5, 7, 8, 9 of
  the unpaged order, not 6-10. Fetch each window whole (no `Count`) and check that the
  header count equals the rows parsed. `SearchMax=0` is needed (the form default is
  lower; 250 is the usual Domino default, not measured here).
- `[fld_Program] CONTAINS kw_Food` narrows to the food program but also drops wholesale
  manufacturers that Flathead counts as food: for 10/08-10/14 the unfiltered query held
  17 graded rows, exactly the 17 rows and grade mix of the county's own weekly PDF
  (A+ 13, A 2, A- 1, C- 1; `flathead_weekly_2026-10-16.txt` vs. `flathead_ft_oct.htm`),
  while the filtered query held 16. For a restaurant map the filter is the right default;
  E4's `fld_EstType` / `fld_Program` allow finer control.
- Posting lag: a Domino UNID embeds the document's creation time (second half: Julian
  day and centiseconds, decoded offline from the saved lists). Created minus inspected
  over the last 90 days: Missoula 95 of 99 same day, 4 next day; Gallatin 188 of 198
  same day, 8 next day, one each at 9 and 29 days; Flathead 330 of 343 same day, 11 next
  day, one at 4 days. Creation is not web publication (not measurable), but the newest
  inspection visible in all three is 2026-11-20, the last working day before the survey.
  A 14-day margin in front of every window (as `MCESD_POSTING_LAG_DAYS` does) plus
  `--skip-known-inspections` would cover the late ones for one list request.

### Behaviour under the survey's load

About 63 requests to the host in ~12 minutes across four databases, all 200 except three
deliberate path probes (404). No throttling, no challenge, no cookies required. Latency
0.2-1 s for small pages, 2-6.5 s for the first 90-day / year-sized query on a database.

### How it would fit the pipeline

- New small family ("Paragon Inspections Online"), config = `<db>` + city/county label +
  score policy. Shape is the Tyler ColdFusion / Louisiana pattern: date-window list, then
  one or two small requests per inspection, HTML parsed with stdlib regex.
- Scoring for Missoula and Gallatin: no official score; two source-stated tiers, so the
  Louisiana rule (`100 - 5 per Critical - 1 per Non-Critical`, RFI = Critical) applies
  and the source belongs in `SOURCE_TIER_SCORE_SOURCES` / `SOURCE_TIER_SOURCES`.
  Flathead has an official grade (below).
- No coordinates: Census batch geocoding from the split address fields.
- Violation line for the text rules / Jev: title + observation. Titles here are
  violation statements ("Equipment food-contact surfaces not clean."), not neutral form
  headings; the `RECOMMENDED RESOLUTION` paragraph is boilerplate code text and should
  not be sent to the pest/sewage regex or Jev.
- Inspection types seen: Routine, Follow-Up, Pre-Operational, Complaint / Complaint
  Inspection, Ownership Change, Ad-Hoc Routine, Site Visit, Remodel Inspection,
  Compliance, Closeout, Special, Investigation, Other. Pre-Operational is 5-15% of the
  food rows (Flathead 16 of 343, Gallatin 30 of 198, Missoula 77 of 536; Flathead grades
  them `N/A`); whether to ship them is a policy choice.
- Overlap with the state portal: these counties' restaurants also exist there as
  placeholder rows. If the Montana MHD fetcher ships those placeholders, the same
  restaurant can appear twice under two spellings (state licence name vs. Paragon name).
  Decide which source owns Missoula, Gallatin and Flathead counties and drop the other's
  rows there.
- Platform risk: this is a legacy system (Domino, "for best results use Internet
  Explorer"). Lewis & Clark County's Paragon database is still up but returned 1 food
  inspection for 11/08-11/21, i.e. Helena has effectively moved to the state system. If
  Missoula, Gallatin or Flathead follow, the Paragon feed goes quiet and the state portal
  starts carrying their real inspections; `_record_group` freshness would show it as
  STALE. Register one fetch group per county so that is visible per county.

---

## Missoula Public Health (Missoula City-County Health Department)

1. **Agency / population / URL.** Missoula Public Health, Environmental Health, 301 W
   Alder St, Missoula. County population ~120k. Public page
   `https://www.missoulapublichealth.org/inspections-permits/food-service` links
   "Restaurant Inspections" to
   `https://www.inspectionsonline.us/foodsafety/mtmissoulamissoula/search.htm` (verified
   in the page HTML). The county's other page (`missoulacounty.gov/.../health-department-permits-and-inspections`)
   only mentions NextRequest (search excerpt, not fetched).
2. **Platform.** Paragon Inspections Online, `mt/missoulamissoula/inspect.nsf`.
3. **Endpoints.** E1-E7 above. Real sample (inspection `C606764EDE49163887258DB900649390`):
   - E3 row: `Routine | 03/13/2026 | 2 RFI | hidden score cell "A"`
   - E4: `~BIGA PIZZA~BIGA PIZZA~241~W MAIN ST~MISSOULA~kw_MT~59802~kw_10. Food Service/Catering (Retail)~406 728 2579~03/13/2026~2~2~~kw_Yes~0|0|...|1|...` (names space-padded)
   - E5 violation 1: `3-501.18 | Ready-to eat time/temperature control for safety food not discarded when required. | RISK FACTOR/INTERVENTION | OBSERVATIONS: Today cooked mushrooms are being kept beyond their discard date. Ensure all TCS items are marked with a date label ...`
   - E5 violation 2: `4-501.114 | Chlorine solution maintained at improper concentration. | RISK FACTOR/INTERVENTION | Corrected On Site | Today a sani bucket (chlorine) in the back of house was at 0 ppm ...`
4. **Date filter / paging / caps.** Proven (table above; single-day inclusive test was
   run here). 1,000-row cap, no paging.
5. **Score.** None official. Published: RFI violation count per inspection (older
   reports: critical / non-critical counts). The history view carries a letter
   (A+ / A / B / C) in a column the county has switched off (`xx_SrchScorDspOpt=kw_No`,
   `display:none`); 2026 YTD mix A+ 269, A 148, B 101, C 7, blank 11. It is in the
   served HTML but the county chose not to show it, and it does not track the RFI count
   cleanly (1 RFI appears as both A+ and A). Recommendation: do not ship it as an
   official score; compute from the tiers.
6. **Violations.** Full text per inspection in E5: Food Code reference, violation
   title, tier (RFI or not), corrected-on-site flag, recommended resolution, inspector's
   observation. Two tiers only.
7. **Coordinates / address.** No coordinates. Split fields in E4: street number, street
   name, city, state (`kw_MT`), zip, phone. Mixed case across records (`MISSOULA` /
   `Missoula`).
8. **Ids.** Inspection: Domino UNID (32 hex) in every link. Facility: Domino UNID
   (category key of E3 / E7, link target of E2). No licence number on the public pages.
9. **Volume / history / freshness.** Food program: 99 inspections in the 90 days
   08/24-11/20/2026 (2-15 a week, mean 7.6); 558 in 2025; 536 in 2026 to date. All
   programs: 12-13 a week in November. Types in 2026: Routine 425, Pre-Operational 77,
   Follow-Up 29, Other 4, Special 1. History back to 2009 (oldest row seen 11/02/2009;
   more than 1,000 food inspections dated before 2012). Newest seen 11/20/2026.
   4,313 facility categories in the history view, 1,362 searchable facilities (all
   programs). Note: the department's site talks of ~1,000-1,500 inspections a year; the
   portal's food count is about half that. The state list reportedly showed 49 Missoula
   entries for October (third-party count, unverified) against 34 food inspections here,
   which is the same order, so I read this as the county's real volume rather than a
   partial feed, but it is not proven.
10. **Requests.** Weekly (14-day window, food filter): 1 list + 2 per new inspection,
    about 20-35. 90-day backfill: 3 monthly lists + 2 x 99, about 200. Full history
    (~9-10k inspections, optional): ~20k requests in yearly-or-smaller windows.
11. **Blockers.** None met. No robots.txt; the history view carries
    `<meta name="robots" content="noindex">` (an indexing hint). Legacy platform.
12. **Verdict: BUILD.** New "Paragon Inspections Online" fetcher (shared with Gallatin
    and Flathead); list+detail HTML like the Tyler ColdFusion portals, scored like
    Louisiana (two source-stated tiers).

## Gallatin City-County Health Department ("Healthy Gallatin")

1. **Agency / population / URL.** Gallatin City-County Health Department, Environmental
   Health Services, 400 E Babcock St, Bozeman (Bozeman, Belgrade, Big Sky, West
   Yellowstone, Three Forks, Manhattan). ~125k people. Public page
   `https://www.healthygallatin.org/environmental-health/inspection-reports/restaurant-inspection-reports/`
   embeds `https://www.inspectionsonline.us/foodsafety/mtgallatinbozeman/search.htm` in
   an iframe (verified).
2. **Platform.** Paragon Inspections Online, `mt/gallatinbozeman/inspect.nsf`.
3. **Endpoints.** E1-E7. Real sample (inspection `7E6383D9EC3E344F86258E5D007026AC`):
   - list row: `Routine | 08/24/2026 | 5 RFI | hidden score cell "95.5"`
   - E4: `~Bunker Bar & Grill (The)~Bunker Bar & Grill (The)~3240~Black Otter Rd~Big Sky~kw_MT~59716~kw_FR 01 Food Service Establishment|kw_FR 02 Tavern or Bar|kw_FR 10 Food Service / Catering (On-site Retail Only)~[phone removed]~08/24/2026~5~7~~kw_Yes~0|0|0|0|0|0|1|...`
   - E5: 7 violations, 5 RFI, e.g. `3-301.11 | Direct hand contact when handling ready-to-eat foods. | RISK FACTOR/INTERVENTION | Observed one employee grab a lime and put it on the edge of the glass using bare hands ...` and a non-RFI line `5-205.11 | Handwashing sink used for purposes other than handwashing. | Corrected On Site | Observed a measuring cup sitting in the handwashing sink ...`
4. **Date filter / paging / caps.** Proven (table above). Same cap and paging caveat.
5. **Score.** None official. Published: RFI count. The switched-off column
   (`xx_SrchScorDspOpt=kw_No`) holds numbers (`100.0`, `95.5`, `90.9`, `0.0` on
   2015-2019 rows, blank on older ones) that are a vendor artifact: an inspection with
   3 RFI shows 100.0. Do not use.
6. **Violations.** As Missoula: full text, code reference, RFI / non-RFI, corrected on
   site.
7. **Coordinates / address.** None in the portal. Split address in E4. The county's
   "EHS Interactive Mapper" (ArcGIS Experience `6c5d3097540f4895be52c11b0bda0731`) is
   not readable anonymously through the ArcGIS sharing API (`CONT_0001 Item does not
   exist or is inaccessible`), so no coordinate source from there.
8. **Ids.** Inspection UNID, facility UNID (e.g. `0057246FBE816D5B872579F8005F110C`).
   Establishment type is multi-valued (`FR 01 ...|FR 02 ...`).
9. **Volume / history / freshness.** Food program: 198 inspections in the 90 days to
   11/20/2026 (10-27 a week, mean 15): Routine 137, Pre-Operational 30, Ownership
   Change 9, Complaint Inspection 9, Follow-Up 4, Other 4, Site Visit 2, Remodel 2,
   Investigation 1. All programs: 19-33 a week in November. History from 2012 (0 food
   inspections dated before 2012; a facility's oldest row 05/24/2012; more than 1,000
   before 2014). Newest seen 11/20/2026.
10. **Requests.** Weekly: 1 list + 2 per new inspection, about 30-55. 90-day backfill:
    3 lists + 2 x 198, about 400.
11. **Blockers.** None met. A third-party site (`mtfoodreports.com`, "Gallatin Valley
    Health Inspection Aggregator", seen in search results, not fetched) already
    republishes this portal.
12. **Verdict: BUILD.** Same fetcher as Missoula, config entry only.

## Flathead City-County Health Department

1. **Agency / population / URL.** Flathead City-County Health Department, Environmental
   Health, 40 11th St W Suite 210, Kalispell (Kalispell, Whitefish, Columbia Falls,
   Bigfork, Lakeside, Somers, Hungry Horse, West Glacier). ~110k people. Public page
   `https://flatheadcounty.gov/department-directory/health/environmental-health/retail-and-wholesale-food-safety`
   links "Look Up Inspection Results" to
   `https://www.inspectionsonline.us/foodsafety/mtflatheadkalispell/search.htm` (verified).
2. **Platform.** Paragon Inspections Online, `mt/flatheadkalispell/inspect.nsf`. A
   second, weaker channel: weekly PDFs on the county site (below).
3. **Endpoints.** E1-E7. Real sample (inspection `FF87AD116E324A1E87258EAD005B4539`):
   - list row: `Routine | 11/12/2026 | 4 RFI | D+`
   - E4: `~Firebrand Lounge, The~Food Service~650~East 3rd Street~Whitefish~kw_MT~59937~kw_1. Food Service Establishment~[phone removed]~11/12/2026~4~9~~kw_Yes~0|0|0|0|0|0|1|...`
   - E5: 9 violations, 4 RFI, e.g. `3-501.16 | (A) Inadequate hot holding of PHF/TCS food. | RISK FACTOR/INTERVENTION | Observed sausage links on hot hold being double stacked ... The top row of sausage links was below 135 ...`; non-RFI `6-501.14 | (All) Ventilation systems not maintained clean ... | Ventilation grates are greasy above the grill, can drip onto food below.`
   - Facility history (E3) for the sister record "Firebrand Lounge, The - Bar/Espresso"
     (`B9D9E604EC70EAB08725882C0052DFB1`): `04/01/2026 A+`, `03/25/2025 A-`, `05/09/2024 A+`, `04/19/2023 B-`.
4. **Date filter / paging / caps.** Proven (table above), plus the cross-check against
   the county's own weekly list (17 of 17 graded rows, same grades). Same cap and paging
   caveat.
5. **Score. Official letter grade.** Here the score column is switched ON
   (`xx_SrchScorDspOpt=kw_Yes`), the grade is on the placard at the restaurant, in the
   weekly PDFs and in the Board of Health packets. Board of Health Policy 2010-01 EH
   ("Scoring of Food Service Establishments", adopted 2010-04-15; scanned PDF, read
   visually) and the "Food Purveyor Grading Scale" PDF:

   | Grade | Demerits | Grade | Demerits | Grade | Demerits |
   |---|---|---|---|---|---|
   | A+ | 0-3 | B+ | 12-15 | C+ | 24-28 |
   | A | 4-7 | B | 16-19 | C | 29-33 |
   | A- | 8-11 | B- | 20-23 | C- | 34-38 |
   | D+ | 39-45 | D | 46-52 | D- | 53-59 |
   | F | 60+ | | | | |

   The demerit number itself is not published, only the band, and the per-violation
   values are not public. The grade is in the list / history rows only, not in the E5
   report. Last 90 days (food): A+ 201, A 51, A- 40, B+ 13, B 3, B- 3, C+ 1, C- 1,
   D+ 1, `N/A` 29 (all 16 Pre-Operational, the Closeout, and 12 Routine rows; a
   Board of Health packet excerpt seen in search results lists a bar's Routine as N/A).
   Blank cells occurred only on rows outside the food program.
   Design choice for the builder: keep the county's letter as the official result and
   derive `risk_score` from its band (e.g. 100 minus the band's midpoint), or compute
   from tiers as for Missoula. They disagree at the bottom: the D+ sample (4 RFI + 5
   other) is 39-45 demerits for the county but 100 - 20 - 5 = 75 under the Louisiana
   rule. The county's own number is the defensible one.
6. **Violations.** As Missoula: full text, code reference, RFI / non-RFI, corrected on
   site. Titles carry a subsection prefix (`(A & B) Cold time/temperature control ...`).
7. **Coordinates / address.** None. Split address in E4.
8. **Ids.** Inspection UNID, facility UNID. One premises is several facility records,
   one per licensed component (`Firebrand Lounge, The` + `Food Service` / `Bar/Espresso`;
   a grocery's Deli / Bakery / Store), each inspected and graded separately, often on
   the same day at the same address: `fld_EstabName` is the business, `fld_FaciName` the
   component. With `make_restaurant_id(name, address, city)` on the business name these
   collapse into one restaurant and `_same_day_rank` keeps the worst of the day. The
   state permit number (e.g. `FS328279 -1`) appears only in the weekly PDFs.
9. **Volume / history / freshness.** Food program: 343 inspections in the 90 days to
   11/20/2026 (11-46 a week, mean 26): Routine 313, Pre-Operational 16, Ad-Hoc Routine
   10, Follow-Up 2, Compliance 1, Closeout 1. All programs: 34-40 a week in November.
   The Board of Health packet for September 2026 (search excerpt, not fetched) reports
   102-168 food inspections a month in 2026, the same order as measured here. History
   to at least 11/21/2008 (more than 1,000 food inspections before 2012); 5,755 facility
   categories. Newest seen 11/20/2026.
10. **Requests.** Weekly: 1 list + 2 per new inspection, about 55-95. 90-day backfill:
    3 lists + 2 x 343, about 700.
11. **Blockers.** None on the Paragon host. On `flatheadcounty.gov`: `robots.txt`
    disallows `/download_file`, and the "Weekly food inspection grades" link on the
    Environmental Health page is `/download_file/view/e62d0556-341f-493d-b1d0-16d822486292/421`,
    which redirects to an unguessable `/application/files/<4>/<4>/<4>/Weekly_Food_Inspections_<Month>_<D>_<YYYY>.pdf`.
    So the PDF series cannot be discovered without the disallowed path; treat it as a
    manual cross-check, not a source. (Disclosure: before reading robots.txt I fetched
    two policy PDFs through `/download_file/view/...`; nothing after.) PDF columns:
    Establishment Name, Facility Name, Location City, Grade, Purpose of Inspection,
    Inspection Date, Facility Permit #; no violations, no address. Newest PDF found via
    search index: `.../7217/9242/6135/Weekly_Food_Inspections_October_16_2026.pdf`
    (inspections 10/08-10/14).
12. **Verdict: BUILD.** Same fetcher, plus the official grade from the list row.

## Ravalli County Environmental Health (Hamilton)

1. Ravalli County Environmental Health, 215 S 4th St Suite D, Hamilton; ~47k people.
   `https://ravallicounty.gov/159/Environmental-Health`,
   `https://ravallicounty.gov/162/Food-Service-Licensed-Establishments` (CivicPlus).
2. Platform: none for inspection results.
3. Fetched pages 159, 161 ("Environmental Health Talk", empty) and 162: the only links
   are plan-review forms, fee schedules, a food FAQ PDF and the DPHHS / DEQ home pages.
   No link or text about inspection results or reports. No database under the guessed
   Paragon name (`/mt/ravallihamilton/inspect.nsf/Search?OpenForm` is 404; other names
   not tried). Two web searches found no portal, newspaper listing or PDF series.
4-10. Not applicable.
11. Blocker: nothing published. Whether the county keeps its inspections in Paragon, on
    paper or elsewhere is **unverified**; a records request to `[email removed]` is the
    only route.
12. **Verdict: NOTHING ONLINE.** The state portal's placeholder rows are all there is.

## Carbon County (Red Lodge) and Fergus County (Lewistown)

- **Carbon County Environmental Health** (17 W 11th St, Red Lodge; ~11k people,
  population from memory): `https://carbonmt.gov/environmental-health/` fetched (238 KB).
  It covers septic, plan review, fees and regulations; no inspection results, reports or
  search link anywhere on the page. Guessed Paragon name `/mt/carbonredlodge/` is 404.
  **NOTHING ONLINE.**
- **Fergus County Environmental Health** (712 W Main St #101, Lewistown; ~12k people,
  population from memory; one sanitarian, "field days are usually Wednesday &
  Thursday"): `https://fergusmt.gov/health-department-environmental-health` fetched.
  Licensed-establishment section links only to DPHHS, temporary-food and cottage-food
  forms and a complaint form. Guessed Paragon name `/mt/ferguslewistown/` is 404.
  **NOTHING ONLINE.**

---

## Same platform elsewhere (not surveyed in depth)

- **Kansas City, MO** (`mo/usakansascity/inspect.nsf`, wrapper
  `/foodsafety/mousakansascity/search.htm`): one E6 request,
  `[fld_InspectionDate]>=11/16/2026 AND <=11/17/2026`, returned 82 inspections (Routine
  64, Follow-Up 8, Complaint 7, Pre-Operational 2, Emergency 1; rows say `N critical`;
  score cells 95.6-100.0, display setting not checked). The site says it is updated
  daily. A Paragon fetcher built for Montana would very likely reach Kansas City
  (~508k) with a config entry; it needs its own survey (tiers, score policy, volume
  ~200+ a week).
- **Platte County, MO** (`MO/PlattePlatteCity/Inspect.nsf`; from search results, not
  fetched): history from January 2018.
- **Lewis & Clark County, MT** (`mt/lewisandclarkhelena/inspect.nsf`): up, history from
  2012-05-03, but only 1 food inspection dated 11/08-11/21/2026. Helena's current
  inspections are in the state portal; nothing to build here.

## Request log

`inspectionsonline.us`: ~63 requests (Missoula ~27, Gallatin ~11, Flathead ~14, Lewis &
Clark 2, Kansas City 1, root/robots/3 name probes 5, plus JS libraries).
`flatheadcounty.gov`: 6. `ravallicounty.gov`: 4. `carbonmt.gov`: 1. `fergusmt.gov`: 1.
`missoulapublichealth.org`: 1. `healthygallatin.org`: 1. `arcgis.com`: 2. No proxy
refusals, no TLS errors, no CAPTCHA, login or bot challenge on any host.
