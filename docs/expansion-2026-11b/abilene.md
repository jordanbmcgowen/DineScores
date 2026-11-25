# Abilene / Taylor County, TX — `fetch_abilene` (Tyler EnerGov Civic Access)

CLI slug `abilene`, fetch group `Abilene`, source label
`Abilene Environmental Health`, `source_id` `abl_<case number>`, metro `''`.

## The source

City of Abilene Environmental Health Division (Abilene-Taylor County Public
Health District; city ~125k, county ~143k). Inspections are published through
Tyler EnerGov "Civic Access Public"
(`https://cityofabilenetx-energovweb.tylerhost.net/apps/civic-access-public/`),
an Angular SPA over an anonymous JSON API under `civicapi/`: no cookie, token,
tenant header, CAPTCHA or WAF seen; `robots.txt` 404. The tenant holds only
Environmental Health records. The city moved to new inspection types in
September 2026; earlier visits are "Legacy Retail Food Establishment" (16,780
rows back to 2019).

Routes used (all verified live 2026-11-22/23):

| Call | Use |
|---|---|
| `POST search-inspections` | the list: `requestedDateFrom` (server-side), `requestedDateTo: null`, `caseTypeId` optional, `pageNumber` 1-based, `pageSize` 100, `totalCount` |
| `GET inspection/{caseId}` | header: type, status, requested / scheduled / completed dates, `companyName`, `mainAddress`, permit link (`linkId`, `linkModule`) |
| `GET inspection/inspection-score-violation/{caseId}` | the official `violationScore` |
| `POST violations-information` `{"inspectionId", "isInViolation": true, "module": 7, "inspectionViolationItemId": ""}` | one row per OUT item: form item number and title, the agency's tier group, the inspector's notes |
| `GET operational-permit/{linkId}/summary` | permit class (scope), business name fallback |
| `GET details/business/{linkId}/page-title` | the name when the inspection links a business instead of a permit (`linkModule` 0) |

`source_url` is the SPA's own route `details/inspection/{caseId}`.
The header carries the inspector's name and e-mail and a free-text comment;
the permit summary its issuer's name and e-mail and, in `description`, often
the business's contact e-mail. None of them is read into a record, and the
fixtures replace them with `<redacted>`.

## How a window is read

1. **List.** `search-inspections` over every type, `requestedDateFrom` =
   window start − `ABL_REQUESTED_MARGIN_DAYS` (60), no upper bound, paged to
   a short page; the unique count must reach `totalCount` and a page that
   adds nothing fails closed. Plus an undated listing of each new food type
   (`RFE-Routine`, `RFE-Reinspection`; 3 + 1 pages today) whose rows with no
   scheduled date join by their requested date (see "Completeness").
2. **Headers** of every listed food row (type filter first). The completed
   date is the inspection's date. Kept: completed on or after window start −
   `ABL_COMPLETED_MARGIN_DAYS` (14), status Completed / Passed / Failed, and
   not already stored (`SKIP_KNOWN_INSPECTIONS`, which the weekly passes).
3. **Per kept inspection**, oldest first: the permit summary (once per permit;
   out-of-scope classes stop here), the score, and (below 100) the OUT lines.

## Rules chosen, and why

- **Inspection date = `completedDate`.** The requested date is the visit's due
  date, set when the previous visit closed (the next routine is created at
  once as a "Scheduled" inspection due on the 1st of a month 2-6 months out).
  Measured over the 432 completed food inspections requested since
  2026-08-24: completed − requested = 0 days for the median, 20 at p90, 27 at
  p95, 49 at most (RFE-Routine; legacy rows 0, schools ≤ 30). A visit done
  EARLY keeps its future due date: RFEI-ROUT-000886-2026 was completed
  2026-11-20 and is requested 2027-05-01 (11 of 249 routines negative, down to
  −162 days). Hence the open upper bound and the 60-day back margin.
- **Score.** `risk_score` = `original_score` = the agency's `violationScore`
  (0-100, 70 is the pass line). It equals 100 − 3 × Priority − 2 × Priority
  Foundation − 1 × Core counted **per distinct form item**: 103 of 103 scored
  new-format inspections in the November capture, and 10 of 10 non-100 legacy
  ones (the legacy rows cite an item once per finding, e.g. 69082 scores 71
  with 20 lines over 16 items; counting every line gives 64). Over the
  90-day backfill: 407 of 407 below 100 (below). The score endpoint's own per-tier
  counts are NOT usable as the check: on 22 of 105 inspections they carry a
  blank severity name.
  **An inspection whose lines do not reproduce its score is dropped and held**
  (counted; McKinney's rule, not Harris's): the lines are a separate request,
  and on this source a mismatch has only been seen as "part of the answer was
  not read". More than 5% of the checked inspections disagreeing raises.
  A score of 0 (or none) is "no score": the RFE-Reinspection visits re-check
  only the failed items and are stored at 0.0 (2 of 3 seen); they are left
  out, not dropped. A re-inspection with a real score ships as
  'Reinspection'.
- **Tiers** are the agency's (`violationCategoryGroupName`: "Priority Items (3
  Points)", "Priority Foundation Items (2 Points)", "Core Items (1 Point)");
  `Abilene Environmental Health` is registered in `jev_reader.SOURCE_TIER_SOURCES`.
  An OUT row with any other group, or no title, makes the inspection unread.
- **Violation text** = `<form item title>: <inspector's notes>`, or the title
  alone (notes are empty on ~65% of new-format lines and on every legacy
  line). A line repeated verbatim ships once.
- **Pest and sewage titles.** Item 34 "No Evidence of Insect contamination,
  rodent/other animals" ships as `Pests and animals: <notes>` (the Hedgerow
  `HH_PEST_TITLE` rule); bare, it is the TFER title already registered in
  `CITED_PEST_ITEM_TITLES` (pest chip, "Pests found", never an F;
  `LISTED_PEST_TITLE_RE` also removes it from the regex text). Item 20
  "Approved Sewage/Wastewater Disposal System, proper disposal" does not trip
  `SEWAGE_ISSUE_RE` alone, but does after item 19's title in the joined text
  ("... proper backflow device|||Approved Sewage ...": `back` within 40
  characters of `sewage`), or before notes that start with a floor or a
  discharge; it ships under the neutral heading `Wastewater disposal`.
  Observations keep their F: "Observed sewage backing up from the floor
  drain", "Observed live roaches", "Observed rodent droppings" all still force
  it (tested). In the November data item 34 was cited for door closers, door
  gaps and a propped-open door (one also notes flies), item 20 for grease
  around the grease bin and a mop bucket dumped outside.
- **Scope.** Ship: `RFE-Routine`, `RFE-Reinspection` (when scored), `Legacy
  Retail Food Establishment`. Dropped by type: `RFE-Bi-Annual School`, mobile
  (`MF-*`, `Legacy Mobile Food`), `Temporary Food Service`, pools, day care,
  interactive water features; any other type is dropped with a warning.
  Dropped by permit class: `School - Biannual Inspection`, `Academy` (the
  university dining contractor's permit), `Day Care`, `Nursing Home`,
  `Hospital`, `Mobile Food`, `Food Demo`, `Wholesale`. Dropped by name: until
  the school class existed the district kitchens held 'Full Service' permits
  (now Inactive) and were inspected as plain routines or legacy visits, so
  the Tyler fetchers' `TP_INSTITUTION_RE` plus a local "early childhood" /
  "…ISD" rule (`_abl_institutional`) leave them out: 17 of the backfill's 499
  (14 school kitchens, 2 at the district warehouse, 1 Chick-fil-A counter
  inside Hendrick Medical Center, which the shared rule's "medical center"
  catches as it would for Georgia and Louisiana). Only food types were
  listed since 2026-08-24 (RFE-Routine 250, Legacy 158, RFE-Bi-Annual School
  22, RFE-Reinspection 3); the pool and day-care types never appear in the
  public search.
- **City and ZIP.** No route carries either: header and list give the street
  only ("4534 BUFFALO GAP RD", "2025 S 14th ST Unit:STE A"), the permit summary
  the same street plus `districtName`, which is "City of Abilene" on every
  permit read (104 in November, all in the backfill, below). The address
  lookup `public-records-search/search-address/{street}` returns
  "street\r\nABILENE, TX 796xx", but it is a fuzzy parcel match: 7 of 99
  streets returned nothing and 5 returned 2-3 different ZIPs for one street,
  so it is not used. City is `Abilene`, ZIP empty, and main() geocodes. A
  Taylor County address outside the city would carry the wrong city; none was
  seen (the tenant is the City's own system).
- **Names and addresses.** `companyName` (title-cased when ALL CAPS, possessives,
  ordinals and "Mc" fixed); the permit's `businessName`, or the business page
  title, when blank. Addresses are always title-cased, "Unit:" dropped,
  directions kept upper ("ES 11th St" is Abilene's East South 11th).
  `make_restaurant_id` lower-cases its inputs, so casing never splits a place.

## Completeness (measured)

- **Against per-permit histories.** For 30 randomly chosen permits,
  `POST inspections/existing {"entityId": linkId, "module": 15}` lists every
  inspection of the permit. All 37 completed food inspections requested since
  2026-08-24 were in the window search; the only ones missing were 17
  "Scheduled" future visits (requested 2027-02..05), i.e. not done yet. The
  survey's worry (case numbers run to 886 while ~280 are listed) is explained:
  the number sequence is shared by every inspection created, including the
  scheduled next visits.
- **The date filter's blind spot.** RFEI-ROUT-000151-2026 (requested
  2026-09-28, completed, score 97) is listed by an undated search and by its
  case number, but by no `requestedDate` window, even the single day: the
  filter leaves out a row with no scheduled date. 1 of the 251 routine rows
  (the undated routine listing has 251, the window since 2026-08-24 has 250).
  Hence the undated sweep of the new food types; the legacy type (finished)
  is not swept, so a legacy row with no scheduled date would be missed by a
  backfill (none of the 3 legacy rows checked lacked one).
- **Posting lag** (a visit listed only days after its completed date) could
  not be measured from one snapshot: between the survey's listing
  (2026-11-22 04:40 UTC) and ours (2026-11-23 11:15 UTC) nothing new appeared.
  The 14-day completed margin re-reads such late rows; it costs only headers
  for inspections already stored.

## Volume and requests

- About 30 food inspections a week (RFE-Routine by week 39-47: 26, 31, 8, 28,
  25, 4, 59, 22, 19).
- A weekly run (window start ≈ 8 days back): 4-8 search pages, one header per
  food row requested in the last ~68 days (~280 today), then for each
  inspection completed in the last ~22 days one permit summary (cached per
  permit), one score and one lines call (none at 100). The 14-day live run
  below made 642 requests in 690 s; the weekly's `--skip-known-inspections`
  removes the detail calls for embedded inspections. One request stream,
  `ABL_DELAY` 1 s after every answer.
- Failure handling (Harris's shape): 4 attempts with 5/15/45 s backoff (a
  429/503 Retry-After honoured); a 400/404/410 is that record's own problem;
  `ABL_BREAKER_TRIP` (3) requests in a row exhausted → `_ABL_BLOCKED`, the
  group reports `blocked`. An unread header holds the window start (its date is
  unknown); an unread permit / score / lines drops and holds that inspection
  (counted, key = case number); a breaker or deadline cut holds the rest
  uncounted and reports `blocked` / `skipped`. A list page that cannot be read
  or comes up short raises. More than half of ≥ 20 tried inspections unread
  raises; so does a score mismatch on more than 5%.

## Live verification (2026-11-23, Jev off, OpenStreetMap fallback off)

- **Test mode** (`--mode test`, limit 25, last 30 days): 99 requests, 109 s,
  24 records (one unscored re-inspection, 3 school rows left out), 24/24
  geocoded by the Census batch, group `ok`, latest 2026-11-20.
- **14-day window** (`--mode full --since-date 2026-11-09`; keeps completed
  from 2026-10-26, lists requested from 2026-09-10): 302 rows from the window
  search (4 pages) + 1 unscheduled row from the sweep (3 + 1 pages), 285
  headers, 642 requests in 690 s. 119 records 2026-10-26..2026-11-20, 117
  restaurants; grades A 91 / B 26 / C 1 / F 1 (the F is the 66 Failed
  routine); **0 text-forced F's**; 111/119 (93%) geocoded by Census (the
  8 misses: two campus addresses, suites, "ES 11th St"; CI's capped
  Nominatim fallback retries them); scores 66-100, none out of range; left
  out 162 completed before the window, 17 school-type, 3 unscored, 1 Academy,
  1 'Completed' row with no completed date. (This run predates the
  institution-name rule, which would also drop its one hospital food-court
  record.)
- **Score rule over the backfill**: every one of the 407 inspections the run
  read scored below 100 (220 new-format, 187 legacy) reproduced
  100 − 3P − 2PF − C over distinct items; nothing was dropped or held.

## Backfill hand-off (`--since-date 2026-08-24`)

`/tmp/claude-0/-home-user-DineScores/302e47b7-d066-5f23-8c3f-df0221d21f67/scratchpad/backfill/abilene/`

- `run.log`: 716 rows requested since 2026-06-25 (8 pages) + the sweeps,
  693 headers, **2,054 requests in 2,212 s**, 499 inspections, group `ok`,
  latest 2026-11-20, no drop, hold, mismatch or unread page.
- The run predates the institution-name rule; its output was then filtered
  with that rule's own function (`_abl_institutional`) and re-written:
  `records.json.gz` **482 inspections** (2026-08-10..2026-11-20; 239
  new-format, 243 legacy), 416 restaurants; `d1.sql` regenerated from those
  records with `--input-records` (416 restaurants, 472 inspections after the
  one-per-day rule). The unfiltered originals are kept beside them
  (`*_unfiltered.*`). `source_freshness.json` is the run's own (group
  `Abilene`, source `Abilene Environmental Health`, latest 2026-11-20, no
  hold).
- Grades A 403 / B 65 / C 13 / F 1, 0 text-forced F's, 438/482 (91%)
  geocoded by Census alone. 35 item-34 lines (10 with notes, all door closers,
  gaps or a propped door; one notes flies) and 24 item-20 lines, none an F.
- 10 restaurant-days carry two inspections (two permits at one name and
  address, e.g. Ollie's 000102 and 000151, or two legacy entries of one
  visit); the one-per-day rule keeps the worst.

## Known gaps / open decisions

- Undocumented vendor API: a Tyler release can rename routes. Every shape
  check fails closed; the group goes red, it never ships zero rows quietly.
- Not tested from GitHub-hosted runners (IIS on tylerhost.net; no WAF seen).
- City is always Abilene and ZIP empty (see above).
- University dining permitted as "Full Service" (McMurry Cafeteria, HSU - The
  Caf) ships; the `Academy` class (Chartwells at a campus) does not. Owner's
  call whether campus dining belongs on the map.
- Re-inspections stored at 0.0 are left out; a revised report keeps its case
  number and is upserted when re-read inside the margins.
- Posting lag unmeasured (see above); the requested-date margin is 60 days
  against a measured maximum of 49, so a visit done more than ~2 months after
  its due date is missed.
