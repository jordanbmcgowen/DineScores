# Waco / McLennan County, TX (Waco-McLennan County Public Health District)

Built 2026-11-23 from the survey in the expansion scratchpad
(`survey/central.md`, "Waco"). Score-only source: one demerit total per
inspection, no violation text, coordinates on every row.

## Source

- Agency: Waco-McLennan County Public Health District, Environmental Health
  (Waco, Bellmead, Hewitt, Woodway, Robinson, Lacy Lakeview, McGregor, West
  and the rest of McLennan County, ~270k people).
- Endpoint: ArcGIS Online hosted view `foodScoresView` (org WacoGIS,
  item `ea352c6df0af40e2b60166276059c36f`), read by the city's own scores app:
  `https://services2.arcgis.com/oUXiR7ziAPAzGw6X/arcgis/rest/services/foodScoresView/FeatureServer/0/query`.
  The city's web page answers 403 (Akamai) to scripts; the ArcGIS host answers
  every request.
- Fields used: `Date`, `Business`, `Address`, `City`, `Zipcode`, `TotalScore`,
  `Status`, `SVALUE` (inspection type) and the point geometry (`outSR=4326`).
  `IORDER` mirrors `SVALUE`; `mapDate` is always null.
- maxRecordCount 2000; server-side date filter (`Date >= DATE 'YYYY-MM-DD'`)
  and pagination verified. The whole layer is 5,066 rows (3 pages), 2025-01-02
  .. 2026-11-13 at the time of writing; the layer is refreshed in batches
  (`dataLastEditDate` 2026-11-17, newest row 2026-11-13 still on 11-23).
- `Date` holds LOCAL wall-clock time written as UTC (the UTC hours peak 08-17),
  so the UTC calendar date is the inspection day, and the server's `DATE`
  filter compares the same way.

## Names

| | |
|---|---|
| CLI slug | `waco` |
| Fetch group | `Waco` |
| Source label | `Waco-McLennan County Public Health District` |
| source_id | `wac_<make_restaurant_id(name, address, city)>_<YYYYMMDD>` (`_2`, `_3` for further rows of that place and day) |
| metro | `''` |

## Rules and why

- **Score**: `TotalScore` is demerit points, lower is better (the district:
  "the smaller the score the better"), 0-50 seen. `risk_score = 100 -
  demerits` (the Texas form score, McKinney's rule) and `original_score` keeps
  the demerit total. `results` is `"<n> demerits (<Status>)"`.
- **A FAILED inspection is an F**: `risk_score` is capped at 65
  (`WAC_FAILED_RISK_CAP`; owner decision 2026-11-23, below).
- **No violations**: nothing goes to Jev, no chips or summaries, no
  text-forced F, no pest-title handling needed.
- **Identity**: no permit or inspection number, and OBJECTIDs are renumbered
  when the table is reloaded (5,066 rows numbered 482,697-490,201), so they are
  never used. `source_id` hashes the normalised name, address and city (the
  restaurant id) plus the date; a test checks that renumbered OBJECTIDs change
  nothing.
- **Duplicates**: an exact duplicate row (same content, another timestamp: 63
  in the layer) ships once; distinct rows of one place and day (a failed
  renewal and a passing re-check, three same-day routine rows at a campground)
  each ship, worst first, and the pipeline's same-day rule keeps the worst.
- **Skipped**: `CANCELED` and `REQUESTED` rows (not inspections); permits the
  district marked closed by appending `-INACTIVE` (also `-INACTIVATE(D)`,
  `-INACTIVED`) to the name (583 rows): they are closed places, and a record
  shipped while the place was open keeps its own id because the marker is
  never part of a shipped name; mobile units (`- MOBILE`, `FOOD TRUCK`,
  `Foodtruck`, any `Truck` that is not a truck stop) and dated one-off events
  (`OTAKU FESTIVAL - 2026`): 20 rows; institutional kitchens by name
  (`TP_INSTITUTION_RE` + `TX_SCORE_ONLY_INSTITUTION_RE`: schools and their
  Sodexo kitchens, `*ISD` concessions, daycares, nursing homes, the county
  jail, MHMR, Baylor's residential dining halls): 371 rows, every dropped name
  reviewed (137 names).
- **Kept**: `PERMIT` inspections (375 shipped). They are not always
  pre-opening: 88 of the 289 places with a PERMIT row and other rows had the
  PERMIT visit after earlier inspections (ownership changes, re-permits), so
  they cannot be told apart from opening visits and ship like the others.
  Food-hall stalls and truck-park vendors at fixed addresses (Union Hall, the
  Dry Creek market) stay.
- **City**: `WACO (UNINCORPORATED)` -> `Waco` (the suffix is dropped);
  plain `.title()` (`Mcgregor`, the label rule McKinney documents).
- **Coordinates**: the layer's own point on every row (100% placed).
- **Window**: weekly windows never start later than `WAC_REREAD_DAYS` (60) ago,
  the Williamson County rule: the layer lags ~9 days and is reloaded in
  batches, a 60-day window is one page (~400 rows), and re-shipping is
  idempotent. No freshness holds.

## Fail closed

| Answer | Outcome |
|---|---|
| count query fails, not JSON, ArcGIS error, no `count` | **error** |
| a page fails (HTTP error, HTML, ArcGIS error, pagination not advancing) | **error** |
| fewer rows read than the server counts | **error** |
| a row without the layer's fields (a renamed column) | **error** |
| >1% rows with an unreadable date or demerit total, or more than half of a 20+ row window without `TotalScore` | **error** |
| a `Status` value outside the known list | shipped as published, WARNING logged (it never moves the grade) |
| fetch deadline already passed | **skipped** |

## The district's FAILED flag: an F (owner decision, 2026-11-23)

About 21% of shipped inspections carry a FAILED status (`FAILED` or `FAILED
WITH PENALTY BLIC`; "BLIC" is not explained anywhere public). Scored on
demerits alone they graded A 225 / B 458 / C 155 / F 30 over the backfill
(4,026 inspections, 868 FAILED), and not one Waco restaurant was an F.

**What FAILED means.** The layer has no items, so the reason for one
inspection cannot be read. The district's own rules say what the verdict is:
a re-inspection is required. The county order (section 6, amended 2026-06-02)
names three grounds: inspection items or an imminent health hazard not
corrected on the spot, violations not corrected since the previous
inspection, or more than 30 demerits. A district inspector told KWTX
(2024-03-19) "you can actually fail an inspection with 2 points": no hot
water is 2 demerits and grounds for closure. The re-inspection costs the
place $200, which is most likely what "with penalty" records (inferred, not
stated by the district).

**What the data shows** (the backfill, 2025-01-02 to 2026-11-13):

| demerits | inspections | failed |
|---|---|---|
| 0-4 | 2,396 | 36 (1.5%) |
| 5-9 | 760 | 148 (19%) |
| 10-14 | 407 | 246 (60%) |
| 15-19 | 238 | 214 (90%) |
| 20 and up | 225 | 224 (99.6%) |

- 847 of the 868 failures are followed by another visit, 812 of them a
  Re-inspection, at a median of 8 days (662 within 14 days).
- 695 of those next visits pass; the median goes from 15 demerits at the
  failure to 2 at the re-inspection. 152 fail again.
- 441 failures were at a permit renewal, 206 at a routine inspection, 156 at
  a re-inspection, 41 at a permit inspection, 24 at a complaint.
- 5 failures have 0 demerits (two permit inspections, two complaints, one
  re-inspection), 61 have 5 or fewer: a ground the total does not show.

**The rule.** `risk_score = min(100 - demerits, 65)` for a FAILED row, the
cap the pipeline already uses for a failed inspection elsewhere (the "a
failed inspection is an F" rule; closures cap at 55). `original_score` and
`results` keep what the district published ("4 demerits (Failed)").
`Passed with exceptions` and `Partial pass` (5 rows) are not failures.

**What it does to the map** (1,315 restaurants; the grade is the weighted
score of the last three visits, and an F on the latest visit is an F):

| rule | A | B | C | F |
|---|---|---|---|---|
| demerits only (as first built) | 1,252 | 61 | 2 | 0 |
| FAILED capped at 65 (shipped) | 1,010 | 277 | 3 | 25 |

The F clears itself: a place is an F only while a failed visit is its latest
(25 restaurants on 2026-11-23), and a failure in the last three visits then
pulls the weighted grade down a step after it passes re-inspection (about
270 places, almost all A to B).


## Measured (live, 2026-11-23)

- Test mode: 1 count + 1 page, 258 rows since 2026-10-09, 25 newest shipped.
- Recent window (`--mode full --since-date 2026-11-09`, widened to 2026-09-24
  by the re-read margin): 2 requests, 369 rows (server count 369), 324
  inspections 2026-09-24 .. 2026-11-13, 265 restaurants. Grades A 248 / B 55 /
  C 20 / F 1; 0 text-forced F. Risk 66-100. 100% with source coordinates.
- Backfill (whole layer, `--since-date 2025-01-01`): 1 count + 3 pages, 5,066
  rows (server count 5,066), 4,026 inspections 2025-01-02 .. 2026-11-13, 1,315
  restaurants, 3,959 D1 inspection rows (one per restaurant and day). Grades
  A 3,257 / B 583 / C 156 / F 30; 0 text-forced F; risk 50-100; coordinates
  4,026/4,026 inside 31.29-31.81 N, 97.44-96.83 W. Skipped: 583 inactive, 371
  institutional, 63 exact duplicates, 20 mobile/event, 2 canceled, 1
  requested.

## Backfill hand-off

`backfill/waco/` in the expansion scratchpad: `records.json.gz` (4,026
records), `d1.sql`, `restaurants.json`, `source_freshness.json` (group `Waco`
latest 2026-11-13, status ok), `run.log`. Jev was off (nothing to read).

## Known gaps

- No violations and no ids.
- History starts 2025-01-02 and may be a rolling window (unknown whether old
  rows age out).
- The layer lags ~9 days and refreshes in batches; a run between batches ships
  nothing new (`ok` with the same latest).
- Closed places' history is not shipped (their rows carry `-INACTIVE`).
