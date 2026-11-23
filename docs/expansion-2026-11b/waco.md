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
- **The FAILED flag never moves the grade** (owner decision, below).
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

## Owner decision: the district's FAILED flag

About 21% of shipped inspections carry a FAILED status (`FAILED` or `FAILED
WITH PENALTY BLIC`; "BLIC" is not explained anywhere public). Many of them
have few demerits, so `100 - demerits` alone grades them A or B. Over the whole
backfill (4,026 inspections): 868 FAILED, graded A 225 / B 458 / C 155 / F 30;
225 of them at 10 demerits or fewer, 61 at 5 or fewer, 5 at 0. PASSED rows
grade A 3,022 / B 119 / C 1 (the district passes up to 23 demerits).
The district evidently fails on something other than the total (most likely
any priority item, as many Texas jurisdictions do), but the layer has no
items to check that against. Options:

1. ship as built (grade from the score, FAILED visible only in `results`);
2. cap `risk_score` for FAILED rows (as Williamson County caps an IHH closure
   at 55), which turns 838 A/B/C grades into F's on a flag whose rule is
   unknown;
3. a milder cap (e.g. FAILED cannot grade above C).

The fetcher implements 1. Changing it is one line in `fetch_waco`.

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
