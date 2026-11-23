# Garland, TX (city ArcGIS MapServer) — build log

CLI slug `garland` (also fetched by `dfw`, like Mesquite, Denton, McKinney and
Lewisville), fetch group `Garland`, source label `Garland Health Inspections`,
metro `DFW`. Fetcher `fetch_garland` in `dinescores_pipeline.py` (section
"GARLAND, TX", just above `geocode_census_batch`); tests
`tests/test_garland.py` (50 tests, fixtures `tests/fixtures/garland/`).

## The source

City of Garland Health Department (Dallas County, ~246k people, its own food
program). The public "Restaurant Scores" map reads a city-hosted ArcGIS
Server 11.5 MapServer, no token, no robots.txt:

`https://maps2.garlandtx.gov/server2/rest/services/Health/Restaurant_Scores/MapServer`

- **Layer 4 "Restaurant Scores"**: one row per PERMIT with its latest
  inspection: `FACILITY_ID, PERMIT_NUMBER, PROGRAM_IDENTIFIER, NAME, ADDRESS,
  GIS_LATITUDE, GIS_LONGITUDE, INSPECTION_DATE, SCORE, PE` and a WGS84 point.
  1,143 rows (2026-11-23), newest 2026-11-20.
- **Table 3 "Restaurant violations"**: one row per cited item of that same
  inspection (`VIOLATION_CODE` 1601-1647 = Texas form items 1-47, `POINTS`,
  `VIOLATION_DESCRIPTION`, `V_MEMO` = the inspector's observation), or one row
  with a null code for a clean inspection; adds `CITY/STATE/ZIP`. 7,478 rows.
- Both filter server-side on `INSPECTION_DATE` (midnight UTC on every row, so
  the UTC date is the inspection date), page by `resultOffset`
  (maxRecordCount 2000) and answer `returnCountOnly`.
- The city's report viewer for one permit,
  `https://maps2.garlandtx.gov/customgispages/restaurantviolations/?facilityid=..&permitid=..`,
  is the record's `source_url`.
- The old `foodindex.garlandtx.gov` grid is frozen at 2026-06-30 and is not
  read.

A weekly run is 5 requests (layer count + 1 page, table count + 2 pages for
the 90-day re-read), one at a time, ~1 s apart.

## Rules chosen

- **Score**: `SCORE` is the city's official 0-100 score; it ships unchanged as
  `risk_score` and `original_score`. Each cited item is one violation line,
  tiered by the city's points (3 priority / 2 priority foundation / 1 core;
  items 1-20 / 21-33 / 34-47, verified per code over all 7,478 rows). A 0-point
  row is an observation the inspector did not count ("not being counted as a
  violation") and is not a line. `jev_reader.SOURCE_TIER_SOURCES` lists the
  label, so Jev never re-tiers or re-scores it.
- **Complete or nothing**: per inspection the points must be the item's form
  weight, every table row must carry the layer's score, and the points must add
  up to `100 - SCORE`; a score below 100 with no table row at all is not a
  clean read. Any of these drops the inspection (named in the run report's
  note); 10+ of them making up >5% of the window raise. A 100 with no table
  row ships clean (no points is what a 100 means). Every read is checked
  against the server's count; a short read, HTTP error, non-JSON answer,
  ArcGIS error or a page whose OBJECTIDs do not advance raises and nothing
  ships. Past the fetch deadline nothing is requested ('skipped').
- **The 30 rows where 100 - points != SCORE** (whole table, 1,248 permit
  inspections): 23 unscored rows (SCORE 0, null code, 2023-2024, not in layer
  4), 2 more table-only permits (PT4755 with a null score, PT4336 scored 100
  over a 2-point item), 4 school summer meal sites (PE 1628, scored 100 with
  3-4 cited points: the district's sites are not scored), and PT2896, a
  2023-10-04 convenience-store inspection on an older form (item 2 at 5 points,
  item 16 "Warewashing" at 2, item 40 "Toxic items" at 3; 12 points for an
  89). Only PT2896 is in scope and in layer 4; it is dropped (and named in the
  note) whenever a window reaches 2023, which a weekly window never does.
  Nothing points at a stale table: no permit carries two dates, and every
  layer row has its table rows on the same date.
- **Snapshot, not history**: a re-inspection overwrites the permit's row in
  both objects, so the weekly run is the history. Every run re-reads at least
  `GAR_REREAD_DAYS` (90) and holds nothing (the Williamson rule, "a snapshot
  re-reads instead of holding"): a late, revised or not-yet-adding-up
  inspection ships on a later run, and re-shipping is idempotent
  (`source_id` = `garland_<permit>_<YYYYMMDD>`; D1 keys inspections by
  restaurant + date). **What a missed week costs**: the earlier visit of
  every permit inspected twice since the last successful run (typically the
  failed routine a follow-up replaced); it can never be recovered. Nothing
  else is lost.
- **Text**: McKinney's public titles for the 47 form items, then the
  inspector's observation (leading dash/bullet dropped, phones and e-mails
  scrubbed). Item 34's table title ("No Evidence of Insect Contamination,
  Rodent/other Animals") never ships: the line is `Pests and animals:
  <observation>` (`HH_PEST_TITLE`), item 20 (sewage) carries the observation
  alone, and a cited item without an observation gets McKinney's neutral
  placeholder ("Pest control item cited on the inspection report; no
  observation recorded", already in `CITED_PEST_ITEM_TITLES`; one live row).
  Lines end in sentence punctuation (the text-F regex never crosses a period)
  and are capped at 500 characters; when the cap would cut off the sentence
  naming a pest or sewage finding (bilingual memos run to 3,000 characters),
  those sentences move to the front.
- **Scope** (PE, the old grid's labels): 1601 full service, 1602 fast service,
  1603 snack bar (includes the Curtis Culwell Center's concession stands),
  1604 convenience store, 1605 supermarket, 1606 processor, 1609 warehouse,
  1611 assisted living, 1612 hospital / nursing home ship. 1607 child care,
  1608 school, 1610 mobile (none today), 1624 plan review (one, scored 0) and
  1628 school summer meal sites do not. 1614 (one smoothie stand, last seen
  2024-06) is unknown and logged. The shared DFW name rule also drops the
  school district's warehouse and freezer and an adult day care. Permits that
  exist only in table 3 (105: temporary events, grease haulers, unscored
  rows) are never read: only layer-4 rows ship.
- **Identity**: name is `NAME - PROGRAM_IDENTIFIER` when the identifier names
  a department or tenant ('Kroger Food Store #557 - Deli', "Garland Homeboy's -
  Street Taco"), as the city's map titles it; 193 permits share a name and an
  address with another one (every shipped record still gets its own id). `'0'` and office notes ('INSPECTIONS ONLY') are
  not departments. Address is layer 4's street address; city is always
  Garland (every layer-4 permit's table rows say GARLAND).
- **Coordinates**: the layer's point (SR 4326), else the GIS columns; four
  permits carry the two columns exchanged and are put back; anything outside
  `coords_plausible(.., 'TX')` is dropped. 25 permits have none and go to the
  Census geocoder (10 of 11 in-scope ones geocoded in the backfill run).

## Live verification (2026-11-23)

- `--mode test`: 4 requests, 206 permits since 2026-09-24, 25 records, newest
  2026-11-20, group `ok`.
- `--mode full --since-date 2026-11-09` (widened to 2026-08-25 by the
  re-read): 5 requests, 426 permits, 2,821 table rows, 363 records
  (2026-08-25 .. 2026-11-20; skipped 54 school, 8 child care, 1 by name), no
  inspection dropped. Grades A 166 / B 122 / C 61 / F 14 (7 by score, 7
  text-forced). Of the 35 records inside the requested 14 days: A 25 / B 7 /
  C 3, no F. 362/363 located, scores 55-100, no implausible coordinates.
- Text-forced F's in the 90 days: 6 real findings (3 cockroaches by the
  sink, roaches in a hand-sink drain, a dead roach on the fryer, droppings
  and dead roaches, 2 roaches in a walk-in, a dead roach in an aisle) and
  **1 false one**: PT4745, item 18 "Observed a raid roach bottle near the hand
  wash station" (a pesticide, not a roach).

## Backfill hand-off

`/tmp/claude-0/-home-user-DineScores/302e47b7-d066-5f23-8c3f-df0221d21f67/scratchpad/backfill/garland/`
(`records.json.gz`, `d1.sql`, `restaurants.json`, `source_freshness.json`,
`run.log`): one full read of the layer (`--since-date 2023-10-01`, 6
requests): 1,143 permits, 7,478 table rows, **1,014 records** (one per
permit, 1,014 distinct restaurant ids), dates 2025-02-25 .. 2026-11-20 (152
in 2025, 862 in 2026; 376 since 2026-08-24). Skipped: 67 school, 42 child
care, 14 summer meal sites, 3 by name, 1 plan review, 1 PE 1614; dropped
PT2896 (old form). Grades A 487 / B 336 / C 163 / F 28 (10 by score, 18
text-forced). 1,013 of 1,014 located. Jev was off; the absorbing run reads
the lines.

Text-forced F's in the backfill: 14 named findings (roaches, ants crawling,
rodent droppings) and **4 regex false positives on item 18 (toxic
substances), each a pesticide named after its pest**: PT2094 "Ant
killer/pesticide was observed", PT3420 "a can of ant spray", PT2148 "a
residential rat bait", PT4745 "a raid roach bottle". Jev's hazard decision
(decide mode, removal needs the 0.6 floor) is expected to clear them; under
the regex fallback they stay F.

## Known gaps and open decisions

- **Pesticide wording trips the shared text-F regex** (4 of 1,014 records
  above). Not fixed here: it is `PEST_EVIDENCE_RE` (and its mirror in
  `src/grading.js`), shared by every source. A guard like "pest word followed
  by spray / bait / killer / bottle / trap" would be a cross-source rule
  change for the owner to decide and measure.
- No history before the first run beyond each permit's latest inspection; a
  missed week loses replaced visits for good (see Snapshot).
- No inspection type or result text in either object (`inspection_type` and
  `results` ship empty).
- The workflow's Sunday city list (`.github/workflows/refresh-data.yml`)
  names DFW cities individually: `garland` has to be added there to run
  weekly (not edited here).
- Not verified: reachability from GitHub-hosted runners (only from this
  session); the city's own revision cadence ("This report data may be under
  review and may be revised").
- `public/data.js` already holds a few 'Garland' city records from another
  source (not identified here; temporary-event permits such as "Jazzy's Tacos Llc (Temp)"); they
  have different names and ids and do not collide.
