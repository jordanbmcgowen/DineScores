# feat/dfw-holes progress log (McKinney, Lewisville, Denton)

Branch: feat/dfw-holes. Section in dinescores_pipeline.py:
"DFW CITIES WITHOUT A PORTAL ON MHD: McKINNEY, LEWISVILLE, DENTON".
If you are a successor agent: read this file, `git diff`, run
`python3 -m unittest discover -s tests` from the worktree root, continue.

## Status: complete, FIX pass applied (2026-11-03)

- [x] Recon read; live probes of the uncertain shapes.
- [x] data/certs/lewisville_ca_bundle.pem (certifi 2026.07.22 + GoDaddy G2
      intermediate, provenance in the header).
- [x] Section written: shared retry GET (_dfw_hole_get), fetch_mckinney,
      fetch_lewisville, fetch_denton; Irving stub deleted; main() tuple entry
      (('irving','dfw'),'Irving',fetch_irving) -> (('denton','dfw'),'Denton',
      fetch_denton).
- [x] jev_reader: 'McKinney Health Inspections' in SOURCE_TIER_SCORE_SOURCES
      and SOURCE_TIER_SOURCES; tests/test_jev_reader.py test_policies extended
      (also asserts Lewisville/Denton labels are 'official').
- [x] Fixtures (tests/fixtures/{mckinney,lewisville,denton}/) and tests
      (tests/test_mckinney.py, test_lewisville.py, test_denton.py): 73 tests.
- [x] Suite green: 347 tests, node tests/test_relay.mjs, py_compile.
- [x] Live verification (below).

## FIX pass (2026-11-03), after two adversarial reviews

Fixed (code + tests, suite green: 362 tests, node tests/test_relay.mjs, py_compile):
- Date bombs: the 'fresh data: no note' assertions in test_mckinney / test_denton
  compared fixture dates (2026-10-30 / 2026-10-28) with today; they now move the
  fixture rows to today's date. RecordTests.setUpClass fetches (all three modules)
  snapshot/restore _GROUP_NOTES/_FRESHNESS_HOLDS/_GROUP_STATUS so nothing leaks.
- McKinney permit layers fail CLOSED: an unreadable layer 0/1/2/3 raises
  RuntimeError like an unreadable layer 4 (shipping without them put schools,
  mobile units and splash pads on the map for good and dropped permit-named rows
  without a hold).
- McKinney scoring: risk_score = 100 - demerits (the TX form score, what
  Richardson, Dallas and Plano ship); original_score stays the demerit total.
  Jev: 'McKinney Health Inspections' moved out of SOURCE_TIER_SCORE_SOURCES
  (score 'official', tiers 'fixed'). The 5/2/1 recomputation was the DFW outlier.
- McKinney tripwire: per inspection 3P+2PF+C must equal total_score (true on
  503/503 live inspections); a mismatch drops + holds that inspection
  (counted=True, key=number); >= MCK_MISMATCH_MIN (10) mismatches above
  MCK_MISMATCH_MAX_SHARE (5%) of the window raise. Emptied item columns can
  never read as clean 100s.
- McKinney comment parser: '#NN' items are read in any section ('Observations:'
  headers, repeated 'Temperatures:' headers), dash items inside a named section
  are read when they look like observations ('34 - Observed live roaches...'),
  '#9 Observed' (no punctuation) and inline '... supplied. #34. Observed ...'
  markers are read, header-shaped lines (CFM/PIC/Manager/Owner/Pest Control
  without a sentence) never join a block while sentence continuations do.
  Live 90-day corpus (503 inspections, 1462 cited items): neutral lines
  78 (5.3%) -> 21 (1.4%), records with every line neutral 16 -> 2, neutral
  item-34 lines 3 -> 0, raw-text pest/sewage evidence missing from every
  shipped line 0 -> 0; the one pre-existing line that changed is the inline
  case (item 31 no longer swallows item 34's sentence). Residual: 2 lines of
  the form '10, 29 - Observed ...' (two items at once) stay unattributed.
- Hazard safety net: a CITED item 34 / 20 whose own text carries no pest /
  sewage sentence gets the report's pest / sewage sentences (MCK_HAZARD_ITEMS),
  so a cited pest item never hides a roach behind the neutral phrase. Not
  applied when no pest item was cited.
- City label 'Mckinney' (plain .title()), matching the 37 D1 rows / 20 data.js
  records the Plano portal already wrote; 'McKinney' would have split the town
  in cities.js, the city= filter and the embed floors. Follow-up for the owner:
  a rename seed could move everything to 'McKinney' later.
- Scope rule shared by the three cities (DFW_HOLE_NON_RESTAURANT_RE): schools /
  child care by name (LISD cafeterias, Kiddie Academy, Montessori, Goddard,
  Primrose, learning centers, early-education) + 'micro market'; McKinney also
  drops the layer-0 class 'Micro Market - Vending Machine'. Measured: Lewisville
  75 of 1146 rows (35 names) dropped; Denton 0; McKinney adds Applebee
  Montessori Academy, Aramark Micromarket, Lowe's micro markets. Denton's
  'Mobile' names all carry a fixed address (the source does not say they are
  mobile) and are kept, documented.
- Lewisville / Denton: a window of >= 20 rows with more than half unscored
  raises (a re-typed Score column must be a red run, not a green 'empty').
- Denton: ORDER BY every column (a total order; 681 of 2570 permit+date pairs
  have several distinct rows, which a partial order could shuffle across a page
  boundary); the new shape verified live against the WAF (200). Dead MAX()
  fixture/harness branch removed.
- Docstrings: full mode starts 2024-01-01 (main()), not the 2017 layer start.

Rejected / not changed:
- Reviewer B's mobile-unit scope for Denton: every Denton row has an address, so
  the source does not let us tell; dropping by a 'Mobile' in the name would also
  drop a fixed snack bar (TACO LADY MOBILE-SNACK BAR, 9 inspections at a suite).
- Nursing homes, park concessions, golf courses stay (fixed-address kitchens
  serving food; not in the task's drop list).

Live post-fix runs (2026-11-03, this container through the agent proxy, TLS verified,
Lewisville against the committed bundle; outputs /tmp/<slug>_fixed.json and
/tmp/<slug>_30d_fixed.json):

| run | records | wall | requests | coords | grades | notes |
|---|---|---|---|---|---|---|
| mckinney --mode test | 25 | 16 s (13 s Census) | 6 + Census | 25/25 (9 Census) | A20 B5 | 341 rows / 60 d, 59 school/micro-market + 3 unnamed skipped; 83 lines, 1 neutral |
| lewisville --mode test | 25 | 1.4 s | 1 | 25/25 | A23 B2 | 1146 rows / 365 d: 19 unscored + 85 school/child-care dropped |
| denton --mode test | 25 | 12 s | 2 + Census | 23/25 (10/12 Census) | A21 B4 | one 2000-row page of 3134 pairs (bounded read), total ORDER BY accepted by the WAF |
| mckinney --since-date 2026-10-04 | 117 (131 rows; 9 scope + 1 unnamed skipped) | 7.7 s | 6 + Census | 117/117 (16 Census) | A100 B14 C3 | risk min 76 (24 demerits); 389 lines, 4 neutral; 0 text-forced F; tiers add up on 117/117 |
| lewisville --since-date 2026-10-04 | 74 (80 rows; 6 school rows) | 1.1 s | 1 | 74/74 | A67 B6 C1 | min score 77 |
| denton --since-date 2026-10-04 | 65 (server count 65) | 21 s | 2 + Census | 62/65 (27/30 Census) | A49 B16 | min 84 |

Every record carries the 21 contract keys, float/None coordinates, exact
severity strings, no newline, lines <= 500 chars, metro 'DFW', state 'TX',
city 'Mckinney' / 'Lewisville' / 'Denton'. No holds, no warnings, no
mismatches in any live run.

## Design (decided after the recon docs + probe captures + live probes)

- McKinney: ArcGIS MapServer layer 4 via _arcgis_query_all, where
  actual_end_date >= DATE 'YYYY-MM-DD' (verified), explicit outFields (no
  inspector_name / attachments), a returnCountOnly count first and a raise
  when rows < count. Coordinates: geometry (outSR 4326) -> POINT_X/Y ->
  layer-0 permit join on link_number -> None (main() geocodes). Dates are
  epoch ms holding LOCAL wall-clock flagged UTC (hours 7-15 UTC), so the UTC
  date is the local date. total_score = DEMERITS: the 47 item columns hold
  exactly {0, TX weight} (verified on 131 live rows; items 1-20 = 3 pts,
  21-34 = 2, 35-47 = 1) -> tier from points (3 priority / 2 PF / 1 core),
  risk_score = 100 - demerits (the TX form score; FIX pass), original_score =
  demerits, results "<n> demerits (<status>)". Violation text = "<form title>: <inspector's
  '#NN.' block from custom_comments>" (items 20 sewage and 34 pests carry
  the observation ALONE, no title: a bare TX title "No Evidence of Insect
  contamination, rodent/other animals" trips PEST_EVIDENCE_RE by itself);
  a cited item with no attributable text ships a neutral phrase (1.4% of
  lines over 90 days after the FIX pass; a cited pest/sewage item gets the
  report's hazard sentence instead). The report header (CFM's personal name, pest
  contractor) is never read; phones/e-mails scrubbed. Same inspection
  appears once per PDF attachment -> dedupe on inspection_number.
  Scope: drop status 'Void', purpose '3 - Pre-Opening', permits on layers
  1-3 (mobile/temporary, farmers market, schools & daycares), layer-0
  WORK_CLASS in {Mobile Food Establishment, Farmers Market Temporary,
  Public Interactive Water Feature}, and school/daycare names
  (MCK_SCHOOL_NAME_RE). Verified over 90 days: all 54 skipped records are
  MISD cafeterias/concessions (30 by name, 24 by layer 3); 3 'no name' rows
  were grocery department sub-permits ("See grocery report") and one blank
  permit. source_url = per-inspection REST f=html page (verified 200).
- Lewisville: one GET HealthInspectionScores_byDate&DateBegin&DateEnd (ISO),
  DateBegin floored at today-365 (the service keeps the trailing 365 days; a
  window starting 2024-01-01 returns those 365 days, verified). verify=
  data/certs/lewisville_ca_bundle.pem for this host (default store FAILS:
  missing GoDaddy G2 intermediate; eservices host does not proxy the API).
  Error marker {"projects":[],"BusList":[{"Error_ID":...}]}, non-JSON or
  non-200 -> raise. Scores only: original_score = risk_score = Score;
  'NO SCORE'/'PASS' rows dropped (19 of 1146 in 365 days: a daycare, snow-
  cone/hot-dog stands). zip ''. InspectionDate 'Z' is local time: [:10].
- Denton: CKAN datastore_search_sql with the verified shapes only: COUNT
  over a DISTINCT subquery with WHERE, SELECT DISTINCT <10 cols> WHERE
  "HealthScoreDate" >= 'YYYYMMDD' ORDER BY "HealthScoreDate" DESC,
  <every column> LIMIT 2000 OFFSET n (a total order; paged until a short
  page; count checked when complete). HTML/403 (Cloudflare WAF) or any non-JSON -> raise
  at once; 429/5xx retried 3x. Scores only; RESULT text -> results
  ('Approved', 'Failed', 'Approved with conditions'...); the letter
  (Denton cutoffs 94/87/80) is never copied. Dedupe (PERMIT_NO, date),
  worst wins. Every Type in the data is a food type -> nothing filtered.
- All three: since in both formats and None; limit keeps the newest and
  leaves no lag note (a bounded read); complete reads call
  _note_source_lag(<group>, newest) once (21-day threshold inside it);
  oldest-first, worst-first within a day; no detail phase -> no holds except
  McKinney's drop-and-hold of an inspection whose item points do not add up
  (complete-or-raise otherwise), asserted by the tests.

## Live results (2026-11-03, from this container through the agent proxy)

| run | records | wall | requests | coords | grades |
|---|---|---|---|---|---|
| mckinney --mode test | 25 | 6 s | 6 (count, page, layers 0-3) + Census | 25/25 (9 Census) | A20 B4 C1 |
| lewisville --mode test | 25 | 2 s | 1 | 25/25 | A24 B1 |
| denton --mode test | 25 | 11 s | 2 (count, 1 page of 2000 distinct rows) + Census | 23/25 (10/12 Census) | A21 B4 |
| mckinney --since-date 2026-10-04 | 118 (131 rows, 8 schools + 1 unnamed skipped) | 8 s | 6 | 118/118 (16 Census) | A92 B21 C5 |
| lewisville --since-date 2026-10-04 | 80 | 1 s | 1 | 80/80 | A73 B6 C1 |
| denton --since-date 2026-10-04 | 65 (server count 65) | 22 s | 2 | 62/65 (27/30 Census) | A49 B16 |
| mckinney --since-date 2026-08-05 (90 d) | 446 (518 rows, 54 schools + 3 unnamed) | 14 s | 6 | 445/445 (29 Census) | A345 B73 C24 F3 |

McKinney calibration: 90-day grades A 77% / B 16% / C 5% / F 0.7%; the
three F's are text-forced by real roach observations ("Observed roach on
wall in dish area", "Observed roaches in dry storage", "Observed live roach
near dredging station"), none by score; demerits median 3, max 24 (risk
70). 1452 violation lines, 78 (5%) with the neutral no-observation phrase,
0 regex pest/sewage hits from titles. Not a mapping problem.

Sample records are in /tmp/{mckinney,lewisville,denton}_30d.json (review
output: newest inspection per restaurant).

## Not done / for the integrator

- README/CLAUDE.md/.github not touched (integrator). Slugs: mckinney,
  lewisville, denton (and 'dfw' runs all three).
- Backfill: McKinney has history to 2017-10 (17k inspections, ~9 pages of
  2000 in --mode full); Denton to 2010 (~3.1k distinct permit+date pairs,
  2 pages); Lewisville only the trailing 365 days (~1.1k rows), so its
  backfill must run soon and every week.
- Jev: McKinney lines carry real observations; Lewisville/Denton have none.
