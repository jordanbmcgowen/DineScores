# feat/dfw-holes progress log (McKinney, Lewisville, Denton)

Branch: feat/dfw-holes. Section in dinescores_pipeline.py:
"DFW CITIES WITHOUT A PORTAL ON MHD: McKINNEY, LEWISVILLE, DENTON".
If you are a successor agent: read this file, `git diff`, run
`python3 -m unittest discover -s tests` from the worktree root, continue.

## Plan (decided 2026-11-03 after reading the recon docs + probe captures)

- Irving stub deleted (blocked source), fetch_denton added; main() tuple
  entry (('irving','dfw'),'Irving',fetch_irving) -> (('denton','dfw'),
  'Denton', fetch_denton).
- McKinney: ArcGIS MapServer layer 4 (inspections) via _arcgis_query_all
  with where=actual_end_date >= DATE 'YYYY-MM-DD' (verified), count check
  (returnCountOnly) -> raise if rows < count; coords: geometry (outSR 4326)
  -> POINT_X/Y -> layer 0 permits join on link_number -> None (main
  geocodes). total_score = DEMERITS: 47 item columns hold exactly {0,
  weight}; TX form weights verified live on 131 rows: items 1-20 = 3 pts
  (priority), 21-34 = 2 (priority_foundation), 35-47 = 1 (core).
  risk_score = calc_risk_score(violations), original_score = demerits,
  results = "<n> demerits (<status>)". Violation text = the inspector's
  "#NN." block from custom_comments (Violations: section only; the header
  carries the CFM's personal name and is never used). Drop status 'Void'
  and purpose '3 - Pre-Opening'. source_url = per-inspection REST f=html
  page (verified 200). Jev: register 'McKinney Health Inspections' in
  SOURCE_TIER_SCORE_SOURCES + SOURCE_TIER_SOURCES.
- Lewisville: one GET (HealthInspectionScores_byDate, DateBegin/DateEnd
  ISO). TLS: host omits the GoDaddy G2 intermediate -> verify=
  data/certs/lewisville_ca_bundle.pem (certifi + intermediate, header has
  provenance). Verified 2026-11-03: default verify fails, bundle 200 in
  0.6 s; eservices host does not proxy the API; a window starting
  2024-01-01 returns the trailing 365 days (1146 rows), so no floor needed.
  Scores only; 'NO SCORE'/'PASS' rows dropped (unscored).
- Denton: CKAN datastore_search_sql; verified shapes: COUNT over a
  DISTINCT subquery with WHERE, top-level SELECT DISTINCT ... WHERE
  "HealthScoreDate" >= 'YYYYMMDD' ORDER BY ... DESC LIMIT/OFFSET paging
  (100+100+14 = count 214 for 90 days), MAX(). HTML/403 body = WAF block ->
  raise. Types are all food types (no schools/daycare/mobile) -> nothing
  to filter. Scores only, RESULT text in results, letter NOT copied
  (Denton cutoffs 94/87/80).

## Done
- [x] Recon read; live probes of the uncertain shapes (above).
- [x] data/certs/lewisville_ca_bundle.pem created.

## Next
- [ ] Write the section (helpers + three fetchers), main() tuple edit,
      jev_reader registration + test_policies.
- [ ] Fixtures + tests (tests/test_mckinney.py, test_lewisville.py,
      test_denton.py).
- [ ] Live runs: --mode test and 30-day/90-day windows; record below.

## Live results
(none yet)

## Open questions
- McKinney: ~half of the cited items have no "#N." block by a strict
  line-start regex; analysing the comment formats before writing the parser.
