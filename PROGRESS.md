# Williamson County (WCCHD) fetcher — progress log

Branch `feat/williamson`, worktree `/home/user/wt-williamson`. Section header in
`dinescores_pipeline.py`: "WILLIAMSON COUNTY, TX (WCCHD inspection-scores zip snapshot)".

## Status: FIX PASS — reviewer findings fixed, 325 tests green; live re-verification in progress

## Done
- [x] Read the recon docs (contract / wiring / tests), the probe report and the capture
      directory; analysed the real 2026-10-29 snapshot (12,145 permits, 6,734 inspections).
- [x] `fetch_williamson` + helpers in the stub section (constants `WM_*`, `_wm_title`,
      `_wm_city`, `_wm_address`, `_wm_out_of_scope`, `_wm_read_feed_info`, `_wm_download`,
      `_wm_unzip`, `_wm_rows`, `_WmDeadlineCut`). Edits confined to that section (one diff hunk).
- [x] Fixtures: `tests/fixtures/williamson/{businesses.csv,inspections.csv}` (byte-verbatim
      rows for 19 chosen permits, no BOM, CRLF — the real file has no BOM) and the real
      `feed_info.xlsx` (9,354 B).
- [x] `tests/test_williamson.py`: 42 no-network tests. Whole suite: 316 tests OK;
      `node tests/test_relay.mjs` PASS; `py_compile` OK (Python 3.13 locally; 3.11 syntax only).
- [x] Live runs from this container (2026-11-03, see "Live results").

## Design decisions (also in the section comment)
- One GET of the zip per run (258,656 B, ETag/Last-Modified logged); refuse (RuntimeError) on a
  missing member, unreadable feed_date, < 1000 inspection rows, < 2000 business rows, > 1%
  unparseable rows, non-zip body, or any non-200/429/5xx status (403/404 = the object moved).
  429/5xx/connection errors retry 3x with 10 s doubling delays, never past the deadline.
- Official score -> original_score and risk_score (clamped 0..100); score <= 0 dropped with a
  warning; IHH -> results 'Imminent Health Hazard - closed', risk capped at 55 (F).
- Scope by permit code: drop -TFP, -MF, -SAN, school kitchens (-ISD, -SFEC, -FFES*, `<city>S-`
  prefixes) and the district's 'TEST' / 'Test Project …' / 'TEST Preopen …' permits; keep
  -FFEC (commissary kitchens) and -UNP.
- Normalisation: whitespace collapsed; city stripped of trailing punctuation / glued zip and
  title-cased; ALL-CAPS names/addresses title-cased with possessives, ordinals and
  road/company codes protected; mixed case kept as published; suites kept as published.
- Output newest-first; `limit` truncates (no lag note then); `_note_source_lag('Williamson
  County', newest)` after a complete read. Deadline before/while downloading ->
  `_set_group_status('Williamson County', 'skipped', …)` and `[]`.
- No per-record detail phase exists, so no `_hold_freshness` is ever needed (nothing can be
  dropped transiently: the whole file is one atomic object).
- No Jev registration needed: no violation lines; unregistered label = 'official' / 'free'.

## Live results (2026-11-03, from this container through the proxy)
- Offline parse of the captured 2026-10-29 zip: 5,343 records from 6,734 rows (1,250
  school-kitchen rows, 33 test-permit rows, 11 mobile-unit rows, 86 orphans, 8 negative
  scores, 3 exact duplicates dropped); grades B 1944 / A 1513 / C 1246 / F 640 (12.0% F,
  57 of them IHH closures); no duplicate source_ids; 1,744 distinct addresses.
- `--mode test --cities williamson`: exit 0, 10 s wall, 1 GET (258,656 B, etag
  "64002b2b6ed04fbe80149ce346bd88fc", last-modified 2026-10-29); 25 inspections (newest,
  2026-10-01..2026-10-28) = 21 restaurants; geocoded 23/25; grades B 12 / C 7 / A 1 / F 1.
- `--mode full --since-date 2026-08-05 --cities williamson` (90 days): exit 0, 31 s wall,
  1 GET; 138 raw rows -> 117 inspections (6 orphans, 13 school, 2 test dropped) = 93
  restaurants in 11 towns; geocoded 109/117 (93%); every coordinate inside the county box
  (30.45-30.85 N, -97.93..-97.39 E); grades B 41 / C 27 / A 17 / F 8; results Pass 85 /
  Fail 6 / IHH 2.
- Census batch alone on 50 distinct window addresses: 40/50 (Public_AR_Current), 41/50 with
  the Public_AR_Census2020 pass; misses are Texas highway designations ('16219 N RM 620 Rd',
  '3701 W SH 29, Building 17', '2700 N IH 35', '560 W US 79, Ste. 950', '6020 FM 1660, C')
  and new subdivision streets in Leander.

## Open questions / follow-ups for the integrator
- School kitchens are 18.6% of the feed's rows; dropped per the scope rule (Tennessee/Omaha
  precedent). Remove the 'school kitchen' entry from `WM_OUT_OF_SCOPE` to keep them.
- Census misses are mostly 'RM/FM/SH/IH/US <number>' roads; expanding those in
  `geocode_census_batch`'s match_address (outside this section) would likely lift the rate.
- Feed cadence is unverified (one observation); the lag note explains a stale feed.
- Backfill: `--mode full --since-date 2021-12-01 --cities williamson` ships 5,343 inspections
  / 1,744 addresses; ~20% miss the Census batch and Nominatim is capped at 150 per call, so
  raise `nominatim_fallback_cap` for that one run (or accept ~200 coordinate-less rows).

## Fix pass (2026-11-03) — two adversarial reviews
Findings reproduced against the captured 2026-10-29 snapshot (`python3 -I` analysis) before fixing.
- FIXED (blocking, both reviewers) orphan rows lost for good: in-scope orphans are 24 rows / 13
  permits (0.44%); every 2026-numbered one (27-85 days old at the snapshot) is above businesses.csv's
  newest 2026 sequence (1215), i.e. a permit not yet issued; the older ones (2023-1869-FFE, CP-1011,
  ...) are permanently orphaned. Fix: `WM_REREAD_DAYS` (180) — in weekly mode the window never starts
  later than 180 days ago (one GET anyway; merge and D1 upserts idempotent; ~360-820 rows/week
  re-shipped at 2026/2025 volumes). The suggested `_hold_freshness` was REJECTED as the mechanism:
  `_record_group` caps the group's `latest` at the hold date and the report flags age > 21 days
  STALE, so an orphan hold would raise a false STALE flag for three runs and then give the record
  up before the observed resolution times. Also covers Reviewer B's late-publication concern.
- FIXED (should_fix) no guard on businesses.csv coverage: `WM_MAX_ORPHAN_SHARE` (5%) over the whole
  file's food-establishment rows raises; `WM_MIN_BUSINESS_ROWS` 2000 -> 6000 (the registry only grows).
- FIXED (should_fix) test permits at the district's office: `WM_TEST_ADDRESS_RE` drops every permit
  at 355 Texas Ave (all 35 are internal: 'TEST 355', 'ABC Food', 'WCCHD', 'Food Sampling', a SNAP-Ed
  station). -FFER/-FFED/-FFEX left in scope: no real rows, and a remodel permit's inspection is the
  same restaurant.
- FIXED (should_fix) unknown result shipped on its score: results are matched upper-cased against
  Pass/Fail/IHH; anything else is an unparseable row (dropped, WARNING) and more than 1% of them
  refuses the snapshot.
- FIXED (nits) `errors='replace'` decoding -> strict UTF-8, cp1252 fallback with a warning, anything
  else refused; scope check now runs before the join so school orphans are not counted as join
  failures; state 'Texas' -> 'TX' (two-letter codes only, else TX).
- REJECTED (Reviewer B should_fix) 'williamson' missing from refresh-data.yml --cities: the
  integrator's step (.github/ is outside this branch's region); listed as a follow-up.
- Tests: 51 Williamson tests (9 new); whole suite 325 OK; relay PASS; py_compile OK.
