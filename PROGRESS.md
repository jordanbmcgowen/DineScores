# Maricopa County (Phoenix) fetcher — progress log

Branch `feat/maricopa`, worktree `/home/user/wt-maricopa`. Section header in
`dinescores_pipeline.py`: "PHOENIX / MARICOPA COUNTY, AZ (envapp.maricopa.gov + county ArcGIS)".

## Status
- [x] Recon read (contract, wiring, tests, probe report, capture NOTES.md).
- [x] Fetcher code (constants, pacer, transport, parsers, fetch_maricopa) — commit 6ab4483.
- [x] Parsers verified against every captured page (4 permit pages, 5 detail pages, not-available page).
- [x] Live test-mode run (25 records): see Live results.
- [x] Live 14-day bounded run (weekly-equivalent cost measurement) — see Live results.
- [x] Fixtures + tests/test_maricopa.py (44 tests); whole suite green (318 tests, node tests/test_relay.mjs, py_compile) at 01:14 UTC; also green under /usr/bin/python3.11 (CI's version) with PYTHONPATH pointed at the 3.13 requests package.
- [ ] Final cleanup + report.

## Design decisions (so a successor does not re-derive them)
- Group 'Maricopa County', source 'Maricopa County Environmental Services',
  source_id 'mcesd_<permitId>_<inspectionId>', metro 'Phoenix', state 'AZ'.
- Captcha-free path only: zip inventory (POST /Search/Results) -> permit
  pages (GET /Permit/PermitResults/{id}, marker 'N Inspections Retrieved')
  -> detail pages (GET /Inspection/{permit}/{insp}, div#Violations + header
  labels). The reCAPTCHA weekly report is never touched.
- Identity from the envapp concat 'STREET CITY AZ ZIP' with a static
  vocabulary of multi-word Maricopa place names (ArcGIS City has typos:
  AONVDALE, PHEONIX, GELNDALE) — ArcGIS is used for coordinates only.
- Skip-known: (make_restaurant_id, date) pairs in SKIP_KNOWN_INSPECTIONS
  (populated run-wide by the weekly workflow from data.js) skip the detail
  fetch, so the overlap days cost nothing.
- Inventory run-to-run count comparison SKIPPED: no sane store (the
  freshness file is owned by _record_group); a static floor
  (MCESD_MIN_FOOD_PERMITS) catches a catastrophic inventory instead.

## Live results
- 2026-11-03 01:03 test mode (`--mode test --cities maricopa`): 25 records in
  45 s wall, 53 portal requests (8 zip searches -> 907 food permits; 20 permit
  pages in 3 s; 25 detail pages in 39 s with 2 workers), 25/25 with ArcGIS
  coordinates, 0 unread. Grades over the 17 restaurants: 17 A (scores 95-100);
  results: Grade A 9, Not Participating 4, blank (advisory) 2, Grade B 1,
  Re-inspection 1. Violation texts carry title + observation, none trips the
  pest/sewage regex.
- 2026-11-03 01:04 started the 14-day weekly-equivalent run in the background:
  `--mode full --since-date 2026-10-20 --cities maricopa --output /tmp/maricopa_14d.json`,
  log at /tmp/claude-0/-home-user/7e1d0e8e-72d4-5821-aacd-ab7518c42dd1/scratchpad/live/maricopa_14d.log
  (pid 1081). If this file says nothing more about it, the machine restarted:
  check the log, re-run if needed.
  - Inventory: 21,722 food permits from 25,831 rows over 403 zip searches in
    102 s (2 workers, 4 req/s).
  - Listing: 21,722 permit pages, 0 unread, 3,131 s (6.9 pages/s at the 7/s
    pacer, 4 workers), 1,692 inspections dated >= 2026-10-20. NEWEST ROW ACROSS
    ALL PERMITS: 2026-10-29 (5 days before the run) -> the county posts late,
    so MCESD_POSTING_LAG_DAYS (21) widens every window; see the commit after
    f8bf4f4. The margin's value is to be confirmed from the per-date histogram.
  - Details: 1,677 pages read in 2,616 s (0.64 pages/s, 2 workers, 0.5 s delay);
    15 dropped 'unread' (parser returned None; ids not logged by that build —
    diagnostic rescan find_unread.py launched 02:50 UTC, report at
    scratchpad/live/unread_report.json). Whole run 01:04 -> 02:44 UTC
    (100 min). One proxy tunnel reset mid-run, absorbed by the retry ladder.
  - ArcGIS points: 150-id IN(...) chunks answered 404 (URL too long); Census
    geocoding filled in: 1,666/1,677 records with coordinates, 11 without.
    FIX NEEDED: shrink MCESD_ARCGIS_CHUNK (75) — test for the where length.
  - Output: 1,677 records / 1,441 restaurants; grades (latest per restaurant)
    A 1,389 / B 39 / C 3 / F 10 (9 text-forced by named pests, 1 by score 69);
    scores 69-100, 826 at 100; results Grade A 686, Not Participating 393,
    Grade B 111, A+ 79, Re-inspection 62, blank 46, ...; purposes Routine
    1,147, CI Routine 177, Re-Inspection 71, Advisory 45, 'Food AMC Trainings' 1
    (a training visit shipped as an inspection — FIX: keep purposes containing
    'Inspection' only). Cities: Phoenix 498, Mesa 203, Scottsdale 131, Chandler
    91, Gilbert 83, Tempe 82, Glendale 81, Queen Creek 45, ... (multiword
    names split correctly: Sun City 18, Sun City West 11).
  - Per-date counts Oct 20..29: 170,174,177,123,6,4,149,228,208,202 then NOTHING
    dated Oct 30-Nov 3: a sharp ~5-day publication embargo, no visible taper.
    MCESD_POSTING_LAG_DAYS=21 kept as insurance against a late tail that one
    run cannot see (self-healing alone tolerates ~6 days of lag worst case).

## Open questions
- Posting lag: measured 5 days at the newest end (newest row 2026-10-29 on
  2026-11-03 across all permits). The spread (how many days until a date is
  'full') comes from the 14-day run's per-date histogram; MCESD_POSTING_LAG_DAYS
  must exceed it.
- Detail-date vs row-date: the ROW date ships (commit after a9e94f7); a
  mismatch is only logged. Both come from the same county record.
