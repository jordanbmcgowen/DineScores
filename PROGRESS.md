# Maricopa County (Phoenix) fetcher — progress log

Branch `feat/maricopa`, worktree `/home/user/wt-maricopa`. Section header in
`dinescores_pipeline.py`: "PHOENIX / MARICOPA COUNTY, AZ (envapp.maricopa.gov + county ArcGIS)".

## Status
- [x] Recon read (contract, wiring, tests, probe report, capture NOTES.md).
- [x] Fetcher code (constants, pacer, transport, parsers, fetch_maricopa) — commit 6ab4483.
- [x] Parsers verified against every captured page (4 permit pages, 5 detail pages, not-available page).
- [x] Live test-mode run (25 records): see Live results.
- [ ] Live 14-day bounded run (weekly-equivalent cost measurement).
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
  - Details: in progress at 01:58 UTC.

## Open questions
- Posting lag: measured 5 days at the newest end (newest row 2026-10-29 on
  2026-11-03 across all permits). The spread (how many days until a date is
  'full') comes from the 14-day run's per-date histogram; MCESD_POSTING_LAG_DAYS
  must exceed it.
- Detail-date vs row-date: the ROW date ships (commit after a9e94f7); a
  mismatch is only logged. Both come from the same county record.
