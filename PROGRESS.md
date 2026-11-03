# feat/hedgerow — progress log

Fetchers for Hedgerow Software "Hedgehog" public portals: Pima County AZ
(group 'Pima County'), Lubbock TX ('Lubbock'), Mesquite TX ('Mesquite').
Section header in dinescores_pipeline.py: "HEDGEROW PORTALS: PIMA COUNTY AZ,
LUBBOCK TX, MESQUITE TX". Scaffold commit 75d73d8.

## Done
- Read recon docs (contract, wiring, tests), Pima + Lubbock probe reports and
  captures, Mesquite capture files (read-only).
- Live: Mesquite Food Safety program id = 6ee6f958-ea40-45cb-bb87-ba2e06bd1257
  (GET siteInfo?locale=en, menuType 5 'Food Safety'); API base
  https://msqt-app-prod.hedgerowsoftware.com/api/v0/portal/

## Design decisions (so far)
- One shared client `_hh_*` (requests via dp.requests, sleeps via dp.time),
  Chrome UA, retries with backoff on 429/5xx/timeouts, stops at deadline.
- Pima: list sorted by id (atomic, row-count floor), select facilities by
  lastInspectionDate >= since, inspectionDetails per facility (oldest first),
  inspectionResults per inspection; score computed from cannedComments
  severity; rating -> results; Fail/IHH/Probation cap the risk score (F).
- Lubbock: list sorted lastInspectionDate desc, pageSize 500, page while the
  last row >= since; official hazardScore; drop TF15 Temporary Food, School,
  Mobile Food Unit, empty siteAddress.
- Both: drop mobile units (type says mobile/push cart/vehicle; pinned at a
  commissary otherwise — 84 Pima units share one address).
- Holds: unread facility list -> _hold_window_start(source, since, key=fid);
  unread inspectionResults -> _hold_freshness(source, date, key=iid);
  deadline cuts counted=False + _set_group_status(group, 'skipped').

- Live Mesquite (2026-11-03): list = all 689 rows in one response (paging
  ignored), facilityType + lastInspectionDate hidden, hazardScore official
  70-100 (6 null), 268/689 without coordinates, inspectionResults 200 (the
  capture's 500 was not universal), observationComment always null (titles +
  short rule names only), inspection/{id}/details fully redacted. The server
  does NOT honour a sort on the hidden lastInspectionDate (newest dates rise
  and fall down the list) -> every facility's inspection list is read per run.
- Code: shared client + fetch_pima/fetch_lubbock/fetch_mesquite spliced into
  the pipeline section (compiles). Fixtures built: tests/fixtures/hedgerow/
  (16 files, 67 KB, trimmed from the 2026-11-02/03 captures).

- Mesquite public portal found via the city site search:
  https://mesquite.hedgerowsoftware.com/ (SPA + same API, proxied to
  msqt-app-prod); fetcher now uses it (base + source_url), route scheme
  #/{program}/disclosure/facility/{fid}/inspection/{iid} confirmed in all
  three bundles (DisclosureProgram -> [routeParam, 'disclosure']).
- tests/test_hedgerow.py: 51 tests; full suite 325 OK, node relay PASS,
  py_compile OK (2026-11-03).
- Live test mode Lubbock: 25 records / 18 facilities / 48 requests / 30 s,
  25/25 with coordinates (oldest-first from the 2023-11 floor).

- Live test mode (2026-11-03): Pima 25 records / 9 facilities / 35 requests
  / 25 s (5,177 listed; 630 mobile + 109 never-inspected skipped); Mesquite
  25 / 6 facilities / 32 requests / 17 s; all with coordinates.
- Live window runs: Lubbock since 2026-10-20: 72 records / 72 facilities /
  145 requests / 60 s, 72/72 coords, scores 73-100 (A 52 B 16 C 4 by score).
  Pima since 2026-10-10 (portal newest 2026-10-17): 113 records / 112
  facilities / 99 s, 113/113 coords, A 98 B 11 F 3 by score (two Fail caps,
  one Probation), 4 genuine text F's (dead roaches, rodent droppings).
- FOUND + FIXED: Lubbock's TFER item title 'No evidence of Insect
  Contamination, Rodent, Other Animals' tripped the regex text-F on every
  citation (10 false F's in 72 records: fly tape, door gaps, ceiling tiles).
  The negation is now stripped (_HH_NEGATED_TITLE_RE); re-running Lubbock.

- Fix shipped: the Texas pest criterion now ships as 'Pest control:
  <observation>' (_HH_PEST_TITLE_RE): the form title 'No evidence of Insect
  Contamination, Rodent, Other Animals' tripped the regex text-F on its own
  and, with 'Rodent' left in a heading, across the '|||' line join. Lubbock
  window re-run (final code): 72 records, grades by score == by score+text
  (A 52 B 16 C 4), 0 false F's (was 10). jev_reader edit reverted (no longer
  needed; zero edits outside the section).
- Address fixes from Mesquite data: a 2-letter state in the city slot
  ('3220 Gus Thomasson Mesquite 101, TX 75150') and a street suffix before
  the comma ('1700 Rodeo Dr, TX 75149') both mean "no city given".
- Mesquite window (first run, pre-fix code): 136 records / 689 facility
  lists / 826 requests / 296 s fetch, 310 s wall incl. Census geocoding;
  135/136 with coordinates afterwards (1 unresolved); scores 84-100
  (A 127 B 9 by score). Re-running on the final code.
- Suite: 326 tests OK (exit code checked), node tests/test_relay.mjs PASS,
  py_compile OK.

## Next
- Mesquite re-run numbers; final structured report.

## Open questions
- Pima 'Needs Improvement' computes to a B (e.g. 93 for P+C+C); only Fail /
  IHH / Probation cap the score. Owner policy call.
- The shared regex misses 'Accumulation of rodent feces' (evidence word after
  the pest word); Jev's hazard decision covers it in the weekly run.
- Mobile units dropped (Pima 630, Lubbock 146) — pinned at commissaries
  otherwise; flip HH_MOBILE_TYPE_RE if the owner wants them.
