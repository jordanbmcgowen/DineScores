# feat/harris — fetch_harris (Harris County Public Health FSP portal API)

Checkpoint log for a machine that restarts without warning. Read this first
when resuming; never start over.

## Status
- [x] Recon read (contract, wiring, tests, probe report, captures); live probe
      of the search endpoint from this container: HTTP 200, combined >=/<=
      DevExtreme filter + asc/desc sort + skip paging all honoured server-side;
      oldest lastCompletedInspectionDate 2022-02-01; 552 establishments
      inspected in the 14 days to 2026-11-03.
- [x] fetch_harris written (section "HARRIS COUNTY, TX" in dinescores_pipeline.py)
- [x] test-mode live run: 25 records, 21 establishments, 40 requests, 8 s, 24/25 geocoded (Census)
- [x] 2026-11-03 resume: listing pages until a short page REACHES totalCount
      (or an empty page), unique count checked against totalCount (DESC order
      kept: a mid-read update shifts rows forward past the cursor, never
      backward); a 404/410 on an inspection list skips + holds that
      establishment (key=documentId) instead of failing the window; truncated
      computed from the raw listing; breaker state reset per fetch.
- [x] 14-day live run + analysis pass done 2026-11-03 03:09-03:18 UTC, results below
- [x] fixtures captured + scrubbed under tests/fixtures/harris/ (39 files; the
      11 older item lists captured 2026-11-03 03:05 for the since=None tests)
- [x] tests/test_harris.py: 53 tests green (python3 -m unittest tests.test_harris)
- [x] DECIDED: HARRIS_LC_LAG_DAYS = 28 lookback margin on both the search
      filter and the kept inspections (evidence under "Completeness"); a cut
      still holds at the original since. 55 tests.
- [x] whole suite green under 3.11 + 3.13 (329 tests), node tests/test_relay.mjs
- [x] FINAL live re-run with the margin done 2026-11-03 03:31-03:45 UTC (below)
- [x] structured report returned; branch ready for the integrator (squash)
- [x] fix pass after two adversarial reviews (section below): all findings
      fixed or explicitly rejected, re-verified live

## FIX PASS (2026-11-03, after two adversarial reviews) — state
Findings accepted — ALL FIXED in commit 9361e60 (suite 340 tests green,
relay passes, py_compile ok; live test mode re-run 2026-11-03 04:34 UTC:
exit 0, 14 s, 25 records / 21 establishments / 40 requests, 23/25 geocoded,
group ok, no contradiction warning fired). 14-day re-run with the fixed code
(`--mode full --since-date 2026-10-20`, 04:34-04:48 UTC): exit 0, 809 s wall
= 474 s fetch + geocoding; 1,861 establishments (4 pages), 1,540 in scope,
1,495 records from 2,837 requests, 1,381 restaurants, grades A 964 / B 393 /
C 21 / F 3, risk 55-100, original_score never None, 1,099 (80%) with
coordinates (Nominatim read timeouts + its 150 cap left 201), 30 cities, all
'TX', no PII (the one '@' is "Golden Chick @ FM 1960"). Same numbers as the
pre-fix run: the fixes are inert on the normal path. New checks on live data:
0 score<100 inspections without an OUT item; 3 of 1,540 inspection lists
hold nothing within 30 days of the search field (held). Lists-only probe of
the same 1,540 (04:58-05:02 UTC, 1,544 requests, 0 failures): field - newest
listed row = 0 days on 1,501, within 3 days on 36, 3 contradictions (gaps 48,
79, 162 days; each establishment Active, permitApprovalDate or
nextInspectionDate moved on the field's day, the list unchanged: an
inspection recorded but not yet published). DECIDED: keep the per-record
counted hold (key=documentId) — it re-reads them for up to 3 runs, then
gives up; a gap under the 30-day tolerance is re-read by the 28-day margin
anyway. Cost: a held establishment pulls the window start back to its field
date (~40 establishments listed per day of pull-back) for up to 3 runs. The
held ids are now in the warning. All done; suite 340 green (3.11 + 3.13),
relay passes, py_compile ok.
- [x] A1/B1 listing-completeness check dead with the weekly limit (50000):
      key it on `bounded = limit reached`, not on limit being set
- [x] A2 a score<100 inspection whose item list read as [] (or rows with no
      title/OUT) ships clean: treat as unread -> drop + counted hold; mass
      (>=20 scored reads, >50% empty) -> RuntimeError
- [x] A3 every list [] -> green 'empty': lists must hold a row within
      HARRIS_LIST_AGREE_DAYS (30) of the search field (live probe 2026-11-03:
      200 establishments, gap -1..2 days, 0 empty); contradicting ones held,
      mass (>=20 checked, >50%) -> RuntimeError
- [x] A4 search paging unbounded if the server stops honouring skip: raise on
      a page that adds no establishment and on paging a page past totalCount
- [x] A-nit deadline inside the phase-1 retry ladder -> 'skipped' not 'error'
- [x] B2 state[:2] turns 'Texas' into 'TE': only an exact 2-letter code is
      kept, anything else is 'TX'
- [x] B3 one exhausted inspection list fails the whole window every week:
      skip + counted hold (key=documentId) like the 404 path; more than
      HARRIS_LIST_FAILURES_MAX (5) in a run raises
- [x] B4 jev FORM_TITLE_PHRASES: add the five Harris TFER titles whose regex
      fallback is wrong (5, 29, 39, 47, 56); sewage 52 and pest 38 stay out
      (policy) — edit outside the region, listed in the report
Rejected: A5/B 'runner reachability' (not verifiable here; fails to a red
'error'/'blocked'); nit 'breaker-tripping record counted' (deliberate);
nit 'score-100 skip' (documented only); nit 'PROGRESS.md at root' (the brief
requires it); approvalStatus: policy documented, not changed.

## Design decisions (so a successor does not re-litigate them)
- Group 'Harris County', source 'Harris County Public Health', source_id
  'hcph_<inspection number>', state 'TX', metro '' (a 'Houston' chip without
  the City of Houston's own records, which carry '', would mislead; recommend
  the integrator sets both to 'Houston' together).
- Phase 1: POST api/establishment/search, filter lastCompletedInspectionDate
  >= since (ISO T00:00:00), sort by that date DESC + documentId, take 500,
  skip paging, dedupe documentId; a failed page or a short listing RAISES.
- Phase 2: GET get-inspections-for-establishment per establishment (3 workers);
  any exhausted failure RAISES (list phase, no partial window) unless the
  breaker tripped / deadline passed -> stop, _hold_window_start(since),
  _set_group_status blocked/skipped, return [].
- Phase 3: GET get-violations-for-inspection per in-window inspection,
  oldest first; a failure drops that inspection + _hold_freshness(counted=True,
  key=number); breaker/deadline drops the rest with counted=False.
- Score: official 'score' (== 100 - sum numberOfDemerits) copied to
  original_score and risk_score; severity from violationGroup P/F/C; text
  "<violationNumber>. <violationDescription>" bare (so jev FORM_TITLE_PHRASES
  / CITED_PEST_ITEM_TITLES exact-title matching works).
- Types (REVISED after the 14-day probe: 587 inspections = Permitting 225,
  Routine 155, School Inspection 64, Pre-opening 40, Complaint 31,
  Consultation 21, Follow-up 17, Food Borne Illness 15, Daycare Kitchen 14,
  Follow-Up Inspection 3, SV Follow-up 1, Zero/Zero 1): ship Routine
  Inspection, Complaint, Food Borne Illness, Permitting (the ANNUAL permit
  inspection of an operating place, 0-3 months before expiry; for many
  low-risk permits the only inspection they ever get); ship Follow-up /
  Follow-Up Inspection / SV Follow-up once the permit has an operating-type
  visit on record (a Follow-up after a Pre-opening is a pre-opening re-check);
  drop Pre-opening (explicit pre-opening approval type), Non-permitted
  (pre-permit visit, 0 in window), Consultation, School Inspection, Daycare
  Kitchen Inspection; unknown types (Zero/Zero) dropped with a warning.
- Scope: type must be 'Fixed'; serviceType schools, day care, long-term care,
  hospital, detention, shelter, food pantry/warehouse, MFU areas/commissary,
  mobile units, farmer's market vendors are skipped.
- Breaker: HARRIS_BREAKER_TRIP (3) consecutive exhausted requests ->
  _HARRIS_BLOCKED, every further request returns None at once.

## Live results (2026-11-03, this container, plain python requests, TLS verified)
- CLI test mode (`--mode test --cities harris`): exit 0, 14 s wall; 25 records
  from 21 establishments (4 skipped: 2 Public School, 1 pantry, 1 day care),
  40 requests, 9 s fetch; Census geocoded 23/25; 18 restaurants saved.
- CLI 14-day (`--mode full --since-date 2026-10-20 --cities harris`): exit 0,
  327 s wall (143 s fetch + 184 s Census geocoding); 552 establishments
  (2 search pages, totalCount 552), 467 in scope (skipped 68 Public School,
  15 Day Care, 1 pantry, 1 Hospital); 501 in-window inspections, left out 40
  Pre-opening, 20 Consultation, 2 pre-opening Follow-up, 1 Zero/Zero; 438
  records shipped, 843 requests; geocoded 380/438 (87%); 411 restaurants;
  group 'ok', latest 2026-10-31. Cities: Houston 197 (unincorporated mailing
  city), Katy 56, Humble 47, Cypress 32, Spring 32, Tomball 10, Crosby 10,
  Seabrook 9, Bellaire 8, Waller 8, ... 23 cities.
- Analysis pass (every item list read, incl. the 65 score-100 ones): 908
  requests, 153 s. score == 100 - sum(demerits) on 431/438; the 7 others are
  each exactly 2 points ABOVE 100 - demerits (county score kept). Score-100
  inspections with items: 0 of 65 (105 checked in all) -> HARRIS_READ_CLEAN_ITEMS
  stays False. Items with 0 demerits: 0. Demerits: C 1-2, F 2-3, P 2-3; all
  2,210 items 'Out'. 45 distinct titles, longest text 100 chars, no newline;
  the only pest title ('Insects, rodents, and animals not present', 68
  citations) trips neither PEST_EVIDENCE_RE nor has_active_pest_or_sewage
  (0 regex-forced F), but detect_infractions gives it a 'pests' chip.
  Grades: A 294 / B 131 / C 10 / F 3 (2 closures capped at 55, one score 64);
  risk 55-100; original_score never None; results: '' 404, 'Permit not
  approved' 20, 'Follow-up required' 12, 'Closure required' 2; 19 same-day
  pairs (Complaint + Routine); no '<redacted>' in any record; counts
  consistent with calc_risk_score tallies.
- Permitting (224 of 501 in-window): activity Light 144 / Moderate 62 /
  Heavy 13 / Not Open 3; 218 had an earlier operating-type inspection; permit
  commence date median 284 days before (100 within 30 days: the new permit
  term starts at the renewal visit); expiry median 80 days ahead; scores
  64-100 distributed like Routine's (70-100). It is the annual permit
  inspection of an operating place -> ships. Pre-opening (40): Not Open 32,
  first-ever 29, no earlier operating-type 37 -> dropped. Consultation (20):
  18 scored 100 -> dropped.
- Completeness: lastCompletedInspectionDate == newest list date on 454/467
  in-window establishments; 13 differ (8 have the field NEWER than any listed
  inspection, 5 have an inspection 1-9 days newer than the field: Follow-up,
  Follow-Up Inspection, Pre-opening, Food Borne Illness, Consultation; both
  Approved and Pending). Out-of-window probe: of 150 establishments whose
  field is in [2026-09-05, 2026-10-20), 2 hold a Complete inspection inside
  the window (Routine 2026-10-26 with field 10-19; Food Borne Illness
  2026-10-22 with field 10-15), both still not reflected 8-12 days later.
  So the search filter alone misses ~1% of recently-active establishments'
  newest visits. Band probe (band_probe.log): of 240 establishments whose
  field is 1.5-5.5 months old, 5 (2%) hold a Complete, Approved inspection
  5-11 days NEWER than the field, still unreflected months later -> the
  field can stay stuck on the earlier visit until the next one. All 9
  unreflected visits seen (Follow-up, Consultation, Permitting x2, Food
  Borne Illness x2, Routine, Follow-Up Inspection, Pre-opening) came 5-11
  days after a reflected visit. Fix: read every window from
  HARRIS_LC_LAG_DAYS (28) before its start, search filter and kept
  inspections alike; the stuck establishment is found via its earlier visit
  and the later one ships; re-shipped inspections are idempotent upserts.
  Residual: an unreflected visit more than ~28 days after the previous
  reflected one is missed until the next reflected inspection. Weekly cost
  roughly triples (~1,200 lists + ~1,100 item lists per run).
- dateUtc - date = 0 days on all 501 (entered the day of the visit).
- FINAL CLI runs with the margin code (HEAD): test mode exit 0, 14 s wall,
  25 records / 21 establishments / 40 requests / 8 s fetch, 23/25 geocoded,
  18 restaurants (A 14, B 3, C 1). `--mode full --since-date 2026-10-20`
  (effective window 2026-09-22..): exit 0, 788 s wall = 474 s fetch + 314 s
  geocoding; 1,861 establishments (4 search pages), 1,540 in scope (skipped
  250 Public School, 58 Day Care, 4 pantry, 3 Hospital, 3 Detention, 2 Long
  Term Care, 1 School); left out 123 Pre-opening, 73 Consultation, 5
  Non-permitted, 4 pre-opening Follow-up, 2 Zero/Zero; 1,495 records from
  2,837 requests (no 4xx/5xx); 14 score/demerit mismatches (county score
  kept); Census + Nominatim geocoded 1,198/1,494 (Nominatim cap 150 left 201
  for the next run, which the margin re-fetches); 1,381 restaurants, 1,099
  with coordinates, 30 cities; latest-inspection grades A 964 / B 393 / C 21
  / F 3; risk 55-100; types Permitting 675, Routine 561, Follow-up 72,
  Complaint 47, Food Borne Illness 25, SV Follow-up 1; results '' 1,304,
  'Permit not approved' 47, 'Follow-up required' 29, 'Closure required' 1.
  Group 'ok', latest 2026-10-31. Weekly cost at this rate: ~1,500
  establishments listed, ~1,300 lists + ~1,250 item lists, ~2,600 requests,
  ~7-8 min fetch at 3 workers / 0.4 s.
- Host answers plain python requests from this container (every call 200;
  no 429/403 in ~2,300 requests this session). GitHub-runner reachability
  of secureweb.hcphtx.org is UNVERIFIED; it is not in the relay allowlist.

## Probe findings (2026-11-03, window 2026-10-20..11-02)
- score-100 inspections: 0 of 40 had item rows -> HARRIS_READ_CLEAN_ITEMS=False
- score == 100 - sum(demerits) on 446 of 453 scored inspections; 7 differ by
  1-6 points (county score kept, counted in the log)
- dateUtc == date on all 587 (no late entries); all statuses 'Complete'
- 47 distinct item titles, none trips PEST_EVIDENCE_RE/SEWAGE_ISSUE_RE; item
  38 'Insects, rodents, and animals not present' (81 citations) is NOT added
  to jev CITED_PEST_ITEM_TITLES: like King County's '... entrance controlled'
  the TFER item is also cited for door gaps (jev_reader precedent)
- groups C 1373 / F 526 / P 499; demerits C 1-2, F 1-3, P 2-3; all 'Out'
- 13 of 552 window establishments carry coordinates

## Open questions
- GitHub-runner reachability of secureweb.hcphtx.org is unverified; the
  relay allowlist (functions/api/relay.js ALLOWED_HOSTS) would need that host.
