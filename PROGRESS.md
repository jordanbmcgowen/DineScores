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
- [ ] 14-day live run + analysis pass (scratchpad/harris/run_live.sh ->
      live_chain.log, test_run.log, run_14d.log, analysis.log), results below
- [ ] fixtures captured + scrubbed under tests/fixtures/harris/
- [ ] tests/test_harris.py green; whole suite green
- [ ] final cleanup + structured report

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

## Live results
(none yet)

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
