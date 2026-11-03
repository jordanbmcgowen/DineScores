# feat/harris — fetch_harris (Harris County Public Health FSP portal API)

Checkpoint log for a machine that restarts without warning. Read this first
when resuming; never start over.

## Status
- [x] Recon read (contract, wiring, tests, probe report, captures); live probe
      of the search endpoint from this container: HTTP 200, combined >=/<=
      DevExtreme filter + asc/desc sort + skip paging all honoured server-side;
      oldest lastCompletedInspectionDate 2022-02-01; 552 establishments
      inspected in the 14 days to 2026-11-03.
- [ ] fetch_harris written (section "HARRIS COUNTY, TX" in dinescores_pipeline.py)
- [ ] test-mode live run (25 records)
- [ ] 14-day live run, results below
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
- Types: ship Routine Inspection, Complaint, Food Borne Illness always;
  Permitting / Non-permitted / Follow-up only once the permit has an earlier
  operating-type inspection on record (annual renewal inspections of
  operating places); the first permit visits of a new permit are pre-opening
  approvals and are dropped. Consultation dropped. (Permitting is NOT only
  pre-opening: permits are annual and both long-lived sample permits had a
  scored 'Permitting' inspection every year.)
- Scope: type must be 'Fixed'; serviceType schools, day care, long-term care,
  hospital, detention, shelter, food pantry/warehouse, MFU areas/commissary,
  mobile units, farmer's market vendors are skipped.
- Breaker: HARRIS_BREAKER_TRIP (3) consecutive exhausted requests ->
  _HARRIS_BLOCKED, every further request returns None at once.

## Live results
(none yet)

## Open questions
- Does a score-100 inspection ever carry violation rows? (probe running)
- HCPH's pest item title wording vs PEST_EVIDENCE_RE (probe running)
- GitHub-runner reachability of secureweb.hcphtx.org is unverified; the
  relay allowlist (functions/api/relay.js ALLOWED_HOSTS) would need that host.
