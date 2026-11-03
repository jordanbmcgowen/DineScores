# Maricopa County (Phoenix) fetcher — progress log

Branch `feat/maricopa`, worktree `/home/user/wt-maricopa`. Section header in
`dinescores_pipeline.py`: "PHOENIX / MARICOPA COUNTY, AZ (envapp.maricopa.gov + county ArcGIS)".

## Status
- [x] Recon read (contract, wiring, tests, probe report, capture NOTES.md).
- [ ] Fetcher code (constants, pacer, transport, parsers, fetch_maricopa).
- [ ] Parsers verified against the captured pages.
- [ ] Live test-mode run (25 records).
- [ ] Live 14-day bounded run (weekly-equivalent cost measurement).
- [ ] Fixtures + tests/test_maricopa.py; whole suite green.
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
(none yet)

## Open questions
(none yet)
