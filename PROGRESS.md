# feat/tyler-portals — progress log

Georgia (fetch_georgia) + Louisiana (fetch_louisiana) on one shared Tyler
"publicportals" JSON client. Section header in dinescores_pipeline.py:
`GEORGIA + LOUISIANA (Tyler "publicportals" JSON APIs)`.

## Status
- [x] Recon read (contract/wiring/tests, both probe reports, capture NOTES)
- [x] Baseline suite green in the worktree (274 tests, relay tests, py_compile)
- [x] Design probe launched (GA county EH phones for the Atlanta metro chip +
      GNR check; LA permit-type suffix vocabulary) -> scratchpad/tyler_probe/out
- [ ] Shared client + helpers (URL builder, paging, invariant check, retries)
- [ ] fetch_georgia (test mode run)
- [ ] fetch_louisiana (test mode run, printable narrative parser)
- [ ] Fixtures (tests/fixtures/georgia, tests/fixtures/louisiana)
- [ ] tests/test_georgia.py, tests/test_louisiana.py
- [ ] Live runs: --mode test (25) and --mode full --since-date (~14 d) per state
- [ ] Final cleanup + report

## Design decisions (so a successor does not re-derive them)
- Search URL: JSON of base64 values, `keyword` always present (LA 500s without
  it), '/' -> '%2F' in the JSON, then percent-encoded with only '%' safe.
- Windows: one single-day window per day from the lookback start to today,
  oldest first, paged (5 rows/page) until the server returns []. GA bounds are
  inclusive (from=to=day); LA bounds are strictly exclusive (from=day-1,
  to=day+1). since_date=None (test mode) = one open window from the history
  floor, newest first, stopped by `limit`.
- Ordering invariant per day: every row dated that day, decoded numeric ids
  strictly ascending, distinct; a violated day is re-read (TP_DAY_REREADS),
  then the group RAISES (fail closed).
- Detail (inspectionsData) oldest-first in a 2-worker pool; a failed facility
  drops its in-window inspections + _hold_freshness(label, day, key=facility
  id); deadline cuts hold with counted=False and _set_group_status 'skipped'.
- GA: official score -> original_score/risk_score; severity from the citation
  suffix (p)/(pf)/(c), highest wins, Points fallback (9 -> P, 4 -> PF, else C);
  text = item title + inspector observation, CA/code boilerplate stripped;
  'Temporary' purpose (festival booths) dropped; metro 'Atlanta' from the row's
  county EH phone (the only county signal in a row).
- LA: no score -> calc_risk_score over Critical (priority) / Non-Critical
  (core); clean marker {'0': ['None']} -> violations [] risk 100; narrative
  from the printable HTML (titles only on failure, never a hold); permit-type
  suffix rule TBD from the probe; metro from the Parish column.

## Live results
(none yet)

## Open questions / follow-ups
(none yet)
