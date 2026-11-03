# feat/tyler-portals — progress log

Georgia (fetch_georgia) + Louisiana (fetch_louisiana) on one shared Tyler
"publicportals" JSON client. Section header in dinescores_pipeline.py:
`GEORGIA + LOUISIANA (Tyler "publicportals" JSON APIs)`.

## Status
- [x] Recon read (contract/wiring/tests, both probe reports, capture NOTES)
- [x] Baseline suite green in the worktree (274 tests, relay tests, py_compile)
- [x] Design probe launched (GA county EH phones for the Atlanta metro chip +
      GNR check; LA permit-type suffix vocabulary) -> scratchpad/tyler_probe/out
- [x] Shared client + helpers (URL builder, paging, invariant check, retries)
- [x] fetch_georgia (test mode run OK: 25 records, 33 requests, 7 s, 23/25 geocoded)
- [x] fetch_louisiana (test mode run OK: 25 records, 51 requests, 28 s, 23/25 geocoded;
      printable 503s found -> one worker, 1.2 s spacing, Retry-After honoured)
- [x] Fixtures (tests/fixtures/georgia, tests/fixtures/louisiana)
- [ ] tests/test_georgia.py, tests/test_louisiana.py written; being made green
- [ ] Live runs: --mode full --since-date 2026-10-20 per state (launched 2026-11-03 ~01:15 UTC,
      logs in scratchpad/tyler_live/{georgia,louisiana}_14d.log)
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
  from the printable HTML (titles only on failure, never a hold); metro from
  the Parish column.
- Scope rule (both states): the LA permit-name tail is free text (221
  distinct tails in 250 facilities, 378/2,995 inspections with none), so no
  type allowlist is possible; TP_INSTITUTION_RE is a word-boundary keyword
  denylist over facility name (+ LA permit name) for institutional kitchens
  (school/academy/day care/dietary/hospital/nursing/jail/...; "old school"
  exempt, "cafeteria" alone not a trigger). GA's single 'Food Service'
  permit type covers its schools/hospices too, so the same rule applies.
  LA 'Summer Feeding' and GA 'Temporary' purposes are skipped.
- LA /paper/ printables: token-bucket rate limit measured 2026-11-03 (about
  4 requests then paired instant 503s with Retry-After: 3, clearing in <5 s;
  the JSON API never 503'd). LA_PRINTABLE_WORKERS=1, LA_PRINTABLE_DELAY=1.2,
  LA_PRINTABLE_RETRIES=3 honouring Retry-After (+0.5 s), ~2.3 s/printable.
- GA metro: the only county signal in a row is the county EH phone
  ('For More Information Call:'); one number per county verified live on 20
  rows of each of the 9 Atlanta counties (GA_ATLANTA_EH_PHONES). GNR
  (Gwinnett/Newton/Rockdale) returned 0 rows again 2026-11-03; each run
  re-checks the three county filters over its window (3 requests).

## Live results
- 2026-11-03 --mode test georgia: 25 records (11 restaurants), 33 requests, 7 s wall,
  Census geocoded 23/25, all A (newest-listed facilities), text/categories look right.
- 2026-11-03 --mode test louisiana: 25 records (16 restaurants), 51 requests, 28 s wall,
  23/25 geocoded, grades A10/B1/C4/F1 (F = 9 critical + 17 non-critical items);
  16/20 printables 503'd before the pacing fix.
- LA probe 2026-11-03: 250 facilities / 2,995 inspections; avg 0.79 s per API call,
  max 4.8 s; one well-formed inspectionsData answered 500 once (retry OK).

## Open questions / follow-ups
(none yet)
