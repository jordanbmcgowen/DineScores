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

## Next
- tests/test_hedgerow.py; run the suite; live test-mode + 14-day runs.
- Mesquite source_url host: the SPA is not on the API host (root is the
  vendor's "Hedgehog Server" page, /portal/ 404) — find the public SPA host
  or document the URL as unverified.

## Open questions
- Pima 'Needs Improvement' computes to a B (e.g. 93 for P+C+C); only Fail /
  IHH / Probation cap the score. Owner policy call.
