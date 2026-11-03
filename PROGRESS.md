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

## Next
- Verify Mesquite shapes live (lastInspectionDate hidden? inspectionResults 500?)
- Write client + fetchers, fixtures, tests; test-mode and 14-day live runs.

## Open questions
- Mesquite: facility list hides facilityType/lastInspectionDate (capture); if
  so every facility's inspection list must be read each run (689 facilities).
