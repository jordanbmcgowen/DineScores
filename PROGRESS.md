# Williamson County (WCCHD) fetcher — progress log

Branch `feat/williamson`, worktree `/home/user/wt-williamson`. Section header in
`dinescores_pipeline.py`: "WILLIAMSON COUNTY, TX (WCCHD inspection-scores zip snapshot)".

## Done
- [x] Read the recon docs (contract / wiring / tests), the probe report and the capture
      directory; analysed the real 2026-10-29 snapshot (12,145 permits, 6,734 inspections).
- [x] `fetch_williamson` + helpers written in the stub section (constants `WM_*`, `_wm_title`,
      `_wm_city`, `_wm_address`, `_wm_out_of_scope`, `_wm_read_feed_info`, `_wm_download`,
      `_wm_unzip`, `_wm_rows`). Offline smoke run against the captured zip:
      5,343 records from 6,734 rows (1,250 school-kitchen rows, 33 district test-permit rows,
      11 mobile-unit rows, 86 orphans, 8 negative scores, 3 exact duplicates dropped);
      grades B 1944 / A 1513 / C 1246 / F 640; no duplicate source_ids; same-day pairs
      worst-first with a `_2` suffix; 90-day window (since 2026-08-05) = 117 rows,
      87 distinct addresses; `limit=25` keeps the newest and leaves no lag note.
- [x] Fixtures: `tests/fixtures/williamson/{businesses.csv,inspections.csv}` (byte-verbatim
      rows for 19 chosen permits, no BOM, CRLF — the real file has no BOM) and the real
      `feed_info.xlsx` (9,354 B).

## Design decisions (also in the section comment)
- One GET of the zip per run; refuse (RuntimeError) on a missing member, unreadable
  feed_date, < 1000 inspection rows, < 2000 business rows, > 1% unparseable rows,
  non-zip body, or any non-200/429/5xx status (403/404 = the object moved).
- Official score -> original_score and risk_score (clamped); score <= 0 dropped with a
  warning; IHH -> results 'Imminent Health Hazard - closed', risk capped at 55 (F).
- Scope by permit code: drop -TFP, -MF, -SAN, school kitchens (-ISD, -SFEC, -FFES*, `<city>S-`
  prefixes) and the district's 'TEST…' permits; keep -FFEC (commissary kitchens).
- Normalisation: whitespace collapsed; city stripped of trailing punctuation / glued zip and
  title-cased; ALL-CAPS names/addresses title-cased with possessives, ordinals and
  road/company codes protected; mixed case kept as published.
- Output newest-first; `limit` truncates (no lag note then); `_note_source_lag('Williamson
  County', newest)` after a complete read. Deadline before/while downloading ->
  `_set_group_status('Williamson County', 'skipped', …)` and `[]`.
- No per-record detail phase exists, so no `_hold_freshness` is ever needed (nothing can be
  dropped transiently: the whole file is one atomic object).
- No Jev registration needed: no violation lines; unregistered label = 'official' / 'free'.

## Next
- [ ] tests/test_williamson.py (no network) — then run the whole suite + node relay tests.
- [ ] Live: `--mode test --cities williamson` (25 records) and
      `--mode full --since-date 2026-08-05 --cities williamson` (90-day window); report counts,
      timings, geocode coverage, grade distribution, 3 samples; Census hit rate on 50.
- [ ] Final cleanup, commit, structured report.

## Live results
(none yet)

## Open questions
- School kitchens are 18.6% of the feed's rows; dropped per the scope rule (Tennessee/Omaha
  precedent). The integrator may want them back (remove the 'school kitchen' entry from
  `WM_OUT_OF_SCOPE`).
