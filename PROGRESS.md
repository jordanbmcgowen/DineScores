# feat/mhd-expansion — progress log

Branch owns the MyHealthDepartment (MHD) machinery: config tables
(DFW_JURISDICTIONS / MHD_METRO_JURISDICTIONS / MHD_METRO_SLUG_GROUPS),
the `_mhd_*` helpers, `fetch_dfw`, `_fetch_mhd_jurisdiction_api`,
`_parse_mhd_record`, the MHD block in `main()`, tests/test_mhd_buckets.py
and the MHD parts of tests/test_refresh_hardening.py.

## Status (update after every milestone; a successor resumes from here)

- [x] Recon read: understand_mhd/contract/wiring/tests + 8 probe reports.
- [x] Baseline: suite green (274 tests), relay tests pass, py_compile ok.
- [x] Live reachability from this container confirmed (dallas search 25 rows
      in 0.7 s via dp._mhd_search_page; fbceh search 25 rows).
- [ ] (A) AUDIT existing DFW/metro reads: 7-day window vs union of single
      days; unsorted vs sort {inspectionID asc}. Script:
      scratchpad/mhd_audit/audit_mhd_paging.py -> results JSONL in the same dir.
- [ ] (B) MACHINERY: per-config `sort` threaded through search_page /
      search_all_pages / fetch_window / fetch_bucket_window with the
      duplicate/non-monotonic guard (-> None); per-config `group` key and
      one fetch group per distinct group name in main(); row-exclude hook.
- [ ] (C) CONFIG: fbceh, acph, dchd, ccbh, 33 VDH districts (helper), slug
      groups fortbend/arapahoe/douglas/cuyahoga/richmond/hamptonroads/nova/
      virginia; each verified live.
- [ ] Tests extended in tests/test_mhd_buckets.py (+ fixtures).
- [ ] (D) LIVE RUNS: --mode test --cities fortbend arapahoe douglas cuyahoga
      richmond; bounded window run; per-jurisdiction detail volumes.
- [ ] Final cleanup + StructuredOutput.

## Live results (fill in as soon as available)

(none yet)

## Open questions / decisions

(none yet)
