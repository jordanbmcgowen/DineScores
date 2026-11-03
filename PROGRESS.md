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
- [x] (A) AUDIT done (2026-11-03, 16 jurisdictions x 2 weeks, 3,158 rows):
      see "Audit results" below. Script scratchpad/mhd_audit/audit_mhd_paging.py,
      raw results audit_required.jsonl / audit_extra.jsonl (truth ids kept),
      after-measurement after_measure.py (production path vs truth).
- [x] (B) MACHINERY committed (2dd7ac9, fcf0941): sort threaded with the
      repeats guard (order check dropped: Gwinnett), MHD_WINDOW_DAYS 1,
      MHD_DEFAULT_SORT inspectionID asc for plain configs, 'group' key +
      _mhd_metro_groups in main(), 'exclude' row patterns, 'results' field.
- [x] (C) CONFIG committed: fbceh, acph, dchd, ccbh, 33 va-* districts via
      _mhd_va_district; slug groups fortbend/arapahoe/douglas/cuyahoga/
      richmond/hamptonroads/nova/virginia. Live verification running
      (scratchpad/mhd_verify/verify_new_slugs.py -> verify.jsonl).
- [x] Tests extended in tests/test_mhd_buckets.py (+ tests/fixtures/mhd/),
      suite 315 green.
- [ ] (D) LIVE RUNS: --mode test --cities fortbend arapahoe douglas cuyahoga
      richmond; bounded window run; per-jurisdiction detail volumes.
- [ ] Final cleanup + StructuredOutput.

## Audit results (A): what the production MHD read has been losing

Method: for each jurisdiction and two completed weeks (2026-10-12..18,
2026-10-19..25): A = the production read (dp._mhd_fetch_window over the 7-day
window, unsorted, with the cap bisection), B = union of 7 single-day production
reads, C = 7-day window sorted {inspectionID asc}, D = union of 7 single-day
sorted reads. Truth = union of every row any read returned (the date filter
is server-side; 0 out-of-window rows). "repeats" = rows the unsorted read
served twice across pages (raw - unique).

Findings:
1. UNSTABLE PAGING: the unsorted read lost rows in 24 of 32 jurisdiction-weeks,
   188 of 3,158 rows (6.0%); Dallas 20/154 and 28/256, Fort Worth 22/192,
   Tarrant 19/195, El Paso 9/77. Loss == repeats on every such read.
2. PER-PERMIT COLLAPSE: ANY multi-day window returns one row per permit (its
   latest in the window), sorted or not: Plano lost 11 and 7 rows of permits
   inspected twice in the week, Fort Worth 1+1, Multnomah 3+1, Adams 2,
   Utah 1, Yolo 1. Single-day reads see them all.
3. The sorted single-day union (D) missed 0 rows in all 32 cases.
4. Sorted pages were strictly in SQL Server GUID order everywhere EXCEPT
   Gwinnett (disjoint and complete, first page 45-60 rows, no key order),
   hence the guard checks repeats only. Gwinnett's unsorted read lost 0.
5. Unsorted single-day reads also lose rows on days with >25 rows (Dallas
   9 and 21, Tarrant 5 and 27): the sort is needed even with 1-day windows.

| jurisdiction | week | rows (truth) | pipeline 7-day read: missing | repeats | pages | single days unsorted: missing | sorted 7-day: missing | sorted single days: missing | permits with >1 insp | their rows lost by 7-day read | by sorted 7-day | AFTER (production path): missing | extra | searches |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| frisco | 2026-10-12..2026-10-18 | 56 | 5 | 5 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| frisco | 2026-10-19..2026-10-25 | 88 | 3 | 3 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| dallas | 2026-10-12..2026-10-18 | 154 | 20 | 20 | 7 | 9 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| dallas | 2026-10-19..2026-10-25 | 256 | 28 | 253 | 20 | 21 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| plano | 2026-10-12..2026-10-18 | 153 | 11 | 0 | 6 | 0 | 11 | 0 | 11 | 11 | 11 | - | - | - |
| plano | 2026-10-19..2026-10-25 | 182 | 7 | 0 | 8 | 0 | 7 | 0 | 7 | 7 | 7 | - | - | - |
| fort-worth-texas | 2026-10-12..2026-10-18 | 192 | 22 | 21 | 8 | 0 | 1 | 0 | 1 | 1 | 1 | - | - | - |
| fort-worth-texas | 2026-10-19..2026-10-25 | 139 | 11 | 10 | 6 | 0 | 1 | 0 | 1 | 1 | 1 | - | - | - |
| tarrant | 2026-10-12..2026-10-18 | 119 | 6 | 6 | 5 | 5 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| tarrant | 2026-10-19..2026-10-25 | 195 | 19 | 19 | 8 | 27 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| gwinnett | 2026-10-12..2026-10-18 | 122 | 0 | 0 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| gwinnett | 2026-10-19..2026-10-25 | 134 | 0 | 0 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| multco-eh | 2026-10-12..2026-10-18 | 238 | 10 | 232 | 19 | 0 | 3 | 0 | 3 | 4 | 3 | - | - | - |
| multco-eh | 2026-10-19..2026-10-25 | 73 | 1 | 0 | 3 | 0 | 1 | 0 | 1 | 1 | 1 | - | - | - |
| adcogoveh | 2026-10-12..2026-10-18 | 58 | 4 | 4 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| adcogoveh | 2026-10-19..2026-10-25 | 59 | 5 | 3 | 3 | 2 | 2 | 0 | 1 | 2 | 2 | - | - | - |
| epcph | 2026-10-12..2026-10-18 | 65 | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| epcph | 2026-10-19..2026-10-25 | 77 | 9 | 9 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| utah-utahcounty | 2026-10-12..2026-10-18 | 81 | 1 | 0 | 4 | 0 | 1 | 0 | 1 | 1 | 1 | - | - | - |
| utah-utahcounty | 2026-10-19..2026-10-25 | 63 | 0 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| yolocountyeh | 2026-10-12..2026-10-18 | 25 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| yolocountyeh | 2026-10-19..2026-10-25 | 73 | 1 | 0 | 3 | 0 | 1 | 0 | 1 | 1 | 1 | - | - | - |
| clarkcountywa | 2026-10-12..2026-10-18 | 72 | 5 | 5 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| clarkcountywa | 2026-10-19..2026-10-25 | 60 | 2 | 2 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| or-washington-county | 2026-10-12..2026-10-18 | 101 | 2 | 2 | 5 | 2 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| or-washington-county | 2026-10-19..2026-10-25 | 26 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| or-clackamas-county | 2026-10-12..2026-10-18 | 61 | 0 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| or-clackamas-county | 2026-10-19..2026-10-25 | 32 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| larimer-county-health | 2026-10-12..2026-10-18 | 45 | 4 | 4 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| larimer-county-health | 2026-10-19..2026-10-25 | 71 | 5 | 5 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| weldcounty | 2026-10-12..2026-10-18 | 46 | 3 | 3 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |
| weldcounty | 2026-10-19..2026-10-25 | 42 | 1 | 1 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | - | - | - |

TOTAL rows 3158, lost by the production 7-day unsorted read 188 (6.0%)

(AFTER columns: filled by the fixed after_measure.py once it has run.)

## Live results (fill in as soon as available)

(verification in progress)

## Open questions / decisions

(none yet)
