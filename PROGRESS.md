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
      richmond/hamptonroads/nova/virginia. ALL 37 VERIFIED LIVE 2026-11-03
      (production path, week 2026-10-20..26; see "Verification" below).
- [x] Tests extended in tests/test_mhd_buckets.py (+ tests/fixtures/mhd/),
      suite 315 green.
- [x] (D) LIVE RUNS done (test mode 200 records 209 s; 14-day fortbend+douglas
      169 records 120 s); per-jurisdiction detail volumes in the verification table.
- [x] Final cleanup: suite 324 green, relay 24 PASS, py_compile ok, tree clean.

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
| frisco | 2026-10-12..2026-10-18 | 56 | 5 | 5 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| frisco | 2026-10-19..2026-10-25 | 88 | 3 | 3 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| dallas | 2026-10-12..2026-10-18 | 154 | 20 | 20 | 7 | 9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 11 |
| dallas | 2026-10-19..2026-10-25 | 256 | 28 | 253 | 20 | 21 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 15 |
| plano | 2026-10-12..2026-10-18 | 153 | 11 | 0 | 6 | 0 | 11 | 0 | 11 | 11 | 11 | 0 | 0 | 11 |
| plano | 2026-10-19..2026-10-25 | 182 | 7 | 0 | 8 | 0 | 7 | 0 | 7 | 7 | 7 | 0 | 0 | 12 |
| fort-worth-texas | 2026-10-12..2026-10-18 | 192 | 22 | 21 | 8 | 0 | 1 | 0 | 1 | 1 | 1 | 0 | 0 | 12 |
| fort-worth-texas | 2026-10-19..2026-10-25 | 139 | 11 | 10 | 6 | 0 | 1 | 0 | 1 | 1 | 1 | 0 | 0 | 10 |
| tarrant | 2026-10-12..2026-10-18 | 119 | 6 | 6 | 5 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 10 |
| tarrant | 2026-10-19..2026-10-25 | 195 | 19 | 19 | 8 | 27 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 12 |
| gwinnett | 2026-10-12..2026-10-18 | 122 | 0 | 0 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 10 |
| gwinnett | 2026-10-19..2026-10-25 | 134 | 0 | 0 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 10 |
| multco-eh | 2026-10-12..2026-10-18 | 238 | 10 | 232 | 19 | 0 | 3 | 0 | 3 | 4 | 3 | 0 | 0 | 14 |
| multco-eh | 2026-10-19..2026-10-25 | 73 | 1 | 0 | 3 | 0 | 1 | 0 | 1 | 1 | 1 | 0 | 0 | 8 |
| adcogoveh | 2026-10-12..2026-10-18 | 58 | 4 | 4 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| adcogoveh | 2026-10-19..2026-10-25 | 59 | 5 | 3 | 3 | 2 | 2 | 0 | 1 | 2 | 2 | 0 | 0 | 8 |
| epcph | 2026-10-12..2026-10-18 | 65 | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| epcph | 2026-10-19..2026-10-25 | 77 | 9 | 9 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| utah-utahcounty | 2026-10-12..2026-10-18 | 81 | 1 | 0 | 4 | 0 | 1 | 0 | 1 | 1 | 1 | 0 | 0 | 7 |
| utah-utahcounty | 2026-10-19..2026-10-25 | 63 | 0 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| yolocountyeh | 2026-10-12..2026-10-18 | 25 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| yolocountyeh | 2026-10-19..2026-10-25 | 73 | 1 | 0 | 3 | 0 | 1 | 0 | 1 | 1 | 1 | 0 | 0 | 8 |
| clarkcountywa | 2026-10-12..2026-10-18 | 72 | 5 | 5 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| clarkcountywa | 2026-10-19..2026-10-25 | 60 | 2 | 2 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| or-washington-county | 2026-10-12..2026-10-18 | 101 | 2 | 2 | 5 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 9 |
| or-washington-county | 2026-10-19..2026-10-25 | 26 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| or-clackamas-county | 2026-10-12..2026-10-18 | 61 | 0 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| or-clackamas-county | 2026-10-19..2026-10-25 | 32 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| larimer-county-health | 2026-10-12..2026-10-18 | 45 | 4 | 4 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| larimer-county-health | 2026-10-19..2026-10-25 | 71 | 5 | 5 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| weldcounty | 2026-10-12..2026-10-18 | 46 | 3 | 3 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |
| weldcounty | 2026-10-19..2026-10-25 | 42 | 1 | 1 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 7 |

TOTAL rows 3158, lost by the production 7-day unsorted read 188 (6.0%)

AFTER = the production path as committed (sorted inspectionID order, one-day
windows, repeats guard) re-reading the same weeks: 0 rows missing and 0 extra
in all 32 jurisdiction-weeks, at 7-15 searches per jurisdiction-week (before:
2-20, the 20 being Dallas's cap bisection).

Third completed week (2026-10-26..11-01, run 2026-11-03 03:40-03:52Z), all 16
jurisdictions: the OLD unsorted 7-day read (one query, no bisection) against
a sorted single-day truth read, and the committed production path. 1059
rows, old read missing 95 (9.0%), production path missing 0.

| jurisdiction | rows | old read missing | old read repeats | production path missing |
|---|---|---|---|---|
| dallas | 206 | 31 | 30 | 0 |
| plano | 151 | 9 | 0 | 0 |
| fort-worth-texas | 203 | 23 | 21 | 0 |
| tarrant | 190 | 27 | 27 | 0 |
| frisco | 51 | 4 | 4 | 0 |
| gwinnett | 161 | 0 | 0 | 0 |
| multco-eh | 0 | 0 | 0 | 0 |
| or-clackamas-county | 0 | 0 | 0 | 0 |
| larimer-county-health | 48 | 1 | 1 | 0 |
| weldcounty | 49 | 0 | 0 | 0 |

(Multnomah, Washington OR and Clackamas: 0 rows — the Oregon counties publish
about two weeks late, as their lag_days 21 expects.)


## Verification of the new jurisdictions (C) — live, production code path

Each slug read with _fetch_mhd_jurisdiction_api(fetch_violations=False) over
2026-10-20..26 (sorted one-day windows + guard), then 3 detail pages parsed
with _mhd_parse_detail_html. "obs/score" = observations parsed / row score.
Every slug answered rows, every detail page parsed (0 unreadable), no 403/429,
no portal block or detail ban; every raw row unique. Score evidence: fbceh
0-100 (100 <-> 0 observations, 88 <-> 8); acph/dchd demerit totals (0 on
~40% of rows, result Pass/Re-Inspection Required/Closed); ccbh and every VDH
district score 0 + scoreDisplay '' on 100% of rows (no score: 'none').
programName values: fbceh Food|Pools; acph Retail Food|Child Care|Pools|Body
Art; dchd Retail Food|Child Care; ccbh Food|Schools|Body Art|Pools; VDH Food
Establishment only.

37 jurisdictions verified live (window 2026-10-20..2026-10-26, production path, sorted one-day windows)

| key | display | records/week (= detail pages/week) | raw rows | programName values seen | score values (top) | scoreDisplay | details parsed (obs count, score) | top cities | s |
|---|---|---|---|---|---|---|---|---|---|
| fbceh | Fort Bend County | 58 | 69 | Food:58, Pools:11 | 0:11, 98:9, 100:8, 96:8 | 'None':69 | 0/100, 5/94, 8/88 | Katy, Richmond, Sugar Land | 11.9 |
| acph | Arapahoe County | 84 | 104 | Retail Food:89, Child Care:12, Pools:2, Body Art:1 | 0:39, 10:11, 20:7, 45:4 | '':104 | 1/0, 5/45, 5/32 | Aurora, Englewood, Centennial | 14.2 |
| dchd | Douglas County CO | 31 | 41 | Retail Food:31, Child Care:10 | 0:18, 10:5, 15:3, 5:3 | '':41 | 3/35, 2/10, 8/47 | Castle Rock, Highlands Ranch, Lone Tree | 14.1 |
| ccbh | Cuyahoga County | 218 | 265 | Food:218, Schools:22, Body Art:13, Pools:12 | 0:265 | '':265 | 2/None, 0/None, 7/None | Cleveland Heights, Strongsville, Westlake | 34.4 |
| va-richmond | VDH Richmond City | 41 | 41 | Food Establishment:41 | 0:41 | '':41 | 0/None, 6/None, 1/None | Richmond | 15.4 |
| va-henrico | VDH Henrico | 32 | 32 | Food Establishment:32 | 0:32 | '':32 | 1/None, 2/None, 3/None | Richmond, Henrico, Glen Allen | 12.9 |
| va-chesterfield | VDH Chesterfield | 90 | 90 | Food Establishment:90 | 0:90 | '':90 | 2/None, 0/None, 1/None | Midlothian, North Chesterfield, Chester | 13.1 |
| va-chickahominy | VDH Chickahominy | 30 | 30 | Food Establishment:30 | 0:30 | '':30 | 1/None, 0/None, 3/None | Ashland, Mechanicsville, Quinton | 14.5 |
| va-virginia-beach | VDH Virginia Beach | 50 | 50 | Food Establishment:50 | 0:50 | '':50 | 2/None, 4/None, 0/None | Virginia Beach | 14.1 |
| va-norfolk | VDH Norfolk | 82 | 82 | Food Establishment:82 | 0:82 | '':82 | 1/None, 7/None, 1/None | Norfolk | 14.3 |
| va-chesapeake | VDH Chesapeake | 25 | 25 | Food Establishment:25 | 0:25 | '':25 | 2/None, 5/None, 1/None | Chesapeake | 23.3 |
| va-portsmouth | VDH Portsmouth | 18 | 18 | Food Establishment:18 | 0:18 | '':18 | 4/None, 23/None, 13/None | Portsmouth | 13.4 |
| va-hampton | VDH Hampton | 12 | 12 | Food Establishment:12 | 0:12 | '':12 | 1/None, 4/None, 0/None | Hampton | 12.5 |
| va-peninsula | VDH Peninsula | 39 | 39 | Food Establishment:39 | 0:39 | '':39 | 3/None, 1/None, 3/None | Williamsburg, Newport News, Yorktown | 12.8 |
| va-western-tidewater | VDH Western Tidewater | 22 | 22 | Food Establishment:22 | 0:22 | '':22 | 0/None, 4/None, 0/None | Suffolk, Franklin, Isle Of Wight | 13.4 |
| va-arlington | VDH Arlington | 53 | 53 | Food Establishment:53 | 0:53 | '':53 | 0/None, 1/None, 0/None | Arlington, Washington, Falls Church | 12.9 |
| va-alexandria | VDH Alexandria | 26 | 26 | Food Establishment:26 | 0:26 | '':26 | 0/None, 1/None, 2/None | Alexandria, Washington | 13.1 |
| va-loudoun | VDH Loudoun | 61 | 61 | Food Establishment:61 | 0:61 | '':61 | 3/None, 1/None, 6/None | Sterling, Ashburn, Leesburg | 13.9 |
| va-prince-william | VDH Prince William | 12 | 12 | Food Establishment:12 | 0:12 | '':12 | 3/None, 0/None, 0/None | Woodbridge, Manassas, Dumfries | 14.0 |
| va-alleghany-roanoke | VDH Alleghany-Roanoke | 50 | 50 | Food Establishment:50 | 0:50 | '':50 | 4/None, 2/None, 0/None | Roanoke, Salem, Covington | 13.1 |
| va-blue-ridge | VDH Blue Ridge | 19 | 19 | Food Establishment:19 | 0:19 | '':19 | 0/None, 0/None, 10/None | Charlottesville, Gordonsville, Ruckersville | 17.5 |
| va-central | VDH Central Virginia | 31 | 31 | Food Establishment:31 | 0:31 | '':31 | 3/None, 2/None, 2/None | Lynchburg, Forest, Bedford | 13.2 |
| va-central-shenandoah | VDH Central Shenandoah | 40 | 40 | Food Establishment:40 | 0:40 | '':40 | 0/None, 0/None, 0/None | Harrisonburg, Waynesboro, Staunton | 15.0 |
| va-crater | VDH Crater | 19 | 19 | Food Establishment:19 | 0:19 | '':19 | 11/None, 8/None, 2/None | Hopewell, Emporia, Petersburg | 12.5 |
| va-cumberland-plateau | VDH Cumberland Plateau | 17 | 17 | Food Establishment:17 | 0:17 | '':17 | 1/None, 0/None, 0/None | Tazewell, Richlands, North Tazewell | 12.4 |
| va-eastern-shore | VDH Eastern Shore | 10 | 10 | Food Establishment:10 | 0:10 | '':10 | 0/None, 3/None, 3/None | Accomac, Cape Charles, Parksley | 14.2 |
| va-lenowisco | VDH Lenowisco | 17 | 17 | Food Establishment:17 | 0:17 | '':17 | 4/None, 0/None, 1/None | Norton, Nickelsville, Wise | 20.0 |
| va-lord-fairfax | VDH Lord Fairfax | 37 | 37 | Food Establishment:37 | 0:37 | '':37 | 4/None, 5/None, 5/None | Winchester, Front Royal, Woodstock | 15.6 |
| va-mount-rogers | VDH Mount Rogers | 40 | 40 | Food Establishment:40 | 0:40 | '':40 | 0/None, 0/None, 3/None | Bristol, Wytheville, Abingdon | 12.7 |
| va-new-river | VDH New River | 7 | 7 | Food Establishment:7 | 0:7 | '':7 | 9/None, 0/None, 5/None | Blacksburg, Christiansburg, Narrows | 13.0 |
| va-piedmont | VDH Piedmont | 3 | 3 | Food Establishment:3 | 0:3 | '':3 | 3/None, 1/None, 1/None | Drakes Branch, Farmville, Keysville | 12.5 |
| va-pittsylvania-danville | VDH Pittsylvania-Danville | 21 | 21 | Food Establishment:21 | 0:21 | '':21 | 0/None, 1/None, 0/None | Danville, Gretna | 12.3 |
| va-rappahannock | VDH Rappahannock | 24 | 24 | Food Establishment:24 | 0:24 | '':24 | 0/None, 0/None, 0/None | Fredericksburg, King George, Stafford | 22.1 |
| va-rappahannock-rapidan | VDH Rappahannock-Rapidan | 14 | 14 | Food Establishment:14 | 0:14 | '':14 | 1/None, 0/None, 0/None | Culpeper, Warrenton, Bealeton | 12.7 |
| va-southside | VDH Southside | 11 | 11 | Food Establishment:11 | 0:11 | '':11 | 0/None, 2/None, 2/None | South Hill, Clarksville, South Boston | 20.3 |
| va-three-rivers | VDH Three Rivers | 21 | 21 | Food Establishment:21 | 0:21 | '':21 | 2/None, 0/None, 0/None | Montross, Mathews, West Point | 13.2 |
| va-west-piedmont | VDH West Piedmont | 31 | 31 | Food Establishment:31 | 0:31 | '':31 | 0/None, 5/None, 1/None | Martinsville, Rocky Mount, Collinsville | 13.5 |

Weekly detail pages added per fetch group (sum of records/week):
  MHD metros: 173
  MHD metros (midweek): 1223
VDH by metro chip:
  Richmond: 193
  Hampton Roads: 248
  Northern Virginia: 152
  (no chip): 412
all rows unique: True
any portal block/ban flags: False
details unreadable (None): 0


## Live results (D)

### Test mode: `python3 dinescores_pipeline.py --mode test --cities fortbend arapahoe douglas cuyahoga richmond --output /tmp/mhd_test.json`

2026-11-03 03:30:47Z -> 03:34:16Z (3 min 29 s wall). 200 inspections (25 per
jurisdiction x 8: fbceh, acph, dchd, ccbh, va-richmond, va-henrico,
va-chesterfield, va-chickahominy), 198 restaurants saved. Both groups 'ok':
MHD metros 75 records, MHD metros (midweek) 125. 55 search POSTs (one-day
windows from 2026-01-01 until 25 records), 200 detail GETs, 25/25 detail
pages parsed for every jurisdiction, no 403/429, no ban. Geocoding: Census
batch 70/75 + 125 -> 187/198 with coordinates (11 without; one Nominatim
connection failure during the fallback, transient). Grades plausible (the
detail pass recomputes the demerit/no-score jurisdictions; Fort Bend keeps
its official score):

198 restaurant records in /tmp/mhd_test.json
  Arapahoe County Health Dept          n= 25 coords= 25 with_violations= 20 grades={'A': 15, 'B': 5, 'C': 2, 'F': 3} risk min/med/max=47/90/100 official=25 out_of_range=[] results=[('Pass', 24), ('Re-Inspection Required', 1)]
  Cuyahoga County Health Dept          n= 25 coords= 25 with_violations= 20 grades={'A': 11, 'B': 6, 'C': 1, 'F': 7} risk min/med/max=43/88/100 official=0 out_of_range=[] results=[('', 25)]
  Douglas County CO Health Dept        n= 25 coords= 23 with_violations= 23 grades={'A': 10, 'B': 9, 'C': 2, 'F': 4} risk min/med/max=58/88/100 official=25 out_of_range=[] results=[('', 25)]
  Fort Bend County Health Dept         n= 25 coords= 22 with_violations= 21 grades={'A': 15, 'B': 7, 'C': 3} risk min/med/max=70/95/100 official=25 out_of_range=[] results=[('', 25)]
  VDH Chesterfield Health Dept         n= 25 coords= 19 with_violations= 21 grades={'A': 12, 'B': 7, 'C': 2, 'F': 4} risk min/med/max=29/88/100 official=0 out_of_range=[] results=[('', 25)]
  VDH Chickahominy Health Dept         n= 24 coords= 24 with_violations= 13 grades={'A': 14, 'B': 4, 'C': 1, 'F': 5} risk min/med/max=21/95/100 official=0 out_of_range=[] results=[('', 24)]
  VDH Henrico Health Dept              n= 24 coords= 24 with_violations= 21 grades={'A': 9, 'B': 5, 'C': 4, 'F': 6} risk min/med/max=0/85/100 official=0 out_of_range=[] results=[('', 24)]
  VDH Richmond City Health Dept        n= 25 coords= 25 with_violations= 17 grades={'A': 14, 'B': 4, 'C': 3, 'F': 4} risk min/med/max=19/95/100 official=0 out_of_range=[] results=[('', 25)]
   details: 1-03 03:31:00,262 INFO Fort Bend County: 25/25 inspections had violation details
   details: 11-03 03:31:19,573 INFO Arapahoe County: 25/25 inspections had violation details
   details: -03 03:31:40,627 INFO Douglas County CO: 25/25 inspections had violation details
   details: 11-03 03:32:13,553 INFO Cuyahoga County: 25/25 inspections had violation details
   details: -03 03:32:32,583 INFO VDH Richmond City: 25/25 inspections had violation details
   details: 026-11-03 03:32:56,924 INFO VDH Henrico: 25/25 inspections had violation details
   details: 1-03 03:33:21,407 INFO VDH Chesterfield: 25/25 inspections had violation details
   details: 1-03 03:33:45,344 INFO VDH Chickahominy: 25/25 inspections had violation details
   search POSTs (windows logged): 55 ; geocode lines: ['2026-11-03 03:31:43,629 INFO Geocoding 75 records missing coordinates (Census batch)...', '2026-11-03 03:32:01,059 INFO Geocoded 70/75 records', '2026-11-03 03:33:48,346 INFO Geocoding 125 records missing coordinates (Census batch)...', "2026-11-03 03:34:12,195 WARNING Geocode failed for 13747 Lake Eloise Loop, Midlothian, VA: HTTPSConnectionPool(host='nom"]
   total records line: ['Total inspections collected: 200']
{"name": "Liquor 4 U", "address": "11581 S Highway 6 Ste A", "city": "Sugar Land", "state": "TX", "zip": "77498-4932", "latitude": 29.6481601448, "longitude": -95.651270157795, "inspection_date": "2026-01-02", "original_score": 85, "risk_score": 85, "priority_violations": 4, "priority_foundation_violations": 5, "core_violations": 2, "total_violations": 11, "violations": [["food_handling", "priority_foundation", "2: \u00a7228.31(b) \u00a7-- RULES FOR REGULATION OF FOOD ESTABLISHMENTS IN THE UNINCORPORATED AREAS OF FORT BEND COUNTY, TEXAS A C"], ["cross_contamination", "priority", "5: 2-501.11 A food establishment shall have written procedures for employees to follow when responding to vomiting or di"]], "inspection_type": "Food Safety Inspection", "purpose": "Permit", "results": "", "source": "Fort Bend County Health Dept", "source_url": "https://inspections.myhealthdepartment.com/fbceh/i
{"name": "Stonegate Cafe", "address": "16524 Keystone Blvd Ste F", "city": "Parker", "state": "CO", "zip": "80134-3349", "latitude": 39.528262281643, "longitude": -104.806973426956, "inspection_date": "2026-01-05", "original_score": 55, "risk_score": 75, "priority_violations": 5, "priority_foundation_violations": 0, "core_violations": 0, "total_violations": 5, "violations": [["cross_contamination", "priority", "8: 2-301.14 Employees observed not hand washing when required between high-risk activities, such as between raw and read"], ["personal_hygiene", "priority", "10: 6-301.11 A handwashing sink within the facility lacks readily available soap. The inspector observed there was no ha"]], "inspection_type": "Retail Food", "purpose": "Routine", "results": "", "source": "Douglas County CO Health Dept", "source_url": "https://inspections.myhealthdepartment.com/dchd/inspection/?inspectionID=
{"name": "Restaurant Adarra", "address": "501 S Pine St", "city": "Richmond", "state": "VA", "zip": "23220", "latitude": 37.539087125651, "longitude": -77.450388792773, "inspection_date": "2026-01-02", "original_score": null, "risk_score": 95, "priority_violations": 1, "priority_foundation_violations": 0, "core_violations": 0, "total_violations": 1, "violations": [["cross_contamination", "priority", "15: 12VAC5-421-470.A.1.a-c Observed raw shell eggs stored over ready to eat foods cheeses, commercially cured meats in w"]], "inspection_type": "Full Service Restaurant", "purpose": "Routine", "results": "", "source": "VDH Richmond City Health Dept", "source_url": "https://inspections.myhealthdepartment.com/va-richmond/inspection/?inspectionID=BC3CEA96-7459-4C48-A881-6E2FA665EFCD", "source_id": "BC3CEA96-7459-4C48-A881-6E2FA665EFCD", "metro": "Richmond", "_id": "ff642ad9db8ff80c", "_inspecti


### 14-day bounded run: `python3 dinescores_pipeline.py --mode full --since-date 2026-10-20 --cities fortbend douglas --output /tmp/mhd_14d.json`

2026-11-03 03:49:16Z -> 03:51:16Z (120 s wall). Fort Bend 120 inspections
over 15 one-day windows (16 search POSTs), Douglas County CO 49 (14 POSTs);
30 search POSTs + 169 detail GETs in all, 169/169 detail pages parsed, no
403/429, no ban, group 'ok', rc 0, 0 errors. Census batch geocoded 151/169;
165 restaurants saved (148 with coordinates = 90%; the rest retry next run).
Grades: Fort Bend A 94 / B 18 / C 2 / F 3 (official 70-100, median 96; the
F's are text-forced on its official score), Douglas A 33 / B 13 / C 2
(demerit totals 0-55 recomputed to risk 71-100 from the detail pages).

165 restaurant records in /tmp/mhd_14d.json
  Douglas County CO Health Dept        n= 48 coords= 48 with_violations= 34 grades={'A': 33, 'B': 13, 'C': 2} risk min/med/max=71/97/100 official=48 out_of_range=[] results=[('', 48)]
  Fort Bend County Health Dept         n=117 coords=100 with_violations=103 grades={'A': 94, 'B': 18, 'C': 2, 'F': 3} risk min/med/max=70/96/100 official=117 out_of_range=[] results=[('', 117)]
   details: 03 03:50:02,798 INFO Fort Bend County: 120/120 inspections had violation details
   details: -03 03:50:34,186 INFO Douglas County CO: 49/49 inspections had violation details
   search POSTs (windows logged): 30 ; geocode lines: ['2026-11-03 03:50:37,188 INFO Geocoding 169 records missing coordinates (Census batch)...', '2026-11-03 03:51:16,257 INFO Geocoded 151/169 records']
   total records line: ['Total inspections collected: 169']
{"name": "Sushi Hana Fusion Resturaunt", "address": "5610 W Grand Pkwy S Ste 300", "city": "Richmond", "state": "TX", "zip": "77406-5805", "latitude": 29.702662333323, "longitude": -95.776467347094, "inspection_date": "2026-10-26", "original_score": 70, "risk_score": 70, "priority_violations": 13, "priority_foundation_violations": 3, "core_violations": 2, "total_violations": 18, "violations": [["cross_contamination", "priority", "8: 2-301.12(C) To avoid re-contaminating their hands or surrogate prosthetic devices, food employees may use disposable "], ["temperature_control", "priority", "10: 5-202.12(A-B) A handwashing sink shall be equipped to provide water at a temperature of at least 100\u00b0F (38\u00b0C) throug"]], "inspection_type": "Food Safety Inspection", "purpose": "Food Borne Illness", "results": "", "source": "Fort Bend County Health Dept", "source_url": "https://inspections.
{"name": "Roxborough Valero - Krispy Krunchy Chicken", "address": "7887 N Rampart Range Rd", "city": "Littleton", "state": "CO", "zip": "80125-9342", "latitude": 39.476555210089, "longitude": -105.073329158192, "inspection_date": "2026-10-20", "original_score": 35, "risk_score": 91, "priority_violations": 1, "priority_foundation_violations": 2, "core_violations": 0, "total_violations": 3, "violations": [["equipment_utensils", "priority_foundation", "16: 4-501.114(A)-(E),(F)(1)-(2) The chlorine based sanitizer did not test at 50-200ppm at time of inspection. Corrective"], ["temperature_control", "priority", "20: 3-501.14 Cooked fried chicken (43\u00baF-45\u00baF) stored in the walk-in that was prepared 24 and 48 hours prior to time of i"]], "inspection_type": "Retail Food", "purpose": "Routine", "results": "", "source": "Douglas County CO Health Dept", "source_url": "https://inspections.m



## Notes for the integrator

- Slugs for `--cities` (refresh-data.yml line 98): `fortbend arapahoe douglas`
  (group 'MHD metros', +173 detail pages/week, ~22 searches) and
  `cuyahoga virginia` (group 'MHD metros (midweek)', +1,223 detail pages/week:
  ccbh 218 + 33 VDH districts 1,005; ~250 searches at the 7-day window, ~450
  with VDH's lag_days 14). `richmond`, `hamptonroads`, `nova` select VDH
  subsets. main() builds one fetch group per distinct config 'group', so a
  second workflow running only `--cities cuyahoga virginia` on another day
  gives the midweek group its own 240-minute deadline and detail-ban budget
  (1,223 details ~ 5 ban windows of 15 min: ~1.5-2 h). If both groups run in
  the Sunday job instead, expect ~+1.5-2 h of detail time.
- Existing jurisdictions now read one-day windows sorted on inspectionID:
  7-15 searches per jurisdiction-week instead of 2-20 (the AFTER column);
  detail-page counts rise only by the rows that were being lost (~6%, and
  the re-inspection pairs).
- Backfills (never search-only for acph/dchd/ccbh/VDH — demerit or no score;
  fbceh's score is a real 0-100 so a search-only pass is scale-safe but
  ships no violation text): one-day windows make a 90-day backfill 90
  windows x 1-3 pages per jurisdiction (~100-250 searches). Detail pages:
  fbceh ~750, acph ~1,100, dchd ~400, ccbh ~2,800 (~11 ban windows, ~3 h),
  VDH ~13,000 (~14+ h: stage per metro group with --until-date and
  --skip-known-inspections). History floors: fbceh 2025-03-07, acph
  2023-01-04, dchd 2022-09-07, ccbh 2024-11-04, VDH ~2024-10-01.
- Jev: no jev_reader change. score_policy derives from score_scale: Fort
  Bend 'official' (tier 'free' like every MHD county), Arapahoe / Douglas CO /
  Cuyahoga / every VDH district 'computed'. Omaha's 'Douglas County Health
  Dept' untouched (the new label is 'Douglas County CO Health Dept').
- Lord Fairfax ('VDH Lord Fairfax', va-lord-fairfax) is the Winchester-area
  district, NOT Fairfax County; Fairfax County is not on the portal (its
  district link goes to fairfaxcounty.gov) and stays the 'fairfax' source.
- Residual risks: (a) two inspections of ONE permit on the SAME day still
  collapse to the latest (seen twice on Placer in the probes; not measurable
  here without permit pages); (b) VDH 'Washington'/'Falls Church' cities on
  a few Arlington/Alexandria rows are mobile units' base addresses; (c) Fort
  Bend observation lines are mostly TFER rule wording ("... Pg. 163"), so
  text-forced F's rarely fire there (its official score carries the grade);
  (d) Arapahoe/Douglas 'Pre-Operational'/'Opening' visits (score 0, no
  observations) ship as clean A's exactly as the existing CO counties do;
  (e) the sorted-pages guard fails a query only on a repeat after one
  re-read; a jurisdiction that fails raises -> group 'error' -> red run,
  nothing shipped for it, re-read next run (held).

- Bucket mode and the sort (spot check 2026-11-03, Nashville 2026-10-21,
  searchStr 'Nashville' purpose 'Routine'): sorted and unsorted reads
  returned the same 70 ids over 3 pages with no repeats either way, so a
  bucket entry could set 'sort' later; the default stays the portal order
  (one day of evidence, and the TN reads showed no loss).
- Both edited files parse with ast feature_version (3, 11) (CI runs 3.11).

## Open questions / decisions

- Whether the midweek group gets its own workflow/cron (recommended) or
  joins the Sunday run is the integrator's call.
- README/CLAUDE.md/refresh-data.yml edits are left to the integrator.
