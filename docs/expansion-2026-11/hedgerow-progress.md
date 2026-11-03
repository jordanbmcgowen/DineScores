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

- Mesquite public portal found via the city site search:
  https://mesquite.hedgerowsoftware.com/ (SPA + same API, proxied to
  msqt-app-prod); fetcher now uses it (base + source_url), route scheme
  #/{program}/disclosure/facility/{fid}/inspection/{iid} confirmed in all
  three bundles (DisclosureProgram -> [routeParam, 'disclosure']).
- tests/test_hedgerow.py: 51 tests; full suite 325 OK, node relay PASS,
  py_compile OK (2026-11-03).
- Live test mode Lubbock: 25 records / 18 facilities / 48 requests / 30 s,
  25/25 with coordinates (oldest-first from the 2023-11 floor).

- Live test mode (2026-11-03): Pima 25 records / 9 facilities / 35 requests
  / 25 s (5,177 listed; 630 mobile + 109 never-inspected skipped); Mesquite
  25 / 6 facilities / 32 requests / 17 s; all with coordinates.
- Live window runs: Lubbock since 2026-10-20: 72 records / 72 facilities /
  145 requests / 60 s, 72/72 coords, scores 73-100 (A 52 B 16 C 4 by score).
  Pima since 2026-10-10 (portal newest 2026-10-17): 113 records / 112
  facilities / 99 s, 113/113 coords, A 98 B 11 F 3 by score (two Fail caps,
  one Probation), 4 genuine text F's (dead roaches, rodent droppings).
- FOUND + FIXED: Lubbock's TFER item title 'No evidence of Insect
  Contamination, Rodent, Other Animals' tripped the regex text-F on every
  citation (10 false F's in 72 records: fly tape, door gaps, ceiling tiles).
  The negation is now stripped (_HH_NEGATED_TITLE_RE); re-running Lubbock.

- Fix shipped: the Texas pest criterion now ships as 'Pest control:
  <observation>' (_HH_PEST_TITLE_RE): the form title 'No evidence of Insect
  Contamination, Rodent, Other Animals' tripped the regex text-F on its own
  and, with 'Rodent' left in a heading, across the '|||' line join. Lubbock
  window re-run (final code): 72 records, grades by score == by score+text
  (A 52 B 16 C 4), 0 false F's (was 10). jev_reader edit reverted (no longer
  needed; zero edits outside the section).
- Address fixes from Mesquite data: a 2-letter state in the city slot
  ('3220 Gus Thomasson Mesquite 101, TX 75150') and a street suffix before
  the comma ('1700 Rodeo Dr, TX 75149') both mean "no city given".
- Mesquite window (first run, pre-fix code): 136 records / 689 facility
  lists / 826 requests / 296 s fetch, 310 s wall incl. Census geocoding;
  135/136 with coordinates afterwards (1 unresolved); scores 84-100
  (A 127 B 9 by score). Re-running on the final code.
- Suite: 326 tests OK (exit code checked), node tests/test_relay.mjs PASS,
  py_compile OK.

- Mesquite window re-run (final code, 2026-11-03): 136 records / 689
  facility lists / 826 requests / 289 s fetch, 303 s wall incl. geocoding;
  136/136 with coordinates (Census filled the 39% the portal lacks); every
  city parsed as Mesquite; 0 text-forced F's (was 8 with the raw title);
  A 127 / B 9 by score. Suite also green under python3.11 (CI's version).

## Status before the review: DONE (2026-11-03); see the fix pass below.

## Open questions
- Pima 'Needs Improvement' computes to a B (e.g. 93 for P+C+C); only Fail /
  IHH / Probation cap the score. Owner policy call.
- The shared regex misses 'Accumulation of rodent feces' (evidence word after
  the pest word); Jev's hazard decision covers it in the weekly run.
- Mobile units dropped (Pima 630, Lubbock 146) — pinned at commissaries
  otherwise; flip HH_MOBILE_TYPE_RE if the owner wants them.

## Fix pass (2026-11-03, after two adversarial reviews)

Pipeline changes (all inside the Hedgerow section):
- 4xx classes: only 400/404/410 are permanent (dropped, not held); 401/403/
  408/429/5xx/timeouts/non-JSON are transient (retried, dropped AND held).
- Per-portal breaker `HH_BREAKER_TRIP`=3 consecutive exhausted requests ->
  `_HH_BLOCKED[slug]`, every later request fails at once (`_HhError.blocked`),
  unread tail held counted=False, group 'blocked'. Failures in a phase where
  the breaker tripped are held uncounted too (a block is not the record's
  fault). Mass rule: >= `HH_MASS_FAILURE_MIN` (10) held records that are also
  > `HH_MASS_FAILURE_FRACTION` (10%) of the reads -> group 'error'.
- Unread checklists: a checklist with no cited item ships clean only when the
  inspection row agrees (`_hh_clean_is_believable`): Pima hasInfractions
  (live: True <-> cited on 12/12 samples; the all-'No Answer Selected' Fail
  observed by the reviewer has True -> unread/held), Lubbock
  criticalInfractionCount (counts every citation incl. Core: crit=2 <-> 2
  Core items in the fixture). LIVE FACT: Lubbock's and Mesquite's clean
  checklists are EMPTY LISTS (every score-100 visit probed answered []), so
  the reviewer's blunt "empty list = unread" would have dropped every clean
  Texas inspection; Mesquite (no flag, no count) trusts the official score.
- Lubbock paging: echoed pageSize must equal HH_PAGE; every page sorted
  newest-first with nulls last and continuing the previous page; stop on an
  empty page / total reached / last < since / a short page the echo confirms.
- Pima caps: 'Needs Improvement' 79 (C), 'Satisfactory' 89 (B) added to
  PIMA_RATING_RISK_CAP. Basis: the portal's own colours (Excellent/Pass green,
  Good light blue, Satisfactory orange, NI purple, Probation/Fail/IHH red) and
  the county's definition of NI (up to four P/PF items NOT corrected during
  the inspection, KOLD 2020-01-31; the step before Probation). Flyer PDF is
  unreachable (every path serves the SPA index).
- Official-score portals drop unscored visits (score None/0) even with
  citations (contract case a; the Texas form deducts 3/2/1, our 5/2/1 would
  grade them on another scale). No live instance in 208 Texas records.
- Pima's pest title 'Insects, rodents, and animals not present' joins the
  Texas title under the neutral heading, now 'Pests and animals' (the regex
  summariser read 'Pest control' as "No pest control records"); the Pima
  title's 'rodents' tripped has_active_pest_or_sewage across the '|||' join
  when the previous line ended in an evidence word (verified).
- Bootstrap / lag: `lag_margin_days` (Pima 21) widens every dated window;
  holds stay at the asked start. A window that still starts after the
  portal's newest row -> group 'error' with a seeding hint (was a green
  'empty' forever in weekly mode: 8-day window vs ~16-day publication lag).
  Evidence for the spread: Pima list on 2026-11-03 — Thu 10-16 has 15
  facilities vs 26-33 on other Thursdays (Fridays 1-2); list unchanged
  between 04:00 and 13:00 (no publication event observed).
- Lag note for an undated list (Mesquite) only after a complete read.

Rejected: A2's literal rule (empty list = unread for every portal) — see the
live fact above; applied with per-portal cross-checks instead.

### Fix-pass verification (2026-11-03, final code unless noted)
- Suite: 341 tests OK (python3.13; the Hedgerow module also OK under
  /usr/bin/python3.11), node tests/test_relay.mjs 24 PASS / 0 FAIL,
  py_compile OK. tests/test_hedgerow.py: 52 -> 67 tests.
- Test mode (--mode test, 25 records): Lubbock 25 records / 18 facility
  lists / 48 requests / 26 s, 25/25 coords; Mesquite 25 / 6 / 32 requests /
  15 s, 25/25 coords; Pima 20 of 25 candidates shipped / 9 facilities / 35
  requests / 22 s, 20/20 coords — the 5 drops are 2019-2022 checklists the
  county never filled in (3 are all 'No Answer Selected' with hasInfractions
  True; 2 are answered forms with no cited item but hasInfractions True, so
  the flag is a conservative, not a perfect, signal). Held, retried 3 runs.
- Window runs (--mode full --since-date 2026-10-20): Lubbock 72 records /
  72 facility lists / 145 requests / 60 s, 72/72 coords, A 52 / B 16 / C 4
  by score and with the text rule (0 text F's; 13 'Pests and animals'
  lines), one 500-row page (paging checks passed live). Pima (the 21-day
  margin makes the window start 2026-09-29): 314 inspections / 307
  restaurants in 248 s, 314/314 coords; by score A 268 / B 29 / C 8 / F 2,
  with text A 267 / B 27 / C 6 / F 7 — the 5 text F's are genuine (dead
  roaches, a roach in monitoring devices, rodent droppings x2, a rodent
  carcass); ratings Excellent 161 (all A) / Good 95 (A 80, B 15) /
  Educational 18 / Pass 12 / Satisfactory 12 (B 11, C 1) / Needs
  Improvement 7 (all 79 = C, the new cap) / Fail 1 (55) / Probation 1 (62);
  cities Tucson 245, Oro Valley 30, Green Valley 11, Sahuarita 10, Marana 7,
  Amado/Arivaca/Sasabe 1 each. Re-run on the final code: 5,177 listed,
  312 facilities inspected since 09-29 (630 mobile units, 109 never
  inspected skipped), 319 candidates, 5 (1.6%) dropped as unpublished
  checklists (held), 314 shipped / 632 requests / 229 s, records identical
  to the first run (307/307).
- Mesquite (since 2026-10-20, no margin): 136 inspections / 689 facility
  lists / 826 requests / 286 s fetch + 10 s Census geocoding (56/56 of the
  rows the portal published without coordinates), 136/136 coords; scores
  84-100, median 97, A 127 / B 9 by score and with the text rule (0 text
  F's; 8 'Pests and animals' lines); 24 clean records, all score 100; every
  city 'Mesquite'; dates 10-20..11-02.
- Test mode re-run on the final code: Pima 20 of 25 / 35 requests / 20 s,
  Mesquite 25 / 32 / 14 s, Lubbock 25 / 48 / 25 s (all with coordinates).
- Live Pima facility list diffed against the reviewer's capture ~35 min
  earlier (both 2026-11-03 UTC): unchanged (5,177 rows, newest 2026-10-17),
  so no publication event was observed; the frontier shape (Thu 10-16: 15
  facilities vs 26-33 on the other Thursdays, Fridays 1-2) is the evidence
  for the publication spread, not a direct observation of a late report.

### Findings disposition
- A1/B2 (4xx = permanent, no breaker, no mass rule): FIXED.
- A2 (unanswered checklist ships as 100): FIXED with per-portal cross-checks;
  the literal "empty list = unread for every portal" rule was REJECTED (it
  would drop every clean Lubbock/Mesquite inspection: their clean
  checklists are empty lists, verified live).
- A3 (Lubbock paging trusts the server): FIXED (pageSize echo, sort checks).
- A4 (no failure breaker): FIXED (same breaker).
- A should-fix (1-day overlap vs Pima's publication lag): FIXED
  (lag_margin_days 21, holds keyed to the asked start).
- B1 (Pima never bootstraps in weekly mode): FIXED twice over — the margin
  makes the first 8-day window reach the portal's batch, and a window still
  ahead of the portal's newest row reports 'error' with a seeding hint.
- B3 (NI ships as A): FIXED — NI capped at 79, Satisfactory at 89.
- B4 (unscored visit with citations computed on another scale): FIXED —
  dropped on the official-score portals.
- Nits: Pima pest title across the join FIXED (neutral heading, now
  'Pests and animals' for both forms); skip counts now logged; left as is:
  Mesquite's 400-row floor, the 'School' type (not in Lubbock's current
  vocabulary; harmless), the single-word-city deviant regex (two-word cities
  there would mis-split the common form), the paging race (dedupe on id
  covers repeats; a stable id sort is not offered for Lubbock's window).

## Status: FIX PASS DONE — branch feat/hedgerow, not pushed; integrator
squash-merges. Open for the integrator: add pima/lubbock/mesquite to
refresh-data.yml's --cities; seed Pima once (a weekly run now bootstraps on
its own thanks to the margin, but a --since-date 2026-01-01 run with
--freshness-file gives it history); README/CLAUDE.md rows; consider a
per-group default window in _expansion_job for other late publishers.
