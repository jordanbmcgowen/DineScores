# Maricopa County (Phoenix) fetcher — progress log

Branch `feat/maricopa`, worktree `/home/user/wt-maricopa`. Section header in
`dinescores_pipeline.py`: "PHOENIX / MARICOPA COUNTY, AZ (envapp.maricopa.gov + county ArcGIS)".

## Status
- [x] Recon read (contract, wiring, tests, probe report, capture NOTES.md).
- [x] Fetcher code (constants, pacer, transport, parsers, fetch_maricopa) — commit 6ab4483.
- [x] Parsers verified against every captured page (4 permit pages, 5 detail pages, not-available page).
- [x] Live test-mode run (25 records): see Live results.
- [x] Live 14-day bounded run (weekly-equivalent cost measurement) — see Live results.
- [x] Fixtures + tests/test_maricopa.py (44 tests); whole suite green (318 tests, node tests/test_relay.mjs, py_compile) at 01:14 UTC; also green under /usr/bin/python3.11 (CI's version) with PYTHONPATH pointed at the 3.13 requests package.
- [x] Post-run fixes: ArcGIS chunk, purpose filter, city-less concat guard
      ('1155 W OCOTILLO RD AZ 85240' parsed city 'Rd'), title==phrase dedupe.
- [x] Successor (03:00 UTC) resumed: suite green (321 tests, relay, py_compile),
      branch touches only the allowed region (checked `git diff --name-only 75d73d8 HEAD`;
      jev_reader.py / src/*.js were changed by the scaffold commit only).
- [x] ArcGIS 60-id chunk verified LIVE with the real helper: 150 permit ids from
      the 14-day output -> 150 points in 3 queries, 1.6 s, no 404.
- [x] F records in the 14-day output checked: all 9 text-forced F's quote a real
      observation ("Observed multiple live roaches...", "rodent droppings", "rat
      droppings"); no code-title boilerplate trips the regex (9 trips / 1,146 lines);
      0 lines without an observation part; max text 500, no newlines.
- [x] Scope rule documented in the section comment: permit type is the only
      signal; school/hospital/workplace cafeterias ship like every other source
      (data.js already holds 3,785 school-named entries from other sources).
- [x] Diagnosed the 15 'unread' detail pages (rescan 02:50-03:28 UTC, 22,147
      requests, 0 unread permit pages; report scratchpad/live/unread_report.json,
      pages saved under scratchpad/live/unread_pages/). ROOT CAUSES, both fixed:
      (1) one county item ('Violation #') can cite SEVERAL Food Code sections,
      each in its own <p class='mb-1'> with its own tier and 'Addl Notes' (12 of
      the 38 entries on those pages; e.g. item 16 'Food-Contact Surfaces' = a
      Core soda-gun citation + a Priority 0-ppm-sanitizer citation). The parser
      read only the first tier, so the header's Priority tally (which counts
      citations) disagreed -> 13 pages dropped; worse, on pages where the tally
      still matched, a second non-Priority citation was silently lost from the
      score. Now `_mcesd_citations` splits an entry at every tier marker that a
      section citation precedes and `_mcesd_violation` returns one line per
      citation. (2) a re-inspection lists the re-checked item with an EMPTY
      Comments block (2 pages): now `[]` (counted as `empty_entries`, logged),
      not unread. The tally check became asymmetric: header ABOVE the Priority
      citations read = unread (every such page was a missed citation; it is
      the check that found this bug), below = logged only (suspensions,
      re-inspections and corrected-on-site items are not tallied: 3 pages).
      Re-parse of all 15 pages + 6 fixtures: 20 parse, 0 header>parsed, 3
      header<parsed, 1 unread (the 'Inspection Not Available' fixture).
      Fixtures inspection_INSP-143596-2026.html / inspection_INSP-195554-2026.html
      and 5 tests added (52 in the module).
- [x] Post-fix live checks (03:39-03:48 UTC): test mode again -> 25 records,
      53 requests, 0 retries, 25/25 ArcGIS coordinates, 49 s; a sample of 145
      of the 14-day run's detail pages (every 10th restaurant) re-fetched and
      re-parsed: 0 unread, 140 identical, 5 (3.4%) gain a PF/Core line the old
      parser lost (scores -1/-2; one A->B), so the 14-day grade counts above are
      the PRE-fix parser's and shift slightly down; +15 recovered inspections.
- [x] Final cleanup + report (worktree clean, suite 326 OK, relay PASS,
      py_compile OK on 3.13 and 3.11).

## Fix pass (2026-11-03 04:10-04:45 UTC, after two adversarial reviews)
Reviewer findings reproduced against the code before each fix; all in the
section region plus tests/test_maricopa.py (no edits outside).
- FIXED A1/B2 (blocking): the inventory floor and the lag note were gated on
  `not limit`, but main() passes limit=50000 in weekly mode, so both were
  dead in CI (an all-empty inventory returned [] -> green 'empty').
  `_mcesd_inventory` now returns (permits, bounded); the floor applies to
  every scan the limit did not stop and the lag note is left whenever every
  permit page was read. Tests: limit=50000 vs an empty inventory raises;
  limit=50000 still leaves the lag note; limit=2 is bounded.
- FIXED A2/B1 (blocking): `_mcesd_violation` read "no <p class='mb-1'>
  matched" as the county's empty-comments entry, so a renamed paragraph
  class shipped every PF/Core-only page as a clean 100 (verified by
  mutation). Now an entry adds nothing only when its comments paragraph is
  PRESENT and empty, the entry must have a number and a description, its
  visible text outside the comments and 'Repeat Violation' flag paragraphs
  must be exactly the labels + number + title, and a flag carrying a
  citation/tier marker is not a flag. Every mutation the reviewers listed
  (class added/renamed/changed, <div> for <p>, a second mb-0 paragraph with
  text, an unknown paragraph, a missing Description) is UNREAD.
- FIXED A3 (blocking): '1825 N central AVE Phoenix, AZ AZ 85004' split to
  city 'Az'. The splitter strips a repeated trailing state and trailing
  punctuation from the city and street. The FULL live inventory (403 zips,
  25,831 rows, 21,724 food rows captured 04:17-04:19 UTC) was run through
  the old and new splitters: exactly 10 permits change, the double-state
  row plus NINE rows whose zip is nine digits without a hyphen ('850222301')
  or a cut +4 ('85043-22') that the old tail regex rejected outright (the
  whole concat shipped as the street and 'Maricopa County' as the city) —
  MCESD_CONCAT_RE now accepts those tails. No other permit's identity moves.
  Two truly city-less rows remain (city '' -> 'Maricopa County').
- FIXED A4 (should_fix): div#Violations was captured to the FIRST </div>; an
  inner div could truncate the block to nothing (= clean). Now read to its
  own closing tag with nesting counted (`_mcesd_violations_block`), and a
  block headed <h2>Violations</h2> with no entry is unread; non-entry text
  must be exactly '' or 'Violations'.
- FIXED A (bounded) row dates: a permit page row whose date is not M/D/YYYY
  now makes the page unread (a date-format change was a green empty run);
  a well-formed implausible date is skipped and counted (implausible_dates).
  300 random live permit pages re-fetched: 0 unread, 1,994 rows, four
  date shapes (d/d/dddd .. dd/dd/dddd) all parse.
- FIXED B3 / A (bounded) unread details never red: 'unread' + 'unavailable'
  pages (answered, no inspection) count toward the breaker streak (they
  reset it before) and from MCESD_UNREAD_ERROR_MIN (5) in one run the group
  is 'error'; every dropped id of the three counted outcomes is logged (was
  5 per reason). Statuses now merge (listing error + detail skipped/blocked
  both appear in the detail string).
- FIXED B nit: permit pages a listing block ate are held counted=False.
- REJECTED B4 (unread permit page holds at requested_start): correct given
  per-source freshness (an unread permit's rows could be anywhere in the
  window); free in weekly mode (hold inside the 21-day window); costs up to
  3 re-details of the backfill window ONLY for a --since-date backfill run
  WITH --freshness-file, so the recommendation is to run backfills without
  it. Documented in the section comment.
- NOT CHANGED (integrator's region, noted as risks): the ~54-minute listing
  sits in the ungated API pool (MHD groups wait for it); the portal has not
  been probed from a GitHub runner; the slug is not in refresh-data.yml;
  per-record holds/give-ups are not rendered by the report step.
- Regression: old vs new parsers identical on all 23 saved detail pages (15
  live unread pages + 8 fixtures) and 8 permit pages. Suite 328 OK on 3.13
  and 3.11, relay 24 PASS, py_compile OK.
- LIVE test mode after the fix (04:30 UTC): 25 records / 17 restaurants in
  46.6 s, 53 portal requests, 0 retries, 0 unread, 25/25 ArcGIS coordinates,
  the same 17 source_ids as the pre-fix run; grades 17 A (scores 90-100);
  results Grade A 9, Not Participating 4, Grade B 2, Grade C 1,
  Re-inspection 1; 9 violation lines, max 495 chars, no newlines, no
  email/phone, 0 pest-regex trips. Detail-page sample (383 pages: every 5th
  restaurant of the 14-day output + every B/C/F + every re-inspection +
  every pest line) re-fetched 04:36 UTC for an offline re-parse: PENDING at
  this checkpoint (see the next entry).

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
- 2026-11-03 01:03 test mode (`--mode test --cities maricopa`): 25 records in
  45 s wall, 53 portal requests (8 zip searches -> 907 food permits; 20 permit
  pages in 3 s; 25 detail pages in 39 s with 2 workers), 25/25 with ArcGIS
  coordinates, 0 unread. Grades over the 17 restaurants: 17 A (scores 95-100);
  results: Grade A 9, Not Participating 4, blank (advisory) 2, Grade B 1,
  Re-inspection 1. Violation texts carry title + observation, none trips the
  pest/sewage regex.
- 2026-11-03 01:04 started the 14-day weekly-equivalent run in the background:
  `--mode full --since-date 2026-10-20 --cities maricopa --output /tmp/maricopa_14d.json`,
  log at /tmp/claude-0/-home-user/7e1d0e8e-72d4-5821-aacd-ab7518c42dd1/scratchpad/live/maricopa_14d.log
  (pid 1081). If this file says nothing more about it, the machine restarted:
  check the log, re-run if needed.
  - Inventory: 21,722 food permits from 25,831 rows over 403 zip searches in
    102 s (2 workers, 4 req/s).
  - Listing: 21,722 permit pages, 0 unread, 3,131 s (6.9 pages/s at the 7/s
    pacer, 4 workers), 1,692 inspections dated >= 2026-10-20. NEWEST ROW ACROSS
    ALL PERMITS: 2026-10-29 (5 days before the run) -> the county posts late,
    so MCESD_POSTING_LAG_DAYS (21) widens every window; see the commit after
    f8bf4f4. The margin's value is to be confirmed from the per-date histogram.
  - Details: 1,677 pages read in 2,616 s (0.64 pages/s, 2 workers, 0.5 s delay);
    15 dropped 'unread' (parser returned None; ids not logged by that build —
    diagnostic rescan find_unread.py launched 02:50 UTC, report at
    scratchpad/live/unread_report.json). Whole run 01:04 -> 02:44 UTC
    (100 min). One proxy tunnel reset mid-run, absorbed by the retry ladder.
  - ArcGIS points: 150-id IN(...) chunks answered 404 (URL too long); Census
    geocoding filled in: 1,666/1,677 records with coordinates, 11 without.
    FIXED in f6e5414: MCESD_ARCGIS_CHUNK=60 (live probe: 75 ids/1,739-char URL ->
    200; 100 ids/2,239 chars -> 404); test pins the URL length.
  - Output: 1,677 records / 1,441 restaurants; grades (latest per restaurant)
    A 1,389 / B 39 / C 3 / F 10 (9 text-forced by named pests, 1 by score 69);
    scores 69-100, 826 at 100; results Grade A 686, Not Participating 393,
    Grade B 111, A+ 79, Re-inspection 62, blank 46, ...; purposes Routine
    1,147, CI Routine 177, Re-Inspection 71, Advisory 45, 'Food AMC Trainings' 1
    (a training visit shipped as an inspection — FIXED in f6e5414:
    MCESD_SKIP_PURPOSE_RE drops advisory/training/review visits before the
    detail phase; all 45 advisories had 0 violations and no grade). Cities: Phoenix 498, Mesa 203, Scottsdale 131, Chandler
    91, Gilbert 83, Tempe 82, Glendale 81, Queen Creek 45, ... (multiword
    names split correctly: Sun City 18, Sun City West 11).
  - Per-date counts Oct 20..29: 170,174,177,123,6,4,149,228,208,202 then NOTHING
    dated Oct 30-Nov 3: a sharp ~5-day publication embargo, no visible taper.
    MCESD_POSTING_LAG_DAYS=21 kept as insurance against a late tail that one
    run cannot see (self-healing alone tolerates ~6 days of lag worst case).

## Weekly-equivalent cost and backfill estimate (from the 14-day run, 2026-11-03)
- Measured: inventory 403 zip searches in 102 s (2 workers, 4/s); listing 21,722
  permit pages in 3,131 s = 6.97 pages/s, flat for the whole run (2,000 pages
  every ~287 s), 0 unread, no 429/403/5xx seen at INFO level; details 1,677
  pages in 2,616 s = 0.64 pages/s at 2 workers + 0.5 s delay (~2.6 s server
  time per page, as the probe measured). 23,826 portal requests, 97.6 min of
  fetcher wall time for a 14-day window; Census geocoding then took 3 min.
  No sign of throttling at these rates (the probe saw 29.8 pages/s tolerated).
- Volume: 1,692 inspections dated in the 10 published days Oct 20-29 (~169 a
  calendar day, ~1,180 a week; 3% are advisory/training visits now skipped).
- Weekly run (before `--skip-known-inspections` removes the embedded
  restaurants' stored dates): inventory 2 min + listing 52 min + details for
  the widened window: MCESD_POSTING_LAG_DAYS=21 -> ~3,450 pages, 90 min
  (run ~143 min, ~25,600 requests); 14 -> ~2,300 pages, 60 min (~114 min);
  10 -> ~1,650 pages, 43 min (~97 min). The county's embargo measured sharp
  (~5 days, no taper), so 10-14 is defensible once a few runs confirm no
  late tail; 21 is the shipped insurance. All fit the 240-minute deadline.
- 90-day backfill: the same 54 min of inventory+listing (a permit page lists
  the whole 3-year history, so a longer window costs no extra listing) plus
  ~14,800 detail pages = 6.4 h at 2 workers (~37,000 requests): more than one
  weekly deadline. Either a one-off local run (`--mode full --since-date
  <90 days ago> --cities maricopa`, no deadline) or let the weekly job drain
  it: details run oldest-first, a deadline cut holds the newest tail
  (counted=False) and the next run resumes there, ~7,000 pages per run after
  the listing -> 2-3 Sunday runs. MCESD_DETAIL_WORKERS=4 would halve the time
  if the server scales (unverified; it is a server-latency bound).
- Caveat: an unreadable permit page in a backfill run holds the window at the
  backfill's start (freshness has no per-permit scope), so the following run
  re-details that window once (minus skip-known); in weekly mode the hold is
  free because the window is already widened to MCESD_POSTING_LAG_DAYS.

## Open questions
- Posting lag: measured 5 days at the newest end (newest row 2026-10-29 on
  2026-11-03 across all permits). The spread (how many days until a date is
  'full') comes from the 14-day run's per-date histogram; MCESD_POSTING_LAG_DAYS
  must exceed it.
- Detail-date vs row-date: the ROW date ships (commit after a9e94f7); a
  mismatch is only logged. Both come from the same county record.
