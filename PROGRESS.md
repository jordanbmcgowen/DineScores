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
- [x] fetch_georgia, fetch_louisiana, fixtures, tests; 15-day live windows (see
      "Live results, implementation")
- [x] FIX PASS after two adversarial reviews (2026-11-03): see "Fix pass" —
      pipeline section + tests + 6 new Louisiana fixtures; suite 333 tests OK,
      node tests/test_relay.mjs PASS, py_compile OK, 3.11 syntax (ast) OK;
      live test-mode runs OK for both states; bounded live windows re-run
      (numbers below).

## Fix pass (2026-11-03) — what the reviews found, what changed

### Fixed
- **B1 blocking (Louisiana double-counting).** The inspectionsData
  `violations` dict is NOT a finding list: one entry per (category, item,
  tag). Measured live over 810 inspections / 80 facilities: 244 adjacent
  Corrected+Repeat pairs, 37 None+Corrected, never two entries with the same
  tag, never a non-adjacent duplicate. Row-level comparison with the
  printable "Notice of Violations" (3 inspections) showed the notice can hold
  MORE rows than the JSON has items (an item cited under two code sections,
  e.g. 105 floors: 3701.5 and 3701.6 — Cuccio's Food Mart 10-26 follow-up: 7
  entries, 5 items, 6 rows; its 10-12 Meat Market routine: 14 entries, 13
  items, 19 rows = 4 critical + 15 non-critical -> risk 65 (F), not 71 (C)
  from the items, not 59 from the raw entries). So the notice is the finding
  list: `_la_parse_printable` now returns rows with the tier from the table
  they sit in (CRITICAL ITEMS / NON-CRITICAL ITEMS, each printed only when it
  has rows), and `_la_enrich` REBUILDS violations, counts and risk_score from
  the rows (JSON title by item number + narrative, '(repeat violation)' from
  the row's [Repeat] tag). The titles-only path (`_la_build`) merges adjacent
  tag duplicates into one item (repeat if any tag is Repeat) and can only
  undercount. Reviewer B's fix alone (merge) would have left the 19-row case
  at 71.
- **A1 blocking (Georgia hybrid entry).** Confirmed from the implementer's
  15-day output (ga_57441668: score 87, violations [], source_url fsimID
  57558164) and the live API (the facility now serves 57558164 with 5
  violations; the hybrid healed). `_ga_build` holds (never ships) an entry
  with `{}` under a score below 100, a non-int inspectionId, or a
  printablePath fsimID that disagrees with inspectionId. Cost check: in 165
  sampled inspections every fsimID mismatch was a Tourist Accommodation hotel
  (rtype=Tourist2014, `inspectionID=` path), excluded by permit type anyway;
  in the 1,842-record 15-day output the only mismatch was the hybrid. Same
  id checks in `_la_build` (printablePath inspectionID).
- **A2 should_fix.** An unreadable Georgia violation entry (not the 6-string
  list) now holds the inspection instead of shipping the readable lines
  (`violations = None` -> hold).
- **A3 should_fix (+ B nit).** Louisiana `{}` is held, not clean (LDH's
  clean marker is `{'0': ['None']}`; 0 of 491 probed inspections carried {}).
- **A4 should_fix (older in-window inspections of a dropped facility).**
  Reproduced: the list filter is on a facility's LAST inspection, so a
  facility a failed/cut run dropped is re-listed under its newest day only
  and its older in-window inspections fell before the re-read window. Fix:
  `TP_REREAD_MARGIN_DAYS` (14): every read facility ships its inspections
  from `since - 14` (idempotent downstream; a few % more records). Chosen
  over holding at the window start, which re-reads the whole window (for
  Louisiana ~1 h of notices) and freezes a backlog when a deadline cut
  repeats. Residual: a gap over 14 days inside a catch-up window longer than
  16 days.
- **A5 should_fix (mass failure stays green).** `TP_MASS_FAILURE_MIN` (20):
  that many unreadable facility histories make the run red
  (`_set_group_status 'error'`, records ship, holds count); more than half of
  the tried histories RAISE before any hold is registered (nothing ships,
  nothing counted against any record, the window is re-read next run).
- **B2 should_fix (three-line addresses).** `_tp_parse_address` splits on
  the LAST line; earlier lines are the street, PO Box lines dropped unless
  they are all there is ('261 S LIBERTY ST \r\nPO BOX 125 \r\nWAYNESBORO, GA
  30830' -> '261 S Liberty St' / 'Waynesboro', was city 'Po Box 125
  Waynesboro' in 2 live records). Fixed before any production id is minted.
- **B3 should_fix (inspector e-mails).** `_tp_strip_contacts` replaces
  e-mail addresses and phone numbers with '[email]' / '[phone]' in Georgia
  observations and Louisiana narratives (8 of 3,045 live GA lines).
- **B4 should_fix (notice pass unbounded in time).** `LA_PRINTABLE_MAX_MINUTES`
  (env, default 90; a normal week needs ~50-55) bounds the notice pass; an
  inspection the budget or the fetch deadline left unread ships on its items
  and `_hold_freshness(..., counted=False, key=source_id)` so the next run
  completes it (the cut tail is the newest days: cheap, self-healing). A
  FAILED notice (503s, 404, non-notice, other date, no rows) keeps the items
  for good and never holds: one flaky notice must not re-read a week of
  notices. `LA_PRINTABLE_MAX_PER_RUN` (0 = titles only) skips without holding
  (an operator's choice, e.g. backfills). The structural half of B4 — the
  pass runs inside the API pool, so every minute of it delays the sequential
  portal groups — is main()'s ordering and is left to the integrator.
- Nits fixed: a deadline that passes inside `_tp_get_json`'s retry ladder is
  a cut (uncounted hold, 'skipped'), not a counted failure or a RuntimeError
  (`_TpDeadlineCut` for a list page); the GNR county check fails closed
  (raises on an unreadable page); an unknown LDH category label holds
  instead of scoring as core; `limit` truncation holds (uncounted) at the
  newest dropped date; the '511-6-1-.NN' citation form is a rule cut too;
  category is classified AFTER the 500-char cut (GA, LA, enrich).

### Rejected or not changed (with the evidence)
- **A6 should_fix (Louisiana publication-lag margin).** Not evidenced. The
  lag is a sharp batch: at 01:16, 03:19 and 04:00 on 11-03 the newest
  published day was 10-26 and 10-27..10-29 held 0 facilities. A constant lag
  loses nothing (the window starts at latest shipped - 1). Variance would
  show as facilities appearing for already-published days: re-listing 10-20,
  10-21 and 10-26 three hours after the 01:16 run found 19 facilities absent
  from its output under ANY date — 14 institutional kitchens the name-only
  check in the probe missed (the fetcher also checks the permit name) and the
  5 address-variant rows the 01:16 run had skipped and which now parse
  (Carlos Bar, Domino's 5212, Rouses Market #80, Carmen's Bakery, Ragin
  Nutrition). A margin would cost ~7.5 min/day for Louisiana (notices). Added
  a zero-default knob, `TP_LISTING_MARGIN_DAYS` (env), so the integrator can
  widen the window if a longitudinal check (re-list a past window's days a
  week later) ever shows late entries.
- A's nit "the ordering invariant cannot see a removal-induced skipped row":
  true and not cheaply fixable (it would take a second read of every day);
  the per-day invariant catches insertions and reorders, removals mid-run
  are rare (a facility deleted during the minutes a day is paged).
- B's nits left as design: combined designations '(p, c)' take the highest
  tier; inspector names in fixtures (repo convention keeps public-record
  text verbatim); pop-up permits ('Publix Falcons activation ...') cannot be
  told from fixed premises; Gwinnett metro inconsistency and the Jev bare
  pest titles are outside the section (follow-ups below).
- PROGRESS.md collides with the dfw-holes PROGRESS.md on the base branch:
  the task requires this file name; the integrator squash-merges.

### Measurements behind the fix pass (all read-only, 2026-11-03)
- LA duplicate shape: 80 facilities (10-26 listing) / 810 inspections with
  items; adjacent (label, title) pairs: Corrected+Repeat 244, None+Corrected
  37, nothing else; notice rows vs merged items for 14 inspections: equal in
  7, notice larger in 7 (by 1-8 rows).
- GA ids: 20 newest facilities / 165 inspections: fsimID != inspectionId 7,
  all one Tourist Accommodation permit; `{}` under a score < 100: 0 (the
  reviewer's 276-inspection probe also 0); e-mails in notes 0 of 165.
- LA lag: see A6 above.

## Design decisions (so a successor does not re-derive them)
- Search URL: JSON of base64 values, `keyword` always present (LA 500s without
  it), '/' -> '%2F' in the JSON, then percent-encoded with only '%' safe.
- Windows: one single-day window per day from the lookback start to today,
  oldest first, paged (5 rows/page) until the server returns []. GA bounds are
  inclusive (from=to=day); LA bounds are strictly exclusive (from=day-1,
  to=day+1). since_date=None (test mode) = one open window from the history
  floor, newest first, stopped by `limit`. `TP_LISTING_MARGIN_DAYS` (0) can
  widen every window start.
- Ordering invariant per day: every row dated that day, decoded numeric ids
  strictly ascending, distinct; a violated day is re-read (TP_DAY_REREADS),
  then the group RAISES (fail closed).
- Detail (inspectionsData) oldest-listed-first in a 2-worker pool; a failed
  facility drops its in-window inspections + _hold_freshness(label, day,
  key=facility id); deadline cuts (also inside the retry ladder) hold with
  counted=False and _set_group_status 'skipped'. Every read facility ships
  back to `since - TP_REREAD_MARGIN_DAYS` (14). TP_MASS_FAILURE_MIN (20)
  unreadable histories: red run; more than half: raise.
- GA: official score -> original_score/risk_score; severity from the citation
  suffix (p)/(pf)/(c), highest wins, Points fallback (9 -> P, 4 -> PF, else C);
  text = item title + inspector observation, CA/code boilerplate and contact
  details stripped; 'Temporary' purpose (festival booths) dropped; metro
  'Atlanta' from the row's county EH phone (the only county signal in a row).
  Held, never shipped: violations not a dict, an unreadable entry, `{}` under
  a score below 100, a missing/non-int inspectionId or one that disagrees
  with the printable's fsimID, a listed day missing from the history.
- LA: no score -> calc_risk_score over Critical (priority) / Non-Critical
  (core). The notice's rows are the findings (tier from the table); until it
  is read, the JSON's distinct items (tag duplicates merged). Clean marker
  {'0': ['None']} -> violations [] risk 100; `{}`, a malformed entry, an
  unknown label, a bad id -> hold. Metro from the Parish column.
- Scope rule (both states): the LA permit-name tail is free text (221
  distinct tails in 250 facilities, 378/2,995 inspections with none), so no
  type allowlist is possible; TP_INSTITUTION_RE is a word-boundary keyword
  denylist over facility name (+ LA permit name) for institutional kitchens
  (school/academy/day care/dietary/hospital/nursing/jail/...; "old school"
  exempt, "cafeteria" alone not a trigger). GA's single 'Food Service'
  permit type covers its schools/hospices too, so the same rule applies.
  LA 'Summer Feeding' and GA 'Temporary' purposes are skipped.
- LA /paper/ notices: token-bucket rate limit measured 2026-11-03 (about
  4 requests then paired instant 503s with Retry-After: 3, clearing in <5 s;
  the JSON API never 503'd). LA_PRINTABLE_WORKERS=1, LA_PRINTABLE_DELAY=1.2,
  LA_PRINTABLE_RETRIES=3 honouring Retry-After (+0.5 s), ~2.3-2.6 s/notice;
  LA_PRINTABLE_MAX_MINUTES=90 and the fetch deadline bound the pass (unread
  tail held uncounted), LA_PRINTABLE_MAX_PER_RUN=6000 caps it without holding.
- GA metro: the only county signal in a row is the county EH phone
  ('For More Information Call:'); one number per county verified live on 20
  rows of each of the 9 Atlanta counties (GA_ATLANTA_EH_PHONES). GNR
  (Gwinnett/Newton/Rockdale) returned 0 rows again 2026-11-03; each run
  re-checks the three county filters over its window (3 requests; an
  unreadable check page raises).

## Live results, fix pass (2026-11-03, this container, TLS verified)
- `--mode test --cities georgia`: 25 inspections from 25 facilities, 33
  requests, 8 s wall (13 s with geocoding), Census geocoded 23/25; 12
  restaurants in the review file, all A (91-100; the newest-listed facilities
  are the Renaissance Festival's stands, skipped, and Monday's first
  inspections); reviewer B's record checker: 0 problems, 0 residues (no
  e-mails, no rule text), category == classify on every line.
- `--mode test --cities louisiana`: 25 inspections from 25 facilities, 54
  requests, 65 s wall (74 s with geocoding), 20/20 notices read, 23/25
  geocoded; grades A10/B1/C4/F1 (risk 18-100; the F is Fausto's Fried Chicken
  10-26: 12 critical + 22 non-critical rows), 105 lines all with narratives,
  risk == calc_risk_score over the shipped lines on every record, 0 problems.
- Bounded windows: see the next section (appended when the runs finished).

## Live results, implementation (2026-11-03, before the fix pass)
- georgia --mode full --since-date 2026-10-20 (15 day windows): 2,215 facilities
  listed (466 search pages incl. terminators + 3 GNR checks), 2,215 inspectionsData calls,
  1 lag probe = 2,685 requests, 221 s in requests, 9.0 min wall at TP_WORKERS=2 (4 transient
  500s, all recovered on the first retry; 0 give-ups, 0 drops). 1,882 inspections shipped
  (1,842 restaurants; institutional kitchens and Temporary booths skipped). Census geocoded
  1,754/1,882 = 93% (+ Nominatim fallback hit its 150 cap, 51 left for next run; 2
  implausible Nominatim points dropped by the existing sanity check). Grades by score
  A 1,547 / B 254 / C 31 / F 10 (risk 47-100, median 97); regex text-F would add 30.
  metro Atlanta 689 / none 1,153. Types: Routine 1,656, Initial 137, Followup 44,
  Issued Provisional Permit 5. Violation lines 3,045 (P 729 / PF 780 / C 1,536); 17%
  contain inspector-pasted rule sentences with no marker (none fire the pest/sewage
  regex) — left in place on purpose. One record of the 1,842 was the hybrid entry the
  fix pass now holds.
  => weekly (9-day window) estimate for Georgia: ~1,650 requests, ~6 min wall.
- louisiana --mode full --since-date 2026-10-20: 1,434 facilities listed (LDH posts
  ~7-8 days late, so the 15 windows held ~8 days of data: 10-20..10-26); 3,127 requests =
  ~600 search pages + 1,434 inspectionsData + 1,084 notices (+ their 503 retries) + 1 lag
  probe; 22.1 min in requests, 65.8 min wall (listing 9.6 min, details 9.2 min, notices
  47 min at one worker ~2.6 s each incl. Retry-After waits; 1,084/1,084 read, 0 failed).
  2 transient 500s on inspectionsData recovered on the first retry; 0 drops/holds;
  6 facilities skipped for an unparsable address (fixed the same day). 1,392 inspections
  (1,146 restaurants), Census geocoded 95%. Grades then A 803 / B 192 / C 101 / F 50 —
  computed from the raw JSON entries, i.e. BEFORE the fix pass: reviewer B measured 230
  of 1,146 restaurants double-counted (81 grade changes); the notice-row rebuild
  supersedes both numbers (see the bounded re-run below).
  => weekly estimate for Louisiana: ~3,100 requests, ~65 min wall (47 min notices);
     LA_PRINTABLE_MAX_MINUTES (90) bounds a catch-up week.

## Open questions / follow-ups
- Louisiana HISTORY backfills: the notices are the findings, so a backfill run
  with LA_PRINTABLE_MAX_PER_RUN=0 (titles only, as the handoff suggested for the
  ~200k historical notices) ships items-only scores that can only be too
  generous (the 19-row case reads 71 instead of 65). Either run the backfill in
  dated chunks with notices (about 2.6 s each) or accept items-only history and
  let the weekly correct the records it re-reads.
- Backfills and the freshness file: any hold registered during a backfill run
  with --freshness-file pins the next weekly window at that date (up to the
  400-day cap). Run backfills without --freshness-file, or check the file.
- Institutional names the denylist misses (seen 11-03 among LDH's listed
  facilities): 'Acad' (academy abbreviated), 'Primary' (school), 'Pantry' alone,
  'Child Development Center', 'Comunity Home' (typo), 'Head' + 'HS' (Head Start
  sites), care homes named like residences ('Fidelis Care II', 'Cottages At The
  Glen'). The permit-name tail ('Dietary', 'School Cafeteria') catches many of
  them in the real build; extend TP_INSTITUTION_RE if they show up in the weekly.
- Jev bare pest titles: a Georgia line with empty notes ships the bare title
  "Insects, rodents, and animals not present"; a Louisiana record without a
  notice ships bare titles such as "Insect/rodent control". Neither is in
  jev_reader.CITED_PEST_ITEM_TITLES (outside this section) — the integrator may
  add the GA title there.
- LDH pest wording is a templated statement ("Rodents are present in the
  establishment." / "Insects are present in the establishment. ANTS IN THE BACK
  STORAGE AREA"); PEST_EVIDENCE_RE needs an evidence word before the pest noun,
  so the regex forced 0 F's; Jev reads these lines in the weekly run. Worth a
  look at the first weekly Jev stats for Louisiana.
- Gwinnett: the MHD 'gwinnett' config has metro '' while this fetcher gives the
  nine core counties 'Atlanta'; a one-line config change (outside this section)
  would make the chip consistent.
- GA metro depends on GA_ATLANTA_EH_PHONES (county EH phone per row); if a county
  changes its number the chip silently disappears for it (records still ship).
- main() ordering: the Louisiana notice pass (~50 min a normal week) runs inside
  the API pool and delays the sequential portal groups by that much; moving the
  pool's slow members or the sequential groups is the integrator's call.
- Backfill cost: Georgia ~40-50k facilities / Louisiana 28.6k facilities for the
  whole ~3-year history (one inspectionsData each, 5 rows per search page).

## Handoff notes for the integrator (docs/workflow are theirs)
- Slugs: `georgia` / `atlanta` -> group 'Georgia' (source 'Georgia DPH', source_id
  'ga_<inspectionId>'); `louisiana` / `neworleans` -> group 'Louisiana' (source
  'Louisiana LDH', source_id 'lald_<inspectionId>'). Both are API-class groups
  (api_since, 400-day cap) run in the pool with geocode_missing_coords.
- README source-table row drafts:
  | Georgia (statewide, 156 counties; Atlanta's Fulton/DeKalb/Cobb) | Tyler
  "publicportals" JSON API at `ga.healthinspections.us/stateofgeorgia`: one
  single-day facility search per day of the window (5 rows/page to the []
  terminator, ordering invariant checked, a shifted day re-read then the group
  errors), one `inspectionsData` call per listed facility, every read facility
  shipping back 14 days before the window. OFFICIAL 0-100 score; severity from
  the state's (p)/(pf)/(c) citation designations (Points fallback); text = form
  item + inspector observation with the corrective-action, rule and contact
  boilerplate stripped. An inconsistent entry (deductions with no items, an id
  that disagrees with its printable) is held, never shipped. 'Temporary' (event
  booth) inspections and institutional kitchens (by name) are skipped;
  Gwinnett/Newton/Rockdale are GNR Health (MHD `gwinnett`) and re-checked each
  run. No coordinates: Census-geocoded. metro 'Atlanta' from the county EH phone
  for the nine core counties. |
  | Louisiana (statewide, 64 parishes; New Orleans, Baton Rouge chips) | Same
  API at `la.healthinspections.us` (date bounds strictly exclusive; `keyword`
  key mandatory). No score: `risk = 100 - 5*Critical - 1*Non-Critical` over the
  rows of each inspection's printable "Notice of Violations" (one rate-limited
  worker, ~2.6 s each, LA_PRINTABLE_MAX_MINUTES=90; an unread notice ships the
  JSON's distinct items and is completed next run); `{'0': ['None']}` is the
  explicit clean marker, `{}` is held. Institutional kitchens skipped by name.
  Census-geocoded. |
- CLAUDE.md: add Georgia + Louisiana to the coverage list, remove them from
  "Candidate sources surveyed but not added", and note under the Jev policy
  bullet that both state their tiers (already registered in jev_reader).
- Workflow: append `georgia louisiana` to the --cities line of refresh-data.yml.
- Backfill: run `--mode full --since-date <date> --cities georgia` (and
  louisiana) in chunks, without --freshness-file (see follow-ups); the portals
  hold ~3 years (GA from 2023-11-06, LA from 2023-11-07).
