# Wichita Falls TX (Hedgerow portal, fourth tenant)

Wichita Falls-Wichita County Public Health District, Environmental Health
(Wichita County, ~130k: Wichita Falls, Burkburnett, Iowa Park, Electra).
Built 2026-11-23 on the survey in `survey/central.md` ("Wichita Falls").

- CLI slug `wichitafalls`, fetch group `Wichita Falls`, source label
  `Wichita Falls Environmental Health` (registered in jev_reader's
  `SOURCE_TIER_SOURCES`: the district states each citation's tier), source_id
  `wfalls_<inspection GUID>`, metro `''`.
- `fetch_wichitafalls` = `_fetch_hedgehog('wichitafalls')`, an entry in
  `HH_PORTALS` configured like Lubbock (paged list sorted
  `lastInspectionDate` desc, official score, `results` 'Grade X').

## Source

`https://wfeh-portal.hedgerowsoftware.com/api/v0/portal/`, anonymous JSON,
no CAPTCHA (`startup` says `isCaptchaConfigured: false`). Food program GUID
`d88b111d-edad-49f8-83a8-8c1d7931f99b` (the same GUID as Lubbock's).

- `POST disclosure/program/facilities` (pageSize 500, sorted
  `lastInspectionDate` desc): 764 rows in two pages; the sort and the page
  size were re-verified on 2026-11-23 (all dated rows descending across both
  pages, the two never-inspected rows last). 738 of 764 rows carry
  coordinates; every in-scope facility read so far had them.
- `GET disclosure/program/{P}/facility/{F}/inspectionDetails/`: every
  inspection of a facility with `hazardScore`, `criticalInfractionCount`,
  `nonCriticalInfractionCount`, `inspectionNumber`.
- `GET disclosure/program/inspectionResults/{I}`: the cited items only
  (`Out of Compliance` / `Corrected During Inspection`), `[]` for a clean
  visit.

## Rules and why

All differences from Lubbock are config keys on the `wichitafalls` entry,
read with `.get()`: Pima, Lubbock and Mesquite carry none of them, and
`tests/test_hedgerow.py` passes unedited.

1. **Score**: the official `hazardScore` (Texas form, 3/2/1 per cited item
   and tier) is `original_score` and `risk_score`, never recomputed. Check:
   in the 2026-08-24..11-19 window, 403 of the 403 checklists that show every
   citation the row counts reproduce the score exactly as
   100 - (3/2/1 once per distinct item and tier) with the tier rule below.
2. **Tier** (`tier_from_category`): the citation's `severity` when it states
   one; else the `(P)/(PF)/(C)` the district typed at the end of the item's
   category (`severity` is null on 309 of 809 recent citations). One
   category's suffix is wrong: `Demonstration of Knowledge/Personnel (P)`
   (PIC and food-handler items) is Priority Foundation in every citation that
   states a severity (34 of 34) and the score deducts 2 for it
   (`category_tier_fixes`; without it 21 of the 221 fully-shown checklists
   in the survey's analysis would not reproduce). Categories carry no suffix
   before ~2025-04 (the severity was stated then); a citation there without
   a severity (1 of 63 sampled 2023-08..2025-03 citations) takes the suffix
   the district gives the same category later (`category_tiers`; all 69
   suffix-less citations that state a severity agree with it). A citation
   with no severity and an unknown category raises `_HhTierError`: the
   inspection is dropped and held (counted) like an unread checklist,
   never guessed as core. Measured: 0 in the 14-day window; 3 in the
   whole-history backfill (2024-10-16, 2025-01-30, 2025-01-31), all the
   HACCP/variance item under 'Conformance with Approved Procedures', a
   category never printed with a suffix. That item's 6 citations that state
   a severity say Priority Foundation and the two of the three whose rows
   show a score deduct exactly 2, so the category was added to
   `category_tiers` AFTER the backfill ran: those 3 inspections are not in
   the backfill records (see below).
3. **Infraction counts** (`count_fields`): an empty checklist is clean only
   when critical + non-critical counts are both 0 (`criticalInfractionCount`
   alone is 0 on visits whose citations are all non-critical). All 182 empty
   recent checklists with both counts 0 scored 100. 20 of 423 recent
   checklists show fewer citations than the row counts (4 empty: three
   pop-ups and a caterer, all out of scope; 16 short by one to five, still
   short three months later): they ship with the official score, as
   Lubbock's would, because the gap is permanent and the score is the
   district's.
4. **Facility type from the name** (`name_type_sep`, `name_class_re`):
   `facilityType` is null on every row; the class follows the last `' - '`
   of `facilityName`. A suffix that is a permit class (Retail/Food
   Establishment (size, including typos like 'Establihsment'), grocery
   departments: Multi-departmental Grocery Store, Produce/Meat/Fish Market,
   Bakery, Deli, (Cooking Area) Snack Bar, Automated Grocery Department,
   '(add-on)', Concession Stand, Catering Business, Pop-up ...) is cut off
   the name, so a re-classed permit keeps its restaurant id and a grocery's
   departments share the store's (worst of the day first, the house rule for
   several permits at one address). A suffix that names one stand stays in
   the name ('MPEC Kay Yeager Coliseum - NE Concession Stand (Concession
   #2)'; four stands share 1000 5th St).
5. **Scope** (`skip_types`, `skip_names`): dropped are pop-up vendors (28
   rows, most without an address; `HH_MOBILE_TYPE_RE` catches the one push
   cart), catering businesses (5; three share one kitchen at 3406 Buchanan
   St, the commissary problem the family drops mobile units for) and school
   cafeterias by name (37: elementary/middle/high/junior high/christian/
   academy school, ISD), as Lubbock drops its 'School' type. Shipped: retail
   food establishments (restaurants, stores, daycares, Head Start, hospitals,
   senior living, churches), grocery departments, concession stands (venues,
   pools, booster clubs). 693 of 764 facilities are in scope.
6. **Addresses** (`cities`): split at a known town first (two-word 'Iowa
   Park' and 'Wichita Falls', commas inside streets like '106 S Red River,
   Expressway Burkburnett TX', rows without a state). All 693 in-scope rows
   land on Wichita Falls (625), Burkburnett (57), Iowa Park (36) or Electra
   (16) as of 2026-11-23; an unlisted town falls through to the family's
   rules. Streets keep the district's truncated suite stubs ('Kemp Blvd.,
   Suite A', 'Lawrence Rd., Ste.').
7. **Placeholders**: the canned comment 'See violation observations above'
   is never shipped as a rule name (`rule_placeholder_re`); the inspection
   type 'Please Select the Inspection Type' falls back to the reason
   (`type_placeholder_re`).
8. **Pest title**: the district prints the Texas criterion 'No evidence of
   Insect Contamination, Rodent, Other Animals', which the family's
   `_HH_PEST_TITLE_RE` already ships as 'Pests and animals: <observation>'.
   Tested: flies and door gaps stay Core lines without an F; 'Observed 3
   dead cockroaches' and 'evidence of rodent droppings' force F as named
   findings.
9. **Publication lag** (`lag_margin_days` 14): every dated window widens 14
   days (holds stay at the asked start). Evidence: the facility list read on
   Saturday 2026-11-22 05:11 UTC and again Monday 2026-11-23 11:18 UTC was
   byte-identical, newest inspection Thursday 2026-11-19; no Friday 11-20
   report was up three days later, Friday 11-14 shows none nine days later
   and the last two weeks show 15 and 17 inspections against 22-48 in the
   eleven before. Whether that is lag or a slow fortnight could not be
   settled from two snapshots one weekend apart; the margin costs ~60 extra
   requests a week and the weekly lookback already restarts the day before
   the latest shipped inspection. **Open:** re-read the list after a working
   week and count inspections dated before the earlier read that appeared
   since; lower the margin to Lubbock's 0 if none do.

## Live numbers

- `--mode test` (2026-11-23): 25 records, 39 requests, 14 s; the first run
  surfaced the suffix-less 2025 categories (one 2025-03-07 citation with no
  severity), which rule 2 now covers.
- `--mode full --since-date 2026-11-09` (read from 2026-10-26 with the
  margin): 78 records, 2026-10-26..2026-11-19, 154 requests, 49 s, 75
  facility lists, 66 restaurants. Grades A 76, B 1, F 1. The F is text
  forced and real: The Pump City Diner 2026-11-04 (score 84), 'Pests and
  animals: There is evidence of rodent droppings in the kitchen ...'. Tiers:
  26 priority, 30 priority foundation, 112 core. All 78 carry the portal's
  coordinates (nothing to geocode); cities Wichita Falls 67, Burkburnett 7,
  Electra 3, Iowa Park 1; scores 84-100, none out of range.

## Backfill (hand-off, not absorbed)

`/tmp/claude-0/-home-user-DineScores/302e47b7-d066-5f23-8c3f-df0221d21f67/scratchpad/backfill/wichitafalls/`
(`records.json.gz`, `d1.sql`, `restaurants.json`, `source_freshness.json`,
`run.log`): whole published history, `--since-date 2023-01-01`, Jev off.

- 764 facilities listed, 691 inspection lists read, 2,867 inspections in
  range; 3,560 requests in 1,189 s (~3 a second, one portal, 3 workers).
- 2,812 records, 2023-08-06..2026-11-19 (2023: 1, 2024: 458, 2025: 1,197,
  2026: 1,156); 627 restaurants; D1 SQL 627 restaurants / 2,600 inspections
  (one per restaurant per day). Status `ok`.
- Grades (`calculate_vetted_grade(risk_score, build_violation_text(...))`):
  A 2,682, B 77, C 13, F 40. 39 F's are text-forced and every one names a
  finding (live or dead cockroaches, rodent droppings/feces, rats, one
  sewage overflow); one F is the score itself. Scores 50-100, none out of
  range; types Permit 1,538, Routine 1,124 (+67 'Routine'), Re-Inspection
  52, Educational 2, 29 blank (the empty drop-down with no reason).
- Cities: Wichita Falls 2,410, Burkburnett 211, Iowa Park 128, Electra 63.
  Every record carries the portal's coordinates (0 geocoded).
- Tiers: 712 priority, 1,148 priority foundation, 2,911 core.
- Dropped, not shipped: 52 'checklist unpublished' (empty checklist, the row
  counts citations; 2024-08..2026-07, none newer) and the 3 tier-less ones
  above. All 55 are held (counted) in the run's freshness file, hold date
  2024-08-19, so the source's recorded latest there is 2024-08-18. They are
  permanent gaps, not transient: run `scripts/backfill/trim_holds.py` when
  absorbing (all 55 are older than 45 days) or the first weekly runs will
  re-read from 2024-08 until they are given up. Such a re-read would pick
  up the 3 tier-less inspections with the new category entry.

## Known gaps

- Scores sit high (recent window: 53 in the 90s, 23 at 100, 2 in the 80s):
  the district's own scale, kept as is.
- 16 of 423 recent checklists show fewer citations than the row counts; the
  official score ships, the violation list is short (rule 3).
- History before the October 2024 go-live is what the district migrated
  (2024-08 onward plus one 2023-08 inspection).
- The publication lag is not settled (rule 9).
