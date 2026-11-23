# Paragon "Inspections Online": Missoula, Gallatin, Flathead MT and Kansas City MO

Built 2026-11-23 from the Montana survey of 2026-11-22
(`survey/montana.md`) plus a live survey of Kansas City done for this build
(below). Code: the `PARAGON "INSPECTIONS ONLINE"` section of
`dinescores_pipeline.py` (`IO_PORTALS`, `_fetch_paragon`, `fetch_missoula`,
`fetch_gallatin`, `fetch_flathead`, `fetch_kansas_city`), its dispatch block
in `main()`, the four labels in `jev_reader.py`, `tests/test_paragon.py`,
`tests/fixtures/paragon/`.

| CLI slug(s) | Fetch group | Source label | Database | People |
|---|---|---|---|---|
| `missoula` | Missoula County | Missoula Public Health | `mt/missoulamissoula/inspect.nsf` | ~120k |
| `bozeman`, `gallatin` | Gallatin County | Gallatin City-County Health Department | `mt/gallatinbozeman/inspect.nsf` | ~125k |
| `kalispell`, `flathead` | Flathead County | Flathead City-County Health Department | `mt/flatheadkalispell/inspect.nsf` | ~110k |
| `kansascity` | Kansas City | Kansas City MO Health Department | `mo/usakansascity/inspect.nsf` | ~510k |

No label ends in ` Health Dept` (the MHD suffix `score_policy` keys on) and no
group collides with an MHD display name (tested). The Montana state MHD entry
already drops these three counties' placeholder rows, so there is no overlap.
Jackson County MO (MHD `mo-jackson`) covers the Kansas City suburbs and maps
a stray `KCMO` city to 'Kansas City'; the two departments inspect different
premises, so overlap is limited to such mislabelled rows.

## The platform

`https://inspectionsonline.us`, IBM/HCL Lotus Domino, one `inspect.nsf` per
agency, anonymous, no CAPTCHA or bot challenge, no robots.txt. Three GETs:

1. **List** `vw_InspectionsPubSumm-NT?SearchView&SearchOrder=4&SearchMax=0&Query=[fld_Program]+CONTAINS+kw_Food+AND+[fld_InspectionDate]%3E=MM/DD/YYYY+AND+[fld_InspectionDate]%3C=MM/DD/YYYY`
   — `<h4>N documents found</h4>` and one row per inspection: UNID, type,
   date, `N RFI` / `N critical`, a score cell. Bounds inclusive. Rows carry no
   facility.
2. **Field values** `(ag_getDocValues)?OpenAgent&xx_UNID=<unid>&xx_fldList=fld_EstabName~fld_FaciName~fld_FStreetNo~fld_FStreetName~fld_FCity~fld_FProv~fld_FPCode~fld_EstType~fld_EstTypeAlt~fld_InspectionDate~fld_AllcvCounter~fld_AllvCounter~fld_Program`
   — `~`-joined values in the order asked.
3. **Report** `(ag_dspPubDetail)?OpenAgent&pUNID=<unid>` — one row per
   violation: code, title, tier (`RISK FACTOR/INTERVENTION` in Montana,
   `CRITICAL` in Kansas City, empty otherwise), corrected-on-site, a
   boilerplate code paragraph (never shipped) and the inspector's observation.

Limits that shape the read (Montana survey, re-measured on Kansas City):

- **1,000-match cap.** A query over the cap answers `1000 documents found`
  (Kansas City 2026-08-24..09-20) or `N documents returned; 1000 found`.
  Windows are `chunk_days` long (92 Missoula/Gallatin, 61 Flathead, 7 Kansas
  City) and any window at the cap is halved until it is not; one day at the
  cap raises.
- **Paging does not page** (Start/Count returns scattered rows), so each
  window is read whole and its parsed rows must equal the header count (one
  re-read, then raise). A row dated outside the window raises (the filter not
  applied).
- **Integrity counters.** `fld_AllvCounter` (lines) and `fld_AllcvCounter`
  (higher-tier lines) were equal to the parsed rows on every report checked:
  Montana 228/228 (30 days, all three counties), Kansas City 50/50 (one week)
  and then 734 of 735 in the live 4-week run. The one exception, Thai House
  2026-11-09, prints `Critical 6 / Non-critical 1` in its own header above 10
  rows (8 critical): the counters were not refreshed after an edit. Rule:
  fewer rows (of either tier) than the counters = unread (dropped, held);
  more rows = every row ships.

## Rules chosen

- **Score.** Missoula, Gallatin and Kansas City publish no official score.
  Their HTML carries a vendor score column the agency switched off
  (`xx_SrchScorDspOpt=kw_No`); it does not track the findings (Gallatin
  `100.0` on a 3-RFI visit, Kansas City `97.4` on a report with no violation,
  `100.0` on one with two) and is never shipped. `risk_score =
  calc_risk_score` over the source's two tiers: RFI/Critical -> `priority`
  (5), the rest -> `core` (1) — Louisiana's rule. `original_score` is None.
- **Two corrections after the first backfill** (`_io_shipped_rows`, applied
  once a report has passed its counters):
  1. *A commendation is not a finding.* Flathead prints its bonus credit for
     temperature logs as rows under the Risk Factor tier ("Adequate daily
     cold holding logs are maintained. This facility exceeds regulatory
     requirements ... Bonus credit has been awarded"): 78 of the 232
     higher-tier Flathead rows in the first backfill, each costing the
     restaurant 5 points. `IO_COMMENDATION_RE` drops them; the document's
     counters count them, so they are dropped after the counter check.
  2. *Montana's Risk Factor tier is a form section, not a severity.* It
     holds the Food Code's Priority and Priority Foundation provisions
     alike. The 30 standard titles in `IO_MT_PF_TITLES` cite provisions the
     FDA Food Code marks Pf at most (date marking 3-501.17, test kit
     4-302.14, hand sink use, access, soap and water temperature 5-205.11 /
     6-301.11 / 5-202.12, PIC 2-101.11 to 2-103.11, cooling methods
     3-501.15, clean to sight and touch 4-601.11(A), pest control
     6-501.111, and the rest listed in the code comment) and ship as
     `priority_foundation` (2 points). Every other Risk Factor row stays
     `priority`, an unseen title included. Kansas City's `Critical` is the
     city's own severity and is not touched.
- **Flathead's official letter** (list row score cell; Board of Health
  Policy 2010-01 EH, adopted 2010-04-15, and the "Food Purveyor Grading
  Scale", both read from the county's PDFs) caps the computed score:

  | County letter | Demerits | Cap on risk_score | DineScores grade at the cap |
  |---|---|---|---|
  | A+ / A / A- | 0-3 / 4-7 / 8-11 | none | computed |
  | B+ / B / B- | 12-15 / 16-19 / 20-23 | 89 | B |
  | C+ / C / C- | 24-28 / 29-33 / 34-38 | 79 | C |
  | D+ / D / D- | 39-45 / 46-52 / 53-59 | 69 | F |
  | F | 60+ | 69 | F |

  The policy: "Grades of D and lower — these facilities pose considerable
  risk to the public and immediate steps must be taken or closure proceedings
  will be implemented", hence D at the F cap. A cap never raises a score.
  `N/A` and blank (pre-opening visits, some mobile/manufacturer routines) do
  not cap. An unknown letter makes the inspection unread (held) rather than
  shipping uncapped. The letter goes to `results` (`Grade D+`).
- **Jev.** All four labels are in `SOURCE_TIER_SOURCES` and
  `SOURCE_TIER_SCORE_SOURCES` (`tier_policy` fixed, `score_policy`
  source_tiers: never re-tiered or re-scored, so Flathead's cap survives).
- **Scope.** Food program only (the query's own filter, and `fld_Program`
  must hold `kw_Food`). Inspection types: Pre-Operational and Closeout never
  ship (and cost no request); `Other` and `Site Visit` ship only when they
  cite something (Missoula's `Other` at a hotel kitchen was a 4-line report,
  Gallatin's an empty note); Routine, Routine Inspection, Ad-Hoc Routine,
  Follow-Up, Complaint(/Inspection), Investigation, Ownership Change, Remodel
  Inspection, Compliance, Special, Emergency ship; anything else is skipped
  and counted, and a window that is mostly unknown types reports `error`.
  Dropped by establishment type when EVERY type the facility holds matches
  mobile / pushcart / truck / temporary / vending / school / `Fee Exempt`
  (Kansas City's `Fee Exempt` was a school kitchen 8 of 8 times), so
  `Food Service Establishment|Mobile Food Service` (a restaurant that also
  runs a truck) ships. Also dropped: a name ending in `Mobile` (`PORTER
  UNHERD COFFEE MOBILE`, typed as a restaurant), a Flathead component label
  naming a mobile/temporary unit (`Mobile (Trailer)`, `Temporary Food`, type
  often blank), the test facilities kept in the live databases (Gallatin's
  `000 Food Service Test`, Missoula's `Practice Food New`).
- **Names and ids.** `fld_EstabName` is the business; Flathead's
  `fld_FaciName` is the licensed component (`Food Service`, `Bar/Espresso`,
  `Deli`), so components share one restaurant id and `_same_day_rank` keeps
  the worst of the day (14 same-day shared ids in Flathead's 4 weeks, 11 in
  Kansas City's). ALL-CAPS names/streets are title-cased; a trailing article
  moves to the front (`1400 HOUSE, THE` -> `The 1400 House`, `Bunker Bar &
  Grill (The)`). City: `.title()` (Missoula mixes `MISSOULA`/`Missoula`), one
  Gallatin typo mapped (`Bozmean`), and Kansas City's hand-typed city
  (`Kanas City`, `Kasnas City`, `Kansas`, `Kc`) replaced by `Kansas City`
  (the department inspects KCMO only).
- **Violation text.** `title: observation`, the code paragraph never
  shipped. Kansas City titles are `Short name - whole code sentence`; the
  short name is kept when it is 3-60 characters. Flathead's `(A & B) `,
  `(A-E) `, `(All) ` subsection prefixes are dropped.
- **Pest titles.** Every title naming pests seen in the captures (`Removing
  Dead or Trapped Birds, Insects, Rodents, and Other Pests`, `Effective pest
  control measures not in place`, `Insect control devices ...`, `Openings to
  outside not protected against entry of pest ...`, `(A-E) Outer openings not
  protected against the entry of insects and/or rodents`) was tested alone and
  after a line ending in an evidence word: none forces an F. Every fixture
  report's titles are tested too. No `CITED_PEST_ITEM_TITLES` entry needed.
- **Window.** The weekly window comes from `weekly_since(group,
  FRESHNESS_LOOKBACK_PORTAL_DAYS)` (portal cap, not the 400-day API cap) and
  is read from `IO_MARGIN_DAYS` (14) earlier; holds stay at the window
  asked for. In the margin only, an inspection already in
  `--skip-known-inspections` costs one request (its report is not re-read).
  Posting lag from the UNID creation stamps: Kansas City 13 weeks, 2,692 of
  2,810 same day, 97 next day, tail 2-10 days and one at 23; Montana one at
  29 days in Gallatin.
- **Failure handling** (Hedgerow model). Listing is atomic: any failure,
  short page, cap at one day or out-of-window row raises and the group ships
  nothing. Per inspection: 400/404/410 dropped without a hold; anything else
  (5xx, 401/403, timeouts, unexpected shape, document dated another day,
  missing counters, fewer rows than counters, unknown tier label) retried
  where transient, then dropped and held with `key=<UNID>` (counted). Three
  requests in a row that exhaust their retries trip a per-database breaker
  (`blocked`; the rest held uncounted). The deadline drops the newest tail
  (oldest first; held uncounted; `skipped`). 10+ unread and >10% -> `error`.
  `_note_source_lag` from the newest listed row.
- **Weekly cost.** A weekly window (~8 days + the 14-day margin) is ~650
  Kansas City inspections, ~1,250 requests, ~21 minutes at 1 request/s
  (fewer with `--skip-known-inspections`, which halves the margin's cost);
  the three Montana databases add ~250 requests. All four share one
  throttle, so they run serially in effect (~25 minutes) inside the pool.
- **Politeness.** All four databases share one host; `_io_throttle` spaces
  request STARTS at least 1.0 s apart across every thread of the run.

## Kansas City survey (2026-11-23)

- Wrapper `/foodsafety/mousakansascity/search.htm` iframes the same
  `Search?OpenForm`; the JavaScript libraries are byte-identical to
  Missoula's (same query, same field list, `kw_Food` default program).
- Volume (food program): 2026-08-24..09-20 hit the cap (1000+), 09-21..10-18
  855, 10-19..11-22 955: ~215-250 a week. One week (11-09..15): 133
  (Veterans Day). The program filter changed nothing that week (133 both).
  Types over 13 weeks: Routine 1,803, Follow-Up 636, Complaint 212,
  Pre-Operational 118, Routine Inspection 35, Emergency 6.
- History: inspections back to at least 2008 (1990..2009 and Q1 2010 both
  cap); January 2015 549, January 2020 604, January 2025 660.
- Tiers: `CRITICAL` vs not; header `Critical Violations N` / `Non-critical
  Violations N`; `Comply by` dates; full observations. Same counters.
- Establishment types: `Restaurant/Deli <size>`, `Retail Grocery/Market
  <size>`, `Bar/Tavern`, `Retail Bakery`, `School - Public/Private`, `Fee
  Exempt` (school kitchens), `Mobile Unit/Pushcart`.
- Follow-ups are a third of the volume and usually cite nothing (they
  re-check the open items): they ship like every other source's follow-ups,
  so a restaurant's newest inspection is often a clean follow-up. Owner
  decision flagged in the report.
- **Platte County, MO** is on the platform too
  (`mo/platteplattecity/inspect.nsf`, answers the same query: 46 food
  inspections 10-19..11-22). Not built.

## Live verification

Test mode (Missoula, 30 days, newest 25): 22 records, 48 requests, 21/22
geocoded.

`--mode full --since-date 2026-11-09` (reads from 10-26), all four. The
Montana groups ran in the first combined run; Kansas City's leg of that run
hit a since-fixed logging bug after 25 minutes and was re-run alone with the
final code.

| Group | Listed | Records | Requests | Dates | Geocoded | Grades A/B/C/F | Text-forced F |
|---|---|---|---|---|---|---|---|
| Missoula | 36 | 29 | 63 | 10-27..11-20 | 28/29 | 13/8/3/5 | 0 |
| Gallatin | 57 | 33 | 80 | 10-26..11-20 | 28/33 | 20/8/5/0 | 0 |
| Flathead | 115 | 96 | 205 | 10-26..11-20 | 89/96 | 86/9/0/1 | 0 |
| Kansas City | 760 | 627 | 1,367 | 10-26..11-22 | 565/627 | 465/83/18/61 | 34 |

All `ok`, no unread inspection, nothing out of range (scores 0-100, two-letter
states, no newline or >500-char line). Kansas City wall time 23 min at ~1
request/s. Flathead's letters in the window: A+ 64, A 13, A- 12, B+ 2, D+ 1,
none 4; the D+ (Firebrand Lounge, 4 RFI + 5) computes to 75 and is capped to
69.

Kansas City's 34 text-forced F's were read one by one: 33 name a finding
(live or dead roaches, mouse/rodent droppings, a dead mouse). One is a false
F from the shared regex, not a title: Ming's Garden 2026-10-29, `Restricted
Use Pesticides: Observed a bag of mouse repellent ...` ("observed ...
mouse"). In production Jev (decide mode) answers that line; the regex rule is
shared and was not changed here.

Skipped in Kansas City's 4 weeks: Pre-Operational 25, Fee Exempt 47, schools
56, Mobile Unit/Pushcart 5.

## Backfill (2026-11-23, 12:33-14:36 UTC)

One combined run, all four databases, `--mode full --since-date 2026-08-24`
(read from 2026-08-10 with the margin), `--deadline-minutes 240`: 7,347
requests in 2 h 04 min, every group `ok`, no inspection unread, held or cut.
One combined records file (`scratchpad/backfill/paragon/records.json.gz`).

| Group | Listed | Records | Requests | Dates | Geocoded | A/B/C/F | of which text-forced F |
|---|---|---|---|---|---|---|---|
| Missoula | 123 | 83 | 187 | 08-11..11-20 | 95% | 40/28/6/9 | 0 |
| Gallatin | 245 | 157 | 369 | 08-10..11-20 | 86% | 103/32/12/10 | 4 |
| Flathead | 406 | 326 | 713 | 08-10..11-20 | 92% | 267/54/3/2 | 0 |
| Kansas City | 3,327 | 2,871 | 6,078 | 08-10..11-22 | 92% | 2114/405/123/229 | 125 |

Missoula shipped two visits of a test facility (`Practice Food New`, `100
Practice Drive`, city `Practice`) found only in this run; the rule now drops
it and the two records were removed from the hand-off file (the unfiltered
file and the run's `d1.sql`, which still holds them, sit next to it).
Gallatin's four text F's are live/dead cockroaches and mouse feces
(`Premises not maintained free of insects, rodents or other pests:
Observed ...`). Kansas City's 125 were scanned for lines that name only a
device or product: 7 records are F's on wording like `Observed an open snap
mouse trap`, `Observed 1 green rodent bait`, `Observed a bag of mouse
repellent`, `Observed hot shot roach, spider, and ant killer` with no
finding (Crows Coffee 10-15, Brooklyn Mart Deli 10-13, Happy Foods 09-15,
Brick House 08-14, Cupini's 08-11, Ming's Garden 10-29, KC Art Institute -
Cafe Nerman 08-26). That is the shared regex reading "observed ... mouse";
Jev's named-vermin guard is what answers those lines in production.

## Backfill, second run (2026-11-23, 14:46-15:09 UTC): the Montana counties

Re-read with the two corrections in place (`--since-date 2026-08-24`, the
three Montana databases only; Kansas City's first-run records were absorbed
as they were, since neither correction touches them).

| Group | Inspections | A / B / C / F now | Same inspections, first run |
|---|---|---|---|
| Missoula | 81 | 46 / 25 / 7 / 3 | 39 / 28 / 6 / 8 |
| Gallatin | 155 | 113 / 27 / 12 / 3 | 104 / 33 / 12 / 6 |
| Flathead | 324 | 297 / 23 / 2 / 2 | 266 / 53 / 3 / 2 |

- The remaining F's by score: Missoula 62, 68 and 69 (3 to 6 Priority rows
  plus 3 to 5 Pf rows each), Gallatin 48, 52 and 59 (6 or 7 Priority rows
  and a dozen others; two in West Yellowstone), Flathead 54 (the county's
  own Grade F) and the D+ capped at 69.
- Flathead: no commendation row ships (78 in the first run); 90 clean
  inspections (was 78).
- Absorbed as `data/d1_seed_2026-11-23_paragonmt_2026-08-24.sql.gz`: 482
  restaurants, 496 inspection rows, grades A 384 / B 71 / C 19 / F 8; one
  text-forced F (a failing septic system in Kalispell). 43 restaurants
  without coordinates.
- Two inspections per county were not read (connection resets on the field
  values request, three attempts each; all six were read in the first run)
  and are held: Missoula 2026-11-12, Gallatin 2026-09-09, Flathead
  2026-08-31. The first weekly run re-reads each county from its hold.
- Kansas City (first run, absorbed 2026-11-23): 2,871 inspections, 1,929
  restaurants, grades A 1,594 / B 256 / C 30 / F 49; 38 restaurants carry a
  text-forced F on their newest inspection (mice, roaches, and six drain
  back-ups the violation reader read as sewage).

## Known gaps and open points

- **Missoula was graded much harder than the county grades itself; fixed.**
  With every Risk Factor row at 5 points, 9 of 83 Missoula inspections (11%)
  were F's by score over the first backfill and 6 of 157 in Gallatin (a bar
  with 8 RFI rows = 59), while Missoula's own unpublished 2026 letters were
  A+ 269, A 148, B 101, C 7, no F. The rows were test strips, date marks,
  soap at a hand sink and a missing person in charge next to real Priority
  findings. Correction 2 above scores the Pf titles at 2 points; the numbers
  after it are in "Backfill, second run" below. Still open for the owner:
  the titles NOT in the table stay at 5 points, including a few the Food Code
  splits between P and Pf ("Food not protected from contamination",
  "Non-compliance with approved HACCP plan or variance").
- Follow-ups that cite nothing ship as 100 (all sources do this).
- No coordinates in the source: Census batch geocoding (85-97% in the
  runs above; the rest retry via Nominatim in CI).
- Platform risk: a legacy Domino host ("best viewed in Internet Explorer");
  Lewis & Clark County's database went quiet when Helena moved to the state
  system. One group per database makes that visible per county as STALE.
- Not verified: reachability from a GitHub-hosted runner (all requests went
  through this session's egress proxy).
