# Grading review, 2026-11-27

A review of how every source's inspections become a score and a letter,
checked against the live database (424,932 restaurants). Ranked by how many
grades each finding moved or could move.

## What was found

| # | Finding | Size | Status |
|---|---|---|---|
| 1 | Sources with no stated severity are scored by keyword, which ran harsh where the city publishes its own total | Fort Worth and Frisco: 611 F's on passing city scores (Fort Worth 15% F against Dallas 1.5%) | Fixed for Fort Worth and Frisco (the city's demerit total is the score now). Cincinnati (221 F's on "Approved - Minor Violations"), Virginia, Cuyahoga, Houston, Chicago, NYC, San Francisco, Delaware and Utah County are still keyword-read |
| 2 | Rows that are not inspections counted as a clean 100 or a flat 70 | Chicago 2,049 "Out of Business", 267 "No Entry", 25 "Not Ready"; Sacramento "Not Applicable" at 70 | Fixed |
| 3 | The owner's rule "closed or failed is an F" was only on the newer sources | Chicago Fail (108 of 163 an A or B), San Francisco Closure (45 of 70), NYC closed (44 of 142 not F), Florida emergency orders (about 90 of 100 not F) | Fixed |
| 4 | The 60/30/10 blend shows a better letter than the latest inspection | 2,826 places (70 LA County C's shown as B) | Open: a policy call |
| 5 | The pest rule was all or nothing | About 7,000 text F's; one dead roach in a trap graded like an infestation | Fixed: dead or trapped pests and ants cap at C |
| 6 | NYC's official points are not used | 638 places the city grades C show A or B | Open |
| 7 | Old inspections read as current | 37,341 places (9%) last inspected over 18 months ago | Open |

Jev is in use and healthy: it reads every new violation line in the weekly
refresh and the daily Orange County poll, decides the pest or sewage hazard
and the summary phrase, and never scores. Last run: 3,112 lines, no
fallbacks, agreement with the regex on 742 of 753 F calls, about $0.64 a week.

## What changed on 2026-11-27

1. **Fort Worth and Frisco** use the Texas form score, 100 minus the city's
   demerit total, and each line takes its form item's tier (items 1-20 three
   points, 21-33 two, 34-47 one), as Dallas, Plano and McKinney already do.
2. **Non-inspections are skipped** by the fetchers (`CHICAGO_NOT_INSPECTED`,
   `SACRAMENTO_NOT_INSPECTED`, `BOSTON_NOT_INSPECTED`).
3. **Verdict caps**: Chicago Fail, San Francisco Closure, NYC "Establishment
   Closed by DOHMH" and Florida emergency orders cap the inspection at 65
   (`VERDICT_FAIL_RISK_CAP`); San Francisco Conditional Pass at 79.
4. **Pest rule** (`pest_finding_is_minor`, `text_cap_of_lines`): a finding
   that shows only dead or trapped pests, or names no pest but ants, caps
   the grade at C. Live pests, droppings, nests, un-negated "activity" and
   sewage stay an F, as does any line the rule cannot place.

## The regrade of stored rows

`scripts/grading_regrade.py`, from two exports of the live database, wrote
`data/d1_seed_2026-11-27_grading_rules.sql.gz` (31,473 guarded statements)
and patched 633 embedded records in `public/data.js`.

| Source | Change | Grades moved |
|---|---|---|
| Fort Worth | 15,528 inspections rescored | 583 F's gone (19 to A, 448 to B, 116 to C); 1,195 B to A; 519 C to B; 185 C to A; 39 down a letter |
| Frisco | 1,738 inspections rescored | 63 F's gone; 146 B to A; 81 C up; 3 down |
| Chicago | 2,285 closed or never-inspected places removed, 209 stray rows removed, 510 Fail inspections capped | 126 to F; 96 A to B; 43 F to C (dead pests) |
| San Francisco | 132 Closure / Conditional Pass inspections capped | 57 to F; 59 to C |
| NYC | 243 closed-by-DOHMH inspections capped | 44 to F; 17 A to B |
| Florida | 699 emergency-order inspections capped | 92 to F; 97 down a letter |
| Sacramento | 569 "Not Applicable" rows removed, 20 places with nothing else removed | 66 up (C or B to A or B) |
| Boston | 54 out-of-business places removed | 2 F to C |
| Every other source | Pest rule only | 420 F to C (Santa Clara 170, Dallas 29, Tarrant 29, Orange County 23, Georgia 20) |

In all: 1,111 F's lifted (646 in Fort Worth and Frisco on the city's own
score, 465 on the pest rule) and 319 new F's from verdicts the agencies gave
themselves.

Examples (before to after):

- Mexican Inn Cafe #6, Fort Worth: F (58) to B (82). The city's own total was 21 demerits.
- Jack In The Box #4832, Fort Worth: F (69) to A (92).
- Jj Fish, Chicago: A (91) to F. The city's result was Fail.
- Madfish, St Pete Beach: A (93) to F. Emergency order recommended.
- Taylor Street Coffee Shop, San Francisco: A (94) to F. Closure.
- Jack In The Box #494, Carmichael: C (70) to A (100). The 70 was a "Not Applicable" row.
- Fletcher's Corny Dog, Dallas: F (99) to C. "Observed dead roaches in the walk in cooler."
- Lucky #723, Santa Clara County: F (95) to C. "Few dead cockroaches observed in glue trap by the back delivery door."
- Tuyet Tofu, Dallas: stays F. "Dead and live roaches."

An F stays wherever the stored text cannot settle it: no pest line among the
20 stored, or a pest line cut at 400 characters.

## Left open

- Items 4, 6 and 7 above.
- The keyword-read sources. Virginia's lines cite FDA Food Code sections, so
  a section table like Colorado's could tier them.
- Closure caps differ by source (Orange County CA 50, Sonoma 55, the rest
  65); all are F's, only the number differs.
- A Chicago or Boston place that closes later only ages out; the fetcher
  skips the out-of-business row but does not delete the restaurant.
