# Regrade from stored text, 2026-10-11

Rows written before the weekly run read violations with Jev carried the regex's answers, so two restaurants with the same violation could show different grades and chips. This regrade re-read every restaurant's stored latest inspection with the weekly decide rules (`JEV_DECIDE=hazard,summary`, the rules of PR #41) and wrote the result back. Scores did not move.

## How

1. `export-d1.yml` exported the live database at 2026-10-11 22:28 UTC: 330,340 restaurants, every one with its latest inspection; 171,205 have stored violation text.
2. `scripts/jev_regrade.py --judge` asked Jev about the 91,886 stored lines it had never read. All were answered: 11,671 requests, none failed, 320.5 million input tokens, about $13.46. Every line on a complete inspection now has an answer (108,305).
3. The same script wrote `data/d1_seed_2026-10-11_jev_regrade.sql.gz` (146,339 guarded row updates) and the data.js changes.

## What may change

D1 keeps at most 20 lines per inspection, each cut at 400 characters, so the regrade only changes what the stored text fully explains:

- A text-forced F changes only when today's regex reproduces the stored F decision from the stored lines (the reader that wrote the row) and no line was cut. San Francisco, NYC and Houston print only the code's standard wording, so a cut there hides nothing more; every one of their 1,280 distinct stored lines is a code description.
- Chips and summaries change under the same completeness rule.
- Every UPDATE matches the row's id, inspection date and last write time, so a restaurant the weekly run or the Orange County poll rewrote after the export is left alone, and loading the seed twice changes nothing more.
- data.js records change only when they hold the same inspection, and only the 8,000 records that embed summaries get new summaries.

Held back as stored: 16,417 rows with cut text, 35 F decisions the stored text does not explain.

## Result

| change | restaurants |
|---|---|
| rows updated in D1 | 146,339 |
| grade F to A | 1,743 |
| grade F to B | 598 |
| grade F to C | 211 |
| grade A, B or C to F | 178 |
| chips changed | 103,641 |
| summaries changed | 139,304 |
| pest chip removed | 13,785 |
| pest chip added | 1,834 |
| data.js records changed | 13,753 (266 grades) |

F's removed (2,552): San Francisco 1,360 and Montgomery County 799 (pest items printed without an observation: pest chip, no F), Houston 111, Richardson 94 (flies, which never force an F), Chicago 50 (door gaps and similar read by the regex as evidence), Delaware 30, Fort Worth 25, Dallas 20 (a can of Raid, ant spray), and old regex artifacts that joined two lines ("delivery" read as "live").

F's added (178): Santa Clara 84, DC 31, Dallas 19, Boston 9 and others, nearly all rodent droppings or roaches observed, written with the evidence word after the animal ("Rodent droppings observed"), which the regex never read. Every one was read by hand. Two look wrong: "exposed black sewer lines over food storage racks" names no sewage, and "traces of gray water observed outside" is grey water, which the Phase 1 policy keeps from forcing an F. A requirement that an added sewage F name a backup or spill would block those two but also five real ones, so it was not added.

Pest chips removed (13,785) are almost all premises, harborage and proofing items that carried the chip only because the regex saw the word "vermin" or "rodent": LA County's "Premises; personal/cleaning items; vermin-proofing" (5,978), San Diego item 44, NYS item 14B, Chicago door gaps, San Francisco's litter item, and Las Vegas's combined pest-control-and-animals item, left to Jev on purpose.

## Checks

- Rows the weekly run had already written with Jev and complete text: 2,869 of 2,869 reproduce exactly under the old rules, so the script follows the weekly run's own code path.
- The whole seed applied to a SQLite copy of the export changes exactly the 146,339 intended rows to exactly the intended values, nothing else; a second load changes nothing. 3,659 statements, at most 40 rows and 37 KB each.
- D1 accepted the `UPDATE ... FROM (VALUES ...)` form in a probe that matched no rows.
- data.js: same 68,420 records in the same order, only grade, chip and summary fields differ, 23.9 MB.

## Fixed along the way (PR #41)

Reading every added F turned up one rule gap: a code title's generic list ("flies, insects, rodents and other pests") counted as naming a vermin, so a dead cricket in a daycare room and a live spider got F's. Over all 132,193 cached lines the fix changes exactly those two decisions.

## Known and left as is

Fort Worth's title "Dead insects, rodents and other pests have not been removed" gives the old regex an F for dead crickets, june bugs and water bugs (13 stored lines). Those rows were F before Jev and are F now; fixing them is a separate change to the regex fallback.
