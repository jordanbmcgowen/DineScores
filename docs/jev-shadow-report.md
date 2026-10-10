# Jev shadow report: the violation reader on TypeSafe Jev

Phase 1 of moving DineScores' violation reader from regex to Jev. Nothing here changed a published grade. Model `jev-1.13.0` (pinned), question set `qs3`, confidence floors category 0.6, hazard 0.6, summary 0.6.

## The short version

Jev read 35,823 distinct violation texts covering 724,567 of the 958,421 stored lines: every text behind a text-forced F, every text summarized "Sewage backup/issue", every regex-unclassified text, and a 20,000-text sample of the rest weighted by how often each line appears.

Of today's 7,364 text-forced F grades, Jev keeps 4,710 and drops 2,654 (36.0%). It adds 32 F's the regex misses. Flies and gnats, which do not force an F today, would add 186 more if you say so, and pests reported without a kind (a new `pests_unspecified` hazard, see below) would add 2,396. Across all 271,853 restaurants, 8,220 letter grades change (850 of them on restaurants embedded in data.js). 161 of those changes come from the reader's own mechanics (per-line F check, explicit tier markers) and would happen with Jev switched off; the rest is Jev.

Cost measured on the qs3 runs: 3,858 input tokens per line, $0.000162 per line. A full backfill of all 250,447 distinct texts is about $40.58; a weekly refresh (about 3,970 new distinct texts a week) is about $0.64.

What needs your decision is in the last section.

## What was built

`jev_reader.py` is the reader. Jev only picks options from fixed lists: a category (the 13 `VIOLATION_CATEGORIES` plus `unclassified`), a hazard (`vermin_observed`, `pests_unspecified`, `flies_or_gnats_observed`, `sewage_observed`, `conditions_only`, `none`) and a summary phrase (the 27 phrases `_paraphrase_violation` can return plus `none_of_these`). Code does the rest: the tier comes from `SEVERITY_MAP` unless the source states one or the text carries an explicit marker, the F comes from a set in code (`F_FORCING_HAZARDS`, today's policy: vermin or sewage; not flies, and not pests reported without a kind, see the decisions), and chips and summary severity come from code maps over category and hazard. `calc_risk_score` and `calculate_vetted_grade` are untouched and a test proves they make no requests.

Fail open: a timeout, a 429 or 529 after four retries with backoff, a 401, a 422, a malformed answer, or a top answer below its confidence floor means today's regex answers that line. Every answer is cached by sha256(model id, question-set version, normalized text) in append-only gzipped JSONL under `data/jev_judgments/`; a cached line is never re-asked, and the question-set version is pinned by a fingerprint test so a wording change forces a version bump. Only the violation line is sent. The key is read from `TYPESAFE_API_KEY`, is scrubbed from every log line and exception, and request bodies are never logged.

`JEV_MODE` is `off` by default (nothing runs), `shadow` (ask and cache, regex decides) or `decide` (Jev decides the questions in `JEV_DECIDE`, default all three). Nothing is wired into the fetchers yet; that is phase 2.

### No F from a batched answer

Requests carry about 14 lines each (the three questions repeat per line, so a request is about 45k tokens). The first full shadow run turned up a batch artifact: "Item 11B: Wiping cloths dirty, not stored properly in sanitizing solutions", a New York State line stored 1,088 times, came back `vermin_observed` at confidence 1.00. Asked alone it is `none` at 0.84 to 0.96 every time, and it is `none` in every other random batch I put it in, including right after a live-roach line. Jev had answered the hazard question about a neighbouring line. Eighteen other texts, each stored once, showed the same signature (a confident pest hazard on a text with no pest word: a missing first-aid kit, dented cans, a cooler at 48F). That one common text would have put a false F on about 1,000 NYS restaurants, and the cache would have frozen it.

The reader now never takes a pest or sewage hazard from a batched request: it re-asks that line alone and caches the single-line answer. A `none` on a text that names a pest or sewage outright is re-asked the same way, so a real roach line cannot lose its F to the same confusion. Cost: one extra single-line request for roughly 7% of lines. The cached shadow answers below were re-verified this way before the grade impact was computed, and the cache format gained one rule: files load in name order and a later file wins for a key that appears twice, which is how a verification run corrects an answer without editing history.

### Tier markers (code, not Jev)

39,704 stored lines carry an explicit tier marker: `(P):`, `(Pf):`, `(C):` (Boston, Wake County), `**C**`/`**PF**` (Chicago, Wake, Multnomah) and `; Priority;` / `; Priority Foundation;` / `; Core;` (Wake County). The stored tier disagrees with the marker on 18,140 of them (45.7%). Lines by source: Wake County Open Data 35,551, Boston Open Data 3,845, Chicago Open Data 295, El Paso County CO Health Dept 13.

The parser rejects the traps: Cincinnati's Ohio-code paragraph letters (`3717-1-04.8(P) -`, `4(P) -`, 9,020 lines across Cincinnati, Dallas, Houston and Chicago) and Boston's paragraph ranges (`(A)-(P)`, 1,011 lines across El Paso County, Plano, Utah County and Boston). A real marker is followed by a colon and is not preceded by a digit, a hyphen or a closing parenthesis. Both traps are unit-tested.

Precedence in `decide()`: a tier the fetcher passes from a source field (Boston `viol_level`, Seattle RED/BLUE, demerit bands) wins, then a marker in the text, then `SEVERITY_MAP[category]`. Wake County is the one source where that order matters: its stored tier comes from the points actually deducted and its text carries the FDA designation, and they disagree on 46% of lines (a Priority item corrected on site gets fewer points). Wake's score is official, so this is display only. The fetcher's choice (points) is kept; say if you want the marker instead.

### Chips and summary severity (code maps)

Chips (`inf`):

| from | value | chip |
|---|---|---|
| category | temperature_control | temp |
| category | personal_hygiene | hygiene |
| category | equipment_utensils | equipment |
| category | facility_design | equipment |
| category | water_sewage | equipment |
| category | general_cleanliness | equipment |
| category | maintenance | equipment |
| category | employee_training | docs |
| hazard | vermin_observed | pests |
| hazard | pests_unspecified | pests |
| hazard | flies_or_gnats_observed | pests |
| hazard | sewage_observed | equipment |
| hazard | conditions_only | equipment |
| summary phrase | Permit not posted | docs |
| summary phrase | No pest control records | docs |
| summary phrase | Missing thermometers | temp |

No chip for `pest_control` by category: a door gap is not "pests"; the pests chip comes from an observed hazard. No chip for `cross_contamination`, `food_source`, `food_handling`, `waste_management`, `unclassified`, as today (today they only fire on stray words).

Summary severity and category (`vs[].severity`, `vs[].category`), precedence hazard, then phrase, then category:

| from | value | severity | category |
|---|---|---|---|
| hazard | vermin_observed | CRITICAL | Active Hazard |
| hazard | pests_unspecified | CRITICAL | Active Hazard |
| hazard | flies_or_gnats_observed | CRITICAL | Active Hazard |
| hazard | sewage_observed | CRITICAL | Active Hazard |
| phrase | Gaps/Openings (Risk of entry) | INFO | Structural Risk |
| phrase | Live roaches found | CRITICAL | Active Hazard |
| phrase | Rodent activity detected | CRITICAL | Active Hazard |
| phrase | Flies/Gnats present | CRITICAL | Active Hazard |
| phrase | Sewage backup/issue | CRITICAL | Active Hazard |
| phrase | Food spoiled/unsafe | CRITICAL | Active Hazard |
| phrase | Raw food touching ready-to-eat | CRITICAL | Active Hazard |
| phrase | Mold/Slime in ice machine or bin | CRITICAL | Active Hazard |
| phrase | Chemicals unsafe/unlabeled | CRITICAL | Active Hazard |
| phrase | Food not kept cold enough | WARNING | Risk Factor |
| phrase | Food not kept hot enough | WARNING | Risk Factor |
| phrase | Dented/Damaged cans | WARNING | Risk Factor |
| phrase | Missing thermometers | WARNING | Risk Factor |
| phrase | Hand sink blocked/dirty | WARNING | Risk Factor |
| phrase | Improper hand washing | WARNING | Risk Factor |
| phrase | Worker eating/drinking near food | WARNING | Risk Factor |
| phrase | Hair restraints missing | WARNING | Risk Factor |
| phrase | Long nails/jewelry on staff | WARNING | Risk Factor |
| phrase | Dirty food contact surfaces | WARNING | Risk Factor |
| phrase | Dishwasher not sanitizing | WARNING | Risk Factor |
| phrase | No pest control records | INFO | Documentation |
| phrase | Permit not posted | INFO | Documentation |
| phrase | Plumbing leaks | INFO | Maintenance |
| phrase | Floors/Walls dirty or damaged | INFO | Maintenance |
| phrase | Dirty equipment surfaces | INFO | Maintenance |
| phrase | Lights missing shields | INFO | Maintenance |
| phrase | Vent hood grease buildup | INFO | Maintenance |
| category | temperature_control | WARNING | Risk Factor |
| category | cross_contamination | WARNING | Risk Factor |
| category | food_source | WARNING | Risk Factor |
| category | personal_hygiene | WARNING | Risk Factor |
| category | food_handling | WARNING | Risk Factor |
| category | equipment_utensils | WARNING | Risk Factor |
| category | employee_training | INFO | Documentation |
| category | pest_control | INFO | Structural Risk |
| category | anything else | INFO | Maintenance |

One more code rule: a pest or sewage phrase ("Live roaches found", "Rodent activity detected", "Flies/Gnats present", "Sewage backup/issue") is only used when the hazard question saw that hazard. If the hazard answer is confident and says otherwise, the phrase is dropped and today's truncation is shown instead, whichever side proposed the phrase. This is what keeps the 6,532 NYC backflow lines from ever reading "Sewage backup/issue" even when Jev's own summary is unsure.

## Pilot: 200 real lines

200 stored lines (the 8 golden lines plus 192 sampled across text-forced F's, sewage summaries, unclassified, marker lines, "TITLE: observation" lines and random). The timing and token numbers are from the qs2 run; the final question set qs3 (which added `pests_unspecified`) was re-run on the same 200 lines and also passes 8 of 8 golden lines; the readings in the golden table are qs3.

| measure | value |
|---|---|
| requests | 15, 0 failed, 14 to 15 lines each |
| wall time | 2.5 s for 200 lines (4 in flight) |
| latency per request | mean 0.60 s, max 0.89 s |
| input tokens per line | 3,035 (the three questions' criteria repeat per line; the text itself is about 70) |
| cost per line | $0.000127 |
| golden lines | 8 of 8 pass |
| agreement with regex, top answer | category 107/200, hazard (F yes/no) 182/200, summary 106/200 |
| confidence median / 10th pct | category 1.00 / 0.60, hazard 0.89 / 0.52, summary 0.96 / 0.69 |
| single-line vs 14-line batch (goldens) | same top answer on 23 of 24 question-line pairs; the one difference is a hazard at 0.40 vs 0.45, both below the floor |

Golden lines, final reading:

| line | expected | Jev reading (qs3) |
|---|---|---|
| Richardson: "No Evidence of Insect contamination, rodent/other animals: There is a hole or gap..." | conditions_only, no F, "Gaps/Openings (Risk of entry)" | conditions_only (0.98), no F, "Gaps/Openings (Risk of entry)" (1.00). Today: F. |
| Richardson: "...Insects were present in the facility. Fruit flies in back area" | flies_or_gnats_observed, no F, "Flies/Gnats present" | flies_or_gnats_observed (0.98), no F, "Flies/Gnats present" (1.00). Today: F, "Rodent activity detected". |
| Chicago: "INSECTS, RODENTS, & ANIMALS NOT PRESENT: Found live cockroaches..." | vermin_observed, F stays, "Live roaches found" | vermin_observed (1.00), F, "Live roaches found" (1.00). Today the summary is the truncated title: the regex misses the plural "cockroaches". |
| Chicago: "...OBSERVED A 1/4\" GAP AT THE BOTTOM OF THE REAR EXIT DOOR..." | conditions_only, no F, "Gaps/Openings" | conditions_only (1.00), no F, "Gaps/Openings (Risk of entry)" (1.00) |
| NYC: "Anti-siphonage or back-flow prevention device not provided..." (6,532 lines) | never "Sewage backup/issue", no F | water_sewage (1.00), conditions_only (0.76), no F, summary falls to the truncated text (the hazard rule drops "Sewage backup/issue" whichever side proposes it). |
| Chicago: "NON-FOOD/FOOD CONTACT SURFACES CLEAN: shelves inside prep cooler not clean..." | general_cleanliness (Core), no temp chip | general_cleanliness (0.86), core, "Dirty equipment surfaces" (0.90), chips [equipment]. Today: food_handling (Pf), chips temp+equipment. |
| "WAREWASHING AND SANITIZING PROCEDURES" (452 lines) | no hygiene chip | equipment_utensils (1.00), none_of_these (0.99) so the text itself, chips [equipment]. Today: hygiene+equipment. |
| Boston: "Good Repair and Proper Adjustment-Equipment (C): Repair leak under the dish machine..." | Core from the marker | core from the marker; category equipment_utensils (0.71), "Plumbing leaks" (0.99) |

The first pilot (qs1) failed the NYC backflow line: Jev's summary was "Sewage backup/issue" at 0.44, below the floor, so today's regex answered, and the regex says exactly that phrase. Two fixes followed, both kept: the hazard rule above, and sharper wording (an observation says something was observed, found, noted, seen or present; a code title, a rule, a corrective instruction or the code's definition of a violation is not one, even when it names insects, rodents or sewage; sewage means sewer or septic waste, so a dripping prep-sink drain is a condition, not sewage). qs1 also read LA County's rule text "...when rodent droppings are visible, or live insects are seen during an inspection" as flies observed at confidence 1.00; qs2 reads it as conditions_only.

### Option order

The jaggedness notes say Jev can lean toward the first option. Re-asking 600 cached texts with every question's options in reverse order flipped the top answer on: category 60/600 (10.0%), 21/523 (4.0%) at confidence 0.6 or above; hazard 41/600 (6.8%), 16/534 (3.0%) at confidence 0.6 or above; summary 51/600 (8.5%), 21/543 (3.9%) at confidence 0.6 or above. The F decision itself flipped on 4/600 texts. Flips concentrate below 0.6 confidence, which is one reason the floors below sit where they do.

## Agreement with the regex

Judge set: 35,823 texts (forced_f 2,260, sewage_summary 651, unclassified 12,980, sample 20,000; the strata overlap), 35,823 judged. Reader stats this run: 0 requests, 0 failed, 0 lines judged, 35,823 from cache, 0 fell back.

"Agree" means Jev's top answer equals today's regex answer; for hazard it means the same F yes/no decision. Disagreement is not error: the hand-check below says who was right.

| question | judged texts | agree | disagree | agree % | floor | texts at or above floor |
|---|---|---|---|---|---|---|
| category | 35823 | 12903 | 22920 | 36.02% | 0.6 | 87.74% |
| hazard | 35823 | 35183 | 640 | 98.21% | 0.6 | 88.36% |
| summary | 35823 | 19846 | 15977 | 55.4% | 0.6 | 91.7% |

### Category confusion (regex row, Jev column; distinct texts)

| regex \ Jev | pest control | temperature co | cross contamin | personal hygie | food source | water sewage | food handling | equipment uten | employee train | facility desig | general cleanl | maintenance | waste manageme | unclassified | total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| pest control | 2569 | 3 | 69 | 2 | 2 | 6 | 8 | 21 |  | 1 | 63 | 12 | 48 |  | 2804 |
| temperature co |  | 2595 | 161 | 39 | 51 | 56 | 860 | 708 | 49 | 271 | 346 | 219 | 1 | 14 | 5370 |
| cross contamin | 2 | 4 | 473 | 334 | 63 | 65 | 501 | 621 | 92 | 131 | 31 | 20 | 4 | 26 | 2367 |
| personal hygie | 1 | 2 | 5 | 772 | 1 | 123 | 8 | 28 | 18 | 736 | 28 | 40 | 13 | 24 | 1799 |
| food source | 1 | 9 | 296 | 4 | 511 | 13 | 119 | 184 | 92 | 12 | 53 | 7 | 4 | 71 | 1376 |
| water sewage |  |  | 1 | 1 |  | 814 | 4 | 31 |  | 82 | 21 | 14 | 3 | 7 | 978 |
| food handling | 8 | 18 | 19 | 35 | 14 | 10 | 311 | 1194 | 401 | 117 | 618 | 248 | 148 | 4 | 3145 |
| equipment uten | 3 | 4 | 7 | 14 |  | 21 | 33 | 1261 | 15 | 93 | 422 | 366 | 22 | 6 | 2267 |
| employee train | 4 | 6 | 4 | 25 | 4 | 4 | 14 | 12 | 290 | 14 | 18 | 6 | 3 | 19 | 423 |
| facility desig | 3 | 16 | 7 | 55 | 2 | 37 | 18 | 20 | 6 | 404 | 120 | 62 | 22 | 38 | 810 |
| general cleanl | 3 | 7 | 11 | 37 | 9 | 21 | 122 | 146 | 7 | 47 | 468 | 170 | 89 | 26 | 1163 |
| maintenance | 30 | 32 | 2 | 1 | 3 | 32 | 1 | 19 | 1 | 13 | 2 | 51 | 36 | 6 | 229 |
| waste manageme |  |  |  |  |  | 3 | 5 | 16 |  | 2 | 16 |  | 64 | 6 | 112 |
| unclassified | 255 | 1412 | 913 | 1476 | 327 | 280 | 1846 | 1924 | 454 | 790 | 483 | 464 | 36 | 2320 | 12980 |

Biggest moves (texts, lines):

| regex | Jev | texts | lines |
|---|---|---|---|
| food_handling | equipment_utensils | 1194 | 87,633 |
| equipment_utensils | general_cleanliness | 422 | 50,187 |
| food_handling | general_cleanliness | 618 | 36,139 |
| personal_hygiene | facility_design | 736 | 31,256 |
| cross_contamination | food_handling | 501 | 21,901 |
| unclassified | equipment_utensils | 1924 | 13,618 |
| unclassified | cross_contamination | 913 | 12,293 |
| cross_contamination | equipment_utensils | 621 | 11,179 |
| facility_design | maintenance | 62 | 11,103 |
| unclassified | temperature_control | 1412 | 9,774 |
| temperature_control | food_handling | 860 | 9,178 |
| food_handling | employee_training | 401 | 7,355 |

### Hazard: today's F decision against Jev's hazard (distinct texts)

| regex forces F | Jev hazard | texts |
|---|---|---|
| False | none | 29124 |
| False | conditions_only | 4217 |
| True | vermin_observed | 1665 |
| True | conditions_only | 360 |
| False | flies_or_gnats_observed | 131 |
| True | flies_or_gnats_observed | 123 |
| True | pests_unspecified | 77 |
| False | sewage_observed | 49 |
| True | sewage_observed | 30 |
| False | vermin_observed | 26 |
| False | pests_unspecified | 16 |
| True | none | 5 |

### Confidence distribution (distinct texts; bucket = floor of confidence to one decimal)

| question | 0.0 | 0.1 | 0.2 | 0.3 | 0.4 | 0.5 | 0.6 | 0.7 | 0.8 | 0.9 | 1.0 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| category | 2 | 67 | 282 | 707 | 1533 | 1801 | 1814 | 2161 | 3299 | 8918 | 15239 |
| hazard | 0 | 0 | 6 | 28 | 1765 | 2371 | 2917 | 4458 | 8586 | 13773 | 1919 |
| summary | 0 | 0 | 7 | 162 | 889 | 1917 | 2237 | 2671 | 3245 | 12393 | 12302 |

## Grade impact (computed in code, today's cutoffs and weights)

Three grades per restaurant, all recomputed from the stored rows: today's functions over the stored tiers and the joined text; the new reader with every judgment withheld (per-line F check, tier markers: the reader's own mechanics); and the new reader with Jev. The recomputed today-grade matches the stored `vetted_grade` on all but 3,256 restaurants (rows whose latest inspection or history differs from what the seeds hold); the stored grade is not used as the baseline for that reason.

| grade | today | with Jev |
|---|---|---|
| A | 223,463 | 228,513 |
| B | 29,973 | 28,462 |
| C | 6,938 | 6,438 |
| F | 11,479 | 8,440 |

8,220 restaurants change letter. Decomposed: 161 change from the reader's mechanics alone (no Jev), 8,111 from Jev's judgments on top of that. 850 of the changed restaurants are in data.js.

### Where the score is re-scored, and how the sources were told apart

Jev's category can only move points where DineScores computes the score from regex tiers. The policy per source was read off the fetcher code (`SCORE_POLICY` in `scripts/jev_shadow.py`) and verified against the stored rows: for every "computed" source, `calc_risk_score` over the stored tiers reproduces the stored `risk_score` on 95% or more of latest inspections; for official sources it mostly does not. MyHealthDepartment jurisdictions are decided per record: official when the portal published a 0-100 score, computed when it published demerits (Frisco, Fort Worth, the Colorado counties, Utah County) or nothing (Clark County WA, Yolo) or the record has no score. Sources whose tiers come from a source field (Boston levels, Detroit, Minneapolis, LA points, Wake points, Seattle RED/BLUE, SNHD demerits, Richardson demerits, NYS critical flag) are never re-tiered. Chicago and NYC are re-tiered conservatively: a stored core line stays core (Chicago item 30+, NYC non-critical) and a stored Priority Foundation line never drops below it (the fetchers bump those from the item number or the critical flag, which the stored row no longer carries).

| source | policy | restaurants | recompute = stored | recompute differs |
|---|---|---|---|---|
| LA County Open Data | official | 35,609 | 25441 | 9197 |
| NYC Open Data | computed | 27,311 | 26531 | 1 |
| SC DPH Food Grades | official | 24,953 | 0 | 0 |
| NYS Department of Health | official | 21,080 | 12119 | 1339 |
| Southern Nevada Health District | official | 17,662 | 3235 | 8687 |
| Florida DBPR | official | 14,730 | 0 | 0 |
| King County Open Data | official | 11,649 | 578 | 4194 |
| Chicago Open Data | computed | 9,957 | 6059 | 3 |
| DataSF | computed | 9,849 | 5347 | 6 |
| Houston Health Dept | computed | 8,421 | 4467 | 54 |
| Dallas Health Dept | official | 7,221 | 544 | 4287 |
| Sacramento County Open Data | official | 6,202 | 0 | 1676 |
| Austin Open Data | official | 6,192 | 0 | 0 |
| Multnomah County Health Dept | official | 5,795 | 449 | 1503 |
| Louisville Metro Open Data | official | 5,135 | 0 | 0 |
| Boston Open Data | source_tiers | 5,073 | 724 | 0 |
| Montgomery County Open Data | computed | 5,017 | 1219 | 44 |
| Fort Worth Health Dept | computed | 4,330 | 2780 | 5 |
| Fairfax County Open Data | official | 3,815 | 0 | 0 |
| Delaware Open Data | computed | 3,793 | 3375 | 0 |
| Wake County Open Data | official | 3,245 | 0 | 3049 |
| Tarrant County Health Dept | official | 3,000 | 197 | 2069 |
| Cincinnati Open Data | computed | 2,960 | 1888 | 46 |
| Minneapolis Open Data | official | 2,689 | 110 | 1992 |
| Detroit Open Data | source_tiers | 2,686 | 1395 | 0 |
| Washington County OR Health Dept | official | 2,427 | 232 | 1092 |
| Plano Health Dept | official | 2,304 | 97 | 1011 |
| DC Health | official | 2,163 | 172 | 977 |
| Arlington Open Data | official | 2,099 | 0 | 0 |
| El Paso County CO Health Dept | computed | 2,067 | 1453 | 0 |
| Utah County Health Dept | computed | 1,968 | 1206 | 4 |
| Clackamas County Health Dept | official | 1,624 | 167 | 452 |
| Larimer County Health Dept | computed | 1,509 | 1330 | 0 |
| Clark County WA Health Dept | computed | 1,490 | 861 | 1 |
| Frisco Health Dept | computed | 1,372 | 690 | 6 |
| Adams County Health Dept | computed | 1,345 | 1088 | 0 |
| Weld County Health Dept | computed | 1,054 | 863 | 0 |
| Yolo County Health Dept | computed | 927 | 553 | 0 |
| Richardson Health Dept | official | 707 | 146 | 444 |
| Plano Health Dept | computed | 301 | 20 | 0 |
| Multnomah County Health Dept | computed | 94 | 0 | 0 |
| Tarrant County Health Dept | computed | 20 | 0 | 0 |
| Clackamas County Health Dept | computed | 6 | 0 | 0 |
| Dallas Health Dept | computed | 1 | 0 | 0 |
| Washington County OR Health Dept | computed | 1 | 0 | 0 |

### Letter-grade transitions by source (changed restaurants only, today to with-Jev)

| source | today | new | restaurants |
|---|---|---|---|
| NYC Open Data | B | A | 2441 |
| DataSF | F | A | 848 |
| Montgomery County Open Data | F | A | 809 |
| NYC Open Data | C | B | 572 |
| DataSF | F | B | 412 |
| Houston Health Dept | B | A | 369 |
| DataSF | B | A | 264 |
| Houston Health Dept | C | B | 215 |
| DataSF | F | C | 169 |
| DataSF | C | B | 145 |
| NYC Open Data | F | C | 141 |
| Houston Health Dept | F | C | 134 |
| Delaware Open Data | B | A | 104 |
| Delaware Open Data | A | B | 88 |
| Fort Worth Health Dept | C | B | 85 |
| Fort Worth Health Dept | B | A | 73 |
| Houston Health Dept | F | B | 64 |
| Fort Worth Health Dept | F | C | 62 |
| Richardson Health Dept | F | B | 56 |
| Houston Health Dept | F | A | 51 |
| Chicago Open Data | F | A | 48 |
| Richardson Health Dept | F | A | 41 |
| Larimer County Health Dept | B | A | 36 |
| Cincinnati Open Data | B | A | 31 |
| Cincinnati Open Data | C | B | 29 |
| El Paso County CO Health Dept | B | A | 29 |
| Houston Health Dept | A | B | 29 |
| Delaware Open Data | B | C | 28 |
| Fort Worth Health Dept | B | C | 27 |
| Cincinnati Open Data | F | C | 25 |
| El Paso County CO Health Dept | C | B | 25 |
| Fort Worth Health Dept | A | B | 24 |
| Clark County WA Health Dept | B | A | 23 |
| Delaware Open Data | C | B | 22 |
| Montgomery County Open Data | F | B | 21 |
| NYC Open Data | A | B | 20 |
| Delaware Open Data | F | A | 19 |
| Frisco Health Dept | A | B | 19 |
| Chicago Open Data | F | B | 18 |
| Delaware Open Data | C | F | 18 |
| Fort Worth Health Dept | C | F | 18 |
| Dallas Health Dept | F | B | 17 |
| Houston Health Dept | B | C | 17 |
| Utah County Health Dept | A | B | 17 |
| Utah County Health Dept | B | A | 17 |
| Chicago Open Data | B | A | 16 |
| Fort Worth Health Dept | F | B | 16 |
| Larimer County Health Dept | C | B | 16 |
| Wake County Open Data | F | A | 16 |
| Washington County OR Health Dept | F | A | 15 |
| DataSF | A | B | 14 |
| Richardson Health Dept | F | C | 14 |
| El Paso County CO Health Dept | F | C | 13 |
| Utah County Health Dept | C | B | 11 |
| Adams County Health Dept | C | F | 10 |
| Delaware Open Data | F | C | 10 |
| NYC Open Data | F | B | 10 |
| Chicago Open Data | A | B | 9 |
| Detroit Open Data | F | A | 9 |
| Larimer County Health Dept | A | B | 9 |
| Multnomah County Health Dept | F | A | 9 |
| Utah County Health Dept | B | C | 9 |
| Frisco Health Dept | B | C | 8 |
| Yolo County Health Dept | A | B | 8 |
| Adams County Health Dept | B | C | 7 |
| Cincinnati Open Data | F | B | 7 |
| Clark County WA Health Dept | C | B | 7 |
| Fort Worth Health Dept | F | A | 7 |
| Larimer County Health Dept | B | C | 7 |
| Minneapolis Open Data | F | A | 7 |
| Chicago Open Data | F | C | 6 |
| Clark County WA Health Dept | A | B | 6 |
| Dallas Health Dept | F | A | 6 |
| Delaware Open Data | F | B | 6 |
| Frisco Health Dept | B | F | 6 |
| Frisco Health Dept | C | F | 6 |
| Tarrant County Health Dept | F | A | 6 |
| Weld County Health Dept | C | B | 6 |
| Yolo County Health Dept | B | C | 6 |
| Adams County Health Dept | F | C | 5 |
| Cincinnati Open Data | C | F | 5 |
| Dallas Health Dept | A | F | 5 |
| DataSF | B | C | 5 |
| Larimer County Health Dept | C | F | 5 |
| Multnomah County Health Dept | F | B | 5 |
| Adams County Health Dept | A | B | 4 |
| Chicago Open Data | A | F | 4 |
| Chicago Open Data | C | B | 4 |
| Dallas Health Dept | B | F | 4 |
| Delaware Open Data | B | F | 4 |
| El Paso County CO Health Dept | A | B | 4 |
| Houston Health Dept | C | F | 4 |
| King County Open Data | F | B | 4 |
| LA County Open Data | F | A | 4 |
| Larimer County Health Dept | F | C | 4 |
| Utah County Health Dept | F | C | 4 |
| Washington County OR Health Dept | F | B | 4 |
| Yolo County Health Dept | B | A | 4 |
| Adams County Health Dept | B | A | 3 |
| Adams County Health Dept | C | B | 3 |
| Cincinnati Open Data | A | B | 3 |
| Cincinnati Open Data | B | C | 3 |
| LA County Open Data | F | B | 3 |
| NYC Open Data | B | C | 3 |
| Utah County Health Dept | C | F | 3 |
| Utah County Health Dept | F | A | 3 |
| Weld County Health Dept | B | A | 3 |
| Clark County WA Health Dept | C | F | 2 |
| El Paso County CO Health Dept | B | C | 2 |
| El Paso County CO Health Dept | F | B | 2 |
| Houston Health Dept | B | F | 2 |
| Houston Health Dept | C | A | 2 |
| King County Open Data | F | A | 2 |
| King County Open Data | F | C | 2 |
| Montgomery County Open Data | A | B | 2 |
| Multnomah County Health Dept | A | F | 2 |
| NYS Department of Health | F | A | 2 |
| NYS Department of Health | F | B | 2 |
| Plano Health Dept | F | A | 2 |
| Weld County Health Dept | A | B | 2 |
| Yolo County Health Dept | C | F | 2 |
| Boston Open Data | B | F | 1 |
| Boston Open Data | F | A | 1 |
| Boston Open Data | F | B | 1 |
| Chicago Open Data | B | C | 1 |
| Chicago Open Data | B | F | 1 |
| Chicago Open Data | C | F | 1 |
| Cincinnati Open Data | A | F | 1 |
| Cincinnati Open Data | B | F | 1 |
| Clackamas County Health Dept | A | F | 1 |
| Clackamas County Health Dept | B | F | 1 |
| Clark County WA Health Dept | F | C | 1 |
| DC Health | A | F | 1 |
| DC Health | F | A | 1 |
| DC Health | F | C | 1 |
| Dallas Health Dept | C | F | 1 |
| Dallas Health Dept | F | C | 1 |
| DataSF | C | F | 1 |
| Detroit Open Data | F | B | 1 |
| El Paso County CO Health Dept | F | A | 1 |
| Fort Worth Health Dept | A | F | 1 |
| Fort Worth Health Dept | B | F | 1 |
| Frisco Health Dept | B | A | 1 |
| Frisco Health Dept | F | A | 1 |
| Larimer County Health Dept | A | F | 1 |
| Minneapolis Open Data | F | B | 1 |
| Multnomah County Health Dept | B | F | 1 |
| Multnomah County Health Dept | F | C | 1 |
| NYC Open Data | C | A | 1 |
| Plano Health Dept | A | F | 1 |
| Plano Health Dept | B | A | 1 |
| Plano Health Dept | F | B | 1 |
| Plano Health Dept | F | C | 1 |
| Utah County Health Dept | F | B | 1 |
| Wake County Open Data | A | F | 1 |
| Washington County OR Health Dept | B | F | 1 |
| Washington County OR Health Dept | C | F | 1 |
| Washington County OR Health Dept | F | C | 1 |
| Weld County Health Dept | B | C | 1 |
| Weld County Health Dept | F | A | 1 |
| Weld County Health Dept | F | B | 1 |
| Weld County Health Dept | F | C | 1 |
| Yolo County Health Dept | A | F | 1 |
| Yolo County Health Dept | B | F | 1 |
| Yolo County Health Dept | C | B | 1 |
| Yolo County Health Dept | F | C | 1 |

Decomposed (step `reader_mechanics_no_jev` is what happens with Jev off; `jev_judgments` is Jev on top of that):

| step | source | from | to | restaurants |
|---|---|---|---|---|
| jev_judgments | NYC Open Data | B | A | 2441 |
| jev_judgments | DataSF | F | A | 848 |
| jev_judgments | Montgomery County Open Data | F | A | 809 |
| jev_judgments | NYC Open Data | C | B | 572 |
| jev_judgments | DataSF | F | B | 412 |
| jev_judgments | Houston Health Dept | B | A | 371 |
| jev_judgments | DataSF | B | A | 264 |
| jev_judgments | Houston Health Dept | C | B | 216 |
| jev_judgments | DataSF | F | C | 169 |
| jev_judgments | DataSF | C | B | 145 |
| jev_judgments | NYC Open Data | F | C | 141 |
| jev_judgments | Houston Health Dept | F | C | 135 |
| jev_judgments | Delaware Open Data | B | A | 104 |
| jev_judgments | Delaware Open Data | A | B | 88 |
| jev_judgments | Fort Worth Health Dept | C | B | 85 |
| jev_judgments | Fort Worth Health Dept | B | A | 72 |
| jev_judgments | Houston Health Dept | F | B | 64 |
| jev_judgments | Fort Worth Health Dept | F | C | 62 |
| jev_judgments | Houston Health Dept | F | A | 51 |
| jev_judgments | Richardson Health Dept | F | B | 49 |
| jev_judgments | Richardson Health Dept | F | A | 40 |
| jev_judgments | Chicago Open Data | F | A | 35 |
| jev_judgments | Cincinnati Open Data | B | A | 31 |
| jev_judgments | Cincinnati Open Data | C | B | 29 |
| jev_judgments | Delaware Open Data | B | C | 28 |
| jev_judgments | Larimer County Health Dept | B | A | 28 |
| jev_judgments | Fort Worth Health Dept | B | C | 27 |
| jev_judgments | Houston Health Dept | A | B | 25 |
| jev_judgments | Fort Worth Health Dept | A | B | 24 |
| jev_judgments | Cincinnati Open Data | F | C | 23 |
| jev_judgments | Delaware Open Data | C | B | 22 |
| jev_judgments | Montgomery County Open Data | F | B | 21 |
| jev_judgments | El Paso County CO Health Dept | C | B | 20 |
| jev_judgments | NYC Open Data | A | B | 20 |
| jev_judgments | Delaware Open Data | F | A | 19 |
| jev_judgments | Frisco Health Dept | A | B | 19 |
| jev_judgments | Delaware Open Data | C | F | 18 |
| jev_judgments | Fort Worth Health Dept | C | F | 18 |
| jev_judgments | Dallas Health Dept | F | B | 17 |
| jev_judgments | El Paso County CO Health Dept | B | A | 17 |
| jev_judgments | Houston Health Dept | B | C | 17 |
| jev_judgments | Utah County Health Dept | A | B | 17 |
| jev_judgments | Utah County Health Dept | B | A | 17 |
| jev_judgments | Fort Worth Health Dept | F | B | 16 |
| jev_judgments | Washington County OR Health Dept | F | A | 15 |
| jev_judgments | Richardson Health Dept | F | C | 14 |
| jev_judgments | Chicago Open Data | B | A | 12 |
| jev_judgments | DataSF | A | B | 12 |
| jev_judgments | Larimer County Health Dept | A | B | 12 |
| jev_judgments | Larimer County Health Dept | C | B | 12 |
| jev_judgments | Wake County Open Data | F | A | 12 |
| jev_judgments | Utah County Health Dept | C | B | 11 |
| jev_judgments | Adams County Health Dept | C | F | 10 |
| jev_judgments | Clark County WA Health Dept | B | A | 10 |
| jev_judgments | Delaware Open Data | F | C | 10 |
| jev_judgments | NYC Open Data | F | B | 10 |
| jev_judgments | Weld County Health Dept | A | B | 10 |
| jev_judgments | Chicago Open Data | F | B | 9 |
| jev_judgments | Detroit Open Data | F | A | 9 |
| jev_judgments | El Paso County CO Health Dept | F | C | 9 |
| jev_judgments | Larimer County Health Dept | B | C | 9 |
| jev_judgments | Multnomah County Health Dept | F | A | 9 |
| jev_judgments | Utah County Health Dept | B | C | 9 |
| jev_judgments | Clark County WA Health Dept | A | B | 8 |
| jev_judgments | Frisco Health Dept | B | C | 8 |
| jev_judgments | Yolo County Health Dept | A | B | 8 |
| jev_judgments | Adams County Health Dept | B | C | 7 |
| jev_judgments | Cincinnati Open Data | F | B | 7 |
| jev_judgments | Fort Worth Health Dept | F | A | 7 |
| jev_judgments | Minneapolis Open Data | F | A | 7 |
| jev_judgments | Dallas Health Dept | F | A | 6 |
| jev_judgments | Delaware Open Data | F | B | 6 |
| jev_judgments | Frisco Health Dept | B | F | 6 |
| jev_judgments | Frisco Health Dept | C | F | 6 |
| jev_judgments | Tarrant County Health Dept | F | A | 6 |
| jev_judgments | Yolo County Health Dept | B | C | 6 |
| jev_judgments | Adams County Health Dept | F | C | 5 |
| jev_judgments | Cincinnati Open Data | C | F | 5 |
| jev_judgments | Dallas Health Dept | A | F | 5 |
| jev_judgments | DataSF | B | C | 5 |
| jev_judgments | Larimer County Health Dept | C | F | 5 |
| jev_judgments | Multnomah County Health Dept | F | B | 5 |
| jev_judgments | Adams County Health Dept | A | B | 4 |
| jev_judgments | Chicago Open Data | A | B | 4 |
| jev_judgments | Chicago Open Data | A | F | 4 |
| jev_judgments | Chicago Open Data | C | B | 4 |
| jev_judgments | Chicago Open Data | F | C | 4 |
| jev_judgments | Dallas Health Dept | B | F | 4 |
| jev_judgments | Delaware Open Data | B | F | 4 |
| jev_judgments | El Paso County CO Health Dept | B | C | 4 |
| jev_judgments | Houston Health Dept | C | F | 4 |
| jev_judgments | Utah County Health Dept | F | C | 4 |
| jev_judgments | Washington County OR Health Dept | F | B | 4 |
| jev_judgments | Adams County Health Dept | B | A | 3 |
| jev_judgments | Adams County Health Dept | C | B | 3 |
| jev_judgments | Cincinnati Open Data | A | B | 3 |
| jev_judgments | Cincinnati Open Data | B | C | 3 |
| jev_judgments | El Paso County CO Health Dept | A | B | 3 |
| jev_judgments | NYC Open Data | B | C | 3 |
| jev_judgments | Utah County Health Dept | C | F | 3 |
| jev_judgments | Utah County Health Dept | F | A | 3 |
| jev_judgments | Weld County Health Dept | B | A | 3 |
| jev_judgments | Weld County Health Dept | C | B | 3 |
| jev_judgments | Yolo County Health Dept | B | A | 3 |
| jev_judgments | Clark County WA Health Dept | C | B | 2 |
| jev_judgments | Clark County WA Health Dept | C | F | 2 |
| jev_judgments | El Paso County CO Health Dept | F | B | 2 |
| jev_judgments | Houston Health Dept | B | F | 2 |
| jev_judgments | Houston Health Dept | C | A | 2 |
| jev_judgments | Larimer County Health Dept | F | C | 2 |
| jev_judgments | Montgomery County Open Data | A | B | 2 |
| jev_judgments | Multnomah County Health Dept | A | F | 2 |
| jev_judgments | Plano Health Dept | F | A | 2 |
| jev_judgments | Weld County Health Dept | B | C | 2 |
| jev_judgments | Yolo County Health Dept | C | F | 2 |
| jev_judgments | Boston Open Data | B | F | 1 |
| jev_judgments | Boston Open Data | F | A | 1 |
| jev_judgments | Boston Open Data | F | B | 1 |
| jev_judgments | Chicago Open Data | B | C | 1 |
| jev_judgments | Chicago Open Data | B | F | 1 |
| jev_judgments | Chicago Open Data | C | F | 1 |
| jev_judgments | Cincinnati Open Data | A | F | 1 |
| jev_judgments | Cincinnati Open Data | B | F | 1 |
| jev_judgments | Clackamas County Health Dept | A | F | 1 |
| jev_judgments | Clackamas County Health Dept | B | F | 1 |
| jev_judgments | Clark County WA Health Dept | B | C | 1 |
| jev_judgments | DC Health | A | F | 1 |
| jev_judgments | DC Health | F | A | 1 |
| jev_judgments | DC Health | F | C | 1 |
| jev_judgments | Dallas Health Dept | C | F | 1 |
| jev_judgments | Dallas Health Dept | F | C | 1 |
| jev_judgments | DataSF | C | F | 1 |
| jev_judgments | Detroit Open Data | F | B | 1 |
| jev_judgments | El Paso County CO Health Dept | C | F | 1 |
| jev_judgments | El Paso County CO Health Dept | F | A | 1 |
| jev_judgments | Fort Worth Health Dept | A | F | 1 |
| jev_judgments | Fort Worth Health Dept | B | F | 1 |
| jev_judgments | Frisco Health Dept | B | A | 1 |
| jev_judgments | Frisco Health Dept | F | A | 1 |
| jev_judgments | Larimer County Health Dept | A | F | 1 |
| jev_judgments | Minneapolis Open Data | F | B | 1 |
| jev_judgments | Multnomah County Health Dept | B | F | 1 |
| jev_judgments | Multnomah County Health Dept | F | C | 1 |
| jev_judgments | NYC Open Data | C | A | 1 |
| jev_judgments | Plano Health Dept | A | F | 1 |
| jev_judgments | Plano Health Dept | B | A | 1 |
| jev_judgments | Plano Health Dept | F | B | 1 |
| jev_judgments | Plano Health Dept | F | C | 1 |
| jev_judgments | Utah County Health Dept | F | B | 1 |
| jev_judgments | Wake County Open Data | A | F | 1 |
| jev_judgments | Washington County OR Health Dept | B | F | 1 |
| jev_judgments | Washington County OR Health Dept | C | F | 1 |
| jev_judgments | Washington County OR Health Dept | F | C | 1 |
| jev_judgments | Weld County Health Dept | F | A | 1 |
| jev_judgments | Weld County Health Dept | F | B | 1 |
| jev_judgments | Weld County Health Dept | F | C | 1 |
| jev_judgments | Yolo County Health Dept | A | F | 1 |
| jev_judgments | Yolo County Health Dept | B | F | 1 |
| jev_judgments | Yolo County Health Dept | F | C | 1 |
| reader_mechanics_no_jev | Clark County WA Health Dept | B | A | 15 |
| reader_mechanics_no_jev | Chicago Open Data | F | A | 13 |
| reader_mechanics_no_jev | El Paso County CO Health Dept | B | A | 12 |
| reader_mechanics_no_jev | Larimer County Health Dept | B | A | 11 |
| reader_mechanics_no_jev | Chicago Open Data | F | B | 9 |
| reader_mechanics_no_jev | Weld County Health Dept | B | A | 8 |
| reader_mechanics_no_jev | El Paso County CO Health Dept | C | B | 7 |
| reader_mechanics_no_jev | Richardson Health Dept | F | B | 7 |
| reader_mechanics_no_jev | Clark County WA Health Dept | C | B | 6 |
| reader_mechanics_no_jev | Houston Health Dept | A | B | 6 |
| reader_mechanics_no_jev | Larimer County Health Dept | C | B | 6 |
| reader_mechanics_no_jev | Chicago Open Data | A | B | 5 |
| reader_mechanics_no_jev | El Paso County CO Health Dept | F | C | 5 |
| reader_mechanics_no_jev | Chicago Open Data | B | A | 4 |
| reader_mechanics_no_jev | King County Open Data | F | B | 4 |
| reader_mechanics_no_jev | LA County Open Data | F | A | 4 |
| reader_mechanics_no_jev | Wake County Open Data | F | A | 4 |
| reader_mechanics_no_jev | Weld County Health Dept | C | B | 4 |
| reader_mechanics_no_jev | DataSF | A | B | 3 |
| reader_mechanics_no_jev | LA County Open Data | F | B | 3 |
| reader_mechanics_no_jev | Chicago Open Data | F | C | 2 |
| reader_mechanics_no_jev | Cincinnati Open Data | F | C | 2 |
| reader_mechanics_no_jev | King County Open Data | F | A | 2 |
| reader_mechanics_no_jev | King County Open Data | F | C | 2 |
| reader_mechanics_no_jev | Larimer County Health Dept | F | C | 2 |
| reader_mechanics_no_jev | NYS Department of Health | F | A | 2 |
| reader_mechanics_no_jev | NYS Department of Health | F | B | 2 |
| reader_mechanics_no_jev | Clark County WA Health Dept | F | C | 1 |
| reader_mechanics_no_jev | DataSF | B | A | 1 |
| reader_mechanics_no_jev | DataSF | B | C | 1 |
| reader_mechanics_no_jev | DataSF | C | B | 1 |
| reader_mechanics_no_jev | El Paso County CO Health Dept | A | B | 1 |
| reader_mechanics_no_jev | Fort Worth Health Dept | B | A | 1 |
| reader_mechanics_no_jev | Houston Health Dept | B | C | 1 |
| reader_mechanics_no_jev | Houston Health Dept | C | F | 1 |
| reader_mechanics_no_jev | Richardson Health Dept | F | A | 1 |
| reader_mechanics_no_jev | Yolo County Health Dept | B | A | 1 |
| reader_mechanics_no_jev | Yolo County Health Dept | C | B | 1 |

data.js (embedded restaurants) transitions:

| today | new | restaurants |
|---|---|---|
| A | B | 50 |
| A | F | 3 |
| B | A | 191 |
| B | C | 27 |
| B | F | 14 |
| C | B | 113 |
| C | F | 16 |
| F | A | 289 |
| F | B | 71 |
| F | C | 76 |

### Text-forced F's

A text-forced F is an F that the score does not explain (weighted and latest scores both 70 or more). Today: 7,364. Jev keeps 4,710, drops 2,654, adds 32; flies or gnats would add 186. With Jev withheld the reader alone drops 54 and adds 0: 54 of the drops are F's that no single stored line supports, which today's regex assembles across two joined lines (the evidence word in one line, the pest word at the start of the next). Per source:

| source | text-forced F today | kept | dropped | of which cross-line only | added | flies would add | unspecified pests would add |
|---|---|---|---|---|---|---|---|
| NYC Open Data | 4040 | 4040 | 0 | 0 | 0 | 0 | 0 |
| DataSF | 1344 | 3 | 1341 | 0 | 0 | 0 | 0 |
| Montgomery County Open Data | 830 | 0 | 830 | 0 | 0 | 0 | 830 |
| Chicago Open Data | 145 | 76 | 69 | 23 | 5 | 3 | 2 |
| Richardson Health Dept | 120 | 9 | 111 | 8 | 0 | 43 | 10 |
| Houston Health Dept | 108 | 0 | 108 | 0 | 0 | 0 | 0 |
| Dallas Health Dept | 105 | 81 | 24 | 0 | 10 | 25 | 2 |
| Fort Worth Health Dept | 97 | 69 | 28 | 0 | 2 | 44 | 14 |
| Multnomah County Health Dept | 93 | 78 | 15 | 0 | 3 | 12 | 0 |
| Minneapolis Open Data | 78 | 70 | 8 | 0 | 0 | 5 | 0 |
| Tarrant County Health Dept | 66 | 60 | 6 | 0 | 0 | 8 | 3 |
| DC Health | 49 | 47 | 2 | 0 | 1 | 0 | 0 |
| Wake County Open Data | 44 | 28 | 16 | 4 | 1 | 16 | 1 |
| Washington County OR Health Dept | 43 | 23 | 20 | 0 | 2 | 20 | 1 |
| Boston Open Data | 33 | 31 | 2 | 0 | 1 | 0 | 0 |
| Delaware Open Data | 29 | 0 | 29 | 0 | 0 | 0 | 0 |
| Cincinnati Open Data | 19 | 18 | 1 | 0 | 1 | 1 | 1 |
| Plano Health Dept | 19 | 15 | 4 | 0 | 1 | 1 | 1 |
| Yolo County Health Dept | 13 | 13 | 0 | 0 | 2 | 0 | 0 |
| Utah County Health Dept | 12 | 8 | 4 | 0 | 0 | 1 | 2 |
| Weld County Health Dept | 12 | 9 | 3 | 0 | 0 | 2 | 0 |
| El Paso County CO Health Dept | 11 | 8 | 3 | 0 | 0 | 2 | 0 |
| Clackamas County Health Dept | 10 | 10 | 0 | 0 | 2 | 1 | 0 |
| Detroit Open Data | 10 | 0 | 10 | 0 | 0 | 0 | 0 |
| King County Open Data | 8 | 0 | 8 | 8 | 0 | 0 | 0 |
| LA County Open Data | 7 | 0 | 7 | 7 | 0 | 0 | 0 |
| Larimer County Health Dept | 6 | 6 | 0 | 0 | 1 | 0 | 0 |
| Frisco Health Dept | 5 | 4 | 1 | 0 | 0 | 0 | 0 |
| NYS Department of Health | 4 | 0 | 4 | 4 | 0 | 0 | 1523 |
| Clark County WA Health Dept | 3 | 3 | 0 | 0 | 0 | 0 | 0 |
| Adams County Health Dept | 1 | 1 | 0 | 0 | 0 | 2 | 0 |
| Southern Nevada Health District | 0 | 0 | 0 | 0 | 0 | 0 | 6 |

### The unspecified-pests policy, both ways

Some sources record pests without naming them: NYS "Item 14A: Insects, rodents present" (1,665 lines), Montgomery County's fixed category name "Evidence of rodents or insects observed" (2,174), San Francisco's standard vermin text "Eliminate the infestation/activity of cockroaches/rodents/flies/vermin..." (1,411), SNHD "Gross unsanitary occurrences or conditions including pest infestation" (89), plus "dead insects" and "insect carcasses" lines. Today's regex forces an F on the Montgomery and SF texts (they happen to contain an evidence word next to "rodents" or "cockroaches") and not on the NYS text, so there is no consistent today to keep. Jev now returns a separate hazard, `pests_unspecified`, for all of them, and code decides. The default in this PR is that it does NOT force an F (today's stated policy names species; "insects" may be flies). The alternative is one line in `F_FORCING_HAZARDS`.

| policy | F total | grade changes vs today | text-forced F kept | dropped | added | flies would add |
|---|---|---|---|---|---|---|
| default: pests_unspecified does not force an F | 8,440 | 8,220 | 4,710 | 2,654 | 32 | 186 |
| alternative: it forces an F | 10,836 | 8,902 | 5,568 | 1,796 | 1,571 | 184 |

Transitions by source under the alternative (changed only):

| source | today | new | restaurants |
|---|---|---|---|
| NYC Open Data | B | A | 2441 |
| NYS Department of Health | A | F | 1340 |
| DataSF | F | A | 848 |
| NYC Open Data | C | B | 572 |
| DataSF | F | B | 412 |
| Houston Health Dept | B | A | 369 |
| DataSF | B | A | 264 |
| Houston Health Dept | C | B | 215 |
| DataSF | F | C | 169 |
| NYS Department of Health | B | F | 153 |
| DataSF | C | B | 145 |
| NYC Open Data | F | C | 141 |
| Houston Health Dept | F | C | 134 |
| Delaware Open Data | B | A | 104 |
| Delaware Open Data | A | B | 88 |
| Fort Worth Health Dept | C | B | 85 |
| Fort Worth Health Dept | B | A | 73 |
| Houston Health Dept | F | B | 64 |
| Fort Worth Health Dept | F | C | 58 |
| Richardson Health Dept | F | B | 52 |
| Houston Health Dept | F | A | 51 |
| Chicago Open Data | F | A | 48 |
| Richardson Health Dept | F | A | 37 |
| Larimer County Health Dept | B | A | 36 |
| Cincinnati Open Data | B | A | 31 |
| NYS Department of Health | C | F | 30 |
| Cincinnati Open Data | C | B | 29 |
| El Paso County CO Health Dept | B | A | 29 |
| Houston Health Dept | A | B | 29 |
| Delaware Open Data | B | C | 28 |

## Summary and chip changes

Summary phrase changes cover 313,889 lines among the judged texts. The largest (regex phrase, Jev phrase, lines, one example):

| regex summary | Jev summary | lines | example |
|---|---|---|---|
| Dirty food contact surfaces | <truncated text> | 101,581 | NONFOOD-CONTACT SURFACES CLEAN AND IN GOOD REPAIR |
| Plumbing leaks | <truncated text> | 23,098 | PLUMBING: PLUMBING IN GOOD REPAIR, PROPER BACKFLOW DEVICES |
| Vent hood grease buildup | <truncated text> | 20,181 | ADEQUATE VENTILATION AND LIGHTING; DESIGNATED AREAS, USE |
| Food not kept cold enough | <truncated text> | 16,779 | PROPER HOT AND COLD HOLDING TEMPERATURES |
| Permit not posted | <truncated text> | 14,433 | SIGNS POSTED; LAST INSPECTION REPORT AVAILABLE |
| Chemicals unsafe/unlabeled | <truncated text> | 10,379 | TOXIC SUBSTANCES PROPERLY IDENTIFIED, STORED, USED |
| Floors/Walls dirty or damaged | <truncated text> | 9,440 | Item 8A: Food not protected during storage, preparation, display, transportation and service, from potential sources of contamination (e.g., |
| <truncated text> | Food not kept cold enough | 9,227 | Cold TCS food item held above 41 °F; smoked or processed fish held above 38 °F; intact raw eggs held above 45 °F; or reduced oxygen packaged |
| Improper hand washing | <truncated text> | 8,867 | Handwashing (as required, when required, proper glove use, no bare hand contact of ready to eat foods). Food handler health restrictions as  |
| Raw food touching ready-to-eat | <truncated text> | 8,225 | FOOD SEPARATED AND PROTECTED |
| Sewage backup/issue | <truncated text> | 8,009 | Anti-siphonage or back-flow prevention device not provided where required; equipment or floor not properly drained; sewage disposal system i |
| Dishwasher not sanitizing | <truncated text> | 7,639 | WAREWASHING FACILITIES: ADEQUATE, MAINTAINED, PROPERLY USE, TEST STRIPS AVAILABLE |
| Dirty food contact surfaces | Dirty equipment surfaces | 5,946 | Item 11D: Non food contact surfaces of equipment not clean |
| <truncated text> | Floors/Walls dirty or damaged | 5,543 | Item 15A: Floors, walls, ceilings, not smooth, properly constructed, in disrepair, dirty surfaces |
| <truncated text> | Food not kept hot enough | 5,276 | Hot TCS food item not held at or above 140 °F. |
| Worker eating/drinking near food | <truncated text> | 4,851 | 3-501.17 Ready-To-Eat Time/Temperature Control for Safety Food, Date Marking |
| Dirty food contact surfaces | Dishwasher not sanitizing | 4,652 | Dishwashing and ware washing: Cleaning and sanitizing of tableware, including dishes, utensils, and equipment deficient. |
| No pest control records | <truncated text> | 3,945 | Effective pest control measures. Animals restricted as required. |
| <truncated text> | Rodent activity detected | 3,857 | Evidence of rodents or insects observed |
| Food spoiled/unsafe | <truncated text> | 3,762 | Food wholesome; not spoiled, contaminated, or adulterated. |
| Hair restraints missing | <truncated text> | 2,786 | PERSONAL CLEANLINESS AND HAIR RESTRAINTS |
| Improper hand washing | Hand sink blocked/dirty | 2,656 | Item 12E: Handwashing facilities inaccessible, improperly located, dirty, in disrepair, improper fixtures, soap, and single service towels o |
| Vent hood grease buildup | Lights missing shields | 2,375 | Item 15B: Lighting and ventilation inadequate, fixtures not shielded, dirty ventilation hoods, ductwork, filters, exhaust fans |
| <truncated text> | Hand sink blocked/dirty | 1,926 | (A) A HANDWASHING SINK shall be maintained so that it is accessible at all times for EMPLOYEE use and used for no other purpose. (Pf) (B) An |
| Hand sink blocked/dirty | <truncated text> | 1,855 | Wash hands sign not posted near or above hand washing sink. |

Chip changes (lines, with examples):

| chip | change | lines | examples |
|---|---|---|---|
| hygiene | removed | 114,251 | Non-food contact surface or equipment made of unacceptable material, not kept clean, or no / ADEQUATE HANDWASHING FACILITIES SUPPLIED & ACCESSIBLE |
| equipment | added | 82,674 | Physical facility in sound condition and maintained. / WIPING CLOTHS: PROPERLY USED AND STORED |
| docs | removed | 67,313 | ADEQUATE VENTILATION AND LIGHTING; DESIGNATED AREAS, USE / Signs and certifications as required. |
| equipment | removed | 58,107 | Non-food contact surface or equipment made of unacceptable material, not kept clean, or no / Food, supplies, or equipment not protected from potential source of contamination during s |
| pests | removed | 42,405 | PREMISES; PERSONAL/CLEANING ITEMS; VERMIN-PROOFING / Establishment is not free of harborage or conditions conducive to rodents, insects or othe |
| temp | removed | 27,159 | # 21A. HOT WATER AVAILABLE / HOT AND COLD WATER AVAILABLE |
| docs | added | 13,076 | # 01B. FOOD SAFETY CERTIFICATION / FOOD SAFETY CERTIFICATION |
| hygiene | added | 4,943 | Personal cleanliness maintained. Personal effects properly stored. / 2.4(C)(18) - I. Employee Health - Written Procedures for Cleaning Up Vomiting and Diarrhea |
| temp | added | 4,461 | TIME AS A PUBLIC HEALTH CONTROL; PROCEDURES & RECORDS / Item 11A: Manual facilities inadequate, technique incorrect; mechanical facilities not ope |
| pests | added | 105 | Clean all physical facilities (floors, walls, ceilings). Keep them clean. Observed mouse d / Clean all physical facilities (floors, walls, ceilings). Keep them clean. Dead cockroaches |

Wording proposals, not shipped: "Roaches found" instead of "Live roaches found" (today the phrase is used for dead roaches too, and Jev is told so); "Rodents or droppings found" instead of "Rodent activity detected"; a new phrase "Bare-hand contact with food" (there is none today, so those lines fall to the truncated text); "Pests found" for lines like "Evidence of rodents or insects observed" (2,174 lines) that cannot say which pest. None of these would change a grade.

## Hand-check: 100 random disagreements per question

I read 100 random disagreements per question and judged which side is right ("jev", "regex", "both" when both are defensible, "neither"). The full rows with the reason for each verdict are in `data/jev_shadow/handcheck_<question>.csv` and the appendix below.

| question | checked | Jev right | regex right | both defensible | neither | Jev share of decided |
|---|---|---|---|---|---|---|
| category | 100 | 87 | 2 | 6 | 5 | 97.8% |
| hazard | 100 | 88 | 6 | 1 | 5 | 93.6% |
| summary | 100 | 93 | 3 | 0 | 4 | 96.9% |

### Confidence floor sweep (hand-checked disagreements at or above each floor; coverage = share of judged texts at or above it)

`category`

| floor | checked above | Jev right | regex right | both | neither | Jev precision | coverage |
|---|---|---|---|---|---|---|---|
| 0.0 | 100 | 87 | 2 | 6 | 5 | 97.8% | 100.0% |
| 0.3 | 99 | 87 | 2 | 6 | 4 | 97.8% | 99.0% |
| 0.4 | 97 | 87 | 2 | 5 | 3 | 97.8% | 97.0% |
| 0.5 | 89 | 84 | 1 | 1 | 3 | 98.8% | 92.8% |
| 0.6 | 84 | 81 | 1 | 1 | 1 | 98.8% | 87.7% |
| 0.7 | 79 | 77 | 0 | 1 | 1 | 100.0% | 82.7% |
| 0.8 | 72 | 70 | 0 | 1 | 1 | 100.0% | 76.6% |
| 0.9 | 63 | 63 | 0 | 0 | 0 | 100.0% | 67.4% |

`hazard`

| floor | checked above | Jev right | regex right | both | neither | Jev precision | coverage |
|---|---|---|---|---|---|---|---|
| 0.0 | 100 | 88 | 6 | 1 | 5 | 93.6% | 100.0% |
| 0.3 | 100 | 88 | 6 | 1 | 5 | 93.6% | 100.0% |
| 0.4 | 100 | 88 | 6 | 1 | 5 | 93.6% | 99.9% |
| 0.5 | 93 | 85 | 3 | 1 | 4 | 96.6% | 95.0% |
| 0.6 | 92 | 84 | 3 | 1 | 4 | 96.6% | 88.4% |
| 0.7 | 90 | 83 | 2 | 1 | 4 | 97.6% | 80.2% |
| 0.8 | 83 | 78 | 1 | 1 | 3 | 98.7% | 67.8% |
| 0.9 | 76 | 72 | 1 | 1 | 2 | 98.6% | 43.8% |

`summary`

| floor | checked above | Jev right | regex right | both | neither | Jev precision | coverage |
|---|---|---|---|---|---|---|---|
| 0.0 | 100 | 93 | 3 | 0 | 4 | 96.9% | 100.0% |
| 0.3 | 100 | 93 | 3 | 0 | 4 | 96.9% | 100.0% |
| 0.4 | 100 | 93 | 3 | 0 | 4 | 96.9% | 99.5% |
| 0.5 | 96 | 91 | 2 | 0 | 3 | 97.8% | 97.0% |
| 0.6 | 88 | 84 | 1 | 0 | 3 | 98.8% | 91.7% |
| 0.7 | 79 | 76 | 1 | 0 | 2 | 98.7% | 85.5% |
| 0.8 | 69 | 67 | 1 | 0 | 1 | 98.5% | 78.0% |
| 0.9 | 61 | 60 | 0 | 0 | 1 | 100.0% | 68.9% |

### Recommended floors

0.6 for all three questions, now the default in `CONFIDENCE_FLOORS`. Below 0.6 the hand-check still finds the regex right on a few lines (hazard at 0.46 to 0.48, category at 0.42, summary at 0.40 to 0.52) and the option-order test flipped 20 to 60% of top answers; at or above 0.6 the order flips fall to 3 to 4%, Jev is right on 97 to 99% of checked disagreements, and 88% (category), 88% (hazard) and 92% (summary) of judged texts still clear the bar. The hazard question could go to 0.7 (80% coverage) if you want the F decision stricter still; the two confident hazard errors left in the sample (rule text read as flies at 1.00, a lubricant can read as vermin at 0.76) are not fixed by any floor, only by wording or by the policy on unspecified pests.

## Cost

| measure | value |
|---|---|
| input tokens per line, all in | 3,858 |
| price | $0.042 per million input tokens; output is free |
| cost per line | $0.000162 |
| all qs3 API spend (shadow + re-verification + pilot) | $5.81 for 35,823 texts (138,214,825 input tokens, 5,739 requests) |
| of which batched shadow | 3,345 tokens per text; single-line verification adds 6,669 tokens for the ~7% of texts it touches |
| full backfill of every distinct stored text | $40.58 (250,447 texts) |
| weekly refresh | $0.64 (3,970 new distinct texts a week, mean of the last 12 full weeks in the seeds) |
| latency | mean 0.53 s per request, max 1.52 s; 35,823 texts judged in 525 s with 3 requests in flight |

The per-line cost is almost entirely the three questions' criteria, which repeat for every line in a request (about 10 KB of JSON per line). Batching saves requests and latency but not tokens. One lever if cost ever matters: ask hazard for every line and category/summary only for lines the regex is unsure about; at current prices there is no reason to.

Note on cache keys: 45,587 stored lines (4.8%) sit at the 400-character D1 store cap, while the fetchers classify up to 500 characters. The cache is keyed on the exact slice the pipeline classifies, so those texts will be asked once more in phase 2 with their full slice. Everything else is cached now.

## What still needs your decision

1. **Pests reported without a kind.** `pests_unspecified` does not force an F in this PR. Flipping it adds about 2,396 F's (NYS "Item 14A: Insects, rodents present" alone is about 1,500 restaurants) and keeps today's F on Montgomery County's "Evidence of rodents or insects observed" (about 800 restaurants) and San Francisco's vermin boilerplate (about 1,400). The table above has both outcomes. One line in `F_FORCING_HAZARDS`.

2. **Flies and gnats.** Still no F. Making them force an F adds about 186 F's, mostly Richardson, Fort Worth, Dallas and the Portland-metro counties.

3. **Bare pest code titles whose citation implies a finding.** Houston (167 lines), Delaware (154) and Detroit (40) store the code title "Removing Dead or Trapped Birds, Insects, Rodents and other Pests" with no observation. Today it forces an F because it contains "Dead" and "Rodents"; Jev reads a title. That is the reading you asked for (a title is not an observation), and it costs those sources their text-forced F's (Houston 108, Delaware 29, Detroit 10). If you would rather keep them, the clean route is the same policy switch as item 1 with those titles described as pests_unspecified.

4. **Grey water.** Jev now calls a leaking sink drain, a dripping pipe or an overflowing grey-water tank `conditions_only` (no F) and keeps `sewage_observed` for sewer, floor-drain and toilet backups and sewage on the floor. The hand-check has two borderline cases (a waste-water tank overflowing onto the floor; waste water pooling at an outdoor drain pipe). Say if you want "wastewater on the floor" to force an F.

5. **Wake County tiers.** The fetcher sets the tier from the points deducted; the text carries the FDA designation and they disagree on 46% of Wake lines. Display only (Wake's score is official). The reader keeps the fetcher's choice; one line to prefer the marker.

6. **Phase 2 wiring.** `JEV_MODE` into the fetchers and `refresh-data.yml` (key from secrets, `--jev-deadline-minutes` inside the job timeout, coverage in the run summary, the cache committed with data.js, `JEV_DECIDE` per question). Recommended first step: `JEV_DECIDE=hazard,summary` in decide mode and category in shadow for one more week, because category moves points on the computed-score sources (NYC, SF, Houston, Cincinnati, Delaware, the demerit MHD jurisdictions) and that is the change with the most grade movement that is not an F.

7. **Phase 3 regrade.** Weekly runs only rewrite that week's restaurants, so most of the 8,220 grade changes above wait for the full regrade from stored text (dry run with this transition table first, then scoped D1 updates inside the 100KB and 40-row/80KB caps).

8. **Summary wording.** "Roaches found" for "Live roaches found" (used for dead roaches today and in the question), "Rodents or droppings found" for "Rodent activity detected", and new phrases "Pests found" (unspecified pests, about 5,700 lines) and "Bare-hand contact with food" (no phrase today). No grade impact; a question-set bump when you decide.

## Appendix: hand-check rows

`who` is my verdict; confidence is Jev's. Full text in the CSVs.

### category

| # | lines | regex | Jev | conf | who | why | text |
|---|---|---|---|---|---|---|---|
| 0 | 1 | food_handling | equipment_utensils | 0.99 | jev | utensil drying is equipment handling | 44: 4-901.11 Observed utensils drying in paper towels. |
| 1 | 1 | cross_contamination | personal_hygiene | 0.35 | both | personal items on food racks: food_handling fits best; Jev 0.35 below floor | Store clothing and other personal items in lockers or other suitable places. Observed employee clothing and personal items stored on food storage and equipment storage racks. Store these items separat |
| 2 | 1 | unclassified | general_cleanliness | 0.46 | jev | mop storage; below floor | Mops not properly placed in a position to properly air dry.(CORRECT VIOLATION WITHIN 14 CALENDAR DAYS) |
| 3 | 1 | unclassified | general_cleanliness | 0.66 | jev | substance on crates is cleanliness | 49: 4-601.11(C) OBSERVED SUBSTANCE ON CRATES IN RIC |
| 4 | 1 | equipment_utensils | general_cleanliness | 0.46 | both | decorative materials not cleanable: equipment or facility | Attachments. Decorative materials in other than consumer areas attached to walls and ceilings not easily cleanable / not maintained in good repair / not kept clean. |
| 5 | 1 | equipment_utensils | maintenance | 1.0 | jev | coving replacement is maintenance | 6.4(A) - Repairing The physical facilities were not maintained in good repair. Floor-wall coving needs replaced in some areas around the dish machine room. Observed missing coving on left side. |
| 6 | 1 | unclassified | equipment_utensils | 0.95 | jev | film on ice bin is a food-contact surface | 47: 4-101.19 SHALL REMOVE FILM ON ICE BIN |
| 7 | 1 | unclassified | food_handling | 0.78 | jev | food in grocery bag is food handling | 15: Observed cut onions stored in plastic grocery bag in walk-in cooler. Corrective Actions: Ensure to store food products in food grade materials, such as zip lock bags. |
| 8 | 1 | food_handling | general_cleanliness | 1.0 | jev | exteriors and interiors needing cleaning | NON-FOOD/FOOD CONTACT SURFACES CLEAN: CLEAN THE EXTERIOR OF ALL FOOD STORAGE CONTAINERS SITTING ON THE WIRE STORAGE SHELVES, INTERIOR OF THE PREP COOLERS, THE TOP OF THE STOVE AND WIRE STORAGE SHELVES |
| 9 | 1 | unclassified | temperature_control | 1.0 | jev | 48F is temperature | 22: 3-501.16(A)(2) Observed chopped vegetables on the prep line at 48 F. |
| 10 | 1 | cross_contamination | facility_design | 0.58 | jev | hand sink misuse is facility (Pf), not cross-contamination (P) | 5.1(O)(2) - III. Preventing Contamination by Hands - Using a handwashing sink - Operation and Maintenance Handwashing facility is being used for purposes other than handwashing. Discontinue storing ut |
| 11 | 1 | pest_control | maintenance | 0.42 | regex | door propped open is a pest-entry condition | 32: 06265 The exit door was propped open with a rat trap being used as a doorstop. WAC: Provide, install, repair or restore solid, self-closing, tight-fitting doors which must be solid and tight-fitti |
| 12 | 1 | unclassified | cross_contamination | 0.9 | jev | unlabeled chemical bucket | 28: 7-101.11 Observed spray bucket without name of chemical |
| 13 | 1 | facility_design | equipment_utensils | 0.76 | jev | glasses exposed to splash is equipment storage | 44: 4-904.11 Glasses near the wait station hand sink were stored under the paper towel dispenser where they were exposed to splash. |
| 14 | 1 | unclassified | personal_hygiene | 1.0 | jev | not washing hands is hygiene | 8: 2-301.12(A) Wash hands for minimum 20 seconds. Observed employee not wash hands for 20 seconds |
| 15 | 3 | facility_design | general_cleanliness | 1.0 | jev | walls and floors need cleaning | Physical facilities installed; maintained & clean 6-501.114; Core; Heavy cleaning is needed on the walls and floors throughout facility. The walls and floors in the dish area has a lot of black residu |
| 16 | 1 | cross_contamination | equipment_utensils | 0.57 | neither | scoop left in food is food_handling by the criteria; Jev chose equipment at 0.57, regex cross_contamination | In-use utensils: properly stored 3-304.12 In-Use Utensils; Between-Use Storage (C); Scoop with no handle was being stored inside a container of pasta. If utensils are stored inside of food ingredients |
| 17 | 1 | temperature_control | general_cleanliness | 0.67 | jev | food debris on equipment is cleanliness; regex tripped on temperature words | NON-FOOD/FOOD CONTACT SURFACES CLEAN: 4-602.13 OBSERVED ALL HEAVY COOKING EQUIPMENT (FRYERS, GRILL), ROLLING CARTS, PREP TABLES, STORAGE BINS, WIRED RACKS, ALL COOLING UNITS IN FRONT PREP WITH ACUMMUL |
| 18 | 1 | temperature_control | water_sewage | 1.0 | jev | sewage odor is water_sewage | 41: 114171, 114190, 114192, 114193, 114193.1, 114199, 114201, 114269 Observed strong sewage odor in the back of the facility by the water heater. Per operator, management is aware of the issue and has |
| 19 | 2 | personal_hygiene | facility_design | 0.97 | jev | item placed above hand sink is facility | 11: §9-46(e)(1)(B) OBSERVED POTATO CUTTER PLACED ABOVE THE LARGER HAND SINK. Corrective Actions: INSTRUCTED THE MANAGER TO MOVE THE POTATO CUTTER AWAY FROM THE HAND SINK WITHIN 24 HOURS AND EMAIL A PI |
| 20 | 1 | food_source | cross_contamination | 1.0 | jev | unlabeled bleach bottle | Toxic substances properly identified stored & used 7-102.11; Priority Foundation; A spray bottle of bleach solution was observed without a label. Label working containers of toxic materials such as cl |
| 21 | 1 | food_handling | general_cleanliness | 0.97 | jev | non-food contact surfaces clean | Non-food contact surfaces clean 4-601.11(C ) Keep non food contact surfaces of equipment clean to avoid build up of dust; food residue or debris.4-602.13 Non food contact surfaces of equipment shall b |
| 22 | 1 | food_handling | maintenance | 0.99 | jev | wall covering not cleanable is maintenance | Physical facilities installed; maintained & clean 6-201.16 Wall and Ceiling Coverings and Coatings (C). (A) Wall and ceiling covering materials shall be attached so that they are EASILY CLEANABLE. - O |
| 23 | 1 | unclassified | food_source | 1.0 | jev | unlicensed sources is food_source | 11: 3-201.11(A) 3-201.11(A) Compliance with Food Law, Lawful Sources (P). Food shall be obtained from sources that comply with law. Tamales, Cookies and beef jerky are purchased from sources that do n |
| 24 | 1 | temperature_control | facility_design | 0.84 | jev | HVAC vent cover is facility | 43: 1. Heating, ventilating, and air conditioning systems (HVAC) not properly designed, installed, and/or maintained. OBSERVED HVAC VENT COVER NOT PROPERLY INSTALLED/SLIGHTLY HANGING FROM CEILING. |
| 25 | 1 | unclassified | equipment_utensils | 0.24 | neither | stuck paper towel dispenser is facility_design; Jev 0.24, regex unclassified | 10: The paper towel dispenser at the coffee station had paper towels stuck in the dispenser. Towels adjusted to properly dispense. |
| 26 | 1 | unclassified | facility_design | 0.66 | jev | paper towel machine not working | 47: 4-501.11(A) PAPER TOWEL MACHINE NOT WORKING |
| 27 | 1 | temperature_control | equipment_utensils | 1.0 | jev | no tool to verify dish temperature | Have an item to verify the surface temperature of dishes reaches 160 degrees F inside mechanical hot water dishwashers. There was no independent tool on site to measure the hot water sanitization temp |
| 28 | 5 | temperature_control | food_handling | 1.0 | jev | date marking is food_handling | 3-501.17 VIOLATION OF SECTION 3-501.17 *PRIORITY FOUNDATION* Ready-to-eat food is not properly date-marked, specifically: ***Braised artichokes were dated 6/13. PIC stated they were frozen, and thawed |
| 29 | 1 | unclassified | facility_design | 0.92 | jev | soap dispenser | 10: 6-301.11 HAND SOAP DISPENSER IS NOT PROPERLY WORKING; CANNOT DISPENSE HAND SOAP |
| 30 | 1 | equipment_utensils | general_cleanliness | 1.0 | jev | residue along floors | Physical facilities installed; maintained & clean 6-501.12; Core; Residue accumulation along floors under and around equipment and shelving. PHYSICAL FACILITIES shall be cleaned as often as necessary  |
| 31 | 1 | unclassified | facility_design | 1.0 | jev | light intensity | Facilities-lighting intensity prep and safety areas The light intensity was <50 foot candles in food prep or other area where employee safety is a factor. replace burnt out light bulbs above cooking a |
| 32 | 1 | unclassified | food_handling | 0.74 | jev | employee food above public food | 15: Observed employee food and beverage items stored above food offered to the public. Corrective Actions: Ensure to store all employee items below any food offered to the public. |
| 33 | 1 | unclassified | food_handling | 0.91 | jev | consumer advisory | 25: Observed the menu was missing disclosures for some foods offered undercooked, such as salmon and steak. Provide a disclosure statement that identifies the animal foods that are to be served raw or |
| 34 | 1 | unclassified | personal_hygiene | 0.97 | jev | no hand washing; hygiene | 8: 2-301.12(A) MO WATER NO WASH HANDS |
| 35 | 1 | food_handling | employee_training | 1.0 | jev | no certified manager | Certified Food Protection Manager 2-102.12 (A); Core; No CFPM on site during inspection. The person in charge shall be a certified food protection manager (CFPM) who has shown proficiency of required  |
| 36 | 1 | equipment_utensils | maintenance | 1.0 | jev | damaged ceiling tiles | 6.4(A) - Repairing The physical facilities were not maintained in good repair. Dammaged celing tiles. Replace all Dammaged celing tiles. |
| 37 | 1 | water_sewage | facility_design | 0.61 | regex | hot water not working is water_sewage | 51: 5-205.15 OBSERVED HOT WATER NOZZLE NOT WORKING AT 3-COMP SINK |
| 38 | 1 | personal_hygiene | water_sewage | 1.0 | jev | drainpipe leaking under hand sink is plumbing | PLUMBING INSTALLED; PROPER BACKFLOW DEVICES: OBSERVED DRAINPIPE LEAKING UNDER HAND WASHING SINK IN PREP AREA. INSTRUCTED TO REPAIR LEAKING DRAINPIPE AND MAINTAIN. |
| 39 | 1 | temperature_control | maintenance | 0.48 | jev | laminate shelving; below floor | Non-food contact surfaces of equipment must be made of materials which are corrosion-resistant, non-absorbent, and smooth. Remove equipment with non-food contact surfaces made of other materials. The  |
| 40 | 2 | food_handling | equipment_utensils | 1.0 | jev | residue on nozzle is a food-contact surface | 10: 1. Food contact surface is not clean to sight or touch, and was not cleaned before use or storage. OBSERVED ACCUMULATION OF DRINK RESIDUE ON NOZZLE OF MILK SHAKE MACHINE. |
| 41 | 1 | facility_design | unclassified | 1.0 | jev | inspection report posting is unclassified | 2: §9-68(c)(1)(K) Observed facility has not posted either their most recent inspection report or a notice indicating that the most recent inspection report is availble upon request. Corrective Actions |
| 42 | 1 | facility_design | general_cleanliness | 0.75 | jev | floor drains not clean | 55: Observed floor drains, at front service area and underneath three compartment sink in the back room area, not kept thoroughly cleaned. Corrective Actions: Must regularly and thoroughly clean floor |
| 43 | 1 | unclassified | food_handling | 0.84 | jev | no date on noodles | 23: 3-501.17(A) NO DATE ON NOODLES |
| 44 | 1 | cross_contamination | food_handling | 0.87 | both | allergen consumer warning: labeling or allergen control | 3.5(D)(1) - other forms of information Consumer warnings were not provided. -Consumer warnings for ice creams containing allergen products like nuts are not provided. |
| 45 | 1 | temperature_control | equipment_utensils | 1.0 | jev | ice machine surfaces | 39: 04. Surfaces of utensils and equipment contacting non-TCS foods are not properly cleaned. OBSERVED BLACK ORGANIC MATTER ON ICE MACHINE SURFACES. |
| 46 | 1 | food_handling | equipment_utensils | 0.87 | jev | in-use utensils and knives | 39: 01. In-use utensils used for food preparation or dispensing are not properly stored between uses. OBSERVED A SCOOP USED TO SCOOP MULTIPLE FOOD PRODUCTS ON THE LINE OBSERVED KNIVES STORED ON RUSTY  |
| 47 | 1 | food_handling | equipment_utensils | 1.0 | jev | wet stacking | Utensils; equipment & linens: properly stored; dried & handled 4-903.11(A); (B) and (D); Core; Metal containers on the rack near the dish machine were stacked together while still wet. A few plastic c |
| 48 | 1 | unclassified | facility_design | 1.0 | jev | blocked hand sink | 11: §9-46(e)(1)(A) OBSERVED BAR HANDSINK BLOCKED BY BUCKET STORED IN BASIN AT THE BAR. Corrective Actions: SHALL REMOVE THE BUCKET FROM THE HANDSINK BASIN. SHALL ENSURE THAT HANDSINK IS ACCESSIBLE AT  |
| 49 | 2 | unclassified | pest_control | 0.96 | jev | prohibiting animals; closest category | 6-501.115 Prohibiting Animals |
| 50 | 1 | unclassified | facility_design | 0.71 | jev | towel on hand sink | 31: 2. Improper usage of hand-washing sink.OBSERVED TOWEL STORED ON HANDSINK |
| 51 | 1 | unclassified | food_source | 0.89 | jev | dented cans | 13: 3-101.11 Observed multiple dented cans stored on shelves |
| 52 | 1 | unclassified | food_handling | 0.3 | neither | boxes in prep sink; Jev 0.30, regex unclassified | Discontinue the use of a food preparation sink for anything other than food preparation. OBSERVED MULTIPLE BOXES STORED INSIDE AND AROUND THE FOOD PREPARATION SINK. DISCONTINUE PRACTICE. THE FOOD PREP |
| 53 | 1 | unclassified | temperature_control | 1.0 | jev | cold food above 41 | 22: 3-501.16(A)(2) cold food not reach 41 degrees f or below |
| 54 | 1 | food_source | cross_contamination | 1.0 | jev | unlabeled degreaser | 7.0(B) - X. Chemical - Working Containers - Common Name Working containers of poisonous/toxic materials are not labeled. Unlabeled degreaser on the floor by rack. Working containers of poisonous/toxic |
| 55 | 1 | equipment_utensils | facility_design | 0.89 | jev | items in hand sink basin | 10: A lid, rags, and food debris were observed in the basin of the hand sink in the back of the facility. Corrective action: The person in charge was advised to remove the items above and to sanitize  |
| 56 | 1 | unclassified | employee_training | 1.0 | jev | food handler certificates | 3: §9-6(b)(4) COPIES OF FOOD HANLDER CERTIFICATES ARE NOT AVAILABLE FOR REVIEW Corrective Actions: NEED TO PRINT COPIES OF FOOD HANLDER CERTIFICATES FOR ALL FOOD WORKERS AND KEEP THEM AVAILABLE FOR RE |
| 57 | 1 | unclassified | facility_design | 0.73 | jev | hand sink access obstructed | 31: 2. Improper usage of hand-washing sink. OBSERVED OBSTRUCTED ICEMACHINE ON THE ACCESS TO HANDSINK. |
| 58 | 1 | temperature_control | facility_design | 0.88 | jev | hand sink flow and hot water; regex tripped on temperature word | Handwashing sinks supplied & accessible 5-202.12; Core; The handsink in the back had a very poor flow and only had cold water.- Provide at least 100F water at handsinks.- Please repair sink to proper  |
| 59 | 1 | unclassified | general_cleanliness | 0.96 | jev | dusty fans | 39: 3-305.11(A) Observed dusty fans on the prep line. Observed dusty fans in the RIC. |
| 60 | 1 | food_handling | equipment_utensils | 1.0 | jev | soiled lids stored as clean | Food-contact surfaces: cleaned & sanitized 4-601.11 (A); Priority Foundation; Noted a few soiled serving container lids were stored as being clean on the rack above the 3 compartment sink. EQUIPMENT F |
| 61 | 1 | unclassified | cross_contamination | 1.0 | jev | raw bacon above RTE | 15: 3-302.11(A)(1)(a) Observed raw bacon stored above RTE strawberry sauce in the WIC. |
| 62 | 1 | temperature_control | food_handling | 0.99 | jev | date marking | 3.4(G)(1) - VI. Time/Temperature Controlled Safety Food - Ready-to-eat, TCS - date marking Refrigerated, ready-to-eat, TCS food that is prepared in the food facility and held refrigerated for more tha |
| 63 | 1 | equipment_utensils | food_handling | 0.42 | both | single-use items on the floor: storage or equipment | SINGLE-USE/SINGLE-SERVICE ARTICLES: PROPERLY STORED & USED: OBSERVED A FEW ITEMS STORED ON FLOOR UNDERNEATH 3-COMPARTMENT SINK. MUST ELEVATE ITEMS AND MAINTAIN AT ALL TIMES. |
| 64 | 1 | unclassified | temperature_control | 1.0 | jev | cooler cannot hold 41 | 33: Observed salad cooler was unable to keep foods at 41°F or below. Corrective Actions: Ensure salad cooler is capable of keeping foods at 41°F or below. |
| 65 | 1 | food_handling | equipment_utensils | 1.0 | jev | wet stacking | Utensils; equipment & linens: properly stored; dried & handled 4-903.11 (A); (B); and (D) Equipment; Utensils; Linens and Single-Service and Single-Use Articles Storing (C) Metal containers in the cle |
| 66 | 1 | food_source | equipment_utensils | 1.0 | jev | ice dispenser soiled | Food-contact surfaces: cleaned & sanitized 4-602.11 Equipment food-contact surfaces and utensils - Frequency The ice dispenser of the beverage machine is soiled with a brown organic build up. (4) In E |
| 67 | 1 | unclassified | equipment_utensils | 1.0 | jev | worn spatula | 47: 4-502.11(A) OBSERVED HEAVILY WORN SPATULA. SHALL REPLACE |
| 68 | 3 | temperature_control | general_cleanliness | 0.99 | jev | equipment needs detailed cleaning | Non-food contact surfaces clean 4-602.13; Core; Multiple pieces of equipment are in need of detailed cleaning: hinges of hot hold cabinets; sides of makeline equipment; wheels/castors of equipment on  |
| 69 | 1 | unclassified | equipment_utensils | 0.57 | neither | mop drying is facility or cleanliness; Jev equipment 0.57 | 55: 6-501.16 Must air dry mop |
| 70 | 1 | unclassified | temperature_control | 1.0 | jev | chicken at 109F | 21: 3-501.16(A)(1) CHICKEN 109F |
| 71 | 1 | personal_hygiene | facility_design | 0.95 | jev | hand sink lacked hot water | 10: The handwashing sink next to the stovetop lacked hot water. Corrective action: Person in charge submitted a maintenance request for the hot water to maintain a minimum of 85 degrees F. |
| 72 | 1 | cross_contamination | food_handling | 0.42 | jev | utensils stored in service ice; below floor | 43: In the bar building located out the back of the restaurant stirring, mashing and squeezing utensils for drinks were observed being stored directly in the service ice. Discontinue storing in use ut |
| 73 | 1 | cross_contamination | facility_design | 0.91 | jev | no paper towels | 6.2(C) - III. Preventing Contamination by Hands - Facilities Handwashing lavatory had no, unapproved, or inoperable hand drying provision. Observed missing paper towels from hand washing sink. Must ha |
| 74 | 1 | food_handling | maintenance | 1.0 | jev | cracked light shield | Keep the physical facilities in good repair. OBSERVED A BADLY CRACKED CEILING LIGHT SHIELD COVER ABOVE THE FOOD PREP AREA. REPLACE WITH A NEW LIGHT SHIELD COVER. |
| 75 | 1 | unclassified | temperature_control | 1.0 | jev | missing thermometer | 36: 4-204.112(B) Observed missing thermometer in multiple coolers. |
| 76 | 1 | unclassified | cross_contamination | 0.61 | jev | chemical spray bottles | 18: chemical spray bottles |
| 77 | 1 | general_cleanliness | maintenance | 0.92 | jev | rust on racks | 47: 4-202.16 Storage racks inside walk in have rust build up |
| 78 | 1 | unclassified | cross_contamination | 0.52 | jev | eye drops on prep table | 28: 7-208.11 OBSERVED EYE DROPS ON PREP TABLE |
| 79 | 1 | food_handling | general_cleanliness | 0.86 | neither | non-cleanable shelving material is equipment or facility; Jev general_cleanliness 0.86 | Non-food contact surfaces of equipment must be made of materials which are corrosion-resistant, non-absorbent, and smooth. Remove equipment with non-food contact surfaces made of other materials. Obse |
| 80 | 1 | unclassified | equipment_utensils | 1.0 | jev | test strips | 48: 4-302.14 PROVIDE TEST STRIPS |
| 81 | 1 | unclassified | food_handling | 1.0 | jev | food on the ground | 39: 3-305.11(A) Observed food stored on ground. All food items shall be stored at least 6 in above ground. |
| 82 | 1 | unclassified | cross_contamination | 0.94 | jev | chemicals as door stopper | 28: Observed chemicals being used as door stopper |
| 83 | 1 | temperature_control | food_handling | 0.98 | jev | date marking | 23: 3-501.17 3-501.17 Ready-To-Eat Time/Temperature Control for Safety Food, Date Marking (Pf). Refrigerated, Ready-To-Eat, Time/Temperature Control for Safety Food prepared and held for more than 24  |
| 84 | 1 | unclassified | cross_contamination | 0.45 | both | chemical bottles on hand sinks: chemicals or hand sink misuse; below floor | 31: 2. Improper usage of hand-washing sink. OBSERVED CHEMICAL SPRAY BOTTLES STORED ON HANDSINKS |
| 85 | 1 | unclassified | temperature_control | 1.0 | jev | foods above 41 | 22: 3-501.16(A)(2) OBSERVED FOODS IN REACH IN COOLER ABOVE 41 DEGREES. SHALL KEEP LIDS CLOSED DURING SERVICE TO KEEP FOODS AT APPROPRIATE TEMPURATURE |
| 86 | 16 | cross_contamination | food_handling | 0.54 | jev | single-use articles storage | 45: 4-903.11(A),(C) Single-use articles are not stored in a way that prevents contamination, and at least 6 inches above the floor. |
| 87 | 1 | temperature_control | equipment_utensils | 1.0 | jev | dish machine thermometer | Warewashing facilities: installed; maintained & used; test strips . 4-302.13; Priority Foundation; No thermometer is provided for measuring the temperature of the hot water sanitizing dish machine. In |
| 88 | 1 | food_source | cross_contamination | 0.99 | jev | unlabeled chemical | 28: Observed chemical stored in a spray bottle with no label. Corrective Actions: Ensure all chemicals stored in secondary containers are labeled. |
| 89 | 1 | cross_contamination | food_handling | 0.75 | jev | cooked beef uncovered | 9: 1. Food is not protected from cross-contamination, or other contamination, during storage, preparation, holding, display or is in contact with unclean surfaces. OBSERVED COOKED BEEF IN THE METAL PA |
| 90 | 1 | cross_contamination | food_handling | 0.99 | jev | food not covered | 37: 2. Food is not protected from contamination. OBSERVED MULTIPLE CONTAINERS OF FOOD PRODUCTS (SHRIMP, CHICKEN, BEEF) NOT COVERED IN THE PREP COOLER. |
| 91 | 2 | food_source | cross_contamination | 1.0 | jev | unlabeled spray bottle | 18: 1. Toxic substances (chemicals/materials/first aid kit/employee medication) located in the food establishment are not labeled properly in original or working containers.OBSERVED SPRAY BOTTLE UNLAB |
| 92 | 1 | unclassified | cross_contamination | 0.91 | jev | household Raid stored | 28: 7-202.11 Observed household chemicals of raid stored on premises |
| 93 | 1 | personal_hygiene | facility_design | 1.0 | jev | restroom hand sink blocked | 10: Observed the handsink in the restroom was blocked by various items at time of inspection. Handwashing sinks must be kept accessible for use at all times. Do not place or store items in, on, or in  |
| 94 | 1 | food_handling | general_cleanliness | 1.0 | jev | waste receptacle and floor soiled | Nonfood Contact Surfaces (C): Interior of the waste receptacle and the floor with soils- Clean properly |
| 95 | 2 | food_handling | general_cleanliness | 0.94 | jev | grease under the fryer | 4.5(A)(2)(3) - Equipment, food-contact surfaces, nonfood-contact surfaces, and utensils Non-food contact surfaces are dirty. Grease accumulation was observed on the sides and underneath the fryer. The |
| 96 | 1 | unclassified | temperature_control | 1.0 | jev | shrimp at 47 | 22: 3-501.16(A)(2) OBSERVED SHRIMP TEMPURA AT 47. SHALL BE 41 DEGREES OR BELOW |
| 97 | 1 | unclassified | food_handling | 0.87 | jev | consumer advisory | The establishment does not have any consumer advisory notices regarding the potential health risks associated with the consumption of raw or undercooked animal foods.(CORRECT VIOLATION WITHIN 5 CALEND |
| 98 | 1 | water_sewage | maintenance | 0.43 | both | grease receptor exterior: maintenance or plumbing; below floor | PLUMBING INSTALLED; PROPER BACKFLOW DEVICES: OBSERVED THE EXTERIOR SURFACES OF THE GREASE RECEPTOR IN NEED OF CLEANING/PAINTING. TOP OF RECEPTOR HAS RUST STAINS AND SIDE EXTERIOR SURFACES PEELING PAIN |
| 99 | 1 | unclassified | temperature_control | 0.96 | jev | thermometer | 36: 4-203.11 SHALL CHANGE FOOD THERMOMTER WITH SMALLER DIGITS |

### hazard

| # | lines | regex | Jev | conf | who | why | text |
|---|---|---|---|---|---|---|---|
| 0 | 1 | forces F | conditions_only | 0.88 | jev | roach spray is a pest-control product, not a roach | 18: 3. Toxic substances (chemicals/materials/first aid kits/employee medications) located in the food establishment are not approved for use in a food facility.OBSERVED HOUSEHOLD ROACH SPRAY IN TRASH  |
| 1 | 1 | forces F | conditions_only | 0.99 | jev | loose rodent bait is a condition | Contain rodent bait in a covered, tamper-resistant bait station. Observed loose rodent bait throughout kitchen and basement. Clean up/remove. Bait must be contained. |
| 2 | 1 | forces F | conditions_only | 0.48 | regex | cheese next to rodent contamination is rodent evidence; Jev 0.48 is below the floor so regex decides anyway | 7: 1. Food is not safe, in good condition, unadulterated, and honestly presented. OBSERVED CHESSE IN CLOSE PROXIMITY TO RODENT CONTAMINATION. |
| 3 | 1 | forces F | flies_or_gnats_observed | 0.99 | jev | small flying pests are flies | 6-501.111(C) VIOLATION OF SECTION 6-501.111(C) *PRIORITY FOUNDATION* The premises is not maintained free of insects, rodents, and other pests, specifically: OBSERVED A SMALL FLYING PEST IN THE MAP SIN |
| 4 | 1 | forces F | conditions_only | 1.0 | jev | door not rodent-proofed, no pest seen | INSECTS, RODENTS, & ANIMALS NOT PRESENT: FOUND REAR EXIT DOOR NOT COMPLETELY RODENT PROOFED AS REQUIRED; INSTRUCTED TO SEAL BOTTOM AREA OF DOOR LEADING TO DISPOSAL AREA AND REPLACE METAL THRESH HOLE. |
| 5 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | flies present, no F under policy | No Evidence of Insect contamination, rodent/other animals: Flies present |
| 6 | 1 | forces F | conditions_only | 0.46 | regex | evidence of rodent activity (droppings) is evidence; Jev 0.46 below floor | Cleaning Floors Dustless Methods (C): Clean the floors in the basement & old rest room to remove evidence of rodent activity(droppings) |
| 7 | 1 | no F | vermin_observed | 1.0 | jev | roach activity observed; regex misses the word order | 38: 6-501.111 Roach activity observed at dishwash area. |
| 8 | 2 | forces F | conditions_only | 0.99 | jev | rule text | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 9 | 1 | forces F | conditions_only | 0.95 | jev | slow drains, text says not sewage; regex trips on backflow+sewage | 51: 5-205.15 OBSERVED BACK ROOM HANDWASH SINK DRAIN HAS BACKFLOW (slow flow, not complete cloggage) OBSERVED 3 COMPARTMENT SINK + HANDWASH SINK HAS BACKFLOW (not sewage) |
| 10 | 1 | no F | vermin_observed | 1.0 | jev | mice dropping under the sink; regex misses | 6.4(K) - Controlling Pests The presence of insects, rodents, and other pests is not being adequately controlled or minimized. Mice dropping under the sink in the kitchen . Rid premises from rodent, re |
| 11 | 1 | forces F | conditions_only | 1.0 | jev | sticky trap | 28: Found one sticky mouse trap located on the rack in the dry storage area. Poisonous or toxic materials shall be stored so they cannot contaminate food, equipment, utensils, linens, or single-servic |
| 12 | 1 | forces F | conditions_only | 0.9 | jev | coving rule text | 6.1(C)(1) - Floor and wall junctures - coved, and enclosed or sealed In FSOs or RFEs in which cleaning methods other than water flushing are used for cleaning floors, the floor and wall junctures shal |
| 13 | 2 | forces F | flies_or_gnats_observed | 1.0 | jev | flying pests | 6-501.111(C) VIOLATION OF SECTION 6-501.111(C) *PRIORITY FOUNDATION* The premises is not maintained free of insects, rodents, and other pests, specifically: OBSERVED DURING SEMIANNUAL INSPECTION LARGE |
| 14 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | flies | 32: Inspector observed a high presence of flies in the back storage area (10+), ware washing area (3+), and under the point of sale counter (10+). Rule: The premises must be maintained free of infesta |
| 15 | 1 | forces F | flies_or_gnats_observed | 0.92 | jev | dead insects and flies; no F either way, label defensible | No Evidence of Insect contamination, rodent/other animals: The dead insects were found in the kitchen by the ice machines and trash cans located by the soda dispensers. / Flies were present throughout |
| 16 | 1 | forces F | conditions_only | 1.0 | jev | hole or gap | No Evidence of Insect contamination, rodent/other animals: There is a hole or gap along the floor, wall or ceiling. Back door needs to be resealed |
| 17 | 1 | forces F | pests_unspecified | 0.77 | jev | insect remains, kind unknown | Insects & rodents not present; no unauthorized animals 6-501.112 Removing Dead or Trapped Birds; Insects; Rodents and other Pests (C) Insect remains found at back exit and storage closet. Dead insects |
| 18 | 1 | forces F | conditions_only | 0.98 | jev | snap traps | 18: §228.208(b) Observed uncovered rodent snap traps |
| 19 | 1 | forces F | flies_or_gnats_observed | 0.98 | jev | dead flies on strip | 34: 3. Dead insects, rodents and other pests have not been removed from the facility. OBSERVED DEAD FLIES ON THE FLY STRIP. |
| 20 | 1 | forces F | conditions_only | 0.76 | jev | roach spray cans | 7-202.11 VIOLATION OF SECTION 7-202.11 *PRIORITY FOUNDATION* Unnecessary poisonous or toxic materials are on the premises, specifically: OBSERVED NUMEROUS CANS OF ROAD ROACH SPRAY IN KITCHEN AND IN SP |
| 21 | 1 | forces F | conditions_only | 0.8 | jev | text says no sewage back-up observed | Keep the plumbing system in good repair. Observed one broken urinal and hand sink (in men's rest room -upstairs) and one toilet in lady's restroom (first level). No sewage back-up observed at this tim |
| 22 | 1 | forces F | conditions_only | 0.45 | jev | rule text; 0.45 is below the floor so regex F stands | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 23 | 1 | forces F | conditions_only | 1.0 | jev | door gap | INSECTS, RODENTS, & ANIMALS NOT PRESENT: OUTER OPENINGS NOT PROTECTED. OBSERVED FRONT ENTRANCE DOOR NOT RODENT PROOF DOOR HAS 1/2 INCH GAP UNDERNEATH DOOR. MUST MAKE DOOR RODENT PROOF TO PREVENT PEST  |
| 24 | 1 | forces F | pests_unspecified | 0.83 | jev | a few insects in traps, kind unknown | Insects & rodents not present; no unauthorized animals 6-501.112 Removing Dead or Trapped Birds; Insects; Rodents and other Pests (Core); Observed a few insects in traps in kitchen. Last recorded pest |
| 25 | 1 | no F | vermin_observed | 0.99 | jev | rodent nesting materials; regex misses | 44e: 114259, 114259.2, 114259.3 Rodent nesting materials observed on/near an air return vent on the ceiling of the facility, directly above food preparation areas. This area must be cleaned and saniti |
| 26 | 1 | forces F | conditions_only | 1.0 | jev | gaps large enough for rodents, none seen | 38: 6-202.15 Outer openings are not protected against the entry of pests. Replace sweeps on doors to the outside - observed gaps along bottom of doors large enough to allow rodent entry. |
| 27 | 2 | no F | sewage_observed | 0.95 | jev | floor drain coming up is a backup; regex misses | 5-402.13 VIOLATION OF SECTION 5-402.13 *PRIORITY* Sewage is not disposed of through an approved system or in an approved manner, specifically: WATER IS COMING UP FROM THE FLOOR DRAIN IN THE MIDDLE OF  |
| 28 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | one fly plus rule text | 23: California Retail Food Code §114259.1. Observed one fly near the mochi donut display case. The premises of each food facility shall be kept free of vermin. Cockroaches, mice, rats, and similar pes |
| 29 | 1 | forces F | conditions_only | 0.99 | jev | minor drain leak; regex trips on sewage in corrective text | 53: §9-48(b)(4) Observed the three-compartment sink near the ice cream station to have a minor leak at the drain pipe. Corrective Actions: Stop using the three-compartment sink at the front of the kit |
| 30 | 1 | forces F | conditions_only | 1.0 | jev | uncovered rodent bait | 28: 7-206.12 Rodent bait is not contained in a covered, tamper-resistant bait station. Observed that downstairs there is uncovered rodent bait in a room with some food storage. Owner has bait boxes an |
| 31 | 1 | forces F | conditions_only | 1.0 | jev | hole or gap plus pest control record | No Evidence of Insect contamination, rodent/other animals: There is a hole or gap along the floor, wall or ceiling and must be repaired/filled up / Please remove or relocate the insect device to avoid |
| 32 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | gnats | 23: California Retail Food Code §114259.1. Observed signficant amount of gnats on the door of the room that houses the janitorial sink. The premises of each food facility shall be kept free of vermin. |
| 33 | 1 | forces F | conditions_only | 0.92 | jev | harborage conditions, no pest seen | 39: §9-54(a)(11)(D) Observed premises not being maintained free of insects, rodents and other pests and failing to eliminate harborage conditions. Observed accumulated debris and trash observed walls, |
| 34 | 1 | forces F | pests_unspecified | 0.92 | jev | dead insects, kind unknown | No Evidence of Insect contamination, rodent/other animals: Dead insects were observed in the kitchen area / Pest Control Company: ABC / Serviced Day: 05/29/2026 |
| 35 | 1 | forces F | conditions_only | 0.86 | jev | roach spray | 18: OBSERVED CANNED ROACH SPRAY INSIDE OF ESTABLISHMENT 3. Toxic substances (chemicals/materials/first aid kits/employee medications) located in the food establishment are not approved for use in a fo |
| 36 | 1 | forces F | conditions_only | 0.98 | jev | glue boards with dead flying insects; no F either way | Removing Dead or Trapped Birds Insects Rodents and other Pest (C): Remove/replace all soiled glue boards(dead flying insects) |
| 37 | 1 | forces F | conditions_only | 0.47 | regex | dead or moving roaches present; Jev 0.47 below floor | 38: 6-501.112 SHALL REMOVE DEAD OR MOVING PEST - ROACHES |
| 38 | 2 | forces F | conditions_only | 0.99 | jev | ant spray and bait stations | 7-206.11 VIOLATION OF SECTION 7-206.11 *PRIORITY* Restricted use pesticides are not approved, specifically: OBSERVED RAID ANT SPRAY AND ANT BAIT STATIONS ON COUNTER BEHIND BAR Corrective Actions: REQU |
| 39 | 1 | forces F | flies_or_gnats_observed | 0.9 | jev | house flies | Remove dead or trapped birds, insects, rodents, and other pests. live house flies observed in the facility. Note: Exterminate them and keep the exterior door closed. |
| 40 | 2 | forces F | conditions_only | 0.98 | jev | hole or gap | No Evidence of Insect contamination, rodent/other animals: There is a hole or gap along the floor, wall or ceiling. |
| 41 | 1 | forces F | pests_unspecified | 0.46 | jev | gnats; no F either way, label should be flies | No Evidence of Insect contamination, rodent/other animals: Insects were present in the facility, Actively treating the area for the gnats. |
| 42 | 1 | forces F | conditions_only | 0.96 | jev | pest control record | No Evidence of Insect contamination, rodent/other animals: All Safe / 06/08/2026; |
| 43 | 55 | forces F | conditions_only | 0.72 | neither | SF corrective boilerplate for a sewage violation: no observation in the text, but the citation implies a backflow; polic | Eliminate sewage backflow into the facility. Clean and sanitize all areas contaminated with sewage with a bleach solution (1 cup bleach per 9 cups water).Hire a licensed plumbing contractor if necessa |
| 44 | 1 | forces F | conditions_only | 0.93 | jev | unapproved roach bait | 28: 7-206.11 7-206.11 Restricted Use Pesticides, Criteria (P). Restricted use pesticides shall meet the requirements of the Code. Observed unapproved roach bait in dry storage. Discussed with PIC abou |
| 45 | 1 | forces F | pests_unspecified | 0.93 | jev | dead insects, kind unknown | 34: 3. Dead insects, rodents and other pests have not been removed from the facility. OBSERVED MULTIPLE DEAD INSECTS IN LIGHTSHIELD BY EMPLOYEE LOCKERS AND WHERE BULK FLOUR KEPT. |
| 46 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | flies observed | No Evidence of Insect contamination, rodent/other animals: Flies were observed / Pest Control Company: None Reported; |
| 47 | 1 | forces F | conditions_only | 0.4 | neither | pests accumulated in sticky traps is pests_unspecified; Jev 0.40 below floor | Insects & rodents not present; no unauthorized animals 6-501.112 Remove dead or trapped birds; insects; rodents; and other pests. Multiple sticky traps had accumulations of pests. Remove sticky traps  |
| 48 | 1 | forces F | conditions_only | 0.96 | jev | rule text | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 49 | 1 | forces F | conditions_only | 1.0 | jev | door not rodent proof | INSECTS, RODENTS, & ANIMALS NOT PRESENT: 6-202.15 OBSERVED DOCK DOOR AND REAR EXIT NOT RODENT PROOF. INSTRUCTED TO PRVIDE IN ORDER TO PREVENT PEST ENTRY. |
| 50 | 1 | forces F | conditions_only | 1.0 | jev | roach spray | 28: 7-202.11 OBSERVED A CAN OF ROACH SPRAY INSIDE OF BACK KITCHEN AREA. NOT ALLOWED TO USE IN STORE; PLEASE UTILIZE COMMERCIAL PEST CONTROL AS OFTEN AS NECESSARY |
| 51 | 1 | forces F | conditions_only | 0.71 | jev | rule text | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 52 | 1 | forces F | flies_or_gnats_observed | 1.0 | neither | rule text read as flies at 1.00; regex F also wrong | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 53 | 1 | forces F | conditions_only | 0.99 | jev | holes or gaps | No Evidence of Insect contamination, rodent/other animals: There are holes or gaps along the floor, wall or ceiling, that must be reparied |
| 54 | 1 | forces F | conditions_only | 0.82 | jev | non-sewage condensate plumbing | 53: §9-48(c)(2) Observed steam table equipment in front service area does not have plumbing installed under each basin that routes non-sewage liquid to a floor drain for proper disposal. REPEAT VIOLAT |
| 55 | 1 | forces F | conditions_only | 1.0 | jev | harborage | No Evidence of Insect contamination, rodent/other animals: Eliminate the harborage area for insects or pests. Please eliminate/remove/discard all unused equipment to avoid pest harborage / Pest Contro |
| 56 | 1 | forces F | conditions_only | 1.0 | jev | door opening | INSECTS, RODENTS, & ANIMALS NOT PRESENT: OBSERVED OUTER OPENINGS NOT PROTECTED. OBSERVED FRONT ENTRANCE DOOR NO RODENT PROOF OBSERVED 1/2 INCH OPENING ON BOTTOM DOOR. MUST MAKE DOOR RODENT PROOF TO PR |
| 57 | 2 | forces F | conditions_only | 0.96 | jev | leak contained by a bucket | 53: §9-48(b)(4) Observed a leak at the three-component sink being contained by a red bucket on the ground. The person in charge is aware of the leak and has tried to repair the issue, but the leak rem |
| 58 | 2 | forces F | pests_unspecified | 0.44 | jev | fewer than 5 dead pests, kind unknown; 0.44 below floor | 6-501.112 VIOLATION OF SECTION 6-501.112 Dead or trapped birds, insects, rodents or other pests are not removed from the establishment at a proper frequency, specifically: OBSERVED LESS THAN 5 DEAD PE |
| 59 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | fruit flies | No Evidence of Insect contamination, rodent/other animals: Fruit flies were present in the bar. |
| 60 | 1 | no F | sewage_observed | 0.95 | both | waste water tank overflowing onto the floor: grey water, but on the floor; either reading defensible | 5-402.13 VIOLATION OF SECTION 5-402.13 *PRIORITY* Sewage is not disposed of through an approved system or in an approved manner, specifically: OBSERVED WASTE WATER TANK OVERFLOWING ONTO FLOOR AND DRAI |
| 61 | 1 | forces F | conditions_only | 0.95 | jev | mouse bait | Contain rodent bait in a covered, tamper-resistant bait station. Observed mouse bait in basement. Ensure a only a certified pest control operator is placing chemicals and pest deterrents as recommende |
| 62 | 2 | forces F | flies_or_gnats_observed | 0.9 | jev | dead flies | Insects & rodents not present; no unauthorized animals 6-501.112; Core; Several dead flies in hallway leading to the side exit door that leads to the mini dumpsters. Dead or trapped birds; insects; ro |
| 63 | 1 | forces F | conditions_only | 0.99 | jev | door not rodent proof | INSECTS, RODENTS, & ANIMALS NOT PRESENT: OBSERVED REAR EXIT DOOR NOT RODENT PROOF. INSTRUCTED TO PROVIDE AND MAINTAIN TO PREVENT PEST ENTRY. |
| 64 | 1 | forces F | conditions_only | 0.78 | jev | rule text | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 65 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | fruit flies | No Evidence of Insect contamination, rodent/other animals: Insects were present in the facility Fruit flies |
| 66 | 4 | forces F | conditions_only | 0.98 | jev | closure-conditions rule text | "(a) Summary closure conditions. The manager of environmental health may summarily close any licensed food establishment for the following reasons: (1) Any one of the following conditions exist on, or |
| 67 | 1 | forces F | conditions_only | 1.0 | jev | glue traps | Removing Dead or Trapped Birds Insects Rodents and other Pest (C): Remove/replace the soiled glue traps from the basement. |
| 68 | 1 | forces F | conditions_only | 1.0 | jev | ant spray | 28: 7-206.11 OBSERVED A GALLON SIZE ANT SPRAY |
| 69 | 1 | forces F | conditions_only | 1.0 | jev | door opening | INSECTS, RODENTS, & ANIMALS NOT PRESENT: OUTER OPENINGS OBSERVED FONT ENTRANCE DOUBLE DOORS NOT RODENT PROOF DOORS HAVE 1/2 INCH OPENING ON BOTTOM DOORS. MUST MAKE DOORS RODENT PROOF TO PREVENT PEST E |
| 70 | 1 | no F | vermin_observed | 0.82 | neither | a cricket is a named insect but hardly vermin; policy call | 38: 6-501.111 Observed cricket in establishment |
| 71 | 1 | forces F | conditions_only | 0.94 | jev | rule text | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 72 | 2 | no F | sewage_observed | 0.99 | jev | closed due to wastewater backup | 54: Serious health violations were identified during today's health inspection. You must close your food facility immediately to the public until appropriate corrective actions are completed. CLOSED D |
| 73 | 1 | forces F | conditions_only | 0.6 | jev | fruit flies plus hole; no F either way, label should be flies | No Evidence of Insect contamination, rodent/other animals: Fruit flies / Seal/fill up the hole by the walk in cooler |
| 74 | 1 | forces F | conditions_only | 0.98 | jev | door sweep and cracks | 6.1(M) - Outer Openings - Protected Outer openings of a food facility were not protected against the entry of insects and rodents. Observed a door sweep in the back of the restaurant that needs repair |
| 75 | 1 | forces F | conditions_only | 0.57 | jev | food stored in a bathroom closet | 39: Canned food and single use plates are stored in the sliding door closet located in the bathroom upstairs. Food must be stored in approved areas. Discontinue storing food in locker rooms, toilet ro |
| 76 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | gnats | No Evidence of Insect contamination, rodent/other animals: Gnats present at the bar handsink |
| 77 | 1 | no F | sewage_observed | 0.99 | jev | sewer drains with waste water coming out, sewage tracked through prep; regex misses | SEWAGE & WASTE WATER PROPERLY DISPOSED: 2 SEWAR DRAINS IN MAIN PREP AREA WITH WASTE WATER COMING OUT. OBSERVED FOOD HANDLERS CONTINUING FOOD PREP WHILE TRACKING SEWAGE THROUGHOUT PREP AND DINING AREA. |
| 78 | 1 | forces F | conditions_only | 0.99 | jev | insect control device on the floor | No Evidence of Insect contamination, rodent/other animals: The insect control device was found on the floor under the hand sink. |
| 79 | 1 | no F | vermin_observed | 1.0 | jev | roach activity | 38: 6-501.111 Roach activity observed at prep table by handwash sink. |
| 80 | 1 | no F | vermin_observed | 0.76 | regex | WD 30 is a lubricant; Jev misreads it as vermin at 0.76 even alone | 28: 7-202.11 OBSERVED WD 30 NEAR MEAT SLICER |
| 81 | 1 | forces F | conditions_only | 1.0 | jev | door not rodent proof | INSECTS, RODENTS, & ANIMALS NOT PRESENT: 6-202.15 OBSERVED EXIT DOOR ON SIDE OF BUILDING NOT RODENT PROOF. INSTURCTED TO PROVIDE AND MAINTAIN TO PREVENT PEST ENTRY |
| 82 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | gnats | No Evidence of Insect contamination, rodent/other animals: Gnats present throughout the kitchen |
| 83 | 1 | forces F | flies_or_gnats_observed | 0.78 | jev | flies in sticky traps | Insects & rodents not present; no unauthorized animals 6-501.112 Remove dead or trapped birds; insects; rodents; and other pests. Multiple sticky fly traps had large accumulations of flies. CDI: Stick |
| 84 | 1 | no F | sewage_observed | 0.99 | regex | waste water pooling at an outdoor drain pipe is grey water, not sewage | 20: 1. Sewage/wastewater not disposed of in approved manner to prevent contamination. OBSERVED ACCUMULATION OF WASTE WATER AT DRAINAGE PIPELINE OUTSIDE BY DUMPSTER AREA. |
| 85 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | flying pests | 6-501.111(C) VIOLATION OF SECTION 6-501.111(C) *PRIORITY FOUNDATION* The premises is not maintained free of insects, rodents, and other pests, specifically: OBSERVED SMALL FLYING PESTS IN THE FRONT LO |
| 86 | 2 | forces F | flies_or_gnats_observed | 1.0 | neither | rule text read as flies at 1.00; regex F also wrong | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 87 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | flies | No Evidence of Insect contamination, rodent/other animals: Flies are present in the kitchen |
| 88 | 1 | forces F | conditions_only | 0.94 | jev | roach insecticide | TOXIC SUBSTANCES PROPERLY IDENTIFIED, STORED, & USED: OBSERVED RESIDENTIAL ROACH INSECTICIDE, BORIC ACID STORED IN DRY STORAGE AREA. INFORMED THAT RESIDENTIAL USE INSECTICIDES ARE NOT PERMITTED IN COM |
| 89 | 1 | forces F | conditions_only | 1.0 | jev | rule text | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 90 | 1 | forces F | conditions_only | 1.0 | jev | door gap | INSECTS, RODENTS, & ANIMALS NOT PRESENT: 6-202.15 OBSERVED THE DOORS BY THE PATIO NOT RODENT PROOFED WITH A MORE THAN ONE INCH GAP AT THE BOTTOM OF BOTH DOORS. INSTRUCTED TO MAKE SAID DOORS RODENT PRO |
| 91 | 1 | forces F | conditions_only | 0.99 | jev | mouse trap and small opening | 44e: 114259, 114259.2, 114259.3 Observed mouse trap on food contact surface. Corrected immediately. Observed small opening at the back door of the facility. Repair immediately. |
| 92 | 1 | forces F | flies_or_gnats_observed | 1.0 | jev | fruit flies | 6-501.112 VIOLATION OF SECTION 6-501.112 Dead or trapped birds, insects, rodents or other pests are not removed from the establishment at a proper frequency, specifically: LARGE AMOUNT OF FRUIT FLIES  |
| 93 | 1 | forces F | conditions_only | 0.63 | regex | evidence of rodent droppings is evidence; Jev 0.63 | Cleaning Floors Dustless Methods (C): Clean the floor under the 3 bay sink to remove soils. Clean the basement location to remove evidence of rodent droppings |
| 94 | 1 | forces F | conditions_only | 1.0 | jev | weatherstrip | No Evidence of Insect contamination, rodent/other animals: The outer door in the back of the kitchen needs to be weatherstrip leading to the outside |
| 95 | 2 | forces F | conditions_only | 0.93 | jev | empty rodent repellent container | 7-206.11 VIOLATION OF SECTION 7-206.11 *PRIORITY* Restricted use pesticides are not approved, specifically: *** Observed an empty container of TOMCAT rodent repellent on the chemical storage rack. Cor |
| 96 | 1 | forces F | conditions_only | 1.0 | jev | snap trap | 18: §228.208(b) Observed snap- type mouse trap under kitchen counters- |
| 97 | 1 | forces F | conditions_only | 0.95 | jev | residential rat trap | 34: 4. Insect control devices are improperly designed and/or installed. OBSERVED RESIDENTIAL RAT TRAP BEHIND REACH IN COOLER. |
| 98 | 1 | forces F | conditions_only | 0.98 | jev | roach spray | 32: 2. The equipment used to store, or prepare, food does not exceed National Sanitation Foundation (NSF) standards, or their equivalent; and may not be safe to use for food. OBSERVED HOUSEHOLD CHEMIC |
| 99 | 1 | forces F | conditions_only | 0.89 | jev | ant and roach killer can | Toxic substances properly identified stored & used 7-202.12; Priority; Found a can of "Hot Shot- Ant; Roach; and Spider Killer" on lower shelf with label stating "For residential use only". Poisonous  |

### summary

| # | lines | regex | Jev | conf | who | why | text |
|---|---|---|---|---|---|---|---|
| 0 | 1 | Worker eating/drinking near food | Plumbing leaks | 1.0 | jev | drainpipe leak; regex tripped on 'drink' | PLUMBING INSTALLED; PROPER BACKFLOW DEVICES: 5-205.15// NOTED HANDSINK NEXT TO FOUNTAIN DRINK AND SLUSHY MACHINE WITH LEAK COMING FROM THE DRAINPIPE. INSTRUCTED TO REPAIR |
| 1 | 1 | 16: Clean GASKETS, MICROWAVE AND CAN OPE | Dirty equipment surfaces | 0.6 | jev | gaskets and microwave are equipment surfaces | 16: Clean GASKETS, MICROWAVE AND CAN OPENER |
| 2 | 1 | 6: Employee food and beverage was observ | Worker eating/drinking near food | 0.78 | jev | employee food near prep areas | 6: Employee food and beverage was observed being stored throughout different mulitple locations as well as near prep areas |
| 3 | 1 | 10: 6-301.11 MISSING PAPAER TOWEL I NTHE | Hand sink blocked/dirty | 0.98 | jev | missing paper towel | 10: 6-301.11 MISSING PAPAER TOWEL I NTHE HND SINK |
| 4 | 1 | Chemicals unsafe/unlabeled | Dishwasher not sanitizing | 1.0 | jev | no chlorine at dish machine | 4.4(P) - Warewashing equipment: Determining Chemical Sanitizer Concentration Sanitizer concentration is not being monitored. No chlorine dispensed at final rinse cycle at dish machine |
| 5 | 1 | 48: 4-302.14 SHALL HAVE CHLORINE TEST ST | Dishwasher not sanitizing | 0.53 | jev | test strips | 48: 4-302.14 SHALL HAVE CHLORINE TEST STRIPS TO TEST CHLORINE CONCENTRATIONS. |
| 6 | 1 | Worker eating/drinking near food | Dirty equipment surfaces | 0.6 | jev | dirt on dispenser and shelves | 16: 4-601.11(A) Observed dirt and debris on drink dispenser of ice machine, shelves of the RIC, and front plate of ice machine. |
| 7 | 1 | Using a Handwashing Sink-Operation and M | Hand sink blocked/dirty | 1.0 | jev | hand sink used as dump sink and blocked | Using a Handwashing Sink-Operation and Maintenance (Pf): Bar handwashing sink observed being used as dump sink and vice versa/ observed blocked by strainer and mixing cup. Discontinue Kitchen main han |
| 8 | 1 | Dishwasher not sanitizing | none_of_these | 0.93 | jev | wooden grater: no phrase fits | 48: §9-37(e)(2)(A) Observed wooden grater in warewash area. Wooden grater did not have any accreditation that showed it is food commercial grade. Corrective Actions: Certified food manager shall remov |
| 9 | 2523 | Permit not posted | none_of_these | 1.0 | jev | permit scope text, not a posting; regex wrong on 2,523 lines | Operating within the parameters of the health permit. Compliance with Time as a Public Health Control, waiver, specialized process, and Hazard Analysis Critical Control Point (HACCP) plan. |
| 10 | 1 | Vent hood grease buildup | none_of_these | 1.0 | jev | vomit procedures | 6: §9-12 Observed no written procedures available for responding to diarrheal and vomiting events. Corrective Actions: Provide and maintain written procedures outlining the proper cleanup and disinfec |
| 11 | 1 | Sewage backup/issue | none_of_these | 1.0 | jev | a category string | SEWAGE AND WASTEWATER DISPOSAL, VERMIN AND ANIMAL CONTAMINATION, HANDWASHING FACILITIES, HOT AND COLD HOLDING TEMPERATURES |
| 12 | 1 | Dirty food contact surfaces | Mold/Slime in ice machine or bin | 0.52 | regex | mineral buildup is not mold; Jev 0.52 | FOOD & NON-FOOD CONTACT SURFACES CLEANABLE, PROPERLY DESIGNED, CONSTRUCTED & USED: OBSERVED MINOR MINERAL BUILD UP INSIDE OF SODA GUN LOCATED AT MAIN BAR. MUST CLEAN AND MAINTAIN. |
| 13 | 1 | 9: OBSERVED RAW SHELLED EGGS OVER CHEESE | Raw food touching ready-to-eat | 1.0 | jev | raw eggs over cheesecake | 9: OBSERVED RAW SHELLED EGGS OVER CHEESE CAKE. 1. Food is not protected from cross-contamination, or other contamination, during storage, preparation, holding, display or is in contact with unclean su |
| 14 | 1 | Chemicals unsafe/unlabeled | Dishwasher not sanitizing | 0.99 | jev | no test strips | 48: Observed no chemical strips for the chemical dish machine. Corrective Actions: Must obtain chemical test strips for chemical dish machine. |
| 15 | 1 | Dirty food contact surfaces | Dirty equipment surfaces | 1.0 | jev | non-food contact debris | Non-food contact surfaces clean 4-601.11(B) and (C); Core; Space under the drawer unit cooler has accumulation of food debris/residue; inside walls/ceiling of microwaves need more frequent cleaning. N |
| 16 | 1 | 1: OBSERVED ICE MACHINE WITH BLACK ORGAN | Mold/Slime in ice machine or bin | 1.0 | jev | black matter in ice machine | 1: OBSERVED ICE MACHINE WITH BLACK ORGANIC MATTER |
| 17 | 1 | Dirty food contact surfaces | none_of_these | 0.96 | jev | towel drying: no phrase fits | Utensils; equipment & linens: properly stored; dried & handled 4-901.11 (A); Core; Observed a food employee drying a food container with a towel after going through the dish machine. After cleaning an |
| 18 | 1 | Chemicals unsafe/unlabeled | none_of_these | 0.82 | jev | unlabeled salt and sugar are not chemicals | Food properly labeled: original container 3-302.12; Core; Containers of salt and sugar and other spices that can't be clearly identified along the cook line did not have a label. Except for containers |
| 19 | 1 | Worker eating/drinking near food | none_of_these | 0.53 | jev | personal sauce stored; no phrase fits | 2-401.11 VIOLATION OF SECTION 2-401.11 *PRIORITY FOUNDATION* Food employees eat, drink or use tobacco in unapproved areas or use an inappropriate beverage container for drinking, specifically: OBSERVE |
| 20 | 1 | Worker eating/drinking near food | none_of_these | 0.98 | jev | date marking: no phrase fits | Use an effective method to date mark refrigerated, ready-to-eat, TCS food prepared and held for more than 24 hours. Day one is the day the food was made. Observed no date marking system being used for |
| 21 | 1 | 48: 4-302.14 SHALL PROVIDE APPROPRIATE T | Dishwasher not sanitizing | 0.77 | jev | test strips | 48: 4-302.14 SHALL PROVIDE APPROPRIATE TEST STRIPS FOR 3 COMP SINK |
| 22 | 1 | Dirty food contact surfaces | Mold/Slime in ice machine or bin | 0.85 | regex | bins with debris stored as clean are dirty food-contact items, not mold | Non-food contact surfaces clean 4-601.11(B) and (C); Core; Bins of smallwares and pan lids stored as clean with debris/residue accumulation in the bins. NonFOOD-CONTACT SURFACES of EQUIPMENT shall be  |
| 23 | 1 | Floors/Walls dirty or damaged | none_of_these | 0.97 | jev | onions on floor | Contamination prevented during food preparation; storage & display 3-305.11;Core; Two bags of onions stored on the floor. Store food in a clean; dry location; not exposed to contamination. Keep at lea |
| 24 | 1 | Sewage backup/issue | none_of_these | 0.49 | jev | condensation line is not sewage; below floor but hazard rule drops the regex phrase | Dispose of condensate drainage, no sewage liquids and rainwater from the point of discharge to disposal according to Minnesota Pollution Control Agency Rules Chapters 7080, 7081 and 7083 Observed cond |
| 25 | 1 | Worker eating/drinking near food | Dirty food contact surfaces | 1.0 | jev | can opener blade | 10: 3. Food contact surfaces or utensils were not cleaned and sanitized, as required, when changing from raw to Ready-To-Eat foods, changing between different raw animal foods, or when contamination h |
| 26 | 1 | Worker eating/drinking near food | none_of_these | 0.83 | jev | date marking | 3.4(G)(2) - VI. Time/Temperature Controlled Safety Food - Ready-to-eat, TCS - date marking Ready-to-eat, TCS food that was prepared in a food processing plant and subsequently opened in a FSO or a RFE |
| 27 | 1 | Dirty food contact surfaces | Dirty equipment surfaces | 0.91 | jev | mold on non-food surfaces is dirty equipment | 4.5(A)(2)(3) - Equipment, food-contact surfaces, nonfood-contact surfaces, and utensils Non-food contact surfaces are dirty. Observed mold on bus pan holding soiled dishes and on metal wall plate near |
| 28 | 1 | Worker eating/drinking near food | none_of_these | 0.75 | jev | code definition of eating/drinking rule | 6: 2-401.11 2-401.11 Eating, Drinking, or Using Tobacco Products (C). An employee shall eat, drink, or use any form of tobacco products only in designated areas where the contamination of exposed food |
| 29 | 1 | Dirty food contact surfaces | Dirty equipment surfaces | 1.0 | jev | residue on racks | Non-food contact surfaces clean 4-601.11 (B) and (C); Residue accumulation and debris along wire racks in walk-in cooler; prep coolers; low oven; and low shelf of roller cart. NonFOOD-CONTACT SURFACES |
| 30 | 1 | Vent hood grease buildup | none_of_these | 1.0 | jev | vomit procedures | 6: §9-12 Failure of food establishment to have written procedures to follow and equipment listed in the written procedures when responding to vomit and diarrhea events. Corrective Actions: A food esta |
| 31 | 58 | Dirty food contact surfaces | Dishwasher not sanitizing | 0.76 | jev | test kit not provided | 29: 2. A test kit or other device that accurately measures the concentration in mg/L of sanitizing solutions, or for the utensil surface temperature is not provided and accurate to +/- 2°F in the inte |
| 32 | 1 | Floors/Walls dirty or damaged | none_of_these | 0.93 | jev | food on floor | 3.2(Q) - Food Storage: Preventing Contamination from the Premises Food items are not protected from contamination during storage. Discontinue storing containers of food directly on floor in walk ins |
| 33 | 1 | Handwashing sinks supplied & accessible  | Hand sink blocked/dirty | 1.0 | jev | hand sink blocked | Handwashing sinks supplied & accessible 5-205.11; Priority Foundation; Kitchen handwashing sink in dish area completely blocked with large table; large bin in the sink; large strainer and multiple pan |
| 34 | 1 | Improper hand washing | none_of_these | 0.79 | jev | bare-hand contact has no phrase; regex phrase untrue | No bare hand contact with RTE foods or pre- approved alternate procedure properly followed 3-301.11(B) An employee at the sushi station was cutting cucumbers with his bare hands. Except when washing p |
| 35 | 13 | Vent hood grease buildup | none_of_these | 1.0 | jev | vermin-proofing code text | A food facility shall at all times be constructed, equipped, and maintained, and operated as to prevent the entrance and harborage of animals, birds, and vermin, including, but not limited to rodents  |
| 36 | 1 | Dirty food contact surfaces | Mold/Slime in ice machine or bin | 0.96 | jev | pink residue in ice machine | FOOD-CONTACT SURFACES: CLEANED & SANITIZED: OBSERVED A SMALL AMOUNT OF PINK RESIDUE IN THE CREASE OF THE INSIDE PANEL OF THE ICE MACHINE. INSTRUCTED TO CLEAN AND MAINTAIN. |
| 37 | 1 | Improper hand washing | none_of_these | 0.46 | neither | manual dishwashing order wrong: Dishwasher not sanitizing fits loosely; Jev 0.46, regex Improper hand washing wrong | 14b: 114097, 114099.1, 114099.2, 114099.4, 114099.6, 114099.7, 114101, 114105, 114109, 114111, 114185.3 (a-c) When asked to demonstrate how to wash dishes, the employee stated that dishes are rinsed a |
| 38 | 1 | Dirty food contact surfaces | none_of_these | 0.95 | jev | instruction to the PIC | 1: 2-103.11 Person in charge must ensure employees are properly cleaning and sanitizing utensils |
| 39 | 1 | Keep the physical facilities in good rep | Floors/Walls dirty or damaged | 0.99 | jev | broken tiles | Keep the physical facilities in good repair. Observed tiles inside the keg walk-in cooler broken - repair. |
| 40 | 1 | Hand sink blocked/dirty | Chemicals unsafe/unlabeled | 0.99 | jev | concentrate on prep sink drainboard | Toxic substances properly identified stored & used 7-201.11; Priority; Bagged wash solution concentrate was found stored on the drainboard of the prep sink. Employee stated that the bag is stored here |
| 41 | 1 | Dirty food contact surfaces | Dishwasher not sanitizing | 0.98 | jev | no test strips | 48: Observed no test strips for any sanitizer on site. Corrective Actions: Must obtain chemcial test strips. |
| 42 | 1 | 15: 3-302.11(A)(1)(b) Raw beef patties w | Raw food touching ready-to-eat | 1.0 | jev | raw beef next to tortillas | 15: 3-302.11(A)(1)(b) Raw beef patties were stored next to tortillas in cooler drawer. |
| 43 | 1 | Floors/Walls dirty or damaged | none_of_these | 0.61 | jev | ice buildup is neither dirt nor damage | 6-501.11 VIOLATION OF SECTION 6-501.11 Physical facilities are not maintained in good repair, specifically: OBSERVED ICE BUILD UP ON WALL UNDER THE FAN BOX OF THE WALK IN FREEZER. Corrective Actions:  |
| 44 | 1 | Packaged and Unpackaged Food-Separation  | Raw food touching ready-to-eat | 0.98 | jev | raw chicken above beef, RTE below | Packaged and Unpackaged Food-Separation Packaging and Segregation (P): In the walk-in refrigerator observed raw chicken stored directly above raw beef and RTE cut vegetables stored directly below raw  |
| 45 | 2 | Chemicals unsafe/unlabeled | none_of_these | 0.89 | jev | unlabeled flour is not a chemical | 41: 1. Food is not properly labeled. FLOUR WITHOUT LABEL |
| 46 | 1 | Improper hand washing | Food not kept cold enough | 1.0 | jev | cooling violation; regex phrase absurd | 20: 3-501.14 3-501.14(A) Cooling (P). Cooked time/temperature control for safety food shall be cooled within 2 hours from 57°C (135°F) to 21°C (70°F) and within a total of 6 hours from 57°C (135°F) to |
| 47 | 1 | Raw food touching ready-to-eat | none_of_these | 0.67 | jev | wiping cloths not in sanitizer | Store moist wiping cloths used with raw animal foods in separate sanitizing solution from cloths used for other purposes. Wet wiping cloth not stored in the chemical sanitizer bucket between use. Note |
| 48 | 1 | 37: OBSERVATION: Inspector observed sush | Food not kept cold enough | 0.4 | regex | sushi paddle in room-temperature water is not cold holding; Jev 0.40 below floor | 37: OBSERVATION: Inspector observed sushi paddle with rice pieces on it resting in a container of water at room temperature. WAC: In-use utensils for TCS (time/temperature control for safety) foods In |
| 49 | 2 | Dirty food contact surfaces | none_of_these | 0.52 | jev | torn gaskets | Equipment; food & non-food contact surfaces approved; cleanable; properly designed; constructed & used 4-501.11; Core - Some refrigerator door gaskets were torn or peeling...Food service equipment sha |
| 50 | 1 | Hand sink blocked/dirty | none_of_these | 0.91 | jev | chips next to hand sink | Contamination prevented during food preparation; storage & display 3-305.11 Core; Food Storage - Chips stored next to hand sink. Food shall be protected from contamination by storing the food in a cle |
| 51 | 1 | Chemicals unsafe/unlabeled | Dishwasher not sanitizing | 0.84 | jev | no sanitizer available | Utensils that have been washing in the 3-cpmpartment sinks are not sanitized. The chemical sanitizers were not available.(CORRECT VIOLATION WITHIN 5 CALENDAR DAYS) |
| 52 | 1 | Worker eating/drinking near food | Food spoiled/unsafe | 0.97 | jev | expired RTE food not discarded | 3.4(H)(1) - VI. Time/Temperature Controlled Safety Food - Ready-to-eat, TCS - disposition Ready-to-eat, TCS food was not discarded by the date mark, or was undated and not discarded or was not discard |
| 53 | 1 | 49: 4-601.11(C) INNER TOP OF THE MICROWA | Dirty equipment surfaces | 0.69 | jev | microwave interior | 49: 4-601.11(C) INNER TOP OF THE MICROWAVE SHALL BE CLEANED |
| 54 | 2 | Floors/Walls dirty or damaged | none_of_these | 0.97 | jev | carrots on floor | 40: §9-22(a)(1)(C) OBSERVED A BAG OF CARROTS ON FLOOR IN WALK IN COOLER Corrective Actions: OWNER MOVED CARROTS FROM FLOOR. TRAIN STAFF ON PROPER FOOD STORAGE |
| 55 | 1 | Mold/Slime in ice machine or bin | Floors/Walls dirty or damaged | 0.48 | jev | soiled vent and moldy wall; below floor | 6.4(B)(1) - Cleaning- Frequency and Restrictions The physical facilities are not cleaned as often as necessary to keep them clean. Soiled ceiling vent above 3 comp. Sink. Mold on 3 comp. Wall. |
| 56 | 1 | 11: §9-52(a)(3)(A) Missing a supply of p | Hand sink blocked/dirty | 0.94 | jev | missing paper towels | 11: §9-52(a)(3)(A) Missing a supply of paper towels at kitchen handsinks. Paper towels not provided from a dispensing unit. Corrective Actions: Missing a supply of paper towels at kitchen handsinks. P |
| 57 | 1 | 15: 3-302.11(A)(1)(b) OBSERVED RAW EGGS  | Raw food touching ready-to-eat | 0.99 | jev | raw eggs above boiled eggs | 15: 3-302.11(A)(1)(b) OBSERVED RAW EGGS STORED ABOVE BOILED EGGS. |
| 58 | 1 | Floors/Walls dirty or damaged | Mold/Slime in ice machine or bin | 0.96 | jev | brown substance in ice machine | 16: 4-601.11(A) Observed brown substance in ceiling of ice machine. |
| 59 | 1 | 56: §9-54(a)(2)(A) The inspector observe | Floors/Walls dirty or damaged | 1.0 | jev | dirty floors walls ceilings | 56: §9-54(a)(2)(A) The inspector observed dirty floors, walls, and ceilings throughout the back of house area. Corrective Actions: You must clean under and around the equipment and clean floors, walls |
| 60 | 1 | Floors/Walls dirty or damaged | none_of_these | 0.71 | jev | two issues, truncation is fair | 37: 1. Food is not properly stored or is stored in prohibited areas. OBSERVED ITEMS STORED ON FLOOR NEAR DISHWASHER. 2. Food is not protected from contamination. OBSERVED RAW BEEF UNCOVERED IN WALK IN |
| 61 | 1 | 33: 114115(b), 114257 Observed food resi | Dirty food contact surfaces | 0.63 | neither | debris under equipment on the floor is a floor issue; Jev food-contact 0.63, regex truncation | 33: 114115(b), 114257 Observed food residue and other debris under the ice machine, reach in refrigerator, and dry storage racks. Correct immediately. |
| 62 | 1 | Permit not posted | none_of_these | 0.97 | jev | DBA change is not a posting | (A)-(P) Person-In-Charge-Duties (Pf): Provide a DBA change to add "Patty Haus" to permit. |
| 63 | 1 | Dirty food contact surfaces | Dirty equipment surfaces | 0.98 | jev | rust on shelving | 42: 1. Nonfood-contact surfaces are not cleaned at a frequency necessary to preclude accumulation of soil residues. OBSERVED ACCUMULATION OF RUST ON SHELVING UNDER GRILL. |
| 64 | 1 | There are no paper towels at the handwas | Hand sink blocked/dirty | 0.99 | jev | no paper towels | There are no paper towels at the handwashing sink. Person on duty replaced towels during time of inspection. |
| 65 | 1 | Handwashing sinks supplied & accessible  | Hand sink blocked/dirty | 0.95 | jev | paper towels | Handwashing sinks supplied & accessible 6-301.12; Priority Foundation; Paper towels were not available at either of the handwashing sinks in the kitchen. Each handwashing sink or group of adjacent han |
| 66 | 375 | Dirty food contact surfaces | none_of_these | 0.54 | jev | test kit rule text | 4-302.14 A test kit or other device that accurately measures the concentration in MG/L of SANITIZING solutions shall be provided |
| 67 | 1 | Gaps/Openings (Risk of entry) | Live roaches found | 0.91 | jev | dead American roaches; phrase covers dead roaches today | 38: Observed several dead American roaches under the soda fountain and in the back utility closet. Observed an at home chemical sprayer in the utility closet. Observed the back door had a large gap at |
| 68 | 1 | 36: 4-203.12 ENSURE ALL COOLER HAVE THER | Missing thermometers | 0.67 | jev | cooler thermometers | 36: 4-203.12 ENSURE ALL COOLER HAVE THERMOMETERS AND THEY ARE ACCURATE |
| 69 | 1 | Chemicals unsafe/unlabeled | none_of_these | 0.9 | jev | date labels are not chemicals | 23: Observed several prepped food items with no date label. Corrective Actions: Ensure all prepped food items have a date label. |
| 70 | 1 | Floors/Walls dirty or damaged | none_of_these | 0.99 | jev | wall covering code text | (A) Wall and ceiling covering materials shall be attached so that they are EASILY CLEANABLE. (B) Except in areas used only for dry storage, concrete, porous blocks, or bricks used for indoor wall cons |
| 71 | 1 | Food spoiled/unsafe | none_of_these | 0.6 | jev | microwaving in wrong plastic: no phrase fits | 3 - V. Food from Approved Source - Food Safe, Unadulterated and Honestly Presented Food is either unsafe, adulterated or not honestly presented. Observed food being microwaved in plastic containers no |
| 72 | 1 | Worker eating/drinking near food | Raw food touching ready-to-eat | 1.0 | jev | eggs over drinks | 3.2(C)(1)(a) - VII. Protection from Contamination - Packaged and Unpackaged Food - Separation, Packaging, and Segregation Ready-to-eat food was not protected from cross-contamination by raw animal foo |
| 73 | 1 | Improper hand washing | none_of_these | 0.95 | jev | unwashed produce is not hand washing | Washing fruits & vegetables 3-302.15; Employee observed taking produce that was just received; straight out of the box to cut without washing the produce. Wash fruits and vegetables prior to use. |
| 74 | 1 | Dirty food contact surfaces | Dirty equipment surfaces | 0.62 | jev | microwave grime | 17: §9-41(a)(1)(A) Observed the inside of the microwave unit across from grill to have a build up of grime and other debris. Corrective Actions: WITHIN 24 HOURS SHALL ENSURE MICROWAVE IS CLEANED. SHAL |
| 75 | 1 | 49: 4-601.11(C) Clean the walls and floo | Floors/Walls dirty or damaged | 0.56 | jev | oil on walls and floors | 49: 4-601.11(C) Clean the walls and floors behind the grill of oil spills. |
| 76 | 2 | Sewage backup/issue | none_of_these | 0.79 | jev | auxiliary grey-water tank is not a backup | 5-403.11 VIOLATION OF SECTION 5-403.11 *PRIORITY* Sewage is not disposed through an approved sewage disposal system, specifically: USING AUXILIARY WASTE WATER TAN FOR GREY WATER DISPOSAL. 4/1/2026 I D |
| 77 | 1 | Vent hood grease buildup | none_of_these | 0.53 | jev | dust in restroom vent is not the hood | 56: Observed what appeared to be dust accumulation in the air vent located in the women's restroom . Corrective Actions: Must clean air vent. |
| 78 | 1 | 18: OBSERVED HOUSEHOLD PESTICIDE STORED  | Chemicals unsafe/unlabeled | 0.97 | jev | household pesticide | 18: OBSERVED HOUSEHOLD PESTICIDE STORED IN KITCHEN AREA. EMPLOYEE REMOVED PESTICIDE. ENSURE THAT ONLY RESTAURANT APPROVED PESTICIDES ARE USED IN ESTABLISHMENT. |
| 79 | 1 | Floors/Walls dirty or damaged | none_of_these | 0.99 | jev | single-use articles code text | Single-use & single-service articles: properly stored & used 4-903.11(A)(C) Store single-use and single-service articles in a clean; dry location; where they are not exposed to splash; dust; or other  |
| 80 | 1 | Vent hood grease buildup | Live roaches found | 0.99 | neither | 6-501.112 title read as roaches at 0.99; the hazard rule drops the phrase, but the answer is wrong | 38: 6-501.112 6-501.112 Removing Dead or Trapped Birds, Insects, Rodents and other Pests (C). Dead or trapped pests shall be removed from control devices and the premises at a frequency that prevents  |
| 81 | 1 | Dirty food contact surfaces | none_of_these | 1.0 | jev | boxes on floor | FOOD & NON-FOOD CONTACT SURFACES CLEANABLE, PROPERLY DESIGNED, CONSTRUCTED & USED: OBSERVED BOXES AND CONTAINERS STORED ON FLOOR IN FIRST FLOOR AND BASEMENT DRY STORAGE AREA. INSTRUCTED TO ELEVATE ALL |
| 82 | 1 | Chemicals unsafe/unlabeled | none_of_these | 0.57 | jev | sanitizer bucket at 0 ppm: no phrase fits | Wiping cloths: properly used & stored 3-304.14; Core; Sanitizer concentration too low in sanitizer bucket; measured at 0ppm quat. Wiping cloths not fully submerged in sanitizer. Cloths in-use for wipi |
| 83 | 1 | Vent hood grease buildup | none_of_these | 1.0 | jev | vomit procedures | Procedures for responding to vomiting & diarrheal events 2-501.11; Priority Foundation; PIC could not locate or recall procedures for a diarrheal or vomiting event.- A FOOD ESTABLISHMENT shall have wr |
| 84 | 1 | Food not kept hot enough | Missing thermometers | 0.89 | jev | hot-holding thermometers in poor repair | Equipment; food & non-food contact surfaces approved; cleanable; properly designed; constructed & used Observation: thermometers on the alto sham are in poor repair. Replace thermometers on the hot ho |
| 85 | 1 | Dirty food contact surfaces | Gaps/Openings (Risk of entry) | 0.75 | neither | split door gaskets are not pest openings; Jev 0.75, regex wrong too | Food and Non-food Contact surfaces cleanable, properly designed, constructed, and used: Door gaskets on the front reach in coolers need to be replaced due to being split. / Remove all old equipment th |
| 86 | 1 | Floors/Walls dirty or damaged | none_of_these | 0.97 | jev | mops not raised | Storing Maintenance Tools (C): Mops brooms etc not raised 6" above the floor |
| 87 | 1 | Hand sink blocked/dirty | Improper hand washing | 0.91 | jev | hands washed at the wrong sink | 9: §9-10(a)(5) Observed PIC attempt to wash his hands at three-compartment sink and not at designated handwashing sink in warewashing area. Corrective Actions: Food employees shall clean their hands i |
| 88 | 3 | 2: §9-68(c)(1)(K) Observed no sign or pl | Permit not posted | 0.94 | jev | no inspection-report sign | 2: §9-68(c)(1)(K) Observed no sign or placard notifying customers that a copy of the most recent establishment inspection report is available upon request. Corrective Actions: Shall post a sign or pla |
| 89 | 1 | 16: 4-601.11(A) Observed observed food d | Dirty food contact surfaces | 1.0 | jev | debris on slicer and can opener | 16: 4-601.11(A) Observed observed food debris on the slicer, food scale, and can opener in the prep area. |
| 90 | 1 | Floors/Walls dirty or damaged | none_of_these | 0.97 | jev | mops in service sink | Physical facilities installed; maintained & clean 6-501.16; Core; Mops leaning up against wall in service sink; mop-head side up. After use; mops shall be placed in a position that allows them to air- |
| 91 | 0 | 39: 04. Surfaces of utensils and equipme | Dirty food contact surfaces | 0.96 | jev | residue on coffee nozzles | 39: 04. Surfaces of utensils and equipment contacting non-TCS foods are not properly cleaned. OBSERVED ACCUMULATION OF RESIDUE ON DISPENSER NOZZLES ON ALL COFFEE MACHINES NEAR RESTROOMS Corrected On S |
| 92 | 1 | Dishwasher not sanitizing | Live roaches found | 1.0 | jev | live roaches on the dish machine; regex said dishwasher | Insects & rodents not present; no unauthorized animals 6-501.111; Core; Observed live roaches on top of the dish machine. The premises shall be maintained free of insects; rodents; and other pests. Th |
| 93 | 1 | Rodent activity detected | none_of_these | 0.7 | jev | rule text | The services of a licensed pest control operator shall be required when a facility has evidence of rodent or insect activity to the extent that rodent droppings are visible, or live insects are seen d |
| 94 | 1 | Dirty food contact surfaces | Mold/Slime in ice machine or bin | 0.98 | jev | pink substance in ice bin | FOOD-CONTACT SURFACES: CLEANED & SANITIZED: OBSERVED INTERIOR TOP OF BIN OF ICE MACHINE COMPARTMENT WITH PINK SUBSTANCE. INSTRUCTED TO CLEAN, SANITIZED AND MAINTAIN UNIT AT ALL TIMES TO PREVENT CONTAM |
| 95 | 298 | Openings. Outside openings not protected | Gaps/Openings (Risk of entry) | 0.81 | jev | Houston openings item: the cited condition is openings | Openings. Outside openings not protected against the entrance of insects by filling or closing holes and other gaps along floors / walls / ceilings / tight-fitting, self-closing doors, kept closed / c |
| 96 | 0 | Vent hood grease buildup | Dirty equipment surfaces | 0.87 | jev | dirty equipment exteriors | 49: 4-601.11(C) Clean exterior of dirty equipment, clean dirty vent hood, clean dirty floors throughout kitchen, clean dirty floors in WIF, sand and paint or replace dirty/rusted storage shelves throu |
| 97 | 1 | Dirty food contact surfaces | none_of_these | 0.79 | jev | sanitizer bucket at 0 ppm | Wiping cloths: properly used & stored 3-304.14; Core; A sanitizer bucket registered 0 ppm of Quat. Wet wiping cloths shall be stored in sanitizer at the appropriate concentration. Quat shall register  |
| 98 | 1 | 16: 4-601.11(A) Observed heavy debris bu | Dirty food contact surfaces | 0.93 | jev | debris around can opener | 16: 4-601.11(A) Observed heavy debris build up around can opener on food prep table |
| 99 | 1 | Store raw eggs in refrigeration equipmen | Food not kept cold enough | 0.99 | jev | raw eggs at 57F | Store raw eggs in refrigeration equipment at 45 degrees F (7 degrees C) or less. Raw shell eggs found sitting at 57 F (ambient temperature) on the cook line. Note: Store them in refrigerator. PIC relo |

