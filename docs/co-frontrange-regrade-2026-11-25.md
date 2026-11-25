# Colorado Front Range regrade, 2026-11-25

The six Front Range counties (El Paso, Larimer, Adams, Weld, Arapahoe,
Douglas) were scored on keywords: each violation line was given a tier by
the words in it. On the state's own portal that method produced four times
the F's the state's printed tiers do (`docs/expansion-2026-11b/mhd.md`).
This regrade moves the rows already stored to the state's tier wherever a
line carries it. The fetcher reads lines the same way from this date on.

## How a line's tier is known

`dinescores_pipeline._co_line_tier`, in this order:

1. CDPHE's phrase, "This is a Core item ..." (none of these six print it).
2. El Paso County's bracket after the cited provision's title:
   `23: 3-501.18 3-501.17 Ready-To-Eat ... Date Marking (Pf). Refrigerated ...`
   The bracket belongs to the provision the line quotes, which is not always
   the library section it is filed under. Where both can be read they agree
   on 5,956 of 6,043 stored lines; the bracket wins on the other 87.
3. The section the line opens with, looked up in `CO_SECTION_TIERS` (the
   tier CDPHE's lines state for each of 184 library sections).

A line with none of these keeps its stored keyword tier.

## What it reaches

| County | Stored lines | On the state's tier | Share |
|---|---|---|---|
| El Paso (Colorado Springs) | 6,870 | 6,437 | 94% |
| Douglas | 1,005 | 736 | 73% |
| Adams | 7,423 | 863 | 12% |
| Arapahoe | 3,938 | 392 | 10% |
| Larimer (Fort Collins) | 6,950 | 210 | 3% |
| Weld (Greeley) | 5,279 | 92 | 2% |

The four counties at the bottom mostly write the form's item number and the
inspector's own sentence (`13: Three dented cans of oyster sauce ...`), so
most of their lines, and most of their grades, stay on keywords. Keyword
tiers call 56 to 71% of their lines Priority; the state's own mix on CDPHE's
portal is 36%. There is no stated tier to read on those lines, and the item
number does not settle it (14 of the 52 items CDPHE cited were always one
tier).

## Result

Restaurant grades before and after, from the export of 2026-11-25 21:24 UTC
(8,358 restaurants, 10,047 inspections):

| County | Restaurants | Changed | A | B | C | F | A after | B after | C after | F after |
|---|---|---|---|---|---|---|---|---|---|---|
| El Paso | 2,485 | 1,269 | 1,727 | 490 | 186 | 82 | 1,961 | 426 | 60 | 38 |
| Larimer | 1,878 | 53 | 994 | 560 | 237 | 87 | 1,006 | 557 | 231 | 84 |
| Adams | 1,686 | 140 | 931 | 409 | 217 | 129 | 949 | 424 | 200 | 113 |
| Weld | 1,234 | 20 | 740 | 343 | 112 | 39 | 741 | 342 | 112 | 39 |
| Arapahoe | 748 | 64 | 284 | 207 | 142 | 115 | 292 | 209 | 145 | 102 |
| Douglas | 327 | 164 | 205 | 88 | 29 | 5 | 230 | 75 | 20 | 2 |

2,050 inspections and 1,710 restaurants change. 1,860 inspection scores go
up and 150 go down (a keyword tier can be too soft as well as too harsh);
the rest only change the tier recorded on a line. 197 of the 1,253 embedded
`data.js` records of these counties moved with them.

## Rules the regrade kept to

- A score is recomputed (100 - 5P - 2PF - C) only where the stored score is
  exactly what the stored tiers give. That held for every stored inspection
  with lines (8,008 of 8,008 under the 20-line cap); the one inspection at
  the cap had no line to move.
- The weighted score is rebuilt from as many of the newest inspections as
  reproduce the stored one (the run that wrote the row saw one, two or
  three). Every changed restaurant's stored weighted score matched one of
  them.
- A text-forced F is not re-decided: the text did not change. Where the
  stored F was explained by the old scores and the new scores are not an F,
  the pest and sewage decision is needed. 75 such restaurants have a stored
  line cut at 400 characters (El Paso County's lines open with the code's
  wording, so the observation is what is cut). Those are answered from the
  chips and summaries the original run wrote from the full text: 68 name no
  pest or sewage finding and follow their new scores, 2 carry a roach or
  sewage summary and stay F, 5 are undecidable and stay F.
- Chips and summaries do not move.
- Both stored Closed inspections (Arapahoe) already scored under the cap of
  65; nothing changed for them.

## Checks

- The seed was applied to a SQLite copy of the exported rows with the D1
  schema: 3,760 rows change on the first load, a second load leaves the
  state identical, the grade counts equal the table above, and afterwards
  every inspection's stored score again equals the score its stored tiers
  give.
- Every UPDATE is guarded (restaurants by id, inspection date and
  `updated_at`; inspections by id and the stored score), so a row the weekly
  run rewrites before the seed loads is left alone.
- `tests/test_co_regrade.py` covers the rules above, the guards and the
  `data.js` pass.

## Found on the way, not changed here

El Paso County, Adams, Weld and Larimer publish child care, body art, waste
tyre and pool inspections on the same portal slugs, and 945 of those places
are stored as restaurants (601 child care, 173 body art, 162 waste tyre
generators such as car dealers, 9 splash pads); 134 are in `data.js`. The
configs are `programs: ['Retail Food']` from this date, so no more arrive.
Removing the stored ones is a DELETE and waits for the owner.

## Re-running it

    # 1. export-d1.yml with sources = the six labels separated by "|"
    python3 scripts/co_regrade.py --export d1_export.jsonl.gz --report-dir out      # dry run
    python3 scripts/co_regrade.py --export d1_export.jsonl.gz --report-dir out \
        --seed data/d1_seed_<date>_co_frontrange_regrade.sql.gz \
        --data-js public/data.js --write-data-js
