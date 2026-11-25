# Colorado Front Range regrade, 2026-11-25

The six Front Range counties (El Paso, Larimer, Adams, Weld, Arapahoe,
Douglas) were scored on keywords: each violation line was given a tier by
the words in it. On the state's own portal that method produced four times
the F's the state's printed tiers do (`docs/expansion-2026-11b/mhd.md`).
This regrade moves the rows already stored to the state's tier wherever a
line carries it. The fetcher reads lines the same way from this date on.

It ran in two passes on the same day. The first (below, "First pass") used
only tiers a line states, and fixed El Paso County and Douglas. The second
("Second pass") gave the lines that state nothing their form item's usual
tier, which is what moved Adams, Arapahoe, Larimer and Weld, and removed
the stored places that are not restaurants.

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

A line with none of these takes its form item's usual tier (second pass);
before that it kept its stored keyword tier.

## First pass: what a stated tier reaches

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
portal is 36%. There is no stated tier to read on those lines. (The first
pass stopped here, on the ground that the item number does not settle the
tier. Measured properly in the second pass, it nearly does.)

### First-pass result

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

## Second pass: the form item's usual tier

The four counties' lines open with the form's item number. Colorado's form
is one statewide form of 56 items, each covering a handful of library
provisions, so the item says most of what the tier is.

**The table.** Over the 12,970 lines of the ten Colorado portals that state
their tier, each item's most common tier (`CO_ITEM_TIERS`, 49 items: an item
needs four stated lines and a majority). 22 items carry one tier on every
line (cold holding is always Priority, wiping cloths always Core, the
certified manager always Core). The weakest are the split items: toxic
substances 62%, plumbing 61%, cooking temperature 59%, cooling methods 68%,
food condition 66%, shellfish records 69%. Item 29 (variance / HACCP: 40 /
38 / 22%) has no majority and stays on keywords.

**Tested held out.** Table built from CDPHE, Pueblo and Mesa County only,
then scored on the other counties' lines that state their tier:

| Scored on | Lines | Method | Tier right | Inspection letter right | Mean score error |
|---|---|---|---|---|---|
| El Paso County | 6,437 | keywords | 41% | 69% | 4.8 |
| | | violation reader's category | 42% | 71% | 4.4 |
| | | form item | 92% | 95% | 0.6 |
| Douglas | 736 | keywords | 37% | 68% | 3.7 |
| | | violation reader's category | 47% | 72% | 3.1 |
| | | form item | 92% | 95% | 0.6 |
| Adams | 863 | keywords | 38% | 59% | 5.6 |
| | | violation reader's category | 46% | 67% | 4.5 |
| | | form item | 90% | 93% | 0.9 |
| Arapahoe | 392 | keywords | 38% | 56% | 5.8 |
| | | violation reader's category | 56% | 70% | 4.0 |
| | | form item | 86% | 83% | 1.4 |

"Inspection letter right" is over inspections whose every line states its
tier (1,851 in El Paso County, 199 Douglas, 172 Adams, 70 Arapahoe): does
the method give the inspection the letter the state's tiers give it.
Larimer's 208 stated lines came out lower (form item 80%, keywords 37%), as
did Broomfield's 69 (81% and 55%).

**The violation reader does not fix this.** The route first proposed was to
let Jev's category decide each line's tier. On these lines its category is
confident and usually sensible, but `SEVERITY_MAP` gives a category one
tier, and this form does not work that way: "no certified food protection
manager" reads as employee training (Priority Foundation in the map) and is
Core on the form; "no paper towels at the hand sink" reads as personal
hygiene (Priority) and is Priority Foundation. It was not switched on.

**Against the counties' own numbers.** Each county publishes a risk-index
demerit total per inspection (Pass 0-49, Re-Inspection Required 50-109).
F's among inspections the county itself scored in its Pass band:

| County | Pass-band inspections | F before | F after |
|---|---|---|---|
| Adams | 1,270 | 99 | 27 |
| Arapahoe | 638 | 84 | 31 |
| Larimer | 1,578 | 65 | 8 |
| Weld | 1,154 | 23 | 0 |
| Broomfield | 27 | 4 | 0 |

The correlation between our score and the county's demerits is the same or
higher after (Adams 0.80 to 0.82, Larimer 0.78 to 0.80, Weld 0.82 to 0.86).

**Result.** Restaurants only, from the export of 2026-11-25 22:46 UTC, after
both seeds:

| Source | Restaurants | A | B | C | F | A after | B after | C after | F after |
|---|---|---|---|---|---|---|---|---|---|
| Adams | 1,439 | 732 | 399 | 197 | 111 | 833 | 427 | 141 | 38 |
| Arapahoe | 748 | 292 | 209 | 145 | 102 | 340 | 249 | 112 | 47 |
| Larimer | 1,696 | 906 | 509 | 209 | 72 | 1,168 | 431 | 71 | 26 |
| Weld | 1,122 | 687 | 305 | 104 | 26 | 797 | 277 | 36 | 12 |
| Douglas | 327 | 230 | 75 | 20 | 2 | 235 | 80 | 10 | 2 |
| Broomfield | 32 | 8 | 3 | 12 | 9 | 9 | 9 | 9 | 5 |
| El Paso, CDPHE, Pueblo, Mesa | 3,203 | 2,263 | 714 | 150 | 76 | 2,264 | 713 | 150 | 76 |
| All ten | 8,567 | 5,118 | 2,214 | 837 | 398 | 5,646 | 2,186 | 529 | 206 |

4,271 inspections and 3,309 restaurants change; 547 embedded records moved.
The two stored Closed inspections (Arapahoe) rose above 65 on the new tiers
and are held at the cap. Stored lines are now 39% Priority, 34% Priority
Foundation, 27% Core; the state's own mix is 36 / 38 / 26.

It is an estimate: about one line in ten gets the wrong tier, mostly on the
split items, in both directions.

## Second pass: the places that are not restaurants

El Paso County, Adams, Weld and Larimer publish child care, body art, waste
tyre and pool inspections on the same portal slugs, and 945 such places
were stored as restaurants: 601 child care, 173 body art, 162 waste tyre
generators (car dealers, tyre shops), 9 splash pads; 745 of them graded A,
30 graded F. `scripts/co_nonfood.py` deleted them on the owner's word.

- A place is deleted only if none of its stored inspections is a food
  inspection, and the DELETE checks that again in SQL.
- The stored inspection type names the program on three portals. Larimer
  leaves it empty on child-care rows. Each of its 220 untyped rows was
  looked up on the portal: 218 are Child Care, one is a Retail Food row of a
  portal test record (kept), one is no longer on the portal and reads as
  child care (toy sanitising, a first-aid kit).
- Two schools hold a cafeteria and a child-care licence at one address.
  They keep their rows; the child-care inspection is deleted and the row is
  rebuilt from the food inspection (one goes from A at 96 to B at 88).
- 134 of the 945 were in `data.js` and left it.

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

Both second-pass seeds were replayed together on a SQLite copy of the
22:46 export: 1,985 and 7,580 rows change, a second load leaves the state
identical, no non-restaurant and no orphaned inspection remains, and every
inspection's stored score equals the score its stored tiers give (8,374 of
8,374).

## Re-running it

    # 1. export-d1.yml with sources = the six labels separated by "|"
    python3 scripts/co_regrade.py --export d1_export.jsonl.gz --report-dir out      # dry run
    python3 scripts/co_regrade.py --export d1_export.jsonl.gz --report-dir out \
        --seed data/d1_seed_<date>_co_frontrange_regrade.sql.gz \
        --data-js public/data.js --write-data-js
