# MyHealthDepartment: the directory probe and fourteen new jurisdictions

2026-11-22. The portal's root page (`https://inspections.myhealthdepartment.com/`)
lists every tenant: 341 slugs with the agency's name and address. Every slug
the pipeline did not already read (262, leaving out staging, test, demo and
tobacco tenants) was sent one sorted search for 2026-11-16..19; 136 answered
rows. That list, not a web search, is where Montana, Tyler, St. Louis and the
Colorado state program were found.

## What was added

All fourteen run in the `MHD metros (midweek)` fetch group (the Wednesday
cron). Per jurisdiction: four weekdays read in full with the production
query (single-day windows sorted on inspectionID), a six-week vocabulary read
(one row per permit, up to the 225-row cap), and eight detail pages through
`_mhd_parse_detail_html`.

| key | CLI slug | agency | score | rows a day (4 weekdays) | what ships |
|---|---|---|---|---|---|
| `montana` | `montana` | Montana DPHHS, statewide | none (0 or null) | 31, 43, 53, 56 (24-38 retail food) | RETAIL FOOD, purpose Routine or Follow-up, with the clean proof |
| `aaphd` | `amarillo` | City of Amarillo (Potter, Randall) | official 0-100 (71-100) | 12, 14, 15, 15 | programName Food Hygiene, inspectionType GENERAL FOOD INSPECTION |
| `net` | `tyler` | Northeast Texas Public Health District (Smith County) | Texas demerit total (0-22) | 2, 4, 9, 9 | programName Food |
| `orangecotx` | `orangecountytx` | Orange County, Texas | official 0-100 as a string (61-100) | 0, 1, 2, 6 | programName Food |
| `cheyenne-laramie` | `cheyenne`, `wyoming` | Cheyenne-Laramie County Health | none | 9, 10, 11, 12 | programName Food, the item-999 comment line removed |
| `casper-natrona` | `casper`, `wyoming` | Casper-Natrona County Health | none | 1, 2, 2, 5 | programName Food |
| `tetonwy` | `jacksonhole`, `wyoming` | Teton County WY | none | 0, 0, 1, 4 | programName Food |
| `firstdistrict` | `minot` | First District Health Unit, ND | official 0-100 (96-100) | 2, 4, 6, 11 | inspectionType FOOD ESTABLISHMENT under three programs |
| `mo-stlouis` | `stlouis` | City of St. Louis | official 0-100 (83-100) plus the city's letter | 10, 14, 17, 21 | programName Food, one city spelling |
| `mo-jackson` | `jacksoncountymo` | Jackson County Environmental Health, MO | none (0) | 10, 16, 27, 29 | programName Food Establishment |
| `colorado` | `coloradostate` | Colorado CDPHE (counties without their own program) | risk-index demerits (0-117) | 25, 26, 27, 32 | programName Retail Food |
| `pueblo` | `pueblo` | Pueblo Dept. of Public Health and Environment | risk-index demerits (0-52) | 9, 10, 10, 24 | programName Food |
| `mesacountyco` | `grandjunction` | Mesa County Public Health | risk-index demerits (0-74) | 2, 4, 5, 7 | programName Retail Food |
| `broomfield` | `broomfield` | City and County of Broomfield | risk-index demerits | 0, 0, 1, 2 | programName Retail Food |

Shared rules for these entries: mobile units are dropped by permit type
(`MHD_MOBILE_PERMIT_RE`; a Mesa County taco truck's address is "Mesa
County"), pre-opening visits are dropped by purpose
(`MHD_PREOPENING_PURPOSE_RE`: Pre-opening, Pre-Operational, Pre-Op, Opening),
no metro chip.

## Machinery added for them

- `inspection_types`: allowlist on the row's inspectionType. Amarillo's
  VENDED WATER/ICE (27-30) and FARMERS MARKET (17) inspections share the food
  program on other scales and would grade F. Minot's FOOD ESTABLISHMENT form
  is used under three programs (restaurants, school and child-care kitchens,
  lodging) while SCHOOL INSPECTION, FOOD SAMPLER and FOOD TEMP/MOBILE are
  other forms.
- `purpose_allow`: allowlist on the row's purpose, read by
  `_mhd_row_purpose` (`purpose`, or the first non-empty `purpose*` field:
  NET Health sends `purpose_retail_food`, Amarillo `purpose1`/`purpose2`).
- `city_map`: one spelling per town. St. Louis arrives as SAINT LOUIS,
  St Louis and St. Louis; Lee's Summit as Lees Summit and Lee's Summit. The
  restaurant id hashes the city, and the city index is keyed on it.
- `observation_exclude`: a regex of observation lines that are not
  violations. Every Cheyenne report ends in an item 999 line of general
  comments ("Proper hot holding ... Facility is clean and organized").
- score scale `tx-demerit`: the Texas form's demerit total, shipped as
  `risk_score = 100 - demerits` and trusted through the detail pass
  (`MHD_TRUSTED_SCORE_SCALES`), `original_score` the demerit total. Checked
  on Smith County: in 22 of 22 inspections the demerits fall between the
  number of distinct cited items and three times that number.
- `clean_proof: 'printable-grid'` (Montana), below.
- A published score of 0 on a 0-100 scale is "not scored": the row is graded
  from its observations and left out when it has none. Found in the Orange
  County TX backfill (7 of 50 rows: every `Visit`, one Routine, one
  Follow-Up, none with an observation, each an F at risk 0) and already
  present in production for Portland-metro lodging visits.
- `results` now also reads the Colorado verdict from `scoreDisplay` (Pass,
  Re-Inspection Required, Closed) where a portal sends it there.

## Montana

The state portal is new: 2 rows on a March 2026 day, 9 in June, 40 in
August, 57 in September. Three things on it are not inspection reports.

1. **Purpose `Other`**: 40 to 55 percent of the retail-food rows. Licence
   notes ("did not operate this year", "changed ownership") and the
   placeholders of counties that keep their inspections in their own
   systems. 21 of 21 sampled across June, August and September, and every
   Missoula, Bozeman, Kalispell, Hamilton, Red Lodge and Lewistown row seen,
   had an empty observations block; the two original PDFs read were empty
   forms; one comment says "Inspection completed in Paragon". Not shipped
   (`purpose_allow`). Missoula, Gallatin and Flathead counties are covered by
   the Paragon source instead; Ravalli, Carbon and Fergus publish nothing.
2. **`Routine` rows that only log a paper report.** 21 of the first 25
   Routine rows of 2026 (February to August) had no observations. Of the
   three whose original PDF could be read, one (Stillwater County, June) was
   a real clean inspection with its compliance grid filled in (29 IN / N/A /
   N/O marks read by the pipeline's PDF reader) and two (Billings, February
   and June) were empty forms: 0 marks. A cited inspection (Helena, August)
   also shows 29. So a zero-observation Montana inspection ships only when
   its printable shows the grid (`_mhd_prove_clean`, at least
   `MHD_GRID_MIN_MARKS` = 10 marks); without a grid it is dropped for good;
   an unread printable drops and holds. The portal answered "error
   retrieving printable" for 2 of 6 requests in that probe.
3. **Pre-opening** visits, dropped like everywhere else in this group.

Observation lines read `NN: <observation> Corrective Actions: <Food Code
section>: <its text>`. The section text after a pest item ("The premises
shall be maintained free of insects, rodents, and other pests ...") is the
generic list the text-F rules already remove; a test pins that the rule text
alone never forces an F and that named droppings before it still do.

### Backfill, 2026-08-01 to 2026-11-21 (run 2026-11-23)

- 794 Routine and Follow-up retail-food inspections after the filters (1,315
  rows dropped by program, 1,671 by purpose, 69 mobile units).
- Detail pages: 794 of 794 read across three ban windows (426, 308, 60).
- 358 had no observations. Their printables: 342 showed the grid and ship
  clean, 11 had none (logged paper reports, dropped for good), 5 could not be
  read (dropped and held; the one from August was recorded as given up so
  the first weekly run starts at 2026-11-12, not in August). This phase took
  2 h 18 min, about 23 s a report: a normal week has ~35 of them.
- Shipped: 778 inspections, 754 restaurants in 110 towns (Billings 179,
  Great Falls 61, Helena 56, Butte 54, Havre 40, Livingston 35). 44% clean.
  By month: August 66, September 181, October 312, November 219.
- Restaurant grades: A 632, B 81, C 26, F 15. Eleven F's come from the
  score. Four are text-forced, every one recorded mouse or rodent droppings
  (Gardiner, Livingston, Libby, Ronan). The violation reader cleared the one
  F only the regex gave (rat traps stored next to food) and added one the
  regex missed ("Mouse poop observed").
- 90 inspections have no coordinates (the Census geocoder did not place
  them and the second geocoding pass, `scripts/backfill/geocode_backlog.py`,
  was not run in this session). They are in D1 and searchable, not on the
  map, until that pass runs or the place is inspected again.

## Colorado: scored on the state's tier

Found in the first backfill, before anything shipped. Colorado's form cites
violations from one statewide library, and CDPHE's template prints each
entry's designation in the line:

    38: Insects, rodents and animals not present . This is a Core item
    6-501.111(A-B), and (D) - Controlling Pests (C) Pests were present ...

The detail pass scored every MHD line on keywords (`classify_violation`, the
method for portals that state nothing) and ignored the phrase. Over CDPHE's
3,239 lines of 2026-09-21..11-21:

| the state says | lines | keyword tier: priority | priority foundation | core |
|---|---|---|---|---|
| Priority | 1,161 | 1,044 | 69 | 48 |
| Priority Foundation | 1,238 | 894 | 168 | 176 |
| Core | 836 | 405 | 368 | 63 |

(4 lines, the item-57 imminent-hazard closure, state no tier.) The
inspection grades that came out of it:

| CDPHE inspections | A | B | C | F | F on a state verdict of Pass |
|---|---|---|---|---|---|
| keyword tiers (first read, 806) | 341 | 216 | 130 | 119 | 61 of 729 |
| the state's tiers (re-read, 807) | 444 | 236 | 97 | 30 | 6 of 730 |

The re-read returned the same lines for all 806 inspections of the first
read; only the tiers moved (priority 2,345 to 1,164, priority foundation 605
to 1,239, core 289 to 838). 45 inspections score lower than before: a
keyword tier can be too soft as well as too harsh.

`'tiers': 'colorado'` on a config now makes `_co_line_tier` read the printed
designation. Pueblo, Mesa County and Broomfield use the same library and
print the section without it (`38: 6-501.111(A-B), and (D) Pests were
present ...`), so their lines take the tier CDPHE's lines state for that
section: `CO_SECTION_TIERS`, 184 sections read off those 3,235 lines, none
stated with two tiers. A line with neither a stated tier nor a known section
keeps the keyword tier, and the fetch log counts them.

| portal | lines | scored on the state's tier | inspection F's before | after |
|---|---|---|---|---|
| Pueblo County | 641 | 627 (98%) | 16 of 243 | 1 |
| Mesa County | 311 | 309 (99%) | 7 of 123 | 3 |
| Broomfield | 221 | 69 (31%) | 14 of 38 | 10 |

Broomfield's inspectors mostly write the item number and their own sentence
(`37: No method of notification of major food allergens ...`), so two thirds
of its lines stay keyword-tiered and its grades stay the harshest of the
group (9 of 32 restaurants an F, four of them real roach or dropping
findings). The form's item number does not settle the tier: only 14 of the
52 items CDPHE cited were always one tier.

Not changed: the six Front Range counties (Arapahoe, Douglas, El Paso,
Larimer, Weld, Adams). They print the same sections with no designation and
are still keyword-scored, stored history included. Moving them is a regrade
from a D1 export, and worth doing: the table would reach them as it reaches
Pueblo, and the keyword method gave CDPHE four times the F's the state's own
tiers do.

The state's verdict (Pass 0-49 demerits, Re-Inspection Required 50-109,
Closed 110+ or an imminent hazard) is shipped in `results` and does not cap
the grade. On the state's tiers a Closed inspection can still grade A (a
water-line break: one line). Whether Closed should force an F, as FAILED
does in Waco, is the owner's call.

## Backfill of the other thirteen, 2026-09-21 to 2026-11-21 (run 2026-11-23)

One pipeline run per jurisdiction in sequence (one portal, one ban budget),
every detail page read unless noted. Restaurant grades are after the
violation reader; "text F" counts restaurants whose newest inspection is an F
only because of a named pest or sewage finding.

| jurisdiction | inspections | restaurants | A | B | C | F | text F | no map pin |
|---|---|---|---|---|---|---|---|---|
| Amarillo | 567 | 492 | 405 | 66 | 12 | 9 | 9 | 49 |
| Tyler / Smith County | 213 | 176 | 160 | 11 | 2 | 3 | 3 | 18 |
| Orange County TX | 43 | 43 | 40 | 1 | 0 | 2 | 1 | 9 |
| St. Louis | 837 | 640 | 609 | 19 | 0 | 12 | 11 | 26 |
| Jackson County MO | 654 | 538 | 382 | 122 | 25 | 9 | 5 | 53 |
| Cheyenne, Casper, Jackson WY | 347 | 294 | 188 | 58 | 24 | 24 | 1 | 26 |
| Minot / First District ND | 112 | 107 | 94 | 9 | 3 | 1 | 0 | 20 |
| Pueblo County | 243 | 236 | 175 | 45 | 13 | 3 | 2 | 16 |
| Mesa County (Grand Junction) | 123 | 117 | 81 | 29 | 5 | 2 | 1 | 13 |
| Broomfield | 38 | 32 | 8 | 3 | 12 | 9 | 4 | 3 |
| Colorado CDPHE (state-inspected counties) | 807 | 769 | 423 | 235 | 79 | 32 | 12 | 111 |

Notes from reading them:

- **Text-forced F's are findings.** Every one in Amarillo, Tyler, St. Louis,
  Jackson County, Wyoming and the Colorado counties names live or dead
  roaches, mice or droppings. Orange County TX had 7 of 50 rows published at
  a score of 0 with nothing cited (the unscored-zero rule above); they do
  not ship.
- **Wyoming by county** (inspection grades): Cheyenne 85 / 34 / 7 / 4 of 130,
  Casper 92 / 19 / 7 / 7 of 125, Teton 35 / 13 / 15 / 29 of 92. Teton's
  inspectors cite a median of five items a visit, two thirds of them from
  the form's risk-factor section, and the F share is the same under
  form-section tiers (28) and when each form item counts once (26): it is
  the county's citing, not the keyword method. Teton's follow-up reports
  reprint the routine inspection's lines (Persephone 12 and 12, Hatch
  Taqueria 13 and 13) with a dated note on the corrected ones, so a
  follow-up grades like the visit it follows. One Teton detail page could
  not be read and was recorded as given up.
- **St. Louis** ships 837 rows as 663 inspections: stadium and arena stands
  share a name and an address, and one inspection per restaurant per day is
  kept (the worst).
- **Bans**: Amarillo one 15-minute window (456 pages in), St. Louis two,
  Jackson County two, CDPHE two; the small ones none. The four Colorado
  portals were read twice (the second time on the state's tiers); only the
  second read shipped.

## Jurisdictions that answered rows and were not added

One search each, 2026-11-16..19, first page only (25+ means the page was full).
The score column is a guess from that page's values, not a verified scale.
Already covered by another source: `san-francisco`, `sacramento`, `boston`
(body art only on the portal), `gwinnett-ga`.

| slug | agency | rows | programs | score on the page |
|---|---|---|---|---|
| `adams-wa` | Adams County Health Department | 8 | Food | demerit/other |
| `aph` | Austin Public Health | 25+ | Food | 0-100? |
| `appletondsps` | Appleton Health Department | 2 | Body Art | no score |
| `az-santa-cruz` | Santa Cruz County Health Services | 7 | Food, Pool/Spa | 0-100? |
| `az_yavapai` | Yavapai County Community Health Services | 25+ | Food, Recreational Water | no score |
| `beckleyraleigh` | Beckley-Raleigh County Health Department | 5 | Food | no score |
| `berkeley` | Berkeley-Morgan County Health Departments | 25+ | Body Art, Childcare, Food | no score |
| `bfhd` | Benton-Franklin Health District | 25+ | Food | demerit/other |
| `boston` | Boston Public Health Commission | 5 | Body Art, Nail Salons | no score |
| `brhdut` | Bear River Health Department | 25+ | Emissions, Food, Tanning | no score |
| `bureau-putnam-marshall` | Bureau, Putnam & Marshall County Health Dept. | 9 | Food | no score |
| `ca-lakecounty` | County of Lake Health Services Department | 2 | Food Protection | no score |
| `ca-san-luis-county` | COUNTY OF SAN LUIS OBISPO HEALTH AGENCY ENVIRONM | 25+ | Retail Food Safety | no score |
| `ca_tehama` | TEHAMA COUNTY ENVIRONMENTAL HEALTH | 8 | FOOD, PUBLIC WATER, UNDERGROUND STORAGE TANK | no score |
| `cabellhuntington` | Cabell-Huntington Health Department | 25+ | Childcare, Food, Recreational Water | no score |
| `chd` | Chatham Health District | 10 | Retail Food, Salon | no score |
| `christian` | Christian County Health Department | 6 | Food | 0-100? |
| `clallam` | Clallam County Environmental Health Services | 16 | Food, Recreational Water | demerit/other |
| `dkchd` | DeKalb County Health Department | 14 | Food | no score |
| `douglasdsps` | Douglas County DSPS | 1 | Body Art | no score |
| `fayette` | Fayette County Health Department | 25+ | Food, Hotel | no score |
| `ffhd` | Fairfield County Health Department | 7 | Food | no score |
| `fulton` | Fulton County Health Department | 5 | Food | no score |
| `georgia-georgia-department-of-public-health` | Georgia Department of Public Health-Staging | 11 | Food, Pools, Tourist | 0-100? |
| `governmentofnwt` | Government of Northwest Territories - Environmen | 25+ | Drinking Water, Food, General Sanitation, Recreational Water | no score |
| `grant-county` | Grant County Health District | 21 | Food, Recreational Water | 0-100? |
| `graysharbor` | Grays Harbor County | 25+ | Food | demerit/other |
| `greenbrier` | Greenbrier County Health Department | 14 | Food, Hotel | no score |
| `gwinnett-ga` | GNR Public Health | 43 | Body Art, Food, Tourist | 0-100? |
| `hancock` | Hancock County Health Department | 16 | Food | no score |
| `harrisonclarksburg` | Harrison-Clarksburg Health Department | 15 | Food, Hotel, Manufactured Home Community, School | no score |
| `harrisoncountyph` | Harrison County Public Health | 4 | Food | no score |
| `il-clinton-county` | Clinton County Health Department | 4 | Food | no score |
| `il-hancock` | Hancock County Health Department | 2 | Food | no score |
| `il-jackson` | Jackson County Health Department | 18 | Food | no score |
| `il-montgomery-county` | Montgomery County Health Department | 1 | Food | no score |
| `il-msh-county` | McDonough, Schuyler and Henderson Health Departm | 23 | McDonough - Food | no score |
| `il-pike-county` | Pike County Health Department | 4 | Food | no score |
| `il_oakparkdph` | Oak Park Public Health Department | 3 | Retail Food | no score |
| `island-health` | Island Health | 25+ | Community Care, Drinking Water, Food | no score |
| `jackson_wv` | Jackson County Health Department | 2 | Food | no score |
| `jefferson_wv` | Jefferson County Health Department | 10 | Childcare, Food, Hotel | no score |
| `jhd` | City of Joplin Health Department | 17 | Food | no score |
| `johnsoncounty` | Johnson County Community Health Services | 13 | Food | 0-100? |
| `kanawhacharleston` | Kanawha-Charleston Health Department | 25+ | Body Art, Food, Recreational Water | no score |
| `knoxpublichealth` | Knox Public Health | 19 | Food | no score |
| `lchd` | Licking County Health Department | 25+ | Food | no score |
| `ldchealth` | Lawrence-Douglas County Public Health | 8 | Cereal Malt Beverage Retailer | no score |
| `lewis` | Lewis County Health Department | 11 | Food | no score |
| `maine_department_of_agriculture` | Maine Department of Agriculture, Conservation &  | 9 | Feed, Seed & Fertilizer, Food & Fuel | no score |
| `marathondsps` | Marathon County Health Department | 1 | Body Art | no score |
| `marin-county` | County of Marin, CA | 25+ | Body Art, Food, Housing, Medical Waste | no score |
| `marion` | Marion County Health Department | 2 | Food | no score |
| `marionpublichealth` | Marion Public Health | 6 | Food | no score |
| `mchd_wv` | Monroe County Health Department | 3 | Campground, Food | no score |
| `mercer` | Mercer County Health Department | 7 | Food, Hotel | no score |
| `midohiovalley` | Mid-Ohio Valley Health Department | 25+ | Childcare, Food, Hotel | no score |
| `mo-cass-county` | Cass County Health Department | 17 | Food | no score |
| `monongalia` | Monongalia County Health Department | 25+ | Body Art, Food, Recreational Water, Septic Removal | no score |
| `morgan` | Berkeley-Morgan County Health Departments | 8 | Food | no score |
| `nd-north-dakota-department-of-health-and-human-services-ndhhs` | North Dakota Department of Health and Human Serv | 1 | Parks | no score |
| `oh-union-county-health-department` | Union County Health Department | 6 | Food | no score |
| `oh_allen` | Allen County Public Health | 19 | Food, Recreational Water | no score |
| `oh_athens` | Athens City-County Health Department | 17 | Food, Recreational Water | no score |
| `oh_butler` | Butler County General Health District | 25+ | Food, Recreational Water | no score |
| `oh_canton` | Canton City Public Health Department | 20 | Food | no score |
| `oh_carroll` | Carroll County General Health District | 6 | Food | no score |
| `oh_clermont` | Clermont County Public Health | 25+ | Food, Recreational Water | no score |
| `oh_clinton` | Clinton County Health District | 8 | Food | no score |
| `oh_coshocton` | Coshocton Public Health District | 4 | Food | no score |
| `oh_delaware` | Delaware Public Health District | 25+ | Body Art, Food | no score |
| `oh_galioncity` | Galion City Health Department | 7 | Food | no score |
| `oh_geauga` | Geauga Public Health | 3 | Campground, Food, Recreational Water | no score |
| `oh_guernsey` | Cambridge-Guernsey County Health Department | 5 | Food | no score |
| `oh_hamilton` | Hamilton County Public Health | 25+ | Food | no score |
| `oh_hancock` | Hancock Public Health | 16 | Food, Recreational Water | no score |
| `oh_huron` | Huron County Public Health | 16 | Food | no score |
| `oh_lake` | Lake County General Health District | 5 | Campground, Food, Recreational Water | no score |
| `oh_lawrence` | Lawrence County Health Department | 18 | Food | no score |
| `oh_lorain` | Lorian County Public Health | 25+ | Food | no score |
| `oh_mariettabelpre` | Marietta/Belpre Health Department | 14 | Food | no score |
| `oh_massillon` | Massillon Health Department | 1 | Food | no score |
| `oh_medina` | Medina County | 24 | Body Art, Food | no score |
| `oh_miami` | Miami County Public Health | 25+ | Food | no score |
| `oh_middletown` | City of Middletown Health Department | 8 | Food | no score |
| `oh_morrow` | Morrow County Health District | 3 | Food | no score |
| `oh_pickaway` | Pickaway County Public Health | 15 | Food | no score |
| `oh_portage` | Portage County Health District | 6 | Campground, Food | no score |
| `oh_portsmouth` | Portsmouth City Health Department | 2 | Food | no score |
| `oh_rchd` | Ross County Health District | 11 | Food | no score |
| `oh_richland` | Richland County Public Health | 25+ | Food, Recreational Water | no score |
| `oh_seneca` | Seneca County General Health District | 10 | Food | no score |
| `oh_stark` | Stark County Health Department | 25+ | Food | no score |
| `oh_toledolucas` | Toledo Lucas County Health Department | 25+ | Body Art, Food, Recreational Water | no score |
| `oh_tuscarawas` | Tuscarawas County Health Department | 13 | Body Art, Food | no score |
| `oh_wood` | Wood County Health Department | 10 | Food | no score |
| `oh_zanesvillemuskingum` | Zanesville-Muskingum County Health Department | 1 | Food | no score |
| `pchd` | Placer County Environmental Health | 25+ | Body Art , Pool, Retail Food | no score |
| `putnam` | Putnam County Health Department | 19 | Food | no score |
| `sacramento` | Sacramento County Environmental Management Depar | 25+ | BODY ART, FOOD PROTECTION | no score |
| `san-francisco` | San Francisco Department of Public Health, Envir | 25+ | Food | no score |
| `scdoh` | Somerset County Department of Health | 20 | Retail Food | 0-100? |
| `scph` | County of Summit, OH | 25+ | Food, Recreational Water | no score |
| `soh` | State of Hawaii, Department of Health | 25+ | Food | no score |
| `st-clair-county` | St. Clair County Health Department | 25+ | Food | no score |
| `taneycounty` | Taney County Health Department | 18 | Food, General, Hotel | no score |
| `tx_jaspernewton` | Jasper Newton County Public Health District | 2 | Food | no score |
| `ucnc` | Union County Environmental Health | 25+ | Child Care, Food Service, Institutions, Tattoo | 0-100? |
| `uhd` | Uncas Health District | 1 | Salon | no score |
| `utah-centralutah-county` | Central Utah Health Department | 6 | Food, Recreational Water | 0-100? |
| `wa-columbiacounty` | Columbia County Public Health | 25+ | Food | no score |
| `wa-okanogan` | Okanogan County Public Health District | 9 |  | demerit/other |
| `wa-wallawalla-county` | Walla Walla County Department of Community Healt | 6 | Food | demerit/other |
| `wa_islandcounty` | Island County Health Department | 13 | Food | demerit/other |
| `wadoh` | Washington State Department of Health | 25+ | Temporary Worker Housing, Transient Accommodations | no score |
| `wisconsin-datcp` | Wisconsin Department of Agriculture, Trade and C | 25+ | Food, Lodging, Recreational Water | no score |
| `wisconsin-monroe` | Monroe County Health Department | 11 | Food, Recreational Water | no score |
| `wv-hampshire-county` | HAMPSHIRE COUNTY HEALTH DEPARTMENT | 1 | Food | no score |
| `wv-mineral` | Mineral County Health Department | 13 | Food | no score |
| `wv-wayne` | Wayne County Health Department | 9 | Food, Recreational Water | no score |
| `wv-wheeling-ohio` | Wheeling-Ohio County Health Department | 11 | Body Art, Childcare, Food, Hotel | no score |
| `wyandotcounty` | Wyandot County | 7 | Food | no score |

Answered no rows for that window (empty, retired or not publishing): `anaheim-fire-and-rescue`, `apache-az`, `arizona`, `az-lapaz-county`, `bhsj`, `boone`, `cchd`, `cityofracinedsps`, `co-douglas-county`, `ctnedoh`, `davidson-metro`, `dsps`, `eauclairecountydsps`, `evanston`, `ga-clayton-county-health-district`, `ga-coastal-health-district`, `ga-cobb-douglas-health-district`, `ga-dekalb-health-district`, `ga-district-4-health-district`, `ga-east-central-health-district`, `ga-fulton-county-board-of-health`, `ga-georgia-department-of-public-health`, `ga-north-central-health-district`, `ga-north-georgia-health-district`, `ga-north-health-district`, `ga-northeast-health-district`, `ga-northwest-georgia-health-district`, `ga-south-central-health-district`, `ga-south-health-district`, `ga-southeast-health-district`, `ga-southwest-health-district`, `ga-west-central-health-district`, `gbph`, `gdph`, `gnrpublichealth`, `henry-stark`, `hpeph`, `il-lasalle-county`, `il-ogle-county`, `il_kankakee`, `jacksoncountyhd`, `knox`, `lacrossecountydsps`, `lenawee-mi`, `lgldhu`, `llcthd`, `lmasdhd`, `long-beach`, `mcs`, `mecklenburg`, `mecknc`, `mo-polk`, `morgancountyhd`, `mptx`, `myhealthdepartment.com/supportforum`, `nashua-nh`, `navajo`, `nc-transylvania`, `nicholas`, `northeast-colorado`, `oh_ashtabula`, `oh_brown`, `oh_cleveland`, `oh_gallia`, `oh_henry`, `oh_highland`, `oh_holms`, `oh_madison`, `oh_mahoning`, `oh_monroe`, `oh_perry`, `oh_putnam`, `oh_shelby`, `oh_vanwert`, `oh_wayne`, `or-baker-county`, `or-benton-county`, `or-clatsop-county`, `or-columbia-county`, `or-coos-county`, `or-crook-county`, `or-curry-county`, `or-deschutes-county`, `or-douglas-county`, `or-grant-county`, `or-harney-county`, `or-hood-river-county`, `or-jackson-county`, `or-jefferson-county`, `or-josephine-county`, `or-klamath-county`, `or-lake-county`, `or-lane-county`, `or-lincoln-county`, `or-linn-county`, `or-malheur-county`, `or-marion-county`, `or-north-central-public-health`, `or-polk-county`, `or-tillamook-county`, `or-umatilla-morrow-county`, `or-union-county`, `or-wallowa-county`, `or-wheeler-county`, `or-yamhill-county`, `oregon`, `phs`, `polkcountymo`, `portage-ohio`, `riverside-county-ca`, `rockcountydsps`, `santa-cruz-ca`, `sheboygandsps`, `sonoma`, `tda`, `tx-dallas-county`, `tx_southplains`, `ut-saltlake-county`, `va-fairfax`, `vermilion-county`, `virginia`, `walla-walla`, `wchd`, `wi_outagamiedsps`, `wv-grafton-taylor`, `wv-hardy`.
