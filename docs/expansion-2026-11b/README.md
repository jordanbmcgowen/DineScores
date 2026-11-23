# Expansion, 2026-11-22: Texas, Montana and the states around them

A second live survey, three weeks after `docs/expansion-2026-11/`. It
started from the MyHealthDepartment portal's own directory (the portal root
lists every tenant, 341 of them) and from five regional surveys of Texas and
Montana health departments that are not on that portal.

Files here:

- `mhd.md`: the portal directory probe, the fourteen jurisdictions added
  from it and what was measured for each.
- `survey-west.md`, `survey-gulf.md`, `survey-central.md`, `survey-dfw.md`,
  `survey-montana.md`: the regional surveys (endpoints, sample requests,
  verdicts). Contact addresses and phone numbers are removed.
- one file per source built outside the portal (`paragon.md`, `elpaso.md`,
  `waco.md`, `corpuschristi.md`, `garland.md`, `abilene.md`,
  `wichitafalls.md`).

## What the survey found, by people covered per unit of work

| Source | People | Platform | Published | Verdict |
|---|---|---|---|---|
| Montana statewide (Billings, Helena, Great Falls, Butte and the small counties) | ~700k | MyHealthDepartment `montana` | observations, no score | built (portal config) |
| El Paso city and county | ~866k | city form, one POST per window | official 0-100, no violations | built |
| Kansas City, MO | ~510k | Paragon Inspections Online | Critical / Non-critical findings, observations, no score | built |
| Missoula, Gallatin (Bozeman), Flathead (Kalispell) counties | ~355k | Paragon Inspections Online | observations with two tiers; Flathead a letter grade | built |
| Corpus Christi / Nueces County | ~353k | InspectHub | 100 minus demerits, item points, observations | built |
| St. Louis (city) | ~280k | MyHealthDepartment `mo-stlouis` | official 0-100, observations | built (portal config) |
| Jackson County MO (Lee's Summit, Blue Springs, Raytown, Grandview) | ~300k | MyHealthDepartment `mo-jackson` | observations, no score | built (portal config) |
| Waco / McLennan County | ~270k | ArcGIS Feature Service | demerits, no violations | built |
| Amarillo (Potter and Randall counties) | ~265k | MyHealthDepartment `aaphd` | official 0-100, observations | built (portal config) |
| Garland | ~246k | city ArcGIS MapServer | official 0-100, item points, observations | built |
| Tyler / Smith County | ~245k | MyHealthDepartment `net` | Texas demerit total, observations | built (portal config) |
| Colorado mountain and plains counties (Vail, Aspen, Breckenridge, Durango, Steamboat) | not counted | MyHealthDepartment `colorado` | risk-index demerits, observations | built (portal config) |
| Pueblo, Mesa (Grand Junction), Broomfield | ~400k | MyHealthDepartment | risk-index demerits, observations | built (portal config) |
| Abilene / Taylor County | ~143k | Tyler EnerGov civic API | official score, tiers, observations | built |
| Wichita Falls | ~130k | Hedgerow portal | official 0-100, tiers, observations | built |
| Cheyenne, Casper, Jackson WY | ~200k | MyHealthDepartment | observations, no score | built (portal config) |
| Minot and north-central North Dakota | ~95k | MyHealthDepartment `firstdistrict` | official 0-100, observations | built (portal config) |
| Orange County, Texas | ~85k | MyHealthDepartment `orangecotx` | official 0-100, observations | built (portal config) |

Populations are round figures from memory of census counts, not re-checked.
Whether each "built" row above shipped, and with what numbers, is in its own
file; this table is the survey's ranking, written before the builds finished.

## Reachable from GitHub's runners

The builds ran from a cloud container, so the weekly job's own network was
checked separately: `probe-sources.yml`, run 45194249225 on 2026-11-23.
El Paso (the form page, behind Cloudflare), Waco, Garland, Corpus Christi,
Abilene, Wichita Falls and Paragon all answered 200 from the ubuntu and
macOS runners. The MyHealthDepartment portal refuses runners as it always
has (403) and answers through the site's relay; a Montana printable that
the portal took 23 seconds to build came back through the relay as a PDF.
The probe loads El Paso's form page, not a search POST, so the first Sunday
run is the real test of that one.

## Surveyed and not built

Worth building next, roughly in order:

- **Austin on the portal.** Austin Public Health now publishes on
  MyHealthDepartment (`aph`): official 0-100 scores and observation text,
  current to within days, where the Socrata dataset DineScores reads runs six
  weeks behind and has scores only. It is not a config entry: the portal's
  names and addresses hash to different restaurant ids than the stored ones
  (1 id match in 74 rows; the stored addresses carry "Austin, TX 78749-1473"
  inside the street line), so a switch needs an id map built from a D1 export
  first, the way King County's was.
- **Hawaii statewide** (1.4M): MyHealthDepartment `soh`, 56-76 rows a day,
  no score, observations with HAR section numbers. Needs a Hawaii box in
  `coords_plausible` (everything outside the continental US is rejected
  today) and a look at its `Other` and `Complaint` purposes.
- **Galveston County Health District** (351k): Salesforce guest endpoint,
  official 0-100, no violations; only 10 inspections were dated after
  2026-10-02, so posting looks batched or stalled. Watch for a few weeks.
- **Bell County** (Killeen, Temple; 400k): weekly PDFs with scores, item
  points and narrative, posted three to six weeks late; needs a PDF layout
  parser.
- **Brazos County** (Bryan, College Station; 250k): Inspect2Go PDFs, page 1
  of the Texas form only, no date filter (~1,560 list calls a week).
- **Beaumont** (115k): ArcGIS layer plus a Crystal Reports PDF per
  inspection; the layer has no address.
- **New Braunfels** (116k), **Seguin** (39k), **Nacogdoches** (32k),
  **Rowlett** (62k): small score-only sources (ArcGIS snapshot, ArcGIS,
  Google Sheet, Google Sheet).
- **Little Elm, Sachse, Pilot Point, Justin**: one vendor (InstantInspector),
  plain HTML reports with all 47 Texas items; no date filter (~410 report
  pages a week). Its robots.txt asks AI crawlers to stay out, so sending its
  text to the violation reader is an owner decision.
- **More MyHealthDepartment tenants** that answer live rows (see `mhd.md`
  for the full list): Hamilton County OH (Cincinnati's suburbs), Summit,
  Stark, Lorain, Butler and thirty other Ohio counties, Benton-Franklin WA
  (Tri-Cities), Yavapai AZ, Marin and San Luis Obispo CA, Union County NC,
  St. Clair County IL, Somerset County NJ, the West Virginia counties,
  Wisconsin DATCP.

Blocked, stale or nothing online: Laredo (portal frozen at 2026-01-30 and
its inspection call returns 500), Odessa / Ector County (vendor host gone),
Brownsville and Cameron County, McAllen and Hidalgo County, Midland,
Montgomery County TX, Brazoria County, Pasadena TX, Baytown, Port Arthur,
Missouri City (bot challenge), San Marcos / Hays County (login), Texarkana,
Texas DSHS (publishes nothing for the areas it inspects, which include
Comal, Guadalupe, Denton, Rockwall, Ellis, Kaufman, Johnson and Parker
county programs), Irving and Carrollton (bot challenges), Grand Prairie
(letters only, stale), Allen, Mansfield, Euless, Addison, Coppell, Cedar
Hill, DeSoto, Duncanville, Lancaster, Highland Park; Ravalli, Carbon and
Fergus counties in Montana. The MyHealthDepartment slug `tx-dallas-county`
(Dallas County's contract cities) exists and has never published a row;
`ut-saltlake-county`, `oh_cleveland`, `riverside-county-ca`, `long-beach`,
`mecklenburg` and `davidson-metro` exist and are empty too.
