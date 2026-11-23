# Corpus Christi / Nueces County, TX (InspectHub)

Built 2026-11-22/23 from the gulf survey (`survey/gulf.md`, "Corpus Christi /
Nueces County"). CLI slug `corpuschristi`, fetch group `Corpus Christi`,
source label `Corpus Christi-Nueces County Public Health District`, source
ids `cctx_<inspection id>`. Fetcher `fetch_corpus_christi` in
`dinescores_pipeline.py`; tests `tests/test_corpus_christi.py` (61, no
network) with captures under `tests/fixtures/corpuschristi/`.

## The source

The Corpus Christi-Nueces County Public Health District publishes its food
inspections on InspectHub (`app.inspecthub.com`, ColdFusion, server-side
HTML; no script, cookie, token, login or challenge). The tenant key is in the
link on the city's "Restaurant Inspection Scores" page; a wrong key answers
HTTP 200 `{ 'ERROR':'Invalid Key' }`.

- `remote.cfm?k=<key>&s=100` is the whole listing: 2,203 rows in one 527 KB
  response, one per LOCATION (the `NAME - STREET` string; there is no
  facility or permit id), each its latest inspection: score, form name
  ("Type") and date. No paging, date filter or search parameter.
- `remote.cfm?k=<key>&s=100&i=<id>` is one inspection: name and street in the
  `<h3>`, `Date: November, 03 2026`, `Score : 89`, one row per cited item
  (points 3/2/1, `<item>. <title>`, the inspector's note, corrected-on-site
  flag) and "Other Inspections for <location>": every other inspection of the
  location, linked by id with its date and score (back to 2015).
- `s` is the score base: the page prints `s` minus the points deducted. The
  district's own link uses `s=100`.

A window costs the listing, one page per food location whose latest
inspection is in it, and one page per earlier in-window inspection named in
those pages' history tables. About 75 food inspections a week; one request
stream with a one-second pause (`CC_DELAY`).

## Rules chosen

- **Score.** The page prints the 0-100 number (100 minus demerits), so
  `original_score` and `risk_score` are both that number (Dallas's
  convention; McKinney's demerit-total `original_score` is for sources that
  publish the demerits). `results` carries the district's own band and the
  demerit total ("Good (11 demerits)": 0-10 Excellent, 11-20 Good, 21-30
  Acceptable, 31+ Poor). Each item's points are its tier: 3 priority, 2
  priority foundation, 1 core; the label is in jev_reader's
  `SOURCE_TIER_SOURCES` (official score, never re-tiered or re-scored).
- **Agreement.** Three statements of each score must agree or the inspection
  is dropped and held: the listing row (or the history row for an earlier
  visit), the page's Score line, and 100 minus the page's points. Measured:
  every itemised page agreed (58 of 58 in the first capture set, 94 of 94 in
  the 14-day run, 518 of 518 in the backfill). `CC_MISMATCH_MIN` (10)
  disagreeing pages making up more than 5% raise (McKinney's rule), since that
  means the page changed rather than one record being odd.
- **Clean visits.** A clean inspection's page prints `Score : 0` over an empty
  table (the history table and the listing say 100): 100 of 104 sampled
  clean-listed food pages. It ships as a 100 only when the listing / history
  row says 100; a rowless page whose listing says anything else is unread
  (held). A window of 20+ inspections that is 90%+ clean raises (about 45%
  are clean).
- **Shapes that are never shipped:**
  - An id the portal has not published: HTTP 200, the table shell, "This
    inspection does not have any violations.", no name (sometimes a date).
    Dropped and HELD (it is what a lost page looks like). Re-probed 119426,
    119429 and 119446 a day apart: still unpublished.
  - `CC_UNSCORED`: name and date match the listing, no Score line, the "no
    violations" sentence. Pools print this; so did 4 of 104 sampled
    clean-listed food pages (22 of 968 pages in the backfill), dated 2026-10-07 to 2026-11-20 (Glow & Go
    Nutrition, Rock's Discount Vitamins, Champs meat market, Gulf Coast
    Capital): a standing shape, not a publication lag, so it is skipped
    WITHOUT a hold (a hold would pin every weekly window for three runs), and
    its history table is still followed.
  - `CC_NO_DETAIL`: a Score line with "Violation information on this
    inspection is not available. However the recorded score is: [ 96 / 100 ]
    (4 demerits)." in place of the items (Cottens Catering, 2026-11-11, beside
    an itemised 96 the same day). Shipping it would show a 96 with "no
    violations"; skipped without a hold like `CC_UNSCORED`.
  - More than 25% of the pages read in those two shapes raises.
- **Scope.** The listing's "Type" is the form the inspection was written on;
  the source has no routine / follow-up / complaint / pre-opening field, so
  `inspection_type` is empty and a pre-opening visit cannot be told apart
  (follow-ups ship as their own inspections). Forms: `2024
  Inspections-Restaurants` (1,810 locations), `Restaurant` (46, pre-2024 form
  with the older TX item numbers) and `Inspection - Restaurant 2024` (1) ship;
  the pool, spa and splash-pad forms (336) and `Flea Market Food Vendors`
  (10: market stalls, push carts, event booths) do not. An unknown form name
  is not shipped and, from 20 locations, raises (the names carry years).
  By name: mobile units, food trucks/trailers, push carts, temporary events,
  farmers markets (`CC_SKIP_NAME_RE`; 'MOBILE HOME' is a trailer park's
  pool), and institutional kitchens with Georgia/Louisiana's
  `TP_INSTITUTION_RE` plus ISD / CISD, early childhood, juvenile justice,
  jr. high, senior citizen center and senior living (`CC_SCHOOL_DISTRICT_RE`):
  151 of the 1,857 food locations on 2026-11-23. Permit notes typed into the
  name between asterisks (`**90 DAY TEMPORARY PERMIT**`) are removed before
  the id is made; a name with `TEMP EVENT` in such a note is skipped.
- **City and zip.** Nothing in the source names the town: the listing, the
  page and the printable carry the street line only (no city, state, zip or
  coordinates), and the district also inspects Port Aransas, Robstown,
  Bishop, Banquete, Agua Dulce and Driscoll. Every record ships as
  `Corpus Christi`, `TX`, zip `''`, never guessed per record (the city is
  hashed into `make_restaurant_id`, so it must not depend on a geocoder).
  Port Aransas locations therefore carry the wrong city and mostly fail to
  geocode (see the numbers).
- **Pest and sewage titles.** Each item ships as `<form title>: <note>`. The
  pest item (`38. Insects, rodents, and animals not present`, the old form's
  `34. No Evidence of Insect contamination, rodent/other animals`) ships under
  the Hedgerow heading `Pests and animals: <note>`, the sewage item
  (`52. Sewage and waste water properly disposed`) under `Wastewater
  disposal: <note>`. "Gnats present" grades by score; "Rodent droppings found
  storage room" still forces an F through the note. A pest item cited with
  NO note keeps its printed title, which jev_reader already registers in
  `CITED_PEST_ITEM_TITLES` (pest chip, "Pests found", no F).
- **Freshness.** Inspection ids are issued on the inspection day, and a page
  was on the listing the day after; whether the district publishes or
  corrects inspections later was not measurable, so every dated window is
  read `CC_REREAD_DAYS` (7) earlier than asked (holds point at the start the
  caller asked for). An unread latest page holds the window start (its earlier
  in-window visits are unknown), an unread earlier visit its own day; the
  deadline and the breaker (`CC_BREAKER_TRIP` = 3 requests, or 3 non-pages, in
  a row) hold without counting and report 'skipped' / 'blocked'. A listing
  that is not the listing (fewer than 1,000 rows, a re-shaped row, mostly
  undated) raises and ships nothing.

## Measured

- Test mode (25 newest locations): 27 requests, 24 records (one location,
  Glow & Go, was the unscored shape).
- 14-day window (`--since-date 2026-11-09`, read from 2026-11-02): 173
  requests in 182 s, 169 inspections 2026-11-02..2026-11-21 from 172 pages
  (2 unscored, 1 score-without-items skipped; none dropped), 75 clean.
  Grades: A 161, B 5, F 3; scores 83-100. The three F's are text-forced,
  each a named finding: "Rodent droppings found storage room", "Rodent
  droppings in back prep kitchen area and in dinning room", "Rodent droppings
  in back storage by canisters". Census geocoded 126 of 169 (75%), all inside
  a Nueces County box; the misses are Port Aransas streets (Cotter, Alister,
  State Hwy 361, On The Beach: wrong city), `SPID` / `S.P.I.D.` abbreviations,
  `Mcardle St.` for McArdle Rd, and suite-laden Padre Island addresses. The
  OpenStreetMap fallback was off in this session; in CI it will take some of
  them.
- Backfill (`--since-date 2026-08-24`, read from 2026-08-17): 969 requests in
  1,017 s, 945 inspections 2026-08-17..2026-11-21 from 968 pages, nothing
  dropped or held, no score disagreement (518 itemised pages, all three
  statements equal), 427 clean, 22 unscored visits and 1 score-without-items
  skipped. 891 restaurants, 929 D1 inspections (same-day visits collapse).
  Grades: A 903, B 32, C 2, F 8; scores 75-100. All 8 F's are text-forced by
  named findings (rodent or rat droppings in 7, "flies and roach activity" in
  1). Census geocoded 737 of 945 (78%), all inside a Nueces County box.

## Known gaps and open decisions

- No town: Port Aransas (and the smaller towns) ship as Corpus Christi and
  mostly do not geocode. A street-to-town rule would be a guess; the honest
  fix is a source field, which does not exist.
- No inspection type: follow-ups and pre-opening visits ship like routine
  inspections.
- Two shapes (`CC_UNSCORED`, `CC_NO_DETAIL`) are skipped, so a place whose
  only visit is one of them does not appear.
- The 7-day re-read is a guess at late publication, not a measurement.
- Institutional kitchens (schools, hospitals, senior centers) are skipped
  by name, as for Georgia and Louisiana; Mesquite ships its schools. The
  owner may prefer one rule for Texas.
