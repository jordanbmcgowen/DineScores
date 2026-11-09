# Emerald Coast expansion

Florida DBPR already supplies statewide restaurant inspections. WaterColor and
Seaside are unincorporated beach communities; many establishments are filed under
Santa Rosa Beach or Inlet Beach. Do not add another scraper for the same permits
or rename their postal cities: either would split existing restaurant IDs.

## Sources and coverage

- [DBPR restaurant public records](https://www2.myfloridalicense.com/hotels-restaurants/public-records/):
  District 6 current-fiscal-year CSV and the explicitly published FY2025-26
  statewide workbook. The focused backfill selects **Walton, Okaloosa, and Bay**
  by the agency's county column, not city spelling. It includes inland locations
  in those counties as well as WaterColor, Seaside, Grayton, Seagrove, Alys,
  Rosemary, Inlet Beach, Miramar/Sandestin, Destin and Panama City Beach.
- [Walton County Tourism's neighborhoods](https://www.visitsouthwalton.com/neighborhoods/):
  South Walton comprises 16 beach neighborhoods, including Miramar and Sandestin.
  The existing 30A selector is narrower. South Walton now has a separate broader
  discovery envelope covering the beach communities, US-98 and Point Washington.
  These map rectangles support discovery; they are not legal town boundaries.
- [Florida's agency responsibilities](https://www.floridahealth.gov/licensing-regulations/regulated-facilities/food-safety-and-sanitation/hygiene-codes-and-standards/):
  DBPR covers restaurants, caterers and mobile food vehicles. Grocery stores and
  other FDACS establishments and DOH facilities are separate systems. This does
  not add those sources or claim every food business is covered.

The frozen workbook adds history from July 2025 through June 2026, while the
current extracts cover the fiscal year starting July 2026. Future archives are
not guessed from a URL pattern; verify a new published link before extending
history. Historical inspections do not establish that a business is still open.

## Safe backfill

```sh
python3 scripts/backfill_emerald_coast.py \
  --output /tmp/emerald-coast.json --geocode

python3 dinescores_pipeline.py --input-records /tmp/emerald-coast.json \
  --output /tmp/emerald-coast-restaurants.json \
  --output-data-js /tmp/emerald-coast-data.js \
  --merge-existing-data-js public/data.js \
  --output-d1-sql /tmp/emerald-coast.sql
```

The first command writes raw records and a `.rejected.json` companion report.
The second uses the ordinary ID, scoring, merge and idempotent SQL writers; it
writes review files only. Do not pass the statewide freshness file for this
one-time county-scoped historical import. Review changes against a fresh database
export before applying SQL or replacing embedded data. Keep existing records and
coordinates; a missing geocode must never erase a known location.

No output is opened until both source downloads have parsed successfully. The
historical workbook's sparse cells retain their column positions. Exact duplicate
inspection IDs are coalesced; conflicting records stop the import. Unknown,
negative, fractional or inconsistent severity counts never become a score of 100.
Historical count failures go into the companion report; without an explicit
rejection collector, the ordinary statewide importer fails the group and retains
its previous data rather than silently advancing past a partial district fetch.

Live validation returned **5,725 inspections for 2,435 restaurant IDs**, from
2025-07-01 through 2026-11-06 (4,303 historical plus 1,422 current-year rows).
This is a candidate inspection inventory, not a count of new live locations.
The public API returned 1,201 existing records in the broader coastal bounding
box; those are not directly comparable to whole-county counts. The candidate
records have no coordinates until the optional geocoding pass runs.

Two FY2025-26 inspections are quarantined for follow-up:

- `fl_3669393_2` (Walton, 2025-09-30): one total violation with no severity breakdown.
- `fl_966_1` (Bay, 2026-04-20): total and severity counts do not reconcile.

No severity is inferred. The historical workbook has an extra field after the
severity columns in some rows; only named, position-preserved columns are read.

DBPR extracts have no coordinates or violation narratives. The optional geocoding
pass uses the existing Census/Nominatim implementation; unresolved addresses
remain unresolved, never pinned to a community center. No new pest findings or
violation summaries can be inferred from category totals alone. Historical-only
restaurants may be closed and must be displayed with their actual inspection date.
The normal statewide Sunday refresh already includes all three counties.

## Ennis

Pending clarification: the user requested “Ennis, MO”. Confirm the intended state
or nearby town before choosing an agency. No Missouri/Texas/Montana source is
implemented on an assumption.

## Checks

- Offline Python suite, including captured District 6 rows.
- Relay regression suite and geographic selector tests.
- Production frontend build; built assets remain gitignored.
- Live official workbook/CSV validation with an explicit rejection report.
