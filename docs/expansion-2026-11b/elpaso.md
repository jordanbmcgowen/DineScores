# El Paso, TX (City of El Paso Department of Public Health)

Built 2026-11-23 from the survey in the expansion scratchpad (`survey/west.md`,
"El Paso"). Score-only source: one official 0-100 number per inspection, no
violation text.

## Source

- Agency: City of El Paso Department of Public Health, Food Inspection
  program. Covers the City of El Paso, Anthony, Clint, Horizon City, Socorro,
  Vinton and unincorporated El Paso County (~866k people).
- Endpoint: `https://foodscores.elpasotexas.gov/`, a city PHP page behind
  Cloudflare. ONE `multipart/form-data` POST of the search form
  (`businessName`, `address`, `city`, `zipCode`, `fromDate`, `toDate`,
  `minScore`, `maxScore`, `search`; only the dates are filled) answers every
  inspection of the window as one HTML table `#resultsTable` (DataTables pages
  it in the browser; the server sends every row, 29,942 rows in one answer
  for 2010-2018). ~12-35 s server time, whatever the window size.
- `fromDate` is inclusive, `toDate` EXCLUSIVE (11-18..11-18 answered 0 rows,
  11-18..11-19 only the 18th), so the window ends tomorrow.
- Columns: Permit ID (Accela record id, `HFFX11-00387`), Date `MM/DD/YYYY`,
  Business Name, Address (the full postal string), City, Zip Code, Sanitary
  Code (`1310 Routine Inspection`, `1320 Re-inspection`, `1300 Site
  Assessment`), Score.
- The "Download CSV" button reads `output.csv`, ONE server-side file
  rewritten by any visitor's last search. Never read.
- Not to be confused with MHD's `epcph` (El Paso County, **Colorado**, already
  shipping as `El Paso County CO Health Dept`).

## Names

| | |
|---|---|
| CLI slug | `elpaso` |
| Fetch group | `El Paso` |
| Source label | `City of El Paso Public Health` |
| source_id | `elp_<permit>_<YYYYMMDD>` (`_2`, `_3` for further rows of that permit and day) |
| metro | `''` |

## Rules and why

- **Score**: `risk_score` = `original_score` = the city's official score
  (higher is better; the city's own rule: 70+ passes, 69 or less starts
  closure action, 60 or less closes). DineScores' F (<70) lines up with the
  city's closure-action line.
- **No violations**: `violations` is `[]`, so nothing goes to Jev, no chips or
  summaries, no text-forced F, and no pest-title handling is needed. The
  grade is the score's.
- **Identity**: the source has no inspection id but the permit id is stable
  (one name and one address per permit across all 23,153 rows of 2024-2026),
  so `source_id` is permit + date rather than a hash of name and address.
  Two permits can share a name and address (Montana Vista Food Store
  HFFX11-00318/-00319, inspected together); they share a restaurant id and
  the pipeline's same-day rule keeps the worse score.
- **Duplicates**: an exact duplicate row (19 in 2024-2026) ships once.
  Distinct rows of one permit and day (81 permit-days in 2024-2026, mostly a
  failed routine and its same-day re-inspection, e.g. Lin's Grand Buffet 68
  then 87) each ship, worst first; the 68 is what the map shows for that day.
- **Scope**: `1300 Site Assessment` rows (unscored pre-opening visits) are
  skipped. The default search excludes mobile units (`HFMB` permits appear
  only with `mobile=Mobile`; 2,385 rows in 2024-2026, none since 2026-06);
  an HFMB row is dropped anyway. There is no facility type, so institutional
  kitchens are dropped by name: `TP_INSTITUTION_RE` (Georgia/Louisiana) plus
  `TX_SCORE_ONLY_INSTITUTION_RE` (adult foster homes, personal care homes,
  care centers, children's homes, `*ISD` concessions, juvenile probation,
  the state veterans home, MHMR). 2,987 of 23,153 rows (13%) in 2024-2026;
  every dropped name was reviewed (659 names): schools, daycares, nursing
  homes, adult foster homes, a karate academy. Convenience stores, dollar
  stores, Herbalife "nutrition" clubs, churches and hotel kitchens stay (the
  city permits them as food establishments).
- **City**: the City cell, with its typos repaired (`CANUTILLO79835`,
  `HORIZON` -> Horizon City). `EL PASO COUNTY` (233 rows, unincorporated) and
  a blank cell take the town that owns the ZIP (79821 Anthony, 79835
  Canutillo, 79836 Clint, 79838 Fabens, 79849 San Elizario, 79853 Tornillo),
  else El Paso: records always carry a real city for the geocoder and the
  city index.
- **Address**: the street part of the Address cell; trailing ZIP, state
  (`TX`/`TEXAS`) and town removed; the permit system's empty unit fields
  (`AddrAs#`, `plat a#`, `parcel#`) dropped, `Addr A# B` / `Former# 114` keep
  their value; `SUITE# F` / `BLDG# C` written `Ste F` / `Bldg C` after the
  street (the Census matcher strips a trailing unit written that way); ALL
  CAPS title-cased with Texas road codes kept (`FM 1110`). A row with no
  street number left (`0`, `BLVD, EL PASO, TX 79925`: 1 row) is dropped.
- **No coordinates**: `main()` geocodes (Census batch).
- **Window**: weekly windows never start later than `EP_REREAD_DAYS` (35)
  ago, the Williamson County rule: the whole window is one POST anyway (~700
  rows, ~450 KB), postings run a day or two late, and re-shipping is
  idempotent. No freshness holds: nothing is read per record, so a window is
  read completely or not at all.

## Fail closed

| Answer | Outcome |
|---|---|
| Cloudflare challenge (`cf-mitigated: challenge`, "Just a moment...", Turnstile, "Sorry, you have been blocked") or HTTP 401/403 | group **blocked**, nothing shipped, recorded latest unchanged; never retried or worked around |
| 429 / 5xx / timeout / reset | retried 3 times (10 s, 20 s), never past the fetch deadline; then **error** |
| other non-200, TLS failure | **error** at once (verification is never relaxed) |
| 200 without `#resultsTable` or with other headers, a row without 8 cells | **error** |
| >1% rows with an unreadable date/score, or more than half the window unscored (>= 20 rows) | **error** |
| a window of 21+ days with fewer than 4 rows a day (the quietest 21 days of 2024-2026 held 236 rows, ~11 a day) | **error** ("or the city stopped publishing") |
| fetch deadline already passed | **skipped** |

Both blocked and error make the weekly run red (the last workflow step), so a
refused runner is never silent.

## GitHub-hosted runners and Cloudflare (NOT tested)

Every request from this session (curl and the fetcher) was served 200, but
Cloudflare's JavaScript-detection snippet is on the page, and whether it
challenges GitHub's Azure egress cannot be tested from here. If it does, the
group reports `blocked` with the HTTP status and the run goes red; nothing
ships and the recorded latest stays, so the first run that gets through
re-reads the whole gap (the window starts at the recorded latest, capped at
400 days).

The fix would be the site's own relay. `functions/api/relay.js` already
forwards POST bodies and the caller's `Content-Type` (multipart boundary
included) and follows redirects by hand; it only allows two hostnames today.
The change (not made here):

1. add `'foodscores.elpasotexas.gov'` to `ALLOWED_HOSTS` in
   `functions/api/relay.js`, with a case in `tests/test_relay.mjs`;
2. in `_ep_post`, send the POST through `_relay(EP_URL)` the way the SNHD
   fetcher does (relayed URL plus the `X-Relay-Token` header, the same
   `files=` body).

Caveat: a Cloudflare Worker fetching a Cloudflare-fronted site with bot
protection can itself be challenged; that would also read as `blocked`, not
as an empty week.

## Measured (live, 2026-11-23)

- Test mode (`--mode test`): 1 POST, 338 rows for 2026-11-09 .. 11-24, 25
  newest shipped, 25/25 geocoded.
- Recent window (`--mode full --since-date 2026-11-09`, widened to
  2026-10-19 by the re-read margin): 1 POST, 812 rows, 731 inspections
  2026-10-19 .. 2026-11-20 (71 institutional, 10 site assessments skipped),
  726 restaurants. Grades A 583 / B 127 / C 20 / F 1; 0 text-forced F (no
  text). Scores 63-100. Geocoded 672/731 (91.9%) by the Census batch alone
  (the OpenStreetMap fallback is off in this session; CI runs it for up to its
  cap). Source newest 2026-11-20 (posting lag ~2 days).
- Backfill: see below.

## Backfill hand-off

BACKFILL_PLACEHOLDER

## Known gaps

- No violations, ever (the full report is an Open Records request).
- No facility type: the institutional filter is by name only.
- No coordinates; ~8% of addresses miss the Census geocoder (shopping-center
  units, `Spc -A-20` style tails, new streets on the east side).
- Mobile units are not covered (the city lists them without an address).
- Cloudflare behaviour toward GitHub runners is unknown (above).
