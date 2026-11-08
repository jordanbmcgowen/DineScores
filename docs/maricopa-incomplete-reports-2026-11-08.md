# Maricopa incomplete inspection reports, November 8, 2026

## Finding

[Refresh run 42497427566](https://github.com/jordanbmcgowen/DineScores/actions/runs/42497427566)
shipped 4,417 Maricopa inspections and rejected 12 HTTP-200 detail pages.
All 12 were fetched individually again for this diagnosis; none was a
transport error, a CAPTCHA, or an `Inspection Not Available` response.

- Eight reports contain local MCEHC regulations with no published Priority /
  Priority Foundation / Core tier: dog-friendly patios (five), indoor play
  areas (two), and a micro-market refrigerator (one).
- Three reports contain free-text observations without a tier: date marking,
  plumbing / physical facilities / lighting, and damaged restroom flooring.
- `FD2501186/INSP-193498-2026` (KOREAN BBQ & HOT POT, October 14) shows grade D
  and three priority violations in its header but replaces the violation
  block with **Data Not Currently Available** and asks to try again later.

The exact public source URLs and expected reasons are in
`tests/fixtures/maricopa/nov8_incomplete/manifest.json`. Each HTML fixture is
the source page's unmodified `<main>` element, captured November 8.

The source-stated scoring policy cannot safely assign tiers to the 11
untiered reports. A violation item number/title can span multiple tiers;
a county letter grade or the aggregate Priority count does not establish
which missing citation has which tier. The temporary-error report must
never turn into a clean inspection either. This fix deliberately does not
manufacture the missing information or suppress the source error.

## Repair

- Report missing published tiers and the county's temporary data error
  separately from unrecognized markup.
- Persist exact incomplete candidates in `pending_details` under the
  Maricopa source in the freshness file. They do not increment the generic
  three-run give-up count, including when a later attempt times out or a
  deadline cuts the run short.
- Retry those exact candidates outside the rolling date window and even
  if a subsequent listing omits the report. A same-day embedded inspection
  cannot mask the pending report. Existing two-failure holds are honored
  on the first run of this code, before the durable queue has been written.
- Only a fully parsed report leaves the queue. Keep a partial source red
  while any report remains pending, even below the usual five-page threshold.
- Keep all successful records and the existing merge/D1 transaction path.
  No production data, freshness history, seeds or schedules were edited.

The next normal refresh adopts the 12 existing holds and persists any still
incomplete report. A bulk rerun would not restore information missing from
the county's responses and is not part of this repair. The source remains
incomplete until the county publishes usable content or a separately
verified source supplies the missing information.

## Verification

Tests replay all 12 actual pages and assert they are not shipped clean.
They cover JSON-persisted retries beyond the three-run give-up, a newer
lookback window, same-day known inspections, absent listings, transport
failure, deadline interruption, and removal only after a complete read.
The repository's entire offline Python suite, relay tests and Python compile
checks are required before publication.
