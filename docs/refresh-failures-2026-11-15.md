# November 15 refresh diagnosis

Run [43734964206](https://github.com/jordanbmcgowen/DineScores/actions/runs/43734964206)
checked out d1249ad and committed successful updates as 1a6f3e0. The D1
import succeeded (1,200 queries); the final completeness step correctly
failed on four sources. This change does not alter production data,
freshness, pending queues, schedules, grading, or completeness checks.

## Florida

The first district CSV download failed. The old exception suppressed the
underlying transport/status error, so the historical cause cannot be
identified more precisely from this run's logs. There was no retry.

Add three bounded attempts for connection errors, timeouts, HTTP 429 and
500/502/503/504, with growing backoff and a deadline stop. Permanent HTTP
errors and TLS errors fail immediately. Include the original exception in
the diagnostic. A failed district still aborts the entire statewide read;
CSV/schema validation and invalid-count holds remain unchanged.

Live verification from this environment: all seven current-fiscal-year
CSVs downloaded and parsed for the failed run's start date, 2026-11-06.
The importer returned 2,730 inspections through 2026-11-14 and quarantined
one row with invalid official counts. The Florida source remains an error
while that row is unresolved; download recovery does not imply complete
source data. No live data was saved or applied to D1.

## McKinney

The city returns HTTP 200 with an ArcGIS error payload, code 400,
extendedCode -2147467261, message `Unable to complete operation.`.
Reproduced for the original date-filtered count, unfiltered `where=1=1`
count, and equivalent date predicates. Layer metadata is readable, but a
one-row feature query also fails (`Pagination is not supported.`).
This is an unresolved upstream service failure; changing the date syntax
or bypassing the count check would not establish completeness.

Retry the generic operation-error payload through the existing three-attempt
DFW helper, just as HTTP server errors are retried. Invalid-query payloads
still fail immediately. Persistent failures still abort and report an
error; count reconciliation and permit-layer requirements are unchanged.

## Louisiana

The run exhausted four attempts on HTTP 500 for search page 60 of
2026-11-05, then rejected the partial facility list. The importer already
retries transient statuses and validates complete ordered daily pages.
A direct recheck of pages 59/60 from this environment, including the
importer's configured headers, returned HTTP 403, so the original 500's
cause could not be reproduced or safely repaired here. Keep the failed
source visible and its retry window intact. Do not interpret an unreadable
page as the end of pagination. No Louisiana code change is justified by
this evidence.

## Maricopa

The run shipped 3,627 readable inspections and retained 17 pending reports:
13 missing published tiers, three county `Data Not Currently Available`
responses, and one unread report. The durable queue already retries these
outside the rolling window and only removes fully parsed reports; see
[the previous diagnosis](maricopa-incomplete-reports-2026-11-08.md).

Directly rechecked the unread report,
[FD-67337 / INSP-046891-2026](https://envapp.maricopa.gov/Inspection/FD-67337/INSP-046891-2026):
its header states November 9, grade B, one priority violation, but the
response has no violation details. The current parser correctly rejects
it. Do not infer tiers or a clean score from these incomplete reports.
No Maricopa code or queue changes are warranted.

## Validation

- `python -m unittest discover -s tests`: 766 tests pass.
- `node tests/test_relay.mjs`: passes.
- New regressions cover Florida transient recovery, permanent/TLS errors,
  deadline stops, exhausted downloads after an earlier district succeeded,
  and McKinney HTTP-200 operation-error recovery/exhaustion versus invalid
  queries.
- Live read-only probes described above; no production refresh or deploy.
