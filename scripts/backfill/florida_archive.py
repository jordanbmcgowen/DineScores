#!/usr/bin/env python3
"""Florida DBPR's frozen FY2025-26 statewide workbook -> inspection records.

The weekly refresh reads only the current fiscal year's district CSVs (from
July 1), so a restaurant DBPR inspected in FY2025-26 but not yet this year is
not in the database. This reads DBPR's published statewide archive (the same
file and the same parser `backfill_emerald_coast.py` uses for three counties)
for every county and writes the records dated before the current fiscal year
for `--input-records` (absorb.sh). No database writes, no freshness changes;
rows whose official counts do not reconcile go to the companion report.

  python3 scripts/backfill/florida_archive.py --output /tmp/fl.records.json.gz
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import dinescores_pipeline as dp  # noqa: E402
from backfill_emerald_coast import ARCHIVE, workbook_rows  # noqa: E402

CURRENT_FY_START = '2026-07-01'     # the district CSVs hold everything from here
# DBPR republished the workbook with the database's column names (seen
# 2026-11-27); the district CSVs keep the long names the parser reads.
WORKBOOK_COLUMNS = {
    'DISTRICT': 'District', 'COUNTYCODE': 'County Number', 'CNTY_DESC': 'County Name',
    'PROFESSION': 'License Type Code', 'LICENSE_NO': 'License Number',
    'DBA_NAME': 'Business (DBA-Does Business As) Name', 'LOC_ADDRESS': 'Location Address',
    'LOC_CITY': 'Location City', 'LOC_ZIP': 'Location Zip Code', 'INSP_NO': 'Inspection Number',
    'VISIT_NO': 'Visit Number', 'INSPCLASS': 'Inspection Class', 'INSPTYPE': 'Inspection Type',
    'DISPOSITION': 'Inspection Disposition', 'INSP_DATE': 'Inspection Date',
    'CRIT_VIOL': 'Number of Critical Violations', 'NONCRIT_VIOL': 'Number of Noncritical Violations',
    'VIOLATIONS': 'Number of Total Violations', 'HIGH_VIOL': 'Number of High Priority Violations',
    'INTERMED_VIOL': 'Number of Intermediate Violations', 'BASIC_VIOL': 'Number of Basic Violations',
    'PDA': 'PDA Status', 'LIC_ID': 'License ID', 'INSP_VST_ID': 'Inspection Visit ID',
}


def renamed(rows):
    """The workbook's rows with its header in the district CSVs' names."""
    header = next(rows, None)
    if header is None:
        return
    yield [WORKBOOK_COLUMNS.get(c.strip(), c.strip()) for c in header]
    yield from rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    parser.add_argument('--before', default=CURRENT_FY_START,
                        help='keep inspections dated before this day (default %(default)s)')
    args = parser.parse_args()
    response = dp.requests.get(ARCHIVE, headers={'User-Agent': dp.CHROME_UA}, timeout=600)
    response.raise_for_status()
    rejected = []
    rows = dp._parse_florida_rows(renamed(workbook_rows(response.content)), 'FY2526', rejected=rejected)
    unique = {}
    for r in rows:
        if r['inspection_date'] >= args.before:
            continue
        prev = unique.get(r['source_id'])
        if prev and prev != r:
            raise ValueError(f"Conflicting official inspection {r['source_id']}; review before merging")
        unique[r['source_id']] = r
    records = sorted(unique.values(), key=lambda r: (r['inspection_date'], r['source_id']))
    if len(records) < 50000:
        raise RuntimeError(f'Only {len(records)} FY2025-26 inspections parsed; refusing a partial archive')
    Path(args.output + '.rejected.json').write_text(json.dumps(rejected, indent=2) + '\n')
    dp.write_records_json(records, args.output)
    ids = {dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in records}
    print(json.dumps({'inspections': len(records), 'restaurants': len(ids), 'rejected': len(rejected),
                      'oldest': records[0]['inspection_date'], 'newest': records[-1]['inspection_date']},
                     indent=2))


if __name__ == '__main__':
    main()
