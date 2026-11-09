#!/usr/bin/env python3
"""Fetch official Emerald Coast inspections; emit raw records for pipeline import.

No database writes, no freshness changes. Run the normal pipeline with
--input-records on the result to merge/map/seed only after reviewing it.
"""
import argparse
import io
import json
from pathlib import Path
import sys
import zipfile
import xml.etree.ElementTree as ET

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import dinescores_pipeline as dp

COUNTIES = ('Walton', 'Okaloosa', 'Bay')
# Verified link on DBPR's public-records page, not an inferred URL pattern.
ARCHIVE = 'https://www2.myfloridalicense.com/hr/inspections/fdinspi_2526.xlsx'
NS = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}


def workbook_rows(content):
    """Read DBPR's first worksheet, retaining sparse-cell column positions.

    FY2526 dates are strings; numeric dates are rejected by the shared parser.
    Stream rows (the statewide worksheet expands to hundreds of MB).
    """
    with zipfile.ZipFile(io.BytesIO(content)) as book:
        strings = []
        if 'xl/sharedStrings.xml' in book.namelist():
            with book.open('xl/sharedStrings.xml') as source:
                for _, element in ET.iterparse(source, events=('end',)):
                    if element.tag == '{%s}si' % NS['m']:
                        strings.append(''.join(t.text or '' for t in element.findall('.//m:t', NS)))
                        element.clear()
        with book.open('xl/worksheets/sheet1.xml') as source:
            # Clear sheetData after each row, including its already-cleared
            # child, to keep memory bounded over the statewide archive.
            context = ET.iterparse(source, events=('start', 'end'))
            sheet_data = None
            for event, element in context:
                if event == 'start' and element.tag == '{%s}sheetData' % NS['m']:
                    sheet_data = element
                if event != 'end' or element.tag != '{%s}row' % NS['m']:
                    continue
                row = []
                for cell in element:
                    reference = cell.get('r', '')
                    column = 0
                    for letter in reference:
                        if not letter.isalpha():
                            break
                        column = column * 26 + ord(letter.upper()) - 64
                    if not column:
                        raise ValueError('Workbook cell has no column reference')
                    row.extend([''] * max(0, column - len(row)))
                    value = cell.find('m:v', NS)
                    text = value.text or '' if value is not None else ''
                    if cell.get('t') == 's':
                        text = strings[int(text)]
                    elif cell.get('t') == 'inlineStr':
                        text = ''.join(t.text or '' for t in cell.findall('.//m:t', NS))
                    row[column - 1] = text
                yield row
                element.clear()
                if sheet_data is not None:
                    sheet_data.clear()


def fetch_records(include_archive=True, rejected=None):
    records = []
    if include_archive:
        response = dp.requests.get(ARCHIVE, headers={'User-Agent': dp.CHROME_UA}, timeout=180)
        response.raise_for_status()
        records.extend(dp._parse_florida_rows(workbook_rows(response.content),
                                              'FY2526', counties=COUNTIES, rejected=rejected))
    records.extend(dp.fetch_florida(district_files=['6fdinspi.csv'], counties=COUNTIES))
    # Archive/current overlap must not weight an inspection twice. Identical
    # source IDs have the same meaning as in the statewide importer.
    unique = {}
    for record in records:
        key = record['source_id']
        previous = unique.get(key)
        if previous and previous != record:
            raise ValueError(f'Conflicting official inspection {key}; review before merging')
        unique[key] = record
    return sorted(unique.values(), key=lambda r: (r['inspection_date'], r['source_id']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, help='Raw inspection JSON (not a live database)')
    parser.add_argument('--current-only', action='store_true', help='Skip the frozen FY2025-26 archive')
    parser.add_argument('--geocode', action='store_true', help='Use the pipeline geocoder before saving')
    args = parser.parse_args()
    rejected = []
    records = fetch_records(include_archive=not args.current_only, rejected=rejected)
    if not records:
        raise RuntimeError('No coastal inspections returned; refusing an empty backfill')
    if args.geocode:
        dp.geocode_missing_coords(records)
    # All inputs must validate before opening an output file.
    report_path = args.output + '.rejected.json'
    Path(report_path).write_text(json.dumps(rejected, indent=2) + '\n')
    dp.write_records_json(records, args.output)
    print(json.dumps({'rejected': len(rejected), 'rejection_report': report_path, 'inspections': len(records), 'cities': sorted({r['city'] for r in records}),
                      'oldest': records[0]['inspection_date'], 'newest': records[-1]['inspection_date'],
                      'with_coordinates': sum(r['latitude'] is not None for r in records)}, indent=2))


if __name__ == '__main__':
    main()
