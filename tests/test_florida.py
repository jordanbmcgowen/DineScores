"""Offline tests using public District 6 rows captured 2026-11-09."""
import csv
import io
import importlib.util
from pathlib import Path
import unittest
from unittest import mock
import zipfile
import tempfile
import sqlite3
import dinescores_pipeline as dp
ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'tests/fixtures/florida/district6.csv'
spec = importlib.util.spec_from_file_location('backfill', ROOT / 'scripts/backfill_emerald_coast.py')
bf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bf)

class FloridaTests(unittest.TestCase):
    def setUp(self):
        self.rows = list(csv.reader(io.StringIO(FIXTURE.read_text())))
        self.header = [c.strip() for c in self.rows[0]]
    def parse(self, rows=None, **kwargs):
        return dp._parse_florida_rows(iter(self.rows if rows is None else rows), 'fixture', **kwargs)
    def test_county_scope_and_score_identity(self):
        coast = self.parse(counties=bf.COUNTIES)
        self.assertGreater(len(self.parse()), len(coast))
        self.assertEqual(len(coast), 1)
        d = dict(zip(self.header, self.rows[1])); r = coast[0]
        self.assertEqual(r['source_id'], 'fl_'+d['Inspection Number']+'_'+d['Visit Number'])
        self.assertEqual(r['risk_score'], 100-5*int(d['Number of High Priority Violations'])-2*int(d['Number of Intermediate Violations'])-int(d['Number of Basic Violations']))
        self.assertIsNone(r['latitude']); self.assertEqual(r['violations'], [])
        rows = [self.rows[0], self.rows[1].copy()]
        rows[1][self.header.index('County Name')] = 'Walton'
        rows[1][self.header.index('Location City')] = 'SANTA ROSA BEACH'
        self.assertEqual(self.parse(rows, counties=bf.COUNTIES)[0]['source_id'], r['source_id'])
    def test_bad_counts_fail_or_are_explicitly_quarantined(self):
        for value in ('', '-1', 'unknown', '0.5', '9'):
            with self.subTest(value=value):
                rows = [self.rows[0], self.rows[1].copy()]
                rows[1][self.header.index('Number of High Priority Violations')] = value
                with self.assertRaises(RuntimeError): self.parse(rows)
                rejected = []
                self.assertEqual(self.parse(rows, rejected=rejected), [])
                self.assertEqual(len(rejected), 1)
                self.assertEqual(rejected[0]['source_id'], self.parse(counties=bf.COUNTIES)[0]['source_id'])
    def test_empty_schema_and_short_rows_fail(self):
        for rows in ([], [['error page']], [self.rows[0], ['D6', '13', 'Bay', '2001']]):
            with self.assertRaises((ValueError, RuntimeError)): self.parse(rows)
    def test_window_and_limit(self):
        self.assertEqual(self.parse(since_date='2099-01-01'), [])
        self.assertEqual(len(self.parse(limit=1)), 1)
    def test_download_failure_never_returns_partial_state(self):
        response = mock.Mock(content=FIXTURE.read_bytes())
        with mock.patch.object(dp.requests, 'get', side_effect=[response, dp.requests.ConnectionError('down')]):
            with self.assertRaisesRegex(RuntimeError, 'partial extract'):
                dp.fetch_florida(district_files=['6fdinspi.csv', '7fdinspi.csv'])
    def test_historical_sql_keeps_newer_live_state_and_is_idempotent(self):
        record = self.parse(counties=bf.COUNTIES)[0]
        newer = dict(record, inspection_date='2026-10-27', risk_score=77,
                     latitude=30.2, longitude=-85.8)
        historical = dict(record, inspection_date='2025-07-02', risk_score=100)
        database = sqlite3.connect(':memory:')
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'review.sql')
            dp.write_d1_sql([newer], path)
            database.executescript(Path(path).read_text())
            dp.write_d1_sql([historical], path)
            sql = Path(path).read_text()
            database.executescript(sql)
            database.executescript(sql)
        self.assertEqual(database.execute('SELECT inspection_date,risk_score,lat,lng FROM restaurants').fetchone(),
                         ('2026-10-27', 77, 30.2, -85.8))
        self.assertEqual(database.execute('SELECT COUNT(*) FROM inspections').fetchone()[0], 2)
        database.close()

    def test_sparse_workbook_cells_keep_positions(self):
        out = io.BytesIO()
        with zipfile.ZipFile(out, 'w') as book:
            book.writestr('xl/sharedStrings.xml', '<sst xmlns="'+bf.NS['m']+'"><si><t>District</t></si></sst>')
            book.writestr('xl/worksheets/sheet1.xml', '<worksheet xmlns="'+bf.NS['m']+'"><sheetData><row><c r="A1" t="s"><v>0</v></c><c r="R1"><v>4</v></c><c r="U1" t="inlineStr"><is><t>Basic</t></is></c></row></sheetData></worksheet>')
        row = list(bf.workbook_rows(out.getvalue()))[0]
        self.assertEqual(row[0], 'District'); self.assertEqual(row[15:18], ['', '', '4']); self.assertEqual(row[20], 'Basic')
    def test_deduplication_and_conflicting_inspections(self):
        record = self.parse(counties=bf.COUNTIES)[0]
        response = mock.Mock(content=b'workbook')
        for current, conflict in ((record, False), (dict(record, risk_score=1), True)):
            with mock.patch.object(dp.requests, 'get', return_value=response), mock.patch.object(bf, 'workbook_rows', return_value=iter(self.rows)), mock.patch.object(dp, 'fetch_florida', return_value=[current]):
                if conflict:
                    with self.assertRaisesRegex(ValueError, 'Conflicting'): bf.fetch_records()
                else: self.assertEqual(bf.fetch_records(), [record])

if __name__ == '__main__': unittest.main()
