"""--output-records / --input-records: a run's inspection records survive a
JSON round trip (plain or gzipped) so a backfill split across runs can be
absorbed into one data.js merge without refetching."""
import gzip
import json
import os
import tempfile
import unittest
from datetime import date

import dinescores_pipeline as dp


def _record(**over):
    rec = {
        'name': 'Taco Hut', 'address': '1 Main St', 'city': 'Phoenix', 'state': 'AZ',
        'zip': '85001', 'latitude': 33.45, 'longitude': -112.07,
        'inspection_date': '2026-10-01', 'original_score': None, 'risk_score': 93,
        'priority_violations': 1, 'priority_foundation_violations': 1, 'core_violations': 0,
        'total_violations': 2,
        'violations': [['Food Temperature', 'priority', 'Cold holding at 48F'],
                       ['Hand Washing', 'priority_foundation', 'No soap at hand sink']],
        'inspection_type': 'Routine', 'results': 'A', 'source': 'Maricopa County Environmental Services',
        'source_url': 'https://example.test/1', 'source_id': 'mc_1', 'metro': 'Phoenix',
    }
    rec.update(over)
    return rec


class RecordsRoundTripTests(unittest.TestCase):
    def test_plain_and_gzipped_files_round_trip(self):
        recs = [_record(), _record(name='Pho 2', inspection_date='2026-10-02',
                                   _jev={'forces_f': False, 'chips': ['temp'], 'summaries': ['Cold food too warm']})]
        with tempfile.TemporaryDirectory() as d:
            plain = os.path.join(d, 'a.json')
            gz = os.path.join(d, 'b.json.gz')
            dp.write_records_json(recs[:1], plain)
            dp.write_records_json(recs[1:], gz)
            with gzip.open(gz, 'rt') as f:
                self.assertEqual(json.load(f)[0]['name'], 'Pho 2')
            back = dp.read_records_json([plain, gz])
        self.assertEqual(back, recs)                      # exact: lists stay lists, _jev kept
        self.assertEqual(dp.make_restaurant_id(back[0]['name'], back[0]['address'], back[0]['city']),
                         dp.make_restaurant_id(recs[0]['name'], recs[0]['address'], recs[0]['city']))

    def test_dates_are_written_as_text(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, 'a.json')
            dp.write_records_json([_record(inspection_date=date(2026, 10, 3))], path)
            back = dp.read_records_json([path])
        self.assertEqual(back[0]['inspection_date'], '2026-10-03')

    def test_refuses_files_that_are_not_records(self):
        with tempfile.TemporaryDirectory() as d:
            bad = os.path.join(d, 'bad.json')
            with open(bad, 'w') as f:
                json.dump([{'_id': 'x', 'name': 'no date'}], f)
            with self.assertRaises(ValueError):
                dp.read_records_json([bad])
            with open(bad, 'w') as f:
                json.dump({'name': 'not a list'}, f)
            with self.assertRaises(ValueError):
                dp.read_records_json([bad])


if __name__ == '__main__':
    unittest.main()
