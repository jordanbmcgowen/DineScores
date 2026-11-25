"""scripts/backfill/pins_seed.py: coordinates-only seed for restaurants placed
after their backfill was absorbed (no network)."""
import gzip
import json
import os
import sqlite3
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), 'scripts', 'backfill'))

import dinescores_pipeline as dp  # noqa: E402
import pins_seed as ps  # noqa: E402


def rec(name, address, city='Billings', state='MT', lat=None, lng=None):
    return {'name': name, 'address': address, 'city': city, 'state': state, 'latitude': lat, 'longitude': lng,
            'inspection_date': '2026-11-10'}


class PinsSeedTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        ref = [rec(f'Ref {n}', f'{n} Main St', lat=45.78 + n * 0.001, lng=-108.50) for n in range(5)]
        ref += [rec('Lone', '1 Elm St', city='Roundup', lat=46.44, lng=-108.54)]       # a town with one reference
        self.ref = os.path.join(self.tmp.name, 'ref.json.gz')
        with gzip.open(self.ref, 'wt') as f:
            json.dump(ref, f)
        self.centres = ps.town_centres([self.ref])

    def test_town_centres_need_enough_placed_restaurants(self):
        self.assertEqual(list(self.centres), [('Billings', 'MT')])
        self.assertAlmostEqual(self.centres[('Billings', 'MT')][0], 45.782, places=3)

    def test_a_pin_far_from_its_town_is_left_out(self):
        placed = [rec('Near', '9 Oak St', lat=45.80, lng=-108.52),
                  rec('Far', '4800 Main', lat=47.50, lng=-111.30),                    # Great Falls, 300 km away
                  rec('Other State', '1 Pine St', lat=41.14, lng=-104.82),            # Cheyenne
                  rec('Small Town', '2 Elm St', city='Roundup', lat=46.45, lng=-108.54),
                  rec('Unplaced', '3 Elm St')]
        pins, stats, far = ps.choose(placed, self.centres)
        ids = {name: dp.make_restaurant_id(name, addr, city) for name, addr, city in
               (('Near', '9 Oak St', 'Billings'), ('Small Town', '2 Elm St', 'Roundup'))}
        self.assertEqual(set(pins), set(ids.values()))
        self.assertEqual(dict(stats), {'kept_near_its_town': 1, 'too_far_from_its_town': 1,
                                       'outside_the_state': 1, 'kept_no_town_reference': 1})
        self.assertEqual([f[0] for f in far], ['Far'])
        self.assertLess(ps._km((45.78, -108.50), (45.80, -108.52)), 5)

    def test_the_seed_only_fills_missing_coordinates(self):
        pins = {'aaa': (45.8, -108.52), 'bbb': (45.9, -108.6)}
        sql = ps.seed_sql(pins)
        self.assertEqual(sql.count('AND lat IS NULL;'), 2)
        db = sqlite3.connect(':memory:')
        db.executescript(dp.D1_SCHEMA_SQL)
        db.execute("INSERT INTO restaurants (id, name, city, weighted_score) VALUES ('aaa', 'A', 'Billings', 77)")
        db.execute("INSERT INTO restaurants (id, name, city, lat, lng) VALUES ('bbb', 'B', 'Billings', 1.0, 2.0)")
        db.executescript(sql)
        db.executescript(sql)
        rows = {r[0]: r[1:] for r in db.execute('SELECT id, lat, lng, weighted_score FROM restaurants')}
        self.assertEqual(rows['aaa'], (45.8, -108.52, 77))
        self.assertEqual(rows['bbb'][:2], (1.0, 2.0), 'a pin already there is never moved')

    def test_data_js_records_without_coordinates_are_filled(self):
        recs = [{'i': 'aaa', 'n': 'A', 'lt': None, 'ln': None, 'rs': 90},
                {'i': 'bbb', 'n': 'B', 'lt': 1.0, 'ln': 2.0, 'rs': 80},
                {'i': 'ccc', 'n': 'C', 'lt': None, 'ln': None, 'rs': 70}]
        content = '/* h */\nwindow.DATA = ' + json.dumps(recs, separators=(',', ':')) + ';\n'
        out, filled = ps.fill_data_js(content, {'aaa': (45.8000004, -108.52), 'bbb': (9.0, 9.0)})
        got = json.loads(out.split('window.DATA = ', 1)[1].rstrip(';\n'))
        self.assertEqual(filled, 1)
        self.assertEqual((got[0]['lt'], got[0]['ln'], got[0]['rs']), (45.8, -108.52, 90))
        self.assertEqual(got[1:], recs[1:])


if __name__ == '__main__':
    unittest.main()
