"""scripts/unscored_cleanup.py and the Portland-metro inspection-type filter
(no network)."""
import json
import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), 'scripts'))

import dinescores_pipeline as dp  # noqa: E402
import unscored_cleanup as uc  # noqa: E402

MULT = 'Multnomah County Health Dept'
DAL = 'Dallas Health Dept'

SCHEMA = """
CREATE TABLE restaurants (id TEXT PRIMARY KEY, name TEXT NOT NULL, address TEXT, city TEXT NOT NULL,
  state TEXT, zip TEXT, lat REAL, lng REAL, metro TEXT, inspection_date TEXT, original_score INTEGER,
  risk_score INTEGER, weighted_score INTEGER, vetted_grade TEXT, infractions TEXT, summaries TEXT,
  inspection_count INTEGER DEFAULT 1, source TEXT, source_url TEXT, updated_at TEXT);
CREATE TABLE inspections (id TEXT PRIMARY KEY, restaurant_id TEXT NOT NULL, inspection_date TEXT,
  risk_score INTEGER, original_score INTEGER, inspection_type TEXT, results TEXT, violations TEXT,
  source_id TEXT, source_url TEXT);
"""


def insp(iid, date, rs, os_, itype, lines=()):
    return {'id': iid, 'inspection_date': date, 'risk_score': rs, 'original_score': os_,
            'inspection_type': itype, 'results': '', 'violations': json.dumps([list(v) for v in lines]),
            'source_id': None}


def row(rid, source, inspections, rs, ws, vg, date=None):
    return {'id': rid, 'name': rid, 'city': 'Portland', 'state': 'OR', 'metro': 'Portland', 'source': source,
            'inspection_date': date or inspections[0]['inspection_date'], 'original_score': rs,
            'risk_score': rs, 'weighted_score': ws, 'vetted_grade': vg, 'infractions': [], 'summaries': [],
            'inspection_count': len(inspections), 'updated_at': None, 'inspections': inspections}


def rows():
    return [
        # A motel: every stored inspection is a lodging one.
        row('motel', MULT, [insp('m1', '2026-11-04', 0, 0, 'Traveler\'s Accommodation 2009'),
                            insp('m2', '2025-11-01', None, None, 'Traveler\'s Accommodation 2009')], 0, 0, 'F'),
        # A hotel with a restaurant: its latest row is the lodging 0.
        row('hotel', MULT, [insp('h1', '2026-03-25', 0, 0, 'Traveler\'s Accommodation 2009'),
                            insp('h2', '2025-12-04', 100, 100, 'Food Restaurant and B&B 2024'),
                            insp('h3', '2025-06-10', 95, 95, 'Food Restaurant and B&B 2024')], 0, 0, 'F'),
        # Dallas: the row stands on a scored visit; an older No Score 0 made it F.
        row('cafe', DAL, [insp('c1', '2026-06-04', 100, 100, 'Retail Food 2025'),
                          insp('c2', '2026-03-25', 0, 0, 'Retail Food Establishment (No Score)'),
                          insp('c3', '2025-12-04', 100, 100, 'Retail Food 2025')], 100, 70, 'F'),
        # Dallas: an unscored visit with cited lines is graded from them.
        row('grocer', DAL, [insp('g1', '2026-03-04', 0, 0, 'Retail Food Establishment (No Score)',
                                 [('food_source', 'priority', '10: SHALL CLEAN ICE BIN'),
                                  ('general', 'core', '45: CLEAN FLOORS')]),
                            insp('g2', '2025-09-01', 88, 88, 'Retail Food 2025')], 0, 0, 'F'),
        # Untouched.
        row('fine', MULT, [insp('f1', '2026-01-01', 96, 96, 'Food Restaurant and B&B 2024')], 96, 96, 'A'),
    ]


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.p = uc.plan(rows())
        self.mixed = {m['row']['id']: m for m in self.p['mixed']}

    def test_lodging_only_place_is_deleted(self):
        self.assertEqual([r['id'] for r in self.p['delete']], ['motel'])

    def test_hotel_row_is_rebuilt_from_its_food_inspection(self):
        nb = self.mixed['hotel']['rebuild']
        self.assertEqual((nb['inspection_date'], nb['risk_score'], nb['vetted_grade']), ('2025-12-04', 100, 'A'))
        self.assertEqual([i['id'] for i in self.mixed['hotel']['remove']], ['h1'])

    def test_sticky_f_from_an_older_zero_is_lifted(self):
        ro = self.mixed['cafe']['regrade_only']
        self.assertEqual((ro['weighted_score'], ro['vetted_grade']), (100, 'A'))

    def test_unscored_visit_with_lines_is_graded_from_them(self):
        (r, i), = self.p['regrade']
        self.assertEqual((r['id'], i['risk_score']), ('grocer', 94))
        nb = self.mixed['grocer']['rebuild']
        self.assertEqual((nb['risk_score'], nb['vetted_grade']), (94, 'A'))

    def test_untouched_restaurant_is_not_planned(self):
        self.assertNotIn('fine', self.mixed)


class SeedTests(unittest.TestCase):
    def test_seed_applies_once_and_is_guarded(self):
        data = rows()
        db = sqlite3.connect(':memory:')
        db.executescript(SCHEMA)
        for r in data:
            db.execute('INSERT INTO restaurants (id, name, city, source, inspection_date, risk_score, '
                       'weighted_score, vetted_grade, inspection_count) VALUES (?,?,?,?,?,?,?,?,?)',
                       (r['id'], r['name'], r['city'], r['source'], r['inspection_date'], r['risk_score'],
                        r['weighted_score'], r['vetted_grade'], r['inspection_count']))
            for i in r['inspections']:
                db.execute('INSERT INTO inspections (id, restaurant_id, inspection_date, risk_score, '
                           'original_score, inspection_type, violations) VALUES (?,?,?,?,?,?,?)',
                           (i['id'], r['id'], i['inspection_date'], i['risk_score'], i['original_score'],
                            i['inspection_type'], None if i['violations'] == '[]' else i['violations']))
        # The cafe gained a newer inspection since the export: its row stays.
        db.execute("UPDATE restaurants SET inspection_date = '2026-11-20', weighted_score = 90 WHERE id = 'cafe'")
        sql, _ = uc.seed_sql(uc.plan(data))
        db.executescript(sql)
        q = lambda s: db.execute(s).fetchall()  # noqa: E731
        self.assertEqual(q('SELECT id FROM restaurants ORDER BY id'), [('cafe',), ('fine',), ('grocer',), ('hotel',)])
        self.assertEqual(q("SELECT vetted_grade, inspection_count FROM restaurants WHERE id = 'hotel'"), [('A', 2)])
        self.assertEqual(q("SELECT weighted_score, vetted_grade FROM restaurants WHERE id = 'cafe'"), [(90, 'F')])
        self.assertEqual(q("SELECT risk_score, original_score FROM inspections WHERE id = 'g1'"), [(94, None)])
        self.assertEqual(q("SELECT COUNT(*) FROM inspections WHERE restaurant_id = 'motel'"), [(0,)])
        snapshot = q('SELECT * FROM restaurants ORDER BY id') + q('SELECT * FROM inspections ORDER BY id')
        db.executescript(sql)
        self.assertEqual(snapshot, q('SELECT * FROM restaurants ORDER BY id') + q('SELECT * FROM inspections ORDER BY id'))

    def test_a_food_inspection_saves_a_place_from_deletion(self):
        data = rows()[:1]
        db = sqlite3.connect(':memory:')
        db.executescript(SCHEMA)
        db.execute("INSERT INTO restaurants (id, name, city, source) VALUES ('motel', 'm', 'P', ?)", (MULT,))
        db.execute("INSERT INTO inspections (id, restaurant_id, inspection_type, risk_score, original_score) "
                   "VALUES ('new', 'motel', 'Food Restaurant and B&B 2024', 92, 92)")
        sql, _ = uc.seed_sql(uc.plan(data))
        db.executescript(sql)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM restaurants').fetchone(), (1,))


class FetcherFilterTests(unittest.TestCase):
    def test_portland_metro_drops_lodging_parks_camps_and_pools(self):
        for slug in ('multco-eh', 'or-washington-county', 'or-clackamas-county'):
            exclude = dp._mhd_exclude_patterns(dp.MHD_METRO_JURISDICTIONS[slug])
            for t in ("Traveler's Accommodation 2009", 'Recreational Park 2009', 'Organizational Camp 2020',
                      'Pool 2014'):
                self.assertEqual(dp._mhd_row_rejection({'inspectionType': t}, exclude=exclude), 'excluded')
            for t in ('Food Restaurant and B&B 2024', 'Food Mobile 2024', 'Food School 2013'):
                self.assertEqual(dp._mhd_row_rejection({'inspectionType': t}, exclude=exclude), '')


if __name__ == '__main__':
    unittest.main()
