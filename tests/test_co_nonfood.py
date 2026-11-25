"""scripts/co_nonfood.py: removing the stored places that are not restaurants
from the Colorado Front Range sources (no network). The seed is applied to an
in-memory SQLite copy of the D1 schema to check the guards."""
import json
import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), 'scripts'))

import dinescores_pipeline as dp  # noqa: E402
import co_nonfood as nf  # noqa: E402

EPC, ADAMS, WELD, LARIMER = nf.SOURCES
COLD = '22: Sliced tomatoes were held at 47F in the cold top.'
CFPM = '2: A certified food protection manager was not on staff.'


def insp(rid, date, itype, risk=100, lines=(), source_id=None):
    return {'id': dp.make_inspection_id(rid, date), 'inspection_date': date, 'risk_score': risk,
            'original_score': 10, 'inspection_type': itype, 'results': '',
            'violations': json.dumps([['x', 'priority', t] for t in lines]),
            'source_id': source_id or f'{rid}-{date}'}


def row(rid, source, inspections, vg='A'):
    latest = inspections[0]
    return {'id': rid, 'name': f'Place {rid}', 'city': 'Greeley', 'state': 'CO', 'source': source,
            'inspection_date': latest['inspection_date'], 'original_score': 10,
            'risk_score': latest['risk_score'], 'weighted_score': latest['risk_score'], 'vetted_grade': vg,
            'infractions': '["temp"]', 'summaries': '[]', 'inspection_count': len(inspections),
            'updated_at': '2026-11-14T10:00:00+00:00', 'inspections': inspections}


class IsNonFoodTests(unittest.TestCase):
    def test_the_stored_type_names_the_program(self):
        for t in ('Child Care - Routine', 'Body Art', 'Body Art 2026 NEW', 'Waste Tire Generator',
                  'Waste Tire Hauler', 'Water Recreation', 'Recreation/Pool/Spa'):
            self.assertTrue(nf.is_non_food(EPC, t), t)
        for t in ('Retail Food', 'Routine', 'Re-Inspection', 'Imminent Health Hazard'):
            self.assertFalse(nf.is_non_food(LARIMER, t), t)
        self.assertFalse(nf.is_non_food(EPC, ''), 'only Larimer leaves its child-care rows untyped')
        self.assertFalse(nf.is_non_food('Dallas Health Dept', 'Body Art'), 'only the four mixed portals')

    def test_larimer_untyped_rows_are_child_care_but_for_the_checked_exception(self):
        self.assertTrue(nf.is_non_food(LARIMER, ''))
        self.assertTrue(nf.is_non_food(LARIMER, None, 'SOME-ID'))
        (food_id,) = nf.LARIMER_UNTYPED_FOOD_IDS
        self.assertFalse(nf.is_non_food(LARIMER, '', food_id))
        for t in nf.FOOD_TYPES:
            self.assertFalse(nf.NON_FOOD_TYPE_RE.search(t), t)


class PlanAndSeedTests(unittest.TestCase):
    def setUp(self):
        self.rows = [
            row('tire', EPC, [insp('tire', '2026-10-01', 'Waste Tire Generator', 95)]),
            row('care', LARIMER, [insp('care', '2026-10-02', '', 62, ['26: Toys are sanitized weekly.'])], vg='F'),
            row('cafe', ADAMS, [insp('cafe', '2026-10-03', 'Retail Food', 90, [COLD, COLD])]),
            # a school: the row was built from its child-care visit, the cafeteria was inspected earlier
            row('school', WELD, [insp('school', '2026-10-29', 'Child Care - Routine', 96, ['9: 7.7.1 x']),
                                 insp('school', '2026-10-14', 'Retail Food', 88, [COLD, CFPM, CFPM])]),
            # a school whose row is already on its food inspection
            row('school2', ADAMS, [insp('school2', '2026-10-20', 'Retail Food', 100),
                                   insp('school2', '2026-09-01', 'Body Art', 90)]),
            row('other', 'Douglas County CO Health Dept', [insp('other', '2026-10-05', 'Body Art', 95)]),
        ]
        self.plan = nf.plan(self.rows)
        self.sql, self.counts = nf.seed_sql(self.plan)
        self.db = sqlite3.connect(':memory:')
        self.db.executescript(dp.D1_SCHEMA_SQL)
        for r in self.rows:
            self.db.execute(
                'INSERT INTO restaurants (id, name, city, source, inspection_date, original_score, risk_score,'
                ' weighted_score, vetted_grade, infractions, summaries, inspection_count, source_url, updated_at)'
                ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (r['id'], r['name'], r['city'], r['source'], r['inspection_date'], r['original_score'],
                 r['risk_score'], r['weighted_score'], r['vetted_grade'], r['infractions'], r['summaries'],
                 r['inspection_count'], 'https://portal/latest', r['updated_at']))
            for i in r['inspections']:
                self.db.execute(
                    'INSERT INTO inspections (id, restaurant_id, inspection_date, risk_score, original_score,'
                    ' inspection_type, violations, source_url) VALUES (?,?,?,?,?,?,?,?)',
                    (i['id'], r['id'], i['inspection_date'], i['risk_score'], i['original_score'],
                     i['inspection_type'], i['violations'], f"https://portal/{i['id']}"))

    def _ids(self, table='restaurants'):
        col = 'id' if table == 'restaurants' else 'restaurant_id'
        return sorted({r[0] for r in self.db.execute(f'SELECT {col} FROM {table}')})

    def test_plan(self):
        self.assertEqual(sorted(r['id'] for r in self.plan['delete']), ['care', 'tire'])
        self.assertEqual(sorted(m['row']['id'] for m in self.plan['mixed']), ['school', 'school2'])
        by = {m['row']['id']: m for m in self.plan['mixed']}
        self.assertIsNone(by['school2']['rebuild'])
        nb = by['school']['rebuild']
        self.assertEqual((nb['inspection_date'], nb['risk_score'], nb['weighted_score'], nb['vetted_grade']),
                         ('2026-10-14', 88, 88, 'B'))
        self.assertEqual(nb['history'], [['2026-10-14', 88]])
        self.assertEqual(self.plan['stats']['delete: waste tyre'], 1)
        self.assertEqual(self.plan['stats']['delete: child care'], 1)
        self.assertEqual(self.plan['stats']['delete: graded F'], 1)

    def test_the_seed_removes_them_and_loading_twice_changes_nothing_more(self):
        for stmt in self.sql.split(';\n'):
            self.assertLess(len(stmt.encode()), 100_000)
        self.db.executescript(self.sql)
        self.assertEqual(self._ids(), ['cafe', 'other', 'school', 'school2'])
        self.assertEqual(self._ids('inspections'), ['cafe', 'other', 'school', 'school2'])
        kinds = sorted(r[0] for r in self.db.execute('SELECT inspection_type FROM inspections'))
        self.assertEqual(kinds, ['Body Art', 'Retail Food', 'Retail Food', 'Retail Food'],
                         "another county's body-art row is not this cleanup's business")
        school = self.db.execute('SELECT inspection_date, risk_score, weighted_score, vetted_grade, '
                                 'inspection_count, source_url, updated_at FROM restaurants WHERE id = ?',
                                 ('school',)).fetchone()
        self.assertEqual(school, ('2026-10-14', 88, 88, 'B', 1, 'https://portal/school_20261014',
                                  '2026-11-14T10:00:00+00:00'))
        school2 = self.db.execute('SELECT inspection_date, risk_score, inspection_count FROM restaurants '
                                  'WHERE id = ?', ('school2',)).fetchone()
        self.assertEqual(school2, ('2026-10-20', 100, 1))
        before = self.db.execute('SELECT * FROM restaurants ORDER BY id').fetchall()
        self.db.executescript(self.sql)
        self.assertEqual(self.db.execute('SELECT * FROM restaurants ORDER BY id').fetchall(), before)

    def test_a_place_that_gained_a_food_inspection_is_not_deleted(self):
        self.db.execute("INSERT INTO inspections (id, restaurant_id, inspection_date, risk_score, inspection_type)"
                        " VALUES ('tire_20261120', 'tire', '2026-11-20', 97, 'Retail Food')")
        self.db.executescript(self.sql)
        self.assertIn('tire', self._ids())
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM inspections WHERE restaurant_id = 'tire'").fetchone()[0], 2)
        self.assertNotIn('care', self._ids())

    def test_a_row_the_weekly_run_rewrote_is_not_rebuilt(self):
        self.db.execute("UPDATE restaurants SET updated_at = '2026-11-29T10:00:00+00:00', "
                        "inspection_date = '2026-11-28', risk_score = 91 WHERE id = 'school'")
        self.db.executescript(self.sql)
        got = self.db.execute("SELECT inspection_date, risk_score FROM restaurants WHERE id = 'school'").fetchone()
        self.assertEqual(got, ('2026-11-28', 91))


class DataJsTests(unittest.TestCase):
    def test_deleted_places_leave_and_a_mixed_record_is_rebuilt(self):
        rows = [
            row('tire', EPC, [insp('tire', '2026-10-01', 'Waste Tire Generator', 95)]),
            row('school', WELD, [insp('school', '2026-10-29', 'Child Care - Routine', 96, ['9: 7.7.1 x']),
                                 insp('school', '2026-10-14', 'Retail Food', 88, [COLD, CFPM, CFPM])]),
        ]
        p = nf.plan(rows)
        recs = [{'i': 'tire', 'n': 'Tyres', 'd': '2026-10-01', 'rs': 95, 'ws': 95, 'vg': 'A'},
                {'i': 'school', 'n': 'School', 'd': '2026-10-29', 'rs': 96, 'ws': 94, 'vg': 'A', 'os': 5, 'ic': 2,
                 'inf': ['docs'], 'h': [['2026-10-29', 96], ['2026-10-14', 88]]},
                {'i': 'zz', 'n': 'Cafe', 'd': '2026-10-03', 'rs': 90, 'ws': 90, 'vg': 'A'}]
        content = '/* h */\nwindow.DATA = ' + json.dumps(recs, separators=(',', ':')) + ';\n'
        out, stats = nf.clean_data_js(content, p)
        got = json.loads(out.split('window.DATA = ', 1)[1].rstrip(';\n'))
        self.assertEqual([r['i'] for r in got], ['school', 'zz'])
        self.assertEqual((got[0]['d'], got[0]['rs'], got[0]['ws'], got[0]['vg'], got[0]['ic']),
                         ('2026-10-14', 88, 88, 'B', 1))
        self.assertEqual(got[0]['h'], [['2026-10-14', 88]])
        self.assertEqual(got[1], recs[2])
        self.assertEqual((stats['records removed'], stats['mixed records rebuilt']), (1, 1))


if __name__ == '__main__':
    unittest.main()
