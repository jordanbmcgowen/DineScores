"""Output rules for data.js and D1: the public wording of two summary phrases,
one inspection per restaurant per day (the worst), and a kept record's F.
No network."""
import gzip
import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader as jr           # noqa: E402


ROACH = ('24. No rodents, insects, birds, or animals (MIN): Observed a dead American Cockroach at the '
         'sticky trap below the cooler. Eliminate the activity of cockroaches from the food facility.')
DEBRIS = '45. Floors, walls and ceilings: built, maintained, and clean (OUT): Observed debris at the floor.'
DROPPINGS = 'No rodents, insects, birds, or animals: Rodent droppings observed in the boiler room in the basement.'
FLIES = ('No rodents, insects, birds, or animals: 1. Observed Vermin: Documented in the following areas: '
         '- Flies on produce - Flies throughout facility.')


def inspection(text, rs, source_id, date='2026-10-07', lat=33.81, lng=-117.92, name='Disneyland'):
    return {'name': name, 'address': '1313 S Harbor Blvd', 'city': 'Anaheim', 'state': 'CA',
            'zip': '92802', 'latitude': lat, 'longitude': lng, 'metro': 'Orange County',
            'inspection_date': date, 'risk_score': rs, 'original_score': None,
            'source': 'Orange County Health Care Agency', 'source_id': source_id,
            'source_url': f'https://example.test/{source_id}', 'inspection_type': 'Routine',
            'results': 'Pass', 'violations': [['general_cleanliness', 'core', text]],
            'total_violations': 1, 'priority_violations': 0}


def read_js(path):
    with open(path) as f:
        return json.loads(f.read().split('window.DATA = ', 1)[1].rstrip(';\n'))


class SummaryWordingTests(unittest.TestCase):

    def test_two_phrases_have_new_public_wording(self):
        texts = [s['text'] for s in dp.summarize_violations(
            'Observed a live roach under the sink|||Rodent droppings found in dry storage')]
        self.assertIn('Roaches found', texts)
        self.assertIn('Rodents or droppings found', texts)
        self.assertNotIn('Live roaches found', texts)
        # Jev still picks from the unchanged phrase list (cached answers stay valid);
        # only what the site shows changes.
        self.assertIn('Live roaches found', jr.SUMMARY_PHRASES)
        j = {jr.normalize_text(ROACH): {
            'category': {'choice': 'pest_control', 'confidence': 1.0},
            'hazard': {'choice': 'vermin_observed', 'confidence': 1.0},
            'summary': {'choice': 'Live roaches found', 'confidence': 1.0}}}
        self.assertEqual(jr.summarize_violations([ROACH], j)[0]['text'], 'Roaches found')
        self.assertEqual(dp.display_summary_text('Roaches found'), 'Roaches found')   # idempotent

    def test_records_kept_from_earlier_runs_are_renamed_in_data_js(self):
        d = tempfile.mkdtemp()
        old = [{'n': 'Old Diner', 'a': '1 Main St', 'c': 'Austin', 'd': '2026-10-01', 'rs': 92,
                'h': [['2026-10-01', 92]], 'vg': 'F', 'i': 'old1', 'lt': 30.2, 'ln': -97.7,
                'vs': [{'text': 'Live roaches found', 'verbatim': 'Dead roach in trap', 'severity': 'CRITICAL',
                        'category': 'Active Hazard'},
                       {'text': 'Rodent activity detected', 'verbatim': 'Droppings', 'severity': 'CRITICAL',
                        'category': 'Active Hazard'}]}]
        with open(f'{d}/old.js', 'w') as f:
            f.write('window.DATA = ' + json.dumps(old) + ';\n')
        dp.write_data_js([], f'{d}/new.js', merge_from=f'{d}/old.js')
        texts = [s['text'] for s in read_js(f'{d}/new.js')[0]['vs']]
        self.assertEqual(texts, ['Roaches found', 'Rodents or droppings found'])

    def test_d1_seed_renames_stored_summaries(self):
        path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            'data', 'd1_seed_2026-10-10_summary_display_names.sql.gz')
        db = sqlite3.connect(':memory:')
        db.execute('CREATE TABLE restaurants (id TEXT PRIMARY KEY, summaries TEXT)')
        db.executemany('INSERT INTO restaurants VALUES (?, ?)', [
            ('a', json.dumps([{'text': 'Live roaches found', 'verbatim': 'Live roaches found'}], separators=(',', ':'))),
            ('b', json.dumps([{'text': 'Rodent activity detected', 'verbatim': 'x'}])),
            ('c', None)])
        sql = gzip.open(path, 'rt').read()
        db.executescript(sql)
        db.executescript(sql)                       # idempotent
        rows = dict(db.execute('SELECT id, summaries FROM restaurants'))
        self.assertEqual(json.loads(rows['a'])[0], {'text': 'Roaches found', 'verbatim': 'Live roaches found'})
        self.assertEqual(json.loads(rows['b'])[0]['text'], 'Rodents or droppings found')
        self.assertIsNone(rows['c'])


class SameDayTests(unittest.TestCase):

    def setUp(self):
        self.clean = inspection(DEBRIS, 98, '059E8AF0')
        self.roach = inspection(ROACH, 90, 'F9464360')

    def test_data_js_shows_the_worst_inspection_of_the_day_in_any_order(self):
        d = tempfile.mkdtemp()
        for n, order in enumerate(([self.clean, self.roach], [self.roach, self.clean])):
            out = f'{d}/out{n}.js'
            dp.write_data_js([dict(i) for i in order], out)
            r = read_js(out)[0]
            self.assertEqual(r['vg'], 'F')
            self.assertEqual(r['url'], 'https://example.test/F9464360')
            self.assertEqual(r['h'], [['2026-10-07', 90]])
            self.assertEqual(r['ic'], 1)
            self.assertEqual(r['vs'][0]['text'], 'Roaches found')

    def test_a_later_clean_day_still_wins_over_an_older_bad_one(self):
        d = tempfile.mkdtemp()
        later = inspection(DEBRIS, 98, 'NEW', date='2026-10-09')
        dp.write_data_js([self.roach, later], f'{d}/out.js')
        r = read_js(f'{d}/out.js')[0]
        self.assertEqual(r['d'], '2026-10-09')
        self.assertEqual([h[0] for h in r['h']], ['2026-10-09', '2026-10-07'])

    def test_coordinates_come_from_a_sibling_when_the_worst_has_none(self):
        d = tempfile.mkdtemp()
        roach = inspection(ROACH, 90, 'F9464360', lat=None, lng=None)
        dp.write_data_js([self.clean, roach], f'{d}/out.js')
        r = read_js(f'{d}/out.js')[0]
        self.assertEqual((r['lt'], r['ln']), (33.81, -117.92))
        sql_path = f'{d}/out.sql'
        dp.write_d1_sql([self.clean, roach], sql_path, include_schema=False)
        self.assertIn('33.81,-117.92', open(sql_path).read())

    def test_d1_keeps_the_worst_inspection_of_the_day_in_any_order(self):
        d = tempfile.mkdtemp()
        for n, order in enumerate(([self.clean, self.roach], [self.roach, self.clean])):
            path = f'{d}/out{n}.sql'
            dp.write_d1_sql([dict(i) for i in order], path, include_schema=False)
            sql = open(path).read()
            self.assertIn('F9464360', sql)
            self.assertNotIn('059E8AF0', sql)
            self.assertIn("'F'", sql)

    def test_records_kept_from_earlier_runs_keep_their_history(self):
        # The rule needs the inspections themselves; an older record's history
        # is left as it is until the restaurant is fetched again, so its card
        # and its grade keep describing the same inspection.
        d = tempfile.mkdtemp()
        old = [{'n': 'Grocer', 'a': '2 Elm St', 'c': 'Nashville', 'd': '2026-10-07', 'rs': 95, 'vg': 'B',
                'h': [['2026-10-07', 95], ['2026-10-07', 70], ['2026-08-01', 95]], 'i': 'g1', 'lt': 36.1, 'ln': -86.8}]
        with open(f'{d}/old.js', 'w') as f:
            f.write('window.DATA = ' + json.dumps(old) + ';\n')
        dp.write_data_js([], f'{d}/new.js', merge_from=f'{d}/old.js')
        r = read_js(f'{d}/new.js')[0]
        self.assertEqual(r['h'], old[0]['h'])
        self.assertEqual(r['vg'], 'B')                    # today's weighting, unchanged


class KeptRecordGradeTests(unittest.TestCase):

    def two_weeks(self, text, forces_f):
        d = tempfile.mkdtemp()
        rec = inspection(text, 95, 'X', name='Test Kitchen')
        rec['_jev'] = {'forces_f': forces_f, 'chips': None, 'summaries': None}
        dp.write_data_js([rec], f'{d}/week1.js')
        dp.write_data_js([], f'{d}/week2.js', merge_from=f'{d}/week1.js')
        return read_js(f'{d}/week1.js')[0]['vg'], read_js(f'{d}/week2.js')[0]['vg']

    def test_a_jev_f_survives_the_next_weekly_rebuild(self):
        self.assertFalse(dp.has_active_pest_or_sewage(DROPPINGS))        # the regex misses it
        self.assertEqual(self.two_weeks(DROPPINGS, True), ('F', 'F'))

    def test_a_regex_f_jev_removed_does_not_come_back(self):
        self.assertTrue(dp.has_active_pest_or_sewage(FLIES))             # the regex fires on it
        self.assertEqual(self.two_weeks(FLIES, False), ('A', 'A'))

    def test_score_rules_still_reapply_to_kept_records(self):
        d = tempfile.mkdtemp()
        old = [{'n': 'Low Score', 'a': '3 Oak St', 'c': 'Austin', 'd': '2026-10-01', 'rs': 65, 'vg': 'C',
                'h': [['2026-10-01', 65]], 'i': 'l1', 'lt': 30.2, 'ln': -97.7}]
        with open(f'{d}/old.js', 'w') as f:
            f.write('window.DATA = ' + json.dumps(old) + ';\n')
        dp.write_data_js([], f'{d}/new.js', merge_from=f'{d}/old.js')
        self.assertEqual(read_js(f'{d}/new.js')[0]['vg'], 'F')


if __name__ == '__main__':
    unittest.main()
