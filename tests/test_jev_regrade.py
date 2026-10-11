"""scripts/jev_regrade.py with no network: which stored rows may change, the
guarded seed against SQLite, and the data.js rewrite."""
import json
import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), 'scripts'))

import dinescores_pipeline as dp  # noqa: E402
import jev_reader as jr  # noqa: E402
import jev_regrade as rg  # noqa: E402

MONTGOMERY = 'Evidence of rodents or insects observed'
ROACHES = 'Roach observed in the dish area'          # the regex wants the evidence word first
DOOR_GAP = 'INSECTS, RODENTS, & ANIMALS NOT PRESENT: OBSERVED 1/4 INCH GAP AT REAR EXIT DOOR SWEEP.'
COLD = 'Milk held at 48F in the reach-in cooler'


def judged(haz, summ=jr.SUMMARY_NO_MATCH, conf=0.95, cat='pest_control'):
    return {'category': {'choice': cat, 'confidence': conf, 'probabilities': {}},
            'hazard': {'choice': haz, 'confidence': conf, 'probabilities': {}},
            'summary': {'choice': summ, 'confidence': conf, 'probabilities': {}}}


JUDGMENTS = {
    MONTGOMERY: judged('pests_unspecified'),
    ROACHES: judged('vermin_observed', summ='Live roaches found'),
    DOOR_GAP: judged('conditions_only', summ='Gaps/Openings (Risk of entry)'),
    COLD: judged('none', summ='Food not kept cold enough', cat='temperature_control'),
}


def row(lines, grade, ws=95, rs=95, source='Montgomery County Open Data', inf=None, summaries=None,
        rid='r1', date='2026-05-01', updated='2026-05-02T00:00:00+00:00'):
    joined = '|||'.join(lines)
    if inf is None:
        inf = dp.detect_infractions(joined)
    if summaries is None:
        summaries = dp.summarize_violations(joined)
        for s in summaries:
            s['verbatim'] = s['verbatim'][:200]
    return {'id': rid, 'source': source, 'inspection_date': date, 'weighted_score': ws, 'risk_score': rs,
            'vetted_grade': grade, 'updated_at': updated,
            'infractions': json.dumps(inf) if inf else None,
            'summaries': json.dumps(summaries) if summaries else None,
            'latest': {'id': f'{rid}_x', 'violations': json.dumps([['pest_control', 'core', t] for t in lines])}}


class RowTests(unittest.TestCase):
    def test_an_f_the_stored_text_explains_follows_the_new_reading(self):
        self.assertTrue(dp.has_active_pest_or_sewage(MONTGOMERY))      # the regex's F
        r = rg.regrade_row(row([MONTGOMERY, COLD], 'F'), JUDGMENTS)
        self.assertEqual((r['old_vg'], r['new_vg']), ('F', 'A'))
        self.assertIn('pests', r['new_inf'])                           # the warning stays
        self.assertTrue(r['changed'])

    def test_a_named_roach_with_a_firm_answer_adds_the_f(self):
        self.assertFalse(dp.has_active_pest_or_sewage(ROACHES))
        r = rg.regrade_row(row([ROACHES], 'A', inf=[], summaries=[]), JUDGMENTS)
        self.assertEqual((r['old_vg'], r['new_vg']), ('A', 'F'))
        # a stored A the regex would have made an F was not written by the regex: left alone
        r = rg.regrade_row(row(['Live roaches observed under the prep table'], 'A'),
                           {'Live roaches observed under the prep table': judged('vermin_observed')})
        self.assertEqual(r['new_vg'], 'A')
        self.assertIn('f_not_explained_by_stored_text', r['kept'])

    def test_scores_decide_when_they_say_f(self):
        r = rg.regrade_row(row([MONTGOMERY], 'F', ws=65, rs=60), JUDGMENTS)
        self.assertEqual(r['new_vg'], 'F')
        r = rg.regrade_row(row([DOOR_GAP], 'B', ws=84, rs=82), JUDGMENTS)
        self.assertEqual(r['new_vg'], 'B')

    def test_an_f_the_stored_text_cannot_explain_is_kept(self):
        # stored F, but nothing in the stored lines gives the regex an F
        r = rg.regrade_row(row([COLD], 'F'), JUDGMENTS)
        self.assertEqual(r['new_vg'], 'F')
        self.assertIn('f_not_explained_by_stored_text', r['kept'])
        # a stored letter the scores do not give is left alone
        r = rg.regrade_row(row([MONTGOMERY], 'C', ws=95, rs=95), JUDGMENTS)
        self.assertEqual(r['new_vg'], 'C')

    def test_cut_text_keeps_the_grade_chips_and_summaries(self):
        long_line = MONTGOMERY + '. ' + 'x' * 450
        cut = long_line[:rg.LINE_CHARS]
        judgments = dict(JUDGMENTS, **{cut: judged('pests_unspecified')})
        r = rg.regrade_row(row([cut, COLD], 'F', inf=['temp'], summaries=[]), judgments)
        self.assertEqual(r['new_vg'], 'F')
        self.assertIn('f_text_cut', r['kept'])
        self.assertIn('chips_text_cut', r['kept'])
        self.assertEqual((r['new_inf'], r['new_sum']), (['temp'], []))
        # 20 stored lines may be the cap: same
        r = rg.regrade_row(row([MONTGOMERY] + [COLD] * 19, 'F'), JUDGMENTS)
        self.assertIn('f_text_cut', r['kept'])

    def test_feeds_of_standard_wording_count_a_cut_line_as_complete(self):
        sf_long = 'Provide walls / ceilings using materials that are durable, smooth, ' + 'y' * 400
        sf_cut = sf_long[:rg.LINE_CHARS]
        self.assertTrue(rg.complete([sf_cut], 'DataSF'))
        self.assertFalse(rg.complete([sf_cut], 'Chicago Open Data'))
        sf_vermin = ('Eliminate the infestation/activity of cockroaches/rodents/flies/vermin from the food '
                     'facility by using only approved methods. ' + 'z' * 400)[:rg.LINE_CHARS]
        self.assertTrue(rg.complete([sf_vermin], 'Chicago Open Data'))   # a cited item's boilerplate
        self.assertFalse(rg.complete([sf_cut] * 20, 'DataSF'))           # the line cap still counts

    def test_rows_without_violations_or_inspection_are_skipped(self):
        self.assertEqual(rg.regrade_row(row([], 'A'), JUDGMENTS)['skip'], 'no_violations')
        r = row([COLD], 'A')
        r['latest'] = None
        self.assertEqual(rg.regrade_row(r, JUDGMENTS)['skip'], 'no_latest_inspection')

    def test_unchanged_row_is_not_rewritten(self):
        first = rg.regrade_row(row([DOOR_GAP], 'A', inf=None), JUDGMENTS)
        again = row([DOOR_GAP], 'A', inf=first['new_inf'], summaries=first['new_sum'])
        self.assertFalse(rg.regrade_row(again, JUDGMENTS)['changed'])


class SeedTests(unittest.TestCase):
    def test_guarded_updates_against_sqlite(self):
        db = sqlite3.connect(':memory:')
        db.executescript(dp.D1_SCHEMA_SQL)
        stored = [('a', '2026-05-01', 'T1'), ('b', '2026-06-01', 'T2'), ('c', '2026-05-01', 'T9'),
                  ('d', '2026-05-01', None)]
        for rid, date, upd in stored:
            db.execute('INSERT INTO restaurants (id, name, city, inspection_date, vetted_grade, updated_at)'
                       ' VALUES (?,?,?,?,?,?)', (rid, 'n', 'c', date, 'F', upd))
        results = [
            {'id': 'a', 'inspection_date': '2026-05-01', 'updated_at': 'T1', 'changed': True,
             'new_vg': 'A', 'new_inf': ['pests'], 'new_sum': [{'text': "it's", 'verbatim': 'v'}]},
            {'id': 'b', 'inspection_date': '2026-05-01', 'updated_at': 'T1', 'changed': True,   # re-inspected
             'new_vg': 'A', 'new_inf': [], 'new_sum': []},
            {'id': 'c', 'inspection_date': '2026-05-01', 'updated_at': 'T1', 'changed': True,   # rewritten
             'new_vg': 'A', 'new_inf': [], 'new_sum': []},
            {'id': 'd', 'inspection_date': '2026-05-01', 'updated_at': None, 'changed': True,
             'new_vg': 'B', 'new_inf': [], 'new_sum': []},
            {'id': 'e', 'changed': False},
        ]
        sql, n = rg.seed_sql(results)
        self.assertEqual(n, 4)
        db.executescript(sql)
        db.executescript(sql)                               # loading twice changes nothing more
        got = {r[0]: r[1:] for r in db.execute('SELECT id, vetted_grade, infractions, summaries FROM restaurants')}
        self.assertEqual(got['a'], ('A', '["pests"]', '[{"text":"it\'s","verbatim":"v"}]'))
        self.assertEqual(got['b'][0], 'F')
        self.assertEqual(got['c'][0], 'F')
        self.assertEqual(got['d'][0], 'B')

    def test_statements_stay_inside_the_d1_caps(self):
        big = [{'text': 'p', 'verbatim': 'w' * 200, 'severity': 'INFO', 'category': 'Maintenance'}] * 5
        results = [{'id': f'r{i:04d}', 'inspection_date': '2026-05-01', 'updated_at': 'T', 'changed': True,
                    'new_vg': 'A', 'new_inf': ['equipment'], 'new_sum': big} for i in range(500)]
        sql, n = rg.seed_sql(results)
        stmts = [s for s in sql.split(';\n') if s.strip()]
        self.assertEqual(n, 500)
        for s in stmts:
            self.assertLess(len(s.encode('utf-8')), 100_000)
            self.assertLessEqual(s.count("\n('"), dp.D1_BATCH_ROWS)


class DataJsTests(unittest.TestCase):
    def content(self, recs):
        return '/* header */\nwindow.DATA = ' + json.dumps(recs, separators=(',', ':')) + ';\n'

    def test_same_inspection_only_and_unchanged_bytes_otherwise(self):
        recs = [{'i': 'r1', 'd': '2026-05-01', 'ws': 95, 'rs': 95, 'vg': 'F', 'inf': ['pests'], 'vs': []},
                {'i': 'r2', 'd': '2026-07-01', 'ws': 95, 'rs': 95, 'vg': 'F', 'inf': ['pests'], 'vs': []},
                {'i': 'r3', 'd': '2026-05-01', 'ws': 60, 'rs': 95, 'vg': 'F', 'inf': [], 'vs': []}]
        content = self.content(recs)
        same, stats = rg.regrade_data_js(content, {})
        self.assertEqual(same, content)                     # nothing to apply: byte for byte
        r1 = rg.regrade_row(row([MONTGOMERY], 'F', rid='r1'), JUDGMENTS)
        r2 = rg.regrade_row(row([MONTGOMERY], 'F', rid='r2'), JUDGMENTS)
        r3 = rg.regrade_row(row([MONTGOMERY], 'F', rid='r3'), JUDGMENTS)
        new, stats = rg.regrade_data_js(content, {'r1': r1, 'r2': r2, 'r3': r3})
        got = {r['i']: r for r in json.loads(new.split('window.DATA = ', 1)[1].rstrip(';\n'))}
        self.assertEqual(got['r1']['vg'], 'A')
        self.assertEqual(got['r1']['vs'], r1['new_sum'])
        self.assertEqual(got['r2']['vg'], 'F')              # data.js holds a later inspection
        self.assertEqual(got['r3']['vg'], 'F')              # its own weighted score is an F
        self.assertEqual(stats['other_inspection'], 1)
        self.assertTrue(new.startswith('/* header */\n'))

    def test_a_slim_record_stays_slim(self):
        # records past write_data_js's summaries budget carry no 'vs'
        content = self.content([{'i': 'r1', 'd': '2026-05-01', 'ws': 95, 'rs': 95, 'vg': 'F', 'inf': ['pests']}])
        r1 = rg.regrade_row(row([MONTGOMERY], 'F', rid='r1'), JUDGMENTS)
        new, stats = rg.regrade_data_js(content, {'r1': r1})
        rec = json.loads(new.split('window.DATA = ', 1)[1].rstrip(';\n'))[0]
        self.assertNotIn('vs', rec)
        self.assertEqual(rec['vg'], 'A')


if __name__ == '__main__':
    unittest.main()
