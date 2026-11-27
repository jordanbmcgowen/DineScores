"""scripts/grading_regrade.py on made-up export rows. No network."""
import json
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, 'scripts'))

import grading_regrade as g   # noqa: E402


def insp(date, risk, results='Pass', lines=(), original=None, iid=None):
    return {'id': iid or 'r_' + date.replace('-', ''), 'inspection_date': date, 'risk_score': risk,
            'original_score': original, 'results': results, 'inspection_type': 'Routine',
            'violations': json.dumps([['other', 'core', t] for t in lines])}


def row(source, insps, grade, ws=None, rid='r'):
    latest = insps[0]
    return {'id': rid, 'name': 'Test Cafe', 'city': 'X', 'source': source, 'inspections': insps,
            'inspection_date': latest['inspection_date'], 'risk_score': latest['risk_score'],
            'weighted_score': latest['risk_score'] if ws is None else ws, 'vetted_grade': grade,
            'updated_at': '2026-11-20T00:00:00+00:00', 'latest': latest}


class HistoryPlanTests(unittest.TestCase):

    def test_fort_worth_takes_the_city_total(self):
        r = row('Fort Worth Health Dept', [insp('2026-10-01', 58, original=21,
                                                lines=['3: Cold holding at 50F', '39: Wiping cloths'])], 'F')
        (it,) = g.plan_history([r], {})
        self.assertEqual(it['new'], {'risk_score': 79, 'weighted_score': 79, 'vetted_grade': 'C'})
        _, old, risk, lines = it['insp_updates'][0]
        self.assertEqual((old, risk), (58, 79))
        self.assertEqual([l[1] for l in lines], ['priority', 'core'])

    def test_chicago_fail_is_capped_and_out_of_business_is_deleted(self):
        fail = row('Chicago Open Data', [insp('2026-10-13', 91, 'Fail')], 'A')
        gone = row('Chicago Open Data', [insp('2026-10-13', 100, 'Out of Business'),
                                         insp('2026-01-05', 95, 'Pass')], 'A', rid='g')
        skipped = row('Chicago Open Data', [insp('2026-10-13', 100, 'No Entry'),
                                            insp('2026-01-05', 85, 'Pass')], 'A', rid='s')
        a, b, c = g.plan_history([fail, gone, skipped], {})
        self.assertEqual(a['new'], {'risk_score': 65, 'weighted_score': 65, 'vetted_grade': 'F'})
        self.assertTrue(b['delete'])
        self.assertFalse(c['delete'])
        self.assertEqual(c['rebuild']['inspection_date'], '2026-01-05')
        self.assertEqual(c['new']['vetted_grade'], 'B')
        sql, n = g.seed_sql([a, b, c], [])
        self.assertIn("DELETE FROM restaurants WHERE id = 'g'", sql)
        self.assertIn("updated_at = '2026-11-20T00:00:00+00:00'", sql)
        self.assertNotIn("DELETE FROM restaurants WHERE id = 's'", sql)

    def test_an_untouched_row_is_not_in_the_plan(self):
        r = row('NYC Open Data', [insp('2026-10-13', 91, 'Violations were cited')], 'A')
        self.assertEqual(g.plan_history([r], {}), [])


class PestPlanTests(unittest.TestCase):

    def _row(self, line, grade='F', risk=95):
        r = row('Dallas Health Dept', [insp('2026-10-13', risk, lines=[line])], grade)
        del r['inspections']
        return r

    def test_dead_roach_becomes_a_c_and_a_live_one_stays(self):
        dead = self._row('38: 6-501.112 OBSERVED MULTIPLE DEAD COCKROACHES. SHALL CLEAN')
        live = self._row('38: OBSERVED DEAD AND LIVE ROACHES')
        plan = g.plan_pests([dead, live], {}, set())
        self.assertEqual([p['new']['vetted_grade'] for p in plan], ['C'])
        self.assertIs(plan[0]['row'], dead)

    def test_an_f_the_text_cannot_explain_stays(self):
        # the line that forced the F is not among the stored ones
        self.assertEqual(g.plan_pests([self._row('Floors dirty')], {}, set()), [])
        # a pest line cut at the storage limit may hide a live finding
        cut = self._row(('OBSERVED DEAD ROACHES ' + 'x' * 400)[:400])
        self.assertEqual(g.plan_pests([cut], {}, set()), [])

    def test_a_low_score_f_is_not_a_text_f(self):
        self.assertEqual(g.plan_pests([self._row('OBSERVED DEAD ROACHES', risk=60)], {}, set()), [])


if __name__ == '__main__':
    unittest.main()
