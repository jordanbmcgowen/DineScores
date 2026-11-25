"""scripts/co_regrade.py: the Colorado Front Range regrade on the state's
tiers (no network). The seed is applied to an in-memory SQLite copy of the D1
schema to check the guards."""
import json
import os
import sqlite3
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), 'scripts'))

import dinescores_pipeline as dp  # noqa: E402
import co_regrade as cr  # noqa: E402

CFPM = '2: 2-102.12(A) 2-102.12(A) Certified Food Protection Manager (C). The person in charge shall be certified.'
COLD = '22: 3-501.16 (A)(2) 3-501.16(A)(2) TCS Food, Cold Holding (P). Sliced tomatoes were held at 47F.'
DATE = '23: 3-501.17 3-501.17 Ready-To-Eat TCS Food, Date Marking (Pf). Cooked rice was not date marked.'
WIPE = '41: 3-304.14 Wiping cloths were stored on the cutting board.'
FREE = '10: The hand sink by the grill was blocked by a bucket.'     # item 10: usually Priority Foundation
NOITEM = 'The floor under the fryer was soiled.'                     # no item number: nothing to read
HACCP = '29: The HACCP plan for the sushi rice was not on file.'     # item 29 has no usual tier
ROACH = '38: 6-501.111(A-B), and (D) Live roaches were observed under the dish machine.'


def insp(rid, date, lines, risk=None, results='', itype='Retail Food'):
    """A stored inspection: lines are (stored tier, text) pairs."""
    vio = [['temperature_control', tier, text] for tier, text in lines]
    if risk is None:
        risk = cr._risk(vio)
    return {'id': dp.make_inspection_id(rid, date), 'inspection_date': date, 'risk_score': risk,
            'original_score': 20, 'inspection_type': itype, 'results': results,
            'violations': json.dumps(vio), 'source_id': 'X'}


def row(rid, inspections, ws=None, vg=None, summaries=(), chips=(), source='El Paso County CO Health Dept'):
    latest = inspections[0]
    rs = latest['risk_score']
    ws = rs if ws is None else ws
    if vg is None:
        vg = 'F' if (dp.calculate_vetted_grade(ws) == 'F' or dp.calculate_vetted_grade(rs) == 'F') \
            else dp.calculate_vetted_grade(ws)
    return {'id': rid, 'name': f'Place {rid}', 'city': 'Colorado Springs', 'state': 'CO', 'metro': '',
            'source': source, 'inspection_date': latest['inspection_date'], 'original_score': 20,
            'risk_score': rs, 'weighted_score': ws, 'vetted_grade': vg,
            'infractions': json.dumps(list(chips)), 'summaries': json.dumps(list(summaries)),
            'inspection_count': len(inspections), 'updated_at': '2026-11-14T10:00:00+00:00',
            'inspections': inspections}


class InspectionTests(unittest.TestCase):
    def test_lines_move_to_the_stated_tier_and_the_score_follows(self):
        i = insp('r1', '2026-11-10', [('priority', CFPM), ('priority', COLD), ('priority', DATE),
                                      ('priority_foundation', WIPE), ('priority', FREE)])
        self.assertEqual(i['risk_score'], 100 - 5 - 5 - 5 - 2 - 5)
        out = cr.regrade_inspection(i)
        self.assertEqual([v[1] for v in out['lines']],
                         ['core', 'priority', 'priority_foundation', 'core', 'priority_foundation'])
        self.assertEqual((out['stated'], out['item'], out['retiered'], out['n']), (4, 1, 4, 5))
        self.assertEqual(out['new_risk'], 100 - 1 - 5 - 2 - 1 - 2)
        self.assertTrue(out['changed'])
        self.assertEqual([v[2] for v in out['lines']], [CFPM, COLD, DATE, WIPE, FREE], 'text untouched')

    def test_a_tier_can_go_up(self):
        i = insp('r1', '2026-11-10', [('core', COLD)])
        out = cr.regrade_inspection(i)
        self.assertEqual((out['old_risk'], out['new_risk']), (99, 95))

    def test_a_line_that_states_nothing_takes_its_form_items_usual_tier(self):
        i = insp('r1', '2026-11-10', [('priority', FREE), ('priority', '2: No certified manager on staff.'),
                                      ('core', '22: Sliced tomatoes were held at 47F.')])
        out = cr.regrade_inspection(i)
        self.assertEqual([v[1] for v in out['lines']], ['priority_foundation', 'core', 'priority'])
        self.assertEqual((out['stated'], out['item'], out['retiered']), (0, 3, 3))
        self.assertEqual((out['old_risk'], out['new_risk']), (100 - 5 - 5 - 1, 100 - 2 - 1 - 5))

    def test_nothing_to_read_nothing_moves(self):
        i = insp('r1', '2026-11-10', [('priority', NOITEM), ('priority_foundation', HACCP)])
        out = cr.regrade_inspection(i)
        self.assertEqual((out['changed'], out['lines'], out['new_risk']), (False, None, i['risk_score']))
        self.assertEqual((out['stated'], out['item']), (0, 0))
        clean = cr.regrade_inspection(insp('r1', '2026-11-11', []))
        self.assertEqual((clean['changed'], clean['new_risk']), (False, 100))

    def test_a_restaurant_with_another_programs_inspection_is_left_to_the_cleanup(self):
        care = insp('r1', '2026-11-10', [('priority', '22: 7.9.1(A)(1) Jackets were on the floor.')],
                    itype='Child Care - Routine')
        food = insp('r1', '2026-05-01', [('priority', CFPM)])
        r = cr.regrade_restaurant(row('r1', [care, food], source='Adams County Health Dept'), {})
        self.assertEqual((r['skip'], r['changed'], r['inspections']), ('has_non_food_inspections', False, []))
        # item 22 on the food form is cold holding; on a child-care report it is not
        self.assertEqual(json.loads(care['violations'])[0][1], 'priority')
        untyped = insp('r2', '2026-11-10', [('priority', '26: Toys are sanitized once a week.')], itype='')
        r = cr.regrade_restaurant(row('r2', [untyped], source='Larimer County Health Dept'), {})
        self.assertEqual(r['skip'], 'has_non_food_inspections')
        r = cr.regrade_restaurant(row('r2', [untyped], source='Broomfield Health Dept'), {})
        self.assertIsNone(r['skip'], 'only the four mixed portals carry other programs')

    def test_a_score_the_stored_tiers_do_not_explain_is_left_alone(self):
        i = insp('r1', '2026-11-10', [('priority', CFPM)], risk=80)
        out = cr.regrade_inspection(i)
        self.assertEqual((out['changed'], out['new_risk'], out['note']), (False, 80, 'score_not_from_stored_tiers'))

    def test_at_the_line_cap_the_score_moves_by_the_stored_lines(self):
        lines = [('priority', CFPM)] * 20          # the stored 20 of, say, 24
        i = insp('r1', '2026-11-10', lines, risk=5)
        out = cr.regrade_inspection(i)
        self.assertEqual((out['note'], out['new_risk']), ('at_line_cap', 5 + 20 * 4))
        zero = cr.regrade_inspection(insp('r1', '2026-11-10', lines, risk=0))
        self.assertEqual((zero['changed'], zero['new_risk']), (False, 0), 'a clamped 0 cannot be moved')

    def test_a_closed_inspection_is_capped(self):
        out = cr.regrade_inspection(insp('r1', '2026-11-10', [('priority', COLD)], results='Closed'))
        self.assertEqual((out['old_risk'], out['new_risk'], out['closed'], out['changed']), (95, 65, True, True))
        self.assertIsNone(out['lines'], 'no tier moved: only the score is written')
        low = cr.regrade_inspection(insp('r1', '2026-11-10', [('priority', COLD)] * 9, results='closed'))
        self.assertEqual((low['old_risk'], low['new_risk'], low['changed']), (55, 55, False))
        other = cr.regrade_inspection(insp('r1', '2026-11-10', [('priority', COLD)],
                                           results='Re-Inspection Required'))
        self.assertEqual(other['new_risk'], 95)


class RestaurantTests(unittest.TestCase):
    def test_grade_follows_the_new_scores(self):
        lines = [('priority', CFPM)] * 7                       # 65 on keywords, 93 on the state's tier
        r = cr.regrade_restaurant(row('r1', [insp('r1', '2026-11-10', lines)]), {})
        self.assertEqual((r['old_rs'], r['old_vg'], r['new_rs'], r['new_ws'], r['new_vg']), (65, 'F', 93, 93, 'A'))
        self.assertTrue(r['changed'])

    def test_weighted_score_uses_as_many_inspections_as_the_stored_one_did(self):
        new = insp('r1', '2026-11-10', [('priority', CFPM)] * 2)       # 90 -> 98
        old = insp('r1', '2026-05-01', [('priority', CFPM)] * 6)       # 70 -> 94
        two = dp._weighted_from_history([[None, 90], [None, 70]])
        r = cr.regrade_restaurant(row('r1', [new, old], ws=two), {})
        self.assertEqual((r['k'], r['new_ws']), (2, dp._weighted_from_history([[None, 98], [None, 94]])))
        one = cr.regrade_restaurant(row('r1', [new, old], ws=90), {})
        self.assertEqual((one['k'], one['new_ws']), (1, 98))
        none = cr.regrade_restaurant(row('r1', [new, old], ws=50, vg='F'), {})
        self.assertEqual(none['skip'], 'weighted_score_not_from_history')
        self.assertFalse(none['changed'])

    def test_a_text_forced_f_stays(self):
        lines = [('priority', CFPM), ('core', ROACH)]          # 94 -> 98, graded F for the roaches
        r = cr.regrade_restaurant(row('r1', [insp('r1', '2026-11-10', lines)], vg='F'), {})
        self.assertEqual((r['new_rs'], r['new_vg']), (98, 'F'))

    def test_an_f_the_scores_explained_is_re_read_from_complete_text(self):
        clean = [('priority', CFPM)] * 7
        r = cr.regrade_restaurant(row('r1', [insp('r1', '2026-11-10', clean)]), {})
        self.assertEqual((r['new_vg'], r['kept']), ('A', []))
        with_roach = clean + [('priority', ROACH)]             # 60 -> 92, and roaches in the text
        r = cr.regrade_restaurant(row('r1', [insp('r1', '2026-11-10', with_roach)]), {})
        self.assertEqual((r['old_vg'], r['new_rs'], r['new_vg']), ('F', 92, 'F'))
        self.assertEqual(r['kept'], ['f_kept_pest_or_sewage'])

    def test_cut_text_is_answered_by_the_stored_summaries(self):
        cut = [('priority', CFPM + ' ' + 'x' * 400)] * 7        # a 400-character line: the end is missing
        base = [insp('r1', '2026-11-10', cut)]
        ok = [{'text': 'Chemicals unsafe/unlabeled', 'severity': 'CRITICAL'},
              {'text': 'Improper hand washing', 'severity': 'WARNING'}]
        r = cr.regrade_restaurant(row('r1', base, summaries=ok, chips=['hygiene']), {})
        self.assertEqual((r['new_vg'], r['kept']), ('A', ['f_lifted_on_stored_summaries']))
        roach = ok + [{'text': 'Roaches found', 'severity': 'CRITICAL'}]
        r = cr.regrade_restaurant(row('r1', base, summaries=roach, chips=['pests']), {})
        self.assertEqual((r['new_vg'], r['kept']), ('F', ['f_kept_stored_hazard_summary']))
        r = cr.regrade_restaurant(row('r1', base, summaries=ok, chips=['pests']), {})
        self.assertEqual((r['new_vg'], r['kept']), ('F', ['f_kept_undecidable']), 'a pest chip nothing explains')
        raw = [{'text': '52: 5-402.11 Backflow...', 'severity': 'CRITICAL'}]
        r = cr.regrade_restaurant(row('r1', base, summaries=raw), {})
        self.assertEqual(r['kept'], ['f_kept_undecidable'], 'a hazard-level line with no phrase')
        flies = ok + [{'text': 'Flies/Gnats present', 'severity': 'CRITICAL'}]
        r = cr.regrade_restaurant(row('r1', base, summaries=flies, chips=['pests']), {})
        self.assertEqual(r['new_vg'], 'A', 'flies set the chip and force nothing')

    def test_stored_hazard(self):
        self.assertEqual(cr.stored_hazard({'infractions': '[]', 'summaries': '[]'}), 'none')
        self.assertEqual(cr.stored_hazard({'infractions': 'x', 'summaries': '[]'}), 'unclear')
        for phrase in ('Live roaches found', 'Roaches found', 'Rodent activity detected',
                       'Rodents or droppings found', 'Sewage backup/issue'):
            self.assertEqual(cr.stored_hazard({'infractions': '[]', 'summaries': json.dumps(
                [{'text': phrase, 'severity': 'CRITICAL'}])}), 'forcing', phrase)
        five = [{'text': 'Chemicals unsafe/unlabeled', 'severity': 'CRITICAL'}] * 5
        self.assertEqual(cr.stored_hazard({'infractions': '[]', 'summaries': json.dumps(five)}), 'unclear')

    def test_rows_that_are_not_what_the_export_says_are_skipped(self):
        i = insp('r1', '2026-11-10', [('priority', CFPM)])
        r = row('r1', [i])
        r['risk_score'] = 50                                   # the restaurant row is from another reading
        self.assertEqual(cr.regrade_restaurant(r, {})['skip'], 'latest_inspection_not_the_stored_one')
        r = row('r1', [i], vg='A')
        r['weighted_score'] = r['risk_score'] = i['risk_score'] = 80     # stored A at 80: not from scores
        i['violations'] = json.dumps([['x', 'priority', CFPM]] * 4)
        self.assertEqual(cr.regrade_restaurant(r, {})['skip'], 'grade_not_from_scores')
        self.assertEqual(cr.regrade_restaurant(row('r1', [insp('r1', '2026-11-10', [])]), {})['changed'], False)


class SeedTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.executescript(dp.D1_SCHEMA_SQL)
        self.rows = [
            row('r1', [insp('r1', '2026-11-10', [('priority', CFPM)] * 7),
                       insp('r1', '2026-05-01', [('priority', COLD)])]),
            row('r2', [insp('r2', '2026-11-09', [('priority', NOITEM)])]),               # nothing to read
            row('r3', [insp('r3', '2026-11-08', [('priority', COLD)], results='Closed')]),
        ]
        for r in self.rows:
            self.db.execute(
                'INSERT INTO restaurants (id, name, city, source, inspection_date, risk_score, weighted_score,'
                ' vetted_grade, infractions, summaries, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (r['id'], r['name'], r['city'], r['source'], r['inspection_date'], r['risk_score'],
                 r['weighted_score'], r['vetted_grade'], r['infractions'], r['summaries'], r['updated_at']))
            for i in r['inspections']:
                self.db.execute(
                    'INSERT INTO inspections (id, restaurant_id, inspection_date, risk_score, results, violations)'
                    ' VALUES (?,?,?,?,?,?)',
                    (i['id'], r['id'], i['inspection_date'], i['risk_score'], i['results'], i['violations']))
        self.results = [cr.regrade_restaurant(r, {}) for r in self.rows]
        self.sql, self.counts = cr.seed_sql(self.results)

    def _state(self):
        rest = {r[0]: r[1:] for r in self.db.execute(
            'SELECT id, risk_score, weighted_score, vetted_grade FROM restaurants')}
        ins = {r[0]: (r[1], [v[1] for v in json.loads(r[2])]) for r in self.db.execute(
            'SELECT id, risk_score, violations FROM inspections')}
        return rest, ins

    def test_counts_and_statement_sizes(self):
        self.assertEqual(self.counts['inspections'], 2, 'r1 latest (re-tiered) and r3 (closed); r1 older is unchanged')
        self.assertEqual(self.counts['restaurants'], 2)
        for stmt in self.sql.split(';\n'):
            self.assertLess(len(stmt.encode()), 100_000)
        self.assertNotIn('r2', self.sql)

    def test_the_seed_moves_the_rows_and_loading_twice_changes_nothing_more(self):
        self.db.executescript(self.sql)
        rest, ins = self._state()
        self.assertEqual(rest['r1'], (93, 93, 'A'))
        self.assertEqual(rest['r2'], (95, 95, 'A'))
        self.assertEqual(rest['r3'], (65, 65, 'F'))
        self.assertEqual(ins['r1_20261110'], (93, ['core'] * 7))
        self.assertEqual(ins['r1_20260501'], (95, ['priority']))
        self.assertEqual(ins['r3_20261108'], (65, ['priority']))
        before = self._state()
        self.db.executescript(self.sql)
        self.assertEqual(self._state(), before)

    def test_a_row_the_weekly_run_rewrote_is_left_alone(self):
        self.db.execute("UPDATE restaurants SET updated_at = '2026-11-29T10:00:00+00:00', risk_score = 88, "
                        "weighted_score = 88, vetted_grade = 'B' WHERE id = 'r1'")
        self.db.execute("UPDATE inspections SET risk_score = 88 WHERE id = 'r1_20261110'")
        self.db.executescript(self.sql)
        rest, ins = self._state()
        self.assertEqual(rest['r1'], (88, 88, 'B'))
        self.assertEqual(ins['r1_20261110'][0], 88)
        self.assertEqual(rest['r3'], (65, 65, 'F'), 'the other rows still load')


class DataJsTests(unittest.TestCase):
    def _content(self, recs):
        return '/* header */\nwindow.DATA = ' + json.dumps(recs, separators=(',', ':')) + ';\n'

    def _recs(self, content):
        return json.loads(content.split('window.DATA = ', 1)[1].rstrip(';\n'))

    def test_scores_move_only_where_they_are_the_exported_ones(self):
        new = insp('r1', '2026-11-10', [('priority', CFPM)] * 7)          # 65 -> 93
        old = insp('r1', '2026-05-01', [('priority', CFPM)] * 2)          # 90 -> 98
        res = cr.regrade_restaurant(row('r1', [new, old]), {})
        recs = [
            {'i': 'r1', 'd': '2026-11-10', 'rs': 65, 'ws': 73, 'vg': 'F', 'h': [['2026-11-10', 65], ['2026-05-01', 90]]},
            {'i': 'zz', 'd': '2026-11-10', 'rs': 65, 'ws': 65, 'vg': 'F', 'h': [['2026-11-10', 65]]},
        ]
        content, stats = cr.regrade_data_js(self._content(recs), {'r1': res})
        out = self._recs(content)
        self.assertEqual(out[0]['h'], [['2026-11-10', 93], ['2026-05-01', 98]])
        self.assertEqual((out[0]['rs'], out[0]['ws'], out[0]['vg']),
                         (93, dp._weighted_from_history([['', 93], ['', 98]]), 'A'))
        self.assertEqual(out[1], recs[1], 'another restaurant is untouched')
        self.assertEqual(stats['records_moved'], 1)
        self.assertTrue(content.startswith('/* header */\nwindow.DATA = '))

    def test_a_record_holding_other_scores_is_left_alone(self):
        new = insp('r1', '2026-11-10', [('priority', CFPM)] * 7)
        res = cr.regrade_restaurant(row('r1', [new]), {})
        recs = [{'i': 'r1', 'd': '2026-11-20', 'rs': 70, 'ws': 70, 'vg': 'C', 'h': [['2026-11-20', 70], ['2026-11-10', 64]]}]
        content, stats = cr.regrade_data_js(self._content(recs), {'r1': res})
        self.assertEqual(self._recs(content), recs)
        self.assertEqual(stats['records_moved'], 0)

    def test_a_text_forced_f_in_the_record_stays(self):
        lines = [('priority', CFPM), ('core', ROACH)]
        res = cr.regrade_restaurant(row('r1', [insp('r1', '2026-11-10', lines)], vg='F'), {})
        recs = [{'i': 'r1', 'd': '2026-11-10', 'rs': 94, 'ws': 94, 'vg': 'F', 'h': [['2026-11-10', 94]]}]
        content, _ = cr.regrade_data_js(self._content(recs), {'r1': res})
        out = self._recs(content)[0]
        self.assertEqual((out['rs'], out['ws'], out['vg']), (98, 98, 'F'))


if __name__ == '__main__':
    unittest.main()
