"""The grading rules of 2026-11-27: the softer pest rule (dead or trapped
pests and ants cap the grade at C; live pests, droppings and sewage force
the F), the Texas form score for Fort Worth and Frisco, and the agencies'
own verdicts on the older sources. No network."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dinescores_pipeline as dp   # noqa: E402
import jev_reader as jr            # noqa: E402

MINOR = (
    '38: 6-501.112 OBSERVED MULTIPLE DEAD COCKROACHES. SHALL CLEAN',
    'No rodents, insects, birds, or animals (MIN): Observed a dead American cockroach under the storage '
    'shelves. A thorough inspection for vermin activity was conducted',
    'OBSERVED A FEW ROACHES IN STICKY TRAP UNDER SERVING LINE',
    'Small dead mouse noted on a glue board deep in the basement',
    '38: 6-501.111 OBSERVED ANTS BY 3 COMP SINK. SHALL CONTROL PEST',
    'Observed ants crawling at the warewashing sink',
    'Observed one dead cockroach on floor of pantry. Saw no other signs of pest activity.',
    'Observed a dead cockroach on the floor. A thorough inspection for vermin activity did not reveal any',
)
SERIOUS = (
    "Evidence of mice or live mice in establishment's food or non-food areas.",
    'Pests and animals: Observed rodent droppings throughout facility.',
    'OBSERVED A LARGE LIVE ROACH RUN PAST TOWARDS THE 3-COMPARTMENT SINK',
    'Observed two live cockroaches in dishwashing area and under shelving in glue trap',
    'Observed dead roaches and a roach nest behind the cooler',
    'SEWAGE & WASTE WATER PROPERLY DISPOSED: sewage backs up from the floor drain',
    'OSBERVED DEAD AND LIIVE ROACHES',
    'Observed dead pests on shelving. Observed rodent activity below shelves.',
    'There was evidence observed of roach activity(dead insects)',
    # a negated live word still keeps the F: the test errs toward the F
    'Observed dead roaches in the trap; no live roaches seen',
)


class PestRuleTests(unittest.TestCase):

    def test_minor_findings_cap_at_c(self):
        for line in MINOR:
            self.assertTrue(dp.has_active_pest_or_sewage(line), line)
            self.assertEqual(dp.regex_text_cap(line), 'C', line)
            self.assertEqual(dp.calculate_vetted_grade(99, line), 'C', line)
            self.assertEqual(dp.calculate_vetted_grade(75, line), 'C', line)
            self.assertEqual(dp.calculate_vetted_grade(60, line), 'F', line)   # a cap only lowers

    def test_serious_findings_still_force_the_f(self):
        for line in SERIOUS:
            self.assertEqual(dp.regex_text_cap(line), 'F', line)
            self.assertEqual(dp.calculate_vetted_grade(99, line), 'F', line)

    def test_a_flagged_line_naming_no_pest_is_never_minor(self):
        # the reader can flag a sewage line in words the regex does not know;
        # 426 stored F's were read on 2026-11-27 and three such lines (a
        # failing septic system, a backed-up floor drain) came out minor
        for line in ('Sewage and other liquid wastes not disposed through an approved system: '
                     'Septic system is failing in driveway',
                     '51: OBSERVED BACKED UP FLOOR DRAIN NEAR IN PLACE KETTLE POT',
                     'Grease trap overflowing onto the floor'):
            self.assertFalse(dp.pest_finding_is_minor(line), line)
            self.assertEqual(dp.text_cap_of_lines([(line, True)]), 'F', line)
        self.assertTrue(dp.pest_finding_is_minor('Dead insects in the light shield above the prep table'))
        self.assertFalse(dp.pest_finding_is_minor('Live insects on the prep table'))

    def test_one_serious_line_among_minor_ones_is_an_f(self):
        self.assertEqual(dp.regex_text_cap('|||'.join([MINOR[0], 'Floors dirty', SERIOUS[1]])), 'F')
        self.assertEqual(dp.regex_text_cap('|||'.join([MINOR[0], MINOR[3], 'Floors dirty'])), 'C')
        self.assertIsNone(dp.regex_text_cap('Floors dirty|||No paper towels at the hand sink'))
        self.assertEqual(dp.calculate_vetted_grade(95), 'A')

    def test_jev_decision_carries_the_split(self):
        self.assertEqual(dp._text_cap('x', {'forces_f': True}), 'F')            # an old reading: no split
        self.assertEqual(dp._text_cap('x', {'forces_f': True, 'pest_minor': True}), 'C')
        self.assertEqual(dp._text_cap('x', {'forces_f': True, 'pest_minor': False}), 'F')
        self.assertIsNone(dp._text_cap(MINOR[0], {'forces_f': False}))          # Jev said no finding
        self.assertEqual(dp._text_cap(MINOR[0], {}), 'C')                       # no reading: the regex
        self.assertEqual(dp.text_cap_of_lines([(MINOR[0], True), (SERIOUS[0], False)]), 'C')
        self.assertEqual(dp.text_cap_of_lines([(MINOR[0], True), (SERIOUS[0], True)]), 'F')

    def test_the_jev_pass_marks_a_minor_record(self):
        def judged(text):
            return {jr.normalize_text(text): {
                'hazard': {'choice': 'vermin_observed', 'confidence': 0.99, 'probabilities': {}},
                'category': {'choice': 'pest_control', 'confidence': 0.99, 'probabilities': {}},
                'summary': {'choice': 'Live roaches found', 'confidence': 0.99, 'probabilities': {}}}}

        def run(text):
            rec = {'source': 'Dallas Health Dept', 'original_score': 99, 'risk_score': 99,
                   'violations': [['pest_control', 'core', text]]}
            stats = {'decisions': {q: {'jev': 0, 'regex': 0} for q in jr.QUESTIONS},
                     'f_vs_regex': {'agree': 0, 'regex_only': 0, 'jev_only': 0},
                     'retiered_lines': 0, 'rescored_records': 0}
            jr._apply_to_record(rec, rec['violations'], judged(text), {'hazard', 'summary'}, stats, dp)
            return rec['_jev']

        minor = run(MINOR[0])
        self.assertEqual((minor['forces_f'], minor['pest_minor']), (True, True))
        self.assertEqual(dp._text_cap('', minor), 'C')
        serious = run(SERIOUS[2])
        self.assertEqual((serious['forces_f'], serious['pest_minor']), (True, False))
        self.assertEqual(dp._text_cap('', serious), 'F')

    def test_a_kept_record_keeps_its_cap(self):
        fresh = {'d': '2026-11-01', 'rs': 97, 'h': [['2026-11-01', 97]], 'v': [['pest_control', 'core', MINOR[0]]]}
        dp._apply_grades(fresh)
        self.assertEqual((fresh['vg'], fresh['tc']), ('C', 1))
        kept = {'d': '2026-11-01', 'rs': 97, 'h': [['2026-11-01', 97]], 'vg': 'C', 'tc': 1, 'vs': []}
        dp._apply_grades(kept)                       # no text in hand: the stored cap stands
        self.assertEqual((kept['vg'], kept['tc']), ('C', 1))
        plain = {'d': '2026-11-01', 'rs': 97, 'h': [['2026-11-01', 97]], 'vg': 'A', 'vs': []}
        dp._apply_grades(plain)
        self.assertEqual(plain['vg'], 'A')
        self.assertNotIn('tc', plain)
        sticky = {'d': '2026-11-01', 'rs': 97, 'h': [['2026-11-01', 97]], 'vg': 'F', 'vs': []}
        dp._apply_grades(sticky)
        self.assertEqual(sticky['vg'], 'F')
        low = {'d': '2026-11-01', 'rs': 60, 'h': [['2026-11-01', 60]], 'v': [['pest_control', 'core', MINOR[0]]]}
        dp._apply_grades(low)
        self.assertEqual(low['vg'], 'F')
        self.assertNotIn('tc', low)

    def test_same_day_rank_puts_a_capped_inspection_above_a_clean_one(self):
        clean = {'risk_score': 90, 'violations': []}
        capped = {'risk_score': 95, 'violations': [['pest_control', 'core', MINOR[0]]]}
        serious = {'risk_score': 99, 'violations': [['pest_control', 'core', SERIOUS[0]]]}
        self.assertGreater(dp._same_day_rank(capped), dp._same_day_rank(clean))
        self.assertGreater(dp._same_day_rank(serious), dp._same_day_rank(capped))


class TexasFormTests(unittest.TestCase):

    def test_fort_worth_and_frisco_ship_the_form_score(self):
        for key in ('fort-worth-texas', 'frisco'):
            cfg = dp.DFW_JURISDICTIONS[key]
            self.assertEqual((cfg['score_scale'], cfg['tiers']), ('tx-demerit', 'tx-form'))
        row = {'establishmentName': 'Golden Glaze Donut', 'addressLine1': '3300 Western Center Blvd',
               'city': 'Fort Worth', 'state': 'TX', 'inspectionDate': '2026-07-02', 'score': 20,
               'inspectionID': 'X1'}
        rec = dp._parse_mhd_record(row, 'fort-worth-texas', 'Fort Worth', 'Fort Worth', 'TX', 'tx-demerit', 'DFW')
        self.assertEqual((rec['original_score'], rec['risk_score']), (20, 80))    # was 63, an F
        self.assertEqual(jr.score_policy('Fort Worth Health Dept', 20), 'official')
        self.assertEqual(jr.tier_policy('Frisco Health Dept'), 'fixed')

    def test_item_numbers_are_the_tiers(self):
        self.assertEqual(dp._tx_form_tier('9: 1. Food is not protected'), ('priority', 'item'))
        self.assertEqual(dp._tx_form_tier('20: x'), ('priority', 'item'))
        self.assertEqual(dp._tx_form_tier('21: Certified food protection manager'), ('priority_foundation', 'item'))
        self.assertEqual(dp._tx_form_tier('33: 3 compartment sink'), ('priority_foundation', 'item'))
        self.assertEqual(dp._tx_form_tier('34: back door gap'), ('core', 'item'))
        self.assertEqual(dp._tx_form_tier('47: 02. Sign not posted'), ('core', 'item'))
        for odd in ('48: x', '0: x', 'Observed ants', '', None):
            self.assertEqual(dp._tx_form_tier(odd), (None, None))

    def test_a_row_without_a_total_is_scored_from_its_distinct_items(self):
        # Golden Glaze Donut, Fort Worth, 2026-07-02: the city's total was 20
        lines = ['6: a', '9: b', '21: c', '22: d', '29: e', '31: f', '31: g', '36: h', '41: i', '42: j',
                 '44: k', '45: l', '47: m', '47: n']
        self.assertEqual(dp._tx_form_score(lines), 80)
        self.assertEqual(dp._tx_form_score([]), 100)
        self.assertIsNone(dp._tx_form_score(['6: a', 'no item number here']))
        self.assertIs(dp.MHD_FORM_SCORERS['tx-form'], dp._tx_form_score)


class VerdictTests(unittest.TestCase):

    def _chicago(self, results, violations=''):
        return {'dba_name': 'Test Cafe', 'inspection_date': '2026-11-10T00:00:00.000', 'results': results,
                'violations': violations, 'address': '1 N State St', 'inspection_id': results}

    def test_chicago_skips_non_inspections_and_caps_a_fail(self):
        rows = [self._chicago('Out of Business'), self._chicago('No Entry'), self._chicago('Not Ready'),
                self._chicago('Business Not Located'), self._chicago('Pass'),
                self._chicago('Fail', '55. PHYSICAL FACILITIES INSTALLED, MAINTAINED & CLEAN - Comments: FLOOR'),
                self._chicago('Pass w/ Conditions')]
        with mock.patch.object(dp, '_socrata_fetch_pages', return_value=rows):
            out = {r['results']: r for r in dp.fetch_chicago(since_date='2026-11-01')}
        self.assertEqual(sorted(out), ['Fail', 'Pass', 'Pass w/ Conditions'])
        self.assertEqual(out['Fail']['risk_score'], dp.VERDICT_FAIL_RISK_CAP)
        self.assertEqual(dp.calculate_vetted_grade(out['Fail']['risk_score']), 'F')
        self.assertEqual(out['Pass']['risk_score'], 100)
        self.assertEqual(out['Pass w/ Conditions']['risk_score'], 100)

    def test_the_caps_are_an_f_and_a_c(self):
        self.assertEqual(dp.calculate_vetted_grade(dp.VERDICT_FAIL_RISK_CAP), 'F')
        self.assertEqual(dp.calculate_vetted_grade(dp.VERDICT_CONDITIONAL_RISK_CAP), 'C')
        self.assertEqual(dp.SF_VERDICT_CAPS, {'closure': 65, 'conditional pass': 79})

    def test_new_york_closures(self):
        self.assertTrue(dp.NYC_CLOSED_RE.match(
            'Establishment Closed by DOHMH. Violations were cited in the following area(s) and those '
            'requiring immediate action were addressed.'))
        self.assertFalse(dp.NYC_CLOSED_RE.match('Establishment re-opened by DOHMH.'))
        self.assertFalse(dp.NYC_CLOSED_RE.match('Violations were cited in the following area(s).'))

    def test_florida_emergency_orders(self):
        for d in ('Emergency order recommended', 'Emergency Order Callback Not Complied',
                  'Emergency Order Callback Time Extension', 'Facility Temporarily Closed'):
            self.assertTrue(dp.FL_EMERGENCY_ORDER_RE.match(d), d)
        for d in ('Emergency Order Callback Complied', 'Inspection Completed - No Further Action',
                  'Administrative complaint recommended', 'Warning Issued', 'Call Back - Complied'):
            self.assertFalse(dp.FL_EMERGENCY_ORDER_RE.match(d), d)

    def test_non_inspection_results(self):
        self.assertIn('NOT APPLICABLE', dp.SACRAMENTO_NOT_INSPECTED)
        self.assertNotIn('NOT APPLICABLE', dp.SACRAMENTO_RESULT_RISK)
        self.assertIn('HE_OutBus', dp.BOSTON_NOT_INSPECTED)


if __name__ == '__main__':
    unittest.main()
