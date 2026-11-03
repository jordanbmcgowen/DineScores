"""The text-F rule: code titles that list every kind of pest never force an F
on their own, a finding written after them still does, and droppings written
pest-first count unless negated. Also state-name normalisation and the
records-file contract."""
import gzip
import json
import os
import tempfile
import unittest

import dinescores_pipeline as dp
import jev_reader as jr


class ListedPestTitleTests(unittest.TestCase):
    def test_listed_titles_alone_do_not_force_an_f(self):
        for text in ('34. No Evidence of Insect contamination, rodent/other animals',          # Harris, older TFER form
                     'No evidence of Insect Contamination, Rodent, Other Animals',              # TFER wording
                     '45: 3717-1-06.4(K) Presence of live insects, rodents, and other pests.',  # Ohio (Cuyahoga)
                     'Presence of live insects, rodents, and other pests: Facility has gaps under the rear door.',
                     '45: 3717-1-06.4(K) Presence of live insects, rodents, and other pests. Observed gnats near the drain.'):
            self.assertFalse(dp.has_active_pest_or_sewage(text), text)

    def test_a_finding_after_the_title_keeps_its_f(self):
        for text in ('Presence of live insects, rodents, and other pests: Observed one live roach on the wall by the dish machine.',
                     'No Evidence of Insect contamination, rodent/other animals: rodent droppings were observed under the shelves',
                     '45: 3717-1-06.4(K) Presence of live insects, rodents, and other pests. Observed apparent mouse droppings in the corner.'):
            self.assertTrue(dp.has_active_pest_or_sewage(text), text)

    def test_droppings_written_pest_first(self):
        self.assertTrue(dp.has_active_pest_or_sewage('Rodent droppings on the floor inside the customer service area.'))
        self.assertTrue(dp.has_active_pest_or_sewage('Pests and animals: Rodent feces found in several lower cabinets.'))
        self.assertTrue(dp.has_active_pest_or_sewage('Clean up rodent droppings. 6-501.111(B) Controlling pests.'))
        for text in ('Pests and animals: Did not observe any rodent droppings on the shelving upstairs.',
                     'Dry storage is free of mouse droppings; keep it organized.',
                     'Remove clutter to prevent rodent droppings from accumulating.',
                     'No rat droppings were found during the inspection.'):
            self.assertFalse(dp.has_active_pest_or_sewage(text), text)

    def test_evidence_first_rule_is_unchanged(self):
        self.assertTrue(dp.has_active_pest_or_sewage('Observed live roaches under the prep table.'))
        self.assertFalse(dp.has_active_pest_or_sewage('INSECTS, RODENTS, & ANIMALS NOT PRESENT'))
        self.assertFalse(dp.has_active_pest_or_sewage(
            'Dead or trapped birds, insects, rodents, and other pests shall be removed from control devices'))

    def test_the_older_item_34_title_printed_bare_is_a_cited_pest_item(self):
        self.assertEqual(jr.cited_pest_item('34. No Evidence of Insect contamination, rodent/other animals'), 'pests_unspecified')
        self.assertIsNone(jr.cited_pest_item(
            'No Evidence of Insect contamination, rodent/other animals: Gnats present at the bar handsink'))


class StateCodeTests(unittest.TestCase):
    def test_spelled_out_states_become_codes(self):
        self.assertEqual(dp.state_code('VIRGINIA'), 'VA')
        self.assertEqual(dp.state_code(' Texas '), 'TX')
        self.assertEqual(dp.state_code('va'), 'VA')
        self.assertEqual(dp.state_code('District of Columbia'), 'DC')
        self.assertEqual(dp.state_code(''), '')
        self.assertEqual(dp.state_code('ONTARIO'), 'ONTARIO')

    def _rec(self, state):
        return {'name': 'Ice Zone', 'address': '636 Johnston Willis Dr', 'city': 'North Chesterfield',
                'state': state, 'zip': '23236', 'latitude': 37.5, 'longitude': -77.6,
                'inspection_date': '2026-10-20', 'original_score': None, 'risk_score': 95,
                'priority_violations': 0, 'priority_foundation_violations': 0, 'core_violations': 1,
                'total_violations': 1, 'violations': [['general', 'core', 'Floor not clean']],
                'inspection_type': 'Routine', 'results': '', 'source': 'VDH Chesterfield Health Dept',
                'source_url': '', 'source_id': 'x1', 'metro': 'Richmond'}

    def test_both_output_paths_write_the_code(self):
        with tempfile.TemporaryDirectory() as d:
            js, sql = os.path.join(d, 'data.js'), os.path.join(d, 'out.sql')
            dp.write_data_js([self._rec('VIRGINIA')], js)
            dp.write_d1_sql([self._rec('VIRGINIA')], sql)
            data = json.loads(open(js).read().split('window.DATA = ', 1)[1].rstrip(';\n'))
            self.assertEqual(data[0]['s'], 'VA')
            text = open(sql).read()
            self.assertIn("'VA'", text)
            self.assertNotIn("'VIRGINIA'", text)

    def test_a_kept_record_heals_on_the_next_write(self):
        with tempfile.TemporaryDirectory() as d:
            js = os.path.join(d, 'data.js')
            dp.write_data_js([self._rec('VIRGINIA')], js)
            body = open(js).read().replace('"s":"VA"', '"s":"VIRGINIA"')   # an old file with the spelled-out state
            open(js, 'w').write(body)
            other = dict(self._rec('VA'), name='Other Place', source_id='x2')
            dp.write_data_js([other], js, merge_from=js)
            data = json.loads(open(js).read().split('window.DATA = ', 1)[1].rstrip(';\n'))
            self.assertEqual({r['s'] for r in data}, {'VA'})


class RecordsContractTests(unittest.TestCase):
    def _write(self, d, recs):
        path = os.path.join(d, 'r.json.gz')
        with gzip.open(path, 'wt') as f:
            json.dump(recs, f)
        return path

    def test_empty_address_or_city_is_accepted_empty_name_is_not(self):
        base = StateCodeTests()._rec('VA')
        with tempfile.TemporaryDirectory() as d:
            ok = self._write(d, [dict(base, address='', city='')])
            self.assertEqual(len(dp.read_records_json([ok])), 1)
            bad = self._write(d, [dict(base, name='')])
            with self.assertRaises(ValueError):
                dp.read_records_json([bad])
            bad = self._write(d, [dict(base, risk_score=True)])
            with self.assertRaises(ValueError):
                dp.read_records_json([bad])


if __name__ == '__main__':
    unittest.main()
