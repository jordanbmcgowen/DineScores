"""Tests for the McKinney, TX fetcher (city ArcGIS MapServer, demerit points
per TX form item). No network: requests.get is mocked with trimmed captures
from the live server taken 2026-11-03 (tests/fixtures/mckinney/): ten
layer-4 inspection rows (outSR 4326; one inspection appears twice, once per
PDF attachment, as the layer serves it), the seven layer-0 permits they
reference plus a mobile unit and a splash pad, two layer-3 school permits
and an empty layer-1 answer. Everything is verbatim except the certified
food manager's personal name in each report header, replaced by 'Jane Doe'
(the parser must never read that line anyway). The comment layouts the
parser tests quote are shortened copies of live reports (FOOD-015857,
FOOD-025293, FOOD-030012, FOOD-010797, FOOD-032570 of 2026)."""
import json
import logging
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'mckinney')


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


class _Resp:
    def __init__(self, payload, status=200, text=None, ctype='application/json; charset=UTF-8'):
        self._payload = payload
        self.status_code = status
        self.headers = {'content-type': ctype}
        self.text = text if text is not None else json.dumps(payload)
        self.content = self.text.encode()

    def json(self):
        if self._payload is None:
            raise ValueError('no JSON')
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            err = dp.requests.HTTPError(f'{self.status_code}')
            err.response = self
            raise err


def _epoch_ms(date_str, hour=12):
    d = datetime.strptime(date_str, '%Y-%m-%d').replace(hour=hour, tzinfo=timezone.utc)
    return int(d.timestamp() * 1000)


def _page_dated(date_str):
    """The fixture page with every inspection moved to `date_str`."""
    page = _load('inspections_page.json')
    for f in page['features']:
        f['attributes']['actual_end_date'] = _epoch_ms(date_str)
    return page


def _page_with(number, **attrs):
    """The fixture page with one inspection's attributes changed."""
    page = _load('inspections_page.json')
    for f in page['features']:
        if f['attributes']['inspection_number'] == number:
            f['attributes'].update(attrs)
    return page


TODAY = datetime.now().strftime('%Y-%m-%d')


def run_fetch(since=None, limit=None, page=None, count=None, permits=None, schools=None,
              answers=None):
    """fetch_mckinney against the fixtures. `answers` maps a layer suffix
    ('4/count', '4', '0', '1', '2', '3') to a _Resp, an exception to raise or
    a list of those served in order, overriding the fixture answer."""
    page = page if page is not None else _load('inspections_page.json')
    count = count if count is not None else _load('count.json')
    permits = permits if permits is not None else _load('permits_page.json')
    schools = schools if schools is not None else _load('schools_layer.json')
    answers = dict(answers or {})
    calls = []

    def serve(key, default):
        override = answers.get(key)
        if isinstance(override, list):
            override = override.pop(0) if override else None
        if isinstance(override, Exception):
            raise override
        return override if override is not None else default

    def fake_get(url, params=None, headers=None, timeout=None, verify=True, **kw):
        params = dict(params or {})
        calls.append((url, params))
        layer = url[len(dp.MCK_BASE) + 1:].split('/')[0]
        if layer == '4' and params.get('returnCountOnly'):
            return serve('4/count', _Resp(count))
        if layer == '4' and str(params.get('orderByFields', '')).startswith('actual_end_date'):
            return serve('4/newest', _Resp({'features': []}))
        if layer == '4':
            return serve('4', _Resp(page))
        if layer == '0':
            return serve('0', _Resp(permits))
        if layer == '3':
            return serve('3', _Resp(schools))
        if layer in ('1', '2'):
            return serve(layer, _Resp(_load('empty_layer.json')))
        raise AssertionError(f'unexpected request {url}')

    with mock.patch.object(dp.requests, 'get', side_effect=fake_get), \
            mock.patch.object(dp.time, 'sleep'):
        records = dp.fetch_mckinney(since_date=since, limit=limit)
    return records, calls


def _by_id(records):
    return {r['source_id']: r for r in records}


def _snapshot():
    return (dict(dp._GROUP_NOTES), dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
            dp._FETCH_DEADLINE, dp.log.level)


def _restore(saved):
    dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(saved[0])
    dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(saved[1])
    dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(saved[2])
    dp._FETCH_DEADLINE = saved[3]
    dp.log.setLevel(saved[4])


class McKinneyBase(unittest.TestCase):
    def setUp(self):
        self._saved = _snapshot()
        dp._GROUP_NOTES.clear()
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._FETCH_DEADLINE = None
        dp.log.setLevel(logging.WARNING)

    def tearDown(self):
        _restore(self._saved)


class RequestShapeTests(McKinneyBase):
    def test_count_then_page_then_the_permit_layers(self):
        _, calls = run_fetch(since='2026-10-01T00:00:00')
        self.assertEqual([c[0].rsplit('/MapServer/', 1)[1] for c in calls],
                         ['4/query', '4/query', '0/query', '1/query', '2/query', '3/query'])
        count, page = calls[0][1], calls[1][1]
        self.assertEqual(count['where'], "actual_end_date >= DATE '2026-10-01'")
        self.assertEqual(count['returnCountOnly'], 'true')
        self.assertEqual(page['where'], "actual_end_date >= DATE '2026-10-01'")
        self.assertEqual(page['outSR'], '4326')               # WGS84 geometry, not State Plane
        self.assertEqual(page['orderByFields'], 'OBJECTID')
        self.assertEqual(page['resultRecordCount'], dp.MCK_PAGE)
        self.assertEqual(page['resultOffset'], 0)
        self.assertNotIn('inspector_name', page['outFields'])  # never requested
        for col, _ in dp.MCK_ITEMS:
            self.assertIn(col, page['outFields'])

    def test_since_date_in_both_formats(self):
        for since in ('2026-10-04', '2026-10-04T00:00:00'):
            _, calls = run_fetch(since=since)
            self.assertEqual(calls[1][1]['where'], "actual_end_date >= DATE '2026-10-04'", since)

    def test_no_since_means_the_history_floor_or_a_bounded_test_window(self):
        _, calls = run_fetch(since=None)
        self.assertEqual(calls[1][1]['where'], f"actual_end_date >= DATE '{dp.MCK_HISTORY_START}'")
        _, calls = run_fetch(since=None, limit=25)
        expected = (datetime.now() - timedelta(days=dp.MCK_TEST_WINDOW_DAYS)).strftime('%Y-%m-%d')
        self.assertEqual(calls[1][1]['where'], f"actual_end_date >= DATE '{expected}'")

    def test_window_is_reapplied_to_the_rows(self):
        records, _ = run_fetch(since='2026-10-20')
        self.assertEqual(sorted(_by_id(records)),
                         ['mck_FOOD-027854-2026', 'mck_FOOD-028293-2026', 'mck_FOOD-034691-2026'])
        self.assertTrue(all(r['inspection_date'] >= '2026-10-20' for r in records))


class RecordTests(McKinneyBase):
    @classmethod
    def setUpClass(cls):
        saved = _snapshot()                      # the class-level fetch leaves no note or hold behind
        try:
            cls.records, _ = run_fetch(since='2026-10-01')
        finally:
            _restore(saved)
        cls.by_id = _by_id(cls.records)

    def test_scored_record_whole_dict(self):
        r = self.by_id['mck_FOOD-034691-2026']
        self.assertEqual(r, {
            'name': 'Square Burger',
            'address': '115 N Kentucky St',
            'city': 'Mckinney',                    # the label the Plano portal's rows already carry
            'state': 'TX',
            'zip': '75069',
            'latitude': 33.197859303327235,
            'longitude': -96.61599092159656,
            'inspection_date': '2026-10-30',
            'original_score': 13,                  # the city's demerit total, never a 0-100 score
            'risk_score': 87,                      # the TX form score: 100 - 13
            'priority_violations': 2,
            'priority_foundation_violations': 1,
            'core_violations': 5,
            'total_violations': 8,
            'violations': r['violations'],
            'inspection_type': 'Routine',
            'results': '13 demerits (Approved)',
            'source': 'McKinney Health Inspections',
            'source_url': 'https://maps.mckinneytexas.org/mckinney/rest/services/MapServices/'
                          'FoodPermitsAndInspections/MapServer/4/query?where=inspection_number'
                          '%3D%27FOOD-034691-2026%27&outFields=%2A&f=html',
            'source_id': 'mck_FOOD-034691-2026',
            'metro': 'DFW',
        })
        self.assertEqual(len(r['violations']), 8)
        self.assertTrue(all(len(v) == 3 for v in r['violations']))
        self.assertTrue(all(v[1] in ('priority', 'priority_foundation', 'core') for v in r['violations']))
        self.assertTrue(all(len(v[2]) <= 500 and '\n' not in v[2] for v in r['violations']))
        # The tiers add up to the city's total (3 / 2 / 1 points), the score is not recomputed.
        _, pv, pfv, cv = dp.calc_risk_score(r['violations'])
        self.assertEqual(3 * pv + 2 * pfv + cv, r['original_score'])
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'B')

    def test_points_set_the_tier_and_the_title_prefixes_the_observation(self):
        sev = {v[2].split(':')[0]: v[1] for v in self.by_id['mck_FOOD-034691-2026']['violations']}
        self.assertEqual(sev['Food separated and protected'], 'priority')                      # item 9, 3 pts
        self.assertEqual(sev['Toxic substances properly identified, stored and used'], 'priority')  # 18
        self.assertEqual(sev['Food and non-food contact surfaces cleanable, properly designed and used'],
                         'priority_foundation')                                                    # 32, 2 pts
        self.assertEqual(sev['Wiping cloths properly used and stored'], 'core')                   # 36, 1 pt
        texts = [v[2] for v in self.by_id['mck_FOOD-034691-2026']['violations']]
        self.assertIn('Food separated and protected: Observed raw chicken over raw eggs. '
                      'Separate according to cook temperature. Food must be separated and '
                      'protected to limit contamination.', texts)
        # Sub-observations of one item are joined into that item's single line.
        self.assertTrue(any(t.startswith('Non-food contact surfaces clean: Observed accumulation')
                            and 'Observed rust on air vents' in t for t in texts))
        for cat, _, text in self.by_id['mck_FOOD-034691-2026']['violations']:
            self.assertEqual(cat, dp.classify_violation(text)[0])

    def test_clean_inspection_ships_as_100_with_no_lines(self):
        r = self.by_id['mck_FOOD-029818-2026']                  # Jim's Pizza, 0 demerits
        self.assertEqual((r['original_score'], r['risk_score'], r['violations'],
                          r['total_violations']), (0, 100, [], 0))
        self.assertEqual(r['results'], '0 demerits (Approved)')

    def test_demerits_are_inverted_like_the_tx_form_and_the_tiers_counted(self):
        r = self.by_id['mck_FOOD-027854-2026']                  # Zaza Thai, 24 demerits
        self.assertEqual(r['original_score'], 24)
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'],
                          r['core_violations']), (3, 5, 5))
        self.assertEqual(r['risk_score'], 100 - 24)             # not 100 - 5P - 2PF - C (= 70)
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'C')
        # Item 34 (pests) carries the inspector's words alone, no title; both of
        # its observations are kept, and a door gap plus flies is not a forced F.
        pest = [v for v in r['violations'] if v[0] == 'pest_control']
        self.assertEqual(len(pest), 1)
        self.assertEqual(pest[0][1], 'priority_foundation')
        self.assertTrue(pest[0][2].startswith('Observed back door open.'))
        self.assertIn(' | Observed flies in kitchen.', pest[0][2])
        self.assertFalse(dp.has_active_pest_or_sewage(pest[0][2]))

    def test_no_header_layout_is_read(self):
        r = self.by_id['mck_FOOD-020026-2026']                  # Anejo: '45 - text' lines, no header
        self.assertEqual((r['original_score'], r['risk_score']), (3, 97))
        self.assertEqual([v[2] for v in r['violations']], [
            'Food and non-food contact surfaces cleanable, properly designed and used: Observed '
            'dishwasher not attached to wall. Reseal to wall and remove old sealant',
            'Physical facilities installed, maintained and clean: Observed low grout. Raise to level',
        ])

    def test_duplicate_attachment_rows_collapse_to_one_inspection(self):
        self.assertEqual(sum(1 for r in self.records if r['source_id'] == 'mck_FOOD-029563-2026'), 1)
        self.assertEqual(len(self.records), 7)

    def test_permit_layer_supplies_coordinates_for_rows_without_geometry(self):
        r = self.by_id['mck_FOOD-027470-2026']                  # Starbuck's: no geometry on layer 4
        self.assertAlmostEqual(r['latitude'], 33.215470012702504)
        self.assertAlmostEqual(r['longitude'], -96.63839545106617)
        r = self.by_id['mck_FOOD-028293-2026']
        self.assertAlmostEqual(r['longitude'], -96.637265852607)

    def test_names_and_addresses_are_normalised(self):
        self.assertEqual(self.by_id['mck_FOOD-028293-2026']['name'], 'Asian Buffet')    # repeated 5x in the layer
        self.assertEqual(self.by_id['mck_FOOD-027470-2026']['name'], "Starbuck's Coffee #6416")
        self.assertEqual(self.by_id['mck_FOOD-029563-2026']['name'], "Arby's #5387")   # mixed case kept
        self.assertEqual(self.by_id['mck_FOOD-020026-2026']['address'], '4150 Eldorado Pkwy 500')
        self.assertEqual(dp._mck_clean_name('ASIAN BUFFET\n\nASIAN BUFFET\nASIAN BUFFET'), 'Asian Buffet')
        self.assertEqual(dp._mck_city_state_zip('MCKINNEY, TX 75069'), ('Mckinney', 'TX', '75069'))
        self.assertEqual(dp._mck_city_state_zip('FAIRVIEW, TX 75069-1234'), ('Fairview', 'TX', '75069'))
        self.assertEqual(dp._mck_city_state_zip(None), ('Mckinney', 'TX', ''))
        self.assertTrue(all(r['city'] == 'Mckinney' for r in self.records))

    def test_schools_are_dropped_by_layer_and_by_name(self):
        self.assertNotIn('mck_FOOD-028991-2026', self.by_id)   # Johnson Middle School: on layer 3
        self.assertNotIn('mck_FOOD-027620-2026', self.by_id)   # Ogle Elementary: on no layer, by name

    def test_oldest_first_and_worst_first_within_a_day(self):
        dates = [r['inspection_date'] for r in self.records]
        self.assertEqual(dates, sorted(dates))
        same_day = [r['source_id'] for r in self.records if r['inspection_date'] == '2026-10-30']
        self.assertEqual(same_day, ['mck_FOOD-028293-2026', 'mck_FOOD-034691-2026'])   # 81 before 87

    def test_dates_are_the_local_wall_clock_date(self):
        self.assertEqual(dp._mck_date(1793373775000), '2026-10-30')   # 15:22 'UTC' = 3:22 pm local
        self.assertEqual(dp._mck_date(_epoch_ms('2026-10-13', hour=7)), '2026-10-13')
        self.assertIsNone(dp._mck_date(None))
        self.assertIsNone(dp._mck_date('x'))

    def test_make_restaurant_id_is_stable(self):
        again, _ = run_fetch(since='2026-10-01')
        for a, b in zip(self.records, again):
            self.assertEqual(dp.make_restaurant_id(a['name'], a['address'], a['city']),
                             dp.make_restaurant_id(b['name'], b['address'], b['city']))
        r = self.by_id['mck_FOOD-034691-2026']
        self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']), '438a65933c47857f')

    def test_no_holds_on_a_clean_window(self):
        run_fetch(since='2026-10-01')
        self.assertEqual(dp._FRESHNESS_HOLDS, {})
        self.assertEqual(dp._GROUP_STATUS, {})


class ScopeTests(McKinneyBase):
    def test_void_and_pre_opening_rows_and_mobile_permits_are_skipped(self):
        page = _load('inspections_page.json')
        for f in page['features']:
            a = f['attributes']
            if a['inspection_number'] == 'FOOD-029818-2026':
                a['status'] = 'Void'
            if a['inspection_number'] == 'FOOD-020026-2026':
                a['purpose_of_inspection'] = '3 - Pre-Opening'
            if a['inspection_number'] == 'FOOD-027470-2026':
                a['link_number'] = 'HEALTH2024-00019'          # KING KUP TRUCK: Mobile Food Establishment
        records, _ = run_fetch(since='2026-10-01', page=page)
        ids = set(_by_id(records))
        self.assertNotIn('mck_FOOD-029818-2026', ids)
        self.assertNotIn('mck_FOOD-020026-2026', ids)
        self.assertNotIn('mck_FOOD-027470-2026', ids)
        self.assertEqual(len(records), 4)

    def test_micro_market_class_and_name_are_skipped(self):
        permits = _load('permits_page.json')
        for f in permits['features']:
            if f['attributes']['link_number'] == 'HEALTH2024-00794':   # Starbuck's permit
                f['attributes']['WORK_CLASS'] = 'Micro Market - Vending Machine'
        records, _ = run_fetch(since='2026-10-01', permits=permits)
        self.assertNotIn('mck_FOOD-027470-2026', _by_id(records))
        records, _ = run_fetch(since='2026-10-01',
                               page=_page_with('FOOD-029818-2026', permitted_name='ARAMARK MICROMARKET'))
        self.assertNotIn('mck_FOOD-029818-2026', _by_id(records))

    def test_the_shared_name_rule(self):
        self.assertIs(dp.MCK_SCHOOL_NAME_RE, dp.DFW_HOLE_NON_RESTAURANT_RE)
        for name in ('Little Steps Daycare', 'LISD - Degan Elementary', 'MISD Child Nutrition',
                     'Applebee Montessori Academy', 'Kiddie Academy of Castle Hills',
                     'The Goddard School', 'Childtime Learning Center', 'Boyd High School Concession',
                     'The Learning Experience Academy of Early Education'):
            self.assertIsNotNone(dp.DFW_HOLE_NON_RESTAURANT_RE.search(name), name)
        for name in ('Burger Academy', 'Academy Sports & Outdoors', 'Culinary School Cafe',
                     'Lake Village Nursing Home', 'Lake Park Baseball Concessions', 'Kids Pizza'):
            self.assertIsNone(dp.DFW_HOLE_NON_RESTAURANT_RE.search(name), name)
        records, _ = run_fetch(since='2026-10-01',
                               page=_page_with('FOOD-029818-2026', permitted_name='APPLEBEE MONTESSORI ACADEMY'))
        self.assertNotIn('mck_FOOD-029818-2026', _by_id(records))

    def test_reinspection_purpose_ships_with_its_type(self):
        records, _ = run_fetch(since='2026-10-01',
                               page=_page_with('FOOD-029818-2026', purpose_of_inspection='6 - Reinspection'))
        self.assertEqual(_by_id(records)['mck_FOOD-029818-2026']['inspection_type'], 'Reinspection')

    def test_unnamed_row_falls_back_to_the_permit_name_or_is_skipped(self):
        page = _load('inspections_page.json')
        for f in page['features']:
            if f['attributes']['inspection_number'] == 'FOOD-027470-2026':
                f['attributes']['permitted_name'] = ''        # permit HEALTH2024-00794 is on layer 0
            if f['attributes']['inspection_number'] == 'FOOD-029818-2026':
                f['attributes']['permitted_name'] = None
                f['attributes']['link_number'] = 'HEALTH1999-99999'
        records, _ = run_fetch(since='2026-10-01', page=page)
        by_id = _by_id(records)
        self.assertEqual(by_id['mck_FOOD-027470-2026']['name'], "Starbuck's Coffee #6416")
        self.assertNotIn('mck_FOOD-029818-2026', by_id)


class ViolationParserTests(unittest.TestCase):
    def test_header_layout_variants(self):
        blocks = dp._mck_violation_blocks(
            'CFM: Jane Doe 04/26\nPest Control: Acme 10/26\n\nObservations:\n38 - Ambient WIC\n'
            '80 - Cooked Shrimp HH**\n\nViolations:\n#9. Observed raw chicken over raw eggs. Separate.\n'
            '#42:Observed dust on vent in WIC. Clean.\n\tB) Observed rust on air vents. Remove.\n'
            '45 - Observed damaged floor tiles. Repair.\n\nComments:\n- Repair pipe above oven.\n')
        self.assertEqual(dict(blocks), {
            9: ['Observed raw chicken over raw eggs. Separate.'],
            42: ['Observed dust on vent in WIC. Clean. Observed rust on air vents. Remove.'],
            45: ['Observed damaged floor tiles. Repair.'],
        })                                      # 38 and 80 (temperatures) are not items

    def test_no_header_layout_and_repeated_items(self):
        blocks = dp._mck_violation_blocks(
            'CFM\n\tJane Doe\nPest\n\tLatin Pest\n\t9/26\nReach-in 39\nPrep 38\n\n'
            '34 - Observed back door open.  Keep closed.\n34 - Observed flies in kitchen.\n'
            '45- Observed damaged cove tile. Repair. Recurring violation\n'
            'CFM: John Roe\n')
        self.assertEqual(blocks[34], ['Observed back door open. Keep closed.', 'Observed flies in kitchen.'])
        self.assertEqual(blocks[45], ['Observed damaged cove tile. Repair. Recurring violation'])
        self.assertEqual(set(blocks), {34, 45})
        for texts in blocks.values():
            for t in texts:
                self.assertNotIn('Jane Doe', t)
                self.assertNotIn('John Roe', t)
        self.assertEqual(dp._mck_violation_blocks(None), {})
        self.assertEqual(dp._mck_violation_blocks('No violations observed'), {})

    def test_items_under_an_observations_header_are_read(self):
        # FOOD-015857 layout: the '#NN.' lines sit under 'Observations:', no 'Violations:' at all.
        blocks = dp._mck_violation_blocks(
            'TEMPERATURES:\nWalk in 40F\n\nObservations:\n#10. Observed dirty can opener.\n'
            '#34. Observed roaches under the prep table.\n\nComments:\n')
        self.assertEqual(dict(blocks), {10: ['Observed dirty can opener.'],
                                        34: ['Observed roaches under the prep table.']})
        # FOOD-030012 layout: dash items under 'Observations:' with no blank line before them;
        # the temperature notes above keep their dash form and are not items.
        blocks = dp._mck_violation_blocks(
            'CFM: Jane Doe\nPEST CONTROL: Ecolab 9/26\n\nTemperatures:\n\t38 - Tomatoes Prep Cooler\n'
            '\t40 - Ham WIC\n\t185 - Beans HH\n\nObservations:\n34 - Observed large opening under the rear '
            'door not providing adequate protection from pests.  Repair. \n42 - Observed areas of debris '
            'accumulation on the floor under and around equipment.  Clean these areas. \n\nComments:')
        self.assertEqual(set(blocks), {34, 42})
        self.assertEqual(blocks[34], ['Observed large opening under the rear door not providing '
                                      'adequate protection from pests. Repair.'])

    def test_items_after_a_repeated_temperatures_header_are_read(self):
        # FOOD-025293 layout: a second 'Temperatures:' header between the items.
        blocks = dp._mck_violation_blocks(
            'Violations:\n#9. Observed raw beef over produce. Separate.\nTemperatures:\nWalk in 38F\n'
            '#18. Observed unlabeled spray bottle. Label.\n')
        self.assertEqual(dict(blocks), {9: ['Observed raw beef over produce. Separate.'],
                                        18: ['Observed unlabeled spray bottle. Label.']})
        # A dash observation inside a temperature block (no blank line) is read too; '38 - Walk in' is not.
        blocks = dp._mck_violation_blocks(
            'Temperatures:\n38 - Walk in\n34 - Observed live roaches under the dish machine. Treat.\n'
            '45 - Observed damaged floor. Repair.')
        self.assertEqual(dict(blocks), {34: ['Observed live roaches under the dish machine. Treat.'],
                                        45: ['Observed damaged floor. Repair.']})

    def test_hash_without_punctuation_and_inline_markers(self):
        # FOOD-032570 layout: '#9 Observed ...' blocks separated by blank lines.
        blocks = dp._mck_violation_blocks(
            '#9 Observed canned drink products stored in ice.  All ice must be removed.\n\n'
            '#42 Observed discolored ceiling tiles throughout.\n\n#27. Observed damaged prep table.')
        self.assertEqual(set(blocks), {9, 42, 27})
        self.assertEqual(blocks[9], ['Observed canned drink products stored in ice. All ice must be removed.'])
        # FOOD-010797 layout: two items pasted on one line.
        blocks = dp._mck_violation_blocks(
            'Violations:\n#31. Observed handwashing facility without paper towels. Supply. '
            '#34. Observed no pest control invoice. Controlling pests.')
        self.assertEqual(dict(blocks), {31: ['Observed handwashing facility without paper towels. Supply.'],
                                        34: ['Observed no pest control invoice. Controlling pests.']})

    def test_header_shaped_lines_never_join_a_block_but_sentences_do(self):
        blocks = dp._mck_violation_blocks(
            'Violations:\n#34. Observed droppings under shelf.\n'
            'Pest control last treated 10/1; live roaches seen in dish area.\n'
            '#45. Observed damaged floor.\nPIC: John Smith\nManager: Jane Roe\nOwner: Jim Roe\n'
            '#36. Observed wiping cloths on the counter.\nTemps: WIC 38\n')
        self.assertEqual(dict(blocks), {
            34: ['Observed droppings under shelf. Pest control last treated 10/1; live roaches seen in dish area.'],
            45: ['Observed damaged floor.'],
            36: ['Observed wiping cloths on the counter.'],
        })
        joined = ' '.join(t for texts in blocks.values() for t in texts)
        for private in ('John Smith', 'Jane Roe', 'Jim Roe', 'WIC 38'):
            self.assertNotIn(private, joined)

    def test_cited_item_without_text_gets_a_neutral_phrase(self):
        attrs = {col: 0 for col, _ in dp.MCK_ITEMS}
        attrs.update({'approved_sewage': 3, 'no_insect_contamination': 2, 'wiping_cloths': 1,
                      'custom_comments': 'Violations:\n#36. Observed wiping cloths on the counter.'})
        v = dp._mck_violations(attrs)
        self.assertEqual([(x[1], x[2]) for x in v], [
            ('priority', 'Plumbing and wastewater disposal item cited on the inspection report; '
                         'no observation recorded'),
            ('priority_foundation', 'Pest control item cited on the inspection report; '
                                    'no observation recorded'),
            ('core', 'Wiping cloths properly used and stored: Observed wiping cloths on the counter.'),
        ])
        for _, _, text in v:
            self.assertFalse(dp.has_active_pest_or_sewage(text))

    def test_a_cited_pest_item_gets_the_reports_pest_sentence_wherever_it_sits(self):
        attrs = {col: 0 for col, _ in dp.MCK_ITEMS}
        attrs.update({'no_insect_contamination': 2, 'physical_facilities_maintained': 1})
        # The roach sentence sits in the comments tail, which the parser never attributes.
        attrs['custom_comments'] = ('Violations:\n#45. Observed damaged floor.\n\nComments:\n'
                                    'Observed live roaches under the dish machine; pest control scheduled.')
        pest = [x for x in dp._mck_violations(attrs) if x[0] == 'pest_control']
        self.assertEqual(pest, [['pest_control', 'priority_foundation',
                                 'Observed live roaches under the dish machine;']])
        self.assertTrue(dp.has_active_pest_or_sewage(pest[0][2]))
        # Its own observation is kept and the hazard sentence appended once.
        attrs['custom_comments'] = ('Violations:\n#34. Observed back door open.\n#45. Observed damaged '
                                    'floor. Observed roach droppings behind the ice machine.')
        pest = [x for x in dp._mck_violations(attrs) if x[0] == 'pest_control'][0]
        self.assertEqual(pest[2], 'Observed back door open. | Observed roach droppings behind the ice machine.')
        # The pest contractor header and an unnamed-pest observation add nothing.
        attrs['custom_comments'] = ('CFM: Jane Doe\nPest Control: Orkin 10/26\n\nViolations:\n'
                                    '#34. Observed gap under the back door; pests could enter.')
        pest = [x for x in dp._mck_violations(attrs) if x[0] == 'pest_control'][0]
        self.assertEqual(pest[2], 'Observed gap under the back door; pests could enter.')
        self.assertFalse(dp.has_active_pest_or_sewage(pest[2]))
        # A pest sentence on a report whose inspector cited no pest item is not invented into one.
        attrs['no_insect_contamination'] = 0
        attrs['custom_comments'] = 'Violations:\n#45. Observed damaged floor.\n\nComments:\nObserved live roaches.'
        v = dp._mck_violations(attrs)
        self.assertEqual([(x[0], x[1]) for x in v], [(dp.classify_violation(v[0][2])[0], 'core')])
        self.assertNotIn('roach', v[0][2])
        self.assertNotEqual(v[0][0], 'pest_control')

    def test_a_cited_sewage_item_gets_the_sewage_sentence(self):
        attrs = {col: 0 for col, _ in dp.MCK_ITEMS}
        attrs.update({'approved_sewage': 3, 'custom_comments':
                      'Temperatures:\nWalk in 38F\nObservations:\n20 - Observed sewage backing up from the '
                      'floor drain by the dish machine. Repair.\n'})
        v = dp._mck_violations(attrs)
        self.assertEqual(v, [['water_sewage', 'priority',
                              'Observed sewage backing up from the floor drain by the dish machine. Repair.']])
        self.assertTrue(dp.has_active_pest_or_sewage(v[0][2]))

    def test_phone_numbers_and_emails_are_scrubbed(self):
        attrs = {col: 0 for col, _ in dp.MCK_ITEMS}
        attrs.update({'certified_food_manager': 2,
                      'custom_comments': 'Violations:\n#21. No CFM on site; call 972-555-0100 or cfm@example.com.'})
        text = dp._mck_violations(attrs)[0][2]
        self.assertNotIn('972-555-0100', text)
        self.assertNotIn('cfm@example.com', text)
        self.assertIn('[phone]', text)

    def test_item_weights_follow_the_tx_form(self):
        self.assertEqual(len(dp.MCK_ITEMS), 47)
        self.assertEqual(dp.MCK_ITEMS[19][0], 'approved_sewage')          # item 20: no title
        self.assertEqual(dp.MCK_ITEMS[33][0], 'no_insect_contamination')  # item 34: no title
        self.assertEqual((dp.MCK_ITEMS[19][1], dp.MCK_ITEMS[33][1]), ('', ''))
        self.assertEqual(dp.MCK_ITEMS[0][0], 'temp_cooling_time')
        self.assertEqual(dp.MCK_ITEMS[46][0], 'other_violations')
        self.assertEqual(set(dp.MCK_HAZARD_ITEMS), {20, 34})


class HazardRecordTests(McKinneyBase):
    OBS_LAYOUT = ('CFM: Jane Doe 04/26\nPest Control: Cornerstone 10/26\n\nTEMPERATURES:\n'
                  'Walk in refrigerator 40F\n\nObservations:\n'
                  '#9. Observed raw chicken over raw eggs. Separate according to cook temperature.\n'
                  '#18. Observed Spray bottle without label. Label.\n'
                  '#32. Observed wiping cloth stored under cutting boards. Remove.\n'
                  '#34. Observed roaches under the prep table. Pest control to treat.\n'
                  '#36. Observed wiping cloth improperly stored. Remove.\n'
                  '#37. Observed food item being stored on the floor. Remove.\n'
                  '#42. Observed accumulation on vents. Clean.\n'
                  '#45. Observed damaged floor tiles. Repair.\n'
                  '#47. Observed unlabeled bulk bins. Label.\n\nComments:\n')

    def test_a_roach_under_an_observations_header_forces_the_f(self):
        page = _page_with('FOOD-034691-2026', custom_comments=self.OBS_LAYOUT,
                          no_insect_contamination=2, total_score=15)
        r = _by_id(run_fetch(since='2026-10-01', page=page)[0])['mck_FOOD-034691-2026']
        self.assertEqual((r['original_score'], r['risk_score'], r['total_violations']), (15, 85, 9))
        pest = [v for v in r['violations'] if v[0] == 'pest_control']
        self.assertEqual(pest, [['pest_control', 'priority_foundation',
                                 'Observed roaches under the prep table. Pest control to treat.']])
        self.assertIn('Food separated and protected: Observed raw chicken over raw eggs. Separate '
                      'according to cook temperature.', [v[2] for v in r['violations']])
        self.assertFalse(any(v[2].endswith(dp.MCK_NO_OBSERVATION) for v in r['violations']))
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score'], dp.build_violation_text(r['violations'])), 'F')


class FailureTests(McKinneyBase):
    def test_count_shortfall_raises_instead_of_shipping(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01', count={'count': 11})
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_transport_failure_raises(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01',
                      answers={'4/count': dp.requests.ConnectionError('reset')})
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01', answers={'4': dp.requests.ConnectionError('reset')})

    def test_server_errors_are_retried_then_raise(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01', answers={'4/count': [_Resp(None, 503, text='down', ctype='text/html')] * 5})
        with mock.patch.object(dp, 'DFW_HOLE_RETRIES', 2):
            with self.assertRaises(RuntimeError):
                run_fetch(since='2026-10-01',
                          answers={'4/count': [_Resp(None, 503, text='down', ctype='text/html')] * 5})
        # A page-level 5xx (inside the shared ArcGIS pager) raises the same way.
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01', answers={'4': [_Resp(None, 502, text='bad gateway')] * 5})

    def test_arcgis_error_payload_raises(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01',
                      answers={'4': _Resp({'error': {'code': 400, 'message': 'Invalid query'}})})
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01',
                      answers={'4/count': _Resp({'error': {'code': 400, 'message': 'Invalid query'}})})

    def test_html_answer_is_a_block_not_data(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01',
                      answers={'4/count': _Resp(None, 403, text='<html>blocked</html>', ctype='text/html')})

    def test_unreadable_permit_layers_are_fatal(self):
        # Without layer 3 a school concession would ship as a restaurant for good; without
        # layer 0 mobile units would, and rows named only by their permit would be dropped
        # without a hold. Either read failing raises like the inspections layer failing.
        page = _page_with('FOOD-028991-2026', permitted_name='Johnson Concession Stand')   # only layer 3 tells
        records, _ = run_fetch(since='2026-10-01', page=page)
        self.assertNotIn('mck_FOOD-028991-2026', _by_id(records))
        for layer in ('0', '3', '1'):
            with self.assertRaises(RuntimeError, msg=layer) as ctx:
                run_fetch(since='2026-10-01', page=page, answers={layer: dp.requests.ConnectionError('reset')})
            self.assertIn('nothing shipped', str(ctx.exception))
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_points_that_do_not_add_up_drop_and_hold_that_inspection(self):
        page = _page_with('FOOD-034691-2026', total_score=12)          # the items still add up to 13
        with self.assertLogs('dinescores', level='WARNING') as logs:
            records, _ = run_fetch(since='2026-10-01', page=page)
        self.assertNotIn('mck_FOOD-034691-2026', _by_id(records))
        self.assertEqual(len(records), 6)                                # the rest ships
        self.assertEqual(dp._FRESHNESS_HOLDS,
                         {dp.MCK_SOURCE: {'2026-10-30|FOOD-034691-2026': (True, False)}})
        self.assertTrue(any('do not add up' in m for m in logs.output))
        # Item columns emptied while total_score keeps the demerits: never a clean 100.
        cleared = {col: 0 for col, _ in dp.MCK_ITEMS}
        page = _page_with('FOOD-027854-2026', **cleared)
        records, _ = run_fetch(since='2026-10-01', page=page)
        self.assertNotIn('mck_FOOD-027854-2026', _by_id(records))

    def test_many_mismatches_raise_and_hold_nothing(self):
        page = _load('inspections_page.json')
        for f in page['features']:
            if f['attributes']['inspection_number'] in ('FOOD-034691-2026', 'FOOD-027854-2026'):
                f['attributes']['total_score'] = 1
        with mock.patch.object(dp, 'MCK_MISMATCH_MIN', 2):                 # 2 of 7 > 5%
            with self.assertRaises(RuntimeError) as ctx:
                run_fetch(since='2026-10-01', page=page)
        self.assertIn('item columns changed', str(ctx.exception))
        self.assertEqual(dp._FRESHNESS_HOLDS, {})
        with mock.patch.object(dp, 'MCK_MISMATCH_MIN', 2), mock.patch.object(dp, 'MCK_MISMATCH_MAX_SHARE', 0.5):
            records, _ = run_fetch(since='2026-10-01', page=page)         # 2 of 7 < 50%: dropped and held
        self.assertEqual(len(records), 5)
        self.assertEqual(sorted(dp._FRESHNESS_HOLDS[dp.MCK_SOURCE]),
                         ['2026-10-22|FOOD-027854-2026', '2026-10-30|FOOD-034691-2026'])

    def test_empty_window_returns_nothing_and_probes_the_newest_row(self):
        records, calls = run_fetch(since='2026-10-01', page={'features': []}, count={'count': 0},
                                   answers={'4/newest': _Resp({'features': [{'attributes': {
                                       'actual_end_date': _epoch_ms('2026-01-15')}}]})})
        self.assertEqual(records, [])
        self.assertTrue(any(str(c[1].get('orderByFields', '')).startswith('actual_end_date') for c in calls))
        self.assertIn('McKinney', dp._GROUP_NOTES)
        self.assertIn('2026-01-15', dp._GROUP_NOTES['McKinney'])


class WindowAndLagTests(McKinneyBase):
    def test_limit_keeps_the_newest_and_leaves_no_note(self):
        records, _ = run_fetch(since='2026-10-01', limit=2)
        self.assertEqual([r['source_id'] for r in records], ['mck_FOOD-028293-2026', 'mck_FOOD-034691-2026'])
        records, _ = run_fetch(since='2026-01-01', limit=2, page=_page_dated('2026-01-15'))
        self.assertEqual(len(records), 2)
        self.assertNotIn('McKinney', dp._GROUP_NOTES)          # a bounded read never saw the end

    def test_source_lag_note_when_the_layer_is_behind(self):
        records, _ = run_fetch(since='2026-01-01', page=_page_dated('2026-01-15'))
        self.assertEqual(len(records), 7)
        self.assertIn('McKinney', dp._GROUP_NOTES)
        self.assertIn('2026-01-15', dp._GROUP_NOTES['McKinney'])
        dp._GROUP_NOTES.clear()
        run_fetch(since='2026-01-01', page=_page_dated(TODAY))  # fresh data (dated today): no note
        self.assertNotIn('McKinney', dp._GROUP_NOTES)

    def test_limit_larger_than_the_window_is_a_complete_read(self):
        records, _ = run_fetch(since='2026-01-01', limit=50000, page=_page_dated('2026-01-15'))
        self.assertEqual(len(records), 7)
        self.assertIn('McKinney', dp._GROUP_NOTES)


if __name__ == '__main__':
    unittest.main()
