"""Tests for the Garland, TX fetcher (city ArcGIS MapServer; a snapshot of
each permit's latest inspection). No network: requests.get is mocked with
trimmed captures of the live server taken 2026-11-23
(tests/fixtures/garland/): 27 rows of layer 4 'Restaurant Scores' and the
166 rows table 3 'Restaurant violations' holds for those permits, verbatim
(the memos carry no personal names, phone numbers or e-mail addresses).
The rows were picked for what they exercise: a clean 100, the pest item with
a door-gap note, a crawling cockroach, rodent droppings and an empty memo,
the sewage item over a backed-up drain, an observation scored 0 points, a
1,278-character memo, a grocery deli and a convenience-store taqueria, a
permit with exchanged GIS columns and one with no location, the old
2023 form whose points do not add up, schools, child care, a plan review, a
school summer meal site, an event-center concession stand and the school
district's warehouse."""
import copy
import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'garland')
SINCE = '2023-01-01'           # older than every fixture row: the whole fixture is in the window
GAR_EDGE = dp.GAR_TEXT_MAX - len('Other violations: ') - 9   # memo length that lands a line on the cap


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


def _epoch_ms(date_str):
    d = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    return int(d.timestamp() * 1000)


def _layer():
    return _load('scores_layer.json')


def _table():
    return _load('violations_table.json')


def _edit(payload, permit, code=None, **attrs):
    """Change the attributes of `permit`'s rows (only the one with
    VIOLATION_CODE `code` when given) in a layer or table payload."""
    for f in payload['features']:
        a = f['attributes']
        if a['PERMIT_NUMBER'] == permit and (code is None or a.get('VIOLATION_CODE') == code):
            a.update(attrs)
    return payload


def _drop(payload, permit, code='*'):
    """Remove `permit`'s rows (only those with VIOLATION_CODE `code` unless '*')."""
    payload['features'] = [f for f in payload['features']
                           if not (f['attributes']['PERMIT_NUMBER'] == permit
                                   and (code == '*' or f['attributes'].get('VIOLATION_CODE') == code))]
    return payload


def run_fetch(since=SINCE, limit=None, layer=None, table=None, answers=None, counts=None):
    """fetch_garland against the fixtures. The fake server pages by
    resultOffset / resultRecordCount and answers counts from the payloads.
    `answers` maps '4', '3', '4/count', '3/count' or '4/newest' to a _Resp,
    an exception or a list of those served in order (then the default)."""
    layer = layer if layer is not None else _layer()
    table = table if table is not None else _table()
    counts = dict(counts or {})
    answers = {k: (list(v) if isinstance(v, list) else v) for k, v in (answers or {}).items()}
    calls = []

    def serve(key, default):
        override = answers.get(key)
        if isinstance(override, list):
            override = override.pop(0) if override else None
        if isinstance(override, Exception):
            raise override
        return override if override is not None else default

    def page(payload, params):
        offset = int(params.get('resultOffset') or 0)
        size = int(params.get('resultRecordCount') or 2000)
        feats = payload['features'][offset:offset + size]
        out = {k: v for k, v in payload.items() if k != 'features'}
        out['features'] = feats
        if offset + size < len(payload['features']):
            out['exceededTransferLimit'] = True
        return out

    def fake_get(url, params=None, headers=None, timeout=None, verify=True, **kw):
        params = dict(params or {})
        calls.append((url, params))
        obj = url[len(dp.GAR_BASE) + 1:].split('/')[0]
        payload = layer if obj == '4' else table if obj == '3' else None
        if payload is None:
            raise AssertionError(f'unexpected request {url}')
        if params.get('returnCountOnly'):
            n = counts.get(obj, len(payload['features']))
            return serve(f'{obj}/count', _Resp({'count': n}))
        if params.get('orderByFields') == 'INSPECTION_DATE DESC':
            return serve('4/newest', _Resp({'features': []}))
        return serve(obj, _Resp(page(payload, params)))

    with mock.patch.object(dp.requests, 'get', side_effect=fake_get), \
            mock.patch.object(dp.time, 'sleep'):
        records = dp.fetch_garland(since_date=since, limit=limit)
    return records, calls


def _by_permit(records):
    return {r['source_id'].split('_')[1]: r for r in records}


def _grade(r):
    return dp.calculate_vetted_grade(r['risk_score'], dp.build_violation_text(r['violations']))


class GarlandBase(unittest.TestCase):
    def setUp(self):
        self._saved = (dict(dp._GROUP_NOTES), dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
                       dp._FETCH_DEADLINE, dp.log.level)
        dp._GROUP_NOTES.clear()
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._FETCH_DEADLINE = None
        dp.log.setLevel('CRITICAL')

    def tearDown(self):
        s = self._saved
        dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(s[0])
        dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(s[1])
        dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(s[2])
        dp._FETCH_DEADLINE = s[3]
        dp.log.setLevel(s[4])


class TestRecords(GarlandBase):
    def setUp(self):
        super().setUp()
        self.records, self.calls = run_fetch()
        self.by = _by_permit(self.records)

    def test_what_ships(self):
        # 27 layer rows: schools (1608), child care (1607), the plan review
        # (1624), the summer meal site (1628), the 1614 concession (unknown
        # program element), the ISD warehouse (by name) and the 2023 form
        # whose points do not add up are left out.
        self.assertEqual(sorted(self.by), sorted([
            'PT0031', 'PT0034', 'PT0048', 'PT0106', 'PT0148', 'PT1681', 'PT2286', 'PT3161',
            'PT3261', 'PT3390', 'PT3447', 'PT4449', 'PT4450', 'PT4451', 'PT4452', 'PT4454',
            'PT4746', 'PT4830', 'PT4921']))
        for p in ('PT0369', 'PT0431', 'PT0507', 'PT5031', 'PT5192', 'PT4453', 'PT0388', 'PT2896'):
            self.assertNotIn(p, self.by)

    def test_record_shape(self):
        r = self.by['PT1681']
        self.assertEqual(set(r), {
            'name', 'address', 'city', 'state', 'zip', 'latitude', 'longitude', 'inspection_date',
            'original_score', 'risk_score', 'priority_violations', 'priority_foundation_violations',
            'core_violations', 'total_violations', 'violations', 'inspection_type', 'results',
            'source', 'source_url', 'source_id', 'metro'})
        self.assertEqual(r['name'], 'La Media Luna')
        self.assertEqual(r['address'], '3460 W Walnut St')
        self.assertEqual((r['city'], r['state'], r['zip']), ('Garland', 'TX', '75042'))
        self.assertEqual(r['inspection_date'], '2026-11-20')
        self.assertEqual(r['source'], 'Garland Health Inspections')
        self.assertEqual(r['metro'], 'DFW')
        self.assertEqual(r['source_id'], 'garland_PT1681_20261120')
        self.assertIn('permitid=PT1681', r['source_url'])
        self.assertIn('facilityid=FA0001543', r['source_url'])
        self.assertAlmostEqual(r['latitude'], 32.9155686, places=5)
        self.assertAlmostEqual(r['longitude'], -96.6815045, places=5)
        for rec in self.records:
            self.assertRegex(rec['state'], r'^[A-Z]{2}$')
            for cat, sev, text in rec['violations']:
                self.assertIn(sev, ('priority', 'priority_foundation', 'core'))
                self.assertLessEqual(len(text), dp.GAR_TEXT_MAX)
                self.assertNotIn('\n', text)
                self.assertNotIn('\r', text)

    def test_official_score_kept(self):
        # risk_score is the city's SCORE, never recomputed from the lines.
        for p, score in (('PT1681', 97), ('PT2286', 60), ('PT3390', 71), ('PT0031', 100), ('PT0148', 73)):
            self.assertEqual(self.by[p]['risk_score'], score)
            self.assertEqual(self.by[p]['original_score'], score)

    def test_points_are_tiers(self):
        for r in self.records:
            pv, pfv, cv = (r['priority_violations'], r['priority_foundation_violations'],
                           r['core_violations'])
            self.assertEqual(100 - r['risk_score'], 3 * pv + 2 * pfv + cv, r['source_id'])
            self.assertEqual(r['total_violations'], len(r['violations']))
        r = self.by['PT3161']   # 95: items 18 (3 points), 37 and 47 (1 each)
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'],
                          r['core_violations']), (1, 0, 2))

    def test_zero_point_observation_is_not_a_line(self):
        # Starbucks: item 28 at 0 points 'not being counted as a violation'.
        r = self.by['PT0106']
        self.assertEqual(r['risk_score'], 91)
        self.assertFalse(any('date-labeled' in t for _, _, t in r['violations']))
        self.assertEqual(len(r['violations']), 5)

    def test_clean_inspection(self):
        r = self.by['PT0031']
        self.assertEqual((r['risk_score'], r['violations'], r['total_violations']), (100, [], 0))
        self.assertEqual(_grade(r), 'A')

    def test_names(self):
        self.assertEqual(self.by['PT0048']['name'], 'Kroger Food Store #557 - Deli')
        self.assertEqual(self.by['PT3161']['name'], "Garland Homeboy's - Street Taco")
        self.assertEqual(self.by['PT4449']['name'], "Curtis Culwell Center Concessions - Ted E's Kitchen 1")
        # A grocery's departments are told apart.
        self.assertEqual(len({dp.make_restaurant_id(r['name'], r['address'], r['city'])
                              for r in self.records}), len(self.records))

    def test_office_note_identifier_is_not_a_department(self):
        a = {'NAME': 'PEARL C ANDERSON CHILDCARE GROUP', 'PROGRAM_IDENTIFIER': 'INSPECTIONS ONLY'}
        self.assertEqual(dp._gar_name(a), 'Pearl C Anderson Childcare Group')
        a = {'NAME': 'SOME PLACE', 'PROGRAM_IDENTIFIER': 'for inspection only'}
        self.assertEqual(dp._gar_name(a), 'Some Place')
        self.assertEqual(dp._gar_name({'NAME': 'CVS PHARMACY #7481', 'PROGRAM_IDENTIFIER': '0'}),
                         'Cvs Pharmacy #7481')

    def test_coordinates(self):
        # Exchanged GIS columns and no point: put back.
        r = self.by['PT4746']
        self.assertAlmostEqual(r['latitude'], 32.848245)
        self.assertAlmostEqual(r['longitude'], -96.569114)
        # No location at all: left for the geocoder.
        r = self.by['PT4921']
        self.assertIsNone(r['latitude'])
        self.assertIsNone(r['longitude'])
        for rec in self.records:
            if rec['latitude'] is not None:
                self.assertTrue(dp.coords_plausible(rec['latitude'], rec['longitude'], 'TX'))

    def test_four_requests(self):
        self.assertEqual(len(self.calls), 4)
        for url, params in self.calls:
            self.assertEqual(params['where'], "INSPECTION_DATE >= DATE '2023-01-01'")

    def test_reshipping_is_idempotent(self):
        again, _ = run_fetch()
        self.assertEqual(again, self.records)


class TestPestAndSewageText(GarlandBase):
    def setUp(self):
        super().setUp()
        self.by = _by_permit(run_fetch()[0])

    def _line(self, permit, start):
        lines = [t for _, _, t in self.by[permit]['violations'] if t.startswith(start)]
        self.assertEqual(len(lines), 1, self.by[permit]['violations'])
        return lines[0]

    def test_door_gap_under_the_pest_item_is_not_an_f(self):
        # Taco Bell 93: item 34 over an air curtain; the form title the
        # table prints ('No Evidence of Insect Contamination, Rodent/other
        # Animals') never ships.
        line = self._line('PT3261', 'Pests and animals:')
        self.assertTrue(line.startswith('Pests and animals: Observed air curtain at the drive-through'))
        self.assertNotIn('No Evidence', line)
        self.assertEqual(_grade(self.by['PT3261']), 'A')

    def test_named_finding_forces_f(self):
        self.assertTrue(self._line('PT3390', 'Pests and animals:').startswith(
            'Pests and animals: Observed a large cockroach crawling in kitchen.'))
        self.assertEqual(_grade(self.by['PT3390']), 'F')      # score 71, F from the roach
        self.assertIn('rodent droppings', self._line('PT0034', 'Pests and animals:'))
        self.assertEqual(_grade(self.by['PT0034']), 'F')      # score 85, F from the droppings

    def test_empty_pest_memo_is_a_cited_item_without_f(self):
        line = self._line('PT4921', 'Pest control item')
        self.assertEqual(line, 'Pest control item cited on the inspection report; no observation recorded.')
        self.assertFalse(dp.has_active_pest_or_sewage(line))
        self.assertEqual(jev_reader.cited_pest_item(line), 'pests_unspecified')

    def test_form_titles_alone_never_force_f(self):
        self.assertFalse(dp.has_active_pest_or_sewage(dp._gar_line(34, '')))
        self.assertFalse(dp.has_active_pest_or_sewage(dp._gar_line(20, '')))
        self.assertFalse(dp.has_active_pest_or_sewage(dp._gar_line(34, 'Repair the gap under the rear door.')))
        # Every line ends a sentence, so an evidence word closing one line
        # cannot pair with a pest word opening the next.
        text = dp.build_violation_text([
            ['x', 'core', dp._gar_line(45, 'Observed no soap found')],
            ['x', 'core', dp._gar_line(34, 'Rodent proofing needed at the door')]])
        self.assertFalse(dp.has_active_pest_or_sewage(text))

    def test_sewage_item_carries_the_observation_alone(self):
        line = self._line('PT3447', 'Observed drains backing up')
        self.assertIn('wastewater', line)

    def test_long_memo_capped_and_bullets_dropped(self):
        line = self._line('PT2286', 'Pests and animals:')
        self.assertLessEqual(len(line), dp.GAR_TEXT_MAX)
        self.assertTrue(line.endswith('...'))
        self.assertTrue(line.startswith('Pests and animals: Live and dead cockroaches were observed.'))
        self.assertEqual(dp._gar_line(14, '- Observed employee'),
                         'Hands cleaned and properly washed; gloves used properly: Observed employee.')
        self.assertEqual(dp._gar_line(14, '•\tWash hands.'),
                         'Hands cleaned and properly washed; gloves used properly: Wash hands.')

    def test_finding_past_the_cap_moves_to_the_front(self):
        memo = ('Air curtain at the rear door does not turn on. ' * 12
                + 'Observed live cockroaches under the three-compartment sink.')
        self.assertGreater(len(memo), dp.GAR_TEXT_MAX)
        line = dp._gar_line(34, memo)
        self.assertLessEqual(len(line), dp.GAR_TEXT_MAX)
        self.assertTrue(line.startswith('Pests and animals: Observed live cockroaches'))
        self.assertTrue(dp.has_active_pest_or_sewage(line))
        # A line of exactly the cap without a closing period stays within it
        # (two 2026 memos ended in a bare PDF link).
        for n in range(GAR_EDGE - 5, GAR_EDGE + 5):
            line = dp._gar_line(47, 'x' * 8 + ' ' + 'y' * n)
            self.assertLessEqual(len(line), dp.GAR_TEXT_MAX)
            self.assertTrue(line.endswith(('.', '...')))
        # A moved sentence loses its bullet too (PT0148's bilingual memo, 2026-09-17).
        line = dp._gar_line(34, 'English: Air curtain was observed not functioning. ' * 12
                            + '--English: Observed a dead roach on the fryer equipment.')
        self.assertTrue(line.startswith('Pests and animals: English: Observed a dead roach'), line)
        # Without a finding the order is the inspector's.
        plain = dp._gar_line(34, 'Air curtain at the rear door does not turn on. ' * 12)
        self.assertTrue(plain.startswith('Pests and animals: Air curtain'))


class TestCompleteness(GarlandBase):
    def test_below_100_without_violation_rows_is_not_clean(self):
        records, _ = run_fetch(table=_drop(_table(), 'PT1681'))
        self.assertNotIn('PT1681', _by_permit(records))
        self.assertIn('PT1681', dp._GROUP_NOTES.get('Garland', ''))
        self.assertEqual(dp._FRESHNESS_HOLDS, {})      # the snapshot re-read replaces holds

    def test_100_without_violation_rows_ships_clean(self):
        records, _ = run_fetch(table=_drop(_table(), 'PT0031'))
        r = _by_permit(records)['PT0031']
        self.assertEqual((r['risk_score'], r['violations'], r['zip']), (100, [], ''))

    def test_points_must_add_up(self):
        # One cited line missing (half-read table): dropped, never a 97+3.
        records, _ = run_fetch(table=_drop(_table(), 'PT3161', code='1618'))
        self.assertNotIn('PT3161', _by_permit(records))
        self.assertIn('PT3161', dp._GROUP_NOTES['Garland'])

    def test_points_must_be_the_form_weight(self):
        # Item 31 at 3 points and the score moved to match: the sum adds up,
        # the weight does not.
        table = _edit(_edit(_table(), 'PT1681', code='1631', POINTS=3.0), 'PT1681', SCORE=96.0)
        records, _ = run_fetch(table=table, layer=_edit(_layer(), 'PT1681', SCORE=96.0))
        self.assertNotIn('PT1681', _by_permit(records))
        self.assertIn('the Texas form weighs it 2', dp._GROUP_NOTES['Garland'] + str(
            dp._gar_violations([f['attributes'] for f in table['features']
                                if f['attributes']['PERMIT_NUMBER'] == 'PT1681'], 96.0)))

    def test_table_score_must_match_layer(self):
        records, _ = run_fetch(table=_edit(_table(), 'PT1681', SCORE=95.0))
        self.assertNotIn('PT1681', _by_permit(records))

    def test_unknown_item_code(self):
        problems = dp._gar_violations([{'VIOLATION_CODE': '1648', 'POINTS': 1.0, 'SCORE': 99.0}], 99.0)
        self.assertIsNone(problems[0])
        self.assertIn('unknown item code', problems[1])
        problems = dp._gar_violations([{'VIOLATION_CODE': '1618', 'POINTS': None, 'SCORE': 97.0}], 97.0)
        self.assertIn('no points', problems[1])

    def test_old_form_reports_why(self):
        rows = [f['attributes'] for f in _table()['features'] if f['attributes']['PERMIT_NUMBER'] == 'PT2896']
        violations, problem = dp._gar_violations(rows, 89.0)
        self.assertIsNone(violations)
        self.assertIn('item 2 carries 5 points', problem)

    def test_many_unreadable_inspections_raise(self):
        table = _table()
        table['features'] = [f for f in table['features']
                             if f['attributes']['VIOLATION_CODE'] is None]   # every cited row gone
        with self.assertRaisesRegex(RuntimeError, 'cannot be read completely'):
            run_fetch(table=table)

    def test_many_unknown_program_elements_raise(self):
        layer = _layer()
        for f in layer['features']:
            f['attributes']['PE'] = 'FOOD'
        with self.assertRaisesRegex(RuntimeError, 'program element'):
            run_fetch(layer=layer)

    def test_unscored_layer_raises(self):
        layer = _layer()
        for f in layer['features']:
            f['attributes']['SCORE'] = None
        with mock.patch.object(dp, 'DFW_HOLE_UNSCORED_MIN', 5):
            with self.assertRaisesRegex(RuntimeError, 'no usable SCORE'):
                run_fetch(layer=layer)

    def test_a_few_unscored_rows_are_dropped(self):
        records, _ = run_fetch(layer=_edit(_layer(), 'PT1681', SCORE=None))
        self.assertNotIn('PT1681', _by_permit(records))
        self.assertIn('PT3161', _by_permit(records))


class TestWindowAndFailures(GarlandBase):
    def test_since_filters_rows(self):
        records, calls = run_fetch(since='2026-11-01')
        # 2026-11-01 is past the re-read floor, so the read starts earlier;
        # what the fake server returns is filtered on the fetcher's own since.
        floor = (datetime.now() - timedelta(days=dp.GAR_REREAD_DAYS)).strftime('%Y-%m-%d')
        self.assertEqual(calls[0][1]['where'], f"INSPECTION_DATE >= DATE '{floor}'")
        for r in records:
            self.assertGreaterEqual(r['inspection_date'], floor)

    def test_reread_never_narrows_an_older_window(self):
        _, calls = run_fetch(since='2024-01-01T00:00:00')
        self.assertEqual(calls[0][1]['where'], "INSPECTION_DATE >= DATE '2024-01-01'")

    def test_reread_disabled(self):
        with mock.patch.object(dp, 'GAR_REREAD_DAYS', 0):
            records, calls = run_fetch(since='2026-11-17')
        self.assertEqual(calls[0][1]['where'], "INSPECTION_DATE >= DATE '2026-11-17'")
        self.assertEqual(sorted(_by_permit(records)), ['PT1681'])

    def test_rows_before_since_are_ignored(self):
        layer = _layer()
        with mock.patch.object(dp, 'GAR_REREAD_DAYS', 0):
            records, _ = run_fetch(since='2026-08-01', layer=layer)
        self.assertEqual(sorted(_by_permit(records)),
                         ['PT0031', 'PT0148', 'PT1681', 'PT2286', 'PT3161', 'PT4746', 'PT4921'])

    def test_no_since_with_limit_is_a_bounded_window(self):
        records, calls = run_fetch(since=None, limit=2)
        start = (datetime.now() - timedelta(days=dp.GAR_TEST_WINDOW_DAYS)).strftime('%Y-%m-%d')
        self.assertEqual(calls[0][1]['where'], f"INSPECTION_DATE >= DATE '{start}'")
        self.assertLessEqual(len(records), 2)

    def test_no_since_no_limit_reads_the_history(self):
        _, calls = run_fetch(since=None)
        self.assertEqual(calls[0][1]['where'], f"INSPECTION_DATE >= DATE '{dp.GAR_HISTORY_START}'")

    def test_limit_keeps_the_newest(self):
        records, _ = run_fetch(limit=2)
        self.assertEqual([r['source_id'] for r in records],
                         ['garland_PT3161_20261116', 'garland_PT1681_20261120'])

    def test_lag_note(self):
        with mock.patch.object(dp, '_note_source_lag') as note:
            run_fetch()
        note.assert_called_once_with('Garland', '2026-11-20')

    def test_paging(self):
        with mock.patch.object(dp, 'GAR_PAGE', 10):
            records, calls = run_fetch()
        # layer: count + 3 pages, table: count + 17 pages
        self.assertEqual(len(calls), 1 + 3 + 1 + 17)
        self.assertEqual(len(records), 19)

    def test_empty_window_reads_no_table(self):
        layer = _layer()
        layer['features'] = []
        records, calls = run_fetch(layer=layer)
        self.assertEqual(records, [])
        self.assertEqual(len(calls), 3)          # count, read, the newest-row probe for the lag note

    def test_count_shortfall_raises(self):
        with self.assertRaisesRegex(RuntimeError, 'read 27 of 30 rows of the scores layer'):
            run_fetch(counts={'4': 30})
        with self.assertRaisesRegex(RuntimeError, 'violations table'):
            run_fetch(counts={'3': 500})

    def test_bad_count_raises(self):
        with self.assertRaisesRegex(RuntimeError, 'no count'):
            run_fetch(answers={'4/count': _Resp({'features': []})})

    def test_http_errors(self):
        with mock.patch.object(dp, 'DFW_HOLE_RETRIES', 2):
            with self.assertRaisesRegex(RuntimeError, 'HTTP 503'):
                run_fetch(answers={'4/count': [_Resp(None, 503, 'x'), _Resp(None, 503, 'x')]})
        with self.assertRaisesRegex(RuntimeError, 'HTTP 403'):
            run_fetch(answers={'4/count': _Resp(None, 403, '<html>', ctype='text/html')})
        with self.assertRaisesRegex(RuntimeError, 'non-JSON'):
            run_fetch(answers={'3/count': _Resp(None, 200, '<html>', ctype='text/html')})
        with self.assertRaisesRegex(RuntimeError, 'scores layer unreadable'):
            run_fetch(answers={'4': _Resp({'error': {'code': 400, 'message': 'Invalid query'}})})
        with self.assertRaisesRegex(RuntimeError, 'violations table unreadable'):
            run_fetch(answers={'3': [_Resp(None, 500, 'x')] * 3})

    def test_transient_count_error_is_retried(self):
        records, _ = run_fetch(answers={'4/count': [_Resp(None, 502, 'x')]})
        self.assertEqual(len(records), 19)

    def test_wrong_shape_raises(self):
        with self.assertRaisesRegex(RuntimeError, 'unreadable'):
            run_fetch(answers={'4': _Resp({'rows': []})})
        broken = _layer()
        broken['features'][3]['attributes']['OBJECTID'] = 1      # pagination does not advance
        with self.assertRaisesRegex(RuntimeError, 'unreadable'):
            run_fetch(layer=broken)

    def test_deadline_skips_without_requests(self):
        dp._FETCH_DEADLINE = 1.0     # long past
        records, calls = run_fetch()
        self.assertEqual((records, calls), ([], []))
        self.assertEqual(dp._GROUP_STATUS['Garland'][0], 'skipped')


class TestHelpers(GarlandBase):
    def test_dates_are_utc_dates(self):
        self.assertEqual(dp._gar_date(1795132800000), '2026-11-20')
        self.assertEqual(dp._gar_date(_epoch_ms('2026-01-07')), '2026-01-07')
        self.assertIsNone(dp._gar_date(None))
        self.assertIsNone(dp._gar_date('x'))

    def test_point(self):
        self.assertEqual(dp._gar_point({'x': -96.6, 'y': 32.9}, {}), (-96.6, 32.9))
        self.assertEqual(dp._gar_point(None, {'GIS_LATITUDE': 32.9, 'GIS_LONGITUDE': -96.6}), (-96.6, 32.9))
        self.assertEqual(dp._gar_point(None, {'GIS_LATITUDE': -96.6, 'GIS_LONGITUDE': 32.9}), (-96.6, 32.9))
        self.assertEqual(dp._gar_point({'x': 2.3, 'y': 48.8}, {}), (None, None))     # not Texas
        self.assertEqual(dp._gar_point({'x': 'NaN', 'y': None}, {'GIS_LATITUDE': 0}), (None, None))

    def test_item_weights(self):
        self.assertEqual([dp._gar_item_weight(n) for n in (1, 20, 21, 33, 34, 47)], [3, 3, 2, 2, 1, 1])

    def test_jev_policy(self):
        self.assertEqual(jev_reader.score_policy(dp.GAR_SOURCE, 95), 'official')
        self.assertEqual(jev_reader.tier_policy(dp.GAR_SOURCE), 'fixed')

    def test_dispatch_registers_the_group(self):
        with open(os.path.join(os.path.dirname(FIXTURES), '..', '..', 'dinescores_pipeline.py')) as fh:
            src = fh.read()
        self.assertIn("(('garland', 'dfw'), GAR_GROUP, fetch_garland)", src)


if __name__ == '__main__':
    unittest.main()
