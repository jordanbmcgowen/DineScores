"""Tests for fetch_santa_clara (Santa Clara County, CA — the county's LIVES
feed on data.sccgov.org). No network: requests.get is mocked over small
fixtures captured from the three live tables on 2026-10-10."""
import copy
import json
import os
import re
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'santa_clara')


def _load(name):
    with open(os.path.join(FIXTURES, name)) as f:
        return json.load(f)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _page(rows, params):
    off = int(params.get('$offset', 0))
    lim = int(params.get('$limit', 50000))
    return rows[off:off + lim]


def fake_socrata(inspections, violations, business, calls):
    """A requests.get stand-in that answers the three SCC tables the way
    Socrata would for the handful of query shapes the fetcher uses."""
    def get(url, params=None, headers=None, timeout=None, **kw):
        params = dict(params or {})
        calls.append((url, params))
        if url == dp.SCC_INSPECTIONS_URL:
            if '$select' in params:
                return FakeResponse([{'max_date': max(r['date'] for r in inspections)}])
            rows = inspections
            where = params.get('$where')
            if where:
                m = re.fullmatch(r"date >= '(\d{8})'", where)
                assert m, f'unexpected $where {where!r}'
                rows = [r for r in rows if r['date'] >= m.group(1)]
            rows = sorted(rows, key=lambda r: (r['date'], r['inpsection_id']), reverse=True)
            return FakeResponse(_page(rows, params))
        if url == dp.SCC_VIOLATIONS_URL:
            rows = violations
            where = params.get('$where')
            if where:
                m = re.fullmatch(r"inspection_id in \((.*)\)", where)
                assert m, f'unexpected $where {where!r}'
                ids = set(re.findall(r"'([^']*)'", m.group(1)))
                rows = [v for v in rows if v['inspection_id'] in ids]
            return FakeResponse(_page(rows, params))
        if url == dp.SCC_BUSINESS_URL:
            return FakeResponse(_page(business, params))
        raise AssertionError(f'unexpected URL {url}')
    return get


class SantaClaraFetchTests(unittest.TestCase):
    def setUp(self):
        self.inspections = _load('inspections.json')
        self.violations = _load('violations.json')
        self.business = _load('business.json')
        self.calls = []
        self._notes = dict(dp._GROUP_NOTES)
        dp._GROUP_NOTES.clear()

    def tearDown(self):
        dp._GROUP_NOTES.clear()
        dp._GROUP_NOTES.update(self._notes)

    def run_fetch(self, since_date=None, limit=None, inspections=None, violations=None,
                  business=None):
        self.calls.clear()
        fake = fake_socrata(inspections if inspections is not None else self.inspections,
                            violations if violations is not None else self.violations,
                            business if business is not None else self.business,
                            self.calls)
        with mock.patch.object(dp.requests, 'get', side_effect=fake), \
                mock.patch.object(dp.time, 'sleep'):
            return dp.fetch_santa_clara(since_date=since_date, limit=limit)

    def by_source_id(self, records):
        return {r['source_id']: r for r in records}

    # ── record shape ──────────────────────────────────────────────────────
    def test_record_dict_for_a_scored_green_inspection(self):
        recs = self.by_source_id(self.run_fetch(since_date='2026-10-01'))
        r = recs['scc_DA0LYU07I']
        self.assertEqual(r['name'], "Luigi'S")   # .title() of the ALL-CAPS source, as elsewhere
        self.assertEqual(r['address'], '2495 S Winchester Bl')
        self.assertEqual(r['city'], 'Campbell')
        self.assertEqual(r['state'], 'CA')
        self.assertEqual(r['zip'], '95008')
        self.assertAlmostEqual(r['latitude'], 37.2794778)
        self.assertAlmostEqual(r['longitude'], -121.9497749)
        self.assertEqual(r['inspection_date'], '2026-10-08')
        self.assertEqual(r['original_score'], 89)
        self.assertEqual(r['risk_score'], 89)
        self.assertEqual(r['inspection_type'], 'ROUTINE INSPECTION')
        self.assertEqual(r['results'], 'Green placard')
        self.assertEqual(r['source'], 'Santa Clara County Open Data')
        self.assertEqual(r['source_url'], dp.SCC_SOURCE_PAGE)
        self.assertEqual(r['metro'], '')
        self.assertEqual(r['total_violations'], 5)
        self.assertEqual(len(r['violations']), 5)
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'B')
        for cat, sev, text in r['violations']:
            self.assertIn(sev, ('priority', 'priority_foundation', 'core'))
            self.assertLessEqual(len(text), 500)

    def test_every_fixture_inspection_is_emitted_once(self):
        recs = self.run_fetch()
        self.assertEqual(sorted(r['source_id'] for r in recs),
                         ['scc_DA0LYU07I', 'scc_DA0T90ZVD', 'scc_DAF03LEWI', 'scc_DAWN5KH5H'])
        self.assertTrue(all(r['latitude'] and r['longitude'] for r in recs))

    # ── id stability ──────────────────────────────────────────────────────
    def test_restaurant_ids_are_stable_across_runs_and_whitespace(self):
        first = {r['source_id']: dp.make_restaurant_id(r['name'], r['address'], r['city'])
                 for r in self.run_fetch()}
        # Second run: the same facilities with sloppier whitespace in BUSINESS.
        business = copy.deepcopy(self.business)
        for b in business:
            b['name'] = '  ' + b['name'].replace(' ', '  ') + ' '
            b['address'] = b['address'].replace(' ', '   ')
            b['city'] = ' ' + b['city'] + '  '
        second = {r['source_id']: dp.make_restaurant_id(r['name'], r['address'], r['city'])
                  for r in self.run_fetch(business=business)}
        self.assertEqual(first, second)
        self.assertEqual(len(set(first.values())), 4)

    # ── severity: the county's critical flag wins ─────────────────────────
    def test_critical_flag_overrides_classifier_and_counts_follow(self):
        recs = self.by_source_id(self.run_fetch())
        red = recs['scc_DAWN5KH5H']
        by_text = {v[2].split(':')[0]: v for v in red['violations']}
        # critical=True on a pest item the regex classifier files as 'core'
        self.assertEqual(by_text['No rodents, insects, birds, or animals'][1], 'priority')
        self.assertEqual(by_text['No rodents, insects, birds, or animals'][0], 'pest_control')
        # critical=False on a 'hot water' item the classifier calls 'priority'
        # is held at priority_foundation
        self.assertEqual(by_text['Hot and cold water available'][1], 'priority_foundation')
        # non-critical core item stays core
        self.assertEqual(by_text['Toxic substances properly identified, stored, used'][1], 'core')
        sevs = [v[1] for v in red['violations']]
        self.assertEqual(red['priority_violations'], sevs.count('priority'))
        self.assertEqual(red['priority_foundation_violations'], sevs.count('priority_foundation'))
        self.assertEqual(red['core_violations'], sevs.count('core'))
        self.assertEqual(red['priority_violations'], 1)
        self.assertEqual(red['total_violations'], 5)

        green = recs['scc_DA0LYU07I']
        hand = next(v for v in green['violations'] if v[2].startswith('Signs posted'))
        self.assertEqual(hand[0], 'personal_hygiene')      # handwashing text...
        self.assertEqual(hand[1], 'priority_foundation')   # ...but not county-critical
        self.assertEqual(green['priority_violations'], 0)

    def test_critical_accepts_string_booleans(self):
        self.assertEqual(dp._scc_violation({'description': 'Floor clean', 'critical': 'true',
                                            'violation_comment': 'Soiled floor'})[1], 'priority')
        self.assertEqual(dp._scc_violation({'description': 'Floor clean', 'critical': 'false',
                                            'violation_comment': 'Soiled floor'})[1], 'core')
        self.assertIsNone(dp._scc_violation({'description': '', 'critical': True,
                                             'violation_comment': None}))

    # ── violation text ────────────────────────────────────────────────────
    def test_corrective_action_tail_is_stripped_but_observation_kept(self):
        recs = self.by_source_id(self.run_fetch())
        flies = next(v for v in recs['scc_DA0LYU07I']['violations']
                     if v[2].startswith('No rodents'))
        self.assertEqual(flies[2], 'No rodents, insects, birds, or animals: Two house flies '
                                   'found at kitchen/prep area. No direct contamination observed.')
        for r in recs.values():
            for v in r['violations']:
                self.assertNotIn('[CA]', v[2])
                self.assertNotIn('\n', v[2])
        self.assertEqual(dp._scc_violation_text('Item', None), 'Item')
        self.assertEqual(dp._scc_violation_text('', 'Seen [CA] fix it'), 'Seen')
        self.assertEqual(dp._scc_violation_text('Item', '[CA] fix it'), 'Item')

    # ── scoring ───────────────────────────────────────────────────────────
    def test_red_placard_caps_risk_score_to_an_f(self):
        r = self.by_source_id(self.run_fetch())['scc_DAWN5KH5H']
        self.assertEqual(r['original_score'], 83)       # the county's own score survives
        self.assertEqual(r['risk_score'], dp.SCC_RED_PLACARD_CAP)
        self.assertEqual(r['risk_score'], 55)
        self.assertEqual(r['results'], 'Red placard (closed)')
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'F')

    def test_red_placard_cap_never_raises_a_lower_score(self):
        inspections = copy.deepcopy(self.inspections)
        for i in inspections:
            if i['inpsection_id'] == 'DAWN5KH5H':
                i['score'] = '40'
        r = self.by_source_id(self.run_fetch(inspections=inspections))['scc_DAWN5KH5H']
        self.assertEqual(r['risk_score'], 40)
        self.assertEqual(r['original_score'], 40)

    def test_yellow_placard_is_spelled_out_capped_at_c_and_blank_result_is_empty(self):
        inspections = copy.deepcopy(self.inspections)
        for i in inspections:
            if i['inpsection_id'] == 'DA0LYU07I':
                i['result'] = 'Y'
            elif i['inpsection_id'] == 'DA0T90ZVD':
                i.pop('result')
        recs = self.by_source_id(self.run_fetch(inspections=inspections))
        self.assertEqual(recs['scc_DA0LYU07I']['results'], 'Yellow placard (conditional pass)')
        # A conditional pass never grades above C, whatever the point score.
        self.assertEqual(recs['scc_DA0LYU07I']['risk_score'], dp.SCC_YELLOW_PLACARD_CAP)
        self.assertEqual(recs['scc_DA0LYU07I']['original_score'], 89)
        self.assertEqual(dp.calculate_vetted_grade(recs['scc_DA0LYU07I']['risk_score']), 'C')
        self.assertEqual(recs['scc_DA0T90ZVD']['results'], '')

    def test_missing_score_is_computed_from_violations(self):
        recs = self.by_source_id(self.run_fetch())
        # Real unscored follow-up with no violations on file: a clean 100.
        clean = recs['scc_DAF03LEWI']
        self.assertIsNone(clean['original_score'])
        self.assertEqual(clean['risk_score'], 100)
        self.assertEqual(clean['total_violations'], 0)
        self.assertEqual(clean['inspection_type'], 'FOLLOW-UP INSPECTION')
        self.assertEqual(clean['name'], 'Shoreline Amphitheatre - Commissary Warehouse')
        self.assertEqual(clean['city'], 'Mountain View')

        # The same inspection with the score blanked must fall back to the
        # 5/2/1 computation over its (county-anchored) severities.
        inspections = copy.deepcopy(self.inspections)
        for i in inspections:
            if i['inpsection_id'] == 'DA0T90ZVD':
                i.pop('score')
        r = self.by_source_id(self.run_fetch(inspections=inspections))['scc_DA0T90ZVD']
        self.assertIsNone(r['original_score'])
        expected, pv, pfv, cv = dp.calc_risk_score([tuple(v) for v in r['violations']])
        self.assertEqual(r['risk_score'], expected)
        self.assertEqual((pv, pfv, cv), (r['priority_violations'],
                                         r['priority_foundation_violations'],
                                         r['core_violations']))
        self.assertEqual(pv, 1)   # the county-critical K07 temperature item
        self.assertEqual(r['risk_score'], 100 - 5 * pv - 2 * pfv - cv)

    # ── window ────────────────────────────────────────────────────────────
    def test_window_converts_iso_since_to_the_feeds_yyyymmdd_text(self):
        self.assertEqual(dp._scc_window_start('2026-09-01T00:00:00'), '20260901')
        self.assertEqual(dp._scc_window_start('2026-09-01'), '20260901')
        self.assertIsNone(dp._scc_window_start(None))
        self.assertIsNone(dp._scc_window_start(''))
        self.assertIsNone(dp._scc_window_start('garbage'))

        for since in ('2026-09-01T00:00:00', '2026-09-01'):
            self.run_fetch(since_date=since)
            insp_calls = [p for u, p in self.calls
                          if u == dp.SCC_INSPECTIONS_URL and '$select' not in p]
            self.assertTrue(insp_calls)
            self.assertEqual(insp_calls[0]['$where'], "date >= '20260901'")
            self.assertEqual(insp_calls[0]['$order'], 'date DESC, inpsection_id')

        self.run_fetch(since_date=None)
        insp_calls = [p for u, p in self.calls
                      if u == dp.SCC_INSPECTIONS_URL and '$select' not in p]
        self.assertNotIn('$where', insp_calls[0])

    def test_window_filters_rows_and_empty_window_returns_nothing(self):
        inspections = copy.deepcopy(self.inspections)
        inspections[0]['date'] = '20260815'
        recs = self.run_fetch(since_date='2026-09-01', inspections=inspections)
        self.assertEqual(len(recs), 3)
        self.assertNotIn('scc_' + inspections[0]['inpsection_id'],
                         [r['source_id'] for r in recs])
        self.assertEqual(self.run_fetch(since_date='2027-01-01'), [])

    def test_test_mode_limit_caps_the_page_and_the_results(self):
        recs = self.run_fetch(limit=2)
        self.assertEqual(len(recs), 2)
        insp_calls = [p for u, p in self.calls
                      if u == dp.SCC_INSPECTIONS_URL and '$select' not in p]
        self.assertEqual(insp_calls[0]['$limit'], 2)

    # ── violations fetch strategy ─────────────────────────────────────────
    def test_violations_are_requested_in_id_chunks_for_narrow_windows(self):
        self.run_fetch(since_date='2026-10-01')
        viol_calls = [p for u, p in self.calls if u == dp.SCC_VIOLATIONS_URL]
        self.assertEqual(len(viol_calls), 1)
        where = viol_calls[0]['$where']
        self.assertTrue(where.startswith('inspection_id in ('))
        self.assertEqual(sorted(re.findall(r"'([^']*)'", where)),
                         ['DA0LYU07I', 'DA0T90ZVD', 'DAF03LEWI', 'DAWN5KH5H'])

    def test_violation_chunking_and_bulk_switch(self):
        ids = [f'ID{n:05d}' for n in range(250)]
        calls = []
        fake = fake_socrata([], [{'inspection_id': i, 'description': 'x', 'critical': False,
                                  'violation_comment': 'y'} for i in ids], [], calls)
        with mock.patch.object(dp.requests, 'get', side_effect=fake), \
                mock.patch.object(dp.time, 'sleep'):
            grouped = dp._scc_fetch_violations(ids, window_days=30)
            self.assertEqual(len(calls), 3)       # 100 + 100 + 50
            self.assertTrue(all('$where' in p for _, p in calls))
            self.assertEqual(len(grouped), 250)

            calls.clear()
            many = [f'ID{n:05d}' for n in range(400)]
            grouped = dp._scc_fetch_violations(many, window_days=None)
            self.assertEqual(len(calls), 1)       # whole table, one page
            self.assertNotIn('$where', calls[0][1])
            self.assertEqual(len(grouped), 250)   # only ids present in the table

            calls.clear()
            dp._scc_fetch_violations(many, window_days=30)
            self.assertEqual(len(calls), 4)       # 60-day rule not met: still chunked

    # ── joins and edge cases ──────────────────────────────────────────────
    def test_inspection_without_a_business_row_is_skipped(self):
        inspections = copy.deepcopy(self.inspections)
        inspections.append({'business_id': 'PR9999999', 'inpsection_id': 'DAORPHAN1',
                            'date': '20261008', 'score': '95', 'result': 'G',
                            'type': 'ROUTINE INSPECTION'})
        with self.assertLogs('dinescores', level='WARNING') as logs:
            recs = self.run_fetch(inspections=inspections)
        self.assertEqual(len(recs), 4)
        self.assertNotIn('scc_DAORPHAN1', [r['source_id'] for r in recs])
        self.assertTrue(any('1 inspections skipped' in line for line in logs.output))

    def test_same_day_routine_is_emitted_before_its_followup(self):
        inspections = copy.deepcopy(self.inspections)
        inspections.append({'business_id': 'PR0301404', 'inpsection_id': 'DA0FOLLOW',
                            'date': '20261008', 'result': 'G', 'type': 'FOLLOW-UP INSPECTION'})
        recs = self.run_fetch(inspections=inspections)
        ids = [r['source_id'] for r in recs]
        self.assertLess(ids.index('scc_DA0LYU07I'), ids.index('scc_DA0FOLLOW'))
        self.assertEqual(recs[ids.index('scc_DA0FOLLOW')]['original_score'], None)

    def test_same_day_closure_is_emitted_before_an_equal_green_visit(self):
        # Two unscored follow-ups on one day, Green id sorting before Red: the
        # closure must still come first so the outputs keep it.
        inspections = copy.deepcopy(self.inspections)
        inspections += [
            {'business_id': 'PR0301404', 'inpsection_id': 'DA0AAAAAA', 'date': '20261008',
             'result': 'G', 'type': 'FOLLOW-UP INSPECTION'},
            {'business_id': 'PR0301404', 'inpsection_id': 'DA0ZZZZZZ', 'date': '20261008',
             'result': 'R', 'type': 'FOLLOW-UP INSPECTION'},
        ]
        ids = [r['source_id'] for r in self.run_fetch(inspections=inspections)]
        self.assertLess(ids.index('scc_DA0LYU07I'), ids.index('scc_DA0ZZZZZZ'))
        self.assertLess(ids.index('scc_DA0ZZZZZZ'), ids.index('scc_DA0AAAAAA'))

    def test_bad_coordinates_and_zip_plus_four(self):
        business = copy.deepcopy(self.business)
        for b in business:
            if b['business_id'] == 'PR0301404':
                b['latitude'], b['longitude'] = '', None
                b['postal_code'] = '95008-1234'
        r = self.by_source_id(self.run_fetch(business=business))['scc_DA0LYU07I']
        self.assertIsNone(r['latitude'])
        self.assertIsNone(r['longitude'])
        self.assertEqual(r['zip'], '95008')

    def test_city_typo_is_repaired_and_mixed_case_name_is_kept(self):
        business = copy.deepcopy(self.business)
        for b in business:
            if b['business_id'] == 'PR0302910':
                b['city'] = 'MOUNTIAN VIEW'
                b['name'] = 'Taco Bell - 24783'
        r = self.by_source_id(self.run_fetch(business=business))['scc_DA0T90ZVD']
        self.assertEqual(r['city'], 'Mountain View')
        self.assertEqual(r['name'], 'Taco Bell - 24783')

    def test_source_lag_note_uses_the_feeds_newest_date(self):
        inspections = copy.deepcopy(self.inspections)
        for i in inspections:
            i['date'] = '20260101'
        self.run_fetch(inspections=inspections)
        self.assertIn('Santa Clara County', dp._GROUP_NOTES)
        self.assertIn('2026-01-01', dp._GROUP_NOTES['Santa Clara County'])

    def test_iso_date_helper(self):
        self.assertEqual(dp._scc_iso_date('20261008'), '2026-10-08')
        self.assertEqual(dp._scc_iso_date(''), '')
        self.assertEqual(dp._scc_iso_date(None), '')
        self.assertEqual(dp._scc_iso_date('2026-10-08'), '')


if __name__ == '__main__':
    unittest.main()
