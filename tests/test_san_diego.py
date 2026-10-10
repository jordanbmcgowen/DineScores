"""Tests for the San Diego County (sdfoodinfo.org) fetcher. No network: the
search POST is mocked with trimmed records captured from the live endpoint
on 2026-10-10 (tests/fixtures/san_diego/search_page1.json)."""
import copy
import json
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       'fixtures', 'san_diego', 'search_page1.json')


def _payload():
    with open(FIXTURE) as fh:
        return json.load(fh)


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status
        self.content = json.dumps(payload).encode()

    def raise_for_status(self):
        if self.status_code >= 400:
            err = dp.requests.HTTPError(f'{self.status_code}')
            err.response = self
            raise err

    def json(self):
        return self._payload


def _fetch(since=None, limit=None, pages=None):
    """Run fetch_san_diego against the fixture (or a list of page payloads)."""
    pages = pages or [_payload()]
    with mock.patch.object(dp.requests, 'post', side_effect=[_Resp(p) for p in pages]) as post:
        records = dp.fetch_san_diego(since_date=since, limit=limit)
    return records, post


def _by_id(records):
    return {r['source_id']: r for r in records}


class RequestShapeTests(unittest.TestCase):
    def test_form_encoded_window_query(self):
        _, post = _fetch(since='2026-09-01T00:00:00')
        kwargs = post.call_args.kwargs
        self.assertIn('data', kwargs)          # application/x-www-form-urlencoded
        self.assertNotIn('json', kwargs)
        data = dict(kwargs['data'])
        self.assertEqual(data['start_date'], '2026-09-01')
        self.assertEqual(data['end_date'], dp.datetime.now().strftime('%Y-%m-%d'))
        self.assertEqual(data['page_number'], 1)
        self.assertEqual(data['page_count'], dp.SD_PAGE)
        self.assertGreaterEqual(kwargs['timeout'], 240)
        self.assertEqual(post.call_args.args[0], dp.SD_SEARCH_URL)

    def test_no_since_means_the_sites_history_floor(self):
        _, post = _fetch(since=None)
        self.assertEqual(dict(post.call_args.kwargs['data'])['start_date'], dp.SD_HISTORY_START)

    def test_pages_until_total_count_and_dedupes_on_inspection_id(self):
        full = _payload()
        page1 = {'result': full['result'][:2], 'total_count': 3}
        # Page 2 repeats a facility from page 1 (a boundary shift) plus the last one.
        page2 = {'result': full['result'][1:], 'total_count': 3}
        with mock.patch.object(dp, 'SD_PAGE', 2):
            records, post = _fetch(pages=[page1, page2])
        self.assertEqual(post.call_count, 2)
        self.assertEqual(dict(post.call_args_list[1].kwargs['data'])['page_number'], 2)
        ids = [r['source_id'] for r in records]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(len(records), 8)

    def test_exhausted_retries_raise_instead_of_shipping_a_partial_window(self):
        with mock.patch.object(dp.requests, 'post',
                               side_effect=dp.requests.ConnectionError('reset')), \
                mock.patch.object(dp.time, 'sleep'):
            with self.assertRaises(RuntimeError):
                dp.fetch_san_diego(since_date='2026-09-01')

    def test_limit_truncates(self):
        records, _ = _fetch(limit=2)
        self.assertEqual(len(records), 2)


class RecordTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.records, _ = _fetch(since=None)
        cls.by_id = _by_id(cls.records)

    def test_only_routine_and_reinspection_ship(self):
        # Fixture holds 1 Status Verification, 1 Site Investigation and 1
        # 'No Access' re-inspection; none of them become records.
        self.assertEqual(len(self.records), 8)
        self.assertEqual(set(r['inspection_type'] for r in self.records),
                         {'Routine', 'Re-inspection'})
        self.assertNotIn('sd_7903326', self.by_id)   # Status Verification
        self.assertNotIn('sd_7822371', self.by_id)   # Site Investigation
        self.assertNotIn('sd_8015237', self.by_id)   # No Access

    def test_scored_routine_record(self):
        r = self.by_id['sd_7835417']
        self.assertEqual(r, {
            'name': '7-Eleven Store 34177B',
            'address': '1007 05Th Ave',
            'city': 'San Diego',
            'state': 'CA',
            'zip': '92101',
            'latitude': 32.7159517,
            'longitude': -117.1598683,
            'inspection_date': '2026-10-07',
            'original_score': 90,
            'risk_score': 90,
            'priority_violations': 1,
            'priority_foundation_violations': 3,
            'core_violations': 1,
            'total_violations': 5,
            'violations': r['violations'],
            'inspection_type': 'Routine',
            'results': 'Grade A',
            'source': 'San Diego County DEH',
            'source_url': 'https://sdfoodinfo.org/restaurants/restaurant_detail.htm?business_id=211915377781',
            'source_id': 'sd_7835417',
            'metro': 'San Diego',
        })
        self.assertEqual(len(r['violations']), 5)
        self.assertTrue(all(len(v) == 3 for v in r['violations']))

    def test_street_city_zip_split(self):
        r = self.by_id['sd_8029308']
        # '950 06TH AVE, SUITE# C, SAN DIEGO, CA 92101-5231' + zip '92101-5231'
        self.assertEqual(r['address'], '950 06Th Ave, Suite# C')
        self.assertEqual(r['city'], 'San Diego')
        self.assertEqual(r['zip'], '92101')
        self.assertEqual(dp._sd_split_address('123 MAIN ST, LA MESA, CA 91942-1234', 'LA MESA', '91942-1234'),
                         ('123 MAIN ST', 'LA MESA', '91942'))
        # Fields missing: fall back to the address tail.
        self.assertEqual(dp._sd_split_address('123 Main St, Chula Vista, CA 91910', '', ''),
                         ('123 Main St', 'Chula Vista', '91910'))
        self.assertEqual(dp._sd_title('LA JOLLA'), 'La Jolla')
        self.assertEqual(dp._sd_title("JOE'S DINER"), "Joe's Diner")
        self.assertEqual(dp._sd_title('SDUSD - Food & Nutrition Services'),
                         'SDUSD - Food & Nutrition Services')   # not all caps: untouched

    def test_major_flag_overrides_severity_and_counts_follow(self):
        r = self.by_id['sd_7835417']
        sev = {v[2]: v[1] for v in r['violations']}
        # Major vermin item: classifier says core (pest_control), county says major.
        self.assertEqual(sev['23. No rodents, insects, birds or animals: Vermin'], 'priority')
        # Minor water item: classifier says priority (water_sewage), minor never is.
        self.assertEqual(sev['21. Hot & cold water available'], 'priority_foundation')
        self.assertEqual(sev['1b. Food Handler Training'], 'priority_foundation')
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'],
                          r['core_violations']), (1, 3, 1))
        self.assertEqual(sum(1 for v in r['violations'] if v[1] == 'priority'), 1)
        # The description repeats the item name only when it adds something.
        self.assertNotIn(': Food Handler Training', sev)
        self.assertIn('23. No rodents, insects, birds or animals: Vermin', sev)
        self.assertEqual(dp._sd_violation({'violation': 'Vermin', 'violation_text': 'Vermin',
                                           'major_violation': 'N', 'status': 'Out of Compliance - Major',
                                           'violation_accela': '23. No rodents, insects, birds or animals'})[1],
                         'priority')
        self.assertIsNone(dp._sd_violation({'violation': '', 'violation_text': '', 'violation_accela': ''}))

    def test_unscored_reinspection_is_computed_from_violations(self):
        r = self.by_id['sd_7835265']
        self.assertIsNone(r['original_score'])
        # 1 priority (5) + 1 priority_foundation (2) + 1 core (1)
        self.assertEqual(r['risk_score'], 92)
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'],
                          r['core_violations'], r['total_violations']), (1, 1, 1, 3))
        self.assertEqual(r['results'], '')          # plain Complete, no grade
        self.assertEqual(r['inspection_type'], 'Re-inspection')

    def test_closure_caps_risk_so_it_grades_f(self):
        closed = self.by_id['sd_8011542']           # re-inspection, 1 major item → would compute 95
        self.assertEqual(closed['risk_score'], 55)
        self.assertEqual(closed['results'], 'Ordered Closed')
        self.assertEqual(dp.calculate_vetted_grade(closed['risk_score']), 'F')
        routine_closed = self.by_id['sd_7746759']   # routine, score 0, 8 items → computes 76
        self.assertEqual(routine_closed['risk_score'], 55)
        self.assertIsNone(routine_closed['original_score'])
        self.assertEqual((routine_closed['priority_violations'],
                          routine_closed['priority_foundation_violations'],
                          routine_closed['core_violations']), (3, 4, 1))
        reopened = self.by_id['sd_8028857']
        self.assertEqual(reopened['risk_score'], 100)
        self.assertEqual(reopened['results'], 'Approved to Reopen')

    def test_zero_coordinates_become_none(self):
        r = self.by_id['sd_7838954']
        self.assertIsNone(r['latitude'])
        self.assertIsNone(r['longitude'])
        self.assertEqual(r['address'], '12750 Carmel Country Rd, Suite# 100')
        self.assertEqual(r['zip'], '92130')
        self.assertEqual((r['original_score'], r['risk_score'], r['results']), (100, 100, ''))
        self.assertEqual(dp._sd_coord('32.7159517'), 32.7159517)
        self.assertIsNone(dp._sd_coord('0'))
        self.assertIsNone(dp._sd_coord(''))
        self.assertIsNone(dp._sd_coord(None))

    def test_make_restaurant_id_is_stable(self):
        again, _ = _fetch(since=None)
        for a, b in zip(self.records, again):
            self.assertEqual(dp.make_restaurant_id(a['name'], a['address'], a['city']),
                             dp.make_restaurant_id(b['name'], b['address'], b['city']))
        r = self.by_id['sd_7835417']
        self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']), '1473caa1cae6e744')
        # Every inspection of a facility lands on the same restaurant id.
        ids = {dp.make_restaurant_id(x['name'], x['address'], x['city'])
               for x in self.records if x['source_url'].endswith('211982182791')}
        self.assertEqual(len(ids), 1)


class WindowTests(unittest.TestCase):
    def test_completed_date_filter_applies_to_the_full_history(self):
        records, _ = _fetch(since='2026-01-01')
        ids = set(_by_id(records))
        self.assertNotIn('sd_7157731', ids)        # 2024-02-06 routine
        self.assertNotIn('sd_7835265', ids)        # 2025-08-23 re-inspection
        self.assertEqual(len(records), 6)
        records, _ = _fetch(since='2026-09-01T00:00:00')
        self.assertEqual(sorted(_by_id(records)), ['sd_7835417', 'sd_7838954', 'sd_8029308'])
        self.assertTrue(all(r['inspection_date'] >= '2026-09-01' for r in records))

    def test_source_lag_note_uses_the_newest_date_seen(self):
        saved = dict(dp._GROUP_NOTES)
        try:
            dp._GROUP_NOTES.clear()
            payload = _payload()
            for fac in payload['result']:
                for insp in fac['inspections']:
                    insp['completed_date'] = '2026-01-15'
            _fetch(since='2026-01-01', pages=[payload])
            self.assertIn('San Diego County', dp._GROUP_NOTES)
            self.assertIn('2026-01-15', dp._GROUP_NOTES['San Diego County'])
        finally:
            dp._GROUP_NOTES.clear()
            dp._GROUP_NOTES.update(saved)

    def test_bad_facility_row_is_skipped_not_fatal(self):
        payload = _payload()
        payload['result'].insert(0, 'not-a-dict')
        payload['total_count'] += 1
        records, _ = _fetch(pages=[payload])
        self.assertEqual(len(records), 8)


if __name__ == '__main__':
    unittest.main()
