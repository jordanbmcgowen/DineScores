"""Tests for the Lewisville, TX fetcher (the city's HealthInspectionScores
web service). No network: requests.get is mocked with eight rows trimmed
verbatim from the live 365-day answer captured 2026-11-03
(tests/fixtures/lewisville/bydate.json: scored rows, a 'PASS' and a
'NO SCORE' row, a same-day pair for one permit) and the service's own error
marker (bydate_error.json)."""
import json
import logging
import os
import ssl
import sys
import unittest
from datetime import datetime, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'lewisville')


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


class _Resp:
    def __init__(self, payload, status=200, text=None, ctype='application/json; charset=utf-8'):
        self._payload = payload
        self.status_code = status
        self.headers = {'content-type': ctype}
        self.text = text if text is not None else json.dumps(payload)
        self.content = self.text.encode()

    def json(self):
        if self._payload is None:
            raise ValueError('no JSON')
        return self._payload


def run_fetch(since=None, limit=None, payload=None, answers=None):
    """fetch_lewisville with requests.get mocked; `answers` (a list of _Resp
    or exceptions) overrides the fixture answer in order."""
    payload = payload if payload is not None else _load('bydate.json')
    answers = list(answers or [])
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None, verify=True, **kw):
        calls.append({'url': url, 'params': params, 'verify': verify, 'headers': headers})
        if answers:
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer
        return _Resp(payload)

    with mock.patch.object(dp.requests, 'get', side_effect=fake_get), \
            mock.patch.object(dp.time, 'sleep'):
        records = dp.fetch_lewisville(since_date=since, limit=limit)
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


class LewisvilleBase(unittest.TestCase):
    def setUp(self):
        self._saved = _snapshot()
        dp._GROUP_NOTES.clear()
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._FETCH_DEADLINE = None
        dp.log.setLevel(logging.WARNING)

    def tearDown(self):
        _restore(self._saved)


class RequestShapeTests(LewisvilleBase):
    def test_one_get_with_the_window_and_the_ca_bundle(self):
        _, calls = run_fetch(since='2026-10-04T00:00:00')
        self.assertEqual(len(calls), 1)
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.assertEqual(calls[0]['url'],
                         f'{dp.LEW_API}?webservice={dp.LEW_SERVICE}&DateBegin=2026-10-04&DateEnd={tomorrow}')
        self.assertIsNone(calls[0]['params'])
        self.assertEqual(calls[0]['verify'], dp.LEW_CA_BUNDLE)       # verified, against the bundle
        self.assertEqual(calls[0]['headers']['User-Agent'], dp.CHROME_UA)
        _, calls = run_fetch(since='2026-10-04')
        self.assertIn('DateBegin=2026-10-04&', calls[0]['url'])

    def test_the_bundle_is_certifi_plus_the_godaddy_intermediate(self):
        self.assertTrue(os.path.exists(dp.LEW_CA_BUNDLE))
        with open(dp.LEW_CA_BUNDLE) as fh:
            text = fh.read()
        self.assertIn('Go Daddy Secure Certificate Authority - G2', text)   # provenance header
        self.assertIn('certs.godaddy.com/repository/gdig2.crt', text)
        self.assertGreater(text.count('-----BEGIN CERTIFICATE-----'), 50)   # the public roots too
        ctx = ssl.create_default_context(cafile=dp.LEW_CA_BUNDLE)           # loads as a CA file
        self.assertTrue(ctx.get_ca_certs())
        with mock.patch.object(dp.os.path, 'exists', return_value=False):
            with self.assertLogs('dinescores', level='WARNING'):
                self.assertIs(dp._lew_verify(), True)                      # never False

    def test_window_start_is_floored_at_the_services_retention(self):
        floor = (datetime.now() - timedelta(days=dp.LEW_RETENTION_DAYS)).strftime('%Y-%m-%d')
        _, calls = run_fetch(since=None)
        self.assertIn(f'DateBegin={floor}&', calls[0]['url'])
        _, calls = run_fetch(since='2024-01-01T00:00:00')
        self.assertIn(f'DateBegin={floor}&', calls[0]['url'])


class RecordTests(LewisvilleBase):
    @classmethod
    def setUpClass(cls):
        saved = _snapshot()                      # the class-level fetch leaves no note or hold behind
        try:
            cls.records, _ = run_fetch(since='2026-01-01')
        finally:
            _restore(saved)
        cls.by_id = _by_id(cls.records)

    def test_scored_record_whole_dict(self):
        self.assertEqual(self.by_id['lew_271483'], {
            'name': "Luci's Bakery",
            'address': '1305 S Sh 121 Bus Suite 166',
            'city': 'Lewisville',
            'state': 'TX',
            'zip': '',
            'latitude': 33.025486628,
            'longitude': -96.995372584,
            'inspection_date': '2026-11-02',
            'original_score': 90,
            'risk_score': 90,                      # the city's official 0-100 score
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': '',
            'results': '',
            'source': 'Lewisville Environmental Health',
            'source_url': 'https://eservices.cityoflewisville.com/healthinspectionscores/',
            'source_id': 'lew_271483',
            'metro': 'DFW',
        })
        self.assertEqual(dp.calculate_vetted_grade(self.by_id['lew_271483']['risk_score']), 'A')
        self.assertEqual(dp.calculate_vetted_grade(self.by_id['lew_243637']['risk_score']), 'C')  # 75

    def test_unscored_visits_are_dropped_not_shipped_as_100(self):
        self.assertNotIn('lew_241563', self.by_id)        # 'PASS' (a shaved-ice stand)
        self.assertNotIn('lew_261736', self.by_id)        # 'NO SCORE' (a daycare)
        self.assertEqual(len(self.records), 6)
        self.assertTrue(all(0 < r['risk_score'] <= 100 for r in self.records))

    def test_a_mostly_unscored_window_raises_instead_of_an_empty_green_run(self):
        payload = _load('bydate.json')
        base = payload['projects'][-1]
        payload['projects'] = []
        for i in range(24):                               # 24 rows, 13 with a re-typed Score
            row = dict(base)
            row['InspectionID'] = 900000 + i
            row['Score'] = '95.0' if i < 13 else '95'
            payload['projects'].append(row)
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(since='2026-01-01', payload=payload)
        self.assertIn('no usable Score', str(ctx.exception))
        for row in payload['projects'][:2]:
            row['Score'] = '95'                           # 11 of 24: data, dropped quietly
        records, _ = run_fetch(since='2026-01-01', payload=payload)
        self.assertEqual(len(records), 13)

    def test_school_and_child_care_kitchens_are_dropped_by_name(self):
        payload = _load('bydate.json')
        for row in payload['projects']:
            if row['InspectionID'] == 271483:
                row['BusinessName'] = 'LISD - DEGAN ELEMENTARY'
            if row['InspectionID'] == 271487:
                row['BusinessName'] = 'KIDDIE ACADEMY OF CASTLE HILLS'
        by_id = _by_id(run_fetch(since='2026-01-01', payload=payload)[0])
        self.assertNotIn('lew_271483', by_id)
        self.assertNotIn('lew_271487', by_id)
        self.assertEqual(by_id['lew_251498']['name'], 'Lake Park Baseball Concessions')   # a concession stays

    def test_same_day_visits_of_one_permit_both_ship(self):
        self.assertIn('lew_251498', self.by_id)
        self.assertIn('lew_251499', self.by_id)
        self.assertEqual(self.by_id['lew_251498']['inspection_date'], '2026-05-27')
        rid = {dp.make_restaurant_id(r['name'], r['address'], r['city'])
               for r in self.records if r['source_id'] in ('lew_251498', 'lew_251499')}
        self.assertEqual(len(rid), 1)

    def test_dates_are_the_local_date_not_a_utc_conversion(self):
        self.assertEqual(dp._lew_date('2026-11-02T13:44:00.000Z'), '2026-11-02')
        self.assertEqual(dp._lew_date('2026-03-26T00:00:00.000Z'), '2026-03-26')
        self.assertIsNone(dp._lew_date(''))
        self.assertIsNone(dp._lew_date('1900-01-01T00:00:00.000Z'))

    def test_order_and_ids(self):
        dates = [r['inspection_date'] for r in self.records]
        self.assertEqual(dates, sorted(dates))
        again, _ = run_fetch(since='2026-01-01')
        for a, b in zip(self.records, again):
            self.assertEqual(dp.make_restaurant_id(a['name'], a['address'], a['city']),
                             dp.make_restaurant_id(b['name'], b['address'], b['city']))
        r = self.by_id['lew_271483']
        self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']), '48616de746aca82b')
        self.assertEqual(dp._dfw_hole_title("POPEYE'S LOUISIANA KITCHEN"), "Popeye's Louisiana Kitchen")
        self.assertEqual(dp._dfw_hole_title('Already Mixed  Case'), 'Already Mixed Case')

    def test_no_holds_are_ever_written(self):
        self.assertEqual(dp._FRESHNESS_HOLDS, {})
        self.assertEqual(dp._GROUP_STATUS, {})


class WindowTests(LewisvilleBase):
    def test_window_is_reapplied_and_limit_keeps_the_newest(self):
        records, _ = run_fetch(since='2026-10-01')
        self.assertEqual(sorted(_by_id(records)), ['lew_270819', 'lew_271483', 'lew_271487'])
        records, _ = run_fetch(since='2026-01-01', limit=2)
        self.assertEqual(sorted(_by_id(records)), ['lew_271483', 'lew_271487'])
        self.assertNotIn('Lewisville', dp._GROUP_NOTES)

    def test_duplicate_inspection_ids_collapse(self):
        payload = _load('bydate.json')
        payload['projects'].append(dict(payload['projects'][-1]))
        records, _ = run_fetch(since='2026-01-01', payload=payload)
        self.assertEqual(len(records), 6)

    def test_zero_or_missing_coordinates_become_none(self):
        payload = _load('bydate.json')
        for row in payload['projects']:
            if row['InspectionID'] == 271483:
                row['Lat'], row['Lng'] = 0, 0
            if row['InspectionID'] == 270819:
                row.pop('Lat'); row['Lng'] = None
        by_id = _by_id(run_fetch(since='2026-01-01', payload=payload)[0])
        self.assertEqual((by_id['lew_271483']['latitude'], by_id['lew_271483']['longitude']), (None, None))
        self.assertEqual((by_id['lew_270819']['latitude'], by_id['lew_270819']['longitude']), (None, None))
        self.assertEqual(dp._dfw_hole_coord('33.0'), 33.0)
        self.assertIsNone(dp._dfw_hole_coord('abc'))

    def test_source_lag_note_when_the_service_is_behind(self):
        payload = _load('bydate.json')
        for row in payload['projects']:
            row['InspectionDate'] = '2026-01-15T09:00:00.000Z'
        run_fetch(since='2026-01-01', payload=payload)
        self.assertIn('Lewisville', dp._GROUP_NOTES)
        self.assertIn('2026-01-15', dp._GROUP_NOTES['Lewisville'])
        dp._GROUP_NOTES.clear()
        run_fetch(since='2026-01-01', payload=payload, limit=2)      # a bounded read: no note
        self.assertNotIn('Lewisville', dp._GROUP_NOTES)

    def test_empty_window_is_an_empty_list(self):
        records, _ = run_fetch(since='2026-10-01', payload={'projects': []})
        self.assertEqual(records, [])


class FailureTests(LewisvilleBase):
    def test_the_services_error_marker_raises(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01', payload=_load('bydate_error.json'))

    def test_wrong_shapes_raise(self):
        for payload in ([], {'rows': []}, {'projects': 'nope'}, 'text'):
            with self.subTest(payload=payload):
                with self.assertRaises(RuntimeError):
                    run_fetch(since='2026-10-01', payload=payload)

    def test_non_json_and_http_errors_raise(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01', answers=[_Resp(None, 200, text='<html>maintenance</html>', ctype='text/html')])
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01', answers=[_Resp(None, 404, text='gone', ctype='text/html')])

    def test_server_errors_are_retried_then_raise(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01', answers=[_Resp(None, 503, text='down')] * dp.DFW_HOLE_RETRIES)
        answers = [_Resp(None, 503, text='down'), _Resp(_load('bydate.json'))]
        records, calls = run_fetch(since='2026-01-01', answers=answers)
        self.assertEqual(len(calls), 2)                        # one retry, then the data
        self.assertEqual(len(records), 6)

    def test_connection_errors_raise_after_the_ladder(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-01', answers=[dp.requests.ConnectionError('reset')] * dp.DFW_HOLE_RETRIES)

    def test_tls_failure_raises_at_once_and_is_never_relaxed(self):
        with self.assertRaises(RuntimeError):
            _, calls = run_fetch(since='2026-10-01', answers=[dp.requests.exceptions.SSLError('bad chain')])
        src = open(dp.__file__).read()
        self.assertNotIn('verify=False', src)


if __name__ == '__main__':
    unittest.main()
