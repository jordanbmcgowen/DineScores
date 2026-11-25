"""Tests for the El Paso, TX fetcher (foodscores.elpasotexas.gov, one
multipart POST answering the window as an HTML table). No network:
requests.post is mocked with the page captured live 2026-11-22
(tests/fixtures/elpaso/window.html: the 50 rows of the 2026-11-18 answer plus
20 rows picked verbatim from the 2024-01-01 .. 2026-11-23 answer: a failed
routine and its same-day re-inspection, an exact duplicate row, two permits
at one address on one day, the 'EL PASO COUNTY' / 'HORIZON' / 'CANUTILLO79835'
city cells, empty unit fields, a street-less address, site assessments, an
unscored routine, a closure-range score, a school and an adult foster home)
and the same page with an empty table (empty.html, a one-day window)."""
import logging
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'elpaso')


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as fh:
        return fh.read()


CHALLENGE = ('<!DOCTYPE html><html lang="en-US"><head><title>Just a moment...</title></head>'
             '<body><div id="challenge-platform"></div></body></html>')


class _Resp:
    def __init__(self, text, status=200, headers=None):
        self.text = text
        self.status_code = status
        self.headers = {'content-type': 'text/html; charset=UTF-8'}
        self.headers.update(headers or {})
        self.content = text.encode()


def run_fetch(since=None, limit=None, body=None, answers=None, floor=0):
    """fetch_elpaso with requests.post mocked; `answers` (a list of _Resp or
    exceptions) overrides the fixture answer in order. `floor` patches
    EP_MIN_ROWS_PER_DAY (the fixture holds 70 rows, far below the floor of a
    multi-year window)."""
    body = body if body is not None else _load('window.html')
    answers = list(answers or [])
    calls = []

    def fake_post(url, files=None, data=None, headers=None, timeout=None, **kw):
        calls.append({'url': url, 'files': files, 'data': data, 'headers': headers, 'timeout': timeout})
        if answers:
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer
        return _Resp(body)

    with mock.patch.object(dp.requests, 'post', side_effect=fake_post), \
            mock.patch.object(dp.time, 'sleep'), \
            mock.patch.object(dp, 'EP_MIN_ROWS_PER_DAY', floor):
        records = dp.fetch_elpaso(since_date=since, limit=limit)
    return records, calls


def _by_id(records):
    return {r['source_id']: r for r in records}


def _days_ago(n):
    return (datetime.now() - timedelta(days=n)).strftime('%Y-%m-%d')


def _snapshot():
    return (dict(dp._GROUP_NOTES), dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
            dp._FETCH_DEADLINE, dp.log.level)


def _restore(saved):
    dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(saved[0])
    dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(saved[1])
    dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(saved[2])
    dp._FETCH_DEADLINE = saved[3]
    dp.log.setLevel(saved[4])


class ElPasoBase(unittest.TestCase):
    def setUp(self):
        self._saved = _snapshot()
        dp._GROUP_NOTES.clear()
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._FETCH_DEADLINE = None
        dp.log.setLevel(logging.CRITICAL)

    def tearDown(self):
        _restore(self._saved)


class RequestShapeTests(ElPasoBase):
    def test_one_multipart_post_with_the_window(self):
        _, calls = run_fetch(since='2024-01-01T00:00:00')
        self.assertEqual(len(calls), 1)
        c = calls[0]
        self.assertEqual(c['url'], 'https://foodscores.elpasotexas.gov/')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.assertEqual(c['files']['fromDate'], (None, '2024-01-01'))
        self.assertEqual(c['files']['toDate'], (None, tomorrow))         # toDate is exclusive
        for blank in ('businessName', 'address', 'city', 'zipCode', 'minScore', 'maxScore', 'search'):
            self.assertEqual(c['files'][blank], (None, ''))
        self.assertNotIn('mobile', c['files'])                           # mobile units are not asked for
        self.assertIsNone(c['data'])
        self.assertEqual(c['headers']['User-Agent'], dp.CHROME_UA)
        self.assertGreaterEqual(c['timeout'], 120)

    def test_weekly_windows_never_start_later_than_the_reread_margin(self):
        _, calls = run_fetch(since=_days_ago(3))
        self.assertEqual(calls[0]['files']['fromDate'], (None, _days_ago(dp.EP_REREAD_DAYS)))
        _, calls = run_fetch(since='2025-06-01')                           # an older start is kept
        self.assertEqual(calls[0]['files']['fromDate'], (None, '2025-06-01'))

    def test_default_windows(self):
        _, calls = run_fetch(since=None, limit=25)                         # test mode
        self.assertEqual(calls[0]['files']['fromDate'], (None, _days_ago(dp.EP_TEST_WINDOW_DAYS)))
        _, calls = run_fetch(since=None)
        self.assertEqual(calls[0]['files']['fromDate'], (None, dp.EP_HISTORY_START))


class RecordTests(ElPasoBase):
    @classmethod
    def setUpClass(cls):
        saved = _snapshot()
        try:
            dp.log.setLevel(logging.CRITICAL)
            cls.records, _ = run_fetch(since='2024-01-01')
        finally:
            _restore(saved)
        cls.by_id = _by_id(cls.records)

    def test_record_whole_dict(self):
        self.assertEqual(self.by_id['elp_HFFX11-00387_20240821'], {
            'name': "Lin's Grand Buffet",
            'address': '1921 Zaragoza Rd Bldg C',
            'city': 'El Paso',
            'state': 'TX',
            'zip': '79938',
            'latitude': None,
            'longitude': None,
            'inspection_date': '2024-08-21',
            'original_score': 68,
            'risk_score': 68,                          # the city's official 0-100 score
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': 'Routine Inspection',
            'results': '',
            'source': 'City of El Paso Public Health',
            'source_url': 'https://foodscores.elpasotexas.gov/',
            'source_id': 'elp_HFFX11-00387_20240821',
            'metro': '',
        })

    def test_counts_and_scope(self):
        # 70 rows: 1 exact duplicate, 3 site assessments, 1 unscored routine,
        # 5 institutional kitchens (3 schools, a child-care center, an adult
        # foster home) and 1 street-less address are not shipped.
        self.assertEqual(len(self.records), 59)
        names = {r['name'] for r in self.records}
        self.assertNotIn('Coronado High School', names)
        self.assertNotIn('Adult Foster Care', names)
        self.assertFalse(any('High School' in n for n in names))
        self.assertNotIn('Starbucks 84092', names)                  # 1300 Site Assessment
        self.assertNotIn('Elemi Restaurant', names)                 # a routine with no score
        self.assertNotIn("Tiff's Treats", names)                    # address 'BLVD, EL PASO, TX 79925'
        self.assertTrue(all(0 < r['risk_score'] <= 100 for r in self.records))
        self.assertTrue(all(r['state'] == 'TX' and r['violations'] == [] for r in self.records))

    def test_same_day_rows_of_one_permit_ship_worst_first(self):
        routine = self.by_id['elp_HFFX11-00387_20240821']
        again = self.by_id['elp_HFFX11-00387_20240821_2']
        self.assertEqual((routine['risk_score'], routine['inspection_type']), (68, 'Routine Inspection'))
        self.assertEqual((again['risk_score'], again['inspection_type']), (87, 'Re-inspection'))
        # one restaurant: the pipeline's same-day rule keeps the 68
        self.assertEqual(dp.make_restaurant_id(routine['name'], routine['address'], routine['city']),
                         dp.make_restaurant_id(again['name'], again['address'], again['city']))
        self.assertGreater(dp._same_day_rank(routine), dp._same_day_rank(again))   # higher = worse

    def test_exact_duplicate_row_ships_once(self):
        walgreens = [r for r in self.records if r['name'] == 'Walgreens #10582']
        self.assertEqual([r['source_id'] for r in walgreens], ['elp_HFFX13-00902_20260821'])

    def test_two_permits_at_one_address_share_a_restaurant(self):
        a = self.by_id['elp_HFFX11-00318_20261113']
        b = self.by_id['elp_HFFX11-00319_20261113']
        self.assertEqual((a['risk_score'], b['risk_score']), (84, 93))
        self.assertEqual((a['city'], a['address']), ('El Paso', '14700 Montana Ave'))   # 'EL PASO COUNTY', 79938
        self.assertEqual(dp.make_restaurant_id(a['name'], a['address'], a['city']),
                         dp.make_restaurant_id(b['name'], b['address'], b['city']))

    def test_cities_and_addresses(self):
        r = self.by_id['elp_HFFX23-00451_20240621']                 # City 'HORIZON'
        self.assertEqual((r['address'], r['city'], r['zip']), ('13700 Horizon Ste A', 'Horizon City', '79928'))
        r = self.by_id['elp_HFFX24-00205_20240712']                 # ', AddrAs#,'
        self.assertEqual(r['address'], '1504 N Zaragoza Rd')
        r = self.by_id['elp_HFFX12-00599_20240605']                 # 'EL PASO, TEXAS 79912'
        self.assertEqual(r['address'], '910 E Redd Ste F & G')
        r = self.by_id['elp_HFFX19-00598_20240131']                 # 'EL PASO 79901', no state
        self.assertEqual(r['address'], '101 El Paso St')
        r = self.by_id['elp_HFFX15-00400_20240320']                 # a bare street
        self.assertEqual(r['address'], '5200 Montana Ave')
        r = self.by_id['elp_HFFX25-00182_20251120']                 # no ZIP anywhere
        self.assertEqual((r['address'], r['zip']), ('1300 Airway', ''))
        self.assertEqual({r['city'] for r in self.records} - {'El Paso'},
                         {'Horizon City', 'Socorro', 'San Elizario', 'Canutillo', 'Anthony', 'Clint',
                          'Fabens', 'Vinton', 'Tornillo'} & {r['city'] for r in self.records})

    def test_city_helper(self):
        self.assertEqual(dp._ep_city('EL PASO COUNTY', '79836'), 'Clint')
        self.assertEqual(dp._ep_city('EL PASO COUNTY', '79938'), 'El Paso')
        self.assertEqual(dp._ep_city('', '79849'), 'San Elizario')
        self.assertEqual(dp._ep_city('', ''), 'El Paso')
        self.assertEqual(dp._ep_city('CANUTILLO79835', '79835'), 'Canutillo')
        self.assertEqual(dp._ep_city('HORIZON', '79928'), 'Horizon City')
        self.assertEqual(dp._ep_city('SAN ELIZARIO', '79849'), 'San Elizario')

    def test_street_helper(self):
        self.assertEqual(dp._ep_street('1881 GEORGE DIETER DR, Addr A# B, EL PASO, TX 79936', 'EL PASO'),
                         '1881 George Dieter Dr, B')
        self.assertEqual(dp._ep_street('6800 DONIPHAN, CANUTILLO79835, TX 79835', 'CANUTILLO79835'),
                         '6800 Doniphan')
        self.assertEqual(dp._ep_street('8825 NORTH LOOP DR, SUITE# 105, EL PASO, TX 79907', 'EL PASO'),
                         '8825 North Loop Dr Ste 105')
        self.assertEqual(dp._ep_street('14290 MONTANA SPC A., EL PASO COUNTY, TX 79938', 'EL PASO COUNTY'),
                         '14290 Montana Spc A')
        self.assertEqual(dp._ep_street('1300 HORIZON BLVD., EL PASO COUNTY, TX 79928', 'EL PASO COUNTY'),
                         '1300 Horizon Blvd')
        self.assertEqual(dp._ep_street('12500 FM 1110, CLINT, TX 79836', 'CLINT'), '12500 FM 1110')
        self.assertEqual(dp._ep_street('0', 'EL PASO'), '')
        self.assertEqual(dp._ep_street('BLVD, EL PASO, TX 79925', 'EL PASO'), '')
        self.assertEqual(dp._ep_street('', 'EL PASO'), '')

    def test_grades_come_from_the_score_alone(self):
        r = self.by_id['elp_HFFX15-00857_20250514']                 # 56: the city's closure range
        self.assertEqual(r['risk_score'], 56)
        for rec in self.records:
            text = dp.build_violation_text(rec['violations'])
            self.assertEqual(text, '')
            self.assertEqual(dp.calculate_vetted_grade(rec['risk_score'], text),
                             dp.calculate_vetted_grade(rec['risk_score']))
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'F')
        self.assertEqual(dp.calculate_vetted_grade(self.by_id['elp_HFFX11-00387_20240821_2']['risk_score']), 'B')

    def test_ids_are_stable_and_unique(self):
        self.assertEqual(len(self.by_id), len(self.records))
        again, _ = run_fetch(since='2024-01-01')
        self.assertEqual([r['source_id'] for r in again], [r['source_id'] for r in self.records])
        self.assertEqual([dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in again],
                         [dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in self.records])
        r = self.by_id['elp_HFFX11-00387_20240821']
        self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']), '775d842c13f2113e')

    def test_order_is_oldest_first(self):
        dates = [r['inspection_date'] for r in self.records]
        self.assertEqual(dates, sorted(dates))
        self.assertEqual(dates[0], '2024-01-31')
        self.assertEqual(dates[-1], '2026-11-18')


class WindowTests(ElPasoBase):
    def test_window_is_reapplied_client_side(self):
        records, _ = run_fetch(since='2026-11-13')
        self.assertTrue(records)
        self.assertTrue(all(r['inspection_date'] >= min('2026-11-13', _days_ago(dp.EP_REREAD_DAYS))
                            for r in records))
        records, _ = run_fetch(since='2026-01-01')
        self.assertEqual(min(r['inspection_date'] for r in records), '2026-08-21')

    def test_limit_keeps_the_newest_and_leaves_no_lag_note(self):
        records, _ = run_fetch(since='2024-01-01', limit=3)
        self.assertEqual(len(records), 3)
        self.assertTrue(all(r['inspection_date'] == '2026-11-18' for r in records))
        self.assertNotIn('El Paso', dp._GROUP_NOTES)

    def test_source_lag_note(self):
        body = _load('window.html').replace('/2026 </td>', '/2025 </td>')   # newest 2025-11-20
        run_fetch(since='2024-01-01', body=body)
        self.assertIn('2025-11-20', dp._GROUP_NOTES.get('El Paso', ''))

    def test_empty_table_for_a_short_window_is_empty(self):
        records, _ = run_fetch(since=None, limit=25, body=_load('empty.html'), floor=dp.EP_MIN_ROWS_PER_DAY)
        self.assertEqual(records, [])
        self.assertEqual(dp._GROUP_STATUS, {})

    def test_too_few_rows_for_a_long_window_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(since='2026-01-01', body=_load('empty.html'), floor=4)
        self.assertIn('only 0 rows', str(ctx.exception))
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-01-01', floor=4)                      # 70 rows for ~330 days
        records, _ = run_fetch(since=_days_ago(10), floor=1)            # 70 rows for 36 days: read
        self.assertIsInstance(records, list)

    def test_no_holds_and_no_status_on_a_clean_read(self):
        run_fetch(since='2024-01-01')
        self.assertEqual(dp._FRESHNESS_HOLDS, {})
        self.assertEqual(dp._GROUP_STATUS, {})


class FailureTests(ElPasoBase):
    def test_cloudflare_challenge_reports_blocked_and_ships_nothing(self):
        for resp in (_Resp(CHALLENGE, 403, {'cf-mitigated': 'challenge'}),
                     _Resp(CHALLENGE, 503),
                     _Resp('<html>Sorry, you have been blocked</html>', 403),
                     _Resp('forbidden', 403),
                     _Resp(CHALLENGE, 200)):                       # a challenge served as 200
            with self.subTest(status=resp.status_code, text=resp.text[:30]):
                dp._GROUP_STATUS.clear()
                records, calls = run_fetch(since='2026-11-01', answers=[resp])
                self.assertEqual(records, [])
                self.assertEqual(len(calls), 1)                     # never retried or worked around
                self.assertEqual(dp._GROUP_STATUS['El Paso'][0], 'blocked')
                self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_blocked_is_recorded_as_a_red_group(self):
        saved = dp._FRESHNESS
        try:
            dp._FRESHNESS = {'sources': {}, 'groups': {'El Paso': {'latest': '2026-11-18'}}}
            run_fetch(since='2026-11-01', answers=[_Resp(CHALLENGE, 403, {'cf-mitigated': 'challenge'})])
            dp._record_group('El Paso', [])
            self.assertEqual(dp._RUN_GROUPS['El Paso']['status'], 'blocked')
            self.assertEqual(dp._FRESHNESS['groups']['El Paso']['latest'], '2026-11-18')
        finally:
            dp._FRESHNESS = saved
            dp._RUN_GROUPS.pop('El Paso', None)

    def test_wrong_pages_raise(self):
        page = _load('window.html')
        bad = {
            'no table': '<html><body>Maintenance</body></html>',
            'renamed header': page.replace('<th>Score</th>', '<th>Points</th>'),
            'extra column': page.replace('<th>Score</th>', '<th>Score</th><th>Grade</th>'),
            'short row': page.replace('<td> 84 </td>', '', 1),
        }
        for label, body in bad.items():
            with self.subTest(label):
                with self.assertRaises(RuntimeError):
                    run_fetch(since='2024-01-01', answers=[_Resp(body)])
                self.assertNotEqual(dp._GROUP_STATUS.get('El Paso', ('',))[0], 'blocked')

    def test_mostly_unscored_or_unreadable_windows_raise(self):
        page = _load('window.html')
        import re
        unscored = re.sub(r'<td> \d+ </td> <!-- Inspection Score -->', '<td>  </td> <!-- Inspection Score -->', page)
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(since='2024-01-01', answers=[_Resp(unscored)])
        self.assertIn('carry no score', str(ctx.exception))
        undated = page.replace('11/18/2026', '2026-11-18')
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(since='2024-01-01', answers=[_Resp(undated)])
        self.assertIn('unreadable date', str(ctx.exception))

    def test_server_errors_are_retried_then_raise(self):
        answers = [_Resp('busy', 503), _Resp(_load('window.html'))]
        records, calls = run_fetch(since='2024-01-01', answers=answers)
        self.assertEqual(len(calls), 2)
        self.assertEqual(len(records), 59)
        with self.assertRaises(RuntimeError):
            run_fetch(since='2024-01-01', answers=[_Resp('down', 502)] * dp.EP_RETRIES)
        with self.assertRaises(RuntimeError):
            run_fetch(since='2024-01-01', answers=[dp.requests.ConnectionError('reset')] * dp.EP_RETRIES)
        with self.assertRaises(RuntimeError):
            run_fetch(since='2024-01-01', answers=[_Resp('gone', 404)])

    def test_tls_failure_raises_at_once(self):
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(since='2024-01-01', answers=[dp.requests.exceptions.SSLError('bad chain')])
        self.assertIn('TLS', str(ctx.exception))

    def test_deadline_skips_before_the_request(self):
        dp._FETCH_DEADLINE = 1.0                                  # long past
        records, calls = run_fetch(since='2024-01-01')
        self.assertEqual((records, calls), ([], []))
        self.assertEqual(dp._GROUP_STATUS['El Paso'][0], 'skipped')

    def test_mobile_permits_are_dropped_if_ever_listed(self):
        page = _load('window.html').replace('HFFX11-00297', 'HFMB24-00297')
        records, _ = run_fetch(since='2024-01-01', answers=[_Resp(page)])
        self.assertFalse(any('HFMB' in r['source_id'] for r in records))


class ScopeTests(unittest.TestCase):
    def test_institutional_names(self):
        for name in ('Coronado High School', 'Little Learners Academy & Daycare', 'Adult Foster Care',
                     "Garcia's Adult Foster Home", 'Socorro Isd-Sac I Concession #1', 'Regent Care Center Of El Paso',
                     'Tierra Este Nursing & Rehab', 'El Paso County Juvenile Probation', 'Casa Infantil Montessori',
                     "Paso Del Norte Children's Development Center"):
            with self.subTest(name):
                self.assertTrue(dp._tx_score_only_institutional(name))
        for name in ("Church's Chicken #23", 'Home 2 Suites El Paso', 'Herbalife Nutrition Club',
                     'The Olive Garden Italian Restaurant #1202', "Grandma's Home", 'Kids Campus Snack Bar',
                     'Hotel Paso Del Norte- Main Kitchen', 'Waco Truck Stop'):
            with self.subTest(name):
                self.assertFalse(dp._tx_score_only_institutional(name))


if __name__ == '__main__':
    unittest.main()
