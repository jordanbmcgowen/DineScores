"""Tests for the Waco / McLennan County, TX fetcher (the district's public
ArcGIS view 'foodScoresView'). No network: requests.get is mocked with 59
features picked verbatim from the whole-layer capture of 2026-11-22
(tests/fixtures/waco/page_0.json and page_1.json, served as two pages of 40):
every row of 2026-11-10 .. 11-13, an exact duplicate row with another
timestamp, three same-day rows of one place, '-INACTIVE' permits, a food
truck, a dated event, a school, CANCELED and REQUESTED rows, a FAILED row at 0
demerits, the 50-demerit maximum, an '(UNINCORPORATED)' city and a row
without a ZIP."""
import json
import logging
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'waco')
PAGE = 40


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _pages():
    return [_load('page_0.json'), _load('page_1.json')]


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

    def raise_for_status(self):
        if self.status_code >= 400:
            raise dp.requests.HTTPError(f'HTTP {self.status_code}')


def run_fetch(since='2025-01-01', limit=None, pages=None, count=None, data_answers=None, count_answers=None):
    """fetch_waco with requests.get mocked: count queries answer `count`
    (default: the rows in `pages`), data queries answer the page at their
    resultOffset (WAC_PAGE patched to 40); *_answers override in order."""
    pages = pages if pages is not None else _pages()
    total = sum(len(p['features']) for p in pages)
    data_answers, count_answers = list(data_answers or []), list(count_answers or [])
    calls = []

    def fake_get(url, params=None, headers=None, timeout=None, **kw):
        params = dict(params or {})
        calls.append({'url': url, 'params': params, 'headers': headers})
        if params.get('returnCountOnly'):
            if count_answers:
                a = count_answers.pop(0)
                if isinstance(a, Exception):
                    raise a
                return a
            return _Resp({'count': total if count is None else count})
        if params.get('orderByFields') == 'Date DESC':
            newest = max((f for p in pages for f in p['features']), key=lambda f: f['attributes']['Date'],
                         default=None)
            return _Resp({'features': [newest] if newest else []})
        if data_answers:
            a = data_answers.pop(0)
            if isinstance(a, Exception):
                raise a
            return a
        index = int(params.get('resultOffset', 0)) // PAGE
        return _Resp(pages[index] if index < len(pages) else {'features': []})

    with mock.patch.object(dp.requests, 'get', side_effect=fake_get), \
            mock.patch.object(dp.time, 'sleep'), mock.patch.object(dp, 'WAC_PAGE', PAGE):
        records = dp.fetch_waco(since_date=since, limit=limit)
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


class WacoBase(unittest.TestCase):
    def setUp(self):
        self._saved = _snapshot()
        dp._GROUP_NOTES.clear()
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._FETCH_DEADLINE = None
        dp.log.setLevel(logging.CRITICAL)

    def tearDown(self):
        _restore(self._saved)


def _rid(name, address, city):
    return dp.make_restaurant_id(name, address, city)


class RequestShapeTests(WacoBase):
    def test_count_then_pages_with_the_date_filter(self):
        _, calls = run_fetch(since='2025-01-01T00:00:00')
        count, page0, page1 = calls[0], calls[1], calls[2]
        self.assertEqual(len(calls), 3)
        self.assertEqual(count['url'], dp.WAC_LAYER + '/query')
        self.assertEqual(count['params']['where'], "Date >= DATE '2025-01-01'")
        self.assertEqual(count['params']['returnCountOnly'], 'true')
        self.assertEqual(page0['params']['where'], "Date >= DATE '2025-01-01'")
        self.assertEqual(page0['params']['outSR'], '4326')
        self.assertEqual((page0['params']['resultOffset'], page1['params']['resultOffset']), (0, PAGE))
        for f in dp.WAC_FIELDS:
            self.assertIn(f, page0['params']['outFields'])
        self.assertEqual(page0['headers']['User-Agent'], dp.CHROME_UA)

    def test_reread_margin_and_default_windows(self):
        _, calls = run_fetch(since=_days_ago(2))
        self.assertEqual(calls[0]['params']['where'], f"Date >= DATE '{_days_ago(dp.WAC_REREAD_DAYS)}'")
        _, calls = run_fetch(since=None, limit=25)
        self.assertEqual(calls[0]['params']['where'], f"Date >= DATE '{_days_ago(dp.WAC_TEST_WINDOW_DAYS)}'")
        _, calls = run_fetch(since=None)
        self.assertEqual(calls[0]['params']['where'], f"Date >= DATE '{dp.WAC_HISTORY_START}'")


class RecordTests(WacoBase):
    @classmethod
    def setUpClass(cls):
        saved = _snapshot()
        try:
            dp.log.setLevel(logging.CRITICAL)
            cls.records, _ = run_fetch()
        finally:
            _restore(saved)
        cls.by_id = _by_id(cls.records)

    def _find(self, name, date=None):
        return [r for r in self.records if r['name'] == name and (date is None or r['inspection_date'] == date)]

    def test_record_whole_dict(self):
        rid = _rid('Shaan Sea Food Restaurant', '1824 W Waco Dr', 'Waco')
        self.assertEqual(self.by_id[f'wac_{rid}_20261112'], {
            'name': 'Shaan Sea Food Restaurant',
            'address': '1824 W Waco Dr',
            'city': 'Waco',
            'state': 'TX',
            'zip': '76701',
            'latitude': self.by_id[f'wac_{rid}_20261112']['latitude'],
            'longitude': self.by_id[f'wac_{rid}_20261112']['longitude'],
            'inspection_date': '2026-11-12',
            'original_score': 12,                     # the district's demerit total
            'risk_score': 65,                         # FAILED: capped (100 - demerits would be 88)
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': 'Permit renewal',
            'results': '12 demerits (Failed with penalty (BLIC))',
            'source': 'Waco-McLennan County Public Health District',
            'source_url': dp.WAC_APP_URL,
            'source_id': f'wac_{rid}_20261112',
            'metro': '',
        })
        r = self.by_id[f'wac_{rid}_20261112']
        self.assertAlmostEqual(r['latitude'], 31.55, delta=0.1)       # the source's own point
        self.assertAlmostEqual(r['longitude'], -97.15, delta=0.1)

    def test_counts_and_scope(self):
        self.assertEqual(len(self.records), 50)
        names = {r['name'] for r in self.records}
        for gone in ('King Donut', 'King Donut-Inactive', 'Los Compadres-Inactive',   # closed permits
                     'Kajun Kluck Truck', 'The Pizza Truck',                         # mobile units
                     'Otaku Festival - 2026',                                         # a dated event
                     'West Isd-High/Middle School',                                   # a school
                     "Ninfa's Waco",                                                  # REQUESTED
                     'Baylor Football - 2025 - Inactive'):                            # CANCELED
            self.assertNotIn(gone, names)
        self.assertTrue(all(r['violations'] == [] and r['state'] == 'TX' for r in self.records))
        self.assertTrue(all(r['latitude'] and r['longitude'] for r in self.records))

    def test_score_rule_and_failed_flag(self):
        worst = self._find('Helados La Azteca - Valley Mills Dr')[0]
        self.assertEqual((worst['original_score'], worst['risk_score']), (50, 50))
        self.assertEqual(dp.calculate_vetted_grade(worst['risk_score']), 'F')
        # A FAILED inspection is an F whatever its demerits: the district's own
        # verdict (re-inspection required), which the demerit total does not carry.
        failed_clean = self._find('Dominos Pizza #6992')[0]                 # FAILED at 0 demerits
        self.assertEqual((failed_clean['original_score'], failed_clean['risk_score'],
                          failed_clean['results']), (0, 65, '0 demerits (Failed)'))
        self.assertEqual(dp.calculate_vetted_grade(failed_clean['risk_score'],
                                                   dp.build_violation_text(failed_clean['violations'])), 'F')
        self.assertEqual(failed_clean['inspection_type'], 'Permit')
        self.assertEqual(dp.WAC_FAILED_RISK_CAP, 65)
        self.assertEqual(dp.calculate_vetted_grade(dp.WAC_FAILED_RISK_CAP), 'F')
        failed = passed = 0
        for r in self.records:
            form_score = 100 - r['original_score']
            if '(Failed' in r['results']:
                failed += 1
                self.assertEqual(r['risk_score'], min(form_score, 65), r['results'])
                self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'F')
            else:
                passed += 1
                self.assertEqual(r['risk_score'], form_score, r['results'])
        self.assertTrue(failed >= 3 and passed >= 3, (failed, passed))
        # a failure worse than the cap keeps its own number
        self.assertEqual(worst['risk_score'], 50)
        # 'Passed with exceptions' and 'Partial pass' are not failures
        self.assertFalse('PASSED WITH EXCEPTIONS'.startswith('FAILED'))

    def test_exact_duplicate_rows_ship_once(self):
        sison = self._find('Sison Tacos')
        self.assertEqual(len(sison), 1)
        self.assertEqual(sison[0]['inspection_date'], '2025-01-17')

    def test_same_day_rows_ship_worst_first(self):
        camp = self._find('Camp Fimfo Waco', '2025-03-13')
        rid = _rid('Camp Fimfo Waco', '1000 Chickadee Ln', 'Waco')
        got = {r['source_id']: r['risk_score'] for r in camp}
        # the FAILED row (4 demerits, capped at 65) is the day's worst and ships first
        self.assertEqual(got, {f'wac_{rid}_20250313': 65, f'wac_{rid}_20250313_2': 90,
                               f'wac_{rid}_20250313_3': 100})
        self.assertEqual(max(camp, key=dp._same_day_rank)['risk_score'], 65)
        self.assertEqual({r['source_id']: r['original_score'] for r in camp}[f'wac_{rid}_20250313'], 4)
        grounds = self._find('Common Grounds Sub', '2026-11-10')
        self.assertEqual(sorted((r['risk_score'], r['original_score']) for r in grounds),
                         [(65, 14), (100, 0)], 'the 14-demerit visit was FAILED')

    def test_names_cities_addresses(self):
        pub = self._find("Macdaddy's Pub")[0]
        self.assertEqual((pub['address'], pub['city'], pub['zip']), ('101 E Long St', 'Elm Mott', '76640'))
        self.assertEqual(self._find('Glory Bell Coffee')[0]['zip'], '')           # Zipcode null
        self.assertTrue(self._find('The Chateau Waco'))                             # 'CHATEAU WACO, THE'
        self.assertTrue(self._find('Murphy USA #7895'))
        self.assertEqual(self._find('Murphy USA #7895')[0]['address'], '1519 N IH 35')
        self.assertEqual(self._find('Dairy Queen')[0]['city'], 'Mcgregor')         # plain .title()
        self.assertEqual(dp._wac_city('WACO (UNINCORPORATED)'), 'Waco')
        self.assertEqual(dp._wac_city('BRUCEVILLE-EDDY'), 'Bruceville-Eddy')
        self.assertEqual(dp._wac_name('PIZZA TRUCK, THE'), 'The Pizza Truck')

    def test_dates_are_the_utc_calendar_day(self):
        # 490170 is 2026-11-13 16:15 'UTC' (local wall-clock time): the 13th in any zone
        self.assertTrue(self._find('Sip City', '2026-11-13'))
        self.assertEqual(dp._mck_date(1794586551000), '2026-11-13')

    def test_ids_are_stable_and_unique(self):
        self.assertEqual(len(self.by_id), len(self.records))
        again, _ = run_fetch()
        self.assertEqual([r['source_id'] for r in again], [r['source_id'] for r in self.records])
        # renumbered OBJECTIDs (a reload of the table) change nothing
        pages = _pages()
        for p in pages:
            for f in p['features']:
                f['attributes']['OBJECTID'] += 100000
        moved, _ = run_fetch(pages=pages)
        self.assertEqual([r['source_id'] for r in moved], [r['source_id'] for r in self.records])

    def test_order_and_no_holds(self):
        dates = [r['inspection_date'] for r in self.records]
        self.assertEqual(dates, sorted(dates))
        self.assertEqual(dp._FRESHNESS_HOLDS, {})


class ScopeTests(unittest.TestCase):
    def test_inactive_marker_variants(self):
        for name in ('KING DONUT-INACTIVE', 'AMIGO FOOD MART - INACTIVE', "ARMSTRONG'S BAYOU CAFE-INACTIVATE",
                     'DOMO SUSHI - INACTIVATED', 'COMIDA DEL RANCHO-INACTIVED', 'UNION GROVE-INACTIVE'):
            self.assertTrue(dp.WAC_INACTIVE_RE.search(name), name)
        self.assertFalse(dp.WAC_INACTIVE_RE.search('INACTIVE WEAR CAFE'))

    def test_mobile_and_event_names(self):
        for name in ('Kajun Kluck Truck', "Vitek's Foodtruck", 'Rositas Taqueria-Mobiles',
                     "Le's Kitchen - Mobile", 'Tru Jamaica Food Truck'):
            self.assertTrue(dp.WAC_MOBILE_RE.search(name), name)
        for name in ('Waco Truck Stop', 'Cowboy Truck Stop', 'Dairy Queen'):
            self.assertFalse(dp.WAC_MOBILE_RE.search(name), name)
        self.assertTrue(dp.WAC_EVENT_RE.search('SPRING CARNIVAL - 2025 - INACTIVE'))
        self.assertTrue(dp.WAC_EVENT_RE.search('TEXAS FOOD TRUCK SHOWDOWN - 2026-INACTIVE'))
        self.assertFalse(dp.WAC_EVENT_RE.search('CEFCO #2016'))


class WindowTests(WacoBase):
    def test_limit_keeps_the_newest(self):
        records, _ = run_fetch(limit=5)
        self.assertEqual(len(records), 5)
        self.assertTrue(all(r['inspection_date'] >= '2026-11-12' for r in records))
        self.assertNotIn('Waco', dp._GROUP_NOTES)

    def test_window_is_reapplied_client_side(self):
        records, _ = run_fetch(since='2026-11-12')
        self.assertTrue(all(r['inspection_date'] >= min('2026-11-12', _days_ago(dp.WAC_REREAD_DAYS))
                            for r in records))

    def test_source_lag_note(self):
        pages = _pages()
        for p in pages:
            for f in p['features']:
                f['attributes']['Date'] -= 400 * 86400 * 1000            # a layer a year behind
        run_fetch(since=None, pages=pages)
        self.assertIn('2025-10-09', dp._GROUP_NOTES.get('Waco', ''))
        dp._GROUP_NOTES.clear()
        empty = [{'features': []}]
        records, calls = run_fetch(pages=empty, count=0)
        self.assertEqual(records, [])
        self.assertEqual(calls[-1]['params'].get('orderByFields'), 'Date DESC')    # the newest probe


class FailureTests(WacoBase):
    def test_shortfall_against_the_count_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(count=60)
        self.assertIn('read 59 of 60', str(ctx.exception))

    def test_count_failures_raise(self):
        for answer in (_Resp({'error': {'code': 400, 'message': 'Invalid query'}}),
                       _Resp({'nothing': 1}),
                       _Resp(None, 200, text='<html>blocked</html>', ctype='text/html'),
                       _Resp(None, 403, text='denied', ctype='text/html')):
            with self.subTest(answer=answer.text[:30]):
                with self.assertRaises(RuntimeError):
                    run_fetch(count_answers=[answer])

    def test_page_failures_raise(self):
        for answer in (_Resp({'error': {'code': 500, 'message': 'boom'}}),
                       _Resp(None, 200, text='<html>blocked</html>', ctype='text/html'),
                       _Resp(None, 404, text='gone', ctype='text/html'),
                       dp.requests.ConnectionError('reset')):
            with self.subTest(answer=str(answer)[:30]):
                with self.assertRaises(RuntimeError):
                    run_fetch(data_answers=[answer])

    def test_a_renamed_field_raises(self):
        pages = _pages()
        for f in pages[1]['features']:
            f['attributes']['Score'] = f['attributes'].pop('TotalScore')
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(pages=pages)
        self.assertIn('lacks the layer fields', str(ctx.exception))

    def test_mostly_unscored_or_unreadable_windows_raise(self):
        pages = _pages()
        for p in pages:
            for f in p['features']:
                f['attributes']['TotalScore'] = None
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(pages=pages)
        self.assertIn('no TotalScore', str(ctx.exception))
        pages = _pages()
        for f in pages[0]['features'][:5]:
            f['attributes']['TotalScore'] = 'n/a'
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(pages=pages)
        self.assertIn('unreadable', str(ctx.exception))
        pages = _pages()
        pages[0]['features'][2]['attributes']['TotalScore'] = -3                 # one bad row: dropped
        records, _ = run_fetch(pages=pages)
        self.assertEqual(len(records), 49)

    def test_deadline_skips_before_any_request(self):
        dp._FETCH_DEADLINE = 1.0
        records, calls = run_fetch()
        self.assertEqual((records, calls), ([], []))
        self.assertEqual(dp._GROUP_STATUS['Waco'][0], 'skipped')

    def test_unknown_status_ships_as_published(self):
        pages = _pages()
        pages[1]['features'][0]['attributes']['Status'] = 'CONDITIONAL PASS'
        with self.assertLogs('dinescores', level='WARNING'):
            records, _ = run_fetch(pages=pages)
        self.assertIn('0 demerits (Conditional Pass)', {r['results'] for r in records})


if __name__ == '__main__':
    unittest.main()
