"""Tests for the Denton, TX fetcher (CKAN DataStore SQL over the city's
daily 'latest restaurant health scores' export). No network: requests.get
is mocked with trimmed captures from the live DataStore taken 2026-11-03
(tests/fixtures/denton/): a DISTINCT page of nine verbatim rows (a FAILED
result, a null RESULT, a row without permit or date, rows without
coordinates, an 8-digit and 14-digit HealthScoreDate), the matching count
answer and the title of Cloudflare's 403 block page."""
import json
import logging
import os
import re
import sys
import unittest
from datetime import datetime
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'denton')


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _text(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return fh.read()


class _Resp:
    def __init__(self, payload, status=200, text=None, ctype='application/json;charset=utf-8'):
        self._payload = payload
        self.status_code = status
        self.headers = {'content-type': ctype}
        self.text = text if text is not None else json.dumps(payload)
        self.content = self.text.encode()

    def json(self):
        if self._payload is None:
            raise ValueError('no JSON')
        return self._payload


def run_fetch(since=None, limit=None, rows=None, count=None, answers=None):
    """fetch_denton with requests.get mocked. The fixture page is served in
    LIMIT/OFFSET slices so paging can be tested by patching DEN_PAGE;
    `answers` maps 'count' | 'rows' to a _Resp, an exception or a list of
    those served in order."""
    rows = rows if rows is not None else _load('sql_rows.json')
    count = count if count is not None else _load('sql_count.json')
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
        sql = (params or {}).get('sql', '')
        calls.append({'url': url, 'sql': sql, 'verify': verify})
        if sql.startswith('SELECT COUNT'):
            return serve('count', _Resp(count))
        m = re.search(r'LIMIT (\d+) OFFSET (\d+)$', sql)
        if not m:
            raise AssertionError(f'unexpected SQL shape: {sql}')
        page = json.loads(json.dumps(rows))
        lim, off = int(m.group(1)), int(m.group(2))
        page['result']['records'] = rows['result']['records'][off:off + lim]
        return serve('rows', _Resp(page))

    with mock.patch.object(dp.requests, 'get', side_effect=fake_get), \
            mock.patch.object(dp.time, 'sleep'):
        records = dp.fetch_denton(since_date=since, limit=limit)
    return records, calls


def _by_id(records):
    return {r['source_id']: r for r in records}


def _rows_dated(yyyymmdd, rows=None):
    """The fixture rows with every dated row moved to `yyyymmdd`."""
    rows = rows if rows is not None else _load('sql_rows.json')
    for r in rows['result']['records']:
        if r['HealthScoreDate']:
            r['HealthScoreDate'] = f'{yyyymmdd}000000'
    return rows


def _snapshot():
    return (dict(dp._GROUP_NOTES), dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
            dp._FETCH_DEADLINE, dp.log.level)


def _restore(saved):
    dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(saved[0])
    dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(saved[1])
    dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(saved[2])
    dp._FETCH_DEADLINE = saved[3]
    dp.log.setLevel(saved[4])


class DentonBase(unittest.TestCase):
    def setUp(self):
        self._saved = _snapshot()
        dp._GROUP_NOTES.clear()
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._FETCH_DEADLINE = None
        dp.log.setLevel(logging.WARNING)

    def tearDown(self):
        _restore(self._saved)


class RequestShapeTests(DentonBase):
    def test_count_over_distinct_then_distinct_rows_newest_first(self):
        _, calls = run_fetch(since='2026-10-04T00:00:00')
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(c['url'] == dp.DEN_API + 'datastore_search_sql' for c in calls))
        self.assertTrue(all(c['verify'] is True for c in calls))
        self.assertEqual(calls[0]['sql'],
                         'SELECT COUNT(*) AS n FROM (SELECT DISTINCT "PERMIT_NO","HealthScoreDate" '
                         f'FROM "{dp.DEN_RESOURCE}" WHERE "HealthScoreDate" >= \'20261004\') t')
        # A total order: every column, so a permit+date pair with several distinct
        # rows can never straddle a page boundary in a different order per page.
        self.assertEqual(calls[1]['sql'],
                         'SELECT DISTINCT "PERMIT_NO","Name","Address","Type","Lat","Long","RESULT",'
                         '"HealthScoreInt","HealthScoreLetter","HealthScoreDate" '
                         f'FROM "{dp.DEN_RESOURCE}" WHERE "HealthScoreDate" >= \'20261004\' '
                         'ORDER BY "HealthScoreDate" DESC, "PERMIT_NO", "Name", "Address", "Type", "Lat", '
                         f'"Long", "RESULT", "HealthScoreInt", "HealthScoreLetter" LIMIT {dp.DEN_PAGE} OFFSET 0')
        # Only the SQL shapes verified against the WAF: no functions in the where clause, no MAX().
        for c in calls:
            self.assertNotRegex(c['sql'], r'SUBSTR|TO_DATE|CAST\(|MAX\(')

    def test_since_date_in_both_formats_and_the_history_floor(self):
        for since in ('2026-10-04', '2026-10-04T00:00:00'):
            _, calls = run_fetch(since=since)
            self.assertIn('"HealthScoreDate" >= \'20261004\'', calls[1]['sql'], since)
        _, calls = run_fetch(since=None)
        self.assertIn('"HealthScoreDate" >= \'20100101\'', calls[1]['sql'])

    def test_pages_until_a_short_page(self):
        with mock.patch.object(dp, 'DEN_PAGE', 4):
            records, calls = run_fetch(since='2010-01-01')
        offsets = [int(re.search(r'OFFSET (\d+)$', c['sql']).group(1)) for c in calls[1:]]
        self.assertEqual(offsets, [0, 4, 8])                      # 4 + 4 + 1 rows, then stop
        self.assertEqual(len(records), 8)

    def test_limit_stops_paging_and_keeps_the_newest(self):
        with mock.patch.object(dp, 'DEN_PAGE', 4):
            records, calls = run_fetch(since='2010-01-01', limit=3)
        self.assertEqual(len(calls), 2)                           # count + one page
        self.assertEqual([r['source_id'] for r in records],
                         ['den_1809-0116_20261027', 'den_1610-0672_20261028', 'den_HC2001-131_20261028'])
        self.assertNotIn('Denton', dp._GROUP_NOTES)               # a bounded read never saw the end


class RecordTests(DentonBase):
    @classmethod
    def setUpClass(cls):
        saved = _snapshot()                      # the class-level fetch leaves no note or hold behind
        try:
            cls.records, _ = run_fetch(since='2010-01-01')
        finally:
            _restore(saved)
        cls.by_id = _by_id(cls.records)

    def test_scored_record_whole_dict(self):
        self.assertEqual(self.by_id['den_1610-0672_20261028'], {
            'name': 'Old West',
            'address': '2420 S I35 East',
            'city': 'Denton',
            'state': 'TX',
            'zip': '',
            'latitude': 33.184704,
            'longitude': -97.106847,
            'inspection_date': '2026-10-28',
            'original_score': 90,
            'risk_score': 90,                      # the city's official 0-100 score
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': '',
            'results': 'Approved',
            'source': 'Denton Open Data',
            'source_url': 'https://data.cityofdenton.com/dataset/restaurant-health-scores',
            'source_id': 'den_1610-0672_20261028',
            'metro': 'DFW',
        })
        # DineScores grades the score; Denton's own letter ('B: 87-93') is never copied.
        self.assertEqual(dp.calculate_vetted_grade(90), 'A')
        self.assertTrue(all('87-93' not in r['results'] and '94-100' not in r['results']
                            for r in self.records))

    def test_rows_without_coordinates_get_none(self):
        r = self.by_id['den_HC2001-131_20261028']                 # Wal Mart: Lat/Long null
        self.assertIsNone(r['latitude'])
        self.assertIsNone(r['longitude'])
        self.assertEqual(r['name'], 'Wal Mart Supercenter # 467')

    def test_result_text_and_old_date_format(self):
        self.assertEqual(self.by_id['den_2209-0771_20260506']['results'], 'Failed')
        self.assertEqual(self.by_id['den_2209-0771_20260506']['risk_score'], 74)
        self.assertEqual(dp.calculate_vetted_grade(74), 'C')
        self.assertEqual(self.by_id['den_2104-0617_20221003']['results'], '')      # RESULT null
        self.assertEqual(self.by_id['den_0811-0106_20240521']['inspection_date'], '2024-05-21')  # 8 digits
        self.assertEqual(dp._den_result_text('APP W/COND'), 'Approved with conditions')
        self.assertEqual(dp._den_result_text('Something Else'), 'Something Else')
        self.assertEqual(dp._den_date('20261028000000'), '2026-10-28')
        self.assertEqual(dp._den_date('20240521'), '2024-05-21')
        self.assertIsNone(dp._den_date(None))
        self.assertIsNone(dp._den_date('2026'))
        self.assertIsNone(dp._den_date('abcdefgh'))

    def test_rows_without_a_permit_or_date_are_skipped(self):
        self.assertEqual(len(self.records), 8)                    # nine rows, one with no permit/date
        self.assertFalse(any(r['name'] == "Charley's Philly Steaks" for r in self.records))

    def test_order_and_ids(self):
        dates = [r['inspection_date'] for r in self.records]
        self.assertEqual(dates, sorted(dates))
        same_day = [r['source_id'] for r in self.records if r['inspection_date'] == '2026-10-27']
        self.assertEqual(same_day, ['den_0402-0477_20261027', 'den_2009-0704_20261027',
                                    'den_1809-0116_20261027'])    # 84, 91, 100: worst first
        again, _ = run_fetch(since='2010-01-01')
        for a, b in zip(self.records, again):
            self.assertEqual(dp.make_restaurant_id(a['name'], a['address'], a['city']),
                             dp.make_restaurant_id(b['name'], b['address'], b['city']))
        r = self.by_id['den_1610-0672_20261028']
        self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']), '2b24b521b42ed2e5')

    def test_no_holds_are_ever_written(self):
        self.assertEqual(dp._FRESHNESS_HOLDS, {})
        self.assertEqual(dp._GROUP_STATUS, {})


class WindowTests(DentonBase):
    def test_window_is_reapplied_to_the_rows(self):
        records, _ = run_fetch(since='2026-10-28')
        self.assertEqual(sorted(_by_id(records)), ['den_1610-0672_20261028', 'den_HC2001-131_20261028'])

    def test_same_permit_and_day_keeps_the_worst_row(self):
        rows = _load('sql_rows.json')
        dup = dict(rows['result']['records'][1])                  # Old West 90
        dup['HealthScoreInt'] = '85'
        rows['result']['records'].insert(2, dup)
        records, _ = run_fetch(since='2026-10-28', rows=rows)
        self.assertEqual(len(records), 2)
        self.assertEqual(_by_id(records)['den_1610-0672_20261028']['risk_score'], 85)

    def test_unscored_rows_are_dropped(self):
        rows = _load('sql_rows.json')
        rows['result']['records'][1]['HealthScoreInt'] = '0'
        rows['result']['records'][0]['HealthScoreInt'] = None
        records, _ = run_fetch(since='2026-10-28', rows=rows)
        self.assertEqual(records, [])                             # two rows: below the tripwire's floor

    def test_a_mostly_unscored_window_raises_instead_of_an_empty_green_run(self):
        rows = _load('sql_rows.json')
        base = [dict(r) for r in rows['result']['records'] if r['PERMIT_NO']]
        many = []
        for i in range(24):                                       # 24 distinct permits, one date
            r = dict(base[i % len(base)])
            r['PERMIT_NO'] = f'9999-{i:04d}'
            r['HealthScoreDate'] = '20261028000000'
            r['HealthScoreInt'] = '95.0' if i < 13 else '95'      # 13 of 24 re-typed: int() fails
            many.append(r)
        rows['result']['records'] = many
        count = _load('sql_count.json')
        count['result']['records'] = [{'n': 24}]
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(since='2026-10-04', rows=rows, count=count)
        self.assertIn('no usable HealthScoreInt', str(ctx.exception))
        for r in many[:2]:                                        # 11 of 24: still data, dropped quietly
            r['HealthScoreInt'] = '95'
        records, _ = run_fetch(since='2026-10-04', rows=rows, count=count)
        self.assertEqual(len(records), 13)

    def test_school_and_child_care_names_are_dropped(self):
        rows = _load('sql_rows.json')
        rows['result']['records'][1]['Name'] = 'DENTON ISD - STRICKLAND MIDDLE SCHOOL'
        rows['result']['records'][0]['Name'] = 'PRIMROSE SCHOOL OF DENTON'
        records, _ = run_fetch(since='2026-10-28', rows=rows)
        self.assertEqual(records, [])
        # A 'Mobile' in a Denton name is kept: every row has a fixed address, so the
        # source does not say the unit is mobile.
        rows = _load('sql_rows.json')
        rows['result']['records'][1]['Name'] = 'TACO LADY MOBILE-SNACK BAR'
        records, _ = run_fetch(since='2026-10-28', rows=rows)
        self.assertEqual(_by_id(records)['den_1610-0672_20261028']['name'], 'Taco Lady Mobile-Snack Bar')

    def test_source_lag_note_when_the_export_is_behind(self):
        count = _load('sql_count.json')
        count['result']['records'] = [{'n': 5}]
        run_fetch(since='2026-01-01', rows=_rows_dated('20260115'), count=count)
        self.assertIn('Denton', dp._GROUP_NOTES)
        self.assertIn('2026-01-15', dp._GROUP_NOTES['Denton'])
        dp._GROUP_NOTES.clear()
        today = datetime.now().strftime('%Y%m%d')
        run_fetch(since='2026-10-04', rows=_rows_dated(today), count=count)   # fresh rows: no note
        self.assertNotIn('Denton', dp._GROUP_NOTES)


class FailureTests(DentonBase):
    def test_count_shortfall_raises_instead_of_shipping(self):
        count = _load('sql_count.json')
        count['result']['records'] = [{'n': 50}]
        with self.assertRaises(RuntimeError):
            run_fetch(since='2010-01-01', count=count)
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_waf_block_page_raises_without_retrying(self):
        blocked = _Resp(None, 403, text=_text('blocked.html'), ctype='text/html; charset=UTF-8')
        with self.assertRaises(RuntimeError) as ctx:
            run_fetch(since='2026-10-04', answers={'count': [blocked] * 3})
        self.assertIn('HTML', str(ctx.exception))
        blocked = _Resp(None, 403, text=_text('blocked.html'), ctype='text/html; charset=UTF-8')
        calls_seen = []
        real = dp.requests.get

        def counting(url, params=None, headers=None, timeout=None, verify=True, **kw):
            calls_seen.append(1)
            return blocked

        with mock.patch.object(dp.requests, 'get', side_effect=counting), \
                mock.patch.object(dp.time, 'sleep'):
            with self.assertRaises(RuntimeError):
                dp.fetch_denton(since_date='2026-10-04')
        self.assertEqual(len(calls_seen), 1)
        self.assertIs(dp.requests.get, real)

    def test_server_errors_are_retried_then_raise(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-04', answers={'count': [_Resp(None, 503, text='down')] * 5})
        answers = {'rows': [_Resp(None, 502, text='bad gateway'), None]}
        records, calls = run_fetch(since='2010-01-01', answers=answers)
        self.assertEqual(len(calls), 3)                           # count, a 502, the page
        self.assertEqual(len(records), 8)

    def test_connection_errors_and_bad_payloads_raise(self):
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-04', answers={'count': [dp.requests.ConnectionError('reset')] * 5})
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-04', answers={'rows': _Resp({'success': False, 'error': {'message': 'bad sql'}})})
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-04', answers={'rows': _Resp({'success': True, 'result': {'records': 'nope'}})})
        with self.assertRaises(RuntimeError):
            run_fetch(since='2026-10-04', answers={'count': _Resp({'success': True, 'result': {'records': []}})})


if __name__ == '__main__':
    unittest.main()
