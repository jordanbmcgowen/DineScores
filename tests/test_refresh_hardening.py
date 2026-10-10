"""Tests for the weekly-refresh hardening in dinescores_pipeline.py: the
self-healing lookback, the relay credential, SNHD rate-limit handling and
stalest-first MHD ordering. No network: every HTTP call is mocked."""
import json
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402


class LookbackTests(unittest.TestCase):
    def setUp(self):
        self._saved = dp._FRESHNESS
        dp._FRESHNESS = {'sources': {}, 'groups': {}}

    def tearDown(self):
        dp._FRESHNESS = self._saved

    def test_default_window_when_nothing_recorded(self):
        start = datetime.now() - timedelta(days=8)
        self.assertEqual(dp._lookback_since('X', start, dp.FRESHNESS_LOOKBACK_API_DAYS), start)

    def test_quiet_source_is_read_back_to_its_last_shipped_day(self):
        # LA County: last shipped 2026-06-30, source resumed 100 days later.
        # The old 45-day cap started the window at today-45 and lost July
        # and August for good; the window must start the day before the
        # recorded latest instead.
        latest = (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d')
        dp._FRESHNESS['groups']['LA County'] = {'latest': latest}
        start = datetime.now() - timedelta(days=8)
        since = dp._lookback_since('LA County', start, dp.FRESHNESS_LOOKBACK_API_DAYS)
        expected = datetime.strptime(latest, '%Y-%m-%d') - timedelta(days=dp.FRESHNESS_OVERLAP_DAYS)
        self.assertEqual(since, expected)

    def test_portal_backlog_beyond_three_weeks_is_not_abandoned(self):
        latest = (datetime.now() - timedelta(days=40)).strftime('%Y-%m-%d')
        dp._FRESHNESS['sources']['Dallas Health Dept'] = {'latest': latest, 'group': 'DFW'}
        dp._FRESHNESS['sources']['Plano Health Dept'] = {'latest': '2026-01-01', 'group': 'DFW'}
        start = datetime.now() - timedelta(days=8)
        # The group follows its most lagging source, bounded by the cap.
        since = dp._lookback_since('DFW', start, dp.FRESHNESS_LOOKBACK_PORTAL_DAYS,
                                   source_labels=['Dallas Health Dept', 'Plano Health Dept'])
        floor = datetime.now() - timedelta(days=dp.FRESHNESS_LOOKBACK_PORTAL_DAYS)
        self.assertLess(abs((since - floor).total_seconds()), 5)
        self.assertGreater(dp.FRESHNESS_LOOKBACK_PORTAL_DAYS, 40)

    def test_window_never_moves_later_than_the_default(self):
        dp._FRESHNESS['groups']['NYC'] = {'latest': datetime.now().strftime('%Y-%m-%d')}
        start = datetime.now() - timedelta(days=8)
        self.assertEqual(dp._lookback_since('NYC', start, dp.FRESHNESS_LOOKBACK_API_DAYS), start)


class RelayTests(unittest.TestCase):
    def setUp(self):
        self._saved = (dp.PORTAL_RELAY_URL, dp.PORTAL_RELAY_TOKEN, dict(dp._RELAY_OIDC))

    def tearDown(self):
        dp.PORTAL_RELAY_URL, dp.PORTAL_RELAY_TOKEN, oidc = self._saved
        dp._RELAY_OIDC.clear()
        dp._RELAY_OIDC.update(oidc)

    def test_no_relay_leaves_request_untouched(self):
        dp.PORTAL_RELAY_URL = ''
        url, params, headers = dp._relay('https://x/y', {'a': 1})
        self.assertEqual((url, params, headers), ('https://x/y', {'a': 1}, {}))

    def test_static_token_is_presented_and_params_fold_into_target(self):
        dp.PORTAL_RELAY_URL = 'https://dinescores.com/api/relay'
        dp.PORTAL_RELAY_TOKEN = 'secret'
        url, params, headers = dp._relay(
            'https://www.southernnevadahealthdistrict.org/wp-json/x', {'per_page': 100, 'page': 2})
        self.assertIsNone(params)
        self.assertTrue(url.startswith('https://dinescores.com/api/relay?url=https%3A%2F%2Fwww.southernnevadahealthdistrict.org'))
        self.assertIn('per_page%3D100', url)
        self.assertEqual(headers, {'X-Relay-Token': 'secret'})

    def test_relay_url_itself_is_never_relayed(self):
        dp.PORTAL_RELAY_URL = 'https://dinescores.com/api/relay'
        dp.PORTAL_RELAY_TOKEN = 'secret'
        url, _, headers = dp._relay('https://dinescores.com/api/relay?url=x')
        self.assertEqual(url, 'https://dinescores.com/api/relay?url=x')
        self.assertEqual(headers, {})

    def test_oidc_token_minted_in_actions_and_cached(self):
        dp.PORTAL_RELAY_URL = 'https://dinescores.com/api/relay'
        dp.PORTAL_RELAY_TOKEN = ''
        dp._RELAY_OIDC.update(token='', exp=0.0, warned=False)
        import base64, json
        payload = base64.urlsafe_b64encode(json.dumps({'exp': 4102444800}).encode()).decode().rstrip('=')
        jwt = f'eyJhbGciOiJSUzI1NiJ9.{payload}.sig'
        resp = mock.Mock()
        resp.raise_for_status = mock.Mock()
        resp.json = mock.Mock(return_value={'value': jwt})
        env = {'ACTIONS_ID_TOKEN_REQUEST_URL': 'https://pipelines.actions.githubusercontent.com/x?api-version=2.0',
               'ACTIONS_ID_TOKEN_REQUEST_TOKEN': 'runner-token'}
        with mock.patch.dict(os.environ, env), mock.patch.object(dp.requests, 'get', return_value=resp) as get:
            url, _, headers = dp._relay('https://inspections.myhealthdepartment.com/', None)
            url2, _, headers2 = dp._relay('https://inspections.myhealthdepartment.com/dallas', None)
        self.assertEqual(headers, {'X-Relay-Token': jwt})
        self.assertEqual(headers2, {'X-Relay-Token': jwt})
        self.assertEqual(get.call_count, 1, 'a valid token is reused, not re-minted per request')
        self.assertEqual(get.call_args.kwargs['params'], {'audience': dp.RELAY_OIDC_AUDIENCE})
        self.assertIn('bearer runner-token', get.call_args.kwargs['headers']['Authorization'])
        self.assertTrue(url.startswith('https://dinescores.com/api/relay?url='))

    def test_no_credential_means_direct_request(self):
        dp.PORTAL_RELAY_URL = 'https://dinescores.com/api/relay'
        dp.PORTAL_RELAY_TOKEN = ''
        dp._RELAY_OIDC.update(token='', exp=0.0, warned=False)
        env = {k: '' for k in ('ACTIONS_ID_TOKEN_REQUEST_URL', 'ACTIONS_ID_TOKEN_REQUEST_TOKEN')}
        with mock.patch.dict(os.environ, env):
            url, params, headers = dp._relay('https://inspections.myhealthdepartment.com/', None)
        self.assertEqual(url, 'https://inspections.myhealthdepartment.com/')
        self.assertEqual(headers, {})


class SnhdRateLimitTests(unittest.TestCase):
    def setUp(self):
        dp._SNHD_RATE_LIMITED_UNTIL = 0.0
        dp._SNHD_FAILURES_LOGGED = 0

    def _response(self, status, body=None):
        r = mock.Mock()
        r.status_code = status
        r.headers = {}
        if status >= 400:
            err = dp.requests.HTTPError(f'{status} error')
            err.response = r
            r.raise_for_status = mock.Mock(side_effect=err)
        else:
            r.raise_for_status = mock.Mock()
        r.json = mock.Mock(return_value=body)
        return r

    def test_401_pauses_then_retries_and_succeeds(self):
        responses = [self._response(401), self._response(200, {'ok': 1})]
        slept = []
        with mock.patch.object(dp.requests, 'get', side_effect=responses), \
             mock.patch.object(dp.time, 'sleep', side_effect=lambda s: slept.append(s)), \
             mock.patch.object(dp, 'SNHD_RATE_LIMIT_WAIT', 90.0):
            # Make the shared pause clock already expired so the test does
            # not wait; what matters is that the pause was requested.
            with mock.patch.object(dp, '_snhd_wait_if_limited'):
                out = dp._snhd_get('https://www.southernnevadahealthdistrict.org/wp-json/x')
        self.assertEqual(out, {'ok': 1})
        self.assertGreaterEqual(dp._SNHD_RATE_LIMITED_UNTIL, dp.time.time() + 80)
        self.assertEqual(slept, [], 'a 401 pauses the shared clock instead of sleeping 5s')

    def test_persistent_401_fails_with_the_status_in_the_message(self):
        responses = [self._response(401)] * 4
        with mock.patch.object(dp.requests, 'get', side_effect=responses), \
             mock.patch.object(dp, '_snhd_wait_if_limited'), \
             mock.patch.object(dp.time, 'sleep'):
            out = dp._snhd_get('https://www.southernnevadahealthdistrict.org/wp-json/x')
        self.assertIsNone(out)
        self.assertIn('401', dp._SNHD_LAST_ERROR)


class MhdOrderTests(unittest.TestCase):
    def test_stalest_jurisdiction_runs_first(self):
        saved = dp._FRESHNESS
        dp._FRESHNESS = {'groups': {}, 'sources': {
            'Dallas Health Dept': {'latest': '2026-09-20'},
            'Plano Health Dept': {'latest': '2026-09-05'},
            # Frisco: never recorded → first
        }}
        order = []

        def fake_fetch(session, slug, config, *a, **kw):
            order.append(slug)
            return []
        try:
            with mock.patch.object(dp, '_fetch_mhd_jurisdiction_api', side_effect=fake_fetch), \
                 mock.patch.object(dp.time, 'sleep'):
                dp.fetch_dfw({'dallas': dp.DFW_JURISDICTIONS['dallas'],
                              'plano': dp.DFW_JURISDICTIONS['plano'],
                              'frisco': dp.DFW_JURISDICTIONS['frisco']})
        finally:
            dp._FRESHNESS = saved
        self.assertEqual(order, ['frisco', 'plano', 'dallas'])


if __name__ == '__main__':
    unittest.main()


class FreshnessHoldTests(unittest.TestCase):
    """A transient drop must keep the source's recorded latest from moving
    past it, or the next run's lookback never reads that day again."""

    def setUp(self):
        self._saved = (dp._FRESHNESS, dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS))
        dp._FRESHNESS = {'groups': {'Omaha': {'latest': '2026-09-20'}},
                         'sources': {'Douglas County Health Dept': {'latest': '2026-09-20',
                                                                   'group': 'Omaha'}}}
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()

    def tearDown(self):
        dp._FRESHNESS = self._saved[0]
        dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(self._saved[1])
        dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(self._saved[2])

    def _rec(self, d, source='Douglas County Health Dept'):
        return {'inspection_date': d, 'source': source}

    def _recent(self, days_ago):
        return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

    def test_a_drop_caps_the_recorded_latest_at_the_day_before_it(self):
        shipped = [self._rec(self._recent(2)), self._rec(self._recent(9))]
        dp._hold_freshness('Douglas County Health Dept', self._recent(6))
        dp._record_group('Omaha', shipped)
        cap = self._recent(7)
        self.assertEqual(dp._FRESHNESS['sources']['Douglas County Health Dept']['latest'], cap)
        self.assertEqual(dp._FRESHNESS['groups']['Omaha']['latest'], cap)
        self.assertEqual(dp._FRESHNESS_HOLDS, {})          # consumed
        # The run's own report still says what shipped.
        self.assertEqual(dp._RUN_GROUPS['Omaha']['latest'], self._recent(2))

    def test_a_hold_pulls_back_an_already_recorded_latest(self):
        dp._FRESHNESS['sources']['Douglas County Health Dept']['latest'] = self._recent(1)
        dp._FRESHNESS['groups']['Omaha']['latest'] = self._recent(1)
        dp._hold_freshness('Douglas County Health Dept', self._recent(4))
        dp._record_group('Omaha', [])
        self.assertEqual(dp._FRESHNESS['groups']['Omaha']['latest'], self._recent(5))

    def test_the_earliest_drop_wins_with_no_age_limit(self):
        for days in (3, 5, 4):
            dp._hold_freshness('Douglas County Health Dept', self._recent(days))
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        self.assertEqual(dp._FRESHNESS['groups']['Omaha']['latest'], self._recent(6))
        # A catch-up run's drop from two months back is held like last week's.
        dp._hold_freshness('Douglas County Health Dept', self._recent(60))
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        self.assertEqual(dp._FRESHNESS['groups']['Omaha']['latest'], self._recent(61))

    def test_a_date_dropped_three_runs_in_a_row_is_given_up_not_a_fresh_drop(self):
        src = 'Douglas County Health Dept'
        for run in range(dp.FRESHNESS_HOLD_GIVE_UP_RUNS - 1):
            dp._hold_freshness(src, self._recent(30))
            dp._record_group('Omaha', [self._rec(self._recent(1))])
            self.assertEqual(dp._FRESHNESS['sources'][src]['latest'], self._recent(31))
            self.assertEqual(dp._FRESHNESS['sources'][src]['hold']['failures'],
                             {self._recent(30): run + 1})
        # Third run: the 30-day-old record fails again, and a 5-day-old one
        # fails for the first time. The old one is given up; the new one holds.
        dp._hold_freshness(src, self._recent(30))
        dp._hold_freshness(src, self._recent(5))
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        self.assertEqual(dp._FRESHNESS['sources'][src]['latest'], self._recent(6))
        self.assertEqual(dp._FRESHNESS['sources'][src]['hold'],
                         {'date': self._recent(5), 'failures': {self._recent(5): 1}})
        self.assertEqual(dp._FRESHNESS['sources'][src]['given_up'], [self._recent(30)])
        # A clean run clears the hold; the given-up date stays given up.
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        self.assertNotIn('hold', dp._FRESHNESS['sources'][src])
        self.assertEqual(dp._FRESHNESS['sources'][src]['latest'], self._recent(1))
        dp._hold_freshness(src, self._recent(30))
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        self.assertNotIn('hold', dp._FRESHNESS['sources'][src])
        self.assertEqual(dp._FRESHNESS['sources'][src]['latest'], self._recent(1))

    def test_two_bad_records_a_day_apart_cannot_take_turns_holding(self):
        # Each failing date keeps its own count, so both are given up on the
        # third run instead of handing the hold back and forth.
        src = 'Douglas County Health Dept'
        dp._FRESHNESS['sources'][src]['latest'] = self._recent(30)
        dp._FRESHNESS['groups']['Omaha']['latest'] = self._recent(30)
        for run in range(dp.FRESHNESS_HOLD_GIVE_UP_RUNS):
            since = dp._lookback_since('Omaha', datetime.now() - timedelta(days=8), 400)
            for d in (self._recent(30), self._recent(29)):
                self.assertGreaterEqual(d, since.strftime('%Y-%m-%d'))   # both re-read
                dp._hold_freshness(src, d)
            dp._record_group('Omaha', [self._rec(self._recent(1))])
        e = dp._FRESHNESS['sources'][src]
        self.assertNotIn('hold', e)
        self.assertEqual(e['given_up'], [self._recent(30), self._recent(29)])
        self.assertEqual(e['latest'], self._recent(1))
        # The next window is the normal one, so neither is read again.
        since = dp._lookback_since('Omaha', datetime.now() - timedelta(days=8), 400)
        self.assertEqual(since.strftime('%Y-%m-%d'), self._recent(8))

    def test_drops_the_deadline_or_a_block_caused_hold_but_never_give_up(self):
        src = 'Douglas County Health Dept'
        for _ in range(dp.FRESHNESS_HOLD_GIVE_UP_RUNS + 2):
            dp._hold_freshness(src, self._recent(12), counted=False)
            dp._record_group('Omaha', [self._rec(self._recent(1))])
            self.assertEqual(dp._FRESHNESS['sources'][src]['latest'], self._recent(13))
        self.assertEqual(dp._FRESHNESS['sources'][src]['hold'],
                         {'date': self._recent(12), 'failures': {}})
        self.assertNotIn('given_up', dp._FRESHNESS['sources'][src])
        # One drop of a date counts only if every drop of it was a failure.
        dp._hold_freshness(src, self._recent(12))
        dp._hold_freshness(src, self._recent(12), counted=False)
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        self.assertEqual(dp._FRESHNESS['sources'][src]['hold']['failures'], {})

    def test_a_hold_in_the_older_single_date_form_keeps_counting(self):
        src = 'Douglas County Health Dept'
        dp._FRESHNESS['sources'][src]['hold'] = {'date': self._recent(9), 'runs': 2}
        dp._hold_freshness(src, self._recent(9))
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        self.assertNotIn('hold', dp._FRESHNESS['sources'][src])
        self.assertEqual(dp._FRESHNESS['sources'][src]['given_up'], [self._recent(9)])

    def test_a_window_start_hold_makes_the_next_window_start_there(self):
        dp._hold_window_start('Douglas County Health Dept', self._recent(20))
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        since = dp._lookback_since('Omaha', datetime.now() - timedelta(days=8), 400)
        self.assertEqual(since.strftime('%Y-%m-%d'), self._recent(20))

    def test_one_held_source_does_not_make_a_multi_source_group_look_stale(self):
        dp._FRESHNESS = {'groups': {}, 'sources': {}}
        shipped = [self._rec(self._recent(1), f'County {i} Health Dept') for i in range(5)]
        dp._hold_freshness('County 0 Health Dept', self._recent(24))
        dp._record_group('MHD metros', shipped)
        self.assertEqual(dp._FRESHNESS['sources']['County 0 Health Dept']['latest'], self._recent(25))
        self.assertEqual(dp._FRESHNESS['sources']['County 3 Health Dept']['latest'], self._recent(1))
        self.assertEqual(dp._FRESHNESS['groups']['MHD metros']['latest'], self._recent(1))

    def test_a_hold_on_another_groups_source_is_left_alone(self):
        dp._hold_freshness('Some Other Source', self._recent(3))
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        self.assertIn('Some Other Source', dp._FRESHNESS_HOLDS)

    def test_a_reported_partial_run_is_not_ok(self):
        dp._set_group_status('Omaha', 'skipped', 'fetch deadline reached; 12 PDFs not read')
        dp._record_group('Omaha', [self._rec(self._recent(1))])
        run = dp._RUN_GROUPS['Omaha']
        self.assertEqual(run['status'], 'skipped')
        self.assertIn('12 PDFs not read', run['detail'])


class MhdSkippedJurisdictionTests(unittest.TestCase):
    def test_deadline_starved_jurisdictions_are_reported(self):
        saved = (dp._FETCH_DEADLINE, dp._MHD_PORTAL_BLOCKED)
        try:
            dp._FETCH_DEADLINE = 1.0          # long past
            dp._MHD_PORTAL_BLOCKED = False
            skipped = []
            with mock.patch.object(dp.time, 'sleep'):
                out = dp.fetch_dfw({'dallas': dp.DFW_JURISDICTIONS['dallas'],
                                    'plano': dp.DFW_JURISDICTIONS['plano']}, skipped=skipped)
        finally:
            dp._FETCH_DEADLINE, dp._MHD_PORTAL_BLOCKED = saved
        self.assertEqual(out, [])
        self.assertEqual(sorted(skipped), ['Dallas (deadline)', 'Plano (deadline)'])


class SnhdBreakerTests(unittest.TestCase):
    def setUp(self):
        self._saved = (dp._SNHD_BLOCKED, dp._SNHD_PAUSED_TOTAL, dp._SNHD_EXHAUSTED_STREAK,
                       dp._SNHD_RATE_LIMITED_UNTIL)
        dp._SNHD_BLOCKED = False
        dp._SNHD_PAUSED_TOTAL = 0.0
        dp._SNHD_EXHAUSTED_STREAK = 0
        dp._SNHD_RATE_LIMITED_UNTIL = 0.0

    def tearDown(self):
        (dp._SNHD_BLOCKED, dp._SNHD_PAUSED_TOTAL, dp._SNHD_EXHAUSTED_STREAK,
         dp._SNHD_RATE_LIMITED_UNTIL) = self._saved

    def _resp401(self):
        r = mock.Mock()
        r.status_code = 401
        r.headers = {}
        err = dp.requests.HTTPError('401 Unauthorized')
        err.response = r
        r.raise_for_status = mock.Mock(side_effect=err)
        return r

    def _resp(self, status):
        r = mock.Mock()
        r.status_code = status
        r.headers = {}
        err = dp.requests.HTTPError(f'{status} error')
        err.response = r
        r.raise_for_status = mock.Mock(side_effect=err)
        return r

    def test_a_lasting_403_trips_the_breaker_too(self):
        with mock.patch.object(dp.requests, 'get', return_value=self._resp(403)), \
             mock.patch.object(dp, '_snhd_wait_if_limited'), \
             mock.patch.object(dp.time, 'sleep'):
            for _ in range(dp.SNHD_BREAKER_TRIP):
                self.assertIsNone(dp._snhd_get('https://www.southernnevadahealthdistrict.org/x'))
        self.assertTrue(dp._SNHD_BLOCKED)

    def test_a_missing_permit_is_not_retried_and_does_not_count(self):
        with mock.patch.object(dp.requests, 'get', return_value=self._resp(404)) as get, \
             mock.patch.object(dp.time, 'sleep'):
            for _ in range(5):
                self.assertIsNone(dp._snhd_get('https://www.southernnevadahealthdistrict.org/gone'))
        self.assertEqual(get.call_count, 5)          # one attempt each
        self.assertFalse(dp._SNHD_BLOCKED)

    def test_a_success_ends_the_throttling_episode(self):
        ok = mock.Mock(status_code=200, headers={})
        ok.raise_for_status = mock.Mock()
        ok.json = mock.Mock(return_value={})
        dp._SNHD_PAUSED_TOTAL = 800.0
        with mock.patch.object(dp.requests, 'get', return_value=ok):
            dp._snhd_get('https://www.southernnevadahealthdistrict.org/x')
        self.assertEqual(dp._SNHD_PAUSED_TOTAL, 0.0)

    def test_a_lasting_401_trips_the_breaker_and_later_requests_fail_fast(self):
        with mock.patch.object(dp.requests, 'get', return_value=self._resp401()) as get, \
             mock.patch.object(dp, '_snhd_wait_if_limited'), \
             mock.patch.object(dp.time, 'sleep'):
            for _ in range(dp.SNHD_BREAKER_TRIP):
                self.assertIsNone(dp._snhd_get('https://www.southernnevadahealthdistrict.org/x'))
            self.assertTrue(dp._SNHD_BLOCKED)
            calls = get.call_count
            self.assertIsNone(dp._snhd_get('https://www.southernnevadahealthdistrict.org/z'))
            self.assertEqual(get.call_count, calls, 'no request once the breaker is open')

    def test_pauses_adding_up_trip_the_breaker(self):
        with mock.patch.object(dp, 'SNHD_MAX_TOTAL_PAUSE', 300.0):
            dp._snhd_pause_all(200)
            dp._snhd_pause_all(150)       # overlaps the first: counts as no extension
            self.assertAlmostEqual(dp._SNHD_PAUSED_TOTAL, 200, delta=1)
            self.assertFalse(dp._SNHD_BLOCKED)
            dp._SNHD_RATE_LIMITED_UNTIL = 0.0     # that pause has run out
            dp._snhd_pause_all(150)
        self.assertTrue(dp._SNHD_BLOCKED)

    def test_a_success_resets_the_streak(self):
        ok = mock.Mock(status_code=200, headers={})
        ok.raise_for_status = mock.Mock()
        ok.json = mock.Mock(return_value={'ok': 1})
        dp._SNHD_EXHAUSTED_STREAK = 1
        with mock.patch.object(dp.requests, 'get', return_value=ok):
            self.assertEqual(dp._snhd_get('https://www.southernnevadahealthdistrict.org/x'), {'ok': 1})
        self.assertEqual(dp._SNHD_EXHAUSTED_STREAK, 0)


class DataJsByteCapTests(unittest.TestCase):
    def test_the_written_file_never_exceeds_the_byte_cap(self):
        import tempfile
        recs = []
        for city in range(30):
            for k in range(40):
                recs.append({'name': f'R{city}-{k}', 'address': f'{k} Main St', 'city': f'Town{city}',
                             'state': 'TX', 'zip': '75001', 'latitude': 32.0, 'longitude': -96.0,
                             'inspection_date': f'2026-{1 + k % 9:02d}-{1 + k % 27:02d}',
                             'risk_score': 95, 'violations': [], 'source': 'X', 'source_url': 'u'})
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, 'data.js')
            with mock.patch.object(dp, 'DATA_JS_MAX_BYTES', 250_000):
                dp.write_data_js(recs, out)
            size = os.path.getsize(out)
            data = json.loads(open(out).read().split('window.DATA = ', 1)[1].rstrip(';\n'))
        self.assertLessEqual(size, 250_000)
        self.assertLess(len(data), 1200, 'something was trimmed')
        # Every town keeps its floor of most recent records.
        per_city = {}
        for r in data:
            per_city[r['c']] = per_city.get(r['c'], 0) + 1
        self.assertEqual(len(per_city), 30)
        self.assertTrue(all(n >= min(dp.EMBED_CITY_FLOOR, 40) for n in per_city.values()))


class VegasHoldTests(unittest.TestCase):
    """Las Vegas re-reads a window, not a day: its hold must give the next
    run the same window start, or the give-up count never sees one date
    twice and the window slides back two days a run."""
    SRC = 'Southern Nevada Health District'

    def setUp(self):
        self._saved = (dp._FRESHNESS, dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
                       dp._SNHD_BLOCKED, dp.SNHD_DETAIL_DELAY, dp._FETCH_DEADLINE)
        self.latest = self._recent(2)
        dp._FRESHNESS = {'groups': {'Las Vegas': {'latest': self.latest}},
                         'sources': {self.SRC: {'latest': self.latest, 'group': 'Las Vegas'}}}
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._SNHD_BLOCKED = False
        dp.SNHD_DETAIL_DELAY = 0
        dp._FETCH_DEADLINE = None

    def tearDown(self):
        (dp._FRESHNESS, holds, status, dp._SNHD_BLOCKED, dp.SNHD_DETAIL_DELAY,
         dp._FETCH_DEADLINE) = self._saved
        dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(holds)
        dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(status)

    def _recent(self, days_ago):
        return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')

    def _run(self, bad):
        """One weekly Las Vegas run against three permits; `bad` decides what
        the BAD permit's detail does: 'failed' (5xx), 'permanent' (404) or
        'cut' (trips the breaker)."""
        rows = [{'permit_number': pn, 'restaurant_name': pn, 'address': '1 Strip',
                 'city_name': 'Las Vegas', 'date_current': d, 'current_grade': 'A',
                 'current_demerits': 3}
                for pn, d in (('OK1', self._recent(1)), ('OK6', self._recent(6)),
                              ('BAD', self._recent(5)))]

        def fake_get(url, params=None, retries=4, info=None):
            if url == dp.SNHD_API:
                return {'results': rows}
            pn = url.rsplit('/', 1)[1]
            if pn != 'BAD':
                row = next(r for r in rows if r['permit_number'] == pn)
                return {'date_current': row['date_current'], 'current_grade': 'A',
                        'current_demerits': 3, 'current_violations_resolved': [],
                        'previous_inspections': []}
            if bad == 'permanent' and info is not None:
                info['permanent'] = True
            if bad == 'cut':
                dp._SNHD_BLOCKED = True
            return None

        since = dp._lookback_since('Las Vegas', datetime.now() - timedelta(days=8),
                                   dp.FRESHNESS_LOOKBACK_API_DAYS)
        with mock.patch.object(dp, '_snhd_get', fake_get):
            out = dp.fetch_vegas(since_date=since.strftime('%Y-%m-%dT00:00:00'))
        dp._record_group('Las Vegas', out)
        dp._SNHD_BLOCKED = False
        return since.strftime('%Y-%m-%d'), out

    def test_a_failing_permit_holds_the_same_window_then_is_given_up(self):
        starts = []
        for _ in range(dp.FRESHNESS_HOLD_GIVE_UP_RUNS):
            since, out = self._run('failed')
            starts.append(since)
            self.assertEqual({r['inspection_date'] for r in out},
                             {self._recent(1), self._recent(6)})
        # The window never slid back: every run started on the same day.
        self.assertEqual(starts, [self._recent(8)] * dp.FRESHNESS_HOLD_GIVE_UP_RUNS)
        e = dp._FRESHNESS['sources'][self.SRC]
        self.assertNotIn('hold', e)
        self.assertEqual(len(e['given_up']), 1)
        self.assertEqual(e['latest'], self._recent(1))
        # Still failing next week: given up, so it holds nothing.
        self._run('failed')
        self.assertNotIn('hold', dp._FRESHNESS['sources'][self.SRC])
        self.assertEqual(dp._FRESHNESS['sources'][self.SRC]['latest'], self._recent(1))

    def test_a_missing_permit_is_not_held_at_all(self):
        self._run('permanent')
        e = dp._FRESHNESS['sources'][self.SRC]
        self.assertNotIn('hold', e)
        self.assertEqual(e['latest'], self._recent(1))

    def test_a_breaker_cut_holds_without_counting_and_reports_blocked(self):
        for _ in range(dp.FRESHNESS_HOLD_GIVE_UP_RUNS + 1):
            since, _ = self._run('cut')
            self.assertEqual(since, self._recent(8))
            self.assertEqual(dp._RUN_GROUPS['Las Vegas']['status'], 'blocked')
        e = dp._FRESHNESS['sources'][self.SRC]
        self.assertEqual(e['hold']['failures'], {})
        self.assertNotIn('given_up', e)
