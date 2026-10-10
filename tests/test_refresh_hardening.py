"""Tests for the weekly-refresh hardening in dinescores_pipeline.py: the
self-healing lookback, the relay credential, SNHD rate-limit handling and
stalest-first MHD ordering. No network: every HTTP call is mocked."""
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
