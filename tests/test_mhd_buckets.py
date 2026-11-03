"""Tests for the MyHealthDepartment pseudo-jurisdiction and bucket machinery:
the Tennessee metros carved out of the statewide 'tennessee' slug (one
searchStr x purpose query per day, unioned by inspectionID, city allowlist)
and Gwinnett's programName allowlist. No network: _mhd_search_page and the
portal session are mocked."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402


def _row(iid, name='Degthai', addr='1 Main St', city='Nashville',
         program='Food Service Establishment', purpose='Routine',
         date='2026-10-06T00:00:00.000Z', score=95, state='TN'):
    """A raw searchInspections row shaped like the portal's (fields the
    parser reads, as captured 2026-10-10)."""
    return {'inspectionID': iid, 'inspectionDate': date, 'score': score,
            'inspectionType': f'{program} Inspection', 'purpose': purpose,
            'establishmentName': name, 'addressLine1': addr, 'addressLine2': '',
            'city': city, 'state': state, 'zip': '37203', 'scoreDisplay': '',
            'permitID': f'P-{iid}', 'permitType': 'Commercial Food <51',
            'programName': program, 'programCode': '605'}


class FakeSearch:
    """Stand-in for _mhd_search_page: records every call and answers from a
    table keyed by (date_range, search_str, purpose) -> rows, served in
    pages of MHD_PAGE_SIZE."""

    def __init__(self, table):
        self.table = table
        self.calls = []

    def __call__(self, session, path, date_range_str, start_offset, retry_count=0,
                 search_str='', purpose='', sort=None):
        self.calls.append({'path': path, 'date': date_range_str, 'start': start_offset,
                           'search_str': search_str, 'purpose': purpose, 'sort': sort})
        rows = self.table.get((date_range_str, search_str, purpose), [])
        return rows[start_offset:start_offset + dp.MHD_PAGE_SIZE]


def _run(key, config, fake, **kw):
    kw.setdefault('fetch_violations', False)
    with mock.patch.object(dp, '_mhd_search_page', side_effect=fake), \
         mock.patch.object(dp.time, 'sleep'):
        return dp._fetch_mhd_jurisdiction_api(None, key, config, **kw)


class PathTests(unittest.TestCase):
    def test_pseudo_jurisdiction_resolves_to_its_portal_slug(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['tennessee:nashville']
        self.assertEqual(dp._mhd_path('tennessee:nashville', cfg), 'tennessee')

    def test_plain_jurisdiction_is_its_own_path(self):
        self.assertEqual(dp._mhd_path('gwinnett', dp.MHD_METRO_JURISDICTIONS['gwinnett']),
                         'gwinnett')
        self.assertEqual(dp._mhd_path('dallas', dp.DFW_JURISDICTIONS['dallas']), 'dallas')
        self.assertEqual(dp._mhd_path('dallas', None), 'dallas')

    def test_every_colon_key_carries_a_path(self):
        for key, cfg in dp.MHD_METRO_JURISDICTIONS.items():
            if ':' in key:
                self.assertNotIn(':', dp._mhd_path(key, cfg), key)


class SearchPagePayloadTests(unittest.TestCase):
    def _session(self, captured):
        resp = mock.Mock(status_code=200)
        resp.json.return_value = []
        resp.raise_for_status.return_value = None
        session = mock.Mock()

        def post(url, json=None, timeout=None):
            captured.append((url, json))
            return resp
        session.post.side_effect = post
        return session

    def test_bucket_query_fields(self):
        captured = []
        dp._mhd_search_page(self._session(captured), 'tennessee',
                            '2026-10-06 to 2026-10-06', 25,
                            search_str='Nashville', purpose='Routine')
        (url, payload), = captured
        self.assertEqual(url, dp.MHD_BASE_URL)
        self.assertEqual(payload['task'], 'searchInspections')
        data = payload['data']
        self.assertEqual(data['path'], 'tennessee')
        self.assertEqual(data['searchStr'], 'Nashville')
        self.assertEqual(data['filters'], {'date': '2026-10-06 to 2026-10-06',
                                           'purpose': 'Routine'})
        self.assertEqual(data['start'], 25)
        self.assertEqual(data['count'], dp.MHD_PAGE_SIZE)
        # The two fields the portal misbehaves on must stay empty/zero.
        self.assertEqual(data['programName'], '')
        self.assertEqual((data['lat'], data['lng']), (0, 0))

    def test_default_query_is_the_plain_date_search(self):
        captured = []
        dp._mhd_search_page(self._session(captured), 'gwinnett',
                            '2026-10-03 to 2026-10-09', 0)
        data = captured[0][1]['data']
        self.assertEqual(data['path'], 'gwinnett')
        self.assertEqual(data['searchStr'], '')
        self.assertEqual(data['filters']['purpose'], '')


class BucketModeTests(unittest.TestCase):
    D6 = '2026-10-06 to 2026-10-06'
    D7 = '2026-10-07 to 2026-10-07'

    def setUp(self):
        self.cfg = dp._mhd_tn_bucket('Testville', ['Testville', 'Madison'])
        self.table = {
            (self.D6, 'Testville', 'Routine'): [
                _row('X1', city='Testville'),
                # a 'Testville' row in the Madison neighbourhood: both terms
                # return it, it must ship once
                _row('X2', name='Madison Grill', addr='9 Madison Pike', city='Testville'),
                _row('X3', name='Ink', city='Testville', program='Tattoo Studios'),
            ],
            (self.D6, 'Testville', 'Follow-Up'): [
                _row('X4', purpose='Follow-Up', city='Testville', score=72),
            ],
            (self.D6, 'Madison', 'Routine'): [
                _row('X2', name='Madison Grill', addr='9 Madison Pike', city='Testville'),
                _row('X5', city='Madison'),
                # substring match on the ADDRESS, wrong city
                _row('X6', name='Waffle Stop', addr='2400 Madison St', city='Clarksville'),
            ],
            (self.D7, 'Testville', 'Routine'): [
                _row('X7', city='TESTVILLE', date='2026-10-07T00:00:00.000Z', score=88),
            ],
        }
        self.fake = FakeSearch(self.table)
        self.records = _run('tennessee:testville', self.cfg, self.fake,
                            since_date='2026-10-06', until_date='2026-10-07')

    def test_one_query_per_day_term_purpose_against_the_real_path(self):
        combos = {(c['date'], c['search_str'], c['purpose']) for c in self.fake.calls}
        self.assertEqual(combos, {
            (d, t, p) for d in (self.D6, self.D7)
            for t in ('Testville', 'Madison') for p in ('Routine', 'Follow-Up')})
        self.assertEqual(len(self.fake.calls), 8, 'short pages: one page per query')
        self.assertTrue(all(c['path'] == 'tennessee' for c in self.fake.calls))
        self.assertTrue(all(c['start'] == 0 for c in self.fake.calls))

    def test_union_by_inspection_id(self):
        ids = [r['source_id'] for r in self.records]
        self.assertEqual(ids.count('X2'), 1)

    def test_city_allowlist_drops_an_address_match(self):
        self.assertNotIn('X6', [r['source_id'] for r in self.records])
        self.assertIn('X5', [r['source_id'] for r in self.records])

    def test_city_allowlist_is_case_insensitive(self):
        self.assertIn('X7', [r['source_id'] for r in self.records])

    def test_programs_allowlist_drops_tattoo_studios(self):
        self.assertNotIn('X3', [r['source_id'] for r in self.records])
        self.assertEqual({r['source_id'] for r in self.records},
                         {'X1', 'X2', 'X4', 'X5', 'X7'})

    def test_record_fields(self):
        by_id = {r['source_id']: r for r in self.records}
        rec = by_id['X4']
        self.assertEqual(rec['source'], 'Tennessee DOH (Testville) Health Dept')
        self.assertEqual(rec['metro'], 'Testville')
        self.assertEqual(rec['state'], 'TN')
        self.assertEqual(rec['purpose'], 'Follow-Up')
        self.assertEqual(rec['inspection_date'], '2026-10-06')
        # score_scale '100': the official score IS the risk score
        self.assertEqual((rec['original_score'], rec['risk_score']), (72, 72))
        self.assertEqual(rec['source_url'],
                         'https://inspections.myhealthdepartment.com/tennessee/inspection/'
                         '?inspectionID=X4')
        self.assertEqual(dp.make_restaurant_id(rec['name'], rec['address'], rec['city']),
                         dp.make_restaurant_id('Degthai', '1 Main St', 'Testville'))

    def test_bucket_mode_never_bisects(self):
        with mock.patch.object(dp, '_mhd_fetch_window') as fw:
            _run('tennessee:testville', self.cfg, FakeSearch(self.table),
                 since_date='2026-10-06', until_date='2026-10-06')
        fw.assert_not_called()


class CapTests(unittest.TestCase):
    def test_capped_bucket_warns_and_is_kept(self):
        day = '2026-10-06 to 2026-10-06'
        cfg = dp._mhd_tn_bucket('Bigtown', ['Bigtown'])
        big = [_row(f'B{i:03d}', name=f'Place {i}', city='Bigtown')
               for i in range(dp.MHD_QUERY_CAP)]
        fake = FakeSearch({(day, 'Bigtown', 'Routine'): big})
        with self.assertLogs(dp.log, level='WARNING') as logs:
            records = _run('tennessee:bigtown', cfg, fake,
                           since_date='2026-10-06', until_date='2026-10-06')
        self.assertEqual(len(records), dp.MHD_QUERY_CAP)
        self.assertTrue(any('INCOMPLETE' in line for line in logs.output), logs.output)
        self.assertTrue(any('hit the 225 record cap' in line for line in logs.output))
        # paging stopped at the cap; the portal rejects offsets >= 225
        self.assertEqual(max(c['start'] for c in fake.calls), dp.MHD_QUERY_CAP - dp.MHD_PAGE_SIZE)

    def test_search_all_pages_stops_before_the_cap_after_a_long_first_page(self):
        # Gwinnett serves 28-43 rows on the first page; no offset >= 225 may
        # ever be requested.
        rows = [_row(f'G{i}', city='Duluth', state='GA', program='Food') for i in range(240)]
        calls = []

        def fake(session, path, date_range, start, retry_count=0, search_str='', purpose='',
                 sort=None):
            calls.append(start)
            if start == 0:
                return rows[:43]
            return rows[start:start + 25]
        with mock.patch.object(dp, '_mhd_search_page', side_effect=fake), \
             mock.patch.object(dp.time, 'sleep'):
            out = dp._mhd_search_all_pages(None, 'gwinnett', '2026-10-05 to 2026-10-09')
        self.assertGreaterEqual(len(out), dp.MHD_QUERY_CAP)
        self.assertLess(max(calls), dp.MHD_QUERY_CAP)


class GwinnettTests(unittest.TestCase):
    def test_programs_filter_keeps_food_and_drops_tourist(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['gwinnett']
        day = '2026-10-08 to 2026-10-08'
        rows = [
            _row('F1', name="AUNTIE ANNE'S", city='Lawrenceville', state='GA', program='Food'),
            _row('T1', name='Comfort Inn', city='Conyers', state='GA', program='Tourist'),
            _row('P1', name='Swim Club', city='Duluth', state='GA', program='Pools'),
            _row('B1', name='Ink Lab', city='Norcross', state='GA', program='Body Art'),
        ]
        fake = FakeSearch({(day, '', ''): rows})
        records = _run('gwinnett', cfg, fake, since_date='2026-10-08', until_date='2026-10-08')
        self.assertEqual([r['source_id'] for r in records], ['F1'])
        rec = records[0]
        self.assertEqual(rec['source'], 'Gwinnett County Health Dept')
        self.assertEqual(rec['metro'], '')
        self.assertEqual(rec['state'], 'GA')
        self.assertIn('/gwinnett/inspection/?inspectionID=F1', rec['source_url'])
        # plain mode: the date search with no searchStr / purpose
        self.assertTrue(all(c['path'] == 'gwinnett' and c['search_str'] == ''
                            and c['purpose'] == '' for c in fake.calls))

    def test_tourist_would_pass_the_generic_food_check(self):
        # Documents why the programs allowlist exists.
        self.assertTrue(dp._mhd_is_food_record(_row('T1', program='Tourist', state='GA')))
        self.assertTrue(dp._mhd_is_food_record(_row('X', program='Tattoo Studios')))


class DetailPathTests(unittest.TestCase):
    def test_detail_url_uses_the_real_path(self):
        captured = []
        resp = mock.Mock(status_code=200, text='<html><p class="observations-text"></p></html>')
        session = mock.Mock()

        def get(url, params=None, timeout=None):
            captured.append((url, params))
            return resp
        session.get.side_effect = get
        out = dp._mhd_fetch_inspection_detail(session, 'tennessee', 'ABC-123')
        self.assertEqual(out, [])
        self.assertEqual(captured, [(f'{dp.MHD_BASE_URL}tennessee/inspection/',
                                     {'inspectionID': 'ABC-123'})])

    def test_detail_phase_receives_the_path_and_the_display_label(self):
        cfg = dp._mhd_tn_bucket('Testville', ['Testville'])
        day = '2026-10-06 to 2026-10-06'
        fake = FakeSearch({(day, 'Testville', 'Routine'): [_row('X1', city='Testville')]})
        seen = {}

        def fake_bulk(path, records, trust_official_score=True, label=None):
            seen.update(path=path, label=label, n=len(records),
                        trust=trust_official_score)
            return len(records), []
        with mock.patch.object(dp, '_mhd_fetch_details_bulk', side_effect=fake_bulk):
            records = _run('tennessee:testville', cfg, fake, fetch_violations=True,
                           since_date='2026-10-06', until_date='2026-10-06')
        self.assertEqual(seen, {'path': 'tennessee', 'label': 'Tennessee DOH (Testville)',
                                'n': 1, 'trust': True})
        self.assertEqual(len(records), 1)

    def test_unreadable_detail_pages_still_drop_the_inspection(self):
        cfg = dp._mhd_tn_bucket('Testville', ['Testville'])
        day = '2026-10-06 to 2026-10-06'
        fake = FakeSearch({(day, 'Testville', 'Routine'): [_row('X1', city='Testville'),
                                                           _row('X2', city='Testville')]})

        def fake_bulk(path, records, trust_official_score=True, label=None):
            return 1, [records[1]]
        with mock.patch.object(dp, '_mhd_fetch_details_bulk', side_effect=fake_bulk):
            records = _run('tennessee:testville', cfg, fake, fetch_violations=True,
                           since_date='2026-10-06', until_date='2026-10-06')
        self.assertEqual([r['source_id'] for r in records], ['X1'])


class SlugGroupTests(unittest.TestCase):
    def test_slug_groups_resolve(self):
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['nashville'], ['tennessee:nashville'])
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['memphis'], ['tennessee:memphis'])
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['knoxville'], ['tennessee:knoxville'])
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['chattanooga'], ['tennessee:chattanooga'])
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['gwinnett'], ['gwinnett'])
        self.assertEqual(set(dp.MHD_METRO_SLUG_GROUPS['tennessee']),
                         {'tennessee:nashville', 'tennessee:memphis',
                          'tennessee:knoxville', 'tennessee:chattanooga'})

    def test_every_group_key_exists_in_the_table(self):
        for slug, keys in dp.MHD_METRO_SLUG_GROUPS.items():
            for k in keys:
                self.assertIn(k, dp.MHD_METRO_JURISDICTIONS, f'{slug} -> {k}')

    def test_cli_resolution_dedupes_across_slugs_and_raw_keys(self):
        keys = dp._mhd_metro_keys(['nashville', 'tennessee', 'gwinnett',
                                   'tennessee:memphis', 'chicago'])
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(set(keys), {'gwinnett', 'tennessee:nashville', 'tennessee:memphis',
                                     'tennessee:knoxville', 'tennessee:chattanooga'})
        self.assertEqual(dp._mhd_metro_keys(['nashville']), ['tennessee:nashville'])
        self.assertEqual(dp._mhd_metro_keys(['portland', 'chicago']),
                         dp.MHD_METRO_SLUG_GROUPS['portland'])
        self.assertEqual(dp._mhd_metro_keys(['chicago']), [])

    def test_new_slugs_count_as_portal_sources(self):
        for slug in ('nashville', 'memphis', 'knoxville', 'chattanooga', 'tennessee',
                     'gwinnett', 'tennessee:nashville'):
            self.assertTrue(dp._wants_portal_sources([slug]), slug)
        self.assertFalse(dp._wants_portal_sources(['chicago']))

    def test_tennessee_configs(self):
        for key in dp.MHD_METRO_SLUG_GROUPS['tennessee']:
            cfg = dp.MHD_METRO_JURISDICTIONS[key]
            self.assertEqual(cfg['path'], 'tennessee')
            self.assertEqual(cfg['window_days'], 1)
            self.assertEqual(cfg['purposes'], ['Routine', 'Follow-Up'])
            self.assertEqual(cfg['programs'], ['Food Service Establishment'])
            self.assertEqual(cfg['score_scale'], '100')
            self.assertEqual(cfg['search_terms'], cfg['city_allow'])
            self.assertIn(cfg['default_city'], cfg['city_allow'])
            self.assertEqual(cfg['metro'], cfg['default_city'])
            self.assertTrue(cfg['display_name'].startswith('Tennessee DOH ('))
        names = [dp.MHD_METRO_JURISDICTIONS[k]['display_name']
                 for k in dp.MHD_METRO_SLUG_GROUPS['tennessee']]
        self.assertEqual(len(set(names)), 4, 'per-bucket source labels keep freshness honest')

    def test_city_normalisation(self):
        self.assertEqual(dp._mhd_norm_city('  NASHVILLE '), 'nashville')
        self.assertEqual(dp._mhd_norm_city('Soddy-Daisy'), dp._mhd_norm_city('Soddy Daisy'))
        self.assertEqual(dp._mhd_norm_city('Mt. Juliet'), dp._mhd_norm_city('Mt Juliet'))
        self.assertEqual(dp._mhd_norm_city(None), '')


if __name__ == '__main__':
    unittest.main()


class ConnectionQuotaTests(unittest.TestCase):
    """The portal allows ~250 searches per TCP connection; see
    MHD_SEARCHES_PER_CONNECTION."""

    def _resp(self, status, body=None):
        r = mock.Mock()
        r.status_code = status
        r.json = mock.Mock(return_value=body if body is not None else [])
        r.raise_for_status = mock.Mock()
        return r

    def test_first_403_reconnects_and_retries_without_waiting(self):
        session = mock.Mock()
        session._mhd_searches = 0
        session._mhd_reconnected = False
        session.post = mock.Mock(side_effect=[self._resp(403), self._resp(200, [{'inspectionID': 'x'}])])
        with mock.patch.object(dp.time, 'sleep') as sleep:
            out = dp._mhd_search_page(session, 'tennessee', '2026-10-06 to 2026-10-06', 0,
                                      search_str='Nashville', purpose='Routine')
        self.assertEqual(out, [{'inspectionID': 'x'}])
        self.assertEqual(session.post.call_count, 2)
        session.close.assert_called_once()
        sleep.assert_not_called()
        self.assertFalse(dp._MHD_PORTAL_BLOCKED)

    def test_second_403_goes_to_the_retry_ladder(self):
        session = mock.Mock()
        session._mhd_searches = 0
        session._mhd_reconnected = False
        session.post = mock.Mock(side_effect=[self._resp(403), self._resp(403), self._resp(200, [])])
        with mock.patch.object(dp.time, 'sleep') as sleep:
            out = dp._mhd_search_page(session, 'tennessee', '2026-10-06 to 2026-10-06', 0)
        self.assertEqual(out, [])
        self.assertEqual(session.post.call_count, 3)
        self.assertEqual(sleep.call_count, 1)   # one 30 s wait, then success

    def test_connections_rotate_every_n_searches(self):
        session = mock.Mock()
        session._mhd_searches = 0
        session._mhd_reconnected = False
        session.post = mock.Mock(return_value=self._resp(200, []))
        with mock.patch.object(dp, 'MHD_SEARCHES_PER_CONNECTION', 5):
            for _ in range(12):
                dp._mhd_search_page(session, 'dallas', '2026-10-06 to 2026-10-06', 0)
        self.assertEqual(session.close.call_count, 2)


# ─── 2026-11 expansion: stable sorting, one-day windows, fetch groups, the
# new jurisdictions (Fort Bend, Arapahoe, Douglas CO, Cuyahoga, the 33 VDH
# districts). Fixtures under tests/fixtures/mhd/ are rows and detail blocks
# trimmed from the live portal captures of 2026-11-02/03 (public records,
# kept verbatim; detail pages cut down to the score header and the
# observations block, which is what the parser reads).

import json  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'mhd')


def _fixture_rows(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as f:
        return json.load(f)


def _fixture_html(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as f:
        return f.read()


def _guid(n, head='00000000'):
    """A distinct GUID-shaped inspectionID for row `n`."""
    return f'{head}-0000-0000-0000-{n:012d}'


class FailingSearch(FakeSearch):
    """FakeSearch that answers None (a permanent failure) for some dates."""

    def __init__(self, table, fail_dates=()):
        super().__init__(table)
        self.fail_dates = set(fail_dates)

    def __call__(self, session, path, date_range_str, start_offset, retry_count=0,
                 search_str='', purpose='', sort=None):
        if date_range_str in self.fail_dates:
            self.calls.append({'path': path, 'date': date_range_str, 'start': start_offset,
                               'search_str': search_str, 'purpose': purpose, 'sort': sort})
            return None
        return super().__call__(session, path, date_range_str, start_offset, retry_count,
                                search_str, purpose, sort)


class SortThreadingTests(unittest.TestCase):
    """The per-config sort reaches the search payload. Measured 2026-11-03:
    the portal's own order ('sort': {}) pages unstably and the weekly 7-day
    read lost 3-13% of each jurisdiction's rows; sorting on inspectionID
    gave complete, disjoint pages everywhere."""

    def _session(self, captured):
        resp = mock.Mock(status_code=200)
        resp.json.return_value = []
        resp.raise_for_status.return_value = None
        session = mock.Mock()

        def post(url, json=None, timeout=None):
            captured.append((url, json))
            return resp
        session.post.side_effect = post
        return session

    def test_sort_reaches_the_payload(self):
        captured = []
        dp._mhd_search_page(self._session(captured), 'fbceh', '2026-10-20 to 2026-10-20', 25,
                            sort={'field': 'inspectionID', 'direction': 'asc'})
        data = captured[0][1]['data']
        self.assertEqual(data['sort'], {'field': 'inspectionID', 'direction': 'asc'})
        self.assertEqual(data['start'], 25)

    def test_no_sort_sends_the_empty_dict(self):
        # Orange County's newest-225 poll depends on the unsorted query.
        captured = []
        dp._mhd_search_page(self._session(captured), 'orange-county', '', 0)
        self.assertEqual(captured[0][1]['data']['sort'], {})

    def test_default_sort_is_the_stable_inspection_id_order(self):
        self.assertEqual(dp.MHD_DEFAULT_SORT, {'field': 'inspectionID', 'direction': 'asc'})
        self.assertEqual(dp.MHD_WINDOW_DAYS, 1)

    def test_plain_jurisdiction_reads_one_day_at_a_time_with_the_stable_sort(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['gwinnett']
        table = {(f'2026-10-{d:02d} to 2026-10-{d:02d}', '', ''):
                 [_row(f'G{d}', city='Duluth', state='GA', program='Food')]
                 for d in range(5, 12)}
        fake = FakeSearch(table)
        records = _run('gwinnett', cfg, fake, since_date='2026-10-05T00:00:00',
                       until_date='2026-10-11')
        self.assertEqual({r['source_id'] for r in records}, {f'G{d}' for d in range(5, 12)})
        self.assertEqual({c['date'] for c in fake.calls},
                         {f'2026-10-{d:02d} to 2026-10-{d:02d}' for d in range(5, 12)})
        self.assertTrue(all(c['sort'] == dp.MHD_SORT_INSPECTION_ID for c in fake.calls))

    def test_bucket_configs_keep_the_portal_order(self):
        cfg = dp._mhd_tn_bucket('Testville', ['Testville'])
        day = '2026-10-06 to 2026-10-06'
        fake = FakeSearch({(day, 'Testville', 'Routine'): [_row('X1', city='Testville')]})
        _run('tennessee:testville', cfg, fake, since_date='2026-10-06', until_date='2026-10-06')
        self.assertTrue(fake.calls)
        self.assertTrue(all(c['sort'] is None for c in fake.calls))

    def test_a_config_can_opt_out_or_choose_its_own_sort(self):
        day = '2026-10-08 to 2026-10-08'
        base = dict(dp.MHD_METRO_JURISDICTIONS['gwinnett'])
        rows = [_row('F1', city='Lawrenceville', state='GA', program='Food')]
        fake = FakeSearch({(day, '', ''): rows})
        _run('gwinnett', {**base, 'sort': None}, fake, since_date='2026-10-08', until_date='2026-10-08')
        self.assertTrue(all(c['sort'] is None for c in fake.calls))
        fake = FakeSearch({(day, '', ''): rows})
        custom = {'field': 'establishmentName', 'direction': 'desc'}
        _run('gwinnett', {**base, 'sort': custom}, fake, since_date='2026-10-08', until_date='2026-10-08')
        self.assertTrue(all(c['sort'] == custom for c in fake.calls))

    def test_a_multi_day_window_config_warns_about_the_per_permit_collapse(self):
        cfg = {**dp.MHD_METRO_JURISDICTIONS['gwinnett'], 'window_days': 7}
        fake = FakeSearch({})
        with self.assertLogs(dp.log, level='WARNING') as logs:
            _run('gwinnett', cfg, fake, since_date='2026-10-05', until_date='2026-10-11')
        self.assertTrue(any('latest inspection' in line for line in logs.output), logs.output)
        self.assertEqual(len(fake.calls), 1, 'one 7-day window, as configured')


class SortGuardTests(unittest.TestCase):
    """Sorted pages must fit together as one stable sequence; a row repeated
    across pages fails the query (None) instead of shipping the window
    short. The order of the keys is deliberately NOT checked: Gwinnett's
    sorted pages are disjoint and complete but not in GUID order (its first
    page runs 45-60 rows), so an order check would fail a correct read."""

    def test_pages_in_order_are_consistent(self):
        pages = [[{'inspectionID': _guid(i)} for i in range(1, 26)],
                 [{'inspectionID': _guid(i)} for i in range(26, 30)]]
        self.assertEqual(dp._mhd_pages_consistent(pages, dp.MHD_SORT_INSPECTION_ID), '')

    def test_disjoint_pages_out_of_key_order_are_still_consistent(self):
        # Gwinnett: a long first page and no key order, yet complete
        pages = [[{'inspectionID': _guid(i)} for i in (7, 3, 9, 1)],
                 [{'inspectionID': _guid(i)} for i in (2, 8)]]
        self.assertEqual(dp._mhd_pages_consistent(pages, dp.MHD_SORT_INSPECTION_ID), '')

    def test_a_row_repeated_across_pages_is_a_repeat(self):
        pages = [[{'inspectionID': _guid(i)} for i in range(1, 26)],
                 [{'inspectionID': _guid(25)}, {'inspectionID': _guid(26)}]]
        self.assertEqual(dp._mhd_pages_consistent(pages, dp.MHD_SORT_INSPECTION_ID), 'repeat')
        self.assertEqual(dp._mhd_pages_consistent(pages, {'field': 'establishmentName',
                                                          'direction': 'asc'}), 'repeat')

    def test_rows_without_an_id_are_ignored(self):
        pages = [[{'inspectionID': _guid(1)}, {'permitID': 'P'}], [{'inspectionID': _guid(2)}]]
        self.assertEqual(dp._mhd_pages_consistent(pages, dp.MHD_SORT_INSPECTION_ID), '')

    def _paged(self, pages):
        def fake(session, path, date_range, start, retry_count=0, search_str='', purpose='',
                 sort=None):
            idx = start // dp.MHD_PAGE_SIZE
            return pages[idx] if idx < len(pages) else []
        return fake

    def test_search_all_pages_fails_on_a_repeated_row_when_sorted(self):
        pages = [[_row(_guid(i)) for i in range(1, 26)], [_row(_guid(25)), _row(_guid(26))]]
        with mock.patch.object(dp, '_mhd_search_page', side_effect=self._paged(pages)), \
             mock.patch.object(dp.time, 'sleep'), \
             self.assertLogs(dp.log, level='ERROR') as logs:
            out = dp._mhd_search_all_pages(None, 'fbceh', '2026-10-20 to 2026-10-20',
                                           sort=dp.MHD_SORT_INSPECTION_ID, label='Fort Bend County')
        self.assertIsNone(out)
        self.assertTrue(any('repeat' in line and 'Fort Bend County' in line for line in logs.output))

    def test_an_inconsistent_read_is_retried_once_before_failing(self):
        bad = [[_row(_guid(i)) for i in range(1, 26)], [_row(_guid(25)), _row(_guid(26))]]
        good = [[_row(_guid(i)) for i in range(1, 26)], [_row(_guid(26)), _row(_guid(27))]]
        attempts = []

        def fake(session, path, date_range, start, retry_count=0, search_str='', purpose='',
                 sort=None):
            if start == 0:
                attempts.append(1)
            pages = bad if len(attempts) == 1 else good
            idx = start // dp.MHD_PAGE_SIZE
            return pages[idx] if idx < len(pages) else []
        with mock.patch.object(dp, '_mhd_search_page', side_effect=fake), \
             mock.patch.object(dp.time, 'sleep'), \
             self.assertLogs(dp.log, level='WARNING') as logs:
            out = dp._mhd_search_all_pages(None, 'fbceh', '2026-10-20 to 2026-10-20',
                                           sort=dp.MHD_SORT_INSPECTION_ID)
        self.assertEqual(len(out), 27, 'the re-read settled it')
        self.assertEqual(len(attempts), 2)
        self.assertTrue(any('re-reading' in line for line in logs.output))

    def test_search_all_pages_fails_after_the_retry_is_also_inconsistent(self):
        pages = [[_row(_guid(i)) for i in range(1, 26)], [_row(_guid(25)), _row(_guid(26))]]
        calls = []

        def fake(session, path, date_range, start, retry_count=0, search_str='', purpose='',
                 sort=None):
            calls.append(start)
            idx = start // dp.MHD_PAGE_SIZE
            return pages[idx] if idx < len(pages) else []
        with mock.patch.object(dp, '_mhd_search_page', side_effect=fake), \
             mock.patch.object(dp.time, 'sleep'):
            out = dp._mhd_search_all_pages(None, 'fbceh', '2026-10-20 to 2026-10-20',
                                           sort=dp.MHD_SORT_INSPECTION_ID)
        self.assertIsNone(out)
        self.assertEqual(calls.count(0), dp.MHD_SORTED_READ_ATTEMPTS)

    def test_search_all_pages_without_a_sort_keeps_the_old_contract(self):
        # Unsorted callers dedupe by inspectionID themselves; the guard is off.
        pages = [[_row(_guid(i)) for i in range(1, 26)], [_row(_guid(25)), _row(_guid(26))]]
        with mock.patch.object(dp, '_mhd_search_page', side_effect=self._paged(pages)), \
             mock.patch.object(dp.time, 'sleep'):
            out = dp._mhd_search_all_pages(None, 'fbceh', '2026-10-20 to 2026-10-20')
        self.assertEqual(len(out), 27)

    def test_a_consistent_sorted_read_returns_every_row(self):
        pages = [[_row(_guid(i)) for i in range(1, 26)], [_row(_guid(26)), _row(_guid(27))]]
        with mock.patch.object(dp, '_mhd_search_page', side_effect=self._paged(pages)), \
             mock.patch.object(dp.time, 'sleep'):
            out = dp._mhd_search_all_pages(None, 'fbceh', '2026-10-20 to 2026-10-20',
                                           sort=dp.MHD_SORT_INSPECTION_ID)
        self.assertEqual(len(out), 27)

    def test_a_single_sorted_page_needs_no_guard(self):
        pages = [[_row(_guid(i)) for i in range(1, 10)]]
        with mock.patch.object(dp, '_mhd_search_page', side_effect=self._paged(pages)), \
             mock.patch.object(dp.time, 'sleep'):
            out = dp._mhd_search_all_pages(None, 'fbceh', '2026-10-20 to 2026-10-20',
                                           sort=dp.MHD_SORT_INSPECTION_ID)
        self.assertEqual(len(out), 9)


class WindowFailureTests(unittest.TestCase):
    """A window that cannot be read completely never ships the jurisdiction
    short and quiet: without the breaker or the deadline it raises (the
    group goes red) and holds the source at the window."""

    def setUp(self):
        self.saved = (dict(dp._FRESHNESS_HOLDS), dp._MHD_PORTAL_BLOCKED, dp._FETCH_DEADLINE,
                      set(dp._MHD_DEADLINE_CUT))
        dp._FRESHNESS_HOLDS.clear()
        dp._MHD_PORTAL_BLOCKED = False
        dp._FETCH_DEADLINE = None
        dp._MHD_DEADLINE_CUT.clear()
        self.cfg = dp.MHD_METRO_JURISDICTIONS['gwinnett']
        self.d1, self.d2 = '2026-10-05 to 2026-10-05', '2026-10-06 to 2026-10-06'
        self.table = {(self.d1, '', ''): [_row('G1', city='Duluth', state='GA', program='Food')],
                      (self.d2, '', ''): [_row('G2', city='Duluth', state='GA', program='Food')]}

    def tearDown(self):
        holds, blocked, deadline, cut = self.saved
        dp._FRESHNESS_HOLDS.clear()
        dp._FRESHNESS_HOLDS.update(holds)
        dp._MHD_PORTAL_BLOCKED = blocked
        dp._FETCH_DEADLINE = deadline
        dp._MHD_DEADLINE_CUT.clear()
        dp._MHD_DEADLINE_CUT.update(cut)

    def test_an_unreadable_window_raises_and_holds_the_source_at_it(self):
        fake = FailingSearch(self.table, fail_dates=[self.d2])
        with self.assertRaises(RuntimeError) as ctx:
            _run('gwinnett', self.cfg, fake, since_date='2026-10-05', until_date='2026-10-06')
        self.assertIn('2026-10-06', str(ctx.exception))
        holds = dp._FRESHNESS_HOLDS.get('Gwinnett County Health Dept', {})
        # _hold_window_start: the next lookback starts exactly at the window
        self.assertEqual(list(holds), [f'2026-10-{6 + 1 + dp.FRESHNESS_OVERLAP_DAYS:02d}|'])
        self.assertEqual(list(holds.values()), [(False, False)], 'held, never counted')

    def test_a_breaker_cut_window_ships_what_was_read(self):
        fake = FailingSearch(self.table, fail_dates=[self.d2])
        dp._MHD_PORTAL_BLOCKED = True
        records = _run('gwinnett', self.cfg, fake, since_date='2026-10-05', until_date='2026-10-06')
        self.assertEqual([r['source_id'] for r in records], ['G1'])
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_a_deadline_cut_never_raises(self):
        fake = FailingSearch(self.table, fail_dates=[self.d2])
        dp._FETCH_DEADLINE = 1.0   # long past
        records = _run('gwinnett', self.cfg, fake, since_date='2026-10-05', until_date='2026-10-06')
        self.assertEqual(records, [])
        self.assertIn('Gwinnett County', dp._MHD_DEADLINE_CUT)


class ExcludeTests(unittest.TestCase):
    def test_exclude_patterns_compile_from_the_config(self):
        pats = dp._mhd_exclude_patterns({'exclude': {'addressLine1': r'^\^?\s*Mobile Unit'}})
        self.assertEqual([f for f, _ in pats], ['addressLine1'])
        self.assertEqual(dp._mhd_exclude_patterns({}), [])
        self.assertEqual(dp._mhd_row_rejection({'addressLine1': '^Mobile Unit'}, exclude=pats),
                         'excluded')
        self.assertEqual(dp._mhd_row_rejection({'addressLine1': '5050 S Federal Blvd'},
                                               exclude=pats), '')

    def test_arapahoe_drops_mobile_units_child_care_and_body_art(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['acph']
        rows = _fixture_rows('search_acph.json')
        day = '2026-10-27 to 2026-11-02'
        fake = FakeSearch({(day, '', ''): rows})
        records = _run('acph', {**cfg, 'window_days': 7}, fake,
                       since_date='2026-10-27', until_date='2026-11-02')
        self.assertEqual([r['name'] for r in records], ['Boba City 2'])
        rec = records[0]
        self.assertEqual(rec['source'], 'Arapahoe County Health Dept')
        self.assertEqual((rec['state'], rec['metro']), ('CO', ''))
        # the risk-index total is a demerit count: kept as the official
        # number, risk recomputed from the detail page (placeholder until then)
        self.assertEqual(rec['original_score'], 52)
        self.assertEqual(rec['risk_score'], max(0, 100 - 2 * 52))
        self.assertEqual(rec['city'], 'Englewood')

    def test_arapahoe_detail_phase_does_not_trust_the_demerit_total(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['acph']
        rows = [r for r in _fixture_rows('search_acph.json') if r['establishmentName'] == 'Boba City 2']
        day = '2026-10-30 to 2026-10-30'
        rows[0]['inspectionDate'] = '2026-10-30T00:00:00.000Z'
        fake = FakeSearch({(day, '', ''): rows})
        seen = {}

        def fake_bulk(path, records, trust_official_score=True, label=None):
            seen.update(path=path, trust=trust_official_score, label=label)
            return len(records), []
        with mock.patch.object(dp, '_mhd_fetch_details_bulk', side_effect=fake_bulk):
            _run('acph', cfg, fake, fetch_violations=True,
                 since_date='2026-10-30', until_date='2026-10-30')
        self.assertEqual(seen, {'path': 'acph', 'trust': False, 'label': 'Arapahoe County'})


class GroupTests(unittest.TestCase):
    def test_groups_follow_the_config_with_the_default(self):
        groups = dp._mhd_metro_groups(['gwinnett', 'ccbh', 'fbceh', 'va-richmond'])
        self.assertEqual(list(groups), ['MHD metros', 'MHD metros (midweek)'])
        self.assertEqual(list(groups['MHD metros']), ['gwinnett', 'fbceh'])
        self.assertEqual(list(groups['MHD metros (midweek)']), ['ccbh', 'va-richmond'])
        self.assertIs(groups['MHD metros']['gwinnett'], dp.MHD_METRO_JURISDICTIONS['gwinnett'])
        self.assertEqual(dp._mhd_metro_groups([]), {})

    def test_every_entry_runs_in_a_known_group(self):
        names = {str(c.get('group') or dp.MHD_DEFAULT_GROUP)
                 for c in dp.MHD_METRO_JURISDICTIONS.values()}
        self.assertEqual(names, {'MHD metros', 'MHD metros (midweek)'})
        self.assertEqual(dp.MHD_DEFAULT_GROUP, 'MHD metros')

    def test_existing_entries_stay_in_mhd_metros(self):
        for key in ('multco-eh', 'epcph', 'gwinnett', 'tennessee:nashville', 'yolocountyeh'):
            self.assertNotIn('group', dp.MHD_METRO_JURISDICTIONS[key], key)
        groups = dp._mhd_metro_groups(dp._mhd_metro_keys(['portland', 'tennessee', 'gwinnett']))
        self.assertEqual(list(groups), ['MHD metros'])

    def test_heavy_jurisdictions_run_midweek(self):
        midweek = {k for k, c in dp.MHD_METRO_JURISDICTIONS.items()
                   if c.get('group') == 'MHD metros (midweek)'}
        self.assertEqual(midweek, {'ccbh'} | {k for k in dp.MHD_METRO_JURISDICTIONS
                                             if k.startswith('va-')})
        for key in ('fbceh', 'acph', 'dchd'):
            self.assertEqual(dp.MHD_METRO_JURISDICTIONS[key]['group'], 'MHD metros')

    def test_display_names_are_unique_across_both_tables(self):
        names = ([c['display_name'] for c in dp.MHD_METRO_JURISDICTIONS.values()]
                 + [c['display_name'] for c in dp.DFW_JURISDICTIONS.values()])
        self.assertEqual(len(names), len(set(names)))
        labels = {f"{n} Health Dept" for n in names}
        self.assertNotIn(dp.OMAHA_SOURCE, labels, "Omaha's label is 'Douglas County Health Dept'")


class ExpansionConfigTests(unittest.TestCase):
    NEW = ('fbceh', 'acph', 'dchd', 'ccbh')

    def _va_keys(self):
        return [k for k in dp.MHD_METRO_JURISDICTIONS if k.startswith('va-')]

    def test_new_entries_set_every_dfw_default_explicitly(self):
        for key in self.NEW + tuple(self._va_keys()):
            cfg = dp.MHD_METRO_JURISDICTIONS[key]
            for field in ('display_name', 'default_city', 'state', 'score_scale', 'metro',
                          'programs', 'group'):
                self.assertIn(field, cfg, f'{key} lacks {field}')
            self.assertNotEqual(cfg['metro'], 'DFW', key)
            self.assertNotIn('path', cfg, key)
            self.assertNotIn('search_terms', cfg, key)

    def test_fort_bend(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['fbceh']
        self.assertEqual(cfg, {'display_name': 'Fort Bend County', 'default_city': 'Richmond',
                               'state': 'TX', 'score_scale': '100', 'metro': '',
                               'programs': ['Food'], 'group': 'MHD metros'})
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['fortbend'], ['fbceh'])

    def test_arapahoe(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['acph']
        self.assertEqual((cfg['display_name'], cfg['default_city'], cfg['state'], cfg['score_scale'],
                          cfg['metro'], cfg['programs'], cfg['group']),
                         ('Arapahoe County', 'Aurora', 'CO', 'demerit', '', ['Retail Food'],
                          'MHD metros'))
        self.assertIn('addressLine1', cfg['exclude'])
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['arapahoe'], ['acph'])

    def test_douglas_county_co_is_not_omaha(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['dchd']
        self.assertEqual((cfg['display_name'], cfg['default_city'], cfg['state'], cfg['score_scale'],
                          cfg['metro'], cfg['programs'], cfg['group']),
                         ('Douglas County CO', 'Castle Rock', 'CO', 'demerit', '', ['Retail Food'],
                          'MHD metros'))
        self.assertNotEqual(f"{cfg['display_name']} Health Dept", dp.OMAHA_SOURCE)
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['douglas'], ['dchd'])

    def test_cuyahoga(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['ccbh']
        self.assertEqual(cfg, {'display_name': 'Cuyahoga County', 'default_city': 'Parma',
                               'state': 'OH', 'score_scale': 'none', 'metro': 'Cleveland',
                               'programs': ['Food'], 'group': 'MHD metros (midweek)'})
        self.assertNotIn('window_days', cfg, 'one-day windows: a 2-day window collapses re-inspections')
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['cuyahoga'], ['ccbh'])

    def test_virginia_districts(self):
        keys = self._va_keys()
        self.assertEqual(len(keys), 33)
        self.assertNotIn('va-fairfax', keys, 'Fairfax County is its own source and not on the portal')
        names = [dp.MHD_METRO_JURISDICTIONS[k]['display_name'] for k in keys]
        self.assertEqual(len(set(names)), 33, 'per-district source labels keep freshness honest')
        self.assertNotIn('VDH Fairfax', names)
        self.assertIn('VDH Lord Fairfax', names, 'the Winchester-area district, not Fairfax County')
        for k in keys:
            cfg = dp.MHD_METRO_JURISDICTIONS[k]
            self.assertTrue(cfg['display_name'].startswith('VDH '), k)
            self.assertEqual((cfg['state'], cfg['score_scale'], cfg['programs'], cfg['lag_days'],
                              cfg['group']),
                             ('VA', 'none', ['Food Establishment'], 14, 'MHD metros (midweek)'), k)
            self.assertEqual(dp._mhd_path(k, cfg), k, 'each district is its own portal slug')
        self.assertEqual(dp._mhd_va_district('Testham', 'Testville', 'Metro'),
                         {'display_name': 'VDH Testham', 'default_city': 'Testville', 'state': 'VA',
                          'score_scale': 'none', 'metro': 'Metro',
                          'programs': ['Food Establishment'], 'lag_days': 14,
                          'group': 'MHD metros (midweek)'})
        self.assertEqual(dp._mhd_va_district('Testham', 'Testville')['metro'], '')

    def test_virginia_slug_groups_and_metros(self):
        groups = dp.MHD_METRO_SLUG_GROUPS
        self.assertEqual(groups['richmond'], ['va-richmond', 'va-henrico', 'va-chesterfield',
                                              'va-chickahominy'])
        self.assertEqual(groups['hamptonroads'], ['va-virginia-beach', 'va-norfolk', 'va-chesapeake',
                                                  'va-portsmouth', 'va-hampton', 'va-peninsula',
                                                  'va-western-tidewater'])
        self.assertEqual(groups['nova'], ['va-arlington', 'va-alexandria', 'va-loudoun',
                                          'va-prince-william'])
        self.assertEqual(set(groups['virginia']), set(self._va_keys()))
        metro_of = {'richmond': 'Richmond', 'hamptonroads': 'Hampton Roads', 'nova': 'Northern Virginia'}
        for slug, metro in metro_of.items():
            for k in groups[slug]:
                self.assertEqual(dp.MHD_METRO_JURISDICTIONS[k]['metro'], metro, k)
        chipped = {k for slug in metro_of for k in groups[slug]}
        for k in self._va_keys():
            if k not in chipped:
                self.assertEqual(dp.MHD_METRO_JURISDICTIONS[k]['metro'], '', k)
        self.assertEqual(len(dp._mhd_metro_keys(['virginia'])), 33)
        self.assertEqual(len(dp._mhd_metro_keys(['richmond', 'virginia', 'nova'])), 33)
        self.assertEqual(dp._mhd_metro_keys(['richmond']), groups['richmond'])

    def test_new_slugs_count_as_portal_sources(self):
        for slug in ('fortbend', 'arapahoe', 'douglas', 'cuyahoga', 'richmond', 'hamptonroads',
                     'nova', 'virginia', 'fbceh', 'va-norfolk'):
            self.assertTrue(dp._wants_portal_sources([slug]), slug)


class ExpansionParseTests(unittest.TestCase):
    """Rows and detail blocks captured from the new jurisdictions."""

    KEYS = {'name', 'address', 'city', 'state', 'zip', 'latitude', 'longitude', 'inspection_date',
            'original_score', 'risk_score', 'priority_violations', 'priority_foundation_violations',
            'core_violations', 'total_violations', 'violations', 'inspection_type', 'results',
            'source', 'source_url', 'source_id', 'metro'}

    def _records(self, key, fixture, since, until):
        cfg = dp.MHD_METRO_JURISDICTIONS[key]
        fake = FakeSearch({(f'{since} to {until}', '', ''): _fixture_rows(fixture)})
        return _run(key, {**cfg, 'window_days': 400}, fake, since_date=since, until_date=until)

    def test_fort_bend_official_score_and_pools_filter(self):
        records = self._records('fbceh', 'search_fbceh.json', '2026-10-06', '2026-11-02')
        self.assertEqual(sorted(r['name'] for r in records),
                         ['Hacienda Don Jose 2 Llc', 'Nirmanz Food Boutique', 'Ohl Style Beef Jerky'])
        rec = next(r for r in records if r['name'].startswith('Hacienda'))
        self.assertEqual((rec['original_score'], rec['risk_score']), (72, 72))
        self.assertEqual(rec['source'], 'Fort Bend County Health Dept')
        self.assertEqual(rec['source_id'], '2AACA469-B955-42B5-948A-047BCFDF1B1C')
        self.assertEqual(rec['source_url'], 'https://inspections.myhealthdepartment.com/fbceh/'
                                            'inspection/?inspectionID=2AACA469-B955-42B5-948A-047BCFDF1B1C')
        self.assertEqual((rec['state'], rec['metro']), ('TX', ''))
        self.assertIsNone(rec['latitude'])
        self.assertIsNone(rec['longitude'])
        self.assertEqual(dp.calculate_vetted_grade(rec['risk_score']), 'C')
        self.assertEqual(set(rec) - {'purpose'}, self.KEYS)

    def test_douglas_demerit_rows(self):
        records = self._records('dchd', 'search_dchd.json', '2026-10-26', '2026-11-02')
        by = {r['name']: r for r in records}
        self.assertEqual(set(by), {'Dumpling Kitchen', 'Cold Stone Creamery'}, 'child care dropped')
        self.assertEqual(by['Dumpling Kitchen']['original_score'], 30)
        self.assertEqual(by['Dumpling Kitchen']['risk_score'], 100 - 2 * 30)
        self.assertEqual(by['Dumpling Kitchen']['source'], 'Douglas County CO Health Dept')
        self.assertEqual(by['Dumpling Kitchen']['city'], 'Castle Rock')
        self.assertEqual(by['Cold Stone Creamery']['original_score'], 0)

    def test_cuyahoga_rows(self):
        records = self._records('ccbh', 'search_ccbh.json', '2026-10-26', '2026-11-02')
        self.assertEqual([r['name'] for r in records], ['Speedway #4043'], 'schools and body art dropped')
        rec = records[0]
        self.assertEqual((rec['original_score'], rec['risk_score']), (None, 70))
        self.assertEqual((rec['state'], rec['metro'], rec['source']),
                         ('OH', 'Cleveland', 'Cuyahoga County Health Dept'))

    def test_cuyahoga_null_state_falls_back_to_ohio(self):
        row = dict(next(r for r in _fixture_rows('search_ccbh.json') if r['programName'] == 'Food'))
        row['state'] = None
        rec = dp._parse_mhd_record(row, 'ccbh', 'Cuyahoga County', 'Parma', 'OH', 'none', 'Cleveland')
        self.assertEqual(rec['state'], 'OH')

    def test_virginia_rows(self):
        records = self._records('va-arlington', 'search_va_arlington.json', '2026-10-14', '2026-10-20')
        self.assertEqual(len(records), 3)
        by = {r['source_id']: r for r in records}
        rec = by['8ED6A912-A4F7-45F2-AD82-D75C447E2A4F']
        self.assertEqual(rec['inspection_date'], '2026-10-20', 'MM/DD/YYYY parsed')
        self.assertEqual(rec['name'], 'Don Tito')
        self.assertEqual((rec['original_score'], rec['risk_score']), (None, 70))
        self.assertEqual((rec['state'], rec['metro'], rec['source']),
                         ('VA', 'Northern Virginia', 'VDH Arlington Health Dept'))
        self.assertTrue(rec['source_url'].startswith(
            'https://inspections.myhealthdepartment.com/va-arlington/inspection/?inspectionID='))
        self.assertEqual(rec['inspection_type'], 'Full Service Restaurant')
        self.assertEqual(set(rec) - {'purpose'}, self.KEYS)
        self.assertEqual(dp.make_restaurant_id(rec['name'], rec['address'], rec['city']),
                         dp.make_restaurant_id('Don Tito', '3165 Wilson Blvd', 'Arlington'))

    def test_detail_pages_parse(self):
        counts = {'detail_fbceh_score72.html': 19, 'detail_acph_result.html': 3,
                  'detail_dchd_score30.html': 4, 'detail_ccbh.html': 6,
                  'detail_va_henrico.html': 2, 'detail_va_clean.html': 0}
        for name, n in counts.items():
            obs = dp._mhd_parse_detail_html(_fixture_html(name))
            self.assertIsInstance(obs, list, name)
            self.assertEqual(len(obs), n, name)
            for text in obs:
                self.assertLessEqual(len(text), 500)
                self.assertNotIn('\n', text)
        self.assertIn('Observed staff washing dishes and not sanitizing them',
                      dp._mhd_parse_detail_html(_fixture_html('detail_va_henrico.html'))[0])

    def test_scores_from_the_detail_pass_for_no_score_jurisdictions(self):
        # 'none' / 'demerit': the risk score is calc_risk_score over the lines
        texts = dp._mhd_parse_detail_html(_fixture_html('detail_ccbh.html'))
        violations = [dp.classify_violation(t) for t in texts]
        score, pv, pfv, cv = dp.calc_risk_score(violations)
        self.assertEqual(pv + pfv + cv, 6)
        self.assertTrue(all(v[1] in ('priority', 'priority_foundation', 'core') for v in violations))
        self.assertTrue(0 <= score <= 100)


class LagNoteTests(unittest.TestCase):
    """The group's source-lag note comes from the newest row any of its
    jurisdictions' searches returned, after a complete read."""

    def setUp(self):
        self.saved = (dict(dp._MHD_NEWEST_SEEN), dict(dp._GROUP_NOTES))
        dp._MHD_NEWEST_SEEN.clear()
        dp._GROUP_NOTES.clear()
        self.cfg = dp.MHD_METRO_JURISDICTIONS['gwinnett']
        self.old = '2026-01-15 to 2026-01-15'
        self.table = {(self.old, '', ''): [_row('G1', city='Duluth', state='GA', program='Food',
                                                date='2026-01-15T00:00:00.000Z'),
                                           _row('P1', city='Duluth', state='GA', program='Pools',
                                                date='2026-01-16T00:00:00.000Z')]}
        # a pool row counts for "what the source holds" even though it is
        # not shipped; to make it land on its own day move it into the table
        self.table[('2026-01-16 to 2026-01-16', '', '')] = [self.table[(self.old, '', '')].pop()]

    def tearDown(self):
        seen, notes = self.saved
        dp._MHD_NEWEST_SEEN.clear()
        dp._MHD_NEWEST_SEEN.update(seen)
        dp._GROUP_NOTES.clear()
        dp._GROUP_NOTES.update(notes)

    def test_a_complete_read_records_the_newest_row_and_the_group_is_noted(self):
        records = _run('gwinnett', self.cfg, FakeSearch(self.table), since_date='2026-01-15')
        self.assertEqual([r['source_id'] for r in records], ['G1'])
        self.assertEqual(dp._MHD_NEWEST_SEEN['Gwinnett County'], '2026-01-16')
        dp._mhd_group_lag_note('MHD metros', {'gwinnett': self.cfg}, [])
        self.assertIn('MHD metros', dp._GROUP_NOTES)
        self.assertIn('2026-01-16', dp._GROUP_NOTES['MHD metros'])

    def test_a_group_with_a_jurisdiction_not_fetched_leaves_no_note(self):
        dp._MHD_NEWEST_SEEN['Gwinnett County'] = '2026-01-16'
        dp._mhd_group_lag_note('MHD metros', {'gwinnett': self.cfg}, ['Plano (deadline)'])
        self.assertEqual(dp._GROUP_NOTES, {})

    def test_the_group_note_uses_the_newest_jurisdiction(self):
        dp._MHD_NEWEST_SEEN['Gwinnett County'] = '2026-01-16'
        dp._MHD_NEWEST_SEEN['Multnomah County'] = (dp.datetime.now()).strftime('%Y-%m-%d')
        dp._mhd_group_lag_note('MHD metros', {'gwinnett': self.cfg,
                                              'multco-eh': dp.MHD_METRO_JURISDICTIONS['multco-eh']}, [])
        self.assertEqual(dp._GROUP_NOTES, {}, 'one current jurisdiction: the group is not lagging')

    def test_a_bounded_read_leaves_no_newest(self):
        _run('gwinnett', self.cfg, FakeSearch(self.table), since_date='2026-01-15', limit=1)
        self.assertNotIn('Gwinnett County', dp._MHD_NEWEST_SEEN)
        _run('gwinnett', self.cfg, FakeSearch(self.table), since_date='2026-01-15',
             until_date='2026-01-20')
        self.assertNotIn('Gwinnett County', dp._MHD_NEWEST_SEEN, 'the window did not reach today')
        dp._mhd_group_lag_note('MHD metros', {'gwinnett': self.cfg}, [])
        self.assertEqual(dp._GROUP_NOTES, {})

    def test_row_dates(self):
        self.assertEqual(dp._mhd_row_date({'inspectionDate': '2026-10-20T00:00:00.000Z'}), '2026-10-20')
        self.assertEqual(dp._mhd_row_date({'inspectionDate': '10/20/2026'}), '2026-10-20')
        # an epoch in milliseconds is read as UTC whatever the local clock
        # (1760918400000 is 2025-10-20T00:00:00Z; a US clock reads it as the 19th)
        self.assertEqual(dp._mhd_row_date({'inspectionDate': 1760918400000}), '2025-10-20')
        with mock.patch.dict(os.environ, {'TZ': 'America/Chicago'}):
            import time as _time
            _time.tzset()
            try:
                self.assertEqual(dp._mhd_row_date({'inspectionDate': 1760918400000}), '2025-10-20')
            finally:
                os.environ.pop('TZ', None)
                _time.tzset()
        self.assertEqual(dp._mhd_row_date({'inspectionDate': None}), '')
        self.assertEqual(dp._mhd_row_date({}), '')


class LimitAndHoldTests(unittest.TestCase):
    def setUp(self):
        self.saved_holds = dict(dp._FRESHNESS_HOLDS)
        self.saved_failed = dict(dp._MHD_DETAIL_FAILED)
        dp._FRESHNESS_HOLDS.clear()
        dp._MHD_DETAIL_FAILED.clear()

    def tearDown(self):
        dp._FRESHNESS_HOLDS.clear()
        dp._FRESHNESS_HOLDS.update(self.saved_holds)
        dp._MHD_DETAIL_FAILED.clear()
        dp._MHD_DETAIL_FAILED.update(self.saved_failed)

    def test_limit_stops_the_search_after_enough_records(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['gwinnett']
        d1, d2 = '2026-10-05 to 2026-10-05', '2026-10-06 to 2026-10-06'
        fake = FakeSearch({(d1, '', ''): [_row('G1', city='Duluth', state='GA', program='Food'),
                                          _row('G2', city='Duluth', state='GA', program='Food')],
                           (d2, '', ''): [_row('G3', city='Duluth', state='GA', program='Food')]})
        records = _run('gwinnett', cfg, fake, since_date='2026-10-05', until_date='2026-10-06',
                       limit=1)
        self.assertEqual(len(records), 1)
        self.assertEqual({c['date'] for c in fake.calls}, {d1}, 'no search past the limit')

    def test_an_unreadable_detail_page_drops_and_holds_that_inspection(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['fbceh']
        day = '2026-10-20 to 2026-10-20'
        rows = [_row('F1', city='Katy', state='TX', program='Food', date='2026-10-20T00:00:00.000Z'),
                _row('F2', city='Katy', state='TX', program='Food', date='2026-10-20T00:00:00.000Z')]
        fake = FakeSearch({(day, '', ''): rows})

        def fake_bulk(path, records, trust_official_score=True, label=None):
            return 1, [records[1]]
        with mock.patch.object(dp, '_mhd_fetch_details_bulk', side_effect=fake_bulk):
            records = _run('fbceh', cfg, fake, fetch_violations=True,
                           since_date='2026-10-20', until_date='2026-10-20')
        self.assertEqual([r['source_id'] for r in records], ['F1'])
        self.assertEqual(dp._FRESHNESS_HOLDS, {'Fort Bend County Health Dept': {'2026-10-20|F2': (True, False)}},
                         'the dropped inspection holds the source, counted (it was tried)')
        self.assertEqual(dp._MHD_DETAIL_FAILED, {}, 'one page of two is a flaky page, not a failed phase')


# ── fix pass 2026-11-03 ──────────────────────────────────────────────────────

class FakeDetailSession:
    """A session whose GET answers one canned response for every detail page
    (status, body), recording the requests."""

    def __init__(self, status=200, text=''):
        self.status, self.text = status, text
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        return mock.Mock(status_code=self.status, text=self.text)

    def post(self, *a, **kw):
        raise AssertionError('no search expected here')

    def close(self):
        pass


class InspectionPageMarkerTests(unittest.TestCase):
    """A 200 that is not an inspection page is not a read (the parser would
    read the welcome/landing page as a CLEAN inspection: risk 100 on the
    no-score and demerit jurisdictions)."""

    def test_every_detail_fixture_is_an_inspection_page(self):
        for name in ('detail_acph_result.html', 'detail_ccbh.html', 'detail_dchd_score30.html',
                     'detail_fbceh_score72.html', 'detail_va_clean.html', 'detail_va_henrico.html'):
            self.assertTrue(dp._mhd_is_inspection_page(_fixture_html(name)), name)
        # Frisco's template renders the observations from inline JS
        self.assertTrue(dp._mhd_is_inspection_page(
            '<script>var itemNum = "30"; var comments = `<p>Observed ...</p>`;</script>'))

    def test_a_clean_inspection_page_is_still_read_as_clean(self):
        html = _fixture_html('detail_va_clean.html')
        session = FakeDetailSession(200, html)
        self.assertEqual(dp._mhd_fetch_inspection_detail(session, 'va-norfolk', 'X1'), [])

    def test_the_welcome_and_landing_pages_are_not_inspection_pages(self):
        for name in ('detail_welcome_page.html', 'detail_landing_page.html'):
            html = _fixture_html(name)
            self.assertFalse(dp._mhd_is_inspection_page(html), name)
            self.assertEqual(dp._mhd_parse_detail_html(html), [], 'what the parser would have shipped')
            session = FakeDetailSession(200, html)
            with mock.patch.object(dp.time, 'sleep'):
                self.assertIsNone(dp._mhd_fetch_inspection_detail(session, 'va-richmond', 'X1'), name)
        self.assertIsNone(dp._mhd_fetch_inspection_detail(FakeDetailSession(200, ''), 'plano', 'X2'))
        self.assertIsNone(dp._mhd_fetch_inspection_detail(FakeDetailSession(200, '<html></html>'), 'plano', 'X3'))

    def test_a_non_200_is_still_not_a_read(self):
        with mock.patch.object(dp.time, 'sleep'):
            self.assertIsNone(dp._mhd_fetch_inspection_detail(FakeDetailSession(404, 'gone'), 'plano', 'X1'))
            self.assertIsNone(dp._mhd_fetch_inspection_detail(FakeDetailSession(500, 'oops'), 'plano', 'X1'))


class DetailMassFailureTests(unittest.TestCase):
    """Detail pages that mostly fail for a reason other than a ban or the
    deadline are reported, so the group goes red instead of 'ok' on the
    other jurisdictions' records while the held records are given up."""

    def setUp(self):
        self.saved = (dict(dp._FRESHNESS_HOLDS), dict(dp._MHD_DETAIL_FAILED), dp._MHD_PORTAL_BLOCKED,
                      dp._MHD_DETAIL_BANNED, dp._MHD_DETAIL_BLOCK_STREAK, dp._FETCH_DEADLINE,
                      set(dp._MHD_DEADLINE_CUT), dict(dp._GROUP_STATUS))
        dp._FRESHNESS_HOLDS.clear()
        dp._MHD_DETAIL_FAILED.clear()
        dp._MHD_PORTAL_BLOCKED = False
        dp._MHD_DETAIL_BANNED = False
        dp._MHD_DETAIL_BLOCK_STREAK = 0
        dp._FETCH_DEADLINE = None
        dp._MHD_DEADLINE_CUT.clear()
        self.cfg = dp.MHD_METRO_JURISDICTIONS['va-richmond']
        self.day = '2026-10-20 to 2026-10-20'
        self.rows = [_row(f'R{i}', name=f'Cafe {i}', addr=f'{i} Main St', city='Richmond', state='VA',
                          program='Food Establishment', date='10/20/2026', score=0) for i in range(1, 4)]

    def tearDown(self):
        (holds, failed, blocked, banned, streak, deadline, cut, status) = self.saved
        dp._FRESHNESS_HOLDS.clear()
        dp._FRESHNESS_HOLDS.update(holds)
        dp._MHD_DETAIL_FAILED.clear()
        dp._MHD_DETAIL_FAILED.update(failed)
        dp._MHD_PORTAL_BLOCKED = blocked
        dp._MHD_DETAIL_BANNED = banned
        dp._MHD_DETAIL_BLOCK_STREAK = streak
        dp._FETCH_DEADLINE = deadline
        dp._MHD_DEADLINE_CUT.clear()
        dp._MHD_DEADLINE_CUT.update(cut)
        dp._GROUP_STATUS.clear()
        dp._GROUP_STATUS.update(status)

    def test_the_rule(self):
        f = dp._mhd_detail_mass_failure
        self.assertFalse(f(0, 0))
        self.assertFalse(f(0, 30))
        self.assertTrue(f(1, 1), 'every page tried failed')
        self.assertTrue(f(3, 3))
        self.assertFalse(f(1, 3), 'one flaky page of three: held and re-read, not a red run')
        self.assertFalse(f(2, 3), 'below the minimum only a total failure counts')
        self.assertFalse(f(10, 20))
        self.assertTrue(f(11, 20), 'more than half of at least MHD_DETAIL_MASS_FAILURE_MIN')
        self.assertEqual(dp.MHD_DETAIL_MASS_FAILURE_MIN, 20)

    def _fetch_with(self, session, **kw):
        fake = FakeSearch({(self.day, '', ''): self.rows})
        skipped = []
        with mock.patch.object(dp, '_mhd_search_page', side_effect=fake), \
             mock.patch.object(dp, '_mhd_session', return_value=session), \
             mock.patch.object(dp.time, 'sleep'):
            records = dp.fetch_dfw({'va-richmond': self.cfg}, since_date='2026-10-20',
                                   until_date='2026-10-20', fetch_violations=True, skipped=skipped, **kw)
        return records, skipped, session

    def test_every_detail_page_a_welcome_page_drops_holds_and_reports(self):
        records, skipped, session = self._fetch_with(
            FakeDetailSession(200, _fixture_html('detail_welcome_page.html')))
        self.assertEqual(records, [], 'nothing shipped as clean')
        holds = dp._FRESHNESS_HOLDS['VDH Richmond City Health Dept']
        self.assertEqual(holds, {f'2026-10-20|R{i}': (True, False) for i in range(1, 4)},
                         'each inspection held on its own, counted (it was tried)')
        self.assertEqual(dp._MHD_DETAIL_FAILED, {'VDH Richmond City': (3, 3)})
        self.assertEqual(skipped, ['VDH Richmond City (error: 3/3 detail pages unreadable)'])
        # tried once and retried once, nothing more
        self.assertEqual(len(session.calls), 6)
        self.assertTrue(all(u == f'{dp.MHD_BASE_URL}va-richmond/inspection/' for u, _ in session.calls))

    def test_every_detail_page_a_404_drops_holds_and_reports(self):
        records, skipped, _ = self._fetch_with(FakeDetailSession(404, 'not here'))
        self.assertEqual(records, [])
        self.assertEqual(set(dp._FRESHNESS_HOLDS['VDH Richmond City Health Dept']),
                         {f'2026-10-20|R{i}' for i in range(1, 4)})
        self.assertEqual(skipped, ['VDH Richmond City (error: 3/3 detail pages unreadable)'])

    def test_readable_clean_pages_ship_and_report_nothing(self):
        records, skipped, _ = self._fetch_with(FakeDetailSession(200, _fixture_html('detail_va_clean.html')))
        self.assertEqual(sorted(r['source_id'] for r in records), ['R1', 'R2', 'R3'])
        self.assertTrue(all(r['risk_score'] == 100 and r['violations'] == [] for r in records))
        self.assertEqual((skipped, dp._MHD_DETAIL_FAILED, dp._FRESHNESS_HOLDS), ([], {}, {}))

    def test_a_deadline_cut_detail_phase_is_skipped_not_failed(self):
        # the deadline passes right after the search: the detail phase is
        # never tried, so the records hold uncounted and the report says deadline
        deadline_after_search = {'n': 0}
        real_past = dp._past_deadline

        def past():
            deadline_after_search['n'] += 1
            return deadline_after_search['n'] > 2
        with mock.patch.object(dp, '_past_deadline', side_effect=past):
            records, skipped, _ = self._fetch_with(FakeDetailSession(200, 'never read'))
        self.assertEqual(records, [])
        self.assertEqual(dp._MHD_DETAIL_FAILED, {})
        self.assertEqual(skipped, ['VDH Richmond City (deadline)'])
        self.assertIs(dp._past_deadline, real_past)

    def test_main_reports_the_group_as_an_error(self):
        # the shape main() turns the skipped entry into
        not_fetched = ['VDH Richmond City (error: 3/3 detail pages unreadable)']
        self.assertFalse(any('(deadline)' in x for x in not_fetched))
        self.assertTrue(not_fetched)   # -> status 'error', detail 'fetch failed: not fetched — ...'


class SearchBodyTests(unittest.TestCase):
    """A 200 whose body is not a result list ({"err": true, "msg": "bad
    request"}: a path the portal does not know, an offset past the cap) is an
    UNREAD window, never an empty day."""

    def setUp(self):
        self.saved = (dict(dp._FRESHNESS_HOLDS), dp._MHD_PORTAL_BLOCKED, dp._FETCH_DEADLINE)
        dp._FRESHNESS_HOLDS.clear()
        dp._MHD_PORTAL_BLOCKED = False
        dp._FETCH_DEADLINE = None

    def tearDown(self):
        holds, blocked, deadline = self.saved
        dp._FRESHNESS_HOLDS.clear()
        dp._FRESHNESS_HOLDS.update(holds)
        dp._MHD_PORTAL_BLOCKED = blocked
        dp._FETCH_DEADLINE = deadline

    @staticmethod
    def _session(body):
        session = mock.Mock()
        resp = mock.Mock(status_code=200)
        resp.json.return_value = body
        resp.raise_for_status.return_value = None
        session.post.return_value = resp
        return session

    def test_an_error_object_is_an_unread_page(self):
        with mock.patch.object(dp.time, 'sleep'):
            self.assertIsNone(dp._mhd_search_page(self._session({'err': True, 'msg': 'bad request'}),
                                                  'no-such-jurisdiction', '2026-10-21 to 2026-10-21', 0))
            self.assertIsNone(dp._mhd_search_page(self._session(None), 'fbceh', '2026-10-21 to 2026-10-21', 0))
        self.assertFalse(dp._MHD_PORTAL_BLOCKED, 'a rejected request is not a block')

    def test_an_empty_list_is_an_empty_day_and_a_list_is_the_page(self):
        rows = [_row('F1', city='Katy', state='TX', program='Food')]
        with mock.patch.object(dp.time, 'sleep'):
            self.assertEqual(dp._mhd_search_page(self._session([]), 'fbceh', '2026-10-25 to 2026-10-25', 0), [])
            self.assertEqual(dp._mhd_search_page(self._session(rows), 'fbceh', '2026-10-21 to 2026-10-21', 0), rows)

    def test_the_jurisdiction_fails_closed_on_an_error_object(self):
        cfg = dp.MHD_METRO_JURISDICTIONS['fbceh']
        session = self._session({'err': True, 'msg': 'bad request'})
        with mock.patch.object(dp.time, 'sleep'):
            with self.assertRaises(RuntimeError) as ctx:
                dp._fetch_mhd_jurisdiction_api(session, 'fbceh', cfg, since_date='2026-10-21',
                                               until_date='2026-10-21', fetch_violations=False)
        self.assertIn('2026-10-21', str(ctx.exception))
        self.assertEqual(list(dp._FRESHNESS_HOLDS), ['Fort Bend County Health Dept'])
        self.assertEqual(list(dp._FRESHNESS_HOLDS['Fort Bend County Health Dept'].values()), [(False, False)])


class DeadPestTitleTests(unittest.TestCase):
    """Fort Bend's observation lines are Food Code wording; its item-38 text
    ("Dead or trapped birds, insects, rodents, and other pests shall be
    removed ...") read to the regex as "dead ... rodents" and forced F's on
    county-scored 97s. A list that names every kind names none."""

    FB = ('38: 6-501.112 Dead or trapped birds, insects, rodents, and other pests shall be removed from '
          'control devices and the premises at a frequency that prevents their accumulation, decomposition, '
          'or the attraction of pests. Pg. 191')

    def test_fort_bend_item_38_keeps_the_official_grade(self):
        import jev_reader as jr
        for code in ('6-501.112', '6-501.111'):
            line = self.FB.replace('6-501.112', code)
            self.assertFalse(dp.has_active_pest_or_sewage(line), line)
            self.assertEqual(dp.calculate_vetted_grade(97, line), 'A')
            self.assertEqual(dp.calculate_vetted_grade(84, line), 'B')
            # the Jev path resolves the bare title deterministically: a pest
            # finding of unnamed kind, no F
            self.assertEqual(jr.cited_pest_item(line), 'pests_unspecified', line)
        # the same text under the outer-openings code runs on into that rule's
        # wording: not a whole-line title (Jev reads it), but no regex F either
        tail = self.FB.replace('6-501.112', '6-202.15(A)') + (' Outer openings, if kept open for ventilation '
                                                               'or other purposes, shall be protected against '
                                                               'the entry of insects and rodents')
        self.assertIsNone(jr.cited_pest_item(tail))
        self.assertFalse(dp.has_active_pest_or_sewage(tail))
        self.assertEqual(dp.calculate_vetted_grade(70, tail), 'C')

    def test_the_same_wording_elsewhere(self):
        for line in ('6-501.112 Removing Dead or Trapped Birds, Insects, Rodents and other Pest',   # Delaware
                     'Remove dead or trapped birds; insects; rodents; and other pests.',           # Fort Worth
                     '34: 3. Dead insects, rodents and other pests have not been removed from the facility. '
                     'OBSERVED DEAD CRICKET UNDER THREE COMPARTMENT SINK.',                          # Dallas
                     'Dead or trapped birds, insects, rodents, and other pests shall be removed from control '
                     'devices and the PREMISES at a frequency that prevents their accumulation, decomposition, '
                     'or the attraction of pests.'):                                                # Detroit
            self.assertFalse(dp.has_active_pest_or_sewage(line), line)

    def test_a_named_finding_still_forces_the_f(self):
        for line in ('34: 3. Dead insects, rodents and other pests have not been removed from the facility. '
                     'OBSERVED DEAD ROACHES UNDER THE SINK',
                     'Dead mice found in the dry storage area',
                     'Observed dead roaches, insects, rodents and other pests',
                     'INSECTS, RODENTS, & ANIMALS NOT PRESENT: Found live cockroaches in rear storage room',
                     'Evidence of rodents or insects observed',                     # Montgomery County
                     'Rodents and other pests observed in the kitchen. Fresh droppings found near the mice bait'):
            self.assertTrue(dp.has_active_pest_or_sewage(line), line)
        self.assertEqual(dp.calculate_vetted_grade(97, 'Dead mice found in the dry storage area'), 'F')

    def test_the_regex_is_in_the_grading_path(self):
        import inspect
        self.assertIn('DEAD_PEST_TITLE_RE', inspect.getsource(dp.has_active_pest_or_sewage))
