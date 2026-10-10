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
                 search_str='', purpose=''):
        self.calls.append({'path': path, 'date': date_range_str, 'start': start_offset,
                           'search_str': search_str, 'purpose': purpose})
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

        def fake(session, path, date_range, start, retry_count=0, search_str='', purpose=''):
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
        resp = mock.Mock(status_code=200, text='<html></html>')
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
