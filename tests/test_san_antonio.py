"""Tests for the San Antonio (Metro Health, Tyler healthinspections.us)
fetcher: search-page parsing and pagination, the 100-facility cap and its
per-zip fallback, estab-page parsing (three violation layouts), the record
dict, id stability and the safety contract — an unreadable estab page never
ships as a clean inspection. No network: _tyler_request is mocked with
trimmed copies of pages saved from the portal on 2026-10-10."""
import os
import sys
import unittest
from datetime import datetime
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'san_antonio')


def load(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as f:
        return f.read()


class FakeResponse:
    def __init__(self, text):
        self.text = text
        self.status_code = 200


FIT_PREP = ('1777343', '4320270', '210 FIT PREP MEALS', '328 NORTH PARK DR',
            '78216', 'San Antonio', '10/01/2026')
BAANTHAI = ('1788001', '4320251', 'BAANTHAI THAI CUISINE', '9386 HUEBNER RD #101',
            '78240', 'San Antonio', '10/01/2026')


class SearchParserTests(unittest.TestCase):
    def test_first_page_ids_name_street_zip_date(self):
        matched, rows = dp._sa_parse_search(load('search_p1.html'))
        self.assertEqual(matched, 70)
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0], FIT_PREP)
        self.assertIn(BAANTHAI, rows)   # suite number kept on the street line
        self.assertTrue(all(r[6] == '10/01/2026' for r in rows))
        self.assertTrue(all(len(r[4]) == 5 for r in rows))

    def test_second_page_is_the_next_ten(self):
        _, page1 = dp._sa_parse_search(load('search_p1.html'))
        matched, page2 = dp._sa_parse_search(load('search_p2.html'))
        self.assertEqual(matched, 70)
        self.assertEqual(len(page2), 10)
        self.assertFalse({(r[0], r[1]) for r in page1} & {(r[0], r[1]) for r in page2})
        # The city token is sometimes shouted; the fetcher normalises it.
        self.assertIn('SAN ANTONIO', {r[5] for r in page2})

    def test_zip_restricted_page(self):
        matched, rows = dp._sa_parse_search(load('search_zip.html'))
        self.assertEqual((matched, len(rows)), (10, 10))
        self.assertEqual({r[4] for r in rows}, {'78216'})

    def test_cap_detection(self):
        matched, rows = dp._sa_parse_search(load('search_capped.html'))
        self.assertEqual(matched, 100)
        self.assertGreaterEqual(matched, dp.SA_SEARCH_CAP)
        self.assertEqual(len(rows), 10)

    def test_empty_and_non_results_pages(self):
        self.assertEqual(dp._sa_parse_search(load('search_empty.html')), (0, []))
        self.assertEqual(dp._sa_parse_search('<html><body>Service Unavailable</body></html>'),
                         (None, []))

    def test_search_url(self):
        url = dp._sa_search_url(datetime(2026, 10, 1), start=11, zc='78216')
        self.assertIn('sd=10/01/2026&ed=10/01/2026', url)
        self.assertIn('&zc=78216&', url)
        self.assertTrue(url.endswith('&start=11'))
        self.assertIn('dtRng=YES', url)


class SearchDayTests(unittest.TestCase):
    def setUp(self):
        self.sleep = mock.patch.object(dp.time, 'sleep', lambda *_: None)
        self.sleep.start()
        self.addCleanup(self.sleep.stop)
        self.saved_deadline = dp._FETCH_DEADLINE
        dp._FETCH_DEADLINE = None
        self.addCleanup(setattr, dp, '_FETCH_DEADLINE', self.saved_deadline)

    def test_paginates_until_the_count_is_reached(self):
        starts = []

        def fake(session, method, url, **kw):
            start = int(url.rsplit('start=', 1)[1])
            starts.append(start)
            if start == 1:
                return FakeResponse(load('search_p1.html'))
            if start == 11:
                return FakeResponse(load('search_p2.html'))
            return FakeResponse('<html><body><b>70 Facilities matched </b></body></html>')

        with mock.patch.object(dp, '_tyler_request', fake):
            matched, rows = dp._sa_search_day(object(), datetime(2026, 10, 1))
        self.assertEqual(matched, 70)
        self.assertEqual(len(rows), 20)
        self.assertEqual(starts, [1, 11, 21])   # stops on the first page without rows

    def test_single_page_result_makes_one_request(self):
        calls = []
        fake = lambda s, m, url, **kw: (calls.append(url), FakeResponse(load('search_zip.html')))[1]
        with mock.patch.object(dp, '_tyler_request', fake):
            matched, rows = dp._sa_search_day(object(), datetime(2026, 10, 1), zc='78216')
        self.assertEqual((matched, len(rows), len(calls)), (10, 10, 1))
        self.assertIn('zc=78216', calls[0])

    def test_capped_day_is_requeried_per_zip(self):
        urls = []

        def fake(session, method, url, **kw):
            urls.append(url)
            if '&zc=&' in url:
                # the whole-day query: 100 matched, rows listed on page 1 only
                if url.endswith('start=1'):
                    return FakeResponse(load('search_capped.html'))
                return FakeResponse('<html><body><b>100 Facilities matched </b></body></html>')
            return FakeResponse(load('search_zip.html'))

        stats = {'days': 0, 'capped_days': 0, 'recovered_days': 0}
        pool = {'78216', '78240'}
        with mock.patch.object(dp, '_tyler_request', fake):
            rows = dp._sa_search_day_partitioned(object(), datetime(2026, 10, 1), pool, stats)
        zips_queried = sorted(u.split('&zc=')[1].split('&')[0] for u in urls if '&zc=&' not in u)
        # every zip in the pool plus every zip seen in the capped page's rows
        _, capped_rows = dp._sa_parse_search(load('search_capped.html'))
        self.assertEqual(zips_queried, sorted(pool | {r[4] for r in capped_rows}))
        self.assertEqual(stats['capped_days'], 1)
        keys = {(r[0], r[1]) for r in rows}
        self.assertTrue({(r[0], r[1]) for r in capped_rows} <= keys)
        _, zip_rows = dp._sa_parse_search(load('search_zip.html'))
        self.assertTrue({(r[0], r[1]) for r in zip_rows} <= keys)

    def test_uncapped_day_is_not_partitioned(self):
        urls = []
        fake = lambda s, m, url, **kw: (urls.append(url), FakeResponse(load('search_zip.html')))[1]
        stats = {'days': 0, 'capped_days': 0, 'recovered_days': 0}
        with mock.patch.object(dp, '_tyler_request', fake):
            rows = dp._sa_search_day_partitioned(object(), datetime(2026, 10, 1), set(dp.SA_ZIPS), stats)
        self.assertEqual((len(rows), len(urls), stats['capped_days']), (10, 1, 0))

    def test_failed_search_raises(self):
        with mock.patch.object(dp, '_tyler_request', lambda *a, **k: None):
            with self.assertRaises(RuntimeError):
                dp._sa_search_day(object(), datetime(2026, 10, 1))


class EstabParserTests(unittest.TestCase):
    def test_old_layout_rule_text_then_at_inspection(self):
        blocks = dp._sa_parse_estab(load('estab_violations.html'))
        self.assertEqual(len(blocks), 1)
        b = blocks[0]
        self.assertEqual((b['date'], b['score'], len(b['texts'])), ('09/14/2026', 85, 8))
        # finding first, then the Food Code cite and rule text; item number dropped
        self.assertTrue(b['texts'][0].startswith('WITNESSED FOOD PRODUCTION PERSONEL/COOK HANDLE PHONE'))
        self.assertIn('2-301.14 When to wash', b['texts'][0])
        self.assertFalse(b['texts'][0].startswith('14 '))
        self.assertTrue(any('C.O.S.: PERSONNEL PROCEEDED TO ADD THE REQUIRED DATE LABELS' in t
                            for t in b['texts']))
        self.assertTrue(all(len(t) <= 500 for t in b['texts']))
        self.assertTrue(all('\n' not in t for t in b['texts']))

    def test_clean_page_is_a_block_with_no_texts(self):
        self.assertEqual(dp._sa_parse_estab(load('estab_clean.html')),
                         [{'date': '10/01/2026', 'score': 100, 'texts': []}])

    def test_facility_page_lists_history_newest_first(self):
        blocks = dp._sa_parse_estab(load('estab_history.html'))
        self.assertEqual([b['date'] for b in blocks],
                         ['10/01/2026', '04/02/2026', '10/02/2025', '04/02/2025', '10/02/2024'])
        self.assertEqual([b['score'] for b in blocks], [98, 95, 98, 94, 95])
        newest = blocks[0]
        self.assertEqual(len(newest['texts']), 2)
        # "Observation: <finding> FDA Code <cite>: <rule>" layout — label dropped
        self.assertTrue(newest['texts'][0].startswith('Uncovered food containers found in reach-in cooler'))
        self.assertIn('FDA Code 3-302.11(A)(4)', newest['texts'][0])
        # bare "<item> <finding>" layout
        self.assertTrue(blocks[2]['texts'][0].startswith('Buckets of sauce found stored on floor'))

    def test_not_a_facility_page_is_none_not_clean(self):
        self.assertIsNone(dp._sa_parse_estab(load('search_p1.html')))
        self.assertIsNone(dp._sa_parse_estab('Service Unavailable'))
        self.assertIsNone(dp._sa_parse_estab(''))

    def test_missing_score_is_none(self):
        page = load('estab_clean.html').replace('100 \n', '\n', 1)
        blocks = dp._sa_parse_estab(page)
        self.assertEqual(len(blocks), 1)
        self.assertIsNone(blocks[0]['score'])


def baanthai_facility():
    return {'license_id': '1788001', 'name': 'BAANTHAI THAI CUISINE',
            'street': '9386 HUEBNER RD #101', 'zip': '78240',
            'inspections': {'2026-10-01': '4320251'}, 'newest_hit': '2026-10-01'}


class RecordTests(unittest.TestCase):
    def test_record_dict_last_inspection_and_history(self):
        blocks = dp._sa_parse_estab(load('estab_history.html'))
        recs = dp._sa_build_records(baanthai_facility(), blocks, '2026-01-01', '2026-10-10')
        self.assertEqual([r['inspection_date'] for r in recs], ['2026-10-01', '2026-04-02'])
        last, earlier = recs
        self.assertEqual(last['name'], 'Baanthai Thai Cuisine')
        self.assertEqual(last['address'], '9386 Huebner Rd #101')
        self.assertEqual((last['city'], last['state'], last['zip']), ('San Antonio', 'TX', '78240'))
        self.assertIsNone(last['latitude'])
        self.assertIsNone(last['longitude'])
        self.assertEqual((last['original_score'], last['risk_score']), (98, 98))
        self.assertEqual(last['total_violations'], 2)
        self.assertEqual(last['priority_violations'] + last['priority_foundation_violations']
                         + last['core_violations'], 2)
        self.assertEqual(len(last['violations']), 2)
        self.assertTrue(all(isinstance(v, list) and len(v) == 3 for v in last['violations']))
        self.assertEqual(last['source'], 'San Antonio Metro Health')
        self.assertEqual(last['source_id'], 'sa_1788001_4320251')
        self.assertEqual(last['source_url'],
                         dp.SA_BASE + 'estab.cfm?licenseID=1788001&inspectionID=4320251')
        self.assertEqual((last['inspection_type'], last['results'], last['metro']), ('', '', ''))
        # an inspection the search did not list carries no id on the page:
        # date-based id, facility URL
        self.assertEqual(earlier['source_id'], 'sa_1788001_20260402')
        self.assertEqual(earlier['source_url'], dp.SA_BASE + 'estab.cfm?licenseID=1788001')
        self.assertEqual((earlier['original_score'], earlier['risk_score']), (95, 95))
        self.assertEqual(earlier['total_violations'], 3)

    def test_window_bounds_blocks(self):
        blocks = dp._sa_parse_estab(load('estab_history.html'))
        recs = dp._sa_build_records(baanthai_facility(), blocks, '2026-10-01', '2026-10-01')
        self.assertEqual([r['source_id'] for r in recs], ['sa_1788001_4320251'])
        recs = dp._sa_build_records(baanthai_facility(), blocks, '2025-01-01', '2025-12-31')
        self.assertEqual([r['inspection_date'] for r in recs], ['2025-10-02', '2025-04-02'])

    def test_official_score_is_the_risk_score(self):
        fac = {'license_id': '9', 'name': '4 MISSION CAFE', 'street': '12522 SOUTHTON RD',
               'zip': '78223', 'inspections': {'2026-09-14': '77'}, 'newest_hit': '2026-09-14'}
        blocks = dp._sa_parse_estab(load('estab_violations.html'))
        rec, = dp._sa_build_records(fac, blocks, '2026-09-01', '2026-09-30')
        computed, _, _, _ = dp.calc_risk_score([dp.classify_violation(t) for t in blocks[0]['texts']])
        self.assertNotEqual(computed, 85)
        self.assertEqual((rec['original_score'], rec['risk_score']), (85, 85))
        self.assertEqual(rec['total_violations'], 8)
        self.assertEqual(rec['source_id'], 'sa_9_77')

    def test_missing_score_falls_back_to_computed(self):
        fac = baanthai_facility()
        blocks = [{'date': '10/01/2026', 'score': None,
                   'texts': ['Observed: Rodent droppings on shelves — 6-501.111 Controlling pests.']}]
        rec, = dp._sa_build_records(fac, blocks, '2026-10-01', '2026-10-01')
        self.assertIsNone(rec['original_score'])
        self.assertEqual(rec['risk_score'],
                         dp.calc_risk_score([dp.classify_violation(blocks[0]['texts'][0])])[0])
        self.assertEqual(rec['violations'][0][0], 'pest_control')

    def test_clean_block_keeps_score_100(self):
        fac = {'license_id': '1777343', 'name': '210 FIT PREP MEALS', 'street': '328 NORTH PARK DR',
               'zip': '78216', 'inspections': {'2026-10-01': '4320270'}, 'newest_hit': '2026-10-01'}
        rec, = dp._sa_build_records(fac, dp._sa_parse_estab(load('estab_clean.html')),
                                    '2026-10-01', '2026-10-01')
        self.assertEqual((rec['risk_score'], rec['original_score'], rec['violations']), (100, 100, []))

    def test_ids_are_stable_across_parses(self):
        blocks = dp._sa_parse_estab(load('estab_history.html'))
        a, = dp._sa_build_records(baanthai_facility(), blocks, '2026-10-01', '2026-10-01')
        b, = dp._sa_build_records(baanthai_facility(), dp._sa_parse_estab(load('estab_history.html')),
                                  '2026-10-01', '2026-10-01')
        self.assertEqual(a['source_id'], b['source_id'])
        rid = dp.make_restaurant_id(a['name'], a['address'], a['city'])
        self.assertEqual(rid, dp.make_restaurant_id(b['name'], b['address'], b['city']))
        self.assertEqual(rid, dp.make_restaurant_id('Baanthai Thai Cuisine', '9386 Huebner Rd #101',
                                                    'San Antonio'))


class FetchTests(unittest.TestCase):
    """fetch_san_antonio end to end with the portal mocked: one day whose
    search lists ten facilities (the zip fixture), two readable estab pages
    and eight that fail."""

    def setUp(self):
        self.sleep = mock.patch.object(dp.time, 'sleep', lambda *_: None)
        self.sleep.start()
        self.addCleanup(self.sleep.stop)
        self.saved = (dp._FETCH_DEADLINE, dict(dp._GROUP_NOTES))
        dp._FETCH_DEADLINE = None
        self.addCleanup(self._restore)
        self.estab_requests = []

    def _restore(self):
        dp._FETCH_DEADLINE, notes = self.saved
        dp._GROUP_NOTES.clear()
        dp._GROUP_NOTES.update(notes)

    def portal(self, estab_pages):
        def fake(session, method, url, **kw):
            if 'search.cfm' in url:
                return FakeResponse(load('search_zip.html'))
            lic = url.rsplit('licenseID=', 1)[1]
            self.estab_requests.append(lic)
            page = estab_pages.get(lic)
            return FakeResponse(load(page)) if page else None
        return fake

    def test_records_from_readable_pages_only(self):
        # 1778101 (BITE ME TRUCK) is served BAANTHAI's history page: its
        # newest block is dated 10/01, matching the search result.
        pages = {'1777343': 'estab_clean.html', '1778101': 'estab_history.html'}
        with mock.patch.object(dp, '_tyler_request', self.portal(pages)):
            results = dp.fetch_san_antonio(since_date='2026-10-01', until_date='2026-10-01')
        by_id = {r['source_id']: r for r in results}
        self.assertEqual(set(by_id), {'sa_1777343_4320270', 'sa_1778101_4320271'})
        fit = by_id['sa_1777343_4320270']
        self.assertEqual((fit['name'], fit['address'], fit['zip']),
                         ('210 Fit Prep Meals', '328 North Park Dr', '78216'))
        self.assertEqual((fit['original_score'], fit['risk_score'], fit['violations']), (100, 100, []))
        self.assertEqual(fit['inspection_date'], '2026-10-01')
        truck = by_id['sa_1778101_4320271']
        self.assertEqual(truck['name'], 'Bite Me Truck')
        self.assertEqual((truck['original_score'], truck['risk_score'], truck['total_violations']),
                         (98, 98, 2))
        self.assertTrue(truck['source_url'].endswith('licenseID=1778101&inspectionID=4320271'))
        # every facility fetched once in the pool; the eight failures retried once each
        self.assertEqual(len([l for l in self.estab_requests if l == '1777343']), 1)
        self.assertEqual(len([l for l in self.estab_requests if l == '1785075']), 2)
        self.assertEqual(len(self.estab_requests), 10 + 8)

    def test_unreadable_pages_drop_the_inspection(self):
        with mock.patch.object(dp, '_tyler_request', self.portal({})):
            results = dp.fetch_san_antonio(since_date='2026-10-01', until_date='2026-10-01')
        self.assertEqual(results, [])

    def test_consecutive_failures_abort_the_run(self):
        with mock.patch.object(dp, 'TYLER_ABORT_AFTER', 3), \
                mock.patch.object(dp, '_tyler_request', self.portal({})):
            with self.assertRaises(RuntimeError):
                dp.fetch_san_antonio(since_date='2026-10-01', until_date='2026-10-01')

    def test_deadline_before_detail_phase_ships_nothing(self):
        state = {'searched': False}
        inner = self.portal({'1777343': 'estab_clean.html'})

        def fake(session, method, url, **kw):
            r = inner(session, method, url, **kw)
            if 'search.cfm' in url:
                state['searched'] = True
            return r

        with mock.patch.object(dp, '_tyler_request', fake), \
                mock.patch.object(dp, '_past_deadline', lambda: state['searched']):
            results = dp.fetch_san_antonio(since_date='2026-10-01', until_date='2026-10-01')
        self.assertEqual(results, [])
        self.assertEqual(self.estab_requests, [])

    def test_only_searched_days_ship(self):
        # limit stops the search after 09/30 (the mock lists the 10/01
        # facilities for every day); their 10/01 inspections must not ship,
        # or freshness would advance past a day nobody searched.
        pages = {'1777343': 'estab_clean.html'}
        with mock.patch.object(dp, '_tyler_request', self.portal(pages)):
            results = dp.fetch_san_antonio(since_date='2026-09-30', until_date='2026-10-01', limit=5)
        self.assertEqual(results, [])
        self.assertNotIn('sanantonio', dp._GROUP_NOTES)   # no lag note from a bounded run
        with mock.patch.object(dp, '_tyler_request', self.portal(pages)):
            results = dp.fetch_san_antonio(since_date='2026-10-01', until_date='2026-10-01', limit=5)
        self.assertEqual([r['inspection_date'] for r in results], ['2026-10-01'])

    def test_search_only_debug_path(self):
        with mock.patch.object(dp, '_tyler_request', self.portal({})):
            results = dp.fetch_san_antonio(since_date='2026-10-01', until_date='2026-10-01',
                                           fetch_violations=False)
        self.assertEqual(len(results), 10)
        self.assertEqual(self.estab_requests, [])
        by_id = {r['source_id']: r for r in results}
        self.assertTrue(by_id['sa_1778101_4320271']['source_url'].endswith(
            'licenseID=1778101&inspectionID=4320271'))
        self.assertTrue(all(r['risk_score'] == 100 and r['violations'] == [] for r in results))

    def test_limit_caps_facilities(self):
        pages = {'1777343': 'estab_clean.html', '1778101': 'estab_clean.html'}
        with mock.patch.object(dp, '_tyler_request', self.portal(pages)):
            results = dp.fetch_san_antonio(since_date='2026-10-01', until_date='2026-10-01', limit=2)
        self.assertLessEqual(len(results), 2)
        self.assertLessEqual(len(set(self.estab_requests)), 2)


if __name__ == '__main__':
    unittest.main()
