"""Tests for fetch_abilene (Abilene / Taylor County, TX — the City of Abilene's
Tyler EnerGov "Civic Access" API). No network: requests.Session is replaced
by a stand-in that answers the civicapi routes from fixtures captured live on
2026-11-22/23: 12 search rows (new-format routines, a score-0 re-inspection,
a routine with no scheduled date that only an undated search lists, a visit
done early whose requested date is in 2027, three legacy inspections, a
university cafeteria, a school) with each one's header, score, OUT lines and
operational permit (or business title). The inspector's and permit issuer's
names and e-mails, the header's free-text comment and the permit's
description (a contact e-mail) were replaced by '<redacted>' so a test can
prove none of them leaves the fetcher; every other field is verbatim."""
import contextlib
import copy
import json
import logging
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader as jr  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'abilene')

# caseIds
GALVESTON = '2e0ad5f2-56d9-4ed0-abb2-298ac102043c'   # RFEI-ROUT-000414-2026, Failed 66, items 20 and 34 with notes
FUZZYS = '3fd84f76-8d3e-4076-8f8b-2cd83b219012'      # 000605, 90, item 34 with no notes
BRAUMS_320 = '4afcd747-c231-4ef1-ab6c-922540394244'  # 000318, 83, item 20 with no notes
BRAUMS_315 = '9d501c17-d643-48f4-887d-8c5df3e0c3bd'  # 000400, 87, completed 2026-11-05
TACO_CASA = 'bf342c3e-5932-4f69-9b88-6ce9e5a312ef'   # RFEI-REIN-000871-2026, score 0
OLLIES = 'd4ed954c-67b1-4e45-b8f1-ccd7eb0d5da5'      # 000151, no scheduled date, linked to a business
CHARCUTERIE = '3fa5e0c7-7863-43b0-8403-ec97ceccb1f1'  # 000886, requested 2027-05-01, completed 2026-11-20, 100
TACO_BUENO = '49c06099-e8e9-4462-a026-a91cc9376fa9'  # legacy 69082, 71, items cited twice
SHED = '3d5fbf9b-1d68-4ae8-82e2-3e8c508f23ee'        # legacy 69070, 95
MARKET_ST = '07b0d547-784e-40a7-a8ef-3f2b45f8fe4f'   # legacy 69016, 100
CHARTWELLS = '9df09cb3-dd1f-4ac2-9135-6d44d31af42e'  # 000157, permit class 'Academy'
SCHOOL_NUMBER = 'RFEI-BAS-000119-2026'

SINCE = '2026-11-20'           # keep from 2026-11-06, search from 2026-09-21
IN_WINDOW = {'abl_RFEI-ROUT-000414-2026', 'abl_RFEI-ROUT-000605-2026', 'abl_RFEI-ROUT-000318-2026',
             'abl_RFEI-ROUT-000886-2026'}
WHOLE_RECORD = IN_WINDOW | {'abl_RFEI-ROUT-000400-2026', 'abl_RFEI-ROUT-000151-2026', 'abl_69082', 'abl_69070',
                            'abl_69016'}
CONTRACT_KEYS = ('name', 'address', 'city', 'state', 'zip', 'latitude', 'longitude', 'inspection_date',
                 'original_score', 'risk_score', 'priority_violations', 'priority_foundation_violations',
                 'core_violations', 'total_violations', 'violations', 'inspection_type', 'results',
                 'source', 'source_url', 'source_id', 'metro')
TYPE_IDS = {v: k for k, v in dp.ABL_SWEEP_TYPE_IDS.items()}


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as f:
        return json.load(f)


def load_fixtures():
    out = {'search': _load('search_rows.json')}
    for kind in ('header', 'score', 'violations', 'permit', 'business'):
        out[kind] = {}
    for name in sorted(os.listdir(FIXTURES)):
        kind, _, rest = name.partition('_')
        if kind in out and kind != 'search':
            out[kind][rest[:-5].lower()] = _load(name)
    return out


def fields(row):
    return {f['label']: f['value'] for f in row['searchResultFields']}


class FakeResponse:
    def __init__(self, payload, status=200, headers=None):
        self._payload = payload
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            err = dp.requests.HTTPError(f'{self.status_code}')
            err.response = self
            raise err

    def json(self):
        if isinstance(self._payload, str):
            raise ValueError('not JSON')          # the SPA shell or an IIS error page
        return self._payload


class FakePortal:
    """requests.Session stand-in for the civicapi routes, answering the way the
    live API does: search-inspections filters on the requested date (and, like
    the live server, leaves out a row with no scheduled date when it does),
    on caseTypeId, pages 1-based with totalCount; the per-inspection and
    per-permit routes answer their fixture or 404. `fail` queues answers (an
    exception or a FakeResponse) served before the real one, keyed (route,
    id) — ('search', page), ('header', caseId), ('score', caseId),
    ('lines', caseId), ('permit', linkId), ('business', linkId); `fail_all`
    names routes whose every call raises ConnectionError."""

    def __init__(self, fx):
        self.fx = fx
        self.calls = []
        self.fail, self.fail_all = {}, set()
        self.total_override = None
        self.ignore_page = False

    def _queued(self, key):
        q = self.fail.get(key)
        if q:
            x = q.pop(0)
            if isinstance(x, Exception):
                raise x
            return x
        if key[0] in self.fail_all:
            raise dp.requests.ConnectionError('reset')
        return None

    def post(self, url, data=None, timeout=None, headers=None):
        body = json.loads(data)
        self.calls.append(('POST', url, body, headers, timeout))
        if url == dp.ABL_SEARCH_URL:
            page = body['pageNumber']
            queued = self._queued(('search', page))
            if queued is not None:
                return queued
            rows = list(self.fx['search'])
            frm = body['requestedDateFrom']
            assert body['requestedDateTo'] is None, body
            if frm:
                rows = [r for r in rows if fields(r)['Scheduled Date']
                        and (fields(r)['Requested Date'] or '') >= frm]
            type_id = body['publicRecordsCommonSearchCriteria']['caseTypeId']
            if type_id:
                rows = [r for r in rows if r['caseTypeWorkClassFriendlyName'] == TYPE_IDS[type_id]]
            rows.sort(key=lambda r: r['caseNumber'], reverse=True)
            size = body['pageSize']
            start = 0 if self.ignore_page else (page - 1) * size
            total = len(rows) if self.total_override is None else self.total_override
            return FakeResponse({'entityPublicSearchResults': copy.deepcopy(rows[start:start + size]),
                                 'pageNumber': page - 1, 'pageSize': size, 'totalCount': total})
        if url == dp.ABL_VIOLATIONS_URL:
            cid = body['inspectionId'].lower()
            assert body == {'inspectionId': body['inspectionId'], 'isInViolation': True, 'module': 7,
                            'inspectionViolationItemId': ''}, body
            queued = self._queued(('lines', cid))
            if queued is not None:
                return queued
            if cid not in self.fx['violations']:
                return FakeResponse({'statusCode': 404}, status=404)
            return FakeResponse(copy.deepcopy(self.fx['violations'][cid]))
        raise AssertionError(url)

    def get(self, url, timeout=None, headers=None):
        self.calls.append(('GET', url, None, headers, timeout))
        for kind, template in (('score', dp.ABL_SCORE_URL), ('permit', dp.ABL_PERMIT_URL),
                               ('business', dp.ABL_BUSINESS_URL), ('header', dp.ABL_HEADER_URL)):
            prefix, _, suffix = template.partition('{}')
            if url.startswith(prefix) and url.endswith(suffix):
                key = url[len(prefix):len(url) - len(suffix)].lower()
                if '/' in key:
                    continue
                queued = self._queued((kind, key))
                if queued is not None:
                    return queued
                if key not in self.fx[kind]:
                    return FakeResponse({'statusCode': 404}, status=404)
                return FakeResponse(copy.deepcopy(self.fx[kind][key]))
        raise AssertionError(url)

    def routes(self, kind):
        """The ids requested on one route, in order."""
        out = []
        for method, url, body, _, _ in self.calls:
            if kind == 'search' and url == dp.ABL_SEARCH_URL:
                out.append(body)
            elif kind == 'lines' and url == dp.ABL_VIOLATIONS_URL:
                out.append(body['inspectionId'].lower())
            elif method == 'GET':
                template = {'header': dp.ABL_HEADER_URL, 'score': dp.ABL_SCORE_URL, 'permit': dp.ABL_PERMIT_URL,
                            'business': dp.ABL_BUSINESS_URL}.get(kind)
                if template:
                    prefix, _, suffix = template.partition('{}')
                    key = url[len(prefix):len(url) - len(suffix)] if url.startswith(prefix) and url.endswith(suffix) else ''
                    if key and '/' not in key:
                        out.append(key.lower())
        return out


def by_id(records):
    return {r['source_id']: r for r in records}


class AbileneTestCase(unittest.TestCase):
    """Saves, clears and restores every pipeline global the fetcher touches
    (discovery runs all test modules in one process)."""
    GLOBALS = ('_ABL_BLOCKED', '_ABL_EXHAUSTED_STREAK', '_ABL_FAILURES_LOGGED', '_ABL_LAST_ERROR',
               '_FETCH_DEADLINE', '_FRESHNESS', 'ABL_PAGE', 'ABL_DELAY', 'ABL_BREAKER_TRIP',
               'ABL_MASS_FAILURE_MIN', 'ABL_REQUESTED_MARGIN_DAYS', 'ABL_COMPLETED_MARGIN_DAYS')
    DICTS = ('_GROUP_NOTES', '_FRESHNESS_HOLDS', '_GROUP_STATUS', '_ABL_STATS')

    def setUp(self):
        self._saved = {g: getattr(dp, g) for g in self.GLOBALS}
        self._saved_dicts = {d: dict(getattr(dp, d)) for d in self.DICTS}
        self._saved_known = set(dp.SKIP_KNOWN_INSPECTIONS)
        for d in self.DICTS:
            getattr(dp, d).clear()
        dp.SKIP_KNOWN_INSPECTIONS.clear()
        dp._FETCH_DEADLINE, dp._FRESHNESS = None, None
        self._level = dp.log.level
        dp.log.setLevel(logging.CRITICAL)
        self.fx = load_fixtures()

    def tearDown(self):
        for g, v in self._saved.items():
            setattr(dp, g, v)
        for d, v in self._saved_dicts.items():
            getattr(dp, d).clear()
            getattr(dp, d).update(v)
        dp.SKIP_KNOWN_INSPECTIONS.clear()
        dp.SKIP_KNOWN_INSPECTIONS.update(self._saved_known)
        dp.log.setLevel(self._level)

    def portal(self):
        return FakePortal(self.fx)

    def fetch(self, since_date=SINCE, limit=None, portal=None, past_deadline=None):
        portal = portal or self.portal()
        sleeps = []
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(dp.requests, 'Session', return_value=portal))
            stack.enter_context(mock.patch.object(dp.time, 'sleep', side_effect=sleeps.append))
            if past_deadline is not None:
                stack.enter_context(mock.patch.object(dp, '_past_deadline', side_effect=past_deadline))
            records = dp.fetch_abilene(since_date=since_date, limit=limit)
        return records, portal, sleeps

    def holds(self):
        return dict(dp._FRESHNESS_HOLDS.get(dp.ABL_SOURCE, {}))


# ── request shape ─────────────────────────────────────────────────────────
class RequestShapeTests(AbileneTestCase):
    def test_search_body_and_headers(self):
        records, portal, sleeps = self.fetch()
        searches = portal.routes('search')
        first = searches[0]
        self.assertEqual(first['requestedDateFrom'], '2026-09-21T00:00:00')     # SINCE less 60 days
        self.assertIsNone(first['requestedDateTo'])                              # open: a visit done early
        self.assertIsNone(first['scheduledDateFrom'])
        self.assertEqual(first['publicRecordsCommonSearchCriteria']['caseTypeId'], '')
        self.assertEqual((first['pageNumber'], first['pageSize']), (1, dp.ABL_PAGE))
        # the undated sweep of each new food type
        sweeps = searches[1:]
        self.assertEqual([s['publicRecordsCommonSearchCriteria']['caseTypeId'] for s in sweeps],
                         list(dp.ABL_SWEEP_TYPE_IDS.values()))
        self.assertTrue(all(s['requestedDateFrom'] is None for s in sweeps))
        for method, url, body, headers, timeout in portal.calls:
            self.assertEqual(headers['User-Agent'], dp.CHROME_UA)
            self.assertTrue(headers['Accept'].startswith('application/json'))
            self.assertEqual(timeout, dp.ABL_TIMEOUT)
            self.assertEqual(headers.get('Content-Type') == 'application/json', method == 'POST')
        # one request stream, ABL_DELAY after every answer
        self.assertEqual(sleeps, [dp.ABL_DELAY] * len(portal.calls))
        self.assertEqual(dp._ABL_STATS['requests'], len(portal.calls))

    def test_each_phase_requests_only_what_it_needs(self):
        records, portal, _ = self.fetch()
        self.assertEqual(len(portal.routes('search')), 3)
        # every listed food inspection's header (the school row never: its type is dropped at the list)
        self.assertEqual(set(portal.routes('header')),
                         {GALVESTON, FUZZYS, BRAUMS_320, BRAUMS_315, TACO_CASA, OLLIES, CHARCUTERIE, CHARTWELLS})
        # permits of the inspections completed in the window only
        self.assertEqual(len(portal.routes('permit')), 6)
        # no score for the cafeteria (out of scope by permit class) or the 2026-11-05 visit
        self.assertEqual(set(portal.routes('score')), {GALVESTON, FUZZYS, BRAUMS_320, TACO_CASA, CHARCUTERIE})
        # no lines for a 100 or an unscored re-inspection; oldest completed first
        self.assertEqual(portal.routes('lines'), [BRAUMS_320, GALVESTON, FUZZYS])

    def test_both_since_formats_give_the_same_window(self):
        for since in (SINCE, SINCE + 'T00:00:00'):
            with self.subTest(since=since):
                records, _, _ = self.fetch(since_date=since)
                self.assertEqual(set(by_id(records)), IN_WINDOW)
                self.assertTrue(all(r['inspection_date'] >= '2026-11-06' for r in records))

    def test_no_since_reads_the_whole_record(self):
        records, portal, _ = self.fetch(since_date=None)
        self.assertIsNone(portal.routes('search')[0]['requestedDateFrom'])
        self.assertEqual(set(by_id(records)), WHOLE_RECORD)

    def test_completed_margin_reads_back_two_weeks(self):
        dp.ABL_COMPLETED_MARGIN_DAYS = 0
        records, _, _ = self.fetch(since_date='2026-11-16')
        self.assertEqual(set(by_id(records)),
                         {'abl_RFEI-ROUT-000414-2026', 'abl_RFEI-ROUT-000605-2026', 'abl_RFEI-ROUT-000318-2026',
                          'abl_RFEI-ROUT-000886-2026'})
        dp.ABL_COMPLETED_MARGIN_DAYS = 14
        records, _, _ = self.fetch(since_date='2026-11-16')
        self.assertIn('abl_RFEI-ROUT-000400-2026', by_id(records))          # completed 2026-11-05

    def test_requested_margin_finds_a_visit_requested_long_before_it_was_done(self):
        # 000605 was requested 2026-11-02 and done 2026-11-20: a window
        # starting on its completion day must still list it.
        dp.ABL_REQUESTED_MARGIN_DAYS = 0
        records, _, _ = self.fetch(since_date='2026-11-20')
        self.assertNotIn('abl_RFEI-ROUT-000605-2026', by_id(records))
        dp.ABL_REQUESTED_MARGIN_DAYS = 60
        records, _, _ = self.fetch(since_date='2026-11-20')
        self.assertIn('abl_RFEI-ROUT-000605-2026', by_id(records))

    def test_a_visit_done_early_is_listed_by_the_open_upper_bound(self):
        records, _, _ = self.fetch()
        rec = by_id(records)['abl_RFEI-ROUT-000886-2026']
        self.assertEqual(rec['inspection_date'], '2026-11-20')           # the completed date, not 2027-05-01

    def test_paging_reads_every_page(self):
        dp.ABL_PAGE = 2
        records, portal, _ = self.fetch(since_date=None)
        self.assertEqual(set(by_id(records)), WHOLE_RECORD)
        pages = [b['pageNumber'] for b in portal.routes('search') if not b['publicRecordsCommonSearchCriteria']['caseTypeId']]
        self.assertEqual(pages, [1, 2, 3, 4, 5, 6])                      # 11 undated-search rows (no unscheduled one)

    def test_limit_reads_only_the_most_recently_requested(self):
        records, portal, _ = self.fetch(limit=2)
        # 000886 (requested 2027-05-01) and 000871 (2026-11-19)
        self.assertEqual(set(portal.routes('header')), {CHARCUTERIE, TACO_CASA})
        self.assertEqual(set(by_id(records)), {'abl_RFEI-ROUT-000886-2026'})
        self.assertNotIn(dp.ABL_GROUP, dp._GROUP_NOTES)


# ── the unscheduled-row sweep ─────────────────────────────────────────────
class UnscheduledSweepTests(AbileneTestCase):
    def test_the_date_filter_misses_a_row_without_a_scheduled_date(self):
        portal = self.portal()
        body = dp._abl_search_body('2026-09-01', 1, 100)
        rows = portal.post(dp.ABL_SEARCH_URL, data=json.dumps(body)).json()['entityPublicSearchResults']
        self.assertNotIn('RFEI-ROUT-000151-2026', [r['caseNumber'] for r in rows])

    def test_the_sweep_adds_it_by_its_requested_date(self):
        records, portal, _ = self.fetch(since_date='2026-09-28')
        rec = by_id(records)['abl_RFEI-ROUT-000151-2026']
        self.assertEqual(rec['inspection_date'], '2026-09-28')
        # linked to a business, not a permit: the name comes from its page title
        self.assertEqual(portal.routes('business'), ['c9744e30-fa94-47f4-8fd1-4eb71793f7b4'])
        self.assertEqual(rec['name'], "Ollie's Bargain Outlet Inc. #518")
        self.assertEqual(rec['address'], '841 N Judge Ely Blvd')
        self.assertEqual(rec['risk_score'], 97)

    def test_the_sweep_keeps_rows_requested_before_the_search_window_out(self):
        records, portal, _ = self.fetch()                  # search from 2026-09-21; 000151 requested 09-28
        self.assertIn(OLLIES, portal.routes('header'))     # listed, then left out by its completed date
        self.assertNotIn('abl_RFEI-ROUT-000151-2026', by_id(records))
        dp.ABL_REQUESTED_MARGIN_DAYS = 0                   # search from 2026-11-20: not listed at all
        records, portal, _ = self.fetch()
        self.assertNotIn(OLLIES, portal.routes('header'))


# ── record dict ───────────────────────────────────────────────────────────
class RecordTests(AbileneTestCase):
    def test_record_contract(self):
        records, _, _ = self.fetch(since_date=None)
        for r in records:
            self.assertEqual(tuple(r), CONTRACT_KEYS)
            self.assertEqual((r['city'], r['state'], r['zip'], r['metro']), ('Abilene', 'TX', '', ''))
            self.assertIsNone(r['latitude'])
            self.assertIsNone(r['longitude'])
            self.assertEqual(r['source'], dp.ABL_SOURCE)
            self.assertEqual(r['original_score'], r['risk_score'])
            self.assertEqual(r['total_violations'], len(r['violations']))
            self.assertEqual(r['total_violations'], r['priority_violations'] + r['priority_foundation_violations']
                             + r['core_violations'])
            for cat, sev, text in r['violations']:
                self.assertIn(sev, ('priority', 'priority_foundation', 'core'))
                self.assertLessEqual(len(text), 500)
                self.assertNotIn('\n', text)
            self.assertTrue(dp.sane_inspection_date(r['inspection_date']))

    def test_failed_routine(self):
        records, _, _ = self.fetch()
        r = by_id(records)['abl_RFEI-ROUT-000414-2026']
        self.assertEqual(r['name'], 'Galveston Seafood & Grill')
        self.assertEqual(r['address'], '4534 Buffalo Gap Rd')
        self.assertEqual(r['inspection_date'], '2026-11-19')
        self.assertEqual((r['original_score'], r['risk_score']), (66, 66))
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'], r['core_violations']),
                         (7, 2, 9))
        self.assertEqual(r['inspection_type'], 'Routine')
        self.assertEqual(r['results'], 'Failed')
        self.assertEqual(r['source_url'], dp.ABL_BASE + 'details/inspection/' + GALVESTON)
        self.assertEqual(r['violations'][0], ['temperature_control', 'priority',
                                              'Proper Cold Holding temperature(41°F/ 45°F): Observed raw fish '
                                              'stored in bucket of water at 64F for less than 4 hours. COS'])
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score'], dp.build_violation_text(r['violations'])), 'F')

    def test_names_and_addresses_are_normalised(self):
        records, _, _ = self.fetch(since_date=None)
        got = {r['source_id']: (r['name'], r['address']) for r in records}
        self.assertEqual(got['abl_RFEI-ROUT-000886-2026'], ('Sweet & Sour Charcuterie LLC', '2025 S 14th St Ste A'))
        self.assertEqual(got['abl_RFEI-ROUT-000605-2026'], ("Fuzzy's Taco Shop", '2439 N Judge Ely Blvd'))
        self.assertEqual(got['abl_69070'], ('The Shed Market', '7925 Buffalo Gap Rd'))

    def test_address_variants_give_one_restaurant_id(self):
        a = dp._abl_address('2025 S 14th ST Unit:STE A')
        self.assertEqual(a, dp._abl_address('2025 S 14TH ST Unit: STE A'))
        self.assertEqual(dp.make_restaurant_id('X', a, 'Abilene'),
                         dp.make_restaurant_id('X', dp._abl_address('2025 s 14th st unit:  ste a'), 'Abilene'))
        self.assertEqual(dp._abl_address('4310 BUFFALO GAP RD, SPACE #7'), '4310 Buffalo Gap Rd, Space #7')
        self.assertEqual(dp._abl_address('3161 S 23RD ST'), '3161 S 23rd St')

    def test_nothing_redacted_leaves_the_fetcher(self):
        records, _, _ = self.fetch(since_date=None)
        self.assertNotIn('redacted', json.dumps(records))

    def test_sorted_newest_first(self):
        records, _, _ = self.fetch(since_date=None)
        dates = [r['inspection_date'] for r in records]
        self.assertEqual(dates, sorted(dates, reverse=True))


# ── scope ─────────────────────────────────────────────────────────────────
class ScopeTests(AbileneTestCase):
    def test_unscored_reinspection_is_left_out(self):
        records, portal, _ = self.fetch()
        self.assertNotIn('abl_RFEI-REIN-000871-2026', by_id(records))
        self.assertIn(TACO_CASA, portal.routes('score'))
        self.assertNotIn(TACO_CASA, portal.routes('lines'))
        self.assertNotIn(dp.ABL_SOURCE, dp._FRESHNESS_HOLDS)        # left out, not dropped

    def test_a_scored_reinspection_ships(self):
        self.fx['score'][TACO_CASA]['scoreDetail']['violationScore'] = 98.0
        records, _, _ = self.fetch()
        r = by_id(records)['abl_RFEI-REIN-000871-2026']
        self.assertEqual((r['inspection_type'], r['risk_score'], r['priority_foundation_violations']),
                         ('Reinspection', 98, 1))

    def test_institutional_permit_class_is_left_out(self):
        records, portal, _ = self.fetch()
        self.assertNotIn('abl_RFEI-ROUT-000157-2026', by_id(records))
        self.assertNotIn(CHARTWELLS, portal.routes('score'))
        self.assertIn('Academy', dp.ABL_SKIP_PERMIT_CLASSES)

    def test_school_type_is_dropped_at_the_list(self):
        records, portal, _ = self.fetch(since_date=None)
        self.assertNotIn('abl_' + SCHOOL_NUMBER, by_id(records))
        self.assertEqual(len(portal.routes('header')), 11)

    def test_unknown_type_is_dropped_with_a_warning(self):
        row = copy.deepcopy(self.fx['search'][0])
        row.update(caseId='00000000-0000-0000-0000-000000000001', caseNumber='XYZ-1',
                   caseTypeWorkClassFriendlyName='Farmers Market Booth')
        self.fx['search'].append(row)
        with self.assertLogs(dp.log, level='WARNING') as logs:
            records, portal, _ = self.fetch()
        self.assertTrue(any('Farmers Market Booth' in m and 'decide' in m for m in logs.output))
        self.assertNotIn('00000000-0000-0000-0000-000000000001', portal.routes('header'))

    def test_a_status_that_is_not_a_closed_report_is_left_out(self):
        self.fx['header'][FUZZYS]['inspectionStatusName'] = 'Scheduled'
        records, _, _ = self.fetch()
        self.assertNotIn('abl_RFEI-ROUT-000605-2026', by_id(records))

    def test_known_inspections_are_not_read_again(self):
        rid = dp.make_restaurant_id("Fuzzy's Taco Shop", '2439 N Judge Ely Blvd', 'Abilene')
        dp.SKIP_KNOWN_INSPECTIONS.add((rid, '2026-11-20'))
        records, portal, _ = self.fetch()
        self.assertNotIn('abl_RFEI-ROUT-000605-2026', by_id(records))
        self.assertNotIn(FUZZYS, portal.routes('score'))


# ── score rule ────────────────────────────────────────────────────────────
class ScoreTests(AbileneTestCase):
    def test_every_fixture_score_is_100_minus_points_per_distinct_item(self):
        for cid, lines in self.fx['violations'].items():
            score = dp._abl_score(self.fx['score'][cid])
            if not score:
                continue
            with self.subTest(cid=cid):
                _, demerits = dp._abl_violations(lines)
                self.assertEqual(score, 100 - demerits)

    def test_legacy_items_cited_twice_count_once_and_ship_once(self):
        records, _, _ = self.fetch(since_date=None)
        r = by_id(records)['abl_69082']
        self.assertEqual(r['risk_score'], 71)
        self.assertEqual(len(self.fx['violations'][TACO_BUENO]), 20)
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'], r['core_violations']),
                         (4, 5, 7))
        texts = [v[2] for v in r['violations']]
        self.assertEqual(len(texts), len(set(texts)))

    def test_lines_that_do_not_reproduce_the_score_drop_and_hold(self):
        del self.fx['violations'][GALVESTON][0]          # a Priority line lost: lines say 69, score 66
        records, _, _ = self.fetch()
        self.assertNotIn('abl_RFEI-ROUT-000414-2026', by_id(records))
        self.assertEqual(self.holds(), {'2026-11-19|RFEI-ROUT-000414-2026': (True, False)})

    def test_mass_score_mismatch_raises(self):
        dp.ABL_MASS_FAILURE_MIN = 2
        for cid in (GALVESTON, FUZZYS):
            del self.fx['violations'][cid][0]
        with self.assertRaisesRegex(RuntimeError, 'do not add up'):
            self.fetch()

    def test_scored_below_100_with_no_out_line_is_unread(self):
        self.fx['violations'][FUZZYS] = []
        records, _, _ = self.fetch()
        self.assertNotIn('abl_RFEI-ROUT-000605-2026', by_id(records))
        self.assertIn('2026-11-20|RFEI-ROUT-000605-2026', self.holds())

    def test_unknown_tier_is_unread(self):
        self.fx['violations'][FUZZYS][0]['violationCategoryGroupName'] = 'Critical Items (4 Points)'
        records, _, _ = self.fetch()
        self.assertNotIn('abl_RFEI-ROUT-000605-2026', by_id(records))
        self.assertEqual(self.holds(), {'2026-11-20|RFEI-ROUT-000605-2026': (True, False)})

    def test_only_out_rows_are_violations(self):
        lines = copy.deepcopy(self.fx['violations'][FUZZYS])
        lines.append(dict(lines[0], observationType='IN', itemReference='3', inspectorNotes='ok'))
        out, demerits = dp._abl_violations(lines)
        self.assertEqual((len(out), demerits), (8, 10))
        with self.assertRaises(ValueError):
            dp._abl_violations({'message': 'error'})


# ── pest and sewage titles ────────────────────────────────────────────────
class PestAndSewageTitleTests(AbileneTestCase):
    def texts(self, cid):
        return [v[2] for v in dp._abl_violations(self.fx['violations'][cid])[0]]

    def test_item_34_with_an_observation_ships_under_the_neutral_heading(self):
        texts = self.texts(GALVESTON)
        self.assertIn('Pests and animals: Observed kitchen back door does not fully self close.', texts)
        self.assertFalse(any('rodent' in t.lower() for t in texts))

    def test_item_34_without_an_observation_is_the_registered_cited_pest_title(self):
        texts = self.texts(FUZZYS)
        self.assertIn('No Evidence of Insect contamination, rodent/other animals', texts)
        self.assertEqual(jr.cited_pest_item('No Evidence of Insect contamination, rodent/other animals'),
                         'pests_unspecified')

    def test_item_20_ships_under_a_neutral_heading(self):
        self.assertIn('Wastewater disposal', self.texts(BRAUMS_320))
        self.assertTrue(any(t.startswith('Wastewater disposal: Observed employee dump mop bucket')
                            for t in self.texts(GALVESTON)))
        self.assertFalse(any('sewage' in t.lower() for cid in (GALVESTON, BRAUMS_320, TACO_BUENO)
                             for t in self.texts(cid)))

    def test_no_fixture_inspection_is_a_text_forced_f(self):
        records, _, _ = self.fetch(since_date=None)
        for r in records:
            with self.subTest(r['source_id']):
                self.assertFalse(dp.has_active_pest_or_sewage(dp.build_violation_text(r['violations'])))

    def test_the_raw_titles_would_have_forced_an_f(self):
        # the agency's own wording, joined the way the grader joins lines
        raw = ('Water from approved source; Plumbing installed; proper backflow device|||'
               'Approved Sewage/Wastewater Disposal System, proper disposal')
        self.assertTrue(dp.has_active_pest_or_sewage(raw))
        lines = [{'observationType': 'OUT', 'itemReference': '19', 'sortOrder': 19,
                  'violationItemText': 'Water from approved source; Plumbing installed; proper backflow device',
                  'violationCategoryGroupName': 'Priority Items (3 Points)', 'inspectorNotes': ''},
                 {'observationType': 'OUT', 'itemReference': '20', 'sortOrder': 20,
                  'violationItemText': 'Approved Sewage/Wastewater Disposal System, proper disposal',
                  'violationCategoryGroupName': 'Priority Items (3 Points)', 'inspectorNotes': ''}]
        out, _ = dp._abl_violations(lines)
        self.assertFalse(dp.has_active_pest_or_sewage(dp.build_violation_text(out)))

    def test_a_named_finding_still_forces_the_f(self):
        for item, title, notes in (
                ('34', 'No Evidence of Insect contamination, rodent/other animals',
                 'Observed live roaches under the prep table.'),
                ('34', 'No Evidence of Insect contamination, rodent/other animals',
                 'Observed rodent droppings in dry storage.'),
                ('20', 'Approved Sewage/Wastewater Disposal System, proper disposal',
                 'Observed sewage backing up from the floor drain in the kitchen.')):
            with self.subTest(notes=notes):
                out, _ = dp._abl_violations([{'observationType': 'OUT', 'itemReference': item, 'sortOrder': 1,
                                              'violationItemText': title, 'inspectorNotes': notes,
                                              'violationCategoryGroupName': 'Core Items (1 Point)'}])
                self.assertTrue(dp.has_active_pest_or_sewage(out[0][2]), out)

    def test_flies_and_door_gaps_do_not(self):
        out, _ = dp._abl_violations(self.fx['violations'][BRAUMS_315])
        self.assertFalse(dp.has_active_pest_or_sewage(dp.build_violation_text(out)))

    def test_source_states_its_tiers(self):
        self.assertEqual(dp.ABL_SOURCE, 'Abilene Environmental Health')
        self.assertIn(dp.ABL_SOURCE, jr.SOURCE_TIER_SOURCES)
        self.assertNotIn(dp.ABL_SOURCE, jr.SCORE_FROM_TIERS_SOURCES)


# ── failures ──────────────────────────────────────────────────────────────
class FailureTests(AbileneTestCase):
    def test_search_page_error_raises(self):
        portal = self.portal()
        portal.fail[('search', 1)] = [dp.requests.ConnectionError('reset')] * dp.ABL_RETRIES
        dp.ABL_BREAKER_TRIP = 99
        with self.assertRaisesRegex(RuntimeError, 'search page 1 could not be read'):
            self.fetch(portal=portal)

    def test_search_wrong_shape_raises(self):
        portal = self.portal()
        portal.fail[('search', 1)] = [FakeResponse('<html>maintenance</html>')] * dp.ABL_RETRIES
        dp.ABL_BREAKER_TRIP = 99
        with self.assertRaises(RuntimeError):
            self.fetch(portal=portal)
        portal = self.portal()
        portal.fail[('search', 1)] = [FakeResponse({'message': 'error'})]
        with self.assertRaises(RuntimeError):
            self.fetch(portal=portal)

    def test_short_listing_raises(self):
        portal = self.portal()
        portal.total_override = 50
        with self.assertRaisesRegex(RuntimeError, 'listing incomplete'):
            self.fetch(portal=portal)

    def test_a_server_that_stops_paging_raises(self):
        dp.ABL_PAGE = 2
        portal = self.portal()
        portal.ignore_page = True
        with self.assertRaisesRegex(RuntimeError, 'not paging'):
            self.fetch(portal=portal, since_date=None)

    def test_transient_error_is_retried(self):
        portal = self.portal()
        portal.fail[('score', GALVESTON)] = [FakeResponse('err', status=503), dp.requests.Timeout('slow')]
        records, _, sleeps = self.fetch(portal=portal)
        self.assertIn('abl_RFEI-ROUT-000414-2026', by_id(records))
        self.assertIn(dp.ABL_BACKOFF, sleeps)
        self.assertIn(dp.ABL_BACKOFF * 3, sleeps)
        self.assertEqual(self.holds(), {})

    def test_unreadable_header_holds_the_window(self):
        portal = self.portal()
        portal.fail[('header', FUZZYS)] = [dp.requests.ConnectionError('reset')] * dp.ABL_RETRIES
        records, _, _ = self.fetch(portal=portal)
        self.assertEqual(set(by_id(records)), IN_WINDOW - {'abl_RFEI-ROUT-000605-2026'})
        # held at the window start (its date is unknown), counted, keyed on the caseId
        self.assertEqual(self.holds(), {f'2026-11-22|{FUZZYS}': (True, False)})

    def test_unreadable_lines_drop_and_hold(self):
        portal = self.portal()
        portal.fail[('lines', GALVESTON)] = [dp.requests.ConnectionError('reset')] * dp.ABL_RETRIES
        records, _, _ = self.fetch(portal=portal)
        self.assertNotIn('abl_RFEI-ROUT-000414-2026', by_id(records))
        self.assertEqual(self.holds(), {'2026-11-19|RFEI-ROUT-000414-2026': (True, False)})
        self.assertNotIn(dp.ABL_GROUP, dp._GROUP_STATUS)

    def test_a_404_score_is_dropped_held_and_not_retried(self):
        del self.fx['score'][FUZZYS]
        records, portal, _ = self.fetch()
        self.assertNotIn('abl_RFEI-ROUT-000605-2026', by_id(records))
        self.assertEqual(portal.routes('score').count(FUZZYS), 1)
        self.assertEqual(self.holds(), {'2026-11-20|RFEI-ROUT-000605-2026': (True, False)})

    def test_unreadable_permit_drops_and_holds(self):
        lid = self.fx['header'][GALVESTON]['linkId'].lower()
        del self.fx['permit'][lid]
        records, _, _ = self.fetch()
        self.assertNotIn('abl_RFEI-ROUT-000414-2026', by_id(records))
        self.assertIn('2026-11-19|RFEI-ROUT-000414-2026', self.holds())

    def test_one_failing_route_between_good_answers_is_not_a_block(self):
        portal = self.portal()
        portal.fail_all.add('lines')
        records, _, _ = self.fetch(portal=portal)
        self.assertFalse(dp._ABL_BLOCKED)
        self.assertNotIn(dp.ABL_GROUP, dp._GROUP_STATUS)
        self.assertEqual(set(by_id(records)), {'abl_RFEI-ROUT-000886-2026'})       # the 100 needs no lines
        self.assertEqual(len(self.holds()), 3)

    def test_breaker_trips_and_reports_blocked(self):
        portal = self.portal()
        portal.fail_all.update({'permit', 'score', 'lines', 'business'})
        records, _, _ = self.fetch(portal=portal)
        self.assertEqual(records, [])
        self.assertEqual(dp._GROUP_STATUS[dp.ABL_GROUP][0], 'blocked')
        self.assertTrue(dp._ABL_BLOCKED)
        # the three permits tried through every retry are counted holds;
        # every later inspection is held without counting
        holds = self.holds()
        self.assertEqual(sum(1 for c, _ in holds.values() if c), 3)
        self.assertEqual(sum(1 for c, _ in holds.values() if not c), 3)

    def test_breaker_during_headers_ships_nothing(self):
        portal = self.portal()
        portal.fail_all.add('header')
        records, _, _ = self.fetch(portal=portal)
        self.assertEqual(records, [])
        self.assertEqual(dp._GROUP_STATUS[dp.ABL_GROUP][0], 'blocked')
        self.assertEqual(self.holds(), {'2026-11-22|window': (False, False)})

    def test_mass_header_failure_raises(self):
        dp.ABL_MASS_FAILURE_MIN = 2
        dp.ABL_BREAKER_TRIP = 99
        portal = self.portal()
        portal.fail_all.add('header')
        with self.assertRaisesRegex(RuntimeError, 'headers could not be read'):
            self.fetch(portal=portal)

    def test_deadline_before_the_list_ships_nothing(self):
        records, portal, _ = self.fetch(past_deadline=lambda: True)
        self.assertEqual(records, [])
        self.assertEqual(portal.calls, [])
        self.assertEqual(dp._GROUP_STATUS[dp.ABL_GROUP][0], 'skipped')
        self.assertEqual(self.holds(), {'2026-11-22|window': (False, False)})

    def test_deadline_in_the_detail_phase_drops_the_newest_tail(self):
        portal = self.portal()

        def past():
            # past the deadline once the second score has been asked for
            return len(portal.routes('score')) >= 2

        records, _, _ = self.fetch(portal=portal, past_deadline=past)
        self.assertEqual(dp._GROUP_STATUS[dp.ABL_GROUP][0], 'skipped')
        shipped = set(by_id(records))
        self.assertTrue(shipped)
        self.assertTrue(shipped < IN_WINDOW)
        for key, (counted, _) in self.holds().items():
            self.assertFalse(counted, key)


if __name__ == '__main__':
    unittest.main()
