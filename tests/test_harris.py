"""Tests for fetch_harris (Harris County, TX — Harris County Public Health's
Food Safety Program portal API at secureweb.hcphtx.org/fspportal). No
network: requests.Session is replaced by a stand-in that answers the three
API endpoints from fixtures captured live on 2026-11-03 — one establishment
search page (10 real establishments, each with its full inspection list,
plus one synthetic Mobile Food Unit row) and the OUT-item lists of every
scored inspection they hold. The rows carry the owner's name, phone, e-mail,
id number and sales-tax id and the inspector's name; those fields were
replaced by '<redacted>' (so a test can prove nothing of them leaves the
fetcher); every other field is verbatim."""
import contextlib
import copy
import json
import logging
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader as jr  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'harris')

# establishment documentIds in search_page.json
CHARGE_UP = 'aef56cba-0bb7-4e97-8938-277e3b7d6fd6'      # Spring; Routine, Permitting, Follow-up
TMG = 'ebe4d714-a129-412e-bf05-4d4c8014e4b3'            # Katy; a Complaint with a closure
HANKS = '2f32a9ea-269d-4281-9ace-6d594f9103ba'          # Houston; Permitting scored 71, permit not approved
SCHOOL = '313009e7-668c-4abb-8bb5-9c797c3bbe24'         # Public School: out of scope
MARBLE_SLAB = '005262e1-4ff0-4a84-896b-40aa634b5e84'    # Cypress; Follow-Up Inspection, score != 100 - demerits
MCDONALDS = '3ea51110-f40e-4fd7-986a-2c5e97f963d4'      # Cypress; Complaint + Routine on the same day
PADDOCK = '3baf8664-fb07-4802-a940-fbace8a08e37'        # name 'Paddock Lounge ' (trailing space)
EL_KIOSKO = 'd9137020-80ad-412f-a96d-a23f1593e58c'      # has coordinates; only pre-opening visits
TITOS = 'e3e1f893-f231-4ed0-a47b-2172240fbfa0'          # SV Follow-ups, a Consultation, a 'Zero/Zero'
TOKYO = '2875b67e-4a2d-434e-b252-00051e98ec47'          # Permitting x2, a Consultation
TACO_TRUCK = 'synthetic-mobile-0000'                    # type Mobile: out of scope

SINCE = '2026-10-20'
SRC, GROUP = dp.HARRIS_SOURCE, dp.HARRIS_GROUP
# The inspections dated >= SINCE whose visit type ships.
IN_WINDOW = {'hcph_I0492917', 'hcph_I0493043', 'hcph_I0493073', 'hcph_I0493090', 'hcph_I0493151',
             'hcph_I0493152', 'hcph_I0493159', 'hcph_I0493169', 'hcph_I0493186'}
CONTRACT_KEYS = ('name', 'address', 'city', 'state', 'zip', 'latitude', 'longitude', 'inspection_date',
                 'original_score', 'risk_score', 'priority_violations', 'priority_foundation_violations',
                 'core_violations', 'total_violations', 'violations', 'inspection_type', 'results',
                 'source', 'source_url', 'source_id', 'metro')


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as f:
        return json.load(f)


def load_fixtures():
    rows = _load('search_page.json')['data']
    lists, items = {}, {}
    for name in sorted(os.listdir(FIXTURES)):
        if name.startswith('inspections_'):
            lists[name[len('inspections_'):-5]] = _load(name)
        elif name.startswith('violations_'):
            items[name[len('violations_'):-5]] = _load(name)
    return rows, lists, items


def aged(rows, lists, days):
    """Copies of the fixtures with every date pushed back `days` days."""
    def shift(d):
        s = str(d or '')
        if len(s) < 10:
            return d
        return (datetime.strptime(s[:10], '%Y-%m-%d') - timedelta(days=days)).strftime('%Y-%m-%d') + s[10:]
    rows, lists = copy.deepcopy(rows), copy.deepcopy(lists)
    for r in rows:
        r['lastCompletedInspectionDate'] = shift(r.get('lastCompletedInspectionDate'))
    for insps in lists.values():
        for i in insps:
            for k in ('date', 'dateUtc', 'startDate', 'endDate'):
                i[k] = shift(i.get(k))
    return rows, lists


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
            raise ValueError('not JSON')          # the Angular shell or an error page
        return self._payload


class FakePortal:
    """requests.Session stand-in answering the three HCPH endpoints the way
    the live API does: a server-side >= filter on lastCompletedInspectionDate,
    the requested sort, skip/take paging with totalCount; bare lists from the
    inspection and item endpoints. `fail` queues answers (an exception or a
    FakeResponse) served before the real one, keyed ('search', skip) /
    ('list', documentId) / ('items', inspection number); `fail_all` names
    kinds whose every call raises ConnectionError."""

    def __init__(self, rows, lists, items):
        self.rows, self.lists, self.items = rows, lists, items
        self.calls = []
        self.fail, self.fail_all = {}, set()
        self.total_override = None
        self.clamp = None               # a server that caps `take`
        self.after_page = None          # callback(portal, page_no, rows_served)
        self.on_list = self.on_items = None   # callback(id) once that answer was served
        self.pages_served = 0

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
        if url != dp.HARRIS_SEARCH_URL:
            raise AssertionError(url)
        queued = self._queued(('search', body.get('skip', 0)))
        if queued is not None:
            return queued
        rows = list(self.rows)
        if body.get('filter'):
            (field, op, value), = body['filter']
            assert (field, op) == ('lastCompletedInspectionDate', '>=') and value.endswith('T00:00:00'), body['filter']
            rows = [r for r in rows if (r.get('lastCompletedInspectionDate') or '') >= value[:10]]
        sort = body.get('sort') or []
        assert sort and sort[0]['selector'] == 'lastCompletedInspectionDate', sort
        rows.sort(key=lambda r: r['documentId'])
        rows.sort(key=lambda r: r.get('lastCompletedInspectionDate') or '', reverse=bool(sort[0].get('desc')))
        skip, take = body.get('skip', 0), body['take']
        if self.clamp:
            take = min(take, self.clamp)
        page = rows[skip:skip + take]
        total = len(rows) if self.total_override is None else self.total_override
        self.pages_served += 1
        if self.after_page:
            self.after_page(self, self.pages_served, page)
        return FakeResponse({'data': page, 'totalCount': total, 'groupCount': None})

    def get(self, url, params=None, timeout=None, headers=None):
        self.calls.append(('GET', url, dict(params or {}), headers, timeout))
        key = params['id']
        if url == dp.HARRIS_INSPECTIONS_URL:
            queued = self._queued(('list', key))
            if queued is not None:
                return queued
            if key not in self.lists:
                raise AssertionError(f'no inspection-list fixture for {key}')
            resp = FakeResponse(copy.deepcopy(self.lists[key]))
            if self.on_list:
                self.on_list(key)
            return resp
        if url == dp.HARRIS_VIOLATIONS_URL:
            queued = self._queued(('items', key))
            if queued is not None:
                return queued
            if key not in self.items:
                raise AssertionError(f'no item-list fixture for {key}')
            resp = FakeResponse(copy.deepcopy(self.items[key]))
            if self.on_items:
                self.on_items(key)
            return resp
        raise AssertionError(url)

    def gets(self, url, key=None):
        return [c for c in self.calls if c[0] == 'GET' and c[1] == url and (key is None or c[2].get('id') == key)]

    def search_bodies(self):
        return [c[2] for c in self.calls if c[0] == 'POST']


def run_fetch(portal, since_date=None, limit=None, past_deadline=None):
    sleeps = []
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(dp.requests, 'Session', return_value=portal))
        stack.enter_context(mock.patch.object(dp.time, 'sleep', side_effect=sleeps.append))
        if past_deadline is not None:
            stack.enter_context(mock.patch.object(dp, '_past_deadline', side_effect=past_deadline))
        records = dp.fetch_harris(since_date=since_date, limit=limit)
    return records, sleeps


def by_id(records):
    return {r['source_id']: r for r in records}


class HarrisTestCase(unittest.TestCase):
    """Saves, clears and restores every pipeline global the fetcher touches
    (discovery runs all test modules in one process)."""
    GLOBALS = ('_HARRIS_BLOCKED', '_HARRIS_EXHAUSTED_STREAK', '_HARRIS_FAILURES_LOGGED', '_HARRIS_LAST_ERROR',
               '_FETCH_DEADLINE', '_FRESHNESS', 'HARRIS_WORKERS', 'HARRIS_DELAY', 'HARRIS_PAGE')
    DICTS = ('_GROUP_NOTES', '_FRESHNESS_HOLDS', '_GROUP_STATUS', '_HARRIS_STATS')

    def setUp(self):
        self._saved = {g: getattr(dp, g) for g in self.GLOBALS}
        self._saved_dicts = {d: dict(getattr(dp, d)) for d in self.DICTS}
        for d in self.DICTS:
            getattr(dp, d).clear()
        dp._HARRIS_BLOCKED, dp._HARRIS_EXHAUSTED_STREAK = False, 0
        dp._FETCH_DEADLINE, dp._FRESHNESS = None, None
        # One worker: the stand-in is not thread-safe, and the oldest-first
        # item order is then exact.
        dp.HARRIS_WORKERS, dp.HARRIS_DELAY = 1, 0
        self._level = dp.log.level
        dp.log.setLevel(logging.WARNING)
        self.rows, self.lists, self.items = load_fixtures()

    def tearDown(self):
        for g, v in self._saved.items():
            setattr(dp, g, v)
        for d, v in self._saved_dicts.items():
            getattr(dp, d).clear()
            getattr(dp, d).update(v)
        dp.log.setLevel(self._level)

    def portal(self):
        return FakePortal(self.rows, self.lists, self.items)

    def fetch(self, since_date=SINCE, limit=None, portal=None, past_deadline=None):
        portal = portal or self.portal()
        records, sleeps = run_fetch(portal, since_date, limit, past_deadline)
        return records, portal, sleeps

    def row(self, did):
        return next(r for r in self.rows if r['documentId'] == did)

    def inspection(self, did, number):
        return next(i for i in self.lists[did] if i['number'] == number)


# ── request shape ─────────────────────────────────────────────────────────
class RequestShapeTests(HarrisTestCase):
    def test_search_body_filter_sort_and_headers(self):
        records, portal, _ = self.fetch()
        first = portal.calls[0]
        self.assertEqual(first[0], 'POST')
        self.assertEqual(first[1], dp.HARRIS_SEARCH_URL)
        self.assertEqual(first[2], {
            'skip': 0, 'take': dp.HARRIS_PAGE, 'requireTotalCount': True,
            'sort': [{'selector': 'lastCompletedInspectionDate', 'desc': True},
                     {'selector': 'documentId', 'desc': False}],
            'filter': [['lastCompletedInspectionDate', '>=', '2026-10-20T00:00:00']]})
        self.assertEqual(first[3]['User-Agent'], dp.CHROME_UA)
        self.assertEqual(first[3]['App-Version'], '1.0')
        self.assertEqual(first[3]['Content-Type'], 'application/json')
        self.assertTrue(first[3]['Accept'].startswith('application/json'))
        self.assertEqual(first[4], dp.HARRIS_TIMEOUT)
        for call in portal.gets(dp.HARRIS_INSPECTIONS_URL) + portal.gets(dp.HARRIS_VIOLATIONS_URL):
            self.assertEqual(list(call[2]), ['id'])
            self.assertEqual(call[3]['User-Agent'], dp.CHROME_UA)
            self.assertNotIn('Content-Type', call[3])
            self.assertEqual(call[4], dp.HARRIS_TIMEOUT)

    def test_both_since_formats_give_the_same_window(self):
        for since in (SINCE, SINCE + 'T00:00:00'):
            with self.subTest(since=since):
                records, portal, _ = self.fetch(since_date=since)
                self.assertEqual(set(by_id(records)), IN_WINDOW)
                self.assertEqual(portal.search_bodies()[0]['filter'],
                                 [['lastCompletedInspectionDate', '>=', '2026-10-20T00:00:00']])
                self.assertTrue(all(r['inspection_date'] >= SINCE for r in records))

    def test_no_since_reads_the_whole_published_record(self):
        records, portal, _ = self.fetch(since_date=None)
        self.assertNotIn('filter', portal.search_bodies()[0])
        ids = by_id(records)
        self.assertEqual(len(ids), 29)
        self.assertIn('hcph_I0476460', ids)          # 2025-12-30
        self.assertIn('hcph_I0453873', ids)          # 2024-11-15
        self.assertTrue(IN_WINDOW <= set(ids))

    def test_each_phase_requests_only_what_it_needs(self):
        records, portal, _ = self.fetch()
        listed = {c[2]['id'] for c in portal.gets(dp.HARRIS_INSPECTIONS_URL)}
        self.assertEqual(listed, {CHARGE_UP, TMG, HANKS, MARBLE_SLAB, MCDONALDS, PADDOCK, EL_KIOSKO, TITOS, TOKYO})
        read = [c[2]['id'] for c in portal.gets(dp.HARRIS_VIOLATIONS_URL)]
        # non-100 inspections of the window only, oldest first; a score of
        # 100 has no OUT items, so its list is never asked for
        self.assertEqual(read, ['I0492917', 'I0493043', 'I0493073', 'I0493151', 'I0493152', 'I0493169', 'I0493186'])
        probes = [b for b in portal.search_bodies() if b['take'] == 1]
        self.assertEqual(len(probes), 1)
        self.assertNotIn('filter', probes[0])
        self.assertEqual(dp._HARRIS_STATS['requests'], 1 + 9 + 7 + 1)
        self.assertEqual(len(portal.calls), dp._HARRIS_STATS['requests'])


# ── record dict ───────────────────────────────────────────────────────────
class RecordTests(HarrisTestCase):
    _cache = None

    def setUp(self):
        super().setUp()
        if RecordTests._cache is None:
            RecordTests._cache = self.fetch()
        self.records, self.portal_used, _ = RecordTests._cache
        self.recs = by_id(self.records)

    def test_closure_record_whole_dict(self):
        self.assertEqual(self.recs['hcph_I0493073'], {
            'name': 'TMG Burger Grill',
            'address': '6191 Hwy Blvd Ste 101',
            'city': 'Katy',
            'state': 'TX',
            'zip': '77494',
            'latitude': None,
            'longitude': None,
            'inspection_date': '2026-10-29',
            'original_score': 99,                  # the county's score
            'risk_score': 55,                      # capped: the county ordered a closure
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 1,
            'total_violations': 1,
            'violations': [['food_handling', 'core', '49. Non-food contact surfaces clean']],
            'inspection_type': 'Complaint',
            'results': 'Closure required',
            'source': 'Harris County Public Health',
            'source_url': 'https://secureweb.hcphtx.org/fspportal/establishment/' + TMG,
            'source_id': 'hcph_I0493073',
            'metro': '',
        })
        self.assertEqual(dp.calculate_vetted_grade(self.recs['hcph_I0493073']['risk_score']), 'F')

    def test_permitting_record_with_county_tiers(self):
        r = self.recs['hcph_I0493169']
        items = self.items['I0493169']
        self.assertEqual(tuple(r), CONTRACT_KEYS)
        self.assertEqual((r['name'], r['address'], r['city'], r['state'], r['zip']),
                         ("Hank's Cajun Grill", '4232 Hwy 6 N', 'Houston', 'TX', '77084'))
        self.assertEqual((r['inspection_date'], r['original_score'], r['risk_score']), ('2026-10-30', 71, 71))
        self.assertEqual(r['inspection_type'], 'Permitting')
        self.assertEqual(r['results'], 'Permit not approved')      # outranks 'Follow-up required'
        self.assertEqual(r['source_id'], 'hcph_I0493169')
        self.assertEqual(r['source_url'], dp.HARRIS_ESTABLISHMENT_URL.format(HANKS))
        self.assertEqual(r['total_violations'], len(items), 18)
        groups = [i['violationGroup'] for i in items]
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'], r['core_violations']),
                         (groups.count('P'), groups.count('F'), groups.count('C')))
        self.assertEqual(sum(int(i['numberOfDemerits']) for i in items), 100 - r['original_score'])
        expected = [[dp.classify_violation(f"{i['violationNumber']}. {i['violationDescription']}")[0],
                     dp.HARRIS_GROUP_SEVERITY[i['violationGroup']],
                     f"{i['violationNumber']}. {i['violationDescription']}"] for i in items]
        self.assertEqual(r['violations'], expected)
        self.assertTrue(all(len(v) == 3 and v[1] in ('priority', 'priority_foundation', 'core')
                            and len(v[2]) <= 500 and '\n' not in v[2] for v in r['violations']))
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'C')
        self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']), 'e6ad0087d84ceb58')

    def test_county_tier_overrides_the_classifier_and_counts_follow(self):
        # the regex would call this line priority_foundation; the county says Core
        self.assertEqual(dp.classify_violation('49. Non-food contact surfaces clean')[1], 'priority_foundation')
        self.assertEqual(self.recs['hcph_I0493073']['violations'][0][1], 'core')
        for r in self.records:
            _, pv, pfv, cv = dp.calc_risk_score(r['violations'])
            self.assertEqual((r['priority_violations'], r['priority_foundation_violations'], r['core_violations'],
                              r['total_violations']), (pv, pfv, cv, len(r['violations'])), r['source_id'])

    def test_a_score_of_100_ships_clean_without_an_item_request(self):
        for sid in ('hcph_I0493090', 'hcph_I0493159'):
            r = self.recs[sid]
            self.assertEqual((r['original_score'], r['risk_score'], r['violations'], r['total_violations']),
                             (100, 100, [], 0), sid)
            self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'A')
            self.assertEqual(self.portal_used.gets(dp.HARRIS_VIOLATIONS_URL, sid[5:]), [])

    def test_official_score_is_copied_even_when_the_items_disagree(self):
        # Marble Slab's follow-up: score 93, items worth 9 demerits (91)
        r = self.recs['hcph_I0492917']
        self.assertEqual((r['original_score'], r['risk_score']), (93, 93))
        self.assertEqual(sum(int(i['numberOfDemerits']) for i in self.items['I0492917']), 9)
        with self.assertLogs('dinescores', level='WARNING') as logs:
            self.fetch()
        self.assertTrue(any('not 100 minus' in m for m in logs.output), logs.output)

    def test_inspection_type_display_and_results(self):
        types = {sid: self.recs[sid]['inspection_type'] for sid in self.recs}
        self.assertEqual(types['hcph_I0492917'], 'Follow-up')      # 'Follow-Up Inspection'
        self.assertEqual(types['hcph_I0493090'], 'Follow-up')
        self.assertEqual(types['hcph_I0493152'], 'Routine')         # 'Routine Inspection'
        self.assertEqual(types['hcph_I0493151'], 'Complaint')
        self.assertEqual(types['hcph_I0493186'], 'Permitting')
        self.assertEqual({sid: r['results'] for sid, r in self.recs.items() if r['results']},
                         {'hcph_I0493073': 'Closure required', 'hcph_I0493169': 'Permit not approved'})
        # a follow-up the inspector required, with nothing worse to say
        records, _, _ = self.fetch(since_date='2026-03-01')
        self.assertEqual(by_id(records)['hcph_I0479929']['results'], 'Follow-up required')

    def test_nothing_private_leaves_the_fetcher(self):
        self.assertFalse(any('<redacted>' in json.dumps(r) for r in self.records))
        for r in self.records:
            self.assertEqual(tuple(r), CONTRACT_KEYS)
        dp.log.setLevel(logging.INFO)
        with self.assertLogs('dinescores', level='INFO') as logs:
            self.fetch()
        joined = '\n'.join(logs.output)
        self.assertNotIn('<redacted>', joined)
        self.assertNotIn('@', joined)

    def test_coordinates(self):
        # only El Kiosko carries coordinates in the fixture and it ships nothing
        self.assertTrue(all(r['latitude'] is None and r['longitude'] is None for r in self.records))
        self.row(MCDONALDS)['latitude'], self.row(MCDONALDS)['longitude'] = '30.0633124', -95.4478649
        records, _, _ = self.fetch()
        r = by_id(records)['hcph_I0493152']
        self.assertAlmostEqual(r['latitude'], 30.0633124)
        self.assertAlmostEqual(r['longitude'], -95.4478649)
        for bad in (None, '', '0', 0, 0.0, 'abc', float('nan')):
            self.assertIsNone(dp._harris_coord(bad), repr(bad))
        self.assertEqual(dp._harris_coord('29.5'), 29.5)

    def test_name_address_city_normalisation(self):
        r = by_id(self.records)['hcph_I0493186']
        self.assertEqual(r['name'], 'Paddock Lounge')               # trailing space collapsed
        self.assertEqual(self.recs['hcph_I0493152']['name'], "McDonald's #36342")   # mixed case kept
        row = self.row(HANKS)
        row['name'], row['address'], row['zip'] = "  HANK'S  CAJUN GRILL ", '4232 HWY 6 N', '77084-1234'
        records, _, _ = self.fetch()
        r = by_id(records)['hcph_I0493169']
        self.assertEqual((r['name'], r['address'], r['zip']), ("Hank's Cajun Grill", '4232 Hwy 6 N', '77084'))
        self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']), 'e6ad0087d84ceb58')
        row['city'], row['state'] = '', None
        records, _, _ = self.fetch()
        r = by_id(records)['hcph_I0493169']
        self.assertEqual((r['city'], r['state']), ('Houston', 'TX'))

    def test_ids_are_stable_run_to_run(self):
        again, _, _ = self.fetch()
        key = lambda r: (r['source_id'], dp.make_restaurant_id(r['name'], r['address'], r['city']))
        self.assertEqual(sorted(map(key, again)), sorted(map(key, self.records)))
        tmg = {dp.make_restaurant_id(r['name'], r['address'], r['city'])
               for r in self.records if r['source_id'] in ('hcph_I0493073', 'hcph_I0493159')}
        self.assertEqual(len(tmg), 1)

    def test_policy_registration(self):
        self.assertEqual((GROUP, SRC, dp.HARRIS_METRO), ('Harris County', 'Harris County Public Health', ''))
        self.assertEqual(jr.tier_policy(SRC), 'fixed')
        self.assertEqual(jr.score_policy(SRC, 93), 'official')
        self.assertTrue(all(r['metro'] == '' and r['source'] == SRC and r['state'] == 'TX' for r in self.records))


# ── visit types and scope ─────────────────────────────────────────────────
class PolicyTests(HarrisTestCase):
    def test_which_visit_types_ship(self):
        with self.assertLogs('dinescores', level='WARNING') as logs:
            records, _, _ = self.fetch(since_date='2026-10-01')
        ids = set(by_id(records))
        self.assertTrue({'hcph_I0493169', 'hcph_I0493186'} <= ids)          # Permitting
        self.assertTrue({'hcph_I0493073', 'hcph_I0493151'} <= ids)          # Complaint
        self.assertIn('hcph_I0493152', ids)                                 # Routine Inspection
        self.assertTrue({'hcph_I0493090', 'hcph_I0493159', 'hcph_I0493043', 'hcph_I0492917'} <= ids)  # follow-ups of operating places
        for dropped in ('hcph_I0493027', 'hcph_I0493040',    # Pre-opening
                        'hcph_I0493131',                     # Follow-up with only a Pre-opening before it
                        'hcph_I0493144',                     # Consultation
                        'hcph_I0492022'):                    # Zero/Zero (unknown)
            self.assertNotIn(dropped, ids)
        self.assertTrue(any('unknown type Zero/Zero' in m for m in logs.output), logs.output)

    def test_history_wide_policy(self):
        records, portal, _ = self.fetch(since_date=None)
        ids = by_id(records)
        for dropped in ('hcph_I0472987', 'hcph_I0470771',    # Non-permitted
                        'hcph_I0473646',                     # Hank's follow-up to its pre-opening
                        'hcph_I0463099', 'hcph_I0493144'):   # Consultation
            self.assertNotIn(dropped, ids)
        self.assertIn('hcph_I0491786', ids)                  # TMG follow-up after a Permitting
        sv = ids['hcph_I0489974']                            # SV Follow-up scored 56
        self.assertEqual((sv['inspection_type'], sv['risk_score']), ('SV Follow-up', 56))
        self.assertEqual(dp.calculate_vetted_grade(sv['risk_score']), 'F')
        self.assertEqual(ids['hcph_I0490742']['results'], 'Closure required')
        self.assertEqual(ids['hcph_I0490742']['risk_score'], dp.HARRIS_CLOSURE_CAP)

    def test_non_food_establishments_are_skipped(self):
        day_care = dict(self.row(TMG), documentId='synthetic-daycare', serviceType='Day Care Facility')
        market = dict(self.row(TMG), documentId='synthetic-market', type='FarmersMarket')
        nameless = dict(self.row(TMG), documentId='synthetic-nameless', name='  ')
        self.rows += [day_care, market, nameless]
        records, portal, _ = self.fetch()
        listed = {c[2]['id'] for c in portal.gets(dp.HARRIS_INSPECTIONS_URL)}
        for did in (SCHOOL, TACO_TRUCK, 'synthetic-daycare', 'synthetic-market', 'synthetic-nameless'):
            self.assertNotIn(did, listed)
        names = {r['name'] for r in records}
        self.assertFalse(names & {'Gloria Marshall Elementary', 'Synthetic Taco Truck'})
        self.assertEqual(set(by_id(records)), IN_WINDOW)

    def test_incomplete_or_undated_inspections_are_dropped(self):
        hanks = self.inspection(HANKS, 'I0493169')
        hanks['status'] = 'In Progress'
        self.inspection(PADDOCK, 'I0493186')['date'] = '2031-01-01'          # a typo years ahead
        self.inspection(MCDONALDS, 'I0493152')['date'] = None
        self.lists[TMG].append('not a row')
        self.lists[TMG].append(dict(self.inspection(TMG, 'I0493073'), number=''))
        records, _, _ = self.fetch()
        self.assertEqual(set(by_id(records)),
                         IN_WINDOW - {'hcph_I0493169', 'hcph_I0493186', 'hcph_I0493152'})

    def test_unscored_visits(self):
        hanks = self.inspection(HANKS, 'I0493169')
        hanks['score'] = 0
        self.items['I0493169'] = []
        records, _, _ = self.fetch()
        self.assertNotIn('hcph_I0493169', by_id(records))      # nothing to grade on: dropped
        hanks['score'] = None
        self.items['I0493169'] = load_fixtures()[2]['I0493169']
        records, _, _ = self.fetch()
        r = by_id(records)['hcph_I0493169']
        self.assertIsNone(r['original_score'])
        self.assertEqual(r['risk_score'], 100 - 29)             # the county's own rule over its items
        hanks['score'] = 108
        records, _, _ = self.fetch()
        self.assertEqual(by_id(records)['hcph_I0493169']['risk_score'], 100)

    def test_item_rows_that_are_not_findings(self):
        self.items['I0493073'] += [
            dict(self.items['I0493073'][0], violationNumber=1, violationDescription='Permit posted', complianceResult='In'),
            dict(self.items['I0493073'][0], violationNumber=7, violationDescription='   '),
            'junk']
        records, _, _ = self.fetch()
        self.assertEqual(by_id(records)['hcph_I0493073']['violations'],
                         [['food_handling', 'core', '49. Non-food contact surfaces clean']])
        self.assertIsNone(dp._harris_violation({'violationDescription': 'x', 'complianceResult': 'In'}))
        self.assertIsNone(dp._harris_violation(None))
        self.assertEqual(dp._harris_violation({'violationNumber': 38, 'violationGroup': 'c',
                                               'violationDescription': ' Insects, rodents, and animals not present '}),
                         ['pest_control', 'core', '38. Insects, rodents, and animals not present'])

    def test_a_cited_pest_item_title_does_not_force_an_f(self):
        self.items['I0493073'].append(dict(self.items['I0493073'][0], violationNumber=38, violationGroup='C',
                                           numberOfDemerits=2,
                                           violationDescription='Insects, rodents, and animals not present'))
        self.inspection(TMG, 'I0493073')['isClosureRequired'] = 'false'
        self.inspection(TMG, 'I0493073')['score'] = 97
        records, _, _ = self.fetch()
        r = by_id(records)['hcph_I0493073']
        self.assertEqual(r['violations'][1][:2], ['pest_control', 'core'])
        text = dp.build_violation_text(r['violations'])
        self.assertFalse(dp.has_active_pest_or_sewage(text))
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score'], text), 'A')


# ── window, limit, lag note ───────────────────────────────────────────────
class WindowAndLimitTests(HarrisTestCase):
    def test_limit_bounds_the_listing_and_the_records(self):
        records, portal, _ = self.fetch(since_date=None, limit=5)
        self.assertEqual(portal.search_bodies()[0]['take'], 5)
        self.assertEqual(len(portal.search_bodies()), 1)         # no newest-date probe after a bounded read
        self.assertEqual(len(records), 5)
        dates = [r['inspection_date'] for r in records]
        self.assertEqual(dates, sorted(dates, reverse=True))
        self.assertEqual([r['source_id'] for r in records][:2], ['hcph_I0493169', 'hcph_I0493186'])   # same day: worst first
        self.assertEqual(dp._GROUP_NOTES, {})

    def test_limit_25_like_test_mode(self):
        records, portal, _ = self.fetch(since_date=None, limit=25)
        self.assertEqual(len(records), 25)
        ids = set(by_id(records))
        for oldest in ('hcph_I0453873', 'hcph_I0463030', 'hcph_I0466764', 'hcph_I0470301'):
            self.assertNotIn(oldest, ids)
        self.assertEqual(dp._GROUP_NOTES, {})
        self.assertEqual(len(portal.search_bodies()), 1)

    def test_lag_note_when_the_source_itself_is_behind(self):
        self.rows, self.lists = aged(self.rows, self.lists, 400)     # newest row 2025-09-25
        with mock.patch.object(dp, '_note_source_lag', wraps=dp._note_source_lag) as note:
            records, _, _ = self.fetch(since_date=None)
        self.assertEqual(len(records), 29)
        self.assertEqual(note.call_count, 1)
        self.assertEqual(note.call_args.args, (GROUP, '2025-09-25'))
        self.assertIn(GROUP, dp._GROUP_NOTES)
        self.assertIn('2025-09-25', dp._GROUP_NOTES[GROUP])
        dp._GROUP_NOTES.clear()
        with mock.patch.object(dp, '_note_source_lag', wraps=dp._note_source_lag) as note:
            self.fetch(since_date=None, limit=3)
        self.assertEqual(note.call_count, 0)                          # a bounded read never saw the end
        self.assertEqual(dp._GROUP_NOTES, {})

    def test_a_fresh_source_leaves_no_note(self):
        self.fetch()
        self.assertEqual(dp._GROUP_NOTES, {})

    def test_an_empty_window_is_empty_and_still_probes_the_newest_date(self):
        records, portal, _ = self.fetch(since_date='2026-12-01')
        self.assertEqual(records, [])
        self.assertEqual(len(portal.search_bodies()), 2)
        self.assertEqual(portal.gets(dp.HARRIS_INSPECTIONS_URL), [])
        self.assertEqual(dp._GROUP_STATUS, {})

    def test_window_filter_is_inclusive_of_its_first_day(self):
        records, _, _ = self.fetch(since_date='2026-10-26')
        self.assertIn('hcph_I0492917', by_id(records))             # dated exactly 2026-10-26


# ── paging ────────────────────────────────────────────────────────────────
class PagingTests(HarrisTestCase):
    def test_pages_follow_until_the_total(self):
        dp.HARRIS_PAGE = 4
        records, portal, _ = self.fetch(since_date=None)
        self.assertEqual([b['skip'] for b in portal.search_bodies() if b['take'] == 4], [0, 4, 8])
        self.assertEqual(len(by_id(records)), 29)

    def test_a_server_that_clamps_take_is_paged_through(self):
        portal = self.portal()
        portal.clamp = 3
        records, portal, _ = self.fetch(since_date=None, portal=portal)
        self.assertEqual([b['skip'] for b in portal.search_bodies() if b['take'] == dp.HARRIS_PAGE], [0, 3, 6, 9])
        self.assertEqual(len(by_id(records)), 29)

    def test_a_row_already_read_that_moves_to_the_top_is_deduped(self):
        dp.HARRIS_PAGE = 4
        portal = self.portal()

        def bump(p, page_no, served):
            if page_no == 1:
                served[1]['lastCompletedInspectionDate'] = '2026-10-31'   # re-inspected mid-read
        portal.after_page = bump
        records, portal, _ = self.fetch(since_date=None, portal=portal)
        self.assertEqual(len({c[2]['id'] for c in portal.gets(dp.HARRIS_INSPECTIONS_URL)}), 9)
        self.assertEqual(len(by_id(records)), 29)

    def test_a_row_not_yet_read_that_moves_to_the_top_fails_closed(self):
        dp.HARRIS_PAGE = 4
        portal = self.portal()

        def bump(p, page_no, served):
            if page_no == 1:
                unread = [r for r in p.rows if r not in served]
                unread[-1]['lastCompletedInspectionDate'] = '2026-10-31'
        portal.after_page = bump
        with self.assertRaises(RuntimeError) as cm:
            self.fetch(since_date=None, portal=portal)
        self.assertIn('incomplete', str(cm.exception))
        self.assertEqual(portal.gets(dp.HARRIS_INSPECTIONS_URL), [])

    def test_a_listing_short_of_total_count_fails_closed(self):
        portal = self.portal()
        portal.total_override = 12
        with self.assertRaises(RuntimeError):
            self.fetch(portal=portal)

    def test_a_page_without_total_count_fails_closed(self):
        portal = self.portal()
        portal.fail[('search', 0)] = [FakeResponse({'data': list(self.rows)})]
        with self.assertRaises(RuntimeError):
            self.fetch(portal=portal)

    def test_an_inspection_shared_by_two_permits_ships_once(self):
        twin = dict(self.row(TMG), documentId='twin-of-tmg')
        self.rows.append(twin)
        self.lists['twin-of-tmg'] = copy.deepcopy(self.lists[TMG])
        records, _, _ = self.fetch()
        self.assertEqual([r['source_id'] for r in records].count('hcph_I0493073'), 1)
        self.assertEqual(set(by_id(records)), IN_WINDOW)


# ── failures ──────────────────────────────────────────────────────────────
class FailureTests(HarrisTestCase):
    def test_a_search_failure_raises_rather_than_ship_a_partial_window(self):
        portal = self.portal()
        portal.fail_all.add('search')
        with self.assertRaises(RuntimeError):
            records, _, sleeps = self.fetch(portal=portal)
        self.assertEqual(len(portal.search_bodies()), dp.HARRIS_RETRIES)
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_backoff_between_attempts(self):
        portal = self.portal()
        portal.fail_all.add('search')
        sleeps = []
        with mock.patch.object(dp.requests, 'Session', return_value=portal), \
                mock.patch.object(dp.time, 'sleep', side_effect=sleeps.append), \
                self.assertRaises(RuntimeError):
            dp.fetch_harris(since_date=SINCE)
        self.assertEqual(sleeps, [dp.HARRIS_BACKOFF, dp.HARRIS_BACKOFF * 3, dp.HARRIS_BACKOFF * 9])

    def test_the_spa_shell_in_place_of_json_is_a_failure(self):
        portal = self.portal()
        portal.fail[('search', 0)] = [FakeResponse('<!doctype html><html>app</html>')] * dp.HARRIS_RETRIES
        with self.assertRaises(RuntimeError):
            self.fetch(portal=portal)

    def test_a_503_is_retried_honouring_retry_after(self):
        portal = self.portal()
        portal.fail[('search', 0)] = [FakeResponse(None, 503, {'Retry-After': '20'})]
        records, portal, sleeps = self.fetch(portal=portal)
        self.assertEqual(set(by_id(records)), IN_WINDOW)
        self.assertIn(20, sleeps)

    def test_an_unreadable_inspection_list_raises(self):
        portal = self.portal()
        portal.fail[('list', HANKS)] = [dp.requests.ConnectionError('reset')] * dp.HARRIS_RETRIES
        with self.assertRaises(RuntimeError) as cm:
            self.fetch(portal=portal)
        self.assertIn('inspection lists could not be read', str(cm.exception))
        self.assertEqual(len(portal.gets(dp.HARRIS_INSPECTIONS_URL, HANKS)), dp.HARRIS_RETRIES)
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_an_establishment_the_api_no_longer_knows_is_skipped_and_held(self):
        portal = self.portal()
        portal.fail[('list', HANKS)] = [FakeResponse(None, 404)]
        with self.assertLogs('dinescores', level='WARNING') as logs:
            records, portal, _ = self.fetch(portal=portal)
        self.assertEqual(set(by_id(records)), IN_WINDOW - {'hcph_I0493169'})
        self.assertEqual(len(portal.gets(dp.HARRIS_INSPECTIONS_URL, HANKS)), 1)      # not retried
        self.assertEqual(dp._FRESHNESS_HOLDS, {SRC: {f'2026-10-30|{HANKS}': (True, False)}})
        self.assertEqual(dp._GROUP_STATUS, {})
        self.assertTrue(any('404/410' in m for m in logs.output))

    def test_an_unreadable_item_list_drops_and_holds_that_inspection(self):
        portal = self.portal()
        portal.fail[('items', 'I0493169')] = [dp.requests.ConnectionError('reset')] * dp.HARRIS_RETRIES
        records, portal, _ = self.fetch(portal=portal)
        self.assertEqual(set(by_id(records)), IN_WINDOW - {'hcph_I0493169'})
        self.assertEqual(len(portal.gets(dp.HARRIS_VIOLATIONS_URL, 'I0493169')), dp.HARRIS_RETRIES)
        self.assertEqual(dp._FRESHNESS_HOLDS, {SRC: {'2026-10-30|I0493169': (True, False)}})
        self.assertEqual(dp._GROUP_STATUS, {})
        self.assertFalse(dp._HARRIS_BLOCKED)

    def test_a_404_item_list_is_a_counted_drop(self):
        portal = self.portal()
        portal.fail[('items', 'I0493169')] = [FakeResponse(None, 404)]
        records, portal, _ = self.fetch(portal=portal)
        self.assertNotIn('hcph_I0493169', by_id(records))
        self.assertEqual(len(portal.gets(dp.HARRIS_VIOLATIONS_URL, 'I0493169')), 1)
        self.assertEqual(dp._FRESHNESS_HOLDS, {SRC: {'2026-10-30|I0493169': (True, False)}})

    def test_the_breaker_trips_in_the_item_phase(self):
        portal = self.portal()
        portal.fail_all.add('items')
        records, portal, _ = self.fetch(portal=portal)
        self.assertTrue(dp._HARRIS_BLOCKED)
        self.assertEqual(set(by_id(records)), {'hcph_I0493090', 'hcph_I0493159'})    # the 100s need no read
        self.assertEqual(len(portal.gets(dp.HARRIS_VIOLATIONS_URL)), dp.HARRIS_BREAKER_TRIP * dp.HARRIS_RETRIES)
        self.assertEqual(dp._GROUP_STATUS[GROUP][0], 'blocked')
        self.assertEqual(dp._FRESHNESS_HOLDS, {SRC: {
            '2026-10-26|I0492917': (True, False), '2026-10-29|I0493043': (True, False),
            '2026-10-29|I0493073': (True, False),                                   # tried and failed
            '2026-10-30|I0493151': (False, False), '2026-10-30|I0493152': (False, False),
            '2026-10-30|I0493169': (False, False), '2026-10-30|I0493186': (False, False)}})  # cut by the breaker
        self.assertEqual(dp._GROUP_NOTES, {})

    def test_the_breaker_trips_in_the_list_phase(self):
        portal = self.portal()
        portal.fail_all.add('list')
        records, portal, _ = self.fetch(portal=portal)
        self.assertEqual(records, [])
        self.assertEqual(len(portal.gets(dp.HARRIS_INSPECTIONS_URL)), dp.HARRIS_BREAKER_TRIP * dp.HARRIS_RETRIES)
        self.assertEqual(dp._GROUP_STATUS[GROUP][0], 'blocked')
        hold_day = (datetime(2026, 10, 20) + timedelta(days=1 + dp.FRESHNESS_OVERLAP_DAYS)).strftime('%Y-%m-%d')
        self.assertEqual(dp._FRESHNESS_HOLDS, {SRC: {f'{hold_day}|window': (False, False)}})
        self.assertEqual(portal.gets(dp.HARRIS_VIOLATIONS_URL), [])

    def test_a_cut_without_a_window_has_nothing_to_hold(self):
        portal = self.portal()
        portal.fail_all.add('list')
        records, _, _ = self.fetch(since_date=None, portal=portal)
        self.assertEqual(records, [])
        self.assertEqual(dp._GROUP_STATUS[GROUP][0], 'blocked')
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_the_deadline_cuts_the_item_phase_oldest_first(self):
        portal = self.portal()
        state = {'cut': False}
        portal.on_items = lambda number: state.__setitem__('cut', True)
        records, portal, _ = self.fetch(portal=portal, past_deadline=lambda: state['cut'])
        self.assertEqual(set(by_id(records)), {'hcph_I0492917', 'hcph_I0493090', 'hcph_I0493159'})
        self.assertEqual([c[2]['id'] for c in portal.gets(dp.HARRIS_VIOLATIONS_URL)], ['I0492917'])
        self.assertEqual(dp._GROUP_STATUS[GROUP][0], 'skipped')
        self.assertEqual(dp._FRESHNESS_HOLDS, {SRC: {
            '2026-10-29|I0493043': (False, False), '2026-10-29|I0493073': (False, False),
            '2026-10-30|I0493151': (False, False), '2026-10-30|I0493152': (False, False),
            '2026-10-30|I0493169': (False, False), '2026-10-30|I0493186': (False, False)}})

    def test_the_deadline_cuts_the_list_phase(self):
        portal = self.portal()
        state = {'cut': False}
        portal.on_list = lambda did: state.__setitem__('cut', True)
        records, portal, _ = self.fetch(portal=portal, past_deadline=lambda: state['cut'])
        self.assertEqual(records, [])
        self.assertEqual(len(portal.gets(dp.HARRIS_INSPECTIONS_URL)), 1)
        self.assertEqual(dp._GROUP_STATUS[GROUP][0], 'skipped')
        self.assertEqual(list(dp._FRESHNESS_HOLDS[SRC]), ['2026-10-22|window'])

    def test_a_deadline_already_past_ships_nothing_and_asks_nothing(self):
        records, portal, _ = self.fetch(past_deadline=lambda: True)
        self.assertEqual(records, [])
        self.assertEqual(portal.calls, [])
        self.assertEqual(dp._GROUP_STATUS[GROUP][0], 'skipped')
        self.assertEqual(list(dp._FRESHNESS_HOLDS[SRC]), ['2026-10-22|window'])

    def test_the_breaker_is_reset_for_a_new_fetch(self):
        dp._HARRIS_BLOCKED, dp._HARRIS_EXHAUSTED_STREAK = True, 3
        records, _, _ = self.fetch()
        self.assertEqual(set(by_id(records)), IN_WINDOW)
        self.assertFalse(dp._HARRIS_BLOCKED)

    def test_a_record_that_fails_once_does_not_trip_the_breaker(self):
        portal = self.portal()
        for number in ('I0492917', 'I0493043'):
            portal.fail[('items', number)] = [dp.requests.ConnectionError('reset')] * dp.HARRIS_RETRIES
        records, _, _ = self.fetch(portal=portal)
        self.assertFalse(dp._HARRIS_BLOCKED)
        self.assertEqual(set(by_id(records)), IN_WINDOW - {'hcph_I0492917', 'hcph_I0493043'})
        self.assertEqual(dp._GROUP_STATUS, {})


# ── order and helpers ─────────────────────────────────────────────────────
class OrderTests(HarrisTestCase):
    def test_records_newest_first_and_same_day_worst_first(self):
        records, _, _ = self.fetch()
        dates = [r['inspection_date'] for r in records]
        self.assertEqual(dates, sorted(dates, reverse=True))
        ids = [r['source_id'] for r in records]
        i, j = ids.index('hcph_I0493152'), ids.index('hcph_I0493151')
        self.assertEqual(j, i + 1)                        # equal visits: the higher source_id first
        self.inspection(MCDONALDS, 'I0493151')['isClosureRequired'] = 'true'
        records, _, _ = self.fetch()
        ids = [r['source_id'] for r in records]
        self.assertLess(ids.index('hcph_I0493151'), ids.index('hcph_I0493152'))   # the closure first
        self.assertEqual(ids[0], 'hcph_I0493151')

    def test_helpers(self):
        self.assertEqual(dp._harris_clean("JOE'S BAR-B-Q  #2"), "Joe's Bar-B-Q #2")
        self.assertEqual(dp._harris_clean(' Mixed  Case '), 'Mixed Case')
        self.assertEqual(dp._harris_clean(None), '')
        for truthy in ('true', 'True', 1, '1', 'Yes'):
            self.assertTrue(dp._harris_flag(truthy), truthy)
        for falsy in ('false', 0, '0', 'No', 'DNA', None, ''):
            self.assertFalse(dp._harris_flag(falsy), falsy)
        self.assertEqual(dp._harris_demerits([{'numberOfDemerits': 2}, {'numberOfDemerits': 'x'}, 'junk',
                                              {'numberOfDemerits': None}]), 2)
        self.assertEqual(dp.HARRIS_GROUP_SEVERITY, {'P': 'priority', 'F': 'priority_foundation', 'C': 'core'})
        self.assertLessEqual(dp.HARRIS_WORKERS if dp.HARRIS_WORKERS != 1 else self._saved['HARRIS_WORKERS'], 4)


if __name__ == '__main__':
    unittest.main()
