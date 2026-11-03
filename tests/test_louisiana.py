"""Tests for the Louisiana LDH statewide fetcher (Tyler "publicportals" JSON
API at la.healthinspections.us). No network: requests.Session is replaced by
a fake portal answering search/{json}/{page}, inspectionsData/{id},
facilities/0 and the printable notices from tests/fixtures/louisiana/ —
trimmed verbatim captures from the live API on 2026-11-02 (the first page of
the facility list, the last page, Jay's Bar B Que's seven inspections with
the explicit clean marker, a school cafeteria's history, two printable
notices, the portal's 500 page) plus two short synthetic histories for the
listed facilities whose histories were not captured."""
import base64
import copy
import json
import logging
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest import mock
from urllib.parse import unquote

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'louisiana')
DAY = '2026-10-26'                      # every fixture facility was last inspected on this day
TODAY = datetime.now().strftime('%Y-%m-%d')
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# Listed facilities, in list order (ids ascending): DeRidder Junior High
# School, Fausto's Fried Chicken, Pinewood Elementary, Cuccio's Food Mart,
# Wedgewood Elementary, Jay's Bar B Que.
JUNIOR_HIGH, FAUSTOS, PINEWOOD, CUCCIOS, WEDGEWOOD, JAYS = (
    'MjUwMA==', 'MjU5Nw==', 'MzAzMg==', 'NDMyOQ==', 'NTg0OQ==', 'NTk3MQ==')
PRINTABLE = ('https://la.healthinspections.us/paper/_paper_inspection_form.cfm?inspectionID={}&'
             'parentTableName=tblInspection&dsn=DHD_626&domainid=626&wguid=1367&wgunm=sysuser&wgdmn=626&'
             'logintime=%2200:00:00%20AM%22&public=1')


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _text(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return fh.read()


def _printable_path(insp_id):
    return (f'/paper/_paper_inspection_form.cfm?inspectionID={insp_id}&parentTableName=tblInspection&'
            f'dsn=DHD_626&domainid=626&wguid=1367&wgunm=sysuser&wgdmn=626&logintime="00:00:00 AM"&public=1')


def _inspection(insp_id, fid, date, permit_name, purpose, violations):
    return {'printablePath': _printable_path(insp_id), 'inspectionId': insp_id, 'facilityId': fid,
            'columns': {'0': date, '1': f'Permit Name: {permit_name}', '2': f'Inspection Purpose: {purpose}'},
            'violations': violations}


# Synthetic histories (the probe did not capture these two facilities).
FAUSTOS_HISTORY = [
    _inspection(151712900, FAUSTOS, '10-26-2026', "Fausto's Fried Chicken", 'Routine/Renewal',
                {'0': ['Critical Items', '9 - cold holding temperature', None],
                 '1': ['Non-Critical Items', '105 - floors', ' - Repeat'],
                 '2': ['Non-Critical Items', '58 - insects/rodents', None]}),
    _inspection(151600000, FAUSTOS, '04-02-2026', "Fausto's Fried Chicken", 'Routine/Renewal', {'0': ['None']}),
]
CUCCIOS_HISTORY = [
    _inspection(151712901, CUCCIOS, '10-26-2026', "CUCCIO'S FOOD MART Grocery", 'Routine/Renewal', {'0': ['None']}),
    _inspection(151712902, CUCCIOS, '10-26-2026', "CUCCIO'S FOOD MART Deli", 'Routine/Renewal',
                {'0': ['Non-Critical Items', '75 - non-food contact surfaces clean', ' - Corrected']}),
]


def _histories():
    school = _load('inspections_school.json')
    return {JUNIOR_HIGH: school, PINEWOOD: copy.deepcopy(school), WEDGEWOOD: copy.deepcopy(school),
            FAUSTOS: copy.deepcopy(FAUSTOS_HISTORY), CUCCIOS: copy.deepcopy(CUCCIOS_HISTORY),
            JAYS: _load('inspections_restaurant.json')}


def _mmddyyyy(day):
    return datetime.strptime(day, '%Y-%m-%d').strftime('%m-%d-%Y')


def _redated(rows, histories, day):
    rows = copy.deepcopy(rows)
    histories = copy.deepcopy(histories)
    for r in rows:
        r['columns']['1'] = f'Last Inspection Date: {_mmddyyyy(day)}'
    for hist in histories.values():
        for insp in hist:
            if insp['columns']['0'] == _mmddyyyy(DAY):
                insp['columns']['0'] = _mmddyyyy(day)
    return rows, histories


class _Resp:
    def __init__(self, payload=None, status=200, text=None, headers=None):
        self.status_code = status
        self._payload = payload
        self.text = text if text is not None else (json.dumps(payload) if payload is not None else '')
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError('not JSON')
        return self._payload


class FakePortal:
    """requests.Session stand-in for the Louisiana API and its printables.
    `printables` maps a printable URL to an HTML string, a status int, a list
    of per-call answers (consumed in order) or an exception."""

    def __init__(self, days, histories, newest_rows=None, printables=None):
        self.days, self.histories = days, histories
        self.newest_rows = newest_rows if newest_rows is not None else []
        self.printables = printables or {}
        self.calls = []
        self.searches = []
        self.list_fail = None

    def get(self, url, headers=None, timeout=None):
        self.calls.append(url)
        assert headers['User-Agent'] == dp.CHROME_UA
        if url.startswith(dp.LA_API):
            return self._api(url[len(dp.LA_API):])
        if '/paper/' in url:
            return self._printable(url)
        raise AssertionError(f'unexpected request {url}')

    def _api(self, path):
        if path.startswith('search/'):
            enc, page = path[len('search/'):].rsplit('/', 1)
            raw = json.loads(unquote(enc).replace('%2F', '/'))
            filters = {k: base64.b64decode(v).decode('utf-8') if v else '' for k, v in raw.items()}
            page = int(page)
            self.searches.append((filters, page))
            assert 'keyword' in raw, 'the server answers 500 without the keyword key'
            if self.list_fail:
                return _Resp(None, status=self.list_fail, text='<html>down</html>')
            rows = self._rows_for(filters)
            return _Resp(rows[page * dp.TP_PAGE_SIZE:(page + 1) * dp.TP_PAGE_SIZE])
        if path.startswith('inspectionsData/'):
            answer = self.histories.get(path[len('inspectionsData/'):], 404)
            if isinstance(answer, Exception):
                raise answer
            if isinstance(answer, int):
                return _Resp(None, status=answer, text=_text('error_500.html'))
            return _Resp(answer)
        if path == 'facilities/0':
            return _Resp(self.newest_rows)
        raise AssertionError(f'unexpected API request {path}')

    def _rows_for(self, filters):
        start = datetime.strptime(filters['from'], '%Y-%m-%d')
        if filters.get('to'):                   # strictly exclusive bounds: one day is (day-1, day+1)
            end = datetime.strptime(filters['to'], '%Y-%m-%d')
            assert (end - start).days == 2, filters
            return list(self.days.get((start + timedelta(days=1)).strftime('%Y-%m-%d'), []))
        out = []                                # the open window: every day after `from`, newest first
        for d in sorted(self.days, reverse=True):
            if d > filters['from']:
                out.extend(self.days[d])
        return out

    def _printable(self, url):
        answer = self.printables.get(url, 404)
        if isinstance(answer, list):
            answer = answer.pop(0) if len(answer) > 1 else answer[0]
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, int):
            return _Resp(None, status=answer, text='The service is unavailable.',
                         headers={'Retry-After': '3'} if answer == 503 else {})
        return _Resp(None, text=answer)


def _portal(day=None, printables=None):
    rows = _load('search_day_p0.json') + _load('search_day_p1.json')
    histories = _histories()
    if day:
        rows, histories = _redated(rows, histories, day)
    if printables is None:
        printables = {PRINTABLE.format(151712897): _text('printable_violations.html'),
                      PRINTABLE.format(151712675): _text('printable_single.html')}
    return FakePortal({day or DAY: rows}, histories, newest_rows=rows[:5], printables=printables)


def _fetch(portal, since=None, limit=None, workers=None):
    """fetch_louisiana against the fake portal; workers=1 makes the detail
    call order deterministic for the tests that assert it."""
    with mock.patch.object(dp.requests, 'Session', return_value=portal), \
            mock.patch.object(dp.time, 'sleep'), \
            mock.patch.object(dp, 'TP_WORKERS', workers or dp.TP_WORKERS):
        return dp.fetch_louisiana(since_date=since, limit=limit)


def _by_id(records):
    return {r['source_id']: r for r in records}


class LouisianaTestCase(unittest.TestCase):
    def setUp(self):
        self._saved = (dict(dp._GROUP_NOTES), dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
                       dp._FRESHNESS, dp._FETCH_DEADLINE, dict(dp._TP_STATS))
        dp._GROUP_NOTES.clear()
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._FRESHNESS = None
        dp._FETCH_DEADLINE = None
        self._level = dp.log.level
        dp.log.setLevel(logging.WARNING)

    def tearDown(self):
        dp.log.setLevel(self._level)
        notes, holds, status, fresh, deadline, stats = self._saved
        dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(notes)
        dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(holds)
        dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(status)
        dp._TP_STATS.clear(); dp._TP_STATS.update(stats)
        dp._FRESHNESS = fresh
        dp._FETCH_DEADLINE = deadline


class RequestShapeTests(LouisianaTestCase):
    def test_search_url_matches_the_probe_and_always_carries_the_keyword(self):
        url = dp._tp_search_url(dp.LA_API, {'keyword': '', 'from': '2026-10-18', 'to': '2026-10-27'}, 0)
        self.assertEqual(url, 'https://la.healthinspections.us/API/index.cfm/search/%7B%22keyword%22%3A%22%22'
                              '%2C%22from%22%3A%22MjAyNi0xMC0xOA%3D%3D%22%2C%22to%22%3A%22MjAyNi0xMC0yNw%3D%3D%22%7D/0')
        self.assertIn('keyword', json.loads(unquote(dp._tp_search_url(dp.LA_API, {'from': '2026-10-18'}, 0)
                                                    .rsplit('/', 2)[1])))

    def test_one_day_is_asked_with_exclusive_bounds_and_paged_to_the_terminator(self):
        portal = _portal(day=YESTERDAY)
        records = _fetch(portal, since=YESTERDAY, workers=1)
        before = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        after = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.assertEqual(portal.searches[0], ({'keyword': '', 'from': before, 'to': TODAY}, 0))
        self.assertEqual([(f['from'], p) for f, p in portal.searches],
                         [(before, 0), (before, 1), (before, 2), (YESTERDAY, 0)])
        self.assertEqual(portal.searches[-1][0]['to'], after)
        self.assertEqual(sorted(_by_id(records)),
                         ['lald_151712897', 'lald_151712900', 'lald_151712901', 'lald_151712902'])
        details = [u.rsplit('/', 1)[1] for u in portal.calls if 'inspectionsData/' in u]
        self.assertEqual(details, [JUNIOR_HIGH, FAUSTOS, PINEWOOD, CUCCIOS, WEDGEWOOD, JAYS])

    def test_since_date_in_both_formats_excludes_older_inspections(self):
        for since in (YESTERDAY, f'{YESTERDAY}T00:00:00'):
            records = _fetch(_portal(day=YESTERDAY), since=since)
            self.assertEqual(len(records), 4, since)
            self.assertTrue(all(r['inspection_date'] == YESTERDAY for r in records))
            self.assertNotIn('lald_151684519', _by_id(records))          # Jay's 2026-07-27

    def test_no_since_is_the_open_window_from_the_history_floor(self):
        portal = _portal()
        records = _fetch(portal)
        floor = (datetime.strptime(dp.LA_HISTORY_START, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        self.assertEqual(portal.searches[0][0], {'keyword': '', 'from': floor})
        self.assertEqual([p for _, p in portal.searches], [0, 1, 2])
        self.assertEqual(len(records), 7 + 2 + 2)                   # Jay's whole history + the two others

    def test_limit_bounds_the_read_and_leaves_no_lag_note(self):
        portal = _portal()
        records = _fetch(portal, limit=3)
        self.assertEqual(len(records), 3)
        self.assertEqual([p for _, p in portal.searches], [0])
        self.assertFalse(any(u.endswith('facilities/0') for u in portal.calls))
        self.assertNotIn('Louisiana', dp._GROUP_NOTES)
        self.assertLessEqual(len([u for u in portal.calls if '/paper/' in u]), 3)


class RecordTests(LouisianaTestCase):
    def setUp(self):
        super().setUp()
        self.portal = _portal()
        self.records = _fetch(self.portal)
        self.by_id = _by_id(self.records)

    def test_record_dict_with_the_printables_narrative(self):
        self.assertEqual(self.by_id['lald_151712897'], {
            'name': "Jay's Bar B Que",
            'address': '4215 Government St',
            'city': 'Baton Rouge',
            'state': 'LA',
            'zip': '70806',
            'latitude': None,
            'longitude': None,
            'inspection_date': '2026-10-26',
            'original_score': None,
            'risk_score': 92,
            'priority_violations': 1,
            'priority_foundation_violations': 0,
            'core_violations': 3,
            'total_violations': 4,
            'violations': [
                ['employee_training', 'priority',
                 'Food safety certification: A current state food safety certificate is not held by the '
                 'owner or a designated employee of the establishment. (repeat violation)'],
                ['food_handling', 'core',
                 'Equipment construction, good repair: Non-food contact equipment is not maintained in good '
                 'repair. Gaskets on reach in cooler (repeat violation)'],
                ['food_handling', 'core',
                 'Non-food contact surfaces clean: Non-food contact surfaces of equipment have an accumulation '
                 'of dust, dirt, food residue and other debris. Fryer has an accumulation of grease; '
                 'microwave (repeat violation)'],
                ['food_handling', 'core',
                 'Garbage receptacles: Outside receptacles for garbage, etc. did not have tight fitting lids, '
                 'doors, or covers. (repeat violation)'],
            ],
            'inspection_type': 'Follow-up',
            'results': '',
            'source': 'Louisiana LDH',
            'source_url': PRINTABLE.format(151712897),
            'source_id': 'lald_151712897',
            'metro': 'Baton Rouge',
        })
        for r in self.records:
            self.assertEqual(len(r), 21)
            for cat, sev, text in r['violations']:
                self.assertIn(sev, ('priority', 'core'))
                self.assertEqual(cat, dp.classify_violation(text)[0])
                self.assertLessEqual(len(text), 500)
                self.assertNotIn('\n', text)
                self.assertNotIn('|', text)

    def test_no_score_means_the_risk_is_computed_from_the_states_two_tiers(self):
        r = self.by_id['lald_151712897']
        self.assertEqual(r['risk_score'], dp.calc_risk_score(r['violations'])[0])
        self.assertEqual(r['risk_score'], 100 - 5 * 1 - 1 * 3)
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'A')
        heavy = self.by_id['lald_151633169']                         # 1 critical + 4 non-critical
        self.assertEqual((heavy['risk_score'], heavy['priority_violations'], heavy['core_violations']), (91, 1, 4))
        self.assertTrue(all(r['original_score'] is None for r in self.records))

    def test_the_explicit_clean_marker_ships_as_a_clean_100(self):
        clean = self.by_id['lald_151551920']                         # {'0': ['None']}
        self.assertEqual((clean['violations'], clean['risk_score'], clean['total_violations']), ([], 100, 0))
        self.assertEqual(dp.calculate_vetted_grade(clean['risk_score']), 'A')
        self.assertFalse(any(PRINTABLE.format(151551920) == u for u in self.portal.calls))   # nothing to read

    def test_titles_alone_when_no_printable_was_read(self):
        r = self.by_id['lald_151684519']                             # same four items, no printable fixture
        self.assertEqual([v[2] for v in r['violations']],
                         ['Food safety certification', 'Equipment construction, good repair',
                          'Non-food contact surfaces clean', 'Garbage receptacles'])
        self.assertEqual(r['risk_score'], 92)

    def test_institutional_kitchens_and_summer_feeding_are_skipped(self):
        self.assertFalse(any(r['name'].endswith('School') or 'Elementary' in r['name'] for r in self.records))
        self.assertNotIn('lald_151712675', self.by_id)
        self.assertTrue(dp._tp_institutional('CANDYLAND KINDER KOLLEGE', 'CANDYLAND KINDER KOLLEGE Dietary'))
        self.assertTrue(dp._tp_institutional('CHATEAU TERREBONNE HEALTH CARE', 'nursing home cafeteria'))
        self.assertFalse(dp._tp_institutional('PICCADILLY CAFETERIA', 'PICCADILLY CAFETERIA Restaurant'))
        self.assertFalse(dp._tp_institutional("JAY'S BAR B QUE", "JAY'S BAR B QUE Restaurant"))
        feeding = [_inspection(1, FAUSTOS, '10-26-2026', 'X', 'Summer Feeding', {'0': ['None']})]
        build = dp._la_build_factory({})
        self.assertEqual(build(_load('search_day_p0.json')[1], feeding, None, DAY), ([], []))

    def test_metro_follows_the_parish(self):
        self.assertEqual(self.by_id['lald_151712897']['metro'], 'Baton Rouge')
        self.assertEqual(self.by_id['lald_151712900']['metro'], '')          # Allen Parish
        row = copy.deepcopy(_load('search_day_p0.json')[1])
        row['columns']['2'] = 'Parish: Orleans'
        recs, _ = dp._la_build_factory({})(row, FAUSTOS_HISTORY, None, DAY)
        self.assertEqual({r['metro'] for r in recs}, {'New Orleans'})

    def test_coordinates_are_none_for_the_geocoder(self):
        self.assertTrue(all(r['latitude'] is None and r['longitude'] is None for r in self.records))

    def test_address_variants_seen_live_still_ship(self):
        # six of 1,434 live rows (2026-11-03): one line with no city, or a corrupted zip
        self.assertEqual(dp._tp_parse_address('402 N CARROLLTON AVE LA 70119 ', 'LA', 'NEW ORLEANS'),
                         ('402 N Carrollton Ave', 'New Orleans', '70119'))
        self.assertEqual(dp._tp_parse_address('1120 COOLIDGE  DR STE C LA 70503 ', 'LA', 'LAFAYETTE'),
                         ('1120 Coolidge Dr Ste C', 'Lafayette', '70503'))
        self.assertEqual(dp._tp_parse_address('3830 LA 44 \r\nPAULINA, LA 7.076', 'LA'),
                         ('3830 La 44', 'Paulina', ''))
        self.assertIsNone(dp._tp_parse_address('402 N CARROLLTON AVE LA 70119 ', 'LA'))      # no city anywhere
        self.assertIsNone(dp._tp_parse_address('402 N CARROLLTON AVE TX 70119 ', 'LA', 'HOUSTON'))
        row = copy.deepcopy(_load('search_day_p0.json')[1])
        row['mapAddress'] = row['columns']['0'] = '14514 HIGHWAY 165 LA 70648'
        recs, holds = dp._la_build_factory({})(row, FAUSTOS_HISTORY, None, DAY)
        self.assertEqual((recs[0]['address'], recs[0]['city'], recs[0]['zip']), ('14514 Highway 165', 'Kinder', '70648'))
        self.assertEqual(holds, [])

    def test_ids_are_stable_and_same_day_permits_are_worst_first(self):
        again = _by_id(_fetch(_portal()))
        for sid, r in self.by_id.items():
            self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']),
                             dp.make_restaurant_id(again[sid]['name'], again[sid]['address'], again[sid]['city']))
        jays = [r for r in self.records if r['source_id'].startswith('lald_1517') and r['city'] == 'Baton Rouge']
        self.assertEqual(len({dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in jays}), 1)
        self.assertEqual(dp.make_restaurant_id("Jay's Bar B Que", '4215 Government St', 'Baton Rouge'),
                         dp.make_restaurant_id(self.by_id['lald_151712897']['name'],
                                               self.by_id['lald_151712897']['address'],
                                               self.by_id['lald_151712897']['city']))
        # Cuccio's two permits (grocery, deli) inspected the same day: the one with a finding first
        cuccios = [r['source_id'] for r in self.records if r['name'] == "Cuccio's Food Mart"]
        self.assertEqual(cuccios, ['lald_151712902', 'lald_151712901'])
        self.assertEqual(self.by_id['lald_151712902']['address'], '1025 N Avenue G')
        self.assertEqual(self.by_id['lald_151712902']['city'], 'Crowley')


class PrintableTests(LouisianaTestCase):
    def test_parser_reads_date_and_item_narratives(self):
        parsed = dp._la_parse_printable(_text('printable_violations.html'))
        self.assertEqual(parsed['date'], '2026-10-26')
        self.assertEqual(sorted(parsed['items']), [23, 67, 75, 98])
        self.assertEqual(parsed['items'][75], ['Non-food contact surfaces of equipment have an accumulation of '
                                               'dust, dirt, food residue and other debris. Fryer has an '
                                               'accumulation of grease; microwave'])
        self.assertEqual(dp._la_parse_printable(_text('printable_single.html'))['items'],
                         {1: ['Food meets the definition of adulterated. Dented can of vegetables']})
        self.assertIsNone(dp._la_parse_printable(_text('error_500.html')))
        self.assertIsNone(dp._la_parse_printable(''))

    def test_a_503_is_retried_after_the_hint_and_a_dead_printable_keeps_the_titles(self):
        printables = {PRINTABLE.format(151712897): [503, _text('printable_violations.html')],
                      PRINTABLE.format(151712900): 503}
        portal = _portal(printables=printables)
        with mock.patch.object(dp.requests, 'Session', return_value=portal), \
                mock.patch.object(dp.time, 'sleep') as sleep:
            records = dp.fetch_louisiana()
        by_id = _by_id(records)
        self.assertTrue(by_id['lald_151712897']['violations'][0][2].startswith(
            'Food safety certification: A current state'))
        self.assertEqual(portal.calls.count(PRINTABLE.format(151712897)), 2)
        self.assertEqual(portal.calls.count(PRINTABLE.format(151712900)), dp.LA_PRINTABLE_RETRIES)
        self.assertEqual([v[2] for v in by_id['lald_151712900']['violations']],
                         ['Cold holding temperature', 'Floors (repeat violation)', 'Insects/rodents'])
        self.assertEqual(by_id['lald_151712900']['risk_score'], 93)         # a failed printable changes no score
        self.assertEqual(dp._FRESHNESS_HOLDS, {})                           # and never holds the window
        self.assertIn(mock.call(3.5), sleep.call_args_list)                 # Retry-After 3 + 0.5
        # an inspection whose printable 404s keeps its titles too (one attempt, not retried)
        self.assertEqual(portal.calls.count(PRINTABLE.format(151684519)), 1)
        self.assertEqual(by_id['lald_151684519']['violations'][0][2], 'Food safety certification')

    def test_a_printable_for_another_date_or_a_non_notice_keeps_the_titles(self):
        wrong_day = _text('printable_violations.html').replace('Date<br>10/26/2026', 'Date<br>10/25/2026')
        printables = {PRINTABLE.format(151712897): wrong_day,
                      PRINTABLE.format(151712900): _text('error_500.html')}
        records = _fetch(_portal(day=YESTERDAY, printables=printables), since=YESTERDAY)
        by_id = _by_id(records)
        self.assertEqual(by_id['lald_151712897']['violations'][0][2], 'Food safety certification (repeat violation)')
        self.assertEqual(by_id['lald_151712900']['violations'][0][2], 'Cold holding temperature')

    def test_the_per_run_cap_and_the_deadline_bound_the_narrative_pass(self):
        portal = _portal(day=YESTERDAY)
        with mock.patch.object(dp, 'LA_PRINTABLE_MAX_PER_RUN', 1):
            records = _fetch(portal, since=YESTERDAY)
        self.assertEqual(len([u for u in portal.calls if '/paper/' in u]), 1)
        self.assertEqual(len(records), 4)
        portal = _portal(day=YESTERDAY)
        gate = mock.Mock(side_effect=[False, False] + [False] * 6 + [True] * 10)   # days, histories, then cut
        with mock.patch.object(dp, '_past_deadline', gate):
            records = _fetch(portal, since=YESTERDAY)
        self.assertEqual(len(records), 4)
        self.assertFalse(any('/paper/' in u for u in portal.calls))
        self.assertNotIn('Louisiana', dp._GROUP_STATUS)                      # every inspection still shipped


class FailureTests(LouisianaTestCase):
    def test_an_unreadable_list_page_raises(self):
        portal = _portal(day=YESTERDAY)
        portal.list_fail = 500
        with self.assertRaises(RuntimeError):
            _fetch(portal, since=YESTERDAY)
        self.assertFalse(any('inspectionsData/' in u for u in portal.calls))

    def test_a_failed_history_drops_the_facility_and_holds_its_day(self):
        portal = _portal(day=YESTERDAY)
        portal.histories[FAUSTOS] = 500
        records = _fetch(portal, since=YESTERDAY)
        self.assertEqual(sorted(_by_id(records)), ['lald_151712897', 'lald_151712901', 'lald_151712902'])
        self.assertEqual(portal.calls.count(f'{dp.LA_API}inspectionsData/{FAUSTOS}'), dp.TP_RETRIES)
        self.assertEqual(dp._FRESHNESS_HOLDS, {'Louisiana LDH': {f'{YESTERDAY}|2597': (True, False)}})

    def test_a_malformed_violation_entry_or_missing_field_is_never_clean(self):
        build = dp._la_build_factory({})
        row = _load('search_day_p0.json')[1]
        broken = copy.deepcopy(FAUSTOS_HISTORY)
        broken[0]['violations'] = {'0': ['Critical Items']}             # no item title
        recs, holds = build(row, broken, None, DAY)
        self.assertEqual([r['source_id'] for r in recs], ['lald_151600000'])
        self.assertEqual(holds, [DAY])
        missing = copy.deepcopy(FAUSTOS_HISTORY)
        del missing[0]['violations']
        self.assertEqual(build(row, missing, None, DAY)[1], [DAY])
        unknown = copy.deepcopy(FAUSTOS_HISTORY)
        unknown[0]['violations']['0'][0] = 'Major Items'
        recs, holds = build(row, unknown, None, DAY)
        self.assertEqual(holds, [])
        self.assertEqual(recs[0]['violations'][0][1], 'core')          # an unknown label reads as core, logged

    def test_the_deadline_cuts_the_detail_phase_with_uncounted_holds(self):
        portal = _portal(day=YESTERDAY)
        gate = mock.Mock(side_effect=[False, False] + [True] * 20)
        with mock.patch.object(dp, '_past_deadline', gate):
            records = _fetch(portal, since=YESTERDAY)
        self.assertEqual(records, [])
        self.assertEqual(dp._GROUP_STATUS['Louisiana'][0], 'skipped')
        self.assertEqual(set(dp._FRESHNESS_HOLDS['Louisiana LDH'].values()), {(False, False)})
        self.assertEqual(len(dp._FRESHNESS_HOLDS['Louisiana LDH']), 6)


class LagNoteTests(LouisianaTestCase):
    def test_a_lagging_portal_leaves_a_note_after_a_complete_read(self):
        rows, histories = _redated(_load('search_day_p0.json') + _load('search_day_p1.json'),
                                   _histories(), '2026-01-15')
        portal = FakePortal({'2026-01-15': rows}, histories, newest_rows=rows[:5])
        records = _fetch(portal)
        self.assertTrue(records)
        self.assertIn('Louisiana', dp._GROUP_NOTES)
        self.assertIn('2026-01-15', dp._GROUP_NOTES['Louisiana'])

    def test_a_current_portal_leaves_no_note_and_an_empty_window_is_empty(self):
        _fetch(_portal(day=YESTERDAY), since=YESTERDAY)
        self.assertNotIn('Louisiana', dp._GROUP_NOTES)
        portal = FakePortal({}, {}, newest_rows=_load('facilities_last.json'))
        self.assertEqual(_fetch(portal, since=YESTERDAY), [])
        self.assertEqual(dp._tp_newest_listed(portal, {'api': dp.LA_API, 'group': 'Louisiana',
                                                       'portal': dp.LA_PORTAL}), '2023-11-07')


if __name__ == '__main__':
    unittest.main()
