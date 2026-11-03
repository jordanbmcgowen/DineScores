"""Tests for the Georgia DPH statewide fetcher (Tyler "publicportals" JSON API
at ga.healthinspections.us/stateofgeorgia). No network: requests.Session is
replaced by a fake portal that answers search/{json}/{page},
inspectionsData/{id} and facilities/0 from tests/fixtures/georgia/ —
trimmed verbatim rows captured from the live API on 2026-11-02 (seven
facilities listed under 2026-10-30 with their two or three newest
inspections, the Renaissance Festival stands' history, the unfiltered list's
first page)."""
import base64
import copy
import json
import logging
import os
import sys
import time
import unittest
from datetime import datetime, timedelta
from unittest import mock
from urllib.parse import unquote

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'georgia')
DAY = '2026-10-30'                      # every fixture facility was last inspected on this day
TODAY = datetime.now().strftime('%Y-%m-%d')
YESTERDAY = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# Fixture facilities, in list order (ids ascending): Fulton, Fulton (senior
# center), Fulton, Cobb, Cobb (middle school), Fulton, DeKalb.
CHEROKEE, SENIOR, MARGARITAS, DUNKIN, SCHOOL, FIN, KADHAI = (
    'ODc1MDE3', 'ODgyNTc2', 'ODgzOTky', 'MzE2NDE1NQ==', 'MzE2NjcwMg==', 'MTI5ODYxMjg=', 'MTMwMDY3MTQ=')
FESTIVAL_ROW = {
    'mapAddress': '6905 VIRLYN B. SMITH \r\nFAIRBURN, GA 30213',
    'columns': {'0': '6905 VIRLYN B. SMITH \r\nFAIRBURN, GA 30213', '1': 'Phone Number: (770) 964-8575',
                '2': 'Permit Type: Food Service', '3': 'Permit Number: FSP-060-007884',
                '4': 'Last Inspection Score: 100', '5': 'Last Inspection Date: 10-31-2026',
                '6': 'For More Information Call: (770) 520-7500 ext. '},
    'id': 'OTQ4OTgwNw==', 'name': 'Renaissance Festival: Beef Jerky #1'}


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _mmddyyyy(day):
    return datetime.strptime(day, '%Y-%m-%d').strftime('%m-%d-%Y')


def _redated(rows, histories, day):
    """Copies of the fixture rows and histories with the listed day moved to
    `day` and every inspection shifted by the same number of days, so the
    gaps between a facility's inspections (Fin Sushi Thai's routine ten days
    before its follow-up) stay what they were live."""
    delta = datetime.strptime(day, '%Y-%m-%d') - datetime.strptime(DAY, '%Y-%m-%d')
    rows = copy.deepcopy(rows)
    histories = copy.deepcopy(histories)
    for r in rows:
        r['columns']['5'] = f'Last Inspection Date: {_mmddyyyy(day)}'
    for hist in histories.values():
        for insp in hist:
            moved = datetime.strptime(dp._tp_date(insp['columns']['0']), '%Y-%m-%d') + delta
            insp['columns']['0'] = f'Date: {moved:%m-%d-%Y}'
    return rows, histories


def _days_before(day, n):
    return (datetime.strptime(day, '%Y-%m-%d') - timedelta(days=n)).strftime('%Y-%m-%d')


class _Resp:
    def __init__(self, payload=None, status=200, text=None):
        self.status_code = status
        self._payload = payload
        self.text = text if text is not None else (json.dumps(payload) if payload is not None else '')
        self.headers = {}

    def json(self):
        if self._payload is None:
            raise ValueError('not JSON')
        return self._payload


class FakePortal:
    """requests.Session stand-in for the Georgia API. `days` maps a day to
    the rows listed under it (all pages, in server order); `histories` maps a
    base64 facility id to its inspection list, an HTTP status to answer, or
    an exception to raise; `excluded` maps a county filter value to rows."""

    def __init__(self, days, histories, newest_rows=None, excluded=None, shift_once=()):
        self.days, self.histories = days, histories
        self.newest_rows = newest_rows if newest_rows is not None else []
        self.excluded = excluded or {}
        self.shift_once = set(shift_once)      # days whose first read comes back reversed
        self.calls = []
        self.searches = []                     # (filters, page)
        self.list_fail = None                  # status to answer every search page with
        self.county_fail = None                # status to answer the GNR county checks with

    def get(self, url, headers=None, timeout=None):
        self.calls.append(url)
        assert headers['User-Agent'] == dp.CHROME_UA
        assert url.startswith(dp.GA_API), url
        path = url[len(dp.GA_API):]
        if path.startswith('search/'):
            enc, page = path[len('search/'):].rsplit('/', 1)
            raw = json.loads(unquote(enc).replace('%2F', '/'))
            filters = {k: base64.b64decode(v).decode('utf-8') if v else '' for k, v in raw.items()}
            page = int(page)
            self.searches.append((filters, page))
            if self.list_fail or (self.county_fail and filters.get('county')):
                return _Resp(None, status=self.list_fail or self.county_fail, text='<html>down</html>')
            rows = self._rows_for(filters)
            return _Resp(rows[page * dp.TP_PAGE_SIZE:(page + 1) * dp.TP_PAGE_SIZE])
        if path.startswith('inspectionsData/'):
            answer = self.histories.get(path[len('inspectionsData/'):], 404)
            if isinstance(answer, Exception):
                raise answer
            if isinstance(answer, int):
                return _Resp(None, status=answer, text='<html>error</html>')
            return _Resp(answer)
        if path == 'facilities/0':
            return _Resp(self.newest_rows)
        raise AssertionError(f'unexpected request {url}')

    def _rows_for(self, filters):
        assert 'keyword' in filters
        assert filters.get('permitType') == 'Food Service'
        if filters.get('county'):
            return self.excluded.get(filters['county'], [])
        start, end = filters.get('from'), filters.get('to')
        if end:                                 # a single-day window: inclusive both ends
            assert start == end, filters
            rows = list(self.days.get(start, []))
            if start in self.shift_once:
                self.shift_once.discard(start)
                rows.reverse()
            return rows
        out = []                                # the open window, newest first
        for d in sorted(self.days, reverse=True):
            if d >= start:
                out.extend(self.days[d])
        return out


def _portal(**kw):
    rows = _load('search_day_p0.json') + _load('search_day_p1.json')
    histories = _load('histories.json')
    day = kw.pop('day', None)
    if day:
        rows, histories = _redated(rows, histories, day)
    kw.setdefault('newest_rows', rows[:5])
    return FakePortal({day or DAY: rows}, histories, **kw)


def _fetch(portal, since=None, limit=None, workers=None):
    """fetch_georgia against the fake portal; workers=1 makes the detail
    call order deterministic for the tests that assert it."""
    with mock.patch.object(dp.requests, 'Session', return_value=portal), \
            mock.patch.object(dp.time, 'sleep'), \
            mock.patch.object(dp, 'TP_WORKERS', workers or dp.TP_WORKERS):
        return dp.fetch_georgia(since_date=since, limit=limit)


def _by_id(records):
    return {r['source_id']: r for r in records}


class GeorgiaTestCase(unittest.TestCase):
    """Save, clear and restore the pipeline globals the fetcher touches."""

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


class RequestShapeTests(GeorgiaTestCase):
    def test_search_url_matches_the_portals_encoding(self):
        url = dp._tp_search_url(dp.GA_API, {'county': 'FULTON', 'permitType': 'Food Service',
                                            'from': '2026-10-26', 'to': '2026-11-02'}, 0)
        # the URL the probe used (values base64, keyword always present, '/' -> %2F, one path segment)
        probe = ('https://ga.healthinspections.us/stateofgeorgia/API/index.cfm/search/'
                 '%7B%22county%22:%22RlVMVE9O%22,%22permitType%22:%22Rm9vZCBTZXJ2aWNl%22,'
                 '%22from%22:%22MjAyNi0xMC0yNg==%22,%22to%22:%22MjAyNi0xMS0wMg==%22,%22keyword%22:%22%22%7D/0')
        self.assertEqual(unquote(url), unquote(probe))
        self.assertTrue(url.endswith('/0'))
        self.assertNotIn('{', url)
        self.assertNotIn('"', url)
        # a '/' inside a base64 value survives one server-side decode as %2F
        url = dp._tp_search_url(dp.GA_API, {'keyword': '???'}, 3)    # base64 'Pz8/' carries a slash
        self.assertIn('Pz8%2F', url)
        self.assertTrue(url.endswith('/3'))

    def test_a_day_is_paged_to_the_empty_terminator_and_days_run_oldest_first(self):
        portal = _portal(day=YESTERDAY)
        records = _fetch(portal, since=YESTERDAY, workers=1)
        day_searches = [(f['from'], p) for f, p in portal.searches if not f.get('county')]
        # yesterday: pages 0, 1 (5 + 2 rows) then the [] page 2; today: page 0 is already []
        self.assertEqual(day_searches, [(YESTERDAY, 0), (YESTERDAY, 1), (YESTERDAY, 2), (TODAY, 0)])
        self.assertEqual(len(records), 6)      # five listed-day inspections + Fin Sushi Thai's routine (see below)
        # one inspectionsData call per listed facility, oldest-listed first (all the same day: id order)
        details = [u.rsplit('/', 1)[1] for u in portal.calls if 'inspectionsData/' in u]
        self.assertEqual(details, [CHEROKEE, SENIOR, MARGARITAS, DUNKIN, SCHOOL, FIN, KADHAI])

    def test_since_date_in_both_formats_and_rows_before_it_are_excluded(self):
        for since in (YESTERDAY, f'{YESTERDAY}T00:00:00'):
            records = _fetch(_portal(day=YESTERDAY), since=since)
            by_id = _by_id(records)
            self.assertEqual(sorted(by_id), ['ga_57558159', 'ga_57560231', 'ga_57560247', 'ga_57560275',
                                             'ga_57560333', 'ga_57560379'], since)
            # the five inspections listed under the day, plus the re-read margin: Fin Sushi
            # Thai's routine (76, a C) ten days before its follow-up ships with it, so a
            # facility a failed or cut run dropped loses nothing when it is re-read
            self.assertEqual(sum(r['inspection_date'] == YESTERDAY for r in records), 5)
            self.assertEqual(by_id['ga_57558159']['inspection_date'], _days_before(YESTERDAY, 10))
            self.assertTrue(all(r['inspection_date'] >= _days_before(YESTERDAY, dp.TP_REREAD_MARGIN_DAYS)
                                for r in records))
            self.assertNotIn('ga_57513167', by_id)                 # Fin's February routine stays out

    def test_no_since_means_one_open_window_from_the_history_floor_newest_first(self):
        portal = _portal()
        records = _fetch(portal)
        self.assertEqual(portal.searches[0][0], {'permitType': 'Food Service', 'from': dp.GA_HISTORY_START,
                                                 'keyword': ''})
        self.assertEqual([p for f, p in portal.searches if not f.get('county')], [0, 1, 2])
        # every inspection of the five food facilities, including their older history
        self.assertEqual(len(records), 12)
        self.assertEqual(records[0]['inspection_date'], DAY)              # newest first
        self.assertIn('ga_57513167', _by_id(records))                     # Fin Sushi Thai, 2026-02-17

    def test_limit_bounds_the_listing_and_the_records_and_leaves_no_lag_note(self):
        portal = _portal()
        records = _fetch(portal, limit=2)
        self.assertEqual(len(records), 2)
        self.assertEqual([p for f, p in portal.searches if not f.get('county')], [0])  # 5 rows >= 2
        self.assertFalse(any(u.endswith('facilities/0') for u in portal.calls))
        self.assertNotIn('Georgia', dp._GROUP_NOTES)
        # the records dropped by the limit hold the window (uncounted) at the newest dropped date
        self.assertEqual(dp._FRESHNESS_HOLDS, {'Georgia DPH': {f'{DAY}|': (False, False)}})

    def test_the_gnr_counties_are_checked_and_their_facilities_dropped(self):
        rows = _load('search_day_p0.json')
        portal = _portal(excluded={'GWINNETT': [rows[3]]})           # the portal "lists" Dunkin under Gwinnett
        records = _fetch(portal)
        counties = sorted(f['county'] for f, p in portal.searches if f.get('county'))
        self.assertEqual(counties, ['GWINNETT', 'GWINNETT', 'NEWTON', 'ROCKDALE'])   # Gwinnett paged to its []
        self.assertNotIn('ga_57560231', _by_id(records))
        self.assertIn('ga_57560247', _by_id(records))

    def test_a_facility_listed_on_two_days_is_read_once(self):
        rows = _load('search_day_p0.json') + _load('search_day_p1.json')
        histories = _load('histories.json')
        yrows, yhist = _redated(rows, histories, YESTERDAY)
        trows, _ = _redated(rows, histories, TODAY)
        portal = FakePortal({YESTERDAY: yrows, TODAY: [trows[6]]}, yhist, newest_rows=yrows[:5])
        records = _fetch(portal, since=YESTERDAY)
        details = [u for u in portal.calls if 'inspectionsData/' in u]
        self.assertEqual(len(details), 7)
        ids = [r['source_id'] for r in records]
        self.assertEqual(len(ids), len(set(ids)))


class RecordTests(GeorgiaTestCase):
    def setUp(self):
        super().setUp()
        self.records = _fetch(_portal())
        self.by_id = _by_id(self.records)

    def test_record_dict(self):
        self.assertEqual(self.by_id['ga_57560333'], {
            'name': 'Kadhai Kitchn (Caterer)',
            'address': '3300 Marjan Dr',
            'city': 'Atlanta',
            'state': 'GA',
            'zip': '30340',
            'latitude': None,
            'longitude': None,
            'inspection_date': '2026-10-30',
            'original_score': 88,
            'risk_score': 88,
            'priority_violations': 1,
            'priority_foundation_violations': 0,
            'core_violations': 1,
            'total_violations': 2,
            'violations': [
                ['temperature_control', 'priority',
                 'Proper hot holding temperatures: Observed 2 pans of aloo tikki hot holding below 135F '
                 'in warmer cabinet. COS: PIC placed pans in oven to reheat'],
                ['personal_hygiene', 'core',
                 'Personal cleanliness: Observed several employees working with unpackaged foods with '
                 'wrist jewelry on. COS: Watches removed but reminded PIC that woven/permanent bracelets '
                 'shall be removed too'],
            ],
            'inspection_type': 'Routine',
            'results': '',
            'source': 'Georgia DPH',
            'source_url': 'https://ga.healthinspections.us/_templates/87/food_2015/_report_full.cfm?'
                          'fsimID=57560333&rtype=food_2015&&reportIDParent=0&userID=0&SubType=&dsn=DHD_87&domainid=87',
            'source_id': 'ga_57560333',
            'metro': 'Atlanta',
        })
        for r in self.records:
            self.assertEqual(len(r), 21)
            for cat, sev, text in r['violations']:
                self.assertIn(sev, ('priority', 'priority_foundation', 'core'))
                self.assertEqual(cat, dp.classify_violation(text)[0])
                self.assertLessEqual(len(text), 500)
                self.assertNotIn('\n', text)
                self.assertNotIn('Inspector Notes', text)
                self.assertNotIn('511-6-1', text)                   # rule text never ships

    def test_official_score_is_copied_and_grades_follow_the_states_cutoffs(self):
        low = self.by_id['ga_57558159']                              # Fin Sushi Thai routine, 76
        self.assertEqual((low['original_score'], low['risk_score']), (76, 76))
        self.assertEqual(dp.calculate_vetted_grade(low['risk_score']), 'C')
        self.assertEqual(dp.calculate_vetted_grade(self.by_id['ga_57560333']['risk_score']), 'B')
        self.assertEqual(dp.calculate_vetted_grade(self.by_id['ga_57560231']['risk_score']), 'A')
        self.assertTrue(all(0 <= r['risk_score'] <= 100 for r in self.records))

    def test_severity_comes_from_the_citation_designation_with_a_points_fallback(self):
        low = self.by_id['ga_57558159']
        # (p) 9pts, (pf), (p, pf), (c), (c), (c), (p, c): highest designation wins
        self.assertEqual([v[1] for v in low['violations']],
                         ['priority', 'priority_foundation', 'priority', 'core', 'core', 'core', 'priority'])
        self.assertEqual((low['priority_violations'], low['priority_foundation_violations'],
                          low['core_violations'], low['total_violations']), (3, 1, 3, 7))
        self.assertTrue(low['violations'][0][2].endswith(' (repeat violation)'))
        # null citations fall back to the Points value: 9 -> priority, 1 -> core
        older = self.by_id['ga_57513167']
        self.assertEqual([v[1] for v in older['violations']], ['priority', 'core'])
        self.assertEqual(dp._ga_severity('511-6-1.05(6)(a) - good repair ©', 'Points: 1'), 'core')
        self.assertEqual(dp._ga_severity(None, 'Points: 4'), 'priority_foundation')
        self.assertEqual(dp._ga_severity('x (pf, c)', 'Points: 9'), 'priority_foundation')
        self.assertEqual(dp._ga_severity('x (P, C)', 'Points: 1'), 'priority')

    def test_observation_text_drops_corrective_action_and_rule_boilerplate(self):
        self.assertEqual(dp._ga_observation(
            'Inspector Notes: Violation of Code: [511-6-1.05(7)(b)] Observed mold like residue inside the '
            'ice machine.\r\n CA: Ensure that all food contact surfaces are cleaned and sanitized.'),
            'Observed mold like residue inside the ice machine')
        self.assertEqual(dp._ga_observation(
            'Inspector Notes: CA: Cleaning of Equipment and Utensils.\r\n1. Equipment food-contact surfaces '
            'shall be clean to sight and touch. Pf.\r\nOBSERVED: raw chicken in the produce sink.\r\nCOS: sink sanitized.'),
            'OBSERVED: raw chicken in the produce sink. COS: sink sanitized')
        self.assertEqual(dp._ga_observation(
            'Inspector Notes: Observed box of to-go cups stored on the floor. 511-6-1.04(4)(z) - Miscellaneous '
            'Sources of Contamination (C)\r\n(z) Food shall be protected from contamination'),
            'Observed box of to-go cups stored on the floor')
        self.assertEqual(dp._ga_observation('Inspector Notes: Observed debris in soda nozzle. Ca- clean soda nozzles.'),
                         'Observed debris in soda nozzle')
        self.assertEqual(dp._ga_observation('Inspector Notes: Observed mildew-like substance on racks. RCA: Nonfood-contact '
                                            'surfaces of equipment shall be cleaned at a frequency necessary.'),
                         'Observed mildew-like substance on racks')
        self.assertEqual(dp._ga_observation('Inspector Notes: Observed cold TCS food above 41 F. COS. Discarded. Code: TCS '
                                            'food that uses cold holding shall be held at 41 F or below'),
                         'Observed cold TCS food above 41 F. COS. Discarded')
        self.assertEqual(dp._ga_observation('Inspector Notes: Observed no hand soap at hand sink. Rule: A handwashing sink '
                                            'shall be maintained so that it is accessible at all times'),
                         'Observed no hand soap at hand sink')
        self.assertEqual(dp._ga_observation('Inspector Notes:'), '')
        self.assertEqual(dp._ga_observation(None), '')
        self.assertEqual(dp._ga_observation('Inspector Notes: Observed no certified food safety manager. '
                                            '511-6-1-.03(1)(c) - Food Safety Manager Certification (Pf) The PIC shall'),
                         'Observed no certified food safety manager')
        # inspectors' contact details never ship (8 of 3,045 live lines carried an e-mail)
        self.assertEqual(dp._ga_observation('Inspector Notes: Observed no CFSM certificate. Email verification to '
                                            'Bernotta.Linder@dph.ga.gov or call (404) 555-0199 within 30 days.'),
                         'Observed no CFSM certificate. Email verification to [email] or call [phone] within 30 days')
        # the pasted rule's pest wording must never read as evidence; a real sighting still does
        self.assertFalse(dp.has_active_pest_or_sewage(dp._ga_observation(
            'Inspector Notes: CA: The premises shall be kept free of insects and rodents by routinely '
            'inspecting for evidence of rodents.')))
        roach = self.by_id['ga_57560379']['violations'][1][2]
        self.assertTrue(roach.startswith('Insects, rodents, and animals not present: Observed a roach carcass'))
        self.assertTrue(dp.has_active_pest_or_sewage(roach))
        self.assertEqual(dp._ga_item_title('6-1B - proper hot holding temperatures'), 'Proper hot holding temperatures')
        self.assertEqual(dp._ga_item_title('18 - insects, rodents, and animals not present'),
                         'Insects, rodents, and animals not present')

    def test_a_bare_title_ships_when_the_notes_are_empty(self):
        v = dp._ga_violation(['6-1A - proper cold holding temperatures', '511-6-1.04(6)(f) - cold holding (p)',
                              'Points: 9', 'Corrected during inspection?: No', 'Repeat: No', 'Inspector Notes:'])
        self.assertEqual(v, ['temperature_control', 'priority', 'Proper cold holding temperatures'])
        self.assertIsNone(dp._ga_violation('garbage'))

    def test_metro_comes_from_the_county_eh_phone(self):
        self.assertEqual(self.by_id['ga_57560231']['metro'], 'Atlanta')   # Cobb (770) 435-7815
        self.assertEqual(self.by_id['ga_57560247']['metro'], 'Atlanta')   # Fulton (770) 520-7500
        row = copy.deepcopy(_load('search_day_p0.json')[0])
        row['columns']['6'] = 'For More Information Call: (912) 356-2160'    # Chatham County
        recs, _ = dp._ga_build(row, _load('histories.json')[CHEROKEE], None, DAY)
        self.assertEqual({r['metro'] for r in recs}, {''})

    def test_institutional_kitchens_and_temporary_booths_are_skipped(self):
        self.assertNotIn('ga_57560290', self.by_id)                 # Southeast Neighborhood Senior Center
        self.assertNotIn('ga_57560268', self.by_id)                 # McClure Middle School
        self.assertTrue(dp._tp_institutional('McCLURE MIDDLE SCHOOL'))
        self.assertTrue(dp._tp_institutional('Wilson Hospice House'))
        self.assertFalse(dp._tp_institutional('Old School Diner'))
        self.assertFalse(dp._tp_institutional('Hospitality House Grill'))
        self.assertFalse(dp._tp_institutional('Cherokee Country Club - Kitchen'))
        # festival stands: every inspection is 'Temporary'
        recs, holds = dp._ga_build(FESTIVAL_ROW, _load('inspections_temporary.json'), None, '2026-10-31')
        self.assertEqual((recs, holds), ([], []))
        # a non-Food-Service row never ships
        row = copy.deepcopy(_load('search_day_p0.json')[0])
        row['columns']['2'] = 'Permit Type: Swimming Pool'
        self.assertEqual(dp._ga_build(row, _load('histories.json')[CHEROKEE], None, DAY), ([], []))

    def test_coordinates_are_none_for_the_geocoder(self):
        self.assertTrue(all(r['latitude'] is None and r['longitude'] is None for r in self.records))

    def test_name_and_address_normalisation_keeps_ids_stable(self):
        again = _by_id(_fetch(_portal()))
        for sid, r in self.by_id.items():
            self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']),
                             dp.make_restaurant_id(again[sid]['name'], again[sid]['address'], again[sid]['city']))
        # an ALL-CAPS row and a mixed-case one land on the same id
        row = copy.deepcopy(_load('search_day_p1.json')[0])            # 'Fin Sushi Thai'
        upper = copy.deepcopy(row)
        upper['name'] = '  FIN  SUSHI THAI '
        upper['mapAddress'] = '10955 JONES  BRIDGE RD STE 111 \r\nJOHNS CREEK, GA 30022-1234'
        a, _ = dp._ga_build(row, _load('histories.json')[FIN], None, DAY)
        b, _ = dp._ga_build(upper, _load('histories.json')[FIN], None, DAY)
        self.assertEqual(dp.make_restaurant_id(a[0]['name'], a[0]['address'], a[0]['city']),
                         dp.make_restaurant_id(b[0]['name'], b[0]['address'], b[0]['city']))
        self.assertEqual((b[0]['name'], b[0]['address'], b[0]['city'], b[0]['zip']),
                         ('Fin Sushi Thai', '10955 Jones Bridge Rd Ste 111', 'Johns Creek', '30022'))
        self.assertEqual(dp.make_restaurant_id(a[0]['name'], a[0]['address'], a[0]['city']),
                         dp.make_restaurant_id('Fin Sushi Thai', '10955 Jones Bridge Rd Ste 111', 'Johns Creek'))
        # every inspection of one facility shares the id; same-day records are worst first
        fin = [r for r in self.records if r['source_id'] in ('ga_57560379', 'ga_57558159', 'ga_57513167')]
        self.assertEqual(len({dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in fin}), 1)
        day_scores = [r['risk_score'] for r in self.records if r['inspection_date'] == DAY]
        self.assertEqual(day_scores, sorted(day_scores))


class FailureTests(GeorgiaTestCase):
    def test_an_unreadable_list_page_raises_instead_of_shipping_a_partial_window(self):
        portal = _portal(day=YESTERDAY)
        portal.list_fail = 503
        with self.assertRaises(RuntimeError):
            _fetch(portal, since=YESTERDAY)
        self.assertEqual(len([u for u in portal.calls if 'search/' in u]), dp.TP_RETRIES)
        self.assertFalse(any('inspectionsData/' in u for u in portal.calls))

    def test_a_day_whose_pages_shifted_is_re_read_and_then_raises(self):
        portal = _portal(day=YESTERDAY, shift_once=[YESTERDAY])
        records = _fetch(portal, since=YESTERDAY)
        self.assertEqual(len(records), 6)
        reads = [p for f, p in portal.searches if f.get('from') == YESTERDAY and not f.get('county')]
        self.assertEqual(reads, [0, 1, 2, 0, 1, 2])                 # the reversed read, then the good one
        rows = _load('search_day_p0.json') + _load('search_day_p1.json')
        self.assertIn('out of id order', dp._tp_order_problem(rows[::-1], DAY))
        self.assertIn('listed twice', dp._tp_order_problem(rows + rows[:1], DAY))
        self.assertIn('not 2026-10-29', dp._tp_order_problem(rows, '2026-10-29'))
        self.assertEqual(dp._tp_order_problem(rows, DAY), '')
        always = _portal(day=YESTERDAY)
        always.days[YESTERDAY].reverse()
        with self.assertRaises(RuntimeError):
            _fetch(always, since=YESTERDAY)

    def test_a_failed_history_drops_that_facility_and_holds_its_day(self):
        portal = _portal(day=YESTERDAY)
        portal.histories[MARGARITAS] = 503
        portal.histories[DUNKIN] = dp.requests.ConnectionError('reset')
        records = _fetch(portal, since=YESTERDAY)
        self.assertEqual(sorted(_by_id(records)), ['ga_57558159', 'ga_57560247', 'ga_57560333', 'ga_57560379'])
        self.assertEqual(portal.calls.count(f'{dp.GA_API}inspectionsData/{MARGARITAS}'), dp.TP_RETRIES)
        self.assertEqual(dp._FRESHNESS_HOLDS, {'Georgia DPH': {f'{YESTERDAY}|883992': (True, False),
                                                                f'{YESTERDAY}|3164155': (True, False)}})
        self.assertNotIn('Georgia', dp._GROUP_STATUS)

    def test_unreadable_violations_and_a_missing_listed_inspection_hold_not_ship(self):
        hist = copy.deepcopy(_load('histories.json')[KADHAI])
        hist[0]['violations'] = 'oops'
        recs, holds = dp._ga_build(_load('search_day_p1.json')[1], hist, None, DAY)
        self.assertEqual([r['source_id'] for r in recs], ['ga_57527667', 'ga_57478980'])
        self.assertEqual(holds, [DAY])
        recs, holds = dp._ga_build(_load('search_day_p1.json')[1], hist[1:], None, DAY)
        self.assertEqual(holds, [DAY])                              # the list promised a 10-30 inspection
        # one entry that is not the 6-string list (a layout change) holds the inspection,
        # it never ships with the readable lines only or with violations []
        hist = copy.deepcopy(_load('histories.json')[KADHAI])
        hist[0]['violations']['1'] = {'item': '6-1A - cold holding', 'notes': 'Observed roaches'}
        recs, holds = dp._ga_build(_load('search_day_p1.json')[1], hist, None, DAY)
        self.assertEqual(([r['source_id'] for r in recs], holds), (['ga_57527667', 'ga_57478980'], [DAY]))

    def test_an_inconsistent_history_entry_is_held_not_shipped(self):
        """Live 2026-10-20: the portal served 'Main Banquet Kitchen @ Omni' a hybrid
        entry — the new date, score 87 and printable under the 2024 inspection's id with
        violations {} — and it shipped as a B with no violation text under a colliding
        id. A deduction with no items, an inspectionId that is not an int or disagrees
        with the printable's fsimID, all hold instead."""
        row = _load('search_day_p1.json')[1]
        base = _load('histories.json')[KADHAI]
        hybrid = copy.deepcopy(base)
        hybrid[0]['violations'] = {}                                 # score 88, no items
        recs, holds = dp._ga_build(row, hybrid, None, DAY)
        self.assertEqual(([r['source_id'] for r in recs], holds), (['ga_57527667', 'ga_57478980'], [DAY]))
        mismatch = copy.deepcopy(base)
        mismatch[0]['printablePath'] = mismatch[0]['printablePath'].replace('fsimID=57560333', 'fsimID=57441668')
        self.assertEqual(dp._ga_build(row, mismatch, None, DAY)[1], [DAY])
        for bad_id in (None, '57560333', True):
            no_id = copy.deepcopy(base)
            no_id[0]['inspectionId'] = bad_id
            recs, holds = dp._ga_build(row, no_id, None, DAY)
            self.assertEqual(holds, [DAY], bad_id)
            self.assertFalse(any(r['source_id'] in ('ga_None', 'ga_True') for r in recs))
        # a 100 with no items is a clean inspection, and a printable without an fsimID is no mismatch
        self.assertEqual([r['source_id'] for r in dp._ga_build(row, base, None, DAY)[0]],
                         ['ga_57560333', 'ga_57527667', 'ga_57478980'])
        no_path = copy.deepcopy(base)
        no_path[0]['printablePath'] = ''
        self.assertEqual(dp._ga_build(row, no_path, None, DAY)[1], [])

    def test_a_read_facility_ships_its_inspections_back_to_the_re_read_margin(self):
        """The list filter is on a facility's LAST inspection, so a facility a failed
        or cut run dropped is re-listed under its newest day only; shipping back to
        TP_REREAD_MARGIN_DAYS before the window start recovers the older in-window
        inspections (the C behind a clean follow-up) when it is re-read."""
        row, hist = _load('search_day_p1.json')[0], _load('histories.json')[FIN]
        ids = lambda ship_from: [r['source_id'] for r in dp._ga_build(row, hist, ship_from, DAY)[0]]
        self.assertEqual(ids(None), ['ga_57560379', 'ga_57558159', 'ga_57513167'])
        self.assertEqual(ids('2026-10-20'), ['ga_57560379', 'ga_57558159'])   # inclusive
        self.assertEqual(ids('2026-10-21'), ['ga_57560379'])
        portal = _portal(day=YESTERDAY)
        portal.histories[FIN] = 503                                  # week 1: Fin's history fails
        self.assertNotIn('ga_57558159', _by_id(_fetch(portal, since=YESTERDAY)))
        self.assertEqual(dp._FRESHNESS_HOLDS, {'Georgia DPH': {f'{YESTERDAY}|12986128': (True, False)}})
        portal = _portal(day=YESTERDAY)                              # week 2: re-read from the held day
        records = _fetch(portal, since=_days_before(YESTERDAY, 2))
        self.assertEqual(_by_id(records)['ga_57558159']['risk_score'], 76)

    def test_many_unreadable_histories_make_the_run_red_or_abandon_it(self):
        """A portal that stops answering fails facility after facility: that is not a
        per-record problem for the three-run give-up to write off. TP_MASS_FAILURE_MIN
        unreadable histories report the group 'error' (records ship, holds count);
        more than half raise and nothing ships or counts."""
        with mock.patch.object(dp, 'TP_MASS_FAILURE_MIN', 2):
            portal = _portal(day=YESTERDAY)
            portal.histories[MARGARITAS] = 503
            portal.histories[DUNKIN] = 503
            records = _fetch(portal, since=YESTERDAY)
            self.assertEqual(len(records), 4)
            self.assertEqual(dp._GROUP_STATUS['Georgia'][0], 'error')
            self.assertIn('2 of 7 facility histories', dp._GROUP_STATUS['Georgia'][1])
            self.assertEqual(len(dp._FRESHNESS_HOLDS['Georgia DPH']), 2)
            dp._GROUP_STATUS.clear()
            dp._FRESHNESS_HOLDS.clear()
            portal = _portal(day=YESTERDAY)
            for fid in (CHEROKEE, MARGARITAS, DUNKIN, KADHAI):
                portal.histories[fid] = 503
            with self.assertRaises(RuntimeError) as cm:
                _fetch(portal, since=YESTERDAY)
            self.assertIn('4 of 7 facility histories', str(cm.exception))
            self.assertEqual(dp._FRESHNESS_HOLDS, {})
            self.assertNotIn('Georgia', dp._GROUP_STATUS)

    def test_the_deadline_inside_the_retry_ladder_is_a_cut_not_a_failure(self):
        # the deadline passes while a facility's history is being retried: an uncounted
        # hold and a 'skipped' group, not a counted failure (and no further retries)
        portal = _portal(day=YESTERDAY)
        portal.histories[CHEROKEE] = 503
        gate = mock.Mock(side_effect=[False, False, False] + [True] * 40)   # two days, work() starts, then past
        with mock.patch.object(dp, '_past_deadline', gate):
            records = _fetch(portal, since=YESTERDAY, workers=1)
        self.assertEqual(records, [])
        self.assertEqual(portal.calls.count(f'{dp.GA_API}inspectionsData/{CHEROKEE}'), 1)
        self.assertEqual(dp._GROUP_STATUS['Georgia'][0], 'skipped')
        self.assertEqual(dp._FRESHNESS_HOLDS['Georgia DPH'][f'{YESTERDAY}|875017'], (False, False))
        self.assertEqual(set(dp._FRESHNESS_HOLDS['Georgia DPH'].values()), {(False, False)})
        # the deadline passes while a day's list page is being retried: that day is held,
        # the group is 'skipped', nothing is listed further — no RuntimeError
        portal = _portal(day=YESTERDAY)
        portal.list_fail = 503
        dp._GROUP_STATUS.clear()
        dp._FRESHNESS_HOLDS.clear()
        with mock.patch.object(dp, '_past_deadline', mock.Mock(side_effect=[False] + [True] * 40)):
            records = _fetch(portal, since=YESTERDAY)
        self.assertEqual(records, [])
        self.assertEqual(len([u for u in portal.calls if 'search/' in u]), 1)
        self.assertEqual(dp._GROUP_STATUS['Georgia'][0], 'skipped')
        self.assertEqual(dp._FRESHNESS_HOLDS, {'Georgia DPH': {f'{YESTERDAY}|': (False, False)}})

    def test_the_gnr_county_check_fails_closed(self):
        portal = _portal(day=YESTERDAY)
        portal.county_fail = 503
        with self.assertRaises(RuntimeError):
            _fetch(portal, since=YESTERDAY)

    def test_the_deadline_cuts_the_detail_phase_with_uncounted_holds_and_a_skipped_status(self):
        portal = _portal(day=YESTERDAY)
        gate = mock.Mock(side_effect=[False, False] + [True] * 20)   # two day windows list, then the deadline
        with mock.patch.object(dp, '_past_deadline', gate):
            records = _fetch(portal, since=YESTERDAY)
        self.assertEqual(records, [])
        self.assertEqual(dp._GROUP_STATUS['Georgia'][0], 'skipped')
        self.assertIn('7 facilities not read', dp._GROUP_STATUS['Georgia'][1])
        holds = dp._FRESHNESS_HOLDS['Georgia DPH']
        self.assertEqual(len(holds), 7)
        self.assertTrue(all(k.startswith(YESTERDAY) and v == (False, False) for k, v in holds.items()))
        self.assertNotIn('Georgia', dp._GROUP_NOTES)

    def test_the_deadline_before_a_day_holds_that_day_and_lists_nothing_more(self):
        portal = _portal(day=YESTERDAY)
        dp._FETCH_DEADLINE = time.time() - 1
        records = _fetch(portal, since=YESTERDAY)
        self.assertEqual(records, [])
        self.assertEqual(dp._GROUP_STATUS['Georgia'][0], 'skipped')
        self.assertEqual(dp._FRESHNESS_HOLDS, {'Georgia DPH': {f'{YESTERDAY}|': (False, False)}})
        self.assertFalse(any('search/' in u for u in portal.calls))


class LagNoteTests(GeorgiaTestCase):
    def test_a_lagging_portal_leaves_a_note_after_a_complete_read(self):
        rows, histories = _redated(_load('search_day_p0.json') + _load('search_day_p1.json'),
                                   _load('histories.json'), '2026-01-15')
        portal = FakePortal({'2026-01-15': rows}, histories, newest_rows=rows[:5])
        records = _fetch(portal)
        self.assertTrue(records)
        self.assertIn('Georgia', dp._GROUP_NOTES)
        self.assertIn('2026-01-15', dp._GROUP_NOTES['Georgia'])
        self.assertTrue(any(u.endswith('facilities/0') for u in portal.calls))

    def test_a_current_portal_leaves_no_note(self):
        _fetch(_portal(day=YESTERDAY), since=YESTERDAY)
        self.assertNotIn('Georgia', dp._GROUP_NOTES)

    def test_an_empty_window_is_an_empty_list_not_an_error(self):
        portal = FakePortal({}, {}, newest_rows=_load('facilities_p0.json'))
        self.assertEqual(_fetch(portal, since=YESTERDAY), [])
        self.assertEqual(dp._tp_newest_listed(portal, {'api': dp.GA_API, 'group': 'Georgia',
                                                       'portal': dp.GA_PORTAL}), '2026-10-31')


class HelperTests(unittest.TestCase):
    def test_address_date_and_id_helpers(self):
        self.assertEqual(dp._tp_parse_address('2651 COBB PKWY SE  \r\nATLANTA, GA 30339-3111', 'GA'),
                         ('2651 Cobb Pkwy Se', 'Atlanta', '30339'))
        self.assertIsNone(dp._tp_parse_address('no second line', 'GA'))
        self.assertEqual(dp._tp_parse_address('1 MAIN ST GA 30301', 'GA', 'ATLANTA'), ('1 Main St', 'Atlanta', '30301'))
        self.assertIsNone(dp._tp_parse_address('1 MAIN ST \r\nMOBILE, AL 36602', 'GA'))
        # three-line rows: the last line is the city line, the middle one is street (a
        # PO Box is a mailing address and is dropped) — two live restaurants had the
        # city 'Po Box 125 Waynesboro' / 'Po Box 4564 Eatonton' before this
        self.assertEqual(dp._tp_parse_address('261 S LIBERTY ST \r\nPO BOX 125 \r\nWAYNESBORO, GA 30830', 'GA'),
                         ('261 S Liberty St', 'Waynesboro', '30830'))
        self.assertEqual(dp._tp_parse_address('123 MAIN ST\r\nSUITE 4\r\nATLANTA, GA 30303', 'GA'),
                         ('123 Main St Suite 4', 'Atlanta', '30303'))
        self.assertEqual(dp._tp_parse_address('P.O. BOX 4564 \r\nEATONTON, GA 31024', 'GA'),
                         ('P.O. Box 4564', 'Eatonton', '31024'))
        self.assertIsNone(dp._tp_parse_address('', 'GA'))
        self.assertIsNone(dp._tp_parse_address('\r\n', 'GA'))
        self.assertEqual(dp._tp_strip_contacts('mail jane.doe@dph.ga.gov, call 404-555-0100 or (404) 555-0101; '
                                               'rule 511-6-1.05 at 41 F on 10-30-2026'),
                         'mail [email], call [phone] or [phone]; rule 511-6-1.05 at 41 F on 10-30-2026')
        self.assertEqual(dp._tp_title("JOE'S  DINER"), "Joe's Diner")
        self.assertEqual(dp._tp_title('Publix Falcons activation  10/31'), 'Publix Falcons activation 10/31')
        self.assertEqual(dp._tp_date('Date: 10-30-2026'), '2026-10-30')
        self.assertEqual(dp._tp_date('Last Inspection Date: 10-31-2026'), '2026-10-31')
        self.assertIsNone(dp._tp_date('Date: 13-45-2026'))
        self.assertIsNone(dp._tp_date(''))
        self.assertEqual(dp._tp_facility_id('OTQ4OTgwNw=='), 9489807)
        self.assertIsNone(dp._tp_facility_id('9489807'))
        self.assertIsNone(dp._tp_facility_id(None))
        self.assertEqual(dp._tp_abs_url(dp.GA_PORTAL, '../../_templates/87/x.cfm?fsimID=1&dsn=DHD_87'),
                         'https://ga.healthinspections.us/_templates/87/x.cfm?fsimID=1&dsn=DHD_87')
        self.assertEqual(dp._tp_abs_url(dp.GA_PORTAL, ''), dp.GA_PORTAL)


if __name__ == '__main__':
    unittest.main()
