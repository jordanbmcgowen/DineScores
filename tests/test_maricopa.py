"""Tests for the Phoenix / Maricopa County, AZ fetcher: the envapp.maricopa.gov
zip inventory, permit-page and inspection-detail parsers (both violation
wording generations), the source-stated tiers and computed score, the
record dict, windows, limits, drop-and-hold and the deadline/breaker cuts.
No network: the portal session, the ArcGIS zip list (requests.get) and the
ArcGIS points query (_arcgis_query_all) are mocked with the fixtures under
tests/fixtures/maricopa/, trimmed from pages captured live on 2026-11-02/03
(the <main> element of each page, so the markup the parsers read is
byte-exact; permit pages for permits whose live page was not captured are
single-row pages built from the FD-00570 page's own row markup; search
rows and ArcGIS features are verbatim)."""
import json
import logging
import os
import sys
import time
import unittest
from datetime import datetime, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'maricopa')
SOURCE = 'Maricopa County Environmental Services'
GROUP = 'Maricopa County'


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as fh:
        return fh.read()


def _json(name):
    return json.loads(_load(name))


SEARCHES = {'85004': 'search_85004.json', '85281': 'search_85281.json', '85022': 'search_85022.json'}
PERMITS = {p: f'permit_{p}.html' for p in ('FD-00570', 'FD-00408', 'FD-49153', 'FD-02429', 'FD-00777',
                                            'FD-12738', 'FD-00571', 'FD2602160')}
DETAILS = {
    ('FD-00570', 'INSP-067508-2026'): 'inspection_INSP-067508-2026.html',   # NP: 1 Pf + 2 Core
    ('FD-00408', 'INSP-026373-2026'): 'inspection_zero_violations.html',    # A, empty div#Violations
    ('FD-02429', 'INSP-026626-2024'): 'inspection_INSP-026626-2024.html',   # NP: 3 Priority
    ('FD-00777', 'INSP-055361-2026'): 'inspection_INSP-055361-2026.html',   # B: 1 Priority
    ('FD-12738', '5663423-MCESD'): 'inspection_5663423-MCESD.html',         # legacy wording: Pf + P + C
    ('FD-00571', 'INSP-900001-2026'): 'inspection_INSP-055361-2026.html',   # same day as FD-00570's visit
    ('FD2602160', 'INSP-900002-2026'): 'inspection_zero_violations.html',
}


class FakeResponse:
    def __init__(self, text='', status=200):
        self.text = text
        self.status_code = status
        self.content = text.encode('utf-8')

    def json(self):
        return json.loads(self.text)

    def raise_for_status(self):
        if self.status_code >= 400:
            err = dp.requests.HTTPError(f'{self.status_code}')
            err.response = self
            raise err


class FakePortal:
    """Answers the portal's three endpoints from the fixtures; an override
    maps a zip / permit id / (permit, inspection) to a FakeResponse, an
    exception to raise, a fixture name, or a callable(portal) -> response."""

    def __init__(self):
        self.searches, self.permits, self.details = {}, {}, {}
        self.calls = []                     # (method, url)

    def _answer(self, override, default):
        if override is None:
            return default
        if isinstance(override, Exception):
            raise override
        if callable(override):
            return override(self)
        if isinstance(override, FakeResponse):
            return override
        return FakeResponse(_load(override))

    def post(self, url, data=None, headers=None, timeout=None):
        assert url == dp.MCESD_SEARCH_URL
        zip5 = data['licenseSearchParams[Zip]']
        self.calls.append(('POST', url + '#' + zip5))
        default = FakeResponse(_load(SEARCHES.get(zip5, 'search_empty.json')))
        return self._answer(self.searches.get(zip5), default)

    def get(self, url, headers=None, timeout=None):
        self.calls.append(('GET', url))
        if url.startswith(dp.MCESD_BASE + '/Permit/PermitResults/'):
            pid = url.rsplit('/', 1)[1]
            default = (FakeResponse(_load(PERMITS[pid])) if pid in PERMITS
                       else FakeResponse('<html><body>Server Error</body></html>', 500))
            return self._answer(self.permits.get(pid), default)
        if url.startswith(dp.MCESD_BASE + '/Inspection/'):
            pid, insp = url.rsplit('/', 2)[1:]
            default = FakeResponse(_load(DETAILS.get((pid, insp), 'inspection_not_available.html')))
            return self._answer(self.details.get((pid, insp)), default)
        raise AssertionError(f'unexpected GET {url}')

    def permit_requests(self, pid):
        return sum(1 for m, u in self.calls if u.endswith('/Permit/PermitResults/' + pid))

    def detail_requests(self, pid=None):
        return sum(1 for m, u in self.calls if '/Inspection/' in u and (pid is None or f'/Inspection/{pid}/' in u))


def _fake_arcgis_points(layer_url, where='1=1', out_fields='*', **kw):
    """The captured points for whichever ids the IN (...) clause names."""
    wanted = set(w.strip("'") for w in where[where.index('(') + 1:where.rindex(')')].split(','))
    feats = _json('arcgis_points.json')['features']
    return [f for f in feats if f['attributes']['LicenseNumber'] in wanted]


def _fake_requests_get(url, params=None, headers=None, timeout=None):
    assert url.startswith(dp.MCESD_ARCGIS_LAYER) and params.get('returnDistinctValues') == 'true'
    return FakeResponse(_load('arcgis_zips.json'))


def run_fetch(portal=None, since=None, limit=None, points=_fake_arcgis_points, zips_get=_fake_requests_get):
    portal = portal or FakePortal()
    with mock.patch.object(dp.requests, 'Session', return_value=portal), \
            mock.patch.object(dp.requests, 'get', side_effect=zips_get), \
            mock.patch.object(dp, '_arcgis_query_all', side_effect=points), \
            mock.patch.object(dp.time, 'sleep'):
        records = dp.fetch_maricopa(since_date=since, limit=limit)
    return records, portal


def _by_id(records):
    return {r['source_id']: r for r in records}


def _hold_keys():
    return sorted(dp._FRESHNESS_HOLDS.get(SOURCE, {}))


class MaricopaTestCase(unittest.TestCase):
    """Saves and restores every pipeline global the fetcher touches; the zip
    range is shrunk to three zips and the workers to one so request counts
    and the 'consecutive failures' breaker are deterministic."""

    def setUp(self):
        self._saved = (dp._FRESHNESS, dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
                       dict(dp._GROUP_NOTES), dp._FETCH_DEADLINE, set(dp.SKIP_KNOWN_INSPECTIONS),
                       dict(dp._MCESD_LAST_STATS), dp.log.level)
        dp.log.setLevel(logging.ERROR)
        dp._FRESHNESS = None
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._GROUP_NOTES.clear()
        dp._FETCH_DEADLINE = None
        dp.SKIP_KNOWN_INSPECTIONS.clear()
        # the posting-lag margin and the 3-year history floor are pinned so the
        # fixture's fixed dates give the same windows whenever the suite runs
        for name, value in (('MCESD_ZIP_RANGE', (85003, 85005)), ('MCESD_SEARCH_WORKERS', 1),
                            ('MCESD_LISTING_WORKERS', 1), ('MCESD_DETAIL_WORKERS', 1),
                            ('MCESD_MIN_FOOD_PERMITS', 3), ('MCESD_POSTING_LAG_DAYS', 0),
                            ('MCESD_HISTORY_DAYS', 10000)):
            patcher = mock.patch.object(dp, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self):
        dp._FRESHNESS = self._saved[0]
        dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(self._saved[1])
        dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(self._saved[2])
        dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(self._saved[3])
        dp._FETCH_DEADLINE = self._saved[4]
        dp.SKIP_KNOWN_INSPECTIONS.clear(); dp.SKIP_KNOWN_INSPECTIONS.update(self._saved[5])
        dp._MCESD_LAST_STATS.clear(); dp._MCESD_LAST_STATS.update(self._saved[6])
        dp.log.setLevel(self._saved[7])


# ── parsers ──────────────────────────────────────────────────────────────────

class PermitPageParserTests(unittest.TestCase):
    def test_rows_of_a_live_page(self):
        rows = dp._mcesd_parse_permit_page(_load('permit_FD-00570.html'))
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0], {'date': '8/18/2026', 'purpose': 'Food Routine Inspection',
                                   'grade': 'Not Participating', 'status': 'Completed', 'priority': '0',
                                   'permit': 'FD-00570', 'inspection': 'INSP-067508-2026'})
        self.assertEqual(rows[-1]['inspection'], '5638001-MCESD')        # legacy id form
        grades = {r['grade'] for r in rows}
        self.assertTrue({'Not Participating', '', 'R'} <= grades)       # advisory and re-inspection rows
        self.assertIn('Re-inspection required', {r['status'] for r in rows})

    def test_zero_rows_is_a_complete_page(self):
        self.assertEqual(dp._mcesd_parse_permit_page(_load('permit_FD-49153.html')), [])

    def test_pages_without_the_markers_are_unread(self):
        page = _load('permit_FD-00408.html')
        self.assertIsNone(dp._mcesd_parse_permit_page(''))
        self.assertIsNone(dp._mcesd_parse_permit_page(_load('inspection_not_available.html')))
        self.assertIsNone(dp._mcesd_parse_permit_page(page.replace('Inspections Retrieved', 'Inspections Found')))
        # the marker's count must agree with the table (a layout change
        # that drops rows must not read as a shorter history)
        self.assertIsNone(dp._mcesd_parse_permit_page(page.replace('6 Inspections Retrieved',
                                                                   '7 Inspections Retrieved')))
        self.assertIsNone(dp._mcesd_parse_permit_page(page.replace('<tbody>', '<tbody><tr><td>x</td></tr>')))


class DetailPageParserTests(unittest.TestCase):
    def test_current_wording_priority_lines(self):
        p = dp._mcesd_parse_detail(_load('inspection_INSP-026626-2024.html'))
        self.assertEqual(p['header']['Inspection Date'], '9/26/2024')
        self.assertEqual(p['header']['Grade'], 'Not Participating')
        self.assertEqual(p['header']['Priority Violation'], '3')
        self.assertEqual([v[0] for v in p['violations']], ['9', '23', '28'])
        self.assertEqual([v[2] for v in p['violations']], ['priority'] * 3)
        num, title, sev, text = p['violations'][0]
        self.assertEqual(title, 'No Bare Hand Contact with RTE Foods or a Pre-Approved Alternate Properly Followed')
        # title + the inspector's observation; code citation, AMC-toolbox
        # marketing, predefined comment and corrective action stripped
        self.assertTrue(text.startswith(title + ': Observed waitress use bare hands to grab sliced bread'))
        self.assertTrue(text.endswith('was discarded by PIC at time of inspection.'))
        for needle in ('3-301.11', 'PRIORITY VIOLATION', 'AMC-Toolbox', 'Predefined Comment', 'Corrective Action'):
            self.assertNotIn(needle, text)

    def test_current_wording_foundation_and_core_lines(self):
        p = dp._mcesd_parse_detail(_load('inspection_INSP-067508-2026.html'))
        self.assertEqual([v[2] for v in p['violations']], ['priority_foundation', 'core', 'core'])
        self.assertEqual(p['violations'][2][3],
                         'Wiping Cloths; Properly Used and Stored: Observed large piles of dirty wiping cloths '
                         'beneath bar. Wiping cloths are to be stored clean and dry, in sanitizer solution or be laundered.')

    def test_legacy_wording(self):
        p = dp._mcesd_parse_detail(_load('inspection_5663423-MCESD.html'))
        self.assertEqual([v[2] for v in p['violations']], ['priority_foundation', 'priority', 'core'])
        text = p['violations'][1][3]
        self.assertTrue(text.startswith('Proper Cooling Time and Temperatures: Observed a pan of cooked country gravy'))
        self.assertNotIn('>>>', text)
        self.assertNotIn('Correct By', text)
        self.assertNotIn('3-501.14', text)

    def test_empty_violations_block_is_clean(self):
        p = dp._mcesd_parse_detail(_load('inspection_zero_violations.html'))
        self.assertEqual(p['violations'], [])
        self.assertEqual(p['header']['Grade'], 'A')

    def test_incomplete_pages_are_unread_not_clean(self):
        page = _load('inspection_INSP-055361-2026.html')
        self.assertIsNone(dp._mcesd_parse_detail(''))
        self.assertIsNone(dp._mcesd_parse_detail(_load('inspection_not_available.html')))
        self.assertIsNone(dp._mcesd_parse_detail(page.replace('id="Violations"', 'id="Findings"')))
        self.assertIsNone(dp._mcesd_parse_detail(page.replace('Inspection Status', 'Status')))
        # a tier wording the parser does not know must not read as core
        self.assertIsNone(dp._mcesd_parse_detail(page.replace(' - Priority:', ' - Severe:')))
        # the Priority tally must agree with the header count
        self.assertIsNone(dp._mcesd_parse_detail(page.replace('<br />\n                1\n', '<br />\n                2\n')))
        # text inside the block that is not a violation is a layout change
        self.assertIsNone(dp._mcesd_parse_detail(page.replace('<h2>Violations</h2>', '<h2>Violations</h2><p>Some new block</p>')))

    def test_violation_without_observation_keeps_the_code_phrase(self):
        chunk = ('<p class="mb-0 mt-3"><span class="fw-bold">Violation #:</span> 20</p>'
                 '<p class="mb-0"><span class="fw-bold">Description:</span> Proper Cooling Time and Temperatures </p>'
                 '<p class="commentBlock mb-0"><span class="fw-bold">Comments:</span></p>'
                 "<p class='mb-1'>PRIORITY VIOLATION-3-501.14 - Priority: Cooling.  For a permanent fix to this "
                 "violation, check out these simple tools. https://www.maricopa.gov/4617/AMC-Toolbox---Cooling. </p>")
        self.assertEqual(dp._mcesd_violation(chunk),
                         ('20', 'Proper Cooling Time and Temperatures', 'priority',
                          'Proper Cooling Time and Temperatures: Cooling'))
        self.assertIsNone(dp._mcesd_violation(chunk.replace(' - Priority:', ' - Major:')))


class HelperTests(unittest.TestCase):
    def test_address_split(self):
        cases = {
            '12223 N CAVE CREEK RD PHOENIX AZ 85022': ('12223 N CAVE CREEK RD', 'PHOENIX', '85022'),
            '123 E CAVE CREEK RD CAVE CREEK AZ 85331': ('123 E CAVE CREEK RD', 'CAVE CREEK', '85331'),
            '1 W MAIN ST SUN CITY WEST AZ 85375': ('1 W MAIN ST', 'SUN CITY WEST', '85375'),
            '1 W MAIN ST SUN CITY AZ 85351': ('1 W MAIN ST', 'SUN CITY', '85351'),
            '340 N 3rd ST Phoenix AZ 85004': ('340 N 3rd ST', 'Phoenix', '85004'),
            '100 N 1ST ST PHOENIX AZ 85004-1234': ('100 N 1ST ST', 'PHOENIX', '85004'),
            ' 10  E  BUCKEYE  RD  PHOENIX  AZ  85004 ': ('10 E BUCKEYE RD', 'PHOENIX', '85004'),
            'no state or zip here': ('no state or zip here', '', ''),
            'PHOENIX AZ 85004': ('', 'PHOENIX', '85004'),            # no street: dropped as junk
            '': ('', '', ''),
        }
        for concat, expected in cases.items():
            self.assertEqual(dp._mcesd_split_address(concat), expected, concat)

    def test_permit_filter(self):
        row = {'permitId': 'FD-00570', 'permitType': 'Food Establishment - Eating & Drinking',
               'businessName': 'Renaissance Phoenix Downtown', 'businessAddressConcat': '100 N 1ST ST PHOENIX AZ 85004'}
        self.assertEqual(dp._mcesd_permit(row), {
            'permit': 'FD-00570', 'type': 'Food Establishment - Eating & Drinking',
            'name': 'Renaissance Phoenix Downtown', 'address': '100 N 1St St', 'city': 'Phoenix', 'zip': '85004'})
        for ptype in ('Mobile Food Annual - MF Type 3', 'Food Establishment - Daycare Food Service',
                      'Food Establishment - Food Production', 'Food Establishment - Food Jobber',
                      'Food Establishment - Food Bank', 'Food Establishment - Refrigerated Warehouse'):
            self.assertIsNone(dp._mcesd_permit(dict(row, permitType=ptype)), ptype)
        for ptype in ('Food Establishment - Retail Food', 'Food Establishment - Bakery',
                      'Food Establishment - Meat Market', 'Food Establishment - Micro Market',
                      'Food Establishment - Food Catering'):
            self.assertIsNotNone(dp._mcesd_permit(dict(row, permitType=ptype)), ptype)
        self.assertIsNone(dp._mcesd_permit(dict(row, permitId='MF2403509')))      # a mobile unit
        self.assertIsNone(dp._mcesd_permit(dict(row, businessName='')))
        self.assertIsNone(dp._mcesd_permit(dict(row, businessAddressConcat='PHOENIX AZ 85004')))
        self.assertIsNone(dp._mcesd_permit({}))

    def test_results_text(self):
        cases = [(('A', 'Completed'), 'Grade A'), (('d', 'Completed'), 'Grade D'),
                 (('Cutting Edge A+', 'Completed'), 'Grade A+ (Cutting Edge)'),
                 (('Not Participating', 'Completed'), 'Not Participating'),
                 (('Not Participating', 'Re-inspection required'), 'Not Participating; re-inspection required'),
                 (('R', 'Re-Inspection Completed'), 'Re-inspection'),
                 (('', 'Completed'), ''), (('', 'Re-inspection required'), 'Re-inspection required'),
                 (('NA', 'Completed'), ''), (('Pending', 'Completed'), 'Pending')]
        for (grade, status), expected in cases:
            self.assertEqual(dp._mcesd_results(grade, status), expected, (grade, status))

    def test_dates(self):
        self.assertEqual(dp._mcesd_date('8/18/2026'), '2026-08-18')
        self.assertEqual(dp._mcesd_date(' 10/19/2026 '), '2026-10-19')
        self.assertIsNone(dp._mcesd_date('2026-08-18'))
        self.assertIsNone(dp._mcesd_date('1/1/1999'))
        self.assertIsNone(dp._mcesd_date(''))

    def test_request_retries_and_backoff(self):
        session = mock.Mock()
        session.get.side_effect = [FakeResponse('busy', 429), FakeResponse('ok')]
        with mock.patch.object(dp.time, 'sleep') as sleep:
            r = dp._mcesd_request(session, 'GET', 'https://envapp.maricopa.gov/x')
        self.assertEqual(r.text, 'ok')
        self.assertEqual(session.get.call_count, 2)
        sleep.assert_called_once_with(dp.MCESD_BACKOFF[0])
        session.get.assert_called_with('https://envapp.maricopa.gov/x', headers=mock.ANY, timeout=dp.MCESD_TIMEOUT)
        self.assertEqual(session.get.call_args.kwargs['headers']['User-Agent'], dp.CHROME_UA)
        # a 404 is final at once; exhausted transport errors give None
        session = mock.Mock()
        session.get.return_value = FakeResponse('gone', 404)
        with mock.patch.object(dp.time, 'sleep') as sleep:
            self.assertEqual(dp._mcesd_request(session, 'GET', 'u').status_code, 404)
        self.assertEqual(session.get.call_count, 1)
        sleep.assert_not_called()
        session = mock.Mock()
        session.get.side_effect = dp.requests.ConnectionError('reset')
        with mock.patch.object(dp.time, 'sleep') as sleep:
            self.assertIsNone(dp._mcesd_request(session, 'GET', 'u'))
        self.assertEqual(session.get.call_count, dp.MCESD_ATTEMPTS)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [3, 8])
        # the POST carries the DataTables form field and the XHR header
        session = mock.Mock()
        session.post.return_value = FakeResponse('{"data":[]}')
        dp._mcesd_request(session, 'POST', dp.MCESD_SEARCH_URL, data={'licenseSearchParams[Zip]': '85004'})
        kw = session.post.call_args.kwargs
        self.assertEqual(kw['data'], {'licenseSearchParams[Zip]': '85004'})
        self.assertEqual(kw['headers']['X-Requested-With'], 'XMLHttpRequest')

    def test_pacer_spaces_requests_without_spinning(self):
        pacer = dp._McesdPacer(4.0)
        with mock.patch.object(dp.time, 'sleep') as sleep:
            for _ in range(3):
                pacer.wait()
        # the first slot is immediate, the next two wait their 0.25 s turns
        waits = [round(c.args[0], 2) for c in sleep.call_args_list]
        self.assertEqual(len(waits), 2)
        self.assertTrue(all(0 < w <= 0.5 for w in waits), waits)
        with mock.patch.object(dp.time, 'sleep') as sleep:
            dp._McesdPacer(0).wait()
        sleep.assert_not_called()


# ── end-to-end fetch ──────────────────────────────────────────────────────────

class RecordTests(MaricopaTestCase):
    @classmethod
    def setUpClass(cls):
        # one window that selects only rows with a captured detail page:
        # FD-00570 and FD-00571 (8/18/2026), FD-00408 (10/19/2026), FD2602160 (10/1/2026)
        saved = (dp._FRESHNESS, dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS), dict(dp._GROUP_NOTES), dp.log.level)
        dp._FRESHNESS = None
        dp.log.setLevel(logging.ERROR)
        with mock.patch.object(dp, 'MCESD_ZIP_RANGE', (85003, 85005)), \
                mock.patch.object(dp, 'MCESD_LISTING_WORKERS', 1), \
                mock.patch.object(dp, 'MCESD_DETAIL_WORKERS', 1), \
                mock.patch.object(dp, 'MCESD_MIN_FOOD_PERMITS', 3), \
                mock.patch.object(dp, 'MCESD_POSTING_LAG_DAYS', 0):
            cls.records, cls.portal = run_fetch(since='2026-08-01T00:00:00')
        cls.by_id = _by_id(cls.records)
        dp._FRESHNESS = saved[0]
        dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(saved[1])
        dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(saved[2])
        dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(saved[3])
        dp.log.setLevel(saved[4])

    def test_record_shape(self):
        r = self.by_id['mcesd_FD-00570_INSP-067508-2026']
        self.assertEqual(r, {
            'name': 'Renaissance Phoenix Downtown',
            'address': '100 N 1St St',
            'city': 'Phoenix',
            'state': 'AZ',
            'zip': '85004',
            'latitude': 33.44952599971966,
            'longitude': -112.07278350033016,
            'inspection_date': '2026-08-18',
            'original_score': None,
            'risk_score': 96,
            'priority_violations': 0,
            'priority_foundation_violations': 1,
            'core_violations': 2,
            'total_violations': 3,
            'violations': [
                ['personal_hygiene', 'priority_foundation',
                 'Adequate Handwashing Sinks, Properly Supplied and Accessible: Observed no soap at hand sink '
                 'next to dish washer. PIC provided soap for handwash sink at time of inspection. Handwash sinks '
                 'must be supplied with hot water of at least 100°F, soap and an approved hand drying provision '
                 'at all times. Facilities must be maintained in a condition that promotes handwashing.'],
                ['temperature_control', 'core',
                 'Proper Cooling Methods Used; Adequate Equipment for Temperature Control: Observed chili sauce '
                 'cooling in large covered pot. All TCS foods cooling were uncovered to allow for rapid cooling. '
                 'All TCS foods being cooled in deep containers were transferred to shallow containers to allow '
                 'for rapid cooling. TCS foods must be cooled in: uncovered/loosely covered, shallow containers '
                 'less than 4 inches deep, in equipment designed to rapidly cool, by using an ice bath, by using '
                 'ice as an ingredient, or ano'],                               # capped at 500 characters
                ['equipment_utensils', 'core',
                 'Wiping Cloths; Properly Used and Stored: Observed large piles of dirty wiping cloths beneath '
                 'bar. Wiping cloths are to be stored clean and dry, in sanitizer solution or be laundered.'],
            ],
            'inspection_type': 'Food Routine Inspection',
            'results': 'Not Participating',
            'source': SOURCE,
            'source_url': 'https://envapp.maricopa.gov/Inspection/FD-00570/INSP-067508-2026',
            'source_id': 'mcesd_FD-00570_INSP-067508-2026',
            'metro': 'Phoenix',
        })
        for rec in self.records:
            self.assertTrue(all(len(v) == 3 for v in rec['violations']))
            for cat, sev, text in rec['violations']:
                self.assertIn(sev, ('priority', 'priority_foundation', 'core'))
                self.assertEqual(cat, dp.classify_violation(text)[0])
                self.assertLessEqual(len(text), 500)
                self.assertNotIn('\n', text)
            self.assertEqual(rec['risk_score'], dp.calc_risk_score(rec['violations'])[0])
            self.assertEqual((rec['priority_violations'], rec['priority_foundation_violations'],
                              rec['core_violations']), dp.calc_risk_score(rec['violations'])[1:])
            self.assertEqual(rec['total_violations'], len(rec['violations']))
            self.assertIsNone(rec['original_score'])
            self.assertEqual(rec['state'], 'AZ')
            self.assertEqual(rec['metro'], 'Phoenix')

    def test_window_selects_only_rows_on_or_after_since(self):
        self.assertEqual(sorted(self.by_id), ['mcesd_FD-00408_INSP-026373-2026', 'mcesd_FD-00570_INSP-067508-2026',
                                              'mcesd_FD-00571_INSP-900001-2026', 'mcesd_FD2602160_INSP-900002-2026'])
        self.assertTrue(all(r['inspection_date'] >= '2026-08-01' for r in self.records))
        # older rows never cost a detail page
        self.assertEqual(self.portal.detail_requests(), 4)
        self.assertEqual(self.portal.detail_requests('FD-02429'), 0)
        self.assertEqual(self.portal.detail_requests('FD-12738'), 0)

    def test_inventory_requests(self):
        # three static zips plus the ArcGIS list's 85281 / 85022 / 95040
        posts = [u for m, u in self.portal.calls if m == 'POST']
        self.assertEqual(len(posts), 6)
        # one permit page per food permit, the duplicate search row collapsed,
        # the daycare and the two mobile units never requested
        gets = [u for m, u in self.portal.calls if '/Permit/PermitResults/' in u]
        self.assertEqual(sorted(u.rsplit('/', 1)[1] for u in gets),
                         ['FD-00408', 'FD-00570', 'FD-00571', 'FD-00777', 'FD-02429', 'FD-12738', 'FD2602160'])

    def test_clean_inspection_and_grade_card_text(self):
        r = self.by_id['mcesd_FD-00408_INSP-026373-2026']
        self.assertEqual(r['violations'], [])
        self.assertEqual(r['risk_score'], 100)
        self.assertEqual(r['results'], 'Grade A')
        self.assertEqual(r['inspection_type'], 'CI Food Routine Inspection')
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'A')
        # grade, status and purpose come from the detail page (the row's
        # 'Re-inspection required' / blank advisory grade are not repeated
        # by these fixtures' detail pages), the date from the row
        self.assertEqual(self.by_id['mcesd_FD-00571_INSP-900001-2026']['results'], 'Grade B')
        self.assertEqual(self.by_id['mcesd_FD2602160_INSP-900002-2026']['results'], 'Grade A')
        self.assertEqual(self.by_id['mcesd_FD2602160_INSP-900002-2026']['inspection_date'], '2026-10-01')

    def test_same_day_visits_worst_first_and_shared_identity(self):
        a = self.by_id['mcesd_FD-00570_INSP-067508-2026']
        b = self.by_id['mcesd_FD-00571_INSP-900001-2026']
        self.assertEqual(a['inspection_date'], b['inspection_date'])
        self.assertEqual(dp.make_restaurant_id(a['name'], a['address'], a['city']),
                         dp.make_restaurant_id(b['name'], b['address'], b['city']))
        self.assertEqual((b['risk_score'], a['risk_score']), (95, 96))
        self.assertLess(self.records.index(b), self.records.index(a))         # the worse visit first
        self.assertEqual([r['inspection_date'] for r in self.records],
                         sorted((r['inspection_date'] for r in self.records), reverse=True))

    def test_coordinates_from_arcgis(self):
        taco = self.by_id['mcesd_FD-00408_INSP-026373-2026']
        self.assertAlmostEqual(taco['latitude'], 33.597585, places=5)
        self.assertAlmostEqual(taco['longitude'], -112.037022, places=5)
        self.assertTrue(all(isinstance(r['latitude'], float) and isinstance(r['longitude'], float)
                            for r in self.records))

    def test_mixed_case_concat(self):
        r = self.by_id['mcesd_FD2602160_INSP-900002-2026']
        self.assertEqual((r['name'], r['address'], r['city'], r['zip']),
                         ('Event Network # 5156', '340 N 3rd ST', 'Phoenix', '85004'))

    def test_no_lag_note_or_status_on_a_clean_recent_run(self):
        self.assertNotIn(GROUP, dp._GROUP_NOTES)
        self.assertNotIn(GROUP, dp._GROUP_STATUS)
        self.assertEqual(dp._FRESHNESS_HOLDS, {})


class WindowAndLimitTests(MaricopaTestCase):
    def test_both_since_formats(self):
        for since in ('2026-08-01T00:00:00', '2026-08-01'):
            records, portal = run_fetch(since=since)
            self.assertEqual(len(records), 4, since)
            self.assertEqual(portal.detail_requests(), 4, since)
        # the window start is inclusive
        records, _ = run_fetch(since='2026-08-18')
        self.assertEqual(sorted(_by_id(records)), ['mcesd_FD-00408_INSP-026373-2026', 'mcesd_FD-00570_INSP-067508-2026',
                                                   'mcesd_FD-00571_INSP-900001-2026', 'mcesd_FD2602160_INSP-900002-2026'])
        records, _ = run_fetch(since='2026-08-19')
        self.assertEqual(sorted(_by_id(records)), ['mcesd_FD-00408_INSP-026373-2026', 'mcesd_FD2602160_INSP-900002-2026'])

    def test_empty_window(self):
        records, portal = run_fetch(since='2026-10-20')
        self.assertEqual(records, [])
        self.assertEqual(portal.detail_requests(), 0)
        self.assertNotIn(GROUP, dp._GROUP_STATUS)

    def test_window_reaches_back_the_posting_lag_margin(self):
        # the county publishes late: a weekly window that starts the day
        # before the newest shipped row must still read the days before it
        days_back = (datetime.now() - datetime(2026, 10, 19)).days + 2
        with mock.patch.object(dp, 'MCESD_POSTING_LAG_DAYS', days_back):
            records, portal = run_fetch(since='2026-10-20')
        self.assertEqual([r['source_id'] for r in records], ['mcesd_FD-00408_INSP-026373-2026'])
        # ... while holds still refer to the requested start, so a held
        # window repeats run to run and the give-up count can accumulate
        portal = FakePortal()
        portal.permits['FD-00570'] = FakeResponse('<html>error</html>', 503)
        with mock.patch.object(dp, 'MCESD_POSTING_LAG_DAYS', days_back):
            run_fetch(portal, since='2026-10-20')
        self.assertEqual(dp._FRESHNESS_HOLDS[SOURCE], {'2026-10-22|FD-00570': (True, False)})
        # a far-older since is never narrowed by the margin
        with mock.patch.object(dp, 'MCESD_POSTING_LAG_DAYS', 7):
            records, _ = run_fetch(since='2026-08-01')
        self.assertEqual(len(records), 4)

    def test_no_since_reads_the_whole_history_and_holds_what_it_cannot_read(self):
        # every row of every permit page is a candidate; the rows without a
        # captured detail page get the portal's 'Inspection Not Available'
        # page, which drops them and holds each by its inspection id
        records, portal = run_fetch()
        self.assertEqual(sorted(_by_id(records)), [
            'mcesd_FD-00408_INSP-026373-2026', 'mcesd_FD-00570_INSP-067508-2026', 'mcesd_FD-00571_INSP-900001-2026',
            'mcesd_FD-00777_INSP-055361-2026', 'mcesd_FD-02429_INSP-026626-2024', 'mcesd_FD-12738_5663423-MCESD',
            'mcesd_FD2602160_INSP-900002-2026'])
        self.assertEqual(portal.detail_requests(), 10 + 6 + 5)
        holds = dp._FRESHNESS_HOLDS[SOURCE]
        self.assertEqual(len(holds), 14)
        self.assertIn('2026-04-07|INSP-179467-2025', holds)
        self.assertTrue(all(counted for counted, _ in holds.values()))    # tried and failed: counted
        self.assertNotIn(GROUP, dp._GROUP_STATUS)                           # per-record drops are not red
        legacy = _by_id(records)['mcesd_FD-12738_5663423-MCESD']
        self.assertEqual((legacy['priority_violations'], legacy['priority_foundation_violations'],
                          legacy['core_violations'], legacy['risk_score']), (1, 1, 1, 92))
        three_p = _by_id(records)['mcesd_FD-02429_INSP-026626-2024']
        self.assertEqual((three_p['risk_score'], dp.calculate_vetted_grade(three_p['risk_score'])), (85, 'B'))

    def test_limit_bounds_the_read_and_leaves_no_lag_note(self):
        records, portal = run_fetch(limit=2)
        self.assertEqual(len(records), 2)
        self.assertEqual(portal.detail_requests(), 2)
        # the newest inspections are kept
        self.assertEqual([r['inspection_date'] for r in records], ['2026-10-19', '2026-10-01'])
        # the inventory stopped once two food permits were known (test mode)
        self.assertLessEqual(sum(1 for m, _ in portal.calls if m == 'POST'), 6)
        self.assertNotIn(GROUP, dp._GROUP_NOTES)
        self.assertNotIn(GROUP, dp._GROUP_STATUS)

    def test_lag_note_when_the_source_itself_is_behind(self):
        portal = FakePortal()
        # only the Tempe permits, whose newest row is 9/26/2024
        portal.searches['85004'] = 'search_empty.json'
        portal.searches['85022'] = 'search_empty.json'
        with mock.patch.object(dp, 'MCESD_MIN_FOOD_PERMITS', 1):
            records, _ = run_fetch(portal, since='2024-01-01')
        self.assertEqual(len(records), 2)
        self.assertIn(GROUP, dp._GROUP_NOTES)
        self.assertIn('2024-09-26', dp._GROUP_NOTES[GROUP])

    def test_skip_known_inspections_are_not_detailed(self):
        rid = dp.make_restaurant_id('Renaissance Phoenix Downtown', '100 N 1St St', 'Phoenix')
        dp.SKIP_KNOWN_INSPECTIONS.add((rid, '2026-08-18'))
        records, portal = run_fetch(since='2026-08-01')
        self.assertEqual(sorted(_by_id(records)), ['mcesd_FD-00408_INSP-026373-2026', 'mcesd_FD2602160_INSP-900002-2026'])
        self.assertEqual(portal.detail_requests('FD-00570'), 0)
        self.assertEqual(portal.detail_requests('FD-00571'), 0)
        self.assertEqual(dp._MCESD_LAST_STATS['skipped_known'], 2)

    def test_id_stability(self):
        a, _ = run_fetch(since='2026-08-01')
        b, _ = run_fetch(since='2026-08-01')
        ids_a = {r['source_id']: dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in a}
        ids_b = {r['source_id']: dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in b}
        self.assertEqual(ids_a, ids_b)
        self.assertEqual(ids_a['mcesd_FD-00570_INSP-067508-2026'], 'cbb9440c5c371d0b')
        self.assertEqual(ids_a['mcesd_FD-00408_INSP-026373-2026'], 'e96870bc919f7023')
        # a search row that comes back in a different case gives the same id
        portal = FakePortal()
        rows = _json('search_85004.json')
        for row in rows['data']:
            row['businessName'] = row['businessName'].upper()
            row['businessAddressConcat'] = row['businessAddressConcat'].lower()
        portal.searches['85004'] = FakeResponse(json.dumps(rows))
        c, _ = run_fetch(portal, since='2026-08-01')
        self.assertEqual(dp.make_restaurant_id(*[_by_id(c)['mcesd_FD-00570_INSP-067508-2026'][k]
                                                for k in ('name', 'address', 'city')]), 'cbb9440c5c371d0b')


class FailureTests(MaricopaTestCase):
    def test_inventory_failures_raise_instead_of_shipping(self):
        bad = {'500 after retries': FakeResponse('<html>error</html>', 500),
               'connection error': dp.requests.ConnectionError('reset'),
               'non-JSON body': FakeResponse('<html>maintenance</html>'),
               'no data list': FakeResponse('{"rows": []}'),
               'data not a list': FakeResponse('{"data": null}')}
        for label, answer in bad.items():
            with self.subTest(label):
                portal = FakePortal()
                portal.searches['85004'] = answer
                with self.assertRaises(RuntimeError):
                    run_fetch(portal, since='2026-08-01')
                self.assertEqual(portal.detail_requests(), 0)
        # a 500 on every attempt costs exactly MCESD_ATTEMPTS requests for that zip
        portal = FakePortal()
        portal.searches['85004'] = FakeResponse('x', 503)
        with self.assertRaises(RuntimeError):
            run_fetch(portal, since='2026-08-01')
        self.assertEqual(sum(1 for m, u in portal.calls if m == 'POST' and u.endswith('#85004')), dp.MCESD_ATTEMPTS)

    def test_an_implausibly_small_inventory_raises_but_not_in_test_mode(self):
        with mock.patch.object(dp, 'MCESD_MIN_FOOD_PERMITS', 50):
            with self.assertRaises(RuntimeError):
                run_fetch(since='2026-08-01')
            records, _ = run_fetch(limit=2)
            self.assertEqual(len(records), 2)

    def test_arcgis_zip_list_failure_falls_back_to_the_static_range(self):
        def boom(url, params=None, headers=None, timeout=None):
            raise dp.requests.ConnectionError('arcgis down')
        records, portal = run_fetch(since='2026-08-01', zips_get=boom)
        self.assertEqual(sorted(u.rsplit('#', 1)[1] for m, u in portal.calls if m == 'POST'), ['85003', '85004', '85005'])
        self.assertEqual(sorted(_by_id(records)), ['mcesd_FD-00570_INSP-067508-2026', 'mcesd_FD-00571_INSP-900001-2026',
                                                   'mcesd_FD2602160_INSP-900002-2026'])

    def test_unread_permit_page_holds_the_window_and_goes_red(self):
        portal = FakePortal()
        portal.permits['FD-00408'] = FakeResponse('<html><body>Something else</body></html>')   # 200, no marker
        portal.permits['FD-00570'] = FakeResponse('<html>error</html>', 503)
        records, portal = run_fetch(portal, since='2026-08-01')
        self.assertEqual(sorted(_by_id(records)), ['mcesd_FD-00571_INSP-900001-2026', 'mcesd_FD2602160_INSP-900002-2026'])
        # a 200 without the markers is re-fetched once; a 5xx costs the retries twice
        self.assertEqual(portal.permit_requests('FD-00408'), 2)
        self.assertEqual(portal.permit_requests('FD-00570'), 2 * dp.MCESD_ATTEMPTS)
        # the window is held on each unread permit (counted: tried and failed)
        self.assertEqual(dp._FRESHNESS_HOLDS[SOURCE],
                         {'2026-08-03|FD-00408': (True, False), '2026-08-03|FD-00570': (True, False)})
        status, detail = dp._GROUP_STATUS[GROUP]
        self.assertEqual(status, 'error')
        self.assertIn('2 of 7 permit pages unreadable', detail)

    def test_detail_failures_drop_the_record_and_hold_its_date(self):
        portal = FakePortal()
        portal.details[('FD-00408', 'INSP-026373-2026')] = FakeResponse('<html>error</html>', 500)
        portal.details[('FD-00570', 'INSP-067508-2026')] = FakeResponse(
            _load('inspection_INSP-067508-2026.html').replace('id="Violations"', 'id="Nothing"'))
        portal.details[('FD-00571', 'INSP-900001-2026')] = 'inspection_not_available.html'
        records, portal = run_fetch(portal, since='2026-08-01')
        self.assertEqual([r['source_id'] for r in records], ['mcesd_FD2602160_INSP-900002-2026'])
        self.assertEqual(portal.detail_requests('FD-00408'), dp.MCESD_ATTEMPTS)
        self.assertEqual(portal.detail_requests('FD-00570'), 1)
        self.assertEqual(dp._FRESHNESS_HOLDS[SOURCE], {
            '2026-10-19|INSP-026373-2026': (True, False),
            '2026-08-18|INSP-067508-2026': (True, False),
            '2026-08-18|INSP-900001-2026': (True, False),
        })
        self.assertNotIn(GROUP, dp._GROUP_STATUS)
        self.assertEqual(dp._MCESD_LAST_STATS['dropped'], {'download': 1, 'unread': 1, 'unavailable': 1})

    def test_deadline_before_the_inventory_ships_nothing_and_reports_skipped(self):
        dp._FETCH_DEADLINE = time.time() - 1
        records, portal = run_fetch(since='2026-08-01')
        self.assertEqual(records, [])
        self.assertEqual(portal.calls, [])
        self.assertEqual(dp._GROUP_STATUS[GROUP][0], 'skipped')

    def test_deadline_during_the_listing_holds_the_window(self):
        portal = FakePortal()

        def expire(p):
            dp._FETCH_DEADLINE = time.time() - 1
            return FakeResponse(_load('permit_FD-00408.html'))
        portal.permits['FD-00408'] = expire                     # the first permit page in zip order
        records, portal = run_fetch(portal, since='2026-08-01')
        self.assertEqual(records, [])
        self.assertEqual(portal.detail_requests(), 0)
        self.assertEqual(dp._FRESHNESS_HOLDS[SOURCE], {'2026-08-03|': (False, False)})
        status, detail = dp._GROUP_STATUS[GROUP]
        self.assertEqual(status, 'skipped')
        self.assertIn('fetch deadline reached', detail)

    def test_deadline_during_the_details_ships_the_oldest_and_holds_the_rest(self):
        portal = FakePortal()

        def expire(p):
            dp._FETCH_DEADLINE = time.time() - 1
            return FakeResponse(_load('inspection_INSP-055361-2026.html'))
        # details run oldest first, by (date, permit id): FD-00570's 8/18
        # visit is read first and trips the deadline; everything after it is
        # held without counting against the records
        portal.details[('FD-00570', 'INSP-067508-2026')] = expire
        records, portal = run_fetch(portal, since='2026-08-01')
        self.assertEqual([r['source_id'] for r in records], ['mcesd_FD-00570_INSP-067508-2026'])
        self.assertEqual(records[0]['inspection_date'], '2026-08-18')
        self.assertEqual(portal.detail_requests(), 1)
        held = dp._FRESHNESS_HOLDS[SOURCE]
        self.assertEqual(len(held), 3)
        self.assertTrue(all(v == (False, False) for v in held.values()), held)
        self.assertIn('2026-10-19|INSP-026373-2026', held)
        status, detail = dp._GROUP_STATUS[GROUP]
        self.assertEqual(status, 'skipped')
        self.assertIn('3 detail pages not read', detail)

    def test_breaker_stops_a_blocked_listing(self):
        portal = FakePortal()
        for pid in PERMITS:
            portal.permits[pid] = FakeResponse('<html>Access Denied</html>', 403)
        with mock.patch.object(dp, 'MCESD_BREAKER_TRIP', 3):
            records, portal = run_fetch(portal, since='2026-08-01')
        self.assertEqual(records, [])
        # one batch (MCESD_LISTING_WORKERS * 5 = 5 permits) was tried, then the breaker stopped the phase
        self.assertEqual(len({u for m, u in portal.calls if '/Permit/' in u}), 5)
        self.assertEqual(dp._GROUP_STATUS[GROUP][0], 'blocked')
        self.assertIn('2026-08-03|', dp._FRESHNESS_HOLDS[SOURCE])
        self.assertEqual(dp._FRESHNESS_HOLDS[SOURCE]['2026-08-03|'], (False, False))

    def test_breaker_stops_blocked_details(self):
        portal = FakePortal()
        for key in DETAILS:
            portal.details[key] = FakeResponse('<html>Access Denied</html>', 403)
        with mock.patch.object(dp, 'MCESD_BREAKER_TRIP', 2):
            records, portal = run_fetch(portal, since='2026-08-01')
        self.assertEqual(records, [])
        self.assertEqual(portal.detail_requests(), 2 * dp.MCESD_ATTEMPTS)
        held = dp._FRESHNESS_HOLDS[SOURCE]
        self.assertEqual(sorted(v for v in held.values()), [(False, False), (False, False), (True, False), (True, False)])
        self.assertEqual(dp._GROUP_STATUS[GROUP][0], 'blocked')

    def test_arcgis_points_failure_ships_records_without_coordinates(self):
        def boom(layer_url, where='1=1', out_fields='*', **kw):
            raise RuntimeError('ArcGIS error: down')
        records, _ = run_fetch(since='2026-08-01', points=boom)
        self.assertEqual(len(records), 4)
        self.assertTrue(all(r['latitude'] is None and r['longitude'] is None for r in records))

    def test_points_outside_arizona_or_missing_are_none(self):
        def points(layer_url, where='1=1', out_fields='*', **kw):
            return [{'attributes': {'LicenseNumber': 'FD-00408'}, 'geometry': {'x': 0, 'y': 0}},
                    {'attributes': {'LicenseNumber': 'FD-00570'}, 'geometry': {'x': -96.0, 'y': 41.25}},
                    {'attributes': {'LicenseNumber': 'FD-00571'}, 'geometry': None},
                    {'attributes': {'LicenseNumber': 'FD2602160'}, 'geometry': {'x': -112.07, 'y': 33.45}},
                    {'attributes': {'LicenseNumber': 'FD2602160'}, 'geometry': {'x': -111.0, 'y': 34.0}}]
        records, _ = run_fetch(since='2026-08-01', points=points)
        by_id = _by_id(records)
        for sid in ('mcesd_FD-00408_INSP-026373-2026', 'mcesd_FD-00570_INSP-067508-2026', 'mcesd_FD-00571_INSP-900001-2026'):
            self.assertIsNone(by_id[sid]['latitude'], sid)
            self.assertIsNone(by_id[sid]['longitude'], sid)
        # the first point of a duplicated license wins
        self.assertEqual((by_id['mcesd_FD2602160_INSP-900002-2026']['latitude'],
                          by_id['mcesd_FD2602160_INSP-900002-2026']['longitude']), (33.45, -112.07))


class PolicyTests(unittest.TestCase):
    def test_source_label_is_registered_as_source_stated_tiers(self):
        import jev_reader as jr
        self.assertEqual(jr.tier_policy(SOURCE), 'fixed')
        self.assertEqual(jr.score_policy(SOURCE, None), 'source_tiers')

    def test_not_a_relay_source(self):
        self.assertFalse(dp._wants_portal_sources(['maricopa']))


if __name__ == '__main__':
    unittest.main()
