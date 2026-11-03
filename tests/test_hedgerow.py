"""Tests for the Hedgerow "Hedgehog" portal fetchers: Pima County AZ
(fetch_pima), Lubbock TX (fetch_lubbock) and Mesquite TX (fetch_mesquite),
which share one client. No network: requests.get/post are replaced by a fake
portal that serves the fixtures under tests/fixtures/hedgerow/ — trimmed
copies of the live responses captured on 2026-11-02 (Pima, Lubbock) and
2026-11-03 (Mesquite): facility rows chosen to cover every address and
scope branch, two real inspection lists per portal family, and the
checklists of real inspections (a Needs Improvement, an enforcement Fail, a
seven-citation visit, a score-65 Lubbock visit, Mesquite's title-only
items). Inspection lists for facilities whose list was not captured are
built in code with the real key set."""
import json
import os
import sys
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader as jr  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'hedgerow')
PIMA = dp.HH_PORTALS['pima']
LBK = dp.HH_PORTALS['lubbock']
MESQ = dp.HH_PORTALS['mesquite']
PIMA_SRC = 'Pima County Health Department'
LBK_SRC = 'Lubbock Environmental Health'
MESQ_SRC = 'Mesquite Environmental Health'

# facility ids in the fixtures
DOMINOS = 'f1130177-ffff-ffff-ffff-fffff3130223'
BAG_O_CRAB = '153bd996-312d-44df-9e08-2d8878822275'
RITAS = 'e1f67626-52ce-4a0f-9eae-001c2a981851'
OLIVE_GARDEN = 'f1030331-ffff-ffff-ffff-fffff3040332'
VILLA_1 = '334853ee-5a52-473f-af09-0e36342b4bbb'
VILLA_2 = '14c5faa1-abd7-4f39-be28-7f3068b6d29d'
MEXICO_101 = '4b4d73c4-497b-4c44-9dbf-2e185cf2c521'
FIESTA = '3eeb9e47-8b5f-452d-9a85-5862a3c93be6'
BAHAMA = '388fe9f2-43a4-415d-9228-d21d56ef9d93'
OXXO = '24fdfeb2-ceaf-4604-ba3a-a4f6201728ff'
SUBWAY = '7ee83d12-8090-4d9c-abeb-00897c763bcc'
# inspection ids
DOMINOS_NI = '462a1932-e706-489e-9ca6-952a02b6db11'          # Needs Improvement 2026-10-17
DOMINOS_EDU = '44192d31-8f93-49ed-88e9-8063a62a24e1'         # Educational 2020-10-01
BAG_FAIL = 'df786789-dc92-462c-a19b-7d8f32edaf8b'            # Enforcement Inspection, Fail 2026-10-16
BAG_NI = 'fe5df6e9-c5bb-4a0c-b5e2-687593bc1d28'              # Needs Improvement 2026-10-06
OXXO_99 = '6a38b402-31e5-4ab0-ab30-f6f5f35e5394'             # INSP-019817, score 99, 2026-01-21
OXXO_88 = 'b3f8ee9e-7699-4233-b422-1e9c36a90a20'             # INSP-011693, score 88, 2024-03-21
SUBWAY_OCT = 'c8d657ae-3fcb-496b-975e-ca66117998b0'          # score 97, 2026-10-22


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _insp(iid, date, score=None, rating='Excellent', itype='Standard Frequency Inspection ',
          number=None, reason=None):
    """An inspectionDetails row with the real key set (Pima prints a rating,
    Lubbock a score and number, Mesquite neither type nor number)."""
    return {
        'id': iid, 'programAreaId': 'f1dad0aa-20ce-47ee-b5da-7b4333b0282c', 'facilityName': None,
        'closingComments': None, 'hasInfractions': None, 'inspectionDate': date,
        'openingComments': None, 'inspectionType': itype, 'inspectionReason': reason,
        'inspector': None, 'criticalInfractionCount': None, 'inspectionNumber': number,
        'nonCriticalInfractionCount': None, 'hazardScore': score, 'hazardRating': rating,
        'hazardRatingModelElementId': None, 'actionsTaken': None, 'actionsTakenList': None,
        'showInspectionDisclaimer': False, 'inspectionDisclaimer': None, 'groupingOption': None,
        'documentId': None,
    }


class _Resp:
    def __init__(self, payload, status=200, text=None):
        self._payload = payload
        self.status_code = status
        self.text = text if text is not None else json.dumps(payload)
        self.content = self.text.encode()

    def json(self):
        if self._payload is ValueError:
            raise ValueError('not JSON')
        return self._payload


class Portal:
    """A fake Hedgehog server for one portal. `facilities` is the list
    payload (or a list of page payloads for a paged portal); `inspections`
    maps facility id -> inspection list (or a _Resp / exception to answer
    with); `checklists` maps inspection id -> items (or a _Resp / exception).
    An unknown facility or inspection answers 404."""

    def __init__(self, cfg, facilities, inspections, checklists):
        self.cfg = cfg
        self.facilities = facilities
        self.inspections = inspections
        self.checklists = checklists
        self.calls = []          # (method, path, body)
        self.on_list_done = None

    def _answer(self, table, key):
        got = table.get(key)
        if isinstance(got, Exception):
            raise got
        if isinstance(got, _Resp):
            return got
        if got is None:
            return _Resp({'Message': 'No HTTP resource was found'}, 404)
        return _Resp(got)

    def post(self, url, json=None, headers=None, timeout=None):
        assert url.startswith(self.cfg['base']), url
        path = url[len(self.cfg['base']):]
        self.calls.append(('POST', path, json))
        assert path == 'disclosure/program/facilities', path
        assert headers['User-Agent'].startswith('Mozilla/5.0'), headers
        if isinstance(self.facilities, Exception):
            raise self.facilities
        if isinstance(self.facilities, _Resp):
            return self.facilities
        if isinstance(self.facilities, list):          # paged
            page = json['pageNumber']
            if page >= len(self.facilities):
                return _Resp({'pageNumber': page, 'pageSize': json['pageSize'],
                              'totalNumberOfRecords': 0, 'result': []})
            entry = self.facilities[page]
            if isinstance(entry, Exception):
                raise entry
            return entry if isinstance(entry, _Resp) else _Resp(entry)
        return _Resp(self.facilities)

    def get(self, url, headers=None, timeout=None):
        assert url.startswith(self.cfg['base']), url
        path = url[len(self.cfg['base']):]
        self.calls.append(('GET', path, None))
        assert headers['User-Agent'].startswith('Mozilla/5.0'), headers
        if path.endswith('/inspectionDetails/'):
            prefix = f"disclosure/program/{self.cfg['program']}/facility/"
            assert path.startswith(prefix), path
            fid = path[len(prefix):-len('/inspectionDetails/')]
            resp = self._answer(self.inspections, fid)
            if self.on_list_done:
                self.on_list_done(fid)
            return resp
        prefix = 'disclosure/program/inspectionResults/'
        assert path.startswith(prefix), path
        return self._answer(self.checklists, path[len(prefix):])

    def lists_requested(self):
        return [c[1].split('/facility/')[1].split('/')[0] for c in self.calls
                if c[0] == 'GET' and c[1].endswith('/inspectionDetails/')]

    def checklists_requested(self):
        return [c[1].rsplit('/', 1)[1] for c in self.calls
                if c[0] == 'GET' and '/inspectionResults/' in c[1]]


def _run(portal, fetcher, since=None, limit=None):
    with mock.patch.object(dp.requests, 'get', side_effect=portal.get), \
            mock.patch.object(dp.requests, 'post', side_effect=portal.post), \
            mock.patch.object(dp.time, 'sleep'):
        return fetcher(since_date=since, limit=limit)


def _by_id(records):
    return {r['source_id']: r for r in records}


def _pima_portal(**overrides):
    facilities = _load('pima_facilities.json')
    inspections = {
        DOMINOS: _load('pima_inspections_dominos.json'),
        BAG_O_CRAB: _load('pima_inspections_bagocrab.json'),
        RITAS: [_insp('r1', '2026-05-14T14:49:21.9576592-07:00')],
        OLIVE_GARDEN: [_insp('og1', '2026-07-13T11:26:37.5220708-07:00', rating='Good')],
        # 'VILLA HERMOSA' twice at one address (two permits): a Good visit with
        # seven citations and a clean Excellent one on the same day.
        VILLA_1: [_insp('vh1', '2026-05-13T10:10:10.9078819-07:00', rating='Good')],
        VILLA_2: [_insp('vh2', '2026-05-13T11:35:24.5007191-07:00')],
        MEXICO_101: [_insp('mx1', '2026-06-18T11:00:17.9914412-07:00')],
    }
    clean = _load('pima_results_clean.json')
    checklists = {
        DOMINOS_NI: _load('pima_results_needs_improvement.json'),
        DOMINOS_EDU: _load('pima_results_sonic.json'),
        BAG_FAIL: _load('pima_results_fail.json'),
        'vh1': _load('pima_results_sonic.json'),
    }
    inspections.update(overrides.pop('inspections', {}))
    for insp_list in inspections.values():
        for insp in (insp_list if isinstance(insp_list, list) else []):
            checklists.setdefault(insp['id'], clean)
    checklists.update(overrides.pop('checklists', {}))
    return Portal(PIMA, overrides.pop('facilities', facilities), inspections, checklists)


class _Base(unittest.TestCase):
    """Save, clear and restore every pipeline global the fetchers touch."""

    def setUp(self):
        self._saved = (dp._FRESHNESS, dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
                       dict(dp._GROUP_NOTES), dp._FETCH_DEADLINE)
        dp._FRESHNESS = None
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._GROUP_NOTES.clear()
        dp._FETCH_DEADLINE = None
        # the fixtures are a handful of rows, not a whole program
        for cfg in (PIMA, MESQ):
            p = mock.patch.dict(cfg, {'list_min_rows': 1})
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        dp._FRESHNESS = self._saved[0]
        dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(self._saved[1])
        dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(self._saved[2])
        dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(self._saved[3])
        dp._FETCH_DEADLINE = self._saved[4]


# ── the client ───────────────────────────────────────────────────────────────

class ClientTests(_Base):
    def _get(self, answers):
        calls = []

        def fake(url, headers=None, timeout=None):
            calls.append(url)
            a = answers[min(len(calls) - 1, len(answers) - 1)]
            if isinstance(a, Exception):
                raise a
            return a
        with mock.patch.object(dp.requests, 'get', side_effect=fake), \
                mock.patch.object(dp.time, 'sleep') as sleep:
            try:
                return dp._hh_json('pima', 'disclosure/program/inspectionResults/x'), calls, sleep
            except dp._HhError as e:
                return e, calls, sleep

    def test_a_5xx_is_retried_with_backoff_then_answered(self):
        got, calls, sleep = self._get([_Resp({'Message': 'An error has occurred.'}, 500), _Resp([1, 2])])
        self.assertEqual(got, [1, 2])
        self.assertEqual(len(calls), 2)
        sleep.assert_called_once_with(dp.HH_RETRY_WAIT)

    def test_a_4xx_is_permanent_and_not_retried(self):
        got, calls, _ = self._get([_Resp({'Message': 'not found'}, 404)])
        self.assertIsInstance(got, dp._HhError)
        self.assertTrue(got.permanent)
        self.assertEqual(len(calls), 1)

    def test_exhausted_retries_raise_a_transient_error(self):
        got, calls, _ = self._get([dp.requests.ConnectionError('reset')])
        self.assertIsInstance(got, dp._HhError)
        self.assertFalse(got.permanent)
        self.assertEqual(len(calls), dp.HH_RETRIES)

    def test_a_non_json_200_is_transient(self):
        got, calls, _ = self._get([_Resp(ValueError, text='<html>challenge</html>'), _Resp({'ok': 1})])
        self.assertEqual(got, {'ok': 1})
        self.assertEqual(len(calls), 2)

    def test_no_retry_past_the_deadline(self):
        dp._FETCH_DEADLINE = time.time() - 1
        got, calls, _ = self._get([_Resp(None, 503)])
        self.assertIsInstance(got, dp._HhError)
        self.assertEqual(len(calls), 1)

    def test_requests_go_through_the_module_with_a_chrome_ua(self):
        with mock.patch.object(dp.requests, 'get', return_value=_Resp([])) as get:
            dp._hh_json('lubbock', 'x/')
        self.assertEqual(get.call_args.args[0], LBK['base'] + 'x/')
        self.assertTrue(get.call_args.kwargs['headers']['User-Agent'].startswith('Mozilla/5.0'))
        self.assertEqual(get.call_args.kwargs['timeout'], dp.HH_TIMEOUT)


# ── parsing helpers ──────────────────────────────────────────────────────────

class AddressTests(unittest.TestCase):
    def test_the_rule_and_every_deviant_seen(self):
        cases = [
            # (address, portal, (street, city, state, zip))
            ('4210 N 1st Ave Ste 100, Tucson AZ 85705', PIMA, ('4210 N 1st Ave Ste 100', 'Tucson', 'AZ', '85705')),
            ('3682 W ORANGE GROVE RD, TUCSON AZ 85741', PIMA, ('3682 W Orange Grove Rd', 'Tucson', 'AZ', '85741')),
            ('1103 W Prince Rd, Tucson Arizona 85705', PIMA, ('1103 W Prince Rd', 'Tucson', 'AZ', '85705')),
            ('3541 S 12th ave, Tucsonaz 85713', PIMA, ('3541 S 12th ave', 'Tucson', 'AZ', '85713')),
            ('4860 S 12th Ave, Tucson AZ', PIMA, ('4860 S 12th Ave', 'Tucson', 'AZ', '')),
            ('12900    ORGANIZATION RIDGE RD, MT LEMMON                 AZ 85619', PIMA,
             ('12900 Organization Ridge Rd', 'Mt Lemmon', 'AZ', '85619')),
            ('1825 E River Road, Suites 111 & 119, Tucson Arizona 85711', PIMA,
             ('1825 E River Road, Suites 111 & 119', 'Tucson', 'AZ', '85711')),
            ('100 Main St, Corona De Tucson AZ 85641', PIMA, ('100 Main St', 'Corona De Tucson', 'AZ', '85641')),
            ('2902 4th St, Lubbock TX 79415', LBK, ('2902 4th St', 'Lubbock', 'TX', '79415')),
            ('505 N Gardner Lubbock, TX 79416', LBK, ('505 N Gardner', 'Lubbock', 'TX', '79416')),
            ('2515 82nd St Suite B, Lubbock 794232249 TX', LBK, ('2515 82nd St Suite B', 'Lubbock', 'TX', '79423')),
            ('3913 N Azotea St, Hobbs NM 88240', LBK, ('3913 N Azotea St', 'Hobbs', 'NM', '88240')),
            ('46th St, Ave U - Clapp Park', LBK, ('46th St, Ave U - Clapp Park', 'Lubbock', 'TX', '')),
            # Lubbock keeps the published case (its ALL-CAPS names are brands)
            ('2902 4TH ST, LUBBOCK TX 79415', LBK, ('2902 4TH ST', 'Lubbock', 'TX', '79415')),
            ('3522   Gus Thomasson Rd #110, Mesquite TX 75150', MESQ, ('3522 Gus Thomasson Rd #110', 'Mesquite', 'TX', '75150')),
            # Mesquite's six off-pattern rows (2026-11-03): city before a suite number, no city, comma after the city
            ('3220 Gus Thomasson  Mesquite 101, TX 75150', MESQ, ('3220 Gus Thomasson Mesquite 101', 'Mesquite', 'TX', '75150')),
            ('1700 Rodeo Dr, TX 75149', MESQ, ('1700 Rodeo Dr', 'Mesquite', 'TX', '75149')),
            ('1111 E Davis Mesquite, TX 75149', MESQ, ('1111 E Davis', 'Mesquite', 'TX', '75149')),
            ('1140 IH 30,   Suite #100, Mesquite, TX 75150', MESQ, ('1140 IH 30, Suite #100', 'Mesquite', 'TX', '75150')),
        ]
        for addr, cfg, want in cases:
            with self.subTest(addr=addr):
                self.assertEqual(dp._hh_split_address(addr, cfg), want)

    def test_no_street_is_none(self):
        self.assertIsNone(dp._hh_split_address('', PIMA))
        self.assertIsNone(dp._hh_split_address('   ', PIMA))
        self.assertIsNone(dp._hh_split_address(None, PIMA))
        self.assertIsNone(dp._hh_split_address(', Tucson AZ 85705', PIMA))

    def test_title_casing_only_for_all_caps(self):
        self.assertEqual(dp._hh_title("DOMINO'S PIZZA #7626 "), "Domino's Pizza #7626")
        self.assertEqual(dp._hh_title('4210 N 1ST AVE STE 100'), '4210 N 1st Ave Ste 100')
        self.assertEqual(dp._hh_title("Rita's  Italian Ice"), "Rita's Italian Ice")
        self.assertEqual(dp._hh_title('McDonald\'s #123'), "McDonald's #123")

    def test_coordinates(self):
        self.assertEqual(dp._hh_coord('32.2833974'), 32.2833974)
        self.assertEqual(dp._hh_coord(-110.9607431), -110.9607431)
        for bad in (None, 0, '0', '', 'n/a', 400):
            self.assertIsNone(dp._hh_coord(bad), bad)


class ViolationTests(unittest.TestCase):
    def test_only_cited_items_one_line_per_citation_with_the_agency_tier(self):
        lines = dp._hh_violations(_load('pima_results_needs_improvement.json'))
        self.assertEqual([v[1] for v in lines], ['priority', 'core', 'core'])
        self.assertTrue(all(len(v) == 3 for v in lines))
        for cat, sev, text in lines:
            self.assertEqual(cat, dp.classify_violation(text)[0])
            self.assertIn(sev, ('priority', 'priority_foundation', 'core'))
            self.assertLessEqual(len(text), 500)
            self.assertNotIn('\n', text)
        # title + the inspector's observation; never the Food Code paragraph
        self.assertTrue(lines[1][2].startswith('Adequate ventilation and lighting; designated areas used: '
                                               'Light intensity at ware washing sink'))
        self.assertNotIn('shall', ' '.join(v[2] for v in lines))

    def test_severity_comes_from_the_citation_not_the_classifier(self):
        lines = dp._hh_violations(_load('lubbock_results_score65.json'))
        self.assertEqual([v[1] for v in lines], ['priority', 'priority', 'priority_foundation',
                                                 'priority_foundation', 'core', 'core'])
        # 'Time as a Public Health Control...' is unclassified (would be core) but Priority
        self.assertEqual(lines[1][0], 'unclassified')
        self.assertEqual(lines[1][1], 'priority')

    def test_location_is_appended_and_a_repeated_item_stays_two_lines(self):
        lines = dp._hh_violations(_load('lubbock_results_small.json'))
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0][2], 'Non-Food Contact Surfaces Clean: Cabinet under soda machine had an '
                                      'accumulation of soda syrup and other debris (Soda Machine)')
        self.assertEqual(lines[1][2], 'Non-Food Contact Surfaces Clean: Vent shrouds of walk-in cooler had '
                                      'an accumulation of dust (Walk-in Cooler)')

    def test_without_an_observation_the_short_rule_name_stands_in(self):
        lines = dp._hh_violations(_load('mesquite_results_three_items.json'))
        self.assertEqual([v[2] for v in lines], [
            # the rule text is a 200-character paragraph: title alone
            'Certified Food Manager and Registered with the City of Mesquite must be present',
            'Physical Facilities Installed, Maintained and Cleaned: Drying mops',
            'Other Violations: Handwashing signage',
        ])
        self.assertEqual([v[1] for v in lines], ['priority_foundation', 'core', 'core'])
        one = dp._hh_violations(_load('mesquite_results_one_item.json'))
        # '@____ PPM' placeholder dropped, code citation '228.113(1) - ' stripped
        self.assertEqual(one[0][2], 'Food Contact Surfaces and Returnables; Cleaned and Sanitized: '
                                    'Food-contact surfaces clean to sight and touc')
        self.assertEqual(one[0][1], 'priority')

    def test_a_cited_item_without_a_citation_keeps_the_classifier_tier(self):
        items = [{'description': 'Insects, rodents, and animals not present', 'result': 'Out of Compliance',
                  'isCritical': True, 'cannedComments': None},
                 {'description': 'Hot holding', 'result': 'In Compliance', 'cannedComments': None}]
        lines = dp._hh_violations(items)
        cat, sev, _ = dp.classify_violation('Insects, rodents, and animals not present')
        self.assertEqual(lines, [[cat, sev, 'Insects, rodents, and animals not present']])
        self.assertEqual(cat, 'pest_control')
        self.assertFalse(dp.has_active_pest_or_sewage(lines[0][2]))   # a bare title is not evidence

    def test_the_texas_pest_criterion_ships_under_a_neutral_heading(self):
        def item(obs, rule='228.186(k) - Outer openings, protected'):
            return [{'description': 'No evidence of Insect Contamination, Rodent, Other Animals',
                     'result': 'Out of Compliance', 'isCritical': False,
                     'cannedComments': [{'description': rule, 'severity': 'Core',
                                         'observationComment': obs, 'locationComment': 'Ceiling'}]}]
        # the form's title alone trips the regex ('evidence of' before 'Insect'); the shipped line must not
        self.assertTrue(dp.has_active_pest_or_sewage('No evidence of Insect Contamination, Rodent, Other Animals'))
        trap = dp._hh_violations(item('Sticky fly trap stored over prep table located in center of kitchen'))
        self.assertEqual(trap[0][2], 'Pest control: Sticky fly trap stored over prep table located in '
                                     'center of kitchen (Ceiling)')
        self.assertEqual(trap[0][:2], ['pest_control', 'core'])
        self.assertFalse(dp.has_active_pest_or_sewage(trap[0][2]))
        self.assertFalse(jr.names_vermin(trap[0][2]))                 # Jev's guard sees no vermin either
        # two pest lines of one inspection: 'dead flies' at the end of one line
        # and the next line's heading must not meet across the '|||' join
        joined = dp.build_violation_text(dp._hh_violations(
            item('Fly strips with accumulation of dead flies hanging in kitchen area') + item('Ceiling tile missing')))
        self.assertFalse(dp.has_active_pest_or_sewage(joined))
        roaches = dp._hh_violations(item('Observed live roaches under the prep sink'))
        self.assertTrue(dp.has_active_pest_or_sewage(roaches[0][2]))
        self.assertTrue(jr.names_vermin(roaches[0][2]))
        bare = dp._hh_violations(item(None))                            # Mesquite prints no observation
        self.assertEqual(bare[0][2], 'Pest control: Outer openings, protected (Ceiling)')
        self.assertFalse(dp.has_active_pest_or_sewage(bare[0][2]))
        # Richardson's slash form and a title without the negation are the same criterion
        for title in ('No Evidence of Insect contamination, rodent/other animals',
                      'Evidence of Insect Contamination, Rodent, Other Animals'):
            self.assertTrue(dp._HH_PEST_TITLE_RE.match(title), title)
        self.assertFalse(dp._HH_PEST_TITLE_RE.match('Insects, rodents, and animals not present'))

    def test_clean_checklist_is_empty_and_junk_raises(self):
        self.assertEqual(dp._hh_violations(_load('pima_results_clean.json')), [])
        self.assertEqual(dp._hh_violations([]), [])
        for junk in ({'Message': 'An error has occurred.'}, None, 'x'):
            with self.assertRaises(ValueError):
                dp._hh_violations(junk)

    def test_text_is_capped_at_500_chars(self):
        items = [{'description': 'Title', 'result': 'Out of Compliance',
                  'cannedComments': [{'severity': 'Core', 'observationComment': 'x' * 900}]}]
        self.assertEqual(len(dp._hh_violations(items)[0][2]), 500)


# ── Pima County ──────────────────────────────────────────────────────────────

class PimaFetchTests(_Base):
    def test_list_request_shape(self):
        portal = _pima_portal()
        _run(portal, dp.fetch_pima, since='2026-10-01')
        method, path, body = portal.calls[0]
        self.assertEqual((method, path), ('POST', 'disclosure/program/facilities'))
        # the Food list only answers when sorted by id (facilityName times out)
        self.assertEqual(body, {'sort': [{'field': 'id', 'order': 'asc'}],
                                'disclosureProgramId': PIMA['program'], 'fields': ['facilityName']})
        self.assertEqual(portal.calls.count(portal.calls[0]), 1)       # read once per run

    def test_window_selects_facilities_then_inspections(self):
        for since in ('2026-10-01T00:00:00', '2026-10-01'):
            with self.subTest(since=since):
                dp._GROUP_NOTES.clear()
                portal = _pima_portal()
                records = _run(portal, dp.fetch_pima, since=since)
                # only the two facilities inspected since 2026-10-01 are listed
                self.assertEqual(sorted(portal.lists_requested()), sorted([DOMINOS, BAG_O_CRAB]))
                self.assertEqual(sorted(portal.checklists_requested()), sorted([DOMINOS_NI, BAG_NI, BAG_FAIL]))
                self.assertEqual([r['source_id'] for r in records],
                                 [f'pima_{DOMINOS_NI}', f'pima_{BAG_FAIL}', f'pima_{BAG_NI}'])
                self.assertTrue(all(r['inspection_date'] >= '2026-10-01' for r in records))

    def test_needs_improvement_record(self):
        records = _run(_pima_portal(), dp.fetch_pima, since='2026-10-01')
        r = _by_id(records)[f'pima_{DOMINOS_NI}']
        self.assertEqual(r, {
            'name': "Domino's Pizza #7626",
            'address': '3682 W Orange Grove Rd',
            'city': 'Tucson',
            'state': 'AZ',
            'zip': '85741',
            'latitude': 32.3239092,
            'longitude': -111.0431794,
            'inspection_date': '2026-10-17',
            'original_score': None,
            'risk_score': 93,                       # 100 - 5 - 1 - 1 from the citations
            'priority_violations': 1,
            'priority_foundation_violations': 0,
            'core_violations': 2,
            'total_violations': 3,
            'violations': r['violations'],
            'inspection_type': 'Standard Frequency Inspection',
            'results': 'Needs Improvement',
            'source': PIMA_SRC,
            'source_url': f'https://healthinspect.pima.gov/portal/#/{PIMA["program"]}/disclosure/'
                          f'facility/{DOMINOS}/inspection/{DOMINOS_NI}',
            'source_id': f'pima_{DOMINOS_NI}',
            'metro': 'Tucson',
        })
        self.assertEqual(len(r['violations']), 3)
        self.assertEqual(r['violations'], dp._hh_violations(_load('pima_results_needs_improvement.json')))
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'A')

    def test_a_fail_rating_caps_the_computed_score_at_an_f(self):
        by_id = _by_id(_run(_pima_portal(), dp.fetch_pima, since='2026-10-01'))
        fail = by_id[f'pima_{BAG_FAIL}']
        # two citations (Core + Priority Foundation) compute to 97; the county failed the re-inspection
        self.assertEqual((fail['priority_violations'], fail['priority_foundation_violations'],
                          fail['core_violations']), (0, 1, 1))
        self.assertEqual(fail['risk_score'], dp.PIMA_RATING_RISK_CAP['Fail'])
        self.assertEqual(dp.calculate_vetted_grade(fail['risk_score']), 'F')
        self.assertEqual(fail['results'], 'Fail')
        self.assertEqual(fail['inspection_type'], 'Enforcement Inspection')
        self.assertIsNone(fail['original_score'])
        # a cap never raises a score
        ni = by_id[f'pima_{BAG_NI}']
        self.assertEqual(ni['risk_score'], 100)      # clean checklist fixture, rating Needs Improvement
        self.assertEqual(ni['results'], 'Needs Improvement')

    def test_full_history_ships_educational_and_pass_visits_and_skips_scope(self):
        portal = _pima_portal()
        records = _run(portal, dp.fetch_pima)
        by_id = _by_id(records)
        # 5 Domino's + 4 Bag O Crab + Rita's + Olive Garden + 2 Villa Hermosa + Mexico 101
        self.assertEqual(len(records), 14)
        edu = by_id[f'pima_{DOMINOS_EDU}']
        self.assertEqual((edu['results'], edu['inspection_date'], edu['risk_score']), ('Educational', '2020-10-01', 92))
        self.assertEqual(by_id['pima_6f7ce4ce-2cd3-445b-b59d-1ed9328bd229']['results'], 'Pass')
        # mobile units (incl. the one without an address) and the never-inspected
        # facility are never listed
        self.assertEqual(len(portal.lists_requested()), 7)
        names = {r['name'] for r in records}
        self.assertNotIn("Morty's Backyard - 1179", names)
        self.assertNotIn('Taco Totys - 1169', names)
        self.assertNotIn('Whatadogo - 1442', names)
        self.assertNotIn('ZAKI - 1662', names)

    def test_address_deviants_and_missing_coordinates(self):
        by_id = _by_id(_run(_pima_portal(), dp.fetch_pima))
        mx = by_id['pima_mx1']                     # '1103 W Prince Rd, Tucson Arizona 85705'
        self.assertEqual((mx['address'], mx['city'], mx['zip']), ('1103 W Prince Rd', 'Tucson', '85705'))
        og = by_id['pima_og1']                     # null latitude/longitude on the row
        self.assertEqual((og['name'], og['address']), ('Olive Garden', '1213 W Irvington Rd'))
        self.assertIsNone(og['latitude'])
        self.assertIsNone(og['longitude'])
        others = [r for r in by_id.values() if r['source_id'] != 'pima_og1']
        self.assertTrue(all(isinstance(r['latitude'], float) and isinstance(r['longitude'], float)
                            for r in others))

    def test_same_day_visits_worst_first_and_shared_ids(self):
        records = _run(_pima_portal(), dp.fetch_pima)
        same_day = [r for r in records if r['name'] == 'Villa Hermosa']
        self.assertEqual([r['risk_score'] for r in same_day], [92, 100])
        self.assertEqual(len({dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in same_day}), 1)
        dates = [r['inspection_date'] for r in records]
        self.assertEqual(dates, sorted(dates, reverse=True))

    def test_ids_are_stable_across_runs_and_case(self):
        a = _run(_pima_portal(), dp.fetch_pima, since='2026-10-01')
        b = _run(_pima_portal(), dp.fetch_pima, since='2026-10-01')
        self.assertEqual([dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in a],
                         [dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in b])
        r = _by_id(a)[f'pima_{DOMINOS_NI}']
        self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']), '71243fe5c5b70b4c')
        # the same restaurant printed in mixed case gives the same id
        facilities = _load('pima_facilities.json')
        for row in facilities['result']:
            if row['id'] == DOMINOS:
                row['facilityName'] = "Domino's Pizza #7626"
                row['siteAddress'] = '3682 W Orange Grove Rd,  Tucson AZ 85741'
        c = _by_id(_run(_pima_portal(facilities=facilities), dp.fetch_pima, since='2026-10-01'))
        r2 = c[f'pima_{DOMINOS_NI}']
        self.assertEqual(dp.make_restaurant_id(r2['name'], r2['address'], r2['city']), '71243fe5c5b70b4c')

    def test_limit_bounds_the_read_and_leaves_no_lag_note(self):
        portal = _pima_portal()
        records = _run(portal, dp.fetch_pima, limit=2)
        self.assertEqual(len(records), 2)
        self.assertEqual(len(portal.checklists_requested()), 2)
        self.assertLess(len(portal.lists_requested()), 7)
        self.assertNotIn('Pima County', dp._GROUP_NOTES)

    def test_source_lag_is_noted_after_a_complete_read(self):
        facilities = _load('pima_facilities.json')
        for row in facilities['result']:
            if row.get('lastInspectionDate'):
                row['lastInspectionDate'] = '2026-01-15T10:00:00.0000000-07:00'
        inspections = {fid: [_insp(f'{fid[:4]}-old', '2026-01-15T10:00:00.0000000-07:00')]
                       for fid in _pima_portal().inspections}
        portal = Portal(PIMA, facilities, inspections,
                        {i[0]['id']: _load('pima_results_clean.json') for i in inspections.values()})
        records = _run(portal, dp.fetch_pima)
        self.assertEqual(len(records), 7)
        self.assertIn('Pima County', dp._GROUP_NOTES)
        self.assertIn('2026-01-15', dp._GROUP_NOTES['Pima County'])
        # the fixture's real dates are 17 days old at capture time: no note from them
        dp._GROUP_NOTES.clear()
        with mock.patch.object(dp, 'SOURCE_LAG_NOTE_DAYS', 10 ** 6):
            _run(_pima_portal(), dp.fetch_pima)
        self.assertNotIn('Pima County', dp._GROUP_NOTES)

    def test_a_failed_or_short_facility_list_raises(self):
        with self.assertRaises(RuntimeError):
            _run(_pima_portal(facilities=dp.requests.ConnectionError('reset')), dp.fetch_pima)
        with self.assertRaises(RuntimeError):
            _run(_pima_portal(facilities=_Resp({'Message': 'An error has occurred.'}, 500)), dp.fetch_pima)
        with self.assertRaises(RuntimeError):
            _run(_pima_portal(facilities={'result': None}), dp.fetch_pima)
        with mock.patch.dict(PIMA, {'list_min_rows': 4000}):
            with self.assertRaises(RuntimeError):
                _run(_pima_portal(), dp.fetch_pima)
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_an_unread_checklist_drops_that_inspection_and_holds(self):
        portal = _pima_portal(checklists={
            DOMINOS_NI: _Resp({'Message': 'An error has occurred.'}, 500),      # transient
            BAG_NI: _Resp({'Message': 'An error has occurred.'}, 200),          # not a list
        })
        records = _run(portal, dp.fetch_pima, since='2026-10-01')
        self.assertEqual([r['source_id'] for r in records], [f'pima_{BAG_FAIL}'])
        self.assertEqual(portal.checklists_requested().count(DOMINOS_NI), dp.HH_RETRIES)
        self.assertEqual(dp._FRESHNESS_HOLDS, {PIMA_SRC: {f'2026-10-17|{DOMINOS_NI}': (True, False),
                                                          f'2026-10-06|{BAG_NI}': (True, False)}})
        self.assertNotIn('Pima County', dp._GROUP_STATUS)       # not a deadline cut

    def test_a_gone_checklist_drops_without_a_hold(self):
        portal = _pima_portal(checklists={DOMINOS_NI: _Resp({'Message': 'gone'}, 404)})
        records = _run(portal, dp.fetch_pima, since='2026-10-01')
        self.assertEqual(len(records), 2)
        self.assertEqual(portal.checklists_requested().count(DOMINOS_NI), 1)
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_an_unread_facility_list_holds_the_whole_window(self):
        portal = _pima_portal(inspections={BAG_O_CRAB: dp.requests.ConnectionError('reset')})
        records = _run(portal, dp.fetch_pima, since='2026-10-01')
        self.assertEqual([r['source_id'] for r in records], [f'pima_{DOMINOS_NI}'])
        # held at the window start (via _hold_window_start), counted, keyed by the facility
        self.assertEqual(dp._FRESHNESS_HOLDS, {PIMA_SRC: {f'2026-10-03|{BAG_O_CRAB}': (True, False)}})

    def test_a_deadline_before_the_detail_phase_ships_nothing_and_reports_skipped(self):
        dp._FETCH_DEADLINE = time.time() - 1
        portal = _pima_portal()
        records = _run(portal, dp.fetch_pima, since='2026-10-01')
        self.assertEqual(records, [])
        self.assertEqual(portal.lists_requested(), [])
        self.assertEqual(dp._GROUP_STATUS['Pima County'][0], 'skipped')
        self.assertEqual(dp._FRESHNESS_HOLDS, {PIMA_SRC: {'2026-10-03|facilities': (False, False)}})

    def test_a_deadline_during_the_checklist_phase_holds_the_unread_tail_uncounted(self):
        portal = _pima_portal()

        def expire(fid):
            if fid == DOMINOS:                             # the last facility list, oldest first
                dp._FETCH_DEADLINE = time.time() - 1
        portal.on_list_done = expire
        with mock.patch.object(dp, 'HH_WORKERS', 1):       # sequential, so the cut is deterministic
            records = _run(portal, dp.fetch_pima, since='2026-10-01')
        self.assertEqual(records, [])
        self.assertEqual(portal.checklists_requested(), [])
        self.assertEqual(dp._GROUP_STATUS['Pima County'][0], 'skipped')
        self.assertEqual(dp._FRESHNESS_HOLDS, {PIMA_SRC: {f'2026-10-06|{BAG_NI}': (False, False),
                                                          f'2026-10-16|{BAG_FAIL}': (False, False),
                                                          f'2026-10-17|{DOMINOS_NI}': (False, False)}})

    def test_unrated_visits_never_ship(self):
        portal = _pima_portal(inspections={RITAS: [
            _insp('sv', '2026-05-14T09:00:00.0000000-07:00', rating='Observations/Site Visit'),
            _insp('er', '2026-05-15T09:00:00.0000000-07:00', rating='Emergency Response'),
            _insp('nr', '2026-05-16T09:00:00.0000000-07:00', rating=None),
            _insp('ok', '2026-05-17T09:00:00.0000000-07:00', rating='Satisfactory'),
        ]})
        by_id = _by_id(_run(portal, dp.fetch_pima, since='2026-05-14'))
        ritas = {k for k, r in by_id.items() if r['name'] == "Rita's Italian Ice"}
        self.assertEqual(ritas, {'pima_ok'})
        for skipped in ('sv', 'er', 'nr'):
            self.assertNotIn(skipped, portal.checklists_requested())


# ── Lubbock ──────────────────────────────────────────────────────────────────

def _lbk_pages(rows, page_size, total=None):
    """Server-side view of the facility list: sorted lastInspectionDate desc
    (never-inspected rows last), cut into pages."""
    dated = sorted((r for r in rows if r.get('lastInspectionDate')),
                   key=lambda r: r['lastInspectionDate'], reverse=True)
    rows = dated + [r for r in rows if not r.get('lastInspectionDate')]
    total = len(rows) if total is None else total
    return [{'pageNumber': i // page_size, 'pageSize': page_size, 'totalNumberOfRecords': total,
             'result': rows[i:i + page_size]} for i in range(0, len(rows), page_size)]


def _lbk_portal(page_size=500, **overrides):
    rows = overrides.pop('rows', _load('lubbock_facilities.json')['result'])
    inspections = {
        OXXO: _load('lubbock_inspections.json'),
        FIESTA: [                                          # '505 N Gardner Lubbock, TX 79416'
            _insp('fc1', '2026-11-02T12:47:03.4620881-06:00', score=100, rating='INFO_REDACTED',
                  itype='Routine Inspection', number='INSP-023560', reason=' Compliance')],
        BAHAMA: [                                          # zip before the state
            _insp('bb1', '2026-05-07T12:15:53.5865702-05:00', score=91, rating='INFO_REDACTED',
                  itype='Routine Inspection', number='INSP-021900', reason=' Compliance')],
    }
    inspections.update(overrides.pop('inspections', {}))
    checklists = {OXXO_99: _load('lubbock_results_small.json'),
                  OXXO_88: _load('lubbock_results_score65.json')}
    for insp_list in inspections.values():
        for insp in (insp_list if isinstance(insp_list, list) else []):
            checklists.setdefault(insp['id'], _load('lubbock_results_small.json'))
    checklists.update(overrides.pop('checklists', {}))
    return Portal(LBK, _lbk_pages(rows, page_size), inspections, checklists)


class LubbockFetchTests(_Base):
    def test_list_request_shape_sorted_newest_first(self):
        portal = _lbk_portal()
        _run(portal, dp.fetch_lubbock, since='2026-10-26')
        method, path, body = portal.calls[0]
        self.assertEqual((method, path), ('POST', 'disclosure/program/facilities'))
        self.assertEqual(body, {'pageNumber': 0, 'pageSize': dp.HH_PAGE, 'criteria': '',
                                'sort': [{'field': 'lastInspectionDate', 'order': 'desc'}],
                                'disclosureProgramId': LBK['program'], 'fields': [], 'filters': []})

    def test_official_score_record(self):
        records = _run(_lbk_portal(), dp.fetch_lubbock, since='2026-01-01T00:00:00')
        r = _by_id(records)['lbk_INSP-019817']
        self.assertEqual(r, {
            'name': '(OXXO) DK #401',
            'address': '2902 4th St',
            'city': 'Lubbock',
            'state': 'TX',
            'zip': '79415',
            'latitude': 33.5926248,
            'longitude': -101.8794371,
            'inspection_date': '2026-01-21',
            'original_score': 99,
            'risk_score': 99,
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 2,
            'total_violations': 2,
            'violations': r['violations'],
            'inspection_type': 'Routine Inspection',
            'results': 'Grade A',
            'source': LBK_SRC,
            'source_url': f'https://clhd-portal.hedgerowsoftware.com/#/{LBK["program"]}/disclosure/'
                          f'facility/{OXXO}/inspection/{OXXO_99}',
            'source_id': 'lbk_INSP-019817',
            'metro': '',
        })
        self.assertEqual(r['violations'], dp._hh_violations(_load('lubbock_results_small.json')))

    def test_score_is_copied_not_computed_and_the_letter_follows_the_county_bounds(self):
        records = _run(_lbk_portal(), dp.fetch_lubbock)
        r = _by_id(records)['lbk_INSP-011693']            # score 88 with six citations (2P 2PF 2C)
        self.assertEqual((r['original_score'], r['risk_score']), (88, 88))
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'],
                          r['core_violations'], r['total_violations']), (2, 2, 2, 6))
        self.assertNotEqual(r['risk_score'], dp.calc_risk_score(r['violations'])[0])
        self.assertEqual(r['results'], 'Grade B')
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'B')
        self.assertEqual(dp._lbk_grade(100), 'A')
        self.assertEqual(dp._lbk_grade(80), 'B')
        self.assertEqual(dp._lbk_grade(79), 'C')
        self.assertEqual(dp._lbk_grade(65), 'F')

    def test_scope_temporary_booths_mobile_units_no_address_and_out_of_state_are_dropped(self):
        portal = _lbk_portal()
        records = _run(portal, dp.fetch_lubbock)
        names = {r['name'] for r in records}
        self.assertEqual(names, {'(OXXO) DK #401', 'Fiesta Center', "Bahama Buck's #1103"})
        self.assertEqual(len(records), 6)
        # only the three in-scope facilities had their inspection lists read
        self.assertEqual(len(portal.lists_requested()), 3)
        by_id = _by_id(records)
        self.assertEqual((by_id['lbk_INSP-023560']['address'], by_id['lbk_INSP-023560']['city'],
                          by_id['lbk_INSP-023560']['zip']), ('505 N Gardner', 'Lubbock', '79416'))
        self.assertEqual((by_id['lbk_INSP-021900']['address'], by_id['lbk_INSP-021900']['zip']),
                         ('2515 82nd St Suite B', '79423'))

    def test_pages_until_the_last_row_leaves_the_window(self):
        with mock.patch.object(dp, 'HH_PAGE', 2):
            portal = _lbk_portal(page_size=2)
            records = _run(portal, dp.fetch_lubbock, since='2026-03-01')
        posts = [c for c in portal.calls if c[0] == 'POST']
        # page 0: 2026-11-02, 2026-05-07; page 1: 2026-03-17, 2026-01-21 (< since) -> stop
        self.assertEqual([c[2]['pageNumber'] for c in posts], [0, 1])
        self.assertEqual({r['name'] for r in records}, {'Fiesta Center', "Bahama Buck's #1103"})
        self.assertTrue(all(r['inspection_date'] >= '2026-03-01' for r in records))

    def test_full_history_reads_every_page_and_dedupes_a_boundary_shift(self):
        rows = _load('lubbock_facilities.json')['result']
        with mock.patch.object(dp, 'HH_PAGE', 3):
            portal = _lbk_portal(page_size=3, rows=rows)
            # the server shifted: page 1 repeats the last row of page 0
            portal.facilities[1]['result'].insert(0, portal.facilities[0]['result'][-1])
            records = _run(portal, dp.fetch_lubbock)
        posts = [c for c in portal.calls if c[0] == 'POST']
        self.assertEqual(len(posts), 3)
        self.assertEqual(len(records), 6)
        self.assertEqual(len({r['source_id'] for r in records}), 6)
        self.assertEqual(portal.lists_requested().count(OXXO), 1)

    def test_a_failed_page_raises_instead_of_shipping_a_partial_window(self):
        with mock.patch.object(dp, 'HH_PAGE', 2):
            portal = _lbk_portal(page_size=2)
            portal.facilities[1] = _Resp({'Message': 'An error has occurred.'}, 500)
            with self.assertRaises(RuntimeError):
                _run(portal, dp.fetch_lubbock, since='2026-01-01')
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_source_lag_note(self):
        rows = _load('lubbock_facilities.json')['result']
        for row in rows:
            if row.get('lastInspectionDate'):
                row['lastInspectionDate'] = '2026-01-15T10:00:00.0000000-06:00'
        portal = _lbk_portal(rows=rows, inspections={
            OXXO: [_insp('o1', '2026-01-15T10:00:00.0000000-06:00', score=95, rating='INFO_REDACTED',
                         itype='Routine Inspection', number='INSP-1')],
            FIESTA: [], BAHAMA: []})
        records = _run(portal, dp.fetch_lubbock, since='2026-01-01')
        self.assertEqual(len(records), 1)
        self.assertIn('Lubbock', dp._GROUP_NOTES)
        self.assertIn('2026-01-15', dp._GROUP_NOTES['Lubbock'])

    def test_an_unscored_visit_without_citations_is_dropped_not_shipped_as_100(self):
        # (OXXO)'s row says last inspected 2026-01-21, so its list is read for a January window
        portal = _lbk_portal(inspections={OXXO: [
            _insp('zero', '2026-01-10T10:00:00.0000000-06:00', score=0, rating='INFO_REDACTED',
                  itype='Routine Inspection', number='INSP-Z'),
            _insp('nullv', '2026-01-11T10:00:00.0000000-06:00', score=None, rating='INFO_REDACTED',
                  itype='Routine Inspection', number='INSP-N'),
        ]}, checklists={'zero': [], 'nullv': _load('lubbock_results_small.json')})
        by_id = _by_id(_run(portal, dp.fetch_lubbock, since='2026-01-01'))
        self.assertNotIn('lbk_INSP-Z', by_id)
        computed = by_id['lbk_INSP-N']                      # no score but two Core citations: computed
        self.assertIsNone(computed['original_score'])
        self.assertEqual(computed['risk_score'], 98)
        self.assertEqual(computed['results'], '')

    def test_unread_checklist_drops_and_holds_with_the_inspection_number(self):
        portal = _lbk_portal(checklists={OXXO_99: dp.requests.Timeout('slow')})
        records = _run(portal, dp.fetch_lubbock, since='2026-01-01')
        self.assertNotIn('lbk_INSP-019817', _by_id(records))
        self.assertEqual(dp._FRESHNESS_HOLDS, {LBK_SRC: {f'2026-01-21|{OXXO_99}': (True, False)}})


# ── Mesquite ─────────────────────────────────────────────────────────────────

MATHIS = '59d7fb6c-dce2-4b48-8fbf-02bb8f3c0d55'
WILKINSON = 'd90ba6e7-6bbc-44cf-83bc-0185173feb97'
KAMIKAZE = '3ec10ed0-6de1-4897-a180-04ca74190800'
EXPRESS = 'c42eb68a-7417-4a58-ba65-149b8328e81f'            # never inspected (null hazardScore)


def _mesq_portal(**overrides):
    facilities = _load('mesquite_facilities.json')
    inspections = {
        SUBWAY: _load('mesquite_inspections_subway.json'),
        MATHIS: [_insp('m1', '2026-09-10T11:00:00.0000000-05:00', score=94, rating='INFO_REDACTED', itype=None)],
        WILKINSON: [_insp('w1', '2026-10-15T11:00:00.0000000-05:00', score=97, rating='INFO_REDACTED', itype=None)],
        KAMIKAZE: [_insp('k1', '2026-10-30T11:00:00.0000000-05:00', score=85, rating='INFO_REDACTED', itype=None)],
        EXPRESS: [],
    }
    inspections.update(overrides.pop('inspections', {}))
    checklists = {'42855a42-5770-47c3-bee0-25df32990801': _load('mesquite_results_one_item.json'),
                  '57969a16-ac2a-4387-a960-27951bd195f2': _load('mesquite_results_three_items.json'),
                  SUBWAY_OCT: _load('mesquite_results_one_item.json')}
    for insp_list in inspections.values():
        for insp in (insp_list if isinstance(insp_list, list) else []):
            checklists.setdefault(insp['id'], _load('mesquite_results_thawing.json'))
    checklists.update(overrides.pop('checklists', {}))
    return Portal(MESQ, facilities, inspections, checklists)


class MesquiteFetchTests(_Base):
    def test_every_facility_list_is_read_whatever_the_window(self):
        # the list hides lastInspectionDate, so the window can only be applied
        # per inspection; the list itself is read once, sorted by id
        portal = _mesq_portal()
        records = _run(portal, dp.fetch_mesquite, since='2026-10-01T00:00:00')
        self.assertEqual(portal.calls[0][2], {'sort': [{'field': 'id', 'order': 'asc'}],
                                              'disclosureProgramId': MESQ['program'],
                                              'fields': ['facilityName']})
        self.assertEqual(sorted(portal.lists_requested()), sorted([SUBWAY, MATHIS, WILKINSON, KAMIKAZE, EXPRESS]))
        self.assertEqual(sorted(portal.checklists_requested()), sorted([SUBWAY_OCT, 'w1', 'k1']))
        self.assertEqual([r['source_id'] for r in records], ['mesq_k1', f'mesq_{SUBWAY_OCT}', 'mesq_w1'])

    def test_record(self):
        records = _run(_mesq_portal(), dp.fetch_mesquite, since='2026-10-01')
        r = _by_id(records)[f'mesq_{SUBWAY_OCT}']
        self.assertEqual(r, {
            'name': 'Subway Restaurant #7504',
            'address': '3522 Gus Thomasson Rd #110',
            'city': 'Mesquite',
            'state': 'TX',
            'zip': '75150',
            'latitude': 32.8121788,
            'longitude': -96.6341255,
            'inspection_date': '2026-10-22',
            'original_score': 97,
            'risk_score': 97,
            'priority_violations': 1,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 1,
            'violations': [['food_handling', 'priority',
                            'Food Contact Surfaces and Returnables; Cleaned and Sanitized: '
                            'Food-contact surfaces clean to sight and touc']],
            'inspection_type': '',
            'results': '',
            'source': MESQ_SRC,
            'source_url': f'https://mesquite.hedgerowsoftware.com/#/{MESQ["program"]}/disclosure/'
                          f'facility/{SUBWAY}/inspection/{SUBWAY_OCT}',
            'source_id': f'mesq_{SUBWAY_OCT}',
            'metro': 'DFW',
        })
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'A')

    def test_missing_coordinates_are_none_for_the_geocoder(self):
        by_id = _by_id(_run(_mesq_portal(), dp.fetch_mesquite))
        m = by_id['mesq_m1']
        self.assertEqual((m['name'], m['address'], m['zip']), ('Mathis Donuts', '540 Clay Mathis Rd 180', '75181'))
        self.assertIsNone(m['latitude'])
        self.assertIsNone(m['longitude'])
        self.assertEqual(by_id['mesq_k1']['results'], '')
        self.assertEqual(by_id['mesq_k1']['risk_score'], 85)

    def test_full_history_and_ids(self):
        records = _run(_mesq_portal(), dp.fetch_mesquite)
        self.assertEqual(len(records), 8)                 # 5 Subway + Mathis + Wilkinson + Kamikaze
        subway = [r for r in records if r['name'] == 'Subway Restaurant #7504']
        self.assertEqual(len({dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in subway}), 1)
        self.assertEqual(dp.make_restaurant_id(subway[0]['name'], subway[0]['address'], subway[0]['city']),
                         'b00ab1005218bb6d')
        self.assertEqual([r['inspection_date'] for r in subway],
                         ['2026-10-22', '2026-08-18', '2026-06-05', '2026-02-02', '2025-11-20'])

    def test_a_deadline_cut_in_the_facility_phase_holds_the_window_and_reports_skipped(self):
        portal = _mesq_portal()
        seen = []
        real = dp._past_deadline

        def late():
            seen.append(1)
            return len(seen) > 2            # past the deadline once the first facility chunk started
        with mock.patch.object(dp, '_past_deadline', side_effect=late):
            records = _run(portal, dp.fetch_mesquite, since='2026-10-01')
        self.assertEqual(records, [])
        self.assertEqual(dp._GROUP_STATUS['Mesquite'][0], 'skipped')
        self.assertIn(MESQ_SRC, dp._FRESHNESS_HOLDS)
        self.assertIn('2026-10-03|facilities', dp._FRESHNESS_HOLDS[MESQ_SRC])
        self.assertEqual(dp._FRESHNESS_HOLDS[MESQ_SRC]['2026-10-03|facilities'], (False, False))
        self.assertIs(dp._past_deadline, real)


class PolicyTests(unittest.TestCase):
    def test_jev_policies_for_the_three_labels(self):
        # Pima's score is computed from tiers the county states; Lubbock's and
        # Mesquite's are official. None may be re-tiered.
        self.assertEqual(jr.score_policy(PIMA_SRC, None), 'source_tiers')
        self.assertEqual(jr.score_policy(LBK_SRC, 99), 'official')
        self.assertEqual(jr.score_policy(MESQ_SRC, 97), 'official')
        for src in (PIMA_SRC, LBK_SRC, MESQ_SRC):
            self.assertEqual(jr.tier_policy(src), 'fixed', src)

    def test_pacing_constants_are_modest(self):
        self.assertLessEqual(dp.HH_WORKERS, 4)
        self.assertGreater(dp.HH_DELAY, 0)
        self.assertGreaterEqual(dp.HH_RETRIES, 2)


if __name__ == '__main__':
    unittest.main()
