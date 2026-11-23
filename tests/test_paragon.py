"""Tests for the Paragon "Inspections Online" fetchers: Missoula, Gallatin and
Flathead counties MT and Kansas City MO (fetch_missoula, fetch_gallatin,
fetch_flathead, fetch_kansas_city), which share one client. No network:
requests.get is replaced by a fake Domino server that serves the fixtures
under tests/fixtures/paragon/ — captured live on 2026-11-22 (Montana) and
2026-11-23 (Kansas City):
  <slug>_list.htm      a real date-query result page cut down to a handful of
                       its rows (the fake server answers every window with the
                       rows dated inside it and the page's own header, count
                       rewritten);
  <slug>_dv_<UNID>.txt the field-value agent's answer for that inspection,
                       its captured values put in IO_DOC_FIELDS order (the
                       agent joins the values '~' in the order asked; the
                       Kansas City week capture did not ask fld_Program, which
                       is kw_Food on every row of the food query);
  <slug>_det_<UNID>.htm the public report, business phone blanked.
`datetime.now` is pinned to 2026-11-23 so nothing depends on today's date."""
import os
import re
import sys
import unittest
from datetime import datetime
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader as jr  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'paragon')
ROW = "<DIV class='gt row'>"
SOURCES = {slug: cfg['source'] for slug, cfg in dp.IO_PORTALS.items()}
FETCHERS = {'missoula': dp.fetch_missoula, 'gallatin': dp.fetch_gallatin,
            'flathead': dp.fetch_flathead, 'kansascity': dp.fetch_kansas_city}

# Missoula
MSO_OTHER = '6C5F33114450DA7287258E98007D573E'      # 'Other' at a hotel kitchen, 3 RFI of 4, 2026-10-22
MSO_LABOCA = '8B842A667E7E145187258EA6006537DE'     # clean Routine, types Food Service|Mobile, 2026-11-05
MSO_MOBILE = '695701D5EF0AFCCD87258EAD00694ED4'     # SKC TERIYAKI MOBILE 3, type 7. Mobile Food Service
MSO_PREOP = '92004FC30CF9225687258EA6005BDDFA'      # Pre-Operational, 2026-11-05
MSO_PORTER = 'B82CE56760D45F9987258EAD005FAE42'     # 'PORTER UNHERD COFFEE MOBILE' typed as a restaurant
MSO_DRAM = 'BF26195E41E0235F87258EA0006A27BD'       # 'DRAM SHOP CENTRAL, THE', 0 RFI of 1, 2026-10-30
MSO_DOUBLEK = '45661ECF01491DE487258EB40066A88C'    # DOUBLE K RANCH BAR, 8 RFI of 9, 2026-11-19
# Gallatin
GAL_BBQ = 'F205219422BE5FA686258EB5007E34F3'        # Complaint Inspection, 2 RFI of 10, 2026-11-20
GAL_SCHOOL = '8DE190F2306DE87F87258E9F005A8009'     # Amsterdam School (FR 13 School)
GAL_OTHER = '521E986FBBD7610186258EB40069D105'      # 'Other', nothing cited
GAL_TEST = 'F41CBBE11ABD815786258EB3005EBC01'       # '000 Food Service Test', Site Visit
GAL_SHOPPE = 'E30607DCBF0A780386258E9C0069B8CB'     # 'Shoppe Coffeehouse Bakery (The)', Ownership Change 3/4
GAL_TANGLE = 'B50ADC369F2FF5D687258EA3007D9894'     # Complaint Inspection, 4 RFI of 7
# Flathead (county letter in the list row)
FHD_FIREBRAND = 'FF87AD116E324A1E87258EAD005B4539'  # D+, 4 RFI of 9
FHD_REM_CPLUS = 'FC10830F72A92EB387258E98006CD808'  # C+, Ad-Hoc Routine, 2 RFI of 11
FHD_BUFFET = 'B6EA7D1C1D99206A87258E9E0063A560'     # B+, 2 RFI of 4
FHD_SUNLIFE = 'B6C2BDAEF387076687258EB300680AEA'    # B+, 1 RFI of 4
FHD_CHICK = '9ED3E6BBE8CE4B8587258EB200638151'      # N/A, 1 RFI of 3
FHD_NUTHUT = '88A9CBA29779329087258EAE00743650'     # component 'Mobile (Trailer)'
FHD_SCHOOL = '8888B7350ECFEE3E87258EA50064F477'     # 18. School Food
FHD_REM_FU = 'F97849A4E160CE2987258E9F00690F62'     # A-, Follow-Up, 1 RFI of 2
# Kansas City
KC_PFC = 'D2C896A57A592B1286258EAF0001D7B4'         # 7 critical of 11, dead roaches and mice
KC_FOX = '0B18AD23E7AB76A286258EAF000711EF'         # 0 critical of 2 (vendor score 100.0)
KC_SCHOOL = 'DD3A8F9475A2EBD486258EAD005AAB55'      # 'Fee Exempt' elementary school
KC_OURHOUSE = 'CA91C8DDF18FF2C686258EAB006DAD8C'    # Complaint, 3 critical of 4, mice droppings
KC_MARYS = '2BC7A98ADAAA22DC86258EAF005BB76C'       # Follow-Up, nothing cited (vendor score 97.4)
KC_UGLY = '08C79DDE0C8BBD6886258EAE006C36FE'        # Follow-Up, 1 critical of 1, rodent droppings
KC_PREOP = '496AA040B44049B986258EAB006A9ED2'       # Pre-Operational


def _read(name, binary=False):
    with open(os.path.join(FIXTURES, name), 'rb' if binary else 'r',
              **({} if binary else {'encoding': 'latin-1'})) as fh:
        return fh.read()


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 11, 23, 12, 0, 0)


class _Resp:
    def __init__(self, body, status=200):
        self.status_code = status
        self.content = body if isinstance(body, bytes) else body.encode('latin-1')


def _iso(us):
    return f'{us[6:10]}-{us[0:2]}-{us[3:5]}'


class Server:
    """A fake inspectionsonline.us database for one portal. `dv` and `det`
    map an inspection UNID to its answer (text, _Resp or exception);
    `list_hook(start, end)` may answer a list query instead of the
    fixture rows; `rows` can be edited per test."""

    def __init__(self, slug):
        self.slug = slug
        self.prefix = dp.IO_HOST + dp.IO_PORTALS[slug]['db'] + '/'
        page = _read(f'{slug}_list.htm')
        self.head, *rows = page.split(ROW)
        last, _, self.tail = rows[-1].partition('</DIV></DIV>')
        rows[-1] = last + '</DIV></DIV>'
        self.rows = rows
        self.dv, self.det = {}, {}
        for name in os.listdir(FIXTURES):
            m = re.match(rf'{slug}_(dv|det)_([0-9A-F]{{32}})\.', name)
            if m:
                (self.dv if m.group(1) == 'dv' else self.det)[m.group(2)] = \
                    _read(name, binary=m.group(1) == 'det')
        self.calls = []
        self.list_hook = None

    def page(self, rows, header=None):
        head = re.sub(r'<h4>[^<]*</h4>', header or f'<h4>{len(rows)} documents found</h4>', self.head)
        return head + ''.join(ROW + r for r in rows) + self.tail

    @staticmethod
    def row_date(row):
        return _iso(re.search(r'>(\d\d/\d\d/\d{4})</A>', row).group(1))

    def _answer(self, table, key):
        got = table.get(key)
        if isinstance(got, Exception):
            raise got
        if isinstance(got, _Resp):
            return got
        if got is None:
            return _Resp('<html><body>Not found</body></html>', 404)
        return _Resp(got)

    def get(self, url, headers=None, timeout=None):
        assert url.startswith(self.prefix), url
        assert headers['User-Agent'].startswith('Mozilla/5.0'), headers
        path = url[len(self.prefix):]
        self.calls.append(path)
        if path.startswith('vw_InspectionsPubSumm-NT?SearchView'):
            assert '[fld_Program]+CONTAINS+kw_Food' in path and 'SearchMax=0' in path, path
            m = re.search(r'%3E=(\d\d/\d\d/\d{4})\+AND\+\[fld_InspectionDate\]%3C=(\d\d/\d\d/\d{4})$', path)
            start, end = _iso(m.group(1)), _iso(m.group(2))
            if self.list_hook:
                got = self.list_hook(start, end)
                if got is not None:
                    if isinstance(got, Exception):
                        raise got
                    return got if isinstance(got, _Resp) else _Resp(got)
            return _Resp(self.page([r for r in self.rows if start <= self.row_date(r) <= end]))
        if path.startswith('(ag_getDocValues)?OpenAgent&xx_UNID='):
            unid = re.search(r'xx_UNID=([0-9A-F]{32})', path).group(1)
            assert path.endswith('xx_fldList=' + '~'.join(dp.IO_DOC_FIELDS)), path
            return self._answer(self.dv, unid)
        if path.startswith('(ag_dspPubDetail)?OpenAgent&pUNID='):
            return self._answer(self.det, path.rsplit('=', 1)[1])
        raise AssertionError(f'unexpected request {path}')

    def lists(self):
        return [c for c in self.calls if c.startswith('vw_')]

    def docvals_requested(self):
        return [re.search(r'xx_UNID=([0-9A-F]{32})', c).group(1) for c in self.calls if 'getDocValues' in c]

    def reports_requested(self):
        return [c.rsplit('=', 1)[1] for c in self.calls if 'dspPubDetail' in c]


def _run(server, since='2026-11-01', limit=None):
    with mock.patch.object(dp.requests, 'get', side_effect=server.get), \
            mock.patch.object(dp.time, 'sleep'), \
            mock.patch.object(dp, 'datetime', FixedDatetime):
        return FETCHERS[server.slug](since_date=since, limit=limit)


def _by_unid(records):
    return {r['source_id'].split('_', 1)[1]: r for r in records}


class _Base(unittest.TestCase):
    def setUp(self):
        self._saved = (dp._FRESHNESS, dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
                       dict(dp._GROUP_NOTES), dp._FETCH_DEADLINE, set(dp.SKIP_KNOWN_INSPECTIONS))
        dp._FRESHNESS = None
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._GROUP_NOTES.clear()
        dp._FETCH_DEADLINE = None
        dp.SKIP_KNOWN_INSPECTIONS.clear()
        dp._IO_STREAK.clear()
        dp._IO_BLOCKED.clear()

    def tearDown(self):
        (dp._FRESHNESS, holds, status, notes, dp._FETCH_DEADLINE, known) = self._saved
        dp._FRESHNESS_HOLDS.clear()
        dp._FRESHNESS_HOLDS.update(holds)
        dp._GROUP_STATUS.clear()
        dp._GROUP_STATUS.update(status)
        dp._GROUP_NOTES.clear()
        dp._GROUP_NOTES.update(notes)
        dp.SKIP_KNOWN_INSPECTIONS.clear()
        dp.SKIP_KNOWN_INSPECTIONS.update(known)

    def holds(self, slug):
        return dict(dp._FRESHNESS_HOLDS.get(SOURCES[slug], {}))

    def status(self, slug):
        return dp._GROUP_STATUS.get(dp.IO_PORTALS[slug]['group'])


class ParserTests(_Base):
    def test_list_page_rows(self):
        found, capped, rows = dp._io_parse_list(_read('flathead_list.htm'))
        self.assertEqual((found, capped, len(rows)), (8, False, 8))
        by = {r['unid']: r for r in rows}
        self.assertEqual(by[FHD_FIREBRAND], {'unid': FHD_FIREBRAND, 'type': 'Routine', 'us_date': '11/12/2026',
                                              'date': '2026-11-12', 'count': '4 RFI', 'score': 'D+'})
        self.assertEqual(by[FHD_CHICK]['score'], 'N/A')
        self.assertEqual(by[FHD_REM_CPLUS]['type'], 'Ad-Hoc Routine')
        _, _, kc = dp._io_parse_list(_read('kansascity_list.htm'))
        self.assertEqual({r['unid']: r['count'] for r in kc}[KC_PFC], '7 critical')

    def test_list_headers(self):
        head = "<h4>%s</h4><hr>REF"
        self.assertEqual(dp._io_parse_list(head % '0 documents found'), (0, False, []))
        self.assertEqual(dp._io_parse_list(head % '1 document found')[:2], (1, False))
        self.assertEqual(dp._io_parse_list(head % '1000 documents found')[:2], (1000, True))
        self.assertEqual(dp._io_parse_list(head % '2 documents returned; 1000 found')[:2], (1000, True))
        with self.assertRaises(ValueError):
            dp._io_parse_list('<html><body>Error: the server is busy</body></html>')
        with self.assertRaises(ValueError):
            dp._io_parse_list((head % '1 document found') + ROW + '<DIV>garbled</DIV>')

    def test_docvalues_shape(self):
        dv = dp._io_parse_docvalues(_read(f'flathead_dv_{FHD_FIREBRAND}.txt'))
        self.assertEqual(dv['fld_EstabName'], 'Firebrand Lounge, The')
        self.assertEqual(dv['fld_FaciName'], 'Food Service')
        self.assertEqual((dv['fld_AllcvCounter'], dv['fld_AllvCounter']), ('4', '9'))
        for bad in ('', 'Error 500', '~a~b~c', 'x' + _read(f'flathead_dv_{FHD_FIREBRAND}.txt'),
                    _read(f'flathead_dv_{FHD_FIREBRAND}.txt') + '~extra'):
            with self.assertRaises(ValueError):
                dp._io_parse_docvalues(bad)

    def test_names_and_titles(self):
        self.assertEqual(dp._io_name('1400 HOUSE, THE'), 'The 1400 House')
        self.assertEqual(dp._io_name('Bunker Bar & Grill (The)'), 'The Bunker Bar & Grill')
        self.assertEqual(dp._io_name("JAY'S FOOD STORE    "), "Jay's Food Store")
        self.assertEqual(dp._io_name('P. F. Chang\'s China Bistro'), "P. F. Chang's China Bistro")
        self.assertEqual(dp._io_short_title('(A & B) Cold time/temperature control for safety food not held.'),
                         'Cold time/temperature control for safety food not held')
        self.assertEqual(dp._io_short_title('(A-E) Outer openings not protected against the entry of insects.'),
                         'Outer openings not protected against the entry of insects')
        self.assertEqual(dp._io_short_title('(All) Ventilation systems not maintained clean.'),
                         'Ventilation systems not maintained clean')
        self.assertEqual(dp._io_short_title('Hand Drying Provision - Each handwashing sink, or group of '
                                            'adjacent sinks, is not provided with ...'), 'Hand Drying Provision')
        # a dash with a long head is part of the title
        long_head = 'Food not protected from all sources of contamination during storage and display - other'
        self.assertEqual(dp._io_short_title(long_head), long_head)

    def test_report_parse(self):
        page = dp._io_decode(_read(f'kansascity_det_{KC_PFC}.htm', binary=True))
        v = dp._io_parse_report(page, '2026-11-13')
        self.assertEqual(len(v), 11)
        self.assertEqual(sum(1 for x in v if x[1] == 'priority'), 7)
        self.assertEqual({x[1] for x in v}, {'priority', 'core'})
        self.assertTrue(v[0][2].startswith('Using a Handwashing Sink: Observed cart blocking dish area'))
        self.assertTrue(all('\n' not in x[2] and len(x[2]) <= 500 for x in v))
        # the boilerplate RECOMMENDED RESOLUTION paragraph never ships
        self.assertFalse(any('shall be removed from control devices' in x[2] for x in v))
        with self.assertRaises(ValueError):
            dp._io_parse_report(page, '2026-11-12')            # a report of another day
        with self.assertRaises(ValueError):
            dp._io_parse_report(page.replace('<B>CRITICAL</B>', '<B>MAJOR</B>', 1), '2026-11-13')
        with self.assertRaises(ValueError):
            dp._io_parse_report('<html>Notes database error</html>', '2026-11-13')
        clean = dp._io_decode(_read(f'kansascity_det_{KC_MARYS}.htm', binary=True))
        self.assertEqual(dp._io_parse_report(clean, '2026-11-14'), [])

    def test_decode(self):
        self.assertEqual(dp._io_decode('Café'.encode('cp1252')), 'Café')
        self.assertEqual(dp._io_decode('Café'.encode('utf-8')), 'Café')

    def test_throttle_spaces_requests(self):
        clock = [1000.0]
        slept = []

        def sleep(s):
            slept.append(s)
            clock[0] += s
        with mock.patch.object(dp.time, 'time', side_effect=lambda: clock[0]), \
                mock.patch.object(dp.time, 'sleep', side_effect=sleep):
            dp._IO_LAST_REQUEST[0] = 0.0
            dp._io_throttle()
            clock[0] += 0.25
            dp._io_throttle()
        self.assertEqual(len(slept), 1)
        self.assertAlmostEqual(slept[0], dp.IO_MIN_INTERVAL - 0.25)


class MissoulaTests(_Base):
    def test_window_scope_and_scores(self):
        s = Server('missoula')
        recs = _run(s, since='2026-11-01')
        by = _by_unid(recs)
        # the window starts IO_MARGIN_DAYS before the one asked for, in 92-day chunks to today
        self.assertEqual(s.lists()[0].rsplit('Query=', 1)[1],
                         '[fld_Program]+CONTAINS+kw_Food+AND+[fld_InspectionDate]%3E=10/18/2026'
                         '+AND+[fld_InspectionDate]%3C=11/23/2026')
        self.assertEqual(set(by), {MSO_OTHER, MSO_LABOCA, MSO_DRAM, MSO_DOUBLEK})
        # a pre-opening visit costs nothing; mobile units cost the field values only
        self.assertNotIn(MSO_PREOP, s.docvals_requested())
        self.assertNotIn(MSO_MOBILE, s.reports_requested())
        self.assertNotIn(MSO_PORTER, s.reports_requested())
        other = by[MSO_OTHER]
        self.assertEqual((other['risk_score'], other['priority_violations'], other['core_violations']), (84, 3, 1))
        self.assertEqual(other['inspection_type'], 'Other')
        self.assertEqual(other['name'], 'La Quinta Inn - Food')
        self.assertEqual((other['city'], other['state'], other['zip']), ('Missoula', 'MT', '59808'))
        self.assertEqual(other['source'], 'Missoula Public Health')
        self.assertEqual(other['source_url'], 'https://inspectionsonline.us/mt/missoulamissoula/inspect.nsf/'
                                              f'(ag_dspPubDetail)?OpenAgent&pUNID={MSO_OTHER}')
        self.assertIsNone(other['original_score'])              # the hidden vendor letter never ships
        self.assertEqual(other['results'], '')
        self.assertEqual((other['latitude'], other['longitude']), (None, None))
        self.assertEqual(by[MSO_DRAM]['name'], 'The Dram Shop Central')
        self.assertEqual(by[MSO_DRAM]['risk_score'], 99)
        self.assertEqual(by[MSO_LABOCA]['risk_score'], 100)      # Food Service|Mobile: a fixed premises too
        self.assertEqual(by[MSO_DOUBLEK]['risk_score'], 59)      # 100 - 8x5 - 1
        self.assertEqual(dp.calculate_vetted_grade(59, dp.build_violation_text(by[MSO_DOUBLEK]['violations'])), 'F')
        # newest day first
        self.assertEqual([r['inspection_date'] for r in recs], sorted((r['inspection_date'] for r in recs),
                                                                       reverse=True))
        self.assertEqual(self.holds('missoula'), {})
        self.assertIsNone(self.status('missoula'))

    def test_record_keys_match_the_model(self):
        rec = _run(Server('missoula'))[0]
        self.assertEqual(set(rec), {
            'name', 'address', 'city', 'state', 'zip', 'latitude', 'longitude', 'inspection_date',
            'original_score', 'risk_score', 'priority_violations', 'priority_foundation_violations',
            'core_violations', 'total_violations', 'violations', 'inspection_type', 'results', 'source',
            'source_url', 'source_id', 'metro'})
        for v in rec['violations']:
            self.assertIn(v[1], ('priority', 'core'))

    def test_practice_facility_is_skipped(self):
        s = Server('missoula')
        s.dv[MSO_DOUBLEK] = s.dv[MSO_DOUBLEK].replace('DOUBLE K RANCH BAR', 'Practice Food New')
        by = _by_unid(_run(s, since='2026-11-01'))
        self.assertNotIn(MSO_DOUBLEK, by)
        self.assertNotIn(MSO_DOUBLEK, s.reports_requested())
        self.assertIsNotNone(dp.IO_TEST_NAME_RE.search('100 Practice Drive'))
        self.assertIsNone(dp.IO_TEST_NAME_RE.search('Practice Range Grill'))

    def test_skip_known_only_in_the_margin(self):
        s = Server('missoula')
        known = [(dp.make_restaurant_id('La Quinta Inn - Food', '5059 N Reserve', 'Missoula'), '2026-10-22'),
                 (dp.make_restaurant_id('Double K Ranch Bar', '117 S Higgins Ave', 'Missoula'), '2026-11-19')]
        dp.SKIP_KNOWN_INSPECTIONS.update(known)
        recs = _run(s, since='2026-11-01')
        by = _by_unid(recs)
        self.assertNotIn(MSO_OTHER, by)                      # in the margin and already stored
        self.assertNotIn(MSO_OTHER, s.reports_requested())
        self.assertIn(MSO_DOUBLEK, by)                       # in the window: always read

    def test_test_mode_reads_the_newest(self):
        s = Server('missoula')
        recs = _run(s, since=None, limit=2)
        self.assertIn('%3E=10/24/2026+AND', s.lists()[0])
        # the newest two listed rows are Double K (11-19) and Porter (11-12, a mobile unit, skipped)
        self.assertEqual({r['source_id'] for r in recs}, {f'mso_{MSO_DOUBLEK}'})
        self.assertEqual(set(s.docvals_requested()), {MSO_DOUBLEK, MSO_PORTER})
        self.assertEqual(self.holds('missoula'), {})

    def test_limit_with_a_window_keeps_the_oldest(self):
        s = Server('missoula')
        recs = _run(s, since='2026-11-01', limit=1)
        self.assertEqual([r['source_id'] for r in recs], [f'mso_{MSO_OTHER}'])


class GallatinTests(_Base):
    def test_scope_and_names(self):
        s = Server('gallatin')
        by = _by_unid(_run(s, since='2026-11-01'))
        self.assertEqual(set(by), {GAL_BBQ, GAL_SHOPPE, GAL_TANGLE})
        self.assertNotIn(GAL_OTHER, s.reports_requested())     # an uncited 'Other' is a note, not a clean 100
        self.assertNotIn(GAL_TEST, s.reports_requested())      # the county's test facility
        self.assertNotIn(GAL_SCHOOL, s.reports_requested())
        self.assertEqual(by[GAL_SHOPPE]['name'], 'The Shoppe Coffeehouse Bakery')
        self.assertEqual(by[GAL_SHOPPE]['inspection_type'], 'Ownership Change')
        self.assertEqual(by[GAL_BBQ]['risk_score'], 82)        # 100 - 2x5 - 8
        self.assertEqual(by[GAL_TANGLE]['risk_score'], 77)     # 100 - 4x5 - 3
        self.assertEqual(by[GAL_BBQ]['source'], 'Gallatin City-County Health Department')

    def test_cited_other_ships(self):
        s = Server('gallatin')
        s.dv[GAL_OTHER] = s.dv[GAL_OTHER].replace('~0~0~kw_Food', '~0~1~kw_Food')
        recs = _run(s, since='2026-11-01')
        # a cited 'Other' is read; this report prints no line against the
        # counter's 1, so it is unread: held, not shipped as clean
        self.assertIn(GAL_OTHER, s.reports_requested())
        self.assertNotIn(GAL_OTHER, _by_unid(recs))
        self.assertIn(f'2026-11-19|{GAL_OTHER}', self.holds('gallatin'))


class FlatheadTests(_Base):
    def test_county_band_caps_the_computed_score(self):
        s = Server('flathead')
        by = _by_unid(_run(s, since='2026-11-01'))
        self.assertEqual(set(by), {FHD_FIREBRAND, FHD_REM_CPLUS, FHD_BUFFET, FHD_SUNLIFE, FHD_CHICK, FHD_REM_FU})
        expect = {
            FHD_FIREBRAND: (69, 'Grade D+'),    # 100 - 4x5 - 5 = 75, the county's D+ caps at 69
            FHD_REM_CPLUS: (79, 'Grade C+'),    # 100 - 2x5 - 9 = 81 -> 79
            FHD_BUFFET: (88, 'Grade B+'),       # 88, under the B cap
            FHD_SUNLIFE: (89, 'Grade B+'),      # 92 -> 89
            FHD_CHICK: (93, ''),                # N/A: no band, no cap
            FHD_REM_FU: (94, 'Grade A-'),       # A-range letters never cap
        }
        for unid, (risk, results) in expect.items():
            self.assertEqual((by[unid]['risk_score'], by[unid]['results']), (risk, results), unid)
            self.assertIsNone(by[unid]['original_score'])
        fb = by[FHD_FIREBRAND]
        self.assertEqual(fb['name'], 'The Firebrand Lounge')
        self.assertEqual((fb['address'], fb['city']), ('650 East 3rd Street', 'Whitefish'))
        self.assertEqual(dp.calculate_vetted_grade(fb['risk_score'], dp.build_violation_text(fb['violations'])), 'F')
        self.assertEqual(dp.calculate_vetted_grade(79, ''), 'C')
        # the mobile component and the school kitchen do not ship
        self.assertNotIn(FHD_NUTHUT, s.reports_requested())
        self.assertNotIn(FHD_SCHOOL, s.reports_requested())

    def test_unknown_county_grade_is_unread(self):
        s = Server('flathead')
        s.rows = [r.replace('> D+</A>', '>Z</A>') if FHD_FIREBRAND in r else r for r in s.rows]
        by = _by_unid(_run(s, since='2026-11-01'))
        self.assertNotIn(FHD_FIREBRAND, by)
        self.assertIn(f'2026-11-12|{FHD_FIREBRAND}', self.holds('flathead'))

    def test_grade_caps_cover_the_published_scale(self):
        self.assertEqual(sorted(dp.IO_GRADE_RISK_CAP), sorted(
            ['A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'F']))
        for letter, cap in dp.IO_GRADE_RISK_CAP.items():
            grade = 'A' if cap is None else dp.calculate_vetted_grade(cap, '')
            self.assertEqual(grade, {'A': 'A', 'B': 'B', 'C': 'C', 'D': 'F', 'F': 'F'}[letter[0]], letter)


class KansasCityTests(_Base):
    def test_week_and_scores(self):
        s = Server('kansascity')
        recs = _run(s, since='2026-11-09')
        by = _by_unid(recs)
        self.assertEqual(set(by), {KC_PFC, KC_FOX, KC_OURHOUSE, KC_MARYS, KC_UGLY})
        # 7-day windows from the margin start to today
        self.assertEqual(len(s.lists()), 5)
        self.assertIn('%3E=10/26/2026+AND+[fld_InspectionDate]%3C=11/01/2026', s.lists()[0])
        self.assertNotIn(KC_SCHOOL, s.reports_requested())     # 'Fee Exempt' school kitchen
        self.assertNotIn(KC_PREOP, s.docvals_requested())
        pfc = by[KC_PFC]
        self.assertEqual((pfc['risk_score'], pfc['priority_violations'], pfc['core_violations']), (61, 7, 4))
        self.assertEqual((pfc['address'], pfc['city'], pfc['state'], pfc['zip']),
                         ('102 W 47th St.', 'Kansas City', 'MO', '64112'))
        self.assertEqual(pfc['source'], 'Kansas City MO Health Department')
        self.assertIsNone(pfc['original_score'])                # the switched-off vendor score never ships
        self.assertEqual(by[KC_FOX]['risk_score'], 98)          # vendor column said 100.0
        self.assertEqual(by[KC_MARYS]['risk_score'], 100)       # vendor column said 97.4
        self.assertEqual(by[KC_OURHOUSE]['inspection_type'], 'Complaint')
        self.assertEqual(by[KC_UGLY]['inspection_type'], 'Follow-Up')
        # named findings force the F; the page's titles do not
        for unid in (KC_PFC, KC_OURHOUSE, KC_UGLY):
            self.assertTrue(dp.has_active_pest_or_sewage(dp.build_violation_text(by[unid]['violations'])), unid)
        self.assertFalse(dp.has_active_pest_or_sewage(dp.build_violation_text(by[KC_FOX]['violations'])))


class PestTitleTests(unittest.TestCase):
    """Titles the four databases print that name pests (every one seen in
    the 2026-11 captures) never force an F on their own, alone or joined
    after a line that ends in an evidence word."""
    TITLES = [
        'Removing Dead or Trapped Birds, Insects, Rodents, and Other Pests',
        'Effective pest control measures not in place',
        'Insect control devices do not retain insect within device and/or are not properly installed',
        'Openings to outside not protected against entry of pest as specified in the Food Code; and/or '
        'Protective barriers not provided for exterior walls/roofs',
        'Outer openings not protected against the entry of insects and/or rodents',
        'Insect control devices improperly designed or installed',
    ]

    def test_titles_alone(self):
        for t in self.TITLES:
            self.assertFalse(dp.has_active_pest_or_sewage(t), t)
            self.assertFalse(dp.has_active_pest_or_sewage('Hand soap not observed|||' + t), t)

    def test_titles_from_the_report_pages(self):
        for name in sorted(os.listdir(FIXTURES)):
            if '_det_' not in name:
                continue
            page = dp._io_decode(_read(name, binary=True))
            for raw in re.findall(r'<I>(.*?)</I>', page, re.S):
                title = dp._io_short_title(dp._io_text(raw))
                self.assertFalse(dp.has_active_pest_or_sewage(title), (name, title))

    def test_observation_still_forces(self):
        self.assertTrue(dp.has_active_pest_or_sewage(
            'Removing Dead or Trapped Birds, Insects, Rodents, and Other Pests: Observed dead roaches on sticky '
            'traps throughout downstairs.'))


class FailureTests(_Base):
    def test_capped_window_is_split(self):
        s = Server('kansascity')
        capped = []

        def hook(start, end):
            if start != end and start <= '2026-11-13' <= end:
                capped.append((start, end))
                return s.page([], header='<h4>1000 documents found</h4>')
            return None
        s.list_hook = hook
        by = _by_unid(_run(s, since='2026-11-09'))
        self.assertEqual(set(by), {KC_PFC, KC_FOX, KC_OURHOUSE, KC_MARYS, KC_UGLY})
        self.assertTrue(capped)
        self.assertIn('%3E=11/13/2026+AND+[fld_InspectionDate]%3C=11/13/2026', ''.join(s.lists()))

    def test_capped_single_day_raises(self):
        s = Server('kansascity')
        s.list_hook = lambda a, b: s.page([], header='<h4>2 documents returned; 1000 found</h4>')
        with self.assertRaises(RuntimeError):
            _run(s, since='2026-11-09')
        self.assertEqual(s.docvals_requested(), [])

    def test_short_list_is_reread_then_raises(self):
        s = Server('missoula')
        answers = iter([s.page(s.rows[:2], header='<h4>5 documents found</h4>')])
        s.list_hook = lambda a, b: next(answers, None)
        self.assertTrue(_run(s))                              # the re-read matched
        s = Server('missoula')
        s.list_hook = lambda a, b: s.page(s.rows[:2], header='<h4>5 documents found</h4>')
        with self.assertRaises(RuntimeError):
            _run(s)
        self.assertEqual(len(s.lists()), 1 + dp.IO_LIST_REREADS)
        self.assertEqual(s.docvals_requested(), [])

    def test_rows_outside_the_window_raise(self):
        s = Server('missoula')
        s.list_hook = lambda a, b: s.page(s.rows)               # the filter ignored
        with self.assertRaises(RuntimeError):
            _run(s, since='2026-11-15')

    def test_list_http_failure_raises(self):
        s = Server('missoula')
        s.list_hook = lambda a, b: _Resp('busy', 503)
        with self.assertRaises(RuntimeError):
            _run(s)

    def test_list_deadline_raises(self):
        s = Server('kansascity')
        dp._FETCH_DEADLINE = 1.0
        with self.assertRaises(RuntimeError):
            _run(s, since='2026-11-09')
        self.assertEqual(s.calls, [])

    def test_transient_failures_hold_and_404_does_not(self):
        s = Server('kansascity')
        s.dv[KC_FOX] = _Resp('Service Unavailable', 503)
        s.det[KC_OURHOUSE] = dp.requests.ConnectionError('reset')
        del s.dv[KC_UGLY]                                        # 404: gone from the database
        recs = _run(s, since='2026-11-09')
        by = _by_unid(recs)
        self.assertEqual(set(by), {KC_PFC, KC_MARYS})
        holds = self.holds('kansascity')
        self.assertEqual(holds, {f'2026-11-13|{KC_FOX}': (True, False), f'2026-11-10|{KC_OURHOUSE}': (True, False)})
        self.assertEqual(s.docvals_requested().count(KC_FOX), dp.IO_RETRIES)
        self.assertIsNone(self.status('kansascity'))

    def test_counter_mismatch_and_bad_shapes_are_unread(self):
        s = Server('kansascity')
        s.dv[KC_FOX] = s.dv[KC_FOX].replace('~0~2~', '~0~3~')            # a row more than the report shows
        s.dv[KC_MARYS] = s.dv[KC_MARYS].replace('~0~0~', '~~~')           # counters missing
        s.dv[KC_UGLY] = 'Agent error: field list too long'
        s.det[KC_OURHOUSE] = s.det[KC_OURHOUSE].replace(b'<B>CRITICAL</B>', b'<B></B>', 1)   # tier lost
        by = _by_unid(_run(s, since='2026-11-09'))
        self.assertEqual(set(by), {KC_PFC})
        self.assertEqual(set(self.holds('kansascity')), {
            f'2026-11-13|{KC_FOX}', f'2026-11-14|{KC_MARYS}', f'2026-11-13|{KC_UGLY}', f'2026-11-10|{KC_OURHOUSE}'})

    def test_kansas_city_city_is_fixed(self):
        s = Server('kansascity')
        s.dv[KC_FOX] = s.dv[KC_FOX].replace('~Kansas City ~', '~Kanas City~')
        self.assertEqual(_by_unid(_run(s, since='2026-11-09'))[KC_FOX]['city'], 'Kansas City')

    def test_stale_counters_below_the_rows_ship_every_row(self):
        # Kansas City's Thai House: counters 6 critical of 7 above 10 printed rows
        s = Server('kansascity')
        s.dv[KC_PFC] = s.dv[KC_PFC].replace('~7~11~', '~6~7~')
        by = _by_unid(_run(s, since='2026-11-09'))
        self.assertEqual(by[KC_PFC]['total_violations'], 11)
        self.assertEqual(by[KC_PFC]['risk_score'], 61)
        self.assertEqual(self.holds('kansascity'), {})

    def test_document_dated_another_day_is_unread(self):
        s = Server('kansascity')
        s.dv[KC_FOX] = s.dv[KC_FOX].replace('11/13/2026', '11/12/2026')
        self.assertNotIn(KC_FOX, _by_unid(_run(s, since='2026-11-09')))
        self.assertIn(f'2026-11-13|{KC_FOX}', self.holds('kansascity'))

    def test_breaker_blocks_and_holds_the_rest_uncounted(self):
        s = Server('kansascity')
        for unid in s.dv:
            s.dv[unid] = _Resp('Forbidden', 403)
        recs = _run(s, since='2026-11-09')
        self.assertEqual(recs, [])
        status = self.status('kansascity')
        self.assertEqual(status[0], 'blocked')
        holds = self.holds('kansascity')
        counted = [k for k, v in holds.items() if v[0]]
        self.assertEqual(len(counted), dp.IO_BREAKER_TRIP)      # tried and failed
        self.assertEqual(len(holds), 6)                          # the rest held, not counted
        self.assertEqual(len(s.docvals_requested()), dp.IO_BREAKER_TRIP * dp.IO_RETRIES)

    def test_deadline_cuts_the_newest_tail(self):
        s = Server('kansascity')
        reads = []

        def get(url, headers=None, timeout=None):
            resp = s.get(url, headers, timeout)
            if 'dspPubDetail' in url:
                reads.append(url)
                if len(reads) == 2:
                    dp._FETCH_DEADLINE = 1.0
            return resp
        with mock.patch.object(dp.requests, 'get', side_effect=get), mock.patch.object(dp.time, 'sleep'), \
                mock.patch.object(dp, 'datetime', FixedDatetime):
            recs = dp.fetch_kansas_city(since_date='2026-11-09')
        self.assertEqual(len(recs), 2)
        self.assertEqual(max(r['inspection_date'] for r in recs), '2026-11-13')
        self.assertEqual(self.status('kansascity')[0], 'skipped')
        holds = self.holds('kansascity')
        self.assertTrue(holds)
        self.assertTrue(all(v == (False, False) for v in holds.values()))
        self.assertGreater(min(k[:10] for k in holds), min(r['inspection_date'] for r in recs))

    def test_mass_failure_reports_error(self):
        s = Server('kansascity')
        for unid in (KC_FOX, KC_MARYS):
            s.det[unid] = _Resp('busy', 500)
        with mock.patch.object(dp, 'IO_MASS_FAILURE_MIN', 2), mock.patch.object(dp, 'IO_BREAKER_TRIP', 9):
            _run(s, since='2026-11-09')
        self.assertEqual(self.status('kansascity')[0], 'error')

    def test_unknown_types_report_error(self):
        s = Server('kansascity')
        s.rows = [re.sub(r'TARGET=_self>[^<]*<', 'TARGET=_self>Regular<', r) for r in s.rows]
        with mock.patch.object(dp, 'IO_UNKNOWN_TYPE_MIN', 2):
            recs = _run(s, since='2026-11-09')
        self.assertEqual(recs, [])
        self.assertEqual(s.docvals_requested(), [])
        self.assertEqual(self.status('kansascity')[0], 'error')

    def test_source_lag_note(self):
        s = Server('missoula')
        s.rows = [r for r in s.rows if Server.row_date(r) <= '2026-10-25']
        _run(s, since='2026-10-01')
        self.assertIn('nothing newer published', dp._GROUP_NOTES.get('Missoula County', ''))


class RegistrationTests(unittest.TestCase):
    def test_jev_policy(self):
        for src in SOURCES.values():
            self.assertEqual(jr.tier_policy(src), 'fixed', src)
            self.assertEqual(jr.score_policy(src, None), 'source_tiers', src)

    def test_labels(self):
        groups = [c['group'] for c in dp.IO_PORTALS.values()]
        self.assertEqual(len(set(groups)), 4)
        self.assertEqual(len(set(SOURCES.values())), 4)
        for src in SOURCES.values():
            self.assertFalse(src.endswith(jr.MHD_SOURCE_SUFFIX), src)
        mhd_names = {c['display_name'] for c in list(dp.DFW_JURISDICTIONS.values())
                     + list(dp.MHD_METRO_JURISDICTIONS.values())}
        self.assertFalse(set(groups) & mhd_names)
        self.assertEqual({c['state'] for c in dp.IO_PORTALS.values()}, {'MT', 'MO'})


if __name__ == '__main__':
    unittest.main()
