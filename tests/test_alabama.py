"""Tests for the Alabama statewide fetcher (foodscores.state.al.us, one
WebForms POST per county and restaurant type). No network: requests.Session
is mocked with pages captured live 2026-11-27 (tests/fixtures/alabama:
landing.html, the search form with its 67 counties, the view state cut out;
county.html, nine rows picked verbatim from Madison County's Food Service
Establishment answer, its 'Total Number' set to 9)."""
import os
import re
import sys
import unittest
from datetime import datetime, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'alabama')


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as fh:
        return fh.read()


LANDING = _load('landing.html')
COUNTY = _load('county.html')
EMPTY = re.sub(r'<tr>\s*<td[^>]*>\s*<a id="ctl00_ContentPlaceHolder1_DtList.*?</td>\s*</tr>', '', COUNTY, flags=re.S)
EMPTY = EMPTY.replace('Total Number:  9', 'Total Number:  0')
_ROWS = re.findall(r'<tr>\s*<td[^>]*>\s*<a id="ctl00_ContentPlaceHolder1_DtList.*?</td>\s*</tr>', COUNTY, flags=re.S)
# Madison's institutional answer as the site would print it: the hospital row alone.
INSTITUTIONAL = COUNTY.split(_ROWS[0])[0] + re.sub(r'DtList_ctl\d+_', 'DtList_ctl00_', next(
    r for r in _ROWS if 'Crestwood' in r)) + COUNTY.split(_ROWS[-1])[-1]
INSTITUTIONAL = INSTITUTIONAL.replace('Total Number:  9', 'Total Number:  1')
SESSION_URL = 'https://foodscores.state.al.us/(S(abcdefghijklmnopqrstuvwx))/default.aspx'


class _Resp:
    def __init__(self, text, status=200, url=SESSION_URL):
        self.text = text
        self.status_code = status
        self.url = url


class _FakeSession:
    def __init__(self, answer):
        self.answer = answer
        self.posts = []

    def get(self, url, **kw):
        return _Resp(LANDING)

    def post(self, url, data=None, **kw):
        county = data['ctl00$ContentPlaceHolder1$DrpCnty']
        etype = data['ctl00$ContentPlaceHolder1$DrpEstdType']
        self.posts.append((county, etype))
        return self.answer(county, etype)


RUNTIME_ERROR = _Resp('<html>Runtime Error</html>', status=500)


def _madison_only(county, etype):
    if county == '62':
        return RUNTIME_ERROR                      # Tallapoosa: broken at the source
    if etype == 'All':
        return _Resp(COUNTY if county == '45' else EMPTY)
    if county == '45' and etype == 'Insitutional Food Service':
        return _Resp(INSTITUTIONAL)
    return RUNTIME_ERROR                          # the site's answer to a search with no results


def run_fetch(answer=_madison_only, since=None, limit=None):
    fake = _FakeSession(answer)
    saved = (dict(dp._GROUP_STATUS), dict(dp._GROUP_NOTES))
    dp._GROUP_STATUS.clear()
    dp._GROUP_NOTES.clear()
    try:
        with mock.patch.object(dp.requests, 'Session', return_value=fake), \
                mock.patch.object(dp.time, 'sleep'), \
                mock.patch.object(dp, 'AL_MIN_ROWS', 0):
            records = dp.fetch_alabama(since_date=since, limit=limit)
        status = dict(dp._GROUP_STATUS)
    finally:
        dp._GROUP_STATUS.clear()
        dp._GROUP_STATUS.update(saved[0])
        dp._GROUP_NOTES.clear()
        dp._GROUP_NOTES.update(saved[1])
    return records, fake, status


class AlabamaParseTests(unittest.TestCase):
    def test_parse_counts_rows(self):
        rows = dp._al_parse(COUNTY)
        self.assertEqual(len(rows), 9)
        self.assertEqual(rows[6], {'name': 'Gerry Wayne Smoke House BBQ', 'street': '6000 Pulaski Pike',
                                   'city': 'Huntsville', 'zip': '35810', 'score': '60', 'date': '9/21/2026'})

    def test_count_mismatch_is_unread(self):
        with self.assertRaises(dp._AlUnread):
            dp._al_parse(COUNTY.replace('Total Number:  9', 'Total Number:  10'))

    def test_paging_is_unread(self):
        page = re.sub(r'(id="ctl00_ContentPlaceHolder1_lblPageNumber"[^>]*>)', r'\1Page 1 of 2', COUNTY)
        with self.assertRaises(dp._AlUnread):
            dp._al_parse(page)

    def test_layout_change_is_unread(self):
        with self.assertRaises(dp._AlUnread):
            dp._al_parse('<html><body>Search</body></html>')

    def test_names(self):
        cases = {
            '(DBA) Sushi With Gusto@ 083 Fresh Market': 'Sushi With Gusto@ 083 Fresh Market',
            'Banana Leaf LLC dba Phuket Thai Restaurant at Providence': 'Phuket Thai Restaurant at Providence',
            'Dairy Queen dba Yogiraj Inc': 'Dairy Queen',
            'Taco Bell #029162 DBA Tacala LLC': 'Taco Bell #029162',
            'Steak-Out Restaurants (Right Way Restaurants Inc dba)': 'Steak-Out Restaurants',
            'Boom Vang Brewing Company- Huntsville LLC (dba Back Forty Be': 'Back Forty Be',
            'Tailwind HSV, LLC-dba Comet Coffee': 'Comet Coffee',
            'The Foundry Coffee Roasting Co. (DBA: Foundry Roasters)': 'Foundry Roasters',
            "Hazel Green Burgers INC (dba Culver's)": "Culver's",
            'Heather Adams dba Chick-fil-A Governors DTO': 'Chick-fil-A Governors DTO',
            'Dev 1008 Inc. Harvest Shop Rite (Deli)': 'Dev 1008 Inc. Harvest Shop Rite (Deli)',
            'KROGER #606 DELI': 'Kroger #606 Deli',
            'Dbar Lounge': 'Dbar Lounge',
        }
        for raw, want in cases.items():
            self.assertEqual(dp._al_name(raw), want, raw)


class AlabamaFetchTests(unittest.TestCase):
    def test_sweep(self):
        records, fake, status = run_fetch()
        posted_counties = {c for c, _ in fake.posts}
        self.assertNotIn('37', posted_counties)          # Jefferson: its own department
        self.assertNotIn('49', posted_counties)          # Mobile
        self.assertEqual(len(posted_counties), 65)
        self.assertEqual({t for c, t in fake.posts if c == '1'}, {'All', *dp.AL_EXCLUDED_TYPES})
        self.assertEqual([t for c, t in fake.posts if c == '62'], ['All'] * dp.AL_RETRIES)
        self.assertNotIn(dp.AL_GROUP, status)            # Tallapoosa's 500 is a note, not a failure
        names = [r['name'] for r in records]
        self.assertNotIn('Crestwood Medical Center-Cafeteria', names)     # an institutional type
        self.assertIn('Phuket Thai Restaurant at Providence', names)
        self.assertEqual(len(records), 8)
        r = next(r for r in records if r['name'] == 'Gerry Wayne Smoke House BBQ')
        self.assertEqual((r['state'], r['zip'], r['risk_score'], r['original_score'], r['inspection_date']),
                         ('AL', '35810', 60, 60, '2026-09-21'))
        rid = dp.make_restaurant_id(r['name'], r['address'], r['city'])
        self.assertEqual(r['source_id'], f'al_45_{rid}_20260921')
        self.assertEqual(r['violations'], [])
        self.assertEqual(records, sorted(records, key=lambda x: (x['inspection_date'], x['risk_score'],
                                                                  x['source_id'])))

    def test_weekly_window_rereads_margin(self):
        since = (datetime(2026, 10, 30)).strftime('%Y-%m-%d')        # margin reaches 2026-08-31
        records, _, _ = run_fetch(since=since)
        self.assertEqual(sorted(r['inspection_date'] for r in records),
                         ['2026-09-21', '2026-09-25', '2026-10-16', '2026-10-26'])

    def test_failed_county_goes_red_and_ships_the_rest(self):
        def answer(county, etype):
            if county == '1':
                return _Resp('oops', status=503)
            return _madison_only(county, etype)
        records, fake, status = run_fetch(answer)
        self.assertEqual(status[dp.AL_GROUP][0], 'error')
        self.assertIn('Autauga', status[dp.AL_GROUP][1])
        self.assertEqual(len(records), 8)

    def test_excluded_row_missing_from_all_is_unread(self):
        def answer(county, etype):
            if county == '45' and etype == 'All':
                return _Resp(EMPTY)
            return _madison_only(county, etype)
        records, _, status = run_fetch(answer)
        self.assertEqual(status[dp.AL_GROUP][0], 'error')
        self.assertIn('Madison', status[dp.AL_GROUP][1])

    def test_excluded_type_unreadable_page_drops_the_county(self):
        def answer(county, etype):
            if county == '45' and etype == 'Camp':
                return _Resp('<html>maintenance</html>')
            return _madison_only(county, etype)
        records, _, status = run_fetch(answer)
        self.assertEqual(records, [])
        self.assertEqual(status[dp.AL_GROUP][0], 'error')

    def test_tiny_statewide_read_raises(self):
        fake = _FakeSession(lambda c, t: _Resp(EMPTY))
        with mock.patch.object(dp.requests, 'Session', return_value=fake), \
                mock.patch.object(dp.time, 'sleep'):
            with self.assertRaises(RuntimeError):
                dp.fetch_alabama()

    def test_changed_form_raises(self):
        fake = _FakeSession(_madison_only)
        changed = re.sub(r'<option[^>]*>WINSTON</option>', '', LANDING)
        self.assertNotEqual(changed, LANDING)
        fake.get = lambda url, **kw: _Resp(changed)
        with mock.patch.object(dp.requests, 'Session', return_value=fake), \
                mock.patch.object(dp.time, 'sleep'):
            with self.assertRaises(RuntimeError):
                dp.fetch_alabama()


if __name__ == '__main__':
    unittest.main()
