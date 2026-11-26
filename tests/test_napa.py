"""Tests for the Napa County, CA fetcher (Tyler healthinspections.us, the
older Garrison template; a snapshot of each facility's latest inspection).
No network: the session's requests are mocked with captures of the live
portal taken 2026-11-26 (tests/fixtures/napa/): the four search pages of
facilities last inspected 2026-10-13..2026-10-22 and the report of every
facility those pages ship, plus a 2024 closure report whose item points do
not add up (an old form that printed item 23 twice) and a re-opening visit
with no score. Scripts, styles and image paths were stripped; owner, person
in charge, certified person, representative and inspector names, e-mail
addresses and phone numbers other than the county office's were replaced.
The pages hold an out-of-county caterer row ZIP 94589 American Canyon rows
(a Vallejo ZIP the county uses), schools under three spellings, a change of
ownership inspection, rows with no grade and three re-opening visits."""
import os
import re
import sys
import unittest
from unittest import mock
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'napa')
SINCE = '2026-10-13'


def _read(name):
    with open(os.path.join(FIXTURES, name), encoding='latin-1') as fh:
        return fh.read()


class _Resp:
    def __init__(self, text, status=200):
        self.text = text
        self.status_code = status
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise dp.requests.HTTPError(str(self.status_code))


def run_fetch(since=SINCE, limit=None, reports=None, pages=None, now='2026-10-22'):
    """fetch_napa against the fixtures. Search pages are served by `start`
    for the fixture window and empty for any other window; `reports` and
    `pages` override a report (by inspection id) or a page (by start) with
    a text, a status code or an exception."""
    reports = dict(reports or {})
    pages = dict(pages or {})
    calls = []

    def fake_request(self, method, url, **kw):
        calls.append(url)
        q = parse_qs(urlparse(url).query)
        if '_report_full.cfm' in url:
            insp = q['inspectionID'][0]
            o = reports.get(insp)
            if isinstance(o, Exception):
                raise o
            if isinstance(o, int):
                return _Resp('', o)
            if o is not None:
                return _Resp(o)
            path = os.path.join(FIXTURES, f'report_{insp}.html')
            if not os.path.exists(path):
                raise AssertionError(f'unexpected report {insp}')
            return _Resp(_read(f'report_{insp}.html'))
        if url.rstrip('/') == dp.NAPA_BASE.rstrip('/'):
            return _Resp('<html></html>')
        if 'search.cfm' in url:
            start = q.get('start', ['1'])[0]
            sd = q.get('sd', [''])[0]
            if sd != '10/13/2026':
                return _Resp('<html>no results</html>')
            o = pages.get(start)
            if isinstance(o, int):
                return _Resp('', o)
            if o is not None:
                return _Resp(o)
            return _Resp(_read(f'search_{start}.html'))
        raise AssertionError(f'unexpected request {url}')

    fixed_now = dp.datetime.strptime(now, '%Y-%m-%d')

    class _DT(dp.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    with mock.patch.object(dp.requests.Session, 'request', fake_request), \
            mock.patch.object(dp, 'datetime', _DT), \
            mock.patch.object(dp, 'NAPA_REREAD_DAYS', 9), \
            mock.patch.object(dp.time, 'sleep'):
        dp._GROUP_NOTES.pop(dp.NAPA_GROUP, None)
        dp._GROUP_STATUS.pop(dp.NAPA_GROUP, None)
        records = dp.fetch_napa(since_date=since, limit=limit)
    return records, calls


def _by_id(records):
    return {r['source_id'][len('napa_'):]: r for r in records}


class TestNapaParsing(unittest.TestCase):
    def test_search_rows(self):
        rows = dp._napa_rows(_read('search_1.html'))
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[0], ('544185', '1347190', '7 ELEVEN 41611H', 'Fixed Food', '490 DEVLIN RD',
                                   'Napa', '94558', '2026-10-15', 'A'))
        # 'SAINT HELENA' and 'ST HELENA' are one town
        self.assertEqual(dp._napa_city('SAINT HELENA'), 'St. Helena')
        self.assertEqual(dp._napa_city('St Helena'), 'St. Helena')
        self.assertEqual(dp._napa_city('CALISTOGA,'), 'Calistoga')
        blank = _read('search_1.html').replace('NAPA, CA 94558', ', CA 94558', 1)
        self.assertEqual(dp._napa_rows(blank)[0][5], 'Napa')     # a blank city line takes the ZIP's town

    def test_row_without_grade_keeps_its_own_fields(self):
        rows = {r[1]: r for r in dp._napa_rows(_read('search_21.html'))}
        self.assertEqual(rows['1347221'][8], '')            # PACIFICO DELI: no grade printed
        self.assertEqual(rows['1347221'][2], 'PACIFICO DELI & BAKERY')
        self.assertEqual(rows['1347230'][2], 'ROMBAUER VINEYARDS NORTH')

    def test_report_points_and_observations(self):
        r = dp._napa_parse_report(_read('report_1347183.html'))
        self.assertEqual(r['date'], '2026-10-15')
        self.assertEqual(r['type'], 'Routine')
        self.assertEqual(r['total'], 16)
        self.assertFalse(r['closed'])
        self.assertEqual(sum(p for _, _, p in r['items']), 16)
        v, problem = dp._napa_violations(r)
        self.assertIsNone(problem)
        self.assertEqual(len(v), len({n for n, _, _ in r['items']}))
        for cat, tier, text in v:
            self.assertIn(tier, ('priority', 'priority_foundation', 'core'))
            self.assertNotIn('Code Section', text)
            self.assertNotIn('Correct By', text)

    def test_tiers_follow_the_points(self):
        r = {'total': 9, 'observations': {6: ['Observed no soap.']},
             'items': [(6, 'Adequate handwashing facilities supplied & accessible', 6),
                       (14, 'Food contact surfaces: clean and sanitized', 2),
                       (33, 'Nonfood-contact surfaces clean', 1)]}
        v, problem = dp._napa_violations(r)
        self.assertIsNone(problem)
        self.assertEqual([t for _, t, _ in v], ['priority', 'priority_foundation', 'core'])
        self.assertEqual(v[0][2], 'Adequate handwashing facilities supplied & accessible: Observed no soap.')
        self.assertEqual(v[2][2], 'Nonfood-contact surfaces clean')

    def test_points_that_do_not_add_up(self):
        r = dp._napa_parse_report(_read('report_1144670.html'))
        self.assertTrue(r['closed'])
        self.assertEqual(r['total'], 31)
        v, problem = dp._napa_violations(r)
        self.assertIsNone(v)
        self.assertIn('add up to 33', problem)

    def test_unscored_reopening(self):
        r = dp._napa_parse_report(_read('report_1347142.html'))
        self.assertIsNone(r['total'])
        self.assertTrue(r['type'].startswith('Re-open after closure'))


class TestFetchNapa(unittest.TestCase):
    def test_window(self):
        records, calls = run_fetch()
        by = _by_id(records)
        # 38 rows: 16 schools and a Santa Rosa-free window; 2 of the 22 left are re-opening visits
        self.assertIn('1347216', by)                     # change of ownership ships
        self.assertIn('1347201', by)                     # American Canyon on ZIP 94589
        self.assertIn('1347225', by)
        for school in ('1347178', '1347253', '1347255', '1347179', '1347233', '1347228'):
            self.assertNotIn(school, by)
        for reopen in ('1347208', '1347221'):
            self.assertNotIn(reopen, by)
        self.assertEqual(len(records), 20)
        r = by['1347183']
        self.assertEqual(r['name'], "Winston's Cafe And Bakery")
        self.assertEqual(r['city'], 'Napa')
        self.assertEqual(r['state'], 'CA')
        self.assertEqual(r['zip'], '94559')
        self.assertEqual(r['address'], '1517 Third St')
        self.assertEqual(r['original_score'], 84)
        self.assertEqual(r['risk_score'], 84)
        self.assertEqual(r['inspection_date'], '2026-10-15')
        self.assertEqual(r['source'], dp.NAPA_SOURCE)
        self.assertEqual(r['source_url'], dp.NAPA_REPORT_URL.format('1347183'))
        self.assertEqual(r['results'], 'B')
        # every shipped record's score is 100 - the printed total
        for rec in records:
            self.assertEqual(rec['risk_score'], rec['original_score'])
            self.assertTrue(0 <= rec['risk_score'] <= 100)
        self.assertEqual(by['1347167']['risk_score'], 100)
        self.assertEqual(by['1347167']['violations'], [])
        # the grade bands line up with the county's letters
        for rec in records:
            if rec['results'] in ('A', 'B', 'C'):
                band = 'A' if rec['risk_score'] >= 90 else 'B' if rec['risk_score'] >= 80 else 'C'
                self.assertEqual(band, rec['results'], rec['source_id'])

    def test_change_of_ownership_ships(self):
        records, _ = run_fetch()
        r = _by_id(records)['1347216']
        self.assertEqual(r['inspection_type'][:19], 'Change of Ownership')
        self.assertEqual(r['risk_score'], 100)

    def test_unreadable_report_is_dropped_not_clean(self):
        records, _ = run_fetch(reports={'1347183': 503})
        self.assertNotIn('1347183', _by_id(records))
        self.assertEqual(len(records), 19)
        self.assertIn('not shipped', dp._GROUP_NOTES.get(dp.NAPA_GROUP, ''))

    def test_mismatched_report_is_dropped(self):
        broken = _read('report_1347183.html').replace('>16<', '>15<', 1)
        r = dp._napa_parse_report(broken)
        if r['total'] == 16:      # the total sits in its own cell; edit that one
            broken = re.sub(r'(Total Points Deducted:.*?<td[^>]*>\s*)16', r'\g<1>15', broken, count=1, flags=re.S)
        records, _ = run_fetch(reports={'1347183': broken})
        self.assertNotIn('1347183', _by_id(records))

    def test_many_unreadable_reports_raise(self):
        bad = {i: 500 for i in ('1347190', '1347174', '1347248', '1347201', '1347237', '1347205',
                                '1347234', '1347164', '1347231', '1347191', '1347197')}
        with self.assertRaises(RuntimeError):
            run_fetch(reports=bad)

    def test_closed_caps_the_score(self):
        # an open facility's report has no Facility Closed line; give this one
        # the box exactly as the 2024 closure report prints it
        closure = re.search(r'<p class="dblBdr">.*?Facility Closed</strong>', _read('report_1144670.html'), re.S)
        self.assertIsNotNone(closure)
        page = _read('report_1347183.html')
        closed = page.replace('<table class="totPtsTbl"', closure.group(0) + '</p><table class="totPtsTbl"', 1)
        self.assertNotEqual(closed, page)
        self.assertFalse(dp._napa_parse_report(page)['closed'])
        records, _ = run_fetch(reports={'1347183': closed})
        r = _by_id(records)['1347183']
        self.assertEqual(r['risk_score'], dp.NAPA_CLOSED_RISK_CAP)
        self.assertEqual(r['original_score'], 84)
        self.assertEqual(r['results'], 'Closed')

    def test_failed_search_page_raises(self):
        with self.assertRaises(RuntimeError):
            run_fetch(pages={'21': 500})

    def test_short_read_raises(self):
        # a page that lists fewer facilities than the pager promises
        with self.assertRaises(RuntimeError):
            run_fetch(pages={'11': '<html>no results</html>'})

    def test_limit(self):
        records, _ = run_fetch(limit=3)
        self.assertEqual(len(records), 3)

    def test_reread_floor(self):
        # a recent since is widened to the re-read floor (patched to 9 days)
        records, calls = run_fetch(since='2026-10-20')
        self.assertTrue(any('sd=10%2F13%2F2026' in c or 'sd=10/13/2026' in c for c in calls))
        self.assertEqual(len(records), 20)

    def test_first_weekly_run_reads_the_snapshot(self):
        # CI passes a record limit, and the 90-day run recorded a latest:
        # neither may stop the one whole-snapshot read
        fresh = {'groups': {dp.NAPA_GROUP: {'latest': '2026-10-22'}}, 'sources': {}}
        with mock.patch.object(dp, '_FRESHNESS', fresh):
            _, calls = run_fetch(since='2026-10-20', limit=50000)
        searched = sorted({parse_qs(urlparse(c).query)['sd'][0] for c in calls if 'search.cfm' in c})
        self.assertIn('01/01/2025', searched)
        self.assertEqual(fresh['sources'][dp.NAPA_SOURCE]['history_from'], dp.NAPA_HISTORY_START)
        with mock.patch.object(dp, '_FRESHNESS', fresh):
            _, calls = run_fetch(since='2026-10-20', limit=50000)
        searched = sorted({parse_qs(urlparse(c).query)['sd'][0] for c in calls if 'search.cfm' in c})
        self.assertEqual(searched, ['10/13/2026'])

    def test_registered(self):
        self.assertIn(dp.NAPA_SOURCE, jev_reader.SOURCE_TIER_SOURCES)
        self.assertEqual(jev_reader.score_policy(dp.NAPA_SOURCE, 90), 'official')
        src = open(dp.__file__).read()
        self.assertIn("(('napa',), NAPA_GROUP, fetch_napa)", src)
        wf = open(os.path.join(os.path.dirname(dp.__file__), '.github', 'workflows',
                               'refresh-data.yml')).read()
        self.assertRegex(wf, r'SUNDAY="[^"]*\bnapa\b')


if __name__ == '__main__':
    unittest.main()
