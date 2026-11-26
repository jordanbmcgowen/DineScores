"""Tests for the Sonoma County, CA fetcher (OnBase Public Access: a keyword
search over stored documents, then one inspection report PDF per document).
No network: the search is mocked and each report is a fixture of the text
rows `_sonoma_pdf_lines` read from a live PDF on 2026-11-26
(tests/fixtures/sonoma/). The PDFs themselves are not kept (300-900 KB, with
signatures); permit holders, certified food managers, inspectors, people
the report was sent to, phone numbers and e-mail addresses were replaced
with REDACTED, and the redacted rows were checked to parse exactly like the
originals. The set holds a five-page report with sub-items and a page break
inside an item (conditional_multipage), a re-inspection, clean reports with
and without a comment, a report with three majors, the cottage food, mobile
unit and caterer templates, an attempted visit to a closed business and a
scan with no text layer."""
import json
import os
import sys
import unittest
from unittest import mock
from urllib.parse import unquote

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'sonoma')


def lines(name):
    with open(os.path.join(FIXTURES, name + '.json')) as fh:
        return json.load(fh)


# (fixture, listed name, listed address, stored date, display type)
DOCS = [
    ('conditional_multipage', 'GOOSE & FERN THE', '116 5TH ST', '10/30/2026', 'OleActivePage'),
    ('reinspection', 'DELI BY SAZON', '1 MAIN ST', '11/13/2026', 'OleActivePage'),
    ('clean', 'MATHESON BOOTLEGGERS LLC', '106 MATHESON ST', '11/14/2026', 'OleActivePage'),
    ('many_majors', 'CRYSTALS CORNER', '2 MAIN ST', '11/17/2026', 'OleActivePage'),
    ('two_majors', 'GLEN ELLEN GROCERY', '3 MAIN ST', '11/21/2026', 'OleActivePage'),
    ('routine', 'GRAND CRU CUSTOM CRUSH', '4 MAIN ST', '11/07/2026', 'OleActivePage'),
    ('blank_clean', 'PETALUMA CARE', '1115 B ST', '11/10/2026', 'OleActivePage'),
    ('cottage_food', 'MAD CHOCOLATES', '5 MAIN ST', '11/05/2026', 'OleActivePage'),
    ('mobile', 'GALIMEN ICE CREAM MFF', '463 AVIATION BLVD', '11/21/2026', 'OleActivePage'),
    ('caterer', 'PIZZA SANTO', '1290 DRY CREEK RD', '11/13/2026', 'OleActivePage'),
    ('attempted', 'LA MICHOACANA', '127 HEALDSBURG AVE', '11/05/2026', 'OleActivePage'),
    ('scan_no_text', 'OLD SCAN', '6 MAIN ST', '11/06/2026', 'OleActivePage'),
    ('image', 'IMAGE ONLY', '7 MAIN ST', '11/06/2026', 'Image'),
]


def _search_rows(extra=()):
    rows = []
    for fx, name, addr, stored, dtype in list(DOCS) + list(extra):
        rows.append({'ID': f'DOC-{fx}', 'DisplayType': dtype, 'DisplayColumnValues': [
            {'Value': name}, {'Value': addr}, {'Value': 'Inspections - FOOD'},
            {'Value': stored}, {'Value': stored}]})
    # another document type in the same query is never read
    rows.append({'ID': 'DOC-financial', 'DisplayType': 'OleActivePage', 'DisplayColumnValues': [
        {'Value': 'GOOSE & FERN THE'}, {'Value': '116 5TH ST'}, {'Value': 'Financial'},
        {'Value': '11/01/2026'}, {'Value': '11/01/2026'}]})
    return rows


class _Resp:
    def __init__(self, content=b'', status=200, payload=None):
        self.content = content
        self.status_code = status
        self._payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise dp.requests.HTTPError(str(self.status_code))

    def json(self):
        return self._payload


def run_fetch(since='2026-10-25', limit=None, now='2026-11-26', freshness=None, overrides=None,
              extra_docs=()):
    """fetch_sonoma against the fixtures. `overrides` maps a fixture name to
    replacement rows, an HTTP status or an exception."""
    overrides = dict(overrides or {})
    rows = _search_rows(extra_docs)
    searches, fetched = [], []

    def fake_post(url, json=None, **kw):
        searches.append(json)
        lo = dp.datetime.strptime(json['FromDate'], '%m/%d/%Y')
        hi = dp.datetime.strptime(json['ToDate'], '%m/%d/%Y')
        keep = [r for r in rows
                if lo <= dp.datetime.strptime(r['DisplayColumnValues'][4]['Value'], '%m/%d/%Y') <= hi]
        return _Resp(payload={'Data': keep})

    def fake_get(url, **kw):
        doc = unquote(url.rstrip('/').rsplit('/', 1)[1])
        fetched.append(doc)
        o = overrides.get(doc[4:])
        if isinstance(o, Exception):
            raise o
        if isinstance(o, int):
            return _Resp(status=o)
        return _Resp(content=doc.encode())

    def fake_lines(data):
        fx = data.decode()[4:]
        o = overrides.get(fx)
        if isinstance(o, list):
            return o
        return lines(fx) if os.path.exists(os.path.join(FIXTURES, fx + '.json')) else []

    fixed_now = dp.datetime.strptime(now, '%Y-%m-%d')

    class _DT(dp.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    fresh = freshness if freshness is not None else {'groups': {dp.SONOMA_GROUP: {'latest': '2026-10-24'}},
                                                     'sources': {}}
    with mock.patch.object(dp.requests, 'post', fake_post), \
            mock.patch.object(dp.requests, 'get', fake_get), \
            mock.patch.object(dp, '_sonoma_pdf_lines', fake_lines), \
            mock.patch.object(dp, 'datetime', _DT), \
            mock.patch.object(dp, '_FRESHNESS', fresh), \
            mock.patch.object(dp.time, 'sleep'):
        dp._GROUP_NOTES.pop(dp.SONOMA_GROUP, None)
        dp._GROUP_STATUS.pop(dp.SONOMA_GROUP, None)
        dp._FRESHNESS_HOLDS.clear()
        records = dp.fetch_sonoma(since_date=since, limit=limit)
        holds = dict(dp._FRESHNESS_HOLDS)
        dp._FRESHNESS_HOLDS.clear()
    return records, searches, fetched, holds


def by_name(records):
    return {r['name']: r for r in records}


class TestParse(unittest.TestCase):
    def test_multipage_report(self):
        rep = dp._sonoma_parse_lines(lines('conditional_multipage'))
        self.assertEqual(rep['unread'], '')
        self.assertEqual(rep['head']['record'], 'PR0030387')
        self.assertEqual(rep['head']['date'], '10/28/2026')
        self.assertEqual(rep['head']['pe'], 'PE - 3R21')
        nums = [n for n, _, _, _ in rep['items']]
        self.assertEqual(nums, [1, 6, 13, 14, 27, 29, 33, 36, 41, 42, 44, 44, 44, 45])
        self.assertEqual([s for _, _, s, _ in rep['items']].count('MAJ'), 1)
        # the item cut by a page break keeps the comment from both pages,
        # and the numbered sentences inside a comment are not items
        e44 = [c for n, t, _, c in rep['items'] if t.startswith('Facility is equipped')][0]
        self.assertIn('Back door leading directly', e44)
        self.assertIn('gap under the back kitchen door', e44)
        self.assertNotIn('Violation Code', e44)
        # an empty General Comments section stays empty (it used to swallow
        # every item's text, so "doors remain closed" read as a closure)
        self.assertEqual(rep['comments'], '')

    def test_clean_reports(self):
        rep = dp._sonoma_parse_lines(lines('clean'))
        self.assertTrue(rep['clean'])
        self.assertIn('No violations were observed', rep['comments'])
        self.assertTrue(dp._sonoma_parse_lines(lines('blank_clean'))['clean'])

    def test_unreadable(self):
        self.assertEqual(dp._sonoma_parse_lines([])['unread'], 'no text in the PDF')
        rows = lines('routine')
        # an item that lost its Status line
        k = next(i for i, x in enumerate(rows) if x.startswith('Status:'))
        self.assertTrue(dp._sonoma_parse_lines(rows[:k] + ['Status:'] + rows[k + 1:])['unread'])
        # every Status line changed shape: never a clean report
        gone = [x for x in rows if not x.startswith('Status:')]
        self.assertIn('cited items', dp._sonoma_parse_lines(gone)['unread'])
        # a Status line the reader cannot tie to an item
        self.assertIn('Status lines', dp._sonoma_parse_lines(rows + ['Status: MAJ'])['unread'])
        # no Observations section and no "No violations observed"
        self.assertTrue(dp._sonoma_parse_lines(rows[:8])['unread'])

    def test_tiers(self):
        v = dp._sonoma_violations([(6, 'Hands', 'MAJ', ''), (7, 'Temps', 'MIN', 'x'),
                                   (23, 'Signs', 'MIN', ''), (24, 'Thermometers', 'MIN', '')])
        self.assertEqual([t for _, t, _ in v], ['priority', 'priority_foundation',
                                                 'priority_foundation', 'core'])
        self.assertEqual(v[1][2], 'Temps: x')

    def test_closure_and_attempted_wording(self):
        # comments as the county's inspectors wrote them (2026-11)
        closed = ["This facility's permit is suspended due to rodent activity in the facility.",
                  "Facility's permit suspended due to rodent activity within the facility.",
                  'Permit is suspended for cockroach activity within the facility.',
                  "The facility's permit is temporarily suspended following activation of the Ansul"]
        for c in closed:
            self.assertTrue(dp.SONOMA_CLOSED_RE.search(c), c)
        not_closed = ['Facility was closed to the public at the time of the inspection.',
                      'Ensure doors remain closed or provide a screen on the doors.',
                      'Re-inspection conducted following the closure due to rodents.']
        for c in not_closed:
            self.assertFalse(dp.SONOMA_CLOSED_RE.search(c), c)
        attempted = ['A routine inspection was attempted; however, the facility was closed.',
                     'Unable to inspect facility, doors locked, no one answered on phone.',
                     'Sign on the door reporting operator closed until January. No inspection completed.',
                     'Facility found operating without valid permit during inspection. This facility '
                     'shall remain closed until permit fees have been paid. A full inspection of the '
                     'facility was not completed during this visit.']
        for c in attempted:
            self.assertTrue(dp.SONOMA_ATTEMPTED_RE.search(c), c)
        self.assertTrue(dp.SONOMA_SHUT_RE.search('observed that the facility was closed. A sign'))

    def test_names(self):
        self.assertEqual(dp._sonoma_name('Goose &  Fern, The'), 'The Goose & Fern')
        self.assertEqual(dp._sonoma_name('Matheson Bootleggers L L C'), 'Matheson Bootleggers LLC')
        self.assertEqual(dp._sonoma_name('ROHNERT PARK GOLF LP DBA FOXTAIL GOLF CLUB'),
                         'Foxtail Golf Club')


class TestFetch(unittest.TestCase):
    def test_ships_retail_food_only(self):
        records, searches, fetched, holds = run_fetch()
        names = by_name(records)
        self.assertEqual(sorted(names), sorted([
            'The Goose & Fern', 'Deli By Sazon', 'Matheson Bootleggers LLC', 'Crystals Corner',
            'Glen Ellen Grocery', 'Grand Cru Custom Crush', 'Petaluma Care & Rehabilitation']))
        self.assertNotIn('DOC-image', fetched)          # a scan is never downloaded
        self.assertNotIn('DOC-financial', fetched)
        self.assertEqual(holds, {})
        # one search per 15-day stored window from 21 days before the start
        self.assertEqual(searches[0]['FromDate'], '10/04/2026')
        self.assertEqual(searches[-1]['ToDate'], '11/26/2026')

    def test_record_fields_and_scores(self):
        r = by_name(run_fetch()[0])
        g = r['The Goose & Fern']
        self.assertEqual((g['city'], g['state'], g['zip']), ('Santa Rosa', 'CA', '95401'))
        self.assertEqual(g['address'], '116 5th St')
        self.assertEqual(g['inspection_date'], '2026-10-28')
        self.assertEqual(g['source_id'], 'sonoma_PR0030387_20261028')
        self.assertIsNone(g['original_score'])
        self.assertEqual(g['total_violations'], 14)
        self.assertEqual(g['results'], 'Pass')               # one major is not a yellow placard
        self.assertEqual(g['risk_score'], dp.calc_risk_score(g['violations'])[0])
        self.assertEqual(r['Matheson Bootleggers LLC']['risk_score'], 100)
        self.assertEqual(r['Deli By Sazon']['inspection_type'], 'Re-inspection')
        self.assertEqual(r['Grand Cru Custom Crush']['inspection_type'], 'Routine')

    def test_two_or_more_majors_cap(self):
        r = by_name(run_fetch()[0])
        for name in ('Crystals Corner', 'Glen Ellen Grocery'):
            self.assertEqual(r[name]['results'], 'Conditional pass')
            self.assertLessEqual(r[name]['risk_score'], dp.SONOMA_YELLOW_RISK_CAP)

    def test_closure_cap(self):
        rows = lines('routine')
        k = rows.index('General Comments:')
        rows = rows[:k + 1] + ['The facility was closed due to an imminent health hazard.'] + rows[k + 1:]
        r = by_name(run_fetch(overrides={'routine': rows})[0])['Grand Cru Custom Crush']
        self.assertEqual(r['results'], 'Closed')
        self.assertLessEqual(r['risk_score'], dp.SONOMA_CLOSED_RISK_CAP)

    def test_unread_report_is_held(self):
        records, _, _, holds = run_fetch(overrides={'routine': 503})
        self.assertNotIn('Grand Cru Custom Crush', by_name(records))
        self.assertIn(dp.SONOMA_GROUP, holds)
        rows = lines('routine')
        rows = [x for x in rows if not x.startswith('Status:')]
        records, _, _, holds = run_fetch(overrides={'routine': rows})
        self.assertNotIn('Grand Cru Custom Crush', by_name(records))
        self.assertIn(dp.SONOMA_GROUP, holds)
        self.assertIn('not read', dp._GROUP_NOTES.get(dp.SONOMA_GROUP, ''))

    def test_mass_unread_raises(self):
        extra = [(f'bad{i}', f'BAD {i}', '1 X ST', '11/10/2026', 'OleActivePage') for i in range(25)]
        bad = lines('routine')
        bad = [x for x in bad if not x.startswith('Status:')]
        with self.assertRaises(RuntimeError):
            run_fetch(extra_docs=extra, overrides={f'bad{i}': bad for i in range(25)})

    def test_window_before_start_is_skipped(self):
        names = by_name(run_fetch(since='2026-11-12')[0])
        # stored from 10/22: the 10/28 report is read, it is before the
        # margin floor only when the window starts later
        self.assertNotIn('The Goose & Fern', by_name(run_fetch(since='2026-11-20')[0]))
        self.assertIn('Glen Ellen Grocery', names)

    def test_listed_before_the_window_is_not_downloaded(self):
        _, _, fetched, _ = run_fetch(since='2026-11-20')
        self.assertNotIn('DOC-conditional_multipage', fetched)     # listed 10/30
        self.assertIn('DOC-two_majors', fetched)

    def test_deadline_holds_and_skips(self):
        with mock.patch.object(dp, '_past_deadline', side_effect=[False] + [True] * 10):
            records, _, fetched, holds = run_fetch()
        self.assertEqual(records, [])
        self.assertEqual(fetched, [])
        self.assertIn(dp.SONOMA_GROUP, holds)
        self.assertEqual(dp._GROUP_STATUS.get(dp.SONOMA_GROUP, ('',))[0], 'skipped')

    def test_first_run_reads_history_even_with_a_limit(self):
        _, searches, _, _ = run_fetch(freshness={'groups': {}, 'sources': {}}, limit=50000)
        start = dp.datetime(2026, 11, 26) - dp.timedelta(days=dp.SONOMA_HISTORY_DAYS
                                                         + dp.SONOMA_STORED_MARGIN_DAYS)
        self.assertEqual(searches[0]['FromDate'], start.strftime('%m/%d/%Y'))

    def test_registered(self):
        self.assertIn(dp.SONOMA_SOURCE, jev_reader.SOURCE_TIER_SOURCES)
        self.assertIn(dp.SONOMA_SOURCE, jev_reader.SOURCE_TIER_SCORE_SOURCES)
        with open(dp.__file__) as fh:
            src = fh.read()
        self.assertIn("(('sonoma',), SONOMA_GROUP, fetch_sonoma)", src)
        with open(os.path.join(os.path.dirname(dp.__file__), '.github', 'workflows',
                               'refresh-data.yml')) as fh:
            wf = fh.read()
        self.assertRegex(wf, r'SUNDAY="[^"]*\bsonoma\b')


if __name__ == '__main__':
    unittest.main()
