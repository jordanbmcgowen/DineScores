"""Tests for the Corpus Christi / Nueces County, TX fetcher (InspectHub
public listing of the Corpus Christi-Nueces County Public Health District).
No network: the fetcher's requests.Session is replaced by a fake serving
captures from the live portal taken 2026-11-22 (tests/fixtures/
corpuschristi/): the listing trimmed to 21 of its 2,203 rows (food rows the
inspection pages below belong to, plus pool, splash-pad, flea-market,
mobile, food-truck, school and temporary-event rows the fetcher must skip)
and 17 inspection pages verbatim (no person is named on them; the inspector
appears only on the printable PDF, which is never fetched):
  119406 Taqueria La Tapatia #3, 86, rodent droppings (history: 118244)
  119237 / 119241 Casa Vieja Jalisco, two listing rows, both 2026-11-05
  119277 / 119246 Wine Dreams, two clean visits ('Score : 0', empty table)
  119211 Hunan Express #3, 89 (3+3+2+2+1 points)
  118295 Ol' Steakhouse, sewage item and a roach note
  119102 Hunan Express #2, nine items cited with no note (pest item included)
  118159 Snoopy's Pier, 'Gnats present'
  118506 Stripes, rodent droppings under two items
  102379 Whiskey Rio, the pre-2024 form ('34. No Evidence of Insect ...')
  118928 / 118174 Burger King, a clean latest and an earlier 97
  119426 an id the portal has not published (200, no name, no score)
  116310 a pool page (name, the 'no violations' sentence, no score)
  119443 Glow & Go Nutrition, a food visit in that same shape (captured
         2026-11-23), which the listing scores 100."""
import inspect
import logging
import os
import re
import sys
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader as jr  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'corpuschristi')


def _fixture(name):
    with open(os.path.join(FIXTURES, name), encoding='utf-8') as fh:
        return fh.read()


def _page(insp_id):
    return _fixture(f'detail_{insp_id}.html')


class _Resp:
    def __init__(self, text='', status=200):
        self.text = text
        self.status_code = status


class _Session:
    """Serves the fixtures (an inspection page that was not captured answers
    like an unpublished id: dropped and held). `answers` maps 'list' or an inspection id to a
    _Resp, an exception, or a list of those served in order (the last one
    repeats)."""

    def __init__(self, answers=None, listing=None):
        self.answers = {k: (list(v) if isinstance(v, list) else [v]) for k, v in (answers or {}).items()}
        self.listing = listing
        self.calls = []

    def get(self, url, timeout=None):
        m = re.search(r'&i=(\d+)$', url)
        key = int(m.group(1)) if m else 'list'
        self.calls.append(key)
        if key in self.answers:
            seq = self.answers[key]
            a = seq.pop(0) if len(seq) > 1 else seq[0]
            if isinstance(a, Exception):
                raise a
            return a
        if key == 'list':
            return _Resp(self.listing if self.listing is not None else _fixture('list.html'))
        if not os.path.exists(os.path.join(FIXTURES, f'detail_{key}.html')):
            return _Resp(_page(119426))          # not captured: answers like an unpublished id
        return _Resp(_page(key))


def run_fetch(since=None, limit=None, answers=None, listing=None):
    sess = _Session(answers, listing)
    with mock.patch.object(dp, '_cc_session', return_value=sess), \
            mock.patch.object(dp.time, 'sleep'), \
            mock.patch.object(dp, 'CC_LIST_MIN_ROWS', 10):
        out = dp.fetch_corpus_christi(since_date=since, limit=limit)
    return out, sess.calls


def _by_id(records):
    return {r['source_id']: r for r in records}


def _hold_keys(source=None):
    return {k.split('|', 1)[1]: v[0] for k, v in dp._FRESHNESS_HOLDS.get(source or dp.CC_SOURCE, {}).items()}


def _listing_with(insp_id, **cells):
    """The listing fixture with one row's score / form / date cell replaced."""
    html = _fixture('list.html')
    m = re.search(r'(<tr onClick="[^"]*&i=%d\'" class="link">)<td>(.*?)</td><td>(.*?)</td><td>(.*?)</td>'
                  r'<td>(.*?)</td></tr>' % insp_id, html)
    loc, score, form, date = m.group(2), m.group(3), m.group(4), m.group(5)
    row = (f"{m.group(1)}<td>{cells.get('location', loc)}</td><td>{cells.get('score', score)}</td>"
           f"<td>{cells.get('form', form)}</td><td>{cells.get('date', date)}</td></tr>")
    return html.replace(m.group(0), row)


class CorpusChristiBase(unittest.TestCase):
    def setUp(self):
        self._saved = (dict(dp._GROUP_NOTES), dict(dp._FRESHNESS_HOLDS), dict(dp._GROUP_STATUS),
                       dp._FETCH_DEADLINE, dp.log.level)
        dp._GROUP_NOTES.clear()
        dp._FRESHNESS_HOLDS.clear()
        dp._GROUP_STATUS.clear()
        dp._FETCH_DEADLINE = None
        dp.log.setLevel(logging.ERROR)

    def tearDown(self):
        notes, holds, status, deadline, level = self._saved
        dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(notes)
        dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(holds)
        dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(status)
        dp._FETCH_DEADLINE = deadline
        dp.log.setLevel(level)


class ParserTests(CorpusChristiBase):
    def test_listing(self):
        with mock.patch.object(dp, 'CC_LIST_MIN_ROWS', 10):
            rows = dp._cc_parse_list(_fixture('list.html'))
        self.assertEqual(len(rows), 21)
        hunan = next(r for r in rows if r['id'] == 119211)
        self.assertEqual(hunan, {'id': 119211, 'location': 'HUNAN EXPRESS #3 - 5614 MCARDLE ST.', 'score': 89,
                                 'form': '2024 Inspections-Restaurants', 'date': '2026-11-03'})
        self.assertEqual({r['form'] for r in rows},
                         {'2024 Inspections-Restaurants', 'Restaurant', 'Flea Market Food Vendors',
                          'Splash Pools 2026', 'Public Swimming Pool & Spa 2026'})

    def test_listing_in_another_shape_raises(self):
        # a wrong tenant key answers 200 with this body
        for body in ("{ 'ERROR':'Invalid Key' }", '', '<html><body>Maintenance</body></html>'):
            with self.assertRaises(ValueError):
                dp._cc_parse_list(body)
        with self.assertRaisesRegex(ValueError, 'only 21 rows'):
            dp._cc_parse_list(_fixture('list.html'))            # the real minimum is 1,000
        bad = _fixture('list.html').replace('<td>November 03, 2026</td>', '<td>2026-11-03</td><td>x</td>')
        with mock.patch.object(dp, 'CC_LIST_MIN_ROWS', 10), self.assertRaisesRegex(ValueError, 'unknown shape'):
            dp._cc_parse_list(bad)

    def test_listing_mostly_undated_raises(self):
        html = re.sub(r'<td>[A-Z][a-z]+ \d\d, \d{4}</td></tr>', '<td>soon</td></tr>', _fixture('list.html'))
        with mock.patch.object(dp, 'CC_LIST_MIN_ROWS', 10), self.assertRaisesRegex(ValueError, 'no readable date'):
            dp._cc_parse_list(html)

    def test_inspection_page(self):
        page = dp._cc_parse_detail(_page(119211))
        self.assertEqual((page['id'], page['name'], page['street'], page['date'], page['score']),
                         (119211, 'HUNAN EXPRESS #3', '5614 MCARDLE ST.', '2026-11-03', 89))
        self.assertEqual([(r[0], r[1]) for r in page['rows']], [(3, '15'), (3, '28'), (2, '10'), (2, '23'), (1, '44')])
        self.assertEqual(page['rows'][0][2], 'Food separated and protected')
        self.assertEqual(page['rows'][4][3], 'Can opener needs to be cleaned.')
        self.assertEqual(page['rows'][4][4], 'No')
        self.assertFalse(page['empty_note'])
        self.assertTrue(page['history'])
        self.assertTrue(all(isinstance(h[0], int) for h in page['history']))

    def test_name_with_a_dash_keeps_it(self):
        page = dp._cc_parse_detail(_page(119277))
        self.assertEqual(page['name'], "WINE DREAMS, LLC - DBA SIREN'S CALL WINERY")
        self.assertEqual(page['street'], '14225 S. PADRE ISLAND DR #6')

    def test_history_rows(self):
        page = dp._cc_parse_detail(_page(119406))
        self.assertIn((118244, '2026-09-01', 90), page['history'])
        clean = dp._cc_parse_detail(_page(119277))
        self.assertIn((119246, '2026-11-05', 100), clean['history'])     # a clean visit is 100 here

    def test_clean_page_prints_zero_over_an_empty_table(self):
        page = dp._cc_parse_detail(_page(119277))
        self.assertEqual((page['score'], page['rows'], page['empty_note']), (0, [], False))

    def test_unpublished_id_and_pool_page_carry_no_score(self):
        unpub = dp._cc_parse_detail(_page(119426))
        self.assertEqual((unpub['name'], unpub['score'], unpub['rows'], unpub['empty_note']), ('', None, [], True))
        pool = dp._cc_parse_detail(_page(116310))
        self.assertTrue(pool['name'])
        self.assertIsNone(pool['score'])

    def test_not_an_inspection_page(self):
        self.assertIsNone(dp._cc_parse_detail('Invalid properties detected, cannot load dataset.'))
        self.assertIsNone(dp._cc_parse_detail(''))

    def test_violation_row_in_another_shape_raises(self):
        html = _page(119211).replace('<td>3</td>', '<td>3</td><td>extra</td>', 1)
        self.assertNotEqual(html, _page(119211))
        with self.assertRaises(ValueError):
            dp._cc_parse_detail(html)

    def test_dates(self):
        self.assertEqual(dp._cc_date('November 03, 2026'), '2026-11-03')
        self.assertEqual(dp._cc_date('November, 03 2026'), '2026-11-03')
        self.assertIsNone(dp._cc_date('April 01, 715'))
        self.assertIsNone(dp._cc_date('Smarch 01, 2026'))
        self.assertIsNone(dp._cc_date(''))


class CheckTests(CorpusChristiBase):
    def listed(self, insp_id, **over):
        with mock.patch.object(dp, 'CC_LIST_MIN_ROWS', 10):
            row = next(r for r in dp._cc_parse_list(_fixture('list.html')) if r['id'] == insp_id)
        row.update(over)
        return row

    def test_scored_page_that_adds_up_is_read(self):
        self.assertEqual(dp._cc_check(dp._cc_parse_detail(_page(119211)), 119211, self.listed(119211)), '')

    def test_clean_page_is_read_only_when_the_listing_says_100(self):
        page = dp._cc_parse_detail(_page(119277))
        self.assertEqual(dp._cc_check(page, 119277, self.listed(119277)), '')
        self.assertTrue(dp._cc_check(page, 119277, self.listed(119277, score=95)).startswith('score'))
        self.assertTrue(dp._cc_check(page, 119277, self.listed(119277, score=None)))

    def test_unpublished_id_is_never_read(self):
        page = dp._cc_parse_detail(_page(119426))
        why = dp._cc_check(page, 119426, {'location': 'X - 1 MAIN', 'date': '2026-11-20', 'score': 100})
        self.assertIn('not published', why)

    def test_points_that_do_not_add_up(self):
        page = dp._cc_parse_detail(_page(119211))
        self.assertIn('89 on the page, 90 on the listing',
                      dp._cc_check(page, 119211, self.listed(119211, score=90)))
        page['score'] = 90
        self.assertIn('89 from the rows', dp._cc_check(page, 119211, self.listed(119211, score=90)))
        page = dp._cc_parse_detail(_page(119211))
        page['rows'][0] = (4,) + page['rows'][0][1:]
        self.assertIn('without 3, 2 or 1 points', dp._cc_check(page, 119211, self.listed(119211)))

    def test_unscored_food_visit(self):
        page = dp._cc_parse_detail(_page(119443))
        self.assertEqual((page['name'], page['date'], page['score'], page['rows'], page['empty_note']),
                         ('GLOW & GO NUTRITION', '2026-11-20', None, [], True))
        self.assertEqual(dp._cc_check(page, 119443, self.listed(119443)), dp.CC_UNSCORED)
        # a page with cited rows and no Score line is a broken page, not an unscored visit
        broken = dp._cc_parse_detail(_page(119211))
        broken['score'] = None
        self.assertEqual(dp._cc_check(broken, 119211, self.listed(119211)), 'no score on the page')

    def test_another_location_date_or_id(self):
        page = dp._cc_parse_detail(_page(119211))
        self.assertIn('another location', dp._cc_check(page, 119211, self.listed(119102)))
        self.assertIn('dated', dp._cc_check(page, 119211, self.listed(119211, date='2026-11-04')))
        self.assertIn('inspection 119211', dp._cc_check(page, 119212, self.listed(119211)))


class WindowTests(CorpusChristiBase):
    def test_window_reads_latest_then_earlier_in_window(self):
        out, calls = run_fetch(since='2026-11-01T00:00:00')
        # the listing, each food location whose latest is on or after
        # 2026-10-25 (CC_REREAD_DAYS before), oldest first, then the earlier
        # in-window visits their history tables name (119241 is a listing
        # row of its own and is read once)
        self.assertEqual(calls, ['list', 119102, 119211, 119237, 119241, 119277, 119406, 119443, 119246])
        self.assertEqual([r['source_id'] for r in out],
                         ['cctx_119406', 'cctx_119277', 'cctx_119237', 'cctx_119241', 'cctx_119246',
                          'cctx_119211', 'cctx_119102'])
        self.assertEqual(dp._FRESHNESS_HOLDS, {})
        self.assertEqual(dp._GROUP_STATUS, {})

    def test_history_reaches_an_earlier_visit(self):
        out, calls = run_fetch(since='2026-09-01')
        self.assertIn(118244, calls)          # Tapatia #3's 2026-09-01 visit, only in 119406's history
        self.assertIn(118174, calls)          # Burger King's 2026-08-25 visit (window read from 08-25)
        recs = _by_id(out)
        self.assertEqual(recs['cctx_118244']['risk_score'], 90)
        self.assertEqual(recs['cctx_118244']['name'], 'Taqueria La Tapatia #3')
        self.assertEqual(len(calls), len(set(calls)))      # every page once

    def test_nothing_older_than_the_window(self):
        out, calls = run_fetch(since='2026-11-28')        # read from 11-21; the newest is 11-20
        self.assertEqual(calls, ['list'])
        self.assertEqual(out, [])
        self.assertEqual(dp._GROUP_STATUS, {})

    def test_test_mode_takes_the_newest_locations(self):
        out, calls = run_fetch(limit=2)
        self.assertEqual(calls[:3], ['list', 119406, 119443])
        self.assertLessEqual(len(out), 2)
        self.assertEqual(out[0]['source_id'], 'cctx_119406')

    def test_oldest_location_first_so_a_cut_drops_the_newest(self):
        _, calls = run_fetch(since='2026-10-20')
        latest = [c for c in calls[1:] if c in (119102, 119211, 119237, 119241, 119277, 119406, 119443)]
        self.assertEqual(latest, [119102, 119211, 119237, 119241, 119277, 119406, 119443])


PAGE_IDS = (119406, 118244, 119237, 119241, 119277, 119246, 119211, 118295, 119102, 118159, 118506, 102379,
            118928, 118174)


def _records_from_pages():
    """Every captured inspection page as the record the fetcher builds."""
    return {f'cctx_{i}': dp._cc_record(i, dp._cc_parse_detail(_page(i))) for i in PAGE_IDS}


class RecordTests(CorpusChristiBase):
    @classmethod
    def setUpClass(cls):
        cls.recs = _records_from_pages()

    def test_record_shape(self):
        r = self.recs['cctx_119211']
        self.assertEqual(set(r), {'name', 'address', 'city', 'state', 'zip', 'latitude', 'longitude',
                                  'inspection_date', 'original_score', 'risk_score', 'priority_violations',
                                  'priority_foundation_violations', 'core_violations', 'total_violations',
                                  'violations', 'inspection_type', 'results', 'source', 'source_url',
                                  'source_id', 'metro'})
        self.assertEqual((r['name'], r['address'], r['city'], r['state'], r['zip']),
                         ('Hunan Express #3', '5614 Mcardle St.', 'Corpus Christi', 'TX', ''))
        self.assertIsNone(r['latitude'])
        self.assertEqual(r['source'], 'Corpus Christi-Nueces County Public Health District')
        self.assertEqual(r['source_url'], dp.CC_BASE + '?k=' + dp.CC_TENANT_KEY + '&s=100&i=119211')
        self.assertEqual((r['metro'], r['inspection_type']), ('', ''))

    def test_score_is_the_districts_and_tiers_are_its_points(self):
        r = self.recs['cctx_119211']
        self.assertEqual((r['original_score'], r['risk_score']), (89, 89))
        self.assertEqual([v[1] for v in r['violations']],
                         ['priority', 'priority', 'priority_foundation', 'priority_foundation', 'core'])
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'], r['core_violations'],
                          r['total_violations']), (2, 2, 1, 5))
        self.assertEqual(r['results'], 'Good (11 demerits)')
        self.assertEqual(r['violations'][4][2], 'Utensils, equipment & linens; properly stored, dried, handled: '
                                                'Can opener needs to be cleaned')
        self.assertTrue(r['violations'][0][2].startswith('Food separated and protected: Food must be stored'))

    def test_clean_visit_ships_as_100(self):
        r = self.recs['cctx_119277']
        self.assertEqual((r['risk_score'], r['original_score'], r['violations'], r['results']),
                         (100, 100, [], 'Excellent (0 demerits)'))
        self.assertEqual(self.recs['cctx_119246']['risk_score'], 100)       # an earlier clean visit

    def test_two_listing_rows_one_restaurant(self):
        a, b = self.recs['cctx_119237'], self.recs['cctx_119241']
        self.assertEqual(dp.make_restaurant_id(a['name'], a['address'], a['city']),
                         dp.make_restaurant_id(b['name'], b['address'], b['city']))
        self.assertEqual((a['risk_score'], b['risk_score']), (91, 98))

    def test_old_form_ships(self):
        r = self.recs['cctx_102379']
        self.assertEqual((r['inspection_date'], r['risk_score']), ('2023-09-19', 93))

    def test_skips_by_form_and_name(self):
        out, calls = run_fetch(since='2023-01-01')
        # pool, splash pad, flea-market stall, mobile unit, food truck, school,
        # senior center and temporary event: never requested
        for skipped in (116310, 117459, 111767, 117103, 113693, 116633, 103992, 105577):
            self.assertNotIn(skipped, calls)
        # every food location of the listing is (Yardarm, Whiskey Rio: the pre-2024 form)
        for read in (101711, 102379, 119406, 118159):
            self.assertIn(read, calls)
        self.assertIn('cctx_102379', _by_id(out))

    def test_every_record_grades(self):
        for r in self.recs.values():
            self.assertIn(dp.calculate_vetted_grade(r['risk_score'], dp.build_violation_text(r['violations'])),
                          ('A', 'B', 'C', 'F'))
            for v in r['violations']:
                self.assertNotIn('\n', v[2])
                self.assertLessEqual(len(v[2]), 500)


class NameTests(unittest.TestCase):
    def test_title_case_only_all_caps(self):
        self.assertEqual(dp._cc_title("OL' STEAKHOUSE CO."), "Ol' Steakhouse Co.")
        self.assertEqual(dp._cc_title("JOE'S 6TH STREET  GRILL"), "Joe's 6th Street Grill")
        self.assertEqual(dp._cc_title('Aerodrome Distilling'), 'Aerodrome Distilling')

    def test_permit_notes_leave_the_name(self):
        self.assertEqual(dp._cc_clean_name('BIEN MERITE **90 DAY TEMPORARY PERMIT**'), 'BIEN MERITE')
        self.assertEqual(dp._cc_clean_name('COUYON CRAWFISH LLC ** 2 YR TEMP EVENT**'), 'COUYON CRAWFISH LLC')

    def test_scope_by_name(self):
        self.assertEqual(dp._cc_out_of_scope('BLACK MARLIN MOBILE'), 'mobile unit / event booth')
        self.assertEqual(dp._cc_out_of_scope('COUSINS MAINE LOBSTER - TEMP EVENT'), 'mobile unit / event booth')
        self.assertEqual(dp._cc_out_of_scope('PORT ROYAL FOOD TRUCK'), 'mobile unit / event booth')
        self.assertEqual(dp._cc_out_of_scope('ANCHOR HARBOR MOBILE HOME COMMUNITY'), '')
        self.assertEqual(dp._cc_out_of_scope('BANQUETE ISD - FOOTBALL CONCESSION STAND'),
                         'school / institutional kitchen')
        self.assertEqual(dp._cc_out_of_scope('CORPUS CHRISTI REHABILITATION HOSPITAL'),
                         'school / institutional kitchen')
        self.assertEqual(dp._cc_out_of_scope('DEL MAR COLLEGE-HOSPITALITY AND CULINARY INSTITUTE'), '')
        self.assertEqual(dp._cc_out_of_scope('BIEN MERITE **90 DAY TEMPORARY PERMIT**'), '')
        for name in ('WEST OSO JR. HIGH', 'NORTHWEST SENIOR CITIZEN CENTER', 'REFLECTIONS SENIOR LIVING'):
            self.assertEqual(dp._cc_out_of_scope(name), 'school / institutional kitchen', name)

    def test_form_scope(self):
        for form in ('2024 Inspections-Restaurants', 'Restaurant', 'Inspection - Restaurant 2024'):
            self.assertEqual(dp._cc_form_scope(form), 'food')
        for form in ('Public Swimming Pool & Spa 2026', 'Pool Form 2021', 'Splash Pools 2026',
                     'Public Swimming Pools and Spas 2024', 'Flea Market Food Vendors'):
            self.assertEqual(dp._cc_form_scope(form), 'skip')
        self.assertEqual(dp._cc_form_scope('2027 Retail Inspections'), 'unknown')


class PestTitleTests(CorpusChristiBase):
    @classmethod
    def setUpClass(cls):
        cls.recs = _records_from_pages()

    def lines(self, sid):
        return [v[2] for v in self.recs[sid]['violations']]

    def grade(self, sid):
        r = self.recs[sid]
        return dp.calculate_vetted_grade(r['risk_score'], dp.build_violation_text(r['violations']))

    def test_no_pest_title_ships_with_a_note(self):
        for r in self.recs.values():
            for v in r['violations']:
                self.assertFalse(re.match(r'(?i)(insects, rodents|no evidence of insect)[^:]*:', v[2]), v[2])
                self.assertFalse(v[2].lower().startswith('sewage'), v[2])

    def test_gnats_do_not_force_an_f(self):
        self.assertIn('Pests and animals: Gnats present', self.lines('cctx_118159'))
        self.assertEqual(self.grade('cctx_118159'), 'B')
        self.assertIn('Pests and animals: Gnats', self.lines('cctx_102379'))      # the pre-2024 form's item 34
        self.assertEqual(self.grade('cctx_102379'), 'A')

    def test_named_vermin_in_the_note_still_forces_an_f(self):
        self.assertIn('Pests and animals: Rodent droppings found storage room call pest control',
                      self.lines('cctx_119406'))
        self.assertEqual(self.grade('cctx_119406'), 'F')
        self.assertEqual(self.grade('cctx_118506'), 'F')
        self.assertIn('Pests and animals: Observed flies and roach activity in kitchen area, dry storage shelves. '
                      'Follow up with pest services', self.lines('cctx_118295'))
        self.assertEqual(self.grade('cctx_118295'), 'F')

    def test_sewage_item_under_a_neutral_heading(self):
        self.assertIn('Wastewater disposal: Observed missing mop sink, work order must be in place for '
                      'replacement', self.lines('cctx_118295'))

    def test_pest_item_without_a_note_keeps_the_registered_title(self):
        lines = self.lines('cctx_119102')
        self.assertIn('Insects, rodents, and animals not present', lines)
        self.assertEqual(jr.cited_pest_item('Insects, rodents, and animals not present'), 'pests_unspecified')
        self.assertFalse(dp.has_active_pest_or_sewage(dp.build_violation_text(self.recs['cctx_119102']['violations'])))
        self.assertEqual(self.grade('cctx_119102'), 'B')

    def test_title_words_alone_never_force_an_f(self):
        for title in ('Insects, rodents, and animals not present', 'Pests and animals',
                      'No Evidence of Insect contamination, rodent/other animals', 'Wastewater disposal',
                      'Pests and animals: Gnats near ice machine',
                      'Wastewater disposal: Water leaking from the floor drain'):
            self.assertFalse(dp.has_active_pest_or_sewage(title), title)
        rows = [(1, '34', 'No Evidence of Insect contamination, rodent/other animals', '', 'No'),
                (2, '52', 'Sewage and waste water properly disposed', 'Sewage backing up from floor drain', 'No')]
        lines = [v[2] for v in dp._cc_violations(rows)]
        self.assertEqual(lines[0], 'No Evidence of Insect contamination, rodent/other animals')
        self.assertEqual(lines[1], 'Wastewater disposal: Sewage backing up from floor drain')
        self.assertTrue(dp.has_active_pest_or_sewage(lines[1]))           # the note still tells


class JevPolicyTests(unittest.TestCase):
    def test_source_stated_tiers_and_official_score(self):
        self.assertIn(dp.CC_SOURCE, jr.SOURCE_TIER_SOURCES)
        self.assertEqual(jr.tier_policy(dp.CC_SOURCE), 'fixed')
        self.assertEqual(jr.score_policy(dp.CC_SOURCE, 89), 'official')
        self.assertNotIn(dp.CC_SOURCE, jr.SOURCE_TIER_SCORE_SOURCES)

    def test_slug_and_group_in_main(self):
        src = inspect.getsource(dp.main)
        self.assertIn("(('corpuschristi',), CC_GROUP, fetch_corpus_christi)", src)
        self.assertEqual(dp.CC_GROUP, 'Corpus Christi')


class FailureTests(CorpusChristiBase):
    def test_listing_http_error_raises_after_retries(self):
        with self.assertRaisesRegex(RuntimeError, 'listing unreadable'):
            run_fetch(since='2026-11-01', answers={'list': _Resp('', 500)})
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_listing_refused_is_not_retried(self):
        sess = _Session({'list': _Resp('', 403)})
        with mock.patch.object(dp, '_cc_session', return_value=sess), mock.patch.object(dp.time, 'sleep'), \
                self.assertRaises(RuntimeError):
            dp.fetch_corpus_christi(since_date='2026-11-01')
        self.assertEqual(sess.calls, ['list'])

    def test_listing_retries_a_timeout(self):
        out, calls = run_fetch(since='2026-11-20', answers={'list': [dp.requests.Timeout('slow'),
                                                                     _Resp(_fixture('list.html'))]})
        self.assertEqual(calls[:2], ['list', 'list'])
        self.assertEqual([r['source_id'] for r in out], ['cctx_119406'])
        self.assertIn(119443, calls)

    def test_wrong_key_body_raises(self):
        with self.assertRaisesRegex(RuntimeError, 'listing unreadable'):
            run_fetch(since='2026-11-01', listing="{ 'ERROR':'Invalid Key' }")

    def test_unknown_form_names(self):
        listing = _listing_with(119211, form='2027 Retail Inspections')
        out, _ = run_fetch(since='2026-11-01', listing=listing)
        self.assertNotIn('cctx_119211', _by_id(out))                       # not shipped, logged
        with mock.patch.object(dp, 'CC_UNKNOWN_FORM_MIN', 1), self.assertRaisesRegex(RuntimeError, 'does not know'):
            run_fetch(since='2026-11-01', listing=listing)

    def test_latest_page_failing_holds_the_window(self):
        out, calls = run_fetch(since='2026-11-01', answers={119211: _Resp('', 502)})
        self.assertEqual(calls.count(119211), dp.CC_RETRIES)
        self.assertNotIn('cctx_119211', _by_id(out))
        self.assertEqual(len(out), 6)
        holds = dp._FRESHNESS_HOLDS[dp.CC_SOURCE]
        self.assertEqual(list(holds), ['2026-11-03|119211'])    # window start 11-01 + 1 + overlap, counted
        self.assertEqual(holds['2026-11-03|119211'], (True, False))

    def test_earlier_page_failing_holds_its_own_day(self):
        out, _ = run_fetch(since='2026-11-01', answers={119246: dp.requests.ConnectionError('reset')})
        self.assertNotIn('cctx_119246', _by_id(out))
        self.assertEqual(_hold_keys(), {'119246': True})
        self.assertEqual(list(dp._FRESHNESS_HOLDS[dp.CC_SOURCE]), ['2026-11-05|119246'])

    def test_unpublished_page_for_a_listed_id_is_dropped_not_clean(self):
        out, calls = run_fetch(since='2026-11-01', answers={119277: _Resp(_page(119426))})
        self.assertNotIn('cctx_119277', _by_id(out))
        self.assertEqual(calls.count(119277), 2)                  # re-read once
        self.assertEqual(_hold_keys(), {'119277': True})
        self.assertNotIn(119246, calls)        # its history was never read, so the window is held instead

    def test_unscored_visit_is_read_once_skipped_and_not_held(self):
        out, calls = run_fetch(since='2026-11-15')
        self.assertEqual(calls.count(119443), 1)
        self.assertNotIn('cctx_119443', _by_id(out))
        self.assertEqual(dp._FRESHNESS_HOLDS, {})
        self.assertEqual(dp._GROUP_STATUS, {})

    def test_mostly_unscored_raises(self):
        with mock.patch.object(dp, 'CC_MISMATCH_MIN', 1), mock.patch.object(dp, 'CC_UNSCORED_MAX_SHARE', 0.2), \
                self.assertRaisesRegex(RuntimeError, 'carry no score'):
            run_fetch(since='2026-11-15')

    def test_glitch_then_good_page_ships(self):
        out, _ = run_fetch(since='2026-11-01', answers={119211: [_Resp(_page(119426)), _Resp(_page(119211))]})
        self.assertIn('cctx_119211', _by_id(out))
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_score_mismatch_drops_and_holds(self):
        out, _ = run_fetch(since='2026-11-01', listing=_listing_with(119211, score='90'))
        self.assertNotIn('cctx_119211', _by_id(out))
        self.assertEqual(_hold_keys(), {'119211': True})
        self.assertEqual(dp._GROUP_STATUS, {})

    def test_many_mismatches_raise(self):
        listing = _listing_with(119211, score='90')
        with mock.patch.object(dp, 'CC_MISMATCH_MIN', 1), self.assertRaisesRegex(RuntimeError, 'disagree'):
            run_fetch(since='2026-11-01', listing=listing)

    def test_mostly_clean_window_raises(self):
        with mock.patch.object(dp, 'CC_MASS_CLEAN_MIN', 2), mock.patch.object(dp, 'CC_MASS_CLEAN_SHARE', 0.25), \
                self.assertRaisesRegex(RuntimeError, 'read as clean'):
            run_fetch(since='2026-11-01')

    def test_breaker_blocks_the_rest_and_holds_uncounted(self):
        bad = _Resp('', 503)
        out, calls = run_fetch(since='2026-11-01', answers={i: bad for i in (119102, 119211, 119237, 119241)})
        self.assertEqual(out, [])
        self.assertEqual(len([c for c in calls if c != 'list']), dp.CC_BREAKER_TRIP * dp.CC_RETRIES)
        self.assertEqual(dp._GROUP_STATUS[dp.CC_GROUP][0], 'blocked')
        keys = _hold_keys()
        self.assertTrue(keys['119102'])                    # tried and failed before the trip: counted
        self.assertFalse(keys['119237'])                   # the trip itself is the portal's, not the record's
        self.assertFalse(keys['locations'])                # the rest of the window: held, not counted

    def test_pages_that_are_not_inspections_trip_the_breaker(self):
        junk = _Resp('Invalid properties detected, cannot load dataset.')
        out, calls = run_fetch(since='2026-11-01', answers={i: junk for i in (119102, 119211, 119237, 119241,
                                                                               119277, 119406, 119443)})
        self.assertEqual(out, [])
        self.assertEqual(dp._GROUP_STATUS[dp.CC_GROUP][0], 'blocked')
        self.assertLess(len(calls), 1 + 2 * 7)

    def test_deadline_cut_reports_skipped_and_holds_uncounted(self):
        dp._FETCH_DEADLINE = time.time() - 1
        out, calls = run_fetch(since='2026-11-01')
        self.assertEqual(out, [])
        self.assertEqual(calls, ['list'])
        self.assertEqual(dp._GROUP_STATUS[dp.CC_GROUP][0], 'skipped')
        self.assertEqual(_hold_keys(), {'locations': False})

    def test_deadline_in_the_history_phase_holds_each_day(self):
        real = dp._cc_read
        def read(session, insp_id, listed):
            res = real(session, insp_id, listed)
            if insp_id == 119443:                       # the last latest page; the deadline passes after it
                dp._FETCH_DEADLINE = time.time() - 1
            return res
        with mock.patch.object(dp, '_cc_read', side_effect=read):
            out, calls = run_fetch(since='2026-11-01')
        self.assertNotIn(119246, calls)
        self.assertEqual(len(out), 6)
        self.assertEqual(_hold_keys(), {'119246': False})
        self.assertEqual(list(dp._FRESHNESS_HOLDS[dp.CC_SOURCE]), ['2026-11-05|119246'])
        self.assertEqual(dp._GROUP_STATUS[dp.CC_GROUP][0], 'skipped')


if __name__ == '__main__':
    unittest.main()
