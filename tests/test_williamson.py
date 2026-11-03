"""Tests for the Williamson County (WCCHD) zip-snapshot fetcher. No network:
the one GET is mocked with a zip built in memory from
tests/fixtures/williamson/. businesses.csv and inspections.csv there are
byte-verbatim rows (CRLF, no BOM, quoted comma addresses) for 19 permits
trimmed from the district's 2026-10-29 snapshot
(https://storage.googleapis.com/ehyelp/WCCHD%20Food%20Inspections.zip,
captured 2026-11-03); feed_info.xlsx is the real workbook from that zip.
Edge rows the live file does not have (a BOM, a bad date, an unknown
result, a reshaped header, other permit codes) are synthesised here."""
import io
import logging
import os
import re
import sys
import time
import unittest
import zipfile
from datetime import date, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402
import jev_reader as jr  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'williamson')
BUS_HDR = 'business_id,name,address,city,state'
INS_HDR = 'business_id,date,score,result,type'
KEYS = {'name', 'address', 'city', 'state', 'zip', 'latitude', 'longitude', 'inspection_date',
        'original_score', 'risk_score', 'priority_violations', 'priority_foundation_violations',
        'core_violations', 'total_violations', 'violations', 'inspection_type', 'results',
        'source', 'source_url', 'source_id', 'metro'}


def _fixture(name):
    with open(os.path.join(FIXTURES, name), 'rb') as fh:
        return fh.read()


def _csv(header, rows):
    return ('\r\n'.join([header] + list(rows)) + '\r\n').encode('utf-8')


def _xlsx(rows):
    """A one-sheet workbook with inline strings and bare numbers (no
    sharedStrings part), the other way Excel writes cells."""
    ns = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'

    def cell(ref, v):
        if isinstance(v, (int, float)):
            return f'<c r="{ref}"><v>{v}</v></c>'
        return f'<c r="{ref}" t="inlineStr"><is><t>{v}</t></is></c>'
    body = ''.join(f'<row r="{ri}">' + ''.join(cell(f'{chr(65 + ci)}{ri}', v) for ci, v in enumerate(row))
                   + '</row>' for ri, row in enumerate(rows, 1))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        z.writestr('xl/workbook.xml', f'<workbook xmlns="{ns}"><sheets><sheet name="Sheet1" sheetId="1"/></sheets></workbook>')
        z.writestr('xl/worksheets/sheet1.xml',
                   f'<?xml version="1.0" encoding="UTF-8"?><worksheet xmlns="{ns}"><sheetData>{body}</sheetData></worksheet>')
    return buf.getvalue()


def _zip(businesses=None, inspections=None, feed_info=None, drop=(), prefix=''):
    """A snapshot zip: the fixture members unless overridden."""
    members = {
        'feed_info.xlsx': _fixture('feed_info.xlsx') if feed_info is None else feed_info,
        'businesses.csv': _fixture('businesses.csv') if businesses is None else businesses,
        'inspections.csv': _fixture('inspections.csv') if inspections is None else inspections,
    }
    for name in drop:
        members.pop(name)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z:
        for name, data in members.items():
            z.writestr(prefix + name, data)
    return buf.getvalue()


class _Resp:
    def __init__(self, content=b'', status=200):
        self.content = content
        self.status_code = status
        self.headers = {'ETag': '"64002b2b6ed04fbe80149ce346bd88fc"',
                        'Last-Modified': 'Thu, 29 Oct 2026 15:56:36 GMT'}


def _fetch(since=None, limit=None, zip_bytes=None, replies=None, guards=False):
    """Run fetch_williamson against one zip, or a scripted list of replies
    and exceptions. The fixture is far smaller than a real snapshot, so the
    row-count guards are lowered unless a test asks for them."""
    if replies is None:
        replies = [_Resp(_zip() if zip_bytes is None else zip_bytes)]
    floor = (dp.WM_MIN_INSPECTION_ROWS, dp.WM_MIN_BUSINESS_ROWS) if guards else (1, 1)
    with mock.patch.object(dp.requests, 'get', side_effect=replies) as get, \
            mock.patch.object(dp.time, 'sleep') as sleep, \
            mock.patch.object(dp, 'WM_MIN_INSPECTION_ROWS', floor[0]), \
            mock.patch.object(dp, 'WM_MIN_BUSINESS_ROWS', floor[1]):
        records = dp.fetch_williamson(since_date=since, limit=limit)
    return records, get, sleep


def _by_id(records):
    return {r['source_id']: r for r in records}


class _Base(unittest.TestCase):
    """Save, clear and restore the pipeline globals the fetcher touches."""

    def setUp(self):
        self._saved = (dict(dp._GROUP_NOTES), dict(dp._GROUP_STATUS), dict(dp._FRESHNESS_HOLDS),
                       dp._FETCH_DEADLINE, dp.log.level)
        dp._GROUP_NOTES.clear()
        dp._GROUP_STATUS.clear()
        dp._FRESHNESS_HOLDS.clear()
        dp._FETCH_DEADLINE = None
        dp.log.setLevel(logging.WARNING)

    def tearDown(self):
        notes, status, holds, deadline, level = self._saved
        dp._GROUP_NOTES.clear(); dp._GROUP_NOTES.update(notes)
        dp._GROUP_STATUS.clear(); dp._GROUP_STATUS.update(status)
        dp._FRESHNESS_HOLDS.clear(); dp._FRESHNESS_HOLDS.update(holds)
        dp._FETCH_DEADLINE = deadline
        dp.log.setLevel(level)


class RequestShapeTests(_Base):
    def test_one_get_with_browser_headers_and_timeout(self):
        records, get, sleep = _fetch()
        self.assertEqual(len(records), 46)
        self.assertEqual(get.call_count, 1)          # the zip is the whole dataset: no paging
        self.assertEqual(get.call_args.args[0], dp.WM_ZIP_URL)
        self.assertEqual(get.call_args.kwargs['headers']['User-Agent'], dp.CHROME_UA)
        self.assertEqual(get.call_args.kwargs['timeout'], dp.WM_TIMEOUT)
        sleep.assert_not_called()

    def test_transient_failures_retry_with_backoff_then_succeed(self):
        replies = [dp.requests.ConnectionError('reset'), _Resp(b'', 503), _Resp(_zip())]
        records, get, sleep = _fetch(replies=replies)
        self.assertEqual(len(records), 46)
        self.assertEqual(get.call_count, 3)
        self.assertEqual([c.args[0] for c in sleep.call_args_list],
                         [dp.WM_RETRY_DELAY, dp.WM_RETRY_DELAY * 2])

    def test_429_is_retried_like_a_5xx(self):
        records, get, _ = _fetch(replies=[_Resp(b'slow down', 429), _Resp(_zip())])
        self.assertEqual(len(records), 46)
        self.assertEqual(get.call_count, 2)

    def test_exhausted_retries_raise_instead_of_shipping_a_partial_window(self):
        replies = [dp.requests.ConnectionError('reset')] * dp.WM_RETRIES
        with self.assertRaises(RuntimeError):
            _fetch(replies=replies)
        # Nothing shipped, nothing reported as read: the merge keeps last
        # week's records and _record_group marks the group 'error'.
        self.assertNotIn(dp.WM_GROUP, dp._GROUP_NOTES)
        self.assertNotIn(dp.WM_GROUP, dp._GROUP_STATUS)
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_a_moved_or_private_object_fails_at_once(self):
        for status in (403, 404):
            with self.subTest(status=status):
                with self.assertRaises(RuntimeError) as ctx:
                    _fetch(replies=[_Resp(b'<Error><Code>AccessDenied</Code></Error>', status)])
                self.assertIn(str(status), str(ctx.exception))

    def test_deadline_before_the_download_reports_skipped_and_ships_nothing(self):
        dp._FETCH_DEADLINE = time.time() - 1
        records, get, _ = _fetch()
        self.assertEqual(records, [])
        get.assert_not_called()
        self.assertEqual(dp._GROUP_STATUS[dp.WM_GROUP][0], 'skipped')

    def test_deadline_during_retries_reports_skipped(self):
        with mock.patch.object(dp, '_past_deadline', side_effect=[False, True]):
            records, get, sleep = _fetch(replies=[dp.requests.ConnectionError('reset')])
        self.assertEqual(records, [])
        self.assertEqual(get.call_count, 1)
        sleep.assert_not_called()
        status, detail = dp._GROUP_STATUS[dp.WM_GROUP]
        self.assertEqual(status, 'skipped')
        self.assertIn('deadline', detail)


class SnapshotGuardTests(_Base):
    def test_a_fixture_sized_snapshot_is_refused_with_the_real_row_minimums(self):
        with self.assertRaises(RuntimeError) as ctx:
            _fetch(guards=True)
        self.assertIn('truncated', str(ctx.exception))

    def test_a_missing_member_is_refused(self):
        for member in ('feed_info.xlsx', 'businesses.csv', 'inspections.csv'):
            with self.subTest(member=member):
                with self.assertRaises(RuntimeError) as ctx:
                    _fetch(zip_bytes=_zip(drop=(member,)))
                self.assertIn(member, str(ctx.exception))

    def test_a_non_zip_body_is_refused(self):
        with self.assertRaises(RuntimeError) as ctx:
            _fetch(zip_bytes=b'<html>Service Unavailable</html>')
        self.assertIn('not a zip', str(ctx.exception))

    def test_an_unreadable_feed_info_is_refused(self):
        for bad in (_xlsx([['contact_email'], ['eh@wilco.org']]), b'not a workbook'):
            with self.subTest(bad=bad[:12]):
                with self.assertRaises(RuntimeError) as ctx:
                    _fetch(zip_bytes=_zip(feed_info=bad))
                self.assertIn('feed_info', str(ctx.exception))

    def test_a_reshaped_header_is_refused(self):
        rows = _fixture('inspections.csv').decode().split('\r\n')
        rows[0] = 'business_id,inspection_date,score,result,type'
        with self.assertRaises(RuntimeError) as ctx:
            _fetch(zip_bytes=_zip(inspections='\r\n'.join(rows).encode()))
        self.assertIn("'date'", str(ctx.exception))

    def test_more_than_one_percent_unparseable_rows_is_refused(self):
        bad = _fixture('inspections.csv') + b'RR-679,2026-01-05,88,Pass,Routine\r\n'   # 1 of 57
        with self.assertRaises(RuntimeError) as ctx:
            _fetch(zip_bytes=_zip(inspections=bad))
        self.assertIn('unparseable', str(ctx.exception))

    def test_a_few_unparseable_rows_are_skipped_not_fatal(self):
        good = [f'RR-679,{date(2025, 1, 1) + timedelta(days=i):%Y%m%d},90,Pass,Routine' for i in range(200)]
        bad = ['RR-679,2025-13-45,90,Pass,Routine', 'RR-679,20250901,n/a,Pass,Routine']   # 2 of 202
        records, _, _ = _fetch(zip_bytes=_zip(inspections=_csv(INS_HDR, good + bad)))
        self.assertEqual(len(records), 200)
        self.assertTrue(all(r['risk_score'] == 90 for r in records))

    def test_members_inside_a_folder_are_found_by_basename(self):
        records, _, _ = _fetch(zip_bytes=_zip(prefix='WCCHD Food Inspections/'))
        self.assertEqual(len(records), 46)


class RecordTests(_Base):
    @classmethod
    def setUpClass(cls):
        saved, level = dict(dp._GROUP_NOTES), dp.log.level
        dp.log.setLevel(logging.WARNING)
        try:
            cls.records, _, _ = _fetch()
        finally:
            dp._GROUP_NOTES.clear()
            dp._GROUP_NOTES.update(saved)
            dp.log.setLevel(level)
        cls.by_id = _by_id(cls.records)

    def test_scope_and_count(self):
        # 56 rows: 2 Thrall ISD + 3 Georgetown HS (school kitchens), 1 mobile
        # unit, 1 district test permit, 1 exact duplicate, 1 orphan and 1
        # negative score go; 46 inspections of 14 food establishments ship.
        self.assertEqual(len(self.records), 46)
        permits = {re.fullmatch(r'wcchd_(.+)_\d{8}(?:_\d+)?', r['source_id']).group(1) for r in self.records}
        for permit in ('GES-1-ISD', 'THS-2-ISD', '2025-322-MF', '2025-1412-FFE', '2026-1304-FFE'):
            self.assertNotIn(permit, permits)
        self.assertEqual(len(permits), 14)

    def test_official_score_record_shape(self):
        r = self.by_id['wcchd_RR-679_20250806']
        self.assertEqual(r, {
            'name': "Bush's Chicken",
            'address': '1410 Round Rock Ave',
            'city': 'Round Rock',
            'state': 'TX',
            'zip': '',
            'latitude': None,
            'longitude': None,
            'inspection_date': '2025-08-06',
            'original_score': 80,
            'risk_score': 80,
            'priority_violations': 0,
            'priority_foundation_violations': 0,
            'core_violations': 0,
            'total_violations': 0,
            'violations': [],
            'inspection_type': 'Routine',
            'results': 'Pass',
            'source': 'Williamson County and Cities Health District',
            'source_url': 'https://www.wcchd.org/healthy_spaces/inspection_scores.php',
            'source_id': 'wcchd_RR-679_20250806',
            'metro': '',
        })
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'B')
        for rec in self.records:
            self.assertEqual(set(rec), KEYS)
            self.assertIsNone(rec['latitude'])        # no coordinates in the feed: main() geocodes
            self.assertIsNone(rec['longitude'])
            self.assertEqual((rec['zip'], rec['state'], rec['metro']), ('', 'TX', ''))
            self.assertEqual(rec['violations'], [])
            self.assertEqual(rec['original_score'], rec['original_score'] and int(rec['original_score']))
            self.assertTrue(0 <= rec['risk_score'] <= 100)

    def test_newest_first_and_mixed_case_kept_as_published(self):
        first = self.records[0]
        self.assertEqual(first['source_id'], 'wcchd_2026-1214-FFE_20261028')
        self.assertEqual(first['name'], 'Sangam Chettinad Indian Cuisine and Bar')
        self.assertEqual(first['address'], '14300 Ronald Reagan, 106')
        self.assertEqual(first['city'], 'Cedar Park')
        dates = [r['inspection_date'] for r in self.records]
        self.assertEqual(dates, sorted(dates, reverse=True))

    def test_fail_result_grades_f_on_the_official_score(self):
        r = self.by_id['wcchd_2026-1214-FFE_20261014']
        self.assertEqual((r['original_score'], r['risk_score'], r['results']), (39, 39, 'Fail'))
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'F')

    def test_ihh_closure_caps_risk_to_f_and_keeps_the_published_score(self):
        closed = self.by_id['wcchd_LE-0002_20241202']       # score 100, closed anyway
        self.assertEqual(closed['original_score'], 100)
        self.assertEqual(closed['risk_score'], dp.WM_IHH_RISK_CAP)
        self.assertEqual(closed['results'], 'Imminent Health Hazard - closed')
        self.assertEqual(closed['inspection_type'], 'Follow-up')
        self.assertEqual(dp.calculate_vetted_grade(closed['risk_score']), 'F')
        # A cap never raises a lower score.
        for sid, score in (('wcchd_CP-1032_20260813', 58), ('wcchd_RR-0005_20240703', 60)):
            r = self.by_id[sid]
            self.assertEqual((r['original_score'], r['risk_score']), (score, min(score, dp.WM_IHH_RISK_CAP)))
        self.assertEqual(sum(1 for r in self.records if r['results'].startswith('Imminent')), 3)

    def test_inspection_types(self):
        self.assertEqual(self.by_id['wcchd_RR-0005_20240813']['inspection_type'], 'Follow-up')
        self.assertEqual(self.by_id['wcchd_RR-0005_20251015']['inspection_type'], 'Routine')
        self.assertEqual({r['inspection_type'] for r in self.records}, {'Routine', 'Follow-up'})
        self.assertEqual({r['results'] for r in self.records},
                         {'Pass', 'Fail', 'Imminent Health Hazard - closed'})

    def test_negative_score_and_orphan_rows_are_dropped_with_a_warning(self):
        self.assertNotIn('wcchd_RR-0230_20220526', self.by_id)     # score -71
        self.assertEqual(sum(1 for s in self.by_id if s.startswith('wcchd_RR-0230_')), 3)
        self.assertNotIn('wcchd_2026-1304-FFE_20260805', self.by_id)  # no businesses.csv row
        with self.assertLogs('dinescores', level='WARNING') as logs:
            _fetch()
        text = '\n'.join(logs.output)
        self.assertIn('1 no businesses.csv row', text)
        self.assertIn('1 score <= 0', text)

    def test_same_day_pair_ships_worst_first_with_distinct_ids(self):
        fail = self.by_id['wcchd_RR-1410_20250606']
        passed = self.by_id['wcchd_RR-1410_20250606_2']
        self.assertEqual((fail['original_score'], fail['results'], fail['inspection_type']),
                         (65, 'Fail', 'Routine'))
        self.assertEqual((passed['original_score'], passed['results'], passed['inspection_type']),
                         (84, 'Pass', 'Follow-up'))
        self.assertLess(self.records.index(fail), self.records.index(passed))
        ids = [r['source_id'] for r in self.records]
        self.assertEqual(len(ids), len(set(ids)))
        # Both land on one restaurant id and one D1 inspection day; the
        # output paths keep the worst.
        self.assertEqual(dp.make_restaurant_id(fail['name'], fail['address'], fail['city']),
                         dp.make_restaurant_id(passed['name'], passed['address'], passed['city']))

    def test_an_exact_duplicate_row_is_emitted_once(self):
        same_day = [s for s in self.by_id if s.startswith('wcchd_LE-0002_20260714')]
        self.assertEqual(same_day, ['wcchd_LE-0002_20260714'])
        self.assertEqual(self.by_id['wcchd_LE-0002_20260714']['original_score'], 66)

    def test_normalisation_of_published_strings(self):
        cases = {
            'wcchd_2025-2186-FFE_20260317': ('Highway 79 Gas Station', '2207 W 2nd St, 100', 'Taylor'),  # 'TAYLOR,'
            'wcchd_CP-0118_20241120': ('Cava Grill', '905 E Whitestone Blvd', 'Cedar Park'),        # 'CAVA  GRILL'
            'wcchd_2022-340-FFE_20250227': ('Aparicio’s', '11880 Hero Way West, 201', 'Leander'),
            'wcchd_CP-733_20251117': ("Bush's Chicken", '12336 N FM 620 Rd', 'Austin'),
            'wcchd_2024-790-FFE_20251013': ('H-E-B', '10110 RM 2338', 'Georgetown'),
            'wcchd_GE-681_20250902': ('600 Degrees Pizzeria & Drafthouse', '124 E 8th St', 'Georgetown'),
            'wcchd_CP-0077_20260129': ('Here And There Hospitality, LLC', '200 Buttercup Creek Blvd', 'Cedar Park'),
            'wcchd_RR-0005_20251015': ('Ros Niyom Thai', '2000 S IH 35', 'Round Rock'),
            'wcchd_CP-1032_20260813': ('Donut Crown', '1201 N Lakeline Blvd, Ste 750', 'Cedar Park'),
            'wcchd_LE-0002_20260728': ('Whipped', '15609 Ronald W Reagan Blvd', 'Leander'),
        }
        for sid, (name, address, city) in cases.items():
            r = self.by_id[sid]
            self.assertEqual((r['name'], r['address'], r['city']), (name, address, city), sid)
        self.assertEqual(self.by_id['wcchd_CP-733_20251117']['metro'], '')   # Austin rows: no metro chip

    def test_title_city_and_address_helpers(self):
        self.assertEqual(dp._wm_title("LIL'  MAMA'S KITCHEN"), "Lil' Mama's Kitchen")
        self.assertEqual(dp._wm_title("SMOKEY MO'S BBQ"), "Smokey Mo's BBQ")
        self.assertEqual(dp._wm_title('TOYS R US'), 'Toys R Us')                 # name codes only
        self.assertEqual(dp._wm_title('McDonald’s Restaurant# 39672'), 'McDonald’s Restaurant# 39672')
        self.assertEqual(dp._wm_address('216 E US 79'), '216 E US 79')
        self.assertEqual(dp._wm_address('4701 TX 183A TOLL RD'), '4701 TX 183A Toll Rd')
        self.assertEqual(dp._wm_address('3333 CR 119, STE 45'), '3333 CR 119, Ste 45')
        self.assertEqual(dp._wm_address('1 NE INNER LOOP'), '1 NE Inner Loop')
        self.assertEqual(dp._wm_address('445 E Morrow St,'), '445 E Morrow St')
        self.assertEqual(dp._wm_address('5430 E. Palm Valley Blvd'), '5430 E. Palm Valley Blvd')
        self.assertEqual(dp._wm_address('2800 S IH 35, #300'), '2800 S IH 35, #300')   # suites as published
        for raw, want in (('TAYLOR,', 'Taylor'), ('Round rock', 'Round Rock'), ('round rock', 'Round Rock'),
                          ('Leander 78641', 'Leander'), ('Liberty Hill78642', 'Liberty Hill'),
                          (', Georgetown', 'Georgetown'), ('Round Rock`', 'Round Rock'),
                          ('Cedar park', 'Cedar Park'), ('301 w bagdad ave', ''), ('', '')):
            self.assertEqual(dp._wm_city(raw), want, raw)

    def test_make_restaurant_id_is_stable_across_runs_and_source_casing(self):
        again, _, _ = _fetch()
        for a, b in zip(self.records, again):
            self.assertEqual(a['source_id'], b['source_id'])
            self.assertEqual(dp.make_restaurant_id(a['name'], a['address'], a['city']),
                             dp.make_restaurant_id(b['name'], b['address'], b['city']))
        r = self.by_id['wcchd_RR-679_20250806']
        self.assertEqual(dp.make_restaurant_id(r['name'], r['address'], r['city']), '1197a7af02b04f5c')
        # The district re-keys casing and whitespace between snapshots:
        # 'ROUND ROCK' / 'round rock', 'CAVA  GRILL' / 'CAVA GRILL' and
        # 'TAYLOR,' / 'Taylor' must land on the same restaurant.
        variant = (_fixture('businesses.csv')
                   .replace(b'ROUND ROCK', b'round rock')
                   .replace(b"BUSH'S CHICKEN,1410", b"bush's chicken,1410")
                   .replace(b'CAVA  GRILL', b'CAVA GRILL')
                   .replace(b'"TAYLOR,"', b'Taylor'))
        other, _, _ = _fetch(zip_bytes=_zip(businesses=variant))
        ids = lambda recs: {r['source_id']: dp.make_restaurant_id(r['name'], r['address'], r['city'])  # noqa: E731
                            for r in recs}
        self.assertEqual(ids(other), ids(self.records))
        # Every inspection of one permit lands on one restaurant id.
        self.assertEqual(len({dp.make_restaurant_id(x['name'], x['address'], x['city'])
                              for x in self.records if x['source_id'].startswith('wcchd_LE-0002_')}), 1)

    def test_jev_policy_is_official_and_no_relay_is_needed(self):
        # No violation lines: nothing for Jev to re-tier, the score is the
        # county's and stays frozen. The label must not read as an MHD county.
        self.assertEqual(jr.score_policy(dp.WM_SOURCE, 90), 'official')
        self.assertEqual(jr.tier_policy(dp.WM_SOURCE), 'free')
        self.assertFalse(dp.WM_SOURCE.endswith(jr.MHD_SOURCE_SUFFIX))
        self.assertFalse(dp._wants_portal_sources(['williamson']))

    def test_a_complete_read_leaves_no_hold_and_no_partial_status(self):
        # One atomic object: nothing can be dropped transiently, so there is
        # never a freshness hold; a complete read reports a plain 'ok'.
        _fetch()
        self.assertEqual(dp._FRESHNESS_HOLDS, {})
        self.assertNotIn(dp.WM_GROUP, dp._GROUP_STATUS)


class WindowTests(_Base):
    def test_since_date_in_both_formats_is_inclusive(self):
        for since in ('2026-08-13', '2026-08-13T00:00:00'):
            records, _, _ = _fetch(since=since)
            self.assertEqual(sorted(_by_id(records)),
                             ['wcchd_2026-1214-FFE_20261014', 'wcchd_2026-1214-FFE_20261028',
                              'wcchd_CP-1032_20260813'], since)
            self.assertTrue(all(r['inspection_date'] >= since[:10] for r in records))
        records, _, _ = _fetch(since='2026-08-14')
        self.assertEqual(len(records), 2)

    def test_no_since_means_the_whole_file(self):
        records, _, _ = _fetch()
        self.assertEqual(len(records), 46)
        self.assertEqual(min(r['inspection_date'] for r in records), '2022-04-20')

    def test_an_empty_window_is_an_empty_list_not_an_error(self):
        records, get, _ = _fetch(since='2027-01-01')
        self.assertEqual(records, [])
        self.assertEqual(get.call_count, 1)

    def test_limit_keeps_the_newest_records(self):
        records, _, _ = _fetch(limit=2)
        self.assertEqual([r['source_id'] for r in records],
                         ['wcchd_2026-1214-FFE_20261028', 'wcchd_2026-1214-FFE_20261014'])

    def test_lag_note_names_the_feeds_newest_row_and_a_bounded_read_leaves_none(self):
        businesses = _csv(BUS_HDR, ['RR-679,BUSH\'S CHICKEN,1410 ROUND ROCK AVE,ROUND ROCK,TX',
                                    'GES-1-ISD,GEORGETOWN HIGH SCHOOL,2211 N AUSTIN AVE,GEORGETOWN,TX'])
        # The newest row overall is the school's (skipped): the note still
        # reports the FEED's newest date, since that is what the source holds.
        inspections = _csv(INS_HDR, ['RR-679,20260110,88,Pass,Routine',
                                     'RR-679,20260105,70,Pass,Followup',
                                     'GES-1-ISD,20260115,91,Pass,Routine'])
        snapshot = _zip(businesses=businesses, inspections=inspections)
        records, _, _ = _fetch(since='2026-01-01', zip_bytes=snapshot)
        self.assertEqual(len(records), 2)
        self.assertIn(dp.WM_GROUP, dp._GROUP_NOTES)
        self.assertIn('2026-01-15', dp._GROUP_NOTES[dp.WM_GROUP])
        dp._GROUP_NOTES.clear()
        records, _, _ = _fetch(since='2026-01-01', limit=1, zip_bytes=snapshot)
        self.assertEqual(len(records), 1)
        self.assertNotIn(dp.WM_GROUP, dp._GROUP_NOTES)      # a bounded read leaves no note
        # An empty window still explains the lag.
        records, _, _ = _fetch(since='2026-02-01', zip_bytes=snapshot)
        self.assertEqual(records, [])
        self.assertIn('2026-01-15', dp._GROUP_NOTES.get(dp.WM_GROUP, ''))


class FeedInfoTests(unittest.TestCase):
    def test_the_real_workbook(self):
        self.assertEqual(dp._wm_read_feed_info(_fixture('feed_info.xlsx')), {
            'feed_date': '2026-10-29',
            'feed_version': '1',
            'municipality_name': 'Williamson County and Cities Health District)',
            'municipality_url': 'https://www.wcchd.org/healthy_spaces/',
            'contact_email': 'eh@wilco.org',
        })

    def test_inline_strings_and_numeric_cells(self):
        info = dp._wm_read_feed_info(_xlsx([['feed_date', 'feed_version'], [20261029, 1]]))
        self.assertEqual((info['feed_date'], info['feed_version']), ('2026-10-29', '1'))
        self.assertEqual(dp._wm_read_feed_info(_xlsx([['feed_date'], ['20261029.0']]))['feed_date'],
                         '2026-10-29')

    def test_missing_or_unparseable_feed_date_raises(self):
        for rows in ([['contact_email'], ['eh@wilco.org']], [['feed_date']],
                     [['feed_date'], ['2026-10-29']], [['feed_date'], ['20261399']]):
            with self.subTest(rows=rows):
                with self.assertRaises(ValueError):
                    dp._wm_read_feed_info(_xlsx(rows))
        with self.assertRaises(zipfile.BadZipFile):
            dp._wm_read_feed_info(b'not a workbook')


class CsvEdgeTests(_Base):
    def test_bom_and_lf_variants_parse_the_same(self):
        bom = b'\xef\xbb\xbf'
        variant = _zip(businesses=bom + _fixture('businesses.csv').replace(b'\r\n', b'\n'),
                       inspections=bom + _fixture('inspections.csv').replace(b'\r\n', b'\n'))
        records, _, _ = _fetch(zip_bytes=variant)
        base, _, _ = _fetch()
        self.assertEqual(records, base)

    def test_unknown_result_ships_with_the_score_and_a_warning(self):
        extra = _fixture('inspections.csv') + b'RR-679,20260101,88,Reinspection,Complaint\r\n'
        with self.assertLogs('dinescores', level='WARNING') as logs:
            records, _, _ = _fetch(zip_bytes=_zip(inspections=extra))
        r = _by_id(records)['wcchd_RR-679_20260101']
        self.assertEqual((r['original_score'], r['risk_score'], r['results'], r['inspection_type']),
                         (88, 88, 'Reinspection', 'Complaint'))
        self.assertTrue(any('Reinspection' in line for line in logs.output))

    def test_scores_are_clamped_to_0_100(self):
        extra = _fixture('inspections.csv') + b'RR-679,20260102,105,Pass,Routine\r\n'
        r = _by_id(_fetch(zip_bytes=_zip(inspections=extra))[0])['wcchd_RR-679_20260102']
        self.assertEqual((r['original_score'], r['risk_score']), (105, 100))

    def test_permit_codes_decide_the_scope(self):
        businesses = _csv(BUS_HDR, [
            '2026-1-TFP,Fair Funnel Cakes,100 Fairground Rd,TAYLOR,TX',
            '2026-2-SAN,Septic Job,200 Ranch Rd,GEORGETOWN,TX',
            '2026-3-FFEC,Shared Kitchen Bakery,300 Main St,HUTTO,TX',
            '2026-4-FFES-ISD,Hill Elementary,400 School Rd,LEANDER,TX',
            'RRS-9,PINE ELEMENTARY-Aramark,500 Pine Dr,ROUND ROCK,TX',
            'LES-3-ISD,OAK MIDDLE SCHOOL,600 Oak Dr,LEANDER,TX',
            'HUS-6-SFEC,COTTONWOOD ELEMENTARY-SFE,700 Limmer Loop,HUTTO,TX',
            '2026-5-MF,Taco Truck LLC,800 Commissary Ln,ROUND ROCK,TX',
            '2024-1328-UNP,Fresh Donuts,900 Donut Dr,JARRELL,TX',
            'CP-00170-FFEC,CHASHNEE N\' CREAM AT CASPERS KITCHEN,2051 CYPRESS CREEK RD,CEDAR PARK,TX',
        ])
        permits = [row.split(',')[0] for row in businesses.decode().split('\r\n')[1:-1]]
        inspections = _csv(INS_HDR, [f'{p},20260601,90,Pass,Routine' for p in permits])
        records, _, _ = _fetch(zip_bytes=_zip(businesses=businesses, inspections=inspections))
        self.assertEqual(sorted(r['source_id'] for r in records),
                         ['wcchd_2024-1328-UNP_20260601', 'wcchd_2026-3-FFEC_20260601',
                          'wcchd_CP-00170-FFEC_20260601'])
        self.assertIsNone(dp._wm_out_of_scope('RR-1410'))
        self.assertIsNone(dp._wm_out_of_scope('2026-1214-FFE'))
        self.assertEqual(dp._wm_out_of_scope('2025-322-MF'), 'mobile unit')
        self.assertEqual(dp._wm_out_of_scope('2026-1210-TFP'), 'temporary event booth')
        self.assertEqual(dp._wm_out_of_scope('TAS-11-ISD'), 'school kitchen')
        self.assertEqual(dp._wm_out_of_scope('2022-1722-FFES'), 'school kitchen')

    def test_test_permits_nameless_rows_and_unplaceable_cities_are_dropped(self):
        businesses = _csv(BUS_HDR, [
            '2025-1412-FFE,TEST,355 TEXAS AVE,ROUND ROCK,TX',
            '2021-2578-FFER,Test Project Name,"355 TEXAS AVE, Apt 1232",ROUND ROCK,TX',
            '2026-296-FFE,TEST Preopen Assignment,"355 Texas Ave, Entrance C",Round Rock,TX',
            "2026-7-FFE,Testa's Pizza,1 Main St,ROUND ROCK,TX",
            '2026-8-FFE,,2 Main St,ROUND ROCK,TX',
            '2026-9-FFE,Bagdad Grill,3 Main St,301 w bagdad ave,TX',
            '2026-10-FFE,TEST KITCHEN AUSTIN,4 Main St,ROUND ROCK,TX',
        ])
        permits = [row.split(',')[0] for row in businesses.decode().split('\r\n')[1:-1]]
        inspections = _csv(INS_HDR, [f'{p},20260601,90,Pass,Routine' for p in permits])
        with self.assertLogs('dinescores', level='WARNING') as logs:
            records, _, _ = _fetch(zip_bytes=_zip(businesses=businesses, inspections=inspections))
        self.assertEqual([r['name'] for r in records], ['Test Kitchen Austin', "Testa's Pizza"])
        text = '\n'.join(logs.output)
        self.assertIn('1 no name', text)
        self.assertIn('1 no usable city', text)


if __name__ == '__main__':
    unittest.main()
