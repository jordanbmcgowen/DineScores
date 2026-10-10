"""Orange County, CA: the newest-225 daily poll and its sidecar, the
outcome -> risk mapping, and the printable-PDF violation reader. No network:
the portal search and the PDF endpoint are replaced with captured fixtures
(tests/fixtures/orange_county/: three search pages = 60 real records from
2026-10-07/08, and three printables shrunk to their text — the page JPEG and
font files emptied, the structure tree dropped — with identical extracted
text to the originals)."""
import json
import logging
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'orange_county')
REINSPECTION_ID = 'D08A2CEF-3B41-4292-8FF3-026DA89536D9'   # The Alley Spectrum, Irvine
CLOSED_ID = '0EA22585-99C2-4104-9DD6-9B30E76DC0D8'         # ABC Liquor #3, Midway City
PASS_ID = '95D6D013-60AF-41BA-A9E3-0F2F4CB861F9'           # Shorebreak Hotel, Huntington Beach


def _pages():
    return [json.load(open(os.path.join(FIX, f'search_page_{i}.json'))) for i in range(3)]


def _pdf(name):
    with open(os.path.join(FIX, f'printable_{name}.pdf'), 'rb') as f:
        return f.read()


class _Portal:
    """Stands in for _mhd_search_page: serves fixture pages by start offset."""

    def __init__(self, pages=None, fail=False):
        self.pages = _pages() if pages is None else pages
        self.fail = fail
        self.calls = []

    def search(self, session, slug, date_range_str, start_offset, retry_count=0):
        self.calls.append((slug, date_range_str, start_offset))
        if self.fail:
            return None
        idx = start_offset // dp.MHD_PAGE_SIZE
        return self.pages[idx] if idx < len(self.pages) else []


class OrangeCountyCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.sidecar = os.path.join(self.tmp.name, 'oc.jsonl')
        self._saved = (dp.MHD_DELAY, dp.OC_PDF_DELAY, dp._MHD_PORTAL_BLOCKED,
                       dp._OC_PDF_BLOCKED, dp._FETCH_DEADLINE)
        self._log_level = dp.log.level
        dp.log.setLevel(logging.WARNING)       # the fetcher narrates every run
        dp.MHD_DELAY = 0
        dp.OC_PDF_DELAY = 0
        dp._MHD_PORTAL_BLOCKED = False
        dp._OC_PDF_BLOCKED = False
        dp._FETCH_DEADLINE = None

    def tearDown(self):
        (dp.MHD_DELAY, dp.OC_PDF_DELAY, dp._MHD_PORTAL_BLOCKED,
         dp._OC_PDF_BLOCKED, dp._FETCH_DEADLINE) = self._saved
        dp.log.setLevel(self._log_level)
        self.tmp.cleanup()

    def run_fetch(self, portal=None, pdfs=None, geocode=None, **kw):
        portal = portal or _Portal()
        pdfs = pdfs or {}
        fetched = []

        def fake_pdf(session, inspection_id, retry_count=0):
            fetched.append(inspection_id)
            return pdfs.get(inspection_id)
        with mock.patch.object(dp, '_mhd_search_page', portal.search), \
                mock.patch.object(dp, '_oc_fetch_printable', fake_pdf), \
                mock.patch.object(dp, '_mhd_session', lambda: object()):
            out = dp.fetch_orange_county(sidecar_path=self.sidecar, geocode=geocode, **kw)
        return out, portal, fetched

    def sidecar_rows(self):
        with open(self.sidecar) as f:
            return {r['source_id']: r for r in (json.loads(l) for l in f if l.strip())}


class PollTests(OrangeCountyCase):
    def test_poll_is_the_empty_query_paged_until_a_short_page(self):
        out, portal, _ = self.run_fetch()
        # Pages of 25, 25, 10: the short third page ends the poll (no 4th call).
        self.assertEqual([c[2] for c in portal.calls], [0, 25, 50])
        for slug, date_range, _ in portal.calls:
            self.assertEqual(slug, 'orange-county')
            self.assertEqual(date_range, '')      # the date filter is ignored by the portal
        self.assertEqual(len(out), 60)
        self.assertEqual(len(self.sidecar_rows()), 60)
        self.assertEqual(dp._OC_LAST_POLL['status'], 'ok')
        self.assertEqual(dp._OC_LAST_POLL['new'], 60)
        self.assertEqual(dp._oc_group_outcome(), (None, ''))

    def test_result_mapping_and_record_shape(self):
        out, _, _ = self.run_fetch()
        by_id = {r['source_id']: r for r in out}
        self.assertEqual((by_id[REINSPECTION_ID]['risk_score'], by_id[REINSPECTION_ID]['results']),
                         (75, 'Reinspection required'))
        self.assertEqual((by_id[CLOSED_ID]['risk_score'], by_id[CLOSED_ID]['results']),
                         (50, 'Closed'))
        self.assertEqual((by_id[PASS_ID]['risk_score'], by_id[PASS_ID]['results']),
                         (90, 'Pass'))
        for r in out:
            self.assertIsNone(r['original_score'])
            self.assertEqual(r['source'], 'Orange County Health Care Agency')
            self.assertEqual(r['metro'], 'Orange County')
            self.assertEqual(r['state'], 'CA')
            self.assertIn('task=getPrintable', r['source_url'])
            self.assertIn(f"pKey={r['source_id']}", r['source_url'])
            self.assertRegex(r['zip'], r'^\d{5}$')
            self.assertRegex(r['inspection_date'], r'^\d{4}-\d{2}-\d{2}$')
            self.assertFalse([k for k in r if k.startswith('_')], 'private sidecar keys leaked')
            self.assertEqual(r['city'], r['city'].title())      # ANAHEIM / WESTMINSTER rows
        rec = by_id[REINSPECTION_ID]
        self.assertEqual((rec['name'], rec['address'], rec['city']),
                         ('The Alley Spectrum', '507 Spectrum Center Dr', 'Irvine'))
        self.assertIn('Westminster', {r['city'] for r in out})
        self.assertNotIn('WESTMINSTER', {r['city'] for r in out})
        self.assertEqual(dp.calculate_vetted_grade(90), 'A')
        self.assertEqual(dp.calculate_vetted_grade(75), 'C')
        self.assertEqual(dp.calculate_vetted_grade(50), 'F')

    def test_caret_marker_is_stripped_from_names(self):
        raw = dict(_pages()[0][0], establishmentName='DISNEYLAND^', addressLine1='1313 DISNEYLAND DR')
        rec = dp._oc_build_record(raw)
        self.assertEqual((rec['name'], rec['address']), ('Disneyland', '1313 Disneyland Dr'))
        self.assertEqual(dp._oc_build_record(dict(raw, establishmentName='Ami Sushi ^'))['name'], 'Ami Sushi')
        self.assertIsNone(dp._oc_build_record(dict(raw, establishmentName='^')))

    def test_every_published_outcome_is_mapped(self):
        self.assertEqual(dp._oc_result_fields('CLOSED-OWNER INITIATED'), (50, 'Closed (owner initiated)'))
        self.assertEqual(dp._oc_result_fields('pass'), (90, 'Pass'))
        # Anything new keeps the neutral risk and its own text, never a PASS.
        self.assertEqual(dp._oc_result_fields('PENDING REVIEW'), (70, 'Pending Review'))
        self.assertEqual(dp._oc_result_fields(''), (70, ''))

    def test_pdf_violations_fill_the_record_but_never_its_grade(self):
        out, _, fetched = self.run_fetch(pdfs={REINSPECTION_ID: _pdf('reinspection')})
        # Every new inspection had its PDF attempted once.
        self.assertEqual(sorted(fetched), sorted(r['source_id'] for r in out))
        by_id = {r['source_id']: r for r in out}
        rec = by_id[REINSPECTION_ID]
        self.assertEqual(rec['total_violations'], 8)
        self.assertEqual((rec['priority_violations'], rec['priority_foundation_violations'],
                          rec['core_violations']), (1, 1, 6))
        self.assertEqual(rec['risk_score'], 75)      # the county's outcome, not 100-5-2-6
        self.assertTrue(rec['violations'][0][2].startswith(
            '8. Proper hot and cold holding temperatures (MAJ): The following food items'))
        # The others had no readable PDF: they still ship, with their official
        # result and an empty violation list.
        other = by_id[PASS_ID]
        self.assertEqual((other['risk_score'], other['results'], other['violations']), (90, 'Pass', []))
        rows = self.sidecar_rows()
        self.assertEqual(rows[REINSPECTION_ID]['_pdf'], 'ok')
        self.assertEqual(rows[PASS_ID]['_pdf'], 'failed')

    def test_failed_pdf_is_retried_next_poll_and_a_read_one_is_not(self):
        self.run_fetch()
        _, _, fetched = self.run_fetch(pdfs={REINSPECTION_ID: _pdf('reinspection'),
                                             PASS_ID: _pdf('pass')})
        self.assertEqual(len(fetched), 60)           # every failed one retried
        rows = self.sidecar_rows()
        self.assertEqual(rows[REINSPECTION_ID]['total_violations'], 8)
        self.assertEqual(rows[PASS_ID]['total_violations'], 3)
        _, _, fetched = self.run_fetch()
        self.assertEqual(len(fetched), 58)           # the two read ones are done
        self.assertNotIn(REINSPECTION_ID, fetched)

    def test_second_poll_dedupes_and_keeps_what_the_first_earned(self):
        calls = []

        def geocode(records):
            calls.append(len(records))
            for r in records:
                r['latitude'], r['longitude'] = 33.7, -117.8
        out1, _, _ = self.run_fetch(geocode=geocode)
        rows1 = self.sidecar_rows()
        with mock.patch.object(dp, 'datetime', wraps=dp.datetime) as fake_dt:
            fake_dt.now.return_value = dp.datetime(2030, 1, 1)
            out2, _, _ = self.run_fetch(geocode=geocode)
        rows2 = self.sidecar_rows()
        self.assertEqual(len(out1), len(out2), 60)
        self.assertEqual(len(rows2), 60)
        self.assertEqual(calls, [60])                # geocoded once, not again
        for sid, r in rows2.items():
            self.assertEqual((r['latitude'], r['longitude']), (33.7, -117.8))
            self.assertEqual(r['_polled'], rows1[sid]['_polled'])   # first-seen date kept
        self.assertTrue(all(r['latitude'] == 33.7 for r in out2))

    def test_prune_window_and_since_date_filter(self):
        pages = _pages()
        template = pages[0][0]
        newest = max(r['inspectionDate'][:10] for p in pages for r in p)   # 2026-10-08

        def seeded(days_ago, sid):
            raw = dict(template, inspectionID=sid,
                       inspectionDate=(dp.datetime.strptime(newest, '%Y-%m-%d')
                                       - dp.timedelta(days=days_ago)).strftime('%Y-%m-%dT00:00:00.000Z'))
            rec = dp._oc_build_record(raw)
            rec.update(_polled='2026-10-01', _pdf='ok', latitude=33.0, longitude=-117.0)
            return rec
        old, kept = seeded(15, 'OLD-ONE'), seeded(5, 'KEPT-ONE')
        dp._oc_write_sidecar(self.sidecar, {old['source_id']: old, kept['source_id']: kept})

        out, _, _ = self.run_fetch(pdfs={})
        ids = {r['source_id'] for r in out}
        self.assertNotIn('OLD-ONE', ids)             # older than ten days before the newest
        self.assertIn('KEPT-ONE', ids)
        self.assertEqual(len(out), 61)
        self.assertEqual(dp._OC_LAST_POLL['pruned'], 1)
        # The returned list is newest first.
        dates = [r['inspection_date'] for r in out]
        self.assertEqual(dates, sorted(dates, reverse=True))

        out, _, _ = self.run_fetch(since_date=newest)
        self.assertEqual(len(out), 59)               # 59 of the 60 fixtures are on the newest day
        out, _, _ = self.run_fetch(since_date=f'{newest[:8]}07T00:00:00')
        self.assertEqual(len(out), 60)
        out, _, _ = self.run_fetch(since_date='2026-01-01')
        self.assertEqual(len(out), 61)

    def test_failed_poll_still_ships_the_sidecar_and_reports_an_error(self):
        self.run_fetch()
        out, portal, fetched = self.run_fetch(portal=_Portal(fail=True),
                                              pdfs={REINSPECTION_ID: _pdf('reinspection')})
        self.assertEqual(len(portal.calls), 1)
        self.assertEqual(len(out), 60)               # yesterday's inspections, not nothing
        self.assertEqual(fetched, [])                # no PDF phase while the portal fails
        self.assertEqual(dp._OC_LAST_POLL['status'], 'error')
        status, detail = dp._oc_group_outcome()
        self.assertEqual(status, 'error')
        self.assertIn("shipped the sidecar's 60 inspections", detail)
        self.assertEqual(len(self.sidecar_rows()), 60)

    def test_blocked_portal_skips_the_poll_entirely(self):
        self.run_fetch()
        dp._MHD_PORTAL_BLOCKED = True
        out, portal, fetched = self.run_fetch()
        self.assertEqual(portal.calls, [])
        self.assertEqual(fetched, [])
        self.assertEqual(len(out), 60)
        self.assertEqual(dp._oc_group_outcome()[0], 'blocked')

    def test_deadline_before_the_poll_is_a_skip(self):
        self.run_fetch()
        dp._FETCH_DEADLINE = 1.0                      # long past
        out, portal, _ = self.run_fetch()
        self.assertEqual(portal.calls, [])
        self.assertEqual(len(out), 60)
        self.assertEqual(dp._oc_group_outcome()[0], 'skipped')

    def test_limit_bounds_pages_and_records(self):
        out, portal, _ = self.run_fetch(limit=25)
        self.assertEqual(len(portal.calls), 1)
        self.assertEqual(len(out), 25)

    def test_no_violations_flag_skips_the_pdf_phase(self):
        _, _, fetched = self.run_fetch(fetch_violations=False,
                                       pdfs={REINSPECTION_ID: _pdf('reinspection')})
        self.assertEqual(fetched, [])
        self.assertTrue(all(r['_pdf'] == 'pending' for r in self.sidecar_rows().values()))

    def test_corrupt_sidecar_line_is_skipped_not_fatal(self):
        self.run_fetch()
        with open(self.sidecar, 'a') as f:
            f.write('{not json\n')
        out, _, _ = self.run_fetch()
        self.assertEqual(len(out), 60)

    def test_orange_county_is_a_portal_source(self):
        self.assertTrue(dp._wants_portal_sources(['orangecounty']))
        self.assertTrue(dp._wants_portal_sources(['anaheim']))
        self.assertFalse(dp._wants_portal_sources(['chicago']))


class PrintableParserTests(unittest.TestCase):
    def test_reinspection_report_with_eight_items_across_a_page_break(self):
        parsed = dp._oc_parse_printable(_pdf('reinspection'))
        self.assertEqual((parsed['result'], parsed['inspection_type'], parsed['inspection_date']),
                         ('REINSPECTION', 'Routine Inspection', '10/08/2026'))
        self.assertEqual(parsed['pages'], 6)
        v = parsed['violations']
        self.assertEqual([x['code'] for x in v], ['8', '15', '35', '36', '40', '41', '45', '53'])
        self.assertEqual([x['tag'] for x in v], ['MAJ', 'MIN', 'OUT', 'OUT', 'OUT', 'OUT', 'OUT', 'OUT'])
        self.assertEqual(v[0]['title'], 'Proper hot and cold holding temperatures')
        self.assertEqual(v[3]['title'], 'Equipment/Utensils - approved; installed; good repair; capacity')
        self.assertTrue(v[0]['observation'].startswith(
            'The following food items were measured at the temperatures noted below: 1) Creme brulee'))
        self.assertIn('Corrective Action (COS): The person in charge voluntarily discarded the item.',
                      v[0]['observation'])
        self.assertEqual(v[0]['description'],
                         'Potentially hazardous foods shall be held at or below 41°F or at or above '
                         '135°F. (113996, 113998, 114037)')
        self.assertEqual(v[0]['flags'], ['COS'])
        # Item 45 starts on page 3 and its observation continues on page 4,
        # past the repeated page header, without picking the header up.
        self.assertEqual(v[6]['observation'],
                         'Observed the following: 1) an accumulation of grime and debris on all '
                         'floor sinks in the facility. 2) an accumulation of debris on the floor in '
                         'the back area under all sinks, table and racks. Remove the accumulation '
                         'and maintain them in a clean condition.')
        self.assertTrue(v[6]['description'].startswith('The walls / ceilings shall have durable'))
        self.assertEqual(v[7]['title'], 'Voluntary Condemnation & Destruction (VC&D)')
        self.assertEqual(parsed['general_comments'],
                         'The purpose of this visit was to conduct a routine inspection. '
                         'Food Safety Manager: J Liu | Exp: 04/21/2030')
        text = json.dumps(parsed)
        self.assertNotIn('\\ufffd', text)
        self.assertNotIn('Establishment Name', text)
        self.assertNotIn('Fahrenheit', text)

    def test_violation_rows_take_the_county_tier(self):
        rows = dp._oc_violation_rows(dp._oc_parse_printable(_pdf('reinspection')))
        self.assertEqual(len(rows), 8)
        self.assertEqual([sev for _, sev, _ in rows],
                         ['priority', 'priority_foundation'] + ['core'] * 6)
        self.assertEqual(rows[0][0], 'temperature_control')
        self.assertTrue(rows[0][2].startswith(
            '8. Proper hot and cold holding temperatures (MAJ): The following food items'))
        self.assertTrue(rows[1][2].startswith(
            '15. Food contact surfaces: clean and sanitized (MIN): Observed an accumulation of grime'))
        self.assertTrue(all(len(d) <= 500 for _, _, d in rows))
        _, pv, pfv, cv = dp.calc_risk_score(rows)
        self.assertEqual((pv, pfv, cv), (1, 1, 6))

    def test_closure_reinspection_has_no_itemized_violations(self):
        parsed = dp._oc_parse_printable(_pdf('closed'))
        self.assertEqual((parsed['result'], parsed['inspection_type']), ('CLOSED', 'Reinspection'))
        self.assertEqual(parsed['violations'], [])
        self.assertIn('rodent droppings in the candy boxes', parsed['general_comments'])
        self.assertTrue(parsed['general_comments'].startswith(
            'This inspection is at the request of S. Aldakhil, owner in response to a permit suspension'))
        self.assertEqual(dp._oc_violation_rows(parsed), [])

    def test_pass_report_with_three_items(self):
        parsed = dp._oc_parse_printable(_pdf('pass'))
        self.assertEqual(parsed['result'], 'PASS')
        v = parsed['violations']
        self.assertEqual([(x['code'], x['tag']) for x in v], [('10', 'MIN'), ('35', 'OUT'), ('44', 'OUT')])
        # Here the observation follows "Inspector Comments:" on the same line.
        self.assertTrue(v[0]['observation'].startswith(
            'Observed numerous trays of pasta and chicken cooling at ambient temperature.'))
        self.assertTrue(v[2]['observation'].startswith("1) Observed eployees cell phone"))
        self.assertIn('(114002, 114002.1)', v[0]['description'])

    def test_not_a_pdf_is_none(self):
        self.assertIsNone(dp._oc_parse_printable(b''))
        self.assertIsNone(dp._oc_parse_printable(b'<html>Access denied</html>'))
        self.assertIsNone(dp._oc_parse_printable(None))

    def test_tounicode_cmap_forms(self):
        cmap = (b'1 begincodespacerange <0000> <FFFF> endcodespacerange\n'
                b'2 beginbfchar <0001> <0041> <0002> <00200042> endbfchar\n'
                b'2 beginbfrange <0010> <0012> <0061> <0020> <0021> [<0078> <0079>] endbfrange')
        m = dp._pdf_tounicode(cmap)
        self.assertEqual(m[1], 'A')
        self.assertEqual(m[2], ' B')
        self.assertEqual((m[0x10], m[0x11], m[0x12]), ('a', 'b', 'c'))
        self.assertEqual((m[0x20], m[0x21]), ('x', 'y'))


if __name__ == '__main__':
    unittest.main()
