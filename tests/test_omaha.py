"""Tests for the Omaha / Douglas County, NE fetcher: the stdlib PDF text
reader, the i2g inspection-report parser, the P/PF/C severity
reconciliation and the record dict. No network: the ArcGIS query and the
PDF download are mocked with the fixtures under tests/fixtures/omaha/."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import dinescores_pipeline as dp  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'omaha')
PDF_URL = 'https://douglas.i2g20.com/inspections/food/view-or-download-inspection-details-for-api/{}'


def _pdf(insp_id):
    with open(os.path.join(FIXTURES, f'inspection_{insp_id}.pdf'), 'rb') as f:
        return f.read()


def _feature(insp_id, date, name, address, zip_code, rating, geometry, **extra):
    attrs = {
        'inspection_id': insp_id, 'inspection_date': date, 'est_name': name,
        'est_address': address, 'address_2': None, 'est_city': 'Omaha', 'est_state': 'NE',
        'est_zip': zip_code, 'firm_number': 60450, 'rating': rating,
        'downloadDetailsUrl': PDF_URL.format(insp_id),
        # geocoder output that must never be read as a score
        'Score': 100, 'Status': 'M', 'Match_type': 'A',
    }
    attrs.update(extra)
    return {'attributes': attrs, 'geometry': geometry}


# Greenland Market: rating A, TOTAL P 0 / PF 3 / C 1, items 16, 28, 37, 55 OUT.
GREENLAND = _feature(2, '2025-12-22', 'Greenland Market', '4087 S 84th St', 68127, 'A',
                     {'x': -96.04247793128715, 'y': 41.2202701824544})
# D'Leon's Taco Rico: rating D, TOTAL P 4 / PF 6 / C 4, ten items OUT.
DLEONS = _feature(3332, '2026-10-06', "D'Leon's Taco Rico", '11920 West Center Rd', 68144, 'D',
                  {'x': -96.09928170660302, 'y': 41.23453899213572})
# Dumpling Empire follow-up: no rating, all totals 0, no geometry; the PDF
# header is laid out differently because the rating box is empty.
DUMPLING = _feature(3308, '2026-10-05', 'Dumpling Empire', '5413 S 72nd St', 68117, None, None)


class FakeResponse:
    def __init__(self, content, status=200):
        self.content = content
        self.status_code = status


def run_fetch(features, pdfs, since_date=None, limit=None):
    """fetch_omaha with the layer query and the PDF download mocked. pdfs maps
    inspection_id → bytes, a FakeResponse, or an exception to raise; a
    missing id answers 404 on both attempts."""
    calls = []

    def fake_get(url, headers=None, timeout=None, params=None):
        calls.append(url)
        insp_id = int(url.rsplit('/', 1)[1])
        answer = pdfs.get(insp_id)
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, FakeResponse):
            return answer
        if answer is None:
            return FakeResponse(b'<html>not found</html>', 404)
        return FakeResponse(answer)

    session = mock.Mock()
    session.get.side_effect = fake_get
    with mock.patch.object(dp, '_arcgis_query_all', return_value=features) as query, \
            mock.patch.object(dp.requests, 'Session', return_value=session), \
            mock.patch.object(dp, '_oma_layer_newest', return_value=''), \
            mock.patch.object(dp.time, 'sleep'):
        records = dp.fetch_omaha(since_date=since_date, limit=limit)
    return records, query, calls


class PdfReaderTests(unittest.TestCase):
    def test_rows_come_out_top_down_and_left_to_right(self):
        rows = dp._pdf_text_rows(_pdf(2))
        texts = [' '.join(t for _, t in cells) for _, _, cells in rows]
        self.assertTrue(any(t.startswith('FIRM/ADDR :') and 'Greenland Market' in t for t in texts))
        self.assertIn('TOTAL P: 0 TOTAL PF: 3 TOTAL C: 1', texts)
        ys = [y for _, y, _ in rows]
        self.assertEqual(ys, sorted(ys, reverse=True))

    def test_literal_string_escapes_and_nesting(self):
        text, end = dp._pdf_literal(rb'(Dante\'s \(Mobile\) (nested) \101\n) tail', 0)
        self.assertEqual(text, "Dante's (Mobile) (nested) A\n")
        self.assertEqual(end, len(rb'(Dante\'s \(Mobile\) (nested) \101\n)'))

    def test_text_items_follow_td_and_tm_positioning(self):
        stream = (b'BT 10 700 Td /F1 6 Tf [(Hello)] TJ ET\n'
                  b'BT 1 0 0 1 300 500 Tm (World) Tj ET\n'
                  b'BT 5 5 Td 20 20 Td <41> Tj ET')
        self.assertEqual(dp._pdf_text_items(stream),
                         [(10.0, 700.0, 'Hello'), (300.0, 500.0, 'World'), (25.0, 25.0, 'A')])


class ReportParserTests(unittest.TestCase):
    def test_greenland_market(self):
        r = dp._oma_parse_report(_pdf(2))
        self.assertEqual(r['totals'], (0, 3, 1))
        self.assertEqual(r['rating'], 'A')
        self.assertEqual(r['purpose'], 'Routine')
        self.assertEqual(r['date'], '12/22/2025')
        self.assertEqual(r['out_items'], [
            (16, 'Food-contact surfaces; cleaned & sanitized'),
            (28, 'Toxic substances properly identified, stored, & used'),
            (37, 'Food properly labeled; original container'),
            (55, 'Physical facilities installed, maintained, and clean'),
        ])

    def test_many_out_items(self):
        r = dp._oma_parse_report(_pdf(3332))
        self.assertEqual(r['totals'], (4, 6, 4))
        self.assertEqual(r['rating'], 'D')
        self.assertEqual([n for n, _ in r['out_items']], [8, 10, 15, 16, 20, 23, 28, 39, 43, 56])
        # Items marked IN / N/A / N/O never count, and the explanatory
        # paragraph printed beside item 15 is not mistaken for a title.
        self.assertEqual(dict(r['out_items'])[15], 'Food separated & protected')

    def test_follow_up_without_rating_has_a_shifted_header(self):
        r = dp._oma_parse_report(_pdf(3308))
        self.assertEqual(r['totals'], (0, 0, 0))
        self.assertIsNone(r['rating'])
        self.assertEqual(r['purpose'], 'Follow_up')
        self.assertEqual(r['date'], '10/05/2026')
        self.assertEqual(r['out_items'], [])

    def test_unreadable_pdf_raises(self):
        with self.assertRaises(ValueError):
            dp._oma_parse_report(b'%PDF-1.7\n1 0 obj << >> endobj\n%%EOF')
        with self.assertRaises(ValueError):
            dp._oma_parse_report(b'<html>login</html>')


class SeverityTests(unittest.TestCase):
    def test_seeded_from_food_code_tiers(self):
        items = [(8, 'Hands'), (10, 'Handwashing facilities'), (55, 'Facilities')]
        self.assertEqual(dp._oma_item_severities(items, (1, 1, 1)),
                         {8: 'priority', 10: 'priority_foundation', 55: 'core'})

    def test_totals_bound_the_tiers(self):
        # Greenland: no P cited, 3 PF, 1 C → the two P items become PF and
        # the lower-numbered of the two core items moves up to PF.
        items = [(16, ''), (28, ''), (37, ''), (55, '')]
        self.assertEqual(dp._oma_item_severities(items, (0, 3, 1)),
                         {16: 'priority_foundation', 28: 'priority_foundation',
                          37: 'priority_foundation', 55: 'core'})
        # Five P items against TOTAL P 4: the highest-numbered drops to PF.
        items = [(8, ''), (10, ''), (15, ''), (16, ''), (20, ''), (23, ''), (28, ''),
                 (39, ''), (43, ''), (56, '')]
        sev = dp._oma_item_severities(items, (4, 6, 4))
        self.assertEqual(sum(1 for s in sev.values() if s == 'priority'), 4)
        self.assertEqual(sev[28], 'priority_foundation')
        self.assertEqual(sev[39], 'core')
        # No core cited: the core items move up to the tier with room.
        sev = dp._oma_item_severities([(8, ''), (37, ''), (51, '')], (2, 1, 0))
        self.assertEqual(sev, {8: 'priority', 37: 'priority', 51: 'priority_foundation'})

    def test_empty(self):
        self.assertEqual(dp._oma_item_severities([], (0, 0, 0)), {})


class FetchOmahaTests(unittest.TestCase):
    def test_record_dict(self):
        records, _, _ = run_fetch([GREENLAND], {2: _pdf(2)})
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertEqual(r['name'], 'Greenland Market')
        self.assertEqual(r['address'], '4087 S 84th St')
        self.assertEqual(r['city'], 'Omaha')
        self.assertEqual(r['state'], 'NE')
        self.assertEqual(r['zip'], '68127')             # est_zip is an integer in the layer
        self.assertAlmostEqual(r['latitude'], 41.2202701824544)
        self.assertAlmostEqual(r['longitude'], -96.04247793128715)
        self.assertEqual(r['inspection_date'], '2025-12-22')
        self.assertIsNone(r['original_score'])         # a letter rating, not a number
        self.assertEqual(r['risk_score'], 93)           # 100 - 5*0 - 2*3 - 1*1
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'],
                          r['core_violations'], r['total_violations']), (0, 3, 1, 4))
        self.assertEqual(r['inspection_type'], 'Routine')
        self.assertEqual(r['results'], 'Rating A')
        self.assertEqual(r['source'], 'Douglas County Health Dept')
        self.assertEqual(r['source_url'], PDF_URL.format(2))
        self.assertEqual(r['source_id'], 'oma_2')
        self.assertEqual(r['metro'], '')
        # the OUT items, in form order, with the severity reconciled to the
        # totals (0 P cited) and the category from classify_violation
        self.assertEqual([[sev, desc] for _, sev, desc in r['violations']], [
            ['priority_foundation', 'Food-contact surfaces; cleaned & sanitized'],
            ['priority_foundation', 'Toxic substances properly identified, stored, & used'],
            ['priority_foundation', 'Food properly labeled; original container'],
            ['core', 'Physical facilities installed, maintained, and clean'],
        ])
        for cat, sev, desc in r['violations']:
            self.assertEqual(cat, dp.classify_violation(desc)[0])

    def test_score_from_the_totals_not_the_item_count(self):
        records, _, _ = run_fetch([DLEONS], {3332: _pdf(3332)})
        r = records[0]
        self.assertEqual(r['risk_score'], 64)           # 100 - 20 - 12 - 4
        self.assertEqual(r['total_violations'], 14)     # the inspector's totals ...
        self.assertEqual(len(r['violations']), 10)      # ... over ten OUT items
        self.assertEqual(r['results'], 'Rating D')
        self.assertEqual(dp.calculate_vetted_grade(r['risk_score']), 'F')

    def test_follow_up_without_rating_or_geometry(self):
        records, _, _ = run_fetch([DUMPLING], {3308: _pdf(3308)})
        r = records[0]
        self.assertEqual(r['risk_score'], 100)
        self.assertEqual(r['violations'], [])
        self.assertEqual(r['results'], '')
        self.assertEqual(r['inspection_type'], 'Follow-up')
        self.assertIsNone(r['latitude'])
        self.assertIsNone(r['longitude'])

    def test_failed_pdf_drops_the_inspection(self):
        # 404 on both attempts, a connection error, a non-PDF body and an
        # unparseable PDF each drop their inspection; the others still ship.
        pdfs = {2: _pdf(2),
                3332: dp.requests.ConnectionError('reset'),
                3308: FakeResponse(b'<html>maintenance</html>')}
        broken = _feature(9001, '2026-10-01', 'Broken PDF', '1 Main St', 68102, 'B',
                          {'x': -96.0, 'y': 41.25})
        missing = _feature(9002, '2026-10-02', 'Missing PDF', '2 Main St', 68102, 'B',
                           {'x': -96.0, 'y': 41.25})
        pdfs[9001] = b'%PDF-1.7\n1 0 obj << >> endobj\n%%EOF'
        records, _, calls = run_fetch([GREENLAND, DLEONS, DUMPLING, broken, missing], pdfs)
        self.assertEqual([r['source_id'] for r in records], ['oma_2'])
        # one retry per failed download; the unparseable PDF downloaded once
        self.assertEqual(calls.count(PDF_URL.format(3332)), 2)
        self.assertEqual(calls.count(PDF_URL.format(3308)), 2)
        self.assertEqual(calls.count(PDF_URL.format(9002)), 2)
        self.assertEqual(calls.count(PDF_URL.format(9001)), 1)

    def test_junk_ratings_and_junk_rows(self):
        feats = []
        pdfs = {}
        for i, rating in enumerate(['-', 'aA', 'NA', None, ' b ', 'F']):
            feats.append(_feature(100 + i, '2026-09-0%d' % (i + 1), f'Place {i}', f'{i} Elm St',
                                  68102, rating, {'x': -96.0, 'y': 41.25}))
            pdfs[100 + i] = _pdf(3308)                  # the PDF prints no rating either
        feats.append(_feature(200, '2026-09-09', 'test permit est name', 'test permit est add',
                              90909, 'A', None, est_city='ct test permit', est_state='st test permit'))
        feats.append(_feature(201, '2026-09-09', "AK's Treats (Itinerant Vendor)", '2327 Wycliff St',
                              55114, 'A', None, est_city='Saint Paul', est_state='MN'))
        pdfs[200] = pdfs[201] = _pdf(3308)
        records, _, calls = run_fetch(feats, pdfs)
        by_id = {r['source_id']: r for r in records}
        self.assertEqual(set(by_id), {f'oma_{100 + i}' for i in range(6)})
        self.assertEqual([by_id[f'oma_{100 + i}']['results'] for i in range(6)],
                         ['', '', '', '', 'Rating B', 'Rating F'])
        self.assertFalse(any(u.endswith('/200') or u.endswith('/201') for u in calls))

    def test_pdf_rating_backs_up_a_missing_layer_rating(self):
        feat = _feature(2, '2025-12-22', 'Greenland Market', '4087 S 84th St', 68127, None,
                        {'x': -96.0, 'y': 41.25})
        records, _, _ = run_fetch([feat], {2: _pdf(2)})
        self.assertEqual(records[0]['results'], 'Rating A')

    def test_name_address_normalisation(self):
        feat = _feature(2, '2025-12-22', '  GREENLAND   MARKET ', ' 4087  S 84TH ST', 68127, 'A',
                        {'x': -96.0, 'y': 41.25}, address_2='STE 100', est_city='OMAHA')
        records, _, _ = run_fetch([feat], {2: _pdf(2)})
        r = records[0]
        self.assertEqual(r['name'], 'Greenland Market')
        self.assertEqual(r['address'], '4087 S 84Th St Ste 100')
        self.assertEqual(r['city'], 'Omaha')
        # a mixed-case source value is kept as published
        feat = _feature(2, '2025-12-22', "D'Leon's Taco Rico", '11920 West Center Rd', 68144, 'A',
                        {'x': -96.0, 'y': 41.25}, address_2='Ste 3')
        records, _, _ = run_fetch([feat], {2: _pdf(2)})
        self.assertEqual(records[0]['name'], "D'Leon's Taco Rico")
        self.assertEqual(records[0]['address'], '11920 West Center Rd Ste 3')

    def test_zip_and_coordinates(self):
        feat = _feature(2, '2025-12-22', 'Greenland Market', '4087 S 84th St', 681271234, 'A',
                        {'x': -96.0, 'y': 41.25})
        records, _, _ = run_fetch([feat], {2: _pdf(2)})
        self.assertEqual(records[0]['zip'], '68127')
        feat = _feature(2, '2025-12-22', 'Greenland Market', '4087 S 84th St', None, 'A',
                        {'x': 0, 'y': 0})                # a null-island point is no coordinate
        records, _, _ = run_fetch([feat], {2: _pdf(2)})
        self.assertEqual(records[0]['zip'], '')
        self.assertIsNone(records[0]['latitude'])

    def test_id_stability(self):
        a, _, _ = run_fetch([GREENLAND], {2: _pdf(2)})
        b, _, _ = run_fetch([_feature(2, '2025-12-22', 'GREENLAND MARKET', '4087 S 84TH ST', 68127,
                                      'A', {'x': -96.0, 'y': 41.25})], {2: _pdf(2)})
        ida = dp.make_restaurant_id(a[0]['name'], a[0]['address'], a[0]['city'])
        idb = dp.make_restaurant_id(b[0]['name'], b[0]['address'], b[0]['city'])
        self.assertEqual(ida, idb)
        self.assertEqual(ida, dp.make_restaurant_id('Greenland Market', '4087 S 84th St', 'Omaha'))
        self.assertEqual(a[0]['source_id'], b[0]['source_id'])

    def test_window_and_limit(self):
        feats = [DUMPLING, GREENLAND, DLEONS]            # not newest-first on purpose
        pdfs = {2: _pdf(2), 3332: _pdf(3332), 3308: _pdf(3308)}
        records, query, calls = run_fetch(feats, pdfs, since_date='2026-09-01T00:00:00', limit=2)
        self.assertIn("inspection_date >= DATE '2026-09-01'", query.call_args.kwargs['where'])
        # the limit applies before the PDF phase, newest inspections first
        self.assertEqual(len(calls), 2)
        self.assertEqual([r['source_id'] for r in records], ['oma_3332', 'oma_3308'])
        records, query, _ = run_fetch(feats, pdfs)
        self.assertEqual(query.call_args.kwargs['where'], '1=1')
        self.assertEqual(len(records), 3)

    def test_same_day_pair_puts_the_rated_visit_first(self):
        routine = _feature(4, '2025-12-23', "Arby's", '8429 West Center Rd', 68124, 'A',
                           {'x': -96.0, 'y': 41.23})
        follow = _feature(3, '2025-12-23', "Arby's", '8429 West Center Rd', 68124, None,
                          {'x': -96.0, 'y': 41.23})
        records, _, _ = run_fetch([follow, routine], {4: _pdf(2), 3: _pdf(3308)})
        self.assertEqual([r['source_id'] for r in records], ['oma_4', 'oma_3'])

    def test_purpose_line_is_tidied(self):
        for raw, want in (('Routine', 'Routine'), ('Follow_up', 'Follow-up'),
                          ('Other ,', 'Other'), ('Other , Fire', 'Other, Fire'), ('', '')):
            report = {'totals': (0, 0, 0), 'rating': 'A', 'purpose': raw, 'date': '', 'out_items': []}
            with mock.patch.object(dp, '_oma_parse_report', return_value=report):
                records, _, _ = run_fetch([GREENLAND], {2: _pdf(2)})
            self.assertEqual(records[0]['inspection_type'], want)

    def test_empty_window(self):
        with mock.patch.object(dp, '_arcgis_query_all', return_value=[]), \
                mock.patch.object(dp, '_oma_layer_newest', return_value='2026-10-06') as newest:
            self.assertEqual(dp.fetch_omaha(since_date='2026-10-09'), [])
            newest.assert_called_once()


if __name__ == '__main__':
    unittest.main()
