"""The 2026-11-27 sources: Hawaii statewide, Summit County OH and Placer
County CA on MyHealthDepartment, Platte County MO on Paragon. No network:
rows are shaped like the ones sampled through the relay on 2026-11-27."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.dirname(__file__))

import dinescores_pipeline as dp  # noqa: E402
import jev_reader as jr  # noqa: E402
from test_mhd_buckets import FakeSearch, _run, _row  # noqa: E402

NEW = ('soh', 'scph', 'pchd')
DAY = '2026-11-20 to 2026-11-20'


def _hawaii_page(color, lines=()):
    obs = '<br /><br />'.join(lines)
    return (f'<html><title>Inspection</title><div>Placard Issued: <strong style="color: {color}; '
            f'filter: brightness(150%);">{color}</strong> <strong></strong></div>'
            f'<h3 class="observations-header">Observations &amp; Corrective Actions</h3>'
            f'<p class="observations-text">{obs}</p></html>')


class ConfigTests(unittest.TestCase):
    def test_slugs_and_group(self):
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['hawaii'], ['soh'])
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['akron'], ['scph'])
        self.assertEqual(dp.MHD_METRO_SLUG_GROUPS['placer'], ['pchd'])
        for key in NEW:
            cfg = dp.MHD_METRO_JURISDICTIONS[key]
            self.assertEqual(cfg['group'], dp.MHD_MIDWEEK_GROUP, key)
            self.assertEqual(cfg['score_scale'], 'none', key)
        self.assertEqual(dp.MHD_METRO_JURISDICTIONS['soh']['state'], 'HI')

    def test_workflow_lists(self):
        wf = open(os.path.join(os.path.dirname(__file__), '..', '.github', 'workflows',
                               'refresh-data.yml')).read()
        midweek = next(l for l in wf.splitlines() if l.strip().startswith('MIDWEEK='))
        sunday = next(l for l in wf.splitlines() if l.strip().startswith('SUNDAY='))
        for slug in ('hawaii', 'akron', 'placer'):
            self.assertIn(f' {slug}', midweek)
        self.assertIn(' platte ', sunday)

    def _kept(self, key, rows):
        cfg = dp.MHD_METRO_JURISDICTIONS[key]
        progs = {p.lower() for p in cfg.get('programs', [])}
        types = {t.lower() for t in cfg.get('inspection_types', [])}
        allow = {p.lower() for p in cfg.get('purpose_allow', [])}
        ex = dp._mhd_exclude_patterns(cfg)
        return [r['inspectionID'] for r in rows
                if not dp._mhd_row_rejection(r, progs, (), ex, types, allow)]

    def test_hawaii_row_filters(self):
        rows = []
        for iid, purpose, ptype in (('R', 'Routine', 'Restaurant - large - Risk Category 1'),
                                    ('F', 'Follow-up', 'Market - small - Risk Category 2'),
                                    ('C', 'Complaint', 'Service Area No Food Preparation'),
                                    ('PRE', 'Pre-Op', 'Restaurant - small - Risk Category 2'),
                                    ('OTH', 'Other', 'Restaurant - small - Risk Category 2'),
                                    ('MOB', 'Routine', 'Mobile Unit - Risk Category 3'),
                                    ('SUP', 'Routine', 'Support Kitchen + Mobile - Multi Risk 4'),
                                    ('MFG', 'Routine', 'Food Manufacturer - small - Risk Category 2 ')):
            r = _row(iid, program='Food', purpose=purpose, state='HI', city='Honolulu')
            r['permitType'] = ptype
            rows.append(r)
        self.assertEqual(self._kept('soh', rows), ['R', 'F', 'C'])

    def test_summit_row_filters(self):
        rows = []
        for iid, prog, purpose, ptype in (
                ('S', 'Food', 'Standard,CCP,Complaint', 'Commercial FSO < 25000 sq. ft.'),
                ('FU', 'Food', 'Follow-Up', 'Commercial RFE < 25000 sq. ft.'),
                ('CON', 'Food', 'Consultation', 'Commercial FSO < 25000 sq. ft.'),
                ('VEND', 'Food', 'Standard', 'Vending'),
                ('POOL', 'Recreational Water', 'Standard', 'Pool'),
                ('SCH', 'School', 'Standard', 'Public')):
            r = _row(iid, program=prog, purpose=purpose, state='OH', city='Akron')
            r['permitType'] = ptype
            rows.append(r)
        self.assertEqual(self._kept('scph', rows), ['S', 'FU'])

    def test_placer_row_filters(self):
        rows = []
        for iid, itype, purpose, ptype in (
                ('R', 'Retail Food Facility', 'Routine', 'Restaurant: 0 - 49 Seats'),
                ('MKT', 'Retail Food Facility', 'Follow-up', 'Market - With Food Prep < 5000 Sq Ft'),
                ('MFF', 'Mobile Food Facilities', 'Routine', 'Mobile Food Facility - Full Prep'),
                ('SCH', 'Retail Food Facility', 'Routine', 'School Cafeteria - Satellite'),
                ('BANK', 'Retail Food Facility', 'Routine', 'Food Bank'),
                ('PRE', 'Retail Food Facility', 'Pre-Licensing', 'Restaurant: 0 - 49 Seats')):
            r = _row(iid, program='Retail Food', purpose=purpose, state='CA', city='Roseville')
            r['inspectionType'], r['permitType'] = itype, ptype
            rows.append(r)
        self.assertEqual(self._kept('pchd', rows), ['R', 'MKT'])


class PlacardTests(unittest.TestCase):
    def setUp(self):
        dp._MHD_PAGE_PLACARDS.clear()

    def _fetch(self, key, rows, pages):
        """Run the jurisdiction with detail pages served from `pages`
        (inspectionID -> html) through the real page reader."""
        cfg = dp.MHD_METRO_JURISDICTIONS[key]
        fake = FakeSearch({(DAY, '', ''): rows})

        class Resp:
            def __init__(self, text):
                self.status_code, self.text, self.encoding = 200, text, None

        def fake_bulk(path, records, trust_official_score=True, label=None, **kw):
            for rec in records:
                session = mock.Mock()
                session.get.return_value = Resp(pages[rec['source_id']])
                texts = dp._mhd_fetch_inspection_detail(session, path, rec['source_id'])
                violations = [dp.classify_violation(t) for t in texts]
                rec['violations'] = [list(v) for v in violations]
                rec['risk_score'] = dp.calc_risk_score(violations)[0]
            return len(records), []
        with mock.patch.object(dp, '_mhd_fetch_details_bulk', side_effect=fake_bulk), \
             mock.patch.object(dp, '_mhd_is_inspection_page', return_value=True):
            return {r['source_id']: r for r in _run(key, cfg, fake, fetch_violations=True,
                                                    since_date='2026-11-20', until_date='2026-11-20')}

    def _hawaii_rows(self, *iids):
        return [_row(i, name=f'Cafe {i}', addr=f'{n} Kapiolani Blvd', program='Food', state='HI',
                     city='Honolulu', date='2026-11-20T00:00:00.000Z', score=0)
                for n, i in enumerate(iids, 1)]

    def test_hawaii_placard_from_the_page_caps_the_score(self):
        one = ['10: 11-50-72(c) Handwashing sink did not have paper towels.']
        pages = {'G': _hawaii_page('Green', one), 'Y': _hawaii_page('Yellow', one),
                 'R': _hawaii_page('Red', one), 'CLEAN': _hawaii_page('Green')}
        recs = self._fetch('soh', self._hawaii_rows('G', 'Y', 'R', 'CLEAN'), pages)
        lines = dp.calc_risk_score([dp.classify_violation(one[0])])[0]
        self.assertGreater(lines, 79)
        self.assertEqual(recs['G']['risk_score'], lines)
        self.assertEqual(recs['Y']['risk_score'], 79)
        self.assertEqual(recs['R']['risk_score'], 65)
        self.assertEqual(recs['CLEAN']['risk_score'], 100)
        self.assertEqual(recs['R']['results'], 'Red placard')
        self.assertEqual(recs['G']['results'], 'Green placard')
        for r in recs.values():
            self.assertNotIn('_placard', r)

    def test_a_cap_only_lowers(self):
        lines = [f'{n}: 11-50-32(c) Raw chicken stored above ready-to-eat salad in cooler {n}.'
                 for n in range(1, 9)]
        recs = self._fetch('soh', self._hawaii_rows('Y'), {'Y': _hawaii_page('Yellow', lines)})
        self.assertEqual(recs['Y']['risk_score'],
                         dp.calc_risk_score([dp.classify_violation(t) for t in lines])[0])
        self.assertLess(recs['Y']['risk_score'], 79)

    def test_pages_without_a_placard_raise(self):
        rows = self._hawaii_rows(*[f'N{n}' for n in range(25)])
        pages = {r['inspectionID']: _hawaii_page('Green').replace('Placard Issued:', 'Grade:')
                 for r in rows}
        with self.assertRaises(RuntimeError):
            self._fetch('soh', rows, pages)

    def test_placer_placard_from_the_row(self):
        rows = []
        for n, (iid, outcome) in enumerate((('G', 'Green Placard'), ('Y', 'Yellow Placard'),
                                            ('R', 'Red Placard'), ('B', '')), 1):
            r = _row(iid, name=f'Cafe {iid}', addr=f'{n} Vernon St', program='Retail Food', state='CA',
                     city='Roseville', date='2026-11-20T00:00:00.000Z', score=0)
            r['inspectionType'], r['permitType'] = 'Retail Food Facility', 'Restaurant: 0 - 49 Seats'
            r['InspectionOutcome'] = outcome
            rows.append(r)
        page = ('<h3>Observations &amp; Corrective Actions</h3><p class="observations-text">'
                '14: Observed stuck on food residue on clean containers.</p>')
        recs = self._fetch('pchd', rows, {i: page for i in 'GYRB'})
        self.assertEqual(recs['Y']['risk_score'], 79)
        self.assertEqual(recs['R']['risk_score'], 65)
        self.assertEqual(recs['R']['results'], 'Red placard')
        self.assertGreater(recs['G']['risk_score'], 79)
        self.assertEqual(recs['B']['risk_score'], recs['G']['risk_score'], 'no placard, no cap')

    def test_only_the_placard_sources_carry_caps(self):
        on = sorted(k for k, c in dp.MHD_METRO_JURISDICTIONS.items() if c.get('placard_caps'))
        self.assertEqual(on, ['pchd', 'soh'])
        self.assertLess(dp.MHD_PLACARD_CAPS['red'], 70, 'a closure must read as an F')
        self.assertLess(dp.MHD_PLACARD_CAPS['yellow'], 80)


class HawaiiCoordinateTests(unittest.TestCase):
    def test_hawaii_box(self):
        self.assertTrue(dp.coords_plausible(21.3069, -157.8583, 'HI'))      # Honolulu
        self.assertTrue(dp.coords_plausible(19.6400, -155.9969, 'Hawaii'))  # Kailua-Kona
        self.assertTrue(dp.coords_plausible(22.0964, -159.5261, 'HI'))      # Kauai
        self.assertFalse(dp.coords_plausible(32.78, -96.80, 'HI'), 'a Dallas pin for a Hawaii record')
        self.assertFalse(dp.coords_plausible(21.3069, -157.8583, 'TX'))
        self.assertFalse(dp.coords_plausible(21.3069, -157.8583, ''))

    def test_frontend_mirror(self):
        js = open(os.path.join(os.path.dirname(__file__), '..', 'src', 'grading.js')).read()
        self.assertIn('HI: [18.80, 22.35, -160.40, -154.70]', js)
        self.assertEqual(dp.OUTSIDE_CONUS_BOUNDS['HI'], (18.80, 22.35, -160.40, -154.70))


class PlatteTests(unittest.TestCase):
    def test_config_and_policy(self):
        cfg = dp.IO_PORTALS['platte']
        self.assertEqual(cfg['db'], 'mo/platteplattecity/inspect.nsf')
        self.assertEqual(cfg['state'], 'MO')
        self.assertEqual(cfg['city_map']['Parkvilleq'], 'Parkville')
        self.assertIn(cfg['source'], jr.SOURCE_TIER_SOURCES)
        self.assertIn(cfg['source'], jr.SOURCE_TIER_SCORE_SOURCES)

    def test_dispatch(self):
        with mock.patch.object(dp, '_fetch_paragon', return_value=[]) as f:
            dp.fetch_platte(since_date='2026-11-01')
        f.assert_called_once_with('platte', '2026-11-01', None)


if __name__ == '__main__':
    unittest.main()
