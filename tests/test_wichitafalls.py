"""Tests for Wichita Falls TX (fetch_wichitafalls), the fourth tenant of the
Hedgerow "Hedgehog" portal family (see tests/test_hedgerow.py, whose fake
portal and fixtures-loading helpers this file reuses). No network: the
fixtures under tests/fixtures/wichitafalls/ are trimmed copies of the live
responses captured on 2026-11-22/23 (facility rows chosen to cover every
name-suffix, scope and address branch; six facilities' real inspection
lists; ten real checklists). Observation text naming people was replaced
with neutral wording.

What is new here and tested below: the permit class read from the name
suffix ('name_class_re', 'skip_types', 'skip_names'), the tier read from the
checklist category's '(P)/(PF)/(C)' when the citation has no severity
('tier_from_category', 'category_tiers', _HhTierError), the split infraction
counts ('count_fields'), the known-town address split ('cities') and the two
placeholders. The other three portals carry none of these keys, so their
behaviour is unchanged (tests/test_hedgerow.py runs unedited)."""
import json
import os
import sys
import unittest
from collections import defaultdict
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_hedgerow as th  # noqa: E402

dp = th.dp
jr = th.jr
FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures', 'wichitafalls')
WF = dp.HH_PORTALS['wichitafalls']
WF_SRC = 'Wichita Falls Environmental Health'

# facility ids in the fixtures
TACO_CASA = '3b325b5a-e53d-4c33-ba38-eee651c7d141'      # '... - Retail Food Establishment (Medium)'
LEGACY_HS = '94e3e696-2e1d-4cdb-9396-92c7839c9cb3'      # school cafeteria
WALMART_BAKERY = '7e8dc3ec-09ca-4106-ae6d-7bb53a8bcfaf'
WALMART_DELI = '53dac4db-ceeb-44b7-b67f-3c4d0f5dd143'
PUMP_CITY = 'c14b10b0-a360-49d8-bdb7-8c20788bd3e8'      # 'ELECTRA TX'
RED_BARN = '9caf01db-2f35-434f-83bd-cdbffe220ee3'       # no state in the address
CATERER = '567654d7-9281-4e41-9e60-a2846423231f'
CITY_CAFE = '3e665c87-d55d-408c-97fe-5744ed8090e4'      # 'IOWA PARK TX'
MPEC_NE = 'ec781e2a-64c8-4387-a4e0-3104cda8a580'        # '... - NE Concession Stand (Concession #2)'
DOLLAR_TREE = '75ff1bdf-3722-4e06-95a6-aa7d83cda0b1'    # '106 S Red River, Expressway Burkburnett TX'
TOMMYS = '5e8bbc1c-6042-48b4-95c9-e3c76d9f0911'
BRAUMS = '836bc5c2-e956-4e99-9b3c-84af144b3357'
KATHYS = 'bad2e457-99f8-4ebd-9249-66b1b59b4f0d'
DOLLAR_SAVER = 'e2d72e4a-f8fe-4cac-a5b7-a4c010f5784e'   # '4110 Business, US-287, Iowa Park, TX 76367'
PUSH_CART = 'f392b851-1d89-4ab3-a172-faa39140367e'
# inspection ids
TACO_1119 = '816f3485-5ba7-4056-a026-1bf03a5a6c16'      # INSP-022432, 96, severity null on both citations
TOMMYS_74 = '65c55209-3a27-485e-bc56-4c4e3a6dd01c'      # INSP-021694, 74, every severity null
CITY_CAFE_95 = '0aaa1875-b79d-4f59-b877-f68433e7d0ef'   # INSP-022127, 95, dead cockroaches
PUMP_84 = 'ae716b78-23a7-4e0b-b489-4766796c94c2'        # INSP-022258, 84, rodent droppings
KATHYS_95 = '71828934-0756-41bd-815f-9b8e986a0430'      # INSP-021776, the row counts 4, the checklist shows 3
BRAUMS_98 = '3b2b183c-830c-4b0a-a393-a8c12fd1e059'      # INSP-022027, 98, flies

LISTS = {TACO_CASA: 'tacocasa', TOMMYS: 'tommys', CITY_CAFE: 'citycafe', PUMP_CITY: 'pumpcity',
         KATHYS: 'kathys', BRAUMS: 'braums'}


def _load(name):
    with open(os.path.join(FIXTURES, name)) as fh:
        return json.load(fh)


def _rows():
    return _load('facilities.json')['result']


def _checklists_for(inspections):
    """The captured checklist of every captured inspection, keyed by id."""
    out = {}
    names = {n[len('results_'):-len('.json')] for n in os.listdir(FIXTURES) if n.startswith('results_')}
    for insp_list in inspections.values():
        for insp in insp_list if isinstance(insp_list, list) else []:
            if insp['inspectionNumber'] in names:
                out[insp['id']] = _load(f"results_{insp['inspectionNumber']}.json")
    return out


def _wf_insp(iid, date, score, critical=0, noncritical=0, itype='Routine Inspection', reason=' Routine',
             number=None):
    row = th._insp(iid, date, score=score, rating='INFO_REDACTED', itype=itype, number=number or f'INSP-{iid}',
                   reason=reason, count=critical)
    row['nonCriticalInfractionCount'] = noncritical
    return row


def _wf_portal(page_size=500, **overrides):
    rows = overrides.pop('rows', _rows())
    inspections = {fid: _load(f'inspections_{name}.json') for fid, name in LISTS.items()}
    inspections.update({
        WALMART_BAKERY: [_wf_insp('wb1', '2026-11-06T10:00:00.0000000-06:00', 100)],
        WALMART_DELI: [_wf_insp('wd1', '2026-11-06T11:00:00.0000000-06:00', 97, noncritical=1)],
        RED_BARN: [_wf_insp('rb1', '2026-10-27T10:00:00.0000000-05:00', 100)],
        MPEC_NE: [_wf_insp('mp1', '2026-10-22T10:00:00.0000000-05:00', 100)],
        DOLLAR_TREE: [_wf_insp('dt1', '2026-10-19T10:00:00.0000000-05:00', 100)],
        DOLLAR_SAVER: [_wf_insp('ds1', '2026-09-17T10:00:00.0000000-05:00', 100)],
    })
    inspections.update(overrides.pop('inspections', {}))
    checklists = _checklists_for(inspections)
    checklists.update({'wb1': [], 'rb1': [], 'mp1': [], 'dt1': [], 'ds1': [],
                       'wd1': [_load('results_INSP-022432.json')[1]]})     # one Core citation
    checklists.update(overrides.pop('checklists', {}))
    return th.Portal(WF, th._lbk_pages(rows, page_size), inspections, checklists)


def _official_from_citations(items, cfg=WF):
    """The district's score as the Texas form computes it from what the
    checklist shows: 100 minus 3/2/1 once per cited item and tier."""
    pts = {'priority': 3, 'priority_foundation': 2, 'core': 1}
    seen = set()
    for item in items:
        for c in item.get('cannedComments') or [{}]:
            tier = dp.HH_SEVERITY.get(dp._hh_clean(c.get('severity')).lower()) \
                or dp._hh_category_tier(cfg, item.get('category'))
            seen.add((item['description'], tier))
    return 100 - sum(pts[t] for _, t in seen)


class ConfigTests(unittest.TestCase):
    def test_the_entry(self):
        self.assertEqual(WF['group'], 'Wichita Falls')
        self.assertEqual(WF['source'], WF_SRC)
        self.assertEqual(WF['base'], 'https://wfeh-portal.hedgerowsoftware.com/api/v0/portal/')
        self.assertEqual(WF['program'], 'd88b111d-edad-49f8-83a8-8c1d7931f99b')
        self.assertEqual((WF['list'], WF['list_sort'], WF['dated_list'], WF['official_score'], WF['results']),
                         ('paged', 'lastInspectionDate', True, True, 'grade'))
        self.assertEqual((WF['state'], WF['metro'], WF['lag_margin_days']), ('TX', '', 14))
        self.assertLessEqual(WF['history_start'], '2023-08-01')     # migrated records reach back to 2023-08

    def test_the_other_portals_carry_none_of_the_new_keys(self):
        new = {'name_type_sep', 'name_class_re', 'skip_types', 'skip_names', 'cities', 'tier_from_category',
               'category_tiers', 'category_tier_fixes', 'rule_placeholder_re', 'count_fields', 'type_placeholder_re'}
        for slug in ('pima', 'lubbock', 'mesquite'):
            self.assertEqual(new & set(dp.HH_PORTALS[slug]), set(), slug)
        self.assertEqual(new - set(WF), set())

    def test_jev_policy(self):
        self.assertIn(WF_SRC, jr.SOURCE_TIER_SOURCES)
        self.assertEqual(jr.score_policy(WF_SRC, 96), 'official')
        self.assertEqual(jr.tier_policy(WF_SRC), 'fixed')

    def test_the_fetcher_is_the_shared_client(self):
        with mock.patch.object(dp, '_fetch_hedgehog', return_value=['x']) as fh:
            self.assertEqual(dp.fetch_wichitafalls(since_date='2026-11-01', limit=5), ['x'])
        fh.assert_called_once_with('wichitafalls', '2026-11-01', 5)


class FacilityTests(unittest.TestCase):
    def _fac(self, name, address='3404 MAPLEWOOD DRIVE, Wichita Falls TX 76308'):
        skipped = defaultdict(int)
        row = {'id': 'f1', 'facilityName': name, 'facilityType': None, 'siteAddress': address,
               'lastInspectionDate': '2026-11-19T14:45:26.9591709-06:00', 'latitude': 33.87, 'longitude': -98.53}
        return dp._hh_facility(WF, row, skipped), dict(skipped)

    def test_a_permit_class_is_cut_off_the_name_and_only_a_class(self):
        cases = {
            'Taco Casa #73 - Retail Food Establishment (Medium)': 'Taco Casa #73',
            'Harvest, 7380 FM 171  - Retail Food Establihsment (Small) ': 'Harvest, 7380 FM 171',
            'Dollar General #3930 - DG Market Retail Food Establishment (Restricted Operation)': 'Dollar General #3930',
            'Popeyes - Wichita Falls - Retail Food Establishment (Small)': 'Popeyes - Wichita Falls',
            'Wingstop - Retail Food Establishment (Medium )': 'Wingstop',
            'Sonic - Food Retail Establishment (Small)': 'Sonic',
            'Walmart Supercenter #420 - Deli': 'Walmart Supercenter #420',
            'WinCo Foods - Fish market (add-on)': 'WinCo Foods',
            "Sam's Club #8224 - Multi-Departmental Grocery Store": "Sam's Club #8224",
            'Tortilleria Lupita - Produce Department': 'Tortilleria Lupita',
            'United Supermarket #521 - Cooking Area Snack Bar': 'United Supermarket #521',
            'Boomtown Bay Family Aquatic Center - Concession Stand': 'Boomtown Bay Family Aquatic Center',
            # a suffix that names ONE stand stays: four stands share 1000 5th St
            'MPEC Kay Yeager Coliseum - NE Concession Stand (Concession #2)':
                'MPEC Kay Yeager Coliseum - NE Concession Stand (Concession #2)',
            'United Market Street #517 - Starbucks Snack Bar': 'United Market Street #517 - Starbucks Snack Bar',
            'Nothing Bundt Cakes - Bakery Cafe': 'Nothing Bundt Cakes - Bakery Cafe',
        }
        for raw, name in cases.items():
            fac, skipped = self._fac(raw)
            self.assertIsNotNone(fac, raw)
            self.assertEqual(fac['name'], name, raw)

    def test_out_of_scope_types_and_names(self):
        cases = {
            'Batter Up! Homemade Sweets - Pop-up Food Vendor': 'pop-up vendor',
            'AB Foods - Pop-up ': 'pop-up vendor',
            'Dolce Matcha - Pop-Up Vendor': 'pop-up vendor',
            'Collective Coffee LLC - Pop-up Food Vendor (Push Cart)': 'mobile unit',
            'Southern Girl Catering - Catering Business': 'catering business',
            'Legacy High School - Retail Food Establishment (Large)': 'School',
            'Scotland Park Elementary School - Retail food Establishment (Large)': 'School',
            'W.F. George Middle School - Retail Food Establishment (Large)': 'School',
            'Electra Junior High/High School - Retail Food Establishment (Medium)': 'School',
            'City View Junior/Senior High School - Basketball/Volleyball Concession Stand': 'School',
            'Wichita Christian School - Concession Stand': 'School',
            'Christ Academy School - Retail Food Establishment (Small)': 'School',
            'Burkburnett ISD Warehouse - Retail Food Establishment (Restricted Operation)': 'School',
        }
        for raw, label in cases.items():
            fac, skipped = self._fac(raw)
            self.assertIsNone(fac, raw)
            self.assertEqual(skipped, {label: 1}, raw)
        # these stay in scope
        for raw in ('Haynes Head Start - Retail Food Establishment (Large)',
                    'Dandy Donuts School - Retail Food Establishment (Small)',
                    'Electra Memorial Hospital - Retail Food Establishment (Medium)',
                    'City View Athletic Booster Club Concessions - Football Concession Stand',
                    'Vernon College Micro Market - Retail Food Establishment (Restricted)'):
            self.assertIsNotNone(self._fac(raw)[0], raw)

    def test_addresses_split_at_a_known_town(self):
        cases = {
            '3404 MAPLEWOOD DRIVE, Wichita Falls TX 76308': ('3404 MAPLEWOOD DRIVE', 'Wichita Falls', '76308'),
            '114 West Cash Street, IOWA PARK TX 76367': ('114 West Cash Street', 'Iowa Park', '76367'),
            '4110 Business, US-287, Iowa Park, TX 76367': ('4110 Business, US-287', 'Iowa Park', '76367'),
            '106 S Red River, Expressway Burkburnett TX 76354': ('106 S Red River, Expressway', 'Burkburnett',
                                                                 '76354'),
            '599 South Ave D, Burkburnett Texas 76354': ('599 South Ave D', 'Burkburnett', '76354'),
            '3909 Kell East, Suite#A, WICHITA FALLS 76308': ('3909 Kell East, Suite#A', 'Wichita Falls', '76308'),
            '209 W. Cleveland Avenue, ELECTRA TX 76360': ('209 W. Cleveland Avenue', 'Electra', '76360'),
            '2522 Iowa Park Rd, Wichita Falls TX 76306': ('2522 Iowa Park Rd', 'Wichita Falls', '76306'),
            '4114 Burkburnett Rd., Su, WICHITA FALLS TX 76306': ('4114 Burkburnett Rd., Su', 'Wichita Falls',
                                                                  '76306'),
            # a town the district does not list falls through to the family's rules
            '100 Main St, Holliday TX 76366': ('100 Main St', 'Holliday', '76366'),
        }
        for addr, want in cases.items():
            street, city, state, zip5 = dp._hh_split_address(addr, WF)
            self.assertEqual((street, city, zip5), want, addr)
            self.assertEqual(state, 'TX', addr)

    def test_the_family_rules_still_read_the_other_portals_the_old_way(self):
        # 'Iowa Park, TX' is read city-before-comma by the family's rules: only
        # a portal that names its towns gets the known-town split
        self.assertEqual(dp._hh_split_address('100 Main St Iowa Park, TX 76367', th.LBK)[1], 'Park')
        self.assertEqual(dp._hh_split_address('100 Main St Iowa Park, TX 76367', WF)[1], 'Iowa Park')


class ViolationTests(unittest.TestCase):
    def test_a_citation_without_a_severity_takes_its_categorys_tier(self):
        lines = dp._hh_violations(_load('results_INSP-022432.json'), WF)
        self.assertEqual([v[1] for v in lines], ['priority', 'core'])
        self.assertEqual(lines[0][2], 'Proper Cold Holding Temperature (<41°F or 45°F for shelled eggs): Food on the '
                                      'cold hold rail had temperatures from 49F - 51F (41F or less is required)')
        # without the portal's config the family reads the same checklist with the classifier's tiers
        family = dp._hh_violations(_load('results_INSP-022432.json'))
        self.assertEqual([v[2] for v in family], [v[2] for v in lines])
        self.assertEqual(family, dp._hh_violations(_load('results_INSP-022432.json'), th.LBK))

    def test_the_stated_severity_wins_over_the_category(self):
        # The Pump City Diner: PIC items stated 'Priority Foundation' under '(P)', a
        # severity-null toxic item under 'Chemicals (P)'
        lines = dp._hh_violations(_load('results_INSP-022258.json'), WF)
        tiers = [v[1] for v in lines]
        self.assertEqual(tiers.count('priority'), 3)
        self.assertEqual(tiers.count('priority_foundation'), 4)
        self.assertEqual(tiers.count('core'), 6)
        self.assertTrue(all(v[1] == 'priority_foundation' for v in lines if v[2].startswith('Person in Charge')))

    def test_the_personnel_category_is_priority_foundation(self):
        # every severity-stating citation in 'Demonstration of Knowledge/Personnel (P)' says
        # Priority Foundation (30 of 30 live) and the score deducts 2: Tommy's 74 reproduces only so
        items = _load('results_INSP-021694.json')
        lines = dp._hh_violations(items, WF)
        personnel = [v for v in lines if v[2].startswith(('Person in Charge', 'Food Handler'))]
        self.assertEqual([v[1] for v in personnel], ['priority_foundation'] * 2)
        self.assertEqual(_official_from_citations(items), 74)
        literal = dict(WF, category_tier_fixes={})
        self.assertEqual(_official_from_citations(items, literal), 72)

    def test_every_complete_fixture_checklist_reproduces_the_official_score(self):
        rows = {}
        for name in LISTS.values():
            for insp in _load(f'inspections_{name}.json'):
                rows[insp['inspectionNumber']] = insp
        checked = 0
        for fn in sorted(os.listdir(FIXTURES)):
            if not fn.startswith('results_'):
                continue
            number = fn[len('results_'):-len('.json')]
            items, row = _load(fn), rows[number]
            shown = sum(len(i.get('cannedComments') or [{}]) for i in items)
            if shown < row['criticalInfractionCount'] + row['nonCriticalInfractionCount']:
                continue                     # the portal hides citations here (Kathy's, Braum's, Tommy's 88)
            self.assertEqual(_official_from_citations(items), row['hazardScore'], number)
            checked += 1
        self.assertGreaterEqual(checked, 6)

    def test_a_placeholder_comment_is_not_a_rule_name(self):
        lines = dp._hh_violations(_load('results_INSP-022258.json'), WF)
        self.assertIn('Non-Food Contact Surfaces Clean', [v[2] for v in lines])
        self.assertFalse(any('See violation observations' in v[2] for v in lines))
        # the family (Mesquite's short rule names) would have shipped it
        family = dp._hh_violations(_load('results_INSP-022258.json'))
        self.assertTrue(any('See violation observations above' in v[2] for v in family))

    def test_a_citation_with_no_tier_anywhere_raises(self):
        items = [{'description': 'Proper Cold Holding Temperature', 'result': 'Out of Compliance',
                  'category': {'description': 'Food Safety Miscellany'},
                  'cannedComments': [{'severity': None, 'observationComment': 'Milk at 48F.'}]}]
        with self.assertRaises(dp._HhTierError):
            dp._hh_violations(items, WF)
        items[0]['category'] = None
        with self.assertRaises(dp._HhTierError):
            dp._hh_violations(items, WF)
        # a stated severity needs no category
        items[0]['cannedComments'][0]['severity'] = 'Priority'
        self.assertEqual(dp._hh_violations(items, WF)[0][1], 'priority')
        # the family never raises for it: the classifier's tier stands (Lubbock, Mesquite, Pima)
        items[0]['cannedComments'][0]['severity'] = None
        self.assertEqual(len(dp._hh_violations(items)), 1)
        self.assertTrue(issubclass(dp._HhTierError, ValueError))

    def test_category_suffix_forms(self):
        for desc, tier in (('Physical Facilities (C)', 'core'), ('Chemicals (P)', 'priority'),
                           ('Utensils, Equipment, and Vending (PF)', 'priority_foundation'),
                           ('Consumer Advisory ( pf )', 'priority_foundation'),
                           ('Demonstration of Knowledge/Personnel (P)', 'priority_foundation'),
                           # before 2025-04 the district printed no suffix: its later suffix stands in
                           ('Physical Facilities', 'core'), ('Chemicals', 'priority'),
                           ('Physical Facilities (PF)', 'priority_foundation'),     # a typed suffix wins
                           ('Food Temperature Control/ Identification', 'priority_foundation'),
                           ('Demonstration of Knowledge/Personnel', 'priority_foundation'),
                           # the HACCP/variance item's category, scored 2 off where the score is shown
                           ('Conformance with Approved Procedures', 'priority_foundation'),
                           ('Food Safety Miscellany', None), ('(C) Physical Facilities', None), ('', None)):
            self.assertEqual(dp._hh_category_tier(WF, {'description': desc}), tier, desc)
        self.assertIsNone(dp._hh_category_tier(WF, None))

    def test_the_texas_pest_criterion_ships_under_the_neutral_heading(self):
        cafe = dp._hh_violations(_load('results_INSP-022127.json'), WF)
        pest = [v for v in cafe if v[2].startswith(dp.HH_PEST_TITLE)]
        self.assertEqual(pest[0][2], 'Pests and animals: Observed 3 dead cockroaches in back storage area')
        self.assertEqual(pest[0][1], 'core')
        self.assertFalse(any('Insect Contamination' in v[2] for v in cafe))
        # the title alone, or flies, never move the grade; dead roaches cap it at C, live ones and droppings force an F
        self.assertFalse(dp.has_active_pest_or_sewage(dp.HH_PEST_TITLE))
        self.assertTrue(dp._HH_PEST_TITLE_RE.match('No evidence of Insect Contamination, Rodent, Other Animals'))
        braums = dp._hh_violations(_load('results_INSP-022027.json'), WF)
        self.assertEqual(dp.calculate_vetted_grade(98, dp.build_violation_text(braums)), 'A')
        self.assertEqual(dp.calculate_vetted_grade(95, dp.build_violation_text(cafe)), 'C')   # dead roaches: capped at C
        pump = dp._hh_violations(_load('results_INSP-022258.json'), WF)
        self.assertEqual(dp.calculate_vetted_grade(84, dp.build_violation_text(pump)), 'F')
        # a line ending in an evidence word before the heading does not reach the pest word
        text = dp.build_violation_text([['x', 'core', 'Soap: evidence of no soap found'],
                                        ['pest_control', 'core', 'Pests and animals: Door gap at the back door']])
        self.assertFalse(dp.has_active_pest_or_sewage(text))


class FetchTests(th._Base):
    def test_list_request_shape(self):
        portal = _wf_portal()
        th._run(portal, dp.fetch_wichitafalls, since='2026-11-10')
        method, path, body = portal.calls[0]
        self.assertEqual((method, path), ('POST', 'disclosure/program/facilities'))
        self.assertEqual(body, {'pageNumber': 0, 'pageSize': dp.HH_PAGE, 'criteria': '',
                                'sort': [{'field': 'lastInspectionDate', 'order': 'desc'}],
                                'disclosureProgramId': WF['program'], 'fields': [], 'filters': []})

    def test_record(self):
        records = th._run(_wf_portal(), dp.fetch_wichitafalls, since='2026-11-10')
        r = th._by_id(records)[f'wfalls_{TACO_1119}']
        self.assertEqual(r, {
            'name': 'Taco Casa #73',
            'address': '3404 MAPLEWOOD DRIVE',
            'city': 'Wichita Falls',
            'state': 'TX',
            'zip': '76308',
            'latitude': 33.8736118,
            'longitude': -98.5382038,
            'inspection_date': '2026-11-19',
            'original_score': 96,
            'risk_score': 96,
            'priority_violations': 1,
            'priority_foundation_violations': 0,
            'core_violations': 1,
            'total_violations': 2,
            'violations': dp._hh_violations(_load('results_INSP-022432.json'), WF),
            'inspection_type': 'Permit Inspection',
            'results': 'Grade A',
            'source': WF_SRC,
            'source_url': f'https://wfeh-portal.hedgerowsoftware.com/#/{WF["program"]}/disclosure/'
                          f'facility/{TACO_CASA}/inspection/{TACO_1119}',
            'source_id': f'wfalls_{TACO_1119}',
            'metro': '',
        })

    def test_window_scope_and_the_lag_margin(self):
        portal = _wf_portal()
        records = th._run(portal, dp.fetch_wichitafalls, since='2026-08-24')
        # 14 days before the window asked for: Braum's 2026-08-28 and nothing older
        self.assertEqual(min(r['inspection_date'] for r in records), '2026-08-28')
        self.assertEqual(len(records), 16)
        names = {r['name'] for r in records}
        self.assertEqual(names, {'Taco Casa #73', 'Walmart Supercenter #1148', 'The Pump City Diner',
                                 'Red Barn Butchers LLC', 'City Cafe',
                                 'MPEC Kay Yeager Coliseum - NE Concession Stand (Concession #2)',
                                 'Dollar Tree #10355', "Tommy's Donuts", "Braum's Ice Cream & Dairy Store #178",
                                 "Kathy's Korner", 'Dollar Saver #3'})
        # the school, the pop-up, the caterer and the push cart were never asked for
        for fid in (LEGACY_HS, CATERER, PUSH_CART):
            self.assertNotIn(fid, portal.lists_requested())
        self.assertEqual(dp._FRESHNESS_HOLDS, {})
        self.assertEqual(dp._GROUP_STATUS, {})

    def test_a_grocerys_departments_share_one_restaurant(self):
        records = th._run(_wf_portal(), dp.fetch_wichitafalls, since='2026-11-01')
        walmart = [r for r in records if r['name'] == 'Walmart Supercenter #1148']
        self.assertEqual(len(walmart), 2)
        ids = {dp.make_restaurant_id(r['name'], r['address'], r['city']) for r in walmart}
        self.assertEqual(len(ids), 1)
        # worst of the day first (D1 keeps one inspection per restaurant per day)
        self.assertEqual([r['risk_score'] for r in walmart], [97, 100])

    def test_cities(self):
        records = th._run(_wf_portal(), dp.fetch_wichitafalls, since='2026-08-24')
        by_name = {r['name']: r for r in records}
        self.assertEqual(by_name['City Cafe']['city'], 'Iowa Park')
        self.assertEqual(by_name['The Pump City Diner']['city'], 'Electra')
        self.assertEqual(by_name["Kathy's Korner"]['city'], 'Electra')
        self.assertEqual(by_name['Dollar Tree #10355']['city'], 'Burkburnett')
        self.assertEqual((by_name['Dollar Saver #3']['address'], by_name['Dollar Saver #3']['city']),
                         ('4110 Business, US-287', 'Iowa Park'))
        self.assertEqual((by_name['Red Barn Butchers LLC']['city'], by_name['Red Barn Butchers LLC']['zip']),
                         ('Wichita Falls', '76308'))
        self.assertEqual({r['city'] for r in records}, {'Wichita Falls', 'Iowa Park', 'Electra', 'Burkburnett'})

    def test_the_score_is_official_and_never_recomputed(self):
        records = th._run(_wf_portal(), dp.fetch_wichitafalls, since='2026-08-24')
        r = th._by_id(records)[f'wfalls_{TOMMYS_74}']
        self.assertEqual((r['original_score'], r['risk_score'], r['results']), (74, 74, 'Grade C'))
        self.assertEqual((r['priority_violations'], r['priority_foundation_violations'], r['core_violations']),
                         (4, 5, 16))
        self.assertNotEqual(r['risk_score'], dp.calc_risk_score(r['violations'])[0])

    def test_a_checklist_short_of_the_rows_count_still_ships_its_official_score(self):
        # Kathy's Korner: the row counts 4 citations, the checklist shows 3 (one hidden by the portal);
        # the district's score is the record's score, as Lubbock's would be
        records = th._run(_wf_portal(), dp.fetch_wichitafalls, since='2026-08-24')
        r = th._by_id(records)[f'wfalls_{KATHYS_95}']
        self.assertEqual((r['risk_score'], r['total_violations']), (95, 3))

    def test_an_empty_checklist_is_clean_only_when_both_counts_are_zero(self):
        portal = _wf_portal(inspections={TACO_CASA: [
            _wf_insp('clean', '2026-11-12T10:00:00.0000000-06:00', 100),
            _wf_insp('hidden_c', '2026-11-13T10:00:00.0000000-06:00', 99, noncritical=1),
            _wf_insp('hidden_p', '2026-11-14T10:00:00.0000000-06:00', 97, critical=1),
        ]}, checklists={'clean': [], 'hidden_c': [], 'hidden_p': []})
        by_id = th._by_id(th._run(portal, dp.fetch_wichitafalls, since='2026-11-10'))
        self.assertEqual((by_id['wfalls_clean']['risk_score'], by_id['wfalls_clean']['violations']), (100, []))
        self.assertNotIn('wfalls_hidden_c', by_id)       # criticalInfractionCount alone would have said clean
        self.assertNotIn('wfalls_hidden_p', by_id)
        self.assertEqual(dp._FRESHNESS_HOLDS, {WF_SRC: {'2026-11-13|hidden_c': (True, False),
                                                        '2026-11-14|hidden_p': (True, False)}})

    def test_a_citation_of_no_stated_tier_drops_and_holds_its_inspection(self):
        bad = _load('results_INSP-022432.json')
        bad[0]['category'] = {'description': 'Food Safety Miscellany'}
        portal = _wf_portal(checklists={TACO_1119: bad})
        records = th._run(portal, dp.fetch_wichitafalls, since='2026-11-10')
        self.assertNotIn(f'wfalls_{TACO_1119}', th._by_id(records))
        self.assertEqual(dp._FRESHNESS_HOLDS, {WF_SRC: {f'2026-11-19|{TACO_1119}': (True, False)}})
        self.assertEqual(dp._GROUP_STATUS, {})           # one record: not a mass failure

    def test_many_citations_of_no_stated_tier_turn_the_group_red(self):
        rows = [dict(_rows()[0], id=f'fac{i}') for i in range(12)]
        bad = _load('results_INSP-022432.json')
        bad[0]['category'] = None
        inspections = {f'fac{i}': [_wf_insp(f'i{i}', '2026-11-19T10:00:00.0000000-06:00', 96, noncritical=2)]
                       for i in range(12)}
        portal = _wf_portal(rows=rows, inspections=inspections,
                            checklists={f'i{i}': bad for i in range(12)})
        self.assertEqual(th._run(portal, dp.fetch_wichitafalls, since='2026-11-10'), [])
        self.assertEqual(dp._GROUP_STATUS['Wichita Falls'][0], 'error')

    def test_the_type_drop_down_placeholder_falls_back_to_the_reason(self):
        portal = _wf_portal(inspections={TACO_CASA: [
            _wf_insp('ph', '2026-11-12T10:00:00.0000000-06:00', 100, itype='Please Select the Inspection Type',
                     reason=' Routine')]}, checklists={'ph': []})
        r = th._by_id(th._run(portal, dp.fetch_wichitafalls, since='2026-11-10'))['wfalls_ph']
        self.assertEqual(r['inspection_type'], 'Routine')

    def test_pages_until_the_last_row_leaves_the_window(self):
        with mock.patch.object(dp, 'HH_PAGE', 4):
            portal = _wf_portal(page_size=4)
            records = th._run(portal, dp.fetch_wichitafalls, since='2026-11-15')
        posts = [c for c in portal.calls if c[0] == 'POST']
        # 14-day margin: read from 2026-11-01. Page 0 ends 2026-11-06 (Walmart), page 1 ends 2026-10-27
        self.assertEqual([c[2]['pageNumber'] for c in posts], [0, 1])
        self.assertEqual({r['name'] for r in records}, {'Taco Casa #73', 'Walmart Supercenter #1148',
                                                         'The Pump City Diner'})

    def test_full_history_reads_every_page(self):
        with mock.patch.object(dp, 'HH_PAGE', 5):
            portal = _wf_portal(page_size=5)
            records = th._run(portal, dp.fetch_wichitafalls)
        self.assertEqual(len([c for c in portal.calls if c[0] == 'POST']), 4)
        self.assertEqual(min(r['inspection_date'] for r in records), '2026-08-28')   # only those checklists exist
        self.assertGreater(len(records), 15)

    def test_a_failed_page_raises(self):
        with mock.patch.object(dp, 'HH_PAGE', 4):
            portal = _wf_portal(page_size=4)
            portal.facilities[1] = th._Resp({'Message': 'An error has occurred.'}, 500)
            with self.assertRaises(RuntimeError):
                th._run(portal, dp.fetch_wichitafalls, since='2026-10-01')
        self.assertEqual(dp._FRESHNESS_HOLDS, {})

    def test_an_unread_checklist_drops_and_holds(self):
        portal = _wf_portal(checklists={CITY_CAFE_95: dp.requests.Timeout('slow')})
        records = th._run(portal, dp.fetch_wichitafalls, since='2026-10-20')
        self.assertNotIn(f'wfalls_{CITY_CAFE_95}', th._by_id(records))
        self.assertEqual(dp._FRESHNESS_HOLDS, {WF_SRC: {f'2026-10-22|{CITY_CAFE_95}': (True, False)}})

    def test_the_sources_newest_row_goes_to_the_lag_note_after_a_complete_read(self):
        with mock.patch.object(dp, '_note_source_lag') as note:
            th._run(_wf_portal(), dp.fetch_wichitafalls, since='2026-11-10')
        note.assert_called_once_with('Wichita Falls', '2026-11-19')


if __name__ == '__main__':
    unittest.main()
